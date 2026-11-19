#!/usr/bin/env python3
"""Export the current 1724 Flink/Kafka experiment traces from Prometheus."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import base64
import gzip
import io

import requests
import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
EXPERIMENT_ROOT = SCRIPT_DIR.parent
REPO_ROOT = EXPERIMENT_ROOT.parent.parent
CLUSTER_ROOT = REPO_ROOT / "cluster"
CLUSTER_ENV = CLUSTER_ROOT / "config" / "env.sh"
EXPERIMENT_DATA_DIR = EXPERIMENT_ROOT / "experiment-data"
RUNTIME_DIR = EXPERIMENT_ROOT / ".runtime"
DEFAULT_PROM_URL = "http://localhost:9091"
DEFAULT_FLINK_URL = "http://localhost:8081"

TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"


def log(message: str) -> None:
    stamp = datetime.now(timezone.utc).strftime(TIME_FMT)
    print(f"[{stamp}] {message}", flush=True)


def run(cmd: list[str], *, cwd: Path | None = None) -> str:
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{result.stderr.strip()}")
    return result.stdout


def run_shell(command: str, *, cwd: Path | None = None) -> str:
    result = subprocess.run(
        ["bash", "-lc", command],
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Shell command failed: {command}\n{result.stderr.strip()}")
    return result.stdout


def run_cluster(cmd: list[str], *, cwd: Path | None = None) -> str:
    quoted = shlex.join(cmd)
    return run_shell(
        f"source {CLUSTER_ENV} >/dev/null 2>&1 && {quoted}",
        cwd=cwd,
    )


def slugify(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-").lower()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(TIME_FMT)


def parse_operator_timestamp(text: str) -> datetime | None:
    match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3})", text)
    if not match:
        return None
    base = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return base + timedelta(milliseconds=int(match.group(2)))


@dataclass
class PortForward:
    process: subprocess.Popen[bytes] | None
    log_path: Path

    def stop(self) -> None:
        if self.process is None:
            return
        try:
            os.killpg(self.process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def endpoint_is_healthy(url: str, path: str) -> bool:
    try:
        response = requests.get(url + path, timeout=3)
        return response.ok
    except requests.RequestException:
        return False


def ensure_port_forward(
    *,
    target_url: str,
    health_path: str,
    command: str,
    log_name: str,
) -> PortForward:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    log_path = RUNTIME_DIR / log_name
    if endpoint_is_healthy(target_url, health_path):
        return PortForward(None, log_path)

    handle = open(log_path, "ab", buffering=0)
    proc = subprocess.Popen(
        ["bash", "-lc", f"source {CLUSTER_ENV} >/dev/null 2>&1 && exec {command}"],
        stdout=handle,
        stderr=handle,
        start_new_session=True,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"Port-forward exited immediately: {command}")
        if endpoint_is_healthy(target_url, health_path):
            return PortForward(proc, log_path)
        time.sleep(1)

    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    raise RuntimeError(f"Port-forward did not become healthy: {command}")


class PrometheusClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        response = requests.get(f"{self.base_url}{path}", params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus query failed: {payload}")
        return payload

    def query_range(self, expr: str, start: float, end: float, step_seconds: int) -> list[dict[str, Any]]:
        payload = self._get(
            "/api/v1/query_range",
            {
                "query": expr,
                "start": f"{start:.3f}",
                "end": f"{end:.3f}",
                "step": f"{step_seconds}s",
            },
        )
        return payload["data"]["result"]


def discover_producer_config() -> dict[str, Any]:
    output = run(["ps", "-eo", "args"], cwd=SCRIPT_DIR)
    matches: list[dict[str, Any]] = []
    for line in output.splitlines():
        if "scaling-kafka-coordinator.py" not in line:
            continue
        tps = re.search(r"--tps\s+(\d+)", line)
        events = re.search(r"--events\s+(\d+)", line)
        parallelism = re.search(r"--parallelism\s+(\d+)", line)
        if tps and events and parallelism:
            matches.append(
                {
                    "tps": int(tps.group(1)),
                    "events": int(events.group(1)),
                    "parallelism": int(parallelism.group(1)),
                    "source": "coordinator-process",
                }
            )
    if matches:
        return matches[-1]

    target_host = os.environ.get("TARGET_HOST", "c153")
    host_tag = os.environ.get("HOST_TAG", target_host)

    try:
        raw = run_shell(
            f"ssh {target_host} \"sudo -n docker inspect standalone-insert-kafka-{host_tag} --format '{{{{json .Config.Cmd}}}}'\"",
            cwd=SCRIPT_DIR,
        ).strip()
        if raw:
            cmd = " ".join(json.loads(raw))
            tps = re.search(r"--tps\s+(\d+)", cmd)
            events = re.search(r"--events\s+(\d+)", cmd)
            parallelism = re.search(r"-Dparallelism\\.default=(\d+)", cmd)
            if tps and events and parallelism:
                return {
                    "tps": int(tps.group(1)),
                    "events": int(events.group(1)),
                    "parallelism": int(parallelism.group(1)),
                    "source": "producer-container",
                }
    except Exception:
        pass

    return {"tps": -1, "events": -1, "parallelism": -1, "source": "unknown"}


def discover_deployment(deployment: str) -> dict[str, Any]:
    raw = run_cluster(["kubectl", "get", "flinkdeployment", deployment, "-o", "json"], cwd=CLUSTER_ROOT)
    return json.loads(raw)


def discover_job_vertices(flink_url: str, job_id: str) -> dict[str, Any]:
    response = requests.get(f"{flink_url}/jobs/{job_id}", timeout=20)
    response.raise_for_status()
    return response.json()


def find_vertex_ids(job_detail: dict[str, Any]) -> dict[str, str]:
    ids: dict[str, str] = {}
    for vertex in job_detail.get("vertices", []):
        name = vertex.get("name", "")
        if name.startswith("Join"):
            ids["join"] = vertex["id"]
        elif "bid_kafka" in name:
            ids["bid_source"] = vertex["id"]
        elif "auction_kafka" in name:
            ids["auction_source"] = vertex["id"]
    return ids


def write_series_csv(path: Path, result: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "ts_iso", "series", "value"])
        for series in result:
            metric = series.get("metric", {})
            name = metric.get("task_name") or metric.get("operator_name") or metric.get("pod") or "value"
            values = series.get("values", [])
            for ts, value in values:
                writer.writerow([f"{float(ts):.3f}", to_iso(float(ts)), name, value])


def write_rows_csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def parse_operator_scaling_events(
    *,
    since_seconds: int,
    join_vertex_id: str | None,
    start_ts: float,
    end_ts: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    command = ["kubectl", "logs", "deployment/flink-kubernetes-operator", f"--since={since_seconds}s"]
    output = run_cluster(command, cwd=CLUSTER_ROOT)

    scaling_events: list[dict[str, Any]] = []
    memory_events: list[dict[str, Any]] = []

    lines = output.splitlines()
    for index, line in enumerate(lines):
        timestamp = parse_operator_timestamp(line)
        if timestamp is None:
            continue
        unix_ts = timestamp.timestamp()
        if unix_ts < start_ts - 120 or unix_ts > end_ts + 120:
            continue

        if "SCALINGREPORT" in line and "Parallelism" in line:
            match = re.search(
                r"Vertex ID (?P<vertex>[0-9a-f]+) \| Parallelism (?P<old>\d+) -> (?P<new>\d+)",
                line,
            )
            if not match:
                continue
            if join_vertex_id and match.group("vertex") != join_vertex_id:
                continue
            event: dict[str, Any] = {
                "timestamp": unix_ts,
                "ts_iso": to_iso(unix_ts),
                "vertex_id": match.group("vertex"),
                "parallelism_old": int(match.group("old")),
                "parallelism_new": int(match.group("new")),
                "memory_old_bytes": "",
                "memory_new_bytes": "",
            }
            for nearby in lines[index : index + 6]:
                memory_match = re.search(
                    r"Adjusting memory from (?P<old>\d+) bytes to (?P<new>\d+) bytes",
                    nearby,
                )
                if memory_match:
                    event["memory_old_bytes"] = int(memory_match.group("old"))
                    event["memory_new_bytes"] = int(memory_match.group("new"))
                    memory_events.append(
                        {
                            "timestamp": unix_ts,
                            "ts_iso": to_iso(unix_ts),
                            "memory_old_bytes": int(memory_match.group("old")),
                            "memory_new_bytes": int(memory_match.group("new")),
                        }
                    )
                    break
            scaling_events.append(event)
    return scaling_events, memory_events


def parse_rescale_windows(
    deployment: str,
    *,
    start_ts: float,
    end_ts: float,
) -> list[dict[str, Any]]:
    raw = run_cluster(["kubectl", "get", "events", "-o", "json"], cwd=CLUSTER_ROOT)
    payload = json.loads(raw)
    entries: list[tuple[float, str]] = []
    for item in payload.get("items", []):
        involved = item.get("involvedObject", {})
        if involved.get("kind") != "FlinkDeployment" or involved.get("name") != deployment:
            continue
        message = item.get("message", "")
        if "Job status changed from" not in message:
            continue
        ts_text = (
            item.get("eventTime")
            or item.get("series", {}).get("lastObservedTime")
            or item.get("lastTimestamp")
            or item.get("firstTimestamp")
            or item.get("metadata", {}).get("creationTimestamp")
        )
        if not ts_text:
            continue
        ts = datetime.fromisoformat(ts_text.replace("Z", "+00:00")).timestamp()
        if ts < start_ts - 300 or ts > end_ts + 300:
            continue
        entries.append((ts, message))

    entries.sort()
    windows: list[dict[str, Any]] = []
    pending_start: float | None = None
    pending_label = "rescale"
    for ts, message in entries:
        if "to CREATED" in message or "to RECONCILING" in message:
            pending_start = ts
            pending_label = message.split("Job status changed from ", 1)[-1]
        elif "to RUNNING" in message and pending_start is not None:
            windows.append(
                {
                    "start": pending_start,
                    "end": ts,
                    "start_iso": to_iso(pending_start),
                    "end_iso": to_iso(ts),
                    "label": pending_label,
                }
            )
            pending_start = None
    return windows


def parse_memory_from_spec_events(
    deployment: str,
    *,
    join_vertex_id: str | None,
    start_ts: float,
    end_ts: float,
) -> list[dict[str, Any]]:
    """Extract Justin memory levels from SpecChanged kubernetes events.

    Justin changes memory via pipeline.jobvertex-resourceprofile-overrides rather than
    emitting log lines, and the operator log buffer rotates too fast to be reliable.
    This parses SpecChanged events which persist in the kubernetes event store.
    """
    raw = run_cluster(["kubectl", "get", "events", "-o", "json"], cwd=CLUSTER_ROOT)
    payload = json.loads(raw)
    events: list[dict[str, Any]] = []
    for item in payload.get("items", []):
        involved = item.get("involvedObject", {})
        if involved.get("kind") != "FlinkDeployment" or involved.get("name") != deployment:
            continue
        if item.get("reason") != "SpecChanged":
            continue
        msg = item.get("message", "")
        if "pipeline.jobvertex-resourceprofile-overrides" not in msg:
            continue
        pattern = (
            rf"{join_vertex_id}[^}}]*?managedMemory=.*?\((\d+) bytes\)"
            if join_vertex_id
            else r"managedMemory=.*?\((\d+) bytes\)"
        )
        match = re.search(pattern, msg)
        if not match:
            continue
        managed_bytes = int(match.group(1))
        ts_text = (
            item.get("eventTime")
            or item.get("series", {}).get("lastObservedTime")
            or item.get("lastTimestamp")
            or item.get("firstTimestamp")
            or item.get("metadata", {}).get("creationTimestamp")
        )
        if not ts_text:
            continue
        ts = datetime.fromisoformat(ts_text.replace("Z", "+00:00")).timestamp()
        if ts < start_ts - 300 or ts > end_ts + 300:
            continue
        events.append({
            "timestamp": ts,
            "ts_iso": to_iso(ts),
            "memory_old_bytes": managed_bytes,
            "memory_new_bytes": managed_bytes,
        })
    events.sort(key=lambda e: e["timestamp"])
    return events


def build_parallelism_from_log(
    scaling_events: list[dict[str, Any]],
    *,
    start_ts: float,
    end_ts: float,
) -> list[tuple[float, int]]:
    """Return (timestamp, parallelism) step-function points derived from log SCALINGREPORT events."""
    initial = int(scaling_events[0]["parallelism_old"]) if scaling_events else 1
    points: list[tuple[float, int]] = [(start_ts, initial)]
    for event in scaling_events:
        points.append((float(event["timestamp"]), int(event["parallelism_new"])))
    if points[-1][0] < end_ts:
        points.append((end_ts, points[-1][1]))
    return points


def write_parallelism_log_csv(path: Path, points: list[tuple[float, int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "ts_iso", "series", "value"])
        for ts, parallelism in points:
            writer.writerow([f"{ts:.3f}", to_iso(ts), "join", parallelism])


def build_memory_timeline(
    *,
    start_ts: float,
    end_ts: float,
    memory_events: list[dict[str, Any]],
    autoscaler: str = "ds2",
) -> list[dict[str, Any]]:
    real_changes = [e for e in memory_events if int(e["memory_old_bytes"]) != int(e["memory_new_bytes"])]
    if not real_changes:
        # For Justin, build a flat timeline using the stable memory value so ml= annotations appear.
        # For DS2, memory levels are meaningless — return empty.
        if not memory_events or autoscaler != "justin":
            return []
        initial = int(memory_events[0]["memory_old_bytes"])
    else:
        memory_events = real_changes
        initial = int(memory_events[0]["memory_old_bytes"])

    points = [
        {
            "timestamp": start_ts,
            "ts_iso": to_iso(start_ts),
            "memory_bytes": initial,
        }
    ]
    current = initial
    for event in memory_events:
        new_value = int(event["memory_new_bytes"])
        if new_value == current and points:
            continue
        current = new_value
        points.append(
            {
                "timestamp": float(event["timestamp"]),
                "ts_iso": event["ts_iso"],
                "memory_bytes": current,
            }
        )
    if points[-1]["timestamp"] != end_ts:
        points.append(
            {
                "timestamp": end_ts,
                "ts_iso": to_iso(end_ts),
                "memory_bytes": current,
            }
        )

    unique_values = sorted({int(point["memory_bytes"]) for point in points})
    levels = {value: index for index, value in enumerate(unique_values)}
    for point in points:
        point["memory_level"] = levels[int(point["memory_bytes"])]
        point["memory_mb"] = round(int(point["memory_bytes"]) / (1024 * 1024), 3)
    return points


def _fetch_autoscaler_cm_data(deployment: str) -> dict[str, Any]:
    for attempt in [
        ["kubectl", "get", "configmap", "-l", f"component=autoscaler,app={deployment}", "-o", "json"],
        None,
    ]:
        if attempt is not None:
            try:
                raw = run_cluster(attempt, cwd=CLUSTER_ROOT)
                items = json.loads(raw).get("items", [])
                if items:
                    items.sort(key=lambda item: item.get("metadata", {}).get("creationTimestamp", ""))
                    return items[-1]
            except Exception:
                pass
        else:
            try:
                raw = run_cluster(["kubectl", "get", "configmap", f"autoscaler-{deployment}", "-o", "json"], cwd=CLUSTER_ROOT)
                return json.loads(raw)
            except Exception:
                pass
    return {}


def _parse_cm_ts(key: Any) -> float | None:
    if isinstance(key, datetime):
        ts = key if key.tzinfo else key.replace(tzinfo=timezone.utc)
        return ts.timestamp()
    if isinstance(key, str):
        try:
            return datetime.fromisoformat(key.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def parse_justin_scaling_history(
    deployment: str,
    join_vertex_id: str | None,
    *,
    start_ts: float,
    end_ts: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Parse Justin scaling decisions from autoscaler ConfigMap scalingConfigHistory.

    Returns (parallelism_rows, memory_rows).
    - parallelism_rows: timestamp/ts_iso/series/value rows for join_parallelism.csv
    - memory_rows: timestamp/ts_iso/memory_bytes/memory_mb/memory_level rows
    """
    cm = _fetch_autoscaler_cm_data(deployment)
    data = cm.get("data") or {}
    encoded = data.get("scalingConfigHistory")
    if not encoded:
        log("WARNING: no scalingConfigHistory in autoscaler ConfigMap")
        return [], []

    payload = yaml.safe_load(io.StringIO(gzip.decompress(base64.b64decode(encoded)).decode("utf-8"))) or {}
    if not isinstance(payload, dict):
        log("WARNING: scalingConfigHistory decoded to unexpected type")
        return [], []

    # Parse all snapshots, sorted by (timestamp, period)
    snapshots: list[tuple[float, int, dict[str, dict[str, int]]]] = []
    for ts_key, snapshot in payload.items():
        if not isinstance(snapshot, dict):
            continue
        unix_ts = _parse_cm_ts(ts_key)
        if unix_ts is None:
            continue
        period = int(snapshot.get("period", 0))
        vertices: dict[str, dict[str, int]] = {}
        for vid, info in (snapshot.get("scaling") or {}).items():
            if isinstance(info, dict):
                vertices[str(vid)] = {
                    "parallelism": int(info.get("parallelism", 1)),
                    "memory_level": int(info.get("memoryLevel", 0)),
                }
        snapshots.append((unix_ts, period, vertices))
    snapshots.sort(key=lambda x: (x[0], x[1]))

    def _get_vertex_info(vertices: dict[str, dict[str, int]]) -> dict[str, int] | None:
        if join_vertex_id:
            if join_vertex_id in vertices:
                return vertices[join_vertex_id]
            # prefix match
            for vid, info in vertices.items():
                if vid.startswith(join_vertex_id[:8]):
                    return info
        elif vertices:
            return next(iter(vertices.values()))
        return None

    # Step function: initial state is p=1, ml=0 (Justin always starts here)
    p_steps: list[tuple[float, int]] = [(start_ts, 1)]
    ml_steps: list[tuple[float, int]] = [(start_ts, 0)]

    for unix_ts, _period, vertices in snapshots:
        info = _get_vertex_info(vertices)
        if info is None:
            continue
        p_steps.append((unix_ts, info["parallelism"]))
        ml_steps.append((unix_ts, info["memory_level"]))

    # Close out at end_ts
    if p_steps[-1][0] < end_ts:
        p_steps.append((end_ts, p_steps[-1][1]))
    if ml_steps[-1][0] < end_ts:
        ml_steps.append((end_ts, ml_steps[-1][1]))

    parallelism_rows = [
        {"timestamp": f"{ts:.3f}", "ts_iso": to_iso(ts), "series": "join", "value": str(p)}
        for ts, p in p_steps
    ]
    memory_rows = [
        {
            "timestamp": f"{ts:.3f}",
            "ts_iso": to_iso(ts),
            "memory_bytes": ml,
            "memory_mb": float(ml),
            "memory_level": ml,
        }
        for ts, ml in ml_steps
    ]
    log(f"Justin ConfigMap: {len(snapshots)} snapshots → {len(p_steps)} parallelism steps, {len(ml_steps)} memory steps")
    return parallelism_rows, memory_rows


def build_output_dir(
    *,
    autoscaler: str,
    producer: dict[str, Any],
    explicit_name: str | None,
) -> Path:
    if explicit_name:
        name = explicit_name
    else:
        stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
        tps = producer["tps"] if producer["tps"] >= 0 else "unknown"
        if isinstance(tps, int) and tps % 1000 == 0:
            tps_label = f"{int(tps / 1000)}K"
        else:
            tps_label = str(tps)
        events = producer["events"] if producer["events"] >= 0 else "unknown"
        parallelism = producer["parallelism"] if producer["parallelism"] >= 0 else "unknown"
        name = f"{stamp}__{autoscaler}__producer-tps{tps_label}-p{parallelism}-events{events}"
    path = EXPERIMENT_DATA_DIR / slugify(name)
    path.mkdir(parents=True, exist_ok=True)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment", default="flink")
    parser.add_argument("--prom-url", default=DEFAULT_PROM_URL)
    parser.add_argument("--flink-url", default=DEFAULT_FLINK_URL)
    parser.add_argument("--step-seconds", type=int, default=15)
    parser.add_argument("--job-id")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--name")
    args = parser.parse_args()

    EXPERIMENT_DATA_DIR.mkdir(parents=True, exist_ok=True)

    prom_pf = ensure_port_forward(
        target_url=args.prom_url,
        health_path="/-/ready",
        command="kubectl port-forward --address 0.0.0.0 -n manager svc/prom-kube-prometheus-stack-prometheus 9091:9090",
        log_name="export-prometheus-port-forward.log",
    )
    flink_pf = ensure_port_forward(
        target_url=args.flink_url,
        health_path="/jobs/overview",
        command=f"kubectl port-forward svc/{args.deployment}-rest 8081:8081",
        log_name="export-flink-port-forward.log",
    )

    try:
        deployment = discover_deployment(args.deployment)
        job_status = deployment.get("status", {}).get("jobStatus", {})
        job_id = args.job_id or job_status.get("jobId")
        if not job_id:
            raise RuntimeError("Could not discover a running Flink job id")

        job_detail = discover_job_vertices(args.flink_url, job_id)
        vertex_ids = find_vertex_ids(job_detail)
        producer = discover_producer_config()

        flink_config = deployment["spec"]["flinkConfiguration"]
        if flink_config.get("job.autoscaler.a4s.enabled") == "true":
            autoscaler = "a4s"
        elif flink_config.get("job.autoscaler.justin.enabled") == "true":
            autoscaler = "justin"
        else:
            autoscaler = "ds2"
        output_dir = build_output_dir(autoscaler=autoscaler, producer=producer, explicit_name=args.name)

        start_ts = float(args.start) if args.start else max(float(job_status.get("startTime", 0)) / 1000 - 60, 0)
        end_ts = float(args.end) if args.end else utc_now().timestamp()
        if start_ts >= end_ts:
            raise RuntimeError("Invalid time window: start must be before end")

        prom = PrometheusClient(args.prom_url)
        query_specs = {
            "throughput_total_source": f'sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{{job_id="{job_id}",operator_name=~"Source.*"}})',
            "tm_count": 'count(kube_pod_status_phase{namespace="default",pod=~"flink-taskmanager-.*",phase="Running"})',
            "join_busy_pct": f'avg(flink_taskmanager_job_task_busyTimeMsPerSecond{{job_id="{job_id}",task_name=~"Join.*"}}) / 10',
            "join_cache_hit_rate_pct": (
                f'100 * sum(flink_taskmanager_job_task_operator_rocksdb_block_cache_hit{{job_id="{job_id}",task_id="{vertex_ids.get("join", "")}"}}) / '
                f'(sum(flink_taskmanager_job_task_operator_rocksdb_block_cache_hit{{job_id="{job_id}",task_id="{vertex_ids.get("join", "")}"}}) + '
                f'sum(flink_taskmanager_job_task_operator_rocksdb_block_cache_miss{{job_id="{job_id}",task_id="{vertex_ids.get("join", "")}"}})'
                f')'
            ),
        }
        # DS2: use Prometheus subtask count for join parallelism
        # Justin and A4S use the autoscaler ConfigMap for scaling history.
        if autoscaler == "ds2":
            query_specs["join_parallelism"] = f'count(sum by (subtask_index) (flink_taskmanager_job_task_busyTimeMsPerSecond{{job_id="{job_id}",task_name=~"Join.*"}}))'
        if vertex_ids.get("auction_source"):
            query_specs["kafka_lag_auction"] = (
                f'sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max{{job_id="{job_id}",task_id="{vertex_ids["auction_source"]}"}})'
            )
        if vertex_ids.get("bid_source"):
            query_specs["kafka_lag_bid"] = (
                f'sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max{{job_id="{job_id}",task_id="{vertex_ids["bid_source"]}"}})'
            )

        for name, expr in query_specs.items():
            log(f"Exporting {name}")
            result = prom.query_range(expr, start_ts, end_ts, args.step_seconds)
            write_series_csv(output_dir / f"{name}.csv", result)

        rescale_windows = parse_rescale_windows(args.deployment, start_ts=start_ts, end_ts=end_ts)

        if autoscaler in ("justin", "a4s"):
            # Parallelism and memory level both come from the autoscaler ConfigMap
            justin_para_rows, justin_mem_rows = parse_justin_scaling_history(
                args.deployment,
                vertex_ids.get("join"),
                start_ts=start_ts,
                end_ts=end_ts,
            )
            write_rows_csv(
                output_dir / "join_parallelism.csv",
                ["timestamp", "ts_iso", "series", "value"],
                [[r["timestamp"], r["ts_iso"], r["series"], r["value"]] for r in justin_para_rows],
            )
            write_rows_csv(
                output_dir / "memory_timeline.csv",
                ["timestamp", "ts_iso", "memory_bytes", "memory_mb", "memory_level"],
                [[r["timestamp"], r["ts_iso"], r["memory_bytes"], r["memory_mb"], r["memory_level"]] for r in justin_mem_rows],
            )
            scaling_events: list[dict[str, Any]] = []
        else:
            # DS2: parse operator logs for scaling events; no meaningful memory levels
            since_seconds = int(max(end_ts - start_ts + 600, 1800))
            scaling_events, memory_events = parse_operator_scaling_events(
                since_seconds=since_seconds,
                join_vertex_id=vertex_ids.get("join"),
                start_ts=start_ts,
                end_ts=end_ts,
            )
            memory_timeline = build_memory_timeline(
                start_ts=start_ts,
                end_ts=end_ts,
                memory_events=memory_events,
                autoscaler="ds2",
            )
            write_rows_csv(
                output_dir / "memory_timeline.csv",
                ["timestamp", "ts_iso", "memory_bytes", "memory_mb", "memory_level"],
                [
                    [p["timestamp"], p["ts_iso"], p["memory_bytes"], p["memory_mb"], p["memory_level"]]
                    for p in memory_timeline
                ],
            )

        write_rows_csv(
            output_dir / "scaling_events.csv",
            [
                "timestamp",
                "ts_iso",
                "vertex_id",
                "parallelism_old",
                "parallelism_new",
                "memory_old_bytes",
                "memory_new_bytes",
            ],
            [
                [
                    event["timestamp"],
                    event["ts_iso"],
                    event["vertex_id"],
                    event["parallelism_old"],
                    event["parallelism_new"],
                    event["memory_old_bytes"],
                    event["memory_new_bytes"],
                ]
                for event in scaling_events
            ],
        )
        write_rows_csv(
            output_dir / "rescale_windows.csv",
            ["start", "end", "start_iso", "end_iso", "label"],
            [
                [window["start"], window["end"], window["start_iso"], window["end_iso"], window["label"]]
                for window in rescale_windows
            ],
        )

        metadata = {
            "exported_at": utc_now().strftime(TIME_FMT),
            "deployment": args.deployment,
            "job_id": job_id,
            "job_name": job_detail.get("name"),
            "autoscaler": autoscaler,
            "producer": producer,
            "time_window": {
                "start": start_ts,
                "start_iso": to_iso(start_ts),
                "end": end_ts,
                "end_iso": to_iso(end_ts),
                "step_seconds": args.step_seconds,
            },
            "vertices": vertex_ids,
            "job_detail_excerpt": {
                "timestamps": job_detail.get("timestamps", {}),
                "plan_nodes": job_detail.get("plan", {}).get("nodes", []),
            },
        }
        (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        log(f"Wrote export to {output_dir}")
    finally:
        flink_pf.stop()
        prom_pf.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
