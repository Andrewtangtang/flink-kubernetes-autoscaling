#!/usr/bin/env python3
"""
Observe scaling decisions from the autoscaler ConfigMap.
Automatically detects whether Justin or DS2 is running and shows the
appropriate output (rich snapshots for Justin, parallelism changes for DS2).

Usage:
    ./observe-scaling.py                    # latest scaling decisions
    ./observe-scaling.py --follow           # watch continuously
    ./observe-scaling.py --since 8h         # only snapshots from the last 8 hours
    ./observe-scaling.py --deployment flink # select FlinkDeployment
    ./observe-scaling.py --configmap autoscaler-flink
    ./observe-scaling.py --json             # machine-readable JSON output
"""

from __future__ import annotations

import argparse
import base64
import gzip
import io
import json
import math
import os
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

SHARED_KUBECONFIG = Path("/etc/flink-kubernetes-autoscaling/kubeconfig")
if "KUBECONFIG" not in os.environ and os.access(SHARED_KUBECONFIG, os.R_OK):
    os.environ["KUBECONFIG"] = str(SHARED_KUBECONFIG)


@dataclass
class VertexInfo:
    vertex_id: str
    avg_throughput: float | str
    parallelism: int
    memory_level: int
    vertical_scaling: bool
    horizontal_scaling: bool
    avg_cache_hit_rate: float
    avg_state_latency: float


@dataclass
class ScalingSnapshot:
    timestamp: str
    period: int
    vertices: list[VertexInfo]


@dataclass
class ParallelismDecision:
    vertex_id: str
    current_parallelism: int
    recommended_parallelism: int
    lag: float | None
    window_average_capacity: float | None
    target_data_rate: float | None
    catch_up_data_rate: float | None
    target_processing_capacity: float | None
    raw_parallelism: float | None


@dataclass
class ParallelismDecisionEvent:
    timestamp: str
    vertices: list[ParallelismDecision]


DISPLAY_HEADER = (
    f"{'Vertex':>10}  {'CurP':>4}  {'CurMem':>6}  {'WinAvgCap':>12}  {'CacheHit':>8}  "
    f"{'P':>3}  {'MemLvl':>6}  {'HScale':>6}  {'VScale':>6}  {'StateLat':>8}"
)
DISPLAY_SEP = "-" * len(DISPLAY_HEADER)


def fmt_throughput(value) -> str:
    if isinstance(value, str):
        return value.rjust(12)
    return f"{value:>12.1f}"


def fmt_level(value: int | None, default: str = "?") -> str:
    if value is None:
        return default
    return str(value)


def find_previous_vertex(
    snapshots: list[ScalingSnapshot], snapshot_index: int, vertex_id: str
) -> VertexInfo | None:
    for index in range(snapshot_index - 1, -1, -1):
        for vertex in snapshots[index].vertices:
            if vertex.vertex_id == vertex_id:
                return vertex
    return None


def snapshot_requests_rescale(snapshot: ScalingSnapshot) -> bool:
    return any(
        vertex.horizontal_scaling or vertex.vertical_scaling
        for vertex in snapshot.vertices
    )


def consecutive_no_rescale_windows(
    snapshots: list[ScalingSnapshot], snapshot_index: int
) -> int:
    count = 0
    for index in range(snapshot_index, -1, -1):
        if snapshot_requests_rescale(snapshots[index]):
            break
        count += 1
    return count


def print_snapshot(
    snapshots: list[ScalingSnapshot],
    snapshot_index: int,
) -> None:
    snapshot = snapshots[snapshot_index]
    print(f"\n  ┌─ {snapshot.timestamp}  period={snapshot.period}")
    print(f"  │ {DISPLAY_HEADER}")
    print(f"  │ {DISPLAY_SEP}")
    for vertex in sorted(snapshot.vertices, key=lambda item: item.vertex_id):
        previous = find_previous_vertex(snapshots, snapshot_index, vertex.vertex_id)
        current_parallelism = 1 if previous is None else previous.parallelism
        current_memory_level = 0 if previous is None else previous.memory_level
        horizontal = "yes" if vertex.horizontal_scaling else "no"
        vertical = "yes" if vertex.vertical_scaling else "no"
        cache_hit = (
            f"{vertex.avg_cache_hit_rate:.3f}" if vertex.avg_cache_hit_rate > 0 else "-"
        )
        state_latency = (
            f"{vertex.avg_state_latency:.1f}" if vertex.avg_state_latency > 0 else "-"
        )
        print(
            f"  │ {vertex.vertex_id[:10]:>10}  "
            f"{fmt_level(current_parallelism):>4}  {fmt_level(current_memory_level):>6}  "
            f"{fmt_throughput(vertex.avg_throughput)}  "
            f"{cache_hit:>8}  "
            f"{vertex.parallelism:>3}  {vertex.memory_level:>6}  "
            f"{horizontal:>6}  {vertical:>6}  {state_latency:>8}"
        )
    print(f"  └{'─' * (len(DISPLAY_HEADER) + 1)}")
    no_rescale_windows = consecutive_no_rescale_windows(snapshots, snapshot_index)
    print(
        f"    Consecutive windows without rescaling: {no_rescale_windows}"
    )


def snapshot_to_dict(snapshot: ScalingSnapshot) -> dict:
    return {
        "timestamp": snapshot.timestamp,
        "period": snapshot.period,
        "vertices": [
            {
                "vertexId": vertex.vertex_id,
                "parallelism": vertex.parallelism,
                "memoryLevel": vertex.memory_level,
                "avgThroughput": vertex.avg_throughput,
                "horizontalScaling": vertex.horizontal_scaling,
                "verticalScaling": vertex.vertical_scaling,
                "avgCacheHitRate": vertex.avg_cache_hit_rate,
                "avgStateLatency": vertex.avg_state_latency,
            }
            for vertex in snapshot.vertices
        ],
    }


def run_kubectl(args: list[str]) -> str:
    try:
        return subprocess.check_output(
            ["kubectl", *args], text=True, stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as exc:
        details = exc.output.strip()
        if details:
            raise SystemExit(
                f"Failed to run kubectl {' '.join(args)}:\n{details}"
            ) from exc
        raise SystemExit(f"Failed to run kubectl {' '.join(args)}") from exc


def get_default_deployment_name(explicit: str | None = None) -> str:
    if explicit:
        return explicit

    try:
        output = run_kubectl(["get", "flinkdeployment", "-o", "json"])
        items = json.loads(output).get("items", [])
        names = [item.get("metadata", {}).get("name") for item in items]
        names = [name for name in names if name]
        if "flink" in names:
            return "flink"
        if len(names) == 1:
            return names[0]
    except SystemExit:
        pass

    return "flink"


def parse_duration_expr(expr: str) -> timedelta:
    total = timedelta()
    matches = list(re.finditer(r"(\d+)([smhd])", expr.strip()))
    if not matches or "".join(match.group(0) for match in matches) != expr.strip():
        raise ValueError(f"Unsupported duration expression: {expr}")

    for match in matches:
        value = int(match.group(1))
        unit = match.group(2)
        if unit == "s":
            total += timedelta(seconds=value)
        elif unit == "m":
            total += timedelta(minutes=value)
        elif unit == "h":
            total += timedelta(hours=value)
        elif unit == "d":
            total += timedelta(days=value)

    return total


def parse_iso_datetime(value: str) -> datetime | None:
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_timestamp(dt: datetime) -> str:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]


def normalize_timestamp(value) -> str:
    if isinstance(value, datetime):
        return format_timestamp(value)

    if isinstance(value, str):
        parsed = parse_iso_datetime(value)
        if parsed:
            return format_timestamp(parsed)
        return value

    return str(value)


def get_cutoff_time(since: str | None) -> datetime | None:
    if not since:
        return None
    return datetime.now(timezone.utc) - parse_duration_expr(since)


def parse_history_timestamp_key(key) -> datetime | None:
    if isinstance(key, datetime):
        if key.tzinfo is None:
            return key.replace(tzinfo=timezone.utc)
        return key.astimezone(timezone.utc)
    if isinstance(key, str):
        return parse_iso_datetime(key)
    return None


def to_float_or_str(value):
    if value is None:
        return "-"
    if isinstance(value, (int, float)):
        return parse_number(str(value))
    return parse_number(str(value))


def parse_number(value: str) -> float | str:
    value = value.strip()
    if value == "Infinity":
        return "∞"
    if value == "-Infinity":
        return "-∞"
    if value == "NaN":
        return "NaN"
    try:
        parsed = float(value)
        return int(parsed) if parsed == int(parsed) and abs(parsed) < 1e15 else parsed
    except ValueError:
        return value


def find_autoscaler_configmap(
    deployment: str, configmap_name: str | None = None
) -> tuple[str, dict] | None:
    if configmap_name:
        output = run_kubectl(["get", "configmap", configmap_name, "-o", "json"])
        return configmap_name, json.loads(output)

    output = run_kubectl(
        [
            "get",
            "configmap",
            "-l",
            f"component=autoscaler,app={deployment}",
            "-o",
            "json",
        ]
    )
    items = json.loads(output).get("items", [])
    if items:
        items.sort(
            key=lambda item: item.get("metadata", {}).get("creationTimestamp", "")
        )
        item = items[-1]
        return item.get("metadata", {}).get("name", f"autoscaler-{deployment}"), item

    guessed_name = f"autoscaler-{deployment}"
    try:
        output = run_kubectl(["get", "configmap", guessed_name, "-o", "json"])
        return guessed_name, json.loads(output)
    except SystemExit:
        return None


def decode_autoscaler_state(value: str):
    raw = gzip.decompress(base64.b64decode(value))
    return yaml.safe_load(io.StringIO(raw.decode("utf-8")))


def fetch_configmap_rich_history(
    deployment: str,
    since: str | None = None,
    configmap_name: str | None = None,
) -> tuple[str, list[ScalingSnapshot]]:
    found = find_autoscaler_configmap(deployment, configmap_name)
    if not found:
        return "", []

    cm_name, configmap = found
    data = configmap.get("data") or {}
    rich_history = data.get("scalingConfigHistory")
    if not rich_history:
        return cm_name, []

    payload = decode_autoscaler_state(rich_history) or {}
    cutoff = get_cutoff_time(since)
    snapshots: list[ScalingSnapshot] = []

    if not isinstance(payload, dict):
        return cm_name, []

    for timestamp_key, snapshot in payload.items():
        timestamp = parse_history_timestamp_key(timestamp_key)
        if cutoff and timestamp and timestamp < cutoff:
            continue
        if not isinstance(snapshot, dict):
            continue

        vertices = []
        scaling = snapshot.get("scaling") or {}
        for vertex_id, info in scaling.items():
            if not isinstance(info, dict):
                continue
            vertices.append(
                VertexInfo(
                    vertex_id=str(vertex_id),
                    avg_throughput=to_float_or_str(info.get("avgThroughput")),
                    parallelism=int(info.get("parallelism", -1)),
                    memory_level=int(info.get("memoryLevel", -1)),
                    vertical_scaling=bool(info.get("verticalScaling", False)),
                    horizontal_scaling=bool(info.get("horizontalScaling", False)),
                    avg_cache_hit_rate=float(info.get("avgCacheHitRate", 0.0) or 0.0),
                    avg_state_latency=float(info.get("avgStateLatency", 0.0) or 0.0),
                )
            )

        if vertices:
            snapshots.append(
                ScalingSnapshot(
                    timestamp=normalize_timestamp(timestamp_key),
                    period=int(snapshot.get("period", 0)),
                    vertices=vertices,
                )
            )

    snapshots.sort(key=lambda item: (item.timestamp, item.period))
    return cm_name, snapshots


def detect_autoscaler_algo(deployment: str) -> str:
    """Return 'justin' or 'ds2' based on the FlinkDeployment config."""
    try:
        output = run_kubectl(["get", "flinkdeployment", deployment, "-o", "json"])
        spec = json.loads(output)
        flink_config = spec.get("spec", {}).get("flinkConfiguration", {})
        justin_flag = flink_config.get("job.autoscaler.justin.enabled", "false")
        if justin_flag.strip().lower() == "true":
            return "justin"
    except SystemExit:
        pass
    return "ds2"


DECISION_DISPLAY_HEADER = (
    f"{'Vertex':>10}  {'CurP':>4}  {'WinAvg':>10}  {'TargetRate':>10}  "
    f"{'CatchUp':>10}  {'TargetCap':>10}  {'RawP':>6}  {'RecP':>4}  {'Lag':>10}"
)
DECISION_DISPLAY_SEP = "-" * len(DECISION_DISPLAY_HEADER)


def fmt_metric(value: float | None, width: int = 10) -> str:
    if value is None or (isinstance(value, float) and (value != value)):  # NaN check
        return "-".rjust(width)
    return f"{value:>{width}.1f}"


def fmt_raw_parallelism(value: float | None) -> str:
    if value is None:
        return "-".rjust(6)
    return f"{value:>6.2f}"


def print_parallelism_decision_event(
    events: list[ParallelismDecisionEvent],
    event_index: int,
    title: str | None = None,
) -> None:
    event = events[event_index]
    heading = f"  {title}" if title else ""
    print(f"\n  ┌─ {event.timestamp}{heading}")
    print(f"  │ {DECISION_DISPLAY_HEADER}")
    print(f"  │ {DECISION_DISPLAY_SEP}")
    for vertex in sorted(event.vertices, key=lambda v: v.vertex_id):
        print(
            f"  │ {vertex.vertex_id[:10]:>10}  "
            f"{vertex.current_parallelism:>4}  "
            f"{fmt_metric(vertex.window_average_capacity)}  "
            f"{fmt_metric(vertex.target_data_rate)}  "
            f"{fmt_metric(vertex.catch_up_data_rate)}  "
            f"{fmt_metric(vertex.target_processing_capacity)}  "
            f"{fmt_raw_parallelism(vertex.raw_parallelism)}  "
            f"{vertex.recommended_parallelism:>4}  "
            f"{fmt_metric(vertex.lag)}"
        )
    print(f"  └{'─' * (len(DECISION_DISPLAY_HEADER) + 1)}")


def parallelism_decision_event_to_dict(event: ParallelismDecisionEvent) -> dict:
    return {
        "timestamp": event.timestamp,
        "vertices": [
            {
                "vertexId": v.vertex_id,
                "currentParallelism": v.current_parallelism,
                "recommendedParallelism": v.recommended_parallelism,
                "lag": v.lag,
                "windowAverageProcessingCapacity": v.window_average_capacity,
                "targetDataRate": v.target_data_rate,
                "catchUpDataRate": v.catch_up_data_rate,
                "targetProcessingCapacity": v.target_processing_capacity,
                "rawParallelism": v.raw_parallelism,
            }
            for v in event.vertices
        ],
    }


def metric_value(
    metrics: dict,
    metric_name: str,
    field: str,
) -> float | None:
    entry = metrics.get(metric_name) or metrics.get(metric_name.lower())
    if not isinstance(entry, dict):
        return None
    value = entry.get(field)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def compute_raw_parallelism(
    current_parallelism: int,
    window_average_capacity: float | None,
    target_processing_capacity: float | None,
) -> float | None:
    if (
        current_parallelism <= 0
        or window_average_capacity is None
        or window_average_capacity <= 0
        or target_processing_capacity is None
    ):
        return None
    return current_parallelism * target_processing_capacity / window_average_capacity


def fetch_parallelism_decision_history(
    deployment: str,
    since: str | None = None,
    configmap_name: str | None = None,
) -> tuple[str, list[ParallelismDecisionEvent]]:
    found = find_autoscaler_configmap(deployment, configmap_name)
    if not found:
        return "", []

    cm_name, configmap = found
    data = configmap.get("data") or {}
    encoded = data.get("scalingHistory")
    if not encoded:
        return cm_name, []

    payload = decode_autoscaler_state(encoded) or {}
    if not isinstance(payload, dict):
        return cm_name, []

    cutoff = get_cutoff_time(since)

    # payload is {vertex_id: {timestamp: ScalingSummary}}. Pivot to one event
    # per decision timestamp so all affected vertices are displayed together.
    events_by_ts: dict[str, list[ParallelismDecision]] = {}
    for vertex_id, ts_map in payload.items():
        if not isinstance(ts_map, dict):
            continue
        for ts_key, summary in ts_map.items():
            if not isinstance(summary, dict):
                continue
            ts = parse_history_timestamp_key(ts_key)
            if cutoff and ts and ts < cutoff:
                continue
            ts_str = normalize_timestamp(ts_key)

            metrics = summary.get("metrics") or {}
            window_average_capacity = metric_value(
                metrics, "TRUE_PROCESSING_RATE", "average"
            )
            target_processing_capacity = metric_value(
                metrics, "EXPECTED_PROCESSING_RATE", "current"
            )
            current_parallelism = int(summary.get("currentParallelism", -1))

            vertex_event = ParallelismDecision(
                vertex_id=str(vertex_id),
                current_parallelism=current_parallelism,
                recommended_parallelism=int(summary.get("newParallelism", -1)),
                lag=metric_value(metrics, "LAG", "current"),
                window_average_capacity=window_average_capacity,
                target_data_rate=metric_value(metrics, "TARGET_DATA_RATE", "average"),
                catch_up_data_rate=metric_value(
                    metrics, "CATCH_UP_DATA_RATE", "current"
                ),
                target_processing_capacity=target_processing_capacity,
                raw_parallelism=compute_raw_parallelism(
                    current_parallelism,
                    window_average_capacity,
                    target_processing_capacity,
                ),
            )
            events_by_ts.setdefault(ts_str, []).append(vertex_event)

    events = [
        ParallelismDecisionEvent(timestamp=ts, vertices=verts)
        for ts, verts in events_by_ts.items()
    ]
    events.sort(key=lambda e: e.timestamp)
    return cm_name, events


def find_parallelism_decision_event(
    events: list[ParallelismDecisionEvent],
    timestamp: str,
) -> int | None:
    for index, event in enumerate(events):
        if event.timestamp == timestamp:
            return index
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--since",
        help="Only return snapshots newer than a relative duration such as 30m, 4h, or 2d",
    )
    parser.add_argument(
        "--deployment",
        help="FlinkDeployment name used to find the autoscaler ConfigMap",
    )
    parser.add_argument("--configmap", help="Autoscaler ConfigMap name override")
    parser.add_argument(
        "--follow",
        "-f",
        action="store_true",
        help="Continuously poll for new snapshots",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Poll interval in seconds for --follow (default: 30)",
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    deployment = get_default_deployment_name(args.deployment)
    algo = detect_autoscaler_algo(deployment)
    seen_keys: set[str] = set()
    printed_header = False

    while True:
        if algo == "justin":
            configmap_name, snapshots = fetch_configmap_rich_history(
                deployment,
                since=args.since,
                configmap_name=args.configmap,
            )
            _, parallelism_decisions = fetch_parallelism_decision_history(
                deployment,
                since=args.since,
                configmap_name=args.configmap,
            )

            new_snapshots = []
            for snapshot in snapshots:
                key = f"{snapshot.timestamp}|{snapshot.period}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    new_snapshots.append(snapshot)

            if args.json:
                payload = {
                    "deployment": deployment,
                    "algorithm": "justin",
                    "configMap": configmap_name or None,
                    "richSnapshots": [
                        {
                            **snapshot_to_dict(snapshot),
                            "consecutiveNoRescaleWindows": consecutive_no_rescale_windows(
                                snapshots,
                                next(
                                    index
                                    for index, candidate in enumerate(snapshots)
                                    if candidate.timestamp == snapshot.timestamp
                                    and candidate.period == snapshot.period
                                ),
                            ),
                        }
                        for snapshot in new_snapshots
                    ],
                    "parallelismCalculations": [
                        parallelism_decision_event_to_dict(event)
                        for event in parallelism_decisions
                        if event.timestamp
                        in {snapshot.timestamp for snapshot in new_snapshots}
                    ],
                }
                print(json.dumps(payload, indent=2))
            else:
                if not printed_header:
                    print(f"Deployment: {deployment}")
                    print(f"Algorithm: justin")
                    if configmap_name:
                        print(f"ConfigMap: {configmap_name}")
                    print(
                        "Decision metrics: WinAvgCap is the metrics-window average "
                        "processing capacity; RawP = CurP × TargetCap / WinAvgCap; "
                        "RecP is the rounded and key-group-aligned recommendation."
                    )
                    printed_header = True

                for snapshot in new_snapshots:
                    snapshot_index = next(
                        index
                        for index, candidate in enumerate(snapshots)
                        if candidate.timestamp == snapshot.timestamp
                        and candidate.period == snapshot.period
                    )
                    print_snapshot(snapshots, snapshot_index)
                    decision_index = find_parallelism_decision_event(
                        parallelism_decisions, snapshot.timestamp
                    )
                    if decision_index is not None:
                        print_parallelism_decision_event(
                            parallelism_decisions,
                            decision_index,
                            title="DS2 base calculation before Justin memory policy",
                        )

                if not new_snapshots and not args.follow:
                    print("No rich scaling snapshots found in autoscaler ConfigMap.")

        else:
            configmap_name, decision_events = fetch_parallelism_decision_history(
                deployment,
                since=args.since,
                configmap_name=args.configmap,
            )

            new_events = []
            for event in decision_events:
                key = event.timestamp
                if key not in seen_keys:
                    seen_keys.add(key)
                    new_events.append(event)

            if args.json:
                payload = {
                    "deployment": deployment,
                    "algorithm": "ds2",
                    "configMap": configmap_name or None,
                    "scalingEvents": [
                        parallelism_decision_event_to_dict(e) for e in new_events
                    ],
                }
                print(json.dumps(payload, indent=2))
            else:
                if not printed_header:
                    print(f"Deployment: {deployment}")
                    print(f"Algorithm: ds2")
                    if configmap_name:
                        print(f"ConfigMap: {configmap_name}")
                    print(
                        "Decision metrics: WinAvg is the metrics-window average "
                        "processing capacity; RawP = CurP × TargetCap / WinAvg; "
                        "RecP is the rounded and key-group-aligned recommendation."
                    )
                    printed_header = True

                for event in new_events:
                    event_index = next(
                        i
                        for i, e in enumerate(decision_events)
                        if e.timestamp == event.timestamp
                    )
                    print_parallelism_decision_event(decision_events, event_index)

                if not new_events and not args.follow:
                    print("No DS2 scaling events found in autoscaler ConfigMap.")

        if not args.follow:
            break

        time.sleep(args.interval)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
