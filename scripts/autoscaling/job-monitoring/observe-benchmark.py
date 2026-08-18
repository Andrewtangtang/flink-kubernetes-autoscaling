#!/usr/bin/env python3
"""Observe one checkpoint-aware benchmark and classify capacity stability.

This combines the live Flink/Prometheus metrics, checkpoint-rescale transaction,
autoscaler decision, producer, and Kafka-lag views in one terminal. It is a
read-only observer and does not alter autoscaler timing or trigger rescaling.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
SHARED_KUBECONFIG = Path("/etc/flink-kubernetes-autoscaling/kubeconfig")
if "KUBECONFIG" not in os.environ and os.access(SHARED_KUBECONFIG, os.R_OK):
    os.environ["KUBECONFIG"] = str(SHARED_KUBECONFIG)


def load_sibling(module_name: str, filename: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_DIR / filename)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


METRICS = load_sibling("observe_flink_metrics", "observe-flink-metrics.py")


@dataclass(frozen=True)
class TransactionInfo:
    phase: str = "IDLE"
    decision_timestamp: str | None = None
    restored_running_timestamp: int | None = None
    checkpoint_id: str = "-"
    restart_millis: str = "-"
    error: str = "-"

    @property
    def key(self) -> tuple[str | None, int | None]:
        return self.decision_timestamp, self.restored_running_timestamp


@dataclass(frozen=True)
class ClusterState:
    transaction: TransactionInfo
    algorithm: str
    resource_fingerprint: str
    decision_summary: list[str]


@dataclass(frozen=True)
class ProducerState:
    active: bool | None
    rate: float | None
    job_name: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class StabilitySample:
    timestamp: float
    producer_active: bool | None
    producer_rate: float | None
    kafka_lag: float | None
    resource_fingerprint: str
    transaction_key: tuple[str | None, int | None]


@dataclass(frozen=True)
class WindowResult:
    index: int
    producer_rate: float | None
    lag_start: float | None
    lag_end: float | None
    lag_slope: float | None
    producer_ok: bool
    lag_ok: bool
    resources_ok: bool
    decision_ok: bool
    coverage_ok: bool

    @property
    def passed(self) -> bool:
        return (
            self.producer_ok
            and self.lag_ok
            and self.resources_ok
            and self.decision_ok
            and self.coverage_ok
            and self.lag_slope is not None
        )


@dataclass
class StabilityTracker:
    target_rate: float
    stabilization_seconds: float = 60.0
    window_seconds: float = 120.0
    required_windows: int = 3
    input_tolerance: float = 0.05
    lag_growth_tolerance: float = 0.01
    sample_interval: float = 5.0
    transaction_key: tuple[str | None, int | None] | None = None
    stable_start: float | None = None
    baseline_resource: str | None = None
    samples: list[StabilitySample] = field(default_factory=list)
    windows: list[WindowResult] = field(default_factory=list)
    next_window_index: int = 0
    pass_streak: int = 0
    accepted_status: str | None = None
    accepted_at: float | None = None

    def reset(self) -> None:
        self.transaction_key = None
        self.stable_start = None
        self.baseline_resource = None
        self.samples.clear()
        self.windows.clear()
        self.next_window_index = 0
        self.pass_streak = 0
        self.accepted_status = None
        self.accepted_at = None

    def observe(
        self,
        now: float,
        job_state: str | None,
        transaction: TransactionInfo,
        producer: ProducerState,
        kafka_lag: float | None,
        resource_fingerprint: str,
    ) -> None:
        if (
            transaction.phase != "COMPLETED"
            or transaction.restored_running_timestamp is None
        ):
            self.reset()
            return

        if transaction.key != self.transaction_key:
            self.reset()
            self.transaction_key = transaction.key
            restored_at = transaction.restored_running_timestamp / 1000.0
            # Do not manufacture historical windows when the observer starts late.
            self.stable_start = max(
                restored_at + self.stabilization_seconds,
                now,
            )
            self.baseline_resource = resource_fingerprint

        assert self.stable_start is not None
        if self.accepted_status is not None:
            return
        if job_state != "RUNNING" or now < self.stable_start:
            return

        self.samples.append(
            StabilitySample(
                timestamp=now,
                producer_active=producer.active,
                producer_rate=producer.rate,
                kafka_lag=kafka_lag,
                resource_fingerprint=resource_fingerprint,
                transaction_key=transaction.key,
            )
        )
        self._close_complete_windows(now)
        self._accept_if_complete(now, kafka_lag)

    def _accept_if_complete(self, now: float, kafka_lag: float | None) -> None:
        if self.pass_streak < self.required_windows or kafka_lag is None:
            return
        caught_up_limit = self.target_rate * 2.0
        slope_tolerance = self.target_rate * self.lag_growth_tolerance
        latest_slope = self.windows[-1].lag_slope
        if (
            kafka_lag <= caught_up_limit
            and latest_slope is not None
            and abs(latest_slope) <= slope_tolerance
        ):
            accepted_status = "FULLY_CAUGHT_UP"
        elif latest_slope is not None and latest_slope < -slope_tolerance:
            accepted_status = "CAPACITY_STABLE_CATCHING_UP"
        else:
            accepted_status = "CAPACITY_STABLE_LAG_FLAT"
        self.accepted_status = accepted_status
        self.accepted_at = now

    def _close_complete_windows(self, now: float) -> None:
        assert self.stable_start is not None
        while (
            now
            >= self.stable_start + (self.next_window_index + 1) * self.window_seconds
        ):
            start = self.stable_start + self.next_window_index * self.window_seconds
            end = start + self.window_seconds
            window_samples = [
                sample for sample in self.samples if start <= sample.timestamp < end
            ]
            result = self._assess_window(
                self.next_window_index, start, end, window_samples
            )
            self.windows.append(result)
            self.pass_streak = self.pass_streak + 1 if result.passed else 0
            self.next_window_index += 1

        keep_after = (
            self.stable_start
            + max(0, self.next_window_index - self.required_windows)
            * self.window_seconds
        )
        self.samples = [
            sample for sample in self.samples if sample.timestamp >= keep_after
        ]

    def _assess_window(
        self,
        index: int,
        start: float,
        end: float,
        samples: list[StabilitySample],
    ) -> WindowResult:
        max_gap = max(self.sample_interval * 3, 15.0)
        coverage_ok = bool(samples) and (
            samples[0].timestamp <= start + max_gap
            and samples[-1].timestamp >= end - max_gap
        )
        rates = [
            sample.producer_rate
            for sample in samples
            if sample.producer_rate is not None
        ]
        average_rate = sum(rates) / len(rates) if rates else None
        lower = self.target_rate * (1.0 - self.input_tolerance)
        upper = self.target_rate * (1.0 + self.input_tolerance)
        producer_ok = (
            bool(samples)
            and all(sample.producer_active is True for sample in samples)
            and len(rates) == len(samples)
            and average_rate is not None
            and lower <= average_rate <= upper
        )
        resources_ok = bool(samples) and all(
            sample.resource_fingerprint == self.baseline_resource for sample in samples
        )
        decision_ok = bool(samples) and all(
            sample.transaction_key == self.transaction_key for sample in samples
        )

        lag_samples = [sample for sample in samples if sample.kafka_lag is not None]
        lag_start = lag_samples[0].kafka_lag if lag_samples else None
        lag_end = lag_samples[-1].kafka_lag if lag_samples else None
        lag_slope = None
        lag_ok = False
        if len(lag_samples) >= 2:
            elapsed = lag_samples[-1].timestamp - lag_samples[0].timestamp
            if elapsed > 0:
                raw_slope = (
                    lag_samples[-1].kafka_lag - lag_samples[0].kafka_lag
                ) / elapsed
                tolerance = self.target_rate * self.lag_growth_tolerance
                lag_slope = raw_slope
                lag_coverage_ok = (
                    lag_samples[0].timestamp <= start + max_gap
                    and lag_samples[-1].timestamp >= end - max_gap
                )
                lag_ok = lag_coverage_ok and raw_slope <= tolerance

        return WindowResult(
            index=index,
            producer_rate=average_rate,
            lag_start=lag_start,
            lag_end=lag_end,
            lag_slope=lag_slope,
            producer_ok=producer_ok,
            lag_ok=lag_ok,
            resources_ok=resources_ok,
            decision_ok=decision_ok,
            coverage_ok=coverage_ok,
        )

    def status(
        self,
        now: float,
        job_state: str | None,
        transaction: TransactionInfo,
        producer: ProducerState,
        kafka_lag: float | None,
    ) -> str:
        if transaction.error not in {"", "-", "null", "None"}:
            return "FAILED"
        if transaction.phase == "IDLE":
            return "WAITING_FOR_FIRST_RESCALE"
        if transaction.phase != "COMPLETED" or job_state != "RUNNING":
            return "RESCALING"
        if self.accepted_status is not None:
            return self.accepted_status
        if producer.active is False:
            return "INCONCLUSIVE_PRODUCER_STOPPED"
        if producer.active is None or producer.rate is None:
            return "WAITING_FOR_PRODUCER_METRICS"
        if kafka_lag is None:
            return "WAITING_FOR_LAG_METRICS"
        if self.stable_start is None or now < self.stable_start:
            return "STABILIZING"
        if self.pass_streak < self.required_windows:
            return "CHECKING"
        return "CHECKING"


def get_json(url: str, timeout: float = 5.0) -> Any:
    with urlopen(url, timeout=timeout) as response:
        return json.load(response)


def kubectl_json(*args: str) -> dict[str, Any]:
    output = subprocess.check_output(
        ["kubectl", *args, "-o", "json"],
        text=True,
        stderr=subprocess.STDOUT,
    )
    return json.loads(output)


def parse_transaction(raw: str, annotation_phase: str = "IDLE") -> TransactionInfo:
    payload = yaml.safe_load(raw) if raw else {}
    if not isinstance(payload, dict):
        payload = {}
    restored = payload.get("restoredRunningTimestamp")
    try:
        restored_timestamp = int(restored) if restored is not None else None
    except (TypeError, ValueError):
        restored_timestamp = None
    error = payload.get("error")
    return TransactionInfo(
        phase=str(payload.get("phase") or annotation_phase),
        decision_timestamp=(
            str(payload["decisionTimestamp"])
            if payload.get("decisionTimestamp") is not None
            else None
        ),
        restored_running_timestamp=restored_timestamp,
        checkpoint_id=str(payload.get("completedCheckpointId") or "-"),
        restart_millis=str(payload.get("observedRestartDurationMillis") or "-"),
        error=str(error) if error not in {None, ""} else "-",
    )


def timestamp_sort_key(value: Any) -> float:
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def decode_history(value: str | None) -> Any:
    if not value:
        return None
    import base64
    import gzip

    return yaml.safe_load(gzip.decompress(base64.b64decode(value)).decode("utf-8"))


def nested_metric(metrics: Any, name: str, field_name: str) -> Any:
    if not isinstance(metrics, dict):
        return "-"
    entry = metrics.get(name) or metrics.get(name.lower())
    return entry.get(field_name, "-") if isinstance(entry, dict) else "-"


def latest_decision_summary(data: dict[str, str], algorithm: str) -> list[str]:
    if algorithm == "justin":
        payload = decode_history(data.get("scalingConfigHistory"))
        if not isinstance(payload, dict) or not payload:
            return ["No Justin decision snapshot yet"]
        timestamp, snapshot = max(
            payload.items(), key=lambda item: timestamp_sort_key(item[0])
        )
        scaling = snapshot.get("scaling", {}) if isinstance(snapshot, dict) else {}
        lines = [f"{timestamp} period={snapshot.get('period', '?')}"]
        for vertex_id, info in sorted(scaling.items()):
            if not isinstance(info, dict):
                continue
            lines.append(
                f"{str(vertex_id)[:10]} P={info.get('parallelism', '?')} "
                f"M={info.get('memoryLevel', '?')} "
                f"WinAvgCap={info.get('avgThroughput', '-')} "
                f"CacheHit={info.get('avgCacheHitRate', '-')} "
                f"H={'yes' if info.get('horizontalScaling') else 'no'} "
                f"V={'yes' if info.get('verticalScaling') else 'no'} "
                f"StateLat={info.get('avgStateLatency', '-')}"
            )
        return lines

    payload = decode_history(data.get("scalingHistory"))
    if not isinstance(payload, dict) or not payload:
        return ["No DS2 scaling decision yet"]
    latest_timestamp: Any = None
    latest: list[str] = []
    for vertex_id, histories in payload.items():
        if not isinstance(histories, dict):
            continue
        for timestamp, summary in histories.items():
            if latest_timestamp is None or timestamp_sort_key(
                timestamp
            ) > timestamp_sort_key(latest_timestamp):
                latest_timestamp = timestamp
                latest = []
            if timestamp_sort_key(timestamp) == timestamp_sort_key(latest_timestamp):
                metrics = (
                    summary.get("metrics", {}) if isinstance(summary, dict) else {}
                )
                latest.append(
                    f"{str(vertex_id)[:10]} P={summary.get('currentParallelism', '?')}"
                    f"->{summary.get('newParallelism', '?')} "
                    f"WinAvgCap={nested_metric(metrics, 'TRUE_PROCESSING_RATE', 'average')} "
                    f"TargetRate={nested_metric(metrics, 'TARGET_DATA_RATE', 'average')} "
                    f"CatchUp={nested_metric(metrics, 'CATCH_UP_DATA_RATE', 'current')} "
                    f"TargetCap={nested_metric(metrics, 'EXPECTED_PROCESSING_RATE', 'current')} "
                    f"Lag={nested_metric(metrics, 'LAG', 'current')}"
                )
    return ([str(latest_timestamp)] if latest_timestamp is not None else []) + latest


def cluster_state(deployment: str, namespace: str, configmap_name: str) -> ClusterState:
    resource = kubectl_json(
        "get", "flinkdeployment", deployment, "--namespace", namespace
    )
    config = resource.get("spec", {}).get("flinkConfiguration", {})
    annotations = resource.get("metadata", {}).get("annotations", {})
    annotation_phase = annotations.get(
        "autoscaling.flink.apache.org/checkpoint-rescale-phase", "IDLE"
    )
    configmap = kubectl_json(
        "get", "configmap", configmap_name, "--namespace", namespace
    )
    data = configmap.get("data", {}) or {}
    transaction = parse_transaction(
        data.get("checkpointRescaleTransaction", ""), annotation_phase
    )
    algorithm = (
        "justin"
        if str(config.get("job.autoscaler.justin.enabled", "false")).lower() == "true"
        else "ds2"
    )
    resource_fingerprint = "|".join(
        [
            str(config.get("pipeline.jobvertex-parallelism-overrides", "")),
            str(config.get("pipeline.jobvertex-resourceprofile-overrides", "")),
        ]
    )
    try:
        decision_summary = latest_decision_summary(data, algorithm)
    except Exception as exc:
        decision_summary = [f"Decision history unavailable: {exc}"]
    return ClusterState(
        transaction=transaction,
        algorithm=algorithm,
        resource_fingerprint=resource_fingerprint,
        decision_summary=decision_summary,
    )


def metric_sum(base_url: str, job_id: str, vertex_id: str) -> float | None:
    query = urlencode({"get": "numRecordsOutPerSecond", "agg": "sum"})
    values = get_json(
        f"{base_url.rstrip('/')}/jobs/{job_id}/vertices/{vertex_id}/metrics?{query}"
    )
    if not isinstance(values, list):
        return None
    for metric in values:
        try:
            value = float(metric["sum"])
        except (KeyError, TypeError, ValueError):
            continue
        if math.isfinite(value):
            return value
    return None


def producer_state(base_url: str) -> ProducerState:
    try:
        overview = get_json(f"{base_url.rstrip('/')}/jobs/overview")
        jobs = [
            job for job in overview.get("jobs", []) if job.get("state") == "RUNNING"
        ]
        if not jobs:
            return ProducerState(
                active=False, rate=None, error="no RUNNING producer job"
            )
        jobs.sort(key=lambda job: job.get("start-time", -1), reverse=True)
        job = jobs[0]
        details = get_json(f"{base_url.rstrip('/')}/jobs/{job['jid']}")
        source_vertices = [
            vertex
            for vertex in details.get("vertices", [])
            if str(vertex.get("name", "")).startswith("Source:")
        ]
        rates = [
            metric_sum(base_url, str(job["jid"]), str(vertex["id"]))
            for vertex in source_vertices
            if vertex.get("id")
        ]
        valid_rates = [rate for rate in rates if rate is not None]
        return ProducerState(
            active=True,
            rate=sum(valid_rates) if valid_rates else None,
            job_name=str(job.get("name", job["jid"])),
        )
    except Exception as exc:
        return ProducerState(active=None, rate=None, error=str(exc))


def kafka_lag(prometheus: Any, job_id: str) -> float | None:
    expression = (
        "sum("
        "flink_taskmanager_job_task_operator_"
        "KafkaSourceReader_KafkaConsumer_records_lag_max"
        f'{{job_id="{METRICS.label_value(job_id)}"}}'
        ")"
    )
    samples = prometheus.query(expression)
    values = [METRICS.sample_value(sample) for sample in samples]
    valid = [value for value in values if value is not None]
    return sum(valid) if valid else None


def format_rate(value: float | None) -> str:
    return METRICS.format_rate(value)


def format_window(window: WindowResult) -> str:
    checks = [
        f"producer={'PASS' if window.producer_ok else 'FAIL'}",
        f"lag={'PASS' if window.lag_ok else 'FAIL'}",
        f"P/M={'PASS' if window.resources_ok else 'FAIL'}",
        f"decision={'PASS' if window.decision_ok else 'FAIL'}",
        f"coverage={'PASS' if window.coverage_ok else 'FAIL'}",
    ]
    lag = "-" if window.lag_slope is None else f"{window.lag_slope:+.1f}/s"
    return (
        f"window {window.index + 1}: {'PASS' if window.passed else 'FAIL'} "
        f"input={format_rate(window.producer_rate)}/s lag_slope={lag} "
        + " ".join(checks)
    )


def render(
    snapshot: Any | None,
    cluster: ClusterState | None,
    producer: ProducerState,
    lag: float | None,
    tracker: StabilityTracker,
    now: float,
    metrics_error: str | None,
    cluster_error: str | None,
    summary_only: bool,
) -> str:
    lines: list[str] = []
    if snapshot is not None:
        lines.append(METRICS.render_snapshot(snapshot, summary_only))
    else:
        lines.append(f"Live metrics unavailable: {metrics_error or '-'}")

    lines.extend(["", "CHECKPOINT-AWARE CAPACITY STABILITY"])
    if cluster is None:
        lines.append(f"Cluster state unavailable: {cluster_error or '-'}")
        return "\n".join(lines)

    transaction = cluster.transaction
    job_state = snapshot.job_state if snapshot is not None else None
    status = tracker.status(now, job_state, transaction, producer, lag)
    elapsed = 0.0 if tracker.stable_start is None else now - tracker.stable_start
    stabilization_remaining = max(0.0, -elapsed)
    lines.extend(
        [
            f"Status: {status}",
            f"Algorithm: {cluster.algorithm}  Job: {job_state or '-'}  "
            f"Transaction: {transaction.phase}",
            f"Checkpoint: {transaction.checkpoint_id}  restart_ms={transaction.restart_millis} "
            f"error={transaction.error}",
            f"Producer: "
            f"{'RUNNING' if producer.active is True else 'STOPPED' if producer.active is False else 'UNKNOWN'} "
            f"rate={format_rate(producer.rate)}/s target={format_rate(tracker.target_rate)}/s "
            f"error={producer.error or '-'}",
            f"Kafka lag: {format_rate(lag)} events",
            f"Stabilization remaining: {stabilization_remaining:.0f}s  "
            f"successful windows: {tracker.pass_streak}/{tracker.required_windows}",
        ]
    )
    if tracker.accepted_status is not None:
        accepted_at = datetime.fromtimestamp(
            tracker.accepted_at or now, timezone.utc
        ).isoformat(timespec="seconds")
        lines.extend(
            [
                "",
                "*** EXPERIMENT END CONDITION REACHED ***",
                f"Accepted: {tracker.accepted_status} at {accepted_at}",
                "Save evidence before stopping the producer and Flink job.",
            ]
        )
    for window in tracker.windows[-tracker.required_windows :]:
        lines.append(format_window(window))
    lines.extend(["", "LATEST AUTOSCALER DECISION"])
    lines.extend(f"  {line}" for line in cluster.decision_summary)
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment", default="flink")
    parser.add_argument("--configmap", default="autoscaler-flink")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--flink-url", default=METRICS.DEFAULT_FLINK_URL)
    parser.add_argument("--prometheus-url", default=METRICS.DEFAULT_PROMETHEUS_URL)
    parser.add_argument(
        "--producer-url",
        default=(
            f"http://{os.environ.get('TARGET_IP', os.environ.get('TARGET_HOST', 'c153'))}:"
            f"{os.environ.get('PRODUCER_REST_PORT', '18081')}"
        ),
    )
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--rate-window", default="30s")
    parser.add_argument("--cpu-rate-window", default="2m")
    parser.add_argument(
        "--target-rate", type=float, default=float(os.environ.get("TPS", "0"))
    )
    parser.add_argument("--stabilization-seconds", type=float, default=60.0)
    parser.add_argument("--window-seconds", type=float, default=120.0)
    parser.add_argument("--required-windows", type=int, default=3)
    parser.add_argument("--input-tolerance", type=float, default=0.05)
    parser.add_argument("--lag-growth-tolerance", type=float, default=0.01)
    parser.add_argument("--summary-only", action="store_true")
    parser.add_argument("--no-clear", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--exit-when-stable",
        action="store_true",
        help="Exit with status 0 after printing the first accepted stable state",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.target_rate <= 0:
        parser.error(
            "--target-rate must be positive; source run-env.sh or pass it explicitly"
        )
    if args.interval <= 0 or args.stabilization_seconds < 0 or args.window_seconds <= 0:
        parser.error("interval/window values must be positive")
    if args.required_windows <= 0:
        parser.error("--required-windows must be positive")
    if not 0 <= args.input_tolerance < 1 or not 0 <= args.lag_growth_tolerance < 1:
        parser.error("tolerances must be in [0, 1)")

    flink = METRICS.FlinkClient(args.flink_url)
    prometheus = METRICS.PrometheusClient(args.prometheus_url)
    tracker = StabilityTracker(
        target_rate=args.target_rate,
        stabilization_seconds=args.stabilization_seconds,
        window_seconds=args.window_seconds,
        required_windows=args.required_windows,
        input_tolerance=args.input_tolerance,
        lag_growth_tolerance=args.lag_growth_tolerance,
        sample_interval=args.interval,
    )

    try:
        while True:
            now = time.time()
            cluster = None
            snapshot = None
            lag = None
            cluster_error = None
            metrics_error = None
            producer = producer_state(args.producer_url)

            try:
                cluster = cluster_state(args.deployment, args.namespace, args.configmap)
            except Exception as exc:
                cluster_error = str(exc)

            try:
                job = flink.active_job()
                snapshot = METRICS.collect_snapshot(
                    flink,
                    prometheus,
                    job,
                    args.namespace,
                    args.rate_window,
                    args.cpu_rate_window,
                    None,
                    int(os.environ["EVENTS"]) if os.environ.get("EVENTS") else None,
                    float(os.environ.get("SOURCE_EVENT_SHARE", "1.0")),
                )
                lag = kafka_lag(prometheus, job.job_id)
            except Exception as exc:
                metrics_error = str(exc)

            if cluster is not None:
                tracker.observe(
                    now,
                    snapshot.job_state if snapshot is not None else None,
                    cluster.transaction,
                    producer,
                    lag,
                    cluster.resource_fingerprint,
                )

            if not args.no_clear and not args.once:
                print("\033[2J\033[H", end="")
            print(
                render(
                    snapshot,
                    cluster,
                    producer,
                    lag,
                    tracker,
                    now,
                    metrics_error,
                    cluster_error,
                    args.summary_only,
                ),
                flush=True,
            )
            if args.exit_when_stable and tracker.accepted_status is not None:
                return 0
            if args.once:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
