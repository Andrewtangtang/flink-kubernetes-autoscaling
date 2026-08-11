#!/usr/bin/env python3
"""Display live Flink pod and subtask metrics from Prometheus.

The monitor discovers the active Flink job through the JobManager REST API,
then queries Prometheus for per-subtask busy time and record rates plus
per-TaskManager-pod CPU and memory. It complements observe-scaling.py, which
shows autoscaler decisions only after each metrics window.

Start the repository monitoring port forwards before running this script:

    scripts/autoscaling/job-monitoring/port-forward.sh start
    scripts/autoscaling/job-monitoring/observe-flink-metrics.py

Use --once for a single snapshot or --json for machine-readable JSON lines.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

ACTIVE_JOB_STATES = {
    "RUNNING",
    "RESTARTING",
    "CREATED",
    "INITIALIZING",
    "RECONCILING",
}
DEFAULT_PROMETHEUS_URL = (
    f"http://localhost:{os.environ.get('PROMETHEUS_LOCAL_PORT', '19091')}"
)
DEFAULT_FLINK_URL = f"http://localhost:{os.environ.get('FLINK_LOCAL_PORT', '18082')}"
DEFAULT_NAMESPACE = "default"
DEFAULT_INTERVAL_SECONDS = 5.0
DEFAULT_RATE_WINDOW = "30s"
DEFAULT_CPU_RATE_WINDOW = "2m"
TASKMANAGER_POD_PATTERN = "flink-taskmanager-.*"


class MonitorError(RuntimeError):
    """Raised when a monitoring endpoint cannot provide a usable response."""


class NoActiveJobError(MonitorError):
    """Raised while no active Flink job is visible."""


@dataclass(frozen=True)
class ActiveJob:
    job_id: str
    name: str
    state: str


@dataclass(frozen=True, order=True)
class TaskKey:
    pod: str
    task_name: str
    subtask_index: str


@dataclass
class TaskMetrics:
    pod: str
    node: str
    task_name: str
    subtask_index: str
    busy_percent: float | None = None
    records_in_per_second: float | None = None
    records_out_per_second: float | None = None


@dataclass
class PodMetrics:
    pod: str
    node: str
    task_count: int
    cpu_cores: float | None
    memory_bytes: float | None
    average_busy_percent: float | None
    maximum_busy_percent: float | None
    task_records_in_per_second: float
    task_records_out_per_second: float


@dataclass
class MetricsSnapshot:
    timestamp: str
    job_id: str
    job_name: str
    job_state: str
    rate_window: str
    cpu_rate_window: str
    source_records_out_per_second: float
    source_records_out_total: float
    processed_events_estimate: int | None
    total_events: int | None
    replay_progress_percent: float | None
    pods: list[PodMetrics]
    tasks: list[TaskMetrics]


def get_json(url: str, timeout: float = 5.0) -> dict[str, Any]:
    try:
        with urlopen(url, timeout=timeout) as response:
            return json.load(response)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise MonitorError(f"HTTP request failed for {url}: {exc}") from exc


def label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def valid_rate_window(value: str) -> str:
    if not re.fullmatch(r"[1-9]\d*[smhd]", value):
        raise argparse.ArgumentTypeError(
            "rate window must be a positive Prometheus duration such as 30s or 2m"
        )
    return value


class FlinkClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def active_job(self, job_name: str | None = None) -> ActiveJob:
        overview = get_json(f"{self.base_url}/jobs/overview")
        candidates = []
        for job in overview.get("jobs", []):
            if job.get("state") not in ACTIVE_JOB_STATES:
                continue
            if job_name and job.get("name") != job_name:
                continue
            candidates.append(job)

        if not candidates:
            requested = f" named {job_name}" if job_name else ""
            raise NoActiveJobError(f"No active Flink job{requested}")

        candidates.sort(
            key=lambda job: (
                job.get("start-time", -1),
                job.get("last-modification", -1),
            ),
            reverse=True,
        )
        selected = candidates[0]
        return ActiveJob(
            job_id=str(selected["jid"]),
            name=str(selected.get("name", selected["jid"])),
            state=str(selected.get("state", "UNKNOWN")),
        )


class PrometheusClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def query(self, expression: str) -> list[dict[str, Any]]:
        query_string = urlencode({"query": expression})
        payload = get_json(f"{self.base_url}/api/v1/query?{query_string}")
        if payload.get("status") != "success":
            raise MonitorError(f"Prometheus query failed: {payload}")
        data = payload.get("data", {})
        if data.get("resultType") != "vector":
            raise MonitorError(
                f"Expected Prometheus vector result, got {data.get('resultType')}"
            )
        return list(data.get("result", []))


def sample_value(sample: dict[str, Any]) -> float | None:
    value = sample.get("value")
    if not isinstance(value, list) or len(value) != 2:
        return None
    try:
        parsed = float(value[1])
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def task_key(metric: dict[str, str]) -> TaskKey:
    return TaskKey(
        pod=metric.get("pod") or metric.get("host") or metric.get("tm_id") or "unknown",
        task_name=metric.get("task_name") or metric.get("operator_name") or "unknown",
        subtask_index=metric.get("subtask_index", "?"),
    )


def sample_map(
    samples: list[dict[str, Any]],
) -> dict[TaskKey, float]:
    values: dict[TaskKey, float] = {}
    for sample in samples:
        value = sample_value(sample)
        metric = sample.get("metric")
        if value is None or not isinstance(metric, dict):
            continue
        values[task_key(metric)] = value
    return values


def pod_value_map(
    samples: list[dict[str, Any]],
) -> dict[str, float]:
    values: dict[str, float] = {}
    for sample in samples:
        value = sample_value(sample)
        metric = sample.get("metric")
        if value is None or not isinstance(metric, dict):
            continue
        pod = metric.get("pod")
        if pod:
            values[pod] = value
    return values


def pod_nodes(samples: list[dict[str, Any]]) -> dict[str, str]:
    nodes: dict[str, str] = {}
    for sample in samples:
        metric = sample.get("metric")
        if not isinstance(metric, dict):
            continue
        pod = metric.get("pod")
        node = metric.get("node")
        if pod and node:
            nodes[pod] = node
    return nodes


def estimate_replay_progress(
    source_records_out_total: float,
    total_events: int | None,
    source_event_share: float,
) -> tuple[int | None, float | None]:
    if total_events is None:
        return None, None

    processed_events = min(
        total_events,
        max(0, int(source_records_out_total / source_event_share)),
    )
    return processed_events, processed_events / total_events * 100.0


def metric_queries(
    job_id: str,
    namespace: str,
    rate_window: str,
    cpu_rate_window: str,
) -> dict[str, str]:
    job = label_value(job_id)
    ns = label_value(namespace)
    pods = TASKMANAGER_POD_PATTERN
    return {
        "pod_info": (f'kube_pod_info{{namespace="{ns}",pod=~"{pods}"}}'),
        "busy": (
            "avg by (pod, task_name, subtask_index) "
            f'(flink_taskmanager_job_task_busyTimeMsPerSecond{{job_id="{job}"}})'
        ),
        "records_in": (
            "sum by (pod, task_name, subtask_index) "
            f'(rate(flink_taskmanager_job_task_numRecordsIn{{job_id="{job}"}}'
            f"[{rate_window}]))"
        ),
        "records_out": (
            "sum by (pod, task_name, subtask_index) "
            f'(rate(flink_taskmanager_job_task_numRecordsOut{{job_id="{job}"}}'
            f"[{rate_window}]))"
        ),
        "records_out_total": (
            "sum by (pod, task_name, subtask_index) "
            "(last_over_time("
            f'flink_taskmanager_job_task_numRecordsOut{{job_id="{job}"}}'
            f"[{rate_window}]))"
        ),
        "pod_cpu": (
            "sum by (pod) "
            f'(rate(container_cpu_usage_seconds_total{{namespace="{ns}",'
            f'pod=~"{pods}",container="flink-main-container",cpu="total"}}'
            f"[{cpu_rate_window}]))"
        ),
        "pod_memory": (
            "sum by (pod) "
            f'(container_memory_working_set_bytes{{namespace="{ns}",'
            f'pod=~"{pods}",container="flink-main-container"}})'
        ),
    }


def collect_snapshot(
    prometheus: PrometheusClient,
    job: ActiveJob,
    namespace: str,
    rate_window: str,
    cpu_rate_window: str,
    task_pattern: re.Pattern[str] | None,
    total_events: int | None,
    source_event_share: float,
) -> MetricsSnapshot:
    queries = metric_queries(
        job.job_id,
        namespace,
        rate_window,
        cpu_rate_window,
    )
    results = {name: prometheus.query(query) for name, query in queries.items()}

    nodes = pod_nodes(results["pod_info"])
    active_pods = set(nodes)
    busy = sample_map(results["busy"])
    records_in = sample_map(results["records_in"])
    records_out = sample_map(results["records_out"])
    records_out_total = sample_map(results["records_out_total"])
    pod_cpu = pod_value_map(results["pod_cpu"])
    pod_memory = pod_value_map(results["pod_memory"])

    keys = sorted(set(busy) | set(records_in) | set(records_out))
    tasks = []
    for key in keys:
        if active_pods and key.pod != "unknown" and key.pod not in active_pods:
            continue
        if task_pattern and not task_pattern.search(key.task_name):
            continue
        busy_ms = busy.get(key)
        busy_percent = None if busy_ms is None or busy_ms < 0 else busy_ms / 10.0
        tasks.append(
            TaskMetrics(
                pod=key.pod,
                node=nodes.get(key.pod, "?"),
                task_name=key.task_name,
                subtask_index=key.subtask_index,
                busy_percent=busy_percent,
                records_in_per_second=records_in.get(key),
                records_out_per_second=records_out.get(key),
            )
        )

    grouped: dict[str, list[TaskMetrics]] = {}
    for pod in active_pods:
        grouped[pod] = []
    for task in tasks:
        grouped.setdefault(task.pod, []).append(task)

    pods = []
    for pod, pod_tasks in sorted(grouped.items()):
        busy_values = [
            task.busy_percent for task in pod_tasks if task.busy_percent is not None
        ]
        pods.append(
            PodMetrics(
                pod=pod,
                node=nodes.get(pod, "?"),
                task_count=len(pod_tasks),
                cpu_cores=pod_cpu.get(pod),
                memory_bytes=pod_memory.get(pod),
                average_busy_percent=(
                    sum(busy_values) / len(busy_values) if busy_values else None
                ),
                maximum_busy_percent=max(busy_values) if busy_values else None,
                task_records_in_per_second=sum(
                    task.records_in_per_second or 0.0 for task in pod_tasks
                ),
                task_records_out_per_second=sum(
                    task.records_out_per_second or 0.0 for task in pod_tasks
                ),
            )
        )

    source_records_out = sum(
        value
        for key, value in records_out.items()
        if key.task_name.startswith("Source:")
        and (not active_pods or key.pod == "unknown" or key.pod in active_pods)
    )
    source_records_out_total = sum(
        value
        for key, value in records_out_total.items()
        if key.task_name.startswith("Source:")
        and (not active_pods or key.pod == "unknown" or key.pod in active_pods)
    )
    processed_events_estimate, replay_progress_percent = estimate_replay_progress(
        source_records_out_total,
        total_events,
        source_event_share,
    )

    return MetricsSnapshot(
        timestamp=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        job_id=job.job_id,
        job_name=job.name,
        job_state=job.state,
        rate_window=rate_window,
        cpu_rate_window=cpu_rate_window,
        source_records_out_per_second=source_records_out,
        source_records_out_total=source_records_out_total,
        processed_events_estimate=processed_events_estimate,
        total_events=total_events,
        replay_progress_percent=replay_progress_percent,
        pods=pods,
        tasks=tasks,
    )


def format_rate(value: float | None) -> str:
    if value is None:
        return "-"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:.1f}"


def format_percent(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f}%"


def format_cpu(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}"


def format_memory(value: float | None) -> str:
    return "-" if value is None else f"{value / (1024 ** 3):.2f}G"


def shorten(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return f"{value[: width - 3]}..."


def subtask_sort_key(task: TaskMetrics) -> tuple[str, int, str]:
    try:
        subtask = int(task.subtask_index)
    except ValueError:
        subtask = sys.maxsize
    return task.task_name, subtask, task.pod


def render_snapshot(snapshot: MetricsSnapshot, summary_only: bool) -> str:
    lines = [
        (
            f"[{snapshot.timestamp}] {snapshot.job_name} "
            f"state={snapshot.job_state} job={snapshot.job_id[:10]}"
        ),
        (
            f"TaskManager pods={len(snapshot.pods)} "
            f"tasks={len(snapshot.tasks)} "
            f"total source out ({snapshot.rate_window} avg)="
            f"{format_rate(snapshot.source_records_out_per_second)}/s"
        ),
        (
            f"Prometheus averages: task record rates={snapshot.rate_window}; "
            f"pod CPU={snapshot.cpu_rate_window}"
        ),
    ]
    if (
        snapshot.processed_events_estimate is not None
        and snapshot.total_events is not None
        and snapshot.replay_progress_percent is not None
    ):
        lines.append(
            "Replay progress≈"
            f"{snapshot.processed_events_estimate:,}/{snapshot.total_events:,} "
            f"events ({snapshot.replay_progress_percent:.2f}%); "
            f"source records={snapshot.source_records_out_total:,.0f}"
        )
    lines.extend(
        [
            "",
            "POD SUMMARY",
            (
                f"{'Pod':<30} {'Node':<6} {'Tasks':>5} {'CPU':>6} {'Mem':>7} "
                f"{'AvgBusy':>8} {'MaxBusy':>8} "
                f"{'TaskIn/s':>10} {'TaskOut/s':>10}"
            ),
            "-" * 107,
        ]
    )
    for pod in snapshot.pods:
        lines.append(
            f"{shorten(pod.pod, 30):<30} {shorten(pod.node, 6):<6} "
            f"{pod.task_count:>5} {format_cpu(pod.cpu_cores):>6} "
            f"{format_memory(pod.memory_bytes):>7} "
            f"{format_percent(pod.average_busy_percent):>8} "
            f"{format_percent(pod.maximum_busy_percent):>8} "
            f"{format_rate(pod.task_records_in_per_second):>10} "
            f"{format_rate(pod.task_records_out_per_second):>10}"
        )

    if summary_only:
        return "\n".join(lines)

    lines.extend(
        [
            "",
            "SUBTASK METRICS",
            (
                f"{'Pod':<26} {'Node':<6} {'Sub':>3} {'Busy':>7} "
                f"{'In/s':>9} {'Out/s':>9}  Task"
            ),
            "-" * 110,
        ]
    )
    for task in sorted(snapshot.tasks, key=subtask_sort_key):
        lines.append(
            f"{shorten(task.pod, 26):<26} {shorten(task.node, 6):<6} "
            f"{task.subtask_index:>3} {format_percent(task.busy_percent):>7} "
            f"{format_rate(task.records_in_per_second):>9} "
            f"{format_rate(task.records_out_per_second):>9}  "
            f"{shorten(task.task_name, 54)}"
        )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prometheus-url",
        default=DEFAULT_PROMETHEUS_URL,
        help=f"Prometheus base URL (default: {DEFAULT_PROMETHEUS_URL})",
    )
    parser.add_argument(
        "--flink-url",
        default=DEFAULT_FLINK_URL,
        help=f"Flink REST base URL (default: {DEFAULT_FLINK_URL})",
    )
    parser.add_argument(
        "--namespace",
        default=DEFAULT_NAMESPACE,
        help=f"Flink pod namespace (default: {DEFAULT_NAMESPACE})",
    )
    parser.add_argument("--job-name", help="Require this exact active Flink job name")
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help=f"Refresh interval in seconds (default: {DEFAULT_INTERVAL_SECONDS:g})",
    )
    parser.add_argument(
        "--rate-window",
        type=valid_rate_window,
        default=DEFAULT_RATE_WINDOW,
        help=f"Flink task record-rate window (default: {DEFAULT_RATE_WINDOW})",
    )
    parser.add_argument(
        "--cpu-rate-window",
        type=valid_rate_window,
        default=DEFAULT_CPU_RATE_WINDOW,
        help=(
            "Container CPU-rate window "
            f"(default: {DEFAULT_CPU_RATE_WINDOW}; kubelet scrapes are sparse)"
        ),
    )
    parser.add_argument(
        "--total-events",
        type=int,
        default=int(os.environ["EVENTS"]) if os.environ.get("EVENTS") else None,
        help=(
            "Total raw Nexmark events in the current replay "
            "(default: EVENTS environment variable; disabled if unset)"
        ),
    )
    parser.add_argument(
        "--source-event-share",
        type=float,
        default=float(os.environ.get("SOURCE_EVENT_SHARE", "1.0")),
        help=(
            "Fraction of raw Nexmark events represented by the monitored "
            "Flink sources (default: SOURCE_EVENT_SHARE or 1.0)"
        ),
    )
    parser.add_argument(
        "--task-regex",
        help="Only display tasks whose task_name matches this regular expression",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Hide the per-subtask table",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Print one snapshot and exit instead of refreshing continuously",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print each snapshot as one JSON object",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not clear the terminal before each text snapshot",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be positive")
    if args.total_events is not None and args.total_events <= 0:
        parser.error("--total-events must be positive")
    if not 0 < args.source_event_share <= 1:
        parser.error("--source-event-share must be in the interval (0, 1]")

    try:
        task_pattern = re.compile(args.task_regex) if args.task_regex else None
    except re.error as exc:
        parser.error(f"invalid --task-regex: {exc}")

    flink = FlinkClient(args.flink_url)
    prometheus = PrometheusClient(args.prometheus_url)

    try:
        while True:
            try:
                job = flink.active_job(args.job_name)
                snapshot = collect_snapshot(
                    prometheus,
                    job,
                    args.namespace,
                    args.rate_window,
                    args.cpu_rate_window,
                    task_pattern,
                    args.total_events,
                    args.source_event_share,
                )
                if args.json:
                    print(
                        json.dumps(asdict(snapshot), separators=(",", ":")), flush=True
                    )
                else:
                    if not args.no_clear and not args.once:
                        print("\033[2J\033[H", end="")
                    print(render_snapshot(snapshot, args.summary_only), flush=True)
            except MonitorError as exc:
                if args.once:
                    print(f"Error: {exc}", file=sys.stderr)
                    return 1
                if not args.no_clear:
                    print("\033[2J\033[H", end="")
                timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
                print(f"[{timestamp}] Waiting for metrics: {exc}", flush=True)

            if args.once:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
