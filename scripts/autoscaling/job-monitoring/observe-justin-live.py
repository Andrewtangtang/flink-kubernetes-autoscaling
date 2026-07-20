#!/usr/bin/env python3
"""Observe live Justin decisions by parsing the Flink operator log.

The deployed A4S operator may not persist ``scalingConfigHistory`` in the
autoscaler ConfigMap.  This observer therefore follows the operator log and
formats the latest ``ScalingConfigurations`` entry emitted at each decision.

Examples:
    ./observe-justin-live.py
    ./observe-justin-live.py --follow
    ./observe-justin-live.py --follow --since 5m
    ./observe-justin-live.py --deployment flink --namespace default
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

SHARED_KUBECONFIG = Path("/etc/flink-kubernetes-autoscaling/kubeconfig")
if "KUBECONFIG" not in os.environ and os.access(SHARED_KUBECONFIG, os.R_OK):
    os.environ["KUBECONFIG"] = str(SHARED_KUBECONFIG)

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}")
CONFIGURATION_START_RE = re.compile(r"(?P<period>\d+)=ScalingConfiguration\{scaling=\{")
SCALING_INFORMATION_RE = re.compile(
    r"(?P<vertex>[0-9a-f]{32})=ScalingInformation\{"
    r"avgThroughput=(?P<throughput>[^,]+), "
    r"parallelism=(?P<parallelism>-?\d+), "
    r"memoryLevel=(?P<memory_level>-?\d+), "
    r"managedMemoryMB=(?P<managed_memory>[^,]+), "
    r"verticalScaling=(?P<vertical>true|false), "
    r"horizontalScaling=(?P<horizontal>true|false), "
    r"avgCacheHitRate=(?P<cache_hit>[^,]+), "
    r"avgStateLatency=(?P<state_latency>[^}]+)\}"
)


@dataclass(frozen=True)
class JustinVertexDecision:
    vertex_id: str
    throughput: str
    parallelism: int
    memory_level: int
    managed_memory: str
    vertical_scaling: bool
    horizontal_scaling: bool
    cache_hit: str
    state_latency: str


def strip_ansi(line: str) -> str:
    return ANSI_RE.sub("", line).strip()


def parse_latest_configuration(
    line: str,
) -> tuple[int, list[JustinVertexDecision]] | None:
    starts = list(CONFIGURATION_START_RE.finditer(line))
    if not starts:
        return None

    latest = starts[-1]
    decisions = []
    for match in SCALING_INFORMATION_RE.finditer(line, latest.end()):
        decisions.append(
            JustinVertexDecision(
                vertex_id=match.group("vertex"),
                throughput=match.group("throughput"),
                parallelism=int(match.group("parallelism")),
                memory_level=int(match.group("memory_level")),
                managed_memory=match.group("managed_memory"),
                vertical_scaling=match.group("vertical") == "true",
                horizontal_scaling=match.group("horizontal") == "true",
                cache_hit=match.group("cache_hit"),
                state_latency=match.group("state_latency"),
            )
        )

    if not decisions:
        return None
    return int(latest.group("period")), decisions


def fetch_vertex_names(flink_url: str, timeout: float = 2.0) -> dict[str, str]:
    base_url = flink_url.rstrip("/")
    try:
        with urlopen(f"{base_url}/jobs/overview", timeout=timeout) as response:
            overview = json.load(response)
        active_jobs = [
            job
            for job in overview.get("jobs", [])
            if job.get("state")
            in {"RUNNING", "RESTARTING", "CREATED", "INITIALIZING", "RECONCILING"}
        ]
        if not active_jobs:
            return {}
        active_jobs.sort(
            key=lambda job: (
                job.get("start-time", -1),
                job.get("last-modification", -1),
            ),
            reverse=True,
        )
        job_id = active_jobs[0]["jid"]
        with urlopen(f"{base_url}/jobs/{job_id}", timeout=timeout) as response:
            details = json.load(response)
        return {
            vertex["id"]: vertex.get("name", vertex["id"])
            for vertex in details.get("vertices", [])
        }
    except (OSError, URLError, ValueError, KeyError, json.JSONDecodeError):
        return {}


def short_name(vertex_id: str, vertex_names: dict[str, str]) -> str:
    name = vertex_names.get(vertex_id)
    if not name:
        return vertex_id[:10]
    if len(name) <= 42:
        return name
    return f"{name[:39]}..."


def print_configuration(
    timestamp: str,
    period: int,
    decisions: list[JustinVertexDecision],
    vertex_names: dict[str, str],
) -> None:
    print(f"\nJustin decision at {timestamp} (period {period})")
    print(
        f"{'Vertex':<42} {'P':>3} {'Mem':>4} {'Throughput':>12} "
        f"{'Cache':>8} {'H':>3} {'V':>3} {'StateLat':>10}"
    )
    print("-" * 94)
    for decision in sorted(decisions, key=lambda item: item.vertex_id):
        horizontal = "yes" if decision.horizontal_scaling else "no"
        vertical = "yes" if decision.vertical_scaling else "no"
        print(
            f"{short_name(decision.vertex_id, vertex_names):<42} "
            f"{decision.parallelism:>3} {decision.memory_level:>4} "
            f"{decision.throughput:>12} {decision.cache_hit:>8} "
            f"{horizontal:>3} {vertical:>3} {decision.state_latency:>10}"
        )
    print(flush=True)


def message_after(line: str, marker: str) -> str:
    return line[line.index(marker) :]


def build_kubectl_command(args: argparse.Namespace) -> list[str]:
    command = [
        "kubectl",
        "logs",
        f"deployment/{args.operator_deployment}",
        "--namespace",
        args.operator_namespace,
        "--container",
        args.operator_container,
        f"--since={args.since}",
    ]
    if args.follow:
        command.append("--follow")
    return command


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment", default="flink", help="FlinkDeployment name")
    parser.add_argument(
        "--namespace", default="default", help="FlinkDeployment namespace"
    )
    parser.add_argument(
        "--operator-deployment",
        default="flink-kubernetes-operator",
        help="operator Deployment name",
    )
    parser.add_argument(
        "--operator-namespace", default="default", help="operator namespace"
    )
    parser.add_argument(
        "--operator-container",
        default="flink-kubernetes-operator",
        help="operator container name",
    )
    parser.add_argument(
        "--flink-url",
        default="http://localhost:8081",
        help="Flink REST URL used only to resolve vertex names",
    )
    parser.add_argument("--since", default="30m", help="kubectl log lookback")
    parser.add_argument(
        "--follow", "-f", action="store_true", help="follow new decisions"
    )
    args = parser.parse_args()

    context_marker = f"[{args.namespace}/{args.deployment}]"
    command = build_kubectl_command(args)
    print(f"Deployment: {args.namespace}/{args.deployment}")
    print("Source: live Flink operator log (does not require scalingConfigHistory)")
    print(f"Command: {' '.join(command)}")
    print("Waiting for Justin decisions...", flush=True)

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    def stop_process(_signum, _frame) -> None:
        if process.poll() is None:
            process.terminate()

    signal.signal(signal.SIGINT, stop_process)
    signal.signal(signal.SIGTERM, stop_process)

    assert process.stdout is not None
    saw_relevant_line = False
    for raw_line in process.stdout:
        line = strip_ansi(raw_line)
        if context_marker not in line:
            continue

        parsed = parse_latest_configuration(line)
        if parsed:
            saw_relevant_line = True
            period, decisions = parsed
            timestamp_match = TIMESTAMP_RE.search(line)
            timestamp = timestamp_match.group(0) if timestamp_match else "unknown time"
            vertex_names = fetch_vertex_names(args.flink_url)
            print_configuration(timestamp, period, decisions, vertex_names)
            continue

        if "Justin:" in line:
            saw_relevant_line = True
            timestamp_match = TIMESTAMP_RE.search(line)
            timestamp = timestamp_match.group(0) if timestamp_match else "unknown time"
            print(f"[{timestamp}] {message_after(line, 'Justin:')}", flush=True)
        elif "Incrementing scaling period" in line:
            saw_relevant_line = True
            timestamp_match = TIMESTAMP_RE.search(line)
            timestamp = timestamp_match.group(0) if timestamp_match else "unknown time"
            print(
                f"[{timestamp}] {message_after(line, 'Incrementing scaling period')}",
                flush=True,
            )

    return_code = process.wait()
    if not args.follow and not saw_relevant_line and return_code == 0:
        print(f"No Justin decisions found in the last {args.since}.")
    if return_code not in (0, -signal.SIGTERM):
        print(f"kubectl logs exited with status {return_code}", file=sys.stderr)
        return return_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
