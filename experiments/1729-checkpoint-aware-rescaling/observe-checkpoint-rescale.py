#!/usr/bin/env python3

"""Observe a checkpoint-aware rescale without modifying Kafka or the producer."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from typing import Any
from urllib.request import urlopen


def get_json(url: str) -> dict[str, Any]:
    with urlopen(url, timeout=10) as response:
        return json.load(response)


def kubectl_json(*args: str) -> dict[str, Any]:
    command = ["kubectl", *args, "-o", "json"]
    return json.loads(subprocess.check_output(command, text=True))


def transaction(deployment: str, namespace: str) -> tuple[str, str, str, str]:
    resource = kubectl_json(
        "get", "flinkdeployment", deployment, "--namespace", namespace
    )
    annotations = resource.get("metadata", {}).get("annotations", {})
    phase = annotations.get(
        "autoscaling.flink.apache.org/checkpoint-rescale-phase", "IDLE"
    )
    configmap = kubectl_json(
        "get", "configmap", f"autoscaler-{deployment}", "--namespace", namespace
    )
    raw = configmap.get("data", {}).get("checkpointRescaleTransaction", "")
    checkpoint_id = "-"
    restart_millis = "-"
    error = "-"
    for line in raw.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            continue
        if key.strip() == "completedCheckpointId":
            checkpoint_id = value.strip()
        elif key.strip() == "observedRestartDurationMillis":
            restart_millis = value.strip()
        elif key.strip() == "phase":
            phase = value.strip()
        elif key.strip() == "error":
            error = value.strip()
    return phase, checkpoint_id, restart_millis, error


def active_job(base_url: str) -> dict[str, Any] | None:
    jobs = get_json(f"{base_url}/jobs/overview").get("jobs", [])
    active = [job for job in jobs if job.get("state") not in {"FINISHED", "CANCELED"}]
    return active[0] if active else None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--url",
        default=f"http://localhost:{os.environ.get('FLINK_LOCAL_PORT', '18082')}",
    )
    parser.add_argument("--deployment", default="flink")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--interval", type=float, default=5.0)
    args = parser.parse_args()

    while True:
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        try:
            job = active_job(args.url)
            phase, checkpoint_id, restart_millis, error = transaction(
                args.deployment, args.namespace
            )
            if job is None:
                print(f"[{timestamp}] no active job phase={phase}", flush=True)
            else:
                details = get_json(f"{args.url}/jobs/{job['jid']}")
                checkpoints = get_json(f"{args.url}/jobs/{job['jid']}/checkpoints")
                counts = checkpoints.get("counts", {})
                running_since = details.get("timestamps", {}).get("RUNNING", "-")
                print(
                    f"[{timestamp}] job={job['jid'][:10]} state={job['state']} "
                    f"running_since={running_since} phase={phase} "
                    f"gated_checkpoint={checkpoint_id} "
                    f"restart_ms={restart_millis} "
                    f"checkpoints={counts.get('completed', 0)}/"
                    f"{counts.get('total', 0)} error={error}",
                    flush=True,
                )
        except Exception as exception:  # keep observing transient restarts
            print(f"[{timestamp}] waiting: {exception}", flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
