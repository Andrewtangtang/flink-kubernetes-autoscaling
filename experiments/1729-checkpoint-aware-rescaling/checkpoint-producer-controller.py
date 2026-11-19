#!/usr/bin/env python3

"""Pause Kafka input around checkpoint-gated Flink rescaling transactions."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


PAUSE_ACK_ANNOTATION = (
    "autoscaling.flink.apache.org/checkpoint-rescale-producer-pause-ack"
)
RESUME_ACK_ANNOTATION = (
    "autoscaling.flink.apache.org/checkpoint-rescale-producer-resume-ack"
)
PAUSE_PHASE = "WAITING_PRODUCER_PAUSE"
RESUME_PHASE = "WAITING_PRODUCER_RESUME"
FAILED_PHASE = "FAILED"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PRODUCER_SCRIPT = os.path.abspath(
    os.path.join(
        SCRIPT_DIR,
        "../1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh",
    )
)


@dataclass(frozen=True)
class Transaction:
    transaction_id: str
    phase: str


def log(message: str) -> None:
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{timestamp}] {message}", flush=True)


def run(command: list[str], timeout: int = 30) -> str:
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(command)}: {detail}"
        )
    return result.stdout.strip()


def kubectl_json(*args: str) -> dict[str, Any]:
    return json.loads(run(["kubectl", *args, "-o", "json"]))


def parse_scalar(value: str) -> str:
    return value.strip().strip('"').strip("'")


def parse_transaction(raw: str) -> Transaction | None:
    values: dict[str, str] = {}
    for line in raw.splitlines():
        if not line or line[0].isspace():
            continue
        key, separator, value = line.partition(":")
        if separator:
            values[key.strip()] = parse_scalar(value)

    if not values:
        return None
    transaction_id = values.get("transactionId", "")
    phase = values.get("phase", "")
    if not transaction_id or not phase:
        raise RuntimeError("Checkpoint transaction is missing transactionId or phase")
    return Transaction(transaction_id=transaction_id, phase=phase)


def load_state(
    deployment: str, namespace: str, configmap: str
) -> tuple[Transaction | None, dict[str, str]]:
    resource = kubectl_json(
        "get", "flinkdeployment", deployment, "--namespace", namespace
    )
    annotations = resource.get("metadata", {}).get("annotations", {}) or {}
    state = kubectl_json("get", "configmap", configmap, "--namespace", namespace)
    raw = state.get("data", {}).get("checkpointRescaleTransaction", "")
    return parse_transaction(raw), annotations


def acknowledge(
    deployment: str,
    namespace: str,
    annotation: str,
    transaction_id: str,
) -> None:
    run(
        [
            "kubectl",
            "annotate",
            "flinkdeployment",
            deployment,
            "--namespace",
            namespace,
            f"{annotation}={transaction_id}",
            "--overwrite",
        ]
    )


def control_producer(action: str, producer_script: str) -> None:
    run([producer_script, action], timeout=60)


def reconcile(
    transaction: Transaction | None,
    annotations: dict[str, str],
    deployment: str,
    namespace: str,
    producer_script: str,
) -> str:
    pause_ack = annotations.get(PAUSE_ACK_ANNOTATION, "")
    resume_ack = annotations.get(RESUME_ACK_ANNOTATION, "")

    if transaction is None:
        if pause_ack and resume_ack != pause_ack:
            control_producer("resume", producer_script)
            acknowledge(deployment, namespace, RESUME_ACK_ANNOTATION, pause_ack)
            return f"resumed producer after aborted transaction {pause_ack}"
        return "idle"

    transaction_id = transaction.transaction_id
    if transaction.phase == PAUSE_PHASE:
        if pause_ack == transaction_id:
            return f"producer pause already acknowledged for {transaction_id}"
        control_producer("pause", producer_script)
        acknowledge(deployment, namespace, PAUSE_ACK_ANNOTATION, transaction_id)
        return f"paused producer and acknowledged {transaction_id}"

    if transaction.phase == RESUME_PHASE:
        if resume_ack == transaction_id:
            return f"producer resume already acknowledged for {transaction_id}"
        control_producer("resume", producer_script)
        acknowledge(deployment, namespace, RESUME_ACK_ANNOTATION, transaction_id)
        return f"resumed producer and acknowledged {transaction_id}"

    if transaction.phase == FAILED_PHASE:
        return f"transaction {transaction_id} failed; producer remains fail-closed"

    return f"transaction {transaction_id} phase={transaction.phase}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment", default="flink")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--configmap")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--producer-script", default=PRODUCER_SCRIPT)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    if not os.path.isfile(args.producer_script):
        parser.error(f"producer script not found: {args.producer_script}")

    configmap = args.configmap or f"autoscaler-{args.deployment}"
    previous_message = ""
    while True:
        try:
            transaction, annotations = load_state(
                args.deployment, args.namespace, configmap
            )
            message = reconcile(
                transaction,
                annotations,
                args.deployment,
                args.namespace,
                args.producer_script,
            )
        except Exception as exception:
            message = f"waiting: {exception}"

        if message != previous_message:
            log(message)
            previous_message = message
        if args.once:
            return 0 if not message.startswith("waiting:") else 1
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
