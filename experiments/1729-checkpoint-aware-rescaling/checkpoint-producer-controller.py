#!/usr/bin/env python3
"""Pause a Docker producer during an Operator checkpoint-rescale transaction."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


ANNOTATION_PREFIX = "autoscaling.flink.apache.org/checkpoint-rescale-producer-"
INTENT_ANNOTATION = ANNOTATION_PREFIX + "pause-intent"
PAUSE_ACK_ANNOTATION = ANNOTATION_PREFIX + "pause-ack"
RESUME_ACK_ANNOTATION = ANNOTATION_PREFIX + "resume-ack"
PAUSE_PHASE = "WAITING_PRODUCER_PAUSE"
RESUME_PHASE = "WAITING_PRODUCER_RESUME"


@dataclass(frozen=True)
class Transaction:
    transaction_id: str
    phase: str
    pause_required: bool


def run(command: list[str]) -> str:
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"{' '.join(command)} failed: {detail}")
    return result.stdout.strip()


def kubectl_json(*args: str) -> dict[str, Any]:
    return json.loads(run(["kubectl", *args, "-o", "json"]))


def parse_transaction(raw: str) -> Transaction | None:
    if not raw.strip():
        return None
    values = {}
    for line in raw.splitlines():
        if line and not line[0].isspace():
            key, separator, value = line.partition(":")
            if separator:
                values[key] = value.strip().strip('"\'')
    transaction_id = values.get("transactionId")
    phase = values.get("phase")
    if not transaction_id or not phase:
        raise ValueError("checkpoint transaction lacks an ID or phase")
    required = values.get("producerPauseRequired", "false")
    if required not in {"true", "false"}:
        raise ValueError("invalid producerPauseRequired value")
    if phase in {PAUSE_PHASE, RESUME_PHASE} and required != "true":
        raise ValueError("producer control phase lacks a pause requirement")
    return Transaction(transaction_id, phase, required == "true")


def load_state(
    deployment: str, namespace: str, configmap: str
) -> tuple[Transaction | None, dict[str, str]]:
    resource = kubectl_json("get", "flinkdeployment", deployment, "-n", namespace)
    annotations = resource.get("metadata", {}).get("annotations") or {}
    state = kubectl_json("get", "configmap", configmap, "-n", namespace)
    transaction = parse_transaction(
        (state.get("data") or {}).get("checkpointRescaleTransaction", "")
    )
    return transaction, annotations


def annotate(deployment: str, namespace: str, key: str, value: str) -> None:
    run(
        [
            "kubectl",
            "annotate",
            "flinkdeployment",
            deployment,
            "-n",
            namespace,
            f"{key}={value}",
            "--overwrite",
        ]
    )


def producer_paused(container: str) -> bool:
    status = run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}} {{.State.Paused}}",
            container,
        ]
    )
    if status not in {"true false", "true true"}:
        raise RuntimeError(f"producer {container} is not running: {status}")
    return status == "true true"


def ensure_producer(container: str, paused: bool) -> None:
    if producer_paused(container) != paused:
        run(["docker", "pause" if paused else "unpause", container])
        if producer_paused(container) != paused:
            raise RuntimeError(f"producer {container} did not reach requested state")


def reconcile(
    transaction: Transaction | None,
    annotations: dict[str, str],
    deployment: str,
    namespace: str,
    container: str,
) -> str:
    intent = annotations.get(INTENT_ANNOTATION)
    resume_ack = annotations.get(RESUME_ACK_ANNOTATION)
    if transaction is None or not transaction.pause_required:
        if intent and resume_ack != intent:
            ensure_producer(container, paused=False)
            annotate(deployment, namespace, RESUME_ACK_ANNOTATION, intent)
            return f"resumed producer after transaction {intent} was aborted"
        return "idle" if transaction is None else "producer gate disabled for this transaction"

    transaction_id = transaction.transaction_id
    if transaction.phase == "FAILED":
        return f"transaction {transaction_id} failed; producer state unchanged"
    if transaction.phase == "COMPLETED":
        return f"transaction {transaction_id} completed"
    if transaction.phase == RESUME_PHASE:
        ensure_producer(container, paused=False)
        if resume_ack != transaction_id:
            annotate(deployment, namespace, RESUME_ACK_ANNOTATION, transaction_id)
        return f"producer resumed for {transaction_id}"

    # Record intent before Docker pause so an abort can recover even if ACK fails.
    if intent != transaction_id:
        annotate(deployment, namespace, INTENT_ANNOTATION, transaction_id)
    ensure_producer(container, paused=True)
    if (
        transaction.phase == PAUSE_PHASE
        and annotations.get(PAUSE_ACK_ANNOTATION) != transaction_id
    ):
        annotate(deployment, namespace, PAUSE_ACK_ANNOTATION, transaction_id)
    return f"producer paused for {transaction_id} ({transaction.phase})"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment", default="flink")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--configmap")
    parser.add_argument("--container", required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")

    previous = None
    while True:
        try:
            transaction, annotations = load_state(
                args.deployment,
                args.namespace,
                args.configmap or f"autoscaler-{args.deployment}",
            )
            message = reconcile(
                transaction, annotations, args.deployment, args.namespace, args.container
            )
            error = False
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exception:
            message = f"waiting: {exception}"
            error = True
        if message != previous:
            print(
                f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {message}",
                flush=True,
            )
            previous = message
        if args.once:
            return int(error)
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
