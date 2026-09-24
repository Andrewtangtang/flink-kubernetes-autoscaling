#!/usr/bin/env python3
"""Render the Q20 checkpoint pilot without a cluster-specific configuration file."""

import argparse
import re
from pathlib import Path


EXPERIMENT = Path(__file__).resolve().parent


def checked(value: str, pattern: str, name: str) -> str:
    if not re.fullmatch(pattern, value):
        raise ValueError(f"invalid {name}: {value!r}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", choices=("justin", "ds2"), required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--kafka-bootstrap", required=True)
    parser.add_argument("--checkpoint-pvc", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    replacements = {
        "__RUN_ID__": checked(args.run_id, r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", "run ID"),
        "__FLINK_IMAGE__": checked(args.image, r"[A-Za-z0-9./:_@-]+", "image"),
        "__KAFKA_BOOTSTRAP__": checked(
            args.kafka_bootstrap, r"[A-Za-z0-9.-]+:[0-9]{1,5}", "Kafka endpoint"
        ),
        "__CHECKPOINT_PVC__": checked(
            args.checkpoint_pvc, r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?", "PVC name"
        ),
    }
    template = EXPERIMENT / "jobs" / "q20" / args.policy / "experiment.yaml"
    if args.output.resolve() == template:
        raise ValueError("output must not overwrite the template")
    manifest = template.read_text()
    for placeholder, value in replacements.items():
        manifest = manifest.replace(placeholder, value)
    if re.search(r"__[A-Z0-9_]+__", manifest):
        raise ValueError("unresolved manifest placeholder")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(manifest)


if __name__ == "__main__":
    main()
