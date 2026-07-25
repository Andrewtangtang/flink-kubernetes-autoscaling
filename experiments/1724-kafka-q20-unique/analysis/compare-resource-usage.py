#!/usr/bin/env python3
"""Compare throughput and autoscaler resource allocations for two experiments."""

from __future__ import annotations

import argparse
import base64
import csv
import gzip
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import yaml


MEMORY_PER_LEVEL_MB = 512


def load_series(path: Path) -> tuple[list[float], list[float], float, float]:
    timestamps: list[float] = []
    values: list[float] = []
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            value = row.get("value", "")
            if value in ("", "NaN", "nan"):
                continue
            timestamps.append(float(row["timestamp"]))
            values.append(float(value))

    if not timestamps:
        raise ValueError(f"No samples found in {path}")
    start = timestamps[0]
    return [(timestamp - start) / 60 for timestamp in timestamps], values, start, timestamps[-1]


def load_metadata(path: Path) -> dict[str, Any]:
    return json.loads((path / "metadata.json").read_text(encoding="utf-8"))


def initial_parallelism(path: Path) -> dict[str, int]:
    deployment = (path / "flinkdeployment.yaml").read_text(encoding="utf-8")
    match = re.search(
        r'pipeline\.jobvertex-parallelism-overrides[\\"]*:[\\"]*([^"\\\n]+)',
        deployment,
    )
    if not match:
        raise ValueError(f"Initial parallelism overrides not found in {path}")
    return {
        vertex_id: int(parallelism)
        for vertex_id, parallelism in (
            item.split(":", 1) for item in match.group(1).split(",")
        )
    }


def decode_config_history(path: Path) -> dict[str, Any]:
    configmap = (path / "autoscaler-configmap.yaml").read_text(encoding="utf-8")
    match = re.search(r"^\s+scalingConfigHistory:\s+(\S+)", configmap, re.MULTILINE)
    if not match:
        return {}
    decoded = gzip.decompress(base64.b64decode(match.group(1))).decode()
    return yaml.safe_load(decoded) or {}


def timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def resource_timeline(
    path: Path, start: float, end: float
) -> tuple[list[float], list[float], list[float]]:
    metadata = load_metadata(path)
    autoscaler = metadata["autoscaler"].lower()
    parallelism = initial_parallelism(path)
    events: list[tuple[float, dict[str, int], dict[str, int]]] = []

    if autoscaler == "justin":
        history = decode_config_history(path)
        stateful_vertices = {
            vertex_id
            for decision in history.values()
            for vertex_id, config in decision["scaling"].items()
            if int(config["memoryLevel"]) >= 0
        }
        memory_levels = {vertex_id: 0 for vertex_id in stateful_vertices}
        for event_time, decision in history.items():
            new_parallelism = {
                vertex_id: int(config["parallelism"])
                for vertex_id, config in decision["scaling"].items()
            }
            new_memory_levels = {
                vertex_id: int(config["memoryLevel"])
                for vertex_id, config in decision["scaling"].items()
                if int(config["memoryLevel"]) >= 0
            }
            events.append((timestamp(event_time), new_parallelism, new_memory_levels))

        def memory_mb() -> float:
            return float(
                sum(
                    parallelism[vertex_id]
                    * MEMORY_PER_LEVEL_MB
                    * (memory_level + 1)
                    for vertex_id, memory_level in memory_levels.items()
                )
            )

    elif autoscaler == "ds2":
        memory_levels = {}
        scaling_events = path / "scaling_events.csv"
        with scaling_events.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                events.append(
                    (
                        float(row["timestamp"]),
                        {row["vertex_id"]: int(row["parallelism_new"])},
                        {},
                    )
                )

        def memory_mb() -> float:
            return float(sum(parallelism.values()) * MEMORY_PER_LEVEL_MB)

    else:
        raise ValueError(f"Unsupported autoscaler {autoscaler!r}")

    xs = [0.0]
    cpu = [float(sum(parallelism.values()))]
    memory = [memory_mb()]
    for event_time, new_parallelism, new_memory_levels in sorted(
        events, key=lambda event: event[0]
    ):
        event_minute = max(0.0, (event_time - start) / 60)
        xs.extend([event_minute, event_minute])
        cpu.append(cpu[-1])
        memory.append(memory[-1])
        parallelism.update(new_parallelism)
        memory_levels.update(new_memory_levels)
        cpu.append(float(sum(parallelism.values())))
        memory.append(memory_mb())

    xs.append((end - start) / 60)
    cpu.append(cpu[-1])
    memory.append(memory[-1])
    return xs, cpu, memory


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left")
    parser.add_argument("right")
    parser.add_argument("--output")
    args = parser.parse_args()

    paths = [Path(args.left).resolve(), Path(args.right).resolve()]
    colors = ["tab:blue", "tab:orange"]
    figure, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    memory_axis = axes[1].twinx()

    for path, color in zip(paths, colors, strict=True):
        metadata = load_metadata(path)
        label = metadata["autoscaler"]
        x, throughput, start, end = load_series(path / "throughput_total_source.csv")
        axes[0].plot(x, throughput, label=label, color=color, marker=".", markersize=2)

        resource_x, cpu, memory = resource_timeline(path, start, end)
        axes[1].plot(resource_x, cpu, label=f"{label} CPU count", color=color)
        memory_axis.plot(
            resource_x,
            memory,
            label=f"{label} memory",
            color=color,
            linestyle="--",
        )

    axes[0].set_title("Q4 - Total Source Throughput")
    axes[0].set_ylabel("events/s")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    axes[1].set_title("CPU Count and Memory Allocation")
    axes[1].set_xlabel("Minutes since experiment start")
    axes[1].set_ylabel("CPU count")
    axes[1].set_ylim(bottom=0)
    axes[1].grid(True, alpha=0.3)
    memory_axis.set_ylabel("Memory (MB)")
    memory_axis.set_ylim(bottom=0)
    handles, labels = axes[1].get_legend_handles_labels()
    memory_handles, memory_labels = memory_axis.get_legend_handles_labels()
    axes[1].legend(handles + memory_handles, labels + memory_labels, loc="best")

    figure.suptitle("Q4 Justin vs DS2 Resource Comparison", fontsize=16)
    figure.tight_layout()
    output = (
        Path(args.output).resolve()
        if args.output
        else Path.cwd() / "compare-justin-ds2-resources.png"
    )
    figure.savefig(output, dpi=180)
    print(output)


if __name__ == "__main__":
    main()
