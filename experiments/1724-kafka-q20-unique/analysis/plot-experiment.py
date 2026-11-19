#!/usr/bin/env python3
"""Plot one exported experiment data directory."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

SCRIPT_DIR = Path(__file__).resolve().parent


def load_series(path: Path) -> tuple[list[datetime], list[float]]:
    times: list[datetime] = []
    values: list[float] = []
    if not path.exists():
        return times, values
    with path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            value = row.get("value", "")
            if value in ("", "NaN", "nan"):
                continue
            times.append(datetime.fromtimestamp(float(row["timestamp"]), tz=timezone.utc))
            values.append(float(value))
    return times, values


def load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def step_series(rows: list[dict[str, str]], value_key: str) -> tuple[list[datetime], list[float]]:
    times: list[datetime] = []
    values: list[float] = []
    for row in rows:
        times.append(datetime.fromtimestamp(float(row["timestamp"]), tz=timezone.utc))
        values.append(float(row[value_key]))
    return times, values


def value_at(rows: list[dict[str, str]], timestamp: float, value_key: str) -> str:
    current = rows[0][value_key] if rows else "0"
    for row in rows:
        if float(row["timestamp"]) > timestamp:
            break
        current = row[value_key]
    return current


def drop_short_segments(rows: list[dict[str, str]], end_ts: float, min_seconds: float = 60.0) -> list[dict[str, str]]:
    """Remove rows whose segment (time until next change) is shorter than min_seconds."""
    if not rows:
        return rows
    timestamps = [float(r["timestamp"]) for r in rows] + [end_ts]
    return [row for row, ts, next_ts in zip(rows, timestamps, timestamps[1:]) if next_ts - ts >= min_seconds]


def compress_value_rows(rows: list[dict[str, str]], value_key: str) -> list[dict[str, str]]:
    compressed: list[dict[str, str]] = []
    previous: str | None = None
    for row in rows:
        current = row[value_key]
        if current == previous:
            continue
        compressed.append(row)
        previous = current
    return compressed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiment_dir")
    parser.add_argument("--output")
    args = parser.parse_args()

    experiment_dir = Path(args.experiment_dir).resolve()
    metadata = json.loads((experiment_dir / "metadata.json").read_text(encoding="utf-8"))

    throughput_t, throughput_v = load_series(experiment_dir / "throughput_total_source.csv")
    tm_t, tm_v = load_series(experiment_dir / "tm_count.csv")
    join_p_t, join_p_v = load_series(experiment_dir / "join_parallelism.csv")
    busy_t, busy_v = load_series(experiment_dir / "join_busy_pct.csv")
    hit_t, hit_v = load_series(experiment_dir / "join_cache_hit_rate_pct.csv")
    lag_bid_t, lag_bid_v = load_series(experiment_dir / "kafka_lag_bid.csv")
    lag_auc_t, lag_auc_v = load_series(experiment_dir / "kafka_lag_auction.csv")

    memory_rows = load_rows(experiment_dir / "memory_timeline.csv")
    rescale_rows = load_rows(experiment_dir / "rescale_windows.csv")

    fig, axes = plt.subplots(4, 1, figsize=(18, 13), sharex=True)
    fig.suptitle(
        f"{metadata['job_name']}   [{metadata['autoscaler']}]\n"
        "Total Source Throughput",
        fontsize=20,
        fontweight="bold",
    )

    axes[0].plot(throughput_t, throughput_v, label="Total throughput")
    axes[0].set_ylabel("Throughput (events/s)", fontsize=15)
    axes[0].set_ylim(bottom=0)
    axes[0].legend(loc="upper right", fontsize=12)
    axes[0].grid(True, alpha=0.3)

    axes[1].set_title("CPU & Memory Usage", fontsize=20)
    axes[1].plot(tm_t, tm_v, label="CPU (# TMs)", linewidth=2)
    axes[1].set_ylabel("CPU count (# task managers)", fontsize=15)
    axes[1].set_ylim(bottom=0)
    axes[1].grid(True, alpha=0.3)
    mem_t, mem_v = step_series(memory_rows, "memory_mb")
    if mem_t:
        mem_ax = axes[1].twinx()
        uses_memory_levels = metadata.get("autoscaler") in ("justin", "a4s")
        mem_label = "Memory Level" if uses_memory_levels else "Memory (MB)"
        mem_ax.step(mem_t, mem_v, where="post", linestyle=":", linewidth=2, label=mem_label)
        mem_ax.set_ylabel(mem_label, fontsize=15)
        left_handles, left_labels = axes[1].get_legend_handles_labels()
        right_handles, right_labels = mem_ax.get_legend_handles_labels()
        axes[1].legend(left_handles + right_handles, left_labels + right_labels, loc="upper left", fontsize=12)
    else:
        axes[1].legend(loc="upper left", fontsize=12)

    axes[2].set_title("Kafka Source Lag", fontsize=20)
    axes[2].plot(lag_auc_t, lag_auc_v, label="Source: auction_kafka")
    axes[2].plot(lag_bid_t, lag_bid_v, label="Source: bid_kafka")
    axes[2].set_ylabel("Lag (events)", fontsize=15)
    axes[2].set_ylim(bottom=0)
    axes[2].legend(loc="upper right", fontsize=12)
    axes[2].grid(True, alpha=0.3)

    axes[3].set_title("Join Busy Time", fontsize=20)
    axes[3].plot(busy_t, busy_v, color="tab:green", label="busy%")
    axes[3].set_ylabel("Busy (%)", fontsize=15)
    axes[3].set_ylim(bottom=0)
    axes[3].grid(True, alpha=0.3)
    cache_ax = axes[3].twinx()
    cache_ax.plot(hit_t, hit_v, color="tab:orange", linestyle=":", label="cache hit rate")
    cache_ax.set_ylabel("Cache hit rate (%)", fontsize=15)
    handles, labels = axes[3].get_legend_handles_labels()
    handles2, labels2 = cache_ax.get_legend_handles_labels()
    axes[3].legend(handles + handles2, labels + labels2, loc="upper right", fontsize=12)

    if throughput_t:
        end_ts = throughput_t[-1].timestamp()
        join_rows = drop_short_segments(
            compress_value_rows(
                [
                    {"timestamp": str(point.timestamp()), "value": str(value)}
                    for point, value in zip(join_p_t, join_p_v)
                ],
                "value",
            ),
            end_ts,
        )
        memory_segments = compress_value_rows(memory_rows, "memory_level") if memory_rows else []
        segment_points = sorted(
            {
                *[float(row["timestamp"]) for row in join_rows],
                *[float(row["timestamp"]) for row in memory_segments],
                throughput_t[-1].timestamp(),
            }
        )

        # vertical dotted line at each parallelism transition (all join_rows except the first,
        # which is just the initial state at start_ts)
        for row in join_rows[1:]:
            t = datetime.fromtimestamp(float(row["timestamp"]), tz=timezone.utc)
            for ax in axes:
                ax.axvline(t, color="black", linestyle=":", linewidth=1.2, alpha=0.6, zorder=1)

        y_max = max(throughput_v) if throughput_v else 1.0
        for left, right in zip(segment_points[:-1], segment_points[1:]):
            if right <= left:
                continue
            mid = (left + right) / 2
            p_value = value_at(join_rows, mid, "value")
            label = f"p={int(float(p_value))}"
            if memory_segments:
                label += f" ml={value_at(memory_segments, mid, 'memory_level')}"
            axes[0].text(
                datetime.fromtimestamp(mid, tz=timezone.utc),
                y_max * 0.97,
                label,
                ha="center",
                va="top",
                fontsize=11,
                bbox={"boxstyle": "round,pad=0.2", "facecolor": "white", "alpha": 0.8},
            )

    axes[3].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=timezone.utc))
    axes[3].set_xlabel("Time (UTC)", fontsize=15)
    fig.autofmt_xdate()
    fig.tight_layout(rect=(0, 0, 1, 0.96))

    output = Path(args.output).resolve() if args.output else experiment_dir / "plot.png"
    fig.savefig(output, dpi=180)
    print(output)


if __name__ == "__main__":
    main()
