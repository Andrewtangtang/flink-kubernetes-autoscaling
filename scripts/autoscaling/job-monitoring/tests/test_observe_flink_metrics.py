from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "observe-flink-metrics.py"
SPEC = importlib.util.spec_from_file_location("observe_flink_metrics", SCRIPT)
assert SPEC and SPEC.loader
observe_flink_metrics = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = observe_flink_metrics
SPEC.loader.exec_module(observe_flink_metrics)


class MetricsRenderingTest(unittest.TestCase):
    def test_labels_live_rate_averaging_windows(self) -> None:
        snapshot = observe_flink_metrics.MetricsSnapshot(
            timestamp="2026-07-27T00:00:00+00:00",
            job_id="0123456789abcdef",
            job_name="q18",
            job_state="RUNNING",
            rate_window="30s",
            cpu_rate_window="2m",
            source_records_out_per_second=90_000.0,
            source_records_out_total=1_000_000.0,
            processed_events_estimate=None,
            total_events=None,
            replay_progress_percent=None,
            pods=[],
            tasks=[],
        )

        rendered = observe_flink_metrics.render_snapshot(
            snapshot,
            summary_only=True,
        )

        self.assertIn("total source out (30s avg)=90.0K/s", rendered)
        self.assertIn("task record rates=30s; pod CPU=2m", rendered)


if __name__ == "__main__":
    unittest.main()
