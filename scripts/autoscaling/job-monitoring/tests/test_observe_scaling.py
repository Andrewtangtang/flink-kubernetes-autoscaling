from __future__ import annotations

import base64
import gzip
import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import yaml

SCRIPT = Path(__file__).resolve().parents[1] / "observe-scaling.py"
SPEC = importlib.util.spec_from_file_location("observe_scaling", SCRIPT)
assert SPEC and SPEC.loader
observe_scaling = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = observe_scaling
SPEC.loader.exec_module(observe_scaling)


def encoded_state(payload: dict) -> str:
    raw = yaml.safe_dump(payload, sort_keys=False).encode()
    return base64.b64encode(gzip.compress(raw)).decode()


class ParallelismDecisionHistoryTest(unittest.TestCase):
    def test_parses_window_average_and_reconstructs_raw_parallelism(self) -> None:
        timestamp = "2026-07-27T04:04:08.880069Z"
        payload = {
            "90bea66de1c231edf33913ecd54406c1": {
                timestamp: {
                    "currentParallelism": 4,
                    "newParallelism": 6,
                    "metrics": {
                        "TRUE_PROCESSING_RATE": {
                            "current": float("nan"),
                            "average": 86253.093,
                        },
                        "TARGET_DATA_RATE": {
                            "current": float("nan"),
                            "average": 87778.994,
                        },
                        "CATCH_UP_DATA_RATE": {
                            "current": 438.02,
                            "average": float("nan"),
                        },
                        "EXPECTED_PROCESSING_RATE": {
                            "current": 110162.0,
                            "average": float("nan"),
                        },
                        "LAG": {
                            "current": 1234.0,
                            "average": float("nan"),
                        },
                    },
                }
            }
        }
        configmap = {
            "data": {
                "scalingHistory": encoded_state(payload),
            }
        }

        with patch.object(
            observe_scaling,
            "find_autoscaler_configmap",
            return_value=("autoscaler-flink", configmap),
        ):
            name, events = observe_scaling.fetch_parallelism_decision_history("flink")

        self.assertEqual("autoscaler-flink", name)
        self.assertEqual(1, len(events))
        decision = events[0].vertices[0]
        self.assertEqual(4, decision.current_parallelism)
        self.assertEqual(6, decision.recommended_parallelism)
        self.assertAlmostEqual(86253.093, decision.window_average_capacity)
        self.assertAlmostEqual(87778.994, decision.target_data_rate)
        self.assertAlmostEqual(438.02, decision.catch_up_data_rate)
        self.assertAlmostEqual(110162.0, decision.target_processing_capacity)
        self.assertAlmostEqual(
            4 * 110162.0 / 86253.093,
            decision.raw_parallelism,
        )

        output = io.StringIO()
        with redirect_stdout(output):
            observe_scaling.print_parallelism_decision_event(events, 0)
        rendered = output.getvalue()
        self.assertIn("86253.1", rendered)
        self.assertIn("87779.0", rendered)
        self.assertIn("5.11", rendered)

    def test_raw_parallelism_requires_finite_positive_capacity(self) -> None:
        self.assertIsNone(observe_scaling.compute_raw_parallelism(4, None, 100_000.0))
        self.assertIsNone(observe_scaling.compute_raw_parallelism(4, 0.0, 100_000.0))
        self.assertIsNone(observe_scaling.compute_raw_parallelism(4, 80_000.0, None))


class NoRescaleWindowTest(unittest.TestCase):
    @staticmethod
    def snapshot(period: int, horizontal: bool = False, vertical: bool = False):
        return observe_scaling.ScalingSnapshot(
            timestamp=f"2026-08-22 00:{period:02d}:00,000",
            period=period,
            vertices=[
                observe_scaling.VertexInfo(
                    vertex_id="vertex",
                    avg_throughput=10_000.0,
                    parallelism=1,
                    memory_level=0,
                    vertical_scaling=vertical,
                    horizontal_scaling=horizontal,
                    avg_cache_hit_rate=0.9,
                    avg_state_latency=100.0,
                )
            ],
        )

    def test_counts_consecutive_windows_after_latest_rescale(self) -> None:
        snapshots = [
            self.snapshot(0, vertical=True),
            self.snapshot(1),
            self.snapshot(2),
            self.snapshot(3, horizontal=True),
            self.snapshot(4),
            self.snapshot(5),
        ]

        self.assertEqual(0, observe_scaling.consecutive_no_rescale_windows(snapshots, 0))
        self.assertEqual(2, observe_scaling.consecutive_no_rescale_windows(snapshots, 2))
        self.assertEqual(0, observe_scaling.consecutive_no_rescale_windows(snapshots, 3))
        self.assertEqual(2, observe_scaling.consecutive_no_rescale_windows(snapshots, 5))

    def test_prints_window_streak_without_classifying_stability(self) -> None:
        snapshots = [self.snapshot(0, vertical=True), self.snapshot(1)]
        output = io.StringIO()

        with redirect_stdout(output):
            observe_scaling.print_snapshot(snapshots, 1)

        rendered = output.getvalue()
        self.assertIn("Consecutive windows without rescaling: 1", rendered)
        self.assertNotIn("PASS", rendered)
        self.assertNotIn("FAIL", rendered)


if __name__ == "__main__":
    unittest.main()
