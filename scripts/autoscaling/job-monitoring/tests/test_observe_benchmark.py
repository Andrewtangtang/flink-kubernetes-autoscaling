import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

SCRIPT = Path(__file__).resolve().parents[1] / "observe-benchmark.py"
SPEC = importlib.util.spec_from_file_location("observe_benchmark", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
observe_benchmark = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = observe_benchmark
SPEC.loader.exec_module(observe_benchmark)


class TransactionParsingTest(unittest.TestCase):
    def test_parses_completed_transaction(self) -> None:
        transaction = observe_benchmark.parse_transaction("""
phase: COMPLETED
decisionTimestamp: 2026-08-18T10:00:00Z
restoredRunningTimestamp: 100000
completedCheckpointId: 17
observedRestartDurationMillis: 60379
error: null
""")

        self.assertEqual("COMPLETED", transaction.phase)
        self.assertEqual(100000, transaction.restored_running_timestamp)
        self.assertEqual("17", transaction.checkpoint_id)
        self.assertEqual("-", transaction.error)

    @patch.object(observe_benchmark, "get_json")
    def test_reads_producer_source_rate(self, get_json) -> None:
        source_rates = {
            "person-source": [300.0] * 4,
            "auction-source": [1_800.0] * 4,
            "bid-source": [12_900.0] * 4,
        }

        def response(url: str):
            if url.endswith("/jobs/overview"):
                return {
                    "jobs": [
                        {
                            "jid": "producer-id",
                            "name": "insert_kafka_unique",
                            "state": "RUNNING",
                            "start-time": 1,
                        }
                    ]
                }
            if url.endswith("/jobs/producer-id"):
                return {
                    "vertices": [
                        {"id": vertex_id, "name": f"Source: {vertex_id}"}
                        for vertex_id in source_rates
                    ]
                    + [
                        {"id": "sink-id", "name": "Sink: Kafka"},
                    ]
                }
            parsed = urlparse(url)
            for vertex_id, rates in source_rates.items():
                if parsed.path.endswith(f"/vertices/{vertex_id}/metrics"):
                    metric_names = [
                        f"{index}.Source__{vertex_id}[1].numRecordsOutPerSecond"
                        for index in range(len(rates))
                    ]
                    if not parsed.query:
                        return [
                            {"id": name} for name in metric_names
                        ] + [
                            {
                                "id": (
                                    f"0.Sink__{vertex_id}[2]."
                                    "numRecordsOutPerSecond"
                                )
                            }
                        ]
                    requested = parse_qs(parsed.query)["get"][0].split(",")
                    self.assertEqual(metric_names, sorted(requested))
                    return [
                        {"id": name, "value": str(rate)}
                        for name, rate in zip(metric_names, rates)
                    ]
            self.fail(f"unexpected URL: {url}")

        get_json.side_effect = response

        producer = observe_benchmark.producer_state("http://producer")

        self.assertTrue(producer.active)
        self.assertEqual(60_000.0, producer.rate)

    @patch.object(observe_benchmark, "get_json")
    def test_does_not_use_chained_sink_rate_as_producer_rate(self, get_json) -> None:
        def response(url: str):
            if url.endswith("/jobs/overview"):
                return {
                    "jobs": [
                        {
                            "jid": "producer-id",
                            "name": "insert_kafka_unique",
                            "state": "RUNNING",
                            "start-time": 1,
                        }
                    ]
                }
            if url.endswith("/jobs/producer-id"):
                return {
                    "vertices": [{"id": "source-id", "name": "Source: Nexmark"}]
                }
            if url.endswith("/vertices/source-id/metrics"):
                return [
                    {
                        "id": (
                            "0.Kafka_Writer__nexmark[2]."
                            "numRecordsOutPerSecond"
                        )
                    }
                ]
            self.fail(f"unexpected URL: {url}")

        get_json.side_effect = response

        producer = observe_benchmark.producer_state("http://producer")

        self.assertTrue(producer.active, producer.error)
        self.assertIsNone(producer.rate)


class StabilityTrackerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tracker = observe_benchmark.StabilityTracker(
            target_rate=60_000.0,
            stabilization_seconds=60.0,
            window_seconds=120.0,
            required_windows=3,
            input_tolerance=0.05,
            lag_growth_tolerance=0.01,
            sample_interval=5.0,
        )
        self.transaction = observe_benchmark.TransactionInfo(
            phase="COMPLETED",
            decision_timestamp="2026-08-18T10:00:00Z",
            restored_running_timestamp=0,
        )
        self.producer = observe_benchmark.ProducerState(
            active=True,
            rate=60_000.0,
        )

    def observe_range(self, stop: int, lag_delta_per_second: float) -> None:
        for now in range(0, stop + 1, 5):
            lag = 1_000_000.0 + lag_delta_per_second * now
            self.tracker.observe(
                float(now),
                "RUNNING",
                self.transaction,
                self.producer,
                lag,
                "P4|M2",
            )

    def test_declares_capacity_stable_after_three_passing_windows(self) -> None:
        self.observe_range(420, lag_delta_per_second=-1_000.0)

        self.assertEqual(3, self.tracker.pass_streak)
        self.assertEqual(3, len(self.tracker.windows))
        self.assertTrue(all(window.passed for window in self.tracker.windows))
        self.assertEqual(
            "CAPACITY_STABLE_CATCHING_UP",
            self.tracker.status(
                420.0,
                "RUNNING",
                self.transaction,
                self.producer,
                580_000.0,
            ),
        )
        stopped = observe_benchmark.ProducerState(active=False, rate=None)
        self.assertEqual(
            "CAPACITY_STABLE_CATCHING_UP",
            self.tracker.status(
                430.0,
                "RUNNING",
                self.transaction,
                stopped,
                580_000.0,
            ),
        )

    def test_rejects_a_window_with_persistent_lag_growth(self) -> None:
        self.observe_range(180, lag_delta_per_second=1_000.0)

        self.assertEqual(0, self.tracker.pass_streak)
        self.assertEqual(1, len(self.tracker.windows))
        self.assertFalse(self.tracker.windows[0].passed)
        self.assertFalse(self.tracker.windows[0].lag_ok)
        self.assertAlmostEqual(1_000.0, self.tracker.windows[0].lag_slope)

    def test_new_transaction_resets_completed_windows(self) -> None:
        self.observe_range(180, lag_delta_per_second=-1_000.0)
        new_transaction = observe_benchmark.TransactionInfo(
            phase="COMPLETED",
            decision_timestamp="2026-08-18T10:10:00Z",
            restored_running_timestamp=180_000,
        )

        self.tracker.observe(
            240.0,
            "RUNNING",
            new_transaction,
            self.producer,
            500_000.0,
            "P6|M1",
        )

        self.assertEqual(0, self.tracker.pass_streak)
        self.assertEqual([], self.tracker.windows)
        self.assertEqual(new_transaction.key, self.tracker.transaction_key)

    def test_late_observer_does_not_create_empty_historical_windows(self) -> None:
        self.tracker.observe(
            600.0,
            "RUNNING",
            self.transaction,
            self.producer,
            500_000.0,
            "P4|M2",
        )

        self.assertEqual(600.0, self.tracker.stable_start)
        self.assertEqual([], self.tracker.windows)
        self.assertEqual(0, self.tracker.next_window_index)

    def test_unknown_producer_metrics_are_not_reported_as_stopped(self) -> None:
        unknown = observe_benchmark.ProducerState(
            active=None,
            rate=None,
            error="connection timed out",
        )

        self.assertEqual(
            "WAITING_FOR_PRODUCER_METRICS",
            self.tracker.status(
                60.0,
                "RUNNING",
                self.transaction,
                unknown,
                500_000.0,
            ),
        )


if __name__ == "__main__":
    unittest.main()
