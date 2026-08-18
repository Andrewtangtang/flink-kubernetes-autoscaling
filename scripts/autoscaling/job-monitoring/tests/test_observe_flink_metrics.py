from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

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
            kafka_source_offsets_total=None,
            replay_progress_basis="current_attempt_records",
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

    def test_labels_kafka_offset_progress_as_rescale_safe(self) -> None:
        snapshot = observe_flink_metrics.MetricsSnapshot(
            timestamp="2026-08-17T00:00:00+00:00",
            job_id="0123456789abcdef",
            job_name="q20",
            job_state="RUNNING",
            rate_window="30s",
            cpu_rate_window="2m",
            source_records_out_per_second=0.0,
            source_records_out_total=53_309_338.0,
            kafka_source_offsets_total=98_000_000.0,
            replay_progress_basis="kafka_current_offsets",
            processed_events_estimate=100_000_000,
            total_events=100_000_000,
            replay_progress_percent=100.0,
            pods=[],
            tasks=[],
        )

        rendered = observe_flink_metrics.render_snapshot(snapshot, summary_only=True)

        self.assertIn("100,000,000/100,000,000 events (100.00%)", rendered)
        self.assertIn("Kafka reader offsets=98,000,000 (rescale-safe)", rendered)
        self.assertNotIn("source records=53,309,338", rendered)

    def test_kafka_offset_metric_pattern_preserves_topic_and_partition(self) -> None:
        metric_name = (
            "1.Source__bid_kafka_1.KafkaSourceReader.topic.nexmark-bid."
            "partition.23.currentOffset"
        )

        match = observe_flink_metrics.KAFKA_CURRENT_OFFSET_PATTERN.match(metric_name)

        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual("nexmark-bid", match.group("topic"))
        self.assertEqual("23", match.group("partition"))

    @patch.object(observe_flink_metrics, "get_json")
    def test_sums_unique_kafka_partition_offsets_across_sources(self, get_json) -> None:
        bid_metric = (
            "Source__bid.KafkaSourceReader.topic.nexmark-bid."
            "partition.0.currentOffset"
        )
        auction_metric = (
            "Source__auction.KafkaSourceReader.topic.nexmark-auction."
            "partition.0.currentOffset"
        )

        def response(url: str):
            if url.endswith("/jobs/job-id"):
                return {
                    "vertices": [
                        {"id": "bid", "name": "Source: bid"},
                        {"id": "auction", "name": "Source: auction"},
                        {"id": "join", "name": "Join"},
                    ]
                }
            if "/vertices/bid/" in url and "?" not in url:
                return [{"id": bid_metric}]
            if "/vertices/auction/" in url and "?" not in url:
                return [{"id": auction_metric}]
            if "/vertices/bid/" in url:
                return [{"id": bid_metric, "max": 92_000_000.0}]
            if "/vertices/auction/" in url:
                return [{"id": auction_metric, "max": 6_000_000.0}]
            self.fail(f"unexpected URL: {url}")

        get_json.side_effect = response

        client = observe_flink_metrics.FlinkClient("http://flink")

        self.assertEqual(98_000_000.0, client.kafka_source_offsets_total("job-id"))


if __name__ == "__main__":
    unittest.main()
