"""Offline checks for the pause/acknowledgement recovery boundary."""

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import call, patch


path = Path(__file__).with_name("checkpoint-producer-controller.py")
spec = importlib.util.spec_from_file_location("checkpoint_producer_controller", path)
assert spec is not None and spec.loader is not None
controller = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = controller
spec.loader.exec_module(controller)


class CheckpointProducerControllerTest(unittest.TestCase):
    def test_parse_transaction(self) -> None:
        raw = '---\ntransactionId: "tx-1"\nphase: WAITING_PRODUCER_PAUSE\nproducerPauseRequired: true\n'
        self.assertEqual(
            controller.parse_transaction(raw),
            controller.Transaction("tx-1", controller.PAUSE_PHASE, True),
        )
        with self.assertRaises(ValueError):
            controller.parse_transaction("phase: WAITING_PRODUCER_PAUSE")

    def test_pause_records_intent_before_ack(self) -> None:
        steps = []
        with patch.object(controller, "annotate", side_effect=lambda *args: steps.append(args)), \
             patch.object(controller, "ensure_producer", side_effect=lambda *args, **kwargs: steps.append(kwargs)):
            controller.reconcile(
                controller.Transaction("tx-1", controller.PAUSE_PHASE, True),
                {}, "flink", "default", "producer",
            )
        self.assertEqual(
            steps,
            [
                ("flink", "default", controller.INTENT_ANNOTATION, "tx-1"),
                {"paused": True},
                ("flink", "default", controller.PAUSE_ACK_ANNOTATION, "tx-1"),
            ],
        )

    def test_abort_recovers_pause_without_ack(self) -> None:
        annotations = {controller.INTENT_ANNOTATION: "tx-1"}
        with patch.object(controller, "ensure_producer") as ensure, \
             patch.object(controller, "annotate") as annotate:
            controller.reconcile(None, annotations, "flink", "default", "producer")
        ensure.assert_called_once_with("producer", paused=False)
        annotate.assert_called_once_with(
            "flink", "default", controller.RESUME_ACK_ANNOTATION, "tx-1"
        )

    def test_new_ungated_transaction_recovers_old_pause(self) -> None:
        with patch.object(controller, "ensure_producer") as ensure, \
             patch.object(controller, "annotate") as annotate:
            controller.reconcile(
                controller.Transaction("tx-new", "WAITING_CHECKPOINT", False),
                {controller.INTENT_ANNOTATION: "tx-old"},
                "flink", "default", "producer",
            )
        ensure.assert_called_once_with("producer", paused=False)
        annotate.assert_called_once_with(
            "flink", "default", controller.RESUME_ACK_ANNOTATION, "tx-old"
        )

    def test_retry_rejects_old_ack_and_keeps_producer_paused(self) -> None:
        annotations = {
            controller.INTENT_ANNOTATION: "tx-old",
            controller.PAUSE_ACK_ANNOTATION: "tx-old",
        }
        with patch.object(controller, "ensure_producer") as ensure, \
             patch.object(controller, "annotate") as annotate:
            controller.reconcile(
                controller.Transaction("tx-new", controller.PAUSE_PHASE, True),
                annotations, "flink", "default", "producer",
            )
        ensure.assert_called_once_with("producer", paused=True)
        self.assertEqual(
            annotate.call_args_list,
            [
                call("flink", "default", controller.INTENT_ANNOTATION, "tx-new"),
                call("flink", "default", controller.PAUSE_ACK_ANNOTATION, "tx-new"),
            ],
        )

    def test_resume_ack_requires_successful_unpause(self) -> None:
        transaction = controller.Transaction("tx-1", controller.RESUME_PHASE, True)
        with patch.object(controller, "ensure_producer", side_effect=RuntimeError("Docker unavailable")), \
             patch.object(controller, "annotate") as annotate:
            with self.assertRaises(RuntimeError):
                controller.reconcile(transaction, {}, "flink", "default", "producer")
        annotate.assert_not_called()

    def test_pause_ack_requires_successful_pause(self) -> None:
        transaction = controller.Transaction("tx-1", controller.PAUSE_PHASE, True)
        with patch.object(controller, "ensure_producer", side_effect=RuntimeError("Docker unavailable")), \
             patch.object(controller, "annotate") as annotate:
            with self.assertRaises(RuntimeError):
                controller.reconcile(
                    transaction,
                    {controller.INTENT_ANNOTATION: "tx-1"},
                    "flink", "default", "producer",
                )
        annotate.assert_not_called()

    def test_failed_transaction_remains_fail_closed(self) -> None:
        with patch.object(controller, "ensure_producer") as ensure:
            controller.reconcile(
                controller.Transaction("tx-1", "FAILED", True),
                {controller.INTENT_ANNOTATION: "tx-1"},
                "flink", "default", "producer",
            )
        ensure.assert_not_called()

    def test_docker_pause_is_idempotent_and_verified(self) -> None:
        with patch.object(controller, "run", side_effect=["true false", "", "true true"]) as run:
            controller.ensure_producer("producer", paused=True)
        self.assertEqual(run.call_args_list[1], call(["docker", "pause", "producer"]))
        with patch.object(controller, "run", return_value="true true") as run:
            controller.ensure_producer("producer", paused=True)
        run.assert_called_once()

    def test_docker_resume_is_verified(self) -> None:
        with patch.object(controller, "run", side_effect=["true true", "", "true false"]) as run:
            controller.ensure_producer("producer", paused=False)
        self.assertEqual(run.call_args_list[1], call(["docker", "unpause", "producer"]))


if __name__ == "__main__":
    unittest.main()
