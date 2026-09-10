#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest
from unittest.mock import patch


MODULE_PATH = pathlib.Path(__file__).with_name("checkpoint-producer-controller.py")
SPEC = importlib.util.spec_from_file_location("checkpoint_producer_controller", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
controller = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = controller
SPEC.loader.exec_module(controller)


class CheckpointProducerControllerTest(unittest.TestCase):
    def test_parse_transaction(self) -> None:
        transaction = controller.parse_transaction(
            '---\ntransactionId: "tx-1"\nphase: "WAITING_PRODUCER_PAUSE"\n'
        )

        self.assertEqual(
            transaction,
            controller.Transaction(
                transaction_id="tx-1", phase="WAITING_PRODUCER_PAUSE"
            ),
        )

    @patch.object(controller, "acknowledge")
    @patch.object(controller, "control_producer")
    def test_pause_and_acknowledge(self, control_producer, acknowledge) -> None:
        transaction = controller.Transaction("tx-1", controller.PAUSE_PHASE)

        message = controller.reconcile(
            transaction, {}, "flink", "default", "/producer"
        )

        control_producer.assert_called_once_with("pause", "/producer")
        acknowledge.assert_called_once_with(
            "flink", "default", controller.PAUSE_ACK_ANNOTATION, "tx-1"
        )
        self.assertIn("paused producer", message)

    @patch.object(controller, "acknowledge")
    @patch.object(controller, "control_producer")
    def test_matching_pause_ack_is_idempotent(
        self, control_producer, acknowledge
    ) -> None:
        transaction = controller.Transaction("tx-1", controller.PAUSE_PHASE)

        controller.reconcile(
            transaction,
            {controller.PAUSE_ACK_ANNOTATION: "tx-1"},
            "flink",
            "default",
            "/producer",
        )

        control_producer.assert_not_called()
        acknowledge.assert_not_called()

    @patch.object(controller, "acknowledge")
    @patch.object(controller, "control_producer")
    def test_resume_and_acknowledge(self, control_producer, acknowledge) -> None:
        transaction = controller.Transaction("tx-1", controller.RESUME_PHASE)

        controller.reconcile(
            transaction,
            {controller.PAUSE_ACK_ANNOTATION: "tx-1"},
            "flink",
            "default",
            "/producer",
        )

        control_producer.assert_called_once_with("resume", "/producer")
        acknowledge.assert_called_once_with(
            "flink", "default", controller.RESUME_ACK_ANNOTATION, "tx-1"
        )

    @patch.object(controller, "acknowledge")
    @patch.object(controller, "control_producer")
    def test_abort_recovers_paused_producer(
        self, control_producer, acknowledge
    ) -> None:
        controller.reconcile(
            None,
            {controller.PAUSE_ACK_ANNOTATION: "tx-1"},
            "flink",
            "default",
            "/producer",
        )

        control_producer.assert_called_once_with("resume", "/producer")
        acknowledge.assert_called_once_with(
            "flink", "default", controller.RESUME_ACK_ANNOTATION, "tx-1"
        )

    @patch.object(controller, "acknowledge")
    @patch.object(controller, "control_producer")
    def test_failure_remains_paused(self, control_producer, acknowledge) -> None:
        transaction = controller.Transaction("tx-1", controller.FAILED_PHASE)

        message = controller.reconcile(
            transaction,
            {controller.PAUSE_ACK_ANNOTATION: "tx-1"},
            "flink",
            "default",
            "/producer",
        )

        control_producer.assert_not_called()
        acknowledge.assert_not_called()
        self.assertIn("fail-closed", message)

if __name__ == "__main__":
    unittest.main()
