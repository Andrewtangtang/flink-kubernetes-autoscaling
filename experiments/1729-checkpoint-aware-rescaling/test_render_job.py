"""Small offline check for both checkpoint pilot policy variants."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


EXPERIMENT = Path(__file__).resolve().parent


class RenderJobTest(unittest.TestCase):
    def test_both_policies_render_without_lab_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            for policy, enabled in (("justin", "true"), ("ds2", "false")):
                output = Path(directory) / f"{policy}.yaml"
                subprocess.run(
                    [
                        sys.executable,
                        str(EXPERIMENT / "render-job.py"),
                        "--policy", policy,
                        "--run-id", f"pilot-{policy}",
                        "--image", "registry.example/flink:checkpoint",
                        "--kafka-bootstrap", "kafka.example:9092",
                        "--checkpoint-pvc", "flink-state",
                        "--output", str(output),
                    ],
                    check=True,
                )
                manifest = output.read_text()
                self.assertIn(f"job.autoscaler.justin.enabled: '{enabled}'", manifest)
                self.assertIn("claimName: flink-state", manifest)
                self.assertIn("taskmanager.numberOfTaskSlots: '8'", manifest)
                self.assertIn("job.autoscaler.stabilization.interval: 3m", manifest)
                self.assertIn(f"/mnt/flink-state/pilot-{policy}/checkpoints", manifest)
                self.assertIn("kafka.example:9092", manifest)
                self.assertIn("cbc357ccb763df2852fee8c4fc7d55f2:12", manifest)
                self.assertNotIn("producer-pause", manifest)
                self.assertNotIn("__", manifest)


if __name__ == "__main__":
    unittest.main()
