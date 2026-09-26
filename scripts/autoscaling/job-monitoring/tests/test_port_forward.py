from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "port-forward.sh"


class PortForwardTest(unittest.TestCase):
    def test_start_status_stop_only_managed_processes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            kubectl = root / "kubectl"
            kubectl.write_text(
                "#!/usr/bin/env bash\n"
                "if [[ $1 == get ]]; then exit 0; fi\n"
                "if [[ $1 == port-forward ]]; then exec sleep 30; fi\n"
            )
            kubectl.chmod(0o755)
            curl = root / "curl"
            curl.write_text("#!/usr/bin/env bash\nexit 0\n")
            curl.chmod(0o755)
            ps = root / "ps"
            ps.write_text(
                "#!/usr/bin/env bash\n"
                "echo \"${MOCK_PS_ARGS:-kubectl port-forward svc/flink-rest "
                "svc/prom-kube-prometheus-stack-prometheus svc/prom-grafana}\"\n"
            )
            ps.chmod(0o755)
            env = os.environ.copy()
            env["PATH"] = f"{directory}:{env['PATH']}"
            env["PORT_FORWARD_RUNTIME_DIR"] = str(root / "runtime")

            def run(action: str) -> subprocess.CompletedProcess[str]:
                return subprocess.run(
                    [str(SCRIPT), action],
                    env=env,
                    text=True,
                    capture_output=True,
                    timeout=10,
                    check=True,
                )

            try:
                self.assertIn("flink-rest: http://127.0.0.1:18082", run("start").stdout)
                self.assertIn("flink-rest: pid", run("status").stdout)
            finally:
                run("stop")
            self.assertIn("flink-rest: not running", run("status").stdout)

            unrelated = subprocess.Popen(["sleep", "30"])
            try:
                (root / "runtime" / "flink-rest.pid").write_text(str(unrelated.pid))
                env["MOCK_PS_ARGS"] = "unrelated process"
                run("stop")
                self.assertIsNone(unrelated.poll())
            finally:
                unrelated.terminate()
                unrelated.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
