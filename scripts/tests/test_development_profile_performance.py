from __future__ import annotations

import json
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
BUILDER = ROOT / "scripts/build-development-profile-performance.py"


class DevelopmentProfilePerformanceTests(unittest.TestCase):
    def command(self, output: pathlib.Path) -> list[str]:
        return [
            "python3", str(BUILDER), "--version", "1.2.3", "--git-commit", "a" * 40,
            "--profile-digest", "sha256:" + "b" * 64, "--features", "",
            "--cpu-count", "4", "--memory-bytes", str(8 * 1024**3), "--network-mbps", "100",
            "--cold-ready-seconds", "299", "--warm-ready-seconds", "59",
            "--download-seconds", "20", "--download-bytes", "1024",
            "--idle-rss-bytes", str(6 * 1024**3), "--idle-cpu-percent", "10",
            "--idle-stabilization-seconds", "300", "--project-disk-bytes", str(8 * 1024**3),
            "--source-compilations", "0", "--output", str(output),
        ]

    def test_passed_report_is_closed_and_keeps_production_levels_not_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = pathlib.Path(temporary) / "report.json"
            result = subprocess.run(self.command(output), capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_bytes())
            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["profile"]["name"], "starter")
            self.assertEqual(report["qualification"], {"L4": "not_run", "L5": "not_run", "L6": "not_run"})
            self.assertEqual(len(report["gates"]), 10)

    def test_budget_failure_writes_failed_evidence_and_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = pathlib.Path(temporary) / "report.json"
            command = self.command(output)
            command[command.index("--warm-ready-seconds") + 1] = "61"
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            report = json.loads(output.read_bytes())
            self.assertEqual(report["status"], "failed")
            self.assertIn("failed", {gate["status"] for gate in report["gates"]})

    def test_noncanonical_or_unknown_features_fail_without_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = pathlib.Path(temporary) / "report.json"
            for features in ("model,context", "model,model", "unknown"):
                command = self.command(output)
                command[command.index("--features") + 1] = features
                result = subprocess.run(command, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0, features)
                self.assertFalse(output.exists(), features)


if __name__ == "__main__":
    unittest.main()
