from __future__ import annotations

import json
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
WRITER = ROOT / "scripts" / "write-productization-north-star-report.py"
CHECKER = ROOT / "scripts" / "check-productization-north-star-report.py"
REVISION = "a" * 40
STARTED_EPOCH = 1_788_163_200


class ProductizationNorthStarReportTests(unittest.TestCase):
    def build_report(self, directory: pathlib.Path) -> pathlib.Path:
        marker = directory / "marker.json"
        report = directory / "report.json"
        marker.write_text(json.dumps({
            "schema_version": 1,
            "report_kind": "insight.productization.first-run-marker/v1",
            "run_id": "run_0198f1c3-8f49-7c3e-b1f3-773c28367b90",
            "state": "succeeded",
            "result_verified": True,
            "completed_at": "2026-08-31T08:05:00Z",
        }), encoding="utf-8")
        result = subprocess.run([
            "python3", str(WRITER), "--marker", str(marker), "--output", str(report),
            "--source-revision", REVISION, "--started-epoch", str(STARTED_EPOCH),
            "--fresh-checkout",
        ], cwd=ROOT, check=False, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        return report

    def check(self, report: pathlib.Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run([
            "python3", str(CHECKER), str(report), "--source-revision", REVISION,
        ], cwd=ROOT, check=False, capture_output=True, text=True)

    def test_closed_report_passes(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            report = self.build_report(pathlib.Path(raw_directory))
            result = self.check(report)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("gate=passed", result.stdout)

    def test_elapsed_time_cannot_be_underreported(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            report = self.build_report(pathlib.Path(raw_directory))
            value = json.loads(report.read_text(encoding="utf-8"))
            value["elapsed_to_first_run_ms"] -= 1
            report.write_text(json.dumps(value), encoding="utf-8")
            result = self.check(report)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("timestamp delta", result.stderr)

    def test_slow_or_non_fresh_report_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            report = self.build_report(pathlib.Path(raw_directory))
            value = json.loads(report.read_text(encoding="utf-8"))
            value["environment"]["fresh_checkout"] = False
            report.write_text(json.dumps(value), encoding="utf-8")
            result = self.check(report)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("fresh_checkout", result.stderr)


if __name__ == "__main__":
    unittest.main()
