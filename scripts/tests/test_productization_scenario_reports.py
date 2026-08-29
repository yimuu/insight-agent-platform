import json
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
CHECKER = ROOT / "scripts/check-productization-scenario-reports.py"
FIXTURE_DIRECTORY = ROOT / "tests/fixtures/productization-reports/incomplete"
REVISION = "0" * 40


def run_checker(directory: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "python3",
            str(CHECKER),
            str(directory),
            *arguments,
            "--source-revision",
            REVISION,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


class ProductizationScenarioReportTests(unittest.TestCase):
    def test_allow_incomplete_accepts_a_closed_partial_report(self) -> None:
        result = run_checker(FIXTURE_DIRECTORY, "--allow-incomplete")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("complete_gate=false", result.stdout)

    def test_complete_gate_rejects_a_partial_report_set(self) -> None:
        result = run_checker(FIXTURE_DIRECTORY)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("report set differs", result.stderr)

    def test_passed_status_cannot_hide_a_required_not_run_check(self) -> None:
        source = FIXTURE_DIRECTORY / "deterministic-first-run.json"
        report = json.loads(source.read_text(encoding="utf-8"))
        report["status"] = "passed"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / source.name
            path.write_text(json.dumps(report), encoding="utf-8")
            result = run_checker(Path(directory), "--allow-incomplete")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("status=passed must exactly match", result.stderr)


if __name__ == "__main__":
    unittest.main()
