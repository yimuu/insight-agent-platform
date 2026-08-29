from __future__ import annotations

import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run-productization-base-journey.sh"


class ProductizationBaseJourneyRunnerTests(unittest.TestCase):
    def run_runner(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(RUNNER), *arguments],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )

    def test_help_describes_fresh_real_profile_and_evidence(self) -> None:
        result = self.run_runner("--help")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("fresh base-profile", result.stdout)
        self.assertIn("--report-directory", result.stdout)
        self.assertIn("--keep-dependencies", result.stdout)

    def test_unknown_option_fails_before_build_or_mutation(self) -> None:
        result = self.run_runner("--unknown")
        self.assertEqual(result.returncode, 2)
        self.assertIn("unsupported option: --unknown", result.stderr)
        self.assertNotIn("Compiling", result.stderr)

    def test_existing_project_path_is_rejected_before_build(self) -> None:
        result = self.run_runner("--project", str(ROOT))
        self.assertEqual(result.returncode, 2)
        self.assertIn("does not already exist", result.stderr)
        self.assertNotIn("Compiling", result.stderr)


if __name__ == "__main__":
    unittest.main()
