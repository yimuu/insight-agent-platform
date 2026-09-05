from __future__ import annotations

import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run-productization-journey.sh"


class ProductizationJourneyRunnerTests(unittest.TestCase):
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
        self.assertIn("fresh selected-profile", result.stdout)
        self.assertIn("--report-directory", result.stdout)
        self.assertIn("--features <list|all>", result.stdout)
        self.assertIn("--keep-dependencies", result.stdout)
        self.assertIn("--north-star-report", result.stdout)

    def test_console_path_installs_exact_dependencies_before_build(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        install = 'pnpm --dir "$workspace/web/console" install --frozen-lockfile'
        build = 'pnpm --dir "$workspace/web/console" run build'
        self.assertIn(install, source)
        self.assertIn(build, source)
        self.assertLess(source.index(install), source.index(build))

    def test_north_star_precedes_the_heavy_scenario_test(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        qualifier = "python3 scripts/qualify-productization-first-run.py"
        scenario = "cargo test --locked -p insight-platform-qualification-tests --test productization"
        self.assertIn(qualifier, source)
        self.assertIn(scenario, source)
        self.assertLess(source.index(qualifier), source.index(scenario))

    def test_default_project_uses_short_unix_socket_safe_path(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        self.assertIn(
            'mktemp -d "/tmp/insight-productization.XXXXXX"',
            source,
        )
        self.assertNotIn('${TMPDIR:-/tmp}/insight-productization', source)

    def test_cleanup_can_derive_compose_project_before_process_state_exists(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        self.assertIn('"$project/.insight/project.json"', source)
        self.assertIn('if processes.is_file():', source)
        self.assertIn('tenant_id = identity.get("identity", {}).get("tenant_id", "")', source)
        self.assertIn('project = f"insight-{match.group(1)}" if match else ""', source)

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

    def test_unknown_feature_is_rejected_before_build(self) -> None:
        result = self.run_runner("--features", "expanded")
        self.assertEqual(result.returncode, 2)
        self.assertIn("--features must be all or", result.stderr)
        self.assertNotIn("Compiling", result.stderr)

    def test_all_features_can_emit_all_scenario_reports(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        workflow = (ROOT / ".github/workflows/productization-journey.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            '"PLATFORM_PRODUCTIZATION_REPORT_DIRECTORY=$report_directory"', source
        )
        self.assertIn('"PLATFORM_PRODUCTIZATION_FEATURES=$features"', source)
        scenario_upload = workflow.split(
            "- name: Preserve exact-revision scenario reports", 1
        )[1].split("- name: Preserve fresh-checkout north-star report", 1)[0]
        self.assertNotIn("inputs.features == 'starter'", scenario_upload)
        self.assertIn(
            '--report-directory "$RUNNER_TEMP/productization-reports"', workflow
        )
        self.assertIn(
            "productization-${{ inputs.features }}-scenario-reports-${{ github.sha }}",
            workflow,
        )

    def test_north_star_report_requires_fresh_checkout_clock(self) -> None:
        result = self.run_runner("--north-star-report", "/tmp/north-star.json")
        self.assertEqual(result.returncode, 2)
        self.assertIn("requires --fresh-checkout", result.stderr)
        self.assertNotIn("Compiling", result.stderr)

    def test_workflow_starts_clock_before_checkout_and_preserves_report(self) -> None:
        workflow = (ROOT / ".github/workflows/productization-journey.yml").read_text(
            encoding="utf-8"
        )
        clock = "Record fresh-checkout journey start"
        checkout = "actions/checkout@"
        self.assertLess(workflow.index(clock), workflow.index(checkout))
        self.assertIn("--journey-started-epoch", workflow)
        self.assertIn("--fresh-checkout", workflow)
        self.assertIn("productization-starter-north-star-${{ github.sha }}", workflow)


if __name__ == "__main__":
    unittest.main()
