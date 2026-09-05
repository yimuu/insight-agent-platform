import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "classify_ci_paths", ROOT / "scripts/classify-ci-paths.py"
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ClassifyCiPathsTests(unittest.TestCase):
    def test_docs_only_uses_quick_lane(self) -> None:
        result = MODULE.classify(["docs/current/architecture.md"])
        self.assertTrue(result["quick"])
        self.assertFalse(result["runtime"])
        self.assertFalse(result["cli"])

    def test_console_only_uses_console_without_runtime(self) -> None:
        result = MODULE.classify(["web/console/src/App.tsx"])
        self.assertTrue(result["console"])
        self.assertFalse(result["runtime"])

    def test_cli_productization_change_uses_cli_without_runtime(self) -> None:
        result = MODULE.classify(
            ["crates/insight-cli/src/lib.rs", "tests/productization/example.rs"]
        )
        self.assertTrue(result["cli"])
        self.assertFalse(result["runtime"])

    def test_product_documentation_does_not_expand_the_ci_lane(self) -> None:
        result = MODULE.classify(["docs/current/cli.md"])
        self.assertFalse(result["cli"])
        self.assertFalse(result["runtime"])

    def test_first_run_qualifier_uses_cli_without_runtime(self) -> None:
        result = MODULE.classify(["scripts/qualify-productization-first-run.py"])
        self.assertTrue(result["cli"])
        self.assertFalse(result["runtime"])

    def test_base_journey_runner_uses_cli_without_runtime(self) -> None:
        result = MODULE.classify(
            [
                "scripts/run-productization-journey.sh",
                "scripts/tests/test_productization_journey_runner.py",
            ]
        )
        self.assertTrue(result["cli"])
        self.assertFalse(result["runtime"])

    def test_runtime_mcp_changes_select_full_workspace(self) -> None:
        result = MODULE.classify(["crates/platform-mcp-service/src/main.rs"])
        self.assertTrue(result["runtime"])

    def test_dependency_change_selects_runtime_and_policy(self) -> None:
        result = MODULE.classify(["Cargo.lock"])
        self.assertTrue(result["runtime"])
        self.assertTrue(result["policy"])

    def test_manual_or_scheduled_run_forces_every_lane(self) -> None:
        self.assertTrue(all(MODULE.classify([], force_all=True).values()))

    def test_unknown_script_fails_closed_to_runtime(self) -> None:
        result = MODULE.classify(["scripts/new-runtime-check.py"])
        self.assertTrue(result["runtime"])

    def test_ci_workflow_change_fails_closed_to_runtime(self) -> None:
        result = MODULE.classify([".github/workflows/ci.yml"])
        self.assertTrue(result["runtime"])


if __name__ == "__main__":
    unittest.main()
