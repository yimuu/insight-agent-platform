import importlib.util
from pathlib import Path
import unittest


ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "Cargo.toml").is_file())
SPEC = importlib.util.spec_from_file_location(
    "classify_ci_paths", ROOT / "tools/checks/classify-ci-paths.py"
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
        result = MODULE.classify(["apps/console/src/App.tsx"])
        self.assertTrue(result["console"])
        self.assertFalse(result["runtime"])

    def test_shared_compiler_and_wire_changes_exercise_actual_wasm_console_lane(self) -> None:
        for path in (
            "crates/authoring/platform-agent-compiler/src/boundary.rs",
            "crates/authoring/platform-agent-compiler-wasm/src/lib.rs",
            "crates/definitions/platform-plan/src/typed_expression.rs",
            "crates/foundation/platform-contracts/src/model.rs",
            "contracts/platform-v1/schemas/agent-node-editor.schema.json",
            "contracts/product-experience/agent-compiler/v2/corpus.json",
            "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".github/workflows/ci.yml",
            "tools/rust/platform-contract-tooling/src/bin/agent_compiler_resources.rs",
        ):
            with self.subTest(path=path):
                self.assertTrue(MODULE.classify([path])["console"])

    def test_unrelated_provider_changes_do_not_rebuild_the_browser_compiler(self) -> None:
        result = MODULE.classify(["crates/adapters/platform-egress/src/lib.rs"])
        self.assertTrue(result["runtime"])
        self.assertFalse(result["console"])

    def test_cli_productization_change_uses_cli_without_runtime(self) -> None:
        result = MODULE.classify(
            ["apps/insight-cli/src/lib.rs", "tests/qualification/tests/productization/example.rs"]
        )
        self.assertTrue(result["cli"])
        self.assertFalse(result["runtime"])

    def test_product_documentation_does_not_expand_the_ci_lane(self) -> None:
        result = MODULE.classify(["docs/current/cli.md"])
        self.assertFalse(result["cli"])
        self.assertFalse(result["runtime"])

    def test_first_run_qualifier_uses_cli_without_runtime(self) -> None:
        result = MODULE.classify(["tools/qualification/qualify-productization-first-run.py"])
        self.assertTrue(result["cli"])
        self.assertFalse(result["runtime"])

    def test_base_journey_runner_uses_cli_without_runtime(self) -> None:
        result = MODULE.classify(
            [
                "tools/qualification/run-productization-journey.sh",
                "tools/tests/test_productization_journey_runner.py",
            ]
        )
        self.assertTrue(result["cli"])
        self.assertFalse(result["runtime"])

    def test_runtime_mcp_changes_select_full_workspace(self) -> None:
        result = MODULE.classify(["apps/services/platform-mcp-service/src/main.rs"])
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
