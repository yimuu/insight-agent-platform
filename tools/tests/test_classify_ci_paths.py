import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
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

    def test_installation_inputs_and_consumers_select_runtime_for_actual_installation(self) -> None:
        for path in (
            "deploy/release/development-profile-v1.json",
            "deploy/dev/nats.conf",
            "deploy/images/console.Dockerfile",
            "deploy/helm/insight-platform-installation/templates/phase-job.yaml",
            "tools/rust/platform-deployment-tooling/src/renderer.rs",
            "tools/rust/platform-installation-tooling/src/main.rs",
            "tools/install/platform_compose.py",
            "tools/install/platform_native.py",
            "tools/install/native_runtime.py",
            "tools/install/provider_lifecycle.py",
            "tools/install/public_trust.py",
            "tools/tests/test_installation_native.py",
            "tools/tests/test_public_trust.py",
            "tools/rust/platform-deployment-tooling/src/s3_profile.rs",
            "crates/adapters/platform-openbao/src/transport.rs",
            "tools/qualification/qualify-platform-installation-compose.py",
            "crates/deployment/platform-deployment-contracts/src/installation.rs",
            "crates/deployment/platform-deployment-contracts/src/public_trust.rs",
            "crates/protocols/platform-api/src/model_configuration.rs",
        ):
            with self.subTest(path=path):
                self.assertTrue(MODULE.classify([path])["runtime"])

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

    def test_actual_workflow_dispatch_with_root_merge_and_tag_refs(self) -> None:
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        step = workflow.split("      - name: Resolve closed CI lane set\n", 1)[1].split("\n  quick:", 1)[0]
        script = textwrap.dedent(step.split("        run: |\n", 1)[1])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            environment = {**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
                           "BASH_ENV": os.devnull}

            def git(*args):
                return subprocess.run(["git", *args], cwd=root, env=environment, check=True,
                                      capture_output=True, text=True, timeout=5).stdout.strip()

            git("init", "--quiet", "--initial-branch=main")
            git("config", "user.name", "CI fixture")
            git("config", "user.email", "ci@example.invalid")
            (root / "tools/checks").mkdir(parents=True)
            shutil.copyfile(ROOT / "tools/checks/classify-ci-paths.py", root / "tools/checks/classify-ci-paths.py")
            (root / "README.md").write_text("initial\n")
            git("add", ".")
            git("commit", "--quiet", "-m", "Initial fixture")
            initial = git("rev-parse", "HEAD")
            git("switch", "--quiet", "-c", "feature")
            (root / "docs").mkdir()
            (root / "docs/feature.md").write_text("feature\n")
            git("add", "docs/feature.md")
            git("commit", "--quiet", "-m", "Feature documentation")
            git("switch", "--quiet", "main")
            (root / "README.md").write_text("main documentation\n")
            git("commit", "--quiet", "-am", "Main documentation")
            before_merge = git("rev-parse", "HEAD")
            git("merge", "--quiet", "--no-ff", "feature", "-m", "Merge fixture")
            merged = git("rev-parse", "HEAD")
            git("tag", "-a", "v1.2.3", "-m", "Tag fixture")
            self.assertEqual(git("rev-parse", "v1.2.3^{commit}"), merged)
            # This is the actual old failure trigger, not a mocked empty diff.
            self.assertEqual(git("diff-tree", "--root", "--no-commit-id", "--name-only", "-r", merged), "")
            all_lanes = {"quick": True, "cli": True, "console": True, "runtime": True, "policy": True}
            docs_only = {"quick": True, "cli": False, "console": False, "runtime": False, "policy": False}
            cases = [
                ("new-tag-merge", "push", "tag", "0" * 40, "", merged, all_lanes),
                ("tag-nonzero-before", "push", "tag", before_merge, "", merged, all_lanes),
                ("new-branch-merge", "push", "branch", "0" * 40, "", merged, all_lanes),
                ("new-branch-root", "push", "branch", "0" * 40, "", initial, all_lanes),
                ("docs-push", "push", "branch", before_merge, "", merged, docs_only),
                ("docs-pr", "pull_request", "branch", "0" * 40, initial, merged, docs_only),
                ("manual", "workflow_dispatch", "branch", "", "", merged, all_lanes),
                ("scheduled", "schedule", "branch", "", "", merged, all_lanes),
                ("unchanged-push", "push", "branch", merged, "", merged, None),
                ("invalid-short-zero", "push", "branch", "0", "", merged, None),
            ]
            for name, event, ref_type, before, base, head, expected in cases:
                with self.subTest(name=name):
                    output = root / (name + ".output")
                    env = {**environment, "EVENT_NAME": event, "REF_TYPE": ref_type,
                           "BEFORE_SHA": before, "BASE_SHA": base, "HEAD_SHA": head,
                           "GITHUB_OUTPUT": str(output)}
                    result = subprocess.run(["bash", "-e", "-c", script], cwd=root, env=env,
                                            capture_output=True, text=True, timeout=10)
                    if expected is None:
                        self.assertNotEqual(result.returncode, 0)
                        self.assertFalse(output.exists())
                    else:
                        self.assertEqual(result.returncode, 0, result.stderr)
                        observed = dict(line.split("=", 1) for line in output.read_text().splitlines())
                        self.assertEqual(observed, {key: str(value).lower() for key, value in expected.items()})


if __name__ == "__main__":
    unittest.main()
