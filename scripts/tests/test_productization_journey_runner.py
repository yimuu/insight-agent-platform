from __future__ import annotations

import json
import os
import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run-productization-journey.sh"
WORKFLOW = ROOT / ".github" / "workflows" / "productization-journey.yml"
BOOTSTRAP = ROOT / "scripts" / "bootstrap-platform-kind-local.sh"


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
        self.assertIn("--sandbox-evidence", result.stdout)
        self.assertIn("--aggregate-report", result.stdout)
        self.assertIn("--release-candidate", result.stdout)
        self.assertIn("--console-bundle", result.stdout)
        self.assertIn("--sandbox-environment", result.stdout)

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
        scenario = (
            "cargo test --locked -p insight-platform-qualification-tests "
            "--test productization"
        )
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
            "productization-${{ env.PRODUCTIZATION_FEATURES }}-scenario-reports-${{ github.sha }}",
            workflow,
        )
        strict_branch = 'if [[ "$features" == "all" ]]; then'
        self.assertIn(strict_branch, source)
        self.assertNotIn(
            "--allow-incomplete",
            source[source.index(strict_branch) : source.index("else", source.index(strict_branch))],
        )
        self.assertIn('--aggregate-output "$aggregate_report"', source)
        self.assertIn("PLATFORM_PRODUCTIZATION_SANDBOX_EVIDENCE", source)

    def test_all_fails_closed_without_physical_sandbox_evidence(self) -> None:
        result = self.run_runner("--features", "all")
        self.assertEqual(result.returncode, 2)
        self.assertIn("requires --report-directory", result.stderr)
        self.assertNotIn("Compiling", result.stderr)

    def test_sandbox_implies_remote_capability_and_requires_evidence(self) -> None:
        result = self.run_runner("--features", "sandbox")
        self.assertEqual(result.returncode, 2)
        self.assertIn("requires --sandbox-evidence and --sandbox-environment", result.stderr)
        source = RUNNER.read_text(encoding="utf-8")
        self.assertIn('selected.add("remote-capability")', source)

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

    def test_all_workflow_bootstraps_and_always_cleans_real_opensandbox(self) -> None:
        workflow = (ROOT / ".github/workflows/productization-journey.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("scripts/bootstrap-platform-kind-local.sh", workflow)
        self.assertIn("scripts/run-productization-sandbox-qualification.sh", workflow)
        self.assertIn("INSIGHT_KIND_PLATFORM_DIGEST=$runtime_digest", workflow)
        self.assertIn("INSIGHT_KIND_SANDBOX_RUNNER_DIGEST=$runner_digest", workflow)
        self.assertIn("INSIGHT_KIND_SANDBOX_PACKAGE_DIGEST=$package_digest", workflow)
        self.assertIn("Always remove the disposable Kind cluster", workflow)
        self.assertIn('kind delete cluster --name "$INSIGHT_KIND_CLUSTER_NAME"', workflow)
        self.assertIn("workflow_call:", workflow.split("permissions:", 1)[0])
        ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        productization_call = ci.split("\n  productization:", 1)[-1].split(
            "\n  required:", 1
        )[0]
        self.assertIn(
            "uses: ./.github/workflows/productization-journey.yml",
            productization_call,
        )
        self.assertIn(
            "permissions:\n      contents: read\n      packages: read",
            productization_call,
        )
        self.assertIn("needs: [changes, quick, lint, test, cli, console, policy, productization]", ci)

    def test_candidate_mode_is_prebuilt_only_and_fail_closed(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        workflow = WORKFLOW.read_text(encoding="utf-8")
        release = (ROOT / ".github/workflows/product-release.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn('if [[ -z "$release_candidate" ]]; then\n  cargo build', source)
        self.assertIn("dev_arguments+=(--offline)", source)
        self.assertIn('INSIGHT_CONSOLE_BUNDLE_ROOT=$console_bundle', source)
        self.assertIn("prepare-productization-release-candidate.py", workflow)
        self.assertIn("verify-productization-console-image.py", workflow)
        self.assertIn("INSIGHT_KIND_PLATFORM_INDEX_DIGEST=$runtime_index", workflow)
        self.assertIn(
            "INSIGHT_KIND_SANDBOX_RUNNER_INDEX_DIGEST=$runner_index", workflow
        )
        self.assertIn('"${subject}:build-${GITHUB_SHA}"', workflow)
        self.assertIn("needs: assemble-release", release)
        self.assertIn("artifact_mode: signed-release-candidate", release)

    def test_candidate_indexes_are_closed_by_raw_digest_and_unique_host_child(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(
            workflow.count(
                "go install github.com/google/go-containerregistry/cmd/crane@v0.20.6"
            ),
            1,
        )
        verify_block = workflow.split(
            "for component in runtime sandbox_runner console; do", 1
        )[1].split("runtime_subject=", 1)[0]
        self.assertIn(
            '"$crane_bin" manifest "${subject}@${index_digest}" >"$raw_index"',
            verify_block,
        )
        self.assertIn(
            '[[ "$observed_raw_digest" == "$index_digest" ]]', verify_block
        )

        filter_marker = 'jq -e --arg expected "$platform_digest" \'\n'
        filter_end = '\n            \' "$raw_index" >/dev/null'
        jq_filter = verify_block.split(filter_marker, 1)[1].split(filter_end, 1)[0]
        expected = "sha256:" + "b" * 64
        arm64 = "sha256:" + "c" * 64

        def check(manifests: list[dict[str, object]], media_type: str) -> int:
            index = {
                "schemaVersion": 2,
                "mediaType": media_type,
                "manifests": manifests,
            }
            return subprocess.run(
                ["jq", "-e", "--arg", "expected", expected, jq_filter],
                input=json.dumps(index),
                capture_output=True,
                text=True,
                check=False,
            ).returncode

        host = {
            "digest": expected,
            "platform": {"os": "linux", "architecture": "amd64"},
        }
        other = {
            "digest": arm64,
            "platform": {"os": "linux", "architecture": "arm64"},
        }
        attestation = {
            "digest": "sha256:" + "d" * 64,
            "platform": {"os": "unknown", "architecture": "unknown"},
        }
        media_type = "application/vnd.oci.image.index.v1+json"
        self.assertEqual(check([host, other, attestation], media_type), 0)
        self.assertNotEqual(
            check([{**host, "digest": arm64}, other], media_type), 0
        )
        self.assertNotEqual(check([host, host, other], media_type), 0)
        self.assertNotEqual(
            check([host, other], "application/vnd.oci.image.manifest.v1+json"), 0
        )

    def test_product_images_use_verified_registry_preserving_oci_import(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        bootstrap = BOOTSTRAP.read_text(encoding="utf-8")
        source = workflow.split(
            "- name: Build current-SHA Kind images and complete non-Sandbox seed", 1
        )[1].split(
            "- name: Prepare exact signed runtime and Sandbox runner for Kind", 1
        )[0]
        candidate = workflow.split(
            "- name: Prepare exact signed runtime and Sandbox runner for Kind", 1
        )[1].split("- name: Bootstrap fresh current-SHA Kind", 1)[0]
        self.assertIn(
            "for target in runtime sandbox-runner sandbox-l3-package; do", source
        )
        self.assertIn(
            "docker buildx build --platform linux/amd64 --provenance=false --sbom=false",
            source,
        )
        self.assertIn("scripts/inspect-platform-oci-image.py", source)
        self.assertIn("scripts/verify-platform-sandbox-package-image.py", source)
        self.assertNotIn("docker image inspect", source)
        self.assertNotIn("docker build --target", source)
        self.assertIn(
            'runtime_reference="${runtime_subject}@${runtime_platform}"', candidate
        )
        self.assertIn("--platform linux/amd64 --format=oci --annotate-ref", candidate)
        self.assertIn('"$runtime_reference" "$runtime_layout"', candidate)
        self.assertIn('"$runner_reference" "$runner_layout"', candidate)
        self.assertIn("scripts/inspect-platform-oci-image.py", candidate)
        self.assertIn("scripts/verify-platform-sandbox-package-image.py", candidate)
        self.assertIn("INSIGHT_KIND_PLATFORM_OCI_ARCHIVE=$runtime_archive", candidate)
        self.assertIn(
            "INSIGHT_KIND_SANDBOX_RUNNER_OCI_ARCHIVE=$runner_archive", candidate
        )
        self.assertIn(
            "platform_oci_archive=${INSIGHT_KIND_PLATFORM_OCI_ARCHIVE:-}", bootstrap
        )
        self.assertIn(
            "sandbox_runner_oci_archive=${INSIGHT_KIND_SANDBOX_RUNNER_OCI_ARCHIVE:-}",
            bootstrap,
        )
        self.assertIn("scripts/verify-platform-sandbox-package-image.py", bootstrap)
        self.assertIn("sandbox_runner_image_identity", bootstrap)
        self.assertIn(
            "kind_docker_images=(", bootstrap
        )
        self.assertIn("find_kind_image_by_target_digest()", bootstrap)
        self.assertIn("kind_image_digest_by_reference()", bootstrap)
        self.assertIn('LC_ALL=C docker exec --env LC_ALL=C "$node"', bootstrap)
        self.assertIn('$1 == exact_reference { print $3 }', bootstrap)
        self.assertIn("import_exact_oci_image_into_kind()", bootstrap)
        self.assertIn(
            '"$platform_oci_archive" "$platform_repository@$platform_digest" "$platform_digest"',
            bootstrap,
        )
        self.assertIn(
            '"$sandbox_runner_oci_archive"',
            bootstrap,
        )
        self.assertIn(
            'if [[ "$observed_digest" != "$expected_digest" ]]; then', bootstrap
        )
        self.assertIn(
            'ctr --namespace=k8s.io content get "$expected_digest"', bootstrap
        )
        self.assertIn(
            'ctr --namespace=k8s.io content get "$expected_config_digest"', bootstrap
        )
        self.assertIn("source_oci_manifest", bootstrap)
        self.assertNotIn(
            'platform_import_image="insight-kind/platform-candidate:', bootstrap
        )
        self.assertNotIn('verify_local_image "$platform_image"', bootstrap)
        self.assertNotIn('"docker.io/library/$platform_image"', bootstrap)
        self.assertNotIn('"docker.io/library/$sandbox_package_image"', bootstrap)
        self.assertNotIn("images inspect", bootstrap)

    def test_kind_image_digest_query_uses_unique_ctr_list_row(self) -> None:
        bootstrap = BOOTSTRAP.read_text(encoding="utf-8")
        query_functions = "kind_image_listing() {" + bootstrap.split(
            "kind_image_listing() {", 1
        )[1].split("import_exact_oci_image_into_kind() {", 1)[0]
        reference = "ghcr.io/example/platform-runtime@sha256:" + "a" * 64
        digest = "sha256:" + "b" * 64
        header = "REF TYPE DIGEST SIZE PLATFORMS LABELS"

        def query(listing: str) -> subprocess.CompletedProcess[str]:
            script = f"""
set -euo pipefail
docker() {{
  printf '%s\\n' "$CTR_LISTING"
}}
{query_functions}
kind_image_digest_by_reference node "$EXPECTED_REFERENCE"
"""
            environment = os.environ.copy()
            environment.update(
                {"CTR_LISTING": listing, "EXPECTED_REFERENCE": reference}
            )
            return subprocess.run(
                ["bash", "-c", script],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )

        row = (
            f"{reference} application/vnd.oci.image.manifest.v1+json "
            f"{digest} 42.0 MiB linux/amd64 -"
        )
        result = query(f"{header}\n{row}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), digest)

        duplicate = query(f"{header}\n{row}\n{row}")
        self.assertNotEqual(duplicate.returncode, 0)
        self.assertIn("contains 2 rows", duplicate.stderr)

        malformed = query(
            f"{header}\n{reference} application/vnd.oci.image.manifest.v1+json "
            "not-a-digest 42.0 MiB linux/amd64 -"
        )
        self.assertNotEqual(malformed.returncode, 0)
        self.assertIn("malformed target digest", malformed.stderr)

        inspect_tree = query(
            "└──manifest (application/vnd.oci.image.manifest.v1+json)\n"
            f"   ├── config ({digest})\n"
            "   └── layer (sha256:cccc)"
        )
        self.assertNotEqual(inspect_tree.returncode, 0)

    def test_candidate_seed_cannot_fall_back_to_source(self) -> None:
        bootstrap = BOOTSTRAP.read_text(encoding="utf-8")
        seed_block = bootstrap.split(
            'if [[ ! -d "$seed_project/.insight/runtime/config" ]]; then', 1
        )[1].split("seed_runtime=", 1)[0]
        candidate_guard = 'if [[ -n "$platform_index_digest" ]]; then'
        refusal = "signed candidate seed runtime config is missing; refusing source fallback"
        source_build = (
            '"$insight_bin" dev --path "$seed_project" --features all --from-source'
        )
        self.assertIn(candidate_guard, seed_block)
        self.assertIn(refusal, seed_block)
        self.assertIn(source_build, seed_block)
        self.assertLess(seed_block.index(candidate_guard), seed_block.index(source_build))
        self.assertLess(seed_block.index("exit 1"), seed_block.index(source_build))

        workflow = WORKFLOW.read_text(encoding="utf-8")
        candidate = workflow.split(
            "- name: Prepare exact signed runtime and Sandbox runner for Kind", 1
        )[1].split("- name: Bootstrap fresh current-SHA Kind", 1)[0]
        self.assertNotIn("--from-source", candidate)
        self.assertIn("--offline", candidate)

    def test_quick_ci_runs_candidate_and_sandbox_boundary_tests(self) -> None:
        ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        quick = ci.split("quick:", 1)[1].split("\n  lint:", 1)[0]
        for path in (
            "scripts/tests/test_prepare_productization_release_candidate.py",
            "scripts/tests/test_inspect_platform_oci_image.py",
            "scripts/tests/test_verify_platform_sandbox_package_image.py",
            "scripts/tests/test_platform_kind_l4_opensandbox_probe.py",
        ):
            self.assertIn(path, quick)

    def test_report_directory_must_be_new_and_binds_one_run(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        self.assertIn('--report-directory must name a path that does not already exist', source)
        self.assertIn("PLATFORM_PRODUCTIZATION_QUALIFICATION_RUN_ID", source)
        self.assertIn("PLATFORM_PRODUCTIZATION_ACTUAL_PROFILE", source)
        self.assertIn("PLATFORM_PRODUCTIZATION_PROFILE_DIGEST", source)
        self.assertIn('--sandbox-evidence "$sandbox_evidence"', source)
        self.assertIn('--sandbox-environment "$sandbox_environment"', source)
        self.assertIn('qualification_run_id = sandbox_evidence.get("qualification_run_id")', source)
        self.assertIn(
            '"PLATFORM_PRODUCTIZATION_QUALIFICATION_RUN_ID=$qualification_run_id"',
            source,
        )

    def test_identity_pairs_do_not_nest_heredocs_in_process_substitutions(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        self.assertNotIn(
            "read -r candidate_version candidate_revision < <(python3 -", source
        )
        self.assertNotIn(
            "read -r runtime_profile_digest qualification_run_id < <(python3 -",
            source,
        )
        self.assertIn('candidate_identity="$(python3 -', source)
        self.assertIn('runtime_identity="$(python3 -', source)
        self.assertIn(
            'read -r candidate_version candidate_revision <<< "$candidate_identity"',
            source,
        )
        self.assertIn(
            'read -r runtime_profile_digest qualification_run_id <<< "$runtime_identity"',
            source,
        )

    def test_ci_checker_allows_only_local_unpinned_uses(self) -> None:
        checker = (ROOT / "scripts/check-productization-ci.py").read_text(encoding="utf-8")
        self.assertIn('if action.startswith("./"):', checker)
        result = subprocess.run(
            ["python3", str(ROOT / "scripts/check-productization-ci.py")],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
