#!/usr/bin/env python3
"""Fail closed when Productization CI loses its path-aware lane boundaries."""

import json
import re
from pathlib import Path


ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "Cargo.toml").is_file())
ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
candidate = (ROOT / ".github/workflows/platform-production-candidate.yml").read_text(
    encoding="utf-8"
)
starter_journey = (
    ROOT / ".github/workflows/productization-journey.yml"
).read_text(encoding="utf-8")
product_release = (ROOT / ".github/workflows/product-release.yml").read_text(
    encoding="utf-8"
)
journey_runner = (ROOT / "tools/qualification/run-productization-journey.sh").read_text(
    encoding="utf-8"
)
kind_bootstrap = (ROOT / "tools/qualification/bootstrap-platform-kind-local.sh").read_text(
    encoding="utf-8"
)
dockerfile = (ROOT / "deploy/images/platform.Dockerfile").read_text(encoding="utf-8")
development_profile = json.loads(
    (ROOT / "deploy/release/development-profile-v1.json").read_text(encoding="utf-8")
)
failures: list[str] = []

# These are explicit AWS physical fixtures. Ordinary lifecycle commands only display the
# unified installation entry points; silently using those commands would not run a gate.
for path in (
    ".github/workflows/productization-journey.yml",
    "tools/qualification/run-productization-journey.sh",
    "tools/qualification/qualify-development-profile.sh",
    "tools/qualification/bootstrap-platform-kind-local.sh",
):
    source = (ROOT / path).read_text(encoding="utf-8").replace("\\\n", "")
    if re.search(r'(?:"\$insight_bin"|target/debug/insight|"\$PRODUCTIZATION_RELEASE_CANDIDATE_BINARY")\s+(?:init|dev|token|start|stop|status|logs|reset)\b', source):
        failures.append(f"AWS qualification invokes an ordinary lifecycle entry in {path}")
    if "qualification-aws" not in source:
        failures.append(f"AWS qualification namespace is absent in {path}")
for path, actions in (
    ("tools/qualification/fixture_project.py", ("stop", "reset")),
    ("tools/qualification/qualify-fixture-cleanup.py", ("init",)),
):
    source = (ROOT / path).read_text(encoding="utf-8")
    for action in actions:
        if f'[insight, "qualification-aws", "{action}",' not in source:
            failures.append(f"AWS fixture {action} namespace is absent in {path}")
for path in (
    "tests/qualification/tests/productization/deterministic_first_run.rs",
    "tests/qualification/tests/productization/native_artifact_cors.rs",
):
    source = (ROOT / path).read_text(encoding="utf-8")
    if '"qualification-aws"' not in source or re.search(r'\.args\(\[\s*(?:"(?:stop|start)"|action)\s*,\s*"--path"', source):
        failures.append(f"Rust AWS lifecycle fixture lost its explicit namespace in {path}")

for marker in (
    "Classify changed paths",
    "Quick contracts",
    "Affected CLI",
    "Affected Console",
    "Static contracts and formatting",
    "Workspace Rust verification",
    "Required CI summary",
    "tools/checks/classify-ci-paths.py",
    "REF_TYPE: ${{ github.ref_type }}",
    '[[ "$EVENT_NAME" == "push" && ( "$REF_TYPE" == "tag" || "$BEFORE_SHA" =~ ^0{40}$ ) ]]',
    "tools/checks/check-product-release.py",
    "tools/checks/check-required-ci-results.py",
    "tools/tests/test_product_release.py",
    "tools/tests/test_prepare_productization_release_candidate.py",
    "tools/tests/test_platform_observability_redaction.py",
    "tools/tests/test_required_ci_results.py",
    "needs.changes.outputs.runtime == 'true'",
    "needs.changes.outputs.cli == 'true'",
    "needs.changes.outputs.console == 'true'",
    "needs.changes.outputs.policy == 'true'",
    "corepack pnpm --dir apps/console install --frozen-lockfile",
    "corepack pnpm --dir apps/console browser:fixture:qualify",
    "corepack install --global pnpm@11.19.0",
    "runs-on: ubuntu-24.04",
    'node-version: "24.11.1"',
    "version: v3.19.0",
    "workflow_dispatch:",
    "schedule:",
):
    if marker not in ci:
        failures.append(f"path-aware CI misses {marker!r}")

for forbidden in (
    "docker/build-push-action@",
    "docker push",
    "cosign sign",
    "actions/attest@",
    "image: postgres:",
    "image: nats:",
):
    if forbidden in ci:
        failures.append(f"ordinary CI contains candidate-only operation {forbidden!r}")

quick_contracts = ci.split("\n  quick:", 1)[-1].split("\n  lint:", 1)[0]
console_checks = ci.split("\n  console:", 1)[-1].split("\n  policy:", 1)[0]
if not re.search(r"run\('cargo',\s*\[\s*'build',\s*'--locked',\s*'-p',\s*'insight-platform-agent-compiler-wasm'", (ROOT / "apps/console/scripts/build-agent-compiler.ts").read_text()):
    failures.append("Console WASM build must preserve the exact Cargo.lock dependency closure")
wasm_binding = re.findall(r'^wasm-bindgen\s*=\s*"=([0-9]+\.[0-9]+\.[0-9]+)"$', (ROOT / "crates/authoring/platform-agent-compiler-wasm/Cargo.toml").read_text(), re.MULTILINE)
if len(wasm_binding) != 1:
    raise SystemExit("WASM owner must pin exactly one wasm-bindgen version")
wasm_binding = wasm_binding[0]
source_compiler_name = "Prepare the exact source Console compiler"
source_compiler_step = starter_journey.split(f"      - name: {source_compiler_name}\n", 1)
if len(source_compiler_step) != 2:
    failures.append("source journey must prepare its owning Console compiler")
else:
    source_compiler_step = source_compiler_step[1].split("\n      - ", 1)[0]
    for marker in (
        "if: ${{ env.PRODUCTIZATION_ARTIFACT_MODE == 'source' }}",
        "set -euo pipefail",
        "rustup target add wasm32-unknown-unknown --toolchain 1.94.1",
        f"cargo install --locked wasm-bindgen-cli --version {wasm_binding}",
        f'test "$(wasm-bindgen --version)" = "wasm-bindgen {wasm_binding}"',
    ):
        if marker not in source_compiler_step:
            failures.append(f"source Console preparation lacks {marker}")
    if "continue-on-error" in source_compiler_step or "always()" in source_compiler_step:
        failures.append("source Console compiler preparation must fail closed")
    if not (
        starter_journey.index("shared-key: \"productization-")
        < starter_journey.index(source_compiler_name)
        < starter_journey.index("Build current-SHA Kind images and complete non-Sandbox seed")
        < starter_journey.index("Run fresh public CLI and real Gateway Console journey")
    ):
        failures.append("source Console compiler must be prepared before expensive journey builds")
for marker in (
    "targets: wasm32-unknown-unknown",
    f"cargo install --locked wasm-bindgen-cli --version {wasm_binding}",
    "node apps/console/tests/compiler-resources.ts",
):
    if marker not in console_checks:
        failures.append(f"Console CI lacks its owning compiler verification input: {marker}")
for test_path, boundary in (
    (
        "tools/tests/test_prepare_productization_release_candidate.py",
        "signed candidate preparation",
    ),
    ("tools/tests/test_inspect_platform_oci_image.py", "OCI image identity"),
    (
        "tools/tests/test_verify_platform_sandbox_package_image.py",
        "Sandbox Package image closure",
    ),
    (
        "tools/tests/test_platform_kind_l4_opensandbox_probe.py",
        "OpenSandbox L4 probe",
    ),
):
    if test_path not in quick_contracts:
        failures.append(f"quick CI does not run the {boundary} tests")

static_checks = ci.split("\n  lint:", 1)[-1].split("\n  test:", 1)[0]
rust_verification = ci.split("\n  test:", 1)[-1].split("\n  cli:", 1)[0]
cli_checks = ci.split("\n  cli:", 1)[-1].split("\n  console:", 1)[0]
for forbidden in ("cargo run --locked", "cargo check --locked", "cargo clippy --locked"):
    if forbidden in static_checks:
        failures.append(f"static CI lane still compiles Rust via {forbidden!r}")
for marker in (
    "needs: [changes, quick, lint]",
    "components: clippy",
    "Run authoritative Rust contract validators",
    "cargo run --locked -p insight-platform-contract-tooling --bin check-platform-contracts",
    "cargo run --locked -p insight-platform-contract-tooling --bin platform-qualification",
    "cargo run --locked -p insight-platform-storage-tooling --bin check-platform-schema-contract",
    "cargo clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings",
    "cargo test --locked --workspace --all-targets --all-features",
    "cargo test --locked --workspace --doc --all-features",
):
    if marker not in rust_verification:
        failures.append(f"single-target Rust verification misses {marker!r}")
test_command = "cargo test --locked --workspace --all-targets --all-features"
clippy_command = (
    "cargo clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings"
)
if (
    test_command in rust_verification
    and clippy_command in rust_verification
    and rust_verification.index(test_command) > rust_verification.index(clippy_command)
):
    failures.append("workspace-only Clippy must follow tests and reuse their dependency artifacts")
if "cargo check --locked" in ci:
    failures.append("ordinary CI retains a redundant cargo check before Clippy and tests")
if "cargo clippy --locked -p insight-cli --all-targets --no-deps -- -D warnings" not in cli_checks:
    failures.append("CLI Clippy still re-lints dependencies")
if (
    "cargo test --locked -p insight-platform-qualification-tests --test productization --no-run"
    not in cli_checks
):
    failures.append("CLI CI does not compile the Productization journey harness")

profile_images = {
    dependency["name"]: dependency["image"]
    for dependency in development_profile["dependencies"]
}
for profile_name, workflow_name in (("postgresql", "postgres"), ("nats", "nats")):
    image = profile_images.get(profile_name)
    if not isinstance(image, str) or not re.fullmatch(
        rf"{workflow_name}@sha256:[0-9a-f]{{64}}", image
    ):
        failures.append(
            f"development profile {profile_name} image is not digest-pinned: {image!r}"
        )
    elif f"image: {image}" not in ci:
        failures.append(
            f"ordinary CI {workflow_name} service does not reuse development profile image {image!r}"
        )

for workflow_name, workflow in (
    ("ordinary CI", ci),
    ("productization journey", starter_journey),
    ("product release", product_release),
):
    for action in re.findall(r"^\s*-?\s*uses:\s*([^\s#]+)", workflow, flags=re.MULTILINE):
        if action.startswith("./"):
            continue
        revision = action.rsplit("@", 1)[-1]
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            failures.append(f"{workflow_name} action is not pinned to a commit: {action}")

for marker in (
    "RUNTIME_SELECTED: ${{ needs.changes.outputs.runtime }}",
    "CLI_SELECTED: ${{ needs.changes.outputs.cli }}",
    "CONSOLE_SELECTED: ${{ needs.changes.outputs.console }}",
    "POLICY_SELECTED: ${{ needs.changes.outputs.policy }}",
    "steps:\n      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4\n      - name: Reject any selected lane failure",
    "python3 tools/checks/check-required-ci-results.py",
    '--runtime-selected "$RUNTIME_SELECTED"',
    '--cli-selected "$CLI_SELECTED"',
    '--console-selected "$CONSOLE_SELECTED"',
    '--policy-selected "$POLICY_SELECTED"',
    '--changes-result "$CHANGES_RESULT"',
    '--quick-result "$QUICK_RESULT"',
    '--lint-result "$LINT_RESULT"',
    '--test-result "$TEST_RESULT"',
    '--cli-result "$CLI_RESULT"',
    '--console-result "$CONSOLE_RESULT"',
    '--installation-result "$INSTALLATION_RESULT"',
    '--policy-result "$POLICY_RESULT"',
):
    if marker not in ci:
        failures.append(f"required CI summary misses fail-closed marker {marker!r}")

trigger_block = candidate.split("permissions:", 1)[0]
if "workflow_dispatch:" not in trigger_block:
    failures.append("candidate workflow is not explicitly dispatched")
for forbidden_trigger in ("pull_request:", "schedule:", "tags:"):
    if forbidden_trigger in trigger_block:
        failures.append(f"candidate workflow contains automatic trigger {forbidden_trigger!r}")
if "timeout-minutes: 10" not in candidate or candidate.count("timeout-minutes: 10") < 2:
    failures.append("candidate sign and verify steps must retain bounded ten-minute timeouts")
if dockerfile.count("cargo build --locked --release --workspace") != 1:
    failures.append("candidate deploy/images/platform.Dockerfile must compile the production closure once")
if "cargo build --locked --release --workspace" not in dockerfile:
    failures.append("candidate deploy/images/platform.Dockerfile must use one workspace binary build graph")
for marker in (
    "RUSTFLAGS='-C target-feature=+crt-static'",
    "! readelf -l /workspace/platform-sandbox-runner-core | grep -q INTERP",
    "! readelf -d /workspace/platform-sandbox-runner-core | grep -q NEEDED",
):
    if marker not in dockerfile:
        failures.append(f"Sandbox runner static link contract misses {marker!r}")
if "linker=musl-gcc" in dockerfile:
    failures.append("Sandbox runner forces the Debian musl wrapper as its Rust linker")

journey_trigger = starter_journey.split("permissions:", 1)[0]
for required_trigger in ("workflow_call:", "workflow_dispatch:", "schedule:"):
    if required_trigger not in journey_trigger:
        failures.append(f"10/10 journey misses required trigger {required_trigger!r}")
for forbidden_trigger in ("push:", "pull_request:"):
    if forbidden_trigger in journey_trigger:
        failures.append(f"10/10 journey runs on ordinary change trigger {forbidden_trigger!r}")
for fallback in (
    "PRODUCTIZATION_FEATURES: ${{ inputs.features || 'all' }}",
    "PRODUCTIZATION_ARTIFACT_MODE: ${{ inputs.artifact_mode || 'source' }}",
):
    if fallback not in starter_journey:
        failures.append(f"scheduled 10/10 journey misses closed fallback {fallback!r}")
for marker in (
    "runs-on: ubuntu-24.04",
    "tools/qualification/run-productization-journey.sh",
    "--report-directory",
    "--console-browser",
    "Record fresh-checkout journey start",
    "--north-star-report",
    "--journey-started-epoch",
    "--fresh-checkout",
    "productization-starter-north-star-${{ github.sha }}",
    'node-version: "24.11.1"',
    "actions/setup-go@0a12ed9d6a96ab950c8f026ed9f722fe0da7ef32",
    'go-version: "1.25.1"',
    "pnpm@11.19.0",
    "timeout-minutes: 180",
    "group: productization-journey-${{ github.workflow }}-${{ github.ref }}-${{ inputs.features || 'all' }}-${{ inputs.artifact_mode || 'source' }}",
    "type: choice",
    "PRODUCTIZATION_FEATURES: ${{ inputs.features || 'all' }}",
    "go install sigs.k8s.io/kind@v0.30.0",
    "--features context,mcp,model,remote-capability --from-source",
    "for target in runtime sandbox-runner sandbox-l3-package; do",
    "docker buildx build --platform linux/amd64 --provenance=false --sbom=false",
    "--file deploy/images/platform.Dockerfile --target",
    '--target "$target"',
    "tests/fixtures/Dockerfile.platform-sandbox-l3",
    "tools/release/inspect-platform-oci-image.py",
    "tools/checks/verify-platform-sandbox-package-image.py",
    "--runner-platform-manifest-digest",
    "--package-platform-manifest-digest",
    "INSIGHT_KIND_PLATFORM_DIGEST=$runtime_digest",
    "INSIGHT_KIND_SANDBOX_RUNNER_DIGEST=$runner_digest",
    "INSIGHT_KIND_SANDBOX_PACKAGE_DIGEST=$package_digest",
    "tools/qualification/bootstrap-platform-kind-local.sh",
    "tools/qualification/run-productization-sandbox-qualification.sh",
    '--environment "$INSIGHT_KIND_OUTPUT_DIR/environment.json"',
    '--sandbox-evidence "$RUNNER_TEMP/productization-opensandbox-evidence.json"',
    '--aggregate-report "$RUNNER_TEMP/productization-10-of-10.json"',
    "productization-10-of-10-${{ github.sha }}",
    "Always remove the disposable Kind cluster",
    'kind delete cluster --name "$INSIGHT_KIND_CLUSTER_NAME"',
    "version: v3.19.0",
    "artifact_mode:",
    "signed-release-candidate",
    "prepare-productization-release-candidate.py",
    "go install github.com/google/go-containerregistry/cmd/crane@v0.20.6",
    "for component in runtime sandbox_runner console; do",
    '"$crane_bin" manifest "${subject}@${index_digest}"',
    'observed_raw_digest="sha256:$(shasum -a 256 "$raw_index"',
    '[[ "$observed_raw_digest" == "$index_digest" ]]',
    '.platform.os == "linux"',
    '.platform.architecture == "amd64"',
    '] | length == 1 and .[0].digest == $expected)',
    "release-bundle.sigstore.json",
    '"${subject}:build-${GITHUB_SHA}"',
    '"${subject}@${platform_digest}"',
    "verify-productization-console-image.py",
    "PRODUCTIZATION_RELEASE_CANDIDATE_BINARY",
    "INSIGHT_KIND_PLATFORM_INDEX_DIGEST=$runtime_index",
    "INSIGHT_KIND_SANDBOX_RUNNER_INDEX_DIGEST=$runner_index",
    'runtime_reference="${runtime_subject}@${runtime_platform}"',
    '--platform linux/amd64 --format=oci --annotate-ref',
    '"$runtime_reference" "$runtime_layout"',
    '"$runner_reference" "$runner_layout"',
    "INSIGHT_KIND_PLATFORM_OCI_ARCHIVE=$runtime_archive",
    "INSIGHT_KIND_SANDBOX_RUNNER_OCI_ARCHIVE=$runner_archive",
    '--output "type=oci,dest=$package_archive"',
    '."containerimage.digest"',
    "INSIGHT_KIND_SANDBOX_PACKAGE_OCI_ARCHIVE=$package_archive",
    '--release-candidate-closure "$PRODUCTIZATION_RELEASE_CANDIDATE_CLOSURE"',
    '--sandbox-environment "$INSIGHT_KIND_OUTPUT_DIR/environment.json"',
    '--release-candidate "$PRODUCTIZATION_RELEASE_CANDIDATE"',
    '--console-bundle "$PRODUCTIZATION_RELEASE_CANDIDATE_CONSOLE"',
    "productization-${{ env.PRODUCTIZATION_FEATURES }}-sandbox-evidence-${{ github.sha }}",
):
    if marker not in starter_journey:
        failures.append(f"starter journey qualification misses {marker!r}")
candidate_preparation = starter_journey.split(
    "- name: Prepare exact signed runtime and Sandbox runner for Kind", 1
)[-1].split("- name: Bootstrap fresh current-SHA Kind", 1)[0]
seed_release_name = "- name: Release consumed seed before the fresh public journey"
seed_release = starter_journey.split(seed_release_name, 1)[-1].split("- name:", 1)[0]
for marker in (
    "if: ${{ env.PRODUCTIZATION_SEED_IDENTITY != '' }}",
    "set -euo pipefail",
    "python3 tools/qualification/fixture_project.py cleanup",
    '--project "$RUNNER_TEMP/productization-kind-seed"',
    '--identity "$PRODUCTIZATION_SEED_IDENTITY"',
    '--insight-bin "$PRODUCTIZATION_SEED_BINARY"',
    "--project-name productization-kind-seed",
    '--logs-directory "$RUNNER_TEMP/productization-seed-consumed-logs"',
    'echo "PRODUCTIZATION_SEED_IDENTITY=" >> "$GITHUB_ENV"',
):
    if marker not in seed_release:
        failures.append(f"consumed Kind seed lifecycle misses {marker!r}")
seed_stages = ["- name: Run fail-closed real OpenSandbox L3 qualification", seed_release_name,
               "- name: Run fresh public CLI and real Gateway Console journey"]
if any(stage not in starter_journey for stage in seed_stages) or [starter_journey.find(stage) for stage in seed_stages] != sorted(starter_journey.find(stage) for stage in seed_stages):
    failures.append("consumed Kind seed must be released after L3 and before the fresh public journey")
if "always()" in seed_release or "continue-on-error:" in seed_release:
    failures.append("consumed seed cleanup must block a fresh journey on failure")
if "productization-seed-consumed-logs/*.log" not in starter_journey:
    failures.append("consumed seed diagnostics must be preserved with bounded log selection")
for forbidden in (
    "cargo build",
    "docker build --target runtime",
    "docker build --target sandbox-runner",
    "package_config_digest",
    "--from-source",
):
    if forbidden in candidate_preparation:
        failures.append(f"signed candidate preparation rebuilds or mislabels product artifact {forbidden!r}")

crane_installs = re.findall(
    r"go install github\.com/google/go-containerregistry/cmd/crane(?:@[^\s]+)?",
    starter_journey,
)
if crane_installs != [
    "go install github.com/google/go-containerregistry/cmd/crane@v0.20.6"
]:
    failures.append(f"candidate registry reader is not exactly pinned: {crane_installs!r}")

candidate_index_verification = starter_journey.split(
    "for component in runtime sandbox_runner console; do", 1
)[-1].split("runtime_subject=", 1)[0]
for marker in (
    '"$crane_bin" manifest "${subject}@${index_digest}" >"$raw_index"',
    'observed_raw_digest="sha256:$(shasum -a 256 "$raw_index"',
    '[[ "$observed_raw_digest" == "$index_digest" ]]',
    '.manifests | type == "array"',
    '.platform.os == "linux"',
    '.platform.architecture == "amd64"',
    '] | length == 1 and .[0].digest == $expected)',
):
    if marker not in candidate_index_verification:
        failures.append(f"candidate index closure misses {marker!r}")

for marker in (
    "platform_oci_archive=${INSIGHT_KIND_PLATFORM_OCI_ARCHIVE:-}",
    '[[ -z "$platform_oci_archive" || ! -f "$platform_oci_archive"',
    "kind_docker_images=(",
    "kind_image_listing()",
    "kind_image_digest_by_reference()",
    'LC_ALL=C docker exec --env LC_ALL=C "$node"',
    '$1 == exact_reference { print $3 }',
    'if [[ "$match_count" -ne 1 ]]; then',
    "find_kind_image_by_target_digest()",
    "import_exact_oci_image_into_kind()",
    'valid_kind_oci_repository "$platform_repository"',
    'valid_kind_oci_repository "$sandbox_runner_repository"',
    'valid_kind_oci_repository "$sandbox_package_repository"',
    'valid_kind_oci_repository "$repository"',
    '"$desired_reference" != "$repository@$expected_digest"',
    '--digests --base-name "$repository" --snapshotter=overlayfs - <"$archive"',
    '"$platform_oci_archive" "$platform_repository@$platform_digest" "$platform_digest"',
    'if [[ "$observed_digest" != "$expected_digest" ]]; then',
    'ctr --namespace=k8s.io content get "$expected_digest"',
    'ctr --namespace=k8s.io content get "$expected_config_digest"',
    'kind:(if $platform_index_digest == "" then "source_oci_manifest"',
    "signed candidate seed runtime config is missing; refusing source fallback",
    '"$insight_bin" qualification-aws dev --path "$seed_project" --features context,mcp,model,remote-capability --from-source',
):
    if marker not in kind_bootstrap:
        failures.append(f"Kind candidate import contract misses {marker!r}")
for forbidden in (
    'platform_import_image="insight-kind/platform-candidate:',
    'docker tag "$platform_image" "$platform_import_image"',
    'verify_local_image "$platform_image"',
    'verify_local_image "$sandbox_package_image"',
    '"docker.io/library/$platform_image" "docker.io/library/$platform_repository@$platform_digest"',
    '"docker.io/library/$sandbox_package_image"',
    "images inspect",
):
    if forbidden in kind_bootstrap:
        failures.append(f"Kind candidate import retains Docker-store fallback {forbidden!r}")

seed_fallback = kind_bootstrap.split(
    'if [[ ! -d "$seed_project/.insight/runtime/config" ]]; then', 1
)[-1].split("seed_runtime=", 1)[0]
candidate_seed_guard = 'if [[ -n "$platform_index_digest" ]]; then'
source_seed_build = '"$insight_bin" qualification-aws dev --path "$seed_project" --features context,mcp,model,remote-capability --from-source'
if candidate_seed_guard not in seed_fallback or source_seed_build not in seed_fallback:
    failures.append("Kind seed handling does not retain source mode and guard candidate mode")
elif seed_fallback.index(candidate_seed_guard) > seed_fallback.index(source_seed_build):
    failures.append("Kind candidate seed guard occurs after the source fallback")

if "tools/qualification/qualify-productization-first-run.py" not in journey_runner:
    failures.append("starter journey runner misses the lightweight first-Run qualifier")
for forbidden in ("cosign sign", "docker/build-push-action@", "docker push"):
    if forbidden in starter_journey:
        failures.append(
            f"starter journey qualification contains candidate-only operation {forbidden!r}"
        )
for marker in (
    "-p insight-platform-qualification-tests --test productization",
    '--aggregate-output "$aggregate_report"',
    "PLATFORM_PRODUCTIZATION_SANDBOX_EVIDENCE",
    "PLATFORM_PRODUCTIZATION_SANDBOX_ENVIRONMENT",
    "PLATFORM_PRODUCTIZATION_QUALIFICATION_RUN_ID",
    "PLATFORM_PRODUCTIZATION_ACTUAL_PROFILE",
    "PLATFORM_PRODUCTIZATION_PROFILE_DIGEST",
    "INSIGHT_CONSOLE_BUNDLE_ROOT",
    "dev_arguments+=(--offline)",
):
    if marker not in journey_runner:
        failures.append(f"productization runner misses {marker!r}")
if "-p insight-agent-platform --test productization" in journey_runner:
    failures.append("productization runner still targets the removed legacy root package")

for marker in (
    "productization-10-of-10:",
    "needs: assemble-release",
    "uses: ./.github/workflows/productization-journey.yml",
    "features: all",
    "artifact_mode: signed-release-candidate",
    "release_candidate_artifact: signed-release-candidate-${{ github.sha }}",
    "release_tag: ${{ github.ref_name }}",
    "needs: [assemble-release, development-profile-qualification, productization-10-of-10]",
    "productization-all-scenario-reports-${{ github.sha }}",
    "productization-10-of-10-${{ github.sha }}",
    "tools/checks/check-productization-scenario-reports.py",
    '--source-revision "$GITHUB_SHA"',
    "productization-all-sandbox-evidence-${{ github.sha }}",
    "--include-productization-qualification",
    "--productization-aggregate",
    "--productization-report-directory",
    "--productization-sandbox-evidence",
    "--productization-sandbox-environment",
    "--productization-release-candidate-bundle",
    "preliminary-release-bundle.json",
):
    if marker not in product_release:
        failures.append(f"product release misses required 10/10 evidence marker {marker!r}")
release_productization = product_release.split(
    "\n  productization-10-of-10:", 1
)[-1].split("\n  publish:", 1)[0]
if "permissions:\n      contents: read\n      packages: read" not in release_productization:
    failures.append("product release does not grant the reusable 10/10 package permission")

if "uses: ./.github/workflows/productization-journey.yml" in ci:
    failures.append("ordinary push/PR CI invokes the full Productization journey")
required_ci = ci.split("\n  required:", 1)[-1]
if "productization" in required_ci.lower():
    failures.append("ordinary required CI summary still depends on Productization evidence")
if "needs: [changes, quick, lint, test, cli, console, installation, policy]" not in required_ci:
    failures.append("required CI summary does not close the selected lane set")

if failures:
    raise SystemExit("\n".join(failures))
print("Productization path-aware CI contract passed.")
