#!/usr/bin/env python3
"""Fail closed when the protected product release pipeline loses release controls."""

import json
import re
from pathlib import Path


ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "Cargo.toml").is_file())
workflow = (ROOT / ".github/workflows/product-release.yml").read_text()
generator = (ROOT / "tools/release/build-product-release.py").read_text()
development_report = (ROOT / "tools/development/build-development-profile-performance.py").read_text()
failures = []


def job_block(name: str) -> str:
    match = re.search(
        rf"^  {re.escape(name)}:\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        workflow,
        flags=re.MULTILINE | re.DOTALL,
    )
    if match is None:
        failures.append(f"product release workflow misses job {name!r}")
        return ""
    return match.group("body")

for marker in (
    "macos-15-intel",
    "--prerelease --latest=false",
    "environment: product-release", "aarch64-apple-darwin", "x86_64-apple-darwin",
    "aarch64-unknown-linux-gnu", "x86_64-unknown-linux-gnu", "cargo build --locked --release",
    "insight version --json", "insight doctor --json", "platforms: linux/amd64,linux/arm64",
    "target: runtime", "target: sandbox-runner", "file: deploy/images/console.Dockerfile", "provenance: mode=max",
    "sbom: true", "cosign sign --yes", "cosign verify", "cosign sign-blob",
    "release-bundle.signature.json", "build-release-performance.py", "timeout-minutes: 10",
    "qualify-development-profile.sh", "development-profile-performance.json",
    "--release-assets", "signed-release-candidate-${{ github.sha }}",
    "--include-development-qualification", "Attest qualified ReleaseBundle",
    "--include-productization-qualification",
    "--productization-release-candidate-bundle",
    "preliminary-release-bundle.json",
    "--stabilization-seconds 300", 'node-version: "24.11.1"',
    "gh release create", "already exists and cannot be overwritten", "INSIGHT_RELEASE_PUBLIC_KEY_BASE64",
):
    if marker not in workflow:
        failures.append(f"product release workflow misses {marker!r}")

if workflow.count("file: deploy/images/platform.Dockerfile") != 2:
    failures.append("runtime and sandbox runner builds must select the current owning Dockerfile path")

if workflow.count('--public-key-base64="$RELEASE_PUBLIC_KEY"') != 2:
    failures.append("release signing must bind option-like base64url keys to their argument")

for forbidden in ("docker/setup-qemu-action@", "continue-on-error:", "runner: macos-13", "runner: macos-14", ":latest", ":candidate-", "docker build ", "cargo build --release --workspace"):
    if forbidden in workflow:
        failures.append(f"product release workflow contains forbidden marker {forbidden!r}")

for action in re.findall(r"^\s*-?\s*uses:\s*([^\s#]+)", workflow, flags=re.MULTILINE):
    if action.startswith("./"):
        continue
    revision = action.rsplit("@", 1)[-1]
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        failures.append(f"release action is not pinned to an immutable commit: {action}")

if workflow.count("docker/build-push-action@") != 3:
    failures.append("runtime, sandbox runner, and Console must each have one reusable BuildKit build")
if workflow.count("cache-to: type=gha,mode=max") != 2:
    failures.append("only runtime and Console should export their independent BuildKit caches")
if "REQUIRED_METADATA" not in generator or "validate_cli_archive" not in generator:
    failures.append("release generator lost metadata or archive closure validation")
for marker in ('"L4": "not_run"', '"L5": "not_run"', '"L6": "not_run"'):
    if marker not in development_report:
        failures.append(f"development performance report misses {marker}")

images_job = job_block("images")
native_job = job_block("native-images")
console_job = job_block("console-assets")
if re.search(r"^\s+if:", native_job + images_job + console_job, re.MULTILINE):
    failures.append("current image builds and assembly controls must run unconditionally")
expected_matrix = """    strategy:
      fail-fast: false
      matrix:
        include:
          - arch: amd64
            platform: linux/amd64
            machine: x86_64
            runner: ubuntu-24.04
          - arch: arm64
            platform: linux/arm64
            machine: aarch64
            runner: ubuntu-24.04-arm
"""
matrix = re.search(r"^    strategy:\n.*?(?=^    steps:)", native_job, re.MULTILINE | re.DOTALL)
if matrix is None or matrix[0] != expected_matrix or 'runs-on: ${{ matrix.runner }}' not in native_job:
    failures.append("native image builds require the exact AMD64 and ARM64 host matrix")
if 'test "$(uname -s)" = Linux' not in native_job or 'test "$(uname -m)" = "${{ matrix.machine }}"' not in native_job:
    failures.append("native image builds must verify the actual host architecture")
for marker in ("platforms: ${{ matrix.platform }}", "provenance: mode=max", "sbom: true",
               "assemble-native-release-images.py record-native"):
    if native_job.count(marker) != 2:
        failures.append(f"both native builds must preserve {marker!r}")
for component, image in (("runtime", "RUNTIME_IMAGE"), ("sandbox_runner", "SANDBOX_RUNNER_IMAGE")):
    if f"--component {component} --platform" not in native_job:
        failures.append(f"native {component} build must emit its owning record")
    expected_tag = "tags: ${{ env." + image + " }}:build-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}-${{ matrix.arch }}"
    if expected_tag not in native_job:
        failures.append(f"native {component} candidate tag must bind source, workflow attempt and platform")
if native_job.count("cache-from: type=gha,scope=product-release-runtime-${{ matrix.arch }}") != 2:
    failures.append("native BuildKit caches must be isolated by architecture")
for marker in ("name: native-images-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}-${{ matrix.arch }}",
               "path: ${{ runner.temp }}/native-builds"):
    if marker not in native_job:
        failures.append("native build records must use an exact current artifact identity")
if "needs: [console-assets, native-images]" not in images_job:
    failures.append("image assembly must depend on both current native builds and the single Console build")
for suffix in ("-amd64", "-arm64"):
    marker = "name: native-images-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}" + suffix
    if images_job.count(marker) != 1:
        failures.append("image assembly must download each exact current native artifact once")
console_artifact = "name: console-build-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}"
if console_job.count(console_artifact) != 1 or images_job.count(console_artifact) != 1:
    failures.append("Console assembly must consume the same source and workflow attempt archive")
if "pattern:" in images_job or "merge-multiple:" in images_job:
    failures.append("native image assembly cannot select artifacts by a broad pattern")
for marker in ("assemble-native-release-images.py prepare-console", "assemble-native-release-images.py merge-native"):
    if images_job.count(marker) != 1:
        failures.append(f"image assembly must invoke {marker!r} exactly once")
if not (images_job.find("prepare-console") < images_job.find("merge-native") < images_job.find("id: console")
        < images_job.find("Sign and verify exact image subjects")):
    failures.append("exact Console and native records must be verified before final image signing")
for key, value in (("RUNTIME_DIGEST", "${{ steps.native.outputs.runtime_digest }}"),
                   ("SANDBOX_RUNNER_DIGEST", "${{ steps.native.outputs.runner_digest }}")):
    bindings = re.findall(rf"^          {key}: (.+)$", images_job, re.MULTILINE)
    if not bindings or any(binding != value for binding in bindings):
        failures.append("downstream image evidence must bind the verified native merge outputs")
console_dockerfile = (ROOT / "deploy/images/console.Dockerfile").read_text()
if (re.findall(r"^([A-Z]+)\b", console_dockerfile, re.MULTILINE) != ["FROM", "COPY", "LABEL", "LABEL"]
        or not console_dockerfile.startswith("FROM scratch\nCOPY dist/ /console/\n")
        or "# syntax" in console_dockerfile or "#syntax" in console_dockerfile):
    failures.append("Console must remain a scratch asset copy without target-architecture execution")
expected_budgets = {"cli_build": 1200, "console_build": 300, "runtime_build_push": 3600,
                    "sandbox_runner_build_push": 1800, "console_image_build_push": 300,
                    "sbom": 1200, "provenance": 300, "cosign": 600, "cold_pull": 300, "warm_reuse": 60}
if json.loads((ROOT / "deploy/release/performance-budgets-v1.json").read_text()) != {"schema_version": 1, "seconds": expected_budgets}:
    failures.append("release build budgets differ from the reviewed qualification limits")
if "tools/tests/test_native_release_images.py" not in (ROOT / ".github/workflows/ci.yml").read_text():
    failures.append("CI must verify native source closure, proof preservation and elapsed waiting")
for phase, prefix in (("runtime_build_push", "runtime"), ("sandbox_runner_build_push", "runner")):
    if f'{{"name": "{phase}", "duration_seconds": elapsed("{prefix}")}}' not in images_job:
        failures.append("native performance must consume complete verified start-to-ready intervals")
if "['build', '--locked', '-p', 'insight-platform-agent-compiler-wasm'" not in (ROOT / "apps/console/scripts/build-agent-compiler.mjs").read_text():
    failures.append("Console release WASM build must preserve the exact Cargo.lock dependency closure")
wasm_binding = re.findall(r'^wasm-bindgen\s*=\s*"=([0-9]+\.[0-9]+\.[0-9]+)"$', (ROOT / "crates/authoring/platform-agent-compiler-wasm/Cargo.toml").read_text(), re.MULTILINE)
if len(wasm_binding) != 1:
    raise SystemExit("WASM owner must pin exactly one wasm-bindgen version")
wasm_binding = wasm_binding[0]
for marker in ("targets: wasm32-unknown-unknown", f"cargo install --locked wasm-bindgen-cli --version {wasm_binding}"):
    if marker not in console_job:
        failures.append(f"Console release build lacks its owning Rust/WASM toolchain input: {marker}")
assemble_job = job_block("assemble-release")
qualification_job = job_block("development-profile-qualification")
publish_job = job_block("publish")
if images_job.count("tags: ${{ env.") != 1 or "tags: ${{ env.CONSOLE_IMAGE }}:build-${{ github.sha }}" not in images_job:
    failures.append("Console image must publish only its commit-scoped candidate tag before qualification")
if "needs: [cli, images]" not in assemble_job:
    failures.append("signed release candidate must depend on CLI and image candidate builds")
if "needs: assemble-release" not in qualification_job or "needs: publish" in qualification_job:
    failures.append("development profile qualification must consume the signed candidate before publish")
if (
    "needs: [assemble-release, development-profile-qualification, productization-10-of-10]"
    not in publish_job
):
    failures.append(
        "immutable publish must depend on the signed candidate and both successful qualifications"
    )
if "gh release create" in assemble_job or "gh release create" in qualification_job:
    failures.append("GitHub Release creation is forbidden before qualification")
if publish_job.count("docker buildx imagetools create --tag") != 3:
    failures.append("publish must promote all three exact image digests to release tags")
if "cannot prove release image tag" not in publish_job or "manifest unknown|not found" not in publish_job:
    failures.append("release tag reservation must fail closed on ambiguous registry errors")
if "Finalize and sign qualified ReleaseBundle" not in publish_job:
    failures.append("qualified evidence must be included in a newly signed final ReleaseBundle")
for marker in (
    "--sandbox-evidence",
    "--sandbox-environment",
    "--productization-release-candidate-bundle",
    "preliminary-release-bundle.json",
):
    if marker not in publish_job:
        failures.append(f"qualified publish does not bind required evidence input {marker!r}")
if "gh release create" not in publish_job:
    failures.append("qualified publish job must create the immutable GitHub Release")

anonymous_gate = re.search(
    r"^      - name: Verify exact candidate indexes are anonymously readable\n(?P<body>.*?)(?=^      - |\Z)",
    publish_job, re.MULTILINE | re.DOTALL,
)
expected_anonymous_body = '''        timeout-minutes: 2
        shell: bash
        run: |
          set -euo pipefail
          python3 tools/release/verify-public-release-images.py \\
            --assets "$RUNNER_TEMP/release-assets" \\
            --repository "$GITHUB_REPOSITORY" \\
            --release-tag "$GITHUB_REF_NAME" \\
            --revision "$GITHUB_SHA"
'''
if anonymous_gate is None or anonymous_gate["body"] != expected_anonymous_body:
    failures.append("public release requires the unconditional fail-closed exact index anonymous gate")
elif not (
    publish_job.find('cosign verify-blob --bundle') < anonymous_gate.start()
    < publish_job.find('Finalize and sign qualified ReleaseBundle')
    < publish_job.find('docker buildx imagetools create --tag')
    < publish_job.find('gh release create')
):
    failures.append("anonymous index verification must follow candidate verification and precede publication")
if "tools/tests/test_public_release_images.py" not in (ROOT / ".github/workflows/ci.yml").read_text():
    failures.append("CI must verify anonymous image access boundaries and pipeline enforcement")

if failures:
    raise SystemExit("\n".join(failures))
print("Product release pipeline contract passed.")
