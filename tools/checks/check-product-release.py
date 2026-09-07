#!/usr/bin/env python3
"""Fail closed when the protected product release pipeline loses release controls."""

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
    "docker/setup-qemu-action@", "platforms: arm64", "macos-15-intel",
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

for forbidden in ("runner: macos-13", "runner: macos-14", ":latest", ":candidate-", "docker build ", "cargo build --release --workspace"):
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
qemu = re.search(r"^      - uses: docker/setup-qemu-action@[^\n]+\n(?P<inputs>.*?)(?=^      - |\Z)", images_job, re.MULTILINE | re.DOTALL)
expected_qemu_image = "docker.io/tonistiigi/binfmt@sha256:400a4873b838d1b89194d982c45e5fb3cda4593fbfd7e08a02e76b03b21166f0"
if qemu is None or not re.search(rf"^          image: {re.escape(expected_qemu_image)}$", qemu["inputs"], re.MULTILINE) or not re.search(r"^          platforms: arm64$", qemu["inputs"], re.MULTILINE):
    failures.append("QEMU must install only arm64 from the exact reviewed privileged image")
if qemu is not None and images_job.find("docker/setup-buildx-action@") < qemu.start():
    failures.append("QEMU must be installed before the multi-platform BuildKit builder")
if "['build', '--locked', '-p', 'insight-platform-agent-compiler-wasm'" not in (ROOT / "apps/console/scripts/build-agent-compiler.mjs").read_text():
    failures.append("Console release WASM build must preserve the exact Cargo.lock dependency closure")
wasm_binding = re.findall(r'^wasm-bindgen\s*=\s*"=([0-9]+\.[0-9]+\.[0-9]+)"$', (ROOT / "crates/authoring/platform-agent-compiler-wasm/Cargo.toml").read_text(), re.MULTILINE)
if len(wasm_binding) != 1:
    raise SystemExit("WASM owner must pin exactly one wasm-bindgen version")
wasm_binding = wasm_binding[0]
for marker in ("targets: wasm32-unknown-unknown", f"cargo install --locked wasm-bindgen-cli --version {wasm_binding}"):
    if marker not in images_job:
        failures.append(f"Console release build lacks its owning Rust/WASM toolchain input: {marker}")
assemble_job = job_block("assemble-release")
qualification_job = job_block("development-profile-qualification")
publish_job = job_block("publish")
if images_job.count("tags: ${{ env.") != 3 or images_job.count(":build-${{ github.sha }}") != 3:
    failures.append("image builds must publish only commit-scoped candidate tags before qualification")
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
