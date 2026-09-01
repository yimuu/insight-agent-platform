#!/usr/bin/env python3
"""Fail closed when the protected product release pipeline loses Spec 05 controls."""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
workflow = (ROOT / ".github/workflows/product-release.yml").read_text()
generator = (ROOT / "scripts/build-product-release.py").read_text()
development_report = (ROOT / "scripts/build-development-profile-performance.py").read_text()
failures = []

for marker in (
    "environment: product-release", "aarch64-apple-darwin", "x86_64-apple-darwin",
    "aarch64-unknown-linux-gnu", "x86_64-unknown-linux-gnu", "cargo build --locked --release",
    "insight version --json", "insight doctor --json", "platforms: linux/amd64,linux/arm64",
    "target: runtime", "target: sandbox-runner", "Dockerfile.release", "provenance: mode=max",
    "sbom: true", "cosign sign --yes", "cosign verify", "cosign sign-blob",
    "release-bundle.signature.json", "build-release-performance.py", "timeout-minutes: 10",
    "qualify-development-profile.sh", "development-profile-performance.json",
    "--stabilization-seconds 300",
    "gh release create", "already exists and cannot be overwritten", "INSIGHT_RELEASE_PUBLIC_KEY_BASE64",
):
    if marker not in workflow:
        failures.append(f"product release workflow misses {marker!r}")

for forbidden in (":latest", ":candidate-", "docker build ", "cargo build --release --workspace"):
    if forbidden in workflow:
        failures.append(f"product release workflow contains forbidden marker {forbidden!r}")

for action in re.findall(r"^\s*-?\s*uses:\s*([^\s#]+)", workflow, flags=re.MULTILINE):
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

if failures:
    raise SystemExit("\n".join(failures))
print("Product release pipeline contract passed.")
