#!/usr/bin/env python3
"""Fail closed when the signed production-candidate workflow loses required controls."""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
workflow = (ROOT / ".github/workflows/platform-production-candidate.yml").read_text()
generator = (ROOT / "scripts/build-platform-production-candidate.py").read_text()
dockerfile = (ROOT / "Dockerfile").read_text()
failures = []

required_workflow = (
    "id-token: write", "packages: write", "provenance: mode=max", "sbom: true",
    "docker/build-push-action@", "sigstore/cosign-installer@", "cosign sign --yes",
    "cosign verify", "cosign sign-blob", "cache-from: type=gha",
    "cache-to: type=gha,mode=max", "Sign exact image subjects",
    "Verify exact image signatures", "Generate exact SPDX SBOM files",
    "actions/attest@", "gh attestation verify", "GH_TOKEN: ${{ github.token }}",
    "--predicate-type https://spdx.dev/Document",
    "validate-production-candidate", "candidate-manifest.json", "migration-baseline.sql",
    "qualification-tests.txt", "release-bundle-manifest.json", "cosign verify-blob",
    "environment_repository", "environment_commit", "ENVIRONMENT_REPOSITORY_READ_SSH_KEY",
    "validate-platform-gitops-environment.py", "--environment-closure", "rev-parse HEAD",
    "actions/upload-artifact@", "environment: platform-production-candidate",
)
for marker in required_workflow:
    if marker not in workflow:
        failures.append(f"candidate workflow misses {marker!r}")

for forbidden in ("docker build ", ":latest", "kubectl apply", "helm upgrade", "git push"):
    if forbidden in workflow:
        failures.append(f"candidate workflow contains forbidden release mutation {forbidden!r}")

for redundant in ("cosign attest --yes", "cosign verify-attestation"):
    if redundant in workflow:
        failures.append(f"candidate workflow duplicates GitHub SBOM attestation with {redundant!r}")

if workflow.count("cache-from: type=gha,scope=platform-production-candidate") < 2:
    failures.append("both candidate image builds must restore the shared BuildKit cache")
if workflow.count("cache-to: type=gha,mode=max,scope=platform-production-candidate") != 1:
    failures.append("the expensive runtime build must export the shared BuildKit cache exactly once")

for action in re.findall(r"^\s*-?\s*uses:\s*([^\s#]+)", workflow, flags=re.MULTILINE):
    revision = action.rsplit("@", 1)[-1]
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        failures.append(f"candidate workflow action is not pinned to a commit: {action}")

for role in (
    "management_api", "runtime_api", "scheduler_recovery", "model_worker",
    "capability_native_worker", "capability_remote_worker", "registry_validation_worker", "context_worker", "mcp_host",
    "sandbox_controller", "sandbox_wasi_executor", "sandbox_gvisor_executor",
    "artifact_gateway", "artifact_data_worker", "artifact_maintenance", "egress_secret_broker",
):
    if f'"{role}"' not in generator:
        failures.append(f"candidate generator misses ComponentRole {role}")

if workflow.count("cosign sign-blob") < 2:
    failures.append("candidate workflow must independently sign the CandidateManifest and release bundle index")

production_bins = (
    "insight-agent-platform", "platform-callback-api", "platform-gateway",
    "platform-model-worker", "platform-context-worker", "platform-remote-context-worker",
    "platform-subscription-context-worker", "platform-orchestration-worker",
    "platform-registry-validation-worker",
    "platform-capability-native-worker", "platform-capability-remote-worker",
    "platform-mcp-cleanup-worker", "platform-mcp-host", "platform-mcp-resource-host",
    "platform-mcp-discovery-worker", "platform-mcp-subscription-worker",
    "platform-artifact-data-worker", "platform-artifact-gateway",
    "platform-artifact-maintenance", "platform-egress-broker",
    "platform-security-authority", "platform-sandbox-controller",
    "platform-sandbox-attestor", "platform-sandbox-executor", "platform-sandbox-guest",
)
if dockerfile.count("cargo build --locked --release") != 1:
    failures.append("Dockerfile must compile the production closure in one Cargo invocation")
if "cargo build --locked --release --workspace" not in dockerfile:
    failures.append("Dockerfile production build must select binaries across the workspace")
for build_cache_marker in (
    "cargo install --locked --version 0.1.78 cargo-chef",
    "cargo chef prepare --recipe-path recipe.json",
    "cargo chef cook --locked --release --workspace --recipe-path recipe.json",
):
    if build_cache_marker not in dockerfile:
        failures.append(f"Dockerfile production build misses dependency cache stage {build_cache_marker}")
for binary in production_bins:
    if dockerfile.count(f"--bin {binary}") != 2:
        failures.append(f"Dockerfile production build misses binary {binary}")
for runtime_package in ("python3=3.9.2-3", "nodejs=12.22.12~dfsg-1~deb11u8"):
    if runtime_package not in dockerfile:
        failures.append(f"Dockerfile sandbox guest misses frozen runtime package {runtime_package}")

if failures:
    raise SystemExit("\n".join(failures))
print("Platform signed candidate pipeline contract passed.")
