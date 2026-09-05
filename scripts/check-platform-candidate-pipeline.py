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
    "Reject non-main application revision", 'test "$GITHUB_REF" = "refs/heads/main"',
    "platform-production-candidate.yml@refs/heads/main$",
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

expected_identity = (
    "^https://github.com/${GITHUB_REPOSITORY}/.github/workflows/"
    "platform-production-candidate.yml@refs/heads/main$"
)
certificate_identities = re.findall(
    r'--certificate-identity-regexp\s+"([^"]+)"', workflow
)
if len(certificate_identities) != 4 or any(
    identity != expected_identity for identity in certificate_identities
):
    failures.append(
        "candidate signature verification must bind all four subjects to the main workflow identity"
    )

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
    "sandbox_dispatcher", "opensandbox_server", "opensandbox_controller",
    "artifact_gateway", "artifact_data_worker", "artifact_maintenance", "egress_secret_broker",
):
    if f'"{role}"' not in generator:
        failures.append(f"candidate generator misses ComponentRole {role}")

if workflow.count("cosign sign-blob") < 2:
    failures.append("candidate workflow must independently sign the CandidateManifest and release bundle index")

production_bins = (
    "insight", "platform-schema", "platform-dev-bootstrap", "platform-callback-api", "platform-gateway",
    "platform-model-worker", "platform-context-worker", "platform-context-dataset-worker", "platform-remote-context-worker",
    "platform-subscription-context-worker", "platform-orchestration-worker",
    "platform-registry-validation-worker",
    "platform-capability-native-worker", "platform-capability-remote-worker",
    "platform-mcp-cleanup-worker", "platform-mcp-host", "platform-mcp-resource-host",
    "platform-mcp-discovery-worker", "platform-mcp-subscription-worker",
    "platform-artifact-data-worker", "platform-artifact-gateway",
    "platform-artifact-maintenance", "platform-egress-broker",
    "platform-security-authority", "platform-sandbox-dispatcher",
)
docker_run_instructions = re.findall(
    r"^RUN\s+.*?(?=^[A-Z][A-Z0-9_-]*(?:\s|$)|\Z)",
    dockerfile,
    flags=re.MULTILINE | re.DOTALL,
)
production_builds = [
    instruction
    for instruction in docker_run_instructions
    if "cargo build --locked --release --workspace" in instruction
]
dependency_builds = [
    instruction
    for instruction in docker_run_instructions
    if "cargo chef cook --locked --release --workspace --recipe-path recipe.json"
    in instruction
]
if len(production_builds) != 1:
    failures.append("Dockerfile must compile the production closure in one Cargo invocation")
if not production_builds:
    failures.append("Dockerfile production build must select binaries across the workspace")
for build_cache_marker in (
    "cargo install --locked --version 0.1.78 cargo-chef",
    "cargo chef prepare --recipe-path recipe.json",
    "cargo chef cook --locked --release --workspace --recipe-path recipe.json",
):
    if build_cache_marker not in dockerfile:
        failures.append(f"Dockerfile production build misses dependency cache stage {build_cache_marker}")
for binary in production_bins:
    if (
        len(production_builds) != 1
        or len(dependency_builds) != 1
        or f"--bin {binary}" not in production_builds[0]
        or f"--bin {binary}" not in dependency_builds[0]
    ):
        failures.append(f"Dockerfile production build misses binary {binary}")
for runner_boundary in ("FROM runtime-base AS sandbox-runner", "ENTRYPOINT [\"/usr/local/bin/platform-sandbox-runner\"]"):
    if runner_boundary not in dockerfile:
        failures.append(f"Dockerfile sandbox runner misses {runner_boundary}")
if "-p insight-platform-sandbox-runner --bin platform-sandbox-runner" not in dockerfile:
    failures.append("Dockerfile sandbox runner misses the isolated static Rust core build")
if "COPY --from=builder /workspace/target/release/platform-sandbox-runner /usr/local/bin/platform-sandbox-runner" in dockerfile:
    failures.append("generic runtime image still contains the unusable non-launcher Sandbox runner")

if failures:
    raise SystemExit("\n".join(failures))
print("Platform signed candidate pipeline contract passed.")
