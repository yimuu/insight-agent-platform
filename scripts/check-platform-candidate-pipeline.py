#!/usr/bin/env python3
"""Fail closed when the signed production-candidate workflow loses required controls."""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
workflow = (ROOT / ".github/workflows/platform-production-candidate.yml").read_text()
generator = (ROOT / "scripts/build-platform-production-candidate.py").read_text()
failures = []

required_workflow = (
    "id-token: write", "packages: write", "provenance: mode=max", "sbom: true",
    "docker/build-push-action@", "sigstore/cosign-installer@", "cosign sign --yes",
    "cosign attest --yes", "cosign verify", "cosign verify-attestation", "cosign sign-blob",
    "actions/attest@", "gh attestation verify", "--predicate-type https://spdx.dev/Document",
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

for action in re.findall(r"^\s*-?\s*uses:\s*([^\s#]+)", workflow, flags=re.MULTILINE):
    revision = action.rsplit("@", 1)[-1]
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        failures.append(f"candidate workflow action is not pinned to a commit: {action}")

for role in (
    "management_api", "runtime_api", "scheduler_recovery", "model_worker",
    "capability_native_worker", "capability_remote_worker", "context_worker", "mcp_host",
    "sandbox_controller", "sandbox_wasi_executor", "sandbox_gvisor_executor",
    "artifact_gateway", "artifact_data_worker", "artifact_maintenance", "egress_secret_broker",
):
    if f'"{role}"' not in generator:
        failures.append(f"candidate generator misses ComponentRole {role}")

if workflow.count("cosign sign-blob") < 2:
    failures.append("candidate workflow must independently sign the CandidateManifest and release bundle index")

if failures:
    raise SystemExit("\n".join(failures))
print("Platform signed candidate pipeline contract passed.")
