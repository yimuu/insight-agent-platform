#!/usr/bin/env python3
"""Build one deterministic, closed Platform v2 production CandidateManifest bundle."""

import argparse
import hashlib
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
GIT_COMMIT = re.compile(r"^sha1:[0-9a-f]{40}$")
TIMESTAMP = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$")
COMPONENT_ROLES = (
    "management_api", "runtime_api", "scheduler_recovery", "model_worker",
    "capability_native_worker", "capability_remote_worker", "registry_validation_worker", "context_worker", "mcp_host",
    "sandbox_controller", "sandbox_wasi_executor", "sandbox_gvisor_executor",
    "artifact_gateway", "artifact_data_worker", "artifact_maintenance", "egress_secret_broker",
)
WORKERS = (
    ("orchestration-worker", "orchestration", 16, 2, "runtime"),
    ("registry-validation-worker", "registry_validation", 4, 1, "runtime"),
    ("model-worker", "model", 16, 2, "runtime"),
    ("capability.native", "capability_native", 4, 1, "runtime"),
    ("capability.remote", "capability_remote", 4, 1, "runtime"),
    ("context-worker", "context", 8, 2, "runtime"),
    ("sandbox-executor.wasi", "sandbox", 4, 1, "runtime"),
    ("sandbox-executor.gvisor", "sandbox", 4, 1, "sandbox_guest"),
)
BASE_DEPLOYMENT_PATHS = (Path("Dockerfile"), Path("deploy/helm"))
POLICY_PATHS = (
    Path("Cargo.lock"), Path("deny.toml"), Path("Dockerfile"),
    Path(".github/workflows/platform-production-candidate.yml"),
    Path(".github/workflows/product-release.yml"),
    Path("release"),
    Path("scripts/build-platform-production-candidate.py"),
    Path("scripts/build-platform-release-bundle.py"),
    Path("scripts/build-product-release.py"),
    Path("scripts/build-release-image-metadata.py"),
    Path("scripts/build-release-performance.py"),
    Path("scripts/build-development-profile-performance.py"),
    Path("scripts/qualify-development-profile.sh"),
    Path("scripts/sign-product-release.py"),
    Path("scripts/check-platform-candidate-pipeline.py"),
    Path("scripts/check-product-release.py"),
    Path("scripts/check-crate-boundaries.sh"),
    Path("scripts/check-platform-security-deployment.sh"),
    Path("scripts/check-platform-observability-redaction.py"),
)


def canonical_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def canonical_digest(value):
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def closed_json(path):
    def reject_duplicate(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key {key!r} in {path}")
            result[key] = value
        return result
    return json.loads(path.read_bytes(), object_pairs_hook=reject_duplicate)


def files_under(paths):
    files = []
    for relative in paths:
        path = ROOT / relative
        if path.is_dir():
            files.extend(item for item in path.rglob("*") if item.is_file())
        elif path.is_file():
            files.append(path)
        else:
            raise ValueError(f"candidate closure input is missing: {relative}")
    return sorted(set(files), key=lambda item: item.relative_to(ROOT).as_posix())


def tree_digest(paths):
    closure = [{
        "path": path.relative_to(ROOT).as_posix(),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    } for path in files_under(paths)]
    return canonical_digest(closure)


def external_tree_digest(root):
    files = sorted((item for item in root.rglob("*") if item.is_file()), key=lambda item: item.relative_to(root).as_posix())
    closure = [{
        "path": path.relative_to(root).as_posix(),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    } for path in files]
    return canonical_digest(closure)


def require(pattern, value, name):
    if not pattern.fullmatch(value):
        raise ValueError(f"{name} has an invalid closed format")
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-image-digest", required=True)
    parser.add_argument("--sandbox-guest-image-digest", required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--created-at", required=True)
    parser.add_argument("--environment-closure", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    runtime = require(DIGEST, args.runtime_image_digest, "runtime image digest")
    sandbox_guest = require(DIGEST, args.sandbox_guest_image_digest, "sandbox guest image digest")
    git_commit = require(GIT_COMMIT, args.git_commit, "git commit")
    created_at = require(TIMESTAMP, args.created_at, "created_at")
    environment_closure = args.environment_closure.resolve()
    if not environment_closure.is_dir() or not any(
        item.is_file() for item in environment_closure.rglob("*")
    ):
        raise ValueError("environment closure must be a non-empty directory")

    contract_manifest = closed_json(ROOT / "contracts/platform-v1/manifest.json")
    contract_digest = require(DIGEST, contract_manifest["contract_digest"], "contract digest")
    hard_limits = closed_json(ROOT / "contracts/platform-v1/limits/q1-50.json")
    qualification = closed_json(
        ROOT / "contracts/platform-v1/qualification/production-release-profile.json"
    )

    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    worker_dir = output / "worker-manifests"
    worker_dir.mkdir(exist_ok=True)
    worker_digests = []
    for role, work_class, concurrency, reserved, adapter in WORKERS:
        manifest = {
            "manifest_version": 1,
            "worker_role": role,
            "work_class": work_class,
            "adapter_runtime_digest": runtime if adapter == "runtime" else sandbox_guest,
            "protocol_version": 1,
            "max_concurrency": concurrency,
            "critical_control_reserved_slots": reserved,
        }
        (worker_dir / f"{role}.json").write_bytes(canonical_bytes(manifest) + b"\n")
        worker_digests.append(canonical_digest(manifest))

    candidate = {
        "git_commit": git_commit,
        "contract_digest": contract_digest,
        "database_schema_version": 1,
        "component_images": {role: runtime for role in COMPONENT_ROLES},
        "worker_manifests": sorted(worker_digests),
        "deployment_config_digest": canonical_digest({
            "application": tree_digest(BASE_DEPLOYMENT_PATHS),
            "environment": external_tree_digest(environment_closure),
        }),
        "hard_limit_profile_digest": canonical_digest(hard_limits),
        "policy_baseline_digest": tree_digest(POLICY_PATHS),
        "qualification_profile_digest": canonical_digest(qualification),
        "created_at": created_at,
    }
    candidate_path = output / "candidate-manifest.json"
    candidate_path.write_bytes(canonical_bytes(candidate) + b"\n")
    (output / "candidate-manifest.sha256").write_text(
        f"{hashlib.sha256(candidate_path.read_bytes()).hexdigest()}  candidate-manifest.json\n"
    )


if __name__ == "__main__":
    main()
