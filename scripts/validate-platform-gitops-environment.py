#!/usr/bin/env python3
"""Validate the closed GitOps environment input used by a production candidate."""

import argparse
import hashlib
import json
import re
from pathlib import Path


SHA1 = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
TOP_LEVEL = {
    "schema_version", "environment_name", "environment_class",
    "application_repository", "application_commit",
    "qualification_profile_digest", "deployment", "dependencies", "secret_policy",
}
DEPLOYMENT = {
    "requires_multi_node", "requires_runsc", "requires_validating_admission_policy_v1",
    "wasi_node_selector", "gvisor_node_selector", "attestor_node_selector", "runtime_class",
}
DEPENDENCIES = {
    "postgresql", "nats", "object_storage", "key_management", "secret_management", "telemetry",
}
SECRET_POLICY = {"plaintext_in_git", "kubeconfig_in_git", "references_only"}


def canonical_digest(path: Path) -> str:
    value = json.loads(path.read_bytes())
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def require_keys(value: object, expected: set[str], name: str) -> dict:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(f"{name} must contain exactly {sorted(expected)}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--closure", required=True, type=Path)
    parser.add_argument("--application-repository", required=True)
    parser.add_argument("--application-commit", required=True)
    parser.add_argument("--qualification-profile", required=True, type=Path)
    args = parser.parse_args()

    closure = args.closure.resolve()
    manifest_path = closure / "environment.json"
    if not closure.is_dir() or not manifest_path.is_file() or manifest_path.is_symlink():
        raise ValueError("GitOps closure must contain a regular environment.json")
    if any(path.is_symlink() for path in closure.rglob("*")):
        raise ValueError("GitOps closure must not contain symbolic links")

    manifest = require_keys(json.loads(manifest_path.read_bytes()), TOP_LEVEL, "environment")
    if manifest["schema_version"] != 1:
        raise ValueError("environment schema_version must be 1")
    if manifest["environment_name"] != "production" or manifest["environment_class"] != "production":
        raise ValueError("candidate environment must be production")
    if manifest["application_repository"] != args.application_repository:
        raise ValueError("environment application_repository differs from the candidate repository")
    if not SHA1.fullmatch(args.application_commit) or manifest["application_commit"] != args.application_commit:
        raise ValueError("environment application_commit differs from the exact candidate commit")
    profile_digest = manifest["qualification_profile_digest"]
    if not isinstance(profile_digest, str) or not SHA256.fullmatch(profile_digest):
        raise ValueError("qualification_profile_digest is invalid")
    if profile_digest != canonical_digest(args.qualification_profile):
        raise ValueError("environment qualification_profile_digest differs from the checked-in profile")

    deployment = require_keys(manifest["deployment"], DEPLOYMENT, "deployment")
    for name in ("requires_multi_node", "requires_runsc", "requires_validating_admission_policy_v1"):
        if deployment[name] is not True:
            raise ValueError(f"deployment.{name} must be true")
    if deployment["runtime_class"] != "runsc":
        raise ValueError("deployment.runtime_class must be runsc")
    expected_selectors = {
        "wasi_node_selector": "insight.platform.node-restriction.kubernetes.io/sandbox-wasi",
        "gvisor_node_selector": "insight.platform.node-restriction.kubernetes.io/sandbox-gvisor",
        "attestor_node_selector": "insight.platform.node-restriction.kubernetes.io/sandbox-attestor",
    }
    for field, label in expected_selectors.items():
        if deployment[field] != {label: "true"}:
            raise ValueError(f"deployment.{field} is not the exact protected selector")

    dependencies = require_keys(manifest["dependencies"], DEPENDENCIES, "dependencies")
    if not all(isinstance(value, str) and value for value in dependencies.values()):
        raise ValueError("dependency classes must be non-empty strings")
    secret_policy = require_keys(manifest["secret_policy"], SECRET_POLICY, "secret_policy")
    if secret_policy != {
        "plaintext_in_git": False, "kubeconfig_in_git": False, "references_only": True,
    }:
        raise ValueError("secret_policy must forbid credentials and kubeconfigs in Git")

    print("Platform GitOps production environment closure passed.")


if __name__ == "__main__":
    main()
