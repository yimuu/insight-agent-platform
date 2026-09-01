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
    "requires_multi_node", "requires_opensandbox_kubernetes",
    "requires_validating_admission_policy_v1", "container_runtime",
    "sandbox_control_namespace", "sandbox_workload_namespace",
    "opensandbox_source_commit", "opensandbox_server_image_digest",
    "opensandbox_controller_image_digest", "opensandbox_execd_image_digest",
    "batchsandbox_crd_digest", "kubernetes_provider_template_digest",
    "sandbox_network_policy_digest",
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
    if manifest["schema_version"] != 2:
        raise ValueError("environment schema_version must be 2")
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
    for name in (
        "requires_multi_node", "requires_opensandbox_kubernetes",
        "requires_validating_admission_policy_v1",
    ):
        if deployment[name] is not True:
            raise ValueError(f"deployment.{name} must be true")
    expected = {
        "container_runtime": "containerd-runc",
        "sandbox_control_namespace": "platform-sandbox",
        "sandbox_workload_namespace": "platform-sandbox-workloads",
        "opensandbox_source_commit": "c39b814f36ded4c61d5ac6f9332ee4dfbab86c00",
        "opensandbox_server_image_digest": "sha256:ae8dfbb277f40a39ff01ef35e5e1c10675acfe0fa9db15259b8f323e5efab778",
        "opensandbox_controller_image_digest": "sha256:a9a5f73c1785ebd955336ffa313973a35c1a1b662cb7afc4ea82d92021b3532a",
        "opensandbox_execd_image_digest": "sha256:0d8f44cf4194732719aa79999d4b120c98bdab02bc61e9ad13f75f83af4c2684",
        "batchsandbox_crd_digest": "sha256:6a56fbec00a33acf30a4a9c3418172ad6ac1eba34d081881e6b5dd941cfa59d4",
        "kubernetes_provider_template_digest": "sha256:4203a99badbdd23d7d2684d316ac4011d7df424987c518f899750026f0b7de5a",
        "sandbox_network_policy_digest": "sha256:8e81f38951ef624c530650d5490ed2c8f7f0a058a42c5e87bb9463a43bbb5de0",
    }
    for field, value in expected.items():
        if deployment[field] != value:
            raise ValueError(f"deployment.{field} differs from the accepted OpenSandbox closure")

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
