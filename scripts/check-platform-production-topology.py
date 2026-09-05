#!/usr/bin/env python3
"""Fail-closed OpenSandbox Kubernetes topology preflight for Platform v2 L4."""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path


MAX_INPUT_BYTES = 8 * 1024 * 1024
EXECUTION_ARCHITECTURES = {"amd64", "arm64"}
CONTROL_NAMESPACE = "platform-sandbox"
WORKLOAD_NAMESPACE = "platform-sandbox-workloads"
SOURCE_COMMIT = "c39b814f36ded4c61d5ac6f9332ee4dfbab86c00"
CRD_DIGEST = "sha256:176f3ccba68f75fc8311d34a49551b78e9743659a28d794ecb7f24605675d1af"
EXPECTED_SERVER_RULES = [
    {"apiGroups": ["sandbox.opensandbox.io"], "resources": ["batchsandboxes"], "verbs": ["create", "delete", "get", "list"]},
    {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]},
    {"apiGroups": [""], "resources": ["events"], "verbs": ["get", "list"]},
]
EXPECTED_SERVER_NAMESPACE_RULES = [{
    "apiGroups": [""],
    "resourceNames": [WORKLOAD_NAMESPACE],
    "resources": ["namespaces"],
    "verbs": ["get"],
}]
EXPECTED_CONTROLLER_LEADER_RULES = [
    {"apiGroups": ["coordination.k8s.io"], "resources": ["leases"], "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
    {"apiGroups": [""], "resources": ["events"], "verbs": ["create", "patch"]},
]
EXPECTED_CONTROLLER_RULES = [
    {"apiGroups": [""], "resources": ["pods"], "verbs": ["create", "delete", "get", "list", "patch", "update", "watch"]},
    {"apiGroups": [""], "resources": ["pods/status"], "verbs": ["get", "patch", "update"]},
    {"apiGroups": [""], "resources": ["events"], "verbs": ["create", "patch"]},
    {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["delete", "get", "list", "patch", "update", "watch"]},
    {"apiGroups": ["batch"], "resources": ["jobs/status"], "verbs": ["get", "patch", "update"]},
    {"apiGroups": ["sandbox.opensandbox.io"], "resources": ["batchsandboxes"], "verbs": ["delete", "get", "list", "patch", "update", "watch"]},
    {"apiGroups": ["sandbox.opensandbox.io"], "resources": ["pools", "sandboxsnapshots"], "verbs": ["delete", "get", "list", "patch", "update", "watch"]},
    {"apiGroups": ["sandbox.opensandbox.io"], "resources": ["batchsandboxes/finalizers", "pools/finalizers", "sandboxsnapshots/finalizers"], "verbs": ["update"]},
    {"apiGroups": ["sandbox.opensandbox.io"], "resources": ["batchsandboxes/status", "pools/status", "sandboxsnapshots/status"], "verbs": ["get", "patch", "update"]},
]


class DuplicateKey(ValueError):
    pass


def strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKey(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def load(path):
    raw = Path(path).read_bytes()
    if len(raw) > MAX_INPUT_BYTES:
        raise ValueError(f"{path} exceeds {MAX_INPUT_BYTES} bytes")
    return json.loads(raw, object_pairs_hook=strict_object)


def list_items(document, kind):
    if not isinstance(document, dict) or not isinstance(document.get("items"), list):
        raise ValueError(f"{kind} inventory is not a Kubernetes List")
    items = document["items"]
    if any(not isinstance(item, dict) or item.get("kind") != kind for item in items):
        raise ValueError(f"{kind} inventory contains another resource kind")
    return items


def ready_schedulable_nodes(nodes):
    result = []
    for node in list_items(nodes, "Node"):
        conditions = node.get("status", {}).get("conditions", [])
        ready = any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in conditions
        )
        if ready and node.get("spec", {}).get("unschedulable") is not True:
            result.append(node)
    return result


def validate_version(version, failures):
    pattern = re.compile(
        r"v(?P<major>[0-9]+)\.(?P<minor>[0-9]+)(?:\.[0-9]+)?(?:[-+][0-9A-Za-z.-]+)?"
    )
    server = version.get("serverVersion", {}).get("gitVersion")
    client = version.get("clientVersion", {}).get("gitVersion")
    server_match = pattern.fullmatch(server or "")
    client_match = pattern.fullmatch(client or "")
    if not server_match:
        failures.append("Kubernetes server version is missing or non-canonical")
    if not client_match:
        failures.append("kubectl client version is missing or non-canonical")
    if server_match and client_match and (
        server_match.group("major") != client_match.group("major")
        or abs(int(server_match.group("minor")) - int(client_match.group("minor"))) > 1
    ):
        failures.append("kubectl client/server version skew exceeds one minor release")
    return client, server


def batchsandbox_crd_digest(crd):
    """Digest the reviewed CRD contract while excluding API-server metadata/status/defaults."""
    spec = crd.get("spec", {})
    if not isinstance(spec, dict):
        raise ValueError("BatchSandbox CRD spec is malformed")
    names = spec.get("names", {})
    versions = spec.get("versions", [])
    if not isinstance(names, dict) or not isinstance(versions, list):
        raise ValueError("BatchSandbox CRD spec is malformed")
    normalized_versions = []
    for version in versions:
        if not isinstance(version, dict):
            raise ValueError("BatchSandbox CRD version is malformed")
        normalized_versions.append({
            key: version[key]
            for key in (
                "additionalPrinterColumns", "name", "schema", "served", "storage", "subresources"
            )
            if key in version
        })
    contract = {
        "group": spec.get("group"),
        "names": names,
        "scope": spec.get("scope"),
        "versions": normalized_versions,
    }
    canonical = json.dumps(
        contract, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


def validate_batchsandbox_crd(crd, failures, expected_digest=CRD_DIGEST):
    if crd.get("apiVersion") != "apiextensions.k8s.io/v1" or crd.get("kind") != "CustomResourceDefinition":
        failures.append("BatchSandbox CRD must use apiextensions.k8s.io/v1")
    metadata = crd.get("metadata", {})
    if metadata.get("name") != "batchsandboxes.sandbox.opensandbox.io":
        failures.append("BatchSandbox CRD identity drifted")
    annotations = metadata.get("annotations", {})
    if annotations.get("insight.platform/upstream-commit") != SOURCE_COMMIT:
        failures.append("BatchSandbox CRD is not bound to the reviewed OpenSandbox source commit")
    spec = crd.get("spec", {})
    if spec.get("group") != "sandbox.opensandbox.io" or spec.get("scope") != "Namespaced":
        failures.append("BatchSandbox CRD group or scope drifted")
    names = spec.get("names", {})
    if names.get("kind") != "BatchSandbox" or names.get("plural") != "batchsandboxes":
        failures.append("BatchSandbox CRD names drifted")
    versions = spec.get("versions", [])
    selected = [
        version for version in versions
        if version.get("name") == "v1alpha1"
        and version.get("served") is True
        and version.get("storage") is True
    ]
    if len(selected) != 1:
        failures.append("BatchSandbox CRD must serve exactly one storage v1alpha1 contract")
    observed_digest = batchsandbox_crd_digest(crd)
    if observed_digest != expected_digest:
        failures.append("BatchSandbox CRD normalized contract digest drifted")
    established = any(
        condition.get("type") == "Established" and condition.get("status") == "True"
        for condition in crd.get("status", {}).get("conditions", [])
    )
    if not established:
        failures.append("BatchSandbox CRD is not Established")
    return observed_digest


def validate_services(services, failures):
    required = {
        (CONTROL_NAMESPACE, "sandbox-dispatcher"),
        (CONTROL_NAMESPACE, "opensandbox-server"),
        (CONTROL_NAMESPACE, "opensandbox-controller-metrics"),
    }
    observed = set()
    for service in list_items(services, "Service"):
        metadata = service.get("metadata", {})
        identity = (metadata.get("namespace"), metadata.get("name"))
        labels = metadata.get("labels", {})
        platform_service = (
            identity in required
            or labels.get("app.kubernetes.io/name") == "insight-platform-sandbox"
        )
        if not platform_service:
            continue
        observed.add(identity)
        spec = service.get("spec", {})
        if spec.get("type", "ClusterIP") != "ClusterIP":
            failures.append(f"sandbox service {identity[0]}/{identity[1]} is not internal ClusterIP")
        if spec.get("externalIPs") or spec.get("externalName") or spec.get("loadBalancerIP"):
            failures.append(f"sandbox service {identity[0]}/{identity[1]} exposes an external address")
        if any("nodePort" in port for port in spec.get("ports", [])):
            failures.append(f"sandbox service {identity[0]}/{identity[1]} exposes a node port")
    missing = required - observed
    if missing:
        failures.append(f"required internal sandbox services are missing: {sorted(missing)}")


def validate_ingresses(ingresses, failures):
    for ingress in list_items(ingresses, "Ingress"):
        metadata = ingress.get("metadata", {})
        if metadata.get("namespace") in {CONTROL_NAMESPACE, WORKLOAD_NAMESPACE}:
            failures.append("sandbox namespaces must not contain any public Ingress")


def metadata_identity(resource):
    metadata = resource.get("metadata", {})
    name = metadata.get("name")
    namespace = metadata.get("namespace")
    if not isinstance(name, str) or not name:
        raise ValueError(f"{resource.get('kind', 'resource')} is missing metadata.name")
    return namespace, name


def exact_subject(binding, namespace, name):
    return binding.get("subjects") == [{
        "kind": "ServiceAccount",
        "name": name,
        "namespace": namespace,
    }]


def validate_sandbox_security(
    service_accounts,
    roles,
    role_bindings,
    cluster_roles,
    cluster_role_bindings,
    admissions,
    admission_bindings,
    failures,
):
    account_items = list_items(service_accounts, "ServiceAccount")
    expected_accounts = {
        (CONTROL_NAMESPACE, "sandbox-dispatcher"): False,
        (CONTROL_NAMESPACE, "opensandbox-server"): True,
        (CONTROL_NAMESPACE, "opensandbox-controller"): True,
        (WORKLOAD_NAMESPACE, "sandbox-workload"): False,
    }
    for account, expected_automount in expected_accounts.items():
        matches = [item for item in account_items if metadata_identity(item) == account]
        if len(matches) != 1 or matches[0].get("automountServiceAccountToken") is not expected_automount:
            failures.append(f"Sandbox ServiceAccount authority drifted: {account[0]}/{account[1]}")

    role_items = list_items(roles, "Role")
    server_roles = [
        item for item in role_items
        if metadata_identity(item) == (WORKLOAD_NAMESPACE, "opensandbox-server")
    ]
    if len(server_roles) != 1 or server_roles[0].get("rules") != EXPECTED_SERVER_RULES:
        failures.append("OpenSandbox Server workload Role drifted")

    leader_roles = [
        item for item in role_items
        if metadata_identity(item) == (CONTROL_NAMESPACE, "opensandbox-controller-leader-election")
    ]
    if len(leader_roles) != 1 or leader_roles[0].get("rules") != EXPECTED_CONTROLLER_LEADER_RULES:
        failures.append("OpenSandbox Controller leader-election Role drifted")

    cluster_role_items = list_items(cluster_roles, "ClusterRole")
    controller_roles = [
        item for item in cluster_role_items
        if metadata_identity(item)[1].endswith("-opensandbox-controller")
    ]
    if len(controller_roles) != 1 or controller_roles[0].get("rules") != EXPECTED_CONTROLLER_RULES:
        failures.append("OpenSandbox Controller ClusterRole drifted")
    server_namespace_roles = [
        item for item in cluster_role_items
        if metadata_identity(item)[1].endswith("-opensandbox-server-namespace")
    ]
    if (
        len(server_namespace_roles) != 1
        or server_namespace_roles[0].get("rules") != EXPECTED_SERVER_NAMESPACE_RULES
    ):
        failures.append("OpenSandbox Server namespace ClusterRole drifted")

    role_binding_items = list_items(role_bindings, "RoleBinding")
    cluster_binding_items = list_items(cluster_role_bindings, "ClusterRoleBinding")
    relevant_bindings = []
    for binding in role_binding_items + cluster_binding_items:
        for subject in binding.get("subjects", []):
            identity = (subject.get("namespace"), subject.get("name"))
            if identity in {
                (CONTROL_NAMESPACE, "opensandbox-server"),
                (CONTROL_NAMESPACE, "opensandbox-controller"),
            }:
                relevant_bindings.append(binding)
    expected_binding_shapes = {
        ("RoleBinding", WORKLOAD_NAMESPACE, "opensandbox-server", "Role", "opensandbox-server", "opensandbox-server"),
        ("RoleBinding", CONTROL_NAMESPACE, "opensandbox-controller-leader-election", "Role", "opensandbox-controller-leader-election", "opensandbox-controller"),
    }
    observed_binding_shapes = set()
    for binding in relevant_bindings:
        namespace, name = metadata_identity(binding)
        role_ref = binding.get("roleRef", {})
        subject = binding.get("subjects", [{}])[0]
        if binding.get("kind") == "RoleBinding":
            if exact_subject(binding, CONTROL_NAMESPACE, subject.get("name")):
                observed_binding_shapes.add((
                    "RoleBinding", namespace, name, role_ref.get("kind"), role_ref.get("name"), subject.get("name")
                ))
            else:
                observed_binding_shapes.add(("RoleBinding", namespace, name, None, None, None))
        elif (
            name.endswith("-opensandbox-controller")
            and role_ref.get("kind") == "ClusterRole"
            and str(role_ref.get("name", "")).endswith("-opensandbox-controller")
            and exact_subject(
            binding, CONTROL_NAMESPACE, "opensandbox-controller"
            )
        ):
            observed_binding_shapes.add((
                "ClusterRoleBinding", None, "opensandbox-controller", role_ref.get("kind"), "opensandbox-controller", "opensandbox-controller"
            ))
        elif (
            name.endswith("-opensandbox-server-namespace")
            and role_ref.get("kind") == "ClusterRole"
            and str(role_ref.get("name", "")).endswith("-opensandbox-server-namespace")
            and exact_subject(
            binding, CONTROL_NAMESPACE, "opensandbox-server"
            )
        ):
            observed_binding_shapes.add((
                "ClusterRoleBinding", None, "opensandbox-server-namespace", role_ref.get("kind"), "opensandbox-server-namespace", "opensandbox-server"
            ))
        else:
            observed_binding_shapes.add((binding.get("kind"), namespace, name, None, None, subject.get("name")))
    expected_binding_shapes.update({
        ("ClusterRoleBinding", None, "opensandbox-controller", "ClusterRole", "opensandbox-controller", "opensandbox-controller"),
        ("ClusterRoleBinding", None, "opensandbox-server-namespace", "ClusterRole", "opensandbox-server-namespace", "opensandbox-server"),
    })
    if len(relevant_bindings) != 4 or observed_binding_shapes != expected_binding_shapes:
        failures.append("OpenSandbox Server/Controller RBAC binding closure drifted")

    policy_items = list_items(admissions, "ValidatingAdmissionPolicy")
    binding_items = list_items(admission_bindings, "ValidatingAdmissionPolicyBinding")
    requirements = {
        "opensandbox-inactive-surfaces": ["false"],
        "opensandbox-batchsandbox": [
            f"system:serviceaccount:{CONTROL_NAMESPACE}:opensandbox-server",
            f"system:serviceaccount:{CONTROL_NAMESPACE}:opensandbox-controller",
            "armed-runner-v2",
            "execd-installer",
        ],
        "opensandbox-pods": [
            f"system:serviceaccount:{CONTROL_NAMESPACE}:opensandbox-controller",
            "sandbox-workload",
            "platform-sandbox-runner",
            "persistentVolumeClaim",
        ],
    }
    for suffix, required_terms in requirements.items():
        matching_policies = [
            item for item in policy_items if metadata_identity(item)[1].endswith(suffix)
        ]
        if len(matching_policies) != 1 or matching_policies[0].get("spec", {}).get("failurePolicy") != "Fail":
            failures.append(f"fail-closed {suffix} admission policy is missing")
            continue
        policy = matching_policies[0]
        policy_name = metadata_identity(policy)[1]
        expressions = "\n".join(
            validation.get("expression", "")
            for validation in policy.get("spec", {}).get("validations", [])
        )
        if any(term not in expressions for term in required_terms):
            failures.append(f"{suffix} admission validation closure drifted")
        matching_bindings = [
            item for item in binding_items
            if item.get("spec", {}).get("policyName") == policy_name
            and item.get("spec", {}).get("validationActions") == ["Deny"]
            and item.get("spec", {}).get("matchResources", {}).get("namespaceSelector", {}).get("matchLabels", {}).get(
                "insight.platform/sandbox-workload-namespace"
            ) == "true"
        ]
        if len(matching_bindings) != 1:
            failures.append(f"{suffix} admission binding is missing or does not deny")


def validate_topology(
    version,
    nodes,
    crd,
    services,
    ingresses,
    service_accounts,
    roles,
    role_bindings,
    cluster_roles,
    cluster_role_bindings,
    admissions,
    admission_bindings,
    expected_crd_digest=CRD_DIGEST,
):
    failures = []
    client_version, server_version = validate_version(version, failures)
    ready_nodes = ready_schedulable_nodes(nodes)
    if len(ready_nodes) < 2:
        failures.append("production qualification requires at least two Ready schedulable nodes")

    architectures = set()
    runtime_versions = set()
    for node in ready_nodes:
        metadata = node.get("metadata", {})
        name = metadata.get("name")
        labels = metadata.get("labels", {})
        architecture = labels.get("kubernetes.io/arch")
        operating_system = labels.get("kubernetes.io/os")
        runtime = node.get("status", {}).get("nodeInfo", {}).get("containerRuntimeVersion")
        if not isinstance(name, str) or not name:
            failures.append("Ready node is missing metadata.name")
            continue
        if operating_system != "linux" or architecture not in EXECUTION_ARCHITECTURES:
            failures.append(f"Ready node {name} is not an approved Linux execution architecture")
        else:
            architectures.add(architecture)
        if not isinstance(runtime, str) or not runtime.startswith("containerd://"):
            failures.append(f"Ready node {name} does not use the required containerd runtime")
        else:
            runtime_versions.add(runtime)

    observed_crd_digest = validate_batchsandbox_crd(crd, failures, expected_crd_digest)
    validate_services(services, failures)
    validate_ingresses(ingresses, failures)
    validate_sandbox_security(
        service_accounts,
        roles,
        role_bindings,
        cluster_roles,
        cluster_role_bindings,
        admissions,
        admission_bindings,
        failures,
    )
    if failures:
        raise ValueError("\n".join(failures))

    summary = {
        "schema_version": 2,
        "kubectl_client_version": client_version,
        "kubernetes_server_version": server_version,
        "ready_schedulable_node_count": len(ready_nodes),
        "execution_architectures": sorted(architectures),
        "container_runtime_versions": sorted(runtime_versions),
        "provider": "opensandbox_kubernetes",
        "physical_store": "batchsandbox_crd",
        "batchsandbox_crd_digest": observed_crd_digest,
        "public_ingress": False,
    }
    canonical = json.dumps(summary, sort_keys=True, separators=(",", ":")).encode()
    return summary, "sha256:" + hashlib.sha256(canonical).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--nodes", required=True)
    parser.add_argument("--batchsandbox-crd", required=True)
    parser.add_argument("--services", required=True)
    parser.add_argument("--ingresses", required=True)
    parser.add_argument("--service-accounts", required=True)
    parser.add_argument("--roles", required=True)
    parser.add_argument("--role-bindings", required=True)
    parser.add_argument("--cluster-roles", required=True)
    parser.add_argument("--cluster-role-bindings", required=True)
    parser.add_argument("--validating-admission-policies", required=True)
    parser.add_argument("--validating-admission-policy-bindings", required=True)
    parser.add_argument("--output")
    arguments = parser.parse_args()
    try:
        summary, digest = validate_topology(
            load(arguments.version),
            load(arguments.nodes),
            load(arguments.batchsandbox_crd),
            load(arguments.services),
            load(arguments.ingresses),
            load(arguments.service_accounts),
            load(arguments.roles),
            load(arguments.role_bindings),
            load(arguments.cluster_roles),
            load(arguments.cluster_role_bindings),
            load(arguments.validating_admission_policies),
            load(arguments.validating_admission_policy_bindings),
        )
    except (OSError, ValueError, json.JSONDecodeError, DuplicateKey) as failure:
        print(f"production topology rejected: {failure}", file=sys.stderr)
        return 1
    payload = {"topology": summary, "topology_digest": digest}
    rendered = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    if arguments.output:
        Path(arguments.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
