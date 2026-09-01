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
CRD_DIGEST = "sha256:6a56fbec00a33acf30a4a9c3418172ad6ac1eba34d081881e6b5dd941cfa59d4"


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


def validate_batchsandbox_crd(crd, failures):
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
    established = any(
        condition.get("type") == "Established" and condition.get("status") == "True"
        for condition in crd.get("status", {}).get("conditions", [])
    )
    if not established:
        failures.append("BatchSandbox CRD is not Established")


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


def validate_topology(version, nodes, crd, services, ingresses):
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

    validate_batchsandbox_crd(crd, failures)
    validate_services(services, failures)
    validate_ingresses(ingresses, failures)
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
        "batchsandbox_crd_digest": CRD_DIGEST,
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
    parser.add_argument("--output")
    arguments = parser.parse_args()
    try:
        summary, digest = validate_topology(
            load(arguments.version),
            load(arguments.nodes),
            load(arguments.batchsandbox_crd),
            load(arguments.services),
            load(arguments.ingresses),
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
