#!/usr/bin/env python3
"""Fail-closed production topology preflight for Platform v2 L4 qualification."""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path


MAX_INPUT_BYTES = 8 * 1024 * 1024
EXECUTION_ARCHITECTURES = {"amd64", "arm64"}
GVISOR_LABEL = "insight.platform.node-restriction.kubernetes.io/sandbox-gvisor"
WASI_LABEL = "insight.platform.node-restriction.kubernetes.io/sandbox-wasi"
ATTESTOR_LABEL = "insight.platform.node-restriction.kubernetes.io/sandbox-attestor"


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


def ready_schedulable_nodes(nodes):
    result = []
    for node in nodes.get("items", []):
        conditions = node.get("status", {}).get("conditions", [])
        ready = any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in conditions
        )
        if ready and node.get("spec", {}).get("unschedulable") is not True:
            result.append(node)
    return result


def validate_topology(version, nodes, runtime_class):
    failures = []
    git_version = version.get("serverVersion", {}).get("gitVersion")
    client_version = version.get("clientVersion", {}).get("gitVersion")
    version_pattern = re.compile(
        r"v(?P<major>[0-9]+)\.(?P<minor>[0-9]+)(?:\.[0-9]+)?(?:[-+][0-9A-Za-z.-]+)?"
    )
    server_match = version_pattern.fullmatch(git_version or "")
    client_match = version_pattern.fullmatch(client_version or "")
    if not server_match:
        failures.append("Kubernetes server version is missing or non-canonical")
    if not client_match:
        failures.append("kubectl client version is missing or non-canonical")
    if server_match and client_match and (
        server_match.group("major") != client_match.group("major")
        or abs(int(server_match.group("minor")) - int(client_match.group("minor"))) > 1
    ):
        failures.append("kubectl client/server version skew exceeds one minor release")

    ready_nodes = ready_schedulable_nodes(nodes)
    if len(ready_nodes) < 2:
        failures.append("production qualification requires at least two Ready schedulable nodes")

    pools = {"gvisor": set(), "wasi": set(), "attestor": set()}
    architectures = set()
    for node in ready_nodes:
        metadata = node.get("metadata", {})
        name = metadata.get("name")
        labels = metadata.get("labels", {})
        architecture = labels.get("kubernetes.io/arch")
        operating_system = labels.get("kubernetes.io/os")
        if not isinstance(name, str) or not name:
            failures.append("Ready node is missing metadata.name")
            continue
        if operating_system != "linux" or architecture not in EXECUTION_ARCHITECTURES:
            failures.append(f"Ready node {name} is not an approved Linux execution architecture")
            continue
        architectures.add(architecture)
        if labels.get(GVISOR_LABEL) == "true":
            pools["gvisor"].add(name)
        if labels.get(WASI_LABEL) == "true":
            pools["wasi"].add(name)
        if labels.get(ATTESTOR_LABEL) == "true":
            pools["attestor"].add(name)

    if not pools["gvisor"]:
        failures.append("no Ready NodeRestriction-labeled gVisor node exists")
    if not pools["wasi"]:
        failures.append("no Ready NodeRestriction-labeled WASI node exists")
    overlap = pools["gvisor"] & pools["wasi"]
    if overlap:
        failures.append("gVisor and WASI node pools overlap")
    missing_attestors = (pools["gvisor"] | pools["wasi"]) - pools["attestor"]
    if missing_attestors:
        failures.append("every execution node must carry the attestor NodeRestriction label")

    gvisor_architectures = {
        node.get("metadata", {}).get("labels", {}).get("kubernetes.io/arch")
        for node in ready_nodes
        if node.get("metadata", {}).get("name") in pools["gvisor"]
    }
    if len(gvisor_architectures) != 1:
        failures.append("the first-release gVisor pool must use one exact architecture")

    expected_selector = {
        "kubernetes.io/arch": next(iter(gvisor_architectures), None),
        "kubernetes.io/os": "linux",
        GVISOR_LABEL: "true",
    }
    if runtime_class.get("apiVersion") != "node.k8s.io/v1":
        failures.append("runsc RuntimeClass must use node.k8s.io/v1")
    if runtime_class.get("kind") != "RuntimeClass":
        failures.append("runsc object is not a RuntimeClass")
    if runtime_class.get("metadata", {}).get("name") != "runsc":
        failures.append("RuntimeClass name must be runsc")
    if runtime_class.get("handler") != "runsc":
        failures.append("RuntimeClass handler must be runsc")
    selector = runtime_class.get("scheduling", {}).get("nodeSelector")
    if selector != expected_selector:
        failures.append("runsc RuntimeClass scheduling selector differs from the exact gVisor pool")

    if failures:
        raise ValueError("\n".join(failures))

    summary = {
        "schema_version": 1,
        "kubectl_client_version": client_version,
        "kubernetes_server_version": git_version,
        "ready_schedulable_node_count": len(ready_nodes),
        "execution_architectures": sorted(architectures),
        "gvisor_node_count": len(pools["gvisor"]),
        "wasi_node_count": len(pools["wasi"]),
        "attestor_node_count": len(pools["attestor"]),
        "node_pools_disjoint": True,
        "runtime_class": {
            "name": "runsc",
            "handler": "runsc",
            "scheduling_node_selector": selector,
        },
    }
    canonical = json.dumps(summary, sort_keys=True, separators=(",", ":")).encode()
    return summary, "sha256:" + hashlib.sha256(canonical).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--nodes", required=True)
    parser.add_argument("--runtime-class", required=True)
    parser.add_argument("--output")
    arguments = parser.parse_args()
    try:
        summary, digest = validate_topology(
            load(arguments.version),
            load(arguments.nodes),
            load(arguments.runtime_class),
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
