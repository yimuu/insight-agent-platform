#!/usr/bin/env python3
"""Fail-closed L4 rollout inventory validation for the Platform v2 release closure."""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path


MAX_INPUT_BYTES = 16 * 1024 * 1024
ROLE_LABEL = "insight.platform/component-role"
DIGEST_IMAGE = re.compile(r"^[^@\s]+@(?P<digest>sha256:[0-9a-f]{64})$")
COMPONENT_ROLES = {
    "management_api",
    "runtime_api",
    "scheduler_recovery",
    "model_worker",
    "capability_native_worker",
    "capability_remote_worker",
    "context_worker",
    "mcp_host",
    "sandbox_controller",
    "sandbox_wasi_executor",
    "sandbox_gvisor_executor",
    "artifact_gateway",
    "artifact_data_worker",
    "artifact_maintenance",
    "egress_secret_broker",
}


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


def items(document, expected_kind):
    if not isinstance(document, dict) or not isinstance(document.get("items"), list):
        raise ValueError(f"{expected_kind} inventory is not a Kubernetes List")
    result = document["items"]
    for item in result:
        if not isinstance(item, dict) or item.get("kind") != expected_kind:
            raise ValueError(f"{expected_kind} inventory contains another resource kind")
    return result


def identity(resource):
    metadata = resource.get("metadata", {})
    namespace = metadata.get("namespace")
    name = metadata.get("name")
    if not isinstance(namespace, str) or not namespace or not isinstance(name, str) or not name:
        raise ValueError("workload resource is missing namespace or name")
    return namespace, name


def role_of(resource):
    labels = resource.get("spec", {}).get("template", {}).get("metadata", {}).get("labels", {})
    return labels.get(ROLE_LABEL)


def selector_matches(selector, labels):
    if not isinstance(selector, dict) or set(selector) - {"matchLabels"}:
        return False
    expected = selector.get("matchLabels", {})
    return isinstance(expected, dict) and expected and all(labels.get(k) == v for k, v in expected.items())


def validate_pod_security(role, pod_spec, failures):
    if pod_spec.get("serviceAccountName") in (None, "", "default"):
        failures.append(f"{role} must use a non-default ServiceAccount")
    if pod_spec.get("automountServiceAccountToken") is not False:
        failures.append(f"{role} must disable ServiceAccount token automount")
    pod_security = pod_spec.get("securityContext", {})
    if pod_security.get("runAsNonRoot") is not True:
        failures.append(f"{role} must run as non-root")
    if pod_security.get("seccompProfile", {}).get("type") != "RuntimeDefault":
        failures.append(f"{role} must use the RuntimeDefault seccomp profile")

    containers = pod_spec.get("containers")
    if not isinstance(containers, list) or not containers:
        failures.append(f"{role} has no containers")
        return []
    images = []
    for container in containers:
        name = container.get("name", "<unnamed>")
        security = container.get("securityContext", {})
        if security.get("allowPrivilegeEscalation") is not False:
            failures.append(f"{role}/{name} permits privilege escalation")
        if security.get("readOnlyRootFilesystem") is not True:
            failures.append(f"{role}/{name} root filesystem is writable")
        if security.get("capabilities", {}).get("drop") != ["ALL"]:
            failures.append(f"{role}/{name} must drop exactly ALL capabilities")
        resources = container.get("resources", {})
        for side in ("requests", "limits"):
            values = resources.get(side, {})
            missing = {"cpu", "memory", "ephemeral-storage"} - set(values)
            if missing:
                failures.append(
                    f"{role}/{name} {side} misses {','.join(sorted(missing))}"
                )
        image = container.get("image")
        match = DIGEST_IMAGE.fullmatch(image or "")
        if not match:
            failures.append(f"{role}/{name} image is not pinned by sha256 digest")
        else:
            images.append(match.group("digest"))
    return images


def validate_workloads(candidate, capacity, deployments, daemonsets, policies, pdbs, hpas):
    failures = []
    candidate_images = candidate.get("component_images")
    replica_targets = capacity.get("replicas")
    hpa_targets = capacity.get("hpa")
    if not isinstance(candidate_images, dict) or set(candidate_images) != COMPONENT_ROLES:
        failures.append("candidate component image closure is incomplete")
        candidate_images = {}
    if not isinstance(replica_targets, dict) or set(replica_targets) != COMPONENT_ROLES:
        failures.append("capacity replica closure is incomplete")
        replica_targets = {}
    if not isinstance(hpa_targets, dict) or set(hpa_targets) != COMPONENT_ROLES:
        failures.append("capacity HPA closure is incomplete")
        hpa_targets = {}
    if capacity.get("deployment_config_digest") != candidate.get("deployment_config_digest"):
        failures.append("capacity and candidate deployment configuration digests differ")

    workloads = items(deployments, "Deployment") + items(daemonsets, "DaemonSet")
    by_role = {}
    for workload in workloads:
        role = role_of(workload)
        if role is None:
            continue
        if role not in COMPONENT_ROLES:
            failures.append(f"workload declares unknown component role {role}")
            continue
        by_role.setdefault(role, []).append(workload)
    for role in sorted(COMPONENT_ROLES):
        matches = by_role.get(role, [])
        if len(matches) != 1:
            failures.append(f"{role} must have exactly one live workload, found {len(matches)}")

    policy_items = items(policies, "NetworkPolicy")
    pdb_items = items(pdbs, "PodDisruptionBudget")
    hpa_items = items(hpas, "HorizontalPodAutoscaler")
    service_accounts = {}
    summary_roles = {}
    namespaces = set()
    for role, matches in sorted(by_role.items()):
        if len(matches) != 1:
            continue
        workload = matches[0]
        namespace, name = identity(workload)
        namespaces.add(namespace)
        spec = workload.get("spec", {})
        template = spec.get("template", {})
        pod_spec = template.get("spec", {})
        pod_labels = template.get("metadata", {}).get("labels", {})
        service_account = pod_spec.get("serviceAccountName")
        if isinstance(service_account, str) and service_account:
            owner = service_accounts.setdefault((namespace, service_account), role)
            if owner != role:
                failures.append(f"{role} shares ServiceAccount {service_account} with {owner}")

        images = validate_pod_security(role, pod_spec, failures)
        expected_image = candidate_images.get(role)
        if not images or any(image != expected_image for image in images):
            failures.append(f"{role} live image digest differs from CandidateManifest")

        target = replica_targets.get(role, {})
        minimum = target.get("min_replicas")
        maximum = target.get("max_replicas")
        status = workload.get("status", {})
        if workload.get("kind") == "Deployment":
            desired = spec.get("replicas")
            ready = status.get("readyReplicas", 0)
            if status.get("observedGeneration") != workload.get("metadata", {}).get("generation"):
                failures.append(f"{role} controller has not observed the current generation")
            if status.get("updatedReplicas", 0) != desired or ready != desired or status.get("unavailableReplicas", 0) != 0:
                failures.append(f"{role} rollout is not fully updated and Ready")
        else:
            desired = status.get("desiredNumberScheduled")
            ready = status.get("numberReady", 0)
            if status.get("observedGeneration") != workload.get("metadata", {}).get("generation"):
                failures.append(f"{role} controller has not observed the current generation")
            if status.get("updatedNumberScheduled", 0) != desired or ready != desired or status.get("numberUnavailable", 0) != 0:
                failures.append(f"{role} rollout is not fully updated and Ready")
        if not isinstance(desired, int) or not isinstance(minimum, int) or not isinstance(maximum, int) or not minimum <= desired <= maximum:
            failures.append(f"{role} live replicas fall outside CapacityProfile")

        matching_pdb = [p for p in pdb_items if identity(p)[0] == namespace and selector_matches(p.get("spec", {}).get("selector"), pod_labels)]
        if len(matching_pdb) != 1:
            failures.append(f"{role} must have exactly one matching PodDisruptionBudget")
        if workload.get("kind") == "Deployment":
            matching_hpa = [
                h for h in hpa_items
                if identity(h)[0] == namespace
                and h.get("spec", {}).get("scaleTargetRef", {}).get("apiVersion") == "apps/v1"
                and h.get("spec", {}).get("scaleTargetRef", {}).get("kind") == "Deployment"
                and h.get("spec", {}).get("scaleTargetRef", {}).get("name") == name
            ]
            if len(matching_hpa) != 1:
                failures.append(f"{role} must have exactly one matching HorizontalPodAutoscaler")
            else:
                hpa = matching_hpa[0].get("spec", {})
                if hpa.get("minReplicas") != minimum or hpa.get("maxReplicas") != maximum:
                    failures.append(f"{role} HPA replica bounds differ from CapacityProfile")

        summary_roles[role] = {
            "kind": workload.get("kind"),
            "namespace": namespace,
            "name": name,
            "service_account": service_account,
            "image_digest": expected_image,
            "desired_replicas": desired,
            "ready_replicas": ready,
        }

    for namespace in sorted(namespaces):
        defaults = []
        for policy in policy_items:
            policy_namespace, policy_name = identity(policy)
            spec = policy.get("spec", {})
            if (
                policy_namespace == namespace
                and policy_name == "default-deny"
                and spec.get("podSelector") == {}
                and set(spec.get("policyTypes", [])) == {"Ingress", "Egress"}
                and not spec.get("ingress")
                and not spec.get("egress")
            ):
                defaults.append(policy)
        if len(defaults) != 1:
            failures.append(f"namespace {namespace} must have one exact default-deny NetworkPolicy")

    if failures:
        raise ValueError("\n".join(failures))
    summary = {"schema_version": 1, "roles": summary_roles}
    canonical = json.dumps(summary, sort_keys=True, separators=(",", ":")).encode()
    return summary, "sha256:" + hashlib.sha256(canonical).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    for argument in ("candidate", "capacity", "deployments", "daemonsets", "networkpolicies", "pdbs", "hpas"):
        parser.add_argument(f"--{argument.replace('_', '-')}", required=True)
    parser.add_argument("--output")
    arguments = parser.parse_args()
    try:
        summary, digest = validate_workloads(
            load(arguments.candidate),
            load(arguments.capacity),
            load(arguments.deployments),
            load(arguments.daemonsets),
            load(arguments.networkpolicies),
            load(arguments.pdbs),
            load(arguments.hpas),
        )
    except (OSError, ValueError, json.JSONDecodeError, DuplicateKey) as failure:
        print(f"production workloads rejected: {failure}", file=sys.stderr)
        return 1
    payload = {"workloads": summary, "workloads_digest": digest}
    rendered = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    if arguments.output:
        Path(arguments.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
