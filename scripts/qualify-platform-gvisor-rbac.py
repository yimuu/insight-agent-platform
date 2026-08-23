#!/usr/bin/env python3
"""Exercise the exact gVisor Launcher Kubernetes RBAC matrix."""

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


MATRIX = [
    ("create", "pods", "", True),
    ("delete", "pods", "", True),
    ("get", "pods", "", True),
    ("patch", "pods", "", True),
    ("watch", "pods", "", True),
    ("get", "pods/status", "", True),
    ("list", "pods", "", False),
    ("update", "pods", "", False),
    ("create", "pods/status", "", False),
    ("get", "pods/log", "", False),
    ("create", "pods/exec", "", False),
    ("create", "pods/attach", "", False),
    ("create", "pods/portforward", "", False),
    ("update", "pods/ephemeralcontainers", "", False),
    ("get", "secrets", "", False),
    ("get", "configmaps", "", False),
    ("get", "serviceaccounts", "", False),
    ("get", "roles", "rbac.authorization.k8s.io", False),
    ("get", "rolebindings", "rbac.authorization.k8s.io", False),
    ("get", "clusterroles", "rbac.authorization.k8s.io", False),
    ("get", "clusterrolebindings", "rbac.authorization.k8s.io", False),
    ("get", "nodes", "", False),
    ("get", "runtimeclasses", "node.k8s.io", False),
]


def invoke(kubectl, subject, namespace, verb, resource, api_group):
    command = [
        kubectl,
        "auth",
        "can-i",
        verb,
        resource,
        f"--as={subject}",
        f"--namespace={namespace}",
    ]
    if api_group:
        command.append(f"--api-group={api_group}")
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        message = completed.stderr.strip() or "kubectl auth can-i failed"
        raise RuntimeError(message)
    observed = completed.stdout.strip()
    if observed not in {"yes", "no"}:
        raise RuntimeError("kubectl auth can-i returned a non-canonical decision")
    return observed == "yes"


def evaluate(check):
    results = []
    for verb, resource, api_group, expected in MATRIX:
        observed = check(verb, resource, api_group)
        results.append(
            {
                "api_group": api_group,
                "expected_allowed": expected,
                "observed_allowed": observed,
                "resource": resource,
                "verb": verb,
            }
        )
    return results


def report(subject, namespace, results):
    passed = all(
        result["expected_allowed"] == result["observed_allowed"] for result in results
    )
    body = {
        "schema_version": 1,
        "subject": subject,
        "namespace": namespace,
        "results": results,
        "passed": passed,
        "failure_code": None,
    }
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    body["evidence_digest"] = "sha256:" + hashlib.sha256(canonical).hexdigest()
    return body


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--subject", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args()
    dns_label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    if not re.fullmatch(
        rf"system:serviceaccount:{dns_label}:{dns_label}", arguments.subject
    ) or not re.fullmatch(dns_label, arguments.namespace):
        print("RBAC subject or namespace is not a canonical ServiceAccount scope", file=sys.stderr)
        return 2
    output = Path(arguments.output)
    if output.exists():
        print(f"RBAC evidence output must be a fresh path: {output}", file=sys.stderr)
        return 2
    try:
        results = evaluate(
            lambda verb, resource, api_group: invoke(
                arguments.kubectl,
                arguments.subject,
                arguments.namespace,
                verb,
                resource,
                api_group,
            )
        )
    except (OSError, RuntimeError) as failure:
        evidence = {
            "schema_version": 1,
            "subject": arguments.subject,
            "namespace": arguments.namespace,
            "results": [],
            "passed": False,
            "failure_code": "kubectl_auth_failed",
        }
        canonical = json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
        evidence["evidence_digest"] = "sha256:" + hashlib.sha256(canonical).hexdigest()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(evidence, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        print(f"gVisor Launcher RBAC qualification failed: {failure}", file=sys.stderr)
        return 1
    evidence = report(arguments.subject, arguments.namespace, results)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    if not evidence["passed"]:
        print("gVisor Launcher RBAC matrix drifted", file=sys.stderr)
        return 1
    print(f"gVisor Launcher RBAC qualification passed ({evidence['evidence_digest']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
