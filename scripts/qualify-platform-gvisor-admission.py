#!/usr/bin/env python3
"""Exercise gVisor guest Pod admission with a positive probe and closed bypass matrix."""

import argparse
import copy
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


START_GATE = {"name": "insight.platform/await-fenced-start"}


def canonical_name(seed):
    return "insight-gv-" + hashlib.sha256(seed.encode()).hexdigest()[:32]


def sanitize_source(source, namespace):
    metadata = source.get("metadata", {})
    spec = copy.deepcopy(source.get("spec", {}))
    labels = copy.deepcopy(metadata.get("labels", {}))
    annotations = copy.deepcopy(metadata.get("annotations", {}))
    if not spec or not labels or not annotations:
        raise ValueError("source guest Pod lacks its admitted contract")
    for field in [
        "nodeName",
        "preemptionPolicy",
        "priority",
        "priorityClassName",
        "readinessGates",
        "resourceClaims",
    ]:
        spec.pop(field, None)
    spec["schedulingGates"] = [START_GATE]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": canonical_name("positive"),
            "namespace": namespace,
            "labels": labels,
            "annotations": annotations,
        },
        "spec": spec,
    }


def mutation_cases(base):
    cases = {}

    def mutate(name, mutation):
        pod = copy.deepcopy(base)
        pod["metadata"]["name"] = canonical_name(name)
        mutation(pod)
        cases[name] = pod

    mutate("missing_start_gate", lambda pod: pod["spec"].update(schedulingGates=[]))
    mutate("runc_runtime", lambda pod: pod["spec"].update(runtimeClassName="runc"))
    mutate("direct_node_binding", lambda pod: pod["spec"].update(nodeName="bypass-node"))
    mutate("host_network", lambda pod: pod["spec"].update(hostNetwork=True))
    mutate("wrong_service_account", lambda pod: pod["spec"].update(serviceAccountName="default"))
    mutate(
        "mutable_image",
        lambda pod: pod["spec"]["containers"][0].update(image="insight-sandbox-guest:latest"),
    )
    mutate(
        "privileged_container",
        lambda pod: pod["spec"]["containers"][0]["securityContext"].update(privileged=True),
    )
    mutate(
        "resource_drift",
        lambda pod: pod["spec"]["containers"][0]["resources"]["limits"].update(cpu="999"),
    )
    mutate(
        "secret_env_from",
        lambda pod: pod["spec"]["containers"][0].update(
            envFrom=[{"secretRef": {"name": "forbidden"}}]
        ),
    )
    mutate(
        "secret_env_value",
        lambda pod: pod["spec"]["containers"][0]["env"].append(
            {
                "name": "FORBIDDEN_SECRET",
                "valueFrom": {"secretKeyRef": {"name": "forbidden", "key": "value"}},
            }
        ),
    )
    mutate(
        "host_path_volume",
        lambda pod: pod["spec"]["volumes"].append(
            {"name": "host", "hostPath": {"path": "/", "type": "Directory"}}
        ),
    )
    mutate(
        "extra_volume_mount",
        lambda pod: pod["spec"]["containers"][0]["volumeMounts"].append(
            {"name": "scratch", "mountPath": "/escape"}
        ),
    )
    mutate(
        "extra_fence_annotation",
        lambda pod: pod["metadata"]["annotations"].update(
            {"insight.platform/unreviewed": "true"}
        ),
    )
    def change_token_audience(pod):
        token_volumes = [
            volume
            for volume in pod["spec"]["volumes"]
            if volume.get("name") == "bootstrap-token" and "projected" in volume
        ]
        if len(token_volumes) != 1:
            raise ValueError("guest Pod lacks one bootstrap token volume")
        sources = token_volumes[0]["projected"].get("sources", [])
        tokens = [source["serviceAccountToken"] for source in sources if "serviceAccountToken" in source]
        if len(tokens) != 1:
            raise ValueError("guest Pod lacks one projected service account token")
        tokens[0]["audience"] = "kubernetes.default.svc"

    mutate("wrong_token_audience", change_token_audience)
    mutate(
        "ephemeral_container",
        lambda pod: pod["spec"].update(
            ephemeralContainers=[
                {
                    "name": "debug",
                    "image": "busybox:latest",
                    "command": ["sh"],
                }
            ]
        ),
    )
    return cases


def server_dry_run(kubectl, subject, pod):
    completed = subprocess.run(
        [
            kubectl,
            "create",
            "--dry-run=server",
            "--validate=true",
            f"--as={subject}",
            "-f",
            "-",
            "-o",
            "name",
        ],
        input=json.dumps(pod, sort_keys=True),
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode == 0


def evidence(subject, namespace, source_pod, positive_accepted, results, failure_code=None):
    body = {
        "schema_version": 1,
        "subject": subject,
        "namespace": namespace,
        "source_pod": source_pod,
        "positive_probe_accepted": positive_accepted,
        "results": results,
        "passed": positive_accepted
        and bool(results)
        and all(not result["observed_accepted"] for result in results),
        "failure_code": failure_code,
    }
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    body["evidence_digest"] = "sha256:" + hashlib.sha256(canonical).hexdigest()
    return body


def write_fresh(path, value):
    target = Path(path)
    if target.exists():
        raise ValueError(f"admission evidence output must be a fresh path: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def canonical_scope(subject, namespace, source_pod):
    dns_label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    return (
        re.fullmatch(rf"system:serviceaccount:{dns_label}:{dns_label}", subject)
        and re.fullmatch(dns_label, namespace)
        and re.fullmatch(r"insight-gv-[0-9a-f]{32}", source_pod)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--subject", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--source-pod", required=True)
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args()
    if not canonical_scope(arguments.subject, arguments.namespace, arguments.source_pod):
        print("admission subject, namespace or source Pod is non-canonical", file=sys.stderr)
        return 2
    try:
        source_result = subprocess.run(
            [
                arguments.kubectl,
                "get",
                "pod",
                arguments.source_pod,
                f"--namespace={arguments.namespace}",
                "-o",
                "json",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if source_result.returncode != 0:
            raise RuntimeError("source_guest_unavailable")
        source = json.loads(source_result.stdout)
        positive = sanitize_source(source, arguments.namespace)
        positive_accepted = server_dry_run(arguments.kubectl, arguments.subject, positive)
        if not positive_accepted:
            report = evidence(
                arguments.subject,
                arguments.namespace,
                arguments.source_pod,
                False,
                [],
                "positive_probe_rejected",
            )
        else:
            results = [
                {
                    "case": name,
                    "expected_accepted": False,
                    "observed_accepted": server_dry_run(
                        arguments.kubectl, arguments.subject, pod
                    ),
                }
                for name, pod in mutation_cases(positive).items()
            ]
            report = evidence(
                arguments.subject,
                arguments.namespace,
                arguments.source_pod,
                True,
                results,
            )
        write_fresh(arguments.output, report)
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as failure:
        report = evidence(
            arguments.subject,
            arguments.namespace,
            arguments.source_pod,
            False,
            [],
            "probe_execution_failed",
        )
        try:
            write_fresh(arguments.output, report)
        except (OSError, ValueError):
            pass
        print(f"gVisor admission qualification failed: {failure}", file=sys.stderr)
        return 1
    if not report["passed"]:
        print("gVisor admission bypass matrix failed", file=sys.stderr)
        return 1
    print(f"gVisor admission qualification passed ({report['evidence_digest']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
