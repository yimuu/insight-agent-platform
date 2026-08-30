#!/usr/bin/env python3
"""Complete the smallest real public deterministic Run with the shipped CLI."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import pathlib
import subprocess
import tempfile


CONTRACT_DIGEST = "sha256:" + "a" * 64


def canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def write_json(directory: pathlib.Path, name: str, value: object) -> pathlib.Path:
    path = directory / name
    path.write_bytes(canonical_bytes(value))
    return path


def run_json(insight: pathlib.Path, arguments: list[str]) -> dict:
    result = subprocess.run(
        [str(insight), *arguments], check=True, capture_output=True, text=True
    )
    return json.loads(result.stdout)


def artifact_ref(report: dict, display_name: str) -> dict:
    return {
        "artifact_id": report["artifact_id"],
        "content_digest": report["content_digest"],
        "byte_length": report["byte_length"],
        "media_type": report["media_type"],
        "classification": "internal",
        "display_name": display_name,
    }


def upload(
    insight: pathlib.Path,
    project: pathlib.Path,
    source: pathlib.Path,
    purpose: str,
    display_name: str,
) -> dict:
    return run_json(
        insight,
        [
            "artifact", "upload", "--file", str(source), "--purpose", purpose,
            "--classification", "internal", "--media-type", "application/json",
            "--display-name", display_name, "--timeout-seconds", "120",
            "--path", str(project),
        ],
    )


def published_version(report: dict, kind: str) -> dict:
    return next(item for item in report["published_versions"] if item["resource_kind"] == kind)


def exact_version(version: dict) -> dict:
    return {
        "revision_id": version["resource_version_id"],
        "resource_kind": version["resource_kind"],
        "semantic_digest": version["content_digest"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--insight-bin", required=True, type=pathlib.Path)
    parser.add_argument("--project", required=True, type=pathlib.Path)
    parser.add_argument("--marker", required=True, type=pathlib.Path)
    args = parser.parse_args()
    if not args.insight_bin.is_file() or not args.project.joinpath(".insight/project.json").is_file():
        raise SystemExit("insight binary and initialized project are required")

    schema_document = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"message": {"type": "string", "minLength": 0, "maxLength": 1024,
                                     "x-platform-max-bytes": 4096}},
        "required": ["message"],
        "additionalProperties": False,
    }
    schema_digest = digest(schema_document)
    closed_schema = {"schema_version": 1, "profile": "insight.closed-json-schema/1",
                     "schema": schema_document, "canonical_digest": schema_digest}
    plan = {
        "plan_version": 5,
        "interface_contract_digest": CONTRACT_DIGEST,
        "entry_node_id": "start",
        "dependency_slots": {},
        "nodes": {
            "start": {"kind": "start", "next": "finish"},
            "finish": {"kind": "return", "value": {"source": "run_input",
                                                        "schema_digest": schema_digest}},
        },
    }
    scheduling = {"version": 1, "weight": 1, "burst": 2, "aging_rounds": 2}
    policy_digest = digest({"kind": "local_execution_profile", "scheduling": scheduling})
    applicability_digest = digest({"environment": "local"})

    with tempfile.TemporaryDirectory(prefix="insight-first-run-") as raw_directory:
        directory = pathlib.Path(raw_directory)
        plan_path = write_json(directory, "typed-plan.json", plan)
        authoring_path = write_json(
            directory, "agent-authoring.json",
            {"schema_version": 1, "kind": "deterministic-echo-agent"},
        )
        qualification_path = write_json(
            directory, "qualification.json",
            {"schema_version": 1, "kind": "local-development-qualification"},
        )
        plan_upload = upload(args.insight_bin, args.project, plan_path, "typed_plan", "typed-plan.json")
        operation_id = plan_upload["operation_id"]
        operation = run_json(args.insight_bin, ["operation", "wait", operation_id,
                                                "--timeout-seconds", "120", "--path", str(args.project)])
        if operation["state"] != "succeeded":
            raise SystemExit("typed plan upload did not become ready")
        authoring_upload = upload(args.insight_bin, args.project, authoring_path,
                                  "authoring_document", "agent-authoring.json")
        qualification_upload = upload(args.insight_bin, args.project, qualification_path,
                                      "diagnostic", "qualification.json")
        authoring = artifact_ref(authoring_upload, "agent-authoring.json")
        qualification = artifact_ref(qualification_upload, "qualification.json")

        policy_manifest = {
            "schema_version": 1, "kind": "insight.platform.apply/v1", "resource_noun": "policies",
            "create": {"display_name": "Deterministic local execution profile", "document": {
                "resource_kind": "policy", "spec": {
                    "authoring_package": {"artifact": authoring,
                                          "manifest_digest": authoring_upload["content_digest"]},
                    "contract_digest": policy_digest, "dependency_versions": [], "policy_versions": [],
                    "policy_kind": "scheduling", "rules_digest": digest(scheduling), "selection": None,
                    "scheduling": scheduling, "retention": None, "model_safety": None,
                    "model_budget": None, "model_public_projection": None, "mcp_protocol": None,
                    "mcp_auth": None, "sandbox_isolation": None, "sandbox_resource": None,
                    "sandbox_network": None, "sandbox_artifact_io": None,
                    "sandbox_secret_resolution": None,
                }}},
            "publish": {"kind": "single", "revision_no": 1,
                        "content_digest": policy_digest, "artifact_id": None},
            "deployment": {"environment": "local", "closure": {"resource_kind": "policy",
                "bindings": {"applicability_digest": applicability_digest,
                             "qualification_evidence": qualification}}},
        }
        policy_path = write_json(directory, "policy.apply.json", policy_manifest)
        policy_report = run_json(args.insight_bin, ["apply", "--file", str(policy_path),
                                                    "--timeout-seconds", "120", "--path", str(args.project)])
        policy_revision = exact_version(published_version(policy_report, "policy_revision"))
        policy_closure = {"policy_revision": policy_revision,
                          "applicability_digest": applicability_digest,
                          "qualification_evidence": qualification}
        policy_binding = {
            "deployment": {"deployment_id": policy_report["deployment_id"],
                           "resource_kind": "policy_deployment",
                           "deployment_digest": digest({"schema_version": 1, "resource_kind": "policy",
                                                        "bindings": policy_closure})},
            "revision": policy_revision,
        }
        agent_manifest = {
            "schema_version": 1, "kind": "insight.platform.apply/v1", "resource_noun": "agents",
            "create": {"display_name": "Deterministic echo agent", "document": {
                "resource_kind": "agent", "spec": {
                    "authoring_package": {"artifact": authoring,
                                          "manifest_digest": authoring_upload["content_digest"]},
                    "contract_digest": CONTRACT_DIGEST, "dependency_versions": [],
                    "policy_versions": [policy_revision], "input_schema": closed_schema,
                    "output_schema": closed_schema, "error_schema": closed_schema,
                    "typed_plan_artifact_id": plan_upload["artifact_id"],
                    "typed_plan_digest": plan_upload["content_digest"],
                }}},
            "publish": {"kind": "agent", "revision_no": 1,
                        "interface_content_digest": CONTRACT_DIGEST,
                        "plan_content_digest": plan_upload["content_digest"],
                        "artifact_id": plan_upload["artifact_id"]},
            "deployment": {"environment": "local", "closure": {"resource_kind": "agent",
                "bindings": {"entry_node_id": "start", "entry_node_kind": "start", "slots": [],
                             "policies": [policy_binding], "execution_profile": policy_binding}}},
        }
        agent_path = write_json(directory, "agent.apply.json", agent_manifest)
        agent_report = run_json(args.insight_bin, ["apply", "--file", str(agent_path),
                                                  "--timeout-seconds", "120", "--path", str(args.project)])
        deadline = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        run_path = write_json(directory, "run.json", {
            "agent_id": agent_report["resource_id"],
            "input": {"classification": "internal", "schema_digest": schema_digest,
                      "value": {"kind": "inline", "value": {"message": "hello"}}},
            "deadline": deadline,
        })
        run = run_json(args.insight_bin, ["run", "create", "--file", str(run_path),
                                         "--path", str(args.project)])
        run_id = run["run_id"]
        watched = subprocess.run(
            [str(args.insight_bin), "run", "watch", run_id, "--timeout-seconds", "120",
             "--path", str(args.project)], check=True, capture_output=True, text=True
        )
        terminal = json.loads(watched.stdout.strip().splitlines()[-1])
        result = run_json(args.insight_bin, ["run", "result", run_id, "--path", str(args.project)])
        expected = {"kind": "inline", "value": {"message": "hello"}}
        if terminal.get("kind") != "terminal" or terminal["run"]["state"] != "succeeded":
            raise SystemExit("first Run did not succeed")
        if result.get("value") != expected or result.get("schema_digest") != schema_digest:
            raise SystemExit("first Run Inline result did not match the closed contract")
        marker = {
            "schema_version": 1,
            "report_kind": "insight.productization.first-run-marker/v1",
            "run_id": run_id,
            "state": "succeeded",
            "result_verified": True,
            "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        }
        args.marker.write_bytes(canonical_bytes(marker))


if __name__ == "__main__":
    main()
