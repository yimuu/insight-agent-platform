#!/usr/bin/env python3
"""Validate closed M4 golden-scenario reports against their checked manifest."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "examples/productization/scenarios.json"
REPORT_SCHEMA_PATH = ROOT / "examples/productization/scenario-report.schema.json"
AGGREGATE_SCHEMA_PATH = ROOT / "examples/productization/scenario-aggregate.schema.json"
REPORT_KIND = "insight.productization.scenario-report/v1"
AGGREGATE_KIND = "insight.productization.scenario-aggregate/v1"
CONTRACT_PROFILE = "insight.platform/v1"
SANDBOX_REPORT_KIND = "insight.productization.opensandbox-qualification/v1"
SANDBOX_SCENARIO_ID = "sandbox-and-remote-framework-capability"
SANDBOX_QUALIFIER = "scripts/qualify-platform-sandbox-l3.sh"
EXPECTED_SCENARIO_IDS = (
    "deterministic-first-run",
    "exact-model-streaming-chat",
    "native-and-remote-capability",
    "remote-mcp-tool-and-resource",
    "context-retrieval-and-citation",
    "approval-task-resume",
    "timer-signal-restart-recovery",
    "subagent-quota-and-cancel",
    "artifact-lifecycle-and-rejection",
    SANDBOX_SCENARIO_ID,
)
FEATURES = {"context", "mcp", "model", "remote-capability", "sandbox"}
ACTUAL_PROFILES = {
    "all",
    "starter",
    "starter+context",
    "starter+context,mcp",
    "starter+context,mcp,model",
    "starter+context,mcp,model,remote-capability",
    "starter+context,mcp,model,remote-capability,sandbox",
    "starter+context,mcp,remote-capability",
    "starter+context,mcp,remote-capability,sandbox",
    "starter+context,model",
    "starter+context,model,remote-capability",
    "starter+context,model,remote-capability,sandbox",
    "starter+context,remote-capability",
    "starter+context,remote-capability,sandbox",
    "starter+mcp",
    "starter+mcp,model",
    "starter+mcp,model,remote-capability",
    "starter+mcp,model,remote-capability,sandbox",
    "starter+mcp,remote-capability",
    "starter+mcp,remote-capability,sandbox",
    "starter+model",
    "starter+model,remote-capability",
    "starter+model,remote-capability,sandbox",
    "starter+remote-capability",
    "starter+remote-capability,sandbox",
}
REPORT_FIELDS = {
    "schema_version",
    "report_kind",
    "scenario_id",
    "contract_profile",
    "profile",
    "qualification_run_id",
    "actual_profile",
    "profile_digest",
    "evidence_inputs",
    "automation_layer",
    "source_revision",
    "environment",
    "started_at",
    "finished_at",
    "status",
    "entrypoints",
    "assertions",
    "failure_probes",
}
ENVIRONMENT_FIELDS = {"os", "architecture", "fresh_profile"}
EVIDENCE_INPUT_FIELDS = {"opensandbox_qualification"}
CHECK_FIELDS = {"id", "status", "evidence"}
AGGREGATE_FIELDS = {
    "schema_version",
    "report_kind",
    "contract_profile",
    "source_revision",
    "qualification_run_id",
    "actual_profile",
    "profile_digest",
    "scenario_manifest_digest",
    "scenario_count",
    "status",
    "reports",
}
AGGREGATE_REPORT_FIELDS = {"scenario_id", "profile", "report_digest"}
SANDBOX_EVIDENCE_FIELDS = {
    "bootstrap_environment_digest",
    "checks",
    "environment",
    "finished_at",
    "package_image",
    "platform_image_digest",
    "qualification_run_id",
    "qualifier",
    "release_candidate",
    "report_kind",
    "runtime_contract_digest",
    "sandbox_chart_digest",
    "schema_version",
    "source_revision",
    "started_at",
    "status",
}
SANDBOX_ENVIRONMENT_FIELDS = {"os", "architecture", "fresh_cluster", "cluster_name"}
SANDBOX_CHECK_FIELDS = {"id", "status"}
SANDBOX_CHECK_IDS = (
    "opensandbox_lifecycle",
    "current_runtime_contract",
    "direct_and_disabled_network",
    "package_process_isolation",
    "deadline_limit_enforced",
    "dispatcher_recovery",
)
BOOTSTRAP_ENVIRONMENT_FIELDS = {
    "schema_version",
    "kind",
    "production",
    "git_commit",
    "platform_image_digest",
    "platform_image_repository",
    "platform_image_identity",
    "sandbox_runner_image_digest",
    "sandbox_runner_image_repository",
    "sandbox_runner_image_identity",
    "deployment_config_digest",
    "generated_at",
    "cluster_name",
    "kubeconfig",
}
PLATFORM_IMAGE_IDENTITY_FIELDS = {
    "kind",
    "repository",
    "reference",
    "config_digest",
    "index_digest",
    "platform",
    "platform_digest",
}
RELEASE_CANDIDATE_FIELDS = {
    "release_bundle_digest",
    "runtime",
    "sandbox_runner",
    "console",
}
RELEASE_COMPONENT_FIELDS = {"subject", "index_digest", "platform", "platform_digest"}
REVISION = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
OCI_SUBJECT = re.compile(r"^[a-z0-9.-]+(?::[0-9]+)?/[a-z0-9._/-]+$")
OCI_REPOSITORY = re.compile(
    r"^(?:[a-z0-9.-]+(?::[0-9]+)?/)?[a-z0-9._/-]+$"
)
PACKAGE_IMAGE = re.compile(
    r"^(?:[a-z0-9.-]+(?::[0-9]+)?/)?[a-z0-9._/-]+@sha256:[0-9a-f]{64}$"
)


class DuplicateKeyError(ValueError):
    pass


def fail(message: str) -> None:
    print(f"productization scenario report invalid: {message}", file=sys.stderr)
    raise SystemExit(1)


def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise DuplicateKeyError(f"duplicate object key {key!r}")
        value[key] = item
    return value


def read_json_document(path: Path) -> tuple[object, bytes]:
    try:
        payload = path.read_bytes()
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=reject_duplicate_keys,
        )
        return value, payload
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, DuplicateKeyError) as error:
        fail(f"{path}: {error}")


def read_json(path: Path) -> object:
    return read_json_document(path)[0]


def timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        fail(f"{field} must be an RFC3339 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        fail(f"{field} must be an RFC3339 timestamp")
    if parsed.tzinfo is None:
        fail(f"{field} must include an RFC3339 timezone")
    return parsed


def bounded_string(value: object, field: str, maximum: int = 128) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        fail(f"{field} is invalid")
    return value


def exact_digest(value: object, field: str) -> str:
    if not isinstance(value, str) or not DIGEST.fullmatch(value):
        fail(f"{field} must be an exact sha256 digest")
    return value


def profile_features(profile: str) -> set[str]:
    if profile == "all":
        return FEATURES
    if profile == "starter":
        return set()
    if not profile.startswith("starter+"):
        fail(f"actual_profile is not a canonical feature closure: {profile}")
    return set(profile.removeprefix("starter+").split(","))


def require_regular_file(path: Path, field: str) -> None:
    if path.is_symlink() or not path.is_file():
        fail(f"{field} must be a regular, non-symlink file: {path}")


def framed_tree_digest(root: Path) -> str:
    if root.is_symlink() or not root.is_dir():
        fail(f"sandbox chart root must be a real directory: {root}")
    digest = hashlib.sha256()
    paths = sorted(root.rglob("*"), key=lambda path: path.relative_to(root).as_posix())
    for path in paths:
        if path.is_symlink():
            fail(f"sandbox chart entries must not be symlinks: {path}")
        if path.is_dir():
            continue
        if not path.is_file():
            fail(f"sandbox chart entries must be regular files: {path}")
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return f"sha256:{digest.hexdigest()}"


def validate_checks(
    report: dict[str, object],
    field: str,
    expected_ids: list[str],
) -> bool:
    checks = report.get(field)
    if not isinstance(checks, list) or not checks:
        fail(f"{field} must be a non-empty array")
    observed: list[str] = []
    all_passed = True
    for check in checks:
        if not isinstance(check, dict) or set(check) != CHECK_FIELDS:
            fail(f"every {field} item must use exactly {sorted(CHECK_FIELDS)}")
        check_id = check.get("id")
        status = check.get("status")
        evidence = check.get("evidence")
        if not isinstance(check_id, str) or not check_id or len(check_id) > 96:
            fail(f"{field}.id is invalid")
        if status not in {"passed", "failed", "not_run"}:
            fail(f"{field}.{check_id}.status is not closed")
        if not isinstance(evidence, str) or not evidence or len(evidence) > 1024:
            fail(f"{field}.{check_id}.evidence is invalid")
        observed.append(check_id)
        all_passed = all_passed and status == "passed"
    if observed != expected_ids:
        fail(f"{field} IDs/order differ: expected {expected_ids}, got {observed}")
    return all_passed


def validate_report(
    path: Path,
    scenario: dict[str, object],
    require_passed: bool,
    expected_revision: str,
    sandbox_evidence_digest: str | None,
    sandbox_environment: dict[str, object] | None,
    sandbox_qualification_run_id: str | None,
    sandbox_started_at: datetime | None,
    sandbox_finished_at: datetime | None,
) -> tuple[dict[str, object], bytes]:
    raw, payload = read_json_document(path)
    if not isinstance(raw, dict) or set(raw) != REPORT_FIELDS:
        fail(f"{path}: report must use exactly {sorted(REPORT_FIELDS)}")
    if require_passed and payload != canonical_bytes(raw):
        fail(f"{path}: strict qualification reports must use canonical JSON bytes")
    if raw.get("schema_version") != 1 or raw.get("report_kind") != REPORT_KIND:
        fail(f"{path}: schema/report kind is not v1")
    if raw.get("scenario_id") != scenario["id"]:
        fail(f"{path}: scenario_id does not match manifest")
    if raw.get("contract_profile") != CONTRACT_PROFILE:
        fail(f"{path}: contract_profile must be {CONTRACT_PROFILE}")
    if raw.get("profile") != scenario["profile"]:
        fail(f"{path}: profile does not match manifest")
    qualification_run_id = raw.get("qualification_run_id")
    exact_digest(qualification_run_id, f"{path}: qualification_run_id")
    actual_profile = raw.get("actual_profile")
    if actual_profile not in ACTUAL_PROFILES:
        fail(f"{path}: actual_profile is not a canonical runner feature closure")
    if not profile_features(str(actual_profile)).issuperset(
        profile_features(str(scenario["profile"]))
    ):
        fail(f"{path}: actual_profile does not include the scenario's minimum profile")
    exact_digest(raw.get("profile_digest"), f"{path}: profile_digest")
    evidence_inputs = raw.get("evidence_inputs")
    if not isinstance(evidence_inputs, dict) or not set(evidence_inputs).issubset(
        EVIDENCE_INPUT_FIELDS
    ):
        fail(f"{path}: evidence_inputs must be a closed object")
    if raw.get("scenario_id") == SANDBOX_SCENARIO_ID:
        if set(evidence_inputs) != EVIDENCE_INPUT_FIELDS:
            fail(f"{path}: sandbox report must bind the OpenSandbox qualification")
        observed_sandbox_digest = exact_digest(
            evidence_inputs.get("opensandbox_qualification"),
            f"{path}: evidence_inputs.opensandbox_qualification",
        )
        if (
            sandbox_evidence_digest is not None
            and observed_sandbox_digest != sandbox_evidence_digest
        ):
            fail(f"{path}: sandbox evidence input digest differs from the supplied raw evidence")
        if (
            sandbox_qualification_run_id is not None
            and qualification_run_id != sandbox_qualification_run_id
        ):
            fail(f"{path}: qualification_run_id differs from the supplied raw evidence")
        if sandbox_environment is not None:
            report_environment = raw.get("environment")
            if isinstance(report_environment, dict):
                for field in ("os", "architecture"):
                    if report_environment.get(field) != sandbox_environment.get(field):
                        fail(f"{path}: environment.{field} differs from OpenSandbox evidence")
    elif evidence_inputs:
        fail(f"{path}: only the sandbox scenario may declare evidence_inputs")
    if raw.get("automation_layer") != scenario["automation_layer"]:
        fail(f"{path}: automation_layer does not match manifest")
    revision = raw.get("source_revision")
    if not isinstance(revision, str) or not REVISION.fullmatch(revision):
        fail(f"{path}: source_revision must be an exact 40-character Git commit")
    if revision != expected_revision:
        fail(f"{path}: source_revision differs from the requested qualification revision")
    environment = raw.get("environment")
    if not isinstance(environment, dict) or set(environment) != ENVIRONMENT_FIELDS:
        fail(f"{path}: environment must be closed")
    if environment.get("fresh_profile") is not True:
        fail(f"{path}: fresh_profile must be true")
    for field in ("os", "architecture"):
        value = environment.get(field)
        if not isinstance(value, str) or not value or len(value) > 64:
            fail(f"{path}: environment.{field} is invalid")
    started = timestamp(raw.get("started_at"), "started_at")
    finished = timestamp(raw.get("finished_at"), "finished_at")
    if finished < started:
        fail(f"{path}: finished_at precedes started_at")
    if raw.get("scenario_id") == SANDBOX_SCENARIO_ID:
        if sandbox_started_at is not None and started > sandbox_started_at:
            fail(f"{path}: report started_at does not cover the raw OpenSandbox qualification")
        if sandbox_finished_at is not None and finished < sandbox_finished_at:
            fail(f"{path}: report finished_at does not cover the raw OpenSandbox qualification")

    entrypoints_passed = validate_checks(raw, "entrypoints", scenario["entrypoints"])
    assertions_passed = validate_checks(raw, "assertions", scenario["assertions"])
    probes_passed = validate_checks(raw, "failure_probes", scenario["failure_probes"])
    status = raw.get("status")
    if status not in {"passed", "incomplete", "failed"}:
        fail(f"{path}: status is not closed")
    all_passed = entrypoints_passed and assertions_passed and probes_passed
    if (status == "passed") != all_passed:
        fail(f"{path}: status=passed must exactly match all required checks passing")
    if require_passed and status != "passed":
        fail(f"{path}: required scenario did not pass (status={status})")
    return raw, payload


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_digest(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def validate_release_candidate(value: object, path: Path) -> None:
    if value is None:
        return
    if not isinstance(value, dict) or set(value) != RELEASE_CANDIDATE_FIELDS:
        fail(f"{path}: release_candidate must be null or the closed candidate binding")
    exact_digest(value.get("release_bundle_digest"), f"{path}: release_candidate.release_bundle_digest")
    subject_prefixes: set[str] = set()
    for name, suffix in (
        ("runtime", "/platform-runtime"),
        ("sandbox_runner", "/platform-sandbox-runner"),
        ("console", "/platform-console"),
    ):
        component = value.get(name)
        if not isinstance(component, dict) or set(component) != RELEASE_COMPONENT_FIELDS:
            fail(f"{path}: release_candidate.{name} is not closed")
        subject = component.get("subject")
        if (
            not isinstance(subject, str)
            or not OCI_SUBJECT.fullmatch(subject)
            or "@" in subject
            or not subject.endswith(suffix)
        ):
            fail(f"{path}: release_candidate.{name}.subject is not an untagged OCI subject")
        subject_prefixes.add(subject[: -len(suffix)])
        exact_digest(
            component.get("index_digest"),
            f"{path}: release_candidate.{name}.index_digest",
        )
        if component.get("platform") != "linux/amd64":
            fail(f"{path}: release_candidate.{name}.platform must be linux/amd64")
        exact_digest(
            component.get("platform_digest"),
            f"{path}: release_candidate.{name}.platform_digest",
        )
    if len(subject_prefixes) != 1:
        fail(f"{path}: release candidate component subjects do not share one repository root")


def validate_sandbox_evidence(
    evidence_path: Path,
    environment_path: Path,
    expected_revision: str,
) -> tuple[str, dict[str, object], str, datetime, datetime]:
    require_regular_file(evidence_path, "--sandbox-evidence")
    require_regular_file(environment_path, "--sandbox-environment")
    evidence, evidence_payload = read_json_document(evidence_path)
    if not isinstance(evidence, dict) or set(evidence) != SANDBOX_EVIDENCE_FIELDS:
        fail(f"{evidence_path}: OpenSandbox evidence is not the closed contract")
    if evidence_payload != canonical_bytes(evidence):
        fail(f"{evidence_path}: OpenSandbox evidence must use canonical JSON bytes")
    if (
        evidence.get("schema_version") != 1
        or evidence.get("report_kind") != SANDBOX_REPORT_KIND
        or evidence.get("qualifier") != SANDBOX_QUALIFIER
        or evidence.get("status") != "passed"
        or evidence.get("source_revision") != expected_revision
    ):
        fail(f"{evidence_path}: OpenSandbox evidence summary is not a passed same-revision qualification")
    qualification_run_id = exact_digest(
        evidence.get("qualification_run_id"),
        f"{evidence_path}: qualification_run_id",
    )
    started_at = timestamp(evidence.get("started_at"), f"{evidence_path}: started_at")
    finished_at = timestamp(evidence.get("finished_at"), f"{evidence_path}: finished_at")
    if finished_at < started_at:
        fail(f"{evidence_path}: finished_at precedes started_at")
    environment = evidence.get("environment")
    if not isinstance(environment, dict) or set(environment) != SANDBOX_ENVIRONMENT_FIELDS:
        fail(f"{evidence_path}: OpenSandbox evidence environment is not closed")
    for field in ("os", "architecture"):
        bounded_string(environment.get(field), f"{evidence_path}: environment.{field}", 64)
    if environment.get("fresh_cluster") is not True:
        fail(f"{evidence_path}: environment.fresh_cluster must be true")
    bounded_string(environment.get("cluster_name"), f"{evidence_path}: environment.cluster_name")
    checks = evidence.get("checks")
    if not isinstance(checks, list) or len(checks) != len(SANDBOX_CHECK_IDS):
        fail(f"{evidence_path}: OpenSandbox checks are not the exact closed set")
    for check, expected_id in zip(checks, SANDBOX_CHECK_IDS):
        if (
            not isinstance(check, dict)
            or set(check) != SANDBOX_CHECK_FIELDS
            or check.get("id") != expected_id
            or check.get("status") != "passed"
        ):
            fail(f"{evidence_path}: OpenSandbox check {expected_id} is not exactly passed")
    for field in (
        "runtime_contract_digest",
        "platform_image_digest",
        "sandbox_chart_digest",
        "bootstrap_environment_digest",
    ):
        exact_digest(evidence.get(field), f"{evidence_path}: {field}")
    checked_chart_digest = framed_tree_digest(
        ROOT / "deploy/helm/insight-platform-sandbox"
    )
    if evidence.get("sandbox_chart_digest") != checked_chart_digest:
        fail(
            f"{evidence_path}: sandbox_chart_digest differs from the current checked chart"
        )
    package_image = evidence.get("package_image")
    if not isinstance(package_image, str) or not PACKAGE_IMAGE.fullmatch(package_image):
        fail(f"{evidence_path}: package_image must be an exact OCI digest reference")
    validate_release_candidate(evidence.get("release_candidate"), evidence_path)

    bootstrap, bootstrap_payload = read_json_document(environment_path)
    if not isinstance(bootstrap, dict) or set(bootstrap) != BOOTSTRAP_ENVIRONMENT_FIELDS:
        fail(f"{environment_path}: bootstrap environment is not the closed contract")
    if (
        bootstrap.get("schema_version") != 2
        or bootstrap.get("kind") != "insight.platform/kind-local-mechanics/v2"
        or bootstrap.get("production") is not False
        or bootstrap.get("git_commit") != expected_revision
    ):
        fail(f"{environment_path}: bootstrap environment summary is invalid")
    for field in (
        "platform_image_digest",
        "sandbox_runner_image_digest",
        "deployment_config_digest",
    ):
        exact_digest(bootstrap.get(field), f"{environment_path}: {field}")
    release_candidate = evidence.get("release_candidate")

    def validate_bootstrap_image_identity(
        label: str,
        repository_field: str,
        digest_field: str,
        identity_field: str,
        release_component: str,
    ) -> dict[str, object]:
        repository = bootstrap.get(repository_field)
        if not isinstance(repository, str) or not OCI_REPOSITORY.fullmatch(repository):
            fail(f"{environment_path}: {repository_field} is invalid")
        identity = bootstrap.get(identity_field)
        if not isinstance(identity, dict) or set(identity) != PLATFORM_IMAGE_IDENTITY_FIELDS:
            fail(f"{environment_path}: {identity_field} is not closed")
        if identity.get("repository") != repository:
            fail(f"{environment_path}: {label} image repositories differ")
        exact_digest(identity.get("config_digest"), f"{environment_path}: {label} config digest")
        if identity.get("platform") not in {"linux/amd64", "linux/arm64"}:
            fail(f"{environment_path}: {label} image architecture is unsupported")
        if identity.get("kind") == "source_oci_manifest":
            if release_candidate is not None:
                fail("release candidate evidence cannot bind a source OCI image identity")
            if (
                identity.get("index_digest") is not None
                or exact_digest(
                    identity.get("platform_digest"),
                    f"{environment_path}: source {label} manifest digest",
                )
                != bootstrap.get(digest_field)
                or identity.get("reference")
                != f"{repository}@{identity.get('platform_digest')}"
            ):
                fail(f"{environment_path}: source OCI {label} image identity is inconsistent")
        elif identity.get("kind") == "signed_release_candidate":
            if not isinstance(release_candidate, dict):
                fail("signed bootstrap image requires a release_candidate evidence binding")
            exact_digest(identity.get("index_digest"), f"{environment_path}: {label} index digest")
            exact_digest(
                identity.get("platform_digest"),
                f"{environment_path}: {label} manifest digest",
            )
            if (
                identity.get("platform_digest") != bootstrap.get(digest_field)
                or identity.get("reference")
                != f"{repository}@{identity['platform_digest']}"
            ):
                fail(f"{environment_path}: signed {label} image identity is inconsistent")
            component = release_candidate[release_component]
            for field, component_identity_field in (
                ("subject", "repository"),
                ("index_digest", "index_digest"),
                ("platform", "platform"),
                ("platform_digest", "platform_digest"),
            ):
                if component.get(field) != identity.get(component_identity_field):
                    fail(f"bootstrap {label} image differs from the signed release candidate")
        else:
            fail(f"{environment_path}: {label} image identity kind is invalid")
        return identity

    platform_identity = validate_bootstrap_image_identity(
        "runtime",
        "platform_image_repository",
        "platform_image_digest",
        "platform_image_identity",
        "runtime",
    )
    runner_identity = validate_bootstrap_image_identity(
        "Sandbox runner",
        "sandbox_runner_image_repository",
        "sandbox_runner_image_digest",
        "sandbox_runner_image_identity",
        "sandbox_runner",
    )
    if (
        runner_identity.get("kind") != platform_identity.get("kind")
        or runner_identity.get("platform") != platform_identity.get("platform")
    ):
        fail("bootstrap runtime and Sandbox runner image identity classes differ")
    bootstrap_generated_at = timestamp(
        bootstrap.get("generated_at"), f"{environment_path}: generated_at"
    )
    if started_at < bootstrap_generated_at:
        fail("raw OpenSandbox qualification starts before its bootstrap environment exists")
    if started_at - bootstrap_generated_at > timedelta(hours=3):
        fail("raw OpenSandbox qualification starts outside the three-hour bootstrap window")
    bounded_string(bootstrap.get("cluster_name"), f"{environment_path}: cluster_name")
    bounded_string(bootstrap.get("kubeconfig"), f"{environment_path}: kubeconfig", 4096)
    if bootstrap.get("cluster_name") != environment.get("cluster_name"):
        fail("OpenSandbox evidence and bootstrap environment identify different clusters")
    if bootstrap.get("platform_image_digest") != evidence.get("platform_image_digest"):
        fail("OpenSandbox evidence and bootstrap environment identify different platform images")
    if evidence.get("bootstrap_environment_digest") != sha256_digest(bootstrap_payload):
        fail("OpenSandbox bootstrap_environment_digest differs from the environment file bytes")
    return (
        sha256_digest(evidence_payload),
        environment,
        qualification_run_id,
        started_at,
        finished_at,
    )


def validate_report_schema(schema: object) -> None:
    if not isinstance(schema, dict) or schema.get("additionalProperties") is not False:
        fail("scenario report schema must be a closed object")
    properties = schema.get("properties")
    required = schema.get("required")
    evidence_inputs = properties.get("evidence_inputs", {}) if isinstance(properties, dict) else {}
    expected_sandbox_condition = [
        {
            "if": {
                "properties": {"scenario_id": {"const": SANDBOX_SCENARIO_ID}},
                "required": ["scenario_id"],
            },
            "then": {
                "properties": {
                    "evidence_inputs": {"required": ["opensandbox_qualification"]}
                }
            },
            "else": {"properties": {"evidence_inputs": {"maxProperties": 0}}},
        }
    ]
    environment_schema = properties.get("environment", {}) if isinstance(properties, dict) else {}
    definitions = schema.get("$defs")
    checks_schema = definitions.get("checks") if isinstance(definitions, dict) else None
    if (
        not isinstance(properties, dict)
        or set(properties) != REPORT_FIELDS
        or not isinstance(required, list)
        or set(required) != REPORT_FIELDS
        or properties.get("schema_version", {}).get("const") != 1
        or properties.get("report_kind", {}).get("const") != REPORT_KIND
        or properties.get("contract_profile", {}).get("const") != CONTRACT_PROFILE
        or set(properties.get("actual_profile", {}).get("enum", [])) != ACTUAL_PROFILES
        or properties.get("qualification_run_id", {}).get("pattern") != DIGEST.pattern
        or properties.get("profile_digest", {}).get("pattern") != DIGEST.pattern
        or properties.get("source_revision", {}).get("pattern") != REVISION.pattern
        or set(properties.get("automation_layer", {}).get("enum", [])) != {"P2", "P3"}
        or set(properties.get("status", {}).get("enum", []))
        != {"passed", "incomplete", "failed"}
        or not isinstance(environment_schema, dict)
        or environment_schema.get("additionalProperties") is not False
        or set(environment_schema.get("required", [])) != ENVIRONMENT_FIELDS
        or set(environment_schema.get("properties", {})) != ENVIRONMENT_FIELDS
        or any(
            properties.get(field, {}).get("$ref") != "#/$defs/checks"
            for field in ("entrypoints", "assertions", "failure_probes")
        )
        or not isinstance(evidence_inputs, dict)
        or evidence_inputs.get("additionalProperties") is not False
        or set(evidence_inputs.get("properties", {})) != EVIDENCE_INPUT_FIELDS
        or evidence_inputs.get("properties", {})
        .get("opensandbox_qualification", {})
        .get("pattern")
        != DIGEST.pattern
        or schema.get("allOf") != expected_sandbox_condition
    ):
        fail("scenario report schema differs from the closed evidence authority")
    if (
        not isinstance(checks_schema, dict)
        or checks_schema.get("type") != "array"
        or checks_schema.get("minItems") != 1
        or checks_schema.get("maxItems") != 3
    ):
        fail("scenario report check list schema is not bounded")
    check_item = checks_schema.get("items")
    if (
        not isinstance(check_item, dict)
        or check_item.get("additionalProperties") is not False
        or set(check_item.get("required", [])) != CHECK_FIELDS
        or set(check_item.get("properties", {})) != CHECK_FIELDS
        or set(check_item.get("properties", {}).get("status", {}).get("enum", []))
        != {"passed", "failed", "not_run"}
    ):
        fail("scenario report check item schema is not closed")


def validate_aggregate_schema(schema: object, scenarios: list[dict[str, object]]) -> None:
    if not isinstance(schema, dict) or schema.get("additionalProperties") is not False:
        fail("aggregate schema must be a closed object")
    properties = schema.get("properties")
    required = schema.get("required")
    if (
        not isinstance(properties, dict)
        or set(properties) != AGGREGATE_FIELDS
        or not isinstance(required, list)
        or set(required) != AGGREGATE_FIELDS
        or properties.get("report_kind", {}).get("const") != AGGREGATE_KIND
        or properties.get("contract_profile", {}).get("const") != CONTRACT_PROFILE
        or properties.get("actual_profile", {}).get("const") != "all"
        or properties.get("schema_version", {}).get("const") != 1
        or properties.get("source_revision", {}).get("pattern") != REVISION.pattern
        or properties.get("scenario_manifest_digest", {}).get("pattern") != DIGEST.pattern
        or properties.get("qualification_run_id", {}).get("pattern") != DIGEST.pattern
        or properties.get("profile_digest", {}).get("pattern") != DIGEST.pattern
        or properties.get("scenario_count", {}).get("const") != 10
        or properties.get("status", {}).get("const") != "passed"
        or properties.get("reports", {}).get("minItems") != 10
        or properties.get("reports", {}).get("maxItems") != 10
        or properties.get("reports", {}).get("uniqueItems") is not True
        or properties.get("reports", {}).get("items") is not False
    ):
        fail("aggregate schema differs from the strict 10/10 authority")
    prefix_items = properties["reports"].get("prefixItems")
    if not isinstance(prefix_items, list) or len(prefix_items) != 10:
        fail("aggregate schema must close the ten canonical report positions")
    for item, scenario in zip(prefix_items, scenarios):
        if (
            not isinstance(item, dict)
            or set(item) != {"$ref", "properties"}
            or item.get("$ref") != "#/$defs/report"
            or item.get("properties")
            != {
                "scenario_id": {"const": scenario["id"]},
                "profile": {"const": scenario["profile"]},
            }
        ):
            fail("aggregate schema report positions differ from the fixed manifest")
    definitions = schema.get("$defs")
    report_items = definitions.get("report") if isinstance(definitions, dict) else None
    if (
        not isinstance(report_items, dict)
        or report_items.get("additionalProperties") is not False
        or set(report_items.get("required", [])) != AGGREGATE_REPORT_FIELDS
        or set(report_items.get("properties", {})) != AGGREGATE_REPORT_FIELDS
        or report_items.get("properties", {}).get("report_digest", {}).get("pattern")
        != DIGEST.pattern
    ):
        fail("aggregate report entries differ from the closed evidence authority")


def validate_aggregate(aggregate: object, scenarios: list[dict[str, object]]) -> None:
    if not isinstance(aggregate, dict) or set(aggregate) != AGGREGATE_FIELDS:
        fail("aggregate must use the closed v1 fields")
    if (
        aggregate.get("schema_version") != 1
        or aggregate.get("report_kind") != AGGREGATE_KIND
        or aggregate.get("contract_profile") != CONTRACT_PROFILE
        or aggregate.get("actual_profile") != "all"
        or aggregate.get("scenario_count") != 10
        or aggregate.get("status") != "passed"
    ):
        fail("aggregate constants do not identify a passed 10/10 gate")
    if not isinstance(aggregate.get("source_revision"), str) or not REVISION.fullmatch(
        aggregate["source_revision"]
    ):
        fail("aggregate source_revision is not exact")
    if not isinstance(aggregate.get("scenario_manifest_digest"), str) or not DIGEST.fullmatch(
        aggregate["scenario_manifest_digest"]
    ):
        fail("aggregate manifest digest is not exact")
    exact_digest(aggregate.get("qualification_run_id"), "aggregate qualification_run_id")
    exact_digest(aggregate.get("profile_digest"), "aggregate profile_digest")
    reports = aggregate.get("reports")
    if not isinstance(reports, list) or len(reports) != 10:
        fail("aggregate must contain exactly ten reports")
    observed_ids: set[str] = set()
    for report, scenario in zip(reports, scenarios):
        if not isinstance(report, dict) or set(report) != AGGREGATE_REPORT_FIELDS:
            fail("aggregate report entry is not closed")
        if report.get("scenario_id") != scenario["id"] or report.get("profile") != scenario["profile"]:
            fail("aggregate report order/profile differs from the manifest")
        if report["scenario_id"] in observed_ids:
            fail("aggregate report scenario IDs must be unique")
        observed_ids.add(report["scenario_id"])
        if not isinstance(report.get("report_digest"), str) or not DIGEST.fullmatch(
            report["report_digest"]
        ):
            fail("aggregate report digest is not exact")
    if observed_ids != set(EXPECTED_SCENARIO_IDS):
        fail("aggregate report scenario IDs differ from the fixed 10/10 authority")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("report_directory", type=Path)
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="validate present reports without requiring all ten or passed status",
    )
    parser.add_argument(
        "--source-revision",
        help="exact revision under qualification (defaults to the current Git HEAD)",
    )
    parser.add_argument(
        "--aggregate-output",
        type=Path,
        help="write deterministic 10/10 evidence after, and only after, the strict gate passes",
    )
    parser.add_argument(
        "--sandbox-evidence",
        type=Path,
        help="raw canonical OpenSandbox qualification evidence consumed by the sandbox scenario",
    )
    parser.add_argument(
        "--sandbox-environment",
        type=Path,
        help="exact bootstrap environment whose bytes are bound by the OpenSandbox evidence",
    )
    arguments = parser.parse_args()
    if arguments.aggregate_output is not None and arguments.allow_incomplete:
        fail("--aggregate-output is only valid for the strict complete gate")
    if (arguments.sandbox_evidence is None) != (arguments.sandbox_environment is None):
        fail("--sandbox-evidence and --sandbox-environment must be supplied together")
    if not arguments.allow_incomplete and arguments.sandbox_evidence is None:
        fail("the strict 10/10 gate requires --sandbox-evidence and --sandbox-environment")
    expected_revision = arguments.source_revision
    if expected_revision is None:
        try:
            expected_revision = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError) as error:
            fail(f"cannot resolve current Git revision: {error}")
    if not REVISION.fullmatch(expected_revision):
        fail("--source-revision must be an exact 40-character lowercase Git commit")
    manifest = read_json(MANIFEST_PATH)
    if not isinstance(manifest, dict) or not isinstance(manifest.get("scenarios"), list):
        fail("checked scenario manifest is invalid")
    scenarios = manifest["scenarios"]
    if manifest.get("profile_semantics") != "minimum_required_feature_closure":
        fail("manifest profile semantics are not closed")
    scenario_ids = tuple(
        scenario.get("id") if isinstance(scenario, dict) else None for scenario in scenarios
    )
    if scenario_ids != EXPECTED_SCENARIO_IDS or len(set(scenario_ids)) != 10:
        fail("manifest must contain the unique fixed 10/10 scenario IDs in canonical order")
    validate_report_schema(read_json(REPORT_SCHEMA_PATH))
    validate_aggregate_schema(read_json(AGGREGATE_SCHEMA_PATH), scenarios)
    sandbox_evidence_digest: str | None = None
    sandbox_environment: dict[str, object] | None = None
    sandbox_qualification_run_id: str | None = None
    sandbox_started_at: datetime | None = None
    sandbox_finished_at: datetime | None = None
    if arguments.sandbox_evidence is not None and arguments.sandbox_environment is not None:
        (
            sandbox_evidence_digest,
            sandbox_environment,
            sandbox_qualification_run_id,
            sandbox_started_at,
            sandbox_finished_at,
        ) = validate_sandbox_evidence(
            arguments.sandbox_evidence,
            arguments.sandbox_environment,
            expected_revision,
        )
    expected_names = {f"{scenario['id']}.json" for scenario in scenarios}
    if arguments.report_directory.is_symlink() or not arguments.report_directory.is_dir():
        fail(f"report directory must be a real directory: {arguments.report_directory}")
    entries = sorted(arguments.report_directory.iterdir(), key=lambda path: path.name)
    if arguments.allow_incomplete:
        observed_paths = [path for path in entries if path.suffix == ".json"]
        for path in observed_paths:
            require_regular_file(path, "scenario report")
    else:
        if len(entries) != len(expected_names):
            fail(
                "strict report directory must contain exactly the ten canonical "
                "top-level report files"
            )
        for path in entries:
            require_regular_file(path, "strict scenario report")
        observed_paths = entries
    if not observed_paths:
        fail("report directory contains no scenario reports")
    observed_names = {path.name for path in observed_paths}
    unknown = observed_names - expected_names
    if unknown:
        fail(f"unknown scenario reports: {sorted(unknown)}")
    if not arguments.allow_incomplete and observed_names != expected_names:
        fail(f"report set differs: missing={sorted(expected_names - observed_names)}")
    by_id = {scenario["id"]: scenario for scenario in scenarios}
    validated: dict[str, tuple[dict[str, object], bytes]] = {}
    for path in observed_paths:
        report, payload = validate_report(
            path,
            by_id[path.stem],
            require_passed=not arguments.allow_incomplete,
            expected_revision=expected_revision,
            sandbox_evidence_digest=sandbox_evidence_digest,
            sandbox_environment=sandbox_environment,
            sandbox_qualification_run_id=sandbox_qualification_run_id,
            sandbox_started_at=sandbox_started_at,
            sandbox_finished_at=sandbox_finished_at,
        )
        validated[path.stem] = (report, payload)
    qualification_run_id: str | None = None
    profile_digest: str | None = None
    if not arguments.allow_incomplete:
        qualification_run_ids = {
            str(report[0]["qualification_run_id"]) for report in validated.values()
        }
        actual_profiles = {str(report[0]["actual_profile"]) for report in validated.values()}
        profile_digests = {str(report[0]["profile_digest"]) for report in validated.values()}
        if len(qualification_run_ids) != 1:
            fail("strict 10/10 reports must identify one qualification_run_id")
        if actual_profiles != {"all"}:
            fail("strict 10/10 reports must all identify actual_profile=all")
        if len(profile_digests) != 1:
            fail("strict 10/10 reports must identify one exact runtime profile digest")
        qualification_run_id = qualification_run_ids.pop()
        profile_digest = profile_digests.pop()
        if (
            sandbox_qualification_run_id is not None
            and qualification_run_id != sandbox_qualification_run_id
        ):
            fail("strict 10/10 qualification_run_id differs from the raw OpenSandbox run")
    if arguments.aggregate_output is not None:
        output = arguments.aggregate_output.resolve()
        report_directory = arguments.report_directory.resolve()
        if output.parent == report_directory:
            fail("aggregate output must be outside the scenario report directory")
        aggregate = {
            "schema_version": 1,
            "report_kind": AGGREGATE_KIND,
            "contract_profile": CONTRACT_PROFILE,
            "source_revision": expected_revision,
            "qualification_run_id": qualification_run_id,
            "actual_profile": "all",
            "profile_digest": profile_digest,
            "scenario_manifest_digest": sha256_digest(canonical_bytes(manifest)),
            "scenario_count": len(scenarios),
            "status": "passed",
            "reports": [
                {
                    "scenario_id": scenario["id"],
                    "profile": scenario["profile"],
                    "report_digest": sha256_digest(validated[scenario["id"]][1]),
                }
                for scenario in scenarios
            ],
        }
        validate_aggregate(aggregate, scenarios)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(canonical_bytes(aggregate))
    print(
        f"validated {len(observed_paths)} productization scenario report(s); "
        f"complete_gate={str(not arguments.allow_incomplete).lower()}"
    )


if __name__ == "__main__":
    main()
