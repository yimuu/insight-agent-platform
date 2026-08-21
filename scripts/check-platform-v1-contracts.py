#!/usr/bin/env python3
"""Independent, read-only validator for the Platform v1 contract tree."""

import hashlib
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CONTRACT_ROOT = ROOT / "contracts" / "platform-v1"
FIXTURE_ROOT = CONTRACT_ROOT / "fixtures"
SUITES = [
    "F-ID",
    "F-CANON",
    "F-SCHEMA",
    "F-STATE",
    "F-FENCE",
    "F-EVENT",
    "F-POLICY",
    "F-BACKEND",
    "F-E2E",
    "F-Q1",
]
CONTRACT_FILES = [
    "contracts/platform-v1/openapi.yaml",
    "contracts/platform-v1/registries.json",
    "contracts/platform-v1/errors.json",
    "contracts/platform-v1/events/public-run-events.json",
    "contracts/platform-v1/events/public-run-payloads.schema.json",
    "contracts/platform-v1/schemas/closed-schema-profile.json",
    "contracts/platform-v1/schemas/resource-id.schema.json",
    "contracts/platform-v1/schemas/states.json",
    "contracts/platform-v1/schemas/nominal-types.json",
    "contracts/platform-v1/schemas/frozen-slot-binding.schema.json",
    "contracts/platform-v1/schemas/worker-manifest.schema.json",
    "contracts/platform-v1/schemas/candidate-manifest.schema.json",
    "contracts/platform-v1/schemas/policies/artifact-retention-policy.schema.json",
    "contracts/platform-v1/schemas/policies/scheduling-policy.schema.json",
    "contracts/platform-v1/schemas/nominal/api-problem.schema.json",
    "contracts/platform-v1/schemas/nominal/artifact-ref.schema.json",
    "contracts/platform-v1/schemas/nominal/decimal-money.schema.json",
    "contracts/platform-v1/schemas/nominal/digest.schema.json",
    "contracts/platform-v1/schemas/nominal/failure.schema.json",
    "contracts/platform-v1/schemas/nominal/opaque-list-cursor.schema.json",
    "contracts/platform-v1/schemas/nominal/opaque-run-event-cursor.schema.json",
    "contracts/platform-v1/schemas/nominal/utc-timestamp.schema.json",
    "contracts/platform-v1/schemas/nominal/uuid-v7-id.schema.json",
    "contracts/platform-v1/limits/hard-limit-profile.schema.json",
    "contracts/platform-v1/limits/q1-50.json",
    "contracts/platform-v1/fixtures/manifest.json",
    "contracts/platform-v1/examples/foundation-scalars.json",
    "contracts/platform-v1/examples/q1-orchestration-worker-manifest.json",
    "proto/insight/platform/v1/foundation.proto",
]
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
RESOURCE_ID = re.compile(
    r"^[a-z]+_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
UTC_MICROSECOND = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$"
)


class DuplicateKey(ValueError):
    pass


def strict_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKey(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def load(path):
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=strict_pairs)


def canonical(value):
    # Fixture inputs intentionally use the I-JSON subset where this encoding agrees with JCS.
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def digest(value):
    return "sha256:" + hashlib.sha256(canonical(value)).hexdigest()


def check_fixtures(errors):
    manifest = load(FIXTURE_ROOT / "manifest.json")
    suites = manifest.get("suites")
    if not isinstance(suites, list) or [item.get("suite_id") for item in suites] != SUITES:
        errors.append("fixture manifest suite registry differs from the closed F-ID..F-Q1 order")
        return
    seen_fixture_ids = set()
    for item in suites:
        path = FIXTURE_ROOT / item["file"]
        document = load(path)
        if document.get("suite_id") != item["suite_id"]:
            errors.append(f"{path}: suite_id mismatch")
        actual_digest = digest(document)
        if item.get("digest") != actual_digest:
            errors.append(f"{path}: manifest digest {item.get('digest')} != {actual_digest}")
        cases = document.get("cases")
        if not isinstance(cases, list):
            errors.append(f"{path}: cases must be an array")
            continue
        for case in cases:
            fixture_id = case.get("fixture_id")
            if not fixture_id or fixture_id in seen_fixture_ids:
                errors.append(f"{path}: missing or duplicate fixture_id {fixture_id!r}")
                continue
            seen_fixture_ids.add(fixture_id)
            required = {
                "owner_spec",
                "related_specs",
                "profile",
                "seed",
                "input_digest",
                "privacy_classification",
                "polarity",
            }
            missing = sorted(required - case.keys())
            if missing:
                errors.append(f"{fixture_id}: missing fields {missing}")
            source = case.get("input", case.get("input_artifact"))
            if source is None:
                errors.append(f"{fixture_id}: missing input or input_artifact")
            elif case.get("input_digest") != digest(source):
                errors.append(f"{fixture_id}: input_digest does not match canonical input")
            if ("expected" in case) == ("stable_rejection" in case):
                errors.append(f"{fixture_id}: exactly one of expected/stable_rejection is required")
            if case.get("polarity") not in {"positive", "negative"}:
                errors.append(f"{fixture_id}: invalid polarity")
            if not DIGEST.fullmatch(case.get("input_digest", "")):
                errors.append(f"{fixture_id}: input_digest is not canonical sha256")


def check_limits(errors):
    profile = load(CONTRACT_ROOT / "limits" / "q1-50.json")
    if profile.get("profile_id") != "q1-50" or profile.get("profile_version") != 4:
        errors.append("Q1 limit profile identity/version is invalid")
    expected_families = {
        "api",
        "registry_plan",
        "run_scheduler",
        "model_context_mcp",
        "capability_sandbox",
        "artifact",
        "durable_quota",
        "control_data",
    }
    actual_families = set(profile) - {"profile_id", "profile_version"}
    if actual_families != expected_families:
        errors.append(f"Q1 limit families mismatch: {sorted(actual_families)}")
    for family in expected_families & actual_families:
        if not isinstance(profile[family], dict) or not profile[family]:
            errors.append(f"Q1 family {family} must be a non-empty object")
            continue
        for name, limit in profile[family].items():
            if set(limit) != {"unit", "hard_max", "q1_default", "overflow_outcome"}:
                errors.append(f"Q1 limit {family}.{name} has an open/incomplete shape")
                continue
            hard_max = limit.get("hard_max")
            default = limit.get("q1_default")
            if not isinstance(hard_max, int) or not isinstance(default, int) or not (0 < default <= hard_max):
                errors.append(f"Q1 limit {family}.{name} violates 0 < default <= hard_max")
    scheduler = profile.get("run_scheduler", {})
    lease = scheduler.get("lease_milliseconds", {})
    heartbeat = scheduler.get("heartbeat_milliseconds", {})
    if (
        lease.get("unit") != "milliseconds"
        or heartbeat.get("unit") != "milliseconds"
        or heartbeat.get("hard_max", 0) * 3 >= lease.get("hard_max", 0)
        or heartbeat.get("q1_default", 0) * 3 >= lease.get("q1_default", 0)
    ):
        errors.append("Q1 heartbeat must be strictly below one third of its lease")
    runtime_bundle = profile.get("capability_sandbox", {}).get("runtime_bundle_bytes", {})
    if runtime_bundle != {
        "unit": "bytes",
        "hard_max": 67_108_864,
        "q1_default": 33_554_432,
        "overflow_outcome": "content_rejected",
    }:
        errors.append("Q1 Sandbox runtime bundle limit differs from the closed v4 contract")
    schema = load(CONTRACT_ROOT / "limits" / "hard-limit-profile.schema.json")
    if schema.get("properties", {}).get("profile_version") != {"const": 4}:
        errors.append("HardLimitProfile schema must accept only profile version 4")
    runtime_bundle_schema = (
        schema.get("$defs", {})
        .get("capability_sandbox", {})
        .get("properties", {})
        .get("runtime_bundle_bytes", {})
    )
    if runtime_bundle_schema != {
        "type": "object",
        "additionalProperties": False,
        "required": ["unit", "hard_max", "q1_default", "overflow_outcome"],
        "properties": {
            "unit": {"const": "bytes"},
            "hard_max": {"const": 67_108_864},
            "q1_default": {"const": 33_554_432},
            "overflow_outcome": {"const": "content_rejected"},
        },
    }:
        errors.append("HardLimitProfile schema does not freeze the Sandbox runtime bundle tuple")
    limit_schema = schema.get("$defs", {}).get("limit", {})
    allowed_units = set(limit_schema.get("properties", {}).get("unit", {}).get("enum", []))
    allowed_outcomes = set(
        limit_schema.get("properties", {}).get("overflow_outcome", {}).get("enum", [])
    )
    for family in expected_families & actual_families:
        family_schema = schema.get("$defs", {}).get(family, {})
        if set(family_schema.get("required", [])) != set(profile[family]):
            errors.append(f"Q1 family {family} differs from its closed JSON Schema fields")
        for name, limit in profile[family].items():
            if limit.get("unit") not in allowed_units:
                errors.append(f"Q1 limit {family}.{name} has an unknown unit")
            if limit.get("overflow_outcome") not in allowed_outcomes:
                errors.append(f"Q1 limit {family}.{name} has an unknown overflow outcome")


def check_foundation_surfaces(errors):
    openapi = (CONTRACT_ROOT / "openapi.yaml").read_text(encoding="utf-8")
    if "x-insight-contract-status: implementing-not-current" not in openapi:
        errors.append("OpenAPI must state that it is not current behavior")
    if "  - url: /v1" not in openapi:
        errors.append("OpenAPI must use the clean-cut /v1 server base")
    if "/v2" in openapi:
        errors.append("OpenAPI must not expose /v2")
    callback_contract = [
        "  /mcp/oauth/callback:",
        "      operationId: completeMcpOAuthCallback",
        "      security: []",
        "      x-insight-authentication: oauth_callback_state",
        "      x-insight-permission: none",
        "      x-insight-idempotency: callback_receipt",
        "      x-insight-rate-class: internal_callback",
        "      x-insight-audit: callback_receipt_event_outbox",
        "      x-insight-maximum-raw-query-bytes: 8192",
        '        "200":',
        '        "202":',
        '        "400":',
        '        "405":',
        '        "500":',
        '        "503":',
    ]
    if any(fragment not in openapi for fragment in callback_contract):
        errors.append("MCP OAuth callback OpenAPI contract is incomplete")
    operation_contract = [
        "  /operations/{operation_id}:",
        "      operationId: getOperation",
        "      x-insight-authentication: oidc_or_workload_credential",
        "      x-insight-permission: operation.read",
        "      x-insight-idempotency: read_only",
        "      x-insight-rate-class: control_read",
        "      x-insight-audit: access_log_only",
        '        "200":',
        "    OperationViewV1:",
        "    PublicJobTarget:",
        "    PublicJobState:",
    ]
    if any(fragment not in openapi for fragment in operation_contract):
        errors.append("public Operation Job projection OpenAPI contract is incomplete")
    run_contract = [
        "  /runs:",
        "  /runs/{run_id}:",
        "  /runs/{run_id}/result:",
        "  /runs/{run_id}/events:",
        "  /runs/{run_id}:pause:",
        "  /runs/{run_id}:resume:",
        "  /runs/{run_id}:cancel:",
        "      operationId: createRun",
        "      operationId: getRun",
        "      operationId: getRunResult",
        "      operationId: streamRunEvents",
        "      operationId: pauseRun",
        "      operationId: resumeRun",
        "      operationId: cancelRun",
        "    CreateRunRequestV1:",
        "    RunViewV1:",
        "    RunResultViewV1:",
        "    OpaqueRunEventCursor:",
        "    DurablePublicRunEventPayload:",
    ]
    if any(fragment not in openapi for fragment in run_contract):
        errors.append("public Run OpenAPI contract is incomplete")
    task_contract = [
        "  /tasks/{task_id}:",
        "  /tasks/{task_id}:submit-input:",
        "  /tasks/{task_id}:approve:",
        "  /tasks/{task_id}:reject:",
        "  /tasks/{task_id}:cancel:",
        "      operationId: getTask",
        "      operationId: submitTaskInput",
        "      operationId: approveTask",
        "      operationId: rejectTask",
        "      operationId: cancelTask",
        "    SubmitTaskInputV1:",
        "    TaskViewV1:",
    ]
    if any(fragment not in openapi for fragment in task_contract):
        errors.append("public Task OpenAPI contract is incomplete")
    artifact_contract = [
        "  /artifacts:prepare-upload:",
        "  /artifacts/{artifact_id}:",
        "  /artifacts/{artifact_id}:complete-upload:",
        "  /artifacts/{artifact_id}/content:",
        "  /artifacts/{artifact_id}:delete:",
        "      operationId: prepareArtifactUpload",
        "      operationId: getArtifact",
        "      operationId: completeArtifactUpload",
        "      operationId: downloadArtifactContent",
        "      operationId: deleteArtifact",
        "      x-insight-permission: artifact.write",
        "      x-insight-permission: artifact.read",
        "    ArtifactId:",
        "    PrepareArtifactUploadRequestV1:",
        "    PrepareArtifactUploadResponseV1:",
        "    CompleteArtifactUploadRequestV1:",
        "    ArtifactMutationAcceptedV1:",
        "    ArtifactViewV1:",
    ]
    if any(fragment not in openapi for fragment in artifact_contract):
        errors.append("public Artifact OpenAPI contract is incomplete")
    path_lines = [
        line for line in openapi.splitlines()
        if line.startswith("  /") and line.endswith(":")
    ]
    if path_lines != [
        "  /runs:",
        "  /runs/{run_id}:",
        "  /runs/{run_id}/result:",
        "  /runs/{run_id}/events:",
        "  /runs/{run_id}:pause:",
        "  /runs/{run_id}:resume:",
        "  /runs/{run_id}:cancel:",
        "  /tasks/{task_id}:",
        "  /tasks/{task_id}:submit-input:",
        "  /tasks/{task_id}:approve:",
        "  /tasks/{task_id}:reject:",
        "  /tasks/{task_id}:cancel:",
        "  /artifacts:prepare-upload:",
        "  /artifacts/{artifact_id}:",
        "  /artifacts/{artifact_id}:complete-upload:",
        "  /artifacts/{artifact_id}/content:",
        "  /artifacts/{artifact_id}:delete:",
        "  /operations/{operation_id}:",
        "  /mcp/oauth/callback:",
    ]:
        errors.append("OpenAPI exposes a path outside the reviewed implementing slice")
    if any(token in openapi for token in ["access_token", "refresh_token", "error_description"]):
        errors.append("MCP OAuth callback OpenAPI exposes a forbidden sensitive query field")
    if "DurablePublicRunEventPayload:" not in openapi or (
        "$ref: ./events/public-run-payloads.schema.json" not in openapi
    ):
        errors.append("foundation OpenAPI must consume the durable public Run event payload schema")
    proto = (ROOT / "proto" / "insight" / "platform" / "v1" / "foundation.proto").read_text(
        encoding="utf-8"
    )
    if "package insight.platform.v1;" not in proto:
        errors.append("foundation protobuf package is not insight.platform.v1")


def check_worker_manifest(errors):
    registries = load(CONTRACT_ROOT / "registries.json")
    schema = load(CONTRACT_ROOT / "schemas" / "worker-manifest.schema.json")
    example = load(CONTRACT_ROOT / "examples" / "q1-orchestration-worker-manifest.json")
    properties = schema.get("properties", {})
    expected_fields = {
        "manifest_version",
        "worker_role",
        "work_class",
        "adapter_runtime_digest",
        "protocol_version",
        "max_concurrency",
        "critical_control_reserved_slots",
    }
    if set(schema.get("required", [])) != expected_fields or set(properties) != expected_fields:
        errors.append("WorkerManifest schema is not the closed per-role capacity shape")
    if properties.get("work_class", {}).get("enum") != registries.get("work_classes"):
        errors.append("WorkerManifest WorkClass differs from the machine registry")
    if set(example) != expected_fields:
        errors.append("Q1 WorkerManifest example differs from the closed schema fields")
    if example.get("work_class") not in registries.get("work_classes", []):
        errors.append("Q1 WorkerManifest example has an unknown WorkClass")
    if not DIGEST.fullmatch(example.get("adapter_runtime_digest", "")):
        errors.append("Q1 WorkerManifest adapter/runtime digest is invalid")
    if not isinstance(example.get("max_concurrency"), int) or not 1 <= example.get("max_concurrency", 0) <= 65535:
        errors.append("Q1 WorkerManifest max_concurrency is invalid")
    if not isinstance(example.get("critical_control_reserved_slots"), int) or not 1 <= example.get("critical_control_reserved_slots", 0) <= 65535:
        errors.append("Q1 WorkerManifest critical-control reserve is invalid")


def check_candidate_manifest(errors):
    schema = load(CONTRACT_ROOT / "schemas" / "candidate-manifest.schema.json")
    properties = schema.get("properties", {})
    expected_fields = {
        "git_commit",
        "contract_digest",
        "database_schema_version",
        "component_images",
        "worker_manifests",
        "deployment_config_digest",
        "hard_limit_profile_digest",
        "policy_baseline_digest",
        "qualification_profile_digest",
        "created_at",
    }
    if schema.get("additionalProperties") is not False:
        errors.append("CandidateManifest schema must reject unknown fields")
    if set(schema.get("required", [])) != expected_fields or set(properties) != expected_fields:
        errors.append("CandidateManifest schema differs from its closed qualification shape")
    if properties.get("qualification_profile_digest", {}).get("pattern") != DIGEST.pattern:
        errors.append("CandidateManifest qualification profile digest is invalid")
    if properties.get("git_commit", {}).get("pattern") != (
        "^(sha1:[0-9a-f]{40}|sha256:[0-9a-f]{64})$"
    ):
        errors.append("CandidateManifest git_commit must be an exact tagged object ID")
    schema_version = properties.get("database_schema_version", {})
    if (
        schema_version.get("type") != "integer"
        or schema_version.get("minimum") != 1
        or schema_version.get("maximum") != 4_294_967_295
    ):
        errors.append("CandidateManifest database schema contract version is invalid")
    images = properties.get("component_images", {})
    if (
        images.get("type") != "object"
        or images.get("minProperties") != 1
        or images.get("maxProperties") != 256
        or not images.get("propertyNames", {}).get("enum")
        or images.get("additionalProperties", {}).get("pattern") != DIGEST.pattern
    ):
        errors.append("CandidateManifest component image closure is invalid")
    workers = properties.get("worker_manifests", {})
    if (
        workers.get("type") != "array"
        or workers.get("minItems") != 1
        or workers.get("maxItems") != 512
        or workers.get("uniqueItems") is not True
        or workers.get("items", {}).get("pattern") != DIGEST.pattern
    ):
        errors.append("CandidateManifest worker manifest closure is invalid")


def check_nominal_schemas(errors):
    registry = load(CONTRACT_ROOT / "schemas" / "nominal-types.json")
    expected_names = [
        "ApiProblem",
        "ArtifactRef",
        "DecimalMoney",
        "Digest",
        "Failure",
        "OpaqueListCursor",
        "OpaqueRunEventCursor",
        "UtcTimestamp",
        "UuidV7Id",
    ]
    schemas = registry.get("schemas")
    if not isinstance(schemas, list) or [item.get("name") for item in schemas] != expected_names:
        errors.append("nominal schema registry is not the closed foundation registry")
        return
    if registry.get("parameterized_types") != [
        {
            "name": "ValueRef",
            "reason": "the inline branch is parameterized by the owning closed payload schema; the artifact branch uses ArtifactRef",
        }
    ]:
        errors.append("ValueRef parameterized nominal contract is absent or changed")
    for item in schemas:
        path = CONTRACT_ROOT / item.get("path", "")
        if not path.is_file():
            errors.append(f"nominal schema is absent: {path}")
            continue
        schema = load(path)
        actual_digest = digest(schema)
        if item.get("canonical_digest") != actual_digest:
            errors.append(f"{path}: canonical digest differs from nominal registry")
        expected_reference = (
            f"urn:insight:platform:v1:nominal:{item['name']}@{actual_digest}"
        )
        if item.get("pinned_reference") != expected_reference:
            errors.append(f"{path}: pinned nominal reference is not exact")
        if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
            errors.append(f"{path}: JSON Schema dialect is not 2020-12")

    examples = load(CONTRACT_ROOT / "examples" / "foundation-scalars.json")
    if examples.get("status") != "implementing_not_current":
        errors.append("foundation scalar examples must not claim current behavior")
    if not RESOURCE_ID.fullmatch(examples.get("resource_id", "")):
        errors.append("foundation resource ID example is invalid")
    if not DIGEST.fullmatch(examples.get("digest", "")):
        errors.append("foundation digest example is invalid")
    if not UTC_MICROSECOND.fullmatch(examples.get("timestamp", "")):
        errors.append("foundation timestamp example is not UTC microsecond precision")
    money = examples.get("money", {})
    if (
        not re.fullmatch(r"[A-Z]{3}", money.get("currency", ""))
        or not isinstance(money.get("minor_units"), int)
        or abs(money.get("minor_units", 0)) > 9_007_199_254_740_991
        or not isinstance(money.get("scale"), int)
        or not 0 <= money.get("scale", -1) <= 18
    ):
        errors.append("foundation DecimalMoney example is invalid")
    artifact = examples.get("artifact_ref", {})
    if (
        not str(artifact.get("artifact_id", "")).startswith("art_")
        or not RESOURCE_ID.fullmatch(artifact.get("artifact_id", ""))
        or not DIGEST.fullmatch(artifact.get("content_digest", ""))
        or not isinstance(artifact.get("byte_length"), int)
        or not 0 <= artifact.get("byte_length", -1) <= 1_073_741_824
    ):
        errors.append("foundation ArtifactRef example is invalid")
    value_ref = examples.get("value_ref", {})
    if value_ref.get("kind") != "artifact" or value_ref.get("artifact") != artifact:
        errors.append("foundation ValueRef example does not preserve ArtifactRef exactly")
    if examples.get("list_cursor") == examples.get("run_event_cursor"):
        errors.append("foundation cursor examples must remain purpose-distinct")


def check_contract_manifest(errors):
    manifest = load(CONTRACT_ROOT / "manifest.json")
    if manifest.get("contract_profile") != "insight.platform/v1":
        errors.append("contract manifest profile is invalid")
    if manifest.get("status") != "implementing_not_current":
        errors.append("contract manifest must not claim current behavior during implementation")
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        errors.append("contract manifest files must be a non-empty array")
        return
    paths = []
    for item in files:
        path = ROOT / item.get("path", "")
        paths.append(item.get("path"))
        if not path.is_file():
            errors.append(f"contract manifest file is absent: {path}")
            continue
        raw = path.read_bytes()
        actual = hashlib.sha256(raw).hexdigest()
        if item.get("sha256") != actual or item.get("bytes") != len(raw):
            errors.append(f"contract manifest metadata differs for {path}")
    if len(paths) != len(set(paths)):
        errors.append("contract manifest contains duplicate file paths")
    if paths != CONTRACT_FILES:
        errors.append("contract manifest file closure/order differs from the independent registry")
    closure = canonical(files)
    actual_contract_digest = "sha256:" + hashlib.sha256(closure).hexdigest()
    if manifest.get("contract_digest") != actual_contract_digest:
        errors.append("contract manifest contract_digest does not match its file closure")


def snake_case(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def code_block_after(text, marker):
    start = text.index(marker)
    fence = text.index("```", start)
    end = text.index("```", fence + 3)
    lines = text[fence + 3 : end].strip().splitlines()
    if lines and lines[0] in {"text", "rust", "json", "yaml", "protobuf"}:
        lines = lines[1:]
    return lines


def check_spec_registry_alignment(errors):
    registries = load(CONTRACT_ROOT / "registries.json")
    error_registry = load(CONTRACT_ROOT / "errors.json")
    event_registry = load(CONTRACT_ROOT / "events" / "public-run-events.json")
    event_payload_schema = load(
        CONTRACT_ROOT / "events" / "public-run-payloads.schema.json"
    )
    expected_lock_ranks = [
        {"name": "command_receipt", "ordinal": 10},
        {"name": "scheduler_fairness", "ordinal": 15},
        {"name": "tenant_quota_policy", "ordinal": 20},
        {"name": "parent_root_aggregate", "ordinal": 30},
        {"name": "child_leaf_aggregate", "ordinal": 40},
        {"name": "job_fence", "ordinal": 50},
        {"name": "public_run_stream_head", "ordinal": 60},
        {"name": "append_only_projection_outbox", "ordinal": 70},
    ]
    if registries.get("lock_ranks") != expected_lock_ranks:
        errors.append("03 global lock-rank registry differs from the independent registry")

    expected_artifact_registries = {
        "artifact_purposes": [
            "authoring_document", "interface_contract", "typed_plan", "package", "sbom",
            "backend_binding", "model_generation_defaults", "run_input", "run_output",
            "capability_input", "capability_output", "context_source", "context_derived",
            "mcp_resource", "sandbox_input", "sandbox_output", "diagnostic", "export",
        ],
        "artifact_reference_kinds": [
            "definition", "input", "output", "evidence", "package", "attachment", "result",
            "provenance",
        ],
        "artifact_grant_operations": [
            "read_whole", "read_range", "write_staging", "commit_staging",
        ],
        "artifact_workload_audiences": [
            "principal", "runtime", "registry_worker", "capability_worker", "context_worker",
            "model_worker", "mcp_host", "sandbox_gateway", "artifact_worker",
        ],
        "blob_integrity_states": ["staging", "verified", "corrupt", "deleting", "deleted"],
        "public_job_kinds": [
            "resource_validation", "mcp_discovery", "context_dataset_build",
            "artifact_verify", "artifact_delete",
        ],
    }
    for registry, expected in expected_artifact_registries.items():
        if registries.get(registry) != expected:
            errors.append(f"{registry} differs from the independent Artifact/Operation registry")

    spec07 = (ROOT / "docs/specs/platform-v2/07-scheduler-workers-and-concurrency.md").read_text(
        encoding="utf-8"
    )
    work_class_body = re.search(r"enum WorkClass \{(.*?)\n\}", spec07, re.S)
    work_classes = [
        snake_case(item.strip().rstrip(","))
        for item in work_class_body.group(1).splitlines()
        if item.strip()
    ]
    if work_classes != registries.get("work_classes"):
        errors.append("07 WorkClass differs from the machine registry")
    if registries.get("plan_node_kinds") != [
        "start", "compute", "branch", "fork", "join", "map", "loop",
        "error_boundary", "model_loop", "capability_call", "context_query",
        "child_agent_call", "human_task", "timer_wait", "signal_wait", "return",
        "raise",
    ]:
        errors.append("05 PlanNodeKind registry is not closed")
    if registries.get("scope_kinds") != [
        "root", "branch_leg", "parallel_leg", "map_item", "loop_iteration",
        "model_round", "error_boundary",
    ]:
        errors.append("06 ScopeKind registry is not closed")
    if registries.get("wake_contract_kinds") != [
        "timer", "signal", "human_task", "approval", "remote_invocation",
        "child_run", "retry_deadline",
    ]:
        errors.append("03/06 WakeContractKind registry is not closed")
    if registries.get("interaction_kinds") != [
        "form", "url_consent", "business_input",
    ]:
        errors.append("06 InteractionKind registry is not closed")
    if registries.get("scheduler_priorities") != [
        "low", "normal", "high", "critical_control",
    ]:
        errors.append("07 SchedulerPriority registry is not closed")
    if registries.get("service_classes") != ["low", "normal", "high"]:
        errors.append("17 public ServiceClass registry is not closed")
    expected_work_owner_pairs = [
        {"work_class": "registry_validation", "owner_kind": "job"},
        {"work_class": "orchestration", "owner_kind": "node_execution"},
        {"work_class": "model", "owner_kind": "model_turn"},
        {"work_class": "capability_native", "owner_kind": "capability_invocation"},
        {"work_class": "capability_remote", "owner_kind": "capability_invocation"},
        {"work_class": "mcp", "owner_kind": "mcp_operation"},
        {"work_class": "context", "owner_kind": "context_query"},
        {"work_class": "sandbox", "owner_kind": "job"},
        {"work_class": "interaction", "owner_kind": "interaction"},
        {"work_class": "artifact", "owner_kind": "artifact"},
        {"work_class": "artifact", "owner_kind": "internal_blob"},
        {"work_class": "recovery", "owner_kind": "run"},
        {"work_class": "recovery", "owner_kind": "node_execution"},
        {"work_class": "recovery", "owner_kind": "capability_invocation"},
        {"work_class": "recovery", "owner_kind": "context_query"},
        {"work_class": "recovery", "owner_kind": "mcp_operation"},
        {"work_class": "recovery", "owner_kind": "model_turn"},
        {"work_class": "recovery", "owner_kind": "job"},
    ]
    if registries.get("execution_work_owner_pairs") != expected_work_owner_pairs:
        errors.append("03/07 execution work owner mapping is not closed")
    known_resource_kinds = {
        item.get("name") for item in registries.get("resource_kinds", [])
    }
    if any(
        item["work_class"] not in registries.get("work_classes", [])
        or item["owner_kind"] not in known_resource_kinds
        for item in expected_work_owner_pairs
    ):
        errors.append("execution work owner mapping references an unknown machine kind")

    if registries.get("agent_authoring_modes") != ["structured", "graph"]:
        errors.append("05 Agent authoring mode registry is not closed")
    expected_slot_kinds = ["model", "capability", "context", "child_agent", "skill"]
    if registries.get("dependency_slot_kinds") != expected_slot_kinds:
        errors.append("05 dependency slot kind registry is not closed")
    if registries.get("capability_backend_kinds") != [
        "native",
        "http",
        "grpc",
        "mcp",
        "sandbox",
    ]:
        errors.append("09 capability backend kind registry is not closed")
    if registries.get("capability_idempotency_kinds") != [
        "intrinsic",
        "caller_key",
        "reconcile_before_retry",
        "none",
    ]:
        errors.append("09 capability idempotency kind registry is not closed")
    if registries.get("capability_cancellation_kinds") != [
        "unsupported",
        "best_effort",
        "confirmed",
    ]:
        errors.append("09 capability cancellation kind registry is not closed")
    if registries.get("capability_progress_modes") != ["none", "events"]:
        errors.append("09 capability progress mode registry is not closed")
    if registries.get("capability_progress_durabilities") != [
        "none",
        "live_only",
        "coarse_durable",
    ]:
        errors.append("09 capability progress durability registry is not closed")
    if registries.get("skill_instruction_phases") != [
        "task_understanding",
        "planning",
        "tool_use",
        "validation",
        "output_composition",
    ]:
        errors.append("11 Skill instruction phase registry is not closed")
    if registries.get("skill_instruction_audiences") != [
        "planner",
        "tool_user",
        "validator",
        "composer",
    ]:
        errors.append("11 Skill instruction audience registry is not closed")
    if registries.get("skill_requirement_kinds") != [
        "capability",
        "context",
        "model_feature",
    ]:
        errors.append("11 Skill requirement kind registry is not closed")
    if registries.get("skill_package_entry_kinds") != [
        "manifest",
        "instruction",
        "reference",
        "example",
        "asset",
    ]:
        errors.append("11 Skill package entry kind registry is not closed")
    if registries.get("skill_selection_modes") != [
        "required",
        "plan_selected",
        "policy_selected",
        "model_proposed",
    ]:
        errors.append("11 Skill selection mode registry is not closed")
    if registries.get("context_backend_kinds") != [
        "managed_index",
        "remote_search",
        "mcp_resources",
        "sql_catalog",
        "artifact_collection",
        "native_catalog",
    ]:
        errors.append("12 Context backend kind registry is not closed")
    if registries.get("context_consistency_modes") != [
        "pinned_generation",
        "pin_at_run_admission",
        "latest_at_query_start",
        "external_observation",
    ]:
        errors.append("12 Context consistency mode registry is not closed")
    if registries.get("context_citation_strengths") != [
        "exact",
        "observation_only",
    ]:
        errors.append("12 Context citation strength registry is not closed")
    if registries.get("context_backend_outcome_kinds") != [
        "completed",
        "deferred",
        "retryable_failure",
        "permanent_failure",
    ]:
        errors.append("12 Context backend outcome registry is not closed")
    if registries.get("mcp_transport_kinds") != ["streamable_http"]:
        errors.append("13 MCP transport kind registry is not closed")
    if registries.get("mcp_authorization_principal_kinds") != [
        "per_user",
        "service_identity",
    ]:
        errors.append("13 MCP authorization principal kind registry is not closed")
    if registries.get("mcp_oauth_client_authentication_kinds") != [
        "none",
        "client_secret_basic",
    ]:
        errors.append("13 MCP OAuth client authentication kind registry is not closed")
    if registries.get("model_identity_stabilities") != [
        "pinned",
        "externally_mutable",
    ]:
        errors.append("16 Model identity stability registry is not closed")
    if registries.get("model_modalities") != [
        "text",
        "image",
        "audio",
        "document",
    ]:
        errors.append("16 Model modality registry is not closed")
    if registries.get("sandbox_runtime_families") != [
        "python",
        "node_js",
        "wasm_wasi",
        "reviewed_shell",
    ]:
        errors.append("14 Sandbox runtime family registry is not closed")
    if registries.get("sandbox_isolation_classes") != [
        {"name": "wasm", "security_rank": 1},
        {"name": "sandboxed_container", "security_rank": 2},
    ]:
        errors.append("14 Sandbox isolation class registry is not rank-closed")
    if registries.get("sandbox_abi_versions") != ["v1"]:
        errors.append("14 Sandbox ABI registry is not closed")
    if registries.get("sandbox_cleanup_policies") != ["single_use_destroy"]:
        errors.append("14 Sandbox cleanup policy registry is not closed")
    if registries.get("sandbox_entrypoint_kinds") != [
        "python_module",
        "node_module",
        "wasm_export",
        "reviewed_executable",
    ]:
        errors.append("14 Sandbox entrypoint kind registry is not closed")
    slot_schema = load(CONTRACT_ROOT / "schemas" / "frozen-slot-binding.schema.json")
    if slot_schema.get("$id") != "urn:insight:platform:v1:frozen-slot-binding":
        errors.append("FrozenSlotBinding schema has the wrong canonical ID")
    target_variants = slot_schema.get("properties", {}).get("target", {}).get("oneOf", [])
    schema_slot_kinds = [
        variant.get("properties", {}).get("kind", {}).get("const")
        for variant in target_variants
    ]
    if schema_slot_kinds != expected_slot_kinds:
        errors.append("FrozenSlotBinding schema differs from the dependency slot registry")

    if registries.get("quota_accounting_modes") != ["leased", "consumable", "reclaimable"]:
        errors.append("04 quota accounting mode registry is not closed")
    expected_scope_kinds = [
        "tenant",
        "agent_deployment",
        "work_class",
        "capability_deployment",
        "model_deployment",
        "context_deployment",
        "mcp_deployment",
        "sandbox_profile_revision",
        "run",
        "principal",
    ]
    if registries.get("quota_scope_kinds") != expected_scope_kinds:
        errors.append("04 quota scope kind registry is not closed")
    if registries.get("quota_window_kinds") != [
        "current",
        "run",
        "utc_day",
        "utc_month",
        "lifetime",
    ]:
        errors.append("04 quota window kind registry is not closed")
    limit_profile = load(CONTRACT_ROOT / "limits" / "q1-50.json")
    quota_paths = []
    for descriptor in registries.get("quota_dimensions", []):
        path = descriptor.get("hard_limit_path", "")
        quota_paths.append(path)
        parts = path.split(".")
        if len(parts) != 2 or parts[0] not in limit_profile or parts[1] not in limit_profile[parts[0]]:
            errors.append(f"quota dimension does not resolve to one HardLimitProfile field: {path!r}")
            continue
        limit = limit_profile[parts[0]][parts[1]]
        if descriptor.get("unit") != limit.get("unit"):
            errors.append(f"quota dimension unit differs from HardLimitProfile: {path}")
        if descriptor.get("accounting_mode") not in registries.get("quota_accounting_modes", []):
            errors.append(f"quota dimension has an unknown accounting mode: {path}")
    if len(quota_paths) != len(set(quota_paths)) or not quota_paths:
        errors.append("quota dimension HardLimitProfile paths must be non-empty and unique")

    spec02 = (ROOT / "docs/specs/platform-v2/02-identity-revision-and-deployment.md").read_text(
        encoding="utf-8"
    )
    if "contracts/platform-v1/registries.json" not in spec02:
        errors.append("02 does not name the authoritative prefix machine registry")
    if "| Prefix | 对象 |" in spec02:
        errors.append("02 duplicates the authoritative prefix machine registry")

    spec04 = (ROOT / "docs/specs/platform-v2/04-tenancy-security-and-policy.md").read_text(
        encoding="utf-8"
    )
    policy_body = re.search(r"enum PolicyKind \{(.*?)\n\}", spec04, re.S)
    policy_kinds = [
        snake_case(item.strip().rstrip(","))
        for item in policy_body.group(1).splitlines()
        if item.strip()
    ]
    if not set(policy_kinds).issubset(registries["policy_kinds"]):
        errors.append("04 required PolicyKind values are absent from the machine registry")
    permission_block = re.search(
        r"```text\n(installation\.manage/support.*?)\n```", spec04, re.S
    )
    # Spec 04 may show a deliberately non-exhaustive permission example. Only compare
    # the legacy exhaustive block when it is present; the machine registry remains the
    # closed authority and is validated independently above.
    if permission_block is not None:
        permission_lines = permission_block.group(1).splitlines()
        permissions = []
        for line in permission_lines:
            for token in line.split():
                domain, actions = token.split(".", 1)
                permissions.extend(f"{domain}.{action}" for action in actions.split("/"))
        if permissions != registries["permissions"]:
            errors.append("04 permission registry differs from the machine registry")

    spec05 = (ROOT / "docs/specs/platform-v2/05-agent-and-typed-plan.md").read_text(
        encoding="utf-8"
    )
    platform_codes = [line.strip() for line in code_block_after(spec05, "PlatformFailureCode`首批闭集为")]
    if platform_codes != error_registry["failure"]["platform_codes"]:
        errors.append("05 PlatformFailureCode differs from errors.json")

    spec17 = (ROOT / "docs/specs/platform-v2/17-management-and-runtime-api.md").read_text(
        encoding="utf-8"
    )
    # Accepted spec 17 intentionally summarizes these registries instead of duplicating
    # their complete machine-readable definitions. Keep validating an exhaustive block
    # when an older/full form is supplied, while treating errors.json and events.json as
    # the closed authorities otherwise.
    if "ApiProblemCode`首版闭集" in spec17:
        api_codes = [line.strip() for line in code_block_after(spec17, "ApiProblemCode`首版闭集")]
        if api_codes != error_registry["api_problem"]["codes"]:
            errors.append("17 ApiProblemCode differs from errors.json")
    if "最小closed event types" in spec17:
        event_types = []
        for line in code_block_after(spec17, "最小closed event types"):
            value = line.split("#", 1)[0].strip()
            if value:
                event_types.append(value)
        machine_events = [item["type"] for item in event_registry["event_types"]]
        if event_types != machine_events:
            errors.append("17 PublicRunEventType differs from the machine event registry")

    expected_source_kinds = [
        "run",
        "run_control",
        "node_execution",
        "skill_activation",
        "model_turn",
        "capability_invocation",
        "context_query",
        "child_run_link",
        "interaction",
        "approval_task",
    ]
    if event_registry.get("durable_source_kinds") != expected_source_kinds:
        errors.append("public Run event durable source-kind registry is not closed")
    expected_event_sources = {
        "run.snapshot": None,
        "run.queued": "run",
        "run.started": "run",
        "run.waiting": "run",
        "run.paused": "run_control",
        "run.resumed": "run_control",
        "run.cancelling": "run",
        "run.completed": "run",
        "run.failed": "run",
        "run.cancelled": "run",
        "run.timed_out": "run",
        "node.started": "node_execution",
        "node.completed": "node_execution",
        "node.failed": "node_execution",
        "node.cancelled": "node_execution",
        "node.timed_out": "node_execution",
        "skill.selected": "skill_activation",
        "skill.activated": "skill_activation",
        "skill.rejected": "skill_activation",
        "model.started": "model_turn",
        "model.delta": None,
        "model.tool_intent": "model_turn",
        "model.completed": "model_turn",
        "model.failed": "model_turn",
        "model.cancelled": "model_turn",
        "model.timed_out": "model_turn",
        "capability.started": "capability_invocation",
        "capability.waiting": "capability_invocation",
        "capability.input_required": "capability_invocation",
        "capability.progress": "capability_invocation",
        "capability.completed": "capability_invocation",
        "capability.failed": "capability_invocation",
        "capability.cancelled": "capability_invocation",
        "capability.timed_out": "capability_invocation",
        "context.started": "context_query",
        "context.completed": "context_query",
        "context.failed": "context_query",
        "context.cancelled": "context_query",
        "context.timed_out": "context_query",
        "child.started": "child_run_link",
        "child.waiting": "child_run_link",
        "child.progress": "child_run_link",
        "child.completed": "child_run_link",
        "child.failed": "child_run_link",
        "child.cancelled": "child_run_link",
        "child.timed_out": "child_run_link",
        "interaction.required": "interaction",
        "interaction.resolved": "interaction",
        "approval.required": "approval_task",
        "approval.resolved": "approval_task",
        "stream.live_gap": None,
    }
    machine_event_sources = {
        item.get("type"): item.get("durable_source_kind")
        for item in event_registry.get("event_types", [])
    }
    if machine_event_sources != expected_event_sources:
        errors.append("public Run event durable source mapping differs from the independent registry")

    durable_event_sources = {
        event_type: source_kind
        for event_type, source_kind in expected_event_sources.items()
        if source_kind is not None
    }
    if event_payload_schema.get("additionalProperties") is not False or set(
        event_payload_schema.get("required", [])
    ) != {"event_type", "data"}:
        errors.append("durable public Run event payload root is not closed")
    schema_event_types = (
        event_payload_schema.get("properties", {})
        .get("event_type", {})
        .get("enum", [])
    )
    if schema_event_types != list(durable_event_sources):
        errors.append("durable public Run event payload event-type registry differs")
    branches = event_payload_schema.get("oneOf", [])
    branch_sources = {}
    for branch in branches if isinstance(branches, list) else []:
        properties = branch.get("properties", {})
        event_type = properties.get("event_type", {}).get("const")
        data = properties.get("data", {})
        if branch.get("additionalProperties") is not False or set(
            branch.get("required", [])
        ) != {"event_type", "data"}:
            errors.append(f"durable event payload branch {event_type!r} is not closed")
        if data.get("additionalProperties") is not False or set(
            data.get("required", [])
        ) != {
            "source_kind",
            "source_id",
            "source_projection_version",
            "safe_summary",
        }:
            errors.append(f"durable event data branch {event_type!r} is not closed")
        if event_type in branch_sources:
            errors.append(f"duplicate durable event payload branch {event_type!r}")
        branch_sources[event_type] = (
            data.get("properties", {}).get("source_kind", {}).get("const")
        )
    if branch_sources != durable_event_sources:
        errors.append("durable public Run event payload branches differ from event registry")


def main():
    errors = []
    for path in sorted(CONTRACT_ROOT.rglob("*.json")):
        try:
            load(path)
        except (json.JSONDecodeError, DuplicateKey) as failure:
            errors.append(f"{path}: {failure}")
    check_fixtures(errors)
    check_limits(errors)
    check_foundation_surfaces(errors)
    check_worker_manifest(errors)
    check_candidate_manifest(errors)
    check_nominal_schemas(errors)
    check_contract_manifest(errors)
    check_spec_registry_alignment(errors)
    pending = [
        str(path.relative_to(ROOT))
        for path in CONTRACT_ROOT.rglob("*")
        if path.is_file() and "sha256:pending" in path.read_text(encoding="utf-8")
    ]
    if pending:
        errors.append(f"pending digests remain: {pending}")
    if errors:
        print("Platform v1 contract validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("Platform v1 fixtures, limits, active OpenAPI slice and protobuf foundation are valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
