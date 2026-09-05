#!/usr/bin/env python3
"""Fail closed validation for the Productization M0 golden-scenario manifest."""

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "examples/productization/scenarios.json"
REPORT_SCHEMA = ROOT / "examples/productization/scenario-report.schema.json"
AGGREGATE_SCHEMA = ROOT / "examples/productization/scenario-aggregate.schema.json"
MANIFEST_FIELDS = {
    "schema_version",
    "manifest_kind",
    "contract_profile",
    "profile_semantics",
    "scenarios",
}
EXPECTED_IDS = {
    "deterministic-first-run",
    "exact-model-streaming-chat",
    "native-and-remote-capability",
    "remote-mcp-tool-and-resource",
    "context-retrieval-and-citation",
    "approval-task-resume",
    "timer-signal-restart-recovery",
    "subagent-quota-and-cancel",
    "artifact-lifecycle-and-rejection",
    "sandbox-and-remote-framework-capability",
}
REQUIRED_FIELDS = {
    "id",
    "owner",
    "profile",
    "dependencies",
    "fixture",
    "automation_layer",
    "capabilities",
    "entrypoints",
    "assertions",
    "failure_probes",
}
ALLOWED_PROFILES = {
    "starter",
    "starter+context",
    "starter+mcp,remote-capability",
    "starter+model",
    "starter+remote-capability",
    "starter+remote-capability,sandbox",
    "starter+sandbox",
}
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
REPORT_ENVIRONMENT_FIELDS = {"os", "architecture", "fresh_profile"}
REPORT_CHECK_FIELDS = {"id", "status", "evidence"}
DIGEST_PATTERN = "^sha256:[0-9a-f]{64}$"
REQUIRED_ENTRYPOINTS = {"cli", "http_fixture", "console"}


class DuplicateKeyError(ValueError):
    pass


def fail(message: str) -> None:
    print(f"productization M0 manifest invalid: {message}", file=sys.stderr)
    raise SystemExit(1)


def strict_json(path: Path) -> object:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise DuplicateKeyError(f"{path}: duplicate object key {key!r}")
            value[key] = item
        return value

    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicates)


def list_of_strings(value: object, field: str, scenario_id: str) -> list[str]:
    if not isinstance(value, list) or not value or any(not isinstance(item, str) or not item for item in value):
        fail(f"{scenario_id}.{field} must be a non-empty list of non-empty strings")
    return value


def main() -> None:
    try:
        manifest = strict_json(MANIFEST)
        report_schema = strict_json(REPORT_SCHEMA)
        aggregate_schema = strict_json(AGGREGATE_SCHEMA)
    except (OSError, json.JSONDecodeError, DuplicateKeyError) as error:
        fail(str(error))
    if not isinstance(manifest, dict):
        fail("top level must be an object")
    if set(manifest) != MANIFEST_FIELDS:
        fail(f"top level must use exactly {sorted(MANIFEST_FIELDS)}")
    if manifest.get("schema_version") != 1:
        fail("schema_version must be 1")
    if manifest.get("manifest_kind") != "insight.productization.scenarios/v1":
        fail("manifest_kind is not closed")
    if manifest.get("contract_profile") != "insight.platform/v1":
        fail("contract_profile must be insight.platform/v1")
    if manifest.get("profile_semantics") != "minimum_required_feature_closure":
        fail("profile must mean the minimum required feature closure")
    schema_profiles = (
        report_schema.get("properties", {}).get("profile", {}).get("enum")
        if isinstance(report_schema, dict)
        else None
    )
    if not isinstance(schema_profiles, list) or set(schema_profiles) != ALLOWED_PROFILES:
        fail("scenario report schema profile enum differs from the checked manifest profiles")
    if not isinstance(report_schema, dict):
        fail("scenario report schema must be an object")
    report_properties = report_schema.get("properties", {})
    report_required = report_schema.get("required")
    evidence_inputs = report_properties.get("evidence_inputs", {})
    expected_sandbox_condition = [
        {
            "if": {
                "properties": {
                    "scenario_id": {
                        "const": "sandbox-and-remote-framework-capability"
                    }
                },
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
    report_environment = report_properties.get("environment", {})
    report_definitions = report_schema.get("$defs")
    report_checks = (
        report_definitions.get("checks") if isinstance(report_definitions, dict) else None
    )
    if (
        report_schema.get("additionalProperties") is not False
        or set(report_properties) != REPORT_FIELDS
        or set(report_required or []) != REPORT_FIELDS
        or report_properties.get("schema_version", {}).get("const") != 1
        or report_properties.get("report_kind", {}).get("const")
        != "insight.productization.scenario-report/v1"
        or report_properties.get("contract_profile", {}).get("const")
        != "insight.platform/v1"
        or set(report_properties.get("actual_profile", {}).get("enum", []))
        != ACTUAL_PROFILES
        or report_properties.get("qualification_run_id", {}).get("pattern")
        != DIGEST_PATTERN
        or report_properties.get("profile_digest", {}).get("pattern") != DIGEST_PATTERN
        or report_properties.get("source_revision", {}).get("pattern")
        != "^[0-9a-f]{40}$"
        or set(report_properties.get("automation_layer", {}).get("enum", []))
        != {"P2", "P3"}
        or set(report_properties.get("status", {}).get("enum", []))
        != {"passed", "incomplete", "failed"}
        or not isinstance(report_environment, dict)
        or report_environment.get("additionalProperties") is not False
        or set(report_environment.get("required", [])) != REPORT_ENVIRONMENT_FIELDS
        or set(report_environment.get("properties", {})) != REPORT_ENVIRONMENT_FIELDS
        or any(
            report_properties.get(field, {}).get("$ref") != "#/$defs/checks"
            for field in ("entrypoints", "assertions", "failure_probes")
        )
        or not isinstance(evidence_inputs, dict)
        or evidence_inputs.get("additionalProperties") is not False
        or set(evidence_inputs.get("properties", {})) != {"opensandbox_qualification"}
        or evidence_inputs.get("properties", {})
        .get("opensandbox_qualification", {})
        .get("pattern")
        != DIGEST_PATTERN
        or report_schema.get("allOf") != expected_sandbox_condition
    ):
        fail("scenario report schema is not the closed evidence contract")
    if (
        not isinstance(report_checks, dict)
        or report_checks.get("type") != "array"
        or report_checks.get("minItems") != 1
        or report_checks.get("maxItems") != 3
    ):
        fail("scenario report check list schema must be bounded")
    report_check_item = report_checks.get("items")
    if (
        not isinstance(report_check_item, dict)
        or report_check_item.get("additionalProperties") is not False
        or set(report_check_item.get("required", [])) != REPORT_CHECK_FIELDS
        or set(report_check_item.get("properties", {})) != REPORT_CHECK_FIELDS
        or set(
            report_check_item.get("properties", {}).get("status", {}).get("enum", [])
        )
        != {"passed", "failed", "not_run"}
    ):
        fail("scenario report check item schema must be closed")
    if not isinstance(aggregate_schema, dict):
        fail("scenario aggregate schema must be an object")
    aggregate_properties = aggregate_schema.get("properties", {})
    aggregate_required = aggregate_schema.get("required")
    expected_aggregate_fields = {
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
    if (
        aggregate_schema.get("additionalProperties") is not False
        or set(aggregate_properties) != expected_aggregate_fields
        or set(aggregate_required or []) != expected_aggregate_fields
        or aggregate_properties.get("scenario_count", {}).get("const") != 10
        or aggregate_properties.get("status", {}).get("const") != "passed"
        or aggregate_properties.get("schema_version", {}).get("const") != 1
        or aggregate_properties.get("source_revision", {}).get("pattern")
        != "^[0-9a-f]{40}$"
        or aggregate_properties.get("scenario_manifest_digest", {}).get("pattern")
        != DIGEST_PATTERN
        or aggregate_properties.get("actual_profile", {}).get("const") != "all"
        or aggregate_properties.get("qualification_run_id", {}).get("pattern")
        != DIGEST_PATTERN
        or aggregate_properties.get("profile_digest", {}).get("pattern") != DIGEST_PATTERN
        or aggregate_properties.get("report_kind", {}).get("const")
        != "insight.productization.scenario-aggregate/v1"
        or aggregate_properties.get("reports", {}).get("minItems") != 10
        or aggregate_properties.get("reports", {}).get("maxItems") != 10
        or aggregate_properties.get("reports", {}).get("uniqueItems") is not True
        or aggregate_properties.get("reports", {}).get("items") is not False
    ):
        fail("scenario aggregate schema is not the closed 10/10 contract")
    aggregate_definitions = aggregate_schema.get("$defs")
    aggregate_items = (
        aggregate_definitions.get("report")
        if isinstance(aggregate_definitions, dict)
        else None
    )
    aggregate_report_fields = {"scenario_id", "profile", "report_digest"}
    if (
        not isinstance(aggregate_items, dict)
        or aggregate_items.get("additionalProperties") is not False
        or set(aggregate_items.get("required", [])) != aggregate_report_fields
        or set(aggregate_items.get("properties", {})) != aggregate_report_fields
        or aggregate_items.get("properties", {})
        .get("report_digest", {})
        .get("pattern")
        != DIGEST_PATTERN
    ):
        fail("scenario aggregate report entries are not closed")
    scenarios = manifest.get("scenarios")
    if not isinstance(scenarios, list) or len(scenarios) != len(EXPECTED_IDS):
        fail("scenarios must contain exactly ten entries")
    aggregate_prefix_items = aggregate_properties.get("reports", {}).get("prefixItems")
    if not isinstance(aggregate_prefix_items, list) or len(aggregate_prefix_items) != 10:
        fail("scenario aggregate must close ten canonical report positions")
    for item, scenario in zip(aggregate_prefix_items, scenarios):
        if (
            not isinstance(item, dict)
            or set(item) != {"$ref", "properties"}
            or item.get("$ref") != "#/$defs/report"
            or not isinstance(scenario, dict)
            or item.get("properties")
            != {
                "scenario_id": {"const": scenario.get("id")},
                "profile": {"const": scenario.get("profile")},
            }
        ):
            fail("scenario aggregate report positions differ from the checked manifest")
    observed_ids: set[str] = set()
    for scenario in scenarios:
        if not isinstance(scenario, dict) or set(scenario) != REQUIRED_FIELDS:
            fail("each scenario must use exactly the closed required fields")
        scenario_id = scenario.get("id")
        if not isinstance(scenario_id, str) or not scenario_id:
            fail("scenario id must be a non-empty string")
        if scenario_id in observed_ids:
            fail(f"duplicate scenario id {scenario_id}")
        observed_ids.add(scenario_id)
        if not isinstance(scenario.get("owner"), str) or not scenario["owner"]:
            fail(f"{scenario_id}.owner must name one responsible implementation surface")
        if scenario.get("profile") not in ALLOWED_PROFILES:
            fail(f"{scenario_id}.profile is not a closed starter feature profile")
        for field in ("dependencies", "capabilities", "entrypoints", "assertions", "failure_probes"):
            list_of_strings(scenario.get(field), field, scenario_id)
        fixture = scenario.get("fixture")
        if not isinstance(fixture, str) or not fixture.startswith("tests/productization/"):
            fail(f"{scenario_id}.fixture must reserve a productization test path")
        expected_fixture = f"tests/productization/{scenario_id.replace('-', '_')}.rs"
        if fixture != expected_fixture:
            fail(f"{scenario_id}.fixture must be {expected_fixture}")
        fixture_path = ROOT / fixture
        if not fixture_path.is_file() or fixture_path.is_symlink():
            fail(f"{scenario_id}.fixture must be a checked-in regular file")
        if scenario.get("automation_layer") not in {"P2", "P3"}:
            fail(f"{scenario_id}.automation_layer must be P2 or P3")
        if set(scenario["entrypoints"]) != REQUIRED_ENTRYPOINTS:
            fail(f"{scenario_id}.entrypoints must include exactly CLI, HTTP fixture, and Console")
    if observed_ids != EXPECTED_IDS:
        fail(f"scenario IDs differ: expected {sorted(EXPECTED_IDS)}, got {sorted(observed_ids)}")
    print("productization M0 scenario manifest verified")


if __name__ == "__main__":
    main()
