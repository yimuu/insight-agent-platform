#!/usr/bin/env python3
"""Fail closed validation for the Productization M0 golden-scenario manifest."""

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "examples/productization/scenarios.json"
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
    "wasi-and-remote-framework-capability",
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
    "starter+remote-capability,wasi",
    "starter+wasi",
}
REQUIRED_ENTRYPOINTS = {"cli", "http_fixture", "console"}


def fail(message: str) -> None:
    print(f"productization M0 manifest invalid: {message}", file=sys.stderr)
    raise SystemExit(1)


def list_of_strings(value: object, field: str, scenario_id: str) -> list[str]:
    if not isinstance(value, list) or not value or any(not isinstance(item, str) or not item for item in value):
        fail(f"{scenario_id}.{field} must be a non-empty list of non-empty strings")
    return value


def main() -> None:
    try:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(str(error))
    if not isinstance(manifest, dict):
        fail("top level must be an object")
    if manifest.get("schema_version") != 1:
        fail("schema_version must be 1")
    if manifest.get("manifest_kind") != "insight.productization.scenarios/v1":
        fail("manifest_kind is not closed")
    if manifest.get("contract_profile") != "insight.platform/v1":
        fail("contract_profile must be insight.platform/v1")
    scenarios = manifest.get("scenarios")
    if not isinstance(scenarios, list) or len(scenarios) != len(EXPECTED_IDS):
        fail("scenarios must contain exactly ten entries")
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
        if scenario.get("automation_layer") not in {"P2", "P3"}:
            fail(f"{scenario_id}.automation_layer must be P2 or P3")
        if set(scenario["entrypoints"]) != REQUIRED_ENTRYPOINTS:
            fail(f"{scenario_id}.entrypoints must include exactly CLI, HTTP fixture, and Console")
    if observed_ids != EXPECTED_IDS:
        fail(f"scenario IDs differ: expected {sorted(EXPECTED_IDS)}, got {sorted(observed_ids)}")
    print("productization M0 scenario manifest verified")


if __name__ == "__main__":
    main()
