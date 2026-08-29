#!/usr/bin/env python3
"""Validate closed M4 golden-scenario reports against their checked manifest."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "examples/productization/scenarios.json"
REPORT_KIND = "insight.productization.scenario-report/v1"
CONTRACT_PROFILE = "insight.platform/v1"
REPORT_FIELDS = {
    "schema_version",
    "report_kind",
    "scenario_id",
    "contract_profile",
    "profile",
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
CHECK_FIELDS = {"id", "status", "evidence"}
REVISION = re.compile(r"^[0-9a-f]{40}$")


def fail(message: str) -> None:
    print(f"productization scenario report invalid: {message}", file=sys.stderr)
    raise SystemExit(1)


def read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"{path}: {error}")


def timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        fail(f"{field} must be an RFC3339 string")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        fail(f"{field} must be an RFC3339 timestamp")


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
) -> None:
    raw = read_json(path)
    if not isinstance(raw, dict) or set(raw) != REPORT_FIELDS:
        fail(f"{path}: report must use exactly {sorted(REPORT_FIELDS)}")
    if raw.get("schema_version") != 1 or raw.get("report_kind") != REPORT_KIND:
        fail(f"{path}: schema/report kind is not v1")
    if raw.get("scenario_id") != scenario["id"]:
        fail(f"{path}: scenario_id does not match manifest")
    if raw.get("contract_profile") != CONTRACT_PROFILE:
        fail(f"{path}: contract_profile must be {CONTRACT_PROFILE}")
    if raw.get("profile") != scenario["profile"]:
        fail(f"{path}: profile does not match manifest")
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
    arguments = parser.parse_args()
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
    expected_names = {f"{scenario['id']}.json" for scenario in scenarios}
    if not arguments.report_directory.is_dir():
        fail(f"report directory does not exist: {arguments.report_directory}")
    observed_paths = sorted(arguments.report_directory.glob("*.json"))
    if not observed_paths:
        fail("report directory contains no scenario reports")
    observed_names = {path.name for path in observed_paths}
    unknown = observed_names - expected_names
    if unknown:
        fail(f"unknown scenario reports: {sorted(unknown)}")
    if not arguments.allow_incomplete and observed_names != expected_names:
        fail(f"report set differs: missing={sorted(expected_names - observed_names)}")
    by_id = {scenario["id"]: scenario for scenario in scenarios}
    for path in observed_paths:
        validate_report(
            path,
            by_id[path.stem],
            require_passed=not arguments.allow_incomplete,
            expected_revision=expected_revision,
        )
    print(
        f"validated {len(observed_paths)} productization scenario report(s); "
        f"complete_gate={str(not arguments.allow_incomplete).lower()}"
    )


if __name__ == "__main__":
    main()
