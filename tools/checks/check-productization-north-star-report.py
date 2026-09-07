#!/usr/bin/env python3
"""Validate a closed fresh-checkout Productization north-star report."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re


REPORT_FIELDS = {
    "schema_version", "report_kind", "contract_profile", "source_revision", "profile",
    "environment", "journey_started_at", "first_run_completed_at",
    "elapsed_to_first_run_ms", "maximum_elapsed_ms", "documented_manual_commands",
    "maximum_manual_commands", "external_model_key_required", "run", "checks", "status",
}
ENVIRONMENT_FIELDS = {"os", "architecture", "fresh_checkout", "fresh_profile"}
RUN_FIELDS = {"run_id", "state", "result_verified"}
CHECK_FIELDS = {"id", "status", "evidence"}
EXPECTED_CHECKS = ["doctor", "init", "dev", "first_run"]
REVISION = re.compile(r"^[0-9a-f]{40}$")
RUN_ID = re.compile(r"^run_[0-9a-f-]+$")


def fail(message: str) -> None:
    raise SystemExit(f"productization north-star report invalid: {message}")


def timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        fail(f"{field} must be an RFC3339 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        fail(f"{field} must be an RFC3339 timestamp")
    if parsed.tzinfo is None:
        fail(f"{field} must include a timezone")
    return parsed


def validate(path: Path, expected_revision: str) -> None:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(str(error))
    if not isinstance(report, dict) or set(report) != REPORT_FIELDS:
        fail(f"report must use exactly {sorted(REPORT_FIELDS)}")
    if report["schema_version"] != 1 or report["report_kind"] != "insight.productization.north-star-report/v1":
        fail("schema/report kind is not v1")
    if report["contract_profile"] != "insight.platform/v1" or report["profile"] != "starter":
        fail("report must qualify the starter insight.platform/v1 profile")
    if report["source_revision"] != expected_revision or not REVISION.fullmatch(expected_revision):
        fail("source_revision differs from the exact requested revision")

    environment = report["environment"]
    if not isinstance(environment, dict) or set(environment) != ENVIRONMENT_FIELDS:
        fail("environment must be closed")
    if environment["fresh_checkout"] is not True or environment["fresh_profile"] is not True:
        fail("fresh_checkout and fresh_profile must both be true")
    for field in ("os", "architecture"):
        if not isinstance(environment[field], str) or not environment[field] or len(environment[field]) > 64:
            fail(f"environment.{field} is invalid")

    started = timestamp(report["journey_started_at"], "journey_started_at")
    completed = timestamp(report["first_run_completed_at"], "first_run_completed_at")
    elapsed = report["elapsed_to_first_run_ms"]
    if not isinstance(elapsed, int) or isinstance(elapsed, bool) or elapsed < 0:
        fail("elapsed_to_first_run_ms must be a non-negative integer")
    calculated = int((completed - started).total_seconds() * 1000)
    if elapsed != calculated:
        fail(f"elapsed_to_first_run_ms must equal timestamp delta {calculated}")
    if report["maximum_elapsed_ms"] != 600_000 or elapsed > 600_000:
        fail("checkout-to-first-Run exceeded the 600000ms gate")

    commands = report["documented_manual_commands"]
    if not isinstance(commands, list) or not 1 <= len(commands) <= 3:
        fail("documented_manual_commands must contain one to three commands")
    if any(not isinstance(command, str) or not command or len(command) > 256 for command in commands):
        fail("documented_manual_commands contains an invalid command")
    if report["maximum_manual_commands"] != 3 or report["external_model_key_required"] is not False:
        fail("manual-command or external-model-key gate is not closed")

    run = report["run"]
    if not isinstance(run, dict) or set(run) != RUN_FIELDS:
        fail("run projection must be closed")
    if not isinstance(run["run_id"], str) or not RUN_ID.fullmatch(run["run_id"]):
        fail("run.run_id is invalid")
    if run["state"] != "succeeded" or run["result_verified"] is not True:
        fail("first Run must be succeeded with a verified result")

    checks = report["checks"]
    if not isinstance(checks, list) or len(checks) != len(EXPECTED_CHECKS):
        fail("checks must contain the exact north-star closure")
    observed = []
    for check in checks:
        if not isinstance(check, dict) or set(check) != CHECK_FIELDS:
            fail("every check must be closed")
        observed.append(check["id"])
        if check["status"] != "passed" or not isinstance(check["evidence"], str) or not check["evidence"]:
            fail("every north-star check must contain Passed evidence")
    if observed != EXPECTED_CHECKS:
        fail(f"check order differs: expected {EXPECTED_CHECKS}, got {observed}")
    if report["status"] != "passed":
        fail("status must be passed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--source-revision", required=True)
    arguments = parser.parse_args()
    validate(arguments.report, arguments.source_revision)
    print("validated Productization north-star report; gate=passed")


if __name__ == "__main__":
    main()
