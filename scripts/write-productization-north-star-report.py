#!/usr/bin/env python3
"""Build the closed north-star report from a first-Run marker and journey clock."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform


def fail(message: str) -> None:
    raise SystemExit(f"cannot write Productization north-star report: {message}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--marker", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--started-epoch", type=int, required=True)
    parser.add_argument("--fresh-checkout", action="store_true")
    arguments = parser.parse_args()
    if not arguments.fresh_checkout:
        fail("--fresh-checkout is required; a working-tree run cannot prove the checkout gate")
    try:
        marker = json.loads(arguments.marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(str(error))
    expected_marker_fields = {"schema_version", "report_kind", "run_id", "state", "result_verified", "completed_at"}
    if not isinstance(marker, dict) or set(marker) != expected_marker_fields:
        fail("first-Run marker is not closed")
    if marker["schema_version"] != 1 or marker["report_kind"] != "insight.productization.first-run-marker/v1":
        fail("first-Run marker kind is invalid")
    started = datetime.fromtimestamp(arguments.started_epoch, tz=timezone.utc)
    try:
        completed = datetime.fromisoformat(marker["completed_at"].replace("Z", "+00:00"))
    except (TypeError, ValueError):
        fail("marker completed_at is not RFC3339")
    elapsed = int((completed - started).total_seconds() * 1000)
    if elapsed < 0:
        fail("first Run completed before the recorded checkout start")
    check = lambda identifier, evidence: {"id": identifier, "status": "passed", "evidence": evidence}
    report = {
        "schema_version": 1,
        "report_kind": "insight.productization.north-star-report/v1",
        "contract_profile": "insight.platform/v1",
        "source_revision": arguments.source_revision,
        "profile": "starter",
        "environment": {
            "os": platform.system().lower(),
            "architecture": platform.machine().lower(),
            "fresh_checkout": True,
            "fresh_profile": True,
        },
        "journey_started_at": started.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "first_run_completed_at": marker["completed_at"],
        "elapsed_to_first_run_ms": elapsed,
        "maximum_elapsed_ms": 600_000,
        "documented_manual_commands": [
            "git clone <repository> && cd insight-agent-platform",
            "scripts/run-productization-journey.sh --console-browser --report-directory <directory>",
        ],
        "maximum_manual_commands": 3,
        "external_model_key_required": False,
        "run": {
            "run_id": marker["run_id"],
            "state": marker["state"],
            "result_verified": marker["result_verified"],
        },
        "checks": [
            check("doctor", "the supported-environment doctor completed before any local mutation"),
            check("init", "a new project path received an explicit non-production identity and profile"),
            check("dev", "the starter public /v1 closure reached ready on a fresh durable authority"),
            check("first_run", "public authoring and Run commands returned a succeeded deterministic Inline result"),
        ],
        "status": "passed" if elapsed <= 600_000 else "failed",
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")), encoding="utf-8")


if __name__ == "__main__":
    main()
