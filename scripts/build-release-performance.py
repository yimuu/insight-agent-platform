#!/usr/bin/env python3
"""Build a bounded machine-readable release performance gate from observed step evidence."""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TARGETS = {
    "aarch64-apple-darwin", "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin", "x86_64-unknown-linux-gnu",
}
PHASES = {
    "console_build", "runtime_build_push", "sandbox_runner_build_push",
    "console_image_build_push", "sbom", "provenance", "cosign", "cold_pull", "warm_reuse",
}


def load(path: Path) -> object:
    return json.loads(path.read_bytes())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--evidence-directory", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", args.version):
        raise ValueError("invalid exact version")
    if not re.fullmatch(r"[0-9a-f]{40}", args.git_commit):
        raise ValueError("invalid exact git commit")
    budgets = load(ROOT / "release/performance-budgets-v1.json")["seconds"]
    observations: list[dict[str, object]] = []
    for path in sorted(args.evidence_directory.glob("*.performance.json")):
        value = load(path)
        if not isinstance(value, list):
            raise ValueError(f"{path} must contain an observation array")
        observations.extend(value)
    names = [item.get("name") for item in observations if isinstance(item, dict)]
    required = PHASES | {f"cli_build:{target}" for target in TARGETS}
    if set(names) != required or len(names) != len(required):
        raise ValueError("performance evidence is missing, duplicated, or contains an unknown phase")
    results = []
    blocked = False
    for item in sorted(observations, key=lambda value: value["name"]):
        name = item["name"]
        phase = "cli_build" if name.startswith("cli_build:") else name
        duration = item.get("duration_seconds")
        if not isinstance(duration, (int, float)) or isinstance(duration, bool) or not math.isfinite(duration) or duration < 0:
            raise ValueError(f"{name} duration is invalid")
        if set(item) - {"name", "duration_seconds", "cache_hit", "bytes", "previous_bytes"}:
            raise ValueError(f"{name} observation is not closed")
        budget = budgets[phase]
        status = "passed" if duration <= budget else "release_blocker"
        blocked |= status == "release_blocker"
        result = {"name": name, "duration_seconds": duration, "budget_seconds": budget, "status": status}
        for optional in ("cache_hit", "bytes", "previous_bytes"):
            if optional in item:
                result[optional] = item[optional]
        results.append(result)
    report = {
        "schema_version": 1,
        "kind": "insight.release.performance/v1",
        "version": args.version,
        "git_commit": args.git_commit,
        "status": "release_blocker" if blocked else "passed",
        "measurements": results,
    }
    args.output.write_bytes(json.dumps(report, sort_keys=True, separators=(",", ":")).encode())
    if blocked:
        raise SystemExit("release performance budget exceeded")


if __name__ == "__main__":
    main()
