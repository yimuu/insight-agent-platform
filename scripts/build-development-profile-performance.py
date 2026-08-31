#!/usr/bin/env python3
"""Build the closed, observed single-node development-profile qualification report."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import re


FEATURES = {"context", "mcp", "model", "remote-capability", "wasi"}
SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
REVISION = re.compile(r"^[0-9a-f]{40}$")


def non_negative(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be a finite non-negative number")
    return parsed


def positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def canonical_features(raw: str) -> list[str]:
    if not raw:
        return []
    if raw == "all":
        return sorted(FEATURES)
    values = raw.split(",")
    if (
        any(value not in FEATURES for value in values)
        or len(values) != len(set(values))
        or values != sorted(values)
    ):
        raise ValueError("features must be all or one canonical sorted unique feature set")
    return values


def gate(name: str, observed: int | float, budget: int | float, passed: bool) -> dict[str, object]:
    return {
        "name": name,
        "observed": observed,
        "budget": budget,
        "status": "passed" if passed else "failed",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--profile-digest", required=True)
    parser.add_argument("--features", default="")
    parser.add_argument("--cpu-count", required=True, type=positive_integer)
    parser.add_argument("--memory-bytes", required=True, type=positive_integer)
    parser.add_argument("--network-mbps", required=True, type=positive_integer)
    parser.add_argument("--cold-ready-seconds", required=True, type=non_negative)
    parser.add_argument("--warm-ready-seconds", required=True, type=non_negative)
    parser.add_argument("--download-seconds", required=True, type=non_negative)
    parser.add_argument("--download-bytes", required=True, type=positive_integer)
    parser.add_argument("--idle-rss-bytes", required=True, type=positive_integer)
    parser.add_argument("--idle-cpu-percent", required=True, type=non_negative)
    parser.add_argument("--idle-stabilization-seconds", required=True, type=positive_integer)
    parser.add_argument("--project-disk-bytes", required=True, type=positive_integer)
    parser.add_argument("--source-compilations", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    if not VERSION.fullmatch(args.version):
        raise ValueError("version must be an exact stable semver")
    if not REVISION.fullmatch(args.git_commit):
        raise ValueError("git commit must be exact lowercase hex")
    if not SHA256.fullmatch(args.profile_digest):
        raise ValueError("profile digest must be sha256")
    if args.source_compilations < 0:
        raise ValueError("source compilations cannot be negative")
    features = canonical_features(args.features)
    label = "starter" if not features else ("all" if len(features) == len(FEATURES) else f"starter+{','.join(features)}")

    gates = [
        gate("runner_cpu_count", args.cpu_count, 4, args.cpu_count >= 4),
        gate("runner_memory_bytes", args.memory_bytes, 8 * 1024**3, args.memory_bytes >= 8 * 1024**3),
        gate("runner_network_mbps", args.network_mbps, 100, args.network_mbps >= 100),
        gate("cold_ready_seconds", args.cold_ready_seconds, 300, args.cold_ready_seconds <= 300),
        gate("warm_ready_seconds", args.warm_ready_seconds, 60, args.warm_ready_seconds <= 60),
        gate("idle_stabilization_seconds", args.idle_stabilization_seconds, 300, args.idle_stabilization_seconds >= 300),
        gate("idle_rss_bytes", args.idle_rss_bytes, 6 * 1024**3, args.idle_rss_bytes <= 6 * 1024**3),
        gate("idle_cpu_percent_single_core", args.idle_cpu_percent, 10, args.idle_cpu_percent <= 10),
        gate("project_disk_bytes", args.project_disk_bytes, 8 * 1024**3, args.project_disk_bytes <= 8 * 1024**3),
        gate("source_compilations", args.source_compilations, 0, args.source_compilations == 0),
    ]
    passed = all(item["status"] == "passed" for item in gates)
    report = {
        "schema_version": 1,
        "kind": "insight.dev.performance-report/v1",
        "version": args.version,
        "git_commit": args.git_commit,
        "profile": {"name": label, "features": features, "digest": args.profile_digest},
        "environment": {
            "class": "development",
            "deployment": "single_node",
            "production": False,
            "cpu_count": args.cpu_count,
            "memory_bytes": args.memory_bytes,
            "network_mbps": args.network_mbps,
        },
        "measurements": {
            "cold_ready_seconds": args.cold_ready_seconds,
            "warm_ready_seconds": args.warm_ready_seconds,
            "download_seconds": args.download_seconds,
            "download_content_bytes": args.download_bytes,
            "download_bytes_method": "verified_image_content_size",
            "idle_stabilization_seconds": args.idle_stabilization_seconds,
            "idle_rss_bytes": args.idle_rss_bytes,
            "idle_cpu_percent_single_core": args.idle_cpu_percent,
            "project_disk_bytes": args.project_disk_bytes,
            "source_compilations": args.source_compilations,
        },
        "gates": gates,
        "qualification": {"L4": "not_run", "L5": "not_run", "L6": "not_run"},
        "status": "passed" if passed else "failed",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    if not passed:
        raise SystemExit("development profile performance budget exceeded")


if __name__ == "__main__":
    main()
