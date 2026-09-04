#!/usr/bin/env python3
"""Classify changed repository paths into closed productization CI lanes."""

from __future__ import annotations

import argparse
from pathlib import Path, PurePosixPath
import sys


def fail(message: str) -> None:
    print(f"CI path classification failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def is_under(path: PurePosixPath, prefix: str) -> bool:
    parts = PurePosixPath(prefix).parts
    return path.parts[: len(parts)] == parts


def classify(paths: list[str], force_all: bool = False) -> dict[str, bool]:
    if force_all:
        return {
            "quick": True,
            "cli": True,
            "console": True,
            "runtime": True,
            "mcp_interop": True,
            "policy": True,
        }
    normalized: list[PurePosixPath] = []
    for raw in paths:
        value = raw.strip()
        if not value:
            continue
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts:
            fail(f"unsafe changed path {value!r}")
        normalized.append(path)
    if not normalized:
        fail("changed path set is empty")

    console = any(is_under(path, "web/console") for path in normalized)
    cli = any(
        is_under(path, prefix)
        for path in normalized
        for prefix in (
            "crates/insight-cli",
            "tests/productization",
            "tests/fixtures/productization-reports",
            "examples/productization",
            "release",
        )
    ) or any(
        path.name.startswith("check-productization")
        or path.name.startswith("build-product-release")
        or path.name.startswith("build-release-")
        or path.name == "sign-product-release.py"
        or path.name == "check-product-release.py"
        or path.name == "test_product_release.py"
        or path.name == "run-productization-journey.sh"
        or path.name == "qualify-productization-first-run.py"
        or path.name == "test_productization_journey_runner.py"
        for path in normalized
    )
    policy = any(
        path.as_posix() in {"Cargo.toml", "Cargo.lock", "deny.toml"}
        or (path.name == "Cargo.toml" and is_under(path, "crates"))
        for path in normalized
    )

    non_runtime_prefixes = (
        "docs",
        "web/console",
        "crates/insight-cli",
        "examples/productization",
        "tests/productization",
        "tests/fixtures/productization-reports",
        "release",
    )
    non_runtime_root_files = {
        ".gitignore",
        "AGENTS.md",
        "LICENSE",
        "README.md",
    }
    runtime = policy or any(
        not (
            path.as_posix() in non_runtime_root_files
            or any(is_under(path, prefix) for prefix in non_runtime_prefixes)
            or (
                is_under(path, "scripts")
                and (
                    path.name.startswith("check-productization")
                    or path.name.startswith("build-product-release")
                    or path.name.startswith("build-release-")
                    or path.name == "sign-product-release.py"
                    or path.name == "check-product-release.py"
                    or path.name == "test_product_release.py"
                    or path.name == "classify-ci-paths.py"
                    or path.name == "run-productization-journey.sh"
                    or path.name == "qualify-productization-first-run.py"
                    or path.name == "test_classify_ci_paths.py"
                    or path.name == "test_productization_journey_runner.py"
                    or path.name == "test_productization_scenario_reports.py"
                )
            )
        )
        for path in normalized
    )
    mcp_interop = runtime and any(
        is_under(path, prefix)
        for path in normalized
        for prefix in (
            "crates/mcp",
            "crates/platform-mcp-host",
            "crates/platform-mcp-rpc",
            "crates/platform-mcp-service",
            "crates/platform-mcp-cleanup-worker",
            "proto",
            "contracts/platform-v1",
            "tests/interop",
        )
    )
    return {
        "quick": True,
        "cli": cli,
        "console": console,
        "runtime": runtime,
        "mcp_interop": mcp_interop,
        "policy": policy,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths_file", type=Path, nargs="?")
    parser.add_argument("--all", action="store_true", dest="force_all")
    parser.add_argument("--github-output", type=Path)
    arguments = parser.parse_args()
    if arguments.force_all:
        paths: list[str] = []
    elif arguments.paths_file is None:
        fail("paths_file is required unless --all is used")
    else:
        try:
            paths = arguments.paths_file.read_text(encoding="utf-8").splitlines()
        except OSError as error:
            fail(str(error))
    result = classify(paths, force_all=arguments.force_all)
    rendered = "".join(f"{name}={str(value).lower()}\n" for name, value in result.items())
    if arguments.github_output is None:
        sys.stdout.write(rendered)
    else:
        try:
            with arguments.github_output.open("a", encoding="utf-8") as output:
                output.write(rendered)
        except OSError as error:
            fail(str(error))


if __name__ == "__main__":
    main()
