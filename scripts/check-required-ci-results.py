#!/usr/bin/env python3
"""Fail closed unless every path-selected CI lane has the expected result."""

from __future__ import annotations

import argparse
from collections.abc import Mapping


RESULTS = ("success", "failure", "cancelled", "skipped")


def expected_results(selections: Mapping[str, bool]) -> dict[str, str]:
    runtime = selections["runtime"]
    return {
        "changes": "success",
        "quick": "success",
        "lint": "success" if runtime else "skipped",
        "test": "success" if runtime else "skipped",
        "cli": "success" if selections["cli"] and not runtime else "skipped",
        "console": "success" if selections["console"] else "skipped",
        "policy": "success" if selections["policy"] else "skipped",
        "productization": "success",
    }


def validate_results(
    selections: Mapping[str, bool], actual: Mapping[str, str]
) -> list[str]:
    expected = expected_results(selections)
    return [
        f"{lane} lane expected {expected_result} but was {actual.get(lane)!r}"
        for lane, expected_result in expected.items()
        if actual.get(lane) != expected_result
    ]


def selection(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise argparse.ArgumentTypeError("selection must be true or false")


def main() -> None:
    parser = argparse.ArgumentParser()
    for lane in ("runtime", "cli", "console", "policy"):
        parser.add_argument(f"--{lane}-selected", type=selection, required=True)
    for lane in (
        "changes",
        "quick",
        "lint",
        "test",
        "cli",
        "console",
        "policy",
        "productization",
    ):
        parser.add_argument(f"--{lane}-result", choices=RESULTS, required=True)
    arguments = vars(parser.parse_args())
    selections = {
        lane: arguments[f"{lane.replace('-', '_')}_selected"]
        for lane in ("runtime", "cli", "console", "policy")
    }
    actual = {
        lane: arguments[f"{lane.replace('-', '_')}_result"]
        for lane in (
            "changes",
            "quick",
            "lint",
            "test",
            "cli",
            "console",
            "policy",
            "productization",
        )
    }
    failures = validate_results(selections, actual)
    if failures:
        raise SystemExit("\n".join(failures))
    print("Required CI lane results passed.")


if __name__ == "__main__":
    main()
