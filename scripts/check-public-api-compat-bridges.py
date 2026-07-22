#!/usr/bin/env python3
"""Check the normalized root API against narrowly audited compatibility bridges.

The Phase 0 baseline remains immutable.  Each rule below describes one concrete
parameter that moved behind a crate boundary and the exact generic view bound
that replaced it.  The gate constructs the only accepted API inventory from
the baseline, then compares the complete result byte-for-byte with the actual
normalized inventory.
"""

from __future__ import annotations

import argparse
import contextlib
import difflib
import io
import json
import sys
from dataclasses import dataclass
from pathlib import Path


class BridgeConfigurationError(ValueError):
    """The frozen baseline no longer has the declaration a bridge expects."""


@dataclass(frozen=True)
class BridgeRule:
    name: str
    paths: tuple[str, ...]
    generic_name: str
    concrete_path: str
    view_path: str
    has_receiver: bool


BRIDGE_RULES = (
    BridgeRule(
        name="registered retrieval view",
        paths=(
            "insight_agent_platform::engine::FrozenRetrievalTarget::validate_registered",
            "insight_agent_platform::engine::retrieval::FrozenRetrievalTarget::validate_registered",
        ),
        generic_name="R",
        concrete_path=(
            "insight_agent_platform::resources::retrievals::RegisteredRetrieval"
        ),
        view_path="insight_engine::retrieval::RegisteredRetrievalView",
        has_receiver=True,
    ),
    BridgeRule(
        name="model-tool claim view",
        paths=(
            "insight_agent_platform::engine::TaskExecutionRequest::from_model_tool_claim",
            "insight_agent_platform::engine::worker::TaskExecutionRequest::from_model_tool_claim",
        ),
        generic_name="C",
        concrete_path=(
            "insight_agent_platform::engine::repository::ModelToolTaskClaim"
        ),
        view_path="insight_engine::worker::ModelToolTaskClaimView",
        has_receiver=False,
    ),
)


def borrowed(type_: dict[str, object]) -> dict[str, object]:
    return {
        "borrowed_ref": {
            "is_mutable": False,
            "lifetime": None,
            "type": type_,
        }
    }


def baseline_inputs(rule: BridgeRule) -> list[dict[str, object]]:
    inputs = []
    if rule.has_receiver:
        inputs.append(borrowed({"generic": "Self"}))
    inputs.append(
        borrowed({"resolved_path": {"args": None, "path": rule.concrete_path}})
    )
    return inputs


def bridged_generics(rule: BridgeRule) -> dict[str, object]:
    return {
        "params": [
            {
                "kind": {
                    "type": {
                        "bounds": [
                            {
                                "trait_bound": {
                                    "generic_params": [],
                                    "modifier": "maybe",
                                    "trait": {
                                        "args": None,
                                        "path": "core::marker::Sized",
                                    },
                                }
                            },
                            {
                                "trait_bound": {
                                    "generic_params": [],
                                    "modifier": "none",
                                    "trait": {
                                        "args": None,
                                        "path": rule.view_path,
                                    },
                                }
                            },
                        ],
                        "default": None,
                        "is_synthetic": False,
                    }
                },
                "name": rule.generic_name,
            }
        ],
        "where_predicates": [],
    }


def bridged_inputs(rule: BridgeRule) -> list[dict[str, object]]:
    inputs = []
    if rule.has_receiver:
        inputs.append(borrowed({"generic": "Self"}))
    inputs.append(borrowed({"generic": rule.generic_name}))
    return inputs


def audited_expected_inventory(baseline: str) -> tuple[str, int]:
    rules_by_path = {
        path: rule for rule in BRIDGE_RULES for path in rule.paths
    }
    if len(rules_by_path) != sum(len(rule.paths) for rule in BRIDGE_RULES):
        raise BridgeConfigurationError("bridge rules contain a duplicate public path")

    transformed: list[str] = []
    seen: set[str] = set()
    for raw_line in baseline.splitlines(keepends=True):
        line = raw_line.rstrip("\r\n")
        ending = raw_line[len(line) :]
        path, separator, remainder = line.partition("\t")
        rule = rules_by_path.get(path)
        if rule is None:
            transformed.append(raw_line)
            continue
        if path in seen:
            raise BridgeConfigurationError(f"duplicate baseline declaration: {path}")
        seen.add(path)

        kind, second_separator, declaration_json = remainder.partition("\t")
        if separator != "\t" or second_separator != "\t":
            raise BridgeConfigurationError(f"malformed baseline declaration: {path}")
        if kind != "inherent_function":
            raise BridgeConfigurationError(
                f"expected inherent_function for {path}; found {kind!r}"
            )
        try:
            declaration = json.loads(declaration_json)
        except json.JSONDecodeError as error:
            raise BridgeConfigurationError(
                f"invalid declaration JSON for {path}: {error}"
            ) from error

        expected_generics = {"params": [], "where_predicates": []}
        if declaration.get("generics") != expected_generics:
            raise BridgeConfigurationError(
                f"baseline generics changed for audited bridge {path}"
            )
        signature = declaration.get("sig")
        if not isinstance(signature, dict):
            raise BridgeConfigurationError(f"baseline signature is missing for {path}")
        if signature.get("inputs") != baseline_inputs(rule):
            raise BridgeConfigurationError(
                f"baseline inputs changed for audited bridge {path}"
            )

        declaration["generics"] = bridged_generics(rule)
        signature["inputs"] = bridged_inputs(rule)
        canonical = json.dumps(declaration, ensure_ascii=False, separators=(",", ":"))
        transformed.append(f"{path}\t{kind}\t{canonical}{ending}")

    missing = sorted(set(rules_by_path) - seen)
    if missing:
        raise BridgeConfigurationError(
            "baseline is missing audited bridge declaration(s): " + ", ".join(missing)
        )
    return "".join(transformed), len(seen)


def unified_inventory_diff(
    expected: str, actual: str, baseline_name: str, actual_name: str
) -> str:
    return "".join(
        difflib.unified_diff(
            expected.splitlines(keepends=True),
            actual.splitlines(keepends=True),
            fromfile=f"{baseline_name} (with audited source-compatible bridges)",
            tofile=actual_name,
        )
    )


def check_inventory(baseline: str, actual: str, baseline_name: str, actual_name: str) -> int:
    try:
        expected, accepted_count = audited_expected_inventory(baseline)
    except BridgeConfigurationError as error:
        print(f"public API bridge gate configuration error: {error}", file=sys.stderr)
        return 2

    if actual != expected:
        print(
            "public API differs from the Phase 0 baseline plus the audited bridges:",
            file=sys.stderr,
        )
        print(
            unified_inventory_diff(expected, actual, baseline_name, actual_name),
            end="",
            file=sys.stderr,
        )
        return 1

    print(
        "accepted source-compatible bridge count: "
        f"{accepted_count} declarations ({len(BRIDGE_RULES)} signatures, flat+nested)"
    )
    return 0


def self_test() -> None:
    def baseline_declaration(rule: BridgeRule) -> dict[str, object]:
        return {
            "generics": {"params": [], "where_predicates": []},
            "has_body": True,
            "header": {
                "abi": "Rust",
                "is_async": False,
                "is_const": False,
                "is_unsafe": False,
            },
            "sig": {
                "inputs": baseline_inputs(rule),
                "is_c_variadic": False,
                "output": None,
            },
        }

    lines = ["# fixture\n"]
    for rule in BRIDGE_RULES:
        for path in rule.paths:
            payload = json.dumps(
                baseline_declaration(rule), separators=(",", ":")
            )
            lines.append(f"{path}\tinherent_function\t{payload}\n")
    lines.append("fixture::unchanged\tstruct\t{}\n")
    baseline = "".join(lines)
    expected, count = audited_expected_inventory(baseline)
    assert count == 4
    accepted_output = io.StringIO()
    with contextlib.redirect_stdout(accepted_output):
        assert check_inventory(baseline, expected, "baseline", "actual") == 0
    assert "accepted source-compatible bridge count: 4" in accepted_output.getvalue()

    unexpected = expected + "fixture::leak\tstruct\t{}\n"
    rejected_output = io.StringIO()
    with contextlib.redirect_stderr(rejected_output):
        assert check_inventory(baseline, unexpected, "baseline", "actual") == 1
    assert "--- baseline (with audited source-compatible bridges)" in rejected_output.getvalue()
    assert "+fixture::leak" in rejected_output.getvalue()

    wrong_bound = expected.replace(
        "insight_engine::worker::ModelToolTaskClaimView",
        "insight_engine::worker::WrongView",
        1,
    )
    wrong_bound_output = io.StringIO()
    with contextlib.redirect_stderr(wrong_bound_output):
        assert check_inventory(baseline, wrong_bound, "baseline", "actual") == 1
    assert "WrongView" in wrong_bound_output.getvalue()

    missing = baseline.replace(lines[1], "", 1)
    try:
        audited_expected_inventory(missing)
    except BridgeConfigurationError:
        pass
    else:
        raise AssertionError("missing audited declaration was accepted")
    print("public API bridge checker self-test passed")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", nargs="?", type=Path)
    parser.add_argument("actual", nargs="?", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if args.baseline is None or args.actual is None:
        parser.error("baseline and actual are required unless --self-test is used")
    return check_inventory(
        args.baseline.read_text(),
        args.actual.read_text(),
        str(args.baseline),
        str(args.actual),
    )


if __name__ == "__main__":
    raise SystemExit(main())
