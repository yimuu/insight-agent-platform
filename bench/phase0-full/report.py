#!/usr/bin/env python3
"""Fail-closed evaluator for the fresh full-runtime Phase 0 WAL baseline."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


OLD_FULL_REFERENCE = {
    "source": (
        "bench/results/2026-07-26-qualification-v3/"
        "gate-d-soak-10rps-2h"
    ),
    "report": (
        "bench/reports/"
        "2026-07-26-durable-runtime-50-active-runs-optimized.md"
    ),
    "duration_seconds": 7200,
    "accepted_runs": 71801,
    "total_wal_bytes": 71033480938,
}
OLD_FULL_REFERENCE["wal_bytes_per_accepted"] = (
    OLD_FULL_REFERENCE["total_wal_bytes"]
    / OLD_FULL_REFERENCE["accepted_runs"]
)

MONOTONIC_FIELDS = {
    "wal": (
        "wal_bytes",
        "wal_records",
        "wal_fpi",
        "wal_buffers_full",
    ),
    "bgwriter": (
        "checkpoints_timed",
        "checkpoints_req",
        "checkpoint_write_time_ms",
        "checkpoint_sync_time_ms",
        "buffers_checkpoint",
    ),
    "database": (
        "database_bytes",
        "xact_commit",
        "xact_rollback",
        "temp_files",
        "temp_bytes",
        "deadlocks",
        "blks_read",
        "blks_hit",
        "blk_read_time_ms",
        "blk_write_time_ms",
    ),
    "io": (
        "reads",
        "read_time_ms",
        "writes",
        "write_time_ms",
        "writebacks",
        "writeback_time_ms",
        "extends",
        "extend_time_ms",
        "fsyncs",
        "fsync_time_ms",
    ),
}

PHYSICAL_CATEGORIES = {
    "payload",
    "artifact_object_metadata",
    "structural",
    "mixed",
    "unmapped",
}

RELATION_CATEGORIES = {
    "payload",
    "artifact_object_metadata",
    "structural",
    "catalog",
}

REQUIRED_FULL_LEDGER_GROWTH = {
    "workflow_runs",
    "payloads",
    "execution_events",
    "projection_checkpoints",
    "scheduler_checkpoints",
    "public_event_outbox",
}


def require(condition: bool, failures: List[str], message: str) -> None:
    if not condition:
        failures.append(message)


def load_json(path: str) -> Dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} does not contain a JSON object")
    return value


def write_report(path: str, report: Dict[str, Any]) -> None:
    serialized = json.dumps(report, indent=2, sort_keys=True) + "\n"
    Path(path).write_text(serialized, encoding="utf-8")
    print(serialized, end="")


def nonnegative_int(
    value: Any,
    field: str,
    failures: List[str],
) -> Optional[int]:
    if isinstance(value, bool):
        failures.append(f"{field} is not a non-negative integer")
        return None
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, float) and math.isfinite(value) and value.is_integer():
        parsed = int(value)
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        parsed = int(value)
    else:
        failures.append(f"{field} is not a non-negative integer")
        return None
    if parsed < 0:
        failures.append(f"{field} is negative")
        return None
    return parsed


def finite_float(
    value: Any,
    field: str,
    failures: List[str],
) -> Optional[float]:
    if isinstance(value, bool):
        failures.append(f"{field} is not numeric")
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        failures.append(f"{field} is not numeric")
        return None
    if not math.isfinite(parsed):
        failures.append(f"{field} is not finite")
        return None
    return parsed


def parse_lsn(value: Any, field: str, failures: List[str]) -> Optional[int]:
    if not isinstance(value, str) or re.fullmatch(
        r"[0-9A-F]+/[0-9A-F]{1,8}",
        value,
    ) is None:
        failures.append(f"{field} is not a canonical PostgreSQL LSN")
        return None
    high_text, low_text = value.split("/", 1)
    high = int(high_text, 16)
    low = int(low_text, 16)
    if high > 0xFFFFFFFF or low > 0xFFFFFFFF:
        failures.append(f"{field} has an out-of-range component")
        return None
    return (high << 32) + low


def metric_count(
    summary: Dict[str, Any],
    name: str,
    failures: List[str],
    absent_is_zero: bool = False,
) -> int:
    metrics = summary.get("metrics")
    values = metrics.get(name, {}).get("values") if isinstance(metrics, dict) else None
    if not isinstance(values, dict) or "count" not in values:
        if absent_is_zero:
            return 0
        failures.append(f"required k6 metric {name}.count is missing")
        return 0
    parsed = nonnegative_int(values["count"], f"k6 metric {name}.count", failures)
    return parsed if parsed is not None else 0


def metric_value(
    summary: Dict[str, Any],
    name: str,
    field: str,
    failures: List[str],
) -> Optional[float]:
    metrics = summary.get("metrics")
    values = metrics.get(name, {}).get("values") if isinstance(metrics, dict) else None
    if not isinstance(values, dict) or field not in values:
        failures.append(f"required k6 metric {name}.{field} is missing")
        return None
    return finite_float(values[field], f"k6 metric {name}.{field}", failures)


def validate_profile(
    profile: Dict[str, Any],
    qualification: bool,
    failures: List[str],
) -> Dict[str, int]:
    expected_seconds = nonnegative_int(
        profile.get("expected_seconds"),
        "profile.expected_seconds",
        failures,
    )
    warmup_seconds = nonnegative_int(
        profile.get("warmup_seconds"),
        "profile.warmup_seconds",
        failures,
    )
    arrival_rate = nonnegative_int(
        profile.get("arrival_rate_per_second"),
        "profile.arrival_rate_per_second",
        failures,
    )
    expected_arrivals = nonnegative_int(
        profile.get("expected_arrivals"),
        "profile.expected_arrivals",
        failures,
    )
    require(
        profile.get("persistence_mode") == "full",
        failures,
        "profile persistence mode is not full",
    )
    require(
        profile.get("agent_id") == "action_demo",
        failures,
        "profile agent is not action_demo",
    )
    fixture = profile.get("fixture")
    require(
        isinstance(fixture, dict)
        and fixture.get("request_body")
        == {"text": "phase0 full WAL baseline fixture"}
        and fixture.get("variable_fields") == ["x-request-id"],
        failures,
        "profile fixed action_demo fixture is missing or changed",
    )
    if (
        expected_seconds is not None
        and arrival_rate is not None
        and expected_arrivals is not None
    ):
        require(
            expected_arrivals == expected_seconds * arrival_rate,
            failures,
            "profile expected arrivals do not equal rate times duration",
        )
    if qualification:
        require(
            profile.get("profile") == "qualification",
            failures,
            "formal evaluator received a non-qualification profile",
        )
        require(
            warmup_seconds == 60,
            failures,
            f"formal Phase 0 warm-up is {warmup_seconds}s, expected 60s",
        )
        require(
            expected_seconds == 600,
            failures,
            f"formal Phase 0 duration is {expected_seconds}s, expected 600s",
        )
        require(
            arrival_rate == 10,
            failures,
            f"formal Phase 0 arrival rate is {arrival_rate}/s, expected 10/s",
        )
        require(
            expected_arrivals == 6000,
            failures,
            (
                "formal Phase 0 scheduled arrival count is "
                f"{expected_arrivals}, expected 6000"
            ),
        )
        require(
            profile.get("minimum_wal_keep_size_bytes") == 8589934592,
            failures,
            "formal Phase 0 minimum wal_keep_size is not 8GiB",
        )
    return {
        "warmup_seconds": warmup_seconds or 0,
        "expected_seconds": expected_seconds or 0,
        "arrival_rate": arrival_rate or 0,
        "expected_arrivals": expected_arrivals or 0,
    }


def validate_k6(
    summary: Dict[str, Any],
    expected_seconds: int,
    expected_arrivals: int,
    qualification: bool,
    failures: List[str],
) -> Dict[str, Any]:
    iterations = metric_count(summary, "iterations", failures)
    scheduled = metric_count(
        summary,
        "phase0_full_arrivals_scheduled",
        failures,
    )
    late_arrivals = metric_count(
        summary,
        "phase0_full_arrivals_late",
        failures,
        absent_is_zero=True,
    )
    max_arrival_lateness = metric_value(
        summary,
        "phase0_full_arrival_lateness",
        "max",
        failures,
    )
    p95_arrival_lateness = metric_value(
        summary,
        "phase0_full_arrival_lateness",
        "p(95)",
        failures,
    )
    p99_arrival_lateness = metric_value(
        summary,
        "phase0_full_arrival_lateness",
        "p(99)",
        failures,
    )
    arrival_slot_ms = (
        expected_seconds * 1000 / expected_arrivals
        if expected_arrivals
        else 0.0
    )
    dropped = metric_count(
        summary,
        "dropped_iterations",
        failures,
        absent_is_zero=True,
    )
    accepted = metric_count(summary, "phase0_full_run_accepted", failures)
    observed = metric_count(
        summary,
        "phase0_full_run_terminal_observed",
        failures,
    )
    succeeded = metric_count(summary, "phase0_full_run_succeeded", failures)
    rejected = metric_count(
        summary,
        "phase0_full_run_rejected",
        failures,
        absent_is_zero=True,
    )
    failed = metric_count(
        summary,
        "phase0_full_run_failed",
        failures,
        absent_is_zero=True,
    )
    interrupted = metric_count(
        summary,
        "phase0_full_run_interrupted",
        failures,
        absent_is_zero=True,
    )
    duration_ms = finite_float(
        summary.get("state", {}).get("testRunDurationMs")
        if isinstance(summary.get("state"), dict)
        else None,
        "k6 state.testRunDurationMs",
        failures,
    )
    lifecycle_p95 = metric_value(
        summary,
        "phase0_full_run_lifecycle_duration",
        "p(95)",
        failures,
    )
    lifecycle_p99 = metric_value(
        summary,
        "phase0_full_run_lifecycle_duration",
        "p(99)",
        failures,
    )

    require(
        iterations == scheduled,
        failures,
        (
            f"k6 iterations {iterations} do not equal exact scheduled "
            f"arrivals {scheduled}"
        ),
    )
    require(
        scheduled == accepted + rejected,
        failures,
        (
            f"exact scheduled arrivals {scheduled} do not equal accepted + "
            f"rejected ({accepted} + {rejected})"
        ),
    )
    require(
        observed == accepted,
        failures,
        f"accepted closure is {observed}/{accepted}, expected 100%",
    )
    require(
        succeeded + failed + interrupted == observed,
        failures,
        "terminal outcome counters do not sum to terminal observed",
    )
    require(failed == 0, failures, f"{failed} full Runs failed")
    require(interrupted == 0, failures, f"{interrupted} full Runs were interrupted")
    require(dropped == 0, failures, f"{dropped} scheduled iterations were dropped")
    require(
        late_arrivals == 0,
        failures,
        (
            f"{late_arrivals} exact arrivals reached or exceeded one "
            f"{arrival_slot_ms:.3f}ms slot of lateness"
        ),
    )
    require(
        max_arrival_lateness is not None
        and p95_arrival_lateness is not None
        and p99_arrival_lateness is not None
        and 0 <= p95_arrival_lateness <= p99_arrival_lateness
        <= max_arrival_lateness < arrival_slot_ms,
        failures,
        (
            "exact-arrival lateness p95/p99/max is "
            f"{p95_arrival_lateness}/{p99_arrival_lateness}/"
            f"{max_arrival_lateness}ms, expected ordered non-negative values "
            f"strictly below one {arrival_slot_ms:.3f}ms slot"
        ),
    )

    closure = observed / accepted if accepted else 0.0
    scheduled_success = (
        succeeded / expected_arrivals if expected_arrivals else 0.0
    )
    throughput = observed / expected_seconds if expected_seconds else 0.0
    require(accepted > 0, failures, "no full Run was accepted")
    require(closure == 1.0, failures, "accepted closure is below 100%")
    if qualification:
        require(
            scheduled == expected_arrivals
            and iterations == expected_arrivals
            and dropped == 0,
            failures,
            (
                "formal exact scheduled arrivals/raw iterations/dropped are "
                f"{scheduled}/{iterations}/{dropped}, expected "
                f"{expected_arrivals}/{expected_arrivals}/0"
            ),
        )
        require(
            scheduled_success >= 0.999,
            failures,
            f"scheduled success is {scheduled_success:.6f}, below 0.999",
        )
        require(
            throughput >= 9.0,
            failures,
            f"completed throughput is {throughput:.3f}/s, below 9/s",
        )
        require(
            duration_ms is not None
            and expected_seconds * 1000
            <= duration_ms
            <= (expected_seconds + 30) * 1000,
            failures,
            (
                f"k6 actual duration is {duration_ms}ms; expected "
                f"{expected_seconds}s plus at most 30s graceful completion"
            ),
        )
    return {
        "iterations": iterations,
        "scheduled_arrivals": scheduled,
        "late_arrivals": late_arrivals,
        "arrival_slot_ms": arrival_slot_ms,
        "p95_arrival_lateness_ms": p95_arrival_lateness,
        "p99_arrival_lateness_ms": p99_arrival_lateness,
        "max_arrival_lateness_ms": max_arrival_lateness,
        "dropped_iterations": dropped,
        "accepted": accepted,
        "terminal_observed": observed,
        "succeeded": succeeded,
        "failed": failed,
        "interrupted": interrupted,
        "rejected": rejected,
        "accepted_closure": closure,
        "scheduled_success": scheduled_success,
        "completed_throughput_per_second": throughput,
        "actual_duration_ms": duration_ms,
        "lifecycle_p95_ms": lifecycle_p95,
        "lifecycle_p99_ms": lifecycle_p99,
    }


def validate_durability(
    before: Dict[str, Any],
    after: Dict[str, Any],
    qualification: bool,
    failures: List[str],
) -> None:
    expected = {
        "fsync": {"on"},
        "full_page_writes": {"on"},
        "synchronous_commit": {"on", "remote_apply"},
        "track_io_timing": {"on"},
        "pg_stat_statements_track": {"all"},
        "pg_stat_statements_track_utility": {"on"},
    }
    for label, snapshot in (("before", before), ("after", after)):
        settings = snapshot.get("settings")
        if not isinstance(settings, dict):
            failures.append(f"{label} PostgreSQL settings are missing")
            continue
        for field, accepted in expected.items():
            require(
                settings.get(field) in accepted,
                failures,
                (
                    f"{label} PostgreSQL {field} is {settings.get(field)!r}, "
                    f"expected one of {sorted(accepted)}"
                ),
            )
        keep_bytes = nonnegative_int(
            settings.get("wal_keep_size_bytes"),
            f"{label} wal_keep_size_bytes",
            failures,
        )
        if qualification:
            require(
                keep_bytes is not None and keep_bytes >= 8589934592,
                failures,
                f"{label} wal_keep_size is below the required 8GiB",
            )


def validate_snapshot_counters(
    before: Dict[str, Any],
    after: Dict[str, Any],
    failures: List[str],
) -> Dict[str, Dict[str, float]]:
    result: Dict[str, Dict[str, float]] = {}
    for section, fields in MONOTONIC_FIELDS.items():
        before_section = before.get(section)
        after_section = after.get(section)
        if not isinstance(before_section, dict) or not isinstance(after_section, dict):
            failures.append(f"PostgreSQL {section} snapshot is missing")
            continue
        before_reset = before_section.get("stats_reset")
        after_reset = after_section.get("stats_reset")
        if section == "io":
            reset_present = (
                isinstance(before_reset, list)
                and bool(before_reset)
                and all(isinstance(item, str) and item for item in before_reset)
            )
        else:
            reset_present = isinstance(before_reset, str) and bool(before_reset)
        require(
            reset_present and before_reset == after_reset,
            failures,
            f"PostgreSQL {section}.stats_reset is missing or changed",
        )
        section_delta: Dict[str, float] = {}
        for field in fields:
            left = finite_float(
                before_section.get(field),
                f"before {section}.{field}",
                failures,
            )
            right = finite_float(
                after_section.get(field),
                f"after {section}.{field}",
                failures,
            )
            observed = (
                right - left
                if left is not None and right is not None
                else -1.0
            )
            require(
                observed >= 0,
                failures,
                f"PostgreSQL {section}.{field} delta is negative",
            )
            section_delta[field] = max(observed, 0.0)
        result[section] = section_delta
    return result


def validate_statement_attribution(
    before: Dict[str, Any],
    after: Dict[str, Any],
    top_wal_path: str,
    total_wal_bytes: int,
    failures: List[str],
) -> Dict[str, Any]:
    before_stats = before.get("statement_stats")
    after_stats = after.get("statement_stats")
    if not isinstance(before_stats, dict) or not isinstance(after_stats, dict):
        failures.append("pg_stat_statements boundary metadata is missing")
        return {}
    require(
        isinstance(before_stats.get("stats_reset"), str)
        and before_stats.get("stats_reset") == after_stats.get("stats_reset"),
        failures,
        "pg_stat_statements stats_reset changed",
    )
    before_dealloc = nonnegative_int(
        before_stats.get("dealloc"),
        "before pg_stat_statements dealloc",
        failures,
    )
    after_dealloc = nonnegative_int(
        after_stats.get("dealloc"),
        "after pg_stat_statements dealloc",
        failures,
    )
    require(
        before_dealloc is not None
        and after_dealloc is not None
        and before_dealloc == after_dealloc,
        failures,
        "pg_stat_statements entries were deallocated in the measured interval",
    )

    deltas: Dict[str, float] = {}
    for field in (
        "top_level_wal_bytes",
        "top_level_calls",
        "nested_wal_bytes",
        "nested_calls",
    ):
        left = finite_float(
            before_stats.get(field),
            f"before pg_stat_statements {field}",
            failures,
        )
        right = finite_float(
            after_stats.get(field),
            f"after pg_stat_statements {field}",
            failures,
        )
        observed = right - left if left is not None and right is not None else -1
        require(
            observed >= 0,
            failures,
            f"pg_stat_statements {field} delta is negative",
        )
        deltas[field] = max(observed, 0.0)

    embedded = after.get("top_wal_statements")
    if not isinstance(embedded, list):
        failures.append("embedded top-level top-30 WAL statements are missing")
        embedded = []
    require(
        len(embedded) <= 30,
        failures,
        f"embedded top-WAL statement count is {len(embedded)}, above 30",
    )
    embedded_wal = 0.0
    for index, row in enumerate(embedded):
        if not isinstance(row, dict):
            failures.append(f"embedded top-WAL row {index} is not an object")
            continue
        require(
            row.get("toplevel") is True,
            failures,
            f"embedded top-WAL row {index} is not top-level",
        )
        value = finite_float(
            row.get("wal_bytes"),
            f"embedded top-WAL row {index}.wal_bytes",
            failures,
        )
        if value is not None:
            require(
                value >= 0,
                failures,
                f"embedded top-WAL row {index} has negative WAL",
            )
            embedded_wal += max(value, 0.0)

    expected_header = [
        "queryid",
        "toplevel",
        "calls",
        "rows",
        "total_exec_ms",
        "mean_exec_ms",
        "shared_blks_hit",
        "shared_blks_read",
        "temp_blks_read",
        "temp_blks_written",
        "wal_records",
        "wal_fpi",
        "wal_bytes",
        "query",
    ]
    with Path(top_wal_path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        require(
            reader.fieldnames == expected_header,
            failures,
            "derived top-WAL CSV header is not the closed evidence schema",
        )
        rows = list(reader)
    csv_wal = 0.0
    for index, row in enumerate(rows):
        require(
            row.get("toplevel") == "true",
            failures,
            f"derived top-WAL CSV row {index} is not top-level",
        )
        value = finite_float(
            row.get("wal_bytes"),
            f"derived top-WAL CSV row {index}.wal_bytes",
            failures,
        )
        if value is not None:
            csv_wal += max(value, 0.0)
    require(
        len(rows) == len(embedded),
        failures,
        "derived top-WAL CSV row count differs from the embedded snapshot",
    )
    for index, (csv_row, embedded_row) in enumerate(zip(rows, embedded)):
        if not isinstance(embedded_row, dict):
            continue
        require(
            csv_row.get("query") == embedded_row.get("query"),
            failures,
            f"derived top-WAL CSV query differs at row {index}",
        )
        for field in ("calls", "rows", "wal_records", "wal_fpi", "wal_bytes"):
            csv_value = finite_float(
                csv_row.get(field),
                f"derived top-WAL CSV row {index}.{field}",
                failures,
            )
            embedded_value = finite_float(
                embedded_row.get(field),
                f"embedded top-WAL row {index}.{field}",
                failures,
            )
            require(
                csv_value is not None
                and embedded_value is not None
                and math.isclose(
                    csv_value,
                    embedded_value,
                    rel_tol=0,
                    abs_tol=0.0005,
                ),
                failures,
                (
                    f"derived top-WAL CSV {field} differs from the "
                    f"embedded row at index {index}"
                ),
            )
    require(
        math.isclose(csv_wal, embedded_wal, rel_tol=0, abs_tol=0.5),
        failures,
        "derived top-WAL CSV bytes differ from the embedded snapshot",
    )
    all_top_level = deltas["top_level_wal_bytes"]
    require(
        all_top_level > 0,
        failures,
        "all top-level pg_stat_statements WAL is zero",
    )
    require(
        embedded_wal <= all_top_level + 0.5,
        failures,
        "top-30 WAL exceeds all top-level statement WAL",
    )
    return {
        "stats_reset": before_stats.get("stats_reset"),
        "dealloc": before_dealloc,
        "top_level": {
            "top30_row_count": len(embedded),
            "top30_wal_bytes": embedded_wal,
            "all_wal_bytes": all_top_level,
            "all_calls": deltas["top_level_calls"],
            "top30_to_all_ratio": (
                embedded_wal / all_top_level if all_top_level else 0.0
            ),
            "top30_to_pg_stat_wal_ratio": (
                embedded_wal / total_wal_bytes if total_wal_bytes else 0.0
            ),
            "all_to_pg_stat_wal_ratio": (
                all_top_level / total_wal_bytes if total_wal_bytes else 0.0
            ),
        },
        "nested_diagnostic_not_added_to_top_level": {
            "wal_bytes": deltas["nested_wal_bytes"],
            "calls": deltas["nested_calls"],
        },
    }


def validate_boundaries(
    before: Dict[str, Any],
    after: Dict[str, Any],
    failures: List[str],
) -> Tuple[Optional[int], Optional[int]]:
    left = before.get("boundary")
    right = after.get("boundary")
    if not isinstance(left, dict) or not isinstance(right, dict):
        failures.append("PostgreSQL exact WAL boundary is missing")
        return None, None
    require(
        isinstance(left.get("postmaster_start_time"), str)
        and left.get("postmaster_start_time") == right.get("postmaster_start_time"),
        failures,
        "PostgreSQL postmaster changed across Phase 0",
    )
    start = parse_lsn(left.get("wal_insert_lsn"), "before WAL LSN", failures)
    end = parse_lsn(right.get("wal_insert_lsn"), "after WAL LSN", failures)
    require(
        start is not None and end is not None and end > start,
        failures,
        "Phase 0 after WAL LSN is not greater than before",
    )
    return start, end


def sum_evidence_rows(
    rows: List[Any],
    fields: Tuple[str, ...],
    label: str,
    failures: List[str],
) -> Dict[str, int]:
    totals = {field: 0 for field in fields}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            failures.append(f"{label} row {index} is not an object")
            continue
        for field in fields:
            value = nonnegative_int(
                row.get(field),
                f"{label} row {index}.{field}",
                failures,
            )
            if value is not None:
                totals[field] += value
    return totals


def validate_physical_wal(
    physical: Dict[str, Any],
    start_lsn: Optional[int],
    end_lsn: Optional[int],
    before: Dict[str, Any],
    after: Dict[str, Any],
    total_wal_bytes: int,
    failures: List[str],
) -> Dict[str, Any]:
    require(
        physical.get("extension") == "pg_walinspect"
        and isinstance(physical.get("extension_version"), str)
        and bool(physical.get("extension_version")),
        failures,
        "pg_walinspect identity/version is missing",
    )
    before_lsn = before.get("boundary", {}).get("wal_insert_lsn")
    after_lsn = after.get("boundary", {}).get("wal_insert_lsn")
    require(
        physical.get("start_lsn") == before_lsn
        and physical.get("end_lsn") == after_lsn,
        failures,
        "pg_walinspect LSN interval differs from snapshot boundaries",
    )
    lsn_span = nonnegative_int(
        physical.get("lsn_span_bytes"),
        "physical WAL lsn_span_bytes",
        failures,
    )
    if start_lsn is not None and end_lsn is not None:
        require(
            lsn_span == end_lsn - start_lsn,
            failures,
            "physical WAL LSN byte span is inconsistent",
        )

    total_fields = (
        "record_count",
        "record_length_bytes",
        "main_data_length_bytes",
        "fpi_length_bytes",
    )
    totals_document = physical.get("totals")
    if not isinstance(totals_document, dict):
        failures.append("physical WAL totals are missing")
        totals_document = {}
    totals = {
        field: nonnegative_int(
            totals_document.get(field),
            f"physical WAL totals.{field}",
            failures,
        )
        or 0
        for field in total_fields
    }
    require(
        totals["record_count"] > 0 and totals["record_length_bytes"] > 0,
        failures,
        "pg_walinspect returned no physical records",
    )

    groups = physical.get("groups")
    if not isinstance(groups, list):
        failures.append("physical resource-manager/record-type groups are missing")
        groups = []
    group_sums = sum_evidence_rows(
        groups,
        total_fields,
        "physical group",
        failures,
    )
    require(
        group_sums == totals,
        failures,
        "physical resource-manager/record-type groups do not sum to totals",
    )

    category_rows = physical.get("categories")
    if not isinstance(category_rows, list):
        failures.append("physical payload/structural categories are missing")
        category_rows = []
    categories: Dict[str, Dict[str, int]] = {}
    for index, row in enumerate(category_rows):
        if not isinstance(row, dict) or not isinstance(row.get("category"), str):
            failures.append(f"physical category row {index} is invalid")
            continue
        name = row["category"]
        require(
            name not in categories,
            failures,
            f"physical category {name} is duplicated",
        )
        categories[name] = {
            field: nonnegative_int(
                row.get(field),
                f"physical category {name}.{field}",
                failures,
            )
            or 0
            for field in total_fields
        }
    require(
        set(categories) == PHYSICAL_CATEGORIES,
        failures,
        (
            "physical categories are not the exact closed set: "
            f"{sorted(categories)}"
        ),
    )
    category_sums = {
        field: sum(values[field] for values in categories.values())
        for field in total_fields
    }
    require(
        category_sums == totals,
        failures,
        "physical payload/structural categories do not sum to totals",
    )

    physical_bytes = totals["record_length_bytes"]
    payload_bytes = categories.get("payload", {}).get("record_length_bytes", 0)
    artifact_bytes = categories.get(
        "artifact_object_metadata",
        {},
    ).get("record_length_bytes", 0)
    structural_bytes = categories.get(
        "structural",
        {},
    ).get("record_length_bytes", 0)
    mixed_bytes = categories.get("mixed", {}).get("record_length_bytes", 0)
    unmapped_bytes = categories.get("unmapped", {}).get("record_length_bytes", 0)
    unresolved_bytes = mixed_bytes + unmapped_bytes
    explained_coverage = (
        (physical_bytes - unresolved_bytes) / physical_bytes
        if physical_bytes
        else 0.0
    )
    pg_stat_wal_coverage = (
        physical_bytes / total_wal_bytes if total_wal_bytes else 0.0
    )
    lsn_coverage = physical_bytes / lsn_span if lsn_span else 0.0
    require(
        0.95 <= pg_stat_wal_coverage <= 1.05,
        failures,
        (
            "exact-LSN physical record bytes cover "
            f"{pg_stat_wal_coverage:.6f} of pg_stat_wal; expected 0.95..1.05"
        ),
    )
    require(
        0.95 <= lsn_coverage <= 1.05,
        failures,
        (
            f"physical record bytes cover {lsn_coverage:.6f} of the exact "
            "LSN span; expected 0.95..1.05"
        ),
    )
    require(
        explained_coverage >= 0.95,
        failures,
        (
            "payload/object vs structural record attribution covers "
            f"{explained_coverage:.6f}; expected at least 0.95"
        ),
    )
    require(
        payload_bytes > 0,
        failures,
        "fixed full action_demo produced no payload-relation physical WAL",
    )
    require(
        structural_bytes > 0,
        failures,
        "fixed full action_demo produced no structural physical WAL",
    )
    relation_block_groups = physical.get("relation_block_groups")
    require(
        isinstance(relation_block_groups, list)
        and bool(relation_block_groups)
        and any(
            isinstance(row, dict)
            and row.get("category") == "payload"
            and row.get("schema_name") == "public"
            and row.get("relation_name") == "payloads"
            for row in relation_block_groups
        ),
        failures,
        "physical relation-block attribution does not identify public.payloads",
    )
    return {
        "extension": physical.get("extension"),
        "extension_version": physical.get("extension_version"),
        "start_lsn": physical.get("start_lsn"),
        "end_lsn": physical.get("end_lsn"),
        "lsn_span_bytes": lsn_span,
        "record_totals": totals,
        "record_group_count": len(groups),
        "relation_block_group_count": len(
            relation_block_groups if isinstance(relation_block_groups, list) else []
        ),
        "coverage": {
            "record_bytes_to_pg_stat_wal": pg_stat_wal_coverage,
            "record_bytes_to_lsn_span": lsn_coverage,
            "classified_payload_or_structural": explained_coverage,
            "required_range_for_physical_boundaries": [0.95, 1.05],
            "required_minimum_classification": 0.95,
        },
        "wal_split": {
            "payload_relation_wal_bytes": payload_bytes,
            "artifact_object_metadata_wal_bytes": artifact_bytes,
            "payload_object_wal_bytes": payload_bytes + artifact_bytes,
            "structural_wal_bytes": structural_bytes,
            "mixed_unresolved_wal_bytes": mixed_bytes,
            "unmapped_wal_bytes": unmapped_bytes,
        },
        "categories": categories,
    }


def relation_map(
    document: Dict[str, Any],
    key: str,
    identity: str,
    failures: List[str],
) -> Dict[str, Dict[str, Any]]:
    rows = document.get(key)
    if not isinstance(rows, list):
        failures.append(f"relation snapshot {key} is missing")
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or not isinstance(row.get(identity), str):
            failures.append(f"relation snapshot {key} row {index} is invalid")
            continue
        name = row[identity]
        require(name not in result, failures, f"duplicate {key} identity {name}")
        result[name] = row
    return result


def validate_relation_snapshots(
    before: Dict[str, Any],
    after: Dict[str, Any],
    accepted: int,
    failures: List[str],
) -> Dict[str, Any]:
    before_tables = relation_map(before, "tables", "table_name", failures)
    after_tables = relation_map(after, "tables", "table_name", failures)
    before_indexes = relation_map(before, "indexes", "index_name", failures)
    after_indexes = relation_map(after, "indexes", "index_name", failures)
    require(
        set(before_tables) == set(after_tables),
        failures,
        "table identity set changed across Phase 0",
    )
    require(
        set(before_indexes) == set(after_indexes),
        failures,
        "index identity set changed across Phase 0",
    )

    table_deltas: Dict[str, Dict[str, Any]] = {}
    category_growth = {
        category: {
            "heap_main_bytes": 0,
            "table_and_auxiliary_bytes": 0,
            "indexes_bytes": 0,
            "total_bytes": 0,
        }
        for category in RELATION_CATEGORIES
    }
    size_fields = (
        "heap_main_bytes",
        "table_and_auxiliary_bytes",
        "indexes_bytes",
        "total_bytes",
    )
    for name in sorted(set(before_tables) & set(after_tables)):
        left = before_tables[name]
        right = after_tables[name]
        require(
            left.get("category") == right.get("category")
            and left.get("category") in RELATION_CATEGORIES,
            failures,
            f"table {name} category changed or is unknown",
        )
        require(
            left.get("persistence") == "p" and right.get("persistence") == "p",
            failures,
            f"table {name} is not permanent LOGGED",
        )
        deltas: Dict[str, int] = {}
        for field in size_fields:
            left_value = nonnegative_int(
                left.get(field),
                f"before table {name}.{field}",
                failures,
            )
            right_value = nonnegative_int(
                right.get(field),
                f"after table {name}.{field}",
                failures,
            )
            delta = (
                right_value - left_value
                if left_value is not None and right_value is not None
                else -1
            )
            require(delta >= 0, failures, f"table {name}.{field} shrank")
            deltas[field] = max(delta, 0)
        category = left.get("category")
        if category in category_growth:
            for field in size_fields:
                category_growth[category][field] += deltas[field]
        table_deltas[name] = {
            "category": category,
            **deltas,
        }

    index_deltas: Dict[str, Dict[str, Any]] = {}
    for name in sorted(set(before_indexes) & set(after_indexes)):
        left = before_indexes[name]
        right = after_indexes[name]
        require(
            left.get("table_name") == right.get("table_name")
            and left.get("category") == right.get("category"),
            failures,
            f"index {name} ownership/category changed",
        )
        left_bytes = nonnegative_int(
            left.get("bytes"),
            f"before index {name}.bytes",
            failures,
        )
        right_bytes = nonnegative_int(
            right.get("bytes"),
            f"after index {name}.bytes",
            failures,
        )
        delta = (
            right_bytes - left_bytes
            if left_bytes is not None and right_bytes is not None
            else -1
        )
        require(delta >= 0, failures, f"index {name} shrank")
        index_deltas[name] = {
            "table_name": left.get("table_name"),
            "category": left.get("category"),
            "bytes": max(delta, 0),
        }

    before_rows = before.get("row_counts")
    after_rows = after.get("row_counts")
    if not isinstance(before_rows, dict) or not isinstance(after_rows, dict):
        failures.append("exact relation row counts are missing")
        before_rows = {}
        after_rows = {}
    require(
        set(before_rows) == set(after_rows),
        failures,
        "row-count table set changed across Phase 0",
    )
    row_deltas: Dict[str, int] = {}
    for name in sorted(set(before_rows) & set(after_rows)):
        left = nonnegative_int(
            before_rows[name],
            f"before row count {name}",
            failures,
        )
        right = nonnegative_int(
            after_rows[name],
            f"after row count {name}",
            failures,
        )
        delta = right - left if left is not None and right is not None else -1
        require(delta >= 0, failures, f"row count for {name} decreased")
        row_deltas[name] = max(delta, 0)

    require(
        row_deltas.get("workflow_runs") == accepted,
        failures,
        (
            "workflow_runs growth does not equal accepted full Runs: "
            f"{row_deltas.get('workflow_runs')}/{accepted}"
        ),
    )
    for table in sorted(REQUIRED_FULL_LEDGER_GROWTH - {"workflow_runs"}):
        require(
            row_deltas.get(table, 0) > 0,
            failures,
            f"required full durable ledger {table} did not grow",
        )

    ranked_tables = sorted(
        table_deltas.items(),
        key=lambda item: (-item[1]["total_bytes"], item[0]),
    )
    ranked_indexes = sorted(
        index_deltas.items(),
        key=lambda item: (-item[1]["bytes"], item[0]),
    )
    return {
        "row_delta": row_deltas,
        "required_full_ledger_growth": {
            name: row_deltas.get(name)
            for name in sorted(REQUIRED_FULL_LEDGER_GROWTH)
        },
        "category_growth_bytes": category_growth,
        "table_growth": [
            {"table_name": name, **values} for name, values in ranked_tables
        ],
        "index_growth": [
            {"index_name": name, **values} for name, values in ranked_indexes
        ],
    }


def read_artifact_bytes(path: str, label: str, failures: List[str]) -> int:
    raw = Path(path).read_text(encoding="utf-8").strip()
    parsed = nonnegative_int(raw, label, failures)
    return parsed if parsed is not None else 0


def validate_topology(
    before_path: str,
    after_path: str,
    pod_before_path: str,
    pod_after_path: str,
    failures: List[str],
) -> Dict[str, Any]:
    before = load_json(before_path)
    after = load_json(after_path)
    pods_before = before.get("pods")
    pods_after = after.get("pods")
    if not isinstance(pods_before, list) or not isinstance(pods_after, list):
        failures.append("runtime topology Pod sets are missing")
        return {"before": before, "after": after}

    def selected(
        document: Dict[str, Any],
        pods: List[Any],
        label: str,
    ) -> List[Dict[str, Any]]:
        require(
            document.get("desired_replicas") == 1
            and document.get("ready_replicas") == 1,
            failures,
            f"{label} runtime topology is not exact 1 desired/ready",
        )
        ready = [
            item
            for item in pods
            if isinstance(item, dict)
            and item.get("phase") == "Running"
            and item.get("ready") is True
            and item.get("deleting") is not True
        ]
        require(
            len(pods) == 1 and len(ready) == 1,
            failures,
            f"{label} runtime topology is not one stable Ready Pod",
        )
        return ready

    ready_before = selected(before, pods_before, "before")
    ready_after = selected(after, pods_after, "after")
    uid_before = ready_before[0].get("uid") if len(ready_before) == 1 else None
    uid_after = ready_after[0].get("uid") if len(ready_after) == 1 else None
    restart_before = (
        ready_before[0].get("restart_count") if len(ready_before) == 1 else None
    )
    restart_after = (
        ready_after[0].get("restart_count") if len(ready_after) == 1 else None
    )
    require(
        isinstance(uid_before, str)
        and uid_before == uid_after
        and restart_before == restart_after,
        failures,
        "runtime Pod UID or restart count changed across Phase 0",
    )

    pod_before = load_json(pod_before_path)
    pod_after = load_json(pod_after_path)
    if pod_before.get("local") is True or pod_after.get("local") is True:
        require(
            pod_before.get("local") is True
            and pod_after.get("local") is True
            and pod_before.get("pid") == pod_after.get("pid"),
            failures,
            "local runtime PID changed across Phase 0",
        )
    else:
        require(
            pod_before.get("metadata", {}).get("uid") == uid_before
            and pod_after.get("metadata", {}).get("uid") == uid_after,
            failures,
            "runtime Pod evidence does not match topology evidence",
        )
    return {
        "unique_runtime_uid": uid_before,
        "restart_count_before": restart_before,
        "restart_count_after": restart_after,
    }


def validate_preflight(
    infrastructure_path: Optional[str],
    database_path: Optional[str],
    statistics_reset_path: Optional[str],
    qualification: bool,
    failures: List[str],
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "infrastructure": None,
        "database": None,
        "statistics_reset": None,
    }
    if infrastructure_path is not None:
        result["infrastructure"] = load_json(infrastructure_path)
    if database_path is not None:
        result["database"] = load_json(database_path)
    if statistics_reset_path is not None:
        result["statistics_reset"] = load_json(statistics_reset_path)
    if qualification:
        require(
            isinstance(result["infrastructure"], dict)
            and result["infrastructure"].get("passed") is True,
            failures,
            "formal Phase 0 fresh infrastructure evidence is missing/failed",
        )
        require(
            isinstance(result["database"], dict)
            and result["database"].get("passed") is True,
            failures,
            "formal Phase 0 fresh full-database evidence is missing/failed",
        )
        require(
            isinstance(result["statistics_reset"], dict)
            and result["statistics_reset"].get("passed") is True
            and isinstance(
                result["statistics_reset"].get(
                    "database_stats_reset_after"
                ),
                str,
            )
            and bool(
                result["statistics_reset"].get(
                    "database_stats_reset_after"
                )
            ),
            failures,
            "formal Phase 0 database statistics-reset evidence is missing/failed",
        )
        infrastructure = result["infrastructure"]
        if isinstance(infrastructure, dict):
            require(
                infrastructure.get("deployment", {}).get("persistence_mode")
                == "full"
                and infrastructure.get("storage", {}).get("postgres_pvc_size")
                == "24Gi"
                and infrastructure.get("storage", {}).get(
                    "postgres_wal_keep_size"
                )
                == "8GB",
                failures,
                "fresh infrastructure does not match Phase 0 capacity profile",
            )
        database = result["database"]
        if isinstance(database, dict):
            require(
                database.get("preexisting_workload_rows") == 0
                and database.get("deployment_policy", {}).get("persistence_mode")
                == "full",
                failures,
                "database preflight does not prove a fresh full deployment",
            )
    return result


def evaluate(args: argparse.Namespace) -> int:
    failures: List[str] = []
    before = load_json(args.before)
    after = load_json(args.after)
    relations_before = load_json(args.relations_before)
    relations_after = load_json(args.relations_after)
    physical = load_json(args.physical_wal)
    warmup_summary = load_json(args.warmup)
    summary = load_json(args.k6)
    profile = load_json(args.profile)

    preflight = validate_preflight(
        args.infrastructure_freshness,
        args.database_preflight,
        args.statistics_reset,
        args.qualification,
        failures,
    )
    profile_values = validate_profile(profile, args.qualification, failures)
    reset_epoch = (
        preflight.get("statistics_reset", {}).get(
            "database_stats_reset_after"
        )
        if isinstance(preflight.get("statistics_reset"), dict)
        else None
    )
    if args.qualification:
        require(
            reset_epoch == before.get("database", {}).get("stats_reset"),
            failures,
            (
                "Phase 0 pg_stat_reset epoch does not match the measured "
                "before database statistics epoch"
            ),
        )
    warmup_expected_arrivals = (
        profile_values["warmup_seconds"] * profile_values["arrival_rate"]
    )
    warmup = validate_k6(
        warmup_summary,
        profile_values["warmup_seconds"],
        warmup_expected_arrivals,
        args.qualification,
        failures,
    )
    workload = validate_k6(
        summary,
        profile_values["expected_seconds"],
        profile_values["expected_arrivals"],
        args.qualification,
        failures,
    )
    validate_durability(before, after, args.qualification, failures)
    snapshot_deltas = validate_snapshot_counters(before, after, failures)
    start_lsn, end_lsn = validate_boundaries(before, after, failures)
    total_wal_bytes = int(snapshot_deltas.get("wal", {}).get("wal_bytes", 0))
    require(total_wal_bytes > 0, failures, "pg_stat_wal delta is zero")
    statement = validate_statement_attribution(
        before,
        after,
        args.top_wal,
        total_wal_bytes,
        failures,
    )
    physical_evidence = validate_physical_wal(
        physical,
        start_lsn,
        end_lsn,
        before,
        after,
        total_wal_bytes,
        failures,
    )
    relation_evidence = validate_relation_snapshots(
        relations_before,
        relations_after,
        workload["accepted"],
        failures,
    )

    artifact_before = read_artifact_bytes(
        args.artifact_before,
        "artifact bytes before",
        failures,
    )
    artifact_after = read_artifact_bytes(
        args.artifact_after,
        "artifact bytes after",
        failures,
    )
    artifact_delta = artifact_after - artifact_before
    require(artifact_delta >= 0, failures, "Artifact store bytes decreased")
    topology = validate_topology(
        args.topology_before,
        args.topology_after,
        args.pod_before,
        args.pod_after,
        failures,
    )

    category_growth = relation_evidence.get("category_growth_bytes", {})
    payload_db_growth = category_growth.get("payload", {}).get("total_bytes", 0)
    artifact_metadata_growth = category_growth.get(
        "artifact_object_metadata",
        {},
    ).get("total_bytes", 0)
    structural_db_growth = category_growth.get(
        "structural",
        {},
    ).get("total_bytes", 0)
    wal_per_accepted = (
        total_wal_bytes / workload["accepted"]
        if workload["accepted"]
        else None
    )
    old_ratio = (
        wal_per_accepted / OLD_FULL_REFERENCE["wal_bytes_per_accepted"]
        if wal_per_accepted is not None
        else None
    )

    report = {
        "phase": "0-full",
        "passed": not failures,
        "qualification": args.qualification,
        "profile": profile,
        "preflight": preflight,
        "warmup": {
            **warmup,
            "expected_arrivals": warmup_expected_arrivals,
            "excluded_from_measured_lsn_interval": True,
            "closed_before_measured_lsn_boundary": (
                warmup["accepted"] == warmup["terminal_observed"]
                and warmup["accepted"] == warmup["succeeded"]
                and warmup["failed"] == 0
                and warmup["interrupted"] == 0
            ),
        },
        "workload": workload,
        "postgresql": {
            "counter_delta": snapshot_deltas,
            "total_wal_bytes": total_wal_bytes,
            "wal_bytes_per_accepted": wal_per_accepted,
            "top_level_statement_attribution": statement,
            "physical_wal_attribution": physical_evidence,
        },
        "storage_growth": {
            "payload_object": {
                "payload_relation_bytes": payload_db_growth,
                "artifact_object_metadata_relation_bytes":
                    artifact_metadata_growth,
                "artifact_store_bytes": max(artifact_delta, 0),
                "combined_bytes": (
                    payload_db_growth
                    + artifact_metadata_growth
                    + max(artifact_delta, 0)
                ),
            },
            "structural": {
                "relation_and_index_bytes": structural_db_growth,
            },
            "catalog": {
                "relation_and_index_bytes": category_growth.get(
                    "catalog",
                    {},
                ).get("total_bytes", 0),
            },
            "relations": relation_evidence,
        },
        "runtime_topology": topology,
        "historical_full_2h_reference": {
            **OLD_FULL_REFERENCE,
            "new_to_old_wal_per_accepted_ratio": old_ratio,
            "note": (
                "The historical 71,033,480,938-byte interval is retained only "
                "as a comparison; this report does not relabel it as >=95% "
                "attributed evidence."
            ),
        },
        "classification_method": {
            "physical_wal": (
                "Each pg_walinspect record in the exact LSN interval is counted "
                "once. Block references map heap/index/TOAST filenodes to their "
                "root public relation; records without block references are "
                "structural. Mixed/unmapped records remain unresolved."
            ),
            "payload_object": (
                "PostgreSQL payload records map to payloads; Artifact metadata "
                "records map to the closed artifact relation set; external "
                "object bytes are the Artifact volume before/after delta."
            ),
            "structural": (
                "All other mapped PostgreSQL records and block-free WAL records "
                "are structural. Nested pg_stat_statements is diagnostic and is "
                "never added to top-level statement WAL."
            ),
        },
        "failures": failures,
    }
    write_report(args.output, report)
    return 0 if not failures else 1


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--before", required=True)
    value.add_argument("--after", required=True)
    value.add_argument("--relations-before", required=True)
    value.add_argument("--relations-after", required=True)
    value.add_argument("--physical-wal", required=True)
    value.add_argument("--top-wal", required=True)
    value.add_argument("--warmup", required=True)
    value.add_argument("--k6", required=True)
    value.add_argument("--profile", required=True)
    value.add_argument("--artifact-before", required=True)
    value.add_argument("--artifact-after", required=True)
    value.add_argument("--pod-before", required=True)
    value.add_argument("--pod-after", required=True)
    value.add_argument("--topology-before", required=True)
    value.add_argument("--topology-after", required=True)
    value.add_argument("--infrastructure-freshness")
    value.add_argument("--database-preflight")
    value.add_argument("--statistics-reset")
    value.add_argument("--qualification", action="store_true")
    value.add_argument("--output", required=True)
    return value


def main() -> int:
    return evaluate(parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
