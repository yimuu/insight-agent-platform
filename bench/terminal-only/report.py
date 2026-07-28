#!/usr/bin/env python3
"""Fail-closed evaluators for terminal-only qualification evidence."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any


REQUIRED_FORBIDDEN_DURABLE_TABLES = {
    "agent_publication_heads",
    "artifact_gc_claims",
    "artifact_gc_sweeps",
    "artifact_retention_releases",
    "artifact_store_authority",
    "artifacts",
    "deployment_revisions",
    "execution_events",
    "full_conversation_turns",
    "payloads",
    "projection_checkpoints",
    "public_event_outbox",
    "workflow_definition_public_metadata",
    "workflow_definition_revisions",
    "workflow_definitions",
    "workflow_retrieval_publications",
    "workflow_runs",
}

MONOTONIC_SNAPSHOT_FIELDS = {
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

REQUIRED_LOGGED_QUALIFICATION_RELATIONS = {
    "terminal_run_admissions",
    "terminal_run_results",
    "terminal_content_deletion_jobs",
    "terminal_artifact_staging",
    "conversations",
    "conversation_messages",
    "conversation_summaries",
    "conversation_tombstones",
    "conversation_summary_jobs",
}


def parse_timestamp(value: Any, field: str, failures: list[str]) -> datetime | None:
    if not isinstance(value, str):
        failures.append(f"{field} is missing or is not a timestamp")
        return None
    # PostgreSQL's json/jsonb timestamp rendering removes trailing fractional
    # zeroes, so clock_timestamp() legitimately emits one through six digits.
    # Python 3.9's fromisoformat accepts only three or six fractional digits.
    # Normalize the PostgreSQL precision without changing the represented time.
    match = re.fullmatch(
        r"(?P<prefix>.+[T ]\d{2}:\d{2}:\d{2})"
        r"\.(?P<fraction>\d{1,6})"
        r"(?P<timezone>Z|[+-]\d{2}:\d{2})?",
        value,
    )
    if match is not None:
        timezone = match.group("timezone") or ""
        value = (
            f"{match.group('prefix')}."
            f"{match.group('fraction').ljust(6, '0')}"
            f"{timezone}"
        )
    try:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        failures.append(f"{field} is not a valid ISO-8601 timestamp")
        return None
    if parsed.tzinfo is None:
        failures.append(f"{field} lacks an explicit timezone offset")
        return None
    return parsed


def parse_wal_lsn(value: Any, field: str, failures: list[str]) -> int | None:
    if not isinstance(value, str) or re.fullmatch(
        r"[0-9A-F]+/[0-9A-F]{1,8}",
        value,
    ) is None:
        failures.append(f"{field} is not a canonical PostgreSQL WAL LSN")
        return None
    high_text, low_text = value.split("/", 1)
    high = int(high_text, 16)
    low = int(low_text, 16)
    if high > 0xFFFFFFFF or low > 0xFFFFFFFF:
        failures.append(f"{field} has an out-of-range WAL component")
        return None
    return (high << 32) + low


def parse_nonnegative_integer(
    value: Any,
    field: str,
    failures: list[str],
) -> int | None:
    if isinstance(value, bool):
        failures.append(f"{field} is not a non-negative integer")
        return None
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            failures.append(f"{field} is not a non-negative integer")
            return None
        parsed = int(value)
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        parsed = int(value)
    else:
        failures.append(f"{field} is not a non-negative integer")
        return None
    if parsed < 0:
        failures.append(f"{field} is not a non-negative integer")
        return None
    return parsed


def validate_durability_snapshot(
    snapshot: dict[str, Any],
    label: str,
    failures: list[str],
) -> None:
    settings = snapshot.get("settings")
    if not isinstance(settings, dict):
        failures.append(f"{label} PostgreSQL settings are missing")
        return
    required_settings = {
        "fsync": {"on"},
        "full_page_writes": {"on"},
        "synchronous_commit": {"on", "remote_apply"},
        "track_io_timing": {"on"},
        "pg_stat_statements_track": {"all"},
        "pg_stat_statements_track_utility": {"on"},
    }
    for name, accepted in required_settings.items():
        observed = settings.get(name)
        require(
            observed in accepted,
            failures,
            (
                f"{label} PostgreSQL setting {name} is {observed!r}, "
                f"expected one of {sorted(accepted)}"
            ),
        )

    persistence = snapshot.get("qualification_relation_persistence")
    if not isinstance(persistence, dict):
        failures.append(f"{label} qualification relation persistence is missing")
        return
    for relation in sorted(REQUIRED_LOGGED_QUALIFICATION_RELATIONS):
        require(
            persistence.get(relation) == "p",
            failures,
            (
                f"{label} relation {relation} is not a permanent LOGGED table: "
                f"{persistence.get(relation)!r}"
            ),
        )
    require(
        persistence.get("terminal_runtime_instances") == "u",
        failures,
        (
            f"{label} terminal_runtime_instances persistence is "
            f"{persistence.get('terminal_runtime_instances')!r}, expected 'u'"
        ),
    )


def validate_statement_boundary(
    before: dict[str, Any],
    after: dict[str, Any],
    failures: list[str],
) -> dict[str, Any]:
    before_stats = before.get("statement_stats")
    after_stats = after.get("statement_stats")
    if not isinstance(before_stats, dict) or not isinstance(after_stats, dict):
        failures.append("pg_stat_statements boundary metadata is missing")
        return {}
    before_reset = before_stats.get("stats_reset")
    after_reset = after_stats.get("stats_reset")
    require(
        isinstance(before_reset, str)
        and isinstance(after_reset, str)
        and before_reset == after_reset,
        failures,
        "pg_stat_statements stats_reset changed across the measured interval",
    )
    before_dealloc = parse_nonnegative_integer(
        before_stats.get("dealloc"),
        "before pg_stat_statements dealloc",
        failures,
    )
    after_dealloc = parse_nonnegative_integer(
        after_stats.get("dealloc"),
        "after pg_stat_statements dealloc",
        failures,
    )
    require(
        before_dealloc is not None
        and after_dealloc is not None
        and after_dealloc == before_dealloc,
        failures,
        (
            "pg_stat_statements entries were deallocated or the dealloc "
            "counter is invalid across the measured interval"
        ),
    )
    statement_deltas: dict[str, float] = {}
    for field in (
        "top_level_wal_bytes",
        "top_level_calls",
        "nested_wal_bytes",
        "nested_calls",
    ):
        try:
            before_value = float(before_stats[field])
            after_value = float(after_stats[field])
        except (KeyError, TypeError, ValueError):
            failures.append(f"pg_stat_statements {field} is missing or invalid")
            before_value = 0.0
            after_value = -1.0
        value_delta = after_value - before_value
        require(
            math.isfinite(before_value)
            and math.isfinite(after_value)
            and before_value >= 0
            and value_delta >= 0,
            failures,
            f"pg_stat_statements {field} delta is negative or non-finite",
        )
        statement_deltas[field] = max(value_delta, 0.0)

    before_boundary = before.get("boundary")
    after_boundary = after.get("boundary")
    if not isinstance(before_boundary, dict) or not isinstance(after_boundary, dict):
        failures.append("PostgreSQL WAL boundary metadata is missing")
        return {}
    before_postmaster = parse_timestamp(
        before_boundary.get("postmaster_start_time"),
        "before boundary postmaster_start_time",
        failures,
    )
    after_postmaster = parse_timestamp(
        after_boundary.get("postmaster_start_time"),
        "after boundary postmaster_start_time",
        failures,
    )
    require(
        before_postmaster is not None
        and after_postmaster is not None
        and before_postmaster == after_postmaster,
        failures,
        "PostgreSQL postmaster identity changed across the measured interval",
    )
    before_lsn = parse_wal_lsn(
        before_boundary.get("wal_insert_lsn"),
        "before boundary wal_insert_lsn",
        failures,
    )
    after_lsn = parse_wal_lsn(
        after_boundary.get("wal_insert_lsn"),
        "after boundary wal_insert_lsn",
        failures,
    )
    require(
        before_lsn is not None
        and after_lsn is not None
        and after_lsn > before_lsn,
        failures,
        "after WAL insert LSN must be strictly greater than before WAL insert LSN",
    )
    before_statement = parse_timestamp(
        before_boundary.get("statement_timestamp"),
        "before boundary statement_timestamp",
        failures,
    )
    after_statement = parse_timestamp(
        after_boundary.get("statement_timestamp"),
        "after boundary statement_timestamp",
        failures,
    )
    before_captured = parse_timestamp(
        before.get("captured_at"),
        "before captured_at",
        failures,
    )
    after_captured = parse_timestamp(
        after.get("captured_at"),
        "after captured_at",
        failures,
    )
    require(
        before_statement is not None
        and before_captured is not None
        and after_statement is not None
        and after_captured is not None
        and before_statement <= before_captured
        and before_captured < after_statement
        and after_statement <= after_captured,
        failures,
        (
            "boundary timestamps must satisfy before.statement <= "
            "before.captured < after.statement <= after.captured"
        ),
    )
    return {
        "statement_stats_reset": before_reset,
        "statement_dealloc": before_dealloc,
        "top_level_statement_wal_bytes_delta": statement_deltas[
            "top_level_wal_bytes"
        ],
        "top_level_statement_calls_delta": statement_deltas["top_level_calls"],
        "nested_statement_wal_bytes_delta": statement_deltas["nested_wal_bytes"],
        "nested_statement_calls_delta": statement_deltas["nested_calls"],
        "before": before_boundary,
        "after": after_boundary,
    }


def load_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def k6_metric(
    summary: dict[str, Any],
    name: str,
    field: str,
    failures: list[str],
    *,
    absent_is_zero: bool = False,
) -> float:
    metrics = summary.get("metrics")
    values = metrics.get(name, {}).get("values") if isinstance(metrics, dict) else None
    if not isinstance(values, dict) or field not in values:
        if absent_is_zero:
            return 0.0
        failures.append(f"required k6 metric {name}.{field} is missing")
        return 0.0
    try:
        value = float(values[field])
    except (TypeError, ValueError):
        failures.append(f"k6 metric {name}.{field} is not numeric")
        return 0.0
    if not math.isfinite(value):
        failures.append(f"k6 metric {name}.{field} is not finite")
        return 0.0
    return value


def k6_count(
    summary: dict[str, Any],
    name: str,
    failures: list[str],
    *,
    absent_is_zero: bool = False,
) -> int:
    value = k6_metric(
        summary,
        name,
        "count",
        failures,
        absent_is_zero=absent_is_zero,
    )
    if value < 0 or not value.is_integer():
        failures.append(f"k6 metric {name}.count must be a non-negative integer")
        return 0
    return int(value)


def delta(before: dict[str, Any], after: dict[str, Any], section: str, key: str) -> float:
    return float(after[section][key]) - float(before[section][key])


def validate_snapshot_pair(
    before: dict[str, Any],
    after: dict[str, Any],
    failures: list[str],
) -> dict[str, dict[str, float]]:
    before_captured = parse_timestamp(
        before.get("captured_at"), "before captured_at", failures
    )
    after_captured = parse_timestamp(
        after.get("captured_at"), "after captured_at", failures
    )
    require(
        before_captured is not None
        and after_captured is not None
        and after_captured > before_captured,
        failures,
        "PostgreSQL snapshot timestamps are missing or not increasing",
    )
    validate_durability_snapshot(before, "before", failures)
    validate_durability_snapshot(after, "after", failures)
    for section in ("wal", "bgwriter", "database", "io"):
        before_reset = before.get(section, {}).get("stats_reset")
        after_reset = after.get(section, {}).get("stats_reset")
        require(
            before_reset is not None
            and after_reset is not None
            and before_reset == after_reset,
            failures,
            f"{section} stats_reset changed or is missing",
        )

    observed: dict[str, dict[str, float]] = {}
    for section, fields in MONOTONIC_SNAPSHOT_FIELDS.items():
        observed[section] = {}
        for field in fields:
            try:
                value = delta(before, after, section, field)
            except (KeyError, TypeError, ValueError):
                failures.append(f"{section}.{field} delta cannot be calculated")
                value = 0.0
            require(
                math.isfinite(value) and value >= 0,
                failures,
                f"{section}.{field} delta is negative or non-finite: {value}",
            )
            observed[section][field] = value
    return observed


def validate_forbidden_durable_rows(
    before: dict[str, Any],
    after: dict[str, Any],
    failures: list[str],
) -> dict[str, int]:
    before_rows = before.get("forbidden_durable_rows")
    after_rows = after.get("forbidden_durable_rows")
    if not isinstance(before_rows, dict) or not isinstance(after_rows, dict):
        failures.append("complete forbidden durable table snapshots are missing")
        return {}
    before_names = set(before_rows)
    after_names = set(after_rows)
    require(
        before_names == after_names,
        failures,
        "forbidden durable table set changed between snapshots",
    )
    missing_required = sorted(REQUIRED_FORBIDDEN_DURABLE_TABLES - before_names)
    require(
        not missing_required,
        failures,
        f"forbidden durable table coverage is incomplete: {missing_required}",
    )
    deltas: dict[str, int] = {}
    for name in sorted(before_names | after_names):
        try:
            value = int(after_rows.get(name, 0)) - int(before_rows.get(name, 0))
        except (TypeError, ValueError):
            failures.append(f"forbidden durable table {name} has a non-integer row count")
            value = 0
        require(value >= 0, failures, f"forbidden durable table {name} delta is negative")
        require(value == 0, failures, f"forbidden durable table {name} changed by {value}")
        deltas[name] = value
    return deltas


def top_wal_accounting(
    path: str,
    total_wal_bytes: int,
    failures: list[str],
    top_level_statement_wal_bytes: float,
) -> dict[str, Any]:
    accounted = 0.0
    rows = 0
    try:
        with Path(path).open(encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            required_columns = {"toplevel", "wal_bytes"}
            if reader.fieldnames is None or not required_columns.issubset(
                reader.fieldnames
            ):
                failures.append(
                    "top-WAL CSV is missing the toplevel or wal_bytes column"
                )
            else:
                for index, row in enumerate(reader, start=2):
                    if row.get("toplevel") not in ("true", "t", "True"):
                        failures.append(
                            f"top-WAL CSV row {index} is not a top-level statement"
                        )
                        continue
                    raw_value = row.get("wal_bytes")
                    try:
                        value = float(raw_value) if raw_value not in (None, "") else 0.0
                    except ValueError:
                        failures.append(f"top-WAL CSV row {index} has invalid wal_bytes")
                        continue
                    if not math.isfinite(value) or value < 0:
                        failures.append(
                            f"top-WAL CSV row {index} has negative or non-finite wal_bytes"
                        )
                        continue
                    accounted += value
                    rows += 1
    except OSError as error:
        failures.append(f"top-WAL CSV cannot be read: {error}")
    require(
        rows <= 30,
        failures,
        f"top-WAL CSV contains {rows} rows, expected at most 30",
    )

    top_level = float(top_level_statement_wal_bytes)
    if not math.isfinite(top_level) or top_level < 0:
        failures.append(
            "aggregate top-level pg_stat_statements WAL is negative or non-finite"
        )
        top_level = 0.0
    require(
        top_level + 0.5 >= accounted,
        failures,
        (
            "top-30 pg_stat_statements WAL exceeds the all-statement "
            f"top-level aggregate: {accounted} vs {top_level}"
        ),
    )
    raw_top30_ratio = (
        accounted / total_wal_bytes if total_wal_bytes > 0 else 0.0
    )
    top_level_ratio = (
        top_level / total_wal_bytes if total_wal_bytes > 0 else 0.0
    )
    top30_top_level_coverage = accounted / top_level if top_level > 0 else 0.0
    return {
        "top_statement_rows": rows,
        "top_statement_wal_bytes": accounted,
        "top_level_statement_wal_bytes": top_level,
        "other_top_level_statement_wal_bytes": max(top_level - accounted, 0.0),
        "total_interval_wal_bytes": total_wal_bytes,
        "raw_top30_to_total_ratio": raw_top30_ratio,
        "raw_top_level_to_total_ratio": top_level_ratio,
        "top30_top_level_coverage": top30_top_level_coverage,
        "positive_residual_wal_bytes": max(
            float(total_wal_bytes) - top_level, 0.0
        ),
        "over_attributed_wal_bytes": max(
            top_level - float(total_wal_bytes),
            0.0,
        ),
    }


def validate_maintenance_stats(
    before: dict[str, Any],
    after: dict[str, Any],
    failures: list[str],
) -> dict[str, Any]:
    """Validate maintenance counters as correlation-only diagnostics.

    PostgreSQL exposes no per-autovacuum WAL byte counter. These observations
    therefore never allocate, explain, or otherwise claim residual WAL bytes.
    """

    before_stats = before.get("maintenance_stats")
    after_stats = after.get("maintenance_stats")
    if not isinstance(before_stats, dict) or not isinstance(after_stats, dict):
        failures.append("pg_stat_user_tables maintenance snapshots are missing")
        return {
            "stats_epoch_continuous": False,
            "table_identity_continuous": False,
            "counter_deltas_nonnegative": False,
            "timestamps_coherent": False,
            "maintenance_observed": False,
            "correlation_evidence_valid": False,
            "autovacuum_delta": 0,
            "autoanalyze_delta": 0,
            "table_deltas": [],
        }

    before_epoch = before_stats.get("stats_epoch")
    after_epoch = after_stats.get("stats_epoch")
    before_database_epoch = before.get("database", {}).get("stats_reset")
    after_database_epoch = after.get("database", {}).get("stats_reset")
    epoch_values_valid = all(
        parse_timestamp(value, label, failures) is not None
        for value, label in (
            (before_epoch, "before maintenance stats_epoch"),
            (after_epoch, "after maintenance stats_epoch"),
        )
    )
    stats_epoch_continuous = (
        epoch_values_valid
        and before_epoch == after_epoch
        and before_epoch == before_database_epoch
        and after_epoch == after_database_epoch
    )
    require(
        stats_epoch_continuous,
        failures,
        (
            "pg_stat_user_tables database stats epoch changed, is missing, "
            "or differs from pg_stat_database"
        ),
    )

    before_postmaster = before.get("boundary", {}).get("postmaster_start_time")
    after_postmaster = after.get("boundary", {}).get("postmaster_start_time")
    postmaster_identity_continuous = (
        isinstance(before_postmaster, str)
        and before_postmaster == after_postmaster
    )
    require(
        postmaster_identity_continuous,
        failures,
        "maintenance evidence spans different PostgreSQL postmaster identities",
    )

    counters_valid = True
    timestamps_valid = True

    def parse_tables(
        document: dict[str, Any],
        label: str,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        nonlocal counters_valid, timestamps_valid
        raw_tables = document.get("tables")
        if not isinstance(raw_tables, list):
            failures.append(f"{label} pg_stat_user_tables rows are missing")
            counters_valid = False
            timestamps_valid = False
            return {}
        parsed: dict[tuple[str, str], dict[str, Any]] = {}
        for index, raw_row in enumerate(raw_tables):
            if not isinstance(raw_row, dict):
                failures.append(
                    f"{label} pg_stat_user_tables row {index} is not an object"
                )
                counters_valid = False
                timestamps_valid = False
                continue
            schema_name = raw_row.get("schema_name")
            table_name = raw_row.get("table_name")
            if not isinstance(schema_name, str) or not schema_name:
                failures.append(
                    f"{label} pg_stat_user_tables row {index} lacks schema_name"
                )
                counters_valid = False
                continue
            if not isinstance(table_name, str) or not table_name:
                failures.append(
                    f"{label} pg_stat_user_tables row {index} lacks table_name"
                )
                counters_valid = False
                continue
            key = (schema_name, table_name)
            if key in parsed:
                failures.append(
                    f"{label} pg_stat_user_tables contains duplicate {schema_name}.{table_name}"
                )
                counters_valid = False
                continue
            normalized = dict(raw_row)
            for field in (
                "relation_id",
                "autovacuum_count",
                "autoanalyze_count",
            ):
                numeric_value = parse_nonnegative_integer(
                    raw_row.get(field),
                    f"{label} {schema_name}.{table_name} {field}",
                    failures,
                )
                if numeric_value is None:
                    counters_valid = False
                    numeric_value = 0
                if field == "relation_id" and numeric_value == 0:
                    failures.append(
                        f"{label} {schema_name}.{table_name} relation_id "
                        "must be positive"
                    )
                    counters_valid = False
                normalized[field] = numeric_value
            for field in ("last_autovacuum", "last_autoanalyze"):
                raw_timestamp = raw_row.get(field)
                if raw_timestamp is None:
                    normalized[field] = None
                    continue
                timestamp = parse_timestamp(
                    raw_timestamp,
                    f"{label} {schema_name}.{table_name} {field}",
                    failures,
                )
                if timestamp is None:
                    timestamps_valid = False
                normalized[field] = timestamp
            parsed[key] = normalized
        return parsed

    before_tables = parse_tables(before_stats, "before")
    after_tables = parse_tables(after_stats, "after")
    before_names = set(before_tables)
    after_names = set(after_tables)
    table_identity_continuous = before_names == after_names and bool(before_names)
    require(
        table_identity_continuous,
        failures,
        (
            "pg_stat_user_tables relation set changed, is empty, or is missing "
            "across the measured interval"
        ),
    )

    before_captured = parse_timestamp(
        before.get("captured_at"), "before captured_at", failures
    )
    after_captured = parse_timestamp(
        after.get("captured_at"), "after captured_at", failures
    )
    counter_deltas_nonnegative = counters_valid
    timestamp_evidence_complete = timestamps_valid
    autovacuum_delta = 0
    autoanalyze_delta = 0
    table_deltas: list[dict[str, Any]] = []
    for key in sorted(before_names | after_names):
        before_row = before_tables.get(key)
        after_row = after_tables.get(key)
        if before_row is None or after_row is None:
            counter_deltas_nonnegative = False
            timestamp_evidence_complete = False
            continue
        schema_name, table_name = key
        same_relation = before_row["relation_id"] == after_row["relation_id"]
        require(
            same_relation,
            failures,
            (
                f"pg_stat_user_tables identity changed for "
                f"{schema_name}.{table_name}"
            ),
        )
        table_identity_continuous = table_identity_continuous and same_relation
        row_delta: dict[str, Any] = {
            "schema_name": schema_name,
            "table_name": table_name,
            "relation_id": after_row["relation_id"],
        }
        for counter, timestamp_field in (
            ("autovacuum_count", "last_autovacuum"),
            ("autoanalyze_count", "last_autoanalyze"),
        ):
            count_delta = after_row[counter] - before_row[counter]
            if count_delta < 0:
                failures.append(
                    f"{schema_name}.{table_name} {counter} delta is negative: "
                    f"{count_delta}"
                )
                counter_deltas_nonnegative = False
            before_time = before_row[timestamp_field]
            after_time = after_row[timestamp_field]
            timestamp_coherent = True
            if before_time is not None and (
                after_time is None or after_time < before_time
            ):
                timestamp_coherent = False
            if after_time is not None and (
                after_captured is None or after_time > after_captured
            ):
                timestamp_coherent = False
            if before_time is not None and (
                before_captured is None or before_time > before_captured
            ):
                timestamp_coherent = False
            if count_delta > 0 and (
                after_time is None
                or (before_time is not None and after_time <= before_time)
            ):
                timestamp_coherent = False
            if not timestamp_coherent:
                failures.append(
                    f"{schema_name}.{table_name} {counter} delta lacks a "
                    f"coherent {timestamp_field} transition"
                )
                timestamp_evidence_complete = False
            row_delta[counter] = count_delta
            row_delta[f"{timestamp_field}_before"] = (
                before_time.isoformat() if before_time is not None else None
            )
            row_delta[f"{timestamp_field}_after"] = (
                after_time.isoformat() if after_time is not None else None
            )
            if counter == "autovacuum_count":
                autovacuum_delta += count_delta
            else:
                autoanalyze_delta += count_delta
        table_deltas.append(row_delta)

    maintenance_observed = autovacuum_delta > 0 or autoanalyze_delta > 0
    correlation_evidence_valid = (
        stats_epoch_continuous
        and postmaster_identity_continuous
        and table_identity_continuous
        and counter_deltas_nonnegative
        and timestamp_evidence_complete
        and maintenance_observed
    )
    return {
        "stats_epoch_before": before_epoch,
        "stats_epoch_after": after_epoch,
        "stats_epoch_continuous": stats_epoch_continuous,
        "postmaster_identity_continuous": postmaster_identity_continuous,
        "table_identity_continuous": table_identity_continuous,
        "counter_deltas_nonnegative": counter_deltas_nonnegative,
        "timestamps_coherent": timestamp_evidence_complete,
        "maintenance_observed": maintenance_observed,
        "correlation_evidence_valid": correlation_evidence_valid,
        "autovacuum_delta": autovacuum_delta,
        "autoanalyze_delta": autoanalyze_delta,
        "table_deltas": table_deltas,
    }


def validate_sql_wal_diagnostics(
    accounting: dict[str, Any],
    failures: list[str],
) -> None:
    require(
        accounting["top_statement_rows"] > 0,
        failures,
        "Gate B top-WAL evidence contains no statements",
    )
    require(
        accounting["top_level_statement_wal_bytes"] > 0,
        failures,
        "Gate B top-level tracked SQL WAL is zero",
    )
    require(
        accounting["top30_top_level_coverage"] >= 0.95,
        failures,
        (
            "Gate B top-30 covers only "
            f"{accounting['top30_top_level_coverage']:.6f} of top-level "
            "tracked SQL WAL, expected >= 0.95"
        ),
    )
    require(
        accounting["raw_top_level_to_total_ratio"] <= 1.05,
        failures,
        (
            "Gate B top-level SQL WAL exceeds interval WAL by "
            "more than the 5% sampling tolerance"
        ),
    )


PHYSICAL_WAL_FIELDS = (
    "record_count",
    "record_length_bytes",
    "main_data_length_bytes",
    "fpi_length_bytes",
)


def validate_physical_wal_evidence(
    json_path: str,
    csv_path: str,
    expected_start_lsn: str,
    expected_end_lsn: str,
    total_wal_bytes: int,
    failures: list[str],
) -> dict[str, Any]:
    try:
        evidence = load_json(json_path)
    except (OSError, json.JSONDecodeError) as error:
        failures.append(f"physical WAL JSON cannot be read: {error}")
        evidence = {}
    require(
        evidence.get("extension") == "pg_walinspect",
        failures,
        "physical WAL evidence does not identify pg_walinspect",
    )
    extension_version = evidence.get("extension_version")
    require(
        isinstance(extension_version, str) and bool(extension_version),
        failures,
        "physical WAL evidence lacks pg_walinspect extension_version",
    )
    require(
        evidence.get("start_lsn") == expected_start_lsn
        and evidence.get("end_lsn") == expected_end_lsn,
        failures,
        "physical WAL evidence boundaries differ from snapshot boundaries",
    )

    raw_groups = evidence.get("groups")
    if not isinstance(raw_groups, list):
        failures.append("physical WAL JSON groups are missing")
        raw_groups = []
    parsed_groups: dict[tuple[str, str], dict[str, int]] = {}
    group_sums = {field: 0 for field in PHYSICAL_WAL_FIELDS}
    for index, raw_group in enumerate(raw_groups):
        if not isinstance(raw_group, dict):
            failures.append(f"physical WAL group {index} is not an object")
            continue
        resource_manager = raw_group.get("resource_manager")
        record_type = raw_group.get("record_type")
        if not isinstance(resource_manager, str) or not resource_manager:
            failures.append(
                f"physical WAL group {index} lacks resource_manager"
            )
            continue
        if not isinstance(record_type, str) or not record_type:
            failures.append(f"physical WAL group {index} lacks record_type")
            continue
        identity = (resource_manager, record_type)
        if identity in parsed_groups:
            failures.append(
                "physical WAL JSON contains duplicate group "
                f"{resource_manager}/{record_type}"
            )
            continue
        values: dict[str, int] = {}
        for field in PHYSICAL_WAL_FIELDS:
            value = parse_nonnegative_integer(
                raw_group.get(field),
                (
                    f"physical WAL group {resource_manager}/{record_type} "
                    f"{field}"
                ),
                failures,
            )
            values[field] = 0 if value is None else value
            group_sums[field] += values[field]
        require(
            values["record_count"] > 0
            and values["record_length_bytes"] > 0,
            failures,
            (
                f"physical WAL group {resource_manager}/{record_type} "
                "must contain positive records and record bytes"
            ),
        )
        require(
            values["main_data_length_bytes"] + values["fpi_length_bytes"]
            <= values["record_length_bytes"],
            failures,
            (
                f"physical WAL group {resource_manager}/{record_type} "
                "main-data plus FPI bytes exceed record bytes"
            ),
        )
        parsed_groups[identity] = values

    require(
        bool(parsed_groups),
        failures,
        "physical WAL evidence contains no record groups",
    )
    raw_totals = evidence.get("totals")
    if not isinstance(raw_totals, dict):
        failures.append("physical WAL JSON totals are missing")
        raw_totals = {}
    embedded_totals: dict[str, int] = {}
    for field in PHYSICAL_WAL_FIELDS:
        value = parse_nonnegative_integer(
            raw_totals.get(field),
            f"physical WAL totals {field}",
            failures,
        )
        embedded_totals[field] = 0 if value is None else value
        require(
            embedded_totals[field] == group_sums[field],
            failures,
            (
                f"physical WAL grouped {field} sum is {group_sums[field]}, "
                f"embedded total is {embedded_totals[field]}"
            ),
        )

    csv_groups: dict[tuple[str, str], dict[str, int]] = {}
    expected_columns = [
        "resource_manager",
        "record_type",
        *PHYSICAL_WAL_FIELDS,
    ]
    try:
        with Path(csv_path).open(encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames != expected_columns:
                failures.append(
                    "physical WAL CSV columns do not match the required schema"
                )
            else:
                for index, row in enumerate(reader, start=2):
                    identity = (
                        row.get("resource_manager", ""),
                        row.get("record_type", ""),
                    )
                    if not all(identity) or identity in csv_groups:
                        failures.append(
                            f"physical WAL CSV row {index} has an invalid or "
                            "duplicate identity"
                        )
                        continue
                    values: dict[str, int] = {}
                    for field in PHYSICAL_WAL_FIELDS:
                        value = parse_nonnegative_integer(
                            row.get(field),
                            f"physical WAL CSV row {index} {field}",
                            failures,
                        )
                        values[field] = 0 if value is None else value
                    csv_groups[identity] = values
    except OSError as error:
        failures.append(f"physical WAL CSV cannot be read: {error}")
    require(
        csv_groups == parsed_groups,
        failures,
        "physical WAL CSV is not an exact mechanical projection of the JSON",
    )

    record_length_bytes = group_sums["record_length_bytes"]
    coverage = (
        record_length_bytes / total_wal_bytes if total_wal_bytes > 0 else 0.0
    )
    require(
        0.95 <= coverage <= 1.05,
        failures,
        (
            "pg_walinspect physical record coverage is "
            f"{coverage:.6f}, expected within [0.95, 1.05]"
        ),
    )
    return {
        "source": "pg_walinspect_exact_lsn_interval",
        "extension_version": extension_version,
        "start_lsn": evidence.get("start_lsn"),
        "end_lsn": evidence.get("end_lsn"),
        "group_count": len(parsed_groups),
        "totals": group_sums,
        "pg_stat_wal_interval_bytes": total_wal_bytes,
        "physical_record_coverage": coverage,
        "grouping_json_csv_match": csv_groups == parsed_groups,
        "groups": [
            {
                "resource_manager": identity[0],
                "record_type": identity[1],
                **values,
            }
            for identity, values in sorted(parsed_groups.items())
        ],
    }


def validate_embedded_top_wal(
    after: dict[str, Any],
    accounting: dict[str, Any],
    failures: list[str],
) -> None:
    rows = after.get("top_wal_statements")
    if not isinstance(rows, list):
        failures.append("after snapshot does not embed top-WAL statements")
        return
    embedded_wal = 0.0
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            failures.append(f"embedded top-WAL row {index} is not an object")
            continue
        require(
            row.get("toplevel") is True,
            failures,
            f"embedded top-WAL row {index} is not top-level",
        )
        try:
            value = float(row["wal_bytes"])
        except (KeyError, TypeError, ValueError):
            failures.append(f"embedded top-WAL row {index} has invalid wal_bytes")
            continue
        if not math.isfinite(value) or value < 0:
            failures.append(
                f"embedded top-WAL row {index} has negative or non-finite wal_bytes"
            )
            continue
        embedded_wal += value
    require(
        len(rows) == accounting["top_statement_rows"],
        failures,
        (
            "derived top-WAL CSV row count differs from its embedded boundary "
            f"snapshot: {accounting['top_statement_rows']} vs {len(rows)}"
        ),
    )
    require(
        math.isclose(
            embedded_wal,
            float(accounting["top_statement_wal_bytes"]),
            rel_tol=0,
            abs_tol=0.5,
        ),
        failures,
        (
            "derived top-WAL CSV bytes differ from its embedded boundary "
            f"snapshot: {accounting['top_statement_wal_bytes']} vs {embedded_wal}"
        ),
    )


def validate_runtime_topology(
    before_path: str,
    after_path: str,
    failures: list[str],
) -> dict[str, Any]:
    before = load_json(before_path)
    after = load_json(after_path)
    before_pods = before.get("pods")
    after_pods = after.get("pods")
    if not isinstance(before_pods, list) or not isinstance(after_pods, list):
        failures.append("runtime topology Pod sets are missing")
        return {"before": before, "after": after}

    def ready_pods(document: dict[str, Any], pods: list[Any], label: str) -> list[dict[str, Any]]:
        require(
            document.get("desired_replicas") == 1,
            failures,
            f"{label} runtime Deployment desired replicas is not exactly 1",
        )
        require(
            document.get("ready_replicas") == 1,
            failures,
            f"{label} runtime Deployment ready replicas is not exactly 1",
        )
        ready = [
            pod
            for pod in pods
            if isinstance(pod, dict)
            and pod.get("phase") == "Running"
            and pod.get("ready") is True
            and pod.get("deleting") is not True
        ]
        require(
            len(ready) == 1 and len(pods) == 1,
            failures,
            (
                f"{label} runtime Pod set has {len(pods)} Pods and "
                f"{len(ready)} Ready/non-deleting Pods, expected 1/1"
            ),
        )
        return ready

    ready_before = ready_pods(before, before_pods, "before")
    ready_after = ready_pods(after, after_pods, "after")
    before_uid = ready_before[0].get("uid") if len(ready_before) == 1 else None
    after_uid = ready_after[0].get("uid") if len(ready_after) == 1 else None
    require(
        isinstance(before_uid, str)
        and isinstance(after_uid, str)
        and before_uid == after_uid,
        failures,
        "the unique runtime Pod UID changed or is missing across Gate B",
    )
    return {
        "before": before,
        "after": after,
        "unique_pod_uid_before": before_uid,
        "unique_pod_uid_after": after_uid,
    }


def validate_runtime_samples(
    path: str,
    metric_name: str,
    expected_seconds: float,
    sample_interval_seconds: float,
    failures: list[str],
) -> dict[str, Any]:
    if not math.isfinite(sample_interval_seconds) or sample_interval_seconds <= 0:
        failures.append("runtime sample interval must be finite and positive")
        sample_interval_seconds = 1.0
    blocks: list[tuple[int, list[float]]] = []
    current_epoch: int | None = None
    current_values: list[float] = []
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("# sample_epoch_seconds "):
            if current_epoch is not None:
                blocks.append((current_epoch, current_values))
            try:
                current_epoch = int(line.rsplit(" ", 1)[1])
            except ValueError:
                failures.append(f"runtime sample marker is invalid: {line}")
                current_epoch = None
            current_values = []
            continue
        if current_epoch is None or not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2 or parts[0].split("{", 1)[0] != metric_name:
            continue
        try:
            current_values.append(float(parts[-1]))
        except ValueError:
            current_values.append(math.nan)
    if current_epoch is not None:
        blocks.append((current_epoch, current_values))

    active_values: list[float] = []
    for epoch, values in blocks:
        require(
            len(values) == 1
            and math.isfinite(values[0])
            and values[0] >= 0
            and values[0].is_integer(),
            failures,
            (
                f"runtime sample {epoch} has {len(values)} valid "
                f"{metric_name} values, expected exactly one non-negative integer"
            ),
        )
        if len(values) == 1 and math.isfinite(values[0]):
            active_values.append(values[0])

    epochs = [epoch for epoch, _ in blocks]
    for previous, current in zip(epochs, epochs[1:]):
        require(
            current > previous,
            failures,
            f"runtime sample epochs are not strictly increasing: {previous}, {current}",
        )
    max_gap = max(5.0, sample_interval_seconds * 3.0)
    observed_max_gap = (
        max((current - previous for previous, current in zip(epochs, epochs[1:])), default=0)
    )
    require(
        observed_max_gap <= max_gap,
        failures,
        (
            f"runtime sampling gap is {observed_max_gap}s, "
            f"expected no more than {max_gap:.1f}s"
        ),
    )
    expected_samples = max(1, math.floor(expected_seconds / sample_interval_seconds))
    minimum_samples = max(1, math.floor(expected_samples * 0.95))
    observed_span = float(epochs[-1] - epochs[0]) if len(epochs) >= 2 else 0.0
    minimum_span = max(0.0, expected_seconds * 0.95 - sample_interval_seconds)
    require(
        len(blocks) >= minimum_samples,
        failures,
        (
            f"runtime metric sampling is incomplete: {len(blocks)} samples, "
            f"expected at least {minimum_samples}"
        ),
    )
    require(
        observed_span >= minimum_span,
        failures,
        (
            f"runtime metric sampling spans {observed_span:.1f}s, "
            f"expected at least {minimum_span:.1f}s"
        ),
    )
    return {
        "sample_count": len(blocks),
        "first_epoch_seconds": epochs[0] if epochs else None,
        "last_epoch_seconds": epochs[-1] if epochs else None,
        "span_seconds": observed_span,
        "max_gap_seconds": observed_max_gap,
        "active_values": active_values,
    }


def require(condition: bool, failures: list[str], message: str) -> None:
    if not condition:
        failures.append(message)


def write_report(path: str, report: dict[str, Any]) -> None:
    serialized = json.dumps(report, indent=2, sort_keys=True) + "\n"
    Path(path).write_text(serialized, encoding="utf-8")
    print(serialized, end="")


def prometheus_values(path: str, metric_name: str) -> list[float]:
    values: list[float] = []
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        observed_name = parts[0].split("{", 1)[0]
        if observed_name != metric_name:
            continue
        try:
            values.append(float(parts[-1]))
        except ValueError:
            continue
    return values


def required_prometheus_values(
    path: str,
    metric_name: str,
    failures: list[str],
) -> list[float]:
    values = prometheus_values(path, metric_name)
    if not values:
        failures.append(f"required Prometheus metric {metric_name} is missing from {path}")
        return [0.0]
    if any(not math.isfinite(value) for value in values):
        failures.append(
            f"Prometheus metric {metric_name} has a non-finite value in {path}"
        )
        return [0.0]
    return values


def key_value_file(path: str) -> dict[str, int]:
    values: dict[str, int] = {}
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        key, separator, raw_value = raw_line.partition("=")
        if not separator or not re.fullmatch(r"[A-Za-z0-9_.]+", key):
            continue
        try:
            values[key] = int(raw_value.strip())
        except ValueError:
            continue
    return values


def pod_restart_count(document: dict[str, Any]) -> int | None:
    statuses = document.get("status", {}).get("containerStatuses")
    if not isinstance(statuses, list):
        return None
    return sum(int(status.get("restartCount", 0)) for status in statuses)


def pod_observed_oom(document: dict[str, Any]) -> bool | None:
    statuses = document.get("status", {}).get("containerStatuses")
    if not isinstance(statuses, list):
        return None
    for status in statuses:
        for state_name in ("state", "lastState"):
            terminated = status.get(state_name, {}).get("terminated")
            if isinstance(terminated, dict) and terminated.get("reason") == "OOMKilled":
                return True
    return False


def runtime_identity(document: dict[str, Any]) -> str | None:
    metadata = document.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("uid"), str):
        return f"pod:{metadata['uid']}"
    if document.get("local") is True and isinstance(document.get("pid"), int):
        return f"pid:{document['pid']}"
    return None


def evaluate_gate_a(args: argparse.Namespace) -> int:
    before = load_json(args.before)
    after = load_json(args.after)
    statements = load_json(args.statements)
    expected_messages = 2 if args.conversation else 0
    failures: list[str] = []
    snapshot_counter_deltas = validate_snapshot_pair(before, after, failures)
    admission_delta = int(delta(before, after, "terminal_rows", "terminal_run_admissions"))
    result_delta = int(delta(before, after, "terminal_rows", "terminal_run_results"))
    message_delta = int(delta(before, after, "terminal_rows", "conversation_messages"))
    ledgers = validate_forbidden_durable_rows(before, after, failures)
    admission_insert_calls = int(statements["admission_insert_calls"])
    admission_insert_rows = int(statements["admission_insert_rows"])
    result_insert_calls = int(statements["result_insert_calls"])
    result_insert_rows = int(statements["result_insert_rows"])
    message_insert_calls = int(statements["message_insert_calls"])
    message_insert_rows = int(statements["message_insert_rows"])
    terminal_mutation_calls = int(statements["terminal_mutation_calls"])
    forbidden_durable_mutation_calls = int(
        statements["forbidden_durable_mutation_calls"]
    )
    statement_forbidden_tables = set(statements["forbidden_durable_tables"])
    snapshot_forbidden_tables = set(before.get("forbidden_durable_rows", {}))
    require(
        int(statements["forbidden_durable_table_count"])
        == len(statement_forbidden_tables),
        failures,
        "statement evidence durable table count is inconsistent",
    )
    require(
        statement_forbidden_tables == snapshot_forbidden_tables,
        failures,
        "statement and row-snapshot forbidden durable table coverage differ",
    )
    require(admission_delta == 1, failures, f"admission delta is {admission_delta}, expected 1")
    require(result_delta == 1, failures, f"result delta is {result_delta}, expected 1")
    require(
        message_delta == expected_messages,
        failures,
        f"message delta is {message_delta}, expected {expected_messages}",
    )
    require(
        admission_insert_calls == 1 and admission_insert_rows == 1,
        failures,
        "admission INSERT was not executed exactly once for exactly one row",
    )
    require(
        result_insert_calls == 1 and result_insert_rows == 1,
        failures,
        "result INSERT was not executed exactly once for exactly one row",
    )
    require(
        message_insert_calls == expected_messages
        and message_insert_rows == expected_messages,
        failures,
        (
            "conversation message INSERT calls/rows are "
            f"{message_insert_calls}/{message_insert_rows}, "
            f"expected {expected_messages}/{expected_messages}"
        ),
    )
    require(
        terminal_mutation_calls == 0,
        failures,
        f"terminal admission/result UPDATE/DELETE calls are {terminal_mutation_calls}",
    )
    require(
        forbidden_durable_mutation_calls == 0,
        failures,
        (
            "forbidden durable table mutation calls are "
            f"{forbidden_durable_mutation_calls}"
        ),
    )
    for name, value in ledgers.items():
        require(value == 0, failures, f"existing ledger {name} changed by {value}")
    report = {
        "gate": "A",
        "passed": not failures,
        "mode": "conversation" if args.conversation else "standalone",
        "terminal_row_delta": {
            "terminal_run_admissions": admission_delta,
            "terminal_run_results": result_delta,
            "conversation_messages": message_delta,
        },
        "forbidden_durable_row_delta": ledgers,
        "stats_reset_continuity": {
            section: after[section]["stats_reset"]
            for section in ("wal", "bgwriter", "database", "io")
        },
        "snapshot_counter_delta": snapshot_counter_deltas,
        "write_statement_evidence": {
            **statements,
            "repository_contract": {
                "interface": "TerminalRunStore",
                "admission_boundary": "admit_terminal_run",
                "terminal_boundary": "commit_terminal_result",
                "postgres_source": "crates/storage/src/terminal_store/postgres.rs",
                "observed_fact": (
                    "pg_stat_statements proves the required one-row INSERTs"
                ),
                "explicit_non_claim": (
                    "database-wide transaction counters do not prove an exact "
                    "per-Run transaction count"
                ),
            },
        },
        "failures": failures,
    }
    write_report(args.output, report)
    return 0 if not failures else 1


def validate_gate_b_warmup(
    summary: dict[str, Any],
    expected_seconds: float,
    expected_arrivals: int,
    qualification: bool,
    failures: list[str],
) -> dict[str, Any]:
    iterations = k6_count(summary, "iterations", failures)
    scheduled = k6_count(
        summary,
        "terminal_run_arrivals_scheduled",
        failures,
    )
    late_arrivals = k6_count(
        summary,
        "terminal_run_arrivals_late",
        failures,
        absent_is_zero=True,
    )
    max_arrival_lateness = k6_metric(
        summary,
        "terminal_run_arrival_lateness",
        "max",
        failures,
    )
    p95_arrival_lateness = k6_metric(
        summary,
        "terminal_run_arrival_lateness",
        "p(95)",
        failures,
    )
    p99_arrival_lateness = k6_metric(
        summary,
        "terminal_run_arrival_lateness",
        "p(99)",
        failures,
    )
    arrival_slot_ms = (
        expected_seconds * 1000 / expected_arrivals
        if expected_arrivals
        else 0.0
    )
    accepted = k6_count(summary, "terminal_run_accepted", failures)
    observed = k6_count(
        summary,
        "terminal_run_terminal_observed",
        failures,
    )
    succeeded = k6_count(summary, "terminal_run_succeeded", failures)
    rejected = k6_count(
        summary,
        "terminal_run_rejected",
        failures,
        absent_is_zero=True,
    )
    failed = k6_count(
        summary,
        "terminal_run_failed",
        failures,
        absent_is_zero=True,
    )
    interrupted = k6_count(
        summary,
        "terminal_run_interrupted",
        failures,
        absent_is_zero=True,
    )
    dropped = k6_count(
        summary,
        "dropped_iterations",
        failures,
        absent_is_zero=True,
    )
    try:
        duration_ms = float(summary["state"]["testRunDurationMs"])
    except (KeyError, TypeError, ValueError):
        failures.append(
            "required warm-up k6 state.testRunDurationMs is missing or invalid"
        )
        duration_ms = 0.0
    require(
        math.isfinite(duration_ms)
        and expected_seconds * 1000 <= duration_ms
        <= (expected_seconds + 30) * 1000,
        failures,
        (
            f"warm-up duration is {duration_ms}ms; expected "
            f"{expected_seconds}s plus at most 30s graceful completion"
        ),
    )
    require(
        scheduled == expected_arrivals
        and iterations == expected_arrivals
        and dropped == 0,
        failures,
        (
            "warm-up exact scheduled arrivals/raw iterations/dropped are "
            f"{scheduled}/{iterations}/{dropped}, expected "
            f"{expected_arrivals}/{expected_arrivals}/0"
        ),
    )
    require(
        iterations == scheduled,
        failures,
        (
            f"warm-up iterations {iterations} do not equal exact scheduled "
            f"arrivals {scheduled}"
        ),
    )
    require(
        scheduled == accepted + rejected,
        failures,
        (
            f"warm-up exact arrivals {scheduled} do not equal accepted + "
            f"rejected ({accepted} + {rejected})"
        ),
    )
    require(
        accepted == expected_arrivals,
        failures,
        f"warm-up accepted {accepted}/{expected_arrivals} Runs",
    )
    require(
        observed == accepted,
        failures,
        f"warm-up accepted closure is {observed}/{accepted}",
    )
    require(
        succeeded == accepted,
        failures,
        f"warm-up successful closure is {succeeded}/{accepted}",
    )
    require(rejected == 0, failures, f"warm-up rejected {rejected} Runs")
    require(failed == 0, failures, f"warm-up failed {failed} Runs")
    require(
        interrupted == 0,
        failures,
        f"warm-up interrupted {interrupted} Runs",
    )
    require(dropped == 0, failures, f"warm-up dropped {dropped} arrivals")
    require(
        late_arrivals == 0,
        failures,
        (
            f"warm-up had {late_arrivals} arrivals at least one "
            f"{arrival_slot_ms:.3f}ms slot late"
        ),
    )
    require(
        0 <= p95_arrival_lateness <= p99_arrival_lateness
        <= max_arrival_lateness < arrival_slot_ms,
        failures,
        (
            "warm-up exact-arrival lateness p95/p99/max is "
            f"{p95_arrival_lateness:.3f}/{p99_arrival_lateness:.3f}/"
            f"{max_arrival_lateness:.3f}ms, expected below one "
            f"{arrival_slot_ms:.3f}ms slot"
        ),
    )
    if qualification:
        require(
            math.isclose(expected_seconds, 60.0),
            failures,
            f"qualification warm-up is {expected_seconds}s, expected 60s",
        )
        require(
            expected_arrivals == 600,
            failures,
            (
                "qualification warm-up expected arrivals are "
                f"{expected_arrivals}, expected 600"
            ),
        )
    return {
        "expected_seconds": expected_seconds,
        "expected_arrivals": expected_arrivals,
        "actual_duration_ms": duration_ms,
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
        "rejected": rejected,
        "failed": failed,
        "interrupted": interrupted,
        "closed_before_measured_lsn_boundary": (
            accepted == expected_arrivals
            and observed == accepted
            and succeeded == accepted
            and rejected == 0
            and failed == 0
            and interrupted == 0
            and dropped == 0
        ),
        "excluded_from_measured_lsn_interval": True,
    }


def validate_gate_b_preflight(
    args: argparse.Namespace,
    before: dict[str, Any],
    failures: list[str],
) -> dict[str, Any]:
    evidence = {
        "infrastructure": (
            load_json(args.infrastructure_freshness)
            if args.infrastructure_freshness
            else None
        ),
        "database": (
            load_json(args.database_preflight)
            if args.database_preflight
            else None
        ),
        "statistics_reset": (
            load_json(args.statistics_reset)
            if args.statistics_reset
            else None
        ),
    }
    if args.qualification:
        require(
            isinstance(evidence["infrastructure"], dict)
            and evidence["infrastructure"].get("passed") is True,
            failures,
            "qualification fresh infrastructure evidence is missing/failed",
        )
        require(
            isinstance(evidence["database"], dict)
            and evidence["database"].get("passed") is True,
            failures,
            "qualification fresh database evidence is missing/failed",
        )
        reset = evidence["statistics_reset"]
        reset_epoch = (
            reset.get("database_stats_reset_after")
            if isinstance(reset, dict)
            else None
        )
        require(
            isinstance(reset, dict)
            and reset.get("passed") is True
            and isinstance(reset_epoch, str)
            and bool(reset_epoch),
            failures,
            "qualification database statistics-reset evidence is missing/failed",
        )
        require(
            reset_epoch == before.get("database", {}).get("stats_reset"),
            failures,
            (
                "Gate B pg_stat_reset epoch does not match the measured "
                "before database statistics epoch"
            ),
        )
    return evidence


def evaluate_gate_b(args: argparse.Namespace) -> int:
    before = load_json(args.before)
    after = load_json(args.after)
    warmup_summary = load_json(args.warmup)
    summary = load_json(args.k6)
    failures: list[str] = []
    preflight = validate_gate_b_preflight(args, before, failures)
    warmup = validate_gate_b_warmup(
        warmup_summary,
        float(args.warmup_seconds),
        int(args.warmup_expected_arrivals),
        args.qualification,
        failures,
    )
    snapshot_counter_deltas = validate_snapshot_pair(before, after, failures)
    statement_boundary = validate_statement_boundary(before, after, failures)
    maintenance_evidence = validate_maintenance_stats(before, after, failures)
    accepted = k6_count(summary, "terminal_run_accepted", failures)
    observed = k6_count(summary, "terminal_run_terminal_observed", failures)
    succeeded = k6_count(summary, "terminal_run_succeeded", failures)
    iterations = k6_count(summary, "iterations", failures)
    scheduled = k6_count(
        summary,
        "terminal_run_arrivals_scheduled",
        failures,
    )
    late_arrivals = k6_count(
        summary,
        "terminal_run_arrivals_late",
        failures,
        absent_is_zero=True,
    )
    max_arrival_lateness = k6_metric(
        summary,
        "terminal_run_arrival_lateness",
        "max",
        failures,
    )
    p95_arrival_lateness = k6_metric(
        summary,
        "terminal_run_arrival_lateness",
        "p(95)",
        failures,
    )
    p99_arrival_lateness = k6_metric(
        summary,
        "terminal_run_arrival_lateness",
        "p(99)",
        failures,
    )
    # k6 omits counters that never receive a non-zero sample. These two
    # absence-means-zero counters are not used as positive workload evidence.
    rejected = k6_count(
        summary,
        "terminal_run_rejected",
        failures,
        absent_is_zero=True,
    )
    dropped = k6_count(
        summary,
        "dropped_iterations",
        failures,
        absent_is_zero=True,
    )
    try:
        test_duration_ms = float(summary["state"]["testRunDurationMs"])
    except (KeyError, TypeError, ValueError):
        failures.append("required k6 state.testRunDurationMs is missing or invalid")
        test_duration_ms = 0.0
    require(
        math.isfinite(test_duration_ms) and test_duration_ms > 0,
        failures,
        "k6 state.testRunDurationMs must be finite and positive",
    )
    measured_seconds = float(args.expected_seconds)
    if not math.isfinite(measured_seconds) or measured_seconds <= 0:
        failures.append("expected measured duration must be finite and positive")
        measured_seconds = 0.001
    actual_test_seconds = test_duration_ms / 1000.0
    expected_arrivals_for_slot = int(args.expected_arrivals)
    arrival_slot_ms = (
        measured_seconds * 1000 / expected_arrivals_for_slot
        if expected_arrivals_for_slot > 0
        else 0.0
    )
    lifecycle_p95 = k6_metric(
        summary, "terminal_run_lifecycle_duration", "p(95)", failures
    )
    lifecycle_p99 = k6_metric(
        summary, "terminal_run_lifecycle_duration", "p(99)", failures
    )
    wal_bytes = int(delta(before, after, "wal", "wal_bytes"))
    relation_before = sum(int(value) for value in before["terminal_relation_bytes"].values())
    relation_after = sum(int(value) for value in after["terminal_relation_bytes"].values())
    relation_growth = relation_after - relation_before
    admission_row_delta = int(
        delta(before, after, "terminal_rows", "terminal_run_admissions")
    )
    result_row_delta = int(
        delta(before, after, "terminal_rows", "terminal_run_results")
    )
    wal_per_run = wal_bytes / accepted if accepted else 0.0
    growth_per_run = relation_growth / accepted if accepted else 0.0
    closure = observed / accepted if accepted else 0.0
    success = succeeded / accepted if accepted else 0.0
    throughput = observed / actual_test_seconds if actual_test_seconds > 0 else 0.0
    ledgers = validate_forbidden_durable_rows(before, after, failures)
    checkpoint_delta = int(delta(before, after, "bgwriter", "checkpoints_req"))
    timed_checkpoint_delta = int(delta(before, after, "bgwriter", "checkpoints_timed"))
    deadlock_delta = int(delta(before, after, "database", "deadlocks"))
    temp_delta = int(delta(before, after, "database", "temp_files"))
    temp_bytes_delta = int(delta(before, after, "database", "temp_bytes"))
    xact_commit_delta = int(delta(before, after, "database", "xact_commit"))
    xact_rollback_delta = int(delta(before, after, "database", "xact_rollback"))
    io_delta = {
        key: delta(before, after, "io", key)
        for key in (
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
        )
    }
    checkpoint_timing_delta = {
        "write_time_ms": delta(
            before, after, "bgwriter", "checkpoint_write_time_ms"
        ),
        "sync_time_ms": delta(
            before, after, "bgwriter", "checkpoint_sync_time_ms"
        ),
    }
    database_io_timing_delta = {
        "read_time_ms": delta(before, after, "database", "blk_read_time_ms"),
        "write_time_ms": delta(before, after, "database", "blk_write_time_ms"),
    }

    runtime_admissions_delta = int(
        sum(
            required_prometheus_values(
                args.runtime_after, "terminal_run_admissions_total", failures
            )
        )
        - sum(
            required_prometheus_values(
                args.runtime_before, "terminal_run_admissions_total", failures
            )
        )
    )
    runtime_results_delta = int(
        sum(
            required_prometheus_values(
                args.runtime_after, "terminal_run_results_total", failures
            )
        )
        - sum(
            required_prometheus_values(
                args.runtime_before, "terminal_run_results_total", failures
            )
        )
    )
    runtime_interrupted_delta = int(
        sum(
            required_prometheus_values(
                args.runtime_after, "terminal_run_interrupted_total", failures
            )
        )
        - sum(
            required_prometheus_values(
                args.runtime_before, "terminal_run_interrupted_total", failures
            )
        )
    )
    commit_retry_delta = int(
        sum(
            required_prometheus_values(
                args.runtime_after,
                "terminal_run_terminal_commit_retries_total",
                failures,
            )
        )
        - sum(
            required_prometheus_values(
                args.runtime_before,
                "terminal_run_terminal_commit_retries_total",
                failures,
            )
        )
    )
    final_active_values = required_prometheus_values(
        args.runtime_after, "terminal_run_active", failures
    )
    final_active = int(max(final_active_values)) if final_active_values else -1
    runtime_samples = validate_runtime_samples(
        args.runtime_samples,
        "terminal_run_active",
        measured_seconds,
        float(args.sample_interval_seconds),
        failures,
    )
    sampled_active = runtime_samples["active_values"]
    active_observations = sampled_active + final_active_values
    max_active = int(max(active_observations)) if active_observations else -1
    runtime_sample_count = int(runtime_samples["sample_count"])

    process_before = key_value_file(args.process_before)
    process_after = key_value_file(args.process_after)
    oom_delta = (
        process_after.get("cgroup.oom", 0)
        - process_before.get("cgroup.oom", 0)
    )
    oom_kill_delta = (
        process_after.get("cgroup.oom_kill", 0)
        - process_before.get("cgroup.oom_kill", 0)
    )
    pod_before = load_json(args.pod_before)
    pod_after = load_json(args.pod_after)
    identity_before = runtime_identity(pod_before)
    identity_after = runtime_identity(pod_after)
    restart_before = pod_restart_count(pod_before)
    restart_after = pod_restart_count(pod_after)
    restart_delta = (
        restart_after - restart_before
        if restart_before is not None and restart_after is not None
        else None
    )
    pod_oom_before = pod_observed_oom(pod_before)
    pod_oom_after = pod_observed_oom(pod_after)
    pod_oom_new = pod_oom_after is True and pod_oom_before is not True
    runtime_topology = validate_runtime_topology(
        args.topology_before, args.topology_after, failures
    )
    artifact_before = int(Path(args.artifact_before).read_text().strip())
    artifact_after = int(Path(args.artifact_after).read_text().strip())
    artifact_growth = artifact_after - artifact_before
    wal_accounting = top_wal_accounting(
        args.top_wal,
        wal_bytes,
        failures,
        top_level_statement_wal_bytes=statement_boundary.get(
            "top_level_statement_wal_bytes_delta",
            -1.0,
        ),
    )
    wal_accounting["nested_statement_wal_bytes_diagnostic"] = (
        statement_boundary.get("nested_statement_wal_bytes_delta", -1.0)
    )
    wal_accounting["nested_statement_calls_diagnostic"] = (
        statement_boundary.get("nested_statement_calls_delta", -1.0)
    )
    validate_embedded_top_wal(after, wal_accounting, failures)
    physical_wal = validate_physical_wal_evidence(
        args.physical_wal,
        args.physical_wal_csv,
        str(statement_boundary.get("before", {}).get("wal_insert_lsn", "")),
        str(statement_boundary.get("after", {}).get("wal_insert_lsn", "")),
        wal_bytes,
        failures,
    )
    validate_sql_wal_diagnostics(wal_accounting, failures)

    require(accepted > 0, failures, "no accepted runs were measured")
    require(observed > 0, failures, "no terminal observations were measured")
    require(succeeded > 0, failures, "no successful terminal runs were measured")
    require(
        iterations == scheduled,
        failures,
        (
            f"k6 iterations are {iterations}, but exact scheduled arrivals "
            f"are {scheduled}"
        ),
    )
    require(
        scheduled == accepted + rejected,
        failures,
        (
            f"exact scheduled arrivals are {scheduled}, but accepted + "
            f"rejected is {accepted + rejected}"
        ),
    )
    require(
        admission_row_delta == accepted,
        failures,
        (
            f"terminal_run_admissions row delta is {admission_row_delta}, "
            f"expected {accepted}"
        ),
    )
    require(
        result_row_delta == observed,
        failures,
        (
            f"terminal_run_results row delta is {result_row_delta}, "
            f"expected {observed}"
        ),
    )
    require(closure == 1.0, failures, f"accepted closure is {closure:.6f}, expected 1.0")
    require(success >= 0.999, failures, f"scheduled success is {success:.6f}, expected >= 0.999")
    require(throughput >= 9.0, failures, f"completed throughput is {throughput:.3f}/s, expected >= 9")
    require(lifecycle_p95 <= 1000, failures, f"lifecycle p95 is {lifecycle_p95:.3f}ms")
    require(lifecycle_p99 <= 3000, failures, f"lifecycle p99 is {lifecycle_p99:.3f}ms")
    require(lifecycle_p95 >= 0, failures, f"lifecycle p95 is negative: {lifecycle_p95}")
    require(lifecycle_p99 >= 0, failures, f"lifecycle p99 is negative: {lifecycle_p99}")
    require(wal_per_run <= 32768, failures, f"WAL/run is {wal_per_run:.3f} bytes")
    require(wal_bytes <= 2.2 * 1024**3, failures, f"measured WAL is {wal_bytes} bytes")
    require(growth_per_run <= 16384, failures, f"relation growth/run is {growth_per_run:.3f} bytes")
    require(checkpoint_delta == 0, failures, f"requested checkpoints changed by {checkpoint_delta}")
    require(deadlock_delta == 0, failures, f"deadlocks changed by {deadlock_delta}")
    require(temp_delta == 0, failures, f"temp files changed by {temp_delta}")
    require(temp_bytes_delta == 0, failures, f"temp bytes changed by {temp_bytes_delta}")
    require(rejected == 0, failures, f"{rejected} requests were rejected")
    require(dropped == 0, failures, f"{dropped} iterations were dropped")
    require(
        late_arrivals == 0,
        failures,
        (
            f"{late_arrivals} exact arrivals reached or exceeded one "
            f"{arrival_slot_ms:.3f}ms slot of lateness"
        ),
    )
    require(
        0 <= p95_arrival_lateness <= p99_arrival_lateness
        <= max_arrival_lateness < arrival_slot_ms,
        failures,
        (
            "exact-arrival lateness p95/p99/max is "
            f"{p95_arrival_lateness:.3f}/{p99_arrival_lateness:.3f}/"
            f"{max_arrival_lateness:.3f}ms, expected below one "
            f"{arrival_slot_ms:.3f}ms slot"
        ),
    )
    require(
        runtime_admissions_delta == accepted,
        failures,
        (
            "runtime admission metric delta is "
            f"{runtime_admissions_delta}, expected {accepted}"
        ),
    )
    require(
        runtime_results_delta == observed,
        failures,
        f"runtime result metric delta is {runtime_results_delta}, expected {observed}",
    )
    require(
        runtime_interrupted_delta == 0,
        failures,
        f"runtime interrupted metric changed by {runtime_interrupted_delta}",
    )
    require(final_active >= 0, failures, "terminal_run_active metric is missing")
    require(final_active == 0, failures, f"runtime still has {final_active} active runs")
    require(max_active <= 50, failures, f"runtime active high-water mark is {max_active}")
    require(oom_delta == 0, failures, f"cgroup oom changed by {oom_delta}")
    require(oom_kill_delta == 0, failures, f"cgroup oom_kill changed by {oom_kill_delta}")
    require(restart_delta in (None, 0), failures, f"runtime pod restarted {restart_delta} times")
    require(
        identity_before is not None
        and identity_after is not None
        and identity_before == identity_after,
        failures,
        "runtime Pod UID/local PID changed or is missing across Gate B",
    )
    require(not pod_oom_new, failures, "runtime pod newly observed OOMKilled")
    require(
        relation_growth >= 0,
        failures,
        f"terminal relation growth is negative: {relation_growth}",
    )
    require(
        artifact_growth >= 0,
        failures,
        f"artifact-store growth is negative: {artifact_growth}",
    )
    for name, value in (
        ("runtime admissions", runtime_admissions_delta),
        ("runtime results", runtime_results_delta),
        ("runtime interrupted", runtime_interrupted_delta),
        ("terminal commit retry", commit_retry_delta),
        ("cgroup oom", oom_delta),
        ("cgroup oom_kill", oom_kill_delta),
    ):
        require(value >= 0, failures, f"{name} delta is negative: {value}")
    require(
        restart_delta is None or restart_delta >= 0,
        failures,
        f"runtime pod restart delta is negative: {restart_delta}",
    )
    for name, value in ledgers.items():
        require(value == 0, failures, f"existing ledger {name} changed by {value}")
    if args.qualification:
        expected_arrivals = int(args.expected_arrivals)
        require(
            math.isclose(measured_seconds, 7200.0),
            failures,
            f"qualification expected duration is {measured_seconds:.1f}s, expected 7200s",
        )
        require(
            test_duration_ms >= 7_200_000,
            failures,
            f"qualification duration is only {test_duration_ms / 1000:.1f}s",
        )
        require(
            test_duration_ms <= 7_320_000,
            failures,
            (
                "qualification duration is "
                f"{test_duration_ms / 1000:.1f}s, expected no more than 7320s"
            ),
        )
        require(
            expected_arrivals == 72_000,
            failures,
            (
                f"qualification expected arrivals are {expected_arrivals}, "
                "expected exactly 72000"
            ),
        )
        require(
            scheduled == expected_arrivals
            and iterations == expected_arrivals
            and dropped == 0,
            failures,
            (
                "qualification exact scheduled arrivals/raw iterations/"
                f"dropped are {scheduled}/{iterations}/{dropped}, expected "
                f"{expected_arrivals}/{expected_arrivals}/0"
            ),
        )
        require(
            restart_delta is not None,
            failures,
            "qualification lacks Kubernetes restart-count evidence",
        )
        require(
            "status.VmHWM_kb" in process_after,
            failures,
            "qualification lacks runtime VmHWM evidence",
        )
        require(
            "cgroup.oom" in process_after and "cgroup.oom_kill" in process_after,
            failures,
            "qualification lacks cgroup OOM evidence",
        )
        before_wal_keep_size = parse_nonnegative_integer(
            before.get("settings", {}).get("wal_keep_size_bytes"),
            "before qualification wal_keep_size_bytes",
            failures,
        )
        after_wal_keep_size = parse_nonnegative_integer(
            after.get("settings", {}).get("wal_keep_size_bytes"),
            "after qualification wal_keep_size_bytes",
            failures,
        )
        require(
            before_wal_keep_size is not None
            and after_wal_keep_size is not None
            and before_wal_keep_size == after_wal_keep_size
            and after_wal_keep_size >= 3 * 1024**3,
            failures,
            (
                "qualification wal_keep_size must be stable and at least "
                f"3GiB: before={before_wal_keep_size}, "
                f"after={after_wal_keep_size}"
            ),
        )

    report = {
        "gate": "B",
        "passed": not failures,
        "qualification": args.qualification,
        "preflight": preflight,
        "warmup": warmup,
        "runs": {
            "accepted": accepted,
            "terminal_observed": observed,
            "succeeded": succeeded,
            "rejected": rejected,
            "dropped_iterations": dropped,
            "iterations": iterations,
            "scheduled_arrivals": scheduled,
            "late_arrivals": late_arrivals,
            "arrival_slot_ms": arrival_slot_ms,
            "p95_arrival_lateness_ms": p95_arrival_lateness,
            "p99_arrival_lateness_ms": p99_arrival_lateness,
            "max_arrival_lateness_ms": max_arrival_lateness,
            "expected_arrivals": int(args.expected_arrivals),
            "accepted_closure": closure,
            "scheduled_success": success,
            "completed_throughput_per_second": throughput,
            "configured_measured_seconds": measured_seconds,
            "actual_test_seconds": actual_test_seconds,
        },
        "latency_ms": {"lifecycle_p95": lifecycle_p95, "lifecycle_p99": lifecycle_p99},
        "postgres": {
            "wal_bytes": wal_bytes,
            "structural_wal_bytes": wal_bytes,
            "payload_object_wal_bytes": 0,
            "wal_bytes_per_accepted": wal_per_run,
            "terminal_relation_growth_bytes": relation_growth,
            "relation_growth_bytes_per_accepted": growth_per_run,
            "terminal_admission_row_delta": admission_row_delta,
            "terminal_result_row_delta": result_row_delta,
            "requested_checkpoint_delta": checkpoint_delta,
            "timed_checkpoint_delta": timed_checkpoint_delta,
            "checkpoint_timing_delta": checkpoint_timing_delta,
            "io_delta": io_delta,
            "database_io_timing_delta": database_io_timing_delta,
            "transaction_delta": {
                "committed": xact_commit_delta,
                "rolled_back": xact_rollback_delta,
                "committed_per_accepted": (
                    xact_commit_delta / accepted if accepted else 0.0
                ),
            },
            "deadlock_delta": deadlock_delta,
            "temp_file_delta": temp_delta,
            "temp_bytes_delta": temp_bytes_delta,
            "settings": after["settings"],
            "stats_reset_continuity": {
                section: after[section]["stats_reset"]
                for section in ("wal", "bgwriter", "database", "io")
            },
            "counter_delta": snapshot_counter_deltas,
            "sql_wal_diagnostics": wal_accounting,
            "physical_wal_attribution": physical_wal,
            "maintenance_correlation": maintenance_evidence,
            "measurement_boundary": statement_boundary,
        },
        "runtime": {
            "admissions_delta": runtime_admissions_delta,
            "results_delta": runtime_results_delta,
            "interrupted_delta": runtime_interrupted_delta,
            "terminal_commit_retry_delta": commit_retry_delta,
            "active_final": final_active,
            "active_sampled_max": max_active,
            "metric_sample_count": runtime_sample_count,
            "metric_sampling": {
                key: value
                for key, value in runtime_samples.items()
                if key != "active_values"
            },
            "process_before": process_before,
            "process_after": process_after,
            "pod_restart_delta": restart_delta,
            "identity_before": identity_before,
            "identity_after": identity_after,
            "pod_oom_killed_before": pod_oom_before,
            "pod_oom_killed_after": pod_oom_after,
            "pod_oom_killed_during_gate": pod_oom_new,
            "cgroup_oom_delta": oom_delta,
            "cgroup_oom_kill_delta": oom_kill_delta,
            "topology": runtime_topology,
        },
        "artifact_store": {
            "bytes_before": artifact_before,
            "bytes_after": artifact_after,
            "payload_object_growth_bytes": artifact_growth,
        },
        "forbidden_durable_row_delta": ledgers,
        "note": (
            "SQL and physical WAL evidence are independent views and are never "
            "added together. Top-30 and aggregate SQL accounting include only "
            "toplevel=true statements; nested statements are diagnostic to "
            "avoid track=all double counting. pg_walinspect groups every "
            "physical record in the exact before/after wal_insert_lsn interval "
            "and must cover 95%-105% of the pg_stat_wal delta. Maintenance "
            "counters and timestamps are correlation-only evidence and never "
            "allocate residual bytes. The fixed fixture stores content outside "
            "PostgreSQL, so PostgreSQL WAL is structural and artifact-store "
            "bytes are separate."
        ),
        "failures": failures,
    }
    write_report(args.output, report)
    return 0 if not failures else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    gate_a = subparsers.add_parser("gate-a")
    gate_a.add_argument("--before", required=True)
    gate_a.add_argument("--after", required=True)
    gate_a.add_argument("--statements", required=True)
    gate_a.add_argument("--output", required=True)
    gate_a.add_argument("--conversation", action="store_true")
    gate_a.set_defaults(handler=evaluate_gate_a)
    gate_b = subparsers.add_parser("gate-b")
    gate_b.add_argument("--before", required=True)
    gate_b.add_argument("--after", required=True)
    gate_b.add_argument("--warmup", required=True)
    gate_b.add_argument("--k6", required=True)
    gate_b.add_argument("--runtime-before", required=True)
    gate_b.add_argument("--runtime-after", required=True)
    gate_b.add_argument("--runtime-samples", required=True)
    gate_b.add_argument("--process-before", required=True)
    gate_b.add_argument("--process-after", required=True)
    gate_b.add_argument("--pod-before", required=True)
    gate_b.add_argument("--pod-after", required=True)
    gate_b.add_argument("--topology-before", required=True)
    gate_b.add_argument("--topology-after", required=True)
    gate_b.add_argument("--artifact-before", required=True)
    gate_b.add_argument("--artifact-after", required=True)
    gate_b.add_argument("--top-wal", required=True)
    gate_b.add_argument("--physical-wal", required=True)
    gate_b.add_argument("--physical-wal-csv", required=True)
    gate_b.add_argument("--infrastructure-freshness")
    gate_b.add_argument("--database-preflight")
    gate_b.add_argument("--statistics-reset")
    gate_b.add_argument("--output", required=True)
    gate_b.add_argument("--warmup-seconds", type=float, required=True)
    gate_b.add_argument("--warmup-expected-arrivals", type=int, required=True)
    gate_b.add_argument("--expected-seconds", type=float, required=True)
    gate_b.add_argument("--expected-arrivals", type=int, required=True)
    gate_b.add_argument("--sample-interval-seconds", type=float, required=True)
    gate_b.add_argument("--qualification", action="store_true")
    gate_b.set_defaults(handler=evaluate_gate_b)
    args = parser.parse_args()
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
