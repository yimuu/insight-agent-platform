#!/usr/bin/env python3
"""Independent validator for the clean-cut Platform v1 PostgreSQL baseline."""

import hashlib
import json
import re
import sys
from pathlib import Path


ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "Cargo.toml").is_file())
CRATE = ROOT / "crates" / "adapters" / "platform-postgres"
CONTRACT_PATH = CRATE / "schema-contract.json"
SCHEMA_PATH = CRATE / "schema.sql"
EXPECTED_TABLES = [
    "artifact_blobs",
    "artifact_links",
    "artifacts",
    "deployments",
    "events",
    "invocations",
    "jobs",
    "outbox_events",
    "principals",
    "quota_accounts",
    "quota_ledger",
    "receipts",
    "resource_versions",
    "resources",
    "run_nodes",
    "run_values",
    "runs",
    "scheduler_state",
    "scheduler_tenant_state",
    "secret_bindings",
    "tasks",
    "tenant_principals",
    "tenants",
]
EXPECTED_FUNCTIONS = sorted([
    "history_scan_records(text, timestamp with time zone, text, text, text, text, integer)",
    "history_lock_receipt(text, text)",
    "history_lock_owner(text, text)",
    "history_event_obligations(text, text, bigint)",
    "history_lock_event(text, text)",
    "history_delete_receipt(text, text, text)",
    "history_delete_published_outbox(text, text, timestamp with time zone)",
    "history_delete_event(text, text, text)",
    "history_lock_task_chain(text, text)",
    "history_owner_delivery(text, text[])",
    "history_retire_oauth_chain(text, text, bigint, text, text, text, text)",
    "history_delete_prefix(text, text, bigint, bigint)",
    "history_lock_event_prefix(text, text, bigint, bigint, integer)",
    "history_lock_run(text, text)",
    "history_scan_runs(timestamp with time zone, text, text, text, text, integer)",
    "is_bounded_object(jsonb, integer)",
    "is_platform_id(text)",
    "is_sha256(text)",
    "is_trace_id(text)",
])
REJECTED_PHYSICAL_NAMES = {
    "execution_attempts",
    "continuations",
    "command_receipts",
    "external_callback_inbox",
    "public_run_stream_heads",
    "public_run_event_projections",
    "registry_exact_resources",
    "attempt_transitions",
    "management_operation_transitions",
}


class DuplicateKey(ValueError):
    pass


def strict_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKey(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=strict_pairs)


def prefixed_sha256(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def extract_table_bodies(sql):
    bodies = {}
    pattern = re.compile(r"CREATE TABLE insight_platform\.([a-z][a-z0-9_]*)\s*\(")
    for match in pattern.finditer(sql):
        name = match.group(1)
        position = match.end()
        depth = 1
        quoted = False
        while position < len(sql) and depth:
            character = sql[position]
            if character == "'":
                if quoted and position + 1 < len(sql) and sql[position + 1] == "'":
                    position += 2
                    continue
                quoted = not quoted
            elif not quoted:
                if character == "(":
                    depth += 1
                elif character == ")":
                    depth -= 1
            position += 1
        if depth:
            raise ValueError(f"table {name} has an unclosed body")
        if name in bodies:
            raise ValueError(f"table {name} is declared twice")
        bodies[name] = sql[match.end() : position - 1]
    return bodies


def top_level_segments(body):
    segments = []
    start = 0
    depth = 0
    quoted = False
    position = 0
    while position < len(body):
        character = body[position]
        if character == "'":
            if quoted and position + 1 < len(body) and body[position + 1] == "'":
                position += 2
                continue
            quoted = not quoted
        elif not quoted:
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
            elif character == "," and depth == 0:
                segments.append(body[start:position].strip())
                start = position + 1
        position += 1
    segments.append(body[start:].strip())
    return [segment for segment in segments if segment]


def table_columns(body):
    columns = set()
    for segment in top_level_segments(body):
        first = segment.split(None, 1)[0].lower()
        if first not in {"constraint", "primary", "foreign", "unique", "check"}:
            columns.add(first.strip('"'))
    return columns


def main():
    errors = []
    try:
        contract = load_json(CONTRACT_PATH)
    except (OSError, json.JSONDecodeError, DuplicateKey) as failure:
        print(f"Platform PostgreSQL schema validation failed: {failure}", file=sys.stderr)
        return 1

    expected_top_level = {
        "architecture",
        "contract",
        "functions",
        "schema_snapshot_digest",
        "schema_snapshot",
        "physical_inventory",
        "postgres_major",
        "schema",
        "schema_contract_version",
        "table_count",
        "tables",
    }
    if set(contract) != expected_top_level:
        errors.append("schema contract has missing or unknown top-level fields")
    if contract.get("contract") != "insight.platform/v1/postgres-baseline":
        errors.append("schema contract identity is invalid")
    if contract.get("schema_contract_version") != 14:
        errors.append("schema contract version must be 14")
    if contract.get("postgres_major") != 16:
        errors.append("PostgreSQL major version must be 16")
    if contract.get("schema") != "insight_platform":
        errors.append("authority schema must be insight_platform")
    if contract.get("table_count") != 23:
        errors.append("baseline table count must be exactly 23")
    if contract.get("tables") != EXPECTED_TABLES:
        errors.append("schema contract table set/order differs from ADR-0001")
    if contract.get("functions") != EXPECTED_FUNCTIONS:
        errors.append("schema contract helper function set differs")

    architecture = contract.get("architecture")
    if architecture != {
        "adr": "docs/adr/0001-platform-v2-postgres-baseline.md",
        "business_state_triggers": False,
        "compatibility_schema": False,
        "current_state_is_not_reconstructed_from_events": True,
        "event_payload_is_not_duplicated_in_outbox": True,
    }:
        errors.append("schema architecture flags differ from ADR-0001")

    raw = SCHEMA_PATH.read_bytes()
    sql = raw.decode("utf-8")
    digest = prefixed_sha256(raw)
    if contract.get("schema_snapshot") != {"path": "schema.sql", "digest": digest}:
        errors.append("current schema snapshot identity differs from raw SQL")
    if contract.get("schema_snapshot_digest") != digest:
        errors.append("current schema snapshot digest differs from independent calculation")
    if contract.get("physical_inventory") != {"path": "schema-inventory.json", "digest": prefixed_sha256((CRATE / "schema-inventory.json").read_bytes())}:
        errors.append("physical inventory digest differs from current attachment")
    if (CRATE / "migrations").exists():
        errors.append("obsolete migration chain must not exist")

    if sql:
        upper = sql.upper()
        for forbidden in (
            "DROP TABLE",
            "DROP SCHEMA",
            "CREATE TRIGGER",
            "CREATE CONSTRAINT TRIGGER",
            "CREATE EXTENSION",
            "SQLITE",
        ):
            if forbidden in upper:
                errors.append(f"current schema contains forbidden {forbidden}")
        try:
            table_bodies = extract_table_bodies(sql)
        except ValueError as failure:
            errors.append(str(failure))
            table_bodies = {}
        observed_tables = sorted(table_bodies)
        if observed_tables != EXPECTED_TABLES:
            errors.append("SQL CREATE TABLE set differs from the 23-table contract")
        rejected = sorted(REJECTED_PHYSICAL_NAMES.intersection(table_bodies))
        if rejected:
            errors.append(f"rejected physical tables returned: {rejected}")
        function_names = sorted(
            set(
                re.findall(
                    r"CREATE FUNCTION insight_platform\.([a-z][a-z0-9_]*)\(", sql
                )
            )
        )
        if function_names != sorted([
            "history_delete_prefix", "history_lock_event_prefix", "history_lock_run", "history_scan_runs",
            "history_scan_records", "history_lock_receipt", "history_lock_owner", "history_event_obligations", "history_lock_event", "history_delete_receipt", "history_delete_published_outbox", "history_delete_event", "history_lock_task_chain", "history_owner_delivery", "history_retire_oauth_chain",
            "is_bounded_object",
            "is_platform_id",
            "is_sha256",
            "is_trace_id",
        ]):
            errors.append("SQL helper function set differs from the contract")

        for table, body in table_bodies.items():
            columns = table_columns(body)
            if table not in {
                "principals",
                "scheduler_state",
                "scheduler_tenant_state",
            } and "tenant_id" not in columns:
                errors.append(f"tenant-owned table {table} lacks tenant_id")
            if "payload" in columns:
                for companion in ("payload_schema_version", "payload_digest"):
                    if companion not in columns:
                        errors.append(f"{table}.payload lacks {companion}")
        required_columns = {
            "resources": {"resource_kind", "lifecycle_state", "gate_state", "version"},
            "runs": {"state", "version", "public_sequence", "bindings", "public_replay_floor", "history_holds", "execution_requirement"},
            "jobs": {
                "job_kind",
                "work_class",
                "owner_kind",
                "state",
                "version",
                "attempt_no",
                "lease_epoch",
                "lease_expires_at",
                "scheduler_partition_id", "execution_requirement", "execution_semantic_identity", "attempt_build_digest",
            },
            "tasks": {"state", "generation", "version", "deadline", "current_cleanup_job_id"},
            "scheduler_tenant_state": {"tenant_id", "work_class", "partition_id", "deficit", "job_creation_cutoff", "job_cursor_id"},
            "events": {"aggregate_kind", "aggregate_id", "event_type", "payload_digest"},
            "receipts": {"receipt_kind", "idempotency_key_digest", "request_digest"},
            "outbox_events": {"event_id", "next_publish_at", "claim_epoch"},
            "quota_accounts": {"limit_value", "reserved_value", "used_value", "version"},
            "quota_ledger": {
                "correlation_id",
                "entry_kind",
                "reserved_amount",
                "used_amount",
                "request_digest",
            },
        }
        for table, required in required_columns.items():
            missing = required.difference(table_columns(table_bodies.get(table, "")))
            if missing:
                errors.append(f"{table} lacks required columns {sorted(missing)}")

    rust_paths = sorted((CRATE / "src").glob("*.rs")) + sorted(
        (CRATE / "tests").glob("*.rs")
    )
    job_insert_pattern = re.compile(
        r"INSERT INTO insight_platform\.jobs\s*\((?P<columns>.*?)\)\s*VALUES",
        re.DOTALL,
    )
    for path in rust_paths:
        source = path.read_text(encoding="utf-8")
        for match in job_insert_pattern.finditer(source):
            columns = {
                column.strip() for column in match.group("columns").split(",")
            }
            if "job_kind" not in columns:
                line = source.count("\n", 0, match.start()) + 1
                errors.append(f"{path.relative_to(ROOT)}:{line} Job INSERT lacks job_kind")
        if path.parent.name == "src" and "payload ->> 'kind'" in source:
            errors.append(
                f"{path.relative_to(ROOT)} uses JSON payload kind as a hot Job predicate"
            )
        if path.parent.name == "src" and "owner_kind = 'sandbox_job'" in source:
            errors.append(
                f"{path.relative_to(ROOT)} uses the unregistered sandbox_job owner kind"
            )
        if path.name == "mcp_repository.rs":
            for work_class in ("mcp", "context"):
                missing_kind = re.compile(
                    rf"(?:job\.)?work_class = '{work_class}'\s+"
                    rf"AND (?:job\.)?owner_kind = 'mcp_operation'"
                )
                for match in missing_kind.finditer(source):
                    line = source.count("\n", 0, match.start()) + 1
                    errors.append(
                        f"{path.relative_to(ROOT)}:{line} {work_class} MCP-owned Job "
                        "predicate lacks exact job_kind"
                    )

    if errors:
        for error in errors:
            print(f"Platform PostgreSQL schema validation failed: {error}", file=sys.stderr)
        return 1
    print(
        "Platform PostgreSQL baseline validated "
        f"({len(EXPECTED_TABLES)} tables, current snapshot, no business triggers)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
