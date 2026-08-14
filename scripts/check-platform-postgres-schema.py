#!/usr/bin/env python3
"""Independent validator for the clean-cut Platform v1 PostgreSQL baseline."""

import hashlib
import json
import re
import struct
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CRATE = ROOT / "crates" / "platform-postgres"
CONTRACT_PATH = CRATE / "schema-contract.json"
MIGRATIONS_DIR = CRATE / "migrations"
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
    "schema_migrations",
    "secret_bindings",
    "tasks",
    "tenant_principals",
    "tenants",
]
EXPECTED_FUNCTIONS = [
    "is_bounded_object(jsonb, integer)",
    "is_platform_id(text)",
    "is_sha256(text)",
]
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


def migration_set_digest(migrations):
    digest = hashlib.sha256()
    for migration in migrations:
        digest.update(struct.pack(">q", migration["version"]))
        digest.update(b"\0")
        digest.update(migration["name"].encode("utf-8"))
        digest.update(b"\0")
        digest.update(migration["checksum"].encode("ascii"))
        digest.update(b"\n")
    return "sha256:" + digest.hexdigest()


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
        "migration_set_digest",
        "migrations",
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
    if contract.get("schema_contract_version") != 6:
        errors.append("schema contract version must be 6")
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

    migrations = contract.get("migrations")
    if not isinstance(migrations, list) or len(migrations) != 1:
        errors.append("clean-cut baseline must contain exactly one migration")
        migrations = []
    checked_in_paths = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if [path.name for path in checked_in_paths] != ["0001_platform_baseline.sql"]:
        errors.append("migration directory must contain only 0001_platform_baseline.sql")

    sql = ""
    if migrations and checked_in_paths:
        migration = migrations[0]
        if set(migration) != {"version", "name", "checksum"}:
            errors.append("migration contract has missing or unknown fields")
        if migration.get("version") != 1 or migration.get("name") != "platform_baseline":
            errors.append("baseline migration identity is invalid")
        raw = checked_in_paths[0].read_bytes()
        if migration.get("checksum") != prefixed_sha256(raw):
            errors.append("baseline migration checksum differs from raw SQL")
        if contract.get("migration_set_digest") != migration_set_digest(migrations):
            errors.append("migration set digest differs from independent calculation")
        try:
            sql = raw.decode("utf-8")
        except UnicodeDecodeError:
            errors.append("baseline migration is not UTF-8")

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
                errors.append(f"baseline migration contains forbidden {forbidden}")
        try:
            table_bodies = extract_table_bodies(sql)
        except ValueError as failure:
            errors.append(str(failure))
            table_bodies = {}
        observed_tables = sorted({"schema_migrations", *table_bodies})
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
        if function_names != ["is_bounded_object", "is_platform_id", "is_sha256"]:
            errors.append("SQL helper function set differs from the contract")

        for table, body in table_bodies.items():
            columns = table_columns(body)
            if table not in {
                "principals",
                "scheduler_state",
                "schema_migrations",
            } and "tenant_id" not in columns:
                errors.append(f"tenant-owned table {table} lacks tenant_id")
            if "payload" in columns:
                for companion in ("payload_schema_version", "payload_digest"):
                    if companion not in columns:
                        errors.append(f"{table}.payload lacks {companion}")
        required_columns = {
            "resources": {"resource_kind", "lifecycle_state", "gate_state", "version"},
            "runs": {"state", "version", "public_sequence", "bindings"},
            "jobs": {"state", "version", "attempt_no", "lease_epoch", "lease_expires_at"},
            "tasks": {"state", "generation", "version", "deadline"},
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

    if errors:
        for error in errors:
            print(f"Platform PostgreSQL schema validation failed: {error}", file=sys.stderr)
        return 1
    print(
        "Platform PostgreSQL baseline validated "
        f"({len(EXPECTED_TABLES)} tables, 1 migration, no business triggers)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
