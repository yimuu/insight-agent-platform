# Run History Migration Reset Implementation Plan

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace legacy-preserving run-event migrations with a clean development baseline that discards all existing history data.

**Architecture:** Define the current typed `run_events` table directly in each backend's first migration and remove the rebuild migration. Let sqlx initialize fresh databases without runtime legacy-schema repair.

**Tech Stack:** Rust, sqlx 0.9, SQLite, PostgreSQL, Cargo tests

## Global Constraints

- Existing development history data is disposable and will not be migrated.
- SSE and run-history API event envelopes must not change.
- SQLite and PostgreSQL must use equivalent typed event schemas.

---

### Task 1: Make the typed event schema the migration baseline

**Files:**
- Create: `tests/migration_layout.rs`
- Modify: `migrations/sqlite/202607090001_create_run_history.sql`
- Modify: `migrations/postgres/202607090001_create_run_history.sql`
- Delete: `migrations/sqlite/202607090003_rebuild_run_events_for_typed_events.sql`
- Delete: `migrations/postgres/202607090003_rebuild_run_events_for_typed_events.sql`

**Interfaces:**
- Consumes: sqlx embedded migrations under `migrations/{sqlite,postgres}`
- Produces: fresh `run_events(run_id, type, seq, timestamp, code, message, data)` tables

- [x] **Step 1: Write the failing migration layout test**

```rust
#[test]
fn typed_run_events_are_part_of_the_baseline_migrations() {
    for migration in [SQLITE_001, POSTGRES_001] {
        assert!(migration.contains("type TEXT NOT NULL"));
        assert!(migration.contains("seq"));
        assert!(migration.contains("data TEXT NOT NULL"));
        assert!(!migration.contains("event TEXT NOT NULL"));
    }
    assert!(!sqlite_003.exists());
    assert!(!postgres_003.exists());
}
```

- [x] **Step 2: Run the test and verify the old migration layout fails**

Run: `cargo test --test migration_layout -- --nocapture`

Expected: FAIL because `001` still contains `event/content/result` and both `003` files exist.

- [x] **Step 3: Replace the event tables in both `001` migrations**

```sql
CREATE TABLE IF NOT EXISTS run_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    type TEXT NOT NULL,
    seq INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    code INTEGER NOT NULL,
    message TEXT NOT NULL,
    data TEXT NOT NULL
);
```

Use `BIGSERIAL` and `BIGINT` for PostgreSQL, and create `idx_run_events_run_id` on `(run_id, seq, id)`. Delete both `003` files.

- [x] **Step 4: Run the migration layout test**

Run: `cargo test --test migration_layout -- --nocapture`

Expected: PASS.

### Task 2: Remove runtime legacy migration support

**Files:**
- Modify: `src/history/store.rs`
- Modify: `tests/history_postgres.rs`
- Delete local data: `data/run_history.sqlite3`

**Interfaces:**
- Consumes: current sqlx migrators
- Produces: direct fresh-database initialization through `SQLITE_MIGRATOR.run` and `POSTGRES_MIGRATOR.run`

- [x] **Step 1: Remove legacy upgrade tests**

Delete `sqlite_migration_preserves_legacy_run_events` and `postgres_migration_preserves_legacy_run_events_when_configured`. Keep fresh SQLite and PostgreSQL read/write tests.

- [x] **Step 2: Remove schema preparation and column repair helpers**

Remove calls and definitions for `prepare_sqlite_schema_for_migration`, `prepare_postgres_schema_for_migration`, `ensure_sqlite_legacy_columns`, `add_sqlite_column_if_missing`, and `ensure_postgres_legacy_columns`. Connect each pool and run its migrator directly.

- [x] **Step 3: Delete disposable local history data**

Delete `data/run_history.sqlite3`. Recreate the Docker Compose PostgreSQL volume when running integration tests so no applied migration checksum remains.

- [x] **Step 4: Verify the complete change**

Run:

```bash
cargo fmt --check
cargo check
cargo test
docker compose -f docker-compose.postgres.yml up -d
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' cargo test --test history_postgres -- --nocapture
git diff --check
```

Expected: all commands exit successfully and no test references legacy event migration.
