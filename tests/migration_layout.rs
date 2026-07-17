use std::path::Path;

const SQLITE_V2: &str = include_str!("../migrations/formal_v2/sqlite/202607170001_formal_v2.sql");
const POSTGRES_V2: &str =
    include_str!("../migrations/formal_v2/postgres/202607170001_formal_v2.sql");

#[test]
fn only_formal_v2_migration_directories_remain() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(!root.join("migrations/sqlite").exists());
    assert!(!root.join("migrations/postgres").exists());
    assert!(!root.join("migrations/formal_v1").exists());
    assert!(root.join("migrations/formal_v2/sqlite").is_dir());
    assert!(root.join("migrations/formal_v2/postgres").is_dir());
}

#[test]
fn both_formal_backends_define_equivalent_runtime_tables_and_constraints() {
    for migration in [SQLITE_V2, POSTGRES_V2] {
        let normalized = migration.to_ascii_lowercase();
        assert!(normalized.contains("create table runs"));
        assert!(normalized.contains("create table run_events"));
        assert!(!normalized.contains("create table node_outputs"));
        assert!(!normalized.contains("node_id"));
        assert!(normalized.contains("agent_version"));
        assert!(normalized.contains("attachment"));
        assert!(normalized.contains("'attached'"));
        assert!(normalized.contains("'detached'"));
        for status in [
            "'created'",
            "'running'",
            "'completed'",
            "'failed'",
            "'cancelled'",
            "'interrupted'",
        ] {
            assert!(normalized.contains(status), "missing status {status}");
        }
        assert!(normalized.contains("unique (run_id, seq)"));
        assert!(normalized.contains("on delete cascade"));
        assert!(normalized.contains("error_code"));
        assert!(normalized.contains("error_message"));
        assert!(normalized.contains("error_kind"));
        assert!(normalized.contains("'operation'"));
        assert!(!normalized.contains("'node'"));
        assert!(normalized.contains("status = 'completed'"));
        assert!(normalized.contains("status = 'failed'"));
        assert!(normalized.contains("status in ('cancelled', 'interrupted')"));
    }
    assert!(SQLITE_V2.contains("json_valid(input_summary)"));
    assert!(SQLITE_V2.contains("json_valid(output)"));
    assert!(POSTGRES_V2.contains("JSONB"));
    assert!(POSTGRES_V2.contains("TIMESTAMPTZ"));
}

#[test]
fn postgres_alone_defines_the_exclusive_runtime_owner() {
    let postgres = POSTGRES_V2.to_ascii_lowercase();
    let sqlite = SQLITE_V2.to_ascii_lowercase();

    assert!(postgres.contains("create table runtime_ownership"));
    assert!(postgres.contains("singleton smallint primary key check (singleton = 1)"));
    assert!(postgres.contains("generation bigint not null check (generation >= 0)"));
    assert!(postgres.contains("owner_id text"));
    assert!(postgres.contains("claimed_at timestamptz"));
    assert!(postgres.contains("generation = 0 and owner_id is null and claimed_at is null"));
    assert!(postgres.contains("generation > 0 and owner_id is not null and claimed_at is not null"));
    assert!(postgres.contains("values (1, 0, null, null)"));

    assert!(!sqlite.contains("runtime_ownership"));
}
