use std::path::Path;

const SQLITE_V1: &str = include_str!("../migrations/formal_v1/sqlite/202607100001_formal_v1.sql");
const POSTGRES_V1: &str =
    include_str!("../migrations/formal_v1/postgres/202607100001_formal_v1.sql");

#[test]
fn only_formal_v1_migration_directories_remain() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(!root.join("migrations/sqlite").exists());
    assert!(!root.join("migrations/postgres").exists());
    assert!(root.join("migrations/formal_v1/sqlite").is_dir());
    assert!(root.join("migrations/formal_v1/postgres").is_dir());
}

#[test]
fn both_formal_backends_define_equivalent_runtime_tables_and_constraints() {
    for migration in [SQLITE_V1, POSTGRES_V1] {
        let normalized = migration.to_ascii_lowercase();
        assert!(normalized.contains("create table runs"));
        assert!(normalized.contains("create table run_events"));
        assert!(normalized.contains("create table node_outputs"));
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
    }
    assert!(SQLITE_V1.contains("json_valid(input_summary)"));
    assert!(SQLITE_V1.contains("json_valid(output)"));
    assert!(POSTGRES_V1.contains("JSONB"));
    assert!(POSTGRES_V1.contains("TIMESTAMPTZ"));
}
