use std::path::Path;

const SQLITE_001: &str = include_str!("../migrations/sqlite/202607090001_create_run_history.sql");
const POSTGRES_001: &str =
    include_str!("../migrations/postgres/202607090001_create_run_history.sql");

#[test]
fn typed_run_events_are_part_of_the_baseline_migrations() {
    for migration in [SQLITE_001, POSTGRES_001] {
        assert!(migration.contains("type TEXT NOT NULL"));
        assert!(migration.contains("data TEXT NOT NULL"));
        assert!(!migration.contains("event TEXT NOT NULL"));
        assert!(!migration.contains("content TEXT NOT NULL"));
        assert!(!migration.contains("result TEXT NOT NULL"));
    }
    assert!(SQLITE_001.contains("seq INTEGER NOT NULL"));
    assert!(POSTGRES_001.contains("seq BIGINT NOT NULL"));

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(!root
        .join("migrations/sqlite/202607090003_rebuild_run_events_for_typed_events.sql")
        .exists());
    assert!(!root
        .join("migrations/postgres/202607090003_rebuild_run_events_for_typed_events.sql")
        .exists());
}
