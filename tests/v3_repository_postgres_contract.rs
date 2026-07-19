#[test]
fn postgres_repository_uses_run_locks_rechecks_replay_and_relies_on_db_uniqueness() {
    let source = include_str!("../src/engine/repository/postgres.rs")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();

    assert!(source.contains("from workflow_runs where run_id = $1 for update"));
    assert!(source.matches("load_replay(").count() >= 4);
    assert!(source.contains("may have committed while this transaction waited"));
    assert!(source.contains("on conflict (run_id) do nothing"));
    assert!(source.contains("set next_event_seq = next_event_seq + 1"));
    assert!(source.contains("returning next_event_seq - 1"));
    assert!(source.contains("insert into execution_events"));
    assert!(source.contains("insert into public_event_outbox"));
}

#[test]
fn sqlite_repository_declares_and_enforces_its_single_writer_subset() {
    let source = include_str!("../src/engine/repository/sqlite.rs")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();

    assert!(source.contains("max_connections(1)"));
    assert!(source.contains("writer: arc<mutex<()>>"));
    assert!(source.matches("self.writer.lock().await").count() >= 3);
    assert!(source.contains("this backend is not a multi-runtime lease authority"));
}

#[test]
fn repository_authority_inputs_are_closed_and_content_derived() {
    let source = include_str!("../src/engine/repository/model.rs")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();

    assert!(source.contains("pub fn from_verified_plan("));
    assert!(source.contains("plan.verify()"));
    assert!(source.contains("plan.semantic_hash().as_str()"));
    assert!(source.contains("canonical_value(&binding_projection)"));
    assert!(source.contains("event: pendingexecutionevent"));
    assert!(source.contains("payload: publiceventpayload"));
    assert!(!source.contains("safe_event_payload"));
    assert!(!source.contains("event_kind: string"));
}
