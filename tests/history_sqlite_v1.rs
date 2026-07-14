use std::{path::Path, sync::Arc};

use chrono::{DateTime, TimeZone, Utc};
use insight_agent_platform::{
    events::protocol::{RunEvent, RunEventScope, RunEventType},
    history::{
        repository::RunRepository,
        sqlite::SqliteRunRepository,
        types::{
            summarize_input, NewRun, NodeOutputRecord, RunAttachment, RunStatus, TerminalUpdate,
        },
    },
    outcome::RunOutput,
};
use serde_json::json;
use sqlx::SqlitePool;
use tempfile::tempdir;

const RUN_ID: &str = "run_sqlite_v1";

fn at(second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, second).unwrap()
}

fn new_run(run_id: &str, attachment: RunAttachment) -> NewRun {
    NewRun {
        run_id: run_id.to_string(),
        request_id: format!("req_{run_id}"),
        agent_id: "general-agent".to_string(),
        agent_version: "sha256:abc".to_string(),
        attachment,
        created_at: at(0),
        input_summary: summarize_input(&json!({
            "question":"a private value",
            "image_url":"https://secret.example/image.png"
        })),
    }
}

fn scope(run_id: &str, node_id: Option<&str>) -> RunEventScope {
    RunEventScope {
        request_id: format!("req_{run_id}"),
        run_id: run_id.to_string(),
        agent_id: "general-agent".to_string(),
        agent_version: "sha256:abc".to_string(),
        node_id: node_id.map(str::to_string),
    }
}

fn event(run_id: &str, event_type: RunEventType, seq: u64, node_id: Option<&str>) -> RunEvent {
    RunEvent::ok_at(
        event_type,
        seq,
        scope(run_id, node_id),
        at(seq as u32),
        json!({"seq":seq}),
    )
}

fn completed_update(run_id: &str) -> TerminalUpdate {
    TerminalUpdate::new(
        run_id,
        RunStatus::Completed,
        at(10),
        Some(RunOutput {
            content: Some("answer".to_string()),
            format: Some("text".to_string()),
            data: json!({"answer":"answer"}),
        }),
        None,
        None,
    )
    .unwrap()
}

fn failed_update(run_id: &str) -> TerminalUpdate {
    TerminalUpdate::new(
        run_id,
        RunStatus::Failed,
        at(10),
        None,
        Some("INFRASTRUCTURE_FAILURE".to_string()),
        Some("runtime infrastructure failed".to_string()),
    )
    .unwrap()
}

#[tokio::test]
async fn sqlite_repository_persists_lifecycle_events_outputs_and_replay() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();
    repo.mark_running(RUN_ID, at(1)).await.unwrap();
    let events = vec![
        event(RUN_ID, RunEventType::RunCreated, 1, None),
        event(RUN_ID, RunEventType::RunStarted, 2, None),
        event(RUN_ID, RunEventType::NodeStarted, 3, Some("answer")),
        event(
            RUN_ID,
            RunEventType::BranchFailed,
            4,
            Some("must_be_ignored"),
        ),
    ];
    repo.append_events(&events).await.unwrap();
    repo.put_node_output(NodeOutputRecord {
        run_id: RUN_ID.to_string(),
        node_id: "answer".to_string(),
        output: json!({"text":"ok"}),
        completed_at: at(4),
    })
    .await
    .unwrap();

    let replay = repo.list_events_after(RUN_ID, 1, 100).await.unwrap();
    assert_eq!(
        replay.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(replay[1].node_id.as_deref(), Some("answer"));
    assert_eq!(replay[2].event_type, RunEventType::BranchFailed);
    assert_eq!(replay[2].node_id, None);

    let terminal_event = event(RUN_ID, RunEventType::RunCompleted, 5, None);
    assert!(repo
        .finish_run(completed_update(RUN_ID), terminal_event)
        .await
        .unwrap());
    let losing_update = TerminalUpdate::new(
        RUN_ID,
        RunStatus::Cancelled,
        at(11),
        None,
        Some("RUN_CANCELLED".to_string()),
        Some("run cancelled".to_string()),
    )
    .unwrap();
    assert!(!repo
        .finish_run(
            losing_update,
            RunEvent::error_at(
                RunEventType::RunCancelled,
                5,
                scope(RUN_ID, None),
                at(11),
                "RUN_CANCELLED",
                "run cancelled",
                json!({}),
            ),
        )
        .await
        .unwrap());

    let record = repo.get_run(RUN_ID).await.unwrap().unwrap();
    assert_eq!(record.status, RunStatus::Completed);
    assert_eq!(record.attachment, RunAttachment::Detached);
    assert_eq!(record.agent_version, "sha256:abc");
    assert_eq!(record.started_at, Some(at(1)));
    assert_eq!(record.ended_at, Some(at(10)));
    assert_eq!(record.output.unwrap().content.as_deref(), Some("answer"));
    assert_eq!(record.error_code, None);
    assert_eq!(
        repo.list_events_after(RUN_ID, 0, 100)
            .await
            .unwrap()
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
}

#[tokio::test]
async fn duplicate_event_sequence_is_rejected_without_partial_batch_insertion() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Attached))
        .await
        .unwrap();
    repo.append_events(&[event(RUN_ID, RunEventType::RunCreated, 1, None)])
        .await
        .unwrap();

    let error = repo
        .append_events(&[
            event(RUN_ID, RunEventType::RunStarted, 2, None),
            event(RUN_ID, RunEventType::NodeStarted, 1, Some("answer")),
        ])
        .await
        .unwrap_err();

    assert_eq!(error.code(), "HISTORY_WRITE_FAILED");
    assert_eq!(
        repo.list_events_after(RUN_ID, 0, 100).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn recovery_atomically_fills_missing_events_and_terminal_state() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();
    repo.mark_running(RUN_ID, at(1)).await.unwrap();
    let created = event(RUN_ID, RunEventType::RunCreated, 1, None);
    repo.append_events(std::slice::from_ref(&created))
        .await
        .unwrap();
    let failed = RunEvent::error_at(
        RunEventType::RunFailed,
        2,
        scope(RUN_ID, None),
        at(10),
        "INFRASTRUCTURE_FAILURE",
        "runtime infrastructure failed",
        json!({}),
    );

    let recovered = repo
        .recover_run(failed_update(RUN_ID), failed)
        .await
        .unwrap();
    assert_eq!(recovered.seq, 2);
    assert_eq!(
        repo.get_run(RUN_ID).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
    assert_eq!(
        repo.list_events_after(RUN_ID, 0, 100)
            .await
            .unwrap()
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[tokio::test]
async fn recovery_derives_terminal_sequence_from_locked_durable_state() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();
    repo.mark_running(RUN_ID, at(1)).await.unwrap();
    repo.append_events(&[event(RUN_ID, RunEventType::RunCreated, 1, None)])
        .await
        .unwrap();
    repo.append_events(&[event(RUN_ID, RunEventType::RunStarted, 2, None)])
        .await
        .unwrap();
    let proposal = RunEvent::error_at(
        RunEventType::RunFailed,
        1,
        scope(RUN_ID, None),
        at(10),
        "INFRASTRUCTURE_FAILURE",
        "runtime infrastructure failed",
        json!({}),
    );

    let terminal = repo
        .recover_run(failed_update(RUN_ID), proposal)
        .await
        .unwrap();
    assert_eq!(terminal.seq, 3);
    assert_eq!(terminal.event_type, RunEventType::RunFailed);
    assert_eq!(
        repo.get_run(RUN_ID).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
    assert_eq!(
        repo.list_events_after(RUN_ID, 0, 100).await.unwrap().len(),
        3
    );
}

#[tokio::test]
async fn repository_rejects_invalid_terminal_updates_before_mutating_the_run() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();
    let nonterminal = TerminalUpdate {
        run_id: RUN_ID.to_string(),
        status: RunStatus::Running,
        ended_at: at(10),
        output: None,
        error_code: None,
        error_message: None,
    };

    let error = repo
        .finish_run(
            nonterminal,
            event(RUN_ID, RunEventType::RunCompleted, 1, None),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "HISTORY_EVENT_INVALID");

    let error = repo
        .finish_run(
            completed_update(RUN_ID),
            event(RUN_ID, RunEventType::RunFailed, 1, None),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "HISTORY_EVENT_INVALID");
    assert_eq!(
        repo.get_run(RUN_ID).await.unwrap().unwrap().status,
        RunStatus::Created
    );
    assert!(repo
        .list_events_after(RUN_ID, 0, 100)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn startup_reconciliation_interrupts_created_and_running_runs_with_events() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    for run_id in ["created_run", "running_run"] {
        repo.create_run(new_run(run_id, RunAttachment::Detached))
            .await
            .unwrap();
        repo.append_events(&[event(run_id, RunEventType::RunCreated, 1, None)])
            .await
            .unwrap();
    }
    repo.mark_running("running_run", at(1)).await.unwrap();
    repo.append_events(&[event("running_run", RunEventType::RunStarted, 2, None)])
        .await
        .unwrap();

    assert_eq!(repo.mark_incomplete_interrupted(at(20)).await.unwrap(), 2);

    for (run_id, expected_seq) in [("created_run", 2), ("running_run", 3)] {
        let record = repo.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(record.status, RunStatus::Interrupted);
        assert_eq!(record.ended_at, Some(at(20)));
        assert_eq!(record.error_code.as_deref(), Some("RUN_INTERRUPTED"));
        let events = repo.list_events_after(run_id, 0, 100).await.unwrap();
        assert_eq!(events.last().unwrap().seq, expected_seq);
        assert_eq!(
            events.last().unwrap().event_type,
            RunEventType::RunInterrupted
        );
        assert_eq!(events.last().unwrap().code, "RUN_INTERRUPTED");
    }
    assert_eq!(repo.mark_incomplete_interrupted(at(21)).await.unwrap(), 0);
}

#[test]
fn input_summary_contains_shape_metadata_but_no_values() {
    let input = json!({"secret":"do-not-store", "nested":{"token":"also-secret"}});
    let summary = summarize_input(&input);
    let serialized = serde_json::to_string(&summary).unwrap();

    assert_eq!(summary["keys"], json!(["nested", "secret"]));
    assert_eq!(
        summary["serialized_bytes"],
        serde_json::to_vec(&input).unwrap().len()
    );
    assert!(!serialized.contains("do-not-store"));
    assert!(!serialized.contains("also-secret"));
}

#[tokio::test]
async fn deleting_a_run_cascades_to_events_and_node_outputs() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("history.db");
    let url = sqlite_url(&path);
    let repo = Arc::new(SqliteRunRepository::connect(&url).await.unwrap());
    repo.create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();
    repo.append_events(&[event(RUN_ID, RunEventType::RunCreated, 1, None)])
        .await
        .unwrap();
    repo.put_node_output(NodeOutputRecord {
        run_id: RUN_ID.to_string(),
        node_id: "answer".to_string(),
        output: json!({"text":"ok"}),
        completed_at: at(2),
    })
    .await
    .unwrap();
    drop(repo);

    let pool = SqlitePool::connect(&url).await.unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM runs WHERE run_id = ?")
        .bind(RUN_ID)
        .execute(&pool)
        .await
        .unwrap();
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM run_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    let output_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM node_outputs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_count, 0);
    assert_eq!(output_count, 0);
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}?mode=rwc", path.display())
}
