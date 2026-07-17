use std::{path::Path, sync::Arc, time::Duration};

use chrono::{DateTime, TimeZone, Utc};
use insight_agent_platform::{
    events::protocol::{RunEvent, RunEventScope, RunEventType},
    history::{
        repository::{RunRepository, TerminalProposal, TerminalSequence},
        sqlite::SqliteRunRepository,
        types::{
            summarize_input, NewRun, RunAttachment, RunLifecycle, RunStatus, RunTerminal,
            StopError, TerminalUpdate,
        },
    },
    outcome::{FailureKind, RunFailure, RunOutput},
};
use serde_json::json;
use sqlx::SqlitePool;
use tempfile::tempdir;
use tokio::sync::Barrier;

const RUN_ID: &str = "run_sqlite_v2";

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

fn scope(run_id: &str) -> RunEventScope {
    RunEventScope {
        request_id: format!("req_{run_id}"),
        run_id: run_id.to_string(),
        agent_id: "general-agent".to_string(),
        agent_version: "sha256:abc".to_string(),
    }
}

fn event(run_id: &str, event_type: RunEventType, seq: u64) -> RunEvent {
    RunEvent::ok_at(
        event_type,
        seq,
        scope(run_id),
        at(seq as u32),
        json!({"seq":seq}),
    )
}

fn completed_update(run_id: &str) -> TerminalUpdate {
    TerminalUpdate::new(
        run_id,
        at(10),
        RunTerminal::Completed {
            output: RunOutput {
                content: Some("answer".to_string()),
                format: Some("text".to_string()),
                data: json!({"answer":"answer"}),
            },
        },
    )
}

fn failed_update(run_id: &str) -> TerminalUpdate {
    TerminalUpdate::new(
        run_id,
        at(10),
        RunTerminal::Failed {
            error: RunFailure {
                kind: FailureKind::Infrastructure,
                code: "INFRASTRUCTURE_FAILURE".to_string(),
                message: "runtime infrastructure failed".to_string(),
            },
        },
    )
}

#[tokio::test]
async fn sqlite_repository_persists_lifecycle_events_and_replay() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    repo.check_health().await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();
    repo.mark_running(RUN_ID, at(1)).await.unwrap();
    let events = vec![
        event(RUN_ID, RunEventType::RunCreated, 1),
        event(RUN_ID, RunEventType::RunStarted, 2),
        event(RUN_ID, RunEventType::OperationStarted, 3),
        event(RUN_ID, RunEventType::OperationCompleted, 4),
    ];
    repo.append_events(&events).await.unwrap();

    let replay = repo.list_events_after(RUN_ID, 1, 100).await.unwrap();
    assert_eq!(
        replay.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(replay[1].event_type, RunEventType::OperationStarted);
    assert_eq!(replay[2].event_type, RunEventType::OperationCompleted);

    let committed = repo
        .commit_terminal(
            TerminalProposal::new(scope(RUN_ID), completed_update(RUN_ID)).unwrap(),
            TerminalSequence::Expected(5),
        )
        .await
        .unwrap();
    let expected = RunEvent::ok_at(
        RunEventType::RunCompleted,
        5,
        scope(RUN_ID),
        at(10),
        json!({
            "content": "answer",
            "format": "text",
            "data": {"answer":"answer"},
        }),
    );
    assert_eq!(committed, expected);
    let losing_update = TerminalUpdate::new(
        RUN_ID,
        at(11),
        RunTerminal::Cancelled {
            error: StopError {
                code: "RUN_CANCELLED".to_string(),
                message: "run cancelled".to_string(),
            },
        },
    );
    let authoritative = repo
        .commit_terminal(
            TerminalProposal::new(scope(RUN_ID), losing_update).unwrap(),
            TerminalSequence::Expected(5),
        )
        .await
        .unwrap();
    assert_eq!(authoritative, expected);

    let record = repo.get_run(RUN_ID).await.unwrap().unwrap();
    assert_eq!(record.status(), RunStatus::Completed);
    assert_eq!(record.attachment, RunAttachment::Detached);
    assert_eq!(record.agent_version, "sha256:abc");
    assert_eq!(record.started_at, Some(at(1)));
    assert_eq!(record.ended_at, Some(at(10)));
    assert!(matches!(
        record.lifecycle,
        RunLifecycle::Completed {
            output: RunOutput {
                content: Some(ref content),
                ..
            }
        } if content == "answer"
    ));
    let replay = repo.list_events_after(RUN_ID, 0, 100).await.unwrap();
    assert_eq!(
        replay.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(replay.last(), Some(&expected));
}

#[tokio::test]
async fn duplicate_event_sequence_is_rejected_without_partial_batch_insertion() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Attached))
        .await
        .unwrap();
    repo.append_events(&[event(RUN_ID, RunEventType::RunCreated, 1)])
        .await
        .unwrap();

    let error = repo
        .append_events(&[
            event(RUN_ID, RunEventType::RunStarted, 2),
            event(RUN_ID, RunEventType::OperationStarted, 1),
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
async fn sqlite_repository_rejects_legacy_event_schema_versions() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Attached))
        .await
        .unwrap();
    let mut legacy = event(RUN_ID, RunEventType::RunCreated, 1);
    legacy.schema_version = 1;

    let error = repo.append_events(&[legacy]).await.unwrap_err();

    assert_eq!(error.code(), "HISTORY_EVENT_INVALID");
    assert!(repo
        .list_events_after(RUN_ID, 0, 100)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn sqlite_schema_rejects_inconsistent_run_lifecycle_columns() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("lifecycle-constraints.db");
    let url = sqlite_url(&path);
    let repo = SqliteRunRepository::connect(&url).await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();
    let pool = SqlitePool::connect(&url).await.unwrap();

    let completed_without_output = sqlx::query(
        "UPDATE runs SET status = 'completed', ended_at = CURRENT_TIMESTAMP WHERE run_id = ?",
    )
    .bind(RUN_ID)
    .execute(&pool)
    .await;
    assert!(completed_without_output.is_err());

    let failed_without_error = sqlx::query(
        "UPDATE runs SET status = 'failed', ended_at = CURRENT_TIMESTAMP WHERE run_id = ?",
    )
    .bind(RUN_ID)
    .execute(&pool)
    .await;
    assert!(failed_without_error.is_err());

    let running_with_terminal_error = sqlx::query(
        "UPDATE runs SET error_kind = 'workflow', error_code = 'WORKFLOW_X', error_message = 'x' WHERE run_id = ?",
    )
    .bind(RUN_ID)
    .execute(&pool)
    .await;
    assert!(running_with_terminal_error.is_err());
}

#[tokio::test]
async fn sqlite_reconstruction_rejects_corrupt_terminal_column_combinations() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("corrupt-lifecycle.db");
    let url = sqlite_url(&path);
    let repo = SqliteRunRepository::connect(&url).await.unwrap();
    let pool = SqlitePool::connect(&url).await.unwrap();
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();

    let cases = [
        (
            "created_with_ended_at",
            "created",
            Some("2026-07-10T00:00:10+00:00"),
            None,
            None,
            None,
            None,
        ),
        (
            "running_with_ended_at",
            "running",
            Some("2026-07-10T00:00:10+00:00"),
            None,
            None,
            None,
            None,
        ),
        (
            "completed_without_ended_at",
            "completed",
            None,
            Some(r#"{"data":{}}"#),
            None,
            None,
            None,
        ),
        (
            "failed_without_ended_at",
            "failed",
            None,
            None,
            Some("workflow"),
            Some("WORKFLOW_CORRUPT"),
            Some("corrupt workflow failure"),
        ),
        (
            "cancelled_without_ended_at",
            "cancelled",
            None,
            None,
            None,
            Some("RUN_CANCELLED"),
            Some("corrupt cancellation"),
        ),
        (
            "interrupted_without_ended_at",
            "interrupted",
            None,
            None,
            None,
            Some("RUN_INTERRUPTED"),
            Some("corrupt interruption"),
        ),
    ];

    for (name, status, ended_at, output, error_kind, error_code, error_message) in cases {
        let run_id = format!("{RUN_ID}_{name}");
        repo.create_run(new_run(&run_id, RunAttachment::Detached))
            .await
            .unwrap();
        sqlx::query(
            "UPDATE runs
             SET status = ?, ended_at = ?, output = ?,
                 error_kind = ?, error_code = ?, error_message = ?
             WHERE run_id = ?",
        )
        .bind(status)
        .bind(ended_at)
        .bind(output)
        .bind(error_kind)
        .bind(error_code)
        .bind(error_message)
        .bind(&run_id)
        .execute(&mut *connection)
        .await
        .unwrap();

        let error = repo
            .get_run(&run_id)
            .await
            .expect_err("corrupt ended_at presence must fail reconstruction");
        assert_eq!(error.code(), "HISTORY_TERMINAL_CORRUPT", "case {name}");
    }
}

#[tokio::test]
async fn recovery_atomically_fills_missing_events_and_terminal_state() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();
    repo.mark_running(RUN_ID, at(1)).await.unwrap();
    let created = event(RUN_ID, RunEventType::RunCreated, 1);
    repo.append_events(std::slice::from_ref(&created))
        .await
        .unwrap();
    let recovered = repo
        .commit_terminal(
            TerminalProposal::new(scope(RUN_ID), failed_update(RUN_ID)).unwrap(),
            TerminalSequence::NextDurable,
        )
        .await
        .unwrap();
    assert_eq!(recovered.seq, 2);
    assert_eq!(
        repo.get_run(RUN_ID).await.unwrap().unwrap().status(),
        RunStatus::Failed
    );
    let recovered_record = repo.get_run(RUN_ID).await.unwrap().unwrap();
    assert!(matches!(
        recovered_record.lifecycle,
        RunLifecycle::Failed {
            error: RunFailure {
                kind: FailureKind::Infrastructure,
                ..
            }
        }
    ));
    assert_eq!(
        repo.list_events_after(RUN_ID, 0, 100)
            .await
            .unwrap()
            .last()
            .unwrap()
            .data,
        json!({"kind":"infrastructure"})
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
    repo.append_events(&[event(RUN_ID, RunEventType::RunCreated, 1)])
        .await
        .unwrap();
    repo.append_events(&[event(RUN_ID, RunEventType::RunStarted, 2)])
        .await
        .unwrap();
    let terminal = repo
        .commit_terminal(
            TerminalProposal::new(scope(RUN_ID), failed_update(RUN_ID)).unwrap(),
            TerminalSequence::NextDurable,
        )
        .await
        .unwrap();
    assert_eq!(terminal.seq, 3);
    assert_eq!(terminal.event_type, RunEventType::RunFailed);
    assert_eq!(
        repo.get_run(RUN_ID).await.unwrap().unwrap().status(),
        RunStatus::Failed
    );
    assert_eq!(
        repo.list_events_after(RUN_ID, 0, 100).await.unwrap().len(),
        3
    );

    let durable_winner = repo
        .commit_terminal(
            TerminalProposal::new(scope(RUN_ID), completed_update(RUN_ID)).unwrap(),
            TerminalSequence::NextDurable,
        )
        .await
        .unwrap();
    assert_eq!(durable_winner, terminal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_sqlite_connections_choose_one_authoritative_terminal_without_lock_residue() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("terminal-race.db");
    let url = sqlite_url(&path);
    let completed_repository = SqliteRunRepository::connect(&url).await.unwrap();
    let failed_repository = SqliteRunRepository::connect(&url).await.unwrap();

    completed_repository
        .create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();
    completed_repository
        .mark_running(RUN_ID, at(1))
        .await
        .unwrap();
    completed_repository
        .append_events(&[
            event(RUN_ID, RunEventType::RunCreated, 1),
            event(RUN_ID, RunEventType::RunStarted, 2),
        ])
        .await
        .unwrap();

    let completed_proposal =
        TerminalProposal::new(scope(RUN_ID), completed_update(RUN_ID)).unwrap();
    let failed_proposal = TerminalProposal::new(
        scope(RUN_ID),
        TerminalUpdate::new(
            RUN_ID,
            at(11),
            RunTerminal::Failed {
                error: RunFailure {
                    kind: FailureKind::Infrastructure,
                    code: "RACE_INFRASTRUCTURE_FAILURE".to_string(),
                    message: "race infrastructure failure".to_string(),
                },
            },
        ),
    )
    .unwrap();
    let completed_candidate = completed_proposal.event_at(3);
    let failed_candidate = failed_proposal.event_at(3);
    assert_ne!(completed_candidate, failed_candidate);

    let gate_pool = SqlitePool::connect(&url).await.unwrap();
    let mut gate = gate_pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *gate)
        .await
        .unwrap();

    let start = Arc::new(Barrier::new(3));
    let completed_task = {
        let repository = completed_repository.clone();
        let start = Arc::clone(&start);
        tokio::spawn(async move {
            start.wait().await;
            repository
                .commit_terminal(completed_proposal, TerminalSequence::Expected(3))
                .await
        })
    };
    let failed_task = {
        let repository = failed_repository.clone();
        let start = Arc::clone(&start);
        tokio::spawn(async move {
            start.wait().await;
            repository
                .commit_terminal(failed_proposal, TerminalSequence::Expected(3))
                .await
        })
    };

    start.wait().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !completed_task.is_finished(),
        "completed proposal bypassed or failed while the independent writer lock was held"
    );
    assert!(
        !failed_task.is_finished(),
        "failed proposal bypassed or failed while the independent writer lock was held"
    );
    sqlx::query("ROLLBACK").execute(&mut *gate).await.unwrap();
    drop(gate);
    gate_pool.close().await;

    let (completed_join, failed_join) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(completed_task, failed_task)
    })
    .await
    .expect("independent SQLite terminal race did not converge");
    let completed_result = completed_join
        .expect("completed terminal task panicked")
        .expect("completed terminal proposal failed");
    let failed_result = failed_join
        .expect("failed terminal task panicked")
        .expect("failed terminal proposal failed");

    assert_eq!(completed_result, failed_result);
    let completed_won = completed_result == completed_candidate;
    let failed_won = failed_result == failed_candidate;
    assert_ne!(
        completed_won, failed_won,
        "exactly one proposal must equal the authoritative terminal"
    );
    let authoritative = completed_result;

    let completed_view = completed_repository.get_run(RUN_ID).await.unwrap().unwrap();
    let failed_view = failed_repository.get_run(RUN_ID).await.unwrap().unwrap();
    assert_eq!(completed_view, failed_view);
    assert_eq!(completed_view.ended_at, Some(authoritative.timestamp));
    assert_eq!(completed_view.updated_at, authoritative.timestamp);
    if completed_won {
        assert_eq!(completed_view.status(), RunStatus::Completed);
        assert_eq!(
            completed_view.lifecycle,
            RunLifecycle::Completed {
                output: RunOutput {
                    content: Some("answer".to_string()),
                    format: Some("text".to_string()),
                    data: json!({"answer":"answer"}),
                },
            }
        );
    } else {
        assert_eq!(completed_view.status(), RunStatus::Failed);
        assert_eq!(
            completed_view.lifecycle,
            RunLifecycle::Failed {
                error: RunFailure {
                    kind: FailureKind::Infrastructure,
                    code: "RACE_INFRASTRUCTURE_FAILURE".to_string(),
                    message: "race infrastructure failure".to_string(),
                },
            }
        );
    }

    let completed_events = completed_repository
        .list_events_after(RUN_ID, 0, 100)
        .await
        .unwrap();
    let failed_events = failed_repository
        .list_events_after(RUN_ID, 0, 100)
        .await
        .unwrap();
    assert_eq!(completed_events, failed_events);
    assert_eq!(
        completed_events
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        completed_events
            .iter()
            .filter(|event| {
                matches!(
                    event.event_type,
                    RunEventType::RunCompleted
                        | RunEventType::RunFailed
                        | RunEventType::RunCancelled
                        | RunEventType::RunInterrupted
                )
            })
            .count(),
        1
    );
    assert_eq!(completed_events.last(), Some(&authoritative));

    let third_repository = SqliteRunRepository::connect(&url).await.unwrap();
    let third_proposal = TerminalProposal::new(
        scope(RUN_ID),
        TerminalUpdate::new(
            RUN_ID,
            at(12),
            RunTerminal::Cancelled {
                error: StopError {
                    code: "RACE_CANCELLED".to_string(),
                    message: "later terminal proposal".to_string(),
                },
            },
        ),
    )
    .unwrap();
    assert_ne!(third_proposal.event_at(3), authoritative);
    let third_result = third_repository
        .commit_terminal(third_proposal, TerminalSequence::Expected(3))
        .await
        .unwrap();
    assert_eq!(third_result, authoritative);
    assert_eq!(
        third_repository.get_run(RUN_ID).await.unwrap().unwrap(),
        completed_view
    );
    assert_eq!(
        third_repository
            .list_events_after(RUN_ID, 0, 100)
            .await
            .unwrap(),
        completed_events
    );

    let probe_pool = SqlitePool::connect(&url).await.unwrap();
    let terminal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM run_events
         WHERE run_id = ?
           AND event_type IN ('run.completed', 'run.failed', 'run.cancelled', 'run.interrupted')",
    )
    .bind(RUN_ID)
    .fetch_one(&probe_pool)
    .await
    .unwrap();
    assert_eq!(terminal_count, 1);

    let mut probe = probe_pool.acquire().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *probe).await?;
        let updated = sqlx::query("UPDATE runs SET updated_at = updated_at WHERE run_id = ?")
            .bind(RUN_ID)
            .execute(&mut *probe)
            .await?;
        assert_eq!(updated.rows_affected(), 1);
        sqlx::query("COMMIT").execute(&mut *probe).await?;
        Ok::<(), sqlx::Error>(())
    })
    .await
    .expect("fresh SQLite writer remained blocked after the terminal race")
    .unwrap();
    drop(probe);
    probe_pool.close().await;
}

#[tokio::test]
async fn terminal_proposal_and_expected_sequence_are_validated_before_storage() {
    let repo = SqliteRunRepository::in_memory().await.unwrap();
    repo.create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();

    let scope_error =
        TerminalProposal::new(scope("different_run"), completed_update(RUN_ID)).unwrap_err();
    assert_eq!(scope_error.code(), "HISTORY_EVENT_INVALID");

    let error = repo
        .commit_terminal(
            TerminalProposal::new(scope(RUN_ID), completed_update(RUN_ID)).unwrap(),
            TerminalSequence::Expected(2),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "HISTORY_EVENT_INVALID");
    assert_eq!(
        repo.get_run(RUN_ID).await.unwrap().unwrap().status(),
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
        repo.append_events(&[event(run_id, RunEventType::RunCreated, 1)])
            .await
            .unwrap();
    }
    repo.mark_running("running_run", at(1)).await.unwrap();
    repo.append_events(&[event("running_run", RunEventType::RunStarted, 2)])
        .await
        .unwrap();

    assert_eq!(repo.mark_incomplete_interrupted(at(20)).await.unwrap(), 2);

    for (run_id, expected_seq) in [("created_run", 2), ("running_run", 3)] {
        let record = repo.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(record.status(), RunStatus::Interrupted);
        assert_eq!(record.ended_at, Some(at(20)));
        assert!(matches!(
            record.lifecycle,
            RunLifecycle::Interrupted {
                error: StopError {
                    ref code,
                    ..
                }
            } if code == "RUN_INTERRUPTED"
        ));
        assert!(
            serde_json::to_value(&record).unwrap()["error"]
                .get("kind")
                .is_none(),
            "startup interruption must not fabricate a failure kind"
        );
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
async fn deleting_a_run_cascades_to_events() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("history.db");
    let url = sqlite_url(&path);
    let repo = Arc::new(SqliteRunRepository::connect(&url).await.unwrap());
    repo.create_run(new_run(RUN_ID, RunAttachment::Detached))
        .await
        .unwrap();
    repo.append_events(&[event(RUN_ID, RunEventType::RunCreated, 1)])
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
    assert_eq!(event_count, 0);
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}?mode=rwc", path.display())
}
