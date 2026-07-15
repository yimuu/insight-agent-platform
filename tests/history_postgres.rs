use chrono::{DateTime, TimeZone, Utc};
use insight_agent_platform::{
    events::protocol::{RunEvent, RunEventScope, RunEventType},
    history::{
        postgres::PostgresRunRepository,
        repository::{RunRepository, TerminalProposal, TerminalSequence},
        types::{
            summarize_input, NewRun, NodeOutputRecord, RunAttachment, RunLifecycle, RunStatus,
            RunTerminal, StopError, TerminalUpdate,
        },
    },
    outcome::{FailureKind, RunFailure, RunOutput},
};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe};
use uuid::Uuid;

fn at(second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, second).unwrap()
}

fn new_run(run_id: &str) -> NewRun {
    NewRun {
        run_id: run_id.to_string(),
        request_id: format!("req_{run_id}"),
        agent_id: "general-agent".to_string(),
        agent_version: "sha256:postgres".to_string(),
        attachment: RunAttachment::Detached,
        created_at: at(0),
        input_summary: summarize_input(&json!({"question":"private"})),
    }
}

fn scope(run_id: &str, node_id: Option<&str>) -> RunEventScope {
    RunEventScope {
        request_id: format!("req_{run_id}"),
        run_id: run_id.to_string(),
        agent_id: "general-agent".to_string(),
        agent_version: "sha256:postgres".to_string(),
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
async fn postgres_repository_matches_the_formal_v1_contract() {
    let database_url = std::env::var("RUN_HISTORY_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if std::env::var_os("CI").is_some() && database_url.is_none() {
        panic!("RUN_HISTORY_POSTGRES_URL is required in CI");
    }
    let Some(database_url) = database_url else {
        eprintln!("skipping postgres history test: RUN_HISTORY_POSTGRES_URL is not set");
        return;
    };

    let suffix = Uuid::new_v4();
    let schema = format!("formal_v1_{}", suffix.simple());
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repo = PostgresRunRepository::connect(&scoped_url).await.unwrap();
    repo.check_health().await.unwrap();
    let run_id = format!("run_pg_{suffix}");

    repo.create_run(new_run(&run_id)).await.unwrap();
    repo.mark_running(&run_id, at(1)).await.unwrap();
    repo.append_events(&[
        event(&run_id, RunEventType::RunCreated, 1, None),
        event(&run_id, RunEventType::RunStarted, 2, None),
        event(&run_id, RunEventType::NodeStarted, 3, Some("answer")),
        event(
            &run_id,
            RunEventType::BranchFailed,
            4,
            Some("must_be_ignored"),
        ),
    ])
    .await
    .unwrap();
    repo.put_node_output(NodeOutputRecord {
        run_id: run_id.clone(),
        node_id: "answer".to_string(),
        output: json!({"text":"ok"}),
        completed_at: at(4),
    })
    .await
    .unwrap();
    assert_eq!(
        repo.list_events_after(&run_id, 1, 100)
            .await
            .unwrap()
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    let replay = repo.list_events_after(&run_id, 3, 100).await.unwrap();
    assert_eq!(replay[0].event_type, RunEventType::BranchFailed);
    assert_eq!(replay[0].node_id, None);

    let sequence_error = repo
        .commit_terminal(
            TerminalProposal::new(scope(&run_id, None), completed_update(&run_id)).unwrap(),
            TerminalSequence::Expected(6),
        )
        .await
        .unwrap_err();
    assert_eq!(sequence_error.code(), "HISTORY_EVENT_INVALID");
    assert_eq!(
        repo.get_run(&run_id).await.unwrap().unwrap().status(),
        RunStatus::Running
    );

    let committed = repo
        .commit_terminal(
            TerminalProposal::new(scope(&run_id, None), completed_update(&run_id)).unwrap(),
            TerminalSequence::Expected(5),
        )
        .await
        .unwrap();
    let expected = RunEvent::ok_at(
        RunEventType::RunCompleted,
        5,
        scope(&run_id, None),
        at(10),
        json!({
            "content": "answer",
            "format": "text",
            "data": {"answer":"answer"},
        }),
    );
    assert_eq!(committed, expected);
    let losing_update = TerminalUpdate::new(
        &run_id,
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
            TerminalProposal::new(scope(&run_id, None), losing_update).unwrap(),
            TerminalSequence::Expected(5),
        )
        .await
        .unwrap();
    assert_eq!(authoritative, expected);
    let record = repo.get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(record.status(), RunStatus::Completed);
    assert_eq!(record.agent_version, "sha256:postgres");
    assert!(matches!(
        record.lifecycle,
        RunLifecycle::Completed {
            output: RunOutput {
                content: Some(ref content),
                ..
            }
        } if content == "answer"
    ));
    let replay = repo.list_events_after(&run_id, 0, 100).await.unwrap();
    assert_eq!(
        replay.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(replay.last(), Some(&expected));

    let duplicate_error = repo
        .append_events(&[event(&run_id, RunEventType::RunStarted, 5, None)])
        .await
        .unwrap_err();
    assert_eq!(duplicate_error.code(), "HISTORY_WRITE_FAILED");

    let recovery_id = format!("recovery_pg_{suffix}");
    repo.create_run(new_run(&recovery_id)).await.unwrap();
    repo.mark_running(&recovery_id, at(1)).await.unwrap();
    let created = event(&recovery_id, RunEventType::RunCreated, 1, None);
    repo.append_events(std::slice::from_ref(&created))
        .await
        .unwrap();
    let recovered = repo
        .commit_terminal(
            TerminalProposal::new(scope(&recovery_id, None), failed_update(&recovery_id)).unwrap(),
            TerminalSequence::NextDurable,
        )
        .await
        .unwrap();
    assert_eq!(recovered.seq, 2);
    assert_eq!(
        repo.get_run(&recovery_id).await.unwrap().unwrap().status(),
        RunStatus::Failed
    );
    assert!(matches!(
        repo.get_run(&recovery_id).await.unwrap().unwrap().lifecycle,
        RunLifecycle::Failed {
            error: RunFailure {
                kind: FailureKind::Infrastructure,
                ..
            }
        }
    ));
    assert_eq!(recovered.data, json!({"kind":"infrastructure"}));
    assert_eq!(
        repo.list_events_after(&recovery_id, 0, 100)
            .await
            .unwrap()
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let conflict_id = format!("recovery_conflict_pg_{suffix}");
    repo.create_run(new_run(&conflict_id)).await.unwrap();
    repo.mark_running(&conflict_id, at(1)).await.unwrap();
    repo.append_events(&[event(&conflict_id, RunEventType::RunCreated, 1, None)])
        .await
        .unwrap();
    repo.append_events(&[event(&conflict_id, RunEventType::RunStarted, 2, None)])
        .await
        .unwrap();
    let terminal = repo
        .commit_terminal(
            TerminalProposal::new(scope(&conflict_id, None), failed_update(&conflict_id)).unwrap(),
            TerminalSequence::NextDurable,
        )
        .await
        .unwrap();
    assert_eq!(terminal.seq, 3);
    assert_eq!(
        repo.get_run(&conflict_id).await.unwrap().unwrap().status(),
        RunStatus::Failed
    );
    assert_eq!(
        repo.list_events_after(&conflict_id, 0, 100)
            .await
            .unwrap()
            .len(),
        3
    );
    let durable_winner = repo
        .commit_terminal(
            TerminalProposal::new(scope(&conflict_id, None), completed_update(&conflict_id))
                .unwrap(),
            TerminalSequence::NextDurable,
        )
        .await
        .unwrap();
    assert_eq!(durable_winner, terminal);

    for active_id in ["created_pg", "running_pg"] {
        repo.create_run(new_run(active_id)).await.unwrap();
        repo.append_events(&[event(active_id, RunEventType::RunCreated, 1, None)])
            .await
            .unwrap();
    }
    repo.mark_running("running_pg", at(1)).await.unwrap();
    repo.append_events(&[event("running_pg", RunEventType::RunStarted, 2, None)])
        .await
        .unwrap();
    assert_eq!(repo.mark_incomplete_interrupted(at(20)).await.unwrap(), 2);
    assert_eq!(
        repo.get_run("created_pg").await.unwrap().unwrap().status(),
        RunStatus::Interrupted
    );
    let interrupted = repo.get_run("created_pg").await.unwrap().unwrap();
    assert!(matches!(
        interrupted.lifecycle,
        RunLifecycle::Interrupted {
            error: StopError { .. }
        }
    ));
    assert!(serde_json::to_value(&interrupted).unwrap()["error"]
        .get("kind")
        .is_none());
    assert_eq!(
        repo.list_events_after("running_pg", 0, 100)
            .await
            .unwrap()
            .last()
            .unwrap()
            .event_type,
        RunEventType::RunInterrupted
    );

    let scoped_admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&scoped_url)
        .await
        .unwrap();
    let constraints_id = format!("constraints_pg_{suffix}");
    repo.create_run(new_run(&constraints_id)).await.unwrap();
    assert!(sqlx::query(
        "UPDATE runs SET status = 'completed', ended_at = CURRENT_TIMESTAMP WHERE run_id = $1",
    )
    .bind(&constraints_id)
    .execute(&scoped_admin)
    .await
    .is_err());
    assert!(sqlx::query(
        "UPDATE runs SET status = 'failed', ended_at = CURRENT_TIMESTAMP WHERE run_id = $1",
    )
    .bind(&constraints_id)
    .execute(&scoped_admin)
    .await
    .is_err());
    assert!(
        sqlx::query(
            "UPDATE runs SET error_kind = 'workflow', error_code = 'WORKFLOW_X', error_message = 'x' WHERE run_id = $1",
        )
        .bind(&constraints_id)
        .execute(&scoped_admin)
        .await
        .is_err()
    );

    let drop_lifecycle_constraint: String = sqlx::query_scalar(
        "SELECT format('ALTER TABLE runs DROP CONSTRAINT %I', conname)
         FROM pg_constraint
         WHERE conrelid = 'runs'::regclass
           AND contype = 'c'
           AND pg_get_constraintdef(oid) LIKE '%ended_at%'",
    )
    .fetch_one(&scoped_admin)
    .await
    .unwrap();
    sqlx::query(AssertSqlSafe(drop_lifecycle_constraint))
        .execute(&scoped_admin)
        .await
        .unwrap();

    let corruption_cases = [
        (
            "created_with_ended_at",
            "created",
            Some(at(10)),
            None,
            None,
            None,
            None,
        ),
        (
            "running_with_ended_at",
            "running",
            Some(at(10)),
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

    for (name, status, ended_at, output, error_kind, error_code, error_message) in corruption_cases
    {
        let corrupt_id = format!("corrupt_{name}_{suffix}");
        repo.create_run(new_run(&corrupt_id)).await.unwrap();
        sqlx::query(
            "UPDATE runs
             SET status = $1, ended_at = $2, output = $3::jsonb,
                 error_kind = $4, error_code = $5, error_message = $6
             WHERE run_id = $7",
        )
        .bind(status)
        .bind(ended_at)
        .bind(output)
        .bind(error_kind)
        .bind(error_code)
        .bind(error_message)
        .bind(&corrupt_id)
        .execute(&scoped_admin)
        .await
        .unwrap();

        let error = repo
            .get_run(&corrupt_id)
            .await
            .expect_err("corrupt ended_at presence must fail reconstruction");
        assert_eq!(error.code(), "HISTORY_TERMINAL_CORRUPT", "case {name}");
    }

    sqlx::query("DELETE FROM runs WHERE run_id = $1")
        .bind(&run_id)
        .execute(&scoped_admin)
        .await
        .unwrap();
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM run_events WHERE run_id = $1")
        .bind(&run_id)
        .fetch_one(&scoped_admin)
        .await
        .unwrap();
    let output_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM node_outputs WHERE run_id = $1")
            .bind(&run_id)
            .fetch_one(&scoped_admin)
            .await
            .unwrap();
    assert_eq!(event_count, 0);
    assert_eq!(output_count, 0);

    drop(repo);
    drop(scoped_admin);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}
