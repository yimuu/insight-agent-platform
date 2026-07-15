use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, TimeZone, Utc};
use insight_agent_platform::{
    events::protocol::{RunEvent, RunEventScope, RunEventType},
    history::{
        postgres::PostgresRunRepository,
        repository::{HistoryError, RunRepository, TerminalProposal, TerminalSequence},
        types::{
            summarize_input, NewRun, NodeOutputRecord, RunAttachment, RunLifecycle, RunStatus,
            RunTerminal, StopError, TerminalUpdate,
        },
    },
    outcome::{FailureKind, RunFailure, RunOutput},
};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use uuid::Uuid;

const OWNERSHIP_OPERATION_TIMEOUT: Duration = Duration::from_secs(2);
const OWNERSHIP_PROBE_TIMEOUT: Duration = Duration::from_millis(500);
const OWNERSHIP_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

fn postgres_database_url() -> Option<String> {
    let database_url = std::env::var("RUN_HISTORY_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if std::env::var_os("CI").is_some() && database_url.is_none() {
        panic!("RUN_HISTORY_POSTGRES_URL is required in CI");
    }
    if database_url.is_none() {
        eprintln!("skipping postgres history test: RUN_HISTORY_POSTGRES_URL is not set");
    }
    database_url
}

struct PostgresTestSchema {
    admin: PgPool,
    database_url: String,
    schema: String,
}

impl PostgresTestSchema {
    async fn create(database_url: &str, label: &str) -> Self {
        let schema = format!("formal_v1_{label}_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(4)
            .connect(database_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        Self {
            admin,
            database_url: database_url.to_string(),
            schema,
        }
    }

    fn url(&self, application_name: &str) -> String {
        let separator = if self.database_url.contains('?') {
            '&'
        } else {
            '?'
        };
        format!(
            "{}{separator}options=-csearch_path%3D{}&application_name={application_name}",
            self.database_url, self.schema
        )
    }

    async fn scoped_pool(&self, application_name: &str) -> PgPool {
        PgPoolOptions::new()
            .max_connections(2)
            .connect(&self.url(application_name))
            .await
            .unwrap()
    }

    async fn ownership_row(&self) -> OwnershipRow {
        let scoped = self.scoped_pool("ownership_row_reader").await;
        let (singleton, generation, owner_id, claimed_at) = sqlx::query_as(
            "SELECT singleton, generation, owner_id, claimed_at
             FROM runtime_ownership",
        )
        .fetch_one(&scoped)
        .await
        .unwrap();
        scoped.close().await;
        OwnershipRow {
            singleton,
            generation,
            owner_id,
            claimed_at,
        }
    }

    async fn wait_for_advisory_backend(&self, application_name: &str) -> i32 {
        let deadline = Instant::now() + OWNERSHIP_WAIT_TIMEOUT;
        loop {
            let pid = sqlx::query_scalar(
                "SELECT DISTINCT activity.pid
                 FROM pg_stat_activity activity
                 JOIN pg_locks lock ON lock.pid = activity.pid
                 WHERE activity.application_name = $1
                   AND lock.locktype = 'advisory'
                   AND lock.granted",
            )
            .bind(application_name)
            .fetch_optional(&self.admin)
            .await
            .unwrap();
            if let Some(pid) = pid {
                return pid;
            }
            assert!(
                Instant::now() < deadline,
                "ownership advisory backend was not observed for {application_name}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_blocked_terminal_backends(&self, application_name: &str) -> Vec<i32> {
        let deadline = Instant::now() + OWNERSHIP_WAIT_TIMEOUT;
        loop {
            let pids = sqlx::query_scalar(
                "SELECT DISTINCT pid
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND application_name = $1
                   AND state = 'active'
                   AND wait_event_type = 'Lock'
                   AND query LIKE '%SELECT status FROM runs WHERE run_id = $1 FOR UPDATE%'
                 ORDER BY pid",
            )
            .bind(application_name)
            .fetch_all(&self.admin)
            .await
            .unwrap();
            if pids.len() >= 2 || Instant::now() >= deadline {
                return pids;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn terminate_backend(&self, pid: i32) {
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(&self.admin)
            .await
            .unwrap();
        assert!(terminated, "ownership backend {pid} was not terminated");
    }

    async fn cleanup(self) {
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        self.admin.close().await;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OwnershipRow {
    singleton: i16,
    generation: i64,
    owner_id: Option<String>,
    claimed_at: Option<DateTime<Utc>>,
}

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
    let Some(database_url) = postgres_database_url() else {
        return;
    };

    let suffix = Uuid::new_v4();
    let store = PostgresTestSchema::create(&database_url, "contract").await;
    let scoped_url = store.url(&format!("contract_{}", suffix.simple()));
    let (repo, owner) = PostgresRunRepository::connect_owned(
        &scoped_url,
        OWNERSHIP_OPERATION_TIMEOUT,
        OWNERSHIP_PROBE_TIMEOUT,
    )
    .await
    .unwrap();
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

    scoped_admin.close().await;
    drop(repo);
    owner.release().await.unwrap();
    store.cleanup().await;
}

#[tokio::test]
async fn postgres_independent_connections_resolve_one_authoritative_terminal() {
    let Some(database_url) = postgres_database_url() else {
        return;
    };

    let store = PostgresTestSchema::create(&database_url, "terminal_race").await;
    let repository_application = format!("terminal_race_{}", Uuid::new_v4().simple());
    let (repository, owner) = PostgresRunRepository::connect_owned(
        &store.url(&repository_application),
        OWNERSHIP_OPERATION_TIMEOUT,
        OWNERSHIP_PROBE_TIMEOUT,
    )
    .await
    .unwrap();
    let run_id = format!("terminal_race_{}", Uuid::new_v4().simple());
    repository.create_run(new_run(&run_id)).await.unwrap();
    repository.mark_running(&run_id, at(1)).await.unwrap();
    repository
        .append_events(&[
            event(&run_id, RunEventType::RunCreated, 1, None),
            event(&run_id, RunEventType::RunStarted, 2, None),
        ])
        .await
        .unwrap();

    let completed = TerminalProposal::new(scope(&run_id, None), completed_update(&run_id)).unwrap();
    let failed = TerminalProposal::new(scope(&run_id, None), failed_update(&run_id)).unwrap();
    let completed_requested = completed.event_at(3);
    let failed_requested = failed.event_at(3);

    let inspector = store.scoped_pool("terminal_race_inspector").await;
    let mut held_run_lock = inspector.begin().await.unwrap();
    let locked_run_id: String =
        sqlx::query_scalar("SELECT run_id FROM runs WHERE run_id = $1 FOR UPDATE")
            .bind(&run_id)
            .fetch_one(&mut *held_run_lock)
            .await
            .unwrap();
    assert_eq!(locked_run_id, run_id);

    let start = Arc::new(tokio::sync::Barrier::new(3));
    let first_repository = repository.clone();
    let first_start = start.clone();
    let first = tokio::spawn(async move {
        first_start.wait().await;
        first_repository
            .commit_terminal(completed, TerminalSequence::Expected(3))
            .await
    });
    let second_repository = repository.clone();
    let second_start = start.clone();
    let second = tokio::spawn(async move {
        second_start.wait().await;
        second_repository
            .commit_terminal(failed, TerminalSequence::Expected(3))
            .await
    });
    start.wait().await;

    let blocked_pids = store
        .wait_for_blocked_terminal_backends(&repository_application)
        .await;
    held_run_lock.rollback().await.unwrap();
    assert_eq!(
        blocked_pids.len(),
        2,
        "expected two independent repository backends waiting on the Run row; observed {blocked_pids:?}"
    );
    assert_ne!(blocked_pids[0], blocked_pids[1]);

    let (completed_result, failed_result) = tokio::time::timeout(OWNERSHIP_WAIT_TIMEOUT, async {
        let completed_result = first
            .await
            .expect("completed terminal task panicked")
            .expect("completed terminal proposal failed");
        let failed_result = second
            .await
            .expect("failed terminal task panicked")
            .expect("failed terminal proposal failed");
        (completed_result, failed_result)
    })
    .await
    .expect("terminal race did not resolve after releasing the Run row");
    assert_eq!(completed_result, failed_result);
    let completed_won = completed_result == completed_requested;
    let failed_won = failed_result == failed_requested;
    assert_ne!(completed_won, failed_won, "exactly one proposal must win");
    let authoritative = completed_result;

    let third = TerminalProposal::new(
        scope(&run_id, None),
        TerminalUpdate::new(
            &run_id,
            at(12),
            RunTerminal::Cancelled {
                error: StopError {
                    code: "RUN_CANCELLED".to_string(),
                    message: "run cancelled".to_string(),
                },
            },
        ),
    )
    .unwrap();
    let third_result = repository
        .commit_terminal(third, TerminalSequence::Expected(3))
        .await
        .unwrap();
    assert_eq!(third_result, authoritative);

    let record = repository.get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(record.ended_at, Some(authoritative.timestamp));
    assert_eq!(record.updated_at, authoritative.timestamp);
    if completed_won {
        assert_eq!(record.status(), RunStatus::Completed);
        assert!(matches!(
            record.lifecycle,
            RunLifecycle::Completed {
                output: RunOutput {
                    content: Some(ref content),
                    format: Some(ref format),
                    ref data,
                },
            } if content == "answer" && format == "text" && data == &json!({"answer":"answer"})
        ));
    } else {
        assert_eq!(record.status(), RunStatus::Failed);
        assert!(matches!(
            record.lifecycle,
            RunLifecycle::Failed {
                error: RunFailure {
                    kind: FailureKind::Infrastructure,
                    ref code,
                    ref message,
                },
            } if code == "INFRASTRUCTURE_FAILURE" && message == "runtime infrastructure failed"
        ));
    }

    let replay = repository.list_events_after(&run_id, 0, 100).await.unwrap();
    assert_eq!(
        replay.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(replay.last(), Some(&authoritative));
    assert_eq!(
        replay
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

    let (event_count, minimum_seq, maximum_seq, terminal_count): (
        i64,
        Option<i64>,
        Option<i64>,
        i64,
    ) = sqlx::query_as(
        "SELECT COUNT(*), MIN(seq), MAX(seq),
                COUNT(*) FILTER (
                    WHERE event_type IN (
                        'run.completed', 'run.failed', 'run.cancelled', 'run.interrupted'
                    )
                )
         FROM run_events
         WHERE run_id = $1",
    )
    .bind(&run_id)
    .fetch_one(&inspector)
    .await
    .unwrap();
    assert_eq!(
        (event_count, minimum_seq, maximum_seq, terminal_count),
        (3, Some(1), Some(3), 1)
    );

    let mut lock_probe = inspector.begin().await.unwrap();
    let generation: i64 = sqlx::query_scalar(
        "SELECT generation FROM runtime_ownership WHERE singleton = 1 FOR UPDATE NOWAIT",
    )
    .fetch_one(&mut *lock_probe)
    .await
    .unwrap();
    assert!(generation > 0);
    let probe_run_id: String =
        sqlx::query_scalar("SELECT run_id FROM runs WHERE run_id = $1 FOR UPDATE NOWAIT")
            .bind(&run_id)
            .fetch_one(&mut *lock_probe)
            .await
            .unwrap();
    assert_eq!(probe_run_id, run_id);
    lock_probe.rollback().await.unwrap();

    let idle_in_transaction: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM pg_stat_activity
         WHERE datname = current_database()
           AND application_name = $1
           AND state LIKE 'idle in transaction%'",
    )
    .bind(&repository_application)
    .fetch_one(&store.admin)
    .await
    .unwrap();
    assert_eq!(idle_in_transaction, 0);
    repository.check_health().await.unwrap();

    inspector.close().await;
    drop(repository);
    owner.release().await.unwrap();
    store.cleanup().await;
}

#[tokio::test]
async fn postgres_owner_claims_generation_and_rejects_a_same_store_contender() {
    let Some(database_url) = postgres_database_url() else {
        return;
    };
    let store = PostgresTestSchema::create(&database_url, "contender").await;
    let owner_application = format!("owner_{}", Uuid::new_v4().simple());
    let contender_application = format!("contender_{}", Uuid::new_v4().simple());

    let (repository, owner) = PostgresRunRepository::connect_owned(
        &store.url(&owner_application),
        OWNERSHIP_OPERATION_TIMEOUT,
        OWNERSHIP_PROBE_TIMEOUT,
    )
    .await
    .unwrap();
    repository.check_health().await.unwrap();
    let claimed = store.ownership_row().await;
    assert_eq!(claimed.singleton, 1);
    assert_eq!(claimed.generation, 1);
    assert!(claimed.owner_id.is_some());
    assert!(claimed.claimed_at.is_some());

    let contender_error = PostgresRunRepository::connect_owned(
        &store.url(&contender_application),
        OWNERSHIP_OPERATION_TIMEOUT,
        OWNERSHIP_PROBE_TIMEOUT,
    )
    .await
    .err()
    .expect("same-store contender unexpectedly acquired ownership");
    assert_eq!(contender_error.code(), "HISTORY_STORE_ALREADY_OWNED");
    assert_eq!(store.ownership_row().await, claimed);
    repository.check_health().await.unwrap();

    drop(repository);
    owner.release().await.unwrap();
    store.cleanup().await;
}

#[tokio::test]
async fn postgres_owners_for_different_schemas_can_run_concurrently() {
    let Some(database_url) = postgres_database_url() else {
        return;
    };
    let first_store = PostgresTestSchema::create(&database_url, "isolated_a").await;
    let second_store = PostgresTestSchema::create(&database_url, "isolated_b").await;
    let first_url = first_store.url(&format!("isolated_a_{}", Uuid::new_v4().simple()));
    let second_url = second_store.url(&format!("isolated_b_{}", Uuid::new_v4().simple()));

    let (first, second) = tokio::join!(
        PostgresRunRepository::connect_owned(
            &first_url,
            OWNERSHIP_OPERATION_TIMEOUT,
            OWNERSHIP_PROBE_TIMEOUT,
        ),
        PostgresRunRepository::connect_owned(
            &second_url,
            OWNERSHIP_OPERATION_TIMEOUT,
            OWNERSHIP_PROBE_TIMEOUT,
        ),
    );
    let (first_repository, first_owner) = first.unwrap();
    let (second_repository, second_owner) = second.unwrap();
    let first_claim = first_store.ownership_row().await;
    let second_claim = second_store.ownership_row().await;
    assert_eq!(first_claim.generation, 1);
    assert_eq!(second_claim.generation, 1);
    assert_ne!(first_claim.owner_id, second_claim.owner_id);

    let shared_run_id = format!("isolated_run_{}", Uuid::new_v4().simple());
    first_repository
        .create_run(new_run(&shared_run_id))
        .await
        .unwrap();
    second_repository
        .create_run(new_run(&shared_run_id))
        .await
        .unwrap();
    assert!(first_repository
        .get_run(&shared_run_id)
        .await
        .unwrap()
        .is_some());
    assert!(second_repository
        .get_run(&shared_run_id)
        .await
        .unwrap()
        .is_some());

    drop(first_repository);
    drop(second_repository);
    first_owner.release().await.unwrap();
    second_owner.release().await.unwrap();
    first_store.cleanup().await;
    second_store.cleanup().await;
}

#[tokio::test]
async fn postgres_clean_release_allows_exactly_one_generation_takeover() {
    let Some(database_url) = postgres_database_url() else {
        return;
    };
    let store = PostgresTestSchema::create(&database_url, "release").await;
    let first_url = store.url(&format!("release_a_{}", Uuid::new_v4().simple()));
    let second_url = store.url(&format!("release_b_{}", Uuid::new_v4().simple()));

    let (first_repository, first_owner) = PostgresRunRepository::connect_owned(
        &first_url,
        OWNERSHIP_OPERATION_TIMEOUT,
        OWNERSHIP_PROBE_TIMEOUT,
    )
    .await
    .unwrap();
    let first_claim = store.ownership_row().await;
    first_owner.release().await.unwrap();
    assert_ownership_lost(
        first_repository
            .create_run(new_run(&format!(
                "released_writer_{}",
                Uuid::new_v4().simple()
            )))
            .await
            .unwrap_err(),
    );

    let (second_repository, second_owner) = PostgresRunRepository::connect_owned(
        &second_url,
        OWNERSHIP_OPERATION_TIMEOUT,
        OWNERSHIP_PROBE_TIMEOUT,
    )
    .await
    .unwrap();
    let second_claim = store.ownership_row().await;
    assert_eq!(second_claim.generation, first_claim.generation + 1);
    assert_ne!(second_claim.owner_id, first_claim.owner_id);
    second_repository.check_health().await.unwrap();

    drop(first_repository);
    drop(second_repository);
    second_owner.release().await.unwrap();
    store.cleanup().await;
}

#[tokio::test]
async fn postgres_release_rechecks_the_token_and_fails_closed_after_tampering() {
    let Some(database_url) = postgres_database_url() else {
        return;
    };
    let store = PostgresTestSchema::create(&database_url, "release_fence").await;
    let application = format!("release_fence_{}", Uuid::new_v4().simple());
    let (repository, owner) = PostgresRunRepository::connect_owned(
        &store.url(&application),
        OWNERSHIP_OPERATION_TIMEOUT,
        OWNERSHIP_PROBE_TIMEOUT,
    )
    .await
    .unwrap();
    let mut loss = owner.subscribe_loss();
    let scoped = store.scoped_pool("release_fence_tamper").await;
    sqlx::query(
        "UPDATE runtime_ownership
         SET generation = generation + 1, owner_id = 'tampered-owner', claimed_at = CURRENT_TIMESTAMP
         WHERE singleton = 1",
    )
    .execute(&scoped)
    .await
    .unwrap();

    assert_ownership_lost(owner.release().await.unwrap_err());
    assert!(*loss.borrow_and_update());
    assert_ownership_lost(repository.check_health().await.unwrap_err());

    scoped.close().await;
    drop(repository);
    store.cleanup().await;
}

#[tokio::test]
async fn postgres_backend_loss_fences_all_old_writes_but_preserves_reads() {
    let Some(database_url) = postgres_database_url() else {
        return;
    };
    let store = PostgresTestSchema::create(&database_url, "loss").await;
    let old_application = format!("loss_old_{}", Uuid::new_v4().simple());
    let replacement_application = format!("loss_new_{}", Uuid::new_v4().simple());
    let (old_repository, old_owner) = PostgresRunRepository::connect_owned(
        &store.url(&old_application),
        OWNERSHIP_OPERATION_TIMEOUT,
        OWNERSHIP_PROBE_TIMEOUT,
    )
    .await
    .unwrap();
    let first_claim = store.ownership_row().await;
    let suffix = Uuid::new_v4();
    let readable_id = format!("loss_readable_{suffix}");
    let mark_id = format!("loss_mark_{suffix}");
    let event_id = format!("loss_event_{suffix}");
    let output_id = format!("loss_output_{suffix}");
    let terminal_id = format!("loss_terminal_{suffix}");
    for run_id in [&readable_id, &mark_id, &event_id, &output_id, &terminal_id] {
        old_repository.create_run(new_run(run_id)).await.unwrap();
    }
    old_repository
        .append_events(&[event(&readable_id, RunEventType::RunCreated, 1, None)])
        .await
        .unwrap();
    old_repository
        .append_events(&[event(&event_id, RunEventType::RunCreated, 1, None)])
        .await
        .unwrap();
    old_repository.mark_running(&event_id, at(1)).await.unwrap();
    old_repository
        .append_events(&[event(&terminal_id, RunEventType::RunCreated, 1, None)])
        .await
        .unwrap();
    old_repository
        .mark_running(&terminal_id, at(1))
        .await
        .unwrap();

    let mut ownership_loss = old_owner.subscribe_loss();
    let ownership_backend = store.wait_for_advisory_backend(&old_application).await;
    store.terminate_backend(ownership_backend).await;
    tokio::time::timeout(OWNERSHIP_WAIT_TIMEOUT, async {
        loop {
            if *ownership_loss.borrow() {
                break;
            }
            ownership_loss
                .changed()
                .await
                .expect("ownership loss sender stopped without publishing loss");
        }
    })
    .await
    .expect("ownership monitor did not publish backend loss");
    assert!(old_owner.is_lost());
    assert_ownership_lost(old_repository.check_health().await.unwrap_err());

    let (replacement, replacement_owner) = PostgresRunRepository::connect_owned(
        &store.url(&replacement_application),
        OWNERSHIP_OPERATION_TIMEOUT,
        OWNERSHIP_PROBE_TIMEOUT,
    )
    .await
    .unwrap();
    let replacement_claim = store.ownership_row().await;
    assert_eq!(replacement_claim.generation, first_claim.generation + 1);
    assert_ne!(replacement_claim.owner_id, first_claim.owner_id);

    assert_ownership_lost(
        old_repository
            .create_run(new_run(&format!("loss_create_{suffix}")))
            .await
            .unwrap_err(),
    );
    assert_ownership_lost(
        old_repository
            .mark_running(&mark_id, at(2))
            .await
            .unwrap_err(),
    );
    assert_ownership_lost(
        old_repository
            .append_events(&[event(&event_id, RunEventType::RunStarted, 2, None)])
            .await
            .unwrap_err(),
    );
    assert_ownership_lost(
        old_repository
            .put_node_output(NodeOutputRecord {
                run_id: output_id.clone(),
                node_id: "loss_output".to_string(),
                output: json!({"text":"stale"}),
                completed_at: at(2),
            })
            .await
            .unwrap_err(),
    );
    assert_ownership_lost(
        old_repository
            .commit_terminal(
                TerminalProposal::new(scope(&terminal_id, None), completed_update(&terminal_id))
                    .unwrap(),
                TerminalSequence::Expected(2),
            )
            .await
            .unwrap_err(),
    );
    assert_ownership_lost(
        old_repository
            .mark_incomplete_interrupted(at(20))
            .await
            .unwrap_err(),
    );
    assert_ownership_lost(old_repository.check_health().await.unwrap_err());

    assert!(old_repository
        .get_run(&readable_id)
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        old_repository
            .list_events_after(&readable_id, 0, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    let replacement_run = format!("replacement_write_{suffix}");
    replacement
        .create_run(new_run(&replacement_run))
        .await
        .unwrap();
    replacement.check_health().await.unwrap();
    assert!(replacement
        .get_run(&replacement_run)
        .await
        .unwrap()
        .is_some());

    drop(old_repository);
    drop(old_owner);
    drop(replacement);
    replacement_owner.release().await.unwrap();
    store.cleanup().await;
}

#[tokio::test]
async fn postgres_claim_timeout_is_bounded_sanitized_and_preserves_the_share_fence() {
    let Some(database_url) = postgres_database_url() else {
        return;
    };
    let store = PostgresTestSchema::create(&database_url, "barrier").await;
    let first_application = format!("barrier_a_{}", Uuid::new_v4().simple());
    let second_application = format!("barrier_b_{}", Uuid::new_v4().simple());
    let credential_sentinel = format!("credential_sentinel_{}", Uuid::new_v4().simple());
    let (first_repository, first_owner) = PostgresRunRepository::connect_owned(
        &store.url(&first_application),
        OWNERSHIP_OPERATION_TIMEOUT,
        OWNERSHIP_PROBE_TIMEOUT,
    )
    .await
    .unwrap();
    let first_claim = store.ownership_row().await;
    let barrier_pool = store.scoped_pool("ownership_share_barrier").await;
    let mut barrier = barrier_pool.begin().await.unwrap();
    let locked_generation: i64 = sqlx::query_scalar(
        "SELECT generation FROM runtime_ownership WHERE singleton = 1 FOR SHARE",
    )
    .fetch_one(&mut *barrier)
    .await
    .unwrap();
    assert_eq!(locked_generation, first_claim.generation);
    let schema_oid: i64 =
        sqlx::query_scalar("SELECT oid::bigint FROM pg_namespace WHERE nspname = current_schema()")
            .fetch_one(&barrier_pool)
            .await
            .unwrap();
    let advisory_key = (0x4941_5001_i64 << 32) | schema_oid;

    drop(first_repository);
    first_owner.release().await.unwrap();
    let timeout_url = store.url(&credential_sentinel);
    let started = Instant::now();
    let timed_out_claim = tokio::time::timeout(
        Duration::from_secs(2),
        PostgresRunRepository::connect_owned(
            &timeout_url,
            Duration::from_millis(75),
            OWNERSHIP_PROBE_TIMEOUT,
        ),
    )
    .await
    .expect("ownership claim did not respect its configured operation timeout");
    let claim_error = timed_out_claim
        .err()
        .expect("replacement claimed ownership while the previous write fence was held");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(claim_error.code(), "HISTORY_INIT_FAILED");
    assert_eq!(
        claim_error.to_string(),
        "PostgreSQL history ownership claim timed out"
    );
    let display = claim_error.to_string();
    let debug = format!("{claim_error:?}");
    let first_owner_id = first_claim.owner_id.as_deref().unwrap();
    let advisory_key = advisory_key.to_string();
    for forbidden in [
        timeout_url.as_str(),
        credential_sentinel.as_str(),
        first_owner_id,
        advisory_key.as_str(),
    ] {
        assert!(!display.contains(forbidden), "Display leaked {forbidden}");
        assert!(!debug.contains(forbidden), "Debug leaked {forbidden}");
    }
    assert_eq!(store.ownership_row().await, first_claim);

    barrier.rollback().await.unwrap();
    let second_url = store.url(&second_application);
    let (second_repository, second_owner) = tokio::time::timeout(
        OWNERSHIP_WAIT_TIMEOUT,
        PostgresRunRepository::connect_owned(
            &second_url,
            OWNERSHIP_OPERATION_TIMEOUT,
            OWNERSHIP_PROBE_TIMEOUT,
        ),
    )
    .await
    .expect("replacement did not complete after the share fence was released")
    .expect("replacement failed to acquire ownership after the share fence was released");
    let second_claim = store.ownership_row().await;
    assert_eq!(second_claim.generation, first_claim.generation + 1);
    assert_ne!(second_claim.owner_id, first_claim.owner_id);

    barrier_pool.close().await;
    drop(second_repository);
    second_owner.release().await.unwrap();
    store.cleanup().await;
}

fn assert_ownership_lost(error: HistoryError) {
    assert_eq!(error.code(), "HISTORY_OWNERSHIP_LOST", "{error:?}");
}
