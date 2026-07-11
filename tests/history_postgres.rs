use chrono::{DateTime, TimeZone, Utc};
use insight_agent_platform::{
    dsl::compiled::RunOutput,
    events::protocol::{RunEvent, RunEventScope, RunEventType},
    history::{
        postgres::PostgresRunRepository,
        repository::RunRepository,
        types::{
            summarize_input, NewRun, NodeOutputRecord, RunAttachment, RunStatus, TerminalUpdate,
        },
    },
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

    assert!(repo
        .finish_run(
            completed_update(&run_id),
            event(&run_id, RunEventType::RunCompleted, 5, None),
        )
        .await
        .unwrap());
    let losing_update = TerminalUpdate::new(
        &run_id,
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
                scope(&run_id, None),
                at(11),
                "RUN_CANCELLED",
                "run cancelled",
                json!({}),
            ),
        )
        .await
        .unwrap());
    let record = repo.get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(record.status, RunStatus::Completed);
    assert_eq!(record.agent_version, "sha256:postgres");
    assert_eq!(record.output.unwrap().content.as_deref(), Some("answer"));
    assert_eq!(
        repo.list_events_after(&run_id, 0, 100)
            .await
            .unwrap()
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );

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
    let failed = RunEvent::error_at(
        RunEventType::RunFailed,
        2,
        scope(&recovery_id, None),
        at(10),
        "INFRASTRUCTURE_FAILURE",
        "runtime infrastructure failed",
        json!({}),
    );
    let recovered = repo
        .recover_run(failed_update(&recovery_id), failed)
        .await
        .unwrap();
    assert_eq!(recovered.seq, 2);
    assert_eq!(
        repo.get_run(&recovery_id).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
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
    let proposal = RunEvent::error_at(
        RunEventType::RunFailed,
        1,
        scope(&conflict_id, None),
        at(10),
        "INFRASTRUCTURE_FAILURE",
        "runtime infrastructure failed",
        json!({}),
    );
    let terminal = repo
        .recover_run(failed_update(&conflict_id), proposal)
        .await
        .unwrap();
    assert_eq!(terminal.seq, 3);
    assert_eq!(
        repo.get_run(&conflict_id).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
    assert_eq!(
        repo.list_events_after(&conflict_id, 0, 100)
            .await
            .unwrap()
            .len(),
        3
    );

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
        repo.get_run("created_pg").await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
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
