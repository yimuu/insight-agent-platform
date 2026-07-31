mod support;

use chrono::{DateTime, Duration, Utc};
use insight_durable::{
    ClaimMcpRemoteTasksCommand, CreateMcpRemoteTaskCommand, FinalizeMcpRemoteTaskCommand,
    McpInteractionPrincipal, McpRemoteTaskDurableRepository, McpRemoteTaskId, McpRemoteTaskStatus,
    McpSecretCiphertext, ObserveMcpRemoteTaskCommand,
};
use insight_engine::TransitionOutcome;
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use uuid::Uuid;

fn now() -> DateTime<Utc> {
    "2026-07-30T12:00:00Z".parse().unwrap()
}

fn create() -> CreateMcpRemoteTaskCommand {
    create_named("remote-task-local-1")
}

fn create_named(task_id: &str) -> CreateMcpRemoteTaskCommand {
    CreateMcpRemoteTaskCommand::new(
        McpRemoteTaskId::new(task_id).unwrap(),
        McpInteractionPrincipal::new("tenant-a", "user-a").unwrap(),
        "run-a",
        task_id,
        format!("run-a:{task_id}"),
        "calendar",
        "a".repeat(64),
        "2026-07-28",
        "io.modelcontextprotocol/tasks",
        McpSecretCiphertext::new("enc:v1:opaque-remote-task").unwrap(),
        "b".repeat(64),
        McpSecretCiphertext::new("enc:v1:initial-task-payload").unwrap(),
        "c".repeat(64),
        now(),
        now(),
        now() + Duration::minutes(1),
        500,
        now(),
    )
    .unwrap()
}

async fn exercise_repository<R>(repository: R)
where
    R: McpRemoteTaskDurableRepository,
{
    let task_id = McpRemoteTaskId::new("remote-task-local-1").unwrap();
    assert!(matches!(
        repository.create_mcp_remote_task(create()).await.unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository.create_mcp_remote_task(create()).await.unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    let secret = repository
        .load_mcp_remote_task_secret(&task_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!format!("{secret:?}").contains("opaque-remote-task"));

    let first_claim =
        ClaimMcpRemoteTasksCommand::new("worker-a", now(), now() + Duration::seconds(10), 10)
            .unwrap();
    let claims = repository
        .claim_mcp_remote_tasks(first_claim)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].lease_epoch, 1);
    assert!(repository
        .claim_mcp_remote_tasks(
            ClaimMcpRemoteTasksCommand::new(
                "worker-b",
                now() + Duration::seconds(1),
                now() + Duration::seconds(11),
                10,
            )
            .unwrap()
        )
        .await
        .unwrap()
        .is_empty());

    let stale = ObserveMcpRemoteTaskCommand::new(
        task_id.clone(),
        "observe-stale",
        "worker-b",
        1,
        1,
        McpRemoteTaskStatus::Working,
        now() + Duration::seconds(1),
        500,
        Some(now() + Duration::seconds(2)),
        McpSecretCiphertext::new("enc:v1:working").unwrap(),
        "d".repeat(64),
        None,
        now() + Duration::seconds(1),
    )
    .unwrap();
    assert!(matches!(
        repository.observe_mcp_remote_task(stale).await.unwrap(),
        TransitionOutcome::StateConflict
    ));

    let working = ObserveMcpRemoteTaskCommand::new(
        task_id.clone(),
        "observe-working",
        "worker-a",
        1,
        1,
        McpRemoteTaskStatus::Working,
        now() + Duration::seconds(1),
        500,
        Some(now() + Duration::seconds(2)),
        McpSecretCiphertext::new("enc:v1:working").unwrap(),
        "d".repeat(64),
        None,
        now() + Duration::seconds(1),
    )
    .unwrap();
    assert!(matches!(
        repository
            .observe_mcp_remote_task(working.clone())
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository.observe_mcp_remote_task(working).await.unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));

    let claims = repository
        .claim_mcp_remote_tasks(
            ClaimMcpRemoteTasksCommand::new(
                "worker-b",
                now() + Duration::seconds(2),
                now() + Duration::seconds(12),
                10,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].lease_epoch, 2);
    let completed = ObserveMcpRemoteTaskCommand::new(
        task_id.clone(),
        "observe-completed",
        "worker-b",
        2,
        2,
        McpRemoteTaskStatus::Completed,
        now() + Duration::seconds(3),
        500,
        None,
        McpSecretCiphertext::new("enc:v1:completed").unwrap(),
        "e".repeat(64),
        Some("f".repeat(64)),
        now() + Duration::seconds(3),
    )
    .unwrap();
    let TransitionOutcome::Committed { result } =
        repository.observe_mcp_remote_task(completed).await.unwrap()
    else {
        panic!("terminal observation must win");
    };
    assert_eq!(result.status(), McpRemoteTaskStatus::Completed);
    assert_eq!(result.version(), 3);
    assert!(result.next_poll_at().is_none());
    assert!(result.terminal_receipt_hash().is_some());
    assert!(repository
        .claim_mcp_remote_tasks(
            ClaimMcpRemoteTasksCommand::new(
                "worker-c",
                now() + Duration::seconds(20),
                now() + Duration::seconds(30),
                10,
            )
            .unwrap()
        )
        .await
        .unwrap()
        .is_empty());

    let cancelled_id = McpRemoteTaskId::new("remote-task-local-cancel").unwrap();
    repository
        .create_mcp_remote_task(create_named(cancelled_id.as_str()))
        .await
        .unwrap();
    let cancelled = FinalizeMcpRemoteTaskCommand::new(
        cancelled_id.clone(),
        "local-cancel",
        McpRemoteTaskStatus::Cancelled,
        McpSecretCiphertext::new("enc:v1:cancelled").unwrap(),
        "1".repeat(64),
        "2".repeat(64),
        now() + Duration::seconds(1),
    )
    .unwrap();
    assert!(matches!(
        repository
            .finalize_mcp_remote_task(cancelled.clone())
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .finalize_mcp_remote_task(cancelled)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    assert_eq!(
        repository
            .load_mcp_remote_task(&cancelled_id)
            .await
            .unwrap()
            .unwrap()
            .status(),
        McpRemoteTaskStatus::Cancelled
    );

    let expiry_id = McpRemoteTaskId::new("remote-task-local-expiry").unwrap();
    repository
        .create_mcp_remote_task(create_named(expiry_id.as_str()))
        .await
        .unwrap();
    let early_expiry = FinalizeMcpRemoteTaskCommand::new(
        expiry_id.clone(),
        "local-expiry",
        McpRemoteTaskStatus::Expired,
        McpSecretCiphertext::new("enc:v1:expired").unwrap(),
        "3".repeat(64),
        "4".repeat(64),
        now() + Duration::seconds(1),
    )
    .unwrap();
    assert!(matches!(
        repository
            .finalize_mcp_remote_task(early_expiry)
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
    let expiry = FinalizeMcpRemoteTaskCommand::new(
        expiry_id.clone(),
        "local-expiry",
        McpRemoteTaskStatus::Expired,
        McpSecretCiphertext::new("enc:v1:expired").unwrap(),
        "3".repeat(64),
        "4".repeat(64),
        now() + Duration::minutes(1),
    )
    .unwrap();
    assert!(matches!(
        repository.finalize_mcp_remote_task(expiry).await.unwrap(),
        TransitionOutcome::Committed { .. }
    ));
}

async fn isolated_postgres_repository(
) -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("mcp_remote_tasks_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    support::provision_postgres_schema(&control).await;
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    Some((repository, control, admin, schema))
}

#[tokio::test]
async fn sqlite_remote_tasks_are_durable_leased_fenced_and_terminal_once() {
    let (_temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    exercise_repository(repository).await;
}

#[tokio::test]
async fn postgres_remote_tasks_are_durable_leased_fenced_and_terminal_once() {
    let Some((repository, control, admin, schema)) = isolated_postgres_repository().await else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set TEST_POSTGRES_URL for PostgreSQL MCP Tasks conformance"
        );
        return;
    };
    exercise_repository(repository).await;
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
