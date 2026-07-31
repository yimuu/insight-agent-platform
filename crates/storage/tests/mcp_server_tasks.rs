mod support;

use chrono::{DateTime, Duration, Utc};
use insight_durable::{McpInteractionPrincipal, McpServerTask, McpServerTaskDurableRepository};
use insight_storage::{PostgresDurableRepository, SqliteDurableRepository};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use uuid::Uuid;

fn now() -> DateTime<Utc> {
    "2026-07-30T12:00:00Z".parse().unwrap()
}

fn principal(user_id: &str) -> McpInteractionPrincipal {
    McpInteractionPrincipal::new("tenant-a", user_id).unwrap()
}

async fn exercise_repository<R>(repository: R)
where
    R: McpServerTaskDurableRepository,
{
    let task = McpServerTask::new(
        "task-opaque",
        principal("user-a"),
        "run-terminal-or-full",
        "agent-a",
        now(),
        now() + Duration::hours(1),
    )
    .unwrap();
    assert!(repository
        .create_mcp_server_task(task.clone())
        .await
        .unwrap());
    assert!(!repository
        .create_mcp_server_task(task.clone())
        .await
        .unwrap());
    assert_eq!(
        repository
            .load_mcp_server_task(&principal("user-a"), task.task_id())
            .await
            .unwrap(),
        Some(task.clone())
    );
    assert!(repository
        .load_mcp_server_task(&principal("user-b"), "task-opaque")
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .list_expired_mcp_server_tasks(now() + Duration::minutes(30), 16)
        .await
        .unwrap()
        .is_empty());
    assert!(repository
        .list_expired_mcp_server_tasks(now() + Duration::hours(2), 0)
        .await
        .is_err());
    let expired = repository
        .list_expired_mcp_server_tasks(now() + Duration::hours(2), 16)
        .await
        .unwrap();
    assert_eq!(expired, vec![task.clone()]);
    assert!(!repository
        .delete_expired_mcp_server_task(
            task.task_id(),
            task.expires_at() + Duration::seconds(1),
            now() + Duration::hours(2),
        )
        .await
        .unwrap());
    assert!(repository
        .delete_expired_mcp_server_task(
            task.task_id(),
            task.expires_at(),
            now() + Duration::hours(2),
        )
        .await
        .unwrap());
    assert!(repository
        .load_mcp_server_task(&principal("user-a"), task.task_id())
        .await
        .unwrap()
        .is_none());
}

async fn isolated_postgres_repository(
) -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("mcp_server_tasks_{}", Uuid::new_v4().simple());
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
async fn sqlite_mcp_server_tasks_are_principal_scoped_and_idempotent() {
    let (_temporary, repository): (_, SqliteDurableRepository) =
        support::temporary_sqlite_repository().await;
    exercise_repository(repository).await;
}

#[tokio::test]
async fn postgres_mcp_server_tasks_are_principal_scoped_and_idempotent() {
    let Some((repository, control, admin, schema)) = isolated_postgres_repository().await else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set TEST_POSTGRES_URL for PostgreSQL MCP task conformance"
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
