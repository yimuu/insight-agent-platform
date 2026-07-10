use chrono::Utc;
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe};
use uuid::Uuid;

use insight_agent_platform::{
    engine::event::{RunEvent, RunEventScope, RunEventType},
    history::store::{RunHistoryQuery, RunHistoryStore, RunStatus},
    request_context::RequestContext,
};

#[tokio::test]
async fn postgres_history_store_records_and_filters_runs_when_configured() {
    let Some(database_url) = std::env::var("RUN_HISTORY_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("skipping postgres history test: RUN_HISTORY_POSTGRES_URL is not set");
        return;
    };

    let store = RunHistoryStore::postgres(&database_url).await.unwrap();
    let suffix = Uuid::new_v4();
    let completed_run_id = format!("run_pg_{suffix}_completed");
    let failed_run_id = format!("run_pg_{suffix}_failed");
    let request = RequestContext {
        request_id: format!("req_pg_{suffix}"),
        caller_service: Some("integration-test".to_string()),
        tenant_id: Some("tenant-pg".to_string()),
        user_id: Some("user-pg".to_string()),
    };

    store
        .create_run(
            &completed_run_id,
            "agent-pg",
            &request,
            Utc::now(),
            json!({"case": "completed"}),
        )
        .await;
    store
        .finish_run(&completed_run_id, RunStatus::Completed, None)
        .await;
    store
        .create_run(
            &failed_run_id,
            "agent-pg",
            &request,
            Utc::now(),
            json!({"case": "failed"}),
        )
        .await;
    store
        .finish_run(
            &failed_run_id,
            RunStatus::Failed,
            Some("synthetic failure".to_string()),
        )
        .await;

    let page = store
        .list_runs_page(RunHistoryQuery {
            agent_id: Some("agent-pg".to_string()),
            request_id: Some(request.request_id),
            status: Some(RunStatus::Failed),
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].run_id, failed_run_id);
    assert_eq!(page.items[0].status, RunStatus::Failed);
    assert_eq!(
        page.items[0].caller_service.as_deref(),
        Some("integration-test")
    );
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn postgres_migration_preserves_legacy_run_events_when_configured() {
    let Some(database_url) = std::env::var("RUN_HISTORY_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        eprintln!("skipping postgres migration test: RUN_HISTORY_POSTGRES_URL is not set");
        return;
    };

    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let schema = format!("history_migration_{}", Uuid::new_v4().simple());
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin_pool)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let legacy_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&scoped_url)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE runs (
            run_id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            input_summary TEXT NOT NULL,
            error_message TEXT
        );
        CREATE TABLE run_events (
            id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            event TEXT NOT NULL,
            step_id TEXT,
            timestamp TEXT NOT NULL,
            content TEXT NOT NULL,
            result TEXT NOT NULL,
            code INTEGER NOT NULL,
            message TEXT NOT NULL
        );
        CREATE TABLE step_outputs (
            run_id TEXT NOT NULL,
            step_id TEXT NOT NULL,
            output TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (run_id, step_id)
        );",
    )
    .execute(&legacy_pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO runs (run_id, agent_id, status, started_at, input_summary)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind("run_legacy")
    .bind("agent-a")
    .bind("completed")
    .bind(Utc::now().to_rfc3339())
    .bind("{}")
    .execute(&legacy_pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO run_events (
            run_id, event, step_id, timestamp, content, result, code, message
         ) VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8),
            ($9, $10, $11, $12, $13, $14, $15, $16)",
    )
    .bind("run_legacy")
    .bind("token_delta")
    .bind("generate")
    .bind(Utc::now().to_rfc3339())
    .bind("hello")
    .bind("null")
    .bind(0)
    .bind("ok")
    .bind("run_legacy")
    .bind("run_completed")
    .bind(Option::<String>::None)
    .bind(Utc::now().to_rfc3339())
    .bind("")
    .bind("null")
    .bind(0)
    .bind("ok")
    .execute(&legacy_pool)
    .await
    .unwrap();
    legacy_pool.close().await;

    let store = RunHistoryStore::postgres(&scoped_url).await.unwrap();
    let run = store.get_run("run_legacy").await.unwrap().unwrap();

    assert_eq!(run.request_id, "");
    assert_eq!(run.events.len(), 2);
    assert_eq!(run.events[0].event_type, "content.delta");
    assert_eq!(run.events[0].seq, 1);
    assert_eq!(run.events[0].run_id, "run_legacy");
    assert_eq!(run.events[0].agent_id, "agent-a");
    assert_eq!(run.events[0].data["step_id"], "generate");
    assert_eq!(run.events[0].data["content"], "hello");
    assert_eq!(run.events[1].event_type, "run.completed");
    assert_eq!(run.events[1].seq, 2);
    assert_eq!(run.events[1].data["status"], "completed");
    assert_eq!(run.events[1].data["content"], "hello");
    assert!(run.events[1].data["output"].is_null());

    let request = RequestContext {
        request_id: "req_after_migration".to_string(),
        ..Default::default()
    };
    store
        .create_run(
            "run_after_migration",
            "agent-a",
            &request,
            Utc::now(),
            json!({}),
        )
        .await;
    store
        .record_event(&RunEvent::ok(
            RunEventType::ContentDelta,
            1,
            RunEventScope {
                request_id: request.request_id.clone(),
                run_id: "run_after_migration".to_string(),
                agent_id: "agent-a".to_string(),
                step_id: Some("answer".to_string()),
            },
            json!({"step_id":"answer", "content":"new"}),
        ))
        .await;
    store
        .finish_run("run_after_migration", RunStatus::Completed, None)
        .await;
    let new_run = store.get_run("run_after_migration").await.unwrap().unwrap();
    assert_eq!(new_run.events[0].request_id, "req_after_migration");
    assert_eq!(new_run.events[0].data["step_id"], "answer");

    drop(store);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin_pool)
        .await
        .unwrap();
}
