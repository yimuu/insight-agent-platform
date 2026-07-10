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

    let suffix = Uuid::new_v4();
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let schema = format!("history_fresh_{}", suffix.simple());
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin_pool)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let store = RunHistoryStore::postgres(&scoped_url).await.unwrap();
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
        .record_event(&RunEvent::ok(
            RunEventType::StepCompleted,
            1,
            RunEventScope {
                request_id: request.request_id.clone(),
                run_id: completed_run_id.clone(),
                agent_id: "agent-pg".to_string(),
                step_id: Some("answer".to_string()),
            },
            json!({"step_id":"answer", "status":"completed"}),
        ))
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
            request_id: Some(request.request_id.clone()),
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

    let completed = store.get_run(&completed_run_id).await.unwrap().unwrap();
    assert_eq!(completed.events.len(), 1);
    assert_eq!(completed.events[0].event_type, "step.completed");
    assert_eq!(completed.events[0].seq, 1);
    assert_eq!(completed.events[0].request_id, request.request_id);
    assert_eq!(completed.events[0].data["step_id"], "answer");

    drop(store);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin_pool)
        .await
        .unwrap();
}
