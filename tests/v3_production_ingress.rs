use std::{collections::BTreeSet, sync::Arc, time::Duration};

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use insight_agent_platform::{
    api::formal::{build_router, ApiAuth, BearerHumanPrincipalResolver, FormalApiState},
    catalog_v3::{
        compile_enabled_v3_agents, deploy_v3_agents, LeafDeploymentResolver, ResolvedLeafDeployment,
    },
    dsl::CompileError,
    engine::{
        plan::LeafTaskDescriptor,
        repository::{
            CompleteHumanWorkItemCommand, HumanTaskDurableRepository, HumanTaskPrincipal,
            HumanWorkItemId, ProjectionDurableRepository, ProjectionSubject, ProjectionSubjectKind,
            SqliteDurableRepository,
        },
        LeafTaskKind, TransitionOutcome, VersionTag, WorkerExecutorRegistry,
    },
    history::types::RunStatus,
    runtime::{
        DeployedAgentCatalog, ProductionRunRepository, RequestMetadata, RunService,
        RunServiceConfig,
    },
};
use serde_json::json;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Row, SqlitePool};
use tower::ServiceExt;

struct NoLeafResolver;

impl LeafDeploymentResolver for NoLeafResolver {
    fn resolve_leaf(
        &self,
        _kind: LeafTaskKind,
        _descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        ResolvedLeafDeployment::new(VersionTag::new("unused-worker").unwrap(), json!({}))
    }
}

fn write_agents(root: &std::path::Path) {
    let human = root.join("human_gate");
    std::fs::create_dir_all(&human).unwrap();
    let human_source = r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: human_gate
  name: Human gate
  description: Production human-task ingress gate.
types:
  Approval:
    fields:
      decision: {type: string, enum: [approved, rejected]}
inputs: {}
output: Approval
workflow:
  steps:
    - id: review
      human_task:
        signal: medical_review
        request: {kind: medical_report, report_id: report-1}
        response: Approval
        candidate_groups: [medical-reviewers]
        claim_lease_ms: 60000
    - return: $review
"#;
    std::fs::write(human.join("agent.yaml"), human_source).unwrap();
    let other = root.join("human_gate_other");
    std::fs::create_dir_all(&other).unwrap();
    std::fs::write(
        other.join("agent.yaml"),
        human_source
            .replace("id: human_gate", "id: human_gate_other")
            .replace("name: Human gate", "name: Other human gate")
            .replace("medical-reviewers", "other-reviewers"),
    )
    .unwrap();
    let short_lease = root.join("human_gate_short_lease");
    std::fs::create_dir_all(&short_lease).unwrap();
    std::fs::write(
        short_lease.join("agent.yaml"),
        human_source
            .replace("id: human_gate", "id: human_gate_short_lease")
            .replace("name: Human gate", "name: Short lease human gate")
            .replace("claim_lease_ms: 60000", "claim_lease_ms: 50"),
    )
    .unwrap();

    let timer = root.join("timer_gate");
    std::fs::create_dir_all(&timer).unwrap();
    std::fs::write(
        timer.join("agent.yaml"),
        r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: timer_gate
  name: Timer gate
  description: Production database-clock timer gate.
inputs: {}
output: string
workflow:
  steps:
    - id: delay
      wait: {duration_ms: 250}
    - return: timer-finished
"#,
    )
    .unwrap();
}

async fn setup() -> (tempfile::TempDir, RunService, SqlitePool) {
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_agents(&agents_root);
    let enabled = BTreeSet::from([
        "human_gate".to_owned(),
        "human_gate_other".to_owned(),
        "human_gate_short_lease".to_owned(),
        "timer_gate".to_owned(),
    ]);
    let published = compile_enabled_v3_agents(&agents_root, &enabled).unwrap();
    let deployed = deploy_v3_agents(&published, &NoLeafResolver).unwrap();
    let agents = DeployedAgentCatalog::new(deployed).unwrap();
    let database = temporary.path().join("runtime-ingress.sqlite");
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(8, 2, 1, 32),
    )
    .await
    .unwrap();
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(false)
            .disable_statement_logging(),
    )
    .await
    .unwrap();
    (temporary, service, control)
}

async fn wait_for_completed(service: &RunService, run_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let record = service.get_run(run_id).await.unwrap();
        if record.status().is_terminal() {
            assert_eq!(record.status(), RunStatus::Completed);
            return;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_human_item(
    service: &RunService,
    run_id: &str,
    identity: &str,
    groups: Vec<String>,
) -> insight_agent_platform::engine::repository::HumanWorkItem {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(item) = service
            .list_human_tasks(identity, groups.clone(), 100)
            .await
            .unwrap()
            .into_iter()
            .find(|item| item.run_id().as_str() == run_id)
        {
            return item;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn human_work_item_is_assigned_fenced_typed_and_idempotent_while_paused() {
    let (temporary, service, control) = setup().await;
    let created = service
        .create_detached(
            "human_gate",
            json!({}),
            RequestMetadata {
                request_id: Some("human-ingress-1".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(created.status(), RunStatus::Running);
    let item = wait_for_human_item(
        &service,
        &created.run_id,
        "reviewer",
        vec!["medical-reviewers".to_owned()],
    )
    .await;
    let bypass = service
        .signal(
            &created.run_id,
            "medical_review",
            "human-generic-signal-bypass",
            json!({"decision": "approved"}),
        )
        .await
        .unwrap_err();
    assert_eq!(bypass.code(), "SIGNAL_NOT_WAITING");
    service.pause(&created.run_id).await.unwrap();
    assert!(service
        .list_human_tasks("outsider", vec!["other".to_owned()], 10)
        .await
        .unwrap()
        .is_empty());
    let work_item_id = item.work_item_id().as_str().to_owned();
    let claim = service
        .claim_human_task(
            &work_item_id,
            "reviewer",
            vec!["medical-reviewers".to_owned()],
            "human-claim-1",
        )
        .await
        .unwrap();
    assert_eq!(claim.claimed_by(), Some("reviewer"));

    let mismatch = service
        .complete_human_task(
            &work_item_id,
            "reviewer",
            vec!["medical-reviewers".to_owned()],
            "human-message-wrong-type",
            claim.claim_fence(),
            json!("approved"),
        )
        .await
        .unwrap_err();
    assert_eq!(mismatch.code(), "HUMAN_TASK_COMPLETION_CONFLICT");

    let completed_item = service
        .complete_human_task(
            &work_item_id,
            "reviewer",
            vec!["medical-reviewers".to_owned()],
            "human-message-1",
            claim.claim_fence(),
            json!({"decision": "approved"}),
        )
        .await
        .unwrap();
    assert_eq!(
        completed_item.state(),
        insight_agent_platform::engine::repository::HumanWorkItemState::Completed
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT admission_state FROM workflow_runs WHERE run_id=?",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        "paused"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT signal_state FROM signals_inbox WHERE run_id=?")
            .bind(&created.run_id)
            .fetch_one(&control)
            .await
            .unwrap(),
        "consumed"
    );

    service.resume(&created.run_id).await.unwrap();
    wait_for_completed(&service, &created.run_id).await;
    let replay = service
        .complete_human_task(
            &work_item_id,
            "reviewer",
            vec!["medical-reviewers".to_owned()],
            "human-message-1",
            claim.claim_fence(),
            json!({"decision": "approved"}),
        )
        .await
        .unwrap();
    assert_eq!(
        replay.state(),
        insight_agent_platform::engine::repository::HumanWorkItemState::Completed
    );
    let cross_principal_replay = service
        .complete_human_task(
            &work_item_id,
            "other-reviewer",
            vec!["medical-reviewers".to_owned()],
            "human-message-1",
            claim.claim_fence(),
            json!({"decision": "approved"}),
        )
        .await
        .unwrap_err();
    assert_eq!(
        cross_principal_replay.code(),
        "HUMAN_TASK_COMPLETION_CONFLICT"
    );
    let conflict = service
        .complete_human_task(
            &work_item_id,
            "reviewer",
            vec!["medical-reviewers".to_owned()],
            "human-message-1",
            claim.claim_fence(),
            json!({"decision": "rejected"}),
        )
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), "HUMAN_TASK_COMPLETION_CONFLICT");
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    let repair_repository =
        SqliteDurableRepository::connect_path(&temporary.path().join("runtime-ingress.sqlite"))
            .await
            .unwrap();
    let run_id = insight_agent_platform::engine::RunId::new(created.run_id.clone()).unwrap();
    let signal_id: String =
        sqlx::query_scalar("SELECT signal_id FROM signals_inbox WHERE run_id=?")
            .bind(&created.run_id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM projection_checkpoints
             WHERE run_id=? AND subject_kind IN ('signal','task_outbox','human_work_item')",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        0,
        "durable inbox/outbox/work-item authorities must not enter the repair ledger"
    );

    sqlx::query("UPDATE human_work_items SET request_value=json(?) WHERE work_item_id=?")
        .bind(r#"{"authority_marker":true}"#)
        .bind(&work_item_id)
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("UPDATE signals_inbox SET signal_name=? WHERE signal_id=?")
        .bind("authority-marker")
        .bind(&signal_id)
        .execute(&control)
        .await
        .unwrap();
    repair_repository
        .repair_all_projections(&run_id)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT request_value FROM human_work_items WHERE work_item_id=?",
        )
        .bind(&work_item_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        r#"{"authority_marker":true}"#
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT signal_name FROM signals_inbox WHERE signal_id=?")
            .bind(&signal_id)
            .fetch_one(&control)
            .await
            .unwrap(),
        "authority-marker"
    );
    assert_eq!(
        sqlx::query("DELETE FROM human_work_items WHERE work_item_id=?")
            .bind(&work_item_id)
            .execute(&control)
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    let mut fault = control.acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys=OFF")
        .execute(&mut *fault)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query("DELETE FROM signals_inbox WHERE signal_id=?")
            .bind(&signal_id)
            .execute(&mut *fault)
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    drop(fault);
    repair_repository
        .repair_all_projections(&run_id)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM human_work_items WHERE work_item_id=?",)
            .bind(&work_item_id)
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
        "repair must not recreate a deleted human-task completion authority"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM signals_inbox WHERE signal_id=?")
            .bind(&signal_id)
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
        "repair must not recreate a deleted signal receipt authority"
    );
}

#[tokio::test]
async fn human_work_item_concurrent_claim_has_one_winner_and_cancel_closes_it() {
    let (_temporary, service, control) = setup().await;
    let created = service
        .create_detached("human_gate", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    let item = wait_for_human_item(
        &service,
        &created.run_id,
        "alice",
        vec!["medical-reviewers".to_owned()],
    )
    .await;
    let id = item.work_item_id().as_str().to_owned();
    let left_service = service.clone();
    let left_id = id.clone();
    let left = tokio::spawn(async move {
        left_service
            .claim_human_task(
                &left_id,
                "alice",
                vec!["medical-reviewers".to_owned()],
                "claim-alice",
            )
            .await
    });
    let right_service = service.clone();
    let right_id = id.clone();
    let right = tokio::spawn(async move {
        right_service
            .claim_human_task(
                &right_id,
                "bob",
                vec!["medical-reviewers".to_owned()],
                "claim-bob",
            )
            .await
    });
    let (left, right) = tokio::join!(left, right);
    assert_eq!(
        [left.unwrap().is_ok(), right.unwrap().is_ok()]
            .into_iter()
            .filter(|won| *won)
            .count(),
        1
    );

    service.cancel(&created.run_id).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let state = sqlx::query_scalar::<_, String>(
            "SELECT work_state FROM human_work_items WHERE work_item_id=?",
        )
        .bind(&id)
        .fetch_one(&control)
        .await
        .unwrap();
        if state == "cancelled" {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn sqlite_human_claim_lease_reopens_with_subsecond_database_clock_precision() {
    let (_temporary, service, _control) = setup().await;
    let created = service
        .create_detached(
            "human_gate_short_lease",
            json!({}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let item = wait_for_human_item(
        &service,
        &created.run_id,
        "alice",
        vec!["medical-reviewers".to_owned()],
    )
    .await;
    let id = item.work_item_id().as_str().to_owned();
    let alice_claim = service
        .claim_human_task(
            &id,
            "alice",
            vec!["medical-reviewers".to_owned()],
            "sqlite-short-claim-alice",
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(90)).await;
    let visible = service
        .list_human_tasks("bob", vec!["medical-reviewers".to_owned()], 10)
        .await
        .unwrap();
    assert_eq!(visible.len(), 1);
    let bob_claim = service
        .claim_human_task(
            &id,
            "bob",
            vec!["medical-reviewers".to_owned()],
            "sqlite-short-claim-bob",
        )
        .await
        .unwrap();
    assert!(bob_claim.claim_fence() > alice_claim.claim_fence());
    let stale = service
        .complete_human_task(
            &id,
            "alice",
            vec!["medical-reviewers".to_owned()],
            "sqlite-stale-alice-completion",
            alice_claim.claim_fence(),
            json!({"decision": "approved"}),
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code(), "HUMAN_TASK_COMPLETION_CONFLICT");
    service.cancel(&created.run_id).await.unwrap();
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn human_work_item_visibility_filter_precedes_limit() {
    let (_temporary, service, _control) = setup().await;
    for _ in 0..3 {
        service
            .create_detached("human_gate_other", json!({}), RequestMetadata::default())
            .await
            .unwrap();
    }
    let visible_run = service
        .create_detached("human_gate", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_human_item(
        &service,
        &visible_run.run_id,
        "reviewer",
        vec!["medical-reviewers".to_owned()],
    )
    .await;
    let items = service
        .list_human_tasks("reviewer", vec!["medical-reviewers".to_owned()], 1)
        .await
        .unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].run_id().as_str(), visible_run.run_id);
    assert_eq!(
        items[0].request(),
        &json!({"kind": "medical_report", "report_id": "report-1"})
    );
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn human_principal_tokens_are_request_scoped_and_cannot_call_general_run_routes() {
    let (_temporary, service, _control) = setup().await;
    let created = service
        .create_detached("human_gate", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    let item = wait_for_human_item(
        &service,
        &created.run_id,
        "alice",
        vec!["medical-reviewers".to_owned()],
    )
    .await;
    let work_item_id = item.work_item_id().as_str().to_owned();
    let resolver = BearerHumanPrincipalResolver::new([
        (
            "alice-human-token".to_owned(),
            "alice".to_owned(),
            vec!["medical-reviewers".to_owned()],
        ),
        (
            "bob-human-token".to_owned(),
            "bob".to_owned(),
            vec!["other-reviewers".to_owned()],
        ),
    ])
    .unwrap();
    let app = build_router(FormalApiState {
        service: service.clone(),
        auth: ApiAuth::bearer_token("platform-admin-token")
            .with_human_principal_resolver(Arc::new(resolver)),
        sse_keep_alive_interval: Duration::from_secs(1),
        readiness_probe_timeout: Duration::from_secs(1),
    });

    let alice_list = app
        .clone()
        .oneshot(
            Request::get("/v1/human-tasks?limit=10")
                .header("authorization", "Bearer alice-human-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(alice_list.status(), StatusCode::OK);
    let bob_list = app
        .clone()
        .oneshot(
            Request::get("/v1/human-tasks?limit=10")
                .header("authorization", "Bearer bob-human-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bob_list.status(), StatusCode::OK);

    let missing_request_id = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/human-tasks/{work_item_id}/claim"))
                .header("authorization", "Bearer alice-human-token")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_request_id.status(), StatusCode::BAD_REQUEST);
    let claim = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/human-tasks/{work_item_id}/claim"))
                .header("authorization", "Bearer alice-human-token")
                .header("content-type", "application/json")
                .header("x-request-id", "http-human-claim")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claim.status(), StatusCode::OK);
    let claim: serde_json::Value =
        serde_json::from_slice(&to_bytes(claim.into_body(), 1024 * 1024).await.unwrap()).unwrap();
    let claim_fence = claim["data"]["claim_fence"].as_u64().unwrap();
    let completion = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/human-tasks/{work_item_id}/complete"))
                .header("authorization", "Bearer alice-human-token")
                .header("content-type", "application/json")
                .header("x-request-id", "http-human-complete")
                .body(Body::from(
                    json!({
                        "claim_fence": claim_fence,
                        "value": {"decision": "approved"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(completion.status(), StatusCode::OK);
    wait_for_completed(&service, &created.run_id).await;

    let forbidden_cancel = app
        .clone()
        .oneshot(
            Request::delete(format!("/v1/runs/{}", created.run_id))
                .header("authorization", "Bearer alice-human-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden_cancel.status(), StatusCode::UNAUTHORIZED);
    let forbidden_create = app
        .clone()
        .oneshot(
            Request::post("/v1/agents/human_gate/runs")
                .header("authorization", "Bearer alice-human-token")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden_create.status(), StatusCode::UNAUTHORIZED);
    let admin_not_human = app
        .oneshot(
            Request::get("/v1/human-tasks")
                .header("authorization", "Bearer platform-admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(admin_not_human.status(), StatusCode::UNAUTHORIZED);
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn reserved_human_completion_is_replayed_after_runtime_restart_without_client_retry() {
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_agents(&agents_root);
    let enabled = BTreeSet::from(["human_gate".to_owned()]);
    let published = compile_enabled_v3_agents(&agents_root, &enabled).unwrap();
    let deployed = deploy_v3_agents(&published, &NoLeafResolver).unwrap();
    let agents = DeployedAgentCatalog::new(deployed).unwrap();
    let database = temporary.path().join("human-completion-restart.sqlite");
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let first = RunService::start(
        agents.clone(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(8, 2, 1, 32),
    )
    .await
    .unwrap();
    let created = first
        .create_detached("human_gate", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    let principal =
        HumanTaskPrincipal::new("restart-reviewer", vec!["medical-reviewers".to_owned()]).unwrap();
    let item = wait_for_human_item(
        &first,
        &created.run_id,
        principal.identity(),
        principal.groups().to_vec(),
    )
    .await;
    let id = HumanWorkItemId::new(item.work_item_id().as_str()).unwrap();
    let claim = first
        .claim_human_task(
            id.as_str(),
            principal.identity(),
            principal.groups().to_vec(),
            "restart-claim",
        )
        .await
        .unwrap();
    first.shutdown(Duration::from_secs(1)).await.unwrap();

    assert!(matches!(
        repository
            .complete_human_work_item(
                CompleteHumanWorkItemCommand::new(
                    id.clone(),
                    principal,
                    "restart-completion",
                    claim.claim_fence(),
                    json!({"decision": "approved"}),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(false)
            .disable_statement_logging(),
    )
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM signals_inbox WHERE run_id=?",)
            .bind(&created.run_id)
            .fetch_one(&control)
            .await
            .unwrap(),
        0
    );

    let second = RunService::start(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(8, 2, 1, 32),
    )
    .await
    .unwrap();
    wait_for_completed(&second, &created.run_id).await;
    assert_eq!(
        repository
            .load_human_work_item(&id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        insight_agent_platform::engine::repository::HumanWorkItemState::Completed
    );
    second.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn database_clock_timer_fires_while_paused_and_downstream_waits_for_resume() {
    let (temporary, service, control) = setup().await;
    let created = service
        .create_detached(
            "timer_gate",
            json!({}),
            RequestMetadata {
                request_id: Some("timer-ingress-1".to_owned()),
            },
        )
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let scheduled = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM timers WHERE run_id=? AND timer_kind='wait'",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap();
        if scheduled == 1 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    service.pause(&created.run_id).await.unwrap();
    loop {
        let state = sqlx::query(
            "SELECT t.timer_state,a.lifecycle
             FROM timers t JOIN node_activations a ON a.run_id=t.run_id
                AND a.activation_id=t.activation_id
             WHERE t.run_id=? AND t.timer_kind='wait'",
        )
        .bind(&created.run_id)
        .fetch_optional(&control)
        .await
        .unwrap();
        let Some(state) = state else {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        };
        if state.get::<String, _>("timer_state") == "fired" {
            assert_eq!(state.get::<String, _>("lifecycle"), "succeeded");
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        service.get_run(&created.run_id).await.unwrap().status(),
        RunStatus::Running
    );
    service.resume(&created.run_id).await.unwrap();
    wait_for_completed(&service, &created.run_id).await;
    let timer_id: String =
        sqlx::query_scalar("SELECT timer_id FROM timers WHERE run_id=? AND timer_kind='wait'")
            .bind(&created.run_id)
            .fetch_one(&control)
            .await
            .unwrap();
    let repair_repository =
        SqliteDurableRepository::connect_path(&temporary.path().join("runtime-ingress.sqlite"))
            .await
            .unwrap();
    let run_id = insight_agent_platform::engine::RunId::new(created.run_id.clone()).unwrap();
    let projection =
        ProjectionSubject::new(ProjectionSubjectKind::Timer, timer_id.clone()).unwrap();
    assert!(repair_repository
        .audit_projection(&run_id, &projection)
        .await
        .unwrap()
        .is_match());
    let mut fault = control.acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys=OFF")
        .execute(&mut *fault)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query("DELETE FROM timers WHERE timer_id=?")
            .bind(timer_id)
            .execute(&mut *fault)
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    drop(fault);
    assert!(repair_repository
        .repair_projection(&run_id, &projection)
        .await
        .unwrap()
        .repaired());
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn root_deadline_is_database_clocked_persists_restart_and_ignores_pause() {
    let temporary = tempfile::tempdir().unwrap();
    let agents_root = temporary.path().join("agents");
    std::fs::create_dir_all(&agents_root).unwrap();
    write_agents(&agents_root);
    let enabled = BTreeSet::from(["human_gate".to_owned()]);
    let published = compile_enabled_v3_agents(&agents_root, &enabled).unwrap();
    let deployed = deploy_v3_agents(&published, &NoLeafResolver).unwrap();
    let agents = DeployedAgentCatalog::new(deployed).unwrap();
    let database = temporary.path().join("root-deadline.sqlite");

    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let first = RunService::start(
        agents.clone(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(8, 2, 1, 32)
            .with_run_timeout(Duration::from_millis(250)),
    )
    .await
    .unwrap();
    let created = first
        .create_detached("human_gate", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_human_item(
        &first,
        &created.run_id,
        "deadline-reviewer",
        vec!["medical-reviewers".to_owned()],
    )
    .await;
    first.pause(&created.run_id).await.unwrap();

    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(false)
            .disable_statement_logging(),
    )
    .await
    .unwrap();
    let deadline =
        sqlx::query_scalar::<_, String>("SELECT deadline_at FROM workflow_runs WHERE run_id=?")
            .bind(&created.run_id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT julianday(deadline_at) > julianday('now') FROM workflow_runs WHERE run_id=?",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    assert!(!deadline.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT julianday(json_extract(e.safe_payload,'$.run_deadline_at')) = julianday(r.deadline_at)
             FROM execution_events e JOIN workflow_runs r ON r.run_id=e.run_id
             WHERE e.run_id=? AND e.kind='run.created'",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );

    first.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(first);
    drop(repository);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let second = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(8, 2, 1, 32),
    )
    .await
    .unwrap();
    let settle_by = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if second.get_run(&created.run_id).await.unwrap().status() == RunStatus::Failed {
            break;
        }
        if tokio::time::Instant::now() >= settle_by {
            let run = sqlx::query(
                "SELECT lifecycle,admission_state,termination_intent_reason
                 FROM workflow_runs WHERE run_id=?",
            )
            .bind(&created.run_id)
            .fetch_one(&control)
            .await
            .unwrap();
            let activations = sqlx::query(
                "SELECT activation_id,lifecycle,termination_intent_reason
                 FROM node_activations WHERE run_id=? ORDER BY activation_id",
            )
            .bind(&created.run_id)
            .fetch_all(&control)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>("activation_id"),
                    row.get::<String, _>("lifecycle"),
                    row.get::<Option<String>, _>("termination_intent_reason"),
                )
            })
            .collect::<Vec<_>>();
            panic!(
                "deadline Run did not settle: lifecycle={} admission={} reason={:?} activations={activations:?}",
                run.get::<String, _>("lifecycle"),
                run.get::<String, _>("admission_state"),
                run.get::<Option<String>, _>("termination_intent_reason"),
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let row = sqlx::query(
        "SELECT lifecycle,admission_state,termination_intent_reason
         FROM workflow_runs WHERE run_id=?",
    )
    .bind(&created.run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("lifecycle"), "timed_out");
    assert_eq!(row.get::<String, _>("admission_state"), "closed");
    assert_eq!(
        row.get::<String, _>("termination_intent_reason"),
        "timed_out"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT work_state FROM human_work_items WHERE run_id=?",)
            .bind(&created.run_id)
            .fetch_one(&control)
            .await
            .unwrap(),
        "expired"
    );
    second.shutdown(Duration::from_secs(1)).await.unwrap();
    control.close().await;
}
