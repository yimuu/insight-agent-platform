use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use insight_api::v1::{
    build_agent_management_router, build_router, AgentDebugProfileMode, AgentDebugProfilePolicy,
    AgentManagementApiState, AgentManagementPolicy, ApiAuth, ApiState, OperatorAuth,
    AGENT_ACTIVATE, AGENT_ARCHIVE, AGENT_DEBUG_LIVE, AGENT_DEBUG_SANDBOX, AGENT_DEPLOY,
    AGENT_PUBLISH, AGENT_READ, AGENT_VALIDATE, AGENT_WRITE,
};
use insight_dsl::{
    CompileError, CompileOptions, GraphAuthorDocument, GraphDocumentId, GraphSemanticEdit,
    GraphSemanticEditBatch, NodeView, ViewDocument,
};
use insight_durable::AgentManagementDurableRepository;
use insight_engine::{
    plan::LeafTaskDescriptor, worker::WorkerExecutorRegistry, DefinitionRevisionId, LeafTaskKind,
};
use insight_runtime::{
    catalog::{LeafDeploymentResolver, ResolvedLeafDeployment},
    DeployedAgentCatalog, ProductionRunRepository, RunService, RunServiceConfig,
};
use insight_storage::SqliteDurableRepository;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tower::ServiceExt;

const SQLITE_SCHEMA: &str = include_str!("../database/durable/sqlite/schema.sql");
const OPERATOR_TOKEN: &str = "agent-operator-token";
const SANDBOX_ONLY_TOKEN: &str = "agent-sandbox-only-token";
const PUBLIC_TOKEN: &str = "agent-public-token";

const SOURCE: &str = r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: managed-api
  name: Managed API
  description: Agent management integration fixture
inputs:
  question: string
output: string
workflow:
  steps:
    - return: fixed
"#;

struct NoExternalLeaves;

impl LeafDeploymentResolver for NoExternalLeaves {
    fn resolve_leaf(
        &self,
        _kind: LeafTaskKind,
        _descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        Err(CompileError::new(
            "FIXTURE_EXTERNAL_LEAF_FORBIDDEN",
            "the integration fixture has no external leaf",
        ))
    }
}

fn capabilities() -> BTreeSet<String> {
    BTreeSet::from(
        [
            AGENT_READ,
            AGENT_WRITE,
            AGENT_VALIDATE,
            AGENT_PUBLISH,
            AGENT_DEPLOY,
            AGENT_ACTIVATE,
            AGENT_ARCHIVE,
            AGENT_DEBUG_SANDBOX,
            AGENT_DEBUG_LIVE,
        ]
        .map(str::to_owned),
    )
}

fn sandbox_only_capabilities() -> BTreeSet<String> {
    let mut capabilities = capabilities();
    capabilities.remove(AGENT_DEBUG_LIVE);
    capabilities
}

async fn fixture() -> (
    tempfile::TempDir,
    Arc<SqliteDurableRepository>,
    RunService,
    Router,
) {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("agent-management.sqlite3");
    File::create(&path).unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::from_str(path.to_str().unwrap())
                .unwrap()
                .foreign_keys(true),
        )
        .await
        .unwrap();
    sqlx::raw_sql(SQLITE_SCHEMA).execute(&pool).await.unwrap();
    pool.close().await;
    let repository = Arc::new(SqliteDurableRepository::connect_path(&path).await.unwrap());
    let production_repository: Arc<dyn ProductionRunRepository> = repository.clone();
    let service = RunService::start_with_graph_publication(
        DeployedAgentCatalog::new(Vec::new()).unwrap(),
        production_repository,
        WorkerExecutorRegistry::new(),
        Arc::new(NoExternalLeaves),
        RunServiceConfig::single_process_development(32, 4, 2, 64)
            .with_run_timeout(Duration::from_secs(10)),
    )
    .await
    .unwrap();
    let management = build_agent_management_router(AgentManagementApiState {
        auth: OperatorAuth::new([
            (
                OPERATOR_TOKEN.to_owned(),
                "operator-a".to_owned(),
                capabilities(),
            ),
            (
                SANDBOX_ONLY_TOKEN.to_owned(),
                "operator-sandbox".to_owned(),
                sandbox_only_capabilities(),
            ),
        ])
        .unwrap(),
        repository: repository.clone(),
        run_service: service.clone(),
        policy: Arc::new(AgentManagementPolicy {
            validation_policy_digest: hash(b"agent-policy"),
            cursor_signing_key: [9; 32],
            max_prompt_files: 8,
            max_prompt_file_bytes: 64 * 1024,
            max_package_bytes: 128 * 1024,
            resolution_ttl: chrono::Duration::minutes(5),
            debug_profiles: BTreeMap::from([
                (
                    "author-sandbox".to_owned(),
                    AgentDebugProfilePolicy {
                        mode: AgentDebugProfileMode::Sandbox,
                        max_concurrent_sessions: 4,
                        session_timeout: chrono::Duration::minutes(5),
                        content_retention: chrono::Duration::hours(1),
                        allow_external_actions: false,
                        allow_live_provider_credentials: false,
                    },
                ),
                (
                    "author-live".to_owned(),
                    AgentDebugProfilePolicy {
                        mode: AgentDebugProfileMode::Live,
                        max_concurrent_sessions: 1,
                        session_timeout: chrono::Duration::minutes(2),
                        content_retention: chrono::Duration::minutes(30),
                        allow_external_actions: true,
                        allow_live_provider_credentials: true,
                    },
                ),
            ]),
        }),
        debug_stream_keep_alive_interval: Duration::from_secs(1),
    });
    let public = build_router(ApiState {
        service: service.clone(),
        auth: ApiAuth::bearer_token(PUBLIC_TOKEN),
        sse_keep_alive_interval: Duration::from_secs(1),
        readiness_probe_timeout: Duration::from_secs(1),
    });
    (temporary, repository, service, public.merge(management))
}

fn hash(bytes: &[u8]) -> String {
    format!(
        "sha256:{}",
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn graph_draft_fixture() -> GraphAuthorDocument {
    let source = include_str!("fixtures/dsl/linear.yaml");
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("managed_graph_canvas").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("managed_graph_draft_definition").unwrap(),
            "managed-graph/agent.yaml",
            source,
        ),
    )
    .unwrap()
}

fn graph_publish_fixture() -> GraphAuthorDocument {
    let source = r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: managed-graph
  name: Managed Graph
inputs:
  question: string
output: string
workflow:
  steps:
    - return: graph-fixed
"#;
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("managed_graph_publish_canvas").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("managed_graph_publish_definition").unwrap(),
            "managed-graph/publish.yaml",
            source,
        ),
    )
    .unwrap()
}

fn request(
    method: &str,
    uri: &str,
    token: &str,
    request_id: Option<&str>,
    if_match: Option<&str>,
    body: Option<Value>,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(request_id) = request_id {
        builder = builder.header("x-request-id", request_id);
    }
    if let Some(if_match) = if_match {
        builder = builder.header(header::IF_MATCH, if_match);
    }
    match body {
        Some(body) => builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), 8 * 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap()
}

async fn wait_for_debug_terminal(app: &Router, session_id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = app
            .clone()
            .oneshot(request(
                "GET",
                &format!("/v1/admin/agents/managed-api/debug-sessions/{session_id}"),
                OPERATOR_TOKEN,
                None,
                None,
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        if matches!(
            body["data"]["status"].as_str(),
            Some("succeeded" | "failed" | "cancelled" | "expired")
        ) {
            return body["data"].clone();
        }
        assert!(
            Instant::now() < deadline,
            "Debug Session did not finish: {body}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn agent_management_separates_author_publish_deploy_activate_and_debug_visibility() {
    let (temporary, repository, service, app) = fixture().await;

    let unauthorized = app
        .clone()
        .oneshot(
            Request::get("/v1/admin/agents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(unauthorized.headers()["x-content-type-options"], "nosniff");

    let duplicate_key = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/admin/agents")
                .header(header::AUTHORIZATION, format!("Bearer {OPERATOR_TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-request-id", "duplicate-agent-key")
                .body(Body::from(
                    r#"{"agent_id":"first","agent_id":"second","authoring_mode":"graph","labels":{},"draft":{"source":{"type":"graph","document":{}}}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate_key.status(), StatusCode::BAD_REQUEST);

    let created = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents",
            OPERATOR_TOKEN,
            Some("agent-create-1"),
            None,
            Some(json!({
                "agent_id":"managed-api",
                "authoring_mode":"yaml_package",
                "labels":{"team":"platform"},
                "draft":{"source":{"type":"yaml_package","agent_yaml":SOURCE,"prompt_files":[]}}
            })),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(created.headers()[header::ETAG], "\"agent-1\"");
    assert_eq!(
        created.headers()[header::CACHE_CONTROL],
        "private, no-store"
    );
    assert_eq!(created.headers()["x-content-type-options"], "nosniff");

    let replayed = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents",
            OPERATOR_TOKEN,
            Some("agent-create-1"),
            None,
            Some(json!({
                "agent_id":"managed-api",
                "authoring_mode":"yaml_package",
                "labels":{"team":"platform"},
                "draft":{"source":{"type":"yaml_package","agent_yaml":SOURCE,"prompt_files":[]}}
            })),
        ))
        .await
        .unwrap();
    assert_eq!(replayed.headers()["idempotent-replayed"], "true");

    let public_before_activation = app
        .clone()
        .oneshot(request(
            "GET",
            "/v1/agents/managed-api",
            PUBLIC_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(public_before_activation.status(), StatusCode::NOT_FOUND);

    let live_without_capability = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-api/debug-sessions",
            SANDBOX_ONLY_TOKEN,
            Some("debug-live-forbidden"),
            None,
            Some(json!({
                "source":{"type":"draft","draft_version":1},
                "execution_profile_id":"author-live",
                "input":{"question":"test"}
            })),
        ))
        .await
        .unwrap();
    assert_eq!(live_without_capability.status(), StatusCode::FORBIDDEN);

    let live_without_confirmation = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-api/debug-sessions",
            OPERATOR_TOKEN,
            Some("debug-live-unconfirmed"),
            None,
            Some(json!({
                "source":{"type":"draft","draft_version":1},
                "execution_profile_id":"author-live",
                "input":{"question":"test"}
            })),
        ))
        .await
        .unwrap();
    assert_eq!(
        live_without_confirmation.status(),
        StatusCode::PRECONDITION_REQUIRED
    );
    let live_preview = json_body(live_without_confirmation).await;
    assert_eq!(
        live_preview["code"],
        "AGENT_DEBUG_LIVE_CONFIRMATION_REQUIRED"
    );
    assert_eq!(live_preview["data"]["risk_preview"]["profile_mode"], "live");
    assert_eq!(
        live_preview["data"]["risk_preview"]["cost_risk"]["billing_possible"],
        false
    );
    let serialized_live_preview = serde_json::to_string(&live_preview).unwrap();
    for private_body in ["question", "api_version", "graph-fixed"] {
        assert!(!serialized_live_preview.contains(private_body));
    }
    let risk_preview_hash = live_preview["data"]["risk_preview"]["risk_preview_hash"]
        .as_str()
        .unwrap()
        .to_owned();
    let stale_preview = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-api/debug-sessions",
            OPERATOR_TOKEN,
            Some("debug-live-stale-preview"),
            None,
            Some(json!({
                "source":{"type":"draft","draft_version":1},
                "execution_profile_id":"author-live",
                "input":{"question":"test"},
                "live_confirmation":true,
                "risk_preview_hash":hash(b"different-preview")
            })),
        ))
        .await
        .unwrap();
    assert_eq!(stale_preview.status(), StatusCode::PRECONDITION_REQUIRED);
    let live_debug = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-api/debug-sessions",
            OPERATOR_TOKEN,
            Some("debug-live-confirmed"),
            None,
            Some(json!({
                "source":{"type":"draft","draft_version":1},
                "execution_profile_id":"author-live",
                "input":{"question":"test"},
                "live_confirmation":true,
                "risk_preview_hash":risk_preview_hash
            })),
        ))
        .await
        .unwrap();
    assert_eq!(live_debug.status(), StatusCode::ACCEPTED);
    let live_session_id = json_body(live_debug).await["data"]["debug_session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let live_terminal = wait_for_debug_terminal(&app, &live_session_id).await;
    assert_eq!(live_terminal["status"], "succeeded");

    let debug = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-api/debug-sessions",
            OPERATOR_TOKEN,
            Some("debug-draft-1"),
            None,
            Some(json!({
                "source":{"type":"draft","draft_version":1},
                "execution_profile_id":"author-sandbox",
                "input":{"question":"test"}
            })),
        ))
        .await
        .unwrap();
    assert_eq!(debug.status(), StatusCode::ACCEPTED);
    let session_id = json_body(debug).await["data"]["debug_session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let terminal = wait_for_debug_terminal(&app, &session_id).await;
    assert_eq!(terminal["status"], "succeeded");
    let debug_run_id = terminal["run_id"].as_str().unwrap();
    assert!(debug_run_id.starts_with("debugrun_"));

    let hidden_run = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/runs/{debug_run_id}"),
            PUBLIC_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(hidden_run.status(), StatusCode::NOT_FOUND);

    let stream = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/admin/agents/managed-api/debug-sessions/{session_id}/stream"),
            OPERATOR_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(stream.status(), StatusCode::OK);
    assert!(stream.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
    drop(stream);

    assert_eq!(
        repository
            .cleanup_expired_agent_debug_sessions(
                chrono::Utc::now() + chrono::Duration::hours(2),
                10,
            )
            .await
            .unwrap(),
        2
    );
    let retained = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/admin/agents/managed-api/debug-sessions/{session_id}"),
            OPERATOR_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(retained.status(), StatusCode::OK);
    assert_eq!(
        json_body(retained).await["data"]["source"],
        json!({"content_deleted":true})
    );
    let retained_stream = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/admin/agents/managed-api/debug-sessions/{session_id}/stream"),
            OPERATOR_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(retained_stream.status(), StatusCode::GONE);

    let validation = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-api/validations",
            OPERATOR_TOKEN,
            Some("validate-1"),
            Some("\"draft-1\""),
            Some(json!({})),
        ))
        .await
        .unwrap();
    assert_eq!(validation.status(), StatusCode::ACCEPTED);
    let validation_location = validation.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .to_owned();
    let validation_body = json_body(validation).await;
    let validation_id = validation_body["data"]["validation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        validation_location,
        format!("/v1/admin/agents/managed-api/validations/{validation_id}")
    );

    let published = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-api/revisions",
            OPERATOR_TOKEN,
            Some("publish-1"),
            Some("\"draft-1\""),
            Some(json!({"draft_version":1,"validation_id":validation_id})),
        ))
        .await
        .unwrap();
    assert_eq!(published.status(), StatusCode::CREATED);
    let definition_revision_id = json_body(published).await["data"]["definition_revision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        app.clone()
            .oneshot(request(
                "GET",
                "/v1/agents/managed-api",
                PUBLIC_TOKEN,
                None,
                None,
                None,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND,
        "publish must not activate"
    );

    let resolution = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-api/deployment-resolutions",
            OPERATOR_TOKEN,
            Some("resolve-1"),
            None,
            Some(json!({"definition_revision_id":definition_revision_id})),
        ))
        .await
        .unwrap();
    assert_eq!(resolution.status(), StatusCode::ACCEPTED);
    let resolution_id = json_body(resolution).await["data"]["resolution_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let deployment = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-api/deployments",
            OPERATOR_TOKEN,
            Some("deploy-1"),
            None,
            Some(json!({"definition_revision_id":definition_revision_id,"resolution_id":resolution_id})),
        ))
        .await
        .unwrap();
    assert_eq!(deployment.status(), StatusCode::CREATED);
    let deployment_revision_id = json_body(deployment).await["data"]["deployment_revision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        app.clone()
            .oneshot(request(
                "GET",
                "/v1/agents/managed-api",
                PUBLIC_TOKEN,
                None,
                None,
                None,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND,
        "deploy must not activate"
    );

    let activated = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/agents/managed-api/active-deployment",
            OPERATOR_TOKEN,
            Some("activate-1"),
            Some("\"agent-1\""),
            Some(json!({"deployment_revision_id":deployment_revision_id})),
        ))
        .await
        .unwrap();
    assert_eq!(activated.status(), StatusCode::OK);
    assert_eq!(activated.headers()[header::ETAG], "\"agent-2\"");
    assert_eq!(
        app.clone()
            .oneshot(request(
                "GET",
                "/v1/agents/managed-api",
                PUBLIC_TOKEN,
                None,
                None,
                None,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let historical_admission = app
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/agents/managed-api/deployments/{deployment_revision_id}/runs"),
            PUBLIC_TOKEN,
            None,
            None,
            Some(json!({"question":"bypass"})),
        ))
        .await
        .unwrap();
    assert_eq!(historical_admission.status(), StatusCode::NOT_FOUND);

    let old_graph_api = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/graph-agents/managed-api/revisions",
            PUBLIC_TOKEN,
            None,
            None,
            Some(json!({})),
        ))
        .await
        .unwrap();
    assert_eq!(old_graph_api.status(), StatusCode::NOT_FOUND);

    let nonempty_delete = app
        .clone()
        .oneshot(request(
            "DELETE",
            "/v1/admin/agents/managed-api/active-deployment",
            OPERATOR_TOKEN,
            Some("deactivate-body"),
            Some("\"agent-2\""),
            Some(json!({})),
        ))
        .await
        .unwrap();
    assert_eq!(nonempty_delete.status(), StatusCode::BAD_REQUEST);

    let archived = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-api/archive",
            OPERATOR_TOKEN,
            Some("archive-1"),
            Some("\"agent-2\""),
            Some(json!({})),
        ))
        .await
        .unwrap();
    assert_eq!(archived.status(), StatusCode::OK);
    assert_eq!(
        app.oneshot(request(
            "GET",
            "/v1/agents/managed-api",
            PUBLIC_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap()
        .status(),
        StatusCode::NOT_FOUND
    );

    let audit_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::from_str(
                temporary
                    .path()
                    .join("agent-management.sqlite3")
                    .to_str()
                    .unwrap(),
            )
            .unwrap()
            .foreign_keys(true),
        )
        .await
        .unwrap();
    let invalid_capabilities: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_management_audit_events
         WHERE capability NOT IN('agent.read','agent.write','agent.validate','agent.publish',
                                 'agent.deploy','agent.activate','agent.archive',
                                 'agent.debug.sandbox','agent.debug.live')",
    )
    .fetch_one(&audit_pool)
    .await
    .unwrap();
    assert_eq!(invalid_capabilities, 0);
    let safe_evidence: String = sqlx::query_scalar(
        "SELECT COALESCE(group_concat(event_kind || ':' || capability || ':' || request_id_hash || ':' || COALESCE(before_hash,'') || ':' || COALESCE(after_hash,'')), '') || ':' || COALESCE((SELECT group_concat(safe_payload) FROM agent_management_outbox), '') FROM agent_management_audit_events",
    )
    .fetch_one(&audit_pool)
    .await
    .unwrap();
    for forbidden in [
        "Agent management integration fixture",
        "api_version: insight.agent/v1",
        "question",
        "fixed",
    ] {
        assert!(!safe_evidence.contains(forbidden));
    }
    for expected in [
        "agent.request_rejected",
        "agent.write",
        "agent.validate",
        "agent.publish",
        "agent.deploy",
        "agent.activate",
        "agent.archive",
        "agent.debug.sandbox",
    ] {
        assert!(safe_evidence.contains(expected));
    }
    let rejected: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_management_audit_events
         WHERE event_kind='agent.request_rejected' AND result_code='http_400'",
    )
    .fetch_one(&audit_pool)
    .await
    .unwrap();
    assert!(rejected >= 2);
    audit_pool.close().await;

    service.shutdown(Duration::from_secs(5)).await.unwrap();
}

#[tokio::test]
async fn graph_management_keeps_view_and_semantic_edits_inside_the_draft_boundary() {
    let (_temporary, _repository, service, app) = fixture().await;
    let graph = graph_draft_fixture();
    let graph_document: Value = serde_json::from_slice(&graph.encode_json().unwrap()).unwrap();
    let created = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents",
            OPERATOR_TOKEN,
            Some("graph-agent-create"),
            None,
            Some(json!({
                "agent_id":"managed-graph",
                "authoring_mode":"graph",
                "labels":{"team":"canvas"},
                "draft":{"source":{"type":"graph","document":graph_document}}
            })),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);

    let original_draft = app
        .clone()
        .oneshot(request(
            "GET",
            "/v1/admin/agents/managed-graph/draft",
            OPERATOR_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(original_draft.headers()[header::ETAG], "\"draft-1\"");
    let original_draft = json_body(original_draft).await["data"].clone();

    let initial_view = app
        .clone()
        .oneshot(request(
            "GET",
            "/v1/admin/agents/managed-graph/draft/view",
            OPERATOR_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(initial_view.headers()[header::ETAG], "\"view-0\"");
    let mut view = ViewDocument::new(graph.document_id().clone());
    view.set_node(graph.nodes()[0].id().clone(), NodeView::at(12.0, 34.0))
        .unwrap();
    let view_document: Value = serde_json::from_slice(&view.encode_json().unwrap()).unwrap();
    let saved_view = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/agents/managed-graph/draft/view",
            OPERATOR_TOKEN,
            Some("graph-view-save"),
            Some("\"view-0\""),
            Some(view_document),
        ))
        .await
        .unwrap();
    assert_eq!(saved_view.status(), StatusCode::OK);

    let after_view = app
        .clone()
        .oneshot(request(
            "GET",
            "/v1/admin/agents/managed-graph/draft",
            OPERATOR_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(after_view.headers()[header::ETAG], "\"draft-1\"");
    assert_eq!(json_body(after_view).await["data"], original_draft);

    let edit_batch = GraphSemanticEditBatch::new(
        graph.semantic_hash().clone(),
        DefinitionRevisionId::new("managed_graph_edit_target").unwrap(),
        vec![GraphSemanticEdit::UpsertNode {
            node: graph.nodes()[0].clone(),
        }],
    )
    .unwrap();
    let edit_document: Value = serde_json::from_slice(&edit_batch.encode_json().unwrap()).unwrap();
    let edited = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-graph/draft/semantic-edits",
            OPERATOR_TOKEN,
            Some("graph-semantic-edit"),
            Some("\"draft-1\""),
            Some(edit_document.clone()),
        ))
        .await
        .unwrap();
    assert_eq!(edited.status(), StatusCode::OK);

    let edited_draft = app
        .clone()
        .oneshot(request(
            "GET",
            "/v1/admin/agents/managed-graph/draft",
            OPERATOR_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(edited_draft.headers()[header::ETAG], "\"draft-2\"");
    assert_ne!(json_body(edited_draft).await["data"], original_draft);

    let revisions = app
        .clone()
        .oneshot(request(
            "GET",
            "/v1/admin/agents/managed-graph/revisions",
            OPERATOR_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert!(json_body(revisions).await["data"]["items"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        app.clone()
            .oneshot(request(
                "GET",
                "/v1/agents/managed-graph",
                PUBLIC_TOKEN,
                None,
                None,
                None,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    let stale_edit = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-graph/draft/semantic-edits",
            OPERATOR_TOKEN,
            Some("graph-semantic-edit-stale"),
            Some("\"draft-1\""),
            Some(edit_document),
        ))
        .await
        .unwrap();
    assert_eq!(stale_edit.status(), StatusCode::PRECONDITION_FAILED);

    let publish_graph = graph_publish_fixture();
    let publish_graph_document: Value =
        serde_json::from_slice(&publish_graph.encode_json().unwrap()).unwrap();
    let replaced = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/agents/managed-graph/draft",
            OPERATOR_TOKEN,
            Some("graph-draft-replace"),
            Some("\"draft-2\""),
            Some(json!({"source":{"type":"graph","document":publish_graph_document}})),
        ))
        .await
        .unwrap();
    assert_eq!(replaced.status(), StatusCode::OK);
    let validation = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-graph/validations",
            OPERATOR_TOKEN,
            Some("graph-validate"),
            Some("\"draft-3\""),
            Some(json!({})),
        ))
        .await
        .unwrap();
    assert_eq!(validation.status(), StatusCode::ACCEPTED);
    let validation_id = json_body(validation).await["data"]["validation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let published = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-graph/revisions",
            OPERATOR_TOKEN,
            Some("graph-publish"),
            Some("\"draft-3\""),
            Some(json!({"draft_version":3,"validation_id":validation_id})),
        ))
        .await
        .unwrap();
    assert_eq!(published.status(), StatusCode::CREATED);
    let definition_revision_id = json_body(published).await["data"]["definition_revision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        app.clone()
            .oneshot(request(
                "GET",
                "/v1/agents/managed-graph",
                PUBLIC_TOKEN,
                None,
                None,
                None,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let resolution = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-graph/deployment-resolutions",
            OPERATOR_TOKEN,
            Some("graph-resolve"),
            None,
            Some(json!({"definition_revision_id":definition_revision_id})),
        ))
        .await
        .unwrap();
    assert_eq!(resolution.status(), StatusCode::ACCEPTED);
    let resolution_id = json_body(resolution).await["data"]["resolution_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let deployment = app
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/agents/managed-graph/deployments",
            OPERATOR_TOKEN,
            Some("graph-deploy"),
            None,
            Some(json!({
                "definition_revision_id":definition_revision_id,
                "resolution_id":resolution_id
            })),
        ))
        .await
        .unwrap();
    assert_eq!(deployment.status(), StatusCode::CREATED);
    let deployment_revision_id = json_body(deployment).await["data"]["deployment_revision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        app.clone()
            .oneshot(request(
                "GET",
                "/v1/agents/managed-graph",
                PUBLIC_TOKEN,
                None,
                None,
                None,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let activated = app
        .clone()
        .oneshot(request(
            "PUT",
            "/v1/admin/agents/managed-graph/active-deployment",
            OPERATOR_TOKEN,
            Some("graph-activate"),
            Some("\"agent-1\""),
            Some(json!({"deployment_revision_id":deployment_revision_id})),
        ))
        .await
        .unwrap();
    assert_eq!(activated.status(), StatusCode::OK);
    assert_eq!(
        app.oneshot(request(
            "GET",
            "/v1/agents/managed-graph",
            PUBLIC_TOKEN,
            None,
            None,
            None,
        ))
        .await
        .unwrap()
        .status(),
        StatusCode::OK
    );
    service.shutdown(Duration::from_secs(5)).await.unwrap();
}
