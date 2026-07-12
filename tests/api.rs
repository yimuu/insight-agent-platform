//! Formal V1 HTTP and SSE contract tests.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use futures::StreamExt;
use handlebars::Handlebars;
use insight_agent_platform::{
    api::formal::{build_router, ApiAuth, FormalApiState},
    dsl::{
        compiled::{
            CompiledAgent, CompiledNode, ExecutionPlan, NodeCompilation, NodeControl, NodeOutcome,
            NodeTransition, RunOutput,
        },
        compiler::CompileContext,
        CompileError, EmitPolicy,
    },
    events::hub::{EventHub, EventHubConfig},
    history::{repository::RunRepository, sqlite::SqliteRunRepository, types::RunStatus},
    nodes::{
        default_node_registries,
        registry::{NodeExecutor, NodeType},
    },
    runtime::{
        CompiledAgentRegistry, ExecutionControl, RunContext, RunError, RunService, RunServiceConfig,
    },
    schema::compile_schema,
};
use serde_json::{json, Value};
use tower::ServiceExt;

const TEST_SSE_KEEP_ALIVE: Duration = Duration::from_millis(10);

enum ApiBehavior {
    Complete,
    Block,
}

struct ApiNode;

impl NodeType for ApiNode {
    fn kind(&self) -> &'static str {
        "test.api"
    }

    fn compile(
        &self,
        _node_id: &str,
        _config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        Err(CompileError::new(
            "TEST_ONLY",
            "API test nodes are constructed directly",
        ))
    }
}

#[async_trait]
impl NodeExecutor for ApiNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        _context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        match node.body::<ApiBehavior>()? {
            ApiBehavior::Complete => Ok(NodeOutcome {
                output: json!({"answer":"done"}),
                transition: NodeTransition::Complete(RunOutput {
                    content: Some("done".to_string()),
                    format: Some("text".to_string()),
                    data: json!({"answer":"done"}),
                }),
            }),
            ApiBehavior::Block => {
                control.stopped().await;
                Err(RunError::stopped(control.stop_reason().unwrap()))
            }
        }
    }
}

fn agent(id: &str, behavior: ApiBehavior) -> Arc<CompiledAgent> {
    let node = CompiledNode {
        id: "work".to_string(),
        kind: "test.api".to_string(),
        next: None,
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(30),
        body: Arc::new(behavior),
        edges: Vec::new(),
        references: BTreeSet::new(),
        terminal: true,
        control: NodeControl::Ordinary,
    };
    let nodes = BTreeMap::from([("work".to_string(), node)]);
    Arc::new(CompiledAgent {
        id: id.to_string(),
        name: format!("{id} agent"),
        description: "Safe public metadata".to_string(),
        version_hash: format!("sha256:{id}"),
        input_schema: Arc::new(
            compile_schema(&json!({
                "type": "object",
                "required": ["text"],
                "additionalProperties": false,
                "properties": {"text": {"type": "string"}}
            }))
            .unwrap(),
        ),
        entry: "work".to_string(),
        execution_plan: ExecutionPlan::sequential("work", nodes.keys().cloned()),
        nodes,
        templates: Arc::new(Handlebars::new()),
    })
}

async fn fixture(auth: ApiAuth, capacity: usize) -> (Router, RunService) {
    let repository = Arc::new(SqliteRunRepository::in_memory().await.unwrap());
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 16,
            journal_capacity: 32,
            journal_batch_size: 4,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let agents = vec![
        agent("fast", ApiBehavior::Complete),
        agent("blocking", ApiBehavior::Block),
    ];
    let agents = CompiledAgentRegistry::new(agents).unwrap();
    let (_, mut executors) = default_node_registries().unwrap();
    executors.register(ApiNode).unwrap();
    let service = RunService::new(
        agents,
        executors,
        repository_trait,
        events,
        RunServiceConfig {
            max_concurrent_runs: capacity,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(30),
        },
    )
    .unwrap();
    let router = build_router(FormalApiState {
        service: service.clone(),
        auth,
        sse_keep_alive_interval: TEST_SSE_KEEP_ALIVE,
    });
    (router, service)
}

fn request(method: Method, uri: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&value).unwrap())
        }
        None => Body::empty(),
    };
    builder.body(body).unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn wait_for_status(service: &RunService, run_id: &str, status: RunStatus) {
    for _ in 0..200 {
        if service.get_run(run_id).await.unwrap().status == status {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("run {run_id} did not reach {status:?}");
}

fn sse_data(body: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(body)
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str(data).unwrap())
        .collect()
}

#[tokio::test]
async fn health_is_public_while_v1_honors_explicit_bearer_auth() {
    let auth = ApiAuth::bearer_token("super-secret-token");
    assert!(!format!("{auth:?}").contains("super-secret-token"));
    let (app, service) = fixture(auth, 4).await;

    let health = app
        .clone()
        .oneshot(request(Method::GET, "/health", None))
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(json_body(health).await["code"], "OK");

    let unauthorized = app
        .clone()
        .oneshot(request(Method::GET, "/v1/agents", None))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(json_body(unauthorized).await["code"], "UNAUTHORIZED");

    let mut authorized = request(Method::GET, "/v1/agents", None);
    authorized.headers_mut().insert(
        header::AUTHORIZATION,
        "Bearer super-secret-token".parse().unwrap(),
    );
    assert_eq!(
        app.clone().oneshot(authorized).await.unwrap().status(),
        StatusCode::OK
    );
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    let degraded = app
        .oneshot(request(Method::GET, "/health", None))
        .await
        .unwrap();
    assert_eq!(degraded.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json_body(degraded).await["code"], "RUNTIME_UNHEALTHY");
}

#[tokio::test]
async fn agent_metadata_omits_runtime_internals_and_unknown_agents_are_hidden() {
    let (app, _) = fixture(ApiAuth::disabled(), 4).await;
    let response = app
        .clone()
        .oneshot(request(Method::GET, "/v1/agents", None))
        .await
        .unwrap();
    let body = json_body(response).await;

    assert_eq!(body["code"], "OK");
    assert_eq!(body["message"], "ok");
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    let encoded = body.to_string();
    assert!(!encoded.contains("nodes"));
    assert!(!encoded.contains("templates"));
    assert!(!encoded.contains("super-secret-token"));

    let missing = app
        .oneshot(request(Method::GET, "/v1/agents/disabled", None))
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(json_body(missing).await["code"], "AGENT_NOT_FOUND");
}

#[tokio::test]
async fn detached_creation_uses_direct_input_returns_202_and_supports_lookup() {
    let (app, service) = fixture(ApiAuth::disabled(), 4).await;
    let mut create = request(
        Method::POST,
        "/v1/agents/fast/runs",
        Some(json!({"text":"hello"})),
    );
    create
        .headers_mut()
        .insert("x-request-id", "req-client".parse().unwrap());
    let response = app.clone().oneshot(create).await.unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.headers()["x-request-id"], "req-client");
    let created = json_body(response).await;
    assert_eq!(created["code"], "OK");
    assert_eq!(created["data"]["request_id"], "req-client");
    assert_eq!(created["data"]["attachment"], "detached");
    let run_id = created["data"]["run_id"].as_str().unwrap();
    wait_for_status(&service, run_id, RunStatus::Completed).await;

    let lookup = app
        .oneshot(request(Method::GET, &format!("/v1/runs/{run_id}"), None))
        .await
        .unwrap();
    let lookup = json_body(lookup).await;
    assert_eq!(lookup["data"]["status"], "completed");
    assert_eq!(lookup["data"]["input_summary"]["keys"], json!(["text"]));
    assert!(!lookup.to_string().contains("hello"));
}

#[tokio::test]
async fn attached_creation_validates_before_sse_and_exposes_formal_event_headers() {
    let (app, _) = fixture(ApiAuth::disabled(), 4).await;

    let invalid_schema = app
        .clone()
        .oneshot(request(
            Method::POST,
            "/v1/agents/fast/runs/stream",
            Some(json!({"input":{"text":"wrapped"}})),
        ))
        .await
        .unwrap();
    assert_eq!(invalid_schema.status(), StatusCode::BAD_REQUEST);
    assert_ne!(
        invalid_schema.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    assert_eq!(json_body(invalid_schema).await["code"], "INPUT_INVALID");

    let malformed = Request::builder()
        .method(Method::POST)
        .uri("/v1/agents/fast/runs/stream")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{"))
        .unwrap();
    let malformed = app.clone().oneshot(malformed).await.unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(malformed).await["code"], "INPUT_INVALID");

    let mut valid = request(
        Method::POST,
        "/v1/agents/fast/runs/stream",
        Some(json!({"text":"hello"})),
    );
    valid
        .headers_mut()
        .insert("x-request-id", "req-stream".parse().unwrap());
    let response = app.oneshot(valid).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-run-id"));
    assert_eq!(response.headers()["x-request-id"], "req-stream");
    assert!(response.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));

    let body = tokio::time::timeout(
        Duration::from_secs(1),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("terminal event must close Attached SSE immediately")
    .unwrap();
    let events = sse_data(&body);
    assert_eq!(events.first().unwrap()["type"], "run.created");
    assert_eq!(events.last().unwrap()["type"], "run.completed");
    assert!(events.iter().all(|event| event["schema_version"] == 1));
    assert!(events
        .iter()
        .all(|event| event["request_id"] == "req-stream"));
    assert_eq!(
        events
            .iter()
            .map(|event| event["seq"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn attached_stream_emits_configured_keep_alive_and_disconnects_cleanly() {
    let (app, service) = fixture(ApiAuth::disabled(), 4).await;
    let response = app
        .oneshot(request(
            Method::POST,
            "/v1/agents/blocking/runs/stream",
            Some(json!({"text":"hello"})),
        ))
        .await
        .unwrap();
    let run_id = response.headers()["x-run-id"].to_str().unwrap().to_string();
    let mut body = response.into_body().into_data_stream();

    let encoded = tokio::time::timeout(Duration::from_millis(250), async {
        let mut encoded = String::new();
        while !encoded.contains(": keep-alive") {
            let chunk = body.next().await.unwrap().unwrap();
            encoded.push_str(&String::from_utf8_lossy(&chunk));
        }
        encoded
    })
    .await
    .expect("configured keepalive must be emitted");
    assert!(encoded.contains(": keep-alive"));
    drop(body);
    tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_status(&service, &run_id, RunStatus::Cancelled),
    )
    .await
    .expect("dropping SSE must cancel the Attached Run");
}

#[tokio::test]
async fn event_replay_route_and_recovery_headers_are_not_supported() {
    let (app, service) = fixture(ApiAuth::disabled(), 4).await;
    let created = app
        .clone()
        .oneshot(request(
            Method::POST,
            "/v1/agents/fast/runs",
            Some(json!({"text":"hello"})),
        ))
        .await
        .unwrap();
    let created = json_body(created).await;
    let run_id = created["data"]["run_id"].as_str().unwrap();
    wait_for_status(&service, run_id, RunStatus::Completed).await;

    for uri in [
        format!("/v1/runs/{run_id}/events"),
        format!("/v1/runs/{run_id}/events?after_seq=0"),
    ] {
        let response = app
            .clone()
            .oneshot(request(Method::GET, &uri, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    let mut request = request(Method::GET, &format!("/v1/runs/{run_id}/events"), None);
    request
        .headers_mut()
        .insert("last-event-id", "3".parse().unwrap());
    assert_eq!(
        app.oneshot(request).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn cancellation_is_idempotent_and_capacity_maps_to_run_conflict() {
    let (app, service) = fixture(ApiAuth::disabled(), 1).await;
    let first = app
        .clone()
        .oneshot(request(
            Method::POST,
            "/v1/agents/blocking/runs",
            Some(json!({"text":"hello"})),
        ))
        .await
        .unwrap();
    let first = json_body(first).await;
    let run_id = first["data"]["run_id"].as_str().unwrap();
    wait_for_status(&service, run_id, RunStatus::Running).await;

    let conflict = app
        .clone()
        .oneshot(request(
            Method::POST,
            "/v1/agents/blocking/runs",
            Some(json!({"text":"second"})),
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(json_body(conflict).await["code"], "RUN_CONFLICT");

    for _ in 0..2 {
        let cancelled = app
            .clone()
            .oneshot(request(Method::DELETE, &format!("/v1/runs/{run_id}"), None))
            .await
            .unwrap();
        assert_eq!(cancelled.status(), StatusCode::OK);
        assert_eq!(json_body(cancelled).await["data"]["status"], "cancelled");
    }
}
