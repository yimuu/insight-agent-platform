use std::{
    collections::BTreeSet,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use insight_agent_platform::{
    api::formal::{build_router, ApiAuth, FormalApiState},
    catalog::AgentCatalog,
    dsl::vnext::compiler::WorkflowCompiler,
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::RunEventType,
    },
    history::{
        repository::RunRepository,
        sqlite::SqliteRunRepository,
        types::{RunLifecycle, RunRecord, RunStatus},
    },
    outcome::FailureKind,
    resources::{
        actions::{
            Action, ActionContext, ActionDescriptor, ActionRegistry, CancellationClass,
            EffectClass, IdempotencyClass,
        },
        models::ModelRegistry,
    },
    runtime::{RequestMetadata, RunError, RunService, RunServiceConfig, StopReason},
};
use serde_json::{json, Value};
use sqlx::SqlitePool;
use tempfile::{tempdir, TempDir};
use tokio::{sync::Notify, time::sleep};
use tower::ServiceExt;

const AUTH_TOKEN: &str = "delivery-contract-token";
const PRIVATE_FAILURE_DIAGNOSTIC: &str = "PRIVATE_OPERATION_DIAGNOSTIC_7f74a6d5_MUST_NOT_BE_PUBLIC";
const PUBLIC_OPERATION_FAILURE_MESSAGE: &str = "operation failed";
const TEST_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Default)]
struct OperationTracker {
    blocking_started: AtomicUsize,
    action_started: AtomicUsize,
    action_stopped: AtomicUsize,
    action_cleaned: AtomicUsize,
    action_returned: AtomicUsize,
    changed: Notify,
}

impl OperationTracker {
    async fn wait_for_blocking_started(&self, expected: usize) {
        tokio::time::timeout(TEST_TIMEOUT, async {
            while self.blocking_started.load(Ordering::SeqCst) < expected {
                self.changed.notified().await;
            }
        })
        .await
        .expect("blocking operation did not start before the deadline");
    }

    async fn wait_for_action_started(&self) {
        tokio::time::timeout(TEST_TIMEOUT, async {
            while self.action_started.load(Ordering::SeqCst) == 0 {
                self.changed.notified().await;
            }
        })
        .await
        .expect("action.call did not start before the deadline");
    }
}

struct CooperativeCancelAction {
    tracker: Arc<OperationTracker>,
}

#[async_trait]
impl Action for CooperativeCancelAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: "test.cooperative_cancel",
            version: "1.0.0",
            input_schema: json!({
                "type": "object",
                "required": ["text"],
                "properties": {"text": {"type": "string"}},
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "required": ["ok"],
                "properties": {"ok": {"type": "boolean"}},
                "additionalProperties": false
            }),
            effect: EffectClass::Mutating,
            idempotency: IdempotencyClass::NonIdempotent,
            cancellation: CancellationClass::Cooperative,
            required_capabilities: BTreeSet::new(),
        }
    }

    async fn call(&self, _input: Value, context: ActionContext) -> Result<Value, RunError> {
        self.tracker.action_started.fetch_add(1, Ordering::SeqCst);
        self.tracker.changed.notify_waiters();
        context.control.stopped().await;
        assert_eq!(context.control.stop_reason(), Some(StopReason::Cancelled));
        self.tracker.action_stopped.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(25)).await;
        self.tracker.action_cleaned.fetch_add(1, Ordering::SeqCst);
        self.tracker.action_returned.fetch_add(1, Ordering::SeqCst);
        self.tracker.changed.notify_waiters();
        Ok(json!({"ok": true}))
    }
}

struct DeliveryAction {
    tracker: Arc<OperationTracker>,
}

#[async_trait]
impl Action for DeliveryAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: "test.delivery",
            version: "1.0.0",
            input_schema: json!({
                "type": "object",
                "required": ["mode"],
                "properties": {"mode": {"type": "string"}},
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "required": ["ok"],
                "properties": {"ok": {"type": "boolean"}},
                "additionalProperties": false
            }),
            effect: EffectClass::Pure,
            idempotency: IdempotencyClass::Idempotent,
            cancellation: CancellationClass::Cooperative,
            required_capabilities: BTreeSet::new(),
        }
    }

    async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
        match input.get("mode").and_then(Value::as_str) {
            Some("complete") => Ok(json!({"ok": true})),
            Some("block") => {
                self.tracker.blocking_started.fetch_add(1, Ordering::SeqCst);
                self.tracker.changed.notify_waiters();
                context.control.stopped().await;
                Err(RunError::stopped(
                    context
                        .control
                        .stop_reason()
                        .expect("a released blocking operation has a stop reason"),
                ))
            }
            Some("fail") => Err(RunError::operation(
                "TEST_OPERATION_FAILED",
                PRIVATE_FAILURE_DIAGNOSTIC,
            )),
            _ => Err(RunError::operation(
                "TEST_DELIVERY_CONFIG_INVALID",
                "test delivery operation config is invalid",
            )),
        }
    }
}

struct Fixture {
    app: Router,
    service: RunService,
    repository: Arc<SqliteRunRepository>,
    tracker: Arc<OperationTracker>,
    database_url: String,
    _directory: TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempdir().unwrap();
        let path = directory.path().join("delivery-contract.db");
        let database_url = format!("sqlite://{}?mode=rwc", path.display());
        let repository = Arc::new(SqliteRunRepository::connect(&database_url).await.unwrap());
        let repository_trait: Arc<dyn RunRepository> = repository.clone();
        let events = EventHub::new(
            Arc::clone(&repository_trait),
            EventHubConfig {
                subscriber_capacity: 16,
                journal_capacity: 64,
                journal_batch_size: 8,
                operation_timeout: Duration::from_secs(1),
            },
        );

        let tracker = Arc::new(OperationTracker::default());
        let mut actions = ActionRegistry::default();
        actions
            .register(DeliveryAction {
                tracker: Arc::clone(&tracker),
            })
            .unwrap();
        actions
            .register(CooperativeCancelAction {
                tracker: Arc::clone(&tracker),
            })
            .unwrap();
        let compiler = WorkflowCompiler::new(ModelRegistry::default(), actions);
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut agents = [
            ("complete_agent", "complete"),
            ("blocking_agent", "block"),
            ("failing_agent", "fail"),
        ]
        .into_iter()
        .map(|(id, mode)| {
            Arc::new(
                compiler
                    .compile_source(root, &agent_source(id, mode))
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
        agents.push(Arc::new(
            compiler
                .compile_source(root, &action_cancel_agent_source())
                .unwrap(),
        ));
        let agents = AgentCatalog::new(agents).unwrap();
        let service = RunService::new(
            agents,
            repository_trait,
            events,
            RunServiceConfig {
                max_concurrent_runs: 4,
                max_concurrent_operations: 8,
                max_concurrent_operations_per_run: 4,
                operation_timeout: Duration::from_secs(30),
                operation_cancel_grace_period: Duration::from_millis(100),
                max_template_output_bytes: 64 * 1024,
                run_timeout: Duration::from_secs(30),
            },
        )
        .unwrap();
        let app = build_router(FormalApiState {
            service: service.clone(),
            auth: ApiAuth::bearer_token(AUTH_TOKEN),
            sse_keep_alive_interval: Duration::from_millis(10),
            readiness_probe_timeout: Duration::from_secs(1),
        });

        Self {
            app,
            service,
            repository,
            tracker,
            database_url,
            _directory: directory,
        }
    }

    async fn shutdown(&self) {
        tokio::time::timeout(TEST_TIMEOUT, self.service.shutdown(TEST_TIMEOUT))
            .await
            .expect("service shutdown exceeded the outer deadline")
            .unwrap();
    }
}

fn agent_source(id: &str, mode: &str) -> String {
    format!(
        r#"
api_version: insight.agent/v2
kind: agent
metadata:
  id: {id}
  name: {id}
  description: Delivery contract fixture.
schema_dialect: https://json-schema.org/draft/2020-12/schema
input:
  schema:
    type: object
    required: [text]
    properties:
      text: {{type: string}}
    additionalProperties: true
output:
  data_schema:
    type: object
    required: [ok]
    properties:
      ok: {{type: boolean}}
    additionalProperties: false
workflow:
  steps:
    - kind: action
      id: work
      call: test.delivery
      inputs:
        mode:
          template:
            text: {mode}
  result:
    return:
      content: {{literal: done}}
      format: text
      data: {{from: steps.work.output}}
"#
    )
}

fn action_cancel_agent_source() -> String {
    r#"
api_version: insight.agent/v2
kind: agent
metadata:
  id: action_cancel_agent
  name: Action Cancel Agent
  description: Verifies cooperative action.call cancellation.
schema_dialect: https://json-schema.org/draft/2020-12/schema
input:
  schema:
    type: object
    required: [text]
    properties:
      text: {type: string}
    additionalProperties: false
output:
  data_schema:
    type: object
    required: [ok]
    properties:
      ok: {type: boolean}
    additionalProperties: false
workflow:
  steps:
    - kind: action
      id: work
      call: test.cooperative_cancel
      inputs:
        text: {from: input.text}
  result:
    return:
      content: {literal: done}
      format: text
      data: {from: steps.work.output}
"#
    .to_string()
}

fn request(method: Method, uri: &str, body: Body, authorized: bool) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if authorized {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {AUTH_TOKEN}"));
    }
    builder.body(body).unwrap()
}

fn json_request(method: Method, uri: &str, body: Value) -> Request<Body> {
    let mut request = request(
        method,
        uri,
        Body::from(serde_json::to_vec(&body).unwrap()),
        true,
    );
    request
        .headers_mut()
        .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    request
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn wait_for_status(service: &RunService, run_id: &str, expected: RunStatus) -> RunRecord {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let record = service.get_run(run_id).await.unwrap();
            if record.status() == expected {
                return record;
            }
            assert!(
                !record.status().is_terminal(),
                "run reached unexpected terminal status {:?}",
                record.status()
            );
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("run did not reach the expected status before the deadline")
}

#[tokio::test]
async fn protected_routes_require_bearer_and_reject_invalid_json_before_creating_a_run() {
    let fixture = Fixture::new().await;

    let unauthorized = fixture
        .app
        .clone()
        .oneshot(request(Method::GET, "/v1/agents", Body::empty(), false))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(json_body(unauthorized).await["code"], "UNAUTHORIZED");

    let mut malformed = request(
        Method::POST,
        "/v1/agents/complete_agent/runs",
        Body::from("{"),
        true,
    );
    malformed
        .headers_mut()
        .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    let malformed = fixture.app.clone().oneshot(malformed).await.unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(malformed).await["code"], "INPUT_INVALID");

    let schema_invalid = fixture
        .app
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/v1/agents/complete_agent/runs",
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(schema_invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(schema_invalid).await["code"], "INPUT_INVALID");

    let inspector = SqlitePool::connect(&fixture.database_url).await.unwrap();
    let run_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs")
        .fetch_one(&inspector)
        .await
        .unwrap();
    assert_eq!(
        run_count, 0,
        "invalid requests must not create durable runs"
    );
    inspector.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn delete_is_idempotent_and_commits_one_cancelled_terminal() {
    let fixture = Fixture::new().await;
    let created = fixture
        .app
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/v1/agents/blocking_agent/runs",
            json!({"text": "cancel me"}),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::ACCEPTED);
    let created = json_body(created).await;
    let run_id = created["data"]["run_id"].as_str().unwrap().to_string();
    fixture.tracker.wait_for_blocking_started(1).await;

    let uri = format!("/v1/runs/{run_id}");
    let first = fixture
        .app
        .clone()
        .oneshot(request(Method::DELETE, &uri, Body::empty(), true))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first = json_body(first).await;
    assert_eq!(first["data"]["status"], "cancelled");

    let second = fixture
        .app
        .clone()
        .oneshot(request(Method::DELETE, &uri, Body::empty(), true))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second = json_body(second).await;
    assert_eq!(second["data"], first["data"]);

    let events = fixture
        .repository
        .list_events_after(&run_id, 0, 100)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == RunEventType::RunCancelled)
            .count(),
        1,
        "repeated DELETE must not create another terminal event"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn action_call_receives_stop_drains_cleanup_and_cannot_complete_after_cancel() {
    let fixture = Fixture::new().await;
    let created = fixture
        .app
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/v1/agents/action_cancel_agent/runs",
            json!({"text": "cancel the real action"}),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::ACCEPTED);
    let run_id = json_body(created).await["data"]["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    fixture.tracker.wait_for_action_started().await;

    let cancelled = fixture
        .app
        .clone()
        .oneshot(request(
            Method::DELETE,
            &format!("/v1/runs/{run_id}"),
            Body::empty(),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::OK);
    assert_eq!(json_body(cancelled).await["data"]["status"], "cancelled");

    assert_eq!(fixture.tracker.action_stopped.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tracker.action_cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tracker.action_returned.load(Ordering::SeqCst), 1);

    let record = fixture.service.get_run(&run_id).await.unwrap();
    assert!(matches!(record.lifecycle, RunLifecycle::Cancelled { .. }));
    let events = fixture
        .repository
        .list_events_after(&run_id, 0, 100)
        .await
        .unwrap();
    assert!(!events.iter().any(|event| {
        matches!(
            event.event_type,
            RunEventType::OperationCompleted | RunEventType::RunCompleted
        )
    }));
    assert!(!serde_json::to_string(&events)
        .unwrap()
        .contains("\"ok\":true"));

    let inspector = SqlitePool::connect(&fixture.database_url).await.unwrap();
    let stored_output: Option<String> =
        sqlx::query_scalar("SELECT output FROM runs WHERE run_id = ?")
            .bind(&run_id)
            .fetch_one(&inspector)
            .await
            .unwrap();
    assert!(stored_output.is_none());
    inspector.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn dropping_attached_http_response_cancels_and_drains_the_run() {
    let fixture = Fixture::new().await;
    let response = fixture
        .app
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/v1/agents/blocking_agent/runs/stream",
            json!({"text": "disconnect me"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let run_id = response.headers()["x-run-id"].to_str().unwrap().to_string();
    fixture.tracker.wait_for_blocking_started(1).await;

    drop(response);

    let cancelled = wait_for_status(&fixture.service, &run_id, RunStatus::Cancelled).await;
    assert_eq!(cancelled.status(), RunStatus::Cancelled);
    let events = fixture
        .repository
        .list_events_after(&run_id, 0, 100)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == RunEventType::RunCancelled)
            .count(),
        1
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn run_service_sqlite_row_contains_only_input_shape_metadata() {
    const PLAIN_SECRET: &str = "plain-input-secret-18f4c792";
    const NESTED_SECRET: &str = "nested-input-secret-29a6d8e3";
    const ESCAPED_SECRET: &str = "escaped-input-secret-\"quote\\slash\nline-3bc7e1f5";

    let fixture = Fixture::new().await;
    let input = json!({
        "text": PLAIN_SECRET,
        "nested": {"token": NESTED_SECRET},
        "escaped": ESCAPED_SECRET,
        "count": 7
    });
    let expected_summary = json!({
        "keys": ["count", "escaped", "nested", "text"],
        "serialized_bytes": serde_json::to_vec(&input).unwrap().len()
    });
    let created = fixture
        .service
        .create_detached("complete_agent", input, RequestMetadata::default())
        .await
        .unwrap();
    assert_eq!(created.input_summary, expected_summary);
    let completed = wait_for_status(&fixture.service, &created.run_id, RunStatus::Completed).await;
    assert_eq!(completed.input_summary, expected_summary);

    let inspector = SqlitePool::connect(&fixture.database_url).await.unwrap();
    let (raw_summary, raw_output): (String, Option<String>) =
        sqlx::query_as("SELECT input_summary, output FROM runs WHERE run_id = ?")
            .bind(&created.run_id)
            .fetch_one(&inspector)
            .await
            .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&raw_summary).unwrap(),
        expected_summary
    );
    let event_surfaces: Vec<(String, String)> =
        sqlx::query_as("SELECT message, data FROM run_events WHERE run_id = ? ORDER BY seq")
            .bind(&created.run_id)
            .fetch_all(&inspector)
            .await
            .unwrap();
    let durable_text = format!("{raw_summary}{raw_output:?}{event_surfaces:?}");
    for secret in [PLAIN_SECRET, NESTED_SECRET, ESCAPED_SECRET] {
        assert!(
            !durable_text.contains(secret),
            "durable SQLite surfaces contain original input content"
        );
    }
    inspector.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn operation_failure_uses_fixed_public_message_and_never_persists_private_diagnostic() {
    let fixture = Fixture::new().await;
    let created = fixture
        .service
        .create_detached(
            "failing_agent",
            json!({"text": "trigger failure"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let failed = wait_for_status(&fixture.service, &created.run_id, RunStatus::Failed).await;
    let RunLifecycle::Failed { error } = &failed.lifecycle else {
        panic!("expected failed run lifecycle")
    };
    assert_eq!(error.kind, FailureKind::Operation);
    assert_eq!(error.code, "TEST_OPERATION_FAILED");
    assert_eq!(error.message, PUBLIC_OPERATION_FAILURE_MESSAGE);
    assert!(!serde_json::to_string(&failed)
        .unwrap()
        .contains(PRIVATE_FAILURE_DIAGNOSTIC));

    let events = fixture
        .repository
        .list_events_after(&created.run_id, 0, 100)
        .await
        .unwrap();
    for event_type in [RunEventType::OperationFailed, RunEventType::RunFailed] {
        let event = events
            .iter()
            .find(|event| event.event_type == event_type)
            .expect("failure event must be durable");
        assert_eq!(event.code, "TEST_OPERATION_FAILED");
        assert_eq!(event.message, PUBLIC_OPERATION_FAILURE_MESSAGE);
    }
    assert!(!serde_json::to_string(&events)
        .unwrap()
        .contains(PRIVATE_FAILURE_DIAGNOSTIC));

    let inspector = SqlitePool::connect(&fixture.database_url).await.unwrap();
    let (raw_run_message, raw_event_text): (String, String) = sqlx::query_as(
        "SELECT r.error_message,
                GROUP_CONCAT(e.message || e.data, '')
         FROM runs r
         JOIN run_events e ON e.run_id = r.run_id
         WHERE r.run_id = ?
         GROUP BY r.run_id, r.error_message",
    )
    .bind(&created.run_id)
    .fetch_one(&inspector)
    .await
    .unwrap();
    assert_eq!(raw_run_message, PUBLIC_OPERATION_FAILURE_MESSAGE);
    assert!(!raw_event_text.contains(PRIVATE_FAILURE_DIAGNOSTIC));
    inspector.close().await;
    fixture.shutdown().await;
}
