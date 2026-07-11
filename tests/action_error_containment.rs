use std::{
    fmt,
    io::{self, Write},
    path::Path,
    sync::{Arc, Mutex},
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
    dsl::compiler::{AgentCompiler, CompileLimits},
    events::hub::{EventHub, EventHubConfig},
    history::{repository::RunRepository, sqlite::SqliteRunRepository, types::RunStatus},
    nodes::default_node_registries,
    resources::{
        actions::{Action, ActionContext, ActionDescriptor, ActionRegistry},
        models::ModelRegistry,
    },
    runtime::{CompiledAgentRegistry, RunError, RunService, RunServiceConfig},
};
use serde_json::{json, Value};
use sqlx::SqlitePool;
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;
use tracing_subscriber::fmt::MakeWriter;

const INPUT_SECRET: &str = "attached-input-never-expose";
const OUTPUT_SECRET: &str = "detached-output-never-expose";
const PARALLEL_SECRET: &str = "parallel-output-never-expose";

#[derive(Clone)]
struct ContainmentAction {
    calls: Arc<Mutex<Vec<Value>>>,
}

impl fmt::Debug for ContainmentAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContainmentAction")
    }
}

#[async_trait]
impl Action for ContainmentAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            name: "containment",
            input_schema: json!({
                "type":"object",
                "required":["mode","payload"],
                "additionalProperties":false,
                "properties":{
                    "mode":{"enum":["valid","invalid_output"]},
                    "payload":{"type":"object"}
                }
            }),
            output_schema: json!({
                "type":"object",
                "required":["result"],
                "additionalProperties":false,
                "properties":{"result":{"type":"integer"}}
            }),
            idempotent: true,
            streams_content: false,
        }
    }

    async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
        self.calls.lock().unwrap().push(input.clone());
        if input["mode"].as_str() == Some("invalid_output") {
            Ok(json!({"result":input["payload"]["secret"].clone()}))
        } else {
            Ok(json!({"result":1}))
        }
    }
}

#[derive(Clone)]
struct BufferWriter(Arc<Mutex<Vec<u8>>>);

struct BufferGuard(Arc<Mutex<Vec<u8>>>);

impl Write for BufferGuard {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for BufferWriter {
    type Writer = BufferGuard;

    fn make_writer(&'a self) -> Self::Writer {
        BufferGuard(Arc::clone(&self.0))
    }
}

struct Fixture {
    _root: TempDir,
    app: Router,
    service: RunService,
    database_url: String,
    calls: Arc<Mutex<Vec<Value>>>,
    logs: Arc<Mutex<Vec<u8>>>,
}

fn write_agent(root: &Path, id: &str, nodes: &str) {
    let directory = root.join(id);
    std::fs::create_dir_all(&directory).unwrap();
    let yaml = format!(
        r#"version: 1
id: {id}
name: {id}
input:
  schema:
    type: object
    required: [secret]
    additionalProperties: false
    properties:
      secret: {{type: string}}
{nodes}
"#
    );
    std::fs::write(directory.join("agent.yaml"), yaml).unwrap();
}

fn write_agents(root: &Path) {
    write_agent(
        root,
        "linear-input",
        r#"entry: call
nodes:
  call:
    type: core.action
    next: result
    config:
      action: containment
      input: {mode: valid, payload: "{{ input.secret }}"}
  result:
    type: core.output
    config: {data: {ok: true}}
"#,
    );
    write_agent(
        root,
        "linear-output",
        r#"entry: call
nodes:
  call:
    type: core.action
    next: result
    config:
      action: containment
      input:
        mode: invalid_output
        payload: {secret: "{{ input.secret }}"}
  result:
    type: core.output
    config: {data: {ok: true}}
"#,
    );
    write_agent(
        root,
        "parallel-output",
        r#"entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {bad: bad_action, good: good_action}
      join: collect
  bad_action:
    type: core.action
    next: collect
    config:
      action: containment
      input:
        mode: invalid_output
        payload: {secret: "{{ input.secret }}"}
  good_action:
    type: core.action
    next: collect
    config:
      action: containment
      input:
        mode: valid
        payload: {secret: "{{ input.secret }}"}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config: {data: {ok: true}}
"#,
    );
    write_agent(
        root,
        "success",
        r#"entry: call
nodes:
  call:
    type: core.action
    next: result
    config:
      action: containment
      input:
        mode: valid
        payload: {secret: "{{ input.secret }}"}
  result:
    type: core.output
    config: {data: {ok: true}}
"#,
    );
}

async fn fixture() -> Fixture {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_writer(BufferWriter(Arc::clone(&logs)))
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let root = tempdir().unwrap();
    write_agents(root.path());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut actions = ActionRegistry::default();
    actions
        .register(ContainmentAction {
            calls: Arc::clone(&calls),
        })
        .unwrap();
    let (node_types, executors) = default_node_registries().unwrap();
    let compiler = AgentCompiler::new(
        node_types,
        ModelRegistry::default(),
        actions,
        Duration::from_secs(5),
        CompileLimits {
            max_fork_branches: 8,
        },
    );
    let agents = [
        "linear-input",
        "linear-output",
        "parallel-output",
        "success",
    ]
    .into_iter()
    .map(|id| Arc::new(compiler.compile_dir(&root.path().join(id)).unwrap()))
    .collect();
    let agents = CompiledAgentRegistry::new(agents).unwrap();

    let database_path = root.path().join("history.sqlite3");
    let database_url = format!("sqlite://{}", database_path.display());
    let repository = Arc::new(
        SqliteRunRepository::connect_path(&database_path)
            .await
            .unwrap(),
    );
    let repository_trait: Arc<dyn RunRepository> = repository;
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 32,
            journal_capacity: 64,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let service = RunService::new(
        agents,
        executors,
        repository_trait,
        events,
        RunServiceConfig {
            max_concurrent_runs: 2,
            max_parallel_node_executions: 8,
            max_parallel_branches_per_run: 4,
            run_timeout: Duration::from_secs(5),
        },
    )
    .unwrap();
    let app = build_router(FormalApiState {
        service: service.clone(),
        auth: ApiAuth::disabled(),
        sse_keep_alive_interval: Duration::from_millis(10),
    });

    Fixture {
        _root: root,
        app,
        service,
        database_url,
        calls,
        logs,
    }
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
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn wait_for_status(service: &RunService, run_id: &str, expected: RunStatus) {
    for _ in 0..200 {
        if service.get_run(run_id).await.unwrap().status == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("run {run_id} did not reach {expected:?}");
}

async fn create_detached(app: &Router, agent_id: &str, secret: &str) -> String {
    let response = app
        .clone()
        .oneshot(request(
            Method::POST,
            &format!("/v1/agents/{agent_id}/runs"),
            Some(json!({"secret":secret})),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    json_body(response).await["data"]["run_id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn raw_history(database_url: &str, run_id: &str) -> String {
    let pool = SqlitePool::connect(database_url).await.unwrap();
    let run: (Option<String>,) = sqlx::query_as("SELECT error_message FROM runs WHERE run_id = ?")
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let events: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT event_type, code, message, data FROM run_events WHERE run_id = ? ORDER BY seq",
    )
    .bind(run_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    pool.close().await;
    format!("{run:?}{events:?}")
}

fn assert_safe_failure(encoded: &str, code: &str, message: &str, secret: &str) {
    assert!(encoded.contains(code), "missing {code}: {encoded}");
    assert!(encoded.contains(message), "missing {message}: {encoded}");
    assert!(!encoded.contains(secret), "secret leaked: {encoded}");
}

#[tokio::test]
async fn action_validation_instances_never_escape_the_runtime_boundary() {
    let fixture = fixture().await;

    let attached = fixture
        .app
        .clone()
        .oneshot(request(
            Method::POST,
            "/v1/agents/linear-input/runs/stream",
            Some(json!({"secret":INPUT_SECRET})),
        ))
        .await
        .unwrap();
    assert_eq!(attached.status(), StatusCode::OK);
    let attached_run_id = attached.headers()["x-run-id"].to_str().unwrap().to_string();
    let attached_body = tokio::time::timeout(
        Duration::from_secs(1),
        to_bytes(attached.into_body(), usize::MAX),
    )
    .await
    .unwrap()
    .unwrap();
    let attached_text = String::from_utf8(attached_body.to_vec()).unwrap();
    assert_safe_failure(
        &attached_text,
        "ACTION_INPUT_INVALID",
        "action input validation failed",
        INPUT_SECRET,
    );
    assert!(attached_text.contains("node.failed"));
    assert!(attached_text.contains("run.failed"));
    assert!(fixture.calls.lock().unwrap().is_empty());
    let attached_history = raw_history(&fixture.database_url, &attached_run_id).await;
    assert_safe_failure(
        &attached_history,
        "ACTION_INPUT_INVALID",
        "action input validation failed",
        INPUT_SECRET,
    );

    let detached_run_id = create_detached(&fixture.app, "linear-output", OUTPUT_SECRET).await;
    wait_for_status(&fixture.service, &detached_run_id, RunStatus::Failed).await;
    let detached = fixture
        .app
        .clone()
        .oneshot(request(
            Method::GET,
            &format!("/v1/runs/{detached_run_id}"),
            None,
        ))
        .await
        .unwrap();
    let detached_text = json_body(detached).await.to_string();
    assert_safe_failure(
        &detached_text,
        "ACTION_OUTPUT_INVALID",
        "action output validation failed",
        OUTPUT_SECRET,
    );
    let detached_history = raw_history(&fixture.database_url, &detached_run_id).await;
    assert_safe_failure(
        &detached_history,
        "ACTION_OUTPUT_INVALID",
        "action output validation failed",
        OUTPUT_SECRET,
    );

    let parallel_run_id = create_detached(&fixture.app, "parallel-output", PARALLEL_SECRET).await;
    wait_for_status(&fixture.service, &parallel_run_id, RunStatus::Completed).await;
    let parallel_history = raw_history(&fixture.database_url, &parallel_run_id).await;
    assert!(parallel_history.contains("branch.failed"));
    assert_safe_failure(
        &parallel_history,
        "ACTION_OUTPUT_INVALID",
        "action output validation failed",
        PARALLEL_SECRET,
    );

    let success_run_id = create_detached(&fixture.app, "success", "safe-success-value").await;
    wait_for_status(&fixture.service, &success_run_id, RunStatus::Completed).await;
    assert!(fixture.calls.lock().unwrap().len() >= 4);

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
    let logs = String::from_utf8(fixture.logs.lock().unwrap().clone()).unwrap();
    for secret in [INPUT_SECRET, OUTPUT_SECRET, PARALLEL_SECRET] {
        assert!(!logs.contains(secret), "secret leaked to tracing: {logs}");
    }
}
