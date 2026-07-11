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
const INPUT_CODE: &str = "ACTION_INPUT_INVALID";
const INPUT_MESSAGE: &str = "action input validation failed";
const OUTPUT_CODE: &str = "ACTION_OUTPUT_INVALID";
const OUTPUT_MESSAGE: &str = "action output validation failed";

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

#[derive(Debug)]
struct RawRun {
    error_code: Option<String>,
    error_message: Option<String>,
}

#[derive(Debug)]
struct RawEvent {
    event_type: String,
    code: String,
    message: String,
    data: String,
}

#[derive(Debug)]
struct RawHistory {
    run: RawRun,
    events: Vec<RawEvent>,
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
    let mut last_observed = None;
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let observed = service.get_run(run_id).await.unwrap().status;
            last_observed = Some(observed);
            if observed == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    if result.is_err() {
        panic!("run {run_id} did not reach {expected:?}; last observed status: {last_observed:?}");
    }
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

fn sse_events(body: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(body)
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str(data).unwrap())
        .collect()
}

fn json_event<'a>(events: &'a [Value], event_type: &str) -> &'a Value {
    let matches = events
        .iter()
        .filter(|event| event["type"] == event_type)
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one {event_type} SSE event: {events:?}"
    );
    matches[0]
}

fn assert_json_failure_event(
    event: &Value,
    event_type: &str,
    code: &str,
    message: &str,
    secret: &str,
) {
    assert_eq!(event["type"], event_type);
    assert_eq!(event["code"], code);
    assert_eq!(event["message"], message);
    assert!(
        !event.to_string().contains(secret),
        "secret leaked in {event_type} SSE event: {event}"
    );
}

async fn raw_history(database_url: &str, run_id: &str) -> RawHistory {
    let pool = SqlitePool::connect(database_url).await.unwrap();
    let run: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT error_code, error_message FROM runs WHERE run_id = ?")
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
    RawHistory {
        run: RawRun {
            error_code: run.0,
            error_message: run.1,
        },
        events: events
            .into_iter()
            .map(|(event_type, code, message, data)| RawEvent {
                event_type,
                code,
                message,
                data,
            })
            .collect(),
    }
}

fn raw_event<'a>(history: &'a RawHistory, event_type: &str) -> &'a RawEvent {
    let matches = history
        .events
        .iter()
        .filter(|event| event.event_type == event_type)
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one {event_type} history event: {history:?}"
    );
    matches[0]
}

fn assert_raw_run_failure(history: &RawHistory, code: &str, message: &str, secret: &str) {
    assert_eq!(history.run.error_code.as_deref(), Some(code));
    assert_eq!(history.run.error_message.as_deref(), Some(message));
    assert!(
        !history
            .run
            .error_code
            .as_deref()
            .unwrap_or_default()
            .contains(secret),
        "secret leaked in raw Run error code: {history:?}"
    );
    assert!(
        !history
            .run
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains(secret),
        "secret leaked in raw Run error message: {history:?}"
    );
}

fn assert_raw_failure_event(
    history: &RawHistory,
    event_type: &str,
    code: &str,
    message: &str,
    secret: &str,
) {
    let event = raw_event(history, event_type);
    assert_eq!(event.code, code);
    assert_eq!(event.message, message);
    for (field, value) in [
        ("code", event.code.as_str()),
        ("message", event.message.as_str()),
        ("data", event.data.as_str()),
    ] {
        assert!(
            !value.contains(secret),
            "secret leaked in raw {event_type}.{field}: {event:?}"
        );
    }
}

fn assert_raw_event_data_is_safe(history: &RawHistory, secret: &str) {
    for event in &history.events {
        assert!(
            !event.data.contains(secret),
            "secret leaked in raw {}.data: {event:?}",
            event.event_type
        );
    }
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
    let attached_events = sse_events(&attached_body);
    assert!(!attached_events.is_empty());
    for event in &attached_events {
        assert!(
            !event.to_string().contains(INPUT_SECRET),
            "secret leaked in SSE event: {event}"
        );
    }
    assert_json_failure_event(
        json_event(&attached_events, "node.failed"),
        "node.failed",
        INPUT_CODE,
        INPUT_MESSAGE,
        INPUT_SECRET,
    );
    assert_json_failure_event(
        json_event(&attached_events, "run.failed"),
        "run.failed",
        INPUT_CODE,
        INPUT_MESSAGE,
        INPUT_SECRET,
    );
    assert!(fixture.calls.lock().unwrap().is_empty());
    let attached_history = raw_history(&fixture.database_url, &attached_run_id).await;
    assert_raw_run_failure(&attached_history, INPUT_CODE, INPUT_MESSAGE, INPUT_SECRET);
    assert_raw_failure_event(
        &attached_history,
        "node.failed",
        INPUT_CODE,
        INPUT_MESSAGE,
        INPUT_SECRET,
    );
    assert_raw_failure_event(
        &attached_history,
        "run.failed",
        INPUT_CODE,
        INPUT_MESSAGE,
        INPUT_SECRET,
    );
    assert_raw_event_data_is_safe(&attached_history, INPUT_SECRET);

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
    let detached = json_body(detached).await;
    assert_eq!(detached["data"]["error_code"], OUTPUT_CODE);
    assert_eq!(detached["data"]["error_message"], OUTPUT_MESSAGE);
    assert!(
        !detached.to_string().contains(OUTPUT_SECRET),
        "secret leaked in Detached GET: {detached}"
    );
    let detached_history = raw_history(&fixture.database_url, &detached_run_id).await;
    assert_raw_run_failure(
        &detached_history,
        OUTPUT_CODE,
        OUTPUT_MESSAGE,
        OUTPUT_SECRET,
    );
    assert_raw_failure_event(
        &detached_history,
        "node.failed",
        OUTPUT_CODE,
        OUTPUT_MESSAGE,
        OUTPUT_SECRET,
    );
    assert_raw_failure_event(
        &detached_history,
        "run.failed",
        OUTPUT_CODE,
        OUTPUT_MESSAGE,
        OUTPUT_SECRET,
    );
    assert_raw_event_data_is_safe(&detached_history, OUTPUT_SECRET);

    let parallel_run_id = create_detached(&fixture.app, "parallel-output", PARALLEL_SECRET).await;
    wait_for_status(&fixture.service, &parallel_run_id, RunStatus::Completed).await;
    let parallel_history = raw_history(&fixture.database_url, &parallel_run_id).await;
    assert_eq!(parallel_history.run.error_code, None);
    assert_eq!(parallel_history.run.error_message, None);
    assert_raw_failure_event(
        &parallel_history,
        "branch.failed",
        OUTPUT_CODE,
        OUTPUT_MESSAGE,
        PARALLEL_SECRET,
    );
    assert_raw_event_data_is_safe(&parallel_history, PARALLEL_SECRET);

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
