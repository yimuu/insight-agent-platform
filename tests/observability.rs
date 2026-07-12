use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::Path,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    dsl::compiler::{AgentCompiler, CompileLimits},
    events::hub::{EventHub, EventHubConfig},
    history::{repository::RunRepository, sqlite::SqliteRunRepository, types::RunStatus},
    nodes::default_node_registries,
    resources::{
        actions::{Action, ActionContext, ActionDescriptor, ActionRegistry},
        models::{ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry},
    },
    runtime::{CompiledAgentRegistry, RequestMetadata, RunError, RunService, RunServiceConfig},
};
use serde_json::{json, Value};
use tempfile::{tempdir, TempDir};
use tracing::{
    field::{Field, Visit},
    Event, Level, Subscriber,
};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    Layer, Registry,
};

const INPUT_SECRET: &str = "observability-input-secret";
const OUTPUT_SECRET: &str = "observability-output-secret";
const CHAT_PROMPT_SECRET: &str = "observability-prompt-secret";
const CHAT_RESPONSE_SECRET: &str = "observability-response-secret";
const IMAGE_SECRET: &str = "observability-image-secret";
const USAGE_SECRET: &str = "observability-usage-secret";

#[derive(Debug, Clone)]
struct RecordedEvent {
    level: Level,
    fields: BTreeMap<String, String>,
}

impl RecordedEvent {
    fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }
}

#[derive(Clone)]
struct RecordingLayer {
    events: Arc<Mutex<Vec<RecordedEvent>>>,
}

struct FieldRecorder<'a> {
    fields: &'a mut BTreeMap<String, String>,
}

impl Visit for FieldRecorder<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for RecordingLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut FieldRecorder {
            fields: &mut fields,
        });
        self.events.lock().unwrap().push(RecordedEvent {
            level: *event.metadata().level(),
            fields,
        });
    }
}

static RECORDED_EVENTS: OnceLock<Arc<Mutex<Vec<RecordedEvent>>>> = OnceLock::new();
static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

fn install_recorder() -> Arc<Mutex<Vec<RecordedEvent>>> {
    Arc::clone(RECORDED_EVENTS.get_or_init(|| {
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = Registry::default().with(RecordingLayer {
            events: Arc::clone(&events),
        });
        let _ = tracing::subscriber::set_global_default(subscriber);
        events
    }))
}

async fn reset_logs() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    install_recorder().lock().unwrap().clear();
    guard
}

fn logs() -> Vec<RecordedEvent> {
    install_recorder().lock().unwrap().clone()
}

fn info_logs(event_name: &str) -> Vec<RecordedEvent> {
    logs()
        .into_iter()
        .filter(|event| event.level == Level::INFO)
        .filter(|event| event.field("event_name") == Some(event_name))
        .collect()
}

fn assert_logs_exclude(secrets: &[&str]) {
    let rendered = format!("{:?}", logs());
    for secret in secrets {
        assert!(
            !rendered.contains(secret),
            "log output unexpectedly contained secret {secret}: {rendered}"
        );
    }
}

fn write_agent(root: &Path, id: &str, yaml_tail: &str) {
    let directory = root.join(id);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("agent.yaml"),
        format!(
            r#"version: 1
id: {id}
name: {id}
input:
  schema:
    type: object
    additionalProperties: true
{yaml_tail}
"#
        ),
    )
    .unwrap();
}

#[derive(Clone)]
struct ObservabilityAction;

#[async_trait]
impl Action for ObservabilityAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            name: "observability.action",
            input_schema: json!({"type":"object", "additionalProperties":true}),
            output_schema: json!({"type":"object", "additionalProperties":true}),
            idempotent: true,
            streams_content: false,
        }
    }

    async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
        if input.get("fail").and_then(Value::as_bool) == Some(true) {
            Err(RunError::new(
                "OBSERVABILITY_ACTION_FAILED",
                "synthetic action failure",
            ))
        } else {
            Ok(json!({"secret": OUTPUT_SECRET, "ok": true}))
        }
    }
}

#[derive(Clone)]
struct ObservabilityModel;

impl fmt::Debug for ObservabilityModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ObservabilityModel")
    }
}

#[async_trait]
impl ChatModel for ObservabilityModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::from([ModelCapability::Vision])
    }

    fn validate_parameters(
        &self,
        _parameters: &Value,
    ) -> Result<(), insight_agent_platform::dsl::CompileError> {
        Ok(())
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::iter([
            Ok(ChatChunk {
                text: "safe".to_string(),
                finish_reason: None,
                usage: None,
            }),
            Ok(ChatChunk {
                text: CHAT_RESPONSE_SECRET.to_string(),
                finish_reason: Some("stop".to_string()),
                usage: Some(json!({"detail": USAGE_SECRET})),
            }),
        ])))
    }
}

struct Fixture {
    _root: TempDir,
    service: RunService,
}

async fn fixture(agent_ids: &[&str]) -> Fixture {
    let root = tempdir().unwrap();
    write_agent(
        root.path(),
        "linear",
        r#"entry: prepare
nodes:
  prepare:
    type: core.template
    next: result
    config:
      value: "{{ input.secret }}"
  result:
    type: core.output
    config:
      data:
        secret: "{{ nodes.prepare.output }}"
"#,
    );
    write_agent(
        root.path(),
        "failure",
        r#"entry: call
nodes:
  call:
    type: core.action
    next: result
    config:
      action: observability.action
      input: {fail: true, secret: "{{ input.secret }}"}
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
    );
    write_agent(
        root.path(),
        "parallel",
        r#"entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {a: branch_a, b: branch_b}
      join: collect
  branch_a:
    type: core.template
    next: collect
    config: {value: a}
  branch_b:
    type: core.template
    next: collect
    config: {value: b}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
    );
    write_agent(
        root.path(),
        "chat",
        r#"entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    config:
      model: obs
      parameters: {temperature: 0}
      messages:
        - role: user
          content:
            - type: text
              text: "prompt={{ input.secret }}"
            - type: image_url
              image_url: {url: "https://example.test/observability-image-secret.png"}
  result:
    type: core.output
    config:
      data:
        text: "{{ nodes.answer.output.text }}"
"#,
    );

    let mut actions = ActionRegistry::default();
    actions.register(ObservabilityAction).unwrap();
    let mut models = ModelRegistry::default();
    models.register("obs", ObservabilityModel).unwrap();
    let (node_types, executors) = default_node_registries().unwrap();
    let compiler = AgentCompiler::new(
        node_types,
        models,
        actions,
        Duration::from_secs(5),
        CompileLimits {
            max_fork_branches: 8,
        },
    );
    let agents = agent_ids
        .iter()
        .map(|id| Arc::new(compiler.compile_dir(&root.path().join(id)).unwrap()))
        .collect();
    let agents = CompiledAgentRegistry::new(agents).unwrap();
    let database_path = root.path().join("history.sqlite3");
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
            max_concurrent_runs: 4,
            max_parallel_node_executions: 8,
            max_parallel_branches_per_run: 4,
            run_timeout: Duration::from_secs(5),
        },
    )
    .unwrap();
    Fixture {
        _root: root,
        service,
    }
}

async fn wait_for_status(service: &RunService, run_id: &str, expected: RunStatus) {
    for _ in 0..200 {
        if service.get_run(run_id).await.unwrap().status == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("run {run_id} did not reach {expected:?}");
}

#[tokio::test]
async fn run_and_node_info_logs_are_body_free_for_linear_success() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["linear"]).await;
    let created = fixture
        .service
        .create_detached(
            "linear",
            json!({"secret": INPUT_SECRET}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Completed).await;

    let started = info_logs("run.started");
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].field("run_id"), Some(created.run_id.as_str()));
    assert_eq!(started[0].field("agent_id"), Some("linear"));

    let finished = info_logs("run.finished");
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].field("status"), Some("completed"));
    assert_eq!(finished[0].field("error_code"), Some(""));
    assert!(
        finished[0]
            .field("elapsed_ms")
            .unwrap()
            .parse::<u64>()
            .unwrap()
            < 30_000
    );
    assert!(
        finished[0]
            .field("output_bytes")
            .unwrap()
            .parse::<usize>()
            .unwrap()
            > 0
    );

    let node_completed = info_logs("node.completed");
    assert_eq!(node_completed.len(), 2);
    assert!(node_completed
        .iter()
        .all(|event| event.field("run_id") == Some(created.run_id.as_str())));
    assert!(node_completed.iter().any(|event| {
        event.field("node_id") == Some("prepare")
            && event.field("kind") == Some("core.template")
            && event
                .field("output_bytes")
                .unwrap()
                .parse::<usize>()
                .unwrap()
                > 0
    }));
    assert!(node_completed.iter().any(|event| {
        event.field("node_id") == Some("result")
            && event.field("kind") == Some("core.output")
            && event
                .field("output_bytes")
                .unwrap()
                .parse::<usize>()
                .unwrap()
                > 0
    }));
    assert_logs_exclude(&[
        INPUT_SECRET,
        OUTPUT_SECRET,
        CHAT_PROMPT_SECRET,
        CHAT_RESPONSE_SECRET,
        IMAGE_SECRET,
        USAGE_SECRET,
    ]);
}

#[tokio::test]
async fn run_and_node_info_logs_are_body_free_for_action_failure() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["failure"]).await;
    let created = fixture
        .service
        .create_detached(
            "failure",
            json!({"secret": INPUT_SECRET}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Failed).await;

    let node_failed = info_logs("node.failed");
    assert_eq!(node_failed.len(), 1);
    assert_eq!(node_failed[0].field("node_id"), Some("call"));
    assert_eq!(
        node_failed[0].field("error_code"),
        Some("OBSERVABILITY_ACTION_FAILED")
    );
    assert_eq!(node_failed[0].field("error_kind"), Some("node"));

    let finished = info_logs("run.finished");
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].field("status"), Some("failed"));
    assert_eq!(
        finished[0].field("error_code"),
        Some("OBSERVABILITY_ACTION_FAILED")
    );
    assert_eq!(finished[0].field("output_bytes"), Some("0"));
    assert_logs_exclude(&[
        INPUT_SECRET,
        OUTPUT_SECRET,
        CHAT_PROMPT_SECRET,
        CHAT_RESPONSE_SECRET,
        IMAGE_SECRET,
        USAGE_SECRET,
    ]);
}

#[tokio::test]
async fn parallel_nodes_emit_once_and_run_finish_is_once() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["parallel"]).await;
    let created = fixture
        .service
        .create_detached("parallel", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Completed).await;

    let completed = info_logs("node.completed");
    for node_id in ["fanout", "branch_a", "branch_b", "collect", "result"] {
        assert_eq!(
            completed
                .iter()
                .filter(|event| event.field("node_id") == Some(node_id))
                .count(),
            1,
            "node {node_id} should have one completion log"
        );
    }
    assert_eq!(info_logs("run.finished").len(), 1);
}

#[tokio::test]
async fn chat_info_logs_counts_and_sizes_without_bodies() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["chat"]).await;
    let created = fixture
        .service
        .create_detached(
            "chat",
            json!({"secret": CHAT_PROMPT_SECRET}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Completed).await;

    let requests = info_logs("chat.request");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].field("node_id"), Some("answer"));
    assert_eq!(requests[0].field("messages_count"), Some("1"));
    assert_eq!(requests[0].field("image_parts_count"), Some("1"));
    assert_eq!(requests[0].field("parameters_keys_count"), Some("1"));

    let responses = info_logs("chat.response");
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].field("node_id"), Some("answer"));
    assert_eq!(responses[0].field("chunks_count"), Some("2"));
    assert_eq!(responses[0].field("finish_reason_present"), Some("true"));
    assert!(
        responses[0]
            .field("text_bytes")
            .unwrap()
            .parse::<usize>()
            .unwrap()
            >= CHAT_RESPONSE_SECRET.len()
    );
    assert!(
        responses[0]
            .field("usage_bytes")
            .unwrap()
            .parse::<usize>()
            .unwrap()
            > 0
    );
    assert_logs_exclude(&[
        CHAT_PROMPT_SECRET,
        CHAT_RESPONSE_SECRET,
        IMAGE_SECRET,
        USAGE_SECRET,
    ]);
}
