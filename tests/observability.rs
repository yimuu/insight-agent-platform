use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::{stream, StreamExt};
use insight_agent_platform::{
    dsl::compiler::{AgentCompiler, CompileLimits},
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::RunEventType,
    },
    history::{repository::RunRepository, sqlite::SqliteRunRepository, types::RunStatus},
    nodes::default_node_registries,
    resources::{
        actions::{Action, ActionContext, ActionDescriptor, ActionRegistry},
        models::{ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry},
        openai_chat::{OpenAiChatLimits, OpenAiChatModel, OpenAiTransportPolicy},
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
const DYNAMIC_TEXT_SECRET: &str = "observability-dynamic-text-secret";
const DYNAMIC_IMAGE_SECRET: &str = "observability-dynamic-image-secret";
const DYNAMIC_MESSAGE_SECRET: &str = "observability-invalid-dynamic-secret";
const SELECT_SECRET: &str = "observability-select-secret";
const WORKFLOW_FAILURE_INPUT: &str = "observability-workflow-input-secret";
const WORKFLOW_FAILURE_MESSAGE: &str = "observability workflow failure message";

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
struct ObservabilityModel {
    calls: Arc<AtomicUsize>,
}

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
        self.calls.fetch_add(1, Ordering::Relaxed);
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
    repository: Arc<SqliteRunRepository>,
    model_calls: Arc<AtomicUsize>,
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
    type: core.end
    config:
      outcome: success
      data:
        secret: "{{ nodes.prepare.output }}"
"#,
    );
    write_agent(
        root.path(),
        "select",
        r#"entry: route
nodes:
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: left}]
      default: right
  left:
    type: core.template
    next: selected
    config: {value: observability-select-secret}
  right:
    type: core.template
    next: selected
    config: {value: unused}
  selected:
    type: core.select
    next: result
    config: {sources: [left, right]}
  result:
    type: core.end
    config:
      outcome: success
      data: {value: "{{ nodes.selected.output.value }}"}
"#,
    );
    write_agent(
        root.path(),
        "action_success",
        r#"entry: call
nodes:
  call:
    type: core.action
    next: result
    config:
      action: observability.action
      input: {secret: "{{ input.secret }}"}
  result:
    type: core.end
    config:
      outcome: success
      data:
        action_ok: "{{ nodes.call.output.ok }}"
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
    type: core.end
    config:
      outcome: success
      data: {ok: true}
"#,
    );
    write_agent(
        root.path(),
        "authored_failure",
        r#"entry: result
nodes:
  result:
    type: core.end
    config:
      outcome: failure
      code: WORKFLOW_OBSERVABILITY_FAILED
      message: observability workflow failure message
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
    next: end_a
    config: {value: a}
  end_a:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.branch_a.output }}"}}
  branch_b:
    type: core.template
    next: end_b
    config: {value: b}
  end_b:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.branch_b.output }}"}}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.end
    config:
      outcome: success
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
    type: core.end
    config:
      outcome: success
      data:
        text: "{{ nodes.answer.output.text }}"
"#,
    );
    write_agent(
        root.path(),
        "chat_optional_image",
        r#"entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    config:
      model: obs
      messages:
        - role: user
          content:
            - type: text
              text: "prompt={{ input.secret }}"
            - type: image_url
              optional: true
              image_url: {url: "{{ input.image_url }}"}
  result:
    type: core.end
    config:
      outcome: success
      data:
        text: "{{ nodes.answer.output.text }}"
"#,
    );
    write_agent(
        root.path(),
        "chat_empty_after_optional_image",
        r#"entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    config:
      model: obs
      messages:
        - role: user
          content:
            - type: image_url
              optional: true
              image_url: {url: "{{ input.image_url }}"}
  result:
    type: core.end
    config:
      outcome: success
      data:
        text: "{{ nodes.answer.output.text }}"
"#,
    );
    write_agent(
        root.path(),
        "chat_dynamic",
        r#"entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    config:
      model: obs
      messages:
        - role: system
          content: system
        - from:
            path: input.messages
            allowed_content: [text, image_url]
  result:
    type: core.end
    config:
      outcome: success
      data:
        text: "{{ nodes.answer.output.text }}"
"#,
    );
    write_agent(
        root.path(),
        "chat_dynamic_invalid",
        r#"entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    config:
      model: obs
      messages:
        - from: {path: input.messages}
  result:
    type: core.end
    config: {outcome: success, data: {ok: true}}
"#,
    );

    let mut actions = ActionRegistry::default();
    actions.register(ObservabilityAction).unwrap();
    let model_calls = Arc::new(AtomicUsize::new(0));
    let mut models = ModelRegistry::default();
    models
        .register(
            "obs",
            ObservabilityModel {
                calls: Arc::clone(&model_calls),
            },
        )
        .unwrap();
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
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
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
        repository,
        model_calls,
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
            && event.field("kind") == Some("core.end")
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
async fn select_info_logs_record_metadata_without_selected_bodies() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["select"]).await;
    let created = fixture
        .service
        .create_detached("select", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Completed).await;

    let completed = info_logs("node.completed");
    let selected = completed
        .iter()
        .find(|event| event.field("node_id") == Some("selected"))
        .expect("Select must emit one completion log");
    assert_eq!(selected.field("kind"), Some("core.select"));
    assert!(
        selected
            .field("output_bytes")
            .unwrap()
            .parse::<usize>()
            .unwrap()
            > 0
    );
    assert_logs_exclude(&[SELECT_SECRET]);
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
async fn authored_end_failure_uses_production_terminal_path_without_info_log_bodies() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["authored_failure"]).await;
    let created = fixture
        .service
        .create_detached(
            "authored_failure",
            json!({"secret": WORKFLOW_FAILURE_INPUT}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Failed).await;

    let failed = fixture.service.get_run(&created.run_id).await.unwrap();
    assert_eq!(failed.status, RunStatus::Failed);
    assert_eq!(
        failed.error_code.as_deref(),
        Some("WORKFLOW_OBSERVABILITY_FAILED")
    );
    assert_eq!(
        failed.error_message.as_deref(),
        Some(WORKFLOW_FAILURE_MESSAGE)
    );
    assert_eq!(failed.output, None);

    let events = fixture
        .repository
        .list_events_after(&created.run_id, 0, 100)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::NodeCompleted,
            RunEventType::RunFailed,
        ]
    );
    assert_eq!(events[3].node_id.as_deref(), Some("result"));
    assert_eq!(events[4].data, json!({"kind":"workflow"}));
    assert_eq!(events[4].code, "WORKFLOW_OBSERVABILITY_FAILED");
    assert_eq!(events[4].message, WORKFLOW_FAILURE_MESSAGE);

    let completed = info_logs("node.completed");
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].field("node_id"), Some("result"));
    assert_eq!(completed[0].field("kind"), Some("core.end"));
    assert!(info_logs("node.failed").is_empty());

    let finished = info_logs("run.finished");
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].field("status"), Some("failed"));
    assert_eq!(
        finished[0].field("error_code"),
        Some("WORKFLOW_OBSERVABILITY_FAILED")
    );
    assert_eq!(finished[0].field("output_bytes"), Some("0"));
    assert_logs_exclude(&[WORKFLOW_FAILURE_MESSAGE, WORKFLOW_FAILURE_INPUT]);
}

#[tokio::test]
async fn action_success_logs_node_bytes_without_action_bodies() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["action_success"]).await;
    let created = fixture
        .service
        .create_detached(
            "action_success",
            json!({"secret": INPUT_SECRET}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Completed).await;

    let node_completed = info_logs("node.completed");
    let action_log = node_completed
        .iter()
        .find(|event| event.field("node_id") == Some("call"))
        .expect("action node should emit node.completed");
    assert_eq!(action_log.field("kind"), Some("core.action"));
    assert_eq!(
        action_log
            .field("output_bytes")
            .unwrap()
            .parse::<usize>()
            .unwrap(),
        serde_json::to_vec(&json!({"secret": OUTPUT_SECRET, "ok": true}))
            .unwrap()
            .len()
    );
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

#[tokio::test]
async fn omitted_optional_image_logs_zero_rendered_image_parts_without_bodies() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["chat_optional_image"]).await;
    let created = fixture
        .service
        .create_detached(
            "chat_optional_image",
            json!({"secret": CHAT_PROMPT_SECRET}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Completed).await;

    assert_eq!(fixture.model_calls.load(Ordering::Relaxed), 1);
    let requests = info_logs("chat.request");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].field("node_id"), Some("answer"));
    assert_eq!(requests[0].field("messages_count"), Some("1"));
    assert_eq!(requests[0].field("image_parts_count"), Some("0"));
    assert_logs_exclude(&[
        CHAT_PROMPT_SECRET,
        CHAT_RESPONSE_SECRET,
        IMAGE_SECRET,
        USAGE_SECRET,
    ]);
}

#[tokio::test]
async fn message_emptied_by_optional_image_filtering_logs_no_request_or_model_call() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["chat_empty_after_optional_image"]).await;
    let created = fixture
        .service
        .create_detached(
            "chat_empty_after_optional_image",
            json!({}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Failed).await;

    let failed = fixture.service.get_run(&created.run_id).await.unwrap();
    assert_eq!(
        failed.error_code.as_deref(),
        Some("CHAT_CONTENT_PARTS_EMPTY")
    );
    assert_eq!(fixture.model_calls.load(Ordering::Relaxed), 0);
    assert!(info_logs("chat.request").is_empty());
    assert!(info_logs("chat.response").is_empty());
    assert_logs_exclude(&[
        CHAT_PROMPT_SECRET,
        CHAT_RESPONSE_SECRET,
        IMAGE_SECRET,
        USAGE_SECRET,
    ]);
}

#[tokio::test]
async fn dynamic_chat_logs_final_counts_without_bodies() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["chat_dynamic"]).await;
    let created = fixture
        .service
        .create_detached(
            "chat_dynamic",
            json!({"messages":[
                {"role":"assistant", "content":DYNAMIC_TEXT_SECRET},
                {"role":"user", "content":[
                    {"type":"text", "text":"look"},
                    {"type":"image_url", "image_url":{
                        "url":format!("https://example.test/{DYNAMIC_IMAGE_SECRET}.png")
                    }}
                ]}
            ]}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Completed).await;

    assert_eq!(fixture.model_calls.load(Ordering::Relaxed), 1);
    let requests = info_logs("chat.request");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].field("messages_count"), Some("3"));
    assert_eq!(requests[0].field("image_parts_count"), Some("1"));
    assert_logs_exclude(&[
        DYNAMIC_TEXT_SECRET,
        DYNAMIC_IMAGE_SECRET,
        CHAT_RESPONSE_SECRET,
    ]);
}

#[tokio::test]
async fn invalid_dynamic_chat_logs_no_request_or_body() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["chat_dynamic_invalid"]).await;
    let created = fixture
        .service
        .create_detached(
            "chat_dynamic_invalid",
            json!({"messages":[{
                "role":"user",
                "content":DYNAMIC_MESSAGE_SECRET,
                "extra":true
            }]}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_status(&fixture.service, &created.run_id, RunStatus::Failed).await;

    let failed = fixture.service.get_run(&created.run_id).await.unwrap();
    assert_eq!(
        failed.error_code.as_deref(),
        Some("CHAT_DYNAMIC_MESSAGES_INVALID")
    );
    assert_eq!(fixture.model_calls.load(Ordering::Relaxed), 0);
    assert!(info_logs("chat.request").is_empty());
    assert!(info_logs("chat.response").is_empty());
    assert_logs_exclude(&[DYNAMIC_MESSAGE_SECRET]);
}

async fn openai_model_with_response(response_body: String) -> OpenAiChatModel {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0_u8; 2048];
        let _ = socket.read(&mut buffer).await.unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });

    OpenAiChatModel::new_with_limits_and_transport_policy(
        Some("provider-secret-key".to_string()),
        format!("http://{address}/v1"),
        "obs-provider".to_string(),
        BTreeSet::new(),
        Duration::from_secs(1),
        Duration::from_secs(1),
        OpenAiChatLimits::default(),
        OpenAiTransportPolicy::AllowLoopbackHttp,
    )
    .unwrap()
}

#[tokio::test]
async fn openai_provider_logs_response_metadata_without_body_or_key() {
    let _guard = reset_logs().await;
    let response_body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{CHAT_RESPONSE_SECRET}\"}},\"finish_reason\":null}}]}}\n\n\
         data: {{\"choices\":[{{\"delta\":{{}},\"finish_reason\":\"stop\"}}],\"usage\":{{\"detail\":\"{USAGE_SECRET}\"}}}}\n\n"
    );
    let model = openai_model_with_response(response_body.clone()).await;
    let mut stream = model
        .stream_chat(ChatRequest {
            messages: vec![
                insight_agent_platform::resources::models::ChatMessage::from_text(
                    insight_agent_platform::resources::models::ChatRole::User,
                    CHAT_PROMPT_SECRET,
                ),
            ],
            parameters: json!({}),
        })
        .await
        .unwrap();
    while stream.next().await.transpose().unwrap().is_some() {}

    let requests = info_logs("openai.request");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].field("model"), Some("obs-provider"));
    assert_eq!(requests[0].field("messages_count"), Some("1"));
    assert_eq!(requests[0].field("image_parts_count"), Some("0"));
    assert_eq!(requests[0].field("parameters_keys_count"), Some("0"));

    let responses = info_logs("openai.response");
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].field("model"), Some("obs-provider"));
    assert_eq!(
        responses[0]
            .field("upstream_bytes")
            .unwrap()
            .parse::<usize>()
            .unwrap(),
        response_body.len()
    );
    assert_eq!(responses[0].field("chunks_count"), Some("2"));
    assert!(
        responses[0]
            .field("usage_bytes")
            .unwrap()
            .parse::<usize>()
            .unwrap()
            > 0
    );
    assert_logs_exclude(&[
        "provider-secret-key",
        CHAT_PROMPT_SECRET,
        CHAT_RESPONSE_SECRET,
        USAGE_SECRET,
    ]);
}
