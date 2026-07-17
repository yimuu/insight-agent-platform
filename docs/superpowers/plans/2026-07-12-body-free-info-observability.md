# Body-free INFO Observability Implementation Plan

> **Historical implementation record:** the privacy principle remains current, but node-era names and authored examples below are superseded. Current LLM/Action authoring and privacy contracts are defined by [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add body-free structured INFO logs for Run, node, chat, and OpenAI-compatible provider lifecycle metadata.

**Architecture:** Add small crate-local observability helpers for byte sizing and elapsed milliseconds, then emit structured `tracing::info!` records at existing execution boundaries. Verify with a test-only recording subscriber that captures structured fields and asserts counts, byte sizes, and absence of fixture bodies/secrets.

**Tech Stack:** Rust, Tokio, `tracing`, `tracing-subscriber`, `serde_json`, existing `RunCoordinator`, `runtime::execution`, `core.chat`, and `OpenAiChatModel`.

## Global Constraints

- A7 is structured logging only; do not add metrics, OpenTelemetry, exporters, dashboards, sampling, or retention policy.
- Logs may contain identifiers, enum/status values, elapsed durations, counts, and byte lengths.
- Logs must not contain raw request input values, rendered prompts/messages, model response text, action input/output bodies, event payload bodies, full URLs, query strings, headers, credentials, API keys, bearer tokens, or configured secrets.
- Use UTF-8/JSON byte semantics: string size is `str::len()`, JSON size is `serde_json::to_vec(value).len()`, Run output size is serialized `RunOutput`, usage size is serialized usage JSON.
- Serialization failure while computing log byte fields must produce `0` and must not change runtime behavior.
- Elapsed fields use monotonic `Instant` and integer milliseconds with saturating conversion.
- Every A7 structured INFO record must include `event_name = "..."` so tests do not rely on formatted message strings.
- Public runtime behavior and API response shapes must not change.

---

## File Structure

- Create `src/observability.rs`
  - Owns crate-local helpers: `json_size_bytes`, `elapsed_ms`, and `duration_ms`.
  - Contains unit tests for byte and saturating duration behavior.
- Modify `src/lib.rs`
  - Exposes `observability` as `pub(crate)`.
- Modify `src/runtime/coordinator.rs`
  - Emits `run.started` and `run.finished` INFO records.
- Modify `src/runtime/execution.rs`
  - Emits `node.completed` and `node.failed` INFO records.
- Modify `src/nodes/chat.rs`
  - Emits `chat.request` and `chat.response` INFO records.
- Modify `src/resources/openai_chat.rs`
  - Extends provider request metadata and emits provider response metadata.
- Create `tests/observability.rs`
  - Provides a global recording subscriber and representative service/model/action/provider scenarios.
- Modify `README.md`
  - Documents the body-free INFO logging boundary.

---

### Task 1: Add observability byte/time helpers

**Files:**
- Create: `src/observability.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Produces:
  - `pub(crate) fn json_size_bytes<T: serde::Serialize>(value: &T) -> usize`
  - `pub(crate) fn duration_ms(duration: std::time::Duration) -> u64`
  - `pub(crate) fn elapsed_ms(started: std::time::Instant) -> u64`
- Consumed by later tasks from `runtime::coordinator`, `runtime::execution`, `nodes::chat`, and `resources::openai_chat`.

- [ ] **Step 1: Write failing helper tests**

Create `src/observability.rs` with tests first:

```rust
use std::time::{Duration, Instant};

pub(crate) fn json_size_bytes<T: serde::Serialize>(_value: &T) -> usize {
    unimplemented!("A7 observability helper not implemented")
}

pub(crate) fn duration_ms(_duration: Duration) -> u64 {
    unimplemented!("A7 observability helper not implemented")
}

pub(crate) fn elapsed_ms(_started: Instant) -> u64 {
    unimplemented!("A7 observability helper not implemented")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde_json::json;

    #[test]
    fn json_size_counts_serialized_bytes_without_exposing_body() {
        let value = json!({"secret":"body-free-secret","n":1});
        assert_eq!(
            json_size_bytes(&value),
            serde_json::to_vec(&value).unwrap().len()
        );
    }

    #[test]
    fn json_size_returns_zero_when_serialization_fails() {
        #[derive(Serialize)]
        struct FailingNumber {
            value: f64,
        }

        let value = FailingNumber { value: f64::NAN };
        assert_eq!(json_size_bytes(&value), 0);
    }

    #[test]
    fn duration_ms_saturates_to_u64_max() {
        let huge = Duration::from_millis(u64::MAX).saturating_add(Duration::from_millis(1));
        assert_eq!(duration_ms(huge), u64::MAX);
    }
}
```

- [ ] **Step 2: Register the module and verify tests fail because helpers are unimplemented**

Add this line to `src/lib.rs`:

```rust
pub(crate) mod observability;
```

Run:

```bash
cargo test observability -- --nocapture
```

Expected: FAIL with panic messages containing `A7 observability helper not implemented`.

- [ ] **Step 3: Implement the helpers**

Replace the helper bodies in `src/observability.rs`:

```rust
use std::time::{Duration, Instant};

pub(crate) fn json_size_bytes<T: serde::Serialize>(value: &T) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

pub(crate) fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn elapsed_ms(started: Instant) -> u64 {
    duration_ms(started.elapsed())
}
```

- [ ] **Step 4: Verify helper tests pass**

Run:

```bash
cargo test observability -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/lib.rs src/observability.rs
git commit -m "feat: add observability byte helpers"
```

---

### Task 2: Add Run and node lifecycle INFO records

**Files:**
- Create: `tests/observability.rs`
- Modify: `src/runtime/coordinator.rs`
- Modify: `src/runtime/execution.rs`

**Interfaces:**
- Consumes: `observability::{elapsed_ms, json_size_bytes}`.
- Produces structured INFO fields:
  - Run: `event_name`, `run_id`, `request_id`, `agent_id`, `agent_version`, `attachment`, `status`, `elapsed_ms`, `output_bytes`, `error_code`.
  - Node: `event_name`, `run_id`, `request_id`, `agent_id`, `agent_version`, `node_id`, `kind`, `elapsed_ms`, `output_bytes`, `error_code`, `error_kind`.

- [ ] **Step 1: Create the recording subscriber test harness**

Create `tests/observability.rs` with this harness and fixtures:

```rust
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
        self.fields.insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields.insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields.insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields.insert(field.name().to_string(), value.to_string());
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
        event.record(&mut FieldRecorder { fields: &mut fields });
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
        }
    }

    async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
        if input.get("fail").and_then(Value::as_bool) == Some(true) {
            Err(RunError::new("OBSERVABILITY_ACTION_FAILED", "synthetic action failure"))
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
```

- [ ] **Step 2: Write failing Run/node linear success test**

Append this test to `tests/observability.rs`:

```rust
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
    assert!(finished[0]
        .field("elapsed_ms")
        .unwrap()
        .parse::<u64>()
        .unwrap()
        < 30_000);
    assert!(finished[0]
        .field("output_bytes")
        .unwrap()
        .parse::<usize>()
        .unwrap()
        > 0);

    let node_completed = info_logs("node.completed");
    assert_eq!(node_completed.len(), 2);
    assert!(node_completed.iter().all(|event| event.field("run_id") == Some(created.run_id.as_str())));
    assert!(node_completed.iter().any(|event| {
        event.field("node_id") == Some("prepare")
            && event.field("kind") == Some("core.template")
            && event.field("output_bytes").unwrap().parse::<usize>().unwrap() > 0
    }));
    assert!(node_completed.iter().any(|event| {
        event.field("node_id") == Some("result")
            && event.field("kind") == Some("core.output")
            && event.field("output_bytes").unwrap().parse::<usize>().unwrap() > 0
    }));
    assert_logs_exclude(&[INPUT_SECRET, OUTPUT_SECRET]);
}
```

- [ ] **Step 3: Run the linear observability test and verify it fails**

Run:

```bash
cargo test --test observability run_and_node_info_logs_are_body_free_for_linear_success -- --nocapture
```

Expected: FAIL because `run.started`, `run.finished`, and `node.completed` records are absent.

- [ ] **Step 4: Implement Run lifecycle logs**

In `src/runtime/coordinator.rs`, add imports:

```rust
use std::{sync::Arc, time::Instant};

use crate::observability::{elapsed_ms, json_size_bytes};
```

Thread an `Instant` through `execute_managed`, `execute_inner`, `complete`, `finish_error`, and `recover_infrastructure_failure`:

```rust
async fn execute_managed(
    &self,
    new_run: NewRun,
    input: Value,
    stop: StopSignal,
    state: Arc<RunState>,
) -> Result<RunStatus, RunError> {
    let started = Instant::now();
    match self
        .execute_inner(&new_run, input, stop, Arc::clone(&state), started)
        .await
    {
        Ok(status) => Ok(status),
        Err(error) => {
            tracing::error!(
                run_id = new_run.run_id,
                code = error.code(),
                "run infrastructure failed; recovering durable terminal state"
            );
            self.recover_infrastructure_failure(&state, &new_run, started)
                .await
        }
    }
}
```

After `RunStarted` is published in `execute_inner`, emit:

```rust
tracing::info!(
    event_name = "run.started",
    run_id = new_run.run_id.as_str(),
    request_id = new_run.request_id.as_str(),
    agent_id = new_run.agent_id.as_str(),
    agent_version = new_run.agent_version.as_str(),
    attachment = new_run.attachment.as_str(),
    "run started"
);
```

At the end of successful completion, after `commit_terminal_state` returns, emit:

```rust
let durable = self
    .commit_terminal_state(state, run, published, RunStatus::Completed)
    .await?;
tracing::info!(
    event_name = "run.finished",
    run_id = run.run_id.as_str(),
    request_id = run.request_id.as_str(),
    agent_id = run.agent_id.as_str(),
    agent_version = run.agent_version.as_str(),
    attachment = run.attachment.as_str(),
    status = durable.as_str(),
    elapsed_ms = elapsed_ms(started),
    output_bytes = json_size_bytes(&output),
    error_code = "",
    "run finished"
);
Ok(durable)
```

At the end of `finish_error`, use the same fields with `output_bytes = 0_usize` and `error_code = error.code()`.

At the end of `recover_infrastructure_failure`, use `status = durable.as_str()`, `output_bytes = 0_usize`, and `error_code = "INFRASTRUCTURE_FAILURE"`.

- [ ] **Step 5: Implement node lifecycle logs**

In `src/runtime/execution.rs`, add imports:

```rust
use std::{sync::Arc, time::Instant};

use crate::observability::{elapsed_ms, json_size_bytes};
```

In `execute_node_inner`, add:

```rust
let started = Instant::now();
let node_kind = node.kind.clone();
```

After `NodeFailed` event publication succeeds, before returning the failure, emit:

```rust
tracing::info!(
    event_name = "node.failed",
    run_id = context.metadata().run_id.as_str(),
    request_id = context.metadata().request_id.as_str(),
    agent_id = context.metadata().agent_id.as_str(),
    agent_version = context.metadata().agent_version.as_str(),
    node_id = node_id.as_str(),
    kind = node_kind.as_str(),
    elapsed_ms = elapsed_ms(started),
    error_code = error.code(),
    error_kind = run_error_kind(error.kind()),
    "node failed"
);
```

After `NodeCompleted` event publication succeeds, before returning `NodeExecutionResult`, emit:

```rust
tracing::info!(
    event_name = "node.completed",
    run_id = context.metadata().run_id.as_str(),
    request_id = context.metadata().request_id.as_str(),
    agent_id = context.metadata().agent_id.as_str(),
    agent_version = context.metadata().agent_version.as_str(),
    node_id = node_id.as_str(),
    kind = node_kind.as_str(),
    elapsed_ms = elapsed_ms(started),
    output_bytes = json_size_bytes(&outcome.output),
    "node completed"
);
```

Add helper:

```rust
fn run_error_kind(kind: RunErrorKind) -> &'static str {
    match kind {
        RunErrorKind::Node => "node",
        RunErrorKind::Stop => "stop",
        RunErrorKind::Infrastructure => "infrastructure",
    }
}
```

- [ ] **Step 6: Verify the linear test passes**

Run:

```bash
cargo test --test observability run_and_node_info_logs_are_body_free_for_linear_success -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Add and verify failure path test**

Append this test:

```rust
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
    assert_logs_exclude(&[INPUT_SECRET, OUTPUT_SECRET]);
}
```

Run:

```bash
cargo test --test observability run_and_node_info_logs_are_body_free_for_action_failure -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Add and verify parallel once-only test**

Append this test:

```rust
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
```

Run:

```bash
cargo test --test observability parallel_nodes_emit_once_and_run_finish_is_once -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Run all observability tests and commit**

Run:

```bash
cargo test --test observability -- --nocapture
```

Expected: PASS.

Commit:

```bash
git add src/runtime/coordinator.rs src/runtime/execution.rs tests/observability.rs
git commit -m "feat: add run and node info observability"
```

---

### Task 3: Add chat node INFO records

**Files:**
- Modify: `src/nodes/chat.rs`
- Modify: `tests/observability.rs`

**Interfaces:**
- Consumes: `observability::{elapsed_ms, json_size_bytes}` and `tests/observability.rs` recording helpers.
- Produces:
  - `chat.request`
  - `chat.response`

- [ ] **Step 1: Write failing chat observability test**

Append this test to `tests/observability.rs`:

```rust
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
    assert!(responses[0]
        .field("text_bytes")
        .unwrap()
        .parse::<usize>()
        .unwrap()
        >= CHAT_RESPONSE_SECRET.len());
    assert!(responses[0]
        .field("usage_bytes")
        .unwrap()
        .parse::<usize>()
        .unwrap()
        > 0);
    assert_logs_exclude(&[
        CHAT_PROMPT_SECRET,
        CHAT_RESPONSE_SECRET,
        IMAGE_SECRET,
        USAGE_SECRET,
    ]);
}
```

- [ ] **Step 2: Run the chat test and verify it fails**

Run:

```bash
cargo test --test observability chat_info_logs_counts_and_sizes_without_bodies -- --nocapture
```

Expected: FAIL because `chat.request` and `chat.response` records are absent.

- [ ] **Step 3: Implement chat request/response logs**

In `src/nodes/chat.rs`, add:

```rust
use std::{collections::BTreeSet, sync::Arc, time::Instant};

use crate::observability::{elapsed_ms, json_size_bytes};
```

After rendering `messages` and before `stream_chat`, add:

```rust
let messages_count = request.messages.len();
let image_parts_count = request
    .messages
    .iter()
    .map(|message| message.image_urls().len())
    .sum::<usize>();
let parameters_keys_count = request
    .parameters
    .as_object()
    .map_or(0, |parameters| parameters.len());
tracing::info!(
    event_name = "chat.request",
    run_id = context.metadata().run_id.as_str(),
    request_id = context.metadata().request_id.as_str(),
    agent_id = context.metadata().agent_id.as_str(),
    agent_version = context.metadata().agent_version.as_str(),
    node_id = node.id.as_str(),
    messages_count,
    image_parts_count,
    parameters_keys_count,
    "chat request metadata"
);
let chat_started = Instant::now();
```

In the stream loop, track:

```rust
let mut chunks_count = 0_usize;
let mut text_bytes = 0_usize;
```

For every received chunk, increment:

```rust
chunks_count += 1;
text_bytes = text_bytes.saturating_add(chunk.text.len());
```

Before returning `NodeOutcome`, compute final usage bytes and emit:

```rust
let usage_bytes = usage.as_ref().map_or(0, json_size_bytes);
tracing::info!(
    event_name = "chat.response",
    run_id = context.metadata().run_id.as_str(),
    request_id = context.metadata().request_id.as_str(),
    agent_id = context.metadata().agent_id.as_str(),
    agent_version = context.metadata().agent_version.as_str(),
    node_id = node.id.as_str(),
    chunks_count,
    text_bytes,
    usage_bytes,
    finish_reason_present = finish_reason.is_some(),
    elapsed_ms = elapsed_ms(chat_started),
    "chat response metadata"
);
```

- [ ] **Step 4: Verify chat test and prior observability tests**

Run:

```bash
cargo test --test observability chat_info_logs_counts_and_sizes_without_bodies -- --nocapture
cargo test --test observability -- --nocapture
```

Expected: both commands PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/nodes/chat.rs tests/observability.rs
git commit -m "feat: add chat info observability"
```

---

### Task 4: Add OpenAI provider INFO response metadata

**Files:**
- Modify: `src/resources/openai_chat.rs`
- Modify: `tests/observability.rs`

**Interfaces:**
- Consumes: `observability::{elapsed_ms, json_size_bytes}`.
- Produces provider INFO record with `event_name = "openai.response"`, `model`, `upstream_bytes`, `chunks_count`, `usage_bytes`, and `elapsed_ms`.

- [ ] **Step 1: Write failing provider observability test**

Append this test and helper to `tests/observability.rs`:

```rust
async fn openai_model_with_response(response_body: String) -> insight_agent_platform::resources::openai_chat::OpenAiChatModel {
    use insight_agent_platform::resources::openai_chat::OpenAiChatModel;
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

    OpenAiChatModel::new(
        Some("provider-secret-key".to_string()),
        format!("http://{address}/v1"),
        "obs-provider".to_string(),
        BTreeSet::new(),
        Duration::from_secs(1),
        Duration::from_secs(1),
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
            messages: vec![insight_agent_platform::resources::models::ChatMessage::from_text(
                insight_agent_platform::resources::models::ChatRole::User,
                CHAT_PROMPT_SECRET,
            )],
            parameters: json!({}),
        })
        .await
        .unwrap();
    while stream.next().await.transpose().unwrap().is_some() {}

    let responses = info_logs("openai.response");
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].field("model"), Some("obs-provider"));
    assert_eq!(
        responses[0].field("upstream_bytes").unwrap().parse::<usize>().unwrap(),
        response_body.len()
    );
    assert_eq!(responses[0].field("chunks_count"), Some("2"));
    assert!(responses[0]
        .field("usage_bytes")
        .unwrap()
        .parse::<usize>()
        .unwrap()
        > 0);
    assert_logs_exclude(&[
        "provider-secret-key",
        CHAT_PROMPT_SECRET,
        CHAT_RESPONSE_SECRET,
        USAGE_SECRET,
    ]);
}
```

- [ ] **Step 2: Run the provider test and verify it fails**

Run:

```bash
cargo test --test observability openai_provider_logs_response_metadata_without_body_or_key -- --nocapture
```

Expected: FAIL because `openai.response` is absent.

- [ ] **Step 3: Implement provider response metadata**

In `src/resources/openai_chat.rs`, add:

```rust
use std::{collections::BTreeSet, collections::VecDeque, fmt, time::Duration, time::Instant};

use crate::observability::{elapsed_ms, json_size_bytes};
```

Extend `StreamState`:

```rust
struct StreamState {
    bytes: std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    decoder: SseDecoder,
    pending: VecDeque<ChatChunk>,
    upstream_bytes: usize,
    limits: OpenAiChatLimits,
    model: String,
    started: Instant,
    chunks_count: usize,
    usage_bytes: usize,
}
```

Initialize the new fields:

```rust
StreamState {
    bytes: Box::pin(response.bytes_stream()),
    decoder: SseDecoder::new(self.limits),
    pending: VecDeque::new(),
    upstream_bytes: 0,
    limits: self.limits,
    model: self.model.clone(),
    started: Instant::now(),
    chunks_count: 0,
    usage_bytes: 0,
}
```

When yielding a pending chunk, count it without logging body:

```rust
if let Some(chunk) = state.pending.pop_front() {
    state.chunks_count += 1;
    state.usage_bytes = state
        .usage_bytes
        .saturating_add(chunk.usage.as_ref().map_or(0, json_size_bytes));
    return Ok(Some((chunk, state)));
}
```

When the upstream stream ends and no pending chunks remain, emit:

```rust
tracing::info!(
    event_name = "openai.response",
    model = state.model.as_str(),
    upstream_bytes = state.upstream_bytes,
    chunks_count = state.chunks_count,
    usage_bytes = state.usage_bytes,
    elapsed_ms = elapsed_ms(state.started),
    "OpenAI-compatible chat response metadata"
);
return Ok(None);
```

Keep the existing request log body-free. Add `parameters_keys_count` if the request parameters object is already available:

```rust
let parameters_keys_count = parameters.len();
tracing::info!(
    event_name = "openai.request",
    model = self.model,
    messages_count,
    image_parts_count,
    parameters_keys_count,
    "sending OpenAI-compatible chat request"
);
```

- [ ] **Step 4: Verify provider and all observability tests**

Run:

```bash
cargo test --test observability openai_provider_logs_response_metadata_without_body_or_key -- --nocapture
cargo test --test observability -- --nocapture
```

Expected: both commands PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/resources/openai_chat.rs tests/observability.rs
git commit -m "feat: add provider info observability"
```

---

### Task 5: Document body-free INFO logging and run final verification

**Files:**
- Modify: `README.md`
- Modify only if needed by formatter/clippy: files changed in Tasks 1-4.

**Interfaces:**
- Consumes: A7 implemented structured logs.
- Produces: README observability statement.

- [ ] **Step 1: Update README**

In `README.md`, after the paragraph describing health/runtime behavior or near the architecture section, add:

```markdown
INFO 日志是结构化且 body-free 的：Run、节点、Chat 和 provider 记录只包含 `run_id`、`request_id`、`agent_id`、`agent_version`、节点 ID/type、状态、耗时、计数和序列化字节数。日志不记录请求输入值、prompt、模型输出、Action 输入/输出、事件 payload、带 query 的完整 URL、请求/响应头或凭据。当前基线只提供结构化日志，不包含 metrics backend 或 exporter。
```

- [ ] **Step 2: Run focused observability tests**

Run:

```bash
cargo test --test observability -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Run full verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --quiet
```

Expected: all commands PASS.

- [ ] **Step 4: Inspect final diff**

Run:

```bash
git status --short
git diff --stat
git diff -- README.md src/lib.rs src/observability.rs src/runtime/coordinator.rs src/runtime/execution.rs src/nodes/chat.rs src/resources/openai_chat.rs tests/observability.rs
```

Expected:

- changed files are limited to A7 helper, logging sites, tests, and README;
- no API response schema changes;
- no persistence schema changes;
- no metrics/exporter dependency added.

- [ ] **Step 5: Commit**

Run:

```bash
git add README.md src/lib.rs src/observability.rs src/runtime/coordinator.rs src/runtime/execution.rs src/nodes/chat.rs src/resources/openai_chat.rs tests/observability.rs
git commit -m "docs: document body-free info observability"
```

---

## Final Verification Before Merge

- [ ] Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --quiet
git status --short --branch
```

Expected:

- formatting passes;
- clippy passes with `-D warnings`;
- all tests pass;
- branch is clean after commits;
- there is no new metrics backend/exporter dependency;
- structured INFO logs are body-free by test evidence.
