# Action Error Containment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `BASE-P1-010` by ensuring Action JSON Schema validation never places raw input or output values into runtime errors, events, history, HTTP responses, SSE, or default tracing.

**Architecture:** Sanitize at the source in `RegisteredAction`: treat JSON Schema validation as a boolean decision and emit exact fixed `RunError` code/message pairs. Keep every downstream public and persistence shape unchanged, verify containment end to end, and require an explicit deployment-time history reset instead of migrating prior unstructured error text.

**Tech Stack:** Rust 1.94.1, `jsonschema` 0.18.3, Tokio, Axum, SQLx SQLite/PostgreSQL, tracing, Cargo, Markdown

## Global Constraints

- Implement only A0 Sensitive Error Containment from `docs/superpowers/specs/2026-07-11-stable-baseline-remediation-program-design.md`.
- Do not implement A1–A8 or any dependency-governance milestone.
- Preserve `ACTION_INPUT_INVALID` with exact message `action input validation failed`.
- Preserve `ACTION_OUTPUT_INVALID` with exact message `action output validation failed`.
- Never call `ValidationError::to_string()` or serialize/format an Action validation instance, enum/constant value, property name, instance path, or schema path into a public/default diagnostic.
- Do not change `RunError`, HTTP/SSE/event/RunRecord shapes, repository traits, migrations, Cargo manifests, the lockfile, or dependency policy.
- Do not add automatic history deletion, a reset API, or a reset CLI.
- Historical Run data is disposable; deployment explicitly resets SQLite or PostgreSQL history before reopening traffic.
- Use TDD for production behavior, keep stable error codes as the compatibility boundary, and commit each independently reviewable deliverable.

---

## File responsibility map

- `src/resources/actions.rs`: sole production source of safe Action input/output validation errors.
- `tests/resource_registries.rs`: direct registry validation, keyword matrix, exact message, secret absence, and no-call-on-invalid-input contract.
- `tests/core_chat_action.rs`: real `ActionNode` rendered-input and returned-output propagation contract.
- `tests/action_error_containment.rs`: production compiler/runtime/EventHub/SQLite/HTTP/SSE/parallel end-to-end containment contract.
- `docs/formal-v1-breaking-changes.md`: single source of truth for destructive history reset and rollout/rollback consequences.
- `README.md`: short link to the reset procedure without duplicating SQL.

### Task 1: Make Action validation errors instance-free at the source

**Files:**
- Modify: `tests/resource_registries.rs`
- Modify: `tests/core_chat_action.rs`
- Modify: `src/resources/actions.rs`

**Interfaces:**
- Consumes: `RegisteredAction::validate_input(&Value) -> Result<(), RunError>` and `RegisteredAction::call(Value, ActionContext) -> Result<Value, RunError>`.
- Produces: exact safe input/output code-message pairs inherited by every later runtime and transport layer.

- [ ] **Step 1: Strengthen the direct registry test with exact secret-safe assertions**

In `tests/resource_registries.rs`, replace `InvalidOutputAction::call` with a value that would expose a unique secret under the current formatter:

```rust
async fn call(&self, _input: Value, _context: ActionContext) -> Result<Value, RunError> {
    Ok(json!({"secret":"never-expose-output"}))
}
```

Replace `action_registry_validates_input_and_output` with:

```rust
#[tokio::test]
async fn action_registry_validates_input_and_output_without_exposing_instances() {
    const INPUT_SECRET: &str = "never-expose-input";
    const OUTPUT_SECRET: &str = "never-expose-output";

    let mut registry = ActionRegistry::default();
    registry.register(EchoAction).unwrap();
    registry.register(InvalidOutputAction).unwrap();

    let echo = registry.resolve("echo").unwrap();
    assert_eq!(echo.descriptor().name, "echo");
    assert!(echo.descriptor().idempotent);

    let input_error = echo
        .validate_input(&json!({"text":{"secret":INPUT_SECRET}}))
        .unwrap_err();
    assert_eq!(input_error.code(), "ACTION_INPUT_INVALID");
    assert_eq!(input_error.message(), "action input validation failed");
    assert!(!input_error.to_string().contains(INPUT_SECRET));
    assert!(!format!("{input_error:?}").contains(INPUT_SECRET));

    assert_eq!(
        echo.call(json!({"text":"hi"}), test_action_context())
            .await
            .unwrap(),
        json!({"text":"hi"})
    );

    let invalid = registry.resolve("invalid_output").unwrap();
    let output_error = invalid
        .call(json!({}), test_action_context())
        .await
        .unwrap_err();
    assert_eq!(output_error.code(), "ACTION_OUTPUT_INVALID");
    assert_eq!(output_error.message(), "action output validation failed");
    assert!(!output_error.to_string().contains(OUTPUT_SECRET));
    assert!(!format!("{output_error:?}").contains(OUTPUT_SECRET));
}
```

- [ ] **Step 2: Add a representative JSON Schema keyword matrix**

Add this test-only Action below `InvalidOutputAction` in `tests/resource_registries.rs`:

```rust
struct SchemaMatrixAction;

#[async_trait]
impl Action for SchemaMatrixAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            name: "schema_matrix",
            input_schema: json!({
                "type":"object",
                "required":["typed","sized","patterned","selected","choice"],
                "additionalProperties":false,
                "properties":{
                    "typed":{"type":"integer"},
                    "sized":{"type":"string","maxLength":4},
                    "patterned":{"type":"string","pattern":"^allowed$"},
                    "selected":{"enum":["allowed"]},
                    "choice":{"oneOf":[{"type":"integer"},{"const":"allowed"}]}
                }
            }),
            output_schema: json!({"type":"object"}),
            idempotent: true,
        }
    }

    async fn call(&self, _input: Value, _context: ActionContext) -> Result<Value, RunError> {
        Ok(json!({}))
    }
}
```

Add the matrix test:

```rust
#[test]
fn action_validation_keyword_matrix_never_exposes_values() {
    const SECRET: &str = "keyword-matrix-never-expose";
    let mut registry = ActionRegistry::default();
    registry.register(SchemaMatrixAction).unwrap();
    let action = registry.resolve("schema_matrix").unwrap();

    let cases = [
        json!({"typed":SECRET,"sized":"okay","patterned":"allowed","selected":"allowed","choice":1}),
        json!({"typed":1,"sized":SECRET,"patterned":"allowed","selected":"allowed","choice":1}),
        json!({"typed":1,"sized":"okay","patterned":SECRET,"selected":"allowed","choice":1}),
        json!({"typed":1,"sized":"okay","patterned":"allowed","selected":SECRET,"choice":1}),
        json!({"typed":1,"sized":"okay","patterned":"allowed","selected":"allowed","choice":SECRET}),
    ];

    for input in cases {
        let error = action.validate_input(&input).unwrap_err();
        assert_eq!(error.code(), "ACTION_INPUT_INVALID");
        assert_eq!(error.message(), "action input validation failed");
        assert!(!format!("{error:?}").contains(SECRET));
    }
}
```

- [ ] **Step 3: Strengthen the real ActionNode test**

In `tests/core_chat_action.rs`, change the invalid-output branch of `EchoAction::call` to:

```rust
if self.invalid_output {
    Ok(json!({"echoed":input["payload"]["secret"].clone()}))
} else {
    Ok(json!({"echoed":input["payload"].clone()}))
}
```

Replace `action_validates_rendered_input_and_returned_output` with:

```rust
#[tokio::test]
async fn action_validation_errors_are_fixed_and_instance_free() {
    const INPUT_SECRET: &str = "rendered-input-never-expose";
    const OUTPUT_SECRET: &str = "returned-output-never-expose";

    let valid_calls = Arc::new(Mutex::new(Vec::new()));
    let mut actions = ActionRegistry::default();
    actions
        .register(EchoAction {
            calls: Arc::clone(&valid_calls),
            invalid_output: false,
        })
        .unwrap();
    actions
        .register(EchoAction {
            calls: Arc::new(Mutex::new(Vec::new())),
            invalid_output: true,
        })
        .unwrap();
    let models = ModelRegistry::default();

    let mut compile_context = CompileContext::new(&models, &actions);
    let invalid_input = ActionNode
        .compile(
            "echo",
            json!({"action":"echo", "input":{"payload":"{{ input.secret }}"}}),
            &mut compile_context,
        )
        .unwrap();
    let invalid_input_node = compiled_node("echo", "core.action", EmitPolicy::None, invalid_input);
    let invalid_input_context = context(json!({"secret":INPUT_SECRET}))
        .with_templates(Arc::new(compile_context.into_templates()));
    let (control, _) = capturing_control();
    let input_error = ActionNode
        .execute(&invalid_input_node, &invalid_input_context, &control)
        .await
        .unwrap_err();
    assert_eq!(input_error.code(), "ACTION_INPUT_INVALID");
    assert_eq!(input_error.message(), "action input validation failed");
    assert!(!format!("{input_error:?}").contains(INPUT_SECRET));
    assert!(valid_calls.lock().unwrap().is_empty());

    let mut compile_context = CompileContext::new(&models, &actions);
    let invalid_output = ActionNode
        .compile(
            "bad",
            json!({
                "action":"bad_echo",
                "input":{"payload":{"secret":"{{ input.secret }}"}}
            }),
            &mut compile_context,
        )
        .unwrap();
    let invalid_output_node = compiled_node("bad", "core.action", EmitPolicy::None, invalid_output);
    let invalid_output_context = context(json!({"secret":OUTPUT_SECRET}))
        .with_templates(Arc::new(compile_context.into_templates()));
    let output_error = ActionNode
        .execute(&invalid_output_node, &invalid_output_context, &control)
        .await
        .unwrap_err();
    assert_eq!(output_error.code(), "ACTION_OUTPUT_INVALID");
    assert_eq!(output_error.message(), "action output validation failed");
    assert!(!format!("{output_error:?}").contains(OUTPUT_SECRET));
}
```

- [ ] **Step 4: Run the focused tests and verify the current implementation fails**

Run:

```bash
cargo test --test resource_registries --test core_chat_action -- --nocapture
```

Expected: FAIL in the new exact-message/secret-absence assertions because `validate_json` currently joins `ValidationError::to_string()` output containing the invalid instance.

- [ ] **Step 5: Replace instance-bearing formatting with a boolean decision**

In `src/resources/actions.rs`, replace `validate_json` with:

```rust
fn validate_json(
    validator: &JSONSchema,
    value: &Value,
    code: &'static str,
    message: &'static str,
) -> Result<(), RunError> {
    if !validator.is_valid(value) {
        return Err(RunError::new(code, message));
    }
    Ok(())
}
```

Do not add logging or retain a `ValidationError` iterator.

- [ ] **Step 6: Run the focused tests and verify they pass**

Run:

```bash
cargo test --test resource_registries --test core_chat_action -- --nocapture
```

Expected: PASS; input/output codes and fixed messages match exactly, all secret-absence assertions pass, and existing successful Action/cancellation/streaming behavior remains green.

- [ ] **Step 7: Run a source guard against instance-bearing validation formatting**

Run:

```bash
rg -n 'validator\.validate|ValidationError|error\.to_string\(\)|details\.join' \
  src/resources/actions.rs
```

Expected: no match in the Action runtime validation path. Schema-compilation errors during registry startup may continue using their existing safe schema error path; the runtime instance formatter must be absent.

- [ ] **Step 8: Commit the source boundary fix**

```bash
git add src/resources/actions.rs tests/resource_registries.rs tests/core_chat_action.rs
git commit -m "fix: contain action validation errors"
```

### Task 2: Prove containment across runtime, SSE, history, parallel settlement, and logs

**Files:**
- Create: `tests/action_error_containment.rs`

**Interfaces:**
- Consumes: the safe `RegisteredAction` code/message contract from Task 1 and existing production compiler/runtime/repository/router interfaces.
- Produces: an end-to-end regression gate for linear/parallel, Attached/Detached, live/persisted, and tracing sinks.

- [ ] **Step 1: Create the test fixture and four production-compiled Agents**

Create `tests/action_error_containment.rs` with the following imports, Action, writer, and fixture helpers:

```rust
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
    let agents = ["linear-input", "linear-output", "parallel-output", "success"]
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
```

- [ ] **Step 2: Add HTTP, wait, and raw-history assertion helpers**

Append to `tests/action_error_containment.rs`:

```rust
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
    let run: (Option<String>,) = sqlx::query_as(
        "SELECT error_message FROM runs WHERE run_id = ?",
    )
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
```

- [ ] **Step 3: Add one serial end-to-end test covering every sink**

Append this single test so the global tracing subscriber is installed exactly once in the test binary:

```rust
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

    fixture.service.shutdown(Duration::from_secs(1)).await.unwrap();
    let logs = String::from_utf8(fixture.logs.lock().unwrap().clone()).unwrap();
    for secret in [INPUT_SECRET, OUTPUT_SECRET, PARALLEL_SECRET] {
        assert!(!logs.contains(secret), "secret leaked to tracing: {logs}");
    }
}
```

- [ ] **Step 4: Run the end-to-end containment contract**

Run:

```bash
cargo test --test action_error_containment -- --nocapture --test-threads=1
```

Expected: PASS. The single test must observe exact safe messages in Attached SSE, Detached GET, raw SQLite Run/event rows, and parallel branch history; it must observe no fixture secret and finish with a successful subsequent Run.

- [ ] **Step 5: Run the complete A0 focused suite**

Run:

```bash
cargo test --test resource_registries --test core_chat_action \
  --test action_error_containment -- --nocapture --test-threads=1
```

Expected: PASS with no secret printed in failure output or tracing.

- [ ] **Step 6: Commit the end-to-end regression gate**

```bash
git add tests/action_error_containment.rs
git commit -m "test: verify action error containment end to end"
```

### Task 3: Document destructive history reset and rollout

**Files:**
- Modify: `docs/formal-v1-breaking-changes.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: the A0 fixed error contract and the user-approved disposable-history policy.
- Produces: one operational source of truth for reset, rollout, rollback, and smoke verification.

- [ ] **Step 1: Add the A0 reset section to the breaking-change guide**

Append this section immediately after the existing `## 历史重置` section in `docs/formal-v1-breaking-changes.md`:

````markdown
### A0 Action validation error containment

旧版本可能把 Action JSON Schema 校验失败的原始 input/output instance 写入 Run 和事件错误消息。A0 从校验源头改为固定消息：

- `ACTION_INPUT_INVALID` / `action input validation failed`
- `ACTION_OUTPUT_INVALID` / `action output validation failed`

错误码和 HTTP/SSE/Event/Run 数据结构不变；动态 validator 文本不再是兼容合同。由于旧错误文本不可可靠区分诊断内容和敏感值，本次升级不迁移或扫描历史 Run，部署前必须显式重置历史。

停止所有使用目标 history store 的进程后执行以下一种操作。

SQLite：

```bash
SQLITE_DB=/absolute/path/to/run_history.sqlite3
rm -f "$SQLITE_DB" "$SQLITE_DB-wal" "$SQLITE_DB-shm"
```

PostgreSQL：

```bash
psql "$RUN_HISTORY_DATABASE_URL" <<'SQL'
BEGIN;
TRUNCATE TABLE node_outputs, run_events, runs;
COMMIT;
SQL
```

不要删除 SQLx migration metadata。应用不会自动删除历史，也不提供 reset API/CLI。删除后的历史不能通过回滚旧二进制恢复；不要把 A0 前的 Run/Event 数据重新导入运行库。

部署顺序：停止服务、重置历史、部署新二进制、启动并完成 migration check、运行 `cargo test --test action_error_containment -- --nocapture --test-threads=1`、检查 `/health`，然后恢复流量。
````

- [ ] **Step 2: Add a non-duplicating README link**

After the PostgreSQL contract-test command in README's history-backend section, add:

```markdown
A0 Action 校验错误安全修复不兼容既有 Run 历史；部署前按[正式 V1 破坏性变更中的 A0 重置流程](docs/formal-v1-breaking-changes.md#a0-action-validation-error-containment)停止服务并显式清空历史。应用不会自动删除数据。
```

Do not copy the reset SQL into README.

- [ ] **Step 3: Validate documentation consistency**

Run:

```bash
rg -n 'A0 Action validation error containment|TRUNCATE TABLE node_outputs, run_events, runs|应用不会自动删除' \
  docs/formal-v1-breaking-changes.md README.md
rg -n 'ACTION_INPUT_INVALID|ACTION_OUTPUT_INVALID' \
  docs/formal-v1-breaking-changes.md
git diff --check
```

Expected: the guide contains the exact code/message pairs and reset commands; README contains only the link/summary; Markdown has no whitespace errors.

- [ ] **Step 4: Commit the operational contract**

```bash
git add docs/formal-v1-breaking-changes.md README.md
git commit -m "docs: document action error history reset"
```

### Task 4: Run complete release gates and prepare the A0 handoff

**Files:**
- Read: `src/resources/actions.rs`
- Read: `tests/resource_registries.rs`
- Read: `tests/core_chat_action.rs`
- Read: `tests/action_error_containment.rs`
- Read: `docs/formal-v1-breaking-changes.md`
- Read: `README.md`

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces: verified A0 branch ready for independent code review and merge choice.

- [ ] **Step 1: Re-run the secret-formatting source guard**

Run:

```bash
if rg -n 'validator\.validate|ValidationError|error\.to_string\(\)|details\.join' \
  src/resources/actions.rs; then
  echo 'instance-bearing Action validation formatting remains' >&2
  exit 1
fi
```

Expected: exit 0 with no match.

- [ ] **Step 2: Run formatting and strict lint**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: both commands exit 0 with zero warnings.

- [ ] **Step 3: Run focused and full Rust tests**

```bash
cargo test --test resource_registries --test core_chat_action \
  --test action_error_containment -- --nocapture --test-threads=1
cargo test --all-targets
```

Expected: every test passes; the full suite has zero failures and the PostgreSQL harness's default local skip remains explicitly qualified.

- [ ] **Step 4: Run dependency policy gates**

```bash
cargo audit
cargo deny check
```

Expected: both exit 0. `cargo audit` retains only the already accepted-for-review `paste` unmaintained warning; `cargo deny` retains the documented duplicate and unmatched-license warnings. A0 must not change the dependency graph.

- [ ] **Step 5: Run the real PostgreSQL contract gate**

```bash
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' \
  cargo test --test history_postgres -- --nocapture
```

Expected: one real PostgreSQL test passes without taking its environment-missing early-return path. If port 5433 is occupied, inspect the existing service for Compose equivalence and do not stop an unrelated container.

- [ ] **Step 6: Verify change scope and commits**

Run:

```bash
git diff --check
git status --short
git log --oneline --decorate -6
git diff --name-only main...HEAD
```

Expected changed paths:

```text
README.md
docs/formal-v1-breaking-changes.md
src/resources/actions.rs
tests/action_error_containment.rs
tests/core_chat_action.rs
tests/resource_registries.rs
```

No migration, manifest, lockfile, repository shape, API shape, Agent YAML, or A1–A8 implementation file may appear.

- [ ] **Step 7: Request independent code review**

The reviewer must verify:

```text
1. No validation instance or validator Display text can enter RunError.
2. Exact stable codes/messages are preserved across input and output paths.
3. Invalid input never invokes the Action.
4. Linear/parallel, Attached/Detached, SSE/GET/raw history/tracing tests use unique secrets and inspect the intended Run.
5. History reset is explicit and never automatic.
6. Public and persistence shapes and the dependency graph are unchanged.
7. A1–A8 remain out of scope.
```

Fix every Critical or Important review defect, rerun the relevant focused test, and then rerun Steps 1–6 before presenting merge/PR/keep/discard options.
