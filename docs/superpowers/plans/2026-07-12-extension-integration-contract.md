# Extension Integration Contract Implementation Plan

> **Historical / superseded:** this plan targets the removed public node/generic-operation extension surface. The current authored surface has no generic extension escape hatch; use typed Actions as defined by [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the statically linked custom-node extension contract end to end through YAML compilation, runtime dispatch, durable events, node output persistence, and terminal Run state.

**Architecture:** Keep production interfaces unchanged. Define synthetic extension nodes inside `tests/node_extensions.rs`, register their compile-time and runtime halves through existing registries, compile temporary YAML through `AgentCompiler`, then execute via `RunService` backed by a small test `RunRepository`.

**Tech Stack:** Rust, Tokio, `async-trait`, `serde`, `serde_json`, `tempfile`, existing `AgentCompiler`, `RunService`, `EventHub`, `RunRepository`, `NodeTypeRegistry`, and `NodeExecutorRegistry`.

## Global Constraints

- Extension nodes are statically linked Rust code registered at process setup.
- No dynamic libraries, WASM, downloaded plugins, remote extension execution, tenant policy, or binary ABI work in A6.
- Do not change production public interfaces unless a test proves the existing registry seams cannot express the contract safely.
- Do not edit `src/nodes/mod.rs` to register the synthetic test node.
- Do not rewrite `docs/reviews/2026-07-11-stable-baseline-review.md`; it is historical review evidence.
- Use TDD: write the failing test first, verify the expected failure, implement the smallest change, run the focused test, then commit.

---

## File Structure

- `tests/node_extensions.rs`
  - Owns all synthetic extension node fixtures.
  - Adds a small in-memory `ExtensionRepository` test double that implements `RunRepository` and exposes stored events/outputs for assertions.
  - Adds helper functions for temporary YAML agent compilation and `RunService` construction.
  - Adds end-to-end success, mismatch, and compile-time parity tests.
- `README.md`
  - Clarifies the static extension contract, the two registration halves, and the explicit non-goals.

No `src/` production files are expected to change.

---

### Task 1: Add the production-path custom-node success test

**Files:**
- Modify: `tests/node_extensions.rs`

**Interfaces:**
- Consumes: `NodeType`, `NodeExecutor`, `AgentCompiler::new`, `default_node_registries`, `RunService::new`, `RunRepository`, `EventHub`.
- Produces: reusable test helpers:
  - `fn compile_extension_agent(yaml: &str) -> Arc<CompiledAgent>`
  - `fn default_extension_executors() -> NodeExecutorRegistry`
  - `fn service_for(agent: Arc<CompiledAgent>, executors: NodeExecutorRegistry) -> (RunService, Arc<ExtensionRepository>)`
  - `async fn wait_for_status(service: &RunService, run_id: &str, expected: RunStatus) -> RunRecord`
  - `async fn event_types(repository: &ExtensionRepository, run_id: &str) -> Vec<RunEventType>`

- [ ] **Step 1: Write the failing end-to-end success test**

Append this test to `tests/node_extensions.rs` before adding the helper functions it calls:

```rust
#[tokio::test]
async fn custom_node_runs_through_compiler_service_events_repository_and_terminal() {
    let agent = compile_extension_agent(
        r#"
version: 1
id: extension_agent
name: Extension Agent
input:
  schema:
    type: object
entry: constant
nodes:
  constant:
    type: test.constant
    next: result
    config:
      value: 42
  result:
    type: core.output
    config:
      content:
        template: "value={{ nodes.constant.output.value }}"
      format: text
      data:
        value: "{{ nodes.constant.output.value }}"
"#,
    );
    let (service, repository) = service_for(agent, default_extension_executors());

    let created = service
        .create_detached(
            "extension_agent",
            json!({"question":"hello"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();

    let completed = wait_for_status(&service, &created.run_id, RunStatus::Completed).await;
    let output = completed.output.unwrap();
    assert_eq!(output.content.as_deref(), Some("value=42"));
    assert_eq!(output.format.as_deref(), Some("text"));
    assert_eq!(output.data, json!({"value":"42"}));

    let outputs = repository.outputs_for(&created.run_id).await;
    assert!(outputs.iter().any(|output| {
        output.node_id == "constant" && output.output == json!({"value":42})
    }));

    assert_eq!(
        event_types(&repository, &created.run_id).await,
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::NodeCompleted,
            RunEventType::NodeStarted,
            RunEventType::NodeCompleted,
            RunEventType::RunCompleted,
        ]
    );
    let events = repository.events_for(&created.run_id).await;
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
    assert!(events.iter().any(|event| {
        event.event_type == RunEventType::NodeStarted
            && event.node_id.as_deref() == Some("constant")
            && event.data == json!({"type":"test.constant"})
    }));
}
```

- [ ] **Step 2: Run the focused test and verify it fails because helpers are missing**

Run:

```bash
cargo test --test node_extensions custom_node_runs_through_compiler_service_events_repository_and_terminal -- --nocapture
```

Expected: FAIL to compile with unresolved helper/type names such as `compile_extension_agent`, `service_for`, `RequestMetadata`, or `RunStatus`.

- [ ] **Step 3: Replace imports and extension fixture definitions**

At the top of `tests/node_extensions.rs`, replace the import block and `ConstantConfig` with:

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_agent_platform::{
    dsl::{
        compiled::{
            CompiledAgent, CompiledNode, NextPolicy, NodeCompilation, NodeControl,
            NodeEnvelopeRules, NodeOutcome, NodeTransition,
        },
        compiler::{AgentCompiler, CompileContext, CompileLimits},
        CompileError, EmitPolicy,
    },
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::{RunEvent, RunEventScope, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{
            NewRun, NodeOutputRecord, RunRecord, RunStatus, TerminalUpdate,
        },
    },
    nodes::{
        default_node_registries,
        registry::{NodeExecutor, NodeExecutorRegistry, NodeType, NodeTypeRegistry},
    },
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::{
        stop_pair, CompiledAgentRegistry, ExecutionControl, RequestMetadata, RunContext, RunError,
        RunMetadata, RunService, RunServiceConfig,
    },
};
use serde::Deserialize;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConstantConfig {
    value: Value,
    #[serde(default)]
    references: Vec<String>,
}

#[derive(Debug)]
struct ConstantBody {
    value: Value,
}
```

Then update `ConstantNode::compile` and `ConstantNode::execute` to use `ConstantBody.value: Value` and custom references:

```rust
impl NodeType for ConstantNode {
    fn kind(&self) -> &'static str {
        "test.constant"
    }

    fn compile(
        &self,
        _node_id: &str,
        config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: ConstantConfig = serde_json::from_value(config).map_err(|error| {
            CompileError::new("NODE_CONFIG_INVALID", error.to_string())
        })?;
        Ok(NodeCompilation {
            body: Arc::new(ConstantBody { value: config.value }),
            edges: Vec::new(),
            references: config.references.into_iter().collect(),
            terminal: false,
            control: NodeControl::Ordinary,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Required,
                allows_content_emit: false,
            },
        })
    }
}

#[async_trait]
impl NodeExecutor for ConstantNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        _context: &RunContext,
        _control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        let config = node.body::<ConstantBody>()?;
        Ok(NodeOutcome {
            output: json!({"value": config.value.clone()}),
            transition: NodeTransition::Next,
        })
    }
}
```

- [ ] **Step 4: Add the in-memory repository test double**

Add this below the existing direct trait tests or below `run_context_exposes_only_formal_template_roots`:

```rust
#[derive(Default)]
struct ExtensionRepository {
    records: Mutex<BTreeMap<String, RunRecord>>,
    events: Mutex<BTreeMap<String, Vec<RunEvent>>>,
    outputs: Mutex<Vec<NodeOutputRecord>>,
}

impl ExtensionRepository {
    async fn events_for(&self, run_id: &str) -> Vec<RunEvent> {
        self.events
            .lock()
            .await
            .get(run_id)
            .cloned()
            .unwrap_or_default()
    }

    async fn outputs_for(&self, run_id: &str) -> Vec<NodeOutputRecord> {
        self.outputs
            .lock()
            .await
            .iter()
            .filter(|output| output.run_id == run_id)
            .cloned()
            .collect()
    }
}

#[async_trait]
impl RunRepository for ExtensionRepository {
    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
        self.records.lock().await.insert(
            run.run_id.clone(),
            RunRecord {
                run_id: run.run_id,
                request_id: run.request_id,
                agent_id: run.agent_id,
                agent_version: run.agent_version,
                attachment: run.attachment,
                status: RunStatus::Created,
                started_at: None,
                ended_at: None,
                updated_at: run.created_at,
                input_summary: run.input_summary,
                output: None,
                error_code: None,
                error_message: None,
            },
        );
        Ok(())
    }

    async fn mark_running(
        &self,
        run_id: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), HistoryError> {
        let mut records = self.records.lock().await;
        let record = records
            .get_mut(run_id)
            .ok_or_else(|| HistoryError::new("RUN_NOT_FOUND", "run not found"))?;
        record.status = RunStatus::Running;
        record.started_at = Some(started_at);
        record.updated_at = started_at;
        Ok(())
    }

    async fn append_events(&self, events: &[RunEvent]) -> Result<(), HistoryError> {
        let mut stored = self.events.lock().await;
        for event in events {
            stored
                .entry(event.run_id.clone())
                .or_default()
                .push(event.clone());
        }
        Ok(())
    }

    async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), HistoryError> {
        self.outputs.lock().await.push(output);
        Ok(())
    }

    async fn finish_run(
        &self,
        update: TerminalUpdate,
        event: RunEvent,
    ) -> Result<bool, HistoryError> {
        let mut records = self.records.lock().await;
        let record = records
            .get_mut(&update.run_id)
            .ok_or_else(|| HistoryError::new("RUN_NOT_FOUND", "run not found"))?;
        if record.status.is_terminal() {
            return Ok(false);
        }
        record.status = update.status;
        record.ended_at = Some(update.ended_at);
        record.updated_at = update.ended_at;
        record.output = update.output;
        record.error_code = update.error_code;
        record.error_message = update.error_message;
        drop(records);
        self.events
            .lock()
            .await
            .entry(event.run_id.clone())
            .or_default()
            .push(event);
        Ok(true)
    }

    async fn recover_run(
        &self,
        update: TerminalUpdate,
        mut terminal: RunEvent,
    ) -> Result<RunEvent, HistoryError> {
        terminal.seq = self
            .events
            .lock()
            .await
            .get(&update.run_id)
            .and_then(|events| events.last())
            .map_or(1, |event| event.seq + 1);
        self.finish_run(update, terminal.clone()).await?;
        Ok(terminal)
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
        Ok(self.records.lock().await.get(run_id).cloned())
    }

    async fn list_events_after(
        &self,
        run_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, HistoryError> {
        Ok(self
            .events
            .lock()
            .await
            .get(run_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|event| event.seq > after_seq)
            .take(limit)
            .collect())
    }

    async fn mark_incomplete_interrupted(&self, at: DateTime<Utc>) -> Result<u64, HistoryError> {
        let mut records = self.records.lock().await;
        let mut interrupted = Vec::new();
        for record in records.values_mut() {
            if matches!(record.status, RunStatus::Created | RunStatus::Running) {
                record.status = RunStatus::Interrupted;
                record.ended_at = Some(at);
                record.updated_at = at;
                record.error_code = Some("RUN_INTERRUPTED".to_string());
                record.error_message =
                    Some("run interrupted during startup reconciliation".to_string());
                interrupted.push((
                    record.run_id.clone(),
                    record.request_id.clone(),
                    record.agent_id.clone(),
                    record.agent_version.clone(),
                ));
            }
        }
        drop(records);
        let mut events = self.events.lock().await;
        for (run_id, request_id, agent_id, agent_version) in &interrupted {
            let run_events = events.entry(run_id.clone()).or_default();
            let seq = run_events.last().map_or(1, |event| event.seq + 1);
            run_events.push(RunEvent::error_at(
                RunEventType::RunInterrupted,
                seq,
                RunEventScope::for_run(request_id, run_id, agent_id, agent_version),
                at,
                "RUN_INTERRUPTED",
                "run interrupted during startup reconciliation",
                json!({}),
            ));
        }
        Ok(interrupted.len() as u64)
    }
}
```

- [ ] **Step 5: Add compiler, service, and assertion helpers**

Add these helpers below `ExtensionRepository`:

```rust
fn extension_compiler() -> AgentCompiler {
    let (mut node_types, _) = default_node_registries().unwrap();
    node_types.register(ConstantNode).unwrap();
    AgentCompiler::new(
        node_types,
        ModelRegistry::default(),
        ActionRegistry::default(),
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 32,
        },
    )
}

fn write_agent(yaml: &str) -> (TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("agent");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("agent.yaml"), yaml).unwrap();
    (temp, root)
}

fn compile_extension_agent(yaml: &str) -> Arc<CompiledAgent> {
    let (_temp, root) = write_agent(yaml);
    Arc::new(extension_compiler().compile_dir(&root).unwrap())
}

fn default_extension_executors() -> NodeExecutorRegistry {
    let (_, mut executors) = default_node_registries().unwrap();
    executors.register(ConstantNode).unwrap();
    executors
}

fn run_service_config() -> RunServiceConfig {
    RunServiceConfig {
        max_concurrent_runs: 4,
        max_parallel_node_executions: 8,
        max_parallel_branches_per_run: 4,
        run_timeout: Duration::from_secs(30),
    }
}

fn service_for(
    agent: Arc<CompiledAgent>,
    executors: NodeExecutorRegistry,
) -> (RunService, Arc<ExtensionRepository>) {
    let repository = Arc::new(ExtensionRepository::default());
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let agents = CompiledAgentRegistry::new(vec![agent]).unwrap();
    let service =
        RunService::new(agents, executors, repository_trait, events, run_service_config()).unwrap();
    (service, repository)
}

async fn wait_for_status(
    service: &RunService,
    run_id: &str,
    expected: RunStatus,
) -> RunRecord {
    let mut last = None;
    for _ in 0..200 {
        let record = service.get_run(run_id).await.unwrap();
        last = Some(record.status);
        if record.status == expected {
            return record;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("run {run_id} did not reach {expected:?}; last status: {last:?}");
}

async fn event_types(repository: &ExtensionRepository, run_id: &str) -> Vec<RunEventType> {
    repository
        .events_for(run_id)
        .await
        .into_iter()
        .map(|event| event.event_type)
        .collect()
}
```

- [ ] **Step 6: Update existing direct trait assertions for `Value`**

In `registered_node_compiles_and_executes_without_core_changes`, keep the direct trait coverage but update the expected config/output to match `ConstantBody.value: Value`:

```rust
let compilation = types
    .resolve("test.constant")
    .unwrap()
    .compile("constant", json!({"value":42}), &mut compile_context)
    .unwrap();
assert_eq!(compilation.edges, Vec::<String>::new());
assert_eq!(compilation.references, BTreeSet::new());
assert!(!compilation.terminal);
assert_eq!(compilation.envelope.next, NextPolicy::Required);
assert_eq!(compilation.control, NodeControl::Ordinary);
assert_eq!(NodeTransition::ActivateFork, NodeTransition::ActivateFork);
```

Keep the existing outcome assertion:

```rust
assert_eq!(outcome.output, json!({"value":42}));
assert_eq!(outcome.transition, NodeTransition::Next);
```

In `compiled_node_rejects_wrong_body_type`, update the downcast assertion to use the compiled body type:

```rust
assert_eq!(
    node.body::<ConstantBody>().unwrap_err().code(),
    "NODE_BODY_TYPE_MISMATCH"
);
```

- [ ] **Step 7: Run the focused success test and verify it passes**

Run:

```bash
cargo test --test node_extensions custom_node_runs_through_compiler_service_events_repository_and_terminal -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Run all extension tests**

Run:

```bash
cargo test --test node_extensions -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Commit**

Run:

```bash
git add tests/node_extensions.rs
git commit -m "test: cover extension success production path"
```

---

### Task 2: Add runtime mismatch coverage

**Files:**
- Modify: `tests/node_extensions.rs`

**Interfaces:**
- Consumes: helpers from Task 1.
- Produces:
  - `fn executors_without_constant() -> NodeExecutorRegistry`
  - `fn executors_with_wrong_body() -> NodeExecutorRegistry`
  - runtime tests for missing executor and body mismatch.

- [ ] **Step 1: Write failing missing-executor and body-mismatch tests**

Append these tests:

```rust
#[tokio::test]
async fn custom_node_missing_executor_terminalizes_as_infrastructure_failure() {
    let agent = compile_extension_agent(extension_success_yaml());
    let (service, repository) = service_for(agent, executors_without_constant());

    let created = service
        .create_detached("extension_agent", json!({}), RequestMetadata::default())
        .await
        .unwrap();

    let failed = wait_for_status(&service, &created.run_id, RunStatus::Failed).await;
    assert_eq!(failed.error_code.as_deref(), Some("INFRASTRUCTURE_FAILURE"));
    assert_eq!(
        event_types(&repository, &created.run_id).await,
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::RunFailed,
        ]
    );
}

#[tokio::test]
async fn custom_node_body_mismatch_terminalizes_as_node_failure() {
    let agent = compile_extension_agent(extension_success_yaml());
    let (service, repository) = service_for(agent, executors_with_wrong_body());

    let created = service
        .create_detached("extension_agent", json!({}), RequestMetadata::default())
        .await
        .unwrap();

    let failed = wait_for_status(&service, &created.run_id, RunStatus::Failed).await;
    assert_eq!(failed.error_code.as_deref(), Some("NODE_BODY_TYPE_MISMATCH"));
    assert_eq!(
        event_types(&repository, &created.run_id).await,
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::NodeFailed,
            RunEventType::RunFailed,
        ]
    );
    assert!(repository
        .outputs_for(&created.run_id)
        .await
        .into_iter()
        .all(|output| output.node_id != "constant"));
}
```

- [ ] **Step 2: Run the focused tests and verify they fail because helpers are missing**

Run:

```bash
cargo test --test node_extensions custom_node_missing_executor_terminalizes_as_infrastructure_failure -- --nocapture
cargo test --test node_extensions custom_node_body_mismatch_terminalizes_as_node_failure -- --nocapture
```

Expected: both commands FAIL to compile with unresolved `extension_success_yaml`, `executors_without_constant`, `executors_with_wrong_body`, or `WrongBodyExecutor`.

- [ ] **Step 3: Add shared YAML and executor helper functions**

Add this helper near the compiler helpers:

```rust
fn extension_success_yaml() -> &'static str {
    r#"
version: 1
id: extension_agent
name: Extension Agent
input:
  schema:
    type: object
entry: constant
nodes:
  constant:
    type: test.constant
    next: result
    config:
      value: 42
  result:
    type: core.output
    config:
      content:
        template: "value={{ nodes.constant.output.value }}"
      format: text
      data:
        value: "{{ nodes.constant.output.value }}"
"#
}

fn executors_without_constant() -> NodeExecutorRegistry {
    let (_, executors) = default_node_registries().unwrap();
    executors
}
```

Then update the Task 1 success test to call `compile_extension_agent(extension_success_yaml())` instead of embedding duplicate YAML.

- [ ] **Step 4: Add the wrong-body executor fixture**

Add this fixture near `ConstantNode`:

```rust
#[derive(Debug)]
struct WrongBodyConfig;

#[derive(Debug, Clone, Copy)]
struct WrongBodyExecutor;

impl NodeType for WrongBodyExecutor {
    fn kind(&self) -> &'static str {
        "test.constant"
    }

    fn compile(
        &self,
        _node_id: &str,
        _config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        Err(CompileError::new(
            "TEST_ONLY",
            "wrong-body executor is runtime-only",
        ))
    }
}

#[async_trait]
impl NodeExecutor for WrongBodyExecutor {
    async fn execute(
        &self,
        node: &CompiledNode,
        _context: &RunContext,
        _control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        let _wrong = node.body::<WrongBodyConfig>()?;
        Ok(NodeOutcome {
            output: json!({}),
            transition: NodeTransition::Next,
        })
    }
}

fn executors_with_wrong_body() -> NodeExecutorRegistry {
    let (_, mut executors) = default_node_registries().unwrap();
    executors.register(WrongBodyExecutor).unwrap();
    executors
}
```

- [ ] **Step 5: Run the mismatch tests**

Run:

```bash
cargo test --test node_extensions custom_node_missing_executor_terminalizes_as_infrastructure_failure -- --nocapture
cargo test --test node_extensions custom_node_body_mismatch_terminalizes_as_node_failure -- --nocapture
```

Expected: both commands PASS.

- [ ] **Step 6: Run all extension tests**

Run:

```bash
cargo test --test node_extensions -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add tests/node_extensions.rs
git commit -m "test: cover extension runtime contract failures"
```

---

### Task 3: Add compile-time parity coverage

**Files:**
- Modify: `tests/node_extensions.rs`

**Interfaces:**
- Consumes: `extension_compiler`, `write_agent`, and `ConstantNode` reference support from Task 1.
- Produces:
  - `fn assert_extension_compile_error(yaml: &str, code: &'static str)`
  - compile-time tests for envelope, reference, and emit parity.

- [ ] **Step 1: Write failing compile-time parity tests**

Append these tests:

```rust
#[test]
fn custom_node_required_next_is_enforced_by_agent_compiler() {
    assert_extension_compile_error(
        r#"
version: 1
id: extension_agent
name: Extension Agent
input:
  schema: {type: object}
entry: constant
nodes:
  constant:
    type: test.constant
    config:
      value: 42
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "NODE_NEXT_REQUIRED",
    );
}

#[test]
fn custom_node_references_use_shared_graph_validation() {
    assert_extension_compile_error(
        r#"
version: 1
id: extension_agent
name: Extension Agent
input:
  schema: {type: object}
entry: constant
nodes:
  constant:
    type: test.constant
    next: result
    config:
      value: 42
      references: [result]
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "INVALID_NODE_REFERENCE",
    );
}

#[test]
fn custom_node_content_emit_requires_envelope_support() {
    assert_extension_compile_error(
        r#"
version: 1
id: extension_agent
name: Extension Agent
input:
  schema: {type: object}
entry: constant
nodes:
  constant:
    type: test.constant
    emit: content
    next: result
    config:
      value: 42
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "NODE_EMIT_UNSUPPORTED",
    );
}
```

- [ ] **Step 2: Run the focused tests and verify helper is missing**

Run:

```bash
cargo test --test node_extensions custom_node_required_next_is_enforced_by_agent_compiler -- --nocapture
cargo test --test node_extensions custom_node_references_use_shared_graph_validation -- --nocapture
cargo test --test node_extensions custom_node_content_emit_requires_envelope_support -- --nocapture
```

Expected: each command FAILS to compile with unresolved `assert_extension_compile_error`.

- [ ] **Step 3: Add the compile-error helper**

Add this helper below `compile_extension_agent`:

```rust
fn assert_extension_compile_error(yaml: &str, code: &'static str) {
    let (_temp, root) = write_agent(yaml);
    let error = extension_compiler().compile_dir(&root).unwrap_err();
    assert_eq!(error.code(), code, "unexpected error: {error}");
}
```

- [ ] **Step 4: Run the focused compile-time parity tests**

Run:

```bash
cargo test --test node_extensions custom_node_required_next_is_enforced_by_agent_compiler -- --nocapture
cargo test --test node_extensions custom_node_references_use_shared_graph_validation -- --nocapture
cargo test --test node_extensions custom_node_content_emit_requires_envelope_support -- --nocapture
```

Expected: each command PASS.

- [ ] **Step 5: Run all extension tests**

Run:

```bash
cargo test --test node_extensions -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
git add tests/node_extensions.rs
git commit -m "test: cover extension compile parity"
```

---

### Task 4: Document and verify the A6 extension contract

**Files:**
- Modify: `README.md`
- Modify: `tests/node_extensions.rs` only if formatting or clippy requires small cleanup.

**Interfaces:**
- Consumes: successful tests from Tasks 1-3.
- Produces: README wording that explains static extension registration and A6 non-goals.

- [ ] **Step 1: Update README extension wording**

In `README.md`, replace the paragraph immediately before the extension registration code block:

```markdown
条件节点和其他节点一样通过注册表解析。新增静态链接节点只需实现 `NodeType` 和 `NodeExecutor`，然后在启动注册表中注册；DSL 解析器、图校验器、协调器、事件系统和 HTTP 层不需要增加分支：
```

with:

```markdown
条件节点和其他节点一样通过注册表解析。新增节点是静态链接的 Rust 扩展：实现 `NodeType` 负责编译期 config、envelope、边和引用声明，实现 `NodeExecutor` 负责运行期执行；两者分别注册到编译期和运行期注册表。注册后，自定义节点走同一套 DSL 解析、图校验、调度、事件、节点输出和终态提交路径，核心节点源码、调度器和 HTTP 层不需要增加分支：
```

After the registration code block, add:

```markdown
这不是动态插件系统：V1 不加载外部动态库、WASM、远程插件或下载代码。扩展代码由平台进程在构建/启动时显式链接和注册；如果编译期类型和运行期 executor 注册不一致，Run 会按普通运行时错误路径失败并写入终态。
```

- [ ] **Step 2: Run formatting**

Run:

```bash
cargo fmt --all -- --check
```

Expected: PASS. If it fails, run `cargo fmt --all`, inspect the diff, and include the formatting changes in this task's commit.

- [ ] **Step 3: Run focused extension tests**

Run:

```bash
cargo test --test node_extensions -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Run full verification**

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --quiet
```

Expected: both commands PASS.

- [ ] **Step 5: Inspect final diff**

Run:

```bash
git status --short
git diff --stat
git diff -- README.md tests/node_extensions.rs
```

Expected:

- only `README.md` and `tests/node_extensions.rs` changed since the previous commit;
- no `src/` production files changed;
- no modifications to `docs/reviews/2026-07-11-stable-baseline-review.md`.

- [ ] **Step 6: Commit**

Run:

```bash
git add README.md tests/node_extensions.rs
git commit -m "docs: document extension integration contract"
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
- production `src/` files are unchanged unless an implementation task records a justified minimal interface change.
