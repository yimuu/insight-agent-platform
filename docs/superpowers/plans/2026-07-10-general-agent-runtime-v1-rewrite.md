# General Agent Runtime Formal V1 Rewrite Implementation Plan

> **Historical / superseded:** this plan implemented the removed graph/node baseline. See [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md) for the current authored contract and [DSL vNext Region/SSA Design](../specs/2026-07-16-dsl-vnext-region-ssa-design.md) for retained Region/SSA semantics.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the prototype runtime with the formal V1 compiler, extensible node system, deterministic run service, replayable bounded events, equivalent SQLite/PostgreSQL history, and explicit attached/detached HTTP APIs.

**Architecture:** Build the formal V1 alongside the prototype until its compiler, nodes, repositories, event hub, and runtime pass isolated tests. Switch `main` and `/v1` to the new stack only after those boundaries are verified, then delete the prototype modules and migrate all checked-in agents.

**Tech Stack:** Rust 1.94.1, Tokio, Axum 0.7, serde/serde_yaml, serde_json, jsonschema, Handlebars 6, cel-interpreter 0.10, sqlx 0.9, SQLite, PostgreSQL, tracing, futures

## Global Constraints

- The rewritten contracts are the first formal V1: HTTP `/v1`, DSL `version: 1`, and event `schema_version: 1`.
- Prototype HTTP, SSE, YAML, and history contracts are deleted; no compatibility shim or data migration is added.
- The graph is a sequential DAG with `core.template`, `core.chat`, `core.action`, `core.condition`, and `core.output`; loops, parallelism, joins, and subflows are rejected.
- Node extensions are statically linked through `NodeTypeRegistry`; dynamic libraries, WASM, and downloaded plugins are excluded.
- Every enabled agent compiles before the server listens, and every reachable path ends at `core.output`.
- Runs have exactly one terminal state: `completed`, `failed`, `cancelled`, or `interrupted`.
- Attached runs use a reconnect grace period; detached runs do not depend on subscribers.
- V1 performs no automatic retries.
- Raw request input and prompt/model/action bodies are not stored or logged at INFO.
- SQLite and PostgreSQL repository behavior must be equivalent.
- Every behavior change starts with a failing test, is implemented minimally, and ends with focused plus full verification.
- The approved design is `docs/superpowers/specs/2026-07-10-general-agent-runtime-v1-rewrite-design.md`.

## Target File Map

```text
src/dsl/{mod,raw,compiled,graph,compiler}.rs
src/nodes/{mod,registry,template,chat,action,condition,output}.rs
src/resources/{mod,models,actions}.rs
src/runtime/{mod,control,context,state,coordinator,attachment,service}.rs
src/events/{mod,protocol,hub,journal}.rs
src/history/{mod,types,repository,sqlite,postgres}.rs
src/api/{mod,auth,response,sse,routes}.rs
```

During Tasks 1-17 the formal modules coexist with the prototype `agent`, `engine`, `model`, `tools`, `code`, and `handlers` modules. Task 18 performs the atomic cutover and deletes superseded code.

---

### Task 1: Pin the toolchain and establish a clean quality baseline

**Files:**
- Create: `rust-toolchain.toml`
- Modify: `Cargo.toml`
- Modify: `src/code/registry.rs`
- Modify: `src/config.rs`
- Modify: `src/model/providers.rs`

**Interfaces:**
- Consumes: current prototype crate and Rust 1.94.1 installed in the workspace
- Produces: reproducible Rust toolchain and a strict-Clippy-clean starting point

- [ ] **Step 1: Capture the existing strict Clippy failures**

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: FAIL with `len_without_is_empty`, `needless_borrows_for_generic_args`, and `derivable_impls`.

- [ ] **Step 2: Pin the compiler and add rewrite dependencies**

Create `rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.94.1"
components = ["clippy", "rustfmt"]
profile = "minimal"
```

Add these dependencies to `Cargo.toml` and remove unused `tower-http`:

```toml
humantime = "2"
regex = "1"
sha2 = "0.10"
tokio-util = { version = "0.7", features = ["rt"] }
```

Add Tokio `sync` and `time` features to the existing Tokio dependency.

- [ ] **Step 3: Fix the three baseline Clippy findings**

Add this method to `CodeRegistry`:

```rust
pub fn is_empty(&self) -> bool {
    self.handlers.is_empty()
}
```

Pass `path` rather than `&path` to `fs::read_to_string` in `load_platform_yaml`. Derive `Default` for `ModelType` and annotate `Llm` with `#[default]`.

- [ ] **Step 4: Verify the baseline**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

Expected: all commands exit 0.

- [ ] **Step 5: Commit the baseline**

```bash
git add rust-toolchain.toml Cargo.toml Cargo.lock src/code/registry.rs src/config.rs src/model/providers.rs
git commit -m "chore: establish formal v1 quality baseline"
```

### Task 2: Parse the strict formal V1 DSL envelopes

**Files:**
- Create: `src/dsl/mod.rs`
- Create: `src/dsl/raw.rs`
- Create: `tests/dsl_raw.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: YAML bytes and `humantime::parse_duration`
- Produces: `RawAgent`, `RawNode`, `EmitPolicy`, `DurationSpec`, and `parse_raw_agent(&str)`

- [ ] **Step 1: Write failing strict-parser tests**

Create `tests/dsl_raw.rs` with these cases:

```rust
#[test]
fn parses_formal_v1_node_envelope() {
    let agent = parse_raw_agent(FORMAL_V1).unwrap();
    assert_eq!(agent.version, 1);
    assert_eq!(agent.entry, "answer");
    assert_eq!(agent.nodes["answer"].kind, "core.template");
    assert_eq!(agent.nodes["answer"].emit, EmitPolicy::Content);
    assert_eq!(agent.nodes["answer"].timeout.unwrap().get().as_secs(), 5);
}

#[test]
fn rejects_unknown_agent_and_node_fields() {
    let top = parse_raw_agent(&FORMAL_V1.replace("version: 1", "version: 1\nunknown: true"));
    assert!(top.unwrap_err().to_string().contains("unknown field"));

    let node = parse_raw_agent(&FORMAL_V1.replace("emit: content", "emit: content\ntyop: broken"));
    assert!(node.unwrap_err().to_string().contains("unknown field"));
}

#[test]
fn rejects_unsupported_version_and_invalid_duration() {
    assert!(parse_raw_agent(&FORMAL_V1.replace("version: 1", "version: 2")).is_err());
    assert!(parse_raw_agent(&FORMAL_V1.replace("timeout: 5s", "timeout: 0s")).is_err());
}
```

Use a fixture containing `entry: answer`, `type: core.template`, `emit: content`, `timeout: 5s`, and an output node.

- [ ] **Step 2: Run the parser test and verify it fails to compile**

Run: `cargo test --test dsl_raw -- --nocapture`

Expected: FAIL because `insight_agent_platform::dsl` does not exist.

- [ ] **Step 3: Implement the raw types and duration parser**

Define in `src/dsl/raw.rs`:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawAgent {
    pub version: u32,
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub input: RawInput,
    #[serde(default)]
    pub prompts: BTreeMap<String, String>,
    pub entry: String,
    pub nodes: BTreeMap<String, RawNode>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawNode {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub next: Option<String>,
    #[serde(default)]
    pub emit: EmitPolicy,
    #[serde(default)]
    pub timeout: Option<DurationSpec>,
    pub config: Value,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmitPolicy {
    #[default]
    None,
    Content,
}
```

Define strict `RawInput { schema: Value }`. Implement `DurationSpec` with custom `Deserialize` and `Serialize`: parse with `humantime::parse_duration`, reject zero, serialize to the original normalized unit string, and expose `get() -> Duration`. Implement:

```rust
pub fn parse_raw_agent(yaml: &str) -> Result<RawAgent, CompileError> {
    let agent: RawAgent = serde_yaml::from_str(yaml)
        .map_err(|error| CompileError::yaml(error.to_string()))?;
    if agent.version != 1 {
        return Err(CompileError::unsupported_version(agent.version));
    }
    Ok(agent)
}
```

Define the initial `CompileError { code: &'static str, message: String }` in `dsl/mod.rs` with `Display`, `Error`, `yaml`, and `unsupported_version` constructors.

- [ ] **Step 4: Export the module and pass tests**

Add `pub mod dsl;` to `src/lib.rs` and exports from `src/dsl/mod.rs`.

Run: `cargo test --test dsl_raw -- --nocapture`

Expected: PASS.

- [ ] **Step 5: Commit strict DSL parsing**

```bash
git add src/dsl src/lib.rs tests/dsl_raw.rs
git commit -m "feat: parse strict formal v1 agent dsl"
```

### Task 3: Introduce typed model and action resource registries

**Files:**
- Create: `src/resources/mod.rs`
- Create: `src/resources/models.rs`
- Create: `src/resources/actions.rs`
- Create: `src/runtime/mod.rs`
- Create: `src/runtime/control.rs`
- Create: `tests/resource_registries.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: named resource aliases, JSON Schema values, cancellation tokens, and bounded content emitters
- Produces: `ModelRegistry`, `ChatModel`, `ActionRegistry`, `Action`, and `ExecutionControl`

- [ ] **Step 1: Write failing registry contract tests**

Create tests proving:

```rust
#[test]
fn registries_reject_duplicate_aliases() {
    let mut models = ModelRegistry::default();
    models.register("default_chat", FakeChatModel::default()).unwrap();
    assert_eq!(models.register("default_chat", FakeChatModel::default()).unwrap_err().code(), "DUPLICATE_MODEL");

    let mut actions = ActionRegistry::default();
    actions.register(EchoAction).unwrap();
    assert_eq!(actions.register(EchoAction).unwrap_err().code(), "DUPLICATE_ACTION");
}

#[tokio::test]
async fn action_registry_validates_input_and_output() {
    let action = registry.resolve("echo").unwrap();
    assert!(action.validate_input(&json!({"text": 7})).is_err());
    assert_eq!(action.call(json!({"text":"hi"}), test_action_context()).await.unwrap(), json!({"text":"hi"}));
}
```

Also assert model capability lookup and provider-parameter validation.

- [ ] **Step 2: Run the tests and verify missing modules**

Run: `cargo test --test resource_registries -- --nocapture`

Expected: FAIL because the formal resource registries do not exist.

- [ ] **Step 3: Implement `ExecutionControl`**

Define in `src/runtime/control.rs`:

```rust
#[derive(Clone)]
pub struct ExecutionControl {
    stop: StopSignal,
    deadline: Instant,
}
```

Define `StopReason::{Cancelled, Interrupted}` plus paired `StopController`/`StopSignal` backed by one `CancellationToken` and an atomic reason set before cancellation. Provide `stop_reason`, `stopped`, and `remaining`. Add a formal `RunError { code: &'static str, message: String }` to `runtime/mod.rs` with sanitized constructors for cancellation, interruption, timeout, invalid action data, and upstream failures.

- [ ] **Step 4: Implement the model registry**

Define:

```rust
#[async_trait]
pub trait ChatModel: Send + Sync {
    fn capabilities(&self) -> BTreeSet<ModelCapability>;
    fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError>;
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError>;
}

#[derive(Clone, Default)]
pub struct ModelRegistry {
    models: BTreeMap<String, Arc<dyn ChatModel>>,
}
```

Use `register(&mut self, alias, model)` and reject duplicate aliases. Registry clones are cheap because model values are `Arc`. `ChatRequest` contains rendered messages and provider-validated JSON parameters. `ChatChunk` contains `text`, optional `finish_reason`, and optional `usage`.

- [ ] **Step 5: Implement the action registry with compiled schemas**

Define:

```rust
pub struct ActionDescriptor {
    pub name: &'static str,
    pub input_schema: Value,
    pub output_schema: Value,
    pub idempotent: bool,
}

#[async_trait]
pub trait Action: Send + Sync {
    fn descriptor(&self) -> ActionDescriptor;
    async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError>;
}
```

`ActionRegistry::register` compiles both JSON schemas and stores a `RegisteredAction` with validators plus the action. `RegisteredAction::call` validates input, invokes the action, then validates output.

- [ ] **Step 6: Pass resource tests and commit**

Run:

```bash
cargo test --test resource_registries -- --nocapture
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS.

```bash
git add src/resources src/runtime src/lib.rs tests/resource_registries.rs Cargo.toml Cargo.lock
git commit -m "feat: add formal v1 resource registries"
```

### Task 4: Define compiled agents and the node extension contract

**Files:**
- Create: `src/dsl/compiled.rs`
- Create: `src/dsl/compiler.rs`
- Create: `src/nodes/mod.rs`
- Create: `src/nodes/registry.rs`
- Create: `src/runtime/context.rs`
- Create: `tests/node_extensions.rs`
- Modify: `src/dsl/mod.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `RawNode`, `CompileContext`, `RunContext`, and `ExecutionControl`
- Produces: `CompiledAgent`, `CompiledNode`, `NodeCompilation`, `NodeTypeRegistry`, `NodeExecutorRegistry`, `NodeOutcome`, and `RunOutput`

- [ ] **Step 1: Write a failing synthetic-node extension test**

The test registers `test.constant`, compiles its raw config, stores the type-erased body, executes it, and asserts:

```rust
assert_eq!(compilation.edges, Vec::<String>::new());
assert_eq!(compilation.references, BTreeSet::new());
assert!(!compilation.terminal);
assert_eq!(compilation.envelope.next, NextPolicy::Required);

let outcome = executor.execute(&compiled_node, &context, &control).await.unwrap();
assert_eq!(outcome.output, json!({"value": 42}));
assert_eq!(outcome.transition, NodeTransition::Next);
```

Also register the same kind twice and assert `DUPLICATE_NODE_TYPE`.

- [ ] **Step 2: Run the test and verify missing extension interfaces**

Run: `cargo test --test node_extensions -- --nocapture`

Expected: FAIL because `nodes::registry` and compiled DSL types do not exist.

- [ ] **Step 3: Implement compiled data structures**

Define:

```rust
pub type CompiledBody = Arc<dyn Any + Send + Sync>;

pub struct NodeCompilation {
    pub body: CompiledBody,
    pub edges: Vec<String>,
    pub references: BTreeSet<String>,
    pub terminal: bool,
    pub envelope: NodeEnvelopeRules,
}

pub struct NodeEnvelopeRules {
    pub next: NextPolicy,
    pub allows_content_emit: bool,
}

pub enum NextPolicy {
    Required,
    Forbidden,
}

pub struct CompiledNode {
    pub id: String,
    pub kind: String,
    pub next: Option<String>,
    pub emit: EmitPolicy,
    pub timeout: Duration,
    pub body: CompiledBody,
    pub edges: Vec<String>,
    pub references: BTreeSet<String>,
    pub terminal: bool,
}

pub struct CompiledAgent {
    pub id: String,
    pub name: String,
    pub description: String,
    pub version_hash: String,
    pub input_schema: Arc<JSONSchema>,
    pub entry: String,
    pub nodes: BTreeMap<String, CompiledNode>,
    pub templates: Arc<Handlebars<'static>>,
}
```

Add `CompiledNode::body<T: Any>(&self) -> Result<&T, RunError>`.

Create the initial `CompileContext` in `dsl/compiler.rs` with references to `ModelRegistry` and `ActionRegistry`, a strict no-escape Handlebars registry, and helper accessors. Task 5 adds prompt loading, template registration, graph validation, and hashing.

- [ ] **Step 4: Implement node compiler and executor registries**

Use the approved signatures:

```rust
pub trait NodeType: Send + Sync {
    fn kind(&self) -> &'static str;
    fn compile(&self, node_id: &str, config: Value, context: &mut CompileContext)
        -> Result<NodeCompilation, CompileError>;
}

#[async_trait]
pub trait NodeExecutor: Send + Sync {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError>;
}
```

`NodeTypeRegistry` and `NodeExecutorRegistry` use the same names and reject duplicates. `NodeTransition` has `Next`, `Goto(String)`, and `Complete(RunOutput)` variants. `AgentCompiler` applies `NodeEnvelopeRules`: it requires or forbids common `next`, rejects `emit: content` when unsupported, and adds the accepted common `next` to compiled graph edges.

- [ ] **Step 5: Implement formal run context rendering data**

`RunContext` contains immutable run metadata, input, and completed node outputs. Its `template_data()` returns exactly `input`, `run`, and `nodes.<id>.output`; it exposes no platform config or secrets.

- [ ] **Step 6: Pass extension tests and commit**

Run:

```bash
cargo test --test node_extensions -- --nocapture
cargo test --test dsl_raw --test resource_registries
```

Expected: PASS.

```bash
git add src/dsl src/nodes src/runtime/context.rs src/lib.rs tests/node_extensions.rs
git commit -m "feat: define extensible formal v1 node contract"
```

### Task 5: Compile prompts, references, graph topology, and agent hashes

**Files:**
- Create: `src/dsl/graph.rs`
- Modify: `src/dsl/compiler.rs`
- Create: `tests/dsl_compiler.rs`
- Modify: `src/dsl/mod.rs`
- Modify: `src/dsl/compiled.rs`

**Interfaces:**
- Consumes: an agent directory, `NodeTypeRegistry`, `ModelRegistry`, `ActionRegistry`, and compiler defaults
- Produces: `AgentCompiler::compile_dir(&Path) -> Result<CompiledAgent, CompileError>`

- [ ] **Step 1: Write failing compiler and graph tests**

Create temporary formal agents and assert exact compile codes for:

```rust
assert_compile_error(agent_with_missing_entry(), "ENTRY_NOT_FOUND");
assert_compile_error(agent_with_missing_edge(), "NODE_EDGE_NOT_FOUND");
assert_compile_error(agent_with_cycle(), "GRAPH_CYCLE");
assert_compile_error(agent_with_unreachable_node(), "NODE_UNREACHABLE");
assert_compile_error(agent_with_non_output_terminal(), "OUTPUT_REQUIRED");
assert_compile_error(agent_referencing_future_node(), "INVALID_NODE_REFERENCE");
assert_compile_error(agent_with_prompt_escape(), "PROMPT_PATH_ESCAPE");
```

Also compile the same agent twice and assert equal `version_hash`; change a prompt file and assert a different hash.

- [ ] **Step 2: Run tests and verify the compiler is missing**

Run: `cargo test --test dsl_compiler -- --nocapture`

Expected: FAIL because `AgentCompiler` does not exist.

- [ ] **Step 3: Implement strict prompt and template compilation**

Expand the Task 4 `CompileContext` to contain the agent root, resolved prompt bodies, model/action registries, a strict `Handlebars<'static>` with `no_escape`, generated inline-template names, and collected normalized hash inputs.

Provide:

```rust
pub struct TemplateProgram {
    pub name: String,
    pub references: BTreeSet<String>,
}

impl CompileContext<'_> {
    pub fn compile_inline_template(
        &mut self,
        owner: &str,
        field: &str,
        source: &str,
    ) -> Result<TemplateProgram, CompileError>;

    pub fn compile_prompt_ref(
        &mut self,
        owner: &str,
        field: &str,
        prompt_ref: &str,
    ) -> Result<TemplateProgram, CompileError>;
}
```

Register templates in Handlebars during compilation. Extract only formal dotted references matching `nodes.<id>.output` and reject bracket-notation node references in V1.

- [ ] **Step 4: Implement DAG validation**

In `graph.rs`, implement DFS cycle detection, reachability from `entry`, edge existence, and reverse predecessor sets. For every `NodeCompilation.references`, require that the referenced node dominates the current node: it must be present on every path from `entry` to the current node. This prevents branch-dependent missing output at runtime.

Require every terminal node to have `terminal == true`, every reachable path to end in a terminal node, and every terminal node to be a formal output node once built-ins are registered.

- [ ] **Step 5: Implement `AgentCompiler` and stable SHA-256 hash**

Use this public constructor:

```rust
pub struct AgentCompiler {
    node_types: NodeTypeRegistry,
    models: ModelRegistry,
    actions: ActionRegistry,
    default_node_timeout: Duration,
}

impl AgentCompiler {
    pub fn compile_dir(&self, root: &Path) -> Result<CompiledAgent, CompileError>;
}
```

For every raw node, resolve its node type, compile node-specific config, then apply `NodeEnvelopeRules`: require or forbid common `next`, reject `emit: content` when `allows_content_emit` is false, add accepted common `next` to compiled edges, and apply the platform default timeout when the node omits one.

Hash a canonical JSON representation of the parsed raw agent, prompt names plus contents in sorted order, and resolved resource aliases. Format the result as `sha256:<lowercase hex>`.

- [ ] **Step 6: Pass compiler tests and commit**

Run:

```bash
cargo test --test dsl_compiler -- --nocapture
cargo test --test dsl_raw --test node_extensions
```

Expected: PASS.

```bash
git add src/dsl tests/dsl_compiler.rs Cargo.toml Cargo.lock
git commit -m "feat: compile formal v1 agent graphs"
```

### Task 6: Implement `core.template` and `core.condition`

**Files:**
- Create: `src/nodes/template.rs`
- Create: `src/nodes/condition.rs`
- Create: `tests/core_template_condition.rs`
- Modify: `src/nodes/mod.rs`

**Interfaces:**
- Consumes: `CompileContext`, recursive JSON templates, CEL, and formal run context
- Produces: compiled/executable `core.template` and `core.condition` node types

- [ ] **Step 1: Write failing template and condition tests**

Cover recursive template rendering and no HTML escaping:

```rust
assert_eq!(
    execute_template(json!({"text":"{{ input.text }}", "count": 2}), json!({"text":"A&B"})).await,
    json!({"text":"A&B", "count":2})
);
```

Cover ordered condition selection and default:

```rust
assert_eq!(execute_condition("medical").await.transition, NodeTransition::Goto("medical_answer".into()));
assert_eq!(execute_condition("unknown").await.transition, NodeTransition::Goto("general_answer".into()));
```

Assert compile failures for an invalid CEL expression, missing cases, missing default, a common `next` on condition, and `emit: content` on a template whose output is not statically a string.

- [ ] **Step 2: Run tests and verify built-ins are missing**

Run: `cargo test --test core_template_condition -- --nocapture`

Expected: FAIL because the two node implementations do not exist.

- [ ] **Step 3: Implement recursive compiled templates**

Define:

```rust
enum CompiledTemplateValue {
    String(TemplateProgram),
    Array(Vec<CompiledTemplateValue>),
    Object(BTreeMap<String, CompiledTemplateValue>),
    Literal(Value),
}
```

`TemplateNodeType::compile` strictly deserializes `{ value: Value }`, compiles every string leaf, unions references, and returns the common `next` edge. `TemplateNodeExecutor` renders recursively and emits the complete rendered string only when `emit == Content`.

- [ ] **Step 4: Implement precompiled conditions**

Strictly deserialize:

```rust
struct ConditionConfig {
    cases: Vec<ConditionCaseConfig>,
    default: String,
}

struct ConditionCaseConfig {
    when: String,
    next: String,
}
```

Compile each CEL program at agent compile time. At runtime add `input`, `run`, and `nodes` JSON objects to CEL context, evaluate in order, require bool, and choose the first true case or default. Output `{ "matched_case": <index-or-null>, "next": <target> }`.

- [ ] **Step 5: Pass focused tests and commit**

Run:

```bash
cargo test --test core_template_condition -- --nocapture
cargo test --test dsl_compiler
```

Expected: PASS.

```bash
git add src/nodes tests/core_template_condition.rs
git commit -m "feat: add template and condition nodes"
```

### Task 7: Implement `core.chat` and `core.action`

**Files:**
- Create: `src/nodes/chat.rs`
- Create: `src/nodes/action.rs`
- Create: `tests/core_chat_action.rs`
- Modify: `src/nodes/mod.rs`

**Interfaces:**
- Consumes: `ModelRegistry`, `ActionRegistry`, compiled message/input templates, and `ExecutionControl`
- Produces: normalized chat JSON output and schema-validated action JSON output

- [ ] **Step 1: Write failing chat tests**

Use a recording fake model and assert:

```rust
assert_eq!(request.messages[0].role, ChatRole::System);
assert_eq!(request.messages[1].text(), Some("A&B"));
assert_eq!(request.messages[1].image_urls(), vec!["https://example.test/report.png"]);
assert_eq!(output["text"], "Hello");
assert_eq!(emitted_chunks, vec!["Hel", "lo"]);
```

Add compile failures for missing model alias, invalid provider parameters, image content without `Vision`, empty messages, and invalid roles.

- [ ] **Step 2: Write failing action tests**

Use an `echo` action and assert recursive input rendering, input/output schema validation, cooperative cancellation propagation, and final output. Leaf chunks remain runtime-internal; only the validated final result may become workflow data.

- [ ] **Step 3: Run the tests and verify both built-ins are missing**

Run: `cargo test --test core_chat_action -- --nocapture`

Expected: FAIL because chat and action node implementations do not exist.

- [ ] **Step 4: Implement chat compilation and execution**

Strictly parse `model`, non-empty `messages`, and optional object `parameters`. Compile text and image URL templates. Resolve the model alias and validate parameters and capabilities during compilation.

Consume the model stream under `tokio::select!` with cancellation and deadline. Accumulate text, forward non-empty deltas through `ExecutionControl` only for `emit: content`, and return:

```rust
json!({
    "text": text,
    "finish_reason": finish_reason,
    "usage": usage,
})
```

- [ ] **Step 5: Implement action compilation and execution**

Strictly parse `{ action: String, input: Value }`, compile recursive string templates, resolve the registered action, and validate streaming-content compatibility. Render and validate input at runtime, invoke with an `ActionContext` derived from `ExecutionControl`, validate output, and return it unchanged.

- [ ] **Step 6: Pass focused tests and commit**

Run:

```bash
cargo test --test core_chat_action -- --nocapture
cargo test --test resource_registries --test node_extensions
```

Expected: PASS.

```bash
git add src/nodes tests/core_chat_action.rs
git commit -m "feat: add chat and action nodes"
```

### Task 8: Implement `core.output` and the default core registry

**Files:**
- Create: `src/nodes/output.rs`
- Create: `tests/core_output.rs`
- Create: `tests/formal_agent_compile.rs`
- Modify: `src/nodes/mod.rs`
- Modify: `src/dsl/compiler.rs`

**Interfaces:**
- Consumes: optional content template, format, optional recursive data template
- Produces: `RunOutput` and `default_node_registries()` containing all five formal core types

- [ ] **Step 1: Write failing output-contract tests**

Test content-only, data-only, and combined output:

```rust
assert_eq!(out.content.as_deref(), Some("answer"));
assert_eq!(out.format.as_deref(), Some("markdown"));
assert_eq!(out.data, json!({"answer":"answer"}));
```

Assert compile failures when both content and data are absent, format is absent with content, format is unsupported, `next` is present, or `emit` is `content`.

- [ ] **Step 2: Write a complete formal-agent compile test**

Create a temporary agent using all five core node types with two condition branches converging on output. Register fake model/action resources and assert compilation succeeds, every node is reachable, and the hash begins with `sha256:`.

- [ ] **Step 3: Run tests and verify output/default registration is missing**

Run: `cargo test --test core_output --test formal_agent_compile -- --nocapture`

Expected: FAIL.

- [ ] **Step 4: Implement the output node and default registries**

Strictly parse:

```rust
struct OutputConfig {
    content: Option<TemplateSource>,
    format: Option<OutputFormat>,
    data: Option<Value>,
}
```

Compile templates, require the approved combinations, mark the node terminal, and return `NodeTransition::Complete(RunOutput { content, format, data })`.

Implement:

```rust
pub fn default_node_registries() -> Result<(NodeTypeRegistry, NodeExecutorRegistry), CompileError>;
```

Register exactly `core.template`, `core.chat`, `core.action`, `core.condition`, and `core.output`.

- [ ] **Step 5: Pass the formal compiler suite and commit**

Run:

```bash
cargo test --test core_output --test formal_agent_compile -- --nocapture
cargo test --test dsl_compiler --test core_template_condition --test core_chat_action
```

Expected: PASS.

```bash
git add src/nodes src/dsl/compiler.rs tests/core_output.rs tests/formal_agent_compile.rs
git commit -m "feat: complete formal v1 core node set"
```

### Task 9: Define the formal V1 event and run record protocols

**Files:**
- Create: `src/events/mod.rs`
- Create: `src/events/protocol.rs`
- Create: `src/history/types.rs`
- Create: `tests/formal_protocol.rs`
- Modify: `src/history/mod.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: run/node identifiers, formal `RunOutput`, and JSON payloads
- Produces: serialized `RunEvent`, `RunStatus`, `RunAttachment`, `RunRecord`, `NewRun`, `NodeOutputRecord`, and `TerminalUpdate`

- [ ] **Step 1: Write failing serialization and transition tests**

Assert the exact event envelope:

```rust
let value = serde_json::to_value(event).unwrap();
assert_eq!(value["schema_version"], 1);
assert_eq!(value["type"], "node.completed");
assert_eq!(value["code"], "OK");
assert_eq!(value["node_id"], "plan");
```

Assert `node_id` is absent for run events and all statuses/attachments serialize as snake case. Assert `RunStatus::is_terminal()` is true only for completed, failed, cancelled, and interrupted.

- [ ] **Step 2: Run tests and verify protocol modules are missing**

Run: `cargo test --test formal_protocol -- --nocapture`

Expected: FAIL.

- [ ] **Step 3: Implement the event envelope**

Define `RunEventType` with exactly:

```rust
RunCreated, RunStarted, NodeStarted, ContentDelta, NodeCompleted,
NodeFailed, RunCompleted, RunFailed, RunCancelled, RunInterrupted
```

Define:

```rust
pub struct RunEvent {
    pub schema_version: u32,
    #[serde(rename = "type")]
    pub event_type: RunEventType,
    pub seq: u64,
    pub request_id: String,
    pub run_id: String,
    pub agent_id: String,
    pub agent_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(rename = "time")]
    pub timestamp: DateTime<Utc>,
    pub code: String,
    pub message: String,
    pub data: Value,
}
```

Constructors always set `schema_version: 1` and use stable codes.

- [ ] **Step 4: Implement formal history record types**

Define `RunStatus::{Created, Running, Completed, Failed, Cancelled, Interrupted}`, `RunAttachment::{Attached, Detached}`, `RunRecord`, `RunSummary`, `NodeOutputRecord`, `NewRun`, and `TerminalUpdate`. Store `agent_version`, attachment, timestamps, input summary, optional `RunOutput`, and sanitized error code/message. `TerminalUpdate` contains run ID, one terminal status, ended time, optional output, and optional sanitized error fields; its constructor rejects nonterminal statuses.

- [ ] **Step 5: Pass protocol tests and commit**

Run: `cargo test --test formal_protocol -- --nocapture`

Expected: PASS.

```bash
git add src/events src/history/types.rs src/history/mod.rs src/lib.rs tests/formal_protocol.rs
git commit -m "feat: define formal v1 runtime protocol"
```

### Task 10: Add isolated formal V1 migrations and SQLite repository behavior

**Files:**
- Create: `migrations/formal_v1/sqlite/202607100001_formal_v1.sql`
- Create: `migrations/formal_v1/postgres/202607100001_formal_v1.sql`
- Create: `src/history/repository.rs`
- Create: `src/history/sqlite.rs`
- Rewrite: `tests/migration_layout.rs`
- Create: `tests/history_sqlite_v1.rs`
- Modify: `src/history/mod.rs`

**Interfaces:**
- Consumes: formal `NewRun`, `RunEvent`, node outputs, terminal updates, and replay cursors
- Produces: backend-independent `RunRepository` plus SQLite implementation

- [ ] **Step 1: Write failing migration-layout assertions**

Require each formal V1 backend to define `runs`, `run_events`, and `node_outputs`; require `(run_id, seq)` uniqueness, formal statuses, attachment, agent version, JSON output, and foreign-key cascade. Prototype migrations remain temporarily so the old runtime stays testable until Task 18.

- [ ] **Step 2: Write failing SQLite repository tests**

Test this sequence:

```rust
repo.create_run(new_run()).await.unwrap();
repo.mark_running(RUN_ID, STARTED_AT).await.unwrap();
repo.append_events(&events_1_to_3()).await.unwrap();
repo.put_node_output(RUN_ID, "answer", json!({"text":"ok"}), NOW).await.unwrap();
assert_eq!(repo.list_events_after(RUN_ID, 1, 100).await.unwrap()[0].seq, 2);
assert!(repo.finish_run(terminal_update(), terminal_event()).await.unwrap());
assert!(!repo.finish_run(other_terminal_update(), other_terminal_event()).await.unwrap());
```

Also test startup reconciliation from created/running to interrupted, cascade deletion, duplicate sequence rejection, and input summary excluding values.

- [ ] **Step 3: Run tests and verify the prototype schema fails**

Run: `cargo test --test migration_layout --test history_sqlite_v1 -- --nocapture`

Expected: FAIL.

- [ ] **Step 4: Create fresh formal V1 schemas**

SQLite must include:

```sql
CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_version TEXT NOT NULL,
    attachment TEXT NOT NULL CHECK (attachment IN ('attached', 'detached')),
    status TEXT NOT NULL CHECK (status IN ('created','running','completed','failed','cancelled','interrupted')),
    started_at TEXT,
    ended_at TEXT,
    updated_at TEXT NOT NULL,
    input_summary TEXT NOT NULL CHECK (json_valid(input_summary)),
    output TEXT CHECK (output IS NULL OR json_valid(output)),
    error_code TEXT,
    error_message TEXT
);
```

Add `run_events` with a run foreign key, `UNIQUE(run_id, seq)`, and JSON-valid data. Add `node_outputs` with `(run_id, node_id)` primary key and JSON-valid output. Enable foreign keys on every SQLite connection.

PostgreSQL defines equivalent tables with `TIMESTAMPTZ`, `JSONB`, check constraints, and foreign keys; Task 11 implements its repository. Formal repository migrators embed only `migrations/formal_v1/{sqlite,postgres}` and never mix prototype migration checksums.

- [ ] **Step 5: Define the repository trait**

Define `HistoryError { code: &'static str, message: String, source: Option<Box<dyn Error + Send + Sync>> }`. Its `Display` exposes only the safe message; tracing may record the source chain after SQL connection strings are redacted.

```rust
#[async_trait]
pub trait RunRepository: Send + Sync {
    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError>;
    async fn mark_running(&self, run_id: &str, started_at: DateTime<Utc>) -> Result<(), HistoryError>;
    async fn append_events(&self, events: &[RunEvent]) -> Result<(), HistoryError>;
    async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), HistoryError>;
    async fn finish_run(&self, update: TerminalUpdate, event: RunEvent) -> Result<bool, HistoryError>;
    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, HistoryError>;
    async fn list_events_after(&self, run_id: &str, after_seq: u64, limit: usize) -> Result<Vec<RunEvent>, HistoryError>;
    async fn mark_incomplete_interrupted(&self, at: DateTime<Utc>) -> Result<u64, HistoryError>;
}
```

`finish_run` uses one transaction and updates only created/running rows; it appends the terminal event in the same transaction and returns whether it won the terminal race. `mark_incomplete_interrupted` processes each stale row transactionally and appends a `run.interrupted` event at `MAX(seq) + 1`, so startup reconciliation changes both state and replay history.

- [ ] **Step 6: Implement SQLite and pass tests**

Use typed conversion helpers that return `HistoryError` for invalid stored status, timestamps, or JSON; never substitute `Utc::now()` or `Value::Null` for corrupt data.

Run:

```bash
cargo test --test migration_layout --test history_sqlite_v1 -- --nocapture
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS.

- [ ] **Step 7: Commit the SQLite baseline**

```bash
git add migrations src/history tests/migration_layout.rs tests/history_sqlite_v1.rs
git commit -m "feat: add formal v1 sqlite history repository"
```

### Task 11: Implement PostgreSQL repository parity

**Files:**
- Create: `src/history/postgres.rs`
- Rewrite: `tests/history_postgres.rs`
- Modify: `src/history/mod.rs`

**Interfaces:**
- Consumes: the `RunRepository` contract and PostgreSQL formal V1 migration
- Produces: `PostgresRunRepository` with behavior matching SQLite

- [ ] **Step 1: Rewrite the PostgreSQL integration test as parity tests**

Share a `repository_contract(Arc<dyn RunRepository>)` async helper with SQLite or repeat the exact assertions for create, running transition, batch events, output, replay, one-winner terminal transition, cascade delete, and startup interruption.

In CI mode, missing `RUN_HISTORY_POSTGRES_URL` must panic with:

```rust
if std::env::var_os("CI").is_some() && database_url.is_none() {
    panic!("RUN_HISTORY_POSTGRES_URL is required in CI");
}
```

Local missing configuration prints an explicit skip message and returns.

- [ ] **Step 2: Run the PostgreSQL test and verify the repository is missing**

Run with the Compose database:

```bash
docker compose -f docker-compose.postgres.yml up -d
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' cargo test --test history_postgres -- --nocapture
```

Expected: FAIL because `PostgresRunRepository` does not exist.

- [ ] **Step 3: Implement PostgreSQL transactions and conversions**

Implement every `RunRepository` method using `$1` placeholders, `TIMESTAMPTZ`, and `JSONB`. `finish_run` begins a transaction, conditionally updates a nonterminal run, appends the terminal event only when the update affected one row, then commits.

- [ ] **Step 4: Run both backend contracts**

Run:

```bash
cargo test --test history_sqlite_v1 -- --nocapture
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' cargo test --test history_postgres -- --nocapture
```

Expected: PASS with equivalent assertions.

- [ ] **Step 5: Commit PostgreSQL parity**

```bash
git add src/history/postgres.rs src/history/mod.rs tests/history_postgres.rs
git commit -m "feat: add formal v1 postgres history parity"
```

### Task 12: Build the bounded event hub, journal worker, and replay

**Files:**
- Create: `src/events/hub.rs`
- Create: `src/events/journal.rs`
- Create: `tests/event_hub.rs`
- Modify: `src/events/mod.rs`

**Interfaces:**
- Consumes: formal events without sequence, `Arc<dyn RunRepository>`, and capacities
- Produces: `EventHub::publish`, `publish_terminal`, `subscribe`, and `replay_after`

- [ ] **Step 1: Write failing event-system tests**

Test:

```rust
assert_eq!(hub.publish(scope(), NodeStarted, json!({})).await.unwrap().seq, 1);
assert_eq!(hub.publish(scope(), ContentDelta, json!({"content":"a"})).await.unwrap().seq, 2);
assert_eq!(hub.replay_after(RUN_ID, 0).await.unwrap().iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);
```

Add tests for two subscribers receiving identical ordered events, bounded subscriber lag returning `SubscriberLagged { last_seq }`, durable replay beyond ring capacity, journal queue closure failing the run, and terminal broadcast occurring only after repository terminal commit.

- [ ] **Step 2: Run tests and verify hub/journal are missing**

Run: `cargo test --test event_hub -- --nocapture`

Expected: FAIL.

- [ ] **Step 3: Implement per-run sequence and bounded live delivery**

Use a per-run `EventRunState` guarded by a Tokio mutex and one bounded Tokio broadcast channel:

```rust
struct EventRunState {
    next_seq: u64,
    ring: VecDeque<RunEvent>,
    live: broadcast::Sender<RunEvent>,
}
```

`EventSubscription` tracks its last delivered sequence. `broadcast::error::RecvError::Lagged` maps to `SubscriberLagged { last_seq }`, after which the API closes SSE and the client reconnects with `after_seq=last_seq`. Publishing never waits on a slow subscriber.

- [ ] **Step 4: Implement the ordered journal worker**

Use a bounded command queue:

```rust
enum JournalCommand {
    Append(RunEvent),
    PutOutput { output: NodeOutputRecord, reply: oneshot::Sender<Result<(), HistoryError>> },
    Finish { update: TerminalUpdate, event: RunEvent, reply: oneshot::Sender<Result<bool, HistoryError>> },
    Flush(oneshot::Sender<Result<(), HistoryError>>),
}
```

The worker batches adjacent Append commands up to `batch_size`, preserves order, and calls repository methods. Queue send awaits capacity and fails explicitly if the worker closed.

- [ ] **Step 5: Implement replay and terminal barrier**

`replay_after` loads durable events, merges higher-sequence ring entries, sorts by sequence, and deduplicates. `publish_terminal` allocates the final sequence, sends `Finish`, waits for success, and broadcasts only if the repository won the terminal transition.

- [ ] **Step 6: Pass event tests and commit**

Run:

```bash
cargo test --test event_hub -- --nocapture
cargo test --test history_sqlite_v1
```

Expected: PASS.

```bash
git add src/events tests/event_hub.rs
git commit -m "feat: add bounded replayable formal v1 events"
```

### Task 13: Implement the deterministic coordinator and lifecycle state machine

**Files:**
- Create: `src/runtime/state.rs`
- Create: `src/runtime/coordinator.rs`
- Create: `tests/run_coordinator.rs`
- Modify: `src/runtime/mod.rs`

**Interfaces:**
- Consumes: `CompiledAgent`, executor registry, event hub/journal, repository, input, and cancellation token
- Produces: one sequential DAG execution ending in exactly one terminal state

- [ ] **Step 1: Write failing lifecycle transition tests**

Assert:

```rust
assert!(RunState::Created.transition(RunStatus::Running).is_ok());
assert!(RunState::Running.transition(RunStatus::Completed).is_ok());
assert!(RunState::Completed.transition(RunStatus::Failed).is_err());
```

Add a concurrent terminal race test where cancellation and output completion compete; assert exactly one repository terminal update and one terminal event.

- [ ] **Step 2: Write failing coordinator behavior tests**

Use synthetic nodes to cover successful sequence, condition goto, output completion, node failure, timeout, explicit cancellation, missing executor infrastructure failure, and node outputs becoming visible only after completion. Assert events are:

```text
run.created, run.started,
node.started, node.completed, ...,
run.completed
```

Failure emits `node.failed` then `run.failed`; cancellation emits only the applicable node event followed by `run.cancelled`.

- [ ] **Step 3: Run tests and verify state/coordinator are missing**

Run: `cargo test --test run_coordinator -- --nocapture`

Expected: FAIL.

- [ ] **Step 4: Implement terminal transition guard**

`RunState` owns current status behind a Tokio mutex and exposes:

```rust
pub async fn start(&self) -> Result<(), RunError>;
pub async fn try_terminal(&self, status: RunStatus) -> Result<bool, RunError>;
pub async fn status(&self) -> RunStatus;
```

Only created-to-running and nonterminal-to-terminal transitions are legal. Repository `finish_run` remains the durable race winner.

- [ ] **Step 5: Implement sequential coordinator execution**

`RunCoordinator::execute` publishes created/started, walks from `agent.entry`, resolves the matching executor, derives node deadline, and executes under cancellation plus timeout. On `Next`, use common `next`; on `Goto`, use the target; on `Complete`, persist formal output and finish completed.

After successful node execution, await `PutOutput`, publish `node.completed`, then await `Journal::flush` before moving to the successor. This creates one persistence barrier per node rather than one transaction per model delta. Map `RunError` into sanitized node/run events and map `StopReason::Cancelled`/`Interrupted` to their matching terminal states. Use cooperative stopping rather than aborting the coordinator task.

- [ ] **Step 6: Pass coordinator tests and commit**

Run:

```bash
cargo test --test run_coordinator -- --nocapture
cargo test --test event_hub --test core_output
```

Expected: PASS.

```bash
git add src/runtime tests/run_coordinator.rs
git commit -m "feat: execute formal v1 run state machine"
```

### Task 14: Implement attached and detached `RunService`

**Files:**
- Create: `src/runtime/attachment.rs`
- Create: `src/runtime/service.rs`
- Create: `tests/run_service.rs`
- Modify: `src/runtime/mod.rs`

**Interfaces:**
- Consumes: compiled-agent registry, coordinator factory, repository, event hub, concurrency limit, and lifecycle settings
- Produces: create, subscribe/replay, inspect, cancel, reconcile, and shutdown operations

- [ ] **Step 1: Write failing attached-run lease tests with paused Tokio time**

Cover:

```rust
let subscription = service.create_attached(...).await.unwrap();
drop(subscription);
tokio::time::advance(GRACE - Duration::from_millis(1)).await;
assert_eq!(service.get_run(run_id).await.unwrap().status, RunStatus::Running);

let reconnected = service.subscribe(run_id, after_seq).await.unwrap();
tokio::time::advance(GRACE).await;
assert_eq!(service.get_run(run_id).await.unwrap().status, RunStatus::Running);
drop(reconnected);
tokio::time::advance(GRACE).await;
assert_eq!(service.get_run(run_id).await.unwrap().status, RunStatus::Cancelled);
```

Use a blocking synthetic node so execution remains active. Attached expiry calls `StopController::request(StopReason::Cancelled)`.

- [ ] **Step 2: Write failing detached, cancel, concurrency, and reconciliation tests**

Assert detached runs complete without subscribers; cancellation is idempotent; already completed runs do not change; creation returns `RUN_CAPACITY_EXCEEDED` before inserting a run when `try_acquire_owned` cannot obtain the configured semaphore permit; startup marks stored created/running records interrupted; shutdown requests `Interrupted` for active detached runs and `Cancelled` for attached runs.

- [ ] **Step 3: Run tests and verify service/attachment are missing**

Run: `cargo test --test run_service -- --nocapture`

Expected: FAIL.

- [ ] **Step 4: Implement active run ownership**

Store:

```rust
struct ActiveRun {
    attachment: RunAttachment,
    stop: StopController,
    state: Arc<RunState>,
    subscribers: usize,
    grace_generation: u64,
    task: JoinHandle<()>,
}
```

`RunSubscription` owns an event receiver plus an `Arc<SubscriptionLease>`. Dropping the last clone notifies `RunService`; for attached runs it starts a generation-checked grace timer. Reconnection increments subscribers and invalidates the prior timer.

- [ ] **Step 5: Implement public service methods**

```rust
pub async fn create_attached(&self, agent_id: &str, input: Value, request: RequestMetadata) -> Result<AttachedRun, ServiceError>;
pub async fn create_detached(&self, agent_id: &str, input: Value, request: RequestMetadata) -> Result<RunRecord, ServiceError>;
pub async fn subscribe(&self, run_id: &str, after_seq: u64) -> Result<RunSubscription, ServiceError>;
pub async fn get_run(&self, run_id: &str) -> Result<RunRecord, ServiceError>;
pub async fn cancel(&self, run_id: &str) -> Result<RunRecord, ServiceError>;
pub async fn reconcile_startup(&self) -> Result<u64, ServiceError>;
pub async fn shutdown(&self, deadline: Duration) -> Result<(), ServiceError>;
```

Validate input schema before creating the repository row. Generate request/run IDs before returning attached SSE so the API can set headers.

- [ ] **Step 6: Pass service tests and commit**

Run:

```bash
cargo test --test run_service -- --nocapture
cargo test --test run_coordinator --test event_hub
```

Expected: PASS.

```bash
git add src/runtime tests/run_service.rs
git commit -m "feat: add attached and detached run service"
```

### Task 15: Adapt OpenAI chat and built-in capabilities to formal resources

**Files:**
- Create: `src/resources/openai_chat.rs`
- Create: `src/resources/builtin_actions.rs`
- Create: `tests/formal_resources.rs`
- Modify: `src/resources/mod.rs`
- Modify: `src/resources/models.rs`
- Modify: `src/resources/actions.rs`

**Interfaces:**
- Consumes: formal `ChatModel`, `Action`, request parameters, cancellation, and redaction rules
- Produces: OpenAI-compatible chat resource plus `current_time`, restricted HTTP GET, and example text-metrics actions

- [ ] **Step 1: Write failing OpenAI adapter tests**

Port request serialization and fragmented UTF-8 SSE tests to formal `ChatRequest`/`ChatChunk`. Add tests that provider parameters are merged only from the allowed schema, request cancellation drops the HTTP stream, no API key appears in Debug/error text, and upstream response bodies are not returned to clients.

- [ ] **Step 2: Write failing built-in action tests**

Port current-time and restricted-HTTP security tests. Add descriptor assertions for JSON schemas, idempotency, and streaming support. Port text metrics as `example.text_metrics` and assert its output schema.

- [ ] **Step 3: Run tests and verify formal adapters are missing**

Run: `cargo test --test formal_resources -- --nocapture`

Expected: FAIL.

- [ ] **Step 4: Implement the formal OpenAI-compatible adapter**

Move the hardened SSE decoder into `resources/openai_chat.rs`. Implement `ChatModel`; build requests from formal messages and validated parameters; retain bearer redaction; configure connect and request timeouts from platform resources; map transport/status/stream failures to stable sanitized `RunError` codes.

- [ ] **Step 5: Implement built-in actions**

Implement `Action` for current time, HTTP GET, and text metrics. HTTP GET remains HTTPS-only, redirect-free, allowlist-only, time/size bounded, and sanitized. Register only actions enabled by platform configuration.

- [ ] **Step 6: Pass adapter tests and commit**

Run:

```bash
cargo test --test formal_resources -- --nocapture
cargo test --test core_chat_action
```

Expected: PASS.

```bash
git add src/resources tests/formal_resources.rs
git commit -m "feat: adapt providers and actions to formal v1"
```

### Task 16: Replace platform configuration with strict formal V1 configuration

**Files:**
- Create: `src/platform_config.rs`
- Create: `tests/platform_config_v1.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: explicit platform YAML and named environment secrets
- Produces: `PlatformConfig` with resolved paths, auth mode, resources, history backend, and runtime limits

- [ ] **Step 1: Write failing strict configuration tests**

Test explicit missing `PLATFORM_CONFIG`, unknown fields, relative paths resolved from the platform file, explicit disabled auth, bearer-env auth with missing/empty secret, safe default enabled-agent policy, invalid capacities/timeouts, and redacted Debug output.

Assert there is no `public` or `default_public` field in the formal schema.

- [ ] **Step 2: Run tests and verify prototype configuration behavior fails**

Run: `cargo test --test platform_config_v1 -- --nocapture`

Expected: FAIL because `platform_config` does not exist.

- [ ] **Step 3: Implement strict formal structures**

Use tagged auth:

```rust
#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum AuthYaml {
    Disabled,
    BearerEnv { token_env: String },
}
```

Add positive nonzero runtime settings for maximum concurrent runs, default node/run timeout, reconnect grace, subscriber capacity, replay ring capacity, journal capacity, and journal batch size. Resolve agent/model/history paths relative to the platform file parent.

- [ ] **Step 4: Keep the formal parser isolated until cutover**

Export `pub mod platform_config;` without changing prototype `config.rs` or checked-in YAML. Tests use temporary formal config fixtures. Task 18 moves this implementation behind the final `config` module and rewrites checked-in configuration atomically with `main`.

- [ ] **Step 5: Pass configuration tests and commit**

Run:

```bash
cargo test --test platform_config_v1 -- --nocapture
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS.

```bash
git add src/platform_config.rs src/lib.rs tests/platform_config_v1.rs
git commit -m "feat: add strict formal v1 platform config"
```

### Task 17: Replace the HTTP and SSE API with formal V1 routes

**Files:**
- Create: `src/api/formal/mod.rs`
- Create: `src/api/formal/routes.rs`
- Create: `src/api/formal/sse.rs`
- Create: `src/api/formal/auth.rs`
- Create: `src/api/formal/response.rs`
- Create: `tests/formal_api.rs`
- Modify: `src/api/mod.rs`

**Interfaces:**
- Consumes: `RunService`, compiled-agent metadata, formal auth, request headers, and event subscriptions
- Produces: formal `/v1` JSON/SSE API with attached/detached creation, replay, lookup, and cancellation

- [ ] **Step 1: Replace API tests with formal contract tests**

Cover exact endpoints, direct input bodies, 202 detached response, attached `X-Run-Id`, `schema_version: 1`, `after_seq` replay, invalid cursor, run lookup, idempotent DELETE, auth modes, disabled agent 404, invalid JSON/schema before SSE, and prompt/secret omission from agent metadata.

Assert JSON responses use string codes:

```json
{"code":"OK","message":"ok","data":{}}
```

- [ ] **Step 2: Run API tests and verify prototype contracts fail**

Run: `cargo test --test formal_api -- --nocapture`

Expected: FAIL because `api::formal` does not exist.

- [ ] **Step 3: Implement formal auth and response mapping**

Keep `/health` public. Apply explicit bearer middleware to `/v1`. Map service errors to `INPUT_INVALID`, `UNAUTHORIZED`, `AGENT_NOT_FOUND`, `RUN_NOT_FOUND`, `RUN_CONFLICT`, `UPSTREAM_FAILURE`, or `INTERNAL`, never returning internal source chains.

- [ ] **Step 4: Implement routes and SSE adapter**

Attached POST validates input through `RunService`, gets the run ID before constructing the response, inserts `X-Run-Id`/`X-Request-Id`, and streams the initial subscription. Detached POST returns 202. Replay GET parses `after_seq` as u64 and returns replay followed by live events for active runs. DELETE returns the stable terminal/current run representation.

Configure SSE keepalive without inventing protocol events; keepalive comments do not consume sequence numbers.

- [ ] **Step 5: Pass API tests and commit**

Run:

```bash
cargo test --test formal_api -- --nocapture
cargo test --test run_service --test formal_protocol
```

Expected: PASS.

```bash
git add src/api tests/formal_api.rs
git commit -m "feat: expose formal v1 runtime api"
```

### Task 18: Migrate agents, wire `main`, and remove the prototype runtime

**Files:**
- Rewrite: `agents/researcher/agent.yaml`
- Rewrite: `agents/code_node_demo/agent.yaml`
- Rewrite: `agents/medical_report_interpreter/agent.yaml`
- Modify: prompt files where formal context names changed
- Rewrite: `config/platform.yaml`
- Rewrite: `config/models.yaml`
- Rewrite: `.env.example`
- Rewrite: `src/main.rs`
- Rewrite: `src/lib.rs`
- Replace: `src/config.rs` with the verified `src/platform_config.rs` implementation
- Replace: `src/api/` prototype files with the verified `src/api/formal/` implementation
- Delete: `src/agent/`
- Delete: `src/engine/`
- Delete: `src/code/`
- Delete: `src/tools/`
- Delete: `src/handlers/`
- Delete: prototype `src/model/` files after OpenAI adapter migration
- Delete: `src/history/store.rs`
- Delete: prototype `migrations/sqlite/` and `migrations/postgres/`
- Delete: `tests/agent_loader.rs`
- Delete: `tests/model_providers.rs`
- Delete: `tests/prompt_renderer.rs`
- Delete: `tests/runner.rs`
- Delete: prototype `tests/api.rs` after `tests/formal_api.rs` is renamed to `tests/api.rs`
- Rewrite: `tests/migration_layout.rs` to reject prototype migration directories
- Create: `tests/repository_agents_v1.rs`

**Interfaces:**
- Consumes: all formal V1 modules and checked-in platform configuration
- Produces: one formal runtime binary with no prototype execution path

- [ ] **Step 1: Write failing repository-agent compilation tests**

Compile all enabled checked-in agents through the production compiler and registries. Assert researcher uses private plan plus public answer, code demo resolves `example.text_metrics`, and medical agent resolves vision chat messages and an explicit output node.

- [ ] **Step 2: Rewrite all agent YAML to formal V1**

Use `version`, `entry`, and `nodes`. Replace prompt/text with template, LLM with chat, code with action, condition with formal condition, and add explicit output nodes. Use `nodes.<id>.output` context and model aliases from platform config.

Rewrite checked-in platform/model configuration in the same change: bind loopback, set `auth.mode: disabled`, explicitly list enabled agents, define named model resources, select formal SQLite history, and set positive development capacities/timeouts. Keep only secret environment-variable names in YAML and `.env.example`.

- [ ] **Step 3: Wire formal startup**

`main` loads strict config, builds model/action and node registries, compiles enabled agents, initializes the selected repository, reconciles stale runs, starts journal/event/run services, builds the formal API, and serves with graceful shutdown calling `RunService::shutdown`.

- [ ] **Step 4: Remove prototype modules and rewrite affected tests**

Delete old loaders, runner, event sender, tool/code registries, history store, response codes, prototype model types, and prototype migration directories. Move the already-tested formal API/config implementations into their final module locations and rename `tests/formal_api.rs` to `tests/api.rs`. Delete the four named prototype integration tests because their behaviors are covered by formal compiler/node/resource/runtime suites. Update `migration_layout` to assert only formal V1 migration directories remain. `src/lib.rs` exports only formal modules.

- [ ] **Step 5: Verify the formal binary and repository agents**

Run:

```bash
cargo test --test repository_agents_v1 -- --nocapture
cargo check --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS with no source reference to prototype `StepConfig`, `RunEngine`, `ToolRegistry`, `CodeRegistry`, numeric API codes, or `default_public`.

- [ ] **Step 6: Commit the cutover**

```bash
git add -A
git commit -m "refactor: cut over to formal v1 agent runtime"
```

### Task 19: Document breaking changes and install CI quality gates

**Files:**
- Rewrite: `README.md`
- Create: `docs/formal-v1-breaking-changes.md`
- Create: `.github/workflows/ci.yml`
- Create: `deny.toml`
- Modify: `docker-compose.postgres.yml`

**Interfaces:**
- Consumes: the completed formal V1 implementation and approved design
- Produces: generic-platform documentation, migration rationale, and enforceable CI

- [ ] **Step 1: Write the breaking-change guide from the approved table**

For every removed prototype construct, document old shape, formal V1 shape, reason, and one migration example. Include steps-array to DAG, prompt/text to template, LLM to chat, tool/code to action, implicit final output to `core.output`, stream to emit, public removal, attached/detached API, event string codes, and history reset.

- [ ] **Step 2: Rewrite README around the generic platform**

Document compiler flow, five core nodes, custom NodeType registration, custom Action registration, named models, attached/detached curl examples, reconnect with `after_seq`, explicit cancellation, platform config, history backends, and local verification. Present medical interpretation only as a multimodal example.

- [ ] **Step 3: Add dependency-policy and CI files**

Create `deny.toml` with this initial policy; adjust the allow list only when `cargo deny check licenses` identifies a resolved dependency's SPDX license and the license is reviewed:

```toml
[advisories]
yanked = "deny"

[licenses]
confidence-threshold = 0.8
allow = [
    "Apache-2.0",
    "Apache-2.0 WITH LLVM-exception",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "MIT",
    "MPL-2.0",
    "Unicode-3.0",
    "Zlib",
]

[bans]
multiple-versions = "warn"
wildcards = "allow"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
```

Create CI jobs that run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo install --locked cargo-audit
cargo install --locked cargo-deny
cargo audit
cargo deny check
```

The workflow uses `actions/checkout@v4`, `dtolnay/rust-toolchain@master` with toolchain `1.94.1`, and a PostgreSQL `16-alpine` service with the credentials already used by `docker-compose.postgres.yml`. Set `CI=1` and `RUN_HISTORY_POSTGRES_URL=postgres://insight:insight@localhost:5432/insight_agent_platform` for tests. Cache Cargo registry/build directories but never cache `.env` or database data.

- [ ] **Step 4: Verify documentation commands against the binary**

Start the server with auth disabled on loopback and a fake/local model configuration. Exercise health, agent list, a deterministic action-only detached run, event replay, lookup, and cancellation commands exactly as documented.

- [ ] **Step 5: Run the complete final gate**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
git diff --check
git status --short
```

Then run the PostgreSQL contract with `RUN_HISTORY_POSTGRES_URL`. If `cargo-audit` and `cargo-deny` are installed locally, run them; CI remains the mandatory dependency-policy gate.

Expected: all available commands exit 0, PostgreSQL tests execute rather than silently skip, and `git status --short` contains only the intended documentation/CI changes before commit.

- [ ] **Step 6: Commit documentation and CI**

```bash
git add README.md docs/formal-v1-breaking-changes.md .github/workflows/ci.yml deny.toml docker-compose.postgres.yml
git commit -m "docs: publish formal v1 runtime contracts"
```

## Plan Completion Audit

- Spec coverage: Tasks 2-8 cover DSL/compiler/nodes/resources; Tasks 9-12 cover protocol/history/events; Tasks 13-14 cover lifecycle and attachment; Tasks 15-18 cover adapters/config/API/cutover; Task 19 covers documentation and gates.
- Non-goals remain excluded: no compatibility, loops, parallelism, dynamic plugins, distributed recovery, retries, hot reload, or industry-specific controls.
- Type consistency: `CompiledAgent`, `CompiledNode`, `NodeCompilation`, `NodeOutcome`, `RunOutput`, `RunEvent`, `RunRecord`, `RunRepository`, `EventHub`, `RunCoordinator`, and `RunService` are introduced once and consumed by later tasks with the same names.
- Terminal consistency: only repository `finish_run` wins a terminal race; terminal events are published after its transaction succeeds.
