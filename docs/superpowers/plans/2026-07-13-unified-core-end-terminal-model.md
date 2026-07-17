# Unified `core.end` Terminal Model Implementation Plan

> **Historical / superseded:** authored `core.end` was removed in favor of lexical block `return`/`raise`. See [DSL vNext Region/SSA Design](../specs/2026-07-16-dsl-vnext-region-ssa-design.md).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `core.output` and implicit branch-to-Join settlement with one typed, scope-aware `core.end` terminal model across DSL, graph compilation, scheduling, events, persistence, API, Agents, and documentation.

**Architecture:** Add shared typed outcome values and make `NodeTransition::End(TerminalOutcome)` the only authored terminal transition. Replace untyped node edges with typed control edges, make Fork-to-Join a structural continuation, require every successful branch path to end explicitly, and make authored failure a successfully executed End rather than `RunError`. Persist Run terminals through a tagged internal lifecycle and expose consistent success/failure shapes through events and HTTP.

**Tech Stack:** Rust 2021, Tokio, Axum, Serde/serde_json, Handlebars, CEL, SQLx SQLite/PostgreSQL, tracing, Cargo test/Clippy/audit/deny.

## Global Constraints

- Final public DSL contains `core.end` and contains neither `core.output` nor `core.fail`.
- `core.end.outcome` is compile-time static and accepts exactly `success` or `failure`.
- Failure codes match `WORKFLOW_[A-Z][A-Z0-9_]*` and are at most 64 UTF-8 bytes.
- Failure messages are static, non-blank, single-line, control-character-free, and at most 256 UTF-8 bytes.
- End forbids common `next` and `emit: content`.
- Every statically successful main and Fork-branch path ends at End.
- Branches never point directly to Join; ForkContinuation is the only structural incoming relationship to Join.
- Branch End terminates only its branch; main End terminates the Run.
- Authored failure publishes `node.completed`, not `node.failed`.
- Stop and infrastructure failures never become settled branch data.
- Join remains explicit `mode: all_settled` and never decides Run success.
- Select remains one-of-N convergence and considers only direct executable predecessors.
- Nested Fork, resume, retries, named Condition cases, and new Join modes remain out of scope.
- INFO logs contain no End content, End data, branch bodies, or workflow failure messages.
- SQLite and PostgreSQL enforce equivalent Run terminal constraints.
- The initial Formal V1 migrations are rewritten in place; incompatible local databases must be recreated.
- No new runtime dependency is required.

---

## File Responsibility Map

### New files

- `src/outcome.rs`: shared Run output, End outcome, workflow error, and failure-origin types used by DSL, runtime, history, and API.
- `src/nodes/end.rs`: strict End configuration compiler and executor.
- `tests/core_end.rs`: End compile/execute/envelope/registry tests, replacing `tests/core_output.rs`.
- `agents/workflow_failure_demo/agent.yaml`: no-secret authored-failure Agent used by real-binary smoke coverage.

### Removed files

- `src/nodes/output.rs`: replaced by `src/nodes/end.rs`.
- `tests/core_output.rs`: renamed and rewritten as `tests/core_end.rs`.

### Compiler and graph files

- `src/dsl/compiled.rs`: `NodeControl::End`, typed `ControlEdge`, and `NodeTransition::End`.
- `src/dsl/compiler.rs`: common-next edge construction and removal of `terminal: bool`.
- `src/dsl/graph.rs`: typed structural traversal and End-only dead-end validation.
- `src/dsl/plan.rs`: ForkContinuation ownership and branch all-paths-End proof.
- `src/dsl/select.rs`: direct-executable predecessor and reachability filtering.

### Node files

- `src/nodes/mod.rs`: register exactly the eight final built-ins.
- `src/nodes/action.rs`, `chat.rs`, `condition.rs`, `fork.rs`, `join.rs`, `select.rs`, `template.rs`: typed edge construction and removal of `terminal` fields.

### Runtime files

- `src/runtime/mod.rs`: typed timeout classification helpers.
- `src/runtime/execution.rs`: preserve End as success and classify node/timeout/stop/infrastructure failures without code parsing.
- `src/runtime/state.rs`: typed branch success/failure payloads.
- `src/runtime/scheduler.rs`: scope-aware End handling and ForkContinuation activation.
- `src/runtime/coordinator.rs`: convert scheduler outcomes into typed durable Run terminals.
- `src/runtime/service.rs`: consume lifecycle accessors instead of independently nullable terminal fields.

### History, events, API, and migrations

- `src/history/types.rs`: tagged Run lifecycle, `RunTerminal`, typed `TerminalUpdate`, and serialization.
- `src/history/repository.rs`: validate terminal event against typed terminal state.
- `src/history/sqlite.rs`, `src/history/postgres.rs`: bind/read `error_kind` and reconstruct valid lifecycle variants.
- `src/events/hub.rs`: carry typed terminal updates through terminal publication and recovery.
- `src/api/formal/routes.rs`: continue serializing `RunRecord` with flat status plus mutually exclusive output/error.
- `migrations/formal_v1/sqlite/202607100001_formal_v1.sql`: add terminal consistency CHECK and `error_kind`.
- `migrations/formal_v1/postgres/202607100001_formal_v1.sql`: PostgreSQL-equivalent constraint.

### Repository fixtures and documentation

- `agents/code_node_demo/agent.yaml`, `agents/medical_report_interpreter/agent.yaml`, `agents/parallel_researcher/agent.yaml`, `agents/researcher/agent.yaml`: migrate successful terminals and explicit parallel branch Ends.
- `README.md`: replace Output and implicit Join examples with End and structured branch returns.
- `docs/formal-v1-breaking-changes.md`: document local database recreation and the terminal rewrite.
- `tests/binary_smoke.rs`: real binary success and authored workflow-failure polling.
- Existing Rust integration tests listed in each task: migrate fixture YAML and assert the new exact contracts.

---

### Task 1: Replace `core.output` with typed `core.end` for main-flow termination

**Files:**
- Create: `src/outcome.rs`
- Create: `src/nodes/end.rs`
- Rename: `tests/core_output.rs` -> `tests/core_end.rs`
- Delete: `src/nodes/output.rs`
- Modify: `src/lib.rs`
- Modify: `src/dsl/compiled.rs`
- Modify: `src/dsl/compiler.rs`
- Modify: `src/dsl/graph.rs`
- Modify: `src/nodes/mod.rs`
- Modify: `src/nodes/action.rs`
- Modify: `src/nodes/chat.rs`
- Modify: `src/nodes/condition.rs`
- Modify: `src/nodes/fork.rs`
- Modify: `src/nodes/join.rs`
- Modify: `src/nodes/select.rs`
- Modify: `src/nodes/template.rs`
- Modify: `src/runtime/scheduler.rs`
- Modify: `src/runtime/coordinator.rs`
- Modify: `tests/core_end.rs`
- Modify fixture YAML in: `tests/action_error_containment.rs`, `tests/api.rs`, `tests/chat_memory_bounds.rs`, `tests/core_chat_action.rs`, `tests/core_template_condition.rs`, `tests/dsl_compiler.rs`, `tests/dsl_parallel.rs`, `tests/dsl_raw.rs`, `tests/dsl_select.rs`, `tests/formal_agent_compile.rs`, `tests/medical_report_follow_up.rs`, `tests/node_extensions.rs`, `tests/observability.rs`, `tests/repository_agents_v1.rs`, `tests/run_coordinator.rs`, `tests/run_scheduler.rs`, `tests/run_service.rs`
- Modify: `agents/code_node_demo/agent.yaml`
- Modify: `agents/medical_report_interpreter/agent.yaml`
- Modify: `agents/parallel_researcher/agent.yaml`
- Modify: `agents/researcher/agent.yaml`

**Interfaces:**
- Consumes: existing strict template compilation, `RunError`, node envelope validation, ordinary node execution persistence.
- Produces: `RunOutput`, `EndOutcomeKind`, `WorkflowError`, `TerminalOutcome`, `FailureKind`, `RunFailure`; `NodeControl::End { outcome }`; `NodeTransition::End(TerminalOutcome)`; `EndNode` registered as `core.end`.

- [ ] **Step 1: Rename the contract test and write failing End tests**

Run:

```bash
git mv tests/core_output.rs tests/core_end.rs
```

Rewrite the imports and helper names, then add these exact behavioral assertions:

```rust
#[tokio::test]
async fn end_success_returns_a_typed_terminal_outcome() {
    let outcome = execute_end(
        json!({
            "outcome":"success",
            "content":{"template":"{{ input.answer }}"},
            "format":"text",
            "data":{"answer":"{{ input.answer }}"}
        }),
        json!({"answer":"done"}),
    )
    .await
    .unwrap();

    assert_eq!(
        outcome.transition,
        NodeTransition::End(TerminalOutcome::Success {
            output: RunOutput {
                content: Some("done".into()),
                format: Some("text".into()),
                data: json!({"answer":"done"}),
            },
        })
    );
    assert_eq!(
        outcome.output,
        json!({
            "outcome":"success",
            "output":{"content":"done","format":"text","data":{"answer":"done"}}
        })
    );
}

#[tokio::test]
async fn end_failure_is_a_successfully_executed_workflow_outcome() {
    let outcome = execute_end(
        json!({
            "outcome":"failure",
            "code":"WORKFLOW_ALL_BRANCHES_FAILED",
            "message":"all parallel branches failed"
        }),
        json!({"secret":"must-not-be-rendered"}),
    )
    .await
    .unwrap();

    assert_eq!(
        outcome.transition,
        NodeTransition::End(TerminalOutcome::Failure {
            error: WorkflowError {
                code: "WORKFLOW_ALL_BRANCHES_FAILED".into(),
                message: "all parallel branches failed".into(),
            },
        })
    );
    assert_eq!(outcome.output["outcome"], "failure");
    assert_eq!(outcome.output["error"]["kind"], "workflow");
}

#[test]
fn end_rejects_mixed_invalid_and_dynamic_failure_contracts() {
    assert_end_compile_error(json!({"outcome":"success"}), "END_VALUE_REQUIRED");
    assert_end_compile_error(
        json!({"outcome":"success","content":{"template":"answer"}}),
        "END_FORMAT_REQUIRED",
    );
    assert_end_compile_error(
        json!({"outcome":"success","format":"text","data":{"ok":true}}),
        "END_FORMAT_WITHOUT_CONTENT",
    );
    assert_end_compile_error(
        json!({"outcome":"failure","code":"RUN_TIMEOUT","message":"x"}),
        "END_FAILURE_CODE_INVALID",
    );
    assert_end_compile_error(
        json!({"outcome":"failure","code":"WORKFLOW_X","message":"line 1\nline 2"}),
        "END_FAILURE_MESSAGE_INVALID",
    );
    for message in ["   ".to_string(), "bad\u{0000}message".to_string(), "x".repeat(257)] {
        assert_end_compile_error(
            json!({"outcome":"failure","code":"WORKFLOW_X","message":message}),
            "END_FAILURE_MESSAGE_INVALID",
        );
    }
    assert_end_compile_error(
        json!({
            "outcome":"failure",
            "code":"WORKFLOW_X",
            "message":"{{ input.reason }}"
        }),
        "END_FAILURE_MESSAGE_INVALID",
    );
    assert_end_compile_error(
        json!({
            "outcome":"failure",
            "code":format!("WORKFLOW_{}", "X".repeat(56)),
            "message":"x"
        }),
        "END_FAILURE_CODE_INVALID",
    );
    assert_end_compile_error(
        json!({
            "outcome":"failure",
            "code":"WORKFLOW_X",
            "message":"x",
            "data":{"not":"allowed"}
        }),
        "NODE_CONFIG_INVALID",
    );
}
```

Also retain the renamed Output coverage as End coverage: one success test with content+format only, one with recursive data only, and the combined case above. In `tests/dsl_compiler.rs`, add complete Agent fixtures asserting an End with common `next` fails as `NODE_NEXT_FORBIDDEN` and an End with `emit: content` fails as `NODE_EMIT_UNSUPPORTED`.

- [ ] **Step 2: Run the renamed test to prove the contract is absent**

Run:

```bash
cargo test --locked --test core_end
```

Expected: compilation fails because `nodes::end`, `TerminalOutcome`, and `NodeTransition::End` do not exist.

- [ ] **Step 3: Add the shared outcome types**

Create `src/outcome.rs` with these public definitions:

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndOutcomeKind {
    Success,
    Failure,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum TerminalOutcome {
    Success { output: RunOutput },
    Failure { error: WorkflowError },
}

impl TerminalOutcome {
    pub fn kind(&self) -> EndOutcomeKind {
        match self {
            Self::Success { .. } => EndOutcomeKind::Success,
            Self::Failure { .. } => EndOutcomeKind::Failure,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Workflow,
    Node,
    Timeout,
    Infrastructure,
}

impl FailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Workflow => "workflow",
            Self::Node => "node",
            Self::Timeout => "timeout",
            Self::Infrastructure => "infrastructure",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "workflow" => Some(Self::Workflow),
            "node" => Some(Self::Node),
            "timeout" => Some(Self::Timeout),
            "infrastructure" => Some(Self::Infrastructure),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunFailure {
    pub kind: FailureKind,
    pub code: String,
    pub message: String,
}
```

Use the struct-style `TerminalOutcome` variants above consistently in implementation and tests. Export the module from `src/lib.rs`:

```rust
pub mod outcome;
```

- [ ] **Step 4: Replace compiled terminal flags and transitions with typed End control**

In `src/dsl/compiled.rs`, import the shared types, remove `RunOutput` from this file, remove both `terminal: bool` fields, and use:

```rust
use crate::outcome::{EndOutcomeKind, TerminalOutcome};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeControl {
    Ordinary,
    Fork {
        branches: BTreeMap<String, String>,
        join: String,
    },
    Join {
        policy: JoinPolicy,
    },
    Select {
        sources: BTreeSet<String>,
    },
    End {
        outcome: EndOutcomeKind,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeTransition {
    Next,
    Goto(String),
    ActivateFork,
    End(TerminalOutcome),
}
```

Remove the `terminal` field from every `NodeCompilation` constructor and every direct `CompiledNode` fixture listed in the Files block.

- [ ] **Step 5: Implement the strict End compiler and executor**

Create `src/nodes/end.rs`. Preserve the successful-output rendering logic from the deleted Output node, but compile this strict union:

```rust
#[derive(Debug, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum EndConfig {
    Success {
        content: Option<TemplateSource>,
        format: Option<OutputFormat>,
        data: Option<Value>,
    },
    Failure {
        code: String,
        message: String,
    },
}

#[derive(Debug)]
enum CompiledEnd {
    Success {
        content: Option<TemplateProgram>,
        format: Option<OutputFormat>,
        data: Option<CompiledTemplateValue>,
    },
    Failure(WorkflowError),
}
```

Validate workflow codes without adding a regex dependency:

```rust
fn valid_workflow_code(code: &str) -> bool {
    let Some(suffix) = code.strip_prefix("WORKFLOW_") else {
        return false;
    };
    let mut chars = suffix.chars();
    matches!(chars.next(), Some('A'..='Z'))
        && chars.all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
        && code.len() <= 64
}

fn valid_workflow_message(message: &str) -> bool {
    !message.trim().is_empty()
        && message.len() <= 256
        && !message.chars().any(char::is_control)
        && !message.contains("{{")
        && !message.contains("}}")
}
```

Return `END_VALUE_REQUIRED`, `END_FORMAT_REQUIRED`, and `END_FORMAT_WITHOUT_CONTENT` for the three success-shape violations exercised above; return `END_FAILURE_CODE_INVALID` and `END_FAILURE_MESSAGE_INVALID` for failure validation. Serde's denied unknown fields owns mixed-variant rejection as `NODE_CONFIG_INVALID`.

Compile to:

```rust
NodeCompilation {
    body: Arc::new(compiled),
    edges: Vec::new(),
    references,
    control: NodeControl::End { outcome },
    envelope: NodeEnvelopeRules {
        next: NextPolicy::Forbidden,
        allows_content_emit: false,
    },
}
```

Execute success and failure as normal node outcomes:

```rust
fn render_run_output(
    content: &Option<TemplateProgram>,
    format: Option<OutputFormat>,
    data: Option<&CompiledTemplateValue>,
    context: &RunContext,
) -> Result<RunOutput, RunError> {
    let template_data = context.template_data();
    let content = content
        .as_ref()
        .map(|template| {
            context
                .templates()
                .render(&template.name, &template_data)
                .map_err(|error| {
                    RunError::new(
                        "TEMPLATE_RENDER_FAILED",
                        format!(
                            "failed to render end template '{}': {error}",
                            template.name
                        ),
                    )
                })
        })
        .transpose()?;
    let data = data
        .map(|data| data.render(context, &template_data))
        .transpose()?
        .unwrap_or(Value::Null);
    Ok(RunOutput {
        content,
        format: format.map(|format| format.as_str().to_string()),
        data,
    })
}

match node.body::<CompiledEnd>()? {
    CompiledEnd::Success { content, format, data } => {
        let output = render_run_output(content, *format, data.as_ref(), context)?;
        Ok(NodeOutcome {
            output: json!({"outcome":"success", "output":&output}),
            transition: NodeTransition::End(TerminalOutcome::Success { output }),
        })
    }
    CompiledEnd::Failure(error) => Ok(NodeOutcome {
        output: json!({"outcome":"failure", "error":{
            "kind":"workflow", "code":&error.code, "message":&error.message
        }}),
        transition: NodeTransition::End(TerminalOutcome::Failure {
            error: error.clone(),
        }),
    }),
}
```

The JSON expressions borrow the typed values, leaving ownership available for the terminal transition.

- [ ] **Step 6: Replace the registry and delete Output**

Update `src/nodes/mod.rs` to expose and register End exactly once:

```rust
pub mod end;
// remove: pub mod output;

use self::end::EndNode;

types.register(EndNode)?;
executors.register(EndNode)?;
```

Delete `src/nodes/output.rs`. Update the registry test expected kinds to:

```rust
let expected = vec![
    "core.action",
    "core.chat",
    "core.condition",
    "core.end",
    "core.fork",
    "core.join",
    "core.select",
    "core.template",
];
```

- [ ] **Step 7: Make graph validation recognize only typed End dead ends**

Defer missing required-`next` envelope rejection until graph validation. After
edge existence and cycle checks, validate reachable End/non-End leaves before
reporting unreachable declared nodes. This makes a reachable non-End leaf
return `END_REQUIRED`, including when its missing successor also orphans a
declared downstream node; retain `NODE_UNREACHABLE` only when there is no
earlier reachable non-End leaf.

Replace the string-based `core.output` checks in `src/dsl/graph.rs` with:

```rust
let is_end = matches!(node.control, NodeControl::End { .. });
if is_end && !node.edges.is_empty() {
    return Err(CompileError::new(
        "END_HAS_SUCCESSOR",
        format!("end node '{node_id}' cannot have outgoing edges"),
    ));
}
if !is_end && node.edges.is_empty() {
    return Err(CompileError::new(
        "END_REQUIRED",
        format!("reachable path ends at non-end node '{node_id}'"),
    ));
}
```

At this task boundary, existing branch-to-Join fixtures still provide outgoing edges and continue to compile. Task 3 removes that topology.

- [ ] **Step 8: Handle main-scope End in scheduler and coordinator**

Change `SchedulerResult` to:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum SchedulerResult {
    Ended(TerminalOutcome),
    Failed(RunError),
    Stopped(RunError),
}
```

In the main-scope transition match:

```rust
NodeTransition::End(outcome) => {
    return Ok(SchedulerResult::Ended(outcome));
}
```

Keep branch End as an invariant until Task 3:

```rust
NodeTransition::End(_) => {
    return Err(invariant(format!(
        "branch node '{}' ended before structured branch terminal support",
        node.id
    )));
}
```

In `RunCoordinator`, route End success to the existing completion method and End failure to a dedicated authored-failure method that publishes `run.failed` with `data: {"kind":"workflow"}` and writes the existing terminal error fields. Do not construct `RunError` for authored failure.

- [ ] **Step 9: Migrate every executable fixture from Output to End success**

For every file listed in this task's Files block, replace:

```yaml
result:
  type: core.output
  config:
    content: {template: "{{ nodes.render.output }}"}
    format: text
```

with:

```yaml
result:
  type: core.end
  config:
    outcome: success
    content: {template: "{{ nodes.render.output }}"}
    format: text
```

For data-only fixtures, insert `outcome: success` before `data`. Do not yet rewrite branch-to-Join edges; Task 3 owns that semantic migration.

Verify source, executable fixtures, and Agents contain no Output node:

```bash
rg -n "core\.output|OutputNode|NodeTransition::Complete" src tests agents
```

Expected: no matches.

- [ ] **Step 10: Run focused and broad Task 1 verification**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test core_end
cargo test --locked --test dsl_compiler
cargo test --locked --test run_coordinator
cargo test --locked --all-targets
```

Expected: every command exits 0. Parallel tests still use their old branch-to-Join topology but use End for main Run termination.

- [ ] **Step 11: Commit the canonical End contract**

```bash
git add src tests agents
git commit -m "feat: replace output with unified end node"
```

---

### Task 2: Introduce typed executable and structural control edges

**Files:**
- Modify: `src/dsl/compiled.rs`
- Modify: `src/dsl/compiler.rs`
- Modify: `src/dsl/graph.rs`
- Modify: `src/dsl/plan.rs`
- Modify: `src/dsl/select.rs`
- Modify: `src/nodes/action.rs`
- Modify: `src/nodes/chat.rs`
- Modify: `src/nodes/condition.rs`
- Modify: `src/nodes/end.rs`
- Modify: `src/nodes/fork.rs`
- Modify: `src/nodes/join.rs`
- Modify: `src/nodes/select.rs`
- Modify: `src/nodes/template.rs`
- Modify: `tests/core_end.rs`
- Modify: `tests/core_select.rs`
- Modify: `tests/core_template_condition.rs`
- Modify: `tests/dsl_compiler.rs`
- Modify: `tests/dsl_parallel.rs`
- Modify: `tests/dsl_select.rs`
- Modify: `tests/fork_join_nodes.rs`
- Modify: `tests/node_extensions.rs`

**Interfaces:**
- Consumes: `NodeControl`, `EndOutcomeKind`, current raw `next`, Condition cases, Fork branch map and paired Join.
- Produces: `ControlEdge::{Direct, Conditional, ForkBranch, ForkContinuation}`; `target()`, `is_direct_executable()`, and structural/executable target iterators used by graph, plan, and Select.

- [ ] **Step 1: Write failing typed-edge contract tests**

Update `fork_and_join_compile_to_typed_controls` to require:

```rust
assert_eq!(
    fork.edges,
    vec![
        ControlEdge::ForkBranch {
            branch_id: "source_a".into(),
            target: "search_a".into(),
        },
        ControlEdge::ForkBranch {
            branch_id: "source_b".into(),
            target: "search_b".into(),
        },
        ControlEdge::ForkContinuation {
            target: "collect".into(),
        },
    ]
);
```

Update the Condition compile test to assert ordered Conditional edges:

```rust
assert_eq!(
    condition.edges,
    vec![
        ControlEdge::Conditional { target: "a".into() },
        ControlEdge::Conditional { target: "b".into() },
        ControlEdge::Conditional { target: "fallback".into() },
    ]
);
```

Add a Select regression proving a ForkContinuation is not a direct predecessor candidate.

- [ ] **Step 2: Run typed-edge tests and observe the type mismatch**

Run:

```bash
cargo test --locked --test fork_join_nodes fork_and_join_compile_to_typed_controls
cargo test --locked --test core_template_condition
cargo test --locked --test dsl_select
```

Expected: compilation fails because node edges are still `Vec<String>`.

- [ ] **Step 3: Define `ControlEdge` and traversal helpers**

Add to `src/dsl/compiled.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlEdge {
    Direct { target: String },
    Conditional { target: String },
    ForkBranch { branch_id: String, target: String },
    ForkContinuation { target: String },
}

impl ControlEdge {
    pub fn target(&self) -> &str {
        match self {
            Self::Direct { target }
            | Self::Conditional { target }
            | Self::ForkBranch { target, .. }
            | Self::ForkContinuation { target } => target,
        }
    }

    pub fn is_direct_executable(&self) -> bool {
        matches!(self, Self::Direct { .. } | Self::Conditional { .. })
    }
}

impl CompiledNode {
    pub fn structural_targets(&self) -> impl Iterator<Item = &str> {
        self.edges.iter().map(ControlEdge::target)
    }

    pub fn direct_executable_targets(&self) -> impl Iterator<Item = &str> {
        self.edges
            .iter()
            .filter(|edge| edge.is_direct_executable())
            .map(ControlEdge::target)
    }
}
```

Change both compilation structs to `pub edges: Vec<ControlEdge>`.

- [ ] **Step 4: Emit typed edges from compiler and built-in nodes**

In `AgentCompiler`, wrap common next:

```rust
edges.push(ControlEdge::Direct {
    target: next.clone(),
});
```

In Condition, preserve authored case order and append default last:

```rust
edges.push(ControlEdge::Conditional {
    target: case.next.clone(),
});
// after cases
edges.push(ControlEdge::Conditional {
    target: config.default.clone(),
});
```

In Fork, sort through the existing `BTreeMap` and produce branch edges plus one continuation:

```rust
let mut edges = config
    .branches
    .iter()
    .map(|(branch_id, target)| ControlEdge::ForkBranch {
        branch_id: branch_id.clone(),
        target: target.clone(),
    })
    .collect::<Vec<_>>();
edges.push(ControlEdge::ForkContinuation {
    target: config.join.clone(),
});
```

Action, Chat, Template, Join, Select, and End keep empty node-specific edges; their common `next` is inserted as Direct by the compiler where required.

- [ ] **Step 5: Make structural graph algorithms traverse every typed edge**

In `src/dsl/graph.rs`, replace string-edge loops with `edge.target()` or `node.structural_targets()` for:

```rust
// existence
for edge in &node.edges {
    if !nodes.contains_key(edge.target()) {
        return Err(CompileError::new(
            "NODE_EDGE_NOT_FOUND",
            format!(
                "node '{node_id}' points to missing node '{}'",
                edge.target()
            ),
        ));
    }
}

// DFS / reachability
for target in node.structural_targets() {
    visit(target, nodes, visiting, visited)?;
}
```

Build dominator predecessor sets from every structural edge so ForkContinuation makes Join/post-Join structurally reachable without making branch-local nodes dominate Join.

- [ ] **Step 6: Restrict Select topology to direct executable edges**

In `src/dsl/select.rs`, build exact predecessors only from Direct and Conditional edges:

```rust
for (node_id, node) in nodes {
    for edge in node.edges.iter().filter(|edge| edge.is_direct_executable()) {
        predecessors
            .get_mut(edge.target())
            .expect("graph edges were validated before Select validation")
            .insert(node_id.clone());
    }
}
```

Use `direct_executable_targets()` in Select pairwise reachability. ForkBranch and ForkContinuation are never Select source paths.

- [ ] **Step 7: Update plan traversal and extension fixtures**

In `src/dsl/plan.rs`, use `direct_executable_targets()` when walking inside a branch and `structural_targets()` only for whole-plan ownership checks. Keep the existing direct-to-Join settlement rule until Task 3.

Update custom node tests so extension compilations construct:

```rust
edges: vec![ControlEdge::Direct {
    target: "result".into(),
}],
```

or an empty vector when the compiler supplies common next.

- [ ] **Step 8: Verify typed-edge graph behavior**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test fork_join_nodes
cargo test --locked --test dsl_compiler
cargo test --locked --test dsl_parallel
cargo test --locked --test dsl_select
cargo test --locked --test node_extensions
cargo test --locked --all-targets
```

Expected: every command exits 0; Agent hash tests continue passing because raw normalized DSL already contains node kind, ordered Condition cases, Fork branches/Join, and End config.

- [ ] **Step 9: Commit typed edges**

```bash
git add src tests
git commit -m "refactor: type workflow control edges"
```

---

### Task 3: Make Fork branches return explicit End outcomes and enrich Join results

**Files:**
- Create: `tests/fixtures/structured_end_agent.yaml`
- Modify: `src/dsl/graph.rs`
- Modify: `src/dsl/plan.rs`
- Modify: `src/nodes/join.rs`
- Modify: `src/outcome.rs`
- Modify: `src/runtime/mod.rs`
- Modify: `src/runtime/execution.rs`
- Modify: `src/runtime/state.rs`
- Modify: `src/runtime/scheduler.rs`
- Modify: `src/runtime/coordinator.rs`
- Modify: `agents/parallel_researcher/agent.yaml`
- Modify: `tests/action_error_containment.rs`
- Modify: `tests/dsl_parallel.rs`
- Modify: `tests/dsl_select.rs`
- Modify: `tests/fork_join_nodes.rs`
- Modify: `tests/formal_agent_compile.rs`
- Modify: `tests/observability.rs`
- Modify: `tests/run_coordinator.rs`
- Modify: `tests/run_scheduler.rs`
- Modify: `tests/run_service.rs`

**Interfaces:**
- Consumes: typed ControlEdge, `TerminalOutcome`, ForkContinuation, existing branch task containment and EventHub ordering.
- Produces: every branch path ending at End; `BranchResult::Succeeded { output: RunOutput }`; `BranchError { kind, code, message }`; Join failure counts; scheduler handling for branch End.

- [ ] **Step 1: Write compiler tests for the new branch topology**

Create `tests/fixtures/structured_end_agent.yaml` as the shared compiler/runtime fixture:

```yaml
version: 1
id: structured-end-agent
name: Structured End Agent
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {a: make_a, b: make_b}
      join: collect
  make_a:
    type: core.template
    next: end_a
    config: {value: a}
  end_a:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.make_a.output }}"}}
  make_b:
    type: core.template
    next: end_b
    config: {value: b}
  end_b:
    type: core.end
    config: {outcome: failure, code: WORKFLOW_B_REJECTED, message: branch b rejected}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.end
    config: {outcome: success, data: {summary: "{{ nodes.collect.output.summary }}"}}
```

Load it in `tests/dsl_parallel.rs` with:

```rust
fn structured_end_yaml() -> &'static str {
    include_str!("fixtures/structured_end_agent.yaml")
}

fn branch_directly_targets_join_yaml() -> String {
    structured_end_yaml().replace(
        "  make_a:\n    type: core.template\n    next: end_a\n    config: {value: a}\n  end_a:\n    type: core.end\n    config: {outcome: success, data: {value: \"{{ nodes.make_a.output }}\"}}",
        "  make_a:\n    type: core.template\n    next: collect\n    config: {value: a}",
    )
}

fn branch_dead_ends_without_end_yaml() -> String {
    structured_end_yaml().replace(
        "  make_a:\n    type: core.template\n    next: end_a\n    config: {value: a}\n  end_a:\n    type: core.end\n    config: {outcome: success, data: {value: \"{{ nodes.make_a.output }}\"}}",
        "  make_a:\n    type: core.template\n    config: {value: a}",
    )
}

fn ordinary_node_targets_join_yaml() -> String {
    structured_end_yaml()
        .replace("entry: fanout", "entry: route")
        .replace(
            "nodes:\n  fanout:",
            "nodes:\n  route:\n    type: core.condition\n    config:\n      cases: [{when: \"true\", next: fanout}]\n      default: collect\n  fanout:",
        )
}
```

Add exact negative assertions:

```rust
assert_compile_error(
    &branch_directly_targets_join_yaml(),
    "BRANCH_DIRECT_JOIN_FORBIDDEN",
);
assert_compile_error(
    &branch_dead_ends_without_end_yaml(),
    "END_REQUIRED",
);
assert_compile_error(
    &ordinary_node_targets_join_yaml(),
    "JOIN_DIRECT_PREDECESSOR_FORBIDDEN",
);
```

- [ ] **Step 2: Write scheduler tests for successful and failed branch Ends**

Load the same fixture in `tests/run_scheduler.rs` and use the existing `compile_parallel_agent`, `parallel_scheduler`, `context_with_agent_input`, `SchedulerRepository`, and `stop_pair` test infrastructure:

```rust
fn structured_end_yaml() -> &'static str {
    include_str!("fixtures/structured_end_agent.yaml")
}

struct StructuredEndFixture {
    result: SchedulerResult,
    join_output: Value,
    events: Vec<RunEvent>,
    operations: Vec<String>,
}

async fn run_structured_end_agent() -> StructuredEndFixture {
    let agent = compile_parallel_agent(structured_end_yaml());
    let context = context_with_agent_input(&agent, "run_structured_end", json!({}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();
    let result = scheduler.run(context, stop).await.unwrap();
    let join_output = repository
        .outputs
        .lock()
        .await
        .iter()
        .find(|output| output.node_id == "collect")
        .expect("collect output")
        .output
        .clone();
    let events = repository.events.lock().await.clone();
    let operations = repository.operations.lock().await.clone();
    StructuredEndFixture {
        result,
        join_output,
        events,
        operations,
    }
}

#[tokio::test]
async fn explicit_branch_ends_settle_then_activate_join() {
    let fixture = run_structured_end_agent().await;
    assert!(matches!(
        fixture.result,
        SchedulerResult::Ended(TerminalOutcome::Success { .. })
    ));
    assert_eq!(fixture.join_output["summary"]["total"], 2);
    assert_eq!(fixture.join_output["summary"]["succeeded"], 1);
    assert_eq!(fixture.join_output["summary"]["failed"], 1);
    assert_eq!(fixture.join_output["summary"]["failures"]["workflow"], 1);
    assert!(fixture.events.iter().any(|event| {
        event.event_type == RunEventType::NodeCompleted
            && event.node_id.as_deref() == Some("end_b")
    }));
    assert!(!fixture.events.iter().any(|event| {
        event.event_type == RunEventType::NodeFailed
            && event.node_id.as_deref() == Some("end_b")
    }));
    let output_position = fixture
        .operations
        .iter()
        .position(|operation| operation == "output:end_b")
        .unwrap();
    let node_completed_position = fixture
        .operations
        .iter()
        .position(|operation| operation == "event:node.completed:end_b")
        .unwrap();
    let branch_failed_position = fixture
        .operations
        .iter()
        .position(|operation| operation == "event:branch.failed:-")
        .unwrap();
    assert!(output_position < node_completed_position);
    assert!(node_completed_position < branch_failed_position);
}
```

Import `TerminalOutcome` and `RunEventType` alongside the existing scheduler/event test imports. Add separate contained node-error and node-timeout cases so Join counts `node` and `timeout` without parsing codes.

Add three post-Join policy tests using the same fixture shape:

- `all_workflow_failed_branches_still_run_join_then_main_failure_end`: both branch Ends fail, Join reports `succeeded: 0`, and a Condition routes to main `core.end(failure)`; assert the scheduler returns workflow failure only after the Join output is durable.
- `all_node_failed_branches_still_run_join`: both branch executors return typed node failures; assert Join reports `node: 2` and its successor executes.
- `partial_success_can_end_as_explicit_degraded_success`: one successful and one failed branch; assert main End success data includes the Join counts rather than Join choosing the Run outcome.

Migrate the existing stop, interruption, node-timeout, task-panic, and infrastructure-drain scheduler tests to the new End transitions. Keep their sibling-drain assertions and additionally assert no `BranchResult` or Join output is produced for stop/infrastructure paths.

- [ ] **Step 3: Run the new topology and scheduler tests red**

Run:

```bash
cargo test --locked --test dsl_parallel
cargo test --locked --test run_scheduler explicit_branch_ends_settle_then_activate_join
```

Expected: compiler rejects branch End under the old missing-Join rule and scheduler treats branch End as an invariant.

- [ ] **Step 4: Replace branch-to-Join traversal with all-paths-End proof**

Keep the validation order from the design: after edge existence and cycle
checks, generic graph validation rejects every reachable non-End leaf as
`END_REQUIRED` before reporting downstream unreachable declarations;
execution-plan construction then proves that branch traversal stops only at
End and rejects illegal Join entry with Fork/branch context.

In `collect_branch_nodes`, walk only Direct/Conditional targets and stop successfully at End:

```rust
if matches!(node.control, NodeControl::End { .. }) {
    continue;
}
let targets = node.direct_executable_targets().collect::<Vec<_>>();
if targets.is_empty() {
    return Err(CompileError::new(
        "BRANCH_END_REQUIRED",
        format!(
            "fork node '{fork_id}' branch '{branch_id}' has a path ending at non-end node '{node_id}'"
        ),
    ));
}
if targets.iter().any(|target| *target == join_id) {
    return Err(CompileError::new(
        "BRANCH_DIRECT_JOIN_FORBIDDEN",
        format!(
            "fork node '{fork_id}' branch '{branch_id}' points directly to join '{join_id}'"
        ),
    ));
}
pending.extend(targets);
```

Replace old Join predecessor coverage with a check that no Direct or Conditional edge targets any Join:

```rust
if edge.is_direct_executable() && matches!(nodes[edge.target()].control, NodeControl::Join { .. }) {
    return Err(CompileError::new(
        "JOIN_DIRECT_PREDECESSOR_FORBIDDEN",
        format!("node '{node_id}' points directly to join '{}'", edge.target()),
    ));
}
```

ForkContinuation ownership remains the only accepted Join relationship.

- [ ] **Step 5: Add typed branch failure origin**

Change runtime state to:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BranchError {
    pub kind: FailureKind,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BranchResult {
    Succeeded {
        terminal_node_id: String,
        output: RunOutput,
    },
    Failed {
        terminal_node_id: String,
        error: BranchError,
    },
}
```

Add a typed timeout kind to `RunErrorKind` and make `RunError::timeout()` construct it. Map node errors to `FailureKind::Node` and node deadlines to `FailureKind::Timeout`; do not infer origin from `error.code()`.

- [ ] **Step 6: Settle branch End transitions in the scheduler**

After the End node has been marked succeeded and its output persisted, handle branch End directly:

```rust
match result.outcome.transition {
    NodeTransition::End(TerminalOutcome::Success { output }) => {
        self.settle_branch_result(
            state,
            &scope,
            BranchResult::Succeeded {
                terminal_node_id: result.node_id,
                output,
            },
        )
        .await?;
        continue;
    }
    NodeTransition::End(TerminalOutcome::Failure { error }) => {
        self.settle_branch_result(
            state,
            &scope,
            BranchResult::Failed {
                terminal_node_id: result.node_id,
                error: BranchError {
                    kind: FailureKind::Workflow,
                    code: error.code,
                    message: error.message,
                },
            },
        )
        .await?;
        continue;
    }
    NodeTransition::Next => {
        let target = node.next.as_deref().ok_or_else(|| {
            invariant(format!(
                "successful branch node '{}' did not identify a successor",
                node.id
            ))
        })?;
        state.validate_branch_target(fork_id, branch_id, target)?;
        state.activate(&self.agent, Some(target), scope, context)?;
    }
    NodeTransition::Goto(target) => {
        state.validate_branch_target(fork_id, branch_id, &target)?;
        state.activate(&self.agent, Some(&target), scope, context)?;
    }
    NodeTransition::ActivateFork => {
        return Err(invariant(format!(
            "branch node '{}' requested nested fork activation",
            node.id
        )));
    }
}
continue;
```

Add one settlement helper so authored and unexpected branch failures share exactly one `BranchResult`/branch-event construction path while preserving `node.completed` versus `node.failed` at the executor layer:

```rust
async fn settle_branch_result(
    &self,
    state: &mut SchedulerState,
    scope: &WorkScope,
    result: BranchResult,
) -> Result<(), RunError> {
    let WorkScope::Branch { fork_id, branch_id } = scope else {
        return Err(invariant("only a branch scope can settle a branch"));
    };
    let fork_id = fork_id.clone();
    let branch_id = branch_id.clone();
    let event_scope = branch_scope(state, &fork_id)?;
    state.settle_branch(scope, result.clone())?;
    match result {
        BranchResult::Succeeded {
            terminal_node_id,
            ..
        } => {
            self.events
                .publish(
                    event_scope,
                    RunEventType::BranchCompleted,
                    json!({
                        "fork_id": fork_id,
                        "branch_id": branch_id,
                        "terminal_node_id": terminal_node_id,
                    }),
                )
                .await
                .map_err(event_error)?;
        }
        BranchResult::Failed {
            terminal_node_id,
            error,
        } => {
            self.events
                .publish_error(
                    event_scope,
                    RunEventType::BranchFailed,
                    &error.code,
                    &error.message,
                    json!({
                        "fork_id": fork_id,
                        "branch_id": branch_id,
                        "terminal_node_id": terminal_node_id,
                        "kind": error.kind,
                        "error": &error,
                    }),
                )
                .await
                .map_err(event_error)?;
        }
    }
    state.activate_join_if_settled(&self.agent)
}
```

Replace the existing contained `NodeExecutionFailure::Node` branch block with a call to this helper. Classify it directly from `RunErrorKind`:

```rust
let kind = match error.kind() {
    RunErrorKind::Node => FailureKind::Node,
    RunErrorKind::Timeout => FailureKind::Timeout,
    RunErrorKind::Stop | RunErrorKind::Infrastructure => {
        return Err(invariant(
            "contained node failure had a non-settleable runtime origin",
        ));
    }
};
self.settle_branch_result(
    state,
    &scope,
    BranchResult::Failed {
        terminal_node_id: node_id,
        error: BranchError {
            kind,
            code: error.code().to_string(),
            message: error.message().to_string(),
        },
    },
)
.await?;
continue;
```

- [ ] **Step 7: Enrich Join summary without changing all-settled control**

Count typed origins in `src/nodes/join.rs`:

```rust
let workflow = results.values().filter(|result| matches!(
    result,
    BranchResult::Failed { error, .. } if error.kind == FailureKind::Workflow
)).count();
let node = results.values().filter(|result| matches!(
    result,
    BranchResult::Failed { error, .. } if error.kind == FailureKind::Node
)).count();
let timeout = results.values().filter(|result| matches!(
    result,
    BranchResult::Failed { error, .. } if error.kind == FailureKind::Timeout
)).count();

let summary = json!({
    "total": results.len(),
    "succeeded": succeeded,
    "failed": failed,
    "failures": {
        "workflow": workflow,
        "node": node,
        "timeout": timeout,
    },
});
```

Assert `failed == workflow + node + timeout` in unit tests.

- [ ] **Step 8: Migrate every parallel fixture to explicit branch End**

Replace each branch tail:

```yaml
normalize_a:
  type: core.template
  next: collect
  config: {value: normalized-a}
```

with:

```yaml
normalize_a:
  type: core.template
  next: end_a
  config: {value: normalized-a}
end_a:
  type: core.end
  config:
    outcome: success
    data: {value: "{{ nodes.normalize_a.output }}"}
```

Apply this exact structure to `agents/parallel_researcher/agent.yaml` and every Fork fixture in the Task 3 test files. Keep `collect` declared only through `fanout.config.join`.

Verify no executable branch edge points to Join in Agents/tests:

```bash
rg -n -U "type: core\.(template|chat|action|select)\n(?:.*\n){0,3}\s+next: collect" agents tests
```

Expected: no matches for branch tails. Post-Join ordinary nodes named `collect` are not allowed by the new graph validator regardless of text search.

- [ ] **Step 9: Run structured-concurrency verification**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test action_error_containment
cargo test --locked --test dsl_parallel
cargo test --locked --test dsl_select
cargo test --locked --test fork_join_nodes
cargo test --locked --test run_scheduler
cargo test --locked --test run_coordinator
cargo test --locked --test run_service
cargo test --locked --all-targets
```

Expected: every command exits 0; all-failed and partial-success Join cases execute after every branch settles.

- [ ] **Step 10: Commit structured branch returns**

```bash
git add src tests agents/parallel_researcher/agent.yaml
git commit -m "feat: make fork branches return end outcomes"
```

---

### Task 4: Make Run lifecycle, repositories, events, and API terminal data type-safe

**Files:**
- Modify: `src/outcome.rs`
- Modify: `src/history/types.rs`
- Modify: `src/history/repository.rs`
- Modify: `src/history/sqlite.rs`
- Modify: `src/history/postgres.rs`
- Modify: `src/events/hub.rs`
- Modify: `src/runtime/coordinator.rs`
- Modify: `src/runtime/service.rs`
- Modify: `src/api/formal/routes.rs`
- Modify: `migrations/formal_v1/sqlite/202607100001_formal_v1.sql`
- Modify: `migrations/formal_v1/postgres/202607100001_formal_v1.sql`
- Modify: `tests/api.rs`
- Modify: `tests/event_hub.rs`
- Modify: `tests/formal_protocol.rs`
- Modify: `tests/history_postgres.rs`
- Modify: `tests/history_sqlite_v1.rs`
- Modify: `tests/migration_layout.rs`
- Modify: `tests/run_coordinator.rs`
- Modify: `tests/run_service.rs`

**Interfaces:**
- Consumes: `RunOutput`, `RunFailure`, `FailureKind`, `TerminalOutcome`, existing RunStatus and EventHub exactly-once terminal publication.
- Produces: `RunTerminal`, `RunLifecycle`, `RunSummaryLifecycle`, typed `TerminalUpdate`, `error_kind` persistence, flat mutually exclusive HTTP terminal fields.

- [ ] **Step 1: Write failing lifecycle serialization and invariant tests**

Replace the nullable terminal construction test in `tests/formal_protocol.rs` with:

```rust
#[test]
fn run_lifecycle_serializes_mutually_exclusive_terminal_shapes() {
    let completed = RunLifecycle::Completed {
        output: RunOutput {
            content: Some("answer".into()),
            format: Some("text".into()),
            data: json!({"answer":"answer"}),
        },
    };
    assert_eq!(
        serde_json::to_value(completed).unwrap(),
        json!({
            "status":"completed",
            "output":{"content":"answer","format":"text","data":{"answer":"answer"}}
        })
    );

    let failed = RunLifecycle::Failed {
        error: RunFailure {
            kind: FailureKind::Workflow,
            code: "WORKFLOW_REJECTED".into(),
            message: "workflow rejected".into(),
        },
    };
    assert_eq!(
        serde_json::to_value(failed).unwrap(),
        json!({
            "status":"failed",
            "error":{"kind":"workflow","code":"WORKFLOW_REJECTED","message":"workflow rejected"}
        })
    );
}

#[test]
fn terminal_update_derives_status_from_the_terminal_variant() {
    let update = TerminalUpdate::new(
        "run_1",
        at(10),
        RunTerminal::Failed {
            error: RunFailure {
                kind: FailureKind::Node,
                code: "NODE_FAILED".into(),
                message: "node failed".into(),
            },
        },
    );
    assert_eq!(update.status(), RunStatus::Failed);
}
```

Add API assertions that running records omit `output` and `error`, completed records expose only `output`, and failed records expose only nested `error`.

Add exact event-contract assertions in `tests/run_coordinator.rs`:

```rust
let events = repository.list_events_after(&run_id, 0, 32).await.unwrap();
assert_eq!(
    events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect::<Vec<_>>(),
    vec![
        "run.created",
        "run.started",
        "node.started",
        "node.completed",
        "run.failed",
    ]
);
assert_eq!(events[3].node_id.as_deref(), Some("reject"));
assert_eq!(events[3].code, "OK");
assert_eq!(events[4].data, json!({"kind":"workflow"}));
assert!(!events.iter().any(|event| event.event_type == RunEventType::NodeFailed));
```

For unexpected main node failure, assert `node.started`, `node.failed`, then `run.failed(kind=node|timeout)`. For an authored branch failure, assert the causal order `node.started(end_b) < node.completed(end_b) < branch.failed(kind=workflow) < node.started(collect)` while allowing sibling branch events to interleave. Assert one and only one branch terminal event and one and only one Run terminal event per scope.

- [ ] **Step 2: Write failing migration constraint tests**

In `tests/history_sqlite_v1.rs`, execute direct invalid inserts/updates and require constraint failure for:

```sql
-- completed without output
UPDATE runs SET status = 'completed', ended_at = CURRENT_TIMESTAMP WHERE run_id = ?;

-- failed without error kind/code/message
UPDATE runs SET status = 'failed', ended_at = CURRENT_TIMESTAMP WHERE run_id = ?;

-- running with terminal error fields
UPDATE runs SET error_kind = 'workflow', error_code = 'WORKFLOW_X', error_message = 'x'
WHERE run_id = ?;
```

Mirror the legal-state assertions in PostgreSQL parity tests.

In `tests/event_hub.rs` and both history suites, add recovery cases proving a typed `FailureKind::Infrastructure` is stored and replayed unchanged, terminal publication remains exactly once after a retry, and startup reconciliation produces `Interrupted` with no failure kind rather than fabricating workflow failure.

- [ ] **Step 3: Run lifecycle/history tests red**

Run:

```bash
cargo test --locked --test formal_protocol
cargo test --locked --test history_sqlite_v1
cargo test --locked --test migration_layout
```

Expected: compilation fails because the typed lifecycle does not exist and current migrations accept invalid terminal combinations.

- [ ] **Step 4: Define typed lifecycle and terminal update**

In `src/history/types.rs`, use:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RunLifecycle {
    Created,
    Running,
    Completed { output: RunOutput },
    Failed { error: RunFailure },
    Cancelled { error: StopError },
    Interrupted { error: StopError },
}

impl RunLifecycle {
    pub fn status(&self) -> RunStatus {
        match self {
            Self::Created => RunStatus::Created,
            Self::Running => RunStatus::Running,
            Self::Completed { .. } => RunStatus::Completed,
            Self::Failed { .. } => RunStatus::Failed,
            Self::Cancelled { .. } => RunStatus::Cancelled,
            Self::Interrupted { .. } => RunStatus::Interrupted,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StopError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RunTerminal {
    Completed { output: RunOutput },
    Failed { error: RunFailure },
    Cancelled { error: StopError },
    Interrupted { error: StopError },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TerminalUpdate {
    pub run_id: String,
    pub ended_at: DateTime<Utc>,
    pub terminal: RunTerminal,
}

impl RunTerminal {
    pub fn status(&self) -> RunStatus {
        match self {
            Self::Completed { .. } => RunStatus::Completed,
            Self::Failed { .. } => RunStatus::Failed,
            Self::Cancelled { .. } => RunStatus::Cancelled,
            Self::Interrupted { .. } => RunStatus::Interrupted,
        }
    }

    pub fn output(&self) -> Option<&RunOutput> {
        match self {
            Self::Completed { output } => Some(output),
            _ => None,
        }
    }

    pub fn failure(&self) -> Option<&RunFailure> {
        match self {
            Self::Failed { error } => Some(error),
            _ => None,
        }
    }

    pub fn stop_error(&self) -> Option<&StopError> {
        match self {
            Self::Cancelled { error } | Self::Interrupted { error } => Some(error),
            _ => None,
        }
    }

    pub fn error_code(&self) -> Option<&str> {
        self.failure()
            .map(|error| error.code.as_str())
            .or_else(|| self.stop_error().map(|error| error.code.as_str()))
    }

    pub fn error_message(&self) -> Option<&str> {
        self.failure()
            .map(|error| error.message.as_str())
            .or_else(|| self.stop_error().map(|error| error.message.as_str()))
    }
}

impl TerminalUpdate {
    pub fn new(
        run_id: impl Into<String>,
        ended_at: DateTime<Utc>,
        terminal: RunTerminal,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            ended_at,
            terminal,
        }
    }

    pub fn status(&self) -> RunStatus {
        self.terminal.status()
    }
}
```

Change `RunRecord` to contain `#[serde(flatten)] pub lifecycle: RunLifecycle` and add `status()` delegating to `lifecycle.status()`. Define the summary lifecycle without completed output but with the exact terminal error variants:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RunSummaryLifecycle {
    Created,
    Running,
    Completed,
    Failed { error: RunFailure },
    Cancelled { error: StopError },
    Interrupted { error: StopError },
}

impl From<&RunLifecycle> for RunSummaryLifecycle {
    fn from(lifecycle: &RunLifecycle) -> Self {
        match lifecycle {
            RunLifecycle::Created => Self::Created,
            RunLifecycle::Running => Self::Running,
            RunLifecycle::Completed { .. } => Self::Completed,
            RunLifecycle::Failed { error } => Self::Failed {
                error: error.clone(),
            },
            RunLifecycle::Cancelled { error } => Self::Cancelled {
                error: error.clone(),
            },
            RunLifecycle::Interrupted { error } => Self::Interrupted {
                error: error.clone(),
            },
        }
    }
}
```

`RunSummary` contains `#[serde(flatten)] pub lifecycle: RunSummaryLifecycle`. No accessor may synthesize or parse an error code.

- [ ] **Step 5: Rewrite both initial migrations with equivalent terminal constraints**

Add `error_kind` and one full-state CHECK. SQLite form:

```sql
error_kind TEXT CHECK (
    error_kind IS NULL OR error_kind IN ('workflow', 'node', 'timeout', 'infrastructure')
),
error_code TEXT,
error_message TEXT,
CHECK (
    (status IN ('created', 'running')
        AND ended_at IS NULL
        AND output IS NULL
        AND error_kind IS NULL
        AND error_code IS NULL
        AND error_message IS NULL)
    OR
    (status = 'completed'
        AND ended_at IS NOT NULL
        AND output IS NOT NULL
        AND error_kind IS NULL
        AND error_code IS NULL
        AND error_message IS NULL)
    OR
    (status = 'failed'
        AND ended_at IS NOT NULL
        AND output IS NULL
        AND error_kind IS NOT NULL
        AND error_code IS NOT NULL
        AND error_message IS NOT NULL)
    OR
    (status IN ('cancelled', 'interrupted')
        AND ended_at IS NOT NULL
        AND output IS NULL
        AND error_kind IS NULL
        AND error_code IS NOT NULL
        AND error_message IS NOT NULL)
)
```

Use the same boolean expression in PostgreSQL with JSONB output. Preserve existing indexes and foreign keys.

- [ ] **Step 6: Bind and reconstruct typed lifecycle in SQLite and PostgreSQL**

Add `error_kind` to every insert/select/update/recovery query and row struct. Build lifecycle through one checked mapper:

```rust
fn lifecycle_from_columns(
    status: RunStatus,
    output: Option<RunOutput>,
    error_kind: Option<FailureKind>,
    error_code: Option<String>,
    error_message: Option<String>,
) -> Result<RunLifecycle, HistoryError> {
    match (status, output, error_kind, error_code, error_message) {
        (RunStatus::Created, None, None, None, None) => Ok(RunLifecycle::Created),
        (RunStatus::Running, None, None, None, None) => Ok(RunLifecycle::Running),
        (RunStatus::Completed, Some(output), None, None, None) => {
            Ok(RunLifecycle::Completed { output })
        }
        (RunStatus::Failed, None, Some(kind), Some(code), Some(message)) => {
            Ok(RunLifecycle::Failed {
                error: RunFailure {
                    kind,
                    code,
                    message,
                },
            })
        }
        (RunStatus::Cancelled, None, None, Some(code), Some(message)) => {
            Ok(RunLifecycle::Cancelled {
                error: StopError { code, message },
            })
        }
        (RunStatus::Interrupted, None, None, Some(code), Some(message)) => {
            Ok(RunLifecycle::Interrupted {
                error: StopError { code, message },
            })
        }
        (status, _, _, _, _) => Err(HistoryError::new(
            "HISTORY_TERMINAL_CORRUPT",
            format!(
                "run columns are inconsistent with lifecycle status '{}'",
                status.as_str()
            ),
        )),
    }
}
```

Parse `error_kind` with one exact conversion rather than accepting arbitrary strings:

```rust
fn parse_failure_kind(value: &str) -> Result<FailureKind, HistoryError> {
    FailureKind::parse(value).ok_or_else(|| {
        HistoryError::new(
            "HISTORY_TERMINAL_CORRUPT",
            format!("stored failure kind '{value}' is invalid"),
        )
    })
}
```

`finish_run` and recovery obtain bind values only through typed `TerminalUpdate` accessors.

- [ ] **Step 7: Update coordinator and EventHub terminal publication**

Map authoritative sources without code parsing. Handle scheduler failures inline so no untyped conversion helper remains:

```rust
let terminal = match scheduler_result {
    SchedulerResult::Ended(TerminalOutcome::Success { output }) => {
        RunTerminal::Completed { output }
    }
    SchedulerResult::Ended(TerminalOutcome::Failure { error }) => {
        RunTerminal::Failed {
            error: RunFailure {
                kind: FailureKind::Workflow,
                code: error.code,
                message: error.message,
            },
        }
    }
    SchedulerResult::Failed(error) => {
        let kind = match error.kind() {
            RunErrorKind::Node => FailureKind::Node,
            RunErrorKind::Timeout => FailureKind::Timeout,
            RunErrorKind::Infrastructure => FailureKind::Infrastructure,
            RunErrorKind::Stop => {
                return Err(RunError::infrastructure(
                    "RUN_TERMINAL_INVALID",
                    "scheduler returned a stop error as a failed result",
                ));
            }
        };
        RunTerminal::Failed {
            error: RunFailure {
                kind,
                code: error.code().to_string(),
                message: error.message().to_string(),
            },
        }
    }
    SchedulerResult::Stopped(error) => match error.stop_reason() {
        Some(StopReason::Cancelled) => RunTerminal::Cancelled {
            error: StopError {
                code: error.code().to_string(),
                message: error.message().to_string(),
            },
        },
        Some(StopReason::Interrupted) => RunTerminal::Interrupted {
            error: StopError {
                code: error.code().to_string(),
                message: error.message().to_string(),
            },
        },
        Some(StopReason::TimedOut) => RunTerminal::Failed {
            error: RunFailure {
                kind: FailureKind::Timeout,
                code: error.code().to_string(),
                message: error.message().to_string(),
            },
        },
        None => {
            return Err(RunError::infrastructure(
                "RUN_TERMINAL_INVALID",
                "scheduler returned an untyped stop result",
            ));
        }
    },
};
```

Publish `run.failed` with top-level code/message and `data: {"kind": failure.kind}`. Completed data remains the RunOutput. Cancelled/interrupted event types remain unchanged. Infrastructure recovery writes `FailureKind::Infrastructure`.

Update EventHub recovery requests to clone typed TerminalUpdate and validate terminal event type through `update.status()`.

- [ ] **Step 8: Update service/API consumers to lifecycle accessors**

Replace direct `record.status` reads with `record.status()`. Serialize RunRecord's flattened lifecycle so the exact JSON shapes are:

```json
{"status":"running"}
```

```json
{"status":"completed","output":{"data":{}}}
```

```json
{"status":"failed","error":{"kind":"workflow","code":"WORKFLOW_X","message":"x"}}
```

Do not serialize inapplicable output/error fields as null.

- [ ] **Step 9: Run persistence, recovery, API, and parity verification**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test formal_protocol
cargo test --locked --test history_sqlite_v1
cargo test --locked --test history_postgres
cargo test --locked --test migration_layout
cargo test --locked --test event_hub
cargo test --locked --test run_coordinator
cargo test --locked --test run_service
cargo test --locked --test api
cargo test --locked --all-targets
```

Expected: every command exits 0; PostgreSQL integration tests may skip execution when their configured database is unavailable, but compile and migration-layout parity must pass.

- [ ] **Step 10: Commit typed durable lifecycle**

```bash
git add src migrations tests
git commit -m "refactor: type durable run terminal state"
```

---

### Task 5: Complete Agent, documentation, observability, and real-binary migration

**Files:**
- Create: `agents/workflow_failure_demo/agent.yaml`
- Modify: `agents/code_node_demo/agent.yaml`
- Modify: `agents/medical_report_interpreter/agent.yaml`
- Modify: `agents/parallel_researcher/agent.yaml`
- Modify: `agents/researcher/agent.yaml`
- Modify: `README.md`
- Modify: `docs/formal-v1-breaking-changes.md`
- Modify: `src/runtime/execution.rs`
- Modify: `src/runtime/coordinator.rs`
- Modify: `tests/binary_smoke.rs`
- Modify: `tests/formal_agent_compile.rs`
- Modify: `tests/observability.rs`
- Modify: `tests/platform_config_v1.rs`
- Modify: `tests/repository_agents_v1.rs`

**Interfaces:**
- Consumes: final End/Fork/Join/lifecycle contracts from Tasks 1-4.
- Produces: body-free End metadata logs, repository-wide canonical DSL examples, no-secret success and workflow-failure binary smoke coverage.

- [ ] **Step 1: Write failing End observability tests**

Add a workflow failure message secret constant and assert:

```rust
#[tokio::test]
async fn authored_end_failure_logs_metadata_without_message_or_bodies() {
    let _guard = reset_logs().await;
    let fixture = fixture(&["workflow_failure"]).await;
    let record = fixture
        .service
        .create_detached(
            "workflow_failure",
            json!({"secret": INPUT_SECRET}),
            RequestMetadata { request_id: None },
        )
        .await
        .unwrap();
    wait_for_terminal(&fixture.service, &record.run_id).await;

    let end = info_logs("node.completed")
        .into_iter()
        .find(|event| event.field("kind") == Some("core.end"))
        .expect("end completion log");
    assert_eq!(end.field("terminal_outcome"), Some("failure"));

    let finished = info_logs("run.finished").pop().expect("run finish log");
    assert_eq!(finished.field("failure_kind"), Some("workflow"));
    assert_eq!(finished.field("error_code"), Some("WORKFLOW_OBSERVABILITY_REJECTED"));
    assert_logs_exclude(&[INPUT_SECRET, WORKFLOW_MESSAGE_SECRET]);
}
```

Update the fixture writer with an End failure Agent whose static message equals `WORKFLOW_MESSAGE_SECRET`. Replace the status-specific polling helper with this terminal-aware helper so the test does not assume authored failure will complete successfully:

```rust
async fn wait_for_terminal(service: &RunService, run_id: &str) -> RunRecord {
    for _ in 0..200 {
        let record = service.get_run(run_id).await.unwrap();
        if record.status().is_terminal() {
            return record;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("run {run_id} did not reach a terminal status");
}
```

- [ ] **Step 2: Run observability test red**

Run:

```bash
cargo test --locked --test observability authored_end_failure_logs_metadata_without_message_or_bodies -- --nocapture
```

Expected: failure because terminal outcome/failure kind fields are not yet logged.

- [ ] **Step 3: Add safe End and Run terminal log metadata**

Before logging `node.completed`, derive only the transition tag:

```rust
let terminal_outcome = match &outcome.transition {
    NodeTransition::End(TerminalOutcome::Success { .. }) => "success",
    NodeTransition::End(TerminalOutcome::Failure { .. }) => "failure",
    _ => "",
};
```

Record `terminal_outcome` and serialized envelope size, never the envelope. Extend `TerminalLogSummary` with `failure_kind: Option<FailureKind>` and log its stable string plus error code. Do not log message.

- [ ] **Step 4: Add a checked-in no-secret failure Agent**

Create `agents/workflow_failure_demo/agent.yaml`:

```yaml
version: 1
id: workflow_failure_demo
name: Workflow Failure Demo
description: Demonstrates an authored workflow failure terminal.
input:
  schema:
    type: object
    additionalProperties: false
entry: reject
nodes:
  reject:
    type: core.end
    config:
      outcome: failure
      code: WORKFLOW_DEMO_REJECTED
      message: workflow failure demo rejected the run
```

Do not enable it in `config/platform.quickstart.yaml`; the binary test supplies a temporary enabled list.

- [ ] **Step 5: Extend real-binary smoke to success and failure**

In the temporary platform config, enable:

```yaml
enabled:
  - code_node_demo
  - workflow_failure_demo
```

Rename polling to `wait_for_terminal_run` and stop on `completed`, `failed`, `cancelled`, or `interrupted`. Add a second detached Run:

```rust
let failed = create_and_wait(
    &client,
    &base_url,
    "workflow_failure_demo",
    json!({}),
).await;
assert_eq!(failed["data"]["status"], "failed");
assert_eq!(failed["data"]["error"]["kind"], "workflow");
assert_eq!(failed["data"]["error"]["code"], "WORKFLOW_DEMO_REJECTED");
assert!(failed["data"].get("output").is_none());
```

Keep method+URL labels in every HTTP diagnostic and retain graceful child shutdown.

- [ ] **Step 6: Rewrite README and current breaking-change documentation**

README must include:

```text
core.end: Ends the current Run or Fork-branch scope with an explicit success or failure outcome.
```

Replace every current `core.output` example with `core.end(outcome: success)`. Replace the parallel example so each branch ends explicitly and no branch points to Join. Document:

- End success/failure strict union;
- branch scope versus main scope;
- failure End emits `node.completed` followed by branch/run failure;
- Join output `error.kind` and nested failure counts;
- all-failed Join still runs;
- Condition decides failure/degraded success;
- local Formal V1 databases must be recreated after the migration rewrite.

Update `docs/formal-v1-breaking-changes.md` with the exact reset instruction already used for other Formal V1 pre-adoption breaking changes; do not edit historical superpowers design documents.

- [ ] **Step 7: Complete repository fixture and formal compile coverage**

Update `tests/formal_agent_compile.rs` so the all-built-ins Agent includes exactly the eight final nodes and ends with End. Update `tests/repository_agents_v1.rs` to compile all five checked-in Agents, while asserting `config/platform.yaml` enables only the four production Agents and `workflow_failure_demo` remains opt-in. Update the quickstart configuration test to keep the no-key path limited to `code_node_demo`.

Run repository scope searches:

```bash
rg -n "core\.output|OutputNode|NodeTransition::Complete" README.md agents src tests docs/formal-v1-breaking-changes.md
rg -n -U "type: core\.(template|chat|action|select)\n(?:.*\n){0,3}\s+next: collect" README.md agents tests
```

Expected: no matches.

- [ ] **Step 8: Run migration, observability, and binary verification**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --test formal_agent_compile
cargo test --locked --test repository_agents_v1
cargo test --locked --test platform_config_v1
cargo test --locked --test observability -- --nocapture --test-threads=1
cargo test --locked --test binary_smoke -- --nocapture --test-threads=1
cargo test --locked --all-targets
```

Expected: every command exits 0; the real binary returns one completed Run and one failed Run with workflow origin.

- [ ] **Step 9: Commit repository-wide migration**

```bash
git add README.md docs/formal-v1-breaking-changes.md agents src tests
git commit -m "docs: migrate formal v1 to unified end semantics"
```

---

### Task 6: Run full verification, scope audit, and whole-branch review

**Files:**
- Verify: entire repository
- Compare: `docs/superpowers/specs/2026-07-13-unified-core-end-terminal-model-design.md`
- Compare: `docs/superpowers/plans/2026-07-13-unified-core-end-terminal-model.md`

**Interfaces:**
- Consumes: Tasks 1-5 complete and committed.
- Produces: fresh evidence that the final branch implements the complete spec with no legacy public terminal contract.

- [ ] **Step 1: Prove repository scope is clean**

Run:

```bash
git status --short
rg -n "core\.output|OutputNode|NodeTransition::Complete" README.md agents src tests docs/formal-v1-breaking-changes.md
rg -n "core\.fail" README.md agents src tests docs/formal-v1-breaking-changes.md
rg -n "terminal: bool|pub terminal: bool" src tests
git diff --check 29c6e7d..HEAD
```

Expected: clean status; all three searches produce no matches; diff check exits 0.

- [ ] **Step 2: Run formatting, lint, and locked full tests**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
```

Expected: every command exits 0 with no warnings treated as errors and no failed tests.

- [ ] **Step 3: Run dependency security and policy gates**

Run:

```bash
cargo audit
cargo deny check
```

Expected: both exit 0. Existing accepted duplicate/MPL allowance warnings are acceptable only when the final deny line is `advisories ok, bans ok, licenses ok, sources ok` and no new policy failure appears.

- [ ] **Step 4: Audit every design acceptance criterion against tests**

Build a local checklist from Sections 20 and 21 of the design and record the exact test covering each item. Minimum mapping:

```text
End union/envelope                  tests/core_end.rs
main End scheduler/events           tests/run_scheduler.rs, tests/run_coordinator.rs
typed edges/End paths               tests/dsl_compiler.rs, tests/dsl_parallel.rs
Select preservation                 tests/dsl_select.rs
branch End/Join taxonomy            tests/fork_join_nodes.rs, tests/run_scheduler.rs
typed terminal persistence          tests/formal_protocol.rs, tests/history_sqlite_v1.rs, tests/history_postgres.rs
HTTP/SSE consistency                tests/api.rs, tests/event_hub.rs
body-free observability             tests/observability.rs
real process success/failure        tests/binary_smoke.rs
checked-in Agent compilation        tests/repository_agents_v1.rs
```

If any acceptance item has no direct assertion, add the missing focused test, run it red, implement the minimal correction, and rerun its owning task gate before continuing.

- [ ] **Step 5: Request whole-branch architecture and code review**

Use `superpowers:requesting-code-review` against the range `29c6e7d..HEAD`. The review request must explicitly inspect:

```text
typed graph correctness and dominance
ForkContinuation non-executability
branch settlement and cancellation draining
authored failure versus node failure event semantics
terminal persistence invariants and recovery
HTTP/event/body-free observability contracts
legacy core.output/core.fail absence
spec and plan coverage
```

Expected: reviewer reports no Critical, Major, or Minor correctness issue and says the branch is ready to integrate.

- [ ] **Step 6: Apply review fixes with fresh verification when required**

For each actionable finding, first add or tighten a focused failing test, then make the smallest correction. After the final correction, rerun all commands from Steps 1-3. Commit real review fixes as:

```bash
git add src tests migrations agents README.md docs/formal-v1-breaking-changes.md
git diff --cached --check
git commit -m "fix: address unified end review findings"
```

If review is clean and no file changes, do not create an empty commit.

- [ ] **Step 7: Finish the development branch**

Use `superpowers:verification-before-completion`, then `superpowers:finishing-a-development-branch`. Present the four integration options only after all fresh gates and review pass.

---

## Spec Coverage Matrix

| Design sections | Owning plan tasks |
|---|---|
| 1-7 context, decision, End DSL, typed outcome, scope | Task 1 |
| 8-10 Fork topology, typed edges, graph validation | Tasks 2-3 |
| 11-13 Join output, policy, scheduler | Task 3 |
| 14-17 events, failure taxonomy, lifecycle, HTTP | Tasks 3-4 |
| 18 observability and data safety | Task 5 |
| 19 registry and breaking migration | Tasks 1, 3, 5 |
| 20 verification strategy | Tasks 1-6 |
| 21 acceptance criteria | Task 6 |
| 22 reviewable implementation boundaries | All task/commit boundaries |
