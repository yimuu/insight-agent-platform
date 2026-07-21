# Core Select Condition Convergence Implementation Plan

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **Historical / superseded:** authored `core.select` was removed. See [DSL Authoring Surface Redesign](../specs/2026-07-17-dsl-authoring-surface-redesign.md) for current `switch` syntax and [DSL vNext Region/SSA Design](../specs/2026-07-16-dsl-vnext-region-ssa-design.md) for retained lowering semantics.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a strict `core.select` node that converts exactly one visible output from mutually exclusive condition paths into one dominating `{source_node_id, value}` output.

**Architecture:** `SelectNode` compiles its explicit source list into an immutable `BTreeSet<String>` stored in both the node body and `NodeControl::Select`. A focused compiler pass runs after execution-region planning and before ordinary reference validation to prove source existence, exact predecessor equality, region locality, and pairwise non-reachability. Runtime execution remains an ordinary `Next` node: it reads already-visible outputs from `RunContext`, requires exactly one present source, and relies on the existing scheduler, event, persistence, timeout, cancellation, and `all_settled` behavior.

**Tech Stack:** Rust 1.94.1, Serde/serde_json, Tokio, existing Formal V1 DAG compiler, `RunContext`, `NodeType`/`NodeExecutor` registries, Cargo integration tests, SQLite observability fixture.

## Global Constraints

- `core.select` is one-of-N condition-path convergence; it must not aggregate parallel fork branches.
- `core.join` remains the only fixed parallel convergence primitive and keeps `mode: all_settled` unchanged.
- `config.sources` is required, contains at least two unique canonical node IDs, and has no priority or fallback semantics.
- `next` is required, `emit: content` is forbidden, and Select is non-terminal.
- The declared source set must exactly equal the Select node's direct predecessor set.
- Every source must share the Select node's `NodeRegion` and be pairwise unreachable from every other source.
- Existing `BRANCH_*` and `JOIN_*` validation errors remain authoritative when fork-region compilation rejects an invalid topology before Select validation.
- Source references are not ordinary `CompiledNode.references`; ordinary dominance validation must remain strict and unchanged.
- Exactly one visible source succeeds with `{source_node_id, value}`; source values are copied exactly without coercion or merging.
- `Some(Value::Null)` is present; an unexecuted source remains absent and is never globally inserted as JSON `null`.
- Zero visible sources fail with `SELECT_SOURCE_MISSING`; multiple visible sources fail with `SELECT_SOURCE_AMBIGUOUS`.
- Compile and runtime diagnostics may contain structural node IDs but never source output bodies.
- No new scheduler transition, RunContext field, event type, repository schema, migration, API, SSE, model, Action, or provider contract.
- Do not add a new enabled production Agent for this milestone.
- No new crate dependency.
- Preserve all existing Agents and fork/join/reference tests.

---

## File Structure

- Create `src/nodes/select.rs`: local config validation plus the ordinary Select executor.
- Modify `src/nodes/mod.rs`: export and register `SelectNode` in both default registries.
- Modify `src/dsl/compiled.rs`: add typed `NodeControl::Select { sources }` metadata.
- Create `src/dsl/select.rs`: topology validation isolated from generic dominance validation.
- Modify `src/dsl/mod.rs`: expose the Select validator inside the DSL crate.
- Modify `src/dsl/compiler.rs`: run Select validation after execution-plan compilation and before reference validation.
- Modify `src/dsl/graph.rs`: keep the public `validate_graph` helper consistent with the compiler pipeline.
- Create `tests/core_select.rs`: node config, envelope, execution, null, error, and stop tests.
- Create `tests/dsl_select.rs`: valid/invalid graph topology and reference tests.
- Modify `tests/core_output.rs`: default registry parity includes `core.select`.
- Modify `tests/run_scheduler.rs`: deterministic main-path and branch-local scheduler/event/persistence coverage.
- Modify `tests/formal_agent_compile.rs`: prove Select output is a valid input to Template, Action, Chat, and Output nodes.
- Modify `tests/observability.rs`: prove selected values remain absent from INFO logs.
- Modify `README.md`: list all eight built-in nodes and document complete Select DSL plus Select-vs-Join semantics.

## Task 1: Add the Typed Select Node Contract and Executor

**Files:**
- Create: `tests/core_select.rs`
- Create: `src/nodes/select.rs`
- Modify: `src/dsl/compiled.rs`
- Modify: `src/nodes/mod.rs`
- Modify: `tests/core_output.rs`

**Interfaces:**
- Consumes: `NodeType::compile(&self, node_id: &str, config: Value, context: &mut CompileContext<'_>) -> Result<NodeCompilation, CompileError>`.
- Consumes: `RunContext::node_output(&self, node_id: &str) -> Option<&Value>`.
- Produces: `NodeControl::Select { sources: BTreeSet<String> }`.
- Produces: compiled Select body type `BTreeSet<String>`.
- Produces: `SelectNode` implementing `NodeType` and `NodeExecutor` for kind `core.select`.
- Produces: stable success output `{source_node_id, value}` and node errors `SELECT_SOURCE_MISSING` / `SELECT_SOURCE_AMBIGUOUS`.

- [ ] **Step 1: Create the failing node contract tests**

Create `tests/core_select.rs` with:

```rust
use std::{collections::BTreeSet, time::Duration};

use chrono::Utc;
use insight_agent_platform::{
    dsl::{
        compiled::{CompiledNode, NextPolicy, NodeControl, NodeOutcome, NodeTransition},
        compiler::CompileContext,
        EmitPolicy,
    },
    nodes::default_node_registries,
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::{
        stop_pair, ExecutionControl, RunContext, RunError, RunMetadata, StopReason,
    },
};
use serde_json::{json, Value};

fn compile_select(node_id: &str, config: Value) -> Result<insight_agent_platform::dsl::compiled::NodeCompilation, insight_agent_platform::dsl::CompileError> {
    let (types, _) = default_node_registries().unwrap();
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);
    types
        .resolve("core.select")?
        .compile(node_id, config, &mut context)
}

fn compiled_node(compilation: insight_agent_platform::dsl::compiled::NodeCompilation) -> CompiledNode {
    CompiledNode {
        id: "selected".to_string(),
        kind: "core.select".to_string(),
        next: Some("result".to_string()),
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(1),
        body: compilation.body,
        edges: compilation.edges,
        references: compilation.references,
        terminal: compilation.terminal,
        control: compilation.control,
    }
}

fn context(outputs: impl IntoIterator<Item = (&'static str, Value)>) -> RunContext {
    let mut context = RunContext::new(
        RunMetadata {
            run_id: "run_select".to_string(),
            request_id: "req_select".to_string(),
            agent_id: "select_agent".to_string(),
            agent_version: "sha256:select".to_string(),
            started_at: Utc::now(),
        },
        json!({}),
    );
    for (node_id, output) in outputs {
        context.set_node_output(node_id, output);
    }
    context
}

fn control() -> ExecutionControl {
    let (_, signal) = stop_pair();
    ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) })
}

async fn execute(
    outputs: impl IntoIterator<Item = (&'static str, Value)>,
) -> Result<NodeOutcome, RunError> {
    let compilation = compile_select(
        "selected",
        json!({"sources":["medical", "general"]}),
    )
    .unwrap();
    let node = compiled_node(compilation);
    let (_, executors) = default_node_registries().unwrap();
    executors
        .resolve("core.select")
        .unwrap()
        .execute(&node, &context(outputs), &control())
        .await
}

#[test]
fn select_compiles_to_a_typed_ordinary_successor_contract() {
    let compilation = compile_select(
        "selected",
        json!({"sources":["medical", "general"]}),
    )
    .unwrap();

    assert_eq!(compilation.envelope.next, NextPolicy::Required);
    assert!(!compilation.envelope.allows_content_emit);
    assert!(compilation.edges.is_empty());
    assert!(compilation.references.is_empty());
    assert!(!compilation.terminal);
    assert_eq!(
        compilation.control,
        NodeControl::Select {
            sources: BTreeSet::from(["general".to_string(), "medical".to_string()]),
        }
    );
}

#[test]
fn select_rejects_invalid_local_contracts_with_stable_codes() {
    let cases = [
        (json!({}), "NODE_CONFIG_INVALID"),
        (json!({"sources":[], "extra":true}), "NODE_CONFIG_INVALID"),
        (json!({"sources":[]}), "SELECT_SOURCE_COUNT_INVALID"),
        (json!({"sources":["medical"]}), "SELECT_SOURCE_COUNT_INVALID"),
        (
            json!({"sources":["medical", "medical"]}),
            "SELECT_SOURCE_DUPLICATE",
        ),
        (
            json!({"sources":["medical", "bad-id"]}),
            "SELECT_SOURCE_ID_INVALID",
        ),
        (
            json!({"sources":["selected", "medical"]}),
            "SELECT_SOURCE_ID_INVALID",
        ),
    ];

    for (config, expected) in cases {
        let error = compile_select("selected", config)
            .err()
            .expect("invalid Select config must fail");
        assert_eq!(error.code(), expected, "unexpected error: {error}");
    }
}

#[tokio::test]
async fn select_returns_the_only_visible_source_without_coercion() {
    assert_eq!(
        execute([("medical", json!({"text":"answer"}))])
            .await
            .unwrap(),
        NodeOutcome {
            output: json!({
                "source_node_id": "medical",
                "value": {"text":"answer"},
            }),
            transition: NodeTransition::Next,
        }
    );
}

#[tokio::test]
async fn select_treats_an_executed_json_null_as_present() {
    assert_eq!(
        execute([("general", Value::Null)]).await.unwrap().output,
        json!({"source_node_id":"general", "value":null})
    );
}

#[tokio::test]
async fn select_rejects_zero_and_multiple_visible_sources_without_output_bodies() {
    let missing = execute([]).await.unwrap_err();
    assert_eq!(missing.code(), "SELECT_SOURCE_MISSING");
    assert_eq!(missing.message(), "select node 'selected' has no completed source");

    let ambiguous = execute([
        ("medical", json!({"secret":"medical-secret"})),
        ("general", json!({"secret":"general-secret"})),
    ])
    .await
    .unwrap_err();
    assert_eq!(ambiguous.code(), "SELECT_SOURCE_AMBIGUOUS");
    assert_eq!(
        ambiguous.message(),
        "select node 'selected' has multiple completed sources: general, medical"
    );
    assert!(!ambiguous.message().contains("medical-secret"));
    assert!(!ambiguous.message().contains("general-secret"));
}

#[tokio::test]
async fn select_preserves_the_authoritative_stop_reason() {
    let compilation = compile_select(
        "selected",
        json!({"sources":["medical", "general"]}),
    )
    .unwrap();
    let node = compiled_node(compilation);
    let (controller, signal) = stop_pair();
    assert!(controller.request(StopReason::Cancelled));
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });
    let (_, executors) = default_node_registries().unwrap();

    let error = executors
        .resolve("core.select")
        .unwrap()
        .execute(&node, &context([]), &control)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "RUN_CANCELLED");
    assert_eq!(error.stop_reason(), Some(StopReason::Cancelled));
}
```

In `tests/core_output.rs`, replace the `expected` vector in `default_registries_contain_all_formal_core_nodes` with:

```rust
    let expected = vec![
        "core.action",
        "core.chat",
        "core.condition",
        "core.fork",
        "core.join",
        "core.output",
        "core.select",
        "core.template",
    ];
```

- [ ] **Step 2: Run the focused tests and verify the red state**

Run:

```bash
cargo test --test core_select --test core_output -- --nocapture
```

Expected: FAIL because `core.select` is not registered and the registry parity list differs.

- [ ] **Step 3: Add the typed control variant**

In `src/dsl/compiled.rs`, add this variant after `Join`:

```rust
    Select {
        sources: BTreeSet<String>,
    },
```

- [ ] **Step 4: Implement `SelectNode`**

Create `src/nodes/select.rs` with:

```rust
use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules,
            NodeOutcome, NodeTransition,
        },
        compiler::CompileContext,
        references::is_dsl_identifier,
        CompileError,
    },
    nodes::registry::{NodeExecutor, NodeType},
    runtime::{ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectConfig {
    sources: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct SelectNode;

impl NodeType for SelectNode {
    fn kind(&self) -> &'static str {
        "core.select"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: SelectConfig = serde_json::from_value(config).map_err(|error| {
            CompileError::new(
                "NODE_CONFIG_INVALID",
                format!("invalid core.select config for node '{node_id}': {error}"),
            )
        })?;
        if config.sources.len() < 2 {
            return Err(CompileError::new(
                "SELECT_SOURCE_COUNT_INVALID",
                format!("select node '{node_id}' must define at least two sources"),
            ));
        }

        let mut sources = BTreeSet::new();
        for source in config.sources {
            if source == node_id || !is_dsl_identifier(&source) {
                return Err(CompileError::new(
                    "SELECT_SOURCE_ID_INVALID",
                    format!("select node '{node_id}' has invalid source ID '{source}'"),
                ));
            }
            if !sources.insert(source.clone()) {
                return Err(CompileError::new(
                    "SELECT_SOURCE_DUPLICATE",
                    format!("select node '{node_id}' declares source '{source}' more than once"),
                ));
            }
        }

        Ok(NodeCompilation {
            body: Arc::new(sources.clone()),
            edges: Vec::new(),
            references: BTreeSet::new(),
            terminal: false,
            control: NodeControl::Select { sources },
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Required,
                allows_content_emit: false,
            },
        })
    }
}

#[async_trait]
impl NodeExecutor for SelectNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        if let Some(reason) = control.stop_reason() {
            return Err(RunError::stopped(reason));
        }
        let sources = node.body::<BTreeSet<String>>()?;
        let visible = sources
            .iter()
            .filter_map(|source| context.node_output(source).map(|value| (source, value)))
            .collect::<Vec<_>>();

        match visible.as_slice() {
            [(source, value)] => Ok(NodeOutcome {
                output: json!({"source_node_id": source, "value": value}),
                transition: NodeTransition::Next,
            }),
            [] => Err(RunError::new(
                "SELECT_SOURCE_MISSING",
                format!("select node '{}' has no completed source", node.id),
            )),
            values => {
                let source_ids = values
                    .iter()
                    .map(|(source, _)| source.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(RunError::new(
                    "SELECT_SOURCE_AMBIGUOUS",
                    format!(
                        "select node '{}' has multiple completed sources: {source_ids}",
                        node.id
                    ),
                ))
            }
        }
    }
}
```

- [ ] **Step 5: Register the node in both default registries**

In `src/nodes/mod.rs`:

1. Add `pub mod select;`.
2. Add `select::SelectNode` to the `use self::{...}` block.
3. Register `SelectNode` after `JoinNode` in both `types` and `executors`.

The two added registration statements are:

```rust
    types.register(SelectNode)?;
```

and:

```rust
    executors.register(SelectNode)?;
```

- [ ] **Step 6: Run focused tests and formatting**

Run:

```bash
cargo fmt --all
cargo test --test core_select --test core_output -- --nocapture
```

Expected: all `core_select` tests PASS and the registry parity test reports the sorted eight-node list.

- [ ] **Step 7: Commit Task 1**

Run:

```bash
git add src/dsl/compiled.rs src/nodes/mod.rs src/nodes/select.rs tests/core_select.rs tests/core_output.rs
git commit -m "feat: add core select node contract"
```

Expected: one commit containing only the typed node contract, executor, registry updates, and focused tests.

## Task 2: Prove Select Topology at Compile Time

**Files:**
- Create: `src/dsl/select.rs`
- Create: `tests/dsl_select.rs`
- Modify: `src/dsl/mod.rs`
- Modify: `src/dsl/compiler.rs`
- Modify: `src/dsl/graph.rs`

**Interfaces:**
- Consumes: `NodeControl::Select { sources: BTreeSet<String> }` from Task 1.
- Consumes: `ExecutionPlan::node_regions: BTreeMap<String, NodeRegion>`.
- Produces: `validate_selects(nodes: &BTreeMap<String, CompiledNode>, plan: &ExecutionPlan) -> Result<(), CompileError>`.
- Produces: stable graph errors `SELECT_SOURCE_NOT_FOUND`, `SELECT_PREDECESSOR_MISMATCH`, `SELECT_REGION_INVALID`, and `SELECT_SOURCES_NOT_EXCLUSIVE`.
- Preserves: ordinary `CompiledNode.references` and `validate_references` dominance behavior.

- [ ] **Step 1: Create the failing graph integration tests**

Create `tests/dsl_select.rs` with compiler helpers matching `tests/dsl_parallel.rs` and these fixtures/tests:

```rust
use std::{fs, path::Path, time::Duration};

use insight_agent_platform::{
    dsl::{
        compiled::{NodeControl, NodeRegion},
        compiler::{AgentCompiler, CompileLimits},
    },
    nodes::default_node_registries,
    resources::{actions::ActionRegistry, models::ModelRegistry},
};
use tempfile::TempDir;

fn compiler() -> AgentCompiler {
    let (types, _) = default_node_registries().unwrap();
    AgentCompiler::new(
        types,
        ModelRegistry::default(),
        ActionRegistry::default(),
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 8,
        },
    )
}

fn write_agent(yaml: &str) -> (TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("agent");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("agent.yaml"), yaml).unwrap();
    (temp, root)
}

fn compile(yaml: &str) -> insight_agent_platform::dsl::compiled::CompiledAgent {
    let (_temp, root) = write_agent(yaml);
    compiler().compile_dir(Path::new(&root)).unwrap()
}

fn assert_compile_error(yaml: &str, expected: &'static str) {
    let (_temp, root) = write_agent(yaml);
    let error = compiler().compile_dir(Path::new(&root)).unwrap_err();
    assert_eq!(error.code(), expected, "unexpected error: {error}");
}

fn select_yaml() -> &'static str {
    r#"
version: 1
id: select-agent
name: Select Agent
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: medical}]
      default: general
  medical:
    type: core.template
    next: selected
    config: {value: {text: medical}}
  general:
    type: core.template
    next: selected
    config: {value: {text: general}}
  selected:
    type: core.select
    next: result
    config: {sources: [medical, general]}
  result:
    type: core.output
    config:
      data:
        source: "{{ nodes.selected.output.source_node_id }}"
        text: "{{ nodes.selected.output.value.text }}"
"#
}

#[test]
fn compiles_condition_convergence_and_dominating_select_references() {
    let agent = compile(select_yaml());

    assert_eq!(
        agent.nodes["selected"].control,
        NodeControl::Select {
            sources: ["general".to_string(), "medical".to_string()]
                .into_iter()
                .collect(),
        }
    );
    assert_eq!(agent.execution_plan.node_regions["selected"], NodeRegion::Linear);
    assert_eq!(
        agent.nodes["result"].references,
        ["selected".to_string()].into_iter().collect()
    );
}

#[test]
fn compiles_multi_way_condition_convergence() {
    let agent = compile(
        r#"
version: 1
id: multi-select
name: Multi Select
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases:
        - {when: "input.kind == 'a'", next: a}
        - {when: "input.kind == 'b'", next: b}
      default: c
  a:
    type: core.template
    next: selected
    config: {value: a}
  b:
    type: core.template
    next: selected
    config: {value: b}
  c:
    type: core.template
    next: selected
    config: {value: c}
  selected:
    type: core.select
    next: result
    config: {sources: [a, b, c]}
  result:
    type: core.output
    config: {data: {value: "{{ nodes.selected.output.value }}"}}
"#,
    );

    assert_eq!(
        agent.nodes["selected"].control,
        NodeControl::Select {
            sources: ["a".to_string(), "b".to_string(), "c".to_string()]
                .into_iter()
                .collect(),
        }
    );
}

#[test]
fn select_requires_next_and_rejects_content_emit() {
    assert_compile_error(
        &select_yaml().replace("    next: result\n", ""),
        "NODE_NEXT_REQUIRED",
    );
    assert_compile_error(
        &select_yaml().replace(
            "    type: core.select\n    next: result",
            "    type: core.select\n    next: result\n    emit: content",
        ),
        "NODE_EMIT_UNSUPPORTED",
    );
}

#[test]
fn source_order_changes_the_agent_hash_without_changing_control_semantics() {
    let authored = compile(select_yaml());
    let reversed = compile(&select_yaml().replace(
        "sources: [medical, general]",
        "sources: [general, medical]",
    ));

    assert_ne!(authored.version_hash, reversed.version_hash);
    assert_eq!(authored.nodes["selected"].control, reversed.nodes["selected"].control);
}

#[test]
fn rejects_missing_and_mismatched_sources_with_select_codes() {
    assert_compile_error(
        &select_yaml().replace("[medical, general]", "[medical, missing]"),
        "SELECT_SOURCE_NOT_FOUND",
    );
    assert_compile_error(
        &select_yaml().replace("[medical, general]", "[medical, route]"),
        "SELECT_PREDECESSOR_MISMATCH",
    );
}

#[test]
fn rejects_sources_connected_by_a_path() {
    assert_compile_error(
        r#"
version: 1
id: sequential-sources
name: Sequential Sources
input:
  schema: {type: object}
entry: first
nodes:
  first:
    type: core.condition
    config:
      cases: [{when: "true", next: second}]
      default: selected
  second:
    type: core.template
    next: selected
    config: {value: second}
  selected:
    type: core.select
    next: result
    config: {sources: [first, second]}
  result:
    type: core.output
    config: {data: {value: "{{ nodes.selected.output.value }}"}}
"#,
        "SELECT_SOURCES_NOT_EXCLUSIVE",
    );
}

#[test]
fn compiles_select_inside_one_fork_branch() {
    let agent = compile(
        r#"
version: 1
id: branch-local-select
name: Branch Local Select
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {choice: route, fixed: fixed}
      join: collect
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: left}]
      default: right
  left:
    type: core.template
    next: branch_select
    config: {value: left}
  right:
    type: core.template
    next: branch_select
    config: {value: right}
  branch_select:
    type: core.select
    next: collect
    config: {sources: [left, right]}
  fixed:
    type: core.template
    next: collect
    config: {value: fixed}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config: {data: {ok: true}}
"#,
    );

    assert_eq!(
        agent.execution_plan.node_regions["branch_select"],
        NodeRegion::Branch {
            fork_id: "fanout".to_string(),
            branch_id: "choice".to_string(),
        }
    );
}

#[test]
fn existing_fork_validation_rejects_sibling_branch_convergence_first() {
    assert_compile_error(
        r#"
version: 1
id: sibling-select
name: Sibling Select
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {a: a, b: b}
      join: collect
  a:
    type: core.template
    next: selected
    config: {value: a}
  b:
    type: core.template
    next: selected
    config: {value: b}
  selected:
    type: core.select
    next: collect
    config: {sources: [a, b]}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config: {data: {ok: true}}
"#,
        "BRANCH_CROSS_REGION_EDGE",
    );
}

#[test]
fn rejects_join_and_linear_sources_with_different_regions() {
    assert_compile_error(
        r#"
version: 1
id: mixed-region-select
name: Mixed Region Select
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: fanout}]
      default: outside
  fanout:
    type: core.fork
    config:
      branches: {a: a, b: b}
      join: collect
  a:
    type: core.template
    next: collect
    config: {value: a}
  b:
    type: core.template
    next: collect
    config: {value: b}
  collect:
    type: core.join
    next: selected
    config: {mode: all_settled}
  outside:
    type: core.template
    next: selected
    config: {value: outside}
  selected:
    type: core.select
    next: result
    config: {sources: [collect, outside]}
  result:
    type: core.output
    config: {data: {ok: true}}
"#,
        "SELECT_REGION_INVALID",
    );
}

#[test]
fn downstream_nodes_cannot_bypass_select_dominance() {
    assert_compile_error(
        &select_yaml().replace(
            "nodes.selected.output.value.text",
            "nodes.medical.output.text",
        ),
        "INVALID_NODE_REFERENCE",
    );
}
```

- [ ] **Step 2: Run the focused graph tests and verify the red state**

Run:

```bash
cargo test --test dsl_select -- --nocapture
```

Expected: at least `rejects_missing_and_mismatched_sources_with_select_codes` fails because the compiler has no Select-specific topology pass.

- [ ] **Step 3: Implement the focused Select topology validator**

Create `src/dsl/select.rs` with:

```rust
use std::collections::{BTreeMap, BTreeSet};

use super::{
    compiled::{CompiledNode, ExecutionPlan, NodeControl},
    CompileError,
};

pub(crate) fn validate_selects(
    nodes: &BTreeMap<String, CompiledNode>,
    plan: &ExecutionPlan,
) -> Result<(), CompileError> {
    let predecessors = node_predecessors(nodes);

    for (select_id, node) in nodes {
        let NodeControl::Select { sources } = &node.control else {
            continue;
        };
        if sources.len() < 2 {
            return Err(CompileError::new(
                "SELECT_SOURCE_COUNT_INVALID",
                format!("select node '{select_id}' must define at least two sources"),
            ));
        }
        if sources.contains(select_id) {
            return Err(CompileError::new(
                "SELECT_SOURCE_ID_INVALID",
                format!("select node '{select_id}' cannot select itself"),
            ));
        }
        for source in sources {
            if !nodes.contains_key(source) {
                return Err(CompileError::new(
                    "SELECT_SOURCE_NOT_FOUND",
                    format!("select node '{select_id}' declares missing source '{source}'"),
                ));
            }
        }

        if &predecessors[select_id] != sources {
            return Err(CompileError::new(
                "SELECT_PREDECESSOR_MISMATCH",
                format!(
                    "select node '{select_id}' sources must exactly match its direct predecessors"
                ),
            ));
        }

        let select_region = &plan.node_regions[select_id];
        for source in sources {
            if plan.node_regions.get(source) != Some(select_region) {
                return Err(CompileError::new(
                    "SELECT_REGION_INVALID",
                    format!(
                        "select node '{select_id}' and source '{source}' must share one execution region"
                    ),
                ));
            }
        }

        let sources = sources.iter().collect::<Vec<_>>();
        for (index, left) in sources.iter().enumerate() {
            for right in sources.iter().skip(index + 1) {
                if is_reachable(left, right, nodes) || is_reachable(right, left, nodes) {
                    return Err(CompileError::new(
                        "SELECT_SOURCES_NOT_EXCLUSIVE",
                        format!(
                            "select node '{select_id}' sources '{left}' and '{right}' are connected by a path"
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}

fn node_predecessors(
    nodes: &BTreeMap<String, CompiledNode>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut predecessors = nodes
        .keys()
        .map(|node_id| (node_id.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for (node_id, node) in nodes {
        for edge in &node.edges {
            predecessors
                .get_mut(edge)
                .expect("graph edges were validated before Select validation")
                .insert(node_id.clone());
        }
    }
    predecessors
}

fn is_reachable(
    from: &str,
    target: &str,
    nodes: &BTreeMap<String, CompiledNode>,
) -> bool {
    let mut visited = BTreeSet::new();
    let mut pending = nodes[from].edges.clone();
    while let Some(node_id) = pending.pop() {
        if node_id == target {
            return true;
        }
        if visited.insert(node_id.clone()) {
            pending.extend(nodes[&node_id].edges.iter().cloned());
        }
    }
    false
}
```

- [ ] **Step 4: Insert Select validation into both compiler entry points**

In `src/dsl/mod.rs`, add:

```rust
pub(crate) mod select;
```

In `src/dsl/compiler.rs`:

1. Add `select::validate_selects` to the `use super::{...}` imports.
2. Change the validation sequence to:

```rust
        validate_graph_structure(&raw.entry, &nodes)?;
        let execution_plan = compile_execution_plan(&raw.entry, &nodes, self.limits)?;
        validate_selects(&nodes, &execution_plan)?;
        validate_references(&raw.entry, &nodes, &execution_plan)?;
```

In `src/dsl/graph.rs`, import `select::validate_selects` and change `validate_graph` to:

```rust
pub fn validate_graph(
    entry: &str,
    nodes: &BTreeMap<String, CompiledNode>,
    plan: &ExecutionPlan,
) -> Result<(), CompileError> {
    validate_graph_structure(entry, nodes)?;
    validate_selects(nodes, plan)?;
    validate_references(entry, nodes, plan)
}
```

- [ ] **Step 5: Run graph, reference, and fork regression tests**

Run:

```bash
cargo fmt --all
cargo test --test dsl_select --test dsl_compiler --test dsl_parallel -- --nocapture
```

Expected: all tests PASS; sibling branch convergence retains `BRANCH_CROSS_REGION_EDGE`, direct candidate bypass retains `INVALID_NODE_REFERENCE`, mixed Join/linear sources fail with `SELECT_REGION_INVALID`, and valid branch-local Select compiles.

- [ ] **Step 6: Commit Task 2**

Run:

```bash
git add src/dsl/mod.rs src/dsl/compiler.rs src/dsl/graph.rs src/dsl/select.rs tests/dsl_select.rs
git commit -m "feat: validate select convergence topology"
```

Expected: one commit containing the topology pass and its integration coverage.

## Task 3: Verify Scheduler, Events, Persistence, and `all_settled`

**Files:**
- Modify: `tests/run_scheduler.rs`

**Interfaces:**
- Consumes: compiled Select body `BTreeSet<String>` from Task 1.
- Consumes: `Scheduler::run(context, stop) -> Result<SchedulerResult, RunError>`.
- Produces: deterministic Condition -> alternatives -> Select -> Output scheduler fixture.
- Proves: unselected paths do not execute; ordinary Select output/events persist; main Select errors fail the Run; branch-local Select errors settle only that branch under existing `all_settled`.

- [ ] **Step 1: Add reusable Select scheduler fixtures**

In `tests/run_scheduler.rs`, add these functions after `two_branch_yaml`:

```rust
fn select_yaml() -> &'static str {
    r#"
version: 1
id: scheduler-select
name: Scheduler Select
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases: [{when: "input.kind == 'medical'", next: medical}]
      default: general
  medical:
    type: core.template
    next: selected
    config: {value: {text: medical-answer}}
  general:
    type: core.template
    next: selected
    config: {value: {text: general-answer}}
  selected:
    type: core.select
    next: result
    config: {sources: [medical, general]}
  result:
    type: core.output
    config:
      data:
        source: "{{ nodes.selected.output.source_node_id }}"
        answer: "{{ nodes.selected.output.value.text }}"
"#
}

fn branch_select_yaml() -> &'static str {
    r#"
version: 1
id: scheduler-branch-select
name: Scheduler Branch Select
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {choice: route, fixed: fixed}
      join: collect
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: left}]
      default: right
  left:
    type: core.template
    next: branch_select
    config: {value: left}
  right:
    type: core.template
    next: branch_select
    config: {value: right}
  branch_select:
    type: core.select
    next: collect
    config: {sources: [left, right]}
  fixed:
    type: core.template
    next: collect
    config: {value: fixed}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config: {data: {done: true}}
"#
}

fn context_with_input(run_id: &str, input: Value) -> RunContext {
    RunContext::new(
        RunMetadata {
            run_id: run_id.to_string(),
            request_id: format!("req_{run_id}"),
            agent_id: "scheduler-agent".to_string(),
            agent_version: "sha256:scheduler".to_string(),
            started_at: Utc::now(),
        },
        input,
    )
}
```

Replace the existing `context` body with:

```rust
fn context(run_id: &str) -> RunContext {
    context_with_input(run_id, json!({}))
}
```

- [ ] **Step 2: Add the successful path, event, and persistence test**

Add:

```rust
#[tokio::test]
async fn select_scheduler_runs_only_the_chosen_path_and_persists_stable_output() {
    for (kind, selected, unselected, answer) in [
        ("medical", "medical", "general", "medical-answer"),
        ("general", "general", "medical", "general-answer"),
    ] {
        let agent = compile_parallel_agent(select_yaml());
        let repository = Arc::new(SchedulerRepository::default());
        let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
        let (_, stop) = stop_pair();

        assert_eq!(
            scheduler
                .run(
                    context_with_input(&format!("run_select_{kind}"), json!({"kind":kind})),
                    stop,
                )
                .await
                .unwrap(),
            SchedulerResult::Completed(RunOutput {
                content: None,
                format: None,
                data: json!({"source":selected, "answer":answer}),
            })
        );

        let outputs = repository.outputs.lock().await.clone();
        assert_eq!(
            outputs
                .iter()
                .find(|output| output.node_id == "selected")
                .unwrap()
                .output,
            json!({
                "source_node_id": selected,
                "value": {"text": answer},
            })
        );
        assert!(outputs.iter().any(|output| output.node_id == selected));
        assert!(!outputs.iter().any(|output| output.node_id == unselected));

        let events = repository.events.lock().await.clone();
        assert!(events.iter().any(|event| {
            event.event_type.as_str() == "node.completed"
                && event.node_id.as_deref() == Some("selected")
        }));
        assert!(!events.iter().any(|event| {
            event.event_type.as_str() == "node.started"
                && event.node_id.as_deref() == Some(unselected)
        }));
        let operations = repository.operations.lock().await.clone();
        let output_position = operations
            .iter()
            .position(|operation| operation == "output:selected")
            .unwrap();
        let completed_position = operations
            .iter()
            .position(|operation| operation == "event:node.completed:selected")
            .unwrap();
        assert!(output_position < completed_position);
    }
}
```

- [ ] **Step 3: Add main and branch failure-classification tests**

Add:

```rust
#[tokio::test]
async fn select_missing_source_outside_a_fork_fails_the_run() {
    let mut agent = compile_parallel_agent(select_yaml());
    agent.nodes.get_mut("selected").unwrap().body =
        Arc::new(BTreeSet::from(["never_completed".to_string()]));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();

    let SchedulerResult::Failed(error) = scheduler
        .run(
            context_with_input("run_select_missing", json!({"kind":"medical"})),
            stop,
        )
        .await
        .unwrap()
    else {
        panic!("missing Select source must fail the main Run");
    };
    assert_eq!(error.code(), "SELECT_SOURCE_MISSING");
    assert!(repository.events.lock().await.iter().any(|event| {
        event.event_type.as_str() == "node.failed"
            && event.node_id.as_deref() == Some("selected")
    }));
}

#[tokio::test]
async fn select_missing_source_inside_a_fork_settles_only_that_branch() {
    let mut agent = compile_parallel_agent(branch_select_yaml());
    agent.nodes.get_mut("branch_select").unwrap().body =
        Arc::new(BTreeSet::from(["never_completed".to_string()]));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();

    assert_eq!(
        scheduler
            .run(context("run_branch_select_missing"), stop)
            .await
            .unwrap(),
        SchedulerResult::Completed(RunOutput {
            content: None,
            format: None,
            data: json!({"done":true}),
        })
    );
    assert_eq!(
        repository
            .outputs
            .lock()
            .await
            .iter()
            .find(|output| output.node_id == "collect")
            .unwrap()
            .output["summary"],
        json!({"total":2, "succeeded":1, "failed":1})
    );
    let events = repository.events.lock().await.clone();
    assert!(events.iter().any(|event| {
        event.event_type.as_str() == "node.failed"
            && event.node_id.as_deref() == Some("branch_select")
            && event.code == "SELECT_SOURCE_MISSING"
    }));
    assert!(events.iter().any(|event| {
        event.event_type.as_str() == "branch.failed"
            && event.data["branch_id"] == "choice"
    }));
}
```

- [ ] **Step 4: Run the focused scheduler tests**

Run:

```bash
cargo fmt --all
cargo test --test run_scheduler select_ -- --nocapture
```

Expected: all Select scheduler tests PASS. The branch-local failure completes the Run with one succeeded and one failed branch, proving no `all_settled` change is required.

- [ ] **Step 5: Commit Task 3**

Run:

```bash
git add tests/run_scheduler.rs
git commit -m "test: cover select scheduler semantics"
```

Expected: one test-only commit for scheduler, event, persistence, and branch-settlement behavior.

## Task 4: Cover Built-in Consumers, Observability, and Documentation

**Files:**
- Modify: `tests/formal_agent_compile.rs`
- Modify: `tests/observability.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: stable Select output paths `nodes.<select_id>.output.source_node_id` and `nodes.<select_id>.output.value`.
- Produces: compile coverage for Template, Action, Chat, and Output consumers.
- Completes the propagation proof together with Task 3: Task 3 proves the scheduler stores Select output in ordinary path context and Output renders it; this task proves the unchanged Template, Action, and Chat compilers accept the same dominating context paths, while their existing executor suites continue to cover ordinary context reads.
- Produces: body-free INFO-log coverage using a selected secret value.
- Produces: complete README DSL example and explicit Select-vs-Join guidance.

- [ ] **Step 1: Add built-in consumer compilation coverage**

Append this test to `tests/formal_agent_compile.rs`:

```rust
#[test]
fn select_output_compiles_for_all_builtin_consumers() {
    let directory = tempdir().unwrap();
    std::fs::write(
        directory.path().join("agent.yaml"),
        r#"
version: 1
id: select-consumers
name: Select Consumers
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: medical}]
      default: general
  medical:
    type: core.template
    next: selected
    config: {value: {text: medical}}
  general:
    type: core.template
    next: selected
    config: {value: {text: general}}
  selected:
    type: core.select
    next: render
    config: {sources: [medical, general]}
  render:
    type: core.template
    next: classify
    config:
      value: "{{ nodes.selected.output.value.text }}"
  classify:
    type: core.action
    next: answer
    config:
      action: classify
      input:
        text: "{{ nodes.selected.output.value.text }}"
  answer:
    type: core.chat
    next: result
    config:
      model: primary
      messages:
        - role: user
          content: "{{ nodes.selected.output.value.text }}"
      parameters: {}
  result:
    type: core.output
    config:
      data:
        source: "{{ nodes.selected.output.source_node_id }}"
        rendered: "{{ nodes.render.output }}"
        kind: "{{ nodes.classify.output.kind }}"
        answer: "{{ nodes.answer.output.text }}"
"#,
    )
    .unwrap();

    let mut models = ModelRegistry::default();
    models.register("primary", FakeModel).unwrap();
    let mut actions = ActionRegistry::default();
    actions.register(ClassifyAction).unwrap();
    let (types, _) = default_node_registries().unwrap();
    let compiler = AgentCompiler::new(
        types,
        models,
        actions,
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 8,
        },
    );

    let agent = compiler.compile_dir(directory.path()).unwrap();
    assert_eq!(
        agent.nodes["render"].references,
        ["selected".to_string()].into_iter().collect()
    );
    assert_eq!(
        agent.nodes["classify"].references,
        ["selected".to_string()].into_iter().collect()
    );
    assert_eq!(
        agent.nodes["answer"].references,
        ["selected".to_string()].into_iter().collect()
    );
}
```

- [ ] **Step 2: Add Select log-redaction coverage**

In `tests/observability.rs`:

1. Add `const SELECT_SECRET: &str = "observability-select-secret";` with the other secrets.
2. In `fixture`, add this deterministic test Agent construction:

```rust
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
    type: core.output
    config:
      data: {value: "{{ nodes.selected.output.value }}"}
"#,
    );
```

3. Add this test after the linear-success log test:

```rust
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
    assert!(selected
        .field("output_bytes")
        .unwrap()
        .parse::<usize>()
        .unwrap()
        > 0);
    assert_logs_exclude(&[SELECT_SECRET]);
}
```

- [ ] **Step 3: Update README built-in-node and convergence documentation**

In `README.md`:

1. Change `正式 V1 内置五种节点` to `正式 V1 内置八种节点`.
2. Add these rows to the built-in-node table:

```markdown
| `core.fork` | 显式启动固定并行分支 |
| `core.join` | 以 `all_settled` 汇合 fork 分支并输出稳定汇总 |
| `core.select` | 将互斥条件路径中唯一已执行的结果汇合为稳定输出 |
```

3. After the first complete Agent DSL example, add this subsection and complete example:

````markdown
### 条件结果汇合

```yaml
version: 1
id: select_demo
name: Select Demo
input:
  schema:
    type: object
    additionalProperties: false
    required: [kind]
    properties:
      kind:
        type: string

entry: route
nodes:
  route:
    type: core.condition
    config:
      cases:
        - when: "input.kind == 'medical'"
          next: medical
      default: general

  medical:
    type: core.template
    next: selected_answer
    config:
      value:
        kind: medical
        text: "medical answer"

  general:
    type: core.template
    next: selected_answer
    config:
      value:
        kind: general
        text: "general answer"

  selected_answer:
    type: core.select
    next: result
    config:
      sources: [medical, general]

  result:
    type: core.output
    config:
      data:
        source: "{{ nodes.selected_answer.output.source_node_id }}"
        answer: "{{ nodes.selected_answer.output.value.text }}"
```
````
4. Follow that example with this exact contract summary:

```markdown
`core.select` 只用于互斥路径的一选一汇合。`sources` 必须完整列出 Select 的直接前驱，并且这些前驱必须处于同一执行区域且彼此不可达。运行时恰好一个来源可见才成功；已执行节点返回的 JSON `null` 仍是有效值，未执行来源不会被自动补 `null`。下游统一引用 `nodes.selected_answer.output.source_node_id` 和 `nodes.selected_answer.output.value`，不能绕过 Select 直接引用某个条件分支节点。

Select 与 Join 的职责不同：Condition 只选择一条路径，因此使用 `core.select`；Fork 会执行全部固定分支，因此使用 `core.join` 和显式 `mode: all_settled`。Select 不做数组拼接、对象合并、优先级回退或并行聚合。
```

5. In the `agents/parallel_researcher` section, keep the existing Join example and add one sentence before it: `条件路径结果使用 core.select；以下示例是并行分支，因此继续使用 core.fork/core.join。`

- [ ] **Step 4: Run consumer and observability tests**

Run:

```bash
cargo fmt --all
cargo test --test formal_agent_compile select_output_compiles_for_all_builtin_consumers -- --exact --nocapture
cargo test --test observability select_info_logs_record_metadata_without_selected_bodies -- --exact --nocapture
```

Expected: both focused tests PASS; the observability test finds one body-free `core.select` completion log.

- [ ] **Step 5: Commit Task 4**

Run:

```bash
git add README.md tests/formal_agent_compile.rs tests/observability.rs
git commit -m "docs: document select convergence contract"
```

Expected: one commit containing consumer compatibility, log safety, and user-facing DSL documentation.

## Task 5: Run the Complete Verification Gate

**Files:**
- Verify only; no planned file changes.

**Interfaces:**
- Consumes: all Task 1-4 commits.
- Produces: fresh repository-wide evidence for formatting, linting, tests, dependency audit, and policy checks.

- [ ] **Step 1: Run formatting in check mode**

Run:

```bash
cargo fmt --all -- --check
```

Expected: exit 0 with no formatting diff.

- [ ] **Step 2: Run Clippy with warnings denied**

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: exit 0 with no warning.

- [ ] **Step 3: Run the locked full test suite**

Run:

```bash
cargo test --locked --all-targets
```

Expected: exit 0 with zero failed tests, including all existing fork/join, reference, scheduler, repository, and Agent fixtures.

- [ ] **Step 4: Run dependency security and policy checks**

Run:

```bash
cargo audit
cargo deny check
```

Expected: both commands exit 0.

- [ ] **Step 5: Verify the final repository state**

Run:

```bash
git status --short
git log -4 --oneline
```

Expected: clean status and four feature commits for node contract, compiler topology, scheduler coverage, and documentation/observability. Do not claim completion until every command above has fresh successful output.

## Deferred Follow-on Work

After this milestone is merged and verified, start a separate design for explicit workflow failure and `all_settled` consumption:

1. add a built-in `core.fail` node;
2. route a post-Join `core.condition` using `nodes.<join>.output.summary.succeeded`;
3. define deliberate failure or degraded output when no parallel branch succeeds.

Named Condition case IDs and production concurrency verification remain separate later milestones.
