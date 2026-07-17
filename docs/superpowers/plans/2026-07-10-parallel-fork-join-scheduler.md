# Parallel Fork/Join Scheduler Implementation Plan

> **Historical / superseded:** authored Fork/Join nodes and the flat scheduler were removed. See [DSL vNext Region/SSA Design](../specs/2026-07-16-dsl-vnext-region-ssa-design.md).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded, fixed parallel branch execution through explicit `core.fork` and `core.join` nodes, with multi-node branch subgraphs, durable `all_settled` results, and unchanged sequential-Agent behavior.

**Architecture:** Extend compilation with immutable fork-region metadata and region-aware reference validation, then replace the coordinator's single cursor with a single-owner ready-queue scheduler. Node futures execute concurrently behind process-wide and per-Run semaphores, while only the scheduler mutates node/branch state; branch-local failures become join data and infrastructure or Run-stop failures remain globally terminal.

**Tech Stack:** Rust 1.94.1, Tokio `JoinSet`/`Semaphore`/`Notify`, serde/serde_yaml, serde_json, Handlebars 6, CEL, Axum SSE, sqlx SQLite/PostgreSQL, existing `EventHub` journal

## Global Constraints

- The approved design is `docs/superpowers/specs/2026-07-10-parallel-fork-join-scheduler-design.md`.
- DSL and event schema versions remain `1`; HTTP `/v1`, Run creation, cancellation, lookup, and SSE routes do not change.
- The only new control nodes are `core.fork` and `core.join`; join mode is exactly `all_settled`.
- A fork has at least 2 and at most `runtime.max_fork_branches: 32` fixed named branches.
- Nested fork regions, dynamic `foreach`, cycles, wait/HITL, subflows, restart resume, multi-tenancy, and additional join policies remain out of scope.
- Branch regions are vertex-disjoint, every non-failing branch path reaches its paired join, and only paired branch regions may enter that join.
- Post-join nodes consume branch work through the join output and cannot directly reference internal branch nodes.
- `runtime.max_parallel_node_executions: 32` is process-wide and `runtime.max_parallel_branches_per_run: 8` bounds one Run.
- Branch-local executor errors and node timeouts settle only that branch; Run cancellation, whole-Run timeout, journal/repository failure, invariant failure, and task panic are globally fatal.
- The coordinator remains the only owner of durable Run terminal state, and `EventHub` remains the only allocator of event `seq`.
- Process restart still marks incomplete Runs `interrupted`; active scheduler state is not restored.
- Existing sequential Agent YAML and output behavior remain unchanged.
- Every behavior change starts with a failing test, uses deterministic synchronization rather than timing guesses, and ends with focused verification and a commit.

## Target File Map

```text
src/dsl/
  compiled.rs       Compiled control metadata, ExecutionPlan data types
  compiler.rs       Node compilation plus plan construction call
  graph.rs          Structural, region, path, and reference validation
  plan.rs           Immutable fork/join region compiler
  mod.rs            Plan exports
src/nodes/
  fork.rs           core.fork config compiler and activation directive
  join.rs           core.join config compiler and all_settled envelope
  mod.rs             Built-in registration
  registry.rs        Existing static extension boundary
src/runtime/
  context.rs         Frozen base plus branch-local output layers
  state.rs           NodeState, BranchState, BranchResult, scheduler state
  execution.rs       One node lifecycle, permits, timeout, persistence barrier
  scheduler.rs       Ready queue, branch settlement, join barrier
  coordinator.rs     Run lifecycle and unique terminal ownership
  service.rs         Process-wide node semaphore and per-Run limits
  mod.rs             Runtime exports and classified RunError
src/events/protocol.rs
src/config.rs
src/catalog.rs
src/main.rs
config/platform.yaml
agents/parallel_researcher/
tests/dsl_parallel.rs
tests/fork_join_nodes.rs
tests/run_scheduler.rs
tests/formal_protocol.rs
tests/event_hub.rs
tests/history_sqlite_v1.rs
tests/history_postgres.rs
tests/platform_config_v1.rs
tests/formal_agent_compile.rs
tests/api.rs
README.md
```

No migration is added: branch lifecycle is represented by existing durable events and node outputs, not a new table.

---

### Task 1: Add strict compilation and scheduler limits

**Files:**
- Modify: `src/config.rs`
- Modify: `src/dsl/compiler.rs`
- Modify: `src/catalog.rs`
- Modify: `src/main.rs`
- Modify: `src/runtime/service.rs`
- Modify: `config/platform.yaml`
- Modify: `README.md`
- Modify: `tests/platform_config_v1.rs`
- Modify: `tests/dsl_compiler.rs`
- Modify: `tests/core_output.rs`
- Modify: `tests/core_template_condition.rs`
- Modify: `tests/formal_agent_compile.rs`
- Modify: `tests/repository_agents_v1.rs`
- Modify: `tests/api.rs`
- Modify: `tests/run_service.rs`

**Interfaces:**
- Consumes: strict `runtime` YAML and existing `AgentCompiler`/`RunService` construction
- Produces: `CompileLimits { max_fork_branches }` and positive scheduler capacities available to later tasks

- [ ] **Step 1: Write failing configuration tests**

Extend `base_yaml` in `tests/platform_config_v1.rs` with:

```yaml
  max_fork_branches: 32
  max_parallel_node_executions: 32
  max_parallel_branches_per_run: 8
```

Assert the resolved values and add all three zero-value cases to `zero_capacities_and_durations_are_rejected`:

```rust
assert_eq!(config.runtime.max_fork_branches, 32);
assert_eq!(config.runtime.max_parallel_node_executions, 32);
assert_eq!(config.runtime.max_parallel_branches_per_run, 8);

for (from, to) in [
    ("max_fork_branches: 32", "max_fork_branches: 0"),
    (
        "max_parallel_node_executions: 32",
        "max_parallel_node_executions: 0",
    ),
    (
        "max_parallel_branches_per_run: 8",
        "max_parallel_branches_per_run: 0",
    ),
] {
    let yaml = base_yaml("  mode: disabled").replace(from, to);
    let (_directory, path) = write_config(&yaml);
    assert_eq!(
        load(&path, BTreeMap::new()).unwrap_err().code(),
        "PLATFORM_RUNTIME_INVALID"
    );
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `cargo test --test platform_config_v1 -- --nocapture`

Expected: FAIL because `RuntimeYaml` rejects the three unknown fields and `RuntimeConfig` has no matching members.

- [ ] **Step 3: Add exact configuration and compiler limit types**

Add to `src/dsl/compiler.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompileLimits {
    pub max_fork_branches: usize,
}
```

Add these fields to both `RuntimeConfig` and `RuntimeYaml` in `src/config.rs`:

```rust
pub max_fork_branches: usize,
pub max_parallel_node_executions: usize,
pub max_parallel_branches_per_run: usize,
```

Include all three values in the positive-capacity array and copy them into the resolved `RuntimeConfig`. Add `limits: CompileLimits` to `AgentCompiler`, extend `AgentCompiler::new` with a final `limits` argument, and expose:

```rust
pub fn limits(&self) -> CompileLimits {
    self.limits
}
```

Extend `compile_enabled_agents` with `limits: CompileLimits`, pass it from `main`, and add these members to `RunServiceConfig`:

```rust
pub max_parallel_node_executions: usize,
pub max_parallel_branches_per_run: usize,
```

Validate both as nonzero in `RunService::new`. At every test compiler call, use:

```rust
CompileLimits {
    max_fork_branches: 32,
}
```

At every test `RunServiceConfig`, use:

```rust
max_parallel_node_executions: 32,
max_parallel_branches_per_run: 8,
```

Update both configuration examples with the approved values.

- [ ] **Step 4: Verify config and constructor plumbing**

Run:

```bash
cargo test --test platform_config_v1 --test dsl_compiler --test run_service
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS; no production behavior uses the new limits yet.

- [ ] **Step 5: Commit strict limits**

```bash
git add src/config.rs src/dsl/compiler.rs src/catalog.rs src/main.rs src/runtime/service.rs config/platform.yaml README.md tests/platform_config_v1.rs tests/dsl_compiler.rs tests/core_output.rs tests/core_template_condition.rs tests/formal_agent_compile.rs tests/repository_agents_v1.rs tests/api.rs tests/run_service.rs
git commit -m "feat: configure bounded parallel execution"
```

### Task 2: Introduce typed compiled and runtime control directives

**Files:**
- Modify: `src/dsl/compiled.rs`
- Modify: `src/dsl/compiler.rs`
- Modify: `src/nodes/action.rs`
- Modify: `src/nodes/chat.rs`
- Modify: `src/nodes/condition.rs`
- Modify: `src/nodes/output.rs`
- Modify: `src/nodes/template.rs`
- Modify: `tests/node_extensions.rs`
- Modify: `tests/dsl_compiler.rs`
- Modify: `tests/core_template_condition.rs`
- Modify: `tests/run_coordinator.rs`
- Modify: `tests/run_service.rs`
- Modify: `tests/api.rs`

**Interfaces:**
- Consumes: existing node compilation and `NodeOutcome`
- Produces: `NodeControl`, `JoinPolicy`, and `NodeTransition::ActivateFork` without scheduler string matching

- [ ] **Step 1: Write failing typed-control assertions**

In `tests/node_extensions.rs`, assert ordinary extensions compile as ordinary control nodes and add a runtime transition equality assertion:

```rust
assert_eq!(compilation.control, NodeControl::Ordinary);
assert_eq!(NodeTransition::ActivateFork, NodeTransition::ActivateFork);
```

Import `NodeControl` and keep the existing extension test otherwise unchanged.

- [ ] **Step 2: Run the extension test and verify it fails to compile**

Run: `cargo test --test node_extensions -- --nocapture`

Expected: FAIL because `NodeControl`, `NodeCompilation::control`, and `NodeTransition::ActivateFork` do not exist.

- [ ] **Step 3: Define exact control contracts**

Add to `src/dsl/compiled.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinPolicy {
    AllSettled,
}

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
}
```

Add `pub control: NodeControl` to `NodeCompilation` and `CompiledNode`. Add this variant without changing existing transition meanings:

```rust
pub enum NodeTransition {
    Next,
    Goto(String),
    ActivateFork,
    Complete(RunOutput),
}
```

Copy `compilation.control` into every `CompiledNode` in `AgentCompiler`. Set `control: NodeControl::Ordinary` in all five current core node compilers and all synthetic/custom `NodeCompilation` or `CompiledNode` literals in the listed tests.

- [ ] **Step 4: Verify typed controls preserve existing nodes**

Run:

```bash
cargo test --test node_extensions --test core_template_condition --test run_coordinator
cargo test --test dsl_compiler --test run_service --test api
```

Expected: PASS; current Agents still emit only `Next`, `Goto`, and `Complete`.

- [ ] **Step 5: Commit the typed control boundary**

```bash
git add src/dsl/compiled.rs src/dsl/compiler.rs src/nodes tests/node_extensions.rs tests/dsl_compiler.rs tests/core_template_condition.rs tests/run_coordinator.rs tests/run_service.rs tests/api.rs
git commit -m "refactor: add typed node scheduling directives"
```

### Task 3: Add `core.fork`, `core.join`, and the stable join envelope

**Files:**
- Create: `src/nodes/fork.rs`
- Create: `src/nodes/join.rs`
- Create: `tests/fork_join_nodes.rs`
- Modify: `src/nodes/mod.rs`
- Modify: `src/runtime/context.rs`
- Modify: `src/runtime/state.rs`
- Modify: `src/runtime/mod.rs`

**Interfaces:**
- Consumes: `NodeControl`, `NodeTransition`, strict JSON node config, and `RunContext`
- Produces: registered `core.fork`/`core.join`, `BranchResult`, `BranchError`, and deterministic join JSON

- [ ] **Step 1: Write failing core-node contract tests**

Create `tests/fork_join_nodes.rs` with compiler-level tests for exact envelope rules and an executor-level join assertion:

```rust
#[test]
fn fork_and_join_compile_to_typed_controls() {
    let (types, _) = default_node_registries().unwrap();
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);

    let fork = types.resolve("core.fork").unwrap()
        .compile(
            "fanout",
            json!({
                "branches": {"source_b": "search_b", "source_a": "search_a"},
                "join": "collect"
            }),
            &mut context,
        ).unwrap();
    assert_eq!(fork.envelope.next, NextPolicy::Forbidden);
    assert!(!fork.envelope.allows_content_emit);
    assert_eq!(fork.edges, vec!["search_a", "search_b"]);
    assert_eq!(
        fork.control,
        NodeControl::Fork {
            branches: BTreeMap::from([
                ("source_a".into(), "search_a".into()),
                ("source_b".into(), "search_b".into()),
            ]),
            join: "collect".into(),
        }
    );

    let join = types.resolve("core.join").unwrap()
        .compile("collect", json!({"mode":"all_settled"}), &mut context)
        .unwrap();
    assert_eq!(join.envelope.next, NextPolicy::Required);
    assert_eq!(join.control, NodeControl::Join { policy: JoinPolicy::AllSettled });
}
```

Add async execution coverage whose `RunContext` contains:

```rust
let results = BTreeMap::from([
    (
        "source_a".to_string(),
        BranchResult::Succeeded {
            terminal_node_id: "summarize_a".to_string(),
            output: json!({"text":"result a"}),
        },
    ),
    (
        "source_b".to_string(),
        BranchResult::Failed {
            terminal_node_id: "search_b".to_string(),
            error: BranchError {
                code: "UPSTREAM_FAILURE".to_string(),
                message: "upstream service failed".to_string(),
            },
        },
    ),
]);
```

Assert `NodeTransition::Next` and the exact `branches` plus `summary: {total: 2, succeeded: 1, failed: 1}` JSON from the approved design. Add an all-failed input and assert the join still returns `Next` with `summary: {total: 2, succeeded: 0, failed: 2}`. Add negative cases for one branch, branch IDs outside `[A-Za-z_][A-Za-z0-9_-]*`, empty targets, missing join, unknown join mode, and unknown config fields.

Add a context isolation test:

```rust
let mut main = test_context();
main.set_node_output("prepare", json!({"query":"rust"}));
let mut source_a = main.fork_branch();
let mut source_b = main.fork_branch();
source_a.set_node_output("search_a", json!({"text":"a"}));
source_b.set_node_output("search_b", json!({"text":"b"}));
assert!(source_a.node_output("search_b").is_none());
assert!(source_b.node_output("search_a").is_none());
assert_eq!(source_a.node_output("prepare"), Some(&json!({"query":"rust"})));
```

- [ ] **Step 2: Run the node test and verify it fails**

Run: `cargo test --test fork_join_nodes -- --nocapture`

Expected: FAIL because both core node types and branch result contracts are absent.

- [ ] **Step 3: Implement branch result and layered context contracts**

Add to `src/runtime/state.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BranchError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BranchResult {
    Succeeded {
        terminal_node_id: String,
        output: Value,
    },
    Failed {
        terminal_node_id: String,
        error: BranchError,
    },
}
```

Refactor `RunContext` to keep `base_node_outputs: Arc<BTreeMap<String, Value>>`, `local_node_outputs: BTreeMap<String, Value>`, and `join_results: Option<Arc<BTreeMap<String, BranchResult>>>`. Preserve existing `node_output`, `set_node_output`, and template roots, and add:

```rust
pub fn fork_branch(&self) -> Self;
pub fn with_join_results(&self, results: BTreeMap<String, BranchResult>) -> Self;
pub fn branch_results(&self) -> Option<&BTreeMap<String, BranchResult>>;
```

`fork_branch` freezes the currently visible base plus local outputs into a new shared base and starts with an empty local map. `with_join_results` does the same and attaches immutable join results. Neither method exposes sibling-local outputs.

- [ ] **Step 4: Implement and register both control nodes**

`ForkNode::compile` must deserialize:

```rust
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForkConfig {
    branches: BTreeMap<String, String>,
    join: String,
}
```

It validates at least two entries, validates every branch ID against `[A-Za-z_][A-Za-z0-9_-]*`, rejects blank targets and a blank join, produces sorted entry edges, `NextPolicy::Forbidden`, no content emit, and `NodeControl::Fork`. Its executor returns:

```rust
NodeOutcome {
    output: json!({"branches": body.branches.keys().collect::<Vec<_>>(), "join": body.join}),
    transition: NodeTransition::ActivateFork,
}
```

`JoinNode::compile` accepts only `{"mode":"all_settled"}`, requires envelope `next`, and produces `NodeControl::Join`. Its executor requires `context.branch_results()`, serializes the `BTreeMap` directly, counts each variant, and returns `NodeTransition::Next`. Missing scheduler results return `RunError::new("JOIN_RESULTS_MISSING", "join node requires settled branch results")`.

Register both nodes in both default registries and export `BranchError`/`BranchResult` from `runtime/mod.rs`.

- [ ] **Step 5: Verify node contracts and context isolation**

Run:

```bash
cargo test --test fork_join_nodes --test node_extensions
cargo test --test core_template_condition --test core_output
```

Expected: PASS, including deterministic branch-key ordering and all-failed join success serialization.

- [ ] **Step 6: Commit the new control nodes**

```bash
git add src/nodes/fork.rs src/nodes/join.rs src/nodes/mod.rs src/runtime/context.rs src/runtime/state.rs src/runtime/mod.rs tests/fork_join_nodes.rs
git commit -m "feat: add fork and all-settled join nodes"
```

### Task 4: Compile immutable fork regions and reject malformed topology

**Files:**
- Create: `src/dsl/plan.rs`
- Create: `tests/dsl_parallel.rs`
- Modify: `src/dsl/compiled.rs`
- Modify: `src/dsl/compiler.rs`
- Modify: `src/dsl/graph.rs`
- Modify: `src/dsl/mod.rs`
- Modify: `tests/dsl_compiler.rs`
- Modify: `tests/formal_agent_compile.rs`
- Modify: `tests/run_coordinator.rs`
- Modify: `tests/run_service.rs`
- Modify: `tests/api.rs`

**Interfaces:**
- Consumes: fully compiled node map, typed `NodeControl`, `CompileLimits`
- Produces: immutable `ExecutionPlan`, exact fork/join pairing, and `NodeRegion` lookup

- [ ] **Step 1: Write a valid multi-node parallel DSL test**

Create `tests/dsl_parallel.rs` with a temp Agent containing `prepare -> fanout`, two named branches, a condition in one branch, paired `collect`, and `collect -> result`. Assert:

```rust
let fork = &agent.execution_plan.forks["fanout"];
assert_eq!(fork.join_id, "collect");
assert_eq!(fork.policy, JoinPolicy::AllSettled);
assert_eq!(fork.branches["source_a"].entry, "search_a");
assert_eq!(
    fork.branches["source_a"].nodes,
    BTreeSet::from(["search_a".to_string(), "summarize_a".to_string()])
);
assert_eq!(
    agent.execution_plan.node_regions["search_b"],
    NodeRegion::Branch {
        fork_id: "fanout".to_string(),
        branch_id: "source_b".to_string(),
    }
);
assert_eq!(
    agent.execution_plan.node_regions["collect"],
    NodeRegion::Join { fork_id: "fanout".to_string() }
);
```

Also assert a current sequential fixture has an empty `forks` map and every node is `NodeRegion::Linear`.

- [ ] **Step 2: Run the DSL test and verify it fails**

Run: `cargo test --test dsl_parallel -- --nocapture`

Expected: FAIL because `ExecutionPlan` and fork-region compilation do not exist.

- [ ] **Step 3: Define the immutable plan types**

Add to `src/dsl/compiled.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRegion {
    Linear,
    Branch { fork_id: String, branch_id: String },
    Join { fork_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchPlan {
    pub branch_id: String,
    pub entry: String,
    pub nodes: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkPlan {
    pub fork_id: String,
    pub join_id: String,
    pub branches: BTreeMap<String, BranchPlan>,
    pub policy: JoinPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPlan {
    pub entry: String,
    pub forks: BTreeMap<String, ForkPlan>,
    pub node_regions: BTreeMap<String, NodeRegion>,
}
```

Add `pub execution_plan: ExecutionPlan` to `CompiledAgent` and include fork IDs in its `Debug` output. Provide this test-builder-safe constructor:

```rust
impl ExecutionPlan {
    pub fn sequential(
        entry: impl Into<String>,
        node_ids: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            entry: entry.into(),
            forks: BTreeMap::new(),
            node_regions: node_ids
                .into_iter()
                .map(|node_id| (node_id, NodeRegion::Linear))
                .collect(),
        }
    }
}
```

Update every direct `CompiledAgent` builder in `tests/run_coordinator.rs`, `tests/run_service.rs`, and `tests/api.rs` to build its node map first and set `execution_plan: ExecutionPlan::sequential(entry, nodes.keys().cloned())`. This keeps every intermediate commit all-targets compilable.

- [ ] **Step 4: Build plan topology with exact rejection codes**

Implement `compile_execution_plan(entry, nodes, limits)` in `src/dsl/plan.rs`. Walk each declared branch until its paired join, stopping at that join and collecting every visited node. Reject these conditions with stable codes:

```text
FORK_BRANCH_COUNT_INVALID
FORK_BRANCH_LIMIT_EXCEEDED
FORK_JOIN_NOT_FOUND
FORK_JOIN_KIND_INVALID
JOIN_PAIRING_INVALID
BRANCH_PATH_MISSING_JOIN
BRANCH_REGION_OVERLAP
BRANCH_CROSS_REGION_EDGE
BRANCH_NESTED_FORK
JOIN_PREDECESSOR_INVALID
```

The walk treats every edge of a condition as a required successful path. It permits another fork only after the current join, so sequential fork regions remain valid. Initialize unclaimed nodes as `Linear`, branch nodes as `Branch`, and paired joins as `Join`.

Split current `validate_graph` so missing edges, cycles, reachability, and terminal-output rules run before plan construction, while reference validation runs after it. Build the plan in `AgentCompiler` and store it in `CompiledAgent`.

- [ ] **Step 5: Add malformed-region tests and make them pass**

Use string mutations or explicit fixtures to assert each code for: wrong-kind join, absent join, branch escaping to output, overlapping entries, one branch entering its sibling, direct fork-to-join bypass, nested fork, a condition path bypassing join, an outside predecessor entering join, one join claimed by two forks, and 33 branches with a limit of 32.

Run:

```bash
cargo test --test dsl_parallel -- --nocapture
cargo test --test dsl_compiler --test formal_agent_compile
```

Expected: PASS; sequential compilation remains unchanged.

- [ ] **Step 6: Commit immutable plan compilation**

```bash
git add src/dsl/plan.rs src/dsl/compiled.rs src/dsl/compiler.rs src/dsl/graph.rs src/dsl/mod.rs tests/dsl_parallel.rs tests/dsl_compiler.rs tests/formal_agent_compile.rs tests/run_coordinator.rs tests/run_service.rs tests/api.rs
git commit -m "feat: compile validated fork join regions"
```

### Task 5: Enforce region-aware node-output references

**Files:**
- Modify: `src/dsl/graph.rs`
- Modify: `src/dsl/compiler.rs`
- Modify: `tests/dsl_parallel.rs`
- Modify: `tests/dsl_compiler.rs`

**Interfaces:**
- Consumes: dominators plus `ExecutionPlan::node_regions`
- Produces: pre-fork and same-branch references only, with join-mediated post-branch access

- [ ] **Step 1: Add failing reference-boundary tests**

Add four tests to `tests/dsl_parallel.rs`:

```rust
assert_compile_ok(valid_parallel_yaml_with(
    "summarize_a",
    "{{ nodes.prepare.output.query }}",
));
assert_compile_ok(valid_parallel_yaml_with(
    "summarize_a",
    "{{ nodes.search_a.output.text }}",
));
assert_compile_error(
    valid_parallel_yaml_with("summarize_a", "{{ nodes.search_b.output.text }}"),
    "CROSS_BRANCH_REFERENCE",
);
assert_compile_error(
    valid_parallel_yaml_with("result", "{{ nodes.summarize_a.output.text }}"),
    "POST_JOIN_BRANCH_REFERENCE",
);
```

Add a successful post-join reference to `{{ nodes.collect.output.branches.source_a.output.text }}` and retain existing dominating-predecessor tests.

- [ ] **Step 2: Run the reference tests and verify precise codes fail**

Run: `cargo test --test dsl_parallel reference -- --nocapture`

Expected: FAIL because the generic dominator validator reports `INVALID_NODE_REFERENCE` instead of explicit region violations.

- [ ] **Step 3: Make reference validation plan-aware**

Change the validator signature to:

```rust
pub fn validate_references(
    entry: &str,
    nodes: &BTreeMap<String, CompiledNode>,
    plan: &ExecutionPlan,
) -> Result<(), CompileError>;
```

Before the existing dominance check, classify each source and target region:

```rust
match (&plan.node_regions[node_id], &plan.node_regions[reference]) {
    (
        NodeRegion::Branch { fork_id, branch_id },
        NodeRegion::Branch { fork_id: other_fork, branch_id: other_branch },
    ) if fork_id != other_fork || branch_id != other_branch => {
        return Err(CompileError::new("CROSS_BRANCH_REFERENCE", message));
    }
    (NodeRegion::Linear | NodeRegion::Join { .. }, NodeRegion::Branch { .. }) => {
        return Err(CompileError::new("POST_JOIN_BRANCH_REFERENCE", message));
    }
    _ => {}
}
```

Then apply the current self/missing/dominator rule. This keeps pre-fork dominators valid inside a branch and rejects references that are not guaranteed on every condition path.

- [ ] **Step 4: Verify reference and sequential graph behavior**

Run:

```bash
cargo test --test dsl_parallel --test dsl_compiler
cargo test --test core_template_condition --test core_output
```

Expected: PASS with exact region error codes and no sequential regression.

- [ ] **Step 5: Commit region-aware references**

```bash
git add src/dsl/graph.rs src/dsl/compiler.rs tests/dsl_parallel.rs tests/dsl_compiler.rs
git commit -m "feat: enforce fork region reference boundaries"
```

### Task 6: Add branch lifecycle events to the durable protocol

**Files:**
- Modify: `src/events/protocol.rs`
- Modify: `tests/formal_protocol.rs`
- Modify: `tests/event_hub.rs`
- Modify: `tests/history_sqlite_v1.rs`
- Modify: `tests/history_postgres.rs`

**Interfaces:**
- Consumes: existing event envelope, journal, replay, and repository event strings
- Produces: `branch.started`, `branch.completed`, and `branch.failed` as run-level durable events

- [ ] **Step 1: Write failing protocol and replay tests**

Extend the exact event-type test with:

```rust
RunEventType::BranchStarted,
RunEventType::BranchCompleted,
RunEventType::BranchFailed,
```

and expected strings:

```rust
json!("branch.started"),
json!("branch.completed"),
json!("branch.failed"),
```

Add a branch envelope assertion:

```rust
let event = RunEvent::ok_at(
    RunEventType::BranchCompleted,
    6,
    scope(Some("must_be_ignored")),
    at(6),
    json!({
        "fork_id":"fanout",
        "branch_id":"source_a",
        "terminal_node_id":"summarize_a"
    }),
);
let value = serde_json::to_value(event).unwrap();
assert!(value.get("node_id").is_none());
```

Publish the three branch event types through `EventHub`, replay after the first sequence, and assert unique contiguous sequences. Add one SQLite and one PostgreSQL repository round-trip containing `BranchFailed`.

- [ ] **Step 2: Run focused tests and verify they fail**

Run: `cargo test --test formal_protocol --test event_hub --test history_sqlite_v1 -- --nocapture`

Expected: FAIL because the three variants cannot be constructed or parsed.

- [ ] **Step 3: Extend the event protocol without changing schema version**

Add the three enum variants, serialization names, `as_str`, and `parse` arms. Replace `is_run_scoped` with:

```rust
pub fn is_node_scoped(self) -> bool {
    matches!(
        self,
        Self::NodeStarted | Self::ContentDelta | Self::NodeCompleted | Self::NodeFailed
    )
}
```

In `RunEvent::new`, retain `scope.node_id` only when `event_type.is_node_scoped()`; branch and Run events therefore omit it. Keep `EVENT_SCHEMA_VERSION` equal to `1`.

- [ ] **Step 4: Verify durable branch event parity**

Run:

```bash
cargo test --test formal_protocol --test event_hub --test history_sqlite_v1
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' cargo test --test history_postgres -- --nocapture
```

Expected: all available repository tests PASS; if the local PostgreSQL service is not running, start it with `docker compose -f docker-compose.postgres.yml up -d` and rerun.

- [ ] **Step 5: Commit protocol events**

```bash
git add src/events/protocol.rs tests/formal_protocol.rs tests/event_hub.rs tests/history_sqlite_v1.rs tests/history_postgres.rs
git commit -m "feat: persist branch lifecycle events"
```

### Task 7: Extract one-node execution and bounded admission

**Files:**
- Create: `src/runtime/execution.rs`
- Modify: `src/runtime/mod.rs`
- Modify: `src/runtime/control.rs`
- Modify: `src/runtime/coordinator.rs`
- Modify: `src/runtime/service.rs`
- Modify: `src/nodes/registry.rs`
- Create: `tests/run_scheduler.rs`
- Modify: `tests/run_coordinator.rs`

**Interfaces:**
- Consumes: compiled node, branch/main context snapshot, executor registry, `EventHub`, repository-backed output barrier, external stop signal, and two semaphores
- Produces: `execute_node`, typed `NodeExecutionFailure`, and process/per-Run concurrency enforcement

- [ ] **Step 1: Write deterministic permit and error-classification tests**

Create `tests/run_scheduler.rs` with a synthetic executor that increments an atomic in-flight counter, notifies `started`, waits on a `Notify` release, then decrements. Drive three `execute_node` futures with a global semaphore of 2 and per-Run semaphore of 1; assert only one reaches `started` before release. In a second case use per-Run 3 and global 2; assert exactly two start.

Add classifications:

```rust
assert!(matches!(
    RunError::new("UPSTREAM_FAILURE", "failed").kind(),
    RunErrorKind::Node
));
assert!(matches!(
    RunError::infrastructure("EVENT_APPEND_FAILED", "failed").kind(),
    RunErrorKind::Infrastructure
));
assert!(matches!(
    RunError::stopped(StopReason::Cancelled).kind(),
    RunErrorKind::Stop
));
```

- [ ] **Step 2: Run the scheduler test and verify it fails to compile**

Run: `cargo test --test run_scheduler execution -- --nocapture`

Expected: FAIL because the execution module, limiter, and error kinds do not exist.

- [ ] **Step 3: Classify failures by origin, never by code matching**

Add to `src/runtime/mod.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunErrorKind {
    Node,
    Stop,
    Infrastructure,
}
```

Store `kind: RunErrorKind` in `RunError`. Keep `RunError::new` as node-local, make `stopped` produce `Stop`, and add:

```rust
pub fn infrastructure(code: &'static str, message: impl Into<String>) -> Self;
pub fn kind(&self) -> RunErrorKind;
```

Use `RunError::infrastructure` for missing executors and all history/event adapter errors. Content-emitter journal errors must propagate as infrastructure errors even though they pass through a node executor.

- [ ] **Step 4: Implement the node execution lifecycle**

Define:

```rust
#[derive(Debug)]
pub enum NodeExecutionFailure {
    Node { node_id: String, error: RunError },
    Stop { node_id: String, error: RunError },
    Infrastructure(RunError),
}

pub struct NodeExecutionResult {
    pub node_id: String,
    pub context: RunContext,
    pub outcome: NodeOutcome,
}

#[derive(Clone)]
pub struct ExecutionLimiter {
    global: Arc<Semaphore>,
    per_run: Arc<Semaphore>,
}

impl ExecutionLimiter {
    pub fn new(global: Arc<Semaphore>, per_run: Arc<Semaphore>) -> Self {
        Self { global, per_run }
    }
}
```

`ExecutionLimiter::acquire` obtains one owned per-Run permit and one owned process permit, selecting against the external stop signal at both waits. `execute_node` then performs exactly:

```text
acquire both permits
-> publish node.started
-> resolve executor
-> select executor / external stop / node timeout
-> on executor failure publish node.failed
-> on success put_node_output
-> publish node.completed
-> return context plus outcome
```

The two permit guards live through output persistence and `node.completed`. Event/repository errors return `Infrastructure`; executor errors and node timeout return `Node`; external stop returns `Stop`. `Node` and `Stop` retain the attempted node ID so the scheduler can settle a branch or report the stopped work without parsing an error message. A node/stop error publishes `node.failed` only if `node.started` was already durable; cancellation while waiting for permits emits no synthetic node event. The function never changes Run terminal state.

- [ ] **Step 5: Make the current cursor use the extracted execution boundary**

Add `ExecutionLimiter` to `RunCoordinator::new`. In the existing sequential `current` loop, replace inline executor resolution, `ExecutionControl`, timeout selection, output persistence, and node events with one `execute_node` call. Match the result as:

```rust
match execute_node(...).await {
    Ok(result) => {
        context = result.context;
        context.set_node_output(&result.node_id, result.outcome.output.clone());
        result.outcome
    }
    Err(NodeExecutionFailure::Node { error, .. })
    | Err(NodeExecutionFailure::Stop { error, .. }) => {
        return self.finish_error(&state, new_run, error).await;
    }
    Err(NodeExecutionFailure::Infrastructure(error)) => return Err(error),
}
```

Change `finish_error` to accept only the terminal `RunError` and remove its current node-event publication block. The execution boundary now owns `node.failed`, preventing duplicate events before the scheduler cutover.

- [ ] **Step 6: Wire the global semaphore into the service**

Add `node_capacity: Arc<Semaphore>` to `RunServiceInner`, initialized from `max_parallel_node_executions`. When launching a Run, construct `ExecutionLimiter` from the shared node semaphore and a new per-Run semaphore sized by `max_parallel_branches_per_run`, then pass it to the coordinator. Update direct coordinator test builders the same way. Do not remove the existing independent Run-capacity semaphore.

- [ ] **Step 7: Verify admission, persistence order, and classification**

Run:

```bash
cargo test --test run_scheduler execution -- --nocapture
cargo test --test run_coordinator --test run_service
```

Expected: PASS; operation logs prove node output precedes `node.completed`, and counters never exceed either permit limit.

- [ ] **Step 8: Commit the execution boundary**

```bash
git add src/runtime/execution.rs src/runtime/mod.rs src/runtime/control.rs src/runtime/coordinator.rs src/runtime/service.rs src/nodes/registry.rs tests/run_scheduler.rs tests/run_coordinator.rs
git commit -m "refactor: isolate bounded node execution"
```

### Task 8: Replace the single cursor with a sequentially compatible scheduler

**Files:**
- Create: `src/runtime/scheduler.rs`
- Modify: `src/runtime/state.rs`
- Modify: `src/runtime/coordinator.rs`
- Modify: `src/runtime/mod.rs`
- Modify: `tests/run_scheduler.rs`
- Modify: `tests/run_coordinator.rs`

**Interfaces:**
- Consumes: `ExecutionPlan`, `execute_node`, typed transitions, Run context, and stop signal
- Produces: single-owner ready queue and `SchedulerResult` consumed by `RunCoordinator`

- [ ] **Step 1: Write failing sequential scheduler parity tests**

Move the behavioral intent of the current `coordinator_executes_next_goto_and_complete_with_persistence_barriers` into scheduler coverage. Assert the exact path `prepare -> route -> answer -> result`, predecessor visibility, one execution per node, output equality, and the existing event order. Add a condition-style `Goto` case where the unselected node's counter stays zero.

- [ ] **Step 2: Run sequential scheduler tests and verify they fail**

Run: `cargo test --test run_scheduler sequential -- --nocapture`

Expected: FAIL because `Scheduler` and its finite state types do not exist.

- [ ] **Step 3: Define scheduler-owned finite states**

Add to `src/runtime/state.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Pending,
    Ready,
    Running,
    Succeeded,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchState {
    Pending,
    Running,
    Succeeded,
    Failed,
}
```

In `scheduler.rs`, define private `WorkScope::{Main, Branch { fork_id, branch_id }}`, `ReadyNode`, and state maps. Define the coordinator boundary:

```rust
pub enum SchedulerResult {
    Completed(RunOutput),
    Failed(RunError),
    Stopped(RunError),
}
```

Infrastructure errors remain `Err(RunError)` so the coordinator's existing recovery path handles them.

- [ ] **Step 4: Implement the single-owner ready loop for ordinary graphs**

Initialize only the Agent entry as ready. The scheduler task is the sole writer of ready queues and state maps; spawned node futures return `NodeExecutionResult` and do not mutate scheduler state. Handle directives exactly:

```rust
NodeTransition::Next => activate(node.next.as_deref())
NodeTransition::Goto(target) => activate(Some(&target))
NodeTransition::Complete(output) => return SchedulerResult::Completed(output)
NodeTransition::ActivateFork => validate the compiled plan contains this fork
```

For this task, encountering `ActivateFork` returns infrastructure error `SCHEDULER_FORK_UNSUPPORTED`; Task 9 replaces that branch. Reject duplicate activation or an impossible target with `SCHEDULER_INVARIANT_VIOLATION` as infrastructure failure.

- [ ] **Step 5: Delegate graph advancement from the coordinator**

Keep Run creation, `run.created`, repository `mark_running`, `run.started`, terminal publication, and infrastructure recovery in `RunCoordinator`. Replace its `current` loop with one scheduler call and map:

```rust
Ok(SchedulerResult::Completed(output)) => self.complete(..., output).await
Ok(SchedulerResult::Failed(error))
| Ok(SchedulerResult::Stopped(error)) => {
    self.finish_error(..., error).await
}
Err(error) => Err(error)
```

Keep `finish_error` terminal-only as established in Task 7. `execute_node` owns `node.failed`, so the coordinator must not publish a duplicate.

- [ ] **Step 6: Verify full sequential compatibility**

Run:

```bash
cargo test --test run_scheduler sequential -- --nocapture
cargo test --test run_coordinator --test run_service --test api
cargo test --test core_template_condition --test core_chat_action --test core_output
```

Expected: PASS with the pre-scheduler outputs, status mapping, and node event ordering.

- [ ] **Step 7: Commit the scheduler cutover**

```bash
git add src/runtime/scheduler.rs src/runtime/state.rs src/runtime/coordinator.rs src/runtime/mod.rs tests/run_scheduler.rs tests/run_coordinator.rs
git commit -m "refactor: drive runs with a ready queue scheduler"
```

### Task 9: Execute branch subgraphs concurrently and join all settled results

**Files:**
- Modify: `src/runtime/scheduler.rs`
- Modify: `src/runtime/context.rs`
- Modify: `src/runtime/state.rs`
- Modify: `tests/run_scheduler.rs`
- Modify: `tests/fork_join_nodes.rs`
- Modify: `tests/api.rs`

**Interfaces:**
- Consumes: compiled `ForkPlan`, branch contexts, branch event protocol, and concurrent node results
- Produces: overlap, branch barriers, exact partial-failure results, and continuation after join

- [ ] **Step 1: Write deterministic overlap and barrier tests**

Build a directly compiled synthetic Agent with two two-node branches. Each branch entry notifies a shared start barrier and waits until both have arrived; the test releases both without elapsed-time assertions. Assert:

```rust
assert_eq!(max_in_flight.load(Ordering::SeqCst), 2);
assert_eq!(execution_count("search_a"), 1);
assert_eq!(execution_count("summarize_a"), 1);
assert_eq!(execution_count("search_b"), 1);
assert_eq!(execution_count("summarize_b"), 1);
assert!(!join_started_before_all_branches.load(Ordering::SeqCst));
```

Add a 10-branch case with `max_parallel_branches_per_run: 3`; use counters and `Notify` gates to prove the maximum is 3 and every waiting branch eventually starts.

Add a branch-local `Goto` case with two possible targets and assert exactly the selected target executes before settlement; the other target remains `Pending` until the branch settles and is then never admitted.

- [ ] **Step 2: Write failing partial and all-failed tests**

Add one case where `search_b` returns `RunError::new("UPSTREAM_FAILURE", "upstream service failed")` and `source_a` succeeds. Assert the Run completes, `summarize_b` never executes, and `collect` output equals:

```rust
json!({
    "branches": {
        "source_a": {
            "status":"succeeded",
            "terminal_node_id":"summarize_a",
            "output":{"text":"result a"}
        },
        "source_b": {
            "status":"failed",
            "terminal_node_id":"search_b",
            "error": {
                "code":"UPSTREAM_FAILURE",
                "message":"upstream service failed"
            }
        }
    },
    "summary":{"total":2,"succeeded":1,"failed":1}
})
```

Add an all-failed case asserting the join still completes and post-join output runs. Add a node-timeout case where only the timed-out branch fails.

- [ ] **Step 3: Run parallel tests and verify scheduler failure**

Run: `cargo test --test run_scheduler parallel -- --nocapture`

Expected: FAIL with `SCHEDULER_FORK_UNSUPPORTED` or missing branch activation behavior.

- [ ] **Step 4: Implement fork activation and isolated branch contexts**

Replace the unsupported branch with compiled-plan lookup. Persisted fork completion already occurred inside `execute_node`; then, in deterministic branch-ID order:

1. set branch state to `Running`;
2. publish `branch.started` with `fork_id` and `branch_id`;
3. create `main_context.fork_branch()`;
4. enqueue the declared entry in `WorkScope::Branch`.

The `branch.started` event means activation into the ready queue, not acquisition of a node permit. Keep no sibling outputs in a branch context.

- [ ] **Step 5: Settle branches and activate the join exactly once**

For a successful branch node, update only that branch context. If its selected successor is the paired join, store:

```rust
BranchResult::Succeeded {
    terminal_node_id: node_id,
    output: outcome.output,
}
```

then publish `branch.completed`. For `NodeExecutionFailure::Node { node_id, error }`, store `BranchResult::Failed` with that node ID and only `RunError::code()` and `RunError::message()`, publish `branch.failed`, and do not enqueue the failed path's successors. This is the sanitization boundary: never serialize `Debug`, backtraces, provider response bodies, or error sources.

When and only when every branch is terminal, create `main_context.with_join_results(results)`, set the paired join ready once, and clear the active fork after join completion. The join executor's output is added to the main context; branch-local maps are never merged.

- [ ] **Step 6: Assert durable branch/join ordering**

From stored events, assert:

```text
fanout node.completed
< every branch.started
< each branch's node.completed|node.failed
< its branch.completed|branch.failed
< collect node.started
< collect node.completed
```

Also assert `content.delta` from siblings may interleave while all event sequences remain unique and contiguous.

- [ ] **Step 7: Verify all-settled execution**

Run:

```bash
cargo test --test run_scheduler parallel -- --nocapture
cargo test --test fork_join_nodes --test event_hub
cargo test --test run_coordinator --test run_service --test api
```

Before running the last command, add an API test that creates a detached parallel Run, waits for terminal completion, reconnects to `/v1/runs/{run_id}/events?after_seq={branch_started_seq}`, and asserts the SSE frames contain the remaining branch terminal events plus join events with the same increasing `seq` values stored by the repository. Expected: PASS for overlap, per-Run backpressure, partial failure, all-failed success, timeout isolation, join barrier order, and SSE replay parity.

- [ ] **Step 8: Commit parallel scheduling**

```bash
git add src/runtime/scheduler.rs src/runtime/context.rs src/runtime/state.rs tests/run_scheduler.rs tests/fork_join_nodes.rs tests/api.rs
git commit -m "feat: execute fork branches with all-settled join"
```

### Task 10: Make cancellation and infrastructure failure globally fatal

**Files:**
- Modify: `src/runtime/execution.rs`
- Modify: `src/runtime/scheduler.rs`
- Modify: `src/runtime/coordinator.rs`
- Modify: `src/runtime/service.rs`
- Modify: `tests/run_scheduler.rs`
- Modify: `tests/run_coordinator.rs`
- Modify: `tests/run_service.rs`

**Interfaces:**
- Consumes: external `StopSignal`, task cancellation token, classified execution failures, and coordinator recovery
- Produces: global fan-out stop, no post-stop admission, panic containment, and one durable terminal Run state

- [ ] **Step 1: Write failing global-stop tests**

Add a two-branch executor that waits on `ExecutionControl::stopped()`. After both start, request `StopReason::Cancelled`; assert both observe stop, no ready successor starts, Run status is `cancelled`, and exactly one terminal event/update exists. Repeat with `StopReason::TimedOut` and assert Run status `failed` with `RUN_TIMEOUT`.

Add an attached reconnect-grace service test proving both active branch tasks stop after the grace expires. Keep detached behavior independent of subscribers.

- [ ] **Step 2: Write failing infrastructure and panic tests**

Add cases for:

- journal append failure while one sibling is running;
- repository node-output failure;
- missing executor in one branch;
- a synthetic executor panic;
- a forced duplicate node activation invariant.

Each case asserts sibling work is cancelled, no join is started, branch failure data is not fabricated, the coordinator recovery terminal is `INFRASTRUCTURE_FAILURE`, and terminal persistence happens once.

- [ ] **Step 3: Run failure tests and observe the incomplete behavior**

Run: `cargo test --test run_scheduler global -- --nocapture`

Expected: FAIL because the scheduler does not yet cancel and drain all work for every global failure source.

- [ ] **Step 4: Add scheduler-wide task cancellation and draining**

Give each scheduler a private `CancellationToken` cloned into every `execute_node`. The execution wrapper selects on both external Run stop and this scheduler token. On infrastructure failure or `JoinSet::join_next` error:

```rust
task_cancel.cancel();
ready.clear();
mark_pending_and_ready_skipped();
while join_set.join_next().await.is_some() {}
return Err(RunError::infrastructure(
    "INFRASTRUCTURE_FAILURE",
    "runtime infrastructure failed",
));
```

On external stop, perform the same admission stop and drain, but return `SchedulerResult::Stopped` using the external reason so the coordinator preserves `cancelled`, `interrupted`, or `RUN_TIMEOUT` semantics. Never convert `NodeExecutionFailure::Infrastructure` or a task panic into `BranchResult::Failed`.

- [ ] **Step 5: Preserve terminal ownership and service health behavior**

Keep all terminal publication in `RunCoordinator`. Its infrastructure `Err` continues through `recover_infrastructure_failure`; if recovery itself fails, `RunService` marks itself unhealthy as today. Ensure a terminal race reads the durable repository state rather than publishing a second terminal event.

- [ ] **Step 6: Verify cancellation and global failure gates**

Run:

```bash
cargo test --test run_scheduler global -- --nocapture
cargo test --test run_coordinator --test run_service --test api
cargo test --test event_hub
```

Expected: PASS; no node starts after stop, all in-flight branch work ends, and every Run has exactly one terminal state/event.

- [ ] **Step 7: Commit global failure semantics**

```bash
git add src/runtime/execution.rs src/runtime/scheduler.rs src/runtime/coordinator.rs src/runtime/service.rs tests/run_scheduler.rs tests/run_coordinator.rs tests/run_service.rs
git commit -m "fix: make scheduler infrastructure failures run-fatal"
```

### Task 11: Check in a parallel Agent, document contracts, and pass every gate

**Files:**
- Create: `agents/parallel_researcher/agent.yaml`
- Create: `agents/parallel_researcher/prompts/system.md`
- Create: `agents/parallel_researcher/prompts/perspective_a.md`
- Create: `agents/parallel_researcher/prompts/perspective_b.md`
- Create: `agents/parallel_researcher/prompts/synthesize.md`
- Modify: `config/platform.yaml`
- Modify: `README.md`
- Modify: `docs/formal-v1-breaking-changes.md`
- Modify: `tests/formal_agent_compile.rs`
- Modify: `tests/repository_agents_v1.rs`

**Interfaces:**
- Consumes: completed compiler, scheduler, event, and configuration contracts
- Produces: a checked-in multi-node parallel example and release-level evidence

- [ ] **Step 1: Write failing checked-in Agent compilation assertions**

Extend repository Agent tests to compile `parallel_researcher` and assert:

```rust
let agent = registry.get("parallel_researcher").unwrap();
let fork = &agent.execution_plan.forks["fanout"];
assert_eq!(fork.branches.len(), 2);
assert_eq!(fork.join_id, "collect");
assert!(fork.branches.values().all(|branch| branch.nodes.len() >= 2));
assert_eq!(fork.policy, JoinPolicy::AllSettled);
```

- [ ] **Step 2: Run checked-in Agent tests and verify they fail**

Run: `cargo test --test repository_agents_v1 --test formal_agent_compile -- --nocapture`

Expected: FAIL because `agents/parallel_researcher` is absent.

- [ ] **Step 3: Add the fixed parallel research example**

Create a strict V1 Agent with this topology:

```text
prepare -> fanout(core.fork)
  perspective_a: analyze_a(core.chat) -> normalize_a(core.template) -> collect
  perspective_b: analyze_b(core.chat) -> normalize_b(core.template) -> collect
collect(core.join, all_settled) -> synthesize(core.chat) -> result(core.output)
```

The branch prompts may reference `nodes.prepare.output`; `synthesize` references only `nodes.collect.output`, never `analyze_a`, `normalize_a`, `analyze_b`, or `normalize_b`. Add `parallel_researcher` to the default enabled Agent list.

- [ ] **Step 4: Document exact DSL, output, event, and failure behavior**

Add to `README.md`:

- complete `core.fork`/`core.join` YAML;
- the exact partial-success JSON envelope;
- `branch.started`, `branch.completed`, `branch.failed` event payloads;
- explanation that `branch.started` means ready-queue activation;
- process-wide and per-Run limit meanings;
- cancellation versus branch-local failure behavior;
- no nested fork, resume, additional join modes, or direct post-join branch references.

Update `docs/formal-v1-breaking-changes.md` to state why internal interfaces changed: topology must be compiled once, a scalar cursor cannot model synchronization, node events cannot express branch settlement, and unbounded parallel work is unsafe. State explicitly that HTTP routes and sequential YAML did not change.

- [ ] **Step 5: Run focused feature verification**

Run:

```bash
cargo test --test dsl_parallel --test fork_join_nodes --test run_scheduler -- --nocapture
cargo test --test formal_protocol --test event_hub --test history_sqlite_v1
cargo test --test repository_agents_v1 --test formal_agent_compile
```

Expected: all focused tests PASS.

- [ ] **Step 6: Run the complete local quality suite**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo audit
cargo deny check
```

Expected: every command exits 0.

- [ ] **Step 7: Verify real PostgreSQL parity**

Run:

```bash
docker compose -f docker-compose.postgres.yml up -d
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' cargo test --test history_postgres -- --nocapture
```

Expected: PASS, including branch event round-trip and contiguous sequences.

- [ ] **Step 8: Inspect the final diff for scope and secrets**

Run:

```bash
git diff --check
git status --short
rg -n "api[_-]?key|authorization|bearer|password" agents/parallel_researcher README.md config/platform.yaml
```

Expected: no whitespace errors; status lists only intended feature files; the secret scan finds documentation/config field names only and no credential value.

- [ ] **Step 9: Commit the example and release documentation**

```bash
git add agents/parallel_researcher config/platform.yaml README.md docs/formal-v1-breaking-changes.md tests/formal_agent_compile.rs tests/repository_agents_v1.rs
git commit -m "docs: publish parallel workflow baseline"
```

- [ ] **Step 10: Request final code review before integration**

Use `superpowers:requesting-code-review` against the complete commit range. Resolve every correctness or specification issue, rerun Step 6 plus Step 7, and only then use `superpowers:finishing-a-development-branch` to choose merge or cleanup.
