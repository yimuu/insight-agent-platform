# Unified `core.end` Terminal Model Design

**Date:** 2026-07-13  
**Status:** Approved for implementation planning  
**Scope:** Formal V1 DSL, compiler graph model, scheduler, Fork/Join settlement, events, history, API, checked-in Agents, documentation, and verification

## 1. Context

The current Formal V1 runtime has asymmetric workflow termination:

- `core.output` returns `NodeTransition::Complete(RunOutput)` for successful Run completion.
- A node that returns `RunError` follows the failure path and may fail either the current Fork branch or the entire Run.
- Fork branches signal successful settlement by targeting their paired `core.join`; the scheduler uses the node immediately before Join as the branch terminal node and branch output.
- Graph validation hard-codes `core.output` as the only legal terminal node type.

This model works, but it conflates several distinct concepts:

1. successful node execution;
2. authored workflow success;
3. authored workflow failure;
4. unexpected node failure;
5. external stop and infrastructure failure;
6. branch settlement and Run termination.

The project is still pre-adoption and has no compatibility requirement for existing Agent DSL, stored history, or event consumers. The design therefore replaces the current terminal model in one step instead of adding compatibility aliases or preserving two competing ways to end a workflow.

## 2. Decision

Formal V1 will use one terminal node: `core.end`.

- Remove `core.output`.
- Do not add `core.fail`.
- Every statically successful main-flow path must end at `core.end`.
- Every statically successful Fork-branch path must end at `core.end`.
- A Fork branch no longer points to its paired Join.
- The Fork declaration provides a typed continuation dependency on the Join.
- `core.end` ends the current execution scope only.
- `core.end(outcome: success)` produces Run success in the main scope and branch success in a Fork branch.
- `core.end(outcome: failure)` produces authored workflow failure in the main scope and authored branch failure in a Fork branch.
- An authored failure is a successful execution of the End node, not a `RunError`.
- Cancellation, interruption, timeout, and infrastructure failure remain runtime-owned and cannot be authored by DSL.
- `core.join` remains an explicit `all_settled` aggregator.
- `core.condition` remains responsible for business routing after Join.
- `core.select` remains responsible for one-of-N data convergence when mutually exclusive paths continue to shared downstream work.

## 3. Goals

- Make success and authored failure symmetric, typed workflow outcomes.
- Separate workflow outcome from node execution health.
- Make branch return values and failures explicit.
- Replace string-based terminal validation with typed control semantics.
- Separate executable graph edges from Fork synchronization dependencies.
- Preserve strict reference dominance and Fork region isolation.
- Keep Join as a pure aggregation boundary.
- Make invalid Run terminal field combinations unrepresentable internally.
- Expose stable failure origin metadata to Join, events, history, API, and logs.
- Establish one canonical Formal V1 DSL with no legacy aliases.

## 4. Non-goals

- Nested Fork regions.
- Resume or checkpoint continuation.
- Retry or compensation policies.
- Join modes other than `all_settled`.
- Dynamic or templated End outcomes.
- Structured arbitrary failure bodies.
- DSL-authored cancellation, interruption, or timeout.
- A distinct `degraded` Run status.
- Named Condition cases.
- Changing Select's one-of-N output contract.

## 5. `core.end` DSL Contract

`core.end` uses a strict internally tagged configuration. `outcome` is required and must be either `success` or `failure`.

### 5.1 Success

```yaml
finish:
  type: core.end
  config:
    outcome: success
    content:
      template: "Processing completed"
    format: markdown
    data:
      result: "{{ nodes.answer.output }}"
```

Success fields:

- `content`: optional `{template: string}` rendered with the ordinary strict template context.
- `format`: optional `text` or `markdown`; required when `content` exists and forbidden otherwise.
- `data`: optional recursive JSON template value.
- At least one of `content` or `data` is required.
- When `data` is omitted, the runtime value is JSON `null` only if `content` is present.
- Template and data references use the existing shared reference extractor and dominance validator.

The success output uses the existing stable Run output shape:

```json
{
  "content": "Processing completed",
  "format": "markdown",
  "data": {"result": {}}
}
```

Absent optional `content` and `format` fields are omitted during serialization. `data` is always present.

### 5.2 Failure

```yaml
fail_all:
  type: core.end
  config:
    outcome: failure
    code: WORKFLOW_ALL_BRANCHES_FAILED
    message: all parallel branches failed
```

Failure fields:

- `code`: required static string matching `WORKFLOW_[A-Z][A-Z0-9_]*`, at most 64 UTF-8 bytes.
- `message`: required static string, non-blank after trimming, one line, free of control characters, and at most 256 UTF-8 bytes.
- Templates, CEL, node references, and dynamic interpolation are forbidden in failure fields.
- `content`, `format`, and `data` are forbidden for failure outcomes.

The `WORKFLOW_` namespace prevents authored errors from impersonating runtime-owned `RUN_`, `NODE_`, scheduler, history, or infrastructure errors.

### 5.3 Envelope Rules

For both outcomes:

- common `next` is forbidden;
- `emit: content` is forbidden;
- the default `emit: none` is accepted;
- the node has no outgoing executable edge;
- `outcome` is compile-time static and cannot depend on input or prior node outputs.

### 5.4 Degraded Success

Degraded completion is normal success with explicit business metadata:

```yaml
finish_degraded:
  type: core.end
  config:
    outcome: success
    data:
      degraded: true
      branch_summary: "{{ nodes.collect.output.summary }}"
```

No `degraded` lifecycle status is added. Consumers decide how to interpret the explicit result data.

## 6. Typed Terminal Model

The compiled/runtime model introduces authored terminal outcomes:

```rust
pub enum TerminalOutcome {
    Success(RunOutput),
    Failure(WorkflowError),
}

pub struct WorkflowError {
    pub code: String,
    pub message: String,
}
```

`WorkflowError` is not `RunError`. Returning `TerminalOutcome::Failure` means the End executor completed its authored responsibility successfully.

`NodeTransition` becomes:

```rust
pub enum NodeTransition {
    Next,
    Goto(String),
    ActivateFork,
    End(TerminalOutcome),
}
```

`NodeTransition::Complete(RunOutput)` is removed.

The End executor returns a normal `NodeOutcome`:

- success node output: `{"outcome":"success","output":<RunOutput>}`;
- failure node output: `{"outcome":"failure","error":{"kind":"workflow","code":...,"message":...}}`;
- transition: `NodeTransition::End(...)`.

Template rendering, compiled-body mismatch, cancellation observation, or other executor defects still return typed `RunError` and follow the ordinary node failure path.

## 7. Scope Semantics

`core.end` always terminates the current scheduler scope. The scope is derived from the compiler execution plan and is not configurable in DSL.

| Region | `success` | `failure` |
|---|---|---|
| Main/linear | complete Run | fail Run with workflow origin |
| Fork branch | settle branch succeeded | settle branch failed with workflow origin |

A branch End can never directly terminate the Run. This is a structured-concurrency invariant: a child scope returns to its owning Fork, and the parent workflow decides the overall policy after Join.

The node state for either End outcome is `Succeeded`, because the End node executed correctly. The surrounding branch or Run state may nevertheless become failed for `outcome: failure`.

## 8. Fork and Join Topology

### 8.1 New Shape

Branches no longer point to Join:

```yaml
start:
  type: core.fork
  config:
    branches:
      perspective_a: analyze_a
      perspective_b: analyze_b
    join: collect

analyze_a:
  type: core.chat
  next: end_a
  config: {}

end_a:
  type: core.end
  config:
    outcome: success
    data:
      answer: "{{ nodes.analyze_a.output.text }}"

analyze_b:
  type: core.chat
  next: end_b
  config: {}

end_b:
  type: core.end
  config:
    outcome: success
    data:
      answer: "{{ nodes.analyze_b.output.text }}"

collect:
  type: core.join
  next: decide
  config:
    mode: all_settled
```

`start.config.join` is a synchronization continuation, not an executable edge activated when Fork starts.

### 8.2 Branch Settlement

A branch settles when either:

- it executes `core.end(success)`;
- it executes `core.end(failure)`;
- an ordinary branch node returns a contained node or node-timeout failure.

External stop and infrastructure failure do not settle an individual branch. They stop or fail the entire Run and drain active branch work under the existing global-failure rules.

When every branch has settled, the scheduler activates the paired Join exactly once.

### 8.3 Static Branch Rules

- Every statically successful path from a branch entry must reach a branch-local `core.end`.
- A branch-local End belongs to that branch region.
- No branch node may have a direct edge to the paired Join.
- No branch may enter another branch region.
- No branch may enter a Join owned by another Fork.
- Nested Fork remains rejected.
- A branch may contain Condition and Select nodes under their existing rules.
- Mutually exclusive branch paths may terminate at separate End nodes without Select when no downstream convergence is needed.
- If mutually exclusive paths converge for more work before ending, they must use Select exactly as in the existing contract.

## 9. Typed Control Edges

The current untyped `Vec<String>` edge representation conflates ordinary successors, Condition targets, and Fork branch entries, while the paired Join is stored separately. The new compiler model uses typed edges:

```rust
pub enum ControlEdge {
    Direct { target: String },
    Conditional { target: String },
    ForkBranch { branch_id: String, target: String },
    ForkContinuation { target: String },
}
```

Semantics:

- `Direct`: statically declared ordinary successor.
- `Conditional`: one of the Condition node's runtime-selected targets.
- `ForkBranch`: a branch entry activated when Fork begins.
- `ForkContinuation`: the paired Join activated only after every branch settles.

Graph algorithms use edge kinds deliberately:

- existence, cycle detection, whole-agent reachability, and agent hashing include every structural edge;
- scheduler activation uses Direct, the chosen Conditional edge, and all ForkBranch edges;
- scheduler settlement activates ForkContinuation exactly once;
- Select predecessor equality considers only direct executable incoming edges and never treats ForkContinuation as a Select candidate;
- reference dominance uses the structural continuation path so pre-Fork nodes dominate Join and post-Join work, while branch-local nodes do not;
- Fork-region validation remains authoritative for sibling and post-Join reference errors.

`terminal: bool` is removed from `NodeCompilation` and `CompiledNode`. `NodeControl::End` is the typed terminal declaration.

The compile-time control stores only `EndOutcomeKind::Success` or `EndOutcomeKind::Failure`; rendered templates and the validated workflow error remain in the compiled node body. Canonical Agent hashing includes every typed edge kind, branch ID, Condition target order, Fork continuation, End outcome kind, and normalized End configuration so structurally different workflows cannot share a version hash.

## 10. Graph Validation

The compiler enforces the following order so stable structural diagnostics remain authoritative:

1. node and edge target existence;
2. typed structural cycle detection;
3. End envelope and no-successor requirements for reachable nodes;
4. whole-agent structural reachability;
5. Fork declaration and Join ownership;
6. branch-region construction and all-paths-End proof;
7. sibling-region and illegal incoming/outgoing edge checks;
8. Join ownership and no-direct-entry checks;
9. Select topology validation;
10. shared reference dominance and cross-region validation.

Required-`next` envelope handling is deferred until this graph pass. A reachable
non-End node without an executable successor reports `END_REQUIRED`, even when
the missing successor also leaves declared downstream nodes unreachable.
`NODE_UNREACHABLE` remains authoritative only when no earlier reachable
non-End leaf exists.

Required graph rules:

- Every statically successful main-flow path ends at End.
- Every statically successful branch path ends at End.
- An End node has no outgoing edge.
- Every Join is owned by exactly one Fork.
- A Join has no ordinary, Conditional, or branch direct incoming edge.
- A ForkContinuation is the only structural incoming relationship to its Join.
- A Join remains non-terminal and requires ordinary `next`.
- An unexpected runtime node failure may end execution before the statically declared End; this does not weaken the compile-time success-path proof.

## 11. Join Output Contract

Successful branch:

```json
{
  "status": "succeeded",
  "terminal_node_id": "end_a",
  "output": {
    "data": {"answer": "..."}
  }
}
```

Authored branch failure:

```json
{
  "status": "failed",
  "terminal_node_id": "reject_b",
  "error": {
    "kind": "workflow",
    "code": "WORKFLOW_LOW_CONFIDENCE",
    "message": "branch confidence is insufficient"
  }
}
```

Unexpected node failure:

```json
{
  "status": "failed",
  "terminal_node_id": "analyze_b",
  "error": {
    "kind": "node",
    "code": "MODEL_REQUEST_FAILED",
    "message": "model request failed"
  }
}
```

Node timeout uses `kind: "timeout"`. Failure origin is derived from typed runtime state, never inferred by parsing an error code.

Stable Join aggregate:

```json
{
  "branches": {
    "perspective_a": {},
    "perspective_b": {}
  },
  "summary": {
    "total": 2,
    "succeeded": 1,
    "failed": 1,
    "failures": {
      "workflow": 1,
      "node": 0,
      "timeout": 0
    }
  }
}
```

Invariants:

- `total == succeeded + failed`;
- `failed == failures.workflow + failures.node + failures.timeout`;
- infrastructure and external-stop failures never appear as settled branch results;
- all branches failed is still a successful Join execution under `all_settled`;
- Join never decides whether the Run should fail.

## 12. Post-Join Policy

The canonical all-settled policy flow is explicit:

```yaml
decide:
  type: core.condition
  config:
    cases:
      - when: nodes.collect.output.summary.succeeded > 0
        next: synthesize
    default: fail_all

synthesize:
  type: core.template
  next: finish
  config:
    value:
      degraded: "{{ nodes.collect.output.summary.failed }}"
      branches: "{{ nodes.collect.output.branches }}"

finish:
  type: core.end
  config:
    outcome: success
    data: "{{ nodes.synthesize.output }}"

fail_all:
  type: core.end
  config:
    outcome: failure
    code: WORKFLOW_ALL_BRANCHES_FAILED
    message: all parallel branches failed
```

The templates use the existing recursive string-leaf rendering contract: Join aggregates, Condition routes, Template prepares the result, and End terminates.

## 13. Scheduler Semantics

The scheduler replaces `Completed(RunOutput)` with an authored End result:

```rust
pub enum SchedulerResult {
    Ended(TerminalOutcome),
    Failed(RunError),
    Stopped(RunError),
}
```

Infrastructure failure remains the scheduler's outer `Err(RunError)` and uses the existing durable recovery path.

Main-scope End:

- End success returns `SchedulerResult::Ended(Success(...))`.
- End failure returns `SchedulerResult::Ended(Failure(...))`.

Branch-scope End:

- End success settles `BranchResult::Succeeded` with the End output.
- End failure settles `BranchResult::Failed` with `kind: workflow`.
- Both mark the End node itself succeeded.
- Both publish the scope event only after the End node output and `node.completed` are durable.

Unexpected branch-node failures retain current containment:

- mark the failing node failed;
- publish `node.failed`;
- settle the branch failed with `kind: node` or `kind: timeout`;
- continue other branches;
- activate Join after all branches settle.

Stop and infrastructure failures retain current global cancellation, draining, and recovery behavior.

## 14. Event Contract

No new event type is added. The current Formal V1 event type set remains exact, but terminal-node semantics are updated before public adoption.

### 14.1 Main Success

```text
node.started(core.end)
node.completed(core.end)
run.completed
```

### 14.2 Authored Main Failure

```text
node.started(core.end)
node.completed(core.end)
run.failed(kind=workflow)
```

There is no `node.failed` because the End node executed as authored.

### 14.3 Authored Branch Failure

```text
node.started(core.end)
node.completed(core.end)
branch.failed(kind=workflow)
```

### 14.4 Unexpected Node Failure

```text
node.started
node.failed
branch.failed(kind=node|timeout) | run.failed(kind=node|timeout)
```

Ordering invariants:

- successful End node output is durable before `node.completed`;
- failure End terminal envelope is durable before `node.completed`;
- `node.completed` is durable before the resulting branch or Run terminal event;
- Run terminal events remain exactly once;
- Branch terminal events remain exactly once;
- infrastructure recovery never fabricates a workflow outcome.

Error events keep the top-level `code` and `message`. `run.failed` and `branch.failed` data additionally contain stable `kind`. `node.completed` uses `code: "OK"` because it reports successful End-node execution.

`EVENT_SCHEMA_VERSION` remains `1` because this design defines the pre-adoption Formal V1 contract rather than migrating a released protocol.

## 15. Failure Taxonomy

Stable public failure kinds:

```rust
pub enum FailureKind {
    Workflow,
    Node,
    Timeout,
    Infrastructure,
}
```

- `workflow`: authored `core.end(failure)`.
- `node`: unexpected node executor or node-owned validation/provider/action failure.
- `timeout`: typed node deadline or Run deadline expiration.
- `infrastructure`: event, repository, scheduler-task, or other runtime infrastructure failure.

Cancelled and interrupted are distinct Run statuses, not failure kinds. Branch results never contain infrastructure, cancelled, or interrupted outcomes.

Failure kind is carried by typed runtime values and persisted explicitly. It is never inferred from code prefixes.

## 16. Run Lifecycle and Persistence

Internal terminal construction must make contradictory fields unrepresentable:

```rust
pub enum RunTerminal {
    Completed { output: RunOutput },
    Failed { error: RunFailure },
    Cancelled { code: String, message: String },
    Interrupted { code: String, message: String },
}

pub struct RunFailure {
    pub kind: FailureKind,
    pub code: String,
    pub message: String,
}
```

`TerminalUpdate` accepts `RunTerminal` rather than independent status/output/error options. `RunStatus` is derived from the terminal variant.

Repository storage may remain column-oriented, but both formal backends enforce equivalent constraints:

- completed requires output and forbids error fields;
- failed requires `error_kind`, `error_code`, and `error_message`, and forbids output;
- cancelled and interrupted require their fixed system error fields and forbid output;
- created and running forbid every terminal field;
- terminal timestamps are present only for terminal states.

Because there is no deployed history compatibility requirement, the checked-in Formal V1 initial SQLite and PostgreSQL migrations are updated in place. Existing local development databases must be recreated; the application does not silently rewrite incompatible history.

## 17. HTTP Representation

The API keeps a flat, poll-friendly `status` while exposing mutually exclusive terminal details.

Running:

```json
{"status":"running"}
```

Completed:

```json
{
  "status": "completed",
  "output": {
    "content": "...",
    "format": "markdown",
    "data": {}
  }
}
```

Failed:

```json
{
  "status": "failed",
  "error": {
    "kind": "workflow",
    "code": "WORKFLOW_ALL_BRANCHES_FAILED",
    "message": "all parallel branches failed"
  }
}
```

Serialized records omit inapplicable output/error fields instead of returning independently nullable fields. Rust lifecycle types or custom serialization enforce consistency. Run summaries omit completed output bodies but include failure metadata when present.

Attached SSE event shapes follow Section 14. Detached polling continues to use `status` until a terminal state is reached.

## 18. Observability and Data Safety

INFO logs remain body-free.

End completion logs may include:

- run/request/agent/node identifiers;
- node kind;
- elapsed time;
- terminal outcome (`success` or `failure`);
- output/envelope serialized byte count;
- failure kind and code.

INFO logs must not include:

- End content;
- End data;
- workflow failure message;
- branch output;
- prompt, input, model output, or action bodies.

Failure messages are static DSL literals and may appear in durable error events and history, but never in INFO logs. Compile diagnostics may include structural IDs and field names but not rendered values.

Metrics and logs can now distinguish authored workflow failure from node, timeout, and infrastructure failure without parsing error codes.

## 19. Registry, DSL, and Migration

The default built-in node set remains eight nodes:

```text
core.template
core.chat
core.action
core.condition
core.fork
core.join
core.select
core.end
```

Migration is intentionally breaking and immediate:

- delete the `core.output` node type and executor;
- add the `core.end` node type and executor;
- do not register a `core.output` alias;
- do not implement `core.fail`;
- migrate every checked-in Agent to End success/failure;
- migrate every Fork fixture from branch-to-Join edges to branch-local End nodes;
- update README, formal examples, smoke fixtures, and built-in node counts;
- update initial Formal V1 database migrations and require local database recreation;
- accept Agent version hash changes;
- retain no compatibility parser or deprecation window.

## 20. Verification Strategy

### 20.1 End Contract

- Compile success with content-only, data-only, and combined output.
- Reject success with neither content nor data.
- Enforce content/format pairing.
- Render recursive success data and validate references.
- Compile valid failure codes/messages.
- Reject invalid namespace, length, blank, multiline, control-character, templated, and mixed-variant failure config.
- Reject `next` and `emit: content`.
- Prove End compiles to typed terminal control.

### 20.2 Graph and Plan

- Require End on every statically successful main path.
- Require End on every statically successful branch path.
- Reject End with outgoing edges.
- Reject branch-to-Join direct edges.
- Reject ordinary or Condition edges into Join.
- Require exactly one owning Fork per Join.
- Prove ForkContinuation participates in reachability, cycles, and dominance without becoming executable.
- Preserve cross-branch and post-Join reference errors.
- Preserve Select predecessor and region validation.
- Reject nested Fork as before.

### 20.3 Scheduler

- Main success End completes Run.
- Main failure End fails Run without `node.failed`.
- Branch success End settles succeeded with explicit output.
- Branch failure End settles workflow failure while the End node succeeds.
- Unexpected branch node failure remains contained.
- Node timeout is counted separately.
- Partial success reaches Join.
- All branches workflow-failed still reach Join.
- All branches node-failed still reach Join.
- Condition can route zero-success Join output to main failure End.
- Degraded success can complete with explicit data.
- Stop and infrastructure failures cancel/drain siblings and never appear in Join results.

### 20.4 Events and Persistence

- Assert the exact event sequences in Section 14.
- Persist End envelope before `node.completed`.
- Persist branch terminal state before branch event publication.
- Persist Run terminal state and terminal event atomically through EventHub.
- Enforce RunTerminal invariants in memory and both repositories.
- Verify SQLite/PostgreSQL constraint parity.
- Verify startup reconciliation and terminal recovery preserve failure kind.
- Verify API completed/failed/active shapes omit incompatible fields.

### 20.5 Observability

- Log End success and failure metadata exactly once.
- Assert content, data, branch bodies, and workflow messages are absent from INFO logs.
- Assert failure kind/code are present where intended.
- Preserve existing cancellation, branch-draining, and run-finished log ordering.

### 20.6 Binary Smoke

Real-binary coverage must include:

1. the existing no-key success path migrated to `core.end(success)`;
2. an authored workflow-failure Agent that reaches `core.end(failure)`;
3. detached polling until `completed` or `failed` rather than a single lookup;
4. exact completed output and failed error-kind/code assertions;
5. clean process shutdown.

## 21. Acceptance Criteria

1. Formal V1 exposes `core.end` and no longer exposes `core.output` or `core.fail`.
2. End success/failure configuration is a strict compile-time union.
3. Authored failure is a typed terminal outcome, not `RunError`.
4. Main End success completes the Run; main End failure fails it with workflow origin.
5. Branch End success/failure settles only the branch.
6. End failure publishes `node.completed`, never `node.failed`.
7. Unexpected executor failure still publishes `node.failed`.
8. Every statically successful main and branch path ends explicitly.
9. Branches do not point directly to Join.
10. ForkContinuation is structural, non-executable, cycle-aware, reachability-aware, and dominance-aware.
11. Join remains `all_settled`, runs after all contained branch failures, and never decides Run success.
12. Join output distinguishes workflow, node, and timeout failure origins.
13. Condition plus End expresses full success, degraded success, and total failure.
14. Select semantics and shared reference safety remain intact.
15. Cancellation, interruption, timeout, and infrastructure failure cannot be authored or captured as workflow End data.
16. Run terminal combinations are type-safe internally and constraint-safe in both repositories.
17. HTTP and SSE terminal representations are consistent with durable history.
18. INFO logs remain body-free and message-free.
19. Every checked-in Agent, fixture, README example, and smoke test uses the new model.
20. Formatting, Clippy, locked full tests, audit, deny, diff checks, and final whole-branch review pass.

## 22. Implementation Boundaries

This is one coordinated Formal V1 rewrite, but implementation should still use reviewable commits and checkpoints:

1. typed terminal/config contract;
2. typed graph edges and plan validation;
3. scheduler branch/Run End handling;
4. Join result and failure taxonomy;
5. Run lifecycle, repositories, API, and events;
6. checked-in Agent/README/smoke migration;
7. whole-project verification and architecture review.

No phase may retain a second public terminal DSL. Temporary internal compilation breakage is acceptable inside the feature worktree, but the final branch must expose only the unified contract.
