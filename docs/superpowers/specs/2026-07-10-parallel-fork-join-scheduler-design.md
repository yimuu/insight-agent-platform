# Explicit Fork/Join Parallel Scheduler Design

**Date:** 2026-07-10  
**Status:** Approved for implementation planning
**Scope:** Fixed parallel subgraphs with `all_settled` join semantics

## 1. Context

Formal V1 compiles a strict Agent DSL into an immutable DAG, but the runtime currently interprets that graph with one `current` cursor. It supports sequential execution, conditional routing, and a single successful output path. The model is deterministic and easy to reason about, but it cannot execute independent model or Action calls concurrently.

The first workflow-expressiveness extension will support fixed, named parallel branches. Each branch may contain multiple nodes and conditions. Branches settle independently, and a join exposes both successful outputs and sanitized failures to downstream nodes.

This is a runtime-kernel capability, not a low-code product feature. The implementation should establish a scheduler foundation that can later support `foreach`, wait/resume, and subflows without implementing those features now.

## 2. Goals

- Add explicit `core.fork` and `core.join` nodes.
- Allow a fork to activate multiple fixed, named branch subgraphs concurrently.
- Allow each branch to contain multiple existing node types and conditional paths.
- Wait until every branch is settled before running the paired join.
- Treat ordinary branch failures as `all_settled` data while preserving successful sibling work.
- Preserve the current Run lifecycle, durable event ordering, SSE replay, explicit cancellation, and one-terminal-state guarantees.
- Keep existing sequential Agent DSL valid and behaviorally unchanged.
- Replace the one-cursor graph driver with a bounded ready-queue scheduler that can be extended deliberately.

## 3. Non-goals

- Nested fork regions.
- Dynamic `foreach` or map/reduce expansion.
- Cycles or loops.
- Wait nodes, external signals, or human-in-the-loop suspension.
- Process-restart continuation of active execution graphs. Startup reconciliation still marks incomplete Runs `interrupted`.
- `any`, `quorum`, or `fail_fast` join policies.
- Subflows.
- Multi-tenant identity, RBAC, billing, or tenant-scoped administration.
- A visual workflow editor.

The runtime remains an internal execution service. A future control plane may select tenant-authorized Agents, models, and Actions and wrap repository access with tenant scope, but tenant concepts do not enter the DSL or scheduler.

## 4. Chosen Approach

Use explicit top-level fork and join nodes and introduce a finite ready-queue scheduler.

Two alternatives were rejected:

1. A composite `core.parallel` node containing nested branch DSL would be quicker to add, but it would create a second graph model inside one node and force future `foreach`, wait, and subflow features to reimplement scheduling internally.
2. Treating every multi-edge node as an implicit parallel split would make conditional and parallel edges ambiguous and hide join behavior.

Explicit fork/join has a higher initial implementation cost, but it gives parallel activation and synchronization named, compilable semantics. It also maps naturally to a future visual graph without making every outgoing edge parallel.

## 5. DSL Contract

The formal syntax is:

```yaml
fanout:
  type: core.fork
  config:
    branches:
      source_a: search_a
      source_b: search_b
    join: collect

search_a:
  type: core.action
  next: summarize_a
  config: { ... }

summarize_a:
  type: core.chat
  next: collect
  config: { ... }

search_b:
  type: core.action
  next: summarize_b
  config: { ... }

summarize_b:
  type: core.chat
  next: collect
  config: { ... }

collect:
  type: core.join
  next: synthesize
  config:
    mode: all_settled
```

### 5.1 `core.fork`

- Requires at least two named branches.
- Branch IDs use the same identifier grammar as node IDs and are unique within the fork.
- Each branch maps to an existing entry node.
- Requires one paired join node ID.
- Forbids envelope `next` because the named branches are its outgoing control flow.
- Forbids `emit: content`.
- The number of branches must not exceed `runtime.max_fork_branches`.

### 5.2 `core.join`

- Requires `mode: all_settled`; no other mode is accepted in this milestone.
- Requires envelope `next`.
- Forbids `emit: content`.
- Must be paired with exactly one fork.
- Executes once, after every paired branch is settled.

### 5.3 Branch-region rules

- Branch nodes remain in the top-level `nodes` map and keep globally unique node IDs.
- A branch may contain ordinary nodes and `core.condition` nodes.
- Every non-failing path from a branch entry must reach the paired join.
- Branch regions must be vertex-disjoint before the join.
- A branch cannot enter another branch, escape to post-join nodes, bypass the join, or contain another fork.
- Only the paired branch regions may enter the join.
- The post-join graph follows the existing acyclic, reachable, output-terminated rules.

## 6. Compilation and Execution Plan

`CompiledAgent` gains an immutable `ExecutionPlan` alongside compiled nodes. It contains ordinary edges plus explicit fork-region metadata:

```rust
struct ExecutionPlan {
    entry: String,
    forks: BTreeMap<String, ForkPlan>,
    node_regions: BTreeMap<String, NodeRegion>,
}

struct ForkPlan {
    fork_id: String,
    join_id: String,
    branches: BTreeMap<String, BranchPlan>,
    policy: JoinPolicy,
}

struct BranchPlan {
    branch_id: String,
    entry: String,
    nodes: BTreeSet<String>,
}
```

Exact field visibility may differ, but the compiled plan must make branch membership and fork/join pairing immutable and queryable without rediscovering topology at runtime.

The compiler will:

1. Resolve all node types and ordinary edges as today.
2. Compile fork and join node bodies through the static node registry.
3. Locate each branch region from its declared entry to the declared join.
4. Reject cross-region edges, overlapping regions, bypasses, nested forks, missing joins, and joins paired with multiple forks.
5. Validate the complete DAG for cycles, reachability, and terminal output paths.
6. Validate references using region-aware dominance rules.

### 6.1 Reference rules

- A branch node may reference pre-fork nodes that dominate the fork.
- A branch node may reference guaranteed predecessors in the same branch.
- It may not reference another branch.
- Post-join nodes may continue referencing pre-fork dominators.
- Post-join nodes may not reference internal branch node outputs directly. They must consume the paired join output.

The last rule ensures that partial failure never creates a missing template root downstream.

## 7. Runtime Scheduler

The coordinator remains responsible for Run creation, transition to `running`, cancellation ownership, infrastructure recovery, and the unique durable terminal state. Graph advancement moves into a scheduler.

Suggested runtime boundaries:

```text
src/runtime/
  coordinator.rs   Run lifecycle and terminal ownership
  scheduler.rs     ready queue, node/branch state, task collection
  execution.rs     one node execution and timeout boundary
  state.rs         Run, node, and branch state types
```

Node state is finite:

```text
pending -> ready -> running -> succeeded
                           \-> failed
pending/ready -------------> skipped (global cancellation only)
```

Branch state is:

```text
pending -> running -> succeeded|failed
```

One scheduler task owns all state transitions and ready-queue mutations. Node execution futures run concurrently, but they return outcomes to the scheduler; they do not mutate scheduler state. This single-owner model prevents join activation races and duplicate node execution.

### 7.1 Scheduler flow

1. Activate the Agent entry node.
2. Execute ordinary nodes exactly as today and activate the selected `Next` or `Goto` target.
3. When a fork succeeds, emit its node completion, mark each branch running, emit `branch.started`, and enqueue every branch entry. Here, `branch.started` means the branch has been activated and admitted to its ready queue; it does not guarantee that the first node has acquired an execution permit.
4. Within a branch, activate at most one successor for that branch at a time. Different branches may run concurrently.
5. When a successful node selects the paired join, settle that branch as succeeded instead of scheduling the join immediately.
6. When a branch-local node fails, settle the branch as failed and do not activate its remaining path.
7. Once all branches settle, enqueue the join exactly once.
8. Persist the join output, then continue through its ordinary `next` edge.

Fork and join compile through the node registry. The scheduler does not compare node-kind strings. Control executors return typed scheduling directives, and the compiled `ExecutionPlan` supplies synchronization metadata.

## 8. Context Isolation

Parallel branches must not share mutable `RunContext` node-output maps.

At fork time, the scheduler freezes the pre-fork context as a read-only base. Each branch receives a layered context:

```text
BranchContext
  read-only pre-fork input/run/node outputs
  + branch-local completed node outputs
```

A branch cannot observe sibling outputs while running. Successful node outputs are still persisted under their global node IDs, but they are not merged into the outer template context.

The join receives scheduler-owned `BranchResult` values. Only the persisted join output is added to the outer context. This prevents data races and gives downstream templates one stable partial-success contract.

## 9. Join Output Contract

`core.join` produces:

```json
{
  "branches": {
    "source_a": {
      "status": "succeeded",
      "terminal_node_id": "summarize_a",
      "output": {"text": "result a"}
    },
    "source_b": {
      "status": "failed",
      "terminal_node_id": "search_b",
      "error": {
        "code": "UPSTREAM_FAILURE",
        "message": "upstream service failed"
      }
    }
  },
  "summary": {
    "total": 2,
    "succeeded": 1,
    "failed": 1
  }
}
```

- A succeeded branch exposes the output of the node whose successor was the join.
- A failed branch exposes the failed node and a stable, sanitized error.
- A failed branch has no fabricated output.
- Successful intermediate branch outputs remain in history but are not copied into the join envelope.
- The join succeeds even if every branch failed. Downstream condition or output nodes decide whether to degrade, report failure details, or deliberately fail through a later node.
- Branch keys are serialized deterministically by branch ID. Consumers must not infer execution order from object order.

Downstream access uses the join node:

```handlebars
{{ nodes.collect.output.branches.source_a.output.text }}
```

## 10. Failure Classification

Branch-local failures are data under `all_settled`:

- Model or Action failures.
- Node-level timeouts.
- Runtime template rendering or condition evaluation failures.
- Other ordinary `RunError` values returned by a node executor.

The scheduler emits `node.failed`, then `branch.failed`, stops that branch, and lets siblings continue.

Run-fatal failures are never captured as branch data:

- Explicit Run cancellation.
- Attached reconnect-grace expiration.
- Whole-Run timeout.
- Journal, repository, or event-sequence failure.
- Scheduler invariant violation.
- Spawned task panic or unexpected task cancellation.

These failures request cancellation for every running branch, prevent further ready nodes from starting, mark pending/ready work skipped internally, and use the existing coordinator recovery path to commit one Run terminal state.

The implementation must classify failures by where they occur, not by matching arbitrary error-code strings. Executor results are branch-local; persistence, journal, and scheduler failures are infrastructure-level.

Content already emitted by a branch is not retracted if that branch later fails. Clients use `branch.failed` and the join output as the authoritative branch result.

## 11. Cancellation and Timeouts

- Every running node receives the existing cooperative stop signal.
- Explicit cancellation and shutdown broadcast the same Run stop reason to all node executions.
- Node-level timeout fails only its branch when inside a fork region.
- Whole-Run timeout remains fatal.
- The scheduler stops admitting ready work after a global stop is observed.
- The coordinator remains the only component allowed to commit Run terminal state.

Process restart behavior does not change. Scheduler state is not restored from history; incomplete Runs become `interrupted` at startup.

## 12. Concurrency and Backpressure

Add strict positive runtime settings:

```yaml
runtime:
  max_fork_branches: 32
  max_parallel_node_executions: 32
  max_parallel_branches_per_run: 8
```

- `max_fork_branches` is a compile-time structural bound per fork.
- `max_parallel_node_executions` is a process-wide semaphore shared by all Runs.
- `max_parallel_branches_per_run` prevents one Run from occupying the global pool.
- A fork may declare more branches than the per-Run execution limit, up to `max_fork_branches`; excess branch entries remain ready and start as permits become available.
- Existing `max_concurrent_runs` continues to bound active Runs independently.
- Sequential node executions also consume one global node-execution permit.

Fork and join control work follows the same node lifecycle, although their permit holding time should be negligible. Providers and Actions may continue enforcing narrower component-specific limits.

## 13. Events and Persistence

Add formal event types:

```text
branch.started
branch.completed
branch.failed
```

Branch events are run-level events with no `node_id`. Their `data` contains `fork_id`, `branch_id`, and, for terminal branch events, `terminal_node_id`; failures include the sanitized stable error.

Internal branch nodes retain the existing node events and globally unique node IDs. `content.delta` events from different branches may interleave.

Required durable ordering per branch is:

```text
node output
-> node.completed
-> branch.completed
```

Required fork/join ordering is:

```text
fork node.completed
-> branch.started...
-> every branch.completed|branch.failed
-> join node.started
-> join output
-> join node.completed
```

EventHub remains the sole sequence allocator. Concurrency may change which sibling event receives the next sequence, but all events retain one durable, gap-free Run sequence and the existing SSE replay contract.

No branch-state table is added in this milestone. Node outputs, branch events, join output, and the Run terminal record are sufficient for audit and replay because active scheduler restoration is explicitly out of scope.

## 14. API and Compatibility

HTTP routes, Run creation modes, Run lookup, explicit cancellation, and SSE replay remain unchanged.

Existing sequential Agent YAML requires no changes. It compiles to an execution plan with no fork regions, and the scheduler normally has one ready node, preserving current behavior.

Formal internal interfaces change for a concrete reason:

- `CompiledAgent` gains `ExecutionPlan` because topology and synchronization must be compiled, not rediscovered during execution.
- `NodeTransition` evolves into typed scheduler directives because one target is no longer sufficient.
- Runtime configuration gains explicit concurrency limits because parallel execution without bounded admission is unsafe.
- The event protocol gains branch lifecycle types because node events alone cannot express an `all_settled` branch result.

These are additive HTTP/Event behaviors and deliberate internal Rust API changes. No compatibility adapter is required for internal traits.

## 15. Test Strategy

### 15.1 DSL and compiler

- Valid two-branch and multi-node fork/join graphs.
- Missing or wrong-kind join.
- Empty branches and duplicate branch IDs.
- Missing branch entries.
- Cross-branch, overlapping, bypass, and post-join escape edges.
- Nested fork rejection.
- Branch conditions whose every path does or does not reach the join.
- Cross-branch and post-join direct reference rejection.
- Existing sequential agents compile unchanged.

### 15.2 Scheduler

- Deterministic synchronization proves two branch nodes overlap in execution.
- Global and per-Run permits bound concurrency.
- Excess fixed branches wait and eventually start.
- Join never starts before all branches settle.
- A node executes at most once.
- Conditions select only one path inside their branch.
- Sequential graphs retain current ordering and output.

Tests should use barriers and notifications rather than wall-clock timing assertions.

### 15.3 Partial failure

- One successful and one failed branch.
- All branches failed while join still succeeds.
- Node timeout settles one branch and leaves siblings running.
- A failed branch does not schedule downstream branch nodes.
- Join success/error envelopes and summaries are exact and sanitized.

### 15.4 Global failure and cancellation

- Explicit cancel reaches all running branches.
- Ready work does not start after global cancellation.
- Whole-Run timeout is fatal.
- Journal and repository failures do not become branch results.
- Task panic is infrastructure-fatal.
- Terminal races still persist exactly one terminal event and state.

### 15.5 Events and repositories

- Concurrent events receive unique, contiguous sequences.
- Branch lifecycle events replay by `after_seq`.
- Join events occur after all branch terminal events.
- Join output and branch node outputs persist correctly.
- SQLite and PostgreSQL satisfy the same contract.

## 16. Acceptance Criteria

The milestone is complete when:

1. A checked-in Agent contains two fixed, multi-node branches that demonstrably execute concurrently.
2. One branch can fail while its sibling completes.
3. The join waits for all branches and produces the exact stable `all_settled` envelope.
4. Cancellation and infrastructure failure preserve one durable Run terminal state.
5. Branch and node events replay with a contiguous Run sequence.
6. Existing sequential Agents require no DSL changes and preserve their results.
7. Format, strict Clippy, all targets, SQLite, real PostgreSQL, audit, and deny gates pass.
