# Task 9 Report: Concurrent Fork Branches and All-Settled Join

## Status

Implemented concurrent execution of compiler-produced fixed branch regions. The scheduler now owns one active fork, admits every branch in deterministic branch-ID order, executes ready branch nodes through the existing `JoinSet` and node semaphores, settles branch-local node failures as sanitized data, and activates the paired join exactly once after every branch is terminal.

## TDD Evidence

### RED

Command:

```text
cargo test --test run_scheduler parallel -- --nocapture
```

Observed exit 101 with all six initial parallel tests failing specifically at infrastructure `SCHEDULER_FORK_UNSUPPORTED`. The barrier tests fail-fast if the scheduler exits before branch activation, so the RED run did not rely on elapsed-time overlap assertions.

The RED suite covered compiler-produced two-branch overlap, ten-branch backpressure, branch-local `Goto`, partial failure, all-failed continuation, and node-timeout isolation.

### GREEN

The final focused command passed all eight parallel tests:

```text
cargo test --test run_scheduler parallel -- --nocapture
8 passed; 0 failed
```

The two additional tests prove typed Stop and Infrastructure failures remain global and are never serialized into `BranchResult`.

## Deterministic Concurrency Evidence

- The primary fixture is compiled through `AgentCompiler`; only deterministic synthetic executor bodies replace selected compiled node bodies. `ExecutionPlan`, fork/join pairing, branch entries, node regions, and branch node sets are compiler-produced.
- Both branch entries increment an atomic in-flight counter, register a pinned `Notify::notified()` waiter with `enable()`, and wait on an explicit release flag/notification. The test observes the condition `in_flight == 2` before release and records `max_in_flight == 2`.
- Each of `search_a`, `summarize_a`, `search_b`, and `summarize_b` executes exactly once. Each summarizer sees its own search output and asserts the sibling search output is absent.
- The ten-branch fixture uses the same condition-based gate with per-Run capacity three. It observes exactly three concurrent entries, releases them, then proves all ten execute once and the maximum remains three.
- The branch-local `Goto` fixture compiles two possible targets in one branch and proves only the selected target executes; the other remains unadmitted.

## All-Settled Results and Failure Boundary

- Successful branches store only terminal node ID and terminal output.
- Node failures and node timeouts settle only their branch, skip its successors, and store only `RunError::code()` and `RunError::message()`.
- Partial failure produces the exact stable join envelope with one success and one `UPSTREAM_FAILURE`; the failed branch's summarizer executes zero times.
- Both failed branches still activate `collect`, whose output reports `total: 2`, `succeeded: 0`, `failed: 2`; the post-join output executes once.
- Stop and Infrastructure failures return through the existing global scheduler paths and publish no `branch.failed` result for the affected branch. Task 10 still owns explicit sibling fan-out/drain hardening.

## Event and SSE Ordering

Stored scheduler events prove:

```text
fanout node.completed
< branch.started
< branch node.completed|node.failed
< branch.completed|branch.failed
< collect node.started
< collect node.completed
```

All sequences are unique and contiguous. Both sibling `content.delta` node IDs occur in the shared ordered stream. `branch.started` means ready-queue activation, before node-permit acquisition.

The detached API fixture compiles a real parallel Agent, waits for durable terminal completion, selects the first stored `branch.started` sequence as the cursor, reconnects to `/v1/runs/{run_id}/events?after_seq=...`, and compares every replayed `(seq, type, node_id)` tuple with repository history. Remaining branch terminal events and `collect` start/completion are present in the same increasing order.

## Verification

```text
cargo test --test run_scheduler parallel -- --nocapture
8 passed; 0 failed

cargo test --test fork_join_nodes --test event_hub
22 passed; 0 failed

cargo test --test run_coordinator --test run_service --test api
23 passed; 0 failed

cargo fmt --check
cargo clippy --all-targets -- -D warnings
git diff --check
all exited 0

cargo test
165 integration tests passed; 0 failed; no compiler warnings
```

## Files

- `src/runtime/scheduler.rs`: active-fork ownership, deterministic branch activation, branch-scoped advancement, sanitized settlement, all-settled join admission, and branch events.
- `tests/run_scheduler.rs`: compiler-backed deterministic overlap/barrier, capacity, isolation, `Goto`, partial/all-failed, timeout, Stop, Infrastructure, exact-once, and event-order tests.
- `tests/api.rs`: compiler-backed detached parallel Agent and repository-to-SSE replay parity.
- `.superpowers/sdd/task-9-report.md`: RED/GREEN and verification evidence.

`RunContext`, `BranchResult`, and `BranchState` already exposed the required immutable fork/join layering and finite states, so no production changes were needed in `context.rs` or `state.rs`. Existing `fork_join_nodes` tests already cover the exact join envelope and context freeze semantics; scheduler integration now exercises those contracts end to end.

## Self-Review

- Only the scheduler task mutates node states, branch states, ready nodes, active-fork results, or join admission. Spawned tasks return owned execution results.
- Branches receive separate `main_context.fork_branch()` values. Branch-local outputs are never merged into main or sibling contexts.
- A branch successor is admitted only when it belongs to that compiled branch region; selecting the paired join settles the branch instead of running the join early.
- Node-state transition guards prevent duplicate execution. Branch-state/result guards prevent duplicate settlement. The final settlement is the sole point that changes the join from Pending to Ready.
- Fork completion is durable before branch events because `execute_node` persists `node.completed` before returning `ActivateFork`. Branch terminal events are awaited before join admission.
- Join output is written into the main/join context and becomes visible to post-join nodes; branch maps remain encapsulated in immutable join results.
- Failure serialization never uses `Debug`, error sources, provider bodies, or backtraces.
- Formatting, Clippy with warnings denied, diff whitespace checks, focused matrices, and the full suite are clean.

## Concerns

Task 10 remains responsible for explicitly signalling and draining sibling tasks on global Stop or Infrastructure failure. Task 9 preserves their global classification and never captures them as branch data; dropping the current `JoinSet` still aborts remaining tasks on scheduler exit.

## Review Fixes

Review identified that successful join execution advanced with the temporary join context, leaving scheduler-owned `BranchResult` visible through `RunContext::branch_results()` to downstream custom executors.

### RED

The downstream test executor was strengthened to require both conditions:

```text
context.branch_results().is_none()
context.node_output("collect") == Some(expected_join_output)
```

Command:

```text
cargo test --test run_scheduler parallel -- --nocapture
```

Observed exit 101: both the full-success and partial-failure downstream executors panicked specifically because `context.branch_results().is_none()` was false. The other seven parallel tests passed.

### GREEN

After successful join execution, the scheduler now takes the matched `ActiveFork`, rebuilds the continuation context from its saved main context, inserts only the already-persisted join node output, and then advances. The temporary join-results context is no longer observable downstream, and clearing the active fork at this boundary permits later compiled forks.

Coverage was also strengthened to prove:

- `branch.started` is emitted in deterministic branch-ID order;
- for every successful branch, `fanout node.completed < branch.started < terminal node.completed < branch.completed < collect node.started`;
- for a failed branch, `branch.started < node.failed < branch.failed < collect node.started`;
- a compiler-produced `fork_a -> join_a -> fork_b -> join_b -> result` graph completes, each branch/result node executes once, and both joins start exactly once.

Final review verification:

```text
cargo test --test run_scheduler parallel -- --nocapture
9 passed; 0 failed

cargo test --test run_scheduler --test api
26 passed; 0 failed

cargo fmt --check
cargo clippy --all-targets -- -D warnings
git diff --check
all exited 0

cargo test
166 integration tests passed; 0 failed; no compiler warnings
```
