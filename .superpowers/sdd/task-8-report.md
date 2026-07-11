# Task 8 Report: Sequential Ready-Queue Scheduler

## Status

Implemented the single-owner scheduler cutover. `RunCoordinator` now owns Run lifecycle and terminal publication while `Scheduler` owns graph advancement, its FIFO ready queue, node/branch state maps, and a `JoinSet` of node tasks.

## TDD Evidence

### RED 1: scheduler parity API

Command:

```text
cargo test --test run_scheduler sequential -- --nocapture
```

Observed exit 101. Compilation failed on unresolved imports `runtime::Scheduler` and `runtime::SchedulerResult`, proving the parity tests required the new scheduler boundary.

### GREEN 1: sequential parity

The same command passed 2 tests:

```text
sequential_scheduler_goto_never_executes_unselected_path ... ok
sequential_scheduler_preserves_path_context_output_and_node_event_order ... ok
test result: ok. 2 passed; 0 failed
```

This covers `prepare -> route -> answer -> result`, predecessor visibility, exact `RunOutput`, one execution per selected node, exact node-event order, and zero executions of the unselected `Goto` target.

### RED 2: typed fork boundary

Command:

```text
cargo test --test run_scheduler sequential -- --nocapture
```

Observed exit 101. The valid compiled fork test received `SCHEDULER_INVARIANT_VIOLATION` instead of the required `SCHEDULER_FORK_UNSUPPORTED`; the other four sequential scheduler tests passed.

### GREEN 2: scheduler defensive contract

The same command passed all 5 sequential scheduler tests. It verifies duplicate activation and missing targets map to infrastructure `SCHEDULER_INVARIANT_VIOLATION`, while a validated compiled fork maps to infrastructure `SCHEDULER_FORK_UNSUPPORTED`.

## Focused Verification

```text
cargo test --test run_scheduler -- --nocapture
11 passed; 0 failed

cargo test --test run_coordinator --test run_service --test api
22 passed; 0 failed

cargo test --test core_template_condition --test core_chat_action --test core_output
17 passed; 0 failed
```

The coordinator set retains exact sequential output/event order, node failure ordering, typed cancellation/interruption/timeout handling, infrastructure recovery, and terminal-state ownership.

## Full Verification

Command:

```text
cargo test
```

Observed exit 0: all 157 integration tests passed, with 0 failures and no compiler warnings.

## Files

- `src/runtime/scheduler.rs`: scheduler boundary, FIFO ready queue, `JoinSet`, finite-state ownership, typed results, activation validation.
- `src/runtime/state.rs`: `NodeState` and `BranchState` finite states.
- `src/runtime/coordinator.rs`: delegates graph advancement and retains Run lifecycle/terminal publication.
- `src/runtime/mod.rs`: scheduler and state exports.
- `tests/run_scheduler.rs`: deterministic sequential parity, `Goto`, invariant, and fork-boundary tests.
- `tests/run_coordinator.rs`: names the coordinator lifecycle-ownership integration contract.

## Self-Review

- Only the scheduler loop mutates ready/state maps; spawned node tasks receive owned inputs and return typed execution results.
- FIFO activation plus state-transition guards preserve sequential ordering and prevent a node from executing twice.
- The completed node output is inserted into its returned context before the selected successor is activated.
- `Goto` activates only its selected target; the ordinary `next` target remains pending and never executes.
- `execute_node` remains the only `node.failed` publisher; the scheduler emits no events directly.
- `RunCoordinator` remains the only owner of completed/failed/cancelled/interrupted Run terminal events and infrastructure recovery.
- Infrastructure failures remain `Err(RunError)` across the scheduler boundary; typed node and stop failures use `SchedulerResult`.
- `git diff --check` passed.

## Concerns

`ActivateFork` intentionally returns `SCHEDULER_FORK_UNSUPPORTED` after validating that its compiled fork plan exists. Branch activation and branch-state transitions are deliberately deferred to Task 9.
