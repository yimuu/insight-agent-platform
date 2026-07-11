# A2 Preparing/Active Lifecycle Ownership Design

**Status:** Approved for written spec in conversation on 2026-07-11; written-spec review pending.

## Context

The stable-baseline review identifies `BASE-P1-006`: shutdown can miss a Run after it has passed admission but before it is inserted into the active map. Today `RunService::prepare_run` checks admission, acquires run capacity, then awaits durable work such as `repository.create_run(...)` and `events.open_run(...)`. Only after `create_attached` or `create_detached` returns from preparation does `RunService::launch` insert the Run into `active`.

`RunService::shutdown` closes admission and waits only for `active`. If shutdown begins while a request is between admission and active insertion, the service can report shutdown complete while a handler later launches work or leaves a durable `created` Run for startup reconciliation. This is an internal lifecycle race; it does not require public HTTP, event, repository, or DSL shape changes.

## Goals

- Register an owned lifecycle record for every admitted Run before the first durable await.
- Make shutdown drain both preparing and active ownership.
- Make cancellation of the caller future safe at every preparation await point.
- Preserve attachment-specific terminal semantics:
  - Attached preparing Runs stopped by shutdown or subscription drop become `Cancelled` with `RUN_CANCELLED`.
  - Detached preparing Runs stopped by shutdown become `Interrupted` with `RUN_INTERRUPTED`.
- Never launch scheduler work across a closed shutdown epoch.
- Release run capacity, EventHub state, and lifecycle maps exactly once on every preparing/active path.
- Provide deterministic tests for each preparation await boundary and launch boundary.

## Non-goals

- Changing public HTTP, SSE, event, Run, repository, DSL, or migration shapes.
- Adding a new public cancellation API or changing detached/attached endpoint contracts.
- Implementing A3 authoritative stop semantics for unbacked custom-node stop errors.
- Implementing A4 durable recovery supervisor or post-commit EventHub isolation changes.
- Adding metrics, tracing fields, or observability output.
- Changing startup reconciliation behavior for Runs left by a crashed older process.

## Selected approach

Use two internal ownership maps in `RunServiceInner`:

```rust
preparing: Mutex<BTreeMap<String, PreparingRun>>,
active: Mutex<BTreeMap<String, ActiveRun>>,
lifecycle_changed: watch::Sender<u64>,
```

`preparing` records an admitted Run that owns capacity and may already have a durable row, but does not yet have a scheduler task. `active` continues to represent lifecycle entries backed by a spawned task. That task can be either normal scheduler execution or a short finalizer that writes a terminal state for a stopped preparing Run. Keeping the states separate avoids pretending that a preparing Run already has executable work while still giving shutdown a complete drain target.

### Preparing ownership

`RunService::prepare_run` creates the run ID, stop pair, `RunState`, and capacity permit before the first durable await. It then inserts a `PreparingRun` under `preparing`.

The preparing entry owns:

- `NewRun`
- `RunAttachment`
- `StopController` and `StopSignal`
- `Arc<RunState>`
- `OwnedSemaphorePermit`
- agent and input needed for launch
- a durable flag that becomes true after `repository.create_run(...)` succeeds

Every insertion, removal, or promotion sends `lifecycle_changed`.

The caller future is also represented by a local preparation guard. Dropping that guard without promotion is a real lifecycle event, because Rust may cancel an async request future at any await point. The guard must synchronously remove a non-durable preparing entry, or move a durable preparing entry to an active finalizer task. This prevents capacity, EventHub, or map leakage when an HTTP request is dropped before `create_attached` or `create_detached` reaches `launch`.

If `repository.create_run(...)` fails before durability exists, preparation removes the entry, drops the permit, and returns the existing service history error.

If durability exists and the stop signal is already set before launch, the service must not run the scheduler. It writes a terminal state based on the stop reason and removes the preparing entry.

### Promotion to active

`launch` becomes an internal promotion operation rather than the first owner registration point. It removes the matching entry from `preparing` and then does one of two things:

1. If admission is still open and the stop signal has no reason, it inserts an `ActiveRun` and spawns the existing scheduler execution task.
2. If shutdown or subscription release already requested stop, it inserts an `ActiveRun` backed by a finalizer task. The finalizer writes the attachment-specific terminal state and then removes the active entry.

This makes the handoff atomic from the shutdown perspective: a Run is either preparing, active, or removed; there is no untracked gap.

### Shutdown behavior

`RunService::shutdown(deadline)`:

1. sets `accepting = false`;
2. snapshots both `preparing` and `active`;
3. requests an attachment-specific stop reason for every snapshot entry:
   - `Attached` -> `StopReason::Cancelled`
   - `Detached` -> `StopReason::Interrupted`
4. waits until both maps are empty, bounded by the passed shutdown deadline.

If a preparing await is blocked longer than the shutdown deadline, shutdown returns `SHUTDOWN_TIMEOUT`. This is intentional: reporting successful shutdown while an admitted Run is still owned by the process is incorrect.

### Attached subscription release during preparation

`SubscriptionLease::drop` calls `LeaseOwner::release_subscription`. That implementation must check both maps:

- If the Run is active and attached, request `Cancelled` as today.
- If the Run is preparing and attached, request `Cancelled`; if it later reaches promotion, it must be finalized instead of launched.

This preserves the live-only SSE decision: an attached client disconnect cancels the Run immediately, even if the request is still in the preparation window.

### Terminalizing a stopped preparing Run

Terminalizing a stopped preparing Run uses the same durable/public semantics as scheduler-driven stop completion:

| Stop reason | Status | Event type | Error code | Error message |
|---|---|---|---|---|
| `Cancelled` | `cancelled` | `run.cancelled` | `RUN_CANCELLED` | `run cancelled` |
| `Interrupted` | `interrupted` | `run.interrupted` | `RUN_INTERRUPTED` | `run interrupted` |
| `TimedOut` | `failed` | `run.failed` | `RUN_TIMEOUT` | `run timed out` |

`TimedOut` is included for completeness and parity with existing `RunError::stopped`, but A2's primary entry points are shutdown and attached disconnect.

The terminal path should reuse a small internal helper rather than duplicating status/event/code mapping in multiple branches. That helper must use EventHub's existing durable publication path, so terminal writes and live terminal broadcasting stay consistent with existing coordinator behavior.

If a preparing Run already has a durable row but never entered coordinator execution, the finalizer publishes `run.created` before the terminal event, then skips `run.started`. This keeps the public event sequence coherent without pretending the Run ever reached `running`:

```text
run.created -> run.cancelled | run.interrupted | run.failed
```

### EventHub state

Preparing still calls `events.open_run(&run_id)` before returning to attached/detached handlers. If the Run is stopped before launch and terminalized during preparation or promotion, the terminal event removes the EventHub run state through existing terminal cleanup.

A2 does not change EventHub's public API or replay behavior. Any deterministic test hook needed to hold `open_run` or `subscribe` must remain private to tests and must not appear in platform configuration.

### Capacity and active cleanup

The run capacity permit is owned by exactly one lifecycle entry:

- initially by `PreparingRun`;
- moved into `ActiveRun` during normal promotion or finalizer promotion;
- dropped when the active or preparing entry is removed.

The implementation must not clone, leak, or temporarily drop the permit before terminalization. Capacity tests must prove that a stopped preparing Run releases capacity and a subsequent Run can be admitted.

## Error and compatibility contract

A2 changes internal ordering only. It keeps:

- existing HTTP status mapping;
- existing `/v1` route set;
- existing event types and event payload shapes;
- existing Run status values and error codes;
- existing repository trait and migration shapes;
- existing startup reconciliation contract.

The only observable behavior change is safer shutdown/disconnect handling for a previously racy window. A Run that previously could launch after shutdown or survive as `created` will now be stopped and terminalized according to its attachment.

## Test strategy

Use deterministic barriers instead of timing-sensitive sleeps. Tests may add private test-only hooks or test repositories, but no production configuration knob.

Required focused tests:

1. **Shutdown while `create_run` is blocked**
   - Arrange a detached Run admitted into `preparing` with `repository.create_run` held.
   - Start shutdown and assert it does not complete while preparation is blocked.
   - Release the barrier and assert the Run is either not durable if create failed, or terminalized as `Interrupted` if create succeeded.
   - Assert maps are empty and capacity is released.

2. **Shutdown after durable create before launch**
   - Hold a later preparation boundary after `create_run` succeeds.
   - Start shutdown.
   - Assert the Run is not launched, then assert durable status `Interrupted`, event sequence `run.created -> run.interrupted`, and no active scheduler execution occurred.

3. **Attached disconnect before launch**
   - Create an attached Run and hold the boundary between subscription creation and launch.
   - Drop the subscription.
   - Release the boundary and assert durable status `Cancelled`, event sequence `run.created -> run.cancelled`, no scheduler execution, and EventHub run state cleanup.

4. **Detached `get_run` window before launch**
   - Hold `repository.get_run` inside `create_detached` after durable create/open.
   - Start shutdown.
   - Release `get_run` and assert `launch` finalizes instead of running scheduler.

5. **Dropped caller future during preparation**
   - Drop an attached or detached create future while it is blocked at a preparation await.
   - If no durable row exists, assert the preparing entry and permit are removed.
   - If a durable row exists, assert the finalizer terminalizes it and releases capacity.

6. **Shutdown drains both preparing and active**
   - Run one active blocking attached Run and one preparing detached Run.
   - Shutdown must request `Cancelled` for the attached Run and `Interrupted` for the detached Run, then return only after both lifecycle maps are empty.

7. **Admission after shutdown**
   - Once shutdown closes admission, new `create_attached` and `create_detached` calls fail with `RUN_SERVICE_STOPPING` before acquiring capacity or creating durable rows.

8. **No public-shape drift**
   - Existing API, protocol, repository, migration, and scheduler tests continue to pass.

## Rollout and rollback

A2 requires no data migration and no history reset. Rolling forward changes only the process-local ownership model. Rolling back restores the previous racy behavior; existing terminalized Runs remain valid because A2 uses existing status and event contracts.

## Acceptance checklist

- No path exists between capacity acquisition and active insertion where shutdown cannot see the Run.
- Shutdown waits for `preparing` and `active` maps to empty.
- Dropped create futures cannot leak preparing entries or capacity.
- Attached preparing disconnect cancels before launch.
- Detached preparing shutdown interrupts before launch.
- A stopped preparing Run never starts scheduler execution.
- Pre-launch terminalization publishes `run.created` and exactly one terminal event, but not `run.started`.
- Durable terminal writes use existing event/repository terminal paths.
- Capacity and EventHub state are released on all success, error, cancellation, and shutdown paths.
- Public HTTP/SSE/event/Run/repository shapes, migrations, dependency graph, A3, and A4 remain unchanged.
