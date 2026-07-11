# A3 Authoritative Stop Semantics Design

**Status:** Design direction approved in conversation on 2026-07-11; written-spec review pending.

## Context

The stable-baseline review identifies `BASE-P1-007`: a node executor can return `RunError::stopped(...)` without the runtime's shared stop signal having a reason, or can return a stop reason different from the shared first-winner reason. Today `execute_node_inner` publishes a node failure event using the returned error and then classifies `RunErrorKind::Stop` as `NodeExecutionFailure::Stop`. `Scheduler` returns `SchedulerResult::Stopped(error)` and drains sibling tasks without setting the shared stop signal.

That means an extension can accidentally or intentionally manufacture stop authority. In a parallel run, an unbacked stopped error can make the scheduler wait for siblings that never observe a stop signal, while a mismatched stopped error can make durable terminal status disagree with the reason observed by other branches.

A3 is an internal runtime hardening milestone. It does not change public HTTP, SSE, event envelope, repository, DSL, migration, or node-executor method signatures.

## Goals

- Make `StopSignal::reason()` the only authoritative source of Run stop reason.
- Normalize executor-returned stopped errors to the shared first-winner reason when that reason exists.
- Treat executor-returned stopped errors without a shared reason as infrastructure failures.
- Prevent fabricated or mismatched stop reasons from controlling durable Run terminal status.
- Promptly cancel/drain sibling execution wrappers when an unbacked stopped error appears.
- Preserve existing external cancellation, shutdown interruption, run timeout, and attached disconnect semantics.
- Keep extension signatures source-compatible while tightening the semantic contract.

## Non-goals

- Changing `RunError`, `RunErrorKind`, `StopReason`, `ExecutionControl`, or `NodeExecutor` public type signatures.
- Adding a public self-cancel API for nodes.
- Changing node-level `RunError::timeout()` behavior; node timeout remains a node failure, not a typed Run stop.
- Changing A2 preparing/active lifecycle ownership.
- Implementing A4 durable recovery ownership or EventHub post-commit isolation.
- Adding metrics or observability fields.

## Selected approach

Normalize stopped errors at the node execution boundary, before any node failure event is published and before the scheduler sees the failure.

`execute_node_inner` already owns both values needed for the decision:

- the executor-returned `RunError`;
- the shared `ExecutionControl`, backed by the Run's `StopSignal`.

When executor execution returns an error:

1. Infrastructure errors remain infrastructure.
2. Non-stop node errors remain node errors and publish `node.failed` as today.
3. Stop errors are passed through a new internal normalization helper.

The helper has this contract:

```rust
fn normalize_stop_error(control: &ExecutionControl, returned: RunError) -> Result<RunError, RunError>
```

- If `returned.kind() != RunErrorKind::Stop`, return `Ok(returned)`.
- If `returned.kind() == RunErrorKind::Stop` and `control.stop_reason()` is `Some(shared)`, return `Ok(RunError::stopped(shared))`.
- If `returned.kind() == RunErrorKind::Stop` and `control.stop_reason()` is `None`, return `Err(RunError::infrastructure("UNBACKED_STOP", "node returned a stop error without a runtime stop signal"))`.

The exact internal infrastructure code may be specific, but the durable Run terminal continues to use the existing infrastructure recovery contract: `failed` with `INFRASTRUCTURE_FAILURE`. A3 must not add a public terminal status or event type.

## Error flow

### External/shared stop

For normal cancellation, shutdown interruption, run timeout, or attached disconnect:

1. `StopController::request(reason)` stores the first reason and cancels the token.
2. Nodes observe `ExecutionControl::stopped()` or `stop_reason()`.
3. If a node returns `RunError::stopped(...)`, the execution boundary rewrites it to the shared reason.
4. `node.failed` and the Run terminal event use the same shared reason.

This preserves current external stop behavior and removes mismatched-return ambiguity.

### Unbacked stopped return

If a node returns `RunError::stopped(...)` while the shared signal has no reason:

1. the execution boundary classifies it as infrastructure;
2. no trusted stop-shaped `node.failed` event is published for that error;
3. `Scheduler::run` enters its existing infrastructure path;
4. sibling execution wrappers are cancelled with the scheduler-private cancellation token;
5. the coordinator recovers one generic failed terminal using the existing infrastructure recovery path.

This deliberately treats unbacked stopped errors as extension/runtime contract violations, not user cancellation or interruption.

### Mismatched stopped return

If the shared signal is `Interrupted` but a node returns `RunError::stopped(Cancelled)`, the result is `Interrupted`. If the shared signal is `TimedOut` but a node returns `Cancelled`, the result is `TimedOut`.

The returned reason cannot override the shared first winner.

## Component changes

### `src/runtime/execution.rs`

- Add an internal stop-normalization helper.
- Apply it before `publish_error(...)` and `classify_failure(...)`.
- Return `NodeExecutionFailure::Infrastructure` for unbacked stopped returns.
- Keep acquisition-stop and `control.stopped()` paths unchanged because both derive directly from `StopSignal`.

### `src/runtime/scheduler.rs`

No new public control flow is required. Existing infrastructure handling already cancels wrapper tasks with `task_cancel` and drains them. A3 tests must prove that this path handles unbacked stopped returns without waiting on siblings that never received a shared stop signal.

### `src/runtime/coordinator.rs`

No shape change is required. Existing infrastructure recovery continues to produce one durable `failed` terminal with `INFRASTRUCTURE_FAILURE`. Existing stop handling continues to map shared `Cancelled`, `Interrupted`, and `TimedOut` to their current statuses and event types.

## Extension contract

Extensions may still return `RunError::stopped(reason)` when they are responding to the runtime's shared stop signal. The safe pattern remains:

```rust
control.stopped().await;
Err(RunError::stopped(control.stop_reason().unwrap()))
```

Extensions must not use `RunError::stopped(...)` as a local control-flow shortcut or self-cancellation mechanism. Local business failures should return `RunError::new(...)`; failures caused by missing executors, event persistence, registry corruption, runtime cancellation wrappers, or other platform infrastructure should return `RunError::infrastructure(...)`.

A3 does not change Rust signatures, but it changes the semantics of invalid extension behavior: unbacked stopped returns no longer produce cancelled/interrupted/timed-out Runs.

## Compatibility contract

A3 keeps:

- existing `/v1` route set;
- existing HTTP status mapping;
- existing event envelope shape and event type set;
- existing Run status values and error codes;
- existing repository traits and migrations;
- existing `NodeExecutor` method signatures;
- existing external stop terminal semantics.

The only observable behavior change is for executors that return typed stop errors without, or inconsistent with, the runtime's shared stop signal. Those cases now fail closed instead of fabricating or overriding Run stop authority.

## Test strategy

Use deterministic scheduler/coordinator fixtures. Do not use sleeps except bounded timeouts for proving a deadlock no longer occurs.

Required tests:

1. **Unbacked stop is infrastructure**
   - A node returns `RunError::stopped(Interrupted)` while `StopSignal::reason()` is `None`.
   - Assert scheduler returns infrastructure failure, not `SchedulerResult::Stopped`.
   - Assert no trusted `node.failed` stop event is published for that fabricated stop.

2. **Unbacked stop cancels siblings**
   - In a parallel Run, one branch returns an unbacked stopped error while another branch blocks forever or waits only for the shared stop.
   - Assert scheduler returns promptly through infrastructure cancellation.
   - Assert successors/join are not started and execution permits are released.

3. **Shared reason overrides returned reason**
   - For at least `shared=Interrupted, returned=Cancelled` and `shared=Cancelled, returned=Interrupted`, assert the final scheduler/coordinator result uses the shared reason.
   - Assert `node.failed` code/message and Run terminal event/status match the shared reason, not the returned reason.

4. **Run timeout remains authoritative**
   - When shared reason is `TimedOut`, a returned `Cancelled` or `Interrupted` stop becomes `RUN_TIMEOUT` and a failed terminal.
   - Existing node-level `NODE_TIMEOUT` tests remain unchanged.

5. **Existing external stop behavior remains**
   - Existing cancellation, shutdown interruption, attached disconnect, and run timeout tests continue to pass.

6. **No public-shape drift**
   - Existing protocol, repository, API, migration, and formal tests continue to pass.

## Rollout and rollback

A3 requires no migration and no retained-history reset. Rolling forward changes only runtime classification of invalid extension stopped errors. Rolling back restores the previous permissive behavior; Runs terminalized as generic failed infrastructure remain valid under existing status and event contracts.

## Acceptance checklist

- Executor-returned stop errors cannot create stop authority.
- Shared stop reason is the only durable terminal reason for stopped Runs.
- Mismatched returned reasons are normalized before node and Run events.
- Unbacked stopped returns fail closed through infrastructure recovery.
- Sibling wrappers drain promptly for unbacked stopped returns.
- Public routes, event envelope, event type set, repository shape, migrations, and DSL remain unchanged.
