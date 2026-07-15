# Outer Run-Task Panic Fail-Stop Design

**Date:** 2026-07-15

**Status:** Implemented and verified

**Scope:** Panic containment around spawned Run scheduler/finalizer tasks, exact
process-local ownership cleanup, sticky runtime-fatal signaling, best-effort durable
terminal recovery, and process-level fail-stop behavior

## 1. Context

`RunService` owns every admitted Run in either `preparing` or `active`. Promotion to
`active` transfers the Run-capacity permit to an `ActiveRun` and spawns either the
normal coordinator task or a prelaunch terminal finalizer. Both spawned futures
currently remove their active entry and notify lifecycle waiters only at the normal
tail of the future.

The scheduler contains panics from its own branch task set, but that does not cover a
panic in the outer coordinator future, timeout wrapper, prelaunch finalizer, or future
service code added around those paths. Such a panic skips the normal tail and can
leave all of the following behind:

- an `active` entry whose task has already ended;
- an unreleased global Run-capacity permit;
- cancellation and shutdown waiters that never observe a lifecycle transition;
- a durable `created` or `running` Run without a terminal;
- a process that still advertises readiness even though one of its Run owners died.

This is a fail-stop boundary, not merely a per-Run application error. Once an outer
Run owner panics, the process can no longer prove that all execution bookkeeping is
authoritative.

## 2. Decision

Every spawned scheduler or prelaunch-finalizer task is wrapped by one common outer
panic boundary and one exact active-ownership guard.

An observed outer Run-task panic performs this ordered transition:

1. close Run admission immediately;
2. publish a sticky process-level runtime-fatal signal;
3. attempt to recover the affected durable Run to the existing infrastructure-failed
   terminal;
4. remove the Run from `active`, release its capacity permit, and notify lifecycle
   waiters exactly once;
5. let the binary drain runtime and HTTP within the existing deadlines and exit
   nonzero.

The fatal transition is irreversible for the lifetime of the process. Successful
terminal recovery does not make the process healthy again.

## 3. Failure Boundary

The containment boundary covers the complete body of both task kinds after promotion:

- construction and execution of `RunCoordinator`;
- the whole-Run timeout selection and its post-execution checks;
- durable prelaunch terminal publication;
- panic-recovery work invoked by the outer wrapper.

The wrapper uses `AssertUnwindSafe` plus asynchronous `catch_unwind`. It never formats,
logs, returns, or persists the panic payload.

Panics already converted by scheduler-owned branch joins continue to use their
existing Run-level infrastructure-failure behavior. This design adds a second,
outermost boundary; it does not weaken or remove the scheduler boundary.

Forced process termination, runtime teardown after all service owners are dropped,
and arbitrary non-cooperative native blocking work remain outside unwind containment.
Those cases continue to rely on startup reconciliation and the configured shutdown
hard deadline.

## 4. Sticky Runtime-Fatal State

`RunServiceInner` gains a process-local fatal state consisting of:

- an atomic sticky flag for synchronous checks;
- a Tokio watch sender for lifecycle notification.

The state transition is idempotent. The first caller closes admission and changes the
flag from false to true; all later callers preserve true. A receiver created before
or after the transition observes the same fatal state.

`RunService` exposes internal lifecycle methods for the binary:

- `is_fatal()` for a final sticky recheck;
- `subscribe_fatal()` for the main shutdown select.

These methods may be public-but-hidden because the binary is a separate crate target,
but they are not a supported extension or client API.

Closing admission makes existing readiness checks return the current sanitized
`503/RUNTIME_UNHEALTHY` response. New Run creation continues to use the existing
`503/RUN_SERVICE_UNAVAILABLE` boundary. No new HTTP response shape or public status is
introduced.

Closing or otherwise losing the fatal watch channel is treated conservatively as a
fatal notification by the binary. The sender is normally retained by the service for
the full process lifetime.

## 5. Active Ownership and Exact Cleanup

Both spawned task kinds install an armed ownership guard before executing task work.
The guard owns only cleanup authority, not a second copy of the permit. Cleanup:

1. removes the matching `ActiveRun` under the existing poison-tolerant active-map
   lock;
2. drops the removed entry, which drops its `OwnedSemaphorePermit`;
3. advances `lifecycle_changed` so cancellation and shutdown waiters recheck state.

Normal completion explicitly runs the same cleanup and disarms the guard. Unwinding
uses `Drop`. Removal is idempotent, so recovery failure or a second cleanup attempt
cannot release another Run's ownership.

Promotion also gains an explicit start gate. The spawned task installs its cleanup
guard and waits until the caller has inserted the corresponding `ActiveRun`, released
the `preparing` and `active` locks, and published the promotion lifecycle change. The
caller then releases the gate. If promotion itself unwinds or drops the gate sender,
the task exits through the guard; cleanup waits for any held active lock and cannot
run ahead of insertion.

This preserves the invariant that a Run is always visible in `preparing`, visible in
`active`, or fully cleaned. There is no spawn-before-insert window in which the task
can finish without an owned map entry.

## 6. Durable Run Recovery

After catching a panic, the outer wrapper best-effort invokes the coordinator's
existing infrastructure recovery semantics. The attempted terminal remains:

- status: `failed`;
- failure kind: `infrastructure`;
- code: `INFRASTRUCTURE_FAILURE`;
- message: `runtime infrastructure failed`.

The recovery path uses the existing EventHub terminal and repository compare-and-set
boundary. If another terminal is already authoritative, that terminal wins. No second
terminal event is appended.

The prelaunch finalizer retains enough Run context to use the same infrastructure
recovery boundary if it panics after a durable create. It does not manufacture a
`run.started` event for a Run that never started.

Recovery is intentionally best effort. If EventHub or repository recovery returns an
error or itself panics, the active guard still removes ownership and releases
capacity, the process remains fatal, and a nonterminal durable row is left for the
next process's startup reconciliation. The runtime must never report success or
remain ready merely because terminal recovery failed.

## 7. Process Lifecycle Integration

The binary subscribes to runtime fatality before serving HTTP and selects it alongside
the existing signal, unexpected HTTP-stop, and PostgreSQL ownership-loss triggers.
Runtime fatality uses the existing two-phase drain:

1. admission is already closed by the fatal transition;
2. `RunService::shutdown` stops and drains all remaining preparing/active Runs;
3. Axum graceful shutdown begins only after runtime drain completes or fails;
4. the entire sequence remains bounded by `shutdown_hard_deadline`.

Sticky state is rechecked after drain so a signal or HTTP-stop race cannot hide a
concurrent Run-task panic. Final failure precedence is:

1. PostgreSQL ownership loss;
2. runtime fatality;
3. unexpected HTTP-server stop;
4. a clean shutdown signal.

If runtime fatality occurs while PostgreSQL ownership is still healthy, the binary
retains ownership through runtime and HTTP drain, explicitly releases it afterward,
and then returns a stable nonzero runtime-fatal error. If ownership is lost at any
point, the ownership-loss path wins and the old process does not perform a normal
unlock.

Runtime or HTTP drain failures are logged with their existing sanitized codes and do
not convert a fatal trigger into success. The hard deadline remains authoritative if
any drain stage fails to converge.

## 8. Sensitive-Data Contract

Rust's default panic hook prints panic payloads before `catch_unwind` returns. The
production binary therefore installs a fixed panic hook after tracing initialization.
The hook emits only:

- code: `PROCESS_PANICKED`;
- message: `process panic captured`.

The caught payload is discarded without `Debug` or display formatting. Run IDs may
be attached only by the containment code that already owns them; panic text, Run
input, model output, credentials, database URLs, and arbitrary payload values never
enter logs, terminal records, or public responses through this path.

The hook deliberately uses a generic process-panic classification because it is global
and also sanitizes panics outside Run tasks. The RunService containment and main fatal
trigger use the more specific stable `RUNTIME_TASK_PANICKED` code, and the top-level
returned error uses a fixed sanitized message. The durable affected Run continues to
use the pre-existing `INFRASTRUCTURE_FAILURE` contract, keeping process failure
distinct from Run terminal semantics.

## 9. Verification

Deterministic tests must cover both outer task kinds and avoid timing-only assertions.

### 9.1 RunService containment

Tests inject a panic after active promotion in the scheduler path and prove:

1. runtime fatality becomes true and a pre-subscribed receiver wakes;
2. a receiver subscribed afterward observes sticky fatality immediately;
3. admission and readiness remain closed;
4. the affected Run reaches `failed/INFRASTRUCTURE_FAILURE` when recovery is
   available;
5. `preparing` and `active` become empty;
6. the global capacity permit is released;
7. cancellation/lifecycle waiters wake rather than hanging;
8. shutdown completes within a bounded deadline.

A corresponding prelaunch-finalizer injection proves the same ownership and fatal
properties without publishing `run.started`.

A recovery-failure case proves that active ownership, permit release, fatality, and
shutdown do not depend on successful terminal persistence. A start-gate test proves
task cleanup cannot finish before active insertion.

### 9.2 Process trigger races

Main-lifecycle unit tests prove the sticky notification and final decision policy:

- fatal wait observes both sticky true and channel closure;
- a fatal sticky recheck wins over a signal-selected clean outcome;
- runtime fatality wins over unexpected HTTP stop;
- PostgreSQL ownership loss wins over runtime fatality;
- healthy PostgreSQL ownership uses the clean-release policy.

The tests exercise extracted lifecycle decision helpers where real-binary panic
injection would require a public or production test knob. Source sequencing retains
the already-awaited runtime and HTTP drain results before applying that decision and
releasing healthy ownership. No panic-injection knob is added.

### 9.3 Sensitive-data containment

Tests use a sentinel secret as the injected panic payload and prove all project-owned
fatal formatting, recovery records, and returned errors contain only stable
code/message values. The production hook is tested through its fixed formatting
helper without replacing the global hook concurrently across the test suite.

## 10. Compatibility and Out of Scope

This change does not alter:

- public HTTP routes, status mappings, or JSON envelopes;
- SSE, Run event, terminal, Agent, or DSL shapes;
- repository traits, SQL schemas, or migration layout;
- Attached versus Detached shutdown terminal semantics;
- PostgreSQL advisory-lock or generation-fencing protocols;
- automatic in-process runtime restart or task respawn.

The process exits so its deployment supervisor can start a clean runtime. For
PostgreSQL, the replacement must acquire the existing exclusive-store ownership
contract before reconciliation.

## 11. Acceptance Criteria

1. No outer scheduler/finalizer panic can leave an active entry or Run-capacity permit.
2. Lifecycle and shutdown waiters are notified after panic cleanup.
3. Admission/readiness close before best-effort recovery begins.
4. Runtime fatality is sticky and observable before and after subscription.
5. Recoverable Runs use exactly the existing infrastructure-failed terminal contract.
6. Failed recovery cannot restore health, suppress process failure, or prevent local
   ownership cleanup.
7. Signal and HTTP-stop races cannot turn runtime fatality into exit zero.
8. PostgreSQL ownership-loss precedence and clean-release ordering remain correct.
9. Production panic output never includes the panic payload.
10. Focused and complete repository gates pass before remediation item 6 is marked
    `Addressed`.
