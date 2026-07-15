# Outer Run-Task Panic Fail-Stop Implementation Plan

**Goal:** Contain panics around spawned Run scheduler/finalizer tasks, release all
process-local Run ownership exactly once, and make the production process drain and
exit nonzero.

**Design:**
`docs/superpowers/specs/2026-07-15-outer-run-task-panic-fail-stop-design.md`

**Status:** Implemented and verified

## Constraints

- Close admission and publish sticky fatality before attempting durable recovery.
- Preserve the existing `failed/infrastructure/INFRASTRUCTURE_FAILURE` Run terminal.
- Never format or log a caught panic payload.
- Release active-map ownership, lifecycle waiters, and Run capacity even if recovery
  returns an error or panics.
- Preserve signal drain, PostgreSQL ownership-loss precedence, and explicit clean
  owner release after a healthy-store fatal drain.
- Do not change public HTTP, SSE, Run, event, Agent, repository, or migration shapes.
- Use deterministic gates and bounded timeouts; do not add a public panic-injection
  configuration knob.

## Task 1: Freeze RED service contracts

1. Add a deterministic scheduler-path panic injection after active promotion.
2. Prove pre- and post-transition fatal subscribers observe the sticky state.
3. Prove admission/readiness close and subsequent Run creation is rejected.
4. Prove the affected Run reaches the existing infrastructure-failed terminal when
   repository recovery is available.
5. Prove active/preparing maps empty, capacity is released, lifecycle waiters wake,
   and bounded shutdown completes.
6. Add an equivalent prelaunch-finalizer panic case with no `run.started` event.
7. Add a recovery-failure case proving local cleanup is independent of persistence.

## Task 2: Implement exact task ownership and containment

1. Add a common active-task guard whose normal and unwind paths remove the matching
   active entry, drop its permit, and notify lifecycle exactly once.
2. Add a one-shot start gate so spawned task work cannot finish before active-map
   insertion and promotion notification.
3. Wrap the complete scheduler and finalizer task bodies in asynchronous
   `catch_unwind` without inspecting the panic payload.
4. Add idempotent sticky runtime-fatal state to `RunServiceInner`.
5. On panic, close admission, publish fatality, run best-effort recovery, and always
   finish through the ownership guard.

## Task 3: Reuse durable infrastructure recovery

1. Expose a crate-internal coordinator recovery entry point that retains the current
   stable terminal code/message and repository compare-and-set behavior.
2. Retain sufficient `NewRun` and `RunState` context for scheduler and durable
   prelaunch-finalizer panic recovery.
3. Keep an already-authoritative terminal unchanged.
4. Treat recovery errors or a second panic as best effort: log only stable codes,
   preserve fatality, and leave any incomplete durable row for startup reconciliation.

## Task 4: Integrate process fail-stop behavior

1. Subscribe to runtime fatality before serving HTTP and add `RuntimeFatal` to the
   main shutdown trigger.
2. Reuse runtime-first, HTTP-second drain under the existing hard deadline.
3. Recheck sticky ownership loss and runtime fatality after drain.
4. Enforce final precedence `OwnershipLost > RuntimeFatal > HttpStopped > Signal`.
5. On runtime fatality with healthy PostgreSQL ownership, explicitly release the owner
   after both drains and then exit nonzero.
6. Install a production panic hook that emits only
   `PROCESS_PANICKED/process panic captured`; keep Run fail-stop logs on the more
   specific `RUNTIME_TASK_PANICKED` code.
7. Add main-lifecycle tests for sticky notification, channel closure, trigger races,
   and owner-release ordering without adding a production panic knob.

## Task 5: Synchronize documentation and status

1. Document outer Run-task fail-stop behavior and supervisor restart expectations in
   README.
2. Mark this design and plan implemented only after focused and complete gates pass.
3. Change remediation item 6 from `Open` to `Addressed` and cite the direct scheduler,
   finalizer, cleanup, recovery-failure, and process-trigger tests.
4. Preserve the remaining independent verification items without broadening this
   milestone.

## Task 6: Complete gates and independent review

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --lib runtime::service::tests -- --nocapture --test-threads=1
cargo test --locked --all-targets --all-features -- --nocapture --test-threads=1
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo audit
cargo deny check
git diff --check
```

Then independently review:

- guard and start-gate ordering under normal completion, panic, and task teardown;
- panic coverage around both spawned task kinds and recovery itself;
- stable fatal-state races against signal, HTTP stop, and ownership loss;
- PostgreSQL owner retention/release behavior;
- panic-payload and Run-input containment in logs, errors, and durable records.
