# Authoritative Stop Semantics Implementation Plan

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `BASE-P1-007` by making the runtime shared `StopSignal` the only authority for stopped Run terminal reasons.

**Architecture:** `src/runtime/execution.rs` becomes the enforcement boundary: executor-returned `RunError::stopped(...)` is normalized before `node.failed` publication and before scheduler classification. Shared stop reasons are preserved and returned reasons are ignored; unbacked stopped returns become infrastructure failures so scheduler-owned cancellation drains siblings.

**Tech Stack:** Rust, Tokio, existing `RunError`, `RunErrorKind`, `StopSignal`, `ExecutionControl`, `Scheduler`, `RunCoordinator`, `EventHub`, and in-memory test repositories.

## Global Constraints

- Execute implementation work on a feature branch or isolated worktree named `fix/authoritative-stop-semantics`.
- Do not change public HTTP, SSE, event envelope, repository, DSL, migration, or node-executor method signatures.
- Do not change `RunError`, `RunErrorKind`, `StopReason`, `ExecutionControl`, or `NodeExecutor` public type signatures.
- Do not add a public self-cancel API for nodes.
- Node-level `RunError::timeout()` remains a node failure with `NODE_TIMEOUT`, not a typed Run stop.
- Shared `StopSignal::reason()` is the only authoritative source for stopped Run terminal reason.
- Unbacked executor-returned stopped errors fail closed through infrastructure recovery.
- Existing external cancellation, shutdown interruption, run timeout, and attached disconnect semantics must continue to pass.
- No dependency, migration, or platform configuration changes.
- Commit after each task that reaches its verification gate.

---

## File Structure

- Modify `tests/run_scheduler.rs`
  - Extend the scheduler test behavior fixture so tests can return arbitrary stopped reasons and mismatched reasons after the shared runtime signal fires.
  - Add RED tests proving unbacked stopped returns are infrastructure failures and cancel siblings.

- Modify `src/runtime/execution.rs`
  - Add `normalize_execution_error(&ExecutionControl, RunError) -> Result<RunError, RunError>`.
  - Apply normalization before node failure event publication and before `classify_failure`.

- Modify `tests/run_coordinator.rs`
  - Extend the coordinator fixture with a mismatched stopped-return behavior.
  - Add tests proving durable terminal status/event/code uses the shared reason, not the returned reason.

- No changes to:
  - `src/api/*`
  - `src/events/protocol.rs`
  - `src/history/repository.rs`
  - `src/runtime/control.rs`
  - `migrations/*`
  - `Cargo.toml`
  - `Cargo.lock`

---

### Task 1: Add RED scheduler coverage for unbacked stopped returns

**Files:**
- Modify: `tests/run_scheduler.rs`

**Interfaces:**
- Consumes: existing `SchedulerBehavior`, `SchedulerExecutor`, `scheduler_agent`, `compile_parallel_agent`, `replace_behavior`, `parallel_scheduler`, `SchedulerRepository`.
- Produces:
  - `SchedulerBehavior::Stop { reason: StopReason, executions: Arc<AtomicUsize> }`
  - `SchedulerBehavior::ReturnedStopAfterRuntimeStop { returned: StopReason, executions: Arc<AtomicUsize>, started: Arc<Notify> }`
  - test `unbacked_executor_stop_is_infrastructure_failure`
  - test `unbacked_executor_stop_cancels_parallel_siblings_and_releases_permits`

- [ ] **Step 1: Make scheduler fixture stop reason configurable**

In `tests/run_scheduler.rs`, replace the existing enum variant:

```rust
    Stop {
        executions: Arc<AtomicUsize>,
    },
```

with:

```rust
    Stop {
        reason: StopReason,
        executions: Arc<AtomicUsize>,
    },
    ReturnedStopAfterRuntimeStop {
        returned: StopReason,
        executions: Arc<AtomicUsize>,
        started: Arc<Notify>,
    },
```

Replace the existing executor arm:

```rust
            SchedulerBehavior::Stop { executions } => {
                executions.fetch_add(1, Ordering::SeqCst);
                Err(RunError::stopped(StopReason::Interrupted))
            }
```

with:

```rust
            SchedulerBehavior::Stop { reason, executions } => {
                executions.fetch_add(1, Ordering::SeqCst);
                Err(RunError::stopped(*reason))
            }
            SchedulerBehavior::ReturnedStopAfterRuntimeStop {
                returned,
                executions,
                started,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                control.stopped().await;
                Err(RunError::stopped(*returned))
            }
```

Update the existing `SchedulerBehavior::Stop` construction in `parallel_scheduler_never_captures_stop_as_a_branch_result` to include `reason: StopReason::Interrupted`.

- [ ] **Step 2: Add unbacked single-node RED test**

Append this test near `parallel_scheduler_never_captures_stop_as_a_branch_result`:

```rust
#[tokio::test]
async fn unbacked_executor_stop_is_infrastructure_failure() {
    let executions = Arc::new(AtomicUsize::new(0));
    let agent = scheduler_agent(
        vec![scheduler_node(
            "self_stop",
            None,
            SchedulerBehavior::Stop {
                reason: StopReason::Interrupted,
                executions: Arc::clone(&executions),
            },
        )],
        "self_stop",
    );
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = scheduler(agent, Arc::clone(&repository));
    let (_, stop) = stop_pair();

    let error = scheduler
        .run(context("run_unbacked_stop"), stop)
        .await
        .unwrap_err();

    assert_eq!(error.code(), "INFRASTRUCTURE_FAILURE");
    assert_eq!(error.kind(), RunErrorKind::Infrastructure);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    let events = repository.events.lock().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type.as_str(), "node.started");
}
```

- [ ] **Step 3: Add unbacked parallel sibling-drain RED test**

Append this test after the single-node RED test:

```rust
#[tokio::test]
async fn unbacked_executor_stop_cancels_parallel_siblings_and_releases_permits() {
    let mut agent = compile_parallel_agent(two_branch_yaml());
    let stopper_runs = Arc::new(AtomicUsize::new(0));
    let blocked_runs = Arc::new(AtomicUsize::new(0));
    let successors = Arc::new(AtomicUsize::new(0));
    replace_behavior(
        &mut agent,
        "search_a",
        SchedulerBehavior::Stop {
            reason: StopReason::Interrupted,
            executions: Arc::clone(&stopper_runs),
        },
    );
    replace_behavior(
        &mut agent,
        "search_b",
        SchedulerBehavior::WaitForever {
            executions: Arc::clone(&blocked_runs),
        },
    );
    for node_id in ["summarize_a", "summarize_b", "collect", "result"] {
        replace_behavior(
            &mut agent,
            node_id,
            SchedulerBehavior::Next {
                output: json!({}),
                require_output: None,
                executions: Arc::clone(&successors),
            },
        );
    }
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 2);
    let (_, stop) = stop_pair();

    let error = tokio::time::timeout(
        Duration::from_secs(1),
        scheduler.run(context("run_unbacked_parallel_stop"), stop),
    )
    .await
    .expect("unbacked stopped return must cancel sibling wrappers")
    .unwrap_err();

    assert_eq!(error.code(), "INFRASTRUCTURE_FAILURE");
    assert_eq!(stopper_runs.load(Ordering::SeqCst), 1);
    assert_eq!(blocked_runs.load(Ordering::SeqCst), 1);
    assert_eq!(successors.load(Ordering::SeqCst), 0);
    assert!(!repository.events.lock().await.iter().any(|event| {
        matches!(
            event.event_type.as_str(),
            "node.failed" | "branch.completed" | "branch.failed"
        )
    }));
}
```

- [ ] **Step 4: Run RED tests**

Run:

```bash
cargo test --test run_scheduler unbacked_executor_stop_is_infrastructure_failure -- --nocapture
cargo test --test run_scheduler unbacked_executor_stop_cancels_parallel_siblings_and_releases_permits -- --nocapture
```

Expected:

- `unbacked_executor_stop_is_infrastructure_failure` fails because current scheduler returns `SchedulerResult::Stopped`.
- `unbacked_executor_stop_cancels_parallel_siblings_and_releases_permits` fails by timeout or by returning `SchedulerResult::Stopped`, proving siblings do not receive authoritative infrastructure cancellation.

- [ ] **Step 5: Commit RED tests**

```bash
git add tests/run_scheduler.rs
git commit -m "test: cover unbacked executor stop"
```

---

### Task 2: Normalize stopped errors at execution boundary

**Files:**
- Modify: `src/runtime/execution.rs`

**Interfaces:**
- Consumes:
  - `ExecutionControl::stop_reason() -> Option<StopReason>`
  - `RunError::stopped(StopReason) -> RunError`
  - `RunError::infrastructure(&'static str, impl Into<String>) -> RunError`
- Produces:
  - `normalize_execution_error(control: &ExecutionControl, error: RunError) -> Result<RunError, RunError>`
  - `unbacked_stop_error() -> RunError`

- [ ] **Step 1: Add normalization helper**

In `src/runtime/execution.rs`, add this helper near `classify_failure`:

```rust
fn normalize_execution_error(
    control: &ExecutionControl,
    error: RunError,
) -> Result<RunError, RunError> {
    if error.kind() != RunErrorKind::Stop {
        return Ok(error);
    }
    match control.stop_reason() {
        Some(reason) => Ok(RunError::stopped(reason)),
        None => Err(unbacked_stop_error()),
    }
}

fn unbacked_stop_error() -> RunError {
    RunError::infrastructure(
        "UNBACKED_STOP",
        "node returned a stop error without a runtime stop signal",
    )
}
```

- [ ] **Step 2: Apply normalization before node failure publication**

In `execute_node_inner`, replace the current error branch:

```rust
        Err(error) => {
            events
                .publish_error(
                    node_scope,
                    RunEventType::NodeFailed,
                    error.code(),
                    error.message(),
                    json!({}),
                )
                .await
                .map_err(|event| NodeExecutionFailure::Infrastructure(event_error(event)))?;
            return Err(classify_failure(&node_id, error));
        }
```

with:

```rust
        Err(error) => {
            let error = normalize_execution_error(&control, error)
                .map_err(NodeExecutionFailure::Infrastructure)?;
            events
                .publish_error(
                    node_scope,
                    RunEventType::NodeFailed,
                    error.code(),
                    error.message(),
                    json!({}),
                )
                .await
                .map_err(|event| NodeExecutionFailure::Infrastructure(event_error(event)))?;
            return Err(classify_failure(&node_id, error));
        }
```

- [ ] **Step 3: Run RED tests again**

Run:

```bash
cargo test --test run_scheduler unbacked_executor_stop_is_infrastructure_failure -- --nocapture
cargo test --test run_scheduler unbacked_executor_stop_cancels_parallel_siblings_and_releases_permits -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Run scheduler suite**

Run:

```bash
cargo test --test run_scheduler -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit implementation**

```bash
git add src/runtime/execution.rs tests/run_scheduler.rs
git commit -m "fix: normalize executor stop errors"
```

---

### Task 3: Cover shared-reason override at scheduler and coordinator levels

**Files:**
- Modify: `tests/run_scheduler.rs`
- Modify: `tests/run_coordinator.rs`
- Modify: `src/runtime/execution.rs` only if Task 3 exposes a normalization defect

**Interfaces:**
- Consumes:
  - `SchedulerBehavior::ReturnedStopAfterRuntimeStop`
  - `normalize_execution_error`
- Produces:
  - test `shared_stop_reason_overrides_executor_returned_reason`
  - `Behavior::ReturnedStopAfterRuntimeStop`
  - test `coordinator_uses_shared_stop_reason_when_executor_returns_mismatched_stop`

- [ ] **Step 1: Add scheduler mismatch test**

Append this test near the external stop tests in `tests/run_scheduler.rs`:

```rust
#[tokio::test]
async fn shared_stop_reason_overrides_executor_returned_reason() {
    for (shared, returned, expected) in [
        (
            StopReason::Interrupted,
            StopReason::Cancelled,
            RunError::stopped(StopReason::Interrupted),
        ),
        (
            StopReason::Cancelled,
            StopReason::Interrupted,
            RunError::stopped(StopReason::Cancelled),
        ),
        (
            StopReason::TimedOut,
            StopReason::Cancelled,
            RunError::stopped(StopReason::TimedOut),
        ),
    ] {
        let started = Arc::new(Notify::new());
        let executions = Arc::new(AtomicUsize::new(0));
        let agent = scheduler_agent(
            vec![scheduler_node(
                "mismatch",
                None,
                SchedulerBehavior::ReturnedStopAfterRuntimeStop {
                    returned,
                    executions: Arc::clone(&executions),
                    started: Arc::clone(&started),
                },
            )],
            "mismatch",
        );
        let repository = Arc::new(SchedulerRepository::default());
        let scheduler = scheduler(agent, Arc::clone(&repository));
        let (controller, stop) = stop_pair();
        let execution = tokio::spawn(async move {
            scheduler
                .run(context("run_shared_stop_override"), stop)
                .await
        });

        started.notified().await;
        assert!(controller.request(shared));

        assert_eq!(
            execution.await.unwrap().unwrap(),
            SchedulerResult::Stopped(expected.clone())
        );
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        let events = repository.events.lock().await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].event_type.as_str(), "node.failed");
        assert_eq!(events[1].code, expected.code());
    }
}
```

- [ ] **Step 2: Add coordinator fixture behavior**

In `tests/run_coordinator.rs`, add this enum variant to `Behavior`:

```rust
    ReturnedStopAfterRuntimeStop {
        returned: StopReason,
        started: Arc<Notify>,
    },
```

Add this executor arm:

```rust
            Behavior::ReturnedStopAfterRuntimeStop { returned, started } => {
                started.notify_one();
                control.stopped().await;
                Err(RunError::stopped(*returned))
            }
```

- [ ] **Step 3: Add coordinator mismatch terminal test**

Append this test near `typed_external_stop_reasons_keep_their_terminal_statuses`:

```rust
#[tokio::test]
async fn coordinator_uses_shared_stop_reason_when_executor_returns_mismatched_stop() {
    for (shared, returned, expected_status, expected_event, expected_code) in [
        (
            StopReason::Interrupted,
            StopReason::Cancelled,
            RunStatus::Interrupted,
            RunEventType::RunInterrupted,
            "RUN_INTERRUPTED",
        ),
        (
            StopReason::Cancelled,
            StopReason::Interrupted,
            RunStatus::Cancelled,
            RunEventType::RunCancelled,
            "RUN_CANCELLED",
        ),
        (
            StopReason::TimedOut,
            StopReason::Cancelled,
            RunStatus::Failed,
            RunEventType::RunFailed,
            "RUN_TIMEOUT",
        ),
    ] {
        let repository = Arc::new(MemoryRepository::default());
        let started = Arc::new(Notify::new());
        let agent = agent(
            vec![node(
                "mismatch",
                None,
                Duration::from_secs(5),
                Behavior::ReturnedStopAfterRuntimeStop {
                    returned,
                    started: Arc::clone(&started),
                },
            )],
            "mismatch",
        );
        let coordinator = coordinator(agent, Arc::clone(&repository), true);
        let (controller, stop) = stop_pair();
        let execution = coordinator.execute(new_run(), json!({}), stop);
        let request_stop = async {
            started.notified().await;
            controller.request(shared)
        };
        let (result, requested) = tokio::join!(execution, request_stop);

        assert!(requested);
        assert_eq!(result.unwrap(), expected_status);
        let events = repository.events.lock().await;
        assert_eq!(events[3].event_type, RunEventType::NodeFailed);
        assert_eq!(events[3].code, expected_code);
        assert_eq!(events[4].event_type, expected_event);
        assert_eq!(events[4].code, expected_code);
    }
}
```

- [ ] **Step 4: Run focused mismatch tests**

Run:

```bash
cargo test --test run_scheduler shared_stop_reason_overrides_executor_returned_reason -- --nocapture
cargo test --test run_coordinator coordinator_uses_shared_stop_reason_when_executor_returns_mismatched_stop -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run runtime-focused suites**

Run:

```bash
cargo test --test run_scheduler -- --nocapture
cargo test --test run_coordinator -- --nocapture
cargo test --test run_service -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit tests**

```bash
git add tests/run_scheduler.rs tests/run_coordinator.rs src/runtime/execution.rs
git commit -m "test: cover shared stop authority"
```

---

### Task 4: Final verification and public-shape guard

**Files:**
- No production code changes unless verification exposes a concrete defect.

**Interfaces:**
- Consumes: all previous A3 commits.
- Produces: verified A3 branch ready for local merge.

- [ ] **Step 1: Run formatting check**

Run:

```bash
cargo fmt --all -- --check
```

Expected: PASS.

- [ ] **Step 2: Run Clippy**

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS.

- [ ] **Step 3: Run full tests**

Run:

```bash
cargo test --all-targets --quiet
```

Expected: PASS.

- [ ] **Step 4: Confirm public-shape files did not change**

Run:

```bash
git diff --name-only main..HEAD
```

Expected changed paths are limited to:

```text
src/runtime/execution.rs
tests/run_scheduler.rs
tests/run_coordinator.rs
```

The A3 design and plan docs may also appear when the branch includes documentation commits.

- [ ] **Step 5: Confirm no dependency or migration drift**

Run:

```bash
git diff -- Cargo.toml Cargo.lock migrations
```

Expected: no output.

- [ ] **Step 6: Commit verification notes only if the plan checklist was edited**

```bash
git add docs/superpowers/plans/2026-07-11-authoritative-stop-semantics.md
git commit -m "docs: record authoritative stop verification"
```

Skip this commit when the plan file was not edited during execution.

