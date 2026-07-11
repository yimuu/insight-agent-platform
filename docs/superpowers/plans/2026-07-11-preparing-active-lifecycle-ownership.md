# Preparing/Active Lifecycle Ownership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the shutdown race where an admitted Run can sit between capacity acquisition and active-map insertion without being owned by shutdown.

**Architecture:** `RunService` will own two lifecycle maps: `preparing` for admitted Runs that have not launched scheduler work, and `active` for spawned scheduler or finalizer tasks. A local preparation guard keeps caller-future cancellation safe across every preparation await, while shutdown and subscription release request typed stops against both maps and wait for lifecycle drain.

**Tech Stack:** Rust, Tokio, `Arc<Mutex<BTreeMap<...>>>`, `watch::Sender`, existing `EventHub`, existing `RunRepository`, existing `RunStatus` and `RunEventType` contracts.

## Global Constraints

- Do not change public HTTP, SSE, event, Run, repository, DSL, or migration shapes.
- Do not add dependencies or platform configuration.
- Execute implementation work on a feature branch or isolated worktree named `fix/preparing-active-lifecycle`.
- Preserve existing `/v1` routes, event payload shapes, status values, and error codes.
- Attached preparing stops use `RunStatus::Cancelled`, `RunEventType::RunCancelled`, and `RUN_CANCELLED`.
- Detached preparing stops use `RunStatus::Interrupted`, `RunEventType::RunInterrupted`, and `RUN_INTERRUPTED`.
- Pre-launch terminalization publishes `run.created` followed by exactly one terminal event, and never publishes `run.started`.
- Tests must use deterministic gates or bounded Tokio timeouts; do not rely on long sleeps.
- Commit after each task that reaches its verification gate.

---

## File Structure

- Modify `src/runtime/service.rs`
  - Add `PreparingRun`, reduced `PreparedRun`, and `PreparingGuard`.
  - Add `preparing` map and rename lifecycle notification from active-only to lifecycle-wide.
  - Move normal launch into a promotion path from preparing to active.
  - Add finalizer promotion for stopped durable preparing Runs.
  - Update shutdown and subscription release to inspect both preparing and active.
  - Add private `#[cfg(test)]` unit tests for launch-boundary cases that require internal hooks.

- Modify `tests/run_service.rs`
  - Extend `CountingRepository` with deterministic gates for `create_run` and `get_run`.
  - Add integration tests that prove shutdown waits for preparing work and admission closes cleanly.
  - Keep all public-shape assertions in integration tests.

- No changes to these files:
  - `src/api/*`
  - `src/events/protocol.rs`
  - `src/history/repository.rs`
  - `migrations/*`
  - `Cargo.toml`

---

### Task 1: Add repository gates and failing shutdown-preparing test

**Files:**
- Modify: `tests/run_service.rs`

**Interfaces:**
- Consumes: existing `CountingRepository`, `service_with_agents`, `wait_for_status`.
- Produces:
  - `RepositoryHooks`
  - `RepositoryGate`
  - `service_with_repository_hooks(config: RunServiceConfig, hooks: RepositoryHooks) -> Result<(RunService, Arc<CountingRepository>), ServiceError>`
  - failing test `shutdown_waits_for_detached_run_blocked_in_create_run`

- [ ] **Step 1: Add deterministic repository gates**

In `tests/run_service.rs`, extend the imports:

```rust
use tokio::sync::{Mutex, Notify, watch};
```

Replace the existing single `use tokio::sync::Mutex;` line with the combined import above.

Add these helper types above `struct CountingRepository`:

```rust
#[derive(Clone, Default)]
struct RepositoryHooks {
    create_run: Option<Arc<RepositoryGate>>,
    get_run: Option<Arc<RepositoryGate>>,
}

struct RepositoryGate {
    entered: watch::Sender<bool>,
    release: Notify,
    used: AtomicBool,
}

impl RepositoryGate {
    fn new() -> Arc<Self> {
        let (entered, _) = watch::channel(false);
        Arc::new(Self {
            entered,
            release: Notify::new(),
            used: AtomicBool::new(false),
        })
    }

    async fn block_once(&self) {
        if !self.used.swap(true, Ordering::SeqCst) {
            let _ = self.entered.send(true);
            self.release.notified().await;
        }
    }

    async fn wait_entered(&self) {
        let mut entered = self.entered.subscribe();
        while !*entered.borrow() {
            entered.changed().await.unwrap();
        }
    }

    fn release(&self) {
        self.release.notify_waiters();
    }
}
```

Add the field to `CountingRepository`:

```rust
    hooks: RepositoryHooks,
```

Update `CountingRepository::create_run` and `CountingRepository::get_run`:

```rust
    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
        if let Some(gate) = self.hooks.create_run.as_ref() {
            gate.block_once().await;
        }
        self.creates.fetch_add(1, Ordering::SeqCst);
        self.records.lock().await.insert(
            run.run_id.clone(),
            RunRecord {
                run_id: run.run_id,
                request_id: run.request_id,
                agent_id: run.agent_id,
                agent_version: run.agent_version,
                attachment: run.attachment,
                status: RunStatus::Created,
                started_at: None,
                ended_at: None,
                updated_at: run.created_at,
                input_summary: run.input_summary,
                output: None,
                error_code: None,
                error_message: None,
            },
        );
        Ok(())
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
        if let Some(gate) = self.hooks.get_run.as_ref() {
            gate.block_once().await;
        }
        Ok(self.records.lock().await.get(run_id).cloned())
    }
```

Update repository construction in `service_with_agents`:

```rust
async fn service_with_repository_hooks(
    config: RunServiceConfig,
    agents: Vec<Arc<CompiledAgent>>,
    hooks: RepositoryHooks,
) -> Result<(RunService, Arc<CountingRepository>), ServiceError> {
    let repository = Arc::new(CountingRepository {
        records: Mutex::new(BTreeMap::new()),
        events: Mutex::new(BTreeMap::new()),
        outputs: Mutex::new(Vec::new()),
        creates: AtomicUsize::new(0),
        event_history_reads: AtomicUsize::new(0),
        fail_appends: AtomicBool::new(false),
        hooks,
    });
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let agents = CompiledAgentRegistry::new(agents).unwrap();
    let mut executors = NodeExecutorRegistry::default();
    executors.register(ServiceNode).unwrap();
    let service = RunService::new(agents, executors, repository_trait, events, config)?;
    Ok((service, repository))
}

async fn service_with_agents(
    config: RunServiceConfig,
    agents: Vec<Arc<CompiledAgent>>,
) -> Result<(RunService, Arc<CountingRepository>), ServiceError> {
    service_with_repository_hooks(config, agents, RepositoryHooks::default()).await
}
```

- [ ] **Step 2: Add the failing test**

Append this test after `capacity_is_rejected_before_a_second_run_is_inserted`:

```rust
#[tokio::test]
async fn shutdown_waits_for_detached_run_blocked_in_create_run() {
    let create_gate = RepositoryGate::new();
    let (service, repository) = service_with_repository_hooks(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(3600),
        },
        vec![agent("fast", ServiceBehavior::Complete)],
        RepositoryHooks {
            create_run: Some(Arc::clone(&create_gate)),
            get_run: None,
        },
    )
    .await
    .unwrap();

    let creator_service = service.clone();
    let create_task = tokio::spawn(async move {
        creator_service
            .create_detached("fast", json!({}), RequestMetadata::default())
            .await
    });
    create_gate.wait_entered().await;

    let shutdown_service = service.clone();
    let mut shutdown_task =
        tokio::spawn(async move { shutdown_service.shutdown(Duration::from_secs(1)).await });
    tokio::select! {
        result = &mut shutdown_task => panic!("shutdown completed while create_run was blocked: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
    }

    create_gate.release();
    let created = create_task.await.unwrap().unwrap();
    shutdown_task.await.unwrap().unwrap();

    let interrupted = service.get_run(&created.run_id).await.unwrap();
    assert_eq!(interrupted.status, RunStatus::Interrupted);
    assert_eq!(interrupted.error_code.as_deref(), Some("RUN_INTERRUPTED"));
    assert_eq!(repository.creates.load(Ordering::SeqCst), 1);
}
```

- [ ] **Step 3: Run the focused test and confirm the race is reproduced**

Run:

```bash
cargo test --test run_service shutdown_waits_for_detached_run_blocked_in_create_run -- --nocapture
```

Expected: FAIL because current shutdown only waits on `active`, so it can complete before the gate releases.

- [ ] **Step 4: Commit the failing test**

```bash
git add tests/run_service.rs
git commit -m "test: cover preparing run missed by shutdown"
```

---

### Task 2: Add preparing ownership, guard, and shutdown drain

**Files:**
- Modify: `src/runtime/service.rs`

**Interfaces:**
- Consumes:
  - `RepositoryGate` failing test from Task 1.
  - existing `RunRepository::finish_run` through `EventHub::publish_terminal`.
- Produces:
  - `PreparingRun`
  - `PreparedRun { run_id, request_id, guard }`
  - `PreparingGuard`
  - `RunServiceInner::notify_lifecycle`
  - `RunServiceInner::promote_preparing`
  - `RunServiceInner::finalize_preparing_from_guard_drop`
  - `lock_preparing`
  - `lock_active`

- [ ] **Step 1: Update imports**

In `src/runtime/service.rs`, change the crate imports to include event and terminal types:

```rust
use serde_json::{json, Value};
```

Replace the existing `use serde_json::Value;` line.

Extend the crate imports:

```rust
    events::{
        hub::EventHub,
        protocol::{RunEventScope, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{summarize_input, NewRun, RunAttachment, RunRecord, RunStatus, TerminalUpdate},
    },
```

- [ ] **Step 2: Add lifecycle structs**

Replace the existing `ActiveRun` and `PreparedRun` definitions with:

```rust
struct ActiveRun {
    attachment: RunAttachment,
    stop: StopController,
    task: JoinHandle<()>,
    _permit: OwnedSemaphorePermit,
}

struct PreparingRun {
    agent: Arc<CompiledAgent>,
    new_run: NewRun,
    input: Value,
    attachment: RunAttachment,
    stop: StopController,
    signal: StopSignal,
    state: Arc<RunState>,
    permit: OwnedSemaphorePermit,
    durable: bool,
}

struct PreparedRun {
    run_id: String,
    request_id: String,
    guard: PreparingGuard,
}

struct PreparingGuard {
    inner: Arc<RunServiceInner>,
    run_id: String,
    armed: bool,
}

impl PreparingGuard {
    fn new(inner: Arc<RunServiceInner>, run_id: String) -> Self {
        Self {
            inner,
            run_id,
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PreparingGuard {
    fn drop(&mut self) {
        if self.armed {
            self.inner
                .finalize_preparing_from_guard_drop(&self.run_id);
        }
    }
}
```

- [ ] **Step 3: Add preparing map and lifecycle notification**

Update `RunServiceInner`:

```rust
    preparing: Mutex<BTreeMap<String, PreparingRun>>,
    active: Mutex<BTreeMap<String, ActiveRun>>,
    lifecycle_changed: watch::Sender<u64>,
    accepting: AtomicBool,
```

Update `RunService::new`:

```rust
                preparing: Mutex::new(BTreeMap::new()),
                active: Mutex::new(BTreeMap::new()),
                lifecycle_changed: watch::channel(0).0,
                accepting: AtomicBool::new(true),
```

Replace all references to `active_changed` with `lifecycle_changed`.

- [ ] **Step 4: Add locking and lifecycle helper functions**

Add these helpers near `lock_active`:

```rust
fn lock_preparing(inner: &RunServiceInner) -> MutexGuard<'_, BTreeMap<String, PreparingRun>> {
    inner
        .preparing
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_active(inner: &RunServiceInner) -> MutexGuard<'_, BTreeMap<String, ActiveRun>> {
    inner
        .active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn stop_reason_for_attachment(attachment: RunAttachment) -> StopReason {
    match attachment {
        RunAttachment::Attached => StopReason::Cancelled,
        RunAttachment::Detached => StopReason::Interrupted,
    }
}

fn run_scope(run: &NewRun) -> RunEventScope {
    RunEventScope::for_run(
        run.request_id.clone(),
        run.run_id.clone(),
        run.agent_id.clone(),
        run.agent_version.clone(),
    )
}

fn terminal_spec(reason: StopReason) -> (RunStatus, RunEventType, &'static str, &'static str) {
    match reason {
        StopReason::Cancelled => (
            RunStatus::Cancelled,
            RunEventType::RunCancelled,
            "RUN_CANCELLED",
            "run cancelled",
        ),
        StopReason::Interrupted => (
            RunStatus::Interrupted,
            RunEventType::RunInterrupted,
            "RUN_INTERRUPTED",
            "run interrupted",
        ),
        StopReason::TimedOut => (
            RunStatus::Failed,
            RunEventType::RunFailed,
            "RUN_TIMEOUT",
            "run timed out",
        ),
    }
}
```

Add these methods on `RunServiceInner`:

```rust
impl RunServiceInner {
    fn notify_lifecycle(&self) {
        self.lifecycle_changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    fn lifecycle_is_empty(&self) -> bool {
        lock_preparing(self).is_empty() && lock_active(self).is_empty()
    }

    fn mark_preparing_durable(&self, run_id: &str) {
        if let Some(run) = lock_preparing(self).get_mut(run_id) {
            run.durable = true;
        }
        self.notify_lifecycle();
    }

    fn remove_preparing(&self, run_id: &str) {
        let removed = lock_preparing(self).remove(run_id);
        drop(removed);
        self.notify_lifecycle();
    }

    fn lifecycle_stops(&self) -> Vec<(StopController, RunAttachment)> {
        let preparing = lock_preparing(self)
            .values()
            .map(|run| (run.stop.clone(), run.attachment))
            .collect::<Vec<_>>();
        let active = lock_active(self)
            .values()
            .map(|run| {
                let _already_finished = run.task.is_finished();
                (run.stop.clone(), run.attachment)
            })
            .collect::<Vec<_>>();
        preparing.into_iter().chain(active).collect()
    }

}
```

- [ ] **Step 5: Rework prepare_run to insert preparing before durable await**

Replace the body from capacity acquisition through return with this shape:

```rust
        let permit = Arc::clone(&self.inner.capacity)
            .try_acquire_owned()
            .map_err(|_| ServiceError::new("RUN_CAPACITY_EXCEEDED", "run capacity exceeded"))?;
        let run_id = format!("run_{}", Uuid::new_v4().simple());
        let request_id = request
            .request_id
            .filter(|request_id| !request_id.trim().is_empty())
            .unwrap_or_else(|| format!("req_{}", Uuid::new_v4().simple()));
        let new_run = NewRun {
            run_id: run_id.clone(),
            request_id: request_id.clone(),
            agent_id: agent.id.clone(),
            agent_version: agent.version_hash.clone(),
            attachment,
            created_at: Utc::now(),
            input_summary: summarize_input(&input),
        };
        let (stop, signal) = stop_pair();
        lock_preparing(&self.inner).insert(
            run_id.clone(),
            PreparingRun {
                agent,
                new_run: new_run.clone(),
                input,
                attachment,
                stop,
                signal,
                state: Arc::new(RunState::new()),
                permit,
                durable: false,
            },
        );
        self.inner.notify_lifecycle();
        let guard = PreparingGuard::new(Arc::clone(&self.inner), run_id.clone());

        self.inner
            .repository
            .create_run(new_run.clone())
            .await
            .map_err(|error| {
                self.inner.remove_preparing(&run_id);
                service_history_error(error)
            })?;
        self.inner.mark_preparing_durable(&run_id);
        self.inner.events.open_run(&run_id).await;

        Ok(PreparedRun {
            run_id,
            request_id,
            guard,
        })
```

- [ ] **Step 6: Rework create paths to use reduced PreparedRun**

Update `create_attached`:

```rust
        let prepared = self
            .prepare_run(agent_id, input, request, RunAttachment::Attached)
            .await?;
        let run_id = prepared.run_id.clone();
        let request_id = prepared.request_id.clone();
        let live = self.inner.events.subscribe(&run_id).await;
        let owner: Arc<dyn LeaseOwner> = self.inner.clone();
        let lease = SubscriptionLease::new(owner, &run_id);
        self.launch(prepared);
        Ok(AttachedRun {
            run_id: run_id.clone(),
            request_id,
            subscription: RunSubscription::new(run_id, live, lease),
        })
```

Update `create_detached`:

```rust
        let prepared = self
            .prepare_run(agent_id, input, request, RunAttachment::Detached)
            .await?;
        let record = self.get_run(&prepared.run_id).await?;
        self.launch(prepared);
        Ok(record)
```

- [ ] **Step 7: Implement finalizer publication helper**

Add this async function near `service_history_error`:

```rust
async fn publish_prelaunch_terminal(
    inner: Arc<RunServiceInner>,
    run: NewRun,
    reason: StopReason,
) -> Result<(), ServiceError> {
    inner
        .events
        .publish(
            run_scope(&run),
            RunEventType::RunCreated,
            json!({
                "status": RunStatus::Created,
                "attachment": run.attachment,
            }),
        )
        .await
        .map_err(|error| ServiceError::new(error.code(), error.to_string()))?;
    let (status, event_type, code, message) = terminal_spec(reason);
    let update = TerminalUpdate::new(
        &run.run_id,
        status,
        Utc::now(),
        None,
        Some(code.to_string()),
        Some(message.to_string()),
    )
    .map_err(|error| ServiceError::new(error.code(), error.to_string()))?;
    inner
        .events
        .publish_terminal(run_scope(&run), event_type, update, code, message, json!({}))
        .await
        .map_err(|error| ServiceError::new(error.code(), error.to_string()))?;
    Ok(())
}
```

- [ ] **Step 8: Replace launch with promotion from preparing**

Replace `fn launch(&self, prepared: PreparedRun)` with:

```rust
    fn launch(&self, prepared: PreparedRun) {
        let run_id = prepared.run_id.clone();
        let promoted = self.promote_preparing(&run_id);
        prepared.guard.disarm();
        if !promoted {
            tracing::warn!(run_id, "prepared run was already removed before launch");
        }
    }

    fn promote_preparing(&self, run_id: &str) -> bool {
        let mut preparing = lock_preparing(&self.inner);
        let Some(preparing_run) = preparing.remove(run_id) else {
            return false;
        };
        let mut active = lock_active(&self.inner);
        let should_run = self.inner.accepting.load(Ordering::Acquire)
            && preparing_run.stop.reason().is_none();
        let active_run = if should_run {
            self.spawn_scheduler_active(preparing_run)
        } else {
            let reason = preparing_run
                .stop
                .reason()
                .unwrap_or_else(|| stop_reason_for_attachment(preparing_run.attachment));
            preparing_run.stop.request(reason);
            self.spawn_finalizer_active(preparing_run, reason)
        };
        active.insert(run_id.to_string(), active_run);
        drop(active);
        drop(preparing);
        self.inner.notify_lifecycle();
        true
    }
```

Add `spawn_scheduler_active` by moving the current scheduler task body into this method:

```rust
    fn spawn_scheduler_active(&self, preparing: PreparingRun) -> ActiveRun {
        let PreparingRun {
            agent,
            new_run,
            input,
            attachment,
            stop,
            signal,
            state,
            permit,
            durable: _,
        } = preparing;
        let run_id = new_run.run_id.clone();
        let task_stop = stop.clone();
        let task_state = Arc::clone(&state);
        let inner = Arc::clone(&self.inner);
        let task_run_id = run_id.clone();
        let task = tokio::spawn(async move {
            let coordinator = RunCoordinator::new(
                agent,
                inner.executors.clone(),
                inner.events.clone(),
                Arc::clone(&inner.repository),
                ExecutionLimiter::new(
                    Arc::clone(&inner.node_capacity),
                    Arc::new(Semaphore::new(inner.config.max_parallel_branches_per_run)),
                ),
            );
            let execution =
                coordinator.execute_existing(new_run, input, signal, Arc::clone(&task_state));
            tokio::pin!(execution);
            let result = tokio::select! {
                result = &mut execution => result,
                _ = sleep(inner.config.run_timeout) => {
                    task_stop.request(StopReason::TimedOut);
                    execution.await
                }
            };
            if let Err(error) = result {
                inner.accepting.store(false, Ordering::Release);
                tracing::error!(
                    run_id = task_run_id,
                    code = error.code(),
                    "run coordinator failed"
                );
            }
            if !inner.events.is_healthy() {
                inner.accepting.store(false, Ordering::Release);
            }
            let removed = lock_active(&inner).remove(&task_run_id);
            drop(removed);
            inner.notify_lifecycle();
        });
        ActiveRun {
            attachment,
            stop,
            task,
            _permit: permit,
        }
    }
```

Add `spawn_finalizer_active`:

```rust
    fn spawn_finalizer_active(&self, preparing: PreparingRun, reason: StopReason) -> ActiveRun {
        let PreparingRun {
            new_run,
            attachment,
            stop,
            permit,
            durable,
            ..
        } = preparing;
        let inner = Arc::clone(&self.inner);
        let run_id = new_run.run_id.clone();
        let task_run_id = run_id.clone();
        let task = tokio::spawn(async move {
            if durable {
                if let Err(error) =
                    publish_prelaunch_terminal(Arc::clone(&inner), new_run, reason).await
                {
                    inner.accepting.store(false, Ordering::Release);
                    tracing::error!(
                        run_id = task_run_id,
                        code = error.code(),
                        "prelaunch finalizer failed"
                    );
                }
            }
            let removed = lock_active(&inner).remove(&task_run_id);
            drop(removed);
            inner.notify_lifecycle();
        });
        ActiveRun {
            attachment,
            stop,
            task,
            _permit: permit,
        }
    }
```

- [ ] **Step 9: Add guard-drop finalization method**

Add this method on `RunServiceInner`:

```rust
    fn finalize_preparing_from_guard_drop(self: &Arc<Self>, run_id: &str) {
        let mut preparing = lock_preparing(self);
        let Some(preparing_run) = preparing.remove(run_id) else {
            return;
        };
        let mut active = lock_active(self);
        let reason = preparing_run
            .stop
            .reason()
            .unwrap_or_else(|| stop_reason_for_attachment(preparing_run.attachment));
        preparing_run.stop.request(reason);
        let service = RunService {
            inner: Arc::clone(self),
        };
        let active_run = service.spawn_finalizer_active(preparing_run, reason);
        active.insert(run_id.to_string(), active_run);
        drop(active);
        drop(preparing);
        self.notify_lifecycle();
    }
```

- [ ] **Step 10: Update shutdown to stop and wait on both maps**

Replace `shutdown` with:

```rust
    pub async fn shutdown(&self, deadline: Duration) -> Result<(), ServiceError> {
        self.inner.accepting.store(false, Ordering::Release);
        let mut lifecycle_changed = self.inner.lifecycle_changed.subscribe();
        let stops = self.inner.lifecycle_stops();
        for (stop, attachment) in stops {
            stop.request(stop_reason_for_attachment(attachment));
        }
        let wait_for_empty = async {
            loop {
                if self.inner.lifecycle_is_empty() {
                    break;
                }
                lifecycle_changed.changed().await.map_err(|_| {
                    ServiceError::new("SHUTDOWN_FAILED", "run completion channel closed")
                })?;
            }
            Ok::<(), ServiceError>(())
        };
        let result = timeout(deadline, wait_for_empty)
            .await
            .map_err(|_| ServiceError::new("SHUTDOWN_TIMEOUT", "run shutdown timed out"))?;
        result?;
        Ok(())
    }
```

- [ ] **Step 11: Run the focused test**

Run:

```bash
cargo test --test run_service shutdown_waits_for_detached_run_blocked_in_create_run -- --nocapture
```

Expected: PASS.

- [ ] **Step 12: Run existing service tests**

Run:

```bash
cargo test --test run_service -- --nocapture
```

Expected: PASS.

- [ ] **Step 13: Commit**

```bash
git add src/runtime/service.rs tests/run_service.rs
git commit -m "fix: track preparing runs during shutdown"
```

---

### Task 3: Cover durable pre-launch finalization event semantics

**Files:**
- Modify: `tests/run_service.rs`
- Modify: `src/runtime/service.rs` only if Task 2 needs correction

**Interfaces:**
- Consumes:
  - `RepositoryHooks::get_run`
  - `publish_prelaunch_terminal`
  - `terminal_spec`
- Produces:
  - test `shutdown_after_durable_create_finalizes_before_detached_launch`
  - event sequence assertion helper `run_event_types`

- [ ] **Step 1: Add event sequence helper**

Add this helper after `wait_for_status`:

```rust
async fn run_event_types(
    repository: &CountingRepository,
    run_id: &str,
) -> Vec<RunEventType> {
    repository
        .events
        .lock()
        .await
        .get(run_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|event| event.event_type)
        .collect()
}
```

- [ ] **Step 2: Add detached get_run-window test**

Append this test after `shutdown_waits_for_detached_run_blocked_in_create_run`:

```rust
#[tokio::test]
async fn shutdown_after_durable_create_finalizes_before_detached_launch() {
    let get_gate = RepositoryGate::new();
    let (service, repository) = service_with_repository_hooks(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(3600),
        },
        vec![agent("fast", ServiceBehavior::Complete)],
        RepositoryHooks {
            create_run: None,
            get_run: Some(Arc::clone(&get_gate)),
        },
    )
    .await
    .unwrap();

    let creator_service = service.clone();
    let create_task = tokio::spawn(async move {
        creator_service
            .create_detached("fast", json!({}), RequestMetadata::default())
            .await
    });
    get_gate.wait_entered().await;

    let shutdown_service = service.clone();
    let mut shutdown_task =
        tokio::spawn(async move { shutdown_service.shutdown(Duration::from_secs(1)).await });
    tokio::select! {
        result = &mut shutdown_task => panic!("shutdown completed while get_run was blocked: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
    }

    get_gate.release();
    let created = create_task.await.unwrap().unwrap();
    shutdown_task.await.unwrap().unwrap();
    let interrupted = wait_for_status(&service, &created.run_id, RunStatus::Interrupted).await;

    assert_eq!(interrupted.started_at, None);
    assert_eq!(interrupted.error_code.as_deref(), Some("RUN_INTERRUPTED"));
    assert_eq!(
        run_event_types(&repository, &created.run_id).await,
        vec![RunEventType::RunCreated, RunEventType::RunInterrupted]
    );
}
```

- [ ] **Step 3: Run the new test**

Run:

```bash
cargo test --test run_service shutdown_after_durable_create_finalizes_before_detached_launch -- --nocapture
```

Expected: PASS after Task 2. If it fails with `run.started` or `run.completed`, inspect `promote_preparing` and ensure `accepting == false` selects `spawn_finalizer_active`.

- [ ] **Step 4: Run event protocol regression tests**

Run:

```bash
cargo test --test event_hub -- --nocapture
cargo test --test formal_protocol -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/runtime/service.rs tests/run_service.rs
git commit -m "test: cover prelaunch terminal events"
```

---

### Task 4: Cover attached disconnect and dropped caller future during preparation

**Files:**
- Modify: `src/runtime/service.rs`
- Modify: `tests/run_service.rs`

**Interfaces:**
- Consumes:
  - `PreparingGuard`
  - `RunServiceInner::finalize_preparing_from_guard_drop`
  - existing `SubscriptionLease::drop`
- Produces:
  - `LeaseOwner::release_subscription` checks preparing and active maps
  - test `dropped_detached_create_future_releases_capacity_after_durable_create`
  - private unit test `attached_subscription_drop_before_launch_finalizes_cancelled`

- [ ] **Step 1: Update subscription release to check preparing first**

Replace `LeaseOwner for RunServiceInner` with:

```rust
impl LeaseOwner for RunServiceInner {
    fn release_subscription(self: Arc<Self>, run_id: &str) {
        let preparing_stop = {
            let preparing = lock_preparing(&self);
            preparing
                .get(run_id)
                .filter(|run| run.attachment == RunAttachment::Attached)
                .map(|run| run.stop.clone())
        };
        if let Some(stop) = preparing_stop {
            stop.request(StopReason::Cancelled);
            return;
        }

        let active_stop = {
            let active = lock_active(&self);
            active
                .get(run_id)
                .filter(|run| run.attachment == RunAttachment::Attached)
                .map(|run| run.stop.clone())
        };
        if let Some(stop) = active_stop {
            stop.request(StopReason::Cancelled);
        }
    }
}
```

- [ ] **Step 2: Add dropped detached future integration test**

Append this test after the Task 3 tests:

```rust
#[tokio::test]
async fn dropped_detached_create_future_releases_capacity_after_durable_create() {
    let get_gate = RepositoryGate::new();
    let (service, repository) = service_with_repository_hooks(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(3600),
        },
        vec![agent("fast", ServiceBehavior::Complete)],
        RepositoryHooks {
            create_run: None,
            get_run: Some(Arc::clone(&get_gate)),
        },
    )
    .await
    .unwrap();

    let creator_service = service.clone();
    let create_task = tokio::spawn(async move {
        creator_service
            .create_detached("fast", json!({}), RequestMetadata::default())
            .await
    });
    get_gate.wait_entered().await;
    create_task.abort();
    let _ = create_task.await;
    get_gate.release();

    let run_id = loop {
        if let Some(run_id) = repository.records.lock().await.keys().next().cloned() {
            break run_id;
        }
        tokio::task::yield_now().await;
    };
    wait_for_status(&service, &run_id, RunStatus::Interrupted).await;

    let next = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(&service, &next.run_id, RunStatus::Completed).await;
}
```

- [ ] **Step 3: Add private attached launch-boundary unit test**

At the bottom of `src/runtime/service.rs`, add a `#[cfg(test)]` module that constructs a `PreparingRun` directly, inserts it into `preparing`, drops an attached subscription lease, and promotes the Run:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use crate::history::types::NodeOutputRecord;
    use serde_json::json;

    #[derive(Default)]
    struct UnitRepository {
        runs: tokio::sync::Mutex<BTreeMap<String, RunRecord>>,
        events: tokio::sync::Mutex<Vec<crate::events::protocol::RunEvent>>,
    }

    #[async_trait]
    impl RunRepository for UnitRepository {
        async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
            self.runs.lock().await.insert(
                run.run_id.clone(),
                RunRecord {
                    run_id: run.run_id,
                    request_id: run.request_id,
                    agent_id: run.agent_id,
                    agent_version: run.agent_version,
                    attachment: run.attachment,
                    status: RunStatus::Created,
                    started_at: None,
                    ended_at: None,
                    updated_at: run.created_at,
                    input_summary: run.input_summary,
                    output: None,
                    error_code: None,
                    error_message: None,
                },
            );
            Ok(())
        }

        async fn mark_running(
            &self,
            run_id: &str,
            started_at: DateTime<Utc>,
        ) -> Result<(), HistoryError> {
            let mut runs = self.runs.lock().await;
            let run = runs
                .get_mut(run_id)
                .ok_or_else(|| HistoryError::new("RUN_NOT_FOUND", "run not found"))?;
            run.status = RunStatus::Running;
            run.started_at = Some(started_at);
            run.updated_at = started_at;
            Ok(())
        }

        async fn append_events(
            &self,
            events: &[crate::events::protocol::RunEvent],
        ) -> Result<(), HistoryError> {
            self.events.lock().await.extend_from_slice(events);
            Ok(())
        }

        async fn put_node_output(&self, _output: NodeOutputRecord) -> Result<(), HistoryError> {
            Ok(())
        }

        async fn finish_run(
            &self,
            update: TerminalUpdate,
            event: crate::events::protocol::RunEvent,
        ) -> Result<bool, HistoryError> {
            let mut runs = self.runs.lock().await;
            let run = runs
                .get_mut(&update.run_id)
                .ok_or_else(|| HistoryError::new("RUN_NOT_FOUND", "run not found"))?;
            run.status = update.status;
            run.ended_at = Some(update.ended_at);
            run.updated_at = update.ended_at;
            run.error_code = update.error_code;
            run.error_message = update.error_message;
            drop(runs);
            self.events.lock().await.push(event);
            Ok(true)
        }

        async fn recover_run(
            &self,
            update: TerminalUpdate,
            terminal: crate::events::protocol::RunEvent,
        ) -> Result<crate::events::protocol::RunEvent, HistoryError> {
            self.finish_run(update, terminal.clone()).await?;
            Ok(terminal)
        }

        async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
            Ok(self.runs.lock().await.get(run_id).cloned())
        }

        async fn list_events_after(
            &self,
            run_id: &str,
            after_seq: u64,
            limit: usize,
        ) -> Result<Vec<crate::events::protocol::RunEvent>, HistoryError> {
            Ok(self
                .events
                .lock()
                .await
                .iter()
                .filter(|event| event.run_id == run_id && event.seq > after_seq)
                .take(limit)
                .cloned()
                .collect())
        }

        async fn mark_incomplete_interrupted(
            &self,
            _at: DateTime<Utc>,
        ) -> Result<u64, HistoryError> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn attached_subscription_drop_before_launch_finalizes_cancelled() {
        let repository = Arc::new(UnitRepository::default());
        let repository_trait: Arc<dyn RunRepository> = repository.clone();
        let events = EventHub::new(
            Arc::clone(&repository_trait),
            crate::events::hub::EventHubConfig {
                subscriber_capacity: 8,
                journal_capacity: 32,
                journal_batch_size: 8,
                operation_timeout: Duration::from_secs(1),
            },
        );
        let service = RunService::new(
            CompiledAgentRegistry::new(Vec::new()).unwrap(),
            NodeExecutorRegistry::default(),
            Arc::clone(&repository_trait),
            events,
            RunServiceConfig {
                max_concurrent_runs: 1,
                max_parallel_node_executions: 1,
                max_parallel_branches_per_run: 1,
                run_timeout: Duration::from_secs(1),
            },
        )
        .unwrap();
        let permit = Arc::clone(&service.inner.capacity)
            .try_acquire_owned()
            .unwrap();
        let new_run = NewRun {
            run_id: "run_unit_attached".to_string(),
            request_id: "req_unit_attached".to_string(),
            agent_id: "agent".to_string(),
            agent_version: "sha256:agent".to_string(),
            attachment: RunAttachment::Attached,
            created_at: Utc::now(),
            input_summary: json!({"keys":[], "serialized_bytes":2}),
        };
        repository.create_run(new_run.clone()).await.unwrap();
        let (stop, signal) = stop_pair();
        lock_preparing(&service.inner).insert(
            new_run.run_id.clone(),
            PreparingRun {
                agent: Arc::new(CompiledAgent {
                    id: "agent".to_string(),
                    name: "agent".to_string(),
                    description: String::new(),
                    version_hash: "sha256:agent".to_string(),
                    input_schema: Arc::new(jsonschema::JSONSchema::compile(&json!({})).unwrap()),
                    entry: "missing".to_string(),
                    execution_plan: crate::dsl::compiled::ExecutionPlan::sequential(
                        "missing",
                        Vec::<String>::new(),
                    ),
                    nodes: BTreeMap::new(),
                    templates: Arc::new(handlebars::Handlebars::new()),
                }),
                new_run: new_run.clone(),
                input: json!({}),
                attachment: RunAttachment::Attached,
                stop,
                signal,
                state: Arc::new(RunState::new()),
                permit,
                durable: true,
            },
        );
        service.inner.notify_lifecycle();

        let owner: Arc<dyn LeaseOwner> = service.inner.clone();
        drop(SubscriptionLease::new(owner, &new_run.run_id));
        service.launch(PreparedRun {
            run_id: new_run.run_id.clone(),
            request_id: new_run.request_id.clone(),
            guard: PreparingGuard::new(Arc::clone(&service.inner), new_run.run_id.clone()),
        });

        for _ in 0..200 {
            if let Some(record) = repository.get_run(&new_run.run_id).await.unwrap() {
                if record.status == RunStatus::Cancelled {
                    assert_eq!(record.error_code.as_deref(), Some("RUN_CANCELLED"));
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
        panic!("attached preparing run did not become cancelled");
    }
}
```

- [ ] **Step 4: Run the dropped-future and attached unit tests**

Run:

```bash
cargo test --test run_service dropped_detached_create_future_releases_capacity_after_durable_create -- --nocapture
cargo test runtime::service::tests::attached_subscription_drop_before_launch_finalizes_cancelled -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run all service tests**

Run:

```bash
cargo test --test run_service -- --nocapture
cargo test runtime::service::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/runtime/service.rs tests/run_service.rs
git commit -m "fix: finalize stopped preparing runs"
```

---

### Task 5: Final verification and public-shape guard

**Files:**
- Modify: `docs/superpowers/plans/2026-07-11-preparing-active-lifecycle-ownership.md` only to check off completed steps if executing manually.
- No production code changes unless verification exposes a concrete defect.

**Interfaces:**
- Consumes: all previous task commits.
- Produces: verified A2 branch ready for review or merge.

- [ ] **Step 1: Run formatting**

Run:

```bash
cargo fmt --all -- --check
```

Expected: PASS.

- [ ] **Step 2: Run clippy**

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
git diff --name-only HEAD~4..HEAD
```

Expected: changed files are limited to:

```text
src/runtime/service.rs
tests/run_service.rs
```

The plan file itself may appear if the implementation commits are made on top of the planning commit.

- [ ] **Step 5: Confirm no dependency or migration drift**

Run:

```bash
git diff -- Cargo.toml Cargo.lock migrations
```

Expected: no output.

- [ ] **Step 6: Commit verification notes only if the plan checklist was updated**

```bash
git add docs/superpowers/plans/2026-07-11-preparing-active-lifecycle-ownership.md
git commit -m "docs: record preparing lifecycle verification"
```

Skip this commit when the plan file was not edited during execution.
