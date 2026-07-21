# Durable Recovery and Live-State Finalization Implementation Plan

> **归档状态：历史记录。** 本文不代表当前生产合同；请从[现行文档](../../current/README.md)开始阅读。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `BASE-P1-008` and `BASE-P1-009` by bounding foreground terminal recovery, isolating live EventHub state deterministically, and draining background recovery owners during shutdown.

**Architecture:** `EventHub` remains the recovery boundary. It performs a bounded foreground `recover_run`, validates durable suffixes before live broadcast, isolates run state on uncertainty or reconciliation failure, and owns one deduplicated background recovery task per Run. `RunService` keeps active/preparing ownership unchanged but extends shutdown to wait for EventHub recovery owners with the caller's deadline.

**Tech Stack:** Rust, Tokio, `broadcast`, `watch`, existing `RunRepository`, `EventHub`, `RunService`, SQLite/PostgreSQL repository contracts, in-memory integration tests.

## Global Constraints

- Implement only A4 Durable recovery and live-state finalization from `docs/superpowers/specs/2026-07-11-durable-recovery-finalization-design.md`.
- No cross-process recovery queue, distributed lock, or external worker.
- No repository trait redesign.
- No new public SSE replay or reconnect semantics.
- No attempt to guarantee that a database operation cancelled by client timeout did not commit.
- No metrics backend or structured observability expansion.
- No PostgreSQL or SQLite schema change.
- Preserve existing HTTP, SSE envelope, event envelope, repository trait, DSL, migration, and dependency shapes.
- Foreground direct recovery must be bounded by `EventHubConfig::operation_timeout`.
- Background recovery must be deduplicated to at most one owner per Run.
- EventHub must remove live run state and close subscribers after recovery handoff or post-commit reconciliation failure.
- Only fully validated durable suffixes may be broadcast.
- RunService shutdown must wait for EventHub recovery owners within the caller's shutdown deadline.
- Commit after each task that reaches its verification gate.

---

## File Structure

- Modify `src/events/hub.rs`
  - Add internal `RecoveryRequest`.
  - Add internal recovery-owner registry and recovery change notification.
  - Add `EventHub::wait_for_recoveries(deadline: Duration) -> Result<(), EventError>`.
  - Add `EventHub::retained_recovery_count() -> usize` as a test-facing diagnostic consistent with existing `retained_run_count`.
  - Replace direct unbounded `repository.recover_run` in `recover_terminal` with bounded foreground recovery, live-state isolation, and background handoff.
  - Keep repository trait and event protocol unchanged.

- Modify `src/runtime/service.rs`
  - Extend `RunService::shutdown` to compute remaining deadline after active/preparing drain.
  - Await `self.inner.events.wait_for_recoveries(remaining)` and map its timeout to `SHUTDOWN_TIMEOUT`.

- Modify `tests/event_hub.rs`
  - Extend `MemoryRepository` with recovery/list blocking controls.
  - Add RED tests for foreground recovery timeout handoff, owner deduplication, background convergence, and reconciliation failure cleanup.

- Modify `tests/run_service.rs`
  - Extend `CountingRepository` and service helpers with a recovery gate and configurable EventHub operation timeout.
  - Add RED tests proving shutdown waits for background recovery owners and admission remains unhealthy while recovery is in progress.

- Do not change:
  - `Cargo.toml`
  - `Cargo.lock`
  - `migrations/*`
  - `src/api/*`
  - `src/events/protocol.rs`
  - `src/history/repository.rs`
  - DSL files unless a compile error proves a needed internal import adjustment.

---

### Task 1: Add RED EventHub recovery-owner and live-isolation coverage

**Files:**
- Modify: `tests/event_hub.rs`

**Interfaces:**
- Consumes existing `EventHub::recover_terminal`, `EventHub::retained_run_count`, `EventSubscription::recv`, `MemoryRepository`, `TerminalUpdate`, `RunEvent`.
- Produces tests that require:
  - `EventHub::retained_recovery_count(&self) -> usize`
  - `EventHub::wait_for_recoveries(&self, deadline: Duration) -> Result<(), EventError>`
  - bounded foreground recovery
  - live-state isolation after recovery handoff
  - deduplicated background owners

- [ ] **Step 1: Extend the EventHub memory repository fixture**

In `tests/event_hub.rs`, replace the current `MemoryRepository` struct with this version:

```rust
#[derive(Default)]
struct MemoryRepository {
    events: Mutex<BTreeMap<String, Vec<RunEvent>>>,
    outputs: Mutex<Vec<NodeOutputRecord>>,
    terminal_updates: Mutex<Vec<TerminalUpdate>>,
    fail_appends: AtomicBool,
    block_appends: AtomicBool,
    commit_then_block_appends: AtomicBool,
    append_called: Notify,
    allow_append: Notify,
    terminal_called: Notify,
    allow_terminal: Notify,
    block_terminal: AtomicBool,
    recover_calls: AtomicUsize,
    block_recover: AtomicBool,
    allow_recover: Notify,
    list_calls: AtomicUsize,
    block_list: AtomicBool,
    allow_list: Notify,
    override_list: Mutex<Option<Vec<RunEvent>>>,
}
```

Add these helper methods to the existing `impl MemoryRepository` block:

```rust
    async fn wait_recover_calls(&self, expected: usize) {
        for _ in 0..200 {
            if self.recover_calls.load(Ordering::SeqCst) >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "recover_run was called {} times, expected at least {expected}",
            self.recover_calls.load(Ordering::SeqCst)
        );
    }

    async fn set_override_list(&self, events: Vec<RunEvent>) {
        *self.override_list.lock().await = Some(events);
    }
```

Update `recover_run` in the `RunRepository for MemoryRepository` impl to count and optionally block direct/background recovery:

```rust
    async fn recover_run(
        &self,
        update: TerminalUpdate,
        mut terminal: RunEvent,
    ) -> Result<RunEvent, HistoryError> {
        self.recover_calls.fetch_add(1, Ordering::SeqCst);
        if self.block_recover.load(Ordering::SeqCst) {
            self.allow_recover.notified().await;
        }
        terminal.seq = self
            .events
            .lock()
            .await
            .get(&update.run_id)
            .and_then(|events| events.last())
            .map_or(1, |event| event.seq + 1);
        self.finish_run(update, terminal.clone()).await?;
        Ok(terminal)
    }
```

Update `list_events_after` to support blocked and overridden reconciliation reads:

```rust
    async fn list_events_after(
        &self,
        run_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, HistoryError> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        if self.block_list.load(Ordering::SeqCst) {
            self.allow_list.notified().await;
        }
        if let Some(events) = self.override_list.lock().await.clone() {
            return Ok(events
                .into_iter()
                .filter(|event| event.seq > after_seq)
                .take(limit)
                .collect());
        }
        Ok(self
            .events
            .lock()
            .await
            .get(run_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|event| event.seq > after_seq)
            .take(limit)
            .collect())
    }
```

- [ ] **Step 2: Add a shared terminal-update helper**

Append this helper near the existing `scope_for` and `config` helpers:

```rust
fn failed_update(run_id: &str, second: u32) -> TerminalUpdate {
    TerminalUpdate::new(
        run_id,
        RunStatus::Failed,
        at(second),
        None,
        Some("INFRASTRUCTURE_FAILURE".to_string()),
        Some("runtime infrastructure failed".to_string()),
    )
    .unwrap()
}
```

- [ ] **Step 3: Add RED test for foreground timeout handoff and background convergence**

Append this test near `recovery_derives_terminal_after_an_uncertain_append_commit`:

```rust
#[tokio::test]
async fn recovery_timeout_isolates_live_state_and_hands_off_one_owner() {
    let repository = Arc::new(MemoryRepository::default());
    repository.fail_appends.store(true, Ordering::SeqCst);
    repository.block_recover.store(true, Ordering::SeqCst);
    let hub = EventHub::new(
        repository.clone(),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 8,
            journal_batch_size: 4,
            operation_timeout: Duration::from_millis(20),
        },
    );
    let mut subscriber = hub.subscribe(RUN_ID).await;

    assert_eq!(
        hub.publish(scope(None), RunEventType::RunCreated, json!({}))
            .await
            .unwrap_err()
            .code(),
        "SYNTHETIC_WRITE_FAILURE"
    );

    let error = tokio::time::timeout(
        Duration::from_millis(250),
        hub.recover_terminal(
            scope(None),
            RunEventType::RunFailed,
            failed_update(RUN_ID, 21),
            "INFRASTRUCTURE_FAILURE",
            "runtime infrastructure failed",
            json!({}),
        ),
    )
    .await
    .expect("foreground recovery must be bounded")
    .unwrap_err();

    assert_eq!(error.code(), "JOURNAL_OPERATION_TIMEOUT");
    assert_eq!(hub.retained_run_count().await, 0);
    assert_eq!(hub.retained_recovery_count().await, 1);
    let closed = tokio::time::timeout(Duration::from_millis(50), subscriber.recv())
        .await
        .expect("subscriber must be closed after recovery handoff")
        .unwrap_err();
    assert_eq!(closed.code(), "SUBSCRIPTION_CLOSED");
    repository.wait_recover_calls(2).await;

    repository.block_recover.store(false, Ordering::SeqCst);
    repository.allow_recover.notify_waiters();
    hub.wait_for_recoveries(Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(hub.retained_recovery_count().await, 0);
    assert_eq!(repository.terminal_updates.lock().await.len(), 1);
    assert_eq!(repository.stored_sequences(RUN_ID).await, vec![1]);
}
```

Expected RED before production changes: compile failure for missing `retained_recovery_count` and `wait_for_recoveries`, or timeout because foreground recovery is unbounded.

- [ ] **Step 4: Add RED test for duplicate foreground callers reusing one owner**

Append this test after the previous one:

```rust
#[tokio::test]
async fn duplicate_recovery_timeouts_reuse_the_same_background_owner() {
    let repository = Arc::new(MemoryRepository::default());
    repository.fail_appends.store(true, Ordering::SeqCst);
    repository.block_recover.store(true, Ordering::SeqCst);
    let hub = EventHub::new(
        repository.clone(),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 8,
            journal_batch_size: 4,
            operation_timeout: Duration::from_millis(20),
        },
    );

    assert_eq!(
        hub.publish(scope(None), RunEventType::RunCreated, json!({}))
            .await
            .unwrap_err()
            .code(),
        "SYNTHETIC_WRITE_FAILURE"
    );

    for second in [22, 23] {
        let error = tokio::time::timeout(
            Duration::from_millis(250),
            hub.recover_terminal(
                scope(None),
                RunEventType::RunFailed,
                failed_update(RUN_ID, second),
                "INFRASTRUCTURE_FAILURE",
                "runtime infrastructure failed",
                json!({}),
            ),
        )
        .await
        .expect("foreground recovery must be bounded")
        .unwrap_err();
        assert_eq!(error.code(), "JOURNAL_OPERATION_TIMEOUT");
    }

    assert_eq!(hub.retained_run_count().await, 0);
    assert_eq!(hub.retained_recovery_count().await, 1);
    tokio::time::timeout(Duration::from_millis(50), repository.wait_recover_calls(3))
        .await
        .expect("duplicate recovery calls must share one background owner");
    assert_eq!(
        repository.recover_calls.load(Ordering::SeqCst),
        3,
        "two foreground attempts plus one deduplicated background owner"
    );

    repository.block_recover.store(false, Ordering::SeqCst);
    repository.allow_recover.notify_waiters();
    hub.wait_for_recoveries(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(hub.retained_recovery_count().await, 0);
}
```

Expected RED before production changes: compile failure for missing EventHub recovery diagnostics or timeout because recovery is unbounded.

- [ ] **Step 5: Add RED tests for post-commit reconciliation cleanup**

Append these tests after `recovery_derives_terminal_after_an_uncertain_append_commit`:

```rust
#[tokio::test]
async fn authoritative_recovery_terminal_with_blocked_reconciliation_closes_live_state() {
    let repository = Arc::new(MemoryRepository::default());
    repository
        .commit_then_block_appends
        .store(true, Ordering::SeqCst);
    repository.block_list.store(true, Ordering::SeqCst);
    let hub = EventHub::new(
        repository.clone(),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 8,
            journal_batch_size: 4,
            operation_timeout: Duration::from_millis(20),
        },
    );
    let mut subscriber = hub.subscribe(RUN_ID).await;

    assert_eq!(
        hub.publish(scope(None), RunEventType::RunCreated, json!({}))
            .await
            .unwrap_err()
            .code(),
        "JOURNAL_OPERATION_TIMEOUT"
    );

    let error = hub
        .recover_terminal(
            scope(None),
            RunEventType::RunFailed,
            failed_update(RUN_ID, 24),
            "INFRASTRUCTURE_FAILURE",
            "runtime infrastructure failed",
            json!({}),
        )
        .await
        .unwrap_err();

    assert_eq!(error.code(), "JOURNAL_OPERATION_TIMEOUT");
    assert_eq!(hub.retained_run_count().await, 0);
    let closed = tokio::time::timeout(Duration::from_millis(50), subscriber.recv())
        .await
        .expect("subscriber must be closed after reconciliation timeout")
        .unwrap_err();
    assert_eq!(closed.code(), "SUBSCRIPTION_CLOSED");
    assert_eq!(repository.terminal_updates.lock().await.len(), 1);
    assert_eq!(repository.stored_sequences(RUN_ID).await, vec![1, 2]);
}

#[tokio::test]
async fn authoritative_recovery_terminal_with_mismatched_history_closes_without_broadcast() {
    let repository = Arc::new(MemoryRepository::default());
    repository
        .commit_then_block_appends
        .store(true, Ordering::SeqCst);
    let hub = EventHub::new(
        repository.clone(),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 8,
            journal_batch_size: 4,
            operation_timeout: Duration::from_millis(20),
        },
    );
    let mut subscriber = hub.subscribe(RUN_ID).await;

    assert_eq!(
        hub.publish(scope(None), RunEventType::RunCreated, json!({}))
            .await
            .unwrap_err()
            .code(),
        "JOURNAL_OPERATION_TIMEOUT"
    );
    let wrong_terminal = RunEvent::error(
        RunEventType::RunCancelled,
        2,
        scope(None),
        "RUN_CANCELLED",
        "run cancelled by explicit request",
        json!({}),
    );
    repository.set_override_list(vec![wrong_terminal]).await;

    let error = hub
        .recover_terminal(
            scope(None),
            RunEventType::RunFailed,
            failed_update(RUN_ID, 25),
            "INFRASTRUCTURE_FAILURE",
            "runtime infrastructure failed",
            json!({}),
        )
        .await
        .unwrap_err();

    assert_eq!(error.code(), "HISTORY_TERMINAL_EVENT_MISMATCH");
    assert_eq!(hub.retained_run_count().await, 0);
    let closed = tokio::time::timeout(Duration::from_millis(50), subscriber.recv())
        .await
        .expect("subscriber must be closed after reconciliation mismatch")
        .unwrap_err();
    assert_eq!(closed.code(), "SUBSCRIPTION_CLOSED");
}
```

Expected RED before production changes: the blocked reconciliation test times out or retains state; the mismatch test returns an error but retains EventHub state and leaves subscribers open.

- [ ] **Step 6: Run RED EventHub tests**

Run:

```bash
cargo test --test event_hub recovery_timeout_isolates_live_state_and_hands_off_one_owner -- --nocapture
cargo test --test event_hub duplicate_recovery_timeouts_reuse_the_same_background_owner -- --nocapture
cargo test --test event_hub authoritative_recovery_terminal_with_blocked_reconciliation_closes_live_state -- --nocapture
cargo test --test event_hub authoritative_recovery_terminal_with_mismatched_history_closes_without_broadcast -- --nocapture
```

Expected: all fail for the reasons listed in the preceding steps.

- [ ] **Step 7: Commit RED EventHub tests**

```bash
git add tests/event_hub.rs
git commit -m "test: cover durable recovery handoff"
```

---

### Task 2: Implement bounded EventHub recovery and live-state isolation

**Files:**
- Modify: `src/events/hub.rs`
- Test: `tests/event_hub.rs`

**Interfaces:**
- Consumes:
  - `RunRepository::recover_run(update: TerminalUpdate, terminal: RunEvent) -> Result<RunEvent, HistoryError>`
  - `EventHubConfig::operation_timeout`
  - existing `EventHub::remove_run_state`
- Produces:
  - `EventHub::wait_for_recoveries(&self, deadline: Duration) -> Result<(), EventError>`
  - `EventHub::retained_recovery_count(&self) -> usize`
  - internal `RecoveryRequest`
  - internal recovery-owner registry
  - bounded foreground `recover_terminal`

- [ ] **Step 1: Update EventHub imports and inner state**

In `src/events/hub.rs`, change the Tokio import to include `watch`:

```rust
use tokio::{
    sync::{broadcast, watch, Mutex},
    time::timeout,
};
```

Add this cloneable request type after `EventRunState`:

```rust
#[derive(Clone)]
struct RecoveryRequest {
    scope: RunEventScope,
    event_type: RunEventType,
    update: TerminalUpdate,
    code: String,
    message: String,
    data: Value,
}

impl RecoveryRequest {
    fn run_id(&self) -> &str {
        &self.scope.run_id
    }

    fn terminal_event(&self, seq: u64) -> RunEvent {
        RunEvent::error(
            self.event_type,
            seq,
            self.scope.clone(),
            self.code.clone(),
            self.message.clone(),
            self.data.clone(),
        )
    }
}
```

Extend `EventHubInner`:

```rust
struct EventHubInner {
    repository: Arc<dyn RunRepository>,
    journal: EventJournal,
    states: Mutex<HashMap<String, Arc<Mutex<EventRunState>>>>,
    recoveries: Mutex<HashMap<String, ()>>,
    recovery_changed: watch::Sender<u64>,
    subscriber_capacity: usize,
    operation_timeout: Duration,
}
```

Update `EventHub::new` to initialize the new fields:

```rust
let (recovery_changed, _) = watch::channel(0);
Self {
    inner: Arc::new(EventHubInner {
        repository,
        journal,
        states: Mutex::new(HashMap::new()),
        recoveries: Mutex::new(HashMap::new()),
        recovery_changed,
        subscriber_capacity: config.subscriber_capacity.max(1),
        operation_timeout: config.operation_timeout,
    }),
}
```

- [ ] **Step 2: Add recovery-owner diagnostics and drain API**

Add these public methods near `retained_run_count`:

```rust
    pub async fn retained_recovery_count(&self) -> usize {
        self.inner.recoveries.lock().await.len()
    }

    pub async fn wait_for_recoveries(&self, deadline: Duration) -> Result<(), EventError> {
        let mut changed = self.inner.recovery_changed.subscribe();
        let wait = async {
            loop {
                if self.inner.recoveries.lock().await.is_empty() {
                    return Ok(());
                }
                changed
                    .changed()
                    .await
                    .map_err(|_| EventError::JournalClosed)?;
            }
        };
        timeout(deadline, wait)
            .await
            .map_err(|_| EventError::JournalOperationTimeout)?
    }
```

Add this helper to `impl EventHubInner` or as a private free function near `commit_live_event`:

```rust
impl EventHubInner {
    fn notify_recovery_changed(&self) {
        self.recovery_changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
}
```

- [ ] **Step 3: Add live-state isolation and recovery-owner helpers**

Rename or wrap `remove_run_state` with this exact cleanup primitive:

```rust
    async fn isolate_run_state(&self, run_id: &str, expected: &Arc<Mutex<EventRunState>>) {
        let mut states = self.inner.states.lock().await;
        if states
            .get(run_id)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            states.remove(run_id);
        }
    }
```

Keep existing callers working by either replacing `remove_run_state(...)` calls with `isolate_run_state(...)` or by changing `remove_run_state` to delegate to `isolate_run_state`.

Add these owner helpers inside `impl EventHub`:

```rust
    async fn start_recovery_owner(&self, request: RecoveryRequest) {
        let run_id = request.run_id().to_string();
        {
            let mut recoveries = self.inner.recoveries.lock().await;
            if recoveries.contains_key(&run_id) {
                return;
            }
            recoveries.insert(run_id.clone(), ());
        }
        self.inner.notify_recovery_changed();

        let hub = self.clone();
        tokio::spawn(async move {
            hub.run_recovery_owner(run_id, request).await;
        });
    }

    async fn run_recovery_owner(&self, run_id: String, request: RecoveryRequest) {
        let terminal = request.terminal_event(1);
        if let Err(error) = self
            .inner
            .repository
            .recover_run(request.update.clone(), terminal)
            .await
        {
            tracing::error!(
                run_id,
                code = error.code(),
                "background terminal recovery failed"
            );
        }
        self.finish_recovery_owner(&run_id).await;
    }

    async fn finish_recovery_owner(&self, run_id: &str) {
        self.inner.recoveries.lock().await.remove(run_id);
        self.inner.notify_recovery_changed();
    }
```

- [ ] **Step 4: Add bounded foreground recovery helper**

Add this helper inside `impl EventHub`:

```rust
    async fn recover_terminal_direct(
        &self,
        request: RecoveryRequest,
    ) -> Result<Option<RunEvent>, EventError> {
        let run_id = request.run_id().to_string();
        let state_handle = self.run_state(&run_id).await;
        let mut state = state_handle.lock().await;
        ensure_sequence_available(&state)?;
        let event = request.terminal_event(state.next_seq);
        let recovered = timeout(
            self.inner.operation_timeout,
            self.inner
                .repository
                .recover_run(request.update.clone(), event),
        )
        .await;

        let authoritative = match recovered {
            Ok(Ok(authoritative)) => authoritative,
            Ok(Err(error)) => {
                drop(state);
                self.isolate_run_state(&run_id, &state_handle).await;
                self.start_recovery_owner(request).await;
                return Err(EventError::History(error));
            }
            Err(_) => {
                drop(state);
                self.isolate_run_state(&run_id, &state_handle).await;
                self.start_recovery_owner(request).await;
                return Err(EventError::JournalOperationTimeout);
            }
        };

        let reconcile = self
            .reconcile_durable_through(&run_id, &mut state, &authoritative)
            .await;
        match reconcile {
            Ok(()) => {
                drop(state);
                self.isolate_run_state(&run_id, &state_handle).await;
                Ok((authoritative.event_type == request.event_type).then_some(authoritative))
            }
            Err(error) => {
                drop(state);
                self.isolate_run_state(&run_id, &state_handle).await;
                Err(error)
            }
        }
    }
```

This helper deliberately starts the background owner only when the authoritative terminal is unknown. If `recover_run` returns an authoritative terminal and reconciliation then fails, EventHub isolates live state and returns the reconciliation error without spawning another owner.

- [ ] **Step 5: Replace the unbounded direct recovery branch**

In `recover_terminal`, replace this block:

```rust
        let run_id = scope.run_id.clone();
        let state_handle = self.run_state(&run_id).await;
        let mut state = state_handle.lock().await;
        ensure_sequence_available(&state)?;
        let event = RunEvent::error(event_type, state.next_seq, scope, code, message, data);
        let authoritative = self.inner.repository.recover_run(update, event).await?;
        self.reconcile_durable_through(&run_id, &mut state, &authoritative)
            .await?;
        drop(state);
        self.remove_run_state(&run_id, &state_handle).await;
        Ok((authoritative.event_type == event_type).then_some(authoritative))
```

with:

```rust
        let request = RecoveryRequest {
            scope,
            event_type,
            update,
            code,
            message,
            data,
        };
        self.recover_terminal_direct(request).await
```

Ensure earlier successful `publish_terminal` paths now call `isolate_run_state` instead of `remove_run_state`, or leave `remove_run_state` as a delegating wrapper.

- [ ] **Step 6: Run EventHub GREEN tests**

Run:

```bash
cargo fmt --all
cargo test --test event_hub recovery_timeout_isolates_live_state_and_hands_off_one_owner -- --nocapture
cargo test --test event_hub duplicate_recovery_timeouts_reuse_the_same_background_owner -- --nocapture
cargo test --test event_hub authoritative_recovery_terminal_with_blocked_reconciliation_closes_live_state -- --nocapture
cargo test --test event_hub authoritative_recovery_terminal_with_mismatched_history_closes_without_broadcast -- --nocapture
cargo test --test event_hub -- --nocapture
```

Expected: all pass.

- [ ] **Step 7: Commit EventHub implementation**

```bash
git add src/events/hub.rs tests/event_hub.rs
git commit -m "fix: bound durable recovery handoff"
```

---

### Task 3: Add RunService shutdown drain for EventHub recovery owners

**Files:**
- Modify: `src/runtime/service.rs`
- Modify: `tests/run_service.rs`

**Interfaces:**
- Consumes:
  - `EventHub::wait_for_recoveries(&self, deadline: Duration) -> Result<(), EventError>`
  - `EventError::JournalOperationTimeout`
  - existing `RunService::shutdown(deadline: Duration) -> Result<(), ServiceError>`
- Produces:
  - shutdown waits for background recovery owners
  - `SHUTDOWN_TIMEOUT` when recovery owners exceed the remaining shutdown deadline

- [ ] **Step 1: Extend run_service recovery test hooks**

In `tests/run_service.rs`, replace `RepositoryHooks` with:

```rust
#[derive(Clone, Default)]
struct RepositoryHooks {
    create_run: Option<Arc<RepositoryGate>>,
    get_run: Option<Arc<RepositoryGate>>,
    recover_run: Option<Arc<RecoveryGate>>,
}
```

Add this recovery gate after `RepositoryGate`:

```rust
struct RecoveryGate {
    calls: AtomicUsize,
    entered: watch::Sender<usize>,
    release: Notify,
    released: AtomicBool,
}

impl RecoveryGate {
    fn new() -> Arc<Self> {
        let (entered, _) = watch::channel(0);
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            entered,
            release: Notify::new(),
            released: AtomicBool::new(false),
        })
    }

    async fn block_until_released(&self) {
        let calls = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.entered.send(calls);
        if !self.released.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
    }

    async fn wait_calls(&self, expected: usize) {
        let mut entered = self.entered.subscribe();
        while *entered.borrow() < expected {
            entered.changed().await.unwrap();
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}
```

Update `CountingRepository::recover_run`:

```rust
    async fn recover_run(
        &self,
        update: TerminalUpdate,
        mut terminal: RunEvent,
    ) -> Result<RunEvent, HistoryError> {
        if let Some(gate) = self.hooks.recover_run.as_ref() {
            gate.block_until_released().await;
        }
        terminal.seq = self
            .events
            .lock()
            .await
            .get(&update.run_id)
            .and_then(|events| events.last())
            .map_or(1, |event| event.seq + 1);
        self.finish_run(update, terminal.clone()).await?;
        Ok(terminal)
    }
```

- [ ] **Step 2: Add configurable EventHub operation timeout to service tests**

Replace `service_with_repository_hooks` with a delegating helper:

```rust
async fn service_with_repository_hooks(
    config: RunServiceConfig,
    agents: Vec<Arc<CompiledAgent>>,
    hooks: RepositoryHooks,
) -> Result<(RunService, Arc<CountingRepository>), ServiceError> {
    service_with_repository_hooks_and_event_timeout(
        config,
        agents,
        hooks,
        Duration::from_secs(1),
    )
    .await
}

async fn service_with_repository_hooks_and_event_timeout(
    config: RunServiceConfig,
    agents: Vec<Arc<CompiledAgent>>,
    hooks: RepositoryHooks,
    operation_timeout: Duration,
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
            operation_timeout,
        },
    );
    let agents = CompiledAgentRegistry::new(agents).unwrap();
    let mut executors = NodeExecutorRegistry::default();
    executors.register(ServiceNode).unwrap();
    let service = RunService::new(agents, executors, repository_trait, events, config)?;
    Ok((service, repository))
}
```

- [ ] **Step 3: Add RED RunService shutdown-drain test**

Append this test near `permanent_journal_failure_rejects_later_runs_and_marks_service_unhealthy`:

```rust
#[tokio::test]
async fn shutdown_waits_for_background_recovery_owner() {
    let recover_gate = RecoveryGate::new();
    let (service, repository) = service_with_repository_hooks_and_event_timeout(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(3600),
        },
        vec![agent("fast", ServiceBehavior::Complete)],
        RepositoryHooks {
            create_run: None,
            get_run: None,
            recover_run: Some(Arc::clone(&recover_gate)),
        },
        Duration::from_millis(20),
    )
    .await
    .unwrap();
    repository.fail_appends.store(true, Ordering::SeqCst);

    let created = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(250), recover_gate.wait_calls(2))
        .await
        .expect("foreground timeout must hand off to a background recovery owner");
    assert!(!service.is_healthy());

    let error = service
        .shutdown(Duration::from_millis(40))
        .await
        .unwrap_err();
    assert_eq!(error.code(), "SHUTDOWN_TIMEOUT");

    recover_gate.release();
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    let recovered = service.get_run(&created.run_id).await.unwrap();
    assert_eq!(recovered.status, RunStatus::Failed);
    assert_eq!(
        recovered.error_code.as_deref(),
        Some("INFRASTRUCTURE_FAILURE")
    );
}
```

Expected RED before service implementation: shutdown returns before the blocked background recovery owner completes.

- [ ] **Step 4: Add RED admission-unhealthy assertion**

Append this test after the previous one:

```rust
#[tokio::test]
async fn recovery_handoff_releases_active_ownership_but_keeps_service_unhealthy() {
    let recover_gate = RecoveryGate::new();
    let (service, repository) = service_with_repository_hooks_and_event_timeout(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(3600),
        },
        vec![agent("fast", ServiceBehavior::Complete)],
        RepositoryHooks {
            create_run: None,
            get_run: None,
            recover_run: Some(Arc::clone(&recover_gate)),
        },
        Duration::from_millis(20),
    )
    .await
    .unwrap();
    repository.fail_appends.store(true, Ordering::SeqCst);

    let created = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(250), recover_gate.wait_calls(2))
        .await
        .expect("foreground timeout must hand off to a background recovery owner");

    let error = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "RUN_SERVICE_UNAVAILABLE");

    recover_gate.release();
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    assert_eq!(
        service.get_run(&created.run_id).await.unwrap().status,
        RunStatus::Failed
    );
}
```

Expected after Task 2: this guard passes only if recovery handoff releases foreground ownership, keeps admission unhealthy, and lets shutdown complete after the background owner is released.

- [ ] **Step 5: Run RED RunService tests**

Run:

```bash
cargo test --test run_service shutdown_waits_for_background_recovery_owner -- --nocapture
cargo test --test run_service recovery_handoff_releases_active_ownership_but_keeps_service_unhealthy -- --nocapture
```

Expected: `shutdown_waits_for_background_recovery_owner` fails because shutdown does not wait for EventHub recoveries.

- [ ] **Step 6: Implement shutdown recovery drain**

In `src/runtime/service.rs`, change the `std::time` import to include `Instant`:

```rust
    time::{Duration, Instant},
```

In `RunService::shutdown`, replace the existing body:

```rust
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
```

with:

```rust
        self.inner.accepting.store(false, Ordering::Release);
        let started = Instant::now();
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
        let remaining = deadline
            .checked_sub(started.elapsed())
            .ok_or_else(|| ServiceError::new("SHUTDOWN_TIMEOUT", "run shutdown timed out"))?;
        self.inner
            .events
            .wait_for_recoveries(remaining)
            .await
            .map_err(|error| {
                if error.code() == "JOURNAL_OPERATION_TIMEOUT" {
                    ServiceError::new("SHUTDOWN_TIMEOUT", "run shutdown timed out")
                } else {
                    ServiceError::new(error.code(), error.to_string())
                }
            })?;
        Ok(())
```

- [ ] **Step 7: Run RunService GREEN suite**

Run:

```bash
cargo fmt --all
cargo test --test run_service shutdown_waits_for_background_recovery_owner -- --nocapture
cargo test --test run_service recovery_handoff_releases_active_ownership_but_keeps_service_unhealthy -- --nocapture
cargo test --test run_service -- --nocapture
```

Expected: all pass.

- [ ] **Step 8: Commit RunService drain**

```bash
git add src/runtime/service.rs tests/run_service.rs
git commit -m "fix: drain recovery owners on shutdown"
```

---

### Task 4: Final verification and public-shape guard

**Files:**
- No production code changes unless verification exposes a concrete defect.

**Interfaces:**
- Consumes all previous A4 commits.
- Produces verified A4 branch ready for local merge.

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
src/events/hub.rs
src/runtime/service.rs
tests/event_hub.rs
tests/run_service.rs
```

The A4 design and plan docs may also appear when the branch includes documentation commits.

- [ ] **Step 5: Confirm no dependency or migration drift**

Run:

```bash
git diff -- Cargo.toml Cargo.lock migrations
```

Expected: no output.

- [ ] **Step 6: Confirm repository/API/protocol/DSL public shape did not drift**

Run:

```bash
git diff -- src/api src/events/protocol.rs src/history/repository.rs src/dsl
```

Expected: no output.

- [ ] **Step 7: Commit verification notes only if this plan checklist was edited**

```bash
git add docs/superpowers/plans/2026-07-11-durable-recovery-finalization.md
git commit -m "docs: record durable recovery verification"
```

Skip this commit when the plan file was not edited during execution.
