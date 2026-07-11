# Live-Only SSE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace public SSE replay with one atomic live-only Attached stream, immediate disconnect cancellation, and a transport-owned five-second keepalive while preserving durable event journaling and Detached polling.

**Architecture:** `POST /v1/agents/{agent_id}/runs/stream` subscribes to a newly opened EventHub broadcast channel before execution starts and owns the only subscription lease. EventHub persists and sequences events before broadcasting but no longer keeps a replay ring or exposes history reads; dropping the Attached lease requests cancellation immediately. The HTTP layer owns keepalive timing, while Detached Runs remain independent and are observed through `GET /v1/runs/{run_id}`.

**Tech Stack:** Rust 2021, Tokio, Axum 0.7 SSE, Serde/YAML, SQLx SQLite/PostgreSQL, Tower integration tests

## Global Constraints

- Remove `GET /v1/runs/{run_id}/events`, `after_seq`, and `Last-Event-ID` recovery behavior without a compatibility adapter.
- Keep the event envelope at `schema_version: 1`; retain monotonic `seq` and SSE `id` for ordering and audit correlation only.
- Persist journal events before broadcast and preserve SQLite/PostgreSQL event rows, terminal transactions, uncertain-commit recovery, and repository `list_events_after` for internal recovery only.
- Subscribe before launching every Attached Run; do not add a live subscription operation for Detached Runs.
- End SSE immediately after emitting a durable terminal event.
- Cancel an active Attached Run immediately when its sole subscription lease drops; do not retain a reconnect grace timer.
- Configure `runtime.sse_keep_alive_interval` as a strictly positive duration; the checked-in value must be exactly `5s`.
- Keep `runtime.run_timeout: 5m`; do not add a separate SSE maximum duration.
- Remove `runtime.attached_reconnect_grace` and `runtime.replay_ring_capacity`; strict configuration must reject both as unknown.
- Preserve parallel branch draining and exactly one durable Run terminal state.
- Do not change Agent DSL, Run/event schemas, database tables, or migrations.

## File Responsibility Map

- `src/api/formal/{routes,sse}.rs`: expose atomic Attached streaming, cursor-free failures, immediate terminal EOF, and HTTP-owned keepalive.
- `src/runtime/{attachment,service}.rs`: own one live receiver/lease, subscribe before launch, and cancel on lease drop.
- `src/events/hub.rs`: sequence, durably journal, and broadcast; read history only for internal terminal recovery.
- `src/config.rs`, `config/platform.yaml`, `src/main.rs`: define, validate, and wire the strict transport setting.
- `tests/{api,run_service,event_hub,platform_config_v1}.rs`: enforce the new behavior; coordinator/scheduler tests only need constructor updates.
- `README.md`, `docs/formal-v1-breaking-changes.md`: document the client migration and deletion rationale.

At execution start, record the pre-implementation commit with `BASE_SHA=$(git rev-parse HEAD)` and retain that value for final diff review and code review.

---

### Task 1: Delete the public replay route and cursor contract

**Files:**
- Modify: `src/api/formal/routes.rs:1-220`
- Modify: `src/api/formal/sse.rs:1-87`
- Modify: `tests/api.rs:1-596`

**Interfaces:**
- Consumes: existing `RunService::create_attached` and `RunSubscription::recv`
- Produces: no `/v1/runs/:run_id/events` route and `transport_error_payload(code: &'static str) -> serde_json::Value`

- [ ] **Step 1: Replace replay tests with a failing route-absence test**

Delete the two replay tests plus `parallel_fixture`, `parallel_agent`, `ApiBehavior::Next`, and their now-unused imports. Add:

```rust
#[tokio::test]
async fn event_replay_route_and_recovery_headers_are_not_supported() {
    let (app, service) = fixture(ApiAuth::disabled(), 4).await;
    let created = app
        .clone()
        .oneshot(request(
            Method::POST,
            "/v1/agents/fast/runs",
            Some(json!({"text":"hello"})),
        ))
        .await
        .unwrap();
    let created = json_body(created).await;
    let run_id = created["data"]["run_id"].as_str().unwrap();
    wait_for_status(&service, run_id, RunStatus::Completed).await;

    for uri in [
        format!("/v1/runs/{run_id}/events"),
        format!("/v1/runs/{run_id}/events?after_seq=0"),
    ] {
        let response = app
            .clone()
            .oneshot(request(Method::GET, &uri, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    let mut request = request(
        Method::GET,
        &format!("/v1/runs/{run_id}/events"),
        None,
    );
    request
        .headers_mut()
        .insert("last-event-id", "3".parse().unwrap());
    assert_eq!(
        app.oneshot(request).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
}
```

- [ ] **Step 2: Verify the test fails against the old route**

Run: `cargo test --test api event_replay_route_and_recovery_headers_are_not_supported -- --exact --nocapture`

Expected: FAIL because the old route returns SSE instead of `404`.

- [ ] **Step 3: Remove the route and handler**

Remove `Query`, `serde::Deserialize`, `EventQuery`, and `subscribe_events`. The route list becomes:

```rust
let v1 = Router::new()
    .route("/v1/agents", get(list_agents))
    .route("/v1/agents/:agent_id", get(get_agent))
    .route(
        "/v1/agents/:agent_id/runs/stream",
        post(create_attached_run),
    )
    .route("/v1/agents/:agent_id/runs", post(create_detached_run))
    .route("/v1/runs/:run_id", get(get_run).delete(cancel_run))
    .route_layer(middleware::from_fn(
        move |headers: HeaderMap, request: Request<Body>, next: Next| {
            let auth = auth.clone();
            async move {
                if !auth.accepts(&headers) {
                    return Err(ApiError::unauthorized());
                }
                Ok::<Response, ApiError>(next.run(request).await)
            }
        },
    ));
```

- [ ] **Step 4: Add a failing unit contract for cursor-free failures**

Append to `src/api/formal/sse.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::transport_error_payload;

    #[test]
    fn transport_error_does_not_advertise_recovery() {
        let payload = transport_error_payload("SUBSCRIBER_LAGGED");
        assert_eq!(payload["code"], "SUBSCRIBER_LAGGED");
        assert_eq!(payload["message"], "event stream closed");
        assert_eq!(payload["data"], serde_json::json!({}));
        assert!(payload.get("after_seq").is_none());
        assert!(!payload.to_string().contains("reconnect"));
    }
}
```

Run: `cargo test --lib transport_error_does_not_advertise_recovery -- --exact --nocapture`

Expected: FAIL to compile because the helper is absent.

- [ ] **Step 5: Implement cursor-free failure encoding**

Remove the special `ReplayFinished` arm. Replace the general error arm and helper with:

```rust
Err(error) => {
    tracing::debug!(
        run_id = subscription.run_id,
        code = error.code(),
        last_seq = subscription.last_seq(),
        "formal SSE subscription closed"
    );
    match transport_error(error.code()) {
        Ok(event) => Some((Ok(event), None)),
        Err(encoding_error) => {
            tracing::error!(
                run_id = subscription.run_id,
                code = "SSE_ENCODE_FAILED",
                error = %encoding_error,
                "formal SSE transport error encoding failed"
            );
            None
        }
    }
}
```

```rust
fn transport_error(code: &'static str) -> Result<Event, axum::Error> {
    Event::default()
        .event("transport.error")
        .json_data(transport_error_payload(code))
}

fn transport_error_payload(code: &'static str) -> serde_json::Value {
    serde_json::json!({
        "code": code,
        "message": "event stream closed",
        "data": {},
    })
}
```

- [ ] **Step 6: Verify and commit the HTTP contract**

Run:

```bash
cargo test --lib transport_error_does_not_advertise_recovery -- --exact --nocapture
cargo test --test api -- --nocapture
```

Expected: PASS.

Commit:

```bash
git add src/api/formal/routes.rs src/api/formal/sse.rs tests/api.rs
git commit -m "refactor: remove public sse replay endpoint"
```

---

### Task 2: Make Attached subscriptions live-only and cancel immediately

**Files:**
- Modify: `src/runtime/attachment.rs:1-107`
- Modify: `src/runtime/service.rs:1-575`
- Modify: `src/main.rs:68-82`
- Modify: `tests/run_service.rs:230-600`

**Interfaces:**
- Consumes: `EventHub::subscribe(run_id) -> EventSubscription`, `StopController::request(StopReason)`
- Produces: `RunSubscription::new(run_id, live, lease)` and immediate `LeaseOwner::release_subscription`

- [ ] **Step 1: Make disconnect tests require immediate cancellation**

Replace the grace test with:

```rust
#[tokio::test]
async fn attached_run_disconnect_cancels_immediately() {
    let (service, _) = service(2).await;
    let attached = service
        .create_attached("blocking", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    let run_id = attached.run_id.clone();
    wait_for_status(&service, &run_id, RunStatus::Running).await;

    drop(attached.subscription);
    let cancelled = tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_status(&service, &run_id, RunStatus::Cancelled),
    )
    .await
    .expect("Attached cancellation must not wait for a reconnect grace period");
    assert_eq!(cancelled.status, RunStatus::Cancelled);
}
```

Rename the parallel grace test to `attached_disconnect_immediately_stops_and_drains_all_active_branches`. Remove `pause`/`advance`; after both branches start use:

```rust
drop(attached.subscription);
tokio::time::timeout(
    Duration::from_secs(1),
    wait_for_status(&service, &run_id, RunStatus::Cancelled),
)
.await
.expect("parallel Attached cancellation must drain promptly");
assert_eq!(stopped.load(Ordering::SeqCst), 2);
assert_eq!(active.load(Ordering::SeqCst), 0);
```

Retain the assertions for one terminal event and no post-join node.

- [ ] **Step 2: Verify old grace behavior fails**

Run:

```bash
cargo test --test run_service attached_run_disconnect_cancels_immediately -- --exact --nocapture
cargo test --test run_service attached_disconnect_immediately_stops_and_drains_all_active_branches -- --exact --nocapture
```

Expected: FAIL or time out because cancellation waits for the configured grace.

- [ ] **Step 3: Replace `runtime/attachment.rs` with one live receiver**

```rust
use std::sync::Arc;

use crate::events::hub::{EventError, EventSubscription};
use crate::events::protocol::RunEvent;

pub(crate) trait LeaseOwner: Send + Sync {
    fn release_subscription(self: Arc<Self>, run_id: &str);
}

pub(crate) struct SubscriptionLease {
    owner: Arc<dyn LeaseOwner>,
    run_id: String,
}

impl SubscriptionLease {
    pub(crate) fn new(owner: Arc<dyn LeaseOwner>, run_id: impl Into<String>) -> Self {
        Self { owner, run_id: run_id.into() }
    }
}

impl Drop for SubscriptionLease {
    fn drop(&mut self) {
        Arc::clone(&self.owner).release_subscription(&self.run_id);
    }
}

pub struct RunSubscription {
    pub run_id: String,
    live: EventSubscription,
    _lease: SubscriptionLease,
}

impl RunSubscription {
    pub(crate) fn new(
        run_id: impl Into<String>,
        live: EventSubscription,
        lease: SubscriptionLease,
    ) -> Self {
        Self { run_id: run_id.into(), live, _lease: lease }
    }

    pub fn last_seq(&self) -> u64 { self.live.last_seq() }

    pub async fn recv(&mut self) -> Result<RunEvent, EventError> {
        self.live.recv().await
    }
}

pub struct AttachedRun {
    pub run_id: String,
    pub request_id: String,
    pub subscription: RunSubscription,
}
```

- [ ] **Step 4: Remove reconnect state and existing-Run subscription**

Use these service types and validation:

```rust
#[derive(Debug, Clone, Copy)]
pub struct RunServiceConfig {
    pub max_concurrent_runs: usize,
    pub max_parallel_node_executions: usize,
    pub max_parallel_branches_per_run: usize,
    pub run_timeout: Duration,
}

struct ActiveRun {
    attachment: RunAttachment,
    stop: StopController,
    task: JoinHandle<()>,
    _permit: OwnedSemaphorePermit,
}
```

```rust
if config.max_concurrent_runs == 0
    || config.max_parallel_node_executions == 0
    || config.max_parallel_branches_per_run == 0
    || config.run_timeout.is_zero()
{
    return Err(ServiceError::new(
        "RUN_SERVICE_CONFIG_INVALID",
        "run service capacities and durations must be greater than zero",
    ));
}
```

Delete `RunService::subscribe`. Create Attached/Detached Runs with:

```rust
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

```rust
let record = self.get_run(&prepared.new_run.run_id).await?;
self.launch(prepared);
Ok(record)
```

Change `launch(&self, prepared: PreparedRun)` and insert:

```rust
ActiveRun { attachment, stop, task, _permit: permit }
```

`PreparedRun::state` remains and is cloned into the coordinator task; it is only removed from `ActiveRun`.

- [ ] **Step 5: Cancel immediately from the lease drop**

Remove `sleep_until`, `Instant`, subscriber/grace fields, delayed tasks, and `service_event_error`. Keep `sleep` for Run timeout. Use:

```rust
impl LeaseOwner for RunServiceInner {
    fn release_subscription(self: Arc<Self>, run_id: &str) {
        let stop = {
            let active = lock_active(&self);
            active
                .get(run_id)
                .filter(|run| run.attachment == RunAttachment::Attached)
                .map(|run| run.stop.clone())
        };
        if let Some(stop) = stop {
            stop.request(StopReason::Cancelled);
        }
    }
}
```

Remove `attached_reconnect_grace` from every `RunServiceConfig` literal. The main literal is:

```rust
RunServiceConfig {
    max_concurrent_runs: config.runtime.max_concurrent_runs,
    max_parallel_node_executions: config.runtime.max_parallel_node_executions,
    max_parallel_branches_per_run: config.runtime.max_parallel_branches_per_run,
    run_timeout: config.runtime.run_timeout,
}
```

- [ ] **Step 6: Add history-read and terminal-race coverage**

Add `event_history_reads: AtomicUsize` to `CountingRepository`, initialize it to zero, and increment its `list_events_after` implementation:

```rust
self.event_history_reads.fetch_add(1, Ordering::SeqCst);
```

Add:

```rust
#[tokio::test]
async fn attached_terminal_drop_never_reads_history_or_rewrites_completion() {
    let (service, repository) = service(2).await;
    let attached = service
        .create_attached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    let run_id = attached.run_id.clone();
    let mut subscription = attached.subscription;

    loop {
        let event = subscription.recv().await.unwrap();
        if matches!(event.event_type, RunEventType::RunCompleted) { break; }
    }
    drop(subscription);
    tokio::task::yield_now().await;

    assert_eq!(service.get_run(&run_id).await.unwrap().status, RunStatus::Completed);
    assert_eq!(repository.event_history_reads.load(Ordering::SeqCst), 0);
    let events = repository.events.lock().await;
    let terminal = events[&run_id].iter().filter(|event| matches!(
        event.event_type,
        RunEventType::RunCompleted | RunEventType::RunFailed
            | RunEventType::RunCancelled | RunEventType::RunInterrupted
    )).collect::<Vec<_>>();
    assert_eq!(terminal.len(), 1);
    assert_eq!(terminal[0].event_type, RunEventType::RunCompleted);
}
```

- [ ] **Step 7: Verify and commit the runtime change**

Run:

```bash
cargo test --test run_service -- --nocapture
cargo test --test api -- --nocapture
```

Expected: PASS; Attached cancellation is immediate, parallel work drains, Detached behavior is unchanged, and terminal drop performs no history query/rewrite.

Commit:

```bash
git add src/runtime/attachment.rs src/runtime/service.rs src/main.rs tests/run_service.rs tests/api.rs
git commit -m "refactor: make attached subscriptions live only"
```

---
### Task 3: Remove EventHub replay state and retain recovery reads

**Files:**
- Modify: `src/events/hub.rs:1-499`
- Modify: `src/main.rs:55-67`
- Modify: `src/api/formal/sse.rs:16-51`
- Modify: `tests/event_hub.rs:1-788`
- Modify: `tests/api.rs:142-160`
- Modify: `tests/run_service.rs:438-453`
- Modify: `tests/run_coordinator.rs:475-490`
- Modify: `tests/run_scheduler.rs:730-745`

**Interfaces:**
- Consumes: `RunRepository`, `EventJournal`, Tokio `broadcast`
- Produces: ring-free `EventHubConfig`, live `EventSubscription`, internal-only terminal recovery queries

- [ ] **Step 1: Make tests use a ring-free EventHub contract**

Replace the EventHub test helper:

```rust
fn config(subscriber_capacity: usize) -> EventHubConfig {
    EventHubConfig {
        subscriber_capacity,
        journal_capacity: 8,
        journal_batch_size: 4,
        operation_timeout: Duration::from_secs(1),
    }
}
```

Update calls from `config(ring, subscribers)` to `config(subscribers)` and remove `ring_capacity` from explicit literals in every listed file. Delete the ring replay test. Replace the first sequencing test with:

```rust
#[tokio::test]
async fn publish_allocates_and_persists_ordered_sequences() {
    let repository = Arc::new(MemoryRepository::default());
    let hub = EventHub::new(repository.clone(), config(8));
    assert_eq!(hub.publish(scope(None), RunEventType::RunCreated, json!({})).await.unwrap().seq, 1);
    assert_eq!(hub.publish(scope(None), RunEventType::RunStarted, json!({})).await.unwrap().seq, 2);
    hub.flush().await.unwrap();
    assert_eq!(repository.stored_sequences(RUN_ID).await, vec![1, 2]);
}
```

Rename the branch replay test to `branch_lifecycle_events_use_contiguous_durable_sequences`; retain its `node_id.is_none()` checks, flush, and assert:

```rust
assert_eq!(repository.stored_sequences(RUN_ID).await, vec![1, 2, 3]);
```

Delete terminal-test assertions using `replay_after`/`subscribe_existing`; retain terminal broadcast and `retained_run_count() == 0`.

- [ ] **Step 2: Verify the old EventHub surface fails to compile**

Run: `cargo test --test event_hub --no-run`

Expected: FAIL because `EventHubConfig` still requires `ring_capacity`.

- [ ] **Step 3: Reduce EventHub to sequence plus live broadcast**

Remove `BTreeMap`/`VecDeque` imports and use:

```rust
#[derive(Debug, Clone, Copy)]
pub struct EventHubConfig {
    pub subscriber_capacity: usize,
    pub journal_capacity: usize,
    pub journal_batch_size: usize,
    pub operation_timeout: Duration,
}

struct EventRunState {
    next_seq: u64,
    live: broadcast::Sender<RunEvent>,
}

struct EventHubInner {
    repository: Arc<dyn RunRepository>,
    journal: EventJournal,
    states: Mutex<HashMap<String, Arc<Mutex<EventRunState>>>>,
    subscriber_capacity: usize,
    operation_timeout: Duration,
}
```

Construct the hub/state with:

```rust
pub fn new(repository: Arc<dyn RunRepository>, config: EventHubConfig) -> Self {
    let journal = EventJournal::new(
        Arc::clone(&repository), config.journal_capacity,
        config.journal_batch_size, config.operation_timeout,
    );
    Self {
        inner: Arc::new(EventHubInner {
            repository,
            journal,
            states: Mutex::new(HashMap::new()),
            subscriber_capacity: config.subscriber_capacity.max(1),
            operation_timeout: config.operation_timeout,
        }),
    }
}

async fn run_state(&self, run_id: &str) -> Arc<Mutex<EventRunState>> {
    let mut states = self.inner.states.lock().await;
    Arc::clone(states.entry(run_id.to_string()).or_insert_with(|| {
        let (live, _) = broadcast::channel(self.inner.subscriber_capacity);
        Arc::new(Mutex::new(EventRunState { next_seq: 1, live }))
    }))
}
```

- [ ] **Step 4: Delete replay symbols and simplify live commit**

Delete `subscribe_existing`, `replay_after`, `replay_page_after`, `ReplayPage`, `ReplayTruncated`, and `ReplayFinished`, including error-code/display arms. Use:

```rust
Self::SubscriberLagged { last_seq } => {
    write!(formatter, "subscriber lagged after sequence {last_seq}")
}

fn commit_live_event(state: &mut EventRunState, event: RunEvent) {
    state.next_seq += 1;
    let _ = state.live.send(event);
}
```

Update every commit call. Preserve both `list_events_after` calls: the one-event authoritative terminal check in `publish_terminal` and the bounded reconciliation query in `reconcile_durable_through`.

The final main constructor is:

```rust
EventHubConfig {
    subscriber_capacity: config.runtime.subscriber_capacity,
    journal_capacity: config.runtime.journal_capacity,
    journal_batch_size: config.runtime.journal_batch_size,
    operation_timeout: config.runtime.journal_operation_timeout,
}
```

Use the same four fields in all test constructors. Ensure `src/api/formal/sse.rs` has only `Ok(event)` and generic `Err(error)` branches; no `ReplayFinished` match remains.

- [ ] **Step 5: Add a no-history-read live-publish test**

Add `list_calls: AtomicUsize` to `MemoryRepository`, increment it at the start of its `list_events_after`, then add:

```rust
#[tokio::test]
async fn ordinary_live_publish_never_reads_event_history() {
    let repository = Arc::new(MemoryRepository::default());
    let hub = EventHub::new(repository.clone(), config(8));
    let mut subscription = hub.subscribe(RUN_ID).await;
    let published = hub.publish(scope(None), RunEventType::RunCreated, json!({})).await.unwrap();
    assert_eq!(subscription.recv().await.unwrap(), published);
    assert_eq!(repository.list_calls.load(Ordering::SeqCst), 0);
}
```

- [ ] **Step 6: Verify EventHub and dependent runtime tests**

Run:

```bash
cargo test --test event_hub -- --nocapture
cargo test --test run_coordinator --test run_scheduler -- --nocapture
cargo test --test run_service --test api -- --nocapture
rg -n "ReplayPage|ReplayTruncated|ReplayFinished|replay_page_after|replay_after|subscribe_existing|ring_capacity" src/events src/runtime src/api tests/event_hub.rs tests/run_service.rs tests/run_coordinator.rs tests/run_scheduler.rs tests/api.rs
rg -n "list_events_after" src/events/hub.rs src/history tests/history_sqlite_v1.rs tests/history_postgres.rs
```

Expected: all tests PASS; the first search has no matches; the second shows only internal recovery/repository contracts.

- [ ] **Step 7: Commit the EventHub simplification**

```bash
git add src/events/hub.rs src/api/formal/sse.rs src/main.rs tests/event_hub.rs tests/api.rs tests/run_service.rs tests/run_coordinator.rs tests/run_scheduler.rs
git commit -m "refactor: remove event replay state"
```

---

### Task 4: Add strict transport-owned keepalive configuration

**Files:**
- Modify: `src/config.rs:76-98,270-398`
- Modify: `config/platform.yaml:27-40`
- Modify: `src/api/formal/routes.rs:1-155`
- Modify: `src/api/formal/sse.rs:1-65`
- Modify: `src/main.rs:85-95`
- Modify: `tests/platform_config_v1.rs:1-222`
- Modify: `tests/api.rs:1-455`

**Interfaces:**
- Consumes: Axum `KeepAlive`, `RunSubscription`
- Produces: `RuntimeConfig::sse_keep_alive_interval`, `FormalApiState::sse_keep_alive_interval`, `response_stream(subscription, keep_alive_interval)`

- [ ] **Step 1: Write failing strict-config tests**

Replace the two removed YAML fields in `base_yaml` with:

```yaml
  sse_keep_alive_interval: 5s
  subscriber_capacity: 64
```

Add `time::Duration` to the existing `std` imports, then assert:

```rust
assert_eq!(config.runtime.sse_keep_alive_interval, Duration::from_secs(5));
```

Add `("sse_keep_alive_interval: 5s", "sse_keep_alive_interval: 0s")` to zero-duration cases and add:

```rust
#[test]
fn removed_sse_recovery_settings_are_unknown() {
    for removed in ["  attached_reconnect_grace: 10s\n", "  replay_ring_capacity: 256\n"] {
        let yaml = base_yaml("  mode: disabled").replace(
            "  sse_keep_alive_interval: 5s\n",
            &format!("  sse_keep_alive_interval: 5s\n{removed}"),
        );
        let (_directory, path) = write_config(&yaml);
        assert_eq!(load(&path, BTreeMap::new()).unwrap_err().code(), "PLATFORM_CONFIG_INVALID");
    }
}

#[test]
fn invalid_sse_keep_alive_duration_is_rejected() {
    let yaml = base_yaml("  mode: disabled")
        .replace("sse_keep_alive_interval: 5s", "sse_keep_alive_interval: soon");
    let (_directory, path) = write_config(&yaml);
    assert_eq!(load(&path, BTreeMap::new()).unwrap_err().code(), "PLATFORM_RUNTIME_INVALID");
}
```

Run: `cargo test --test platform_config_v1 -- --nocapture`

Expected: FAIL because strict YAML rejects the new field.

- [ ] **Step 2: Replace runtime configuration fields**

Use the same fields in public and raw runtime types:

```rust
pub struct RuntimeConfig {
    pub max_concurrent_runs: usize,
    pub max_fork_branches: usize,
    pub max_parallel_node_executions: usize,
    pub max_parallel_branches_per_run: usize,
    pub default_node_timeout: Duration,
    pub run_timeout: Duration,
    pub sse_keep_alive_interval: Duration,
    pub subscriber_capacity: usize,
    pub journal_capacity: usize,
    pub journal_batch_size: usize,
    pub journal_operation_timeout: Duration,
}
```

```rust
struct RuntimeYaml {
    max_concurrent_runs: usize,
    max_fork_branches: usize,
    max_parallel_node_executions: usize,
    max_parallel_branches_per_run: usize,
    default_node_timeout: String,
    run_timeout: String,
    sse_keep_alive_interval: String,
    subscriber_capacity: usize,
    journal_capacity: usize,
    journal_batch_size: usize,
    journal_operation_timeout: String,
}
```

Remove replay capacity from validation and resolve:

```rust
sse_keep_alive_interval: positive_duration(
    &raw.sse_keep_alive_interval,
    "runtime.sse_keep_alive_interval",
)?,
```

Set `config/platform.yaml` to:

```yaml
  default_node_timeout: 60s
  run_timeout: 5m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 128
  journal_capacity: 1024
  journal_batch_size: 32
  journal_operation_timeout: 30s
```

- [ ] **Step 3: Write a failing configurable-keepalive API test**

Import `futures::StreamExt`, define `const TEST_SSE_KEEP_ALIVE: Duration = Duration::from_millis(10);`, and add the intended field to the test state literal. Add:

```rust
#[tokio::test]
async fn attached_stream_emits_configured_keep_alive_and_disconnects_cleanly() {
    let (app, service) = fixture(ApiAuth::disabled(), 4).await;
    let response = app.oneshot(request(
        Method::POST,
        "/v1/agents/blocking/runs/stream",
        Some(json!({"text":"hello"})),
    )).await.unwrap();
    let run_id = response.headers()["x-run-id"].to_str().unwrap().to_string();
    let mut body = response.into_body().into_data_stream();

    let encoded = tokio::time::timeout(Duration::from_millis(250), async {
        let mut encoded = String::new();
        while !encoded.contains(": keep-alive") {
            let chunk = body.next().await.unwrap().unwrap();
            encoded.push_str(&String::from_utf8_lossy(&chunk));
        }
        encoded
    }).await.expect("configured keepalive must be emitted");
    assert!(encoded.contains(": keep-alive"));
    drop(body);
    tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_status(&service, &run_id, RunStatus::Cancelled),
    ).await.expect("dropping SSE must cancel the Attached Run");
}
```

Run: `cargo test --test api attached_stream_emits_configured_keep_alive_and_disconnects_cleanly -- --exact --nocapture`

Expected: FAIL to compile because state has no keepalive field, or time out on the hard-coded 15-second interval.

- [ ] **Step 4: Thread keepalive only through HTTP state**

Define:

```rust
#[derive(Clone)]
pub struct FormalApiState {
    pub service: RunService,
    pub auth: ApiAuth,
    pub sse_keep_alive_interval: Duration,
}
```

Call:

```rust
let mut response = response_stream(
    attached.subscription,
    state.sse_keep_alive_interval,
).into_response();
```

Change SSE construction:

```rust
pub(crate) fn response_stream(
    subscription: RunSubscription,
    keep_alive_interval: Duration,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
```

```rust
Sse::new(stream).keep_alive(
    KeepAlive::new().interval(keep_alive_interval).text("keep-alive"),
)
```

Wire main:

```rust
let app = build_router(FormalApiState {
    service: service.clone(),
    auth: ApiAuth::from(&config.auth),
    sse_keep_alive_interval: config.runtime.sse_keep_alive_interval,
});
```

Do not put the interval in RunService, EventHub, or scheduler configuration.

- [ ] **Step 5: Make terminal EOF bounded in the API test**

Replace the Attached success body read with:

```rust
let body = tokio::time::timeout(
    Duration::from_secs(1),
    to_bytes(response.into_body(), usize::MAX),
).await.expect("terminal event must close Attached SSE immediately").unwrap();
```

Retain assertions that `run.created` is first, `run.completed` is last, and all `seq` values are contiguous.

- [ ] **Step 6: Verify and commit keepalive configuration**

Run:

```bash
cargo test --test platform_config_v1 -- --nocapture
cargo test --test api -- --nocapture
cargo test --test run_service -- --nocapture
```

Expected: PASS; config resolves exactly `5s`, the test override emits at 10 ms, terminal EOF is prompt, and disconnect cancels.

Commit:

```bash
git add src/config.rs config/platform.yaml src/api/formal/routes.rs src/api/formal/sse.rs src/main.rs tests/platform_config_v1.rs tests/api.rs
git commit -m "feat: configure attached sse keepalive"
```

---

### Task 5: Publish the live-only contract and run release gates

**Files:**
- Modify: `README.md:80-230`
- Modify: `docs/formal-v1-breaking-changes.md:10-96`
- Verify: `src/`, `tests/`, `config/`, SQLite/PostgreSQL repository suites

**Interfaces:**
- Consumes: completed live-only HTTP/runtime/config behavior from Tasks 1-4
- Produces: current client documentation and release-level verification evidence

- [ ] **Step 1: Update the README configuration and Attached/Detached contract**

Replace the runtime sample fields with:

```yaml
runtime:
  max_concurrent_runs: 32
  max_fork_branches: 32
  max_parallel_node_executions: 32
  max_parallel_branches_per_run: 8
  default_node_timeout: 60s
  run_timeout: 5m
  sse_keep_alive_interval: 5s
  subscriber_capacity: 128
  journal_capacity: 1024
  journal_batch_size: 32
  journal_operation_timeout: 30s
```

Replace the Attached lifecycle prose with:

```text
创建 attached Run 会原子地订阅实时事件并启动执行，响应头包含 X-Run-Id 和 X-Request-Id。终态事件写入历史后发送，发送后 SSE 立即关闭。客户端断开会立即取消仍在运行的 attached Run；该接口不支持重连补发。

SSE 每 5 秒发送 keepalive 注释用于尽快发现半开连接；注释不是协议事件，不占用 seq。网络栈、代理和调度会影响实际发现时间，因此 5 秒是检测目标而不是硬实时保证。
```

Replace query/replay/cancel examples with:

```bash
# Detached：创建后通过 Run 资源轮询，断开不会停止任务
curl --silent --request POST \
  --header 'content-type: application/json' \
  --data '{"text":"hello rust world"}' \
  http://127.0.0.1:3000/v1/agents/code_node_demo/runs

curl --silent http://127.0.0.1:3000/v1/runs/run_xxx
curl --silent --request DELETE http://127.0.0.1:3000/v1/runs/run_xxx
```

State explicitly: `GET /v1/runs/{run_id}/events` and `after_seq` are removed; `seq`/SSE `id` remain ordering/audit identifiers, not recovery cursors; `DELETE` remains idempotent.

- [ ] **Step 2: Document the deliberate breaking interface change**

In `docs/formal-v1-breaking-changes.md`, replace the EventHub migration row with:

```markdown
| 传输消费时才写历史 | 独立 `EventHub` + bounded journal + live-only Attached SSE | 持久化不能依赖 SSE 消费者；公开补发会让重连潮直接竞争数据库连接和 journal 写入 | Attached 使用 `/runs/stream`；Detached 使用 `/runs` 后轮询 Run 资源 |
```

Replace the introductory claim that HTTP routes did not change with:

```text
fork/join 引入的 DSL、调度和事件接口变化是有意的；本次 live-only SSE 基线还会删除公开事件恢复路由。删除原因不是事件不再持久化，而是公开补发会让重连潮直接竞争数据库连接与 journal 写入，并且既有 Run 的纯实时订阅无法补齐创建到订阅之间的事件缺口。
```

Make the endpoint list exactly:

```text
GET    /health
GET    /v1/agents
GET    /v1/agents/{agent_id}
POST   /v1/agents/{agent_id}/runs/stream
POST   /v1/agents/{agent_id}/runs
GET    /v1/runs/{run_id}
DELETE /v1/runs/{run_id}
```

Replace the reconnect paragraph with:

```text
attached POST 在构造 SSE 前完成 JSON 与 Schema 校验，先订阅实时广播再启动 Run，并返回 X-Run-Id；终态事件发送后连接立即结束，非终态连接断开会立即取消 Run。正式 V1 不提供 GET /runs/{run_id}/events、after_seq 或 Last-Event-ID 恢复：公开恢复会让并发重连直接查询事件库，而 live-only 的既有 Run 订阅又无法保证创建到订阅之间无缺口。detached POST 返回 202，客户端通过 GET Run 轮询最终状态；DELETE 是唯一显式取消接口且幂等。

事件的 schema_version 仍为 1，seq 和 SSE id 仍表示单 Run 顺序并用于审计关联，但不再表示可恢复游标。事件继续先持久化再广播；数据库事件历史保留给内部终态恢复和审计，不删除表、不执行 migration。
```

- [ ] **Step 3: Run documentation and dead-contract scans**

Run:

```bash
rg -n "attached_reconnect_grace|replay_ring_capacity|ReplayPage|ReplayTruncated|ReplayFinished|reconnect with after_seq|/events\?after_seq" src config README.md docs/formal-v1-breaking-changes.md
rg -n "attached_reconnect_grace|replay_ring_capacity|/events\?after_seq|last-event-id" tests/platform_config_v1.rs tests/api.rs
rg -n "sse_keep_alive_interval|live-only|不支持重连补发|轮询" config/platform.yaml README.md docs/formal-v1-breaking-changes.md
git diff --check
```

Expected: the first command returns no matches; the second command finds only explicit unknown-field and `404` regression tests; the third finds the 5-second setting and live-only client guidance; `git diff --check` exits 0.

- [ ] **Step 4: Commit client and migration documentation**

```bash
git add README.md docs/formal-v1-breaking-changes.md
git commit -m "docs: publish live-only sse contract"
```

- [ ] **Step 5: Run focused feature verification**

Run:

```bash
cargo test --test platform_config_v1 --test api --test run_service --test event_hub -- --nocapture
cargo test --test run_coordinator --test run_scheduler -- --nocapture
cargo test --test history_sqlite_v1 --test formal_protocol -- --nocapture
```

Expected: every focused test passes, including immediate cancellation, terminal EOF, no SSE history read, internal recovery, branch drain, persistence, and config strictness.

- [ ] **Step 6: Run the complete local quality suite**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo audit
cargo deny check
```

Expected: every command exits 0 with no warnings or failing tests.

- [ ] **Step 7: Verify real PostgreSQL parity**

Run:

```bash
docker compose -f docker-compose.postgres.yml up -d
RUN_HISTORY_POSTGRES_URL='postgres://insight:insight@127.0.0.1:5433/insight_agent_platform' cargo test --test history_postgres -- --nocapture
```

Expected: PASS; PostgreSQL event sequencing, terminal transaction conflict/recovery, branch events, and startup reconciliation remain intact without a schema migration.

- [ ] **Step 8: Inspect final scope and interface deletion**

Run:

```bash
git diff --check "$BASE_SHA"..HEAD
git status --short
rg -n "attached_reconnect_grace|replay_ring_capacity|ReplayPage|ReplayTruncated|ReplayFinished|replay_page_after|subscribe_existing" src config README.md docs/formal-v1-breaking-changes.md
rg -n "attached_reconnect_grace|replay_ring_capacity|/events\?after_seq|last-event-id" tests/platform_config_v1.rs tests/api.rs
rg -n "list_events_after" src/events/hub.rs src/history tests/history_sqlite_v1.rs tests/history_postgres.rs
```

Expected: no whitespace errors or uncommitted feature files; removed replay/grace symbols have no production/documentation matches; test matches are limited to explicit rejection/`404` contracts; history reads remain only in internal recovery/repository code and repository tests.

- [ ] **Step 9: Request final review before integration**

Use `superpowers:requesting-code-review` against the complete implementation commit range. Resolve every correctness/specification issue, rerun Steps 5-7, then use `superpowers:finishing-a-development-branch` to select merge, PR, or cleanup.
