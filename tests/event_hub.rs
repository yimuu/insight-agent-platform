use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use insight_agent_platform::{
    dsl::compiled::RunOutput,
    events::{
        hub::{EventError, EventHub, EventHubConfig},
        protocol::{RunEvent, RunEventScope, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{NewRun, NodeOutputRecord, RunRecord, RunStatus, TerminalUpdate},
    },
};
use serde_json::json;
use tokio::sync::{Mutex, Notify};

const RUN_ID: &str = "run_events";

fn at(second: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, second).unwrap()
}

fn scope(node_id: Option<&str>) -> RunEventScope {
    scope_for(RUN_ID, node_id)
}

fn scope_for(run_id: &str, node_id: Option<&str>) -> RunEventScope {
    RunEventScope {
        request_id: "req_events".to_string(),
        run_id: run_id.to_string(),
        agent_id: "agent_events".to_string(),
        agent_version: "sha256:events".to_string(),
        node_id: node_id.map(str::to_string),
    }
}

fn config(subscriber_capacity: usize) -> EventHubConfig {
    EventHubConfig {
        subscriber_capacity,
        journal_capacity: 8,
        journal_batch_size: 4,
        operation_timeout: Duration::from_secs(1),
    }
}

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

impl MemoryRepository {
    async fn stored_sequences(&self, run_id: &str) -> Vec<u64> {
        self.events
            .lock()
            .await
            .get(run_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|event| event.seq)
            .collect()
    }

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
}

#[async_trait]
impl RunRepository for MemoryRepository {
    async fn create_run(&self, _run: NewRun) -> Result<(), HistoryError> {
        Ok(())
    }

    async fn mark_running(
        &self,
        _run_id: &str,
        _started_at: chrono::DateTime<Utc>,
    ) -> Result<(), HistoryError> {
        Ok(())
    }

    async fn append_events(&self, events: &[RunEvent]) -> Result<(), HistoryError> {
        self.append_called.notify_one();
        if self.commit_then_block_appends.load(Ordering::SeqCst) {
            let mut stored = self.events.lock().await;
            for event in events {
                stored
                    .entry(event.run_id.clone())
                    .or_default()
                    .push(event.clone());
            }
            drop(stored);
            self.allow_append.notified().await;
            return Ok(());
        }
        if self.block_appends.load(Ordering::SeqCst) {
            self.allow_append.notified().await;
        }
        if self.fail_appends.load(Ordering::SeqCst) {
            return Err(HistoryError::new(
                "SYNTHETIC_WRITE_FAILURE",
                "synthetic append failure",
            ));
        }
        let mut stored = self.events.lock().await;
        for event in events {
            stored
                .entry(event.run_id.clone())
                .or_default()
                .push(event.clone());
        }
        Ok(())
    }

    async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), HistoryError> {
        self.outputs.lock().await.push(output);
        Ok(())
    }

    async fn finish_run(
        &self,
        update: TerminalUpdate,
        event: RunEvent,
    ) -> Result<bool, HistoryError> {
        self.terminal_called.notify_one();
        if self.block_terminal.load(Ordering::SeqCst) {
            self.allow_terminal.notified().await;
        }
        let mut terminal_updates = self.terminal_updates.lock().await;
        if terminal_updates
            .iter()
            .any(|existing| existing.run_id == update.run_id)
        {
            return Ok(false);
        }
        terminal_updates.push(update);
        self.events
            .lock()
            .await
            .entry(event.run_id.clone())
            .or_default()
            .push(event);
        Ok(true)
    }

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

    async fn get_run(&self, _run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
        Ok(None)
    }

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

    async fn mark_incomplete_interrupted(
        &self,
        _at: chrono::DateTime<Utc>,
    ) -> Result<u64, HistoryError> {
        Ok(0)
    }
}

#[tokio::test]
async fn publish_allocates_and_persists_ordered_sequences() {
    let repository = Arc::new(MemoryRepository::default());
    let hub = EventHub::new(repository.clone(), config(8));
    assert_eq!(
        hub.publish(scope(None), RunEventType::RunCreated, json!({}))
            .await
            .unwrap()
            .seq,
        1
    );
    assert_eq!(
        hub.publish(scope(None), RunEventType::RunStarted, json!({}))
            .await
            .unwrap()
            .seq,
        2
    );
    hub.flush().await.unwrap();
    assert_eq!(repository.stored_sequences(RUN_ID).await, vec![1, 2]);
}

#[tokio::test]
async fn branch_lifecycle_events_use_contiguous_durable_sequences() {
    let repository = Arc::new(MemoryRepository::default());
    let hub = EventHub::new(repository.clone(), config(8));

    let mut published = Vec::new();
    for event_type in [
        RunEventType::BranchStarted,
        RunEventType::BranchCompleted,
        RunEventType::BranchFailed,
    ] {
        published.push(
            hub.publish(scope(Some("must_be_ignored")), event_type, json!({}))
                .await
                .unwrap(),
        );
    }

    assert_eq!(
        published.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(published.iter().all(|event| event.node_id.is_none()));
    hub.flush().await.unwrap();
    assert_eq!(repository.stored_sequences(RUN_ID).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn two_subscribers_receive_identical_ordered_events() {
    let hub = EventHub::new(Arc::new(MemoryRepository::default()), config(8));
    let mut first = hub.subscribe(RUN_ID).await;
    let mut second = hub.subscribe(RUN_ID).await;

    hub.publish(scope(None), RunEventType::RunCreated, json!({}))
        .await
        .unwrap();
    hub.publish(scope(None), RunEventType::RunStarted, json!({}))
        .await
        .unwrap();

    let first_events = vec![first.recv().await.unwrap(), first.recv().await.unwrap()];
    let second_events = vec![second.recv().await.unwrap(), second.recv().await.unwrap()];
    assert_eq!(first_events, second_events);
    assert_eq!(
        first_events
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[tokio::test]
async fn nonterminal_event_is_broadcast_only_after_the_repository_commit() {
    let repository = Arc::new(MemoryRepository::default());
    repository.block_appends.store(true, Ordering::SeqCst);
    let hub = EventHub::new(repository.clone(), config(8));
    let mut subscriber = hub.subscribe(RUN_ID).await;
    let publishing = {
        let hub = hub.clone();
        tokio::spawn(async move {
            hub.publish(scope(None), RunEventType::RunCreated, json!({}))
                .await
        })
    };
    repository.append_called.notified().await;

    assert!(
        tokio::time::timeout(Duration::from_millis(10), subscriber.recv())
            .await
            .is_err()
    );
    repository.block_appends.store(false, Ordering::SeqCst);
    repository.allow_append.notify_one();

    let published = publishing.await.unwrap().unwrap();
    assert_eq!(subscriber.recv().await.unwrap(), published);
    assert_eq!(repository.stored_sequences(RUN_ID).await, vec![1]);
}

#[tokio::test]
async fn lagging_subscriber_is_closed_with_its_last_delivered_sequence() {
    let hub = EventHub::new(Arc::new(MemoryRepository::default()), config(2));
    let mut subscriber = hub.subscribe(RUN_ID).await;

    for _ in 0..3 {
        hub.publish(scope(None), RunEventType::RunStarted, json!({}))
            .await
            .unwrap();
    }

    assert!(matches!(
        subscriber.recv().await.unwrap_err(),
        EventError::SubscriberLagged { last_seq: 0 }
    ));
}

#[tokio::test]
async fn full_journal_queue_fails_immediately_instead_of_blocking_the_run() {
    let repository = Arc::new(MemoryRepository::default());
    repository.block_appends.store(true, Ordering::SeqCst);
    let hub = EventHub::new(
        repository.clone(),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 1,
            journal_batch_size: 1,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let first = {
        let hub = hub.clone();
        tokio::spawn(async move {
            hub.publish(
                scope_for("run_queue_1", None),
                RunEventType::RunCreated,
                json!({}),
            )
            .await
        })
    };
    repository.append_called.notified().await;
    let second = {
        let hub = hub.clone();
        tokio::spawn(async move {
            hub.publish(
                scope_for("run_queue_2", None),
                RunEventType::RunStarted,
                json!({}),
            )
            .await
        })
    };
    tokio::task::yield_now().await;

    let error = hub
        .publish(
            scope_for("run_queue_3", Some("work")),
            RunEventType::NodeStarted,
            json!({}),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "JOURNAL_CAPACITY_EXCEEDED");
    repository.block_appends.store(false, Ordering::SeqCst);
    repository.allow_append.notify_one();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
}

#[tokio::test]
async fn saturated_queue_failure_drops_no_broadcast_and_each_run_can_recover() {
    let repository = Arc::new(MemoryRepository::default());
    repository.block_appends.store(true, Ordering::SeqCst);
    let hub = EventHub::new(
        repository.clone(),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 1,
            journal_batch_size: 1,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let mut subscribers = Vec::new();
    for run_id in ["run_failure_1", "run_failure_2", "run_failure_3"] {
        subscribers.push(hub.subscribe(run_id).await);
    }
    let first = {
        let hub = hub.clone();
        tokio::spawn(async move {
            hub.publish(
                scope_for("run_failure_1", None),
                RunEventType::RunCreated,
                json!({}),
            )
            .await
        })
    };
    repository.append_called.notified().await;
    let second = {
        let hub = hub.clone();
        tokio::spawn(async move {
            hub.publish(
                scope_for("run_failure_2", None),
                RunEventType::RunCreated,
                json!({}),
            )
            .await
        })
    };
    tokio::task::yield_now().await;
    assert_eq!(
        hub.publish(
            scope_for("run_failure_3", None),
            RunEventType::RunCreated,
            json!({}),
        )
        .await
        .unwrap_err()
        .code(),
        "JOURNAL_CAPACITY_EXCEEDED"
    );
    repository.fail_appends.store(true, Ordering::SeqCst);
    repository.block_appends.store(false, Ordering::SeqCst);
    repository.allow_append.notify_one();
    assert_eq!(
        first.await.unwrap().unwrap_err().code(),
        "SYNTHETIC_WRITE_FAILURE"
    );
    assert_eq!(second.await.unwrap().unwrap_err().code(), "JOURNAL_CLOSED");

    for subscriber in &mut subscribers {
        assert!(
            tokio::time::timeout(Duration::from_millis(10), subscriber.recv())
                .await
                .is_err()
        );
    }
    for (index, run_id) in ["run_failure_1", "run_failure_2", "run_failure_3"]
        .into_iter()
        .enumerate()
    {
        let update = TerminalUpdate::new(
            run_id,
            RunStatus::Failed,
            at(20 + index as u32),
            None,
            Some("INFRASTRUCTURE_FAILURE".to_string()),
            Some("runtime infrastructure failed".to_string()),
        )
        .unwrap();
        hub.recover_terminal(
            scope_for(run_id, None),
            RunEventType::RunFailed,
            update,
            "INFRASTRUCTURE_FAILURE",
            "runtime infrastructure failed",
            json!({}),
        )
        .await
        .unwrap();
        assert_eq!(
            subscribers[index].recv().await.unwrap().event_type,
            RunEventType::RunFailed
        );
        assert_eq!(repository.stored_sequences(run_id).await, vec![1]);
    }
    assert_eq!(hub.retained_run_count().await, 0);
}

#[tokio::test]
async fn journal_operation_timeout_bounds_terminal_persistence_waits() {
    let repository = Arc::new(MemoryRepository::default());
    repository.block_terminal.store(true, Ordering::SeqCst);
    let hub = EventHub::new(
        repository.clone(),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 8,
            journal_batch_size: 4,
            operation_timeout: Duration::from_millis(20),
        },
    );
    let update = TerminalUpdate::new(
        RUN_ID,
        RunStatus::Failed,
        at(11),
        None,
        Some("INFRASTRUCTURE_FAILURE".to_string()),
        Some("runtime infrastructure failed".to_string()),
    )
    .unwrap();

    let error = hub
        .publish_terminal(
            scope(None),
            RunEventType::RunFailed,
            update,
            "INFRASTRUCTURE_FAILURE",
            "runtime infrastructure failed",
            json!({}),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "JOURNAL_OPERATION_TIMEOUT");
}

#[tokio::test]
async fn recovery_derives_terminal_after_an_uncertain_append_commit() {
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
    assert_eq!(repository.stored_sequences(RUN_ID).await, vec![1]);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), subscriber.recv())
            .await
            .is_err()
    );

    let failed = TerminalUpdate::new(
        RUN_ID,
        RunStatus::Failed,
        at(14),
        None,
        Some("INFRASTRUCTURE_FAILURE".to_string()),
        Some("runtime infrastructure failed".to_string()),
    )
    .unwrap();
    let terminal = hub
        .recover_terminal(
            scope(None),
            RunEventType::RunFailed,
            failed,
            "INFRASTRUCTURE_FAILURE",
            "runtime infrastructure failed",
            json!({}),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.seq, 2);
    assert_eq!(
        subscriber.recv().await.unwrap().event_type,
        RunEventType::RunCreated
    );
    assert_eq!(
        subscriber.recv().await.unwrap().event_type,
        RunEventType::RunFailed
    );
    assert_eq!(repository.stored_sequences(RUN_ID).await, vec![1, 2]);
    assert_eq!(hub.retained_run_count().await, 0);
}

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
    let replayed_created = RunEvent::error(
        RunEventType::RunCreated,
        1,
        scope(None),
        "OK",
        "ok",
        json!({}),
    );
    let wrong_terminal = RunEvent::error(
        RunEventType::RunCancelled,
        2,
        scope(None),
        "RUN_CANCELLED",
        "run cancelled by explicit request",
        json!({}),
    );
    repository
        .set_override_list(vec![replayed_created, wrong_terminal])
        .await;

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

#[tokio::test]
async fn timed_out_terminal_write_is_cancelled_before_recovery() {
    let repository = Arc::new(MemoryRepository::default());
    repository.block_terminal.store(true, Ordering::SeqCst);
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
    let completed = TerminalUpdate::new(
        RUN_ID,
        RunStatus::Completed,
        at(12),
        Some(RunOutput {
            content: Some("done".to_string()),
            format: Some("text".to_string()),
            data: json!({}),
        }),
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        hub.publish_terminal(
            scope(None),
            RunEventType::RunCompleted,
            completed,
            "OK",
            "ok",
            json!({}),
        )
        .await
        .unwrap_err()
        .code(),
        "JOURNAL_OPERATION_TIMEOUT"
    );
    repository.block_terminal.store(false, Ordering::SeqCst);

    let failed = TerminalUpdate::new(
        RUN_ID,
        RunStatus::Failed,
        at(13),
        None,
        Some("INFRASTRUCTURE_FAILURE".to_string()),
        Some("runtime infrastructure failed".to_string()),
    )
    .unwrap();
    assert!(hub
        .recover_terminal(
            scope(None),
            RunEventType::RunFailed,
            failed,
            "INFRASTRUCTURE_FAILURE",
            "runtime infrastructure failed",
            json!({}),
        )
        .await
        .unwrap()
        .is_some());

    assert_eq!(
        subscriber.recv().await.unwrap().event_type,
        RunEventType::RunFailed
    );
    assert_eq!(
        repository.terminal_updates.lock().await[0].status,
        RunStatus::Failed
    );
    assert_eq!(hub.retained_run_count().await, 0);
}

#[tokio::test]
async fn ordinary_live_publish_never_reads_event_history() {
    let repository = Arc::new(MemoryRepository::default());
    let hub = EventHub::new(repository.clone(), config(8));
    let mut subscription = hub.subscribe(RUN_ID).await;
    let published = hub
        .publish(scope(None), RunEventType::RunCreated, json!({}))
        .await
        .unwrap();
    assert_eq!(subscription.recv().await.unwrap(), published);
    assert_eq!(repository.list_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn journal_worker_failure_closes_the_queue_instead_of_dropping_history() {
    let repository = Arc::new(MemoryRepository::default());
    repository.fail_appends.store(true, Ordering::SeqCst);
    let hub = EventHub::new(repository, config(8));

    let error = hub
        .publish(scope(None), RunEventType::RunCreated, json!({}))
        .await
        .unwrap_err();
    assert_eq!(error.code(), "SYNTHETIC_WRITE_FAILURE");
    assert!(!hub.is_healthy());

    assert_eq!(hub.flush().await.unwrap_err().code(), "JOURNAL_CLOSED");
}

#[tokio::test]
async fn terminal_event_is_broadcast_only_after_the_repository_commit() {
    let repository = Arc::new(MemoryRepository::default());
    repository.block_terminal.store(true, Ordering::SeqCst);
    let hub = EventHub::new(repository.clone(), config(8));
    let mut subscriber = hub.subscribe(RUN_ID).await;
    let update = TerminalUpdate::new(
        RUN_ID,
        RunStatus::Completed,
        at(10),
        Some(RunOutput {
            content: Some("done".to_string()),
            format: Some("text".to_string()),
            data: json!({}),
        }),
        None,
        None,
    )
    .unwrap();
    let publishing = {
        let hub = hub.clone();
        tokio::spawn(async move {
            hub.publish_terminal(
                scope(None),
                RunEventType::RunCompleted,
                update,
                "OK",
                "ok",
                json!({"content":"done", "format":"text", "data":{}}),
            )
            .await
        })
    };

    repository.terminal_called.notified().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), subscriber.recv())
            .await
            .is_err()
    );
    repository.allow_terminal.notify_one();

    let published = publishing.await.unwrap().unwrap().unwrap();
    let received = subscriber.recv().await.unwrap();
    assert_eq!(received, published);
    assert_eq!(received.event_type, RunEventType::RunCompleted);
    assert_eq!(repository.terminal_updates.lock().await.len(), 1);
    assert_eq!(hub.retained_run_count().await, 0);
}
