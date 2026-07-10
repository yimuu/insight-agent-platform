use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
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
    RunEventScope {
        request_id: "req_events".to_string(),
        run_id: RUN_ID.to_string(),
        agent_id: "agent_events".to_string(),
        agent_version: "sha256:events".to_string(),
        node_id: node_id.map(str::to_string),
    }
}

fn config(ring_capacity: usize, subscriber_capacity: usize) -> EventHubConfig {
    EventHubConfig {
        ring_capacity,
        subscriber_capacity,
        journal_capacity: 8,
        journal_batch_size: 4,
    }
}

#[derive(Default)]
struct MemoryRepository {
    events: Mutex<BTreeMap<String, Vec<RunEvent>>>,
    outputs: Mutex<Vec<NodeOutputRecord>>,
    terminal_updates: Mutex<Vec<TerminalUpdate>>,
    fail_appends: AtomicBool,
    terminal_called: Notify,
    allow_terminal: Notify,
    block_terminal: AtomicBool,
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
        if !terminal_updates.is_empty() {
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

    async fn get_run(&self, _run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
        Ok(None)
    }

    async fn list_events_after(
        &self,
        run_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, HistoryError> {
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
async fn publish_allocates_sequence_and_replay_returns_ordered_events() {
    let repository = Arc::new(MemoryRepository::default());
    let hub = EventHub::new(repository, config(8, 8));

    assert_eq!(
        hub.publish(scope(Some("plan")), RunEventType::NodeStarted, json!({}))
            .await
            .unwrap()
            .seq,
        1
    );
    assert_eq!(
        hub.publish(
            scope(Some("plan")),
            RunEventType::ContentDelta,
            json!({"content":"a"}),
        )
        .await
        .unwrap()
        .seq,
        2
    );

    assert_eq!(
        hub.replay_after(RUN_ID, 0)
            .await
            .unwrap()
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[tokio::test]
async fn two_subscribers_receive_identical_ordered_events() {
    let hub = EventHub::new(Arc::new(MemoryRepository::default()), config(8, 8));
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
async fn lagging_subscriber_is_closed_with_its_last_delivered_sequence() {
    let hub = EventHub::new(Arc::new(MemoryRepository::default()), config(8, 2));
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
async fn replay_reads_durable_events_that_fell_out_of_the_active_ring() {
    let repository = Arc::new(MemoryRepository::default());
    let hub = EventHub::new(repository.clone(), config(2, 8));
    for _ in 0..4 {
        hub.publish(scope(None), RunEventType::RunStarted, json!({}))
            .await
            .unwrap();
    }
    hub.flush().await.unwrap();
    assert_eq!(repository.stored_sequences(RUN_ID).await, vec![1, 2, 3, 4]);

    assert_eq!(
        hub.replay_after(RUN_ID, 0)
            .await
            .unwrap()
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
}

#[tokio::test]
async fn journal_worker_failure_closes_the_queue_instead_of_dropping_history() {
    let repository = Arc::new(MemoryRepository::default());
    repository.fail_appends.store(true, Ordering::SeqCst);
    let hub = EventHub::new(repository, config(8, 8));

    hub.publish(scope(None), RunEventType::RunCreated, json!({}))
        .await
        .unwrap();
    let error = hub.flush().await.unwrap_err();

    assert_eq!(error.code(), "JOURNAL_CLOSED");
}

#[tokio::test]
async fn terminal_event_is_broadcast_only_after_the_repository_commit() {
    let repository = Arc::new(MemoryRepository::default());
    repository.block_terminal.store(true, Ordering::SeqCst);
    let hub = EventHub::new(repository.clone(), config(8, 8));
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
}
