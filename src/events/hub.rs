use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    error::Error,
    fmt,
    sync::Arc,
};

use serde_json::Value;
use tokio::sync::{broadcast, Mutex};

use crate::history::{
    repository::{HistoryError, RunRepository},
    types::{NodeOutputRecord, TerminalUpdate},
};

use super::{
    journal::EventJournal,
    protocol::{RunEvent, RunEventScope, RunEventType},
};

#[derive(Debug, Clone, Copy)]
pub struct EventHubConfig {
    pub ring_capacity: usize,
    pub subscriber_capacity: usize,
    pub journal_capacity: usize,
    pub journal_batch_size: usize,
}

#[derive(Debug)]
pub enum EventError {
    SubscriberLagged { last_seq: u64 },
    SubscriptionClosed,
    JournalClosed,
    SequenceExhausted,
    History(HistoryError),
}

impl EventError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::SubscriberLagged { .. } => "SUBSCRIBER_LAGGED",
            Self::SubscriptionClosed => "SUBSCRIPTION_CLOSED",
            Self::JournalClosed => "JOURNAL_CLOSED",
            Self::SequenceExhausted => "EVENT_SEQUENCE_EXHAUSTED",
            Self::History(error) => error.code(),
        }
    }
}

impl fmt::Display for EventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SubscriberLagged { last_seq } => write!(
                formatter,
                "subscriber lagged; reconnect after sequence {last_seq}"
            ),
            Self::SubscriptionClosed => formatter.write_str("event subscription closed"),
            Self::JournalClosed => formatter.write_str("event journal closed"),
            Self::SequenceExhausted => formatter.write_str("event sequence exhausted"),
            Self::History(error) => error.fmt(formatter),
        }
    }
}

impl Error for EventError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::History(error) => Some(error),
            _ => None,
        }
    }
}

impl From<HistoryError> for EventError {
    fn from(error: HistoryError) -> Self {
        Self::History(error)
    }
}

struct EventRunState {
    next_seq: u64,
    ring: VecDeque<RunEvent>,
    live: broadcast::Sender<RunEvent>,
}

struct EventHubInner {
    repository: Arc<dyn RunRepository>,
    journal: EventJournal,
    states: Mutex<HashMap<String, Arc<Mutex<EventRunState>>>>,
    ring_capacity: usize,
    subscriber_capacity: usize,
}

#[derive(Clone)]
pub struct EventHub {
    inner: Arc<EventHubInner>,
}

impl EventHub {
    pub fn new(repository: Arc<dyn RunRepository>, config: EventHubConfig) -> Self {
        let journal = EventJournal::new(
            Arc::clone(&repository),
            config.journal_capacity,
            config.journal_batch_size,
        );
        Self {
            inner: Arc::new(EventHubInner {
                repository,
                journal,
                states: Mutex::new(HashMap::new()),
                ring_capacity: config.ring_capacity.max(1),
                subscriber_capacity: config.subscriber_capacity.max(1),
            }),
        }
    }

    pub async fn publish(
        &self,
        scope: RunEventScope,
        event_type: RunEventType,
        data: Value,
    ) -> Result<RunEvent, EventError> {
        self.publish_with_code(scope, event_type, "OK", "ok", data)
            .await
    }

    pub async fn publish_error(
        &self,
        scope: RunEventScope,
        event_type: RunEventType,
        code: impl Into<String>,
        message: impl Into<String>,
        data: Value,
    ) -> Result<RunEvent, EventError> {
        self.publish_with_code(scope, event_type, code, message, data)
            .await
    }

    async fn publish_with_code(
        &self,
        scope: RunEventScope,
        event_type: RunEventType,
        code: impl Into<String>,
        message: impl Into<String>,
        data: Value,
    ) -> Result<RunEvent, EventError> {
        let state = self.run_state(&scope.run_id).await;
        let mut state = state.lock().await;
        ensure_sequence_available(&state)?;
        let event = RunEvent::error(event_type, state.next_seq, scope, code, message, data);
        self.inner.journal.append(event.clone()).await?;
        commit_live_event(&mut state, event.clone(), self.inner.ring_capacity);
        Ok(event)
    }

    pub async fn publish_terminal(
        &self,
        scope: RunEventScope,
        event_type: RunEventType,
        update: TerminalUpdate,
        code: impl Into<String>,
        message: impl Into<String>,
        data: Value,
    ) -> Result<Option<RunEvent>, EventError> {
        let state = self.run_state(&scope.run_id).await;
        let mut state = state.lock().await;
        ensure_sequence_available(&state)?;
        let event = RunEvent::error(event_type, state.next_seq, scope, code, message, data);
        if !self.inner.journal.finish(update, event.clone()).await? {
            return Ok(None);
        }
        commit_live_event(&mut state, event.clone(), self.inner.ring_capacity);
        Ok(Some(event))
    }

    pub async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), EventError> {
        self.inner.journal.put_output(output).await
    }

    pub async fn flush(&self) -> Result<(), EventError> {
        self.inner.journal.flush().await
    }

    pub async fn subscribe(&self, run_id: &str) -> EventSubscription {
        let state = self.run_state(run_id).await;
        let receiver = state.lock().await.live.subscribe();
        EventSubscription {
            receiver,
            last_seq: 0,
            closed: false,
        }
    }

    pub async fn replay_after(
        &self,
        run_id: &str,
        after_seq: u64,
    ) -> Result<Vec<RunEvent>, EventError> {
        self.inner.journal.flush().await?;
        let durable = self
            .inner
            .repository
            .list_events_after(run_id, after_seq, usize::MAX)
            .await?;
        let active_state = self.inner.states.lock().await.get(run_id).cloned();
        let in_memory = match active_state {
            Some(state) => state
                .lock()
                .await
                .ring
                .iter()
                .filter(|event| event.seq > after_seq)
                .cloned()
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };
        let mut merged = BTreeMap::new();
        for event in durable.into_iter().chain(in_memory) {
            merged.insert(event.seq, event);
        }
        Ok(merged.into_values().collect())
    }

    async fn run_state(&self, run_id: &str) -> Arc<Mutex<EventRunState>> {
        let mut states = self.inner.states.lock().await;
        Arc::clone(states.entry(run_id.to_string()).or_insert_with(|| {
            let (live, _) = broadcast::channel(self.inner.subscriber_capacity);
            Arc::new(Mutex::new(EventRunState {
                next_seq: 1,
                ring: VecDeque::with_capacity(self.inner.ring_capacity),
                live,
            }))
        }))
    }
}

fn commit_live_event(state: &mut EventRunState, event: RunEvent, ring_capacity: usize) {
    state.next_seq += 1;
    if state.ring.len() == ring_capacity {
        state.ring.pop_front();
    }
    state.ring.push_back(event.clone());
    let _ = state.live.send(event);
}

fn ensure_sequence_available(state: &EventRunState) -> Result<(), EventError> {
    if state.next_seq == u64::MAX {
        Err(EventError::SequenceExhausted)
    } else {
        Ok(())
    }
}

pub struct EventSubscription {
    receiver: broadcast::Receiver<RunEvent>,
    last_seq: u64,
    closed: bool,
}

impl EventSubscription {
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub async fn recv(&mut self) -> Result<RunEvent, EventError> {
        if self.closed {
            return Err(EventError::SubscriptionClosed);
        }
        match self.receiver.recv().await {
            Ok(event) => {
                self.last_seq = event.seq;
                Ok(event)
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                self.closed = true;
                Err(EventError::SubscriberLagged {
                    last_seq: self.last_seq,
                })
            }
            Err(broadcast::error::RecvError::Closed) => {
                self.closed = true;
                Err(EventError::SubscriptionClosed)
            }
        }
    }
}
