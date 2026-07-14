use std::{collections::HashMap, error::Error, fmt, sync::Arc, time::Duration};

use serde_json::Value;
use tokio::{
    sync::{broadcast, watch, Mutex},
    time::timeout,
};

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
    pub subscriber_capacity: usize,
    pub journal_capacity: usize,
    pub journal_batch_size: usize,
    pub operation_timeout: Duration,
}

#[derive(Debug)]
pub enum EventError {
    SubscriberLagged { last_seq: u64 },
    SubscriptionClosed,
    JournalClosed,
    JournalCapacityExceeded,
    JournalOperationTimeout,
    SequenceExhausted,
    History(HistoryError),
}

impl EventError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::SubscriberLagged { .. } => "SUBSCRIBER_LAGGED",
            Self::SubscriptionClosed => "SUBSCRIPTION_CLOSED",
            Self::JournalClosed => "JOURNAL_CLOSED",
            Self::JournalCapacityExceeded => "JOURNAL_CAPACITY_EXCEEDED",
            Self::JournalOperationTimeout => "JOURNAL_OPERATION_TIMEOUT",
            Self::SequenceExhausted => "EVENT_SEQUENCE_EXHAUSTED",
            Self::History(error) => error.code(),
        }
    }
}

impl fmt::Display for EventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SubscriberLagged { last_seq } => {
                write!(formatter, "subscriber lagged after sequence {last_seq}")
            }
            Self::SubscriptionClosed => formatter.write_str("event subscription closed"),
            Self::JournalClosed => formatter.write_str("event journal closed"),
            Self::JournalCapacityExceeded => formatter.write_str("event journal queue is full"),
            Self::JournalOperationTimeout => {
                formatter.write_str("event journal operation timed out")
            }
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
    live: broadcast::Sender<RunEvent>,
}

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

struct EventHubInner {
    repository: Arc<dyn RunRepository>,
    journal: EventJournal,
    states: Mutex<HashMap<String, Arc<Mutex<EventRunState>>>>,
    recoveries: Mutex<HashMap<String, ()>>,
    recovery_changed: watch::Sender<u64>,
    subscriber_capacity: usize,
    operation_timeout: Duration,
}

impl EventHubInner {
    fn notify_recovery_changed(&self) {
        self.recovery_changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
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
            config.operation_timeout,
        );
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
        commit_live_event(&mut state, event.clone());
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
        validate_terminal_request(&scope, event_type, &update)?;
        let run_id = scope.run_id.clone();
        let state_handle = self.run_state(&run_id).await;
        let mut state = state_handle.lock().await;
        ensure_sequence_available(&state)?;
        let event = RunEvent::error(event_type, state.next_seq, scope, code, message, data);
        if !self.inner.journal.finish(update, event.clone()).await? {
            let expected_seq = state.next_seq;
            let existing = timeout(
                self.inner.operation_timeout,
                self.inner
                    .repository
                    .list_events_after(&run_id, expected_seq.saturating_sub(1), 1),
            )
            .await
            .map_err(|_| EventError::JournalOperationTimeout)??;
            if let Some(existing) = existing
                .into_iter()
                .next()
                .filter(|existing| existing.seq == expected_seq && is_terminal(existing.event_type))
            {
                commit_live_event(&mut state, existing);
                drop(state);
                self.isolate_run_state(&run_id, &state_handle).await;
                return Ok(None);
            }
            return Err(EventError::History(HistoryError::new(
                "HISTORY_TERMINAL_EVENT_MISSING",
                "run is terminal but its authoritative terminal event is missing",
            )));
        }
        commit_live_event(&mut state, event.clone());
        drop(state);
        self.isolate_run_state(&run_id, &state_handle).await;
        Ok(Some(event))
    }

    pub async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), EventError> {
        self.inner.journal.put_output(output).await
    }

    pub async fn flush(&self) -> Result<(), EventError> {
        self.inner.journal.flush().await
    }

    pub async fn recover_terminal(
        &self,
        scope: RunEventScope,
        event_type: RunEventType,
        update: TerminalUpdate,
        code: impl Into<String>,
        message: impl Into<String>,
        data: Value,
    ) -> Result<Option<RunEvent>, EventError> {
        validate_terminal_request(&scope, event_type, &update)?;
        let code = code.into();
        let message = message.into();
        match self
            .publish_terminal(
                scope.clone(),
                event_type,
                update.clone(),
                code.clone(),
                message.clone(),
                data.clone(),
            )
            .await
        {
            Ok(event) => return Ok(event),
            Err(EventError::SequenceExhausted) => return Err(EventError::SequenceExhausted),
            Err(_) => {}
        }

        self.inner.journal.close_and_wait().await?;

        let request = RecoveryRequest {
            scope,
            event_type,
            update,
            code,
            message,
            data,
        };
        self.recover_terminal_direct(request).await
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

    pub async fn open_run(&self, run_id: &str) {
        self.run_state(run_id).await;
    }

    pub async fn retained_run_count(&self) -> usize {
        self.inner.states.lock().await.len()
    }

    pub async fn retained_recovery_count(&self) -> usize {
        self.inner.recoveries.lock().await.len()
    }

    pub async fn wait_for_recoveries(&self, deadline: Duration) -> Result<(), EventError> {
        if self.inner.recoveries.lock().await.is_empty() {
            return Ok(());
        }
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

    pub fn is_healthy(&self) -> bool {
        self.inner.journal.is_healthy()
    }

    async fn run_state(&self, run_id: &str) -> Arc<Mutex<EventRunState>> {
        let mut states = self.inner.states.lock().await;
        Arc::clone(states.entry(run_id.to_string()).or_insert_with(|| {
            let (live, _) = broadcast::channel(self.inner.subscriber_capacity);
            Arc::new(Mutex::new(EventRunState { next_seq: 1, live }))
        }))
    }

    async fn isolate_run_state(&self, run_id: &str, expected: &Arc<Mutex<EventRunState>>) {
        let mut states = self.inner.states.lock().await;
        if states
            .get(run_id)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            states.remove(run_id);
        }
    }

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

    async fn reconcile_durable_through(
        &self,
        run_id: &str,
        state: &mut EventRunState,
        terminal: &RunEvent,
    ) -> Result<(), EventError> {
        let missing = terminal
            .seq
            .checked_sub(state.next_seq)
            .and_then(|difference| difference.checked_add(1))
            .ok_or_else(|| {
                EventError::History(HistoryError::new(
                    "HISTORY_RECOVERY_DIVERGED",
                    "authoritative terminal sequence precedes live state",
                ))
            })?;
        if missing > 2 {
            return Err(EventError::History(HistoryError::new(
                "HISTORY_RECOVERY_DIVERGED",
                "too many durable events were missing from live state",
            )));
        }
        let durable = timeout(
            self.inner.operation_timeout,
            self.inner.repository.list_events_after(
                run_id,
                state.next_seq.saturating_sub(1),
                usize::try_from(missing).unwrap_or(2),
            ),
        )
        .await
        .map_err(|_| EventError::JournalOperationTimeout)??;
        let mut expected_seq = state.next_seq;
        for event in &durable {
            if event.seq != expected_seq {
                return Err(EventError::History(HistoryError::new(
                    "HISTORY_RECOVERY_GAP",
                    "durable event sequence diverged during recovery",
                )));
            }
            expected_seq = expected_seq.checked_add(1).ok_or_else(|| {
                EventError::History(HistoryError::new(
                    "HISTORY_DATA_INVALID",
                    "durable event sequence overflowed",
                ))
            })?;
        }
        if durable.last() != Some(terminal) {
            return Err(EventError::History(HistoryError::new(
                "HISTORY_TERMINAL_EVENT_MISMATCH",
                "persisted final event does not match authoritative terminal event",
            )));
        }
        for event in durable {
            commit_live_event(state, event);
        }
        Ok(())
    }
}

fn commit_live_event(state: &mut EventRunState, event: RunEvent) {
    state.next_seq += 1;
    let _ = state.live.send(event);
}

fn ensure_sequence_available(state: &EventRunState) -> Result<(), EventError> {
    if state.next_seq == u64::MAX {
        Err(EventError::SequenceExhausted)
    } else {
        Ok(())
    }
}

fn is_terminal(event_type: RunEventType) -> bool {
    matches!(
        event_type,
        RunEventType::RunCompleted
            | RunEventType::RunFailed
            | RunEventType::RunCancelled
            | RunEventType::RunInterrupted
    )
}

fn validate_terminal_request(
    scope: &RunEventScope,
    event_type: RunEventType,
    update: &TerminalUpdate,
) -> Result<(), EventError> {
    let expected_type = match update.status() {
        crate::history::types::RunStatus::Completed => RunEventType::RunCompleted,
        crate::history::types::RunStatus::Failed => RunEventType::RunFailed,
        crate::history::types::RunStatus::Cancelled => RunEventType::RunCancelled,
        crate::history::types::RunStatus::Interrupted => RunEventType::RunInterrupted,
        crate::history::types::RunStatus::Created | crate::history::types::RunStatus::Running => {
            unreachable!("typed terminal update is terminal")
        }
    };
    if update.run_id != scope.run_id || event_type != expected_type {
        return Err(EventError::History(HistoryError::new(
            "HISTORY_EVENT_INVALID",
            "terminal event does not match its typed update",
        )));
    }
    Ok(())
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
