//! Server-sent event transport for the v1 HTTP API.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::Stream;
use insight_engine::{
    events::protocol::RunEventType,
    run_stream::{
        DurableRunStreamSnapshot, LiveRunStreamBroker, LiveRunStreamDelivery,
        LiveRunStreamSubscriber, RunCompletedSnapshot, RunFailedSnapshot, RunInitialSnapshot,
        RunStatus, RunStoppedSnapshot, RunStreamEvent, RunStreamGapAction, RunTerminalKind,
    },
    RunId,
};
use insight_runtime::{
    terminal_only::{TerminalAttachedRun, TerminalRunSubscription},
    AttachedRun, ConversationStreamDelivery, ConversationStreamPrivacy,
    ConversationVisibilityGuard, FullConversationVisibilityGuard, RunService, RunSubscription,
};
use serde::de::DeserializeOwned;
use tokio::{sync::mpsc, time::Instant};

const OUTBOUND_EVENT_CAPACITY: usize = 32;

struct OutboundEvent {
    event: Result<Event, Infallible>,
    terminal_barrier: Option<Arc<dyn TerminalFrameBarrier>>,
}

struct RunOutboundStream {
    inner: mpsc::Receiver<OutboundEvent>,
    conversation_privacy: Option<ConversationStreamPrivacy>,
    delivered_conversation_privacy: Option<ConversationStreamDelivery>,
    delivered_terminal_barrier: Option<Arc<dyn TerminalFrameBarrier>>,
}

impl RunOutboundStream {
    fn new(
        receiver: mpsc::Receiver<OutboundEvent>,
        conversation_privacy: Option<ConversationStreamPrivacy>,
    ) -> Self {
        Self {
            inner: receiver,
            conversation_privacy,
            delivered_conversation_privacy: None,
            delivered_terminal_barrier: None,
        }
    }
}

impl Stream for RunOutboundStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.delivered_conversation_privacy = None;
        if self
            .conversation_privacy
            .as_ref()
            .is_some_and(ConversationStreamPrivacy::is_cancelled)
        {
            self.inner.close();
            while self.inner.try_recv().is_ok() {}
            self.delivered_terminal_barrier = None;
            return Poll::Ready(None);
        }
        // A terminal fence is retained across the poll that hands the frame
        // to Axum/Hyper. It is released only when transport asks for the next
        // frame (or drops the stream), so privacy DELETE cannot complete in
        // the gap between queueing and consuming the terminal frame.
        self.delivered_terminal_barrier = None;
        match self.inner.poll_recv(context) {
            Poll::Ready(Some(outbound)) => {
                if let Some(privacy) = self.conversation_privacy.clone() {
                    let Some(delivery) = privacy.try_begin_delivery() else {
                        self.inner.close();
                        while self.inner.try_recv().is_ok() {}
                        return Poll::Ready(None);
                    };
                    self.delivered_conversation_privacy = Some(delivery);
                }
                self.delivered_terminal_barrier = outbound.terminal_barrier;
                Poll::Ready(Some(outbound.event))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Terminal-frame barrier checked after the authoritative snapshot and live
/// tail are calibrated, immediately before the HTTP stream may publish its
/// terminal frame.
///
/// Conversation turns use this seam to make the atomic
/// `run result + assistant message` transaction an explicit transport
/// prerequisite and to recheck privacy authority. Implementations must not
/// carry terminal content in the barrier itself.
#[async_trait]
pub trait TerminalFrameBarrier: Send + Sync {
    async fn wait_until_committed(&self, run_id: &str) -> Result<(), TerminalFrameBarrierError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalFrameBarrierError {
    code: &'static str,
    message: &'static str,
}

impl TerminalFrameBarrierError {
    pub const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    pub const fn message(&self) -> &'static str {
        self.message
    }
}

struct SnapshotCommitBarrier;

#[async_trait]
impl TerminalFrameBarrier for SnapshotCommitBarrier {
    async fn wait_until_committed(&self, _run_id: &str) -> Result<(), TerminalFrameBarrierError> {
        // For ordinary Runs the run snapshot itself is the commit
        // authority loaded immediately before this hook.
        Ok(())
    }
}

struct FullConversationSnapshotCommitBarrier {
    service: RunService,
    conversation_id: String,
    tenant_id: String,
    user_id: String,
    visibility_guard: tokio::sync::Mutex<Option<FullConversationVisibilityGuard>>,
}

struct TerminalConversationSnapshotCommitBarrier {
    service: RunService,
    tenant_id: String,
    user_id: String,
    visibility_guard: tokio::sync::Mutex<Option<ConversationVisibilityGuard>>,
}

#[async_trait]
impl TerminalFrameBarrier for TerminalConversationSnapshotCommitBarrier {
    async fn wait_until_committed(&self, run_id: &str) -> Result<(), TerminalFrameBarrierError> {
        let guard = self
            .service
            .acquire_visible_terminal_conversation_run(&self.tenant_id, &self.user_id, run_id)
            .await
            .map_err(|_| {
                TerminalFrameBarrierError::new(
                    "RUN_STREAM_TERMINAL_UNAVAILABLE",
                    "Run stream terminal snapshot is no longer available",
                )
            })?;
        *self.visibility_guard.lock().await = Some(guard);
        Ok(())
    }
}

#[async_trait]
impl TerminalFrameBarrier for FullConversationSnapshotCommitBarrier {
    async fn wait_until_committed(&self, run_id: &str) -> Result<(), TerminalFrameBarrierError> {
        let guard = self
            .service
            .acquire_visible_full_conversation_run(
                &self.conversation_id,
                &self.tenant_id,
                &self.user_id,
                run_id,
            )
            .await
            .map_err(|_| {
                TerminalFrameBarrierError::new(
                    "RUN_STREAM_TERMINAL_UNAVAILABLE",
                    "Run stream terminal snapshot is no longer available",
                )
            })?;
        *self.visibility_guard.lock().await = Some(guard);
        Ok(())
    }
}

pub(crate) fn run_stream(
    attached: AttachedRun,
    keep_alive_interval: Duration,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    run_stream_with_terminal_barrier(
        attached,
        keep_alive_interval,
        Arc::new(SnapshotCommitBarrier),
    )
}

pub(crate) fn full_conversation_run_stream(
    attached: AttachedRun,
    keep_alive_interval: Duration,
    service: RunService,
    conversation_id: String,
    tenant_id: String,
    user_id: String,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    run_stream_with_terminal_barrier(
        attached,
        keep_alive_interval,
        Arc::new(FullConversationSnapshotCommitBarrier {
            service,
            conversation_id,
            tenant_id,
            user_id,
            visibility_guard: tokio::sync::Mutex::new(None),
        }),
    )
}

pub(crate) fn terminal_run_stream(
    attached: TerminalAttachedRun,
    keep_alive_interval: Duration,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    terminal_run_stream_with_terminal_barrier(
        attached,
        keep_alive_interval,
        Arc::new(SnapshotCommitBarrier),
    )
}

pub(crate) fn terminal_conversation_run_stream(
    attached: TerminalAttachedRun,
    keep_alive_interval: Duration,
    service: RunService,
    tenant_id: String,
    user_id: String,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    terminal_run_stream_with_terminal_barrier(
        attached,
        keep_alive_interval,
        Arc::new(TerminalConversationSnapshotCommitBarrier {
            service,
            tenant_id,
            user_id,
            visibility_guard: tokio::sync::Mutex::new(None),
        }),
    )
}

pub(crate) fn run_stream_with_terminal_barrier(
    attached: AttachedRun,
    keep_alive_interval: Duration,
    terminal_frame_barrier: Arc<dyn TerminalFrameBarrier>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    run_stream_from_attached(
        RunAttachedStream::from_durable(attached),
        keep_alive_interval,
        terminal_frame_barrier,
    )
}

pub(crate) fn terminal_run_stream_with_terminal_barrier(
    attached: TerminalAttachedRun,
    keep_alive_interval: Duration,
    terminal_frame_barrier: Arc<dyn TerminalFrameBarrier>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    run_stream_from_attached(
        RunAttachedStream::from_terminal(attached),
        keep_alive_interval,
        terminal_frame_barrier,
    )
}

fn run_stream_from_attached(
    attached: RunAttachedStream,
    keep_alive_interval: Duration,
    terminal_frame_barrier: Arc<dyn TerminalFrameBarrier>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let outbound_write_timeout = attached.outbound_write_timeout;
    let conversation_privacy = attached.conversation_privacy.clone();
    let (sender, receiver) = mpsc::channel(OUTBOUND_EVENT_CAPACITY);
    tokio::spawn(async move {
        let mut dispatcher = RunStreamDispatcher::new(attached, terminal_frame_barrier);
        loop {
            let public_event = tokio::select! {
                _ = sender.closed() => {
                    tracing::debug!(
                        run_id = dispatcher.run_id(),
                        code = "SSE_OUTBOUND_CLOSED",
                        "run-stream client closed the bounded output"
                    );
                    break;
                }
                _ = conversation_privacy_cancelled(dispatcher.conversation_privacy()) => {
                    tracing::debug!(
                        run_id = dispatcher.run_id(),
                        code = "SSE_CONVERSATION_PRIVACY_DELETED",
                        "Run stream stopped before publishing more Conversation content"
                    );
                    break;
                }
                public_event = dispatcher.next_event() => match public_event {
                    Some(public_event) => public_event,
                    None => break,
                },
            };
            if dispatcher
                .conversation_privacy()
                .as_ref()
                .is_some_and(ConversationStreamPrivacy::is_cancelled)
            {
                break;
            }
            let run_terminal = public_event.is_run_terminal();
            let ends_stream = public_event.ends_stream();
            let encoded = match encode_event(&public_event) {
                Ok(encoded) => encoded,
                Err(error) => {
                    tracing::error!(
                        run_id = dispatcher.run_id(),
                        code = "SSE_ENCODE_FAILED",
                        error = %error,
                        "run-stream event encoding failed"
                    );
                    break;
                }
            };
            let terminal_barrier =
                run_terminal.then(|| Arc::clone(&dispatcher.terminal_frame_barrier));
            match tokio::time::timeout(
                outbound_write_timeout,
                sender.send(OutboundEvent {
                    event: Ok(encoded),
                    terminal_barrier,
                }),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => {
                    tracing::debug!(
                        run_id = dispatcher.run_id(),
                        code = "SSE_OUTBOUND_UNWRITABLE",
                        "run-stream client stopped accepting bounded output"
                    );
                    break;
                }
            }
            if ends_stream {
                break;
            }
        }
    });

    Sse::new(RunOutboundStream::new(receiver, conversation_privacy)).keep_alive(
        KeepAlive::new()
            .interval(keep_alive_interval)
            .text("keep-alive"),
    )
}

async fn conversation_privacy_cancelled(privacy: Option<ConversationStreamPrivacy>) {
    match privacy {
        Some(privacy) => privacy.cancelled().await,
        None => futures::future::pending::<()>().await,
    }
}

enum RunLifecycleSourceEvent {
    Running,
    Terminal(DurableRunStreamSnapshot),
}

#[async_trait]
trait RunLifecycleSource: Send {
    async fn recv_lifecycle(&mut self) -> Result<RunLifecycleSourceEvent, &'static str>;
}

#[async_trait]
impl RunLifecycleSource for DurableLifecycleSource {
    async fn recv_lifecycle(&mut self) -> Result<RunLifecycleSourceEvent, &'static str> {
        if !self.initial_snapshot_checked {
            self.initial_snapshot_checked = true;
            match self.subscription.load_run_stream_snapshot().await {
                Ok(snapshot) => return Ok(RunLifecycleSourceEvent::Terminal(snapshot)),
                Err(error) if error.code() == "RUN_STREAM_SNAPSHOT_UNAVAILABLE" => {}
                Err(error) => return Err(error.code()),
            }
        }
        loop {
            match self.subscription.recv().await {
                Ok(event) if event.event_type == RunEventType::RunStarted => {
                    return Ok(RunLifecycleSourceEvent::Running);
                }
                Ok(event)
                    if matches!(
                        event.event_type,
                        RunEventType::RunCompleted
                            | RunEventType::RunFailed
                            | RunEventType::RunCancelled
                            | RunEventType::RunInterrupted
                    ) =>
                {
                    return self
                        .subscription
                        .load_run_stream_snapshot()
                        .await
                        .map(RunLifecycleSourceEvent::Terminal)
                        .map_err(|error| error.code());
                }
                Ok(_) => {}
                Err(error) => return Err(error.code()),
            }
        }
    }
}

struct DurableLifecycleSource {
    subscription: RunSubscription,
    initial_snapshot_checked: bool,
}

struct TerminalLifecycleSource {
    subscription: TerminalRunSubscription,
    running_pending: bool,
}

#[async_trait]
impl RunLifecycleSource for TerminalLifecycleSource {
    async fn recv_lifecycle(&mut self) -> Result<RunLifecycleSourceEvent, &'static str> {
        if self.running_pending {
            self.running_pending = false;
            return Ok(RunLifecycleSourceEvent::Running);
        }
        self.subscription
            .recv_terminal()
            .await
            .map(RunLifecycleSourceEvent::Terminal)
            .map_err(|error| error.code())
    }
}

struct RunAttachedStream {
    run_id: String,
    subscription: Box<dyn RunLifecycleSource>,
    live_run_stream: Box<dyn LiveRunStreamSubscriber>,
    live_run_stream_broker: Arc<dyn LiveRunStreamBroker>,
    terminal_barrier_timeout: Duration,
    outbound_write_timeout: Duration,
    conversation_privacy: Option<ConversationStreamPrivacy>,
}

impl RunAttachedStream {
    fn from_durable(attached: AttachedRun) -> Self {
        Self {
            run_id: attached.run_id,
            subscription: Box::new(DurableLifecycleSource {
                subscription: attached.subscription,
                initial_snapshot_checked: false,
            }),
            live_run_stream: attached.live_run_stream,
            live_run_stream_broker: attached.live_run_stream_broker,
            terminal_barrier_timeout: attached.terminal_barrier_timeout,
            outbound_write_timeout: attached.outbound_write_timeout,
            conversation_privacy: attached.conversation_privacy,
        }
    }

    fn from_terminal(attached: TerminalAttachedRun) -> Self {
        let running_pending = attached.subscription.load_run_stream_snapshot().is_none();
        Self {
            run_id: attached.run_id,
            subscription: Box::new(TerminalLifecycleSource {
                subscription: attached.subscription,
                running_pending,
            }),
            live_run_stream: attached.live_run_stream,
            live_run_stream_broker: attached.live_run_stream_broker,
            terminal_barrier_timeout: attached.terminal_barrier_timeout,
            outbound_write_timeout: attached.outbound_write_timeout,
            conversation_privacy: attached.conversation_privacy,
        }
    }
}

struct RunStreamDispatcher {
    attached: RunAttachedStream,
    terminal_frame_barrier: Arc<dyn TerminalFrameBarrier>,
    pending: VecDeque<RunStreamEvent>,
    next_sequence: u64,
    terminal_snapshot: Option<DurableRunStreamSnapshot>,
    terminal_barrier_deadline: Option<Instant>,
    live_open: bool,
    seen_sealed_items: BTreeSet<String>,
    seen_unknown_tail_items: BTreeSet<String>,
    seen_item_watermarks: BTreeMap<String, u64>,
    running: bool,
    done: bool,
}

impl RunStreamDispatcher {
    fn new(
        attached: RunAttachedStream,
        terminal_frame_barrier: Arc<dyn TerminalFrameBarrier>,
    ) -> Self {
        let run = RunInitialSnapshot::new(attached.run_id.clone(), RunStatus::Created)
            .expect("attached Run IDs are valid public labels");
        Self {
            attached,
            terminal_frame_barrier,
            pending: VecDeque::from([RunStreamEvent::RunLifecycleCreated {
                sequence_number: 0,
                run,
            }]),
            next_sequence: 1,
            terminal_snapshot: None,
            terminal_barrier_deadline: None,
            live_open: true,
            seen_sealed_items: BTreeSet::new(),
            seen_unknown_tail_items: BTreeSet::new(),
            seen_item_watermarks: BTreeMap::new(),
            running: false,
            done: false,
        }
    }

    fn run_id(&self) -> &str {
        &self.attached.run_id
    }

    fn conversation_privacy(&self) -> Option<ConversationStreamPrivacy> {
        self.attached.conversation_privacy.clone()
    }

    fn allocate_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        sequence
    }

    async fn next_event(&mut self) -> Option<RunStreamEvent> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Some(event);
            }
            if self.done {
                return None;
            }
            if self.terminal_snapshot.is_some() {
                if self.live_open {
                    let deadline = self
                        .terminal_barrier_deadline
                        .expect("terminal barrier has a deadline");
                    match tokio::time::timeout_at(deadline, self.attached.live_run_stream.recv())
                        .await
                    {
                        Ok(Ok(delivery)) => {
                            if let Some(event) = self.project_live_delivery(delivery) {
                                return Some(event);
                            }
                            continue;
                        }
                        Ok(Err(_)) | Err(_) => self.live_open = false,
                    }
                }
                self.enqueue_manifest_unknown_tail_gaps();
                if let Some(event) = self.pending.pop_front() {
                    return Some(event);
                }
                let snapshot = self
                    .terminal_snapshot
                    .take()
                    .expect("terminal snapshot is present");
                let sequence = self.allocate_sequence();
                self.done = true;
                return Some(match terminal_event(snapshot, sequence) {
                    Ok(event) => event,
                    Err(()) => protocol_error(
                        sequence,
                        "RUN_STREAM_SNAPSHOT_INVALID",
                        "terminal Run snapshot is invalid",
                    ),
                });
            }

            if self.live_open && self.running {
                let terminal = &mut self.attached.subscription;
                let live = &mut self.attached.live_run_stream;
                tokio::select! {
                    lifecycle = terminal.recv_lifecycle() => {
                        match lifecycle {
                            Ok(RunLifecycleSourceEvent::Running) => {
                                if self.running {
                                    continue;
                                }
                                self.running = true;
                                let sequence_number = self.allocate_sequence();
                                return Some(RunStreamEvent::RunLifecycleRunning {
                                    sequence_number,
                                    run: RunInitialSnapshot::new(
                                        self.attached.run_id.clone(),
                                        RunStatus::Running,
                                    ).expect("attached Run IDs are valid public labels"),
                                });
                            }
                            Ok(RunLifecycleSourceEvent::Terminal(snapshot)) => {
                                if let Err(error) = self.begin_terminal_barrier(snapshot).await {
                                    return Some(self.terminal_barrier_error(error));
                                }
                            }
                            Err(_) => {
                                let sequence = self.allocate_sequence();
                                self.done = true;
                                return Some(protocol_error(
                                    sequence,
                                    "RUN_STREAM_SOURCE_CLOSED",
                                    "Run stream closed before terminal calibration",
                                ));
                            }
                        }
                    }
                    live_event = live.recv() => {
                        match live_event {
                            Ok(delivery) => {
                                if let Some(event) = self.project_live_delivery(delivery) {
                                    return Some(event);
                                }
                            }
                            Err(_) => self.live_open = false,
                        }
                    }
                }
            } else {
                match self.attached.subscription.recv_lifecycle().await {
                    Ok(RunLifecycleSourceEvent::Running) => {
                        if self.running {
                            continue;
                        }
                        self.running = true;
                        let sequence_number = self.allocate_sequence();
                        return Some(RunStreamEvent::RunLifecycleRunning {
                            sequence_number,
                            run: RunInitialSnapshot::new(
                                self.attached.run_id.clone(),
                                RunStatus::Running,
                            )
                            .expect("attached Run IDs are valid public labels"),
                        });
                    }
                    Ok(RunLifecycleSourceEvent::Terminal(snapshot)) => {
                        if let Err(error) = self.begin_terminal_barrier(snapshot).await {
                            return Some(self.terminal_barrier_error(error));
                        }
                    }
                    Err(_) => {
                        let sequence = self.allocate_sequence();
                        self.done = true;
                        return Some(protocol_error(
                            sequence,
                            "RUN_STREAM_SOURCE_CLOSED",
                            "Run stream closed before terminal calibration",
                        ));
                    }
                }
            }
        }
    }

    async fn begin_terminal_barrier(
        &mut self,
        snapshot: DurableRunStreamSnapshot,
    ) -> Result<(), TerminalFrameBarrierError> {
        self.terminal_frame_barrier
            .wait_until_committed(&self.attached.run_id)
            .await?;
        if let Ok(run_id) = RunId::new(self.attached.run_id.clone()) {
            let _ = self.attached.live_run_stream_broker.close_run(&run_id);
        }
        self.terminal_snapshot = Some(snapshot);
        self.terminal_barrier_deadline =
            Some(Instant::now() + self.attached.terminal_barrier_timeout);
        Ok(())
    }

    fn terminal_barrier_error(&mut self, error: TerminalFrameBarrierError) -> RunStreamEvent {
        let sequence = self.allocate_sequence();
        self.done = true;
        protocol_error(sequence, error.code(), error.message())
    }

    fn project_live_delivery(&mut self, delivery: LiveRunStreamDelivery) -> Option<RunStreamEvent> {
        match delivery {
            LiveRunStreamDelivery::Publication(publication) => {
                if let Some(identity) = publication.output_item_identity() {
                    self.seen_item_watermarks
                        .entry(identity.item_id().to_owned())
                        .and_modify(|watermark| {
                            *watermark = (*watermark).max(publication.local_sequence())
                        })
                        .or_insert(publication.local_sequence());
                }
                let sequence = self.allocate_sequence();
                Some(publication.into_public_event(sequence))
            }
            LiveRunStreamDelivery::Gap(gap) => {
                if gap.has_unknown_tail() {
                    self.seen_unknown_tail_items
                        .insert(gap.identity().item_id().to_owned());
                } else if let Some(missing_to) = gap.missing_to() {
                    self.seen_item_watermarks
                        .entry(gap.identity().item_id().to_owned())
                        .and_modify(|watermark| *watermark = (*watermark).max(missing_to))
                        .or_insert(missing_to);
                }
                let sequence = self.allocate_sequence();
                Some(gap.into_public_event(sequence))
            }
            LiveRunStreamDelivery::Seal(seal) => {
                self.seen_sealed_items
                    .insert(seal.identity().item_id().to_owned());
                None
            }
        }
    }

    fn enqueue_manifest_unknown_tail_gaps(&mut self) {
        let Some(snapshot) = self.terminal_snapshot.as_ref() else {
            return;
        };
        let Some(items) = snapshot.public_item_manifest().as_array() else {
            return;
        };
        let pending = manifest_items_without_terminal_evidence(
            items,
            &self.seen_sealed_items,
            &self.seen_unknown_tail_items,
            &self.seen_item_watermarks,
        );
        for (item_id, attempt_no, missing_from) in pending {
            self.seen_unknown_tail_items.insert(item_id.clone());
            let sequence_number = self.allocate_sequence();
            self.pending.push_back(RunStreamEvent::RunStreamGap {
                sequence_number,
                item_id,
                attempt_no,
                missing_from,
                missing_to: None,
                unknown_tail: true,
                action: RunStreamGapAction::DiscardProvisionalItem,
            });
        }
    }
}

/// A durable item status proves the final snapshot, not delivery of its
/// transient live tail. Before terminal calibration the dispatcher therefore
/// needs either the broker seal or an already delivered unknown-tail gap for
/// every manifest item. A finite gap alone is not terminal evidence.
fn manifest_items_without_terminal_evidence(
    items: &[serde_json::Value],
    sealed_items: &BTreeSet<String>,
    unknown_tail_items: &BTreeSet<String>,
    item_watermarks: &BTreeMap<String, u64>,
) -> Vec<(String, u32, u64)> {
    items
        .iter()
        .filter_map(|item| {
            let item = item.as_object()?;
            let item_id = item.get("item_id")?.as_str()?.to_owned();
            if sealed_items.contains(&item_id) || unknown_tail_items.contains(&item_id) {
                return None;
            }
            let attempt_no = u32::try_from(item.get("attempt_no")?.as_u64()?).ok()?;
            let missing_from = item_watermarks
                .get(&item_id)
                .map_or(0, |watermark| watermark.saturating_add(1));
            Some((item_id, attempt_no, missing_from))
        })
        .collect()
}

fn terminal_event(
    snapshot: DurableRunStreamSnapshot,
    sequence_number: u64,
) -> Result<RunStreamEvent, ()> {
    let event = match snapshot.terminal_kind() {
        RunTerminalKind::Completed => RunStreamEvent::RunLifecycleCompleted {
            sequence_number,
            run: decode::<RunCompletedSnapshot>(snapshot.run())?,
        },
        RunTerminalKind::Failed => RunStreamEvent::RunLifecycleFailed {
            sequence_number,
            run: decode::<RunFailedSnapshot>(snapshot.run())?,
        },
        RunTerminalKind::TimedOut => RunStreamEvent::RunLifecycleTimedOut {
            sequence_number,
            run: decode::<RunFailedSnapshot>(snapshot.run())?,
        },
        RunTerminalKind::Cancelled => RunStreamEvent::RunLifecycleCancelled {
            sequence_number,
            run: decode::<RunStoppedSnapshot>(snapshot.run())?,
        },
        RunTerminalKind::Interrupted => RunStreamEvent::RunLifecycleInterrupted {
            sequence_number,
            run: decode::<RunStoppedSnapshot>(snapshot.run())?,
        },
    };
    serde_json::to_value(&event).map_err(|_| ())?;
    Ok(event)
}

fn decode<T: DeserializeOwned>(value: &serde_json::Value) -> Result<T, ()> {
    serde_json::from_value(value.clone()).map_err(|_| ())
}

fn protocol_error(
    sequence_number: u64,
    code: impl Into<String>,
    message: impl Into<String>,
) -> RunStreamEvent {
    let code = code.into();
    let code = if code.starts_with("RUN_STREAM_")
        && code.len() <= 128
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        code
    } else {
        "RUN_STREAM_FAILED".to_owned()
    };
    let message = message.into();
    let message =
        if !message.is_empty() && message.len() <= 512 && !message.chars().any(char::is_control) {
            message
        } else {
            "Run stream failed".to_owned()
        };
    RunStreamEvent::RunStreamError {
        sequence_number,
        code,
        message,
    }
}

fn encode_event(event: &RunStreamEvent) -> Result<Event, axum::Error> {
    Event::default()
        .event(event.event_type().as_str())
        .json_data(event)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use futures::StreamExt;
    use insight_engine::{
        run_stream::{
            adapter::durable_run_stream_snapshot_new, DurableRunStreamSnapshot, RunTerminalKind,
            RUN_STREAM_PROTOCOL_VERSION,
        },
        ContentHash, RunId,
    };
    use insight_runtime::{InMemoryLiveRunStreamBroker, LiveRunStreamBroker};
    use serde_json::json;
    use tokio::sync::Notify;

    use super::{
        manifest_items_without_terminal_evidence, protocol_error, RunAttachedStream,
        RunLifecycleSource, RunLifecycleSourceEvent, RunOutboundStream, RunStreamDispatcher,
        SnapshotCommitBarrier, TerminalFrameBarrier, TerminalFrameBarrierError,
    };

    struct UnusedTerminalSource;

    #[async_trait]
    impl RunLifecycleSource for UnusedTerminalSource {
        async fn recv_lifecycle(&mut self) -> Result<RunLifecycleSourceEvent, &'static str> {
            panic!("terminal snapshot was installed directly by the test")
        }
    }

    struct DeleteRaceBarrier {
        entered: Notify,
        release: Notify,
        deleted: AtomicBool,
    }

    #[async_trait]
    impl TerminalFrameBarrier for DeleteRaceBarrier {
        async fn wait_until_committed(
            &self,
            _run_id: &str,
        ) -> Result<(), TerminalFrameBarrierError> {
            self.entered.notify_one();
            self.release.notified().await;
            if self.deleted.load(Ordering::Acquire) {
                Err(TerminalFrameBarrierError::new(
                    "RUN_STREAM_TERMINAL_UNAVAILABLE",
                    "Run stream terminal snapshot is no longer available",
                ))
            } else {
                Ok(())
            }
        }
    }

    fn terminal_snapshot_with_marker(marker: &str) -> DurableRunStreamSnapshot {
        let run_id = "run_privacy_race".to_owned();
        let run = json!({
            "id": run_id,
            "object": "run",
            "status": "completed",
            "output": [],
            "result": {"marker": marker},
            "tool_results": [],
            "retrievals": [],
            "usage": null,
            "usage_status": "unavailable"
        });
        let manifest = json!([]);
        let projection = json!({
            "protocol": RUN_STREAM_PROTOCOL_VERSION,
            "run_id": run_id,
            "terminal_kind": "run.lifecycle.completed",
            "run": run,
            "public_item_manifest": manifest,
        });
        let hash = ContentHash::from_bytes(&serde_jcs::to_vec(&projection).unwrap());
        durable_run_stream_snapshot_new(run_id, RunTerminalKind::Completed, run, manifest, hash)
            .unwrap()
    }

    #[test]
    fn run_stream_envelope_has_no_replay_id() {
        let event = protocol_error(7, "BROKER_LOST", "stream observation was lost");
        let encoded = serde_json::to_value(event).unwrap();
        assert_eq!(encoded["type"], "run.stream.error");
        assert_eq!(encoded["sequence_number"], 7);
        assert_eq!(encoded["code"], "RUN_STREAM_FAILED");
        assert!(!encoded.to_string().contains("output_bytes"));
    }

    #[test]
    fn terminal_barrier_requires_live_evidence_even_for_completed_manifest_items() {
        let items = vec![
            json!({"item_id": "msg_completed", "attempt_no": 1, "status": "completed"}),
            json!({"item_id": "msg_incomplete", "attempt_no": 2, "status": "incomplete"}),
            json!({
                "item_id": "msg_unsealed",
                "attempt_no": 3,
                "status": "incomplete_unsealed"
            }),
        ];
        let sealed = BTreeSet::from(["msg_incomplete".to_owned()]);
        let unknown_tail = BTreeSet::from(["msg_unsealed".to_owned()]);

        assert_eq!(
            manifest_items_without_terminal_evidence(
                &items,
                &sealed,
                &unknown_tail,
                &BTreeMap::from([("msg_completed".to_owned(), 7)]),
            ),
            vec![("msg_completed".to_owned(), 1, 8)]
        );
    }

    #[tokio::test]
    async fn privacy_delete_after_commit_before_terminal_frame_cannot_leak_snapshot() {
        const PRIVATE_MARKER: &str = "private-terminal-marker";
        let run_id = RunId::new("run_privacy_race").unwrap();
        let broker = Arc::new(InMemoryLiveRunStreamBroker::new(4, 4).unwrap());
        let live_run_stream = broker.subscribe(run_id.clone()).await.unwrap();
        let barrier = Arc::new(DeleteRaceBarrier {
            entered: Notify::new(),
            release: Notify::new(),
            deleted: AtomicBool::new(false),
        });
        let attached = RunAttachedStream {
            run_id: run_id.to_string(),
            subscription: Box::new(UnusedTerminalSource),
            live_run_stream,
            live_run_stream_broker: broker,
            terminal_barrier_timeout: Duration::from_secs(1),
            outbound_write_timeout: Duration::from_secs(1),
            conversation_privacy: None,
        };
        let mut dispatcher = RunStreamDispatcher::new(attached, barrier.clone());
        dispatcher.pending.clear();
        dispatcher.live_open = false;

        let snapshot = terminal_snapshot_with_marker(PRIVATE_MARKER);
        let next = tokio::spawn(async move {
            if let Err(error) = dispatcher.begin_terminal_barrier(snapshot).await {
                return dispatcher.terminal_barrier_error(error);
            }
            dispatcher.next_event().await.unwrap()
        });
        barrier.entered.notified().await;
        // This models DELETE completing after the result commit while the
        // terminal-frame authority check is deliberately paused.
        barrier.deleted.store(true, Ordering::Release);
        barrier.release.notify_one();

        let event = next.await.unwrap();
        let encoded = serde_json::to_string(&event).unwrap();
        assert_eq!(event.event_type().as_str(), "run.stream.error");
        assert!(!encoded.contains(PRIVATE_MARKER));
        assert!(encoded.contains("RUN_STREAM_TERMINAL_UNAVAILABLE"));
    }

    #[tokio::test]
    async fn transport_retains_terminal_fence_until_frame_is_consumed() {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let barrier: Arc<dyn TerminalFrameBarrier> = Arc::new(SnapshotCommitBarrier);
        sender
            .send(super::OutboundEvent {
                event: Ok(axum::response::sse::Event::default().event("run.lifecycle.completed")),
                terminal_barrier: Some(Arc::clone(&barrier)),
            })
            .await
            .unwrap();
        drop(sender);
        let mut stream = RunOutboundStream::new(receiver, None);

        assert!(stream.next().await.is_some());
        assert_eq!(
            Arc::strong_count(&barrier),
            2,
            "transport must retain the privacy fence with the delivered terminal frame"
        );
        assert!(stream.next().await.is_none());
        assert_eq!(Arc::strong_count(&barrier), 1);
    }

    #[tokio::test]
    async fn privacy_delete_discards_already_buffered_live_delta() {
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let privacy = insight_runtime::ConversationStreamPrivacy::new();
        sender
            .send(super::OutboundEvent {
                event: Ok(axum::response::sse::Event::default()
                    .event("run.output.text.delta")
                    .data("private-buffered-delta")),
                terminal_barrier: None,
            })
            .await
            .unwrap();
        privacy.cancel();

        let mut stream = RunOutboundStream::new(receiver, Some(privacy));
        assert!(
            stream.next().await.is_none(),
            "a buffered private delta must not be observable after privacy deletion starts"
        );
        assert!(sender.is_closed());
    }

    #[tokio::test]
    async fn privacy_delete_waits_for_in_flight_poll_and_no_frame_follows_completion() {
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let privacy = insight_runtime::ConversationStreamPrivacy::new();
        for marker in ["linearized-before-delete", "must-never-follow-delete"] {
            sender
                .send(super::OutboundEvent {
                    event: Ok(axum::response::sse::Event::default()
                        .event("run.output.text.delta")
                        .data(marker)),
                    terminal_barrier: None,
                })
                .await
                .unwrap();
        }
        drop(sender);
        let mut stream = RunOutboundStream::new(receiver, Some(privacy.clone()));
        assert!(stream.next().await.is_some());

        let mut deletion = tokio::spawn(async move { privacy.cancel_and_wait().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut deletion)
                .await
                .is_err(),
            "DELETE must not complete while a frame delivery poll is in flight"
        );
        assert!(
            stream.next().await.is_none(),
            "the buffered post-cancel frame must be discarded"
        );
        deletion.await.unwrap();
        assert!(
            stream.next().await.is_none(),
            "no frame may be delivered after DELETE completes"
        );
    }
}
