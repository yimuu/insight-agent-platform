//! Core NATS adapter for the transient, live-only Run Stream plane.
//!
//! The adapter owns no durable state. Publications, gaps, and seals are
//! bounded observations; the durable terminal Run snapshot remains in the
//! repository and never crosses this transport.

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet, VecDeque},
    fmt,
    fmt::Write as _,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};

use async_nats::{
    connection::State, Client, ConnectError, ConnectErrorKind, ConnectOptions, Event, ServerError,
    Subscriber,
};
use async_trait::async_trait;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio::{runtime::Handle, task::JoinHandle, time};
use tokio_util::sync::CancellationToken;
use url::Url;

use insight_engine::run_stream::adapter::RunQueue;
use insight_engine::run_stream::{
    LiveRunStreamBroker, LiveRunStreamBrokerCapability, LiveRunStreamBrokerError,
    LiveRunStreamByteLimits, LiveRunStreamCloseOutcome, LiveRunStreamDelivery, LiveRunStreamGap,
    LiveRunStreamItemIdentity, LiveRunStreamPublication, LiveRunStreamPublishOutcome,
    LiveRunStreamSeal, LiveRunStreamSubscriber, LiveRunStreamTerminalBarrierOutcome,
    RunStreamEventType,
};
use insight_engine::RunId;

use crate::run_stream_wire::{
    decode, encode_gap, encode_publication, encode_seal, LIVE_RUN_STREAM_BUS_MAX_FRAME_BYTES,
};

const RUN_STREAM_BUS_CONFIG_INVALID: &str = "RUN_STREAM_BUS_CONFIG_INVALID";
const RUN_STREAM_BUS_AUTH_FAILED: &str = "RUN_STREAM_BUS_AUTH_FAILED";
const RUN_STREAM_BUS_UNAVAILABLE: &str = "RUN_STREAM_BUS_UNAVAILABLE";
const RUN_STREAM_BUS_NOT_READY: &str = "RUN_STREAM_BUS_NOT_READY";
const RUN_STREAM_BUS_SHUTDOWN_TIMEOUT: &str = "RUN_STREAM_BUS_SHUTDOWN_TIMEOUT";
const RUN_STREAM_BUS_SUBSCRIBER_EXISTS: &str = "RUN_STREAM_BUS_SUBSCRIBER_EXISTS";
const MAX_OUTBOUND_REAPER_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsCoreTlsOptions {
    pub required: bool,
    pub root_certificates: Vec<PathBuf>,
    pub client_certificate: Option<PathBuf>,
    pub client_private_key: Option<PathBuf>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct NatsCoreLiveRunStreamBrokerOptions {
    pub servers: Vec<String>,
    pub namespace: String,
    pub credentials: Option<String>,
    pub tls: NatsCoreTlsOptions,
    pub connect_timeout: Duration,
    pub subscription_ready_timeout: Duration,
    pub reconnect_min_delay: Duration,
    pub reconnect_max_delay: Duration,
    pub max_pending_messages: usize,
    pub max_pending_bytes: usize,
    pub body_queue_capacity: usize,
    pub control_queue_capacity: usize,
    pub max_frame_bytes: usize,
    pub max_item_bytes: usize,
    pub max_run_bytes: usize,
    pub drain_timeout: Duration,
    pub outbound_idle_timeout: Duration,
}

impl fmt::Debug for NatsCoreLiveRunStreamBrokerOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsCoreLiveRunStreamBrokerOptions")
            .field("servers", &self.servers)
            .field("namespace", &self.namespace)
            .field(
                "credentials",
                &self.credentials.as_ref().map(|_| "REDACTED"),
            )
            .field("tls", &self.tls)
            .field("connect_timeout", &self.connect_timeout)
            .field(
                "subscription_ready_timeout",
                &self.subscription_ready_timeout,
            )
            .field("reconnect_min_delay", &self.reconnect_min_delay)
            .field("reconnect_max_delay", &self.reconnect_max_delay)
            .field("max_pending_messages", &self.max_pending_messages)
            .field("max_pending_bytes", &self.max_pending_bytes)
            .field("body_queue_capacity", &self.body_queue_capacity)
            .field("control_queue_capacity", &self.control_queue_capacity)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("max_item_bytes", &self.max_item_bytes)
            .field("max_run_bytes", &self.max_run_bytes)
            .field("drain_timeout", &self.drain_timeout)
            .field("outbound_idle_timeout", &self.outbound_idle_timeout)
            .finish()
    }
}

impl NatsCoreLiveRunStreamBrokerOptions {
    pub fn validate(&self) -> Result<(), LiveRunStreamBrokerError> {
        if self.servers.is_empty()
            || self.servers.len() > 16
            || self.servers.iter().collect::<BTreeSet<_>>().len() != self.servers.len()
            || self.servers.iter().any(|server| !valid_server_url(server))
            || !valid_namespace(&self.namespace)
            || self.body_queue_capacity == 0
            || self.control_queue_capacity == 0
            || self.connect_timeout.is_zero()
            || self.subscription_ready_timeout.is_zero()
            || self.reconnect_min_delay.is_zero()
            || self.reconnect_min_delay > self.reconnect_max_delay
            || self.reconnect_max_delay > Duration::from_secs(30)
            || self.max_pending_messages == 0
            || self.max_pending_messages > 1_000_000
            || self.max_pending_bytes < self.max_frame_bytes
            || self.max_pending_bytes > 1_024 * 1_024 * 1_024
            || self.drain_timeout.is_zero()
            || self.outbound_idle_timeout.is_zero()
            || self.max_frame_bytes < 256
            || self.max_frame_bytes > LIVE_RUN_STREAM_BUS_MAX_FRAME_BYTES
            || self.tls.client_certificate.is_some() != self.tls.client_private_key.is_some()
        {
            return Err(config_invalid());
        }
        LiveRunStreamByteLimits::new(
            self.max_frame_bytes,
            self.max_item_bytes,
            self.max_run_bytes,
        )?;
        Ok(())
    }

    fn byte_limits(&self) -> LiveRunStreamByteLimits {
        LiveRunStreamByteLimits {
            max_frame_bytes: self.max_frame_bytes,
            max_item_bytes: self.max_item_bytes,
            max_run_bytes: self.max_run_bytes,
        }
    }
}

fn valid_server_url(value: &str) -> bool {
    let Ok(url) = Url::parse(value) else {
        return false;
    };
    matches!(url.scheme(), "nats" | "tls")
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && url.host_str().is_some()
        && url.port().is_some()
        && matches!(url.path(), "" | "/")
}

pub fn nats_live_run_stream_subject(
    namespace: &str,
    run_id: &RunId,
) -> Result<String, LiveRunStreamBrokerError> {
    if !valid_namespace(namespace) {
        return Err(config_invalid());
    }
    let digest = Sha256::digest(run_id.as_str().as_bytes());
    let mut run_key = String::with_capacity(64);
    for byte in digest {
        let _ = write!(run_key, "{byte:02x}");
    }
    Ok(format!("insight.{namespace}.run_stream.v1.{run_key}"))
}

fn valid_namespace(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=64).contains(&bytes.len())
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

#[derive(Clone)]
pub struct NatsCoreLiveRunStreamBroker {
    inner: Arc<NatsBrokerInner>,
}

struct NatsBrokerInner {
    client: Client,
    options: NatsCoreLiveRunStreamBrokerOptions,
    runtime: Handle,
    accepting: AtomicBool,
    outbound: Mutex<BTreeMap<RunId, Arc<OutboundRegistration>>>,
    inbound: Mutex<BTreeMap<RunId, Arc<InboundRegistration>>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    publisher_task: Mutex<Option<JoinHandle<()>>>,
    maintenance_cancel: CancellationToken,
    transport: Arc<TransportQueue>,
    stats: Arc<NatsRunStreamStats>,
}

struct OutboundRegistration {
    queue: Arc<RunQueue>,
    activity: Mutex<OutboundActivity>,
}

struct OutboundActivity {
    last_activity: Instant,
    open_items: BTreeSet<LiveRunStreamItemIdentity>,
}

struct InboundRegistration {
    queue: Arc<RunQueue>,
    cancel: CancellationToken,
}

impl OutboundRegistration {
    fn new(queue: Arc<RunQueue>) -> Self {
        Self::new_at(queue, Instant::now())
    }

    fn new_at(queue: Arc<RunQueue>, last_activity: Instant) -> Self {
        Self {
            queue,
            activity: Mutex::new(OutboundActivity {
                last_activity,
                open_items: BTreeSet::new(),
            }),
        }
    }

    fn begin_item(&self, identity: &LiveRunStreamItemIdentity) {
        let mut activity = lock(&self.activity);
        activity.last_activity = Instant::now();
        activity.open_items.insert(identity.clone());
    }

    fn finish_item(&self, identity: &LiveRunStreamItemIdentity) -> bool {
        let mut activity = lock(&self.activity);
        activity.last_activity = Instant::now();
        activity.open_items.remove(identity);
        activity.open_items.is_empty()
    }

    fn is_idle_at(&self, now: Instant, idle_timeout: Duration) -> bool {
        let activity = lock(&self.activity);
        now.saturating_duration_since(activity.last_activity) >= idle_timeout
    }
}

impl Drop for NatsBrokerInner {
    fn drop(&mut self) {
        self.accepting.store(false, Ordering::Release);
        self.maintenance_cancel.cancel();
        self.transport.close();
        for registration in lock(&self.outbound).values() {
            registration.queue.close();
        }
        for registration in lock(&self.inbound).values() {
            registration.cancel.cancel();
            registration.queue.close();
        }
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
        if let Some(task) = lock(&self.publisher_task).take() {
            task.abort();
        }
    }
}

impl NatsCoreLiveRunStreamBroker {
    pub async fn connect(
        options: NatsCoreLiveRunStreamBrokerOptions,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        options.validate()?;
        // The workspace also uses SQLx's aws-lc-rs Rustls feature. Explicitly
        // choose the adapter's audited `ring` provider before Rustls builds a
        // client config so feature unification cannot cause a process panic.
        // If another component already installed a provider, that process-wide
        // choice is retained.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let runtime = Handle::try_current().map_err(|_| unavailable())?;
        let stats = Arc::new(NatsRunStreamStats::default());
        let client_capacity = options
            .max_pending_messages
            .min((options.max_pending_bytes / options.max_frame_bytes).max(1));
        let event_stats = Arc::clone(&stats);
        let reconnect_min = options.reconnect_min_delay;
        let reconnect_max = options.reconnect_max_delay;
        let mut connect = ConnectOptions::new()
            .name("insight-run-stream-v1")
            .connection_timeout(options.connect_timeout)
            .max_reconnects(None)
            .client_capacity(client_capacity)
            .subscription_capacity(
                options
                    .body_queue_capacity
                    .saturating_add(options.control_queue_capacity)
                    .max(1),
            )
            .require_tls(options.tls.required)
            .event_callback(move |event| {
                let stats = Arc::clone(&event_stats);
                async move { stats.record_connection_event(event) }
            })
            .reconnect_delay_callback(move |attempt| {
                reconnect_delay(attempt, reconnect_min, reconnect_max)
            });
        if let Some(credentials) = options.credentials.as_deref() {
            connect = connect
                .credentials(credentials)
                .map_err(|_| config_invalid())?;
        }
        for root in &options.tls.root_certificates {
            connect = connect.add_root_certificates(root.clone());
        }
        if let (Some(certificate), Some(private_key)) = (
            options.tls.client_certificate.as_ref(),
            options.tls.client_private_key.as_ref(),
        ) {
            connect = connect.add_client_certificate(certificate.clone(), private_key.clone());
        }
        let servers = options
            .servers
            .iter()
            .map(|server| server.parse::<async_nats::ServerAddr>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| config_invalid())?;
        let client = time::timeout(options.connect_timeout, connect.connect(servers))
            .await
            .map_err(|_| unavailable())?
            .map_err(map_connect_error)?;
        if client.max_payload() < options.max_frame_bytes {
            return Err(config_invalid());
        }
        time::timeout(options.connect_timeout, client.flush())
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
        stats.connected.store(true, Ordering::Release);
        stats.ever_connected.store(true, Ordering::Release);
        let transport = Arc::new(TransportQueue::new(
            options.max_pending_messages,
            options.max_pending_bytes,
            options.max_frame_bytes,
        ));
        let inner = Arc::new(NatsBrokerInner {
            client: client.clone(),
            options,
            runtime: runtime.clone(),
            accepting: AtomicBool::new(true),
            outbound: Mutex::new(BTreeMap::new()),
            inbound: Mutex::new(BTreeMap::new()),
            tasks: Mutex::new(Vec::new()),
            publisher_task: Mutex::new(None),
            maintenance_cancel: CancellationToken::new(),
            transport: Arc::clone(&transport),
            stats: Arc::clone(&stats),
        });
        let publisher = runtime.spawn(nats_publisher(
            client,
            inner.options.namespace.clone(),
            transport,
            stats,
        ));
        *lock(&inner.publisher_task) = Some(publisher);
        let maintenance = runtime.spawn(outbound_maintenance(Arc::downgrade(&inner)));
        lock(&inner.tasks).push(maintenance);
        Ok(Self { inner })
    }

    pub fn is_accepting(&self) -> bool {
        self.inner.accepting.load(Ordering::Acquire)
    }

    pub fn namespace(&self) -> &str {
        &self.inner.options.namespace
    }

    pub async fn check_readiness(
        &self,
        readiness_timeout: Duration,
    ) -> Result<(), LiveRunStreamBrokerError> {
        if !self.is_accepting()
            || self.inner.client.connection_state() != State::Connected
            || self
                .inner
                .stats
                .authorization_failed
                .load(Ordering::Acquire)
        {
            if self
                .inner
                .stats
                .authorization_failed
                .load(Ordering::Acquire)
            {
                return Err(auth_failed());
            }
            return Err(not_ready());
        }
        if self.inner.client.max_payload() < self.inner.options.max_frame_bytes {
            return Err(not_ready());
        }
        time::timeout(readiness_timeout, self.inner.client.flush())
            .await
            .map_err(|_| not_ready())?
            .map_err(|_| not_ready())?;
        if self
            .inner
            .stats
            .authorization_failed
            .load(Ordering::Acquire)
        {
            return Err(auth_failed());
        }
        Ok(())
    }

    pub async fn shutdown(&self, grace: Duration) -> Result<(), LiveRunStreamBrokerError> {
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.maintenance_cancel.cancel();
        let budget = grace.min(self.inner.options.drain_timeout);
        let deadline = Instant::now() + budget;

        let outbound = std::mem::take(&mut *lock(&self.inner.outbound));
        for registration in outbound.into_values() {
            registration.queue.close();
        }
        let inbound = std::mem::take(&mut *lock(&self.inner.inbound));
        for registration in inbound.into_values() {
            registration.cancel.cancel();
            registration.queue.close();
        }

        let tasks = std::mem::take(&mut *lock(&self.inner.tasks));
        let tasks_drained = wait_for_tasks(tasks, remaining(deadline)).await;
        self.inner.transport.close();
        let publisher = lock(&self.inner.publisher_task).take();
        let publisher_drained = match publisher {
            Some(task) => wait_for_tasks(vec![task], remaining(deadline)).await,
            None => true,
        };
        let client_drained = time::timeout(remaining(deadline), self.inner.client.drain())
            .await
            .is_ok_and(|result| result.is_ok());
        if tasks_drained && publisher_drained && client_drained {
            Ok(())
        } else {
            Err(LiveRunStreamBrokerError::new(
                RUN_STREAM_BUS_SHUTDOWN_TIMEOUT,
                "NATS Core live Run stream shutdown exceeded its bounded grace period",
            ))
        }
    }

    fn outbound_registration(
        &self,
        run_id: &RunId,
        item: Option<&LiveRunStreamItemIdentity>,
    ) -> Option<Arc<OutboundRegistration>> {
        if !self.is_accepting()
            || self
                .inner
                .stats
                .authorization_failed
                .load(Ordering::Acquire)
            || self.inner.client.connection_state() != State::Connected
        {
            return None;
        }
        let mut registrations = lock(&self.inner.outbound);
        if !self.is_accepting()
            || self
                .inner
                .stats
                .authorization_failed
                .load(Ordering::Acquire)
            || self.inner.client.connection_state() != State::Connected
        {
            return None;
        }
        let registration = match registrations.entry(run_id.clone()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let registration = Arc::new(OutboundRegistration::new(Arc::new(
                    RunQueue::new_with_limits(
                        run_id.clone(),
                        self.inner.options.body_queue_capacity,
                        self.inner.options.control_queue_capacity,
                        self.inner.options.byte_limits(),
                    ),
                )));
                entry.insert(Arc::clone(&registration));
                let task = self.inner.runtime.spawn(outbound_pump(
                    run_id.clone(),
                    Arc::clone(&registration.queue),
                    Arc::clone(&self.inner.transport),
                    Arc::clone(&self.inner.stats),
                    self.inner.options.max_frame_bytes,
                ));
                lock(&self.inner.tasks).push(task);
                registration
            }
        };
        if let Some(item) = item {
            registration.begin_item(item);
        }
        Some(registration)
    }
}

impl fmt::Debug for NatsCoreLiveRunStreamBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsCoreLiveRunStreamBroker")
            .field("namespace", &self.inner.options.namespace)
            .field("accepting", &self.is_accepting())
            .field("connection_state", &self.inner.client.connection_state())
            .field("outbound_runs", &lock(&self.inner.outbound).len())
            .field("inbound_runs", &lock(&self.inner.inbound).len())
            .finish()
    }
}

#[async_trait]
impl LiveRunStreamBroker for NatsCoreLiveRunStreamBroker {
    fn deployment_capability(&self) -> LiveRunStreamBrokerCapability {
        LiveRunStreamBrokerCapability::Shared
    }

    fn prometheus_metrics(&self) -> String {
        let active_tasks = lock(&self.inner.tasks)
            .iter()
            .filter(|task| !task.is_finished())
            .count()
            + usize::from(
                lock(&self.inner.publisher_task)
                    .as_ref()
                    .is_some_and(|task| !task.is_finished()),
            );
        self.inner.stats.prometheus_metrics(
            self.is_accepting(),
            self.inner.client.connection_state(),
            lock(&self.inner.inbound).len(),
            active_tasks,
            self.inner.transport.snapshot(),
        )
    }

    fn record_terminal_barrier(
        &self,
        outcome: LiveRunStreamTerminalBarrierOutcome,
        duration: Duration,
    ) {
        self.inner.stats.record_terminal_barrier(outcome, duration);
    }

    async fn check_readiness(
        &self,
        readiness_timeout: Duration,
    ) -> Result<(), LiveRunStreamBrokerError> {
        NatsCoreLiveRunStreamBroker::check_readiness(self, readiness_timeout).await
    }

    async fn shutdown(&self, grace: Duration) -> Result<(), LiveRunStreamBrokerError> {
        NatsCoreLiveRunStreamBroker::shutdown(self, grace).await
    }

    async fn subscribe(
        &self,
        run_id: RunId,
    ) -> Result<Box<dyn LiveRunStreamSubscriber>, LiveRunStreamBrokerError> {
        if self
            .inner
            .stats
            .authorization_failed
            .load(Ordering::Acquire)
        {
            return Err(auth_failed());
        }
        if !self.is_accepting() || self.inner.client.connection_state() != State::Connected {
            return Err(not_ready());
        }
        if lock(&self.inner.inbound).contains_key(&run_id) {
            return Err(subscriber_exists());
        }
        let subject = nats_live_run_stream_subject(&self.inner.options.namespace, &run_id)?;
        let started = Instant::now();
        let subscribe = async {
            let subscriber = self.inner.client.subscribe(subject).await.map_err(|_| ())?;
            self.inner.client.flush().await.map_err(|_| ())?;
            if self
                .inner
                .stats
                .authorization_failed
                .load(Ordering::Acquire)
            {
                return Err(());
            }
            Ok::<_, ()>(subscriber)
        };
        let mut subscriber =
            time::timeout(self.inner.options.subscription_ready_timeout, subscribe)
                .await
                .map_err(|_| not_ready())?
                .map_err(|_| {
                    if self
                        .inner
                        .stats
                        .authorization_failed
                        .load(Ordering::Acquire)
                    {
                        auth_failed()
                    } else {
                        not_ready()
                    }
                })?;
        self.inner
            .stats
            .record_subscription_ready(started.elapsed());

        let registration = Arc::new(InboundRegistration {
            queue: Arc::new(RunQueue::new_with_limits(
                run_id.clone(),
                self.inner.options.body_queue_capacity,
                self.inner.options.control_queue_capacity,
                self.inner.options.byte_limits(),
            )),
            cancel: CancellationToken::new(),
        });
        let registration_error = {
            let mut registrations = lock(&self.inner.inbound);
            if !self.is_accepting() {
                Some(not_ready())
            } else {
                match registrations.entry(run_id.clone()) {
                    Entry::Occupied(_) => Some(subscriber_exists()),
                    Entry::Vacant(entry) => {
                        entry.insert(Arc::clone(&registration));
                        None
                    }
                }
            }
        };
        if let Some(error) = registration_error {
            let _ = subscriber.unsubscribe().await;
            return Err(error);
        }
        {
            let task = self.inner.runtime.spawn(inbound_pump(
                subscriber,
                run_id.clone(),
                Arc::clone(&registration),
                Arc::clone(&self.inner.stats),
                self.inner.options.max_frame_bytes,
            ));
            lock(&self.inner.tasks).push(task);
        }
        Ok(Box::new(NatsCoreLiveRunStreamSubscriber {
            run_id,
            registration,
            owner: Arc::downgrade(&self.inner),
        }))
    }

    fn publish(&self, publication: LiveRunStreamPublication) -> LiveRunStreamPublishOutcome {
        let class = publication_class(&publication);
        let run_id = publication.run_id().clone();
        let Some(registration) =
            self.outbound_registration(&run_id, publication.output_item_identity())
        else {
            return LiveRunStreamPublishOutcome::RunClosed;
        };
        let outcome = registration.queue.publish(publication);
        self.inner.stats.record_local_outcome(class, outcome);
        outcome
    }

    fn seal(&self, seal: LiveRunStreamSeal) -> LiveRunStreamPublishOutcome {
        let run_id = seal.identity().run_id().clone();
        let identity = seal.identity().clone();
        let Some(registration) = self.outbound_registration(&run_id, None) else {
            return LiveRunStreamPublishOutcome::RunClosed;
        };
        let outcome = registration.queue.seal(seal);
        // Keep the per-Run queue until explicit close or bounded idle reap. A
        // seal closes one output item, not the Run, and retaining the queue
        // preserves rejection of late frames and ordering with later live
        // tool/retrieval observations.
        let _ = registration.finish_item(&identity);
        if outcome == LiveRunStreamPublishOutcome::ControlQueueFull {
            self.inner.stats.record_drop("seal", "control_full");
        }
        outcome
    }

    fn close_run(&self, run_id: &RunId) -> LiveRunStreamCloseOutcome {
        let outbound = lock(&self.inner.outbound).remove(run_id);
        let inbound = lock(&self.inner.inbound).remove(run_id);
        let mut outcome = LiveRunStreamCloseOutcome::default();
        if let Some(registration) = outbound {
            outcome = registration.queue.close();
        }
        if let Some(registration) = inbound {
            registration.cancel.cancel();
            let _ = registration.queue.close();
        }
        outcome
    }
}

struct NatsCoreLiveRunStreamSubscriber {
    run_id: RunId,
    registration: Arc<InboundRegistration>,
    owner: Weak<NatsBrokerInner>,
}

#[async_trait]
impl LiveRunStreamSubscriber for NatsCoreLiveRunStreamSubscriber {
    fn run_id(&self) -> &RunId {
        &self.run_id
    }

    async fn recv(&mut self) -> Result<LiveRunStreamDelivery, LiveRunStreamBrokerError> {
        self.registration.queue.recv().await
    }
}

impl Drop for NatsCoreLiveRunStreamSubscriber {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let removed = {
            let mut registrations = lock(&owner.inbound);
            if registrations
                .get(&self.run_id)
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &self.registration))
            {
                registrations.remove(&self.run_id)
            } else {
                None
            }
        };
        if let Some(registration) = removed {
            registration.cancel.cancel();
            registration.queue.close();
        }
    }
}

#[derive(Clone, Copy)]
enum TransportClass {
    Output,
    Tool,
    Retrieval,
    Gap,
    Seal,
}

impl TransportClass {
    const fn label(self) -> &'static str {
        match self {
            Self::Output => "output",
            Self::Tool => "tool",
            Self::Retrieval => "retrieval",
            Self::Gap => "gap",
            Self::Seal => "seal",
        }
    }

    const fn is_body(self) -> bool {
        matches!(self, Self::Output | Self::Tool | Self::Retrieval)
    }
}

struct TransportMessage {
    run_id: RunId,
    payload: Vec<u8>,
    class: TransportClass,
    enqueued_at: Instant,
}

struct TransportQueue {
    max_messages: usize,
    max_bytes: usize,
    body_message_limit: usize,
    body_byte_limit: usize,
    state: Mutex<TransportQueueState>,
    notify: tokio::sync::Notify,
}

#[derive(Default)]
struct TransportQueueState {
    queues: BTreeMap<RunId, VecDeque<TransportMessage>>,
    ready_runs: VecDeque<RunId>,
    pending_messages: usize,
    pending_bytes: usize,
    pending_body_messages: usize,
    pending_body_bytes: usize,
    closed: bool,
}

#[derive(Clone, Copy)]
struct TransportQueueSnapshot {
    messages: usize,
    bytes: usize,
    body_messages: usize,
    body_bytes: usize,
}

impl TransportQueue {
    fn new(max_messages: usize, max_bytes: usize, max_frame_bytes: usize) -> Self {
        let control_message_reserve = (max_messages / 8).max(1).min(max_messages);
        let control_byte_reserve = max_frame_bytes.min(max_bytes);
        Self {
            max_messages,
            max_bytes,
            body_message_limit: max_messages.saturating_sub(control_message_reserve),
            body_byte_limit: max_bytes.saturating_sub(control_byte_reserve),
            state: Mutex::new(TransportQueueState::default()),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn try_enqueue_body(&self, message: TransportMessage) -> bool {
        let size = message.payload.len();
        let mut state = lock(&self.state);
        if state.closed
            || size > self.body_byte_limit
            || state.pending_messages >= self.max_messages
            || state.pending_bytes.saturating_add(size) > self.max_bytes
            || state.pending_body_messages >= self.body_message_limit
            || state.pending_body_bytes.saturating_add(size) > self.body_byte_limit
        {
            return false;
        }
        state.pending_body_messages += 1;
        state.pending_body_bytes += size;
        Self::enqueue(&mut state, message, size);
        drop(state);
        self.notify.notify_one();
        true
    }

    fn try_enqueue_control(&self, message: TransportMessage) -> bool {
        let size = message.payload.len();
        let mut state = lock(&self.state);
        if state.closed
            || size > self.max_bytes
            || state.pending_messages >= self.max_messages
            || state.pending_bytes.saturating_add(size) > self.max_bytes
        {
            return false;
        }
        Self::enqueue(&mut state, message, size);
        drop(state);
        self.notify.notify_one();
        true
    }

    fn enqueue(state: &mut TransportQueueState, message: TransportMessage, size: usize) {
        let run_id = message.run_id.clone();
        let queue = state.queues.entry(run_id.clone()).or_default();
        let was_empty = queue.is_empty();
        queue.push_back(message);
        if was_empty {
            state.ready_runs.push_back(run_id);
        }
        state.pending_messages += 1;
        state.pending_bytes += size;
    }

    async fn recv(&self) -> Option<TransportMessage> {
        loop {
            let notified = self.notify.notified();
            {
                let mut state = lock(&self.state);
                while let Some(run_id) = state.ready_runs.pop_front() {
                    let mut remove_run = false;
                    let message = if let Some(queue) = state.queues.get_mut(&run_id) {
                        let message = queue.pop_front();
                        remove_run = queue.is_empty();
                        message
                    } else {
                        None
                    };
                    if remove_run {
                        state.queues.remove(&run_id);
                    } else if message.is_some() {
                        state.ready_runs.push_back(run_id);
                    }
                    if message.is_some() {
                        return message;
                    }
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }

    fn finish(&self, bytes: usize, class: TransportClass) {
        let mut state = lock(&self.state);
        state.pending_messages = state.pending_messages.saturating_sub(1);
        state.pending_bytes = state.pending_bytes.saturating_sub(bytes);
        if class.is_body() {
            state.pending_body_messages = state.pending_body_messages.saturating_sub(1);
            state.pending_body_bytes = state.pending_body_bytes.saturating_sub(bytes);
        }
    }

    fn close(&self) {
        lock(&self.state).closed = true;
        self.notify.notify_waiters();
    }

    fn snapshot(&self) -> TransportQueueSnapshot {
        let state = lock(&self.state);
        TransportQueueSnapshot {
            messages: state.pending_messages,
            bytes: state.pending_bytes,
            body_messages: state.pending_body_messages,
            body_bytes: state.pending_body_bytes,
        }
    }
}

async fn outbound_pump(
    run_id: RunId,
    queue: Arc<RunQueue>,
    transport: Arc<TransportQueue>,
    stats: Arc<NatsRunStreamStats>,
    max_frame_bytes: usize,
) {
    while let Ok(delivery) = queue.recv().await {
        match delivery {
            LiveRunStreamDelivery::Publication(publication) => {
                let class = publication_class(&publication);
                let fallback = publication.output_item_identity().cloned().map(|identity| {
                    LiveRunStreamGap::known(
                        identity,
                        publication.local_sequence(),
                        publication.local_sequence(),
                    )
                    .expect("one discarded output sequence is a valid gap")
                });
                match encode_publication(&publication, max_frame_bytes) {
                    Ok(encoded) => {
                        let message = TransportMessage {
                            run_id: run_id.clone(),
                            payload: encoded.into_bytes(),
                            class,
                            enqueued_at: Instant::now(),
                        };
                        if !transport.try_enqueue_body(message) {
                            stats.record_drop(class.label(), "pending_full");
                            if let Some(gap) = fallback {
                                enqueue_fallback_gap(
                                    &transport,
                                    &stats,
                                    &run_id,
                                    &gap,
                                    max_frame_bytes,
                                );
                            }
                        }
                    }
                    Err(_) => {
                        stats.record_drop(class.label(), "oversize");
                        if let Some(gap) = fallback {
                            enqueue_fallback_gap(
                                &transport,
                                &stats,
                                &run_id,
                                &gap,
                                max_frame_bytes,
                            );
                        }
                    }
                }
            }
            LiveRunStreamDelivery::Gap(gap) => {
                enqueue_fallback_gap(&transport, &stats, &run_id, &gap, max_frame_bytes);
            }
            LiveRunStreamDelivery::Seal(seal) => match encode_seal(&seal, max_frame_bytes) {
                Ok(encoded) => {
                    let message = TransportMessage {
                        run_id: run_id.clone(),
                        payload: encoded.into_bytes(),
                        class: TransportClass::Seal,
                        enqueued_at: Instant::now(),
                    };
                    if !transport.try_enqueue_control(message) {
                        stats.record_drop("seal", "pending_full");
                        let gap = LiveRunStreamGap::unknown_tail(seal.identity().clone(), 0);
                        enqueue_fallback_gap(&transport, &stats, &run_id, &gap, max_frame_bytes);
                    }
                }
                Err(_) => {
                    stats.record_drop("seal", "oversize");
                    let gap = LiveRunStreamGap::unknown_tail(seal.identity().clone(), 0);
                    enqueue_fallback_gap(&transport, &stats, &run_id, &gap, max_frame_bytes);
                }
            },
        }
        tokio::task::yield_now().await;
    }
}

fn enqueue_fallback_gap(
    transport: &TransportQueue,
    stats: &NatsRunStreamStats,
    run_id: &RunId,
    gap: &LiveRunStreamGap,
    max_frame_bytes: usize,
) {
    let Ok(encoded) = encode_gap(gap, max_frame_bytes) else {
        stats.record_drop("gap", "oversize");
        return;
    };
    let message = TransportMessage {
        run_id: run_id.clone(),
        payload: encoded.into_bytes(),
        class: TransportClass::Gap,
        enqueued_at: Instant::now(),
    };
    if transport.try_enqueue_control(message) {
        stats.record_gap(if gap.has_unknown_tail() {
            "unknown_tail"
        } else {
            "known"
        });
    } else {
        stats.record_drop("gap", "control_full");
    }
}

async fn nats_publisher(
    client: Client,
    namespace: String,
    transport: Arc<TransportQueue>,
    stats: Arc<NatsRunStreamStats>,
) {
    while let Some(message) = transport.recv().await {
        let size = message.payload.len();
        // Do not hand disconnected publications to async-nats' reconnect
        // buffer. Core NATS is intentionally live-only: frames observed while
        // disconnected are loss evidence, not deferred replay after reconnect.
        let outcome = if client.connection_state() == State::Connected {
            match nats_live_run_stream_subject(&namespace, &message.run_id) {
                Ok(subject) => client
                    .publish(subject, message.payload.into())
                    .await
                    .is_ok(),
                Err(_) => false,
            }
        } else {
            false
        };
        stats.record_publish(
            message.class.label(),
            if outcome { "published" } else { "error" },
            message.enqueued_at.elapsed(),
        );
        transport.finish(size, message.class);
    }
}

async fn inbound_pump(
    mut subscriber: Subscriber,
    run_id: RunId,
    registration: Arc<InboundRegistration>,
    stats: Arc<NatsRunStreamStats>,
    max_frame_bytes: usize,
) {
    loop {
        let message = tokio::select! {
            _ = registration.cancel.cancelled() => break,
            message = subscriber.next() => message,
        };
        let Some(message) = message else {
            break;
        };
        let Ok(encoded) = std::str::from_utf8(&message.payload) else {
            stats.record_decode_error("utf8");
            continue;
        };
        let Ok(delivery) = decode(encoded, &run_id, max_frame_bytes) else {
            stats.record_decode_error("wire");
            continue;
        };
        let outcome = match delivery {
            LiveRunStreamDelivery::Publication(publication) => {
                registration.queue.publish(publication)
            }
            LiveRunStreamDelivery::Gap(gap) => registration.queue.accept_gap(gap),
            LiveRunStreamDelivery::Seal(seal) => registration.queue.seal(seal),
        };
        if matches!(
            outcome,
            LiveRunStreamPublishOutcome::DroppedWithGap
                | LiveRunStreamPublishOutcome::DroppedBestEffort
                | LiveRunStreamPublishOutcome::ControlQueueFull
        ) {
            stats.record_drop("output", "inbound_queue_full");
        }
    }
    let _ = subscriber.unsubscribe().await;
    registration.queue.close();
}

fn publication_class(publication: &LiveRunStreamPublication) -> TransportClass {
    if publication.output_item_identity().is_some() {
        return TransportClass::Output;
    }
    if publication.payload_type() == RunStreamEventType::RunRetrievalCompleted {
        TransportClass::Retrieval
    } else {
        TransportClass::Tool
    }
}

async fn outbound_maintenance(owner: Weak<NatsBrokerInner>) {
    loop {
        let Some(inner) = owner.upgrade() else {
            break;
        };
        let cancel = inner.maintenance_cancel.clone();
        let interval = outbound_reaper_interval(inner.options.outbound_idle_timeout);
        drop(inner);
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = time::sleep(interval) => {}
        }
        let Some(inner) = owner.upgrade() else {
            break;
        };
        if !inner.accepting.load(Ordering::Acquire) {
            break;
        }
        reap_idle_outbound(&inner, Instant::now());
        lock(&inner.tasks).retain(|task| !task.is_finished());
    }
}

fn outbound_reaper_interval(idle_timeout: Duration) -> Duration {
    (idle_timeout / 2)
        .max(Duration::from_millis(1))
        .min(MAX_OUTBOUND_REAPER_INTERVAL)
}

fn reap_idle_outbound(inner: &NatsBrokerInner, now: Instant) -> usize {
    let expired = {
        let mut registrations = lock(&inner.outbound);
        take_idle_outbound(&mut registrations, now, inner.options.outbound_idle_timeout)
    };
    let count = expired.len();
    for registration in expired {
        registration.queue.close();
    }
    count
}

fn take_idle_outbound(
    registrations: &mut BTreeMap<RunId, Arc<OutboundRegistration>>,
    now: Instant,
    idle_timeout: Duration,
) -> Vec<Arc<OutboundRegistration>> {
    let expired = registrations
        .iter()
        .filter(|(_, registration)| registration.is_idle_at(now, idle_timeout))
        .map(|(run_id, _)| run_id.clone())
        .collect::<Vec<_>>();
    expired
        .into_iter()
        .filter_map(|run_id| registrations.remove(&run_id))
        .collect()
}

#[derive(Default)]
struct NatsRunStreamStats {
    connected: AtomicBool,
    ever_connected: AtomicBool,
    authorization_failed: AtomicBool,
    reconnects: AtomicU64,
    slow_consumers: AtomicU64,
    subscription_ready_count: AtomicU64,
    subscription_ready_nanos: AtomicU64,
    publish_latency_count: AtomicU64,
    publish_latency_nanos: AtomicU64,
    counters: Mutex<BTreeMap<(&'static str, &'static str, &'static str), u64>>,
    #[cfg(test)]
    server_errors: AtomicU64,
}

impl NatsRunStreamStats {
    fn record_local_outcome(&self, class: TransportClass, outcome: LiveRunStreamPublishOutcome) {
        let reason = match outcome {
            LiveRunStreamPublishOutcome::DroppedWithGap => Some("producer_queue_full"),
            LiveRunStreamPublishOutcome::DroppedOversizeWithGap => Some("oversize"),
            LiveRunStreamPublishOutcome::DroppedBestEffort => Some("producer_queue_full"),
            LiveRunStreamPublishOutcome::ControlQueueFull => Some("control_full"),
            _ => None,
        };
        if let Some(reason) = reason {
            self.record_drop(class.label(), reason);
        }
    }

    fn record_connection_event(&self, event: Event) {
        #[cfg(test)]
        if matches!(&event, Event::ServerError(_)) {
            self.server_errors.fetch_add(1, Ordering::Relaxed);
        }
        match event {
            Event::Connected => {
                if self.ever_connected.swap(true, Ordering::AcqRel) {
                    self.reconnects.fetch_add(1, Ordering::Relaxed);
                }
                self.connected.store(true, Ordering::Release);
            }
            Event::Disconnected | Event::Closed => {
                self.connected.store(false, Ordering::Release);
            }
            Event::SlowConsumer(_) => {
                self.slow_consumers.fetch_add(1, Ordering::Relaxed);
            }
            Event::ServerError(ServerError::AuthorizationViolation) => {
                self.authorization_failed.store(true, Ordering::Release);
            }
            Event::ServerError(ServerError::Other(message)) => {
                // nats-server uses distinct text for publish and subscription
                // permission failures. Classify only the stable violation
                // token and discard the rest because it can contain a raw
                // subject.
                if message.to_ascii_lowercase().contains("violation") {
                    self.authorization_failed.store(true, Ordering::Release);
                }
            }
            Event::ServerError(ServerError::SlowConsumer(_)) => {
                self.slow_consumers.fetch_add(1, Ordering::Relaxed);
            }
            Event::LameDuckMode | Event::Draining | Event::ClientError(_) => {}
        }
    }

    fn record_publish(&self, class: &'static str, outcome: &'static str, latency: Duration) {
        *lock(&self.counters)
            .entry(("publish", class, outcome))
            .or_default() += 1;
        self.publish_latency_count.fetch_add(1, Ordering::Relaxed);
        self.publish_latency_nanos.fetch_add(
            latency.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn record_drop(&self, class: &'static str, reason: &'static str) {
        *lock(&self.counters)
            .entry(("drop", class, reason))
            .or_default() += 1;
    }

    fn record_gap(&self, kind: &'static str) {
        *lock(&self.counters)
            .entry(("gap", kind, "transport"))
            .or_default() += 1;
    }

    fn record_decode_error(&self, reason: &'static str) {
        *lock(&self.counters)
            .entry(("decode", "message", reason))
            .or_default() += 1;
    }

    fn record_subscription_ready(&self, latency: Duration) {
        self.subscription_ready_count
            .fetch_add(1, Ordering::Relaxed);
        self.subscription_ready_nanos.fetch_add(
            latency.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn record_terminal_barrier(
        &self,
        outcome: LiveRunStreamTerminalBarrierOutcome,
        duration: Duration,
    ) {
        let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        let mut counters = lock(&self.counters);
        *counters
            .entry(("barrier_count", outcome.label(), "observed"))
            .or_default() += 1;
        let total = counters
            .entry(("barrier_nanos", outcome.label(), "observed"))
            .or_default();
        *total = total.saturating_add(nanos);
    }

    fn prometheus_metrics(
        &self,
        accepting: bool,
        state: State,
        active_subscriptions: usize,
        active_tasks: usize,
        pending: TransportQueueSnapshot,
    ) -> String {
        let mut output = String::new();
        let ready = accepting && state == State::Connected;
        let _ = writeln!(
            output,
            "run_stream_bus_ready{{backend=\"nats_core\"}} {}",
            u8::from(ready)
        );
        for candidate in [State::Pending, State::Connected, State::Disconnected] {
            let _ = writeln!(
                output,
                "run_stream_bus_connections{{backend=\"nats_core\",state=\"{}\"}} {}",
                candidate,
                u8::from(state == candidate)
            );
        }
        let _ = writeln!(
            output,
            "run_stream_bus_reconnect_total{{backend=\"nats_core\",outcome=\"connected\"}} {}",
            self.reconnects.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "run_stream_bus_active_subscriptions{{backend=\"nats_core\"}} {active_subscriptions}"
        );
        let _ = writeln!(
            output,
            "run_stream_bus_tasks{{backend=\"nats_core\",state=\"active\"}} {active_tasks}"
        );
        for (class, messages, bytes) in [
            ("all", pending.messages, pending.bytes),
            ("body", pending.body_messages, pending.body_bytes),
            (
                "control",
                pending.messages.saturating_sub(pending.body_messages),
                pending.bytes.saturating_sub(pending.body_bytes),
            ),
        ] {
            let _ = writeln!(
                output,
                "run_stream_bus_pending_messages{{backend=\"nats_core\",queue_class=\"{class}\"}} {messages}"
            );
            let _ = writeln!(
                output,
                "run_stream_bus_pending_bytes{{backend=\"nats_core\",queue_class=\"{class}\"}} {bytes}"
            );
        }
        let _ = writeln!(
            output,
            "run_stream_bus_slow_consumer_total{{backend=\"nats_core\",scope=\"client\"}} {}",
            self.slow_consumers.load(Ordering::Relaxed)
        );
        let publish_count = self.publish_latency_count.load(Ordering::Relaxed);
        let publish_nanos = self.publish_latency_nanos.load(Ordering::Relaxed);
        let _ = writeln!(
            output,
            "run_stream_bus_publish_latency_seconds_count{{backend=\"nats_core\"}} {publish_count}"
        );
        let _ = writeln!(
            output,
            "run_stream_bus_publish_latency_seconds_sum{{backend=\"nats_core\"}} {:.9}",
            publish_nanos as f64 / 1_000_000_000.0
        );
        let subscription_count = self.subscription_ready_count.load(Ordering::Relaxed);
        let subscription_nanos = self.subscription_ready_nanos.load(Ordering::Relaxed);
        let _ = writeln!(
            output,
            "run_stream_bus_subscription_ready_seconds_count{{backend=\"nats_core\"}} {subscription_count}"
        );
        let _ = writeln!(
            output,
            "run_stream_bus_subscription_ready_seconds_sum{{backend=\"nats_core\"}} {:.9}",
            subscription_nanos as f64 / 1_000_000_000.0
        );
        let counters = lock(&self.counters);
        for outcome in LiveRunStreamTerminalBarrierOutcome::ALL {
            let label = outcome.label();
            let count = counters
                .get(&("barrier_count", label, "observed"))
                .copied()
                .unwrap_or_default();
            let nanos = counters
                .get(&("barrier_nanos", label, "observed"))
                .copied()
                .unwrap_or_default();
            let _ = writeln!(
                output,
                "run_stream_bus_terminal_barrier_seconds_count{{backend=\"nats_core\",outcome=\"{label}\"}} {count}"
            );
            let _ = writeln!(
                output,
                "run_stream_bus_terminal_barrier_seconds_sum{{backend=\"nats_core\",outcome=\"{label}\"}} {:.9}",
                nanos as f64 / 1_000_000_000.0
            );
        }
        for (family, zero_sample) in [
            (
                "publish",
                "run_stream_bus_publish_total{backend=\"nats_core\",event_class=\"output\",outcome=\"published\"} 0\n",
            ),
            (
                "drop",
                "run_stream_bus_dropped_total{backend=\"nats_core\",event_class=\"output\",reason=\"producer_queue_full\"} 0\n",
            ),
            (
                "gap",
                "run_stream_bus_gap_total{backend=\"nats_core\",gap_kind=\"known\",reason=\"transport\"} 0\n",
            ),
            (
                "decode",
                "run_stream_bus_decode_error_total{backend=\"nats_core\",reason=\"wire\"} 0\n",
            ),
        ] {
            if !counters.keys().any(|(candidate, _, _)| *candidate == family) {
                output.push_str(zero_sample);
            }
        }
        for ((family, class, outcome), count) in counters.iter() {
            match *family {
                "publish" => {
                    let _ = writeln!(
                        output,
                        "run_stream_bus_publish_total{{backend=\"nats_core\",event_class=\"{class}\",outcome=\"{outcome}\"}} {count}"
                    );
                }
                "drop" => {
                    let _ = writeln!(
                        output,
                        "run_stream_bus_dropped_total{{backend=\"nats_core\",event_class=\"{class}\",reason=\"{outcome}\"}} {count}"
                    );
                }
                "gap" => {
                    let _ = writeln!(
                        output,
                        "run_stream_bus_gap_total{{backend=\"nats_core\",gap_kind=\"{class}\",reason=\"{outcome}\"}} {count}"
                    );
                }
                "decode" => {
                    let _ = writeln!(
                        output,
                        "run_stream_bus_decode_error_total{{backend=\"nats_core\",reason=\"{outcome}\"}} {count}"
                    );
                }
                "barrier_count" | "barrier_nanos" => {}
                _ => {}
            }
        }
        output
    }
}

fn reconnect_delay(attempt: usize, min: Duration, max: Duration) -> Duration {
    let min_millis = min.as_millis().max(1).min(u128::from(u64::MAX)) as u64;
    let max_millis = max.as_millis().max(1).min(u128::from(u64::MAX)) as u64;
    let multiplier = 1u64 << attempt.min(16);
    let base = min_millis.saturating_mul(multiplier).min(max_millis);
    let jitter_span = (base / 5).max(1);
    let seed = (attempt as u64).wrapping_mul(6_364_136_223_846_793_005);
    let width = jitter_span.saturating_mul(2).saturating_add(1);
    let offset = seed % width;
    let jittered = base
        .saturating_sub(jitter_span)
        .saturating_add(offset)
        .min(max_millis);
    Duration::from_millis(jittered.max(1))
}

async fn wait_for_tasks(mut tasks: Vec<JoinHandle<()>>, grace: Duration) -> bool {
    let completed = time::timeout(grace, async {
        for task in &mut tasks {
            let _ = task.await;
        }
    })
    .await
    .is_ok();
    if completed {
        return true;
    }
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
    false
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

fn config_invalid() -> LiveRunStreamBrokerError {
    LiveRunStreamBrokerError::new(
        RUN_STREAM_BUS_CONFIG_INVALID,
        "NATS Core live Run stream configuration is invalid",
    )
}

fn unavailable() -> LiveRunStreamBrokerError {
    LiveRunStreamBrokerError::new(
        RUN_STREAM_BUS_UNAVAILABLE,
        "NATS Core live Run stream bus is unavailable",
    )
}

fn map_connect_error(error: ConnectError) -> LiveRunStreamBrokerError {
    if matches!(
        error.kind(),
        ConnectErrorKind::Authentication | ConnectErrorKind::AuthorizationViolation
    ) {
        auth_failed()
    } else {
        unavailable()
    }
}

fn auth_failed() -> LiveRunStreamBrokerError {
    LiveRunStreamBrokerError::new(
        RUN_STREAM_BUS_AUTH_FAILED,
        "NATS Core live Run stream authentication or authorization failed",
    )
}

fn not_ready() -> LiveRunStreamBrokerError {
    LiveRunStreamBrokerError::new(
        RUN_STREAM_BUS_NOT_READY,
        "NATS Core live Run stream bus is not ready",
    )
}

fn subscriber_exists() -> LiveRunStreamBrokerError {
    LiveRunStreamBrokerError::new(
        RUN_STREAM_BUS_SUBSCRIBER_EXISTS,
        "a live Run stream subscriber already exists for this Run",
    )
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_engine::{run_stream::LiveRunObservationIdentity, ActivationId, AttemptNo};

    fn run(value: &str) -> RunId {
        RunId::new(value).unwrap()
    }

    fn identity(run_id: RunId) -> LiveRunStreamItemIdentity {
        LiveRunStreamItemIdentity::new(
            run_id,
            ActivationId::new("activation_nats_test").unwrap(),
            AttemptNo::FIRST,
            1,
            "item_nats_test",
            0,
        )
        .unwrap()
    }

    fn publication(
        identity: LiveRunStreamItemIdentity,
        sequence: u64,
        delta: &str,
    ) -> LiveRunStreamPublication {
        LiveRunStreamPublication::new(
            identity,
            sequence,
            insight_engine::run_stream::LiveRunStreamPayload::OutputTextDelta {
                content_index: 0,
                delta: delta.to_owned(),
            },
        )
        .unwrap()
    }

    fn integration_options(server: String, namespace: &str) -> NatsCoreLiveRunStreamBrokerOptions {
        NatsCoreLiveRunStreamBrokerOptions {
            servers: vec![server],
            namespace: namespace.to_owned(),
            credentials: None,
            tls: NatsCoreTlsOptions {
                required: false,
                root_certificates: Vec::new(),
                client_certificate: None,
                client_private_key: None,
            },
            connect_timeout: Duration::from_secs(5),
            subscription_ready_timeout: Duration::from_secs(5),
            reconnect_min_delay: Duration::from_millis(50),
            reconnect_max_delay: Duration::from_millis(500),
            max_pending_messages: 512,
            max_pending_bytes: 8 * 1_024 * 1_024,
            body_queue_capacity: 128,
            control_queue_capacity: 32,
            max_frame_bytes: 64 * 1_024,
            max_item_bytes: 4 * 1_024 * 1_024,
            max_run_bytes: 16 * 1_024 * 1_024,
            drain_timeout: Duration::from_secs(5),
            outbound_idle_timeout: Duration::from_secs(60),
        }
    }

    #[test]
    fn subject_is_stable_full_length_and_body_free() {
        let first = nats_live_run_stream_subject("prod_cn1", &run("run_private_a")).unwrap();
        let second = nats_live_run_stream_subject("prod_cn1", &run("run_private_b")).unwrap();
        assert!(first.starts_with("insight.prod_cn1.run_stream.v1."));
        assert_eq!(first.rsplit('.').next().unwrap().len(), 64);
        assert_ne!(first, second);
        assert!(!first.contains("run_private_a"));
    }

    #[test]
    fn options_debug_redacts_credentials() {
        let options = test_options();
        let debug = format!("{options:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("PRIVATE_NKEY_SEED"));
    }

    #[test]
    fn transport_queue_reserves_control_and_round_robins_runs() {
        let queue = TransportQueue::new(8, 8 * 1_024, 1_024);
        for (run_id, byte) in [("run_a", 1), ("run_a", 2), ("run_b", 3)] {
            assert!(queue.try_enqueue_body(TransportMessage {
                run_id: run(run_id),
                payload: vec![byte; 8],
                class: TransportClass::Output,
                enqueued_at: Instant::now(),
            }));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let first = queue.recv().await.unwrap();
            queue.finish(first.payload.len(), first.class);
            let second = queue.recv().await.unwrap();
            queue.finish(second.payload.len(), second.class);
            assert_eq!(first.run_id.as_str(), "run_a");
            assert_eq!(second.run_id.as_str(), "run_b");
        });
    }

    #[test]
    fn transport_queue_preserves_per_run_order_and_control_reserve() {
        let queue = TransportQueue::new(8, 8 * 1_024, 1_024);
        for index in 0..7 {
            assert!(queue.try_enqueue_body(TransportMessage {
                run_id: run("run_ordered"),
                payload: vec![index; 8],
                class: TransportClass::Output,
                enqueued_at: Instant::now(),
            }));
        }
        assert!(!queue.try_enqueue_body(TransportMessage {
            run_id: run("run_other"),
            payload: vec![8; 8],
            class: TransportClass::Output,
            enqueued_at: Instant::now(),
        }));
        assert!(queue.try_enqueue_control(TransportMessage {
            run_id: run("run_ordered"),
            payload: vec![9; 8],
            class: TransportClass::Seal,
            enqueued_at: Instant::now(),
        }));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            for expected in 0..7 {
                let message = queue.recv().await.unwrap();
                assert_eq!(message.payload[0], expected);
                queue.finish(message.payload.len(), message.class);
            }
            let seal = queue.recv().await.unwrap();
            assert_eq!(seal.payload[0], 9);
            assert!(matches!(seal.class, TransportClass::Seal));
            queue.finish(seal.payload.len(), seal.class);
        });
    }

    #[test]
    fn reconnect_delay_is_bounded_and_jittered() {
        let min = Duration::from_millis(100);
        let max = Duration::from_secs(5);
        for attempt in 0..100 {
            let delay = reconnect_delay(attempt, min, max);
            assert!(delay >= Duration::from_millis(1));
            assert!(delay <= max);
        }
    }

    #[tokio::test]
    async fn backend_contract_runs_for_in_memory() {
        let broker = Arc::new(crate::run_stream::InMemoryLiveRunStreamBroker::new(8, 8).unwrap())
            as Arc<dyn LiveRunStreamBroker>;
        exercise_backend_contract(broker).await;
    }

    #[tokio::test]
    async fn backend_contract_runs_for_real_nats_when_configured() {
        let Ok(server) = std::env::var("TEST_NATS_URL") else {
            return;
        };
        let namespace = format!("test_{}", uuid::Uuid::new_v4().simple());
        let mut options = integration_options(server, &namespace);
        options.body_queue_capacity = 8;
        options.control_queue_capacity = 8;
        let concrete = NatsCoreLiveRunStreamBroker::connect(options).await.unwrap();
        let broker = Arc::new(concrete.clone()) as Arc<dyn LiveRunStreamBroker>;
        exercise_backend_contract(broker).await;
        concrete.shutdown(Duration::from_secs(5)).await.unwrap();
    }

    #[tokio::test]
    async fn real_nats_cross_runtime_fanout_and_shared_connection_when_configured() {
        let Ok(server) = std::env::var("TEST_NATS_URL") else {
            return;
        };
        let namespace = format!("test_{}", uuid::Uuid::new_v4().simple());
        let publisher =
            NatsCoreLiveRunStreamBroker::connect(integration_options(server.clone(), &namespace))
                .await
                .unwrap();
        let subscriber_a =
            NatsCoreLiveRunStreamBroker::connect(integration_options(server.clone(), &namespace))
                .await
                .unwrap();
        let subscriber_b =
            NatsCoreLiveRunStreamBroker::connect(integration_options(server, &namespace))
                .await
                .unwrap();
        let run_id = RunId::random();
        let item = identity(run_id.clone());
        let mut self_subscription = publisher.subscribe(run_id.clone()).await.unwrap();
        let mut a = subscriber_a.subscribe(run_id.clone()).await.unwrap();
        let mut b = subscriber_b.subscribe(run_id.clone()).await.unwrap();

        let subject = nats_live_run_stream_subject(&namespace, &run_id).unwrap();
        let wrong_run = RunId::random();
        let wrong_wire = encode_publication(
            &publication(identity(wrong_run), 0, "wrong-run"),
            64 * 1_024,
        )
        .unwrap();
        let valid_wire =
            encode_publication(&publication(item.clone(), 0, "future"), 64 * 1_024).unwrap();
        let mut future_wire: serde_json::Value = serde_json::from_str(&valid_wire).unwrap();
        future_wire["schema_version"] = serde_json::json!(2);
        for invalid in [
            b"{".to_vec(),
            wrong_wire.into_bytes(),
            serde_json::to_vec(&future_wire).unwrap(),
            vec![b'x'; 64 * 1_024 + 1],
        ] {
            publisher
                .inner
                .client
                .publish(subject.clone(), invalid.into())
                .await
                .unwrap();
        }
        publisher.inner.client.flush().await.unwrap();

        assert_eq!(
            publisher.publish(publication(item.clone(), 0, "fanout")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert_eq!(
            publisher.seal(LiveRunStreamSeal::new(
                item.clone(),
                Some(0),
                insight_engine::run_stream::LiveRunStreamSealStatus::Completed,
            )),
            LiveRunStreamPublishOutcome::SealEnqueued
        );
        for subscriber in [&mut self_subscription, &mut a, &mut b] {
            let LiveRunStreamDelivery::Publication(received) =
                time::timeout(Duration::from_secs(5), subscriber.recv())
                    .await
                    .unwrap()
                    .unwrap()
            else {
                panic!("both ordinary subscribers must receive the body")
            };
            assert_eq!(received.local_sequence(), 0);
            let LiveRunStreamDelivery::Seal(received) =
                time::timeout(Duration::from_secs(5), subscriber.recv())
                    .await
                    .unwrap()
                    .unwrap()
            else {
                panic!("body must precede its seal")
            };
            assert_eq!(received.identity(), &item);
        }

        // Fifty dynamic subscriptions are multiplexed through the one Client
        // owned by this broker; subscribe never constructs another Client.
        let monitor_url = std::env::var("TEST_NATS_MONITOR_URL").ok();
        let connections_before = match monitor_url.as_deref() {
            Some(url) => Some(nats_monitor_connections(url).await),
            None => None,
        };
        let connection_probe = NatsCoreLiveRunStreamBroker::connect(integration_options(
            publisher.inner.options.servers[0].clone(),
            &format!("{namespace}_connections"),
        ))
        .await
        .unwrap();
        let mut subscriptions = Vec::new();
        for _ in 0..50 {
            subscriptions.push(connection_probe.subscribe(RunId::random()).await.unwrap());
        }
        assert_eq!(lock(&connection_probe.inner.inbound).len(), 50);
        if let (Some(url), Some(before)) = (monitor_url.as_deref(), connections_before) {
            assert_eq!(nats_monitor_connections(url).await, before + 1);
        }
        publisher.record_terminal_barrier(
            LiveRunStreamTerminalBarrierOutcome::Complete,
            Duration::from_millis(5),
        );
        let metrics = publisher.prometheus_metrics();
        for family in [
            "run_stream_bus_ready",
            "run_stream_bus_connections",
            "run_stream_bus_reconnect_total",
            "run_stream_bus_active_subscriptions",
            "run_stream_bus_tasks",
            "run_stream_bus_publish_total",
            "run_stream_bus_publish_latency_seconds",
            "run_stream_bus_pending_messages",
            "run_stream_bus_pending_bytes",
            "run_stream_bus_dropped_total",
            "run_stream_bus_gap_total",
            "run_stream_bus_decode_error_total",
            "run_stream_bus_slow_consumer_total",
            "run_stream_bus_subscription_ready_seconds",
            "run_stream_bus_terminal_barrier_seconds",
        ] {
            assert!(metrics.contains(family), "missing metric family {family}");
        }
        assert!(!metrics.contains(&namespace));
        assert!(!metrics.contains(run_id.as_str()));

        drop(subscriptions);
        connection_probe
            .shutdown(Duration::from_secs(5))
            .await
            .unwrap();
        publisher.shutdown(Duration::from_secs(5)).await.unwrap();
        subscriber_a.shutdown(Duration::from_secs(5)).await.unwrap();
        subscriber_b.shutdown(Duration::from_secs(5)).await.unwrap();
    }

    #[tokio::test]
    async fn real_nats_restart_restores_subscription_without_replaying_offline_frame_when_configured(
    ) {
        let (Ok(server), Ok(container)) = (
            std::env::var("TEST_NATS_URL"),
            std::env::var("TEST_NATS_DOCKER_CONTAINER"),
        ) else {
            return;
        };
        let namespace = format!("test_{}", uuid::Uuid::new_v4().simple());
        let publisher =
            NatsCoreLiveRunStreamBroker::connect(integration_options(server.clone(), &namespace))
                .await
                .unwrap();
        let subscriber =
            NatsCoreLiveRunStreamBroker::connect(integration_options(server.clone(), &namespace))
                .await
                .unwrap();
        let run_id = RunId::random();
        let item = identity(run_id.clone());
        let mut subscription = subscriber.subscribe(run_id).await.unwrap();

        let stop = tokio::task::spawn_blocking({
            let container = container.clone();
            move || {
                std::process::Command::new("docker")
                    .args(["stop", &container])
                    .status()
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert!(stop.success());
        let mut restart_guard = DockerRestartGuard::new(&container);
        time::timeout(Duration::from_secs(10), async {
            while publisher
                .check_readiness(Duration::from_millis(50))
                .await
                .is_ok()
            {
                time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            publisher.publish(publication(item.clone(), 0, "offline-not-replayed")),
            LiveRunStreamPublishOutcome::RunClosed
        );
        time::sleep(Duration::from_millis(250)).await;

        let start = tokio::task::spawn_blocking({
            let container = container.clone();
            move || {
                std::process::Command::new("docker")
                    .args(["start", &container])
                    .status()
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert!(start.success());
        restart_guard.disarm();
        await_nats_tcp_endpoint(&server).await;
        for broker in [&publisher, &subscriber] {
            time::timeout(Duration::from_secs(45), async {
                loop {
                    if broker.check_readiness(Duration::from_secs(1)).await.is_ok() {
                        break;
                    }
                    time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .unwrap();
        }

        assert!(matches!(
            publisher.publish(publication(item, 1, "after-reconnect")),
            LiveRunStreamPublishOutcome::Enqueued | LiveRunStreamPublishOutcome::EnqueuedAfterGap
        ));
        let LiveRunStreamDelivery::Gap(gap) =
            time::timeout(Duration::from_secs(5), subscription.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("the missing offline sequence must surface as a gap")
        };
        assert_eq!((gap.missing_from(), gap.missing_to()), (0, Some(0)));
        let LiveRunStreamDelivery::Publication(received) =
            time::timeout(Duration::from_secs(5), subscription.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("the first post-reconnect frame must follow the gap")
        };
        assert_eq!(received.local_sequence(), 1);

        publisher.shutdown(Duration::from_secs(5)).await.unwrap();
        subscriber.shutdown(Duration::from_secs(5)).await.unwrap();
    }

    #[tokio::test]
    async fn real_nats_shutdown_timeout_is_bounded_when_configured() {
        let Ok(server) = std::env::var("TEST_NATS_URL") else {
            return;
        };
        let namespace = format!("test_{}", uuid::Uuid::new_v4().simple());
        let broker = NatsCoreLiveRunStreamBroker::connect(integration_options(server, &namespace))
            .await
            .unwrap();
        let _subscription = broker.subscribe(RunId::random()).await.unwrap();
        let started = Instant::now();
        if let Err(error) = broker.shutdown(Duration::ZERO).await {
            assert_eq!(error.code(), RUN_STREAM_BUS_SHUTDOWN_TIMEOUT);
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!broker.is_accepting());
    }

    #[tokio::test]
    async fn shutdown_timeout_aborts_pending_tasks() {
        let pending = tokio::spawn(std::future::pending::<()>());
        assert!(!wait_for_tasks(vec![pending], Duration::ZERO).await);
    }

    #[tokio::test]
    async fn real_nats_client_pending_overflow_is_bounded_when_configured() {
        let (Ok(server), Ok(container)) = (
            std::env::var("TEST_NATS_URL"),
            std::env::var("TEST_NATS_DOCKER_CONTAINER"),
        ) else {
            return;
        };
        let namespace = format!("test_{}", uuid::Uuid::new_v4().simple());
        let mut options = integration_options(server, &namespace);
        options.max_pending_messages = 16;
        options.max_pending_bytes = 16 * 64 * 1_024;
        options.body_queue_capacity = 512;
        options.control_queue_capacity = 64;
        options.max_run_bytes = 64 * 1_024 * 1_024;
        let broker = NatsCoreLiveRunStreamBroker::connect(options).await.unwrap();
        let run_id = RunId::random();
        let _subscription = broker.subscribe(run_id.clone()).await.unwrap();

        let paused = docker_container_action(&container, "pause").await;
        assert!(paused);
        let body = "x".repeat(60 * 1_024);
        let activation = ActivationId::new("activation_nats_pending_overflow").unwrap();
        let mut outcomes_valid = true;
        for index in 0..500_u32 {
            let identity = LiveRunStreamItemIdentity::new(
                run_id.clone(),
                activation.clone(),
                AttemptNo::FIRST,
                1,
                format!("item_pending_{index}"),
                index,
            )
            .unwrap();
            let outcome = broker.publish(publication(identity, 0, &body));
            outcomes_valid &= matches!(
                outcome,
                LiveRunStreamPublishOutcome::Enqueued | LiveRunStreamPublishOutcome::DroppedWithGap
            );
        }
        time::sleep(Duration::from_secs(1)).await;
        let metrics = broker.prometheus_metrics();
        let unpaused = docker_container_action(&container, "unpause").await;
        assert!(unpaused);
        assert!(outcomes_valid);
        time::timeout(Duration::from_secs(15), async {
            loop {
                if broker.check_readiness(Duration::from_secs(1)).await.is_ok() {
                    break;
                }
                time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            metrics.contains("outcome=\"error\"") || metrics.contains("reason=\"pending_full\""),
            "pending overflow must be explicitly accounted: {metrics}"
        );
        let _ = broker.shutdown(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn real_nats_slow_consumer_event_is_counted_when_configured() {
        let Ok(server) = std::env::var("TEST_NATS_URL") else {
            return;
        };
        let namespace = format!("test_{}", uuid::Uuid::new_v4().simple());
        let mut options = integration_options(server, &namespace);
        options.body_queue_capacity = 2;
        options.control_queue_capacity = 1;
        let broker = NatsCoreLiveRunStreamBroker::connect(options).await.unwrap();
        let subject = nats_live_run_stream_subject(&namespace, &RunId::random()).unwrap();
        let _intentionally_stalled = broker
            .inner
            .client
            .subscribe(subject.clone())
            .await
            .unwrap();
        broker.inner.client.flush().await.unwrap();
        for _ in 0..128 {
            broker
                .inner
                .client
                .publish(subject.clone(), "slow-consumer-probe".into())
                .await
                .unwrap();
        }
        broker.inner.client.flush().await.unwrap();
        time::timeout(Duration::from_secs(5), async {
            while broker.inner.stats.slow_consumers.load(Ordering::Acquire) == 0 {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!broker.prometheus_metrics().contains(
            "run_stream_bus_slow_consumer_total{backend=\"nats_core\",scope=\"client\"} 0"
        ));
        broker.shutdown(Duration::from_secs(5)).await.unwrap();
    }

    #[tokio::test]
    async fn real_nats_tls_credentials_and_subject_acl_fail_closed_when_configured() {
        let (
            Ok(server),
            Ok(root_certificate),
            Ok(wrong_root_certificate),
            Ok(combined_credentials),
            Ok(publisher_credentials),
            Ok(subscriber_credentials),
            Ok(foreign_credentials),
        ) = (
            std::env::var("TEST_NATS_TLS_URL"),
            std::env::var("TEST_NATS_TLS_CA"),
            std::env::var("TEST_NATS_TLS_WRONG_CA"),
            std::env::var("TEST_NATS_COMBINED_CREDS"),
            std::env::var("TEST_NATS_PUBLISHER_CREDS"),
            std::env::var("TEST_NATS_SUBSCRIBER_CREDS"),
            std::env::var("TEST_NATS_FOREIGN_CREDS"),
        )
        else {
            return;
        };
        let read = |path: &str| std::fs::read_to_string(path).unwrap();
        let secure_options = |credentials: String, root: &str, namespace: &str| {
            let mut options = integration_options(server.clone(), namespace);
            options.credentials = Some(credentials);
            options.tls = NatsCoreTlsOptions {
                required: true,
                root_certificates: vec![PathBuf::from(root)],
                client_certificate: None,
                client_private_key: None,
            };
            options
        };

        let combined = NatsCoreLiveRunStreamBroker::connect(secure_options(
            read(&combined_credentials),
            &root_certificate,
            "qualification",
        ))
        .await
        .unwrap();
        let publisher = NatsCoreLiveRunStreamBroker::connect(secure_options(
            read(&publisher_credentials),
            &root_certificate,
            "qualification",
        ))
        .await
        .unwrap();
        let subscriber = NatsCoreLiveRunStreamBroker::connect(secure_options(
            read(&subscriber_credentials),
            &root_certificate,
            "qualification",
        ))
        .await
        .unwrap();
        let run_id = RunId::random();
        let item = identity(run_id.clone());
        let mut subscription = subscriber.subscribe(run_id).await.unwrap();
        assert_eq!(
            publisher.publish(publication(item, 0, "tls-acl-ok")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert!(matches!(
            time::timeout(Duration::from_secs(5), subscription.recv())
                .await
                .unwrap()
                .unwrap(),
            LiveRunStreamDelivery::Publication(_)
        ));
        let publish_only_subscribe = publisher.subscribe(RunId::random()).await;
        if let Ok(subscription) = publish_only_subscribe {
            drop(subscription);
        }
        let error = await_readiness_error(&publisher).await;
        assert_eq!(error.code(), RUN_STREAM_BUS_AUTH_FAILED);

        let bad_ca = NatsCoreLiveRunStreamBroker::connect(secure_options(
            read(&combined_credentials),
            &wrong_root_certificate,
            "qualification",
        ))
        .await
        .unwrap_err();
        assert_eq!(bad_ca.code(), RUN_STREAM_BUS_UNAVAILABLE);
        let bad_credentials = NatsCoreLiveRunStreamBroker::connect(secure_options(
            read(&foreign_credentials),
            &root_certificate,
            "qualification",
        ))
        .await
        .unwrap_err();
        assert_eq!(bad_credentials.code(), RUN_STREAM_BUS_AUTH_FAILED);

        // The credential is valid for the account but its subject permission
        // excludes this installation namespace. The adapter permanently
        // fails readiness after the server reports the ACL violation.
        let wrong_namespace = NatsCoreLiveRunStreamBroker::connect(secure_options(
            read(&combined_credentials),
            &root_certificate,
            "other_installation",
        ))
        .await
        .unwrap();
        let attempted = wrong_namespace.subscribe(RunId::random()).await;
        if let Ok(subscription) = attempted {
            drop(subscription);
        }
        time::sleep(Duration::from_millis(100)).await;
        assert!(
            wrong_namespace
                .inner
                .stats
                .authorization_failed
                .load(Ordering::Acquire),
            "ACL violation was not classified; observed server errors={}",
            wrong_namespace
                .inner
                .stats
                .server_errors
                .load(Ordering::Relaxed)
        );
        let error = await_readiness_error(&wrong_namespace).await;
        assert_eq!(error.code(), RUN_STREAM_BUS_AUTH_FAILED);

        let publish_denied = NatsCoreLiveRunStreamBroker::connect(secure_options(
            read(&subscriber_credentials),
            &root_certificate,
            "qualification",
        ))
        .await
        .unwrap();
        let denied_run = RunId::random();
        let mut denied_receiver = combined.subscribe(denied_run.clone()).await.unwrap();
        assert_eq!(
            publish_denied.publish(publication(identity(denied_run), 0, "must-not-arrive")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        time::sleep(Duration::from_secs(1)).await;
        assert!(
            publish_denied
                .inner
                .stats
                .authorization_failed
                .load(Ordering::Acquire),
            "publish ACL violation was not classified; server errors={}, metrics={}",
            publish_denied
                .inner
                .stats
                .server_errors
                .load(Ordering::Relaxed),
            publish_denied.prometheus_metrics()
        );
        let error = publish_denied
            .check_readiness(Duration::from_millis(250))
            .await
            .unwrap_err();
        assert_eq!(error.code(), RUN_STREAM_BUS_AUTH_FAILED);
        assert!(
            time::timeout(Duration::from_millis(250), denied_receiver.recv())
                .await
                .is_err()
        );

        for broker in [
            &publish_denied,
            &wrong_namespace,
            &publisher,
            &subscriber,
            &combined,
        ] {
            let _ = broker.shutdown(Duration::from_secs(5)).await;
        }
    }

    fn test_options() -> NatsCoreLiveRunStreamBrokerOptions {
        NatsCoreLiveRunStreamBrokerOptions {
            servers: vec!["nats://127.0.0.1:4222".to_owned()],
            namespace: "test".to_owned(),
            credentials: Some("PRIVATE_NKEY_SEED".to_owned()),
            tls: NatsCoreTlsOptions {
                required: false,
                root_certificates: Vec::new(),
                client_certificate: None,
                client_private_key: None,
            },
            connect_timeout: Duration::from_secs(1),
            subscription_ready_timeout: Duration::from_secs(1),
            reconnect_min_delay: Duration::from_millis(100),
            reconnect_max_delay: Duration::from_secs(5),
            max_pending_messages: 64,
            max_pending_bytes: 64 * 1_024,
            body_queue_capacity: 16,
            control_queue_capacity: 4,
            max_frame_bytes: 4 * 1_024,
            max_item_bytes: 64 * 1_024,
            max_run_bytes: 256 * 1_024,
            drain_timeout: Duration::from_secs(1),
            outbound_idle_timeout: Duration::from_secs(60),
        }
    }

    async fn nats_monitor_connections(base_url: &str) -> u64 {
        let base_url = base_url.to_owned();
        let document = tokio::task::spawn_blocking(move || {
            use std::io::{Read, Write};

            let parsed = url::Url::parse(&base_url).unwrap();
            assert_eq!(parsed.scheme(), "http");
            let host = parsed.host_str().unwrap();
            let port = parsed.port_or_known_default().unwrap();
            let mut stream = std::net::TcpStream::connect((host, port)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request_path = format!("{}/connz", parsed.path().trim_end_matches('/'));
            write!(
                stream,
                "GET {request_path} HTTP/1.0\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).unwrap();
            let body_start = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
                .unwrap();
            serde_json::from_slice::<serde_json::Value>(&response[body_start..]).unwrap()
        })
        .await
        .unwrap();
        document["num_connections"].as_u64().unwrap()
    }

    async fn docker_container_action(container: &str, action: &str) -> bool {
        let container = container.to_owned();
        let action = action.to_owned();
        tokio::task::spawn_blocking(move || {
            std::process::Command::new("docker")
                .args([action.as_str(), container.as_str()])
                .status()
                .is_ok_and(|status| status.success())
        })
        .await
        .unwrap_or(false)
    }

    async fn await_nats_tcp_endpoint(server: &str) {
        let parsed = url::Url::parse(server).unwrap();
        let host = parsed.host_str().unwrap().to_owned();
        let port = parsed.port_or_known_default().unwrap();
        time::timeout(Duration::from_secs(30), async {
            loop {
                if tokio::net::TcpStream::connect((host.as_str(), port))
                    .await
                    .is_ok()
                {
                    break;
                }
                time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
    }

    struct DockerRestartGuard {
        container: String,
        armed: bool,
    }

    impl DockerRestartGuard {
        fn new(container: &str) -> Self {
            Self {
                container: container.to_owned(),
                armed: true,
            }
        }

        fn disarm(&mut self) {
            self.armed = false;
        }
    }

    impl Drop for DockerRestartGuard {
        fn drop(&mut self) {
            if self.armed {
                let _ = std::process::Command::new("docker")
                    .args(["start", &self.container])
                    .status();
            }
        }
    }

    async fn await_readiness_error(
        broker: &NatsCoreLiveRunStreamBroker,
    ) -> LiveRunStreamBrokerError {
        time::timeout(Duration::from_secs(5), async {
            loop {
                match broker.check_readiness(Duration::from_millis(250)).await {
                    Ok(()) => time::sleep(Duration::from_millis(25)).await,
                    Err(error) => break error,
                }
            }
        })
        .await
        .unwrap()
    }

    async fn exercise_backend_contract(broker: Arc<dyn LiveRunStreamBroker>) {
        let before_run = RunId::random();
        let before_outcome = broker.publish(publication(
            identity(before_run.clone()),
            0,
            "before-subscribe",
        ));
        assert!(matches!(
            before_outcome,
            LiveRunStreamPublishOutcome::NoSubscriber | LiveRunStreamPublishOutcome::Enqueued
        ));
        let _ = broker.close_run(&before_run);

        let run_id = RunId::random();
        let item = identity(run_id.clone());
        let mut subscriber = broker.subscribe(run_id.clone()).await.unwrap();
        assert!(broker.subscribe(run_id.clone()).await.is_err());
        assert_eq!(
            broker.publish(publication(item.clone(), 0, "first")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(publication(item.clone(), 0, "duplicate")),
            LiveRunStreamPublishOutcome::RejectedOutOfOrder
        );
        assert_eq!(
            broker.publish(publication(item.clone(), 2, "after-gap")),
            LiveRunStreamPublishOutcome::EnqueuedAfterGap
        );

        let LiveRunStreamDelivery::Publication(first) =
            time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("the retained predecessor must arrive first")
        };
        assert_eq!(first.local_sequence(), 0);
        let LiveRunStreamDelivery::Gap(gap) =
            time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("the skipped sequence must form a known gap")
        };
        assert_eq!((gap.missing_from(), gap.missing_to()), (1, Some(1)));
        let LiveRunStreamDelivery::Publication(after_gap) =
            time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("the later publication must follow its gap")
        };
        assert_eq!(after_gap.local_sequence(), 2);

        let seal = LiveRunStreamSeal::new(
            item,
            Some(2),
            insight_engine::run_stream::LiveRunStreamSealStatus::Incomplete,
        );
        assert_eq!(
            broker.seal(seal.clone()),
            LiveRunStreamPublishOutcome::SealEnqueued
        );
        let LiveRunStreamDelivery::Seal(delivered) =
            time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("the incomplete seal must remain explicit")
        };
        assert_eq!(delivered, seal);
        drop(subscriber);
        let _ = broker.close_run(&run_id);

        let completed_run = RunId::random();
        let completed_item = identity(completed_run.clone());
        let mut completed = broker.subscribe(completed_run.clone()).await.unwrap();
        assert_eq!(
            broker.publish(publication(completed_item.clone(), 0, "completed")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        let completed_seal = LiveRunStreamSeal::new(
            completed_item.clone(),
            Some(0),
            insight_engine::run_stream::LiveRunStreamSealStatus::Completed,
        );
        assert_eq!(
            broker.seal(completed_seal.clone()),
            LiveRunStreamPublishOutcome::SealEnqueued
        );
        assert_eq!(
            broker.seal(completed_seal.clone()),
            LiveRunStreamPublishOutcome::SealExactReplay
        );
        assert_eq!(
            broker.publish(publication(completed_item, 1, "late")),
            LiveRunStreamPublishOutcome::RejectedAfterSeal
        );
        assert!(matches!(
            time::timeout(Duration::from_secs(5), completed.recv())
                .await
                .unwrap()
                .unwrap(),
            LiveRunStreamDelivery::Publication(_)
        ));
        let LiveRunStreamDelivery::Seal(delivered) =
            time::timeout(Duration::from_secs(5), completed.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("completed body must be followed by its seal")
        };
        assert_eq!(delivered, completed_seal);
        let _ = broker.close_run(&completed_run);
        drop(completed);

        let overflow_run = RunId::random();
        let overflow_item = identity(overflow_run.clone());
        let mut overflow = broker.subscribe(overflow_run.clone()).await.unwrap();
        for sequence in 0..8 {
            assert_eq!(
                broker.publish(publication(overflow_item.clone(), sequence, "retained")),
                LiveRunStreamPublishOutcome::Enqueued
            );
        }
        assert_eq!(
            broker.publish(publication(overflow_item.clone(), 8, "overflow")),
            LiveRunStreamPublishOutcome::DroppedWithGap
        );
        let overflow_seal = LiveRunStreamSeal::new(
            overflow_item,
            Some(8),
            insight_engine::run_stream::LiveRunStreamSealStatus::Completed,
        );
        assert_eq!(
            broker.seal(overflow_seal.clone()),
            LiveRunStreamPublishOutcome::SealEnqueued
        );
        for sequence in 0..8 {
            let LiveRunStreamDelivery::Publication(delivery) =
                time::timeout(Duration::from_secs(5), overflow.recv())
                    .await
                    .unwrap()
                    .unwrap()
            else {
                panic!("retained overflow predecessors must stay ordered")
            };
            assert_eq!(delivery.local_sequence(), sequence);
        }
        let LiveRunStreamDelivery::Gap(gap) =
            time::timeout(Duration::from_secs(5), overflow.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("body queue overflow must be explicit")
        };
        assert_eq!((gap.missing_from(), gap.missing_to()), (8, Some(8)));
        let LiveRunStreamDelivery::Seal(delivered) =
            time::timeout(Duration::from_secs(5), overflow.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("control reserve must retain the seal")
        };
        assert_eq!(delivered, overflow_seal);
        let _ = broker.close_run(&overflow_run);
        drop(overflow);

        let best_effort_run = RunId::random();
        let observation = LiveRunObservationIdentity::new(
            best_effort_run.clone(),
            ActivationId::new("activation_nats_contract_observation").unwrap(),
            AttemptNo::FIRST,
            "tool_nats_contract",
        )
        .unwrap();
        let seal_item = identity(best_effort_run.clone());
        let mut best_effort = broker.subscribe(best_effort_run.clone()).await.unwrap();
        for sequence in 0..8 {
            assert_eq!(
                broker.publish(
                    LiveRunStreamPublication::new_run_observation(
                        observation.clone(),
                        sequence,
                        insight_engine::run_stream::LiveRunStreamPayload::ToolStarted {
                            call_id: "contract_call".to_owned(),
                            tool_name: "contract_tool".to_owned(),
                            arguments: None,
                        },
                    )
                    .unwrap()
                ),
                LiveRunStreamPublishOutcome::Enqueued
            );
        }
        assert_eq!(
            broker.publish(
                LiveRunStreamPublication::new_run_observation(
                    observation,
                    8,
                    insight_engine::run_stream::LiveRunStreamPayload::ToolStarted {
                        call_id: "contract_call".to_owned(),
                        tool_name: "contract_tool".to_owned(),
                        arguments: None,
                    },
                )
                .unwrap()
            ),
            LiveRunStreamPublishOutcome::DroppedBestEffort
        );
        let observation_seal = LiveRunStreamSeal::new(
            seal_item,
            None,
            insight_engine::run_stream::LiveRunStreamSealStatus::Completed,
        );
        assert_eq!(
            broker.seal(observation_seal.clone()),
            LiveRunStreamPublishOutcome::SealEnqueued
        );
        let LiveRunStreamDelivery::Seal(delivered) =
            time::timeout(Duration::from_secs(5), best_effort.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("best-effort loss must not consume output-item control capacity")
        };
        assert_eq!(delivered, observation_seal);
        for _ in 0..8 {
            assert!(matches!(
                time::timeout(Duration::from_secs(5), best_effort.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                LiveRunStreamDelivery::Publication(_)
            ));
        }
        let _ = broker.close_run(&best_effort_run);
        drop(best_effort);

        let oversize_run = RunId::random();
        let oversize_item = identity(oversize_run.clone());
        let mut oversize = broker.subscribe(oversize_run.clone()).await.unwrap();
        let oversized_body = "x".repeat(64 * 1_024);
        assert!(matches!(
            broker.publish(publication(oversize_item, 0, &oversized_body)),
            LiveRunStreamPublishOutcome::DroppedWithGap
                | LiveRunStreamPublishOutcome::DroppedOversizeWithGap
        ));
        let LiveRunStreamDelivery::Gap(gap) =
            time::timeout(Duration::from_secs(5), oversize.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("oversize output must become an explicit gap")
        };
        assert_eq!((gap.missing_from(), gap.missing_to()), (0, Some(0)));
        let _ = broker.close_run(&oversize_run);
        drop(oversize);

        let unknown_run = RunId::random();
        let unknown_item = identity(unknown_run.clone());
        let mut unknown = broker.subscribe(unknown_run.clone()).await.unwrap();
        assert_eq!(
            broker.publish(publication(unknown_item, 0, "partial")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert!(matches!(
            time::timeout(Duration::from_secs(5), unknown.recv())
                .await
                .unwrap()
                .unwrap(),
            LiveRunStreamDelivery::Publication(_)
        ));
        let close = broker.close_run(&unknown_run);
        assert_eq!(close.unknown_tail_gaps(), 1);
        let LiveRunStreamDelivery::Gap(gap) = time::timeout(Duration::from_secs(5), unknown.recv())
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("closing an unsealed item must expose an unknown tail")
        };
        assert!(gap.has_unknown_tail());
        assert!(time::timeout(Duration::from_secs(5), unknown.recv())
            .await
            .unwrap()
            .is_err());
    }
}
