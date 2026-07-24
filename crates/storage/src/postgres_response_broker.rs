//! PostgreSQL `LISTEN`/`NOTIFY` adapter for the transient live-response plane.
//!
//! This module intentionally does not create tables. The private transport
//! envelope exists only long enough for PostgreSQL to deliver a notification;
//! response bodies are never recovery authority and are never logged here.

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgListener, PgPool};
use tokio::{runtime::Handle, task::JoinHandle, time};
use tokio_util::sync::CancellationToken;

use insight_engine::response::adapter::{publication_from_source, publication_payload, RunQueue};
#[cfg(test)]
use insight_engine::response::ResponseStreamEvent;
use insight_engine::response::{
    LiveResponseBroker, LiveResponseBrokerCapability, LiveResponseBrokerError,
    LiveResponseByteLimits, LiveResponseCloseOutcome, LiveResponseDelivery, LiveResponseGap,
    LiveResponseItemIdentity, LiveResponsePayload, LiveResponsePublication,
    LiveResponsePublishOutcome, LiveResponseSeal, LiveResponseSealStatus,
    LiveResponseSourceIdentity, LiveResponseSubscriber, LiveWorkflowObservationIdentity,
    ResponseContentPart, ResponseOutputItem, WorkflowPublicError, WorkflowRetrievalResult,
    WorkflowToolContent,
};
use insight_engine::{ActivationId, AttemptNo, ContentHash, RunId};

const POSTGRES_LIVE_RESPONSE_CONFIG_INVALID: &str = "POSTGRES_LIVE_RESPONSE_CONFIG_INVALID";
const POSTGRES_LIVE_RESPONSE_UNAVAILABLE: &str = "POSTGRES_LIVE_RESPONSE_UNAVAILABLE";
const POSTGRES_LIVE_RESPONSE_NOT_READY: &str = "POSTGRES_LIVE_RESPONSE_NOT_READY";
const POSTGRES_LIVE_RESPONSE_SHUTDOWN_TIMEOUT: &str = "POSTGRES_LIVE_RESPONSE_SHUTDOWN_TIMEOUT";
const POSTGRES_LIVE_RESPONSE_SUBSCRIBER_EXISTS: &str = "POSTGRES_LIVE_RESPONSE_SUBSCRIBER_EXISTS";

const POSTGRES_LIVE_RESPONSE_WIRE_VERSION: u8 = 2;
const POSTGRES_LIVE_RESPONSE_CHANNEL_PREFIX: &str = "insight_live_response_";
const POSTGRES_LIVE_RESPONSE_CHANNEL_HASH_BYTES: usize = 40;
const POSTGRES_LIVE_RESPONSE_READINESS_CHANNEL: &str = "insight_live_response_readiness_v1";
const DEFAULT_OUTBOUND_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_OUTBOUND_REAPER_INTERVAL: Duration = Duration::from_secs(1);

/// PostgreSQL requires the textual `NOTIFY` payload to be shorter than 8 KiB.
/// This stricter ceiling leaves room for the server terminator and avoids
/// relying on server-version-specific edge behavior.
pub const POSTGRES_LIVE_RESPONSE_MAX_NOTIFY_BYTES: usize = 7_900;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresLiveResponseBrokerOptions {
    pub body_queue_capacity: usize,
    pub control_queue_capacity: usize,
    pub max_notify_payload_bytes: usize,
    pub max_item_bytes: usize,
    pub max_run_bytes: usize,
    /// Bounds producer-side state when a worker disappears before publishing
    /// a seal. Normal sealed registrations are retired immediately.
    pub outbound_idle_timeout: Duration,
}

impl PostgresLiveResponseBrokerOptions {
    pub fn new(
        body_queue_capacity: usize,
        control_queue_capacity: usize,
        max_notify_payload_bytes: usize,
    ) -> Result<Self, LiveResponseBrokerError> {
        if body_queue_capacity == 0
            || control_queue_capacity == 0
            || max_notify_payload_bytes == 0
            || max_notify_payload_bytes > POSTGRES_LIVE_RESPONSE_MAX_NOTIFY_BYTES
        {
            return Err(LiveResponseBrokerError::new(
                POSTGRES_LIVE_RESPONSE_CONFIG_INVALID,
                "PostgreSQL live response broker bounds are invalid",
            ));
        }
        Ok(Self {
            body_queue_capacity,
            control_queue_capacity,
            max_notify_payload_bytes,
            max_item_bytes: LiveResponseByteLimits::default().max_item_bytes,
            max_run_bytes: LiveResponseByteLimits::default().max_run_bytes,
            outbound_idle_timeout: DEFAULT_OUTBOUND_IDLE_TIMEOUT,
        })
    }

    pub fn with_publication_limits(
        mut self,
        max_item_bytes: usize,
        max_run_bytes: usize,
    ) -> Result<Self, LiveResponseBrokerError> {
        LiveResponseByteLimits::new(self.max_notify_payload_bytes, max_item_bytes, max_run_bytes)?;
        self.max_item_bytes = max_item_bytes;
        self.max_run_bytes = max_run_bytes;
        Ok(self)
    }

    pub fn with_outbound_idle_timeout(
        mut self,
        outbound_idle_timeout: Duration,
    ) -> Result<Self, LiveResponseBrokerError> {
        if outbound_idle_timeout.is_zero() {
            return Err(LiveResponseBrokerError::new(
                POSTGRES_LIVE_RESPONSE_CONFIG_INVALID,
                "PostgreSQL live response broker idle timeout must be non-zero",
            ));
        }
        self.outbound_idle_timeout = outbound_idle_timeout;
        Ok(self)
    }

    fn byte_limits(self) -> LiveResponseByteLimits {
        LiveResponseByteLimits {
            max_frame_bytes: self.max_notify_payload_bytes,
            max_item_bytes: self.max_item_bytes,
            max_run_bytes: self.max_run_bytes,
        }
    }
}

impl Default for PostgresLiveResponseBrokerOptions {
    fn default() -> Self {
        Self {
            body_queue_capacity: 256,
            control_queue_capacity: 32,
            max_notify_payload_bytes: 4 * 1_024,
            max_item_bytes: 4 * 1_024 * 1_024,
            max_run_bytes: 16 * 1_024 * 1_024,
            outbound_idle_timeout: DEFAULT_OUTBOUND_IDLE_TIMEOUT,
        }
    }
}

/// Returns a stable PostgreSQL-safe channel for one complete Run identity.
/// The fixed 160-bit digest keeps the identifier below PostgreSQL's 63-byte
/// name limit without exposing or truncating the caller-provided Run ID.
pub fn postgres_live_response_channel(run_id: &RunId) -> String {
    let digest = ContentHash::from_bytes(run_id.as_str().as_bytes());
    let hex = digest
        .as_str()
        .strip_prefix("sha256:")
        .expect("ContentHash always uses the sha256 prefix");
    format!(
        "{POSTGRES_LIVE_RESPONSE_CHANNEL_PREFIX}{}",
        &hex[..POSTGRES_LIVE_RESPONSE_CHANNEL_HASH_BYTES]
    )
}

#[derive(Clone)]
pub struct PostgresLiveResponseBroker {
    inner: Arc<PostgresBrokerInner>,
}

struct PostgresBrokerInner {
    pool: PgPool,
    options: PostgresLiveResponseBrokerOptions,
    runtime: Handle,
    accepting: AtomicBool,
    outbound: Mutex<BTreeMap<RunId, Arc<OutboundRegistration>>>,
    inbound: Mutex<BTreeMap<RunId, Arc<InboundRegistration>>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    maintenance_cancel: CancellationToken,
}

struct OutboundRegistration {
    queue: Arc<RunQueue>,
    activity: Mutex<OutboundActivity>,
}

struct OutboundActivity {
    last_activity: Instant,
    open_items: BTreeSet<LiveResponseItemIdentity>,
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

    /// Called while the broker's outbound map lock is held. That lock order
    /// makes registration versus final-seal retirement deterministic.
    fn begin_item(&self, identity: &LiveResponseItemIdentity) {
        let mut activity = lock(&self.activity);
        activity.last_activity = Instant::now();
        activity.open_items.insert(identity.clone());
    }

    fn finish_item(&self, identity: &LiveResponseItemIdentity) -> bool {
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

impl Drop for PostgresBrokerInner {
    fn drop(&mut self) {
        self.accepting.store(false, Ordering::Release);
        self.maintenance_cancel.cancel();
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
    }
}

impl PostgresLiveResponseBroker {
    /// Starts a live-only broker and verifies that the configured PostgreSQL
    /// endpoint supports a dedicated `LISTEN` connection. No schema is read or
    /// written by this adapter.
    pub async fn start(
        pool: PgPool,
        options: PostgresLiveResponseBrokerOptions,
    ) -> Result<Self, LiveResponseBrokerError> {
        PostgresLiveResponseBrokerOptions::new(
            options.body_queue_capacity,
            options.control_queue_capacity,
            options.max_notify_payload_bytes,
        )?
        .with_publication_limits(options.max_item_bytes, options.max_run_bytes)?
        .with_outbound_idle_timeout(options.outbound_idle_timeout)?;
        probe_postgres(&pool).await?;
        let runtime = Handle::try_current().map_err(|_| {
            LiveResponseBrokerError::new(
                POSTGRES_LIVE_RESPONSE_UNAVAILABLE,
                "PostgreSQL live response broker requires a Tokio runtime",
            )
        })?;
        let inner = Arc::new(PostgresBrokerInner {
            pool,
            options,
            runtime: runtime.clone(),
            accepting: AtomicBool::new(true),
            outbound: Mutex::new(BTreeMap::new()),
            inbound: Mutex::new(BTreeMap::new()),
            tasks: Mutex::new(Vec::new()),
            maintenance_cancel: CancellationToken::new(),
        });
        let maintenance = runtime.spawn(outbound_maintenance(Arc::downgrade(&inner)));
        lock(&inner.tasks).push(maintenance);
        Ok(Self { inner })
    }

    pub fn options(&self) -> PostgresLiveResponseBrokerOptions {
        self.inner.options
    }

    pub fn is_accepting(&self) -> bool {
        self.inner.accepting.load(Ordering::Acquire)
    }

    /// Checks both a normal PostgreSQL query and the dedicated `LISTEN`
    /// capability within the caller's readiness budget.
    pub async fn check_readiness(
        &self,
        readiness_timeout: Duration,
    ) -> Result<(), LiveResponseBrokerError> {
        if !self.is_accepting() {
            return Err(LiveResponseBrokerError::new(
                POSTGRES_LIVE_RESPONSE_NOT_READY,
                "PostgreSQL live response broker is not accepting work",
            ));
        }
        match time::timeout(readiness_timeout, probe_postgres(&self.inner.pool)).await {
            Ok(result) => result,
            Err(_) => Err(LiveResponseBrokerError::new(
                POSTGRES_LIVE_RESPONSE_NOT_READY,
                "PostgreSQL live response broker readiness timed out",
            )),
        }
    }

    /// Stops accepting publications, drains queued outbound observations for
    /// at most `grace`, and cancels all listeners. The body itself is never
    /// included in shutdown errors or diagnostics.
    pub async fn shutdown(&self, grace: Duration) -> Result<(), LiveResponseBrokerError> {
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.maintenance_cancel.cancel();

        let outbound = {
            let mut registrations = lock(&self.inner.outbound);
            std::mem::take(&mut *registrations)
        };
        for registration in outbound.into_values() {
            registration.queue.close();
        }

        let inbound = {
            let mut registrations = lock(&self.inner.inbound);
            std::mem::take(&mut *registrations)
        };
        for registration in inbound.into_values() {
            registration.cancel.cancel();
            registration.queue.close();
        }

        let tasks = {
            let mut tasks = lock(&self.inner.tasks);
            std::mem::take(&mut *tasks)
        };
        wait_for_tasks(tasks, grace).await
    }

    fn outbound_registration(
        &self,
        run_id: &RunId,
        item: Option<&LiveResponseItemIdentity>,
    ) -> Option<Arc<OutboundRegistration>> {
        if !self.is_accepting() {
            return None;
        }
        let mut registrations = lock(&self.inner.outbound);
        if !self.is_accepting() {
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
                    self.inner.pool.clone(),
                    run_id.clone(),
                    Arc::clone(&registration.queue),
                    self.inner.options.max_notify_payload_bytes,
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

    fn retire_outbound_if_settled(
        &self,
        run_id: &RunId,
        registration: &Arc<OutboundRegistration>,
        identity: &LiveResponseItemIdentity,
    ) {
        let removed = {
            let mut registrations = lock(&self.inner.outbound);
            if registrations
                .get(run_id)
                .is_some_and(|candidate| Arc::ptr_eq(candidate, registration))
                && registration.finish_item(identity)
            {
                registrations.remove(run_id)
            } else {
                None
            }
        };
        if let Some(registration) = removed {
            // Closing preserves every already-enqueued body/gap/seal and lets
            // the pump drain them before it exits. A later independent item
            // may safely create a fresh registration for the same Run.
            registration.queue.close();
        }
    }
}

impl fmt::Debug for PostgresLiveResponseBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresLiveResponseBroker")
            .field("options", &self.inner.options)
            .field("accepting", &self.is_accepting())
            .field("outbound_runs", &lock(&self.inner.outbound).len())
            .field("inbound_runs", &lock(&self.inner.inbound).len())
            .finish()
    }
}

#[async_trait]
impl LiveResponseBroker for PostgresLiveResponseBroker {
    fn deployment_capability(&self) -> LiveResponseBrokerCapability {
        LiveResponseBrokerCapability::Shared
    }

    async fn check_readiness(
        &self,
        readiness_timeout: Duration,
    ) -> Result<(), LiveResponseBrokerError> {
        PostgresLiveResponseBroker::check_readiness(self, readiness_timeout).await
    }

    async fn shutdown(&self, grace: Duration) -> Result<(), LiveResponseBrokerError> {
        PostgresLiveResponseBroker::shutdown(self, grace).await
    }

    async fn subscribe(
        &self,
        run_id: RunId,
    ) -> Result<Box<dyn LiveResponseSubscriber>, LiveResponseBrokerError> {
        if !self.is_accepting() {
            return Err(LiveResponseBrokerError::new(
                POSTGRES_LIVE_RESPONSE_NOT_READY,
                "PostgreSQL live response broker is not accepting subscriptions",
            ));
        }
        if lock(&self.inner.inbound).contains_key(&run_id) {
            return Err(LiveResponseBrokerError::new(
                POSTGRES_LIVE_RESPONSE_SUBSCRIBER_EXISTS,
                "a PostgreSQL live response subscriber already exists for this Run",
            ));
        }

        let channel = postgres_live_response_channel(&run_id);
        let mut listener = PgListener::connect_with(&self.inner.pool)
            .await
            .map_err(|_| unavailable())?;
        listener.listen(&channel).await.map_err(|_| unavailable())?;

        let registration = Arc::new(InboundRegistration {
            queue: Arc::new(RunQueue::new_with_limits(
                run_id.clone(),
                self.inner.options.body_queue_capacity,
                self.inner.options.control_queue_capacity,
                self.inner.options.byte_limits(),
            )),
            cancel: CancellationToken::new(),
        });
        {
            let mut registrations = lock(&self.inner.inbound);
            if !self.is_accepting() {
                return Err(LiveResponseBrokerError::new(
                    POSTGRES_LIVE_RESPONSE_NOT_READY,
                    "PostgreSQL live response broker is not accepting subscriptions",
                ));
            }
            match registrations.entry(run_id.clone()) {
                Entry::Occupied(_) => {
                    return Err(LiveResponseBrokerError::new(
                        POSTGRES_LIVE_RESPONSE_SUBSCRIBER_EXISTS,
                        "a PostgreSQL live response subscriber already exists for this Run",
                    ));
                }
                Entry::Vacant(entry) => {
                    entry.insert(Arc::clone(&registration));
                    // Register the task before releasing the same map lock
                    // acquired first by shutdown. This prevents shutdown from
                    // draining the task list between map insertion and spawn.
                    let task = self.inner.runtime.spawn(inbound_pump(
                        listener,
                        run_id.clone(),
                        Arc::clone(&registration),
                        self.inner.options.max_notify_payload_bytes,
                    ));
                    lock(&self.inner.tasks).push(task);
                }
            }
        }
        Ok(Box::new(PostgresLiveResponseSubscriber {
            run_id,
            registration,
            owner: Arc::downgrade(&self.inner),
        }))
    }

    fn publish(&self, publication: LiveResponsePublication) -> LiveResponsePublishOutcome {
        let run_id = publication.run_id().clone();
        let Some(registration) =
            self.outbound_registration(&run_id, publication.output_item_identity())
        else {
            return LiveResponsePublishOutcome::RunClosed;
        };
        enqueue_outbound_publication(
            &registration.queue,
            publication,
            self.inner.options.max_notify_payload_bytes,
        )
    }

    fn seal(&self, seal: LiveResponseSeal) -> LiveResponsePublishOutcome {
        let run_id = seal.identity().run_id().clone();
        let identity = seal.identity().clone();
        let Some(registration) = self.outbound_registration(&run_id, None) else {
            return LiveResponsePublishOutcome::RunClosed;
        };
        let outcome = match encode_seal(&seal, self.inner.options.max_notify_payload_bytes) {
            Ok(_) => registration.queue.seal(seal),
            Err(_) => oversize_outcome(
                registration
                    .queue
                    .discard_seal_with_gap(seal.identity().clone()),
            ),
        };
        self.retire_outbound_if_settled(&run_id, &registration, &identity);
        outcome
    }

    fn close_run(&self, run_id: &RunId) -> LiveResponseCloseOutcome {
        let outbound = lock(&self.inner.outbound).remove(run_id);
        let inbound = lock(&self.inner.inbound).remove(run_id);
        let mut outcome = LiveResponseCloseOutcome::default();
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

struct PostgresLiveResponseSubscriber {
    run_id: RunId,
    registration: Arc<InboundRegistration>,
    owner: Weak<PostgresBrokerInner>,
}

#[async_trait]
impl LiveResponseSubscriber for PostgresLiveResponseSubscriber {
    fn run_id(&self) -> &RunId {
        &self.run_id
    }

    async fn recv(&mut self) -> Result<LiveResponseDelivery, LiveResponseBrokerError> {
        self.registration.queue.recv().await
    }
}

impl Drop for PostgresLiveResponseSubscriber {
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

fn enqueue_outbound_publication(
    queue: &RunQueue,
    publication: LiveResponsePublication,
    max_notify_payload_bytes: usize,
) -> LiveResponsePublishOutcome {
    if encode_publication(&publication, max_notify_payload_bytes).is_ok() {
        return queue.publish(publication);
    }
    let local_sequence = publication.local_sequence();
    match publication.source().clone() {
        LiveResponseSourceIdentity::OutputItem(identity) => {
            oversize_outcome(queue.discard_with_gap(identity, local_sequence))
        }
        LiveResponseSourceIdentity::WorkflowObservation(identity) => {
            queue.discard_workflow_observation(identity, local_sequence)
        }
    }
}

fn oversize_outcome(outcome: LiveResponsePublishOutcome) -> LiveResponsePublishOutcome {
    match outcome {
        LiveResponsePublishOutcome::DroppedWithGap
        | LiveResponsePublishOutcome::EnqueuedAfterGap => {
            LiveResponsePublishOutcome::DroppedOversizeWithGap
        }
        other => other,
    }
}

async fn outbound_maintenance(owner: Weak<PostgresBrokerInner>) {
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
        // Completed per-Run pump handles are not shutdown authority. Pruning
        // them here keeps the handle registry bounded along with registrations.
        lock(&inner.tasks).retain(|task| !task.is_finished());
    }
}

fn outbound_reaper_interval(idle_timeout: Duration) -> Duration {
    (idle_timeout / 2)
        .max(Duration::from_millis(1))
        .min(MAX_OUTBOUND_REAPER_INTERVAL)
}

fn reap_idle_outbound(inner: &PostgresBrokerInner, now: Instant) -> usize {
    let expired = {
        let mut registrations = lock(&inner.outbound);
        take_idle_outbound(&mut registrations, now, inner.options.outbound_idle_timeout)
    };
    let count = expired.len();
    for registration in expired {
        // Unknown-tail evidence remains a best-effort observation. Closing the
        // queue drains any retained bodies/gaps before ending the pump, while
        // a later producer publication creates a fresh queue and explicit
        // item-local gap from its non-zero sequence.
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

async fn outbound_pump(
    pool: PgPool,
    run_id: RunId,
    queue: Arc<RunQueue>,
    max_notify_payload_bytes: usize,
) {
    let channel = postgres_live_response_channel(&run_id);
    while let Ok(delivery) = queue.recv().await {
        let encoded = match delivery {
            LiveResponseDelivery::Publication(publication) => {
                let source = publication.source().clone();
                let local_sequence = publication.local_sequence();
                match encode_publication(&publication, max_notify_payload_bytes) {
                    Ok(encoded) => Ok(encoded),
                    Err(_) => match source {
                        LiveResponseSourceIdentity::OutputItem(identity) => {
                            LiveResponseGap::known(identity, local_sequence, local_sequence)
                                .map_err(|_| WireError)
                                .and_then(|gap| encode_gap(&gap, max_notify_payload_bytes))
                        }
                        LiveResponseSourceIdentity::WorkflowObservation(_) => continue,
                    },
                }
            }
            LiveResponseDelivery::Gap(gap) => encode_gap(&gap, max_notify_payload_bytes),
            LiveResponseDelivery::Seal(seal) => encode_seal(&seal, max_notify_payload_bytes),
        };
        let Ok(encoded) = encoded else {
            continue;
        };
        // This task is the only code path that performs database I/O for a
        // synchronous publication. Failure is observation loss, never a
        // durable worker failure; a later index/seal or the terminal barrier
        // will expose the missing range.
        let _ = sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&channel)
            .bind(encoded)
            .execute(&pool)
            .await;
    }
}

async fn inbound_pump(
    mut listener: PgListener,
    run_id: RunId,
    registration: Arc<InboundRegistration>,
    max_notify_payload_bytes: usize,
) {
    let expected_channel = postgres_live_response_channel(&run_id);
    loop {
        let notification = tokio::select! {
            _ = registration.cancel.cancelled() => break,
            notification = listener.recv() => notification,
        };
        let Ok(notification) = notification else {
            break;
        };
        if notification.channel() != expected_channel {
            continue;
        }
        let Ok(delivery) = PostgresLiveResponseWire::decode(
            notification.payload(),
            &run_id,
            max_notify_payload_bytes,
        ) else {
            continue;
        };
        match delivery {
            LiveResponseDelivery::Publication(publication) => {
                let _ = registration.queue.publish(publication);
            }
            LiveResponseDelivery::Gap(gap) => {
                let _ = registration.queue.accept_gap(gap);
            }
            LiveResponseDelivery::Seal(seal) => {
                let _ = registration.queue.seal(seal);
            }
        }
    }
    registration.queue.close();
}

async fn probe_postgres(pool: &PgPool) -> Result<(), LiveResponseBrokerError> {
    sqlx::query("SELECT 1")
        .execute(pool)
        .await
        .map_err(|_| unavailable())?;
    let mut listener = PgListener::connect_with(pool)
        .await
        .map_err(|_| unavailable())?;
    listener
        .listen(POSTGRES_LIVE_RESPONSE_READINESS_CHANNEL)
        .await
        .map_err(|_| unavailable())?;
    Ok(())
}

async fn wait_for_tasks(
    mut tasks: Vec<JoinHandle<()>>,
    grace: Duration,
) -> Result<(), LiveResponseBrokerError> {
    let completed = time::timeout(grace, async {
        for task in &mut tasks {
            let _ = task.await;
        }
    })
    .await
    .is_ok();
    if completed {
        return Ok(());
    }
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
    Err(LiveResponseBrokerError::new(
        POSTGRES_LIVE_RESPONSE_SHUTDOWN_TIMEOUT,
        "PostgreSQL live response broker shutdown exceeded its grace period",
    ))
}

fn unavailable() -> LiveResponseBrokerError {
    LiveResponseBrokerError::new(
        POSTGRES_LIVE_RESPONSE_UNAVAILABLE,
        "PostgreSQL live response broker is unavailable",
    )
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug, Clone, Copy)]
struct WireError;

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PostgresLiveResponseWireRef<'a> {
    Publication {
        schema_version: u8,
        source: WireSourceRef<'a>,
        local_sequence: u64,
        payload: WirePayloadRef<'a>,
    },
    Gap {
        schema_version: u8,
        source: WireSourceRef<'a>,
        missing_from: u64,
        missing_to: Option<u64>,
        unknown_tail: bool,
    },
    Seal {
        schema_version: u8,
        source: WireSourceRef<'a>,
        last_local_sequence: Option<u64>,
        status: WireSealStatus,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PostgresLiveResponseWire {
    Publication {
        schema_version: u8,
        source: WireSource,
        local_sequence: u64,
        payload: WirePayload,
    },
    Gap {
        schema_version: u8,
        source: WireSource,
        missing_from: u64,
        missing_to: Option<u64>,
        unknown_tail: bool,
    },
    Seal {
        schema_version: u8,
        source: WireSource,
        last_local_sequence: Option<u64>,
        status: WireSealStatus,
    },
}

impl PostgresLiveResponseWire {
    fn decode(
        encoded: &str,
        expected_run_id: &RunId,
        max_notify_payload_bytes: usize,
    ) -> Result<LiveResponseDelivery, WireError> {
        if encoded.len() > max_notify_payload_bytes
            || encoded.len() > POSTGRES_LIVE_RESPONSE_MAX_NOTIFY_BYTES
        {
            return Err(WireError);
        }
        let wire: Self = serde_json::from_str(encoded).map_err(|_| WireError)?;
        match wire {
            Self::Publication {
                schema_version,
                source,
                local_sequence,
                payload,
            } => {
                validate_wire_version(schema_version)?;
                let source = source.into_live(expected_run_id)?;
                publication_from_source(source, local_sequence, payload.into_live())
                    .map(LiveResponseDelivery::Publication)
                    .map_err(|_| WireError)
            }
            Self::Gap {
                schema_version,
                source,
                missing_from,
                missing_to,
                unknown_tail,
            } => {
                validate_wire_version(schema_version)?;
                let identity = source.into_output_item(expected_run_id)?;
                let gap = match (missing_to, unknown_tail) {
                    (Some(missing_to), false) => {
                        LiveResponseGap::known(identity, missing_from, missing_to)
                            .map_err(|_| WireError)?
                    }
                    (None, true) => LiveResponseGap::unknown_tail(identity, missing_from),
                    (Some(_), true) | (None, false) => return Err(WireError),
                };
                Ok(LiveResponseDelivery::Gap(gap))
            }
            Self::Seal {
                schema_version,
                source,
                last_local_sequence,
                status,
            } => {
                validate_wire_version(schema_version)?;
                let identity = source.into_output_item(expected_run_id)?;
                let status = match status {
                    WireSealStatus::Completed => LiveResponseSealStatus::Completed,
                    WireSealStatus::Incomplete => LiveResponseSealStatus::Incomplete,
                };
                Ok(LiveResponseDelivery::Seal(LiveResponseSeal::new(
                    identity,
                    last_local_sequence,
                    status,
                )))
            }
        }
    }
}

fn encode_publication(
    publication: &LiveResponsePublication,
    max_notify_payload_bytes: usize,
) -> Result<String, WireError> {
    encode_wire(
        &PostgresLiveResponseWireRef::Publication {
            schema_version: POSTGRES_LIVE_RESPONSE_WIRE_VERSION,
            source: WireSourceRef::from_live(publication.source()),
            local_sequence: publication.local_sequence(),
            payload: WirePayloadRef::from_live(publication_payload(publication)),
        },
        max_notify_payload_bytes,
    )
}

fn encode_gap(gap: &LiveResponseGap, max_notify_payload_bytes: usize) -> Result<String, WireError> {
    encode_wire(
        &PostgresLiveResponseWireRef::Gap {
            schema_version: POSTGRES_LIVE_RESPONSE_WIRE_VERSION,
            source: WireSourceRef::OutputItem {
                identity: WireOutputItemIdentityRef::from_live(gap.identity()),
            },
            missing_from: gap.missing_from(),
            missing_to: gap.missing_to(),
            unknown_tail: gap.has_unknown_tail(),
        },
        max_notify_payload_bytes,
    )
}

fn encode_seal(
    seal: &LiveResponseSeal,
    max_notify_payload_bytes: usize,
) -> Result<String, WireError> {
    let status = match seal.status() {
        LiveResponseSealStatus::Completed => WireSealStatus::Completed,
        LiveResponseSealStatus::Incomplete => WireSealStatus::Incomplete,
    };
    encode_wire(
        &PostgresLiveResponseWireRef::Seal {
            schema_version: POSTGRES_LIVE_RESPONSE_WIRE_VERSION,
            source: WireSourceRef::OutputItem {
                identity: WireOutputItemIdentityRef::from_live(seal.identity()),
            },
            last_local_sequence: seal.last_local_sequence(),
            status,
        },
        max_notify_payload_bytes,
    )
}

fn encode_wire(
    wire: &PostgresLiveResponseWireRef<'_>,
    max_notify_payload_bytes: usize,
) -> Result<String, WireError> {
    if max_notify_payload_bytes == 0
        || max_notify_payload_bytes > POSTGRES_LIVE_RESPONSE_MAX_NOTIFY_BYTES
    {
        return Err(WireError);
    }
    let mut buffer = BoundedWireBuffer::new(max_notify_payload_bytes);
    serde_json::to_writer(&mut buffer, wire).map_err(|_| WireError)?;
    String::from_utf8(buffer.into_bytes()).map_err(|_| WireError)
}

struct BoundedWireBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl BoundedWireBuffer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit),
            limit,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl std::io::Write for BoundedWireBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "PostgreSQL live response wire limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn validate_wire_version(schema_version: u8) -> Result<(), WireError> {
    (schema_version == POSTGRES_LIVE_RESPONSE_WIRE_VERSION)
        .then_some(())
        .ok_or(WireError)
}

#[derive(Serialize)]
#[serde(tag = "source_kind", rename_all = "snake_case")]
enum WireSourceRef<'a> {
    OutputItem {
        #[serde(flatten)]
        identity: WireOutputItemIdentityRef<'a>,
    },
    WorkflowObservation {
        #[serde(flatten)]
        identity: WireWorkflowObservationIdentityRef<'a>,
    },
}

impl<'a> WireSourceRef<'a> {
    fn from_live(source: &'a LiveResponseSourceIdentity) -> Self {
        match source {
            LiveResponseSourceIdentity::OutputItem(identity) => Self::OutputItem {
                identity: WireOutputItemIdentityRef::from_live(identity),
            },
            LiveResponseSourceIdentity::WorkflowObservation(identity) => {
                Self::WorkflowObservation {
                    identity: WireWorkflowObservationIdentityRef::from_live(identity),
                }
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "source_kind", rename_all = "snake_case", deny_unknown_fields)]
enum WireSource {
    OutputItem {
        run_id: RunId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
        model_call_no: u32,
        item_id: String,
        output_index: u32,
    },
    WorkflowObservation {
        run_id: RunId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
        source_id: String,
    },
}

impl WireSource {
    fn into_live(self, expected_run_id: &RunId) -> Result<LiveResponseSourceIdentity, WireError> {
        match self {
            Self::OutputItem {
                run_id,
                activation_id,
                attempt_no,
                model_call_no,
                item_id,
                output_index,
            } => {
                if &run_id != expected_run_id {
                    return Err(WireError);
                }
                LiveResponseItemIdentity::new(
                    run_id,
                    activation_id,
                    attempt_no,
                    model_call_no,
                    item_id,
                    output_index,
                )
                .map(LiveResponseSourceIdentity::OutputItem)
                .map_err(|_| WireError)
            }
            Self::WorkflowObservation {
                run_id,
                activation_id,
                attempt_no,
                source_id,
            } => {
                if &run_id != expected_run_id {
                    return Err(WireError);
                }
                LiveWorkflowObservationIdentity::new(run_id, activation_id, attempt_no, source_id)
                    .map(LiveResponseSourceIdentity::WorkflowObservation)
                    .map_err(|_| WireError)
            }
        }
    }

    fn into_output_item(
        self,
        expected_run_id: &RunId,
    ) -> Result<LiveResponseItemIdentity, WireError> {
        match self.into_live(expected_run_id)? {
            LiveResponseSourceIdentity::OutputItem(identity) => Ok(identity),
            LiveResponseSourceIdentity::WorkflowObservation(_) => Err(WireError),
        }
    }
}

#[derive(Serialize)]
struct WireOutputItemIdentityRef<'a> {
    run_id: &'a RunId,
    activation_id: &'a ActivationId,
    attempt_no: AttemptNo,
    model_call_no: u32,
    item_id: &'a str,
    output_index: u32,
}

impl<'a> WireOutputItemIdentityRef<'a> {
    fn from_live(identity: &'a LiveResponseItemIdentity) -> Self {
        Self {
            run_id: identity.run_id(),
            activation_id: identity.activation_id(),
            attempt_no: identity.attempt_no(),
            model_call_no: identity.model_call_no(),
            item_id: identity.item_id(),
            output_index: identity.output_index(),
        }
    }
}

#[derive(Serialize)]
struct WireWorkflowObservationIdentityRef<'a> {
    run_id: &'a RunId,
    activation_id: &'a ActivationId,
    attempt_no: AttemptNo,
    source_id: &'a str,
}

impl<'a> WireWorkflowObservationIdentityRef<'a> {
    fn from_live(identity: &'a LiveWorkflowObservationIdentity) -> Self {
        Self {
            run_id: identity.run_id(),
            activation_id: identity.activation_id(),
            attempt_no: identity.attempt_no(),
            source_id: identity.source_id(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum WireSealStatus {
    Completed,
    Incomplete,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum WirePayloadRef<'a> {
    OutputItemAdded {
        item: &'a ResponseOutputItem,
    },
    ContentPartAdded {
        content_index: u32,
        part: &'a ResponseContentPart,
    },
    OutputTextDelta {
        content_index: u32,
        delta: &'a str,
    },
    OutputTextDone {
        content_index: u32,
        text: &'a str,
    },
    ContentPartDone {
        content_index: u32,
        part: &'a ResponseContentPart,
    },
    FunctionCallArgumentsDelta {
        delta: &'a str,
    },
    FunctionCallArgumentsDone {
        name: &'a str,
        arguments: &'a str,
    },
    OutputItemDone {
        item: &'a ResponseOutputItem,
    },
    FileSearchCallInProgress,
    FileSearchCallSearching,
    FileSearchCallCompleted,
    ToolStarted {
        call_id: &'a str,
        tool_name: &'a str,
        arguments: &'a Option<Value>,
    },
    ToolCompleted {
        call_id: &'a str,
        tool_name: &'a str,
        content: &'a [WorkflowToolContent],
    },
    ToolFailed {
        call_id: &'a str,
        tool_name: &'a str,
        error: &'a WorkflowPublicError,
    },
    RetrievalCompleted {
        retrieval_id: &'a str,
        query: &'a Option<String>,
        results: &'a [WorkflowRetrievalResult],
    },
}

impl<'a> WirePayloadRef<'a> {
    fn from_live(payload: &'a LiveResponsePayload) -> Self {
        match payload {
            LiveResponsePayload::OutputItemAdded { item } => Self::OutputItemAdded { item },
            LiveResponsePayload::ContentPartAdded {
                content_index,
                part,
            } => Self::ContentPartAdded {
                content_index: *content_index,
                part,
            },
            LiveResponsePayload::OutputTextDelta {
                content_index,
                delta,
            } => Self::OutputTextDelta {
                content_index: *content_index,
                delta,
            },
            LiveResponsePayload::OutputTextDone {
                content_index,
                text,
            } => Self::OutputTextDone {
                content_index: *content_index,
                text,
            },
            LiveResponsePayload::ContentPartDone {
                content_index,
                part,
            } => Self::ContentPartDone {
                content_index: *content_index,
                part,
            },
            LiveResponsePayload::FunctionCallArgumentsDelta { delta } => {
                Self::FunctionCallArgumentsDelta { delta }
            }
            LiveResponsePayload::FunctionCallArgumentsDone { name, arguments } => {
                Self::FunctionCallArgumentsDone { name, arguments }
            }
            LiveResponsePayload::OutputItemDone { item } => Self::OutputItemDone { item },
            LiveResponsePayload::FileSearchCallInProgress => Self::FileSearchCallInProgress,
            LiveResponsePayload::FileSearchCallSearching => Self::FileSearchCallSearching,
            LiveResponsePayload::FileSearchCallCompleted => Self::FileSearchCallCompleted,
            LiveResponsePayload::ToolStarted {
                call_id,
                tool_name,
                arguments,
            } => Self::ToolStarted {
                call_id,
                tool_name,
                arguments,
            },
            LiveResponsePayload::ToolCompleted {
                call_id,
                tool_name,
                content,
            } => Self::ToolCompleted {
                call_id,
                tool_name,
                content,
            },
            LiveResponsePayload::ToolFailed {
                call_id,
                tool_name,
                error,
            } => Self::ToolFailed {
                call_id,
                tool_name,
                error,
            },
            LiveResponsePayload::RetrievalCompleted {
                retrieval_id,
                query,
                results,
            } => Self::RetrievalCompleted {
                retrieval_id,
                query,
                results,
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum WirePayload {
    OutputItemAdded {
        item: ResponseOutputItem,
    },
    ContentPartAdded {
        content_index: u32,
        part: ResponseContentPart,
    },
    OutputTextDelta {
        content_index: u32,
        delta: String,
    },
    OutputTextDone {
        content_index: u32,
        text: String,
    },
    ContentPartDone {
        content_index: u32,
        part: ResponseContentPart,
    },
    FunctionCallArgumentsDelta {
        delta: String,
    },
    FunctionCallArgumentsDone {
        name: String,
        arguments: String,
    },
    OutputItemDone {
        item: ResponseOutputItem,
    },
    FileSearchCallInProgress,
    FileSearchCallSearching,
    FileSearchCallCompleted,
    ToolStarted {
        call_id: String,
        tool_name: String,
        arguments: Option<Value>,
    },
    ToolCompleted {
        call_id: String,
        tool_name: String,
        content: Vec<WorkflowToolContent>,
    },
    ToolFailed {
        call_id: String,
        tool_name: String,
        error: WorkflowPublicError,
    },
    RetrievalCompleted {
        retrieval_id: String,
        query: Option<String>,
        results: Vec<WorkflowRetrievalResult>,
    },
}

impl WirePayload {
    fn into_live(self) -> LiveResponsePayload {
        match self {
            Self::OutputItemAdded { item } => LiveResponsePayload::OutputItemAdded { item },
            Self::ContentPartAdded {
                content_index,
                part,
            } => LiveResponsePayload::ContentPartAdded {
                content_index,
                part,
            },
            Self::OutputTextDelta {
                content_index,
                delta,
            } => LiveResponsePayload::OutputTextDelta {
                content_index,
                delta,
            },
            Self::OutputTextDone {
                content_index,
                text,
            } => LiveResponsePayload::OutputTextDone {
                content_index,
                text,
            },
            Self::ContentPartDone {
                content_index,
                part,
            } => LiveResponsePayload::ContentPartDone {
                content_index,
                part,
            },
            Self::FunctionCallArgumentsDelta { delta } => {
                LiveResponsePayload::FunctionCallArgumentsDelta { delta }
            }
            Self::FunctionCallArgumentsDone { name, arguments } => {
                LiveResponsePayload::FunctionCallArgumentsDone { name, arguments }
            }
            Self::OutputItemDone { item } => LiveResponsePayload::OutputItemDone { item },
            Self::FileSearchCallInProgress => LiveResponsePayload::FileSearchCallInProgress,
            Self::FileSearchCallSearching => LiveResponsePayload::FileSearchCallSearching,
            Self::FileSearchCallCompleted => LiveResponsePayload::FileSearchCallCompleted,
            Self::ToolStarted {
                call_id,
                tool_name,
                arguments,
            } => LiveResponsePayload::ToolStarted {
                call_id,
                tool_name,
                arguments,
            },
            Self::ToolCompleted {
                call_id,
                tool_name,
                content,
            } => LiveResponsePayload::ToolCompleted {
                call_id,
                tool_name,
                content,
            },
            Self::ToolFailed {
                call_id,
                tool_name,
                error,
            } => LiveResponsePayload::ToolFailed {
                call_id,
                tool_name,
                error,
            },
            Self::RetrievalCompleted {
                retrieval_id,
                query,
                results,
            } => LiveResponsePayload::RetrievalCompleted {
                retrieval_id,
                query,
                results,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    fn test_identity(run_id: RunId) -> LiveResponseItemIdentity {
        LiveResponseItemIdentity::new(
            run_id,
            ActivationId::new("activation_live_test").unwrap(),
            AttemptNo::FIRST,
            1,
            "item_live_test",
            0,
        )
        .unwrap()
    }

    fn test_workflow_identity(run_id: RunId) -> LiveWorkflowObservationIdentity {
        LiveWorkflowObservationIdentity::new(
            run_id,
            ActivationId::new("activation_workflow_wire_test").unwrap(),
            AttemptNo::FIRST,
            "tool_source_wire_test",
        )
        .unwrap()
    }

    fn text_publication(
        identity: LiveResponseItemIdentity,
        local_sequence: u64,
        text: impl Into<String>,
    ) -> LiveResponsePublication {
        LiveResponsePublication::new(
            identity,
            local_sequence,
            LiveResponsePayload::OutputTextDelta {
                content_index: 0,
                delta: text.into(),
            },
        )
        .unwrap()
    }

    fn workflow_publication(
        identity: LiveWorkflowObservationIdentity,
        local_sequence: u64,
    ) -> LiveResponsePublication {
        LiveResponsePublication::new_workflow_observation(
            identity,
            local_sequence,
            LiveResponsePayload::ToolStarted {
                call_id: "call_wire_test".to_owned(),
                tool_name: "lookup".to_owned(),
                arguments: Some(json!({"public": "argument"})),
            },
        )
        .unwrap()
    }

    #[test]
    fn channel_uses_the_complete_run_identity_and_is_postgres_safe() {
        let first = RunId::new("run_shared_prefix_first").unwrap();
        let second = RunId::new("run_shared_prefix_second").unwrap();
        let first_channel = postgres_live_response_channel(&first);
        assert_eq!(first_channel, postgres_live_response_channel(&first));
        assert_ne!(first_channel, postgres_live_response_channel(&second));
        assert!(first_channel.len() <= 63);
        assert!(first_channel
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_lowercase() || byte.is_ascii_digit()));
    }

    #[test]
    fn private_wire_is_closed_versioned_and_run_scoped() {
        let run_id = RunId::new("run_wire_test").unwrap();
        let publication = text_publication(test_identity(run_id.clone()), 7, "private-body-marker");
        let encoded = encode_publication(&publication, 4 * 1_024).unwrap();
        let delivery = PostgresLiveResponseWire::decode(&encoded, &run_id, 4 * 1_024).unwrap();
        let LiveResponseDelivery::Publication(decoded) = delivery else {
            panic!("publication wire must decode as a publication");
        };
        assert_eq!(decoded, publication);
        assert!(!format!("{decoded:?}").contains("private-body-marker"));

        let mut unknown_field: Value = serde_json::from_str(&encoded).unwrap();
        unknown_field
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_owned(), json!(true));
        assert!(PostgresLiveResponseWire::decode(
            &serde_json::to_string(&unknown_field).unwrap(),
            &run_id,
            4 * 1_024,
        )
        .is_err());

        let mut future_version: Value = serde_json::from_str(&encoded).unwrap();
        future_version["schema_version"] = json!(POSTGRES_LIVE_RESPONSE_WIRE_VERSION + 1);
        assert!(PostgresLiveResponseWire::decode(
            &serde_json::to_string(&future_version).unwrap(),
            &run_id,
            4 * 1_024,
        )
        .is_err());

        let other_run = RunId::new("run_wire_other").unwrap();
        assert!(PostgresLiveResponseWire::decode(&encoded, &other_run, 4 * 1_024).is_err());

        let workflow = workflow_publication(test_workflow_identity(run_id.clone()), 3);
        let workflow_encoded = encode_publication(&workflow, 4 * 1_024).unwrap();
        let LiveResponseDelivery::Publication(decoded_workflow) =
            PostgresLiveResponseWire::decode(&workflow_encoded, &run_id, 4 * 1_024).unwrap()
        else {
            panic!("workflow wire must decode as a publication")
        };
        assert_eq!(decoded_workflow, workflow);
        assert!(!format!("{decoded_workflow:?}").contains("argument"));
    }

    #[test]
    fn private_wire_rejects_source_payload_and_control_source_mismatches() {
        let run_id = RunId::new("run_wire_source_mismatch").unwrap();
        let publication = text_publication(test_identity(run_id.clone()), 0, "answer");
        let encoded = encode_publication(&publication, 4 * 1_024).unwrap();
        let mut wrong_payload: Value = serde_json::from_str(&encoded).unwrap();
        wrong_payload["payload"] = json!({
            "type": "tool_started",
            "call_id": "call_wrong",
            "tool_name": "lookup",
            "arguments": null
        });
        assert!(PostgresLiveResponseWire::decode(
            &serde_json::to_string(&wrong_payload).unwrap(),
            &run_id,
            4 * 1_024,
        )
        .is_err());

        let seal = LiveResponseSeal::new(
            test_identity(run_id.clone()),
            None,
            LiveResponseSealStatus::Completed,
        );
        let encoded_seal = encode_seal(&seal, 4 * 1_024).unwrap();
        let mut workflow_control_source: Value = serde_json::from_str(&encoded_seal).unwrap();
        workflow_control_source["source"] = json!({
            "source_kind": "workflow_observation",
            "run_id": run_id,
            "activation_id": "activation_workflow_wire_test",
            "attempt_no": 1,
            "source_id": "tool_source_wire_test"
        });
        assert!(PostgresLiveResponseWire::decode(
            &serde_json::to_string(&workflow_control_source).unwrap(),
            &RunId::new("run_wire_source_mismatch").unwrap(),
            4 * 1_024,
        )
        .is_err());

        let mut unknown_source: Value = serde_json::from_str(&encoded).unwrap();
        unknown_source["source"]["source_kind"] = json!("future_source");
        assert!(PostgresLiveResponseWire::decode(
            &serde_json::to_string(&unknown_source).unwrap(),
            &RunId::new("run_wire_source_mismatch").unwrap(),
            4 * 1_024,
        )
        .is_err());
    }

    #[tokio::test]
    async fn oversize_publication_is_dropped_with_an_explicit_gap() {
        let run_id = RunId::new("run_oversize_test").unwrap();
        let identity = test_identity(run_id.clone());
        let queue = RunQueue::new_with_limits(run_id, 4, 4, LiveResponseByteLimits::default());
        let publication = text_publication(identity.clone(), 0, "x".repeat(2_048));
        assert_eq!(
            enqueue_outbound_publication(&queue, publication, 512),
            LiveResponsePublishOutcome::DroppedOversizeWithGap
        );
        let LiveResponseDelivery::Gap(gap) = queue.recv().await.unwrap() else {
            panic!("oversize publication must become a gap");
        };
        assert_eq!(gap.identity(), &identity);
        assert_eq!(gap.missing_from(), 0);
        assert_eq!(gap.missing_to(), Some(0));
        assert!(!gap.has_unknown_tail());
    }

    #[tokio::test]
    async fn oversize_workflow_observation_is_best_effort_without_an_item_gap() {
        let run_id = RunId::new("run_workflow_oversize_test").unwrap();
        let observation = test_workflow_identity(run_id.clone());
        let queue =
            RunQueue::new_with_limits(run_id.clone(), 4, 4, LiveResponseByteLimits::default());
        let publication = LiveResponsePublication::new_workflow_observation(
            observation,
            0,
            LiveResponsePayload::ToolStarted {
                call_id: "call_wire_oversize".to_owned(),
                tool_name: "lookup".to_owned(),
                arguments: Some(json!({"value": "x".repeat(2_048)})),
            },
        )
        .unwrap();
        assert_eq!(
            enqueue_outbound_publication(&queue, publication, 512),
            LiveResponsePublishOutcome::DroppedBestEffort
        );

        let seal = LiveResponseSeal::new(
            test_identity(run_id),
            None,
            LiveResponseSealStatus::Completed,
        );
        assert_eq!(
            queue.seal(seal.clone()),
            LiveResponsePublishOutcome::SealEnqueued
        );
        let LiveResponseDelivery::Seal(delivered) = queue.recv().await.unwrap() else {
            panic!("workflow transport loss must not synthesize an output-item gap")
        };
        assert_eq!(delivered, seal);
    }

    #[test]
    fn options_reject_the_postgres_notify_ceiling() {
        assert!(PostgresLiveResponseBrokerOptions::new(1, 1, 1).is_ok());
        assert!(PostgresLiveResponseBrokerOptions::new(0, 1, 1).is_err());
        assert!(PostgresLiveResponseBrokerOptions::new(
            1,
            1,
            POSTGRES_LIVE_RESPONSE_MAX_NOTIFY_BYTES + 1,
        )
        .is_err());
        assert!(PostgresLiveResponseBrokerOptions::new(1, 1, 1)
            .unwrap()
            .with_outbound_idle_timeout(Duration::ZERO)
            .is_err());
    }

    #[tokio::test]
    async fn idle_outbound_reclamation_is_bounded_and_preserves_active_runs() {
        let now = Instant::now();
        let idle_timeout = Duration::from_millis(50);
        let stale_run = RunId::new("run_idle_outbound_stale").unwrap();
        let active_run = RunId::new("run_idle_outbound_active").unwrap();
        let queue = |run_id: RunId| {
            Arc::new(RunQueue::new_with_limits(
                run_id,
                4,
                4,
                LiveResponseByteLimits::default(),
            ))
        };
        let stale = Arc::new(OutboundRegistration::new_at(
            queue(stale_run.clone()),
            now.checked_sub(idle_timeout + Duration::from_millis(1))
                .unwrap(),
        ));
        let active = Arc::new(OutboundRegistration::new_at(queue(active_run.clone()), now));
        let mut registrations = BTreeMap::from([
            (stale_run.clone(), Arc::clone(&stale)),
            (active_run.clone(), Arc::clone(&active)),
        ]);

        let expired = take_idle_outbound(&mut registrations, now, idle_timeout);
        assert_eq!(expired.len(), 1);
        assert!(Arc::ptr_eq(&expired[0], &stale));
        assert!(!registrations.contains_key(&stale_run));
        assert!(registrations.contains_key(&active_run));

        expired[0].queue.close();
        assert_eq!(
            stale.queue.recv().await.unwrap_err().code(),
            "LIVE_RESPONSE_STREAM_CLOSED"
        );
    }

    #[test]
    fn a_registration_settles_only_after_its_last_parallel_item_seals() {
        let run_id = RunId::new("run_parallel_outbound_items").unwrap();
        let first = test_identity(run_id.clone());
        let second = LiveResponseItemIdentity::new(
            run_id.clone(),
            ActivationId::new("activation_live_second").unwrap(),
            AttemptNo::FIRST,
            1,
            "item_live_second",
            1,
        )
        .unwrap();
        let registration = OutboundRegistration::new(Arc::new(RunQueue::new_with_limits(
            run_id,
            4,
            4,
            LiveResponseByteLimits::default(),
        )));
        registration.begin_item(&first);
        registration.begin_item(&second);
        assert!(!registration.finish_item(&first));
        assert!(registration.finish_item(&second));
    }

    #[tokio::test]
    async fn detached_unsealed_publication_is_reclaimed_when_postgres_is_available() {
        let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
            return;
        };
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await
            .unwrap();
        let options = PostgresLiveResponseBrokerOptions::new(4, 4, 4 * 1_024)
            .unwrap()
            .with_outbound_idle_timeout(Duration::from_millis(40))
            .unwrap();
        let broker = PostgresLiveResponseBroker::start(pool.clone(), options)
            .await
            .unwrap();
        let run_id = RunId::random();
        let identity = test_identity(run_id.clone());
        assert_eq!(
            broker.publish(text_publication(identity, 0, "detached-body")),
            LiveResponsePublishOutcome::Enqueued
        );
        assert!(lock(&broker.inner.outbound).contains_key(&run_id));

        time::timeout(Duration::from_secs(2), async {
            while lock(&broker.inner.outbound).contains_key(&run_id) {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("idle detached producer registration was not reclaimed");

        broker.shutdown(Duration::from_secs(5)).await.unwrap();
        pool.close().await;
    }

    #[tokio::test]
    async fn two_broker_instances_exchange_body_then_seal_when_postgres_is_available() {
        let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
            return;
        };
        let publisher_pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await
            .unwrap();
        let subscriber_pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await
            .unwrap();
        let options = PostgresLiveResponseBrokerOptions::new(8, 8, 4 * 1_024).unwrap();
        let publisher = PostgresLiveResponseBroker::start(publisher_pool.clone(), options)
            .await
            .unwrap();
        let subscriber = PostgresLiveResponseBroker::start(subscriber_pool.clone(), options)
            .await
            .unwrap();
        publisher
            .check_readiness(Duration::from_secs(5))
            .await
            .unwrap();

        let run_id = RunId::random();
        let identity = test_identity(run_id.clone());
        let mut subscription = subscriber.subscribe(run_id.clone()).await.unwrap();
        assert_eq!(
            publisher.publish(text_publication(identity.clone(), 0, "cross-broker-body",)),
            LiveResponsePublishOutcome::Enqueued
        );
        assert_eq!(
            publisher.seal(LiveResponseSeal::new(
                identity.clone(),
                Some(0),
                LiveResponseSealStatus::Completed,
            )),
            LiveResponsePublishOutcome::SealEnqueued
        );
        assert!(
            !lock(&publisher.inner.outbound).contains_key(&run_id),
            "the producer-side registration must retire without an HTTP-side close"
        );

        let first = time::timeout(Duration::from_secs(5), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        let LiveResponseDelivery::Publication(publication) = first else {
            panic!("body must arrive before its seal");
        };
        let public = publication.into_public_event(2);
        assert!(matches!(
            public,
            ResponseStreamEvent::ResponseOutputTextDelta {
                delta,
                ..
            } if delta == "cross-broker-body"
        ));

        let second = time::timeout(Duration::from_secs(5), subscription.recv())
            .await
            .unwrap()
            .unwrap();
        let LiveResponseDelivery::Seal(seal) = second else {
            panic!("the second delivery must be the item seal");
        };
        assert_eq!(seal.identity(), &identity);
        assert_eq!(seal.last_local_sequence(), Some(0));

        publisher.close_run(&run_id);
        drop(subscription);
        publisher.shutdown(Duration::from_secs(5)).await.unwrap();
        assert!(publisher
            .check_readiness(Duration::from_millis(10))
            .await
            .is_err());
        subscriber.shutdown(Duration::from_secs(5)).await.unwrap();
        publisher_pool.close().await;
        subscriber_pool.close().await;
    }
}
