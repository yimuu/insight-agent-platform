//! Capacity-aware durable Work orchestration.

use crate::{CoordinatorIdentityFactory, IdentityFactoryError};
use async_trait::async_trait;
use insight_platform_contracts::{
    HardLimitProfile, ResourceId, ResourceKind, TraceFlags, WorkClass,
};
use insight_platform_postgres::repository::{
    ClaimOrchestrationJobs, ClaimedOrchestrationJob, OrchestrationClaimSlot, PgRepository,
    RepositoryError, MAX_ORCHESTRATION_QUOTA_LINES,
};
use insight_platform_rpc_trace::{scope_trace, RpcTraceContext};
use insight_platform_worker::{
    ActiveLocalJobPermit, ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPoolError,
    LocalWorkerPools,
};
use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::Notify,
    task::{JoinError, JoinHandle, JoinSet},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait OrchestrationClaimStore: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn claim_orchestration_jobs(
        &self,
        command: ClaimOrchestrationJobs,
    ) -> Result<Vec<ClaimedOrchestrationJob>, Self::Error>;
}

#[async_trait]
impl OrchestrationClaimStore for PgRepository {
    type Error = RepositoryError;

    async fn claim_orchestration_jobs(
        &self,
        command: ClaimOrchestrationJobs,
    ) -> Result<Vec<ClaimedOrchestrationJob>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let claimed = transaction.claim_orchestration_jobs(command).await?;
        transaction.commit().await?;
        Ok(claimed)
    }
}

pub struct ActiveOrchestrationJob {
    claimed: ClaimedOrchestrationJob,
    shutdown: CancellationToken,
    _local_permit: ActiveLocalJobPermit,
}

impl ActiveOrchestrationJob {
    pub(crate) fn new(
        claimed: ClaimedOrchestrationJob,
        shutdown: CancellationToken,
        local_permit: ActiveLocalJobPermit,
    ) -> Self {
        Self {
            claimed,
            shutdown,
            _local_permit: local_permit,
        }
    }

    pub fn claimed(&self) -> &ClaimedOrchestrationJob {
        &self.claimed
    }

    pub fn job_id(&self) -> &str {
        &self.claimed.job.job_id
    }

    pub fn lease_generation(&self) -> i64 {
        self.claimed.job.lease_epoch
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionDisposition {
    GenerationSettled,
    GenerationAbandoned,
}

#[async_trait]
pub trait OrchestrationJobExecutor: Send + Sync + 'static {
    async fn execute(&self, job: ActiveOrchestrationJob) -> ExecutionDisposition;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorTiming {
    pub coalesce_window: Duration,
    pub safety_scan_interval: Duration,
    pub safety_scan_jitter: Duration,
    pub claim_failure_backoff: Duration,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrchestrationCoordinatorConfig {
    requested_claim_size: usize,
    claim_batch_hard_limit: ClaimBatchHardLimit,
    lease_milliseconds: i64,
    timing: CoordinatorTiming,
}

impl OrchestrationCoordinatorConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        timing: CoordinatorTiming,
    ) -> Result<Self, CoordinatorConfigError> {
        profile
            .validate()
            .map_err(|failure| CoordinatorConfigError::InvalidProfile(failure.to_string()))?;
        let requested_claim_size = usize::try_from(profile.run_scheduler.claim_batch.q1_default)
            .map_err(|_| CoordinatorConfigError::ClaimBatchOutsideAddressSpace)?;
        let lease_milliseconds = i64::try_from(profile.run_scheduler.lease_milliseconds.q1_default)
            .map_err(|_| CoordinatorConfigError::LeaseOutsideRepresentation)?;
        let config = Self {
            requested_claim_size,
            claim_batch_hard_limit: ClaimBatchHardLimit::from_profile(profile)
                .map_err(CoordinatorConfigError::LocalPool)?,
            lease_milliseconds,
            timing,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn requested_claim_size(&self) -> usize {
        self.requested_claim_size
    }

    pub fn lease_milliseconds(&self) -> i64 {
        self.lease_milliseconds
    }

    fn validate(&self) -> Result<(), CoordinatorConfigError> {
        if self.requested_claim_size == 0
            || self.claim_batch_hard_limit.get() == 0
            || self.lease_milliseconds <= 0
            || self.timing.coalesce_window.is_zero()
            || self.timing.safety_scan_interval.is_zero()
            || self.timing.claim_failure_backoff.is_zero()
            || self.timing.drain_grace.is_zero()
            || self.timing.safety_scan_jitter >= self.timing.safety_scan_interval
            || self.timing.claim_failure_backoff > self.timing.safety_scan_interval
        {
            return Err(CoordinatorConfigError::InvalidTiming);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum CoordinatorConfigError {
    InvalidProfile(String),
    ClaimBatchOutsideAddressSpace,
    LeaseOutsideRepresentation,
    InvalidTiming,
    LocalPool(LocalWorkerPoolError),
}

impl fmt::Display for CoordinatorConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProfile(message) => {
                write!(formatter, "invalid HardLimitProfile: {message}")
            }
            Self::ClaimBatchOutsideAddressSpace => {
                formatter.write_str("claim batch does not fit the local address space")
            }
            Self::LeaseOutsideRepresentation => {
                formatter.write_str("lease duration does not fit the repository representation")
            }
            Self::InvalidTiming => formatter.write_str("coordinator timing is invalid"),
            Self::LocalPool(failure) => failure.fmt(formatter),
        }
    }
}

impl Error for CoordinatorConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LocalPool(failure) => Some(failure),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CoordinatorWakeSource {
    LocalCommit = 1,
    Nats = 2,
    Deadline = 4,
    SafetyScan = 8,
    Capacity = 16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CoordinatorWakeBatch(u8);

impl CoordinatorWakeBatch {
    pub fn contains(self, source: CoordinatorWakeSource) -> bool {
        self.0 & source as u8 != 0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

struct CoordinatorWakeState {
    pending: AtomicU8,
    closed: AtomicBool,
    received: AtomicU64,
    coalesced: AtomicU64,
    notify: Notify,
}

impl CoordinatorWakeState {
    fn new() -> Self {
        Self {
            pending: AtomicU8::new(0),
            closed: AtomicBool::new(false),
            received: AtomicU64::new(0),
            coalesced: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    fn wake(&self, source: CoordinatorWakeSource) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        self.received.fetch_add(1, Ordering::Relaxed);
        let previous = self.pending.fetch_or(source as u8, Ordering::AcqRel);
        if previous & source as u8 != 0 {
            self.coalesced.fetch_add(1, Ordering::Relaxed);
        }
        self.notify.notify_one();
        true
    }

    async fn wait(&self) {
        loop {
            if self.pending.load(Ordering::Acquire) != 0 || self.closed.load(Ordering::Acquire) {
                return;
            }
            let notified = self.notify.notified();
            if self.pending.load(Ordering::Acquire) != 0 || self.closed.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn take(&self) -> CoordinatorWakeBatch {
        CoordinatorWakeBatch(self.pending.swap(0, Ordering::AcqRel))
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

#[derive(Clone)]
pub struct CoordinatorWakeHandle {
    state: Arc<CoordinatorWakeState>,
}

impl CoordinatorWakeHandle {
    pub fn wake(&self, source: CoordinatorWakeSource) -> bool {
        self.state.wake(source)
    }

    pub fn local_commit(&self) -> bool {
        self.wake(CoordinatorWakeSource::LocalCommit)
    }

    pub fn nats_hint(&self) -> bool {
        self.wake(CoordinatorWakeSource::Nats)
    }

    pub fn deadline(&self) -> bool {
        self.wake(CoordinatorWakeSource::Deadline)
    }

    pub fn safety_scan(&self) -> bool {
        self.wake(CoordinatorWakeSource::SafetyScan)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorLifecycle {
    Running,
    Draining,
    Stopped,
}

impl CoordinatorLifecycle {
    const fn as_u8(self) -> u8 {
        match self {
            Self::Running => 1,
            Self::Draining => 2,
            Self::Stopped => 3,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Running,
            2 => Self::Draining,
            _ => Self::Stopped,
        }
    }
}

struct CoordinatorMetrics {
    lifecycle: AtomicU8,
    drive_batches: AtomicU64,
    claim_attempts: AtomicU64,
    claim_failures: AtomicU64,
    jobs_claimed: AtomicU64,
    active_jobs: AtomicU64,
    settled_generations: AtomicU64,
    abandoned_generations: AtomicU64,
}

impl CoordinatorMetrics {
    fn new() -> Self {
        Self {
            lifecycle: AtomicU8::new(CoordinatorLifecycle::Running.as_u8()),
            drive_batches: AtomicU64::new(0),
            claim_attempts: AtomicU64::new(0),
            claim_failures: AtomicU64::new(0),
            jobs_claimed: AtomicU64::new(0),
            active_jobs: AtomicU64::new(0),
            settled_generations: AtomicU64::new(0),
            abandoned_generations: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorSnapshot {
    pub lifecycle: CoordinatorLifecycle,
    pub wake_hints_received: u64,
    pub wake_hints_coalesced: u64,
    pub drive_batches: u64,
    pub claim_attempts: u64,
    pub claim_failures: u64,
    pub jobs_claimed: u64,
    pub active_jobs: u64,
    pub settled_generations: u64,
    pub abandoned_generations: u64,
}

pub struct WorkCoordinator<S, E, I> {
    store: Arc<S>,
    executor: Arc<E>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    config: OrchestrationCoordinatorConfig,
}

impl<S, E, I> WorkCoordinator<S, E, I>
where
    S: OrchestrationClaimStore,
    E: OrchestrationJobExecutor,
    I: CoordinatorIdentityFactory + 'static,
{
    pub fn new(
        store: Arc<S>,
        executor: Arc<E>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        config: OrchestrationCoordinatorConfig,
    ) -> Result<Self, WorkCoordinatorError> {
        config
            .validate()
            .map_err(WorkCoordinatorError::InvalidConfig)?;
        let snapshot = pools.snapshot();
        if snapshot.work_class != WorkClass::Orchestration || snapshot.business_capacity == 0 {
            return Err(WorkCoordinatorError::WrongWorkerManifest);
        }
        Ok(Self {
            store,
            executor,
            identities,
            pools,
            config,
        })
    }

    pub fn spawn(self) -> RunningWorkCoordinator {
        let wake_state = Arc::new(CoordinatorWakeState::new());
        let metrics = Arc::new(CoordinatorMetrics::new());
        let cancellation = CancellationToken::new();
        let task_wake = Arc::clone(&wake_state);
        let task_metrics = Arc::clone(&metrics);
        let task_cancellation = cancellation.clone();
        let join =
            tokio::spawn(async move { self.run(task_wake, task_metrics, task_cancellation).await });
        RunningWorkCoordinator {
            wake: CoordinatorWakeHandle { state: wake_state },
            metrics,
            cancellation,
            join: Some(join),
        }
    }

    async fn run(
        self,
        wake: Arc<CoordinatorWakeState>,
        metrics: Arc<CoordinatorMetrics>,
        cancellation: CancellationToken,
    ) -> Result<CoordinatorExit, WorkCoordinatorError> {
        let safety_period = self
            .config
            .timing
            .safety_scan_interval
            .checked_add(deterministic_jitter(
                &self.pools.snapshot().worker_process_generation_id,
                self.config.timing.safety_scan_jitter,
            ))
            .ok_or(WorkCoordinatorError::TimingOverflow)?;
        let mut safety = tokio::time::interval_at(Instant::now() + safety_period, safety_period);
        safety.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut active = JoinSet::new();
        let generation_shutdown = CancellationToken::new();
        let mut fatal = None;
        wake.wake(CoordinatorWakeSource::LocalCommit);

        loop {
            if cancellation.is_cancelled() {
                break;
            }
            let no_capacity = self.pools.snapshot().business_available == 0;
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => {
                    observe_execution_join(joined, &metrics);
                    wake.wake(CoordinatorWakeSource::Capacity);
                }
                _ = safety.tick() => {
                    wake.wake(CoordinatorWakeSource::SafetyScan);
                }
                _ = self.pools.wait_for_business_capacity(), if no_capacity => {
                    wake.wake(CoordinatorWakeSource::Capacity);
                }
                _ = wake.wait() => {
                    if cancellation.is_cancelled() {
                        break;
                    }
                    tokio::select! {
                        _ = cancellation.cancelled() => break,
                        _ = tokio::time::sleep(self.config.timing.coalesce_window) => {}
                    }
                    let batch = wake.take();
                    if batch.is_empty() {
                        continue;
                    }
                    metrics.drive_batches.fetch_add(1, Ordering::Relaxed);
                    match self
                        .drive_once(&mut active, &metrics, &generation_shutdown)
                        .await
                    {
                        Ok(DriveResult::NoCapacity | DriveResult::NoWork) => {}
                        Ok(DriveResult::Claimed { filled_reservation }) => {
                            if filled_reservation && self.pools.snapshot().business_available > 0 {
                                wake.wake(CoordinatorWakeSource::LocalCommit);
                            }
                        }
                        Err(DriveError::StoreUnavailable) => {
                            metrics.claim_failures.fetch_add(1, Ordering::Relaxed);
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(self.config.timing.claim_failure_backoff) => {
                                    wake.wake(CoordinatorWakeSource::SafetyScan);
                                }
                            }
                        }
                        Err(DriveError::Fatal(failure)) => {
                            fatal = Some(failure);
                            break;
                        }
                    }
                }
            }
        }

        metrics
            .lifecycle
            .store(CoordinatorLifecycle::Draining.as_u8(), Ordering::Release);
        wake.close();
        generation_shutdown.cancel();
        let abandoned_on_timeout =
            drain_active_jobs(&mut active, &metrics, self.config.timing.drain_grace).await;
        metrics
            .lifecycle
            .store(CoordinatorLifecycle::Stopped.as_u8(), Ordering::Release);
        let exit = CoordinatorExit {
            abandoned_on_drain_timeout: abandoned_on_timeout,
            snapshot: snapshot(&metrics, &wake),
        };
        fatal.map_or(Ok(exit), Err)
    }

    async fn drive_once(
        &self,
        active: &mut JoinSet<ExecutionDisposition>,
        metrics: &CoordinatorMetrics,
        generation_shutdown: &CancellationToken,
    ) -> Result<DriveResult, DriveError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Orchestration,
                self.config.requested_claim_size,
                self.config.claim_batch_hard_limit,
            )
            .map_err(|failure| DriveError::Fatal(WorkCoordinatorError::LocalPool(failure)))?
        else {
            return Ok(DriveResult::NoCapacity);
        };
        let claim_limit = reservation.claim_limit();
        let command = self
            .build_claim_command(claim_limit)
            .map_err(|failure| DriveError::Fatal(WorkCoordinatorError::Identity(failure)))?;
        let expected_tokens = command
            .slots
            .iter()
            .map(|slot| slot.lease_token_digest.to_string())
            .collect::<BTreeSet<_>>();
        metrics.claim_attempts.fetch_add(1, Ordering::Relaxed);
        let claimed = self
            .store
            .claim_orchestration_jobs(command)
            .await
            .map_err(|_failure| {
                tracing::error!("durable orchestration claim failed");
                DriveError::StoreUnavailable
            })?;
        if claimed.is_empty() {
            return Ok(DriveResult::NoWork);
        }
        let identities = validate_claimed_jobs(
            &claimed,
            &self.pools.snapshot().worker_process_generation_id,
            &expected_tokens,
        )
        .map_err(DriveError::Fatal)?;
        let permits = reservation
            .bind_claimed_jobs(identities)
            .map_err(|failure| DriveError::Fatal(WorkCoordinatorError::LocalPool(failure)))?;
        if permits.len() != claimed.len() {
            return Err(DriveError::Fatal(WorkCoordinatorError::CorruptClaim(
                "claimed Job and local permit cardinality differ",
            )));
        }
        let filled_reservation = claimed.len() == claim_limit;
        metrics
            .jobs_claimed
            .fetch_add(claimed.len() as u64, Ordering::Relaxed);
        metrics
            .active_jobs
            .fetch_add(claimed.len() as u64, Ordering::Relaxed);
        for (claimed, permit) in claimed.into_iter().zip(permits) {
            let executor = Arc::clone(&self.executor);
            let job_shutdown = generation_shutdown.clone();
            let trace = RpcTraceContext::start(claimed.job.trace, TraceFlags::NotSampled).map_err(
                |_| {
                    DriveError::Fatal(WorkCoordinatorError::CorruptClaim(
                        "repository returned an invalid Job trace identity",
                    ))
                },
            )?;
            active.spawn(async move {
                scope_trace(trace, async move {
                    executor
                        .execute(ActiveOrchestrationJob::new(claimed, job_shutdown, permit))
                        .await
                })
                .await
            });
        }
        Ok(DriveResult::Claimed { filled_reservation })
    }

    fn build_claim_command(
        &self,
        claim_limit: usize,
    ) -> Result<ClaimOrchestrationJobs, IdentityFactoryError> {
        let limit = u16::try_from(claim_limit).map_err(|_| IdentityFactoryError::Digest)?;
        let slots = (0..claim_limit)
            .map(|_| orchestration_claim_slot(self.identities.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ClaimOrchestrationJobs {
            worker_id: self.pools.snapshot().worker_process_generation_id,
            limit,
            lease_milliseconds: self.config.lease_milliseconds,
            slots,
        })
    }
}

fn orchestration_claim_slot(
    identities: &impl CoordinatorIdentityFactory,
) -> Result<OrchestrationClaimSlot, IdentityFactoryError> {
    let quota_entry_ids = (0..MAX_ORCHESTRATION_QUOTA_LINES)
        .map(|_| identities.new_resource_id(ResourceKind::QuotaLedgerEntry))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(OrchestrationClaimSlot {
        lease_token_digest: identities.new_lease_token_digest()?,
        quota_reservation_id: identities.new_resource_id(ResourceKind::UsageReservation)?,
        quota_entry_ids,
        run_event_id: identities.new_resource_id(ResourceKind::Event)?,
        run_outbox_id: identities.new_resource_id(ResourceKind::OutboxEvent)?,
        node_event_id: identities.new_resource_id(ResourceKind::Event)?,
        node_outbox_id: identities.new_resource_id(ResourceKind::OutboxEvent)?,
        job_event_id: identities.new_resource_id(ResourceKind::Event)?,
        job_outbox_id: identities.new_resource_id(ResourceKind::OutboxEvent)?,
    })
}

fn validate_claimed_jobs(
    claimed: &[ClaimedOrchestrationJob],
    worker_id: &ResourceId,
    expected_tokens: &BTreeSet<String>,
) -> Result<Vec<ClaimedJobIdentity>, WorkCoordinatorError> {
    if claimed.len() > expected_tokens.len() {
        return Err(WorkCoordinatorError::CorruptClaim(
            "repository returned more Jobs than requested",
        ));
    }
    let mut job_ids = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut identities = Vec::with_capacity(claimed.len());
    for record in claimed {
        let job_id =
            ResourceId::parse_expected(&record.job.job_id, ResourceKind::Job).map_err(|_| {
                WorkCoordinatorError::CorruptClaim("repository returned an invalid Job ID")
            })?;
        let generation = u64::try_from(record.job.lease_epoch).map_err(|_| {
            WorkCoordinatorError::CorruptClaim("repository returned an invalid lease generation")
        })?;
        let token =
            record
                .job
                .lease_token_digest
                .as_ref()
                .ok_or(WorkCoordinatorError::CorruptClaim(
                    "repository returned a Job without a lease token",
                ))?;
        if record.job.work_class != WorkClass::Orchestration.as_str()
            || record.job.state != "leased"
            || record.job.worker_id.as_deref() != Some(worker_id.to_string().as_str())
            || generation == 0
            || !expected_tokens.contains(token)
            || !job_ids.insert(job_id.clone())
            || !tokens.insert(token.clone())
        {
            return Err(WorkCoordinatorError::CorruptClaim(
                "repository returned a structurally inconsistent orchestration lease",
            ));
        }
        identities.push(ClaimedJobIdentity {
            job_id,
            lease_generation: generation,
        });
    }
    Ok(identities)
}

fn deterministic_jitter(worker_id: &ResourceId, maximum: Duration) -> Duration {
    if maximum.is_zero() {
        return Duration::ZERO;
    }
    let maximum_nanos = maximum.as_nanos();
    let sample = u128::from(worker_id.uuid().as_u128() as u64);
    let nanos = sample % (maximum_nanos + 1);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn observe_execution_join(
    joined: Option<Result<ExecutionDisposition, JoinError>>,
    metrics: &CoordinatorMetrics,
) {
    let disposition = match joined {
        Some(Ok(disposition)) => disposition,
        Some(Err(_)) | None => ExecutionDisposition::GenerationAbandoned,
    };
    metrics.active_jobs.fetch_sub(1, Ordering::Relaxed);
    match disposition {
        ExecutionDisposition::GenerationSettled => {
            metrics.settled_generations.fetch_add(1, Ordering::Relaxed);
        }
        ExecutionDisposition::GenerationAbandoned => {
            metrics
                .abandoned_generations
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn drain_active_jobs(
    active: &mut JoinSet<ExecutionDisposition>,
    metrics: &CoordinatorMetrics,
    grace: Duration,
) -> u64 {
    let drain = async {
        while !active.is_empty() {
            let joined = active.join_next().await;
            observe_execution_join(joined, metrics);
        }
    };
    if tokio::time::timeout(grace, drain).await.is_ok() {
        return 0;
    }
    let abandoned = u64::try_from(active.len()).unwrap_or(u64::MAX);
    active.abort_all();
    while !active.is_empty() {
        let joined = active.join_next().await;
        observe_execution_join(joined, metrics);
    }
    abandoned
}

fn snapshot(metrics: &CoordinatorMetrics, wake: &CoordinatorWakeState) -> CoordinatorSnapshot {
    CoordinatorSnapshot {
        lifecycle: CoordinatorLifecycle::from_u8(metrics.lifecycle.load(Ordering::Acquire)),
        wake_hints_received: wake.received.load(Ordering::Relaxed),
        wake_hints_coalesced: wake.coalesced.load(Ordering::Relaxed),
        drive_batches: metrics.drive_batches.load(Ordering::Relaxed),
        claim_attempts: metrics.claim_attempts.load(Ordering::Relaxed),
        claim_failures: metrics.claim_failures.load(Ordering::Relaxed),
        jobs_claimed: metrics.jobs_claimed.load(Ordering::Relaxed),
        active_jobs: metrics.active_jobs.load(Ordering::Relaxed),
        settled_generations: metrics.settled_generations.load(Ordering::Relaxed),
        abandoned_generations: metrics.abandoned_generations.load(Ordering::Relaxed),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriveResult {
    NoCapacity,
    NoWork,
    Claimed { filled_reservation: bool },
}

enum DriveError {
    StoreUnavailable,
    Fatal(WorkCoordinatorError),
}

#[derive(Debug)]
pub enum WorkCoordinatorError {
    InvalidConfig(CoordinatorConfigError),
    WrongWorkerManifest,
    TimingOverflow,
    Identity(IdentityFactoryError),
    LocalPool(LocalWorkerPoolError),
    CorruptClaim(&'static str),
    CoordinatorTask(JoinError),
}

impl fmt::Display for WorkCoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(failure) => failure.fmt(formatter),
            Self::WrongWorkerManifest => formatter
                .write_str("orchestration coordinator requires an orchestration WorkerManifest"),
            Self::TimingOverflow => formatter.write_str("coordinator timer exceeds representation"),
            Self::Identity(failure) => failure.fmt(formatter),
            Self::LocalPool(failure) => failure.fmt(formatter),
            Self::CorruptClaim(message) => write!(formatter, "invalid claim result: {message}"),
            Self::CoordinatorTask(failure) => {
                write!(formatter, "coordinator task failed: {failure}")
            }
        }
    }
}

impl Error for WorkCoordinatorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidConfig(failure) => Some(failure),
            Self::Identity(failure) => Some(failure),
            Self::LocalPool(failure) => Some(failure),
            Self::CoordinatorTask(failure) => Some(failure),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorExit {
    pub abandoned_on_drain_timeout: u64,
    pub snapshot: CoordinatorSnapshot,
}

pub struct RunningWorkCoordinator {
    wake: CoordinatorWakeHandle,
    metrics: Arc<CoordinatorMetrics>,
    cancellation: CancellationToken,
    join: Option<JoinHandle<Result<CoordinatorExit, WorkCoordinatorError>>>,
}

impl RunningWorkCoordinator {
    pub fn is_finished(&self) -> bool {
        self.join
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    pub fn wake_handle(&self) -> CoordinatorWakeHandle {
        self.wake.clone()
    }

    pub fn snapshot(&self) -> CoordinatorSnapshot {
        snapshot(&self.metrics, &self.wake.state)
    }

    pub fn request_drain(&self) {
        self.cancellation.cancel();
    }

    pub async fn shutdown(mut self) -> Result<CoordinatorExit, WorkCoordinatorError> {
        self.request_drain();
        let join = self
            .join
            .take()
            .expect("running coordinator owns exactly one task");
        join.await.map_err(WorkCoordinatorError::CoordinatorTask)?
    }
}

impl Drop for RunningWorkCoordinator {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, SchedulerPriority, Sha256Digest, WorkerManifest,
    };
    use insight_platform_postgres::repository::{JobRecord, TypedPayload};
    use std::{convert::Infallible, sync::atomic::AtomicUsize};

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn id(kind: ResourceKind, suffix: &str) -> ResourceId {
        format!(
            "{}_0198f1c5-0787-75e1-a9e8-d95ca0f3{}",
            kind.descriptor().prefix,
            suffix
        )
        .parse()
        .unwrap()
    }

    fn pools(capacity: u16) -> LocalWorkerPools {
        LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: 1,
                worker_role: "orchestration.primary".to_owned(),
                work_class: WorkClass::Orchestration,
                adapter_runtime_digest: digest('a'),
                protocol_version: 1,
                max_concurrency: capacity,
                critical_control_reserved_slots: 1,
            },
            id(ResourceKind::WorkerProcessGeneration, "5501"),
        )
        .unwrap()
    }

    fn config(drain_grace: Duration) -> OrchestrationCoordinatorConfig {
        OrchestrationCoordinatorConfig::from_profile(
            &checked_in_hard_limit_profile(),
            CoordinatorTiming {
                coalesce_window: Duration::from_millis(2),
                safety_scan_interval: Duration::from_millis(200),
                safety_scan_jitter: Duration::from_millis(5),
                claim_failure_backoff: Duration::from_millis(5),
                drain_grace,
            },
        )
        .unwrap()
    }

    struct FakeStore {
        calls: AtomicU64,
        remaining_jobs: AtomicUsize,
    }

    impl FakeStore {
        fn new(remaining_jobs: usize) -> Self {
            Self {
                calls: AtomicU64::new(0),
                remaining_jobs: AtomicUsize::new(remaining_jobs),
            }
        }

        fn calls(&self) -> u64 {
            self.calls.load(Ordering::Relaxed)
        }

        fn add_job(&self) {
            self.remaining_jobs.fetch_add(1, Ordering::Release);
        }
    }

    #[async_trait]
    impl OrchestrationClaimStore for FakeStore {
        type Error = Infallible;

        async fn claim_orchestration_jobs(
            &self,
            command: ClaimOrchestrationJobs,
        ) -> Result<Vec<ClaimedOrchestrationJob>, Self::Error> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let take = self
                .remaining_jobs
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
            if !take {
                return Ok(Vec::new());
            }
            let factory = crate::UuidCoordinatorIdentityFactory;
            let job_id = factory.new_resource_id(ResourceKind::Job).unwrap();
            let tenant_id = id(ResourceKind::Tenant, "5501");
            let node_id = id(ResourceKind::NodeExecution, "5501");
            let run_id = id(ResourceKind::Run, "5501");
            let now = Utc::now();
            let slot = &command.slots[0];
            let job = JobRecord {
                trace: insight_platform_contracts::TraceIdentityV1::generate(),
                tenant_id: tenant_id.to_string(),
                job_id: job_id.to_string(),
                job_kind: insight_platform_contracts::JobKind::OrchestrationNode
                    .as_str()
                    .to_owned(),
                work_class: WorkClass::Orchestration.as_str().to_owned(),
                owner_kind: ResourceKind::NodeExecution.descriptor().name.to_owned(),
                owner_id: node_id.to_string(),
                invocation_id: None,
                run_id: Some(run_id.to_string()),
                node_id: Some(node_id.to_string()),
                state: "leased".to_owned(),
                version: 2,
                attempt_no: 0,
                attempt_limit: 8,
                lease_epoch: 1,
                worker_id: Some(command.worker_id.to_string()),
                lease_token_digest: Some(slot.lease_token_digest.to_string()),
                lease_expires_at: Some(now + chrono::Duration::seconds(30)),
                heartbeat_at: Some(now),
                scheduled_at: now,
                retry_at: None,
                deadline: now + chrono::Duration::minutes(5),
                priority: SchedulerPriority::Normal,
                wake_kind: None,
                wake_state: None,
                wake_generation: 0,
                request_digest: digest('b').to_string(),
                result_digest: None,
                effect_key_digest: None,
                quota_reservation_id: Some(slot.quota_reservation_id.to_string()),
                payload: TypedPayload::new(1, &serde_json::json!({"kind":"controller"})).unwrap(),
                started_at: None,
                terminal_at: None,
                created_at: now,
                updated_at: now,
            };
            Ok(vec![ClaimedOrchestrationJob {
                job,
                run_version: 2,
                node_version: 2,
                quota_reservation_id: slot.quota_reservation_id.to_string(),
                quota_account_ids: vec![],
            }])
        }
    }

    struct FakeExecutor {
        started: AtomicU64,
        release: Notify,
        block: bool,
    }

    impl FakeExecutor {
        fn new(block: bool) -> Self {
            Self {
                started: AtomicU64::new(0),
                release: Notify::new(),
                block,
            }
        }

        async fn wait_started(&self) {
            tokio::time::timeout(Duration::from_secs(1), async {
                while self.started.load(Ordering::Acquire) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    }

    #[async_trait]
    impl OrchestrationJobExecutor for FakeExecutor {
        async fn execute(&self, job: ActiveOrchestrationJob) -> ExecutionDisposition {
            assert_eq!(
                insight_platform_rpc_trace::current_trace()
                    .unwrap()
                    .identity,
                job.claimed().job.trace
            );
            self.started.fetch_add(1, Ordering::Release);
            if self.block {
                self.release.notified().await;
            }
            ExecutionDisposition::GenerationSettled
        }
    }

    fn coordinator(
        store: Arc<FakeStore>,
        executor: Arc<FakeExecutor>,
        pools: LocalWorkerPools,
        config: OrchestrationCoordinatorConfig,
    ) -> WorkCoordinator<FakeStore, FakeExecutor, crate::UuidCoordinatorIdentityFactory> {
        WorkCoordinator::new(
            store,
            executor,
            Arc::new(crate::UuidCoordinatorIdentityFactory),
            pools,
            config,
        )
        .unwrap()
    }

    #[test]
    fn q1_config_is_bounded_and_rejects_zero_timing() {
        let profile = checked_in_hard_limit_profile();
        let valid = config(Duration::from_millis(50));
        assert_eq!(valid.requested_claim_size(), 64);
        assert_eq!(valid.lease_milliseconds(), 30_000);
        assert!(OrchestrationCoordinatorConfig::from_profile(
            &profile,
            CoordinatorTiming {
                coalesce_window: Duration::ZERO,
                safety_scan_interval: Duration::from_secs(1),
                safety_scan_jitter: Duration::ZERO,
                claim_failure_backoff: Duration::from_millis(1),
                drain_grace: Duration::from_secs(1),
            },
        )
        .is_err());
    }

    #[tokio::test]
    async fn repeated_wake_hints_are_coalesced_into_bounded_claim_drives() {
        let store = Arc::new(FakeStore::new(0));
        let executor = Arc::new(FakeExecutor::new(false));
        let running = coordinator(
            Arc::clone(&store),
            executor,
            pools(2),
            config(Duration::from_millis(50)),
        )
        .spawn();
        let wake = running.wake_handle();
        for _ in 0..100 {
            assert!(wake.nats_hint());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let exit = running.shutdown().await.unwrap();
        assert!(exit.snapshot.wake_hints_coalesced > 0);
        assert!(
            store.calls() <= 3,
            "wake storm caused polling amplification"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_drive_recovers_work_without_any_post_admission_wake_hint() {
        let store = Arc::new(FakeStore::new(0));
        let executor = Arc::new(FakeExecutor::new(false));
        let running = coordinator(
            Arc::clone(&store),
            Arc::clone(&executor),
            pools(1),
            config(Duration::from_millis(50)),
        )
        .spawn();
        for _ in 0..10 {
            tokio::time::advance(Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            if store.calls() > 0 {
                break;
            }
        }
        assert!(store.calls() > 0);

        store.add_job();
        for _ in 0..10 {
            tokio::time::advance(Duration::from_millis(50)).await;
            tokio::task::yield_now().await;
            if executor.started.load(Ordering::Acquire) > 0 {
                break;
            }
        }
        assert_eq!(executor.started.load(Ordering::Acquire), 1);

        let exit = running.shutdown().await.unwrap();
        assert_eq!(exit.snapshot.jobs_claimed, 1);
        assert!(exit.snapshot.drive_batches >= 2);
    }

    #[tokio::test]
    async fn no_local_capacity_means_no_database_claim() {
        let pools = pools(1);
        let held = pools
            .reserve_claim_capacity(
                WorkClass::Orchestration,
                1,
                ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile()).unwrap(),
            )
            .unwrap()
            .unwrap()
            .bind_claimed_jobs(vec![ClaimedJobIdentity {
                job_id: id(ResourceKind::Job, "5501"),
                lease_generation: 1,
            }])
            .unwrap();
        let store = Arc::new(FakeStore::new(0));
        let running = coordinator(
            Arc::clone(&store),
            Arc::new(FakeExecutor::new(false)),
            pools,
            config(Duration::from_millis(50)),
        )
        .spawn();
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert_eq!(store.calls(), 0);
        drop(held);
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.calls() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn claim_binds_local_permit_and_graceful_drain_waits_for_generation() {
        let store = Arc::new(FakeStore::new(1));
        let executor = Arc::new(FakeExecutor::new(true));
        let running = coordinator(
            store,
            Arc::clone(&executor),
            pools(1),
            config(Duration::from_millis(100)),
        )
        .spawn();
        executor.wait_started().await;
        assert_eq!(running.snapshot().active_jobs, 1);
        let release = Arc::clone(&executor);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            release.release.notify_one();
        });
        let exit = running.shutdown().await.unwrap();
        assert_eq!(exit.abandoned_on_drain_timeout, 0);
        assert_eq!(exit.snapshot.lifecycle, CoordinatorLifecycle::Stopped);
        assert_eq!(exit.snapshot.settled_generations, 1);
        assert_eq!(exit.snapshot.active_jobs, 0);
    }

    #[tokio::test]
    async fn drain_timeout_aborts_local_future_without_faking_terminal_result() {
        let executor = Arc::new(FakeExecutor::new(true));
        let running = coordinator(
            Arc::new(FakeStore::new(1)),
            Arc::clone(&executor),
            pools(1),
            config(Duration::from_millis(5)),
        )
        .spawn();
        executor.wait_started().await;
        let exit = running.shutdown().await.unwrap();
        assert_eq!(exit.abandoned_on_drain_timeout, 1);
        assert_eq!(exit.snapshot.abandoned_generations, 1);
        assert_eq!(exit.snapshot.settled_generations, 0);
    }
}
