//! Bounded, sharded safety scanning over the critical-control bulkhead.

use crate::{CoordinatorIdentityFactory, IdentityFactoryError};
use async_trait::async_trait;
use insight_platform_contracts::{HardLimitProfile, ResourceId, ResourceKind, WorkClass};
use insight_platform_postgres::artifact_repository::{
    ArtifactRecoverySlot, DriveExpiredArtifactJobs, RecoveredArtifactJob,
};
use insight_platform_postgres::repository::{
    ConvergedOrchestrationRun, DriveDueOrchestrationRetries, DriveDueOrchestrationWaits,
    DriveExpiredOrchestrationJobs, DriveExpiredOrchestrationTasks, DriveOrchestrationConvergence,
    DueOrchestrationRetrySlot, DueOrchestrationWaitSlot, ExpiredOrchestrationRecoverySlot,
    ExpiredOrchestrationTaskSlot, OrchestrationConvergenceSlot, OrchestrationWakeMutationIds,
    PgRepository, PromotedOrchestrationRetry, RecoveredOrchestrationJob, RepositoryError,
    ResolvedOrchestrationTask, SafetyScanCursor, SafetyScanPage, SafetyScanShard,
    WokenOrchestrationJob, MAX_ORCHESTRATION_QUOTA_LINES,
};
use insight_platform_worker::{LocalWorkerPoolError, LocalWorkerPools};
use std::{
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    task::{JoinError, JoinHandle},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait OrchestrationSafetyStore: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn drive_expired_jobs(
        &self,
        command: DriveExpiredOrchestrationJobs,
    ) -> Result<SafetyScanPage<RecoveredOrchestrationJob>, Self::Error>;

    async fn drive_due_retries(
        &self,
        command: DriveDueOrchestrationRetries,
    ) -> Result<SafetyScanPage<PromotedOrchestrationRetry>, Self::Error>;

    async fn drive_due_waits(
        &self,
        command: DriveDueOrchestrationWaits,
    ) -> Result<SafetyScanPage<WokenOrchestrationJob>, Self::Error>;

    async fn drive_convergence(
        &self,
        command: DriveOrchestrationConvergence,
    ) -> Result<SafetyScanPage<ConvergedOrchestrationRun>, Self::Error>;

    async fn drive_expired_tasks(
        &self,
        command: DriveExpiredOrchestrationTasks,
    ) -> Result<SafetyScanPage<ResolvedOrchestrationTask>, Self::Error>;
}

#[async_trait]
impl OrchestrationSafetyStore for PgRepository {
    type Error = RepositoryError;

    async fn drive_expired_jobs(
        &self,
        command: DriveExpiredOrchestrationJobs,
    ) -> Result<SafetyScanPage<RecoveredOrchestrationJob>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let page = transaction
            .drive_expired_orchestration_jobs(command)
            .await?;
        transaction.commit().await?;
        Ok(page)
    }

    async fn drive_due_retries(
        &self,
        command: DriveDueOrchestrationRetries,
    ) -> Result<SafetyScanPage<PromotedOrchestrationRetry>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let page = transaction.drive_due_orchestration_retries(command).await?;
        transaction.commit().await?;
        Ok(page)
    }

    async fn drive_due_waits(
        &self,
        command: DriveDueOrchestrationWaits,
    ) -> Result<SafetyScanPage<WokenOrchestrationJob>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let page = transaction.drive_due_orchestration_waits(command).await?;
        transaction.commit().await?;
        Ok(page)
    }

    async fn drive_convergence(
        &self,
        command: DriveOrchestrationConvergence,
    ) -> Result<SafetyScanPage<ConvergedOrchestrationRun>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let page = transaction.drive_orchestration_convergence(command).await?;
        transaction.commit().await?;
        Ok(page)
    }

    async fn drive_expired_tasks(
        &self,
        command: DriveExpiredOrchestrationTasks,
    ) -> Result<SafetyScanPage<ResolvedOrchestrationTask>, Self::Error> {
        let mut transaction = self.begin_scheduler_transaction().await?;
        let page = transaction
            .drive_expired_orchestration_tasks(command)
            .await?;
        transaction.commit().await?;
        Ok(page)
    }
}

#[async_trait]
pub trait ArtifactSafetyStore: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn drive_expired_artifact_jobs(
        &self,
        command: DriveExpiredArtifactJobs,
    ) -> Result<SafetyScanPage<RecoveredArtifactJob>, Self::Error>;
}

#[async_trait]
impl ArtifactSafetyStore for PgRepository {
    type Error = RepositoryError;

    async fn drive_expired_artifact_jobs(
        &self,
        command: DriveExpiredArtifactJobs,
    ) -> Result<SafetyScanPage<RecoveredArtifactJob>, Self::Error> {
        PgRepository::drive_expired_artifact_jobs(self, command).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SafetyDriverTiming {
    pub scan_interval: Duration,
    pub scan_jitter: Duration,
    pub failure_backoff: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrchestrationSafetyConfig {
    shard: SafetyScanShard,
    batch_size: u16,
    timing: SafetyDriverTiming,
}

impl OrchestrationSafetyConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        shard: SafetyScanShard,
        timing: SafetyDriverTiming,
    ) -> Result<Self, SafetyConfigError> {
        profile
            .validate()
            .map_err(|failure| SafetyConfigError::InvalidProfile(failure.to_string()))?;
        let batch_size = u16::try_from(profile.control_data.recovery_batch.q1_default)
            .map_err(|_| SafetyConfigError::BatchOutsideRepresentation)?;
        let maximum_shards = u16::try_from(profile.control_data.recovery_shards.hard_max)
            .map_err(|_| SafetyConfigError::ShardOutsideRepresentation)?;
        let config = Self {
            shard,
            batch_size,
            timing,
        };
        config.validate(maximum_shards)?;
        Ok(config)
    }

    pub const fn shard(self) -> SafetyScanShard {
        self.shard
    }

    pub const fn batch_size(self) -> u16 {
        self.batch_size
    }

    fn validate(self, maximum_shards: u16) -> Result<(), SafetyConfigError> {
        validate_safety_config(self.batch_size, self.shard, self.timing, maximum_shards)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactSafetyConfig {
    shard: SafetyScanShard,
    batch_size: u16,
    timing: SafetyDriverTiming,
}

impl ArtifactSafetyConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        shard: SafetyScanShard,
        timing: SafetyDriverTiming,
    ) -> Result<Self, SafetyConfigError> {
        profile
            .validate()
            .map_err(|failure| SafetyConfigError::InvalidProfile(failure.to_string()))?;
        let batch_size = u16::try_from(profile.control_data.recovery_batch.q1_default)
            .map_err(|_| SafetyConfigError::BatchOutsideRepresentation)?;
        let maximum_shards = u16::try_from(profile.control_data.recovery_shards.hard_max)
            .map_err(|_| SafetyConfigError::ShardOutsideRepresentation)?;
        let config = Self {
            shard,
            batch_size,
            timing,
        };
        validate_safety_config(batch_size, shard, timing, maximum_shards)?;
        Ok(config)
    }

    pub const fn shard(self) -> SafetyScanShard {
        self.shard
    }

    pub const fn batch_size(self) -> u16 {
        self.batch_size
    }
}

fn validate_safety_config(
    batch_size: u16,
    shard: SafetyScanShard,
    timing: SafetyDriverTiming,
    maximum_shards: u16,
) -> Result<(), SafetyConfigError> {
    if batch_size == 0
        || shard.count == 0
        || shard.count > maximum_shards
        || shard.index >= shard.count
        || timing.scan_interval.is_zero()
        || timing.failure_backoff.is_zero()
        || timing.scan_jitter >= timing.scan_interval
        || timing.failure_backoff > timing.scan_interval
    {
        return Err(SafetyConfigError::InvalidConfig);
    }
    Ok(())
}

#[derive(Debug)]
pub enum SafetyConfigError {
    InvalidProfile(String),
    BatchOutsideRepresentation,
    ShardOutsideRepresentation,
    InvalidConfig,
}

impl fmt::Display for SafetyConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProfile(message) => {
                write!(formatter, "invalid HardLimitProfile: {message}")
            }
            Self::BatchOutsideRepresentation => {
                formatter.write_str("recovery batch does not fit u16")
            }
            Self::ShardOutsideRepresentation => {
                formatter.write_str("recovery shard count does not fit u16")
            }
            Self::InvalidConfig => formatter.write_str("invalid safety-driver configuration"),
        }
    }
}

impl Error for SafetyConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SafetyDriverLifecycle {
    Running = 1,
    Stopping = 2,
    Stopped = 3,
}

impl SafetyDriverLifecycle {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Running,
            2 => Self::Stopping,
            _ => Self::Stopped,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SafetyDriverSnapshot {
    pub lifecycle: SafetyDriverLifecycle,
    pub cycles: u64,
    pub scan_attempts: u64,
    pub scan_failures: u64,
    pub capacity_skips: u64,
    pub mutations: u64,
    pub full_pages: u64,
    pub shard_wraps: u64,
}

struct SafetyMetrics {
    lifecycle: AtomicU8,
    cycles: AtomicU64,
    scan_attempts: AtomicU64,
    scan_failures: AtomicU64,
    capacity_skips: AtomicU64,
    mutations: AtomicU64,
    full_pages: AtomicU64,
    shard_wraps: AtomicU64,
}

impl SafetyMetrics {
    fn new() -> Self {
        Self {
            lifecycle: AtomicU8::new(SafetyDriverLifecycle::Running as u8),
            cycles: AtomicU64::new(0),
            scan_attempts: AtomicU64::new(0),
            scan_failures: AtomicU64::new(0),
            capacity_skips: AtomicU64::new(0),
            mutations: AtomicU64::new(0),
            full_pages: AtomicU64::new(0),
            shard_wraps: AtomicU64::new(0),
        }
    }
}

#[derive(Default)]
struct SafetyCursors {
    expired_jobs: Option<SafetyScanCursor>,
    due_retries: Option<SafetyScanCursor>,
    due_waits: Option<SafetyScanCursor>,
    convergence: Option<SafetyScanCursor>,
    expired_tasks: Option<SafetyScanCursor>,
}

pub struct OrchestrationSafetyDriver<S, I> {
    store: Arc<S>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    config: OrchestrationSafetyConfig,
}

impl<S, I> OrchestrationSafetyDriver<S, I>
where
    S: OrchestrationSafetyStore,
    I: CoordinatorIdentityFactory + 'static,
{
    pub fn new(
        store: Arc<S>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        config: OrchestrationSafetyConfig,
    ) -> Result<Self, SafetyDriverError> {
        let pool = pools.snapshot();
        if pool.work_class != WorkClass::Orchestration {
            return Err(SafetyDriverError::WrongWorkerManifest);
        }
        if pool.critical_control_capacity == 0 {
            return Err(SafetyDriverError::NoCriticalControlCapacity);
        }
        Ok(Self {
            store,
            identities,
            pools,
            config,
        })
    }

    pub fn spawn(self) -> RunningOrchestrationSafetyDriver {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let metrics = Arc::new(SafetyMetrics::new());
        let task_metrics = Arc::clone(&metrics);
        let join = tokio::spawn(async move { self.run(task_cancellation, task_metrics).await });
        RunningOrchestrationSafetyDriver {
            cancellation,
            metrics,
            join: Some(join),
        }
    }

    async fn run(
        self,
        cancellation: CancellationToken,
        metrics: Arc<SafetyMetrics>,
    ) -> Result<SafetyDriverSnapshot, SafetyDriverError> {
        let period = self
            .config
            .timing
            .scan_interval
            .checked_add(deterministic_safety_jitter(
                &self.pools.snapshot().worker_process_generation_id,
                self.config.timing.scan_jitter,
            ))
            .ok_or(SafetyDriverError::TimingOverflow)?;
        let mut timer = tokio::time::interval_at(Instant::now(), period);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut cursors = SafetyCursors::default();
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = timer.tick() => {
                    metrics.cycles.fetch_add(1, Ordering::Relaxed);
                    if let Err(failure) = self.drive_cycle(&mut cursors, &metrics).await {
                        match failure {
                            SafetyDriveFailure::StoreUnavailable => {
                                metrics.scan_failures.fetch_add(1, Ordering::Relaxed);
                                tokio::select! {
                                    _ = cancellation.cancelled() => break,
                                    _ = tokio::time::sleep(self.config.timing.failure_backoff) => {}
                                }
                            }
                            SafetyDriveFailure::Fatal(failure) => return Err(failure),
                        }
                    }
                }
            }
        }
        metrics
            .lifecycle
            .store(SafetyDriverLifecycle::Stopping as u8, Ordering::Release);
        metrics
            .lifecycle
            .store(SafetyDriverLifecycle::Stopped as u8, Ordering::Release);
        Ok(safety_snapshot(&metrics))
    }

    async fn drive_cycle(
        &self,
        cursors: &mut SafetyCursors,
        metrics: &SafetyMetrics,
    ) -> Result<(), SafetyDriveFailure> {
        self.drive_expired_jobs(cursors, metrics).await?;
        self.drive_due_retries(cursors, metrics).await?;
        self.drive_due_waits(cursors, metrics).await?;
        self.drive_convergence(cursors, metrics).await?;
        self.drive_expired_tasks(cursors, metrics).await?;
        Ok(())
    }

    fn try_control_permit(
        &self,
        metrics: &SafetyMetrics,
    ) -> Result<Option<insight_platform_worker::ActiveCriticalControlPermit>, SafetyDriveFailure>
    {
        let permit = self
            .pools
            .try_acquire_critical_control()
            .map_err(|failure| SafetyDriveFailure::Fatal(SafetyDriverError::LocalPool(failure)))?;
        if permit.is_none() {
            metrics.capacity_skips.fetch_add(1, Ordering::Relaxed);
        }
        Ok(permit)
    }

    async fn drive_expired_jobs(
        &self,
        cursors: &mut SafetyCursors,
        metrics: &SafetyMetrics,
    ) -> Result<(), SafetyDriveFailure> {
        let Some(_permit) = self.try_control_permit(metrics)? else {
            return Ok(());
        };
        let command = DriveExpiredOrchestrationJobs {
            shard: self.config.shard,
            after: cursors.expired_jobs.clone(),
            limit: self.config.batch_size,
            slots: (0..self.config.batch_size)
                .map(|_| expired_job_slot(self.identities.as_ref()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(identity_failure)?,
        };
        metrics.scan_attempts.fetch_add(1, Ordering::Relaxed);
        let page = self
            .store
            .drive_expired_jobs(command)
            .await
            .map_err(|_| SafetyDriveFailure::StoreUnavailable)?;
        observe_page(&page, &mut cursors.expired_jobs, metrics);
        Ok(())
    }

    async fn drive_due_retries(
        &self,
        cursors: &mut SafetyCursors,
        metrics: &SafetyMetrics,
    ) -> Result<(), SafetyDriveFailure> {
        let Some(_permit) = self.try_control_permit(metrics)? else {
            return Ok(());
        };
        let command = DriveDueOrchestrationRetries {
            shard: self.config.shard,
            after: cursors.due_retries.clone(),
            limit: self.config.batch_size,
            slots: (0..self.config.batch_size)
                .map(|_| due_retry_slot(self.identities.as_ref()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(identity_failure)?,
        };
        metrics.scan_attempts.fetch_add(1, Ordering::Relaxed);
        let page = self
            .store
            .drive_due_retries(command)
            .await
            .map_err(|_| SafetyDriveFailure::StoreUnavailable)?;
        observe_page(&page, &mut cursors.due_retries, metrics);
        Ok(())
    }

    async fn drive_due_waits(
        &self,
        cursors: &mut SafetyCursors,
        metrics: &SafetyMetrics,
    ) -> Result<(), SafetyDriveFailure> {
        let Some(_permit) = self.try_control_permit(metrics)? else {
            return Ok(());
        };
        let command = DriveDueOrchestrationWaits {
            shard: self.config.shard,
            after: cursors.due_waits.clone(),
            limit: self.config.batch_size,
            slots: (0..self.config.batch_size)
                .map(|_| due_wait_slot(self.identities.as_ref()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(identity_failure)?,
        };
        metrics.scan_attempts.fetch_add(1, Ordering::Relaxed);
        let page = self
            .store
            .drive_due_waits(command)
            .await
            .map_err(|_| SafetyDriveFailure::StoreUnavailable)?;
        observe_page(&page, &mut cursors.due_waits, metrics);
        Ok(())
    }

    async fn drive_convergence(
        &self,
        cursors: &mut SafetyCursors,
        metrics: &SafetyMetrics,
    ) -> Result<(), SafetyDriveFailure> {
        let Some(_permit) = self.try_control_permit(metrics)? else {
            return Ok(());
        };
        let command = DriveOrchestrationConvergence {
            shard: self.config.shard,
            after: cursors.convergence.clone(),
            limit: self.config.batch_size,
            slots: (0..self.config.batch_size)
                .map(|_| convergence_slot(self.identities.as_ref()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(identity_failure)?,
        };
        metrics.scan_attempts.fetch_add(1, Ordering::Relaxed);
        let page = self
            .store
            .drive_convergence(command)
            .await
            .map_err(|_| SafetyDriveFailure::StoreUnavailable)?;
        observe_page(&page, &mut cursors.convergence, metrics);
        Ok(())
    }

    async fn drive_expired_tasks(
        &self,
        cursors: &mut SafetyCursors,
        metrics: &SafetyMetrics,
    ) -> Result<(), SafetyDriveFailure> {
        let Some(_permit) = self.try_control_permit(metrics)? else {
            return Ok(());
        };
        let command = DriveExpiredOrchestrationTasks {
            shard: self.config.shard,
            after: cursors.expired_tasks.clone(),
            limit: self.config.batch_size,
            slots: (0..self.config.batch_size)
                .map(|_| expired_task_slot(self.identities.as_ref()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(identity_failure)?,
        };
        metrics.scan_attempts.fetch_add(1, Ordering::Relaxed);
        let page = self
            .store
            .drive_expired_tasks(command)
            .await
            .map_err(|_| SafetyDriveFailure::StoreUnavailable)?;
        observe_page(&page, &mut cursors.expired_tasks, metrics);
        Ok(())
    }
}

pub struct ArtifactSafetyDriver<S, I> {
    store: Arc<S>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    config: ArtifactSafetyConfig,
}

impl<S, I> ArtifactSafetyDriver<S, I>
where
    S: ArtifactSafetyStore,
    I: CoordinatorIdentityFactory + 'static,
{
    pub fn new(
        store: Arc<S>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        config: ArtifactSafetyConfig,
    ) -> Result<Self, SafetyDriverError> {
        let pool = pools.snapshot();
        if pool.work_class != WorkClass::Artifact {
            return Err(SafetyDriverError::WrongWorkerManifest);
        }
        if pool.critical_control_capacity == 0 {
            return Err(SafetyDriverError::NoCriticalControlCapacity);
        }
        Ok(Self {
            store,
            identities,
            pools,
            config,
        })
    }

    pub fn spawn(self) -> RunningArtifactSafetyDriver {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let metrics = Arc::new(SafetyMetrics::new());
        let task_metrics = Arc::clone(&metrics);
        let join = tokio::spawn(async move { self.run(task_cancellation, task_metrics).await });
        RunningArtifactSafetyDriver {
            cancellation,
            metrics,
            join: Some(join),
        }
    }

    async fn run(
        self,
        cancellation: CancellationToken,
        metrics: Arc<SafetyMetrics>,
    ) -> Result<SafetyDriverSnapshot, SafetyDriverError> {
        let period = self
            .config
            .timing
            .scan_interval
            .checked_add(deterministic_safety_jitter(
                &self.pools.snapshot().worker_process_generation_id,
                self.config.timing.scan_jitter,
            ))
            .ok_or(SafetyDriverError::TimingOverflow)?;
        let mut timer = tokio::time::interval_at(Instant::now(), period);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut cursor = None;
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = timer.tick() => {
                    metrics.cycles.fetch_add(1, Ordering::Relaxed);
                    if let Err(failure) = self.drive_cycle(&mut cursor, &metrics).await {
                        match failure {
                            SafetyDriveFailure::StoreUnavailable => {
                                metrics.scan_failures.fetch_add(1, Ordering::Relaxed);
                                tokio::select! {
                                    _ = cancellation.cancelled() => break,
                                    _ = tokio::time::sleep(self.config.timing.failure_backoff) => {}
                                }
                            }
                            SafetyDriveFailure::Fatal(failure) => return Err(failure),
                        }
                    }
                }
            }
        }
        metrics
            .lifecycle
            .store(SafetyDriverLifecycle::Stopping as u8, Ordering::Release);
        metrics
            .lifecycle
            .store(SafetyDriverLifecycle::Stopped as u8, Ordering::Release);
        Ok(safety_snapshot(&metrics))
    }

    async fn drive_cycle(
        &self,
        cursor: &mut Option<SafetyScanCursor>,
        metrics: &SafetyMetrics,
    ) -> Result<(), SafetyDriveFailure> {
        let permit = self
            .pools
            .try_acquire_critical_control()
            .map_err(|failure| SafetyDriveFailure::Fatal(SafetyDriverError::LocalPool(failure)))?;
        let Some(_permit) = permit else {
            metrics.capacity_skips.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };
        let command = DriveExpiredArtifactJobs {
            shard: self.config.shard,
            after: cursor.clone(),
            limit: self.config.batch_size,
            slots: (0..self.config.batch_size)
                .map(|_| artifact_recovery_slot(self.identities.as_ref()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(identity_failure)?,
        };
        metrics.scan_attempts.fetch_add(1, Ordering::Relaxed);
        let page = self
            .store
            .drive_expired_artifact_jobs(command)
            .await
            .map_err(|_| SafetyDriveFailure::StoreUnavailable)?;
        observe_page(&page, cursor, metrics);
        Ok(())
    }
}

fn identity_failure(failure: IdentityFactoryError) -> SafetyDriveFailure {
    SafetyDriveFailure::Fatal(SafetyDriverError::Identity(failure))
}

fn observe_page<T>(
    page: &SafetyScanPage<T>,
    cursor: &mut Option<SafetyScanCursor>,
    metrics: &SafetyMetrics,
) {
    metrics
        .mutations
        .fetch_add(page.records.len() as u64, Ordering::Relaxed);
    if page.exhausted {
        metrics.shard_wraps.fetch_add(1, Ordering::Relaxed);
    } else {
        metrics.full_pages.fetch_add(1, Ordering::Relaxed);
    }
    *cursor = page.next_cursor.clone();
}

fn new_id(
    identities: &impl CoordinatorIdentityFactory,
    kind: ResourceKind,
) -> Result<ResourceId, IdentityFactoryError> {
    identities.new_resource_id(kind)
}

fn quota_ids(
    identities: &impl CoordinatorIdentityFactory,
) -> Result<Vec<ResourceId>, IdentityFactoryError> {
    (0..MAX_ORCHESTRATION_QUOTA_LINES)
        .map(|_| new_id(identities, ResourceKind::QuotaLedgerEntry))
        .collect()
}

fn expired_job_slot(
    identities: &impl CoordinatorIdentityFactory,
) -> Result<ExpiredOrchestrationRecoverySlot, IdentityFactoryError> {
    Ok(ExpiredOrchestrationRecoverySlot {
        quota_entry_ids: quota_ids(identities)?,
        run_event_id: new_id(identities, ResourceKind::Event)?,
        run_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        node_event_id: new_id(identities, ResourceKind::Event)?,
        node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        job_event_id: new_id(identities, ResourceKind::Event)?,
        job_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
    })
}

fn due_retry_slot(
    identities: &impl CoordinatorIdentityFactory,
) -> Result<DueOrchestrationRetrySlot, IdentityFactoryError> {
    Ok(DueOrchestrationRetrySlot {
        node_event_id: new_id(identities, ResourceKind::Event)?,
        node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        job_event_id: new_id(identities, ResourceKind::Event)?,
        job_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
    })
}

fn due_wait_slot(
    identities: &impl CoordinatorIdentityFactory,
) -> Result<DueOrchestrationWaitSlot, IdentityFactoryError> {
    Ok(DueOrchestrationWaitSlot {
        idempotency_key_digest: identities.new_lease_token_digest()?,
        request_digest: identities.new_lease_token_digest()?,
        mutations: OrchestrationWakeMutationIds {
            receipt_id: new_id(identities, ResourceKind::Receipt)?,
            node_event_id: new_id(identities, ResourceKind::Event)?,
            node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
            job_event_id: new_id(identities, ResourceKind::Event)?,
            job_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        },
    })
}

fn convergence_slot(
    identities: &impl CoordinatorIdentityFactory,
) -> Result<OrchestrationConvergenceSlot, IdentityFactoryError> {
    Ok(OrchestrationConvergenceSlot {
        quota_entry_ids: quota_ids(identities)?,
        run_event_id: new_id(identities, ResourceKind::Event)?,
        run_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        node_event_id: new_id(identities, ResourceKind::Event)?,
        node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        node_cancelling_event_id: new_id(identities, ResourceKind::Event)?,
        node_cancelling_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        scope_closing_event_id: new_id(identities, ResourceKind::Event)?,
        scope_closing_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        scope_terminal_event_id: new_id(identities, ResourceKind::Event)?,
        scope_terminal_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        job_event_id: new_id(identities, ResourceKind::Event)?,
        job_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
    })
}

fn expired_task_slot(
    identities: &impl CoordinatorIdentityFactory,
) -> Result<ExpiredOrchestrationTaskSlot, IdentityFactoryError> {
    Ok(ExpiredOrchestrationTaskSlot {
        resume_job_id: new_id(identities, ResourceKind::Job)?,
        resume_request_digest: identities.new_lease_token_digest()?,
        task_event_id: new_id(identities, ResourceKind::Event)?,
        task_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        run_event_id: new_id(identities, ResourceKind::Event)?,
        run_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        node_event_id: new_id(identities, ResourceKind::Event)?,
        node_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
        job_event_id: new_id(identities, ResourceKind::Event)?,
        job_outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
    })
}

fn artifact_recovery_slot(
    identities: &impl CoordinatorIdentityFactory,
) -> Result<ArtifactRecoverySlot, IdentityFactoryError> {
    Ok(ArtifactRecoverySlot {
        event_id: new_id(identities, ResourceKind::Event)?,
        outbox_id: new_id(identities, ResourceKind::OutboxEvent)?,
    })
}

fn deterministic_safety_jitter(worker_id: &ResourceId, maximum: Duration) -> Duration {
    if maximum.is_zero() {
        return Duration::ZERO;
    }
    let maximum_nanos = maximum.as_nanos();
    let sample = u128::from(worker_id.uuid().as_u128() as u64);
    let nanos = sample % (maximum_nanos + 1);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn safety_snapshot(metrics: &SafetyMetrics) -> SafetyDriverSnapshot {
    SafetyDriverSnapshot {
        lifecycle: SafetyDriverLifecycle::from_u8(metrics.lifecycle.load(Ordering::Acquire)),
        cycles: metrics.cycles.load(Ordering::Relaxed),
        scan_attempts: metrics.scan_attempts.load(Ordering::Relaxed),
        scan_failures: metrics.scan_failures.load(Ordering::Relaxed),
        capacity_skips: metrics.capacity_skips.load(Ordering::Relaxed),
        mutations: metrics.mutations.load(Ordering::Relaxed),
        full_pages: metrics.full_pages.load(Ordering::Relaxed),
        shard_wraps: metrics.shard_wraps.load(Ordering::Relaxed),
    }
}

enum SafetyDriveFailure {
    StoreUnavailable,
    Fatal(SafetyDriverError),
}

#[derive(Debug)]
pub enum SafetyDriverError {
    WrongWorkerManifest,
    NoCriticalControlCapacity,
    TimingOverflow,
    Identity(IdentityFactoryError),
    LocalPool(LocalWorkerPoolError),
    DriverTask(JoinError),
}

impl fmt::Display for SafetyDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongWorkerManifest => {
                formatter.write_str("safety driver requires the matching WorkerManifest")
            }
            Self::NoCriticalControlCapacity => {
                formatter.write_str("safety driver requires reserved critical-control capacity")
            }
            Self::TimingOverflow => formatter.write_str("safety-driver timer overflowed"),
            Self::Identity(failure) => failure.fmt(formatter),
            Self::LocalPool(failure) => failure.fmt(formatter),
            Self::DriverTask(failure) => write!(formatter, "safety-driver task failed: {failure}"),
        }
    }
}

impl Error for SafetyDriverError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(failure) => Some(failure),
            Self::LocalPool(failure) => Some(failure),
            Self::DriverTask(failure) => Some(failure),
            _ => None,
        }
    }
}

pub struct RunningOrchestrationSafetyDriver {
    cancellation: CancellationToken,
    metrics: Arc<SafetyMetrics>,
    join: Option<JoinHandle<Result<SafetyDriverSnapshot, SafetyDriverError>>>,
}

impl RunningOrchestrationSafetyDriver {
    pub fn snapshot(&self) -> SafetyDriverSnapshot {
        safety_snapshot(&self.metrics)
    }

    pub fn request_stop(&self) {
        self.cancellation.cancel();
    }

    pub async fn shutdown(mut self) -> Result<SafetyDriverSnapshot, SafetyDriverError> {
        self.request_stop();
        let join = self
            .join
            .take()
            .expect("running safety driver owns exactly one task");
        join.await.map_err(SafetyDriverError::DriverTask)?
    }
}

impl Drop for RunningOrchestrationSafetyDriver {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

pub struct RunningArtifactSafetyDriver {
    cancellation: CancellationToken,
    metrics: Arc<SafetyMetrics>,
    join: Option<JoinHandle<Result<SafetyDriverSnapshot, SafetyDriverError>>>,
}

impl RunningArtifactSafetyDriver {
    pub fn snapshot(&self) -> SafetyDriverSnapshot {
        safety_snapshot(&self.metrics)
    }

    pub fn request_stop(&self) {
        self.cancellation.cancel();
    }

    pub async fn shutdown(mut self) -> Result<SafetyDriverSnapshot, SafetyDriverError> {
        self.request_stop();
        let join = self
            .join
            .take()
            .expect("running Artifact safety driver owns exactly one task");
        join.await.map_err(SafetyDriverError::DriverTask)?
    }
}

impl Drop for RunningArtifactSafetyDriver {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, Sha256Digest, WorkClass, WorkerManifest,
    };
    use insight_platform_worker::{ClaimBatchHardLimit, ClaimedJobIdentity};
    use std::{convert::Infallible, sync::Mutex};

    #[derive(Default)]
    struct RecordingStore {
        expired_job_after: Mutex<Vec<Option<SafetyScanCursor>>>,
    }

    #[derive(Default)]
    struct RecordingArtifactStore {
        expired_job_after: Mutex<Vec<Option<SafetyScanCursor>>>,
    }

    #[async_trait]
    impl OrchestrationSafetyStore for RecordingStore {
        type Error = Infallible;

        async fn drive_expired_jobs(
            &self,
            command: DriveExpiredOrchestrationJobs,
        ) -> Result<SafetyScanPage<RecoveredOrchestrationJob>, Self::Error> {
            let mut observations = self.expired_job_after.lock().unwrap();
            observations.push(command.after.clone());
            let first = observations.len() == 1;
            Ok(SafetyScanPage {
                records: Vec::new(),
                next_cursor: first.then(cursor),
                exhausted: !first,
            })
        }

        async fn drive_due_retries(
            &self,
            _command: DriveDueOrchestrationRetries,
        ) -> Result<SafetyScanPage<PromotedOrchestrationRetry>, Self::Error> {
            Ok(empty_page())
        }

        async fn drive_due_waits(
            &self,
            _command: DriveDueOrchestrationWaits,
        ) -> Result<SafetyScanPage<WokenOrchestrationJob>, Self::Error> {
            Ok(empty_page())
        }

        async fn drive_convergence(
            &self,
            _command: DriveOrchestrationConvergence,
        ) -> Result<SafetyScanPage<ConvergedOrchestrationRun>, Self::Error> {
            Ok(empty_page())
        }

        async fn drive_expired_tasks(
            &self,
            _command: DriveExpiredOrchestrationTasks,
        ) -> Result<SafetyScanPage<ResolvedOrchestrationTask>, Self::Error> {
            Ok(empty_page())
        }
    }

    #[async_trait]
    impl ArtifactSafetyStore for RecordingArtifactStore {
        type Error = Infallible;

        async fn drive_expired_artifact_jobs(
            &self,
            command: DriveExpiredArtifactJobs,
        ) -> Result<SafetyScanPage<RecoveredArtifactJob>, Self::Error> {
            let mut observations = self.expired_job_after.lock().unwrap();
            observations.push(command.after.clone());
            let first = observations.len() == 1;
            Ok(SafetyScanPage {
                records: Vec::new(),
                next_cursor: first.then(cursor),
                exhausted: !first,
            })
        }
    }

    fn empty_page<T>() -> SafetyScanPage<T> {
        SafetyScanPage {
            records: Vec::new(),
            next_cursor: None,
            exhausted: true,
        }
    }

    fn cursor() -> SafetyScanCursor {
        SafetyScanCursor {
            sort_at: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            tenant_id: id(ResourceKind::Tenant, "0011"),
            item_id: id(ResourceKind::Job, "0012"),
        }
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

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn pools() -> LocalWorkerPools {
        LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: 1,
                worker_role: "orchestration-runtime".to_owned(),
                work_class: WorkClass::Orchestration,
                adapter_runtime_digest: digest('a'),
                protocol_version: 1,
                max_concurrency: 1,
                critical_control_reserved_slots: 1,
            },
            id(ResourceKind::WorkerProcessGeneration, "0001"),
        )
        .unwrap()
    }

    fn config() -> OrchestrationSafetyConfig {
        OrchestrationSafetyConfig {
            shard: SafetyScanShard::whole(),
            batch_size: 1,
            timing: SafetyDriverTiming {
                scan_interval: Duration::from_millis(100),
                scan_jitter: Duration::ZERO,
                failure_backoff: Duration::from_millis(10),
            },
        }
    }

    fn artifact_pools() -> LocalWorkerPools {
        LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: 1,
                worker_role: "artifact-recovery".to_owned(),
                work_class: WorkClass::Artifact,
                adapter_runtime_digest: digest('b'),
                protocol_version: 1,
                max_concurrency: 1,
                critical_control_reserved_slots: 1,
            },
            id(ResourceKind::WorkerProcessGeneration, "0002"),
        )
        .unwrap()
    }

    fn artifact_config() -> ArtifactSafetyConfig {
        ArtifactSafetyConfig {
            shard: SafetyScanShard::whole(),
            batch_size: 1,
            timing: SafetyDriverTiming {
                scan_interval: Duration::from_millis(100),
                scan_jitter: Duration::ZERO,
                failure_backoff: Duration::from_millis(10),
            },
        }
    }

    #[tokio::test(start_paused = true)]
    async fn high_water_cursor_advances_then_wraps_without_durable_authority() {
        let store = Arc::new(RecordingStore::default());
        let pools = pools();
        let business_reservation = pools
            .reserve_claim_capacity(
                WorkClass::Orchestration,
                1,
                ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile()).unwrap(),
            )
            .unwrap()
            .unwrap();
        let business_permits = business_reservation
            .bind_claimed_jobs(vec![ClaimedJobIdentity {
                job_id: id(ResourceKind::Job, "0013"),
                lease_generation: 1,
            }])
            .unwrap();
        assert_eq!(pools.snapshot().business_available, 0);
        let driver = OrchestrationSafetyDriver::new(
            Arc::clone(&store),
            Arc::new(crate::UuidCoordinatorIdentityFactory),
            pools,
            config(),
        )
        .unwrap()
        .spawn();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        let observations = store.expired_job_after.lock().unwrap().clone();
        assert!(observations.len() >= 2);
        assert!(observations[0].is_none());
        assert_eq!(observations[1], Some(cursor()));
        let snapshot = driver.shutdown().await.unwrap();
        assert_eq!(snapshot.lifecycle, SafetyDriverLifecycle::Stopped);
        assert!(snapshot.scan_attempts >= 8);
        assert!(snapshot.full_pages >= 1);
        assert!(snapshot.shard_wraps >= 7);
        drop(business_permits);
    }

    #[tokio::test(start_paused = true)]
    async fn artifact_recovery_uses_its_own_manifest_and_critical_control_capacity() {
        let store = Arc::new(RecordingArtifactStore::default());
        let pools = artifact_pools();
        let business_reservation = pools
            .reserve_claim_capacity(
                WorkClass::Artifact,
                1,
                ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile()).unwrap(),
            )
            .unwrap()
            .unwrap();
        let business_permits = business_reservation
            .bind_claimed_jobs(vec![ClaimedJobIdentity {
                job_id: id(ResourceKind::Job, "0014"),
                lease_generation: 1,
            }])
            .unwrap();
        assert_eq!(pools.snapshot().business_available, 0);
        let driver = ArtifactSafetyDriver::new(
            Arc::clone(&store),
            Arc::new(crate::UuidCoordinatorIdentityFactory),
            pools,
            artifact_config(),
        )
        .unwrap()
        .spawn();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        let observations = store.expired_job_after.lock().unwrap().clone();
        assert!(observations.len() >= 2);
        assert!(observations[0].is_none());
        assert_eq!(observations[1], Some(cursor()));
        let snapshot = driver.shutdown().await.unwrap();
        assert_eq!(snapshot.lifecycle, SafetyDriverLifecycle::Stopped);
        assert!(snapshot.scan_attempts >= 2);
        drop(business_permits);
    }

    #[test]
    fn profile_freezes_recovery_batch_and_shard_bounds() {
        let profile = checked_in_hard_limit_profile();
        let config = OrchestrationSafetyConfig::from_profile(
            &profile,
            SafetyScanShard {
                index: 15,
                count: 16,
            },
            SafetyDriverTiming {
                scan_interval: Duration::from_secs(1),
                scan_jitter: Duration::from_millis(50),
                failure_backoff: Duration::from_millis(100),
            },
        )
        .unwrap();
        assert_eq!(config.batch_size(), 1_000);
        assert_eq!(config.shard().count, 16);

        assert!(OrchestrationSafetyConfig::from_profile(
            &profile,
            SafetyScanShard {
                index: 255,
                count: 256,
            },
            config.timing,
        )
        .is_ok());
        assert!(OrchestrationSafetyConfig::from_profile(
            &profile,
            SafetyScanShard {
                index: 0,
                count: 257,
            },
            config.timing,
        )
        .is_err());
    }
}
