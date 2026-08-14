//! Bounded Sandbox expired-lease recovery on reserved critical-control capacity.

use crate::{CoordinatorIdentityFactory, IdentityFactoryError, SafetyDriverTiming};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    CommandOutcome, HardLimitProfile, ResourceId, ResourceKind, WorkClass,
};
use insight_platform_postgres::{
    repository::{
        PgRepository, RepositoryError, SafetyScanCursor, SafetyScanPage, SafetyScanShard,
    },
    sandbox_repository::ScanExpiredSandboxLeases,
};
use insight_platform_sandbox::{
    ExpiredSandboxLease, RecoverExpiredSandboxLease, SandboxBackendHostError, SandboxExecutorHost,
    SandboxLeaseRecoveryAction, SandboxLeaseRecoveryAuthority, SandboxLeaseRecoveryDisposition,
    SandboxLeaseRecoveryResult, SandboxRecoveryAudit, SANDBOX_QUOTA_LINES,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxRecoveryStoreFailure {
    Unavailable,
    FirstWinnerLost,
    InvariantViolation,
}

impl fmt::Display for SandboxRecoveryStoreFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "Sandbox recovery store is unavailable",
            Self::FirstWinnerLost => "Sandbox recovery lost the first-winner race",
            Self::InvariantViolation => "Sandbox recovery store invariant failed",
        })
    }
}

impl Error for SandboxRecoveryStoreFailure {}

#[async_trait]
pub trait SandboxRecoveryStore: Send + Sync + 'static {
    async fn scan_expired_sandbox_leases(
        &self,
        command: ScanExpiredSandboxLeases,
    ) -> Result<SafetyScanPage<ExpiredSandboxLease>, SandboxRecoveryStoreFailure>;

    async fn recover_expired_sandbox_lease(
        &self,
        command: RecoverExpiredSandboxLease,
    ) -> Result<CommandOutcome<SandboxLeaseRecoveryResult>, SandboxRecoveryStoreFailure>;
}

#[async_trait]
impl SandboxRecoveryStore for PgRepository {
    async fn scan_expired_sandbox_leases(
        &self,
        command: ScanExpiredSandboxLeases,
    ) -> Result<SafetyScanPage<ExpiredSandboxLease>, SandboxRecoveryStoreFailure> {
        PgRepository::scan_expired_sandbox_leases(self, command)
            .await
            .map_err(classify_repository_failure)
    }

    async fn recover_expired_sandbox_lease(
        &self,
        command: RecoverExpiredSandboxLease,
    ) -> Result<CommandOutcome<SandboxLeaseRecoveryResult>, SandboxRecoveryStoreFailure> {
        SandboxLeaseRecoveryAuthority::recover_expired_sandbox_lease(self, command)
            .await
            .map_err(classify_repository_failure)
    }
}

fn classify_repository_failure(failure: RepositoryError) -> SandboxRecoveryStoreFailure {
    match failure {
        RepositoryError::Database(_) => SandboxRecoveryStoreFailure::Unavailable,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired => SandboxRecoveryStoreFailure::FirstWinnerLost,
        RepositoryError::InvalidInput(_)
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => SandboxRecoveryStoreFailure::InvariantViolation,
    }
}

#[async_trait]
pub trait SandboxExpiredLeaseRecoverer: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn recover(
        &self,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryAction, Self::Error>;
}

#[async_trait]
impl SandboxExpiredLeaseRecoverer for SandboxExecutorHost {
    type Error = SandboxBackendHostError;

    async fn recover(
        &self,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryAction, Self::Error> {
        self.recover_expired_lease(expired).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxRecoveryConfig {
    shard: SafetyScanShard,
    batch_size: u16,
    timing: SafetyDriverTiming,
    receipt_ttl: Duration,
}

impl SandboxRecoveryConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        shard: SafetyScanShard,
        timing: SafetyDriverTiming,
        receipt_ttl: Duration,
    ) -> Result<Self, SandboxRecoveryConfigError> {
        profile
            .validate()
            .map_err(|failure| SandboxRecoveryConfigError::InvalidProfile(failure.to_string()))?;
        let batch_size = u16::try_from(profile.control_data.recovery_batch.q1_default)
            .map_err(|_| SandboxRecoveryConfigError::BatchOutsideRepresentation)?;
        let maximum_shards = u16::try_from(profile.control_data.recovery_shards.hard_max)
            .map_err(|_| SandboxRecoveryConfigError::ShardOutsideRepresentation)?;
        let config = Self {
            shard,
            batch_size,
            timing,
            receipt_ttl,
        };
        config.validate(maximum_shards)?;
        Ok(config)
    }

    fn validate(self, maximum_shards: u16) -> Result<(), SandboxRecoveryConfigError> {
        if self.batch_size == 0
            || self.shard.count == 0
            || self.shard.count > maximum_shards
            || self.shard.index >= self.shard.count
            || self.timing.scan_interval.is_zero()
            || self.timing.failure_backoff.is_zero()
            || self.timing.scan_jitter >= self.timing.scan_interval
            || self.timing.failure_backoff > self.timing.scan_interval
            || self.receipt_ttl.is_zero()
            || ChronoDuration::from_std(self.receipt_ttl).is_err()
        {
            return Err(SandboxRecoveryConfigError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum SandboxRecoveryConfigError {
    InvalidProfile(String),
    BatchOutsideRepresentation,
    ShardOutsideRepresentation,
    InvalidConfig,
}

impl fmt::Display for SandboxRecoveryConfigError {
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
            Self::InvalidConfig => formatter.write_str("invalid Sandbox recovery configuration"),
        }
    }
}

impl Error for SandboxRecoveryConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SandboxRecoveryLifecycle {
    Running = 1,
    Stopping = 2,
    Stopped = 3,
}

impl SandboxRecoveryLifecycle {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Running,
            2 => Self::Stopping,
            _ => Self::Stopped,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SandboxRecoveryCycleReport {
    pub candidates: u64,
    pub requeued: u64,
    pub timed_out: u64,
    pub lost: u64,
    pub replayed: u64,
    pub first_winner_lost: u64,
    pub backend_failures: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxRecoverySnapshot {
    pub lifecycle: SandboxRecoveryLifecycle,
    pub cycles: u64,
    pub scan_failures: u64,
    pub capacity_skips: u64,
    pub candidates: u64,
    pub recovered: u64,
    pub replayed: u64,
    pub first_winner_lost: u64,
    pub backend_failures: u64,
    pub shard_wraps: u64,
}

struct SandboxRecoveryMetrics {
    lifecycle: AtomicU8,
    cycles: AtomicU64,
    scan_failures: AtomicU64,
    capacity_skips: AtomicU64,
    candidates: AtomicU64,
    recovered: AtomicU64,
    replayed: AtomicU64,
    first_winner_lost: AtomicU64,
    backend_failures: AtomicU64,
    shard_wraps: AtomicU64,
}

impl SandboxRecoveryMetrics {
    fn new() -> Self {
        Self {
            lifecycle: AtomicU8::new(SandboxRecoveryLifecycle::Running as u8),
            cycles: AtomicU64::new(0),
            scan_failures: AtomicU64::new(0),
            capacity_skips: AtomicU64::new(0),
            candidates: AtomicU64::new(0),
            recovered: AtomicU64::new(0),
            replayed: AtomicU64::new(0),
            first_winner_lost: AtomicU64::new(0),
            backend_failures: AtomicU64::new(0),
            shard_wraps: AtomicU64::new(0),
        }
    }
}

pub struct SandboxRecoveryDriver<S, B, I> {
    store: Arc<S>,
    backend: Arc<B>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    config: SandboxRecoveryConfig,
}

impl<S, B, I> SandboxRecoveryDriver<S, B, I>
where
    S: SandboxRecoveryStore,
    B: SandboxExpiredLeaseRecoverer,
    I: CoordinatorIdentityFactory + 'static,
{
    pub fn new(
        store: Arc<S>,
        backend: Arc<B>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        config: SandboxRecoveryConfig,
    ) -> Result<Self, SandboxRecoveryDriverError> {
        let pool = pools.snapshot();
        if pool.work_class != WorkClass::Sandbox {
            return Err(SandboxRecoveryDriverError::WrongWorkerManifest);
        }
        if pool.critical_control_capacity == 0 {
            return Err(SandboxRecoveryDriverError::NoCriticalControlCapacity);
        }
        Ok(Self {
            store,
            backend,
            identities,
            pools,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        after: Option<SafetyScanCursor>,
    ) -> Result<
        (
            SafetyScanPage<ExpiredSandboxLease>,
            SandboxRecoveryCycleReport,
        ),
        SandboxRecoveryDriverError,
    > {
        let permit = self
            .pools
            .try_acquire_critical_control()
            .map_err(SandboxRecoveryDriverError::LocalPool)?;
        let Some(_permit) = permit else {
            return Err(SandboxRecoveryDriverError::CriticalControlCapacityBusy);
        };
        let page = self
            .store
            .scan_expired_sandbox_leases(ScanExpiredSandboxLeases {
                shard: self.config.shard,
                after,
                limit: self.config.batch_size,
            })
            .await
            .map_err(SandboxRecoveryDriverError::Store)?;
        let mut report = SandboxRecoveryCycleReport {
            candidates: page.records.len() as u64,
            ..SandboxRecoveryCycleReport::default()
        };
        for expired in &page.records {
            let action = match self.backend.recover(expired.clone()).await {
                Ok(action) => action,
                Err(_) => {
                    report.backend_failures = report.backend_failures.saturating_add(1);
                    continue;
                }
            };
            let command = self.recovery_command(expired, action)?;
            match self.store.recover_expired_sandbox_lease(command).await {
                Ok(outcome) => {
                    let (result, replayed) = match outcome {
                        CommandOutcome::Applied(result) => (result, false),
                        CommandOutcome::Replayed(result) => (result, true),
                    };
                    if replayed {
                        report.replayed = report.replayed.saturating_add(1);
                    }
                    match result.disposition {
                        SandboxLeaseRecoveryDisposition::Requeued => {
                            report.requeued = report.requeued.saturating_add(1)
                        }
                        SandboxLeaseRecoveryDisposition::TimedOut => {
                            report.timed_out = report.timed_out.saturating_add(1)
                        }
                        SandboxLeaseRecoveryDisposition::Lost => {
                            report.lost = report.lost.saturating_add(1)
                        }
                    }
                }
                Err(SandboxRecoveryStoreFailure::FirstWinnerLost) => {
                    report.first_winner_lost = report.first_winner_lost.saturating_add(1);
                }
                Err(failure) => return Err(SandboxRecoveryDriverError::Store(failure)),
            }
        }
        Ok((page, report))
    }

    fn recovery_command(
        &self,
        expired: &ExpiredSandboxLease,
        action: SandboxLeaseRecoveryAction,
    ) -> Result<RecoverExpiredSandboxLease, SandboxRecoveryDriverError> {
        let receipt_ttl = ChronoDuration::from_std(self.config.receipt_ttl)
            .map_err(|_| SandboxRecoveryDriverError::TimingOverflow)?;
        let terminal = !matches!(action, SandboxLeaseRecoveryAction::RequeueUnstarted { .. });
        let mut quota_entry_ids = if terminal {
            (0..SANDBOX_QUOTA_LINES)
                .map(|_| {
                    self.identities
                        .new_resource_id(ResourceKind::QuotaLedgerEntry)
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(SandboxRecoveryDriverError::Identity)?
        } else {
            Vec::new()
        };
        quota_entry_ids.sort();
        let mut command = RecoverExpiredSandboxLease {
            audit: SandboxRecoveryAudit {
                tenant_id: expired.tenant_id.clone(),
                recovery_process_generation_id: self.pools.snapshot().worker_process_generation_id,
                receipt_id: self.new_id(ResourceKind::Receipt)?,
                event_id: self.new_id(ResourceKind::Event)?,
                outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                idempotency_key_digest: self
                    .identities
                    .new_lease_token_digest()
                    .map_err(SandboxRecoveryDriverError::Identity)?,
                request_digest: self
                    .identities
                    .new_lease_token_digest()
                    .map_err(SandboxRecoveryDriverError::Identity)?,
                receipt_expires_at: Utc::now()
                    .checked_add_signed(receipt_ttl)
                    .ok_or(SandboxRecoveryDriverError::TimingOverflow)?,
            },
            sandbox_job_id: expired.sandbox_job_id.clone(),
            invocation_id: expired.invocation_id.clone(),
            job_id: expired.job_id.clone(),
            sandbox_request_digest: expired.request.request_digest.clone(),
            observed_job_version: expired.observed_job_version,
            observed_lease_generation: expired.observed_lease_generation,
            previous_worker_process_generation_id: expired
                .previous_worker_process_generation_id
                .clone(),
            action,
            quota_entry_ids,
        };
        command.audit.idempotency_key_digest = command
            .canonical_idempotency_key_digest()
            .map_err(|_| SandboxRecoveryDriverError::InvalidGeneratedCommand)?;
        command.audit.request_digest = command
            .canonical_request_digest()
            .map_err(|_| SandboxRecoveryDriverError::InvalidGeneratedCommand)?;
        command
            .validate_at(Utc::now())
            .map_err(|_| SandboxRecoveryDriverError::InvalidGeneratedCommand)?;
        Ok(command)
    }

    fn new_id(&self, kind: ResourceKind) -> Result<ResourceId, SandboxRecoveryDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(SandboxRecoveryDriverError::Identity)
    }

    pub fn spawn(self) -> RunningSandboxRecoveryDriver {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let metrics = Arc::new(SandboxRecoveryMetrics::new());
        let task_metrics = Arc::clone(&metrics);
        let join = tokio::spawn(async move { self.run(task_cancellation, task_metrics).await });
        RunningSandboxRecoveryDriver {
            cancellation,
            metrics,
            join: Some(join),
        }
    }

    async fn run(
        self,
        cancellation: CancellationToken,
        metrics: Arc<SandboxRecoveryMetrics>,
    ) -> Result<SandboxRecoverySnapshot, SandboxRecoveryDriverError> {
        let period = self
            .config
            .timing
            .scan_interval
            .checked_add(deterministic_recovery_jitter(
                &self.pools.snapshot().worker_process_generation_id,
                self.config.timing.scan_jitter,
            ))
            .ok_or(SandboxRecoveryDriverError::TimingOverflow)?;
        let mut timer = tokio::time::interval_at(Instant::now(), period);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut cursor = None;
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = timer.tick() => {
                    metrics.cycles.fetch_add(1, Ordering::Relaxed);
                    match self.drive_once(cursor.clone()).await {
                        Ok((page, report)) => {
                            cursor = page.next_cursor;
                            if page.exhausted {
                                metrics.shard_wraps.fetch_add(1, Ordering::Relaxed);
                            }
                            observe_report(&metrics, report);
                        }
                        Err(SandboxRecoveryDriverError::CriticalControlCapacityBusy) => {
                            metrics.capacity_skips.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(SandboxRecoveryDriverError::Store(SandboxRecoveryStoreFailure::Unavailable)) => {
                            metrics.scan_failures.fetch_add(1, Ordering::Relaxed);
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(self.config.timing.failure_backoff) => {}
                            }
                        }
                        Err(failure) => return Err(failure),
                    }
                }
            }
        }
        metrics
            .lifecycle
            .store(SandboxRecoveryLifecycle::Stopping as u8, Ordering::Release);
        metrics
            .lifecycle
            .store(SandboxRecoveryLifecycle::Stopped as u8, Ordering::Release);
        Ok(sandbox_recovery_snapshot(&metrics))
    }
}

fn observe_report(metrics: &SandboxRecoveryMetrics, report: SandboxRecoveryCycleReport) {
    metrics
        .candidates
        .fetch_add(report.candidates, Ordering::Relaxed);
    metrics.recovered.fetch_add(
        report
            .requeued
            .saturating_add(report.timed_out)
            .saturating_add(report.lost),
        Ordering::Relaxed,
    );
    metrics
        .replayed
        .fetch_add(report.replayed, Ordering::Relaxed);
    metrics
        .first_winner_lost
        .fetch_add(report.first_winner_lost, Ordering::Relaxed);
    metrics
        .backend_failures
        .fetch_add(report.backend_failures, Ordering::Relaxed);
}

fn deterministic_recovery_jitter(worker_id: &ResourceId, maximum: Duration) -> Duration {
    if maximum.is_zero() {
        return Duration::ZERO;
    }
    let maximum_nanos = maximum.as_nanos();
    let sample = u128::from(worker_id.uuid().as_u128() as u64);
    let nanos = sample % (maximum_nanos + 1);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn sandbox_recovery_snapshot(metrics: &SandboxRecoveryMetrics) -> SandboxRecoverySnapshot {
    SandboxRecoverySnapshot {
        lifecycle: SandboxRecoveryLifecycle::from_u8(metrics.lifecycle.load(Ordering::Acquire)),
        cycles: metrics.cycles.load(Ordering::Relaxed),
        scan_failures: metrics.scan_failures.load(Ordering::Relaxed),
        capacity_skips: metrics.capacity_skips.load(Ordering::Relaxed),
        candidates: metrics.candidates.load(Ordering::Relaxed),
        recovered: metrics.recovered.load(Ordering::Relaxed),
        replayed: metrics.replayed.load(Ordering::Relaxed),
        first_winner_lost: metrics.first_winner_lost.load(Ordering::Relaxed),
        backend_failures: metrics.backend_failures.load(Ordering::Relaxed),
        shard_wraps: metrics.shard_wraps.load(Ordering::Relaxed),
    }
}

#[derive(Debug)]
pub enum SandboxRecoveryDriverError {
    WrongWorkerManifest,
    NoCriticalControlCapacity,
    CriticalControlCapacityBusy,
    TimingOverflow,
    InvalidGeneratedCommand,
    Identity(IdentityFactoryError),
    LocalPool(LocalWorkerPoolError),
    Store(SandboxRecoveryStoreFailure),
    DriverTask(JoinError),
}

impl fmt::Display for SandboxRecoveryDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongWorkerManifest => {
                formatter.write_str("Sandbox recovery requires a Sandbox WorkerManifest")
            }
            Self::NoCriticalControlCapacity => {
                formatter.write_str("Sandbox recovery requires reserved critical-control capacity")
            }
            Self::CriticalControlCapacityBusy => {
                formatter.write_str("Sandbox critical-control capacity is busy")
            }
            Self::TimingOverflow => formatter.write_str("Sandbox recovery timing overflowed"),
            Self::InvalidGeneratedCommand => {
                formatter.write_str("Sandbox recovery generated an invalid command")
            }
            Self::Identity(failure) => failure.fmt(formatter),
            Self::LocalPool(failure) => failure.fmt(formatter),
            Self::Store(failure) => failure.fmt(formatter),
            Self::DriverTask(failure) => {
                write!(formatter, "Sandbox recovery task failed: {failure}")
            }
        }
    }
}

impl Error for SandboxRecoveryDriverError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(failure) => Some(failure),
            Self::LocalPool(failure) => Some(failure),
            Self::Store(failure) => Some(failure),
            Self::DriverTask(failure) => Some(failure),
            _ => None,
        }
    }
}

pub struct RunningSandboxRecoveryDriver {
    cancellation: CancellationToken,
    metrics: Arc<SandboxRecoveryMetrics>,
    join: Option<JoinHandle<Result<SandboxRecoverySnapshot, SandboxRecoveryDriverError>>>,
}

impl RunningSandboxRecoveryDriver {
    pub fn snapshot(&self) -> SandboxRecoverySnapshot {
        sandbox_recovery_snapshot(&self.metrics)
    }

    pub fn request_stop(&self) {
        self.cancellation.cancel();
    }

    pub async fn shutdown(mut self) -> Result<SandboxRecoverySnapshot, SandboxRecoveryDriverError> {
        self.request_stop();
        let join = self
            .join
            .take()
            .expect("running Sandbox recovery driver owns exactly one task");
        join.await.map_err(SandboxRecoveryDriverError::DriverTask)?
    }
}

impl Drop for RunningSandboxRecoveryDriver {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UuidCoordinatorIdentityFactory;
    use insight_platform_contracts::{checked_in_hard_limit_profile, Sha256Digest, WorkerManifest};
    use insight_platform_worker::{ClaimBatchHardLimit, ClaimedJobIdentity};
    use std::{convert::Infallible, sync::Mutex};

    #[derive(Default)]
    struct EmptyStore {
        scans: Mutex<Vec<Option<SafetyScanCursor>>>,
    }

    #[async_trait]
    impl SandboxRecoveryStore for EmptyStore {
        async fn scan_expired_sandbox_leases(
            &self,
            command: ScanExpiredSandboxLeases,
        ) -> Result<SafetyScanPage<ExpiredSandboxLease>, SandboxRecoveryStoreFailure> {
            self.scans.lock().unwrap().push(command.after);
            Ok(SafetyScanPage {
                records: Vec::new(),
                next_cursor: None,
                exhausted: true,
            })
        }

        async fn recover_expired_sandbox_lease(
            &self,
            _command: RecoverExpiredSandboxLease,
        ) -> Result<CommandOutcome<SandboxLeaseRecoveryResult>, SandboxRecoveryStoreFailure>
        {
            unreachable!("an empty scan cannot submit recovery")
        }
    }

    struct UnusedBackend;

    #[async_trait]
    impl SandboxExpiredLeaseRecoverer for UnusedBackend {
        type Error = Infallible;

        async fn recover(
            &self,
            _expired: ExpiredSandboxLease,
        ) -> Result<SandboxLeaseRecoveryAction, Self::Error> {
            unreachable!("an empty scan cannot invoke the backend")
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
                worker_role: "sandbox-recovery".to_owned(),
                work_class: WorkClass::Sandbox,
                adapter_runtime_digest: digest('a'),
                protocol_version: 1,
                max_concurrency: 1,
                critical_control_reserved_slots: 1,
            },
            id(ResourceKind::WorkerProcessGeneration, "0021"),
        )
        .unwrap()
    }

    fn config() -> SandboxRecoveryConfig {
        SandboxRecoveryConfig {
            shard: SafetyScanShard::whole(),
            batch_size: 1,
            timing: SafetyDriverTiming {
                scan_interval: Duration::from_millis(100),
                scan_jitter: Duration::ZERO,
                failure_backoff: Duration::from_millis(10),
            },
            receipt_ttl: Duration::from_secs(300),
        }
    }

    #[tokio::test]
    async fn reserved_control_capacity_scans_when_sandbox_business_capacity_is_saturated() {
        let pools = pools();
        let business = pools
            .reserve_claim_capacity(
                WorkClass::Sandbox,
                1,
                ClaimBatchHardLimit::from_profile(&checked_in_hard_limit_profile()).unwrap(),
            )
            .unwrap()
            .unwrap();
        let _active = business
            .bind_claimed_jobs(vec![ClaimedJobIdentity {
                job_id: id(ResourceKind::Job, "0022"),
                lease_generation: 1,
            }])
            .unwrap();
        assert_eq!(pools.snapshot().business_available, 0);
        let store = Arc::new(EmptyStore::default());
        let driver = SandboxRecoveryDriver::new(
            Arc::clone(&store),
            Arc::new(UnusedBackend),
            Arc::new(UuidCoordinatorIdentityFactory),
            pools,
            config(),
        )
        .unwrap();
        let (page, report) = driver.drive_once(None).await.unwrap();
        assert!(page.exhausted);
        assert_eq!(report, SandboxRecoveryCycleReport::default());
        assert_eq!(store.scans.lock().unwrap().len(), 1);
    }

    #[test]
    fn config_is_derived_from_the_shared_recovery_limits() {
        let profile = checked_in_hard_limit_profile();
        let config = SandboxRecoveryConfig::from_profile(
            &profile,
            SafetyScanShard::whole(),
            SafetyDriverTiming {
                scan_interval: Duration::from_secs(1),
                scan_jitter: Duration::from_millis(50),
                failure_backoff: Duration::from_millis(100),
            },
            Duration::from_secs(300),
        )
        .unwrap();
        assert_eq!(
            u64::from(config.batch_size),
            profile.control_data.recovery_batch.q1_default
        );
    }
}
