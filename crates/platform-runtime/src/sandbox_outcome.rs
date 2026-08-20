//! Durable Capability-owner convergence for terminal Sandbox Jobs.

use crate::{CoordinatorIdentityFactory, IdentityFactoryError, SafetyDriverTiming};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{CommandOutcome, HardLimitProfile, ResourceKind, WorkClass};
use insight_platform_postgres::{
    repository::{
        PgRepository, RepositoryError, SafetyScanCursor, SafetyScanPage, SafetyScanShard,
    },
    sandbox_repository::ScanPendingSandboxCapabilityOutcomes,
};
use insight_platform_sandbox::{
    MergeSandboxCapabilityOutcome, PendingSandboxCapabilityOutcome, SandboxOutcomeMergeAudit,
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
    sync::Notify,
    task::{JoinError, JoinHandle},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxOutcomeStoreFailure {
    Unavailable,
    FirstWinnerLost,
    InvariantViolation,
}

impl fmt::Display for SandboxOutcomeStoreFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "Sandbox outcome store is unavailable",
            Self::FirstWinnerLost => "Sandbox outcome merge lost the first-winner race",
            Self::InvariantViolation => "Sandbox outcome store invariant failed",
        })
    }
}

impl Error for SandboxOutcomeStoreFailure {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxOutcomeMergeDisposition {
    Applied,
    Replayed,
}

#[async_trait]
pub trait SandboxOutcomeStore: Send + Sync + 'static {
    async fn scan_pending_sandbox_capability_outcomes(
        &self,
        command: ScanPendingSandboxCapabilityOutcomes,
    ) -> Result<SafetyScanPage<PendingSandboxCapabilityOutcome>, SandboxOutcomeStoreFailure>;

    async fn merge_sandbox_capability_outcome(
        &self,
        command: MergeSandboxCapabilityOutcome,
    ) -> Result<SandboxOutcomeMergeDisposition, SandboxOutcomeStoreFailure>;
}

#[async_trait]
impl SandboxOutcomeStore for PgRepository {
    async fn scan_pending_sandbox_capability_outcomes(
        &self,
        command: ScanPendingSandboxCapabilityOutcomes,
    ) -> Result<SafetyScanPage<PendingSandboxCapabilityOutcome>, SandboxOutcomeStoreFailure> {
        PgRepository::scan_pending_sandbox_capability_outcomes(self, command)
            .await
            .map_err(classify_repository_failure)
    }

    async fn merge_sandbox_capability_outcome(
        &self,
        command: MergeSandboxCapabilityOutcome,
    ) -> Result<SandboxOutcomeMergeDisposition, SandboxOutcomeStoreFailure> {
        PgRepository::merge_sandbox_capability_outcome(self, command)
            .await
            .map(|outcome| match outcome {
                CommandOutcome::Applied(_) => SandboxOutcomeMergeDisposition::Applied,
                CommandOutcome::Replayed(_) => SandboxOutcomeMergeDisposition::Replayed,
            })
            .map_err(classify_repository_failure)
    }
}

fn classify_repository_failure(failure: RepositoryError) -> SandboxOutcomeStoreFailure {
    match failure {
        RepositoryError::Database(_) => SandboxOutcomeStoreFailure::Unavailable,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired => SandboxOutcomeStoreFailure::FirstWinnerLost,
        RepositoryError::InvalidInput(_)
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => SandboxOutcomeStoreFailure::InvariantViolation,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxOutcomeConfig {
    shard: SafetyScanShard,
    batch_size: u16,
    timing: SafetyDriverTiming,
    receipt_ttl: Duration,
}

impl SandboxOutcomeConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        shard: SafetyScanShard,
        timing: SafetyDriverTiming,
        receipt_ttl: Duration,
    ) -> Result<Self, SandboxOutcomeConfigError> {
        profile
            .validate()
            .map_err(|failure| SandboxOutcomeConfigError::InvalidProfile(failure.to_string()))?;
        let batch_size = u16::try_from(profile.control_data.recovery_batch.q1_default)
            .map_err(|_| SandboxOutcomeConfigError::BatchOutsideRepresentation)?;
        let maximum_shards = u16::try_from(profile.control_data.recovery_shards.hard_max)
            .map_err(|_| SandboxOutcomeConfigError::ShardOutsideRepresentation)?;
        let config = Self {
            shard,
            batch_size,
            timing,
            receipt_ttl,
        };
        config.validate(maximum_shards)?;
        Ok(config)
    }

    fn validate(self, maximum_shards: u16) -> Result<(), SandboxOutcomeConfigError> {
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
            return Err(SandboxOutcomeConfigError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum SandboxOutcomeConfigError {
    InvalidProfile(String),
    BatchOutsideRepresentation,
    ShardOutsideRepresentation,
    InvalidConfig,
}

impl fmt::Display for SandboxOutcomeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProfile(message) => {
                write!(formatter, "invalid HardLimitProfile: {message}")
            }
            Self::BatchOutsideRepresentation => {
                formatter.write_str("outcome scan batch does not fit u16")
            }
            Self::ShardOutsideRepresentation => {
                formatter.write_str("outcome scan shard count does not fit u16")
            }
            Self::InvalidConfig => formatter.write_str("invalid Sandbox outcome configuration"),
        }
    }
}

impl Error for SandboxOutcomeConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SandboxOutcomeLifecycle {
    Running = 1,
    Stopping = 2,
    Stopped = 3,
}

impl SandboxOutcomeLifecycle {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Running,
            2 => Self::Stopping,
            _ => Self::Stopped,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SandboxOutcomeCycleReport {
    pub candidates: u64,
    pub merged: u64,
    pub replayed: u64,
    pub first_winner_lost: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxOutcomeSnapshot {
    pub lifecycle: SandboxOutcomeLifecycle,
    pub cycles: u64,
    pub hinted_cycles: u64,
    pub scan_failures: u64,
    pub capacity_skips: u64,
    pub candidates: u64,
    pub merged: u64,
    pub replayed: u64,
    pub first_winner_lost: u64,
    pub shard_wraps: u64,
}

struct SandboxOutcomeMetrics {
    lifecycle: AtomicU8,
    cycles: AtomicU64,
    hinted_cycles: AtomicU64,
    scan_failures: AtomicU64,
    capacity_skips: AtomicU64,
    candidates: AtomicU64,
    merged: AtomicU64,
    replayed: AtomicU64,
    first_winner_lost: AtomicU64,
    shard_wraps: AtomicU64,
}

impl SandboxOutcomeMetrics {
    fn new() -> Self {
        Self {
            lifecycle: AtomicU8::new(SandboxOutcomeLifecycle::Running as u8),
            cycles: AtomicU64::new(0),
            hinted_cycles: AtomicU64::new(0),
            scan_failures: AtomicU64::new(0),
            capacity_skips: AtomicU64::new(0),
            candidates: AtomicU64::new(0),
            merged: AtomicU64::new(0),
            replayed: AtomicU64::new(0),
            first_winner_lost: AtomicU64::new(0),
            shard_wraps: AtomicU64::new(0),
        }
    }
}

pub struct SandboxOutcomeDriver<S, I> {
    store: Arc<S>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    config: SandboxOutcomeConfig,
    wake: Arc<Notify>,
}

impl<S, I> SandboxOutcomeDriver<S, I>
where
    S: SandboxOutcomeStore,
    I: CoordinatorIdentityFactory + 'static,
{
    pub fn new(
        store: Arc<S>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        config: SandboxOutcomeConfig,
    ) -> Result<Self, SandboxOutcomeDriverError> {
        let pool = pools.snapshot();
        if pool.work_class != WorkClass::Sandbox {
            return Err(SandboxOutcomeDriverError::WrongWorkerManifest);
        }
        if pool.critical_control_capacity == 0 {
            return Err(SandboxOutcomeDriverError::NoCriticalControlCapacity);
        }
        Ok(Self {
            store,
            identities,
            pools,
            config,
            wake: Arc::new(Notify::new()),
        })
    }

    pub async fn drive_once(
        &self,
        after: Option<SafetyScanCursor>,
    ) -> Result<
        (
            SafetyScanPage<PendingSandboxCapabilityOutcome>,
            SandboxOutcomeCycleReport,
        ),
        SandboxOutcomeDriverError,
    > {
        let permit = self
            .pools
            .try_acquire_critical_control()
            .map_err(SandboxOutcomeDriverError::LocalPool)?;
        let Some(_permit) = permit else {
            return Err(SandboxOutcomeDriverError::CriticalControlCapacityBusy);
        };
        let page = self
            .store
            .scan_pending_sandbox_capability_outcomes(ScanPendingSandboxCapabilityOutcomes {
                shard: self.config.shard,
                after,
                limit: self.config.batch_size,
            })
            .await
            .map_err(SandboxOutcomeDriverError::Store)?;
        let mut report = SandboxOutcomeCycleReport {
            candidates: page.records.len() as u64,
            ..SandboxOutcomeCycleReport::default()
        };
        for candidate in &page.records {
            let command = self.merge_command(candidate)?;
            match self.store.merge_sandbox_capability_outcome(command).await {
                Ok(SandboxOutcomeMergeDisposition::Applied) => {
                    report.merged = report.merged.saturating_add(1);
                }
                Ok(SandboxOutcomeMergeDisposition::Replayed) => {
                    report.replayed = report.replayed.saturating_add(1);
                }
                Err(SandboxOutcomeStoreFailure::FirstWinnerLost) => {
                    report.first_winner_lost = report.first_winner_lost.saturating_add(1);
                }
                Err(failure) => return Err(SandboxOutcomeDriverError::Store(failure)),
            }
        }
        Ok((page, report))
    }

    fn merge_command(
        &self,
        candidate: &PendingSandboxCapabilityOutcome,
    ) -> Result<MergeSandboxCapabilityOutcome, SandboxOutcomeDriverError> {
        candidate
            .validate()
            .map_err(|_| SandboxOutcomeDriverError::InvalidSourceCandidate)?;
        let now = Utc::now();
        let receipt_ttl = ChronoDuration::from_std(self.config.receipt_ttl)
            .map_err(|_| SandboxOutcomeDriverError::TimingOverflow)?;
        let placeholder = self
            .identities
            .new_lease_token_digest()
            .map_err(SandboxOutcomeDriverError::Identity)?;
        let mut command = MergeSandboxCapabilityOutcome {
            audit: SandboxOutcomeMergeAudit {
                tenant_id: candidate.tenant_id.clone(),
                controller_process_generation_id: self
                    .pools
                    .snapshot()
                    .worker_process_generation_id,
                source_event_id: candidate.source_event_id.clone(),
                source_job_version: candidate.source_job_version,
                source_event_payload_digest: candidate.source_event_payload_digest.clone(),
                receipt_id: self.new_id(ResourceKind::Receipt)?,
                event_id: self.new_id(ResourceKind::Event)?,
                outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                idempotency_key_digest: placeholder.clone(),
                request_digest: placeholder,
                receipt_expires_at: now
                    .checked_add_signed(receipt_ttl)
                    .ok_or(SandboxOutcomeDriverError::TimingOverflow)?,
            },
            sandbox_job_id: candidate.sandbox_job_id.clone(),
            invocation_id: candidate.invocation_id.clone(),
            job_id: candidate.job_id.clone(),
            sandbox_request_digest: candidate.sandbox_request_digest.clone(),
            expected_invocation_version: candidate.expected_invocation_version,
        };
        command.audit.idempotency_key_digest = command
            .canonical_idempotency_key_digest()
            .map_err(|_| SandboxOutcomeDriverError::InvalidGeneratedCommand)?;
        command.audit.request_digest = command
            .canonical_request_digest()
            .map_err(|_| SandboxOutcomeDriverError::InvalidGeneratedCommand)?;
        command
            .validate_at(now)
            .map_err(|_| SandboxOutcomeDriverError::InvalidGeneratedCommand)?;
        Ok(command)
    }

    fn new_id(
        &self,
        kind: ResourceKind,
    ) -> Result<insight_platform_contracts::ResourceId, SandboxOutcomeDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(SandboxOutcomeDriverError::Identity)
    }

    pub fn wake_handle(&self) -> SandboxOutcomeWakeHandle {
        SandboxOutcomeWakeHandle {
            wake: Arc::clone(&self.wake),
        }
    }

    pub fn spawn(self) -> RunningSandboxOutcomeDriver {
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let metrics = Arc::new(SandboxOutcomeMetrics::new());
        let task_metrics = Arc::clone(&metrics);
        let wake = Arc::clone(&self.wake);
        let join = tokio::spawn(async move { self.run(task_cancellation, task_metrics).await });
        RunningSandboxOutcomeDriver {
            cancellation,
            metrics,
            wake,
            join: Some(join),
        }
    }

    async fn run(
        self,
        cancellation: CancellationToken,
        metrics: Arc<SandboxOutcomeMetrics>,
    ) -> Result<SandboxOutcomeSnapshot, SandboxOutcomeDriverError> {
        let period = self
            .config
            .timing
            .scan_interval
            .checked_add(deterministic_outcome_jitter(
                self.pools
                    .snapshot()
                    .worker_process_generation_id
                    .uuid()
                    .as_u128(),
                self.config.timing.scan_jitter,
            ))
            .ok_or(SandboxOutcomeDriverError::TimingOverflow)?;
        let mut timer = tokio::time::interval_at(Instant::now(), period);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut cursor = None;
        loop {
            let hinted = tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = timer.tick() => false,
                _ = self.wake.notified() => true,
            };
            metrics.cycles.fetch_add(1, Ordering::Relaxed);
            if hinted {
                metrics.hinted_cycles.fetch_add(1, Ordering::Relaxed);
                cursor = None;
            }
            match self.drive_once(cursor.clone()).await {
                Ok((page, report)) => {
                    cursor = page.next_cursor;
                    if page.exhausted {
                        metrics.shard_wraps.fetch_add(1, Ordering::Relaxed);
                    }
                    observe_outcome_report(&metrics, report);
                }
                Err(SandboxOutcomeDriverError::CriticalControlCapacityBusy) => {
                    metrics.capacity_skips.fetch_add(1, Ordering::Relaxed);
                }
                Err(SandboxOutcomeDriverError::Store(SandboxOutcomeStoreFailure::Unavailable)) => {
                    metrics.scan_failures.fetch_add(1, Ordering::Relaxed);
                    tokio::select! {
                        _ = cancellation.cancelled() => break,
                        _ = tokio::time::sleep(self.config.timing.failure_backoff) => {}
                    }
                }
                Err(failure) => return Err(failure),
            }
        }
        metrics
            .lifecycle
            .store(SandboxOutcomeLifecycle::Stopping as u8, Ordering::Release);
        metrics
            .lifecycle
            .store(SandboxOutcomeLifecycle::Stopped as u8, Ordering::Release);
        Ok(sandbox_outcome_snapshot(&metrics))
    }
}

fn deterministic_outcome_jitter(sample: u128, maximum: Duration) -> Duration {
    if maximum.is_zero() {
        return Duration::ZERO;
    }
    let nanos = sample % (maximum.as_nanos() + 1);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn observe_outcome_report(metrics: &SandboxOutcomeMetrics, report: SandboxOutcomeCycleReport) {
    metrics
        .candidates
        .fetch_add(report.candidates, Ordering::Relaxed);
    metrics.merged.fetch_add(report.merged, Ordering::Relaxed);
    metrics
        .replayed
        .fetch_add(report.replayed, Ordering::Relaxed);
    metrics
        .first_winner_lost
        .fetch_add(report.first_winner_lost, Ordering::Relaxed);
}

fn sandbox_outcome_snapshot(metrics: &SandboxOutcomeMetrics) -> SandboxOutcomeSnapshot {
    SandboxOutcomeSnapshot {
        lifecycle: SandboxOutcomeLifecycle::from_u8(metrics.lifecycle.load(Ordering::Acquire)),
        cycles: metrics.cycles.load(Ordering::Relaxed),
        hinted_cycles: metrics.hinted_cycles.load(Ordering::Relaxed),
        scan_failures: metrics.scan_failures.load(Ordering::Relaxed),
        capacity_skips: metrics.capacity_skips.load(Ordering::Relaxed),
        candidates: metrics.candidates.load(Ordering::Relaxed),
        merged: metrics.merged.load(Ordering::Relaxed),
        replayed: metrics.replayed.load(Ordering::Relaxed),
        first_winner_lost: metrics.first_winner_lost.load(Ordering::Relaxed),
        shard_wraps: metrics.shard_wraps.load(Ordering::Relaxed),
    }
}

#[derive(Debug)]
pub enum SandboxOutcomeDriverError {
    WrongWorkerManifest,
    NoCriticalControlCapacity,
    CriticalControlCapacityBusy,
    InvalidSourceCandidate,
    TimingOverflow,
    InvalidGeneratedCommand,
    Identity(IdentityFactoryError),
    LocalPool(LocalWorkerPoolError),
    Store(SandboxOutcomeStoreFailure),
    DriverTask(JoinError),
}

impl fmt::Display for SandboxOutcomeDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongWorkerManifest => {
                formatter.write_str("Sandbox outcome driver requires a Sandbox WorkerManifest")
            }
            Self::NoCriticalControlCapacity => formatter
                .write_str("Sandbox outcome driver requires reserved critical-control capacity"),
            Self::CriticalControlCapacityBusy => {
                formatter.write_str("Sandbox critical-control capacity is busy")
            }
            Self::InvalidSourceCandidate => {
                formatter.write_str("Sandbox outcome source candidate is invalid")
            }
            Self::TimingOverflow => formatter.write_str("Sandbox outcome timing overflowed"),
            Self::InvalidGeneratedCommand => {
                formatter.write_str("Sandbox outcome driver generated an invalid command")
            }
            Self::Identity(failure) => failure.fmt(formatter),
            Self::LocalPool(failure) => failure.fmt(formatter),
            Self::Store(failure) => failure.fmt(formatter),
            Self::DriverTask(failure) => {
                write!(formatter, "Sandbox outcome task failed: {failure}")
            }
        }
    }
}

impl Error for SandboxOutcomeDriverError {
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

#[derive(Clone)]
pub struct SandboxOutcomeWakeHandle {
    wake: Arc<Notify>,
}

impl SandboxOutcomeWakeHandle {
    /// Coalesces one or more committed terminal-Event hints into a prompt durable safety scan.
    pub fn notify_terminal_event(&self) {
        self.wake.notify_one();
    }
}

pub struct RunningSandboxOutcomeDriver {
    cancellation: CancellationToken,
    metrics: Arc<SandboxOutcomeMetrics>,
    wake: Arc<Notify>,
    join: Option<JoinHandle<Result<SandboxOutcomeSnapshot, SandboxOutcomeDriverError>>>,
}

impl RunningSandboxOutcomeDriver {
    pub fn snapshot(&self) -> SandboxOutcomeSnapshot {
        sandbox_outcome_snapshot(&self.metrics)
    }

    pub fn wake_handle(&self) -> SandboxOutcomeWakeHandle {
        SandboxOutcomeWakeHandle {
            wake: Arc::clone(&self.wake),
        }
    }

    pub fn request_stop(&self) {
        self.cancellation.cancel();
    }

    pub async fn shutdown(mut self) -> Result<SandboxOutcomeSnapshot, SandboxOutcomeDriverError> {
        self.request_stop();
        let join = self
            .join
            .take()
            .expect("running Sandbox outcome driver owns exactly one task");
        join.await.map_err(SandboxOutcomeDriverError::DriverTask)?
    }
}

impl Drop for RunningSandboxOutcomeDriver {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UuidCoordinatorIdentityFactory;
    use chrono::TimeZone;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, ResourceId, Sha256Digest, WorkerManifest,
    };
    use std::sync::Mutex;

    struct RecordingStore {
        candidate: PendingSandboxCapabilityOutcome,
        commands: Mutex<Vec<MergeSandboxCapabilityOutcome>>,
    }

    #[async_trait]
    impl SandboxOutcomeStore for RecordingStore {
        async fn scan_pending_sandbox_capability_outcomes(
            &self,
            _command: ScanPendingSandboxCapabilityOutcomes,
        ) -> Result<SafetyScanPage<PendingSandboxCapabilityOutcome>, SandboxOutcomeStoreFailure>
        {
            Ok(SafetyScanPage {
                records: vec![self.candidate.clone()],
                next_cursor: None,
                exhausted: true,
            })
        }

        async fn merge_sandbox_capability_outcome(
            &self,
            command: MergeSandboxCapabilityOutcome,
        ) -> Result<SandboxOutcomeMergeDisposition, SandboxOutcomeStoreFailure> {
            self.commands.lock().unwrap().push(command);
            Ok(SandboxOutcomeMergeDisposition::Applied)
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
                worker_role: "sandbox-outcome".to_owned(),
                work_class: WorkClass::Sandbox,
                adapter_runtime_digest: digest('a'),
                protocol_version: 1,
                max_concurrency: 1,
                critical_control_reserved_slots: 1,
            },
            id(ResourceKind::WorkerProcessGeneration, "0031"),
        )
        .unwrap()
    }

    fn config() -> SandboxOutcomeConfig {
        SandboxOutcomeConfig::from_profile(
            &checked_in_hard_limit_profile(),
            SafetyScanShard::whole(),
            SafetyDriverTiming {
                scan_interval: Duration::from_millis(100),
                scan_jitter: Duration::ZERO,
                failure_backoff: Duration::from_millis(10),
            },
            Duration::from_secs(300),
        )
        .unwrap()
    }

    fn candidate() -> PendingSandboxCapabilityOutcome {
        let shared_uuid_suffix = "0032";
        PendingSandboxCapabilityOutcome {
            tenant_id: id(ResourceKind::Tenant, "0033"),
            source_event_id: id(ResourceKind::Event, "0034"),
            source_job_version: 7,
            source_event_payload_digest: digest('b'),
            source_event_occurred_at: Utc.with_ymd_and_hms(2026, 8, 13, 1, 0, 0).unwrap(),
            sandbox_job_id: id(ResourceKind::Job, shared_uuid_suffix),
            invocation_id: id(ResourceKind::CapabilityInvocation, "0035"),
            job_id: id(ResourceKind::Job, shared_uuid_suffix),
            sandbox_request_digest: digest('c'),
            expected_invocation_version: 2,
            deadline: Utc.with_ymd_and_hms(2026, 8, 13, 2, 0, 0).unwrap(),
        }
    }

    #[tokio::test]
    async fn terminal_hint_is_only_an_acceleration_for_exact_durable_merge() {
        let store = Arc::new(RecordingStore {
            candidate: candidate(),
            commands: Mutex::new(Vec::new()),
        });
        let driver = SandboxOutcomeDriver::new(
            Arc::clone(&store),
            Arc::new(UuidCoordinatorIdentityFactory),
            pools(),
            config(),
        )
        .unwrap();

        driver.wake_handle().notify_terminal_event();
        let (page, report) = driver.drive_once(None).await.unwrap();

        assert!(page.exhausted);
        assert_eq!(report.candidates, 1);
        assert_eq!(report.merged, 1);
        let commands = store.commands.lock().unwrap();
        let command = commands.first().unwrap();
        assert_eq!(command.audit.source_event_id, candidate().source_event_id);
        assert_eq!(command.expected_invocation_version, 2);
        assert_eq!(
            command.audit.idempotency_key_digest,
            command.canonical_idempotency_key_digest().unwrap()
        );
        assert_eq!(
            command.audit.request_digest,
            command.canonical_request_digest().unwrap()
        );
    }
}
