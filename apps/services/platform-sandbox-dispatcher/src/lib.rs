//! Deployable CR-216 Sandbox Dispatcher driver.
//!
//! The driver reserves process-local Sandbox capacity before claiming shared Jobs. PostgreSQL
//! remains the only business authority; the OpenSandbox provider is called only through the
//! physical lifecycle port implemented by `insight-platform-sandbox`.

use insight_platform_contracts::{canonical_digest, HardLimitProfile, Sha256Digest, WorkClass};
use insight_platform_jobs::store::{SafetyScanCursor, SafetyScanPage};
use insight_platform_opensandbox_executor::dispatcher::{
    OpenSandboxDispatcher, SandboxCleanupProgressV1, SandboxDispatchProgressV1,
};
use insight_platform_sandbox::contracts::{
    CandidateCursorV1, ReconcileSandboxControlsV1, SandboxClaimV1, SandboxCleanupClaimV1,
    SandboxJobRepository, SANDBOX_CONTRACT_SCHEMA_VERSION,
};
use insight_platform_worker::{
    ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPoolError, LocalWorkerPools,
};
use std::{error::Error, fmt, sync::Arc, time::Duration};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const SANDBOX_DISPATCHER_ROLE: &str = "sandbox-dispatcher";

#[derive(Debug, Clone, Copy)]
pub struct SandboxDispatcherTiming {
    pub scan_interval: Duration,
    pub runner_poll_interval: Duration,
    pub cleanup_scan_interval: Duration,
    pub orphan_scan_interval: Duration,
    pub failure_backoff: Duration,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone)]
pub struct SandboxDispatcherConfig {
    claim_size: usize,
    claim_limit: ClaimBatchHardLimit,
    lease_milliseconds: u64,
    cleanup_lease_milliseconds: u64,
    installed_runtime_contract_digest: Sha256Digest,
    timing: SandboxDispatcherTiming,
}

impl SandboxDispatcherConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        timing: SandboxDispatcherTiming,
        installed_runtime_contract_digest: Sha256Digest,
    ) -> Result<Self, SandboxDispatcherDriverError> {
        profile
            .validate()
            .map_err(|_| SandboxDispatcherDriverError::InvalidConfiguration)?;
        let lease_milliseconds = profile.run_scheduler.lease_milliseconds.q1_default;
        let config = Self {
            claim_size: usize::try_from(profile.run_scheduler.claim_batch.q1_default)
                .map_err(|_| SandboxDispatcherDriverError::InvalidConfiguration)?,
            claim_limit: ClaimBatchHardLimit::from_profile(profile)
                .map_err(|_| SandboxDispatcherDriverError::InvalidConfiguration)?,
            lease_milliseconds,
            cleanup_lease_milliseconds: lease_milliseconds,
            installed_runtime_contract_digest,
            timing,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), SandboxDispatcherDriverError> {
        let lease = Duration::from_millis(self.lease_milliseconds);
        if self.claim_size == 0
            || self.lease_milliseconds == 0
            || self.cleanup_lease_milliseconds == 0
            || self.timing.scan_interval.is_zero()
            || self.timing.scan_interval > Duration::from_secs(60)
            || self.timing.runner_poll_interval.is_zero()
            || self.timing.runner_poll_interval >= lease / 3
            || self.timing.cleanup_scan_interval.is_zero()
            || self.timing.orphan_scan_interval.is_zero()
            || self.timing.failure_backoff.is_zero()
            || self.timing.failure_backoff > self.timing.scan_interval
            || self.timing.drain_grace.is_zero()
            || self.timing.drain_grace > Duration::from_secs(120)
        {
            return Err(SandboxDispatcherDriverError::InvalidConfiguration);
        }
        Ok(())
    }

    pub const fn lease_milliseconds(&self) -> u64 {
        self.lease_milliseconds
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SandboxDispatcherReport {
    pub controls_reconciled: u64,
    pub claimed: u64,
    pub terminal: u64,
    pub abandoned: u64,
    pub cleaned: u64,
    pub orphans_examined: u64,
    pub orphans_deleted: u64,
}

pub struct SandboxDispatcherDriver<R, P> {
    repository: Arc<R>,
    dispatcher: Arc<OpenSandboxDispatcher<R, P>>,
    pools: LocalWorkerPools,
    config: SandboxDispatcherConfig,
    cleanup_cursor: tokio::sync::Mutex<Option<SafetyScanCursor>>,
    control_cursor: tokio::sync::Mutex<Option<SafetyScanCursor>>,
}

impl<R, P> SandboxDispatcherDriver<R, P>
where
    R: SandboxJobRepository + 'static,
    R::Error: Error + Send + Sync + 'static,
    P: insight_platform_sandbox::contracts::OpenSandboxProvider + 'static,
{
    pub fn new(
        repository: Arc<R>,
        provider: Arc<P>,
        pools: LocalWorkerPools,
        config: SandboxDispatcherConfig,
    ) -> Result<Self, SandboxDispatcherDriverError> {
        config.validate()?;
        let snapshot = pools.snapshot();
        if snapshot.worker_role != SANDBOX_DISPATCHER_ROLE
            || snapshot.work_class != WorkClass::Sandbox
        {
            return Err(SandboxDispatcherDriverError::WrongWorkerManifest);
        }
        Ok(Self {
            dispatcher: Arc::new(OpenSandboxDispatcher::new(
                Arc::clone(&repository),
                provider,
            )),
            repository,
            pools,
            config,
            cleanup_cursor: tokio::sync::Mutex::new(None),
            control_cursor: tokio::sync::Mutex::new(None),
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<bool>,
        cancellation: &CancellationToken,
    ) -> Result<usize, SandboxDispatcherDriverError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Sandbox,
                self.config.claim_size,
                self.config.claim_limit,
            )
            .map_err(SandboxDispatcherDriverError::LocalCapacity)?
        else {
            return Ok(0);
        };
        let process = self.pools.snapshot();
        let limit = u16::try_from(reservation.claim_limit())
            .map_err(|_| SandboxDispatcherDriverError::InvalidGeneratedCommand)?;
        let lease_token_digests = (0..limit)
            .map(|_| new_digest())
            .collect::<Result<Vec<_>, _>>()?;
        let claims = self
            .repository
            .claim(SandboxClaimV1 {
                worker_manifest: self.pools.manifest().clone(),
                worker_process_generation_id: process.worker_process_generation_id.clone(),
                lease_token_digests,
                limit,
                lease_milliseconds: self.config.lease_milliseconds,
            })
            .await
            .map_err(|failure| SandboxDispatcherDriverError::Repository(Box::new(failure)))?;
        for claim in &claims {
            claim
                .validate()
                .map_err(|_| SandboxDispatcherDriverError::CorruptClaim)?;
            if claim.fence.worker_process_generation_id != process.worker_process_generation_id
                || claim.request.runtime_contract_digest
                    != self.config.installed_runtime_contract_digest
            {
                return Err(SandboxDispatcherDriverError::CorruptClaim);
            }
        }
        let permits = reservation
            .bind_claimed_jobs(
                claims
                    .iter()
                    .map(|claim| ClaimedJobIdentity {
                        job_id: claim.job.job_id.clone(),
                        lease_generation: claim.fence.lease_generation,
                    })
                    .collect(),
            )
            .map_err(SandboxDispatcherDriverError::LocalCapacity)?;
        if permits.len() != claims.len() {
            return Err(SandboxDispatcherDriverError::CorruptClaim);
        }
        let count = claims.len();
        for (claim, permit) in claims.into_iter().zip(permits) {
            let dispatcher = Arc::clone(&self.dispatcher);
            let config = self.config.clone();
            let cancellation = cancellation.child_token();
            active.spawn(async move {
                let _permit = permit;
                match drive_claim(dispatcher, claim, config, cancellation).await {
                    Ok(()) => true,
                    Err(failure) => {
                        eprintln!("platform-sandbox-dispatcher abandoned execution: {failure}");
                        false
                    }
                }
            });
        }
        Ok(count)
    }

    pub async fn cleanup_once(&self) -> Result<usize, SandboxDispatcherDriverError> {
        let Some(_permit) = self
            .pools
            .try_acquire_critical_control()
            .map_err(SandboxDispatcherDriverError::LocalCapacity)?
        else {
            return Ok(0);
        };
        let process = self.pools.snapshot();
        let mut cursor = self.cleanup_cursor.lock().await;
        let page = self
            .repository
            .claim_cleanup(SandboxCleanupClaimV1 {
                after: cursor.clone(),
                worker_manifest: self.pools.manifest().clone(),
                process_generation_id: process.worker_process_generation_id,
                limit: 1,
                lease_milliseconds: self.config.cleanup_lease_milliseconds,
            })
            .await
            .map_err(|failure| SandboxDispatcherDriverError::Repository(Box::new(failure)))?;
        for claim in &page.records {
            claim
                .validate()
                .map_err(|_| SandboxDispatcherDriverError::CorruptClaim)?;
            if claim.fence.process_generation_id
                != self.pools.snapshot().worker_process_generation_id
                || claim.payload.cleanup.owner_build_digest.as_ref()
                    != Some(&self.pools.manifest().worker_build_digest)
            {
                return Err(SandboxDispatcherDriverError::CorruptClaim);
            }
        }
        advance_sandbox_scan(&page, &mut cursor, 1)?;
        drop(cursor);
        let claims = page.records;
        let count = claims.len();
        for mut claim in claims {
            while let SandboxCleanupProgressV1::CandidateAbsent(next) = self
                .dispatcher
                .cleanup_once(claim)
                .await
                .map_err(|failure| SandboxDispatcherDriverError::Dispatch(failure.to_string()))?
            {
                claim = *next;
            }
        }
        Ok(count)
    }

    pub async fn control_once(&self) -> Result<usize, SandboxDispatcherDriverError> {
        let Some(_permit) = self
            .pools
            .try_acquire_critical_control()
            .map_err(SandboxDispatcherDriverError::LocalCapacity)?
        else {
            return Ok(0);
        };
        let mut cursor = self.control_cursor.lock().await;
        let page = self
            .repository
            .reconcile_controls(ReconcileSandboxControlsV1 {
                after: cursor.clone(),
                limit: 1,
            })
            .await
            .map_err(|failure| SandboxDispatcherDriverError::Repository(Box::new(failure)))?;
        for decision in &page.records {
            decision
                .validate()
                .map_err(|_| SandboxDispatcherDriverError::CorruptClaim)?;
            if decision.fence.is_some()
                || !matches!(
                    decision.job.state,
                    insight_platform_contracts::JobState::Cancelled
                        | insight_platform_contracts::JobState::TimedOut
                )
            {
                return Err(SandboxDispatcherDriverError::CorruptClaim);
            }
        }
        advance_sandbox_scan(&page, &mut cursor, 1)?;
        Ok(page.records.len())
    }

    pub async fn sweep_orphan_page(
        &self,
        cursor: CandidateCursorV1,
    ) -> Result<(u16, u16, Option<CandidateCursorV1>), SandboxDispatcherDriverError> {
        let Some(_permit) = self
            .pools
            .try_acquire_critical_control()
            .map_err(SandboxDispatcherDriverError::LocalCapacity)?
        else {
            return Ok((0, 0, Some(cursor)));
        };
        let progress = self
            .dispatcher
            .sweep_orphan_page(cursor)
            .await
            .map_err(|failure| SandboxDispatcherDriverError::Dispatch(failure.to_string()))?;
        Ok((progress.examined, progress.deleted, progress.next))
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<SandboxDispatcherReport, SandboxDispatcherDriverError> {
        let mut report = SandboxDispatcherReport::default();
        let mut active = JoinSet::new();
        let mut scan = interval(self.config.timing.scan_interval);
        let mut cleanup = interval(self.config.timing.cleanup_scan_interval);
        let mut orphan = interval(self.config.timing.orphan_scan_interval);
        let mut orphan_cursor = CandidateCursorV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            opaque: None,
        };
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => {
                    match joined {
                        Some(Ok(true)) => report.terminal = report.terminal.saturating_add(1),
                        _ => report.abandoned = report.abandoned.saturating_add(1),
                    }
                }
                _ = cleanup.tick() => {
                    match self.cleanup_once().await {
                        Ok(count) => report.cleaned = report.cleaned.saturating_add(count as u64),
                        Err(failure) => eprintln!("platform-sandbox-dispatcher cleanup scan failed: {failure}"),
                    }
                }
                _ = orphan.tick() => {
                    match self.sweep_orphan_page(orphan_cursor.clone()).await {
                        Ok((examined, deleted, next)) => {
                            report.orphans_examined = report.orphans_examined.saturating_add(u64::from(examined));
                            report.orphans_deleted = report.orphans_deleted.saturating_add(u64::from(deleted));
                            orphan_cursor = next.unwrap_or(CandidateCursorV1 {
                                schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
                                opaque: None,
                            });
                        }
                        Err(failure) => eprintln!("platform-sandbox-dispatcher orphan scan failed: {failure}"),
                    }
                }
                _ = scan.tick() => {
                    match self.control_once().await {
                        Ok(count) => {
                            report.controls_reconciled = report.controls_reconciled.saturating_add(count as u64)
                        }
                        Err(failure) => eprintln!("platform-sandbox-dispatcher control scan failed: {failure}"),
                    }
                    match self.drive_once(&mut active, &cancellation).await {
                        Ok(claimed) => report.claimed = report.claimed.saturating_add(claimed as u64),
                        Err(failure) => {
                            eprintln!("platform-sandbox-dispatcher claim scan failed: {failure}");
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(self.config.timing.failure_backoff) => {}
                            }
                        }
                    }
                }
            }
        }
        let drain = async {
            while let Some(joined) = active.join_next().await {
                match joined {
                    Ok(true) => report.terminal = report.terminal.saturating_add(1),
                    _ => report.abandoned = report.abandoned.saturating_add(1),
                }
            }
        };
        tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .map_err(|_| SandboxDispatcherDriverError::DrainRequiresTermination)?;
        Ok(report)
    }
}

async fn drive_claim<R, P>(
    dispatcher: Arc<OpenSandboxDispatcher<R, P>>,
    mut leased: insight_platform_sandbox::contracts::LeasedSandboxJobV1,
    config: SandboxDispatcherConfig,
    cancellation: CancellationToken,
) -> Result<(), String>
where
    R: SandboxJobRepository + 'static,
    R::Error: Error + Send + Sync + 'static,
    P: insight_platform_sandbox::contracts::OpenSandboxProvider + 'static,
{
    loop {
        match dispatcher
            .drive_job(leased)
            .await
            .map_err(|failure| failure.to_string())?
        {
            SandboxDispatchProgressV1::TerminalCommitted(_) => return Ok(()),
            SandboxDispatchProgressV1::AwaitingCandidate(next)
            | SandboxDispatchProgressV1::AwaitingRunner(next) => leased = *next,
        }
        tokio::select! {
            _ = cancellation.cancelled() => return Err("sandbox dispatch cancelled".to_owned()),
            _ = tokio::time::sleep(config.timing.runner_poll_interval) => {}
        }
        dispatcher
            .heartbeat(&mut leased, config.lease_milliseconds)
            .await
            .map_err(|failure| failure.to_string())?;
    }
}

fn interval(duration: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval
}

fn new_digest() -> Result<Sha256Digest, SandboxDispatcherDriverError> {
    canonical_digest(&serde_json::json!({
        "domain": "insight.sandbox.lease-token/v1",
        "nonce": Uuid::new_v4(),
    }))
    .map_err(|_| SandboxDispatcherDriverError::InvalidGeneratedCommand)?
    .parse()
    .map_err(|_| SandboxDispatcherDriverError::InvalidGeneratedCommand)
}

#[derive(Debug)]
pub enum SandboxDispatcherDriverError {
    InvalidConfiguration,
    WrongWorkerManifest,
    InvalidGeneratedCommand,
    CorruptClaim,
    LocalCapacity(LocalWorkerPoolError),
    Repository(Box<dyn Error + Send + Sync>),
    Dispatch(String),
    DrainRequiresTermination,
}

impl fmt::Display for SandboxDispatcherDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "sandbox dispatcher configuration is invalid",
            Self::WrongWorkerManifest => "sandbox dispatcher worker manifest is invalid",
            Self::InvalidGeneratedCommand => "sandbox dispatcher generated an invalid command",
            Self::CorruptClaim => "sandbox dispatcher received a corrupt claim",
            Self::LocalCapacity(_) => "sandbox dispatcher local capacity failed",
            Self::Repository(_) => "sandbox dispatcher repository failed",
            Self::Dispatch(_) => "sandbox dispatcher physical drive failed",
            Self::DrainRequiresTermination => "sandbox dispatcher drain requires termination",
        })
    }
}

impl Error for SandboxDispatcherDriverError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LocalCapacity(failure) => Some(failure),
            Self::Repository(failure) => Some(failure.as_ref()),
            _ => None,
        }
    }
}

fn advance_sandbox_scan<T>(
    page: &SafetyScanPage<T>,
    cursor: &mut Option<SafetyScanCursor>,
    limit: usize,
) -> Result<(), SandboxDispatcherDriverError> {
    use insight_platform_contracts::ResourceKind;
    if page.records.len() + page.diagnostics.len() > limit
        || page.exhausted != page.next_cursor.is_none()
        || page.diagnostics.iter().any(|diagnostic| {
            diagnostic.validate().is_err() || diagnostic.item_id.kind() != ResourceKind::Job
        })
    {
        return Err(SandboxDispatcherDriverError::CorruptClaim);
    }
    if let Some(next) = &page.next_cursor {
        next.validate(ResourceKind::Job)
            .map_err(|_| SandboxDispatcherDriverError::CorruptClaim)?;
        if cursor.as_ref().is_some_and(|previous| {
            (next.sort_at, &next.tenant_id, &next.item_id)
                <= (previous.sort_at, &previous.tenant_id, &previous.item_id)
        }) {
            return Err(SandboxDispatcherDriverError::CorruptClaim);
        }
    }
    for diagnostic in &page.diagnostics {
        eprintln!(
            "Sandbox scan retained invalid durable object: phase={:?}, code={:?}",
            diagnostic.phase, diagnostic.code
        );
    }
    *cursor = page.next_cursor.clone();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, ResourceId, ResourceKind, WorkerManifest,
        WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION,
    };

    fn timing() -> SandboxDispatcherTiming {
        SandboxDispatcherTiming {
            scan_interval: Duration::from_secs(1),
            runner_poll_interval: Duration::from_millis(100),
            cleanup_scan_interval: Duration::from_secs(2),
            orphan_scan_interval: Duration::from_secs(3),
            failure_backoff: Duration::from_millis(100),
            drain_grace: Duration::from_secs(1),
        }
    }

    #[test]
    fn timing_is_closed_and_bounded_by_the_job_lease() {
        let profile = checked_in_hard_limit_profile();
        assert!(SandboxDispatcherConfig::from_profile(&profile, timing(), digest()).is_ok());
        let mut invalid = timing();
        invalid.runner_poll_interval =
            Duration::from_millis(profile.run_scheduler.lease_milliseconds.q1_default);
        assert!(matches!(
            SandboxDispatcherConfig::from_profile(&profile, invalid, digest()),
            Err(SandboxDispatcherDriverError::InvalidConfiguration)
        ));
    }

    #[test]
    fn dispatcher_role_and_sandbox_lane_are_exact() {
        let profile = checked_in_hard_limit_profile();
        let config = SandboxDispatcherConfig::from_profile(&profile, timing(), digest()).unwrap();
        assert!(config.lease_milliseconds() > 0);
        let manifest = WorkerManifest {
            worker_build_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
            execution_capabilities: insight_platform_contracts::WorkerExecutionCapabilities {
                schema_version: 1,
                capabilities: vec![
                    insight_platform_sandbox::contracts::sandbox_cleanup_execution_capability(),
                ],
            },
            manifest_version: WORKER_MANIFEST_VERSION,
            worker_role: SANDBOX_DISPATCHER_ROLE.to_owned(),
            work_class: WorkClass::Sandbox,
            adapter_runtime_digest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .parse()
                    .unwrap(),
            protocol_version: WORKER_PROTOCOL_VERSION,
            max_concurrency: 1,
            critical_control_reserved_slots: 1,
        };
        let process =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap();
        assert!(LocalWorkerPools::new(manifest, process).is_ok());
    }

    #[test]
    fn retained_scan_pages_advance_but_invalid_continuations_do_not() {
        use insight_platform_jobs::store::{
            SafeScanDiagnostic, SafetyScanDiagnosticCode, SafetyScanPhase,
        };
        let position = SafetyScanCursor {
            sort_at: "2026-09-06T00:00:00Z".parse().unwrap(),
            tenant_id: ResourceId::from_uuid_v7(ResourceKind::Tenant, Uuid::now_v7()).unwrap(),
            item_id: ResourceId::from_uuid_v7(ResourceKind::Job, Uuid::now_v7()).unwrap(),
        };
        let page: SafetyScanPage<()> = SafetyScanPage {
            records: Vec::new(),
            diagnostics: vec![SafeScanDiagnostic {
                schema_version: 1,
                tenant_id: position.tenant_id.clone(),
                item_id: position.item_id.clone(),
                phase: SafetyScanPhase::OwnerDecode,
                code: SafetyScanDiagnosticCode::InvalidPersistedObject,
            }],
            next_cursor: Some(position.clone()),
            exhausted: false,
        };
        let mut cursor = None;
        advance_sandbox_scan(&page, &mut cursor, 1).unwrap();
        assert_eq!(cursor, Some(position.clone()));
        assert!(matches!(
            advance_sandbox_scan(&page, &mut cursor, 1),
            Err(SandboxDispatcherDriverError::CorruptClaim)
        ));
        assert_eq!(cursor, Some(position));
        let exhausted: SafetyScanPage<()> = SafetyScanPage {
            records: Vec::new(),
            diagnostics: Vec::new(),
            next_cursor: None,
            exhausted: true,
        };
        advance_sandbox_scan(&exhausted, &mut cursor, 1).unwrap();
        assert!(cursor.is_none());
    }

    fn digest() -> Sha256Digest {
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap()
    }
}
