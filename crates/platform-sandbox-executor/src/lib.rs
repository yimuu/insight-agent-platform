//! Dedicated Sandbox Executor process composition.
//!
//! This role is the only process allowed to combine an untrusted-code backend with local claim
//! capacity. It deliberately has no SQL, object-store, general HTTP, or Secret Manager client;
//! production authority and broker access are injected through authenticated internal ports.

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    HardLimitProfile, ResourceId, ResourceIdError, ResourceKind, Sha256Digest, WorkClass,
};
use insight_platform_sandbox::{
    ClaimSandboxJobs, ClaimedManagedMcpSandboxSession, ClaimedSandboxJob,
    ExecuteManagedMcpSandboxSession, ExecuteSandboxJob, ManagedMcpSandboxSessionClaimAuthority,
    ManagedMcpSandboxSessionWorkerAudits, SandboxClaimAuthority, SandboxClaimFailure,
    SandboxExecutionAuthority, SandboxExecutionControlRouter, SandboxExecutorWorker,
    SandboxWorkerAuditBundle, SandboxWorkerCommitIdentity, SANDBOX_QUOTA_LINES,
};
use insight_platform_worker::{
    ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPoolError, LocalWorkerPools,
};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, error::Error, fmt, sync::Arc, time::Duration};
use tokio::{
    task::{JoinError, JoinSet},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub trait ExecutorIdentityFactory: Send + Sync {
    fn new_resource_id(&self, kind: ResourceKind) -> Result<ResourceId, ExecutorIdentityError>;
    fn new_lease_token_digest(&self) -> Result<Sha256Digest, ExecutorIdentityError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UuidExecutorIdentityFactory;

impl ExecutorIdentityFactory for UuidExecutorIdentityFactory {
    fn new_resource_id(&self, kind: ResourceKind) -> Result<ResourceId, ExecutorIdentityError> {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).map_err(ExecutorIdentityError::ResourceId)
    }

    fn new_lease_token_digest(&self) -> Result<Sha256Digest, ExecutorIdentityError> {
        let mut hasher = Sha256::new();
        hasher.update(b"insight.platform/v1/sandbox-executor/lease-token\0");
        hasher.update(Uuid::new_v4().as_bytes());
        format!("sha256:{}", lower_hex(&hasher.finalize()))
            .parse()
            .map_err(|_| ExecutorIdentityError::Digest)
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Debug)]
pub enum ExecutorIdentityError {
    ResourceId(ResourceIdError),
    Digest,
}

impl fmt::Display for ExecutorIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceId(failure) => failure.fmt(formatter),
            Self::Digest => formatter.write_str("generated Executor digest is invalid"),
        }
    }
}

impl Error for ExecutorIdentityError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxExecutionDisposition {
    Settled,
    Abandoned,
}

#[async_trait]
pub trait SandboxJobCommandExecutor: Send + Sync + 'static {
    async fn execute(&self, command: ExecuteSandboxJob) -> SandboxExecutionDisposition;
}

pub struct RegisteredSandboxJobExecutor<A> {
    worker: Arc<SandboxExecutorWorker<A>>,
    control_router: Arc<SandboxExecutionControlRouter>,
}

impl<A> RegisteredSandboxJobExecutor<A> {
    pub fn new(
        worker: Arc<SandboxExecutorWorker<A>>,
        control_router: Arc<SandboxExecutionControlRouter>,
    ) -> Self {
        Self {
            worker,
            control_router,
        }
    }
}

#[async_trait]
impl<A> SandboxJobCommandExecutor for RegisteredSandboxJobExecutor<A>
where
    A: SandboxExecutionAuthority + 'static,
    A::Error: Send,
{
    async fn execute(&self, command: ExecuteSandboxJob) -> SandboxExecutionDisposition {
        match self
            .worker
            .execute_registered(command, self.control_router.as_ref())
            .await
        {
            Ok(_) => SandboxExecutionDisposition::Settled,
            Err(_) => SandboxExecutionDisposition::Abandoned,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxExecutorDriverTiming {
    pub safety_scan_interval: Duration,
    pub claim_failure_backoff: Duration,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxExecutorDriverConfig {
    requested_claim_size: usize,
    claim_batch_hard_limit: ClaimBatchHardLimit,
    lease_milliseconds: u64,
    receipt_ttl: Duration,
    minimum_receipt_ttl: Duration,
    timing: SandboxExecutorDriverTiming,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxExecutorBinding {
    pub isolation_backend_contract_digest: Sha256Digest,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_route: insight_platform_sandbox::NodeAttestorRoute,
}

impl SandboxExecutorDriverConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        receipt_ttl: Duration,
        timing: SandboxExecutorDriverTiming,
    ) -> Result<Self, SandboxExecutorDriverError> {
        profile
            .validate()
            .map_err(|failure| SandboxExecutorDriverError::InvalidConfig(failure.to_string()))?;
        let config = Self {
            requested_claim_size: usize::try_from(profile.run_scheduler.claim_batch.q1_default)
                .map_err(|_| {
                    SandboxExecutorDriverError::InvalidConfig(
                        "claim batch exceeds address space".to_owned(),
                    )
                })?,
            claim_batch_hard_limit: ClaimBatchHardLimit::from_profile(profile)
                .map_err(SandboxExecutorDriverError::LocalPool)?,
            lease_milliseconds: profile.run_scheduler.lease_milliseconds.q1_default,
            receipt_ttl,
            minimum_receipt_ttl: Duration::from_secs(
                profile
                    .capability_sandbox
                    .wall_seconds
                    .hard_max
                    .saturating_add(profile.capability_sandbox.cleanup_seconds.hard_max),
            ),
            timing,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(self) -> Result<(), SandboxExecutorDriverError> {
        if self.requested_claim_size == 0
            || self.lease_milliseconds == 0
            || self.receipt_ttl.is_zero()
            || self.receipt_ttl <= self.minimum_receipt_ttl
            || ChronoDuration::from_std(self.receipt_ttl).is_err()
            || self.timing.safety_scan_interval.is_zero()
            || self.timing.claim_failure_backoff.is_zero()
            || self.timing.claim_failure_backoff > self.timing.safety_scan_interval
            || self.timing.drain_grace.is_zero()
        {
            return Err(SandboxExecutorDriverError::InvalidConfig(
                "Sandbox Executor timing is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SandboxExecutorCycleReport {
    pub claimed: u64,
    pub settled: u64,
    pub abandoned: u64,
}

pub struct SandboxExecutorDriver<S, E, I> {
    claim_authority: Arc<S>,
    executor: Arc<E>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    binding: SandboxExecutorBinding,
    config: SandboxExecutorDriverConfig,
}

impl<S, E, I> SandboxExecutorDriver<S, E, I>
where
    S: SandboxClaimAuthority,
    E: SandboxJobCommandExecutor,
    I: ExecutorIdentityFactory + 'static,
{
    pub fn new(
        claim_authority: Arc<S>,
        executor: Arc<E>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        binding: SandboxExecutorBinding,
        config: SandboxExecutorDriverConfig,
    ) -> Result<Self, SandboxExecutorDriverError> {
        config.validate()?;
        let pool = pools.snapshot();
        if pool.work_class != WorkClass::Sandbox || pool.critical_control_capacity == 0 {
            return Err(SandboxExecutorDriverError::WrongWorkerManifest);
        }
        Ok(Self {
            claim_authority,
            executor,
            identities,
            pools,
            binding,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<SandboxExecutionDisposition>,
    ) -> Result<usize, SandboxExecutorDriverError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Sandbox,
                self.config.requested_claim_size,
                self.config.claim_batch_hard_limit,
            )
            .map_err(SandboxExecutorDriverError::LocalPool)?
        else {
            return Ok(0);
        };
        let claim_limit = reservation.claim_limit();
        let lease_token_digests = (0..claim_limit)
            .map(|_| self.identities.new_lease_token_digest())
            .collect::<Result<Vec<_>, _>>()
            .map_err(SandboxExecutorDriverError::Identity)?;
        let expected_tokens = lease_token_digests.iter().cloned().collect::<BTreeSet<_>>();
        let claimed = self
            .claim_authority
            .claim_sandbox_jobs(ClaimSandboxJobs {
                worker_process_generation_id: self.pools.snapshot().worker_process_generation_id,
                worker_manifest_digest: self.pools.snapshot().worker_manifest_digest,
                isolation_backend_contract_digest: self
                    .binding
                    .isolation_backend_contract_digest
                    .clone(),
                executor_identity_digest: self.binding.executor_identity_digest.clone(),
                attestor_route: self.binding.attestor_route.clone(),
                limit: u16::try_from(claim_limit)
                    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?,
                lease_milliseconds: self.config.lease_milliseconds,
                lease_token_digests,
            })
            .await
            .map_err(SandboxExecutorDriverError::Claim)?;
        if claimed.is_empty() {
            return Ok(0);
        }
        let pool = self.pools.snapshot();
        let identities = validate_claims(&claimed, &pool, &expected_tokens)?;
        let permits = reservation
            .bind_claimed_jobs(identities)
            .map_err(SandboxExecutorDriverError::LocalPool)?;
        if permits.len() != claimed.len() {
            return Err(SandboxExecutorDriverError::CorruptClaim);
        }
        let count = claimed.len();
        for (claimed, permit) in claimed.into_iter().zip(permits) {
            let command = self.execution_command(claimed)?;
            let executor = Arc::clone(&self.executor);
            active.spawn(async move {
                let _permit = permit;
                executor.execute(command).await
            });
        }
        Ok(count)
    }

    fn execution_command(
        &self,
        claimed: ClaimedSandboxJob,
    ) -> Result<ExecuteSandboxJob, SandboxExecutorDriverError> {
        let expires_at = Utc::now()
            .checked_add_signed(
                ChronoDuration::from_std(self.config.receipt_ttl)
                    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?,
            )
            .ok_or(SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        let worker_process_generation_id = self.pools.snapshot().worker_process_generation_id;
        let audits = SandboxWorkerAuditBundle {
            preparing: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
            starting: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
            running: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
            collecting: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
            cancelling: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
            terminal: self.commit_identity(
                &claimed.request.tenant_id,
                &worker_process_generation_id,
                expires_at,
            )?,
        };
        let mut quota_entry_ids = (0..SANDBOX_QUOTA_LINES)
            .map(|_| {
                self.identities
                    .new_resource_id(ResourceKind::QuotaLedgerEntry)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(SandboxExecutorDriverError::Identity)?;
        quota_entry_ids.sort();
        Ok(ExecuteSandboxJob {
            executor_identity_digest: self.binding.executor_identity_digest.clone(),
            attestor_route: self.binding.attestor_route.clone(),
            request: claimed.request,
            fence: claimed.fence,
            audits,
            usage_reservation_id: claimed.usage_reservation_id,
            quota_entry_ids,
        })
    }

    fn commit_identity(
        &self,
        tenant_id: &ResourceId,
        worker_process_generation_id: &ResourceId,
        receipt_expires_at: chrono::DateTime<Utc>,
    ) -> Result<SandboxWorkerCommitIdentity, SandboxExecutorDriverError> {
        Ok(SandboxWorkerCommitIdentity {
            tenant_id: tenant_id.clone(),
            worker_process_generation_id: worker_process_generation_id.clone(),
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(SandboxExecutorDriverError::Identity)?,
            receipt_expires_at,
        })
    }

    fn new_id(&self, kind: ResourceKind) -> Result<ResourceId, SandboxExecutorDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(SandboxExecutorDriverError::Identity)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<SandboxExecutorCycleReport, SandboxExecutorDriverError> {
        let mut active = JoinSet::new();
        let mut report = SandboxExecutorCycleReport::default();
        let mut scan =
            tokio::time::interval_at(Instant::now(), self.config.timing.safety_scan_interval);
        scan.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => {
                    observe_join(joined, &mut report);
                }
                _ = self.pools.wait_for_business_capacity(), if self.pools.snapshot().business_available == 0 => {}
                _ = scan.tick() => {
                    match self.drive_once(&mut active).await {
                        Ok(claimed) => report.claimed = report.claimed.saturating_add(claimed as u64),
                        Err(SandboxExecutorDriverError::Claim(SandboxClaimFailure::Unavailable | SandboxClaimFailure::FirstWinnerLost)) => {
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(self.config.timing.claim_failure_backoff) => {}
                            }
                        }
                        Err(failure) => return Err(failure),
                    }
                }
            }
        }
        let drain = async {
            while let Some(joined) = active.join_next().await {
                observe_join(Some(joined), &mut report);
            }
        };
        if tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .is_err()
        {
            return Err(SandboxExecutorDriverError::DrainRequiresProcessTermination);
        }
        Ok(report)
    }
}

/// Long-lived Managed MCP session executor. The future MUST remain pending while the exact
/// activated session owns physical resources; the driver retains the shared Sandbox permit until
/// this future reports a durable terminal/recovery disposition.
#[async_trait]
pub trait ManagedMcpSandboxSessionCommandExecutor: Send + Sync + 'static {
    async fn execute(
        &self,
        command: ExecuteManagedMcpSandboxSession,
        cancellation: CancellationToken,
    ) -> SandboxExecutionDisposition;
}

/// Dedicated claim loop for Managed MCP subscription sessions. It shares `LocalWorkerPools` with
/// finite Sandbox execution, but uses the closed Managed-session claim authority so neither lane
/// can decode the other's payload shape.
pub struct ManagedMcpSandboxSessionExecutorDriver<S, E, I> {
    claim_authority: Arc<S>,
    executor: Arc<E>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    binding: SandboxExecutorBinding,
    config: SandboxExecutorDriverConfig,
}

impl<S, E, I> ManagedMcpSandboxSessionExecutorDriver<S, E, I>
where
    S: ManagedMcpSandboxSessionClaimAuthority,
    E: ManagedMcpSandboxSessionCommandExecutor,
    I: ExecutorIdentityFactory + 'static,
{
    pub fn new(
        claim_authority: Arc<S>,
        executor: Arc<E>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        binding: SandboxExecutorBinding,
        config: SandboxExecutorDriverConfig,
    ) -> Result<Self, SandboxExecutorDriverError> {
        config.validate()?;
        let pool = pools.snapshot();
        if pool.work_class != WorkClass::Sandbox || pool.critical_control_capacity == 0 {
            return Err(SandboxExecutorDriverError::WrongWorkerManifest);
        }
        Ok(Self {
            claim_authority,
            executor,
            identities,
            pools,
            binding,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<SandboxExecutionDisposition>,
        cancellation: &CancellationToken,
    ) -> Result<usize, SandboxExecutorDriverError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Sandbox,
                self.config.requested_claim_size,
                self.config.claim_batch_hard_limit,
            )
            .map_err(SandboxExecutorDriverError::LocalPool)?
        else {
            return Ok(0);
        };
        let claim_limit = reservation.claim_limit();
        let lease_token_digests = (0..claim_limit)
            .map(|_| self.identities.new_lease_token_digest())
            .collect::<Result<Vec<_>, _>>()
            .map_err(SandboxExecutorDriverError::Identity)?;
        let expected_tokens = lease_token_digests.iter().cloned().collect::<BTreeSet<_>>();
        let claimed = self
            .claim_authority
            .claim_managed_mcp_sandbox_sessions(ClaimSandboxJobs {
                worker_process_generation_id: self.pools.snapshot().worker_process_generation_id,
                worker_manifest_digest: self.pools.snapshot().worker_manifest_digest,
                isolation_backend_contract_digest: self
                    .binding
                    .isolation_backend_contract_digest
                    .clone(),
                executor_identity_digest: self.binding.executor_identity_digest.clone(),
                attestor_route: self.binding.attestor_route.clone(),
                limit: u16::try_from(claim_limit)
                    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?,
                lease_milliseconds: self.config.lease_milliseconds,
                lease_token_digests,
            })
            .await
            .map_err(SandboxExecutorDriverError::Claim)?;
        if claimed.is_empty() {
            return Ok(0);
        }
        let pool = self.pools.snapshot();
        let identities =
            validate_managed_mcp_session_claims(&claimed, &pool, &expected_tokens, &self.binding)?;
        let permits = reservation
            .bind_claimed_jobs(identities)
            .map_err(SandboxExecutorDriverError::LocalPool)?;
        if permits.len() != claimed.len() {
            return Err(SandboxExecutorDriverError::CorruptClaim);
        }
        let count = claimed.len();
        for (claimed, permit) in claimed.into_iter().zip(permits) {
            let command = self.execution_command(claimed)?;
            let executor = Arc::clone(&self.executor);
            let cancellation = cancellation.child_token();
            active.spawn(async move {
                let _permit = permit;
                executor.execute(command, cancellation).await
            });
        }
        Ok(count)
    }

    fn execution_command(
        &self,
        claimed: ClaimedManagedMcpSandboxSession,
    ) -> Result<ExecuteManagedMcpSandboxSession, SandboxExecutorDriverError> {
        let expires_at = Utc::now()
            .checked_add_signed(
                ChronoDuration::from_std(self.config.receipt_ttl)
                    .map_err(|_| SandboxExecutorDriverError::InvalidGeneratedCommand)?,
            )
            .ok_or(SandboxExecutorDriverError::InvalidGeneratedCommand)?;
        let tenant_id = claimed.request.identity.tenant_id.clone();
        let worker_process_generation_id = self.pools.snapshot().worker_process_generation_id;
        Ok(ExecuteManagedMcpSandboxSession {
            claimed,
            executor_identity_digest: self.binding.executor_identity_digest.clone(),
            attestor_route: self.binding.attestor_route.clone(),
            audits: ManagedMcpSandboxSessionWorkerAudits {
                preparing: self.commit_identity(
                    &tenant_id,
                    &worker_process_generation_id,
                    expires_at,
                )?,
                starting: self.commit_identity(
                    &tenant_id,
                    &worker_process_generation_id,
                    expires_at,
                )?,
                ready: self.commit_identity(
                    &tenant_id,
                    &worker_process_generation_id,
                    expires_at,
                )?,
            },
        })
    }

    fn commit_identity(
        &self,
        tenant_id: &ResourceId,
        worker_process_generation_id: &ResourceId,
        receipt_expires_at: chrono::DateTime<Utc>,
    ) -> Result<SandboxWorkerCommitIdentity, SandboxExecutorDriverError> {
        Ok(SandboxWorkerCommitIdentity {
            tenant_id: tenant_id.clone(),
            worker_process_generation_id: worker_process_generation_id.clone(),
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(SandboxExecutorDriverError::Identity)?,
            receipt_expires_at,
        })
    }

    fn new_id(&self, kind: ResourceKind) -> Result<ResourceId, SandboxExecutorDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(SandboxExecutorDriverError::Identity)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<SandboxExecutorCycleReport, SandboxExecutorDriverError> {
        let mut active = JoinSet::new();
        let mut report = SandboxExecutorCycleReport::default();
        let mut scan =
            tokio::time::interval_at(Instant::now(), self.config.timing.safety_scan_interval);
        scan.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => {
                    observe_join(joined, &mut report);
                }
                _ = self.pools.wait_for_business_capacity(), if self.pools.snapshot().business_available == 0 => {}
                _ = scan.tick() => {
                    match self.drive_once(&mut active, &cancellation).await {
                        Ok(claimed) => report.claimed = report.claimed.saturating_add(claimed as u64),
                        Err(SandboxExecutorDriverError::Claim(SandboxClaimFailure::Unavailable | SandboxClaimFailure::FirstWinnerLost)) => {
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(self.config.timing.claim_failure_backoff) => {}
                            }
                        }
                        Err(failure) => return Err(failure),
                    }
                }
            }
        }
        let drain = async {
            while let Some(joined) = active.join_next().await {
                observe_join(Some(joined), &mut report);
            }
        };
        if tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .is_err()
        {
            return Err(SandboxExecutorDriverError::DrainRequiresProcessTermination);
        }
        Ok(report)
    }
}

fn validate_managed_mcp_session_claims(
    claimed: &[ClaimedManagedMcpSandboxSession],
    pool: &insight_platform_worker::LocalWorkerPoolSnapshot,
    expected_tokens: &BTreeSet<Sha256Digest>,
    binding: &SandboxExecutorBinding,
) -> Result<Vec<ClaimedJobIdentity>, SandboxExecutorDriverError> {
    if claimed.len() > expected_tokens.len() {
        return Err(SandboxExecutorDriverError::CorruptClaim);
    }
    let limits = insight_platform_sandbox::SandboxCommandLimits::from_profile(
        &insight_platform_contracts::checked_in_hard_limit_profile(),
    )
    .map_err(|_| SandboxExecutorDriverError::CorruptClaim)?;
    let mut jobs = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut identities = Vec::with_capacity(claimed.len());
    for claim in claimed {
        claim
            .validate_at(Utc::now(), limits)
            .map_err(|_| SandboxExecutorDriverError::CorruptClaim)?;
        if claim.fence.worker_process_generation_id != pool.worker_process_generation_id
            || claim.request.executor_worker_manifest_digest != pool.worker_manifest_digest
            || claim.request.isolation_backend_contract_digest
                != binding.isolation_backend_contract_digest
            || !expected_tokens.contains(&claim.fence.token_digest)
            || !jobs.insert(claim.request.identity.physical_job_id.clone())
            || !tokens.insert(claim.fence.token_digest.clone())
        {
            return Err(SandboxExecutorDriverError::CorruptClaim);
        }
        identities.push(ClaimedJobIdentity {
            job_id: claim.request.identity.physical_job_id.clone(),
            lease_generation: claim.fence.lease_generation,
        });
    }
    Ok(identities)
}

fn validate_claims(
    claimed: &[ClaimedSandboxJob],
    pool: &insight_platform_worker::LocalWorkerPoolSnapshot,
    expected_tokens: &BTreeSet<Sha256Digest>,
) -> Result<Vec<ClaimedJobIdentity>, SandboxExecutorDriverError> {
    if claimed.len() > expected_tokens.len() {
        return Err(SandboxExecutorDriverError::CorruptClaim);
    }
    let limits = insight_platform_sandbox::SandboxCommandLimits::from_profile(
        &insight_platform_contracts::checked_in_hard_limit_profile(),
    )
    .map_err(|_| SandboxExecutorDriverError::CorruptClaim)?;
    let mut jobs = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut identities = Vec::with_capacity(claimed.len());
    for claim in claimed {
        claim
            .validate_at(Utc::now(), limits)
            .map_err(|_| SandboxExecutorDriverError::CorruptClaim)?;
        if claim.fence.worker_process_generation_id != pool.worker_process_generation_id
            || claim.request.executor_worker_manifest_digest != pool.worker_manifest_digest
            || !expected_tokens.contains(&claim.fence.token_digest)
            || !jobs.insert(claim.request.job_id.clone())
            || !tokens.insert(claim.fence.token_digest.clone())
        {
            return Err(SandboxExecutorDriverError::CorruptClaim);
        }
        identities.push(ClaimedJobIdentity {
            job_id: claim.request.job_id.clone(),
            lease_generation: claim.fence.lease_generation,
        });
    }
    Ok(identities)
}

fn observe_join(
    joined: Option<Result<SandboxExecutionDisposition, JoinError>>,
    report: &mut SandboxExecutorCycleReport,
) {
    match joined {
        Some(Ok(SandboxExecutionDisposition::Settled)) => {
            report.settled = report.settled.saturating_add(1)
        }
        Some(Ok(SandboxExecutionDisposition::Abandoned)) | Some(Err(_)) | None => {
            report.abandoned = report.abandoned.saturating_add(1)
        }
    }
}

#[derive(Debug)]
pub enum SandboxExecutorDriverError {
    InvalidConfig(String),
    WrongWorkerManifest,
    InvalidGeneratedCommand,
    CorruptClaim,
    DrainRequiresProcessTermination,
    Identity(ExecutorIdentityError),
    LocalPool(LocalWorkerPoolError),
    Claim(SandboxClaimFailure),
}

impl fmt::Display for SandboxExecutorDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig(_) => "Sandbox Executor driver configuration is invalid",
            Self::WrongWorkerManifest => "Sandbox Executor worker manifest is invalid",
            Self::InvalidGeneratedCommand => "Sandbox Executor generated an invalid command",
            Self::CorruptClaim => "Sandbox claim response is inconsistent",
            Self::DrainRequiresProcessTermination => {
                "Sandbox drain grace expired; process-generation termination is required"
            }
            Self::Identity(_) => "Sandbox Executor identity generation failed",
            Self::LocalPool(_) => "Sandbox Executor local pool failed",
            Self::Claim(_) => "Sandbox claim authority failed",
        })
    }
}

impl Error for SandboxExecutorDriverError {}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, WorkerManifest, WORKER_MANIFEST_VERSION,
        WORKER_PROTOCOL_VERSION,
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct EmptyAuthority {
        claim: Mutex<Option<ClaimSandboxJobs>>,
    }

    #[async_trait]
    impl SandboxClaimAuthority for EmptyAuthority {
        async fn claim_sandbox_jobs(
            &self,
            command: ClaimSandboxJobs,
        ) -> Result<Vec<ClaimedSandboxJob>, SandboxClaimFailure> {
            *self.claim.lock().unwrap() = Some(command);
            Ok(Vec::new())
        }
    }

    struct UnusedExecutor;

    #[async_trait]
    impl SandboxJobCommandExecutor for UnusedExecutor {
        async fn execute(&self, _command: ExecuteSandboxJob) -> SandboxExecutionDisposition {
            panic!("empty authority must not dispatch")
        }
    }

    #[derive(Default)]
    struct EmptyManagedSessionAuthority {
        claim: Mutex<Option<ClaimSandboxJobs>>,
    }

    #[async_trait]
    impl ManagedMcpSandboxSessionClaimAuthority for EmptyManagedSessionAuthority {
        async fn claim_managed_mcp_sandbox_sessions(
            &self,
            command: ClaimSandboxJobs,
        ) -> Result<Vec<ClaimedManagedMcpSandboxSession>, SandboxClaimFailure> {
            *self.claim.lock().unwrap() = Some(command);
            Ok(Vec::new())
        }
    }

    struct UnusedManagedSessionExecutor;

    #[async_trait]
    impl ManagedMcpSandboxSessionCommandExecutor for UnusedManagedSessionExecutor {
        async fn execute(
            &self,
            _command: ExecuteManagedMcpSandboxSession,
            _cancellation: CancellationToken,
        ) -> SandboxExecutionDisposition {
            panic!("empty authority must not dispatch")
        }
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn pools() -> LocalWorkerPools {
        LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: "sandbox-executor.wasi".to_owned(),
                work_class: WorkClass::Sandbox,
                adapter_runtime_digest: digest('a'),
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 2,
                critical_control_reserved_slots: 1,
            },
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap(),
        )
        .unwrap()
    }

    fn micro_vm_pools() -> LocalWorkerPools {
        LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: "sandbox-executor.microvm".to_owned(),
                work_class: WorkClass::Sandbox,
                adapter_runtime_digest: digest('a'),
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 2,
                critical_control_reserved_slots: 1,
            },
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn claim_is_bounded_by_local_capacity_before_authority_access() {
        let authority = Arc::new(EmptyAuthority::default());
        let local_pools = pools();
        let generation = local_pools.snapshot().worker_process_generation_id;
        let worker_manifest_digest = local_pools.snapshot().worker_manifest_digest;
        let driver = SandboxExecutorDriver::new(
            Arc::clone(&authority),
            Arc::new(UnusedExecutor),
            Arc::new(UuidExecutorIdentityFactory),
            local_pools.clone(),
            SandboxExecutorBinding {
                isolation_backend_contract_digest: digest('c'),
                executor_identity_digest: digest('d'),
                attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            },
            SandboxExecutorDriverConfig::from_profile(
                &checked_in_hard_limit_profile(),
                Duration::from_secs(7_200),
                SandboxExecutorDriverTiming {
                    safety_scan_interval: Duration::from_millis(100),
                    claim_failure_backoff: Duration::from_millis(10),
                    drain_grace: Duration::from_secs(1),
                },
            )
            .unwrap(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert_eq!(driver.drive_once(&mut active).await.unwrap(), 0);
        assert!(active.is_empty());
        let claim = authority.claim.lock().unwrap().clone().unwrap();
        assert_eq!(claim.worker_process_generation_id, generation);
        assert_eq!(claim.worker_manifest_digest, worker_manifest_digest);
        assert_eq!(claim.isolation_backend_contract_digest, digest('c'));
        assert_eq!(claim.attestor_route.as_str(), "https://10.0.0.7:9443");
        assert_eq!(claim.limit, 2);
        assert_eq!(claim.lease_token_digests.len(), 2);
        assert_ne!(claim.lease_token_digests[0], claim.lease_token_digests[1]);
        assert_eq!(local_pools.snapshot().business_available, 2);
    }

    #[tokio::test]
    async fn managed_session_claim_uses_the_same_bounded_sandbox_pool_and_closed_lane() {
        let authority = Arc::new(EmptyManagedSessionAuthority::default());
        let local_pools = micro_vm_pools();
        let generation = local_pools.snapshot().worker_process_generation_id;
        let worker_manifest_digest = local_pools.snapshot().worker_manifest_digest;
        let driver = ManagedMcpSandboxSessionExecutorDriver::new(
            Arc::clone(&authority),
            Arc::new(UnusedManagedSessionExecutor),
            Arc::new(UuidExecutorIdentityFactory),
            local_pools.clone(),
            SandboxExecutorBinding {
                isolation_backend_contract_digest: digest('c'),
                executor_identity_digest: digest('d'),
                attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            },
            SandboxExecutorDriverConfig::from_profile(
                &checked_in_hard_limit_profile(),
                Duration::from_secs(7_200),
                SandboxExecutorDriverTiming {
                    safety_scan_interval: Duration::from_millis(100),
                    claim_failure_backoff: Duration::from_millis(10),
                    drain_grace: Duration::from_secs(1),
                },
            )
            .unwrap(),
        )
        .unwrap();
        let mut active = JoinSet::new();
        assert_eq!(
            driver
                .drive_once(&mut active, &CancellationToken::new())
                .await
                .unwrap(),
            0
        );
        assert!(active.is_empty());
        let claim = authority.claim.lock().unwrap().clone().unwrap();
        assert_eq!(claim.worker_process_generation_id, generation);
        assert_eq!(claim.worker_manifest_digest, worker_manifest_digest);
        assert_eq!(claim.isolation_backend_contract_digest, digest('c'));
        assert_eq!(claim.limit, 2);
        assert_eq!(claim.lease_token_digests.len(), 2);
        assert_eq!(local_pools.snapshot().business_available, 2);
    }

    #[test]
    fn driver_rejects_non_sandbox_worker_manifest() {
        let wrong = LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: WORKER_MANIFEST_VERSION,
                worker_role: "model-worker".to_owned(),
                work_class: WorkClass::Model,
                adapter_runtime_digest: digest('b'),
                protocol_version: WORKER_PROTOCOL_VERSION,
                max_concurrency: 1,
                critical_control_reserved_slots: 1,
            },
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
                .unwrap(),
        )
        .unwrap();
        let result = SandboxExecutorDriver::new(
            Arc::new(EmptyAuthority::default()),
            Arc::new(UnusedExecutor),
            Arc::new(UuidExecutorIdentityFactory),
            wrong,
            SandboxExecutorBinding {
                isolation_backend_contract_digest: digest('c'),
                executor_identity_digest: digest('d'),
                attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            },
            SandboxExecutorDriverConfig::from_profile(
                &checked_in_hard_limit_profile(),
                Duration::from_secs(7_200),
                SandboxExecutorDriverTiming {
                    safety_scan_interval: Duration::from_secs(1),
                    claim_failure_backoff: Duration::from_millis(10),
                    drain_grace: Duration::from_secs(1),
                },
            )
            .unwrap(),
        );
        assert!(matches!(
            result,
            Err(SandboxExecutorDriverError::WrongWorkerManifest)
        ));
    }
}
