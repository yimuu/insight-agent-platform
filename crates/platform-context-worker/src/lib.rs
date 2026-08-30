//! Independent Context Worker for bounded, statically installed NativeCatalog adapters.
//!
//! PostgreSQL remains the durable Job and ContextQuery authority. Candidate discovery is
//! read-only; exact-slot claim, heartbeat, quota settlement, and terminal commit use the shared
//! durable contracts.

pub mod dataset;

use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_context::{
    CitationLocator, ClaimContextJobs, CommitContextOutcome, ContextBackendOutcome,
    ContextCitation, ContextClaimSlot, ContextItem, ContextObservation, ContextObservationOutput,
    ContextQueryLimits, ContextRetrievalEvidence, ContextSubscriptionExecutionError,
    ContextSubscriptionRefreshBackend, ContextSubscriptionRefreshFailureClass,
    ContextSubscriptionRefreshResponse, ContextWorkerAudit, NormalizedContextScore,
    RemoteContextFailureClass, RemoteContextSearchConnector, RemoteContextSearchRequest,
    RemoteContextSearchResponse, CONTEXT_QUOTA_LINES, REMOTE_CONTEXT_PROTOCOL_VERSION,
};
use insight_platform_contracts::{
    canonical_digest, ClosedJsonValue, ContextBackendBinding, ContextBackendContract,
    ContextCitationStrength, DataClassification, ExternalLeafFailureMutationIds,
    ExternalLeafResumeMutationIds, Failure, FailureClass, FailureCode, FailureSource,
    HardLimitProfile, PlatformFailureCode, ResourceId, ResourceIdError, ResourceKind, Retryability,
    Sha256Digest, TraceFlags, ValueRef, WorkClass,
};
use insight_platform_jobs::{JobFence, LeasePolicy};
use insight_platform_postgres::{
    context_query_repository::{ClaimableNativeContextJob, ClaimedContextExecution},
    mcp_repository::{
        ClaimContextSubscriptionRefreshJobs, ClaimedContextSubscriptionRefresh,
        CommitContextSubscriptionRefresh, ContextSubscriptionRefreshClaimSlot,
        ContextSubscriptionRefreshRecoverySlot, DriveExpiredContextSubscriptionRefreshJobs,
        CONTEXT_SUBSCRIPTION_REFRESH_MAX_BATCH,
    },
    repository::{HeartbeatJob, JobFence as RepositoryJobFence, PgRepository, RepositoryError},
};
use insight_platform_rpc_trace::{scope_trace, RpcTraceContext};
use insight_platform_worker::{ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPools};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, error::Error, fmt, sync::Arc, time::Duration};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const CONTEXT_WORKER_ROLE: &str = "context-worker";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCatalogAdapter {
    pub schema_version: u32,
    pub installed_adapter_digest: Sha256Digest,
    pub adapter_contract_digest: Sha256Digest,
    pub source_item_identity_digest: Sha256Digest,
    pub content: Value,
    pub structured_fields_schema_digest: Sha256Digest,
    pub score_millionths: Option<i32>,
    pub locator_digest: Sha256Digest,
    pub authorization_evidence_digest: Sha256Digest,
    pub ranking_evidence_digest: Sha256Digest,
    pub display_label: String,
    pub classification: DataClassification,
}

impl NativeCatalogAdapter {
    pub fn validate(&self) -> Result<(), ContextWorkerError> {
        if self.schema_version != 1
            || self.display_label.is_empty()
            || self.display_label.len() > 256
            || self.display_label.chars().any(char::is_control)
            || self
                .score_millionths
                .is_some_and(|score| !(-1_000_000..=1_000_000).contains(&score))
            || serde_json::to_vec(&self.content).map_or(true, |bytes| bytes.len() > 262_144)
        {
            return Err(ContextWorkerError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ContextWorkerTiming {
    pub scan_interval: Duration,
    pub failure_backoff: Duration,
    pub drain_grace: Duration,
    pub receipt_ttl: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct ContextWorkerConfig {
    claim_size: usize,
    recovery_size: u16,
    claim_limit: ClaimBatchHardLimit,
    lease_milliseconds: u64,
    heartbeat_interval: Duration,
    timing: ContextWorkerTiming,
    limits: ContextQueryLimits,
}

impl ContextWorkerConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        timing: ContextWorkerTiming,
    ) -> Result<Self, ContextWorkerError> {
        profile
            .validate()
            .map_err(|_| ContextWorkerError::InvalidConfiguration)?;
        let config = Self {
            claim_size: usize::try_from(profile.run_scheduler.claim_batch.q1_default)
                .map_err(|_| ContextWorkerError::InvalidConfiguration)?,
            recovery_size: u16::try_from(profile.control_data.recovery_batch.q1_default)
                .map_err(|_| ContextWorkerError::InvalidConfiguration)?,
            claim_limit: ClaimBatchHardLimit::from_profile(profile)
                .map_err(|_| ContextWorkerError::InvalidConfiguration)?,
            lease_milliseconds: profile.run_scheduler.lease_milliseconds.q1_default,
            heartbeat_interval: Duration::from_millis(
                profile.run_scheduler.heartbeat_milliseconds.q1_default,
            ),
            timing,
            limits: ContextQueryLimits::from_profile(profile)
                .map_err(|_| ContextWorkerError::InvalidConfiguration)?,
        };
        if config.claim_size == 0
            || config.recovery_size == 0
            || config.lease_milliseconds == 0
            || config.heartbeat_interval.is_zero()
            || config.heartbeat_interval >= Duration::from_millis(config.lease_milliseconds / 3)
            || timing.scan_interval.is_zero()
            || timing.failure_backoff.is_zero()
            || timing.failure_backoff > timing.scan_interval
            || timing.drain_grace.is_zero()
            || timing.receipt_ttl <= Duration::from_millis(config.lease_milliseconds)
        {
            return Err(ContextWorkerError::InvalidConfiguration);
        }
        Ok(config)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContextWorkerReport {
    pub claimed: u64,
    pub settled: u64,
    pub abandoned: u64,
}

pub struct ContextWorkerDriver {
    repository: Arc<PgRepository>,
    pools: LocalWorkerPools,
    adapter: NativeCatalogAdapter,
    config: ContextWorkerConfig,
}

impl ContextWorkerDriver {
    pub fn new(
        repository: Arc<PgRepository>,
        pools: LocalWorkerPools,
        adapter: NativeCatalogAdapter,
        config: ContextWorkerConfig,
    ) -> Result<Self, ContextWorkerError> {
        adapter.validate()?;
        let snapshot = pools.snapshot();
        if snapshot.worker_role != CONTEXT_WORKER_ROLE || snapshot.work_class != WorkClass::Context
        {
            return Err(ContextWorkerError::WrongWorkerManifest);
        }
        Ok(Self {
            repository,
            pools,
            adapter,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<bool>,
    ) -> Result<usize, ContextWorkerError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Context,
                self.config.claim_size,
                self.config.claim_limit,
            )
            .map_err(|_| ContextWorkerError::LocalCapacity)?
        else {
            return Ok(0);
        };
        let candidates = self
            .repository
            .scan_claimable_native_context_jobs(
                &self.pools.snapshot().worker_manifest_digest,
                &self.adapter.installed_adapter_digest,
                &self.adapter.adapter_contract_digest,
                u16::try_from(reservation.claim_limit())
                    .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
            )
            .await?;
        if candidates.is_empty() {
            return Ok(0);
        }
        let slots = candidates
            .iter()
            .map(Self::claim_slot)
            .collect::<Result<Vec<_>, _>>()?;
        let expected_tokens = slots
            .iter()
            .map(|slot| slot.lease_token_digest.clone())
            .collect::<BTreeSet<_>>();
        let process = self.pools.snapshot();
        let claims = self
            .repository
            .claim_context_jobs(ClaimContextJobs {
                worker_process_generation_id: process.worker_process_generation_id.clone(),
                worker_manifest_digest: process.worker_manifest_digest.clone(),
                slots,
                lease_policy: LeasePolicy {
                    requested_milliseconds: self.config.lease_milliseconds,
                    hard_maximum_milliseconds: self.config.lease_milliseconds,
                },
            })
            .await?;
        let permits = reservation
            .bind_claimed_jobs(
                claims
                    .iter()
                    .map(|claim| {
                        claimed_identity(
                            claim,
                            &process.worker_process_generation_id,
                            &expected_tokens,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(|_| ContextWorkerError::CorruptClaim)?;
        if permits.len() != claims.len() {
            return Err(ContextWorkerError::CorruptClaim);
        }
        let count = claims.len();
        for (claim, permit) in claims.into_iter().zip(permits) {
            let repository = Arc::clone(&self.repository);
            let adapter = self.adapter.clone();
            let config = self.config;
            active.spawn(async move {
                let _permit = permit;
                match execute_claim(repository, adapter, config, claim).await {
                    Ok(()) => true,
                    Err(failure) => {
                        eprintln!("platform-context-worker abandoned execution: {failure}");
                        false
                    }
                }
            });
        }
        Ok(count)
    }

    pub async fn recover_once(&self) -> Result<usize, ContextWorkerError> {
        use insight_platform_postgres::{
            context_query_repository::{DriveExpiredContextJobs, ExpiredContextRecoverySlot},
            repository::SafetyScanShard,
        };
        let slots = (0..self.config.recovery_size)
            .map(|_| {
                Ok(ExpiredContextRecoverySlot {
                    quota_entry_ids: ids_array(ResourceKind::QuotaLedgerEntry)?,
                    event_id: new_id(ResourceKind::Event)?,
                    outbox_id: new_id(ResourceKind::OutboxEvent)?,
                })
            })
            .collect::<Result<Vec<_>, ContextWorkerError>>()?;
        self.repository
            .drive_expired_context_jobs(DriveExpiredContextJobs {
                shard: SafetyScanShard::whole(),
                after: None,
                limit: self.config.recovery_size,
                retry_backoff_milliseconds: u64::try_from(
                    self.config.timing.failure_backoff.as_millis(),
                )
                .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
                slots,
            })
            .await
            .map(|page| page.records.len())
            .map_err(ContextWorkerError::Repository)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<ContextWorkerReport, ContextWorkerError> {
        let mut active = JoinSet::new();
        let mut report = ContextWorkerReport::default();
        let mut scan = tokio::time::interval(self.config.timing.scan_interval);
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => {
                    match joined { Some(Ok(true)) => report.settled += 1, _ => report.abandoned += 1 }
                }
                _ = scan.tick() => {
                    match self.recover_once().await {
                        Ok(_) => {}
                        Err(failure) if failure.is_transient_repository_race() => {}
                        Err(failure) => {
                            eprintln!("platform-context-worker recovery failed: {failure}");
                            return Err(failure);
                        }
                    }
                    match self.drive_once(&mut active).await {
                        Ok(claimed) => {
                            report.claimed += u64::try_from(claimed)
                                .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?;
                        }
                        Err(failure) if failure.is_transient_repository_race() => {
                            tokio::select! {
                                _ = cancellation.cancelled() => break,
                                _ = tokio::time::sleep(self.config.timing.failure_backoff) => {}
                            }
                        }
                        Err(failure) => {
                            eprintln!("platform-context-worker claim scan failed: {failure}");
                            return Err(failure);
                        }
                    }
                }
            }
        }
        let drain = async {
            while let Some(joined) = active.join_next().await {
                match joined {
                    Ok(true) => report.settled += 1,
                    _ => report.abandoned += 1,
                }
            }
        };
        tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .map_err(|_| ContextWorkerError::DrainRequiresTermination)?;
        Ok(report)
    }

    fn claim_slot(
        candidate: &ClaimableNativeContextJob,
    ) -> Result<ContextClaimSlot, ContextWorkerError> {
        Ok(ContextClaimSlot {
            tenant_id: candidate.tenant_id.clone(),
            job_id: candidate.job_id.clone(),
            lease_token_digest: new_digest()?,
            quota_reservation_id: new_id(ResourceKind::UsageReservation)?,
            quota_entry_ids: ids_array(ResourceKind::QuotaLedgerEntry)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            resume_mutations: resume_mutations()?,
            failure_mutations: failure_mutations()?,
        })
    }
}

pub struct RemoteContextWorkerDriver {
    repository: Arc<PgRepository>,
    pools: LocalWorkerPools,
    connector: Arc<dyn RemoteContextSearchConnector>,
    config: ContextWorkerConfig,
}

impl RemoteContextWorkerDriver {
    pub fn new(
        repository: Arc<PgRepository>,
        pools: LocalWorkerPools,
        connector: Arc<dyn RemoteContextSearchConnector>,
        config: ContextWorkerConfig,
    ) -> Result<Self, ContextWorkerError> {
        let snapshot = pools.snapshot();
        if snapshot.worker_role != CONTEXT_WORKER_ROLE || snapshot.work_class != WorkClass::Context
        {
            return Err(ContextWorkerError::WrongWorkerManifest);
        }
        Ok(Self {
            repository,
            pools,
            connector,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<bool>,
    ) -> Result<usize, ContextWorkerError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Context,
                self.config.claim_size,
                self.config.claim_limit,
            )
            .map_err(|_| ContextWorkerError::LocalCapacity)?
        else {
            return Ok(0);
        };
        let process = self.pools.snapshot();
        let candidates = self
            .repository
            .scan_claimable_remote_context_jobs(
                &process.worker_manifest_digest,
                u16::try_from(reservation.claim_limit())
                    .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
            )
            .await?;
        if candidates.is_empty() {
            return Ok(0);
        }
        let slots = candidates
            .iter()
            .map(|candidate| {
                Ok(ContextClaimSlot {
                    tenant_id: candidate.tenant_id.clone(),
                    job_id: candidate.job_id.clone(),
                    lease_token_digest: new_digest()?,
                    quota_reservation_id: new_id(ResourceKind::UsageReservation)?,
                    quota_entry_ids: ids_array(ResourceKind::QuotaLedgerEntry)?,
                    event_id: new_id(ResourceKind::Event)?,
                    outbox_id: new_id(ResourceKind::OutboxEvent)?,
                    resume_mutations: resume_mutations()?,
                    failure_mutations: failure_mutations()?,
                })
            })
            .collect::<Result<Vec<_>, ContextWorkerError>>()?;
        let expected_tokens = slots
            .iter()
            .map(|slot| slot.lease_token_digest.clone())
            .collect::<BTreeSet<_>>();
        let claims = self
            .repository
            .claim_context_jobs(ClaimContextJobs {
                worker_process_generation_id: process.worker_process_generation_id.clone(),
                worker_manifest_digest: process.worker_manifest_digest.clone(),
                slots,
                lease_policy: LeasePolicy {
                    requested_milliseconds: self.config.lease_milliseconds,
                    hard_maximum_milliseconds: self.config.lease_milliseconds,
                },
            })
            .await?;
        let permits = reservation
            .bind_claimed_jobs(
                claims
                    .iter()
                    .map(|claim| {
                        claimed_identity(
                            claim,
                            &process.worker_process_generation_id,
                            &expected_tokens,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(|_| ContextWorkerError::CorruptClaim)?;
        if permits.len() != claims.len() {
            return Err(ContextWorkerError::CorruptClaim);
        }
        let count = claims.len();
        for (claim, permit) in claims.into_iter().zip(permits) {
            let repository = Arc::clone(&self.repository);
            let connector = Arc::clone(&self.connector);
            let config = self.config;
            active.spawn(async move {
                let _permit = permit;
                match execute_remote_claim(repository, connector, config, claim).await {
                    Ok(()) => true,
                    Err(failure) => {
                        eprintln!("platform-context-worker abandoned remote execution: {failure}");
                        false
                    }
                }
            });
        }
        Ok(count)
    }

    pub async fn recover_once(&self) -> Result<usize, ContextWorkerError> {
        recover_expired(&self.repository, self.config).await
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<ContextWorkerReport, ContextWorkerError> {
        let mut report = ContextWorkerReport::default();
        let mut active = JoinSet::new();
        let mut scan = tokio::time::interval(self.config.timing.scan_interval);
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => match joined {
                    Some(Ok(true)) => report.settled += 1,
                    Some(_) => report.abandoned += 1,
                    None => {}
                },
                _ = scan.tick() => {
                    if let Err(failure) = self.recover_once().await {
                        if !failure.is_transient_repository_race() {
                            return Err(failure);
                        }
                    }
                    match self.drive_once(&mut active).await {
                        Ok(claimed) => report.claimed += u64::try_from(claimed)
                            .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
                        Err(failure) if failure.is_transient_repository_race() => {
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
        let drain = async {
            while let Some(joined) = active.join_next().await {
                match joined {
                    Ok(true) => report.settled += 1,
                    _ => report.abandoned += 1,
                }
            }
        };
        tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .map_err(|_| ContextWorkerError::DrainRequiresTermination)?;
        Ok(report)
    }
}

/// Drives durable MCP resource-subscription refresh Jobs from the Context Worker while keeping
/// protocol I/O behind the credential-free MCP Host backend contract.
pub struct SubscriptionContextWorkerDriver {
    repository: Arc<PgRepository>,
    pools: LocalWorkerPools,
    backend: Arc<dyn ContextSubscriptionRefreshBackend>,
    config: ContextWorkerConfig,
}

impl SubscriptionContextWorkerDriver {
    pub fn new(
        repository: Arc<PgRepository>,
        pools: LocalWorkerPools,
        backend: Arc<dyn ContextSubscriptionRefreshBackend>,
        config: ContextWorkerConfig,
    ) -> Result<Self, ContextWorkerError> {
        let snapshot = pools.snapshot();
        if snapshot.worker_role != CONTEXT_WORKER_ROLE || snapshot.work_class != WorkClass::Context
        {
            return Err(ContextWorkerError::WrongWorkerManifest);
        }
        Ok(Self {
            repository,
            pools,
            backend,
            config,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<bool>,
    ) -> Result<usize, ContextWorkerError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                WorkClass::Context,
                self.config.claim_size,
                self.config.claim_limit,
            )
            .map_err(|_| ContextWorkerError::LocalCapacity)?
        else {
            return Ok(0);
        };
        let process = self.pools.snapshot();
        let candidates = self
            .repository
            .scan_claimable_context_subscription_refresh_jobs(
                &process.worker_manifest_digest,
                u16::try_from(reservation.claim_limit())
                    .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
            )
            .await?;
        if candidates.is_empty() {
            return Ok(0);
        }
        let slots = candidates
            .iter()
            .map(|candidate| {
                Ok(ContextSubscriptionRefreshClaimSlot {
                    tenant_id: candidate.tenant_id.clone(),
                    job_id: candidate.job_id.clone(),
                    lease_token_digest: new_digest()?,
                    quota_reservation_id: new_id(ResourceKind::UsageReservation)?,
                    quota_reserve_entry_id: new_id(ResourceKind::QuotaLedgerEntry)?,
                    event_id: new_id(ResourceKind::Event)?,
                    outbox_id: new_id(ResourceKind::OutboxEvent)?,
                })
            })
            .collect::<Result<Vec<_>, ContextWorkerError>>()?;
        let expected_tokens = slots
            .iter()
            .map(|slot| slot.lease_token_digest.clone())
            .collect::<BTreeSet<_>>();
        let claims = self
            .repository
            .claim_context_subscription_refresh_jobs(ClaimContextSubscriptionRefreshJobs {
                worker_process_generation_id: process.worker_process_generation_id.clone(),
                worker_manifest_digest: process.worker_manifest_digest.clone(),
                slots,
                lease_policy: LeasePolicy {
                    requested_milliseconds: self.config.lease_milliseconds,
                    hard_maximum_milliseconds: self.config.lease_milliseconds,
                },
            })
            .await?;
        let permits = reservation
            .bind_claimed_jobs(
                claims
                    .iter()
                    .map(|claim| {
                        claimed_subscription_identity(
                            claim,
                            &process.worker_process_generation_id,
                            &expected_tokens,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(|_| ContextWorkerError::CorruptClaim)?;
        if permits.len() != claims.len() {
            return Err(ContextWorkerError::CorruptClaim);
        }
        let count = claims.len();
        for (claim, permit) in claims.into_iter().zip(permits) {
            let repository = Arc::clone(&self.repository);
            let backend = Arc::clone(&self.backend);
            let config = self.config;
            active.spawn(async move {
                let _permit = permit;
                match execute_subscription_claim(repository, backend, config, claim).await {
                    Ok(()) => true,
                    Err(failure) => {
                        eprintln!(
                            "platform-context-worker abandoned subscription refresh: {failure}"
                        );
                        false
                    }
                }
            });
        }
        Ok(count)
    }

    pub async fn recover_once(&self) -> Result<usize, ContextWorkerError> {
        let recovery_size = self
            .config
            .recovery_size
            .min(CONTEXT_SUBSCRIPTION_REFRESH_MAX_BATCH);
        let slots = (0..recovery_size)
            .map(|_| {
                Ok(ContextSubscriptionRefreshRecoverySlot {
                    quota_settlement_entry_id: new_id(ResourceKind::QuotaLedgerEntry)?,
                    event_id: new_id(ResourceKind::Event)?,
                    outbox_id: new_id(ResourceKind::OutboxEvent)?,
                })
            })
            .collect::<Result<Vec<_>, ContextWorkerError>>()?;
        self.repository
            .drive_expired_context_subscription_refresh_jobs(
                DriveExpiredContextSubscriptionRefreshJobs {
                    shard_index: 0,
                    shard_count: 1,
                    limit: recovery_size,
                    retry_backoff_milliseconds: u64::try_from(
                        self.config.timing.failure_backoff.as_millis(),
                    )
                    .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
                    slots,
                },
            )
            .await
            .map(|jobs| jobs.len())
            .map_err(ContextWorkerError::Repository)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<ContextWorkerReport, ContextWorkerError> {
        let mut report = ContextWorkerReport::default();
        let mut active = JoinSet::new();
        let mut scan = tokio::time::interval(self.config.timing.scan_interval);
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => match joined {
                    Some(Ok(true)) => report.settled += 1,
                    Some(_) => report.abandoned += 1,
                    None => {}
                },
                _ = scan.tick() => {
                    if let Err(failure) = self.recover_once().await {
                        if !failure.is_transient_repository_race() {
                            return Err(failure);
                        }
                    }
                    match self.drive_once(&mut active).await {
                        Ok(claimed) => report.claimed += u64::try_from(claimed)
                            .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
                        Err(failure) if failure.is_transient_repository_race() => {
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
        let drain = async {
            while let Some(joined) = active.join_next().await {
                match joined {
                    Ok(true) => report.settled += 1,
                    _ => report.abandoned += 1,
                }
            }
        };
        tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .map_err(|_| ContextWorkerError::DrainRequiresTermination)?;
        Ok(report)
    }
}

async fn recover_expired(
    repository: &PgRepository,
    config: ContextWorkerConfig,
) -> Result<usize, ContextWorkerError> {
    use insight_platform_postgres::context_query_repository::{
        DriveExpiredContextJobs, ExpiredContextRecoverySlot,
    };
    let slots = (0..config.recovery_size)
        .map(|_| {
            Ok(ExpiredContextRecoverySlot {
                quota_entry_ids: ids_array(ResourceKind::QuotaLedgerEntry)?,
                event_id: new_id(ResourceKind::Event)?,
                outbox_id: new_id(ResourceKind::OutboxEvent)?,
            })
        })
        .collect::<Result<Vec<_>, ContextWorkerError>>()?;
    repository
        .drive_expired_context_jobs(DriveExpiredContextJobs {
            shard: insight_platform_postgres::repository::SafetyScanShard::whole(),
            after: None,
            limit: config.recovery_size,
            slots,
            retry_backoff_milliseconds: u64::try_from(config.timing.failure_backoff.as_millis())
                .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
        })
        .await
        .map(|page| page.records.len())
        .map_err(ContextWorkerError::from)
}

async fn execute_claim(
    repository: Arc<PgRepository>,
    adapter: NativeCatalogAdapter,
    config: ContextWorkerConfig,
    claim: ClaimedContextExecution,
) -> Result<(), ContextWorkerError> {
    let process_id = parse_id(&claim.job.worker_id, ResourceKind::WorkerProcessGeneration)?;
    let token: Sha256Digest = claim
        .job
        .lease_token_digest
        .as_deref()
        .ok_or(ContextWorkerError::CorruptClaim)?
        .parse()
        .map_err(|_| ContextWorkerError::CorruptClaim)?;
    let heartbeat = repository
        .heartbeat_job(HeartbeatJob {
            fence: RepositoryJobFence {
                tenant_id: claim.job.tenant_id.clone(),
                job_id: claim.job.job_id.clone(),
                worker_id: process_id.clone(),
                lease_epoch: claim.job.lease_epoch,
                expected_job_version: claim.job.version,
                lease_token_digest: token.clone(),
            },
            lease_milliseconds: i64::try_from(config.lease_milliseconds)
                .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
        })
        .await?;
    let fence = JobFence {
        expected_version: u64::try_from(heartbeat.version)
            .map_err(|_| ContextWorkerError::CorruptClaim)?,
        worker_process_generation_id: process_id.clone(),
        lease_generation: u64::try_from(heartbeat.lease_epoch)
            .map_err(|_| ContextWorkerError::CorruptClaim)?,
        token_digest: token,
    };
    let (outcome, resume_mutations, failure_mutations) =
        match build_output(&adapter, &claim, config.limits) {
            Ok(output) => (
                ContextBackendOutcome::Completed(Box::new(output)),
                Some(claim.resume_mutations.clone()),
                None,
            ),
            Err(ContextWorkerError::AdapterOutputRejected) => (
                ContextBackendOutcome::PermanentFailure {
                    failure: Failure {
                        code: FailureCode::Platform {
                            code: PlatformFailureCode::ContextQueryFailed,
                        },
                        class: FailureClass::Platform,
                        retryability: Retryability::Never,
                        safe_message: Some(
                            "NativeCatalog output did not satisfy the frozen Context contract"
                                .to_owned(),
                        ),
                        details_ref: None,
                        source: FailureSource::Context,
                    },
                },
                None,
                Some(claim.failure_mutations.clone()),
            ),
            Err(failure) => return Err(failure),
        };
    commit_worker_outcome(
        &repository,
        &claim,
        process_id,
        fence,
        PreparedWorkerOutcome {
            outcome,
            resume_mutations,
            failure_mutations,
        },
        config,
    )
    .await
}

struct PreparedWorkerOutcome {
    outcome: ContextBackendOutcome,
    resume_mutations: Option<ExternalLeafResumeMutationIds>,
    failure_mutations: Option<ExternalLeafFailureMutationIds>,
}

async fn commit_worker_outcome(
    repository: &PgRepository,
    claim: &ClaimedContextExecution,
    process_id: ResourceId,
    fence: JobFence,
    prepared: PreparedWorkerOutcome,
    config: ContextWorkerConfig,
) -> Result<(), ContextWorkerError> {
    let audit = ContextWorkerAudit {
        tenant_id: claim.query.tenant_id.clone(),
        worker_process_generation_id: process_id,
        receipt_id: new_id(ResourceKind::Receipt)?,
        event_id: new_id(ResourceKind::Event)?,
        outbox_id: new_id(ResourceKind::OutboxEvent)?,
        idempotency_key_digest: new_digest()?,
        request_digest: claim
            .query
            .payload
            .admission
            .request
            .normalized_query_digest
            .clone(),
        receipt_expires_at: Utc::now()
            .checked_add_signed(
                ChronoDuration::from_std(config.timing.receipt_ttl)
                    .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
            )
            .ok_or(ContextWorkerError::InvalidGeneratedCommand)?,
    };
    let mut transaction = repository.begin_context_query_transaction().await?;
    use insight_platform_context::ContextQueryTransaction as _;
    transaction
        .commit_context_outcome(CommitContextOutcome {
            audit,
            context_query_id: claim.query.context_query_id.clone(),
            expected_query_version: claim.query.version,
            job_id: parse_required_id(&claim.job.job_id, ResourceKind::Job)?,
            fence,
            outcome: prepared.outcome,
            quota_entry_ids: ids_array(ResourceKind::QuotaLedgerEntry)?,
            resume_mutations: prepared.resume_mutations,
            failure_mutations: prepared.failure_mutations,
        })
        .await?;
    transaction.commit().await?;
    Ok(())
}

async fn execute_remote_claim(
    repository: Arc<PgRepository>,
    connector: Arc<dyn RemoteContextSearchConnector>,
    config: ContextWorkerConfig,
    claim: ClaimedContextExecution,
) -> Result<(), ContextWorkerError> {
    let process_id = parse_id(&claim.job.worker_id, ResourceKind::WorkerProcessGeneration)?;
    let token: Sha256Digest = claim
        .job
        .lease_token_digest
        .as_deref()
        .ok_or(ContextWorkerError::CorruptClaim)?
        .parse()
        .map_err(|_| ContextWorkerError::CorruptClaim)?;
    let heartbeat = repository
        .heartbeat_job(HeartbeatJob {
            fence: RepositoryJobFence {
                tenant_id: claim.job.tenant_id.clone(),
                job_id: claim.job.job_id.clone(),
                worker_id: process_id.clone(),
                lease_epoch: claim.job.lease_epoch,
                expected_job_version: claim.job.version,
                lease_token_digest: token.clone(),
            },
            lease_milliseconds: i64::try_from(config.lease_milliseconds)
                .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
        })
        .await?;
    let mut fence = JobFence {
        expected_version: u64::try_from(heartbeat.version)
            .map_err(|_| ContextWorkerError::CorruptClaim)?,
        worker_process_generation_id: process_id.clone(),
        lease_generation: u64::try_from(heartbeat.lease_epoch)
            .map_err(|_| ContextWorkerError::CorruptClaim)?,
        token_digest: token.clone(),
    };
    let request = remote_request(&claim, &fence)?;
    let trace = RpcTraceContext::start(claim.job.trace, TraceFlags::NotSampled)
        .map_err(|_| ContextWorkerError::CorruptClaim)?;
    let query = scope_trace(trace, connector.query(request));
    tokio::pin!(query);
    let mut interval = tokio::time::interval(config.heartbeat_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    let remote = loop {
        tokio::select! {
            result = &mut query => break result,
            _ = interval.tick() => {
                match repository.heartbeat_job(HeartbeatJob {
                    fence: RepositoryJobFence {
                        tenant_id: claim.job.tenant_id.clone(),
                        job_id: claim.job.job_id.clone(),
                        worker_id: process_id.clone(),
                        lease_epoch: i64::try_from(fence.lease_generation)
                            .map_err(|_| ContextWorkerError::CorruptClaim)?,
                        expected_job_version: i64::try_from(fence.expected_version)
                            .map_err(|_| ContextWorkerError::CorruptClaim)?,
                        lease_token_digest: token.clone(),
                    },
                    lease_milliseconds: i64::try_from(config.lease_milliseconds)
                        .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
                }).await {
                    Ok(next) => fence.expected_version = u64::try_from(next.version)
                        .map_err(|_| ContextWorkerError::CorruptClaim)?,
                    Err(RepositoryError::Database(_)) => {}
                    Err(failure) => return Err(ContextWorkerError::Repository(failure)),
                }
            }
        }
    };
    let (outcome, resume_mutations, failure_mutations) = match remote {
        Ok(response) => (
            ContextBackendOutcome::Completed(Box::new(build_remote_output(
                response,
                &claim,
                config.limits,
            )?)),
            Some(claim.resume_mutations.clone()),
            None,
        ),
        Err(failure) => {
            failure
                .validate()
                .map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
            let retryable = matches!(
                failure.class,
                RemoteContextFailureClass::RetryableBeforeDispatch
                    | RemoteContextFailureClass::RetryableAfterDispatch
            );
            let retry_at = Utc::now().checked_add_signed(
                ChronoDuration::from_std(config.timing.failure_backoff)
                    .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
            );
            let may_retry = retryable
                && claim.job.attempt_no < claim.job.attempt_limit
                && retry_at.is_some_and(|retry_at| retry_at < claim.query.deadline);
            let domain_failure = Failure {
                code: FailureCode::Platform {
                    code: PlatformFailureCode::ContextQueryFailed,
                },
                class: FailureClass::External,
                retryability: if may_retry {
                    Retryability::SafeWithinPolicy
                } else {
                    Retryability::Never
                },
                safe_message: Some(failure.safe_message),
                details_ref: None,
                source: FailureSource::Context,
            };
            if may_retry {
                (
                    ContextBackendOutcome::RetryableFailure {
                        failure: domain_failure,
                        retry_at: retry_at.expect("checked retry time"),
                    },
                    None,
                    None,
                )
            } else {
                (
                    ContextBackendOutcome::PermanentFailure {
                        failure: domain_failure,
                    },
                    None,
                    Some(claim.failure_mutations.clone()),
                )
            }
        }
    };
    commit_worker_outcome(
        &repository,
        &claim,
        process_id,
        fence,
        PreparedWorkerOutcome {
            outcome,
            resume_mutations,
            failure_mutations,
        },
        config,
    )
    .await
}

async fn execute_subscription_claim(
    repository: Arc<PgRepository>,
    backend: Arc<dyn ContextSubscriptionRefreshBackend>,
    config: ContextWorkerConfig,
    mut claim: ClaimedContextSubscriptionRefresh,
) -> Result<(), ContextWorkerError> {
    let process_id = parse_id(&claim.job.worker_id, ResourceKind::WorkerProcessGeneration)?;
    let token: Sha256Digest = claim
        .job
        .lease_token_digest
        .as_deref()
        .ok_or(ContextWorkerError::CorruptClaim)?
        .parse()
        .map_err(|_| ContextWorkerError::CorruptClaim)?;
    let heartbeat = repository
        .heartbeat_job(HeartbeatJob {
            fence: RepositoryJobFence {
                tenant_id: claim.job.tenant_id.clone(),
                job_id: claim.job.job_id.clone(),
                worker_id: process_id.clone(),
                lease_epoch: claim.job.lease_epoch,
                expected_job_version: claim.job.version,
                lease_token_digest: token.clone(),
            },
            lease_milliseconds: i64::try_from(config.lease_milliseconds)
                .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
        })
        .await?;
    claim.attempt.job_fence.expected_version =
        u64::try_from(heartbeat.version).map_err(|_| ContextWorkerError::CorruptClaim)?;
    let trace = RpcTraceContext::start(claim.job.trace, TraceFlags::NotSampled)
        .map_err(|_| ContextWorkerError::CorruptClaim)?;
    let refresh = scope_trace(
        trace,
        backend.refresh_subscription_resources(claim.attempt.clone()),
    );
    tokio::pin!(refresh);
    let mut interval = tokio::time::interval(config.heartbeat_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    let backend_result = loop {
        tokio::select! {
            result = &mut refresh => break result,
            _ = interval.tick() => {
                match repository.heartbeat_job(HeartbeatJob {
                    fence: RepositoryJobFence {
                        tenant_id: claim.job.tenant_id.clone(),
                        job_id: claim.job.job_id.clone(),
                        worker_id: process_id.clone(),
                        lease_epoch: i64::try_from(claim.attempt.job_fence.lease_generation)
                            .map_err(|_| ContextWorkerError::CorruptClaim)?,
                        expected_job_version: i64::try_from(claim.attempt.job_fence.expected_version)
                            .map_err(|_| ContextWorkerError::CorruptClaim)?,
                        lease_token_digest: token.clone(),
                    },
                    lease_milliseconds: i64::try_from(config.lease_milliseconds)
                        .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
                }).await {
                    Ok(next) => claim.attempt.job_fence.expected_version = u64::try_from(next.version)
                        .map_err(|_| ContextWorkerError::CorruptClaim)?,
                    Err(RepositoryError::Database(_)) => {}
                    Err(failure) => return Err(ContextWorkerError::Repository(failure)),
                }
            }
        }
    };
    let response = normalize_subscription_response(
        &claim.attempt,
        backend_result,
        config.timing.failure_backoff,
    )?;
    repository
        .commit_context_subscription_refresh(CommitContextSubscriptionRefresh {
            attempt: claim.attempt,
            response,
            quota_settlement_entry_id: new_id(ResourceKind::QuotaLedgerEntry)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
        })
        .await?;
    Ok(())
}

fn normalize_subscription_response(
    attempt: &insight_platform_context::ContextSubscriptionRefreshAttempt,
    result: Result<ContextSubscriptionRefreshResponse, ContextSubscriptionExecutionError>,
    failure_backoff: Duration,
) -> Result<ContextSubscriptionRefreshResponse, ContextWorkerError> {
    let response = match result {
        Ok(response) if response.validate_for(attempt, Utc::now()).is_ok() => response,
        Ok(_) => {
            subscription_permanent_failure(ContextSubscriptionExecutionError::InvalidResponse)?
        }
        Err(
            failure @ (ContextSubscriptionExecutionError::Unavailable
            | ContextSubscriptionExecutionError::CompletionUncertain),
        ) => ContextSubscriptionRefreshResponse::RetryableFailure {
            class: ContextSubscriptionRefreshFailureClass::Dependency,
            evidence_digest: subscription_failure_digest(failure)?,
            retry_after_milliseconds: u64::try_from(failure_backoff.as_millis())
                .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?,
        },
        Err(failure) => subscription_permanent_failure(failure)?,
    };
    response
        .validate_for(attempt, Utc::now())
        .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?;
    Ok(response)
}

fn subscription_permanent_failure(
    failure: ContextSubscriptionExecutionError,
) -> Result<ContextSubscriptionRefreshResponse, ContextWorkerError> {
    Ok(ContextSubscriptionRefreshResponse::PermanentFailure {
        class: ContextSubscriptionRefreshFailureClass::Rejected,
        evidence_digest: subscription_failure_digest(failure)?,
    })
}

fn subscription_failure_digest(
    failure: ContextSubscriptionExecutionError,
) -> Result<Sha256Digest, ContextWorkerError> {
    let code = match failure {
        ContextSubscriptionExecutionError::InvalidAttempt => "invalid_attempt",
        ContextSubscriptionExecutionError::InvalidEvidence => "invalid_evidence",
        ContextSubscriptionExecutionError::InvalidResponse => "invalid_response",
        ContextSubscriptionExecutionError::Rejected => "rejected",
        ContextSubscriptionExecutionError::Unavailable => "unavailable",
        ContextSubscriptionExecutionError::CompletionUncertain => "completion_uncertain",
        ContextSubscriptionExecutionError::Canonicalization => "canonicalization",
    };
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "context_subscription_refresh_failure": code,
    }))
    .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?
    .parse()
    .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)
}

fn remote_request(
    claim: &ClaimedContextExecution,
    fence: &JobFence,
) -> Result<RemoteContextSearchRequest, ContextWorkerError> {
    let admission = &claim.query.payload.admission;
    let ContextBackendContract::RemoteSearch {
        protocol_contract_digest,
        result_mapping_digest,
    } = &admission.implementation.contract.backend
    else {
        return Err(ContextWorkerError::CorruptClaim);
    };
    let ContextBackendBinding::RemoteSearch {
        endpoint,
        endpoint_identity_digest,
        region,
    } = &admission.context_closure.backend
    else {
        return Err(ContextWorkerError::CorruptClaim);
    };
    let request = RemoteContextSearchRequest {
        schema_version: REMOTE_CONTEXT_PROTOCOL_VERSION,
        tenant_id: claim.query.tenant_id.clone(),
        context_query_id: claim.query.context_query_id.clone(),
        job_id: parse_required_id(&claim.job.job_id, ResourceKind::Job)?,
        physical_attempt: u32::try_from(claim.job.attempt_no)
            .map_err(|_| ContextWorkerError::CorruptClaim)?,
        lease_generation: fence.lease_generation,
        context_deployment: admission.binding.context_deployment.clone(),
        implementation_revision: admission.implementation_revision.clone(),
        protocol_contract_digest: protocol_contract_digest.clone(),
        result_mapping_digest: result_mapping_digest.clone(),
        endpoint: endpoint.clone(),
        endpoint_identity_digest: endpoint_identity_digest.clone(),
        region: region.clone(),
        secret_bindings: admission.context_closure.secret_bindings.clone(),
        network_policy: admission
            .context_closure
            .network_policy
            .clone()
            .ok_or(ContextWorkerError::CorruptClaim)?,
        tls_policy: admission
            .context_closure
            .tls_policy
            .clone()
            .ok_or(ContextWorkerError::CorruptClaim)?,
        trust_policy: admission
            .context_closure
            .trust_policy
            .clone()
            .ok_or(ContextWorkerError::CorruptClaim)?,
        query_input: claim.query_input.clone(),
        normalized_query_digest: admission.request.normalized_query_digest.clone(),
        normalized_filter_digest: admission.request.normalized_filter_digest.clone(),
        requested_projection: admission.request.requested_projection.clone(),
        maximum_classification: admission.grant.maximum_classification,
        page_size: admission.request.page_size,
        cursor_digest: admission.request.cursor_digest.clone(),
        maximum_response_bytes: admission
            .implementation
            .contract
            .limits
            .maximum_response_bytes,
        deadline: claim.query.deadline,
    };
    request
        .validate_at(Utc::now())
        .map_err(|_| ContextWorkerError::CorruptClaim)?;
    Ok(request)
}

fn build_output(
    adapter: &NativeCatalogAdapter,
    claim: &ClaimedContextExecution,
    limits: ContextQueryLimits,
) -> Result<ContextObservationOutput, ContextWorkerError> {
    let admission = &claim.query.payload.admission;
    if adapter.classification.rank() > admission.grant.maximum_classification.rank() {
        return Err(ContextWorkerError::AdapterOutputRejected);
    }
    let observed_at = Utc::now();
    let content = ValueRef::Inline {
        value: adapter.content.clone(),
    };
    let content_digest: Sha256Digest = canonical_digest(&adapter.content)
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)?
        .parse()
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
    let authorization = adapter.authorization_evidence_digest.clone();
    let item = ContextItem {
        item_id: new_id(ResourceKind::ContextItem)?,
        source_item_identity_digest: adapter.source_item_identity_digest.clone(),
        content: content.clone(),
        structured_fields: ClosedJsonValue::build(
            adapter.structured_fields_schema_digest.clone(),
            serde_json::json!({}),
        )
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)?,
        score: adapter
            .score_millionths
            .map(|millionths| NormalizedContextScore {
                millionths,
                score_domain_digest: admission.interface.ranking.score_domain_digest.clone(),
            }),
        classification: adapter.classification,
        citation: ContextCitation {
            context_deployment: admission.binding.context_deployment.clone(),
            interface_revision: admission.interface_revision.clone(),
            dataset_view: admission.dataset_view.clone(),
            locator: CitationLocator::RemoteOpaque {
                locator_digest: adapter.locator_digest.clone(),
            },
            strength: ContextCitationStrength::ObservationOnly,
            content_digest,
            observed_at,
            display_label: adapter.display_label.clone(),
        },
        authorization_evidence_digest: authorization.clone(),
    };
    let total_bytes = u64::try_from(
        serde_json::to_vec(&adapter.content)
            .map_err(|_| ContextWorkerError::AdapterOutputRejected)?
            .len(),
    )
    .map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
    let mut observation = ContextObservation {
        schema_version: 1,
        observation_id: new_id(ResourceKind::ContextObservation)?,
        context_query_id: claim.query.context_query_id.clone(),
        dataset_view: admission.dataset_view.clone(),
        normalized_query_digest: admission.request.normalized_query_digest.clone(),
        items: vec![item],
        next_cursor_digest: None,
        evidence: ContextRetrievalEvidence {
            backend_request_digest: admission.request.normalized_query_digest.clone(),
            backend_response_digest: new_digest()?,
            authorization_evidence_digest: authorization,
            ranking_evidence_digest: adapter.ranking_evidence_digest.clone(),
            candidate_count: 1,
            rejected_count: 0,
            truncated: false,
        },
        observed_at,
        total_bytes,
        canonical_digest: new_digest()?,
    };
    observation.canonical_digest = digest_without_field(&observation, "canonical_digest")?;
    observation
        .validate_for(&claim.query.context_query_id, admission, limits)
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
    let mut unsigned = serde_json::to_value(&observation)
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
    unsigned
        .as_object_mut()
        .ok_or(ContextWorkerError::AdapterOutputRejected)?
        .remove("canonical_digest");
    Ok(ContextObservationOutput {
        value_id: new_id(ResourceKind::RunValue)?,
        value_kind: "context_observation".to_owned(),
        classification: adapter.classification,
        value: ValueRef::Inline { value: unsigned },
        artifact_link_id: None,
        observation,
        validation_evidence_digest: new_digest()?,
    })
}

fn build_remote_output(
    response: RemoteContextSearchResponse,
    claim: &ClaimedContextExecution,
    limits: ContextQueryLimits,
) -> Result<ContextObservationOutput, ContextWorkerError> {
    let admission = &claim.query.payload.admission;
    let now = Utc::now();
    let request = remote_request(
        claim,
        &JobFence {
            expected_version: u64::try_from(claim.job.version)
                .map_err(|_| ContextWorkerError::CorruptClaim)?,
            worker_process_generation_id: parse_id(
                &claim.job.worker_id,
                ResourceKind::WorkerProcessGeneration,
            )?,
            lease_generation: u64::try_from(claim.job.lease_epoch)
                .map_err(|_| ContextWorkerError::CorruptClaim)?,
            token_digest: claim
                .job
                .lease_token_digest
                .as_deref()
                .ok_or(ContextWorkerError::CorruptClaim)?
                .parse()
                .map_err(|_| ContextWorkerError::CorruptClaim)?,
        },
    )?;
    response
        .validate_for(&request, now)
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
    let authorization_evidence_digest = digest_value(&serde_json::json!({
        "items": response
            .items
            .iter()
            .map(|item| item.authorization_evidence_digest.clone())
            .collect::<Vec<_>>(),
        "response": response.backend_response_digest,
    }))?;
    let mut total_bytes = 0_u64;
    let mut classification = DataClassification::Public;
    let mut items = Vec::with_capacity(response.items.len());
    for remote in response.items {
        let bytes = serde_json::to_vec(&remote.content)
            .map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
        total_bytes = total_bytes
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| ContextWorkerError::AdapterOutputRejected)?,
            )
            .ok_or(ContextWorkerError::AdapterOutputRejected)?;
        classification = classification.join(remote.classification);
        let content_digest = digest_value(&remote.content)?;
        items.push(ContextItem {
            item_id: new_id(ResourceKind::ContextItem)?,
            source_item_identity_digest: remote.source_item_identity_digest,
            content: ValueRef::Inline {
                value: remote.content,
            },
            structured_fields: ClosedJsonValue::build(
                admission.interface.item_schema_digest.clone(),
                remote.structured_fields,
            )
            .map_err(|_| ContextWorkerError::AdapterOutputRejected)?,
            score: remote
                .score_millionths
                .map(|millionths| NormalizedContextScore {
                    millionths,
                    score_domain_digest: admission.interface.ranking.score_domain_digest.clone(),
                }),
            classification: remote.classification,
            citation: ContextCitation {
                context_deployment: admission.binding.context_deployment.clone(),
                interface_revision: admission.interface_revision.clone(),
                dataset_view: admission.dataset_view.clone(),
                locator: CitationLocator::RemoteOpaque {
                    locator_digest: remote.locator_digest,
                },
                strength: ContextCitationStrength::ObservationOnly,
                content_digest,
                observed_at: response.observed_at,
                display_label: remote.display_label,
            },
            authorization_evidence_digest: remote.authorization_evidence_digest,
        });
    }
    let candidate_count =
        u32::try_from(items.len()).map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
    let mut observation = ContextObservation {
        schema_version: 1,
        observation_id: new_id(ResourceKind::ContextObservation)?,
        context_query_id: claim.query.context_query_id.clone(),
        dataset_view: admission.dataset_view.clone(),
        normalized_query_digest: admission.request.normalized_query_digest.clone(),
        items,
        next_cursor_digest: response.next_cursor_digest,
        evidence: ContextRetrievalEvidence {
            backend_request_digest: response.backend_request_digest,
            backend_response_digest: response.backend_response_digest.clone(),
            authorization_evidence_digest,
            ranking_evidence_digest: response.ranking_evidence_digest,
            candidate_count,
            rejected_count: 0,
            truncated: false,
        },
        observed_at: response.observed_at,
        total_bytes,
        canonical_digest: new_digest()?,
    };
    observation.canonical_digest = digest_without_field(&observation, "canonical_digest")?;
    observation
        .validate_for(&claim.query.context_query_id, admission, limits)
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
    let mut unsigned = serde_json::to_value(&observation)
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
    unsigned
        .as_object_mut()
        .ok_or(ContextWorkerError::AdapterOutputRejected)?
        .remove("canonical_digest");
    Ok(ContextObservationOutput {
        value_id: new_id(ResourceKind::RunValue)?,
        value_kind: "context_observation".to_owned(),
        classification,
        value: ValueRef::Inline { value: unsigned },
        artifact_link_id: None,
        observation,
        validation_evidence_digest: digest_value(&serde_json::json!({
            "backend_response_digest": response.backend_response_digest,
            "remote_revision_digest": response.remote_revision_digest,
        }))?,
    })
}

fn digest_value(value: &Value) -> Result<Sha256Digest, ContextWorkerError> {
    canonical_digest(value)
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)?
        .parse()
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)
}

fn claimed_identity(
    claim: &ClaimedContextExecution,
    process_id: &ResourceId,
    tokens: &BTreeSet<Sha256Digest>,
) -> Result<ClaimedJobIdentity, ContextWorkerError> {
    let worker = parse_id(&claim.job.worker_id, ResourceKind::WorkerProcessGeneration)?;
    let token: Sha256Digest = claim
        .job
        .lease_token_digest
        .as_deref()
        .ok_or(ContextWorkerError::CorruptClaim)?
        .parse()
        .map_err(|_| ContextWorkerError::CorruptClaim)?;
    if &worker != process_id || !tokens.contains(&token) || claim.job.lease_epoch <= 0 {
        return Err(ContextWorkerError::CorruptClaim);
    }
    Ok(ClaimedJobIdentity {
        job_id: parse_required_id(&claim.job.job_id, ResourceKind::Job)?,
        lease_generation: u64::try_from(claim.job.lease_epoch)
            .map_err(|_| ContextWorkerError::CorruptClaim)?,
    })
}

fn claimed_subscription_identity(
    claim: &ClaimedContextSubscriptionRefresh,
    process_id: &ResourceId,
    tokens: &BTreeSet<Sha256Digest>,
) -> Result<ClaimedJobIdentity, ContextWorkerError> {
    let worker = parse_id(&claim.job.worker_id, ResourceKind::WorkerProcessGeneration)?;
    let token: Sha256Digest = claim
        .job
        .lease_token_digest
        .as_deref()
        .ok_or(ContextWorkerError::CorruptClaim)?
        .parse()
        .map_err(|_| ContextWorkerError::CorruptClaim)?;
    if &worker != process_id
        || claim.attempt.worker_process_generation_id != *process_id
        || claim.attempt.job_fence.token_digest != token
        || !tokens.contains(&token)
        || claim.job.lease_epoch <= 0
    {
        return Err(ContextWorkerError::CorruptClaim);
    }
    Ok(ClaimedJobIdentity {
        job_id: parse_required_id(&claim.job.job_id, ResourceKind::Job)?,
        lease_generation: u64::try_from(claim.job.lease_epoch)
            .map_err(|_| ContextWorkerError::CorruptClaim)?,
    })
}

fn resume_mutations() -> Result<ExternalLeafResumeMutationIds, ContextWorkerError> {
    Ok(ExternalLeafResumeMutationIds {
        continuation_node_execution_id: new_id(ResourceKind::NodeExecution)?,
        continuation_job_id: new_id(ResourceKind::Job)?,
        run_event_id: new_id(ResourceKind::Event)?,
        run_outbox_id: new_id(ResourceKind::OutboxEvent)?,
        leaf_node_event_id: new_id(ResourceKind::Event)?,
        leaf_node_outbox_id: new_id(ResourceKind::OutboxEvent)?,
        continuation_node_event_id: new_id(ResourceKind::Event)?,
        continuation_node_outbox_id: new_id(ResourceKind::OutboxEvent)?,
        continuation_job_event_id: new_id(ResourceKind::Event)?,
        continuation_job_outbox_id: new_id(ResourceKind::OutboxEvent)?,
    })
}

fn failure_mutations() -> Result<ExternalLeafFailureMutationIds, ContextWorkerError> {
    Ok(ExternalLeafFailureMutationIds {
        convergence_job_id: new_id(ResourceKind::Job)?,
        run_event_id: new_id(ResourceKind::Event)?,
        run_outbox_id: new_id(ResourceKind::OutboxEvent)?,
        leaf_node_event_id: new_id(ResourceKind::Event)?,
        leaf_node_outbox_id: new_id(ResourceKind::OutboxEvent)?,
        convergence_job_event_id: new_id(ResourceKind::Event)?,
        convergence_job_outbox_id: new_id(ResourceKind::OutboxEvent)?,
    })
}

fn ids_array(kind: ResourceKind) -> Result<[ResourceId; CONTEXT_QUOTA_LINES], ContextWorkerError> {
    Ok([new_id(kind)?, new_id(kind)?, new_id(kind)?])
}

fn new_id(kind: ResourceKind) -> Result<ResourceId, ContextWorkerError> {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).map_err(ContextWorkerError::Identity)
}

fn new_digest() -> Result<Sha256Digest, ContextWorkerError> {
    canonical_digest(&serde_json::json!({"nonce": Uuid::now_v7()}))
        .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)?
        .parse()
        .map_err(|_| ContextWorkerError::InvalidGeneratedCommand)
}

fn parse_id(value: &Option<String>, kind: ResourceKind) -> Result<ResourceId, ContextWorkerError> {
    parse_required_id(
        value.as_deref().ok_or(ContextWorkerError::CorruptClaim)?,
        kind,
    )
}

fn parse_required_id(value: &str, kind: ResourceKind) -> Result<ResourceId, ContextWorkerError> {
    let id: ResourceId = value
        .parse()
        .map_err(|_| ContextWorkerError::CorruptClaim)?;
    if id.kind() != kind {
        return Err(ContextWorkerError::CorruptClaim);
    }
    Ok(id)
}

fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, ContextWorkerError> {
    let mut value =
        serde_json::to_value(value).map_err(|_| ContextWorkerError::AdapterOutputRejected)?;
    value
        .as_object_mut()
        .ok_or(ContextWorkerError::AdapterOutputRejected)?
        .remove(field);
    canonical_digest(&value)
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)?
        .parse()
        .map_err(|_| ContextWorkerError::AdapterOutputRejected)
}

#[derive(Debug)]
pub enum ContextWorkerError {
    InvalidConfiguration,
    WrongWorkerManifest,
    LocalCapacity,
    CorruptClaim,
    InvalidGeneratedCommand,
    AdapterOutputRejected,
    DrainRequiresTermination,
    Identity(ResourceIdError),
    Repository(RepositoryError),
}

impl From<RepositoryError> for ContextWorkerError {
    fn from(value: RepositoryError) -> Self {
        Self::Repository(value)
    }
}

impl ContextWorkerError {
    fn is_transient_repository_race(&self) -> bool {
        matches!(
            self,
            Self::Repository(
                RepositoryError::Conflict(_)
                    | RepositoryError::StaleFence
                    | RepositoryError::LeaseExpired
                    | RepositoryError::Database(_)
            )
        )
    }
}

impl fmt::Display for ContextWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("Context Worker configuration is invalid")
            }
            Self::WrongWorkerManifest => formatter.write_str("Context Worker manifest is invalid"),
            Self::LocalCapacity => formatter.write_str("Context Worker local capacity failed"),
            Self::CorruptClaim => formatter.write_str("Context claim is corrupt"),
            Self::InvalidGeneratedCommand => {
                formatter.write_str("Context Worker generated an invalid command")
            }
            Self::AdapterOutputRejected => {
                formatter.write_str("NativeCatalog adapter output was rejected")
            }
            Self::DrainRequiresTermination => {
                formatter.write_str("Context Worker drain requires process termination")
            }
            Self::Identity(_) => formatter.write_str("Context Worker identity generation failed"),
            Self::Repository(failure) => {
                write!(formatter, "Context durable authority failed: {failure}")
            }
        }
    }
}

impl Error for ContextWorkerError {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use insight_platform_context::{
        ContextSubscriptionRefreshAttempt, ContextSubscriptionRefreshCause,
        ContextSubscriptionRefreshRequest, CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
        CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION,
    };
    use insight_platform_contracts::ExactDeploymentRef;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(label: &str) -> Sha256Digest {
        canonical_digest(&serde_json::json!({"worker_test": label}))
            .unwrap()
            .parse()
            .unwrap()
    }

    fn subscription_attempt(now: chrono::DateTime<Utc>) -> ContextSubscriptionRefreshAttempt {
        let worker = id(ResourceKind::WorkerProcessGeneration, 1);
        let mut request = ContextSubscriptionRefreshRequest {
            schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
            tenant_id: id(ResourceKind::Tenant, 2),
            subscription_id: id(ResourceKind::McpOperation, 3),
            context_deployment: ExactDeploymentRef::new(
                id(ResourceKind::ContextDeployment, 4),
                digest("context-deployment"),
            )
            .unwrap(),
            mcp_deployment: ExactDeploymentRef::new(
                id(ResourceKind::McpDeployment, 5),
                digest("mcp-deployment"),
            )
            .unwrap(),
            discovery_snapshot_id: id(ResourceKind::McpDiscoverySnapshot, 6),
            discovery_snapshot_digest: digest("discovery"),
            resource_uri: "mcp://knowledge/root".to_owned(),
            resource_uri_digest: digest("resource-uri"),
            authorization_generation: 1,
            session_generation: 2,
            event_generation: 3,
            event_key_digest: digest("event-key"),
            body_digest: digest("body"),
            cause: ContextSubscriptionRefreshCause::ResourceUpdated,
            deadline: now + ChronoDuration::minutes(5),
            request_digest: digest("placeholder"),
        };
        request.request_digest = request.canonical_request_digest().unwrap();
        ContextSubscriptionRefreshAttempt {
            schema_version: CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION,
            job_id: id(ResourceKind::Job, 7),
            worker_process_generation_id: worker.clone(),
            job_fence: JobFence {
                expected_version: 4,
                worker_process_generation_id: worker,
                lease_generation: 2,
                token_digest: digest("lease"),
            },
            attempt_number: 1,
            request,
        }
    }

    #[test]
    fn unavailable_subscription_backend_is_retryable_dependency_failure() {
        let attempt = subscription_attempt(Utc::now());
        let response = normalize_subscription_response(
            &attempt,
            Err(ContextSubscriptionExecutionError::Unavailable),
            Duration::from_secs(2),
        )
        .unwrap();
        assert!(matches!(
            response,
            ContextSubscriptionRefreshResponse::RetryableFailure {
                class: ContextSubscriptionRefreshFailureClass::Dependency,
                retry_after_milliseconds: 2_000,
                ..
            }
        ));
    }

    #[test]
    fn invalid_backend_response_is_terminal_rejection() {
        let attempt = subscription_attempt(Utc::now());
        let response = normalize_subscription_response(
            &attempt,
            Ok(ContextSubscriptionRefreshResponse::RetryableFailure {
                class: ContextSubscriptionRefreshFailureClass::Rejected,
                evidence_digest: digest("bad-response"),
                retry_after_milliseconds: 1,
            }),
            Duration::from_secs(2),
        )
        .unwrap();
        assert!(matches!(
            response,
            ContextSubscriptionRefreshResponse::PermanentFailure {
                class: ContextSubscriptionRefreshFailureClass::Rejected,
                ..
            }
        ));
    }

    #[test]
    fn failure_evidence_is_stable_and_class_specific() {
        assert_eq!(
            subscription_failure_digest(ContextSubscriptionExecutionError::Unavailable).unwrap(),
            subscription_failure_digest(ContextSubscriptionExecutionError::Unavailable).unwrap()
        );
        assert_ne!(
            subscription_failure_digest(ContextSubscriptionExecutionError::Unavailable).unwrap(),
            subscription_failure_digest(ContextSubscriptionExecutionError::CompletionUncertain)
                .unwrap()
        );
    }
}
