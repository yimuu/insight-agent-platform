//! Independent Context Worker for bounded, statically installed NativeCatalog adapters.
//!
//! PostgreSQL remains the durable Job and ContextQuery authority. Candidate discovery is
//! read-only; exact-slot claim, heartbeat, quota settlement, and terminal commit use the shared
//! durable contracts.

use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_context::{
    CitationLocator, ClaimContextJobs, CommitContextOutcome, ContextBackendOutcome,
    ContextCitation, ContextClaimSlot, ContextItem, ContextObservation, ContextObservationOutput,
    ContextQueryLimits, ContextRetrievalEvidence, ContextWorkerAudit, NormalizedContextScore,
    CONTEXT_QUOTA_LINES,
};
use insight_platform_contracts::{
    canonical_digest, ClosedJsonValue, ContextCitationStrength, DataClassification,
    ExternalLeafFailureMutationIds, ExternalLeafResumeMutationIds, Failure, FailureClass,
    FailureCode, FailureSource, HardLimitProfile, PlatformFailureCode, ResourceId, ResourceIdError,
    ResourceKind, Retryability, Sha256Digest, ValueRef, WorkClass,
};
use insight_platform_jobs::{JobFence, LeasePolicy};
use insight_platform_postgres::{
    context_query_repository::{ClaimableNativeContextJob, ClaimedContextExecution},
    repository::{HeartbeatJob, JobFence as RepositoryJobFence, PgRepository, RepositoryError},
};
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
            timing,
            limits: ContextQueryLimits::from_profile(profile)
                .map_err(|_| ContextWorkerError::InvalidConfiguration)?,
        };
        if config.claim_size == 0
            || config.recovery_size == 0
            || config.lease_milliseconds == 0
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
                        Err(failure) => return Err(failure),
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
                Some(claim.resume_mutations),
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
                Some(claim.failure_mutations),
            ),
            Err(failure) => return Err(failure),
        };
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
            outcome,
            quota_entry_ids: ids_array(ResourceKind::QuotaLedgerEntry)?,
            resume_mutations,
            failure_mutations,
        })
        .await?;
    transaction.commit().await?;
    Ok(())
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
            Self::Repository(_) => formatter.write_str("Context durable authority failed"),
        }
    }
}

impl Error for ContextWorkerError {}
