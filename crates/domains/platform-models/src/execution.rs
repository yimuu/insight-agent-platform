//! Exact, bounded execution requests and outcome authority ports.
use crate::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::*;
use insight_platform_jobs::JobFence;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelAdapterExecutionRequest {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub model_turn_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    /// Current attempt provenance; frozen adapter semantics are resolved independently.
    pub worker_manifest_digest: Sha256Digest,
    pub attempt_no: u32,
    pub attempt_limit: u32,
    pub lease_generation: u64,
    pub admission_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub quota_ceiling: ModelQuotaCeiling,
    pub model_deployment: ExactDeploymentRef,
    pub model_closure: ModelDeploymentClosure,
    pub profile_revision: insight_platform_contracts::ExactVersionRef,
    pub provider_deployment: ExactDeploymentRef,
    pub provider_closure: ModelProviderDeploymentClosure,
    pub provider_revision: insight_platform_contracts::ExactVersionRef,
    pub provider: ModelProviderResourceSpec,
    pub profile: Box<ModelProfileResourceSpec>,
    pub request: Box<CanonicalModelRequest>,
}
impl ModelAdapterExecutionRequest {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: ModelTurnLimits,
    ) -> Result<(), ModelTurnError> {
        self.model_deployment
            .validate()
            .map_err(|_| ModelTurnError::InvalidRequest)?;
        self.provider_deployment
            .validate()
            .map_err(|_| ModelTurnError::InvalidRequest)?;
        DeploymentClosure::ModelProfile(self.model_closure.clone())
            .validate()
            .map_err(|_| ModelTurnError::InvalidRequest)?;
        DeploymentClosure::ModelProvider(self.provider_closure.clone())
            .validate()
            .map_err(|_| ModelTurnError::InvalidRequest)?;
        ResourceDocument::ModelProvider(self.provider.clone())
            .validate()
            .map_err(|_| ModelTurnError::InvalidRequest)?;
        ResourceDocument::ModelProfile(self.profile.clone())
            .validate()
            .map_err(|_| ModelTurnError::InvalidRequest)?;
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.run_id.kind() != ResourceKind::Run
            || self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.attempt_no == 0
            || self.attempt_limit == 0
            || self.attempt_no > self.attempt_limit
            || self.lease_generation == 0
            || self.quota_ceiling.concurrent_units != 1
            || self.quota_ceiling.requests != 1
            || self.quota_ceiling.tokens
                < self
                    .request
                    .input_token_estimate
                    .saturating_add(u64::from(self.request.max_output_tokens))
            || self.quota_ceiling.tokens > limits.maximum_tokens_per_turn()
            || self.quota_ceiling.cost_microunits == 0
            || self.model_deployment.resource_kind != ResourceKind::ModelDeployment
            || self.provider_deployment.resource_kind != ResourceKind::ModelProviderDeployment
            || self.profile_revision.resource_kind != ResourceKind::ModelProfileRevision
            || self.provider_revision.resource_kind != ResourceKind::ModelProviderRevision
            || self.model_closure.provider_deployment != self.provider_deployment
            || self.model_closure.profile_revision != self.profile_revision
            || self.provider_closure.provider_revision != self.provider_revision
            || self.profile.provider_revision != self.provider_revision
            || self.provider.protocol_policy != self.provider_closure.protocol_policy
            || !insight_platform_contracts::exact_secret_binding_purposes_match(
                &self.provider_closure.secret_bindings,
                &self.provider.credential_requirements,
            )
            || self.profile.catalog_evidence.adapter_contract_digest
                != self.provider.installed_adapter.adapter_contract_digest
            || canonical_request_digest(&self.request)? != self.request_digest
        {
            return Err(ModelTurnError::InvalidRequest);
        }
        self.request
            .validate_for(
                &self.model_turn_id,
                &self.provider,
                &self.profile,
                &self.provider_closure.region,
                now,
                limits,
            )
            .map_err(|_| ModelTurnError::InvalidRequest)
    }
}
#[derive(Debug, Clone)]
pub struct ExecuteModelAdapterJob {
    pub execution: ModelAdapterExecutionRequest,
    pub audit: ModelWorkerAudit,
    pub expected_turn_version: u64,
    pub fence: JobFence,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub resume_mutations: Option<ExternalLeafResumeMutationIds>,
    pub failure_mutations: Option<ExternalLeafFailureMutationIds>,
    pub tool_continuation_mutations: Option<ModelToolContinuationMutationIds>,
}
impl ExecuteModelAdapterJob {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ModelTurnError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ModelTurnError::InvalidCommand)?;
        if self.expected_turn_version == 0
            || self.audit.tenant_id != self.execution.tenant_id
            || self.audit.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.expected_version == 0
            || self.fence.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.lease_generation != self.execution.lease_generation
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.quota_entry_ids.len() != MODEL_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.quota_entry_ids.iter().collect::<BTreeSet<_>>().len()
                != self.quota_entry_ids.len()
            || self.resume_mutations.is_some() != self.failure_mutations.is_some()
            || self.resume_mutations.is_some() != self.tool_continuation_mutations.is_some()
            || self
                .resume_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
            || self
                .failure_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
            || self
                .tool_continuation_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
        {
            return Err(ModelTurnError::InvalidCommand);
        }
        Ok(())
    }
}
#[async_trait]
pub trait ModelExecutionAuthority: Send + Sync {
    type Error;
    type Record;

    async fn commit_model_outcome(
        &self,
        command: CommitModelOutcome,
    ) -> Result<CommandOutcome<Self::Record>, Self::Error>;
}
pub fn canonical_request_digest(
    request: &CanonicalModelRequest,
) -> Result<Sha256Digest, ModelTurnError> {
    let value = serde_json::to_value(request).map_err(|_| ModelTurnError::InvalidRequest)?;
    canonical_digest(&value)
        .map_err(|_| ModelTurnError::InvalidRequest)?
        .parse()
        .map_err(|_| ModelTurnError::InvalidRequest)
}
