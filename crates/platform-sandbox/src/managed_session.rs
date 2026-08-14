use crate::{
    digest_without_field, required_isolation, SandboxCommandLimits, SandboxContractError,
    SandboxExecutionPolicyClosure, SandboxNetworkMode, SandboxResourceEnvelope,
    ScopedArtifactGrant, ScopedSecretGrant, MAX_SANDBOX_ARTIFACT_GRANTS, MAX_SANDBOX_SECRET_GRANTS,
    SANDBOX_PROTOCOL_VERSION, SANDBOX_QUOTA_LINES,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ArtifactGrantOperation, CommandOutcome, DataClassification, Effect, JobState, ResourceDocument,
    ResourceId, ResourceKind, SandboxIsolationClass, SandboxJobState, SandboxRuntimeFamily,
    Sha256Digest, WorkClass,
};
use insight_platform_jobs::{JobFence, JobOwnerRef, JobProjection};
use insight_platform_mcp_host::{
    ManagedMcpSandboxSessionIdentity, McpHostExecutionContract, McpResourceSubscriptionBinding,
    McpSubscriptionWorkerAudit,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Exact callback audience used when the Sandbox session reaches prepared or terminal state.
/// It contains no endpoint, credential, plaintext session handle or caller-selected URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionCallback {
    pub schema_version: u32,
    pub session_identity_digest: Sha256Digest,
    pub audience_identity_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
    pub binding_digest: Sha256Digest,
}

impl ManagedMcpSandboxSessionCallback {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.binding_digest = digest_without_field(&self, "binding_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        identity: &ManagedMcpSandboxSessionIdentity,
        deadline: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != 1
            || self.session_identity_digest != identity.canonical_digest
            || self.expires_at > deadline
            || digest_without_field(self, "binding_digest")? != self.binding_digest
        {
            return Err(SandboxContractError::InvalidCallback);
        }
        Ok(())
    }
}

/// Immutable physical request for one Managed stdio Resource-subscription generation.
///
/// Unlike `SandboxExecutionRequest`, this document has no Capability Invocation, RunValue or
/// output slot. Its sole logical owner is the distinct MCP subscription Job captured in
/// `identity`; its sole physical owner is the Sandbox Job with the same UUID as
/// `identity.sandbox_job_id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionRequest {
    pub schema_version: u32,
    pub protocol_version: insight_platform_contracts::SandboxAbiVersion,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub subscription_binding: McpResourceSubscriptionBinding,
    pub mcp_contract: Box<McpHostExecutionContract>,
    pub runtime_revision: insight_platform_contracts::ExactVersionRef,
    pub runtime: insight_platform_contracts::SandboxRuntimeResourceSpec,
    pub package_revision: insight_platform_contracts::ExactVersionRef,
    pub package: insight_platform_contracts::SandboxPackageResourceSpec,
    pub profile_revision: insight_platform_contracts::ExactVersionRef,
    pub profile: insight_platform_contracts::SandboxProfileResourceSpec,
    pub policies: Box<SandboxExecutionPolicyClosure>,
    pub isolation_class: SandboxIsolationClass,
    pub executor_worker_manifest_digest: Sha256Digest,
    pub isolation_backend_contract_digest: Sha256Digest,
    pub classification: DataClassification,
    pub artifact_grants: Vec<ScopedArtifactGrant>,
    pub secret_grants: Vec<ScopedSecretGrant>,
    pub network_mode: SandboxNetworkMode,
    pub resources: SandboxResourceEnvelope,
    pub deadline: DateTime<Utc>,
    pub callback: ManagedMcpSandboxSessionCallback,
    pub request_digest: Sha256Digest,
}

impl ManagedMcpSandboxSessionRequest {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.request_digest = digest_without_field(&self, "request_digest")?;
        Ok(self)
    }

    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        self.subscription_binding
            .validate_for_execution_contract_at(&self.mcp_contract, now)
            .map_err(|_| SandboxContractError::InvalidExactBinding)?;
        self.identity
            .validate_canonical_for_binding(&self.subscription_binding)
            .map_err(|_| SandboxContractError::InvalidExactBinding)?;
        self.mcp_contract
            .validate_canonical_at(now)
            .map_err(|_| SandboxContractError::InvalidExactBinding)?;
        self.runtime_revision
            .validate()
            .map_err(|_| SandboxContractError::InvalidExactBinding)?;
        self.package_revision
            .validate()
            .map_err(|_| SandboxContractError::InvalidExactBinding)?;
        self.profile_revision
            .validate()
            .map_err(|_| SandboxContractError::InvalidExactBinding)?;
        ResourceDocument::SandboxRuntime(self.runtime.clone())
            .validate()
            .map_err(|_| SandboxContractError::InvalidResourceContract)?;
        ResourceDocument::SandboxPackage(self.package.clone())
            .validate()
            .map_err(|_| SandboxContractError::InvalidResourceContract)?;
        ResourceDocument::SandboxProfile(self.profile.clone())
            .validate()
            .map_err(|_| SandboxContractError::InvalidResourceContract)?;
        let insight_platform_contracts::McpTransportBinding::ManagedStdio {
            package,
            runtime,
            profile,
            isolation,
            isolation_policy,
            resource_policy,
            artifact_io_policy,
        } = &self.mcp_contract.deployment_closure.transport
        else {
            return Err(SandboxContractError::InvalidExactBinding);
        };
        if self.schema_version != 1
            || self.protocol_version != SANDBOX_PROTOCOL_VERSION
            || self.identity.tenant_id != self.subscription_binding.tenant_id
            || self.identity.subscription_id != self.subscription_binding.subscription_id
            || self.identity.logical_job_id != self.subscription_binding.job_id
            || self.runtime_revision.resource_kind != ResourceKind::SandboxRuntimeRevision
            || self.package_revision.resource_kind != ResourceKind::SandboxPackageRevision
            || self.profile_revision.resource_kind != ResourceKind::SandboxProfileRevision
            || runtime != &self.runtime_revision
            || package != &self.package_revision
            || profile != &self.profile_revision
            || *isolation != SandboxIsolationClass::MicroVm
            || self.isolation_class != SandboxIsolationClass::MicroVm
            || isolation_policy != &self.profile.isolation_policy
            || resource_policy != &self.profile.resource_policy
            || artifact_io_policy != &self.profile.artifact_io_policy
            || self.runtime.runtime_family != SandboxRuntimeFamily::ManagedMcpServer
            || self.package.entrypoint_kind
                != insight_platform_contracts::SandboxEntrypointKind::ManagedMcpServer
            || self.package.runtime_revision != self.runtime_revision
            || self.runtime.abi != self.protocol_version
            || !self
                .runtime
                .supported_isolation
                .contains(&SandboxIsolationClass::MicroVm)
            || !self
                .profile
                .allowed_runtime_families
                .contains(&SandboxRuntimeFamily::ManagedMcpServer)
            || !self
                .profile
                .allowed_trust_classes
                .contains(&self.package.trust_class)
            || self.profile.minimum_isolation.security_rank()
                > SandboxIsolationClass::MicroVm.security_rank()
            || required_isolation(
                self.runtime.runtime_family,
                self.package.trust_class,
                Effect::ReadOnly,
                self.classification,
                !self.secret_grants.is_empty(),
                self.network_mode,
            ) != SandboxIsolationClass::MicroVm
            || self.deadline <= now
            || self.artifact_grants.is_empty()
            || self.artifact_grants.len() > MAX_SANDBOX_ARTIFACT_GRANTS
            || self.secret_grants.len() > MAX_SANDBOX_SECRET_GRANTS
            || !self.artifact_grants.iter().any(|grant| {
                grant.operation == ArtifactGrantOperation::ReadWhole
                    && grant.artifact.as_ref() == Some(&self.package.runtime_bundle_artifact)
            })
            || !sorted_unique_ids(self.artifact_grants.iter().map(|grant| &grant.grant_id))
            || !sorted_unique_ids(
                self.secret_grants
                    .iter()
                    .map(|grant| &grant.secret_binding.secret_binding_id),
            )
            || digest_without_field(self, "request_digest")? != self.request_digest
        {
            return Err(SandboxContractError::InvalidExecutionRequest);
        }
        self.callback.validate_for(&self.identity, self.deadline)?;
        if self.callback.expires_at <= now {
            return Err(SandboxContractError::InvalidCallback);
        }
        self.policies.validate_for(
            &self.profile,
            &self.runtime,
            &self.package,
            self.isolation_class,
            self.network_mode,
            &self.resources,
            &self.artifact_grants,
            &self.secret_grants,
        )?;
        validate_artifact_grants(self, now, limits)?;
        validate_secret_grants(self)?;
        self.resources.validate(
            self.isolation_class,
            self.network_mode,
            self.profile.max_job_duration_milliseconds,
            limits,
        )
    }
}

fn validate_artifact_grants(
    request: &ManagedMcpSandboxSessionRequest,
    now: DateTime<Utc>,
    limits: SandboxCommandLimits,
) -> Result<(), SandboxContractError> {
    for grant in &request.artifact_grants {
        if grant.expires_at <= now
            || grant.artifact.as_ref().is_some_and(|artifact| {
                artifact.classification().rank() > request.classification.rank()
            })
        {
            return Err(SandboxContractError::InvalidArtifactGrant);
        }
        grant.validate_for(
            &request.identity.tenant_id,
            &request.identity.sandbox_job_id,
            request.deadline,
            limits,
        )?;
    }
    Ok(())
}

fn validate_secret_grants(
    request: &ManagedMcpSandboxSessionRequest,
) -> Result<(), SandboxContractError> {
    let mut required = request
        .mcp_contract
        .deployment_closure
        .secret_bindings
        .iter()
        .collect::<Vec<_>>();
    required.push(&request.mcp_contract.authorization.token_secret_binding);
    if required
        .iter()
        .map(|binding| &binding.secret_binding_id)
        .collect::<BTreeSet<_>>()
        .len()
        != required.len()
        || request.secret_grants.len() != required.len()
        || required.iter().any(|binding| {
            !request.secret_grants.iter().any(|grant| {
                *binding == &grant.secret_binding
                    && binding.permits_resolved_generation(
                        &grant.secret_binding.secret_binding_id,
                        &grant.secret_binding.purpose,
                        grant.resolved_binding_generation,
                    )
            })
        })
    {
        return Err(SandboxContractError::InvalidSecretGrant);
    }
    for grant in &request.secret_grants {
        grant.validate_for(
            &request.identity.tenant_id,
            &request.identity.sandbox_job_id,
            &request.runtime.image_or_module_digest,
            request.deadline,
        )?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionJobPayload {
    pub schema_version: u32,
    pub request: Box<ManagedMcpSandboxSessionRequest>,
    pub submission_digest: Sha256Digest,
    pub physical_state: SandboxJobState,
    pub phase_sequence: u32,
    pub payload_digest: Sha256Digest,
}

impl ManagedMcpSandboxSessionJobPayload {
    pub fn accepted(
        request: ManagedMcpSandboxSessionRequest,
    ) -> Result<Self, SandboxContractError> {
        let submission_digest = request.request_digest.clone();
        let mut payload = Self {
            schema_version: 1,
            submission_digest: submission_digest.clone(),
            request: Box::new(request),
            physical_state: SandboxJobState::Accepted,
            phase_sequence: 0,
            payload_digest: submission_digest.clone(),
        };
        payload.payload_digest = digest_without_field(&payload, "payload_digest")?;
        Ok(payload)
    }

    pub fn validate_for(
        &self,
        job: &JobProjection,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        self.request.validate_at(job.scheduled_at, limits)?;
        if self.schema_version != 1
            || job.tenant_id != self.request.identity.tenant_id
            || job.job_id != self.request.identity.physical_job_id
            || job.owner
                != (JobOwnerRef {
                    owner_id: self.request.identity.sandbox_job_id.clone(),
                    owner_kind: ResourceKind::SandboxJob,
                })
            || job.work_class != WorkClass::Sandbox
            || job.deadline != self.request.deadline
            || job.attempt_limit != 1
            || self.submission_digest != self.request.request_digest
            || self.physical_state != SandboxJobState::Accepted
            || self.phase_sequence != 0
            || !matches!(job.state, JobState::Ready | JobState::Leased)
            || digest_without_field(self, "payload_digest")? != self.payload_digest
        {
            return Err(SandboxContractError::InvalidJob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AcceptManagedMcpSandboxSession {
    pub audit: McpSubscriptionWorkerAudit,
    pub logical_fence: JobFence,
    pub request: ManagedMcpSandboxSessionRequest,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
}

impl AcceptManagedMcpSandboxSession {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        self.audit
            .validate_at(now)
            .map_err(|_| SandboxContractError::InvalidWorkerCommand)?;
        self.request.validate_at(now, limits)?;
        let identity = &self.request.identity;
        if self.audit.tenant_id != identity.tenant_id
            || self.audit.worker_process_generation_id
                != self.logical_fence.worker_process_generation_id
            || self.logical_fence.expected_version != identity.admitted_logical_job_version
            || self.logical_fence.lease_generation == 0
            || self.audit.request_digest != self.request.request_digest
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.quota_entry_ids.len() != SANDBOX_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.quota_entry_ids.iter().collect::<BTreeSet<_>>().len()
                != self.quota_entry_ids.len()
        {
            return Err(SandboxContractError::InvalidWorkerCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AcceptedManagedMcpSandboxSession {
    pub logical_payload: insight_platform_mcp_host::McpSubscriptionPayload,
    pub logical_state: insight_platform_mcp_host::McpSubscriptionState,
    pub physical_job: JobProjection,
    pub physical_payload: ManagedMcpSandboxSessionJobPayload,
}

#[async_trait]
pub trait ManagedMcpSandboxSessionGatewayAuthority: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn accept_managed_mcp_sandbox_session(
        &self,
        command: AcceptManagedMcpSandboxSession,
    ) -> Result<CommandOutcome<AcceptedManagedMcpSandboxSession>, Self::Error>;
}

pub fn decide_accept_managed_mcp_sandbox_session(
    current: &insight_platform_mcp_host::McpSubscriptionRecord,
    command: &AcceptManagedMcpSandboxSession,
    database_now: DateTime<Utc>,
    limits: SandboxCommandLimits,
) -> Result<AcceptedManagedMcpSandboxSession, SandboxContractError> {
    command.validate_at(database_now, limits)?;
    let identity = &command.request.identity;
    if current.tenant_id != identity.tenant_id
        || current.subscription_id != identity.subscription_id
        || current.job_id != identity.logical_job_id
        || current.version != identity.admitted_subscription_version
        || current.payload.session.version == u64::MAX
        || current.payload.session.generation.checked_add(1) != Some(identity.session_generation)
    {
        return Err(SandboxContractError::InvalidExecutionRequest);
    }
    let link = insight_platform_mcp_host::ManagedMcpSandboxSessionLink::build(
        identity.clone(),
        command.request.request_digest.clone(),
        &current.payload.binding,
    )
    .map_err(|_| SandboxContractError::InvalidExecutionRequest)?;
    let (logical_payload, logical_state) = current
        .payload
        .schedule_managed_sandbox_session(current.payload.session.version, link, database_now)
        .map_err(|_| SandboxContractError::InvalidExecutionRequest)?;
    let physical_job = JobProjection {
        tenant_id: identity.tenant_id.clone(),
        job_id: identity.physical_job_id.clone(),
        work_class: WorkClass::Sandbox,
        owner: JobOwnerRef {
            owner_id: identity.sandbox_job_id.clone(),
            owner_kind: ResourceKind::SandboxJob,
        },
        state: JobState::Ready,
        version: 1,
        attempt_count: 0,
        attempt_limit: 1,
        lease_generation: 0,
        lease: None,
        scheduled_at: database_now,
        retry_at: None,
        wake: None,
        deadline: command.request.deadline,
    };
    physical_job
        .validate()
        .map_err(|_| SandboxContractError::InvalidJob)?;
    let physical_payload = ManagedMcpSandboxSessionJobPayload::accepted(command.request.clone())?;
    physical_payload.validate_for(&physical_job, limits)?;
    Ok(AcceptedManagedMcpSandboxSession {
        logical_payload,
        logical_state,
        physical_job,
        physical_payload,
    })
}

fn sorted_unique_ids<'a>(values: impl Iterator<Item = &'a ResourceId>) -> bool {
    let values = values.collect::<Vec<_>>();
    values.windows(2).all(|pair| pair[0] < pair[1])
        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
}
