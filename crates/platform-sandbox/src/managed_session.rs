use crate::{
    digest_without_field, required_isolation, ClaimSandboxJobs, NodeAttestorRoute,
    SandboxClaimFailure, SandboxCommandLimits, SandboxContractError, SandboxExecutionPolicyClosure,
    SandboxNetworkMode, SandboxResourceEnvelope, SandboxWorkerAudit, ScopedArtifactGrant,
    ScopedSecretGrant, MAX_SANDBOX_ARTIFACT_GRANTS, MAX_SANDBOX_SECRET_GRANTS,
    SANDBOX_PROTOCOL_VERSION, SANDBOX_QUOTA_LINES,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ArtifactGrantOperation, CommandOutcome, DataClassification, Effect, JobState, ResourceDocument,
    ResourceId, ResourceKind, SandboxIsolationClass, SandboxJobState, SandboxRuntimeFamily,
    Sha256Digest, WorkClass,
};
use insight_platform_jobs::{
    decide_observation_update, decide_start, JobFence, JobOwnerRef, JobProjection,
};
use insight_platform_mcp_host::{
    EncryptedMcpState, ManagedMcpSandboxSessionIdentity, McpHostExecutionContract,
    McpResourceSubscriptionBinding, McpSubscriptionRecord, McpSubscriptionState,
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
    pub executor_identity_digest: Option<Sha256Digest>,
    pub attestor_route: Option<NodeAttestorRoute>,
    pub phase_evidence_digest: Option<Sha256Digest>,
    pub ready_binding: Option<ManagedMcpSandboxSessionReadyBinding>,
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
            executor_identity_digest: None,
            attestor_route: None,
            phase_evidence_digest: None,
            ready_binding: None,
            payload_digest: submission_digest.clone(),
        };
        payload.payload_digest = digest_without_field(&payload, "payload_digest")?;
        Ok(payload)
    }

    fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.payload_digest = digest_without_field(&self, "payload_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        job: &JobProjection,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        self.request.validate_at(job.scheduled_at, limits)?;
        let active = matches!(
            self.physical_state,
            SandboxJobState::Preparing | SandboxJobState::Starting | SandboxJobState::Running
        );
        let state_matches = match self.physical_state {
            SandboxJobState::Accepted => matches!(job.state, JobState::Ready | JobState::Leased),
            SandboxJobState::Preparing | SandboxJobState::Starting | SandboxJobState::Running => {
                job.state == JobState::Running
            }
            _ => false,
        };
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
            || !state_matches
            || (self.physical_state == SandboxJobState::Accepted
                && (self.phase_sequence != 0
                    || self.executor_identity_digest.is_some()
                    || self.attestor_route.is_some()
                    || self.phase_evidence_digest.is_some()
                    || self.ready_binding.is_some()))
            || (active
                && (self.phase_sequence == 0
                    || self.executor_identity_digest.is_none()
                    || self.attestor_route.is_none()
                    || self.phase_evidence_digest.is_none()))
            || (self.physical_state == SandboxJobState::Running) != self.ready_binding.is_some()
            || self
                .ready_binding
                .as_ref()
                .is_some_and(|binding| binding.validate_canonical_for(&self.request).is_err())
            || digest_without_field(self, "payload_digest")? != self.payload_digest
        {
            return Err(SandboxContractError::InvalidJob);
        }
        Ok(())
    }
}

/// Credential-free evidence returned by the trusted Managed MCP adapter after the server process
/// has initialized and accepted `resources/subscribe`, but before notification delivery is
/// activated. The encrypted handle is committed with both Jobs before activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionReadyEvidence {
    pub schema_version: u32,
    pub session_identity_digest: Sha256Digest,
    pub sandbox_identity_digest: Sha256Digest,
    pub encrypted_opaque_session: EncryptedMcpState,
    pub established_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub protocol_evidence_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

/// Non-secret durable binding retained by the physical Job. The encrypted opaque session has one
/// current-state authority in the logical MCP subscription; the physical Job keeps only the
/// process/protocol evidence needed to prove which prepared session reached Running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionReadyBinding {
    pub schema_version: u32,
    pub session_identity_digest: Sha256Digest,
    pub sandbox_identity_digest: Sha256Digest,
    pub ready_evidence_digest: Sha256Digest,
    pub established_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub protocol_evidence_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

impl ManagedMcpSandboxSessionReadyEvidence {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.canonical_digest = digest_without_field(&self, "canonical_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        self.validate_canonical_for(request)?;
        if self.established_at > now || self.expires_at <= now {
            return Err(SandboxContractError::InvalidCallback);
        }
        Ok(())
    }

    fn validate_canonical_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
    ) -> Result<(), SandboxContractError> {
        self.encrypted_opaque_session
            .validate()
            .map_err(|_| SandboxContractError::InvalidCallback)?;
        let maximum_session_milliseconds = i64::try_from(
            request
                .mcp_contract
                .server
                .limits
                .maximum_session_milliseconds,
        )
        .map_err(|_| SandboxContractError::InvalidCallback)?;
        let maximum_expiry = self
            .established_at
            .checked_add_signed(chrono::Duration::milliseconds(maximum_session_milliseconds))
            .ok_or(SandboxContractError::InvalidCallback)?;
        if self.schema_version != 1
            || self.session_identity_digest != request.identity.canonical_digest
            || self.expires_at <= self.established_at
            || self.expires_at > maximum_expiry
            || self.expires_at > request.deadline
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(SandboxContractError::InvalidCallback);
        }
        Ok(())
    }

    pub fn durable_binding(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
    ) -> Result<ManagedMcpSandboxSessionReadyBinding, SandboxContractError> {
        self.validate_canonical_for(request)?;
        let mut binding = ManagedMcpSandboxSessionReadyBinding {
            schema_version: 1,
            session_identity_digest: self.session_identity_digest.clone(),
            sandbox_identity_digest: self.sandbox_identity_digest.clone(),
            ready_evidence_digest: self.canonical_digest.clone(),
            established_at: self.established_at,
            expires_at: self.expires_at,
            protocol_evidence_digest: self.protocol_evidence_digest.clone(),
            canonical_digest: self.canonical_digest.clone(),
        };
        binding.canonical_digest = digest_without_field(&binding, "canonical_digest")?;
        Ok(binding)
    }
}

impl ManagedMcpSandboxSessionReadyBinding {
    fn validate_canonical_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
    ) -> Result<(), SandboxContractError> {
        let maximum_session_milliseconds = i64::try_from(
            request
                .mcp_contract
                .server
                .limits
                .maximum_session_milliseconds,
        )
        .map_err(|_| SandboxContractError::InvalidCallback)?;
        let maximum_expiry = self
            .established_at
            .checked_add_signed(chrono::Duration::milliseconds(maximum_session_milliseconds))
            .ok_or(SandboxContractError::InvalidCallback)?;
        if self.schema_version != 1
            || self.session_identity_digest != request.identity.canonical_digest
            || self.expires_at <= self.established_at
            || self.expires_at > maximum_expiry
            || self.expires_at > request.deadline
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(SandboxContractError::InvalidCallback);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManagedMcpSandboxSessionPhaseDecision {
    pub logical_payload: insight_platform_mcp_host::McpSubscriptionPayload,
    pub logical_state: McpSubscriptionState,
    pub physical_job: JobProjection,
    pub physical_payload: ManagedMcpSandboxSessionJobPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitManagedMcpSandboxSessionPhase {
    pub audit: SandboxWorkerAudit,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub fence: JobFence,
    pub target: SandboxJobState,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
    pub phase_evidence_digest: Sha256Digest,
}

impl CommitManagedMcpSandboxSessionPhase {
    pub fn canonical_request_digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        insight_platform_contracts::canonical_digest(&serde_json::json!({
            "attestor_route": self.attestor_route,
            "executor_identity_digest": self.executor_identity_digest,
            "fence": self.fence,
            "identity": self.identity,
            "phase_evidence_digest": self.phase_evidence_digest,
            "schema_version": 1,
            "target": self.target,
            "tenant_id": self.audit.tenant_id,
        }))
        .map_err(|_| SandboxContractError::InvalidWorkerCommand)?
        .parse()
        .map_err(|_| SandboxContractError::InvalidWorkerCommand)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxContractError> {
        self.audit
            .validate_at(now)
            .map_err(|_| SandboxContractError::InvalidWorkerCommand)?;
        if self.audit.tenant_id != self.identity.tenant_id
            || self.audit.worker_process_generation_id != self.fence.worker_process_generation_id
            || self.identity.physical_job_id.kind() != ResourceKind::Job
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || !matches!(
                self.target,
                SandboxJobState::Preparing | SandboxJobState::Starting
            )
            || self.audit.request_digest != self.canonical_request_digest()?
        {
            return Err(SandboxContractError::InvalidWorkerCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitManagedMcpSandboxSessionReady {
    pub audit: SandboxWorkerAudit,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub fence: JobFence,
    pub ready: ManagedMcpSandboxSessionReadyEvidence,
    pub phase_evidence_digest: Sha256Digest,
}

impl CommitManagedMcpSandboxSessionReady {
    pub fn canonical_request_digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        insight_platform_contracts::canonical_digest(&serde_json::json!({
            "fence": self.fence,
            "identity": self.identity,
            "phase_evidence_digest": self.phase_evidence_digest,
            "ready": self.ready,
            "schema_version": 1,
            "tenant_id": self.audit.tenant_id,
        }))
        .map_err(|_| SandboxContractError::InvalidWorkerCommand)?
        .parse()
        .map_err(|_| SandboxContractError::InvalidWorkerCommand)
    }

    pub fn validate_at(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        self.audit
            .validate_at(now)
            .map_err(|_| SandboxContractError::InvalidWorkerCommand)?;
        self.ready.validate_for(request, now)?;
        if self.audit.tenant_id != self.identity.tenant_id
            || self.audit.worker_process_generation_id != self.fence.worker_process_generation_id
            || self.identity != request.identity
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.audit.request_digest != self.canonical_request_digest()?
        {
            return Err(SandboxContractError::InvalidWorkerCommand);
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimedManagedMcpSandboxSession {
    pub request: ManagedMcpSandboxSessionRequest,
    pub fence: JobFence,
    pub usage_reservation_id: ResourceId,
}

impl ClaimedManagedMcpSandboxSession {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        self.request.validate_at(now, limits)?;
        if self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
        {
            return Err(SandboxContractError::InvalidWorkerCommand);
        }
        Ok(())
    }
}

/// Dedicated claim boundary for long-lived Managed MCP session Jobs. Keeping this separate from
/// Capability execution prevents the ordinary Sandbox worker from interpreting a session payload
/// as a finite execution, while both paths still consume the same `work_class=sandbox` pool.
#[async_trait]
pub trait ManagedMcpSandboxSessionClaimAuthority: Send + Sync + 'static {
    async fn claim_managed_mcp_sandbox_sessions(
        &self,
        command: ClaimSandboxJobs,
    ) -> Result<Vec<ClaimedManagedMcpSandboxSession>, SandboxClaimFailure>;
}

#[async_trait]
pub trait ManagedMcpSandboxSessionExecutionAuthority: Send + Sync {
    type Error: Send;

    async fn commit_managed_mcp_sandbox_session_phase(
        &self,
        command: CommitManagedMcpSandboxSessionPhase,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error>;

    async fn commit_managed_mcp_sandbox_session_ready(
        &self,
        command: CommitManagedMcpSandboxSessionReady,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error>;
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

pub fn decide_managed_mcp_sandbox_session_phase(
    current: &McpSubscriptionRecord,
    current_job: &JobProjection,
    current_payload: &ManagedMcpSandboxSessionJobPayload,
    command: &CommitManagedMcpSandboxSessionPhase,
    database_now: DateTime<Utc>,
    limits: SandboxCommandLimits,
) -> Result<ManagedMcpSandboxSessionPhaseDecision, SandboxContractError> {
    command.validate_at(database_now)?;
    validate_current_managed_session(
        current,
        current_job,
        current_payload,
        &command.identity,
        limits,
    )?;
    let physical_job = if command.target == SandboxJobState::Preparing {
        if current_payload.physical_state != SandboxJobState::Accepted {
            return Err(SandboxContractError::InvalidTransition);
        }
        decide_start(current_job, &command.fence, database_now)
            .map_err(|_| SandboxContractError::StaleFence)?
    } else {
        if current_payload.physical_state != SandboxJobState::Preparing
            || current_payload.executor_identity_digest.as_ref()
                != Some(&command.executor_identity_digest)
            || current_payload.attestor_route.as_ref() != Some(&command.attestor_route)
        {
            return Err(SandboxContractError::InvalidTransition);
        }
        decide_observation_update(current_job, &command.fence, database_now)
            .map_err(|_| SandboxContractError::StaleFence)?
    };
    let (logical_payload, logical_state) = if command.target == SandboxJobState::Starting {
        current
            .payload
            .transition_managed_sandbox_session(
                current.payload.session.version,
                &command.identity,
                insight_platform_contracts::McpSessionState::Initializing,
                None,
                current_payload
                    .request
                    .mcp_contract
                    .server
                    .limits
                    .maximum_session_milliseconds,
                database_now,
            )
            .map_err(|_| SandboxContractError::InvalidTransition)?
    } else {
        (current.payload.clone(), current.state)
    };
    let mut physical_payload = current_payload.clone();
    physical_payload.physical_state = command.target;
    physical_payload.phase_sequence = physical_payload
        .phase_sequence
        .checked_add(1)
        .ok_or(SandboxContractError::InvalidTransition)?;
    physical_payload.executor_identity_digest = Some(command.executor_identity_digest.clone());
    physical_payload.attestor_route = Some(command.attestor_route.clone());
    physical_payload.phase_evidence_digest = Some(command.phase_evidence_digest.clone());
    physical_payload = physical_payload.seal()?;
    physical_payload.validate_for(&physical_job, limits)?;
    Ok(ManagedMcpSandboxSessionPhaseDecision {
        logical_payload,
        logical_state,
        physical_job,
        physical_payload,
    })
}

pub fn decide_managed_mcp_sandbox_session_ready(
    current: &McpSubscriptionRecord,
    current_job: &JobProjection,
    current_payload: &ManagedMcpSandboxSessionJobPayload,
    command: &CommitManagedMcpSandboxSessionReady,
    database_now: DateTime<Utc>,
    limits: SandboxCommandLimits,
) -> Result<ManagedMcpSandboxSessionPhaseDecision, SandboxContractError> {
    command.validate_at(&current_payload.request, database_now)?;
    validate_current_managed_session(
        current,
        current_job,
        current_payload,
        &command.identity,
        limits,
    )?;
    if current_payload.physical_state != SandboxJobState::Starting
        || current.payload.session.state
            != insight_platform_contracts::McpSessionState::Initializing
    {
        return Err(SandboxContractError::InvalidTransition);
    }
    let physical_job = decide_observation_update(current_job, &command.fence, database_now)
        .map_err(|_| SandboxContractError::StaleFence)?;
    let (logical_payload, logical_state) = current
        .payload
        .transition_managed_sandbox_session(
            current.payload.session.version,
            &command.identity,
            insight_platform_contracts::McpSessionState::Ready,
            Some((
                command.ready.encrypted_opaque_session.clone(),
                command.ready.expires_at,
            )),
            current_payload
                .request
                .mcp_contract
                .server
                .limits
                .maximum_session_milliseconds,
            database_now,
        )
        .map_err(|_| SandboxContractError::InvalidTransition)?;
    let mut physical_payload = current_payload.clone();
    physical_payload.physical_state = SandboxJobState::Running;
    physical_payload.phase_sequence = physical_payload
        .phase_sequence
        .checked_add(1)
        .ok_or(SandboxContractError::InvalidTransition)?;
    physical_payload.phase_evidence_digest = Some(command.phase_evidence_digest.clone());
    physical_payload.ready_binding = Some(command.ready.durable_binding(&current_payload.request)?);
    physical_payload = physical_payload.seal()?;
    physical_payload.validate_for(&physical_job, limits)?;
    Ok(ManagedMcpSandboxSessionPhaseDecision {
        logical_payload,
        logical_state,
        physical_job,
        physical_payload,
    })
}

fn validate_current_managed_session(
    current: &McpSubscriptionRecord,
    current_job: &JobProjection,
    current_payload: &ManagedMcpSandboxSessionJobPayload,
    identity: &ManagedMcpSandboxSessionIdentity,
    limits: SandboxCommandLimits,
) -> Result<(), SandboxContractError> {
    current_payload.validate_for(current_job, limits)?;
    let link = current
        .payload
        .managed_sandbox_session
        .as_ref()
        .ok_or(SandboxContractError::InvalidExactBinding)?;
    if current.tenant_id != identity.tenant_id
        || current.subscription_id != identity.subscription_id
        || current.job_id != identity.logical_job_id
        || link.identity != *identity
        || link.sandbox_request_digest != current_payload.request.request_digest
        || current_payload.request.identity != *identity
        || current_job.job_id != identity.physical_job_id
    {
        return Err(SandboxContractError::InvalidExactBinding);
    }
    Ok(())
}

fn sorted_unique_ids<'a>(values: impl Iterator<Item = &'a ResourceId>) -> bool {
    let values = values.collect::<Vec<_>>();
    values.windows(2).all(|pair| pair[0] < pair[1])
        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
}
