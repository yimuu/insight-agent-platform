use crate::{
    digest_without_field, required_isolation, ClaimSandboxJobs, HeartbeatSandboxExecution,
    NodeAttestorRoute, SandboxClaimFailure, SandboxCleanupEvidence, SandboxCommandLimits,
    SandboxContractError, SandboxExecutionPolicyClosure, SandboxHeartbeatConfig,
    SandboxNetworkMode, SandboxResourceEnvelope, SandboxWorkerAudit, SandboxWorkerCommitIdentity,
    ScopedArtifactGrant, ScopedSecretGrant, MAX_SANDBOX_ARTIFACT_GRANTS, MAX_SANDBOX_SECRET_GRANTS,
    SANDBOX_PROTOCOL_VERSION, SANDBOX_QUOTA_LINES,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ArtifactGrantOperation, CommandOutcome, DataClassification, Effect, JobState, McpSessionState,
    ResourceDocument, ResourceId, ResourceKind, SandboxIsolationClass, SandboxJobState,
    SandboxRuntimeFamily, Sha256Digest, WorkClass,
};
use insight_platform_jobs::{
    decide_cleanup_reconciliation, decide_heartbeat, decide_observation_update, decide_start,
    JobFence, JobOwnerRef, JobProjection, LeasePolicy,
};
use insight_platform_mcp_host::{
    EncryptedMcpState, ManagedMcpSandboxSessionIdentity, McpHostExecutionContract,
    McpResourceSubscriptionBinding, McpSubscriptionRecord, McpSubscriptionState,
    McpSubscriptionWorkerAudit,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, fmt, future::Future, sync::Arc};

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
    /// Credential-free exact Provider allocation retained from `Starting` onward. Recovery must
    /// be able to address and validate the old instance without reconstructing evidence from an
    /// opaque digest after the lease token has expired.
    pub prepared_binding: Option<PreparedManagedMcpSandboxSession>,
    pub ready_binding: Option<ManagedMcpSandboxSessionReadyBinding>,
    pub cleanup: Option<SandboxCleanupEvidence>,
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
            prepared_binding: None,
            ready_binding: None,
            cleanup: None,
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
        let state_matches = match self.physical_state {
            SandboxJobState::Accepted => matches!(job.state, JobState::Ready | JobState::Leased),
            SandboxJobState::Preparing | SandboxJobState::Starting | SandboxJobState::Running => {
                job.state == JobState::Running
            }
            SandboxJobState::Lost => job.state == JobState::ReconciliationRequired,
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
                    || self.prepared_binding.is_some()
                    || self.ready_binding.is_some()
                    || self.cleanup.is_some()))
            || (matches!(
                self.physical_state,
                SandboxJobState::Preparing | SandboxJobState::Starting | SandboxJobState::Running
            ) && (self.phase_sequence == 0
                || self.executor_identity_digest.is_none()
                || self.attestor_route.is_none()
                || self.phase_evidence_digest.is_none()
                || self.cleanup.is_some()))
            || (self.physical_state == SandboxJobState::Lost
                && (self.phase_sequence < 2
                    || self.executor_identity_digest.is_none()
                    || self.attestor_route.is_none()
                    || self.phase_evidence_digest.is_none()
                    || self.cleanup.is_none()))
            || (self.physical_state == SandboxJobState::Preparing
                && self.prepared_binding.is_some())
            || (matches!(
                self.physical_state,
                SandboxJobState::Starting | SandboxJobState::Running
            ) && self.prepared_binding.is_none())
            || matches!(
                self.physical_state,
                SandboxJobState::Accepted | SandboxJobState::Preparing | SandboxJobState::Starting
            ) && self.ready_binding.is_some()
            || (self.physical_state == SandboxJobState::Running && self.ready_binding.is_none())
            || self
                .ready_binding
                .as_ref()
                .is_some_and(|binding| binding.validate_canonical_for(&self.request).is_err())
            || digest_without_field(self, "payload_digest")? != self.payload_digest
        {
            return Err(SandboxContractError::InvalidJob);
        }
        if let Some(prepared) = &self.prepared_binding {
            prepared.validate_durable_for(
                &self.request,
                self.executor_identity_digest
                    .as_ref()
                    .ok_or(SandboxContractError::InvalidOutcome)?,
            )?;
            if matches!(
                self.physical_state,
                SandboxJobState::Starting | SandboxJobState::Running
            ) {
                let lease = job.lease.as_ref().ok_or(SandboxContractError::InvalidJob)?;
                if prepared.worker_process_generation_id != lease.worker_process_generation_id
                    || prepared.lease_generation != lease.lease_generation
                {
                    return Err(SandboxContractError::InvalidJob);
                }
            }
        }
        if let Some(cleanup) = &self.cleanup {
            cleanup.validate_recovery()?;
            if self.ready_binding.as_ref().is_some_and(|ready| {
                ready.sandbox_identity_digest != cleanup.sandbox_identity_digest
            }) {
                return Err(SandboxContractError::InvalidOutcome);
            }
        }
        if self
            .prepared_binding
            .as_ref()
            .zip(self.ready_binding.as_ref())
            .is_some_and(|(prepared, ready)| {
                prepared.sandbox_identity_digest != ready.sandbox_identity_digest
                    || prepared.identity.canonical_digest != ready.session_identity_digest
            })
        {
            return Err(SandboxContractError::InvalidOutcome);
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub prepared_binding: Option<PreparedManagedMcpSandboxSession>,
}

impl CommitManagedMcpSandboxSessionPhase {
    pub fn canonical_request_digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        insight_platform_contracts::canonical_digest(&serde_json::json!({
            "attestor_route": self.attestor_route,
            "executor_identity_digest": self.executor_identity_digest,
            "fence": self.fence,
            "identity": self.identity,
            "phase_evidence_digest": self.phase_evidence_digest,
            "prepared_binding": self.prepared_binding,
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
            || (self.target == SandboxJobState::Preparing && self.prepared_binding.is_some())
            || (self.target == SandboxJobState::Starting
                && self.prepared_binding.as_ref().is_none_or(|prepared| {
                    prepared
                        .validate_shape_for(
                            &self.identity,
                            &self.fence,
                            &self.executor_identity_digest,
                        )
                        .is_err()
                }))
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitManagedMcpSandboxSessionLost {
    pub audit: SandboxWorkerAudit,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub fence: JobFence,
    pub cleanup: SandboxCleanupEvidence,
    pub session_loss_evidence_digest: Sha256Digest,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
}

impl CommitManagedMcpSandboxSessionLost {
    pub fn canonical_request_digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        insight_platform_contracts::canonical_digest(&serde_json::json!({
            "cleanup": self.cleanup,
            "fence": self.fence,
            "identity": self.identity,
            "quota_entry_ids": self.quota_entry_ids,
            "schema_version": 1,
            "session_loss_evidence_digest": self.session_loss_evidence_digest,
            "tenant_id": self.audit.tenant_id,
            "usage_reservation_id": self.usage_reservation_id,
        }))
        .map_err(|_| SandboxContractError::InvalidWorkerCommand)?
        .parse()
        .map_err(|_| SandboxContractError::InvalidWorkerCommand)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SandboxContractError> {
        self.audit
            .validate_at(now)
            .map_err(|_| SandboxContractError::InvalidWorkerCommand)?;
        self.cleanup.validate_recovery()?;
        if self.audit.tenant_id != self.identity.tenant_id
            || self.audit.worker_process_generation_id != self.fence.worker_process_generation_id
            || self.identity.physical_job_id.kind() != ResourceKind::Job
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.cleanup.observed_at > now
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.quota_entry_ids.len() != SANDBOX_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || !sorted_unique_ids(self.quota_entry_ids.iter())
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

    /// Renews only the shared physical Job lease. It creates no Receipt/Event/Outbox and cannot
    /// extend either the immutable Sandbox deadline or the Managed session expiry.
    async fn heartbeat_managed_mcp_sandbox_session(
        &self,
        command: HeartbeatSandboxExecution,
    ) -> Result<ManagedMcpSandboxSessionPhaseDecision, Self::Error>;

    /// Atomically terminalizes the physical Job and schedules a fresh logical generation only
    /// after exact Provider cleanup has proved the old instance cannot continue.
    async fn commit_managed_mcp_sandbox_session_lost(
        &self,
        command: CommitManagedMcpSandboxSessionLost,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error>;
}

/// Exact mutation identities consumed while establishing one Managed MCP session. Long-lived
/// terminal/recovery commits use their own identities and are intentionally not preallocated here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionWorkerAudits {
    pub preparing: SandboxWorkerCommitIdentity,
    pub starting: SandboxWorkerCommitIdentity,
    pub ready: SandboxWorkerCommitIdentity,
}

impl ManagedMcpSandboxSessionWorkerAudits {
    fn validate_at(
        &self,
        tenant_id: &ResourceId,
        worker_process_generation_id: &ResourceId,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        let audits = [&self.preparing, &self.starting, &self.ready];
        for audit in audits {
            audit
                .validate_at(now)
                .map_err(|_| SandboxContractError::InvalidWorkerCommand)?;
            if &audit.tenant_id != tenant_id
                || &audit.worker_process_generation_id != worker_process_generation_id
            {
                return Err(SandboxContractError::InvalidWorkerCommand);
            }
        }
        if audits
            .iter()
            .map(|audit| &audit.receipt_id)
            .collect::<BTreeSet<_>>()
            .len()
            != audits.len()
            || audits
                .iter()
                .map(|audit| &audit.event_id)
                .collect::<BTreeSet<_>>()
                .len()
                != audits.len()
            || audits
                .iter()
                .map(|audit| &audit.outbox_id)
                .collect::<BTreeSet<_>>()
                .len()
                != audits.len()
            || audits
                .iter()
                .map(|audit| &audit.idempotency_key_digest)
                .collect::<BTreeSet<_>>()
                .len()
                != audits.len()
        {
            return Err(SandboxContractError::InvalidWorkerCommand);
        }
        Ok(())
    }
}

/// Executor-local command built only after the dedicated Managed-session claim wins.
#[derive(Debug, Clone)]
pub struct ExecuteManagedMcpSandboxSession {
    pub claimed: ClaimedManagedMcpSandboxSession,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
    pub audits: ManagedMcpSandboxSessionWorkerAudits,
}

impl ExecuteManagedMcpSandboxSession {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        self.claimed.validate_at(now, limits)?;
        self.audits.validate_at(
            &self.claimed.request.identity.tenant_id,
            &self.claimed.fence.worker_process_generation_id,
            now,
        )
    }
}

/// Credential-free proof that the provider allocated the exact physical instance. An error from
/// `prepare` means the provider proved that no instance survived; once this value exists, every
/// later failure must call `destroy_exact` before the worker returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedManagedMcpSandboxSession {
    pub schema_version: u32,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub provider_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub executor_identity_digest: Sha256Digest,
    pub sandbox_identity_digest: Sha256Digest,
    pub prepare_evidence_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

impl PreparedManagedMcpSandboxSession {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.canonical_digest = digest_without_field(&self, "canonical_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        executor_identity_digest: &Sha256Digest,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != 1
            || self.identity != request.identity
            || self.request_digest != request.request_digest
            || self.worker_process_generation_id != fence.worker_process_generation_id
            || self.provider_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation != fence.lease_generation
            || &self.executor_identity_digest != executor_identity_digest
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }

    fn validate_shape_for(
        &self,
        identity: &ManagedMcpSandboxSessionIdentity,
        fence: &JobFence,
        executor_identity_digest: &Sha256Digest,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != 1
            || &self.identity != identity
            || self.worker_process_generation_id != fence.worker_process_generation_id
            || self.provider_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation != fence.lease_generation
            || &self.executor_identity_digest != executor_identity_digest
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }

    fn validate_durable_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        executor_identity_digest: &Sha256Digest,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != 1
            || self.identity != request.identity
            || self.request_digest != request.request_digest
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.provider_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
            || &self.executor_identity_digest != executor_identity_digest
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

/// One fail-closed Secret delivery attempt for a prepared Managed MCP microVM.
///
/// The request is credential-free. The Egress Broker first reserves the exact read with the
/// Controller, resolves the Secret through its existing KMS/Provider composition, then commits
/// delivery before releasing bytes to the Provider. A replay never receives a second
/// authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSecretDeliveryRequest {
    pub schema_version: u32,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub request_digest: Sha256Digest,
    pub fence: JobFence,
    pub prepared: PreparedManagedMcpSandboxSession,
    pub secret_grant: ScopedSecretGrant,
    pub canonical_digest: Sha256Digest,
}

impl ManagedMcpSandboxSecretDeliveryRequest {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.canonical_digest = digest_without_field(&self, "canonical_digest")?;
        Ok(self)
    }

    pub fn validate_shape(&self) -> Result<(), SandboxContractError> {
        self.secret_grant.validate_for(
            &self.identity.tenant_id,
            &self.identity.sandbox_job_id,
            &self.secret_grant.runtime_digest,
            self.secret_grant.expires_at,
        )?;
        if self.schema_version != 1
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.identity != self.prepared.identity
            || self.request_digest != self.prepared.request_digest
            || self.fence.worker_process_generation_id != self.prepared.worker_process_generation_id
            || self.fence.lease_generation != self.prepared.lease_generation
            || self.fence.expected_version == 0
            || self.secret_grant.tenant_id != self.identity.tenant_id
            || self.secret_grant.sandbox_job_id != self.identity.sandbox_job_id
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(SandboxContractError::InvalidSecretGrant);
        }
        Ok(())
    }
}

/// Controller-issued authorization for exactly one fresh receipt reservation. It contains no
/// provider reference or Secret bytes and is never returned for a replayed reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizedManagedMcpSandboxSecretDelivery {
    pub schema_version: u32,
    pub receipt_id: ResourceId,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub secret_binding: insight_platform_contracts::ExactSecretBindingRef,
    pub resolved_binding_generation: u64,
    pub delivery_request_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
    pub authorization_digest: Sha256Digest,
}

impl AuthorizedManagedMcpSandboxSecretDelivery {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.authorization_digest = digest_without_field(&self, "authorization_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSecretDeliveryRequest,
    ) -> Result<(), SandboxContractError> {
        request.validate_shape()?;
        if self.schema_version != 1
            || self.receipt_id != request.receipt_id
            || self.tenant_id != request.identity.tenant_id
            || self.sandbox_job_id != request.identity.sandbox_job_id
            || self.secret_binding != request.secret_grant.secret_binding
            || self.resolved_binding_generation != request.secret_grant.resolved_binding_generation
            || self.delivery_request_digest != request.canonical_digest
            || self.expires_at != request.secret_grant.expires_at
            || digest_without_field(self, "authorization_digest")? != self.authorization_digest
        {
            return Err(SandboxContractError::InvalidSecretGrant);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedMcpSandboxSecretReservationOutcome {
    Authorized(Box<AuthorizedManagedMcpSandboxSecretDelivery>),
    AlreadyReserved,
    AlreadyDelivered,
}

/// Credential-free durable proof that the Egress Broker completed resolution and the Controller
/// revalidated the current Job/lease/grant immediately before material release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSecretDeliveryEvidence {
    pub schema_version: u32,
    pub receipt_id: ResourceId,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub secret_binding_id: ResourceId,
    pub resolved_binding_generation: u64,
    pub authorization_digest: Sha256Digest,
    pub resolution_evidence_digest: Sha256Digest,
    pub delivered_at: DateTime<Utc>,
    pub evidence_digest: Sha256Digest,
}

impl ManagedMcpSandboxSecretDeliveryEvidence {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.evidence_digest = digest_without_field(&self, "evidence_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSecretDeliveryRequest,
        authorization: &AuthorizedManagedMcpSandboxSecretDelivery,
    ) -> Result<(), SandboxContractError> {
        authorization.validate_for(request)?;
        if self.schema_version != 1
            || self.receipt_id != request.receipt_id
            || self.tenant_id != request.identity.tenant_id
            || self.sandbox_job_id != request.identity.sandbox_job_id
            || self.secret_binding_id != request.secret_grant.secret_binding.secret_binding_id
            || self.resolved_binding_generation != request.secret_grant.resolved_binding_generation
            || self.authorization_digest != authorization.authorization_digest
            || self.delivered_at >= authorization.expires_at
            || digest_without_field(self, "evidence_digest")? != self.evidence_digest
        {
            return Err(SandboxContractError::InvalidSecretGrant);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedMcpSandboxSecretCommitOutcome {
    Delivered(ManagedMcpSandboxSecretDeliveryEvidence),
    Replayed(ManagedMcpSandboxSecretDeliveryEvidence),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedMcpSandboxSecretDeliveryError {
    Unavailable,
    Denied,
    OutcomeUncertain,
}

/// Credential-free locator sealed by the Egress cryptographic boundary after a Managed MCP
/// guest has initialized. The plaintext never contains a Secret or a process/socket path; it is
/// only the exact durable-to-physical fence needed to reject cross-session state substitution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxOpaqueSessionClaims {
    pub schema_version: u32,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub provider_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub sandbox_identity_digest: Sha256Digest,
    pub protocol_evidence_digest: Sha256Digest,
    pub established_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl ManagedMcpSandboxOpaqueSessionClaims {
    pub fn validate_shape(&self, now: DateTime<Utc>) -> Result<(), SandboxContractError> {
        let maximum_expiry = self
            .established_at
            .checked_add_signed(chrono::Duration::milliseconds(
                i64::try_from(insight_platform_contracts::MAX_MCP_SESSION_MILLISECONDS)
                    .map_err(|_| SandboxContractError::InvalidCallback)?,
            ))
            .ok_or(SandboxContractError::InvalidCallback)?;
        if self.schema_version != 1
            || self.identity.schema_version != 1
            || self.identity.tenant_id.kind() != ResourceKind::Tenant
            || self.identity.subscription_id.kind() != ResourceKind::McpOperation
            || self.identity.logical_job_id.kind() != ResourceKind::Job
            || self.identity.sandbox_job_id.kind() != ResourceKind::SandboxJob
            || self.identity.physical_job_id.kind() != ResourceKind::Job
            || self.identity.sandbox_job_id.uuid() != self.identity.physical_job_id.uuid()
            || self.identity.admitted_subscription_version == 0
            || self.identity.admitted_logical_job_version == 0
            || self.identity.session_generation == 0
            || digest_without_field(&self.identity, "canonical_digest")?
                != self.identity.canonical_digest
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.provider_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
            || self.established_at > now
            || self.expires_at <= now
            || self.expires_at <= self.established_at
            || self.expires_at > maximum_expiry
        {
            return Err(SandboxContractError::InvalidCallback);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        self.validate_shape(now)?;
        prepared.validate_for(request, fence, &prepared.executor_identity_digest)?;
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
            || self.identity != request.identity
            || self.request_digest != request.request_digest
            || self.worker_process_generation_id != fence.worker_process_generation_id
            || self.worker_process_generation_id != prepared.worker_process_generation_id
            || self.provider_process_generation_id != prepared.provider_process_generation_id
            || self.lease_generation != fence.lease_generation
            || self.lease_generation != prepared.lease_generation
            || self.sandbox_identity_digest != prepared.sandbox_identity_digest
            || self.expires_at > maximum_expiry
            || self.expires_at > request.deadline
        {
            return Err(SandboxContractError::InvalidCallback);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        let value =
            serde_json::to_value(self).map_err(|_| SandboxContractError::InvalidCallback)?;
        insight_platform_contracts::canonical_digest(&value)
            .map_err(|_| SandboxContractError::InvalidCallback)?
            .parse()
            .map_err(|_| SandboxContractError::InvalidCallback)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedMcpSandboxSessionStateSealError {
    Unavailable,
    Denied,
}

/// Cryptographic port implemented only by the independently deployed Egress Broker. The
/// Provider can request sealing but never receives the Candidate state key material.
#[async_trait]
pub trait ManagedMcpSandboxSessionStateSealer: Send + Sync {
    async fn seal_managed_mcp_sandbox_session_state(
        &self,
        claims: ManagedMcpSandboxOpaqueSessionClaims,
    ) -> Result<EncryptedMcpState, ManagedMcpSandboxSessionStateSealError>;
}

/// Durable Controller authority used only by the Egress Broker. PostgreSQL stores one bounded
/// receipt per read, so `maximum_reads` is enforced without a Secret-specific table or mutation
/// of the Job version fence.
#[async_trait]
pub trait ManagedMcpSandboxSecretDeliveryAuthority: Send + Sync {
    async fn reserve_managed_mcp_sandbox_secret_delivery(
        &self,
        request: &ManagedMcpSandboxSecretDeliveryRequest,
    ) -> Result<ManagedMcpSandboxSecretReservationOutcome, ManagedMcpSandboxSecretDeliveryError>;

    async fn commit_managed_mcp_sandbox_secret_delivery(
        &self,
        request: &ManagedMcpSandboxSecretDeliveryRequest,
        authorization: &AuthorizedManagedMcpSandboxSecretDelivery,
        resolution_evidence_digest: &Sha256Digest,
    ) -> Result<ManagedMcpSandboxSecretCommitOutcome, ManagedMcpSandboxSecretDeliveryError>;
}

/// Provider-side prepared activation. The provider retains the live byte stream behind this
/// credential-free binding; the worker may only release it after durable Ready commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedManagedMcpSandboxSessionActivation {
    pub schema_version: u32,
    pub prepared: PreparedManagedMcpSandboxSession,
    pub ready: ManagedMcpSandboxSessionReadyEvidence,
    pub activation_binding_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

impl PreparedManagedMcpSandboxSessionActivation {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.canonical_digest = digest_without_field(&self, "canonical_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        executor_identity_digest: &Sha256Digest,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        self.prepared
            .validate_for(request, fence, executor_identity_digest)?;
        self.ready.validate_for(request, now)?;
        if self.schema_version != 1
            || self.ready.sandbox_identity_digest != self.prepared.sandbox_identity_digest
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

/// Evidence produced only after the provider released notification delivery on the same prepared
/// instance. Durable Ready remains the authority; this value is execution-plane observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatedManagedMcpSandboxSession {
    pub schema_version: u32,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub sandbox_identity_digest: Sha256Digest,
    pub ready_evidence_digest: Sha256Digest,
    pub activated_at: DateTime<Utc>,
    pub activation_evidence_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

impl ActivatedManagedMcpSandboxSession {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.canonical_digest = digest_without_field(&self, "canonical_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        activation: &PreparedManagedMcpSandboxSessionActivation,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != 1
            || self.identity != activation.prepared.identity
            || self.request_digest != activation.prepared.request_digest
            || self.worker_process_generation_id != activation.prepared.worker_process_generation_id
            || self.lease_generation != activation.prepared.lease_generation
            || self.sandbox_identity_digest != activation.prepared.sandbox_identity_digest
            || self.ready_evidence_digest != activation.ready.canonical_digest
            || self.activated_at < activation.ready.established_at
            || self.activated_at > now
            || self.activated_at >= activation.ready.expires_at
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedMcpSandboxSessionLiveness {
    Alive,
    Exited,
}

/// Exact Provider observation for an activated Managed MCP microVM. Transport failure is not an
/// observation and therefore can never be translated into `Exited` by an Executor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionLivenessEvidence {
    pub schema_version: u32,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub provider_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub executor_identity_digest: Sha256Digest,
    pub sandbox_identity_digest: Sha256Digest,
    pub liveness: ManagedMcpSandboxSessionLiveness,
    pub observed_at: DateTime<Utc>,
    pub evidence_digest: Sha256Digest,
}

impl ManagedMcpSandboxSessionLivenessEvidence {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.evidence_digest = digest_without_field(&self, "evidence_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        activated: &ActivatedManagedMcpSandboxSession,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        prepared.validate_for(request, fence, &prepared.executor_identity_digest)?;
        if self.schema_version != 1
            || self.identity != request.identity
            || self.request_digest != request.request_digest
            || self.worker_process_generation_id != fence.worker_process_generation_id
            || self.provider_process_generation_id != prepared.provider_process_generation_id
            || self.lease_generation != fence.lease_generation
            || self.executor_identity_digest != prepared.executor_identity_digest
            || self.sandbox_identity_digest != prepared.sandbox_identity_digest
            || activated.identity != request.identity
            || activated.request_digest != request.request_digest
            || activated.worker_process_generation_id != fence.worker_process_generation_id
            || activated.lease_generation != fence.lease_generation
            || activated.sandbox_identity_digest != prepared.sandbox_identity_digest
            || self.observed_at < activated.activated_at
            || self.observed_at > now
            || digest_without_field(self, "evidence_digest")? != self.evidence_digest
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

/// Proof that exact cleanup either found no keyed instance or destroyed the prepared instance.
/// Only `Destroyed` can authorize terminalization of a session that reached `Prepared`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "disposition",
    content = "evidence",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ManagedMcpSandboxSessionCleanupOutcome {
    Absent(ManagedMcpSandboxSessionAbsenceEvidence),
    Destroyed(ManagedMcpSandboxSessionDestroyedEvidence),
}

impl ManagedMcpSandboxSessionCleanupOutcome {
    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: Option<&PreparedManagedMcpSandboxSession>,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        match self {
            Self::Absent(evidence) => {
                if prepared.is_some() {
                    return Err(SandboxContractError::InvalidOutcome);
                }
                evidence.validate_for(request, fence, now)
            }
            Self::Destroyed(evidence) => evidence.validate_for(request, fence, prepared, now),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionAbsenceEvidence {
    pub schema_version: u32,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub observed_at: DateTime<Utc>,
    pub evidence_digest: Sha256Digest,
}

impl ManagedMcpSandboxSessionAbsenceEvidence {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.evidence_digest = digest_without_field(&self, "evidence_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != 1
            || self.identity != request.identity
            || self.request_digest != request.request_digest
            || self.worker_process_generation_id != fence.worker_process_generation_id
            || self.lease_generation != fence.lease_generation
            || self.observed_at > now
            || digest_without_field(self, "evidence_digest")? != self.evidence_digest
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionDestroyedEvidence {
    pub schema_version: u32,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub provider_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub executor_identity_digest: Sha256Digest,
    pub sandbox_identity_digest: Sha256Digest,
    pub cleanup: SandboxCleanupEvidence,
    pub evidence_digest: Sha256Digest,
}

impl ManagedMcpSandboxSessionDestroyedEvidence {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.evidence_digest = digest_without_field(&self, "evidence_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: Option<&PreparedManagedMcpSandboxSession>,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        self.cleanup.validate_recovery()?;
        if self.schema_version != 1
            || self.identity != request.identity
            || self.request_digest != request.request_digest
            || self.worker_process_generation_id != fence.worker_process_generation_id
            || self.provider_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation != fence.lease_generation
            || self.sandbox_identity_digest != self.cleanup.sandbox_identity_digest
            || self.cleanup.observed_at > now
            || prepared.is_some_and(|prepared| {
                prepared.identity != self.identity
                    || prepared.request_digest != self.request_digest
                    || prepared.worker_process_generation_id != self.worker_process_generation_id
                    || prepared.provider_process_generation_id
                        != self.provider_process_generation_id
                    || prepared.lease_generation != self.lease_generation
                    || prepared.executor_identity_digest != self.executor_identity_digest
                    || prepared.sandbox_identity_digest != self.sandbox_identity_digest
            })
            || digest_without_field(self, "evidence_digest")? != self.evidence_digest
        {
            return Err(SandboxContractError::InvalidOutcome);
        }
        Ok(())
    }
}

/// Physical Managed-session provider boundary. Implementations run only in the Sandbox execution
/// plane. `initialize` must keep notification delivery disabled, and `activate` must address the
/// exact same instance represented by `PreparedManagedMcpSandboxSessionActivation`.
#[async_trait]
pub trait ManagedMcpSandboxSessionProvider: Send + Sync {
    type Error: Send;

    async fn prepare(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        executor_identity_digest: &Sha256Digest,
    ) -> Result<PreparedManagedMcpSandboxSession, Self::Error>;

    async fn initialize(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
    ) -> Result<PreparedManagedMcpSandboxSessionActivation, Self::Error>;

    async fn activate(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        activation: &PreparedManagedMcpSandboxSessionActivation,
    ) -> Result<ActivatedManagedMcpSandboxSession, Self::Error>;

    async fn observe_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        activated: &ActivatedManagedMcpSandboxSession,
    ) -> Result<ManagedMcpSandboxSessionLivenessEvidence, Self::Error>;

    /// Destroys the exact deterministic request/fence instance. `prepared` is absent only when a
    /// remote prepare response was lost; implementations must still prove the keyed instance is
    /// absent before returning success. This makes a transport timeout fail closed instead of
    /// leaking an untracked microVM.
    async fn destroy_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: Option<&PreparedManagedMcpSandboxSession>,
    ) -> Result<ManagedMcpSandboxSessionCleanupOutcome, Self::Error>;
}

/// Establishment worker that enforces the only valid activation order:
///
/// `commit Preparing -> provider prepare -> commit Starting -> provider initialize ->`
/// `commit Ready -> provider activate`.
pub struct ManagedMcpSandboxSessionWorker<A, P> {
    authority: Arc<A>,
    provider: Arc<P>,
    limits: SandboxCommandLimits,
    heartbeat: SandboxHeartbeatConfig,
}

impl<A, P> ManagedMcpSandboxSessionWorker<A, P> {
    pub fn new(authority: Arc<A>, provider: Arc<P>, limits: SandboxCommandLimits) -> Self {
        Self {
            authority,
            provider,
            limits,
            heartbeat: SandboxHeartbeatConfig::from_profile(
                &insight_platform_contracts::checked_in_hard_limit_profile(),
            )
            .expect("checked-in HardLimitProfile has valid Sandbox heartbeat timing"),
        }
    }

    pub fn with_heartbeat(
        authority: Arc<A>,
        provider: Arc<P>,
        limits: SandboxCommandLimits,
        heartbeat: SandboxHeartbeatConfig,
    ) -> Self {
        Self {
            authority,
            provider,
            limits,
            heartbeat,
        }
    }
}

/// Exact established generation retained by the long-lived Executor supervisor. In particular,
/// the latest Job fence includes every heartbeat that won while Provider I/O was in flight; using
/// the original claim fence for a later heartbeat, terminal commit or cleanup is always stale.
#[derive(Debug, Clone)]
pub struct EstablishedManagedMcpSandboxSession {
    pub request: ManagedMcpSandboxSessionRequest,
    pub fence: JobFence,
    pub usage_reservation_id: ResourceId,
    pub prepared: PreparedManagedMcpSandboxSession,
    pub activated: ActivatedManagedMcpSandboxSession,
}

impl EstablishedManagedMcpSandboxSession {
    pub fn apply_running_heartbeat(
        &mut self,
        decision: &ManagedMcpSandboxSessionPhaseDecision,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        validate_managed_mcp_authority_decision(
            decision,
            &self.request,
            SandboxJobState::Running,
            McpSessionState::Ready,
            limits,
        )?;
        self.fence = next_managed_mcp_fence(decision, &self.fence);
        Ok(())
    }

    pub fn validate_lost_decision(
        &self,
        decision: &ManagedMcpSandboxSessionPhaseDecision,
        cleanup: &SandboxCleanupEvidence,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        decision
            .physical_payload
            .validate_for(&decision.physical_job, limits)?;
        if decision.physical_job.job_id != self.request.identity.physical_job_id
            || decision.physical_job.state != JobState::ReconciliationRequired
            || decision.physical_payload.request.as_ref() != &self.request
            || decision.physical_payload.physical_state != SandboxJobState::Lost
            || decision.physical_payload.cleanup.as_ref() != Some(cleanup)
            || decision.logical_state != McpSubscriptionState::Pending
            || decision.logical_payload.managed_sandbox_session.is_some()
            || !decision.logical_payload.full_reconcile_required
            || decision.logical_payload.session.state != McpSessionState::Disconnected
            || decision
                .logical_payload
                .session
                .encrypted_opaque_session
                .is_some()
        {
            return Err(SandboxContractError::InvalidExactBinding);
        }
        Ok(())
    }
}

enum ManagedMcpProviderStep<T, AuthorityError, ProviderError> {
    Completed(Result<T, ProviderError>),
    HeartbeatFailed {
        failure: ManagedMcpHeartbeatFailure<AuthorityError>,
        completion: Result<T, ProviderError>,
    },
}

enum ManagedMcpHeartbeatFailure<AuthorityError> {
    Contract(SandboxContractError),
    Authority(AuthorityError),
}

impl<A, P> ManagedMcpSandboxSessionWorker<A, P>
where
    A: ManagedMcpSandboxSessionExecutionAuthority,
    P: ManagedMcpSandboxSessionProvider,
{
    pub async fn establish(
        &self,
        command: ExecuteManagedMcpSandboxSession,
    ) -> Result<
        EstablishedManagedMcpSandboxSession,
        ManagedMcpSandboxSessionWorkerError<A::Error, P::Error>,
    > {
        command
            .validate_at(Utc::now(), self.limits)
            .map_err(ManagedMcpSandboxSessionWorkerError::Contract)?;
        let request = &command.claimed.request;
        let mut fence = command.claimed.fence.clone();

        let preparing_evidence = managed_mcp_prepare_intent_digest(&command)
            .map_err(ManagedMcpSandboxSessionWorkerError::Contract)?;
        let preparing = managed_mcp_phase_command(
            &command.audits.preparing,
            request,
            &fence,
            &command.executor_identity_digest,
            &command.attestor_route,
            ManagedMcpPhaseCommit {
                target: SandboxJobState::Preparing,
                evidence_digest: preparing_evidence,
                prepared_binding: None,
            },
        )
        .map_err(ManagedMcpSandboxSessionWorkerError::Contract)?;
        let preparing = self
            .authority
            .commit_managed_mcp_sandbox_session_phase(preparing)
            .await
            .map_err(ManagedMcpSandboxSessionWorkerError::Authority)?;
        let preparing = managed_mcp_outcome_ref(&preparing);
        validate_managed_mcp_authority_decision(
            preparing,
            request,
            SandboxJobState::Preparing,
            McpSessionState::Connecting,
            self.limits,
        )
        .map_err(ManagedMcpSandboxSessionWorkerError::Contract)?;
        fence = next_managed_mcp_fence(preparing, &fence);

        let prepare_fence = fence.clone();
        let prepared = match self
            .await_provider_with_heartbeats(
                request,
                &mut fence,
                SandboxJobState::Preparing,
                McpSessionState::Connecting,
                self.provider
                    .prepare(request, &prepare_fence, &command.executor_identity_digest),
            )
            .await
        {
            ManagedMcpProviderStep::Completed(Ok(prepared)) => prepared,
            ManagedMcpProviderStep::Completed(Err(provider)) => {
                return Err(ManagedMcpSandboxSessionWorkerError::Provider(provider));
            }
            ManagedMcpProviderStep::HeartbeatFailed {
                failure,
                completion,
            } => {
                return Err(match completion {
                    Ok(prepared) => {
                        self.cleanup_after_heartbeat_failure(request, &fence, &prepared, failure)
                            .await
                    }
                    Err(_) => managed_mcp_heartbeat_failure(failure),
                });
            }
        };
        if let Err(contract) =
            prepared.validate_for(request, &fence, &command.executor_identity_digest)
        {
            return Err(
                match self
                    .provider
                    .destroy_exact(request, &fence, Some(&prepared))
                    .await
                {
                    Ok(_) => ManagedMcpSandboxSessionWorkerError::Contract(contract),
                    Err(cleanup) => ManagedMcpSandboxSessionWorkerError::CleanupAfterContract {
                        contract,
                        cleanup,
                    },
                },
            );
        }

        let starting = match managed_mcp_phase_command(
            &command.audits.starting,
            request,
            &fence,
            &command.executor_identity_digest,
            &command.attestor_route,
            ManagedMcpPhaseCommit {
                target: SandboxJobState::Starting,
                evidence_digest: prepared.canonical_digest.clone(),
                prepared_binding: Some(prepared.clone()),
            },
        ) {
            Ok(command) => command,
            Err(contract) => {
                return Err(
                    match self
                        .provider
                        .destroy_exact(request, &fence, Some(&prepared))
                        .await
                    {
                        Ok(_) => ManagedMcpSandboxSessionWorkerError::Contract(contract),
                        Err(cleanup) => ManagedMcpSandboxSessionWorkerError::CleanupAfterContract {
                            contract,
                            cleanup,
                        },
                    },
                );
            }
        };
        let starting = match self
            .authority
            .commit_managed_mcp_sandbox_session_phase(starting)
            .await
        {
            Ok(outcome) => outcome,
            Err(authority) => {
                return Err(
                    match self
                        .provider
                        .destroy_exact(request, &fence, Some(&prepared))
                        .await
                    {
                        Ok(_) => ManagedMcpSandboxSessionWorkerError::Authority(authority),
                        Err(cleanup) => {
                            ManagedMcpSandboxSessionWorkerError::CleanupAfterAuthority {
                                authority,
                                cleanup,
                            }
                        }
                    },
                );
            }
        };
        let starting = managed_mcp_outcome_ref(&starting);
        if let Err(contract) = validate_managed_mcp_authority_decision(
            starting,
            request,
            SandboxJobState::Starting,
            McpSessionState::Initializing,
            self.limits,
        ) {
            return Err(
                match self
                    .provider
                    .destroy_exact(request, &fence, Some(&prepared))
                    .await
                {
                    Ok(_) => ManagedMcpSandboxSessionWorkerError::Contract(contract),
                    Err(cleanup) => ManagedMcpSandboxSessionWorkerError::CleanupAfterContract {
                        contract,
                        cleanup,
                    },
                },
            );
        }
        fence = next_managed_mcp_fence(starting, &fence);

        let initialize_fence = fence.clone();
        let activation = match self
            .await_provider_with_heartbeats(
                request,
                &mut fence,
                SandboxJobState::Starting,
                McpSessionState::Initializing,
                self.provider
                    .initialize(request, &initialize_fence, &prepared),
            )
            .await
        {
            ManagedMcpProviderStep::Completed(Ok(activation)) => activation,
            ManagedMcpProviderStep::Completed(Err(provider)) => {
                return Err(
                    match self
                        .provider
                        .destroy_exact(request, &fence, Some(&prepared))
                        .await
                    {
                        Ok(_) => ManagedMcpSandboxSessionWorkerError::Provider(provider),
                        Err(cleanup) => ManagedMcpSandboxSessionWorkerError::CleanupAfterProvider {
                            provider,
                            cleanup,
                        },
                    },
                );
            }
            ManagedMcpProviderStep::HeartbeatFailed {
                failure,
                completion: _,
            } => {
                return Err(self
                    .cleanup_after_heartbeat_failure(request, &fence, &prepared, failure)
                    .await);
            }
        };
        if let Err(contract) = activation.validate_for(
            request,
            &fence,
            &command.executor_identity_digest,
            Utc::now(),
        ) {
            return Err(
                match self
                    .provider
                    .destroy_exact(request, &fence, Some(&prepared))
                    .await
                {
                    Ok(_) => ManagedMcpSandboxSessionWorkerError::Contract(contract),
                    Err(cleanup) => ManagedMcpSandboxSessionWorkerError::CleanupAfterContract {
                        contract,
                        cleanup,
                    },
                },
            );
        }

        let ready = match managed_mcp_ready_command(
            &command.audits.ready,
            request,
            &fence,
            activation.ready.clone(),
            activation.canonical_digest.clone(),
        ) {
            Ok(command) => command,
            Err(contract) => {
                return Err(
                    match self
                        .provider
                        .destroy_exact(request, &fence, Some(&prepared))
                        .await
                    {
                        Ok(_) => ManagedMcpSandboxSessionWorkerError::Contract(contract),
                        Err(cleanup) => ManagedMcpSandboxSessionWorkerError::CleanupAfterContract {
                            contract,
                            cleanup,
                        },
                    },
                );
            }
        };
        let ready = match self
            .authority
            .commit_managed_mcp_sandbox_session_ready(ready)
            .await
        {
            Ok(outcome) => outcome,
            Err(authority) => {
                return Err(
                    match self
                        .provider
                        .destroy_exact(request, &fence, Some(&prepared))
                        .await
                    {
                        Ok(_) => ManagedMcpSandboxSessionWorkerError::Authority(authority),
                        Err(cleanup) => {
                            ManagedMcpSandboxSessionWorkerError::CleanupAfterAuthority {
                                authority,
                                cleanup,
                            }
                        }
                    },
                );
            }
        };
        let ready = managed_mcp_outcome_ref(&ready);
        if let Err(contract) = validate_managed_mcp_authority_decision(
            ready,
            request,
            SandboxJobState::Running,
            McpSessionState::Ready,
            self.limits,
        ) {
            return Err(
                match self
                    .provider
                    .destroy_exact(request, &fence, Some(&prepared))
                    .await
                {
                    Ok(_) => ManagedMcpSandboxSessionWorkerError::Contract(contract),
                    Err(cleanup) => ManagedMcpSandboxSessionWorkerError::CleanupAfterContract {
                        contract,
                        cleanup,
                    },
                },
            );
        }
        fence = next_managed_mcp_fence(ready, &fence);

        let activate_fence = fence.clone();
        let activated = match self
            .await_provider_with_heartbeats(
                request,
                &mut fence,
                SandboxJobState::Running,
                McpSessionState::Ready,
                self.provider
                    .activate(request, &activate_fence, &activation),
            )
            .await
        {
            ManagedMcpProviderStep::Completed(Ok(activated)) => activated,
            ManagedMcpProviderStep::Completed(Err(provider)) => {
                return Err(
                    match self
                        .provider
                        .destroy_exact(request, &fence, Some(&prepared))
                        .await
                    {
                        Ok(_) => ManagedMcpSandboxSessionWorkerError::Provider(provider),
                        Err(cleanup) => ManagedMcpSandboxSessionWorkerError::CleanupAfterProvider {
                            provider,
                            cleanup,
                        },
                    },
                );
            }
            ManagedMcpProviderStep::HeartbeatFailed {
                failure,
                completion: _,
            } => {
                return Err(self
                    .cleanup_after_heartbeat_failure(request, &fence, &prepared, failure)
                    .await);
            }
        };
        if let Err(contract) = activated.validate_for(&activation, Utc::now()) {
            return Err(
                match self
                    .provider
                    .destroy_exact(request, &fence, Some(&prepared))
                    .await
                {
                    Ok(_) => ManagedMcpSandboxSessionWorkerError::Contract(contract),
                    Err(cleanup) => ManagedMcpSandboxSessionWorkerError::CleanupAfterContract {
                        contract,
                        cleanup,
                    },
                },
            );
        }
        Ok(EstablishedManagedMcpSandboxSession {
            request: command.claimed.request,
            fence,
            usage_reservation_id: command.claimed.usage_reservation_id,
            prepared,
            activated,
        })
    }

    async fn await_provider_with_heartbeats<T, F>(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &mut JobFence,
        physical_state: SandboxJobState,
        logical_state: McpSessionState,
        provider: F,
    ) -> ManagedMcpProviderStep<T, A::Error, P::Error>
    where
        F: Future<Output = Result<T, P::Error>>,
    {
        tokio::pin!(provider);
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + self.heartbeat.interval(),
            self.heartbeat.interval(),
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                completion = &mut provider => {
                    return ManagedMcpProviderStep::Completed(completion);
                }
                _ = heartbeat.tick() => {
                    let heartbeat = HeartbeatSandboxExecution {
                        tenant_id: request.identity.tenant_id.clone(),
                        sandbox_job_id: request.identity.sandbox_job_id.clone(),
                        job_id: request.identity.physical_job_id.clone(),
                        fence: fence.clone(),
                        lease_milliseconds: self.heartbeat.lease_milliseconds(),
                    };
                    let decision = match self
                        .authority
                        .heartbeat_managed_mcp_sandbox_session(heartbeat)
                        .await
                    {
                        Ok(decision) => decision,
                        Err(authority) => {
                            let completion = provider.await;
                            return ManagedMcpProviderStep::HeartbeatFailed {
                                failure: ManagedMcpHeartbeatFailure::Authority(authority),
                                completion,
                            };
                        }
                    };
                    if let Err(contract) = validate_managed_mcp_authority_decision(
                        &decision,
                        request,
                        physical_state,
                        logical_state,
                        self.limits,
                    ) {
                        let completion = provider.await;
                        return ManagedMcpProviderStep::HeartbeatFailed {
                            failure: ManagedMcpHeartbeatFailure::Contract(contract),
                            completion,
                        };
                    }
                    *fence = next_managed_mcp_fence(&decision, fence);
                }
            }
        }
    }

    async fn cleanup_after_heartbeat_failure(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        failure: ManagedMcpHeartbeatFailure<A::Error>,
    ) -> ManagedMcpSandboxSessionWorkerError<A::Error, P::Error> {
        match (
            failure,
            self.provider
                .destroy_exact(request, fence, Some(prepared))
                .await,
        ) {
            (ManagedMcpHeartbeatFailure::Contract(contract), Ok(_)) => {
                ManagedMcpSandboxSessionWorkerError::Contract(contract)
            }
            (ManagedMcpHeartbeatFailure::Authority(authority), Ok(_)) => {
                ManagedMcpSandboxSessionWorkerError::Authority(authority)
            }
            (ManagedMcpHeartbeatFailure::Contract(contract), Err(cleanup)) => {
                ManagedMcpSandboxSessionWorkerError::CleanupAfterContract { contract, cleanup }
            }
            (ManagedMcpHeartbeatFailure::Authority(authority), Err(cleanup)) => {
                ManagedMcpSandboxSessionWorkerError::CleanupAfterAuthority { authority, cleanup }
            }
        }
    }
}

fn managed_mcp_heartbeat_failure<AuthorityError, ProviderError>(
    failure: ManagedMcpHeartbeatFailure<AuthorityError>,
) -> ManagedMcpSandboxSessionWorkerError<AuthorityError, ProviderError> {
    match failure {
        ManagedMcpHeartbeatFailure::Contract(contract) => {
            ManagedMcpSandboxSessionWorkerError::Contract(contract)
        }
        ManagedMcpHeartbeatFailure::Authority(authority) => {
            ManagedMcpSandboxSessionWorkerError::Authority(authority)
        }
    }
}

#[derive(Debug)]
pub enum ManagedMcpSandboxSessionWorkerError<AuthorityError, ProviderError> {
    Contract(SandboxContractError),
    Authority(AuthorityError),
    Provider(ProviderError),
    CleanupAfterContract {
        contract: SandboxContractError,
        cleanup: ProviderError,
    },
    CleanupAfterAuthority {
        authority: AuthorityError,
        cleanup: ProviderError,
    },
    CleanupAfterProvider {
        provider: ProviderError,
        cleanup: ProviderError,
    },
}

impl<AuthorityError, ProviderError> fmt::Display
    for ManagedMcpSandboxSessionWorkerError<AuthorityError, ProviderError>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Contract(_) => "Managed MCP session worker rejected its contract",
            Self::Authority(_) => "Managed MCP session durable authority failed",
            Self::Provider(_) => "Managed MCP session provider failed",
            Self::CleanupAfterContract { .. }
            | Self::CleanupAfterAuthority { .. }
            | Self::CleanupAfterProvider { .. } => {
                "Managed MCP session provider cleanup failed after establishment failure"
            }
        })
    }
}

impl<AuthorityError, ProviderError> Error
    for ManagedMcpSandboxSessionWorkerError<AuthorityError, ProviderError>
where
    AuthorityError: Error + 'static,
    ProviderError: Error + 'static,
{
}

fn managed_mcp_outcome_ref<T>(outcome: &CommandOutcome<T>) -> &T {
    match outcome {
        CommandOutcome::Applied(value) | CommandOutcome::Replayed(value) => value,
    }
}

fn next_managed_mcp_fence(
    decision: &ManagedMcpSandboxSessionPhaseDecision,
    prior: &JobFence,
) -> JobFence {
    JobFence {
        expected_version: decision.physical_job.version,
        worker_process_generation_id: prior.worker_process_generation_id.clone(),
        lease_generation: prior.lease_generation,
        token_digest: prior.token_digest.clone(),
    }
}

fn managed_mcp_prepare_intent_digest(
    command: &ExecuteManagedMcpSandboxSession,
) -> Result<Sha256Digest, SandboxContractError> {
    insight_platform_contracts::canonical_digest(&serde_json::json!({
        "attestor_route": command.attestor_route,
        "domain": "managed_mcp_sandbox_session_prepare_intent",
        "executor_identity_digest": command.executor_identity_digest,
        "fence": command.claimed.fence,
        "identity": command.claimed.request.identity,
        "request_digest": command.claimed.request.request_digest,
        "schema_version": 1,
    }))
    .map_err(|_| SandboxContractError::Canonicalization)?
    .parse()
    .map_err(|_| SandboxContractError::Canonicalization)
}

struct ManagedMcpPhaseCommit {
    target: SandboxJobState,
    evidence_digest: Sha256Digest,
    prepared_binding: Option<PreparedManagedMcpSandboxSession>,
}

fn managed_mcp_phase_command(
    audit: &SandboxWorkerCommitIdentity,
    request: &ManagedMcpSandboxSessionRequest,
    fence: &JobFence,
    executor_identity_digest: &Sha256Digest,
    attestor_route: &NodeAttestorRoute,
    phase: ManagedMcpPhaseCommit,
) -> Result<CommitManagedMcpSandboxSessionPhase, SandboxContractError> {
    let mut command = CommitManagedMcpSandboxSessionPhase {
        audit: audit.materialize(audit.idempotency_key_digest.clone()),
        identity: request.identity.clone(),
        fence: fence.clone(),
        target: phase.target,
        executor_identity_digest: executor_identity_digest.clone(),
        attestor_route: attestor_route.clone(),
        phase_evidence_digest: phase.evidence_digest,
        prepared_binding: phase.prepared_binding,
    };
    command.audit.request_digest = command.canonical_request_digest()?;
    command.validate_at(Utc::now())?;
    Ok(command)
}

fn managed_mcp_ready_command(
    audit: &SandboxWorkerCommitIdentity,
    request: &ManagedMcpSandboxSessionRequest,
    fence: &JobFence,
    ready: ManagedMcpSandboxSessionReadyEvidence,
    phase_evidence_digest: Sha256Digest,
) -> Result<CommitManagedMcpSandboxSessionReady, SandboxContractError> {
    let mut command = CommitManagedMcpSandboxSessionReady {
        audit: audit.materialize(audit.idempotency_key_digest.clone()),
        identity: request.identity.clone(),
        fence: fence.clone(),
        ready,
        phase_evidence_digest,
    };
    command.audit.request_digest = command.canonical_request_digest()?;
    command.validate_at(request, Utc::now())?;
    Ok(command)
}

fn validate_managed_mcp_authority_decision(
    decision: &ManagedMcpSandboxSessionPhaseDecision,
    request: &ManagedMcpSandboxSessionRequest,
    physical_state: SandboxJobState,
    logical_state: insight_platform_contracts::McpSessionState,
    limits: SandboxCommandLimits,
) -> Result<(), SandboxContractError> {
    decision
        .physical_payload
        .validate_for(&decision.physical_job, limits)?;
    let link = decision
        .logical_payload
        .managed_sandbox_session
        .as_ref()
        .ok_or(SandboxContractError::InvalidExactBinding)?;
    if decision.physical_job.job_id != request.identity.physical_job_id
        || decision.physical_payload.request.as_ref() != request
        || decision.physical_payload.physical_state != physical_state
        || decision.logical_payload.session.state != logical_state
        || link.identity != request.identity
        || link.sandbox_request_digest != request.request_digest
    {
        return Err(SandboxContractError::InvalidExactBinding);
    }
    Ok(())
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
            || command.prepared_binding.as_ref().is_none_or(|prepared| {
                prepared
                    .validate_for(
                        &current_payload.request,
                        &command.fence,
                        &command.executor_identity_digest,
                    )
                    .is_err()
                    || prepared.canonical_digest != command.phase_evidence_digest
            })
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
    physical_payload.prepared_binding = command.prepared_binding.clone();
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

pub fn decide_managed_mcp_sandbox_session_heartbeat(
    current: &McpSubscriptionRecord,
    current_job: &JobProjection,
    current_payload: &ManagedMcpSandboxSessionJobPayload,
    command: &HeartbeatSandboxExecution,
    database_now: DateTime<Utc>,
    limits: SandboxCommandLimits,
    maximum_lease_milliseconds: u64,
) -> Result<ManagedMcpSandboxSessionPhaseDecision, SandboxContractError> {
    command
        .validate(maximum_lease_milliseconds)
        .map_err(|_| SandboxContractError::InvalidWorkerCommand)?;
    validate_current_managed_session(
        current,
        current_job,
        current_payload,
        &current_payload.request.identity,
        limits,
    )?;
    if command.tenant_id != current_payload.request.identity.tenant_id
        || command.sandbox_job_id != current_payload.request.identity.sandbox_job_id
        || command.job_id != current_payload.request.identity.physical_job_id
        || !matches!(
            current_payload.physical_state,
            SandboxJobState::Accepted
                | SandboxJobState::Preparing
                | SandboxJobState::Starting
                | SandboxJobState::Running
        )
    {
        return Err(SandboxContractError::InvalidWorkerCommand);
    }
    let physical_job = decide_heartbeat(
        current_job,
        &command.fence,
        database_now,
        LeasePolicy {
            requested_milliseconds: command.lease_milliseconds,
            hard_maximum_milliseconds: maximum_lease_milliseconds,
        },
    )
    .map_err(|_| SandboxContractError::StaleFence)?;
    current_payload.validate_for(&physical_job, limits)?;
    Ok(ManagedMcpSandboxSessionPhaseDecision {
        logical_payload: current.payload.clone(),
        logical_state: current.state,
        physical_job,
        physical_payload: current_payload.clone(),
    })
}

pub fn decide_managed_mcp_sandbox_session_lost(
    current: &McpSubscriptionRecord,
    current_job: &JobProjection,
    current_payload: &ManagedMcpSandboxSessionJobPayload,
    command: &CommitManagedMcpSandboxSessionLost,
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
    if !matches!(
        current_payload.physical_state,
        SandboxJobState::Preparing | SandboxJobState::Starting | SandboxJobState::Running
    ) || current_payload.ready_binding.as_ref().is_some_and(|ready| {
        ready.sandbox_identity_digest != command.cleanup.sandbox_identity_digest
    }) {
        return Err(SandboxContractError::InvalidTransition);
    }
    let cleanup_deadline = current_payload
        .request
        .deadline
        .checked_add_signed(chrono::Duration::milliseconds(
            i64::try_from(current_payload.request.resources.cleanup_milliseconds)
                .map_err(|_| SandboxContractError::InvalidOutcome)?,
        ))
        .ok_or(SandboxContractError::InvalidOutcome)?;
    if command.cleanup.observed_at > cleanup_deadline {
        return Err(SandboxContractError::InvalidOutcome);
    }
    let physical_job =
        decide_cleanup_reconciliation(current_job, &command.fence, database_now, cleanup_deadline)
            .map_err(|_| SandboxContractError::StaleFence)?;
    let (logical_payload, logical_state) = current
        .payload
        .rebuild_managed_sandbox_session_after_loss(
            current.payload.session.version,
            &command.identity,
            database_now,
        )
        .map_err(|_| SandboxContractError::InvalidTransition)?;
    let mut physical_payload = current_payload.clone();
    physical_payload.physical_state = SandboxJobState::Lost;
    physical_payload.phase_sequence = physical_payload
        .phase_sequence
        .checked_add(1)
        .ok_or(SandboxContractError::InvalidTransition)?;
    physical_payload.phase_evidence_digest = Some(command.session_loss_evidence_digest.clone());
    physical_payload.cleanup = Some(command.cleanup.clone());
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
