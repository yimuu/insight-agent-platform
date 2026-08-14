use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ArtifactGrantOperation, ArtifactRef, CapabilityBackendBinding,
    CapabilityBackendContract, CapabilityBackendKind, CapabilityDeploymentClosure,
    CapabilityImplementationResourceSpec, CapabilityInterfaceResourceSpec, CodeTrustClass,
    DataClassification, Effect, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef,
    HardLimitProfile, JsonLimits, ResourceDocument, ResourceId, ResourceKind, SandboxAbiVersion,
    SandboxArtifactIoPolicyDocument, SandboxIsolationClass, SandboxIsolationPolicyDocument,
    SandboxNetworkPolicyDocument, SandboxPackageResourceSpec, SandboxProfileResourceSpec,
    SandboxResourcePolicyDocument, SandboxRuntimeFamily, SandboxRuntimeResourceSpec,
    SandboxSecretResolutionPolicyDocument, Sha256Digest, ValueRef,
};
use insight_platform_mcp_host::{McpHostExecutionContract, McpLogicalOperationRequest};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, error::Error, fmt};

pub const SANDBOX_PROTOCOL_VERSION: SandboxAbiVersion = SandboxAbiVersion::V1;
pub const MAX_SANDBOX_ARTIFACT_GRANTS: usize = 64;
pub const MAX_SANDBOX_SECRET_GRANTS: usize = 32;
pub const MAX_SANDBOX_PORT_BYTES: usize = 128;
pub const MAX_SANDBOX_SAFE_CODE_BYTES: usize = 128;
pub const MAX_SANDBOX_SAFE_MESSAGE_BYTES: usize = 512;

/// Closed logical source for one physical Sandbox attempt.
///
/// A Managed MCP stdio implementation remains logically `Mcp`, but it must enter the Sandbox
/// work class directly. Keeping the exact MCP closure in this tagged source prevents a generic
/// Capability worker from becoming a second physical Job owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "execution",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SandboxExecutionSource {
    SandboxCapability {
        capability_deployment_closure: Box<CapabilityDeploymentClosure>,
    },
    ManagedMcp {
        capability_deployment_closure: Box<CapabilityDeploymentClosure>,
        capability_interface_revision: ExactVersionRef,
        capability_interface: Box<CapabilityInterfaceResourceSpec>,
        capability_implementation_revision: ExactVersionRef,
        capability_implementation: Box<CapabilityImplementationResourceSpec>,
        mcp_contract: Box<McpHostExecutionContract>,
        operation: Box<McpLogicalOperationRequest>,
    },
}

impl SandboxExecutionSource {
    pub const fn capability_deployment_closure(&self) -> &CapabilityDeploymentClosure {
        match self {
            Self::SandboxCapability {
                capability_deployment_closure,
            }
            | Self::ManagedMcp {
                capability_deployment_closure,
                ..
            } => capability_deployment_closure,
        }
    }

    pub const fn capability_binding(&self) -> &CapabilityBackendBinding {
        &self.capability_deployment_closure().backend
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxCommandLimits {
    pub maximum_input_bytes: u64,
    pub maximum_output_bytes: u64,
    pub maximum_cpu_millicores: u32,
    pub maximum_memory_mebibytes: u32,
    pub maximum_pids: u32,
    pub maximum_files: u32,
    pub maximum_io_bytes: u64,
    pub maximum_wall_milliseconds: u64,
    pub maximum_cleanup_milliseconds: u64,
}

impl SandboxCommandLimits {
    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, SandboxContractError> {
        profile
            .validate()
            .map_err(|_| SandboxContractError::InvalidLimits)?;
        let values = &profile.capability_sandbox;
        Ok(Self {
            maximum_input_bytes: values.input_bytes.hard_max,
            maximum_output_bytes: values.output_bytes.hard_max,
            maximum_cpu_millicores: u32::try_from(values.cpu_millicores.hard_max)
                .map_err(|_| SandboxContractError::InvalidLimits)?,
            maximum_memory_mebibytes: u32::try_from(values.memory_mebibytes.hard_max)
                .map_err(|_| SandboxContractError::InvalidLimits)?,
            maximum_pids: u32::try_from(values.pids.hard_max)
                .map_err(|_| SandboxContractError::InvalidLimits)?,
            maximum_files: u32::try_from(values.files.hard_max)
                .map_err(|_| SandboxContractError::InvalidLimits)?,
            maximum_io_bytes: values.io_bytes.hard_max,
            maximum_wall_milliseconds: values
                .wall_seconds
                .hard_max
                .checked_mul(1_000)
                .ok_or(SandboxContractError::InvalidLimits)?,
            maximum_cleanup_milliseconds: values
                .cleanup_seconds
                .hard_max
                .checked_mul(1_000)
                .ok_or(SandboxContractError::InvalidLimits)?,
        })
    }

    pub fn inline_json_limits(self) -> JsonLimits {
        JsonLimits {
            max_bytes: usize::try_from(self.maximum_input_bytes).unwrap_or(usize::MAX),
            max_depth: 128,
            max_items_per_array: 100_000,
            max_properties_per_object: 100_000,
            max_string_bytes: usize::try_from(self.maximum_input_bytes).unwrap_or(usize::MAX),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxNetworkMode {
    None,
    BrokeredEgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxResourceEnvelope {
    pub cpu_millicores: u32,
    pub memory_mebibytes: u32,
    pub pids: u32,
    pub files: u32,
    pub io_bytes: u64,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub result_bytes: u64,
    pub artifact_output_bytes: u64,
    pub network_connections: u32,
    pub network_request_bytes: u64,
    pub network_response_bytes: u64,
    pub startup_milliseconds: u64,
    pub idle_milliseconds: u64,
    pub wall_milliseconds: u64,
    pub cleanup_milliseconds: u64,
    pub wasm_fuel: Option<u64>,
    pub wasm_memory_pages: Option<u32>,
}

impl SandboxResourceEnvelope {
    pub fn validate(
        &self,
        isolation: SandboxIsolationClass,
        network_mode: SandboxNetworkMode,
        profile_max_job_duration_milliseconds: u64,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        let total_output = self
            .stdout_bytes
            .checked_add(self.stderr_bytes)
            .and_then(|value| value.checked_add(self.result_bytes))
            .and_then(|value| value.checked_add(self.artifact_output_bytes))
            .ok_or(SandboxContractError::InvalidResourceEnvelope)?;
        if self.cpu_millicores == 0
            || self.cpu_millicores > limits.maximum_cpu_millicores
            || self.memory_mebibytes == 0
            || self.memory_mebibytes > limits.maximum_memory_mebibytes
            || self.pids == 0
            || self.pids > limits.maximum_pids
            || self.files == 0
            || self.files > limits.maximum_files
            || self.io_bytes == 0
            || self.io_bytes > limits.maximum_io_bytes
            || self.result_bytes == 0
            || total_output > limits.maximum_output_bytes
            || self.startup_milliseconds == 0
            || self.idle_milliseconds == 0
            || self.wall_milliseconds == 0
            || self.wall_milliseconds > limits.maximum_wall_milliseconds
            || self.wall_milliseconds > profile_max_job_duration_milliseconds
            || self.startup_milliseconds > self.wall_milliseconds
            || self.idle_milliseconds > self.wall_milliseconds
            || self.cleanup_milliseconds == 0
            || self.cleanup_milliseconds > limits.maximum_cleanup_milliseconds
            || (network_mode == SandboxNetworkMode::None
                && (self.network_connections != 0
                    || self.network_request_bytes != 0
                    || self.network_response_bytes != 0))
            || (network_mode == SandboxNetworkMode::BrokeredEgress
                && (self.network_connections == 0
                    || self.network_request_bytes == 0
                    || self.network_response_bytes == 0))
            || (isolation == SandboxIsolationClass::Wasm)
                != (self.wasm_fuel.is_some() && self.wasm_memory_pages.is_some())
            || self.wasm_fuel.is_some_and(|value| value == 0)
            || self.wasm_memory_pages.is_some_and(|value| value == 0)
            || self.wasm_memory_pages.is_some_and(|pages| {
                u64::from(pages) > u64::from(self.memory_mebibytes).saturating_mul(16)
            })
        {
            return Err(SandboxContractError::InvalidResourceEnvelope);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeSandboxTraceContext {
    pub trace_id_digest: Sha256Digest,
    pub parent_span_id_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedArtifactGrant {
    pub schema_version: u32,
    pub grant_id: ResourceId,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub operation: ArtifactGrantOperation,
    pub port: String,
    pub artifact: Option<ArtifactRef>,
    pub staging_artifact_id: Option<ResourceId>,
    pub byte_range: Option<SandboxArtifactByteRange>,
    pub maximum_bytes: u64,
    pub generation: u64,
    pub expires_at: DateTime<Utc>,
    pub grant_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxArtifactByteRange {
    pub offset: u64,
    pub length: u64,
}

impl SandboxArtifactByteRange {
    fn validate_for(self, artifact: &ArtifactRef, maximum_bytes: u64) -> bool {
        self.length > 0
            && self.length <= maximum_bytes
            && self
                .offset
                .checked_add(self.length)
                .is_some_and(|end| end <= artifact.byte_length())
    }
}

impl ScopedArtifactGrant {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.grant_digest = digest_without_field(&self, "grant_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        tenant_id: &ResourceId,
        sandbox_job_id: &ResourceId,
        deadline: DateTime<Utc>,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        let operation_shape_valid = match self.operation {
            ArtifactGrantOperation::ReadWhole => self.artifact.as_ref().is_some_and(|artifact| {
                self.staging_artifact_id.is_none()
                    && self.byte_range.is_none()
                    && self.maximum_bytes >= artifact.byte_length()
            }),
            ArtifactGrantOperation::ReadRange => self
                .artifact
                .as_ref()
                .zip(self.byte_range)
                .is_some_and(|(artifact, range)| {
                    self.staging_artifact_id.is_none()
                        && range.validate_for(artifact, self.maximum_bytes)
                }),
            ArtifactGrantOperation::WriteStaging | ArtifactGrantOperation::CommitStaging => {
                self.artifact.is_none()
                    && self.byte_range.is_none()
                    && self
                        .staging_artifact_id
                        .as_ref()
                        .is_some_and(|id| id.kind() == ResourceKind::Artifact)
            }
        };
        if self.schema_version != 1
            || self.grant_id.kind() != ResourceKind::ArtifactGrant
            || &self.tenant_id != tenant_id
            || &self.sandbox_job_id != sandbox_job_id
            || !stable_code(&self.port, MAX_SANDBOX_PORT_BYTES)
            || self.maximum_bytes == 0
            || self.maximum_bytes > limits.maximum_output_bytes.max(limits.maximum_input_bytes)
            || self.generation == 0
            || self.expires_at > deadline
            || !operation_shape_valid
            || self
                .artifact
                .as_ref()
                .is_some_and(|artifact| artifact.validate().is_err())
            || digest_without_field(self, "grant_digest")? != self.grant_digest
        {
            return Err(SandboxContractError::InvalidArtifactGrant);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedSecretGrant {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub secret_binding: ExactSecretBindingRef,
    pub resolved_binding_generation: u64,
    pub runtime_digest: Sha256Digest,
    pub maximum_reads: u32,
    pub expires_at: DateTime<Utc>,
    pub grant_digest: Sha256Digest,
}

impl ScopedSecretGrant {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.grant_digest = digest_without_field(&self, "grant_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        tenant_id: &ResourceId,
        sandbox_job_id: &ResourceId,
        runtime_digest: &Sha256Digest,
        deadline: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != 1
            || &self.tenant_id != tenant_id
            || &self.sandbox_job_id != sandbox_job_id
            || !self.secret_binding.permits_resolved_generation(
                &self.secret_binding.secret_binding_id,
                &self.secret_binding.purpose,
                self.resolved_binding_generation,
            )
            || &self.runtime_digest != runtime_digest
            || self.maximum_reads == 0
            || self.maximum_reads > 16
            || self.expires_at > deadline
            || digest_without_field(self, "grant_digest")? != self.grant_digest
        {
            return Err(SandboxContractError::InvalidSecretGrant);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedSandboxCallback {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub invocation_id: ResourceId,
    pub attempt_no: u32,
    pub audience_identity_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
    pub binding_digest: Sha256Digest,
}

impl ScopedSandboxCallback {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.binding_digest = digest_without_field(&self, "binding_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        tenant_id: &ResourceId,
        sandbox_job_id: &ResourceId,
        invocation_id: &ResourceId,
        attempt_no: u32,
        deadline: DateTime<Utc>,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != 1
            || &self.tenant_id != tenant_id
            || &self.sandbox_job_id != sandbox_job_id
            || &self.invocation_id != invocation_id
            || self.attempt_no != attempt_no
            || self.expires_at > deadline
            || digest_without_field(self, "binding_digest")? != self.binding_digest
        {
            return Err(SandboxContractError::InvalidCallback);
        }
        Ok(())
    }
}

/// Closed, provider-enforceable policy documents for one Sandbox execution.
///
/// Exact Policy Revision identities remain frozen in `SandboxProfileResourceSpec`. Admission loads
/// those revisions and proves that each document below is the document published by that exact
/// revision before a physical Job can be created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxExecutionPolicyClosure {
    pub isolation: SandboxIsolationPolicyDocument,
    pub resource: SandboxResourcePolicyDocument,
    pub network: SandboxNetworkPolicyDocument,
    pub artifact_io: SandboxArtifactIoPolicyDocument,
    pub secret_resolution: Option<SandboxSecretResolutionPolicyDocument>,
}

impl SandboxExecutionPolicyClosure {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn validate_for(
        &self,
        profile: &SandboxProfileResourceSpec,
        runtime: &SandboxRuntimeResourceSpec,
        package: &SandboxPackageResourceSpec,
        isolation_class: SandboxIsolationClass,
        network_mode: SandboxNetworkMode,
        resources: &SandboxResourceEnvelope,
        artifact_grants: &[ScopedArtifactGrant],
        secret_grants: &[ScopedSecretGrant],
    ) -> Result<(), SandboxContractError> {
        self.isolation
            .validate()
            .map_err(|_| SandboxContractError::InvalidPolicyClosure)?;
        self.resource
            .validate()
            .map_err(|_| SandboxContractError::InvalidPolicyClosure)?;
        self.network
            .validate()
            .map_err(|_| SandboxContractError::InvalidPolicyClosure)?;
        self.artifact_io
            .validate()
            .map_err(|_| SandboxContractError::InvalidPolicyClosure)?;
        if let Some(policy) = &self.secret_resolution {
            policy
                .validate()
                .map_err(|_| SandboxContractError::InvalidPolicyClosure)?;
        }

        let input_grants = artifact_grants
            .iter()
            .filter(|grant| {
                matches!(
                    grant.operation,
                    ArtifactGrantOperation::ReadWhole | ArtifactGrantOperation::ReadRange
                )
            })
            .count();
        let output_artifacts = artifact_grants
            .iter()
            .filter_map(|grant| match grant.operation {
                ArtifactGrantOperation::WriteStaging | ArtifactGrantOperation::CommitStaging => {
                    grant.staging_artifact_id.as_ref()
                }
                ArtifactGrantOperation::ReadWhole | ArtifactGrantOperation::ReadRange => None,
            })
            .collect::<BTreeSet<_>>()
            .len();
        let wasm_limits_match = match (resources.wasm_fuel, resources.wasm_memory_pages) {
            (Some(fuel), Some(pages)) => {
                self.resource
                    .maximum_wasm_fuel
                    .is_some_and(|maximum| fuel <= maximum)
                    && self
                        .resource
                        .maximum_wasm_memory_pages
                        .is_some_and(|maximum| pages <= maximum)
            }
            (None, None) => true,
            _ => false,
        };
        let secret_policy_matches = match &self.secret_resolution {
            Some(policy) => secret_grants.iter().all(|grant| {
                policy
                    .allowed_purposes
                    .contains(&grant.secret_binding.purpose)
                    && grant.maximum_reads <= policy.maximum_reads_per_binding
            }),
            None => secret_grants.is_empty(),
        };
        if profile.minimum_isolation.security_rank()
            < self.isolation.minimum_isolation.security_rank()
            || isolation_class.security_rank() < self.isolation.minimum_isolation.security_rank()
            || !profile
                .allowed_runtime_families
                .iter()
                .all(|family| self.isolation.allowed_runtime_families.contains(family))
            || !profile
                .allowed_trust_classes
                .iter()
                .all(|trust| self.isolation.allowed_trust_classes.contains(trust))
            || !self
                .isolation
                .allowed_runtime_families
                .contains(&runtime.runtime_family)
            || !self
                .isolation
                .allowed_trust_classes
                .contains(&package.trust_class)
            || resources.cpu_millicores > self.resource.maximum_cpu_millicores
            || resources.memory_mebibytes > self.resource.maximum_memory_mebibytes
            || resources.pids > self.resource.maximum_pids
            || resources.files > self.resource.maximum_files
            || resources.io_bytes > self.resource.maximum_io_bytes
            || resources.stdout_bytes > self.resource.maximum_stdout_bytes
            || resources.stderr_bytes > self.resource.maximum_stderr_bytes
            || resources.result_bytes > self.resource.maximum_result_bytes
            || resources.artifact_output_bytes > self.resource.maximum_artifact_output_bytes
            || resources.network_connections > self.resource.maximum_network_connections
            || resources.network_request_bytes > self.resource.maximum_network_request_bytes
            || resources.network_response_bytes > self.resource.maximum_network_response_bytes
            || resources.startup_milliseconds > self.resource.maximum_startup_milliseconds
            || resources.idle_milliseconds > self.resource.maximum_idle_milliseconds
            || resources.wall_milliseconds > self.resource.maximum_wall_milliseconds
            || resources.cleanup_milliseconds > self.resource.maximum_cleanup_milliseconds
            || !wasm_limits_match
            || (network_mode == SandboxNetworkMode::BrokeredEgress
                && (self.network.destinations.is_empty()
                    || resources.network_response_bytes > self.network.maximum_response_bytes))
            || u32::try_from(input_grants).map_or(true, |count| {
                count > self.artifact_io.maximum_input_artifacts
            })
            || u32::try_from(output_artifacts).map_or(true, |count| {
                count > self.artifact_io.maximum_output_artifacts
            })
            || artifact_grants.iter().any(|grant| {
                grant.artifact.as_ref().is_some_and(|artifact| {
                    !self
                        .artifact_io
                        .allowed_input_media_types
                        .iter()
                        .any(|media_type| media_type == artifact.media_type())
                })
            })
            || self.secret_resolution.is_some() != profile.secret_policy.is_some()
            || !secret_policy_matches
        {
            return Err(SandboxContractError::InvalidPolicyClosure);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxExecutionRequest {
    pub schema_version: u32,
    pub protocol_version: SandboxAbiVersion,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub expected_invocation_version: u64,
    pub attempt_no: u32,
    pub retry_backoff_milliseconds: u64,
    pub lease_generation: u64,
    pub capability_deployment: ExactDeploymentRef,
    pub execution_source: SandboxExecutionSource,
    pub runtime_revision: ExactVersionRef,
    pub runtime: SandboxRuntimeResourceSpec,
    pub package_revision: ExactVersionRef,
    pub package: SandboxPackageResourceSpec,
    pub profile_revision: ExactVersionRef,
    pub profile: SandboxProfileResourceSpec,
    pub policies: Box<SandboxExecutionPolicyClosure>,
    pub isolation_class: SandboxIsolationClass,
    pub executor_worker_manifest_digest: Sha256Digest,
    pub isolation_backend_contract_digest: Sha256Digest,
    pub effect: Effect,
    pub classification: DataClassification,
    pub input_value_id: ResourceId,
    pub input_schema_digest: Sha256Digest,
    pub input_ref: ValueRef,
    pub output_value_id: ResourceId,
    pub output_schema_digest: Sha256Digest,
    pub artifact_grants: Vec<ScopedArtifactGrant>,
    pub secret_grants: Vec<ScopedSecretGrant>,
    pub network_mode: SandboxNetworkMode,
    pub resources: SandboxResourceEnvelope,
    pub deadline: DateTime<Utc>,
    pub callback: ScopedSandboxCallback,
    pub trace_context: SafeSandboxTraceContext,
    pub request_digest: Sha256Digest,
}

impl SandboxExecutionRequest {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.request_digest = digest_without_field(&self, "request_digest")?;
        Ok(self)
    }

    pub fn bind_lease_generation(
        mut self,
        lease_generation: u64,
    ) -> Result<Self, SandboxContractError> {
        if self.lease_generation != 0 || lease_generation == 0 {
            return Err(SandboxContractError::InvalidExecutionRequest);
        }
        self.lease_generation = lease_generation;
        self.seal()
    }

    pub fn validate_submission_at(
        &self,
        now: DateTime<Utc>,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        self.validate_common_at(now, limits, false)
    }

    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: SandboxCommandLimits,
    ) -> Result<(), SandboxContractError> {
        self.validate_common_at(now, limits, true)
    }

    fn validate_common_at(
        &self,
        now: DateTime<Utc>,
        limits: SandboxCommandLimits,
        leased: bool,
    ) -> Result<(), SandboxContractError> {
        self.capability_deployment
            .validate()
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
        self.execution_source
            .capability_deployment_closure()
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
        let (
            runtime,
            package,
            profile,
            isolation,
            network_policy,
            resource_policy,
            artifact_io_policy,
            secret_policy,
        ) = match &self.execution_source {
            SandboxExecutionSource::SandboxCapability {
                capability_deployment_closure,
            } => {
                let CapabilityBackendBinding::Sandbox {
                    runtime,
                    package,
                    profile,
                    isolation,
                    network_policy,
                    resource_policy,
                    artifact_io_policy,
                    secret_policy,
                } = &capability_deployment_closure.backend
                else {
                    return Err(SandboxContractError::InvalidExactBinding);
                };
                (
                    runtime,
                    package,
                    profile,
                    isolation,
                    network_policy,
                    resource_policy,
                    artifact_io_policy,
                    secret_policy,
                )
            }
            SandboxExecutionSource::ManagedMcp {
                capability_deployment_closure,
                capability_interface_revision,
                capability_interface,
                capability_implementation_revision,
                capability_implementation,
                mcp_contract,
                operation,
            } => {
                let CapabilityBackendBinding::Mcp {
                    mcp_deployment,
                    discovery_snapshot_id,
                    discovery_snapshot_digest,
                    authorization_policy,
                } = &capability_deployment_closure.backend
                else {
                    return Err(SandboxContractError::InvalidExactBinding);
                };
                let insight_platform_contracts::McpTransportBinding::ManagedStdio {
                    package,
                    runtime,
                    profile,
                    isolation,
                    isolation_policy,
                    resource_policy,
                    artifact_io_policy,
                } = &mcp_contract.deployment_closure.transport
                else {
                    return Err(SandboxContractError::InvalidExactBinding);
                };
                mcp_contract
                    .validate_canonical_at(now)
                    .map_err(|_| SandboxContractError::InvalidExactBinding)?;
                operation
                    .validate_for(mcp_contract, now)
                    .map_err(|_| SandboxContractError::InvalidExecutionRequest)?;
                ResourceDocument::CapabilityInterface((**capability_interface).clone())
                    .validate()
                    .map_err(|_| SandboxContractError::InvalidResourceContract)?;
                ResourceDocument::CapabilityImplementation((**capability_implementation).clone())
                    .validate()
                    .map_err(|_| SandboxContractError::InvalidResourceContract)?;
                let CapabilityBackendContract::Mcp(tool_contract) =
                    &capability_implementation.backend_contract
                else {
                    return Err(SandboxContractError::InvalidExactBinding);
                };
                if capability_deployment_closure.interface != *capability_interface_revision
                    || capability_deployment_closure.implementation
                        != *capability_implementation_revision
                    || mcp_deployment != &mcp_contract.deployment
                    || discovery_snapshot_id != &mcp_contract.discovery.snapshot_id
                    || discovery_snapshot_digest != &mcp_contract.discovery.canonical_digest
                    || mcp_contract.deployment_closure.auth_policy.as_ref()
                        != Some(authorization_policy)
                    || operation.tenant_id != self.tenant_id
                    || operation.invocation_id != self.invocation_id
                    || operation.job_id != self.job_id
                    || operation.physical_attempt != self.attempt_no
                    || operation.deadline != self.deadline
                    || operation.effect != capability_interface.effect
                    || operation.idempotency != capability_interface.idempotency
                    || capability_interface.input_schema.canonical_digest
                        != self.input_schema_digest
                    || capability_interface.output_schema.canonical_digest
                        != self.output_schema_digest
                    || matches!(
                        &self.input_ref,
                        ValueRef::Inline { value }
                            if capability_interface.input_schema.validate_instance(value).is_err()
                    )
                    || capability_implementation_revision.resource_kind
                        != ResourceKind::CapabilityImplementationRevision
                    || capability_interface_revision.resource_kind
                        != ResourceKind::CapabilityInterfaceRevision
                    || capability_implementation.interface_revision
                        != *capability_interface_revision
                    || capability_implementation.backend_kind != CapabilityBackendKind::Mcp
                    || capability_implementation.backend_contract_digest
                        != capability_implementation
                            .backend_contract
                            .canonical_digest()
                            .map_err(|_| SandboxContractError::InvalidExactBinding)?
                    || tool_contract.remote_input_schema_digest != operation.params.schema_digest
                    || tool_contract.protocol_profile != mcp_contract.server.protocol_policy
                    || tool_contract.discovery_semantic_evidence_digest
                        != mcp_contract.discovery.objects_digest
                    || (operation.task_requested
                        && (!tool_contract.supports_task
                            || !capability_implementation.features.deferred
                            || !capability_implementation.features.poll))
                    || (operation.continuation.is_some()
                        && (!tool_contract.supports_task
                            || !capability_implementation.features.deferred
                            || !capability_implementation.features.poll))
                    || *isolation != SandboxIsolationClass::MicroVm
                    || self.runtime.runtime_family != SandboxRuntimeFamily::ManagedMcpServer
                    || self.package.entrypoint_kind
                        != insight_platform_contracts::SandboxEntrypointKind::ManagedMcpServer
                    || isolation_policy == resource_policy
                    || isolation_policy == artifact_io_policy
                    || resource_policy == artifact_io_policy
                {
                    return Err(SandboxContractError::InvalidExactBinding);
                }
                (
                    runtime,
                    package,
                    profile,
                    isolation,
                    &self.profile.network_policy,
                    resource_policy,
                    artifact_io_policy,
                    &self.profile.secret_policy,
                )
            }
        };
        if self.schema_version != 1
            || self.protocol_version != SANDBOX_PROTOCOL_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.sandbox_job_id.kind() != ResourceKind::SandboxJob
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_invocation_version == 0
            || self.input_value_id.kind() != ResourceKind::RunValue
            || self.output_value_id.kind() != ResourceKind::RunValue
            || self.input_value_id == self.output_value_id
            || self.sandbox_job_id.uuid() != self.job_id.uuid()
            || self.output_value_id.uuid() != self.job_id.uuid()
            || self.attempt_no == 0
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > 60_000
            || (leased && self.lease_generation == 0)
            || (!leased && self.lease_generation != 0)
            || self.capability_deployment.resource_kind != ResourceKind::CapabilityDeployment
            || self.runtime_revision.resource_kind != ResourceKind::SandboxRuntimeRevision
            || self.package_revision.resource_kind != ResourceKind::SandboxPackageRevision
            || self.profile_revision.resource_kind != ResourceKind::SandboxProfileRevision
            || runtime != &self.runtime_revision
            || package != &self.package_revision
            || profile != &self.profile_revision
            || *isolation != self.isolation_class
            || network_policy != &self.profile.network_policy
            || resource_policy != &self.profile.resource_policy
            || artifact_io_policy != &self.profile.artifact_io_policy
            || secret_policy != &self.profile.secret_policy
            || self.package.runtime_revision != self.runtime_revision
            || self.runtime.abi != self.protocol_version
            || !self
                .runtime
                .supported_isolation
                .contains(&self.isolation_class)
            || !self
                .profile
                .allowed_runtime_families
                .contains(&self.runtime.runtime_family)
            || !self
                .profile
                .allowed_trust_classes
                .contains(&self.package.trust_class)
            || self.isolation_class.security_rank() < self.profile.minimum_isolation.security_rank()
            || self.isolation_class.security_rank()
                < required_isolation(
                    self.runtime.runtime_family,
                    self.package.trust_class,
                    self.effect,
                    self.classification,
                    !self.secret_grants.is_empty(),
                    self.network_mode,
                )
                .security_rank()
            || !entrypoint_matches_runtime(
                self.runtime.runtime_family,
                self.package.entrypoint_kind,
            )
            || (self.secret_grants.is_empty() != self.profile.secret_policy.is_none())
            || self.deadline <= now
            || self.artifact_grants.len() > MAX_SANDBOX_ARTIFACT_GRANTS
            || self.secret_grants.len() > MAX_SANDBOX_SECRET_GRANTS
            || self.artifact_grants.iter().any(|grant| {
                grant.expires_at <= now
                    || grant.artifact.as_ref().is_some_and(|artifact| {
                        artifact.classification().rank() > self.classification.rank()
                    })
            })
            || self
                .secret_grants
                .iter()
                .any(|grant| grant.expires_at <= now)
            || self.callback.expires_at <= now
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
        let mut required_secret_bindings = self
            .execution_source
            .capability_deployment_closure()
            .secret_bindings
            .iter()
            .collect::<Vec<_>>();
        if let SandboxExecutionSource::ManagedMcp { mcp_contract, .. } = &self.execution_source {
            required_secret_bindings.extend(mcp_contract.deployment_closure.secret_bindings.iter());
            required_secret_bindings.push(&mcp_contract.authorization.token_secret_binding);
        }
        if required_secret_bindings
            .iter()
            .map(|binding| &binding.secret_binding_id)
            .collect::<BTreeSet<_>>()
            .len()
            != required_secret_bindings.len()
            || self.secret_grants.len() != required_secret_bindings.len()
            || required_secret_bindings.iter().any(|binding| {
                !self.secret_grants.iter().any(|grant| {
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
        self.input_ref
            .validate(limits.inline_json_limits())
            .map_err(|_| SandboxContractError::InvalidInput)?;
        if let ValueRef::Artifact { artifact } = &self.input_ref {
            let covered = self.artifact_grants.iter().any(|grant| {
                grant.operation == ArtifactGrantOperation::ReadWhole
                    && grant.artifact.as_ref() == Some(artifact)
            });
            if !covered {
                return Err(SandboxContractError::InvalidInput);
            }
        }
        for grant in &self.artifact_grants {
            grant.validate_for(&self.tenant_id, &self.sandbox_job_id, self.deadline, limits)?;
        }
        for grant in &self.secret_grants {
            grant.validate_for(
                &self.tenant_id,
                &self.sandbox_job_id,
                &self.runtime.image_or_module_digest,
                self.deadline,
            )?;
        }
        self.callback.validate_for(
            &self.tenant_id,
            &self.sandbox_job_id,
            &self.invocation_id,
            self.attempt_no,
            self.deadline,
        )?;
        self.resources.validate(
            self.isolation_class,
            self.network_mode,
            self.profile.max_job_duration_milliseconds,
            limits,
        )
    }
}

pub fn required_isolation(
    runtime_family: SandboxRuntimeFamily,
    trust_class: CodeTrustClass,
    effect: Effect,
    classification: DataClassification,
    has_secret: bool,
    network_mode: SandboxNetworkMode,
) -> SandboxIsolationClass {
    if matches!(
        trust_class,
        CodeTrustClass::TenantPublished | CodeTrustClass::ModelGenerated
    ) || matches!(
        runtime_family,
        SandboxRuntimeFamily::ReviewedShell | SandboxRuntimeFamily::ManagedMcpServer
    ) || has_secret
        || network_mode == SandboxNetworkMode::BrokeredEgress
        || effect.risk_rank() >= Effect::IdempotentWrite.risk_rank()
        || classification == DataClassification::Restricted
    {
        SandboxIsolationClass::MicroVm
    } else if runtime_family == SandboxRuntimeFamily::WasmWasi {
        SandboxIsolationClass::Wasm
    } else {
        SandboxIsolationClass::SandboxedContainer
    }
}

fn entrypoint_matches_runtime(
    runtime: SandboxRuntimeFamily,
    entrypoint: insight_platform_contracts::SandboxEntrypointKind,
) -> bool {
    matches!(
        (runtime, entrypoint),
        (
            SandboxRuntimeFamily::Python,
            insight_platform_contracts::SandboxEntrypointKind::PythonModule
        ) | (
            SandboxRuntimeFamily::NodeJs,
            insight_platform_contracts::SandboxEntrypointKind::NodeModule
        ) | (
            SandboxRuntimeFamily::WasmWasi,
            insight_platform_contracts::SandboxEntrypointKind::WasmExport
        ) | (
            SandboxRuntimeFamily::ReviewedShell,
            insight_platform_contracts::SandboxEntrypointKind::ReviewedExecutable
        ) | (
            SandboxRuntimeFamily::ManagedMcpServer,
            insight_platform_contracts::SandboxEntrypointKind::ManagedMcpServer
        )
    )
}

fn sorted_unique_ids<'a>(values: impl Iterator<Item = &'a ResourceId>) -> bool {
    let values = values.collect::<Vec<_>>();
    values.windows(2).all(|pair| pair[0] < pair[1])
        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
}

pub(crate) fn stable_code(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        })
}

pub(crate) fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, SandboxContractError> {
    let mut value =
        serde_json::to_value(value).map_err(|_| SandboxContractError::Canonicalization)?;
    let Value::Object(object) = &mut value else {
        return Err(SandboxContractError::Canonicalization);
    };
    object.remove(field);
    canonical_digest(&value)
        .map_err(|_| SandboxContractError::Canonicalization)?
        .parse()
        .map_err(|_| SandboxContractError::Canonicalization)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxContractError {
    InvalidLimits,
    InvalidResourceEnvelope,
    InvalidArtifactGrant,
    InvalidSecretGrant,
    InvalidPolicyClosure,
    InvalidCallback,
    InvalidExactBinding,
    InvalidResourceContract,
    InvalidExecutionRequest,
    InvalidInput,
    InvalidJob,
    InvalidTransition,
    InvalidOutcome,
    InvalidWorkerCommand,
    StaleFence,
    Canonicalization,
}

impl fmt::Display for SandboxContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimits => "Sandbox hard limits are invalid",
            Self::InvalidResourceEnvelope => "Sandbox resource envelope is invalid",
            Self::InvalidArtifactGrant => "Sandbox Artifact grant is invalid",
            Self::InvalidSecretGrant => "Sandbox Secret grant is invalid",
            Self::InvalidPolicyClosure => "Sandbox Policy closure is invalid",
            Self::InvalidCallback => "Sandbox callback binding is invalid",
            Self::InvalidExactBinding => "Sandbox exact binding is invalid",
            Self::InvalidResourceContract => "Sandbox Resource contract is invalid",
            Self::InvalidExecutionRequest => "Sandbox execution request is invalid",
            Self::InvalidInput => "Sandbox input is invalid or lacks an exact grant",
            Self::InvalidJob => "Sandbox Job projection is invalid",
            Self::InvalidTransition => "Sandbox physical state transition is invalid",
            Self::InvalidOutcome => "Sandbox outcome evidence is invalid",
            Self::InvalidWorkerCommand => "Sandbox Worker command is invalid",
            Self::StaleFence => "Sandbox Job fence is stale",
            Self::Canonicalization => "Sandbox canonicalization failed",
        })
    }
}

impl Error for SandboxContractError {}
