//! CR-216 OpenSandbox Kubernetes contracts.
//!
//! OpenSandbox owns only physical lifecycle.  These types deliberately keep provider observations
//! separate from the shared Job authority and make Package activation a one-way, replay-safe
//! boundary after PostgreSQL has selected one inert candidate.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, JsonLimits, ResourceId, ResourceKind,
    Sha256Digest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{error::Error, fmt};

pub const SANDBOX_CONTRACT_SCHEMA_VERSION: u16 = 1;
pub const MAX_SANDBOX_CANDIDATES: u8 = 4;
pub const MAX_SANDBOX_CANDIDATE_PAGE_ITEMS: u8 = 16;
pub const MAX_SANDBOX_ORPHAN_PAGE_ITEMS: u16 = 100;
pub const MAX_SANDBOX_RUNNER_HEADER_BYTES: u32 = 65_536;
pub const MAX_SANDBOX_DIAGNOSTIC_BYTES: u32 = 65_536;
pub const MAX_SANDBOX_INPUT_BYTES: u64 = 67_108_864;
pub const MAX_SANDBOX_OUTPUT_BYTES: u64 = 268_435_456;
pub const MAX_SANDBOX_ARGV_ITEMS: usize = 64;
pub const MAX_SANDBOX_ARG_BYTES: usize = 4_096;
pub const MAX_SANDBOX_ARGV_BYTES: usize = 16_384;
pub const MAX_OPENSANDBOX_ID_BYTES: usize = 128;
pub const MAX_RUNNER_BOOT_ID_BYTES: usize = 128;
pub const SANDBOX_RUNNER_PORT: u16 = 18_080;
pub const SANDBOX_RUNNER_CONFIG_ENV: &str = "INSIGHT_SANDBOX_RUNNER_CONFIG";
pub const SANDBOX_RUNNER_CONFIG_DIGEST_ENV: &str = "INSIGHT_SANDBOX_RUNNER_CONFIG_DIGEST";
pub const OPENSANDBOX_ID_ENV: &str = "OPENSANDBOX_ID";
pub const MAX_SANDBOX_RUNNER_CONFIG_BYTES: usize = 65_536;
pub const SANDBOX_RUNNER_FRAME_MAGIC: &str = "insight.sandbox.runner/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxProviderKind {
    OpenSandboxKubernetes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxNetworkMode {
    Disabled,
    Direct,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxProvisioningLimitsV1 {
    pub maximum_candidates: u8,
    pub candidate_page_items: u8,
    pub candidate_quiescence_milliseconds: u32,
    pub provisioning_timeout_milliseconds: u32,
    pub orphan_page_items: u16,
    pub runner_header_bytes: u32,
    pub diagnostic_bytes: u32,
}

impl SandboxProvisioningLimitsV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        if !(1..=MAX_SANDBOX_CANDIDATES).contains(&self.maximum_candidates)
            || !(1..=MAX_SANDBOX_CANDIDATE_PAGE_ITEMS).contains(&self.candidate_page_items)
            || !(100..=5_000).contains(&self.candidate_quiescence_milliseconds)
            || !(1_000..=120_000).contains(&self.provisioning_timeout_milliseconds)
            || self.candidate_quiescence_milliseconds >= self.provisioning_timeout_milliseconds
            || !(1..=MAX_SANDBOX_ORPHAN_PAGE_ITEMS).contains(&self.orphan_page_items)
            || !(1_024..=MAX_SANDBOX_RUNNER_HEADER_BYTES).contains(&self.runner_header_bytes)
            || !(1_024..=MAX_SANDBOX_DIAGNOSTIC_BYTES).contains(&self.diagnostic_bytes)
        {
            return Err(SandboxContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxResourceLimitsV1 {
    pub maximum_input_bytes: u64,
    pub maximum_output_bytes: u64,
    pub cpu_millicores: u32,
    pub memory_mebibytes: u32,
    pub pids: u32,
    pub ephemeral_storage_bytes: u64,
    pub wall_milliseconds: u64,
    pub cleanup_milliseconds: u64,
}

impl SandboxResourceLimitsV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        if self.maximum_input_bytes == 0
            || self.maximum_input_bytes > MAX_SANDBOX_INPUT_BYTES
            || self.maximum_output_bytes == 0
            || self.maximum_output_bytes > MAX_SANDBOX_OUTPUT_BYTES
            || self.cpu_millicores == 0
            || self.cpu_millicores > 8_000
            || self.memory_mebibytes == 0
            || self.memory_mebibytes > 16_384
            || self.pids == 0
            || self.pids > 4_096
            || self.ephemeral_storage_bytes == 0
            || self.ephemeral_storage_bytes > 4_294_967_296
            || self.wall_milliseconds == 0
            || self.wall_milliseconds > 3_600_000
            || self.cleanup_milliseconds == 0
            || self.cleanup_milliseconds > 300_000
        {
            return Err(SandboxContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxRuntimeContractV1 {
    pub schema_version: u16,
    pub provider: SandboxProviderKind,
    pub opensandbox_server_release_digest: Sha256Digest,
    pub lifecycle_schema_digest: Sha256Digest,
    pub batchsandbox_crd_digest: Sha256Digest,
    pub batchsandbox_controller_digest: Sha256Digest,
    pub kubernetes_provider_template_digest: Sha256Digest,
    pub runner_protocol_digest: Sha256Digest,
    pub container_runtime_digest: Sha256Digest,
    pub network_policy_digest: Sha256Digest,
}

impl SandboxRuntimeContractV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.provider != SandboxProviderKind::OpenSandboxKubernetes
        {
            return Err(SandboxContractError::InvalidRuntime);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        self.validate()?;
        digest_value(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxProfileDeploymentClosureV1 {
    pub schema_version: u16,
    pub runtime_contract_digest: Sha256Digest,
    pub provider_binding_digest: Sha256Digest,
    pub network_mode: SandboxNetworkMode,
    pub limits: SandboxResourceLimitsV1,
    pub provisioning_limits: SandboxProvisioningLimitsV1,
    pub secret_injection_disabled: bool,
}

impl SandboxProfileDeploymentClosureV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION || !self.secret_injection_disabled
        {
            return Err(SandboxContractError::InvalidProfile);
        }
        self.limits.validate()?;
        self.provisioning_limits.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxExecutionRequestV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub physical_attempt: u32,
    pub package_version_id: ResourceId,
    pub image_uri: String,
    pub runtime_version_id: ResourceId,
    pub sandbox_profile_deployment_id: ResourceId,
    pub runner_argv: Vec<String>,
    pub package_argv: Vec<String>,
    pub input: Value,
    pub input_schema_digest: Sha256Digest,
    pub input_digest: Sha256Digest,
    pub output_schema_digest: Sha256Digest,
    pub network_mode: SandboxNetworkMode,
    pub limits: SandboxResourceLimitsV1,
    pub deadline_at: DateTime<Utc>,
    pub request_digest: Sha256Digest,
}

impl SandboxExecutionRequestV1 {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.input_digest = canonical_value_digest(&self.input, self.limits.maximum_input_bytes)?;
        self.request_digest = self.semantic_digest()?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), SandboxContractError> {
        self.limits.validate()?;
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.physical_attempt == 0
            || self.package_version_id.kind() != ResourceKind::SandboxPackageRevision
            || self.runtime_version_id.kind() != ResourceKind::SandboxRuntimeRevision
            || self.sandbox_profile_deployment_id.kind() != ResourceKind::SandboxProfileDeployment
            || !valid_oci_digest_uri(&self.image_uri)
            || !valid_argv(&self.runner_argv)
            || !valid_argv(&self.package_argv)
            || self.input_digest
                != canonical_value_digest(&self.input, self.limits.maximum_input_bytes)?
            || self.request_digest != self.semantic_digest()?
        {
            return Err(SandboxContractError::InvalidRequest);
        }
        Ok(())
    }

    /// The semantic digest intentionally excludes the physical-attempt counter.
    pub fn semantic_digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        let mut value = serde_json::to_value(self).map_err(|_| SandboxContractError::Canonical)?;
        let object = value
            .as_object_mut()
            .ok_or(SandboxContractError::Canonical)?;
        object.remove("physical_attempt");
        object.remove("request_digest");
        digest_json(&Value::Object(object.clone()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxProvisioningTokenV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub physical_attempt: u32,
    pub execution_request_digest: Sha256Digest,
}

impl SandboxProvisioningTokenV1 {
    pub fn from_request(request: &SandboxExecutionRequestV1) -> Self {
        Self {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            tenant_id: request.tenant_id.clone(),
            job_id: request.job_id.clone(),
            physical_attempt: request.physical_attempt,
            execution_request_digest: request.request_digest.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), SandboxContractError> {
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.job_id.kind() != ResourceKind::Job
            || self.physical_attempt == 0
        {
            return Err(SandboxContractError::InvalidProvisioningToken);
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        self.validate()?;
        let value = serde_json::json!({
            "domain": "insight.sandbox.provision/v1",
            "token": self,
        });
        digest_json(&value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCandidateMetadataV1 {
    pub schema_version: u16,
    pub provisioning_token_digest: Sha256Digest,
    pub execution_request_digest: Sha256Digest,
    pub runtime_contract_digest: Sha256Digest,
    pub profile_deployment_digest: Sha256Digest,
    pub network_mode: SandboxNetworkMode,
}

impl SandboxCandidateMetadataV1 {
    pub fn validate_for(
        &self,
        request: &SandboxExecutionRequestV1,
        token: &SandboxProvisioningTokenV1,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.provisioning_token_digest != token.digest()?
            || self.execution_request_digest != request.request_digest
            || self.network_mode != request.network_mode
        {
            return Err(SandboxContractError::InvalidCandidate);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpenSandboxId(String);

impl OpenSandboxId {
    pub fn parse(value: impl Into<String>) -> Result<Self, SandboxContractError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_OPENSANDBOX_ID_BYTES
            || !value.is_ascii()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(SandboxContractError::InvalidCandidate);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunnerBootId(String);

impl RunnerBootId {
    pub fn parse(value: impl Into<String>) -> Result<Self, SandboxContractError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_RUNNER_BOOT_ID_BYTES
            || !value.is_ascii()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(SandboxContractError::InvalidRunnerFrame);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpaqueActivationToken(String);

impl fmt::Debug for OpaqueActivationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpaqueActivationToken([redacted])")
    }
}

impl OpaqueActivationToken {
    pub fn parse(value: impl Into<String>) -> Result<Self, SandboxContractError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(SandboxContractError::InvalidActivation);
        }
        Ok(Self(value))
    }

    pub fn expose_for_protocol(&self) -> &str {
        &self.0
    }

    pub fn digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        digest_json(&serde_json::json!({
            "domain": "insight.sandbox.activation-token/v1",
            "token": self.0,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxRunnerConfigV1 {
    pub schema_version: u16,
    pub execution_request_digest: Sha256Digest,
    pub input_schema_digest: Sha256Digest,
    pub input_digest: Sha256Digest,
    pub output_schema_digest: Sha256Digest,
    pub activation_token_digest: Sha256Digest,
    pub package_argv: Vec<String>,
    pub maximum_input_bytes: u64,
    pub maximum_output_bytes: u64,
    pub maximum_diagnostic_bytes: u32,
    pub maximum_processes: u32,
    pub wall_milliseconds: u64,
}

impl SandboxRunnerConfigV1 {
    pub fn from_request(
        request: &SandboxExecutionRequestV1,
        activation_token_digest: Sha256Digest,
        maximum_diagnostic_bytes: u32,
    ) -> Result<Self, SandboxContractError> {
        request.validate()?;
        let config = Self {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            execution_request_digest: request.request_digest.clone(),
            input_schema_digest: request.input_schema_digest.clone(),
            input_digest: request.input_digest.clone(),
            output_schema_digest: request.output_schema_digest.clone(),
            activation_token_digest,
            package_argv: request.package_argv.clone(),
            maximum_input_bytes: request.limits.maximum_input_bytes,
            maximum_output_bytes: request.limits.maximum_output_bytes,
            maximum_diagnostic_bytes,
            maximum_processes: request.limits.pids,
            wall_milliseconds: request.limits.wall_milliseconds,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), SandboxContractError> {
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || !valid_absolute_argv(&self.package_argv)
            || self.maximum_input_bytes == 0
            || self.maximum_input_bytes > MAX_SANDBOX_INPUT_BYTES
            || self.maximum_output_bytes == 0
            || self.maximum_output_bytes > MAX_SANDBOX_OUTPUT_BYTES
            || !(1_024..=MAX_SANDBOX_DIAGNOSTIC_BYTES).contains(&self.maximum_diagnostic_bytes)
            || self.maximum_processes == 0
            || self.maximum_processes > 4_096
            || self.wall_milliseconds == 0
            || self.wall_milliseconds > 3_600_000
        {
            return Err(SandboxContractError::InvalidRuntime);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        self.validate()?;
        digest_value(self)
    }

    pub fn canonical_json(&self) -> Result<String, SandboxContractError> {
        self.validate()?;
        let value = serde_json::to_value(self).map_err(|_| SandboxContractError::Canonical)?;
        let bytes = canonical_json(&value).map_err(|_| SandboxContractError::Canonical)?;
        if bytes.len() > MAX_SANDBOX_RUNNER_CONFIG_BYTES {
            return Err(SandboxContractError::InvalidRuntime);
        }
        String::from_utf8(bytes).map_err(|_| SandboxContractError::Canonical)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCandidateV1 {
    pub schema_version: u16,
    pub sandbox_id: OpenSandboxId,
    pub metadata: SandboxCandidateMetadataV1,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxPhysicalPhaseV1 {
    Provisioning,
    CandidateSelected,
    ActivationAuthorized,
    Started,
    Succeeded,
    Failed,
    UnknownOutcome,
    Cleaning,
    Absent,
}

impl SandboxPhysicalPhaseV1 {
    pub fn may_create_candidate(self) -> bool {
        self == Self::Provisioning
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxPhysicalEvidenceV1 {
    pub schema_version: u16,
    pub provisioning_token: SandboxProvisioningTokenV1,
    pub provisioning_token_digest: Sha256Digest,
    pub activation_token: OpaqueActivationToken,
    pub activation_token_digest: Sha256Digest,
    pub candidate_count: u8,
    pub selected_sandbox_id: Option<OpenSandboxId>,
    pub runner_boot_id: Option<RunnerBootId>,
    pub phase: SandboxPhysicalPhaseV1,
    pub result_digest: Option<Sha256Digest>,
    pub cleanup_required: bool,
    pub absence_evidence_digest: Option<Sha256Digest>,
}

impl SandboxPhysicalEvidenceV1 {
    pub fn begin(
        request: &SandboxExecutionRequestV1,
        activation_token: OpaqueActivationToken,
    ) -> Result<Self, SandboxContractError> {
        request.validate()?;
        let provisioning_token = SandboxProvisioningTokenV1::from_request(request);
        let provisioning_token_digest = provisioning_token.digest()?;
        let activation_token_digest = activation_token.digest()?;
        Ok(Self {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            provisioning_token,
            provisioning_token_digest,
            activation_token,
            activation_token_digest,
            candidate_count: 0,
            selected_sandbox_id: None,
            runner_boot_id: None,
            phase: SandboxPhysicalPhaseV1::Provisioning,
            result_digest: None,
            cleanup_required: false,
            absence_evidence_digest: None,
        })
    }

    pub fn observe_candidate(
        &self,
        request: &SandboxExecutionRequestV1,
        candidate: &SandboxCandidateV1,
        limits: &SandboxProvisioningLimitsV1,
    ) -> Result<Self, SandboxContractError> {
        limits.validate()?;
        if !self.phase.may_create_candidate()
            || candidate.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.candidate_count >= limits.maximum_candidates
        {
            return Err(SandboxContractError::CandidateLimitOrLateCandidate);
        }
        candidate
            .metadata
            .validate_for(request, &self.provisioning_token)?;
        let mut next = self.clone();
        next.candidate_count += 1;
        Ok(next)
    }

    pub fn select_candidate(
        &self,
        request: &SandboxExecutionRequestV1,
        candidate: &SandboxCandidateV1,
    ) -> Result<PhysicalDecision<Self>, SandboxContractError> {
        candidate
            .metadata
            .validate_for(request, &self.provisioning_token)?;
        match &self.selected_sandbox_id {
            Some(selected) if selected == &candidate.sandbox_id => {
                Ok(PhysicalDecision::Replayed(self.clone()))
            }
            Some(_) => Err(SandboxContractError::CandidateSelectionConflict),
            None if self.phase == SandboxPhysicalPhaseV1::Provisioning => {
                let mut next = self.clone();
                next.selected_sandbox_id = Some(candidate.sandbox_id.clone());
                next.phase = SandboxPhysicalPhaseV1::CandidateSelected;
                next.cleanup_required = true;
                Ok(PhysicalDecision::Applied(next))
            }
            None => Err(SandboxContractError::InvalidPhysicalTransition),
        }
    }

    pub fn authorize_activation(
        &self,
        sandbox_id: &OpenSandboxId,
        boot_id: RunnerBootId,
    ) -> Result<PhysicalDecision<Self>, SandboxContractError> {
        if self.selected_sandbox_id.as_ref() != Some(sandbox_id) {
            return Err(SandboxContractError::CandidateSelectionConflict);
        }
        match (&self.runner_boot_id, self.phase) {
            (Some(current), SandboxPhysicalPhaseV1::ActivationAuthorized)
                if current == &boot_id =>
            {
                Ok(PhysicalDecision::Replayed(self.clone()))
            }
            (Some(_), _) => Err(SandboxContractError::RunnerBootChanged),
            (None, SandboxPhysicalPhaseV1::CandidateSelected) => {
                let mut next = self.clone();
                next.runner_boot_id = Some(boot_id);
                // Persisted before external activation; this phase is PotentiallyStarted.
                next.phase = SandboxPhysicalPhaseV1::ActivationAuthorized;
                Ok(PhysicalDecision::Applied(next))
            }
            _ => Err(SandboxContractError::InvalidPhysicalTransition),
        }
    }

    pub fn record_runner_state(
        &self,
        frame: &SandboxRunnerStateFrameV1,
    ) -> Result<Self, SandboxContractError> {
        frame.validate()?;
        if self.selected_sandbox_id.as_ref() != Some(&frame.sandbox_id)
            || self.runner_boot_id.as_ref() != Some(&frame.boot_id)
            || self.provisioning_token.execution_request_digest != frame.execution_request_digest
        {
            return Err(SandboxContractError::InvalidRunnerFrame);
        }
        let phase = match frame.phase {
            SandboxRunnerPhaseV1::Armed => {
                if self.phase != SandboxPhysicalPhaseV1::CandidateSelected {
                    return Err(SandboxContractError::InvalidPhysicalTransition);
                }
                self.phase
            }
            SandboxRunnerPhaseV1::ActivationLatched | SandboxRunnerPhaseV1::Started => {
                if !matches!(
                    self.phase,
                    SandboxPhysicalPhaseV1::ActivationAuthorized | SandboxPhysicalPhaseV1::Started
                ) {
                    return Err(SandboxContractError::InvalidPhysicalTransition);
                }
                SandboxPhysicalPhaseV1::Started
            }
            SandboxRunnerPhaseV1::Succeeded => SandboxPhysicalPhaseV1::Succeeded,
            SandboxRunnerPhaseV1::Failed => SandboxPhysicalPhaseV1::Failed,
            SandboxRunnerPhaseV1::UnknownPriorActivation => SandboxPhysicalPhaseV1::UnknownOutcome,
        };
        let mut next = self.clone();
        next.phase = phase;
        Ok(next)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalDecision<T> {
    Applied(T),
    Replayed(T),
}

impl<T> PhysicalDecision<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::Applied(value) | Self::Replayed(value) => value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxRunnerPhaseV1 {
    Armed,
    ActivationLatched,
    Started,
    Succeeded,
    Failed,
    UnknownPriorActivation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxRunnerStateFrameV1 {
    pub magic: String,
    pub schema_version: u16,
    pub sandbox_id: OpenSandboxId,
    pub boot_id: RunnerBootId,
    pub execution_request_digest: Sha256Digest,
    pub phase: SandboxRunnerPhaseV1,
    pub frame_digest: Sha256Digest,
}

impl SandboxRunnerStateFrameV1 {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.magic = SANDBOX_RUNNER_FRAME_MAGIC.to_owned();
        self.frame_digest = digest_without_field(&self, "frame_digest")?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), SandboxContractError> {
        if self.magic != SANDBOX_RUNNER_FRAME_MAGIC
            || self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.frame_digest != digest_without_field(self, "frame_digest")?
        {
            return Err(SandboxContractError::InvalidRunnerFrame);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxActivationFrameV1 {
    pub magic: String,
    pub schema_version: u16,
    pub activation_token: OpaqueActivationToken,
    pub boot_id: RunnerBootId,
    pub execution_request_digest: Sha256Digest,
    pub input_schema_digest: Sha256Digest,
    pub input_digest: Sha256Digest,
    pub declared_input_bytes: u64,
    pub input: Value,
    pub frame_digest: Sha256Digest,
}

impl SandboxActivationFrameV1 {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.magic = SANDBOX_RUNNER_FRAME_MAGIC.to_owned();
        self.declared_input_bytes = u64::try_from(
            canonical_json(&self.input)
                .map_err(|_| SandboxContractError::InvalidActivation)?
                .len(),
        )
        .map_err(|_| SandboxContractError::InvalidActivation)?;
        self.frame_digest = digest_without_field(&self, "frame_digest")?;
        self.validate_wire(MAX_SANDBOX_INPUT_BYTES)?;
        Ok(self)
    }

    pub fn validate_wire(&self, maximum_input_bytes: u64) -> Result<(), SandboxContractError> {
        let actual_input_bytes = u64::try_from(
            canonical_json(&self.input)
                .map_err(|_| SandboxContractError::InvalidActivation)?
                .len(),
        )
        .map_err(|_| SandboxContractError::InvalidActivation)?;
        if self.magic != SANDBOX_RUNNER_FRAME_MAGIC
            || self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.declared_input_bytes != actual_input_bytes
            || actual_input_bytes > maximum_input_bytes
            || self.frame_digest != digest_without_field(self, "frame_digest")?
        {
            return Err(SandboxContractError::InvalidActivation);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        request: &SandboxExecutionRequestV1,
        evidence: &SandboxPhysicalEvidenceV1,
    ) -> Result<(), SandboxContractError> {
        self.validate_wire(request.limits.maximum_input_bytes)?;
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || evidence.phase != SandboxPhysicalPhaseV1::ActivationAuthorized
            || evidence.runner_boot_id.as_ref() != Some(&self.boot_id)
            || evidence.activation_token != self.activation_token
            || evidence.activation_token_digest != self.activation_token.digest()?
            || self.execution_request_digest != request.request_digest
            || self.input_schema_digest != request.input_schema_digest
            || self.input_digest != request.input_digest
            || self.input != request.input
            || canonical_value_digest(&self.input, request.limits.maximum_input_bytes)?
                != self.input_digest
        {
            return Err(SandboxContractError::InvalidActivation);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum SandboxRunnerOutcomeV1 {
    Succeeded {
        output: Value,
        output_schema_digest: Sha256Digest,
        output_digest: Sha256Digest,
        declared_output_bytes: u64,
    },
    Failed {
        failure_class: SandboxFailureClassV1,
        diagnostic_digest: Sha256Digest,
        diagnostic_bytes: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxFailureClassV1 {
    PackageFailed,
    PackageTimedOut,
    OutputInvalid,
    OutputTooLarge,
    RunnerFailure,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxRunnerResultFrameV1 {
    pub magic: String,
    pub schema_version: u16,
    pub execution_request_digest: Sha256Digest,
    pub boot_id: RunnerBootId,
    pub result: SandboxRunnerOutcomeV1,
    pub frame_digest: Sha256Digest,
}

impl SandboxRunnerResultFrameV1 {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.magic = SANDBOX_RUNNER_FRAME_MAGIC.to_owned();
        if let SandboxRunnerOutcomeV1::Succeeded {
            output,
            output_digest,
            declared_output_bytes,
            ..
        } = &mut self.result
        {
            *output_digest = digest_json(output)?;
            *declared_output_bytes = u64::try_from(
                canonical_json(output)
                    .map_err(|_| SandboxContractError::InvalidResult)?
                    .len(),
            )
            .map_err(|_| SandboxContractError::InvalidResult)?;
        }
        self.frame_digest = digest_without_field(&self, "frame_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &SandboxExecutionRequestV1,
        boot_id: &RunnerBootId,
        actual_bytes: usize,
    ) -> Result<(), SandboxContractError> {
        if self.magic != SANDBOX_RUNNER_FRAME_MAGIC
            || self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.execution_request_digest != request.request_digest
            || &self.boot_id != boot_id
            || self.frame_digest != digest_without_field(self, "frame_digest")?
            || u64::try_from(actual_bytes).map_or(true, |bytes| {
                bytes
                    > request
                        .limits
                        .maximum_output_bytes
                        .saturating_add(u64::from(MAX_SANDBOX_RUNNER_HEADER_BYTES))
            })
        {
            return Err(SandboxContractError::InvalidResult);
        }
        match &self.result {
            SandboxRunnerOutcomeV1::Succeeded {
                output,
                output_schema_digest,
                output_digest,
                declared_output_bytes,
            } => {
                let actual_output_bytes = u64::try_from(
                    canonical_json(output)
                        .map_err(|_| SandboxContractError::InvalidResult)?
                        .len(),
                )
                .map_err(|_| SandboxContractError::InvalidResult)?;
                if output_schema_digest != &request.output_schema_digest
                    || output_digest
                        != &canonical_value_digest(output, request.limits.maximum_output_bytes)?
                    || *declared_output_bytes != actual_output_bytes
                    || actual_output_bytes > request.limits.maximum_output_bytes
                {
                    return Err(SandboxContractError::InvalidResult);
                }
            }
            SandboxRunnerOutcomeV1::Failed {
                diagnostic_bytes, ..
            } if *diagnostic_bytes > MAX_SANDBOX_DIAGNOSTIC_BYTES => {
                return Err(SandboxContractError::InvalidResult);
            }
            SandboxRunnerOutcomeV1::Failed { .. } => {}
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, SandboxContractError> {
        canonical_json(&serde_json::to_value(self).map_err(|_| SandboxContractError::Canonical)?)
            .map_err(|_| SandboxContractError::Canonical)
    }
}

pub fn parse_result_frame(
    bytes: &[u8],
    request: &SandboxExecutionRequestV1,
    boot_id: &RunnerBootId,
) -> Result<SandboxRunnerResultFrameV1, SandboxContractError> {
    let maximum = request
        .limits
        .maximum_output_bytes
        .saturating_add(u64::from(MAX_SANDBOX_RUNNER_HEADER_BYTES));
    let maximum = usize::try_from(maximum).map_err(|_| SandboxContractError::InvalidResult)?;
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: maximum,
            max_depth: 32,
            max_items_per_array: 100_000,
            max_properties_per_object: 1_024,
            max_string_bytes: maximum,
        },
    )
    .map_err(|_| SandboxContractError::InvalidResult)?;
    if canonical_json(&value).map_err(|_| SandboxContractError::InvalidResult)? != bytes {
        return Err(SandboxContractError::InvalidResult);
    }
    let frame: SandboxRunnerResultFrameV1 =
        serde_json::from_value(value).map_err(|_| SandboxContractError::InvalidResult)?;
    frame.validate_for(request, boot_id, bytes.len())?;
    Ok(frame)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenSandboxCreateV1 {
    pub schema_version: u16,
    pub image_uri: String,
    pub entrypoint: Vec<String>,
    pub metadata: SandboxCandidateMetadataV1,
    pub runner_config: SandboxRunnerConfigV1,
    pub resource_limits: SandboxResourceLimitsV1,
    pub ttl_seconds: u32,
}

impl OpenSandboxCreateV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        self.runner_config.validate()?;
        self.resource_limits.validate()?;
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || !valid_oci_digest_uri(&self.image_uri)
            || !valid_absolute_argv(&self.entrypoint)
            || self.metadata.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.metadata.execution_request_digest != self.runner_config.execution_request_digest
            || self.resource_limits.maximum_input_bytes != self.runner_config.maximum_input_bytes
            || self.resource_limits.maximum_output_bytes != self.runner_config.maximum_output_bytes
            || self.resource_limits.pids != self.runner_config.maximum_processes
            || self.resource_limits.wall_milliseconds != self.runner_config.wall_milliseconds
            || !(60..=3_600).contains(&self.ttl_seconds)
        {
            return Err(SandboxContractError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateCursorV1 {
    pub schema_version: u16,
    pub opaque: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundedCandidatePageV1 {
    pub schema_version: u16,
    pub items: Vec<SandboxCandidateV1>,
    pub next: Option<CandidateCursorV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenSandboxObservationV1 {
    pub schema_version: u16,
    pub sandbox_id: OpenSandboxId,
    pub present: bool,
    pub observed_at: DateTime<Utc>,
    pub observation_digest: Sha256Digest,
}

impl OpenSandboxObservationV1 {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.observation_digest = digest_without_field(&self, "observation_digest")?;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), SandboxContractError> {
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.observation_digest != digest_without_field(self, "observation_digest")?
        {
            return Err(SandboxContractError::InvalidCandidate);
        }
        Ok(())
    }
}

#[async_trait]
pub trait OpenSandboxProvider: Send + Sync {
    async fn create_candidate(
        &self,
        request: OpenSandboxCreateV1,
    ) -> Result<SandboxCandidateV1, SandboxProviderError>;

    async fn list_candidates(
        &self,
        token_digest: Sha256Digest,
        cursor: CandidateCursorV1,
    ) -> Result<BoundedCandidatePageV1, SandboxProviderError>;

    async fn observe(
        &self,
        sandbox_id: &OpenSandboxId,
    ) -> Result<OpenSandboxObservationV1, SandboxProviderError>;

    async fn runner_state(
        &self,
        sandbox_id: &OpenSandboxId,
    ) -> Result<SandboxRunnerStateFrameV1, SandboxProviderError>;

    async fn activate(
        &self,
        sandbox_id: &OpenSandboxId,
        frame: SandboxActivationFrameV1,
    ) -> Result<SandboxRunnerStateFrameV1, SandboxProviderError>;

    async fn read_result(
        &self,
        sandbox_id: &OpenSandboxId,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, SandboxProviderError>;

    async fn terminate(
        &self,
        sandbox_id: &OpenSandboxId,
    ) -> Result<OpenSandboxObservationV1, SandboxProviderError>;

    async fn prove_absent(
        &self,
        sandbox_id: &OpenSandboxId,
    ) -> Result<OpenSandboxObservationV1, SandboxProviderError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxProviderError {
    InvalidResponse,
    NotReady,
    Unauthorized,
    Unavailable,
    Capacity,
    Conflict,
    Timeout,
}

impl fmt::Display for SandboxProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidResponse => "sandbox provider returned invalid evidence",
            Self::NotReady => "sandbox provider result is not ready",
            Self::Unauthorized => "sandbox provider rejected its caller",
            Self::Unavailable => "sandbox provider is unavailable",
            Self::Capacity => "sandbox provider capacity is exhausted",
            Self::Conflict => "sandbox provider operation conflicted",
            Self::Timeout => "sandbox provider operation timed out",
        })
    }
}

impl Error for SandboxProviderError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxContractError {
    Canonical,
    InvalidLimits,
    InvalidRuntime,
    InvalidProfile,
    InvalidRequest,
    InvalidProvisioningToken,
    InvalidCandidate,
    CandidateLimitOrLateCandidate,
    CandidateSelectionConflict,
    InvalidActivation,
    RunnerBootChanged,
    InvalidRunnerFrame,
    InvalidResult,
    InvalidPhysicalTransition,
}

impl fmt::Display for SandboxContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Canonical => "sandbox contract is not canonical",
            Self::InvalidLimits => "sandbox limits are invalid",
            Self::InvalidRuntime => "sandbox runtime contract is invalid",
            Self::InvalidProfile => "sandbox profile contract is invalid",
            Self::InvalidRequest => "sandbox execution request is invalid",
            Self::InvalidProvisioningToken => "sandbox provisioning token is invalid",
            Self::InvalidCandidate => "sandbox candidate evidence is invalid",
            Self::CandidateLimitOrLateCandidate => "sandbox candidate is late or over limit",
            Self::CandidateSelectionConflict => "another sandbox candidate already won",
            Self::InvalidActivation => "sandbox activation frame is invalid",
            Self::RunnerBootChanged => "sandbox runner boot identity changed",
            Self::InvalidRunnerFrame => "sandbox runner state frame is invalid",
            Self::InvalidResult => "sandbox runner result is invalid",
            Self::InvalidPhysicalTransition => "sandbox physical transition is invalid",
        })
    }
}

impl Error for SandboxContractError {}

fn valid_oci_digest_uri(value: &str) -> bool {
    let Some((repository, digest)) = value.rsplit_once("@sha256:") else {
        return false;
    };
    !repository.is_empty()
        && repository.len() <= 512
        && repository.is_ascii()
        && digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_argv(argv: &[String]) -> bool {
    !argv.is_empty()
        && argv.len() <= MAX_SANDBOX_ARGV_ITEMS
        && argv.iter().all(|item| {
            !item.is_empty() && item.len() <= MAX_SANDBOX_ARG_BYTES && !item.contains('\0')
        })
        && argv
            .iter()
            .map(String::len)
            .try_fold(0usize, usize::checked_add)
            .is_some_and(|bytes| bytes <= MAX_SANDBOX_ARGV_BYTES)
}

fn valid_absolute_argv(argv: &[String]) -> bool {
    valid_argv(argv) && argv.first().is_some_and(|program| program.starts_with('/'))
}

fn canonical_value_digest(
    value: &Value,
    maximum_bytes: u64,
) -> Result<Sha256Digest, SandboxContractError> {
    let bytes = canonical_json(value).map_err(|_| SandboxContractError::Canonical)?;
    if u64::try_from(bytes.len()).map_or(true, |length| length > maximum_bytes) {
        return Err(SandboxContractError::InvalidLimits);
    }
    let reparsed = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: bytes.len().max(1),
            max_depth: 32,
            max_items_per_array: 100_000,
            max_properties_per_object: 1_024,
            max_string_bytes: bytes.len().max(1),
        },
    )
    .map_err(|_| SandboxContractError::Canonical)?;
    if &reparsed != value {
        return Err(SandboxContractError::Canonical);
    }
    digest_json(value)
}

fn digest_value<T: Serialize>(value: &T) -> Result<Sha256Digest, SandboxContractError> {
    let value = serde_json::to_value(value).map_err(|_| SandboxContractError::Canonical)?;
    digest_json(&value)
}

fn digest_json(value: &Value) -> Result<Sha256Digest, SandboxContractError> {
    canonical_digest(value)
        .map_err(|_| SandboxContractError::Canonical)?
        .parse()
        .map_err(|_| SandboxContractError::Canonical)
}

fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, SandboxContractError> {
    let mut value = serde_json::to_value(value).map_err(|_| SandboxContractError::Canonical)?;
    value
        .as_object_mut()
        .ok_or(SandboxContractError::Canonical)?
        .remove(field)
        .ok_or(SandboxContractError::Canonical)?;
    digest_json(&value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use uuid::Uuid;

    fn id(kind: ResourceKind, sequence: u128) -> ResourceId {
        let raw = (sequence & ((1_u128 << 74) - 1)) | (7_u128 << 76) | (2_u128 << 62);
        ResourceId::from_uuid_v7(kind, Uuid::from_u128(raw)).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn limits() -> SandboxResourceLimitsV1 {
        SandboxResourceLimitsV1 {
            maximum_input_bytes: 1_024 * 1_024,
            maximum_output_bytes: 1_024 * 1_024,
            cpu_millicores: 1_000,
            memory_mebibytes: 1_024,
            pids: 128,
            ephemeral_storage_bytes: 64 * 1_024 * 1_024,
            wall_milliseconds: 30_000,
            cleanup_milliseconds: 10_000,
        }
    }

    fn provisioning_limits() -> SandboxProvisioningLimitsV1 {
        SandboxProvisioningLimitsV1 {
            maximum_candidates: 2,
            candidate_page_items: 4,
            candidate_quiescence_milliseconds: 500,
            provisioning_timeout_milliseconds: 10_000,
            orphan_page_items: 20,
            runner_header_bytes: 8_192,
            diagnostic_bytes: 8_192,
        }
    }

    fn request(physical_attempt: u32) -> SandboxExecutionRequestV1 {
        SandboxExecutionRequestV1 {
            schema_version: 1,
            tenant_id: id(ResourceKind::Tenant, 1),
            invocation_id: id(ResourceKind::CapabilityInvocation, 2),
            job_id: id(ResourceKind::Job, 3),
            physical_attempt,
            package_version_id: id(ResourceKind::SandboxPackageRevision, 4),
            image_uri: format!("registry.invalid/package@sha256:{}", "a".repeat(64)),
            runtime_version_id: id(ResourceKind::SandboxRuntimeRevision, 5),
            sandbox_profile_deployment_id: id(ResourceKind::SandboxProfileDeployment, 6),
            runner_argv: vec!["/usr/local/bin/platform-sandbox-runner".to_owned()],
            package_argv: vec!["/opt/insight/package".to_owned()],
            input: serde_json::json!({"question":"answer"}),
            input_schema_digest: digest('b'),
            input_digest: digest('0'),
            output_schema_digest: digest('c'),
            network_mode: SandboxNetworkMode::Direct,
            limits: limits(),
            deadline_at: Utc.timestamp_opt(2_000_000_000, 0).unwrap(),
            request_digest: digest('0'),
        }
        .seal()
        .unwrap()
    }

    fn candidate(request: &SandboxExecutionRequestV1, sandbox_id: &str) -> SandboxCandidateV1 {
        let token = SandboxProvisioningTokenV1::from_request(request);
        SandboxCandidateV1 {
            schema_version: 1,
            sandbox_id: OpenSandboxId::parse(sandbox_id).unwrap(),
            metadata: SandboxCandidateMetadataV1 {
                schema_version: 1,
                provisioning_token_digest: token.digest().unwrap(),
                execution_request_digest: request.request_digest.clone(),
                runtime_contract_digest: digest('d'),
                profile_deployment_digest: digest('e'),
                network_mode: request.network_mode,
            },
            observed_at: request.deadline_at - Duration::seconds(1),
        }
    }

    #[test]
    fn semantic_request_digest_excludes_only_physical_attempt() {
        let first = request(1);
        let second = request(2);
        assert_eq!(first.request_digest, second.request_digest);
        assert_ne!(
            SandboxProvisioningTokenV1::from_request(&first)
                .digest()
                .unwrap(),
            SandboxProvisioningTokenV1::from_request(&second)
                .digest()
                .unwrap()
        );
        let mut changed = first.clone();
        changed.network_mode = SandboxNetworkMode::Disabled;
        assert_ne!(first.request_digest, changed.semantic_digest().unwrap());
    }

    #[test]
    fn contracts_are_closed_and_illegal_config_fails() {
        let mut value = serde_json::to_value(provisioning_limits()).unwrap();
        value["backend"] = Value::String("wasi".to_owned());
        assert!(serde_json::from_value::<SandboxProvisioningLimitsV1>(value).is_err());
        let mut invalid = provisioning_limits();
        invalid.maximum_candidates = MAX_SANDBOX_CANDIDATES + 1;
        assert_eq!(invalid.validate(), Err(SandboxContractError::InvalidLimits));
        assert!(serde_json::from_str::<SandboxNetworkMode>("\"gvisor\"").is_err());

        let mut mutable_image = request(1);
        mutable_image.image_uri = "registry.invalid/package:latest".to_owned();
        assert_eq!(
            mutable_image.validate(),
            Err(SandboxContractError::InvalidRequest)
        );
    }

    #[test]
    fn candidate_selection_is_first_winner_and_activation_closes_create() {
        let request = request(1);
        let token = OpaqueActivationToken::parse("1".repeat(64)).unwrap();
        let evidence = SandboxPhysicalEvidenceV1::begin(&request, token).unwrap();
        let first = candidate(&request, "sandbox-one");
        let second = candidate(&request, "sandbox-two");
        let evidence = evidence
            .observe_candidate(&request, &first, &provisioning_limits())
            .unwrap()
            .observe_candidate(&request, &second, &provisioning_limits())
            .unwrap();
        let selected = evidence
            .select_candidate(&request, &first)
            .unwrap()
            .into_inner();
        assert!(matches!(
            selected.select_candidate(&request, &first).unwrap(),
            PhysicalDecision::Replayed(_)
        ));
        assert_eq!(
            selected.select_candidate(&request, &second),
            Err(SandboxContractError::CandidateSelectionConflict)
        );
        assert_eq!(
            selected.observe_candidate(&request, &second, &provisioning_limits()),
            Err(SandboxContractError::CandidateLimitOrLateCandidate)
        );
        let authorized = selected
            .authorize_activation(&first.sandbox_id, RunnerBootId::parse("boot-one").unwrap())
            .unwrap()
            .into_inner();
        assert!(!authorized.phase.may_create_candidate());
        assert_eq!(
            authorized
                .authorize_activation(&first.sandbox_id, RunnerBootId::parse("boot-two").unwrap(),),
            Err(SandboxContractError::RunnerBootChanged)
        );
    }

    #[test]
    fn activation_and_result_validate_full_identity_digest_and_size() {
        let request = request(1);
        let activation_token = OpaqueActivationToken::parse("2".repeat(64)).unwrap();
        let candidate = candidate(&request, "sandbox-one");
        let boot_id = RunnerBootId::parse("boot-one").unwrap();
        let evidence = SandboxPhysicalEvidenceV1::begin(&request, activation_token.clone())
            .unwrap()
            .select_candidate(&request, &candidate)
            .unwrap()
            .into_inner()
            .authorize_activation(&candidate.sandbox_id, boot_id.clone())
            .unwrap()
            .into_inner();
        let activation = SandboxActivationFrameV1 {
            magic: String::new(),
            schema_version: 1,
            activation_token,
            boot_id: boot_id.clone(),
            execution_request_digest: request.request_digest.clone(),
            input_schema_digest: request.input_schema_digest.clone(),
            input_digest: request.input_digest.clone(),
            declared_input_bytes: 0,
            input: request.input.clone(),
            frame_digest: digest('0'),
        }
        .seal()
        .unwrap();
        activation.validate_for(&request, &evidence).unwrap();

        let frame = SandboxRunnerResultFrameV1 {
            magic: String::new(),
            schema_version: 1,
            execution_request_digest: request.request_digest.clone(),
            boot_id: boot_id.clone(),
            result: SandboxRunnerOutcomeV1::Succeeded {
                output: serde_json::json!({"answer":42}),
                output_schema_digest: request.output_schema_digest.clone(),
                output_digest: digest('0'),
                declared_output_bytes: 0,
            },
            frame_digest: digest('0'),
        }
        .seal()
        .unwrap();
        let bytes = frame.canonical_bytes().unwrap();
        assert_eq!(
            parse_result_frame(&bytes, &request, &boot_id).unwrap(),
            frame
        );

        let mut wrong = frame.clone();
        let SandboxRunnerOutcomeV1::Succeeded { output, .. } = &mut wrong.result else {
            unreachable!()
        };
        *output = serde_json::json!({"answer":43});
        let wrong_bytes = serde_json::to_vec(&wrong).unwrap();
        assert_eq!(
            parse_result_frame(&wrong_bytes, &request, &boot_id),
            Err(SandboxContractError::InvalidResult)
        );
        let mut trailing = bytes.clone();
        trailing.push(b' ');
        assert_eq!(
            parse_result_frame(&trailing, &request, &boot_id),
            Err(SandboxContractError::InvalidResult)
        );
        let mut trailing = bytes;
        trailing.extend_from_slice(b" {}");
        assert_eq!(
            parse_result_frame(&trailing, &request, &boot_id),
            Err(SandboxContractError::InvalidResult)
        );
    }

    #[test]
    fn activation_tokens_are_redacted_and_strict() {
        let token = OpaqueActivationToken::parse("a".repeat(64)).unwrap();
        assert!(!format!("{token:?}").contains(&"a".repeat(64)));
        assert!(OpaqueActivationToken::parse("A".repeat(64)).is_err());
        assert!(OpaqueActivationToken::parse("a".repeat(63)).is_err());
    }
}
