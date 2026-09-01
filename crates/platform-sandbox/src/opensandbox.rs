//! CR-216 OpenSandbox Kubernetes contracts.
//!
//! OpenSandbox owns only physical lifecycle.  These types deliberately keep provider observations
//! separate from the shared Job authority and make Package activation a one-way, replay-safe
//! boundary after PostgreSQL has selected one inert candidate.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, DataClassification, JobState, JsonLimits,
    ResourceId, ResourceKind, Sha256Digest, TraceIdentityV1, WorkClass,
};
use insight_platform_jobs::{JobFence, JobProjection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{error::Error, fmt};

pub const SANDBOX_CONTRACT_SCHEMA_VERSION: u16 = 1;
pub const MAX_SANDBOX_CANDIDATES: u8 = 4;
pub const MAX_SANDBOX_CANDIDATE_PAGE_ITEMS: u8 = 16;
pub const MAX_SANDBOX_ORPHAN_PAGE_ITEMS: u16 = 100;
pub const MAX_SANDBOX_RUNNER_HEADER_BYTES: u32 = 65_536;
pub const MAX_SANDBOX_DIAGNOSTIC_BYTES: u32 = 65_536;
pub const MAX_SANDBOX_INPUT_BYTES: u64 = 1_048_576;
pub const MAX_SANDBOX_OUTPUT_BYTES: u64 = 1_048_576;
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
pub const MAX_SANDBOX_JOB_PAYLOAD_BYTES: usize = 1_048_576;

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
    pub lease_generation: u64,
    pub physical_attempt: u32,
    pub worker_process_generation_id: ResourceId,
    pub package_version_id: ResourceId,
    pub image_uri: String,
    pub runtime_version_id: ResourceId,
    pub runtime_contract_digest: Sha256Digest,
    pub sandbox_profile_deployment_id: ResourceId,
    pub profile_deployment_digest: Sha256Digest,
    pub runner_argv: Vec<String>,
    pub package_argv: Vec<String>,
    pub input_value_id: ResourceId,
    pub output_value_id: ResourceId,
    pub classification: DataClassification,
    pub input: Value,
    pub input_schema_digest: Sha256Digest,
    pub input_digest: Sha256Digest,
    pub output_schema_digest: Sha256Digest,
    pub network_mode: SandboxNetworkMode,
    pub limits: SandboxResourceLimitsV1,
    pub provisioning_limits: SandboxProvisioningLimitsV1,
    pub deadline_at: DateTime<Utc>,
    pub trace: TraceIdentityV1,
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
        self.provisioning_limits.validate()?;
        self.trace
            .validate()
            .map_err(|_| SandboxContractError::InvalidRequest)?;
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.lease_generation == 0
            || self.physical_attempt == 0
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.package_version_id.kind() != ResourceKind::SandboxPackageRevision
            || self.runtime_version_id.kind() != ResourceKind::SandboxRuntimeRevision
            || self.sandbox_profile_deployment_id.kind() != ResourceKind::SandboxProfileDeployment
            || !valid_oci_digest_uri(&self.image_uri)
            || !valid_absolute_argv(&self.runner_argv)
            || !valid_absolute_argv(&self.package_argv)
            || self.input_value_id.kind() != ResourceKind::RunValue
            || self.output_value_id.kind() != ResourceKind::RunValue
            || self.input_value_id == self.output_value_id
            || self.input_digest
                != canonical_value_digest(&self.input, self.limits.maximum_input_bytes)?
            || self.request_digest != self.semantic_digest()?
        {
            return Err(SandboxContractError::InvalidRequest);
        }
        Ok(())
    }

    /// The semantic digest excludes lease/recovery correlation and the body already represented
    /// by `input_digest`.
    pub fn semantic_digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        let mut value = serde_json::to_value(self).map_err(|_| SandboxContractError::Canonical)?;
        let object = value
            .as_object_mut()
            .ok_or(SandboxContractError::Canonical)?;
        object.remove("lease_generation");
        object.remove("physical_attempt");
        object.remove("worker_process_generation_id");
        object.remove("input");
        object.remove("trace");
        object.remove("request_digest");
        digest_json(&Value::Object(object.clone()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxExecutionPlanV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub package_version_id: ResourceId,
    pub image_uri: String,
    pub runtime_version_id: ResourceId,
    pub runtime_contract_digest: Sha256Digest,
    pub sandbox_profile_deployment_id: ResourceId,
    pub profile_deployment_digest: Sha256Digest,
    pub runner_argv: Vec<String>,
    pub package_argv: Vec<String>,
    pub input_value_id: ResourceId,
    pub output_value_id: ResourceId,
    pub classification: DataClassification,
    pub input_schema_digest: Sha256Digest,
    pub input_digest: Sha256Digest,
    pub output_schema_digest: Sha256Digest,
    pub network_mode: SandboxNetworkMode,
    pub limits: SandboxResourceLimitsV1,
    pub provisioning_limits: SandboxProvisioningLimitsV1,
    pub deadline_at: DateTime<Utc>,
    pub request_digest: Sha256Digest,
}

impl SandboxExecutionPlanV1 {
    pub fn from_request(request: &SandboxExecutionRequestV1) -> Result<Self, SandboxContractError> {
        request.validate()?;
        let plan = Self {
            schema_version: request.schema_version,
            tenant_id: request.tenant_id.clone(),
            invocation_id: request.invocation_id.clone(),
            job_id: request.job_id.clone(),
            package_version_id: request.package_version_id.clone(),
            image_uri: request.image_uri.clone(),
            runtime_version_id: request.runtime_version_id.clone(),
            runtime_contract_digest: request.runtime_contract_digest.clone(),
            sandbox_profile_deployment_id: request.sandbox_profile_deployment_id.clone(),
            profile_deployment_digest: request.profile_deployment_digest.clone(),
            runner_argv: request.runner_argv.clone(),
            package_argv: request.package_argv.clone(),
            input_value_id: request.input_value_id.clone(),
            output_value_id: request.output_value_id.clone(),
            classification: request.classification,
            input_schema_digest: request.input_schema_digest.clone(),
            input_digest: request.input_digest.clone(),
            output_schema_digest: request.output_schema_digest.clone(),
            network_mode: request.network_mode,
            limits: request.limits.clone(),
            provisioning_limits: request.provisioning_limits.clone(),
            deadline_at: request.deadline_at,
            request_digest: request.request_digest.clone(),
        };
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<(), SandboxContractError> {
        self.limits.validate()?;
        self.provisioning_limits.validate()?;
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.package_version_id.kind() != ResourceKind::SandboxPackageRevision
            || self.runtime_version_id.kind() != ResourceKind::SandboxRuntimeRevision
            || self.sandbox_profile_deployment_id.kind() != ResourceKind::SandboxProfileDeployment
            || !valid_oci_digest_uri(&self.image_uri)
            || !valid_absolute_argv(&self.runner_argv)
            || !valid_absolute_argv(&self.package_argv)
            || self.input_value_id.kind() != ResourceKind::RunValue
            || self.output_value_id.kind() != ResourceKind::RunValue
            || self.input_value_id == self.output_value_id
            || self.request_digest != self.semantic_digest()?
        {
            return Err(SandboxContractError::InvalidRequest);
        }
        Ok(())
    }

    pub fn semantic_digest(&self) -> Result<Sha256Digest, SandboxContractError> {
        digest_without_field(self, "request_digest")
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bind_request(
        &self,
        lease_generation: u64,
        physical_attempt: u32,
        worker_process_generation_id: ResourceId,
        trace: TraceIdentityV1,
        input: Value,
    ) -> Result<SandboxExecutionRequestV1, SandboxContractError> {
        self.validate()?;
        let request = SandboxExecutionRequestV1 {
            schema_version: self.schema_version,
            tenant_id: self.tenant_id.clone(),
            invocation_id: self.invocation_id.clone(),
            job_id: self.job_id.clone(),
            lease_generation,
            physical_attempt,
            worker_process_generation_id,
            package_version_id: self.package_version_id.clone(),
            image_uri: self.image_uri.clone(),
            runtime_version_id: self.runtime_version_id.clone(),
            runtime_contract_digest: self.runtime_contract_digest.clone(),
            sandbox_profile_deployment_id: self.sandbox_profile_deployment_id.clone(),
            profile_deployment_digest: self.profile_deployment_digest.clone(),
            runner_argv: self.runner_argv.clone(),
            package_argv: self.package_argv.clone(),
            input_value_id: self.input_value_id.clone(),
            output_value_id: self.output_value_id.clone(),
            classification: self.classification,
            input,
            input_schema_digest: self.input_schema_digest.clone(),
            input_digest: self.input_digest.clone(),
            output_schema_digest: self.output_schema_digest.clone(),
            network_mode: self.network_mode,
            limits: self.limits.clone(),
            provisioning_limits: self.provisioning_limits.clone(),
            deadline_at: self.deadline_at,
            trace,
            request_digest: self.request_digest.clone(),
        };
        request.validate()?;
        if request.request_digest != self.request_digest {
            return Err(SandboxContractError::InvalidRequest);
        }
        Ok(request)
    }

    pub fn validate_request(
        &self,
        request: &SandboxExecutionRequestV1,
    ) -> Result<(), SandboxContractError> {
        request.validate()?;
        if &Self::from_request(request)? != self {
            return Err(SandboxContractError::InvalidRequest);
        }
        Ok(())
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

    pub fn from_plan(plan: &SandboxExecutionPlanV1, physical_attempt: u32) -> Self {
        Self {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            tenant_id: plan.tenant_id.clone(),
            job_id: plan.job_id.clone(),
            physical_attempt,
            execution_request_digest: plan.request_digest.clone(),
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
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub physical_attempt: u32,
    pub create_ordinal: u8,
    pub provisioning_token_digest: Sha256Digest,
    pub execution_request_digest: Sha256Digest,
    pub runtime_contract_digest: Sha256Digest,
    pub profile_deployment_digest: Sha256Digest,
    pub network_mode: SandboxNetworkMode,
}

impl SandboxCandidateMetadataV1 {
    pub fn validate_shape(&self) -> Result<(), SandboxContractError> {
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.job_id.kind() != ResourceKind::Job
            || self.physical_attempt == 0
            || !(1..=MAX_SANDBOX_CANDIDATES).contains(&self.create_ordinal)
        {
            return Err(SandboxContractError::InvalidCandidate);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        request: &SandboxExecutionRequestV1,
        token: &SandboxProvisioningTokenV1,
    ) -> Result<(), SandboxContractError> {
        self.validate_shape()?;
        if self.tenant_id != request.tenant_id
            || self.job_id != request.job_id
            || self.physical_attempt != request.physical_attempt
            || self.provisioning_token_digest != token.digest()?
            || self.execution_request_digest != request.request_digest
            || self.runtime_contract_digest != request.runtime_contract_digest
            || self.profile_deployment_digest != request.profile_deployment_digest
            || self.network_mode != request.network_mode
        {
            return Err(SandboxContractError::InvalidCandidate);
        }
        Ok(())
    }

    pub fn validate_for_plan(
        &self,
        plan: &SandboxExecutionPlanV1,
        token: &SandboxProvisioningTokenV1,
    ) -> Result<(), SandboxContractError> {
        self.validate_shape()?;
        if self.tenant_id != plan.tenant_id
            || self.job_id != plan.job_id
            || self.physical_attempt != token.physical_attempt
            || self.provisioning_token_digest != token.digest()?
            || self.execution_request_digest != plan.request_digest
            || self.runtime_contract_digest != plan.runtime_contract_digest
            || self.profile_deployment_digest != plan.profile_deployment_digest
            || self.network_mode != plan.network_mode
        {
            return Err(SandboxContractError::InvalidCandidate);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    pub fn generate() -> Result<Self, SandboxContractError> {
        use ring::rand::{SecureRandom, SystemRandom};
        use std::fmt::Write as _;

        let mut bytes = [0_u8; 32];
        SystemRandom::new()
            .fill(&mut bytes)
            .map_err(|_| SandboxContractError::InvalidActivation)?;
        let mut encoded = String::with_capacity(64);
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}")
                .map_err(|_| SandboxContractError::InvalidActivation)?;
        }
        Self::parse(encoded)
    }

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

impl SandboxCandidateV1 {
    pub fn validate_shape(&self) -> Result<(), SandboxContractError> {
        self.metadata.validate_shape()?;
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION {
            return Err(SandboxContractError::InvalidCandidate);
        }
        Ok(())
    }
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
    pub provisioning_started_at: DateTime<Utc>,
    pub create_authorization_count: u8,
    pub last_create_authorized_at: Option<DateTime<Utc>>,
    pub candidate_count: u8,
    pub candidate_ids: Vec<OpenSandboxId>,
    pub selected_sandbox_id: Option<OpenSandboxId>,
    pub runner_boot_id: Option<RunnerBootId>,
    pub phase: SandboxPhysicalPhaseV1,
    pub result_evidence: Option<Box<SandboxResultEvidenceV1>>,
    pub cleanup_required: bool,
    pub absent_candidate_ids: Vec<OpenSandboxId>,
    pub absence_evidence_digest: Option<Sha256Digest>,
}

impl SandboxPhysicalEvidenceV1 {
    pub fn begin(
        request: &SandboxExecutionRequestV1,
        activation_token: OpaqueActivationToken,
        database_now: DateTime<Utc>,
    ) -> Result<Self, SandboxContractError> {
        request.validate()?;
        if database_now >= request.deadline_at {
            return Err(SandboxContractError::CandidateCreateConflict);
        }
        let provisioning_token = SandboxProvisioningTokenV1::from_request(request);
        let provisioning_token_digest = provisioning_token.digest()?;
        let activation_token_digest = activation_token.digest()?;
        Ok(Self {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            provisioning_token,
            provisioning_token_digest,
            activation_token,
            activation_token_digest,
            provisioning_started_at: database_now,
            create_authorization_count: 0,
            last_create_authorized_at: None,
            candidate_count: 0,
            candidate_ids: Vec::new(),
            selected_sandbox_id: None,
            runner_boot_id: None,
            phase: SandboxPhysicalPhaseV1::Provisioning,
            result_evidence: None,
            cleanup_required: false,
            absent_candidate_ids: Vec::new(),
            absence_evidence_digest: None,
        })
    }

    pub fn authorize_candidate_create(
        &self,
        request: &SandboxExecutionRequestV1,
        limits: &SandboxProvisioningLimitsV1,
        database_now: DateTime<Utc>,
        create_ordinal: u8,
    ) -> Result<PhysicalDecision<Self>, SandboxContractError> {
        request.validate()?;
        limits.validate()?;
        if limits != &request.provisioning_limits
            || self.phase != SandboxPhysicalPhaseV1::Provisioning
            || create_ordinal == 0
            || create_ordinal > limits.maximum_candidates
        {
            return Err(SandboxContractError::CandidateCreateConflict);
        }
        if create_ordinal == self.create_authorization_count {
            return Ok(PhysicalDecision::Replayed(self.clone()));
        }
        if create_ordinal != self.create_authorization_count.saturating_add(1)
            || self.create_authorization_count >= limits.maximum_candidates
        {
            return Err(SandboxContractError::CandidateCreateConflict);
        }
        let elapsed = database_now
            .signed_duration_since(self.provisioning_started_at)
            .num_milliseconds();
        if elapsed < 0 || elapsed > i64::from(limits.provisioning_timeout_milliseconds) {
            return Err(SandboxContractError::CandidateLimitOrLateCandidate);
        }
        if self.last_create_authorized_at.is_some_and(|last| {
            database_now.signed_duration_since(last).num_milliseconds()
                < i64::from(limits.candidate_quiescence_milliseconds)
        }) {
            return Err(SandboxContractError::CandidateLimitOrLateCandidate);
        }
        let mut next = self.clone();
        next.create_authorization_count = create_ordinal;
        next.last_create_authorized_at = Some(database_now);
        Ok(PhysicalDecision::Applied(next))
    }

    pub fn observe_candidate(
        &self,
        request: &SandboxExecutionRequestV1,
        candidate: &SandboxCandidateV1,
        limits: &SandboxProvisioningLimitsV1,
    ) -> Result<Self, SandboxContractError> {
        limits.validate()?;
        if limits != &request.provisioning_limits {
            return Err(SandboxContractError::InvalidLimits);
        }
        candidate
            .metadata
            .validate_for(request, &self.provisioning_token)?;
        if candidate.metadata.create_ordinal > self.create_authorization_count {
            return Err(SandboxContractError::InvalidCandidate);
        }
        if self.candidate_ids.contains(&candidate.sandbox_id) {
            return Ok(self.clone());
        }
        if !self.phase.may_create_candidate()
            || candidate.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.candidate_count >= limits.maximum_candidates
        {
            return Err(SandboxContractError::CandidateLimitOrLateCandidate);
        }
        let mut next = self.clone();
        next.candidate_ids.push(candidate.sandbox_id.clone());
        next.candidate_count += 1;
        next.cleanup_required = true;
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
        if candidate.metadata.create_ordinal > self.create_authorization_count
            || !self.candidate_ids.contains(&candidate.sandbox_id)
        {
            return Err(SandboxContractError::InvalidCandidate);
        }
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
            SandboxRunnerPhaseV1::Succeeded => {
                if !matches!(
                    self.phase,
                    SandboxPhysicalPhaseV1::ActivationAuthorized
                        | SandboxPhysicalPhaseV1::Started
                        | SandboxPhysicalPhaseV1::Succeeded
                ) {
                    return Err(SandboxContractError::InvalidPhysicalTransition);
                }
                SandboxPhysicalPhaseV1::Succeeded
            }
            SandboxRunnerPhaseV1::Failed => {
                if !matches!(
                    self.phase,
                    SandboxPhysicalPhaseV1::ActivationAuthorized
                        | SandboxPhysicalPhaseV1::Started
                        | SandboxPhysicalPhaseV1::Failed
                ) {
                    return Err(SandboxContractError::InvalidPhysicalTransition);
                }
                SandboxPhysicalPhaseV1::Failed
            }
            SandboxRunnerPhaseV1::UnknownPriorActivation => {
                if !matches!(
                    self.phase,
                    SandboxPhysicalPhaseV1::ActivationAuthorized
                        | SandboxPhysicalPhaseV1::Started
                        | SandboxPhysicalPhaseV1::UnknownOutcome
                ) {
                    return Err(SandboxContractError::InvalidPhysicalTransition);
                }
                SandboxPhysicalPhaseV1::UnknownOutcome
            }
        };
        let mut next = self.clone();
        next.phase = phase;
        Ok(next)
    }

    pub fn record_result(
        &self,
        request: &SandboxExecutionRequestV1,
        frame: &SandboxRunnerResultFrameV1,
    ) -> Result<Self, SandboxContractError> {
        let boot_id = self
            .runner_boot_id
            .as_ref()
            .ok_or(SandboxContractError::InvalidPhysicalTransition)?;
        let bytes = frame.canonical_bytes()?;
        frame.validate_for(request, boot_id, bytes.len())?;
        let evidence = SandboxResultEvidenceV1::from_frame(frame);
        if let Some(current) = self.result_evidence.as_deref() {
            return if current == &evidence {
                Ok(self.clone())
            } else {
                Err(SandboxContractError::TerminalConflict)
            };
        }
        let phase = match &frame.result {
            SandboxRunnerOutcomeV1::Succeeded { .. } => SandboxPhysicalPhaseV1::Succeeded,
            SandboxRunnerOutcomeV1::Failed { .. } => SandboxPhysicalPhaseV1::Failed,
        };
        if !matches!(
            self.phase,
            SandboxPhysicalPhaseV1::ActivationAuthorized
                | SandboxPhysicalPhaseV1::Started
                | SandboxPhysicalPhaseV1::Succeeded
                | SandboxPhysicalPhaseV1::Failed
        ) {
            return Err(SandboxContractError::InvalidPhysicalTransition);
        }
        let mut next = self.clone();
        next.phase = phase;
        next.result_evidence = Some(Box::new(evidence));
        next.cleanup_required = true;
        Ok(next)
    }

    pub fn mark_absent(
        &self,
        sandbox_id: &OpenSandboxId,
        evidence_digest: Sha256Digest,
    ) -> Result<Self, SandboxContractError> {
        if !self.candidate_ids.contains(sandbox_id) {
            return Err(SandboxContractError::InvalidCandidate);
        }
        let mut next = self.clone();
        if !next.absent_candidate_ids.contains(sandbox_id) {
            next.absent_candidate_ids.push(sandbox_id.clone());
        }
        if next.absent_candidate_ids.len() == next.candidate_ids.len() {
            next.phase = SandboxPhysicalPhaseV1::Absent;
            next.cleanup_required = false;
            next.absence_evidence_digest = Some(evidence_digest);
        }
        Ok(next)
    }

    pub fn validate_for(&self, plan: &SandboxExecutionPlanV1) -> Result<(), SandboxContractError> {
        let unique_candidates = self
            .candidate_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        let unique_absent = self
            .absent_candidate_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.provisioning_token
                != SandboxProvisioningTokenV1::from_plan(
                    plan,
                    self.provisioning_token.physical_attempt,
                )
            || self.provisioning_token_digest != self.provisioning_token.digest()?
            || self.activation_token_digest != self.activation_token.digest()?
            || self.provisioning_started_at >= plan.deadline_at
            || self.create_authorization_count > plan.provisioning_limits.maximum_candidates
            || (self.create_authorization_count == 0) != self.last_create_authorized_at.is_none()
            || self
                .last_create_authorized_at
                .is_some_and(|last| last < self.provisioning_started_at)
            || self.last_create_authorized_at.is_some_and(|last| {
                last.signed_duration_since(self.provisioning_started_at)
                    .num_milliseconds()
                    > i64::from(plan.provisioning_limits.provisioning_timeout_milliseconds)
            })
            || usize::from(self.candidate_count) != self.candidate_ids.len()
            || self.candidate_count > self.create_authorization_count
            || self.candidate_ids.len() > usize::from(MAX_SANDBOX_CANDIDATES)
            || unique_candidates.len() != self.candidate_ids.len()
            || unique_absent.len() != self.absent_candidate_ids.len()
            || self
                .absent_candidate_ids
                .iter()
                .any(|candidate| !self.candidate_ids.contains(candidate))
            || self
                .selected_sandbox_id
                .as_ref()
                .is_some_and(|selected| !self.candidate_ids.contains(selected))
            || self
                .result_evidence
                .as_deref()
                .is_some_and(|evidence| evidence.validate_for(plan).is_err())
            || (self.result_evidence.is_some()
                && !matches!(
                    self.phase,
                    SandboxPhysicalPhaseV1::Succeeded
                        | SandboxPhysicalPhaseV1::Failed
                        | SandboxPhysicalPhaseV1::Cleaning
                        | SandboxPhysicalPhaseV1::Absent
                ))
            || (self.runner_boot_id.is_some()
                && matches!(
                    self.phase,
                    SandboxPhysicalPhaseV1::Provisioning
                        | SandboxPhysicalPhaseV1::CandidateSelected
                ))
            || (self.runner_boot_id.is_none()
                && matches!(
                    self.phase,
                    SandboxPhysicalPhaseV1::ActivationAuthorized
                        | SandboxPhysicalPhaseV1::Started
                        | SandboxPhysicalPhaseV1::Succeeded
                        | SandboxPhysicalPhaseV1::Failed
                        | SandboxPhysicalPhaseV1::UnknownOutcome
                ))
            || (self.phase == SandboxPhysicalPhaseV1::Absent
                && (self.cleanup_required
                    || self.absence_evidence_digest.is_none()
                    || self.absent_candidate_ids.len() != self.candidate_ids.len()))
        {
            return Err(SandboxContractError::InvalidPhysicalTransition);
        }
        Ok(())
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum SandboxTerminalOutcomeV1 {
    Succeeded {
        result: Box<SandboxRunnerResultFrameV1>,
    },
    Failed {
        failure_class: SandboxFailureClassV1,
        result: Option<Box<SandboxRunnerResultFrameV1>>,
        evidence_digest: Sha256Digest,
    },
    Cancelled {
        evidence_digest: Sha256Digest,
    },
    TimedOut {
        evidence_digest: Sha256Digest,
    },
    UnknownOutcome {
        evidence_digest: Sha256Digest,
    },
}

impl SandboxTerminalOutcomeV1 {
    pub fn job_state(&self) -> JobState {
        match self {
            Self::Succeeded { .. } => JobState::Succeeded,
            Self::Failed { .. } => JobState::Failed,
            Self::Cancelled { .. } => JobState::Cancelled,
            Self::TimedOut { .. } => JobState::TimedOut,
            Self::UnknownOutcome { .. } => JobState::ReconciliationRequired,
        }
    }

    fn validate_for(
        &self,
        request: &SandboxExecutionRequestV1,
        physical: &SandboxPhysicalEvidenceV1,
    ) -> Result<(), SandboxContractError> {
        match self {
            Self::Succeeded { result } => {
                let expected = SandboxResultEvidenceV1::from_frame(result);
                if !matches!(
                    physical.phase,
                    SandboxPhysicalPhaseV1::Succeeded
                        | SandboxPhysicalPhaseV1::Cleaning
                        | SandboxPhysicalPhaseV1::Absent
                ) || physical.result_evidence.as_deref() != Some(&expected)
                {
                    return Err(SandboxContractError::InvalidTerminal);
                }
                let boot_id = physical
                    .runner_boot_id
                    .as_ref()
                    .ok_or(SandboxContractError::InvalidTerminal)?;
                result.validate_for(request, boot_id, result.canonical_bytes()?.len())
            }
            Self::Failed {
                failure_class,
                result,
                ..
            } => {
                let expected = result.as_deref().map(SandboxResultEvidenceV1::from_frame);
                if !matches!(
                    physical.phase,
                    SandboxPhysicalPhaseV1::Failed
                        | SandboxPhysicalPhaseV1::Cleaning
                        | SandboxPhysicalPhaseV1::Absent
                ) || physical.result_evidence.as_deref() != expected.as_ref()
                {
                    return Err(SandboxContractError::InvalidTerminal);
                }
                if let Some(result) = result {
                    let boot_id = physical
                        .runner_boot_id
                        .as_ref()
                        .ok_or(SandboxContractError::InvalidTerminal)?;
                    result.validate_for(request, boot_id, result.canonical_bytes()?.len())?;
                    if !matches!(
                        &result.result,
                        SandboxRunnerOutcomeV1::Failed {
                            failure_class: result_class,
                            ..
                        } if result_class == failure_class
                    ) {
                        return Err(SandboxContractError::InvalidTerminal);
                    }
                }
                Ok(())
            }
            Self::Cancelled { .. } | Self::TimedOut { .. } => {
                if matches!(
                    physical.phase,
                    SandboxPhysicalPhaseV1::Succeeded
                        | SandboxPhysicalPhaseV1::Failed
                        | SandboxPhysicalPhaseV1::UnknownOutcome
                ) {
                    return Err(SandboxContractError::InvalidTerminal);
                }
                Ok(())
            }
            Self::UnknownOutcome { .. } => {
                if !matches!(
                    physical.phase,
                    SandboxPhysicalPhaseV1::UnknownOutcome
                        | SandboxPhysicalPhaseV1::Cleaning
                        | SandboxPhysicalPhaseV1::Absent
                ) || (physical.phase != SandboxPhysicalPhaseV1::Absent
                    && !physical.cleanup_required)
                {
                    return Err(SandboxContractError::InvalidTerminal);
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum SandboxTerminalSummaryV1 {
    Succeeded {
        result: SandboxResultEvidenceV1,
    },
    Failed {
        failure_class: SandboxFailureClassV1,
        result: Option<SandboxResultEvidenceV1>,
        evidence_digest: Sha256Digest,
    },
    Cancelled {
        evidence_digest: Sha256Digest,
    },
    TimedOut {
        evidence_digest: Sha256Digest,
    },
    UnknownOutcome {
        evidence_digest: Sha256Digest,
    },
}

impl SandboxTerminalSummaryV1 {
    pub fn job_state(&self) -> JobState {
        match self {
            Self::Succeeded { .. } => JobState::Succeeded,
            Self::Failed { .. } => JobState::Failed,
            Self::Cancelled { .. } => JobState::Cancelled,
            Self::TimedOut { .. } => JobState::TimedOut,
            Self::UnknownOutcome { .. } => JobState::ReconciliationRequired,
        }
    }

    fn from_outcome(
        outcome: &SandboxTerminalOutcomeV1,
        request: &SandboxExecutionRequestV1,
        physical: &SandboxPhysicalEvidenceV1,
    ) -> Result<Self, SandboxContractError> {
        outcome.validate_for(request, physical)?;
        Ok(match outcome {
            SandboxTerminalOutcomeV1::Succeeded { result } => Self::Succeeded {
                result: SandboxResultEvidenceV1::from_frame(result),
            },
            SandboxTerminalOutcomeV1::Failed {
                failure_class,
                result,
                evidence_digest,
            } => Self::Failed {
                failure_class: *failure_class,
                result: result.as_deref().map(SandboxResultEvidenceV1::from_frame),
                evidence_digest: evidence_digest.clone(),
            },
            SandboxTerminalOutcomeV1::Cancelled { evidence_digest } => Self::Cancelled {
                evidence_digest: evidence_digest.clone(),
            },
            SandboxTerminalOutcomeV1::TimedOut { evidence_digest } => Self::TimedOut {
                evidence_digest: evidence_digest.clone(),
            },
            SandboxTerminalOutcomeV1::UnknownOutcome { evidence_digest } => Self::UnknownOutcome {
                evidence_digest: evidence_digest.clone(),
            },
        })
    }

    fn validate_for(
        &self,
        plan: &SandboxExecutionPlanV1,
        physical: &SandboxPhysicalEvidenceV1,
    ) -> Result<(), SandboxContractError> {
        match self {
            Self::Succeeded { result } => {
                result.validate_for(plan)?;
                if !matches!(
                    physical.phase,
                    SandboxPhysicalPhaseV1::Succeeded
                        | SandboxPhysicalPhaseV1::Cleaning
                        | SandboxPhysicalPhaseV1::Absent
                ) || physical.result_evidence.as_deref() != Some(result)
                {
                    return Err(SandboxContractError::InvalidTerminal);
                }
            }
            Self::Failed {
                failure_class,
                result,
                ..
            } => {
                if let Some(result) = result {
                    result.validate_for(plan)?;
                }
                if !matches!(
                    physical.phase,
                    SandboxPhysicalPhaseV1::Failed
                        | SandboxPhysicalPhaseV1::Cleaning
                        | SandboxPhysicalPhaseV1::Absent
                ) || physical.result_evidence.as_deref() != result.as_ref()
                    || result.as_ref().is_some_and(|result| {
                        !matches!(
                            result,
                            SandboxResultEvidenceV1::Failed {
                                failure_class: result_class,
                                ..
                            } if result_class == failure_class
                        )
                    })
                {
                    return Err(SandboxContractError::InvalidTerminal);
                }
            }
            Self::Cancelled { .. } | Self::TimedOut { .. } => {
                if matches!(
                    physical.phase,
                    SandboxPhysicalPhaseV1::Succeeded
                        | SandboxPhysicalPhaseV1::Failed
                        | SandboxPhysicalPhaseV1::UnknownOutcome
                ) {
                    return Err(SandboxContractError::InvalidTerminal);
                }
            }
            Self::UnknownOutcome { .. } => {
                if !matches!(
                    physical.phase,
                    SandboxPhysicalPhaseV1::UnknownOutcome
                        | SandboxPhysicalPhaseV1::Cleaning
                        | SandboxPhysicalPhaseV1::Absent
                ) {
                    return Err(SandboxContractError::InvalidTerminal);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxTerminalRecordV1 {
    pub schema_version: u16,
    pub summary: SandboxTerminalSummaryV1,
    pub terminal_digest: Sha256Digest,
}

impl SandboxTerminalRecordV1 {
    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.terminal_digest = digest_without_field(&self, "terminal_digest")?;
        Ok(self)
    }

    fn validate_for(
        &self,
        plan: &SandboxExecutionPlanV1,
        physical: &SandboxPhysicalEvidenceV1,
    ) -> Result<(), SandboxContractError> {
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.terminal_digest != digest_without_field(self, "terminal_digest")?
        {
            return Err(SandboxContractError::InvalidTerminal);
        }
        self.summary.validate_for(plan, physical)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCleanupStateV1 {
    pub schema_version: u16,
    pub required: bool,
    pub generation: u64,
    pub owner_process_generation_id: Option<ResourceId>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl SandboxCleanupStateV1 {
    fn initial() -> Self {
        Self {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            required: false,
            generation: 0,
            owner_process_generation_id: None,
            expires_at: None,
        }
    }

    fn for_terminal(physical: &SandboxPhysicalEvidenceV1) -> Self {
        Self {
            required: physical.cleanup_required,
            ..Self::initial()
        }
    }

    fn claim(
        &self,
        process_generation_id: ResourceId,
        database_now: DateTime<Utc>,
        lease_milliseconds: u64,
    ) -> Result<Self, SandboxContractError> {
        if !self.required
            || process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || lease_milliseconds == 0
            || lease_milliseconds > 120_000
            || self
                .expires_at
                .is_some_and(|expires_at| expires_at > database_now)
        {
            return Err(SandboxContractError::InvalidCleanup);
        }
        let duration = Duration::milliseconds(
            i64::try_from(lease_milliseconds).map_err(|_| SandboxContractError::InvalidCleanup)?,
        );
        let expires_at = database_now
            .checked_add_signed(duration)
            .ok_or(SandboxContractError::InvalidCleanup)?;
        Ok(Self {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            required: true,
            generation: self
                .generation
                .checked_add(1)
                .ok_or(SandboxContractError::InvalidCleanup)?,
            owner_process_generation_id: Some(process_generation_id),
            expires_at: Some(expires_at),
        })
    }

    fn validate(
        &self,
        terminal: bool,
        physical: Option<&SandboxPhysicalEvidenceV1>,
    ) -> Result<(), SandboxContractError> {
        let owner_pair = self
            .owner_process_generation_id
            .as_ref()
            .zip(self.expires_at);
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.owner_process_generation_id.is_some() != self.expires_at.is_some()
            || self
                .owner_process_generation_id
                .as_ref()
                .is_some_and(|owner| owner.kind() != ResourceKind::WorkerProcessGeneration)
            || owner_pair.is_some_and(|_| self.generation == 0)
            || (!terminal
                && (self.required
                    || self.generation != 0
                    || self.owner_process_generation_id.is_some()))
            || (terminal
                && self.required != physical.is_some_and(|physical| physical.cleanup_required))
            || (!self.required && self.owner_process_generation_id.is_some())
        {
            return Err(SandboxContractError::InvalidCleanup);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxDispatcherJobPayloadV1 {
    pub schema_version: u16,
    pub plan: Box<SandboxExecutionPlanV1>,
    pub submission_digest: Sha256Digest,
    pub physical: Option<Box<SandboxPhysicalEvidenceV1>>,
    pub terminal: Option<Box<SandboxTerminalRecordV1>>,
    pub cleanup: SandboxCleanupStateV1,
    pub payload_digest: Sha256Digest,
}

impl SandboxDispatcherJobPayloadV1 {
    pub fn accepted(plan: SandboxExecutionPlanV1) -> Result<Self, SandboxContractError> {
        plan.validate()?;
        let submission_digest = plan.request_digest.clone();
        Self {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            plan: Box::new(plan),
            submission_digest,
            physical: None,
            terminal: None,
            cleanup: SandboxCleanupStateV1::initial(),
            payload_digest: zero_digest(),
        }
        .seal()
    }

    pub fn seal(mut self) -> Result<Self, SandboxContractError> {
        self.payload_digest = digest_without_field(&self, "payload_digest")?;
        let bytes = canonical_json(
            &serde_json::to_value(&self).map_err(|_| SandboxContractError::Canonical)?,
        )
        .map_err(|_| SandboxContractError::Canonical)?;
        if bytes.len() > MAX_SANDBOX_JOB_PAYLOAD_BYTES {
            return Err(SandboxContractError::InvalidJob);
        }
        Ok(self)
    }

    pub fn validate_for(&self, job: &JobProjection) -> Result<(), SandboxContractError> {
        self.plan.validate()?;
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || job.tenant_id != self.plan.tenant_id
            || job.job_id != self.plan.job_id
            || job.work_class != WorkClass::Sandbox
            || job.owner.owner_kind != ResourceKind::Job
            || job.owner.owner_id != self.plan.job_id
            || job.deadline != self.plan.deadline_at
            || job.attempt_limit != 1
            || self.submission_digest != self.plan.request_digest
            || self.payload_digest != digest_without_field(self, "payload_digest")?
        {
            return Err(SandboxContractError::InvalidJob);
        }
        if let Some(physical) = &self.physical {
            physical.validate_for(&self.plan)?;
        }
        if let Some(terminal) = &self.terminal {
            terminal.validate_for(
                &self.plan,
                self.physical
                    .as_deref()
                    .ok_or(SandboxContractError::InvalidTerminal)?,
            )?;
            if terminal.summary.job_state() != job.state {
                return Err(SandboxContractError::InvalidTerminal);
            }
        }
        self.cleanup
            .validate(self.terminal.is_some(), self.physical.as_deref())?;
        let state_matches = match job.state {
            JobState::Ready | JobState::Leased => self.terminal.is_none(),
            JobState::Running | JobState::Cancelling => {
                self.physical.is_some() && self.terminal.is_none()
            }
            JobState::Succeeded
            | JobState::Failed
            | JobState::Cancelled
            | JobState::TimedOut
            | JobState::ReconciliationRequired => self.terminal.is_some(),
            _ => false,
        };
        if !state_matches || (job.attempt_count == 0) != self.physical.is_none() {
            return Err(SandboxContractError::InvalidJob);
        }
        Ok(())
    }

    pub fn begin(
        &self,
        request: &SandboxExecutionRequestV1,
        activation_token: OpaqueActivationToken,
        database_now: DateTime<Utc>,
    ) -> Result<Self, SandboxContractError> {
        self.plan.validate_request(request)?;
        if self.physical.is_some() || self.terminal.is_some() {
            return Err(SandboxContractError::InvalidPhysicalTransition);
        }
        let mut next = self.clone();
        next.physical = Some(Box::new(SandboxPhysicalEvidenceV1::begin(
            request,
            activation_token,
            database_now,
        )?));
        next.seal()
    }

    pub fn authorize_candidate_create(
        &self,
        request: &SandboxExecutionRequestV1,
        limits: &SandboxProvisioningLimitsV1,
        database_now: DateTime<Utc>,
        create_ordinal: u8,
    ) -> Result<PhysicalDecision<Self>, SandboxContractError> {
        self.plan.validate_request(request)?;
        let physical = self
            .physical
            .as_deref()
            .ok_or(SandboxContractError::InvalidPhysicalTransition)?;
        match physical.authorize_candidate_create(request, limits, database_now, create_ordinal)? {
            PhysicalDecision::Applied(authorized) => {
                let mut next = self.clone();
                next.physical = Some(Box::new(authorized));
                Ok(PhysicalDecision::Applied(next.seal()?))
            }
            PhysicalDecision::Replayed(_) => Ok(PhysicalDecision::Replayed(self.clone())),
        }
    }

    pub fn record_observation(
        &self,
        request: &SandboxExecutionRequestV1,
        observation: SandboxDurableObservationV1,
    ) -> Result<Self, SandboxContractError> {
        self.plan.validate_request(request)?;
        let physical = self
            .physical
            .as_deref()
            .ok_or(SandboxContractError::InvalidPhysicalTransition)?;
        let next_physical = match observation {
            SandboxDurableObservationV1::Candidate { candidate, limits } => {
                physical.observe_candidate(request, &candidate, &limits)?
            }
            SandboxDurableObservationV1::RunnerState { frame } => {
                physical.record_runner_state(&frame)?
            }
            SandboxDurableObservationV1::Result { frame } => {
                physical.record_result(request, &frame)?
            }
        };
        let mut next = self.clone();
        next.physical = Some(Box::new(next_physical));
        next.seal()
    }

    pub fn select_candidate(
        &self,
        request: &SandboxExecutionRequestV1,
        candidate: &SandboxCandidateV1,
    ) -> Result<PhysicalDecision<Self>, SandboxContractError> {
        self.plan.validate_request(request)?;
        let physical = self
            .physical
            .as_deref()
            .ok_or(SandboxContractError::InvalidPhysicalTransition)?;
        match physical.select_candidate(request, candidate)? {
            PhysicalDecision::Applied(selected) => {
                let mut next = self.clone();
                next.physical = Some(Box::new(selected));
                Ok(PhysicalDecision::Applied(next.seal()?))
            }
            PhysicalDecision::Replayed(_) => Ok(PhysicalDecision::Replayed(self.clone())),
        }
    }

    pub fn authorize_activation(
        &self,
        sandbox_id: &OpenSandboxId,
        boot_id: RunnerBootId,
    ) -> Result<PhysicalDecision<Self>, SandboxContractError> {
        let physical = self
            .physical
            .as_deref()
            .ok_or(SandboxContractError::InvalidPhysicalTransition)?;
        match physical.authorize_activation(sandbox_id, boot_id)? {
            PhysicalDecision::Applied(authorized) => {
                let mut next = self.clone();
                next.physical = Some(Box::new(authorized));
                Ok(PhysicalDecision::Applied(next.seal()?))
            }
            PhysicalDecision::Replayed(_) => Ok(PhysicalDecision::Replayed(self.clone())),
        }
    }

    pub fn terminal(
        &self,
        request: &SandboxExecutionRequestV1,
        outcome: SandboxTerminalOutcomeV1,
    ) -> Result<Self, SandboxContractError> {
        self.plan.validate_request(request)?;
        if self.terminal.is_some() {
            return Err(SandboxContractError::TerminalConflict);
        }
        let physical = self
            .physical
            .as_deref()
            .ok_or(SandboxContractError::InvalidTerminal)?;
        let terminal = SandboxTerminalRecordV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            summary: SandboxTerminalSummaryV1::from_outcome(&outcome, request, physical)?,
            terminal_digest: zero_digest(),
        }
        .seal()?;
        terminal.validate_for(&self.plan, physical)?;
        let mut next = self.clone();
        next.terminal = Some(Box::new(terminal));
        next.cleanup = SandboxCleanupStateV1::for_terminal(physical);
        next.seal()
    }

    pub fn terminal_replays(
        &self,
        request: &SandboxExecutionRequestV1,
        outcome: &SandboxTerminalOutcomeV1,
    ) -> Result<bool, SandboxContractError> {
        self.plan.validate_request(request)?;
        let physical = self
            .physical
            .as_deref()
            .ok_or(SandboxContractError::InvalidTerminal)?;
        let expected = SandboxTerminalSummaryV1::from_outcome(outcome, request, physical)?;
        Ok(self
            .terminal
            .as_deref()
            .is_some_and(|terminal| terminal.summary == expected))
    }

    pub fn claim_cleanup(
        &self,
        job: &JobProjection,
        process_generation_id: ResourceId,
        database_now: DateTime<Utc>,
        lease_milliseconds: u64,
        claimed_job_version: u64,
    ) -> Result<(Self, SandboxCleanupFenceV1), SandboxContractError> {
        self.validate_for(job)?;
        if self.terminal.is_none() || claimed_job_version != job.version.saturating_add(1) {
            return Err(SandboxContractError::InvalidCleanup);
        }
        let cleanup = self.cleanup.claim(
            process_generation_id.clone(),
            database_now,
            lease_milliseconds,
        )?;
        let physical_attempt = self
            .physical
            .as_deref()
            .ok_or(SandboxContractError::InvalidCleanup)?
            .provisioning_token
            .physical_attempt;
        let expires_at = cleanup
            .expires_at
            .ok_or(SandboxContractError::InvalidCleanup)?;
        let mut next = self.clone();
        next.cleanup = cleanup.clone();
        let next = next.seal()?;
        let fence = SandboxCleanupFenceV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            tenant_id: self.plan.tenant_id.clone(),
            job_id: self.plan.job_id.clone(),
            expected_job_version: claimed_job_version,
            physical_attempt,
            cleanup_generation: cleanup.generation,
            process_generation_id,
            expires_at,
        };
        fence.validate()?;
        Ok((next, fence))
    }

    pub fn record_cleanup_observation(
        &self,
        job: &JobProjection,
        fence: &SandboxCleanupFenceV1,
        database_now: DateTime<Utc>,
        observation: SandboxCleanupObservationV1,
        next_job_version: u64,
    ) -> Result<(Self, Option<SandboxCleanupFenceV1>), SandboxContractError> {
        self.validate_for(job)?;
        fence.validate()?;
        let physical = self
            .physical
            .as_deref()
            .ok_or(SandboxContractError::InvalidCleanup)?;
        if self.terminal.is_none()
            || !self.cleanup.required
            || fence.tenant_id != self.plan.tenant_id
            || fence.job_id != self.plan.job_id
            || fence.expected_job_version != job.version
            || fence.physical_attempt != physical.provisioning_token.physical_attempt
            || fence.cleanup_generation != self.cleanup.generation
            || self.cleanup.owner_process_generation_id.as_ref()
                != Some(&fence.process_generation_id)
            || self.cleanup.expires_at != Some(fence.expires_at)
            || database_now >= fence.expires_at
            || next_job_version != job.version.saturating_add(1)
        {
            return Err(SandboxContractError::InvalidCleanup);
        }
        let next_physical = match observation {
            SandboxCleanupObservationV1::Absent {
                sandbox_id,
                evidence_digest,
            } => physical.mark_absent(&sandbox_id, evidence_digest)?,
        };
        let completed = !next_physical.cleanup_required;
        let mut next = self.clone();
        next.physical = Some(Box::new(next_physical));
        if completed {
            next.cleanup.required = false;
            next.cleanup.owner_process_generation_id = None;
            next.cleanup.expires_at = None;
        }
        let next = next.seal()?;
        let next_fence = (!completed).then(|| SandboxCleanupFenceV1 {
            expected_job_version: next_job_version,
            ..fence.clone()
        });
        Ok((next, next_fence))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "observation", rename_all = "snake_case", deny_unknown_fields)]
pub enum SandboxDurableObservationV1 {
    Candidate {
        candidate: SandboxCandidateV1,
        limits: SandboxProvisioningLimitsV1,
    },
    RunnerState {
        frame: SandboxRunnerStateFrameV1,
    },
    Result {
        frame: SandboxRunnerResultFrameV1,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "observation", rename_all = "snake_case", deny_unknown_fields)]
pub enum SandboxCleanupObservationV1 {
    Absent {
        sandbox_id: OpenSandboxId,
        evidence_digest: Sha256Digest,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCleanupFenceV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub expected_job_version: u64,
    pub physical_attempt: u32,
    pub cleanup_generation: u64,
    pub process_generation_id: ResourceId,
    pub expires_at: DateTime<Utc>,
}

impl SandboxCleanupFenceV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_job_version == 0
            || self.physical_attempt == 0
            || self.cleanup_generation == 0
            || self.process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
        {
            return Err(SandboxContractError::InvalidCleanup);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCleanupClaimV1 {
    pub process_generation_id: ResourceId,
    pub limit: u16,
    pub lease_milliseconds: u64,
}

impl SandboxCleanupClaimV1 {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_lease_milliseconds: u64,
    ) -> Result<(), SandboxContractError> {
        if self.process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.limit == 0
            || self.limit > maximum_batch
            || self.lease_milliseconds == 0
            || self.lease_milliseconds > maximum_lease_milliseconds
        {
            return Err(SandboxContractError::InvalidCleanup);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordSandboxCleanupObservationV1 {
    pub fence: SandboxCleanupFenceV1,
    pub observation: SandboxCleanupObservationV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxClaimV1 {
    pub worker_process_generation_id: ResourceId,
    pub lease_token_digests: Vec<Sha256Digest>,
    pub limit: u16,
    pub lease_milliseconds: u64,
}

impl SandboxClaimV1 {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_lease_milliseconds: u64,
    ) -> Result<(), SandboxContractError> {
        let unique = self
            .lease_token_digests
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.limit == 0
            || self.limit > maximum_batch
            || self.lease_token_digests.len() != usize::from(self.limit)
            || unique.len() != self.lease_token_digests.len()
            || self.lease_milliseconds == 0
            || self.lease_milliseconds > maximum_lease_milliseconds
        {
            return Err(SandboxContractError::InvalidJob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeasedSandboxJobV1 {
    pub job: JobProjection,
    pub payload: SandboxDispatcherJobPayloadV1,
    pub request: SandboxExecutionRequestV1,
    pub fence: JobFence,
    pub usage_reservation_id: ResourceId,
}

impl LeasedSandboxJobV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        self.payload.validate_for(&self.job)?;
        self.payload.plan.validate_request(&self.request)?;
        let lease = self
            .job
            .lease
            .as_ref()
            .ok_or(SandboxContractError::InvalidJob)?;
        if self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.fence.expected_version != self.job.version
            || self.fence.worker_process_generation_id != lease.worker_process_generation_id
            || self.fence.lease_generation != lease.lease_generation
            || self.fence.token_digest != lease.token_digest
            || self.request.lease_generation != lease.lease_generation
            || self.request.worker_process_generation_id != lease.worker_process_generation_id
            || self.request.trace != self.job.trace
            || self.payload.physical.as_deref().is_some_and(|physical| {
                physical.provisioning_token.physical_attempt != self.request.physical_attempt
            })
            || self.payload.physical.is_none()
                && self.request.physical_attempt != self.job.attempt_count.saturating_add(1)
        {
            return Err(SandboxContractError::InvalidJob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimedSandboxCleanupV1 {
    pub job: JobProjection,
    pub payload: SandboxDispatcherJobPayloadV1,
    pub fence: SandboxCleanupFenceV1,
}

impl ClaimedSandboxCleanupV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        self.payload.validate_for(&self.job)?;
        self.fence.validate()?;
        let common_invalid = self.job.version != self.fence.expected_job_version
            || self.job.tenant_id != self.fence.tenant_id
            || self.job.job_id != self.fence.job_id
            || self.job.lease.is_some();
        let active_invalid = self.payload.cleanup.required
            && (self.payload.cleanup.generation != self.fence.cleanup_generation
                || self.payload.cleanup.owner_process_generation_id.as_ref()
                    != Some(&self.fence.process_generation_id)
                || self.payload.cleanup.expires_at != Some(self.fence.expires_at));
        let complete_invalid = !self.payload.cleanup.required
            && (self.payload.cleanup.generation != self.fence.cleanup_generation
                || self.payload.cleanup.owner_process_generation_id.is_some()
                || self.payload.cleanup.expires_at.is_some()
                || self.payload.physical.as_deref().is_none_or(|physical| {
                    physical.phase != SandboxPhysicalPhaseV1::Absent || physical.cleanup_required
                }));
        if common_invalid || active_invalid || complete_invalid {
            return Err(SandboxContractError::InvalidCleanup);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxRepositoryDecisionV1 {
    pub job: JobProjection,
    pub payload: SandboxDispatcherJobPayloadV1,
    pub fence: Option<JobFence>,
}

impl SandboxRepositoryDecisionV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        self.payload.validate_for(&self.job)?;
        match (&self.job.lease, &self.fence) {
            (Some(lease), Some(fence))
                if fence.expected_version == self.job.version
                    && fence.worker_process_generation_id == lease.worker_process_generation_id
                    && fence.lease_generation == lease.lease_generation
                    && fence.token_digest == lease.token_digest =>
            {
                Ok(())
            }
            (None, None)
                if self.payload.terminal.is_some() || self.job.state == JobState::Ready =>
            {
                Ok(())
            }
            _ => Err(SandboxContractError::InvalidJob),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateCreateAuthorizationV1 {
    pub decision: SandboxRepositoryDecisionV1,
    pub create_ordinal: u8,
}

impl CandidateCreateAuthorizationV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        self.decision.validate()?;
        let physical = self
            .decision
            .payload
            .physical
            .as_deref()
            .ok_or(SandboxContractError::CandidateCreateConflict)?;
        if self.create_ordinal == 0
            || self.create_ordinal != physical.create_authorization_count
            || physical.phase != SandboxPhysicalPhaseV1::Provisioning
        {
            return Err(SandboxContractError::CandidateCreateConflict);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxFencedIdentityV1 {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
}

impl SandboxFencedIdentityV1 {
    pub fn validate(&self) -> Result<(), SandboxContractError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.job_id.kind() != ResourceKind::Job
            || self.fence.expected_version == 0
            || self.fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.fence.lease_generation == 0
        {
            return Err(SandboxContractError::InvalidJob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordProvisioningIntentV1 {
    pub identity: SandboxFencedIdentityV1,
    pub activation_token: OpaqueActivationToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizeCandidateCreateV1 {
    pub identity: SandboxFencedIdentityV1,
    pub create_ordinal: u8,
    pub limits: SandboxProvisioningLimitsV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordSandboxObservationV1 {
    pub identity: SandboxFencedIdentityV1,
    pub observation: SandboxDurableObservationV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectSandboxCandidateV1 {
    pub identity: SandboxFencedIdentityV1,
    pub candidate: SandboxCandidateV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizeSandboxActivationV1 {
    pub identity: SandboxFencedIdentityV1,
    pub sandbox_id: OpenSandboxId,
    pub boot_id: RunnerBootId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitSandboxTerminalV1 {
    pub identity: SandboxFencedIdentityV1,
    pub outcome: SandboxTerminalOutcomeV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatSandboxJobV1 {
    pub identity: SandboxFencedIdentityV1,
    pub lease_milliseconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxOrphanDispositionV1 {
    RetainProvisioning,
    RetainSelected,
    DeleteUnselected,
    DeleteLateCandidate,
    DeleteStaleAttempt,
    DeleteMissingOwner,
}

impl SandboxOrphanDispositionV1 {
    pub fn may_delete(self) -> bool {
        matches!(
            self,
            Self::DeleteUnselected
                | Self::DeleteLateCandidate
                | Self::DeleteStaleAttempt
                | Self::DeleteMissingOwner
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxOrphanDecisionV1 {
    pub schema_version: u16,
    pub candidate: SandboxCandidateV1,
    pub disposition: SandboxOrphanDispositionV1,
}

impl SandboxOrphanDecisionV1 {
    pub fn new(
        candidate: SandboxCandidateV1,
        disposition: SandboxOrphanDispositionV1,
    ) -> Result<Self, SandboxContractError> {
        let decision = Self {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            candidate,
            disposition,
        };
        decision.validate()?;
        Ok(decision)
    }

    pub fn validate(&self) -> Result<(), SandboxContractError> {
        self.candidate.validate_shape()?;
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION {
            return Err(SandboxContractError::InvalidCandidate);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxRecoveryDecisionV1 {
    ProvisionOrDiscover,
    ObserveSelected,
    ReplayAuthorizedActivation,
    ObservePotentiallyStarted,
    CommitTerminalEvidence,
    Cleanup,
    Complete,
}

pub fn decide_recovery(
    payload: &SandboxDispatcherJobPayloadV1,
) -> Result<SandboxRecoveryDecisionV1, SandboxContractError> {
    if payload.terminal.is_some() {
        return Ok(if payload.cleanup.required {
            SandboxRecoveryDecisionV1::Cleanup
        } else {
            SandboxRecoveryDecisionV1::Complete
        });
    }
    let Some(physical) = payload.physical.as_deref() else {
        return Ok(SandboxRecoveryDecisionV1::ProvisionOrDiscover);
    };
    Ok(match physical.phase {
        SandboxPhysicalPhaseV1::Provisioning => SandboxRecoveryDecisionV1::ProvisionOrDiscover,
        SandboxPhysicalPhaseV1::CandidateSelected => SandboxRecoveryDecisionV1::ObserveSelected,
        SandboxPhysicalPhaseV1::ActivationAuthorized => {
            SandboxRecoveryDecisionV1::ReplayAuthorizedActivation
        }
        SandboxPhysicalPhaseV1::Started => SandboxRecoveryDecisionV1::ObservePotentiallyStarted,
        SandboxPhysicalPhaseV1::Succeeded
        | SandboxPhysicalPhaseV1::Failed
        | SandboxPhysicalPhaseV1::UnknownOutcome => {
            SandboxRecoveryDecisionV1::CommitTerminalEvidence
        }
        SandboxPhysicalPhaseV1::Cleaning => SandboxRecoveryDecisionV1::Cleanup,
        SandboxPhysicalPhaseV1::Absent => SandboxRecoveryDecisionV1::Complete,
    })
}

#[async_trait]
pub trait SandboxJobRepository: Send + Sync {
    type Error: Send;

    async fn claim(&self, request: SandboxClaimV1) -> Result<Vec<LeasedSandboxJobV1>, Self::Error>;

    async fn heartbeat(
        &self,
        command: HeartbeatSandboxJobV1,
    ) -> Result<SandboxRepositoryDecisionV1, Self::Error>;

    async fn record_provisioning_intent(
        &self,
        command: RecordProvisioningIntentV1,
    ) -> Result<SandboxRepositoryDecisionV1, Self::Error>;

    async fn authorize_candidate_create(
        &self,
        command: AuthorizeCandidateCreateV1,
    ) -> Result<PhysicalDecision<CandidateCreateAuthorizationV1>, Self::Error>;

    async fn select_candidate(
        &self,
        command: SelectSandboxCandidateV1,
    ) -> Result<PhysicalDecision<SandboxRepositoryDecisionV1>, Self::Error>;

    async fn authorize_activation(
        &self,
        command: AuthorizeSandboxActivationV1,
    ) -> Result<PhysicalDecision<SandboxRepositoryDecisionV1>, Self::Error>;

    async fn record_physical_observation(
        &self,
        command: RecordSandboxObservationV1,
    ) -> Result<SandboxRepositoryDecisionV1, Self::Error>;

    async fn commit_terminal(
        &self,
        command: CommitSandboxTerminalV1,
    ) -> Result<PhysicalDecision<SandboxRepositoryDecisionV1>, Self::Error>;

    async fn claim_cleanup(
        &self,
        request: SandboxCleanupClaimV1,
    ) -> Result<Vec<ClaimedSandboxCleanupV1>, Self::Error>;

    async fn record_cleanup_observation(
        &self,
        command: RecordSandboxCleanupObservationV1,
    ) -> Result<ClaimedSandboxCleanupV1, Self::Error>;

    async fn decide_orphan(
        &self,
        candidate: SandboxCandidateV1,
    ) -> Result<SandboxOrphanDecisionV1, Self::Error>;

    async fn recover(
        &self,
        tenant_id: &ResourceId,
        job_id: &ResourceId,
    ) -> Result<SandboxRepositoryDecisionV1, Self::Error>;
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
    pub fn from_authorized(
        request: &SandboxExecutionRequestV1,
        evidence: &SandboxPhysicalEvidenceV1,
    ) -> Result<Self, SandboxContractError> {
        request.validate()?;
        let boot_id = evidence
            .runner_boot_id
            .clone()
            .ok_or(SandboxContractError::InvalidActivation)?;
        let frame = Self {
            magic: String::new(),
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            activation_token: evidence.activation_token.clone(),
            boot_id,
            execution_request_digest: request.request_digest.clone(),
            input_schema_digest: request.input_schema_digest.clone(),
            input_digest: request.input_digest.clone(),
            declared_input_bytes: 0,
            input: request.input.clone(),
            frame_digest: zero_digest(),
        }
        .seal()?;
        frame.validate_for(request, evidence)?;
        Ok(frame)
    }

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum SandboxResultEvidenceV1 {
    Succeeded {
        frame_digest: Sha256Digest,
        output_schema_digest: Sha256Digest,
        output_digest: Sha256Digest,
        declared_output_bytes: u64,
    },
    Failed {
        frame_digest: Sha256Digest,
        failure_class: SandboxFailureClassV1,
        diagnostic_digest: Sha256Digest,
        diagnostic_bytes: u32,
    },
}

impl SandboxResultEvidenceV1 {
    pub fn from_frame(frame: &SandboxRunnerResultFrameV1) -> Self {
        match &frame.result {
            SandboxRunnerOutcomeV1::Succeeded {
                output_schema_digest,
                output_digest,
                declared_output_bytes,
                ..
            } => Self::Succeeded {
                frame_digest: frame.frame_digest.clone(),
                output_schema_digest: output_schema_digest.clone(),
                output_digest: output_digest.clone(),
                declared_output_bytes: *declared_output_bytes,
            },
            SandboxRunnerOutcomeV1::Failed {
                failure_class,
                diagnostic_digest,
                diagnostic_bytes,
            } => Self::Failed {
                frame_digest: frame.frame_digest.clone(),
                failure_class: *failure_class,
                diagnostic_digest: diagnostic_digest.clone(),
                diagnostic_bytes: *diagnostic_bytes,
            },
        }
    }

    pub fn frame_digest(&self) -> &Sha256Digest {
        match self {
            Self::Succeeded { frame_digest, .. } | Self::Failed { frame_digest, .. } => {
                frame_digest
            }
        }
    }

    pub fn validate_for(&self, plan: &SandboxExecutionPlanV1) -> Result<(), SandboxContractError> {
        plan.validate()?;
        match self {
            Self::Succeeded {
                output_schema_digest,
                declared_output_bytes,
                ..
            } if output_schema_digest != &plan.output_schema_digest
                || *declared_output_bytes > plan.limits.maximum_output_bytes =>
            {
                Err(SandboxContractError::InvalidResult)
            }
            Self::Failed {
                diagnostic_bytes, ..
            } if *diagnostic_bytes > MAX_SANDBOX_DIAGNOSTIC_BYTES => {
                Err(SandboxContractError::InvalidResult)
            }
            _ => Ok(()),
        }
    }
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
    pub fn from_authorization(
        request: &SandboxExecutionRequestV1,
        evidence: &SandboxPhysicalEvidenceV1,
        create_ordinal: u8,
        ttl_seconds: u32,
    ) -> Result<Self, SandboxContractError> {
        request.validate()?;
        evidence.validate_for(&SandboxExecutionPlanV1::from_request(request)?)?;
        if evidence.phase != SandboxPhysicalPhaseV1::Provisioning
            || create_ordinal == 0
            || create_ordinal != evidence.create_authorization_count
        {
            return Err(SandboxContractError::CandidateCreateConflict);
        }
        let metadata = SandboxCandidateMetadataV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            tenant_id: request.tenant_id.clone(),
            job_id: request.job_id.clone(),
            physical_attempt: request.physical_attempt,
            create_ordinal,
            provisioning_token_digest: evidence.provisioning_token_digest.clone(),
            execution_request_digest: request.request_digest.clone(),
            runtime_contract_digest: request.runtime_contract_digest.clone(),
            profile_deployment_digest: request.profile_deployment_digest.clone(),
            network_mode: request.network_mode,
        };
        let request = Self {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            image_uri: request.image_uri.clone(),
            entrypoint: request.runner_argv.clone(),
            metadata,
            runner_config: SandboxRunnerConfigV1::from_request(
                request,
                evidence.activation_token_digest.clone(),
                request.provisioning_limits.diagnostic_bytes,
            )?,
            resource_limits: request.limits.clone(),
            ttl_seconds,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), SandboxContractError> {
        self.runner_config.validate()?;
        self.resource_limits.validate()?;
        self.metadata.validate_shape()?;
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

    async fn list_operator_candidates(
        &self,
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
    CandidateCreateConflict,
    CandidateLimitOrLateCandidate,
    CandidateSelectionConflict,
    InvalidActivation,
    RunnerBootChanged,
    InvalidRunnerFrame,
    InvalidResult,
    InvalidPhysicalTransition,
    InvalidJob,
    InvalidTerminal,
    InvalidCleanup,
    TerminalConflict,
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
            Self::CandidateCreateConflict => "sandbox candidate create authorization conflicted",
            Self::CandidateLimitOrLateCandidate => "sandbox candidate is late or over limit",
            Self::CandidateSelectionConflict => "another sandbox candidate already won",
            Self::InvalidActivation => "sandbox activation frame is invalid",
            Self::RunnerBootChanged => "sandbox runner boot identity changed",
            Self::InvalidRunnerFrame => "sandbox runner state frame is invalid",
            Self::InvalidResult => "sandbox runner result is invalid",
            Self::InvalidPhysicalTransition => "sandbox physical transition is invalid",
            Self::InvalidJob => "sandbox Job payload is invalid",
            Self::InvalidTerminal => "sandbox terminal evidence is invalid",
            Self::InvalidCleanup => "sandbox cleanup fence or evidence is invalid",
            Self::TerminalConflict => "sandbox terminal commit already has a winner",
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

fn zero_digest() -> Sha256Digest {
    format!("sha256:{}", "0".repeat(64))
        .parse()
        .expect("the all-zero SHA-256 digest is valid")
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
    use insight_platform_jobs::{
        decide_claim, decide_start, decide_terminal, decide_terminal_physical_update, JobOwnerRef,
        LeasePolicy,
    };
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
            maximum_input_bytes: MAX_SANDBOX_INPUT_BYTES,
            maximum_output_bytes: MAX_SANDBOX_OUTPUT_BYTES,
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
            lease_generation: 1,
            physical_attempt,
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 7),
            package_version_id: id(ResourceKind::SandboxPackageRevision, 4),
            image_uri: format!("registry.invalid/package@sha256:{}", "a".repeat(64)),
            runtime_version_id: id(ResourceKind::SandboxRuntimeRevision, 5),
            runtime_contract_digest: digest('d'),
            sandbox_profile_deployment_id: id(ResourceKind::SandboxProfileDeployment, 6),
            profile_deployment_digest: digest('e'),
            runner_argv: vec!["/usr/local/bin/platform-sandbox-runner".to_owned()],
            package_argv: vec!["/opt/insight/package".to_owned()],
            input_value_id: id(ResourceKind::RunValue, 8),
            output_value_id: id(ResourceKind::RunValue, 9),
            classification: DataClassification::Internal,
            input: serde_json::json!({"question":"answer"}),
            input_schema_digest: digest('b'),
            input_digest: digest('0'),
            output_schema_digest: digest('c'),
            network_mode: SandboxNetworkMode::Direct,
            limits: limits(),
            provisioning_limits: provisioning_limits(),
            deadline_at: Utc.timestamp_opt(2_000_000_000, 0).unwrap(),
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
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
                tenant_id: request.tenant_id.clone(),
                job_id: request.job_id.clone(),
                physical_attempt: request.physical_attempt,
                create_ordinal: 1,
                provisioning_token_digest: token.digest().unwrap(),
                execution_request_digest: request.request_digest.clone(),
                runtime_contract_digest: request.runtime_contract_digest.clone(),
                profile_deployment_digest: request.profile_deployment_digest.clone(),
                network_mode: request.network_mode,
            },
            observed_at: request.deadline_at - Duration::seconds(1),
        }
    }

    fn ready_job(request: &SandboxExecutionRequestV1) -> JobProjection {
        JobProjection {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: request.tenant_id.clone(),
            job_id: request.job_id.clone(),
            work_class: WorkClass::Sandbox,
            owner: JobOwnerRef {
                owner_id: request.job_id.clone(),
                owner_kind: ResourceKind::Job,
            },
            state: JobState::Ready,
            version: 1,
            attempt_count: 0,
            attempt_limit: 1,
            lease_generation: 0,
            lease: None,
            scheduled_at: request.deadline_at - Duration::minutes(5),
            retry_at: None,
            wake: None,
            deadline: request.deadline_at,
        }
    }

    #[test]
    fn semantic_request_digest_excludes_recovery_correlation_but_binds_input_digest() {
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
        let mut rebound = first.clone();
        rebound.lease_generation += 1;
        rebound.worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 99);
        rebound.trace = insight_platform_contracts::TraceIdentityV1::generate();
        assert_eq!(first.request_digest, rebound.semantic_digest().unwrap());
        let mut changed_input = first.clone();
        changed_input.input = serde_json::json!({"question":"different"});
        changed_input = changed_input.seal().unwrap();
        assert_ne!(first.request_digest, changed_input.request_digest);
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
    fn candidate_create_authorization_is_one_shot_ordered_and_time_bounded() {
        let request = request(1);
        let limits = provisioning_limits();
        let now = request.deadline_at - Duration::minutes(4);
        let evidence = SandboxPhysicalEvidenceV1::begin(
            &request,
            OpaqueActivationToken::parse("1".repeat(64)).unwrap(),
            now,
        )
        .unwrap();
        assert_eq!(
            evidence.authorize_candidate_create(&request, &limits, now, 2),
            Err(SandboxContractError::CandidateCreateConflict)
        );
        let first = evidence
            .authorize_candidate_create(&request, &limits, now, 1)
            .unwrap()
            .into_inner();
        let create = OpenSandboxCreateV1::from_authorization(&request, &first, 1, 60).unwrap();
        assert_eq!(create.metadata.create_ordinal, 1);
        assert_eq!(create.metadata.network_mode, SandboxNetworkMode::Direct);
        assert_eq!(create.entrypoint, request.runner_argv);
        assert_eq!(
            create.runner_config.activation_token_digest,
            first.activation_token_digest
        );
        assert!(matches!(
            first
                .authorize_candidate_create(&request, &limits, now, 1)
                .unwrap(),
            PhysicalDecision::Replayed(_)
        ));
        assert_eq!(
            first.authorize_candidate_create(
                &request,
                &limits,
                now + Duration::milliseconds(499),
                2,
            ),
            Err(SandboxContractError::CandidateLimitOrLateCandidate)
        );
        let second = first
            .authorize_candidate_create(&request, &limits, now + Duration::milliseconds(500), 2)
            .unwrap()
            .into_inner();
        assert_eq!(second.create_authorization_count, 2);
        assert_eq!(
            second.authorize_candidate_create(
                &request,
                &limits,
                now + Duration::milliseconds(1_000),
                3,
            ),
            Err(SandboxContractError::CandidateCreateConflict)
        );

        let timed_out = SandboxPhysicalEvidenceV1::begin(
            &request,
            OpaqueActivationToken::parse("2".repeat(64)).unwrap(),
            now,
        )
        .unwrap();
        assert_eq!(
            timed_out.authorize_candidate_create(
                &request,
                &limits,
                now + Duration::milliseconds(10_001),
                1,
            ),
            Err(SandboxContractError::CandidateLimitOrLateCandidate)
        );
    }

    #[test]
    fn candidate_selection_is_first_winner_and_activation_closes_create() {
        let request = request(1);
        let token = OpaqueActivationToken::parse("1".repeat(64)).unwrap();
        let now = request.deadline_at - Duration::minutes(4);
        let evidence = SandboxPhysicalEvidenceV1::begin(&request, token, now)
            .unwrap()
            .authorize_candidate_create(&request, &provisioning_limits(), now, 1)
            .unwrap()
            .into_inner()
            .authorize_candidate_create(
                &request,
                &provisioning_limits(),
                now + Duration::milliseconds(500),
                2,
            )
            .unwrap()
            .into_inner();
        let first = candidate(&request, "sandbox-one");
        let second = candidate(&request, "sandbox-two");
        let third = candidate(&request, "sandbox-three");
        let mut wrong_runtime = first.clone();
        wrong_runtime.metadata.runtime_contract_digest = digest('f');
        assert_eq!(
            evidence.observe_candidate(&request, &wrong_runtime, &provisioning_limits()),
            Err(SandboxContractError::InvalidCandidate)
        );
        let mut wrong_profile = first.clone();
        wrong_profile.metadata.profile_deployment_digest = digest('9');
        assert_eq!(
            evidence.observe_candidate(&request, &wrong_profile, &provisioning_limits()),
            Err(SandboxContractError::InvalidCandidate)
        );
        let mut wrong_tenant = first.clone();
        wrong_tenant.metadata.tenant_id = id(ResourceKind::Tenant, 81);
        assert_eq!(
            evidence.observe_candidate(&request, &wrong_tenant, &provisioning_limits()),
            Err(SandboxContractError::InvalidCandidate)
        );
        let mut wrong_job = first.clone();
        wrong_job.metadata.job_id = id(ResourceKind::Job, 82);
        assert_eq!(
            evidence.observe_candidate(&request, &wrong_job, &provisioning_limits()),
            Err(SandboxContractError::InvalidCandidate)
        );
        let mut wrong_attempt = first.clone();
        wrong_attempt.metadata.physical_attempt += 1;
        assert_eq!(
            evidence.observe_candidate(&request, &wrong_attempt, &provisioning_limits()),
            Err(SandboxContractError::InvalidCandidate)
        );
        let mut unauthorized_ordinal = first.clone();
        unauthorized_ordinal.metadata.create_ordinal = 3;
        assert_eq!(
            evidence.observe_candidate(&request, &unauthorized_ordinal, &provisioning_limits()),
            Err(SandboxContractError::InvalidCandidate)
        );
        let retained = SandboxOrphanDecisionV1::new(
            first.clone(),
            SandboxOrphanDispositionV1::RetainProvisioning,
        )
        .unwrap();
        assert!(!retained.disposition.may_delete());
        let deleted = SandboxOrphanDecisionV1::new(
            second.clone(),
            SandboxOrphanDispositionV1::DeleteLateCandidate,
        )
        .unwrap();
        assert!(deleted.disposition.may_delete());
        let evidence = evidence
            .observe_candidate(&request, &first, &provisioning_limits())
            .unwrap()
            .observe_candidate(&request, &second, &provisioning_limits())
            .unwrap();
        assert_eq!(
            evidence
                .observe_candidate(&request, &second, &provisioning_limits())
                .unwrap(),
            evidence
        );
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
            selected.observe_candidate(&request, &third, &provisioning_limits()),
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
        let now = request.deadline_at - Duration::minutes(4);
        let evidence = SandboxPhysicalEvidenceV1::begin(&request, activation_token.clone(), now)
            .unwrap()
            .authorize_candidate_create(&request, &provisioning_limits(), now, 1)
            .unwrap()
            .into_inner()
            .observe_candidate(&request, &candidate, &provisioning_limits())
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
    fn durable_payload_tracks_job_authority_and_cleanup_without_reactivation() {
        let request = request(1);
        let now = request.deadline_at - Duration::minutes(4);
        let ready = ready_job(&request);
        let plan = SandboxExecutionPlanV1::from_request(&request).unwrap();
        let payload = SandboxDispatcherJobPayloadV1::accepted(plan).unwrap();
        let payload_json = serde_json::to_string(&payload).unwrap();
        assert!(!payload_json.contains("question"));
        assert!(!payload_json.contains("answer"));
        payload.validate_for(&ready).unwrap();

        let worker = id(ResourceKind::WorkerProcessGeneration, 20);
        let lease_digest = digest('7');
        let leased = decide_claim(
            &ready,
            now,
            worker.clone(),
            lease_digest.clone(),
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
        )
        .unwrap();
        payload.validate_for(&leased).unwrap();
        let lease = leased.lease.as_ref().unwrap();
        let fence = JobFence {
            expected_version: leased.version,
            worker_process_generation_id: worker.clone(),
            lease_generation: lease.lease_generation,
            token_digest: lease_digest,
        };
        let running = decide_start(&leased, &fence, now + Duration::seconds(1)).unwrap();
        let activation_token = OpaqueActivationToken::parse("9".repeat(64)).unwrap();
        let request = payload
            .plan
            .bind_request(
                running.lease_generation,
                1,
                worker.clone(),
                running.trace,
                request.input.clone(),
            )
            .unwrap();
        let payload = payload.begin(&request, activation_token, now).unwrap();
        payload.validate_for(&running).unwrap();
        assert_eq!(
            decide_recovery(&payload).unwrap(),
            SandboxRecoveryDecisionV1::ProvisionOrDiscover
        );

        let candidate = candidate(&request, "sandbox-one");
        let payload = payload
            .authorize_candidate_create(&request, &provisioning_limits(), now, 1)
            .unwrap()
            .into_inner()
            .record_observation(
                &request,
                SandboxDurableObservationV1::Candidate {
                    candidate: candidate.clone(),
                    limits: provisioning_limits(),
                },
            )
            .unwrap();
        let selected = payload
            .select_candidate(&request, &candidate)
            .unwrap()
            .into_inner();
        assert_eq!(
            decide_recovery(&selected).unwrap(),
            SandboxRecoveryDecisionV1::ObserveSelected
        );

        let boot_id = RunnerBootId::parse("boot-one").unwrap();
        let authorized = selected
            .authorize_activation(&candidate.sandbox_id, boot_id.clone())
            .unwrap()
            .into_inner();
        assert_eq!(
            decide_recovery(&authorized).unwrap(),
            SandboxRecoveryDecisionV1::ReplayAuthorizedActivation
        );
        let started = authorized
            .record_observation(
                &request,
                SandboxDurableObservationV1::RunnerState {
                    frame: SandboxRunnerStateFrameV1 {
                        magic: String::new(),
                        schema_version: 1,
                        sandbox_id: candidate.sandbox_id.clone(),
                        boot_id: boot_id.clone(),
                        execution_request_digest: request.request_digest.clone(),
                        phase: SandboxRunnerPhaseV1::Started,
                        frame_digest: zero_digest(),
                    }
                    .seal()
                    .unwrap(),
                },
            )
            .unwrap();
        assert_eq!(
            decide_recovery(&started).unwrap(),
            SandboxRecoveryDecisionV1::ObservePotentiallyStarted
        );

        let result = SandboxRunnerResultFrameV1 {
            magic: String::new(),
            schema_version: 1,
            execution_request_digest: request.request_digest.clone(),
            boot_id,
            result: SandboxRunnerOutcomeV1::Succeeded {
                output: serde_json::json!({"answer":42}),
                output_schema_digest: request.output_schema_digest.clone(),
                output_digest: zero_digest(),
                declared_output_bytes: 0,
            },
            frame_digest: zero_digest(),
        }
        .seal()
        .unwrap();
        let observed = started
            .record_observation(
                &request,
                SandboxDurableObservationV1::Result {
                    frame: result.clone(),
                },
            )
            .unwrap();
        assert_eq!(
            decide_recovery(&observed).unwrap(),
            SandboxRecoveryDecisionV1::CommitTerminalEvidence
        );
        let terminal_payload = observed
            .terminal(
                &request,
                SandboxTerminalOutcomeV1::Succeeded {
                    result: Box::new(result),
                },
            )
            .unwrap();
        let terminal_json = serde_json::to_string(&terminal_payload).unwrap();
        assert!(!terminal_json.contains("question"));
        assert!(!terminal_json.contains("answer"));
        let terminal_fence = JobFence {
            expected_version: running.version,
            ..fence
        };
        let terminal_job = decide_terminal(
            &running,
            &terminal_fence,
            now + Duration::seconds(2),
            JobState::Succeeded,
        )
        .unwrap();
        terminal_payload.validate_for(&terminal_job).unwrap();
        assert_eq!(
            decide_recovery(&terminal_payload).unwrap(),
            SandboxRecoveryDecisionV1::Cleanup
        );

        let (claimed_cleanup_payload, cleanup_fence) = terminal_payload
            .claim_cleanup(
                &terminal_job,
                worker,
                now + Duration::seconds(3),
                30_000,
                terminal_job.version + 1,
            )
            .unwrap();
        let cleanup_job =
            decide_terminal_physical_update(&terminal_job, terminal_job.version).unwrap();
        claimed_cleanup_payload.validate_for(&cleanup_job).unwrap();
        let stale_cleanup_fence = SandboxCleanupFenceV1 {
            expected_job_version: cleanup_fence.expected_job_version - 1,
            ..cleanup_fence.clone()
        };
        assert_eq!(
            claimed_cleanup_payload.record_cleanup_observation(
                &cleanup_job,
                &stale_cleanup_fence,
                now + Duration::seconds(4),
                SandboxCleanupObservationV1::Absent {
                    sandbox_id: candidate.sandbox_id.clone(),
                    evidence_digest: digest('8'),
                },
                cleanup_job.version + 1,
            ),
            Err(SandboxContractError::InvalidCleanup)
        );
        let (absent, next_fence) = claimed_cleanup_payload
            .record_cleanup_observation(
                &cleanup_job,
                &cleanup_fence,
                now + Duration::seconds(4),
                SandboxCleanupObservationV1::Absent {
                    sandbox_id: candidate.sandbox_id,
                    evidence_digest: digest('8'),
                },
                cleanup_job.version + 1,
            )
            .unwrap();
        assert!(next_fence.is_none());
        let absent_job =
            decide_terminal_physical_update(&cleanup_job, cleanup_job.version).unwrap();
        absent.validate_for(&absent_job).unwrap();
        assert_eq!(
            decide_recovery(&absent).unwrap(),
            SandboxRecoveryDecisionV1::Complete
        );
    }

    #[test]
    fn cancellation_before_candidate_creation_is_a_valid_terminal() {
        let request = request(1);
        let now = request.deadline_at - Duration::minutes(4);
        let running = {
            let ready = ready_job(&request);
            let worker = id(ResourceKind::WorkerProcessGeneration, 21);
            let leased = decide_claim(
                &ready,
                now,
                worker.clone(),
                digest('6'),
                LeasePolicy {
                    requested_milliseconds: 30_000,
                    hard_maximum_milliseconds: 60_000,
                },
            )
            .unwrap();
            let lease = leased.lease.as_ref().unwrap();
            let fence = JobFence {
                expected_version: leased.version,
                worker_process_generation_id: worker,
                lease_generation: lease.lease_generation,
                token_digest: lease.token_digest.clone(),
            };
            decide_start(&leased, &fence, now + Duration::seconds(1)).unwrap()
        };
        let plan = SandboxExecutionPlanV1::from_request(&request).unwrap();
        let payload = SandboxDispatcherJobPayloadV1::accepted(plan)
            .unwrap()
            .begin(
                &request,
                OpaqueActivationToken::parse("5".repeat(64)).unwrap(),
                now + Duration::seconds(1),
            )
            .unwrap()
            .terminal(
                &request,
                SandboxTerminalOutcomeV1::Cancelled {
                    evidence_digest: digest('4'),
                },
            )
            .unwrap();
        let mut terminal_job = running;
        terminal_job.state = JobState::Cancelled;
        terminal_job.version += 1;
        terminal_job.lease = None;
        payload.validate_for(&terminal_job).unwrap();
        assert_eq!(
            decide_recovery(&payload).unwrap(),
            SandboxRecoveryDecisionV1::Complete
        );
    }

    #[test]
    fn activation_tokens_are_redacted_and_strict() {
        let token = OpaqueActivationToken::parse("a".repeat(64)).unwrap();
        assert!(!format!("{token:?}").contains(&"a".repeat(64)));
        let generated = OpaqueActivationToken::generate().unwrap();
        assert_eq!(generated.expose_for_protocol().len(), 64);
        assert_ne!(generated, token);
        assert!(OpaqueActivationToken::parse("A".repeat(64)).is_err());
        assert!(OpaqueActivationToken::parse("a".repeat(63)).is_err());
    }
}
