use crate::Sha256Digest;
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxProviderKind {
    OpenSandboxKubernetes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    pub fn validate(&self) -> Result<(), SandboxPlatformContractError> {
        if !(1..=MAX_SANDBOX_CANDIDATES).contains(&self.maximum_candidates)
            || !(1..=MAX_SANDBOX_CANDIDATE_PAGE_ITEMS).contains(&self.candidate_page_items)
            || !(100..=5_000).contains(&self.candidate_quiescence_milliseconds)
            || !(1_000..=120_000).contains(&self.provisioning_timeout_milliseconds)
            || self.candidate_quiescence_milliseconds >= self.provisioning_timeout_milliseconds
            || !(1..=MAX_SANDBOX_ORPHAN_PAGE_ITEMS).contains(&self.orphan_page_items)
            || !(1_024..=MAX_SANDBOX_RUNNER_HEADER_BYTES).contains(&self.runner_header_bytes)
            || !(1_024..=MAX_SANDBOX_DIAGNOSTIC_BYTES).contains(&self.diagnostic_bytes)
        {
            return Err(SandboxPlatformContractError::InvalidLimits);
        }
        Ok(())
    }

    pub fn bounded_by(&self, maximum: &Self) -> bool {
        self.maximum_candidates <= maximum.maximum_candidates
            && self.candidate_page_items <= maximum.candidate_page_items
            && self.candidate_quiescence_milliseconds <= maximum.candidate_quiescence_milliseconds
            && self.provisioning_timeout_milliseconds <= maximum.provisioning_timeout_milliseconds
            && self.orphan_page_items <= maximum.orphan_page_items
            && self.runner_header_bytes <= maximum.runner_header_bytes
            && self.diagnostic_bytes <= maximum.diagnostic_bytes
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
    pub fn validate(&self) -> Result<(), SandboxPlatformContractError> {
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
            return Err(SandboxPlatformContractError::InvalidLimits);
        }
        Ok(())
    }

    pub fn bounded_by(&self, maximum: &Self) -> bool {
        self.maximum_input_bytes <= maximum.maximum_input_bytes
            && self.maximum_output_bytes <= maximum.maximum_output_bytes
            && self.cpu_millicores <= maximum.cpu_millicores
            && self.memory_mebibytes <= maximum.memory_mebibytes
            && self.pids <= maximum.pids
            && self.ephemeral_storage_bytes <= maximum.ephemeral_storage_bytes
            && self.wall_milliseconds <= maximum.wall_milliseconds
            && self.cleanup_milliseconds <= maximum.cleanup_milliseconds
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
    pub fn validate(&self) -> Result<(), SandboxPlatformContractError> {
        if self.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
            || self.provider != SandboxProviderKind::OpenSandboxKubernetes
        {
            return Err(SandboxPlatformContractError::InvalidRuntime);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, SandboxPlatformContractError> {
        self.validate()?;
        crate::canonical_digest(
            &serde_json::to_value(self).map_err(|_| SandboxPlatformContractError::Canonical)?,
        )
        .map_err(|_| SandboxPlatformContractError::Canonical)?
        .parse()
        .map_err(|_| SandboxPlatformContractError::Canonical)
    }
}

pub fn valid_oci_digest_uri(value: &str) -> bool {
    let Some((repository, digest)) = value.rsplit_once("@sha256:") else {
        return false;
    };
    !repository.is_empty()
        && repository.len() <= 512
        && repository.is_ascii()
        && !repository.chars().any(char::is_whitespace)
        && digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn valid_absolute_argv(argv: &[String]) -> bool {
    !argv.is_empty()
        && argv.len() <= MAX_SANDBOX_ARGV_ITEMS
        && argv.first().is_some_and(|program| program.starts_with('/'))
        && argv.iter().all(|item| {
            !item.is_empty() && item.len() <= MAX_SANDBOX_ARG_BYTES && !item.contains('\0')
        })
        && argv
            .iter()
            .map(String::len)
            .try_fold(0usize, usize::checked_add)
            .is_some_and(|bytes| bytes <= MAX_SANDBOX_ARGV_BYTES)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxPlatformContractError {
    Canonical,
    InvalidRuntime,
    InvalidLimits,
}

impl fmt::Display for SandboxPlatformContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Canonical => "sandbox contract canonicalization failed",
            Self::InvalidRuntime => "sandbox runtime contract is invalid",
            Self::InvalidLimits => "sandbox limits are invalid",
        })
    }
}

impl Error for SandboxPlatformContractError {}
