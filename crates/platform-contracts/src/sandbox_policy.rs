use crate::{
    canonical_digest, CodeTrustClass, ResourceContractError, ResourceId, ResourceKind,
    SandboxIsolationClass, SandboxRuntimeFamily, SecretPurpose, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_SANDBOX_POLICY_SET_ITEMS: usize = 128;
pub const MAX_SANDBOX_DESTINATIONS: usize = 64;
pub const MAX_SANDBOX_MEDIA_TYPE_BYTES: usize = 128;
pub const MAX_ARTIFACT_VERIFICATION_EVIDENCE_TTL_MILLISECONDS: u64 = 86_400_000;
pub const MAX_ARTIFACT_VERIFICATION_RETRY_BACKOFF_MILLISECONDS: u64 = 60_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxIsolationPolicyDocument {
    pub schema_version: u32,
    pub minimum_isolation: SandboxIsolationClass,
    pub allowed_runtime_families: Vec<SandboxRuntimeFamily>,
    pub allowed_trust_classes: Vec<CodeTrustClass>,
    pub fresh_jail_per_job: bool,
    pub deny_runtime_fallback: bool,
    pub deny_host_devices: bool,
}

impl SandboxIsolationPolicyDocument {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if self.schema_version != 1
            || self.allowed_runtime_families.is_empty()
            || self.allowed_runtime_families.len() > MAX_SANDBOX_POLICY_SET_ITEMS
            || self.allowed_trust_classes.is_empty()
            || self.allowed_trust_classes.len() > MAX_SANDBOX_POLICY_SET_ITEMS
            || !strictly_sorted_unique(&self.allowed_runtime_families)
            || !strictly_sorted_unique(&self.allowed_trust_classes)
            || !self.fresh_jail_per_job
            || !self.deny_runtime_fallback
            || !self.deny_host_devices
        {
            return Err(ResourceContractError::InvalidPolicyDocument);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ResourceContractError> {
        policy_digest(self, || self.validate())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxResourcePolicyDocument {
    pub schema_version: u32,
    pub maximum_cpu_millicores: u32,
    pub maximum_memory_mebibytes: u32,
    pub maximum_pids: u32,
    pub maximum_files: u32,
    pub maximum_io_bytes: u64,
    pub maximum_stdout_bytes: u64,
    pub maximum_stderr_bytes: u64,
    pub maximum_result_bytes: u64,
    pub maximum_artifact_output_bytes: u64,
    pub maximum_network_connections: u32,
    pub maximum_network_request_bytes: u64,
    pub maximum_network_response_bytes: u64,
    pub maximum_startup_milliseconds: u64,
    pub maximum_idle_milliseconds: u64,
    pub maximum_wall_milliseconds: u64,
    pub maximum_cleanup_milliseconds: u64,
    pub swap_disabled: bool,
}

impl SandboxResourcePolicyDocument {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if self.schema_version != 1
            || self.maximum_cpu_millicores == 0
            || self.maximum_memory_mebibytes == 0
            || self.maximum_pids == 0
            || self.maximum_files == 0
            || self.maximum_io_bytes == 0
            || self.maximum_result_bytes == 0
            || self.maximum_startup_milliseconds == 0
            || self.maximum_idle_milliseconds == 0
            || self.maximum_wall_milliseconds == 0
            || self.maximum_cleanup_milliseconds == 0
            || self.maximum_startup_milliseconds > self.maximum_wall_milliseconds
            || self.maximum_idle_milliseconds > self.maximum_wall_milliseconds
            || !self.swap_disabled
        {
            return Err(ResourceContractError::InvalidPolicyDocument);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ResourceContractError> {
        policy_digest(self, || self.validate())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxEgressMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCanonicalDestination {
    pub host: String,
    pub port: u16,
    pub path_prefix: String,
    pub methods: Vec<SandboxEgressMethod>,
}

impl SandboxCanonicalDestination {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        let canonical_host = self.host.to_ascii_lowercase();
        if self.host != canonical_host
            || self.host.is_empty()
            || self.host.len() > 253
            || !self.host.is_ascii()
            || self.host.parse::<std::net::IpAddr>().is_ok()
            || self.host.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
            })
            || self.port == 0
            || !self.path_prefix.starts_with('/')
            || self.path_prefix.len() > 1_024
            || self.path_prefix.contains("..")
            || self.path_prefix.chars().any(char::is_control)
            || self.methods.is_empty()
            || !strictly_sorted_unique(&self.methods)
        {
            return Err(ResourceContractError::InvalidPolicyDocument);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxNetworkPolicyDocument {
    pub schema_version: u32,
    pub destinations: Vec<SandboxCanonicalDestination>,
    pub maximum_redirects: u8,
    pub maximum_dns_answers: u16,
    pub maximum_response_bytes: u64,
    pub require_https: bool,
    pub require_tls12_or_newer: bool,
    pub deny_private_addresses: bool,
    pub deny_link_local_addresses: bool,
    pub deny_metadata_addresses: bool,
    pub deny_proxy_environment: bool,
    pub deny_connect_tunnel: bool,
    pub deny_listen: bool,
    pub deny_udp: bool,
}

impl SandboxNetworkPolicyDocument {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if self.schema_version != 1
            || self.destinations.len() > MAX_SANDBOX_DESTINATIONS
            || !strictly_sorted_unique(&self.destinations)
            || self
                .destinations
                .iter()
                .any(|value| value.validate().is_err())
            || self.maximum_redirects > 8
            || self.maximum_dns_answers == 0
            || self.maximum_dns_answers > 64
            || self.maximum_response_bytes == 0
            || !self.require_https
            || !self.require_tls12_or_newer
            || !self.deny_private_addresses
            || !self.deny_link_local_addresses
            || !self.deny_metadata_addresses
            || !self.deny_proxy_environment
            || !self.deny_connect_tunnel
            || !self.deny_listen
            || !self.deny_udp
        {
            return Err(ResourceContractError::InvalidPolicyDocument);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ResourceContractError> {
        policy_digest(self, || self.validate())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxArtifactIoPolicyDocument {
    pub schema_version: u32,
    pub allowed_input_media_types: Vec<String>,
    pub allowed_output_media_types: Vec<String>,
    pub maximum_input_artifacts: u32,
    pub maximum_output_artifacts: u32,
    pub scanner_contract_digest: Sha256Digest,
    pub verification_evidence_ttl_milliseconds: u64,
    pub verification_retry_backoff_milliseconds: u64,
    pub write_storage_binding_digest: Sha256Digest,
    pub encryption_domain_id: ResourceId,
    pub deny_symlink: bool,
    pub deny_hardlink: bool,
    pub deny_device: bool,
    pub deny_fifo: bool,
    pub deny_socket: bool,
    pub deny_sparse_file: bool,
    pub archive_expansion_disabled: bool,
}

impl SandboxArtifactIoPolicyDocument {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if self.schema_version != 3
            || self.allowed_input_media_types.len() > MAX_SANDBOX_POLICY_SET_ITEMS
            || self.allowed_output_media_types.len() > MAX_SANDBOX_POLICY_SET_ITEMS
            || !strictly_sorted_unique(&self.allowed_input_media_types)
            || !strictly_sorted_unique(&self.allowed_output_media_types)
            || self
                .allowed_input_media_types
                .iter()
                .chain(self.allowed_output_media_types.iter())
                .any(|value| !valid_media_type(value))
            || self.maximum_input_artifacts > 64
            || self.maximum_output_artifacts > 64
            || self.verification_evidence_ttl_milliseconds == 0
            || self.verification_evidence_ttl_milliseconds
                > MAX_ARTIFACT_VERIFICATION_EVIDENCE_TTL_MILLISECONDS
            || self.verification_retry_backoff_milliseconds == 0
            || self.verification_retry_backoff_milliseconds
                > MAX_ARTIFACT_VERIFICATION_RETRY_BACKOFF_MILLISECONDS
            || self.verification_retry_backoff_milliseconds
                >= self.verification_evidence_ttl_milliseconds
            || self.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
            || !self.deny_symlink
            || !self.deny_hardlink
            || !self.deny_device
            || !self.deny_fifo
            || !self.deny_socket
            || !self.deny_sparse_file
            || !self.archive_expansion_disabled
        {
            return Err(ResourceContractError::InvalidPolicyDocument);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ResourceContractError> {
        policy_digest(self, || self.validate())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxSecretDeliveryMode {
    BrokeredMemory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxSecretResolutionPolicyDocument {
    pub schema_version: u32,
    pub allowed_purposes: Vec<SecretPurpose>,
    pub maximum_reads_per_binding: u32,
    pub delivery_mode: SandboxSecretDeliveryMode,
    pub deny_environment: bool,
    pub deny_argv: bool,
    pub deny_snapshot: bool,
    pub scrub_after_read: bool,
}

impl SandboxSecretResolutionPolicyDocument {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if self.schema_version != 1
            || self.allowed_purposes.is_empty()
            || self.allowed_purposes.len() > 32
            || !strictly_sorted_unique(&self.allowed_purposes)
            || self.maximum_reads_per_binding == 0
            || self.maximum_reads_per_binding > 32
            || !self.deny_environment
            || !self.deny_argv
            || !self.deny_snapshot
            || !self.scrub_after_read
        {
            return Err(ResourceContractError::InvalidPolicyDocument);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ResourceContractError> {
        policy_digest(self, || self.validate())
    }
}

fn policy_digest<T: Serialize>(
    value: &T,
    validate: impl FnOnce() -> Result<(), ResourceContractError>,
) -> Result<Sha256Digest, ResourceContractError> {
    validate()?;
    canonical_digest(
        &serde_json::to_value(value).map_err(|_| ResourceContractError::Canonicalization)?,
    )
    .map_err(|_| ResourceContractError::Canonicalization)?
    .parse()
    .map_err(|_| ResourceContractError::Canonicalization)
}

fn strictly_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
}

fn valid_media_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SANDBOX_MEDIA_TYPE_BYTES
        && value.is_ascii()
        && value.split_once('/').is_some_and(|(kind, subtype)| {
            !kind.is_empty()
                && !subtype.is_empty()
                && kind.bytes().chain(subtype.bytes()).all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(
                            byte,
                            b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
                        )
                })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn artifact_io_policy() -> SandboxArtifactIoPolicyDocument {
        SandboxArtifactIoPolicyDocument {
            schema_version: 3,
            allowed_input_media_types: vec!["application/octet-stream".to_owned()],
            allowed_output_media_types: vec![],
            maximum_input_artifacts: 8,
            maximum_output_artifacts: 0,
            scanner_contract_digest: digest('8'),
            verification_evidence_ttl_milliseconds: 60_000,
            verification_retry_backoff_milliseconds: 1_000,
            write_storage_binding_digest: digest('9'),
            encryption_domain_id: ResourceId::from_uuid_v7(
                ResourceKind::EncryptionDomain,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            deny_symlink: true,
            deny_hardlink: true,
            deny_device: true,
            deny_fifo: true,
            deny_socket: true,
            deny_sparse_file: true,
            archive_expansion_disabled: true,
        }
    }

    #[test]
    fn artifact_io_v3_freezes_verification_and_storage_policy() {
        let policy = artifact_io_policy();
        assert!(policy.validate().is_ok());
        assert_eq!(policy.canonical_digest(), policy.canonical_digest());
    }

    #[test]
    fn artifact_io_legacy_and_unbounded_verification_are_rejected() {
        let mut legacy = artifact_io_policy();
        legacy.schema_version = 1;
        assert_eq!(
            legacy.validate(),
            Err(ResourceContractError::InvalidPolicyDocument)
        );

        let mut unbounded = artifact_io_policy();
        unbounded.verification_evidence_ttl_milliseconds =
            MAX_ARTIFACT_VERIFICATION_EVIDENCE_TTL_MILLISECONDS + 1;
        assert_eq!(
            unbounded.validate(),
            Err(ResourceContractError::InvalidPolicyDocument)
        );
    }

    #[test]
    fn artifact_io_retry_must_fit_inside_evidence_lifetime() {
        let mut policy = artifact_io_policy();
        policy.verification_retry_backoff_milliseconds =
            policy.verification_evidence_ttl_milliseconds;
        assert_eq!(
            policy.validate(),
            Err(ResourceContractError::InvalidPolicyDocument)
        );
    }
}
