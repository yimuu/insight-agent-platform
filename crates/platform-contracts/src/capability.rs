use crate::{
    canonical_digest, valid_absolute_argv, valid_oci_digest_uri, ArtifactRef,
    CapabilityBackendFeatures, CapabilityBackendKind, DataClassification, DataRegion,
    ExactDeploymentRef, ExactVersionRef, ResourceId, ResourceKind, SecretPurpose, Sha256Digest,
    MAX_ARTIFACT_BYTES, WORKER_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, fmt, str::FromStr};

pub const MAX_CAPABILITY_CREDENTIAL_REQUIREMENTS: usize = 16;
pub const MAX_CAPABILITY_QUALIFIED_NAME_BYTES: usize = 192;
pub const MAX_CAPABILITY_ADAPTER_NAME_BYTES: usize = 192;
pub const MAX_CAPABILITY_ENTRYPOINT_BYTES: usize = 256;
pub const MAX_CAPABILITY_REMOTE_NAME_BYTES: usize = 256;
pub const MAX_CAPABILITY_ENDPOINT_HOST_BYTES: usize = 253;
pub const MAX_CAPABILITY_ENDPOINT_PATH_BYTES: usize = 2_048;
pub const MAX_CAPABILITY_REQUEST_BYTES: u32 = 16 * 1_048_576;
pub const MAX_CAPABILITY_RESPONSE_BYTES: u32 = 64 * 1_048_576;
pub const MAX_CAPABILITY_DIAGNOSTIC_BYTES: u32 = 4 * 1_048_576;
pub const MAX_CAPABILITY_TIMEOUT_MILLISECONDS: u64 = 3_600_000;
pub const MAX_CAPABILITY_ARTIFACT_PORTS: usize = 64;
pub const MAX_CAPABILITY_MEDIA_PATTERNS: usize = 64;
pub const MAX_CAPABILITY_INTERFACE_ARTIFACTS: u16 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityArtifactDirection {
    Input,
    Output,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityArtifactPort {
    pub name: String,
    pub direction: CapabilityArtifactDirection,
    pub media_types: Vec<String>,
    pub maximum_count: u16,
    pub maximum_single_bytes: u64,
    pub maximum_total_bytes: u64,
    pub maximum_classification: DataClassification,
}

impl CapabilityArtifactPort {
    pub fn validate(&self) -> Result<(), CapabilityContractError> {
        if !valid_port_name(&self.name)
            || self.media_types.is_empty()
            || self.media_types.len() > MAX_CAPABILITY_MEDIA_PATTERNS
            || !is_sorted_unique(&self.media_types)
            || self
                .media_types
                .iter()
                .any(|pattern| !valid_media_type_pattern(pattern))
            || self.maximum_count == 0
            || self.maximum_single_bytes == 0
            || self.maximum_single_bytes > MAX_ARTIFACT_BYTES
            || self.maximum_total_bytes < self.maximum_single_bytes
            || self.maximum_total_bytes
                > self
                    .maximum_single_bytes
                    .saturating_mul(u64::from(self.maximum_count))
        {
            return Err(CapabilityContractError::InvalidArtifactContract);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityArtifactContract {
    pub ports: Vec<CapabilityArtifactPort>,
}

impl CapabilityArtifactContract {
    pub fn validate(&self) -> Result<(), CapabilityContractError> {
        if self.ports.len() > MAX_CAPABILITY_ARTIFACT_PORTS {
            return Err(CapabilityContractError::InvalidArtifactContract);
        }
        let mut previous: Option<(CapabilityArtifactDirection, &str)> = None;
        for port in &self.ports {
            port.validate()?;
            let current = (port.direction, port.name.as_str());
            if previous.is_some_and(|value| value >= current) {
                return Err(CapabilityContractError::InvalidArtifactContract);
            }
            previous = Some(current);
        }
        Ok(())
    }

    pub fn maximum_artifact_count(&self) -> Result<u16, CapabilityContractError> {
        self.ports.iter().try_fold(0_u16, |total, port| {
            total
                .checked_add(port.maximum_count)
                .ok_or(CapabilityContractError::InvalidArtifactContract)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityDataFlowPolicy {
    pub maximum_input_classification: DataClassification,
    pub maximum_output_classification: DataClassification,
    pub allowed_regions: Vec<DataRegion>,
    pub declassification_policy: Option<ExactVersionRef>,
}

impl CapabilityDataFlowPolicy {
    pub fn validate(&self) -> Result<(), CapabilityContractError> {
        if self.allowed_regions.is_empty()
            || self.allowed_regions.len() > 32
            || !is_sorted_unique(&self.allowed_regions)
            || self.maximum_output_classification.rank() < self.maximum_input_classification.rank()
                && self.declassification_policy.is_none()
            || self.declassification_policy.as_ref().is_some_and(|policy| {
                policy.resource_kind != ResourceKind::PolicyRevision || policy.validate().is_err()
            })
        {
            return Err(CapabilityContractError::InvalidDataPolicy);
        }
        Ok(())
    }

    pub const fn permits_input(&self, classification: DataClassification) -> bool {
        classification.rank() <= self.maximum_input_classification.rank()
    }

    pub const fn permits_output(
        &self,
        input: DataClassification,
        output: DataClassification,
    ) -> bool {
        output.rank() <= self.maximum_output_classification.rank()
            && (output.rank() >= input.rank() || self.declassification_policy.is_some())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityInterfaceLimits {
    pub maximum_input_bytes: u32,
    pub maximum_output_bytes: u32,
    pub maximum_artifacts: u16,
    pub maximum_execution_milliseconds: u64,
}

impl CapabilityInterfaceLimits {
    pub fn validate(&self) -> Result<(), CapabilityContractError> {
        if self.maximum_input_bytes == 0
            || self.maximum_input_bytes > MAX_CAPABILITY_REQUEST_BYTES
            || self.maximum_output_bytes == 0
            || self.maximum_output_bytes > MAX_CAPABILITY_RESPONSE_BYTES
            || self.maximum_artifacts > MAX_CAPABILITY_INTERFACE_ARTIFACTS
            || self.maximum_execution_milliseconds == 0
            || self.maximum_execution_milliseconds > MAX_CAPABILITY_TIMEOUT_MILLISECONDS
        {
            return Err(CapabilityContractError::InvalidLimits);
        }
        Ok(())
    }
}

/// Stable authoring and discovery name for a Capability Interface.
///
/// Runtime routing never resolves this name: a Run freezes an exact Deployment and Interface
/// revision. Keeping the name in the immutable Interface contract lets audit and composed domain
/// contracts (notably Text2SQL) prove what was selected without introducing a name registry as a
/// second execution authority.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CapabilityName(String);

impl CapabilityName {
    pub fn new(value: impl Into<String>) -> Result<Self, CapabilityContractError> {
        let value = value.into();
        if !valid_capability_name(&value) {
            return Err(CapabilityContractError::InvalidName);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CapabilityName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for CapabilityName {
    type Err = CapabilityContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityEndpointScheme {
    Http,
    Https,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalHttpEndpoint {
    pub scheme: CapabilityEndpointScheme,
    pub host: String,
    pub port: u16,
    pub base_path: String,
}

impl CanonicalHttpEndpoint {
    pub fn validate(&self) -> Result<(), CapabilityContractError> {
        if !valid_endpoint_host(&self.host)
            || self.port == 0
            || !valid_endpoint_path(&self.base_path)
        {
            return Err(CapabilityContractError::InvalidEndpoint);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, CapabilityContractError> {
        self.validate()?;
        let value =
            serde_json::to_value(self).map_err(|_| CapabilityContractError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| CapabilityContractError::Canonicalization)?
            .parse()
            .map_err(|_| CapabilityContractError::Canonicalization)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityBackendLimits {
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
    pub maximum_diagnostic_bytes: u32,
    pub connect_timeout_milliseconds: u64,
    pub first_byte_timeout_milliseconds: u64,
    pub idle_timeout_milliseconds: u64,
    pub total_timeout_milliseconds: u64,
}

impl CapabilityBackendLimits {
    pub fn validate(&self) -> Result<(), CapabilityContractError> {
        if self.maximum_request_bytes == 0
            || self.maximum_request_bytes > MAX_CAPABILITY_REQUEST_BYTES
            || self.maximum_response_bytes == 0
            || self.maximum_response_bytes > MAX_CAPABILITY_RESPONSE_BYTES
            || self.maximum_diagnostic_bytes > MAX_CAPABILITY_DIAGNOSTIC_BYTES
            || self.connect_timeout_milliseconds == 0
            || self.first_byte_timeout_milliseconds == 0
            || self.idle_timeout_milliseconds == 0
            || self.total_timeout_milliseconds == 0
            || self.total_timeout_milliseconds > MAX_CAPABILITY_TIMEOUT_MILLISECONDS
            || self.connect_timeout_milliseconds >= self.total_timeout_milliseconds
            || self.first_byte_timeout_milliseconds >= self.total_timeout_milliseconds
            || self.idle_timeout_milliseconds >= self.total_timeout_milliseconds
        {
            return Err(CapabilityContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCapabilityContract {
    pub adapter_id: String,
    pub adapter_version: String,
    pub module_digest: Sha256Digest,
    pub entrypoint_id: String,
    pub worker_protocol_version: u32,
}

impl NativeCapabilityContract {
    fn validate(&self) -> Result<(), CapabilityContractError> {
        if !valid_qualified_name(&self.adapter_id, MAX_CAPABILITY_ADAPTER_NAME_BYTES)
            || !valid_stable_value(&self.adapter_version, MAX_CAPABILITY_ADAPTER_NAME_BYTES)
            || !valid_qualified_name(&self.entrypoint_id, MAX_CAPABILITY_ENTRYPOINT_BYTES)
            || self.worker_protocol_version != WORKER_PROTOCOL_VERSION
        {
            return Err(CapabilityContractError::InvalidBackend);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpCapabilityContract {
    pub method: HttpCapabilityMethod,
    pub protocol_contract_digest: Sha256Digest,
    pub request_mapping_digest: Sha256Digest,
    pub response_mapping_digest: Sha256Digest,
    pub error_mapping_digest: Sha256Digest,
    pub idempotency_header: Option<String>,
}

impl HttpCapabilityContract {
    fn validate(&self) -> Result<(), CapabilityContractError> {
        if self.idempotency_header.as_deref().is_some_and(|header| {
            header != header.to_ascii_lowercase()
                || !valid_http_header_name(header)
                || is_sensitive_http_header(header)
        }) {
            return Err(CapabilityContractError::InvalidBackend);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpCapabilityMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpCapabilityMethod {
    pub const fn as_http_token(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcCapabilityContract {
    pub protobuf_contract_digest: Sha256Digest,
    pub service_name: String,
    pub method_name: String,
    pub request_mapping_digest: Sha256Digest,
    pub response_mapping_digest: Sha256Digest,
    pub error_mapping_digest: Sha256Digest,
    pub idempotency_metadata_key: Option<String>,
}

impl GrpcCapabilityContract {
    fn validate(&self) -> Result<(), CapabilityContractError> {
        if !valid_qualified_name(&self.service_name, MAX_CAPABILITY_REMOTE_NAME_BYTES)
            || !valid_qualified_name(&self.method_name, MAX_CAPABILITY_REMOTE_NAME_BYTES)
            || self.idempotency_metadata_key.as_deref().is_some_and(|key| {
                key != key.to_ascii_lowercase()
                    || !valid_grpc_metadata_key(key)
                    || is_sensitive_http_header(key)
            })
        {
            return Err(CapabilityContractError::InvalidBackend);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpToolCapabilityContract {
    pub remote_tool_name: String,
    pub remote_input_schema_digest: Sha256Digest,
    pub output_mapping_digest: Sha256Digest,
    pub protocol_profile: ExactVersionRef,
    pub discovery_semantic_evidence_digest: Sha256Digest,
    pub supports_task: bool,
    pub supports_progress: bool,
}

pub const INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledCapabilityCodecRef {
    pub schema_version: u32,
    pub backend_kind: CapabilityBackendKind,
    pub codec_id: String,
    pub codec_version: String,
    pub module_digest: Sha256Digest,
    pub worker_protocol_version: u32,
    pub descriptor_digest: Sha256Digest,
}

impl InstalledCapabilityCodecRef {
    pub fn validate_for(
        &self,
        contract: &CapabilityBackendContract,
    ) -> Result<(), CapabilityContractError> {
        if self.schema_version != INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION
            || !matches!(
                self.backend_kind,
                CapabilityBackendKind::Http
                    | CapabilityBackendKind::Grpc
                    | CapabilityBackendKind::Mcp
            )
            || self.backend_kind != contract.kind()
            || !valid_qualified_name(&self.codec_id, MAX_CAPABILITY_ADAPTER_NAME_BYTES)
            || !valid_stable_value(&self.codec_version, MAX_CAPABILITY_ADAPTER_NAME_BYTES)
            || self.worker_protocol_version != WORKER_PROTOCOL_VERSION
            || self.descriptor_digest != contract.descriptor_digest()?
        {
            return Err(CapabilityContractError::InvalidBackend);
        }
        Ok(())
    }
}

impl McpToolCapabilityContract {
    fn validate(&self) -> Result<(), CapabilityContractError> {
        self.protocol_profile
            .validate()
            .map_err(|_| CapabilityContractError::InvalidBackend)?;
        if !valid_remote_name(&self.remote_tool_name, MAX_CAPABILITY_REMOTE_NAME_BYTES)
            || self.protocol_profile.resource_kind != ResourceKind::PolicyRevision
        {
            return Err(CapabilityContractError::InvalidBackend);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCapabilityContract {
    pub package_contract_digest: Sha256Digest,
    pub image_uri: String,
    pub package_argv: Vec<String>,
    pub dependency_lock_digest: Sha256Digest,
    pub runtime_contract_digest: Sha256Digest,
    pub input_mapping_digest: Sha256Digest,
    pub output_mapping_digest: Sha256Digest,
}

impl SandboxCapabilityContract {
    fn validate(&self) -> Result<(), CapabilityContractError> {
        if !valid_oci_digest_uri(&self.image_uri) || !valid_absolute_argv(&self.package_argv) {
            return Err(CapabilityContractError::InvalidBackend);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "contract",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CapabilityBackendContract {
    Native(NativeCapabilityContract),
    Http(HttpCapabilityContract),
    Grpc(GrpcCapabilityContract),
    Mcp(McpToolCapabilityContract),
    Sandbox(SandboxCapabilityContract),
}

impl CapabilityBackendContract {
    pub const fn kind(&self) -> CapabilityBackendKind {
        match self {
            Self::Native(_) => CapabilityBackendKind::Native,
            Self::Http(_) => CapabilityBackendKind::Http,
            Self::Grpc(_) => CapabilityBackendKind::Grpc,
            Self::Mcp(_) => CapabilityBackendKind::Mcp,
            Self::Sandbox(_) => CapabilityBackendKind::Sandbox,
        }
    }

    pub fn descriptor_digest(&self) -> Result<Sha256Digest, CapabilityContractError> {
        if !matches!(self, Self::Http(_) | Self::Grpc(_) | Self::Mcp(_)) {
            return Err(CapabilityContractError::InvalidBackend);
        }
        let value = serde_json::json!({
            "contract": self,
            "domain": "insight.platform/v1/installed-capability-codec-descriptor",
            "schema_version": INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION,
        });
        canonical_digest(&value)
            .map_err(|_| CapabilityContractError::Canonicalization)?
            .parse()
            .map_err(|_| CapabilityContractError::Canonicalization)
    }

    pub fn validate(
        &self,
        features: &CapabilityBackendFeatures,
    ) -> Result<(), CapabilityContractError> {
        features
            .validate()
            .map_err(|_| CapabilityContractError::InvalidFeatures)?;
        match self {
            Self::Native(contract) => contract.validate(),
            Self::Http(contract) => contract.validate(),
            Self::Grpc(contract) => contract.validate(),
            Self::Mcp(contract) => {
                contract.validate()?;
                if features.deferred && !contract.supports_task
                    || features.progress && !contract.supports_progress
                {
                    return Err(CapabilityContractError::InvalidFeatures);
                }
                Ok(())
            }
            Self::Sandbox(contract) => {
                contract.validate()?;
                if !features.deferred {
                    return Err(CapabilityContractError::InvalidFeatures);
                }
                Ok(())
            }
        }
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, CapabilityContractError> {
        let value =
            serde_json::to_value(self).map_err(|_| CapabilityContractError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| CapabilityContractError::Canonicalization)?
            .parse()
            .map_err(|_| CapabilityContractError::Canonicalization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "binding",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CapabilityBackendBinding {
    Native {
        worker_manifest_digest: Sha256Digest,
        adapter_module_digest: Sha256Digest,
    },
    Http {
        codec: InstalledCapabilityCodecRef,
        worker_manifest_digest: Sha256Digest,
        endpoint: CanonicalHttpEndpoint,
        endpoint_identity_digest: Sha256Digest,
        network_policy: ExactVersionRef,
        tls_policy: ExactVersionRef,
        trust_policy: ExactVersionRef,
    },
    Grpc {
        codec: InstalledCapabilityCodecRef,
        worker_manifest_digest: Sha256Digest,
        endpoint: CanonicalHttpEndpoint,
        endpoint_identity_digest: Sha256Digest,
        network_policy: ExactVersionRef,
        tls_policy: ExactVersionRef,
        trust_policy: ExactVersionRef,
    },
    Mcp {
        codec: InstalledCapabilityCodecRef,
        worker_manifest_digest: Sha256Digest,
        mcp_deployment: ExactDeploymentRef,
        discovery_snapshot_id: ResourceId,
        discovery_snapshot_digest: Sha256Digest,
        authorization_policy: ExactVersionRef,
    },
    Sandbox {
        runtime: ExactVersionRef,
        package: ExactVersionRef,
        profile: crate::ExactSandboxProfileBinding,
    },
}

impl CapabilityBackendBinding {
    pub const fn kind(&self) -> CapabilityBackendKind {
        match self {
            Self::Native { .. } => CapabilityBackendKind::Native,
            Self::Http { .. } => CapabilityBackendKind::Http,
            Self::Grpc { .. } => CapabilityBackendKind::Grpc,
            Self::Mcp { .. } => CapabilityBackendKind::Mcp,
            Self::Sandbox { .. } => CapabilityBackendKind::Sandbox,
        }
    }

    pub fn required_worker_manifest_digest(&self) -> Option<&Sha256Digest> {
        match self {
            Self::Native {
                worker_manifest_digest,
                ..
            }
            | Self::Http {
                worker_manifest_digest,
                ..
            }
            | Self::Grpc {
                worker_manifest_digest,
                ..
            }
            | Self::Mcp {
                worker_manifest_digest,
                ..
            } => Some(worker_manifest_digest),
            Self::Sandbox { .. } => None,
        }
    }

    pub fn validate(&self) -> Result<(), CapabilityContractError> {
        match self {
            Self::Native { .. } => Ok(()),
            Self::Http {
                codec,
                endpoint,
                endpoint_identity_digest,
                network_policy,
                tls_policy,
                trust_policy,
                ..
            }
            | Self::Grpc {
                codec,
                endpoint,
                endpoint_identity_digest,
                network_policy,
                tls_policy,
                trust_policy,
                ..
            } => {
                if codec.backend_kind != self.kind()
                    || endpoint.canonical_digest().as_ref() != Ok(endpoint_identity_digest)
                {
                    return Err(CapabilityContractError::InvalidBinding);
                }
                validate_distinct_policy_refs(&[network_policy, tls_policy, trust_policy])
            }
            Self::Mcp {
                codec,
                mcp_deployment,
                discovery_snapshot_id,
                authorization_policy,
                ..
            } => {
                mcp_deployment
                    .validate()
                    .map_err(|_| CapabilityContractError::InvalidBinding)?;
                authorization_policy
                    .validate()
                    .map_err(|_| CapabilityContractError::InvalidBinding)?;
                if codec.backend_kind != CapabilityBackendKind::Mcp
                    || mcp_deployment.resource_kind != ResourceKind::McpDeployment
                    || discovery_snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
                    || authorization_policy.resource_kind != ResourceKind::PolicyRevision
                {
                    return Err(CapabilityContractError::InvalidBinding);
                }
                Ok(())
            }
            Self::Sandbox {
                runtime,
                package,
                profile,
            } => {
                for (reference, kind) in [
                    (runtime, ResourceKind::SandboxRuntimeRevision),
                    (package, ResourceKind::SandboxPackageRevision),
                ] {
                    reference
                        .validate()
                        .map_err(|_| CapabilityContractError::InvalidBinding)?;
                    if reference.resource_kind != kind {
                        return Err(CapabilityContractError::InvalidBinding);
                    }
                }
                profile
                    .validate()
                    .map_err(|_| CapabilityContractError::InvalidBinding)?;
                Ok(())
            }
        }
    }

    pub fn validate_for(
        &self,
        contract: &CapabilityBackendContract,
    ) -> Result<(), CapabilityContractError> {
        self.validate()?;
        if self.kind() != contract.kind() {
            return Err(CapabilityContractError::InvalidBinding);
        }
        match (self, contract) {
            (Self::Http { codec, .. }, CapabilityBackendContract::Http(_))
            | (Self::Grpc { codec, .. }, CapabilityBackendContract::Grpc(_))
            | (Self::Mcp { codec, .. }, CapabilityBackendContract::Mcp(_)) => {
                codec.validate_for(contract)
            }
            (
                Self::Native {
                    adapter_module_digest,
                    ..
                },
                CapabilityBackendContract::Native(contract),
            ) if adapter_module_digest != &contract.module_digest => {
                Err(CapabilityContractError::InvalidBinding)
            }
            _ => Ok(()),
        }
    }

    pub fn exact_version_refs(&self) -> Vec<&ExactVersionRef> {
        match self {
            Self::Native { .. } => Vec::new(),
            Self::Http {
                network_policy,
                tls_policy,
                trust_policy,
                ..
            }
            | Self::Grpc {
                network_policy,
                tls_policy,
                trust_policy,
                ..
            } => vec![network_policy, tls_policy, trust_policy],
            Self::Mcp {
                authorization_policy,
                ..
            } => vec![authorization_policy],
            Self::Sandbox {
                runtime,
                package,
                profile,
            } => vec![runtime, package, &profile.revision],
        }
    }

    pub fn exact_deployment_refs(&self) -> Vec<&ExactDeploymentRef> {
        match self {
            Self::Mcp { mcp_deployment, .. } => vec![mcp_deployment],
            Self::Sandbox { profile, .. } => vec![&profile.deployment],
            _ => Vec::new(),
        }
    }
}

pub fn validate_capability_credential_requirements(
    requirements: &[SecretPurpose],
) -> Result<(), CapabilityContractError> {
    if requirements.len() > MAX_CAPABILITY_CREDENTIAL_REQUIREMENTS {
        return Err(CapabilityContractError::InvalidCredentials);
    }
    let mut previous = None;
    for requirement in requirements {
        if previous.is_some_and(|value: &SecretPurpose| value >= requirement) {
            return Err(CapabilityContractError::InvalidCredentials);
        }
        previous = Some(requirement);
    }
    Ok(())
}

fn validate_distinct_policy_refs(
    policies: &[&ExactVersionRef],
) -> Result<(), CapabilityContractError> {
    let mut identities = BTreeSet::new();
    for policy in policies {
        policy
            .validate()
            .map_err(|_| CapabilityContractError::InvalidBinding)?;
        if policy.resource_kind != ResourceKind::PolicyRevision
            || !identities.insert(policy.revision_id.clone())
        {
            return Err(CapabilityContractError::InvalidBinding);
        }
    }
    Ok(())
}

fn valid_qualified_name(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/'))
}

fn valid_capability_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CAPABILITY_QUALIFIED_NAME_BYTES
        && value.split('.').all(|segment| {
            let mut bytes = segment.bytes();
            bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
                && bytes.all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'_' | b'-')
                })
        })
}

fn valid_port_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().enumerate().all(|(index, byte)| {
            if index == 0 {
                byte.is_ascii_lowercase()
            } else {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
            }
        })
}

fn valid_media_type_pattern(value: &str) -> bool {
    value.len() <= 255
        && value == value.to_ascii_lowercase()
        && value.is_ascii()
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b' ')
        && value.split_once('/').is_some_and(|(kind, subtype)| {
            !kind.is_empty()
                && !subtype.is_empty()
                && !kind.contains('*')
                && !kind.contains(';')
                && !subtype.contains('/')
                && !subtype.contains(';')
                && (subtype == "*" || !subtype.contains('*'))
        })
}

fn is_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn valid_stable_value(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'+'))
}

fn valid_remote_name(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && !value.chars().any(char::is_control)
        && value.trim() == value
}

fn valid_http_header_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        ..=b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
                )
        })
}

fn valid_endpoint_host(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CAPABILITY_ENDPOINT_HOST_BYTES
        && value.is_ascii()
        && value == value.to_ascii_lowercase()
        && !value.starts_with('.')
        && !value.ends_with('.')
        && !value.contains("..")
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b':' | b'[' | b']')
        })
}

fn valid_endpoint_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CAPABILITY_ENDPOINT_PATH_BYTES
        && value.starts_with('/')
        && value.is_ascii()
        && !value.contains('?')
        && !value.contains('#')
        && !value.contains("//")
        && !value.chars().any(char::is_control)
        && value
            .split('/')
            .skip(1)
            .all(|segment| segment != "." && segment != "..")
}

fn is_sensitive_http_header(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "authorization" | "cookie" | "proxy-authorization" | "set-cookie"
    )
}

fn valid_grpc_metadata_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        })
        && !value.ends_with("-bin")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityContractError {
    InvalidName,
    InvalidLimits,
    InvalidBackend,
    InvalidFeatures,
    InvalidBinding,
    InvalidCredentials,
    InvalidEvidence,
    InvalidEndpoint,
    InvalidArtifactContract,
    InvalidDataPolicy,
    Canonicalization,
}

impl fmt::Display for CapabilityContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidName => "Capability qualified name is invalid",
            Self::InvalidLimits => "Capability backend limits are invalid",
            Self::InvalidBackend => "Capability backend contract is invalid",
            Self::InvalidFeatures => "Capability backend features are incompatible",
            Self::InvalidBinding => "Capability backend binding is invalid",
            Self::InvalidCredentials => "Capability credential requirements are invalid",
            Self::InvalidEvidence => "Capability conformance evidence is invalid",
            Self::InvalidEndpoint => "Capability canonical endpoint is invalid",
            Self::InvalidArtifactContract => "Capability Artifact contract is invalid",
            Self::InvalidDataPolicy => "Capability data-flow policy is invalid",
            Self::Canonicalization => "Capability contract cannot be canonicalized",
        })
    }
}

impl Error for CapabilityContractError {}

pub fn validate_capability_conformance_evidence(
    evidence: &ArtifactRef,
) -> Result<(), CapabilityContractError> {
    evidence
        .validate()
        .map_err(|_| CapabilityContractError::InvalidEvidence)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
        ExactVersionRef::new(
            format!(
                "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
                kind.descriptor().prefix
            )
            .parse()
            .unwrap(),
            digest(character),
        )
        .unwrap()
    }

    fn features() -> CapabilityBackendFeatures {
        CapabilityBackendFeatures {
            deferred: false,
            input_required: false,
            callback: false,
            poll: false,
            progress: false,
            cancellation: false,
            max_remote_state_bytes: 0,
            max_poll_count: 0,
        }
    }

    #[test]
    fn capability_name_is_lowercase_dot_qualified_and_closed() {
        let name = CapabilityName::new("database.query_read-only").unwrap();
        assert_eq!(name.as_str(), "database.query_read-only");
        assert_eq!(name.to_string(), "database.query_read-only");

        for invalid in [
            "",
            ".database",
            "database.",
            "database..query",
            "Database.query",
            "database.Query",
            "database/query",
            "database.1query",
        ] {
            assert_eq!(
                CapabilityName::new(invalid),
                Err(CapabilityContractError::InvalidName),
                "{invalid:?} must be rejected"
            );
        }

        let oversized = "a".repeat(MAX_CAPABILITY_QUALIFIED_NAME_BYTES + 1);
        assert_eq!(
            CapabilityName::new(oversized),
            Err(CapabilityContractError::InvalidName)
        );
    }

    #[test]
    fn interface_artifact_data_flow_and_limit_contracts_fail_closed() {
        let input = CapabilityArtifactPort {
            name: "source".to_owned(),
            direction: CapabilityArtifactDirection::Input,
            media_types: vec!["application/json".to_owned()],
            maximum_count: 2,
            maximum_single_bytes: 1_024,
            maximum_total_bytes: 2_048,
            maximum_classification: DataClassification::Internal,
        };
        let output = CapabilityArtifactPort {
            name: "rendered".to_owned(),
            direction: CapabilityArtifactDirection::Output,
            media_types: vec!["image/*".to_owned()],
            maximum_count: 1,
            maximum_single_bytes: 4_096,
            maximum_total_bytes: 4_096,
            maximum_classification: DataClassification::Confidential,
        };
        let contract = CapabilityArtifactContract {
            ports: vec![input.clone(), output],
        };
        contract.validate().unwrap();
        assert_eq!(contract.maximum_artifact_count().unwrap(), 3);

        let mut unsorted = contract.clone();
        unsorted.ports.reverse();
        assert_eq!(
            unsorted.validate(),
            Err(CapabilityContractError::InvalidArtifactContract)
        );
        let mut invalid_media = input;
        invalid_media.media_types = vec!["Application/JSON".to_owned()];
        assert_eq!(
            invalid_media.validate(),
            Err(CapabilityContractError::InvalidArtifactContract)
        );

        let policy = CapabilityDataFlowPolicy {
            maximum_input_classification: DataClassification::Confidential,
            maximum_output_classification: DataClassification::Internal,
            allowed_regions: vec!["cn-east-1".parse().unwrap()],
            declassification_policy: None,
        };
        assert_eq!(
            policy.validate(),
            Err(CapabilityContractError::InvalidDataPolicy)
        );
        let policy = CapabilityDataFlowPolicy {
            declassification_policy: Some(exact(ResourceKind::PolicyRevision, 20, 'a')),
            ..policy
        };
        policy.validate().unwrap();
        assert!(policy.permits_output(
            DataClassification::Confidential,
            DataClassification::Internal
        ));

        CapabilityInterfaceLimits {
            maximum_input_bytes: MAX_CAPABILITY_REQUEST_BYTES,
            maximum_output_bytes: MAX_CAPABILITY_RESPONSE_BYTES,
            maximum_artifacts: 3,
            maximum_execution_milliseconds: MAX_CAPABILITY_TIMEOUT_MILLISECONDS,
        }
        .validate()
        .unwrap();
        assert_eq!(
            CapabilityInterfaceLimits {
                maximum_input_bytes: MAX_CAPABILITY_REQUEST_BYTES + 1,
                maximum_output_bytes: 1,
                maximum_artifacts: 0,
                maximum_execution_milliseconds: 1,
            }
            .validate(),
            Err(CapabilityContractError::InvalidLimits)
        );
    }

    #[test]
    fn backend_contract_is_closed_digestible_and_feature_checked() {
        let native = CapabilityBackendContract::Native(NativeCapabilityContract {
            adapter_id: "builtin.presentation.render".to_owned(),
            adapter_version: "1.0.0".to_owned(),
            module_digest: digest('a'),
            entrypoint_id: "presentation.render".to_owned(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
        });
        native.validate(&features()).unwrap();
        assert_eq!(native.kind(), CapabilityBackendKind::Native);
        native.canonical_digest().unwrap();

        let mut unsupported = features();
        unsupported.deferred = true;
        unsupported.max_remote_state_bytes = 128;
        let mcp = CapabilityBackendContract::Mcp(McpToolCapabilityContract {
            remote_tool_name: "render".to_owned(),
            remote_input_schema_digest: digest('b'),
            output_mapping_digest: digest('c'),
            protocol_profile: exact(ResourceKind::PolicyRevision, 1, 'd'),
            discovery_semantic_evidence_digest: digest('e'),
            supports_task: false,
            supports_progress: false,
        });
        assert_eq!(
            mcp.validate(&unsupported),
            Err(CapabilityContractError::InvalidFeatures)
        );
    }

    #[test]
    fn deployment_binding_rejects_kind_downgrade_and_native_digest_swap() {
        let contract = CapabilityBackendContract::Native(NativeCapabilityContract {
            adapter_id: "builtin.lookup".to_owned(),
            adapter_version: "1".to_owned(),
            module_digest: digest('a'),
            entrypoint_id: "lookup".to_owned(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
        });
        let wrong_digest = CapabilityBackendBinding::Native {
            worker_manifest_digest: digest('b'),
            adapter_module_digest: digest('c'),
        };
        assert_eq!(
            wrong_digest.validate_for(&contract),
            Err(CapabilityContractError::InvalidBinding)
        );
        let http = CapabilityBackendBinding::Http {
            codec: InstalledCapabilityCodecRef {
                schema_version: INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION,
                backend_kind: CapabilityBackendKind::Http,
                codec_id: "fixture.http".to_owned(),
                codec_version: "1".to_owned(),
                module_digest: digest('1'),
                worker_protocol_version: WORKER_PROTOCOL_VERSION,
                descriptor_digest: digest('2'),
            },
            worker_manifest_digest: digest('3'),
            endpoint: CanonicalHttpEndpoint {
                scheme: CapabilityEndpointScheme::Https,
                host: "api.example.test".to_owned(),
                port: 443,
                base_path: "/v1/capability".to_owned(),
            },
            endpoint_identity_digest: digest('d'),
            network_policy: exact(ResourceKind::PolicyRevision, 2, 'e'),
            tls_policy: exact(ResourceKind::PolicyRevision, 3, 'f'),
            trust_policy: exact(ResourceKind::PolicyRevision, 4, '0'),
        };
        assert_eq!(
            http.validate_for(&contract),
            Err(CapabilityContractError::InvalidBinding)
        );
    }

    #[test]
    fn backend_contract_wire_is_closed_and_unknown_kinds_fail_closed() {
        let native = CapabilityBackendContract::Native(NativeCapabilityContract {
            adapter_id: "builtin.lookup".to_owned(),
            adapter_version: "1".to_owned(),
            module_digest: digest('a'),
            entrypoint_id: "lookup".to_owned(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
        });
        let mut value = serde_json::to_value(native).unwrap();
        value
            .get_mut("contract")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .insert("opaque_config".to_owned(), serde_json::json!({}));
        assert!(serde_json::from_value::<CapabilityBackendContract>(value).is_err());
        assert!(
            serde_json::from_value::<CapabilityBackendContract>(serde_json::json!({
                "kind": "plugin",
                "contract": {}
            }))
            .is_err()
        );
    }

    #[test]
    fn limits_credentials_and_sensitive_http_headers_fail_closed() {
        let limits = CapabilityBackendLimits {
            maximum_request_bytes: 1,
            maximum_response_bytes: 1,
            maximum_diagnostic_bytes: 0,
            connect_timeout_milliseconds: 100,
            first_byte_timeout_milliseconds: 200,
            idle_timeout_milliseconds: 300,
            total_timeout_milliseconds: 300,
        };
        assert_eq!(
            limits.validate(),
            Err(CapabilityContractError::InvalidLimits)
        );

        let first = "service.api_key".parse::<SecretPurpose>().unwrap();
        let second = "service.oauth".parse::<SecretPurpose>().unwrap();
        assert_eq!(
            validate_capability_credential_requirements(&[second, first]),
            Err(CapabilityContractError::InvalidCredentials)
        );

        let http = CapabilityBackendContract::Http(HttpCapabilityContract {
            method: HttpCapabilityMethod::Post,
            protocol_contract_digest: digest('1'),
            request_mapping_digest: digest('2'),
            response_mapping_digest: digest('3'),
            error_mapping_digest: digest('4'),
            idempotency_header: Some("Authorization".to_owned()),
        });
        assert_eq!(
            http.validate(&features()),
            Err(CapabilityContractError::InvalidBackend)
        );
    }

    #[test]
    fn sandbox_and_mcp_feature_claims_are_checked_by_their_exact_contract() {
        let sandbox = CapabilityBackendContract::Sandbox(SandboxCapabilityContract {
            package_contract_digest: digest('1'),
            image_uri: format!("registry.invalid/render@sha256:{}", "a".repeat(64)),
            package_argv: vec!["/opt/insight/render".to_owned()],
            dependency_lock_digest: digest('2'),
            runtime_contract_digest: digest('3'),
            input_mapping_digest: digest('3'),
            output_mapping_digest: digest('4'),
        });
        assert_eq!(
            sandbox.validate(&features()),
            Err(CapabilityContractError::InvalidFeatures)
        );

        let mut progress = features();
        progress.deferred = true;
        progress.progress = true;
        progress.max_remote_state_bytes = 128;
        let mcp = CapabilityBackendContract::Mcp(McpToolCapabilityContract {
            remote_tool_name: "render".to_owned(),
            remote_input_schema_digest: digest('5'),
            output_mapping_digest: digest('6'),
            protocol_profile: exact(ResourceKind::PolicyRevision, 5, '7'),
            discovery_semantic_evidence_digest: digest('8'),
            supports_task: true,
            supports_progress: false,
        });
        assert_eq!(
            mcp.validate(&progress),
            Err(CapabilityContractError::InvalidFeatures)
        );
    }
}
