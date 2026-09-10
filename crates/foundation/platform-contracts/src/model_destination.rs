//! Deployment-owned physical model destinations. These values grant no business authorization.
use crate::{
    CanonicalHttpEndpoint, DataRegion, ExactVersionRef, ResourceKind, SecretPurpose, Sha256Digest,
};
use serde::{Deserialize, Serialize};

pub const OPENAI_RESPONSES_ADAPTER_NAME: &str = "openai.responses/v1";
pub const ANTHROPIC_MESSAGES_ADAPTER_NAME: &str = "anthropic.messages/2023-06-01";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelProviderWireProtocol {
    OpenAiResponses,
    AnthropicMessages,
}

impl ModelProviderWireProtocol {
    pub const fn qualified_name(self) -> &'static str {
        match self {
            Self::OpenAiResponses => OPENAI_RESPONSES_ADAPTER_NAME,
            Self::AnthropicMessages => ANTHROPIC_MESSAGES_ADAPTER_NAME,
        }
    }

    /// The installed wire mapping identity is independent of the executable build and tenant
    /// policies. Change its semantic version when this closed mapping changes.
    pub fn adapter_contract_digest(self) -> Sha256Digest {
        crate::canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "kind": "insight.model-provider-adapter-contract/v1",
            "protocol": self,
            "qualified_name": self.qualified_name(),
            "canonical_request_abi": 1,
            "canonical_response_abi": 1,
            "normalized_stream_abi": 1,
            "provider_wire_request_abi": 2,
            "endpoint_path": self.endpoint_path(),
            "protocol_version": self.protocol_version(),
            "wire_mapping_semantics": match self {
                Self::OpenAiResponses => 4,
                Self::AnthropicMessages => 2,
            },
        }))
        .expect("closed model adapter declaration is canonical JSON")
        .parse()
        .expect("canonical model adapter declaration is SHA256")
    }

    pub const fn endpoint_path(self) -> &'static str {
        match self {
            Self::OpenAiResponses => "/v1/responses",
            Self::AnthropicMessages => "/v1/messages",
        }
    }

    pub const fn protocol_version(self) -> &'static str {
        match self {
            Self::OpenAiResponses => "responses-v1",
            Self::AnthropicMessages => "2023-06-01",
        }
    }
}

/// Deployment-owned destination grant. Current business/account authorization is separate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledModelDestinationGrant {
    pub schema_version: u32,
    pub protocol: ModelProviderWireProtocol,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub credential_purpose: SecretPurpose,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub data_policy: ExactVersionRef,
    pub region: DataRegion,
    #[serde(default)]
    pub development_loopback: bool,
    #[serde(default)]
    pub development_anonymous: bool,
    #[serde(default)]
    pub trusted_root_pem: Option<String>,
}

impl InstalledModelDestinationGrant {
    /// Pure shape validation. Egress additionally validates HTTPS trust material, path and DNS.
    pub fn validate_shape(&self) -> bool {
        self.schema_version == 1
            && self.endpoint.scheme == crate::CapabilityEndpointScheme::Https
            && self.endpoint.validate().is_ok()
            && self.endpoint.canonical_digest().as_ref() == Ok(&self.endpoint_identity_digest)
            && [
                &self.network_policy,
                &self.tls_policy,
                &self.trust_policy,
                &self.data_policy,
            ]
            .iter()
            .all(|policy| {
                policy.resource_kind == ResourceKind::PolicyRevision && policy.validate().is_ok()
            })
            && self
                .trusted_root_pem
                .as_ref()
                .is_none_or(|pem| !pem.is_empty() && pem.len() <= 65_536)
            && (!self.development_anonymous || self.development_loopback)
            && (!self.development_loopback
                || (self.endpoint.host == "localhost" && self.trusted_root_pem.is_some()))
    }

    pub fn same_selector(&self, other: &Self) -> bool {
        self.protocol == other.protocol
            && self.endpoint_identity_digest == other.endpoint_identity_digest
            && self.network_policy == other.network_policy
            && self.tls_policy == other.tls_policy
            && self.trust_policy == other.trust_policy
            && self.data_policy == other.data_policy
            && self.region == other.region
    }
}
