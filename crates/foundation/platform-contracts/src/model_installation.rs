//! One deployment-owned configuration value shared by model configuration, Egress and authoring.
//! Exact Policy and adapter identities must be verified by installation against their real owners.
use crate::{
    canonical_digest, ExactPolicyBinding, ExactVersionRef, InstalledModelAdapter,
    InstalledModelDestinationGrant, ResourceKind, Sha256Digest, ANTHROPIC_MESSAGES_ADAPTER_NAME,
    OPENAI_RESPONSES_ADAPTER_NAME,
};
use serde::{Deserialize, Serialize};

pub const MAX_MODEL_INSTALLATION_DESTINATIONS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfigurationPoliciesV1 {
    pub protocol: ExactVersionRef,
    pub safety: ExactVersionRef,
    pub budget: ExactVersionRef,
    pub public_projection: ExactVersionRef,
    pub selection: ExactPolicyBinding,
    pub execution: ExactPolicyBinding,
}

impl ModelConfigurationPoliciesV1 {
    pub fn validate(&self) -> bool {
        let refs = [
            &self.protocol,
            &self.safety,
            &self.budget,
            &self.public_projection,
            &self.selection.revision,
            &self.execution.revision,
        ];
        refs.iter().all(|exact| {
            exact.resource_kind == ResourceKind::PolicyRevision && exact.validate().is_ok()
        }) && refs
            .iter()
            .map(|exact| &exact.revision_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == refs.len()
            && self.selection.validate().is_ok()
            && self.execution.validate().is_ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInstallationDestinationV1 {
    pub grant: InstalledModelDestinationGrant,
    pub adapter: InstalledModelAdapter,
}

impl ModelInstallationDestinationV1 {
    pub fn validate(&self) -> bool {
        self.grant.validate_shape()
            && self.adapter.validate().is_ok()
            && self.adapter.adapter_contract_digest == self.grant.protocol.adapter_contract_digest()
            && self.adapter.qualified_name
                == match self.grant.protocol {
                    crate::ModelProviderWireProtocol::OpenAiResponses => {
                        OPENAI_RESPONSES_ADAPTER_NAME
                    }
                    crate::ModelProviderWireProtocol::AnthropicMessages => {
                        ANTHROPIC_MESSAGES_ADAPTER_NAME
                    }
                }
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, crate::ModelContractError> {
        if !self.validate() {
            return Err(crate::ModelContractError::InvalidProviderConfiguration);
        }
        canonical_digest(
            &serde_json::to_value(self).map_err(|_| crate::ModelContractError::InvalidJson)?,
        )
        .map_err(|_| crate::ModelContractError::InvalidJson)?
        .parse()
        .map_err(|_| crate::ModelContractError::InvalidJson)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInstallationCatalogV1 {
    pub schema_version: u16,
    pub environment: String,
    pub secret_provider_id: crate::ResourceId,
    pub policies: ModelConfigurationPoliciesV1,
    pub destinations: Vec<ModelInstallationDestinationV1>,
}

impl ModelInstallationCatalogV1 {
    pub fn validate(&self) -> bool {
        self.schema_version == 1
            && self.secret_provider_id.kind() == ResourceKind::SecretProvider
            && !self.environment.is_empty()
            && self.environment.len() <= 64
            && self.environment.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
            })
            && self.policies.validate()
            && !self.destinations.is_empty()
            && self.destinations.len() <= MAX_MODEL_INSTALLATION_DESTINATIONS
            && self
                .destinations
                .iter()
                .all(ModelInstallationDestinationV1::validate)
            && self.destinations.iter().enumerate().all(|(index, item)| {
                !self.destinations[..index]
                    .iter()
                    .any(|other| item.grant.same_selector(&other.grant))
            })
            && self
                .destinations
                .iter()
                .filter_map(|item| item.canonical_digest().ok())
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == self.destinations.len()
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, crate::ModelContractError> {
        if !self.validate() {
            return Err(crate::ModelContractError::InvalidProviderConfiguration);
        }
        canonical_digest(
            &serde_json::to_value(self).map_err(|_| crate::ModelContractError::InvalidJson)?,
        )
        .map_err(|_| crate::ModelContractError::InvalidJson)?
        .parse()
        .map_err(|_| crate::ModelContractError::InvalidJson)
    }
}
