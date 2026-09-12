//! One deployment-owned configuration value shared by model configuration, Egress and authoring.
//! Exact Policy and adapter identities must be verified by installation against their real owners.
use crate::{
    canonical_digest, ExactPolicyBinding, ExactVersionRef, InstalledModelAdapter,
    InstalledModelDestinationGrant, ResourceKind, Sha256Digest,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfigurationPoliciesV2 {
    pub protocol: ExactVersionRef,
    pub safety: ExactVersionRef,
    pub budget: ExactVersionRef,
    pub public_projection: ExactVersionRef,
    pub selection: ExactPolicyBinding,
    pub execution: ExactPolicyBinding,
    pub network: ExactVersionRef,
    pub tls: ExactVersionRef,
    pub trust: ExactVersionRef,
    pub data: ExactVersionRef,
}

impl ModelConfigurationPoliciesV2 {
    pub fn validate(&self) -> bool {
        let refs = [
            &self.protocol,
            &self.safety,
            &self.budget,
            &self.public_projection,
            &self.selection.revision,
            &self.execution.revision,
            &self.network,
            &self.tls,
            &self.trust,
            &self.data,
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

/// Ephemeral resolution of an authorized source against the installed adapter and policies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedModelConfigurationDestination {
    pub grant: InstalledModelDestinationGrant,
    pub adapter: InstalledModelAdapter,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInstallationCatalogV2 {
    pub schema_version: u16,
    pub environment: String,
    pub secret_provider_id: crate::ResourceId,
    pub policies: ModelConfigurationPoliciesV2,
    pub adapters: Vec<InstalledModelAdapter>,
}

impl ModelInstallationCatalogV2 {
    pub fn validate(&self) -> bool {
        self.schema_version == 2
            && self.secret_provider_id.kind() == ResourceKind::SecretProvider
            && !self.environment.is_empty()
            && self.environment.len() <= 64
            && self.environment.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
            })
            && self.policies.validate()
            && !self.adapters.is_empty()
            && self.adapters.len() <= 2
            && self.adapters.iter().all(|adapter| {
                adapter.validate().is_ok()
                    && [
                        crate::ModelProviderWireProtocol::OpenAiResponses,
                        crate::ModelProviderWireProtocol::AnthropicMessages,
                    ]
                    .iter()
                    .any(|p| {
                        adapter.qualified_name == p.qualified_name()
                            && adapter.adapter_contract_digest == p.adapter_contract_digest()
                    })
            })
            && self
                .adapters
                .iter()
                .map(|a| &a.qualified_name)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == self.adapters.len()
    }

    pub fn public_egress(&self) -> crate::InstalledModelPublicEgressV1 {
        crate::InstalledModelPublicEgressV1 {
            schema_version: 1,
            protocols: [
                crate::ModelProviderWireProtocol::OpenAiResponses,
                crate::ModelProviderWireProtocol::AnthropicMessages,
            ]
            .into_iter()
            .filter(|p| {
                self.adapters
                    .iter()
                    .any(|a| a.qualified_name == p.qualified_name())
            })
            .collect(),
            credential_purpose: crate::MODEL_API_KEY_PURPOSE
                .parse()
                .expect("closed purpose"),
            network_policy: self.policies.network.clone(),
            tls_policy: self.policies.tls.clone(),
            trust_policy: self.policies.trust.clone(),
            data_policy: self.policies.data.clone(),
        }
    }

    pub fn destination(
        &self,
        protocol: crate::ModelProviderWireProtocol,
        endpoint: crate::CanonicalHttpEndpoint,
        region: crate::DataRegion,
    ) -> Option<ResolvedModelConfigurationDestination> {
        if !self.validate() {
            return None;
        }
        Some(ResolvedModelConfigurationDestination {
            adapter: self
                .adapters
                .iter()
                .find(|a| a.qualified_name == protocol.qualified_name())?
                .clone(),
            grant: self
                .public_egress()
                .destination(protocol, endpoint, region)?,
        })
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
