use crate::{
    canonical_digest, ArtifactRef, ContextBackendKind, ContextCitationStrength,
    ContextConsistencyMode, DataClassification, DataRegion, ExactDeploymentRef, ExactVersionRef,
    ResourceId, ResourceKind, SecretPurpose, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, fmt};

pub const MAX_CONTEXT_FIELDS: usize = 256;
pub const MAX_CONTEXT_POLICIES: usize = 32;
pub const MAX_CONTEXT_CREDENTIALS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextLocatorKind {
    ArtifactSpan,
    DocumentSection,
    CatalogObject,
    McpResource,
    RemoteOpaque,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextCitationContract {
    pub allowed_strengths: Vec<ContextCitationStrength>,
    pub locator_kinds: Vec<ContextLocatorKind>,
    pub require_content_digest: bool,
    pub maximum_display_label_bytes: u32,
}

impl ContextCitationContract {
    pub fn validate(&self) -> Result<(), ContextContractError> {
        if self.allowed_strengths.is_empty()
            || self.locator_kinds.is_empty()
            || self.maximum_display_label_bytes == 0
            || self.maximum_display_label_bytes > 4_096
            || !is_sorted_unique(&self.allowed_strengths)
            || !is_sorted_unique(&self.locator_kinds)
        {
            return Err(ContextContractError::InvalidCitation);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPaginationContract {
    pub maximum_page_size: u32,
    pub maximum_cursor_bytes: u32,
    pub cursor_ttl_milliseconds: u64,
}

impl ContextPaginationContract {
    pub fn validate(&self) -> Result<(), ContextContractError> {
        if self.maximum_page_size == 0
            || self.maximum_page_size > 10_000
            || self.maximum_cursor_bytes == 0
            || self.maximum_cursor_bytes > 65_536
            || self.cursor_ttl_milliseconds == 0
        {
            return Err(ContextContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextRankingContract {
    pub score_domain_digest: Sha256Digest,
    pub reranker_contract_digest: Option<Sha256Digest>,
    pub maximum_candidates: u32,
}

impl ContextRankingContract {
    pub fn validate(&self) -> Result<(), ContextContractError> {
        if self.maximum_candidates == 0 || self.maximum_candidates > 100_000 {
            return Err(ContextContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextDataPolicyContract {
    pub maximum_classification: DataClassification,
    pub allowed_regions: Vec<DataRegion>,
    pub entitlement_policy: ExactVersionRef,
    pub cache_policy: ExactVersionRef,
    pub maximum_retention_milliseconds: u64,
}

impl ContextDataPolicyContract {
    pub fn validate(&self) -> Result<(), ContextContractError> {
        if self.allowed_regions.is_empty()
            || self.allowed_regions.len() > 32
            || !is_sorted_unique(&self.allowed_regions)
            || self.entitlement_policy.resource_kind != ResourceKind::PolicyRevision
            || self.cache_policy.resource_kind != ResourceKind::PolicyRevision
            || self.entitlement_policy == self.cache_policy
            || self.maximum_retention_milliseconds == 0
        {
            return Err(ContextContractError::InvalidPolicy);
        }
        self.entitlement_policy
            .validate()
            .map_err(|_| ContextContractError::InvalidPolicy)?;
        self.cache_policy
            .validate()
            .map_err(|_| ContextContractError::InvalidPolicy)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextInterfaceLimits {
    pub maximum_query_bytes: u32,
    pub maximum_filter_bytes: u32,
    pub maximum_item_bytes: u32,
    pub maximum_total_bytes: u32,
    pub maximum_items: u32,
    pub maximum_fan_out: u16,
}

impl ContextInterfaceLimits {
    pub fn validate(&self) -> Result<(), ContextContractError> {
        if self.maximum_query_bytes == 0
            || self.maximum_filter_bytes == 0
            || self.maximum_item_bytes == 0
            || self.maximum_total_bytes < self.maximum_item_bytes
            || self.maximum_items == 0
            || self.maximum_fan_out == 0
            || self.maximum_query_bytes > 1_048_576
            || self.maximum_filter_bytes > 1_048_576
            || self.maximum_item_bytes > 16 * 1_048_576
            || self.maximum_total_bytes > 64 * 1_048_576
            || self.maximum_items > 10_000
            || self.maximum_fan_out > 64
        {
            return Err(ContextContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextBackendLimits {
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
    pub maximum_candidates: u32,
    pub maximum_remote_state_bytes: u32,
    pub maximum_poll_count: u32,
    pub total_timeout_milliseconds: u64,
}

impl ContextBackendLimits {
    pub fn validate(&self) -> Result<(), ContextContractError> {
        if self.maximum_request_bytes == 0
            || self.maximum_response_bytes == 0
            || self.maximum_candidates == 0
            || self.total_timeout_milliseconds == 0
            || self.maximum_request_bytes > 16 * 1_048_576
            || self.maximum_response_bytes > 64 * 1_048_576
            || self.maximum_candidates > 100_000
            || self.maximum_remote_state_bytes > 1_048_576
            || self.maximum_poll_count > 10_000
            || (self.maximum_poll_count == 0) != (self.maximum_remote_state_bytes == 0)
        {
            return Err(ContextContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextBackendContract {
    ManagedIndex {
        query_contract_digest: Sha256Digest,
        result_contract_digest: Sha256Digest,
    },
    RemoteSearch {
        protocol_contract_digest: Sha256Digest,
        result_mapping_digest: Sha256Digest,
    },
    McpResources {
        resource_contract_digest: Sha256Digest,
        uri_policy: ExactVersionRef,
    },
    SqlCatalog {
        dialect: String,
        catalog_projection_digest: Sha256Digest,
    },
    ArtifactCollection {
        collection_contract_digest: Sha256Digest,
    },
    NativeCatalog {
        adapter_contract_digest: Sha256Digest,
    },
}

impl ContextBackendContract {
    pub const fn kind(&self) -> ContextBackendKind {
        match self {
            Self::ManagedIndex { .. } => ContextBackendKind::ManagedIndex,
            Self::RemoteSearch { .. } => ContextBackendKind::RemoteSearch,
            Self::McpResources { .. } => ContextBackendKind::McpResources,
            Self::SqlCatalog { .. } => ContextBackendKind::SqlCatalog,
            Self::ArtifactCollection { .. } => ContextBackendKind::ArtifactCollection,
            Self::NativeCatalog { .. } => ContextBackendKind::NativeCatalog,
        }
    }

    pub fn validate(&self) -> Result<(), ContextContractError> {
        match self {
            Self::McpResources { uri_policy, .. } => {
                uri_policy
                    .validate()
                    .map_err(|_| ContextContractError::InvalidBackend)?;
                if uri_policy.resource_kind != ResourceKind::PolicyRevision {
                    return Err(ContextContractError::InvalidBackend);
                }
                Ok(())
            }
            Self::SqlCatalog { dialect, .. }
                if dialect.is_empty()
                    || dialect.len() > 64
                    || !dialect
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte == b'_') =>
            {
                Err(ContextContractError::InvalidBackend)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextBackendBinding {
    ManagedIndex {
        service_identity_digest: Sha256Digest,
        index_namespace_digest: Sha256Digest,
        region: DataRegion,
    },
    RemoteSearch {
        endpoint_identity_digest: Sha256Digest,
        region: DataRegion,
    },
    McpResources {
        mcp_deployment: ExactDeploymentRef,
        discovery_snapshot_id: ResourceId,
        discovery_snapshot_digest: Sha256Digest,
    },
    SqlCatalog {
        database_identity_digest: Sha256Digest,
        dialect: String,
        catalog_scope_digest: Sha256Digest,
    },
    ArtifactCollection {
        collection_identity_digest: Sha256Digest,
    },
    NativeCatalog {
        installed_adapter_digest: Sha256Digest,
    },
}

impl ContextBackendBinding {
    pub const fn kind(&self) -> ContextBackendKind {
        match self {
            Self::ManagedIndex { .. } => ContextBackendKind::ManagedIndex,
            Self::RemoteSearch { .. } => ContextBackendKind::RemoteSearch,
            Self::McpResources { .. } => ContextBackendKind::McpResources,
            Self::SqlCatalog { .. } => ContextBackendKind::SqlCatalog,
            Self::ArtifactCollection { .. } => ContextBackendKind::ArtifactCollection,
            Self::NativeCatalog { .. } => ContextBackendKind::NativeCatalog,
        }
    }

    pub fn validate(&self) -> Result<(), ContextContractError> {
        if let Self::McpResources { mcp_deployment, .. } = self {
            mcp_deployment
                .validate()
                .map_err(|_| ContextContractError::InvalidBackend)?;
        }
        match self {
            Self::McpResources {
                mcp_deployment,
                discovery_snapshot_id,
                ..
            } if mcp_deployment.resource_kind != ResourceKind::McpDeployment
                || discovery_snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot =>
            {
                Err(ContextContractError::InvalidBackend)
            }
            Self::SqlCatalog { dialect, .. }
                if dialect.is_empty()
                    || dialect.len() > 64
                    || !dialect
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte == b'_') =>
            {
                Err(ContextContractError::InvalidBackend)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactDatasetGenerationRef {
    pub dataset_id: ResourceId,
    pub generation_id: ResourceId,
    pub generation_digest: Sha256Digest,
}

impl ExactDatasetGenerationRef {
    pub fn validate(&self) -> Result<(), ContextContractError> {
        if self.dataset_id.kind() != ResourceKind::ContextDataset
            || self.generation_id.kind() != ResourceKind::DatasetGeneration
        {
            return Err(ContextContractError::InvalidDataset);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextConsistencyPolicy {
    PinnedGeneration {
        generation: ExactDatasetGenerationRef,
    },
    PinAtRunAdmission {
        dataset_id: ResourceId,
    },
    LatestAtQueryStart {
        dataset_id: ResourceId,
    },
    ExternalObservation,
}

impl ContextConsistencyPolicy {
    pub const fn mode(&self) -> ContextConsistencyMode {
        match self {
            Self::PinnedGeneration { .. } => ContextConsistencyMode::PinnedGeneration,
            Self::PinAtRunAdmission { .. } => ContextConsistencyMode::PinAtRunAdmission,
            Self::LatestAtQueryStart { .. } => ContextConsistencyMode::LatestAtQueryStart,
            Self::ExternalObservation => ContextConsistencyMode::ExternalObservation,
        }
    }

    pub fn validate(&self) -> Result<(), ContextContractError> {
        match self {
            Self::PinnedGeneration { generation } => generation.validate(),
            Self::PinAtRunAdmission { dataset_id } | Self::LatestAtQueryStart { dataset_id }
                if dataset_id.kind() == ResourceKind::ContextDataset =>
            {
                Ok(())
            }
            Self::ExternalObservation => Ok(()),
            _ => Err(ContextContractError::InvalidDataset),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextBindingSnapshot {
    pub schema_version: u32,
    pub context_binding_id: ResourceId,
    pub owner_agent_deployment_id: ResourceId,
    pub context_deployment: ExactDeploymentRef,
    pub consistency: ContextConsistencyPolicy,
    pub allowed_projection: Vec<String>,
    pub authorization_policy: ExactVersionRef,
    pub ranking_policy: ExactVersionRef,
    pub binding_digest: Sha256Digest,
}

impl ContextBindingSnapshot {
    pub fn build(
        context_binding_id: ResourceId,
        owner_agent_deployment_id: ResourceId,
        context_deployment: ExactDeploymentRef,
        consistency: ContextConsistencyPolicy,
        allowed_projection: Vec<String>,
        authorization_policy: ExactVersionRef,
        ranking_policy: ExactVersionRef,
    ) -> Result<Self, ContextContractError> {
        let mut snapshot = Self {
            schema_version: 1,
            context_binding_id,
            owner_agent_deployment_id,
            context_deployment,
            consistency,
            allowed_projection,
            authorization_policy,
            ranking_policy,
            binding_digest:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .parse()
                    .map_err(|_| ContextContractError::Canonicalization)?,
        };
        snapshot.binding_digest = snapshot.digest_without_binding()?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn validate(&self) -> Result<(), ContextContractError> {
        self.consistency.validate()?;
        self.context_deployment
            .validate()
            .map_err(|_| ContextContractError::InvalidBinding)?;
        self.authorization_policy
            .validate()
            .map_err(|_| ContextContractError::InvalidBinding)?;
        self.ranking_policy
            .validate()
            .map_err(|_| ContextContractError::InvalidBinding)?;
        if self.schema_version != 1
            || self.context_binding_id.kind() != ResourceKind::ContextBinding
            || self.owner_agent_deployment_id.kind() != ResourceKind::AgentDeployment
            || self.context_deployment.resource_kind != ResourceKind::ContextDeployment
            || self.allowed_projection.len() > MAX_CONTEXT_FIELDS
            || !is_sorted_unique(&self.allowed_projection)
            || self.allowed_projection.iter().any(|field| !is_field(field))
            || self.authorization_policy.resource_kind != ResourceKind::PolicyRevision
            || self.ranking_policy.resource_kind != ResourceKind::PolicyRevision
            || self.authorization_policy == self.ranking_policy
            || self.digest_without_binding()? != self.binding_digest
        {
            return Err(ContextContractError::InvalidBinding);
        }
        Ok(())
    }

    fn digest_without_binding(&self) -> Result<Sha256Digest, ContextContractError> {
        let mut value =
            serde_json::to_value(self).map_err(|_| ContextContractError::Canonicalization)?;
        value
            .as_object_mut()
            .ok_or(ContextContractError::Canonicalization)?
            .remove("binding_digest")
            .ok_or(ContextContractError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| ContextContractError::Canonicalization)?
            .parse()
            .map_err(|_| ContextContractError::Canonicalization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunContextDatasetView {
    pub context_binding_id: ResourceId,
    pub context_binding_digest: Sha256Digest,
    pub generation: ExactDatasetGenerationRef,
}

impl RunContextDatasetView {
    pub fn validate_for(
        &self,
        binding: &ContextBindingSnapshot,
    ) -> Result<(), ContextContractError> {
        self.generation.validate()?;
        if self.context_binding_id != binding.context_binding_id
            || self.context_binding_digest != binding.binding_digest
        {
            return Err(ContextContractError::InvalidDataset);
        }
        match &binding.consistency {
            ContextConsistencyPolicy::PinnedGeneration { generation }
                if generation == &self.generation =>
            {
                Ok(())
            }
            ContextConsistencyPolicy::PinAtRunAdmission { dataset_id }
                if dataset_id == &self.generation.dataset_id =>
            {
                Ok(())
            }
            ContextConsistencyPolicy::PinnedGeneration { .. }
            | ContextConsistencyPolicy::PinAtRunAdmission { .. }
            | ContextConsistencyPolicy::LatestAtQueryStart { .. }
            | ContextConsistencyPolicy::ExternalObservation => {
                Err(ContextContractError::InvalidDataset)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextDatasetGenerationSpec {
    pub context_deployment: ExactDeploymentRef,
    pub source_manifest_digest: Sha256Digest,
    pub parser_profile: ExactVersionRef,
    pub chunker_profile: ExactVersionRef,
    pub embedding_model_deployment: Option<ExactDeploymentRef>,
    pub ranking_profile: ExactVersionRef,
    pub index_manifest: ArtifactRef,
    pub validation_evidence: ArtifactRef,
    pub created_by_operation_id: ResourceId,
}

impl ContextDatasetGenerationSpec {
    pub fn validate(&self) -> Result<(), ContextContractError> {
        self.context_deployment
            .validate()
            .map_err(|_| ContextContractError::InvalidDataset)?;
        self.parser_profile
            .validate()
            .map_err(|_| ContextContractError::InvalidDataset)?;
        self.chunker_profile
            .validate()
            .map_err(|_| ContextContractError::InvalidDataset)?;
        self.ranking_profile
            .validate()
            .map_err(|_| ContextContractError::InvalidDataset)?;
        if let Some(deployment) = &self.embedding_model_deployment {
            deployment
                .validate()
                .map_err(|_| ContextContractError::InvalidDataset)?;
        }
        if self.context_deployment.resource_kind != ResourceKind::ContextDeployment
            || self.parser_profile.resource_kind != ResourceKind::PolicyRevision
            || self.chunker_profile.resource_kind != ResourceKind::PolicyRevision
            || self.ranking_profile.resource_kind != ResourceKind::PolicyRevision
            || self.created_by_operation_id.kind() != ResourceKind::Job
            || self
                .embedding_model_deployment
                .as_ref()
                .is_some_and(|deployment| deployment.resource_kind != ResourceKind::ModelDeployment)
        {
            return Err(ContextContractError::InvalidDataset);
        }
        self.index_manifest
            .validate()
            .map_err(|_| ContextContractError::InvalidDataset)?;
        self.validation_evidence
            .validate()
            .map_err(|_| ContextContractError::InvalidDataset)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextImplementationContract {
    pub backend: ContextBackendContract,
    pub credential_requirements: Vec<SecretPurpose>,
    pub limits: ContextBackendLimits,
}

impl ContextImplementationContract {
    pub fn validate(&self) -> Result<(), ContextContractError> {
        self.backend.validate()?;
        self.limits.validate()?;
        if self.credential_requirements.len() > MAX_CONTEXT_CREDENTIALS {
            return Err(ContextContractError::InvalidCredential);
        }
        let mut purposes = BTreeSet::new();
        if self
            .credential_requirements
            .iter()
            .any(|purpose| !purposes.insert(purpose.as_str()))
        {
            return Err(ContextContractError::InvalidCredential);
        }
        Ok(())
    }

    pub fn validate_binding(
        &self,
        binding: &ContextBackendBinding,
    ) -> Result<(), ContextContractError> {
        self.validate()?;
        binding.validate()?;
        if self.backend.kind() != binding.kind()
            || matches!(
                (&self.backend, binding),
                (
                    ContextBackendContract::SqlCatalog {
                        dialect: contract_dialect,
                        ..
                    },
                    ContextBackendBinding::SqlCatalog {
                        dialect: binding_dialect,
                        ..
                    }
                ) if contract_dialect != binding_dialect
            )
        {
            return Err(ContextContractError::InvalidBackend);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextContractError {
    InvalidCitation,
    InvalidLimits,
    InvalidPolicy,
    InvalidBackend,
    InvalidCredential,
    InvalidDataset,
    InvalidBinding,
    Canonicalization,
}

impl fmt::Display for ContextContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCitation => "context citation contract is invalid",
            Self::InvalidLimits => "context limits are invalid",
            Self::InvalidPolicy => "context policy contract is invalid",
            Self::InvalidBackend => "context backend contract is invalid",
            Self::InvalidCredential => "context credential contract is invalid",
            Self::InvalidDataset => "context dataset contract is invalid",
            Self::InvalidBinding => "context binding snapshot is invalid",
            Self::Canonicalization => "context contract canonicalization failed",
        })
    }
}

impl Error for ContextContractError {}

fn is_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn is_field(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}
