use crate::ContextQueryError;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ArtifactRef, ClosedJsonValue, ContextBindingSnapshot,
    ContextDatasetGenerationSpec, ContextImplementationResourceSpec, ContextInterfaceResourceSpec,
    ContextLocatorKind, DataClassification, ExactDatasetGenerationRef, ExactDeploymentRef,
    ExactVersionRef, HardLimitProfile, JsonLimits, PrincipalSnapshot, ResourceId, ResourceKind,
    Sha256Digest, ValueRef,
};
use insight_platform_invocations::ExactInvocationValueRef;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_CONTEXT_SAFE_CODE_BYTES: usize = 128;
pub const MAX_CONTEXT_LOCATOR_SEGMENTS: usize = 64;
pub const MAX_CONTEXT_LOCATOR_SEGMENT_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextQueryLimits {
    maximum_attempts: u32,
    maximum_candidates: u32,
    maximum_items: u32,
    maximum_pages: u32,
    maximum_result_bytes: u64,
    inline_value_limits: JsonLimits,
    maximum_lease_milliseconds: u64,
}

impl ContextQueryLimits {
    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, ContextQueryError> {
        profile
            .validate()
            .map_err(|_| ContextQueryError::InvalidLimits)?;
        let to_u32 =
            |value: u64| u32::try_from(value).map_err(|_| ContextQueryError::InvalidLimits);
        let to_usize =
            |value: u64| usize::try_from(value).map_err(|_| ContextQueryError::InvalidLimits);
        let limits = Self {
            maximum_attempts: to_u32(profile.run_scheduler.attempts_per_work.q1_default)?,
            maximum_candidates: to_u32(profile.model_context_mcp.context_candidates.hard_max)?,
            maximum_items: to_u32(profile.model_context_mcp.context_items.hard_max)?,
            maximum_pages: to_u32(profile.model_context_mcp.context_pages.hard_max)?,
            maximum_result_bytes: profile.durable_quota.context_result_bytes.hard_max,
            inline_value_limits: JsonLimits {
                max_bytes: to_usize(profile.run_scheduler.inline_value_bytes.hard_max)?,
                max_depth: to_usize(profile.api.json_depth.hard_max)?,
                max_properties_per_object: to_usize(profile.api.json_properties.hard_max)?,
                max_items_per_array: to_usize(profile.api.json_items.hard_max)?,
                max_string_bytes: to_usize(profile.run_scheduler.inline_value_bytes.hard_max)?,
            },
            maximum_lease_milliseconds: profile.run_scheduler.lease_milliseconds.hard_max,
        };
        if limits.maximum_attempts == 0
            || limits.maximum_candidates == 0
            || limits.maximum_items == 0
            || limits.maximum_pages == 0
            || limits.maximum_result_bytes == 0
            || limits.maximum_lease_milliseconds == 0
        {
            return Err(ContextQueryError::InvalidLimits);
        }
        Ok(limits)
    }

    pub const fn maximum_attempts(self) -> u32 {
        self.maximum_attempts
    }

    pub const fn maximum_candidates(self) -> u32 {
        self.maximum_candidates
    }

    pub const fn maximum_items(self) -> u32 {
        self.maximum_items
    }

    pub const fn maximum_pages(self) -> u32 {
        self.maximum_pages
    }

    pub const fn maximum_result_bytes(self) -> u64 {
        self.maximum_result_bytes
    }

    pub const fn inline_value_limits(self) -> JsonLimits {
        self.inline_value_limits
    }

    pub const fn maximum_lease_milliseconds(self) -> u64 {
        self.maximum_lease_milliseconds
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextDatasetView {
    Generation {
        exact: ExactDatasetGenerationRef,
    },
    ExternalObservation {
        source_identity_digest: Sha256Digest,
    },
}

impl ContextDatasetView {
    pub fn validate(&self) -> Result<(), ContextQueryError> {
        match self {
            Self::Generation { exact } => exact
                .validate()
                .map_err(|_| ContextQueryError::InvalidDatasetView),
            Self::ExternalObservation { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextQueryRequest {
    pub schema_version: u32,
    pub input: ExactInvocationValueRef,
    pub input_artifact_link_id: Option<ResourceId>,
    pub normalized_query_digest: Sha256Digest,
    pub normalized_filter_digest: Sha256Digest,
    pub requested_projection: Vec<String>,
    pub query_bytes: u32,
    pub filter_bytes: u32,
    pub page_size: u32,
    pub page_ordinal: u32,
    pub cursor_digest: Option<Sha256Digest>,
}

impl ContextQueryRequest {
    pub fn validate_for(
        &self,
        interface: &ContextInterfaceResourceSpec,
        binding: &ContextBindingSnapshot,
        limits: ContextQueryLimits,
    ) -> Result<(), ContextQueryError> {
        self.input
            .validate()
            .map_err(|_| ContextQueryError::InvalidRequest)?;
        if self.schema_version != 1
            || self.input.schema_digest != interface.query_schema_digest
            || self.input_artifact_link_id.is_some()
                != matches!(
                    self.input.storage,
                    insight_platform_invocations::InvocationValueStorage::Artifact { .. }
                )
            || self
                .input_artifact_link_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ArtifactLink)
            || self.query_bytes == 0
            || self.query_bytes > interface.limits.maximum_query_bytes
            || self.filter_bytes > interface.limits.maximum_filter_bytes
            || self.page_size == 0
            || self.page_size > interface.pagination.maximum_page_size
            || self.page_size > limits.maximum_items()
            || self.page_ordinal >= limits.maximum_pages()
            || (self.page_ordinal == 0) != self.cursor_digest.is_none()
            || self.requested_projection.len() > binding.allowed_projection.len()
            || !is_sorted_unique(&self.requested_projection)
            || self
                .requested_projection
                .iter()
                .any(|field| !binding.allowed_projection.contains(field))
        {
            return Err(ContextQueryError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataAccessGrant {
    pub schema_version: u32,
    pub context_query_id: ResourceId,
    pub context_binding_id: ResourceId,
    pub context_binding_digest: Sha256Digest,
    pub context_deployment: ExactDeploymentRef,
    pub principal_digest: Sha256Digest,
    pub allowed_projection: Vec<String>,
    pub maximum_classification: DataClassification,
    pub authorization_policy: ExactVersionRef,
    pub policy_generation: u64,
    pub deadline: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl DataAccessGrant {
    pub fn build(
        context_query_id: ResourceId,
        binding: &ContextBindingSnapshot,
        principal: &PrincipalSnapshot,
        allowed_projection: Vec<String>,
        maximum_classification: DataClassification,
        policy_generation: u64,
        deadline: DateTime<Utc>,
    ) -> Result<Self, ContextQueryError> {
        let mut grant = Self {
            schema_version: 1,
            context_query_id,
            context_binding_id: binding.context_binding_id.clone(),
            context_binding_digest: binding.binding_digest.clone(),
            context_deployment: binding.context_deployment.clone(),
            principal_digest: principal.canonical_digest.clone(),
            allowed_projection,
            maximum_classification,
            authorization_policy: binding.authorization_policy.clone(),
            policy_generation,
            deadline,
            canonical_digest: binding.binding_digest.clone(),
        };
        grant.canonical_digest = digest_without_field(&grant, "canonical_digest")?;
        grant.validate_for(binding, principal)?;
        Ok(grant)
    }

    pub fn validate_for(
        &self,
        binding: &ContextBindingSnapshot,
        principal: &PrincipalSnapshot,
    ) -> Result<(), ContextQueryError> {
        if self.schema_version != 1
            || self.context_query_id.kind() != ResourceKind::ContextQuery
            || self.context_binding_id != binding.context_binding_id
            || self.context_binding_digest != binding.binding_digest
            || self.context_deployment != binding.context_deployment
            || self.principal_digest != principal.canonical_digest
            || self.authorization_policy != binding.authorization_policy
            || self.policy_generation == 0
            || !is_sorted_unique(&self.allowed_projection)
            || self
                .allowed_projection
                .iter()
                .any(|field| !binding.allowed_projection.contains(field))
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(ContextQueryError::InvalidGrant);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CitationLocator {
    ArtifactSpan {
        artifact: ArtifactRef,
        byte_start: u64,
        byte_end: u64,
        page: Option<u32>,
    },
    DocumentSection {
        document_identity_digest: Sha256Digest,
        section_path: Vec<String>,
    },
    CatalogObject {
        catalog_identity_digest: Sha256Digest,
        object_path: Vec<String>,
    },
    McpResource {
        resource_identity_digest: Sha256Digest,
        uri_digest: Sha256Digest,
    },
    RemoteOpaque {
        locator_digest: Sha256Digest,
    },
}

impl CitationLocator {
    pub const fn kind(&self) -> ContextLocatorKind {
        match self {
            Self::ArtifactSpan { .. } => ContextLocatorKind::ArtifactSpan,
            Self::DocumentSection { .. } => ContextLocatorKind::DocumentSection,
            Self::CatalogObject { .. } => ContextLocatorKind::CatalogObject,
            Self::McpResource { .. } => ContextLocatorKind::McpResource,
            Self::RemoteOpaque { .. } => ContextLocatorKind::RemoteOpaque,
        }
    }

    fn validate(&self) -> Result<(), ContextQueryError> {
        match self {
            Self::ArtifactSpan {
                artifact,
                byte_start,
                byte_end,
                page,
            } if artifact.validate().is_ok()
                && byte_start < byte_end
                && *byte_end <= artifact.byte_length()
                && page.is_none_or(|value| value > 0) =>
            {
                Ok(())
            }
            Self::DocumentSection { section_path, .. }
            | Self::CatalogObject {
                object_path: section_path,
                ..
            } if valid_locator_path(section_path) => Ok(()),
            Self::McpResource { .. } | Self::RemoteOpaque { .. } => Ok(()),
            _ => Err(ContextQueryError::InvalidCitation),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextCitation {
    pub context_deployment: ExactDeploymentRef,
    pub interface_revision: ExactVersionRef,
    pub dataset_view: ContextDatasetView,
    pub locator: CitationLocator,
    pub strength: insight_platform_contracts::ContextCitationStrength,
    pub content_digest: Sha256Digest,
    pub observed_at: DateTime<Utc>,
    pub display_label: String,
}

impl ContextCitation {
    pub fn validate_for(
        &self,
        admission: &ContextAdmissionSnapshot,
    ) -> Result<(), ContextQueryError> {
        self.dataset_view.validate()?;
        self.locator.validate()?;
        if self.context_deployment != admission.binding.context_deployment
            || self.interface_revision != admission.interface_revision
            || self.dataset_view != admission.dataset_view
            || !admission
                .interface
                .citation
                .allowed_strengths
                .contains(&self.strength)
            || !admission
                .interface
                .citation
                .locator_kinds
                .contains(&self.locator.kind())
            || self.display_label.is_empty()
            || self.display_label.len()
                > usize::try_from(admission.interface.citation.maximum_display_label_bytes)
                    .map_err(|_| ContextQueryError::InvalidLimits)?
            || self.display_label.chars().any(char::is_control)
        {
            return Err(ContextQueryError::InvalidCitation);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedContextScore {
    pub millionths: i32,
    pub score_domain_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextItem {
    pub item_id: ResourceId,
    pub source_item_identity_digest: Sha256Digest,
    pub content: ValueRef,
    pub structured_fields: ClosedJsonValue,
    pub score: Option<NormalizedContextScore>,
    pub classification: DataClassification,
    pub citation: ContextCitation,
    pub authorization_evidence_digest: Sha256Digest,
}

impl ContextItem {
    fn validate_for(
        &self,
        admission: &ContextAdmissionSnapshot,
        limits: ContextQueryLimits,
    ) -> Result<u64, ContextQueryError> {
        self.content
            .validate(limits.inline_value_limits())
            .map_err(|_| ContextQueryError::InvalidObservation)?;
        self.structured_fields
            .validate()
            .map_err(|_| ContextQueryError::InvalidObservation)?;
        self.citation.validate_for(admission)?;
        let content_digest = value_ref_digest(&self.content)?;
        let bytes = value_ref_bytes(&self.content)?;
        if self.item_id.kind() != ResourceKind::ContextItem
            || !matches!(self.content, ValueRef::Inline { .. })
            || content_digest != self.citation.content_digest
            || self.classification.rank() > admission.grant.maximum_classification.rank()
            || bytes > u64::from(admission.interface.limits.maximum_item_bytes)
            || self.score.as_ref().is_some_and(|score| {
                score.millionths < -1_000_000
                    || score.millionths > 1_000_000
                    || score.score_domain_digest != admission.interface.ranking.score_domain_digest
            })
        {
            return Err(ContextQueryError::InvalidObservation);
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextRetrievalEvidence {
    pub backend_request_digest: Sha256Digest,
    pub backend_response_digest: Sha256Digest,
    pub authorization_evidence_digest: Sha256Digest,
    pub ranking_evidence_digest: Sha256Digest,
    pub candidate_count: u32,
    pub rejected_count: u32,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextObservation {
    pub schema_version: u32,
    pub observation_id: ResourceId,
    pub context_query_id: ResourceId,
    pub dataset_view: ContextDatasetView,
    pub normalized_query_digest: Sha256Digest,
    pub items: Vec<ContextItem>,
    pub next_cursor_digest: Option<Sha256Digest>,
    pub evidence: ContextRetrievalEvidence,
    pub observed_at: DateTime<Utc>,
    pub total_bytes: u64,
    pub canonical_digest: Sha256Digest,
}

impl ContextObservation {
    pub fn validate_for(
        &self,
        query_id: &ResourceId,
        admission: &ContextAdmissionSnapshot,
        limits: ContextQueryLimits,
    ) -> Result<(), ContextQueryError> {
        self.dataset_view.validate()?;
        if self.schema_version != 1
            || self.observation_id.kind() != ResourceKind::ContextObservation
            || &self.context_query_id != query_id
            || self.dataset_view != admission.dataset_view
            || self.normalized_query_digest != admission.request.normalized_query_digest
            || self.items.len()
                > usize::try_from(
                    admission
                        .interface
                        .limits
                        .maximum_items
                        .min(limits.maximum_items()),
                )
                .map_err(|_| ContextQueryError::InvalidLimits)?
            || self.evidence.candidate_count > limits.maximum_candidates()
            || self.evidence.rejected_count > self.evidence.candidate_count
            || !self
                .items
                .windows(2)
                .all(|pair| pair[0].item_id < pair[1].item_id)
        {
            return Err(ContextQueryError::InvalidObservation);
        }
        let mut bytes = 0_u64;
        let mut source_identities = BTreeSet::new();
        for item in &self.items {
            bytes = bytes
                .checked_add(item.validate_for(admission, limits)?)
                .ok_or(ContextQueryError::CounterOverflow)?;
            if !source_identities.insert(&item.source_item_identity_digest) {
                return Err(ContextQueryError::InvalidObservation);
            }
        }
        if bytes != self.total_bytes
            || bytes > u64::from(admission.interface.limits.maximum_total_bytes)
            || bytes > admission.quota_ceiling.result_bytes
            || bytes > limits.maximum_result_bytes()
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(ContextQueryError::InvalidObservation);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextQuotaCeiling {
    pub concurrent_units: u64,
    pub queries: u64,
    pub result_bytes: u64,
}

impl ContextQuotaCeiling {
    pub fn validate(self, limits: ContextQueryLimits) -> Result<(), ContextQueryError> {
        if self.concurrent_units != 1
            || self.queries != 1
            || self.result_bytes == 0
            || self.result_bytes > limits.maximum_result_bytes()
        {
            return Err(ContextQueryError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextAdmissionSnapshot {
    pub schema_version: u32,
    pub context_query_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub slot_id: String,
    pub slot_binding_digest: Sha256Digest,
    pub run_bindings_digest: Sha256Digest,
    pub binding: ContextBindingSnapshot,
    pub context_closure: insight_platform_contracts::ContextDeploymentClosure,
    pub interface_revision: ExactVersionRef,
    pub interface: ContextInterfaceResourceSpec,
    pub implementation_revision: ExactVersionRef,
    pub implementation: ContextImplementationResourceSpec,
    pub dataset_view: ContextDatasetView,
    pub dataset_generation: Option<ContextDatasetGenerationSpec>,
    pub request: ContextQueryRequest,
    pub principal: PrincipalSnapshot,
    pub grant: DataAccessGrant,
    pub policies: Vec<ExactVersionRef>,
    pub quota_ceiling: ContextQuotaCeiling,
    pub attempt_limit: u32,
    pub deadline: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl ContextAdmissionSnapshot {
    pub fn validate(&self, limits: ContextQueryLimits) -> Result<(), ContextQueryError> {
        self.binding
            .validate()
            .map_err(|_| ContextQueryError::InvalidBinding)?;
        insight_platform_contracts::DeploymentClosure::ContextSourceInterface(
            self.context_closure.clone(),
        )
        .validate()
        .map_err(|_| ContextQueryError::InvalidBinding)?;
        insight_platform_contracts::ResourceDocument::ContextSourceInterface(
            self.interface.clone(),
        )
        .validate()
        .map_err(|_| ContextQueryError::InvalidAdmission)?;
        insight_platform_contracts::ResourceDocument::ContextSourceImplementation(
            self.implementation.clone(),
        )
        .validate()
        .map_err(|_| ContextQueryError::InvalidAdmission)?;
        self.implementation
            .contract
            .validate_binding(&self.context_closure.backend)
            .map_err(|_| ContextQueryError::InvalidBinding)?;
        self.dataset_view.validate()?;
        self.request
            .validate_for(&self.interface, &self.binding, limits)?;
        self.principal
            .validate()
            .map_err(|_| ContextQueryError::InvalidAdmission)?;
        self.grant.validate_for(&self.binding, &self.principal)?;
        self.quota_ceiling.validate(limits)?;
        validate_policies(&self.policies)?;
        if self.schema_version != 1
            || self.context_query_id.kind() != ResourceKind::ContextQuery
            || self.run_id.kind() != ResourceKind::Run
            || self.node_execution_id.kind() != ResourceKind::NodeExecution
            || !is_code(&self.slot_id)
            || self.interface_revision.resource_kind != ResourceKind::ContextSourceInterfaceRevision
            || self.implementation_revision.resource_kind
                != ResourceKind::ContextSourceImplementationRevision
            || self.context_closure.interface != self.interface_revision
            || self.context_closure.implementation != self.implementation_revision
            || self.implementation.interface_revision != self.interface_revision
            || self.implementation.backend_kind != self.context_closure.backend.kind()
            || self.implementation.backend_kind != self.implementation.contract.backend.kind()
            || !insight_platform_contracts::exact_secret_binding_purposes_match(
                &self.context_closure.secret_bindings,
                &self.implementation.contract.credential_requirements,
            )
            || !self
                .interface
                .allowed_consistency
                .contains(&self.binding.consistency.mode())
            || self.request.input.run_id != self.run_id
            || self.grant.context_query_id != self.context_query_id
            || self.attempt_limit == 0
            || self.attempt_limit > limits.maximum_attempts()
            || self.deadline != self.grant.deadline
            || !self.policies.contains(&self.binding.authorization_policy)
            || !self.policies.contains(&self.binding.ranking_policy)
            || !self.policies.contains(&self.context_closure.parser_policy)
            || !self.policies.contains(&self.context_closure.ranking_policy)
            || !self.policies.contains(&self.context_closure.data_policy)
            || self
                .context_closure
                .network_policy
                .as_ref()
                .is_some_and(|policy| !self.policies.contains(policy))
            || self.dataset_generation.is_some()
                != matches!(self.dataset_view, ContextDatasetView::Generation { .. })
            || self.dataset_generation.as_ref().is_some_and(|generation| {
                generation.validate().is_err()
                    || generation.context_deployment != self.binding.context_deployment
            })
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(ContextQueryError::InvalidAdmission);
        }
        Ok(())
    }
}

pub(crate) fn digest<T: Serialize>(value: &T) -> Result<Sha256Digest, ContextQueryError> {
    let value = serde_json::to_value(value).map_err(|_| ContextQueryError::Canonicalization)?;
    canonical_digest(&value)
        .map_err(|_| ContextQueryError::Canonicalization)?
        .parse()
        .map_err(|_| ContextQueryError::Canonicalization)
}

pub(crate) fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, ContextQueryError> {
    let mut value = serde_json::to_value(value).map_err(|_| ContextQueryError::Canonicalization)?;
    value
        .as_object_mut()
        .ok_or(ContextQueryError::Canonicalization)?
        .remove(field)
        .ok_or(ContextQueryError::Canonicalization)?;
    canonical_digest(&value)
        .map_err(|_| ContextQueryError::Canonicalization)?
        .parse()
        .map_err(|_| ContextQueryError::Canonicalization)
}

pub(crate) fn value_ref_digest(value: &ValueRef) -> Result<Sha256Digest, ContextQueryError> {
    match value {
        ValueRef::Inline { value } => {
            let digest =
                canonical_digest(value).map_err(|_| ContextQueryError::Canonicalization)?;
            digest
                .parse()
                .map_err(|_| ContextQueryError::Canonicalization)
        }
        ValueRef::Artifact { artifact } => Ok(artifact.content_digest().clone()),
    }
}

fn value_ref_bytes(value: &ValueRef) -> Result<u64, ContextQueryError> {
    match value {
        ValueRef::Inline { value } => u64::try_from(
            serde_json::to_vec(value)
                .map_err(|_| ContextQueryError::Canonicalization)?
                .len(),
        )
        .map_err(|_| ContextQueryError::CounterOverflow),
        ValueRef::Artifact { artifact } => Ok(artifact.byte_length()),
    }
}

fn validate_policies(values: &[ExactVersionRef]) -> Result<(), ContextQueryError> {
    if values.len() > 64
        || !values
            .windows(2)
            .all(|pair| pair[0].revision_id < pair[1].revision_id)
        || values.iter().any(|value| {
            value.resource_kind != ResourceKind::PolicyRevision || value.validate().is_err()
        })
    {
        return Err(ContextQueryError::InvalidAdmission);
    }
    Ok(())
}

fn is_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn is_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CONTEXT_SAFE_CODE_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn valid_locator_path(values: &[String]) -> bool {
    !values.is_empty()
        && values.len() <= MAX_CONTEXT_LOCATOR_SEGMENTS
        && values.iter().all(|value| {
            !value.is_empty()
                && value.len() <= MAX_CONTEXT_LOCATOR_SEGMENT_BYTES
                && !value.chars().any(char::is_control)
                && !value.contains('/')
                && !value.contains('\\')
        })
}
