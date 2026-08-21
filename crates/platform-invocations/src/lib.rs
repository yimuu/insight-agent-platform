//! Pure CapabilityInvocation admission and current-state decisions.
//!
//! This crate owns the logical invocation aggregate. It performs no I/O and never reads a wall
//! clock: repositories provide exact frozen registry facts and database-observed time, then
//! persist accepted decisions in caller-owned transactions.

#![allow(async_fn_in_trait)]

use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, validate_capability_credential_requirements, ArtifactRef,
    CapabilityArtifactContract, CapabilityBackendBinding, CapabilityBackendContract,
    CapabilityBackendFeatures, CapabilityBackendKind, CapabilityBackendLimits,
    CapabilityCancellationKind, CapabilityDataFlowPolicy, CapabilityDeploymentClosure,
    CapabilityIdempotencyKind, CapabilityInterfaceLimits, CapabilityName,
    CapabilityProgressContract, CapabilityProgressMode, CommandAudit, CommandOutcome,
    DataClassification, Effect, ExactDeploymentRef, ExactPolicyBinding, ExactVersionRef, Failure,
    FailureClass, FailureCode, FailureSource, FrozenSlotTarget, HardLimitProfile, InvocationState,
    JsonLimits, NodeExecutionState, Permission, PlanNodeKind, PlatformFailureCode,
    PrincipalSnapshot, ResourceId, ResourceKind, Retryability, RunBindingsSnapshot, RunState,
    SecretPurpose, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

const MAX_SLOT_ID_BYTES: usize = 128;
const MAX_VALUE_KIND_BYTES: usize = 64;
const MAX_SAFE_PROMPT_KEY_BYTES: usize = 128;

mod execution;
pub use execution::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvocationCommandLimits {
    maximum_attempts: u32,
    inline_value_limits: JsonLimits,
    maximum_progress_events: u32,
    maximum_progress_event_bytes: u32,
    maximum_remote_state_bytes: u32,
    maximum_poll_count: u32,
    maximum_deferred_poll_milliseconds: u64,
    maximum_lease_milliseconds: u64,
}

impl InvocationCommandLimits {
    pub fn new(maximum_attempts: u32) -> Result<Self, InvocationError> {
        if maximum_attempts == 0 {
            return Err(InvocationError::InvalidLimits);
        }
        Ok(Self {
            maximum_attempts,
            inline_value_limits: JsonLimits::CONTRACT_FIXTURE,
            maximum_progress_events: 10_000,
            maximum_progress_event_bytes: 1_048_576,
            maximum_remote_state_bytes: 1_048_576,
            maximum_poll_count: 10_000,
            maximum_deferred_poll_milliseconds: 300_000,
            maximum_lease_milliseconds: 60_000,
        })
    }

    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, InvocationError> {
        profile
            .validate()
            .map_err(|_| InvocationError::InvalidLimits)?;
        let to_usize =
            |value: u64| usize::try_from(value).map_err(|_| InvocationError::InvalidLimits);
        let to_u32 = |value: u64| u32::try_from(value).map_err(|_| InvocationError::InvalidLimits);
        let limits = Self {
            maximum_attempts: to_u32(profile.run_scheduler.attempts_per_work.q1_default)?,
            inline_value_limits: JsonLimits {
                max_bytes: to_usize(profile.run_scheduler.inline_value_bytes.hard_max)?,
                max_depth: to_usize(profile.api.json_depth.hard_max)?,
                max_properties_per_object: to_usize(profile.api.json_properties.hard_max)?,
                max_items_per_array: to_usize(profile.api.json_items.hard_max)?,
                max_string_bytes: to_usize(profile.run_scheduler.inline_value_bytes.hard_max)?,
            },
            maximum_progress_events: to_u32(profile.capability_sandbox.progress_events.hard_max)?,
            maximum_progress_event_bytes: to_u32(profile.api.sse_event_bytes.hard_max)?,
            maximum_remote_state_bytes: to_u32(profile.model_context_mcp.response_bytes.hard_max)?,
            maximum_poll_count: to_u32(profile.capability_sandbox.progress_events.hard_max)?,
            maximum_deferred_poll_milliseconds: profile
                .run_scheduler
                .deferred_poll_max_milliseconds
                .hard_max,
            maximum_lease_milliseconds: profile.run_scheduler.lease_milliseconds.hard_max,
        };
        if limits.maximum_attempts == 0
            || limits.maximum_progress_events == 0
            || limits.maximum_progress_event_bytes == 0
            || limits.maximum_remote_state_bytes == 0
            || limits.maximum_poll_count == 0
            || limits.maximum_deferred_poll_milliseconds == 0
            || limits.maximum_lease_milliseconds == 0
        {
            return Err(InvocationError::InvalidLimits);
        }
        Ok(limits)
    }

    pub const fn maximum_attempts(self) -> u32 {
        self.maximum_attempts
    }

    pub const fn inline_value_limits(self) -> JsonLimits {
        self.inline_value_limits
    }

    pub const fn maximum_progress_events(self) -> u32 {
        self.maximum_progress_events
    }

    pub const fn maximum_progress_event_bytes(self) -> u32 {
        self.maximum_progress_event_bytes
    }

    pub const fn maximum_remote_state_bytes(self) -> u32 {
        self.maximum_remote_state_bytes
    }

    pub const fn maximum_poll_count(self) -> u32 {
        self.maximum_poll_count
    }

    pub const fn maximum_deferred_poll_milliseconds(self) -> u64 {
        self.maximum_deferred_poll_milliseconds
    }

    pub const fn maximum_lease_milliseconds(self) -> u64 {
        self.maximum_lease_milliseconds
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InvocationOrigin {
    PlanNode {
        node_execution_id: ResourceId,
    },
    ModelToolCall {
        model_turn_id: ResourceId,
        model_call_id_digest: Sha256Digest,
    },
}

impl InvocationOrigin {
    pub fn validate(&self) -> Result<(), InvocationError> {
        match self {
            Self::PlanNode { node_execution_id }
                if node_execution_id.kind() == ResourceKind::NodeExecution =>
            {
                Ok(())
            }
            Self::ModelToolCall { model_turn_id, .. }
                if model_turn_id.kind() == ResourceKind::ModelTurn =>
            {
                Ok(())
            }
            _ => Err(InvocationError::InvalidOrigin),
        }
    }

    pub fn validate_for(&self, node_execution_id: &ResourceId) -> Result<(), InvocationError> {
        if node_execution_id.kind() != ResourceKind::NodeExecution {
            return Err(InvocationError::InvalidIdentity);
        }
        match self {
            Self::PlanNode {
                node_execution_id: origin_node,
            } if origin_node == node_execution_id => Ok(()),
            Self::ModelToolCall { model_turn_id, .. }
                if model_turn_id.kind() == ResourceKind::ModelTurn =>
            {
                Ok(())
            }
            _ => Err(InvocationError::InvalidOrigin),
        }
    }

    pub const fn required_node_kind(&self) -> PlanNodeKind {
        match self {
            Self::PlanNode { .. } => PlanNodeKind::CapabilityCall,
            Self::ModelToolCall { .. } => PlanNodeKind::ModelLoop,
        }
    }

    pub const fn owner_kind(&self) -> ResourceKind {
        match self {
            Self::PlanNode { .. } => ResourceKind::NodeExecution,
            Self::ModelToolCall { .. } => ResourceKind::ModelTurn,
        }
    }

    pub fn owner_id(&self) -> &ResourceId {
        match self {
            Self::PlanNode { node_execution_id } => node_execution_id,
            Self::ModelToolCall { model_turn_id, .. } => model_turn_id,
        }
    }

    pub fn logical_key(&self) -> String {
        match self {
            Self::PlanNode { node_execution_id } => format!("plan:{node_execution_id}"),
            Self::ModelToolCall {
                model_turn_id,
                model_call_id_digest,
            } => format!("model:{model_turn_id}:{model_call_id_digest}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InvocationValueStorage {
    Inline,
    Artifact { artifact: ArtifactRef },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactInvocationValueRef {
    pub schema_version: u32,
    pub value_id: ResourceId,
    pub run_id: ResourceId,
    pub producing_node_id: Option<ResourceId>,
    pub value_kind: String,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub storage: InvocationValueStorage,
}

impl ExactInvocationValueRef {
    pub fn validate(&self) -> Result<(), InvocationError> {
        if self.schema_version != 1
            || self.value_id.kind() != ResourceKind::RunValue
            || self.run_id.kind() != ResourceKind::Run
            || self
                .producing_node_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::NodeExecution)
            || !is_stable_code(&self.value_kind, MAX_VALUE_KIND_BYTES)
        {
            return Err(InvocationError::InvalidInputValue);
        }
        match &self.storage {
            InvocationValueStorage::Inline => Ok(()),
            InvocationValueStorage::Artifact { artifact }
                if artifact.validate().is_ok()
                    && artifact.content_digest() == &self.content_digest
                    && artifact.classification() == self.classification =>
            {
                Ok(())
            }
            InvocationValueStorage::Artifact { .. } => Err(InvocationError::InvalidInputValue),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CapabilityExecutionInputMaterial {
    Inline { value: serde_json::Value },
    LinkedArtifact { artifact_link_id: ResourceId },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityExecutionInput {
    pub exact: ExactInvocationValueRef,
    pub material: CapabilityExecutionInputMaterial,
}

impl CapabilityExecutionInput {
    pub fn validate(&self) -> Result<(), InvocationError> {
        self.exact.validate()?;
        match (&self.exact.storage, &self.material) {
            (
                InvocationValueStorage::Inline,
                CapabilityExecutionInputMaterial::Inline { value },
            ) => {
                let actual: Sha256Digest = canonical_digest(value)
                    .map_err(|_| InvocationError::Canonicalization)?
                    .parse()
                    .map_err(|_| InvocationError::Canonicalization)?;
                if actual != self.exact.content_digest {
                    return Err(InvocationError::InvalidInputValue);
                }
            }
            (
                InvocationValueStorage::Artifact { .. },
                CapabilityExecutionInputMaterial::LinkedArtifact { artifact_link_id },
            ) if artifact_link_id.kind() == ResourceKind::ArtifactLink => {}
            _ => return Err(InvocationError::InvalidInputValue),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationSelectionEvidence {
    pub schema_version: u32,
    pub candidate_set_digest: Sha256Digest,
    pub selector_input_digest: Sha256Digest,
    pub selected_candidate_ordinal: u16,
    pub canonical_digest: Sha256Digest,
}

impl InvocationSelectionEvidence {
    pub fn build(
        candidates: &[ExactDeploymentRef],
        selected_candidate_ordinal: u16,
        selector_input_digest: Sha256Digest,
    ) -> Result<Self, InvocationError> {
        let ordinal = usize::from(selected_candidate_ordinal);
        if candidates.is_empty() || ordinal >= candidates.len() {
            return Err(InvocationError::InvalidSelection);
        }
        let candidate_set_digest = digest(&serde_json::json!({
            "candidates": candidates,
            "schema_version": 1,
        }))?;
        let mut evidence = Self {
            schema_version: 1,
            candidate_set_digest,
            selector_input_digest,
            selected_candidate_ordinal,
            canonical_digest: candidates[ordinal].deployment_digest.clone(),
        };
        evidence.canonical_digest = digest_without_field(&evidence, "canonical_digest")?;
        Ok(evidence)
    }

    pub fn validate_for(
        &self,
        candidates: &[ExactDeploymentRef],
        selected: &ExactDeploymentRef,
    ) -> Result<(), InvocationError> {
        let rebuilt = Self::build(
            candidates,
            self.selected_candidate_ordinal,
            self.selector_input_digest.clone(),
        )?;
        let ordinal = usize::from(self.selected_candidate_ordinal);
        if &rebuilt != self || candidates.get(ordinal) != Some(selected) {
            return Err(InvocationError::InvalidSelection);
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), InvocationError> {
        if self.schema_version != 1
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(InvocationError::InvalidSelection);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationPolicyDisposition {
    Allowed,
    ApprovalRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationPolicyDecision {
    pub policy: ExactVersionRef,
    pub disposition: InvocationPolicyDisposition,
    pub evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationApprovalRequirement {
    pub policy_revision: ExactVersionRef,
    pub eligible_principal_rule_digest: Sha256Digest,
    pub safe_prompt_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationPolicyDecisionBundle {
    pub schema_version: u32,
    pub decisions: Vec<InvocationPolicyDecision>,
    pub approval: Option<InvocationApprovalRequirement>,
    pub canonical_digest: Sha256Digest,
}

impl InvocationPolicyDecisionBundle {
    pub fn build(
        mut decisions: Vec<InvocationPolicyDecision>,
        approval: Option<InvocationApprovalRequirement>,
    ) -> Result<Self, InvocationError> {
        decisions.sort_by(|left, right| left.policy.revision_id.cmp(&right.policy.revision_id));
        let mut bundle = Self {
            schema_version: 1,
            canonical_digest: decisions.first().map_or_else(
                || digest(&serde_json::json!({"empty": true})),
                |decision| Ok(decision.evidence_digest.clone()),
            )?,
            decisions,
            approval,
        };
        bundle.validate_shape()?;
        bundle.canonical_digest = digest_without_field(&bundle, "canonical_digest")?;
        Ok(bundle)
    }

    pub fn validate_for(
        &self,
        expected_policies: &[ExactVersionRef],
    ) -> Result<(), InvocationError> {
        self.validate_shape()?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(InvocationError::InvalidPolicyBundle);
        }
        let actual = self
            .decisions
            .iter()
            .map(|decision| decision.policy.clone())
            .collect::<Vec<_>>();
        let mut expected = expected_policies.to_vec();
        expected.sort_by(|left, right| left.revision_id.cmp(&right.revision_id));
        expected.dedup_by(|left, right| left.revision_id == right.revision_id);
        if actual != expected {
            return Err(InvocationError::InvalidPolicyBundle);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), InvocationError> {
        if self.schema_version != 1
            || !self
                .decisions
                .windows(2)
                .all(|pair| pair[0].policy.revision_id < pair[1].policy.revision_id)
            || self.decisions.iter().any(|decision| {
                decision.policy.validate().is_err()
                    || decision.policy.resource_kind != ResourceKind::PolicyRevision
            })
        {
            return Err(InvocationError::InvalidPolicyBundle);
        }
        let required = self
            .decisions
            .iter()
            .filter(|decision| {
                decision.disposition == InvocationPolicyDisposition::ApprovalRequired
            })
            .collect::<Vec<_>>();
        match (&self.approval, required.as_slice()) {
            (None, []) => Ok(()),
            (Some(approval), [decision])
                if approval.policy_revision == decision.policy
                    && is_stable_code(&approval.safe_prompt_key, MAX_SAFE_PROMPT_KEY_BYTES) =>
            {
                Ok(())
            }
            _ => Err(InvocationError::InvalidPolicyBundle),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityInterfaceContract {
    pub revision: ExactVersionRef,
    pub qualified_name: CapabilityName,
    pub input_schema_digest: Sha256Digest,
    pub output_schema_digest: Sha256Digest,
    pub error_schema_digest: Sha256Digest,
    pub artifacts: CapabilityArtifactContract,
    pub data_policy: CapabilityDataFlowPolicy,
    pub execution_limits: CapabilityInterfaceLimits,
    pub effect: Effect,
    pub idempotency: CapabilityIdempotencyKind,
    pub cancellation: CapabilityCancellationKind,
    pub progress: CapabilityProgressContract,
}

impl CapabilityInterfaceContract {
    pub fn validate(&self) -> Result<(), InvocationError> {
        if self.revision.validate().is_err()
            || self.revision.resource_kind != ResourceKind::CapabilityInterfaceRevision
            || CapabilityName::new(self.qualified_name.as_str()).is_err()
            || self.artifacts.validate().is_err()
            || self.data_policy.validate().is_err()
            || self.execution_limits.validate().is_err()
            || self.artifacts.maximum_artifact_count().ok()
                != Some(self.execution_limits.maximum_artifacts)
            || self.progress.validate().is_err()
        {
            return Err(InvocationError::InvalidInterface);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityImplementationContract {
    pub revision: ExactVersionRef,
    pub interface_revision: ExactVersionRef,
    pub backend_kind: CapabilityBackendKind,
    pub backend_contract: CapabilityBackendContract,
    pub backend_contract_digest: Sha256Digest,
    pub credential_requirements: Vec<SecretPurpose>,
    pub backend_limits: CapabilityBackendLimits,
    pub features: CapabilityBackendFeatures,
}

impl CapabilityImplementationContract {
    pub fn validate(&self) -> Result<(), InvocationError> {
        if self.revision.validate().is_err()
            || self.interface_revision.validate().is_err()
            || self.revision.resource_kind != ResourceKind::CapabilityImplementationRevision
            || self.interface_revision.resource_kind != ResourceKind::CapabilityInterfaceRevision
            || self.features.validate().is_err()
            || self.backend_limits.validate().is_err()
            || self.backend_contract.validate(&self.features).is_err()
            || validate_capability_credential_requirements(&self.credential_requirements).is_err()
            || self.backend_kind != self.backend_contract.kind()
            || self.backend_contract.canonical_digest().ok().as_ref()
                != Some(&self.backend_contract_digest)
        {
            return Err(InvocationError::InvalidImplementation);
        }
        Ok(())
    }
}

/// Exact immutable contract handed from the durable claim transaction to a Capability Worker.
///
/// It intentionally carries no plaintext secret. Exact binding generation, purpose, provider and
/// resolution policy remain opaque authority metadata and are resolved by the role-scoped secret
/// broker immediately before I/O.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityExecutionContract {
    pub schema_version: u32,
    pub deployment: ExactDeploymentRef,
    pub deployment_closure: CapabilityDeploymentClosure,
    pub implementation: CapabilityImplementationContract,
    pub canonical_digest: Sha256Digest,
}

impl CapabilityExecutionContract {
    pub fn build(
        deployment: ExactDeploymentRef,
        deployment_closure: CapabilityDeploymentClosure,
        implementation: CapabilityImplementationContract,
    ) -> Result<Self, InvocationError> {
        let mut contract = Self {
            schema_version: 1,
            deployment,
            deployment_closure,
            implementation,
            canonical_digest: digest(&serde_json::json!({"empty": true}))?,
        };
        contract.validate_shape()?;
        contract.canonical_digest = digest_without_field(&contract, "canonical_digest")?;
        Ok(contract)
    }

    pub fn validate_for(
        &self,
        admission: &CapabilityAdmissionSnapshot,
    ) -> Result<(), InvocationError> {
        self.validate_shape()?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest
            || self.deployment != admission.deployment
            || self.deployment_closure.interface != admission.interface
            || self.deployment_closure.implementation != admission.implementation
            || self.implementation.revision != admission.implementation
            || self.implementation.interface_revision != admission.interface
            || self.implementation.backend_kind != admission.backend_kind
            || self.implementation.backend_contract_digest != admission.backend_contract_digest
            || self.implementation.features != admission.implementation_features
        {
            return Err(InvocationError::InvalidDeployment);
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), InvocationError> {
        self.validate_shape()?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(InvocationError::InvalidDeployment);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), InvocationError> {
        self.deployment
            .validate()
            .map_err(|_| InvocationError::InvalidDeployment)?;
        self.deployment_closure
            .validate()
            .map_err(|_| InvocationError::InvalidDeployment)?;
        self.implementation.validate()?;
        if self.schema_version != 1
            || self.deployment.resource_kind != ResourceKind::CapabilityDeployment
            || self.deployment_closure.interface != self.implementation.interface_revision
            || self.deployment_closure.implementation != self.implementation.revision
            || self
                .deployment_closure
                .backend
                .validate_for(&self.implementation.backend_contract)
                .is_err()
            || !insight_platform_contracts::exact_secret_binding_purposes_match(
                &self.deployment_closure.secret_bindings,
                &self.implementation.credential_requirements,
            )
        {
            return Err(InvocationError::InvalidDeployment);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityAdmissionSnapshot {
    pub schema_version: u32,
    pub origin_key: InvocationOrigin,
    pub slot_id: String,
    pub slot_binding_digest: Sha256Digest,
    pub run_bindings_digest: Sha256Digest,
    pub selection_policy: ExactPolicyBinding,
    pub selection_evidence: InvocationSelectionEvidence,
    pub deployment: ExactDeploymentRef,
    pub interface: ExactVersionRef,
    pub capability_name: CapabilityName,
    pub implementation: ExactVersionRef,
    pub backend_kind: CapabilityBackendKind,
    pub backend_contract_digest: Sha256Digest,
    pub mcp_runtime: Option<McpCapabilityRuntimeBinding>,
    pub input: ExactInvocationValueRef,
    pub input_artifact_link_id: Option<ResourceId>,
    pub effect: Effect,
    pub idempotency: CapabilityIdempotencyKind,
    pub cancellation: CapabilityCancellationKind,
    pub progress: CapabilityProgressContract,
    pub implementation_features: CapabilityBackendFeatures,
    pub input_schema_digest: Sha256Digest,
    pub output_schema_digest: Sha256Digest,
    pub error_schema_digest: Sha256Digest,
    pub artifact_contract: CapabilityArtifactContract,
    pub data_flow_policy: CapabilityDataFlowPolicy,
    pub interface_limits: CapabilityInterfaceLimits,
    pub policies: InvocationPolicyDecisionBundle,
    pub principal: PrincipalSnapshot,
    pub effect_key_digest: Sha256Digest,
    pub idempotency_key_digest: Sha256Digest,
    pub attempt_limit: u32,
    pub retry_backoff_milliseconds: u64,
    pub deadline: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl CapabilityAdmissionSnapshot {
    pub fn validate(&self) -> Result<(), InvocationError> {
        self.origin_key.validate()?;
        self.selection_evidence.validate()?;
        self.policies.validate_shape()?;
        if digest_without_field(&self.policies, "canonical_digest")?
            != self.policies.canonical_digest
        {
            return Err(InvocationError::InvalidPolicyBundle);
        }
        self.selection_policy
            .validate()
            .map_err(|_| InvocationError::InvalidSelection)?;
        self.deployment
            .validate()
            .map_err(|_| InvocationError::InvalidDeployment)?;
        self.interface
            .validate()
            .map_err(|_| InvocationError::InvalidInterface)?;
        self.implementation
            .validate()
            .map_err(|_| InvocationError::InvalidImplementation)?;
        self.input.validate()?;
        self.progress
            .validate()
            .map_err(|_| InvocationError::InvalidInterface)?;
        self.artifact_contract
            .validate()
            .map_err(|_| InvocationError::InvalidInterface)?;
        self.data_flow_policy
            .validate()
            .map_err(|_| InvocationError::InvalidInterface)?;
        self.interface_limits
            .validate()
            .map_err(|_| InvocationError::InvalidInterface)?;
        self.implementation_features
            .validate()
            .map_err(|_| InvocationError::InvalidImplementation)?;
        self.principal
            .validate()
            .map_err(|_| InvocationError::InvalidPrincipal)?;
        if self.schema_version != 1
            || !is_stable_code(&self.slot_id, MAX_SLOT_ID_BYTES)
            || self.deployment.resource_kind != ResourceKind::CapabilityDeployment
            || self.interface.resource_kind != ResourceKind::CapabilityInterfaceRevision
            || CapabilityName::new(self.capability_name.as_str()).is_err()
            || self.implementation.resource_kind != ResourceKind::CapabilityImplementationRevision
            || (self.backend_kind == CapabilityBackendKind::Mcp) != self.mcp_runtime.is_some()
            || self
                .mcp_runtime
                .as_ref()
                .is_some_and(|binding| binding.validate_for(&self.principal).is_err())
            || self.implementation_features.progress
                != (self.progress.mode == CapabilityProgressMode::Events)
            || self.input.schema_digest != self.input_schema_digest
            || !self
                .data_flow_policy
                .permits_input(self.input.classification)
            || self.artifact_contract.maximum_artifact_count().ok()
                != Some(self.interface_limits.maximum_artifacts)
            || matches!(&self.input.storage, InvocationValueStorage::Artifact { .. })
                != self.input_artifact_link_id.is_some()
            || self
                .input_artifact_link_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ArtifactLink)
            || self.attempt_limit == 0
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > 60_000
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(InvocationError::InvalidAdmission);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityInvocationResult {
    pub schema_version: u32,
    pub output: ExactInvocationValueRef,
    pub result_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityReconciliationSnapshot {
    pub schema_version: u32,
    pub effect: Effect,
    pub last_job_generation: u64,
    pub external_identity_digest: Option<Sha256Digest>,
    pub observation_digest: Sha256Digest,
    pub policy_path_digest: Sha256Digest,
    pub manual: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityInvocationPayload {
    pub schema_version: u32,
    pub admission: CapabilityAdmissionSnapshot,
    pub current_job_id: Option<ResourceId>,
    pub approval_task_id: Option<ResourceId>,
    pub input_task_id: Option<ResourceId>,
    pub detached_pending: Option<CapabilityDetachedPending>,
    pub result: Option<CapabilityInvocationResult>,
    pub failure: Option<Failure>,
    pub reconciliation: Option<CapabilityReconciliationSnapshot>,
}

impl CapabilityInvocationPayload {
    pub fn validate_for(&self, state: InvocationState) -> Result<(), InvocationError> {
        self.admission.validate()?;
        if self.schema_version != 1
            || self
                .current_job_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Job)
            || self
                .approval_task_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ApprovalTask)
            || self
                .input_task_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Interaction)
            || self
                .failure
                .as_ref()
                .is_some_and(|failure| failure.validate(1_024).is_err())
            || self
                .detached_pending
                .as_ref()
                .is_some_and(|pending| match pending {
                    CapabilityDetachedPending::RemoteTask { .. } => false,
                    CapabilityDetachedPending::InputRequired { resolution, .. } => resolution
                        .as_ref()
                        .is_some_and(|resolution| resolution.validate().is_err()),
                })
        {
            return Err(InvocationError::InvalidCurrentState);
        }
        if let Some(result) = &self.result {
            result.output.validate()?;
            if result.schema_version != 1
                || result.output.run_id != self.admission.input.run_id
                || result.output.schema_digest != self.admission.output_schema_digest
            {
                return Err(InvocationError::InvalidCurrentState);
            }
        }
        if self.reconciliation.as_ref().is_some_and(|snapshot| {
            snapshot.schema_version != 1
                || snapshot.effect != self.admission.effect
                || snapshot.last_job_generation == 0
        }) {
            return Err(InvocationError::InvalidCurrentState);
        }
        let valid = match state {
            InvocationState::Created => {
                self.current_job_id.is_none()
                    && self.detached_pending.is_none()
                    && self.result.is_none()
                    && self.failure.is_none()
                    && self.reconciliation.is_none()
            }
            InvocationState::AwaitingApproval => {
                self.approval_task_id.is_some()
                    && self.current_job_id.is_none()
                    && self.detached_pending.is_none()
                    && self.result.is_none()
                    && self.failure.is_none()
            }
            InvocationState::RetryScheduled => {
                self.input_task_id.is_none()
                    && self.detached_pending.is_none()
                    && self.result.is_none()
                    && self.failure.is_none()
                    && self.reconciliation.is_none()
            }
            InvocationState::Ready => {
                self.input_task_id.is_none()
                    && !matches!(
                        &self.detached_pending,
                        Some(CapabilityDetachedPending::InputRequired {
                            resolution: None,
                            ..
                        }) | Some(CapabilityDetachedPending::RemoteTask { .. })
                    )
                    && self.result.is_none()
                    && self.failure.is_none()
                    && self.reconciliation.is_none()
            }
            InvocationState::InFlight | InvocationState::Cancelling => {
                self.current_job_id.is_some()
                    && self.detached_pending.is_none()
                    && self.result.is_none()
                    && self.failure.is_none()
            }
            InvocationState::Deferred => {
                self.current_job_id.is_some()
                    && !matches!(
                        self.detached_pending,
                        Some(CapabilityDetachedPending::InputRequired { .. })
                    )
                    && self.result.is_none()
                    && self.failure.is_none()
            }
            InvocationState::AwaitingInput => {
                self.current_job_id.is_some()
                    && self.input_task_id.is_some()
                    && match &self.detached_pending {
                        // Ordinary remote Capability execution keeps the exact input request in
                        // its fenced Job payload. The Invocation owns only the interaction ID.
                        None => true,
                        // A detached Managed MCP Sandbox Job is already terminal, so its logical
                        // continuation must remain on the Invocation until the next Job exists.
                        Some(CapabilityDetachedPending::InputRequired {
                            request,
                            resolution: None,
                            ..
                        }) => self.input_task_id.as_ref() == Some(&request.input_task_id),
                        Some(
                            CapabilityDetachedPending::InputRequired {
                                resolution: Some(_),
                                ..
                            }
                            | CapabilityDetachedPending::RemoteTask { .. },
                        ) => false,
                    }
                    && self.result.is_none()
                    && self.failure.is_none()
            }
            InvocationState::ReconciliationRequired => {
                self.current_job_id.is_some()
                    && self.detached_pending.is_none()
                    && self.reconciliation.is_some()
                    && self.result.is_none()
            }
            InvocationState::Succeeded => {
                self.detached_pending.is_none() && self.result.is_some() && self.failure.is_none()
            }
            InvocationState::Failed => {
                self.detached_pending.is_none() && self.failure.is_some() && self.result.is_none()
            }
            InvocationState::Cancelled | InvocationState::TimedOut => {
                self.detached_pending.is_none() && self.result.is_none()
            }
        };
        if !valid {
            return Err(InvocationError::InvalidCurrentState);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityInvocationRecord {
    pub tenant_id: ResourceId,
    pub invocation_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub owner_kind: ResourceKind,
    pub owner_id: ResourceId,
    pub logical_key: String,
    pub deployment_id: ResourceId,
    pub input_value_id: ResourceId,
    pub output_value_id: Option<ResourceId>,
    pub effect_key_digest: Sha256Digest,
    pub state: InvocationState,
    pub version: u64,
    pub payload: CapabilityInvocationPayload,
    pub deadline: DateTime<Utc>,
    pub retry_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CapabilityInvocationRecord {
    pub fn validate(&self) -> Result<(), InvocationError> {
        self.payload.validate_for(self.state)?;
        self.payload
            .admission
            .origin_key
            .validate_for(&self.node_execution_id)?;
        let terminal = matches!(
            self.state,
            InvocationState::Succeeded
                | InvocationState::Failed
                | InvocationState::Cancelled
                | InvocationState::TimedOut
        );
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.run_id.kind() != ResourceKind::Run
            || self.node_execution_id.kind() != ResourceKind::NodeExecution
            || self.owner_kind != self.payload.admission.origin_key.owner_kind()
            || self.owner_id != *self.payload.admission.origin_key.owner_id()
            || self.logical_key != self.payload.admission.origin_key.logical_key()
            || self.deployment_id != self.payload.admission.deployment.deployment_id
            || self.input_value_id != self.payload.admission.input.value_id
            || self.output_value_id
                != self
                    .payload
                    .result
                    .as_ref()
                    .map(|result| result.output.value_id.clone())
            || self.effect_key_digest != self.payload.admission.effect_key_digest
            || self.deadline != self.payload.admission.deadline
            || self.version == 0
            || terminal != self.terminal_at.is_some()
            || self.updated_at < self.created_at
            || self.deadline < self.created_at
        {
            return Err(InvocationError::InvalidCurrentState);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AdmitCapabilityInvocation {
    pub audit: CommandAudit,
    pub invocation_id: ResourceId,
    pub run_id: ResourceId,
    pub node_execution_id: ResourceId,
    pub expected_run_version: u64,
    pub expected_node_version: u64,
    pub slot_id: String,
    pub input_value_id: ResourceId,
    pub input_artifact_link_id: Option<ResourceId>,
    pub origin: InvocationOrigin,
    pub selected_candidate_ordinal: u16,
    pub selector_input_digest: Sha256Digest,
    pub policy_decisions: InvocationPolicyDecisionBundle,
    pub approval_task_id: Option<ResourceId>,
    pub requested_attempt_limit: u32,
    pub requested_retry_backoff_milliseconds: u64,
    pub mcp_runtime: Option<McpCapabilityRuntimeRequest>,
}

impl AdmitCapabilityInvocation {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
        limits: InvocationCommandLimits,
    ) -> Result<(), InvocationError> {
        self.audit
            .validate_at(now)
            .map_err(|_| InvocationError::InvalidAudit)?;
        self.origin.validate_for(&self.node_execution_id)?;
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.run_id.kind() != ResourceKind::Run
            || self.node_execution_id.kind() != ResourceKind::NodeExecution
            || self.input_value_id.kind() != ResourceKind::RunValue
            || self
                .input_artifact_link_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ArtifactLink)
            || self.expected_run_version == 0
            || self.expected_node_version == 0
            || !is_stable_code(&self.slot_id, MAX_SLOT_ID_BYTES)
            || self.requested_attempt_limit == 0
            || self.requested_attempt_limit > limits.maximum_attempts
            || self.requested_retry_backoff_milliseconds == 0
            || self.requested_retry_backoff_milliseconds > 60_000
            || self.approval_task_id.is_some() != self.policy_decisions.approval.is_some()
            || self
                .approval_task_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ApprovalTask)
            || self.mcp_runtime.as_ref().is_some_and(|binding| {
                binding.mcp_operation_id.kind() != ResourceKind::McpOperation
                    || binding.authorization_binding_id.kind()
                        != ResourceKind::McpAuthorizationBinding
            })
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpCapabilityRuntimeRequest {
    pub mcp_operation_id: ResourceId,
    pub authorization_binding_id: ResourceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpCapabilityRuntimeBinding {
    pub schema_version: u32,
    pub mcp_operation_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub discovery_snapshot_id: ResourceId,
    pub discovery_snapshot_digest: Sha256Digest,
    pub authorization_binding_id: ResourceId,
    pub authorization_generation: u64,
    pub authorization_context_digest: Sha256Digest,
    pub principal_id: ResourceId,
}

impl McpCapabilityRuntimeBinding {
    pub fn validate(&self) -> Result<(), InvocationError> {
        self.mcp_deployment
            .validate()
            .map_err(|_| InvocationError::InvalidDeployment)?;
        if self.schema_version != 1
            || self.mcp_operation_id.kind() != ResourceKind::McpOperation
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.discovery_snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.authorization_generation == 0
            || self.principal_id.kind() != ResourceKind::Principal
        {
            return Err(InvocationError::InvalidAdmission);
        }
        Ok(())
    }

    pub fn validate_for(&self, principal: &PrincipalSnapshot) -> Result<(), InvocationError> {
        self.validate()?;
        if self.principal_id != principal.principal_id {
            return Err(InvocationError::InvalidAdmission);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CapabilityAdmissionFacts {
    pub run_state: RunState,
    pub run_version: u64,
    pub run_pause_requested: bool,
    pub run_cancel_requested: bool,
    pub run_timeout_requested: bool,
    pub run_deadline: DateTime<Utc>,
    pub run_bindings: RunBindingsSnapshot,
    pub node_state: NodeExecutionState,
    pub node_version: u64,
    pub node_kind: PlanNodeKind,
    pub node_deadline: DateTime<Utc>,
    pub deployment: ExactDeploymentRef,
    pub deployment_closure: CapabilityDeploymentClosure,
    pub interface: CapabilityInterfaceContract,
    pub implementation: CapabilityImplementationContract,
    pub input: ExactInvocationValueRef,
    pub principal: PrincipalSnapshot,
    pub mcp_runtime: Option<McpCapabilityRuntimeBinding>,
    pub database_now: DateTime<Utc>,
}

fn valid_mcp_runtime_binding(
    command: &AdmitCapabilityInvocation,
    facts: &CapabilityAdmissionFacts,
) -> bool {
    match (
        facts.implementation.backend_kind,
        &facts.deployment_closure.backend,
        &command.mcp_runtime,
        &facts.mcp_runtime,
    ) {
        (
            CapabilityBackendKind::Mcp,
            CapabilityBackendBinding::Mcp {
                mcp_deployment,
                discovery_snapshot_id,
                discovery_snapshot_digest,
                ..
            },
            Some(request),
            Some(binding),
        ) => {
            binding.validate_for(&facts.principal).is_ok()
                && request.mcp_operation_id == binding.mcp_operation_id
                && request.authorization_binding_id == binding.authorization_binding_id
                && mcp_deployment == &binding.mcp_deployment
                && discovery_snapshot_id == &binding.discovery_snapshot_id
                && discovery_snapshot_digest == &binding.discovery_snapshot_digest
        }
        (kind, _, None, None) => kind != CapabilityBackendKind::Mcp,
        _ => false,
    }
}

pub fn decide_capability_admission(
    command: &AdmitCapabilityInvocation,
    facts: CapabilityAdmissionFacts,
    limits: InvocationCommandLimits,
) -> Result<CapabilityInvocationRecord, InvocationError> {
    command.validate_at(facts.database_now, limits)?;
    facts
        .run_bindings
        .validate()
        .map_err(|_| InvocationError::InvalidRun)?;
    facts
        .deployment_closure
        .validate()
        .map_err(|_| InvocationError::InvalidDeployment)?;
    facts.interface.validate()?;
    facts.implementation.validate()?;
    facts.input.validate()?;
    facts
        .principal
        .validate()
        .map_err(|_| InvocationError::InvalidPrincipal)?;
    if !valid_mcp_runtime_binding(command, &facts) {
        return Err(InvocationError::InvalidDeployment);
    }
    if facts.run_state != RunState::Running
        || facts.run_version != command.expected_run_version
        || facts.run_pause_requested
        || facts.run_cancel_requested
        || facts.run_timeout_requested
        || facts.node_state != NodeExecutionState::Running
        || facts.node_version != command.expected_node_version
        || facts.node_kind != command.origin.required_node_kind()
        || facts.database_now >= facts.run_deadline
        || facts.database_now >= facts.node_deadline
        || facts.input.run_id != command.run_id
        || facts.input.value_id != command.input_value_id
        || matches!(
            &facts.input.storage,
            InvocationValueStorage::Artifact { .. }
        ) != command.input_artifact_link_id.is_some()
        || facts.input.schema_digest != facts.interface.input_schema_digest
        || !facts
            .interface
            .data_policy
            .permits_input(facts.input.classification)
        || facts.principal.tenant_id != command.audit.tenant_id
        || facts.principal.principal_id != command.audit.principal_id
        || facts.principal.principal_kind != command.audit.principal_kind
        || !facts
            .principal
            .permissions
            .contains(Permission::CapabilityInvoke)
        || facts.run_bindings.principal.principal_id != command.audit.principal_id
    {
        return Err(InvocationError::AdmissionRejected);
    }
    let slot = facts
        .run_bindings
        .slots
        .iter()
        .find(|slot| slot.slot_id == command.slot_id)
        .ok_or(InvocationError::InvalidSelection)?;
    let FrozenSlotTarget::Capability {
        candidates,
        selection_policy,
        ..
    } = &slot.target
    else {
        return Err(InvocationError::InvalidSelection);
    };
    let selected = candidates
        .get(usize::from(command.selected_candidate_ordinal))
        .ok_or(InvocationError::InvalidSelection)?;
    if selected != &facts.deployment
        || facts.deployment.resource_kind != ResourceKind::CapabilityDeployment
        || facts.deployment_closure.interface != facts.interface.revision
        || facts.deployment_closure.implementation != facts.implementation.revision
        || facts.implementation.interface_revision != facts.interface.revision
        || facts
            .deployment_closure
            .backend
            .validate_for(&facts.implementation.backend_contract)
            .is_err()
        || facts.implementation.features.progress
            != (facts.interface.progress.mode == CapabilityProgressMode::Events)
        || facts.interface.progress.max_events > limits.maximum_progress_events
        || facts.interface.progress.max_bytes_per_event > limits.maximum_progress_event_bytes
        || facts.implementation.features.max_remote_state_bytes > limits.maximum_remote_state_bytes
        || facts.implementation.features.max_poll_count > limits.maximum_poll_count
    {
        return Err(InvocationError::InvalidDeployment);
    }
    let selection_evidence = InvocationSelectionEvidence::build(
        candidates,
        command.selected_candidate_ordinal,
        command.selector_input_digest.clone(),
    )?;
    selection_evidence.validate_for(candidates, selected)?;

    let expected_policies = exact_invocation_policies(
        &facts.run_bindings.policies,
        &facts.deployment_closure.policies,
    )?;
    command.policy_decisions.validate_for(&expected_policies)?;
    let deadline = facts.run_deadline.min(facts.node_deadline);
    let attempt_limit = if facts.interface.idempotency == CapabilityIdempotencyKind::None
        && facts.interface.effect.risk_rank() >= Effect::IdempotentWrite.risk_rank()
    {
        1
    } else {
        command.requested_attempt_limit.min(limits.maximum_attempts)
    };
    let key_basis = serde_json::json!({
        "deployment": facts.deployment,
        "input_content_digest": facts.input.content_digest,
        "node_execution_id": command.node_execution_id,
        "origin": command.origin,
        "run_id": command.run_id,
        "tenant_id": command.audit.tenant_id,
    });
    let effect_key_digest = domain_digest("capability_effect", &key_basis)?;
    let idempotency_key_digest = domain_digest("capability_idempotency", &key_basis)?;
    let mut admission = CapabilityAdmissionSnapshot {
        schema_version: 1,
        origin_key: command.origin.clone(),
        slot_id: command.slot_id.clone(),
        slot_binding_digest: slot.binding_digest.clone(),
        run_bindings_digest: facts.run_bindings.canonical_digest.clone(),
        selection_policy: selection_policy.clone(),
        selection_evidence,
        deployment: facts.deployment,
        interface: facts.interface.revision,
        capability_name: facts.interface.qualified_name,
        implementation: facts.implementation.revision,
        backend_kind: facts.implementation.backend_kind,
        backend_contract_digest: facts.implementation.backend_contract_digest,
        mcp_runtime: facts.mcp_runtime,
        input: facts.input,
        input_artifact_link_id: command.input_artifact_link_id.clone(),
        effect: facts.interface.effect,
        idempotency: facts.interface.idempotency,
        cancellation: facts.interface.cancellation,
        progress: facts.interface.progress,
        implementation_features: facts.implementation.features,
        input_schema_digest: facts.interface.input_schema_digest,
        output_schema_digest: facts.interface.output_schema_digest,
        error_schema_digest: facts.interface.error_schema_digest,
        artifact_contract: facts.interface.artifacts,
        data_flow_policy: facts.interface.data_policy,
        interface_limits: facts.interface.execution_limits,
        policies: command.policy_decisions.clone(),
        principal: facts.principal,
        effect_key_digest: effect_key_digest.clone(),
        idempotency_key_digest,
        attempt_limit,
        retry_backoff_milliseconds: command.requested_retry_backoff_milliseconds,
        deadline,
        canonical_digest: effect_key_digest.clone(),
    };
    admission.canonical_digest = digest_without_field(&admission, "canonical_digest")?;
    let state = if command.approval_task_id.is_some() {
        InvocationState::AwaitingApproval
    } else {
        InvocationState::Ready
    };
    let payload = CapabilityInvocationPayload {
        schema_version: 1,
        admission: admission.clone(),
        current_job_id: None,
        approval_task_id: command.approval_task_id.clone(),
        input_task_id: None,
        detached_pending: None,
        result: None,
        failure: None,
        reconciliation: None,
    };
    let record = CapabilityInvocationRecord {
        tenant_id: command.audit.tenant_id.clone(),
        invocation_id: command.invocation_id.clone(),
        run_id: command.run_id.clone(),
        node_execution_id: command.node_execution_id.clone(),
        owner_kind: command.origin.owner_kind(),
        owner_id: command.origin.owner_id().clone(),
        logical_key: command.origin.logical_key(),
        deployment_id: admission.deployment.deployment_id.clone(),
        input_value_id: admission.input.value_id.clone(),
        output_value_id: None,
        effect_key_digest,
        state,
        version: 1,
        payload,
        deadline,
        retry_at: None,
        started_at: None,
        terminal_at: None,
        created_at: facts.database_now,
        updated_at: facts.database_now,
    };
    record.validate()?;
    Ok(record)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityApprovalDecision {
    Approve,
    Reject,
    Cancel,
}

#[derive(Debug, Clone)]
pub struct ResolveCapabilityApproval {
    pub audit: CommandAudit,
    pub invocation_id: ResourceId,
    pub approval_task_id: ResourceId,
    pub expected_invocation_version: u64,
    pub expected_task_generation: u64,
    pub expected_task_version: u64,
    pub eligible_principal_rule_digest: Sha256Digest,
    pub decision: CapabilityApprovalDecision,
}

impl ResolveCapabilityApproval {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), InvocationError> {
        self.audit
            .validate_at(now)
            .map_err(|_| InvocationError::InvalidAudit)?;
        if self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.approval_task_id.kind() != ResourceKind::ApprovalTask
            || self.expected_invocation_version == 0
            || self.expected_task_generation == 0
            || self.expected_task_version == 0
        {
            return Err(InvocationError::InvalidCommand);
        }
        Ok(())
    }
}

pub fn decide_approval_transition(
    current: &CapabilityInvocationRecord,
    command: &ResolveCapabilityApproval,
    database_now: DateTime<Utc>,
) -> Result<CapabilityInvocationRecord, InvocationError> {
    current.validate()?;
    command.validate_at(database_now)?;
    let requirement = current
        .payload
        .admission
        .policies
        .approval
        .as_ref()
        .ok_or(InvocationError::InvalidApproval)?;
    if current.tenant_id != command.audit.tenant_id
        || current.invocation_id != command.invocation_id
        || current.version != command.expected_invocation_version
        || current.state != InvocationState::AwaitingApproval
        || current.payload.approval_task_id.as_ref() != Some(&command.approval_task_id)
        || requirement.eligible_principal_rule_digest != command.eligible_principal_rule_digest
        || database_now >= current.deadline
    {
        return Err(InvocationError::FirstWinnerLost);
    }
    let mut next = current.clone();
    next.version = next
        .version
        .checked_add(1)
        .ok_or(InvocationError::CounterOverflow)?;
    next.updated_at = database_now;
    match command.decision {
        CapabilityApprovalDecision::Approve => next.state = InvocationState::Ready,
        CapabilityApprovalDecision::Reject => {
            next.state = InvocationState::Failed;
            next.payload.failure = Some(Failure {
                code: FailureCode::Platform {
                    code: PlatformFailureCode::CapabilityFailed,
                },
                class: FailureClass::Policy,
                retryability: Retryability::Never,
                safe_message: None,
                details_ref: None,
                source: FailureSource::Capability,
            });
            next.terminal_at = Some(database_now);
        }
        CapabilityApprovalDecision::Cancel => {
            next.state = InvocationState::Cancelled;
            next.terminal_at = Some(database_now);
        }
    }
    next.validate()?;
    Ok(next)
}

pub trait InvocationTransaction {
    type Error;
    type ExecutionRecord;
    type JobRecord;
    type ControlRecord;

    async fn admit_capability_invocation(
        &mut self,
        command: AdmitCapabilityInvocation,
    ) -> Result<CommandOutcome<CapabilityInvocationRecord>, Self::Error>;

    async fn resolve_capability_approval(
        &mut self,
        command: ResolveCapabilityApproval,
    ) -> Result<CommandOutcome<CapabilityInvocationRecord>, Self::Error>;

    async fn prepare_capability_dispatch(
        &mut self,
        command: PrepareCapabilityDispatch,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn commit_capability_outcome(
        &mut self,
        command: CommitCapabilityOutcome,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn commit_capability_cancellation_outcome(
        &mut self,
        command: CommitCapabilityCancellationOutcome,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn wake_capability_invocation(
        &mut self,
        command: WakeCapabilityInvocation,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn resolve_capability_input(
        &mut self,
        command: ResolveCapabilityInput,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn record_capability_progress(
        &mut self,
        command: RecordCapabilityProgress,
    ) -> Result<CommandOutcome<Self::JobRecord>, Self::Error>;

    async fn control_capability_invocation(
        &mut self,
        command: ControlCapabilityInvocation,
    ) -> Result<CommandOutcome<Self::ControlRecord>, Self::Error>;

    async fn resolve_capability_reconciliation(
        &mut self,
        command: ResolveCapabilityReconciliation,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error>;

    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}

pub trait InvocationStore {
    type Error;
    type Transaction<'a>: InvocationTransaction<Error = Self::Error>
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error>;
}

fn exact_invocation_policies(
    run: &[ExactPolicyBinding],
    deployment: &[ExactVersionRef],
) -> Result<Vec<ExactVersionRef>, InvocationError> {
    let mut policies = run
        .iter()
        .map(|binding| binding.revision.clone())
        .chain(deployment.iter().cloned())
        .collect::<Vec<_>>();
    policies.sort_by(|left, right| left.revision_id.cmp(&right.revision_id));
    policies.dedup_by(|left, right| left.revision_id == right.revision_id);
    if policies.iter().any(|policy| {
        policy.validate().is_err() || policy.resource_kind != ResourceKind::PolicyRevision
    }) {
        return Err(InvocationError::InvalidPolicyBundle);
    }
    Ok(policies)
}

pub(crate) fn domain_digest<T: Serialize>(
    domain: &str,
    value: &T,
) -> Result<Sha256Digest, InvocationError> {
    digest(&serde_json::json!({
        "domain": domain,
        "schema_version": 1,
        "value": value,
    }))
}

pub(crate) fn digest<T: Serialize>(value: &T) -> Result<Sha256Digest, InvocationError> {
    let value = serde_json::to_value(value).map_err(|_| InvocationError::Canonicalization)?;
    canonical_digest(&value)
        .map_err(|_| InvocationError::Canonicalization)?
        .parse()
        .map_err(|_| InvocationError::Canonicalization)
}

fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, InvocationError> {
    let mut value = serde_json::to_value(value).map_err(|_| InvocationError::Canonicalization)?;
    let object = value
        .as_object_mut()
        .ok_or(InvocationError::Canonicalization)?;
    if object.remove(field).is_none() {
        return Err(InvocationError::Canonicalization);
    }
    digest(&value)
}

pub(crate) fn is_stable_code(value: &str, maximum_bytes: usize) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= maximum_bytes
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-' | b'.' | b':'))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvocationError {
    InvalidAudit,
    InvalidLimits,
    InvalidIdentity,
    InvalidOrigin,
    InvalidInputValue,
    InvalidSelection,
    InvalidPolicyBundle,
    InvalidInterface,
    InvalidImplementation,
    InvalidDeployment,
    InvalidPrincipal,
    InvalidAdmission,
    InvalidCurrentState,
    InvalidCommand,
    InvalidRun,
    AdmissionRejected,
    InvalidApproval,
    InvalidJob,
    InvalidOutcome,
    InvalidOutput,
    UnsupportedOutcome,
    InvalidWake,
    InvalidProgress,
    InvalidControl,
    DeadlineExceeded,
    FirstWinnerLost,
    CounterOverflow,
    Canonicalization,
}

impl fmt::Display for InvocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidAudit => "Invocation audit is invalid",
            Self::InvalidLimits => "Invocation limits are invalid",
            Self::InvalidIdentity => "Invocation identity is invalid",
            Self::InvalidOrigin => "Invocation origin is invalid",
            Self::InvalidInputValue => "Invocation input ValueRef is invalid",
            Self::InvalidSelection => "Capability selection evidence is invalid",
            Self::InvalidPolicyBundle => "Invocation policy decision bundle is invalid",
            Self::InvalidInterface => "Capability Interface contract is invalid",
            Self::InvalidImplementation => "Capability Implementation contract is invalid",
            Self::InvalidDeployment => "Capability Deployment closure is invalid",
            Self::InvalidPrincipal => "Invocation principal snapshot is invalid",
            Self::InvalidAdmission => "Capability admission snapshot is invalid",
            Self::InvalidCurrentState => "CapabilityInvocation current state is invalid",
            Self::InvalidCommand => "CapabilityInvocation command is invalid",
            Self::InvalidRun => "CapabilityInvocation Run binding is invalid",
            Self::AdmissionRejected => "CapabilityInvocation admission facts reject the call",
            Self::InvalidApproval => "Capability approval binding is invalid",
            Self::InvalidJob => "Capability Job binding or state is invalid",
            Self::InvalidOutcome => "Capability dispatch outcome is invalid",
            Self::InvalidOutput => "Capability output is invalid",
            Self::UnsupportedOutcome => {
                "Capability backend outcome is not supported by the frozen contract"
            }
            Self::InvalidWake => "Capability wake binding is invalid",
            Self::InvalidProgress => "Capability progress is invalid",
            Self::InvalidControl => "Capability control transition is invalid",
            Self::DeadlineExceeded => "CapabilityInvocation deadline has been exceeded",
            Self::FirstWinnerLost => "CapabilityInvocation first-winner was already chosen",
            Self::CounterOverflow => "CapabilityInvocation version overflowed",
            Self::Canonicalization => "CapabilityInvocation canonicalization failed",
        })
    }
}

impl Error for InvocationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use insight_platform_contracts::{PermissionSet, PrincipalKind};

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn digest_value(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn exact_version(value: &str, digest_character: char) -> ExactVersionRef {
        ExactVersionRef::new(id(value), digest_value(digest_character)).unwrap()
    }

    fn policy_binding(deployment: &str, revision: &str, marker: char) -> ExactPolicyBinding {
        ExactPolicyBinding {
            deployment: ExactDeploymentRef::new(id(deployment), digest_value(marker)).unwrap(),
            revision: exact_version(revision, marker),
        }
    }

    fn principal(tenant: &ResourceId, principal: &ResourceId) -> PrincipalSnapshot {
        PrincipalSnapshot::build(
            tenant.clone(),
            principal.clone(),
            PrincipalKind::AgentRunner,
            PermissionSet::new(vec![Permission::CapabilityInvoke]).unwrap(),
            1,
            1,
            1,
        )
        .unwrap()
    }

    #[test]
    fn selection_and_policy_evidence_are_canonical_and_closed() {
        let candidate = ExactDeploymentRef::new(
            id("cdep_0198f1c3-8f49-7c3e-b1f3-773c28367b91"),
            digest_value('a'),
        )
        .unwrap();
        let evidence = InvocationSelectionEvidence::build(
            std::slice::from_ref(&candidate),
            0,
            digest_value('b'),
        )
        .unwrap();
        evidence
            .validate_for(std::slice::from_ref(&candidate), &candidate)
            .unwrap();
        assert!(InvocationSelectionEvidence::build(&[candidate], 1, digest_value('b')).is_err());

        let policy = exact_version("prev_0198f1c3-8f49-7c3e-b1f3-773c28367b92", 'c');
        let bundle = InvocationPolicyDecisionBundle::build(
            vec![InvocationPolicyDecision {
                policy: policy.clone(),
                disposition: InvocationPolicyDisposition::ApprovalRequired,
                evidence_digest: digest_value('d'),
            }],
            Some(InvocationApprovalRequirement {
                policy_revision: policy.clone(),
                eligible_principal_rule_digest: digest_value('e'),
                safe_prompt_key: "approve_capability".to_owned(),
            }),
        )
        .unwrap();
        bundle.validate_for(&[policy]).unwrap();
    }

    #[test]
    fn approval_transition_is_version_and_deadline_first_winner() {
        let now = DateTime::parse_from_rfc3339("2026-08-10T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let tenant = id("ten_0198f1c3-8f49-7c3e-b1f3-773c28367b90");
        let principal_id = id("prn_0198f1c3-8f49-7c3e-b1f3-773c28367b91");
        let invocation_id = id("inv_0198f1c3-8f49-7c3e-b1f3-773c28367b92");
        let node_id = id("nod_0198f1c3-8f49-7c3e-b1f3-773c28367b93");
        let run_id = id("run_0198f1c3-8f49-7c3e-b1f3-773c28367b94");
        let task_id = id("apr_0198f1c3-8f49-7c3e-b1f3-773c28367b95");
        let policy = exact_version("prev_0198f1c3-8f49-7c3e-b1f3-773c28367b96", 'a');
        let policies = InvocationPolicyDecisionBundle::build(
            vec![InvocationPolicyDecision {
                policy: policy.clone(),
                disposition: InvocationPolicyDisposition::ApprovalRequired,
                evidence_digest: digest_value('b'),
            }],
            Some(InvocationApprovalRequirement {
                policy_revision: policy,
                eligible_principal_rule_digest: digest_value('c'),
                safe_prompt_key: "approve_capability".to_owned(),
            }),
        )
        .unwrap();
        let input = ExactInvocationValueRef {
            schema_version: 1,
            value_id: id("val_0198f1c3-8f49-7c3e-b1f3-773c28367b97"),
            run_id: run_id.clone(),
            producing_node_id: None,
            value_kind: "run_input".to_owned(),
            classification: DataClassification::Internal,
            schema_digest: digest_value('d'),
            content_digest: digest_value('e'),
            storage: InvocationValueStorage::Inline,
        };
        let deployment = ExactDeploymentRef::new(
            id("cdep_0198f1c3-8f49-7c3e-b1f3-773c28367b98"),
            digest_value('f'),
        )
        .unwrap();
        let interface = exact_version("cirev_0198f1c3-8f49-7c3e-b1f3-773c28367b99", '1');
        let implementation = exact_version("cimp_0198f1c3-8f49-7c3e-b1f3-773c28367b9a", '2');
        let mut admission = CapabilityAdmissionSnapshot {
            schema_version: 1,
            origin_key: InvocationOrigin::PlanNode {
                node_execution_id: node_id.clone(),
            },
            slot_id: "capability".to_owned(),
            slot_binding_digest: digest_value('3'),
            run_bindings_digest: digest_value('4'),
            selection_policy: policy_binding(
                "pdep_0198f1c3-8f49-7c3e-b1f3-773c28367b9c",
                "prev_0198f1c3-8f49-7c3e-b1f3-773c28367b9b",
                '5',
            ),
            selection_evidence: InvocationSelectionEvidence::build(
                std::slice::from_ref(&deployment),
                0,
                digest_value('6'),
            )
            .unwrap(),
            deployment: deployment.clone(),
            interface,
            capability_name: "fixture.read".parse().unwrap(),
            implementation,
            backend_kind: CapabilityBackendKind::Native,
            backend_contract_digest: digest_value('7'),
            mcp_runtime: None,
            input: input.clone(),
            input_artifact_link_id: None,
            effect: Effect::ReadOnly,
            idempotency: CapabilityIdempotencyKind::Intrinsic,
            cancellation: CapabilityCancellationKind::Confirmed,
            progress: CapabilityProgressContract {
                mode: CapabilityProgressMode::None,
                schema_digest: None,
                max_events: 0,
                max_bytes_per_event: 0,
                minimum_interval_milliseconds: 0,
                durability: insight_platform_contracts::CapabilityProgressDurability::None,
            },
            implementation_features: CapabilityBackendFeatures {
                deferred: false,
                input_required: false,
                callback: false,
                poll: false,
                progress: false,
                cancellation: true,
                max_remote_state_bytes: 0,
                max_poll_count: 0,
            },
            input_schema_digest: input.schema_digest.clone(),
            output_schema_digest: digest_value('8'),
            error_schema_digest: digest_value('9'),
            artifact_contract: CapabilityArtifactContract { ports: vec![] },
            data_flow_policy: CapabilityDataFlowPolicy {
                maximum_input_classification: DataClassification::Restricted,
                maximum_output_classification: DataClassification::Restricted,
                allowed_regions: vec!["global".parse().unwrap()],
                declassification_policy: None,
            },
            interface_limits: CapabilityInterfaceLimits {
                maximum_input_bytes: 1_048_576,
                maximum_output_bytes: 1_048_576,
                maximum_artifacts: 0,
                maximum_execution_milliseconds: 60_000,
            },
            policies,
            principal: principal(&tenant, &principal_id),
            effect_key_digest: digest_value('a'),
            idempotency_key_digest: digest_value('b'),
            attempt_limit: 2,
            retry_backoff_milliseconds: 100,
            deadline: now + Duration::minutes(5),
            canonical_digest: digest_value('c'),
        };
        admission.canonical_digest = digest_without_field(&admission, "canonical_digest").unwrap();
        let current = CapabilityInvocationRecord {
            tenant_id: tenant.clone(),
            invocation_id: invocation_id.clone(),
            run_id,
            node_execution_id: node_id.clone(),
            owner_kind: ResourceKind::NodeExecution,
            owner_id: node_id,
            logical_key: admission.origin_key.logical_key(),
            deployment_id: deployment.deployment_id,
            input_value_id: input.value_id,
            output_value_id: None,
            effect_key_digest: admission.effect_key_digest.clone(),
            state: InvocationState::AwaitingApproval,
            version: 1,
            payload: CapabilityInvocationPayload {
                schema_version: 1,
                admission,
                current_job_id: None,
                approval_task_id: Some(task_id.clone()),
                input_task_id: None,
                detached_pending: None,
                result: None,
                failure: None,
                reconciliation: None,
            },
            deadline: now + Duration::minutes(5),
            retry_at: None,
            started_at: None,
            terminal_at: None,
            created_at: now,
            updated_at: now,
        };
        current.validate().unwrap();
        let audit = CommandAudit {
            tenant_id: tenant,
            principal_id,
            principal_kind: PrincipalKind::AgentRunner,
            receipt_id: id("rcp_0198f1c3-8f49-7c3e-b1f3-773c28367b9c"),
            event_id: id("evt_0198f1c3-8f49-7c3e-b1f3-773c28367b9d"),
            outbox_id: id("obx_0198f1c3-8f49-7c3e-b1f3-773c28367b9e"),
            idempotency_key_digest: digest_value('d'),
            request_digest: digest_value('e'),
            receipt_expires_at: now + Duration::hours(1),
        };
        let command = ResolveCapabilityApproval {
            audit,
            invocation_id,
            approval_task_id: task_id,
            expected_invocation_version: 1,
            expected_task_generation: 1,
            expected_task_version: 1,
            eligible_principal_rule_digest: digest_value('c'),
            decision: CapabilityApprovalDecision::Approve,
        };
        let mut cancel_command = command.clone();
        cancel_command.decision = CapabilityApprovalDecision::Cancel;
        let cancelled =
            decide_approval_transition(&current, &cancel_command, now + Duration::seconds(1))
                .unwrap();
        assert_eq!(cancelled.state, InvocationState::Cancelled);
        assert_eq!(cancelled.version, 2);
        assert_eq!(cancelled.terminal_at, Some(now + Duration::seconds(1)));
        assert!(cancelled.payload.failure.is_none());
        let approved =
            decide_approval_transition(&current, &command, now + Duration::seconds(1)).unwrap();
        assert_eq!(approved.state, InvocationState::Ready);
        assert_eq!(approved.version, 2);
        assert_eq!(
            decide_approval_transition(&approved, &command, now + Duration::seconds(2)),
            Err(InvocationError::FirstWinnerLost)
        );
    }
}
