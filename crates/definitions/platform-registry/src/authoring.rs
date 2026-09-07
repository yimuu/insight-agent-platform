//! Bounded authoring queries. Active selections resolve once to immutable exact
//! targets; no selector is stored as an execution fallback or an alias aggregate.
use insight_platform_contracts::{
    AgentSlotBindingInputV1, ContextConsistencyPolicy, DependencySlotKind, ExactDeploymentRef,
    ExactPolicyBinding, ExactVersionRef, ResourceId, ResourceKind, Sha256Digest,
};
use serde::{Deserialize, Serialize};

pub const MAX_AUTHORING_RESOLVE_SLOTS: usize = 64;
pub const MAX_AUTHORING_RESOLVE_CANDIDATES: usize = 16;
pub const MAX_AUTHORING_QUERY_BYTES: usize = 262_144;
pub const MAX_AUTHORING_QUERY_RESPONSE_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthoringDeploymentSelectorV1 {
    Exact {
        deployment: ExactDeploymentRef,
    },
    Active {
        resource_id: ResourceId,
        environment: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthoringSlotTargetV1 {
    Model {
        candidates: Vec<AuthoringDeploymentSelectorV1>,
        selection_policy: ExactPolicyBinding,
    },
    Capability {
        candidates: Vec<AuthoringDeploymentSelectorV1>,
        selection_policy: ExactPolicyBinding,
        tool_alias: Option<String>,
    },
    Context {
        deployment: AuthoringDeploymentSelectorV1,
        consistency: ContextConsistencyPolicy,
        allowed_projection: Vec<String>,
        authorization_policy: ExactVersionRef,
        ranking_policy: ExactVersionRef,
    },
    ChildAgent {
        candidates: Vec<AuthoringDeploymentSelectorV1>,
        selection_policy: ExactPolicyBinding,
    },
    Skill {
        candidates: Vec<AuthoringDeploymentSelectorV1>,
        selection_policy: ExactPolicyBinding,
    },
}
impl AuthoringSlotTargetV1 {
    pub fn kind(&self) -> DependencySlotKind {
        match self {
            Self::Model { .. } => DependencySlotKind::Model,
            Self::Capability { .. } => DependencySlotKind::Capability,
            Self::Context { .. } => DependencySlotKind::Context,
            Self::ChildAgent { .. } => DependencySlotKind::ChildAgent,
            Self::Skill { .. } => DependencySlotKind::Skill,
        }
    }
    pub fn selectors(&self) -> &[AuthoringDeploymentSelectorV1] {
        match self {
            Self::Model { candidates, .. }
            | Self::Capability { candidates, .. }
            | Self::ChildAgent { candidates, .. }
            | Self::Skill { candidates, .. } => candidates,
            Self::Context { deployment, .. } => std::slice::from_ref(deployment),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoringSlotSelectionV1 {
    pub slot_id: String,
    pub requirement_digest: Sha256Digest,
    /// None means compatibility was not requested; it must not be reported as verified.
    pub interface_contract_digest: Option<Sha256Digest>,
    pub target: AuthoringSlotTargetV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveAgentBindingsRequestV1 {
    pub schema_version: u32,
    pub slots: Vec<AuthoringSlotSelectionV1>,
}
impl ResolveAgentBindingsRequestV1 {
    pub fn validate(&self) -> Result<(), AuthoringQueryError> {
        if self.schema_version != 1
            || self.slots.is_empty()
            || self.slots.len() > MAX_AUTHORING_RESOLVE_SLOTS
        {
            return Err(AuthoringQueryError::Invalid);
        }
        let mut seen = std::collections::BTreeSet::new();
        for slot in &self.slots {
            if !valid_key(&slot.slot_id, 128)
                || !seen.insert(&slot.slot_id)
                || slot.target.selectors().is_empty()
                || slot.target.selectors().len() > MAX_AUTHORING_RESOLVE_CANDIDATES
            {
                return Err(AuthoringQueryError::Invalid);
            }
            match &slot.target {
                AuthoringSlotTargetV1::Model {
                    selection_policy, ..
                }
                | AuthoringSlotTargetV1::ChildAgent {
                    selection_policy, ..
                }
                | AuthoringSlotTargetV1::Skill {
                    selection_policy, ..
                } => selection_policy
                    .validate()
                    .map_err(|_| AuthoringQueryError::Invalid)?,
                AuthoringSlotTargetV1::Capability {
                    selection_policy,
                    tool_alias,
                    ..
                } => {
                    selection_policy
                        .validate()
                        .map_err(|_| AuthoringQueryError::Invalid)?;
                    if tool_alias
                        .as_ref()
                        .is_some_and(|alias| !valid_key(alias, 128))
                    {
                        return Err(AuthoringQueryError::Invalid);
                    }
                }
                AuthoringSlotTargetV1::Context {
                    consistency,
                    allowed_projection,
                    authorization_policy,
                    ranking_policy,
                    ..
                } => {
                    consistency
                        .validate()
                        .map_err(|_| AuthoringQueryError::Invalid)?;
                    if authorization_policy.validate().is_err()
                        || ranking_policy.validate().is_err()
                        || authorization_policy.resource_kind != ResourceKind::PolicyRevision
                        || ranking_policy.resource_kind != ResourceKind::PolicyRevision
                        || authorization_policy == ranking_policy
                        || allowed_projection.len() > insight_platform_contracts::MAX_CONTEXT_FIELDS
                        || allowed_projection.windows(2).any(|pair| pair[0] >= pair[1])
                        || allowed_projection
                            .iter()
                            .any(|field| !valid_key(field, 128))
                    {
                        return Err(AuthoringQueryError::Invalid);
                    }
                }
            }
            for selector in slot.target.selectors() {
                match selector {
                    AuthoringDeploymentSelectorV1::Exact { deployment }
                        if deployment.validate().is_ok()
                            && deployment.resource_kind == deployment_kind(slot.target.kind()) => {}
                    AuthoringDeploymentSelectorV1::Active {
                        resource_id,
                        environment,
                    } if resource_id.kind() == resource_kind(slot.target.kind())
                        && valid_key(environment, 64) => {}
                    _ => return Err(AuthoringQueryError::Invalid),
                }
            }
        }
        if serde_json::to_vec(self)
            .map_err(|_| AuthoringQueryError::Invalid)?
            .len()
            > MAX_AUTHORING_QUERY_BYTES
        {
            return Err(AuthoringQueryError::Invalid);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthoringQueryError {
    Invalid,
    Denied,
    NotFound,
    Disabled,
    ContractMismatch,
    Unavailable,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthoringResolutionV1 {
    Resolved {
        binding: Box<AgentSlotBindingInputV1>,
        deployment_features: Vec<insight_platform_contracts::AgentDeploymentFeaturesV1>,
        observed_contract_digests: Vec<Sha256Digest>,
        contract_match: Option<bool>,
        call_authorized: bool,
    },
    Rejected {
        code: AuthoringQueryError,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoringSlotResolutionV1 {
    pub slot_id: String,
    pub resolution: AuthoringResolutionV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveAgentBindingsResponseV1 {
    pub schema_version: u32,
    pub slots: Vec<AuthoringSlotResolutionV1>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoringDependencyV1 {
    pub schema_version: u32,
    pub kind: DependencySlotKind,
    pub resource_id: ResourceId,
    pub environment: String,
    pub deployment: ExactDeploymentRef,
    pub interface_contract_digest: Sha256Digest,
    pub contract_match: Option<bool>,
    pub call_authorized: bool,
}
pub fn resource_kind(kind: DependencySlotKind) -> ResourceKind {
    match kind {
        DependencySlotKind::Model => ResourceKind::ModelProfile,
        DependencySlotKind::Capability => ResourceKind::CapabilityInterface,
        DependencySlotKind::Context => ResourceKind::ContextSourceInterface,
        DependencySlotKind::ChildAgent => ResourceKind::Agent,
        DependencySlotKind::Skill => ResourceKind::Skill,
    }
}
pub fn deployment_kind(kind: DependencySlotKind) -> ResourceKind {
    match kind {
        DependencySlotKind::Model => ResourceKind::ModelDeployment,
        DependencySlotKind::Capability => ResourceKind::CapabilityDeployment,
        DependencySlotKind::Context => ResourceKind::ContextDeployment,
        DependencySlotKind::ChildAgent => ResourceKind::AgentDeployment,
        DependencySlotKind::Skill => ResourceKind::SkillDeployment,
    }
}
fn valid_key(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value.as_bytes()[0].is_ascii_lowercase()
        && value.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-' | b'.')
        })
}

/// Convert already chosen exact slots to the same bounded read-only resolver contract.
pub fn exact_feature_request(
    bindings: &[AgentSlotBindingInputV1],
) -> Result<Option<ResolveAgentBindingsRequestV1>, AuthoringQueryError> {
    use insight_platform_contracts::AgentSlotTargetInputV1 as Target;
    let mut slots = Vec::new();
    let selectors = |values: &[ExactDeploymentRef]| {
        values
            .iter()
            .cloned()
            .map(|deployment| AuthoringDeploymentSelectorV1::Exact { deployment })
            .collect()
    };
    for binding in bindings {
        let target = match &binding.target {
            Target::Capability {
                candidates,
                selection_policy,
                tool_alias,
            } => AuthoringSlotTargetV1::Capability {
                candidates: selectors(candidates),
                selection_policy: selection_policy.clone(),
                tool_alias: tool_alias.clone(),
            },
            Target::ChildAgent {
                candidates,
                selection_policy,
            } => AuthoringSlotTargetV1::ChildAgent {
                candidates: selectors(candidates),
                selection_policy: selection_policy.clone(),
            },
            Target::Context { binding } => AuthoringSlotTargetV1::Context {
                deployment: AuthoringDeploymentSelectorV1::Exact {
                    deployment: binding.context_deployment.clone(),
                },
                consistency: binding.consistency.clone(),
                allowed_projection: binding.allowed_projection.clone(),
                authorization_policy: binding.authorization_policy.clone(),
                ranking_policy: binding.ranking_policy.clone(),
            },
            Target::Model { .. } | Target::Skill { .. } => continue,
        };
        slots.push(AuthoringSlotSelectionV1 {
            slot_id: binding.slot_id.clone(),
            requirement_digest: binding.requirement_digest.clone(),
            interface_contract_digest: None,
            target,
        });
    }
    if slots.is_empty() {
        return Ok(None);
    }
    let request = ResolveAgentBindingsRequestV1 {
        schema_version: 1,
        slots,
    };
    request.validate()?;
    Ok(Some(request))
}

pub fn resolved_feature_evidence(
    response: &ResolveAgentBindingsResponseV1,
    request: &ResolveAgentBindingsRequestV1,
) -> Result<Vec<insight_platform_contracts::AgentDeploymentFeaturesV1>, AuthoringQueryError> {
    response.validate_for(request)?;
    let mut unique = std::collections::BTreeMap::new();
    for slot in &response.slots {
        let AuthoringResolutionV1::Resolved {
            deployment_features,
            ..
        } = &slot.resolution
        else {
            return Err(AuthoringQueryError::Denied);
        };
        for item in deployment_features {
            if unique
                .insert(item.deployment.deployment_id.clone(), item.clone())
                .is_some_and(|previous| previous != *item)
            {
                return Err(AuthoringQueryError::Invalid);
            }
        }
    }
    Ok(unique.into_values().collect())
}

impl ResolveAgentBindingsResponseV1 {
    pub fn validate_for(
        &self,
        request: &ResolveAgentBindingsRequestV1,
    ) -> Result<(), AuthoringQueryError> {
        request.validate()?;
        if self.schema_version != 1
            || self.slots.len() != request.slots.len()
            || serde_json::to_vec(self)
                .map_err(|_| AuthoringQueryError::Invalid)?
                .len()
                > MAX_AUTHORING_QUERY_RESPONSE_BYTES
        {
            return Err(AuthoringQueryError::Invalid);
        }
        for (expected, actual) in request.slots.iter().zip(&self.slots) {
            if expected.slot_id != actual.slot_id {
                return Err(AuthoringQueryError::Invalid);
            }
            if let AuthoringResolutionV1::Resolved {
                binding,
                observed_contract_digests,
                deployment_features,
                contract_match,
                ..
            } = &actual.resolution
            {
                binding
                    .validate()
                    .map_err(|_| AuthoringQueryError::Invalid)?;
                let (kind, count) = match &binding.target {
                    insight_platform_contracts::AgentSlotTargetInputV1::Model {
                        candidates,
                        ..
                    } => (DependencySlotKind::Model, candidates.len()),
                    insight_platform_contracts::AgentSlotTargetInputV1::Capability {
                        candidates,
                        ..
                    } => (DependencySlotKind::Capability, candidates.len()),
                    insight_platform_contracts::AgentSlotTargetInputV1::Context { .. } => {
                        (DependencySlotKind::Context, 1)
                    }
                    insight_platform_contracts::AgentSlotTargetInputV1::ChildAgent {
                        candidates,
                        ..
                    } => (DependencySlotKind::ChildAgent, candidates.len()),
                    insight_platform_contracts::AgentSlotTargetInputV1::Skill {
                        candidates,
                        ..
                    } => (DependencySlotKind::Skill, candidates.len()),
                };
                if binding.slot_id != expected.slot_id
                    || binding.requirement_digest != expected.requirement_digest
                    || kind != expected.target.kind()
                    || count != expected.target.selectors().len()
                    || observed_contract_digests.len() != count
                    || *contract_match
                        != expected.interface_contract_digest.as_ref().map(|wanted| {
                            observed_contract_digests
                                .iter()
                                .all(|digest| digest == wanted)
                        })
                {
                    return Err(AuthoringQueryError::Invalid);
                }
                let feature_targets = match &binding.target {
                    insight_platform_contracts::AgentSlotTargetInputV1::Capability {
                        candidates,
                        ..
                    }
                    | insight_platform_contracts::AgentSlotTargetInputV1::ChildAgent {
                        candidates,
                        ..
                    } => candidates.as_slice(),
                    insight_platform_contracts::AgentSlotTargetInputV1::Context { binding } => {
                        std::slice::from_ref(&binding.context_deployment)
                    }
                    _ => &[],
                };
                if deployment_features.len() != feature_targets.len()
                    || deployment_features
                        .iter()
                        .zip(feature_targets)
                        .zip(observed_contract_digests)
                        .any(|((features, target), contract)| {
                            features.validate().is_err()
                                || features.deployment != *target
                                || features.interface_contract_digest != *contract
                        })
                {
                    return Err(AuthoringQueryError::Invalid);
                }
            }
        }
        Ok(())
    }
}

/// Server-owned operation classification. A caller cannot request a query
/// exemption for a different route by supplying a flag or choosing POST.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoringQueryOperation {
    DiscoverDependencies,
    ResolveBindings,
}
impl AuthoringQueryOperation {
    pub const fn operation_id(self) -> &'static str {
        match self {
            Self::DiscoverDependencies => "discoverAgentAuthoringDependencies",
            Self::ResolveBindings => "resolveAgentAuthoringBindings",
        }
    }
    pub const fn method(self) -> &'static str {
        match self {
            Self::DiscoverDependencies => "GET",
            Self::ResolveBindings => "POST",
        }
    }
    pub const fn path(self) -> &'static str {
        match self {
            Self::DiscoverDependencies => "/v1/agent-authoring-dependencies",
            Self::ResolveBindings => "/v1/agent-authoring-bindings:resolve",
        }
    }
    pub const fn operation_kind(self) -> &'static str {
        "query"
    }
    pub const fn requires_command_receipt(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoringDependencyFiltersV1 {
    pub kind: DependencySlotKind,
    pub environment: Option<String>,
    pub interface_contract_digest: Option<Sha256Digest>,
}
impl AuthoringDependencyFiltersV1 {
    pub fn validate(&self) -> Result<(), AuthoringQueryError> {
        if self
            .environment
            .as_ref()
            .is_some_and(|value| !valid_key(value, 64))
        {
            return Err(AuthoringQueryError::Invalid);
        }
        Ok(())
    }
}
impl AuthoringDependencyV1 {
    pub fn validate_for(
        &self,
        filters: &AuthoringDependencyFiltersV1,
    ) -> Result<(), AuthoringQueryError> {
        if self.schema_version != 1
            || self.kind != filters.kind
            || self.resource_id.kind() != resource_kind(filters.kind)
            || self.deployment.resource_kind != deployment_kind(filters.kind)
            || self.deployment.validate().is_err()
            || !valid_key(&self.environment, 64)
            || filters
                .environment
                .as_ref()
                .is_some_and(|value| value != &self.environment)
            || self.contract_match
                != filters
                    .interface_contract_digest
                    .as_ref()
                    .map(|value| value == &self.interface_contract_digest)
        {
            return Err(AuthoringQueryError::Invalid);
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct DiscoverAuthoringDependencies {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: insight_platform_contracts::PrincipalKind,
    pub filters: AuthoringDependencyFiltersV1,
    pub page_size: u16,
    pub snapshot_at: Option<chrono::DateTime<chrono::Utc>>,
    pub boundary: Option<(chrono::DateTime<chrono::Utc>, ResourceId)>,
}
#[derive(Debug, Clone)]
pub struct AuthoringDependencyPage {
    pub items: Vec<AuthoringDependencyV1>,
    pub snapshot_at: chrono::DateTime<chrono::Utc>,
    pub next_boundary: Option<(chrono::DateTime<chrono::Utc>, ResourceId)>,
}
