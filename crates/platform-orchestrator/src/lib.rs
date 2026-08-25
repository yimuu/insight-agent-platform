//! Pure durable Run and typed-controller decisions.
//!
//! The module performs no I/O and never reads a wall clock. Repositories provide committed facts
//! and database-observed time; application services persist returned decisions atomically.

#![allow(async_fn_in_trait)]

mod expression;
mod scope_environment;

pub use expression::{
    DataPortKey, ExactDataPortRef, ExpressionError, ExpressionFieldName, ExpressionLimits,
    TypedExpressionProgram, TypedInstruction, MAX_EXPRESSION_FIELD_BYTES,
    MAX_EXPRESSION_INPUT_PORTS, MAX_EXPRESSION_INSTRUCTIONS, MAX_EXPRESSION_STACK_DEPTH,
};
pub use scope_environment::{
    resolve_scope_inputs, ExactRunValueRef, ScopeDataBinding, ScopeDataEnvironmentSnapshot,
    ScopeEnvironmentLimits,
};

use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, CandidateSelectionMode, CandidateSelectionPolicyDocument, ClosedJsonValue,
    CommandAudit, CommandOutcome, DataClassification, ExactDeploymentRef, ExactPolicyBinding,
    Failure, HardLimitProfile, InteractionKind, JobState, JsonLimits, LimitUnit,
    NodeExecutionState, PlanNodeKind, PrincipalSnapshot, ResourceId, ResourceKind,
    RunBindingsSnapshot, RunState, Sha256Digest, ValueRef,
};
use insight_platform_jobs::WakeContract;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{collections::BTreeMap, error::Error, fmt};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PlanNodeKey(String);

impl PlanNodeKey {
    pub fn new(value: String) -> Result<Self, OrchestratorError> {
        let bytes = value.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 128
            || !bytes[0].is_ascii_alphanumeric()
            || !bytes.iter().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-' | b'.' | b':')
            })
        {
            return Err(OrchestratorError::InvalidPlanNodeKey);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for PlanNodeKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinPolicy {
    AllSuccess,
    AllSettled,
    Quorum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinRemainderPolicy {
    Cancel,
    Drain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MapFailurePolicy {
    FailFast,
    AllSettled,
    BoundedErrorCount { maximum_failures: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortAssignment {
    pub output_port: ExactDataPortRef,
    pub expression: TypedExpressionProgram,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchArm {
    pub when: TypedExpressionProgram,
    pub target: PlanNodeKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoopCarriedPort {
    pub body_output_port: ExactDataPortRef,
    pub next_iteration_port: ExactDataPortRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDependencyKind {
    Model,
    Capability,
    Context,
    ChildAgent,
    Skill,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDependencySlot {
    pub kind: RuntimeDependencyKind,
    pub requirement_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildBudgetLimit {
    pub maximum_duration_milliseconds: u64,
    pub maximum_model_tokens: u64,
    pub maximum_capability_calls: u32,
    pub maximum_artifact_bytes: u64,
    pub maximum_descendant_runs: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HumanTaskDefinition {
    Interaction {
        interaction_kind: InteractionKind,
        eligible_principal_rule_digest: Sha256Digest,
        safe_prompt_key: String,
    },
    HumanWork {
        eligible_principal_rule_digest: Sha256Digest,
        safe_prompt_key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateSelectionEvidence {
    pub schema_version: u32,
    pub slot_id: String,
    pub policy_revision_id: ResourceId,
    pub policy_semantic_digest: Sha256Digest,
    pub ordered_candidate_deployment_digests: Vec<Sha256Digest>,
    pub route_value: Option<ExactRunValueRef>,
    pub selected_deployment: ExactDeploymentRef,
    pub result_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

pub fn derive_candidate_selection(
    slot_id: &str,
    policy: &ExactPolicyBinding,
    document: &CandidateSelectionPolicyDocument,
    candidates: &[ExactDeploymentRef],
    route: Option<(&ExactRunValueRef, &ClosedJsonValue)>,
) -> Result<CandidateSelectionEvidence, OrchestratorError> {
    policy
        .validate()
        .map_err(|_| OrchestratorError::InvalidCandidateSelection)?;
    document
        .validate()
        .map_err(|_| OrchestratorError::InvalidCandidateSelection)?;
    if !is_stable_code(slot_id)
        || candidates.is_empty()
        || candidates.len() > 512
        || candidates.iter().any(|candidate| {
            candidate.validate().is_err() || candidate.resource_kind != candidates[0].resource_kind
        })
        || candidates
            .windows(2)
            .any(|pair| candidate_ordering_key(&pair[0]) >= candidate_ordering_key(&pair[1]))
    {
        return Err(OrchestratorError::InvalidCandidateSelection);
    }
    let selected_index = match (document.mode, route) {
        (CandidateSelectionMode::OnlyCandidate, None) if candidates.len() == 1 => 0,
        (CandidateSelectionMode::OrderedFirst, None) => 0,
        (CandidateSelectionMode::RouteHash, Some((reference, value)))
            if reference.value_id.kind() == ResourceKind::RunValue
                && reference.schema_digest == value.schema_digest
                && reference.content_digest == value.canonical_digest
                && document.route_schema_digest.as_ref() == Some(&value.schema_digest)
                && value.validate().is_ok() =>
        {
            digest_modulo(&value.canonical_digest, candidates.len())?
        }
        _ => return Err(OrchestratorError::InvalidCandidateSelection),
    };
    let selected_deployment = candidates[selected_index].clone();
    let result_value = serde_json::to_value(&selected_deployment)
        .map_err(|_| OrchestratorError::Canonicalization)?;
    let result_digest = canonical_digest(&result_value)
        .map_err(|_| OrchestratorError::Canonicalization)?
        .parse()
        .map_err(|_| OrchestratorError::Canonicalization)?;
    let ordered_candidate_deployment_digests = candidates
        .iter()
        .map(|candidate| candidate.deployment_digest.clone())
        .collect::<Vec<_>>();
    let route_value = route.map(|(reference, _)| reference.clone());
    let canonical_value = serde_json::to_value((
        1_u32,
        slot_id,
        &policy.revision.revision_id,
        &policy.revision.semantic_digest,
        &ordered_candidate_deployment_digests,
        &route_value,
        &selected_deployment,
        &result_digest,
    ))
    .map_err(|_| OrchestratorError::Canonicalization)?;
    let evidence_digest = canonical_digest(&canonical_value)
        .map_err(|_| OrchestratorError::Canonicalization)?
        .parse()
        .map_err(|_| OrchestratorError::Canonicalization)?;
    Ok(CandidateSelectionEvidence {
        schema_version: 1,
        slot_id: slot_id.to_owned(),
        policy_revision_id: policy.revision.revision_id.clone(),
        policy_semantic_digest: policy.revision.semantic_digest.clone(),
        ordered_candidate_deployment_digests,
        route_value,
        selected_deployment,
        result_digest,
        canonical_digest: evidence_digest,
    })
}

fn candidate_ordering_key(candidate: &ExactDeploymentRef) -> (String, String) {
    (
        candidate.deployment_id.to_string(),
        candidate.deployment_digest.to_string(),
    )
}

fn digest_modulo(digest: &Sha256Digest, modulus: usize) -> Result<usize, OrchestratorError> {
    let hexadecimal = digest
        .as_str()
        .strip_prefix("sha256:")
        .ok_or(OrchestratorError::InvalidCandidateSelection)?;
    let mut remainder = 0_usize;
    for index in (0..hexadecimal.len()).step_by(2) {
        let byte = u8::from_str_radix(&hexadecimal[index..index + 2], 16)
            .map_err(|_| OrchestratorError::InvalidCandidateSelection)?;
        remainder = (remainder * 256 + usize::from(byte)) % modulus;
    }
    Ok(remainder)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeNode {
    Start {
        next: PlanNodeKey,
    },
    Compute {
        assignments: Vec<PortAssignment>,
        next: PlanNodeKey,
    },
    Branch {
        ordered_arms: Vec<BranchArm>,
        otherwise: PlanNodeKey,
    },
    Fork {
        legs: Vec<PlanNodeKey>,
        join: PlanNodeKey,
    },
    Join {
        policy: JoinPolicy,
        quorum: Option<u16>,
        remainder: Option<JoinRemainderPolicy>,
        next: PlanNodeKey,
    },
    Map {
        items: TypedExpressionProgram,
        item_port: ExactDataPortRef,
        body: PlanNodeKey,
        next: PlanNodeKey,
        maximum_items: u32,
        failure_policy: MapFailurePolicy,
    },
    Loop {
        condition: TypedExpressionProgram,
        carried_ports: Vec<LoopCarriedPort>,
        body: PlanNodeKey,
        exit: PlanNodeKey,
        maximum_iterations: u32,
    },
    ErrorBoundary {
        body: PlanNodeKey,
        handlers: BTreeMap<String, PlanNodeKey>,
    },
    ModelLoop {
        model_slot_id: String,
        skill_slot_ids: Vec<String>,
        capability_slot_ids: Vec<String>,
        input: ExactDataPortRef,
        model_route: Option<ExactDataPortRef>,
        output: ExactDataPortRef,
        maximum_rounds: u16,
        maximum_capability_calls: u32,
        maximum_parallel_calls_per_round: u16,
        token_budget: u64,
        resume: PlanNodeKey,
    },
    CapabilityCall {
        capability_slot_id: String,
        input: ExactDataPortRef,
        candidate_route: Option<ExactDataPortRef>,
        output: ExactDataPortRef,
        attempt_limit: u16,
        retry_backoff_milliseconds: u64,
        resume: PlanNodeKey,
    },
    ContextQuery {
        context_slot_id: String,
        request: ExactDataPortRef,
        result: ExactDataPortRef,
        maximum_items: u32,
        resume: PlanNodeKey,
    },
    ChildAgentCall {
        child_agent_slot_id: String,
        input: ExactDataPortRef,
        candidate_route: Option<ExactDataPortRef>,
        output: ExactDataPortRef,
        budget: ChildBudgetLimit,
        cancellation_policy: ChildCancellationPolicy,
        attempt_limit: u16,
        retry_backoff_milliseconds: u64,
        resume: PlanNodeKey,
    },
    HumanTask {
        definition: HumanTaskDefinition,
        response: ExactDataPortRef,
        timeout_milliseconds: u64,
        resume: PlanNodeKey,
    },
    TimerWait {
        delay_milliseconds: u64,
        resume: PlanNodeKey,
    },
    SignalWait {
        signal_key: String,
        payload: Option<ExactDataPortRef>,
        timeout_milliseconds: u64,
        resume: PlanNodeKey,
    },
    Return {
        value: ExactDataPortRef,
    },
    Raise {
        failure: ExactDataPortRef,
    },
}

impl RuntimeNode {
    pub const fn kind(&self) -> PlanNodeKind {
        match self {
            Self::Start { .. } => PlanNodeKind::Start,
            Self::Compute { .. } => PlanNodeKind::Compute,
            Self::Branch { .. } => PlanNodeKind::Branch,
            Self::Fork { .. } => PlanNodeKind::Fork,
            Self::Join { .. } => PlanNodeKind::Join,
            Self::Map { .. } => PlanNodeKind::Map,
            Self::Loop { .. } => PlanNodeKind::Loop,
            Self::ErrorBoundary { .. } => PlanNodeKind::ErrorBoundary,
            Self::ModelLoop { .. } => PlanNodeKind::ModelLoop,
            Self::CapabilityCall { .. } => PlanNodeKind::CapabilityCall,
            Self::ContextQuery { .. } => PlanNodeKind::ContextQuery,
            Self::ChildAgentCall { .. } => PlanNodeKind::ChildAgentCall,
            Self::HumanTask { .. } => PlanNodeKind::HumanTask,
            Self::TimerWait { .. } => PlanNodeKind::TimerWait,
            Self::SignalWait { .. } => PlanNodeKind::SignalWait,
            Self::Return { .. } => PlanNodeKind::Return,
            Self::Raise { .. } => PlanNodeKind::Raise,
        }
    }

    fn references(&self) -> Vec<&PlanNodeKey> {
        match self {
            Self::Start { next }
            | Self::Compute { next, .. }
            | Self::ModelLoop { resume: next, .. }
            | Self::CapabilityCall { resume: next, .. }
            | Self::ContextQuery { resume: next, .. }
            | Self::ChildAgentCall { resume: next, .. }
            | Self::HumanTask { resume: next, .. }
            | Self::TimerWait { resume: next, .. }
            | Self::SignalWait { resume: next, .. } => vec![next],
            Self::Branch {
                ordered_arms,
                otherwise,
            } => ordered_arms
                .iter()
                .map(|arm| &arm.target)
                .chain(std::iter::once(otherwise))
                .collect(),
            Self::Fork { legs, join } => legs.iter().chain(std::iter::once(join)).collect(),
            Self::Join { next, .. } => vec![next],
            Self::Map { body, next, .. } => vec![body, next],
            Self::Loop { body, exit, .. } => vec![body, exit],
            Self::ErrorBoundary { body, handlers } => {
                std::iter::once(body).chain(handlers.values()).collect()
            }
            Self::Return { .. } | Self::Raise { .. } => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanLimits {
    pub maximum_nodes: usize,
    pub maximum_edges: usize,
    pub maximum_fan_out: usize,
    pub maximum_map_items: u32,
    pub maximum_loop_iterations: u32,
    pub maximum_error_handlers: usize,
    pub expression: ExpressionLimits,
}

impl PlanLimits {
    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, OrchestratorError> {
        profile
            .validate()
            .map_err(|_| OrchestratorError::InvalidPlan)?;
        let registry = &profile.registry_plan;
        if registry.plan_nodes.unit != LimitUnit::Count
            || registry.plan_edges.unit != LimitUnit::Count
            || registry.branch_legs.unit != LimitUnit::Count
            || registry.map_items.unit != LimitUnit::Items
            || registry.loop_iterations.unit != LimitUnit::Count
        {
            return Err(OrchestratorError::InvalidPlan);
        }
        Ok(Self {
            maximum_nodes: usize::try_from(registry.plan_nodes.q1_default)
                .map_err(|_| OrchestratorError::InvalidPlan)?,
            maximum_edges: usize::try_from(registry.plan_edges.q1_default)
                .map_err(|_| OrchestratorError::InvalidPlan)?,
            maximum_fan_out: usize::try_from(registry.branch_legs.q1_default)
                .map_err(|_| OrchestratorError::InvalidPlan)?,
            maximum_map_items: u32::try_from(registry.map_items.q1_default)
                .map_err(|_| OrchestratorError::InvalidPlan)?,
            maximum_loop_iterations: u32::try_from(registry.loop_iterations.q1_default)
                .map_err(|_| OrchestratorError::InvalidPlan)?,
            maximum_error_handlers: usize::try_from(registry.branch_legs.q1_default)
                .map_err(|_| OrchestratorError::InvalidPlan)?,
            expression: ExpressionLimits::from_profile(profile)
                .map_err(|_| OrchestratorError::InvalidPlan)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePlan {
    pub plan_version: u32,
    pub interface_revision_id: ResourceId,
    pub entry_node_id: PlanNodeKey,
    pub dependency_slots: BTreeMap<String, RuntimeDependencySlot>,
    pub nodes: BTreeMap<PlanNodeKey, RuntimeNode>,
}

impl RuntimePlan {
    pub fn validate(&self, limits: PlanLimits) -> Result<(), OrchestratorError> {
        if self.plan_version != 4
            || self.interface_revision_id.kind() != ResourceKind::AgentInterfaceRevision
            || limits.maximum_nodes == 0
            || limits.maximum_edges == 0
            || limits.maximum_fan_out == 0
            || limits.maximum_map_items == 0
            || limits.maximum_loop_iterations == 0
            || limits.maximum_error_handlers == 0
            || self.nodes.is_empty()
            || self.nodes.len() > limits.maximum_nodes
            || !self.nodes.contains_key(&self.entry_node_id)
            || self
                .dependency_slots
                .keys()
                .any(|slot_id| !is_stable_code(slot_id))
        {
            return Err(OrchestratorError::InvalidPlan);
        }
        let mut edge_count = 0_usize;
        for (node_key, node) in &self.nodes {
            validate_node(node_key, node, limits)?;
            let references = node.references();
            edge_count = edge_count
                .checked_add(references.len())
                .ok_or(OrchestratorError::InvalidPlan)?;
            if edge_count > limits.maximum_edges {
                return Err(OrchestratorError::InvalidPlan);
            }
            if references
                .into_iter()
                .any(|reference| !self.nodes.contains_key(reference))
            {
                return Err(OrchestratorError::UnknownPlanNodeReference);
            }
        }
        self.validate_loop_carried_regions()?;
        self.validate_external_leaf_contracts()?;
        self.validate_terminal_ports()?;
        Ok(())
    }

    fn validate_external_leaf_contracts(&self) -> Result<(), OrchestratorError> {
        for (node_key, node) in &self.nodes {
            for (slot_id, expected_kind) in node_dependency_slots(node) {
                if self.dependency_slots.get(slot_id).map(|slot| slot.kind) != Some(expected_kind) {
                    return Err(OrchestratorError::InvalidPlan);
                }
            }
            for port in external_leaf_inputs(node) {
                let Some(producer_key) = port.producer_node_id() else {
                    continue;
                };
                let producer = self
                    .nodes
                    .get(producer_key)
                    .ok_or(OrchestratorError::InvalidPlan)?;
                if producer_key == node_key
                    || !node_declares_output(producer, port)
                    || !self.node_reaches(producer_key, node_key)?
                {
                    return Err(OrchestratorError::InvalidPlan);
                }
            }
        }
        Ok(())
    }

    fn validate_terminal_ports(&self) -> Result<(), OrchestratorError> {
        for (terminal_key, node) in &self.nodes {
            let port = match node {
                RuntimeNode::Return { value } => value,
                RuntimeNode::Raise { failure } => failure,
                _ => continue,
            };
            let Some(producer_key) = port.producer_node_id() else {
                continue;
            };
            let producer = self
                .nodes
                .get(producer_key)
                .ok_or(OrchestratorError::InvalidPlan)?;
            if producer_key == terminal_key
                || !node_declares_output(producer, port)
                || !self.node_reaches(producer_key, terminal_key)?
            {
                return Err(OrchestratorError::InvalidPlan);
            }
        }
        Ok(())
    }

    fn node_reaches(
        &self,
        source: &PlanNodeKey,
        target: &PlanNodeKey,
    ) -> Result<bool, OrchestratorError> {
        let mut visited = std::collections::BTreeSet::new();
        let mut pending = vec![source.clone()];
        while let Some(candidate) = pending.pop() {
            if candidate == *target {
                return Ok(true);
            }
            if !visited.insert(candidate.clone()) {
                continue;
            }
            let node = self
                .nodes
                .get(&candidate)
                .ok_or(OrchestratorError::UnknownPlanNodeReference)?;
            pending.extend(node.references().into_iter().cloned());
        }
        Ok(false)
    }

    fn validate_loop_carried_regions(&self) -> Result<(), OrchestratorError> {
        for (loop_key, node) in &self.nodes {
            let RuntimeNode::Loop {
                carried_ports,
                body,
                exit,
                ..
            } = node
            else {
                continue;
            };
            if carried_ports.is_empty() {
                continue;
            }
            let mut reachable = std::collections::BTreeSet::new();
            let mut pending = vec![body.clone()];
            while let Some(candidate) = pending.pop() {
                if candidate == *loop_key
                    || candidate == *exit
                    || !reachable.insert(candidate.clone())
                {
                    continue;
                }
                let candidate_node = self
                    .nodes
                    .get(&candidate)
                    .ok_or(OrchestratorError::UnknownPlanNodeReference)?;
                pending.extend(candidate_node.references().into_iter().cloned());
            }
            if carried_ports.iter().any(|port| {
                port.body_output_port
                    .producer_node_id()
                    .is_none_or(|producer| !reachable.contains(producer))
            }) {
                return Err(OrchestratorError::InvalidPlan);
            }
        }
        Ok(())
    }

    pub fn canonical_digest(&self, limits: PlanLimits) -> Result<Sha256Digest, OrchestratorError> {
        self.validate(limits)?;
        let value = serde_json::to_value(self).map_err(|_| OrchestratorError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| OrchestratorError::Canonicalization)?
            .parse()
            .map_err(|_| OrchestratorError::Canonicalization)
    }

    pub fn validate_terminal_schema_digests(
        &self,
        output_schema_digest: &Sha256Digest,
        error_schema_digest: &Sha256Digest,
    ) -> Result<(), OrchestratorError> {
        if self.nodes.values().any(|node| match node {
            RuntimeNode::Return { value } => value.schema_digest() != output_schema_digest,
            RuntimeNode::Raise { failure } => failure.schema_digest() != error_schema_digest,
            _ => false,
        }) {
            return Err(OrchestratorError::InvalidPlan);
        }
        Ok(())
    }

    pub fn node(&self, key: &PlanNodeKey) -> Result<&RuntimeNode, OrchestratorError> {
        self.nodes
            .get(key)
            .ok_or(OrchestratorError::UnknownPlanNodeReference)
    }
}

fn node_declares_output(node: &RuntimeNode, port: &ExactDataPortRef) -> bool {
    match node {
        RuntimeNode::Compute { assignments, .. } => assignments
            .iter()
            .any(|assignment| &assignment.output_port == port),
        RuntimeNode::Map { item_port, .. } => item_port == port,
        RuntimeNode::Loop { carried_ports, .. } => carried_ports
            .iter()
            .any(|carried| &carried.next_iteration_port == port),
        RuntimeNode::ModelLoop { output, .. }
        | RuntimeNode::CapabilityCall { output, .. }
        | RuntimeNode::ChildAgentCall { output, .. } => output == port,
        RuntimeNode::ContextQuery { result, .. } => result == port,
        RuntimeNode::HumanTask { response, .. } => response == port,
        RuntimeNode::SignalWait {
            payload: Some(payload),
            ..
        } => payload == port,
        _ => false,
    }
}

fn node_dependency_slots(node: &RuntimeNode) -> Vec<(&str, RuntimeDependencyKind)> {
    match node {
        RuntimeNode::ModelLoop {
            model_slot_id,
            skill_slot_ids,
            capability_slot_ids,
            ..
        } => std::iter::once((model_slot_id.as_str(), RuntimeDependencyKind::Model))
            .chain(
                skill_slot_ids
                    .iter()
                    .map(|slot| (slot.as_str(), RuntimeDependencyKind::Skill)),
            )
            .chain(
                capability_slot_ids
                    .iter()
                    .map(|slot| (slot.as_str(), RuntimeDependencyKind::Capability)),
            )
            .collect(),
        RuntimeNode::CapabilityCall {
            capability_slot_id, ..
        } => vec![(capability_slot_id, RuntimeDependencyKind::Capability)],
        RuntimeNode::ContextQuery {
            context_slot_id, ..
        } => vec![(context_slot_id, RuntimeDependencyKind::Context)],
        RuntimeNode::ChildAgentCall {
            child_agent_slot_id,
            ..
        } => vec![(child_agent_slot_id, RuntimeDependencyKind::ChildAgent)],
        _ => Vec::new(),
    }
}

fn external_leaf_inputs(node: &RuntimeNode) -> Vec<&ExactDataPortRef> {
    match node {
        RuntimeNode::ModelLoop {
            input, model_route, ..
        } => std::iter::once(input).chain(model_route.iter()).collect(),
        RuntimeNode::CapabilityCall {
            input,
            candidate_route,
            ..
        }
        | RuntimeNode::ChildAgentCall {
            input,
            candidate_route,
            ..
        } => std::iter::once(input)
            .chain(candidate_route.iter())
            .collect(),
        RuntimeNode::ContextQuery { request, .. } => vec![request],
        _ => Vec::new(),
    }
}

fn validate_leaf_output(
    node_key: &PlanNodeKey,
    output: &ExactDataPortRef,
) -> Result<(), OrchestratorError> {
    if output.producer_node_id() != Some(node_key) {
        Err(OrchestratorError::InvalidPlan)
    } else {
        Ok(())
    }
}

fn validate_human_task_definition(
    definition: &HumanTaskDefinition,
) -> Result<(), OrchestratorError> {
    let safe_prompt_key = match definition {
        HumanTaskDefinition::Interaction {
            safe_prompt_key, ..
        }
        | HumanTaskDefinition::HumanWork {
            safe_prompt_key, ..
        } => safe_prompt_key,
    };
    if is_stable_code(safe_prompt_key) {
        Ok(())
    } else {
        Err(OrchestratorError::InvalidPlan)
    }
}

fn validate_node(
    node_key: &PlanNodeKey,
    node: &RuntimeNode,
    limits: PlanLimits,
) -> Result<(), OrchestratorError> {
    match node {
        RuntimeNode::Compute { assignments, .. } => {
            validate_assignments(node_key, assignments, limits.expression)
        }
        RuntimeNode::Branch {
            ordered_arms,
            otherwise,
        } => {
            if ordered_arms.is_empty()
                || ordered_arms.len() + 1 > limits.maximum_fan_out
                || ordered_arms.iter().any(|arm| &arm.target == otherwise)
                || has_duplicate_keys(
                    &ordered_arms
                        .iter()
                        .map(|arm| arm.target.clone())
                        .collect::<Vec<_>>(),
                )
                || ordered_arms
                    .iter()
                    .any(|arm| arm.when.validate(limits.expression).is_err())
            {
                Err(OrchestratorError::InvalidPlan)
            } else {
                Ok(())
            }
        }
        RuntimeNode::Fork { legs, join }
            if legs.is_empty()
                || legs.len() > limits.maximum_fan_out
                || legs.iter().any(|leg| leg == join)
                || has_duplicate_keys(legs) =>
        {
            Err(OrchestratorError::InvalidPlan)
        }
        RuntimeNode::Join {
            policy,
            quorum,
            remainder,
            ..
        } => match (policy, quorum, remainder) {
            (JoinPolicy::Quorum, Some(value), Some(_)) if *value > 0 => Ok(()),
            (JoinPolicy::AllSuccess | JoinPolicy::AllSettled, None, None) => Ok(()),
            _ => Err(OrchestratorError::InvalidPlan),
        },
        RuntimeNode::Map {
            items,
            item_port,
            maximum_items,
            ..
        } if *maximum_items == 0
            || *maximum_items > limits.maximum_map_items
            || items.validate(limits.expression).is_err()
            || item_port.producer_node_id() != Some(node_key) =>
        {
            Err(OrchestratorError::InvalidPlan)
        }
        RuntimeNode::Map {
            maximum_items,
            failure_policy: MapFailurePolicy::BoundedErrorCount { maximum_failures },
            ..
        } if *maximum_failures == 0 || maximum_failures > maximum_items => {
            Err(OrchestratorError::InvalidPlan)
        }
        RuntimeNode::Loop {
            condition,
            carried_ports,
            maximum_iterations,
            ..
        } if *maximum_iterations == 0
            || *maximum_iterations > limits.maximum_loop_iterations
            || condition.validate(limits.expression).is_err()
            || carried_ports
                .iter()
                .map(|port| &port.next_iteration_port)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != carried_ports.len()
            || carried_ports
                .iter()
                .map(|port| &port.body_output_port)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != carried_ports.len()
            || carried_ports.iter().any(|port| {
                port.next_iteration_port.producer_node_id() != Some(node_key)
                    || port.body_output_port.producer_node_id().is_none()
                    || port.body_output_port.schema_digest()
                        != port.next_iteration_port.schema_digest()
            }) =>
        {
            Err(OrchestratorError::InvalidPlan)
        }
        RuntimeNode::ErrorBoundary { handlers, .. }
            if handlers.len() > limits.maximum_error_handlers
                || handlers.keys().any(|code| !is_stable_code(code)) =>
        {
            Err(OrchestratorError::InvalidPlan)
        }
        RuntimeNode::ModelLoop {
            skill_slot_ids,
            capability_slot_ids,
            output,
            maximum_rounds,
            maximum_capability_calls,
            maximum_parallel_calls_per_round,
            token_budget,
            ..
        } if *maximum_rounds == 0
            || *maximum_capability_calls == 0
            || *maximum_parallel_calls_per_round == 0
            || *maximum_parallel_calls_per_round as u32 > *maximum_capability_calls
            || *token_budget == 0
            || skill_slot_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != skill_slot_ids.len()
            || capability_slot_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != capability_slot_ids.len()
            || validate_leaf_output(node_key, output).is_err() =>
        {
            Err(OrchestratorError::InvalidPlan)
        }
        RuntimeNode::CapabilityCall {
            output,
            attempt_limit,
            retry_backoff_milliseconds,
            ..
        } if *attempt_limit == 0
            || *attempt_limit > 32
            || *retry_backoff_milliseconds == 0
            || *retry_backoff_milliseconds > 60_000
            || validate_leaf_output(node_key, output).is_err() =>
        {
            Err(OrchestratorError::InvalidPlan)
        }
        RuntimeNode::ContextQuery {
            result,
            maximum_items,
            ..
        } if *maximum_items == 0
            || *maximum_items > limits.maximum_map_items
            || validate_leaf_output(node_key, result).is_err() =>
        {
            Err(OrchestratorError::InvalidPlan)
        }
        RuntimeNode::ChildAgentCall {
            output,
            budget,
            attempt_limit,
            retry_backoff_milliseconds,
            ..
        } if budget.maximum_duration_milliseconds == 0
            || budget.maximum_model_tokens == 0
            || budget.maximum_capability_calls == 0
            || budget.maximum_artifact_bytes == 0
            || budget.maximum_descendant_runs == 0
            || *attempt_limit == 0
            || *attempt_limit > 32
            || *retry_backoff_milliseconds == 0
            || *retry_backoff_milliseconds > 60_000
            || validate_leaf_output(node_key, output).is_err() =>
        {
            Err(OrchestratorError::InvalidPlan)
        }
        RuntimeNode::HumanTask {
            definition,
            response,
            timeout_milliseconds,
            ..
        } if *timeout_milliseconds == 0
            || validate_human_task_definition(definition).is_err()
            || validate_leaf_output(node_key, response).is_err() =>
        {
            Err(OrchestratorError::InvalidPlan)
        }
        RuntimeNode::TimerWait {
            delay_milliseconds, ..
        } if *delay_milliseconds == 0 => Err(OrchestratorError::InvalidPlan),
        RuntimeNode::SignalWait {
            signal_key,
            payload,
            timeout_milliseconds,
            ..
        } if *timeout_milliseconds == 0
            || !is_stable_code(signal_key)
            || payload
                .as_ref()
                .is_some_and(|port| validate_leaf_output(node_key, port).is_err()) =>
        {
            Err(OrchestratorError::InvalidPlan)
        }
        _ => Ok(()),
    }
}

fn validate_assignments(
    node_key: &PlanNodeKey,
    assignments: &[PortAssignment],
    limits: ExpressionLimits,
) -> Result<(), OrchestratorError> {
    let outputs = assignments
        .iter()
        .map(|assignment| &assignment.output_port)
        .collect::<std::collections::BTreeSet<_>>();
    if outputs.len() != assignments.len()
        || assignments
            .iter()
            .any(|assignment| assignment.output_port.producer_node_id() != Some(node_key))
    {
        return Err(OrchestratorError::InvalidPlan);
    }
    let mut available = std::collections::BTreeSet::new();
    for assignment in assignments {
        if assignment.expression.validate(limits).is_err()
            || assignment
                .expression
                .input_ports
                .iter()
                .any(|input| outputs.contains(input) && !available.contains(input))
        {
            return Err(OrchestratorError::InvalidPlan);
        }
        available.insert(&assignment.output_port);
    }
    Ok(())
}

fn has_duplicate_keys(values: &[PlanNodeKey]) -> bool {
    values.windows(2).any(|pair| pair[0] == pair[1])
        || values
            .iter()
            .enumerate()
            .any(|(index, value)| values[index + 1..].contains(value))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildOutcome {
    Active,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl ChildOutcome {
    const fn is_terminal(self) -> bool {
        !matches!(self, Self::Active)
    }

    const fn is_success(self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableWaitKind {
    HumanTask,
    Timer,
    Signal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableWaitOutcome {
    Succeeded,
    Declined,
    TimedOut,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControllerObservation {
    None,
    Branch {
        selected: PlanNodeKey,
    },
    Join {
        children: Vec<ChildOutcome>,
    },
    Map {
        item_count: u32,
    },
    MapSettlement {
        children: Vec<ChildOutcome>,
    },
    Loop {
        iteration: u32,
        condition: bool,
    },
    ErrorBoundary {
        failure_code: Option<String>,
    },
    DurableWait {
        wait_kind: DurableWaitKind,
        outcome: DurableWaitOutcome,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedExpressionInput {
    pub run_value_id: ResourceId,
    pub port: ExactDataPortRef,
    pub classification: DataClassification,
    pub value: ClosedJsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerDerivedOutput {
    pub port: ExactDataPortRef,
    pub value: ClosedJsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerInputEvidence {
    pub run_value_id: ResourceId,
    pub port: ExactDataPortRef,
    pub classification: DataClassification,
    pub content_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerObservationEvidence {
    pub schema_version: u32,
    pub node_execution_id: ResourceId,
    pub node_execution_version: i64,
    pub expression_digests: Vec<Sha256Digest>,
    pub inputs: Vec<ControllerInputEvidence>,
    pub result_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerEvaluation {
    pub observation: ControllerObservation,
    pub outputs: Vec<ControllerDerivedOutput>,
    pub evaluated_results: Vec<ClosedJsonValue>,
    pub effective_classification: DataClassification,
    pub evidence: ControllerObservationEvidence,
}

impl ControllerObservationEvidence {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if self.schema_version != 1
            || self.node_execution_id.kind() != ResourceKind::NodeExecution
            || self.node_execution_version <= 0
            || self.inputs.iter().any(|input| {
                input.run_value_id.kind() != ResourceKind::RunValue
                    || input.content_digest.as_str().is_empty()
            })
            || self
                .inputs
                .iter()
                .map(|input| &input.port)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.inputs.len()
            || controller_evidence_digest(
                &self.node_execution_id,
                self.node_execution_version,
                &self.expression_digests,
                &self.inputs,
                &self.result_digest,
            )? != self.canonical_digest
        {
            return Err(OrchestratorError::InvalidControllerEvidence);
        }
        Ok(())
    }
}

impl ControllerEvaluation {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        self.evidence.validate()?;
        if self
            .outputs
            .iter()
            .map(|output| &output.port)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != self.outputs.len()
            || self.outputs.iter().any(|output| {
                &output.value.schema_digest != output.port.schema_digest()
                    || output.value.validate().is_err()
            })
            || self
                .evaluated_results
                .iter()
                .any(|result| result.validate().is_err())
        {
            return Err(OrchestratorError::InvalidControllerEvidence);
        }
        let value = serde_json::to_value((
            &self.observation,
            &self.outputs,
            &self.evaluated_results,
            self.effective_classification,
        ))
        .map_err(|_| OrchestratorError::Canonicalization)?;
        let result_digest: Sha256Digest = canonical_digest(&value)
            .map_err(|_| OrchestratorError::Canonicalization)?
            .parse()
            .map_err(|_| OrchestratorError::Canonicalization)?;
        if result_digest != self.evidence.result_digest {
            return Err(OrchestratorError::InvalidControllerEvidence);
        }
        Ok(())
    }
}

pub fn derive_expression_controller(
    node: &RuntimeNode,
    committed_inputs: Vec<CommittedExpressionInput>,
    node_execution_id: ResourceId,
    node_execution_version: i64,
    loop_iteration: u32,
    limits: ExpressionLimits,
) -> Result<ControllerEvaluation, OrchestratorError> {
    if node_execution_id.kind() != ResourceKind::NodeExecution || node_execution_version <= 0 {
        return Err(OrchestratorError::InvalidControllerEvidence);
    }
    let mut values = BTreeMap::new();
    let mut authorities = BTreeMap::new();
    let mut classifications = BTreeMap::new();
    let mut input_classifications = Vec::new();
    for input in committed_inputs {
        if input.run_value_id.kind() != ResourceKind::RunValue
            || &input.value.schema_digest != input.port.schema_digest()
            || input.value.validate().is_err()
            || values
                .insert(input.port.clone(), input.value.clone())
                .is_some()
        {
            return Err(OrchestratorError::InvalidControllerEvidence);
        }
        input_classifications.push(input.classification);
        classifications.insert(input.port.clone(), input.classification);
        authorities.insert(input.port, input.run_value_id);
    }

    let required_ports = required_expression_inputs(node)?;
    let programs = match node {
        RuntimeNode::Compute { assignments, .. } => assignments
            .iter()
            .map(|assignment| &assignment.expression)
            .collect::<Vec<_>>(),
        RuntimeNode::Branch { ordered_arms, .. } => {
            ordered_arms.iter().map(|arm| &arm.when).collect()
        }
        RuntimeNode::Map { items, .. } => vec![items],
        RuntimeNode::Loop { condition, .. } => vec![condition],
        _ => return Err(OrchestratorError::ObservationMismatch),
    };
    let seen_ports = required_ports
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    if authorities.len() != required_ports.len()
        || authorities.keys().any(|port| !seen_ports.contains(port))
    {
        return Err(OrchestratorError::InvalidControllerEvidence);
    }
    let used_inputs = required_ports
        .iter()
        .map(|port| {
            let run_value_id = authorities
                .get(port)
                .ok_or(OrchestratorError::InvalidControllerEvidence)?;
            let value = values
                .get(port)
                .ok_or(OrchestratorError::InvalidControllerEvidence)?;
            Ok(ControllerInputEvidence {
                run_value_id: run_value_id.clone(),
                port: port.clone(),
                classification: classifications
                    .get(port)
                    .copied()
                    .ok_or(OrchestratorError::InvalidControllerEvidence)?,
                content_digest: value.canonical_digest.clone(),
            })
        })
        .collect::<Result<Vec<_>, OrchestratorError>>()?;
    let expression_digests = programs
        .iter()
        .map(|program| program.semantic_digest.clone())
        .collect::<Vec<_>>();
    let mut outputs = Vec::new();
    let mut evaluated_results = Vec::new();
    let observation = match node {
        RuntimeNode::Compute { assignments, .. } => {
            for assignment in assignments {
                let result = evaluate_controller_program(&assignment.expression, &values, limits)?;
                values.insert(assignment.output_port.clone(), result.clone());
                evaluated_results.push(result.clone());
                outputs.push(ControllerDerivedOutput {
                    port: assignment.output_port.clone(),
                    value: result,
                });
            }
            ControllerObservation::None
        }
        RuntimeNode::Branch {
            ordered_arms,
            otherwise,
        } => {
            let mut selected = otherwise.clone();
            for arm in ordered_arms {
                let result = evaluate_controller_program(&arm.when, &values, limits)?;
                let condition = result
                    .value
                    .as_bool()
                    .ok_or(OrchestratorError::ExpressionEvaluation)?;
                evaluated_results.push(result);
                if condition {
                    selected = arm.target.clone();
                    break;
                }
            }
            ControllerObservation::Branch { selected }
        }
        RuntimeNode::Map {
            items,
            maximum_items,
            ..
        } => {
            let result = evaluate_controller_program(items, &values, limits)?;
            let item_count = u32::try_from(
                result
                    .value
                    .as_array()
                    .ok_or(OrchestratorError::ExpressionEvaluation)?
                    .len(),
            )
            .map_err(|_| OrchestratorError::ExpressionEvaluation)?;
            if item_count > *maximum_items {
                return Err(OrchestratorError::ExpressionEvaluation);
            }
            evaluated_results.push(result);
            ControllerObservation::Map { item_count }
        }
        RuntimeNode::Loop { condition, .. } => {
            let result = evaluate_controller_program(condition, &values, limits)?;
            let condition = result
                .value
                .as_bool()
                .ok_or(OrchestratorError::ExpressionEvaluation)?;
            evaluated_results.push(result);
            ControllerObservation::Loop {
                iteration: loop_iteration,
                condition,
            }
        }
        _ => unreachable!(),
    };

    let effective_classification = derive_expression_classification(&input_classifications);
    let result_value = serde_json::to_value((
        &observation,
        &outputs,
        &evaluated_results,
        effective_classification,
    ))
    .map_err(|_| OrchestratorError::Canonicalization)?;
    let result_digest = canonical_digest(&result_value)
        .map_err(|_| OrchestratorError::Canonicalization)?
        .parse()
        .map_err(|_| OrchestratorError::Canonicalization)?;
    let canonical_digest = controller_evidence_digest(
        &node_execution_id,
        node_execution_version,
        &expression_digests,
        &used_inputs,
        &result_digest,
    )?;
    let evaluation = ControllerEvaluation {
        observation,
        outputs,
        evaluated_results,
        effective_classification,
        evidence: ControllerObservationEvidence {
            schema_version: 1,
            node_execution_id,
            node_execution_version,
            expression_digests,
            inputs: used_inputs,
            result_digest,
            canonical_digest,
        },
    };
    evaluation.validate()?;
    Ok(evaluation)
}

pub fn derive_expression_classification(
    input_classifications: &[DataClassification],
) -> DataClassification {
    input_classifications
        .iter()
        .copied()
        .reduce(DataClassification::join)
        .unwrap_or(DataClassification::Internal)
}

/// Returns the external data dependencies for one expression controller in deterministic Plan
/// order. Compute outputs produced earlier in the same node are deliberately excluded because
/// they are derived in-memory by the closed evaluator, not read from durable Scope authority.
pub fn required_expression_inputs(
    node: &RuntimeNode,
) -> Result<Vec<ExactDataPortRef>, OrchestratorError> {
    let programs = match node {
        RuntimeNode::Compute { assignments, .. } => assignments
            .iter()
            .map(|assignment| &assignment.expression)
            .collect::<Vec<_>>(),
        RuntimeNode::Branch { ordered_arms, .. } => {
            ordered_arms.iter().map(|arm| &arm.when).collect()
        }
        RuntimeNode::Map { items, .. } => vec![items],
        RuntimeNode::Loop { condition, .. } => vec![condition],
        _ => return Err(OrchestratorError::ObservationMismatch),
    };
    let generated_ports = match node {
        RuntimeNode::Compute { assignments, .. } => assignments
            .iter()
            .map(|assignment| assignment.output_port.clone())
            .collect::<std::collections::BTreeSet<_>>(),
        _ => std::collections::BTreeSet::new(),
    };
    let mut required_ports = Vec::new();
    let mut seen_ports = std::collections::BTreeSet::new();
    for program in programs {
        for port in &program.input_ports {
            if !generated_ports.contains(port) && seen_ports.insert(port.clone()) {
                required_ports.push(port.clone());
            }
        }
    }
    Ok(required_ports)
}

fn evaluate_controller_program(
    program: &TypedExpressionProgram,
    values: &BTreeMap<ExactDataPortRef, ClosedJsonValue>,
    limits: ExpressionLimits,
) -> Result<ClosedJsonValue, OrchestratorError> {
    let mut selected = BTreeMap::new();
    for port in &program.input_ports {
        let value = values
            .get(port)
            .ok_or(OrchestratorError::ExpressionEvaluation)?;
        selected.insert(port.clone(), value.clone());
    }
    program
        .evaluate(&selected, limits)
        .map_err(|_| OrchestratorError::ExpressionEvaluation)
}

#[derive(Serialize)]
struct UnsignedControllerEvidence<'a> {
    schema_version: u32,
    node_execution_id: &'a ResourceId,
    node_execution_version: i64,
    expression_digests: &'a [Sha256Digest],
    inputs: &'a [ControllerInputEvidence],
    result_digest: &'a Sha256Digest,
}

fn controller_evidence_digest(
    node_execution_id: &ResourceId,
    node_execution_version: i64,
    expression_digests: &[Sha256Digest],
    inputs: &[ControllerInputEvidence],
    result_digest: &Sha256Digest,
) -> Result<Sha256Digest, OrchestratorError> {
    let value = serde_json::to_value(UnsignedControllerEvidence {
        schema_version: 1,
        node_execution_id,
        node_execution_version,
        expression_digests,
        inputs,
        result_digest,
    })
    .map_err(|_| OrchestratorError::Canonicalization)?;
    canonical_digest(&value)
        .map_err(|_| OrchestratorError::Canonicalization)?
        .parse()
        .map_err(|_| OrchestratorError::Canonicalization)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeafKind {
    ModelLoop,
    Capability,
    Context,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ControllerDecision {
    CompleteNode {
        activate: Vec<PlanNodeKey>,
    },
    FanOut {
        activate: Vec<PlanNodeKey>,
        create_pending: Vec<PlanNodeKey>,
    },
    WaitForChildren,
    OpenMapItems {
        body: PlanNodeKey,
        next: PlanNodeKey,
        item_count: u32,
        failure_policy: MapFailurePolicy,
    },
    OpenLoopIteration {
        body: PlanNodeKey,
        iteration: u32,
    },
    EnterErrorHandler {
        target: PlanNodeKey,
    },
    DispatchLeaf {
        kind: LeafKind,
        resume: PlanNodeKey,
    },
    CreateChildRun {
        resume: PlanNodeKey,
    },
    CreateDurableWait {
        kind: DurableWaitKind,
        resume: PlanNodeKey,
    },
    CompleteRun {
        value: ExactDataPortRef,
    },
    FailNode {
        code: &'static str,
    },
    FailRun {
        failure: ExactDataPortRef,
    },
}

pub fn decide_controller(
    node: &RuntimeNode,
    observation: &ControllerObservation,
) -> Result<ControllerDecision, OrchestratorError> {
    match (node, observation) {
        (
            RuntimeNode::Start { next } | RuntimeNode::Compute { next, .. },
            ControllerObservation::None,
        ) => Ok(ControllerDecision::CompleteNode {
            activate: vec![next.clone()],
        }),
        (
            RuntimeNode::Branch {
                ordered_arms,
                otherwise,
            },
            ControllerObservation::Branch { selected },
        ) if ordered_arms.iter().any(|arm| &arm.target == selected) || otherwise == selected => {
            Ok(ControllerDecision::CompleteNode {
                activate: vec![selected.clone()],
            })
        }
        (RuntimeNode::Fork { legs, join }, ControllerObservation::None) => {
            Ok(ControllerDecision::FanOut {
                activate: legs.clone(),
                create_pending: vec![join.clone()],
            })
        }
        (
            RuntimeNode::Join {
                policy,
                quorum,
                remainder: _,
                next,
            },
            ControllerObservation::Join { children },
        ) => decide_join(*policy, *quorum, next, children),
        (
            RuntimeNode::Map {
                body,
                next,
                maximum_items,
                failure_policy,
                ..
            },
            ControllerObservation::Map { item_count },
        ) if item_count <= maximum_items => Ok(ControllerDecision::OpenMapItems {
            body: body.clone(),
            next: next.clone(),
            item_count: *item_count,
            failure_policy: *failure_policy,
        }),
        (
            RuntimeNode::Map {
                next,
                failure_policy,
                ..
            },
            ControllerObservation::MapSettlement { children },
        ) => decide_map_settlement(*failure_policy, next, children),
        (
            RuntimeNode::Loop {
                body,
                exit,
                maximum_iterations,
                ..
            },
            ControllerObservation::Loop {
                iteration,
                condition,
            },
        ) => {
            if !condition {
                Ok(ControllerDecision::CompleteNode {
                    activate: vec![exit.clone()],
                })
            } else if iteration < maximum_iterations {
                Ok(ControllerDecision::OpenLoopIteration {
                    body: body.clone(),
                    iteration: *iteration,
                })
            } else {
                Ok(ControllerDecision::FailNode {
                    code: "budget_exhausted",
                })
            }
        }
        (
            RuntimeNode::ErrorBoundary { body, handlers },
            ControllerObservation::ErrorBoundary { failure_code },
        ) => match failure_code {
            None => Ok(ControllerDecision::CompleteNode {
                activate: vec![body.clone()],
            }),
            Some(code) => handlers
                .get(code)
                .cloned()
                .map(|target| ControllerDecision::EnterErrorHandler { target })
                .ok_or(OrchestratorError::UnhandledFailure),
        },
        (RuntimeNode::ModelLoop { resume, .. }, ControllerObservation::None) => {
            Ok(ControllerDecision::DispatchLeaf {
                kind: LeafKind::ModelLoop,
                resume: resume.clone(),
            })
        }
        (RuntimeNode::CapabilityCall { resume, .. }, ControllerObservation::None) => {
            Ok(ControllerDecision::DispatchLeaf {
                kind: LeafKind::Capability,
                resume: resume.clone(),
            })
        }
        (RuntimeNode::ContextQuery { resume, .. }, ControllerObservation::None) => {
            Ok(ControllerDecision::DispatchLeaf {
                kind: LeafKind::Context,
                resume: resume.clone(),
            })
        }
        (RuntimeNode::ChildAgentCall { resume, .. }, ControllerObservation::None) => {
            Ok(ControllerDecision::CreateChildRun {
                resume: resume.clone(),
            })
        }
        (
            RuntimeNode::HumanTask { resume, .. },
            ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::HumanTask,
                outcome: DurableWaitOutcome::Succeeded,
            },
        )
        | (
            RuntimeNode::TimerWait { resume, .. },
            ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::Timer,
                outcome: DurableWaitOutcome::Succeeded,
            },
        )
        | (
            RuntimeNode::SignalWait { resume, .. },
            ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::Signal,
                outcome: DurableWaitOutcome::Succeeded,
            },
        ) => Ok(ControllerDecision::CompleteNode {
            activate: vec![resume.clone()],
        }),
        (
            RuntimeNode::HumanTask { .. },
            ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::HumanTask,
                outcome: DurableWaitOutcome::TimedOut,
            },
        )
        | (
            RuntimeNode::TimerWait { .. },
            ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::Timer,
                outcome: DurableWaitOutcome::TimedOut,
            },
        )
        | (
            RuntimeNode::SignalWait { .. },
            ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::Signal,
                outcome: DurableWaitOutcome::TimedOut,
            },
        ) => Ok(ControllerDecision::FailNode { code: "timeout" }),
        (
            RuntimeNode::HumanTask { .. },
            ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::HumanTask,
                outcome: DurableWaitOutcome::Declined,
            },
        ) => Ok(ControllerDecision::FailNode {
            code: "interaction_declined",
        }),
        (
            RuntimeNode::HumanTask { .. },
            ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::HumanTask,
                outcome: DurableWaitOutcome::Cancelled,
            },
        )
        | (
            RuntimeNode::TimerWait { .. },
            ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::Timer,
                outcome: DurableWaitOutcome::Cancelled,
            },
        )
        | (
            RuntimeNode::SignalWait { .. },
            ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::Signal,
                outcome: DurableWaitOutcome::Cancelled,
            },
        ) => Ok(ControllerDecision::FailNode { code: "cancelled" }),
        (RuntimeNode::HumanTask { resume, .. }, ControllerObservation::None) => {
            Ok(ControllerDecision::CreateDurableWait {
                kind: DurableWaitKind::HumanTask,
                resume: resume.clone(),
            })
        }
        (RuntimeNode::TimerWait { resume, .. }, ControllerObservation::None) => {
            Ok(ControllerDecision::CreateDurableWait {
                kind: DurableWaitKind::Timer,
                resume: resume.clone(),
            })
        }
        (RuntimeNode::SignalWait { resume, .. }, ControllerObservation::None) => {
            Ok(ControllerDecision::CreateDurableWait {
                kind: DurableWaitKind::Signal,
                resume: resume.clone(),
            })
        }
        (RuntimeNode::Return { value }, ControllerObservation::None) => {
            Ok(ControllerDecision::CompleteRun {
                value: value.clone(),
            })
        }
        (RuntimeNode::Raise { failure }, ControllerObservation::None) => {
            Ok(ControllerDecision::FailRun {
                failure: failure.clone(),
            })
        }
        _ => Err(OrchestratorError::ObservationMismatch),
    }
}

fn decide_map_settlement(
    policy: MapFailurePolicy,
    next: &PlanNodeKey,
    children: &[ChildOutcome],
) -> Result<ControllerDecision, OrchestratorError> {
    if children.is_empty() {
        return Err(OrchestratorError::ObservationMismatch);
    }
    let failed = children
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                ChildOutcome::Failed | ChildOutcome::Cancelled | ChildOutcome::TimedOut
            )
        })
        .count();
    match policy {
        MapFailurePolicy::FailFast if failed > 0 => Ok(ControllerDecision::FailNode {
            code: "map_item_failed",
        }),
        MapFailurePolicy::BoundedErrorCount { maximum_failures }
            if failed > maximum_failures as usize =>
        {
            Ok(ControllerDecision::FailNode {
                code: "map_error_limit_exceeded",
            })
        }
        _ if children.iter().all(|outcome| outcome.is_terminal()) => {
            Ok(ControllerDecision::CompleteNode {
                activate: vec![next.clone()],
            })
        }
        _ => Ok(ControllerDecision::WaitForChildren),
    }
}

fn decide_join(
    policy: JoinPolicy,
    quorum: Option<u16>,
    next: &PlanNodeKey,
    children: &[ChildOutcome],
) -> Result<ControllerDecision, OrchestratorError> {
    if children.is_empty() {
        return Err(OrchestratorError::ObservationMismatch);
    }
    let successes = children.iter().filter(|child| child.is_success()).count();
    let active = children.iter().filter(|child| !child.is_terminal()).count();
    match policy {
        JoinPolicy::AllSuccess
            if children
                .iter()
                .any(|child| child.is_terminal() && !child.is_success()) =>
        {
            Ok(ControllerDecision::FailNode {
                code: "child_failed",
            })
        }
        JoinPolicy::AllSuccess if active == 0 => Ok(ControllerDecision::CompleteNode {
            activate: vec![next.clone()],
        }),
        JoinPolicy::AllSettled if active == 0 => Ok(ControllerDecision::CompleteNode {
            activate: vec![next.clone()],
        }),
        JoinPolicy::Quorum => {
            let required = usize::from(quorum.ok_or(OrchestratorError::InvalidPlan)?);
            if successes >= required {
                Ok(ControllerDecision::CompleteNode {
                    activate: vec![next.clone()],
                })
            } else if successes + active < required {
                Ok(ControllerDecision::FailNode {
                    code: "quorum_unreachable",
                })
            } else {
                Ok(ControllerDecision::WaitForChildren)
            }
        }
        _ => Ok(ControllerDecision::WaitForChildren),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunControlSnapshot {
    pub pause_generation: u64,
    pub pause_requested: bool,
    pub cancel_generation: u64,
    pub cancel_requested_at: Option<DateTime<Utc>>,
    pub cancel_reason_code: Option<String>,
    pub cancel_principal: Option<PrincipalSnapshot>,
    pub timeout_generation: u64,
    pub timeout_requested_at: Option<DateTime<Utc>>,
    pub timeout_observed_run_state: Option<String>,
    pub timeout_observed_run_version: Option<u64>,
}

impl RunControlSnapshot {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        let cancel_fields = [
            self.cancel_reason_code.is_some(),
            self.cancel_principal.is_some(),
        ];
        if cancel_fields
            .iter()
            .any(|present| *present != self.cancel_requested_at.is_some())
            || self
                .cancel_reason_code
                .as_ref()
                .is_some_and(|code| !is_stable_code(code))
            || self
                .cancel_principal
                .as_ref()
                .is_some_and(|snapshot| snapshot.validate().is_err())
            || (self.cancel_requested_at.is_some() && self.cancel_generation == 0)
            || (self.timeout_requested_at.is_some() && self.timeout_generation == 0)
            || self.timeout_requested_at.is_some()
                != (self.timeout_observed_run_state.is_some()
                    && self.timeout_observed_run_version.is_some())
            || self
                .timeout_observed_run_state
                .as_ref()
                .is_some_and(|state| !is_stable_code(state))
            || self.timeout_observed_run_version == Some(0)
        {
            return Err(OrchestratorError::InvalidRunControl);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, OrchestratorError> {
        self.validate()?;
        let value = serde_json::to_value(self).map_err(|_| OrchestratorError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| OrchestratorError::Canonicalization)?
            .parse()
            .map_err(|_| OrchestratorError::Canonicalization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlDecision {
    Unchanged(RunControlSnapshot),
    Updated(RunControlSnapshot),
}

pub fn decide_pause(
    current: &RunControlSnapshot,
    expected_generation: u64,
    requested: bool,
) -> Result<ControlDecision, OrchestratorError> {
    current.validate()?;
    if expected_generation != current.pause_generation {
        return Err(OrchestratorError::StaleGeneration);
    }
    if current.pause_requested == requested {
        return Ok(ControlDecision::Unchanged(current.clone()));
    }
    let mut next = current.clone();
    next.pause_generation = next
        .pause_generation
        .checked_add(1)
        .ok_or(OrchestratorError::CounterOverflow)?;
    next.pause_requested = requested;
    Ok(ControlDecision::Updated(next))
}

pub fn decide_cancel(
    current: &RunControlSnapshot,
    expected_generation: u64,
    database_observed_at: DateTime<Utc>,
    reason_code: String,
    principal: PrincipalSnapshot,
) -> Result<ControlDecision, OrchestratorError> {
    current.validate()?;
    if expected_generation != current.cancel_generation {
        return Err(OrchestratorError::StaleGeneration);
    }
    if current.cancel_requested_at.is_some() {
        if current.cancel_reason_code.as_ref() == Some(&reason_code)
            && current.cancel_principal.as_ref() == Some(&principal)
        {
            return Ok(ControlDecision::Unchanged(current.clone()));
        }
        return Err(OrchestratorError::ControlConflict);
    }
    if !is_stable_code(&reason_code) || principal.validate().is_err() {
        return Err(OrchestratorError::InvalidRunControl);
    }
    let mut next = current.clone();
    next.cancel_generation = next
        .cancel_generation
        .checked_add(1)
        .ok_or(OrchestratorError::CounterOverflow)?;
    next.cancel_requested_at = Some(database_observed_at);
    next.cancel_reason_code = Some(reason_code);
    next.cancel_principal = Some(principal);
    Ok(ControlDecision::Updated(next))
}

pub fn decide_timeout(
    current: &RunControlSnapshot,
    expected_generation: u64,
    database_observed_at: DateTime<Utc>,
    deadline: DateTime<Utc>,
    observed_run_state: String,
    observed_run_version: u64,
) -> Result<ControlDecision, OrchestratorError> {
    current.validate()?;
    if expected_generation != current.timeout_generation {
        return Err(OrchestratorError::StaleGeneration);
    }
    if database_observed_at < deadline
        || !is_stable_code(&observed_run_state)
        || observed_run_version == 0
    {
        return Err(OrchestratorError::InvalidTimeoutObservation);
    }
    if current.timeout_requested_at.is_some() {
        if current.timeout_observed_run_state.as_ref() == Some(&observed_run_state)
            && current.timeout_observed_run_version == Some(observed_run_version)
        {
            return Ok(ControlDecision::Unchanged(current.clone()));
        }
        return Err(OrchestratorError::ControlConflict);
    }
    let mut next = current.clone();
    next.timeout_generation = next
        .timeout_generation
        .checked_add(1)
        .ok_or(OrchestratorError::CounterOverflow)?;
    next.timeout_requested_at = Some(database_observed_at);
    next.timeout_observed_run_state = Some(observed_run_state);
    next.timeout_observed_run_version = Some(observed_run_version);
    Ok(ControlDecision::Updated(next))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrchestrationConvergenceReason {
    CancelRequested,
    TimeoutObserved,
    DeadlineExceeded,
    AttemptLimitExhausted,
}

impl OrchestrationConvergenceReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CancelRequested => "cancel_requested",
            Self::TimeoutObserved => "timeout_observed",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::AttemptLimitExhausted => "attempt_limit_exhausted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestrationConvergenceDecision {
    pub reason: OrchestrationConvergenceReason,
    pub run_state: RunState,
    pub node_state: NodeExecutionState,
    pub job_state: JobState,
    pub control: RunControlSnapshot,
}

#[derive(Debug, Clone)]
pub struct OrchestrationConvergenceFacts {
    pub run_state: RunState,
    pub run_version: u64,
    pub run_deadline: DateTime<Utc>,
    pub control: RunControlSnapshot,
    pub node_state: NodeExecutionState,
    pub job_state: JobState,
    pub job_attempt_count: u32,
    pub job_attempt_limit: u32,
    pub job_deadline: DateTime<Utc>,
    pub lease_expires_at: Option<DateTime<Utc>>,
}

/// Chooses the single terminal outcome for an orchestration owner from committed facts.
/// Timeout wins only after its intent is committed or the database clock crosses the immutable
/// deadline; cancellation wins before that; exhausted attempts are considered only when no
/// generation can still produce a fenced result.
pub fn decide_orchestration_convergence(
    facts: OrchestrationConvergenceFacts,
    database_now: DateTime<Utc>,
) -> Result<Option<OrchestrationConvergenceDecision>, OrchestratorError> {
    facts.control.validate()?;
    if facts.run_version == 0
        || facts.job_attempt_limit == 0
        || facts.job_attempt_count > facts.job_attempt_limit
    {
        return Err(OrchestratorError::InvalidRunControl);
    }

    let deadline_exceeded =
        database_now >= facts.run_deadline || database_now >= facts.job_deadline;
    let (reason, run_state, node_state, job_state, control) =
        if facts.control.timeout_requested_at.is_some() || deadline_exceeded {
            let control = if facts.control.timeout_requested_at.is_some() {
                facts.control.clone()
            } else {
                match decide_timeout(
                    &facts.control,
                    facts.control.timeout_generation,
                    database_now,
                    facts.run_deadline.min(facts.job_deadline),
                    facts.run_state.as_str().to_owned(),
                    facts.run_version,
                )? {
                    ControlDecision::Updated(control) | ControlDecision::Unchanged(control) => {
                        control
                    }
                }
            };
            (
                if deadline_exceeded {
                    OrchestrationConvergenceReason::DeadlineExceeded
                } else {
                    OrchestrationConvergenceReason::TimeoutObserved
                },
                RunState::TimedOut,
                NodeExecutionState::TimedOut,
                JobState::TimedOut,
                control,
            )
        } else if facts.control.cancel_requested_at.is_some() {
            (
                OrchestrationConvergenceReason::CancelRequested,
                RunState::Cancelled,
                NodeExecutionState::Cancelled,
                JobState::Cancelled,
                facts.control.clone(),
            )
        } else if facts.job_attempt_count >= facts.job_attempt_limit
            && facts
                .lease_expires_at
                .is_none_or(|expires_at| database_now >= expires_at)
        {
            (
                OrchestrationConvergenceReason::AttemptLimitExhausted,
                RunState::Failed,
                NodeExecutionState::Failed,
                JobState::Failed,
                facts.control.clone(),
            )
        } else {
            return Ok(None);
        };

    let node_can_converge = facts.node_state.can_transition_to(node_state)
        || (facts.node_state == NodeExecutionState::Running
            && node_state == NodeExecutionState::Cancelled
            && NodeExecutionState::Running.can_transition_to(NodeExecutionState::Cancelling)
            && NodeExecutionState::Cancelling.can_transition_to(NodeExecutionState::Cancelled));
    if !facts.run_state.can_transition_to(run_state)
        || !node_can_converge
        || !facts.job_state.can_transition_to(job_state)
    {
        return Err(OrchestratorError::InvalidRunControl);
    }
    Ok(Some(OrchestrationConvergenceDecision {
        reason,
        run_state,
        node_state,
        job_state,
        control,
    }))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunInputValue {
    pub value_id: ResourceId,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub value: ValueRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestrationJobPayload {
    pub bindings_digest: Sha256Digest,
    pub node_execution_id: ResourceId,
    pub root_scope_id: ResourceId,
    pub retry_backoff_milliseconds: u64,
    pub wake_contract: Option<WakeContract>,
    /// A durable external-leaf terminal failure awaiting the shared orchestration
    /// ErrorBoundary/structured-scope convergence path. Such a Job must never dispatch the
    /// external leaf again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_failure: Option<Failure>,
    /// An exact terminal Model response that contains tool intents and must be consumed by the
    /// durable orchestration controller rather than dispatched as a fresh Model activation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_tool_continuation: Option<ModelToolContinuation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolContinuation {
    pub model_turn_id: ResourceId,
    pub response_value_id: ResourceId,
    pub response_digest: Sha256Digest,
    pub round_ordinal: u16,
    pub tool_intent_count: u16,
    /// Empty while the controller still has to fan out tool intents. Once every invocation has
    /// committed, the terminal winner fills this exact, call-ordered result set and wakes the
    /// controller to assemble the next ModelTurn.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<ModelToolResultReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolResultReference {
    pub call_id: String,
    pub invocation_id: ResourceId,
    pub output_value_id: ResourceId,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub classification: DataClassification,
}

impl ModelToolResultReference {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if self.call_id.is_empty()
            || self.call_id.len() > 128
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.output_value_id.kind() != ResourceKind::RunValue
        {
            return Err(OrchestratorError::InvalidRunAdmission);
        }
        Ok(())
    }
}

impl ModelToolContinuation {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.response_value_id.kind() != ResourceKind::RunValue
            || self.round_ordinal == 0
            || self.tool_intent_count == 0
            || (!self.results.is_empty()
                && self.results.len() != usize::from(self.tool_intent_count))
            || self.results.iter().any(|result| result.validate().is_err())
            || self
                .results
                .iter()
                .map(|result| &result.call_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.results.len()
        {
            return Err(OrchestratorError::InvalidRunAdmission);
        }
        Ok(())
    }
}

impl OrchestrationJobPayload {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if self.node_execution_id.kind() != ResourceKind::NodeExecution
            || self.root_scope_id.kind() != ResourceKind::ScopeInstance
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > 60_000
            || self
                .convergence_failure
                .as_ref()
                .is_some_and(|failure| failure.validate(1_024).is_err())
            || self
                .model_tool_continuation
                .as_ref()
                .is_some_and(|continuation| continuation.validate().is_err())
            || [
                self.wake_contract.is_some(),
                self.convergence_failure.is_some(),
                self.model_tool_continuation.is_some(),
            ]
            .into_iter()
            .filter(|present| *present)
            .count()
                > 1
        {
            return Err(OrchestratorError::InvalidRunAdmission);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunAncestrySnapshot {
    pub root_run_id: ResourceId,
    pub parent_run_id: Option<ResourceId>,
    pub parent_node_execution_id: Option<ResourceId>,
    pub parent_child_link_id: Option<ResourceId>,
    pub depth: u16,
    pub ancestry_agent_deployment_ids: Vec<ResourceId>,
}

impl RunAncestrySnapshot {
    pub fn root(run_id: ResourceId, agent_deployment_id: ResourceId) -> Self {
        Self {
            root_run_id: run_id,
            parent_run_id: None,
            parent_node_execution_id: None,
            parent_child_link_id: None,
            depth: 0,
            ancestry_agent_deployment_ids: vec![agent_deployment_id],
        }
    }

    pub fn validate(&self, run_id: &ResourceId) -> Result<(), OrchestratorError> {
        let parent_fields = [
            self.parent_run_id.is_some(),
            self.parent_node_execution_id.is_some(),
            self.parent_child_link_id.is_some(),
        ];
        if self.root_run_id.kind() != ResourceKind::Run
            || self
                .parent_run_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Run)
            || self
                .parent_node_execution_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::NodeExecution)
            || self
                .parent_child_link_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::ChildRunLink)
            || parent_fields
                .iter()
                .any(|present| *present != (self.depth > 0))
            || (self.depth == 0 && &self.root_run_id != run_id)
            || self.ancestry_agent_deployment_ids.is_empty()
            || self.ancestry_agent_deployment_ids.len() > 32
            || self
                .ancestry_agent_deployment_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::AgentDeployment)
        {
            return Err(OrchestratorError::InvalidRunAdmission);
        }
        let mut unique = self.ancestry_agent_deployment_ids.clone();
        unique.sort();
        unique.dedup();
        if unique.len() != self.ancestry_agent_deployment_ids.len() {
            return Err(OrchestratorError::InvalidRunAdmission);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildLinkState {
    Running,
    Waiting,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl ChildLinkState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Cancelling => "cancelling",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }

    pub const fn can_transition_to(self, target: Self) -> bool {
        matches!(
            (self, target),
            (
                Self::Running,
                Self::Waiting
                    | Self::Cancelling
                    | Self::Succeeded
                    | Self::Failed
                    | Self::Cancelled
                    | Self::TimedOut
            ) | (
                Self::Waiting,
                Self::Running
                    | Self::Cancelling
                    | Self::Succeeded
                    | Self::Failed
                    | Self::Cancelled
                    | Self::TimedOut
            ) | (
                Self::Cancelling,
                Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
            )
        )
    }
}

impl fmt::Display for ChildLinkState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for ChildLinkState {
    type Err = OrchestratorError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "running" => Ok(Self::Running),
            "waiting" => Ok(Self::Waiting),
            "cancelling" => Ok(Self::Cancelling),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "timed_out" => Ok(Self::TimedOut),
            _ => Err(OrchestratorError::InvalidChildRun),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildCancellationPolicy {
    CascadeAndWait,
    CascadeWithDeadline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildBudget {
    pub deadline: DateTime<Utc>,
    pub maximum_model_tokens: u64,
    pub maximum_capability_calls: u32,
    pub maximum_artifact_bytes: u64,
    pub maximum_descendant_runs: u32,
}

impl ChildBudget {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if self.maximum_model_tokens == 0
            || self.maximum_capability_calls == 0
            || self.maximum_artifact_bytes == 0
        {
            return Err(OrchestratorError::InvalidChildBudget);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunLinkPayload {
    pub parent_attempt_ordinal: u16,
    pub child_agent_deployment: ExactDeploymentRef,
    pub input_digest: Sha256Digest,
    pub cancellation_policy: ChildCancellationPolicy,
    pub budget: ChildBudget,
}

impl ChildRunLinkPayload {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        self.child_agent_deployment
            .validate()
            .map_err(|_| OrchestratorError::InvalidChildRun)?;
        self.budget.validate()?;
        if self.parent_attempt_ordinal == 0
            || self.child_agent_deployment.resource_kind != ResourceKind::AgentDeployment
        {
            return Err(OrchestratorError::InvalidChildRun);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildRunLinkProjection {
    pub tenant_id: ResourceId,
    pub child_link_id: ResourceId,
    pub parent_run_id: ResourceId,
    pub parent_node_execution_id: ResourceId,
    pub child_run_id: ResourceId,
    pub state: ChildLinkState,
    pub generation: u64,
    pub version: u64,
    pub payload: ChildRunLinkPayload,
    pub deadline: DateTime<Utc>,
    pub terminal_at: Option<DateTime<Utc>>,
}

impl ChildRunLinkProjection {
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        self.payload.validate()?;
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.child_link_id.kind() != ResourceKind::ChildRunLink
            || self.parent_run_id.kind() != ResourceKind::Run
            || self.parent_node_execution_id.kind() != ResourceKind::NodeExecution
            || self.child_run_id.kind() != ResourceKind::Run
            || self.generation == 0
            || self.version == 0
            || self.deadline != self.payload.budget.deadline
            || self.state.is_terminal() != self.terminal_at.is_some()
        {
            return Err(OrchestratorError::InvalidChildRun);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PrepareChildRun {
    pub parent_run_id: ResourceId,
    pub parent_node_execution_id: ResourceId,
    pub child_link_id: ResourceId,
    pub child_run_id: ResourceId,
    pub child_agent_deployment: ExactDeploymentRef,
    pub parent_ancestry: RunAncestrySnapshot,
    pub parent_deadline: DateTime<Utc>,
    pub parent_delegated_budget: Option<ChildBudget>,
    pub parent_delegated_descendant_count: u32,
    pub parent_descendant_count: u32,
    pub maximum_depth: u16,
    pub maximum_descendants: u32,
    pub budget: ChildBudget,
}

pub fn prepare_child_run(
    command: PrepareChildRun,
) -> Result<RunAncestrySnapshot, OrchestratorError> {
    command.budget.validate()?;
    if let Some(parent_budget) = &command.parent_delegated_budget {
        parent_budget.validate()?;
        if command.budget.deadline > parent_budget.deadline
            || command.budget.maximum_model_tokens > parent_budget.maximum_model_tokens
            || command.budget.maximum_capability_calls > parent_budget.maximum_capability_calls
            || command.budget.maximum_artifact_bytes > parent_budget.maximum_artifact_bytes
            || command.budget.maximum_descendant_runs > parent_budget.maximum_descendant_runs
            || command.parent_delegated_descendant_count >= parent_budget.maximum_descendant_runs
            || command.budget.maximum_descendant_runs
                > parent_budget
                    .maximum_descendant_runs
                    .saturating_sub(command.parent_delegated_descendant_count.saturating_add(1))
        {
            return Err(OrchestratorError::InvalidChildBudget);
        }
    } else if command.parent_delegated_descendant_count != 0 {
        return Err(OrchestratorError::InvalidChildBudget);
    }
    command.parent_ancestry.validate(&command.parent_run_id)?;
    command
        .child_agent_deployment
        .validate()
        .map_err(|_| OrchestratorError::InvalidChildRun)?;
    if command.parent_node_execution_id.kind() != ResourceKind::NodeExecution
        || command.child_link_id.kind() != ResourceKind::ChildRunLink
        || command.child_run_id.kind() != ResourceKind::Run
        || command.child_agent_deployment.resource_kind != ResourceKind::AgentDeployment
        || command.maximum_depth == 0
        || command.maximum_depth > 32
        || command.maximum_descendants == 0
        || command.budget.deadline > command.parent_deadline
        || command.parent_descendant_count >= command.maximum_descendants
        || command.budget.maximum_descendant_runs
            > command
                .maximum_descendants
                .saturating_sub(command.parent_descendant_count + 1)
    {
        return Err(OrchestratorError::InvalidChildRun);
    }
    let depth = command
        .parent_ancestry
        .depth
        .checked_add(1)
        .ok_or(OrchestratorError::InvalidChildRun)?;
    if depth > command.maximum_depth {
        return Err(OrchestratorError::ChildDepthExceeded);
    }
    let mut deployments = command.parent_ancestry.ancestry_agent_deployment_ids;
    if deployments.contains(&command.child_agent_deployment.deployment_id) {
        return Err(OrchestratorError::ChildCycle);
    }
    deployments.push(command.child_agent_deployment.deployment_id);
    let ancestry = RunAncestrySnapshot {
        root_run_id: command.parent_ancestry.root_run_id,
        parent_run_id: Some(command.parent_run_id),
        parent_node_execution_id: Some(command.parent_node_execution_id),
        parent_child_link_id: Some(command.child_link_id),
        depth,
        ancestry_agent_deployment_ids: deployments,
    };
    ancestry.validate(&command.child_run_id)?;
    Ok(ancestry)
}

pub fn decide_child_link_cancel(
    current: &ChildRunLinkProjection,
    expected_generation: u64,
    expected_version: u64,
) -> Result<ChildRunLinkProjection, OrchestratorError> {
    current.validate()?;
    if current.generation != expected_generation || current.version != expected_version {
        return Err(OrchestratorError::StaleGeneration);
    }
    if current.state == ChildLinkState::Cancelling {
        return Ok(current.clone());
    }
    if !current.state.can_transition_to(ChildLinkState::Cancelling) {
        return Err(OrchestratorError::ChildFirstWinnerLost);
    }
    let mut next = current.clone();
    next.state = ChildLinkState::Cancelling;
    next.version = next
        .version
        .checked_add(1)
        .ok_or(OrchestratorError::CounterOverflow)?;
    next.validate()?;
    Ok(next)
}

pub fn decide_child_link_terminal(
    current: &ChildRunLinkProjection,
    expected_generation: u64,
    expected_version: u64,
    child_run_state: RunState,
    database_now: DateTime<Utc>,
) -> Result<ChildRunLinkProjection, OrchestratorError> {
    current.validate()?;
    if current.generation != expected_generation
        || current.version != expected_version
        || current.state.is_terminal()
    {
        return Err(OrchestratorError::ChildFirstWinnerLost);
    }
    let target = match child_run_state {
        RunState::Succeeded => ChildLinkState::Succeeded,
        RunState::Failed => ChildLinkState::Failed,
        RunState::Cancelled => ChildLinkState::Cancelled,
        RunState::TimedOut => ChildLinkState::TimedOut,
        _ => return Err(OrchestratorError::InvalidChildRun),
    };
    if !current.state.can_transition_to(target) {
        return Err(OrchestratorError::ChildFirstWinnerLost);
    }
    let mut next = current.clone();
    next.state = target;
    next.version = next
        .version
        .checked_add(1)
        .ok_or(OrchestratorError::CounterOverflow)?;
    next.terminal_at = Some(database_now);
    next.validate()?;
    Ok(next)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunCurrentSnapshot {
    pub schema_version: u32,
    pub control: RunControlSnapshot,
    pub ancestry: RunAncestrySnapshot,
    pub input_value_id: ResourceId,
    pub output_value_id: Option<ResourceId>,
    pub failure: Option<Failure>,
    pub waiting_reason: Option<String>,
}

impl RunCurrentSnapshot {
    pub fn initial(
        run_id: ResourceId,
        agent_deployment_id: ResourceId,
        input_value_id: ResourceId,
    ) -> Self {
        Self {
            schema_version: 1,
            control: RunControlSnapshot {
                pause_generation: 0,
                pause_requested: false,
                cancel_generation: 0,
                cancel_requested_at: None,
                cancel_reason_code: None,
                cancel_principal: None,
                timeout_generation: 0,
                timeout_requested_at: None,
                timeout_observed_run_state: None,
                timeout_observed_run_version: None,
            },
            ancestry: RunAncestrySnapshot::root(run_id, agent_deployment_id),
            input_value_id,
            output_value_id: None,
            failure: None,
            waiting_reason: None,
        }
    }

    pub fn validate(&self, run_id: &ResourceId) -> Result<(), OrchestratorError> {
        self.control.validate()?;
        self.ancestry.validate(run_id)?;
        if self.schema_version != 1
            || self.input_value_id.kind() != ResourceKind::RunValue
            || self
                .output_value_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::RunValue)
            || self
                .failure
                .as_ref()
                .is_some_and(|failure| failure.validate(1_024).is_err())
            || self
                .waiting_reason
                .as_ref()
                .is_some_and(|reason| !is_stable_code(reason))
        {
            return Err(OrchestratorError::InvalidRunAdmission);
        }
        Ok(())
    }
}

impl RunInputValue {
    pub fn validate(&self, inline_limits: JsonLimits) -> Result<(), OrchestratorError> {
        if self.value_id.kind() != ResourceKind::RunValue {
            return Err(OrchestratorError::InvalidRunAdmission);
        }
        self.value
            .validate(inline_limits)
            .map_err(|_| OrchestratorError::InvalidRunInput)?;
        match &self.value {
            ValueRef::Inline { value } => {
                let actual: Sha256Digest = canonical_digest(value)
                    .map_err(|_| OrchestratorError::Canonicalization)?
                    .parse()
                    .map_err(|_| OrchestratorError::Canonicalization)?;
                if actual != self.content_digest {
                    return Err(OrchestratorError::InvalidRunInput);
                }
            }
            ValueRef::Artifact { artifact } => {
                if artifact.content_digest() != &self.content_digest
                    || artifact.classification() != self.classification
                {
                    return Err(OrchestratorError::InvalidRunInput);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AdmitRun {
    pub audit: CommandAudit,
    pub admission_scope_id: ResourceId,
    pub run_id: ResourceId,
    pub agent_deployment_id: ResourceId,
    pub root_scope_id: ResourceId,
    pub entry_node_execution_id: ResourceId,
    pub orchestration_job_id: ResourceId,
    pub entry_plan_node_key: PlanNodeKey,
    pub entry_node_kind: PlanNodeKind,
    pub bindings: RunBindingsSnapshot,
    pub input: RunInputValue,
    pub deadline: DateTime<Utc>,
    pub inline_limits: JsonLimits,
    pub attempt_limit: u16,
    pub retry_backoff_milliseconds: u64,
}

impl AdmitRun {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
        validate_audit(&self.audit, now)?;
        self.bindings
            .validate()
            .map_err(|_| OrchestratorError::InvalidRunAdmission)?;
        self.input.validate(self.inline_limits)?;
        if self.run_id.kind() != ResourceKind::Run
            || !matches!(
                self.admission_scope_id.kind(),
                ResourceKind::Agent | ResourceKind::AgentDeployment
            )
            || self.agent_deployment_id.kind() != ResourceKind::AgentDeployment
            || self.root_scope_id.kind() != ResourceKind::ScopeInstance
            || self.entry_node_execution_id.kind() != ResourceKind::NodeExecution
            || self.orchestration_job_id.kind() != ResourceKind::Job
            || self.bindings.agent.deployment_id != self.agent_deployment_id
            || self.deadline <= now
            || self.attempt_limit == 0
            || self.attempt_limit > 32
            || self.retry_backoff_milliseconds == 0
            || self.retry_backoff_milliseconds > 60_000
        {
            return Err(OrchestratorError::InvalidRunAdmission);
        }
        Ok(())
    }

    pub fn initial_current_snapshot(&self) -> RunCurrentSnapshot {
        RunCurrentSnapshot::initial(
            self.run_id.clone(),
            self.agent_deployment_id.clone(),
            self.input.value_id.clone(),
        )
    }

    pub fn orchestration_job_payload(&self) -> OrchestrationJobPayload {
        OrchestrationJobPayload {
            bindings_digest: self.bindings.canonical_digest.clone(),
            node_execution_id: self.entry_node_execution_id.clone(),
            root_scope_id: self.root_scope_id.clone(),
            retry_backoff_milliseconds: self.retry_backoff_milliseconds,
            wake_contract: None,
            convergence_failure: None,
            model_tool_continuation: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SetRunPause {
    pub audit: CommandAudit,
    pub run_id: ResourceId,
    pub expected_run_version: i64,
    pub expected_pause_generation: u64,
    pub requested: bool,
}

impl SetRunPause {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
        validate_audit(&self.audit, now)?;
        validate_run_fence(&self.run_id, self.expected_run_version)
    }
}

#[derive(Debug, Clone)]
pub struct RequestRunCancel {
    pub audit: CommandAudit,
    pub run_id: ResourceId,
    pub expected_run_version: i64,
    pub expected_cancel_generation: u64,
    pub reason_code: String,
}

impl RequestRunCancel {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
        validate_audit(&self.audit, now)?;
        validate_run_fence(&self.run_id, self.expected_run_version)?;
        if !is_stable_code(&self.reason_code) {
            return Err(OrchestratorError::InvalidRunControl);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ObserveRunTimeout {
    pub audit: CommandAudit,
    pub run_id: ResourceId,
    pub expected_run_version: i64,
    pub expected_timeout_generation: u64,
}

impl ObserveRunTimeout {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
        validate_audit(&self.audit, now)?;
        validate_run_fence(&self.run_id, self.expected_run_version)
    }
}

/// One caller-owned Run transaction. Mutation methods must not commit the outer transaction.
pub trait RunTransaction {
    type Error;
    type RunRecord;

    async fn admit_run(
        &mut self,
        command: AdmitRun,
    ) -> Result<CommandOutcome<Self::RunRecord>, Self::Error>;
    async fn set_run_pause(
        &mut self,
        command: SetRunPause,
    ) -> Result<CommandOutcome<Self::RunRecord>, Self::Error>;
    async fn request_run_cancel(
        &mut self,
        command: RequestRunCancel,
    ) -> Result<CommandOutcome<Self::RunRecord>, Self::Error>;
    async fn observe_run_timeout(
        &mut self,
        command: ObserveRunTimeout,
    ) -> Result<CommandOutcome<Self::RunRecord>, Self::Error>;
    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}

pub trait RunStore {
    type Error;
    type Transaction<'a>: RunTransaction<Error = Self::Error>
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error>;
}

fn validate_audit(audit: &CommandAudit, now: DateTime<Utc>) -> Result<(), OrchestratorError> {
    audit
        .validate_at(now)
        .map_err(|_| OrchestratorError::InvalidAudit)
}

fn validate_run_fence(
    run_id: &ResourceId,
    expected_run_version: i64,
) -> Result<(), OrchestratorError> {
    if run_id.kind() != ResourceKind::Run || expected_run_version <= 0 {
        return Err(OrchestratorError::InvalidRunControl);
    }
    Ok(())
}

fn is_stable_code(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrchestratorError {
    InvalidAudit,
    InvalidPlanNodeKey,
    InvalidPlan,
    UnknownPlanNodeReference,
    ObservationMismatch,
    ExpressionEvaluation,
    InvalidControllerEvidence,
    InvalidCandidateSelection,
    InvalidScopeEnvironment,
    ScopePortConflict,
    ScopePortUnbound,
    UnhandledFailure,
    InvalidRunControl,
    InvalidTimeoutObservation,
    StaleGeneration,
    ControlConflict,
    CounterOverflow,
    Canonicalization,
    InvalidRunAdmission,
    InvalidRunInput,
    InvalidChildRun,
    InvalidChildBudget,
    ChildDepthExceeded,
    ChildCycle,
    ChildFirstWinnerLost,
}

impl fmt::Display for OrchestratorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidAudit => "command audit identity or expiry is invalid",
            Self::InvalidPlanNodeKey => "plan node key is invalid",
            Self::InvalidPlan => "runtime plan is invalid or outside its hard limits",
            Self::UnknownPlanNodeReference => "runtime plan references an unknown node",
            Self::ObservationMismatch => "controller observation does not match the node kind",
            Self::ExpressionEvaluation => {
                "controller expression input, type, result, or hard limit is invalid"
            }
            Self::InvalidControllerEvidence => {
                "controller expression evidence identity or content is invalid"
            }
            Self::InvalidCandidateSelection => {
                "candidate selection policy, inputs, result, or evidence is invalid"
            }
            Self::InvalidScopeEnvironment => {
                "Scope data-port environment is invalid or outside its hard limits"
            }
            Self::ScopePortConflict => "Scope data port is already bound in the local environment",
            Self::ScopePortUnbound => "required Scope data port has no lexical RunValue binding",
            Self::UnhandledFailure => "error boundary has no matching stable failure route",
            Self::InvalidRunControl => "run control snapshot is invalid",
            Self::InvalidTimeoutObservation => "timeout observation is early or invalid",
            Self::StaleGeneration => "run control generation is stale",
            Self::ControlConflict => "run control terminal intent conflicts with committed intent",
            Self::CounterOverflow => "orchestrator counter overflowed",
            Self::Canonicalization => "orchestrator snapshot cannot be canonicalized",
            Self::InvalidRunAdmission => {
                "run admission identity, binding, deadline, or limit is invalid"
            }
            Self::InvalidRunInput => "run input is invalid, unbounded, or has a mismatched digest",
            Self::InvalidChildRun => {
                "child Run identity, deployment, relation, or transition is invalid"
            }
            Self::InvalidChildBudget => "child Run budget is invalid or not bounded",
            Self::ChildDepthExceeded => "child Run depth exceeds the runtime hard limit",
            Self::ChildCycle => "child Run deployment would create a runtime ancestry cycle",
            Self::ChildFirstWinnerLost => "child Run link lost its generation/version first-winner",
        })
    }
}

impl Error for OrchestratorError {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn key(value: &str) -> PlanNodeKey {
        PlanNodeKey::new(value.to_owned()).unwrap()
    }

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn limits() -> PlanLimits {
        PlanLimits {
            maximum_nodes: 64,
            maximum_edges: 256,
            maximum_fan_out: 8,
            maximum_map_items: 16,
            maximum_loop_iterations: 16,
            maximum_error_handlers: 8,
            expression: ExpressionLimits::ABSOLUTE,
        }
    }

    fn literal_program(value: serde_json::Value) -> TypedExpressionProgram {
        TypedExpressionProgram::build(
            vec![],
            vec![TypedInstruction::Literal {
                value: insight_platform_contracts::ClosedJsonValue::build(digest('1'), value)
                    .unwrap(),
            }],
            digest('1'),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap()
    }

    fn item_port(node: &str) -> ExactDataPortRef {
        exact_port(node, "item")
    }

    fn return_input() -> RuntimeNode {
        RuntimeNode::Return {
            value: ExactDataPortRef::RunInput {
                schema_digest: digest('1'),
            },
        }
    }

    fn exact_port(node: &str, port: &str) -> ExactDataPortRef {
        ExactDataPortRef::NodeOutput {
            producer_node_id: key(node),
            port_id: DataPortKey::new(port.to_owned()).unwrap(),
            schema_digest: digest('1'),
        }
    }

    fn load_program(port: ExactDataPortRef) -> TypedExpressionProgram {
        TypedExpressionProgram::build(
            vec![port.clone()],
            vec![TypedInstruction::LoadPort { port }],
            digest('1'),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap()
    }

    #[test]
    fn runtime_plan_is_closed_bounded_and_digestible() {
        let plan = RuntimePlan {
            plan_version: 4,
            interface_revision_id: id("aif_0198f1c5-0787-75e1-a9e8-d95ca0f37001"),
            entry_node_id: key("start"),
            dependency_slots: BTreeMap::new(),
            nodes: BTreeMap::from([
                (
                    key("start"),
                    RuntimeNode::Start {
                        next: key("return"),
                    },
                ),
                (key("return"), return_input()),
            ]),
        };
        plan.validate(limits()).unwrap();
        let mut obsolete = plan.clone();
        obsolete.plan_version = 1;
        assert_eq!(
            obsolete.validate(limits()),
            Err(OrchestratorError::InvalidPlan)
        );
        obsolete.plan_version = 2;
        assert_eq!(
            obsolete.validate(limits()),
            Err(OrchestratorError::InvalidPlan)
        );
        obsolete.plan_version = 3;
        assert_eq!(
            obsolete.validate(limits()),
            Err(OrchestratorError::InvalidPlan)
        );
        assert_eq!(
            plan.canonical_digest(limits()),
            plan.canonical_digest(limits())
        );
        let mut value = serde_json::to_value(&plan).unwrap();
        value["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<RuntimePlan>(value).is_err());
    }

    #[test]
    fn external_leaf_contracts_are_exact_closed_and_bounded() {
        let input = ExactDataPortRef::RunInput {
            schema_digest: digest('1'),
        };
        let output = exact_port("call", "result");
        let mut plan = RuntimePlan {
            plan_version: 4,
            interface_revision_id: id("aif_0198f1c5-0787-75e1-a9e8-d95ca0f37001"),
            entry_node_id: key("call"),
            dependency_slots: BTreeMap::from([(
                "primary_capability".to_owned(),
                RuntimeDependencySlot {
                    kind: RuntimeDependencyKind::Capability,
                    requirement_digest: digest('2'),
                },
            )]),
            nodes: BTreeMap::from([
                (
                    key("call"),
                    RuntimeNode::CapabilityCall {
                        capability_slot_id: "primary_capability".to_owned(),
                        input,
                        candidate_route: None,
                        output: output.clone(),
                        attempt_limit: 3,
                        retry_backoff_milliseconds: 100,
                        resume: key("return"),
                    },
                ),
                (key("return"), RuntimeNode::Return { value: output }),
            ]),
        };
        plan.validate(limits()).unwrap();

        let RuntimeNode::CapabilityCall {
            capability_slot_id, ..
        } = plan.nodes.get_mut(&key("call")).unwrap()
        else {
            panic!("fixture must remain a CapabilityCall")
        };
        *capability_slot_id = "missing".to_owned();
        assert_eq!(plan.validate(limits()), Err(OrchestratorError::InvalidPlan));
    }

    #[test]
    fn candidate_selection_is_closed_deterministic_and_evidence_bound() {
        let policy = ExactPolicyBinding {
            deployment: ExactDeploymentRef::new(
                id("pdep_0198f1c5-0787-75e1-a9e8-d95ca0f37010"),
                digest('a'),
            )
            .unwrap(),
            revision: insight_platform_contracts::ExactVersionRef::new(
                id("prev_0198f1c5-0787-75e1-a9e8-d95ca0f37011"),
                digest('b'),
            )
            .unwrap(),
        };
        let candidates = vec![
            ExactDeploymentRef::new(id("cdep_0198f1c5-0787-75e1-a9e8-d95ca0f37012"), digest('c'))
                .unwrap(),
            ExactDeploymentRef::new(id("cdep_0198f1c5-0787-75e1-a9e8-d95ca0f37013"), digest('d'))
                .unwrap(),
        ];
        let route =
            ClosedJsonValue::build(digest('e'), serde_json::json!({"route": "blue"})).unwrap();
        let route_ref = ExactRunValueRef {
            value_id: id("val_0198f1c5-0787-75e1-a9e8-d95ca0f37014"),
            schema_digest: route.schema_digest.clone(),
            content_digest: route.canonical_digest.clone(),
        };
        let document = CandidateSelectionPolicyDocument {
            schema_version: 1,
            mode: CandidateSelectionMode::RouteHash,
            route_schema_digest: Some(route.schema_digest.clone()),
        };
        let first = derive_candidate_selection(
            "primary_capability",
            &policy,
            &document,
            &candidates,
            Some((&route_ref, &route)),
        )
        .unwrap();
        let replay = derive_candidate_selection(
            "primary_capability",
            &policy,
            &document,
            &candidates,
            Some((&route_ref, &route)),
        )
        .unwrap();
        assert_eq!(first, replay);
        assert!(candidates.contains(&first.selected_deployment));

        let mut reversed = candidates.clone();
        reversed.reverse();
        assert_eq!(
            derive_candidate_selection(
                "primary_capability",
                &policy,
                &document,
                &reversed,
                Some((&route_ref, &route)),
            ),
            Err(OrchestratorError::InvalidCandidateSelection)
        );
        assert_eq!(
            derive_candidate_selection("primary_capability", &policy, &document, &candidates, None,),
            Err(OrchestratorError::InvalidCandidateSelection)
        );
    }

    #[test]
    fn terminal_ports_are_exact_declared_and_reachable() {
        let output = exact_port("compute", "result");
        let plan = RuntimePlan {
            plan_version: 4,
            interface_revision_id: id("aif_0198f1c5-0787-75e1-a9e8-d95ca0f37001"),
            entry_node_id: key("compute"),
            dependency_slots: BTreeMap::new(),
            nodes: BTreeMap::from([
                (
                    key("compute"),
                    RuntimeNode::Compute {
                        assignments: vec![PortAssignment {
                            output_port: output.clone(),
                            expression: literal_program(serde_json::json!({"answer": 42})),
                        }],
                        next: key("return"),
                    },
                ),
                (
                    key("return"),
                    RuntimeNode::Return {
                        value: output.clone(),
                    },
                ),
                (key("other"), return_input()),
            ]),
        };
        plan.validate(limits()).unwrap();
        plan.validate_terminal_schema_digests(&digest('1'), &digest('2'))
            .unwrap();
        assert_eq!(
            plan.validate_terminal_schema_digests(&digest('2'), &digest('2')),
            Err(OrchestratorError::InvalidPlan)
        );

        let mut raised = plan.clone();
        raised.nodes.insert(
            key("return"),
            RuntimeNode::Raise {
                failure: ExactDataPortRef::RunInput {
                    schema_digest: digest('2'),
                },
            },
        );
        raised.validate(limits()).unwrap();
        raised
            .validate_terminal_schema_digests(&digest('1'), &digest('2'))
            .unwrap();

        let mut undeclared = plan.clone();
        let RuntimeNode::Return { value } = undeclared.nodes.get_mut(&key("return")).unwrap()
        else {
            panic!("fixture Return disappeared")
        };
        *value = exact_port("compute", "forged");
        assert_eq!(
            undeclared.validate(limits()),
            Err(OrchestratorError::InvalidPlan)
        );

        let mut unreachable = plan;
        let RuntimeNode::Compute { next, .. } = unreachable.nodes.get_mut(&key("compute")).unwrap()
        else {
            panic!("fixture Compute disappeared")
        };
        *next = key("other");
        assert_eq!(
            unreachable.validate(limits()),
            Err(OrchestratorError::InvalidPlan)
        );
    }

    #[test]
    fn loop_carried_ports_are_exact_schema_matched_and_body_owned() {
        let carried = LoopCarriedPort {
            body_output_port: exact_port("body", "out"),
            next_iteration_port: exact_port("loop", "carried"),
        };
        let plan = RuntimePlan {
            plan_version: 4,
            interface_revision_id: id("aif_0198f1c5-0787-75e1-a9e8-d95ca0f37001"),
            entry_node_id: key("loop"),
            dependency_slots: BTreeMap::new(),
            nodes: BTreeMap::from([
                (
                    key("loop"),
                    RuntimeNode::Loop {
                        condition: literal_program(serde_json::json!(true)),
                        carried_ports: vec![carried.clone()],
                        body: key("body"),
                        exit: key("return"),
                        maximum_iterations: 2,
                    },
                ),
                (
                    key("body"),
                    RuntimeNode::Compute {
                        assignments: vec![PortAssignment {
                            output_port: carried.body_output_port.clone(),
                            expression: literal_program(serde_json::json!({"value": 1})),
                        }],
                        next: key("loop"),
                    },
                ),
                (key("outside"), return_input()),
                (key("return"), return_input()),
            ]),
        };
        plan.validate(limits()).unwrap();

        let mut wrong_schema = plan.clone();
        let RuntimeNode::Loop { carried_ports, .. } =
            wrong_schema.nodes.get_mut(&key("loop")).unwrap()
        else {
            panic!("fixture Loop disappeared")
        };
        carried_ports[0].next_iteration_port = ExactDataPortRef::NodeOutput {
            producer_node_id: key("loop"),
            port_id: DataPortKey::new("carried".to_owned()).unwrap(),
            schema_digest: digest('2'),
        };
        assert_eq!(
            wrong_schema.validate(limits()),
            Err(OrchestratorError::InvalidPlan)
        );

        let mut outside_body = plan;
        let RuntimeNode::Loop { carried_ports, .. } =
            outside_body.nodes.get_mut(&key("loop")).unwrap()
        else {
            panic!("fixture Loop disappeared")
        };
        carried_ports[0].body_output_port = exact_port("outside", "out");
        assert_eq!(
            outside_body.validate(limits()),
            Err(OrchestratorError::InvalidPlan)
        );
    }

    #[test]
    fn compute_assignments_are_exact_topological_and_digest_bound() {
        let future = exact_port("compute", "future");
        let reads_future = TypedExpressionProgram::build(
            vec![future.clone()],
            vec![TypedInstruction::LoadPort {
                port: future.clone(),
            }],
            digest('1'),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        let plan = RuntimePlan {
            plan_version: 4,
            interface_revision_id: id("aif_0198f1c5-0787-75e1-a9e8-d95ca0f37001"),
            entry_node_id: key("compute"),
            dependency_slots: BTreeMap::new(),
            nodes: BTreeMap::from([
                (
                    key("compute"),
                    RuntimeNode::Compute {
                        assignments: vec![PortAssignment {
                            output_port: future,
                            expression: reads_future,
                        }],
                        next: key("return"),
                    },
                ),
                (key("return"), return_input()),
            ]),
        };
        assert_eq!(plan.validate(limits()), Err(OrchestratorError::InvalidPlan));

        let mut forged = literal_program(serde_json::json!(true));
        forged.semantic_digest = digest('f');
        let branch = RuntimePlan {
            plan_version: 4,
            interface_revision_id: id("aif_0198f1c5-0787-75e1-a9e8-d95ca0f37001"),
            entry_node_id: key("branch"),
            dependency_slots: BTreeMap::new(),
            nodes: BTreeMap::from([
                (
                    key("branch"),
                    RuntimeNode::Branch {
                        ordered_arms: vec![BranchArm {
                            when: forged,
                            target: key("left"),
                        }],
                        otherwise: key("right"),
                    },
                ),
                (key("left"), return_input()),
                (key("right"), return_input()),
            ]),
        };
        assert_eq!(
            branch.validate(limits()),
            Err(OrchestratorError::InvalidPlan)
        );
    }

    #[test]
    fn controller_observation_is_derived_from_exact_committed_values() {
        let predicate = exact_port("source", "predicate");
        let node = RuntimeNode::Branch {
            ordered_arms: vec![BranchArm {
                when: load_program(predicate.clone()),
                target: key("selected"),
            }],
            otherwise: key("otherwise"),
        };
        let inputs = vec![CommittedExpressionInput {
            run_value_id: id("val_0198f1c5-0787-75e1-a9e8-d95ca0f37020"),
            port: predicate,
            classification: DataClassification::Confidential,
            value: ClosedJsonValue::build(digest('1'), serde_json::json!(true)).unwrap(),
        }];
        let first = derive_expression_controller(
            &node,
            inputs.clone(),
            id("nod_0198f1c5-0787-75e1-a9e8-d95ca0f37021"),
            3,
            0,
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        let second = derive_expression_controller(
            &node,
            inputs,
            id("nod_0198f1c5-0787-75e1-a9e8-d95ca0f37021"),
            3,
            0,
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.observation,
            ControllerObservation::Branch {
                selected: key("selected")
            }
        );
        assert_eq!(first.evidence.inputs.len(), 1);
        assert_eq!(
            first.effective_classification,
            DataClassification::Confidential
        );
        assert_eq!(first.evidence.expression_digests.len(), 1);
        assert!(first.validate().is_ok());
        let mut forged = first.clone();
        forged.evidence.result_digest = digest('f');
        assert_eq!(
            forged.validate(),
            Err(OrchestratorError::InvalidControllerEvidence)
        );

        let injected = vec![CommittedExpressionInput {
            run_value_id: id("val_0198f1c5-0787-75e1-a9e8-d95ca0f37022"),
            port: exact_port("source", "unexpected"),
            classification: DataClassification::Public,
            value: ClosedJsonValue::build(digest('1'), serde_json::json!(true)).unwrap(),
        }];
        assert_eq!(
            derive_expression_controller(
                &node,
                injected,
                id("nod_0198f1c5-0787-75e1-a9e8-d95ca0f37021"),
                3,
                0,
                ExpressionLimits::ABSOLUTE,
            ),
            Err(OrchestratorError::InvalidControllerEvidence)
        );
    }

    #[test]
    fn expression_classification_is_a_closed_lattice_join_with_internal_empty_default() {
        assert_eq!(
            derive_expression_classification(&[]),
            DataClassification::Internal
        );
        assert_eq!(
            derive_expression_classification(&[DataClassification::Public]),
            DataClassification::Public
        );
        assert_eq!(
            derive_expression_classification(&[
                DataClassification::Public,
                DataClassification::Confidential,
                DataClassification::Internal,
            ]),
            DataClassification::Confidential
        );
        assert_eq!(
            derive_expression_classification(&[
                DataClassification::Confidential,
                DataClassification::Restricted,
            ]),
            DataClassification::Restricted
        );
    }

    #[test]
    fn compute_derivation_chains_outputs_without_external_injection() {
        let first_port = exact_port("compute", "first");
        let second_port = exact_port("compute", "second");
        let increment = TypedExpressionProgram::build(
            vec![first_port.clone()],
            vec![
                TypedInstruction::LoadPort {
                    port: first_port.clone(),
                },
                TypedInstruction::Literal {
                    value: ClosedJsonValue::build(digest('1'), serde_json::json!(1)).unwrap(),
                },
                TypedInstruction::IntegerAdd,
            ],
            digest('1'),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        let node = RuntimeNode::Compute {
            assignments: vec![
                PortAssignment {
                    output_port: first_port,
                    expression: literal_program(serde_json::json!(41)),
                },
                PortAssignment {
                    output_port: second_port,
                    expression: increment,
                },
            ],
            next: key("return"),
        };
        let evaluation = derive_expression_controller(
            &node,
            vec![],
            id("nod_0198f1c5-0787-75e1-a9e8-d95ca0f37023"),
            1,
            0,
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        assert_eq!(
            evaluation.effective_classification,
            DataClassification::Internal
        );
        assert_eq!(evaluation.observation, ControllerObservation::None);
        assert_eq!(evaluation.outputs.len(), 2);
        assert_eq!(evaluation.outputs[1].value.value, serde_json::json!(42));
        assert!(evaluation.evidence.inputs.is_empty());
    }

    #[test]
    fn plan_limits_are_derived_from_the_versioned_q1_profile_and_bound_edges() {
        let profile = insight_platform_contracts::checked_in_hard_limit_profile();
        let derived = PlanLimits::from_profile(&profile).unwrap();
        assert_eq!(derived.maximum_nodes, 2_000);
        assert_eq!(derived.maximum_edges, 8_000);
        assert_eq!(derived.maximum_fan_out, 32);
        assert_eq!(derived.maximum_map_items, 1_000);
        assert_eq!(derived.maximum_loop_iterations, 1_000);

        let start = key("start");
        let finish = key("finish");
        let plan = RuntimePlan {
            plan_version: 4,
            interface_revision_id: id("aif_0198f1c5-0787-75e1-a9e8-d95ca0f37001"),
            entry_node_id: start.clone(),
            dependency_slots: BTreeMap::new(),
            nodes: BTreeMap::from([
                (
                    start,
                    RuntimeNode::Start {
                        next: finish.clone(),
                    },
                ),
                (finish, return_input()),
            ]),
        };
        assert_eq!(
            plan.validate(PlanLimits {
                maximum_edges: 0,
                ..derived
            }),
            Err(OrchestratorError::InvalidPlan)
        );
    }

    #[test]
    fn branch_does_not_activate_unselected_paths() {
        let node = RuntimeNode::Branch {
            ordered_arms: vec![BranchArm {
                when: literal_program(serde_json::json!(false)),
                target: key("left"),
            }],
            otherwise: key("right"),
        };
        assert_eq!(
            decide_controller(
                &node,
                &ControllerObservation::Branch {
                    selected: key("right"),
                },
            ),
            Ok(ControllerDecision::CompleteNode {
                activate: vec![key("right")],
            })
        );
    }

    #[test]
    fn durable_wait_resolution_resumes_or_fails_closed() {
        let response = ExactDataPortRef::NodeOutput {
            producer_node_id: key("task"),
            port_id: DataPortKey::new("response".to_owned()).unwrap(),
            schema_digest: digest('a'),
        };
        let task = RuntimeNode::HumanTask {
            definition: HumanTaskDefinition::HumanWork {
                eligible_principal_rule_digest: digest('b'),
                safe_prompt_key: "review".to_owned(),
            },
            response,
            timeout_milliseconds: 1_000,
            resume: key("next"),
        };
        assert_eq!(
            decide_controller(
                &task,
                &ControllerObservation::DurableWait {
                    wait_kind: DurableWaitKind::HumanTask,
                    outcome: DurableWaitOutcome::Succeeded,
                },
            ),
            Ok(ControllerDecision::CompleteNode {
                activate: vec![key("next")],
            })
        );
        assert_eq!(
            decide_controller(
                &task,
                &ControllerObservation::DurableWait {
                    wait_kind: DurableWaitKind::HumanTask,
                    outcome: DurableWaitOutcome::TimedOut,
                },
            ),
            Ok(ControllerDecision::FailNode { code: "timeout" })
        );
        assert_eq!(
            decide_controller(
                &task,
                &ControllerObservation::DurableWait {
                    wait_kind: DurableWaitKind::Signal,
                    outcome: DurableWaitOutcome::Succeeded,
                },
            ),
            Err(OrchestratorError::ObservationMismatch)
        );
    }

    #[test]
    fn join_and_loop_fail_closed() {
        let join = RuntimeNode::Join {
            policy: JoinPolicy::Quorum,
            quorum: Some(2),
            remainder: Some(JoinRemainderPolicy::Drain),
            next: key("next"),
        };
        assert_eq!(
            decide_controller(
                &join,
                &ControllerObservation::Join {
                    children: vec![ChildOutcome::Succeeded, ChildOutcome::Failed],
                },
            ),
            Ok(ControllerDecision::FailNode {
                code: "quorum_unreachable"
            })
        );
        assert_eq!(
            decide_controller(
                &join,
                &ControllerObservation::Join {
                    children: vec![ChildOutcome::Succeeded, ChildOutcome::Active],
                },
            ),
            Ok(ControllerDecision::WaitForChildren)
        );
        let quorum_one = RuntimeNode::Join {
            policy: JoinPolicy::Quorum,
            quorum: Some(1),
            remainder: Some(JoinRemainderPolicy::Drain),
            next: key("next"),
        };
        assert_eq!(
            decide_controller(
                &quorum_one,
                &ControllerObservation::Join {
                    children: vec![ChildOutcome::Succeeded, ChildOutcome::Active],
                },
            ),
            Ok(ControllerDecision::CompleteNode {
                activate: vec![key("next")]
            })
        );
        let loop_node = RuntimeNode::Loop {
            condition: literal_program(serde_json::json!(true)),
            carried_ports: vec![],
            body: key("body"),
            exit: key("exit"),
            maximum_iterations: 3,
        };
        assert_eq!(
            decide_controller(
                &loop_node,
                &ControllerObservation::Loop {
                    iteration: 3,
                    condition: true,
                },
            ),
            Ok(ControllerDecision::FailNode {
                code: "budget_exhausted"
            })
        );
    }

    #[test]
    fn map_failure_policies_are_bounded_and_deterministic() {
        let all_settled = RuntimeNode::Map {
            items: literal_program(serde_json::json!([])),
            item_port: item_port("map"),
            body: key("body"),
            next: key("next"),
            maximum_items: 4,
            failure_policy: MapFailurePolicy::AllSettled,
        };
        assert_eq!(
            decide_controller(
                &all_settled,
                &ControllerObservation::MapSettlement {
                    children: vec![ChildOutcome::Failed, ChildOutcome::Active],
                },
            ),
            Ok(ControllerDecision::WaitForChildren)
        );
        assert_eq!(
            decide_controller(
                &all_settled,
                &ControllerObservation::MapSettlement {
                    children: vec![ChildOutcome::Failed, ChildOutcome::Cancelled],
                },
            ),
            Ok(ControllerDecision::CompleteNode {
                activate: vec![key("next")],
            })
        );

        let fail_fast = RuntimeNode::Map {
            items: literal_program(serde_json::json!([])),
            item_port: item_port("map"),
            body: key("body"),
            next: key("next"),
            maximum_items: 4,
            failure_policy: MapFailurePolicy::FailFast,
        };
        assert_eq!(
            decide_controller(
                &fail_fast,
                &ControllerObservation::MapSettlement {
                    children: vec![ChildOutcome::Succeeded, ChildOutcome::Failed],
                },
            ),
            Ok(ControllerDecision::FailNode {
                code: "map_item_failed",
            })
        );

        let bounded = RuntimeNode::Map {
            items: literal_program(serde_json::json!([])),
            item_port: item_port("map"),
            body: key("body"),
            next: key("next"),
            maximum_items: 4,
            failure_policy: MapFailurePolicy::BoundedErrorCount {
                maximum_failures: 1,
            },
        };
        assert_eq!(
            decide_controller(
                &bounded,
                &ControllerObservation::MapSettlement {
                    children: vec![ChildOutcome::Failed, ChildOutcome::Succeeded],
                },
            ),
            Ok(ControllerDecision::CompleteNode {
                activate: vec![key("next")],
            })
        );
        assert_eq!(
            decide_controller(
                &bounded,
                &ControllerObservation::MapSettlement {
                    children: vec![
                        ChildOutcome::Failed,
                        ChildOutcome::Cancelled,
                        ChildOutcome::Active,
                    ],
                },
            ),
            Ok(ControllerDecision::FailNode {
                code: "map_error_limit_exceeded",
            })
        );
        assert_eq!(
            decide_controller(
                &bounded,
                &ControllerObservation::MapSettlement {
                    children: Vec::new(),
                },
            ),
            Err(OrchestratorError::ObservationMismatch)
        );

        assert_eq!(
            validate_node(
                &key("map"),
                &RuntimeNode::Map {
                    items: literal_program(serde_json::json!([])),
                    item_port: item_port("map"),
                    body: key("body"),
                    next: key("next"),
                    maximum_items: 2,
                    failure_policy: MapFailurePolicy::BoundedErrorCount {
                        maximum_failures: 3,
                    },
                },
                limits(),
            ),
            Err(OrchestratorError::InvalidPlan)
        );
        assert_eq!(
            validate_node(
                &key("map"),
                &RuntimeNode::Map {
                    items: literal_program(serde_json::json!([])),
                    item_port: item_port("different_map"),
                    body: key("body"),
                    next: key("next"),
                    maximum_items: 2,
                    failure_policy: MapFailurePolicy::AllSettled,
                },
                limits(),
            ),
            Err(OrchestratorError::InvalidPlan)
        );
    }

    fn control() -> RunControlSnapshot {
        RunControlSnapshot {
            pause_generation: 0,
            pause_requested: false,
            cancel_generation: 0,
            cancel_requested_at: None,
            cancel_reason_code: None,
            cancel_principal: None,
            timeout_generation: 0,
            timeout_requested_at: None,
            timeout_observed_run_state: None,
            timeout_observed_run_version: None,
        }
    }

    #[test]
    fn pause_cancel_and_timeout_use_exact_generations() {
        let paused = match decide_pause(&control(), 0, true).unwrap() {
            ControlDecision::Updated(value) => value,
            ControlDecision::Unchanged(_) => panic!("pause must toggle"),
        };
        assert_eq!(paused.pause_generation, 1);
        assert_eq!(
            decide_pause(&paused, 0, false),
            Err(OrchestratorError::StaleGeneration)
        );

        let observed = Utc.with_ymd_and_hms(2026, 8, 9, 12, 0, 0).unwrap();
        let cancelled = match decide_cancel(
            &paused,
            0,
            observed,
            "user_requested".to_owned(),
            PrincipalSnapshot::build(
                id("ten_0198f1c5-0787-75e1-a9e8-d95ca0f37002"),
                id("prn_0198f1c5-0787-75e1-a9e8-d95ca0f37003"),
                insight_platform_contracts::PrincipalKind::AgentRunner,
                insight_platform_contracts::PermissionSet::new(vec![
                    insight_platform_contracts::Permission::RuntimeControl,
                ])
                .unwrap(),
                1,
                1,
                1,
            )
            .unwrap(),
        )
        .unwrap()
        {
            ControlDecision::Updated(value) => value,
            ControlDecision::Unchanged(_) => panic!("cancel must set intent"),
        };
        assert_eq!(cancelled.cancel_generation, 1);

        let cancel_convergence = decide_orchestration_convergence(
            OrchestrationConvergenceFacts {
                run_state: RunState::Cancelling,
                run_version: 2,
                run_deadline: observed + chrono::Duration::minutes(1),
                control: cancelled.clone(),
                node_state: NodeExecutionState::Running,
                job_state: JobState::Running,
                job_attempt_count: 1,
                job_attempt_limit: 3,
                job_deadline: observed + chrono::Duration::minutes(1),
                lease_expires_at: Some(observed + chrono::Duration::seconds(30)),
            },
            observed,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            cancel_convergence.reason,
            OrchestrationConvergenceReason::CancelRequested
        );
        assert_eq!(cancel_convergence.run_state, RunState::Cancelled);

        let timeout_convergence = decide_orchestration_convergence(
            OrchestrationConvergenceFacts {
                run_state: RunState::Running,
                run_version: 3,
                run_deadline: observed,
                control: control(),
                node_state: NodeExecutionState::Running,
                job_state: JobState::Running,
                job_attempt_count: 1,
                job_attempt_limit: 3,
                job_deadline: observed,
                lease_expires_at: Some(observed + chrono::Duration::seconds(30)),
            },
            observed,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            timeout_convergence.reason,
            OrchestrationConvergenceReason::DeadlineExceeded
        );
        assert_eq!(timeout_convergence.run_state, RunState::TimedOut);
        assert_eq!(timeout_convergence.control.timeout_generation, 1);

        assert_eq!(
            decide_timeout(
                &cancelled,
                0,
                observed,
                observed + chrono::Duration::seconds(1),
                "cancelling".to_owned(),
                2,
            ),
            Err(OrchestratorError::InvalidTimeoutObservation)
        );
    }

    #[test]
    fn child_ancestry_rejects_cycles_depth_and_unbounded_delegation() {
        let now = Utc.with_ymd_and_hms(2026, 8, 9, 12, 0, 0).unwrap();
        let parent_run_id = id("run_0198f1c5-0787-75e1-a9e8-d95ca0f37101");
        let parent_deployment_id = id("adep_0198f1c5-0787-75e1-a9e8-d95ca0f37102");
        let child_deployment =
            ExactDeploymentRef::new(id("adep_0198f1c5-0787-75e1-a9e8-d95ca0f37103"), digest('1'))
                .unwrap();
        let command = PrepareChildRun {
            parent_run_id: parent_run_id.clone(),
            parent_node_execution_id: id("nod_0198f1c5-0787-75e1-a9e8-d95ca0f37104"),
            child_link_id: id("crun_0198f1c5-0787-75e1-a9e8-d95ca0f37105"),
            child_run_id: id("run_0198f1c5-0787-75e1-a9e8-d95ca0f37106"),
            child_agent_deployment: child_deployment.clone(),
            parent_ancestry: RunAncestrySnapshot::root(parent_run_id.clone(), parent_deployment_id),
            parent_deadline: now + chrono::Duration::minutes(5),
            parent_delegated_budget: None,
            parent_delegated_descendant_count: 0,
            parent_descendant_count: 2,
            maximum_depth: 4,
            maximum_descendants: 8,
            budget: ChildBudget {
                deadline: now + chrono::Duration::minutes(4),
                maximum_model_tokens: 1_000,
                maximum_capability_calls: 10,
                maximum_artifact_bytes: 1_048_576,
                maximum_descendant_runs: 5,
            },
        };
        let ancestry = prepare_child_run(command.clone()).unwrap();
        assert_eq!(ancestry.depth, 1);
        assert_eq!(
            ancestry.ancestry_agent_deployment_ids.last(),
            Some(&child_deployment.deployment_id)
        );

        let mut cycle = command.clone();
        cycle.child_agent_deployment = ExactDeploymentRef::new(
            command.parent_ancestry.ancestry_agent_deployment_ids[0].clone(),
            digest('2'),
        )
        .unwrap();
        assert_eq!(prepare_child_run(cycle), Err(OrchestratorError::ChildCycle));

        let mut depth = command.clone();
        depth.maximum_depth = 0;
        assert_eq!(
            prepare_child_run(depth),
            Err(OrchestratorError::InvalidChildRun)
        );

        let mut nested = command.clone();
        nested.parent_delegated_budget = Some(ChildBudget {
            deadline: now + chrono::Duration::minutes(4),
            maximum_model_tokens: 1_000,
            maximum_capability_calls: 10,
            maximum_artifact_bytes: 1_048_576,
            maximum_descendant_runs: 8,
        });
        nested.parent_delegated_descendant_count = 2;
        assert!(prepare_child_run(nested).is_ok());

        let mut over_parent_budget = command.clone();
        over_parent_budget.parent_delegated_budget = Some(ChildBudget {
            deadline: now + chrono::Duration::minutes(4),
            maximum_model_tokens: 999,
            maximum_capability_calls: 10,
            maximum_artifact_bytes: 1_048_576,
            maximum_descendant_runs: 8,
        });
        assert_eq!(
            prepare_child_run(over_parent_budget),
            Err(OrchestratorError::InvalidChildBudget)
        );

        let mut descendants = command;
        descendants.budget.maximum_descendant_runs = 6;
        assert_eq!(
            prepare_child_run(descendants),
            Err(OrchestratorError::InvalidChildRun)
        );
    }

    #[test]
    fn child_link_cancel_and_terminal_are_generation_first_winner() {
        let now = Utc.with_ymd_and_hms(2026, 8, 9, 12, 0, 0).unwrap();
        let current = ChildRunLinkProjection {
            tenant_id: id("ten_0198f1c5-0787-75e1-a9e8-d95ca0f37201"),
            child_link_id: id("crun_0198f1c5-0787-75e1-a9e8-d95ca0f37202"),
            parent_run_id: id("run_0198f1c5-0787-75e1-a9e8-d95ca0f37203"),
            parent_node_execution_id: id("nod_0198f1c5-0787-75e1-a9e8-d95ca0f37204"),
            child_run_id: id("run_0198f1c5-0787-75e1-a9e8-d95ca0f37205"),
            state: ChildLinkState::Running,
            generation: 1,
            version: 1,
            payload: ChildRunLinkPayload {
                parent_attempt_ordinal: 1,
                child_agent_deployment: ExactDeploymentRef::new(
                    id("adep_0198f1c5-0787-75e1-a9e8-d95ca0f37206"),
                    digest('3'),
                )
                .unwrap(),
                input_digest: digest('4'),
                cancellation_policy: ChildCancellationPolicy::CascadeAndWait,
                budget: ChildBudget {
                    deadline: now + chrono::Duration::minutes(1),
                    maximum_model_tokens: 100,
                    maximum_capability_calls: 2,
                    maximum_artifact_bytes: 1024,
                    maximum_descendant_runs: 0,
                },
            },
            deadline: now + chrono::Duration::minutes(1),
            terminal_at: None,
        };
        let cancelling = decide_child_link_cancel(&current, 1, 1).unwrap();
        assert_eq!(cancelling.state, ChildLinkState::Cancelling);
        assert_eq!(cancelling.version, 2);
        let succeeded =
            decide_child_link_terminal(&cancelling, 1, 2, RunState::Succeeded, now).unwrap();
        assert_eq!(succeeded.state, ChildLinkState::Succeeded);
        assert_eq!(succeeded.version, 3);
        assert_eq!(
            decide_child_link_terminal(&succeeded, 1, 2, RunState::Cancelled, now),
            Err(OrchestratorError::ChildFirstWinnerLost)
        );
    }
}
