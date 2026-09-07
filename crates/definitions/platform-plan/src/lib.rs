//! The sole owner of versioned Plan definitions, typed expressions and pure validation.
//! No execution state, database, process, provider or current authorization belongs here.

mod expression;
pub use expression::{
    DataPortKey, ExactDataPortRef, ExpressionError, ExpressionFieldName, ExpressionLimits,
    TypedExpressionProgram, TypedInstruction, MAX_EXPRESSION_FIELD_BYTES,
    MAX_EXPRESSION_INPUT_PORTS, MAX_EXPRESSION_INSTRUCTIONS, MAX_EXPRESSION_STACK_DEPTH,
};

pub mod execution;

#[cfg(all(test, feature = "runtime-validation"))]
mod runtime_validation_tests;

use insight_platform_contracts::{
    canonical_digest, canonical_json, ClosedJsonSchema, ClosedValueSchema, HardLimitProfile,
    InteractionKind, LimitUnit, PlanNodeKind, Sha256Digest,
};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{collections::BTreeMap, error::Error, fmt};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PlanNodeKey(String);

impl PlanNodeKey {
    pub fn new(value: String) -> Result<Self, PlanError> {
        let bytes = value.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 128
            || !bytes[0].is_ascii_alphanumeric()
            || !bytes.iter().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-' | b'.' | b':')
            })
        {
            return Err(PlanError::InvalidPlanNodeKey);
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        eligibility_rule: Option<insight_platform_contracts::TaskEligibilityRule>,
        interaction_kind: InteractionKind,
        eligible_principal_rule_digest: Sha256Digest,
        safe_prompt_key: String,
    },
    HumanWork {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        eligibility_rule: Option<insight_platform_contracts::TaskEligibilityRule>,
        eligible_principal_rule_digest: Sha256Digest,
        safe_prompt_key: String,
    },
}

impl HumanTaskDefinition {
    pub fn eligibility_rule(&self) -> Option<&insight_platform_contracts::TaskEligibilityRule> {
        match self {
            Self::Interaction {
                eligibility_rule, ..
            }
            | Self::HumanWork {
                eligibility_rule, ..
            } => eligibility_rule.as_ref(),
        }
    }
    pub fn eligibility_rule_digest(&self) -> &Sha256Digest {
        match self {
            Self::Interaction {
                eligible_principal_rule_digest,
                ..
            }
            | Self::HumanWork {
                eligible_principal_rule_digest,
                ..
            } => eligible_principal_rule_digest,
        }
    }
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
    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, PlanError> {
        profile.validate().map_err(|_| PlanError::InvalidPlan)?;
        let registry = &profile.registry_plan;
        if registry.plan_nodes.unit != LimitUnit::Count
            || registry.plan_edges.unit != LimitUnit::Count
            || registry.branch_legs.unit != LimitUnit::Count
            || registry.map_items.unit != LimitUnit::Items
            || registry.loop_iterations.unit != LimitUnit::Count
        {
            return Err(PlanError::InvalidPlan);
        }
        Ok(Self {
            maximum_nodes: usize::try_from(registry.plan_nodes.q1_default)
                .map_err(|_| PlanError::InvalidPlan)?,
            maximum_edges: usize::try_from(registry.plan_edges.q1_default)
                .map_err(|_| PlanError::InvalidPlan)?,
            maximum_fan_out: usize::try_from(registry.branch_legs.q1_default)
                .map_err(|_| PlanError::InvalidPlan)?,
            maximum_map_items: u32::try_from(registry.map_items.q1_default)
                .map_err(|_| PlanError::InvalidPlan)?,
            maximum_loop_iterations: u32::try_from(registry.loop_iterations.q1_default)
                .map_err(|_| PlanError::InvalidPlan)?,
            maximum_error_handlers: usize::try_from(registry.branch_legs.q1_default)
                .map_err(|_| PlanError::InvalidPlan)?,
            expression: ExpressionLimits::from_profile(profile)
                .map_err(|_| PlanError::InvalidPlan)?,
        })
    }
}

pub const MAX_PLAN_SCHEMA_DOCUMENTS: usize = 256;
pub const MAX_PLAN_SCHEMA_DOCUMENT_BYTES: usize = 4_194_304;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePlan {
    pub plan_version: u32,
    pub interface_contract_digest: Sha256Digest,
    pub entry_node_id: PlanNodeKey,
    pub dependency_slots: BTreeMap<String, RuntimeDependencySlot>,
    pub schema_documents: BTreeMap<Sha256Digest, ClosedValueSchema>,
    pub nodes: BTreeMap<PlanNodeKey, RuntimeNode>,
}

impl RuntimePlan {
    pub fn validate(&self, limits: PlanLimits) -> Result<(), PlanError> {
        if self.plan_version != 6
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
            return Err(PlanError::InvalidPlan);
        }
        self.validate_schema_documents()?;
        let mut edge_count = 0_usize;
        for (node_key, node) in &self.nodes {
            validate_node(node_key, node, limits)?;
            let references = node.references();
            edge_count = edge_count
                .checked_add(references.len())
                .ok_or(PlanError::InvalidPlan)?;
            if edge_count > limits.maximum_edges {
                return Err(PlanError::InvalidPlan);
            }
            if references
                .into_iter()
                .any(|reference| !self.nodes.contains_key(reference))
            {
                return Err(PlanError::UnknownPlanNodeReference);
            }
        }
        self.validate_loop_carried_regions()?;
        self.validate_external_leaf_contracts()?;
        self.validate_terminal_ports()?;
        Ok(())
    }

    /// Checks materialized execution values against the exact frozen schema.
    #[cfg(feature = "runtime-validation")]
    pub fn validate_value_instance(
        &self,
        digest: &Sha256Digest,
        value: &serde_json::Value,
    ) -> Result<(), PlanError> {
        self.schema_documents
            .get(digest)
            .ok_or(PlanError::InvalidPlan)?
            .validate_instance(value)
            .map_err(|_| PlanError::InvalidPlan)
    }

    /// Execution admission checks every frozen literal, including values consumed by an
    /// intermediate instruction whose final output has a different schema. Portable authoring
    /// validation intentionally remains a shape preflight on every target, including WASM.
    #[cfg(feature = "runtime-validation")]
    pub fn validate_node_literal_instances(&self, node_key: &PlanNodeKey) -> Result<(), PlanError> {
        for expression in node_expressions(self.node(node_key)?) {
            for instruction in &expression.instructions {
                if let TypedInstruction::Literal { value } = instruction {
                    self.validate_value_instance(&value.schema_digest, &value.value)?;
                }
            }
        }
        Ok(())
    }

    fn validate_schema_documents(&self) -> Result<(), PlanError> {
        if self.schema_documents.is_empty()
            || self.schema_documents.len() > MAX_PLAN_SCHEMA_DOCUMENTS
        {
            return Err(PlanError::InvalidPlan);
        }
        let mut total = 0usize;
        for (digest, schema) in &self.schema_documents {
            schema.validate().map_err(|_| PlanError::InvalidPlan)?;
            if digest != &schema.canonical_digest {
                return Err(PlanError::InvalidPlan);
            }
            total = total
                .checked_add(
                    canonical_json(
                        &serde_json::to_value(schema).map_err(|_| PlanError::InvalidPlan)?,
                    )
                    .map_err(|_| PlanError::InvalidPlan)?
                    .len(),
                )
                .ok_or(PlanError::InvalidPlan)?;
            if total > MAX_PLAN_SCHEMA_DOCUMENT_BYTES {
                return Err(PlanError::InvalidPlan);
            }
        }
        let mut references = std::collections::BTreeSet::new();
        for node in self.nodes.values() {
            collect_node_schemas(node, &mut references);
            for expression in node_expressions(node) {
                for instruction in &expression.instructions {
                    if let TypedInstruction::Literal { value } = instruction {
                        self.schema_documents
                            .get(&value.schema_digest)
                            .ok_or(PlanError::InvalidPlan)?
                            .validate_literal_shape(&value.value)
                            .map_err(|_| PlanError::InvalidPlan)?;
                    }
                }
            }
            if let RuntimeNode::HumanTask {
                definition,
                response,
                ..
            } = node
            {
                ClosedJsonSchema::try_from(
                    self.schema_documents
                        .get(response.schema_digest())
                        .cloned()
                        .ok_or(PlanError::InvalidPlan)?,
                )
                .map_err(|_| PlanError::InvalidPlan)?;
                let rule = definition
                    .eligibility_rule()
                    .ok_or(PlanError::InvalidPlan)?;
                if rule.canonical_digest().as_ref() != Ok(definition.eligibility_rule_digest()) {
                    return Err(PlanError::InvalidPlan);
                }
            }
        }
        if references
            .iter()
            .any(|digest| !self.schema_documents.contains_key(*digest))
        {
            return Err(PlanError::InvalidPlan);
        }
        Ok(())
    }

    fn validate_external_leaf_contracts(&self) -> Result<(), PlanError> {
        for (node_key, node) in &self.nodes {
            for (slot_id, expected_kind) in node_dependency_slots(node) {
                if self.dependency_slots.get(slot_id).map(|slot| slot.kind) != Some(expected_kind) {
                    return Err(PlanError::InvalidPlan);
                }
            }
            for port in external_leaf_inputs(node) {
                let Some(producer_key) = port.producer_node_id() else {
                    continue;
                };
                let producer = self.nodes.get(producer_key).ok_or(PlanError::InvalidPlan)?;
                if producer_key == node_key
                    || !node_declares_output(producer, port)
                    || !self.node_reaches(producer_key, node_key)?
                {
                    return Err(PlanError::InvalidPlan);
                }
            }
        }
        Ok(())
    }

    fn validate_terminal_ports(&self) -> Result<(), PlanError> {
        for (terminal_key, node) in &self.nodes {
            let port = match node {
                RuntimeNode::Return { value } => value,
                RuntimeNode::Raise { failure } => failure,
                _ => continue,
            };
            let Some(producer_key) = port.producer_node_id() else {
                continue;
            };
            let producer = self.nodes.get(producer_key).ok_or(PlanError::InvalidPlan)?;
            if producer_key == terminal_key
                || !node_declares_output(producer, port)
                || !self.node_reaches(producer_key, terminal_key)?
            {
                return Err(PlanError::InvalidPlan);
            }
        }
        Ok(())
    }

    fn node_reaches(&self, source: &PlanNodeKey, target: &PlanNodeKey) -> Result<bool, PlanError> {
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
                .ok_or(PlanError::UnknownPlanNodeReference)?;
            pending.extend(node.references().into_iter().cloned());
        }
        Ok(false)
    }

    fn validate_loop_carried_regions(&self) -> Result<(), PlanError> {
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
                    .ok_or(PlanError::UnknownPlanNodeReference)?;
                pending.extend(candidate_node.references().into_iter().cloned());
            }
            if carried_ports.iter().any(|port| {
                port.body_output_port
                    .producer_node_id()
                    .is_none_or(|producer| !reachable.contains(producer))
            }) {
                return Err(PlanError::InvalidPlan);
            }
        }
        Ok(())
    }

    pub fn canonical_digest(&self, limits: PlanLimits) -> Result<Sha256Digest, PlanError> {
        self.validate(limits)?;
        let value = serde_json::to_value(self).map_err(|_| PlanError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| PlanError::Canonicalization)?
            .parse()
            .map_err(|_| PlanError::Canonicalization)
    }

    pub fn validate_terminal_schema_digests(
        &self,
        output_schema_digest: &Sha256Digest,
        error_schema_digest: &Sha256Digest,
    ) -> Result<(), PlanError> {
        if self.nodes.values().any(|node| match node {
            RuntimeNode::Return { value } => value.schema_digest() != output_schema_digest,
            RuntimeNode::Raise { failure } => failure.schema_digest() != error_schema_digest,
            _ => false,
        }) {
            return Err(PlanError::InvalidPlan);
        }
        Ok(())
    }

    pub fn node(&self, key: &PlanNodeKey) -> Result<&RuntimeNode, PlanError> {
        self.nodes
            .get(key)
            .ok_or(PlanError::UnknownPlanNodeReference)
    }
}

fn node_expressions(node: &RuntimeNode) -> Vec<&TypedExpressionProgram> {
    match node {
        RuntimeNode::Compute { assignments, .. } => assignments
            .iter()
            .map(|assignment| &assignment.expression)
            .collect(),
        RuntimeNode::Branch { ordered_arms, .. } => {
            ordered_arms.iter().map(|arm| &arm.when).collect()
        }
        RuntimeNode::Map { items, .. } => vec![items],
        RuntimeNode::Loop { condition, .. } => vec![condition],
        _ => vec![],
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

fn collect_expression_schemas<'a>(
    expression: &'a TypedExpressionProgram,
    out: &mut std::collections::BTreeSet<&'a Sha256Digest>,
) {
    out.insert(&expression.output_schema_digest);
    for port in &expression.input_ports {
        out.insert(port.schema_digest());
    }
    for instruction in &expression.instructions {
        match instruction {
            TypedInstruction::LoadPort { port } => {
                out.insert(port.schema_digest());
            }
            TypedInstruction::Literal { value } => {
                out.insert(&value.schema_digest);
            }
            _ => {}
        }
    }
}

fn collect_node_schemas<'a>(
    node: &'a RuntimeNode,
    out: &mut std::collections::BTreeSet<&'a Sha256Digest>,
) {
    match node {
        RuntimeNode::Compute { assignments, .. } => {
            for assignment in assignments {
                out.insert(assignment.output_port.schema_digest());
                collect_expression_schemas(&assignment.expression, out);
            }
        }
        RuntimeNode::Branch { ordered_arms, .. } => {
            for arm in ordered_arms {
                collect_expression_schemas(&arm.when, out);
            }
        }
        RuntimeNode::Map {
            items, item_port, ..
        } => {
            collect_expression_schemas(items, out);
            out.insert(item_port.schema_digest());
        }
        RuntimeNode::Loop {
            condition,
            carried_ports,
            ..
        } => {
            collect_expression_schemas(condition, out);
            for port in carried_ports {
                out.insert(port.body_output_port.schema_digest());
                out.insert(port.next_iteration_port.schema_digest());
            }
        }
        RuntimeNode::ModelLoop {
            input,
            model_route,
            output,
            ..
        } => {
            out.insert(input.schema_digest());
            out.insert(output.schema_digest());
            if let Some(route) = model_route {
                out.insert(route.schema_digest());
            }
        }
        RuntimeNode::CapabilityCall {
            input,
            candidate_route,
            output,
            ..
        }
        | RuntimeNode::ChildAgentCall {
            input,
            candidate_route,
            output,
            ..
        } => {
            out.insert(input.schema_digest());
            out.insert(output.schema_digest());
            if let Some(route) = candidate_route {
                out.insert(route.schema_digest());
            }
        }
        RuntimeNode::ContextQuery {
            request, result, ..
        } => {
            out.insert(request.schema_digest());
            out.insert(result.schema_digest());
        }
        RuntimeNode::HumanTask { response, .. } => {
            out.insert(response.schema_digest());
        }
        RuntimeNode::SignalWait { payload, .. } => {
            if let Some(port) = payload {
                out.insert(port.schema_digest());
            }
        }
        RuntimeNode::Return { value } => {
            out.insert(value.schema_digest());
        }
        RuntimeNode::Raise { failure } => {
            out.insert(failure.schema_digest());
        }
        RuntimeNode::Start { .. }
        | RuntimeNode::Fork { .. }
        | RuntimeNode::Join { .. }
        | RuntimeNode::ErrorBoundary { .. }
        | RuntimeNode::TimerWait { .. } => {}
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
) -> Result<(), PlanError> {
    if output.producer_node_id() != Some(node_key) {
        Err(PlanError::InvalidPlan)
    } else {
        Ok(())
    }
}

fn validate_human_task_definition(definition: &HumanTaskDefinition) -> Result<(), PlanError> {
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
        Err(PlanError::InvalidPlan)
    }
}

pub fn validate_node(
    node_key: &PlanNodeKey,
    node: &RuntimeNode,
    limits: PlanLimits,
) -> Result<(), PlanError> {
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
                Err(PlanError::InvalidPlan)
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
            Err(PlanError::InvalidPlan)
        }
        RuntimeNode::Join {
            policy,
            quorum,
            remainder,
            ..
        } => match (policy, quorum, remainder) {
            (JoinPolicy::Quorum, Some(value), Some(_)) if *value > 0 => Ok(()),
            (JoinPolicy::AllSuccess | JoinPolicy::AllSettled, None, None) => Ok(()),
            _ => Err(PlanError::InvalidPlan),
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
            Err(PlanError::InvalidPlan)
        }
        RuntimeNode::Map {
            maximum_items,
            failure_policy: MapFailurePolicy::BoundedErrorCount { maximum_failures },
            ..
        } if *maximum_failures == 0 || maximum_failures > maximum_items => {
            Err(PlanError::InvalidPlan)
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
            Err(PlanError::InvalidPlan)
        }
        RuntimeNode::ErrorBoundary { handlers, .. }
            if handlers.len() > limits.maximum_error_handlers
                || handlers.keys().any(|code| !is_stable_code(code)) =>
        {
            Err(PlanError::InvalidPlan)
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
            Err(PlanError::InvalidPlan)
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
            Err(PlanError::InvalidPlan)
        }
        RuntimeNode::ContextQuery {
            result,
            maximum_items,
            ..
        } if *maximum_items == 0
            || *maximum_items > limits.maximum_map_items
            || validate_leaf_output(node_key, result).is_err() =>
        {
            Err(PlanError::InvalidPlan)
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
            Err(PlanError::InvalidPlan)
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
            Err(PlanError::InvalidPlan)
        }
        RuntimeNode::TimerWait {
            delay_milliseconds, ..
        } if *delay_milliseconds == 0 => Err(PlanError::InvalidPlan),
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
            Err(PlanError::InvalidPlan)
        }
        _ => Ok(()),
    }
}

fn validate_assignments(
    node_key: &PlanNodeKey,
    assignments: &[PortAssignment],
    limits: ExpressionLimits,
) -> Result<(), PlanError> {
    let outputs = assignments
        .iter()
        .map(|assignment| &assignment.output_port)
        .collect::<std::collections::BTreeSet<_>>();
    if outputs.len() != assignments.len()
        || assignments
            .iter()
            .any(|assignment| assignment.output_port.producer_node_id() != Some(node_key))
    {
        return Err(PlanError::InvalidPlan);
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
            return Err(PlanError::InvalidPlan);
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
pub enum ChildCancellationPolicy {
    CascadeAndWait,
    CascadeWithDeadline,
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
pub enum PlanError {
    InvalidPlanNodeKey,
    InvalidPlan,
    UnknownPlanNodeReference,
    Canonicalization,
}

impl fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidPlanNodeKey => "plan node key is invalid",
            Self::InvalidPlan => "runtime plan is invalid or outside its hard limits",
            Self::UnknownPlanNodeReference => "runtime plan references an unknown node",
            Self::Canonicalization => "plan cannot be canonicalized",
        })
    }
}

impl Error for PlanError {}
