use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map as JsonMap, Value};

use crate::engine::{
    plan::{
        CollectDescriptor, CollectSource, ControlPortId, DataPortId, ErrorBoundaryDescriptor,
        LeafTaskKind, LinkedPlan, LoopDescriptor, MapDescriptor, Node, NodeKind, PlanIndex,
        PlanInputContract, PlanJoinMode, PolicyKind, ScopeId, ScopeKind as PlanScopeKind,
    },
    ActivationId, ContentHash, ControlTokenId, DynamicKey, ForkGroupId, MapItemIdentity,
    ModelError, NodeId, ScopeInstanceId, TerminationReason, TransitionKey, WorkerEffectPolicy,
    WorkerFailureClass,
};

use super::{
    binding::{require_type, DataResolver},
    BoundTaskInput, BranchSelectionAdmissionFact, DeterministicIds, ErrorBoundaryExit,
    ErrorBoundaryFact, ErrorBoundaryPhase, ForkAdmissionFact, ForkGroupFact, ForkLegAdmissionFact,
    ForkLegFact, ForkLegKey, LogicalOccurrence, LoopInstanceFact, LoopIterationFact,
    LoopIterationKey, MapInstanceFact, MapItemFact, MapItemKey, MapItemSeed, NativeOutput,
    PlannedSchedulerAction, ReuseAdmissionCandidate, ReuseAdmissionContract, RunTerminalFact,
    RuntimeValue, SafeError, SchedulerAction, SchedulerCancellationReason, SchedulerCheckpointId,
    SchedulerDecision, SchedulerError, SchedulerFacts, SchedulerIntent, SchedulerPrecondition,
    SchedulerQuiescence, SchedulerTaskKind, StructuralOutcomeFact, SubflowInvocationFact,
    SubflowOutcomeFact, SuccessorAdmissionFact, TaskAdmissionClass, TaskFailureFact,
    TaskOutcomeFact, TaskOutputContract, WaitRegistrationFact, WaitSubjectFact,
    SCHEDULER_DYNAMIC_KEY_DUPLICATE, SCHEDULER_EXPRESSION_INVALID, SCHEDULER_FACT_INCONSISTENT,
    SCHEDULER_FACT_MISSING, SCHEDULER_GRAPH_INVALID, SCHEDULER_LOOP_BUDGET_EXCEEDED,
    SCHEDULER_VALUE_TYPE_MISMATCH,
};

const SCHEDULER_TRANSITION_DOMAIN: &str = "scheduler.action.v2";

/// The exact child-input and parent-output projection of one Subflow call.
/// Both the planner and the durable repository derive this value from the
/// frozen parent Plan plus committed scheduler facts; the action wire is not
/// an authority for any of these fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DerivedSubflowAdmission {
    pub(crate) run_input: RuntimeValue,
    pub(crate) outputs: Vec<TaskOutputContract>,
}

pub(crate) fn derive_subflow_admission(
    index: &PlanIndex<'_>,
    facts: &SchedulerFacts,
    node: &Node,
    descriptor: &crate::engine::plan::SubflowCallDescriptor,
    occurrence: &LogicalOccurrence,
    input_contract: &PlanInputContract,
) -> Result<DerivedSubflowAdmission, SchedulerError> {
    let resolver = DataResolver::for_occurrence(index, facts, occurrence);
    let mut raw_input = JsonMap::new();
    for (name, input) in &descriptor.inputs {
        let port = index
            .data_port(input)
            .ok_or_else(|| graph("Subflow input disappeared"))?;
        if index.source_for_input(input).is_none() && !port.required() {
            continue;
        }
        let Some(value) = resolver.resolve_input_if_present(input, node)? else {
            continue;
        };
        raw_input.insert(name.as_str().to_owned(), value.value().clone());
    }
    let normalized_input = input_contract
        .normalize(Value::Object(raw_input))
        .map_err(|_| {
            SchedulerError::new(
                SCHEDULER_VALUE_TYPE_MISMATCH,
                "Subflow input does not satisfy the linked child input contract",
            )
        })?;
    let run_input = RuntimeValue::new(normalized_input)?;
    let outputs = index
        .data_outputs(node.id())
        .iter()
        .map(|output_id| {
            let port = index
                .data_port(output_id)
                .ok_or_else(|| graph("Subflow output disappeared from PlanIndex"))?;
            Ok(TaskOutputContract::new(
                output_id.clone(),
                port.name().clone(),
                port.value_type().clone(),
                port.required(),
            ))
        })
        .collect::<Result<Vec<_>, SchedulerError>>()?;
    Ok(DerivedSubflowAdmission { run_input, outputs })
}

/// Reconstruct the complete scheduler-owned invocation identity from the
/// pinned parent Plan and one durable occurrence. This is shared by planning
/// and repository admission so no ID carried on the action wire is trusted.
pub(crate) fn derive_subflow_invocation(
    index: &PlanIndex<'_>,
    facts: &SchedulerFacts,
    node: &Node,
    descriptor: &crate::engine::plan::SubflowCallDescriptor,
    occurrence: &LogicalOccurrence,
) -> Result<SubflowInvocationFact, SchedulerError> {
    let parent_scope_instance_id =
        scope_instance_for_occurrence(index, facts.run_id(), node, occurrence)?;
    let ids = DeterministicIds::new(facts.run_id(), index.semantic_hash());
    let parent_activation_id = ids
        .activation(node.id(), &parent_scope_instance_id, occurrence)
        .map_err(|error| id_error(error, "Subflow parent activation"))?;
    let child_run_id = ids
        .child_run(node.id(), &parent_scope_instance_id, occurrence)
        .map_err(|error| id_error(error, "child run"))?;
    let invocation_scope_occurrence =
        occurrence.child(format!("scope:{}", descriptor.invocation_scope_id.as_str()))?;
    let invocation_scope_instance_id = ids
        .scope_instance(
            &descriptor.invocation_scope_id,
            &invocation_scope_occurrence,
        )
        .map_err(|error| id_error(error, "Subflow invocation scope"))?;
    SubflowInvocationFact::new(
        child_run_id,
        parent_activation_id,
        node.id().clone(),
        occurrence.clone(),
        invocation_scope_instance_id,
        parent_scope_instance_id,
        descriptor.invocation_scope_id.clone(),
    )
}

/// Derive the scheduler-owned scope identity for an arbitrary immutable Plan
/// occurrence. Recovery uses the same function to map source dynamic
/// Map/Loop/Subflow occurrences into a distinct target Run; keeping it here
/// prevents a second, subtly different identity algorithm.
pub(crate) fn scope_instance_for_occurrence(
    index: &PlanIndex<'_>,
    run_id: &crate::engine::RunId,
    node: &Node,
    occurrence: &LogicalOccurrence,
) -> Result<ScopeInstanceId, SchedulerError> {
    let mut scope_id = node.scope_id();
    let runtime_scope_id = loop {
        let scope = index.scope(scope_id).ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "node scope or one of its ancestors is absent from PlanIndex",
            )
        })?;
        match scope.kind() {
            PlanScopeKind::Root
            | PlanScopeKind::ForkLeg { .. }
            | PlanScopeKind::MapBody { .. }
            | PlanScopeKind::LoopBody { .. }
            | PlanScopeKind::Subflow { .. } => break scope.id().clone(),
            PlanScopeKind::Lexical
            | PlanScopeKind::BranchArm { .. }
            | PlanScopeKind::ErrorProtected { .. }
            | PlanScopeKind::ErrorHandler { .. }
            | PlanScopeKind::ErrorFinalizer { .. } => {
                scope_id = scope.parent().ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_GRAPH_INVALID,
                        "lexical scope chain does not reach a runtime-owned scope",
                    )
                })?;
            }
        }
    };
    scope_instance_for_runtime_scope(index, run_id, &runtime_scope_id, occurrence)
}

/// Derive one runtime-owned scope instance directly from its static Plan
/// scope. Subflow invocation scopes contain no parent-Plan nodes, so recovery
/// and targeted validation use this form while ordinary node admission uses
/// [`scope_instance_for_occurrence`].
pub(crate) fn scope_instance_for_runtime_scope(
    index: &PlanIndex<'_>,
    run_id: &crate::engine::RunId,
    scope_id: &ScopeId,
    occurrence: &LogicalOccurrence,
) -> Result<ScopeInstanceId, SchedulerError> {
    let scope = index.scope(scope_id).ok_or_else(|| {
        SchedulerError::new(
            SCHEDULER_GRAPH_INVALID,
            "runtime-owned scope is absent from PlanIndex",
        )
    })?;
    if matches!(scope.kind(), PlanScopeKind::Root) {
        return Ok(ScopeInstanceId::root());
    }
    let mut scope_occurrence = occurrence.clone();
    let owner_marker = match scope.kind() {
        PlanScopeKind::ForkLeg { leg_id, .. } => Some(format!("fork_leg:{leg_id}")),
        PlanScopeKind::MapBody { .. } => occurrence
            .segments()
            .iter()
            .rfind(|segment| segment.starts_with("map_item:"))
            .cloned(),
        PlanScopeKind::LoopBody { .. } => occurrence
            .segments()
            .iter()
            .rfind(|segment| {
                segment.starts_with("loop_iteration:") || segment.starts_with("agent_loop_turn:")
            })
            .cloned(),
        PlanScopeKind::Subflow { .. } => None,
        PlanScopeKind::Root
        | PlanScopeKind::Lexical
        | PlanScopeKind::BranchArm { .. }
        | PlanScopeKind::ErrorProtected { .. }
        | PlanScopeKind::ErrorHandler { .. }
        | PlanScopeKind::ErrorFinalizer { .. } => {
            return Err(SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "scope identity requested for a non-runtime-owned Plan scope",
            ));
        }
    };
    if let Some(owner_marker) = owner_marker {
        let owner_index = occurrence
            .segments()
            .iter()
            .rposition(|segment| segment == &owner_marker)
            .ok_or_else(|| {
                SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    "runtime-owned scope has no matching dynamic occurrence segment",
                )
            })?;
        while scope_occurrence.segments().len() > owner_index + 1 {
            scope_occurrence = scope_occurrence
                .parent()
                .expect("a matched occurrence prefix is non-empty");
        }
    }
    let scope_occurrence = scope_occurrence.child(format!("scope:{}", scope.id().as_str()))?;
    DeterministicIds::new(run_id, index.semantic_hash())
        .scope_instance(scope.id(), &scope_occurrence)
        .map_err(|error| id_error(error, "scope instance"))
}

#[derive(Debug, Clone)]
struct InboundControl {
    token_id: ControlTokenId,
    input_port: ControlPortId,
}

enum OutputProgress {
    Action(SchedulerDecision),
    Next {
        node_id: NodeId,
        occurrence: LogicalOccurrence,
        inbound: InboundControl,
    },
}

#[derive(Debug, Clone)]
enum StructuralFrame {
    ForkLeg {
        fork_node_id: NodeId,
        key: ForkLegKey,
    },
    MapItem {
        map_node_id: NodeId,
        key: MapItemKey,
    },
    LoopIteration {
        loop_node_id: NodeId,
        key: LoopIterationKey,
    },
    ErrorBoundary {
        boundary_node_id: NodeId,
        boundary_activation_id: ActivationId,
        boundary_occurrence: LogicalOccurrence,
        phase: ErrorBoundaryPhase,
    },
}

#[derive(Debug, Clone)]
enum CollectContext {
    StaticFork(ForkGroupId),
    Map(ActivationId),
    Loop(ActivationId),
}

#[derive(Debug, Clone, Default)]
struct PathContext {
    frames: Vec<StructuralFrame>,
    collect: Option<CollectContext>,
}

/// Deterministic, side-effect-free planner over a linked immutable Plan and a
/// committed projection. Every returned action is inert; repository code owns
/// CAS, idempotency, event append, wait registration and child-run creation.
pub struct SchedulerPlanner<'linked, 'plan> {
    linked: &'linked LinkedPlan<'plan>,
}

impl<'linked, 'plan> SchedulerPlanner<'linked, 'plan> {
    pub fn new(linked: &'linked LinkedPlan<'plan>) -> Self {
        Self { linked }
    }

    pub fn plan(&self, facts: &SchedulerFacts) -> Result<SchedulerDecision, SchedulerError> {
        if let Some(terminal) = facts.terminal() {
            return Ok(SchedulerDecision::Quiescent(match terminal {
                RunTerminalFact::Succeeded(_) => SchedulerQuiescence::RunSucceeded,
                RunTerminalFact::Failed(_)
                | RunTerminalFact::FailedInternal(_)
                | RunTerminalFact::FailedPlanning(_) => SchedulerQuiescence::RunFailed,
                RunTerminalFact::Cancelled => SchedulerQuiescence::RunCancelled,
                RunTerminalFact::TimedOut | RunTerminalFact::Interrupted => {
                    SchedulerQuiescence::RunFailed
                }
            }));
        }

        let index = self.linked.index();
        let run_input_type = index
            .metadata()
            .input_contract()
            .run_type()
            .map_err(|_| graph("RunInput contract is invalid"))?;
        require_type(facts.run_input(), &run_input_type, "RunInput")?;
        let ids = DeterministicIds::new(facts.run_id(), index.semantic_hash());
        if facts.run_termination_reason().is_some() {
            // Every owned child must first receive the authoritative control
            // termination and be acknowledged. Authored finalizers start only
            // after pre-finalizer child Runs have reached a terminal state.
            // A child authored by the finalizer itself is part of cleanup and
            // must be allowed to run under the original termination intent.
            for (child_run_id, invocation) in facts.subflows() {
                if facts.settled_subflows().contains_key(child_run_id) {
                    continue;
                }
                if node_is_owned_by_error_finalizer(index, invocation.node_id())? {
                    continue;
                }
                if !facts.child_cancellation_requests().contains(child_run_id) {
                    let checkpoint = ids.checkpoint(
                        invocation.node_id(),
                        invocation.parent_scope_instance_id(),
                        invocation.occurrence(),
                        &format!("cancel_child_run:{}", child_run_id.as_str()),
                    );
                    return self.action(
                        facts,
                        checkpoint,
                        SchedulerAction::RequestChildRunCancellation {
                            child_run_id: child_run_id.clone(),
                        },
                    );
                }
                let Some(outcome) = facts.child_subflow_outcomes().get(child_run_id) else {
                    return Ok(SchedulerDecision::Quiescent(
                        SchedulerQuiescence::WaitingForChildRun {
                            child_run_id: child_run_id.clone(),
                            activation_id: invocation.parent_activation_id().clone(),
                        },
                    ));
                };
                let checkpoint = ids.checkpoint(
                    invocation.node_id(),
                    invocation.parent_scope_instance_id(),
                    invocation.occurrence(),
                    "settle_subflow",
                );
                return self.action(
                    facts,
                    checkpoint,
                    SchedulerAction::SettleSubflow {
                        invocation: invocation.clone(),
                        outcome: outcome.clone(),
                    },
                );
            }
        }
        self.plan_path(
            facts,
            &ids,
            index.entry_node().id().clone(),
            LogicalOccurrence::entry(),
            None,
            PathContext::default(),
        )
    }

    /// Converts a body-free planner error into one deterministic repository
    /// action. Repeating planning over the same projection derives the same
    /// checkpoint, transition key, and intent hash.
    pub fn fail_closed_action(
        &self,
        facts: &SchedulerFacts,
        error: &SchedulerError,
    ) -> Result<PlannedSchedulerAction, SchedulerError> {
        self.fail_closed_action_at(
            facts.run_id(),
            facts.projection_version(),
            super::SchedulerPlanningFailure::from_error(error),
        )
    }

    /// Repository fact decoding can itself detect corruption before a full
    /// SchedulerFacts value exists. This constructor preserves the same closed
    /// terminal identity from the minimal authoritative Run projection.
    pub fn fail_closed_action_at(
        &self,
        run_id: &crate::engine::RunId,
        projection_version: u64,
        failure: super::SchedulerPlanningFailure,
    ) -> Result<PlannedSchedulerAction, SchedulerError> {
        let digest = ContentHash::from_bytes(
            format!(
                "insight-agent/scheduler/planning-failure/v1/{}/{}/{}",
                run_id.as_str(),
                self.linked.index().semantic_hash().as_str(),
                failure.internal_code(),
            )
            .as_bytes(),
        );
        let checkpoint = SchedulerCheckpointId::parse(format!(
            "checkpoint_{}",
            digest
                .as_str()
                .strip_prefix("sha256:")
                .unwrap_or(digest.as_str())
        ))?;
        let intent = SchedulerIntent::new(
            run_id.clone(),
            checkpoint.clone(),
            SchedulerAction::FailRunPlanning { failure },
        );
        let intent_hash =
            crate::engine::IntentHash::from_serializable(&intent).map_err(|error| {
                SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    format!("planning failure intent could not be canonicalized: {error}"),
                )
            })?;
        let transition_key = TransitionKey::derive(
            SCHEDULER_TRANSITION_DOMAIN,
            &[
                run_id.as_str(),
                self.linked.index().semantic_hash().as_str(),
                checkpoint.as_str(),
            ],
        )
        .map_err(|error| id_error(error, "planning failure transition"))?;
        Ok(PlannedSchedulerAction::new(
            SchedulerPrecondition::new(projection_version),
            transition_key,
            intent_hash,
            intent,
        ))
    }

    #[allow(clippy::too_many_lines)]
    fn plan_path(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        mut node_id: NodeId,
        mut occurrence: LogicalOccurrence,
        mut inbound: Option<InboundControl>,
        context: PathContext,
    ) -> Result<SchedulerDecision, SchedulerError> {
        let index = self.linked.index();
        let mut visited = BTreeSet::new();

        loop {
            if !visited.insert((node_id.clone(), occurrence.clone())) {
                return Err(SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    "control traversal repeated one logical node occurrence outside a Loop contract",
                ));
            }
            let node = index.node(&node_id).ok_or_else(|| {
                SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    "control route targets a missing Plan node",
                )
            })?;

            if let Some(decision) = self.intercept_structural_arrival(
                facts,
                ids,
                index,
                node,
                &occurrence,
                inbound.as_ref(),
                &context,
            )? {
                return Ok(decision);
            }

            let scope_instance_id = self.scope_instance(ids, index, node, &occurrence)?;
            let activation_id = ids
                .activation(node.id(), &scope_instance_id, &occurrence)
                .map_err(|error| id_error(error, "activation"))?;
            if let Some(action) = self.ensure_activation(
                facts,
                ids,
                node,
                &scope_instance_id,
                &activation_id,
                &occurrence,
                inbound.as_ref(),
            )? {
                return Ok(action);
            }

            match node.kind() {
                NodeKind::Branch(branch) => {
                    if facts.branch_selection_at(node.id(), &occurrence).is_none() {
                        if let Some(reason) = termination_outside_finalizer(facts, &context) {
                            return self.propagate_termination(
                                facts,
                                ids,
                                index,
                                node,
                                &scope_instance_id,
                                &activation_id,
                                &occurrence,
                                context,
                                reason,
                            );
                        }
                    }
                    let resolver = DataResolver::for_occurrence(index, facts, &occurrence);
                    let mut selected = None;
                    for case in &branch.cases {
                        let matches = match &case.condition {
                            Some(condition) => {
                                let value = resolver.evaluate_expression(condition, node)?;
                                match value.value() {
                                    Value::Bool(value) => *value,
                                    Value::Null => {
                                        return Err(SchedulerError::new(
                                            SCHEDULER_VALUE_TYPE_MISMATCH,
                                            "Branch condition evaluated to null/unknown",
                                        ));
                                    }
                                    _ => {
                                        return Err(SchedulerError::new(
                                            SCHEDULER_VALUE_TYPE_MISMATCH,
                                            "Branch condition did not evaluate to Boolean",
                                        ));
                                    }
                                }
                            }
                            None => true,
                        };
                        if matches {
                            selected = Some(case);
                            break;
                        }
                    }
                    let selected = selected.ok_or_else(|| {
                        SchedulerError::new(
                            SCHEDULER_GRAPH_INVALID,
                            "Branch has no matching case or fallback",
                        )
                    })?;
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        &scope_instance_id,
                        &occurrence,
                        "commit_branch_selection",
                    );
                    let route = route_for_output(index, &selected.output_port)?;
                    let successor_occurrence =
                        occurrence.child(format!("edge:{}", route.edge_id))?;
                    let successor = index
                        .node(&route.node_id)
                        .ok_or_else(|| graph("Branch route target disappeared"))?;
                    let successor_scope =
                        self.scope_instance(ids, index, successor, &successor_occurrence)?;
                    let successor_activation = ids
                        .activation(successor.id(), &successor_scope, &successor_occurrence)
                        .map_err(|error| id_error(error, "branch successor activation"))?;
                    let token_id = ids
                        .control_token(
                            node.id(),
                            &scope_instance_id,
                            &occurrence,
                            &selected.output_port,
                        )
                        .map_err(|error| id_error(error, "branch control token"))?;
                    let selection = BranchSelectionAdmissionFact::new(
                        activation_id,
                        node.id().clone(),
                        scope_instance_id,
                        occurrence.clone(),
                        selected.case_id.clone(),
                        selected.output_port.clone(),
                        token_id.clone(),
                        SuccessorAdmissionFact::new(
                            successor_activation.clone(),
                            successor.id().clone(),
                            successor_scope,
                            successor_occurrence.clone(),
                            route.input_port.clone(),
                        ),
                    )?;
                    if !facts.checkpoints().contains(&checkpoint) {
                        return self.action(
                            facts,
                            checkpoint,
                            SchedulerAction::SelectBranchAndAdmit { selection },
                        );
                    }
                    require_projection_fact(
                        facts.branch_selection_at(node.id(), &occurrence)
                            == Some(&selected.case_id),
                        "Branch checkpoint does not match its committed first-winner case",
                    )?;
                    require_projection_fact(
                        facts.emitted_tokens().contains(&token_id),
                        "Branch checkpoint has no emitted selected-route token fact",
                    )?;
                    require_projection_fact(
                        facts.consumed_tokens().contains(&token_id),
                        "Branch checkpoint has no consumed selected-route token fact",
                    )?;
                    require_projection_fact(
                        facts.admitted_activations().contains(&successor_activation),
                        "Branch checkpoint has no admitted successor activation fact",
                    )?;
                    node_id = route.node_id;
                    occurrence = successor_occurrence;
                    inbound = Some(InboundControl {
                        token_id,
                        input_port: route.input_port,
                    });
                }
                NodeKind::LlmTask(_)
                | NodeKind::ActionTask(_)
                | NodeKind::RetrievalTask(_)
                | NodeKind::HttpTask(_)
                | NodeKind::ToolTask(_) => {
                    let task_id = ids.task(node.id(), &scope_instance_id, &occurrence);
                    if facts.reused_activations().contains(&activation_id) {
                        self.require_committed_outputs(index, node, facts, &occurrence)?;
                    } else {
                        let dispatch_checkpoint = ids.checkpoint(
                            node.id(),
                            &scope_instance_id,
                            &occurrence,
                            "dispatch_task",
                        );
                        if !facts.checkpoints().contains(&dispatch_checkpoint) {
                            if let Some(reason) = termination_outside_finalizer(facts, &context) {
                                return self.propagate_termination(
                                    facts,
                                    ids,
                                    index,
                                    node,
                                    &scope_instance_id,
                                    &activation_id,
                                    &occurrence,
                                    context,
                                    reason,
                                );
                            }
                            let leaf = index.leaf_descriptor(node.id()).ok_or_else(|| {
                                SchedulerError::new(
                                    SCHEDULER_GRAPH_INVALID,
                                    "leaf node has no linked descriptor",
                                )
                            })?;
                            let contract = self.linked.descriptor(node.id()).ok_or_else(|| {
                                SchedulerError::new(
                                    SCHEDULER_GRAPH_INVALID,
                                    "leaf node was not contextually linked",
                                )
                            })?;
                            let resolver = DataResolver::for_occurrence(index, facts, &occurrence);
                            let mut inputs = Vec::new();
                            for input_id in index.data_inputs(node.id()) {
                                let port = index.data_port(input_id).ok_or_else(|| {
                                    SchedulerError::new(
                                        SCHEDULER_GRAPH_INVALID,
                                        "leaf input disappeared from PlanIndex",
                                    )
                                })?;
                                if index.source_for_input(input_id).is_none() && !port.required() {
                                    continue;
                                }
                                let Some((value, mut dependencies)) =
                                    resolver.resolve_input_with_dependencies(input_id, node)?
                                else {
                                    continue;
                                };
                                dependencies.remove(&activation_id);
                                inputs.push(BoundTaskInput::with_source_activations(
                                    input_id.clone(),
                                    port.name().clone(),
                                    value,
                                    dependencies,
                                ));
                            }
                            let outputs = self.output_contracts(index, node)?;
                            let descriptor = leaf.descriptor();
                            let effect_policy = frozen_leaf_effect_policy(
                                index,
                                node,
                                contract.worker().effect_policy(),
                            )?;
                            let effect_id = match facts.redrive_effect(node.id(), &occurrence) {
                                Some(inherited) => inherited.effect_id().clone(),
                                None => ids
                                    .effect(node.id(), &scope_instance_id, &occurrence)
                                    .map_err(|error| id_error(error, "effect"))?,
                            };
                            return self.action(
                                facts,
                                dispatch_checkpoint,
                                SchedulerAction::DispatchTask {
                                    task_id,
                                    effect_id,
                                    activation_id,
                                    node_id: node.id().clone(),
                                    admission_class: task_admission_class(&context),
                                    task_kind: scheduler_task_kind(leaf.kind()),
                                    implementation: descriptor.implementation.clone(),
                                    descriptor_version: descriptor.descriptor_version.clone(),
                                    worker_version: contract.worker().worker_version().clone(),
                                    effect_policy,
                                    deployment_binding: contract.deployment_binding().clone(),
                                    public_configuration: descriptor.public_configuration.clone(),
                                    secret_configuration: descriptor.secret_configuration.clone(),
                                    inputs,
                                    outputs,
                                },
                            );
                        }
                        require_projection_fact(
                            facts.dispatched_tasks().contains(&task_id),
                            "dispatch checkpoint has no dispatched-task fact",
                        )?;

                        if let Some(outcome) = facts.task_outcomes().get(&task_id) {
                            match outcome {
                                TaskOutcomeFact::Failed { failure } => {
                                    return self.propagate_failure(
                                        facts,
                                        ids,
                                        index,
                                        node,
                                        &scope_instance_id,
                                        &activation_id,
                                        &occurrence,
                                        context,
                                        failure.clone(),
                                    );
                                }
                                TaskOutcomeFact::Succeeded { outputs } => {
                                    self.validate_outputs(index, node, outputs)?;
                                    let checkpoint = ids.checkpoint(
                                        node.id(),
                                        &scope_instance_id,
                                        &occurrence,
                                        "commit_task_outputs",
                                    );
                                    let completion_materialized = index
                                        .data_outputs(node.id())
                                        .iter()
                                        .all(|port| match outputs.get(port) {
                                            Some(expected) => {
                                                facts.exact_occurrence_value_at(port, &occurrence)
                                                    == Some(expected)
                                                    && facts.exact_occurrence_value_owner_at(
                                                        port,
                                                        &occurrence,
                                                    ) == Some(&activation_id)
                                            }
                                            None => facts
                                                .exact_occurrence_value_at(port, &occurrence)
                                                .is_none(),
                                        });
                                    // Task completion commits its frozen output receipts and both
                                    // value projections atomically. Once recovery has validated and
                                    // materialized those exact facts, a second occurrence-value
                                    // transition would only attempt to rewrite immutable rows.
                                    if !facts.checkpoints().contains(&checkpoint)
                                        && !completion_materialized
                                    {
                                        return self.action(
                                            facts,
                                            checkpoint,
                                            SchedulerAction::CommitOccurrenceValues {
                                                activation_id: activation_id.clone(),
                                                node_id: node.id().clone(),
                                                occurrence: occurrence.clone(),
                                                values: outputs.clone(),
                                            },
                                        );
                                    }
                                    self.require_committed_outputs(
                                        index,
                                        node,
                                        facts,
                                        &occurrence,
                                    )?;
                                }
                            }
                        } else if facts.completed_tasks().contains(&task_id) {
                            self.require_committed_outputs(index, node, facts, &occurrence)?;
                        } else {
                            if let Some(reason) = termination_outside_finalizer(facts, &context) {
                                return self.propagate_termination(
                                    facts,
                                    ids,
                                    index,
                                    node,
                                    &scope_instance_id,
                                    &activation_id,
                                    &occurrence,
                                    context,
                                    reason,
                                );
                            }
                            return Ok(SchedulerDecision::Quiescent(
                                SchedulerQuiescence::WaitingForTask {
                                    task_id,
                                    activation_id,
                                },
                            ));
                        }
                    }

                    let output_port = only_control_output(index, node)?;
                    match self.emit_or_advance(
                        facts,
                        ids,
                        index,
                        node,
                        &scope_instance_id,
                        &activation_id,
                        &occurrence,
                        &output_port,
                    )? {
                        OutputProgress::Action(action) => return Ok(action),
                        OutputProgress::Next {
                            node_id: next,
                            occurrence: next_occurrence,
                            inbound: next_inbound,
                        } => {
                            node_id = next;
                            occurrence = next_occurrence;
                            inbound = Some(next_inbound);
                        }
                    }
                }
                NodeKind::Merge(merge) => {
                    let resolver = DataResolver::for_occurrence(index, facts, &occurrence);
                    let mut outputs = BTreeMap::new();
                    for output_id in index.data_outputs(node.id()) {
                        outputs.insert(output_id.clone(), resolver.resolve_phi(output_id, node)?);
                    }
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        &scope_instance_id,
                        &occurrence,
                        "commit_merge_output",
                    );
                    if !facts.checkpoints().contains(&checkpoint) {
                        if let Some(reason) = termination_outside_finalizer(facts, &context) {
                            return self.propagate_termination(
                                facts,
                                ids,
                                index,
                                node,
                                &scope_instance_id,
                                &activation_id,
                                &occurrence,
                                context,
                                reason,
                            );
                        }
                        return self.action(
                            facts,
                            checkpoint,
                            SchedulerAction::CommitNativeOutput {
                                activation_id,
                                node_id: node.id().clone(),
                                occurrence: occurrence.clone(),
                                output: NativeOutput::Values { values: outputs },
                            },
                        );
                    }
                    for (port, expected) in &outputs {
                        require_projection_fact(
                            facts.value_at(port, &occurrence) == Some(expected),
                            "Merge checkpoint does not match its committed Phi output",
                        )?;
                    }
                    match self.emit_or_advance(
                        facts,
                        ids,
                        index,
                        node,
                        &scope_instance_id,
                        &activation_id,
                        &occurrence,
                        &merge.output_port,
                    )? {
                        OutputProgress::Action(action) => return Ok(action),
                        OutputProgress::Next {
                            node_id: next,
                            occurrence: next_occurrence,
                            inbound: next_inbound,
                        } => {
                            node_id = next;
                            occurrence = next_occurrence;
                            inbound = Some(next_inbound);
                        }
                    }
                }
                NodeKind::Fork(descriptor) => {
                    return self.plan_fork(
                        facts,
                        ids,
                        index,
                        node,
                        descriptor,
                        scope_instance_id,
                        activation_id,
                        occurrence,
                        context,
                    );
                }
                NodeKind::Join(_) => {
                    return Err(SchedulerError::new(
                        SCHEDULER_GRAPH_INVALID,
                        "Join was reached without its correlated Fork group",
                    ));
                }
                NodeKind::Map(descriptor) => {
                    return self.plan_map(
                        facts,
                        ids,
                        index,
                        node,
                        descriptor,
                        scope_instance_id,
                        activation_id,
                        occurrence,
                        context,
                    );
                }
                NodeKind::Collect(descriptor) => {
                    return self.plan_collect(
                        facts,
                        ids,
                        index,
                        node,
                        descriptor,
                        scope_instance_id,
                        activation_id,
                        occurrence,
                        context,
                    );
                }
                NodeKind::Loop(descriptor) => {
                    return self.plan_loop(
                        facts,
                        ids,
                        index,
                        node,
                        descriptor,
                        scope_instance_id,
                        activation_id,
                        occurrence,
                        context,
                    );
                }
                NodeKind::ErrorBoundary(descriptor) => {
                    return self.plan_error_boundary(
                        facts,
                        ids,
                        index,
                        node,
                        descriptor,
                        scope_instance_id,
                        activation_id,
                        occurrence,
                        context,
                    );
                }
                NodeKind::SubflowCall(descriptor) => {
                    return self.plan_subflow(
                        facts,
                        ids,
                        index,
                        node,
                        descriptor,
                        scope_instance_id,
                        activation_id,
                        occurrence,
                        context,
                    );
                }
                NodeKind::WaitSignal(descriptor) => {
                    return self.plan_signal_wait(
                        facts,
                        ids,
                        index,
                        node,
                        descriptor,
                        scope_instance_id,
                        activation_id,
                        occurrence,
                        context,
                        None,
                    );
                }
                NodeKind::HumanTask(descriptor) => {
                    return self.plan_signal_wait(
                        facts,
                        ids,
                        index,
                        node,
                        &crate::engine::plan::WaitSignalDescriptor {
                            signal_name: descriptor.completion_signal.clone(),
                            payload_type: descriptor.response_type.clone(),
                        },
                        scope_instance_id,
                        activation_id,
                        occurrence,
                        context,
                        Some(descriptor),
                    );
                }
                NodeKind::Timer(descriptor) => {
                    return self.plan_timer(
                        facts,
                        ids,
                        index,
                        node,
                        descriptor,
                        scope_instance_id,
                        activation_id,
                        occurrence,
                        context,
                    );
                }
                NodeKind::Return(descriptor) => {
                    if let Some(reason) = termination_outside_finalizer(facts, &context) {
                        return self.propagate_termination(
                            facts,
                            ids,
                            index,
                            node,
                            &scope_instance_id,
                            &activation_id,
                            &occurrence,
                            context,
                            reason,
                        );
                    }
                    let value = DataResolver::for_occurrence(index, facts, &occurrence)
                        .resolve_input(&descriptor.value_input, node)?;
                    require_type(&value, index.metadata().output_type(), "Run output")?;
                    let exit_checkpoint = ids.checkpoint(
                        node.id(),
                        &scope_instance_id,
                        &occurrence,
                        "commit_authored_return",
                    );
                    if !facts.checkpoints().contains(&exit_checkpoint) {
                        return self.action(
                            facts,
                            exit_checkpoint,
                            SchedulerAction::CommitNativeOutput {
                                activation_id: activation_id.clone(),
                                node_id: node.id().clone(),
                                occurrence: occurrence.clone(),
                                output: NativeOutput::Values {
                                    values: BTreeMap::new(),
                                },
                            },
                        );
                    }
                    return self.propagate_return(
                        facts,
                        ids,
                        index,
                        node,
                        &scope_instance_id,
                        &activation_id,
                        &occurrence,
                        context,
                        value,
                    );
                }
                NodeKind::Raise(descriptor) => {
                    if let Some(reason) = termination_outside_finalizer(facts, &context) {
                        return self.propagate_termination(
                            facts,
                            ids,
                            index,
                            node,
                            &scope_instance_id,
                            &activation_id,
                            &occurrence,
                            context,
                            reason,
                        );
                    }
                    let error = DataResolver::for_occurrence(index, facts, &occurrence)
                        .resolve_input(&descriptor.error_input, node)?;
                    require_type(&error, index.metadata().error_type(), "Run error")?;
                    let error = SafeError::try_from(error)?;
                    let exit_checkpoint = ids.checkpoint(
                        node.id(),
                        &scope_instance_id,
                        &occurrence,
                        "commit_authored_raise",
                    );
                    if !facts.checkpoints().contains(&exit_checkpoint) {
                        return self.action(
                            facts,
                            exit_checkpoint,
                            SchedulerAction::CommitNativeOutput {
                                activation_id: activation_id.clone(),
                                node_id: node.id().clone(),
                                occurrence: occurrence.clone(),
                                output: NativeOutput::Values {
                                    values: BTreeMap::new(),
                                },
                            },
                        );
                    }
                    let failure = TaskFailureFact::new(
                        WorkerFailureClass::SafeBusinessFailure,
                        error.code(),
                        Some(error.runtime_value().clone()),
                    )?;
                    if !context.frames.is_empty() {
                        return self.propagate_failure(
                            facts,
                            ids,
                            index,
                            node,
                            &scope_instance_id,
                            &activation_id,
                            &occurrence,
                            context,
                            failure,
                        );
                    }
                    let checkpoint =
                        ids.checkpoint(node.id(), &scope_instance_id, &occurrence, "fail_run");
                    if !facts.checkpoints().contains(&checkpoint) {
                        return self.action(
                            facts,
                            checkpoint,
                            SchedulerAction::FailRun {
                                activation_id,
                                error,
                            },
                        );
                    }
                    require_projection_fact(
                        facts.terminal() == Some(&RunTerminalFact::Failed(error)),
                        "failure checkpoint does not match terminal Run error",
                    )?;
                    return Ok(SchedulerDecision::Quiescent(SchedulerQuiescence::RunFailed));
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn ensure_activation(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        node: &Node,
        scope_instance_id: &ScopeInstanceId,
        activation_id: &ActivationId,
        occurrence: &LogicalOccurrence,
        inbound: Option<&InboundControl>,
    ) -> Result<Option<SchedulerDecision>, SchedulerError> {
        let checkpoint =
            ids.checkpoint(node.id(), scope_instance_id, occurrence, "admit_activation");
        if !facts.admitted_activations().contains(activation_id)
            && !facts.checkpoints().contains(&checkpoint)
        {
            let reuse_candidate = facts
                .pending_reuse_candidate(scope_instance_id, node.id(), occurrence)
                .map(|candidate| {
                    ReuseAdmissionCandidate::new(
                        candidate.candidate_id(),
                        candidate.projection_version(),
                        self.leaf_reuse_contract(facts, node, occurrence)?,
                    )
                })
                .transpose()?;
            return self
                .action(
                    facts,
                    checkpoint,
                    SchedulerAction::AdmitActivation {
                        activation_id: activation_id.clone(),
                        node_id: node.id().clone(),
                        scope_instance_id: scope_instance_id.clone(),
                        occurrence: occurrence.clone(),
                        reuse_candidate,
                    },
                )
                .map(Some);
        }
        require_projection_fact(
            facts.admitted_activations().contains(activation_id),
            "activation checkpoint or structural spawn has no admitted activation fact",
        )?;

        if let Some(inbound) = inbound {
            require_projection_fact(
                facts.emitted_tokens().contains(&inbound.token_id),
                "successor activation references an uncommitted control token",
            )?;
            let checkpoint =
                ids.checkpoint(node.id(), scope_instance_id, occurrence, "consume_token");
            if !facts.consumed_tokens().contains(&inbound.token_id)
                && !facts.checkpoints().contains(&checkpoint)
            {
                return self
                    .action(
                        facts,
                        checkpoint,
                        SchedulerAction::ConsumeToken {
                            token_id: inbound.token_id.clone(),
                            target_activation_id: activation_id.clone(),
                            input_port: inbound.input_port.clone(),
                        },
                    )
                    .map(Some);
            }
            require_projection_fact(
                facts.consumed_tokens().contains(&inbound.token_id),
                "consume checkpoint has no consumed-token fact",
            )?;
        }
        Ok(None)
    }

    fn leaf_reuse_contract(
        &self,
        facts: &SchedulerFacts,
        node: &Node,
        occurrence: &LogicalOccurrence,
    ) -> Result<Option<ReuseAdmissionContract>, SchedulerError> {
        let index = self.linked.index();
        let Some(leaf) = index.leaf_descriptor(node.id()) else {
            return Ok(None);
        };
        let contract = self.linked.descriptor(node.id()).ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "reuse target leaf was not contextually linked",
            )
        })?;
        let resolver = DataResolver::for_occurrence(index, facts, occurrence);
        let mut inputs = Vec::new();
        for input_id in index.data_inputs(node.id()) {
            let port = index
                .data_port(input_id)
                .ok_or_else(|| graph("reuse target input disappeared from PlanIndex"))?;
            if index.source_for_input(input_id).is_none() && !port.required() {
                continue;
            }
            let Some((value, dependencies)) =
                resolver.resolve_input_with_dependencies(input_id, node)?
            else {
                continue;
            };
            inputs.push(BoundTaskInput::with_source_activations(
                input_id.clone(),
                port.name().clone(),
                value,
                dependencies,
            ));
        }
        let outputs = self.output_contracts(index, node)?;
        let descriptor = leaf.descriptor();
        let effect_policy =
            frozen_leaf_effect_policy(index, node, contract.worker().effect_policy())?;
        ReuseAdmissionContract::from_task_parts(
            scheduler_task_kind(leaf.kind()),
            &descriptor.implementation,
            &descriptor.descriptor_version,
            contract.worker().worker_version(),
            &effect_policy,
            contract.deployment_binding(),
            &descriptor.public_configuration,
            &descriptor.secret_configuration,
            &inputs,
            &outputs,
            None,
        )
        .map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    fn intercept_structural_arrival(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        occurrence: &LogicalOccurrence,
        inbound: Option<&InboundControl>,
        context: &PathContext,
    ) -> Result<Option<SchedulerDecision>, SchedulerError> {
        let Some(inbound) = inbound else {
            return Ok(None);
        };
        match node.kind() {
            NodeKind::Join(join) => {
                if let Some((key, fork_node_id)) =
                    context.frames.iter().rev().find_map(|frame| match frame {
                        StructuralFrame::ForkLeg { fork_node_id, key }
                            if fork_node_id == &join.fork_node_id =>
                        {
                            Some((key, fork_node_id))
                        }
                        _ => None,
                    })
                {
                    let fork = index
                        .node(fork_node_id)
                        .ok_or_else(|| graph("Fork disappeared"))?;
                    let NodeKind::Fork(descriptor) = fork.kind() else {
                        return Err(graph("Join correlation owner is not a Fork"));
                    };
                    let leg = descriptor
                        .legs
                        .iter()
                        .find(|leg| &leg.leg_id == key.leg_id())
                        .ok_or_else(|| graph("Join arrival references an unknown Fork leg"))?;
                    let value = facts
                        .value_at(&leg.yield_port, occurrence)
                        .cloned()
                        .ok_or_else(|| {
                            SchedulerError::new(
                                SCHEDULER_FACT_MISSING,
                                "successful Fork leg has no committed typed yield",
                            )
                        })?;
                    let scope = self.scope_instance(ids, index, fork, occurrence)?;
                    let checkpoint = ids.checkpoint(
                        fork.id(),
                        &scope,
                        occurrence,
                        &format!("settle_leg:{}", key.leg_id()),
                    );
                    if !facts.checkpoints().contains(&checkpoint) {
                        let leg = facts.fork_legs().get(key).cloned().ok_or_else(|| {
                            SchedulerError::new(
                                SCHEDULER_FACT_MISSING,
                                "Fork settlement has no admitted leg fact",
                            )
                        })?;
                        return self
                            .action(
                                facts,
                                checkpoint,
                                SchedulerAction::SettleForkLeg {
                                    leg,
                                    outcome: StructuralOutcomeFact::Succeeded { value },
                                },
                            )
                            .map(Some);
                    }
                    require_projection_fact(
                        facts.fork_settlements().contains_key(key),
                        "Fork settlement checkpoint has no leg settlement fact",
                    )?;
                    return Ok(Some(SchedulerDecision::Quiescent(
                        SchedulerQuiescence::WaitingForChildren {
                            scope_instance_ids: vec![],
                        },
                    )));
                }
            }
            NodeKind::Collect(collect) => match &collect.source {
                CollectSource::Map { map_node_id }
                | CollectSource::DynamicMap { map_node_id, .. } => {
                    if let Some(key) = context.frames.iter().rev().find_map(|frame| match frame {
                        StructuralFrame::MapItem {
                            map_node_id: owner,
                            key,
                        } if owner == map_node_id => Some(key),
                        _ => None,
                    }) {
                        let map = index
                            .node(map_node_id)
                            .ok_or_else(|| graph("Map disappeared"))?;
                        let NodeKind::Map(descriptor) = map.kind() else {
                            return Err(graph("Collect source owner is not a Map"));
                        };
                        let value = facts
                            .value_at(&descriptor.yield_port, occurrence)
                            .cloned()
                            .ok_or_else(|| {
                                SchedulerError::new(
                                    SCHEDULER_FACT_MISSING,
                                    "successful Map item has no committed typed yield",
                                )
                            })?;
                        let scope = self.scope_instance(ids, index, map, occurrence)?;
                        let checkpoint = ids.checkpoint(
                            map.id(),
                            &scope,
                            occurrence,
                            &format!("settle_item:{}", key.stable_dynamic_key()),
                        );
                        if !facts.checkpoints().contains(&checkpoint) {
                            let item = facts.map_items().get(key).cloned().ok_or_else(|| {
                                SchedulerError::new(
                                    SCHEDULER_FACT_MISSING,
                                    "Map settlement has no admitted item fact",
                                )
                            })?;
                            return self
                                .action(
                                    facts,
                                    checkpoint,
                                    SchedulerAction::SettleMapItem {
                                        item,
                                        outcome: StructuralOutcomeFact::Succeeded { value },
                                    },
                                )
                                .map(Some);
                        }
                        require_projection_fact(
                            facts.map_settlements().contains_key(key),
                            "Map settlement checkpoint has no item settlement fact",
                        )?;
                        return Ok(Some(SchedulerDecision::Quiescent(
                            SchedulerQuiescence::WaitingForChildren {
                                scope_instance_ids: vec![],
                            },
                        )));
                    }
                }
                CollectSource::Loop {
                    loop_node_id,
                    yield_port,
                    break_input,
                    ..
                } if break_input.as_ref() == Some(&inbound.input_port) => {
                    if let Some(key) = context.frames.iter().rev().find_map(|frame| match frame {
                        StructuralFrame::LoopIteration {
                            loop_node_id: owner,
                            key,
                        } if owner == loop_node_id => Some(key),
                        _ => None,
                    }) {
                        let value =
                            facts
                                .value_at(yield_port, occurrence)
                                .cloned()
                                .ok_or_else(|| {
                                    SchedulerError::new(
                                        SCHEDULER_FACT_MISSING,
                                        "Loop break has no committed next state",
                                    )
                                })?;
                        let loop_node = index
                            .node(loop_node_id)
                            .ok_or_else(|| graph("Loop disappeared"))?;
                        let scope = self.scope_instance(ids, index, loop_node, occurrence)?;
                        let checkpoint = ids.checkpoint(
                            loop_node.id(),
                            &scope,
                            occurrence,
                            &format!("break_iteration:{}", key.iteration()),
                        );
                        if !facts.checkpoints().contains(&checkpoint) {
                            let iteration =
                                facts.loop_iterations().get(key).cloned().ok_or_else(|| {
                                    SchedulerError::new(
                                        SCHEDULER_FACT_MISSING,
                                        "Loop break has no admitted iteration fact",
                                    )
                                })?;
                            return self
                                .action(
                                    facts,
                                    checkpoint,
                                    SchedulerAction::CompleteLoop {
                                        loop_activation_id: key.loop_activation_id().clone(),
                                        iteration: Some(iteration),
                                        state: value,
                                    },
                                )
                                .map(Some);
                        }
                        require_projection_fact(
                            facts
                                .loop_instances()
                                .get(key.loop_activation_id())
                                .is_some_and(LoopInstanceFact::completed),
                            "Loop break checkpoint has no completed loop fact",
                        )?;
                        return Ok(Some(SchedulerDecision::Quiescent(
                            SchedulerQuiescence::WaitingForChildren {
                                scope_instance_ids: vec![],
                            },
                        )));
                    }
                }
                _ => {}
            },
            NodeKind::Loop(loop_descriptor)
                if inbound.input_port == loop_descriptor.continue_input =>
            {
                if let Some(key) = context.frames.iter().rev().find_map(|frame| match frame {
                    StructuralFrame::LoopIteration {
                        loop_node_id: owner,
                        key,
                    } if owner == node.id() => Some(key),
                    _ => None,
                }) {
                    let collect = self.loop_collect(index, node.id())?;
                    let CollectSource::Loop { yield_port, .. } = &collect.source else {
                        unreachable!()
                    };
                    let value =
                        facts
                            .value_at(yield_port, occurrence)
                            .cloned()
                            .ok_or_else(|| {
                                SchedulerError::new(
                                    SCHEDULER_FACT_MISSING,
                                    "Loop continue has no committed next state",
                                )
                            })?;
                    let scope = self.scope_instance(ids, index, node, occurrence)?;
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        &scope,
                        occurrence,
                        &format!("advance_iteration:{}", key.iteration()),
                    );
                    if !facts.checkpoints().contains(&checkpoint) {
                        let iteration =
                            facts.loop_iterations().get(key).cloned().ok_or_else(|| {
                                SchedulerError::new(
                                    SCHEDULER_FACT_MISSING,
                                    "Loop advance has no admitted iteration fact",
                                )
                            })?;
                        return self
                            .action(
                                facts,
                                checkpoint,
                                SchedulerAction::AdvanceLoop {
                                    iteration,
                                    state: value,
                                },
                            )
                            .map(Some);
                    }
                    require_projection_fact(
                        facts
                            .loop_instances()
                            .get(key.loop_activation_id())
                            .is_some_and(|instance| instance.next_iteration() > key.iteration()),
                        "Loop advance checkpoint has no advanced state fact",
                    )?;
                    return Ok(Some(SchedulerDecision::Quiescent(
                        SchedulerQuiescence::WaitingForChildren {
                            scope_instance_ids: vec![],
                        },
                    )));
                }
            }
            NodeKind::ErrorBoundary(boundary) => {
                let matched = context.frames.iter().rev().find_map(|frame| match frame {
                    StructuralFrame::ErrorBoundary {
                        boundary_node_id,
                        boundary_activation_id,
                        boundary_occurrence,
                        phase,
                    } if boundary_node_id == node.id()
                        && ((*phase == ErrorBoundaryPhase::Protected
                            && boundary.protected_completed_input.as_ref()
                                == Some(&inbound.input_port))
                            || (*phase == ErrorBoundaryPhase::Handler
                                && boundary.handler_completed_input.as_ref()
                                    == Some(&inbound.input_port))
                            || (*phase == ErrorBoundaryPhase::Finalizer
                                && boundary.finalizer_completed_input.as_ref()
                                    == Some(&inbound.input_port))) =>
                    {
                        Some((boundary_activation_id, boundary_occurrence, phase))
                    }
                    _ => None,
                });
                if let Some((boundary_activation_id, boundary_occurrence, phase)) = matched {
                    let scope = self.scope_instance(ids, index, node, occurrence)?;
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        &scope,
                        occurrence,
                        match phase {
                            ErrorBoundaryPhase::Protected => "complete_protected",
                            ErrorBoundaryPhase::Handler => "complete_handler",
                            ErrorBoundaryPhase::Finalizer => "complete_finalizer",
                            ErrorBoundaryPhase::Completed => unreachable!(),
                        },
                    );
                    let current = facts
                        .boundary_states()
                        .get(boundary_activation_id)
                        .ok_or_else(|| {
                            SchedulerError::new(
                                SCHEDULER_FACT_INCONSISTENT,
                                "ErrorBoundary child completion has no durable boundary state",
                            )
                        })?;
                    let (next_phase, exit) = match phase {
                        ErrorBoundaryPhase::Protected | ErrorBoundaryPhase::Handler
                            if boundary.finalizer_output.is_some() =>
                        {
                            (ErrorBoundaryPhase::Finalizer, ErrorBoundaryExit::Continue)
                        }
                        ErrorBoundaryPhase::Protected | ErrorBoundaryPhase::Handler => {
                            (ErrorBoundaryPhase::Completed, ErrorBoundaryExit::Continue)
                        }
                        ErrorBoundaryPhase::Finalizer => {
                            (ErrorBoundaryPhase::Completed, current.exit().clone())
                        }
                        ErrorBoundaryPhase::Completed => unreachable!(),
                    };
                    let completed = ErrorBoundaryFact::with_exit(
                        boundary_activation_id.clone(),
                        node.id().clone(),
                        boundary_occurrence.clone(),
                        next_phase,
                        None,
                        exit,
                    )?;
                    if !facts.checkpoints().contains(&checkpoint) {
                        return self
                            .action(
                                facts,
                                checkpoint,
                                SchedulerAction::TransitionErrorBoundary {
                                    boundary: completed,
                                },
                            )
                            .map(Some);
                    }
                    require_projection_fact(
                        facts
                            .boundary_states()
                            .get(boundary_activation_id)
                            .is_some_and(|state| state == &completed),
                        "ErrorBoundary completion checkpoint has no matching durable state fact",
                    )?;
                    return Ok(Some(SchedulerDecision::Quiescent(
                        SchedulerQuiescence::WaitingForChildren {
                            scope_instance_ids: vec![],
                        },
                    )));
                }
            }
            _ => {}
        }
        Ok(None)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn plan_fork(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        descriptor: &crate::engine::plan::ForkDescriptor,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        occurrence: LogicalOccurrence,
        context: PathContext,
    ) -> Result<SchedulerDecision, SchedulerError> {
        let group_id = ids
            .fork_group(node.id(), &scope_instance_id, &occurrence)
            .map_err(|error| id_error(error, "fork group"))?;
        let expected = ForkGroupFact::new(
            group_id.clone(),
            node.id().clone(),
            activation_id.clone(),
            scope_instance_id.clone(),
            occurrence.clone(),
            descriptor.join_mode,
            descriptor
                .legs
                .iter()
                .map(|leg| leg.leg_id.clone())
                .collect(),
        )?;
        let mut legs = Vec::with_capacity(descriptor.legs.len());
        for leg in &descriptor.legs {
            let key = ForkLegKey::new(group_id.clone(), leg.leg_id.clone());
            let base = occurrence.child(format!("fork_leg:{}", leg.leg_id))?;
            let route = route_for_output(index, &leg.output_port)?;
            let body_occurrence = base.child(format!("edge:{}", route.edge_id))?;
            let target = index
                .node(&route.node_id)
                .ok_or_else(|| graph("Fork leg route target disappeared"))?;
            let child_static_scope = self.runtime_owning_scope(index, target)?.id().clone();
            let child_scope = self.scope_instance(ids, index, target, &body_occurrence)?;
            let child_activation = ids
                .activation(target.id(), &child_scope, &body_occurrence)
                .map_err(|error| id_error(error, "fork leg activation"))?;
            let token_id = ids
                .control_token(node.id(), &scope_instance_id, &base, &leg.output_port)
                .map_err(|error| id_error(error, "fork leg token"))?;
            legs.push(ForkLegAdmissionFact::new(
                ForkLegFact::new(
                    key,
                    body_occurrence,
                    child_scope,
                    child_static_scope,
                    target.id().clone(),
                    child_activation,
                    token_id,
                ),
                leg.output_port.clone(),
            ));
        }
        let admission = ForkAdmissionFact::new(expected.clone(), legs)?;
        let checkpoint = ids.checkpoint(node.id(), &scope_instance_id, &occurrence, "open_fork");
        if !facts.checkpoints().contains(&checkpoint) {
            if let Some(reason) = termination_outside_finalizer(facts, &context) {
                return self.propagate_termination(
                    facts,
                    ids,
                    index,
                    node,
                    &scope_instance_id,
                    &activation_id,
                    &occurrence,
                    context,
                    reason,
                );
            }
            return self.action(facts, checkpoint, SchedulerAction::OpenFork { admission });
        }
        require_projection_fact(
            facts.fork_groups().get(&group_id) == Some(&expected),
            "Fork checkpoint does not match its persisted group membership",
        )?;
        for leg in admission.legs() {
            require_projection_fact(
                facts.fork_legs().get(leg.leg().key()) == Some(leg.leg()),
                "Fork checkpoint is missing an exact declared leg fact",
            )?;
            require_projection_fact(
                facts
                    .admitted_activations()
                    .contains(leg.leg().child_activation_id()),
                "Fork checkpoint has a leg without its admitted child activation fact",
            )?;
            require_projection_fact(
                facts.emitted_tokens().contains(leg.leg().token_id()),
                "Fork checkpoint has a leg without its emitted first-token fact",
            )?;
        }

        let fatal = descriptor.legs.iter().find_map(|leg| {
            let key = ForkLegKey::new(group_id.clone(), leg.leg_id.clone());
            facts
                .fork_settlements()
                .get(&key)
                .and_then(StructuralOutcomeFact::failure)
                .filter(|failure| {
                    descriptor.join_mode == PlanJoinMode::AllSuccess
                        || failure.class() != WorkerFailureClass::SafeBusinessFailure
                })
                .cloned()
        });
        if let Some(failure) = fatal {
            let mut draining = Vec::new();
            for leg in &descriptor.legs {
                let key = ForkLegKey::new(group_id.clone(), leg.leg_id.clone());
                if facts.fork_settlements().contains_key(&key) {
                    continue;
                }
                let child = facts
                    .fork_legs()
                    .get(&key)
                    .ok_or_else(|| graph("Fork member was not spawned"))?;
                if !facts
                    .scope_cancellation_requests()
                    .contains(child.scope_instance_id())
                {
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        &scope_instance_id,
                        &occurrence,
                        &format!("cancel_leg:{}", leg.leg_id),
                    );
                    return self.action(
                        facts,
                        checkpoint,
                        SchedulerAction::RequestScopeCancellation {
                            scope_instance_id: child.scope_instance_id().clone(),
                            reason: SchedulerCancellationReason::SiblingFailed,
                        },
                    );
                }
                draining.push(child.scope_instance_id().clone());
            }
            if !draining.is_empty() {
                return Ok(SchedulerDecision::Quiescent(
                    SchedulerQuiescence::WaitingForDrain {
                        scope_instance_ids: draining,
                    },
                ));
            }
            return self.propagate_failure(
                facts,
                ids,
                index,
                node,
                &scope_instance_id,
                &activation_id,
                &occurrence,
                context,
                failure,
            );
        }

        let all_settled = descriptor.legs.iter().all(|leg| {
            facts
                .fork_settlements()
                .contains_key(&ForkLegKey::new(group_id.clone(), leg.leg_id.clone()))
        });
        if !all_settled {
            let mut waiting_scopes = Vec::new();
            for leg in &descriptor.legs {
                let key = ForkLegKey::new(group_id.clone(), leg.leg_id.clone());
                if facts.fork_settlements().contains_key(&key) {
                    continue;
                }
                let child = facts
                    .fork_legs()
                    .get(&key)
                    .ok_or_else(|| graph("Fork member was not spawned"))?;
                let route = route_for_output(index, &leg.output_port)?;
                let mut child_context = context.clone();
                child_context.frames.push(StructuralFrame::ForkLeg {
                    fork_node_id: node.id().clone(),
                    key,
                });
                let decision = self.plan_path(
                    facts,
                    ids,
                    route.node_id,
                    child.occurrence().clone(),
                    Some(InboundControl {
                        token_id: child.token_id().clone(),
                        input_port: route.input_port,
                    }),
                    child_context,
                )?;
                match decision {
                    SchedulerDecision::Action(_) => return Ok(decision),
                    SchedulerDecision::Quiescent(_) => {
                        waiting_scopes.push(child.scope_instance_id().clone())
                    }
                }
            }
            return Ok(SchedulerDecision::Quiescent(
                SchedulerQuiescence::WaitingForChildren {
                    scope_instance_ids: waiting_scopes,
                },
            ));
        }

        let join = index
            .nodes()
            .find(|candidate| {
                matches!(candidate.kind(), NodeKind::Join(value) if value.fork_node_id == *node.id())
            })
            .ok_or_else(|| graph("Fork has no correlated Join"))?;
        let NodeKind::Join(join_descriptor) = join.kind() else {
            unreachable!()
        };
        let join_occurrence = occurrence.child(format!("join_group:{}", group_id))?;
        let join_scope = self.scope_instance(ids, index, join, &join_occurrence)?;
        let join_activation = ids
            .activation(join.id(), &join_scope, &join_occurrence)
            .map_err(|error| id_error(error, "join activation"))?;
        if let Some(action) = self.ensure_activation(
            facts,
            ids,
            join,
            &join_scope,
            &join_activation,
            &join_occurrence,
            None,
        )? {
            return Ok(action);
        }
        let checkpoint = ids.checkpoint(join.id(), &join_scope, &join_occurrence, "complete_join");
        if !facts.checkpoints().contains(&checkpoint) {
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::CompleteFork {
                    group_id: group_id.clone(),
                    join_activation_id: join_activation.clone(),
                },
            );
        }
        require_projection_fact(
            facts.completed_forks().contains(&group_id),
            "Join completion checkpoint has no completed Fork fact",
        )?;
        match self.emit_or_advance(
            facts,
            ids,
            index,
            join,
            &join_scope,
            &join_activation,
            &join_occurrence,
            &join_descriptor.output_port,
        )? {
            OutputProgress::Action(action) => Ok(action),
            OutputProgress::Next {
                node_id,
                occurrence,
                inbound,
            } => {
                let mut next_context = context;
                next_context.collect = Some(CollectContext::StaticFork(group_id));
                self.plan_path(facts, ids, node_id, occurrence, Some(inbound), next_context)
            }
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn plan_map(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        descriptor: &MapDescriptor,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        occurrence: LogicalOccurrence,
        context: PathContext,
    ) -> Result<SchedulerDecision, SchedulerError> {
        let collect = self.map_collect(index, node.id())?;
        let key_field = match &collect.source {
            CollectSource::DynamicMap { key_field, .. } => key_field.as_deref(),
            CollectSource::Map { .. } => None,
            _ => return Err(graph("Map is correlated with a non-Map Collect")),
        };
        let items = DataResolver::for_occurrence(index, facts, &occurrence)
            .evaluate_expression(&descriptor.items, node)?;
        let Value::Array(values) = items.value() else {
            return Err(SchedulerError::new(
                SCHEDULER_VALUE_TYPE_MISMATCH,
                "Map items expression did not evaluate to an array",
            ));
        };
        let mut seeds = Vec::with_capacity(values.len());
        let mut identities = BTreeSet::new();
        for (ordinal, item) in values.iter().enumerate() {
            let ordinal =
                u32::try_from(ordinal).map_err(|_| graph("Map input exceeds u32 item capacity"))?;
            let identity = match key_field {
                Some(field) => {
                    let raw_key = item
                        .as_object()
                        .and_then(|object| object.get(field))
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            SchedulerError::new(
                                SCHEDULER_VALUE_TYPE_MISMATCH,
                                "Map item is missing its required stable string key",
                            )
                        })?;
                    MapItemIdentity::BusinessKey(DynamicKey::new(raw_key.to_owned()).map_err(
                        |error| {
                            SchedulerError::new(
                                SCHEDULER_DYNAMIC_KEY_DUPLICATE,
                                format!(
                                    "Map item key is outside the durable identity contract: {error}"
                                ),
                            )
                        },
                    )?)
                }
                None => MapItemIdentity::Ordinal(ordinal),
            };
            if !identities.insert(identity.clone()) {
                return Err(SchedulerError::new(
                    SCHEDULER_DYNAMIC_KEY_DUPLICATE,
                    "Map input contains a duplicate stable key",
                ));
            }
            seeds.push(MapItemSeed::new(
                ordinal,
                identity,
                RuntimeValue::new(item.clone())?,
            ));
        }
        let expected = MapInstanceFact::new(
            activation_id.clone(),
            node.id().clone(),
            occurrence.clone(),
            seeds,
            descriptor.max_concurrency,
        )?;
        let checkpoint = ids.checkpoint(node.id(), &scope_instance_id, &occurrence, "open_map");
        if !facts.checkpoints().contains(&checkpoint) {
            if let Some(reason) = termination_outside_finalizer(facts, &context) {
                return self.propagate_termination(
                    facts,
                    ids,
                    index,
                    node,
                    &scope_instance_id,
                    &activation_id,
                    &occurrence,
                    context,
                    reason,
                );
            }
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::OpenMap { map: expected },
            );
        }
        require_projection_fact(
            facts.map_instances().get(&activation_id) == Some(&expected),
            "Map checkpoint does not match its persisted input snapshot",
        )?;
        let instance = facts
            .map_instances()
            .get(&activation_id)
            .expect("Map fact was checked");
        let active = instance
            .items()
            .iter()
            .filter(|seed| {
                let key = MapItemKey::new(activation_id.clone(), seed.identity().clone());
                facts.map_items().contains_key(&key) && !facts.map_settlements().contains_key(&key)
            })
            .count();
        let limit = descriptor
            .max_concurrency
            .map_or(usize::MAX, |value| value as usize);
        if active < limit {
            if let Some(seed) = instance.items().iter().find(|seed| {
                !facts.map_items().contains_key(&MapItemKey::new(
                    activation_id.clone(),
                    seed.identity().clone(),
                ))
            }) {
                let key = MapItemKey::new(activation_id.clone(), seed.identity().clone());
                let stable_dynamic_key = seed.stable_dynamic_key();
                let digest = ContentHash::from_bytes(stable_dynamic_key.as_bytes());
                let base = occurrence.child(format!(
                    "map_item:{}",
                    digest.as_str().trim_start_matches("sha256:")
                ))?;
                let route = route_for_output(index, &descriptor_body_output(index, node)?)?;
                let body_occurrence = base.child(format!("edge:{}", route.edge_id))?;
                let target = index
                    .node(&route.node_id)
                    .ok_or_else(|| graph("Map body route target disappeared"))?;
                let child_static_scope = self.runtime_owning_scope(index, target)?.id().clone();
                let child_scope = self.scope_instance(ids, index, target, &body_occurrence)?;
                let child_activation = ids
                    .activation(target.id(), &child_scope, &body_occurrence)
                    .map_err(|error| id_error(error, "Map item activation"))?;
                let output = descriptor_body_output(index, node)?;
                let token_id = ids
                    .control_token(node.id(), &scope_instance_id, &base, &output)
                    .map_err(|error| id_error(error, "Map item token"))?;
                let item = MapItemFact::new(
                    key,
                    seed.ordinal(),
                    body_occurrence,
                    child_scope,
                    child_static_scope,
                    target.id().clone(),
                    child_activation,
                    token_id,
                );
                let checkpoint = ids.checkpoint(
                    node.id(),
                    &scope_instance_id,
                    &occurrence,
                    &format!("spawn_item:{stable_dynamic_key}"),
                );
                return self.action(
                    facts,
                    checkpoint,
                    SchedulerAction::SpawnMapItem {
                        item,
                        item_port: descriptor.item_port.clone(),
                        item_value: seed.value().clone(),
                        output_port: output,
                    },
                );
            }
        }

        let failure = instance.items().iter().find_map(|seed| {
            facts
                .map_settlements()
                .get(&MapItemKey::new(
                    activation_id.clone(),
                    seed.identity().clone(),
                ))
                .and_then(StructuralOutcomeFact::failure)
                .cloned()
        });
        if let Some(failure) = failure {
            let mut draining = Vec::new();
            for item in facts
                .map_items()
                .values()
                .filter(|item| item.key().map_activation_id() == &activation_id)
            {
                if facts.map_settlements().contains_key(item.key()) {
                    continue;
                }
                if !facts
                    .scope_cancellation_requests()
                    .contains(item.scope_instance_id())
                {
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        &scope_instance_id,
                        &occurrence,
                        &format!("cancel_item:{}", item.key().stable_dynamic_key()),
                    );
                    return self.action(
                        facts,
                        checkpoint,
                        SchedulerAction::RequestScopeCancellation {
                            scope_instance_id: item.scope_instance_id().clone(),
                            reason: SchedulerCancellationReason::SiblingFailed,
                        },
                    );
                }
                draining.push(item.scope_instance_id().clone());
            }
            if !draining.is_empty() {
                return Ok(SchedulerDecision::Quiescent(
                    SchedulerQuiescence::WaitingForDrain {
                        scope_instance_ids: draining,
                    },
                ));
            }
            return self.propagate_failure(
                facts,
                ids,
                index,
                node,
                &scope_instance_id,
                &activation_id,
                &occurrence,
                context,
                failure,
            );
        }

        let all_settled = instance.items().iter().all(|seed| {
            facts.map_settlements().contains_key(&MapItemKey::new(
                activation_id.clone(),
                seed.identity().clone(),
            ))
        });
        if !all_settled {
            let mut waiting = Vec::new();
            let output = descriptor_body_output(index, node)?;
            let route = route_for_output(index, &output)?;
            for item in facts
                .map_items()
                .values()
                .filter(|item| item.key().map_activation_id() == &activation_id)
            {
                if facts.map_settlements().contains_key(item.key()) {
                    continue;
                }
                let mut child_context = context.clone();
                child_context.frames.push(StructuralFrame::MapItem {
                    map_node_id: node.id().clone(),
                    key: item.key().clone(),
                });
                let decision = self.plan_path(
                    facts,
                    ids,
                    route.node_id.clone(),
                    item.occurrence().clone(),
                    Some(InboundControl {
                        token_id: item.token_id.clone(),
                        input_port: route.input_port.clone(),
                    }),
                    child_context,
                )?;
                match decision {
                    SchedulerDecision::Action(_) => return Ok(decision),
                    SchedulerDecision::Quiescent(_) => {
                        waiting.push(item.scope_instance_id().clone())
                    }
                }
            }
            return Ok(SchedulerDecision::Quiescent(
                SchedulerQuiescence::WaitingForChildren {
                    scope_instance_ids: waiting,
                },
            ));
        }

        let checkpoint = ids.checkpoint(node.id(), &scope_instance_id, &occurrence, "complete_map");
        if !facts.checkpoints().contains(&checkpoint) {
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::CompleteMap {
                    map_activation_id: activation_id.clone(),
                },
            );
        }
        require_projection_fact(
            facts.completed_maps().contains(&activation_id),
            "Map completion checkpoint has no completed map fact",
        )?;
        let collect_node = index
            .nodes()
            .find(|candidate| candidate.kind() == &NodeKind::Collect(collect.clone()))
            .ok_or_else(|| graph("Map Collect node disappeared"))?;
        let mut next_context = context;
        next_context.collect = Some(CollectContext::Map(activation_id));
        self.plan_path(
            facts,
            ids,
            collect_node.id().clone(),
            occurrence.child(format!("map_collect:{}", node.id()))?,
            None,
            next_context,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_loop(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        descriptor: &LoopDescriptor,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        occurrence: LogicalOccurrence,
        context: PathContext,
    ) -> Result<SchedulerDecision, SchedulerError> {
        let collect = self.loop_collect(index, node.id())?;
        let CollectSource::Loop {
            initial_input,
            state_port,
            ..
        } = &collect.source
        else {
            unreachable!()
        };
        let initial = DataResolver::for_occurrence(index, facts, &occurrence)
            .resolve_input(initial_input, node)?;
        let deadline = descriptor
            .deadline_ms
            .map(|delay| {
                facts
                    .observed_time_ms()
                    .checked_add(delay)
                    .ok_or_else(|| graph("Loop deadline overflowed u64"))
            })
            .transpose()?;
        let expected = LoopInstanceFact::new(
            activation_id.clone(),
            node.id().clone(),
            descriptor.flavor,
            occurrence.clone(),
            initial,
            0,
            facts.observed_time_ms(),
            deadline,
            false,
        )?;
        let checkpoint = ids.checkpoint(node.id(), &scope_instance_id, &occurrence, "open_loop");
        if !facts.checkpoints().contains(&checkpoint) {
            if let Some(reason) = termination_outside_finalizer(facts, &context) {
                return self.propagate_termination(
                    facts,
                    ids,
                    index,
                    node,
                    &scope_instance_id,
                    &activation_id,
                    &occurrence,
                    context,
                    reason,
                );
            }
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::OpenLoop {
                    loop_instance: expected,
                },
            );
        }
        let instance = facts.loop_instances().get(&activation_id).ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "Loop checkpoint has no loop instance fact",
            )
        })?;
        require_projection_fact(
            instance.flavor() == descriptor.flavor,
            "Loop instance flavor differs from its immutable Plan descriptor",
        )?;
        if instance.completed() {
            let collect_node = self.loop_collect_node(index, node.id())?;
            let mut next = context;
            next.collect = Some(CollectContext::Loop(activation_id));
            return self.plan_path(
                facts,
                ids,
                collect_node.id().clone(),
                occurrence.child(format!("loop_collect:{}", node.id()))?,
                None,
                next,
            );
        }

        let iteration_key = LoopIterationKey::new(activation_id.clone(), instance.next_iteration());
        if let Some(iteration) = facts.loop_iterations().get(&iteration_key) {
            let route = route_for_output(index, &descriptor.body_output)?;
            let mut child_context = context;
            child_context.frames.push(StructuralFrame::LoopIteration {
                loop_node_id: node.id().clone(),
                key: iteration_key,
            });
            return self.plan_path(
                facts,
                ids,
                route.node_id,
                iteration.occurrence().clone(),
                Some(InboundControl {
                    token_id: iteration.token_id.clone(),
                    input_port: route.input_port,
                }),
                child_context,
            );
        }

        let exit = DataResolver::for_occurrence(index, facts, &occurrence)
            .evaluate_expression(&descriptor.exit_condition, node)?;
        let Value::Bool(exit) = exit.value() else {
            return Err(SchedulerError::new(
                SCHEDULER_VALUE_TYPE_MISMATCH,
                "Loop exit condition did not evaluate to Boolean",
            ));
        };
        if *exit {
            let checkpoint = ids.checkpoint(
                node.id(),
                &scope_instance_id,
                &occurrence,
                "complete_loop_condition",
            );
            if !facts.checkpoints().contains(&checkpoint) {
                return self.action(
                    facts,
                    checkpoint,
                    SchedulerAction::CompleteLoop {
                        loop_activation_id: activation_id,
                        iteration: None,
                        state: instance.state().clone(),
                    },
                );
            }
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "Loop completion checkpoint has no completed loop fact",
            ));
        }
        let exhausted_iterations = descriptor
            .max_iterations
            .is_some_and(|limit| instance.next_iteration() >= limit);
        let exhausted_deadline = instance
            .deadline_at_ms
            .is_some_and(|deadline| facts.observed_time_ms() >= deadline);
        if exhausted_iterations || exhausted_deadline {
            let failure = TaskFailureFact::new(
                WorkerFailureClass::ControlTermination,
                if exhausted_iterations {
                    "LOOP_MAX_ITERATIONS"
                } else {
                    "LOOP_DEADLINE_EXCEEDED"
                },
                None,
            )?;
            return self.propagate_failure(
                facts,
                ids,
                index,
                node,
                &scope_instance_id,
                &activation_id,
                &occurrence,
                context,
                failure,
            );
        }
        let dynamic_marker = match descriptor.flavor {
            crate::engine::plan::LoopFlavor::Workflow => "loop_iteration",
            crate::engine::plan::LoopFlavor::Agent => "agent_loop_turn",
        };
        let base = occurrence.child(format!("{dynamic_marker}:{}", instance.next_iteration()))?;
        let route = route_for_output(index, &descriptor.body_output)?;
        let body_occurrence = base.child(format!("edge:{}", route.edge_id))?;
        let target = index
            .node(&route.node_id)
            .ok_or_else(|| graph("Loop body route target disappeared"))?;
        let child_static_scope = self.runtime_owning_scope(index, target)?.id().clone();
        let child_scope = self.scope_instance(ids, index, target, &body_occurrence)?;
        let child_activation = ids
            .activation(target.id(), &child_scope, &body_occurrence)
            .map_err(|error| id_error(error, "Loop iteration activation"))?;
        let token_id = ids
            .control_token(
                node.id(),
                &scope_instance_id,
                &base,
                &descriptor.body_output,
            )
            .map_err(|error| id_error(error, "Loop iteration token"))?;
        let iteration = LoopIterationFact::new(
            iteration_key,
            descriptor.flavor,
            body_occurrence,
            child_scope,
            child_static_scope,
            target.id().clone(),
            child_activation,
            token_id,
            instance.state().clone(),
        )?;
        let checkpoint = ids.checkpoint(
            node.id(),
            &scope_instance_id,
            &occurrence,
            &format!("start_iteration:{}", instance.next_iteration()),
        );
        self.action(
            facts,
            checkpoint,
            SchedulerAction::StartLoopIteration {
                iteration,
                state_port: state_port.clone(),
                output_port: descriptor.body_output.clone(),
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_collect(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        descriptor: &CollectDescriptor,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        occurrence: LogicalOccurrence,
        mut context: PathContext,
    ) -> Result<SchedulerDecision, SchedulerError> {
        let collect_context = context.collect.take().ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_GRAPH_INVALID,
                "Collect was reached without its correlated structural instance",
            )
        })?;
        let value = match (&descriptor.source, collect_context) {
            (
                CollectSource::StaticFork {
                    fork_node_id, mode, ..
                },
                CollectContext::StaticFork(group_id),
            ) => {
                let fork = index
                    .node(fork_node_id)
                    .ok_or_else(|| graph("Collect Fork disappeared"))?;
                let NodeKind::Fork(fork) = fork.kind() else {
                    return Err(graph("Static Collect source is not a Fork"));
                };
                let mut object = JsonMap::new();
                for leg in &fork.legs {
                    let outcome = facts
                        .fork_settlements()
                        .get(&ForkLegKey::new(group_id.clone(), leg.leg_id.clone()))
                        .ok_or_else(|| {
                            SchedulerError::new(
                                SCHEDULER_FACT_MISSING,
                                "Collect is missing a Fork leg settlement",
                            )
                        })?;
                    let value = match (mode, outcome) {
                        (PlanJoinMode::AllSuccess, StructuralOutcomeFact::Succeeded { value }) => {
                            value.value().clone()
                        }
                        (PlanJoinMode::AllSettled, StructuralOutcomeFact::Succeeded { value }) => {
                            serde_json::json!({"kind": "ok", "value": value.value()})
                        }
                        (PlanJoinMode::AllSettled, StructuralOutcomeFact::Failed { failure })
                            if failure.class() == WorkerFailureClass::SafeBusinessFailure =>
                        {
                            serde_json::json!({
                                "kind": "error",
                                "error": failure.safe_error().expect("safe failure invariant").value()
                            })
                        }
                        _ => return Err(graph("fatal Fork failure reached Collect")),
                    };
                    object.insert(leg.leg_id.to_string(), value);
                }
                RuntimeValue::new(Value::Object(object))?
            }
            (
                CollectSource::Map { .. } | CollectSource::DynamicMap { .. },
                CollectContext::Map(map_activation),
            ) => {
                let map = facts.map_instances().get(&map_activation).ok_or_else(|| {
                    SchedulerError::new(SCHEDULER_FACT_MISSING, "Map Collect has no input snapshot")
                })?;
                let mut values = Vec::with_capacity(map.items().len());
                for seed in map.items() {
                    let outcome = facts
                        .map_settlements()
                        .get(&MapItemKey::new(
                            map_activation.clone(),
                            seed.identity().clone(),
                        ))
                        .ok_or_else(|| {
                            SchedulerError::new(
                                SCHEDULER_FACT_MISSING,
                                "Map Collect has an unsettled input item",
                            )
                        })?;
                    values.push(
                        outcome
                            .value()
                            .ok_or_else(|| graph("failed Map item reached Collect"))?
                            .value()
                            .clone(),
                    );
                }
                RuntimeValue::new(Value::Array(values))?
            }
            (CollectSource::Loop { .. }, CollectContext::Loop(loop_activation)) => facts
                .loop_instances()
                .get(&loop_activation)
                .filter(|instance| instance.completed())
                .map(|instance| instance.state().clone())
                .ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_FACT_MISSING,
                        "Loop Collect has no completed final state",
                    )
                })?,
            _ => return Err(graph("Collect context does not match its declared source")),
        };
        let port = index
            .data_port(&descriptor.output_port)
            .ok_or_else(|| graph("Collect output port disappeared"))?;
        require_type(&value, port.value_type(), "Collect output")?;
        let checkpoint = ids.checkpoint(
            node.id(),
            &scope_instance_id,
            &occurrence,
            "commit_collect_output",
        );
        if !facts.checkpoints().contains(&checkpoint) {
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::CommitNativeOutput {
                    activation_id: activation_id.clone(),
                    node_id: node.id().clone(),
                    occurrence: occurrence.clone(),
                    output: NativeOutput::Values {
                        values: BTreeMap::from([(descriptor.output_port.clone(), value)]),
                    },
                },
            );
        }
        require_projection_fact(
            facts.value_at(&descriptor.output_port, &occurrence) == Some(&value),
            "Collect checkpoint does not match its committed typed output",
        )?;
        let output = only_control_output(index, node)?;
        match self.emit_or_advance(
            facts,
            ids,
            index,
            node,
            &scope_instance_id,
            &activation_id,
            &occurrence,
            &output,
        )? {
            OutputProgress::Action(action) => Ok(action),
            OutputProgress::Next {
                node_id,
                occurrence,
                inbound,
            } => self.plan_path(facts, ids, node_id, occurrence, Some(inbound), context),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_error_boundary(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        descriptor: &ErrorBoundaryDescriptor,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        occurrence: LogicalOccurrence,
        context: PathContext,
    ) -> Result<SchedulerDecision, SchedulerError> {
        let initial = ErrorBoundaryFact::new(
            activation_id.clone(),
            node.id().clone(),
            occurrence.clone(),
            ErrorBoundaryPhase::Protected,
            None,
        )?;
        let checkpoint = ids.checkpoint(
            node.id(),
            &scope_instance_id,
            &occurrence,
            "open_error_boundary",
        );
        if !facts.checkpoints().contains(&checkpoint) {
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::OpenErrorBoundary { boundary: initial },
            );
        }
        let state = facts.boundary_states().get(&activation_id).ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "ErrorBoundary checkpoint has no boundary state fact",
            )
        })?;
        if let Some(reason) = facts.run_termination_reason() {
            match state.phase() {
                ErrorBoundaryPhase::Protected | ErrorBoundaryPhase::Handler => {
                    if descriptor.finalizer_output.is_some() {
                        let checkpoint = ids.checkpoint(
                            node.id(),
                            &scope_instance_id,
                            &occurrence,
                            "begin_termination_finalizer",
                        );
                        let boundary = ErrorBoundaryFact::with_exit(
                            activation_id.clone(),
                            node.id().clone(),
                            occurrence.clone(),
                            ErrorBoundaryPhase::Finalizer,
                            None,
                            ErrorBoundaryExit::Terminate { reason },
                        )?;
                        return self.action(
                            facts,
                            checkpoint,
                            SchedulerAction::TransitionErrorBoundary { boundary },
                        );
                    }
                    return self.propagate_termination(
                        facts,
                        ids,
                        index,
                        node,
                        &scope_instance_id,
                        &activation_id,
                        &occurrence,
                        context,
                        reason,
                    );
                }
                ErrorBoundaryPhase::Completed => {
                    return self.propagate_termination(
                        facts,
                        ids,
                        index,
                        node,
                        &scope_instance_id,
                        &activation_id,
                        &occurrence,
                        context,
                        reason,
                    );
                }
                ErrorBoundaryPhase::Finalizer => {}
            }
        }
        let (output, phase) = match state.phase() {
            ErrorBoundaryPhase::Protected => {
                (&descriptor.protected_output, ErrorBoundaryPhase::Protected)
            }
            ErrorBoundaryPhase::Handler => {
                let error = state.safe_error().ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_FACT_INCONSISTENT,
                        "handling ErrorBoundary has no safe error",
                    )
                })?;
                if facts.value_at(&descriptor.error_port, &occurrence) != Some(error) {
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        &scope_instance_id,
                        &occurrence,
                        "commit_caught_error",
                    );
                    return self.action(
                        facts,
                        checkpoint,
                        SchedulerAction::CommitOccurrenceValues {
                            activation_id: activation_id.clone(),
                            node_id: node.id().clone(),
                            occurrence: occurrence.clone(),
                            values: BTreeMap::from([(
                                descriptor.error_port.clone(),
                                error.clone(),
                            )]),
                        },
                    );
                }
                (&descriptor.handler_output, ErrorBoundaryPhase::Handler)
            }
            ErrorBoundaryPhase::Finalizer => (
                descriptor.finalizer_output.as_ref().ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_FACT_INCONSISTENT,
                        "finalizing ErrorBoundary has no finalizer output",
                    )
                })?,
                ErrorBoundaryPhase::Finalizer,
            ),
            ErrorBoundaryPhase::Completed => {
                match state.exit() {
                    ErrorBoundaryExit::Rethrow { failure } => {
                        return self.propagate_failure(
                            facts,
                            ids,
                            index,
                            node,
                            &scope_instance_id,
                            &activation_id,
                            &occurrence,
                            context,
                            failure.clone(),
                        )
                    }
                    ErrorBoundaryExit::Terminate { reason } => {
                        return self.propagate_termination(
                            facts,
                            ids,
                            index,
                            node,
                            &scope_instance_id,
                            &activation_id,
                            &occurrence,
                            context,
                            *reason,
                        )
                    }
                    ErrorBoundaryExit::Return {
                        activation_id: return_activation_id,
                        output,
                    } => {
                        return self.propagate_return(
                            facts,
                            ids,
                            index,
                            node,
                            &scope_instance_id,
                            return_activation_id,
                            &occurrence,
                            context,
                            output.clone(),
                        )
                    }
                    ErrorBoundaryExit::Continue => {}
                }
                let completed_output = descriptor.completed_output.as_ref().ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_FACT_INCONSISTENT,
                        "normally completed ErrorBoundary has no completed output",
                    )
                })?;
                match self.emit_or_advance(
                    facts,
                    ids,
                    index,
                    node,
                    &scope_instance_id,
                    &activation_id,
                    &occurrence,
                    completed_output,
                )? {
                    OutputProgress::Action(action) => return Ok(action),
                    OutputProgress::Next {
                        node_id,
                        occurrence,
                        inbound,
                    } => {
                        return self.plan_path(
                            facts,
                            ids,
                            node_id,
                            occurrence,
                            Some(inbound),
                            context,
                        )
                    }
                }
            }
        };
        match self.emit_or_advance(
            facts,
            ids,
            index,
            node,
            &scope_instance_id,
            &activation_id,
            &occurrence,
            output,
        )? {
            OutputProgress::Action(action) => Ok(action),
            OutputProgress::Next {
                node_id,
                occurrence,
                inbound,
            } => {
                let mut child = context;
                child.frames.push(StructuralFrame::ErrorBoundary {
                    boundary_node_id: node.id().clone(),
                    boundary_activation_id: activation_id,
                    boundary_occurrence: occurrence.clone(),
                    phase,
                });
                self.plan_path(facts, ids, node_id, occurrence, Some(inbound), child)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_signal_wait(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        descriptor: &crate::engine::plan::WaitSignalDescriptor,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        occurrence: LogicalOccurrence,
        context: PathContext,
        human_task: Option<&crate::engine::plan::HumanTaskDescriptor>,
    ) -> Result<SchedulerDecision, SchedulerError> {
        let wait_id = ids.wait(node.id(), &scope_instance_id, &occurrence);
        let signal_id = ids
            .signal(
                node.id(),
                &scope_instance_id,
                &occurrence,
                &descriptor.signal_name,
            )
            .map_err(|error| id_error(error, "signal"))?;
        let timeout_ms = frozen_wait_timeout_ms(index, node)?;
        let timer_id = timeout_ms
            .map(|_| ids.timer(node.id(), &scope_instance_id, &occurrence, "wait_timeout"))
            .transpose()
            .map_err(|error| id_error(error, "wait timeout timer"))?;
        let checkpoint =
            ids.checkpoint(node.id(), &scope_instance_id, &occurrence, "register_wait");
        let due_at = if facts.checkpoints().contains(&checkpoint) {
            facts
                .waits()
                .get(&wait_id)
                .ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_FACT_INCONSISTENT,
                        "wait checkpoint has no durable registration",
                    )
                })?
                .due_at_ms()
        } else {
            timeout_ms
                .map(|delay| {
                    facts
                        .observed_time_ms()
                        .checked_add(delay)
                        .ok_or_else(|| graph("wait timeout overflowed u64"))
                })
                .transpose()?
        };
        let expected = WaitRegistrationFact::new(
            wait_id.clone(),
            activation_id.clone(),
            node.id().clone(),
            occurrence.clone(),
            Some(descriptor.signal_name.clone()),
            Some(signal_id),
            timer_id,
            due_at,
            Some(descriptor.payload_type.clone()),
        )?;
        let expected = match human_task {
            Some(descriptor) => {
                let request = DataResolver::for_occurrence(index, facts, &occurrence)
                    .resolve_input(&descriptor.request_input, node)?;
                require_type(&request, &descriptor.request_type, "HumanTask request")?;
                expected.with_human_task(super::HumanTaskWaitFact::new(
                    descriptor.assignees.clone(),
                    descriptor.candidate_groups.clone(),
                    descriptor.claim_lease_ms,
                    request,
                )?)?
            }
            None => expected,
        };
        if !facts.checkpoints().contains(&checkpoint) {
            if let Some(reason) = termination_outside_finalizer(facts, &context) {
                return self.propagate_termination(
                    facts,
                    ids,
                    index,
                    node,
                    &scope_instance_id,
                    &activation_id,
                    &occurrence,
                    context,
                    reason,
                );
            }
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::RegisterWait {
                    registration: expected,
                },
            );
        }
        require_projection_fact(
            facts.waits().get(&wait_id) == Some(&expected),
            "wait checkpoint does not match its durable registration",
        )?;
        let Some(resolution) = facts.wait_resolutions().get(&wait_id) else {
            if let Some(reason) = termination_outside_finalizer(facts, &context) {
                return self.propagate_termination(
                    facts,
                    ids,
                    index,
                    node,
                    &scope_instance_id,
                    &activation_id,
                    &occurrence,
                    context,
                    reason,
                );
            }
            return Ok(SchedulerDecision::Quiescent(
                SchedulerQuiescence::WaitingForWait {
                    wait_id,
                    activation_id,
                },
            ));
        };
        match resolution.subject() {
            WaitSubjectFact::Timer { .. } => {
                let failure = TaskFailureFact::new(
                    WorkerFailureClass::ControlTermination,
                    "WAIT_TIMEOUT",
                    None,
                )?;
                self.propagate_failure(
                    facts,
                    ids,
                    index,
                    node,
                    &scope_instance_id,
                    &activation_id,
                    &occurrence,
                    context,
                    failure,
                )
            }
            WaitSubjectFact::Signal { .. } => {
                let payload = resolution
                    .payload()
                    .expect("signal resolution invariant")
                    .clone();
                let output = index
                    .data_outputs(node.id())
                    .first()
                    .cloned()
                    .ok_or_else(|| graph("WaitSignal has no payload output"))?;
                let checkpoint = ids.checkpoint(
                    node.id(),
                    &scope_instance_id,
                    &occurrence,
                    "commit_wait_payload",
                );
                if !facts.checkpoints().contains(&checkpoint) {
                    return self.action(
                        facts,
                        checkpoint,
                        SchedulerAction::CommitOccurrenceValues {
                            activation_id: activation_id.clone(),
                            node_id: node.id().clone(),
                            occurrence: occurrence.clone(),
                            values: BTreeMap::from([(output.clone(), payload)]),
                        },
                    );
                }
                let control = only_control_output(index, node)?;
                self.follow_output(
                    facts,
                    ids,
                    index,
                    node,
                    scope_instance_id,
                    activation_id,
                    occurrence,
                    control,
                    context,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_timer(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        descriptor: &crate::engine::plan::TimerDescriptor,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        occurrence: LogicalOccurrence,
        context: PathContext,
    ) -> Result<SchedulerDecision, SchedulerError> {
        let delay = DataResolver::for_occurrence(index, facts, &occurrence)
            .evaluate_expression(&descriptor.delay_ms, node)?;
        let delay = delay.value().as_u64().ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_EXPRESSION_INVALID,
                "Timer delay must evaluate to a non-negative integer millisecond count",
            )
        })?;
        let wait_id = ids.wait(node.id(), &scope_instance_id, &occurrence);
        let timer_id = ids
            .timer(node.id(), &scope_instance_id, &occurrence, "timer")
            .map_err(|error| id_error(error, "timer"))?;
        let checkpoint =
            ids.checkpoint(node.id(), &scope_instance_id, &occurrence, "register_timer");
        let due_at = if facts.checkpoints().contains(&checkpoint) {
            facts
                .waits()
                .get(&wait_id)
                .ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_FACT_INCONSISTENT,
                        "Timer checkpoint has no durable registration",
                    )
                })?
                .due_at_ms()
                .ok_or_else(|| {
                    SchedulerError::new(
                        SCHEDULER_FACT_INCONSISTENT,
                        "Timer registration has no durable deadline",
                    )
                })?
        } else {
            facts
                .observed_time_ms()
                .checked_add(delay)
                .ok_or_else(|| graph("Timer due time overflowed u64"))?
        };
        let expected = WaitRegistrationFact::new(
            wait_id.clone(),
            activation_id.clone(),
            node.id().clone(),
            occurrence.clone(),
            None,
            None,
            Some(timer_id),
            Some(due_at),
            None,
        )?;
        if !facts.checkpoints().contains(&checkpoint) {
            if let Some(reason) = termination_outside_finalizer(facts, &context) {
                return self.propagate_termination(
                    facts,
                    ids,
                    index,
                    node,
                    &scope_instance_id,
                    &activation_id,
                    &occurrence,
                    context,
                    reason,
                );
            }
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::RegisterWait {
                    registration: expected,
                },
            );
        }
        require_projection_fact(
            facts.waits().get(&wait_id) == Some(&expected),
            "Timer checkpoint does not match its durable registration",
        )?;
        if facts.wait_resolutions().get(&wait_id).is_none() {
            if let Some(reason) = termination_outside_finalizer(facts, &context) {
                return self.propagate_termination(
                    facts,
                    ids,
                    index,
                    node,
                    &scope_instance_id,
                    &activation_id,
                    &occurrence,
                    context,
                    reason,
                );
            }
            return Ok(SchedulerDecision::Quiescent(
                SchedulerQuiescence::WaitingForWait {
                    wait_id,
                    activation_id,
                },
            ));
        }
        let control = only_control_output(index, node)?;
        self.follow_output(
            facts,
            ids,
            index,
            node,
            scope_instance_id,
            activation_id,
            occurrence,
            control,
            context,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_subflow(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        descriptor: &crate::engine::plan::SubflowCallDescriptor,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        occurrence: LogicalOccurrence,
        context: PathContext,
    ) -> Result<SchedulerDecision, SchedulerError> {
        let invocation = derive_subflow_invocation(index, facts, node, descriptor, &occurrence)?;
        require_projection_fact(
            invocation.parent_scope_instance_id() == &scope_instance_id
                && invocation.parent_activation_id() == &activation_id,
            "Subflow planner context disagrees with its deterministic invocation identity",
        )?;
        let child_run_id = invocation.child_run_id().clone();
        let checkpoint =
            ids.checkpoint(node.id(), &scope_instance_id, &occurrence, "start_subflow");
        if !facts.checkpoints().contains(&checkpoint) {
            let subflow = self.linked.subflow(node.id()).ok_or_else(|| {
                SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    "linked Subflow deployment contract disappeared",
                )
            })?;
            let derived = derive_subflow_admission(
                index,
                facts,
                node,
                descriptor,
                &occurrence,
                subflow.input_contract(),
            )?;
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::StartSubflow {
                    invocation,
                    execution_revision: subflow.execution_revision().clone(),
                    interface_version: descriptor.interface_version.clone(),
                    timeout_ms: descriptor.timeout_ms,
                    run_input: derived.run_input,
                    outputs: derived.outputs,
                },
            );
        }
        require_projection_fact(
            facts.subflows().get(&child_run_id) == Some(&invocation),
            "Subflow checkpoint does not match its child Run identity",
        )?;
        let Some(observed_outcome) = facts.child_subflow_outcomes().get(&child_run_id) else {
            return Ok(SchedulerDecision::Quiescent(
                SchedulerQuiescence::WaitingForChildRun {
                    child_run_id,
                    activation_id,
                },
            ));
        };
        let settle_checkpoint =
            ids.checkpoint(node.id(), &scope_instance_id, &occurrence, "settle_subflow");
        if !facts.checkpoints().contains(&settle_checkpoint) {
            return self.action(
                facts,
                settle_checkpoint,
                SchedulerAction::SettleSubflow {
                    invocation: invocation.clone(),
                    outcome: observed_outcome.clone(),
                },
            );
        }
        let outcome = facts.settled_subflows().get(&child_run_id).ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_FACT_MISSING,
                "Subflow settlement checkpoint has no durable invocation settlement",
            )
        })?;
        require_projection_fact(
            outcome == observed_outcome,
            "settled Subflow outcome differs from the observed child terminal outcome",
        )?;
        match outcome {
            SubflowOutcomeFact::Failed { failure } => self.propagate_failure(
                facts,
                ids,
                index,
                node,
                &scope_instance_id,
                &activation_id,
                &occurrence,
                context,
                failure.clone(),
            ),
            SubflowOutcomeFact::Cancelled => {
                if let Some(reason) = facts.run_termination_reason() {
                    self.propagate_termination(
                        facts,
                        ids,
                        index,
                        node,
                        &scope_instance_id,
                        &activation_id,
                        &occurrence,
                        context,
                        reason,
                    )
                } else {
                    self.propagate_failure(
                        facts,
                        ids,
                        index,
                        node,
                        &scope_instance_id,
                        &activation_id,
                        &occurrence,
                        context,
                        TaskFailureFact::new(
                            WorkerFailureClass::ControlTermination,
                            "CHILD_RUN_CANCELLED",
                            None,
                        )?,
                    )
                }
            }
            SubflowOutcomeFact::Succeeded { outputs } => {
                self.validate_outputs(index, node, outputs)?;
                for (port, expected) in outputs {
                    require_projection_fact(
                        facts.value_at(port, &occurrence) == Some(expected),
                        "Subflow settlement did not atomically commit its named output",
                    )?;
                }
                let control = only_control_output(index, node)?;
                self.follow_output(
                    facts,
                    ids,
                    index,
                    node,
                    scope_instance_id,
                    activation_id,
                    occurrence,
                    control,
                    context,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn propagate_failure(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        scope_instance_id: &ScopeInstanceId,
        activation_id: &ActivationId,
        occurrence: &LogicalOccurrence,
        mut context: PathContext,
        failure: TaskFailureFact,
    ) -> Result<SchedulerDecision, SchedulerError> {
        while let Some(frame) = context.frames.pop() {
            match frame {
                StructuralFrame::ForkLeg { key, .. } => {
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        scope_instance_id,
                        occurrence,
                        &format!("fail_fork_leg:{}", key.leg_id()),
                    );
                    let leg = facts.fork_legs().get(&key).cloned().ok_or_else(|| {
                        SchedulerError::new(
                            SCHEDULER_FACT_MISSING,
                            "failed Fork settlement has no admitted leg fact",
                        )
                    })?;
                    return self.action(
                        facts,
                        checkpoint,
                        SchedulerAction::SettleForkLeg {
                            leg,
                            outcome: StructuralOutcomeFact::Failed { failure },
                        },
                    );
                }
                StructuralFrame::MapItem { key, .. } => {
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        scope_instance_id,
                        occurrence,
                        &format!("fail_map_item:{}", key.stable_dynamic_key()),
                    );
                    let item = facts.map_items().get(&key).cloned().ok_or_else(|| {
                        SchedulerError::new(
                            SCHEDULER_FACT_MISSING,
                            "failed Map settlement has no admitted item fact",
                        )
                    })?;
                    return self.action(
                        facts,
                        checkpoint,
                        SchedulerAction::SettleMapItem {
                            item,
                            outcome: StructuralOutcomeFact::Failed { failure },
                        },
                    );
                }
                StructuralFrame::LoopIteration { key, .. } => {
                    let outcome = StructuralOutcomeFact::Failed {
                        failure: failure.clone(),
                    };
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        scope_instance_id,
                        occurrence,
                        &format!("fail_loop_iteration:{}", key.iteration()),
                    );
                    if !facts.checkpoints().contains(&checkpoint) {
                        let iteration =
                            facts.loop_iterations().get(&key).cloned().ok_or_else(|| {
                                SchedulerError::new(
                                    SCHEDULER_FACT_MISSING,
                                    "failed Loop settlement has no admitted iteration fact",
                                )
                            })?;
                        return self.action(
                            facts,
                            checkpoint,
                            SchedulerAction::SettleLoopIteration { iteration, outcome },
                        );
                    }
                    require_projection_fact(
                        facts.loop_settlements().get(&key) == Some(&outcome),
                        "Loop failure checkpoint has no matching durable scope settlement",
                    )?;
                }
                StructuralFrame::ErrorBoundary {
                    boundary_node_id,
                    boundary_activation_id,
                    boundary_occurrence,
                    phase,
                } if phase == ErrorBoundaryPhase::Protected
                    && failure.class() == WorkerFailureClass::SafeBusinessFailure =>
                {
                    let safe_error = failure
                        .safe_error()
                        .expect("safe failure invariant")
                        .clone();
                    let boundary = ErrorBoundaryFact::new(
                        boundary_activation_id,
                        boundary_node_id,
                        boundary_occurrence,
                        ErrorBoundaryPhase::Handler,
                        Some(safe_error),
                    )?;
                    let checkpoint = ids.checkpoint(
                        node.id(),
                        scope_instance_id,
                        occurrence,
                        "catch_safe_business_failure",
                    );
                    return self.action(
                        facts,
                        checkpoint,
                        SchedulerAction::TransitionErrorBoundary { boundary },
                    );
                }
                StructuralFrame::ErrorBoundary {
                    boundary_node_id,
                    boundary_activation_id,
                    boundary_occurrence,
                    phase,
                } => {
                    let boundary_node = index.node(&boundary_node_id).ok_or_else(|| {
                        SchedulerError::new(
                            SCHEDULER_GRAPH_INVALID,
                            "failure unwind references a missing ErrorBoundary",
                        )
                    })?;
                    let NodeKind::ErrorBoundary(descriptor) = boundary_node.kind() else {
                        return Err(SchedulerError::new(
                            SCHEDULER_GRAPH_INVALID,
                            "failure unwind frame no longer references an ErrorBoundary",
                        ));
                    };
                    if phase == ErrorBoundaryPhase::Finalizer {
                        // A finalizer failure deterministically overrides an
                        // earlier business/infrastructure failure. A durable
                        // control termination, when present, is handled by the
                        // first-winner Run intent before this completed state.
                        let completed = ErrorBoundaryFact::with_exit(
                            boundary_activation_id,
                            boundary_node_id,
                            boundary_occurrence,
                            ErrorBoundaryPhase::Completed,
                            None,
                            ErrorBoundaryExit::Rethrow {
                                failure: failure.clone(),
                            },
                        )?;
                        let checkpoint = ids.checkpoint(
                            node.id(),
                            scope_instance_id,
                            occurrence,
                            "fail_finalizer",
                        );
                        return self.action(
                            facts,
                            checkpoint,
                            SchedulerAction::TransitionErrorBoundary {
                                boundary: completed,
                            },
                        );
                    }
                    if matches!(
                        phase,
                        ErrorBoundaryPhase::Protected | ErrorBoundaryPhase::Handler
                    ) && descriptor.finalizer_output.is_some()
                    {
                        let finalizing = ErrorBoundaryFact::with_exit(
                            boundary_activation_id,
                            boundary_node_id,
                            boundary_occurrence,
                            ErrorBoundaryPhase::Finalizer,
                            None,
                            ErrorBoundaryExit::Rethrow {
                                failure: failure.clone(),
                            },
                        )?;
                        let checkpoint = ids.checkpoint(
                            node.id(),
                            scope_instance_id,
                            occurrence,
                            "unwind_failure_finalizer",
                        );
                        return self.action(
                            facts,
                            checkpoint,
                            SchedulerAction::TransitionErrorBoundary {
                                boundary: finalizing,
                            },
                        );
                    }
                    if matches!(
                        phase,
                        ErrorBoundaryPhase::Protected | ErrorBoundaryPhase::Handler
                    ) {
                        let completed = ErrorBoundaryFact::with_exit(
                            boundary_activation_id,
                            boundary_node_id,
                            boundary_occurrence,
                            ErrorBoundaryPhase::Completed,
                            None,
                            ErrorBoundaryExit::Rethrow {
                                failure: failure.clone(),
                            },
                        )?;
                        let checkpoint = ids.checkpoint(
                            node.id(),
                            scope_instance_id,
                            occurrence,
                            "complete_failure_boundary",
                        );
                        return self.action(
                            facts,
                            checkpoint,
                            SchedulerAction::TransitionErrorBoundary {
                                boundary: completed,
                            },
                        );
                    }
                }
            }
        }

        if failure.class() == WorkerFailureClass::SafeBusinessFailure {
            let error = failure
                .typed_safe_error()
                .expect("safe failure invariant")
                .clone();
            require_type(
                error.runtime_value(),
                index.metadata().error_type(),
                "Run safe error",
            )?;
            let checkpoint =
                ids.checkpoint(node.id(), scope_instance_id, occurrence, "fail_run_safe");
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::FailRun {
                    activation_id: activation_id.clone(),
                    error,
                },
            );
        }
        let checkpoint = ids.checkpoint(
            node.id(),
            scope_instance_id,
            occurrence,
            if failure.code().starts_with("LOOP_") {
                SCHEDULER_LOOP_BUDGET_EXCEEDED
            } else {
                "fail_run_internal"
            },
        );
        self.action(
            facts,
            checkpoint,
            SchedulerAction::FailRunInternal {
                activation_id: activation_id.clone(),
                failure,
            },
        )
    }

    /// Unwinds an authored Return through every enclosing durable finalizer.
    /// Catch is skipped. A Return or Raise executed by the finalizer replaces
    /// the pending business exit; control-termination first-winner semantics
    /// remain authoritative in `plan_error_boundary`.
    #[allow(clippy::too_many_arguments)]
    fn propagate_return(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        scope_instance_id: &ScopeInstanceId,
        return_activation_id: &ActivationId,
        occurrence: &LogicalOccurrence,
        mut context: PathContext,
        output: RuntimeValue,
    ) -> Result<SchedulerDecision, SchedulerError> {
        require_type(&output, index.metadata().output_type(), "Run output")?;
        while let Some(frame) = context.frames.pop() {
            let StructuralFrame::ErrorBoundary {
                boundary_node_id,
                boundary_activation_id,
                boundary_occurrence,
                phase,
            } = frame
            else {
                continue;
            };
            let boundary_node = index.node(&boundary_node_id).ok_or_else(|| {
                SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    "return unwind references a missing ErrorBoundary",
                )
            })?;
            let NodeKind::ErrorBoundary(descriptor) = boundary_node.kind() else {
                return Err(SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    "return unwind frame no longer references an ErrorBoundary",
                ));
            };
            let (next_phase, checkpoint_label) = match phase {
                ErrorBoundaryPhase::Finalizer => {
                    (ErrorBoundaryPhase::Completed, "return_from_finalizer")
                }
                ErrorBoundaryPhase::Protected | ErrorBoundaryPhase::Handler
                    if descriptor.finalizer_output.is_some() =>
                {
                    (ErrorBoundaryPhase::Finalizer, "unwind_return_finalizer")
                }
                ErrorBoundaryPhase::Protected | ErrorBoundaryPhase::Handler => {
                    (ErrorBoundaryPhase::Completed, "complete_return_boundary")
                }
                ErrorBoundaryPhase::Completed => continue,
            };
            let boundary = ErrorBoundaryFact::with_exit(
                boundary_activation_id,
                boundary_node_id,
                boundary_occurrence,
                next_phase,
                None,
                ErrorBoundaryExit::Return {
                    activation_id: return_activation_id.clone(),
                    output: output.clone(),
                },
            )?;
            let checkpoint =
                ids.checkpoint(node.id(), scope_instance_id, occurrence, checkpoint_label);
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::TransitionErrorBoundary { boundary },
            );
        }

        let checkpoint = ids.checkpoint(node.id(), scope_instance_id, occurrence, "complete_run");
        self.action(
            facts,
            checkpoint,
            SchedulerAction::CompleteRun {
                activation_id: return_activation_id.clone(),
                output,
            },
        )
    }

    /// Unwinds a durable control termination through authored finalizers.
    /// Catch is intentionally skipped: cancellation/deadline/interruption are
    /// not business failures. Once all enclosing boundary frames have been
    /// discharged, the repository commits the terminal matching the original
    /// first-winner reason.
    #[allow(clippy::too_many_arguments)]
    fn propagate_termination(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        scope_instance_id: &ScopeInstanceId,
        activation_id: &ActivationId,
        occurrence: &LogicalOccurrence,
        mut context: PathContext,
        reason: TerminationReason,
    ) -> Result<SchedulerDecision, SchedulerError> {
        while let Some(frame) = context.frames.pop() {
            let StructuralFrame::ErrorBoundary {
                boundary_node_id,
                boundary_activation_id,
                boundary_occurrence,
                phase,
            } = frame
            else {
                continue;
            };
            if phase == ErrorBoundaryPhase::Finalizer {
                continue;
            }
            let boundary_node = index.node(&boundary_node_id).ok_or_else(|| {
                SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    "termination unwind references a missing ErrorBoundary",
                )
            })?;
            let NodeKind::ErrorBoundary(descriptor) = boundary_node.kind() else {
                return Err(SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    "termination unwind frame no longer references an ErrorBoundary",
                ));
            };
            if !matches!(
                phase,
                ErrorBoundaryPhase::Protected | ErrorBoundaryPhase::Handler
            ) {
                continue;
            }
            let checkpoint = ids.checkpoint(
                node.id(),
                scope_instance_id,
                occurrence,
                if descriptor.finalizer_output.is_some() {
                    "unwind_termination_finalizer"
                } else {
                    "complete_termination_boundary"
                },
            );
            let next_phase = if descriptor.finalizer_output.is_some() {
                ErrorBoundaryPhase::Finalizer
            } else {
                ErrorBoundaryPhase::Completed
            };
            let boundary = ErrorBoundaryFact::with_exit(
                boundary_activation_id,
                boundary_node_id,
                boundary_occurrence,
                next_phase,
                None,
                ErrorBoundaryExit::Terminate { reason },
            )?;
            return self.action(
                facts,
                checkpoint,
                SchedulerAction::TransitionErrorBoundary { boundary },
            );
        }

        let checkpoint = ids.checkpoint(
            node.id(),
            scope_instance_id,
            occurrence,
            match reason {
                TerminationReason::Cancelled => "cancel_run",
                TerminationReason::TimedOut => "timeout_run",
                TerminationReason::Interrupted => "interrupt_run",
                TerminationReason::Failure => "terminate_failed_run",
            },
        );
        self.action(
            facts,
            checkpoint,
            SchedulerAction::CancelRun {
                activation_id: activation_id.clone(),
                reason,
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn follow_output(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        occurrence: LogicalOccurrence,
        output: ControlPortId,
        context: PathContext,
    ) -> Result<SchedulerDecision, SchedulerError> {
        match self.emit_or_advance(
            facts,
            ids,
            index,
            node,
            &scope_instance_id,
            &activation_id,
            &occurrence,
            &output,
        )? {
            OutputProgress::Action(action) => Ok(action),
            OutputProgress::Next {
                node_id,
                occurrence,
                inbound,
            } => self.plan_path(facts, ids, node_id, occurrence, Some(inbound), context),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_or_advance(
        &self,
        facts: &SchedulerFacts,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        scope_instance_id: &ScopeInstanceId,
        activation_id: &ActivationId,
        occurrence: &LogicalOccurrence,
        output_port: &ControlPortId,
    ) -> Result<OutputProgress, SchedulerError> {
        let token_id = ids
            .control_token(node.id(), scope_instance_id, occurrence, output_port)
            .map_err(|error| id_error(error, "control token"))?;
        let checkpoint = ids.checkpoint(
            node.id(),
            scope_instance_id,
            occurrence,
            &format!("emit_token:{}", output_port.as_str()),
        );
        if !facts.checkpoints().contains(&checkpoint) {
            return self
                .action(
                    facts,
                    checkpoint,
                    SchedulerAction::EmitToken {
                        token_id,
                        source_activation_id: activation_id.clone(),
                        output_port: output_port.clone(),
                        scope_instance_id: scope_instance_id.clone(),
                    },
                )
                .map(OutputProgress::Action);
        }
        require_projection_fact(
            facts.emitted_tokens().contains(&token_id),
            "emit checkpoint has no emitted-token fact",
        )?;
        let route = route_for_output(index, output_port)?;
        let next_occurrence = occurrence.child(format!("edge:{}", route.edge_id))?;
        Ok(OutputProgress::Next {
            node_id: route.node_id,
            occurrence: next_occurrence,
            inbound: InboundControl {
                token_id,
                input_port: route.input_port,
            },
        })
    }

    fn output_contracts(
        &self,
        index: &PlanIndex<'plan>,
        node: &Node,
    ) -> Result<Vec<TaskOutputContract>, SchedulerError> {
        index
            .data_outputs(node.id())
            .iter()
            .map(|output_id| {
                let port = index
                    .data_port(output_id)
                    .ok_or_else(|| graph("node output disappeared from PlanIndex"))?;
                Ok(TaskOutputContract::new(
                    output_id.clone(),
                    port.name().clone(),
                    port.value_type().clone(),
                    port.required(),
                ))
            })
            .collect()
    }

    fn validate_outputs(
        &self,
        index: &PlanIndex<'plan>,
        node: &Node,
        outputs: &BTreeMap<DataPortId, RuntimeValue>,
    ) -> Result<(), SchedulerError> {
        if outputs
            .keys()
            .any(|port| !index.data_outputs(node.id()).contains(port))
        {
            return Err(graph("task or child Run published an undeclared output"));
        }
        for output in index.data_outputs(node.id()) {
            let port = index
                .data_port(output)
                .ok_or_else(|| graph("declared output disappeared"))?;
            match outputs.get(output) {
                Some(value) => require_type(value, port.value_type(), "declared output")?,
                None if port.required() => {
                    return Err(SchedulerError::new(
                        SCHEDULER_FACT_MISSING,
                        "completed execution is missing a required output",
                    ));
                }
                None => {}
            }
        }
        Ok(())
    }

    fn require_committed_outputs(
        &self,
        index: &PlanIndex<'plan>,
        node: &Node,
        facts: &SchedulerFacts,
        occurrence: &LogicalOccurrence,
    ) -> Result<(), SchedulerError> {
        for output in index.data_outputs(node.id()) {
            let port = index
                .data_port(output)
                .ok_or_else(|| graph("declared output disappeared"))?;
            match facts.value_at(output, occurrence) {
                Some(value) => require_type(value, port.value_type(), "committed output")?,
                None if port.required() || index.data_outputs(node.id()).len() == 1 => {
                    return Err(SchedulerError::new(
                        SCHEDULER_FACT_MISSING,
                        "completed execution has no committed declared output",
                    ));
                }
                None => {}
            }
        }
        Ok(())
    }

    fn map_collect(
        &self,
        index: &PlanIndex<'plan>,
        map: &NodeId,
    ) -> Result<CollectDescriptor, SchedulerError> {
        index
            .nodes()
            .find_map(|node| match node.kind() {
                NodeKind::Collect(value)
                    if matches!(&value.source, CollectSource::Map { map_node_id } | CollectSource::DynamicMap { map_node_id, .. } if map_node_id == map) =>
                {
                    Some(value.clone())
                }
                _ => None,
            })
            .ok_or_else(|| graph("Map has no correlated Collect"))
    }

    fn loop_collect(
        &self,
        index: &PlanIndex<'plan>,
        owner: &NodeId,
    ) -> Result<CollectDescriptor, SchedulerError> {
        self.loop_collect_node(index, owner)
            .and_then(|node| match node.kind() {
                NodeKind::Collect(value) => Ok(value.clone()),
                _ => Err(graph("Loop Collect kind changed")),
            })
    }

    fn loop_collect_node(
        &self,
        index: &PlanIndex<'plan>,
        owner: &NodeId,
    ) -> Result<&'plan Node, SchedulerError> {
        index
            .nodes()
            .find(|node| {
                matches!(node.kind(), NodeKind::Collect(value) if matches!(&value.source, CollectSource::Loop { loop_node_id, .. } if loop_node_id == owner))
            })
            .ok_or_else(|| graph("Loop has no correlated Collect"))
    }

    fn scope_instance(
        &self,
        ids: &DeterministicIds<'_>,
        index: &PlanIndex<'plan>,
        node: &Node,
        occurrence: &LogicalOccurrence,
    ) -> Result<ScopeInstanceId, SchedulerError> {
        scope_instance_for_occurrence(index, ids.run_id(), node, occurrence)
    }

    fn runtime_owning_scope(
        &self,
        index: &PlanIndex<'plan>,
        node: &Node,
    ) -> Result<&'plan crate::engine::plan::ScopeMetadata, SchedulerError> {
        let mut scope_id = node.scope_id();
        loop {
            let scope = index.scope(scope_id).ok_or_else(|| {
                SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    "node scope or one of its ancestors is absent from PlanIndex",
                )
            })?;
            match scope.kind() {
                PlanScopeKind::Root
                | PlanScopeKind::ForkLeg { .. }
                | PlanScopeKind::MapBody { .. }
                | PlanScopeKind::LoopBody { .. }
                | PlanScopeKind::Subflow { .. } => return Ok(scope),
                PlanScopeKind::Lexical
                | PlanScopeKind::BranchArm { .. }
                | PlanScopeKind::ErrorProtected { .. }
                | PlanScopeKind::ErrorHandler { .. }
                | PlanScopeKind::ErrorFinalizer { .. } => {
                    scope_id = scope.parent().ok_or_else(|| {
                        SchedulerError::new(
                            SCHEDULER_GRAPH_INVALID,
                            "non-materialized Plan scope has no runtime-owning ancestor",
                        )
                    })?;
                }
            }
        }
    }

    fn action(
        &self,
        facts: &SchedulerFacts,
        checkpoint: SchedulerCheckpointId,
        action: SchedulerAction,
    ) -> Result<SchedulerDecision, SchedulerError> {
        if facts.terminal().is_some() {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "terminal Run projection is missing an earlier scheduler checkpoint",
            ));
        }
        let intent = SchedulerIntent::new(facts.run_id().clone(), checkpoint.clone(), action);
        let intent_hash =
            crate::engine::IntentHash::from_serializable(&intent).map_err(|error| {
                SchedulerError::new(
                    SCHEDULER_GRAPH_INVALID,
                    format!("scheduler intent could not be canonicalized: {error}"),
                )
            })?;
        let transition_key = TransitionKey::derive(
            SCHEDULER_TRANSITION_DOMAIN,
            &[
                facts.run_id().as_str(),
                self.linked.index().semantic_hash().as_str(),
                checkpoint.as_str(),
            ],
        )
        .map_err(|error| id_error(error, "transition"))?;
        Ok(SchedulerDecision::Action(Box::new(
            PlannedSchedulerAction::new(
                SchedulerPrecondition::new(facts.projection_version()),
                transition_key,
                intent_hash,
                intent,
            ),
        )))
    }
}

fn termination_outside_finalizer(
    facts: &SchedulerFacts,
    context: &PathContext,
) -> Option<TerminationReason> {
    facts.run_termination_reason().filter(|_| {
        !context.frames.iter().any(|frame| {
            matches!(
                frame,
                StructuralFrame::ErrorBoundary {
                    phase: ErrorBoundaryPhase::Finalizer,
                    ..
                }
            )
        })
    })
}

fn task_admission_class(context: &PathContext) -> TaskAdmissionClass {
    if context.frames.iter().any(|frame| {
        matches!(
            frame,
            StructuralFrame::ErrorBoundary {
                phase: ErrorBoundaryPhase::Finalizer,
                ..
            }
        )
    }) {
        TaskAdmissionClass::TerminationFinalizer
    } else {
        TaskAdmissionClass::Normal
    }
}

fn node_is_owned_by_error_finalizer(
    index: &PlanIndex<'_>,
    node_id: &NodeId,
) -> Result<bool, SchedulerError> {
    let node = index
        .node(node_id)
        .ok_or_else(|| graph("subflow invocation references a missing node"))?;
    let mut scope_id = Some(node.scope_id());
    while let Some(current) = scope_id {
        let scope = index
            .scope(current)
            .ok_or_else(|| graph("node references a missing lexical scope"))?;
        if matches!(scope.kind(), PlanScopeKind::ErrorFinalizer { .. }) {
            return Ok(true);
        }
        scope_id = scope.parent();
    }
    Ok(false)
}

struct ControlRoute {
    node_id: NodeId,
    input_port: ControlPortId,
    edge_id: crate::engine::plan::ControlEdgeId,
}

fn route_for_output(
    index: &PlanIndex<'_>,
    output: &ControlPortId,
) -> Result<ControlRoute, SchedulerError> {
    let route = index
        .successor_for_output(output)
        .map_err(|_| graph("control output route is invalid in PlanIndex"))?
        .ok_or_else(|| graph("non-terminal control output has no successor"))?;
    Ok(ControlRoute {
        node_id: route.successor().id().clone(),
        input_port: route.input().id().clone(),
        edge_id: route.edge().id().clone(),
    })
}

fn descriptor_body_output(
    index: &PlanIndex<'_>,
    node: &Node,
) -> Result<ControlPortId, SchedulerError> {
    index
        .control_outputs(node.id())
        .iter()
        .find(|port| {
            index
                .control_port(port)
                .is_some_and(|value| value.name().as_str() == "body")
        })
        .cloned()
        .ok_or_else(|| graph("Map has no body control output"))
}

fn only_control_output(
    index: &PlanIndex<'_>,
    node: &Node,
) -> Result<ControlPortId, SchedulerError> {
    let outputs = index.control_outputs(node.id());
    if outputs.len() != 1 {
        return Err(graph("linear node must have exactly one control output"));
    }
    Ok(outputs[0].clone())
}

fn frozen_leaf_effect_policy(
    index: &PlanIndex<'_>,
    node: &Node,
    base: &WorkerEffectPolicy,
) -> Result<WorkerEffectPolicy, SchedulerError> {
    let mut max_attempts = base.max_attempts();
    let mut initial_backoff_ms = base.initial_backoff_ms();
    let mut max_backoff_ms = base.max_backoff_ms();
    let mut timeout_ms = base.timeout_ms();
    for policy in index.policies_for_node(node.id()) {
        match policy.kind() {
            PolicyKind::Retry(retry) => {
                max_attempts = retry.max_attempts;
                initial_backoff_ms = retry.initial_backoff_ms;
                max_backoff_ms = retry.max_backoff_ms;
            }
            PolicyKind::Timeout(timeout) => timeout_ms = timeout.timeout_ms,
            PolicyKind::Budget(_) => {
                return Err(graph(
                    "verified leaf policy has no executable runtime contract",
                ));
            }
        }
    }
    WorkerEffectPolicy::frozen(
        base.effect_class(),
        base.effect_idempotency(),
        max_attempts,
        initial_backoff_ms,
        max_backoff_ms,
        timeout_ms,
        base.cancellation(),
    )
    .map_err(|_| graph("linked worker effect policy is invalid"))
}

fn frozen_wait_timeout_ms(
    index: &PlanIndex<'_>,
    node: &Node,
) -> Result<Option<u64>, SchedulerError> {
    let mut timeout_ms = None;
    for policy in index.policies_for_node(node.id()) {
        match policy.kind() {
            PolicyKind::Timeout(timeout) if timeout_ms.is_none() => {
                timeout_ms = Some(timeout.timeout_ms);
            }
            PolicyKind::Timeout(_) => {
                return Err(graph("verified wait has duplicate timeout policies"));
            }
            PolicyKind::Retry(_) | PolicyKind::Budget(_) => {
                return Err(graph(
                    "verified wait policy has no executable runtime contract",
                ));
            }
        }
    }
    Ok(timeout_ms)
}

fn scheduler_task_kind(kind: LeafTaskKind) -> SchedulerTaskKind {
    match kind {
        LeafTaskKind::Llm => SchedulerTaskKind::Llm,
        LeafTaskKind::Action => SchedulerTaskKind::Action,
        LeafTaskKind::Retrieval => SchedulerTaskKind::Retrieval,
        LeafTaskKind::Http => SchedulerTaskKind::Http,
        LeafTaskKind::Tool => SchedulerTaskKind::Tool,
    }
}

fn require_projection_fact(condition: bool, message: &'static str) -> Result<(), SchedulerError> {
    if condition {
        Ok(())
    } else {
        Err(SchedulerError::new(SCHEDULER_FACT_INCONSISTENT, message))
    }
}

fn graph(message: &'static str) -> SchedulerError {
    SchedulerError::new(SCHEDULER_GRAPH_INVALID, message)
}

fn id_error(error: ModelError, subject: &str) -> SchedulerError {
    SchedulerError::new(
        SCHEDULER_GRAPH_INVALID,
        format!("deterministic {subject} identity could not be constructed: {error}"),
    )
}
