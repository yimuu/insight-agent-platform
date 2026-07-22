//! Workspace-internal adapter surface.
//!
//! These functions preserve invariant-enforcing constructors across package
//! boundaries without adding new inherent methods to the compatibility API.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};

use crate::{
    plan::{
        DataPortId, Node, PlanIndex, PlanInputContract, PlanType, PortName, ScopeId,
        SubflowCallDescriptor,
    },
    scheduler::{
        BoundTaskInput, LogicalOccurrence, PlannedSchedulerAction, RuntimeValue, SchedulerAction,
        SchedulerCheckpointId, SchedulerError, SchedulerFacts, SchedulerIntent,
        SchedulerPrecondition, SubflowInvocationFact, TaskOutputContract,
    },
    ActivationId, ActivationLifecycle, CommittedOutputProof, EventSeq, ExecutionEventId,
    IntentHash, LeaseEpoch, LeaseFence, LeaseGrantProof, ModelError, PublicEventContext,
    PublicEventEnvelope, PublicEventPayload, RetryScheduleProof, RunId, ScopeInstanceId,
    TerminalActivationProof, TerminalActivationResult, TimerId, TransitionKey, ValueRef,
    WaitResolutionProof, WaitResolutionSubject,
};

pub fn model_error(code: &'static str, message: impl Into<String>) -> ModelError {
    ModelError::new(code, message)
}

pub fn lease_grant_proof(
    run_id: RunId,
    activation_id: ActivationId,
    lease_epoch: LeaseEpoch,
    lease_timer_id: TimerId,
    lease_deadline: DateTime<Utc>,
) -> LeaseGrantProof {
    LeaseGrantProof::mint(
        run_id,
        activation_id,
        lease_epoch,
        lease_timer_id,
        lease_deadline,
    )
}

pub fn retry_schedule_proof(
    run_id: RunId,
    activation_id: ActivationId,
    previous_fence: LeaseFence,
    retry_timer_id: TimerId,
    retry_at: DateTime<Utc>,
    remaining_attempt_budget: u32,
) -> Result<RetryScheduleProof, ModelError> {
    RetryScheduleProof::mint(
        run_id,
        activation_id,
        previous_fence,
        retry_timer_id,
        retry_at,
        remaining_attempt_budget,
    )
}

pub fn committed_output_proof(
    run_id: RunId,
    activation_id: ActivationId,
    fence: Option<LeaseFence>,
    value: ValueRef,
) -> CommittedOutputProof {
    CommittedOutputProof::mint(run_id, activation_id, fence, value)
}

#[allow(clippy::too_many_arguments)]
pub fn wait_resolution_proof(
    run_id: RunId,
    activation_id: ActivationId,
    registration_transition_key: TransitionKey,
    subject: WaitResolutionSubject,
    winning_transition_key: TransitionKey,
    winning_event_id: ExecutionEventId,
    output: CommittedOutputProof,
) -> Result<WaitResolutionProof, ModelError> {
    WaitResolutionProof::mint(
        run_id,
        activation_id,
        registration_transition_key,
        subject,
        winning_transition_key,
        winning_event_id,
        output,
    )
}

pub fn terminal_activation_proof(
    run_id: RunId,
    scope_instance_id: ScopeInstanceId,
    activation_id: ActivationId,
    terminal: ActivationLifecycle,
    attempts_drained: bool,
    result: TerminalActivationResult,
) -> Result<TerminalActivationProof, ModelError> {
    TerminalActivationProof::mint(
        run_id,
        scope_instance_id,
        activation_id,
        terminal,
        attempts_drained,
        result,
    )
}

pub fn public_event_envelope(
    context: PublicEventContext,
    seq: EventSeq,
    occurred_at: DateTime<Utc>,
    payload: PublicEventPayload,
) -> PublicEventEnvelope {
    PublicEventEnvelope::new(context, seq, occurred_at, payload)
}

pub fn task_output_contract(
    port_id: DataPortId,
    name: PortName,
    value_type: PlanType,
    required: bool,
) -> TaskOutputContract {
    TaskOutputContract::new(port_id, name, value_type, required)
}

pub fn bound_task_input(
    port_id: DataPortId,
    name: PortName,
    value: RuntimeValue,
) -> BoundTaskInput {
    BoundTaskInput::with_source_activations(port_id, name, value, BTreeSet::new())
}

pub fn scheduler_intent(
    run_id: RunId,
    checkpoint_id: SchedulerCheckpointId,
    action: SchedulerAction,
) -> SchedulerIntent {
    SchedulerIntent::new(run_id, checkpoint_id, action)
}

pub fn scheduler_precondition(expected_projection_version: u64) -> SchedulerPrecondition {
    SchedulerPrecondition::new(expected_projection_version)
}

pub fn planned_scheduler_action(
    precondition: SchedulerPrecondition,
    transition_key: TransitionKey,
    intent_hash: IntentHash,
    intent: SchedulerIntent,
) -> PlannedSchedulerAction {
    PlannedSchedulerAction::new(precondition, transition_key, intent_hash, intent)
}

pub fn derive_subflow_invocation(
    index: &PlanIndex<'_>,
    facts: &SchedulerFacts,
    node: &Node,
    descriptor: &SubflowCallDescriptor,
    occurrence: &LogicalOccurrence,
) -> Result<SubflowInvocationFact, SchedulerError> {
    crate::scheduler::derive_subflow_invocation(index, facts, node, descriptor, occurrence)
}

pub fn derive_subflow_admission(
    index: &PlanIndex<'_>,
    facts: &SchedulerFacts,
    node: &Node,
    descriptor: &SubflowCallDescriptor,
    occurrence: &LogicalOccurrence,
    input_contract: &PlanInputContract,
) -> Result<(RuntimeValue, Vec<TaskOutputContract>), SchedulerError> {
    let derived = crate::scheduler::derive_subflow_admission(
        index,
        facts,
        node,
        descriptor,
        occurrence,
        input_contract,
    )?;
    Ok((derived.run_input, derived.outputs))
}

pub fn scope_instance_for_occurrence(
    index: &PlanIndex<'_>,
    run_id: &RunId,
    node: &Node,
    occurrence: &LogicalOccurrence,
) -> Result<ScopeInstanceId, SchedulerError> {
    crate::scheduler::scope_instance_for_occurrence(index, run_id, node, occurrence)
}

pub fn scope_instance_for_runtime_scope(
    index: &PlanIndex<'_>,
    run_id: &RunId,
    scope_id: &ScopeId,
    occurrence: &LogicalOccurrence,
) -> Result<ScopeInstanceId, SchedulerError> {
    crate::scheduler::scope_instance_for_runtime_scope(index, run_id, scope_id, occurrence)
}
