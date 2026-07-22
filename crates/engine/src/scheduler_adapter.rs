//! Workspace-internal projections for durable scheduler adapters.
//!
//! These free functions expose immutable persistence parts without widening
//! the domain types' inherent API or allowing adapters to bypass validation.

use crate::{
    plan::PlanType, ActivationId, ForkGroupId, LegId, NodeId, ScopeInstanceId, SignalId, TimerId,
};

use crate::scheduler::{
    ForkGroupFact, HumanTaskWaitFact, LogicalOccurrence, LoopIterationFact, RuntimeValue,
    SchedulerWaitId, WaitRegistrationFact,
};

pub struct ForkGroupParts<'a> {
    pub group_id: &'a ForkGroupId,
    pub fork_node_id: &'a NodeId,
    pub fork_activation_id: &'a ActivationId,
    pub parent_scope_instance_id: &'a ScopeInstanceId,
    pub occurrence: &'a LogicalOccurrence,
    pub members: &'a [LegId],
}

pub fn fork_group_parts(group: &ForkGroupFact) -> ForkGroupParts<'_> {
    ForkGroupParts {
        group_id: &group.group_id,
        fork_node_id: &group.fork_node_id,
        fork_activation_id: &group.fork_activation_id,
        parent_scope_instance_id: &group.parent_scope_instance_id,
        occurrence: &group.occurrence,
        members: &group.members,
    }
}

pub fn loop_iteration_state(iteration: &LoopIterationFact) -> &RuntimeValue {
    &iteration.state
}

pub struct WaitRegistrationParts<'a> {
    pub wait_id: &'a SchedulerWaitId,
    pub activation_id: &'a ActivationId,
    pub node_id: &'a NodeId,
    pub occurrence: &'a LogicalOccurrence,
    pub signal_name: Option<&'a str>,
    pub signal_id: Option<&'a SignalId>,
    pub timer_id: Option<&'a TimerId>,
    pub due_at_ms: Option<u64>,
    pub payload_type: Option<&'a PlanType>,
    pub human_task: Option<&'a HumanTaskWaitFact>,
}

pub fn wait_registration_parts(registration: &WaitRegistrationFact) -> WaitRegistrationParts<'_> {
    WaitRegistrationParts {
        wait_id: &registration.wait_id,
        activation_id: &registration.activation_id,
        node_id: &registration.node_id,
        occurrence: &registration.occurrence,
        signal_name: registration.signal_name.as_deref(),
        signal_id: registration.signal_id.as_ref(),
        timer_id: registration.timer_id.as_ref(),
        due_at_ms: registration.due_at_ms,
        payload_type: registration.payload_type.as_ref(),
        human_task: registration.human_task.as_ref(),
    }
}
