//! Process-local scheduler state for terminal-only execution.
//!
//! This module deliberately owns no repository, clock, lease, network, or
//! worker implementation. It applies the inert actions produced by
//! [`crate::SchedulerPlanner`] directly to an in-memory [`SchedulerFacts`]
//! projection. A runtime may therefore execute a complete short Run without
//! appending execution events or materializing durable checkpoints.

use std::collections::BTreeMap;

use crate::{
    scheduler::{
        LogicalOccurrence, RunTerminalFact, SchedulerAction, SchedulerError, SchedulerFacts,
        SchedulerWaitId, TaskFailureFact, TaskOutcomeFact, WaitResolutionFact, WaitSubjectFact,
        SCHEDULER_FACT_INCONSISTENT,
    },
    worker::{TaskExecutionRequest, TaskExecutionResult, WorkerFailure},
    ActivationId, PlannedSchedulerAction, SchedulerTaskId, TerminationReason, WorkerFailureClass,
};

/// One successfully applied process-local scheduler action.
#[derive(Debug, Clone, PartialEq)]
pub enum TerminalSchedulerApply {
    /// The action changed only process-local scheduler facts.
    Applied,
    /// The action admitted one leaf task which the runtime must execute.
    Dispatch {
        request: Box<TaskExecutionRequest>,
        occurrence: LogicalOccurrence,
    },
    /// The exact deterministic checkpoint was already applied.
    ExactReplay,
}

/// Mutable, process-local authority for one terminal-only Run.
///
/// The projection is intentionally lost with the process. Only the admission
/// and eventual terminal result belong in persistent storage.
#[derive(Debug, Clone)]
pub struct TerminalSchedulerState {
    facts: SchedulerFacts,
    activation_occurrences: BTreeMap<ActivationId, LogicalOccurrence>,
    task_occurrences: BTreeMap<SchedulerTaskId, LogicalOccurrence>,
}

impl TerminalSchedulerState {
    pub fn new(facts: SchedulerFacts) -> Self {
        Self {
            facts,
            activation_occurrences: BTreeMap::new(),
            task_occurrences: BTreeMap::new(),
        }
    }

    pub fn facts(&self) -> &SchedulerFacts {
        &self.facts
    }

    pub fn terminal(&self) -> Option<&RunTerminalFact> {
        self.facts.terminal()
    }

    pub fn request_termination(&mut self, reason: TerminationReason) {
        self.facts.request_run_termination(reason);
    }

    pub fn set_observed_time_ms(&mut self, observed_time_ms: u64) {
        self.facts.set_observed_time_ms(observed_time_ms);
    }

    pub fn timer_due_at_ms(&self, wait_id: &SchedulerWaitId) -> Result<u64, SchedulerError> {
        let registration = self
            .facts
            .waits()
            .get(wait_id)
            .ok_or_else(|| inconsistent("terminal timer wait is not registered"))?;
        if registration.timer_id().is_none() {
            return Err(inconsistent(
                "terminal-only execution cannot resolve an external signal wait",
            ));
        }
        registration
            .due_at_ms()
            .ok_or_else(|| inconsistent("terminal timer wait has no due time"))
    }

    pub fn resolve_timer_wait(&mut self, wait_id: &SchedulerWaitId) -> Result<(), SchedulerError> {
        let timer_id = self
            .facts
            .waits()
            .get(wait_id)
            .and_then(|registration| registration.timer_id())
            .cloned()
            .ok_or_else(|| inconsistent("terminal timer wait is unavailable"))?;
        let resolution = WaitResolutionFact::new(WaitSubjectFact::Timer { timer_id }, None)?;
        if self
            .facts
            .resolve_wait_first_winner(wait_id.clone(), resolution)?
        {
            self.bump_projection_version()?;
        }
        Ok(())
    }

    /// Apply one deterministic planner action without producing a durable
    /// event, scheduler checkpoint row, task outbox row, or projection row.
    pub fn apply_planned_action(
        &mut self,
        planned: &PlannedSchedulerAction,
    ) -> Result<TerminalSchedulerApply, SchedulerError> {
        if planned.intent().run_id() != self.facts.run_id() {
            return Err(inconsistent(
                "terminal scheduler action targets a different Run",
            ));
        }
        if self
            .facts
            .checkpoints()
            .contains(planned.intent().checkpoint_id())
        {
            return Ok(TerminalSchedulerApply::ExactReplay);
        }
        if planned.precondition().expected_projection_version() != self.facts.projection_version() {
            return Err(inconsistent(
                "terminal scheduler action projection precondition is stale",
            ));
        }

        let dispatch = self.apply_action(planned.intent().action(), planned)?;
        self.facts
            .commit_checkpoint(planned.intent().checkpoint_id().clone());
        self.bump_projection_version()?;
        Ok(dispatch.unwrap_or(TerminalSchedulerApply::Applied))
    }

    /// Commit one worker success into the process-local scheduler projection.
    pub fn complete_task(
        &mut self,
        request: &TaskExecutionRequest,
        result: &TaskExecutionResult,
    ) -> Result<(), SchedulerError> {
        let task_id = request.task_id().clone();
        if !self.facts.dispatched_tasks().contains(&task_id)
            || self.facts.completed_tasks().contains(&task_id)
        {
            return Err(inconsistent(
                "terminal worker completion has no unique pending dispatch",
            ));
        }
        let occurrence = self
            .task_occurrences
            .get(&task_id)
            .cloned()
            .ok_or_else(|| inconsistent("terminal task occurrence is unavailable"))?;
        for (port, value) in result.outputs() {
            self.facts.record_value_from(
                port.clone(),
                request.activation_id().clone(),
                value.clone(),
            );
            self.facts.record_occurrence_value_from(
                occurrence.clone(),
                port.clone(),
                request.activation_id().clone(),
                value.clone(),
            );
        }
        self.facts.record_task_outcome(
            task_id,
            TaskOutcomeFact::Succeeded {
                outputs: result.outputs().clone(),
            },
        );
        self.bump_projection_version()
    }

    /// Commit one body-free worker failure into the process-local projection.
    pub fn fail_task(
        &mut self,
        request: &TaskExecutionRequest,
        failure: &WorkerFailure,
    ) -> Result<(), SchedulerError> {
        let task_id = request.task_id().clone();
        if !self.facts.dispatched_tasks().contains(&task_id)
            || self.facts.completed_tasks().contains(&task_id)
        {
            return Err(inconsistent(
                "terminal worker failure has no unique pending dispatch",
            ));
        }
        let fact = TaskFailureFact::new(
            failure.class(),
            failure.code(),
            failure.safe_error().cloned(),
        )?;
        self.facts
            .record_task_outcome(task_id, TaskOutcomeFact::Failed { failure: fact });
        self.bump_projection_version()
    }

    fn apply_action(
        &mut self,
        action: &SchedulerAction,
        planned: &PlannedSchedulerAction,
    ) -> Result<Option<TerminalSchedulerApply>, SchedulerError> {
        match action {
            SchedulerAction::FailRunPlanning { failure } => self
                .facts
                .record_terminal(RunTerminalFact::FailedPlanning(*failure)),
            SchedulerAction::AdmitActivation {
                activation_id,
                occurrence,
                reuse_candidate,
                ..
            } => {
                if reuse_candidate.is_some() {
                    return Err(inconsistent(
                        "terminal-only execution cannot consume recovery reuse candidates",
                    ));
                }
                self.record_activation_occurrence(activation_id, occurrence);
                self.facts.record_activation(activation_id.clone());
            }
            SchedulerAction::ConsumeToken { token_id, .. } => {
                self.facts.record_consumed_token(token_id.clone());
            }
            SchedulerAction::EmitToken { token_id, .. } => {
                self.facts.record_emitted_token(token_id.clone());
            }
            SchedulerAction::DispatchTask {
                task_id,
                activation_id,
                ..
            } => {
                let occurrence = self
                    .activation_occurrences
                    .get(activation_id)
                    .cloned()
                    .ok_or_else(|| {
                        inconsistent("terminal task activation occurrence is unavailable")
                    })?;
                let request = TaskExecutionRequest::from_scheduler_intent(planned.intent())
                    .map_err(|_| {
                        inconsistent("terminal scheduler produced an invalid worker request")
                    })?;
                self.facts.record_dispatched_task(task_id.clone());
                self.task_occurrences
                    .insert(task_id.clone(), occurrence.clone());
                return Ok(Some(TerminalSchedulerApply::Dispatch {
                    request: Box::new(request),
                    occurrence,
                }));
            }
            SchedulerAction::CommitNativeOutput {
                activation_id,
                occurrence,
                output,
                ..
            } => {
                let crate::NativeOutput::Values { values } = output;
                for (port, value) in values {
                    self.facts.record_value_from(
                        port.clone(),
                        activation_id.clone(),
                        value.clone(),
                    );
                    self.facts.record_occurrence_value_from(
                        occurrence.clone(),
                        port.clone(),
                        activation_id.clone(),
                        value.clone(),
                    );
                }
            }
            SchedulerAction::SelectBranchAndAdmit { selection } => {
                self.facts.record_branch_selection(
                    selection.branch_node_id().clone(),
                    selection.case_id().clone(),
                );
                self.facts.record_occurrence_branch_selection(
                    selection.occurrence().clone(),
                    selection.branch_node_id().clone(),
                    selection.case_id().clone(),
                );
                self.record_activation_occurrence(
                    selection.successor().activation_id(),
                    selection.successor().occurrence(),
                );
                self.facts
                    .record_activation(selection.successor().activation_id().clone());
                self.facts
                    .record_emitted_token(selection.token_id().clone());
                self.facts
                    .record_consumed_token(selection.token_id().clone());
            }
            SchedulerAction::CommitOccurrenceValues {
                activation_id,
                occurrence,
                values,
                ..
            } => {
                for (port, value) in values {
                    self.facts.record_value_from(
                        port.clone(),
                        activation_id.clone(),
                        value.clone(),
                    );
                    self.facts.record_occurrence_value_from(
                        occurrence.clone(),
                        port.clone(),
                        activation_id.clone(),
                        value.clone(),
                    );
                }
            }
            SchedulerAction::CompleteRun { output, .. } => self
                .facts
                .record_terminal(RunTerminalFact::Succeeded(output.clone())),
            SchedulerAction::FailRun { error, .. } => self
                .facts
                .record_terminal(RunTerminalFact::Failed(error.clone())),
            SchedulerAction::FailRunInternal { failure, .. } => self
                .facts
                .record_terminal(RunTerminalFact::FailedInternal(failure.clone())),
            SchedulerAction::CancelRun { reason, .. } => {
                self.facts.record_terminal(match reason {
                    TerminationReason::Failure => {
                        RunTerminalFact::FailedInternal(TaskFailureFact::new(
                            WorkerFailureClass::InfrastructureFailure,
                            "RUN_TERMINATED_FAILURE",
                            None,
                        )?)
                    }
                    TerminationReason::Cancelled => RunTerminalFact::Cancelled,
                    TerminationReason::Interrupted => RunTerminalFact::Interrupted,
                    TerminationReason::TimedOut => RunTerminalFact::TimedOut,
                });
            }
            SchedulerAction::OpenFork { admission } => {
                self.facts.record_fork_group(admission.group().clone());
                for leg in admission.legs() {
                    self.record_activation_occurrence(
                        leg.leg().child_activation_id(),
                        leg.leg().occurrence(),
                    );
                    self.facts.record_fork_leg(leg.leg().clone());
                }
            }
            SchedulerAction::SettleForkLeg { leg, outcome } => {
                self.facts
                    .settle_fork_leg(leg.key().clone(), outcome.clone());
            }
            SchedulerAction::CompleteFork { group_id, .. } => {
                self.facts.complete_fork(group_id.clone());
            }
            SchedulerAction::RequestScopeCancellation {
                scope_instance_id, ..
            } => {
                self.facts
                    .request_scope_cancellation(scope_instance_id.clone());
                // The terminal engine has no durable child claims. Once the
                // local cancellation token has been fired, drain acknowledgement
                // is process-local and can be recorded immediately.
                self.facts
                    .record_scope_cancelled_and_drained(scope_instance_id);
            }
            SchedulerAction::OpenMap { map } => {
                self.facts.record_map_instance(map.clone());
            }
            SchedulerAction::SpawnMapItem {
                item,
                item_port,
                item_value,
                ..
            } => {
                self.record_activation_occurrence(item.child_activation_id(), item.occurrence());
                self.facts
                    .record_map_item(item.clone(), item_port.clone(), item_value.clone());
            }
            SchedulerAction::SettleMapItem { item, outcome } => {
                self.facts
                    .settle_map_item(item.key().clone(), outcome.clone());
            }
            SchedulerAction::CompleteMap { map_activation_id } => {
                self.facts.complete_map(map_activation_id.clone());
            }
            SchedulerAction::OpenLoop { loop_instance } => {
                self.facts.record_loop_instance(loop_instance.clone());
            }
            SchedulerAction::StartLoopIteration {
                iteration,
                state_port,
                ..
            } => {
                self.record_activation_occurrence(
                    iteration.child_activation_id(),
                    iteration.occurrence(),
                );
                self.facts
                    .record_loop_iteration(iteration.clone(), state_port.clone());
            }
            SchedulerAction::AdvanceLoop { iteration, state } => {
                self.facts
                    .advance_loop(iteration.key().loop_activation_id(), state.clone())?;
            }
            SchedulerAction::SettleLoopIteration { iteration, outcome } => {
                self.facts
                    .settle_loop_iteration(iteration.key().clone(), outcome.clone());
            }
            SchedulerAction::CompleteLoop {
                loop_activation_id,
                state,
                ..
            } => {
                self.facts
                    .complete_loop(loop_activation_id, state.clone())?;
            }
            SchedulerAction::RegisterWait { registration } => {
                self.facts.register_wait(registration.clone());
            }
            SchedulerAction::StartSubflow { invocation, .. } => {
                self.record_activation_occurrence(
                    invocation.parent_activation_id(),
                    invocation.occurrence(),
                );
                self.facts.record_subflow(invocation.clone());
            }
            SchedulerAction::RequestChildRunCancellation { child_run_id } => {
                self.facts.request_child_cancellation(child_run_id.clone());
            }
            SchedulerAction::SettleSubflow {
                invocation,
                outcome,
            } => {
                self.facts
                    .settle_subflow(invocation.child_run_id().clone(), outcome.clone());
            }
            SchedulerAction::OpenErrorBoundary { boundary }
            | SchedulerAction::TransitionErrorBoundary { boundary } => {
                self.facts.record_boundary(boundary.clone());
            }
        }
        Ok(None)
    }

    fn record_activation_occurrence(
        &mut self,
        activation_id: &ActivationId,
        occurrence: &LogicalOccurrence,
    ) {
        self.activation_occurrences
            .insert(activation_id.clone(), occurrence.clone());
    }

    fn bump_projection_version(&mut self) -> Result<(), SchedulerError> {
        let next = self
            .facts
            .projection_version()
            .checked_add(1)
            .ok_or_else(|| inconsistent("terminal scheduler projection version overflowed"))?;
        self.facts.set_projection_version(next);
        Ok(())
    }
}

fn inconsistent(message: &'static str) -> SchedulerError {
    SchedulerError::new(SCHEDULER_FACT_INCONSISTENT, message)
}
