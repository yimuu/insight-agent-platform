use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use super::{
    ActivationAttemptAggregate, ActivationId, ActivationLifecycle, ActivationTerminationReason,
    AdmissionState, ApplyOutcome, ExecutionKind, IntentHash, ModelError, RunId, RunLifecycle,
    RunState, ScopeInstance, ScopeInstanceId, ScopeTracker, ScopeTrackerState, TerminationClaim,
    TerminationIntent, TerminationReason, TransitionKey, TransitionOutcome, ValueRef,
};

#[cfg(test)]
use super::{ChildRequirement, NodeId};

pub const RUN_AGGREGATE_INTENT_SCHEMA_VERSION: u32 = 1;
pub const RUN_AGGREGATE_STATE_INVALID: &str = "ENGINE_RUN_AGGREGATE_STATE_INVALID";
pub const RUN_AGGREGATE_INTENT_CONFLICT: &str = "ENGINE_RUN_AGGREGATE_INTENT_CONFLICT";
pub const RUN_AGGREGATE_SCOPE_UNKNOWN: &str = "ENGINE_RUN_AGGREGATE_SCOPE_UNKNOWN";
pub const RUN_AGGREGATE_SCOPE_CONFLICT: &str = "ENGINE_RUN_AGGREGATE_SCOPE_CONFLICT";
pub const RUN_AGGREGATE_ACTIVATION_UNKNOWN: &str = "ENGINE_RUN_AGGREGATE_ACTIVATION_UNKNOWN";
pub const RUN_AGGREGATE_ACTIVATION_CONFLICT: &str = "ENGINE_RUN_AGGREGATE_ACTIVATION_CONFLICT";
pub const RUN_AGGREGATE_ADMISSION_CLOSED: &str = "ENGINE_RUN_AGGREGATE_ADMISSION_CLOSED";
pub const RUN_AGGREGATE_RETURN_INVALID: &str = "ENGINE_RUN_AGGREGATE_RETURN_INVALID";
pub const RUN_AGGREGATE_DRAIN_BLOCKED: &str = "ENGINE_RUN_AGGREGATE_DRAIN_BLOCKED";

/// Derived business-work projection. It is never persisted as a second source
/// of truth: `RunLifecycle::Waiting` is reconciled from Activation facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunWorkProjection {
    Runnable,
    DurableWaiting,
    Quiescent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunTerminationProgress {
    Draining {
        intent: TerminationIntent,
        blockers: Vec<ActivationId>,
    },
    Terminal {
        lifecycle: RunLifecycle,
    },
}

#[derive(Debug)]
struct ScopeExecutionState {
    instance: ScopeInstance,
    tracker: ScopeTracker,
}

#[derive(Debug, Clone, PartialEq)]
struct ReturnCommit {
    activation_id: ActivationId,
    output: ValueRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunCommandRecord {
    intent_hash: IntentHash,
    result: RunCommandResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RunCommandResult {
    Unit,
    Termination(RunTerminationProgress),
}

/// Aggregate root for one Run's lifecycle, structured scopes, and logical
/// Activations. A repository persists this model with ledger/projection/outbox
/// changes in one transaction; callers cannot obtain mutable child authority.
#[derive(Debug)]
pub struct RunExecutionAggregate {
    run_id: RunId,
    state: RunState,
    scopes: BTreeMap<ScopeInstanceId, ScopeExecutionState>,
    activations: BTreeMap<ActivationId, ActivationAttemptAggregate>,
    parent_scope_by_activation: BTreeMap<ActivationId, ScopeInstanceId>,
    return_commit: Option<ReturnCommit>,
    commands: BTreeMap<TransitionKey, RunCommandRecord>,
}

impl RunExecutionAggregate {
    pub fn new(run_id: RunId) -> Self {
        let root = ScopeInstance::root();
        let root_id = root.id().clone();
        let root_tracker = ScopeTracker::new(run_id.clone(), root_id.clone());
        Self {
            run_id,
            state: RunState::new(),
            scopes: BTreeMap::from([(
                root_id,
                ScopeExecutionState {
                    instance: root,
                    tracker: root_tracker,
                },
            )]),
            activations: BTreeMap::new(),
            parent_scope_by_activation: BTreeMap::new(),
            return_commit: None,
            commands: BTreeMap::new(),
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn state(&self) -> &RunState {
        &self.state
    }

    pub fn root_scope_id(&self) -> ScopeInstanceId {
        ScopeInstanceId::root()
    }

    pub fn scope(&self, scope_id: &ScopeInstanceId) -> Option<&ScopeInstance> {
        self.scopes.get(scope_id).map(|scope| &scope.instance)
    }

    pub fn scope_tracker(&self, scope_id: &ScopeInstanceId) -> Option<&ScopeTracker> {
        self.scopes.get(scope_id).map(|scope| &scope.tracker)
    }

    pub fn activation(&self, activation_id: &ActivationId) -> Option<&ActivationAttemptAggregate> {
        self.activations.get(activation_id)
    }

    pub fn activations(&self) -> impl ExactSizeIterator<Item = &ActivationAttemptAggregate> {
        self.activations.values()
    }

    pub fn output(&self) -> Option<&ValueRef> {
        self.return_commit.as_ref().map(|commit| &commit.output)
    }

    pub fn work_projection(&self) -> RunWorkProjection {
        let mut has_wait = false;
        for activation in self.activations.values() {
            match activation.state().lifecycle() {
                ActivationLifecycle::Created
                | ActivationLifecycle::Ready
                | ActivationLifecycle::Leased
                | ActivationLifecycle::Running
                | ActivationLifecycle::RetryWait
                | ActivationLifecycle::Terminating => return RunWorkProjection::Runnable,
                ActivationLifecycle::Waiting => has_wait = true,
                ActivationLifecycle::Succeeded
                | ActivationLifecycle::Failed
                | ActivationLifecycle::Cancelled
                | ActivationLifecycle::TimedOut => {}
            }
        }
        if has_wait {
            RunWorkProjection::DurableWaiting
        } else {
            RunWorkProjection::Quiescent
        }
    }

    pub fn start(
        &mut self,
        transition_key: TransitionKey,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        #[derive(Serialize)]
        struct Intent {
            operation: &'static str,
        }
        let hash = self.intent_hash(&Intent {
            operation: "run.start",
        })?;
        self.apply_unit(transition_key, hash, |staged| staged.state.start())
    }

    pub fn pause(
        &mut self,
        transition_key: TransitionKey,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        #[derive(Serialize)]
        struct Intent {
            operation: &'static str,
        }
        let hash = self.intent_hash(&Intent {
            operation: "run.pause",
        })?;
        self.apply_unit(transition_key, hash, |staged| {
            staged.state.pause()?;
            Ok(())
        })
    }

    pub fn resume(
        &mut self,
        transition_key: TransitionKey,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        #[derive(Serialize)]
        struct Intent {
            operation: &'static str,
        }
        let hash = self.intent_hash(&Intent {
            operation: "run.resume",
        })?;
        self.apply_unit(transition_key, hash, |staged| {
            staged.state.resume()?;
            Ok(())
        })
    }

    #[cfg(test)]
    pub(crate) fn create_scope(
        &mut self,
        transition_key: TransitionKey,
        instance: ScopeInstance,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            instance: &'a ScopeInstance,
        }
        let hash = self.intent_hash(&Intent {
            operation: "scope.create",
            instance: &instance,
        })?;
        self.apply_unit(transition_key, hash, move |staged| {
            staged.require_admission_open()?;
            instance.validate()?;
            let parent = instance.parent().ok_or_else(|| {
                ModelError::new(
                    RUN_AGGREGATE_SCOPE_CONFLICT,
                    "a second root scope cannot be created",
                )
            })?;
            if !staged.scopes.contains_key(parent) {
                return Err(ModelError::new(
                    RUN_AGGREGATE_SCOPE_UNKNOWN,
                    "child scope references an unknown parent scope",
                ));
            }
            if staged.scopes.contains_key(instance.id()) {
                return Err(ModelError::new(
                    RUN_AGGREGATE_SCOPE_CONFLICT,
                    "scope identity is already present in this Run",
                ));
            }
            let id = instance.id().clone();
            staged.scopes.insert(
                id.clone(),
                ScopeExecutionState {
                    instance,
                    tracker: ScopeTracker::new(staged.run_id.clone(), id),
                },
            );
            Ok(())
        })
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn admit_activation(
        &mut self,
        transition_key: TransitionKey,
        parent_scope_id: ScopeInstanceId,
        execution_scope_id: ScopeInstanceId,
        activation_id: ActivationId,
        node_id: NodeId,
        execution_kind: ExecutionKind,
        requirement: ChildRequirement,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            parent_scope_id: &'a ScopeInstanceId,
            execution_scope_id: &'a ScopeInstanceId,
            activation_id: &'a ActivationId,
            node_id: &'a NodeId,
            execution_kind: &'a ExecutionKind,
            requirement: ChildRequirement,
        }
        let hash = self.intent_hash(&Intent {
            operation: "activation.admit",
            parent_scope_id: &parent_scope_id,
            execution_scope_id: &execution_scope_id,
            activation_id: &activation_id,
            node_id: &node_id,
            execution_kind: &execution_kind,
            requirement,
        })?;
        let child_ready_key = TransitionKey::derive(
            "run.admit_activation.ready",
            &[
                self.run_id.as_str(),
                transition_key.as_str(),
                activation_id.as_str(),
            ],
        )?;
        self.apply_unit(transition_key, hash, move |staged| {
            staged.require_admission_open()?;
            let execution_scope = staged.scopes.get(&execution_scope_id).ok_or_else(|| {
                ModelError::new(
                    RUN_AGGREGATE_SCOPE_UNKNOWN,
                    "Activation execution scope does not exist",
                )
            })?;
            if execution_scope_id != parent_scope_id
                && execution_scope.instance.parent() != Some(&parent_scope_id)
            {
                return Err(ModelError::new(
                    RUN_AGGREGATE_SCOPE_CONFLICT,
                    "Activation execution scope must be its owning scope or a direct child of it",
                ));
            }
            if staged.activations.contains_key(&activation_id) {
                return Err(ModelError::new(
                    RUN_AGGREGATE_ACTIVATION_CONFLICT,
                    "Activation identity is already present in this Run",
                ));
            }
            let parent = staged.scopes.get_mut(&parent_scope_id).ok_or_else(|| {
                ModelError::new(
                    RUN_AGGREGATE_SCOPE_UNKNOWN,
                    "Activation parent scope does not exist",
                )
            })?;
            parent.tracker.admit_child(
                activation_id.clone(),
                execution_scope_id.clone(),
                requirement,
            )?;

            let mut activation = ActivationAttemptAggregate::new(
                staged.run_id.clone(),
                activation_id.clone(),
                node_id,
                execution_scope_id,
                execution_kind,
            );
            match activation.make_ready(child_ready_key)? {
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {}
                TransitionOutcome::StaleLease | TransitionOutcome::StateConflict => {
                    return Err(ModelError::new(
                        RUN_AGGREGATE_ACTIVATION_CONFLICT,
                        "newly admitted Activation could not become ready",
                    ));
                }
            }
            staged
                .parent_scope_by_activation
                .insert(activation_id.clone(), parent_scope_id);
            staged.activations.insert(activation_id, activation);
            staged.reconcile()?;
            Ok(())
        })
    }

    /// Clone-staged child transaction. The closure can use only the child's
    /// invariant-preserving public commands; any error discards all child,
    /// scope-settlement, and Run-projection changes together.
    #[cfg(test)]
    pub(crate) fn transition_activation<T>(
        &mut self,
        activation_id: &ActivationId,
        operation: impl FnOnce(&mut ActivationAttemptAggregate) -> Result<T, ModelError>,
    ) -> Result<T, ModelError> {
        let mut staged = self.staged();
        let activation = staged.activations.get_mut(activation_id).ok_or_else(|| {
            ModelError::new(
                RUN_AGGREGATE_ACTIVATION_UNKNOWN,
                "Activation does not belong to this Run",
            )
        })?;
        let result = operation(activation)?;
        staged.reconcile()?;
        staged.validate()?;
        *self = staged;
        Ok(result)
    }

    /// Scheduler-only success claim. The output is taken from the durable
    /// Return Activation; an API caller cannot provide an unrelated value.
    #[allow(dead_code)] // Stage 4 scheduler is the sole production caller.
    pub(crate) fn complete_from_return(
        &mut self,
        transition_key: TransitionKey,
        return_activation_id: ActivationId,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            return_activation_id: &'a ActivationId,
        }
        let hash = self.intent_hash(&Intent {
            operation: "run.complete_from_return",
            return_activation_id: &return_activation_id,
        })?;
        self.apply_unit(transition_key, hash, move |staged| {
            let return_activation =
                staged
                    .activations
                    .get(&return_activation_id)
                    .ok_or_else(|| {
                        ModelError::new(
                            RUN_AGGREGATE_RETURN_INVALID,
                            "Return Activation does not belong to this Run",
                        )
                    })?;
            if !matches!(
                return_activation.execution_kind(),
                ExecutionKind::SchedulerNative
            ) || return_activation.state().lifecycle() != ActivationLifecycle::Succeeded
            {
                return Err(ModelError::new(
                    RUN_AGGREGATE_RETURN_INVALID,
                    "Run success requires a succeeded scheduler-native Return Activation",
                ));
            }
            let output = return_activation.output().cloned().ok_or_else(|| {
                ModelError::new(
                    RUN_AGGREGATE_RETURN_INVALID,
                    "Return Activation has no durable output",
                )
            })?;
            staged.state.begin_completing()?;
            staged.return_commit = Some(ReturnCommit {
                activation_id: return_activation_id,
                output,
            });
            staged.reconcile()?;
            Ok(())
        })
    }

    pub fn request_termination(
        &mut self,
        transition_key: TransitionKey,
        reason: TerminationReason,
    ) -> Result<TransitionOutcome<RunTerminationProgress>, ModelError> {
        #[derive(Serialize)]
        struct Intent {
            operation: &'static str,
            reason: TerminationReason,
        }
        let hash = self.intent_hash(&Intent {
            operation: "run.request_termination",
            reason,
        })?;
        match self.replay(&transition_key, &hash)? {
            Replay::Exact => {
                let authoritative = match &self
                    .commands
                    .get(&transition_key)
                    .expect("exact replay came from the command ledger")
                    .result
                {
                    RunCommandResult::Termination(progress) => progress.clone(),
                    RunCommandResult::Unit => {
                        return Err(ModelError::new(
                            RUN_AGGREGATE_STATE_INVALID,
                            "termination replay resolved to a non-termination command result",
                        ));
                    }
                };
                return Ok(TransitionOutcome::ExactReplay { authoritative });
            }
            Replay::Missing => {}
        }

        // A committed terminating terminal remains the durable first winner.
        // A later command with a fresh idempotency key observes that authority
        // instead of failing merely because drain has already finished.
        if matches!(
            self.state.lifecycle(),
            RunLifecycle::Failed
                | RunLifecycle::Cancelled
                | RunLifecycle::Interrupted
                | RunLifecycle::TimedOut
        ) {
            let mut staged = self.staged();
            let progress = staged.termination_progress()?;
            staged.record_termination(transition_key, hash, progress.clone());
            staged.validate()?;
            *self = staged;
            return Ok(TransitionOutcome::Committed { result: progress });
        }

        let mut staged = self.staged();
        let claim = staged.state.request_termination(reason)?;
        let authoritative = match claim {
            TerminationClaim::Claimed(intent) | TerminationClaim::Existing(intent) => intent,
        };
        // Completing is not a committed success terminal. A termination claim
        // that wins during drain invalidates the provisional Return commit.
        staged.return_commit = None;
        let activation_reason = match authoritative.reason {
            TerminationReason::Failure => ActivationTerminationReason::Failure,
            TerminationReason::Cancelled | TerminationReason::Interrupted => {
                ActivationTerminationReason::Cancelled
            }
            TerminationReason::TimedOut => ActivationTerminationReason::TimedOut,
        };
        let activation_ids = staged.activations.keys().cloned().collect::<Vec<_>>();
        for activation_id in activation_ids {
            let activation = staged
                .activations
                .get_mut(&activation_id)
                .expect("Activation ID came from the same map");
            if activation.state().lifecycle().is_terminal() {
                continue;
            }
            let child_key = TransitionKey::derive(
                "run.terminate_activation",
                &[
                    staged.run_id.as_str(),
                    transition_key.as_str(),
                    activation_id.as_str(),
                ],
            )?;
            match activation.request_termination(child_key, activation_reason)? {
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {}
                TransitionOutcome::StateConflict => {
                    let already_has_authoritative_termination = activation.state().lifecycle()
                        == ActivationLifecycle::Terminating
                        && activation.state().termination_reason() == Some(activation_reason);
                    if !already_has_authoritative_termination {
                        return Err(ModelError::new(
                            RUN_AGGREGATE_ACTIVATION_CONFLICT,
                            "child Activation rejected the Run termination command as conflicting",
                        ));
                    }
                }
                TransitionOutcome::StaleLease => {
                    return Err(ModelError::new(
                        RUN_AGGREGATE_STATE_INVALID,
                        "termination propagation unexpectedly produced a stale lease",
                    ));
                }
            }
        }
        staged.reconcile()?;
        let progress = staged.termination_progress()?;
        staged.record_termination(transition_key, hash, progress.clone());
        staged.validate()?;
        *self = staged;
        Ok(TransitionOutcome::Committed { result: progress })
    }

    pub fn termination_progress(&self) -> Result<RunTerminationProgress, ModelError> {
        if self.state.lifecycle() == RunLifecycle::Terminating {
            let intent = self.state.termination_intent().ok_or_else(|| {
                ModelError::new(
                    RUN_AGGREGATE_STATE_INVALID,
                    "terminating Run has no authoritative intent",
                )
            })?;
            return Ok(RunTerminationProgress::Draining {
                intent,
                blockers: self.drain_blockers(),
            });
        }
        if matches!(
            self.state.lifecycle(),
            RunLifecycle::Failed
                | RunLifecycle::Cancelled
                | RunLifecycle::Interrupted
                | RunLifecycle::TimedOut
        ) {
            return Ok(RunTerminationProgress::Terminal {
                lifecycle: self.state.lifecycle(),
            });
        }
        Err(ModelError::new(
            RUN_AGGREGATE_STATE_INVALID,
            "Run has no termination claim",
        ))
    }

    pub fn drain_blockers(&self) -> Vec<ActivationId> {
        self.activations
            .iter()
            .filter(|(_, activation)| activation.terminal_proof().is_err())
            .map(|(activation_id, _)| activation_id.clone())
            .collect()
    }

    pub fn validate(&self) -> Result<(), ModelError> {
        self.state.validate()?;
        let root_id = ScopeInstanceId::root();
        let root = self.scopes.get(&root_id).ok_or_else(|| {
            ModelError::new(
                RUN_AGGREGATE_STATE_INVALID,
                "Run has no authoritative root scope",
            )
        })?;
        if root.instance.parent().is_some()
            || root.instance.id() != &root_id
            || root.tracker.run_id() != &self.run_id
            || root.tracker.scope_instance_id() != &root_id
        {
            return Err(ModelError::new(
                RUN_AGGREGATE_STATE_INVALID,
                "Run root scope identity is inconsistent",
            ));
        }

        for (scope_id, scope) in &self.scopes {
            scope.instance.validate()?;
            if scope.instance.id() != scope_id
                || scope.tracker.run_id() != &self.run_id
                || scope.tracker.scope_instance_id() != scope_id
            {
                return Err(ModelError::new(
                    RUN_AGGREGATE_STATE_INVALID,
                    "scope instance and tracker identities diverged",
                ));
            }
            if let Some(parent) = scope.instance.parent() {
                if !self.scopes.contains_key(parent) {
                    return Err(ModelError::new(
                        RUN_AGGREGATE_STATE_INVALID,
                        "scope parent is absent from the Run",
                    ));
                }
            }
        }

        if self.parent_scope_by_activation.len() != self.activations.len() {
            return Err(ModelError::new(
                RUN_AGGREGATE_STATE_INVALID,
                "Activation ownership index is incomplete",
            ));
        }
        for (activation_id, activation) in &self.activations {
            activation.validate()?;
            if activation.run_id() != &self.run_id || activation.activation_id() != activation_id {
                return Err(ModelError::new(
                    RUN_AGGREGATE_STATE_INVALID,
                    "Activation identity belongs to another Run or map key",
                ));
            }
            if !self.scopes.contains_key(activation.scope_instance_id()) {
                return Err(ModelError::new(
                    RUN_AGGREGATE_STATE_INVALID,
                    "Activation execution scope is absent",
                ));
            }
            let parent_scope_id = self
                .parent_scope_by_activation
                .get(activation_id)
                .ok_or_else(|| {
                    ModelError::new(
                        RUN_AGGREGATE_STATE_INVALID,
                        "Activation has no parent scope ownership",
                    )
                })?;
            let child = self
                .scopes
                .get(parent_scope_id)
                .and_then(|scope| scope.tracker.child(activation_id))
                .ok_or_else(|| {
                    ModelError::new(
                        RUN_AGGREGATE_STATE_INVALID,
                        "parent scope did not durably admit its Activation",
                    )
                })?;
            if child.scope_instance_id() != activation.scope_instance_id() {
                return Err(ModelError::new(
                    RUN_AGGREGATE_STATE_INVALID,
                    "scope child and Activation execution scope diverged",
                ));
            }
            let execution_scope = self
                .scopes
                .get(activation.scope_instance_id())
                .expect("Activation execution scope existence was checked above");
            if activation.scope_instance_id() != parent_scope_id
                && execution_scope.instance.parent() != Some(parent_scope_id)
            {
                return Err(ModelError::new(
                    RUN_AGGREGATE_STATE_INVALID,
                    "Activation execution scope is neither its owner nor a direct child scope",
                ));
            }
            if activation.state().lifecycle().is_terminal() && child.settlement().is_none() {
                return Err(ModelError::new(
                    RUN_AGGREGATE_STATE_INVALID,
                    "terminal Activation was not settled into its parent scope",
                ));
            }
        }

        let expected_waiting = self.work_projection() == RunWorkProjection::DurableWaiting;
        match self.state.lifecycle() {
            RunLifecycle::Waiting if !expected_waiting => {
                return Err(ModelError::new(
                    RUN_AGGREGATE_STATE_INVALID,
                    "Run waiting projection has no durable wait or still has runnable work",
                ));
            }
            RunLifecycle::Active if expected_waiting => {
                return Err(ModelError::new(
                    RUN_AGGREGATE_STATE_INVALID,
                    "Run failed to project its durable wait",
                ));
            }
            _ => {}
        }

        if matches!(
            self.state.admission(),
            AdmissionState::Draining | AdmissionState::Closed
        ) && self
            .scopes
            .values()
            .any(|scope| scope.tracker.state() == ScopeTrackerState::Open)
        {
            return Err(ModelError::new(
                RUN_AGGREGATE_STATE_INVALID,
                "draining Run retained an open structured scope",
            ));
        }

        if self.state.lifecycle().is_terminal() {
            if self
                .scopes
                .values()
                .any(|scope| scope.tracker.state() != ScopeTrackerState::Completed)
                || !self.drain_blockers().is_empty()
            {
                return Err(ModelError::new(
                    RUN_AGGREGATE_DRAIN_BLOCKED,
                    "terminal Run retained a live scope or Attempt",
                ));
            }
            match self.state.lifecycle() {
                RunLifecycle::Succeeded if self.return_commit.is_none() => {
                    return Err(ModelError::new(
                        RUN_AGGREGATE_RETURN_INVALID,
                        "succeeded Run has no durable Return output",
                    ));
                }
                RunLifecycle::Failed
                | RunLifecycle::Cancelled
                | RunLifecycle::Interrupted
                | RunLifecycle::TimedOut
                    if self.return_commit.is_some() =>
                {
                    return Err(ModelError::new(
                        RUN_AGGREGATE_RETURN_INVALID,
                        "terminated Run retained a success Return commit",
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn require_admission_open(&self) -> Result<(), ModelError> {
        if self.state.admission() != AdmissionState::Open
            || !matches!(
                self.state.lifecycle(),
                RunLifecycle::Active | RunLifecycle::Waiting
            )
        {
            return Err(ModelError::new(
                RUN_AGGREGATE_ADMISSION_CLOSED,
                "Run cannot admit new work in its current lifecycle/admission state",
            ));
        }
        Ok(())
    }

    fn reconcile(&mut self) -> Result<(), ModelError> {
        self.settle_terminal_activations()?;
        match self.state.lifecycle() {
            RunLifecycle::Active | RunLifecycle::Waiting => self.project_waiting(),
            RunLifecycle::Completing | RunLifecycle::Terminating => self.try_finish_drain(),
            RunLifecycle::Created
            | RunLifecycle::Succeeded
            | RunLifecycle::Failed
            | RunLifecycle::Cancelled
            | RunLifecycle::Interrupted
            | RunLifecycle::TimedOut => Ok(()),
        }
    }

    fn project_waiting(&mut self) -> Result<(), ModelError> {
        match (self.state.lifecycle(), self.work_projection()) {
            (RunLifecycle::Active, RunWorkProjection::DurableWaiting) => self.state.enter_waiting(),
            (RunLifecycle::Waiting, RunWorkProjection::Runnable | RunWorkProjection::Quiescent) => {
                self.state.leave_waiting()
            }
            _ => Ok(()),
        }
    }

    fn settle_terminal_activations(&mut self) -> Result<(), ModelError> {
        let mut settlements = Vec::new();
        for (activation_id, activation) in &self.activations {
            if !activation.state().lifecycle().is_terminal() {
                continue;
            }
            let proof = activation.terminal_proof()?;
            let parent_scope_id = self
                .parent_scope_by_activation
                .get(activation_id)
                .ok_or_else(|| {
                    ModelError::new(
                        RUN_AGGREGATE_STATE_INVALID,
                        "terminal Activation has no parent scope",
                    )
                })?
                .clone();
            settlements.push((parent_scope_id, proof));
        }
        for (parent_scope_id, proof) in settlements {
            let parent = self.scopes.get_mut(&parent_scope_id).ok_or_else(|| {
                ModelError::new(
                    RUN_AGGREGATE_SCOPE_UNKNOWN,
                    "terminal Activation parent scope disappeared",
                )
            })?;
            parent.tracker.settle_child(&proof)?;
        }
        Ok(())
    }

    fn try_finish_drain(&mut self) -> Result<(), ModelError> {
        for scope in self.scopes.values_mut() {
            scope.tracker.begin_closing()?;
        }
        self.complete_drained_scopes()?;
        if !self.drain_blockers().is_empty()
            || self
                .scopes
                .values()
                .any(|scope| scope.tracker.state() != ScopeTrackerState::Completed)
        {
            return Ok(());
        }
        self.state.finish_drain()?;
        match self.state.lifecycle() {
            RunLifecycle::Completing => self.state.finish_success(),
            RunLifecycle::Terminating => {
                self.state.finish_termination()?;
                Ok(())
            }
            _ => Err(ModelError::new(
                RUN_AGGREGATE_STATE_INVALID,
                "drain completion was requested outside completing/terminating",
            )),
        }
    }

    fn complete_drained_scopes(&mut self) -> Result<(), ModelError> {
        loop {
            let completed = self
                .scopes
                .iter()
                .filter(|(_, scope)| scope.tracker.state() == ScopeTrackerState::Completed)
                .map(|(scope_id, _)| scope_id.clone())
                .collect::<BTreeSet<_>>();
            let candidates = self
                .scopes
                .iter()
                .filter_map(|(scope_id, scope)| {
                    if scope.tracker.state() != ScopeTrackerState::Closing
                        || !scope.tracker.completion_blockers().is_empty()
                    {
                        return None;
                    }
                    let children_completed = self.scopes.values().all(|child_scope| {
                        child_scope.instance.parent() != Some(scope_id)
                            || completed.contains(child_scope.instance.id())
                    });
                    children_completed.then(|| scope_id.clone())
                })
                .collect::<Vec<_>>();
            if candidates.is_empty() {
                return Ok(());
            }
            let mut changed = false;
            for scope_id in candidates {
                let scope = self
                    .scopes
                    .get_mut(&scope_id)
                    .expect("candidate scope came from the same map");
                changed |= scope.tracker.complete()? == ApplyOutcome::Applied;
            }
            if !changed {
                return Ok(());
            }
        }
    }

    fn apply_unit(
        &mut self,
        transition_key: TransitionKey,
        intent_hash: IntentHash,
        operation: impl FnOnce(&mut Self) -> Result<(), ModelError>,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        match self.replay(&transition_key, &intent_hash)? {
            Replay::Exact => match &self
                .commands
                .get(&transition_key)
                .expect("exact replay came from the command ledger")
                .result
            {
                RunCommandResult::Unit => {
                    return Ok(TransitionOutcome::ExactReplay { authoritative: () });
                }
                RunCommandResult::Termination(_) => {
                    return Err(ModelError::new(
                        RUN_AGGREGATE_STATE_INVALID,
                        "unit replay resolved to a termination command result",
                    ));
                }
            },
            Replay::Missing => {}
        }
        let mut staged = self.staged();
        operation(&mut staged)?;
        staged.record(transition_key, intent_hash);
        staged.validate()?;
        *self = staged;
        Ok(TransitionOutcome::Committed { result: () })
    }

    fn replay(&self, key: &TransitionKey, intent_hash: &IntentHash) -> Result<Replay, ModelError> {
        match self.commands.get(key) {
            None => Ok(Replay::Missing),
            Some(record) if &record.intent_hash == intent_hash => Ok(Replay::Exact),
            Some(_) => Err(ModelError::new(
                RUN_AGGREGATE_INTENT_CONFLICT,
                "transition key is already bound to a different canonical intent",
            )),
        }
    }

    /// Internal transactional copy. Keeping this explicit prevents callers
    /// from duplicating aggregate authority while preserving rollback-on-error
    /// for model commands.
    fn staged(&self) -> Self {
        Self {
            run_id: self.run_id.clone(),
            state: self.state.clone(),
            scopes: self
                .scopes
                .iter()
                .map(|(scope_id, scope)| {
                    (
                        scope_id.clone(),
                        ScopeExecutionState {
                            instance: scope.instance.clone(),
                            tracker: scope.tracker.staged(),
                        },
                    )
                })
                .collect(),
            activations: self
                .activations
                .iter()
                .map(|(activation_id, activation)| (activation_id.clone(), activation.staged()))
                .collect(),
            parent_scope_by_activation: self.parent_scope_by_activation.clone(),
            return_commit: self.return_commit.clone(),
            commands: self.commands.clone(),
        }
    }

    fn record(&mut self, key: TransitionKey, intent_hash: IntentHash) {
        let previous = self.commands.insert(
            key,
            RunCommandRecord {
                intent_hash,
                result: RunCommandResult::Unit,
            },
        );
        debug_assert!(previous.is_none(), "command replay was preflighted");
    }

    fn record_termination(
        &mut self,
        key: TransitionKey,
        intent_hash: IntentHash,
        progress: RunTerminationProgress,
    ) {
        let previous = self.commands.insert(
            key,
            RunCommandRecord {
                intent_hash,
                result: RunCommandResult::Termination(progress),
            },
        );
        debug_assert!(previous.is_none(), "command replay was preflighted");
    }

    fn intent_hash<T>(&self, command: &T) -> Result<IntentHash, ModelError>
    where
        T: Serialize + ?Sized,
    {
        #[derive(Serialize)]
        struct Envelope<'a, T: ?Sized> {
            schema_version: u32,
            run_id: &'a RunId,
            command: &'a T,
        }
        IntentHash::from_serializable(&Envelope {
            schema_version: RUN_AGGREGATE_INTENT_SCHEMA_VERSION,
            run_id: &self.run_id,
            command,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Replay {
    Missing,
    Exact,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        CommittedOutputProof, ExecutionEventId, SignalId, TransitionOutcome, WaitResolutionProof,
        WaitResolutionSubject,
    };

    fn run_id() -> RunId {
        RunId::new("run_model").unwrap()
    }

    fn activation_id(value: &str) -> ActivationId {
        ActivationId::new(value).unwrap()
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn key(value: &str) -> TransitionKey {
        TransitionKey::derive("run-model-test", &[value]).unwrap()
    }

    fn start(run: &mut RunExecutionAggregate) {
        assert!(matches!(
            run.start(key("start")).unwrap(),
            TransitionOutcome::Committed { .. }
        ));
    }

    fn admit_native(run: &mut RunExecutionAggregate, id: &str) -> ActivationId {
        let activation_id = activation_id(id);
        run.admit_activation(
            key(&format!("admit-{id}")),
            ScopeInstanceId::root(),
            ScopeInstanceId::root(),
            activation_id.clone(),
            node_id(id),
            ExecutionKind::SchedulerNative,
            ChildRequirement::Required,
        )
        .unwrap();
        activation_id
    }

    fn complete_native(
        run: &mut RunExecutionAggregate,
        activation_id: &ActivationId,
        value: serde_json::Value,
    ) {
        let proof = CommittedOutputProof::mint(
            run.run_id().clone(),
            activation_id.clone(),
            None,
            ValueRef::inline(value).unwrap(),
        );
        let command_key = key(&format!("complete-{}", activation_id.as_str()));
        let outcome = run
            .transition_activation(activation_id, |activation| {
                activation.complete_native(command_key, &proof)
            })
            .unwrap();
        assert!(matches!(outcome, TransitionOutcome::Committed { .. }));
    }

    #[test]
    fn waiting_is_derived_and_pause_changes_only_admission() {
        let mut run = RunExecutionAggregate::new(run_id());
        start(&mut run);
        let wait_id = activation_id("wait");
        run.admit_activation(
            key("admit-wait"),
            ScopeInstanceId::root(),
            ScopeInstanceId::root(),
            wait_id.clone(),
            node_id("wait"),
            ExecutionKind::DurableWait,
            ChildRequirement::Required,
        )
        .unwrap();
        let registration_key = key("begin-wait");
        run.transition_activation(&wait_id, |activation| {
            activation.begin_wait(registration_key.clone())
        })
        .unwrap();
        assert_eq!(run.state().lifecycle(), RunLifecycle::Waiting);

        run.pause(key("pause")).unwrap();
        assert_eq!(run.state().lifecycle(), RunLifecycle::Waiting);
        assert_eq!(run.state().admission(), AdmissionState::Paused);
        run.resume(key("resume")).unwrap();
        assert_eq!(run.state().lifecycle(), RunLifecycle::Waiting);

        let output = CommittedOutputProof::mint(
            run.run_id().clone(),
            wait_id.clone(),
            None,
            ValueRef::inline(json!({"signal": true})).unwrap(),
        );
        let winning_key = key("resolve-wait");
        let proof = WaitResolutionProof::mint(
            run.run_id().clone(),
            wait_id.clone(),
            registration_key,
            WaitResolutionSubject::Signal(SignalId::new("signal_resume").unwrap()),
            winning_key.clone(),
            ExecutionEventId::parse("event_00000000000000000000000000000001").unwrap(),
            output,
        )
        .unwrap();
        run.transition_activation(&wait_id, |activation| {
            activation.resolve_wait(winning_key, &proof)
        })
        .unwrap();
        assert_eq!(run.state().lifecycle(), RunLifecycle::Active);
        run.validate().unwrap();
    }

    #[test]
    fn return_cannot_commit_parent_success_before_every_child_drains() {
        let mut run = RunExecutionAggregate::new(run_id());
        start(&mut run);
        let return_id = admit_native(&mut run, "return_node");
        let live_id = admit_native(&mut run, "live_node");
        complete_native(&mut run, &return_id, json!({"answer": 42}));

        run.complete_from_return(key("return"), return_id.clone())
            .unwrap();
        assert_eq!(run.state().lifecycle(), RunLifecycle::Completing);
        assert_eq!(run.state().admission(), AdmissionState::Draining);
        assert!(run.output().is_some());
        assert!(run
            .scope_tracker(&ScopeInstanceId::root())
            .unwrap()
            .completion_blockers()
            .contains(&live_id));

        complete_native(&mut run, &live_id, json!(null));
        assert_eq!(run.state().lifecycle(), RunLifecycle::Succeeded);
        assert_eq!(run.state().admission(), AdmissionState::Closed);
        assert_eq!(
            run.scope_tracker(&ScopeInstanceId::root()).unwrap().state(),
            ScopeTrackerState::Completed
        );
        run.validate().unwrap();
    }

    #[test]
    fn termination_is_first_winner_and_waits_for_live_attempt_drain() {
        use crate::{
            EffectIdempotency, LeaseEpoch, LeaseGrantProof, WorkerCancellation,
            WorkerExecutionPolicy,
        };
        use chrono::{Duration, Utc};

        let mut run = RunExecutionAggregate::new(run_id());
        start(&mut run);
        let worker_id = activation_id("worker");
        run.admit_activation(
            key("admit-worker"),
            ScopeInstanceId::root(),
            ScopeInstanceId::root(),
            worker_id.clone(),
            node_id("worker"),
            ExecutionKind::Worker(
                WorkerExecutionPolicy::new(
                    EffectIdempotency::Idempotent,
                    1,
                    WorkerCancellation::Cooperative,
                )
                .unwrap(),
            ),
            ChildRequirement::Required,
        )
        .unwrap();
        let lease = LeaseGrantProof::mint(
            run.run_id().clone(),
            worker_id.clone(),
            LeaseEpoch::FIRST,
            crate::TimerId::new("lease_timer").unwrap(),
            Utc::now() + Duration::minutes(1),
        );
        let fence = run
            .transition_activation(&worker_id, |activation| {
                activation.claim_worker(key("claim"), &lease)
            })
            .unwrap()
            .committed_result()
            .copied()
            .unwrap();
        run.transition_activation(&worker_id, |activation| {
            activation.mark_running(key("running"), fence)
        })
        .unwrap();

        let first = run
            .request_termination(key("cancel"), TerminationReason::Cancelled)
            .unwrap();
        assert!(matches!(
            first,
            TransitionOutcome::Committed {
                result: RunTerminationProgress::Draining { .. }
            }
        ));
        assert_eq!(run.state().lifecycle(), RunLifecycle::Terminating);
        assert_eq!(run.drain_blockers(), vec![worker_id.clone()]);

        let loser = run
            .request_termination(key("timeout"), TerminationReason::TimedOut)
            .unwrap();
        assert!(matches!(
            loser,
            TransitionOutcome::Committed {
                result: RunTerminationProgress::Draining { .. }
            }
        ));
        assert_eq!(
            run.state().termination_intent(),
            Some(TerminationIntent {
                reason: TerminationReason::Cancelled
            })
        );

        run.transition_activation(&worker_id, |activation| {
            activation.acknowledge_cancellation(key("ack"), fence)
        })
        .unwrap();
        assert_eq!(run.state().lifecycle(), RunLifecycle::Cancelled);
        assert_eq!(run.state().admission(), AdmissionState::Closed);

        let replay = run
            .request_termination(key("cancel"), TerminationReason::Cancelled)
            .unwrap();
        assert!(matches!(
            replay,
            TransitionOutcome::ExactReplay {
                authoritative: RunTerminationProgress::Draining { ref blockers, .. }
            } if blockers == &vec![worker_id]
        ));

        let terminal_loser = run
            .request_termination(key("late-timeout"), TerminationReason::TimedOut)
            .unwrap();
        assert!(matches!(
            terminal_loser,
            TransitionOutcome::Committed {
                result: RunTerminationProgress::Terminal {
                    lifecycle: RunLifecycle::Cancelled
                }
            }
        ));
        assert_eq!(
            run.state().termination_intent(),
            Some(TerminationIntent {
                reason: TerminationReason::Cancelled
            })
        );
        run.validate().unwrap();
    }

    #[test]
    fn termination_child_intent_error_aborts_the_entire_run_transition() {
        let mut run = RunExecutionAggregate::new(run_id());
        start(&mut run);
        let wait_id = activation_id("wait_collision");
        run.admit_activation(
            key("admit-wait-collision"),
            ScopeInstanceId::root(),
            ScopeInstanceId::root(),
            wait_id.clone(),
            node_id("wait_collision"),
            ExecutionKind::DurableWait,
            ChildRequirement::Required,
        )
        .unwrap();

        let run_key = key("cancel-with-child-collision");
        let colliding_child_key = TransitionKey::derive(
            "run.terminate_activation",
            &[run.run_id().as_str(), run_key.as_str(), wait_id.as_str()],
        )
        .unwrap();
        run.transition_activation(&wait_id, |activation| {
            activation.begin_wait(colliding_child_key)
        })
        .unwrap();
        assert_eq!(run.state().lifecycle(), RunLifecycle::Waiting);

        let error = run
            .request_termination(run_key, TerminationReason::Cancelled)
            .unwrap_err();
        assert_eq!(error.code(), crate::aggregate::AGGREGATE_INTENT_CONFLICT);
        assert_eq!(run.state().lifecycle(), RunLifecycle::Waiting);
        assert_eq!(run.state().admission(), AdmissionState::Open);
        assert_eq!(
            run.activation(&wait_id).unwrap().state().lifecycle(),
            ActivationLifecycle::Waiting
        );
        run.validate().unwrap();
    }

    #[test]
    fn exact_run_command_replay_precedes_state_checks_and_changed_intent_is_an_error() {
        let mut run = RunExecutionAggregate::new(run_id());
        let start_key = key("stable");
        assert!(matches!(
            run.start(start_key.clone()).unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            run.start(start_key.clone()).unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        assert_eq!(
            run.pause(start_key).unwrap_err().code(),
            RUN_AGGREGATE_INTENT_CONFLICT
        );
        assert_eq!(run.state().admission(), AdmissionState::Open);
    }

    #[test]
    fn termination_claim_during_success_drain_wins_and_discards_provisional_return() {
        let mut run = RunExecutionAggregate::new(run_id());
        start(&mut run);
        let return_id = admit_native(&mut run, "return_race");
        let live_id = admit_native(&mut run, "live_race");
        complete_native(&mut run, &return_id, json!({"provisional": true}));
        run.complete_from_return(key("return-race"), return_id)
            .unwrap();
        assert_eq!(run.state().lifecycle(), RunLifecycle::Completing);

        run.request_termination(key("cancel-race"), TerminationReason::Cancelled)
            .unwrap();
        assert_eq!(run.state().lifecycle(), RunLifecycle::Cancelled);
        assert!(run.output().is_none());
        assert_eq!(
            run.activation(&live_id).unwrap().state().lifecycle(),
            ActivationLifecycle::Cancelled
        );
        run.validate().unwrap();
    }

    #[test]
    fn nested_scope_must_drain_before_root_scope_can_complete() {
        let mut run = RunExecutionAggregate::new(run_id());
        start(&mut run);
        let child =
            ScopeInstance::loop_iteration(&ScopeInstanceId::root(), node_id("loop_owner"), 0)
                .unwrap();
        let child_id = child.id().clone();
        run.create_scope(key("child-scope"), child).unwrap();
        let return_id = admit_native(&mut run, "return_nested");
        let nested_id = activation_id("nested");
        run.admit_activation(
            key("admit-nested"),
            child_id.clone(),
            child_id.clone(),
            nested_id.clone(),
            node_id("nested"),
            ExecutionKind::SchedulerNative,
            ChildRequirement::Required,
        )
        .unwrap();
        complete_native(&mut run, &return_id, json!("done"));
        run.complete_from_return(key("complete-run"), return_id)
            .unwrap();
        assert_eq!(run.state().lifecycle(), RunLifecycle::Completing);

        complete_native(&mut run, &nested_id, json!(null));
        assert_eq!(
            run.scope_tracker(&child_id).unwrap().state(),
            ScopeTrackerState::Completed
        );
        assert_eq!(run.state().lifecycle(), RunLifecycle::Succeeded);
        run.validate().unwrap();
    }

    #[test]
    fn activation_cannot_be_owned_by_one_sibling_and_execute_in_another() {
        let mut run = RunExecutionAggregate::new(run_id());
        start(&mut run);
        let first =
            ScopeInstance::loop_iteration(&ScopeInstanceId::root(), node_id("loop_a"), 0).unwrap();
        let second =
            ScopeInstance::loop_iteration(&ScopeInstanceId::root(), node_id("loop_b"), 0).unwrap();
        let first_id = first.id().clone();
        let second_id = second.id().clone();
        run.create_scope(key("scope-a"), first).unwrap();
        run.create_scope(key("scope-b"), second).unwrap();

        let sibling_activation = activation_id("sibling_escape");
        let error = run
            .admit_activation(
                key("admit-sibling-escape"),
                first_id.clone(),
                second_id,
                sibling_activation.clone(),
                node_id("sibling_escape"),
                ExecutionKind::SchedulerNative,
                ChildRequirement::Required,
            )
            .unwrap_err();
        assert_eq!(error.code(), RUN_AGGREGATE_SCOPE_CONFLICT);
        assert!(run.activation(&sibling_activation).is_none());
        assert!(run
            .scope_tracker(&first_id)
            .unwrap()
            .completion_blockers()
            .is_empty());
        run.validate().unwrap();
    }
}
