//! Run-level convergence intent. Persistence supplies committed facts and database time.
use super::*;
use insight_platform_contracts::ScopeState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunConvergenceGoal {
    pub reason: OrchestrationConvergenceReason,
    pub terminal_state: RunState,
    pub control: RunControlSnapshot,
    pub failure: Option<Failure>,
}

/// Failure is recorded on the Run once an exhausted orchestration attempt is observed.
/// Subsequent steps cancel siblings while retaining that original failure outcome.
pub fn decide_run_convergence(
    state: RunState,
    version: u64,
    deadline: DateTime<Utc>,
    current: &RunCurrentSnapshot,
    exhausted_attempt: bool,
    database_now: DateTime<Utc>,
) -> Result<Option<RunConvergenceGoal>, OrchestratorError> {
    current.control.validate()?;
    if version == 0 {
        return Err(OrchestratorError::InvalidRunControl);
    }
    if matches!(
        state,
        RunState::Succeeded | RunState::Failed | RunState::Cancelled | RunState::TimedOut
    ) {
        return Ok(None);
    }
    let deadline_exceeded = database_now >= deadline;
    let mut control = current.control.clone();
    let (reason, terminal_state, failure) =
        if control.timeout_requested_at.is_some() || deadline_exceeded {
            if control.timeout_requested_at.is_none() {
                control = match decide_timeout(
                    &control,
                    control.timeout_generation,
                    database_now,
                    deadline,
                    state.as_str().to_owned(),
                    version,
                )? {
                    ControlDecision::Updated(next) | ControlDecision::Unchanged(next) => next,
                };
            }
            (
                if deadline_exceeded {
                    OrchestrationConvergenceReason::DeadlineExceeded
                } else {
                    OrchestrationConvergenceReason::TimeoutObserved
                },
                RunState::TimedOut,
                current.failure.clone(),
            )
        } else if control.cancel_requested_at.is_some() {
            (
                OrchestrationConvergenceReason::CancelRequested,
                RunState::Cancelled,
                current.failure.clone(),
            )
        } else if current.failure.is_some() || exhausted_attempt {
            let failure = current.failure.clone().unwrap_or(Failure {
                code: insight_platform_contracts::FailureCode::Platform {
                    code: insight_platform_contracts::PlatformFailureCode::DependencyUnavailable,
                },
                class: insight_platform_contracts::FailureClass::Platform,
                retryability: insight_platform_contracts::Retryability::Never,
                safe_message: Some(
                    "Orchestration attempt budget exhausted after lease expiry".to_owned(),
                ),
                details_ref: None,
                source: insight_platform_contracts::FailureSource::Platform,
            });
            (
                if current.failure.is_some() {
                    OrchestrationConvergenceReason::FailureObserved
                } else {
                    OrchestrationConvergenceReason::AttemptLimitExhausted
                },
                RunState::Failed,
                Some(failure),
            )
        } else {
            return Ok(None);
        };
    Ok(Some(RunConvergenceGoal {
        reason,
        terminal_state,
        control,
        failure,
    }))
}

/// A failed Run cancels unrelated work; only the actual exhausted execution is failed.
pub fn convergence_member_states(
    goal: &RunConvergenceGoal,
    exhausted_attempt: bool,
) -> (JobState, NodeExecutionState, ScopeState) {
    match goal.terminal_state {
        RunState::TimedOut => (
            JobState::TimedOut,
            NodeExecutionState::TimedOut,
            ScopeState::Failed,
        ),
        RunState::Failed if exhausted_attempt => (
            JobState::Failed,
            NodeExecutionState::Failed,
            ScopeState::Failed,
        ),
        RunState::Failed => (
            JobState::Cancelled,
            NodeExecutionState::Cancelled,
            ScopeState::Failed,
        ),
        _ => (
            JobState::Cancelled,
            NodeExecutionState::Cancelled,
            ScopeState::Cancelled,
        ),
    }
}

/// Propagation keeps the child's own first winner. Parent failure detail and declared
/// interface identities never cross into the child's result contract.
pub fn propagate_parent_convergence(
    parent_deadline: DateTime<Utc>,
    parent: &RunCurrentSnapshot,
    child_state: RunState,
    child_version: u64,
    child_deadline: DateTime<Utc>,
    child: &RunCurrentSnapshot,
    database_now: DateTime<Utc>,
) -> Result<Option<RunCurrentSnapshot>, OrchestratorError> {
    if child_deadline > parent_deadline || child_version == 0 {
        return Err(OrchestratorError::InvalidChildRun);
    }
    if matches!(
        child_state,
        RunState::Succeeded | RunState::Failed | RunState::Cancelled | RunState::TimedOut
    ) {
        return Ok(None);
    }
    if child.control.cancel_requested_at.is_some()
        || child.control.timeout_requested_at.is_some()
        || child.failure.is_some()
    {
        return Ok(Some(child.clone()));
    }
    let mut next = child.clone();
    if parent.control.timeout_requested_at.is_some() || database_now >= parent_deadline {
        if database_now < child_deadline {
            return Ok(None);
        }
        next.control = match decide_timeout(
            &child.control,
            child.control.timeout_generation,
            database_now,
            child_deadline,
            child_state.as_str().to_owned(),
            child_version,
        )? {
            ControlDecision::Updated(control) | ControlDecision::Unchanged(control) => control,
        };
    } else if parent.control.cancel_requested_at.is_some() {
        let reason = parent
            .control
            .cancel_reason_code
            .clone()
            .ok_or(OrchestratorError::InvalidRunControl)?;
        let principal = parent
            .control
            .cancel_principal
            .clone()
            .ok_or(OrchestratorError::InvalidRunControl)?;
        next.control = match decide_cancel(
            &child.control,
            child.control.cancel_generation,
            database_now,
            reason,
            principal,
        )? {
            ControlDecision::Updated(control) | ControlDecision::Unchanged(control) => control,
        };
    } else if parent.failure.is_some() {
        next.failure = Some(Failure {
            code: insight_platform_contracts::FailureCode::Platform {
                code: insight_platform_contracts::PlatformFailureCode::DependencyUnavailable,
            },
            class: insight_platform_contracts::FailureClass::Dependency,
            retryability: insight_platform_contracts::Retryability::Never,
            source: insight_platform_contracts::FailureSource::Agent,
            safe_message: Some("Parent Run is converging after a terminal failure".into()),
            details_ref: None,
        });
    } else {
        return Ok(None);
    }
    Ok(Some(next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use insight_platform_contracts::{
        FailureClass, FailureCode, FailureSource, Permission, PermissionSet, PlatformFailureCode,
        PrincipalKind, Retryability,
    };

    fn initial() -> RunCurrentSnapshot {
        RunCurrentSnapshot::initial(
            "run_0198f1c5-0787-75e1-a9e8-d95ca0f37101".parse().unwrap(),
            "adep_0198f1c5-0787-75e1-a9e8-d95ca0f37102".parse().unwrap(),
            "val_0198f1c5-0787-75e1-a9e8-d95ca0f37103".parse().unwrap(),
        )
    }
    fn failure() -> Failure {
        Failure {
            code: FailureCode::Platform {
                code: PlatformFailureCode::DependencyUnavailable,
            },
            class: FailureClass::Dependency,
            retryability: Retryability::Never,
            source: FailureSource::Agent,
            safe_message: Some("Parent-specific details".into()),
            details_ref: None,
        }
    }
    fn cancelled(now: DateTime<Utc>) -> RunCurrentSnapshot {
        let mut current = initial();
        let principal = PrincipalSnapshot::build(
            "ten_0198f1c5-0787-75e1-a9e8-d95ca0f37104".parse().unwrap(),
            "prn_0198f1c5-0787-75e1-a9e8-d95ca0f37105".parse().unwrap(),
            PrincipalKind::AgentRunner,
            PermissionSet::new(vec![Permission::RuntimeControl]).unwrap(),
            1,
            1,
            1,
        )
        .unwrap();
        current.control = match decide_cancel(
            &current.control,
            0,
            now,
            "user_request".into(),
            principal,
        )
        .unwrap()
        {
            ControlDecision::Updated(control) => control,
            _ => panic!("first cancel must update"),
        };
        current
    }
    #[test]
    fn run_goal_keeps_deadline_priority_and_distinguishes_failed_origin_from_siblings() {
        let now = Utc.with_ymd_and_hms(2026, 9, 6, 0, 0, 0).unwrap();
        let deadline = now + Duration::seconds(10);
        let mut current = initial();
        assert!(
            decide_run_convergence(RunState::Running, 1, deadline, &current, false, now)
                .unwrap()
                .is_none()
        );
        let exhausted = decide_run_convergence(RunState::Running, 1, deadline, &current, true, now)
            .unwrap()
            .unwrap();
        assert_eq!(exhausted.terminal_state, RunState::Failed);
        assert_eq!(
            exhausted.reason,
            OrchestrationConvergenceReason::AttemptLimitExhausted
        );
        assert_eq!(
            convergence_member_states(&exhausted, true),
            (
                JobState::Failed,
                NodeExecutionState::Failed,
                ScopeState::Failed
            )
        );
        assert_eq!(
            convergence_member_states(&exhausted, false),
            (
                JobState::Cancelled,
                NodeExecutionState::Cancelled,
                ScopeState::Failed
            )
        );
        current.failure = Some(failure());
        assert_eq!(
            decide_run_convergence(RunState::Running, 1, deadline, &current, false, now)
                .unwrap()
                .unwrap()
                .reason,
            OrchestrationConvergenceReason::FailureObserved,
        );
        current = cancelled(now);
        current.failure = Some(failure());
        assert_eq!(
            decide_run_convergence(RunState::Cancelling, 2, deadline, &current, true, now)
                .unwrap()
                .unwrap()
                .terminal_state,
            RunState::Cancelled
        );
        let expired =
            decide_run_convergence(RunState::Cancelling, 2, deadline, &current, true, deadline)
                .unwrap()
                .unwrap();
        assert_eq!(expired.terminal_state, RunState::TimedOut);
        assert_eq!(expired.control.timeout_observed_run_version, Some(2));
        assert!(
            decide_run_convergence(RunState::Succeeded, 3, deadline, &current, true, deadline)
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn child_propagation_keeps_first_winner_and_does_not_copy_parent_failure_details() {
        let now = Utc.with_ymd_and_hms(2026, 9, 6, 0, 0, 0).unwrap();
        let deadline = now + Duration::seconds(10);
        let mut parent = initial();
        parent.failure = Some(failure());
        let child = initial();
        let next = propagate_parent_convergence(
            deadline,
            &parent,
            RunState::Waiting,
            1,
            deadline,
            &child,
            now,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            next.failure.as_ref().unwrap().class,
            FailureClass::Dependency
        );
        assert_eq!(
            next.failure.as_ref().unwrap().safe_message.as_deref(),
            Some("Parent Run is converging after a terminal failure")
        );
        assert!(next.failure.as_ref().unwrap().details_ref.is_none());
        let cancelled_child = cancelled(now);
        assert_eq!(
            propagate_parent_convergence(
                deadline,
                &parent,
                RunState::Cancelling,
                2,
                deadline,
                &cancelled_child,
                now
            )
            .unwrap(),
            Some(cancelled_child)
        );
        assert!(propagate_parent_convergence(
            deadline,
            &parent,
            RunState::Succeeded,
            2,
            deadline,
            &child,
            now
        )
        .unwrap()
        .is_none());
        assert!(propagate_parent_convergence(
            deadline,
            &parent,
            RunState::Waiting,
            1,
            deadline + Duration::seconds(1),
            &child,
            now
        )
        .is_err());
        let timeout = propagate_parent_convergence(
            deadline,
            &parent,
            RunState::Waiting,
            1,
            deadline,
            &child,
            deadline,
        )
        .unwrap()
        .unwrap();
        assert_eq!(timeout.control.timeout_requested_at, Some(deadline));
        assert!(timeout.failure.is_none());
    }
}
