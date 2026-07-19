use insight_agent_platform::engine::{
    ActivationLifecycle, ActivationState, ActivationTerminationReason, AdmissionState,
    AttemptLifecycle, AttemptNo, AttemptState, CompletionDisposition, EffectEvidence,
    EffectIdempotency, LeaseEpoch, LeaseFence, RunState, TerminationClaim, TerminationIntent,
    TerminationReason,
};

const MAX_RUN_DEPTH: usize = 5;
const MAX_ACTIVATION_DEPTH: usize = 5;
const MAX_ATTEMPT_DEPTH: usize = 4;

fn epoch(value: u64) -> LeaseEpoch {
    LeaseEpoch::new(value).expect("model epochs start at one")
}

fn dummy_fence() -> LeaseFence {
    LeaseFence::new(AttemptNo::FIRST, epoch(99)).unwrap()
}

#[derive(Debug, Clone, Copy)]
enum RunOperation {
    Start,
    EnterWaiting,
    LeaveWaiting,
    Pause,
    Resume,
    BeginCompleting,
    ClaimFailure,
    ClaimCancelled,
    ClaimInterrupted,
    ClaimTimedOut,
    FinishDrain,
    FinishSuccess,
    FinishTermination,
}

const RUN_OPERATIONS: [RunOperation; 13] = [
    RunOperation::Start,
    RunOperation::EnterWaiting,
    RunOperation::LeaveWaiting,
    RunOperation::Pause,
    RunOperation::Resume,
    RunOperation::BeginCompleting,
    RunOperation::ClaimFailure,
    RunOperation::ClaimCancelled,
    RunOperation::ClaimInterrupted,
    RunOperation::ClaimTimedOut,
    RunOperation::FinishDrain,
    RunOperation::FinishSuccess,
    RunOperation::FinishTermination,
];

#[derive(Debug, Clone, Default)]
struct RunOracle {
    first_intent: Option<TerminationIntent>,
}

#[derive(Debug, Clone, Copy)]
enum AppliedRunOperation {
    Ordinary,
    Termination(TerminationClaim),
}

fn apply_run_operation(
    run: &mut RunState,
    operation: RunOperation,
) -> Result<AppliedRunOperation, insight_agent_platform::engine::ModelError> {
    match operation {
        RunOperation::Start => run.start().map(|()| AppliedRunOperation::Ordinary),
        RunOperation::EnterWaiting => run.enter_waiting().map(|()| AppliedRunOperation::Ordinary),
        RunOperation::LeaveWaiting => run.leave_waiting().map(|()| AppliedRunOperation::Ordinary),
        RunOperation::Pause => run.pause().map(|_| AppliedRunOperation::Ordinary),
        RunOperation::Resume => run.resume().map(|_| AppliedRunOperation::Ordinary),
        RunOperation::BeginCompleting => run
            .begin_completing()
            .map(|()| AppliedRunOperation::Ordinary),
        RunOperation::ClaimFailure => run
            .request_termination(TerminationReason::Failure)
            .map(AppliedRunOperation::Termination),
        RunOperation::ClaimCancelled => run
            .request_termination(TerminationReason::Cancelled)
            .map(AppliedRunOperation::Termination),
        RunOperation::ClaimInterrupted => run
            .request_termination(TerminationReason::Interrupted)
            .map(AppliedRunOperation::Termination),
        RunOperation::ClaimTimedOut => run
            .request_termination(TerminationReason::TimedOut)
            .map(AppliedRunOperation::Termination),
        RunOperation::FinishDrain => run.finish_drain().map(|_| AppliedRunOperation::Ordinary),
        RunOperation::FinishSuccess => run.finish_success().map(|()| AppliedRunOperation::Ordinary),
        RunOperation::FinishTermination => run
            .finish_termination()
            .map(|_| AppliedRunOperation::Ordinary),
    }
}

fn assert_run_invariants(run: &RunState, oracle: &RunOracle, trace: &[RunOperation]) {
    run.validate()
        .unwrap_or_else(|error| panic!("invalid Run after {trace:?}: {error}"));
    if run.lifecycle().is_terminal() {
        assert_eq!(
            run.admission(),
            AdmissionState::Closed,
            "terminal Run retained non-closed admission after {trace:?}"
        );
    }
    assert_eq!(
        run.termination_intent(),
        oracle.first_intent,
        "termination first-winner diverged after {trace:?}"
    );
}

fn enumerate_run_sequences(
    run: RunState,
    oracle: RunOracle,
    trace: &mut Vec<RunOperation>,
    remaining: usize,
) {
    assert_run_invariants(&run, &oracle, trace);
    if remaining == 0 {
        return;
    }

    for operation in RUN_OPERATIONS {
        let before = run.clone();
        let before_lifecycle = before.lifecycle();
        let mut next = before.clone();
        let mut next_oracle = oracle.clone();
        let result = apply_run_operation(&mut next, operation);

        match result {
            Ok(AppliedRunOperation::Termination(claim)) => match claim {
                TerminationClaim::Claimed(intent) => {
                    assert!(
                        next_oracle.first_intent.is_none(),
                        "a second intent claimed first-winner after {trace:?} + {operation:?}"
                    );
                    next_oracle.first_intent = Some(intent);
                }
                TerminationClaim::Existing(intent) => assert_eq!(
                    next_oracle.first_intent,
                    Some(intent),
                    "termination loser did not observe the first intent after {trace:?} + {operation:?}"
                ),
            },
            Ok(AppliedRunOperation::Ordinary) => {}
            Err(_) => assert_eq!(
                next, before,
                "rejected Run transition mutated state after {trace:?} + {operation:?}"
            ),
        }

        if before_lifecycle.is_terminal() {
            assert!(
                result.is_err(),
                "terminal Run accepted {operation:?} after {trace:?}"
            );
            assert_eq!(next, before, "terminal Run reopened after {trace:?}");
        }
        if matches!(operation, RunOperation::Pause | RunOperation::Resume) && result.is_ok() {
            assert_eq!(
                next.lifecycle(),
                before_lifecycle,
                "pause/resume changed business lifecycle after {trace:?} + {operation:?}"
            );
        }

        trace.push(operation);
        enumerate_run_sequences(next, next_oracle, trace, remaining - 1);
        trace.pop();
    }
}

#[test]
fn bounded_run_operation_sequences_preserve_lifecycle_contracts() {
    enumerate_run_sequences(
        RunState::new(),
        RunOracle::default(),
        &mut Vec::with_capacity(MAX_RUN_DEPTH),
        MAX_RUN_DEPTH,
    );
}

#[derive(Debug, Clone, Copy)]
enum ActivationOperation {
    MakeReady,
    ClaimEpoch1,
    ClaimEpoch2,
    ClaimEpoch3,
    MarkRunning,
    RecordEffectStarted,
    CompleteCurrent,
    ExpireCurrentLease,
    RetryDue,
    BeginWait,
    ResolveWait,
    CompleteNative,
    BeginCancelledTermination,
    FinishTermination,
    CompleteStale,
}

const ACTIVATION_OPERATIONS: [ActivationOperation; 15] = [
    ActivationOperation::MakeReady,
    ActivationOperation::ClaimEpoch1,
    ActivationOperation::ClaimEpoch2,
    ActivationOperation::ClaimEpoch3,
    ActivationOperation::MarkRunning,
    ActivationOperation::RecordEffectStarted,
    ActivationOperation::CompleteCurrent,
    ActivationOperation::ExpireCurrentLease,
    ActivationOperation::RetryDue,
    ActivationOperation::BeginWait,
    ActivationOperation::ResolveWait,
    ActivationOperation::CompleteNative,
    ActivationOperation::BeginCancelledTermination,
    ActivationOperation::FinishTermination,
    ActivationOperation::CompleteStale,
];

#[derive(Debug, Clone, Default)]
struct ActivationOracle {
    claimed: Vec<LeaseFence>,
}

fn selected_fence(activation: &ActivationState, oracle: &ActivationOracle) -> LeaseFence {
    activation
        .current_fence()
        .or_else(|| oracle.claimed.last().copied())
        .unwrap_or_else(dummy_fence)
}

fn stale_fence(activation: &ActivationState, oracle: &ActivationOracle) -> LeaseFence {
    oracle
        .claimed
        .iter()
        .copied()
        .find(|fence| Some(*fence) != activation.current_fence())
        .unwrap_or_else(dummy_fence)
}

fn apply_activation_operation(
    activation: &mut ActivationState,
    oracle: &mut ActivationOracle,
    operation: ActivationOperation,
) -> Result<(), insight_agent_platform::engine::ModelError> {
    match operation {
        ActivationOperation::MakeReady => activation.make_ready(),
        ActivationOperation::ClaimEpoch1
        | ActivationOperation::ClaimEpoch2
        | ActivationOperation::ClaimEpoch3 => {
            let value = match operation {
                ActivationOperation::ClaimEpoch1 => 1,
                ActivationOperation::ClaimEpoch2 => 2,
                ActivationOperation::ClaimEpoch3 => 3,
                _ => unreachable!("claim operation was matched above"),
            };
            activation.claim(epoch(value)).map(|fence| {
                if let Some(previous) = oracle.claimed.last() {
                    assert!(fence.attempt_no() > previous.attempt_no());
                    assert!(fence.lease_epoch() > previous.lease_epoch());
                } else {
                    assert_eq!(fence.attempt_no(), AttemptNo::FIRST);
                }
                oracle.claimed.push(fence);
            })
        }
        ActivationOperation::MarkRunning => {
            activation.mark_running(selected_fence(activation, oracle))
        }
        ActivationOperation::RecordEffectStarted => activation
            .record_effect_evidence(selected_fence(activation, oracle), EffectEvidence::Started)
            .map(|_| ()),
        ActivationOperation::CompleteCurrent => {
            activation.complete_success(selected_fence(activation, oracle))
        }
        ActivationOperation::ExpireCurrentLease => {
            let fence = selected_fence(activation, oracle);
            let evidence = activation.effect_evidence().after_lease_loss();
            activation.lease_expired(fence, evidence).map(|_| ())
        }
        ActivationOperation::RetryDue => activation.retry_due(),
        ActivationOperation::BeginWait => activation.begin_wait(),
        ActivationOperation::ResolveWait => activation.resolve_wait(),
        ActivationOperation::CompleteNative => activation.complete_native(),
        ActivationOperation::BeginCancelledTermination => {
            activation.begin_termination(ActivationTerminationReason::Cancelled)
        }
        ActivationOperation::FinishTermination => activation.finish_termination().map(|_| ()),
        ActivationOperation::CompleteStale => {
            activation.complete_success(stale_fence(activation, oracle))
        }
    }
}

fn assert_activation_invariants(
    activation: &ActivationState,
    oracle: &ActivationOracle,
    trace: &[ActivationOperation],
) {
    for windows in oracle.claimed.windows(2) {
        assert!(
            windows[1].attempt_no() > windows[0].attempt_no(),
            "AttemptNo did not increase after {trace:?}"
        );
        assert!(
            windows[1].lease_epoch() > windows[0].lease_epoch(),
            "LeaseEpoch did not increase after {trace:?}"
        );
    }
    for fence in &oracle.claimed {
        if Some(*fence) != activation.current_fence() {
            assert_ne!(
                activation.classify_completion(*fence),
                CompletionDisposition::Current,
                "old lease remained current after {trace:?}"
            );
        }
    }
    if matches!(
        activation.lifecycle(),
        ActivationLifecycle::RetryWait
            | ActivationLifecycle::Waiting
            | ActivationLifecycle::Terminating
            | ActivationLifecycle::Succeeded
            | ActivationLifecycle::Failed
            | ActivationLifecycle::Cancelled
            | ActivationLifecycle::TimedOut
    ) {
        assert_eq!(
            activation.current_fence(),
            None,
            "non-running Activation retained a live fence after {trace:?}"
        );
    }
}

fn enumerate_activation_sequences(
    activation: ActivationState,
    oracle: ActivationOracle,
    trace: &mut Vec<ActivationOperation>,
    remaining: usize,
) {
    assert_activation_invariants(&activation, &oracle, trace);
    if remaining == 0 {
        return;
    }

    for operation in ACTIVATION_OPERATIONS {
        let before = activation.clone();
        let before_oracle = oracle.clone();
        let before_terminal = before.lifecycle().is_terminal();
        let mut next = before.clone();
        let mut next_oracle = before_oracle.clone();
        let result = apply_activation_operation(&mut next, &mut next_oracle, operation);

        if result.is_err() {
            assert_eq!(
                next, before,
                "rejected Activation operation mutated state after {trace:?} + {operation:?}"
            );
            assert_eq!(
                next_oracle.claimed, before_oracle.claimed,
                "rejected claim mutated the model oracle after {trace:?} + {operation:?}"
            );
        }
        if before_terminal {
            assert!(
                result.is_err(),
                "terminal Activation accepted {operation:?} after {trace:?}"
            );
            assert_eq!(next, before, "terminal Activation reopened after {trace:?}");
        }
        if matches!(operation, ActivationOperation::CompleteStale) {
            assert!(
                result.is_err(),
                "stale completion advanced Activation after {trace:?}"
            );
            assert_eq!(next, before);
        }

        trace.push(operation);
        enumerate_activation_sequences(next, next_oracle, trace, remaining - 1);
        trace.pop();
    }
}

#[test]
fn bounded_activation_sequences_fence_old_leases_and_keep_counters_monotonic() {
    for idempotency in [
        EffectIdempotency::Idempotent,
        EffectIdempotency::NonIdempotent,
    ] {
        enumerate_activation_sequences(
            ActivationState::new(idempotency),
            ActivationOracle::default(),
            &mut Vec::with_capacity(MAX_ACTIVATION_DEPTH),
            MAX_ACTIVATION_DEPTH,
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum AttemptOperation {
    Lease,
    MarkRunning,
    RecordEffectStarted,
    RecordEffectCommitted,
    CompleteSuccess,
    CompleteFailure,
    CompleteTimeout,
    Cancel,
    Abandon,
    CompleteWithWrongFence,
}

const ATTEMPT_OPERATIONS: [AttemptOperation; 10] = [
    AttemptOperation::Lease,
    AttemptOperation::MarkRunning,
    AttemptOperation::RecordEffectStarted,
    AttemptOperation::RecordEffectCommitted,
    AttemptOperation::CompleteSuccess,
    AttemptOperation::CompleteFailure,
    AttemptOperation::CompleteTimeout,
    AttemptOperation::Cancel,
    AttemptOperation::Abandon,
    AttemptOperation::CompleteWithWrongFence,
];

fn apply_attempt_operation(
    attempt: &mut AttemptState,
    operation: AttemptOperation,
) -> Result<(), insight_agent_platform::engine::ModelError> {
    let fence = attempt.fence();
    match operation {
        AttemptOperation::Lease => attempt.lease(),
        AttemptOperation::MarkRunning => attempt.mark_running(fence),
        AttemptOperation::RecordEffectStarted => attempt
            .record_effect_evidence(fence, EffectEvidence::Started)
            .map(|_| ()),
        AttemptOperation::RecordEffectCommitted => attempt
            .record_effect_evidence(fence, EffectEvidence::Committed)
            .map(|_| ()),
        AttemptOperation::CompleteSuccess => attempt.complete_success(fence),
        AttemptOperation::CompleteFailure => attempt.complete_failure(fence),
        AttemptOperation::CompleteTimeout => attempt.complete_timeout(fence),
        AttemptOperation::Cancel => attempt.cancel(fence),
        AttemptOperation::Abandon => attempt.abandon_after_lease_expiry(fence).map(|_| ()),
        AttemptOperation::CompleteWithWrongFence => attempt.complete_success(dummy_fence()),
    }
}

fn enumerate_attempt_sequences(
    attempt: AttemptState,
    trace: &mut Vec<AttemptOperation>,
    remaining: usize,
) {
    let fence = attempt.fence();
    if attempt.lifecycle().is_terminal() {
        let expected = match attempt.lifecycle() {
            AttemptLifecycle::Succeeded | AttemptLifecycle::Failed | AttemptLifecycle::TimedOut => {
                CompletionDisposition::AlreadyApplied
            }
            AttemptLifecycle::Abandoned | AttemptLifecycle::Cancelled => {
                CompletionDisposition::Fenced
            }
            AttemptLifecycle::Created | AttemptLifecycle::Leased | AttemptLifecycle::Running => {
                unreachable!("terminal predicate is exhaustive")
            }
        };
        assert_eq!(attempt.classify_completion(fence), expected);
    }
    if remaining == 0 {
        return;
    }

    for operation in ATTEMPT_OPERATIONS {
        let before = attempt.clone();
        let before_terminal = before.lifecycle().is_terminal();
        let mut next = before.clone();
        let result = apply_attempt_operation(&mut next, operation);

        assert_eq!(
            next.fence(),
            before.fence(),
            "Attempt identity changed after {trace:?} + {operation:?}"
        );
        if result.is_err() {
            assert_eq!(
                next, before,
                "rejected Attempt operation mutated state after {trace:?} + {operation:?}"
            );
        }
        if before_terminal {
            assert!(
                result.is_err(),
                "terminal Attempt accepted {operation:?} after {trace:?}"
            );
            assert_eq!(next, before, "terminal Attempt reopened after {trace:?}");
        }
        if matches!(operation, AttemptOperation::CompleteWithWrongFence) {
            assert!(
                result.is_err(),
                "wrong-fence completion advanced Attempt after {trace:?}"
            );
            assert_eq!(next, before);
        }

        trace.push(operation);
        enumerate_attempt_sequences(next, trace, remaining - 1);
        trace.pop();
    }
}

#[test]
fn bounded_attempt_sequences_preserve_fence_and_terminal_immutability() {
    let fence = LeaseFence::new(AttemptNo::FIRST, epoch(1)).unwrap();
    enumerate_attempt_sequences(
        AttemptState::new(fence),
        &mut Vec::with_capacity(MAX_ATTEMPT_DEPTH),
        MAX_ATTEMPT_DEPTH,
    );
}
