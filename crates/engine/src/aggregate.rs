use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    ActivationId, ActivationLifecycle, ActivationState, ActivationTerminationReason,
    AttemptLifecycle, AttemptNo, AttemptState, ContentHash, EffectEvidence, EffectId,
    EffectIdempotency, ExecutionEventId, IntentHash, InternalFailureSummary, LeaseEpoch,
    LeaseExpiryDisposition, LeaseFence, ModelError, NodeId, RunId, ScopeInstanceId, SignalId,
    TimerId, TransitionKey, TransitionOutcome, ValueRef,
};

pub const AGGREGATE_ATTEMPT_MISSING: &str = "ENGINE_AGGREGATE_ATTEMPT_MISSING";
pub const AGGREGATE_ATTEMPT_CONFLICT: &str = "ENGINE_AGGREGATE_ATTEMPT_CONFLICT";
pub const AGGREGATE_INTENT_CONFLICT: &str = "ENGINE_AGGREGATE_INTENT_CONFLICT";
pub const AGGREGATE_STATE_DIVERGED: &str = "ENGINE_AGGREGATE_STATE_DIVERGED";
pub const AGGREGATE_EXECUTION_KIND_MISMATCH: &str = "ENGINE_AGGREGATE_EXECUTION_KIND_MISMATCH";
pub const AGGREGATE_PROOF_MISMATCH: &str = "ENGINE_AGGREGATE_PROOF_MISMATCH";
pub const AGGREGATE_PROOF_EARLY: &str = "ENGINE_AGGREGATE_PROOF_EARLY";
pub const AGGREGATE_RETRY_POLICY_INVALID: &str = "ENGINE_AGGREGATE_RETRY_POLICY_INVALID";
pub const AGGREGATE_RETRY_PROOF_REQUIRED: &str = "ENGINE_AGGREGATE_RETRY_PROOF_REQUIRED";
pub const AGGREGATE_OUTPUT_INVALID: &str = "ENGINE_AGGREGATE_OUTPUT_INVALID";
pub const AGGREGATE_CANCELLATION_NOT_PENDING: &str = "ENGINE_AGGREGATE_CANCELLATION_NOT_PENDING";
pub const AGGREGATE_TERMINAL_PROOF_BLOCKED: &str = "ENGINE_AGGREGATE_TERMINAL_PROOF_BLOCKED";
pub const TERMINAL_PROOF_NOT_TERMINAL: &str = "ENGINE_TERMINAL_PROOF_NOT_TERMINAL";
pub const TERMINAL_PROOF_ATTEMPT_LIVE: &str = "ENGINE_TERMINAL_PROOF_ATTEMPT_LIVE";
pub const TERMINAL_PROOF_RESULT_MISMATCH: &str = "ENGINE_TERMINAL_PROOF_RESULT_MISMATCH";
pub const AGGREGATE_WAIT_PROOF_INVALID: &str = "ENGINE_AGGREGATE_WAIT_PROOF_INVALID";
pub const AGGREGATE_COMMAND_INTENT_SCHEMA_VERSION: u32 = 1;

/// Cancellation behavior frozen from the published node descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerCancellation {
    Cooperative,
    LeaseOnly,
}

/// Closed effect taxonomy frozen at deployment publication.  This is not
/// inferred again from a mutable adapter registry while a Run is executing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerEffectClass {
    Pure,
    ReadOnly,
    Mutating,
}

/// Complete worker effect contract frozen for the lifetime of one
/// Activation.  Retry and deadline decisions must consume this value from the
/// durable DispatchTask intent; runtime configuration and adapter metadata are
/// not consulted again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkerExecutionPolicy {
    effect_class: WorkerEffectClass,
    effect_idempotency: EffectIdempotency,
    max_attempts: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    timeout_ms: u64,
    cancellation: WorkerCancellation,
}

impl WorkerExecutionPolicy {
    /// Compatibility constructor for aggregate-level callers which do not
    /// execute a real worker. Production publication uses [`Self::frozen`].
    pub fn new(
        effect_idempotency: EffectIdempotency,
        max_attempts: u32,
        cancellation: WorkerCancellation,
    ) -> Result<Self, ModelError> {
        Self::frozen(
            WorkerEffectClass::Mutating,
            effect_idempotency,
            max_attempts,
            0,
            0,
            60_000,
            cancellation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn frozen(
        effect_class: WorkerEffectClass,
        effect_idempotency: EffectIdempotency,
        max_attempts: u32,
        initial_backoff_ms: u64,
        max_backoff_ms: u64,
        timeout_ms: u64,
        cancellation: WorkerCancellation,
    ) -> Result<Self, ModelError> {
        if max_attempts == 0 {
            return Err(ModelError::new(
                AGGREGATE_RETRY_POLICY_INVALID,
                "worker retry policy must permit at least one attempt",
            ));
        }
        if max_backoff_ms < initial_backoff_ms {
            return Err(ModelError::new(
                AGGREGATE_RETRY_POLICY_INVALID,
                "worker retry policy must have monotonic backoff bounds",
            ));
        }
        if timeout_ms == 0 {
            return Err(ModelError::new(
                AGGREGATE_RETRY_POLICY_INVALID,
                "worker timeout must be positive",
            ));
        }
        Ok(Self {
            effect_class,
            effect_idempotency,
            max_attempts,
            initial_backoff_ms,
            max_backoff_ms,
            timeout_ms,
            cancellation,
        })
    }

    pub fn effect_class(&self) -> WorkerEffectClass {
        self.effect_class
    }

    pub fn effect_idempotency(&self) -> EffectIdempotency {
        self.effect_idempotency
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub fn initial_backoff_ms(&self) -> u64 {
        self.initial_backoff_ms
    }

    pub fn max_backoff_ms(&self) -> u64 {
        self.max_backoff_ms
    }

    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    pub fn cancellation(&self) -> WorkerCancellation {
        self.cancellation
    }
}

impl<'de> Deserialize<'de> for WorkerExecutionPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            effect_class: WorkerEffectClass,
            effect_idempotency: EffectIdempotency,
            max_attempts: u32,
            initial_backoff_ms: u64,
            max_backoff_ms: u64,
            timeout_ms: u64,
            cancellation: WorkerCancellation,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::frozen(
            wire.effect_class,
            wire.effect_idempotency,
            wire.max_attempts,
            wire.initial_backoff_ms,
            wire.max_backoff_ms,
            wire.timeout_ms,
            wire.cancellation,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Preferred public name. The old name remains the concrete type while the
/// aggregate model is being cut over in the same v3 branch.
pub type WorkerEffectPolicy = WorkerExecutionPolicy;

/// An Activation has exactly one execution capability for its entire life.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "policy", rename_all = "snake_case")]
pub enum ExecutionKind {
    Worker(WorkerExecutionPolicy),
    SchedulerNative,
    DurableWait,
}

impl ExecutionKind {
    #[allow(dead_code)] // Activation construction is scheduler/repository internal.
    fn effect_idempotency(&self) -> EffectIdempotency {
        match self {
            Self::Worker(policy) => policy.effect_idempotency,
            Self::SchedulerNative | Self::DurableWait => EffectIdempotency::Idempotent,
        }
    }

    fn worker_policy(&self) -> Option<&WorkerExecutionPolicy> {
        match self {
            Self::Worker(policy) => Some(policy),
            Self::SchedulerNative | Self::DurableWait => None,
        }
    }
}

/// Repository-owned claim fact. It has no serde implementation, so a worker
/// cannot manufacture a lease by replaying a storage row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseGrantProof {
    run_id: RunId,
    activation_id: ActivationId,
    lease_epoch: LeaseEpoch,
    lease_timer_id: TimerId,
    lease_deadline: DateTime<Utc>,
}

impl LeaseGrantProof {
    #[allow(dead_code)] // Stage 3 repositories mint this authority.
    pub(crate) fn mint(
        run_id: RunId,
        activation_id: ActivationId,
        lease_epoch: LeaseEpoch,
        lease_timer_id: TimerId,
        lease_deadline: DateTime<Utc>,
    ) -> Self {
        Self {
            run_id,
            activation_id,
            lease_epoch,
            lease_timer_id,
            lease_deadline,
        }
    }

    pub fn lease_epoch(&self) -> LeaseEpoch {
        self.lease_epoch
    }

    pub fn lease_deadline(&self) -> &DateTime<Utc> {
        &self.lease_deadline
    }
}

/// Repository clock proof that the exact current lease timer became due.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseExpiryProof {
    run_id: RunId,
    activation_id: ActivationId,
    fence: LeaseFence,
    lease_timer_id: TimerId,
    lease_deadline: DateTime<Utc>,
    observed_at: DateTime<Utc>,
}

impl LeaseExpiryProof {
    #[allow(dead_code)] // Stage 3 repositories mint this authority.
    pub(crate) fn mint(
        run_id: RunId,
        activation_id: ActivationId,
        fence: LeaseFence,
        lease_timer_id: TimerId,
        lease_deadline: DateTime<Utc>,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, ModelError> {
        if observed_at < lease_deadline {
            return Err(ModelError::new(
                AGGREGATE_PROOF_EARLY,
                "lease expiry proof was minted before its durable deadline",
            ));
        }
        Ok(Self {
            run_id,
            activation_id,
            fence,
            lease_timer_id,
            lease_deadline,
            observed_at,
        })
    }

    pub fn fence(&self) -> LeaseFence {
        self.fence
    }

    pub fn observed_at(&self) -> &DateTime<Utc> {
        &self.observed_at
    }
}

/// Repository-owned retry schedule committed with the failed/abandoned
/// Attempt. A public caller cannot choose a timer or inflate its budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryScheduleProof {
    run_id: RunId,
    activation_id: ActivationId,
    previous_fence: LeaseFence,
    retry_timer_id: TimerId,
    retry_at: DateTime<Utc>,
    remaining_attempt_budget: u32,
}

impl RetryScheduleProof {
    #[allow(dead_code)] // Stage 3 repositories mint this authority.
    pub(crate) fn mint(
        run_id: RunId,
        activation_id: ActivationId,
        previous_fence: LeaseFence,
        retry_timer_id: TimerId,
        retry_at: DateTime<Utc>,
        remaining_attempt_budget: u32,
    ) -> Result<Self, ModelError> {
        if remaining_attempt_budget == 0 {
            return Err(ModelError::new(
                AGGREGATE_RETRY_POLICY_INVALID,
                "retry schedule cannot carry an exhausted attempt budget",
            ));
        }
        Ok(Self {
            run_id,
            activation_id,
            previous_fence,
            retry_timer_id,
            retry_at,
            remaining_attempt_budget,
        })
    }
}

/// Repository clock proof for the exact persisted retry timer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryTimerProof {
    run_id: RunId,
    activation_id: ActivationId,
    previous_fence: LeaseFence,
    retry_timer_id: TimerId,
    retry_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    remaining_attempt_budget: u32,
}

impl RetryTimerProof {
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)] // Stage 3 repositories mint this authority.
    pub(crate) fn mint(
        run_id: RunId,
        activation_id: ActivationId,
        previous_fence: LeaseFence,
        retry_timer_id: TimerId,
        retry_at: DateTime<Utc>,
        observed_at: DateTime<Utc>,
        remaining_attempt_budget: u32,
    ) -> Result<Self, ModelError> {
        if observed_at < retry_at {
            return Err(ModelError::new(
                AGGREGATE_PROOF_EARLY,
                "retry timer proof was minted before its durable deadline",
            ));
        }
        if remaining_attempt_budget == 0 {
            return Err(ModelError::new(
                AGGREGATE_RETRY_POLICY_INVALID,
                "retry timer cannot carry an exhausted attempt budget",
            ));
        }
        Ok(Self {
            run_id,
            activation_id,
            previous_fence,
            retry_timer_id,
            retry_at,
            observed_at,
            remaining_attempt_budget,
        })
    }
}

/// Output reference committed by the repository in the same transaction as
/// the terminal checkpoint. It cannot be constructed or deserialized by a
/// worker/API caller.
#[derive(Debug, Clone, PartialEq)]
pub struct CommittedOutputProof {
    run_id: RunId,
    activation_id: ActivationId,
    fence: Option<LeaseFence>,
    value: ValueRef,
    content_hash: ContentHash,
}

impl CommittedOutputProof {
    #[allow(dead_code)] // Stage 3 repositories mint this authority.
    pub(crate) fn mint(
        run_id: RunId,
        activation_id: ActivationId,
        fence: Option<LeaseFence>,
        value: ValueRef,
    ) -> Self {
        let content_hash = value.content_hash().clone();
        Self {
            run_id,
            activation_id,
            fence,
            value,
            content_hash,
        }
    }

    pub fn value(&self) -> &ValueRef {
        &self.value
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }
}

/// Durable subject whose first committed delivery resolved a wait. The
/// repository validates the subject against the registration before minting
/// `WaitResolutionProof`; callers cannot turn this descriptor into authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum WaitResolutionSubject {
    Signal(SignalId),
    Timer(TimerId),
}

/// Repository-owned first-winner fact for one durable wait registration. It
/// binds the registration, delivered subject, winning ledger transition and
/// committed event to the exact output checkpoint.
#[derive(Debug, PartialEq)]
pub struct WaitResolutionProof {
    run_id: RunId,
    activation_id: ActivationId,
    registration_transition_key: TransitionKey,
    subject: WaitResolutionSubject,
    winning_transition_key: TransitionKey,
    winning_event_id: ExecutionEventId,
    output: CommittedOutputProof,
}

impl WaitResolutionProof {
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)] // Stage 3 repositories mint this first-winner authority.
    pub(crate) fn mint(
        run_id: RunId,
        activation_id: ActivationId,
        registration_transition_key: TransitionKey,
        subject: WaitResolutionSubject,
        winning_transition_key: TransitionKey,
        winning_event_id: ExecutionEventId,
        output: CommittedOutputProof,
    ) -> Result<Self, ModelError> {
        if output.run_id != run_id
            || output.activation_id != activation_id
            || output.fence.is_some()
            || output.value.content_hash() != &output.content_hash
        {
            return Err(ModelError::new(
                AGGREGATE_WAIT_PROOF_INVALID,
                "wait resolution proof does not own its unfenced committed output",
            ));
        }
        Ok(Self {
            run_id,
            activation_id,
            registration_transition_key,
            subject,
            winning_transition_key,
            winning_event_id,
            output,
        })
    }

    pub fn registration_transition_key(&self) -> &TransitionKey {
        &self.registration_transition_key
    }

    pub fn subject(&self) -> &WaitResolutionSubject {
        &self.subject
    }

    pub fn winning_transition_key(&self) -> &TransitionKey {
        &self.winning_transition_key
    }

    pub fn winning_event_id(&self) -> &ExecutionEventId {
        &self.winning_event_id
    }

    pub fn output(&self) -> &CommittedOutputProof {
        &self.output
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaseRecord {
    fence: LeaseFence,
    timer_id: TimerId,
    deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetryWaitRecord {
    previous_fence: LeaseFence,
    timer_id: TimerId,
    retry_at: DateTime<Utc>,
    remaining_attempt_budget: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CancellationRecord {
    fence: LeaseFence,
    reason: ActivationTerminationReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WaitRegistrationRecord {
    transition_key: TransitionKey,
}

#[derive(Debug, Clone, PartialEq)]
struct CommandRecord {
    intent_hash: IntentHash,
    result: AggregateTransitionResult,
}

#[derive(Debug, Clone, PartialEq)]
enum AggregateTransitionResult {
    Unit,
    Bool(bool),
    Fence(LeaseFence),
    FailureDisposition(AttemptFailureDisposition),
    TerminationProgress(TerminationProgress),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum AttemptFailureDisposition {
    RetryScheduled {
        timer_id: TimerId,
        retry_at: DateTime<Utc>,
        remaining_attempt_budget: u32,
    },
    Terminal {
        lifecycle: ActivationLifecycle,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "progress", rename_all = "snake_case")]
pub enum TerminationProgress {
    AwaitingAttemptDrain { fence: LeaseFence },
    Terminal { lifecycle: ActivationLifecycle },
}

/// Capability proving one terminal Activation owns no live Attempt, retry
/// timer, lease, or cancellation drain. Production construction is private to
/// `ActivationAttemptAggregate`; control-flow consumers receive it read-only.
#[derive(Debug, Clone, PartialEq)]
pub enum TerminalActivationResult {
    Succeeded {
        output: ValueRef,
        content_hash: ContentHash,
    },
    Failed {
        reason: ActivationTerminationReason,
        failure: Option<InternalFailureSummary>,
    },
    Cancelled,
    TimedOut,
}

impl TerminalActivationResult {
    pub fn lifecycle(&self) -> ActivationLifecycle {
        match self {
            Self::Succeeded { .. } => ActivationLifecycle::Succeeded,
            Self::Failed { .. } => ActivationLifecycle::Failed,
            Self::Cancelled => ActivationLifecycle::Cancelled,
            Self::TimedOut => ActivationLifecycle::TimedOut,
        }
    }

    pub fn succeeded_output(&self) -> Option<&ValueRef> {
        match self {
            Self::Succeeded { output, .. } => Some(output),
            Self::Failed { .. } | Self::Cancelled | Self::TimedOut => None,
        }
    }

    pub fn succeeded_content_hash(&self) -> Option<&ContentHash> {
        match self {
            Self::Succeeded { content_hash, .. } => Some(content_hash),
            Self::Failed { .. } | Self::Cancelled | Self::TimedOut => None,
        }
    }

    pub fn failure_reason(&self) -> Option<ActivationTerminationReason> {
        match self {
            Self::Failed { reason, .. } => Some(*reason),
            Self::Succeeded { .. } | Self::Cancelled | Self::TimedOut => None,
        }
    }

    pub fn failure_summary(&self) -> Option<&InternalFailureSummary> {
        match self {
            Self::Failed {
                failure: Some(failure),
                ..
            } => Some(failure),
            Self::Failed { failure: None, .. }
            | Self::Succeeded { .. }
            | Self::Cancelled
            | Self::TimedOut => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TerminalActivationProof {
    run_id: RunId,
    scope_instance_id: ScopeInstanceId,
    activation_id: ActivationId,
    terminal: ActivationLifecycle,
    attempts_drained: bool,
    result: TerminalActivationResult,
}

impl TerminalActivationProof {
    pub(crate) fn mint(
        run_id: RunId,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        terminal: ActivationLifecycle,
        attempts_drained: bool,
        result: TerminalActivationResult,
    ) -> Result<Self, ModelError> {
        Self::validate_and_mint(
            run_id,
            scope_instance_id,
            activation_id,
            terminal,
            attempts_drained,
            result,
        )
    }

    fn validate_and_mint(
        run_id: RunId,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        terminal: ActivationLifecycle,
        attempts_drained: bool,
        result: TerminalActivationResult,
    ) -> Result<Self, ModelError> {
        if !terminal.is_terminal() {
            return Err(ModelError::new(
                TERMINAL_PROOF_NOT_TERMINAL,
                "terminal proof requires a terminal Activation lifecycle",
            ));
        }
        if !attempts_drained {
            return Err(ModelError::new(
                TERMINAL_PROOF_ATTEMPT_LIVE,
                "terminal proof requires every owned Attempt to be drained",
            ));
        }
        if result.lifecycle() != terminal
            || matches!(
                &result,
                TerminalActivationResult::Succeeded {
                    output,
                    content_hash
                } if output.content_hash() != content_hash
            )
            || matches!(
                &result,
                TerminalActivationResult::Failed { reason, .. }
                    if !matches!(
                        reason,
                        ActivationTerminationReason::Failure
                            | ActivationTerminationReason::EffectOutcomeUnknown
                    )
            )
        {
            return Err(ModelError::new(
                TERMINAL_PROOF_RESULT_MISMATCH,
                "terminal proof result does not match its lifecycle or content hash",
            ));
        }
        Ok(Self {
            run_id,
            scope_instance_id,
            activation_id,
            terminal,
            attempts_drained,
            result,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn terminal(&self) -> ActivationLifecycle {
        self.terminal
    }

    pub fn attempts_drained(&self) -> bool {
        self.attempts_drained
    }

    pub fn result(&self) -> &TerminalActivationResult {
        &self.result
    }

    pub fn succeeded_output(&self) -> Option<&ValueRef> {
        self.result.succeeded_output()
    }

    pub fn succeeded_content_hash(&self) -> Option<&ContentHash> {
        self.result.succeeded_content_hash()
    }

    pub fn failure_reason(&self) -> Option<ActivationTerminationReason> {
        self.result.failure_reason()
    }

    pub fn failure_summary(&self) -> Option<&InternalFailureSummary> {
        self.result.failure_summary()
    }
}

/// Closed authority for one logical Activation and its entire Attempt history.
/// Every mutating command is internally staged and recorded under a complete
/// intent hash before the new state replaces the authoritative value.
#[derive(Debug)]
pub struct ActivationAttemptAggregate {
    run_id: RunId,
    activation_id: ActivationId,
    node_id: NodeId,
    scope_instance_id: ScopeInstanceId,
    effect_id: EffectId,
    execution_kind: ExecutionKind,
    activation: ActivationState,
    attempts: BTreeMap<AttemptNo, AttemptState>,
    current_lease: Option<LeaseRecord>,
    retry_wait: Option<RetryWaitRecord>,
    wait_registration: Option<WaitRegistrationRecord>,
    cancellation: Option<CancellationRecord>,
    output: Option<ValueRef>,
    failure: Option<InternalFailureSummary>,
    commands: BTreeMap<TransitionKey, CommandRecord>,
}

impl ActivationAttemptAggregate {
    #[allow(dead_code)] // Scheduler admission materializes this aggregate.
    pub(crate) fn new(
        run_id: RunId,
        activation_id: ActivationId,
        node_id: NodeId,
        scope_instance_id: ScopeInstanceId,
        execution_kind: ExecutionKind,
    ) -> Self {
        let effect_id = EffectId::for_activation(&run_id, &activation_id);
        let activation = ActivationState::new(execution_kind.effect_idempotency());
        Self {
            run_id,
            activation_id,
            node_id,
            scope_instance_id,
            effect_id,
            execution_kind,
            activation,
            attempts: BTreeMap::new(),
            current_lease: None,
            retry_wait: None,
            wait_registration: None,
            cancellation: None,
            output: None,
            failure: None,
            commands: BTreeMap::new(),
        }
    }

    /// Internal transaction staging without exposing duplicable aggregate
    /// authority to API callers.
    pub(crate) fn staged(&self) -> Self {
        Self {
            run_id: self.run_id.clone(),
            activation_id: self.activation_id.clone(),
            node_id: self.node_id.clone(),
            scope_instance_id: self.scope_instance_id.clone(),
            effect_id: self.effect_id.clone(),
            execution_kind: self.execution_kind.clone(),
            activation: self.activation.clone(),
            attempts: self.attempts.clone(),
            current_lease: self.current_lease.clone(),
            retry_wait: self.retry_wait.clone(),
            wait_registration: self.wait_registration.clone(),
            cancellation: self.cancellation,
            output: self.output.clone(),
            failure: self.failure.clone(),
            commands: self.commands.clone(),
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }

    pub fn execution_kind(&self) -> &ExecutionKind {
        &self.execution_kind
    }

    pub fn state(&self) -> &ActivationState {
        &self.activation
    }

    pub fn attempt(&self, attempt_no: AttemptNo) -> Option<&AttemptState> {
        self.attempts.get(&attempt_no)
    }

    pub fn attempts(&self) -> impl ExactSizeIterator<Item = &AttemptState> {
        self.attempts.values()
    }

    pub fn output(&self) -> Option<&ValueRef> {
        self.output.as_ref()
    }

    pub fn failure(&self) -> Option<&InternalFailureSummary> {
        self.failure.as_ref()
    }

    pub fn current_attempt(&self) -> Option<&AttemptState> {
        self.current_lease
            .as_ref()
            .and_then(|lease| self.attempts.get(&lease.fence.attempt_no()))
    }

    pub fn make_ready(
        &mut self,
        transition_key: TransitionKey,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
        }
        let intent = Intent {
            operation: "activation.make_ready",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
        };
        let hash = self.intent_hash(&intent)?;
        if let Some(outcome) = self.replay_unit(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if self.activation.lifecycle() != ActivationLifecycle::Created {
            return Ok(TransitionOutcome::StateConflict);
        }
        self.commit(
            transition_key,
            hash,
            AggregateTransitionResult::Unit,
            (),
            |aggregate| aggregate.activation.make_ready(),
        )
    }

    /// Atomically claims the Activation and creates+leases its matching
    /// Attempt. Only a repository-owned lease grant can choose the epoch.
    pub fn claim_worker(
        &mut self,
        transition_key: TransitionKey,
        proof: &LeaseGrantProof,
    ) -> Result<TransitionOutcome<LeaseFence>, ModelError> {
        self.require_worker()?;
        self.require_proof_identity(&proof.run_id, &proof.activation_id)?;
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            lease_epoch: LeaseEpoch,
            timer_id: &'a TimerId,
            deadline: &'a DateTime<Utc>,
        }
        let intent = Intent {
            operation: "attempt.claim",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            lease_epoch: proof.lease_epoch,
            timer_id: &proof.lease_timer_id,
            deadline: &proof.lease_deadline,
        };
        let hash = self.intent_hash(&intent)?;
        if let Some(outcome) = self.replay_fence(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if self.activation.lifecycle() != ActivationLifecycle::Ready
            || self.current_lease.is_some()
            || self.retry_wait.is_some()
            || self.wait_registration.is_some()
            || self.cancellation.is_some()
        {
            return Ok(TransitionOutcome::StateConflict);
        }

        let mut staged = self.staged();
        let fence = staged.activation.claim(proof.lease_epoch)?;
        if staged.attempts.contains_key(&fence.attempt_no()) {
            return Err(ModelError::new(
                AGGREGATE_ATTEMPT_CONFLICT,
                "activation attempt number was already materialized",
            ));
        }
        let mut attempt = AttemptState::new(fence);
        attempt.lease()?;
        staged.attempts.insert(fence.attempt_no(), attempt);
        staged.current_lease = Some(LeaseRecord {
            fence,
            timer_id: proof.lease_timer_id.clone(),
            deadline: proof.lease_deadline,
        });
        staged.install_record(
            transition_key,
            hash,
            AggregateTransitionResult::Fence(fence),
        )?;
        *self = staged;
        Ok(TransitionOutcome::Committed { result: fence })
    }

    pub fn mark_running(
        &mut self,
        transition_key: TransitionKey,
        fence: LeaseFence,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        self.require_worker()?;
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            fence: LeaseFence,
        }
        let hash = self.intent_hash(&Intent {
            operation: "attempt.mark_running",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            fence,
        })?;
        if let Some(outcome) = self.replay_unit(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if !self.is_current_fence(fence) {
            return Ok(TransitionOutcome::StaleLease);
        }
        if self.activation.lifecycle() != ActivationLifecycle::Leased {
            return Ok(TransitionOutcome::StateConflict);
        }
        self.commit(
            transition_key,
            hash,
            AggregateTransitionResult::Unit,
            (),
            |aggregate| {
                aggregate.attempt_mut(fence)?.mark_running(fence)?;
                aggregate.activation.mark_running(fence)
            },
        )
    }

    pub fn record_effect_evidence(
        &mut self,
        transition_key: TransitionKey,
        fence: LeaseFence,
        evidence: EffectEvidence,
    ) -> Result<TransitionOutcome<bool>, ModelError> {
        self.require_worker()?;
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            fence: LeaseFence,
            evidence: EffectEvidence,
        }
        let hash = self.intent_hash(&Intent {
            operation: "attempt.effect_evidence",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            fence,
            evidence,
        })?;
        if let Some(outcome) = self.replay_bool(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if !self.is_current_fence(fence) {
            return Ok(TransitionOutcome::StaleLease);
        }
        if !matches!(
            self.activation.lifecycle(),
            ActivationLifecycle::Leased | ActivationLifecycle::Running
        ) {
            return Ok(TransitionOutcome::StateConflict);
        }

        let mut staged = self.staged();
        let attempt_changed = staged
            .attempt_mut(fence)?
            .record_effect_evidence(fence, evidence)?;
        let activation_changed = staged.activation.record_effect_evidence(fence, evidence)?;
        if attempt_changed != activation_changed {
            return Err(ModelError::new(
                AGGREGATE_STATE_DIVERGED,
                "attempt and activation effect evidence diverged",
            ));
        }
        staged.install_record(
            transition_key,
            hash,
            AggregateTransitionResult::Bool(attempt_changed),
        )?;
        *self = staged;
        Ok(TransitionOutcome::Committed {
            result: attempt_changed,
        })
    }

    pub fn complete_worker_success(
        &mut self,
        transition_key: TransitionKey,
        fence: LeaseFence,
        output: &CommittedOutputProof,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        self.require_worker()?;
        self.validate_output_proof(output, Some(fence))?;
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            fence: LeaseFence,
            output_hash: &'a ContentHash,
        }
        let hash = self.intent_hash(&Intent {
            operation: "attempt.complete_success",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            fence,
            output_hash: &output.content_hash,
        })?;
        if let Some(outcome) = self.replay_unit(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if !self.is_current_fence(fence) || self.cancellation.is_some() {
            return Ok(TransitionOutcome::StaleLease);
        }
        if self.activation.lifecycle() != ActivationLifecycle::Running {
            return Ok(TransitionOutcome::StateConflict);
        }
        let value = output.value.clone();
        self.commit(
            transition_key,
            hash,
            AggregateTransitionResult::Unit,
            (),
            move |aggregate| {
                aggregate.attempt_mut(fence)?.complete_success(fence)?;
                aggregate.activation.complete_success(fence)?;
                aggregate.current_lease = None;
                aggregate.output = Some(value);
                Ok(())
            },
        )
    }

    pub fn complete_worker_failure(
        &mut self,
        transition_key: TransitionKey,
        fence: LeaseFence,
        failure: InternalFailureSummary,
        retry: Option<&RetryScheduleProof>,
    ) -> Result<TransitionOutcome<AttemptFailureDisposition>, ModelError> {
        let policy = self.require_worker()?.clone();
        if let Some(proof) = retry {
            self.validate_retry_schedule(proof, fence, &policy)?;
        }
        #[derive(Serialize)]
        struct RetryIntent<'a> {
            timer_id: &'a TimerId,
            retry_at: &'a DateTime<Utc>,
            remaining_attempt_budget: u32,
        }
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            fence: LeaseFence,
            failure: &'a InternalFailureSummary,
            retry: Option<RetryIntent<'a>>,
        }
        let retry_intent = retry.map(|proof| RetryIntent {
            timer_id: &proof.retry_timer_id,
            retry_at: &proof.retry_at,
            remaining_attempt_budget: proof.remaining_attempt_budget,
        });
        let hash = self.intent_hash(&Intent {
            operation: "attempt.complete_failure",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            fence,
            failure: &failure,
            retry: retry_intent,
        })?;
        if let Some(outcome) = self.replay_failure(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if !self.is_current_fence(fence) || self.cancellation.is_some() {
            return Ok(TransitionOutcome::StaleLease);
        }
        if !matches!(
            self.activation.lifecycle(),
            ActivationLifecycle::Leased | ActivationLifecycle::Running
        ) {
            return Ok(TransitionOutcome::StateConflict);
        }

        let retry_is_safe = self
            .attempt_ref(fence)?
            .effect_evidence()
            .permits_automatic_retry(policy.effect_idempotency);
        let budget = policy.max_attempts - fence.attempt_no().get();
        if retry_is_safe && budget > 0 && retry.is_none() {
            return Err(ModelError::new(
                AGGREGATE_RETRY_PROOF_REQUIRED,
                "retryable failure requires a repository-owned retry schedule",
            ));
        }
        if (!retry_is_safe || budget == 0) && retry.is_some() {
            return Err(ModelError::new(
                AGGREGATE_RETRY_POLICY_INVALID,
                "retry schedule was supplied for an unsafe or exhausted failure",
            ));
        }

        let mut staged = self.staged();
        staged.attempt_mut(fence)?.complete_failure(fence)?;
        let evidence = staged.attempt_ref(fence)?.effect_evidence();
        staged.current_lease = None;
        staged.failure = Some(failure);
        let disposition = if retry_is_safe && budget > 0 {
            let proof = retry.expect("retry proof was required above");
            staged.activation.enter_retry_wait(fence, evidence)?;
            staged.retry_wait = Some(RetryWaitRecord {
                previous_fence: fence,
                timer_id: proof.retry_timer_id.clone(),
                retry_at: proof.retry_at,
                remaining_attempt_budget: proof.remaining_attempt_budget,
            });
            AttemptFailureDisposition::RetryScheduled {
                timer_id: proof.retry_timer_id.clone(),
                retry_at: proof.retry_at,
                remaining_attempt_budget: proof.remaining_attempt_budget,
            }
        } else {
            let reason = if evidence == EffectEvidence::Unknown {
                ActivationTerminationReason::EffectOutcomeUnknown
            } else {
                ActivationTerminationReason::Failure
            };
            staged.activation.begin_termination(reason)?;
            let lifecycle = staged.activation.finish_termination()?;
            AttemptFailureDisposition::Terminal { lifecycle }
        };
        staged.install_record(
            transition_key,
            hash,
            AggregateTransitionResult::FailureDisposition(disposition.clone()),
        )?;
        *self = staged;
        Ok(TransitionOutcome::Committed {
            result: disposition,
        })
    }

    /// Abandons an expired Attempt using the exact repository timer proof.
    /// If cancellation already won, expiry only drains that Attempt and never
    /// reopens the Activation for retry.
    pub fn expire_lease(
        &mut self,
        transition_key: TransitionKey,
        proof: &LeaseExpiryProof,
        retry: Option<&RetryScheduleProof>,
    ) -> Result<TransitionOutcome<AttemptFailureDisposition>, ModelError> {
        let policy = self.require_worker()?.clone();
        // Proof identity is authorization, not a mutable-state precondition;
        // it is always checked before replay and is also bound into the hash.
        self.require_proof_identity(&proof.run_id, &proof.activation_id)?;
        if let Some(schedule) = retry {
            self.require_proof_identity(&schedule.run_id, &schedule.activation_id)?;
        }
        #[derive(Serialize)]
        struct RetryIntent<'a> {
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            previous_fence: LeaseFence,
            timer_id: &'a TimerId,
            retry_at: &'a DateTime<Utc>,
            remaining_attempt_budget: u32,
        }
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            fence: LeaseFence,
            lease_timer_id: &'a TimerId,
            lease_deadline: &'a DateTime<Utc>,
            observed_at: &'a DateTime<Utc>,
            retry: Option<RetryIntent<'a>>,
        }
        let retry_intent = retry.map(|schedule| RetryIntent {
            run_id: &schedule.run_id,
            activation_id: &schedule.activation_id,
            previous_fence: schedule.previous_fence,
            timer_id: &schedule.retry_timer_id,
            retry_at: &schedule.retry_at,
            remaining_attempt_budget: schedule.remaining_attempt_budget,
        });
        let hash = self.intent_hash(&Intent {
            operation: "attempt.lease_expired",
            run_id: &proof.run_id,
            activation_id: &proof.activation_id,
            fence: proof.fence,
            lease_timer_id: &proof.lease_timer_id,
            lease_deadline: &proof.lease_deadline,
            observed_at: &proof.observed_at,
            retry: retry_intent,
        })?;
        if let Some(outcome) = self.replay_failure(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if !self.is_current_fence(proof.fence) {
            return Ok(TransitionOutcome::StaleLease);
        }
        self.validate_lease_expiry(proof)?;
        if let Some(schedule) = retry {
            self.validate_retry_schedule(schedule, proof.fence, &policy)?;
        }
        if self.cancellation.is_some() && retry.is_some() {
            return Err(ModelError::new(
                AGGREGATE_RETRY_POLICY_INVALID,
                "termination drain cannot schedule a new Attempt",
            ));
        }

        let mut staged = self.staged();
        let evidence = staged
            .attempt_mut(proof.fence)?
            .abandon_after_lease_expiry(proof.fence)?;
        staged.current_lease = None;
        let disposition = if let Some(cancellation) = staged.cancellation.take() {
            if cancellation.fence != proof.fence {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "cancellation drain fence differs from the expired lease",
                ));
            }
            let lifecycle = staged.activation.finish_termination()?;
            AttemptFailureDisposition::Terminal { lifecycle }
        } else if evidence.permits_automatic_retry(policy.effect_idempotency)
            && policy.max_attempts == proof.fence.attempt_no().get()
        {
            if retry.is_some() {
                return Err(ModelError::new(
                    AGGREGATE_RETRY_POLICY_INVALID,
                    "exhausted lease loss cannot carry a retry schedule",
                ));
            }
            staged
                .activation
                .begin_termination(ActivationTerminationReason::Failure)?;
            let lifecycle = staged.activation.finish_termination()?;
            AttemptFailureDisposition::Terminal { lifecycle }
        } else {
            let lease_disposition = staged.activation.lease_expired(proof.fence, evidence)?;
            match lease_disposition {
                LeaseExpiryDisposition::RetryWait => {
                    let budget = policy.max_attempts - proof.fence.attempt_no().get();
                    let schedule = retry.ok_or_else(|| {
                        ModelError::new(
                            AGGREGATE_RETRY_PROOF_REQUIRED,
                            "retryable lease expiry requires a durable retry schedule",
                        )
                    })?;
                    if budget == 0 {
                        return Err(ModelError::new(
                            AGGREGATE_RETRY_POLICY_INVALID,
                            "lease expiry cannot retry after exhausting attempts",
                        ));
                    }
                    staged.retry_wait = Some(RetryWaitRecord {
                        previous_fence: proof.fence,
                        timer_id: schedule.retry_timer_id.clone(),
                        retry_at: schedule.retry_at,
                        remaining_attempt_budget: schedule.remaining_attempt_budget,
                    });
                    AttemptFailureDisposition::RetryScheduled {
                        timer_id: schedule.retry_timer_id.clone(),
                        retry_at: schedule.retry_at,
                        remaining_attempt_budget: schedule.remaining_attempt_budget,
                    }
                }
                LeaseExpiryDisposition::RetryBlocked => {
                    if retry.is_some() {
                        return Err(ModelError::new(
                            AGGREGATE_RETRY_POLICY_INVALID,
                            "unsafe lease loss cannot carry a retry schedule",
                        ));
                    }
                    let lifecycle = staged.activation.finish_termination()?;
                    AttemptFailureDisposition::Terminal { lifecycle }
                }
            }
        };
        staged.install_record(
            transition_key,
            hash,
            AggregateTransitionResult::FailureDisposition(disposition.clone()),
        )?;
        *self = staged;
        Ok(TransitionOutcome::Committed {
            result: disposition,
        })
    }

    pub fn retry_due(
        &mut self,
        transition_key: TransitionKey,
        proof: &RetryTimerProof,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        self.require_worker()?;
        self.require_proof_identity(&proof.run_id, &proof.activation_id)?;
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            previous_fence: LeaseFence,
            timer_id: &'a TimerId,
            retry_at: &'a DateTime<Utc>,
            observed_at: &'a DateTime<Utc>,
            remaining_attempt_budget: u32,
        }
        let hash = self.intent_hash(&Intent {
            operation: "activation.retry_due",
            run_id: &proof.run_id,
            activation_id: &proof.activation_id,
            previous_fence: proof.previous_fence,
            timer_id: &proof.retry_timer_id,
            retry_at: &proof.retry_at,
            observed_at: &proof.observed_at,
            remaining_attempt_budget: proof.remaining_attempt_budget,
        })?;
        if let Some(outcome) = self.replay_unit(&transition_key, &hash)? {
            return Ok(outcome);
        }
        self.validate_retry_timer(proof)?;
        if self.activation.lifecycle() != ActivationLifecycle::RetryWait {
            return Ok(TransitionOutcome::StateConflict);
        }
        self.commit(
            transition_key,
            hash,
            AggregateTransitionResult::Unit,
            (),
            |aggregate| {
                aggregate.activation.retry_due()?;
                aggregate.retry_wait = None;
                // Failure details describe the current terminal cause, not an
                // earlier Attempt that the retry policy elected to continue.
                aggregate.failure = None;
                Ok(())
            },
        )
    }

    pub fn begin_wait(
        &mut self,
        transition_key: TransitionKey,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        self.require_kind(
            "durable_wait",
            matches!(self.execution_kind, ExecutionKind::DurableWait),
        )?;
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
        }
        let hash = self.intent_hash(&Intent {
            operation: "activation.begin_wait",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
        })?;
        if let Some(outcome) = self.replay_unit(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if self.activation.lifecycle() != ActivationLifecycle::Ready {
            return Ok(TransitionOutcome::StateConflict);
        }
        let registration_transition_key = transition_key.clone();
        self.commit(
            transition_key,
            hash,
            AggregateTransitionResult::Unit,
            (),
            move |aggregate| {
                aggregate.activation.begin_wait()?;
                aggregate.wait_registration = Some(WaitRegistrationRecord {
                    transition_key: registration_transition_key,
                });
                Ok(())
            },
        )
    }

    pub fn resolve_wait(
        &mut self,
        transition_key: TransitionKey,
        proof: &WaitResolutionProof,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        self.require_kind(
            "durable_wait",
            matches!(self.execution_kind, ExecutionKind::DurableWait),
        )?;
        // Repository authority and winning identity are immutable facts. Check
        // them before replay so a foreign proof can never inherit a local
        // command's exact-replay result.
        self.require_proof_identity(&proof.run_id, &proof.activation_id)?;
        self.validate_output_proof(&proof.output, None)?;
        if proof.winning_transition_key != transition_key {
            return Err(ModelError::new(
                AGGREGATE_WAIT_PROOF_INVALID,
                "wait resolution command does not use the proven winning transition",
            ));
        }
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            registration_transition_key: &'a TransitionKey,
            subject: &'a WaitResolutionSubject,
            winning_transition_key: &'a TransitionKey,
            winning_event_id: &'a ExecutionEventId,
            output_hash: &'a ContentHash,
        }
        let hash = self.intent_hash(&Intent {
            operation: "activation.resolve_wait",
            run_id: &proof.run_id,
            activation_id: &proof.activation_id,
            registration_transition_key: &proof.registration_transition_key,
            subject: &proof.subject,
            winning_transition_key: &proof.winning_transition_key,
            winning_event_id: &proof.winning_event_id,
            output_hash: &proof.output.content_hash,
        })?;
        if let Some(outcome) = self.replay_unit(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if self.activation.lifecycle() != ActivationLifecycle::Waiting {
            return Ok(TransitionOutcome::StateConflict);
        }
        if self
            .wait_registration
            .as_ref()
            .map(|registration| &registration.transition_key)
            != Some(&proof.registration_transition_key)
        {
            return Err(ModelError::new(
                AGGREGATE_WAIT_PROOF_INVALID,
                "wait resolution proof does not match the persisted registration",
            ));
        }
        let value = proof.output.value.clone();
        self.commit(
            transition_key,
            hash,
            AggregateTransitionResult::Unit,
            (),
            move |aggregate| {
                aggregate.activation.resolve_wait()?;
                aggregate.wait_registration = None;
                aggregate.output = Some(value);
                Ok(())
            },
        )
    }

    pub fn complete_native(
        &mut self,
        transition_key: TransitionKey,
        output: &CommittedOutputProof,
    ) -> Result<TransitionOutcome<()>, ModelError> {
        self.require_kind(
            "scheduler_native",
            matches!(self.execution_kind, ExecutionKind::SchedulerNative),
        )?;
        self.validate_output_proof(output, None)?;
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            output_hash: &'a ContentHash,
        }
        let hash = self.intent_hash(&Intent {
            operation: "activation.complete_native",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            output_hash: &output.content_hash,
        })?;
        if let Some(outcome) = self.replay_unit(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if self.activation.lifecycle() != ActivationLifecycle::Ready {
            return Ok(TransitionOutcome::StateConflict);
        }
        let value = output.value.clone();
        self.commit(
            transition_key,
            hash,
            AggregateTransitionResult::Unit,
            (),
            move |aggregate| {
                aggregate.activation.complete_native()?;
                aggregate.output = Some(value);
                Ok(())
            },
        )
    }

    /// First-winner termination intent. A live Attempt remains live until its
    /// matching worker acknowledgement or authoritative lease expiry arrives.
    pub fn request_termination(
        &mut self,
        transition_key: TransitionKey,
        reason: ActivationTerminationReason,
    ) -> Result<TransitionOutcome<TerminationProgress>, ModelError> {
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            reason: ActivationTerminationReason,
        }
        let hash = self.intent_hash(&Intent {
            operation: "activation.request_termination",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            reason,
        })?;
        if let Some(outcome) = self.replay_termination(&transition_key, &hash)? {
            return Ok(outcome);
        }
        if self.activation.lifecycle().is_terminal() || self.cancellation.is_some() {
            return Ok(TransitionOutcome::StateConflict);
        }

        let mut staged = self.staged();
        let live_fence = staged.current_lease.as_ref().map(|lease| lease.fence);
        staged.activation.begin_termination(reason)?;
        let progress = if let Some(fence) = live_fence {
            staged.cancellation = Some(CancellationRecord { fence, reason });
            TerminationProgress::AwaitingAttemptDrain { fence }
        } else {
            // A retry-wait timer is durable work owned by this Activation. A
            // winning termination claim cancels that schedule atomically and
            // must never leave a terminal Activation pointing at a live retry.
            staged.retry_wait = None;
            staged.wait_registration = None;
            let lifecycle = staged.activation.finish_termination()?;
            TerminationProgress::Terminal { lifecycle }
        };
        staged.install_record(
            transition_key,
            hash,
            AggregateTransitionResult::TerminationProgress(progress),
        )?;
        *self = staged;
        Ok(TransitionOutcome::Committed { result: progress })
    }

    /// Cooperative worker acknowledgement for a previously requested
    /// cancellation/timeout. It cannot terminate another or stale Attempt.
    pub fn acknowledge_cancellation(
        &mut self,
        transition_key: TransitionKey,
        fence: LeaseFence,
    ) -> Result<TransitionOutcome<TerminationProgress>, ModelError> {
        let policy = self.require_worker()?;
        if policy.cancellation != WorkerCancellation::Cooperative {
            return Err(ModelError::new(
                AGGREGATE_EXECUTION_KIND_MISMATCH,
                "lease-only worker cannot acknowledge cooperative cancellation",
            ));
        }
        #[derive(Serialize)]
        struct Intent<'a> {
            operation: &'static str,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            fence: LeaseFence,
        }
        let hash = self.intent_hash(&Intent {
            operation: "attempt.acknowledge_cancellation",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            fence,
        })?;
        if let Some(outcome) = self.replay_termination(&transition_key, &hash)? {
            return Ok(outcome);
        }
        let Some(cancellation) = self.cancellation else {
            return Err(ModelError::new(
                AGGREGATE_CANCELLATION_NOT_PENDING,
                "worker cancellation acknowledgement has no matching intent",
            ));
        };
        if cancellation.fence != fence || !self.is_current_fence(fence) {
            return Ok(TransitionOutcome::StaleLease);
        }

        let mut staged = self.staged();
        staged.attempt_mut(fence)?.cancel(fence)?;
        staged.current_lease = None;
        staged.cancellation = None;
        let lifecycle = staged.activation.finish_termination()?;
        let progress = TerminationProgress::Terminal { lifecycle };
        staged.install_record(
            transition_key,
            hash,
            AggregateTransitionResult::TerminationProgress(progress),
        )?;
        *self = staged;
        Ok(TransitionOutcome::Committed { result: progress })
    }

    pub fn terminal_proof(&self) -> Result<TerminalActivationProof, ModelError> {
        self.validate()?;
        if !self.activation.lifecycle().is_terminal()
            || self
                .attempts
                .values()
                .any(|attempt| !attempt.lifecycle().is_terminal())
            || self.current_lease.is_some()
            || self.retry_wait.is_some()
            || self.cancellation.is_some()
        {
            return Err(ModelError::new(
                AGGREGATE_TERMINAL_PROOF_BLOCKED,
                "Activation terminal proof is blocked by live durable work",
            ));
        }
        let terminal = self.activation.lifecycle();
        let result = match terminal {
            ActivationLifecycle::Succeeded => {
                let output = self.output.clone().ok_or_else(|| {
                    ModelError::new(
                        AGGREGATE_OUTPUT_INVALID,
                        "succeeded Activation has no authoritative output",
                    )
                })?;
                TerminalActivationResult::Succeeded {
                    content_hash: output.content_hash().clone(),
                    output,
                }
            }
            ActivationLifecycle::Failed => TerminalActivationResult::Failed {
                reason: self.activation.termination_reason().ok_or_else(|| {
                    ModelError::new(
                        TERMINAL_PROOF_RESULT_MISMATCH,
                        "failed Activation has no authoritative termination reason",
                    )
                })?,
                failure: self.failure.clone(),
            },
            ActivationLifecycle::Cancelled => TerminalActivationResult::Cancelled,
            ActivationLifecycle::TimedOut => TerminalActivationResult::TimedOut,
            _ => {
                return Err(ModelError::new(
                    TERMINAL_PROOF_NOT_TERMINAL,
                    "terminal proof requires a terminal Activation lifecycle",
                ));
            }
        };
        TerminalActivationProof::mint(
            self.run_id.clone(),
            self.scope_instance_id.clone(),
            self.activation_id.clone(),
            terminal,
            true,
            result,
        )
    }

    pub fn validate(&self) -> Result<(), ModelError> {
        self.activation.validate()?;
        if self.effect_id != EffectId::for_activation(&self.run_id, &self.activation_id) {
            return Err(ModelError::new(
                AGGREGATE_STATE_DIVERGED,
                "Activation effect ID does not match its stable identity",
            ));
        }

        let mut expected_attempt = 1_u32;
        let mut previous_epoch = None;
        let last_attempt_no = self.attempts.keys().next_back().copied();
        let mut live_attempt = None;
        for (attempt_no, attempt) in &self.attempts {
            attempt.validate()?;
            if attempt_no.get() != expected_attempt || attempt.fence().attempt_no() != *attempt_no {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "Attempt history is not contiguous or its map key was forged",
                ));
            }
            if previous_epoch.is_some_and(|epoch| attempt.fence().lease_epoch() <= epoch) {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "Attempt history lease epochs are not strictly monotonic",
                ));
            }
            if !attempt.lifecycle().is_terminal()
                && (Some(*attempt_no) != last_attempt_no
                    || live_attempt.replace(*attempt_no).is_some())
            {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "only the latest Attempt may remain live",
                ));
            }
            previous_epoch = Some(attempt.fence().lease_epoch());
            expected_attempt = expected_attempt.checked_add(1).ok_or_else(|| {
                ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "Attempt history counter overflowed",
                )
            })?;
        }

        match &self.execution_kind {
            ExecutionKind::Worker(policy) => {
                if self.attempts.len() > policy.max_attempts as usize {
                    return Err(ModelError::new(
                        AGGREGATE_STATE_DIVERGED,
                        "Attempt history exceeds the frozen retry budget",
                    ));
                }
                if self.activation.lifecycle() == ActivationLifecycle::Waiting {
                    return Err(ModelError::new(
                        AGGREGATE_EXECUTION_KIND_MISMATCH,
                        "worker Activation cannot enter durable-wait lifecycle",
                    ));
                }
            }
            ExecutionKind::SchedulerNative | ExecutionKind::DurableWait => {
                if !self.attempts.is_empty()
                    || self.current_lease.is_some()
                    || self.retry_wait.is_some()
                    || self.cancellation.is_some()
                {
                    return Err(ModelError::new(
                        AGGREGATE_EXECUTION_KIND_MISMATCH,
                        "scheduler-native and wait Activations cannot own worker Attempts",
                    ));
                }
                if matches!(self.execution_kind, ExecutionKind::SchedulerNative)
                    && self.activation.lifecycle() == ActivationLifecycle::Waiting
                {
                    return Err(ModelError::new(
                        AGGREGATE_EXECUTION_KIND_MISMATCH,
                        "scheduler-native Activation cannot enter durable-wait lifecycle",
                    ));
                }
            }
        }

        match &self.execution_kind {
            ExecutionKind::DurableWait => {
                if (self.activation.lifecycle() == ActivationLifecycle::Waiting)
                    != self.wait_registration.is_some()
                {
                    return Err(ModelError::new(
                        AGGREGATE_STATE_DIVERGED,
                        "durable-wait registration does not match the waiting lifecycle",
                    ));
                }
            }
            ExecutionKind::Worker(_) | ExecutionKind::SchedulerNative => {
                if self.wait_registration.is_some() {
                    return Err(ModelError::new(
                        AGGREGATE_EXECUTION_KIND_MISMATCH,
                        "non-wait Activation cannot own a wait registration",
                    ));
                }
            }
        }

        if let Some(last) = self.attempts.values().next_back() {
            if self.activation.last_attempt() != Some(last.fence().attempt_no())
                || self.activation.last_lease_epoch() != Some(last.fence().lease_epoch())
            {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "Activation counters do not match its complete Attempt history",
                ));
            }
        } else if self.activation.last_attempt().is_some()
            || self.activation.last_lease_epoch().is_some()
        {
            return Err(ModelError::new(
                AGGREGATE_STATE_DIVERGED,
                "Activation counters claim an Attempt that is absent from history",
            ));
        }

        if let Some(lease) = &self.current_lease {
            let attempt = self
                .attempts
                .get(&lease.fence.attempt_no())
                .ok_or_else(|| {
                    ModelError::new(
                        AGGREGATE_ATTEMPT_MISSING,
                        "current lease references a missing Attempt",
                    )
                })?;
            if attempt.fence() != lease.fence || attempt.lifecycle().is_terminal() {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "current lease does not identify one live Attempt",
                ));
            }
            let lifecycle_allows_lease = matches!(
                self.activation.lifecycle(),
                ActivationLifecycle::Leased | ActivationLifecycle::Running
            ) || (self.activation.lifecycle()
                == ActivationLifecycle::Terminating
                && self.cancellation.is_some());
            if !lifecycle_allows_lease {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "Activation lifecycle cannot own its recorded live lease",
                ));
            }
        }
        if live_attempt
            != self
                .current_lease
                .as_ref()
                .map(|lease| lease.fence.attempt_no())
        {
            return Err(ModelError::new(
                AGGREGATE_STATE_DIVERGED,
                "live Attempt and durable current lease diverged",
            ));
        }
        if matches!(
            self.activation.lifecycle(),
            ActivationLifecycle::Leased | ActivationLifecycle::Running
        ) && self.activation.current_fence()
            != self.current_lease.as_ref().map(|lease| lease.fence)
        {
            return Err(ModelError::new(
                AGGREGATE_STATE_DIVERGED,
                "Activation fence and durable lease record diverged",
            ));
        }
        if self.cancellation.is_some()
            != (self.activation.lifecycle() == ActivationLifecycle::Terminating
                && self.current_lease.is_some())
        {
            return Err(ModelError::new(
                AGGREGATE_STATE_DIVERGED,
                "termination drain record is inconsistent with the live Attempt",
            ));
        }
        if let Some(cancellation) = self.cancellation {
            if self.current_lease.as_ref().map(|lease| lease.fence) != Some(cancellation.fence) {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "cancellation intent and current lease use different fences",
                ));
            }
            if self.activation.termination_reason() != Some(cancellation.reason) {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "cancellation record and Activation termination reason diverged",
                ));
            }
        }

        if let Some(retry) = &self.retry_wait {
            if self.activation.lifecycle() != ActivationLifecycle::RetryWait
                || retry.remaining_attempt_budget == 0
                || self.current_lease.is_some()
            {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "retry timer record is inconsistent with Activation lifecycle",
                ));
            }
            let last = self
                .attempts
                .get(&retry.previous_fence.attempt_no())
                .ok_or_else(|| {
                    ModelError::new(
                        AGGREGATE_ATTEMPT_MISSING,
                        "retry timer references a missing previous Attempt",
                    )
                })?;
            if last.fence() != retry.previous_fence || !last.lifecycle().is_terminal() {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "retry timer does not follow one terminal Attempt",
                ));
            }
            let expected_budget = self
                .execution_kind
                .worker_policy()
                .and_then(|policy| {
                    policy
                        .max_attempts
                        .checked_sub(retry.previous_fence.attempt_no().get())
                })
                .ok_or_else(|| {
                    ModelError::new(
                        AGGREGATE_STATE_DIVERGED,
                        "retry record has no valid frozen attempt budget",
                    )
                })?;
            if retry.remaining_attempt_budget != expected_budget {
                return Err(ModelError::new(
                    AGGREGATE_STATE_DIVERGED,
                    "retry timer budget differs from the frozen retry policy",
                ));
            }
        } else if self.activation.lifecycle() == ActivationLifecycle::RetryWait {
            return Err(ModelError::new(
                AGGREGATE_STATE_DIVERGED,
                "retry-wait Activation has no durable timer record",
            ));
        }

        if self.activation.lifecycle() == ActivationLifecycle::Succeeded {
            if self.output.is_none() {
                return Err(ModelError::new(
                    AGGREGATE_OUTPUT_INVALID,
                    "succeeded Activation requires an owned durable output",
                ));
            }
            if matches!(self.execution_kind, ExecutionKind::Worker(_))
                && self
                    .attempts
                    .values()
                    .next_back()
                    .is_none_or(|attempt| attempt.lifecycle() != AttemptLifecycle::Succeeded)
            {
                return Err(ModelError::new(
                    AGGREGATE_OUTPUT_INVALID,
                    "worker success requires a matching succeeded Attempt",
                ));
            }
        } else if self.output.is_some() {
            return Err(ModelError::new(
                AGGREGATE_OUTPUT_INVALID,
                "non-succeeded Activation cannot own a terminal output",
            ));
        }
        if self.activation.lifecycle().is_terminal()
            && self
                .attempts
                .values()
                .any(|attempt| !attempt.lifecycle().is_terminal())
        {
            return Err(ModelError::new(
                AGGREGATE_STATE_DIVERGED,
                "terminal Activation still owns a live Attempt",
            ));
        }
        Ok(())
    }

    fn intent_hash<T>(&self, command: &T) -> Result<IntentHash, ModelError>
    where
        T: Serialize + ?Sized,
    {
        #[derive(Serialize)]
        struct Envelope<'a, T: ?Sized> {
            schema_version: u32,
            run_id: &'a RunId,
            activation_id: &'a ActivationId,
            node_id: &'a NodeId,
            scope_instance_id: &'a ScopeInstanceId,
            effect_id: &'a EffectId,
            execution_kind: &'a ExecutionKind,
            command: &'a T,
        }

        IntentHash::from_serializable(&Envelope {
            schema_version: AGGREGATE_COMMAND_INTENT_SCHEMA_VERSION,
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            node_id: &self.node_id,
            scope_instance_id: &self.scope_instance_id,
            effect_id: &self.effect_id,
            execution_kind: &self.execution_kind,
            command,
        })
    }

    fn require_worker(&self) -> Result<&WorkerExecutionPolicy, ModelError> {
        self.execution_kind.worker_policy().ok_or_else(|| {
            ModelError::new(
                AGGREGATE_EXECUTION_KIND_MISMATCH,
                "worker command was sent to a scheduler-native or wait Activation",
            )
        })
    }

    fn require_kind(&self, expected: &str, matches: bool) -> Result<(), ModelError> {
        if matches {
            Ok(())
        } else {
            Err(ModelError::new(
                AGGREGATE_EXECUTION_KIND_MISMATCH,
                format!("Activation does not have {expected} execution capability"),
            ))
        }
    }

    fn require_proof_identity(
        &self,
        run_id: &RunId,
        activation_id: &ActivationId,
    ) -> Result<(), ModelError> {
        if run_id == &self.run_id && activation_id == &self.activation_id {
            Ok(())
        } else {
            Err(ModelError::new(
                AGGREGATE_PROOF_MISMATCH,
                "repository proof belongs to another Run or Activation",
            ))
        }
    }

    fn validate_output_proof(
        &self,
        proof: &CommittedOutputProof,
        expected_fence: Option<LeaseFence>,
    ) -> Result<(), ModelError> {
        self.require_proof_identity(&proof.run_id, &proof.activation_id)?;
        if proof.fence != expected_fence || proof.value.content_hash() != &proof.content_hash {
            return Err(ModelError::new(
                AGGREGATE_OUTPUT_INVALID,
                "committed output proof does not match its fence or content hash",
            ));
        }
        Ok(())
    }

    fn validate_lease_expiry(&self, proof: &LeaseExpiryProof) -> Result<(), ModelError> {
        self.require_proof_identity(&proof.run_id, &proof.activation_id)?;
        if proof.observed_at < proof.lease_deadline {
            return Err(ModelError::new(
                AGGREGATE_PROOF_EARLY,
                "lease expiry proof precedes its deadline",
            ));
        }
        let lease = self.current_lease.as_ref().ok_or_else(|| {
            ModelError::new(
                AGGREGATE_PROOF_MISMATCH,
                "lease expiry proof has no current durable lease",
            )
        })?;
        if lease.fence != proof.fence
            || lease.timer_id != proof.lease_timer_id
            || lease.deadline != proof.lease_deadline
        {
            return Err(ModelError::new(
                AGGREGATE_PROOF_MISMATCH,
                "lease expiry proof does not match the persisted lease timer",
            ));
        }
        Ok(())
    }

    fn validate_retry_schedule(
        &self,
        proof: &RetryScheduleProof,
        fence: LeaseFence,
        policy: &WorkerExecutionPolicy,
    ) -> Result<(), ModelError> {
        self.require_proof_identity(&proof.run_id, &proof.activation_id)?;
        let expected_budget = policy
            .max_attempts
            .checked_sub(fence.attempt_no().get())
            .ok_or_else(|| {
                ModelError::new(
                    AGGREGATE_RETRY_POLICY_INVALID,
                    "Attempt number exceeds the frozen retry policy",
                )
            })?;
        if proof.previous_fence != fence
            || proof.remaining_attempt_budget != expected_budget
            || expected_budget == 0
        {
            return Err(ModelError::new(
                AGGREGATE_PROOF_MISMATCH,
                "retry schedule does not match the failed Attempt and remaining budget",
            ));
        }
        Ok(())
    }

    fn validate_retry_timer(&self, proof: &RetryTimerProof) -> Result<(), ModelError> {
        self.require_proof_identity(&proof.run_id, &proof.activation_id)?;
        if proof.observed_at < proof.retry_at {
            return Err(ModelError::new(
                AGGREGATE_PROOF_EARLY,
                "retry timer proof precedes its deadline",
            ));
        }
        let retry = self.retry_wait.as_ref().ok_or_else(|| {
            ModelError::new(
                AGGREGATE_PROOF_MISMATCH,
                "retry timer proof has no persisted retry wait",
            )
        })?;
        if retry.previous_fence != proof.previous_fence
            || retry.timer_id != proof.retry_timer_id
            || retry.retry_at != proof.retry_at
            || retry.remaining_attempt_budget != proof.remaining_attempt_budget
        {
            return Err(ModelError::new(
                AGGREGATE_PROOF_MISMATCH,
                "retry timer proof does not match the persisted retry schedule",
            ));
        }
        Ok(())
    }

    fn is_current_fence(&self, fence: LeaseFence) -> bool {
        self.current_lease.as_ref().map(|lease| lease.fence) == Some(fence)
    }

    fn attempt_ref(&self, fence: LeaseFence) -> Result<&AttemptState, ModelError> {
        self.attempts
            .get(&fence.attempt_no())
            .filter(|attempt| attempt.fence() == fence)
            .ok_or_else(|| {
                ModelError::new(
                    AGGREGATE_ATTEMPT_MISSING,
                    "Attempt fence is absent from the Activation aggregate",
                )
            })
    }

    fn attempt_mut(&mut self, fence: LeaseFence) -> Result<&mut AttemptState, ModelError> {
        self.attempts
            .get_mut(&fence.attempt_no())
            .filter(|attempt| attempt.fence() == fence)
            .ok_or_else(|| {
                ModelError::new(
                    AGGREGATE_ATTEMPT_MISSING,
                    "Attempt fence is absent from the Activation aggregate",
                )
            })
    }

    fn commit<T: Clone>(
        &mut self,
        key: TransitionKey,
        hash: IntentHash,
        stored: AggregateTransitionResult,
        result: T,
        transition: impl FnOnce(&mut Self) -> Result<(), ModelError>,
    ) -> Result<TransitionOutcome<T>, ModelError> {
        let mut staged = self.staged();
        transition(&mut staged)?;
        staged.install_record(key, hash, stored)?;
        *self = staged;
        Ok(TransitionOutcome::Committed { result })
    }

    fn install_record(
        &mut self,
        key: TransitionKey,
        intent_hash: IntentHash,
        result: AggregateTransitionResult,
    ) -> Result<(), ModelError> {
        if self
            .commands
            .insert(
                key,
                CommandRecord {
                    intent_hash,
                    result,
                },
            )
            .is_some()
        {
            return Err(ModelError::new(
                AGGREGATE_ATTEMPT_CONFLICT,
                "transition key was installed twice after replay preflight",
            ));
        }
        self.validate()
    }

    fn replay_result(
        &self,
        key: &TransitionKey,
        hash: &IntentHash,
    ) -> Result<Option<&AggregateTransitionResult>, ModelError> {
        match self.commands.get(key) {
            None => Ok(None),
            Some(record) if &record.intent_hash == hash => Ok(Some(&record.result)),
            Some(_) => Err(ModelError::new(
                AGGREGATE_INTENT_CONFLICT,
                "transition key is already bound to a different canonical intent",
            )),
        }
    }

    fn replay_unit(
        &self,
        key: &TransitionKey,
        hash: &IntentHash,
    ) -> Result<Option<TransitionOutcome<()>>, ModelError> {
        match self.replay_result(key, hash)? {
            None => Ok(None),
            Some(AggregateTransitionResult::Unit) => {
                Ok(Some(TransitionOutcome::ExactReplay { authoritative: () }))
            }
            Some(_) => Err(replay_result_kind_mismatch("unit")),
        }
    }

    fn replay_bool(
        &self,
        key: &TransitionKey,
        hash: &IntentHash,
    ) -> Result<Option<TransitionOutcome<bool>>, ModelError> {
        match self.replay_result(key, hash)? {
            None => Ok(None),
            Some(AggregateTransitionResult::Bool(value)) => {
                Ok(Some(TransitionOutcome::ExactReplay {
                    authoritative: *value,
                }))
            }
            Some(_) => Err(replay_result_kind_mismatch("bool")),
        }
    }

    fn replay_fence(
        &self,
        key: &TransitionKey,
        hash: &IntentHash,
    ) -> Result<Option<TransitionOutcome<LeaseFence>>, ModelError> {
        match self.replay_result(key, hash)? {
            None => Ok(None),
            Some(AggregateTransitionResult::Fence(value)) => {
                Ok(Some(TransitionOutcome::ExactReplay {
                    authoritative: *value,
                }))
            }
            Some(_) => Err(replay_result_kind_mismatch("lease_fence")),
        }
    }

    fn replay_failure(
        &self,
        key: &TransitionKey,
        hash: &IntentHash,
    ) -> Result<Option<TransitionOutcome<AttemptFailureDisposition>>, ModelError> {
        match self.replay_result(key, hash)? {
            None => Ok(None),
            Some(AggregateTransitionResult::FailureDisposition(value)) => {
                Ok(Some(TransitionOutcome::ExactReplay {
                    authoritative: value.clone(),
                }))
            }
            Some(_) => Err(replay_result_kind_mismatch("failure_disposition")),
        }
    }

    fn replay_termination(
        &self,
        key: &TransitionKey,
        hash: &IntentHash,
    ) -> Result<Option<TransitionOutcome<TerminationProgress>>, ModelError> {
        match self.replay_result(key, hash)? {
            None => Ok(None),
            Some(AggregateTransitionResult::TerminationProgress(value)) => {
                Ok(Some(TransitionOutcome::ExactReplay {
                    authoritative: *value,
                }))
            }
            Some(_) => Err(replay_result_kind_mismatch("termination_progress")),
        }
    }
}

fn replay_result_kind_mismatch(expected: &str) -> ModelError {
    ModelError::new(
        AGGREGATE_STATE_DIVERGED,
        format!("stored transition result does not match expected {expected} result"),
    )
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;
    use crate::{InternalFailureCode, InternalFailureKind};

    fn run() -> RunId {
        RunId::new("run_aggregate").unwrap()
    }

    fn activation() -> ActivationId {
        ActivationId::new("activation_aggregate").unwrap()
    }

    fn key(domain: &str) -> TransitionKey {
        TransitionKey::derive(domain, &["aggregate"]).unwrap()
    }

    fn at(second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 18, 1, 2, second).unwrap()
    }

    fn worker(idempotency: EffectIdempotency, max_attempts: u32) -> ActivationAttemptAggregate {
        ActivationAttemptAggregate::new(
            run(),
            activation(),
            NodeId::new("worker").unwrap(),
            ScopeInstanceId::root(),
            ExecutionKind::Worker(
                WorkerExecutionPolicy::new(
                    idempotency,
                    max_attempts,
                    WorkerCancellation::Cooperative,
                )
                .unwrap(),
            ),
        )
    }

    fn grant(epoch: u64, second: u32) -> LeaseGrantProof {
        LeaseGrantProof::mint(
            run(),
            activation(),
            LeaseEpoch::new(epoch).unwrap(),
            TimerId::new(format!("lease_{epoch}")).unwrap(),
            at(second),
        )
    }

    fn output(fence: Option<LeaseFence>, answer: i32) -> CommittedOutputProof {
        output_for(run(), activation(), fence, answer)
    }

    fn output_for(
        run_id: RunId,
        activation_id: ActivationId,
        fence: Option<LeaseFence>,
        answer: i32,
    ) -> CommittedOutputProof {
        CommittedOutputProof::mint(
            run_id,
            activation_id,
            fence,
            ValueRef::inline(json!({"answer": answer})).unwrap(),
        )
    }

    fn failure() -> InternalFailureSummary {
        InternalFailureSummary::new(
            InternalFailureKind::Business,
            InternalFailureCode::new("WORKER_FAILED").unwrap(),
        )
    }

    fn running(aggregate: &mut ActivationAttemptAggregate) -> LeaseFence {
        assert!(matches!(
            aggregate.make_ready(key("ready")).unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            aggregate.make_ready(key("ready")).unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        let fence = aggregate
            .claim_worker(key("claim"), &grant(1, 10))
            .unwrap()
            .committed_result()
            .copied()
            .unwrap();
        aggregate.mark_running(key("running"), fence).unwrap();
        fence
    }

    #[test]
    fn success_is_atomic_with_owned_output_and_exact_replay_precedes_fencing() {
        let mut aggregate = worker(EffectIdempotency::Idempotent, 2);
        let fence = running(&mut aggregate);
        let proof = output(Some(fence), 42);
        assert!(matches!(
            aggregate
                .complete_worker_success(key("success"), fence, &proof)
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert_eq!(
            aggregate.state().lifecycle(),
            ActivationLifecycle::Succeeded
        );
        assert_eq!(
            aggregate.output().unwrap().content_hash(),
            proof.value().content_hash()
        );
        assert!(matches!(
            aggregate
                .complete_worker_success(key("success"), fence, &proof)
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        assert!(matches!(
            aggregate
                .complete_worker_success(key("late"), fence, &proof)
                .unwrap(),
            TransitionOutcome::StaleLease
        ));
        let terminal = aggregate.terminal_proof().unwrap();
        assert_eq!(terminal.succeeded_output(), Some(proof.value()));
        assert_eq!(
            terminal.succeeded_content_hash(),
            Some(proof.content_hash())
        );
    }

    #[test]
    fn cancellation_request_does_not_fake_attempt_drain() {
        let mut aggregate = worker(EffectIdempotency::Idempotent, 2);
        let fence = running(&mut aggregate);
        assert!(matches!(
            aggregate
                .request_termination(key("cancel"), ActivationTerminationReason::Cancelled)
                .unwrap(),
            TransitionOutcome::Committed {
                result: TerminationProgress::AwaitingAttemptDrain { .. }
            }
        ));
        assert_eq!(
            aggregate.state().lifecycle(),
            ActivationLifecycle::Terminating
        );
        assert_eq!(
            aggregate.attempt(fence.attempt_no()).unwrap().lifecycle(),
            AttemptLifecycle::Running
        );
        assert!(aggregate.terminal_proof().is_err());
        assert!(matches!(
            aggregate
                .complete_worker_success(key("lost_race"), fence, &output(Some(fence), 1))
                .unwrap(),
            TransitionOutcome::StaleLease
        ));
        aggregate
            .acknowledge_cancellation(key("cancel_ack"), fence)
            .unwrap();
        assert_eq!(
            aggregate.state().lifecycle(),
            ActivationLifecycle::Cancelled
        );
        assert_eq!(
            aggregate.attempt(fence.attempt_no()).unwrap().lifecycle(),
            AttemptLifecycle::Cancelled
        );
        let terminal = aggregate.terminal_proof().unwrap();
        assert!(matches!(
            terminal.result(),
            TerminalActivationResult::Cancelled
        ));
    }

    #[test]
    fn lease_expiry_requires_exact_timer_and_retry_budget_proofs() {
        let mut aggregate = worker(EffectIdempotency::Idempotent, 2);
        let fence = running(&mut aggregate);
        let wrong = LeaseExpiryProof::mint(
            run(),
            activation(),
            fence,
            TimerId::new("wrong_timer").unwrap(),
            at(10),
            at(11),
        )
        .unwrap();
        assert_eq!(
            aggregate
                .expire_lease(key("wrong_expiry"), &wrong, None)
                .unwrap_err()
                .code(),
            AGGREGATE_PROOF_MISMATCH
        );

        let expiry = LeaseExpiryProof::mint(
            run(),
            activation(),
            fence,
            TimerId::new("lease_1").unwrap(),
            at(10),
            at(11),
        )
        .unwrap();
        let schedule = RetryScheduleProof::mint(
            run(),
            activation(),
            fence,
            TimerId::new("retry_1").unwrap(),
            at(20),
            1,
        )
        .unwrap();
        assert!(matches!(
            aggregate
                .expire_lease(key("expiry"), &expiry, Some(&schedule))
                .unwrap(),
            TransitionOutcome::Committed {
                result: AttemptFailureDisposition::RetryScheduled { .. }
            }
        ));
        assert!(matches!(
            aggregate
                .expire_lease(key("expiry"), &expiry, Some(&schedule))
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        let foreign_expiry = LeaseExpiryProof::mint(
            RunId::new("run_foreign").unwrap(),
            ActivationId::new("activation_foreign").unwrap(),
            fence,
            TimerId::new("lease_1").unwrap(),
            at(10),
            at(11),
        )
        .unwrap();
        assert_eq!(
            aggregate
                .expire_lease(key("expiry"), &foreign_expiry, Some(&schedule))
                .unwrap_err()
                .code(),
            AGGREGATE_PROOF_MISMATCH,
            "foreign proof identity must be rejected before exact replay"
        );
        let foreign_schedule = RetryScheduleProof::mint(
            RunId::new("run_foreign").unwrap(),
            ActivationId::new("activation_foreign").unwrap(),
            fence,
            TimerId::new("retry_1").unwrap(),
            at(20),
            1,
        )
        .unwrap();
        assert_eq!(
            aggregate
                .expire_lease(key("expiry"), &expiry, Some(&foreign_schedule))
                .unwrap_err()
                .code(),
            AGGREGATE_PROOF_MISMATCH,
            "foreign retry schedule must be rejected before exact replay"
        );
        let early = RetryTimerProof::mint(
            run(),
            activation(),
            fence,
            TimerId::new("retry_1").unwrap(),
            at(20),
            at(19),
            1,
        );
        assert_eq!(early.unwrap_err().code(), AGGREGATE_PROOF_EARLY);
        let due = RetryTimerProof::mint(
            run(),
            activation(),
            fence,
            TimerId::new("retry_1").unwrap(),
            at(20),
            at(21),
            1,
        )
        .unwrap();
        aggregate.retry_due(key("retry_due"), &due).unwrap();
        assert!(matches!(
            aggregate.retry_due(key("retry_due"), &due).unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        let foreign_due = RetryTimerProof::mint(
            RunId::new("run_foreign").unwrap(),
            ActivationId::new("activation_foreign").unwrap(),
            fence,
            TimerId::new("retry_1").unwrap(),
            at(20),
            at(21),
            1,
        )
        .unwrap();
        assert_eq!(
            aggregate
                .retry_due(key("retry_due"), &foreign_due)
                .unwrap_err()
                .code(),
            AGGREGATE_PROOF_MISMATCH,
            "foreign retry proof identity must be rejected before exact replay"
        );
        assert_eq!(aggregate.state().lifecycle(), ActivationLifecycle::Ready);
    }

    #[test]
    fn exhausted_lease_loss_terminates_instead_of_stranding_retry_wait() {
        let mut aggregate = worker(EffectIdempotency::Idempotent, 1);
        let fence = running(&mut aggregate);
        let expiry = LeaseExpiryProof::mint(
            run(),
            activation(),
            fence,
            TimerId::new("lease_1").unwrap(),
            at(10),
            at(11),
        )
        .unwrap();
        assert!(matches!(
            aggregate
                .expire_lease(key("exhausted_expiry"), &expiry, None)
                .unwrap(),
            TransitionOutcome::Committed {
                result: AttemptFailureDisposition::Terminal {
                    lifecycle: ActivationLifecycle::Failed
                }
            }
        ));
        let terminal = aggregate.terminal_proof().unwrap();
        assert_eq!(
            terminal.failure_reason(),
            Some(ActivationTerminationReason::Failure)
        );
        assert!(terminal.failure_summary().is_none());
    }

    #[test]
    fn termination_cancels_retry_wait_instead_of_stranding_the_activation() {
        let mut aggregate = worker(EffectIdempotency::Idempotent, 2);
        let fence = running(&mut aggregate);
        let expiry = LeaseExpiryProof::mint(
            run(),
            activation(),
            fence,
            TimerId::new("lease_1").unwrap(),
            at(10),
            at(11),
        )
        .unwrap();
        let schedule = RetryScheduleProof::mint(
            run(),
            activation(),
            fence,
            TimerId::new("retry_cancelled").unwrap(),
            at(20),
            1,
        )
        .unwrap();
        aggregate
            .expire_lease(key("expiry_before_cancel"), &expiry, Some(&schedule))
            .unwrap();
        assert_eq!(
            aggregate.state().lifecycle(),
            ActivationLifecycle::RetryWait
        );

        assert!(matches!(
            aggregate
                .request_termination(
                    key("cancel_retry_wait"),
                    ActivationTerminationReason::Cancelled,
                )
                .unwrap(),
            TransitionOutcome::Committed {
                result: TerminationProgress::Terminal {
                    lifecycle: ActivationLifecycle::Cancelled
                }
            }
        ));
        let terminal = aggregate.terminal_proof().unwrap();
        assert!(matches!(
            terminal.result(),
            TerminalActivationResult::Cancelled
        ));
    }

    #[test]
    fn non_idempotent_unknown_effect_blocks_automatic_retry() {
        let mut aggregate = worker(EffectIdempotency::NonIdempotent, 3);
        let fence = running(&mut aggregate);
        aggregate
            .record_effect_evidence(key("started"), fence, EffectEvidence::Started)
            .unwrap();
        let disposition = aggregate
            .complete_worker_failure(key("failed"), fence, failure(), None)
            .unwrap();
        assert!(matches!(
            disposition,
            TransitionOutcome::Committed {
                result: AttemptFailureDisposition::Terminal {
                    lifecycle: ActivationLifecycle::Failed
                }
            }
        ));
        let terminal = aggregate.terminal_proof().unwrap();
        assert_eq!(
            terminal.failure_reason(),
            Some(ActivationTerminationReason::Failure)
        );
        assert_eq!(terminal.failure_summary(), Some(&failure()));
    }

    #[test]
    fn changed_intent_is_a_stable_error_and_leaves_state_unchanged() {
        let mut aggregate = worker(EffectIdempotency::Idempotent, 1);
        aggregate.make_ready(key("ready")).unwrap();
        let before = aggregate.staged();
        let first = grant(1, 10);
        aggregate.claim_worker(key("claim"), &first).unwrap();
        let after = aggregate.staged();
        let changed = grant(2, 11);
        assert_eq!(
            aggregate
                .claim_worker(key("claim"), &changed)
                .unwrap_err()
                .code(),
            AGGREGATE_INTENT_CONFLICT
        );
        assert_eq!(aggregate.state(), after.state());
        assert_ne!(before.state(), aggregate.state());
    }

    #[test]
    fn durable_wait_requires_repository_first_winner_proof() {
        let run_id = run();
        let wait_id = ActivationId::new("activation_wait_proven").unwrap();
        let registration_key = TransitionKey::derive("wait.register", &["proven"]).unwrap();
        let winning_key = TransitionKey::derive("wait.resolve", &["winner"]).unwrap();
        let mut wait = ActivationAttemptAggregate::new(
            run_id.clone(),
            wait_id.clone(),
            NodeId::new("wait").unwrap(),
            ScopeInstanceId::root(),
            ExecutionKind::DurableWait,
        );
        wait.make_ready(TransitionKey::derive("wait.ready", &["proven"]).unwrap())
            .unwrap();
        wait.begin_wait(registration_key.clone()).unwrap();

        let wrong_registration = WaitResolutionProof::mint(
            run_id.clone(),
            wait_id.clone(),
            TransitionKey::derive("wait.register", &["other"]).unwrap(),
            WaitResolutionSubject::Signal(SignalId::new("signal_ready").unwrap()),
            TransitionKey::derive("wait.resolve", &["wrong_registration"]).unwrap(),
            ExecutionEventId::random(),
            output_for(run_id.clone(), wait_id.clone(), None, 1),
        )
        .unwrap();
        assert_eq!(
            wait.resolve_wait(
                wrong_registration.winning_transition_key().clone(),
                &wrong_registration,
            )
            .unwrap_err()
            .code(),
            AGGREGATE_WAIT_PROOF_INVALID
        );
        assert_eq!(wait.state().lifecycle(), ActivationLifecycle::Waiting);

        let proof = WaitResolutionProof::mint(
            run_id.clone(),
            wait_id.clone(),
            registration_key.clone(),
            WaitResolutionSubject::Signal(SignalId::new("signal_ready").unwrap()),
            winning_key.clone(),
            ExecutionEventId::random(),
            output_for(run_id.clone(), wait_id.clone(), None, 42),
        )
        .unwrap();
        assert_eq!(
            wait.resolve_wait(
                TransitionKey::derive("wait.resolve", &["not_winner"]).unwrap(),
                &proof,
            )
            .unwrap_err()
            .code(),
            AGGREGATE_WAIT_PROOF_INVALID
        );
        assert!(matches!(
            wait.resolve_wait(winning_key.clone(), &proof).unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            wait.resolve_wait(winning_key.clone(), &proof).unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));

        let losing_timer = WaitResolutionProof::mint(
            run_id.clone(),
            wait_id.clone(),
            registration_key.clone(),
            WaitResolutionSubject::Timer(TimerId::new("wait_timeout").unwrap()),
            winning_key.clone(),
            ExecutionEventId::random(),
            output_for(run_id.clone(), wait_id.clone(), None, 99),
        )
        .unwrap();
        assert_eq!(
            wait.resolve_wait(winning_key.clone(), &losing_timer)
                .unwrap_err()
                .code(),
            AGGREGATE_INTENT_CONFLICT
        );

        let foreign_run = RunId::new("run_wait_foreign").unwrap();
        let foreign_activation = ActivationId::new("activation_wait_foreign").unwrap();
        let foreign = WaitResolutionProof::mint(
            foreign_run.clone(),
            foreign_activation.clone(),
            registration_key,
            WaitResolutionSubject::Signal(SignalId::new("signal_ready").unwrap()),
            winning_key.clone(),
            ExecutionEventId::random(),
            output_for(foreign_run, foreign_activation, None, 42),
        )
        .unwrap();
        assert_eq!(
            wait.resolve_wait(winning_key, &foreign).unwrap_err().code(),
            AGGREGATE_PROOF_MISMATCH,
            "foreign wait authority must be rejected before exact replay"
        );

        let terminal = wait.terminal_proof().unwrap();
        assert_eq!(terminal.succeeded_output(), Some(proof.output().value()));
    }

    #[test]
    fn cancelling_wait_consumes_registration_and_binds_terminal_kind() {
        let wait_id = ActivationId::new("activation_wait_timeout").unwrap();
        let mut wait = ActivationAttemptAggregate::new(
            run(),
            wait_id,
            NodeId::new("wait_timeout").unwrap(),
            ScopeInstanceId::root(),
            ExecutionKind::DurableWait,
        );
        wait.make_ready(TransitionKey::derive("wait.ready", &["timeout"]).unwrap())
            .unwrap();
        wait.begin_wait(TransitionKey::derive("wait.register", &["timeout"]).unwrap())
            .unwrap();
        wait.request_termination(
            TransitionKey::derive("wait.terminate", &["timeout"]).unwrap(),
            ActivationTerminationReason::TimedOut,
        )
        .unwrap();
        wait.validate().unwrap();
        assert!(matches!(
            wait.terminal_proof().unwrap().result(),
            TerminalActivationResult::TimedOut
        ));
    }

    #[test]
    fn terminal_proof_rejects_lifecycle_result_and_hash_mismatch() {
        let value = ValueRef::inline(json!({"answer": 42})).unwrap();
        let wrong_hash = ContentHash::from_bytes(b"not-the-output");
        assert_eq!(
            TerminalActivationProof::mint(
                run(),
                ScopeInstanceId::root(),
                activation(),
                ActivationLifecycle::Succeeded,
                true,
                TerminalActivationResult::Succeeded {
                    output: value,
                    content_hash: wrong_hash,
                },
            )
            .unwrap_err()
            .code(),
            TERMINAL_PROOF_RESULT_MISMATCH
        );
        assert_eq!(
            TerminalActivationProof::mint(
                run(),
                ScopeInstanceId::root(),
                activation(),
                ActivationLifecycle::Failed,
                true,
                TerminalActivationResult::Failed {
                    reason: ActivationTerminationReason::Cancelled,
                    failure: None,
                },
            )
            .unwrap_err()
            .code(),
            TERMINAL_PROOF_RESULT_MISMATCH
        );
    }

    #[test]
    fn execution_kind_capabilities_cannot_be_mixed() {
        let mut native = ActivationAttemptAggregate::new(
            run(),
            activation(),
            NodeId::new("branch").unwrap(),
            ScopeInstanceId::root(),
            ExecutionKind::SchedulerNative,
        );
        native.make_ready(key("native_ready")).unwrap();
        assert_eq!(
            native
                .claim_worker(key("native_claim"), &grant(1, 10))
                .unwrap_err()
                .code(),
            AGGREGATE_EXECUTION_KIND_MISMATCH
        );
        native
            .complete_native(key("native_complete"), &output(None, 0))
            .unwrap();
        native.terminal_proof().unwrap();

        let mut wait = ActivationAttemptAggregate::new(
            run(),
            ActivationId::new("activation_wait").unwrap(),
            NodeId::new("wait").unwrap(),
            ScopeInstanceId::root(),
            ExecutionKind::DurableWait,
        );
        wait.make_ready(TransitionKey::derive("wait.ready", &["wait"]).unwrap())
            .unwrap();
        wait.begin_wait(TransitionKey::derive("wait.begin", &["wait"]).unwrap())
            .unwrap();
        assert_eq!(wait.state().lifecycle(), ActivationLifecycle::Waiting);
    }
}
