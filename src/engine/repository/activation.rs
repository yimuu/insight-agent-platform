use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::{
    ActivationId, ActivationLifecycle, ActivationTerminationReason, AttemptNo, ContentHash,
    EffectEvidence, EffectId, EffectIdempotency, ExecutionEventId, ExecutionKind,
    InternalFailureSummary, LeaseEpoch, LeaseFence, NodeId, RunId, ScopeInstanceId, SignalId,
    TimerId, TransitionKey, ValueRef,
};

use super::{RepositoryError, REPOSITORY_CONFIGURATION_INVALID};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WaitLateAuditOutcome {
    NotApplicable,
    AlreadyRecorded,
    Appended,
}

impl WaitLateAuditOutcome {
    pub(super) fn is_loser(self) -> bool {
        !matches!(self, Self::NotApplicable)
    }

    pub(super) fn was_appended(self) -> bool {
        matches!(self, Self::Appended)
    }
}

fn invalid_command() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_CONFIGURATION_INVALID,
        "durable activation repository command is invalid",
    )
}

fn validate_label(value: &str) -> Result<(), RepositoryError> {
    if value.is_empty()
        || value.len() > 256
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(invalid_command());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskDispatchSpec {
    worker_contract: String,
    input_payload_id: Option<String>,
}

impl TaskDispatchSpec {
    pub fn new(
        worker_contract: impl Into<String>,
        input_payload_id: Option<String>,
    ) -> Result<Self, RepositoryError> {
        let value = Self {
            worker_contract: worker_contract.into(),
            input_payload_id,
        };
        validate_label(&value.worker_contract)?;
        if let Some(payload_id) = value.input_payload_id.as_deref() {
            validate_label(payload_id)?;
        }
        Ok(value)
    }

    pub fn worker_contract(&self) -> &str {
        &self.worker_contract
    }

    pub fn input_payload_id(&self) -> Option<&str> {
        self.input_payload_id.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ActivationAdmissionCommand {
    run_id: RunId,
    scope_instance_id: ScopeInstanceId,
    expected_scope_projection_version: u64,
    activation_id: ActivationId,
    node_id: NodeId,
    stable_activation_key: String,
    execution_kind: ExecutionKind,
}

impl ActivationAdmissionCommand {
    pub fn new(
        run_id: RunId,
        scope_instance_id: ScopeInstanceId,
        expected_scope_projection_version: u64,
        activation_id: ActivationId,
        node_id: NodeId,
        stable_activation_key: impl Into<String>,
        execution_kind: ExecutionKind,
    ) -> Result<Self, RepositoryError> {
        let value = Self {
            run_id,
            scope_instance_id,
            expected_scope_projection_version,
            activation_id,
            node_id,
            stable_activation_key: stable_activation_key.into(),
            execution_kind,
        };
        validate_label(&value.stable_activation_key)?;
        Ok(value)
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn expected_scope_projection_version(&self) -> u64 {
        self.expected_scope_projection_version
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub(crate) fn stable_activation_key(&self) -> &str {
        &self.stable_activation_key
    }

    pub fn execution_kind(&self) -> &ExecutionKind {
        &self.execution_kind
    }

    pub fn effect_id(&self) -> EffectId {
        EffectId::for_activation(&self.run_id, &self.activation_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ActivationCasCommand {
    run_id: RunId,
    activation_id: ActivationId,
    expected_projection_version: u64,
}

impl ActivationCasCommand {
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        expected_projection_version: u64,
    ) -> Self {
        Self {
            run_id,
            activation_id,
            expected_projection_version,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn expected_projection_version(&self) -> u64 {
        self.expected_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GrantAttemptLeaseCommand {
    run_id: RunId,
    activation_id: ActivationId,
    expected_projection_version: u64,
    worker_id: String,
    lease_seconds: u32,
    task: TaskDispatchSpec,
}

impl GrantAttemptLeaseCommand {
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        expected_projection_version: u64,
        worker_id: impl Into<String>,
        lease_seconds: u32,
        task: TaskDispatchSpec,
    ) -> Result<Self, RepositoryError> {
        let value = Self {
            run_id,
            activation_id,
            expected_projection_version,
            worker_id: worker_id.into(),
            lease_seconds,
            task,
        };
        validate_label(&value.worker_id)?;
        if value.lease_seconds == 0 || value.lease_seconds > 86_400 {
            return Err(invalid_command());
        }
        Ok(value)
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn expected_projection_version(&self) -> u64 {
        self.expected_projection_version
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn lease_seconds(&self) -> u32 {
        self.lease_seconds
    }

    pub fn task(&self) -> &TaskDispatchSpec {
        &self.task
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FencedAttemptCommand {
    run_id: RunId,
    activation_id: ActivationId,
    fence: LeaseFence,
    fencing_token: String,
    worker_id: String,
    expected_activation_projection_version: u64,
    expected_attempt_projection_version: u64,
}

impl FencedAttemptCommand {
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        fence: LeaseFence,
        fencing_token: impl Into<String>,
        worker_id: impl Into<String>,
        expected_activation_projection_version: u64,
        expected_attempt_projection_version: u64,
    ) -> Result<Self, RepositoryError> {
        let value = Self {
            run_id,
            activation_id,
            fence,
            fencing_token: fencing_token.into(),
            worker_id: worker_id.into(),
            expected_activation_projection_version,
            expected_attempt_projection_version,
        };
        validate_label(&value.fencing_token)?;
        validate_label(&value.worker_id)?;
        Ok(value)
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn fence(&self) -> LeaseFence {
        self.fence
    }

    pub fn fencing_token(&self) -> &str {
        &self.fencing_token
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn expected_activation_projection_version(&self) -> u64 {
        self.expected_activation_projection_version
    }

    pub fn expected_attempt_projection_version(&self) -> u64 {
        self.expected_attempt_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeartbeatAttemptCommand {
    authority: FencedAttemptCommand,
    extend_seconds: u32,
}

impl HeartbeatAttemptCommand {
    pub fn new(
        authority: FencedAttemptCommand,
        extend_seconds: u32,
    ) -> Result<Self, RepositoryError> {
        if extend_seconds == 0 || extend_seconds > 86_400 {
            return Err(invalid_command());
        }
        Ok(Self {
            authority,
            extend_seconds,
        })
    }

    pub fn authority(&self) -> &FencedAttemptCommand {
        &self.authority
    }

    pub fn extend_seconds(&self) -> u32 {
        self.extend_seconds
    }

    pub(crate) fn transition_key(&self) -> Result<TransitionKey, RepositoryError> {
        let attempt = self.authority.fence().attempt_no().get().to_string();
        let epoch = self.authority.fence().lease_epoch().get().to_string();
        let activation_version = self
            .authority
            .expected_activation_projection_version()
            .to_string();
        let attempt_version = self
            .authority
            .expected_attempt_projection_version()
            .to_string();
        let extend = self.extend_seconds.to_string();
        model_data_transition_key(TransitionKey::derive(
            "repository.heartbeat",
            &[
                self.authority.run_id().as_str(),
                self.authority.activation_id().as_str(),
                &attempt,
                &epoch,
                &activation_version,
                &attempt_version,
                &extend,
            ],
        ))
    }
}

fn model_data_transition_key(
    value: Result<TransitionKey, crate::engine::ModelError>,
) -> Result<TransitionKey, RepositoryError> {
    value.map_err(|_| RepositoryError::invalid_data())
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "completion", rename_all = "snake_case")]
pub enum AttemptCompletion {
    Succeeded {
        output: ValueRef,
    },
    Failed {
        reason: ActivationTerminationReason,
        failure: Option<InternalFailureSummary>,
        retry_at: Option<DateTime<Utc>>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CompleteAttemptCommand {
    authority: FencedAttemptCommand,
    completion: AttemptCompletion,
}

impl CompleteAttemptCommand {
    pub fn new(authority: FencedAttemptCommand, completion: AttemptCompletion) -> Self {
        Self {
            authority,
            completion,
        }
    }

    pub fn authority(&self) -> &FencedAttemptCommand {
        &self.authority
    }

    pub fn completion(&self) -> &AttemptCompletion {
        &self.completion
    }

    pub fn run_id(&self) -> &RunId {
        self.authority.run_id()
    }

    pub fn activation_id(&self) -> &ActivationId {
        self.authority.activation_id()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecordEffectEvidenceCommand {
    authority: FencedAttemptCommand,
    evidence: EffectEvidence,
}

impl RecordEffectEvidenceCommand {
    pub fn new(authority: FencedAttemptCommand, evidence: EffectEvidence) -> Self {
        Self {
            authority,
            evidence,
        }
    }

    pub fn authority(&self) -> &FencedAttemptCommand {
        &self.authority
    }

    pub fn evidence(&self) -> EffectEvidence {
        self.evidence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationTimerKind {
    Wait,
    ActivationTimeout,
}

impl ActivationTimerKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Wait => "wait",
            Self::ActivationTimeout => "activation_timeout",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegisterWaitCommand {
    run_id: RunId,
    activation_id: ActivationId,
    expected_projection_version: u64,
    wait_deadline: Option<DateTime<Utc>>,
}

impl RegisterWaitCommand {
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        expected_projection_version: u64,
        wait_deadline: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            run_id,
            activation_id,
            expected_projection_version,
            wait_deadline,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn expected_projection_version(&self) -> u64 {
        self.expected_projection_version
    }

    pub fn wait_deadline(&self) -> Option<&DateTime<Utc>> {
        self.wait_deadline.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScheduleActivationTimerCommand {
    run_id: RunId,
    activation_id: ActivationId,
    kind: ActivationTimerKind,
    deadline: DateTime<Utc>,
}

impl ScheduleActivationTimerCommand {
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        kind: ActivationTimerKind,
        deadline: DateTime<Utc>,
    ) -> Self {
        Self {
            run_id,
            activation_id,
            kind,
            deadline,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn kind(&self) -> ActivationTimerKind {
        self.kind
    }

    pub fn deadline(&self) -> &DateTime<Utc> {
        &self.deadline
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReceiveSignalCommand {
    run_id: RunId,
    signal_id: SignalId,
    message_id: String,
    signal_name: String,
    target_activation_id: ActivationId,
    value: Value,
}

impl ReceiveSignalCommand {
    /// Builds an externally submitted, idempotent signal command.
    pub fn new(
        run_id: RunId,
        signal_id: SignalId,
        message_id: impl Into<String>,
        signal_name: impl Into<String>,
        target_activation_id: ActivationId,
        value: Value,
    ) -> Result<Self, RepositoryError> {
        let command = Self {
            run_id,
            signal_id,
            message_id: message_id.into(),
            signal_name: signal_name.into(),
            target_activation_id,
            value,
        };
        validate_label(&command.message_id)?;
        validate_label(&command.signal_name)?;
        Ok(command)
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn signal_id(&self) -> &SignalId {
        &self.signal_id
    }

    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    pub fn signal_name(&self) -> &str {
        &self.signal_name
    }

    pub fn target_activation_id(&self) -> &ActivationId {
        &self.target_activation_id
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolveSignalCommand {
    run_id: RunId,
    activation_id: ActivationId,
    signal_id: SignalId,
    expected_activation_projection_version: u64,
}

impl ResolveSignalCommand {
    /// Builds the CAS command that attempts to make this signal the wait winner.
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        signal_id: SignalId,
        expected_activation_projection_version: u64,
    ) -> Self {
        Self {
            run_id,
            activation_id,
            signal_id,
            expected_activation_projection_version,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn signal_id(&self) -> &SignalId {
        &self.signal_id
    }

    pub fn expected_activation_projection_version(&self) -> u64 {
        self.expected_activation_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FireTimerCommand {
    run_id: RunId,
    timer_id: TimerId,
    retry_at: Option<DateTime<Utc>>,
}

impl FireTimerCommand {
    /// Builds a timer-worker command. Repository fencing decides whether it wins.
    pub fn new(run_id: RunId, timer_id: TimerId, retry_at: Option<DateTime<Utc>>) -> Self {
        Self {
            run_id,
            timer_id,
            retry_at,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn timer_id(&self) -> &TimerId {
        &self.timer_id
    }

    pub fn retry_at(&self) -> Option<&DateTime<Utc>> {
        self.retry_at.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationCommitReceipt {
    event_seq: u64,
    event_id: String,
    activation_projection_version: u64,
    scope_projection_version: Option<u64>,
}

impl ActivationCommitReceipt {
    pub(crate) fn new(
        event_seq: u64,
        event_id: String,
        activation_projection_version: u64,
        scope_projection_version: Option<u64>,
    ) -> Self {
        Self {
            event_seq,
            event_id,
            activation_projection_version,
            scope_projection_version,
        }
    }

    pub fn event_seq(&self) -> u64 {
        self.event_seq
    }

    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    pub fn activation_projection_version(&self) -> u64 {
        self.activation_projection_version
    }

    pub fn scope_projection_version(&self) -> Option<u64> {
        self.scope_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseGrantAuthority {
    receipt: ActivationCommitReceipt,
    run_id: RunId,
    activation_id: ActivationId,
    fence: LeaseFence,
    fencing_token: String,
    lease_timer_id: TimerId,
    lease_deadline: DateTime<Utc>,
    task_id: String,
}

impl LeaseGrantAuthority {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        receipt: ActivationCommitReceipt,
        run_id: RunId,
        activation_id: ActivationId,
        fence: LeaseFence,
        fencing_token: String,
        lease_timer_id: TimerId,
        lease_deadline: DateTime<Utc>,
        task_id: String,
    ) -> Self {
        Self {
            receipt,
            run_id,
            activation_id,
            fence,
            fencing_token,
            lease_timer_id,
            lease_deadline,
            task_id,
        }
    }

    pub fn receipt(&self) -> &ActivationCommitReceipt {
        &self.receipt
    }

    pub fn fence(&self) -> LeaseFence {
        self.fence
    }

    pub fn fencing_token(&self) -> &str {
        &self.fencing_token
    }

    pub fn lease_timer_id(&self) -> &TimerId {
        &self.lease_timer_id
    }

    pub fn lease_deadline(&self) -> &DateTime<Utc> {
        &self.lease_deadline
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    #[allow(dead_code)]
    pub(crate) fn mint_proof(&self) -> crate::engine::LeaseGrantProof {
        crate::engine::LeaseGrantProof::mint(
            self.run_id.clone(),
            self.activation_id.clone(),
            self.fence.lease_epoch(),
            self.lease_timer_id.clone(),
            self.lease_deadline,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryScheduleAuthority {
    run_id: RunId,
    activation_id: ActivationId,
    previous_fence: LeaseFence,
    timer_id: TimerId,
    retry_at: DateTime<Utc>,
    remaining_attempt_budget: u32,
}

impl RetryScheduleAuthority {
    pub(crate) fn new(
        run_id: RunId,
        activation_id: ActivationId,
        previous_fence: LeaseFence,
        timer_id: TimerId,
        retry_at: DateTime<Utc>,
        remaining_attempt_budget: u32,
    ) -> Self {
        Self {
            run_id,
            activation_id,
            previous_fence,
            timer_id,
            retry_at,
            remaining_attempt_budget,
        }
    }

    pub fn timer_id(&self) -> &TimerId {
        &self.timer_id
    }

    pub fn retry_at(&self) -> &DateTime<Utc> {
        &self.retry_at
    }

    pub fn remaining_attempt_budget(&self) -> u32 {
        self.remaining_attempt_budget
    }

    #[allow(dead_code)]
    pub(crate) fn mint_proof(
        &self,
    ) -> Result<crate::engine::RetryScheduleProof, crate::engine::ModelError> {
        crate::engine::RetryScheduleProof::mint(
            self.run_id.clone(),
            self.activation_id.clone(),
            self.previous_fence,
            self.timer_id.clone(),
            self.retry_at,
            self.remaining_attempt_budget,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AttemptCompletionAuthority {
    Succeeded {
        terminal: CommittedTerminalActivationAuthority,
        fence: LeaseFence,
        output: ValueRef,
    },
    RetryScheduled {
        receipt: ActivationCommitReceipt,
        retry: RetryScheduleAuthority,
    },
    Failed {
        terminal: CommittedTerminalActivationAuthority,
    },
}

impl AttemptCompletionAuthority {
    pub fn receipt(&self) -> &ActivationCommitReceipt {
        match self {
            Self::Succeeded { terminal, .. } | Self::Failed { terminal } => terminal.receipt(),
            Self::RetryScheduled { receipt, .. } => receipt,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CommittedTerminalActivationResult {
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

impl CommittedTerminalActivationResult {
    fn lifecycle(&self) -> ActivationLifecycle {
        match self {
            Self::Succeeded { .. } => ActivationLifecycle::Succeeded,
            Self::Failed { .. } => ActivationLifecycle::Failed,
            Self::Cancelled => ActivationLifecycle::Cancelled,
            Self::TimedOut => ActivationLifecycle::TimedOut,
        }
    }
}

/// Repository-owned proof that an Activation terminal checkpoint and its
/// authoritative execution event committed together. Control persistence must
/// bind an aggregate proof through this wrapper instead of accepting an
/// uncommitted in-memory terminal proof directly.
#[derive(Debug, Clone, PartialEq)]
pub struct CommittedTerminalActivationAuthority {
    receipt: ActivationCommitReceipt,
    run_id: RunId,
    scope_instance_id: ScopeInstanceId,
    activation_id: ActivationId,
    result: CommittedTerminalActivationResult,
}

impl CommittedTerminalActivationAuthority {
    pub(crate) fn new(
        receipt: ActivationCommitReceipt,
        run_id: RunId,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        result: CommittedTerminalActivationResult,
    ) -> Result<Self, RepositoryError> {
        if !result.lifecycle().is_terminal()
            || receipt.activation_projection_version() == 0
            || ExecutionEventId::parse(receipt.event_id().to_owned()).is_err()
        {
            return Err(invalid_command());
        }
        Ok(Self {
            receipt,
            run_id,
            scope_instance_id,
            activation_id,
            result,
        })
    }

    pub fn receipt(&self) -> &ActivationCommitReceipt {
        &self.receipt
    }

    pub fn lifecycle(&self) -> ActivationLifecycle {
        self.result.lifecycle()
    }

    pub fn result(&self) -> &CommittedTerminalActivationResult {
        &self.result
    }

    pub fn bind(
        self,
        proof: crate::engine::TerminalActivationProof,
    ) -> Result<CommittedTerminalActivationProof, RepositoryError> {
        let identity_matches = proof.run_id() == &self.run_id
            && proof.scope_instance_id() == &self.scope_instance_id
            && proof.activation_id() == &self.activation_id
            && proof.terminal() == self.lifecycle()
            && proof.attempts_drained();
        let result_matches = match (&self.result, proof.result()) {
            (
                CommittedTerminalActivationResult::Succeeded {
                    output,
                    content_hash,
                },
                crate::engine::TerminalActivationResult::Succeeded {
                    output: proven_output,
                    content_hash: proven_hash,
                },
            ) => output == proven_output && content_hash == proven_hash,
            (
                CommittedTerminalActivationResult::Failed { reason, failure },
                crate::engine::TerminalActivationResult::Failed {
                    reason: proven_reason,
                    failure: proven_failure,
                },
            ) => reason == proven_reason && failure == proven_failure,
            (
                CommittedTerminalActivationResult::Cancelled,
                crate::engine::TerminalActivationResult::Cancelled,
            )
            | (
                CommittedTerminalActivationResult::TimedOut,
                crate::engine::TerminalActivationResult::TimedOut,
            ) => true,
            _ => false,
        };
        if !identity_matches || !result_matches {
            return Err(RepositoryError::invalid_data());
        }
        Ok(CommittedTerminalActivationProof {
            authority: self,
            aggregate_proof: proof,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommittedTerminalActivationProof {
    authority: CommittedTerminalActivationAuthority,
    aggregate_proof: crate::engine::TerminalActivationProof,
}

impl CommittedTerminalActivationProof {
    pub fn authority(&self) -> &CommittedTerminalActivationAuthority {
        &self.authority
    }

    pub fn aggregate_proof(&self) -> &crate::engine::TerminalActivationProof {
        &self.aggregate_proof
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalReceipt {
    signal_id: SignalId,
    payload_id: String,
    content_hash: ContentHash,
}

impl SignalReceipt {
    pub(crate) fn new(signal_id: SignalId, payload_id: String, content_hash: ContentHash) -> Self {
        Self {
            signal_id,
            payload_id,
            content_hash,
        }
    }

    pub fn signal_id(&self) -> &SignalId {
        &self.signal_id
    }

    pub fn payload_id(&self) -> &str {
        &self.payload_id
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WaitResolutionAuthority {
    terminal: CommittedTerminalActivationAuthority,
    registration_transition_key: TransitionKey,
    subject: crate::engine::WaitResolutionSubject,
    winning_transition_key: TransitionKey,
    output: ValueRef,
}

impl WaitResolutionAuthority {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        receipt: ActivationCommitReceipt,
        run_id: RunId,
        scope_instance_id: ScopeInstanceId,
        activation_id: ActivationId,
        registration_transition_key: TransitionKey,
        subject: crate::engine::WaitResolutionSubject,
        winning_transition_key: TransitionKey,
        output: ValueRef,
    ) -> Result<Self, RepositoryError> {
        let terminal = CommittedTerminalActivationAuthority::new(
            receipt,
            run_id,
            scope_instance_id,
            activation_id,
            CommittedTerminalActivationResult::Succeeded {
                content_hash: output.content_hash().clone(),
                output: output.clone(),
            },
        )?;
        Ok(Self {
            terminal,
            registration_transition_key,
            subject,
            winning_transition_key,
            output,
        })
    }

    pub fn receipt(&self) -> &ActivationCommitReceipt {
        self.terminal.receipt()
    }

    pub fn terminal(&self) -> &CommittedTerminalActivationAuthority {
        &self.terminal
    }

    pub fn subject(&self) -> &crate::engine::WaitResolutionSubject {
        &self.subject
    }

    pub fn output(&self) -> &ValueRef {
        &self.output
    }

    #[allow(dead_code)]
    pub(crate) fn mint_proof(
        &self,
    ) -> Result<crate::engine::WaitResolutionProof, crate::engine::ModelError> {
        let event_id = ExecutionEventId::parse(self.receipt().event_id().to_owned())?;
        let output = crate::engine::CommittedOutputProof::mint(
            self.terminal.run_id.clone(),
            self.terminal.activation_id.clone(),
            None,
            self.output.clone(),
        );
        crate::engine::WaitResolutionProof::mint(
            self.terminal.run_id.clone(),
            self.terminal.activation_id.clone(),
            self.registration_transition_key.clone(),
            self.subject.clone(),
            self.winning_transition_key.clone(),
            event_id,
            output,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TimerFireAuthority {
    LeaseExpired {
        receipt: ActivationCommitReceipt,
        fence: LeaseFence,
        lease_timer_id: TimerId,
        lease_deadline: DateTime<Utc>,
        observed_at: DateTime<Utc>,
        retry: Option<RetryScheduleAuthority>,
        terminal: Option<CommittedTerminalActivationAuthority>,
    },
    RetryDue {
        receipt: ActivationCommitReceipt,
        previous_fence: LeaseFence,
        retry_timer_id: TimerId,
        retry_at: DateTime<Utc>,
        observed_at: DateTime<Utc>,
        remaining_attempt_budget: u32,
    },
    WaitResolved(WaitResolutionAuthority),
    ActivationTimedOut {
        receipt: ActivationCommitReceipt,
        terminal: CommittedTerminalActivationAuthority,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEnvelope {
    schema_version: u32,
    run_id: RunId,
    activation_id: ActivationId,
    node_id: NodeId,
    effect_id: EffectId,
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    fencing_token: String,
    worker_contract: String,
    input_payload_id: Option<String>,
}

impl TaskEnvelope {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        run_id: RunId,
        activation_id: ActivationId,
        node_id: NodeId,
        effect_id: EffectId,
        fence: LeaseFence,
        fencing_token: String,
        task: &TaskDispatchSpec,
    ) -> Self {
        Self {
            schema_version: 1,
            run_id,
            activation_id,
            node_id,
            effect_id,
            attempt_no: fence.attempt_no(),
            lease_epoch: fence.lease_epoch(),
            fencing_token,
            worker_contract: task.worker_contract.clone(),
            input_payload_id: task.input_payload_id.clone(),
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }

    pub fn fence(&self) -> Result<LeaseFence, crate::engine::ModelError> {
        LeaseFence::new(self.attempt_no, self.lease_epoch)
    }

    pub fn fencing_token(&self) -> &str {
        &self.fencing_token
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskClaim {
    run_id: RunId,
    task_id: String,
    claim_token: String,
    claimant: String,
    claim_expires_at: DateTime<Utc>,
    envelope: TaskEnvelope,
}

impl TaskClaim {
    pub(crate) fn new(
        run_id: RunId,
        task_id: String,
        claim_token: String,
        claimant: String,
        claim_expires_at: DateTime<Utc>,
        envelope: TaskEnvelope,
    ) -> Self {
        Self {
            run_id,
            task_id,
            claim_token,
            claimant,
            claim_expires_at,
            envelope,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn claim_token(&self) -> &str {
        &self.claim_token
    }

    pub fn claimant(&self) -> &str {
        &self.claimant
    }

    pub fn claim_expires_at(&self) -> &DateTime<Utc> {
        &self.claim_expires_at
    }

    pub fn envelope(&self) -> &TaskEnvelope {
        &self.envelope
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationProjection {
    run_id: RunId,
    activation_id: ActivationId,
    lifecycle: ActivationLifecycle,
    projection_version: u64,
    current_fence: Option<LeaseFence>,
    current_fencing_token: Option<String>,
    retry_budget_remaining: u32,
    pending_retry_timer_id: Option<TimerId>,
    wait_registration_transition_key: Option<TransitionKey>,
}

impl ActivationProjection {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        run_id: RunId,
        activation_id: ActivationId,
        lifecycle: ActivationLifecycle,
        projection_version: u64,
        current_fence: Option<LeaseFence>,
        current_fencing_token: Option<String>,
        retry_budget_remaining: u32,
        pending_retry_timer_id: Option<TimerId>,
        wait_registration_transition_key: Option<TransitionKey>,
    ) -> Self {
        Self {
            run_id,
            activation_id,
            lifecycle,
            projection_version,
            current_fence,
            current_fencing_token,
            retry_budget_remaining,
            pending_retry_timer_id,
            wait_registration_transition_key,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn lifecycle(&self) -> ActivationLifecycle {
        self.lifecycle
    }

    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }

    pub fn current_fence(&self) -> Option<LeaseFence> {
        self.current_fence
    }

    pub fn current_fencing_token(&self) -> Option<&str> {
        self.current_fencing_token.as_deref()
    }

    pub fn retry_budget_remaining(&self) -> u32 {
        self.retry_budget_remaining
    }

    pub fn pending_retry_timer_id(&self) -> Option<&TimerId> {
        self.pending_retry_timer_id.as_ref()
    }

    pub fn wait_registration_transition_key(&self) -> Option<&TransitionKey> {
        self.wait_registration_transition_key.as_ref()
    }
}

pub(crate) fn execution_kind_fields(kind: &ExecutionKind) -> (&'static str, &'static str, u32) {
    match kind {
        ExecutionKind::Worker(policy) => (
            "worker",
            effect_idempotency_str(policy.effect_idempotency()),
            policy.max_attempts(),
        ),
        ExecutionKind::SchedulerNative => ("scheduler_native", "idempotent", 0),
        ExecutionKind::DurableWait => ("durable_wait", "idempotent", 0),
    }
}

pub(crate) const fn effect_idempotency_str(value: EffectIdempotency) -> &'static str {
    match value {
        EffectIdempotency::Idempotent => "idempotent",
        EffectIdempotency::NonIdempotent => "non_idempotent",
    }
}

pub(crate) fn parse_effect_idempotency(value: &str) -> Result<EffectIdempotency, RepositoryError> {
    match value {
        "idempotent" => Ok(EffectIdempotency::Idempotent),
        "non_idempotent" => Ok(EffectIdempotency::NonIdempotent),
        _ => Err(RepositoryError::invalid_data()),
    }
}

pub(crate) const fn effect_evidence_str(value: EffectEvidence) -> &'static str {
    match value {
        EffectEvidence::NotStarted => "not_started",
        EffectEvidence::Started => "started",
        EffectEvidence::Committed => "committed",
        EffectEvidence::Unknown => "unknown",
    }
}

pub(crate) fn parse_effect_evidence(value: &str) -> Result<EffectEvidence, RepositoryError> {
    match value {
        "not_started" => Ok(EffectEvidence::NotStarted),
        "started" => Ok(EffectEvidence::Started),
        "committed" => Ok(EffectEvidence::Committed),
        "unknown" => Ok(EffectEvidence::Unknown),
        _ => Err(RepositoryError::invalid_data()),
    }
}

pub(crate) fn parse_activation_lifecycle(
    value: &str,
) -> Result<ActivationLifecycle, RepositoryError> {
    match value {
        "created" => Ok(ActivationLifecycle::Created),
        "ready" => Ok(ActivationLifecycle::Ready),
        "leased" => Ok(ActivationLifecycle::Leased),
        "running" => Ok(ActivationLifecycle::Running),
        "retry_wait" => Ok(ActivationLifecycle::RetryWait),
        "waiting" => Ok(ActivationLifecycle::Waiting),
        "terminating" => Ok(ActivationLifecycle::Terminating),
        "succeeded" => Ok(ActivationLifecycle::Succeeded),
        "failed" => Ok(ActivationLifecycle::Failed),
        "cancelled" => Ok(ActivationLifecycle::Cancelled),
        "timed_out" => Ok(ActivationLifecycle::TimedOut),
        _ => Err(RepositoryError::invalid_data()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn run_id() -> RunId {
        RunId::new("run_repository_authority").unwrap()
    }

    fn activation_id() -> ActivationId {
        ActivationId::new("activation_repository_authority").unwrap()
    }

    fn receipt(version: u64) -> ActivationCommitReceipt {
        ActivationCommitReceipt::new(
            1,
            ExecutionEventId::random().as_str().to_owned(),
            version,
            None,
        )
    }

    #[test]
    fn terminal_authority_binds_only_the_matching_aggregate_proof() {
        let run_id = run_id();
        let activation_id = activation_id();
        let scope_id = ScopeInstanceId::root();
        let output = ValueRef::inline(json!({"answer": 42})).unwrap();
        let authority = CommittedTerminalActivationAuthority::new(
            receipt(4),
            run_id.clone(),
            scope_id.clone(),
            activation_id.clone(),
            CommittedTerminalActivationResult::Succeeded {
                content_hash: output.content_hash().clone(),
                output: output.clone(),
            },
        )
        .unwrap();
        let matching = crate::engine::TerminalActivationProof::mint(
            run_id.clone(),
            scope_id.clone(),
            activation_id.clone(),
            ActivationLifecycle::Succeeded,
            true,
            crate::engine::TerminalActivationResult::Succeeded {
                content_hash: output.content_hash().clone(),
                output: output.clone(),
            },
        )
        .unwrap();
        let committed = authority.clone().bind(matching).unwrap();
        assert_eq!(
            committed
                .authority()
                .receipt()
                .activation_projection_version(),
            4
        );

        let mismatched = crate::engine::TerminalActivationProof::mint(
            run_id,
            scope_id,
            activation_id,
            ActivationLifecycle::Succeeded,
            true,
            crate::engine::TerminalActivationResult::Succeeded {
                output: ValueRef::inline(json!("different")).unwrap(),
                content_hash: ValueRef::inline(json!("different"))
                    .unwrap()
                    .content_hash()
                    .clone(),
            },
        )
        .unwrap();
        assert_eq!(
            authority.bind(mismatched).unwrap_err().code(),
            super::super::REPOSITORY_DATA_INVALID
        );
    }

    #[test]
    fn wait_authority_mints_an_unfenced_committed_output_proof() {
        let registration = TransitionKey::derive("wait.register", &["authority"]).unwrap();
        let winning = TransitionKey::derive("wait.resolve", &["authority"]).unwrap();
        let signal_id = SignalId::new("signal_repository_authority").unwrap();
        let output = ValueRef::inline(json!([1, 2, 3])).unwrap();
        let authority = WaitResolutionAuthority::new(
            receipt(2),
            run_id(),
            ScopeInstanceId::root(),
            activation_id(),
            registration.clone(),
            crate::engine::WaitResolutionSubject::Signal(signal_id.clone()),
            winning.clone(),
            output.clone(),
        )
        .unwrap();
        let proof = authority.mint_proof().unwrap();
        assert_eq!(proof.registration_transition_key(), &registration);
        assert_eq!(proof.winning_transition_key(), &winning);
        assert_eq!(
            proof.subject(),
            &crate::engine::WaitResolutionSubject::Signal(signal_id)
        );
        assert_eq!(proof.output().value(), &output);
    }
}
