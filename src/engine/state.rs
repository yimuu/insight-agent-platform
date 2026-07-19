use serde::{Deserialize, Deserializer, Serialize};

use super::{AttemptNo, LeaseEpoch, ModelError};

pub const STATE_INVALID_TRANSITION: &str = "ENGINE_STATE_INVALID_TRANSITION";
pub const STATE_TERMINAL: &str = "ENGINE_STATE_TERMINAL";
pub const LEASE_FENCED: &str = "ENGINE_LEASE_FENCED";
pub const LEASE_EPOCH_NOT_MONOTONIC: &str = "ENGINE_LEASE_EPOCH_NOT_MONOTONIC";
pub const EFFECT_EVIDENCE_INVALID: &str = "ENGINE_EFFECT_EVIDENCE_INVALID";
pub const EFFECT_RETRY_UNSAFE: &str = "ENGINE_EFFECT_RETRY_UNSAFE";
pub const TERMINATION_INTENT_MISSING: &str = "ENGINE_TERMINATION_INTENT_MISSING";

fn invalid_transition(subject: &str, current: &str, requested: &str) -> ModelError {
    ModelError::new(
        STATE_INVALID_TRANSITION,
        format!("{subject} cannot transition from {current} to {requested}"),
    )
}

fn terminal_state(subject: &str) -> ModelError {
    ModelError::new(
        STATE_TERMINAL,
        format!("{subject} is terminal and cannot be reopened"),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunLifecycle {
    Created,
    Active,
    Waiting,
    Completing,
    Terminating,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
    TimedOut,
}

impl RunLifecycle {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Interrupted | Self::TimedOut
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Active => "active",
            Self::Waiting => "waiting",
            Self::Completing => "completing",
            Self::Terminating => "terminating",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
            Self::TimedOut => "timed_out",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionState {
    Open,
    Paused,
    Draining,
    Closed,
}

impl AdmissionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Paused => "paused",
            Self::Draining => "draining",
            Self::Closed => "closed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationReason {
    Failure,
    Cancelled,
    Interrupted,
    TimedOut,
}

impl TerminationReason {
    fn terminal_lifecycle(self) -> RunLifecycle {
        match self {
            Self::Failure => RunLifecycle::Failed,
            Self::Cancelled => RunLifecycle::Cancelled,
            Self::Interrupted => RunLifecycle::Interrupted,
            Self::TimedOut => RunLifecycle::TimedOut,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminationIntent {
    pub reason: TerminationReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationClaim {
    Claimed(TerminationIntent),
    Existing(TerminationIntent),
}

/// Pure Run lifecycle model. Persistence and child-drain accounting live above
/// this type; callers may close admission only after the control tracker proves
/// that every required child has settled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunState {
    lifecycle: RunLifecycle,
    admission: AdmissionState,
    termination_intent: Option<TerminationIntent>,
}

impl<'de> Deserialize<'de> for RunState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            lifecycle: RunLifecycle,
            admission: AdmissionState,
            termination_intent: Option<TerminationIntent>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let state = Self {
            lifecycle: wire.lifecycle,
            admission: wire.admission,
            termination_intent: wire.termination_intent,
        };
        state.validate().map_err(serde::de::Error::custom)?;
        Ok(state)
    }
}

impl RunState {
    pub fn new() -> Self {
        Self {
            lifecycle: RunLifecycle::Created,
            admission: AdmissionState::Open,
            termination_intent: None,
        }
    }

    pub fn lifecycle(&self) -> RunLifecycle {
        self.lifecycle
    }

    pub fn admission(&self) -> AdmissionState {
        self.admission
    }

    pub fn termination_intent(&self) -> Option<TerminationIntent> {
        self.termination_intent
    }

    pub fn can_dispatch_task(&self) -> bool {
        self.lifecycle == RunLifecycle::Active && self.admission == AdmissionState::Open
    }

    pub fn start(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("run")?;
        if self.lifecycle != RunLifecycle::Created {
            return Err(invalid_transition(
                "run lifecycle",
                self.lifecycle.as_str(),
                RunLifecycle::Active.as_str(),
            ));
        }
        if self.admission != AdmissionState::Open {
            return Err(invalid_transition(
                "run admission",
                self.admission.as_str(),
                AdmissionState::Open.as_str(),
            ));
        }
        self.lifecycle = RunLifecycle::Active;
        Ok(())
    }

    pub fn enter_waiting(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("run")?;
        if self.lifecycle != RunLifecycle::Active {
            return Err(invalid_transition(
                "run lifecycle",
                self.lifecycle.as_str(),
                RunLifecycle::Waiting.as_str(),
            ));
        }
        self.lifecycle = RunLifecycle::Waiting;
        Ok(())
    }

    pub fn leave_waiting(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("run")?;
        if self.lifecycle != RunLifecycle::Waiting {
            return Err(invalid_transition(
                "run lifecycle",
                self.lifecycle.as_str(),
                RunLifecycle::Active.as_str(),
            ));
        }
        self.lifecycle = RunLifecycle::Active;
        Ok(())
    }

    /// Pausing changes admission only. In particular it cannot settle or skip a
    /// business Wait represented by `RunLifecycle::Waiting`.
    pub fn pause(&mut self) -> Result<bool, ModelError> {
        self.ensure_not_terminal("run")?;
        match self.admission {
            AdmissionState::Open => {
                self.admission = AdmissionState::Paused;
                Ok(true)
            }
            AdmissionState::Paused => Ok(false),
            AdmissionState::Draining | AdmissionState::Closed => Err(invalid_transition(
                "run admission",
                self.admission.as_str(),
                AdmissionState::Paused.as_str(),
            )),
        }
    }

    /// Resuming opens admission only. It deliberately leaves a business Wait
    /// untouched; only the matching signal/timer transition may settle it.
    pub fn resume(&mut self) -> Result<bool, ModelError> {
        self.ensure_not_terminal("run")?;
        match self.admission {
            AdmissionState::Paused => {
                self.admission = AdmissionState::Open;
                Ok(true)
            }
            AdmissionState::Open => Ok(false),
            AdmissionState::Draining | AdmissionState::Closed => Err(invalid_transition(
                "run admission",
                self.admission.as_str(),
                AdmissionState::Open.as_str(),
            )),
        }
    }

    pub fn begin_completing(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("run")?;
        if !matches!(self.lifecycle, RunLifecycle::Active | RunLifecycle::Waiting) {
            return Err(invalid_transition(
                "run lifecycle",
                self.lifecycle.as_str(),
                RunLifecycle::Completing.as_str(),
            ));
        }
        self.lifecycle = RunLifecycle::Completing;
        self.begin_drain();
        Ok(())
    }

    /// First-winner claim for cancel, timeout, interruption, or fatal failure.
    /// A later request observes the authoritative intent without changing it.
    pub fn request_termination(
        &mut self,
        reason: TerminationReason,
    ) -> Result<TerminationClaim, ModelError> {
        self.ensure_not_terminal("run")?;
        if let Some(intent) = self.termination_intent {
            return Ok(TerminationClaim::Existing(intent));
        }
        if !matches!(
            self.lifecycle,
            RunLifecycle::Created
                | RunLifecycle::Active
                | RunLifecycle::Waiting
                | RunLifecycle::Completing
        ) {
            return Err(invalid_transition(
                "run lifecycle",
                self.lifecycle.as_str(),
                RunLifecycle::Terminating.as_str(),
            ));
        }
        let intent = TerminationIntent { reason };
        self.termination_intent = Some(intent);
        self.lifecycle = RunLifecycle::Terminating;
        self.begin_drain();
        Ok(TerminationClaim::Claimed(intent))
    }

    /// Called only after the durable control tracker proves that no admitted
    /// child or live Attempt remains. Closed admission is the gate for terminal
    /// commit in both the success and termination paths.
    pub fn finish_drain(&mut self) -> Result<bool, ModelError> {
        self.ensure_not_terminal("run")?;
        match self.admission {
            AdmissionState::Draining => {
                self.admission = AdmissionState::Closed;
                Ok(true)
            }
            AdmissionState::Closed => Ok(false),
            AdmissionState::Open | AdmissionState::Paused => Err(invalid_transition(
                "run admission",
                self.admission.as_str(),
                AdmissionState::Closed.as_str(),
            )),
        }
    }

    pub fn finish_success(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("run")?;
        if self.lifecycle != RunLifecycle::Completing {
            return Err(invalid_transition(
                "run lifecycle",
                self.lifecycle.as_str(),
                RunLifecycle::Succeeded.as_str(),
            ));
        }
        if self.termination_intent.is_some() {
            return Err(ModelError::new(
                STATE_INVALID_TRANSITION,
                "run with a termination intent cannot commit success",
            ));
        }
        self.require_closed_admission()?;
        self.lifecycle = RunLifecycle::Succeeded;
        Ok(())
    }

    pub fn finish_termination(&mut self) -> Result<RunLifecycle, ModelError> {
        self.ensure_not_terminal("run")?;
        if self.lifecycle != RunLifecycle::Terminating {
            return Err(invalid_transition(
                "run lifecycle",
                self.lifecycle.as_str(),
                "termination terminal",
            ));
        }
        self.require_closed_admission()?;
        let intent = self.termination_intent.ok_or_else(|| {
            ModelError::new(
                TERMINATION_INTENT_MISSING,
                "terminating run has no authoritative termination intent",
            )
        })?;
        self.lifecycle = intent.reason.terminal_lifecycle();
        Ok(self.lifecycle)
    }

    pub fn validate(&self) -> Result<(), ModelError> {
        let admission_is_reachable = match self.lifecycle {
            RunLifecycle::Created | RunLifecycle::Active | RunLifecycle::Waiting => matches!(
                self.admission,
                AdmissionState::Open | AdmissionState::Paused
            ),
            RunLifecycle::Completing | RunLifecycle::Terminating => matches!(
                self.admission,
                AdmissionState::Draining | AdmissionState::Closed
            ),
            RunLifecycle::Succeeded
            | RunLifecycle::Failed
            | RunLifecycle::Cancelled
            | RunLifecycle::Interrupted
            | RunLifecycle::TimedOut => self.admission == AdmissionState::Closed,
        };
        if !admission_is_reachable {
            return Err(ModelError::new(
                STATE_INVALID_TRANSITION,
                format!(
                    "run lifecycle {} cannot carry admission {}",
                    self.lifecycle.as_str(),
                    self.admission.as_str()
                ),
            ));
        }

        let required_reason = match self.lifecycle {
            RunLifecycle::Terminating => None,
            RunLifecycle::Failed => Some(TerminationReason::Failure),
            RunLifecycle::Cancelled => Some(TerminationReason::Cancelled),
            RunLifecycle::Interrupted => Some(TerminationReason::Interrupted),
            RunLifecycle::TimedOut => Some(TerminationReason::TimedOut),
            RunLifecycle::Created
            | RunLifecycle::Active
            | RunLifecycle::Waiting
            | RunLifecycle::Completing
            | RunLifecycle::Succeeded => {
                if self.termination_intent.is_some() {
                    return Err(ModelError::new(
                        STATE_INVALID_TRANSITION,
                        "non-termination lifecycle cannot carry a termination intent",
                    ));
                }
                return Ok(());
            }
        };
        let intent = self.termination_intent.ok_or_else(|| {
            ModelError::new(
                TERMINATION_INTENT_MISSING,
                "termination lifecycle requires an authoritative intent",
            )
        })?;
        if required_reason.is_some_and(|reason| intent.reason != reason) {
            return Err(ModelError::new(
                STATE_INVALID_TRANSITION,
                format!(
                    "run lifecycle {} does not match termination reason {:?}",
                    self.lifecycle.as_str(),
                    intent.reason
                ),
            ));
        }
        Ok(())
    }

    fn begin_drain(&mut self) {
        if matches!(
            self.admission,
            AdmissionState::Open | AdmissionState::Paused
        ) {
            self.admission = AdmissionState::Draining;
        }
    }

    fn require_closed_admission(&self) -> Result<(), ModelError> {
        if self.admission != AdmissionState::Closed {
            return Err(invalid_transition(
                "run admission",
                self.admission.as_str(),
                AdmissionState::Closed.as_str(),
            ));
        }
        Ok(())
    }

    fn ensure_not_terminal(&self, subject: &str) -> Result<(), ModelError> {
        if self.lifecycle.is_terminal() {
            return Err(terminal_state(subject));
        }
        Ok(())
    }
}

impl Default for RunState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectIdempotency {
    Idempotent,
    NonIdempotent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EffectEvidence {
    #[default]
    NotStarted,
    Started,
    Committed,
    Unknown,
}

impl EffectEvidence {
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::NotStarted, Self::Started)
                    | (Self::NotStarted, Self::Unknown)
                    | (Self::Started, Self::Committed)
                    | (Self::Started, Self::Unknown)
            )
    }

    /// Losing a worker after the effect began makes its outcome ambiguous.
    /// A committed marker stays committed, and an effect never started stays
    /// safe to retry.
    pub fn after_lease_loss(self) -> Self {
        match self {
            Self::Started => Self::Unknown,
            Self::NotStarted | Self::Committed | Self::Unknown => self,
        }
    }

    pub fn permits_automatic_retry(self, idempotency: EffectIdempotency) -> bool {
        idempotency == EffectIdempotency::Idempotent || self == Self::NotStarted
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LeaseFence {
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
}

impl LeaseFence {
    pub fn new(attempt_no: AttemptNo, lease_epoch: LeaseEpoch) -> Result<Self, ModelError> {
        if lease_epoch.get() < u64::from(attempt_no.get()) {
            return Err(ModelError::new(
                LEASE_EPOCH_NOT_MONOTONIC,
                "lease epoch cannot be lower than its attempt number",
            ));
        }
        Ok(Self {
            attempt_no,
            lease_epoch,
        })
    }

    pub fn attempt_no(self) -> AttemptNo {
        self.attempt_no
    }

    pub fn lease_epoch(self) -> LeaseEpoch {
        self.lease_epoch
    }
}

impl<'de> Deserialize<'de> for LeaseFence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            attempt_no: AttemptNo,
            lease_epoch: LeaseEpoch,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.attempt_no, wire.lease_epoch).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionDisposition {
    Current,
    AlreadyApplied,
    Fenced,
    NotRunning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationLifecycle {
    Created,
    Ready,
    Leased,
    Running,
    RetryWait,
    Waiting,
    Terminating,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl ActivationLifecycle {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Ready => "ready",
            Self::Leased => "leased",
            Self::Running => "running",
            Self::RetryWait => "retry_wait",
            Self::Waiting => "waiting",
            Self::Terminating => "terminating",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationTerminationReason {
    Failure,
    Cancelled,
    TimedOut,
    EffectOutcomeUnknown,
}

impl ActivationTerminationReason {
    fn terminal_lifecycle(self) -> ActivationLifecycle {
        match self {
            Self::Failure | Self::EffectOutcomeUnknown => ActivationLifecycle::Failed,
            Self::Cancelled => ActivationLifecycle::Cancelled,
            Self::TimedOut => ActivationLifecycle::TimedOut,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseExpiryDisposition {
    RetryWait,
    RetryBlocked,
}

/// Logical execution of one Plan node. Retry creates a new Attempt while this
/// Activation and its stable effect identity remain unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ActivationState {
    lifecycle: ActivationLifecycle,
    effect_idempotency: EffectIdempotency,
    effect_evidence: EffectEvidence,
    current_fence: Option<LeaseFence>,
    last_attempt: Option<AttemptNo>,
    last_lease_epoch: Option<LeaseEpoch>,
    last_applied_fence: Option<LeaseFence>,
    termination_reason: Option<ActivationTerminationReason>,
}

impl<'de> Deserialize<'de> for ActivationState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            lifecycle: ActivationLifecycle,
            effect_idempotency: EffectIdempotency,
            effect_evidence: EffectEvidence,
            current_fence: Option<LeaseFence>,
            last_attempt: Option<AttemptNo>,
            last_lease_epoch: Option<LeaseEpoch>,
            last_applied_fence: Option<LeaseFence>,
            termination_reason: Option<ActivationTerminationReason>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let state = Self {
            lifecycle: wire.lifecycle,
            effect_idempotency: wire.effect_idempotency,
            effect_evidence: wire.effect_evidence,
            current_fence: wire.current_fence,
            last_attempt: wire.last_attempt,
            last_lease_epoch: wire.last_lease_epoch,
            last_applied_fence: wire.last_applied_fence,
            termination_reason: wire.termination_reason,
        };
        state.validate().map_err(serde::de::Error::custom)?;
        Ok(state)
    }
}

impl ActivationState {
    pub fn new(effect_idempotency: EffectIdempotency) -> Self {
        Self {
            lifecycle: ActivationLifecycle::Created,
            effect_idempotency,
            effect_evidence: EffectEvidence::NotStarted,
            current_fence: None,
            last_attempt: None,
            last_lease_epoch: None,
            last_applied_fence: None,
            termination_reason: None,
        }
    }

    pub fn lifecycle(&self) -> ActivationLifecycle {
        self.lifecycle
    }

    pub fn effect_idempotency(&self) -> EffectIdempotency {
        self.effect_idempotency
    }

    pub fn effect_evidence(&self) -> EffectEvidence {
        self.effect_evidence
    }

    pub fn current_fence(&self) -> Option<LeaseFence> {
        self.current_fence
    }

    pub(crate) fn last_attempt(&self) -> Option<AttemptNo> {
        self.last_attempt
    }

    pub(crate) fn last_lease_epoch(&self) -> Option<LeaseEpoch> {
        self.last_lease_epoch
    }

    pub fn termination_reason(&self) -> Option<ActivationTerminationReason> {
        self.termination_reason
    }

    pub fn validate(&self) -> Result<(), ModelError> {
        let last_fence = match (self.last_attempt, self.last_lease_epoch) {
            (None, None) => None,
            (Some(attempt_no), Some(lease_epoch)) => {
                if lease_epoch.get() < u64::from(attempt_no.get()) {
                    return Err(ModelError::new(
                        LEASE_EPOCH_NOT_MONOTONIC,
                        "activation lease epoch cannot represent its attempt history",
                    ));
                }
                Some(LeaseFence::new(attempt_no, lease_epoch)?)
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(ModelError::new(
                    STATE_INVALID_TRANSITION,
                    "activation last attempt and lease epoch must be present together",
                ));
            }
        };

        if self.lifecycle == ActivationLifecycle::Created && last_fence.is_some() {
            return Err(ModelError::new(
                STATE_INVALID_TRANSITION,
                "created activation cannot carry attempt history",
            ));
        }
        if last_fence.is_none() && self.effect_evidence != EffectEvidence::NotStarted {
            return Err(ModelError::new(
                EFFECT_EVIDENCE_INVALID,
                "activation without attempt history cannot carry effect evidence",
            ));
        }
        if self.lifecycle == ActivationLifecycle::RetryWait && last_fence.is_none() {
            return Err(ModelError::new(
                STATE_INVALID_TRANSITION,
                "retry-wait activation requires attempt history",
            ));
        }

        let requires_current_fence = matches!(
            self.lifecycle,
            ActivationLifecycle::Leased | ActivationLifecycle::Running
        );
        if requires_current_fence != self.current_fence.is_some() {
            return Err(ModelError::new(
                STATE_INVALID_TRANSITION,
                format!(
                    "activation lifecycle {} has inconsistent current lease fence",
                    self.lifecycle.as_str()
                ),
            ));
        }
        if let Some(current) = self.current_fence {
            if Some(current) != last_fence {
                return Err(ModelError::new(
                    LEASE_FENCED,
                    "activation current fence must identify its latest attempt and lease epoch",
                ));
            }
            if self.last_applied_fence == Some(current) {
                return Err(ModelError::new(
                    LEASE_FENCED,
                    "activation current fence cannot already be applied",
                ));
            }
        }

        if let Some(applied) = self.last_applied_fence {
            let latest = last_fence.ok_or_else(|| {
                ModelError::new(
                    LEASE_FENCED,
                    "activation applied fence requires attempt history",
                )
            })?;
            if applied.attempt_no() > latest.attempt_no()
                || applied.lease_epoch() > latest.lease_epoch()
            {
                return Err(ModelError::new(
                    LEASE_FENCED,
                    "activation applied fence is outside its monotonic lease history",
                ));
            }

            let attempt_gap = u64::from(latest.attempt_no().get() - applied.attempt_no().get());
            let epoch_gap = latest.lease_epoch().get() - applied.lease_epoch().get();
            if epoch_gap < attempt_gap
                || (attempt_gap == 0 && applied.lease_epoch() != latest.lease_epoch())
            {
                return Err(ModelError::new(
                    LEASE_EPOCH_NOT_MONOTONIC,
                    "activation applied fence cannot belong to its attempt sequence",
                ));
            }
        }

        match self.lifecycle {
            ActivationLifecycle::Terminating => {
                if self.termination_reason.is_none() {
                    return Err(ModelError::new(
                        TERMINATION_INTENT_MISSING,
                        "terminating activation requires a termination reason",
                    ));
                }
            }
            ActivationLifecycle::Failed => {
                if !matches!(
                    self.termination_reason,
                    Some(
                        ActivationTerminationReason::Failure
                            | ActivationTerminationReason::EffectOutcomeUnknown
                    )
                ) {
                    return Err(ModelError::new(
                        STATE_INVALID_TRANSITION,
                        "failed activation requires a matching termination reason",
                    ));
                }
            }
            ActivationLifecycle::Cancelled => {
                if self.termination_reason != Some(ActivationTerminationReason::Cancelled) {
                    return Err(ModelError::new(
                        STATE_INVALID_TRANSITION,
                        "cancelled activation requires a cancelled termination reason",
                    ));
                }
            }
            ActivationLifecycle::TimedOut => {
                if self.termination_reason != Some(ActivationTerminationReason::TimedOut) {
                    return Err(ModelError::new(
                        STATE_INVALID_TRANSITION,
                        "timed-out activation requires a timed-out termination reason",
                    ));
                }
            }
            ActivationLifecycle::Created
            | ActivationLifecycle::Ready
            | ActivationLifecycle::Leased
            | ActivationLifecycle::Running
            | ActivationLifecycle::RetryWait
            | ActivationLifecycle::Waiting
            | ActivationLifecycle::Succeeded => {
                if self.termination_reason.is_some() {
                    return Err(ModelError::new(
                        STATE_INVALID_TRANSITION,
                        "non-termination activation cannot carry a termination reason",
                    ));
                }
            }
        }

        if self.effect_idempotency == EffectIdempotency::NonIdempotent
            && matches!(
                self.lifecycle,
                ActivationLifecycle::Ready
                    | ActivationLifecycle::RetryWait
                    | ActivationLifecycle::Waiting
            )
            && self.effect_evidence != EffectEvidence::NotStarted
        {
            return Err(ModelError::new(
                EFFECT_RETRY_UNSAFE,
                "non-idempotent activation cannot wait after ambiguous effect evidence",
            ));
        }
        if self.effect_idempotency == EffectIdempotency::NonIdempotent
            && self.lifecycle == ActivationLifecycle::Succeeded
            && self.last_applied_fence != last_fence
            && self.effect_evidence != EffectEvidence::NotStarted
        {
            return Err(ModelError::new(
                EFFECT_RETRY_UNSAFE,
                "non-idempotent native success cannot retain prior effect evidence",
            ));
        }

        Ok(())
    }

    pub fn make_ready(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("activation")?;
        if self.lifecycle != ActivationLifecycle::Created {
            return Err(invalid_transition(
                "activation",
                self.lifecycle.as_str(),
                ActivationLifecycle::Ready.as_str(),
            ));
        }
        self.lifecycle = ActivationLifecycle::Ready;
        Ok(())
    }

    /// Claims the next logical Attempt with a monotonically increasing lease
    /// epoch. The returned fence must accompany every heartbeat/completion.
    pub fn claim(&mut self, lease_epoch: LeaseEpoch) -> Result<LeaseFence, ModelError> {
        self.ensure_not_terminal("activation")?;
        if self.lifecycle != ActivationLifecycle::Ready {
            return Err(invalid_transition(
                "activation",
                self.lifecycle.as_str(),
                ActivationLifecycle::Leased.as_str(),
            ));
        }
        if self
            .last_lease_epoch
            .is_some_and(|previous| lease_epoch <= previous)
        {
            return Err(ModelError::new(
                LEASE_EPOCH_NOT_MONOTONIC,
                "activation lease epoch must increase monotonically",
            ));
        }
        let attempt_no = self
            .last_attempt
            .map(AttemptNo::next)
            .transpose()?
            .unwrap_or(AttemptNo::FIRST);
        let fence = LeaseFence::new(attempt_no, lease_epoch)?;
        self.lifecycle = ActivationLifecycle::Leased;
        self.current_fence = Some(fence);
        self.last_attempt = Some(attempt_no);
        self.last_lease_epoch = Some(lease_epoch);
        self.effect_evidence = EffectEvidence::NotStarted;
        Ok(fence)
    }

    pub fn mark_running(&mut self, fence: LeaseFence) -> Result<(), ModelError> {
        self.ensure_current_fence(fence)?;
        if self.lifecycle != ActivationLifecycle::Leased {
            return Err(invalid_transition(
                "activation",
                self.lifecycle.as_str(),
                ActivationLifecycle::Running.as_str(),
            ));
        }
        self.lifecycle = ActivationLifecycle::Running;
        Ok(())
    }

    pub fn record_effect_evidence(
        &mut self,
        fence: LeaseFence,
        evidence: EffectEvidence,
    ) -> Result<bool, ModelError> {
        self.ensure_current_fence(fence)?;
        if !matches!(
            self.lifecycle,
            ActivationLifecycle::Leased | ActivationLifecycle::Running
        ) {
            return Err(invalid_transition(
                "activation effect evidence",
                self.lifecycle.as_str(),
                "leased_or_running",
            ));
        }
        if !self.effect_evidence.can_transition_to(evidence) {
            return Err(ModelError::new(
                EFFECT_EVIDENCE_INVALID,
                "effect evidence cannot regress or leave an ambiguous outcome",
            ));
        }
        let changed = self.effect_evidence != evidence;
        self.effect_evidence = evidence;
        Ok(changed)
    }

    pub fn complete_success(&mut self, fence: LeaseFence) -> Result<(), ModelError> {
        self.ensure_current_fence(fence)?;
        if self.lifecycle != ActivationLifecycle::Running {
            return Err(invalid_transition(
                "activation",
                self.lifecycle.as_str(),
                ActivationLifecycle::Succeeded.as_str(),
            ));
        }
        self.lifecycle = ActivationLifecycle::Succeeded;
        self.last_applied_fence = Some(fence);
        self.current_fence = None;
        Ok(())
    }

    pub fn enter_retry_wait(
        &mut self,
        fence: LeaseFence,
        evidence: EffectEvidence,
    ) -> Result<(), ModelError> {
        self.ensure_current_fence(fence)?;
        if !matches!(
            self.lifecycle,
            ActivationLifecycle::Leased | ActivationLifecycle::Running
        ) {
            return Err(invalid_transition(
                "activation",
                self.lifecycle.as_str(),
                ActivationLifecycle::RetryWait.as_str(),
            ));
        }
        if !self.effect_evidence.can_transition_to(evidence) {
            return Err(ModelError::new(
                EFFECT_EVIDENCE_INVALID,
                "effect evidence cannot regress or leave an ambiguous outcome",
            ));
        }
        if !evidence.permits_automatic_retry(self.effect_idempotency) {
            return Err(ModelError::new(
                EFFECT_RETRY_UNSAFE,
                "non-idempotent effect evidence does not permit automatic retry",
            ));
        }
        self.effect_evidence = evidence;
        self.lifecycle = ActivationLifecycle::RetryWait;
        self.last_applied_fence = Some(fence);
        self.current_fence = None;
        Ok(())
    }

    /// Fences the expired worker before deciding whether a retry is safe. The
    /// old fence is retained only as audit identity; all later completion calls
    /// for it are classified as `Fenced`.
    pub fn lease_expired(
        &mut self,
        fence: LeaseFence,
        evidence: EffectEvidence,
    ) -> Result<LeaseExpiryDisposition, ModelError> {
        self.ensure_current_fence(fence)?;
        if !matches!(
            self.lifecycle,
            ActivationLifecycle::Leased | ActivationLifecycle::Running
        ) {
            return Err(invalid_transition(
                "activation lease",
                self.lifecycle.as_str(),
                "expired",
            ));
        }
        if !self.effect_evidence.can_transition_to(evidence)
            && self.effect_evidence.after_lease_loss() != evidence
        {
            return Err(ModelError::new(
                EFFECT_EVIDENCE_INVALID,
                "lease expiry carried inconsistent effect evidence",
            ));
        }
        self.effect_evidence = evidence;
        self.current_fence = None;
        if evidence.permits_automatic_retry(self.effect_idempotency) {
            self.lifecycle = ActivationLifecycle::RetryWait;
            Ok(LeaseExpiryDisposition::RetryWait)
        } else {
            self.lifecycle = ActivationLifecycle::Terminating;
            self.termination_reason = Some(ActivationTerminationReason::EffectOutcomeUnknown);
            Ok(LeaseExpiryDisposition::RetryBlocked)
        }
    }

    pub fn retry_due(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("activation")?;
        if self.lifecycle != ActivationLifecycle::RetryWait {
            return Err(invalid_transition(
                "activation",
                self.lifecycle.as_str(),
                ActivationLifecycle::Ready.as_str(),
            ));
        }
        self.lifecycle = ActivationLifecycle::Ready;
        self.current_fence = None;
        Ok(())
    }

    pub fn begin_wait(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("activation")?;
        if self.lifecycle != ActivationLifecycle::Ready {
            return Err(invalid_transition(
                "activation",
                self.lifecycle.as_str(),
                ActivationLifecycle::Waiting.as_str(),
            ));
        }
        self.lifecycle = ActivationLifecycle::Waiting;
        Ok(())
    }

    pub fn resolve_wait(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("activation")?;
        if self.lifecycle != ActivationLifecycle::Waiting {
            return Err(invalid_transition(
                "activation",
                self.lifecycle.as_str(),
                ActivationLifecycle::Succeeded.as_str(),
            ));
        }
        self.lifecycle = ActivationLifecycle::Succeeded;
        Ok(())
    }

    pub fn complete_native(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("activation")?;
        if self.lifecycle != ActivationLifecycle::Ready {
            return Err(invalid_transition(
                "activation",
                self.lifecycle.as_str(),
                ActivationLifecycle::Succeeded.as_str(),
            ));
        }
        self.lifecycle = ActivationLifecycle::Succeeded;
        Ok(())
    }

    pub fn begin_termination(
        &mut self,
        reason: ActivationTerminationReason,
    ) -> Result<(), ModelError> {
        self.ensure_not_terminal("activation")?;
        if self.lifecycle == ActivationLifecycle::Terminating {
            if self.termination_reason == Some(reason) {
                return Ok(());
            }
            return Err(ModelError::new(
                STATE_INVALID_TRANSITION,
                "activation already has a different termination reason",
            ));
        }
        self.lifecycle = ActivationLifecycle::Terminating;
        self.termination_reason = Some(reason);
        self.current_fence = None;
        Ok(())
    }

    pub fn finish_termination(&mut self) -> Result<ActivationLifecycle, ModelError> {
        self.ensure_not_terminal("activation")?;
        if self.lifecycle != ActivationLifecycle::Terminating {
            return Err(invalid_transition(
                "activation",
                self.lifecycle.as_str(),
                "termination terminal",
            ));
        }
        let reason = self.termination_reason.ok_or_else(|| {
            ModelError::new(
                TERMINATION_INTENT_MISSING,
                "terminating activation has no termination reason",
            )
        })?;
        self.lifecycle = reason.terminal_lifecycle();
        Ok(self.lifecycle)
    }

    pub fn classify_completion(&self, fence: LeaseFence) -> CompletionDisposition {
        if self.last_applied_fence == Some(fence) {
            return CompletionDisposition::AlreadyApplied;
        }
        if self.current_fence != Some(fence) {
            return CompletionDisposition::Fenced;
        }
        match self.lifecycle {
            ActivationLifecycle::Leased | ActivationLifecycle::Running => {
                CompletionDisposition::Current
            }
            ActivationLifecycle::Created
            | ActivationLifecycle::Ready
            | ActivationLifecycle::RetryWait
            | ActivationLifecycle::Waiting
            | ActivationLifecycle::Terminating
            | ActivationLifecycle::Succeeded
            | ActivationLifecycle::Failed
            | ActivationLifecycle::Cancelled
            | ActivationLifecycle::TimedOut => CompletionDisposition::Fenced,
        }
    }

    fn ensure_current_fence(&self, fence: LeaseFence) -> Result<(), ModelError> {
        if self.current_fence != Some(fence) {
            return Err(ModelError::new(
                LEASE_FENCED,
                "attempt lease fence is stale or no longer current",
            ));
        }
        Ok(())
    }

    fn ensure_not_terminal(&self, subject: &str) -> Result<(), ModelError> {
        if self.lifecycle.is_terminal() {
            return Err(terminal_state(subject));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptLifecycle {
    Created,
    Leased,
    Running,
    Succeeded,
    Failed,
    TimedOut,
    Abandoned,
    Cancelled,
}

impl AttemptLifecycle {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::TimedOut | Self::Abandoned | Self::Cancelled
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Leased => "leased",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Abandoned => "abandoned",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AttemptState {
    fence: LeaseFence,
    lifecycle: AttemptLifecycle,
    effect_evidence: EffectEvidence,
}

impl<'de> Deserialize<'de> for AttemptState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            fence: LeaseFence,
            lifecycle: AttemptLifecycle,
            effect_evidence: EffectEvidence,
        }

        let wire = Wire::deserialize(deserializer)?;
        let state = Self {
            fence: wire.fence,
            lifecycle: wire.lifecycle,
            effect_evidence: wire.effect_evidence,
        };
        state.validate().map_err(serde::de::Error::custom)?;
        Ok(state)
    }
}

impl AttemptState {
    pub fn new(fence: LeaseFence) -> Self {
        Self {
            fence,
            lifecycle: AttemptLifecycle::Created,
            effect_evidence: EffectEvidence::NotStarted,
        }
    }

    pub fn fence(&self) -> LeaseFence {
        self.fence
    }

    pub fn lifecycle(&self) -> AttemptLifecycle {
        self.lifecycle
    }

    pub fn effect_evidence(&self) -> EffectEvidence {
        self.effect_evidence
    }

    pub fn validate(&self) -> Result<(), ModelError> {
        if self.lifecycle == AttemptLifecycle::Created
            && self.effect_evidence != EffectEvidence::NotStarted
        {
            return Err(ModelError::new(
                EFFECT_EVIDENCE_INVALID,
                "created attempt cannot carry effect evidence",
            ));
        }
        if self.lifecycle == AttemptLifecycle::Abandoned
            && self.effect_evidence == EffectEvidence::Started
        {
            return Err(ModelError::new(
                EFFECT_EVIDENCE_INVALID,
                "abandoned attempt must resolve started effect evidence to unknown",
            ));
        }
        Ok(())
    }

    pub fn lease(&mut self) -> Result<(), ModelError> {
        self.ensure_not_terminal("attempt")?;
        if self.lifecycle != AttemptLifecycle::Created {
            return Err(invalid_transition(
                "attempt",
                self.lifecycle.as_str(),
                AttemptLifecycle::Leased.as_str(),
            ));
        }
        self.lifecycle = AttemptLifecycle::Leased;
        Ok(())
    }

    pub fn mark_running(&mut self, fence: LeaseFence) -> Result<(), ModelError> {
        self.ensure_fence(fence)?;
        self.ensure_not_terminal("attempt")?;
        if self.lifecycle != AttemptLifecycle::Leased {
            return Err(invalid_transition(
                "attempt",
                self.lifecycle.as_str(),
                AttemptLifecycle::Running.as_str(),
            ));
        }
        self.lifecycle = AttemptLifecycle::Running;
        Ok(())
    }

    pub fn record_effect_evidence(
        &mut self,
        fence: LeaseFence,
        evidence: EffectEvidence,
    ) -> Result<bool, ModelError> {
        self.ensure_fence(fence)?;
        self.ensure_not_terminal("attempt")?;
        if !matches!(
            self.lifecycle,
            AttemptLifecycle::Leased | AttemptLifecycle::Running
        ) {
            return Err(invalid_transition(
                "attempt effect evidence",
                self.lifecycle.as_str(),
                "leased_or_running",
            ));
        }
        if !self.effect_evidence.can_transition_to(evidence) {
            return Err(ModelError::new(
                EFFECT_EVIDENCE_INVALID,
                "effect evidence cannot regress or leave an ambiguous outcome",
            ));
        }
        let changed = self.effect_evidence != evidence;
        self.effect_evidence = evidence;
        Ok(changed)
    }

    pub fn complete_success(&mut self, fence: LeaseFence) -> Result<(), ModelError> {
        self.finish_from_running(fence, AttemptLifecycle::Succeeded)
    }

    pub fn complete_failure(&mut self, fence: LeaseFence) -> Result<(), ModelError> {
        self.finish_from_active(fence, AttemptLifecycle::Failed)
    }

    pub fn complete_timeout(&mut self, fence: LeaseFence) -> Result<(), ModelError> {
        self.finish_from_active(fence, AttemptLifecycle::TimedOut)
    }

    pub fn cancel(&mut self, fence: LeaseFence) -> Result<(), ModelError> {
        self.finish_from_active(fence, AttemptLifecycle::Cancelled)
    }

    pub fn abandon_after_lease_expiry(
        &mut self,
        fence: LeaseFence,
    ) -> Result<EffectEvidence, ModelError> {
        self.ensure_fence(fence)?;
        self.ensure_not_terminal("attempt")?;
        if !matches!(
            self.lifecycle,
            AttemptLifecycle::Leased | AttemptLifecycle::Running
        ) {
            return Err(invalid_transition(
                "attempt",
                self.lifecycle.as_str(),
                AttemptLifecycle::Abandoned.as_str(),
            ));
        }
        self.effect_evidence = self.effect_evidence.after_lease_loss();
        self.lifecycle = AttemptLifecycle::Abandoned;
        Ok(self.effect_evidence)
    }

    pub fn classify_completion(&self, fence: LeaseFence) -> CompletionDisposition {
        if self.fence != fence {
            return CompletionDisposition::Fenced;
        }
        match self.lifecycle {
            AttemptLifecycle::Leased | AttemptLifecycle::Running => CompletionDisposition::Current,
            AttemptLifecycle::Succeeded | AttemptLifecycle::Failed | AttemptLifecycle::TimedOut => {
                CompletionDisposition::AlreadyApplied
            }
            AttemptLifecycle::Abandoned | AttemptLifecycle::Cancelled => {
                CompletionDisposition::Fenced
            }
            AttemptLifecycle::Created => CompletionDisposition::NotRunning,
        }
    }

    fn finish_from_running(
        &mut self,
        fence: LeaseFence,
        terminal: AttemptLifecycle,
    ) -> Result<(), ModelError> {
        self.ensure_fence(fence)?;
        self.ensure_not_terminal("attempt")?;
        if self.lifecycle != AttemptLifecycle::Running {
            return Err(invalid_transition(
                "attempt",
                self.lifecycle.as_str(),
                terminal.as_str(),
            ));
        }
        self.lifecycle = terminal;
        Ok(())
    }

    fn finish_from_active(
        &mut self,
        fence: LeaseFence,
        terminal: AttemptLifecycle,
    ) -> Result<(), ModelError> {
        self.ensure_fence(fence)?;
        self.ensure_not_terminal("attempt")?;
        if !matches!(
            self.lifecycle,
            AttemptLifecycle::Leased | AttemptLifecycle::Running
        ) {
            return Err(invalid_transition(
                "attempt",
                self.lifecycle.as_str(),
                terminal.as_str(),
            ));
        }
        self.lifecycle = terminal;
        Ok(())
    }

    fn ensure_fence(&self, fence: LeaseFence) -> Result<(), ModelError> {
        if self.fence != fence {
            return Err(ModelError::new(
                LEASE_FENCED,
                "attempt lease fence is stale or belongs to another attempt",
            ));
        }
        Ok(())
    }

    fn ensure_not_terminal(&self, subject: &str) -> Result<(), ModelError> {
        if self.lifecycle.is_terminal() {
            return Err(terminal_state(subject));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn assert_json_round_trip<T>(value: &T)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_value(value).unwrap();
        let decoded = serde_json::from_value::<T>(encoded).unwrap();
        assert_eq!(&decoded, value);
    }

    fn epoch(value: u64) -> LeaseEpoch {
        LeaseEpoch::new(value).unwrap()
    }

    fn running_activation(
        idempotency: EffectIdempotency,
    ) -> (ActivationState, AttemptState, LeaseFence) {
        let mut activation = ActivationState::new(idempotency);
        activation.make_ready().unwrap();
        let fence = activation.claim(epoch(1)).unwrap();
        let mut attempt = AttemptState::new(fence);
        attempt.lease().unwrap();
        activation.mark_running(fence).unwrap();
        attempt.mark_running(fence).unwrap();
        (activation, attempt, fence)
    }

    #[test]
    fn validating_deserialization_round_trips_reachable_states_and_retry_history() {
        let mut run = RunState::new();
        assert_json_round_trip(&run);
        run.pause().unwrap();
        assert_json_round_trip(&run);
        run.resume().unwrap();
        run.start().unwrap();
        run.enter_waiting().unwrap();
        run.pause().unwrap();
        assert_json_round_trip(&run);
        run.resume().unwrap();
        run.begin_completing().unwrap();
        assert_json_round_trip(&run);
        run.finish_drain().unwrap();
        run.finish_success().unwrap();
        assert_json_round_trip(&run);

        let mut terminated_run = RunState::new();
        terminated_run
            .request_termination(TerminationReason::TimedOut)
            .unwrap();
        assert_json_round_trip(&terminated_run);
        terminated_run.finish_drain().unwrap();
        terminated_run.finish_termination().unwrap();
        assert_json_round_trip(&terminated_run);

        let mut activation = ActivationState::new(EffectIdempotency::Idempotent);
        assert_json_round_trip(&activation);
        activation.make_ready().unwrap();
        let first = activation.claim(epoch(2)).unwrap();
        activation.mark_running(first).unwrap();
        activation
            .record_effect_evidence(first, EffectEvidence::Started)
            .unwrap();
        activation
            .enter_retry_wait(first, EffectEvidence::Started)
            .unwrap();
        assert_json_round_trip(&activation);

        activation.retry_due().unwrap();
        let second = activation.claim(epoch(5)).unwrap();
        assert_json_round_trip(&activation);
        activation.mark_running(second).unwrap();
        activation
            .record_effect_evidence(second, EffectEvidence::Unknown)
            .unwrap();
        activation
            .lease_expired(second, EffectEvidence::Unknown)
            .unwrap();
        assert_json_round_trip(&activation);

        activation.retry_due().unwrap();
        let third = activation.claim(epoch(9)).unwrap();
        activation.mark_running(third).unwrap();
        activation.complete_success(third).unwrap();
        assert_json_round_trip(&activation);

        let fence = LeaseFence::new(AttemptNo::FIRST, epoch(4)).unwrap();
        let mut attempt = AttemptState::new(fence);
        assert_json_round_trip(&attempt);
        attempt.lease().unwrap();
        attempt
            .record_effect_evidence(fence, EffectEvidence::Started)
            .unwrap();
        attempt.abandon_after_lease_expiry(fence).unwrap();
        assert_json_round_trip(&attempt);
    }

    #[test]
    fn run_deserialization_rejects_unreachable_field_combinations() {
        let cases = [
            (
                "created with closed admission",
                serde_json::json!({
                    "lifecycle": "created",
                    "admission": "closed",
                    "termination_intent": null
                }),
            ),
            (
                "completing before admission drain",
                serde_json::json!({
                    "lifecycle": "completing",
                    "admission": "open",
                    "termination_intent": null
                }),
            ),
            (
                "terminating without a reason",
                serde_json::json!({
                    "lifecycle": "terminating",
                    "admission": "draining",
                    "termination_intent": null
                }),
            ),
            (
                "terminal with open admission",
                serde_json::json!({
                    "lifecycle": "succeeded",
                    "admission": "open",
                    "termination_intent": null
                }),
            ),
            (
                "terminal reason mismatch",
                serde_json::json!({
                    "lifecycle": "failed",
                    "admission": "closed",
                    "termination_intent": { "reason": "cancelled" }
                }),
            ),
            (
                "unknown run projection field",
                serde_json::json!({
                    "lifecycle": "created",
                    "admission": "open",
                    "termination_intent": null,
                    "future_field": true
                }),
            ),
        ];

        for (label, value) in cases {
            assert!(
                serde_json::from_value::<RunState>(value).is_err(),
                "accepted {label}"
            );
        }
    }

    #[test]
    fn activation_deserialization_rejects_unreachable_fences_and_history() {
        let leased = serde_json::json!({
            "lifecycle": "leased",
            "effect_idempotency": "idempotent",
            "effect_evidence": "not_started",
            "current_fence": { "attempt_no": 1, "lease_epoch": 1 },
            "last_attempt": 1,
            "last_lease_epoch": 1,
            "last_applied_fence": null,
            "termination_reason": null
        });
        let mut leased_without_current = leased.clone();
        leased_without_current["current_fence"] = serde_json::Value::Null;
        let mut retry_with_current = leased.clone();
        retry_with_current["lifecycle"] = serde_json::json!("retry_wait");
        let mut terminal_with_current = leased.clone();
        terminal_with_current["lifecycle"] = serde_json::json!("succeeded");
        let mut mismatched_current = leased.clone();
        mismatched_current["current_fence"]["lease_epoch"] = serde_json::json!(2);
        let mut already_applied_current = leased.clone();
        already_applied_current["last_applied_fence"] =
            serde_json::json!({ "attempt_no": 1, "lease_epoch": 1 });

        let cases = [
            (
                "created activation with evidence",
                serde_json::json!({
                    "lifecycle": "created",
                    "effect_idempotency": "idempotent",
                    "effect_evidence": "started",
                    "current_fence": null,
                    "last_attempt": null,
                    "last_lease_epoch": null,
                    "last_applied_fence": null,
                    "termination_reason": null
                }),
            ),
            (
                "leased activation without current fence",
                leased_without_current,
            ),
            ("retry wait retaining current fence", retry_with_current),
            (
                "terminal activation retaining current fence",
                terminal_with_current,
            ),
            (
                "current fence not matching latest history",
                mismatched_current,
            ),
            ("current fence already applied", already_applied_current),
            (
                "unpaired last attempt",
                serde_json::json!({
                    "lifecycle": "ready",
                    "effect_idempotency": "idempotent",
                    "effect_evidence": "not_started",
                    "current_fence": null,
                    "last_attempt": 1,
                    "last_lease_epoch": null,
                    "last_applied_fence": null,
                    "termination_reason": null
                }),
            ),
            (
                "lease epoch too small for attempt count",
                serde_json::json!({
                    "lifecycle": "ready",
                    "effect_idempotency": "idempotent",
                    "effect_evidence": "not_started",
                    "current_fence": null,
                    "last_attempt": 3,
                    "last_lease_epoch": 2,
                    "last_applied_fence": null,
                    "termination_reason": null
                }),
            ),
            (
                "applied fence cannot fit monotonic history",
                serde_json::json!({
                    "lifecycle": "ready",
                    "effect_idempotency": "idempotent",
                    "effect_evidence": "not_started",
                    "current_fence": null,
                    "last_attempt": 3,
                    "last_lease_epoch": 5,
                    "last_applied_fence": { "attempt_no": 1, "lease_epoch": 4 },
                    "termination_reason": null
                }),
            ),
            (
                "terminal lifecycle with mismatched reason",
                serde_json::json!({
                    "lifecycle": "failed",
                    "effect_idempotency": "idempotent",
                    "effect_evidence": "not_started",
                    "current_fence": null,
                    "last_attempt": null,
                    "last_lease_epoch": null,
                    "last_applied_fence": null,
                    "termination_reason": "cancelled"
                }),
            ),
            (
                "unsafe non-idempotent retry evidence",
                serde_json::json!({
                    "lifecycle": "retry_wait",
                    "effect_idempotency": "non_idempotent",
                    "effect_evidence": "unknown",
                    "current_fence": null,
                    "last_attempt": 1,
                    "last_lease_epoch": 1,
                    "last_applied_fence": null,
                    "termination_reason": null
                }),
            ),
            (
                "unknown activation projection field",
                serde_json::json!({
                    "lifecycle": "created",
                    "effect_idempotency": "idempotent",
                    "effect_evidence": "not_started",
                    "current_fence": null,
                    "last_attempt": null,
                    "last_lease_epoch": null,
                    "last_applied_fence": null,
                    "termination_reason": null,
                    "future_field": true
                }),
            ),
        ];

        for (label, value) in cases {
            assert!(
                serde_json::from_value::<ActivationState>(value).is_err(),
                "accepted {label}"
            );
        }
    }

    #[test]
    fn attempt_deserialization_rejects_impossible_effect_evidence() {
        for (label, value) in [
            (
                "created attempt with started evidence",
                serde_json::json!({
                    "fence": { "attempt_no": 1, "lease_epoch": 1 },
                    "lifecycle": "created",
                    "effect_evidence": "started"
                }),
            ),
            (
                "abandoned attempt with unresolved started evidence",
                serde_json::json!({
                    "fence": { "attempt_no": 1, "lease_epoch": 1 },
                    "lifecycle": "abandoned",
                    "effect_evidence": "started"
                }),
            ),
            (
                "unknown attempt projection field",
                serde_json::json!({
                    "fence": { "attempt_no": 1, "lease_epoch": 1 },
                    "lifecycle": "created",
                    "effect_evidence": "not_started",
                    "future_field": true
                }),
            ),
        ] {
            assert!(
                serde_json::from_value::<AttemptState>(value).is_err(),
                "accepted {label}"
            );
        }
    }

    #[test]
    fn lease_fence_rejects_impossible_history_and_unknown_wire_fields() {
        assert_eq!(
            LeaseFence::new(AttemptNo::new(2).unwrap(), epoch(1))
                .unwrap_err()
                .code(),
            LEASE_EPOCH_NOT_MONOTONIC
        );
        assert!(serde_json::from_value::<LeaseFence>(json!({
            "attempt_no": 2,
            "lease_epoch": 1
        }))
        .is_err());
        assert!(serde_json::from_value::<LeaseFence>(json!({
            "attempt_no": 1,
            "lease_epoch": 1,
            "unexpected": true
        }))
        .is_err());
    }

    #[test]
    fn pause_and_resume_do_not_settle_a_business_wait() {
        let mut run = RunState::new();
        run.start().unwrap();
        run.enter_waiting().unwrap();
        let mut wait = ActivationState::new(EffectIdempotency::Idempotent);
        wait.make_ready().unwrap();
        wait.begin_wait().unwrap();

        assert!(run.pause().unwrap());
        assert_eq!(run.lifecycle(), RunLifecycle::Waiting);
        assert_eq!(run.admission(), AdmissionState::Paused);
        assert_eq!(wait.lifecycle(), ActivationLifecycle::Waiting);
        assert!(!run.can_dispatch_task());

        assert!(run.resume().unwrap());
        assert_eq!(run.lifecycle(), RunLifecycle::Waiting);
        assert_eq!(wait.lifecycle(), ActivationLifecycle::Waiting);
        assert!(!run.can_dispatch_task());

        wait.resolve_wait().unwrap();
        run.leave_waiting().unwrap();
        assert_eq!(wait.lifecycle(), ActivationLifecycle::Succeeded);
        assert!(run.can_dispatch_task());
    }

    #[test]
    fn first_termination_intent_wins_and_requires_drain_before_terminal() {
        let mut run = RunState::new();
        run.start().unwrap();
        let cancelled = TerminationIntent {
            reason: TerminationReason::Cancelled,
        };
        assert_eq!(
            run.request_termination(TerminationReason::Cancelled)
                .unwrap(),
            TerminationClaim::Claimed(cancelled)
        );
        assert_eq!(run.admission(), AdmissionState::Draining);
        assert_eq!(
            run.request_termination(TerminationReason::TimedOut)
                .unwrap(),
            TerminationClaim::Existing(cancelled)
        );
        assert_eq!(run.termination_intent(), Some(cancelled));
        assert_eq!(
            run.finish_termination().unwrap_err().code(),
            STATE_INVALID_TRANSITION
        );

        run.finish_drain().unwrap();
        assert_eq!(run.finish_termination().unwrap(), RunLifecycle::Cancelled);
        run.validate().unwrap();
    }

    #[test]
    fn terminal_run_never_reopens_or_accepts_a_late_termination_intent() {
        let mut run = RunState::new();
        run.start().unwrap();
        run.begin_completing().unwrap();
        run.finish_drain().unwrap();
        run.finish_success().unwrap();

        for error in [
            run.resume().unwrap_err(),
            run.enter_waiting().unwrap_err(),
            run.request_termination(TerminationReason::Cancelled)
                .unwrap_err(),
        ] {
            assert_eq!(error.code(), STATE_TERMINAL);
        }
        assert_eq!(run.lifecycle(), RunLifecycle::Succeeded);
        assert_eq!(run.admission(), AdmissionState::Closed);
        assert_eq!(run.termination_intent(), None);
    }

    #[test]
    fn run_and_activation_reject_illegal_shortcuts() {
        let mut run = RunState::new();
        assert_eq!(
            run.enter_waiting().unwrap_err().code(),
            STATE_INVALID_TRANSITION
        );
        run.start().unwrap();
        assert_eq!(run.start().unwrap_err().code(), STATE_INVALID_TRANSITION);
        assert_eq!(
            run.finish_success().unwrap_err().code(),
            STATE_INVALID_TRANSITION
        );

        let mut activation = ActivationState::new(EffectIdempotency::Idempotent);
        assert_eq!(
            activation.complete_native().unwrap_err().code(),
            STATE_INVALID_TRANSITION
        );
        activation.make_ready().unwrap();
        activation.complete_native().unwrap();
        assert_eq!(activation.make_ready().unwrap_err().code(), STATE_TERMINAL);
    }

    #[test]
    fn attempt_follows_lease_running_terminal_and_does_not_reopen() {
        let fence = LeaseFence::new(AttemptNo::FIRST, epoch(1)).unwrap();
        let mut attempt = AttemptState::new(fence);
        assert_eq!(
            attempt.complete_success(fence).unwrap_err().code(),
            STATE_INVALID_TRANSITION
        );
        attempt.lease().unwrap();
        attempt.mark_running(fence).unwrap();
        attempt.complete_success(fence).unwrap();
        assert_eq!(attempt.lifecycle(), AttemptLifecycle::Succeeded);
        assert_eq!(
            attempt.classify_completion(fence),
            CompletionDisposition::AlreadyApplied
        );
        assert_eq!(
            attempt.complete_failure(fence).unwrap_err().code(),
            STATE_TERMINAL
        );
    }

    #[test]
    fn lease_expiry_fences_the_old_completion_and_retry_uses_new_identity() {
        let (mut activation, mut attempt, old_fence) =
            running_activation(EffectIdempotency::Idempotent);
        attempt
            .record_effect_evidence(old_fence, EffectEvidence::Started)
            .unwrap();
        activation
            .record_effect_evidence(old_fence, EffectEvidence::Started)
            .unwrap();
        let evidence = attempt.abandon_after_lease_expiry(old_fence).unwrap();
        assert_eq!(evidence, EffectEvidence::Unknown);
        assert_eq!(
            activation.lease_expired(old_fence, evidence).unwrap(),
            LeaseExpiryDisposition::RetryWait
        );
        assert_eq!(
            attempt.classify_completion(old_fence),
            CompletionDisposition::Fenced
        );
        assert_eq!(
            activation.classify_completion(old_fence),
            CompletionDisposition::Fenced
        );
        assert_eq!(
            activation.complete_success(old_fence).unwrap_err().code(),
            LEASE_FENCED
        );

        activation.retry_due().unwrap();
        let new_fence = activation.claim(epoch(2)).unwrap();
        assert_eq!(new_fence.attempt_no().get(), 2);
        assert_eq!(new_fence.lease_epoch().get(), 2);
        assert_eq!(
            activation.classify_completion(old_fence),
            CompletionDisposition::Fenced
        );
        assert_eq!(
            activation.classify_completion(new_fence),
            CompletionDisposition::Current
        );
        assert_eq!(
            activation.claim(epoch(2)).unwrap_err().code(),
            STATE_INVALID_TRANSITION
        );
    }

    #[test]
    fn unknown_non_idempotent_effect_cannot_retry() {
        let (mut activation, mut attempt, fence) =
            running_activation(EffectIdempotency::NonIdempotent);
        attempt
            .record_effect_evidence(fence, EffectEvidence::Started)
            .unwrap();
        activation
            .record_effect_evidence(fence, EffectEvidence::Started)
            .unwrap();
        let evidence = attempt.abandon_after_lease_expiry(fence).unwrap();
        assert_eq!(evidence, EffectEvidence::Unknown);

        assert_eq!(
            activation
                .enter_retry_wait(fence, evidence)
                .unwrap_err()
                .code(),
            EFFECT_RETRY_UNSAFE
        );
        assert_eq!(
            activation.lease_expired(fence, evidence).unwrap(),
            LeaseExpiryDisposition::RetryBlocked
        );
        assert_eq!(activation.lifecycle(), ActivationLifecycle::Terminating);
        assert_eq!(
            activation.termination_reason(),
            Some(ActivationTerminationReason::EffectOutcomeUnknown)
        );
        assert_eq!(
            activation.retry_due().unwrap_err().code(),
            STATE_INVALID_TRANSITION
        );
        assert_eq!(
            activation.finish_termination().unwrap(),
            ActivationLifecycle::Failed
        );
    }

    #[test]
    fn non_idempotent_effect_that_never_started_can_retry() {
        let (mut activation, mut attempt, fence) =
            running_activation(EffectIdempotency::NonIdempotent);
        let evidence = attempt.abandon_after_lease_expiry(fence).unwrap();
        assert_eq!(evidence, EffectEvidence::NotStarted);
        assert_eq!(
            activation.lease_expired(fence, evidence).unwrap(),
            LeaseExpiryDisposition::RetryWait
        );
        activation.retry_due().unwrap();
        assert_eq!(activation.lifecycle(), ActivationLifecycle::Ready);
    }

    #[test]
    fn accepted_completion_is_idempotent_but_a_different_fence_is_rejected() {
        let (mut activation, mut attempt, fence) =
            running_activation(EffectIdempotency::Idempotent);
        attempt.complete_success(fence).unwrap();
        activation.complete_success(fence).unwrap();
        assert_eq!(
            activation.classify_completion(fence),
            CompletionDisposition::AlreadyApplied
        );
        let stale = LeaseFence::new(fence.attempt_no(), epoch(2)).unwrap();
        assert_eq!(
            activation.classify_completion(stale),
            CompletionDisposition::Fenced
        );
    }

    #[test]
    fn effect_evidence_is_monotonic_and_lease_loss_makes_started_unknown() {
        assert!(EffectEvidence::NotStarted.can_transition_to(EffectEvidence::Started));
        assert!(EffectEvidence::Started.can_transition_to(EffectEvidence::Committed));
        assert!(!EffectEvidence::Committed.can_transition_to(EffectEvidence::Started));
        assert_eq!(
            EffectEvidence::Started.after_lease_loss(),
            EffectEvidence::Unknown
        );
        assert!(!EffectEvidence::Unknown.permits_automatic_retry(EffectIdempotency::NonIdempotent));
        assert!(EffectEvidence::Unknown.permits_automatic_retry(EffectIdempotency::Idempotent));
    }
}
