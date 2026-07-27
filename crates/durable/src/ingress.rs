//! Backend-neutral runtime ingress repository contracts.

use super::RepositoryErrorExt as _;

use async_trait::async_trait;

use insight_engine::{plan::PlanType, ActivationId, RunId, SignalId, TimerId};

use super::{ActivationDurableRepository, RepositoryError};

/// The one durable wait that an external signal is allowed to address.
///
/// The scheduler allocates `signal_id`; callers only provide an idempotency
/// `message_id` and a typed value.  Keeping target resolution in the durable
/// repository prevents an API process from routing a signal to a stale or
/// already-settled Activation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalWaitTarget {
    run_id: RunId,
    activation_id: ActivationId,
    signal_id: SignalId,
    payload_type: PlanType,
    activation_projection_version: u64,
}

impl SignalWaitTarget {
    fn new(
        run_id: RunId,
        activation_id: ActivationId,
        signal_id: SignalId,
        payload_type: PlanType,
        activation_projection_version: u64,
    ) -> Self {
        Self {
            run_id,
            activation_id,
            signal_id,
            payload_type,
            activation_projection_version,
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

    pub fn payload_type(&self) -> &PlanType {
        &self.payload_type
    }

    pub fn activation_projection_version(&self) -> u64 {
        self.activation_projection_version
    }
}

/// A signal whose inbox insert committed but whose wait-resolution CAS did not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSignalResolution {
    target: SignalWaitTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalInboxState {
    Pending,
    Consumed,
    Rejected,
    Expired,
}

impl SignalInboxState {
    fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "pending" => Ok(Self::Pending),
            "consumed" => Ok(Self::Consumed),
            "rejected" => Ok(Self::Rejected),
            "expired" => Ok(Self::Expired),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

/// Existing idempotency authority for a previously submitted `message_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingSignalSubmission {
    target: SignalWaitTarget,
    state: SignalInboxState,
}

impl ExistingSignalSubmission {
    fn new(target: SignalWaitTarget, state: SignalInboxState) -> Self {
        Self { target, state }
    }

    pub fn target(&self) -> &SignalWaitTarget {
        &self.target
    }

    pub fn state(&self) -> SignalInboxState {
        self.state
    }
}

impl PendingSignalResolution {
    fn new(target: SignalWaitTarget) -> Self {
        Self { target }
    }

    pub fn target(&self) -> &SignalWaitTarget {
        &self.target
    }
}

/// A database-clock-qualified timer that a timer pump may attempt to fire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueTimer {
    run_id: RunId,
    timer_id: TimerId,
}

impl DueTimer {
    fn new(run_id: RunId, timer_id: TimerId) -> Self {
        Self { run_id, timer_id }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn timer_id(&self) -> &TimerId {
        &self.timer_id
    }
}

#[async_trait]
pub trait RuntimeIngressDurableRepository: ActivationDurableRepository {
    /// Loads the original routing authority before resolving a new public
    /// signal name.  This makes an exact `message_id` replay possible even
    /// after the wait or Run has already reached a terminal state.
    async fn find_existing_signal_submission(
        &self,
        run_id: &RunId,
        message_id: &str,
    ) -> Result<Option<ExistingSignalSubmission>, RepositoryError>;

    /// Returns every unresolved durable wait with this public name.
    ///
    /// A production caller must require exactly one row.  Returning a closed
    /// collection makes ambiguous same-name parallel waits fail explicitly
    /// instead of selecting one by storage order.
    async fn find_open_signal_waits(
        &self,
        run_id: &RunId,
        signal_name: &str,
    ) -> Result<Vec<SignalWaitTarget>, RepositoryError>;

    /// Recovers the durable gap between inbox receipt and wait resolution.
    async fn list_pending_signal_resolutions(
        &self,
        limit: u32,
    ) -> Result<Vec<PendingSignalResolution>, RepositoryError>;

    /// Materializes audit events for durable first-winner losers that no
    /// longer have an in-memory ingress request after a crash. Direct loser
    /// delivery and this reconciler share one stable event identity.
    async fn reconcile_wait_late_audits(&self, limit: u32) -> Result<u64, RepositoryError>;

    /// Returns non-terminal Runs whose persisted root deadline has elapsed
    /// according to the database clock. Paused admission deliberately does
    /// not suspend a root deadline.
    async fn list_due_run_deadlines(&self, limit: u32) -> Result<Vec<RunId>, RepositoryError>;

    /// Uses the database clock to qualify due non-lease timers.
    ///
    /// Lease expiry additionally needs the frozen retry policy and is owned by
    /// the worker lease-reaper, so it is intentionally excluded here.
    async fn list_due_runtime_timers(&self, limit: u32) -> Result<Vec<DueTimer>, RepositoryError>;

    /// Returns the database-clock delay until the next timer or root deadline.
    /// `None` means no ingress deadline exists. This is a scheduling hint only;
    /// due queries remain the authoritative eligibility check.
    async fn next_runtime_ingress_delay(
        &self,
    ) -> Result<Option<std::time::Duration>, RepositoryError>;
}

/// Workspace-internal construction surface for storage adapters.
#[doc(hidden)]
pub mod adapter {
    use insight_engine::{plan::PlanType, ActivationId, RunId, SignalId, TimerId};

    use super::{
        DueTimer, ExistingSignalSubmission, PendingSignalResolution, RepositoryError,
        SignalInboxState, SignalWaitTarget,
    };

    pub fn signal_wait_target(
        run_id: RunId,
        activation_id: ActivationId,
        signal_id: SignalId,
        payload_type: PlanType,
        activation_projection_version: u64,
    ) -> SignalWaitTarget {
        SignalWaitTarget::new(
            run_id,
            activation_id,
            signal_id,
            payload_type,
            activation_projection_version,
        )
    }

    pub fn signal_inbox_state(value: &str) -> Result<SignalInboxState, RepositoryError> {
        SignalInboxState::parse(value)
    }

    pub fn existing_signal_submission(
        target: SignalWaitTarget,
        state: SignalInboxState,
    ) -> ExistingSignalSubmission {
        ExistingSignalSubmission::new(target, state)
    }

    pub fn pending_signal_resolution(target: SignalWaitTarget) -> PendingSignalResolution {
        PendingSignalResolution::new(target)
    }

    pub fn due_timer(run_id: RunId, timer_id: TimerId) -> DueTimer {
        DueTimer::new(run_id, timer_id)
    }
}
