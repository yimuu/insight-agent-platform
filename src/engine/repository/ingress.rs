use async_trait::async_trait;
use serde_json::Value;
use sqlx::Row;

use crate::engine::{plan::PlanType, ActivationId, RunId, SignalId, TimerId};

use super::{
    ActivationDurableRepository, PostgresDurableRepository, RepositoryError,
    SqliteDurableRepository,
};

const MAX_INGRESS_BATCH: u32 = 1_024;

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
}

fn validate_signal_name(value: &str) -> Result<(), RepositoryError> {
    if value.is_empty()
        || value.len() > 256
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

fn validate_limit(limit: u32) -> Result<i64, RepositoryError> {
    if limit == 0 || limit > MAX_INGRESS_BATCH {
        return Err(RepositoryError::invalid_data());
    }
    Ok(i64::from(limit))
}

fn parse_target(
    run_id: String,
    activation_id: String,
    signal_id: String,
    payload_type: Value,
    projection_version: i64,
) -> Result<SignalWaitTarget, RepositoryError> {
    Ok(SignalWaitTarget::new(
        RunId::new(run_id).map_err(|_| RepositoryError::invalid_data())?,
        ActivationId::new(activation_id).map_err(|_| RepositoryError::invalid_data())?,
        SignalId::new(signal_id).map_err(|_| RepositoryError::invalid_data())?,
        serde_json::from_value(payload_type).map_err(|_| RepositoryError::invalid_data())?,
        u64::try_from(projection_version).map_err(|_| RepositoryError::invalid_data())?,
    ))
}

#[async_trait]
impl RuntimeIngressDurableRepository for SqliteDurableRepository {
    async fn find_existing_signal_submission(
        &self,
        run_id: &RunId,
        message_id: &str,
    ) -> Result<Option<ExistingSignalSubmission>, RepositoryError> {
        validate_signal_name(message_id)?;
        let row = sqlx::query(
            "SELECT s.run_id,s.target_activation_id AS activation_id,s.signal_id,
                    s.signal_state,w.payload_type,a.projection_version
             FROM signals_inbox s
             JOIN scheduler_wait_registrations w ON w.run_id=s.run_id
                AND w.activation_id=s.target_activation_id AND w.signal_id=s.signal_id
             JOIN node_activations a ON a.run_id=s.run_id
                AND a.activation_id=s.target_activation_id
             WHERE s.run_id=? AND s.message_id=?",
        )
        .bind(run_id.as_str())
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| {
            let payload_type = row
                .try_get::<String, _>("payload_type")
                .map_err(|_| RepositoryError::invalid_data())?;
            Ok(ExistingSignalSubmission::new(
                parse_target(
                    row.try_get("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("activation_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    serde_json::from_str(&payload_type)
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("projection_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                SignalInboxState::parse(
                    &row.try_get::<String, _>("signal_state")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
            ))
        })
        .transpose()
    }

    async fn reconcile_wait_late_audits(&self, limit: u32) -> Result<u64, RepositoryError> {
        super::sqlite_activation::reconcile_wait_late_audits(self, validate_limit(limit)?).await
    }

    async fn find_open_signal_waits(
        &self,
        run_id: &RunId,
        signal_name: &str,
    ) -> Result<Vec<SignalWaitTarget>, RepositoryError> {
        validate_signal_name(signal_name)?;
        let rows = sqlx::query(
            "SELECT w.run_id,w.activation_id,w.signal_id,w.payload_type,
                    a.projection_version
             FROM scheduler_wait_registrations w
             JOIN node_activations a ON a.run_id=w.run_id
                AND a.activation_id=w.activation_id
             JOIN workflow_runs r ON r.run_id=w.run_id
             LEFT JOIN human_work_items h ON h.run_id=w.run_id AND h.wait_id=w.wait_id
             WHERE w.run_id=? AND w.signal_name=? AND w.winner_kind IS NULL
               AND h.work_item_id IS NULL
               AND w.signal_id IS NOT NULL AND w.payload_type IS NOT NULL
               AND a.lifecycle='waiting' AND a.execution_kind='durable_wait'
               AND r.lifecycle IN ('active','waiting')
               AND r.admission_state IN ('open','paused')
             ORDER BY w.activation_id",
        )
        .bind(run_id.as_str())
        .bind(signal_name)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;

        rows.into_iter()
            .map(|row| {
                let payload_type = row
                    .try_get::<String, _>("payload_type")
                    .map_err(|_| RepositoryError::invalid_data())?;
                parse_target(
                    row.try_get("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("activation_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    serde_json::from_str(&payload_type)
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("projection_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
            })
            .collect()
    }

    async fn list_pending_signal_resolutions(
        &self,
        limit: u32,
    ) -> Result<Vec<PendingSignalResolution>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT s.run_id,s.target_activation_id AS activation_id,s.signal_id,
                    w.payload_type,a.projection_version
             FROM signals_inbox s
             JOIN scheduler_wait_registrations w ON w.run_id=s.run_id
                AND w.activation_id=s.target_activation_id AND w.signal_id=s.signal_id
             JOIN node_activations a ON a.run_id=s.run_id
                AND a.activation_id=s.target_activation_id
             JOIN workflow_runs r ON r.run_id=s.run_id
             WHERE s.signal_state='pending' AND w.winner_kind IS NULL
               AND w.payload_type IS NOT NULL AND a.lifecycle='waiting'
               AND a.execution_kind='durable_wait'
               AND r.lifecycle IN ('active','waiting')
             ORDER BY s.received_at,s.run_id,s.signal_id
             LIMIT ?",
        )
        .bind(validate_limit(limit)?)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;

        rows.into_iter()
            .map(|row| {
                let payload_type = row
                    .try_get::<String, _>("payload_type")
                    .map_err(|_| RepositoryError::invalid_data())?;
                Ok(PendingSignalResolution::new(parse_target(
                    row.try_get("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("activation_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    serde_json::from_str(&payload_type)
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("projection_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?))
            })
            .collect()
    }

    async fn list_due_runtime_timers(&self, limit: u32) -> Result<Vec<DueTimer>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT t.run_id,t.timer_id
             FROM timers t JOIN workflow_runs r ON r.run_id=t.run_id
             WHERE t.timer_state='scheduled'
               AND t.timer_kind IN ('wait','retry','activation_timeout')
               AND julianday(t.deadline_at) <= julianday('now')
               AND r.lifecycle IN ('active','waiting','terminating')
             ORDER BY t.deadline_at,t.run_id,t.timer_id
             LIMIT ?",
        )
        .bind(validate_limit(limit)?)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;

        rows.into_iter()
            .map(|row| {
                Ok(DueTimer::new(
                    RunId::new(
                        row.try_get::<String, _>("run_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                    TimerId::new(
                        row.try_get::<String, _>("timer_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                ))
            })
            .collect()
    }

    async fn list_due_run_deadlines(&self, limit: u32) -> Result<Vec<RunId>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT run_id FROM workflow_runs
             WHERE deadline_at IS NOT NULL
               AND julianday(deadline_at) <= julianday('now')
               AND lifecycle IN ('created','active','waiting','completing')
               AND termination_intent_reason IS NULL
             ORDER BY deadline_at,run_id
             LIMIT ?",
        )
        .bind(validate_limit(limit)?)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.into_iter()
            .map(|row| {
                RunId::new(
                    row.try_get::<String, _>("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())
            })
            .collect()
    }
}

#[async_trait]
impl RuntimeIngressDurableRepository for PostgresDurableRepository {
    async fn find_existing_signal_submission(
        &self,
        run_id: &RunId,
        message_id: &str,
    ) -> Result<Option<ExistingSignalSubmission>, RepositoryError> {
        validate_signal_name(message_id)?;
        let row = sqlx::query(
            "SELECT s.run_id,s.target_activation_id AS activation_id,s.signal_id,
                    s.signal_state,w.payload_type,a.projection_version
             FROM signals_inbox s
             JOIN scheduler_wait_registrations w ON w.run_id=s.run_id
                AND w.activation_id=s.target_activation_id AND w.signal_id=s.signal_id
             JOIN node_activations a ON a.run_id=s.run_id
                AND a.activation_id=s.target_activation_id
             WHERE s.run_id=$1 AND s.message_id=$2",
        )
        .bind(run_id.as_str())
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| {
            Ok(ExistingSignalSubmission::new(
                parse_target(
                    row.try_get("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("activation_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("payload_type")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("projection_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                SignalInboxState::parse(
                    &row.try_get::<String, _>("signal_state")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
            ))
        })
        .transpose()
    }

    async fn reconcile_wait_late_audits(&self, limit: u32) -> Result<u64, RepositoryError> {
        super::postgres_activation::reconcile_wait_late_audits(self, validate_limit(limit)?).await
    }

    async fn find_open_signal_waits(
        &self,
        run_id: &RunId,
        signal_name: &str,
    ) -> Result<Vec<SignalWaitTarget>, RepositoryError> {
        validate_signal_name(signal_name)?;
        let rows = sqlx::query(
            "SELECT w.run_id,w.activation_id,w.signal_id,w.payload_type,
                    a.projection_version
             FROM scheduler_wait_registrations w
             JOIN node_activations a ON a.run_id=w.run_id
                AND a.activation_id=w.activation_id
             JOIN workflow_runs r ON r.run_id=w.run_id
             LEFT JOIN human_work_items h ON h.run_id=w.run_id AND h.wait_id=w.wait_id
             WHERE w.run_id=$1 AND w.signal_name=$2 AND w.winner_kind IS NULL
               AND h.work_item_id IS NULL
               AND w.signal_id IS NOT NULL AND w.payload_type IS NOT NULL
               AND a.lifecycle='waiting' AND a.execution_kind='durable_wait'
               AND r.lifecycle IN ('active','waiting')
               AND r.admission_state IN ('open','paused')
             ORDER BY w.activation_id",
        )
        .bind(run_id.as_str())
        .bind(signal_name)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;

        rows.into_iter()
            .map(|row| {
                parse_target(
                    row.try_get("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("activation_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("payload_type")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("projection_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
            })
            .collect()
    }

    async fn list_pending_signal_resolutions(
        &self,
        limit: u32,
    ) -> Result<Vec<PendingSignalResolution>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT s.run_id,s.target_activation_id AS activation_id,s.signal_id,
                    w.payload_type,a.projection_version
             FROM signals_inbox s
             JOIN scheduler_wait_registrations w ON w.run_id=s.run_id
                AND w.activation_id=s.target_activation_id AND w.signal_id=s.signal_id
             JOIN node_activations a ON a.run_id=s.run_id
                AND a.activation_id=s.target_activation_id
             JOIN workflow_runs r ON r.run_id=s.run_id
             WHERE s.signal_state='pending' AND w.winner_kind IS NULL
               AND w.payload_type IS NOT NULL AND a.lifecycle='waiting'
               AND a.execution_kind='durable_wait'
               AND r.lifecycle IN ('active','waiting')
             ORDER BY s.received_at,s.run_id,s.signal_id
             LIMIT $1",
        )
        .bind(validate_limit(limit)?)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;

        rows.into_iter()
            .map(|row| {
                Ok(PendingSignalResolution::new(parse_target(
                    row.try_get("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("activation_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("payload_type")
                        .map_err(|_| RepositoryError::invalid_data())?,
                    row.try_get("projection_version")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?))
            })
            .collect()
    }

    async fn list_due_runtime_timers(&self, limit: u32) -> Result<Vec<DueTimer>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT t.run_id,t.timer_id
             FROM timers t JOIN workflow_runs r ON r.run_id=t.run_id
             WHERE t.timer_state='scheduled'
               AND t.timer_kind IN ('wait','retry','activation_timeout')
               AND t.deadline_at <= clock_timestamp()
               AND r.lifecycle IN ('active','waiting','terminating')
             ORDER BY t.deadline_at,t.run_id,t.timer_id
             LIMIT $1",
        )
        .bind(validate_limit(limit)?)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;

        rows.into_iter()
            .map(|row| {
                Ok(DueTimer::new(
                    RunId::new(
                        row.try_get::<String, _>("run_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                    TimerId::new(
                        row.try_get::<String, _>("timer_id")
                            .map_err(|_| RepositoryError::invalid_data())?,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
                ))
            })
            .collect()
    }

    async fn list_due_run_deadlines(&self, limit: u32) -> Result<Vec<RunId>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT run_id FROM workflow_runs
             WHERE deadline_at IS NOT NULL
               AND deadline_at <= clock_timestamp()
               AND lifecycle IN ('created','active','waiting','completing')
               AND termination_intent_reason IS NULL
             ORDER BY deadline_at,run_id
             LIMIT $1",
        )
        .bind(validate_limit(limit)?)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.into_iter()
            .map(|row| {
                RunId::new(
                    row.try_get::<String, _>("run_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())
            })
            .collect()
    }
}
