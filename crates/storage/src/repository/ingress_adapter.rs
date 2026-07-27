use super::RepositoryErrorExt as _;

use async_trait::async_trait;
use insight_durable::ingress::adapter as ingress_contract_adapter;
use insight_durable::ingress::{
    DueTimer, ExistingSignalSubmission, PendingSignalResolution, RuntimeIngressDurableRepository,
    SignalWaitTarget,
};
use insight_engine::repository::RepositoryError;
use insight_engine::{ActivationId, RunId, SignalId, TimerId};
use serde_json::Value;
use sqlx::Row;

use super::{PostgresDurableRepository, SqliteDurableRepository};

const MAX_INGRESS_BATCH: u32 = 1_024;

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
    Ok(ingress_contract_adapter::signal_wait_target(
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
            Ok(ingress_contract_adapter::existing_signal_submission(
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
                ingress_contract_adapter::signal_inbox_state(
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
                Ok(ingress_contract_adapter::pending_signal_resolution(
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
                ))
            })
            .collect()
    }

    async fn list_due_runtime_timers(&self, limit: u32) -> Result<Vec<DueTimer>, RepositoryError> {
        let database_now =
            sqlx::query_scalar::<_, String>("SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                .fetch_one(&self.pool)
                .await
                .map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "SELECT t.run_id,t.timer_id
             FROM timers t JOIN workflow_runs r ON r.run_id=t.run_id
             WHERE t.timer_state='scheduled'
               AND t.timer_kind IN ('wait','retry','activation_timeout')
               AND t.deadline_at <= ?
               AND r.lifecycle IN ('active','waiting','terminating')
             ORDER BY t.deadline_at,t.run_id,t.timer_id
             LIMIT ?",
        )
        .bind(&database_now)
        .bind(validate_limit(limit)?)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;

        rows.into_iter()
            .map(|row| {
                Ok(ingress_contract_adapter::due_timer(
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
        let database_now =
            sqlx::query_scalar::<_, String>("SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')")
                .fetch_one(&self.pool)
                .await
                .map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "SELECT run_id FROM workflow_runs
             WHERE deadline_at IS NOT NULL
               AND deadline_at <= ?
               AND lifecycle IN ('created','active','waiting','completing')
               AND termination_intent_reason IS NULL
             ORDER BY deadline_at,run_id
             LIMIT ?",
        )
        .bind(&database_now)
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

    async fn next_runtime_ingress_delay(
        &self,
    ) -> Result<Option<std::time::Duration>, RepositoryError> {
        let row = sqlx::query(
            "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now') AS database_now,
                    MIN(due_at) AS due_at
             FROM (
                 SELECT MIN(deadline_at) AS due_at
                 FROM timers
                 WHERE timer_state='scheduled'
                   AND timer_kind IN ('wait','retry','activation_timeout')
                 UNION ALL
                 SELECT MIN(deadline_at) AS due_at
                 FROM workflow_runs
                 WHERE deadline_at IS NOT NULL
                   AND lifecycle IN ('created','active','waiting','completing')
                   AND termination_intent_reason IS NULL
                 UNION ALL
                 SELECT MIN(due_at) AS due_at
                 FROM wait_late_audit_outbox
                 WHERE audit_state='pending'
                 UNION ALL
                 SELECT MIN(claim_expires_at) AS due_at
                 FROM wait_late_audit_outbox
                 WHERE audit_state='claimed'
             )",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let database_now = row
            .try_get::<String, _>("database_now")
            .map_err(|_| RepositoryError::invalid_data())?;
        let Some(due_at) = row
            .try_get::<Option<String>, _>("due_at")
            .map_err(|_| RepositoryError::invalid_data())?
        else {
            return Ok(None);
        };
        let database_now = chrono::DateTime::parse_from_rfc3339(&database_now)
            .map_err(|_| RepositoryError::invalid_data())?;
        let due_at = chrono::DateTime::parse_from_rfc3339(&due_at)
            .map_err(|_| RepositoryError::invalid_data())?;
        Ok(Some(
            due_at
                .signed_duration_since(database_now)
                .to_std()
                .unwrap_or(std::time::Duration::ZERO),
        ))
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
            Ok(ingress_contract_adapter::existing_signal_submission(
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
                ingress_contract_adapter::signal_inbox_state(
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
                Ok(ingress_contract_adapter::pending_signal_resolution(
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
                ))
            })
            .collect()
    }

    async fn list_due_runtime_timers(&self, limit: u32) -> Result<Vec<DueTimer>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT t.run_id,t.timer_id
             FROM timers t JOIN workflow_runs r ON r.run_id=t.run_id
             WHERE t.timer_state='scheduled'
               AND t.timer_kind IN ('wait','retry','activation_timeout')
               AND t.deadline_at <= statement_timestamp()
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
                Ok(ingress_contract_adapter::due_timer(
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
               AND deadline_at <= statement_timestamp()
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

    async fn next_runtime_ingress_delay(
        &self,
    ) -> Result<Option<std::time::Duration>, RepositoryError> {
        let delay_ms = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT CASE
                        WHEN MIN(due_at) IS NULL THEN NULL
                        ELSE GREATEST(
                            0,
                            FLOOR(
                                EXTRACT(EPOCH FROM (MIN(due_at)-statement_timestamp())) * 1000
                            )
                        )::bigint
                    END
             FROM (
                 SELECT MIN(deadline_at) AS due_at
                 FROM timers
                 WHERE timer_state='scheduled'
                   AND timer_kind IN ('wait','retry','activation_timeout')
                 UNION ALL
                 SELECT MIN(deadline_at) AS due_at
                 FROM workflow_runs
                 WHERE deadline_at IS NOT NULL
                   AND lifecycle IN ('created','active','waiting','completing')
                   AND termination_intent_reason IS NULL
                 UNION ALL
                 SELECT MIN(due_at) AS due_at
                 FROM wait_late_audit_outbox
                 WHERE audit_state='pending'
                 UNION ALL
                 SELECT MIN(claim_expires_at) AS due_at
                 FROM wait_late_audit_outbox
                 WHERE audit_state='claimed'
             ) due",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        delay_ms
            .map(|delay_ms| {
                u64::try_from(delay_ms)
                    .map(std::time::Duration::from_millis)
                    .map_err(|_| RepositoryError::invalid_data())
            })
            .transpose()
    }
}
