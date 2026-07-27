use super::RepositoryErrorExt as _;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_durable::common::adapter::{
    decode_public_projection_decision, i64_from_u64, public_event_ordinal, u64_from_i64,
    StoredPublicProjectionDecision,
};
#[cfg(test)]
use insight_durable::common::adapter::{durable_public_event_envelope, event_id, public_event_id};
use insight_durable::public_outbox::adapter as public_outbox_contract_adapter;
use insight_durable::public_outbox::{
    OrderedPublicEventRead, PublicEventClaim, PublicEventNotificationStream,
    PublicEventOutboxRepository, PublicEventPosition, PublishedPublicEvent,
    PUBLIC_EVENT_NOTIFY_CHANNEL,
};
use insight_engine::repository::RepositoryError;
use insight_engine::{
    ExecutionEventEnvelope, ExecutionEventPayload, InternalFailureKind, PublicEventEnvelope,
    PublicEventKind, PublicEventPayload, PublicFailureKind, RunId, RunLifecycle,
};
use serde_json::Value;
use sqlx::{
    postgres::{PgListener, PgRow},
    sqlite::SqliteRow,
    Row,
};
use uuid::Uuid;

use super::postgres::begin_write_transaction;
use super::{PostgresDurableRepository, SqliteDurableRepository};

const MAX_CLAIM_SECONDS: u32 = 86_400;
const MAX_CLAIM_BATCH: u32 = 1_000;
pub(crate) const MAX_NONTERMINAL_RETENTION_SECONDS: u32 = 10 * 365 * 24 * 60 * 60;
const MAX_PRUNE_BATCH: u32 = 1_000;
const POSTGRES_PUBLIC_EVENT_RETENTION_PRUNE_SQL: &str = r#"
WITH due AS (
    SELECT run_id, public_event_id
    FROM public_event_outbox
    WHERE publish_state = 'published' AND NOT is_terminal
      AND retain_until IS NOT NULL
      AND retain_until <= statement_timestamp()
    ORDER BY retain_until, run_id, public_event_id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
DELETE FROM public_event_outbox AS outbox
USING due
WHERE outbox.run_id = due.run_id
  AND outbox.public_event_id = due.public_event_id
"#;

macro_rules! complete_public_query {
    ($tail:literal) => {
        sqlx::query(concat!(
            "SELECT
                decision.run_id AS decision_run_id,
                decision.execution_event_id,decision.execution_seq,
                decision.execution_occurred_at,decision.execution_transition_key,
                decision.decision AS projection_decision,
                decision.public_event_id AS decision_public_event_id,
                decision.public_ordinal AS decision_public_ordinal,
                decision.public_schema_version AS decision_public_schema_version,
                decision.event_kind AS decision_event_kind,
                decision.is_terminal AS decision_is_terminal,
                receipt.public_event_id AS receipt_public_event_id,
                receipt.causation_event_id AS receipt_causation_event_id,
                receipt.public_ordinal AS receipt_public_ordinal,
                receipt.public_schema_version AS receipt_public_schema_version,
                receipt.event_kind AS receipt_event_kind,
                receipt.is_terminal AS receipt_is_terminal,
                (SELECT COUNT(*) FROM public_event_receipts receipt_count
                 WHERE receipt_count.run_id=decision.run_id
                   AND receipt_count.causation_event_id=decision.execution_event_id)
                    AS receipt_causation_count,
                outbox.run_id AS public_run_id,
                outbox.public_event_id AS outbox_public_event_id,
                outbox.causation_event_id AS public_causation_event_id,
                outbox.public_ordinal AS outbox_public_ordinal,
                outbox.public_schema_version AS outbox_public_schema_version,
                outbox.event_kind AS outbox_event_kind,
                outbox.is_terminal AS outbox_is_terminal,
                outbox.publish_state,outbox.safe_envelope,
                event.schema_version,event.seq,event.event_id,event.run_id,
                event.transition_key,event.intent_hash,event.projection_version_after,
                event.kind,event.node_id,event.scope_instance_id,event.activation_id,
                event.attempt_no,event.causation_event_id,event.safe_payload,event.occurred_at ",
            $tail
        ))
    };
}

struct PostgresPublicEventNotificationStream {
    listener: PgListener,
}

#[async_trait]
impl PublicEventNotificationStream for PostgresPublicEventNotificationStream {
    async fn recv(&mut self) -> Result<String, RepositoryError> {
        self.listener
            .try_recv()
            .await
            .map_err(RepositoryError::storage)
            .and_then(|notification| {
                notification
                    .map(|notification| notification.payload().to_owned())
                    .ok_or_else(RepositoryError::storage_unavailable)
            })
    }
}

#[async_trait]
impl PublicEventOutboxRepository for PostgresDurableRepository {
    async fn claim_public_events(
        &self,
        claimant: &str,
        claim_seconds: u32,
        limit: u32,
    ) -> Result<Vec<PublicEventClaim>, RepositoryError> {
        validate_claim_request(claimant, claim_seconds, limit)?;
        let mut transaction = begin_write_transaction(&self.pool).await?;
        let rows = complete_public_query!(
            "FROM (
                 SELECT candidate.run_id,candidate.public_event_id,candidate.due_at,
                        candidate.execution_seq,candidate.public_ordinal
                 FROM public_event_delivery_heads candidate
                 WHERE candidate.head_state='ready'
                   AND candidate.due_at<=CURRENT_TIMESTAMP
                 ORDER BY candidate.due_at,candidate.run_id,
                          candidate.execution_seq,candidate.public_ordinal,
                          candidate.public_event_id
                 LIMIT $1
                 FOR UPDATE SKIP LOCKED
             ) head
             JOIN public_event_outbox outbox
               ON outbox.run_id=head.run_id AND outbox.public_event_id=head.public_event_id
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=outbox.run_id
              AND decision.execution_event_id=outbox.causation_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=outbox.run_id
              AND receipt.causation_event_id=outbox.causation_event_id
             LEFT JOIN execution_events event
               ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
             ORDER BY head.due_at,head.run_id,head.execution_seq,
                      head.public_ordinal,head.public_event_id
             FOR UPDATE OF outbox SKIP LOCKED"
        )
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;

        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let complete = decode_postgres_complete_public_row(&row, None, None)?;
            if !matches!(
                complete.publish_state.as_deref(),
                Some("pending" | "claimed")
            ) {
                return Err(RepositoryError::invalid_data());
            }
            let run_text = complete.run_id.as_str().to_owned();
            let public_event_id = complete.public_event_id;
            let token = claim_token();
            let claim_expires_at = sqlx::query_scalar::<_, DateTime<Utc>>(
                "UPDATE public_event_outbox
                 SET publish_state = 'claimed', claimed_by = $1, claim_token = $2,
                     claim_expires_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 second'),
                     publish_attempts = publish_attempts + 1
                 WHERE run_id = $4 AND public_event_id = $5
                   AND ((publish_state = 'pending' AND available_at <= CURRENT_TIMESTAMP)
                        OR (publish_state = 'claimed' AND claim_expires_at <= CURRENT_TIMESTAMP))
                 RETURNING claim_expires_at",
            )
            .bind(claimant)
            .bind(&token)
            .bind(i64::from(claim_seconds))
            .bind(&run_text)
            .bind(&public_event_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            let Some(claim_expires_at) = claim_expires_at else {
                continue;
            };
            let claim = public_outbox_contract_adapter::public_event_claim(
                model_run_id(run_text)?,
                public_event_id,
                row.try_get::<Option<String>, _>("public_causation_event_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .ok_or_else(RepositoryError::invalid_data)?,
                row.try_get::<Option<String>, _>("outbox_event_kind")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .ok_or_else(RepositoryError::invalid_data)?,
                row.try_get::<Option<bool>, _>("outbox_is_terminal")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .ok_or_else(RepositoryError::invalid_data)?,
                claimant.to_owned(),
                token,
                claim_expires_at,
                serde_json::to_value(
                    complete
                        .safe_envelope
                        .ok_or_else(RepositoryError::invalid_data)?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            claims.push(claim);
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn publish_public_event(
        &self,
        claim: &PublicEventClaim,
        nonterminal_retention_seconds: u32,
    ) -> Result<bool, RepositoryError> {
        validate_retention_seconds(nonterminal_retention_seconds)?;
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if claim.is_terminal() {
            // A terminal transition owns workflow_runs until commit, where its
            // deferred terminal-public-event FK takes KEY SHARE on this outbox
            // row. The publish trigger also reaches workflow_runs when it
            // drains the last delivery head. Take the Run lock first so these
            // two paths share workflow_runs -> head -> outbox lock ordering
            // instead of deadlocking at the terminal transition's COMMIT.
            sqlx::query("SELECT 1 FROM workflow_runs WHERE run_id=$1 FOR UPDATE")
                .bind(claim.run_id().as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
                .ok_or_else(RepositoryError::invalid_data)?;
        }
        // Public claim/reclaim takes the durable head before the outbox row.
        // Publishing must use the same lock order to avoid a head<->outbox
        // deadlock at lease expiry. The explicit READ COMMITTED transaction
        // also makes the trigger's drain-boundary recheck statement-fresh.
        let locked_head = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
            "SELECT head_state,public_event_id,delivery_state
             FROM public_event_delivery_heads
             WHERE run_id=$1
             FOR UPDATE",
        )
        .bind(claim.run_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::invalid_data)?;
        let authority = complete_public_query!(
            "FROM public_event_outbox outbox
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=outbox.run_id
              AND decision.execution_event_id=outbox.causation_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=outbox.run_id
              AND receipt.causation_event_id=outbox.causation_event_id
             LEFT JOIN execution_events event
               ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
             WHERE outbox.run_id=$1 AND outbox.public_event_id=$2"
        )
        .bind(claim.run_id().as_str())
        .bind(claim.public_event_id())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::invalid_data)?;
        let decoded = decode_postgres_complete_public_row(
            &authority,
            Some(claim.run_id()),
            Some(claim.public_event_id()),
        )?;
        if decoded.terminal != claim.is_terminal()
            || decoded.safe_envelope.as_ref() != Some(claim.safe_envelope())
        {
            return Err(RepositoryError::invalid_data());
        }
        if decoded.publish_state.as_deref() == Some("claimed")
            && (locked_head.0 != "ready"
                || locked_head.1.as_deref() != Some(claim.public_event_id())
                || locked_head.2.as_deref() != Some("claimed"))
        {
            return Err(RepositoryError::invalid_data());
        }
        let published = sqlx::query_scalar::<_, String>(
            "UPDATE public_event_outbox
             SET publish_state = 'published', published_at = clock_timestamp(),
                 published_by = $3, published_claim_token = $4,
                 notified_at = clock_timestamp(), claimed_by = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 retain_until = CASE WHEN is_terminal THEN NULL
                     ELSE clock_timestamp() + ($5 * INTERVAL '1 second') END
             WHERE run_id = $1 AND public_event_id = $2 AND publish_state = 'claimed'
               AND claimed_by = $3 AND claim_token = $4
               AND claim_expires_at > clock_timestamp()
             RETURNING public_event_id",
        )
        .bind(claim.run_id().as_str())
        .bind(claim.public_event_id())
        .bind(claim.claimant())
        .bind(claim.claim_token())
        .bind(i64::from(nonterminal_retention_seconds))
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;

        if let Some(public_event_id) = published {
            if !self.defer_transactional_notifications {
                // Non-runtime repository writers keep the commit-scoped
                // cross-process hint. The runtime delivers locally after this
                // commit and its subscribers independently poll durable order,
                // so putting five NOTIFY calls per short Run in authoritative
                // transactions only adds notification-ordering lock pressure.
                sqlx::query("SELECT pg_notify($1, $2)")
                    .bind(PUBLIC_EVENT_NOTIFY_CHANNEL)
                    .bind(public_event_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(RepositoryError::storage)?;
            }
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(true);
        }

        let exact_replay = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM public_event_outbox
                WHERE run_id = $1 AND public_event_id = $2 AND publish_state = 'published'
                  AND published_by = $3 AND published_claim_token = $4
             )",
        )
        .bind(claim.run_id().as_str())
        .bind(claim.public_event_id())
        .bind(claim.claimant())
        .bind(claim.claim_token())
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        transaction
            .rollback()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(exact_replay)
    }

    async fn prune_expired_public_events(&self, limit: u32) -> Result<u64, RepositoryError> {
        validate_prune_limit(limit)?;
        sqlx::query(POSTGRES_PUBLIC_EVENT_RETENTION_PRUNE_SQL)
            .bind(i64::from(limit))
            .execute(&self.pool)
            .await
            .map(|result| result.rows_affected())
            .map_err(RepositoryError::storage)
    }

    async fn load_terminal_public_event(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PublicEventEnvelope>, RepositoryError> {
        let row = complete_public_query!(
            "FROM public_event_outbox outbox
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=outbox.run_id
              AND decision.execution_event_id=outbox.causation_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=outbox.run_id
              AND receipt.causation_event_id=outbox.causation_event_id
             LEFT JOIN execution_events event
               ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
             WHERE outbox.run_id=$1 AND outbox.is_terminal
               AND outbox.publish_state='published'
             ORDER BY outbox.created_at,outbox.public_event_id LIMIT 1"
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| decode_postgres_public_event_row(&row, Some(run_id), None).map(|row| row.3))
            .transpose()
    }

    async fn load_terminal_public_event_authority(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PublicEventEnvelope>, RepositoryError> {
        let row = complete_public_query!(
            "FROM workflow_runs run
             LEFT JOIN public_event_outbox outbox
               ON outbox.run_id=run.run_id
              AND outbox.public_event_id=run.terminal_public_event_id
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=run.run_id
              AND decision.public_event_id=run.terminal_public_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=run.run_id
              AND receipt.public_event_id=run.terminal_public_event_id
             LEFT JOIN execution_events event
               ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
             WHERE run.run_id=$1 AND run.terminal_public_event_id IS NOT NULL"
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| decode_postgres_public_event_row(&row, Some(run_id), None).map(|row| row.3))
            .transpose()
    }

    async fn load_published_public_event(
        &self,
        public_event_id: &str,
    ) -> Result<Option<PublishedPublicEvent>, RepositoryError> {
        if !valid_public_event_id_hint(public_event_id) {
            return Ok(None);
        }
        let row = complete_public_query!(
            "FROM public_event_outbox outbox
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=outbox.run_id
              AND decision.execution_event_id=outbox.causation_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=outbox.run_id
              AND receipt.causation_event_id=outbox.causation_event_id
             LEFT JOIN execution_events event
               ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
             WHERE outbox.public_event_id=$1 AND outbox.publish_state='published'"
        )
        .bind(public_event_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| {
            let (run_id, public_event_id, terminal, safe_envelope) =
                decode_postgres_public_event_row(&row, None, Some(public_event_id))?;
            let position = position_from_public_envelope(&public_event_id, &safe_envelope)?;
            Ok(public_outbox_contract_adapter::published_public_event(
                run_id,
                public_event_id,
                terminal,
                safe_envelope,
                position,
            ))
        })
        .transpose()
    }

    async fn load_next_public_event(
        &self,
        run_id: &RunId,
        after: Option<&PublicEventPosition>,
    ) -> Result<OrderedPublicEventRead, RepositoryError> {
        if after.is_some_and(|position| position.run_id() != run_id) {
            return Err(RepositoryError::invalid_data());
        }
        let after_seq = after
            .map(|position| i64_from_u64(position.causation_seq()))
            .transpose()?;
        let after_ordinal = after.map(|position| i32::from(position.public_ordinal()));
        let after_id = after.map(PublicEventPosition::public_event_id);
        let row = complete_public_query!(
            "FROM (
                 SELECT public_decision.run_id,public_decision.execution_event_id,
                        public_decision.execution_seq,public_decision.public_ordinal,
                        public_decision.public_event_id
                 FROM public_event_projection_decisions public_decision
                 WHERE public_decision.decision='public'
                 UNION ALL
                 SELECT orphan.run_id,orphan.causation_event_id,event.seq,
                        orphan.public_ordinal,orphan.public_event_id
                 FROM public_event_receipts orphan
                 LEFT JOIN public_event_projection_decisions existing
                   ON existing.run_id=orphan.run_id
                  AND existing.execution_event_id=orphan.causation_event_id
                 LEFT JOIN execution_events event
                   ON event.run_id=orphan.run_id AND event.event_id=orphan.causation_event_id
                 WHERE COALESCE(existing.decision,'')<>'public'
                 UNION ALL
                 SELECT orphan.run_id,orphan.causation_event_id,event.seq,
                        orphan.public_ordinal,orphan.public_event_id
                 FROM public_event_outbox orphan
                 LEFT JOIN public_event_projection_decisions existing
                   ON existing.run_id=orphan.run_id
                  AND existing.execution_event_id=orphan.causation_event_id
                 LEFT JOIN public_event_receipts receipt_witness
                   ON receipt_witness.run_id=orphan.run_id
                  AND receipt_witness.causation_event_id=orphan.causation_event_id
                 LEFT JOIN execution_events event
                   ON event.run_id=orphan.run_id AND event.event_id=orphan.causation_event_id
                 WHERE COALESCE(existing.decision,'')<>'public'
                   AND receipt_witness.public_event_id IS NULL
             ) directory
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=directory.run_id
              AND decision.execution_event_id=directory.execution_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=directory.run_id
              AND receipt.causation_event_id=directory.execution_event_id
             LEFT JOIN public_event_outbox outbox
               ON outbox.run_id=directory.run_id
              AND outbox.causation_event_id=directory.execution_event_id
             LEFT JOIN execution_events event
               ON event.run_id=directory.run_id
              AND event.event_id=directory.execution_event_id
             WHERE directory.run_id=$1
               AND ($2::BIGINT IS NULL
                    OR directory.execution_seq>$2
                    OR (directory.execution_seq=$2 AND directory.public_ordinal>$3)
                    OR (directory.execution_seq=$2 AND directory.public_ordinal=$3
                        AND directory.public_event_id>$4))
             ORDER BY directory.execution_seq,directory.public_ordinal,
                      directory.public_event_id
             LIMIT 1"
        )
        .bind(run_id.as_str())
        .bind(after_seq)
        .bind(after_ordinal)
        .bind(after_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| decode_postgres_ordered_public_event_row(&row, run_id))
            .unwrap_or(Ok(OrderedPublicEventRead::UpToDate))
    }

    async fn open_public_event_notification_stream(
        &self,
    ) -> Result<Option<Box<dyn PublicEventNotificationStream>>, RepositoryError> {
        let mut listener = PgListener::connect_with(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        listener.eager_reconnect(false);
        listener
            .listen(PUBLIC_EVENT_NOTIFY_CHANNEL)
            .await
            .map_err(RepositoryError::storage)?;
        Ok(Some(Box::new(PostgresPublicEventNotificationStream {
            listener,
        })))
    }
}

#[async_trait]
impl PublicEventOutboxRepository for SqliteDurableRepository {
    async fn claim_public_events(
        &self,
        claimant: &str,
        claim_seconds: u32,
        limit: u32,
    ) -> Result<Vec<PublicEventClaim>, RepositoryError> {
        validate_claim_request(claimant, claim_seconds, limit)?;
        let _writer = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let expires_encoded = sqlx::query_scalar::<_, String>(
            "SELECT STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds')",
        )
        .bind(i64::from(claim_seconds))
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let expires = DateTime::parse_from_rfc3339(&expires_encoded)
            .map_err(|_| RepositoryError::invalid_data())?
            .with_timezone(&Utc);
        let rows = complete_public_query!(
            "FROM (
                 SELECT candidate.run_id,candidate.public_event_id,candidate.due_at,
                        candidate.execution_seq,candidate.public_ordinal
                 FROM public_event_delivery_heads candidate
                 WHERE candidate.head_state='ready'
                   AND candidate.due_at<=STRFTIME('%Y-%m-%dT%H:%M:%fZ','now')
                 ORDER BY candidate.due_at,candidate.run_id,
                          candidate.execution_seq,candidate.public_ordinal,
                          candidate.public_event_id
                 LIMIT ?
             ) head
             JOIN public_event_outbox outbox
               ON outbox.run_id=head.run_id AND outbox.public_event_id=head.public_event_id
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=outbox.run_id
              AND decision.execution_event_id=outbox.causation_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=outbox.run_id
              AND receipt.causation_event_id=outbox.causation_event_id
             LEFT JOIN execution_events event
               ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
             ORDER BY head.due_at,head.run_id,head.execution_seq,
                      head.public_ordinal,head.public_event_id"
        )
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;

        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let complete = decode_sqlite_complete_public_row(&row, None, None)?;
            if !matches!(
                complete.publish_state.as_deref(),
                Some("pending" | "claimed")
            ) {
                return Err(RepositoryError::invalid_data());
            }
            let run_text = complete.run_id.as_str().to_owned();
            let public_event_id = complete.public_event_id;
            let token = claim_token();
            let updated = sqlx::query(
                "UPDATE public_event_outbox
                 SET publish_state = 'claimed', claimed_by = ?, claim_token = ?,
                     claim_expires_at = ?, publish_attempts = publish_attempts + 1
                 WHERE run_id = ? AND public_event_id = ?
                   AND ((publish_state = 'pending'
                         AND JULIANDAY(available_at) <= JULIANDAY('now'))
                        OR (publish_state = 'claimed'
                         AND JULIANDAY(claim_expires_at) <= JULIANDAY('now')))",
            )
            .bind(claimant)
            .bind(&token)
            .bind(&expires_encoded)
            .bind(&run_text)
            .bind(&public_event_id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if updated != 1 {
                continue;
            }
            let claim = public_outbox_contract_adapter::public_event_claim(
                model_run_id(run_text)?,
                public_event_id,
                row.try_get::<Option<String>, _>("public_causation_event_id")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .ok_or_else(RepositoryError::invalid_data)?,
                row.try_get::<Option<String>, _>("outbox_event_kind")
                    .map_err(|_| RepositoryError::invalid_data())?
                    .ok_or_else(RepositoryError::invalid_data)?,
                match row
                    .try_get::<Option<i64>, _>("outbox_is_terminal")
                    .map_err(|_| RepositoryError::invalid_data())?
                {
                    Some(0) => false,
                    Some(1) => true,
                    _ => return Err(RepositoryError::invalid_data()),
                },
                claimant.to_owned(),
                token,
                expires,
                serde_json::to_value(
                    complete
                        .safe_envelope
                        .ok_or_else(RepositoryError::invalid_data)?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            claims.push(claim);
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn publish_public_event(
        &self,
        claim: &PublicEventClaim,
        nonterminal_retention_seconds: u32,
    ) -> Result<bool, RepositoryError> {
        validate_retention_seconds(nonterminal_retention_seconds)?;
        let _writer = self.writer.lock().await;
        let authority = complete_public_query!(
            "FROM public_event_outbox outbox
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=outbox.run_id
              AND decision.execution_event_id=outbox.causation_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=outbox.run_id
              AND receipt.causation_event_id=outbox.causation_event_id
             LEFT JOIN execution_events event
               ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
             WHERE outbox.run_id=? AND outbox.public_event_id=?"
        )
        .bind(claim.run_id().as_str())
        .bind(claim.public_event_id())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::invalid_data)?;
        let decoded = decode_sqlite_complete_public_row(
            &authority,
            Some(claim.run_id()),
            Some(claim.public_event_id()),
        )?;
        if decoded.terminal != claim.is_terminal()
            || decoded.safe_envelope.as_ref() != Some(claim.safe_envelope())
        {
            return Err(RepositoryError::invalid_data());
        }
        let rows = sqlx::query(
            "UPDATE public_event_outbox
             SET publish_state = 'published',
                 published_at = STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 notified_at = STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 published_by = ?, published_claim_token = ?,
                 claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
                 retain_until = CASE WHEN is_terminal = 1 THEN NULL
                     ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds') END
             WHERE run_id = ? AND public_event_id = ? AND publish_state = 'claimed'
               AND claimed_by = ? AND claim_token = ?
               AND JULIANDAY(claim_expires_at) > JULIANDAY('now')",
        )
        .bind(claim.claimant())
        .bind(claim.claim_token())
        .bind(i64::from(nonterminal_retention_seconds))
        .bind(claim.run_id().as_str())
        .bind(claim.public_event_id())
        .bind(claim.claimant())
        .bind(claim.claim_token())
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows == 1 {
            return Ok(true);
        }
        let state = sqlx::query_scalar::<_, String>(
            "SELECT publish_state FROM public_event_outbox
             WHERE run_id = ? AND public_event_id = ?
               AND published_by = ? AND published_claim_token = ?",
        )
        .bind(claim.run_id().as_str())
        .bind(claim.public_event_id())
        .bind(claim.claimant())
        .bind(claim.claim_token())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(state.as_deref() == Some("published"))
    }

    async fn prune_expired_public_events(&self, limit: u32) -> Result<u64, RepositoryError> {
        validate_prune_limit(limit)?;
        let _writer = self.writer.lock().await;
        sqlx::query(
            "DELETE FROM public_event_outbox
             WHERE rowid IN (
                 SELECT rowid
                 FROM public_event_outbox
                 WHERE publish_state = 'published' AND is_terminal = 0
                   AND retain_until IS NOT NULL
                   AND JULIANDAY(retain_until) <= JULIANDAY('now')
                 ORDER BY retain_until, run_id, public_event_id
                 LIMIT ?
             )",
        )
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await
        .map(|result| result.rows_affected())
        .map_err(RepositoryError::storage)
    }

    async fn load_terminal_public_event(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PublicEventEnvelope>, RepositoryError> {
        let row = complete_public_query!(
            "FROM public_event_outbox outbox
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=outbox.run_id
              AND decision.execution_event_id=outbox.causation_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=outbox.run_id
              AND receipt.causation_event_id=outbox.causation_event_id
             LEFT JOIN execution_events event
               ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
             WHERE outbox.run_id=? AND outbox.is_terminal=1
               AND outbox.publish_state='published'
             ORDER BY outbox.created_at,outbox.public_event_id LIMIT 1"
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| decode_sqlite_public_event_row(&row, Some(run_id), None).map(|row| row.3))
            .transpose()
    }

    async fn load_terminal_public_event_authority(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PublicEventEnvelope>, RepositoryError> {
        let row = complete_public_query!(
            "FROM workflow_runs run
             LEFT JOIN public_event_outbox outbox
               ON outbox.run_id=run.run_id
              AND outbox.public_event_id=run.terminal_public_event_id
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=run.run_id
              AND decision.public_event_id=run.terminal_public_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=run.run_id
              AND receipt.public_event_id=run.terminal_public_event_id
             LEFT JOIN execution_events event
               ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
             WHERE run.run_id=? AND run.terminal_public_event_id IS NOT NULL"
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| decode_sqlite_public_event_row(&row, Some(run_id), None).map(|row| row.3))
            .transpose()
    }

    async fn load_published_public_event(
        &self,
        public_event_id: &str,
    ) -> Result<Option<PublishedPublicEvent>, RepositoryError> {
        if !valid_public_event_id_hint(public_event_id) {
            return Ok(None);
        }
        let row = complete_public_query!(
            "FROM public_event_outbox outbox
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=outbox.run_id
              AND decision.execution_event_id=outbox.causation_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=outbox.run_id
              AND receipt.causation_event_id=outbox.causation_event_id
             LEFT JOIN execution_events event
               ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
             WHERE outbox.public_event_id=? AND outbox.publish_state='published'"
        )
        .bind(public_event_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| {
            let (run_id, public_event_id, terminal, safe_envelope) =
                decode_sqlite_public_event_row(&row, None, Some(public_event_id))?;
            let position = position_from_public_envelope(&public_event_id, &safe_envelope)?;
            Ok(public_outbox_contract_adapter::published_public_event(
                run_id,
                public_event_id,
                terminal,
                safe_envelope,
                position,
            ))
        })
        .transpose()
    }

    async fn load_next_public_event(
        &self,
        run_id: &RunId,
        after: Option<&PublicEventPosition>,
    ) -> Result<OrderedPublicEventRead, RepositoryError> {
        if after.is_some_and(|position| position.run_id() != run_id) {
            return Err(RepositoryError::invalid_data());
        }
        let after_seq = after
            .map(|position| i64_from_u64(position.causation_seq()))
            .transpose()?;
        let after_ordinal = after.map(|position| i64::from(position.public_ordinal()));
        let after_id = after.map(PublicEventPosition::public_event_id);
        let row = complete_public_query!(
            "FROM (
                 SELECT public_decision.run_id,public_decision.execution_event_id,
                        public_decision.execution_seq,public_decision.public_ordinal,
                        public_decision.public_event_id
                 FROM public_event_projection_decisions public_decision
                 WHERE public_decision.decision='public'
                 UNION ALL
                 SELECT orphan.run_id,orphan.causation_event_id,event.seq,
                        orphan.public_ordinal,orphan.public_event_id
                 FROM public_event_receipts orphan
                 LEFT JOIN public_event_projection_decisions existing
                   ON existing.run_id=orphan.run_id
                  AND existing.execution_event_id=orphan.causation_event_id
                 LEFT JOIN execution_events event
                   ON event.run_id=orphan.run_id AND event.event_id=orphan.causation_event_id
                 WHERE COALESCE(existing.decision,'')<>'public'
                 UNION ALL
                 SELECT orphan.run_id,orphan.causation_event_id,event.seq,
                        orphan.public_ordinal,orphan.public_event_id
                 FROM public_event_outbox orphan
                 LEFT JOIN public_event_projection_decisions existing
                   ON existing.run_id=orphan.run_id
                  AND existing.execution_event_id=orphan.causation_event_id
                 LEFT JOIN public_event_receipts receipt_witness
                   ON receipt_witness.run_id=orphan.run_id
                  AND receipt_witness.causation_event_id=orphan.causation_event_id
                 LEFT JOIN execution_events event
                   ON event.run_id=orphan.run_id AND event.event_id=orphan.causation_event_id
                 WHERE COALESCE(existing.decision,'')<>'public'
                   AND receipt_witness.public_event_id IS NULL
             ) directory
             LEFT JOIN public_event_projection_decisions decision
               ON decision.run_id=directory.run_id
              AND decision.execution_event_id=directory.execution_event_id
             LEFT JOIN public_event_receipts receipt
               ON receipt.run_id=directory.run_id
              AND receipt.causation_event_id=directory.execution_event_id
             LEFT JOIN public_event_outbox outbox
               ON outbox.run_id=directory.run_id
              AND outbox.causation_event_id=directory.execution_event_id
             LEFT JOIN execution_events event
               ON event.run_id=directory.run_id
              AND event.event_id=directory.execution_event_id
             WHERE directory.run_id=?1
               AND (?2 IS NULL
                    OR directory.execution_seq>?2
                    OR (directory.execution_seq=?2 AND directory.public_ordinal>?3)
                    OR (directory.execution_seq=?2 AND directory.public_ordinal=?3
                        AND directory.public_event_id>?4))
             ORDER BY directory.execution_seq,directory.public_ordinal,
                      directory.public_event_id
             LIMIT 1"
        )
        .bind(run_id.as_str())
        .bind(after_seq)
        .bind(after_ordinal)
        .bind(after_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| decode_sqlite_ordered_public_event_row(&row, run_id))
            .unwrap_or(Ok(OrderedPublicEventRead::UpToDate))
    }

    async fn open_public_event_notification_stream(
        &self,
    ) -> Result<Option<Box<dyn PublicEventNotificationStream>>, RepositoryError> {
        Ok(None)
    }
}

fn decode_postgres_ordered_public_event_row(
    row: &PgRow,
    expected_run_id: &RunId,
) -> Result<OrderedPublicEventRead, RepositoryError> {
    let complete = decode_postgres_complete_public_row(row, Some(expected_run_id), None)?;
    let Some(publish_state) = complete.publish_state.as_deref() else {
        return Ok(OrderedPublicEventRead::RetentionGap);
    };
    match publish_state {
        "pending" | "claimed" => Ok(OrderedPublicEventRead::Pending),
        "published" => Ok(OrderedPublicEventRead::Event(Box::new(
            public_outbox_contract_adapter::published_public_event(
                complete.run_id,
                complete.public_event_id,
                complete.terminal,
                complete
                    .safe_envelope
                    .ok_or_else(RepositoryError::invalid_data)?,
                complete.position,
            ),
        ))),
        _ => Err(RepositoryError::invalid_data()),
    }
}

fn decode_sqlite_ordered_public_event_row(
    row: &SqliteRow,
    expected_run_id: &RunId,
) -> Result<OrderedPublicEventRead, RepositoryError> {
    let complete = decode_sqlite_complete_public_row(row, Some(expected_run_id), None)?;
    let Some(publish_state) = complete.publish_state.as_deref() else {
        return Ok(OrderedPublicEventRead::RetentionGap);
    };
    match publish_state {
        "pending" | "claimed" => Ok(OrderedPublicEventRead::Pending),
        "published" => Ok(OrderedPublicEventRead::Event(Box::new(
            public_outbox_contract_adapter::published_public_event(
                complete.run_id,
                complete.public_event_id,
                complete.terminal,
                complete
                    .safe_envelope
                    .ok_or_else(RepositoryError::invalid_data)?,
                complete.position,
            ),
        ))),
        _ => Err(RepositoryError::invalid_data()),
    }
}

fn position_from_public_envelope(
    public_event_id: &str,
    envelope: &PublicEventEnvelope,
) -> Result<PublicEventPosition, RepositoryError> {
    public_outbox_contract_adapter::public_event_position(
        envelope.run_id().clone(),
        envelope.seq().get(),
        public_event_ordinal(envelope.kind()),
        public_event_id.to_owned(),
    )
}

fn decode_safe_envelope(
    run_id: &RunId,
    public_event_id: &str,
    causation_event_id: &str,
    event_kind: &str,
    terminal: bool,
    safe_envelope: Value,
) -> Result<PublicEventEnvelope, RepositoryError> {
    let envelope: PublicEventEnvelope =
        serde_json::from_value(safe_envelope).map_err(|_| RepositoryError::invalid_data())?;
    let envelope_terminal = matches!(
        envelope.kind(),
        PublicEventKind::RunCompleted
            | PublicEventKind::RunFailed
            | PublicEventKind::RunCancelled
            | PublicEventKind::RunInterrupted
    );
    if envelope.run_id() != run_id
        || envelope.public_event_id().as_str() != public_event_id
        || envelope.causation_event_id().as_str() != causation_event_id
        || envelope.kind().as_str() != event_kind
        || envelope_terminal != terminal
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(envelope)
}

fn validate_public_execution_binding(
    public: &PublicEventEnvelope,
    execution: &ExecutionEventEnvelope,
) -> Result<(), RepositoryError> {
    if public.run_id() != execution.run_id()
        || public.causation_event_id() != execution.event_id()
        || public.seq() != execution.seq()
        || public.occurred_at() != execution.occurred_at()
    {
        return Err(RepositoryError::invalid_data());
    }
    let semantic_match = match (public.payload(), execution.payload()) {
        (PublicEventPayload::RunCreated, ExecutionEventPayload::RunCreated { .. }) => true,
        (
            PublicEventPayload::RunStarted,
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Active,
            },
        ) => true,
        (
            PublicEventPayload::RunCompleted,
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Succeeded,
            },
        ) => true,
        (
            PublicEventPayload::RunFailed { .. },
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Failed | RunLifecycle::TimedOut,
            },
        ) => true,
        (
            PublicEventPayload::RunCancelled { .. },
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Cancelled,
            },
        ) => true,
        (
            PublicEventPayload::RunInterrupted { .. },
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Interrupted,
            },
        ) => true,
        (
            PublicEventPayload::OperationStarted { .. },
            ExecutionEventPayload::AttemptRunning { .. },
        ) => true,
        (
            PublicEventPayload::OperationCompleted { output_bytes, .. },
            ExecutionEventPayload::AttemptSucceeded {
                output: Some(output),
            },
        ) => *output_bytes == output.size_bytes(),
        (
            PublicEventPayload::OperationFailed { failure, .. },
            ExecutionEventPayload::AttemptTimedOut,
        ) => {
            failure.kind == PublicFailureKind::Timeout
                && failure.code.as_str() == "OPERATION_TIMEOUT"
        }
        (
            PublicEventPayload::OperationFailed { failure, .. },
            ExecutionEventPayload::AttemptFailed {
                failure: Some(internal),
            },
        ) => match internal.kind() {
            InternalFailureKind::Business | InternalFailureKind::EffectOutcomeUnknown => {
                failure.kind == PublicFailureKind::Operation
                    && failure.code.as_str() == "OPERATION_FAILED"
            }
            InternalFailureKind::Infrastructure | InternalFailureKind::Invariant => {
                failure.kind == PublicFailureKind::Infrastructure
                    && failure.code.as_str() == "OPERATION_FAILED"
            }
            InternalFailureKind::Cancelled => {
                failure.kind == PublicFailureKind::Stop
                    && failure.code.as_str() == "OPERATION_STOPPED"
            }
            InternalFailureKind::Workflow | InternalFailureKind::Timeout => false,
        },
        _ => false,
    };
    if !semantic_match {
        return Err(RepositoryError::invalid_data());
    }
    match public.payload() {
        PublicEventPayload::OperationStarted {
            node_id,
            activation_id,
            attempt_no,
        }
        | PublicEventPayload::OperationCompleted {
            node_id,
            activation_id,
            attempt_no,
            ..
        }
        | PublicEventPayload::OperationFailed {
            node_id,
            activation_id,
            attempt_no,
            ..
        } => {
            if execution.node_id() != Some(node_id)
                || execution.activation_id() != Some(activation_id)
                || execution.attempt_no() != Some(*attempt_no)
            {
                return Err(RepositoryError::invalid_data());
            }
        }
        _ => {
            if execution.node_id().is_some()
                || execution.scope_instance_id().is_some()
                || execution.activation_id().is_some()
                || execution.attempt_no().is_some()
            {
                return Err(RepositoryError::invalid_data());
            }
        }
    }
    Ok(())
}

struct CompletePublicAuthority {
    run_id: RunId,
    public_event_id: String,
    terminal: bool,
    publish_state: Option<String>,
    safe_envelope: Option<PublicEventEnvelope>,
    position: PublicEventPosition,
}

fn decode_postgres_complete_public_row(
    row: &PgRow,
    expected_run_id: Option<&RunId>,
    expected_public_event_id: Option<&str>,
) -> Result<CompletePublicAuthority, RepositoryError> {
    if row
        .try_get::<i64, _>("receipt_causation_count")
        .map_err(|_| RepositoryError::invalid_data())?
        != 1
    {
        return Err(RepositoryError::invalid_data());
    }
    let execution = super::postgres::decode_execution_event_row(row)?;
    let decision_run_id = row
        .try_get::<String, _>("decision_run_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_event_id = row
        .try_get::<String, _>("execution_event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_seq = row
        .try_get::<i64, _>("execution_seq")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_occurred_at = row
        .try_get::<DateTime<Utc>, _>("execution_occurred_at")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_transition_key = row
        .try_get::<String, _>("execution_transition_key")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_public_event_id = row
        .try_get::<Option<String>, _>("decision_public_event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_public_ordinal = row
        .try_get::<Option<i32>, _>("decision_public_ordinal")
        .map_err(|_| RepositoryError::invalid_data())?
        .map(i64::from);
    let decision_public_schema_version = row
        .try_get::<Option<i32>, _>("decision_public_schema_version")
        .map_err(|_| RepositoryError::invalid_data())?
        .map(i64::from);
    let decision_event_kind = row
        .try_get::<Option<String>, _>("decision_event_kind")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_terminal = row
        .try_get::<Option<bool>, _>("decision_is_terminal")
        .map_err(|_| RepositoryError::invalid_data())?;
    let outbox_public_event_id = row
        .try_get::<Option<String>, _>("outbox_public_event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let public_event_id = decode_public_projection_decision(
        StoredPublicProjectionDecision {
            run_id: decision_run_id.clone(),
            execution_event_id: decision_event_id,
            execution_seq: decision_seq,
            execution_occurred_at: decision_occurred_at,
            execution_transition_key: decision_transition_key,
            decision: row
                .try_get("projection_decision")
                .map_err(|_| RepositoryError::invalid_data())?,
            public_event_id: decision_public_event_id,
            public_ordinal: decision_public_ordinal,
            public_schema_version: decision_public_schema_version,
            event_kind: decision_event_kind.clone(),
            is_terminal: decision_terminal,
            receipt_public_event_id: row
                .try_get("receipt_public_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_causation_event_id: row
                .try_get("receipt_causation_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_public_ordinal: row
                .try_get::<Option<i32>, _>("receipt_public_ordinal")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(i64::from),
            receipt_public_schema_version: row
                .try_get::<Option<i32>, _>("receipt_public_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(i64::from),
            receipt_event_kind: row
                .try_get("receipt_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_is_terminal: row
                .try_get("receipt_is_terminal")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_public_event_id: outbox_public_event_id.clone(),
            outbox_causation_event_id: row
                .try_get("public_causation_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_public_ordinal: row
                .try_get::<Option<i32>, _>("outbox_public_ordinal")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(i64::from),
            outbox_public_schema_version: row
                .try_get::<Option<i32>, _>("outbox_public_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?
                .map(i64::from),
            outbox_event_kind: row
                .try_get("outbox_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_is_terminal: row
                .try_get("outbox_is_terminal")
                .map_err(|_| RepositoryError::invalid_data())?,
        },
        &execution,
    )?
    .ok_or_else(RepositoryError::invalid_data)?;
    let run_id = model_run_id(decision_run_id)?;
    if expected_run_id.is_some_and(|expected| expected != &run_id)
        || expected_public_event_id.is_some_and(|expected| expected != public_event_id)
    {
        return Err(RepositoryError::invalid_data());
    }
    let terminal = decision_terminal.ok_or_else(RepositoryError::invalid_data)?;
    let ordinal = decision_public_ordinal.ok_or_else(RepositoryError::invalid_data)?;
    let position = public_outbox_contract_adapter::public_event_position(
        run_id.clone(),
        u64_from_i64(decision_seq)?,
        u16::try_from(ordinal).map_err(|_| RepositoryError::invalid_data())?,
        public_event_id.clone(),
    )?;
    let public_run_id = row
        .try_get::<Option<String>, _>("public_run_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let publish_state = row
        .try_get::<Option<String>, _>("publish_state")
        .map_err(|_| RepositoryError::invalid_data())?;
    let safe_value = row
        .try_get::<Option<Value>, _>("safe_envelope")
        .map_err(|_| RepositoryError::invalid_data())?;
    let safe_envelope = match outbox_public_event_id {
        None if public_run_id.is_none() && publish_state.is_none() && safe_value.is_none() => None,
        Some(outbox_id) => {
            let outbox_run_id = public_run_id.ok_or_else(RepositoryError::invalid_data)?;
            let causation_event_id = row
                .try_get::<Option<String>, _>("public_causation_event_id")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?;
            let outbox_kind = row
                .try_get::<Option<String>, _>("outbox_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?;
            let outbox_terminal = row
                .try_get::<Option<bool>, _>("outbox_is_terminal")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?;
            if outbox_run_id != run_id.as_str() || outbox_id != public_event_id {
                return Err(RepositoryError::invalid_data());
            }
            let envelope = decode_safe_envelope(
                &run_id,
                &public_event_id,
                &causation_event_id,
                &outbox_kind,
                outbox_terminal,
                safe_value.ok_or_else(RepositoryError::invalid_data)?,
            )?;
            validate_public_execution_binding(&envelope, &execution)?;
            Some(envelope)
        }
        _ => return Err(RepositoryError::invalid_data()),
    };
    Ok(CompletePublicAuthority {
        run_id,
        public_event_id,
        terminal,
        publish_state,
        safe_envelope,
        position,
    })
}

fn decode_sqlite_complete_public_row(
    row: &SqliteRow,
    expected_run_id: Option<&RunId>,
    expected_public_event_id: Option<&str>,
) -> Result<CompletePublicAuthority, RepositoryError> {
    if row
        .try_get::<i64, _>("receipt_causation_count")
        .map_err(|_| RepositoryError::invalid_data())?
        != 1
    {
        return Err(RepositoryError::invalid_data());
    }
    let execution = super::sqlite::decode_execution_event_row(row)?;
    let sqlite_bool = |value: Option<i64>| match value {
        None => Ok(None),
        Some(0) => Ok(Some(false)),
        Some(1) => Ok(Some(true)),
        Some(_) => Err(RepositoryError::invalid_data()),
    };
    let decision_run_id = row
        .try_get::<String, _>("decision_run_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_event_id = row
        .try_get::<String, _>("execution_event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_seq = row
        .try_get::<i64, _>("execution_seq")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_occurred_at = super::sqlite::parse_run_timestamp(
        &row.try_get::<String, _>("execution_occurred_at")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let decision_transition_key = row
        .try_get::<String, _>("execution_transition_key")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_public_event_id = row
        .try_get::<Option<String>, _>("decision_public_event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_public_ordinal = row
        .try_get::<Option<i64>, _>("decision_public_ordinal")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_public_schema_version = row
        .try_get::<Option<i64>, _>("decision_public_schema_version")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_event_kind = row
        .try_get::<Option<String>, _>("decision_event_kind")
        .map_err(|_| RepositoryError::invalid_data())?;
    let decision_terminal = sqlite_bool(
        row.try_get("decision_is_terminal")
            .map_err(|_| RepositoryError::invalid_data())?,
    )?;
    let outbox_public_event_id = row
        .try_get::<Option<String>, _>("outbox_public_event_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let public_event_id = decode_public_projection_decision(
        StoredPublicProjectionDecision {
            run_id: decision_run_id.clone(),
            execution_event_id: decision_event_id,
            execution_seq: decision_seq,
            execution_occurred_at: decision_occurred_at,
            execution_transition_key: decision_transition_key,
            decision: row
                .try_get("projection_decision")
                .map_err(|_| RepositoryError::invalid_data())?,
            public_event_id: decision_public_event_id,
            public_ordinal: decision_public_ordinal,
            public_schema_version: decision_public_schema_version,
            event_kind: decision_event_kind,
            is_terminal: decision_terminal,
            receipt_public_event_id: row
                .try_get("receipt_public_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_causation_event_id: row
                .try_get("receipt_causation_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_public_ordinal: row
                .try_get("receipt_public_ordinal")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_public_schema_version: row
                .try_get("receipt_public_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_event_kind: row
                .try_get("receipt_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            receipt_is_terminal: sqlite_bool(
                row.try_get("receipt_is_terminal")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
            outbox_public_event_id: outbox_public_event_id.clone(),
            outbox_causation_event_id: row
                .try_get("public_causation_event_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_public_ordinal: row
                .try_get("outbox_public_ordinal")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_public_schema_version: row
                .try_get("outbox_public_schema_version")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_event_kind: row
                .try_get("outbox_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?,
            outbox_is_terminal: sqlite_bool(
                row.try_get("outbox_is_terminal")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?,
        },
        &execution,
    )?
    .ok_or_else(RepositoryError::invalid_data)?;
    let run_id = model_run_id(decision_run_id)?;
    if expected_run_id.is_some_and(|expected| expected != &run_id)
        || expected_public_event_id.is_some_and(|expected| expected != public_event_id)
    {
        return Err(RepositoryError::invalid_data());
    }
    let terminal = decision_terminal.ok_or_else(RepositoryError::invalid_data)?;
    let ordinal = decision_public_ordinal.ok_or_else(RepositoryError::invalid_data)?;
    let position = public_outbox_contract_adapter::public_event_position(
        run_id.clone(),
        u64_from_i64(decision_seq)?,
        u16::try_from(ordinal).map_err(|_| RepositoryError::invalid_data())?,
        public_event_id.clone(),
    )?;
    let public_run_id = row
        .try_get::<Option<String>, _>("public_run_id")
        .map_err(|_| RepositoryError::invalid_data())?;
    let publish_state = row
        .try_get::<Option<String>, _>("publish_state")
        .map_err(|_| RepositoryError::invalid_data())?;
    let safe_encoded = row
        .try_get::<Option<String>, _>("safe_envelope")
        .map_err(|_| RepositoryError::invalid_data())?;
    let safe_envelope = match outbox_public_event_id {
        None if public_run_id.is_none() && publish_state.is_none() && safe_encoded.is_none() => {
            None
        }
        Some(outbox_id) => {
            let outbox_run_id = public_run_id.ok_or_else(RepositoryError::invalid_data)?;
            let causation_event_id = row
                .try_get::<Option<String>, _>("public_causation_event_id")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?;
            let outbox_kind = row
                .try_get::<Option<String>, _>("outbox_event_kind")
                .map_err(|_| RepositoryError::invalid_data())?
                .ok_or_else(RepositoryError::invalid_data)?;
            let outbox_terminal = sqlite_bool(
                row.try_get("outbox_is_terminal")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?
            .ok_or_else(RepositoryError::invalid_data)?;
            if outbox_run_id != run_id.as_str() || outbox_id != public_event_id {
                return Err(RepositoryError::invalid_data());
            }
            let envelope = decode_safe_envelope(
                &run_id,
                &public_event_id,
                &causation_event_id,
                &outbox_kind,
                outbox_terminal,
                serde_json::from_str(&safe_encoded.ok_or_else(RepositoryError::invalid_data)?)
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            validate_public_execution_binding(&envelope, &execution)?;
            Some(envelope)
        }
        _ => return Err(RepositoryError::invalid_data()),
    };
    Ok(CompletePublicAuthority {
        run_id,
        public_event_id,
        terminal,
        publish_state,
        safe_envelope,
        position,
    })
}

fn decode_postgres_public_event_row(
    row: &PgRow,
    expected_run_id: Option<&RunId>,
    expected_public_event_id: Option<&str>,
) -> Result<(RunId, String, bool, PublicEventEnvelope), RepositoryError> {
    let complete =
        decode_postgres_complete_public_row(row, expected_run_id, expected_public_event_id)?;
    Ok((
        complete.run_id,
        complete.public_event_id,
        complete.terminal,
        complete
            .safe_envelope
            .ok_or_else(RepositoryError::invalid_data)?,
    ))
}

fn decode_sqlite_public_event_row(
    row: &SqliteRow,
    expected_run_id: Option<&RunId>,
    expected_public_event_id: Option<&str>,
) -> Result<(RunId, String, bool, PublicEventEnvelope), RepositoryError> {
    let complete =
        decode_sqlite_complete_public_row(row, expected_run_id, expected_public_event_id)?;
    Ok((
        complete.run_id,
        complete.public_event_id,
        complete.terminal,
        complete
            .safe_envelope
            .ok_or_else(RepositoryError::invalid_data)?,
    ))
}

fn valid_public_event_id_hint(value: &str) -> bool {
    value.starts_with("public_event_") && validate_label(value).is_ok()
}

fn validate_claim_request(
    claimant: &str,
    claim_seconds: u32,
    limit: u32,
) -> Result<(), RepositoryError> {
    validate_label(claimant)?;
    if claim_seconds == 0
        || claim_seconds > MAX_CLAIM_SECONDS
        || limit == 0
        || limit > MAX_CLAIM_BATCH
    {
        return Err(RepositoryError::invalid_configuration());
    }
    Ok(())
}

fn validate_retention_seconds(retention_seconds: u32) -> Result<(), RepositoryError> {
    if retention_seconds == 0 || retention_seconds > MAX_NONTERMINAL_RETENTION_SECONDS {
        return Err(RepositoryError::invalid_configuration());
    }
    Ok(())
}

fn validate_prune_limit(limit: u32) -> Result<(), RepositoryError> {
    if limit == 0 || limit > MAX_PRUNE_BATCH {
        return Err(RepositoryError::invalid_configuration());
    }
    Ok(())
}

fn validate_label(value: &str) -> Result<(), RepositoryError> {
    if value.is_empty()
        || value.len() > 256
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(RepositoryError::invalid_configuration());
    }
    Ok(())
}

fn claim_token() -> String {
    format!("public_claim_{}", Uuid::new_v4().simple())
}

fn model_run_id(value: String) -> Result<RunId, RepositoryError> {
    RunId::new(value).map_err(|_| RepositoryError::invalid_data())
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use insight_durable::model::adapter as model_adapter;
    use insight_durable::{CreateRunCommand, DurableRepository, VersionedPlan};
    use insight_engine::{
        ActivationId, AdmissionState, AttemptNo, ContentHash, DefinitionRevisionId,
        DeploymentRevisionId, ExecutionEventContext, ExecutionEventEnvelope, ExecutionEventPayload,
        ExecutionValueSummary, IntentHash, InternalFailureCode, InternalFailureKind,
        InternalFailureSummary, LeaseEpoch, NodeId, PendingExecutionEvent, PublicErrorCode,
        PublicEventEnvelope, PublicEventKind, PublicEventPayload, PublicFailureKind,
        PublicFailureSummary, RunId, RunLifecycle, ScopeInstanceId, TransitionKey,
        TransitionOutcome, EXECUTION_EVENT_SCHEMA_VERSION,
    };
    use serde_json::{json, Value};
    use sqlx::{
        postgres::PgPoolOptions,
        sqlite::{SqliteConnectOptions, SqlitePoolOptions},
        AssertSqlSafe, Postgres, Row, Transaction,
    };

    use super::super::postgres::{allocate_event_seq, begin_write_transaction, insert_event};
    use super::{
        decode_safe_envelope, durable_public_event_envelope, event_id, public_event_id,
        public_event_ordinal, public_outbox_contract_adapter, validate_claim_request,
        validate_prune_limit, validate_retention_seconds, OrderedPublicEventRead,
        PostgresDurableRepository, PublicEventOutboxRepository, SqliteDurableRepository,
        MAX_CLAIM_BATCH, MAX_CLAIM_SECONDS, MAX_NONTERMINAL_RETENTION_SECONDS, MAX_PRUNE_BATCH,
        POSTGRES_PUBLIC_EVENT_RETENTION_PRUNE_SQL,
    };

    #[test]
    fn public_event_claim_limits_fail_closed() {
        assert!(validate_claim_request("dispatcher-a", 30, 10).is_ok());
        assert!(validate_claim_request("dispatcher a", 30, 10).is_err());
        assert!(validate_claim_request("dispatcher-a", 0, 10).is_err());
        assert!(validate_claim_request("dispatcher-a", MAX_CLAIM_SECONDS + 1, 10).is_err());
        assert!(validate_claim_request("dispatcher-a", 30, 0).is_err());
        assert!(validate_claim_request("dispatcher-a", 30, MAX_CLAIM_BATCH + 1).is_err());
        assert!(validate_retention_seconds(1).is_ok());
        assert!(validate_retention_seconds(0).is_err());
        assert!(validate_retention_seconds(MAX_NONTERMINAL_RETENTION_SECONDS + 1).is_err());
        assert!(validate_prune_limit(1).is_ok());
        assert!(validate_prune_limit(0).is_err());
        assert!(validate_prune_limit(MAX_PRUNE_BATCH + 1).is_err());
    }

    #[test]
    fn postgres_retention_prune_uses_an_indexable_statement_clock() {
        let normalized = POSTGRES_PUBLIC_EVENT_RETENTION_PRUNE_SQL
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        assert!(normalized.contains("retain_until <= statement_timestamp()"));
        assert!(!normalized.contains("clock_timestamp()"));
    }

    async fn provisioned_sqlite_file(database: &std::path::Path) -> SqliteDurableRepository {
        std::fs::File::create(database).unwrap();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(database)
                    .create_if_missing(false)
                    .foreign_keys(true),
            )
            .await
            .unwrap();
        super::super::schema_contract::provision_sqlite_for_test(&pool).await;
        pool.close().await;
        SqliteDurableRepository::connect_path(database)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn sqlite_delivery_head_reopen_preserves_pending_and_rolls_back_with_outbox() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("public-delivery-head-reopen.sqlite");
        let repository = provisioned_sqlite_file(&database).await;
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new("run_public_delivery_reopen").unwrap();
        let created = repository
            .create_run(
                transition_key("delivery.reopen.create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap(),
            )
            .await
            .unwrap();
        let public_event_id = created
            .committed_result()
            .and_then(|receipt| receipt.public_event_id())
            .unwrap()
            .to_owned();

        let mut transaction = repository.pool.begin().await.unwrap();
        sqlx::query(
            "UPDATE public_event_outbox
             SET publish_state='claimed',claimed_by='rollback-dispatcher',
                 claim_token='rollback-public-token',
                 claim_expires_at=STRFTIME('%Y-%m-%dT%H:%M:%fZ','now','+30 seconds'),
                 publish_attempts=publish_attempts+1
             WHERE run_id=? AND public_event_id=?",
        )
        .bind(run_id.as_str())
        .bind(&public_event_id)
        .execute(&mut *transaction)
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT delivery_state FROM public_event_delivery_heads
                 WHERE run_id=? AND public_event_id=?",
            )
            .bind(run_id.as_str())
            .bind(&public_event_id)
            .fetch_one(&mut *transaction)
            .await
            .unwrap(),
            "claimed"
        );
        transaction.rollback().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT delivery_state FROM public_event_delivery_heads
                 WHERE run_id=? AND public_event_id=?",
            )
            .bind(run_id.as_str())
            .bind(&public_event_id)
            .fetch_one(&repository.pool)
            .await
            .unwrap(),
            "pending"
        );

        repository.pool.close().await;
        let reopened = SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap();
        let claims = reopened
            .claim_public_events("reopened-public-dispatcher", 30, 1)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].public_event_id(), public_event_id);
    }

    #[tokio::test]
    async fn sqlite_delivery_head_reopen_accepts_exact_receipt_after_body_prune() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("public-delivery-head-pruned.sqlite");
        let repository = provisioned_sqlite_file(&database).await;
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new("run_public_delivery_pruned").unwrap();
        let create_key = transition_key("delivery.pruned.create");
        let create_command =
            CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap();
        let created = repository
            .create_run(create_key.clone(), create_command.clone())
            .await
            .unwrap();
        let public_event_id = created
            .committed_result()
            .and_then(|receipt| receipt.public_event_id())
            .unwrap()
            .to_owned();
        let claim = repository
            .claim_public_events("pruned-public-dispatcher", 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(repository.publish_public_event(&claim, 1).await.unwrap());
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        assert_eq!(repository.prune_expired_public_events(1).await.unwrap(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM public_event_outbox
                 WHERE run_id=? AND public_event_id=?",
            )
            .bind(run_id.as_str())
            .bind(&public_event_id)
            .fetch_one(&repository.pool)
            .await
            .unwrap(),
            0
        );
        repository.pool.close().await;

        let reopened = SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap();
        assert!(matches!(
            reopened
                .load_next_public_event(&run_id, None)
                .await
                .unwrap(),
            OrderedPublicEventRead::RetentionGap
        ));
        let replay = reopened
            .create_run(create_key, create_command)
            .await
            .unwrap();
        assert_eq!(
            replay
                .committed_result()
                .and_then(|receipt| receipt.public_event_id()),
            Some(public_event_id.as_str())
        );
    }

    #[tokio::test]
    async fn sqlite_public_claim_and_successor_queries_use_bounded_indexes() {
        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        let due_plan = sqlx::query(
            "EXPLAIN QUERY PLAN
             SELECT candidate.run_id,candidate.public_event_id,candidate.due_at,
                    candidate.execution_seq,candidate.public_ordinal
             FROM public_event_delivery_heads candidate
             WHERE candidate.head_state='ready'
               AND candidate.due_at<=STRFTIME('%Y-%m-%dT%H:%M:%fZ','now')
             ORDER BY candidate.due_at,candidate.run_id,candidate.execution_seq,
                      candidate.public_ordinal,candidate.public_event_id
             LIMIT 5",
        )
        .fetch_all(&repository.pool)
        .await
        .unwrap();
        assert!(due_plan.iter().any(|row| {
            row.get::<String, _>("detail")
                .contains("idx_public_delivery_heads_due")
        }));
        let successor_plan = sqlx::query(
            "EXPLAIN QUERY PLAN
             SELECT next.public_event_id
             FROM public_event_projection_decisions current
             JOIN public_event_projection_decisions next ON next.run_id=current.run_id
             JOIN public_event_outbox next_outbox
               ON next_outbox.run_id=next.run_id
              AND next_outbox.causation_event_id=next.execution_event_id
              AND next_outbox.public_event_id=next.public_event_id
             WHERE current.run_id=? AND current.execution_event_id=?
               AND current.decision='public' AND next.decision='public'
               AND next_outbox.publish_state<>'published'
               AND next.execution_seq>current.execution_seq
             ORDER BY next.execution_seq,next.public_ordinal,next.public_event_id
             LIMIT 1",
        )
        .bind("run-plan")
        .bind("event-plan")
        .fetch_all(&repository.pool)
        .await
        .unwrap();
        assert!(successor_plan.iter().any(|row| {
            row.get::<String, _>("detail")
                .contains("idx_public_projection_order")
        }));
    }

    fn transition_key(label: &str) -> TransitionKey {
        TransitionKey::derive("public.outbox.test", &[label]).unwrap()
    }

    fn event(run_id: &RunId, lifecycle: RunLifecycle) -> PendingExecutionEvent {
        PendingExecutionEvent::new(
            ExecutionEventContext::for_run(run_id.clone()),
            ExecutionEventPayload::RunLifecycleChanged { lifecycle },
        )
        .unwrap()
    }

    async fn insert_synthetic_run_started(
        transaction: &mut Transaction<'_, Postgres>,
        run_id: &RunId,
        label: &str,
    ) -> String {
        let transition = transition_key(label);
        let execution_event_id = event_id(&transition);
        let intent_hash = IntentHash::from_serializable(&label).unwrap();
        let event_seq = allocate_event_seq(transaction, run_id).await.unwrap();
        let occurred_at = insert_event(
            transaction,
            run_id,
            event_seq,
            &execution_event_id,
            &transition,
            intent_hash.as_str(),
            0,
            &event(run_id, RunLifecycle::Active),
        )
        .await
        .unwrap();
        let payload = PublicEventPayload::RunStarted;
        let kind = payload.kind();
        let public_id = public_event_id(run_id, &transition, kind);
        let envelope = serde_json::to_value(
            durable_public_event_envelope(
                run_id,
                &public_id,
                &execution_event_id,
                event_seq,
                occurred_at,
                payload,
            )
            .unwrap(),
        )
        .unwrap();
        sqlx::query(
            "INSERT INTO public_event_outbox (
                run_id,public_event_id,causation_event_id,public_ordinal,
                public_schema_version,event_kind,is_terminal,publish_state,
                safe_envelope,available_at,claimed_by,claim_token,claim_expires_at,
                publish_attempts,published_at,published_by,published_claim_token,
                notified_at,retain_until,created_at
             ) VALUES (
                $1,$2,$3,$4,1,$5,FALSE,'pending',$6,CURRENT_TIMESTAMP,
                NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,CURRENT_TIMESTAMP
             )",
        )
        .bind(run_id.as_str())
        .bind(&public_id)
        .bind(&execution_event_id)
        .bind(i32::from(public_event_ordinal(kind)))
        .bind(kind.as_str())
        .bind(envelope)
        .execute(&mut **transaction)
        .await
        .unwrap();
        public_id
    }

    async fn insert_test_public_outbox(
        transaction: &mut Transaction<'_, Postgres>,
        run_id: &RunId,
        public_id: &str,
        execution_event_id: &str,
        kind: PublicEventKind,
        envelope: &Value,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO public_event_outbox (
                run_id,public_event_id,causation_event_id,public_ordinal,
                public_schema_version,event_kind,is_terminal,publish_state,
                safe_envelope,available_at,claimed_by,claim_token,claim_expires_at,
                publish_attempts,published_at,published_by,published_claim_token,
                notified_at,retain_until,created_at
             ) VALUES (
                $1,$2,$3,$4,1,$5,FALSE,'pending',$6,CURRENT_TIMESTAMP,
                NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,CURRENT_TIMESTAMP
             )",
        )
        .bind(run_id.as_str())
        .bind(public_id)
        .bind(execution_event_id)
        .bind(i32::from(public_event_ordinal(kind)))
        .bind(kind.as_str())
        .bind(envelope)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    fn plan() -> VersionedPlan {
        insight_durable::model::adapter::versioned_plan_for_test(
            "definition_public_outbox",
            "agent_public_outbox",
            "Public outbox test",
            DefinitionRevisionId::new("definition_revision_public_outbox_v1").unwrap(),
            DeploymentRevisionId::new("deployment_revision_public_outbox_v1").unwrap(),
            ContentHash::from_bytes(b"public-outbox-plan"),
            ContentHash::from_bytes(b"public-outbox-binding"),
            "compiler-3.0.0",
            "expression-3.0.0",
            json!({"kind": "structured"}),
            json!({"nodes": []}),
            json!({}),
            json!({"model": "fixed"}),
            json!({"worker": "v1"}),
        )
        .unwrap()
    }

    fn committed_test_execution(
        run_id: &RunId,
        label: &str,
        payload: ExecutionEventPayload,
    ) -> (ExecutionEventEnvelope, TransitionKey, DateTime<Utc>) {
        let transition = transition_key(label);
        let context = ExecutionEventContext::for_run(run_id.clone()).for_attempt(
            ScopeInstanceId::root(),
            NodeId::new("operation_node").unwrap(),
            ActivationId::new("activation_operation").unwrap(),
            AttemptNo::FIRST,
        );
        let pending = PendingExecutionEvent::new(context, payload).unwrap();
        let occurred_at = Utc::now();
        let event_id = event_id(&transition);
        let intent_hash = IntentHash::from_serializable(&label).unwrap();
        let execution = serde_json::from_value(json!({
            "schema_version": EXECUTION_EVENT_SCHEMA_VERSION,
            "event_id": event_id,
            "run_id": run_id,
            "transition_key": transition,
            "intent_hash": intent_hash,
            "seq": 1,
            "occurred_at": occurred_at,
            "kind": pending.kind(),
            "node_id": pending.context().node_id(),
            "scope_instance_id": pending.context().scope_instance_id(),
            "activation_id": pending.context().activation_id(),
            "attempt_no": pending.context().attempt_no(),
            "causation_event_id": pending.context().causation_event_id(),
            "payload": pending.payload(),
        }))
        .unwrap();
        (execution, transition, occurred_at)
    }

    #[test]
    fn operation_public_projection_accepts_the_three_closed_positive_mappings() {
        let run_id = RunId::new("run_operation_public_binding").unwrap();
        let cases = [
            (
                "operation.binding.started",
                ExecutionEventPayload::AttemptRunning {
                    lease_epoch: LeaseEpoch::FIRST,
                },
                PublicEventPayload::OperationStarted {
                    node_id: NodeId::new("operation_node").unwrap(),
                    activation_id: ActivationId::new("activation_operation").unwrap(),
                    attempt_no: AttemptNo::FIRST,
                },
            ),
            (
                "operation.binding.completed",
                ExecutionEventPayload::AttemptSucceeded {
                    output: Some(ExecutionValueSummary::new(
                        ContentHash::from_bytes(b"operation-output"),
                        17,
                    )),
                },
                PublicEventPayload::OperationCompleted {
                    node_id: NodeId::new("operation_node").unwrap(),
                    activation_id: ActivationId::new("activation_operation").unwrap(),
                    attempt_no: AttemptNo::FIRST,
                    elapsed_ms: 5,
                    output_bytes: 17,
                },
            ),
            (
                "operation.binding.failed",
                ExecutionEventPayload::AttemptFailed {
                    failure: Some(InternalFailureSummary::new(
                        InternalFailureKind::Business,
                        InternalFailureCode::new("PROVIDER_FAILED").unwrap(),
                    )),
                },
                PublicEventPayload::OperationFailed {
                    node_id: NodeId::new("operation_node").unwrap(),
                    activation_id: ActivationId::new("activation_operation").unwrap(),
                    attempt_no: AttemptNo::FIRST,
                    elapsed_ms: 5,
                    failure: PublicFailureSummary {
                        kind: PublicFailureKind::Operation,
                        code: PublicErrorCode::new("OPERATION_FAILED").unwrap(),
                    },
                },
            ),
        ];
        for (label, execution_payload, public_payload) in cases {
            let (execution, transition, occurred_at) =
                committed_test_execution(&run_id, label, execution_payload);
            let public_id = public_event_id(&run_id, &transition, public_payload.kind());
            let public = durable_public_event_envelope(
                &run_id,
                &public_id,
                execution.event_id().as_str(),
                1,
                occurred_at,
                public_payload,
            )
            .unwrap();
            super::validate_public_execution_binding(&public, &execution).unwrap();
        }
    }

    #[tokio::test]
    async fn ordered_public_read_is_run_bound_gap_aware_and_receipt_validated() {
        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_a = RunId::new("run_ordered_public_a").unwrap();
        let run_b = RunId::new("run_ordered_public_b").unwrap();
        for (label, run_id) in [("ordered.a", &run_a), ("ordered.b", &run_b)] {
            repository
                .create_run(
                    transition_key(label),
                    CreateRunCommand::new(run_id.clone(), &plan, json!({"label": label})).unwrap(),
                )
                .await
                .unwrap();
        }

        assert_eq!(
            repository
                .load_next_public_event(&run_a, None)
                .await
                .unwrap(),
            OrderedPublicEventRead::Pending
        );
        let claims = repository
            .claim_public_events("ordered-dispatcher", 30, 10)
            .await
            .unwrap();
        assert_eq!(claims.len(), 2);
        for claim in &claims {
            assert!(repository.publish_public_event(claim, 1).await.unwrap());
        }

        let first_a = match repository
            .load_next_public_event(&run_a, None)
            .await
            .unwrap()
        {
            OrderedPublicEventRead::Event(event) => event,
            other => panic!("expected published event, got {other:?}"),
        };
        assert_eq!(first_a.run_id(), &run_a);
        assert!(repository
            .load_next_public_event(&run_b, Some(first_a.position()))
            .await
            .is_err());
        assert_eq!(
            repository
                .load_next_public_event(&run_a, Some(first_a.position()))
                .await
                .unwrap(),
            OrderedPublicEventRead::UpToDate
        );

        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        assert!(repository.prune_expired_public_events(10).await.unwrap() >= 2);
        assert_eq!(
            repository
                .load_next_public_event(&run_a, None)
                .await
                .unwrap(),
            OrderedPublicEventRead::RetentionGap
        );

        let run_c = RunId::new("run_ordered_public_c").unwrap();
        repository
            .create_run(
                transition_key("ordered.c"),
                CreateRunCommand::new(run_c.clone(), &plan, json!({"label": "c"})).unwrap(),
            )
            .await
            .unwrap();
        let claim = repository
            .claim_public_events("ordered-corruption-dispatcher", 30, 10)
            .await
            .unwrap()
            .into_iter()
            .find(|claim| claim.run_id() == &run_c)
            .unwrap();
        assert!(repository.publish_public_event(&claim, 60).await.unwrap());
        sqlx::query("DROP TRIGGER public_event_receipt_update_forbidden")
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE public_event_receipts SET public_ordinal=11
             WHERE run_id=? AND public_event_id=?",
        )
        .bind(run_c.as_str())
        .bind(claim.public_event_id())
        .execute(&repository.pool)
        .await
        .unwrap();
        assert!(repository
            .load_next_public_event(&run_c, None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn sqlite_missing_public_receipt_or_decision_fails_all_authority_boundaries() {
        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new("run_missing_public_receipt").unwrap();
        let key = transition_key("missing.receipt.create");
        let command =
            CreateRunCommand::new(run_id.clone(), &plan, json!({"input": "receipt"})).unwrap();
        repository
            .create_run(key.clone(), command.clone())
            .await
            .unwrap();
        let claim = repository
            .claim_public_events("missing-receipt-dispatcher", 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        sqlx::query("DROP TRIGGER public_event_receipt_delete_forbidden")
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM public_event_receipts WHERE run_id=? AND public_event_id=?")
            .bind(run_id.as_str())
            .bind(claim.public_event_id())
            .execute(&repository.pool)
            .await
            .unwrap();
        assert_eq!(
            repository
                .publish_public_event(&claim, 60)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        assert!(repository
            .load_next_public_event(&run_id, None)
            .await
            .is_err());
        assert_eq!(
            repository
                .create_run(key, command)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );

        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new("run_missing_public_decision").unwrap();
        let key = transition_key("missing.decision.create");
        let command =
            CreateRunCommand::new(run_id.clone(), &plan, json!({"input": "decision"})).unwrap();
        repository
            .create_run(key.clone(), command.clone())
            .await
            .unwrap();
        sqlx::query("DROP TRIGGER public_event_projection_decision_delete_forbidden")
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query("PRAGMA foreign_keys=OFF")
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM public_event_projection_decisions WHERE run_id=?")
            .bind(run_id.as_str())
            .execute(&repository.pool)
            .await
            .unwrap();
        assert_eq!(
            repository
                .claim_public_events("missing-decision-dispatcher", 30, 1)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        assert_eq!(
            repository
                .load_next_public_event(&run_id, None)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        assert_eq!(
            repository
                .create_run(key, command)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
    }

    #[tokio::test]
    async fn sqlite_claim_decodes_only_bounded_heads_and_corrupt_head_fails_closed() {
        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new("run_bounded_public_claim").unwrap();
        repository
            .create_run(
                transition_key("bounded.create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key("bounded.start"),
                model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    0,
                    RunLifecycle::Created,
                    AdmissionState::Open,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    event(&run_id, RunLifecycle::Active),
                    Some(model_adapter::public_event_intent(
                        PublicEventPayload::RunStarted,
                    )),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        sqlx::query("DROP TRIGGER execution_event_projection_ledger_immutable")
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE execution_events SET kind='run.created'
             WHERE run_id=? AND seq=2",
        )
        .bind(run_id.as_str())
        .execute(&repository.pool)
        .await
        .unwrap();

        let first = repository
            .claim_public_events("bounded-head-dispatcher", 30, 1)
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(
            first[0].safe_envelope().payload(),
            &PublicEventPayload::RunCreated,
            "a corrupt later row is outside the bounded head decode set"
        );
        assert!(repository
            .publish_public_event(&first[0], 60)
            .await
            .unwrap());
        assert_eq!(
            repository
                .claim_public_events("bounded-corrupt-head-dispatcher", 30, 1)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID",
            "the corrupt row fails closed as soon as it becomes the blocking head"
        );

        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        repository.install_versioned_plan(&plan).await.unwrap();
        for index in 0..24 {
            let run_id = RunId::new(format!("run_public_backlog_{index}")).unwrap();
            repository
                .create_run(
                    transition_key(&format!("backlog.{index}.create")),
                    CreateRunCommand::new(run_id, &plan, json!({"index": index})).unwrap(),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            repository
                .claim_public_events("bounded-backlog-dispatcher", 30, 5)
                .await
                .unwrap()
                .len(),
            5,
            "claim decoding remains capped by the requested batch limit"
        );
    }

    #[tokio::test]
    async fn public_event_claim_is_reclaimable_fenced_and_terminal_is_durable() {
        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new("run_public_outbox").unwrap();
        repository
            .create_run(
                transition_key("create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key("start"),
                model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    0,
                    RunLifecycle::Created,
                    AdmissionState::Open,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    event(&run_id, RunLifecycle::Active),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key("completing"),
                model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    1,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    RunLifecycle::Completing,
                    AdmissionState::Draining,
                    event(&run_id, RunLifecycle::Completing),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key("terminal"),
                model_adapter::run_transition_terminal_success(
                    run_id.clone(),
                    2,
                    json!({"answer": 42}),
                    event(&run_id, RunLifecycle::Succeeded),
                    model_adapter::public_event_intent(PublicEventPayload::RunCompleted),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        assert!(repository
            .load_terminal_public_event(&run_id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            repository
                .load_terminal_public_event_authority(&run_id)
                .await
                .unwrap()
                .unwrap()
                .payload(),
            &PublicEventPayload::RunCompleted
        );

        let created = repository
            .claim_public_events("dispatcher-a", 30, 10)
            .await
            .unwrap();
        assert_eq!(created.len(), 1);
        assert!(!created[0].is_terminal());
        assert_eq!(
            created[0].safe_envelope().payload(),
            &PublicEventPayload::RunCreated
        );
        assert!(repository
            .publish_public_event(&created[0], 60)
            .await
            .unwrap());

        let first = repository
            .claim_public_events("dispatcher-a", 30, 10)
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert!(first[0].is_terminal());
        assert_eq!(
            first[0].safe_envelope().kind(),
            PublicEventKind::RunCompleted
        );
        let event_occurred_at = sqlx::query_scalar::<_, String>(
            "SELECT occurred_at FROM execution_events WHERE run_id = ? AND event_id = ?",
        )
        .bind(run_id.as_str())
        .bind(first[0].causation_event_id())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert_eq!(
            first[0].safe_envelope().occurred_at(),
            &super::super::sqlite::parse_run_timestamp(&event_occurred_at).unwrap()
        );
        let encoded = serde_json::to_value(first[0].safe_envelope()).unwrap();
        let decoded: PublicEventEnvelope = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(&decoded, first[0].safe_envelope());
        let mut injected = encoded;
        injected
            .as_object_mut()
            .unwrap()
            .insert("provider_body".to_owned(), json!("must-not-escape"));
        assert!(decode_safe_envelope(
            first[0].run_id(),
            first[0].public_event_id(),
            first[0].causation_event_id(),
            first[0].event_kind(),
            first[0].is_terminal(),
            injected,
        )
        .is_err());
        assert!(repository
            .load_published_public_event(first[0].public_event_id())
            .await
            .unwrap()
            .is_none());
        assert!(repository
            .load_terminal_public_event(&run_id)
            .await
            .unwrap()
            .is_none());

        sqlx::query(
            "UPDATE public_event_outbox SET claim_expires_at = '2000-01-01T00:00:00.000000Z'
             WHERE run_id = ? AND public_event_id = ?",
        )
        .bind(run_id.as_str())
        .bind(first[0].public_event_id())
        .execute(&repository.pool)
        .await
        .unwrap();

        let second = repository
            .claim_public_events("dispatcher-b", 30, 10)
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_ne!(first[0].claim_token(), second[0].claim_token());
        assert!(!repository
            .publish_public_event(&first[0], 60)
            .await
            .unwrap());
        assert!(repository
            .publish_public_event(&second[0], 60)
            .await
            .unwrap());
        assert!(
            !repository
                .publish_public_event(&first[0], 60)
                .await
                .unwrap(),
            "a superseded token must remain fenced after the winner publishes"
        );
        assert!(repository
            .publish_public_event(&second[0], 60)
            .await
            .unwrap());
        assert!(repository
            .claim_public_events("dispatcher-c", 30, 10)
            .await
            .unwrap()
            .is_empty());

        let terminal = repository
            .load_terminal_public_event(&run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.payload(), &PublicEventPayload::RunCompleted);
        let published = repository
            .load_published_public_event(second[0].public_event_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(published.run_id(), &run_id);
        assert!(published.is_terminal());
        assert_eq!(published.safe_envelope(), &terminal);

        sqlx::query("DROP TRIGGER execution_event_projection_ledger_immutable")
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE execution_events SET kind='run.created'
             WHERE run_id=? AND event_id=?",
        )
        .bind(run_id.as_str())
        .bind(second[0].causation_event_id())
        .execute(&repository.pool)
        .await
        .unwrap();
        assert_eq!(
            repository
                .load_terminal_public_event(&run_id)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        assert_eq!(
            repository
                .load_terminal_public_event_authority(&run_id)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        assert_eq!(
            repository
                .load_published_public_event(second[0].public_event_id())
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        sqlx::query(
            "UPDATE execution_events SET kind='run.lifecycle_changed'
             WHERE run_id=? AND event_id=?",
        )
        .bind(run_id.as_str())
        .bind(second[0].causation_event_id())
        .execute(&repository.pool)
        .await
        .unwrap();

        assert_eq!(repository.prune_expired_public_events(10).await.unwrap(), 0);
        for rewrite in [
            "UPDATE public_event_outbox SET safe_envelope='{}' WHERE run_id=? AND public_event_id=?",
            "UPDATE public_event_outbox SET event_kind='run.failed' WHERE run_id=? AND public_event_id=?",
            "UPDATE public_event_outbox SET public_ordinal=40 WHERE run_id=? AND public_event_id=?",
            "UPDATE public_event_outbox SET is_terminal=0 WHERE run_id=? AND public_event_id=?",
            "UPDATE public_event_outbox SET retain_until=CURRENT_TIMESTAMP WHERE run_id=? AND public_event_id=?",
        ] {
            assert!(
                sqlx::query(rewrite)
                    .bind(run_id.as_str())
                    .bind(second[0].public_event_id())
                    .execute(&repository.pool)
                    .await
                    .is_err(),
                "rewrite unexpectedly succeeded: {rewrite}"
            );
        }
        assert!(sqlx::query(
            "DELETE FROM public_event_outbox WHERE run_id = ? AND public_event_id = ?",
        )
        .bind(run_id.as_str())
        .bind(second[0].public_event_id())
        .execute(&repository.pool)
        .await
        .is_err());
        assert!(repository
            .open_public_event_notification_stream()
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn sqlite_prunes_only_published_nonterminal_events_after_database_deadline() {
        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new("run_public_outbox_retention").unwrap();
        let create_key = transition_key("retention.create");
        let create_command =
            CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap();
        let created = repository
            .create_run(create_key.clone(), create_command.clone())
            .await
            .unwrap();
        let created_public_event_id = created
            .committed_result()
            .and_then(|receipt| receipt.public_event_id())
            .expect("run creation has a permanent public event identity")
            .to_owned();
        repository
            .commit_run_transition(
                transition_key("retention.start"),
                model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    0,
                    RunLifecycle::Created,
                    AdmissionState::Open,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    event(&run_id, RunLifecycle::Active),
                    Some(model_adapter::public_event_intent(
                        PublicEventPayload::RunStarted,
                    )),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let claims = repository
            .claim_public_events("retention-dispatcher", 30, 10)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert!(!claims[0].is_terminal());
        assert!(repository
            .publish_public_event(&claims[0], 1)
            .await
            .unwrap());

        let (published_at, retain_until) = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT published_at, retain_until FROM public_event_outbox
                 WHERE run_id = ? AND public_event_id = ?",
        )
        .bind(run_id.as_str())
        .bind(claims[0].public_event_id())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        let retain_until = retain_until.expect("nonterminal publication gets a deadline");
        assert!(retain_until > published_at);
        assert_eq!(repository.prune_expired_public_events(1).await.unwrap(), 0);
        assert!(sqlx::query(
            "DELETE FROM public_event_outbox WHERE run_id = ? AND public_event_id = ?",
        )
        .bind(run_id.as_str())
        .bind(claims[0].public_event_id())
        .execute(&repository.pool)
        .await
        .is_err());

        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        assert_eq!(repository.prune_expired_public_events(1).await.unwrap(), 1);
        assert!(repository
            .load_published_public_event(claims[0].public_event_id())
            .await
            .unwrap()
            .is_none());
        assert_eq!(claims[0].public_event_id(), created_public_event_id);

        let replay = repository
            .create_run(create_key, create_command)
            .await
            .unwrap();
        assert!(matches!(replay, TransitionOutcome::ExactReplay { .. }));
        assert_eq!(
            replay
                .committed_result()
                .and_then(|receipt| receipt.public_event_id()),
            Some(created_public_event_id.as_str()),
            "outbox body retention must not erase exact-replay identity"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT public_event_id FROM public_event_receipts
                 WHERE run_id = ? AND causation_event_id = ?",
            )
            .bind(run_id.as_str())
            .bind(replay.committed_result().unwrap().event_id())
            .fetch_one(&repository.pool)
            .await
            .unwrap(),
            created_public_event_id
        );
        assert!(sqlx::query(
            "UPDATE public_event_receipts SET event_kind='run.started'
             WHERE run_id = ? AND public_event_id = ?",
        )
        .bind(run_id.as_str())
        .bind(&created_public_event_id)
        .execute(&repository.pool)
        .await
        .is_err());
        assert!(sqlx::query(
            "DELETE FROM public_event_receipts WHERE run_id = ? AND public_event_id = ?",
        )
        .bind(run_id.as_str())
        .bind(&created_public_event_id)
        .execute(&repository.pool)
        .await
        .is_err());
    }

    #[tokio::test]
    async fn sqlite_claim_fails_closed_when_causation_event_schema_is_unknown() {
        let repository = SqliteDurableRepository::in_memory().await.unwrap();
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new("run_public_outbox_unknown_event_schema").unwrap();
        repository
            .create_run(
                transition_key("unknown-schema.create"),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap(),
            )
            .await
            .unwrap();

        // Simulate a legacy/corrupt snapshot admitted with database guards
        // disabled. Claiming must still decode the joined execution authority.
        sqlx::query("DROP TRIGGER execution_event_schema_version_update_supported")
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query("DROP TRIGGER execution_event_projection_ledger_immutable")
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE execution_events SET schema_version=999 WHERE run_id=?")
            .bind(run_id.as_str())
            .execute(&repository.pool)
            .await
            .unwrap();

        let error = repository
            .claim_public_events("unknown-schema-dispatcher", 30, 1)
            .await
            .unwrap_err();
        assert_eq!(error.code(), "ENGINE_REPOSITORY_DATA_INVALID");
    }

    #[tokio::test]
    async fn sqlite_claim_fails_closed_for_supported_but_corrupt_causation_authority() {
        for (case, mutation) in [
            (
                "kind",
                "UPDATE execution_events SET kind='run.lifecycle_changed' WHERE run_id=?",
            ),
            (
                "context",
                "UPDATE execution_events SET scope_instance_id='scope_corrupt' WHERE run_id=?",
            ),
            (
                "payload",
                "UPDATE execution_events SET safe_payload='{}' WHERE run_id=?",
            ),
            (
                "seq",
                "UPDATE execution_events SET seq=seq+100 WHERE run_id=?",
            ),
        ] {
            let repository = SqliteDurableRepository::in_memory().await.unwrap();
            let plan = plan();
            repository.install_versioned_plan(&plan).await.unwrap();
            let run_id = RunId::new(format!("run_public_corrupt_{case}")).unwrap();
            repository
                .create_run(
                    transition_key(&format!("corrupt.{case}.create")),
                    CreateRunCommand::new(run_id.clone(), &plan, json!({"input": case})).unwrap(),
                )
                .await
                .unwrap();

            // Model a supported-schema legacy snapshot whose SQL guards were
            // bypassed. The public claim boundary must decode the complete
            // causation row, not merely trust its schema discriminator.
            sqlx::query("DROP TRIGGER execution_event_projection_ledger_immutable")
                .execute(&repository.pool)
                .await
                .unwrap();
            if case == "context" {
                sqlx::query("PRAGMA foreign_keys=OFF")
                    .execute(&repository.pool)
                    .await
                    .unwrap();
            }
            sqlx::query(mutation)
                .bind(run_id.as_str())
                .execute(&repository.pool)
                .await
                .unwrap();

            let error = repository
                .claim_public_events(&format!("corrupt-{case}-dispatcher"), 30, 1)
                .await
                .unwrap_err();
            assert_eq!(
                error.code(),
                "ENGINE_REPOSITORY_DATA_INVALID",
                "supported-schema {case} corruption must fail closed"
            );
        }
    }

    #[tokio::test]
    async fn postgres_publish_and_concurrent_insert_preserve_next_head_when_available() {
        let database_url = std::env::var("PUBLIC_OUTBOX_TEST_POSTGRES_URL")
            .or_else(|_| std::env::var("TEST_POSTGRES_URL"));
        let Ok(database_url) = database_url else {
            return;
        };
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let schema = format!("public_head_race_{}", &suffix[..16]);
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if database_url.contains('?') { '&' } else { '?' };
        let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
        let repository = PostgresDurableRepository::connect_provisioned_for_test(&scoped_url)
            .await
            .unwrap();
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new(format!("run_public_head_race_{suffix}")).unwrap();
        repository
            .create_run(
                transition_key(&format!("{suffix}.race.create")),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap(),
            )
            .await
            .unwrap();
        let first_claim = repository
            .claim_public_events(&format!("race_dispatcher_{suffix}"), 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();

        // This is publish_public_event's first lock. A later monotonic outbox
        // insert must not touch this ready head.
        let mut publishing = repository.pool.begin().await.unwrap();
        let locked_id = sqlx::query_scalar::<_, Option<String>>(
            "SELECT public_event_id FROM public_event_delivery_heads
             WHERE run_id=$1 FOR UPDATE",
        )
        .bind(run_id.as_str())
        .fetch_one(&mut *publishing)
        .await
        .unwrap();
        assert_eq!(locked_id.as_deref(), Some(first_claim.public_event_id()));

        let later_repository = repository.clone();
        let later_run_id = run_id.clone();
        let later_transition = model_adapter::run_transition_nonterminal(
            run_id.clone(),
            0,
            RunLifecycle::Created,
            AdmissionState::Open,
            RunLifecycle::Active,
            AdmissionState::Open,
            event(&run_id, RunLifecycle::Active),
            Some(model_adapter::public_event_intent(
                PublicEventPayload::RunStarted,
            )),
        )
        .unwrap();
        let later_key = transition_key(&format!("{suffix}.race.start"));
        let later = tokio::spawn(async move {
            later_repository
                .commit_run_transition(later_key, later_transition)
                .await
        });
        let later_outcome = tokio::time::timeout(std::time::Duration::from_secs(10), later)
            .await
            .expect("a monotonic outbox insert must not wait for the ready head")
            .unwrap()
            .unwrap();
        let later_public_id = later_outcome
            .committed_result()
            .and_then(|receipt| receipt.public_event_id())
            .unwrap()
            .to_owned();

        let published = sqlx::query_scalar::<_, String>(
            "UPDATE public_event_outbox
             SET publish_state='published',published_at=clock_timestamp(),
                 published_by=$3,published_claim_token=$4,
                 notified_at=clock_timestamp(),claimed_by=NULL,
                 claim_token=NULL,claim_expires_at=NULL,
                 retain_until=clock_timestamp()+INTERVAL '60 seconds'
             WHERE run_id=$1 AND public_event_id=$2 AND publish_state='claimed'
               AND claimed_by=$3 AND claim_token=$4
             RETURNING public_event_id",
        )
        .bind(run_id.as_str())
        .bind(first_claim.public_event_id())
        .bind(first_claim.claimant())
        .bind(first_claim.claim_token())
        .fetch_one(&mut *publishing)
        .await
        .unwrap();
        assert_eq!(published, first_claim.public_event_id());
        publishing.commit().await.unwrap();

        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT public_event_id FROM public_event_delivery_heads
                 WHERE run_id=$1 AND head_state='ready'",
            )
            .bind(later_run_id.as_str())
            .fetch_one(&repository.pool)
            .await
            .unwrap()
            .as_deref(),
            Some(later_public_id.as_str())
        );
        let second_claim = repository
            .claim_public_events(&format!("race_next_dispatcher_{suffix}"), 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(second_claim.public_event_id(), later_public_id);

        repository.pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
    }

    #[tokio::test]
    async fn postgres_public_head_drain_boundary_and_monotonicity_fail_closed_when_available() {
        let database_url = std::env::var("PUBLIC_OUTBOX_TEST_POSTGRES_URL")
            .or_else(|_| std::env::var("TEST_POSTGRES_URL"));
        let Ok(database_url) = database_url else {
            return;
        };
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let schema = format!("public_head_boundary_{}", &suffix[..16]);
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if database_url.contains('?') { '&' } else { '?' };
        let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
        let repository = PostgresDurableRepository::connect_provisioned_for_test(&scoped_url)
            .await
            .unwrap();
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();

        let mut isolation_tx = begin_write_transaction(&repository.pool).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SHOW transaction_isolation")
                .fetch_one(&mut *isolation_tx)
                .await
                .unwrap(),
            "read committed"
        );
        isolation_tx.rollback().await.unwrap();

        // Hold the event-writer authority while a publisher reaches the empty
        // successor boundary. The writer must be able to insert without
        // waiting for the ready head, and the publisher must re-read after the
        // writer commits instead of draining the Run.
        let boundary_run = RunId::new(format!("run_public_head_boundary_{suffix}")).unwrap();
        repository
            .create_run(
                transition_key(&format!("{suffix}.boundary.create")),
                CreateRunCommand::new(boundary_run.clone(), &plan, json!({"input": 1})).unwrap(),
            )
            .await
            .unwrap();
        let first_claim = repository
            .claim_public_events(&format!("boundary_dispatcher_{suffix}"), 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let mut writer = begin_write_transaction(&repository.pool).await.unwrap();
        sqlx::query("SELECT 1 FROM workflow_runs WHERE run_id=$1 FOR UPDATE")
            .bind(boundary_run.as_str())
            .fetch_one(&mut *writer)
            .await
            .unwrap();

        let publishing_pool = repository.pool.clone();
        let publishing_run = boundary_run.clone();
        let (pid_sender, pid_receiver) = tokio::sync::oneshot::channel();
        let publishing = tokio::spawn(async move {
            let mut transaction = begin_write_transaction(&publishing_pool).await.unwrap();
            let backend_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
                .fetch_one(&mut *transaction)
                .await
                .unwrap();
            sqlx::query(
                "SELECT public_event_id FROM public_event_delivery_heads
                 WHERE run_id=$1 FOR UPDATE",
            )
            .bind(publishing_run.as_str())
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
            pid_sender.send(backend_pid).unwrap();
            sqlx::query(
                "UPDATE public_event_outbox
                 SET publish_state='published',published_at=clock_timestamp(),
                     published_by=$3,published_claim_token=$4,
                     notified_at=clock_timestamp(),claimed_by=NULL,
                     claim_token=NULL,claim_expires_at=NULL,
                     retain_until=clock_timestamp()+INTERVAL '60 seconds'
                 WHERE run_id=$1 AND public_event_id=$2 AND publish_state='claimed'
                   AND claimed_by=$3 AND claim_token=$4",
            )
            .bind(publishing_run.as_str())
            .bind(first_claim.public_event_id())
            .bind(first_claim.claimant())
            .bind(first_claim.claim_token())
            .execute(&mut *transaction)
            .await
            .unwrap();
            transaction.commit().await.unwrap();
        });
        let publishing_pid = pid_receiver.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let waiting = sqlx::query_scalar::<_, bool>(
                    "SELECT COALESCE(wait_event_type='Lock',FALSE)
                     FROM pg_stat_activity WHERE pid=$1",
                )
                .bind(publishing_pid)
                .fetch_optional(&repository.pool)
                .await
                .unwrap()
                .unwrap_or(false);
                if waiting {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("publisher must wait for the workflow_runs boundary lock");

        let successor_id = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            insert_synthetic_run_started(
                &mut writer,
                &boundary_run,
                &format!("{suffix}.boundary.successor"),
            ),
        )
        .await
        .expect("monotonic insertion must not wait for the publisher's ready-head lock");
        writer.commit().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), publishing)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT public_event_id FROM public_event_delivery_heads
                 WHERE run_id=$1 AND head_state='ready'",
            )
            .bind(boundary_run.as_str())
            .fetch_one(&repository.pool)
            .await
            .unwrap()
            .as_deref(),
            Some(successor_id.as_str())
        );

        // Build seq=2 as a deliberately private event, then install a public
        // seq=3 head. A late attempt to bind seq=2 must roll back loudly.
        let regression_run = RunId::new(format!("run_public_head_regression_{suffix}")).unwrap();
        repository
            .create_run(
                transition_key(&format!("{suffix}.regression.create")),
                CreateRunCommand::new(regression_run.clone(), &plan, json!({"input": 1})).unwrap(),
            )
            .await
            .unwrap();
        let private_key = transition_key(&format!("{suffix}.regression.private"));
        repository
            .commit_run_transition(
                private_key.clone(),
                model_adapter::run_transition_nonterminal(
                    regression_run.clone(),
                    0,
                    RunLifecycle::Created,
                    AdmissionState::Open,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    event(&regression_run, RunLifecycle::Active),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let mut later_writer = begin_write_transaction(&repository.pool).await.unwrap();
        let later_id = insert_synthetic_run_started(
            &mut later_writer,
            &regression_run,
            &format!("{suffix}.regression.later"),
        )
        .await;
        later_writer.commit().await.unwrap();
        let regression_created_id = sqlx::query_scalar::<_, String>(
            "SELECT public_event_id FROM public_event_delivery_heads
             WHERE run_id=$1 AND head_state='ready'",
        )
        .bind(regression_run.as_str())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        let regression_claimant = format!("regression_dispatcher_{suffix}");
        let regression_token = format!("regression_token_{suffix}");
        sqlx::query(
            "UPDATE public_event_outbox
             SET publish_state='claimed',claimed_by=$3,claim_token=$4,
                 claim_expires_at=clock_timestamp()+INTERVAL '30 seconds',
                 publish_attempts=publish_attempts+1
             WHERE run_id=$1 AND public_event_id=$2 AND publish_state='pending'",
        )
        .bind(regression_run.as_str())
        .bind(&regression_created_id)
        .bind(&regression_claimant)
        .bind(&regression_token)
        .execute(&repository.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE public_event_outbox
             SET publish_state='published',published_at=clock_timestamp(),
                 published_by=$3,published_claim_token=$4,
                 notified_at=clock_timestamp(),claimed_by=NULL,
                 claim_token=NULL,claim_expires_at=NULL,
                 retain_until=clock_timestamp()+INTERVAL '60 seconds'
             WHERE run_id=$1 AND public_event_id=$2 AND publish_state='claimed'
               AND claimed_by=$3 AND claim_token=$4",
        )
        .bind(regression_run.as_str())
        .bind(&regression_created_id)
        .bind(&regression_claimant)
        .bind(&regression_token)
        .execute(&repository.pool)
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT public_event_id FROM public_event_delivery_heads
                 WHERE run_id=$1 AND head_state='ready'",
            )
            .bind(regression_run.as_str())
            .fetch_one(&repository.pool)
            .await
            .unwrap()
            .as_deref(),
            Some(later_id.as_str())
        );

        let private_event_id = event_id(&private_key);
        let (private_seq, private_occurred_at) = sqlx::query_as::<_, (i64, DateTime<Utc>)>(
            "SELECT seq,occurred_at FROM execution_events
                 WHERE run_id=$1 AND event_id=$2",
        )
        .bind(regression_run.as_str())
        .bind(&private_event_id)
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        let regressed_payload = PublicEventPayload::RunStarted;
        let regressed_kind = regressed_payload.kind();
        let regressed_public_id = public_event_id(&regression_run, &private_key, regressed_kind);
        let regressed_envelope = serde_json::to_value(
            durable_public_event_envelope(
                &regression_run,
                &regressed_public_id,
                &private_event_id,
                u64::try_from(private_seq).unwrap(),
                private_occurred_at,
                regressed_payload,
            )
            .unwrap(),
        )
        .unwrap();

        let mut regression_tx = begin_write_transaction(&repository.pool).await.unwrap();
        let regression_error = insert_test_public_outbox(
            &mut regression_tx,
            &regression_run,
            &regressed_public_id,
            &private_event_id,
            regressed_kind,
            &regressed_envelope,
        )
        .await
        .unwrap_err();
        assert_eq!(
            regression_error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .as_deref(),
            Some("23514")
        );
        assert!(regression_error
            .to_string()
            .contains("public event key regressed"));
        regression_tx.rollback().await.unwrap();

        let mut repeatable_read = repository.pool.begin().await.unwrap();
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *repeatable_read)
            .await
            .unwrap();
        let isolation_error = insert_test_public_outbox(
            &mut repeatable_read,
            &regression_run,
            &regressed_public_id,
            &private_event_id,
            regressed_kind,
            &regressed_envelope,
        )
        .await
        .unwrap_err();
        assert!(isolation_error
            .to_string()
            .contains("public outbox writes require READ COMMITTED"));
        repeatable_read.rollback().await.unwrap();

        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT public_event_id FROM public_event_delivery_heads
                 WHERE run_id=$1 AND head_state='ready'",
            )
            .bind(regression_run.as_str())
            .fetch_one(&repository.pool)
            .await
            .unwrap()
            .as_deref(),
            Some(later_id.as_str())
        );

        repository.pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
    }

    #[tokio::test]
    async fn postgres_terminal_publish_locks_run_before_delivery_head_when_available() {
        let database_url = std::env::var("PUBLIC_OUTBOX_TEST_POSTGRES_URL")
            .or_else(|_| std::env::var("TEST_POSTGRES_URL"));
        let Ok(database_url) = database_url else {
            return;
        };
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let schema = format!("public_terminal_order_{}", &suffix[..12]);
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if database_url.contains('?') { '&' } else { '?' };
        let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
        let repository = PostgresDurableRepository::connect_provisioned_for_test(&scoped_url)
            .await
            .unwrap();
        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new(format!("run_terminal_publish_order_{suffix}")).unwrap();
        repository
            .create_run(
                transition_key(&format!("{suffix}.create")),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key(&format!("{suffix}.start")),
                model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    0,
                    RunLifecycle::Created,
                    AdmissionState::Open,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    event(&run_id, RunLifecycle::Active),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key(&format!("{suffix}.completing")),
                model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    1,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    RunLifecycle::Completing,
                    AdmissionState::Draining,
                    event(&run_id, RunLifecycle::Completing),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key(&format!("{suffix}.terminal")),
                model_adapter::run_transition_terminal_success(
                    run_id.clone(),
                    2,
                    json!({"answer": 42}),
                    event(&run_id, RunLifecycle::Succeeded),
                    model_adapter::public_event_intent(PublicEventPayload::RunCompleted),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let created = repository
            .claim_public_events(&format!("terminal_order_dispatcher_{suffix}"), 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(!created.is_terminal());
        assert!(repository.publish_public_event(&created, 60).await.unwrap());
        let terminal = repository
            .claim_public_events(&format!("terminal_order_dispatcher_{suffix}"), 30, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(terminal.is_terminal());

        let mut transition = begin_write_transaction(&repository.pool).await.unwrap();
        sqlx::query("SELECT 1 FROM workflow_runs WHERE run_id=$1 FOR UPDATE")
            .bind(run_id.as_str())
            .fetch_one(&mut *transition)
            .await
            .unwrap();
        let publishing_repository = repository.clone();
        let publishing = tokio::spawn(async move {
            publishing_repository
                .publish_public_event(&terminal, 60)
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let waits_before_head = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(
                       SELECT 1
                       FROM pg_stat_activity
                       WHERE datname=current_database()
                         AND state='active'
                         AND wait_event_type='Lock'
                         AND query LIKE 'SELECT 1 FROM workflow_runs WHERE run_id=$1 FOR UPDATE%'
                     )",
                )
                .fetch_one(&repository.pool)
                .await
                .unwrap();
                if waits_before_head {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal publisher must wait on workflow_runs before the delivery head");

        let mut head_probe = begin_write_transaction(&repository.pool).await.unwrap();
        sqlx::query(
            "SELECT 1 FROM public_event_delivery_heads
             WHERE run_id=$1 FOR UPDATE NOWAIT",
        )
        .bind(run_id.as_str())
        .fetch_one(&mut *head_probe)
        .await
        .expect("blocked terminal publisher must not own the delivery head");
        head_probe.rollback().await.unwrap();

        transition.commit().await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(10), publishing)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT head_state FROM public_event_delivery_heads WHERE run_id=$1",
            )
            .bind(run_id.as_str())
            .fetch_one(&repository.pool)
            .await
            .unwrap(),
            "drained"
        );

        repository.pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
    }

    #[tokio::test]
    async fn postgres_publish_commits_state_before_notifying_durable_id_when_available() {
        let database_url = std::env::var("PUBLIC_OUTBOX_TEST_POSTGRES_URL")
            .or_else(|_| std::env::var("TEST_POSTGRES_URL"));
        let Ok(database_url) = database_url else {
            return;
        };
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let schema = format!("public_outbox_{}", &suffix[..16]);
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if database_url.contains('?') { '&' } else { '?' };
        let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
        let repository = PostgresDurableRepository::connect_provisioned_for_test(&scoped_url)
            .await
            .unwrap();

        let mut listener = repository
            .open_public_event_notification_stream()
            .await
            .unwrap()
            .expect("PostgreSQL exposes LISTEN/NOTIFY wake-up hints");

        let plan = plan();
        repository.install_versioned_plan(&plan).await.unwrap();
        let run_id = RunId::new(format!("run_public_outbox_{suffix}")).unwrap();
        repository
            .create_run(
                transition_key(&format!("{suffix}.create")),
                CreateRunCommand::new(run_id.clone(), &plan, json!({"input": 1})).unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key(&format!("{suffix}.start")),
                model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    0,
                    RunLifecycle::Created,
                    AdmissionState::Open,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    event(&run_id, RunLifecycle::Active),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key(&format!("{suffix}.completing")),
                model_adapter::run_transition_nonterminal(
                    run_id.clone(),
                    1,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    RunLifecycle::Completing,
                    AdmissionState::Draining,
                    event(&run_id, RunLifecycle::Completing),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key(&format!("{suffix}.terminal")),
                model_adapter::run_transition_terminal_success(
                    run_id.clone(),
                    2,
                    json!({"answer": 42}),
                    event(&run_id, RunLifecycle::Succeeded),
                    model_adapter::public_event_intent(PublicEventPayload::RunCompleted),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let created = repository
            .claim_public_events(&format!("dispatcher_a_{suffix}"), 30, 10)
            .await
            .unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(
            created[0].safe_envelope().payload(),
            &PublicEventPayload::RunCreated
        );
        assert!(repository
            .publish_public_event(&created[0], 60)
            .await
            .unwrap());
        let created_notification =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
                .await
                .expect("run.created publication must wake the listener")
                .unwrap();
        assert_eq!(created_notification, created[0].public_event_id());

        let first = repository
            .claim_public_events(&format!("dispatcher_a_{suffix}"), 30, 10)
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        sqlx::query(
            "UPDATE public_event_outbox
             SET claim_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
             WHERE run_id = $1 AND public_event_id = $2",
        )
        .bind(run_id.as_str())
        .bind(first[0].public_event_id())
        .execute(&repository.pool)
        .await
        .unwrap();
        let winner = repository
            .claim_public_events(&format!("dispatcher_b_{suffix}"), 30, 10)
            .await
            .unwrap();
        assert_eq!(winner.len(), 1);
        assert_ne!(first[0].claim_token(), winner[0].claim_token());
        assert!(!repository
            .publish_public_event(&first[0], 60)
            .await
            .unwrap());
        assert!(repository
            .publish_public_event(&winner[0], 60)
            .await
            .unwrap());

        let notification = tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
            .await
            .expect("committed publication must wake the listener")
            .unwrap();
        assert_eq!(notification, winner[0].public_event_id());

        let durable = repository
            .load_published_public_event(&notification)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable.run_id(), &run_id);
        assert_eq!(durable.safe_envelope(), winner[0].safe_envelope());
        assert!(!repository
            .publish_public_event(&first[0], 60)
            .await
            .unwrap());
        assert!(repository
            .publish_public_event(&winner[0], 60)
            .await
            .unwrap());
        assert_eq!(
            repository
                .load_terminal_public_event(&run_id)
                .await
                .unwrap()
                .unwrap(),
            durable.safe_envelope().clone()
        );

        let terminal_retain_until = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT retain_until FROM public_event_outbox
             WHERE run_id = $1 AND public_event_id = $2",
        )
        .bind(run_id.as_str())
        .bind(winner[0].public_event_id())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert!(terminal_retain_until.is_none());
        assert!(sqlx::query(
            "DELETE FROM public_event_outbox WHERE run_id = $1 AND public_event_id = $2",
        )
        .bind(run_id.as_str())
        .bind(winner[0].public_event_id())
        .execute(&repository.pool)
        .await
        .is_err());
        for rewrite in [
            "UPDATE public_event_outbox SET safe_envelope='{}'::jsonb WHERE run_id=$1 AND public_event_id=$2",
            "UPDATE public_event_outbox SET event_kind='run.failed' WHERE run_id=$1 AND public_event_id=$2",
            "UPDATE public_event_outbox SET public_ordinal=40 WHERE run_id=$1 AND public_event_id=$2",
            "UPDATE public_event_outbox SET is_terminal=false WHERE run_id=$1 AND public_event_id=$2",
            "UPDATE public_event_outbox SET retain_until=clock_timestamp() WHERE run_id=$1 AND public_event_id=$2",
        ] {
            assert!(
                sqlx::query(rewrite)
                    .bind(run_id.as_str())
                    .bind(winner[0].public_event_id())
                    .execute(&repository.pool)
                    .await
                    .is_err(),
                "rewrite unexpectedly succeeded: {rewrite}"
            );
        }

        let retention_run_id = RunId::new(format!("run_public_retention_{suffix}")).unwrap();
        let retention_create_key = transition_key(&format!("{suffix}.retention.create"));
        let retention_create_command =
            CreateRunCommand::new(retention_run_id.clone(), &plan, json!({"input": 2})).unwrap();
        let retention_created = repository
            .create_run(
                retention_create_key.clone(),
                retention_create_command.clone(),
            )
            .await
            .unwrap();
        let retention_created_public_event_id = retention_created
            .committed_result()
            .and_then(|receipt| receipt.public_event_id())
            .expect("run creation has a permanent public event identity")
            .to_owned();
        repository
            .commit_run_transition(
                transition_key(&format!("{suffix}.retention.start")),
                model_adapter::run_transition_nonterminal(
                    retention_run_id.clone(),
                    0,
                    RunLifecycle::Created,
                    AdmissionState::Open,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    event(&retention_run_id, RunLifecycle::Active),
                    Some(model_adapter::public_event_intent(
                        PublicEventPayload::RunStarted,
                    )),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let retention_event_id = sqlx::query_scalar::<_, String>(
            "SELECT public_event_id FROM public_event_outbox
             WHERE run_id = $1 AND event_kind = 'run.created'",
        )
        .bind(retention_run_id.as_str())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert_eq!(retention_event_id, retention_created_public_event_id);

        assert!(sqlx::query(
            "DELETE FROM public_event_outbox WHERE run_id = $1 AND public_event_id = $2",
        )
        .bind(retention_run_id.as_str())
        .bind(&retention_event_id)
        .execute(&repository.pool)
        .await
        .is_err());

        let retention_claimant = format!("retention_dispatcher_{suffix}");
        let retention_token = format!("retention_claim_{suffix}");
        let (causation_event_id, event_kind, is_terminal, claim_expires_at, safe_envelope) =
            sqlx::query_as::<_, (String, String, bool, DateTime<Utc>, Value)>(
                "UPDATE public_event_outbox
                 SET publish_state = 'claimed', claimed_by = $3, claim_token = $4,
                     claim_expires_at = clock_timestamp() + INTERVAL '30 seconds',
                     publish_attempts = publish_attempts + 1
                 WHERE run_id = $1 AND public_event_id = $2 AND publish_state = 'pending'
                 RETURNING causation_event_id, event_kind, is_terminal,
                           claim_expires_at, safe_envelope",
            )
            .bind(retention_run_id.as_str())
            .bind(&retention_event_id)
            .bind(&retention_claimant)
            .bind(&retention_token)
            .fetch_one(&repository.pool)
            .await
            .unwrap();
        let retention_claim = public_outbox_contract_adapter::public_event_claim(
            retention_run_id.clone(),
            retention_event_id.clone(),
            causation_event_id,
            event_kind,
            is_terminal,
            retention_claimant,
            retention_token,
            claim_expires_at,
            safe_envelope,
        )
        .unwrap();
        assert!(repository
            .publish_public_event(&retention_claim, 1)
            .await
            .unwrap());
        let retention_notification =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv())
                .await
                .expect("nonterminal publication must also wake the listener")
                .unwrap();
        assert_eq!(retention_notification, retention_event_id);

        let (published_at, retain_until) =
            sqlx::query_as::<_, (DateTime<Utc>, Option<DateTime<Utc>>)>(
                "SELECT published_at, retain_until FROM public_event_outbox
             WHERE run_id = $1 AND public_event_id = $2",
            )
            .bind(retention_run_id.as_str())
            .bind(&retention_event_id)
            .fetch_one(&repository.pool)
            .await
            .unwrap();
        assert!(retain_until.is_some_and(|deadline| deadline > published_at));
        repository.prune_expired_public_events(1_000).await.unwrap();
        assert!(repository
            .load_published_public_event(&retention_event_id)
            .await
            .unwrap()
            .is_some());
        assert!(sqlx::query(
            "DELETE FROM public_event_outbox WHERE run_id = $1 AND public_event_id = $2",
        )
        .bind(retention_run_id.as_str())
        .bind(&retention_event_id)
        .execute(&repository.pool)
        .await
        .is_err());

        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        assert!(repository.prune_expired_public_events(1_000).await.unwrap() >= 1);
        assert!(repository
            .load_published_public_event(&retention_event_id)
            .await
            .unwrap()
            .is_none());
        let retention_replay = repository
            .create_run(retention_create_key, retention_create_command)
            .await
            .unwrap();
        assert!(matches!(
            retention_replay,
            TransitionOutcome::ExactReplay { .. }
        ));
        assert_eq!(
            retention_replay
                .committed_result()
                .and_then(|receipt| receipt.public_event_id()),
            Some(retention_created_public_event_id.as_str()),
            "PostgreSQL outbox retention must not erase exact-replay identity"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT public_event_id FROM public_event_receipts
                 WHERE run_id = $1 AND causation_event_id = $2",
            )
            .bind(retention_run_id.as_str())
            .bind(retention_replay.committed_result().unwrap().event_id())
            .fetch_one(&repository.pool)
            .await
            .unwrap(),
            retention_created_public_event_id
        );
        assert!(sqlx::query(
            "UPDATE public_event_receipts SET event_kind='run.started'
             WHERE run_id = $1 AND public_event_id = $2",
        )
        .bind(retention_run_id.as_str())
        .bind(&retention_event_id)
        .execute(&repository.pool)
        .await
        .is_err());
        assert!(sqlx::query(
            "DELETE FROM public_event_receipts WHERE run_id = $1 AND public_event_id = $2",
        )
        .bind(retention_run_id.as_str())
        .bind(&retention_event_id)
        .execute(&repository.pool)
        .await
        .is_err());

        let corruption_run_id = RunId::new(format!("run_public_corruption_{suffix}")).unwrap();
        let corruption_created = repository
            .create_run(
                transition_key(&format!("{suffix}.corruption.create")),
                CreateRunCommand::new(
                    corruption_run_id.clone(),
                    &plan,
                    json!({"input": "corruption"}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let corruption_event_id = corruption_created
            .committed_result()
            .unwrap()
            .event_id()
            .to_owned();
        let (original_seq, original_payload) = sqlx::query_as::<_, (i64, Value)>(
            "SELECT seq,safe_payload FROM execution_events
             WHERE run_id=$1 AND event_id=$2",
        )
        .bind(corruption_run_id.as_str())
        .bind(&corruption_event_id)
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        sqlx::query("DROP TRIGGER execution_event_projection_ledger_immutable ON execution_events")
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query(
            "ALTER TABLE execution_events
             DROP CONSTRAINT execution_events_run_id_scope_instance_id_fkey",
        )
        .execute(&repository.pool)
        .await
        .unwrap();

        sqlx::query(
            "UPDATE execution_events SET kind='run.lifecycle_changed'
             WHERE run_id=$1 AND event_id=$2",
        )
        .bind(corruption_run_id.as_str())
        .bind(&corruption_event_id)
        .execute(&repository.pool)
        .await
        .unwrap();
        assert_eq!(
            repository
                .claim_public_events(&format!("corrupt_kind_{suffix}"), 30, 1_000)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        sqlx::query(
            "UPDATE execution_events SET kind='run.created'
             WHERE run_id=$1 AND event_id=$2",
        )
        .bind(corruption_run_id.as_str())
        .bind(&corruption_event_id)
        .execute(&repository.pool)
        .await
        .unwrap();

        sqlx::query(
            "UPDATE execution_events SET scope_instance_id='scope_corrupt'
             WHERE run_id=$1 AND event_id=$2",
        )
        .bind(corruption_run_id.as_str())
        .bind(&corruption_event_id)
        .execute(&repository.pool)
        .await
        .unwrap();
        assert_eq!(
            repository
                .claim_public_events(&format!("corrupt_context_{suffix}"), 30, 1_000)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        sqlx::query(
            "UPDATE execution_events SET scope_instance_id=NULL
             WHERE run_id=$1 AND event_id=$2",
        )
        .bind(corruption_run_id.as_str())
        .bind(&corruption_event_id)
        .execute(&repository.pool)
        .await
        .unwrap();

        sqlx::query(
            "UPDATE execution_events SET safe_payload='{}'::jsonb
             WHERE run_id=$1 AND event_id=$2",
        )
        .bind(corruption_run_id.as_str())
        .bind(&corruption_event_id)
        .execute(&repository.pool)
        .await
        .unwrap();
        assert_eq!(
            repository
                .claim_public_events(&format!("corrupt_payload_{suffix}"), 30, 1_000)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        sqlx::query(
            "UPDATE execution_events SET safe_payload=$3
             WHERE run_id=$1 AND event_id=$2",
        )
        .bind(corruption_run_id.as_str())
        .bind(&corruption_event_id)
        .bind(&original_payload)
        .execute(&repository.pool)
        .await
        .unwrap();

        sqlx::query(
            "UPDATE execution_events SET seq=seq+100
             WHERE run_id=$1 AND event_id=$2",
        )
        .bind(corruption_run_id.as_str())
        .bind(&corruption_event_id)
        .execute(&repository.pool)
        .await
        .unwrap();
        assert_eq!(
            repository
                .claim_public_events(&format!("corrupt_seq_{suffix}"), 30, 1_000)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        sqlx::query(
            "UPDATE execution_events SET seq=$3
             WHERE run_id=$1 AND event_id=$2",
        )
        .bind(corruption_run_id.as_str())
        .bind(&corruption_event_id)
        .bind(original_seq)
        .execute(&repository.pool)
        .await
        .unwrap();

        let bounded_run_id = RunId::new(format!("run_public_bounded_{suffix}")).unwrap();
        repository
            .create_run(
                transition_key(&format!("{suffix}.bounded.create")),
                CreateRunCommand::new(bounded_run_id.clone(), &plan, json!({"input": 3})).unwrap(),
            )
            .await
            .unwrap();
        repository
            .commit_run_transition(
                transition_key(&format!("{suffix}.bounded.start")),
                model_adapter::run_transition_nonterminal(
                    bounded_run_id.clone(),
                    0,
                    RunLifecycle::Created,
                    AdmissionState::Open,
                    RunLifecycle::Active,
                    AdmissionState::Open,
                    event(&bounded_run_id, RunLifecycle::Active),
                    Some(model_adapter::public_event_intent(
                        PublicEventPayload::RunStarted,
                    )),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        sqlx::query("UPDATE execution_events SET kind='run.created' WHERE run_id=$1 AND seq=2")
            .bind(bounded_run_id.as_str())
            .execute(&repository.pool)
            .await
            .unwrap();
        let bounded_claims = repository
            .claim_public_events(&format!("bounded_head_{suffix}"), 30, 1_000)
            .await
            .unwrap();
        let bounded_head = bounded_claims
            .iter()
            .find(|claim| claim.run_id() == &bounded_run_id)
            .unwrap();
        assert_eq!(
            bounded_head.safe_envelope().payload(),
            &PublicEventPayload::RunCreated
        );
        assert!(repository
            .publish_public_event(bounded_head, 60)
            .await
            .unwrap());
        assert_eq!(
            repository
                .claim_public_events(&format!("bounded_corrupt_{suffix}"), 30, 1_000)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        sqlx::query(
            "UPDATE execution_events SET kind='run.lifecycle_changed'
             WHERE run_id=$1 AND seq=2",
        )
        .bind(bounded_run_id.as_str())
        .execute(&repository.pool)
        .await
        .unwrap();

        let missing_receipt_run_id =
            RunId::new(format!("run_public_missing_receipt_{suffix}")).unwrap();
        let missing_receipt_key = transition_key(&format!("{suffix}.missing.receipt"));
        let missing_receipt_command = CreateRunCommand::new(
            missing_receipt_run_id.clone(),
            &plan,
            json!({"input": "missing-receipt"}),
        )
        .unwrap();
        repository
            .create_run(missing_receipt_key.clone(), missing_receipt_command.clone())
            .await
            .unwrap();
        let missing_receipt_claims = repository
            .claim_public_events(&format!("missing_receipt_{suffix}"), 30, 1_000)
            .await
            .unwrap();
        let missing_receipt_claim = missing_receipt_claims
            .iter()
            .find(|claim| claim.run_id() == &missing_receipt_run_id)
            .unwrap();
        sqlx::query("DROP TRIGGER public_event_receipt_delete_forbidden ON public_event_receipts")
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM public_event_receipts WHERE run_id=$1 AND public_event_id=$2")
            .bind(missing_receipt_run_id.as_str())
            .bind(missing_receipt_claim.public_event_id())
            .execute(&repository.pool)
            .await
            .unwrap();
        assert_eq!(
            repository
                .publish_public_event(missing_receipt_claim, 60)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        assert_eq!(
            repository
                .load_next_public_event(&missing_receipt_run_id, None)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        assert_eq!(
            repository
                .create_run(missing_receipt_key, missing_receipt_command)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );

        let missing_decision_run_id =
            RunId::new(format!("run_public_missing_decision_{suffix}")).unwrap();
        let missing_decision_key = transition_key(&format!("{suffix}.missing.decision"));
        let missing_decision_command = CreateRunCommand::new(
            missing_decision_run_id.clone(),
            &plan,
            json!({"input": "missing-decision"}),
        )
        .unwrap();
        repository
            .create_run(
                missing_decision_key.clone(),
                missing_decision_command.clone(),
            )
            .await
            .unwrap();
        sqlx::query(
            "DROP TRIGGER public_event_projection_decision_mutation_guard
             ON public_event_projection_decisions",
        )
        .execute(&repository.pool)
        .await
        .unwrap();
        sqlx::query(
            "ALTER TABLE public_event_delivery_heads
             DROP CONSTRAINT public_event_delivery_heads_run_id_execution_event_id_fkey",
        )
        .execute(&repository.pool)
        .await
        .unwrap();
        sqlx::query("DELETE FROM public_event_projection_decisions WHERE run_id=$1")
            .bind(missing_decision_run_id.as_str())
            .execute(&repository.pool)
            .await
            .unwrap();
        assert_eq!(
            repository
                .claim_public_events(&format!("missing_decision_{suffix}"), 30, 1_000)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        assert_eq!(
            repository
                .load_next_public_event(&missing_decision_run_id, None)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );
        assert_eq!(
            repository
                .create_run(missing_decision_key, missing_decision_command)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_DATA_INVALID"
        );

        assert!(repository
            .load_terminal_public_event(&run_id)
            .await
            .unwrap()
            .is_some());
        drop(listener);
        repository.pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
    }
}
