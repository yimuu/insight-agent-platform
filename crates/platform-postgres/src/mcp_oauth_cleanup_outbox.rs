//! Durable PostgreSQL delivery authority for MCP OAuth PKCE cleanup.
//!
//! Cleanup is a pre-publication stage on the existing shared outbox row. A successful exact
//! Secret Manager delete advances the row to `cleanup_completed`; it deliberately does not set
//! `published_at`, so the Phase 5 committed-event dispatcher can still project the same Event.

use async_trait::async_trait;
use insight_platform_contracts::{
    ResourceId, ResourceKind, Sha256Digest, TraceId, TraceIdentityV1,
};
use insight_platform_mcp_host::{
    ClaimDueMcpOAuthPkceCleanups, ClaimedMcpOAuthPkceCleanup, McpOAuthPkceCleanupCause,
    McpOAuthPkceCleanupDeliveryError, McpOAuthPkceCleanupHint, McpOAuthPkceCleanupOutbox,
    McpOAuthPkceCleanupRequest, McpOAuthPkceCleanupSettlement,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use sqlx::Row;

use crate::repository::PgRepository;

const MAXIMUM_CLEANUP_BATCH: u16 = 64;
const MAXIMUM_CLEANUP_LEASE_MILLISECONDS: u64 = 120_000;
const CLEANUP_EVENT_INVALID: &str = "mcp_oauth_pkce_cleanup_event_invalid";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallbackCleanupEventPayload {
    schema_version: u32,
    authorization_binding_id: ResourceId,
    callback_ingress_generation_id: ResourceId,
    pkce_cleanup: McpOAuthPkceCleanupHint,
    state: String,
    task_id: ResourceId,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpiredCleanupEventPayload {
    schema_version: u32,
    authorization_binding_id: ResourceId,
    pkce_cleanup: McpOAuthPkceCleanupHint,
    scheduler_generation_id: ResourceId,
    task_id: ResourceId,
}

#[async_trait]
impl McpOAuthPkceCleanupOutbox for PgRepository {
    async fn claim_due_mcp_oauth_pkce_cleanups(
        &self,
        command: ClaimDueMcpOAuthPkceCleanups,
    ) -> Result<Vec<ClaimedMcpOAuthPkceCleanup>, McpOAuthPkceCleanupDeliveryError> {
        command.validate(MAXIMUM_CLEANUP_BATCH, MAXIMUM_CLEANUP_LEASE_MILLISECONDS)?;
        let maximum_claims = i64::from(command.maximum_claims);
        let lease_milliseconds = i64::try_from(command.lease_milliseconds)
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::InvalidCommand)?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::Unavailable)?;
        let rows = sqlx::query(
            r#"
            WITH candidates AS (
                SELECT outbox.tenant_id, outbox.outbox_id, outbox.event_id
                FROM insight_platform.outbox_events AS outbox
                JOIN insight_platform.events AS event
                  ON event.tenant_id = outbox.tenant_id
                 AND event.event_id = outbox.event_id
                WHERE outbox.published_at IS NULL
                  AND outbox.next_publish_at <= clock_timestamp()
                  AND event.event_type IN (
                      'mcp.oauth_authorization_completed',
                      'mcp.oauth_authorization_declined',
                      'mcp.oauth_authorization_expired'
                  )
                  AND (
                      (outbox.state IN ('pending', 'cleanup_retry')
                       AND outbox.claim_owner IS NULL
                       AND outbox.claim_expires_at IS NULL)
                      OR
                      (outbox.state = 'cleanup_claimed'
                       AND outbox.claim_expires_at <= clock_timestamp())
                  )
                ORDER BY outbox.next_publish_at, outbox.tenant_id, outbox.outbox_id
                FOR UPDATE OF outbox SKIP LOCKED
                LIMIT $2
            ), claimed AS (
                UPDATE insight_platform.outbox_events AS outbox
                SET state = 'cleanup_claimed',
                    claim_owner = $1,
                    claim_epoch = outbox.claim_epoch + 1,
                    claim_expires_at = clock_timestamp()
                        + ($3::bigint * interval '1 millisecond'),
                    updated_at = clock_timestamp()
                FROM candidates
                WHERE outbox.tenant_id = candidates.tenant_id
                  AND outbox.outbox_id = candidates.outbox_id
                RETURNING outbox.tenant_id, outbox.outbox_id, outbox.event_id,
                          outbox.claim_epoch, outbox.publish_attempts
            )
            SELECT claimed.tenant_id, claimed.outbox_id, claimed.event_id,
                   claimed.claim_epoch, claimed.publish_attempts,
                   event.aggregate_kind, event.aggregate_id, event.event_type,
                   event.visibility, event.payload_schema_version, event.payload,
                   event.payload_digest, event.trace_id
            FROM claimed
            JOIN insight_platform.events AS event
              ON event.tenant_id = claimed.tenant_id
             AND event.event_id = claimed.event_id
            ORDER BY claimed.tenant_id, claimed.outbox_id
            "#,
        )
        .bind(command.claim_owner.to_string())
        .bind(maximum_claims)
        .bind(lease_milliseconds)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::Unavailable)?;

        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let tenant_id = row
                .try_get::<String, _>("tenant_id")
                .ok()
                .and_then(|value| value.parse::<ResourceId>().ok());
            let outbox_id = row
                .try_get::<String, _>("outbox_id")
                .ok()
                .and_then(|value| value.parse::<ResourceId>().ok());
            let event_id = row
                .try_get::<String, _>("event_id")
                .ok()
                .and_then(|value| value.parse::<ResourceId>().ok());
            let claim_epoch = row
                .try_get::<i64, _>("claim_epoch")
                .ok()
                .and_then(|value| u64::try_from(value).ok());
            let publish_attempts = row
                .try_get::<i32, _>("publish_attempts")
                .ok()
                .and_then(|value| u32::try_from(value).ok());
            let parsed = match (
                tenant_id,
                outbox_id,
                event_id,
                claim_epoch,
                publish_attempts,
            ) {
                (
                    Some(tenant_id),
                    Some(outbox_id),
                    Some(event_id),
                    Some(claim_epoch),
                    Some(publish_attempts),
                ) => parse_claimed_event(
                    &row,
                    tenant_id,
                    outbox_id,
                    event_id,
                    command.claim_owner.clone(),
                    claim_epoch,
                    publish_attempts,
                ),
                _ => Err(McpOAuthPkceCleanupDeliveryError::CorruptEvent),
            };
            match parsed {
                Ok(claim) => claims.push(claim),
                Err(_) => {
                    let tenant = row
                        .try_get::<String, _>("tenant_id")
                        .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
                    let outbox = row
                        .try_get::<String, _>("outbox_id")
                        .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
                    sqlx::query(
                        r#"
                        UPDATE insight_platform.outbox_events
                        SET state = 'cleanup_dead',
                            publish_attempts = CASE
                                WHEN publish_attempts < 2147483647
                                THEN publish_attempts + 1
                                ELSE publish_attempts
                            END,
                            claim_owner = NULL,
                            claim_expires_at = NULL,
                            last_failure_code = $3,
                            updated_at = clock_timestamp()
                        WHERE tenant_id = $1 AND outbox_id = $2
                          AND state = 'cleanup_claimed'
                          AND claim_owner = $4
                        "#,
                    )
                    .bind(tenant)
                    .bind(outbox)
                    .bind(CLEANUP_EVENT_INVALID)
                    .bind(command.claim_owner.to_string())
                    .execute(&mut *transaction)
                    .await
                    .map_err(|_| McpOAuthPkceCleanupDeliveryError::Unavailable)?;
                }
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::Unavailable)?;
        Ok(claims)
    }

    async fn settle_mcp_oauth_pkce_cleanup(
        &self,
        claim: &ClaimedMcpOAuthPkceCleanup,
        settlement: McpOAuthPkceCleanupSettlement,
    ) -> Result<bool, McpOAuthPkceCleanupDeliveryError> {
        claim.validate()?;
        let (state, failure_code, delay_milliseconds) = match settlement {
            McpOAuthPkceCleanupSettlement::Completed => ("cleanup_completed", None, 0),
            McpOAuthPkceCleanupSettlement::Retry {
                failure_code,
                delay_milliseconds,
            } => {
                if delay_milliseconds == 0 || delay_milliseconds > 3_600_000 {
                    return Err(McpOAuthPkceCleanupDeliveryError::InvalidCommand);
                }
                ("cleanup_retry", Some(failure_code), delay_milliseconds)
            }
            McpOAuthPkceCleanupSettlement::DeadLetter { failure_code } => {
                ("cleanup_dead", Some(failure_code), 0)
            }
        };
        if failure_code.is_some_and(|code| !valid_failure_code(code)) {
            return Err(McpOAuthPkceCleanupDeliveryError::InvalidCommand);
        }
        let delay_milliseconds = i64::try_from(delay_milliseconds)
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::InvalidCommand)?;
        let result = sqlx::query(
            r#"
            UPDATE insight_platform.outbox_events
            SET state = $6,
                publish_attempts = CASE
                    WHEN publish_attempts < 2147483647
                    THEN publish_attempts + 1
                    ELSE publish_attempts
                END,
                next_publish_at = clock_timestamp()
                    + ($7::bigint * interval '1 millisecond'),
                claim_owner = NULL,
                claim_expires_at = NULL,
                last_failure_code = $8,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1
              AND outbox_id = $2
              AND event_id = $3
              AND state = 'cleanup_claimed'
              AND claim_owner = $4
              AND claim_epoch = $5
              AND claim_expires_at > clock_timestamp()
              AND published_at IS NULL
            "#,
        )
        .bind(claim.request.tenant_id.to_string())
        .bind(claim.outbox_id.to_string())
        .bind(claim.event_id.to_string())
        .bind(claim.claim_owner.to_string())
        .bind(
            i64::try_from(claim.claim_epoch)
                .map_err(|_| McpOAuthPkceCleanupDeliveryError::InvalidCommand)?,
        )
        .bind(state)
        .bind(delay_milliseconds)
        .bind(failure_code)
        .execute(self.pool())
        .await
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::Unavailable)?;
        Ok(result.rows_affected() == 1)
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_claimed_event(
    row: &sqlx::postgres::PgRow,
    tenant_id: ResourceId,
    outbox_id: ResourceId,
    event_id: ResourceId,
    claim_owner: ResourceId,
    claim_epoch: u64,
    publish_attempts: u32,
) -> Result<ClaimedMcpOAuthPkceCleanup, McpOAuthPkceCleanupDeliveryError> {
    if tenant_id.kind() != ResourceKind::Tenant
        || outbox_id.kind() != ResourceKind::OutboxEvent
        || event_id.kind() != ResourceKind::Event
        || row.try_get::<String, _>("visibility").ok().as_deref() != Some("internal")
        || row.try_get::<i32, _>("payload_schema_version").ok() != Some(1)
    {
        return Err(McpOAuthPkceCleanupDeliveryError::CorruptEvent);
    }
    let payload: Value = row
        .try_get("payload")
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
    let expected_digest: Sha256Digest = row
        .try_get::<String, _>("payload_digest")
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?
        .parse()
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
    if canonical_value_digest(&payload)? != expected_digest {
        return Err(McpOAuthPkceCleanupDeliveryError::CorruptEvent);
    }
    let event_type: String = row
        .try_get("event_type")
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
    let trace = TraceIdentityV1::new(
        row.try_get::<String, _>("trace_id")
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?
            .parse::<TraceId>()
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?,
    );
    let aggregate_kind: String = row
        .try_get("aggregate_kind")
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
    let aggregate_id: String = row
        .try_get("aggregate_id")
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
    let (task_id, hint, cause) = match event_type.as_str() {
        "mcp.oauth_authorization_completed" | "mcp.oauth_authorization_declined" => {
            let payload: CallbackCleanupEventPayload = serde_json::from_value(payload)
                .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
            let expected_state = if event_type.ends_with("completed") {
                "responded"
            } else {
                "declined"
            };
            let expected_aggregate_kind = if event_type.ends_with("completed") {
                "mcp_authorization"
            } else {
                "mcp_oauth_task"
            };
            let expected_aggregate_id = if event_type.ends_with("completed") {
                &payload.authorization_binding_id
            } else {
                &payload.task_id
            };
            if payload.schema_version != 1
                || payload.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
                || payload.callback_ingress_generation_id.kind()
                    != ResourceKind::WorkerProcessGeneration
                || payload.task_id.kind() != ResourceKind::Interaction
                || payload.state != expected_state
                || aggregate_kind != expected_aggregate_kind
                || aggregate_id != expected_aggregate_id.to_string()
            {
                return Err(McpOAuthPkceCleanupDeliveryError::CorruptEvent);
            }
            let cause = if event_type.ends_with("completed") {
                McpOAuthPkceCleanupCause::Authorized
            } else {
                McpOAuthPkceCleanupCause::Declined
            };
            (payload.task_id, payload.pkce_cleanup, cause)
        }
        "mcp.oauth_authorization_expired" => {
            let payload: ExpiredCleanupEventPayload = serde_json::from_value(payload)
                .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
            if payload.schema_version != 1
                || payload.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
                || payload.scheduler_generation_id.kind() != ResourceKind::WorkerProcessGeneration
                || payload.task_id.kind() != ResourceKind::Interaction
                || aggregate_kind != "mcp_oauth_task"
                || aggregate_id != payload.task_id.to_string()
            {
                return Err(McpOAuthPkceCleanupDeliveryError::CorruptEvent);
            }
            (
                payload.task_id,
                payload.pkce_cleanup,
                McpOAuthPkceCleanupCause::Expired,
            )
        }
        _ => return Err(McpOAuthPkceCleanupDeliveryError::CorruptEvent),
    };
    let claim = ClaimedMcpOAuthPkceCleanup {
        outbox_id,
        event_id,
        claim_owner,
        claim_epoch,
        publish_attempts,
        trace,
        request: McpOAuthPkceCleanupRequest {
            tenant_id,
            task_id,
            cause,
            hint,
        },
    };
    claim.validate()?;
    Ok(claim)
}

fn canonical_value_digest(value: &Value) -> Result<Sha256Digest, McpOAuthPkceCleanupDeliveryError> {
    let canonical =
        serde_jcs::to_vec(value).map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
    let digest = Sha256::digest(canonical);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}")
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
    }
    encoded
        .parse()
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)
}

fn valid_failure_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 128
        && code.as_bytes()[0].is_ascii_lowercase()
        && code.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_.:".contains(&byte)
        })
}
