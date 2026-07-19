use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgRow, Postgres, Row, Sqlite, Transaction};

use crate::engine::{
    plan::PlanType, ActivationId, ContentHash, ProjectionMutationKind, RunId, RuntimeValue,
    SignalId, TransitionKey, TransitionOutcome,
};

use super::{
    common::canonical_intent_hash,
    postgres_projection::{
        append_projection_mutation_event as append_postgres_projection_mutation_event,
        finalize_projection_checkpoints as finalize_postgres_projection_checkpoints,
    },
    sqlite_projection::{
        append_projection_mutation_event as append_sqlite_projection_mutation_event,
        finalize_projection_checkpoints as finalize_sqlite_projection_checkpoints,
    },
    PostgresDurableRepository, RepositoryError, SqliteDurableRepository,
};

const MAX_WORK_ITEM_LIST: u32 = 1_024;
const MAX_COMPLETION_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HumanWorkItemId(String);

impl HumanWorkItemId {
    pub fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        validate_label(&value)?;
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanWorkItemState {
    Open,
    Claimed,
    Completed,
    Cancelled,
    Expired,
}

impl HumanWorkItemState {
    fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "open" => Ok(Self::Open),
            "claimed" => Ok(Self::Claimed),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HumanTaskPrincipal {
    identity: String,
    groups: Vec<String>,
}

impl HumanTaskPrincipal {
    pub fn new(
        identity: impl Into<String>,
        groups: impl IntoIterator<Item = String>,
    ) -> Result<Self, RepositoryError> {
        let identity = identity.into();
        validate_label(&identity)?;
        let mut groups = groups.into_iter().collect::<Vec<_>>();
        for group in &groups {
            validate_label(group)?;
        }
        groups.sort();
        groups.dedup();
        Ok(Self { identity, groups })
    }
    pub fn identity(&self) -> &str {
        &self.identity
    }
    pub fn groups(&self) -> &[String] {
        &self.groups
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HumanWorkItem {
    work_item_id: HumanWorkItemId,
    run_id: RunId,
    activation_id: ActivationId,
    signal_name: String,
    request: Value,
    response_type: PlanType,
    assignees: Vec<String>,
    candidate_groups: Vec<String>,
    state: HumanWorkItemState,
    claim_fence: u64,
    claimed_by: Option<String>,
    claim_expires_at: Option<String>,
    projection_version: u64,
}

impl HumanWorkItem {
    pub fn work_item_id(&self) -> &HumanWorkItemId {
        &self.work_item_id
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }
    pub fn signal_name(&self) -> &str {
        &self.signal_name
    }
    pub fn request(&self) -> &Value {
        &self.request
    }
    pub fn response_type(&self) -> &PlanType {
        &self.response_type
    }
    pub fn assignees(&self) -> &[String] {
        &self.assignees
    }
    pub fn candidate_groups(&self) -> &[String] {
        &self.candidate_groups
    }
    pub fn state(&self) -> HumanWorkItemState {
        self.state
    }
    pub fn claim_fence(&self) -> u64 {
        self.claim_fence
    }
    pub fn claimed_by(&self) -> Option<&str> {
        self.claimed_by.as_deref()
    }
    pub fn claim_expires_at(&self) -> Option<&str> {
        self.claim_expires_at.as_deref()
    }
    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }

    fn assigned_to(&self, principal: &HumanTaskPrincipal) -> bool {
        (self.assignees.is_empty() && self.candidate_groups.is_empty())
            || self
                .assignees
                .iter()
                .any(|value| value == principal.identity())
            || self
                .candidate_groups
                .iter()
                .any(|candidate| principal.groups().iter().any(|group| group == candidate))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClaimHumanWorkItemCommand {
    work_item_id: HumanWorkItemId,
    principal: HumanTaskPrincipal,
    request_id: String,
}

impl ClaimHumanWorkItemCommand {
    pub fn new(
        work_item_id: HumanWorkItemId,
        principal: HumanTaskPrincipal,
        request_id: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        let request_id = request_id.into();
        validate_label(&request_id)?;
        Ok(Self {
            work_item_id,
            principal,
            request_id,
        })
    }
    pub fn work_item_id(&self) -> &HumanWorkItemId {
        &self.work_item_id
    }
    pub fn principal(&self) -> &HumanTaskPrincipal {
        &self.principal
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HumanWorkItemClaim {
    work_item: HumanWorkItem,
}

impl HumanWorkItemClaim {
    pub fn work_item(&self) -> &HumanWorkItem {
        &self.work_item
    }
    pub fn claim_fence(&self) -> u64 {
        self.work_item.claim_fence()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CompleteHumanWorkItemCommand {
    work_item_id: HumanWorkItemId,
    principal: HumanTaskPrincipal,
    request_id: String,
    claim_fence: u64,
    value: Value,
}

impl CompleteHumanWorkItemCommand {
    pub fn new(
        work_item_id: HumanWorkItemId,
        principal: HumanTaskPrincipal,
        request_id: impl Into<String>,
        claim_fence: u64,
        value: Value,
    ) -> Result<Self, RepositoryError> {
        let request_id = request_id.into();
        validate_label(&request_id)?;
        if claim_fence == 0 {
            return Err(RepositoryError::invalid_data());
        }
        if serde_jcs::to_vec(&value)
            .map_err(|_| RepositoryError::canonicalization())?
            .len()
            > MAX_COMPLETION_VALUE_BYTES
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            work_item_id,
            principal,
            request_id,
            claim_fence,
            value,
        })
    }
    pub fn work_item_id(&self) -> &HumanWorkItemId {
        &self.work_item_id
    }
    pub fn principal(&self) -> &HumanTaskPrincipal {
        &self.principal
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn claim_fence(&self) -> u64 {
        self.claim_fence
    }
    pub fn value(&self) -> &Value {
        &self.value
    }
}

/// Durable authority returned before publishing the scheduler completion
/// signal. Replaying this exact authority closes the process-crash gap.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HumanWorkItemCompletionAuthority {
    work_item: HumanWorkItem,
    signal_id: SignalId,
    value: Value,
    message_id: String,
}

impl HumanWorkItemCompletionAuthority {
    pub fn work_item(&self) -> &HumanWorkItem {
        &self.work_item
    }
    pub fn signal_id(&self) -> &SignalId {
        &self.signal_id
    }
    pub fn value(&self) -> &Value {
        &self.value
    }
    pub fn message_id(&self) -> &str {
        &self.message_id
    }
}

#[async_trait]
pub trait HumanTaskDurableRepository: Send + Sync {
    async fn list_human_work_items(
        &self,
        principal: &HumanTaskPrincipal,
        limit: u32,
    ) -> Result<Vec<HumanWorkItem>, RepositoryError>;

    async fn load_human_work_item(
        &self,
        work_item_id: &HumanWorkItemId,
    ) -> Result<Option<HumanWorkItem>, RepositoryError>;

    async fn claim_human_work_item(
        &self,
        command: ClaimHumanWorkItemCommand,
    ) -> Result<TransitionOutcome<HumanWorkItemClaim>, RepositoryError>;

    /// Reserves an idempotent typed completion. The caller must publish and
    /// resolve the returned signal, then call `finalize_human_work_item`.
    async fn complete_human_work_item(
        &self,
        command: CompleteHumanWorkItemCommand,
    ) -> Result<TransitionOutcome<HumanWorkItemCompletionAuthority>, RepositoryError>;

    async fn finalize_human_work_item(
        &self,
        work_item_id: &HumanWorkItemId,
        request_id: &str,
    ) -> Result<bool, RepositoryError>;

    /// Durable completion reservations whose signal publication may have
    /// been interrupted by process failure.
    async fn list_pending_human_work_item_completions(
        &self,
        limit: u32,
    ) -> Result<Vec<HumanWorkItemCompletionAuthority>, RepositoryError>;

    async fn reconcile_human_work_items(&self, limit: u32) -> Result<u64, RepositoryError>;
}

fn validate_label(value: &str) -> Result<(), RepositoryError> {
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

fn validated_limit(limit: u32) -> Result<i64, RepositoryError> {
    if limit == 0 || limit > MAX_WORK_ITEM_LIST {
        return Err(RepositoryError::invalid_data());
    }
    Ok(i64::from(limit))
}

fn completion_hash(value: &Value) -> Result<String, RepositoryError> {
    let bytes = serde_jcs::to_vec(value).map_err(|_| RepositoryError::canonicalization())?;
    Ok(ContentHash::from_bytes(&bytes).as_str().to_owned())
}

fn completion_message_id(work_item_id: &HumanWorkItemId, request_id: &str) -> String {
    let hash =
        ContentHash::from_bytes(format!("{}:{request_id}", work_item_id.as_str()).as_bytes());
    format!("human_{}", &hash.as_str()[7..39])
}

async fn append_sqlite_human_mutation<T: Serialize + ?Sized>(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    work_item_id: &HumanWorkItemId,
    operation: &str,
    request_id: &str,
    projection_version: u64,
    intent: &T,
) -> Result<(), RepositoryError> {
    let key = TransitionKey::derive(
        "repository.human_work_item",
        &[
            operation,
            run_id.as_str(),
            work_item_id.as_str(),
            request_id,
        ],
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let hash = canonical_intent_hash(intent)?;
    let event_id = append_sqlite_projection_mutation_event(
        tx,
        run_id,
        &key,
        hash.as_str(),
        ProjectionMutationKind::HumanWorkItemMutated,
        projection_version,
    )
    .await?;
    finalize_sqlite_projection_checkpoints(tx, run_id, &event_id).await
}

async fn append_postgres_human_mutation<T: Serialize + ?Sized>(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
    work_item_id: &HumanWorkItemId,
    operation: &str,
    request_id: &str,
    projection_version: u64,
    intent: &T,
) -> Result<(), RepositoryError> {
    let key = TransitionKey::derive(
        "repository.human_work_item",
        &[
            operation,
            run_id.as_str(),
            work_item_id.as_str(),
            request_id,
        ],
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    let hash = canonical_intent_hash(intent)?;
    let event_id = append_postgres_projection_mutation_event(
        tx,
        run_id,
        &key,
        hash.as_str(),
        ProjectionMutationKind::HumanWorkItemMutated,
        projection_version,
    )
    .await?;
    finalize_postgres_projection_checkpoints(tx, run_id, &event_id).await
}

fn sqlite_time(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn parse_json_text<T: serde::de::DeserializeOwned>(value: String) -> Result<T, RepositoryError> {
    serde_json::from_str(&value).map_err(|_| RepositoryError::invalid_data())
}

fn parse_sqlite_work_item(row: &sqlx::sqlite::SqliteRow) -> Result<HumanWorkItem, RepositoryError> {
    Ok(HumanWorkItem {
        work_item_id: HumanWorkItemId::new(
            row.try_get::<String, _>("work_item_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        run_id: RunId::new(
            row.try_get::<String, _>("run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        activation_id: ActivationId::new(
            row.try_get::<String, _>("activation_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        signal_name: row
            .try_get("signal_name")
            .map_err(|_| RepositoryError::invalid_data())?,
        request: parse_json_text(
            row.try_get("request_value")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        response_type: parse_json_text(
            row.try_get("response_type")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        assignees: parse_json_text(
            row.try_get("assignees")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        candidate_groups: parse_json_text(
            row.try_get("candidate_groups")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        state: HumanWorkItemState::parse(
            &row.try_get::<String, _>("work_state")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        claim_fence: u64::try_from(
            row.try_get::<i64, _>("claim_fence")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        claimed_by: row
            .try_get("claimed_by")
            .map_err(|_| RepositoryError::invalid_data())?,
        claim_expires_at: row
            .try_get("claim_expires_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        projection_version: u64::try_from(
            row.try_get::<i64, _>("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    })
}

const SQLITE_LOAD_WORK_ITEM: &str = "SELECT work_item_id,run_id,activation_id,signal_name,request_value,response_type,assignees,candidate_groups,work_state,claim_fence,claimed_by,claim_expires_at,projection_version FROM human_work_items WHERE work_item_id=?";
const SQLITE_LIST_WORK_ITEMS: &str = "SELECT work_item_id,run_id,activation_id,signal_name,request_value,response_type,assignees,candidate_groups,work_state,claim_fence,claimed_by,claim_expires_at,projection_version FROM human_work_items h WHERE work_state IN ('open','claimed') AND ((h.work_state='open' AND ((json_array_length(h.assignees)=0 AND json_array_length(h.candidate_groups)=0) OR EXISTS (SELECT 1 FROM json_each(h.assignees) a WHERE a.value=?) OR EXISTS (SELECT 1 FROM json_each(h.candidate_groups) c JOIN json_each(?) g ON g.value=c.value))) OR (h.work_state='claimed' AND h.claimed_by=?)) ORDER BY created_at,work_item_id LIMIT ?";

async fn load_sqlite_work_item(
    tx: &mut Transaction<'_, Sqlite>,
    id: &HumanWorkItemId,
) -> Result<Option<HumanWorkItem>, RepositoryError> {
    sqlx::query(SQLITE_LOAD_WORK_ITEM)
        .bind(id.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(parse_sqlite_work_item)
        .transpose()
}

async fn reopen_expired_sqlite_claims(
    repository: &SqliteDurableRepository,
) -> Result<u64, RepositoryError> {
    let ids = sqlx::query_scalar::<_, String>(
        "SELECT work_item_id FROM human_work_items
         WHERE work_state='claimed' AND completion_request_id IS NULL
           AND julianday(claim_expires_at)<=julianday('now')
         ORDER BY claim_expires_at,work_item_id LIMIT 1024",
    )
    .fetch_all(&repository.pool)
    .await
    .map_err(RepositoryError::storage)?;
    let mut reopened = 0_u64;
    for value in ids {
        let id = HumanWorkItemId::new(value)?;
        let mut tx = repository
            .pool
            .begin()
            .await
            .map_err(RepositoryError::storage)?;
        let request_id = sqlx::query_scalar::<_, String>(
            "SELECT claim_request_id FROM human_work_items
             WHERE work_item_id=? AND work_state='claimed'",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(request_id) = request_id else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            continue;
        };
        let rows = sqlx::query(
            "UPDATE human_work_items SET work_state='open',claimed_by=NULL,
                claim_request_id=NULL,claim_expires_at=NULL,
                projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP
             WHERE work_item_id=? AND work_state='claimed'
               AND completion_request_id IS NULL
               AND julianday(claim_expires_at)<=julianday('now')",
        )
        .bind(id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows == 1 {
            let item = load_sqlite_work_item(&mut tx, &id)
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            append_sqlite_human_mutation(
                &mut tx,
                item.run_id(),
                &id,
                "expire_claim",
                &request_id,
                item.projection_version(),
                &(&id, &request_id),
            )
            .await?;
            reopened += 1;
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
    }
    Ok(reopened)
}

#[async_trait]
impl HumanTaskDurableRepository for SqliteDurableRepository {
    async fn list_human_work_items(
        &self,
        principal: &HumanTaskPrincipal,
        limit: u32,
    ) -> Result<Vec<HumanWorkItem>, RepositoryError> {
        let limit = validated_limit(limit)?;
        let _writer = self.writer.lock().await;
        reopen_expired_sqlite_claims(self).await?;
        let groups = serde_jcs::to_string(principal.groups())
            .map_err(|_| RepositoryError::canonicalization())?;
        let rows = sqlx::query(SQLITE_LIST_WORK_ITEMS)
            .bind(principal.identity())
            .bind(groups)
            .bind(principal.identity())
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        rows.iter().map(parse_sqlite_work_item).collect()
    }

    async fn load_human_work_item(
        &self,
        id: &HumanWorkItemId,
    ) -> Result<Option<HumanWorkItem>, RepositoryError> {
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let item = load_sqlite_work_item(&mut tx, id).await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(item)
    }

    async fn claim_human_work_item(
        &self,
        command: ClaimHumanWorkItemCommand,
    ) -> Result<TransitionOutcome<HumanWorkItemClaim>, RepositoryError> {
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        sqlx::query("UPDATE human_work_items SET work_state='open',claimed_by=NULL,claim_request_id=NULL,claim_expires_at=NULL,projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP WHERE work_item_id=? AND work_state='claimed' AND completion_request_id IS NULL AND julianday(claim_expires_at)<=julianday('now')")
            .bind(command.work_item_id().as_str()).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        let Some(current) = load_sqlite_work_item(&mut tx, command.work_item_id()).await? else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if current.state == HumanWorkItemState::Claimed
            && current.claimed_by() == Some(command.principal().identity())
        {
            let stored_request = sqlx::query_scalar::<_, Option<String>>(
                "SELECT claim_request_id FROM human_work_items WHERE work_item_id=?",
            )
            .bind(command.work_item_id().as_str())
            .fetch_one(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
            if stored_request.as_deref() == Some(command.request_id()) {
                tx.commit().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::ExactReplay {
                    authoritative: HumanWorkItemClaim { work_item: current },
                });
            }
        }
        if current.state != HumanWorkItemState::Open || !current.assigned_to(command.principal()) {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("UPDATE human_work_items SET work_state='claimed',claimed_by=?,claim_request_id=?,claim_expires_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','+' || (claim_lease_ms / 1000.0) || ' seconds'),claim_fence=claim_fence+1,projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP WHERE work_item_id=? AND work_state='open'")
            .bind(command.principal().identity()).bind(command.request_id()).bind(command.work_item_id().as_str()).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        let item = load_sqlite_work_item(&mut tx, command.work_item_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        append_sqlite_human_mutation(
            &mut tx,
            item.run_id(),
            command.work_item_id(),
            "claim",
            command.request_id(),
            item.projection_version(),
            &command,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: HumanWorkItemClaim { work_item: item },
        })
    }

    async fn complete_human_work_item(
        &self,
        command: CompleteHumanWorkItemCommand,
    ) -> Result<TransitionOutcome<HumanWorkItemCompletionAuthority>, RepositoryError> {
        let hash = completion_hash(command.value())?;
        let runtime_value = RuntimeValue::new(command.value().clone())
            .map_err(|_| RepositoryError::invalid_data())?;
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let Some(item) = load_sqlite_work_item(&mut tx, command.work_item_id()).await? else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if !runtime_value.matches(item.response_type()) {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let row = sqlx::query("SELECT signal_id,completion_request_id,completion_payload_hash FROM human_work_items WHERE work_item_id=?").bind(command.work_item_id().as_str()).fetch_one(&mut *tx).await.map_err(RepositoryError::storage)?;
        let stored_request: Option<String> = row
            .try_get("completion_request_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_hash: Option<String> = row
            .try_get("completion_payload_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        let exact_replay = stored_request.as_deref() == Some(command.request_id())
            && stored_hash.as_deref() == Some(hash.as_str())
            && item.claimed_by() == Some(command.principal().identity())
            && item.claim_fence() == command.claim_fence();
        if !exact_replay {
            if item.state != HumanWorkItemState::Claimed
                || item.claimed_by() != Some(command.principal().identity())
                || item.claim_fence() != command.claim_fence()
            {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let rows = sqlx::query("UPDATE human_work_items SET completion_request_id=?,completion_payload=?,completion_payload_hash=?,projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP WHERE work_item_id=? AND work_state='claimed' AND claimed_by=? AND claim_fence=? AND julianday(claim_expires_at)>julianday('now') AND completion_request_id IS NULL")
                .bind(command.request_id()).bind(serde_jcs::to_string(command.value()).map_err(|_| RepositoryError::canonicalization())?).bind(&hash).bind(command.work_item_id().as_str()).bind(command.principal().identity()).bind(i64::try_from(command.claim_fence()).map_err(|_| RepositoryError::invalid_data())?).execute(&mut *tx).await.map_err(RepositoryError::storage)?.rows_affected();
            if rows != 1 {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
        }
        let work_item = load_sqlite_work_item(&mut tx, command.work_item_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        let signal_id = SignalId::new(
            row.try_get::<String, _>("signal_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if !exact_replay {
            append_sqlite_human_mutation(
                &mut tx,
                work_item.run_id(),
                command.work_item_id(),
                "reserve_completion",
                command.request_id(),
                work_item.projection_version(),
                &command,
            )
            .await?;
        }
        let authority = HumanWorkItemCompletionAuthority {
            work_item,
            signal_id,
            value: command.value().clone(),
            message_id: completion_message_id(command.work_item_id(), command.request_id()),
        };
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(if exact_replay {
            TransitionOutcome::ExactReplay {
                authoritative: authority,
            }
        } else {
            TransitionOutcome::Committed { result: authority }
        })
    }

    async fn finalize_human_work_item(
        &self,
        id: &HumanWorkItemId,
        request_id: &str,
    ) -> Result<bool, RepositoryError> {
        validate_label(request_id)?;
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let rows = sqlx::query("UPDATE human_work_items SET work_state='completed',completed_at=CURRENT_TIMESTAMP,claim_expires_at=NULL,projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP WHERE work_item_id=? AND work_state='claimed' AND completion_request_id=?")
            .bind(id.as_str()).bind(request_id).execute(&mut *tx).await.map_err(RepositoryError::storage)?.rows_affected();
        if rows == 1 {
            let item = load_sqlite_work_item(&mut tx, id)
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            append_sqlite_human_mutation(
                &mut tx,
                item.run_id(),
                id,
                "finalize_completion",
                request_id,
                item.projection_version(),
                &(id, request_id),
            )
            .await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(true);
        }
        let completed = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM human_work_items WHERE work_item_id=? AND work_state='completed' AND completion_request_id=?").bind(id.as_str()).bind(request_id).fetch_one(&mut *tx).await.map_err(RepositoryError::storage)? == 1;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(completed)
    }

    async fn reconcile_human_work_items(&self, limit: u32) -> Result<u64, RepositoryError> {
        let limit = validated_limit(limit)?;
        let _writer = self.writer.lock().await;
        reopen_expired_sqlite_claims(self).await?;
        let ids = sqlx::query_scalar::<_, String>("SELECT h.work_item_id FROM human_work_items h JOIN signals_inbox s ON s.run_id=h.run_id AND s.signal_id=h.signal_id WHERE h.work_state='claimed' AND h.completion_request_id IS NOT NULL AND s.signal_state='consumed' ORDER BY h.work_item_id LIMIT ?")
            .bind(limit).fetch_all(&self.pool).await.map_err(RepositoryError::storage)?;
        let mut completed = 0_u64;
        for value in ids {
            let id = HumanWorkItemId::new(value)?;
            let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
            let request_id = sqlx::query_scalar::<_, String>("SELECT completion_request_id FROM human_work_items WHERE work_item_id=? AND work_state='claimed'")
                .bind(id.as_str()).fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?;
            let Some(request_id) = request_id else {
                tx.commit().await.map_err(RepositoryError::storage)?;
                continue;
            };
            let rows = sqlx::query("UPDATE human_work_items SET work_state='completed',completed_at=CURRENT_TIMESTAMP,claim_expires_at=NULL,projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP WHERE work_item_id=? AND work_state='claimed'")
                .bind(id.as_str()).execute(&mut *tx).await.map_err(RepositoryError::storage)?.rows_affected();
            if rows == 1 {
                let item = load_sqlite_work_item(&mut tx, &id)
                    .await?
                    .ok_or_else(RepositoryError::invalid_data)?;
                append_sqlite_human_mutation(
                    &mut tx,
                    item.run_id(),
                    &id,
                    "reconcile_completion",
                    &request_id,
                    item.projection_version(),
                    &(&id, &request_id),
                )
                .await?;
                completed += 1;
            }
            tx.commit().await.map_err(RepositoryError::storage)?;
        }
        Ok(completed)
    }

    async fn list_pending_human_work_item_completions(
        &self,
        limit: u32,
    ) -> Result<Vec<HumanWorkItemCompletionAuthority>, RepositoryError> {
        let ids = sqlx::query_scalar::<_, String>(
            "SELECT h.work_item_id FROM human_work_items h
             LEFT JOIN signals_inbox s ON s.run_id=h.run_id AND s.signal_id=h.signal_id
             WHERE h.work_state='claimed' AND h.completion_request_id IS NOT NULL
               AND (s.signal_id IS NULL OR s.signal_state='pending')
             ORDER BY h.updated_at,h.work_item_id LIMIT ?",
        )
        .bind(validated_limit(limit)?)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let _writer = self.writer.lock().await;
        let mut values = Vec::with_capacity(ids.len());
        for value in ids {
            let id = HumanWorkItemId::new(value)?;
            let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
            let row = sqlx::query("SELECT signal_id,completion_request_id,completion_payload FROM human_work_items WHERE work_item_id=? AND work_state='claimed' AND completion_request_id IS NOT NULL")
                .bind(id.as_str()).fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?;
            let Some(row) = row else {
                tx.commit().await.map_err(RepositoryError::storage)?;
                continue;
            };
            let item = load_sqlite_work_item(&mut tx, &id)
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            let request_id: String = row
                .try_get("completion_request_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            values.push(HumanWorkItemCompletionAuthority {
                work_item: item,
                signal_id: SignalId::new(
                    row.try_get::<String, _>("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
                value: parse_json_text(
                    row.try_get("completion_payload")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                message_id: completion_message_id(&id, &request_id),
            });
            tx.commit().await.map_err(RepositoryError::storage)?;
        }
        Ok(values)
    }
}

// PostgreSQL implementation follows the same state machine, but row locks and
// database-clock lease comparisons make it safe across runtimes.

const POSTGRES_LOAD_WORK_ITEM: &str = "SELECT work_item_id,run_id,activation_id,signal_name,request_value,response_type,assignees,candidate_groups,work_state,claim_fence,claimed_by,claim_expires_at,projection_version FROM human_work_items WHERE work_item_id=$1";
const POSTGRES_LOCK_WORK_ITEM: &str = "SELECT work_item_id,run_id,activation_id,signal_name,request_value,response_type,assignees,candidate_groups,work_state,claim_fence,claimed_by,claim_expires_at,projection_version FROM human_work_items WHERE work_item_id=$1 FOR UPDATE";
const POSTGRES_LIST_WORK_ITEMS: &str = "SELECT work_item_id,run_id,activation_id,signal_name,request_value,response_type,assignees,candidate_groups,work_state,claim_fence,claimed_by,claim_expires_at,projection_version FROM human_work_items h WHERE work_state IN ('open','claimed') AND ((h.work_state='open' AND ((jsonb_array_length(h.assignees)=0 AND jsonb_array_length(h.candidate_groups)=0) OR h.assignees ? $1 OR h.candidate_groups ?| $2::text[])) OR (h.work_state='claimed' AND h.claimed_by=$1)) ORDER BY created_at,work_item_id LIMIT $3";

fn parse_postgres_work_item(row: &PgRow) -> Result<HumanWorkItem, RepositoryError> {
    Ok(HumanWorkItem {
        work_item_id: HumanWorkItemId::new(
            row.try_get::<String, _>("work_item_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        run_id: RunId::new(
            row.try_get::<String, _>("run_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        activation_id: ActivationId::new(
            row.try_get::<String, _>("activation_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        signal_name: row
            .try_get("signal_name")
            .map_err(|_| RepositoryError::invalid_data())?,
        request: row
            .try_get("request_value")
            .map_err(|_| RepositoryError::invalid_data())?,
        response_type: serde_json::from_value(
            row.try_get::<Value, _>("response_type")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        assignees: serde_json::from_value(
            row.try_get::<Value, _>("assignees")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        candidate_groups: serde_json::from_value(
            row.try_get::<Value, _>("candidate_groups")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        state: HumanWorkItemState::parse(
            &row.try_get::<String, _>("work_state")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        claim_fence: u64::try_from(
            row.try_get::<i64, _>("claim_fence")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        claimed_by: row
            .try_get("claimed_by")
            .map_err(|_| RepositoryError::invalid_data())?,
        claim_expires_at: row
            .try_get::<Option<DateTime<Utc>>, _>("claim_expires_at")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(sqlite_time),
        projection_version: u64::try_from(
            row.try_get::<i64, _>("projection_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
    })
}

async fn load_postgres_work_item(
    tx: &mut Transaction<'_, Postgres>,
    id: &HumanWorkItemId,
) -> Result<Option<HumanWorkItem>, RepositoryError> {
    sqlx::query(POSTGRES_LOCK_WORK_ITEM)
        .bind(id.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(parse_postgres_work_item)
        .transpose()
}

async fn reopen_expired_postgres_claims(
    repository: &PostgresDurableRepository,
) -> Result<u64, RepositoryError> {
    let ids = sqlx::query_scalar::<_, String>(
        "SELECT work_item_id FROM human_work_items
         WHERE work_state='claimed' AND completion_request_id IS NULL
           AND claim_expires_at<=clock_timestamp()
         ORDER BY claim_expires_at,work_item_id LIMIT 1024",
    )
    .fetch_all(&repository.pool)
    .await
    .map_err(RepositoryError::storage)?;
    let mut reopened = 0_u64;
    for value in ids {
        let id = HumanWorkItemId::new(value)?;
        let mut tx = repository
            .pool
            .begin()
            .await
            .map_err(RepositoryError::storage)?;
        let request_id = sqlx::query_scalar::<_, String>(
            "SELECT claim_request_id FROM human_work_items
             WHERE work_item_id=$1 AND work_state='claimed' FOR UPDATE SKIP LOCKED",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(request_id) = request_id else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            continue;
        };
        let rows = sqlx::query(
            "UPDATE human_work_items SET work_state='open',claimed_by=NULL,
                claim_request_id=NULL,claim_expires_at=NULL,
                projection_version=projection_version+1,updated_at=clock_timestamp()
             WHERE work_item_id=$1 AND work_state='claimed'
               AND completion_request_id IS NULL
               AND claim_expires_at<=clock_timestamp()",
        )
        .bind(id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows == 1 {
            let item = load_postgres_work_item(&mut tx, &id)
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            append_postgres_human_mutation(
                &mut tx,
                item.run_id(),
                &id,
                "expire_claim",
                &request_id,
                item.projection_version(),
                &(&id, &request_id),
            )
            .await?;
            reopened += 1;
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
    }
    Ok(reopened)
}

#[async_trait]
impl HumanTaskDurableRepository for PostgresDurableRepository {
    async fn list_human_work_items(
        &self,
        principal: &HumanTaskPrincipal,
        limit: u32,
    ) -> Result<Vec<HumanWorkItem>, RepositoryError> {
        reopen_expired_postgres_claims(self).await?;
        let rows = sqlx::query(POSTGRES_LIST_WORK_ITEMS)
            .bind(principal.identity())
            .bind(principal.groups())
            .bind(validated_limit(limit)?)
            .fetch_all(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        rows.iter().map(parse_postgres_work_item).collect()
    }

    async fn load_human_work_item(
        &self,
        id: &HumanWorkItemId,
    ) -> Result<Option<HumanWorkItem>, RepositoryError> {
        sqlx::query(POSTGRES_LOAD_WORK_ITEM)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(RepositoryError::storage)?
            .as_ref()
            .map(parse_postgres_work_item)
            .transpose()
    }

    async fn claim_human_work_item(
        &self,
        command: ClaimHumanWorkItemCommand,
    ) -> Result<TransitionOutcome<HumanWorkItemClaim>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        sqlx::query(
            "UPDATE human_work_items SET work_state='open',claimed_by=NULL,
                claim_request_id=NULL,claim_expires_at=NULL,
                projection_version=projection_version+1,updated_at=clock_timestamp()
             WHERE work_item_id=$1 AND work_state='claimed'
               AND completion_request_id IS NULL AND claim_expires_at<=clock_timestamp()",
        )
        .bind(command.work_item_id().as_str())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(current) = load_postgres_work_item(&mut tx, command.work_item_id()).await? else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if current.state == HumanWorkItemState::Claimed
            && current.claimed_by() == Some(command.principal().identity())
        {
            let stored_request = sqlx::query_scalar::<_, Option<String>>(
                "SELECT claim_request_id FROM human_work_items WHERE work_item_id=$1",
            )
            .bind(command.work_item_id().as_str())
            .fetch_one(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
            if stored_request.as_deref() == Some(command.request_id()) {
                tx.commit().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::ExactReplay {
                    authoritative: HumanWorkItemClaim { work_item: current },
                });
            }
        }
        if current.state != HumanWorkItemState::Open || !current.assigned_to(command.principal()) {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let rows = sqlx::query(
            "UPDATE human_work_items SET work_state='claimed',claimed_by=$1,
                claim_request_id=$2,
                claim_expires_at=clock_timestamp() + (claim_lease_ms::double precision * interval '1 millisecond'),
                claim_fence=claim_fence+1,projection_version=projection_version+1,
                updated_at=clock_timestamp()
             WHERE work_item_id=$3 AND work_state='open'",
        )
        .bind(command.principal().identity())
        .bind(command.request_id())
        .bind(command.work_item_id().as_str())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let item = load_postgres_work_item(&mut tx, command.work_item_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        append_postgres_human_mutation(
            &mut tx,
            item.run_id(),
            command.work_item_id(),
            "claim",
            command.request_id(),
            item.projection_version(),
            &command,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: HumanWorkItemClaim { work_item: item },
        })
    }

    async fn complete_human_work_item(
        &self,
        command: CompleteHumanWorkItemCommand,
    ) -> Result<TransitionOutcome<HumanWorkItemCompletionAuthority>, RepositoryError> {
        let hash = completion_hash(command.value())?;
        let runtime_value = RuntimeValue::new(command.value().clone())
            .map_err(|_| RepositoryError::invalid_data())?;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let Some(item) = load_postgres_work_item(&mut tx, command.work_item_id()).await? else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if !runtime_value.matches(item.response_type()) {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let row = sqlx::query(
            "SELECT signal_id,completion_request_id,completion_payload_hash
             FROM human_work_items WHERE work_item_id=$1",
        )
        .bind(command.work_item_id().as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let stored_request: Option<String> = row
            .try_get("completion_request_id")
            .map_err(|_| RepositoryError::invalid_data())?;
        let stored_hash: Option<String> = row
            .try_get("completion_payload_hash")
            .map_err(|_| RepositoryError::invalid_data())?;
        let exact_replay = stored_request.as_deref() == Some(command.request_id())
            && stored_hash.as_deref() == Some(hash.as_str())
            && item.claimed_by() == Some(command.principal().identity())
            && item.claim_fence() == command.claim_fence();
        if !exact_replay {
            if item.state != HumanWorkItemState::Claimed
                || item.claimed_by() != Some(command.principal().identity())
                || item.claim_fence() != command.claim_fence()
            {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let rows = sqlx::query(
                "UPDATE human_work_items SET completion_request_id=$1,
                    completion_payload=$2,completion_payload_hash=$3,
                    projection_version=projection_version+1,updated_at=clock_timestamp()
                 WHERE work_item_id=$4 AND work_state='claimed' AND claimed_by=$5
                   AND claim_fence=$6 AND claim_expires_at>clock_timestamp()
                   AND completion_request_id IS NULL",
            )
            .bind(command.request_id())
            .bind(command.value())
            .bind(&hash)
            .bind(command.work_item_id().as_str())
            .bind(command.principal().identity())
            .bind(
                i64::try_from(command.claim_fence())
                    .map_err(|_| RepositoryError::invalid_data())?,
            )
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
        }
        let work_item = load_postgres_work_item(&mut tx, command.work_item_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        let signal_id = SignalId::new(
            row.try_get::<String, _>("signal_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?;
        if !exact_replay {
            append_postgres_human_mutation(
                &mut tx,
                work_item.run_id(),
                command.work_item_id(),
                "reserve_completion",
                command.request_id(),
                work_item.projection_version(),
                &command,
            )
            .await?;
        }
        let authority = HumanWorkItemCompletionAuthority {
            work_item,
            signal_id,
            value: command.value().clone(),
            message_id: completion_message_id(command.work_item_id(), command.request_id()),
        };
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(if exact_replay {
            TransitionOutcome::ExactReplay {
                authoritative: authority,
            }
        } else {
            TransitionOutcome::Committed { result: authority }
        })
    }

    async fn finalize_human_work_item(
        &self,
        id: &HumanWorkItemId,
        request_id: &str,
    ) -> Result<bool, RepositoryError> {
        validate_label(request_id)?;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "UPDATE human_work_items SET work_state='completed',completed_at=clock_timestamp(),
                claim_expires_at=NULL,projection_version=projection_version+1,
                updated_at=clock_timestamp()
             WHERE work_item_id=$1 AND work_state='claimed' AND completion_request_id=$2",
        )
        .bind(id.as_str())
        .bind(request_id)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows == 1 {
            let item = load_postgres_work_item(&mut tx, id)
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            append_postgres_human_mutation(
                &mut tx,
                item.run_id(),
                id,
                "finalize_completion",
                request_id,
                item.projection_version(),
                &(id, request_id),
            )
            .await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(true);
        }
        let completed = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM human_work_items
             WHERE work_item_id=$1 AND work_state='completed' AND completion_request_id=$2",
        )
        .bind(id.as_str())
        .bind(request_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
            == 1;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(completed)
    }

    async fn reconcile_human_work_items(&self, limit: u32) -> Result<u64, RepositoryError> {
        reopen_expired_postgres_claims(self).await?;
        let ids = sqlx::query_scalar::<_, String>(
            "SELECT h.work_item_id FROM human_work_items h
                JOIN signals_inbox s ON s.run_id=h.run_id AND s.signal_id=h.signal_id
                WHERE h.work_state='claimed' AND h.completion_request_id IS NOT NULL
                  AND s.signal_state='consumed'
                ORDER BY h.work_item_id LIMIT $1",
        )
        .bind(validated_limit(limit)?)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut completed = 0_u64;
        for value in ids {
            let id = HumanWorkItemId::new(value)?;
            let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
            let request_id = sqlx::query_scalar::<_, String>("SELECT completion_request_id FROM human_work_items WHERE work_item_id=$1 AND work_state='claimed' FOR UPDATE SKIP LOCKED")
                .bind(id.as_str()).fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?;
            let Some(request_id) = request_id else {
                tx.commit().await.map_err(RepositoryError::storage)?;
                continue;
            };
            let rows = sqlx::query("UPDATE human_work_items h SET work_state='completed',completed_at=clock_timestamp(),claim_expires_at=NULL,projection_version=h.projection_version+1,updated_at=clock_timestamp() WHERE h.work_item_id=$1 AND h.work_state='claimed' AND EXISTS (SELECT 1 FROM signals_inbox s WHERE s.run_id=h.run_id AND s.signal_id=h.signal_id AND s.signal_state='consumed')")
                .bind(id.as_str()).execute(&mut *tx).await.map_err(RepositoryError::storage)?.rows_affected();
            if rows == 1 {
                let item = load_postgres_work_item(&mut tx, &id)
                    .await?
                    .ok_or_else(RepositoryError::invalid_data)?;
                append_postgres_human_mutation(
                    &mut tx,
                    item.run_id(),
                    &id,
                    "reconcile_completion",
                    &request_id,
                    item.projection_version(),
                    &(&id, &request_id),
                )
                .await?;
                completed += 1;
            }
            tx.commit().await.map_err(RepositoryError::storage)?;
        }
        Ok(completed)
    }

    async fn list_pending_human_work_item_completions(
        &self,
        limit: u32,
    ) -> Result<Vec<HumanWorkItemCompletionAuthority>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT h.work_item_id,h.signal_id,h.completion_request_id,h.completion_payload
             FROM human_work_items h
             LEFT JOIN signals_inbox s ON s.run_id=h.run_id AND s.signal_id=h.signal_id
             WHERE h.work_state='claimed' AND h.completion_request_id IS NOT NULL
               AND (s.signal_id IS NULL OR s.signal_state='pending')
             ORDER BY h.updated_at,h.work_item_id LIMIT $1",
        )
        .bind(validated_limit(limit)?)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let id = HumanWorkItemId::new(
                row.try_get::<String, _>("work_item_id")
                    .map_err(|_| RepositoryError::invalid_data())?,
            )?;
            let request_id: String = row
                .try_get("completion_request_id")
                .map_err(|_| RepositoryError::invalid_data())?;
            let item = self
                .load_human_work_item(&id)
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            values.push(HumanWorkItemCompletionAuthority {
                work_item: item,
                signal_id: SignalId::new(
                    row.try_get::<String, _>("signal_id")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
                value: row
                    .try_get("completion_payload")
                    .map_err(|_| RepositoryError::invalid_data())?,
                message_id: completion_message_id(&id, &request_id),
            });
        }
        Ok(values)
    }
}
