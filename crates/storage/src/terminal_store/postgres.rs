use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_engine::{
    repository::{RepositoryError, REPOSITORY_CONSTRAINT_CONFLICT},
    ArtifactRef, ContentHash, DefinitionRevisionId, DeploymentRevisionId, RunId,
};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;
use uuid::Uuid;

use crate::repository::{database_time, PostgresDurableRepository, RepositoryErrorExt as _};

use super::*;

#[derive(Debug)]
struct DeletionIntent {
    tenant_id: String,
    content_ref: String,
    source_kind: ContentDeletionSourceKind,
    source_id: String,
}

fn constraint_conflict() -> RepositoryError {
    RepositoryError::new(
        REPOSITORY_CONSTRAINT_CONFLICT,
        "terminal repository constraint conflict",
    )
}

fn lease_lost() -> RepositoryError {
    RepositoryError::new(
        TERMINAL_RUN_OWNER_LEASE_LOST,
        "terminal runtime owner lease is no longer active",
    )
}

fn run_not_found() -> RepositoryError {
    RepositoryError::new(
        TERMINAL_RUN_NOT_FOUND,
        "terminal-only Run admission was not found",
    )
}

fn conversation_archived() -> RepositoryError {
    RepositoryError::new(
        CONVERSATION_ARCHIVED,
        "conversation is archived and cannot accept new messages",
    )
}

fn conversation_ownership_mismatch() -> RepositoryError {
    RepositoryError::new(
        CONVERSATION_OWNERSHIP_MISMATCH,
        "conversation ownership does not match the requested principal",
    )
}

fn validate_text(value: &str, max_bytes: usize) -> Result<(), RepositoryError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.chars().any(|character| character.is_control())
    {
        return Err(invalid_data());
    }
    Ok(())
}

fn validate_owner(owner: &RuntimeOwner) -> Result<(), RepositoryError> {
    validate_text(&owner.instance_id, 256)?;
    if owner.owner_epoch < 1 {
        return Err(invalid_data());
    }
    Ok(())
}

fn validate_lease(lease: &RuntimeInstanceLease) -> Result<(), RepositoryError> {
    validate_owner(&lease.owner)?;
    validate_text(&lease.endpoint, 4096)?;
    if lease.lease_expires_at < lease.started_at {
        return Err(invalid_data());
    }
    Ok(())
}

fn validate_admission(command: &NewTerminalRunAdmission) -> Result<(), RepositoryError> {
    validate_text(&command.tenant_id, 256)?;
    validate_text(&command.request_id, 256)?;
    validate_text(&command.agent_id, 256)?;
    validate_owner(&command.owner)?;
    if let Some(conversation) = &command.conversation {
        validate_text(&conversation.conversation_id, 256)?;
        validate_text(&conversation.user_message_id, 256)?;
    }
    if let Some(input_ref) = &command.input_ref {
        validate_text(input_ref, 16 * 1024)?;
    }
    Ok(())
}

fn validate_result(command: &NewTerminalRunResult) -> Result<(), RepositoryError> {
    validate_owner(&command.owner)?;
    validate_text(&command.response_id, 256)?;
    if command.output_ref.is_some() != command.output_hash.is_some() {
        return Err(invalid_data());
    }
    if let Some(output_ref) = &command.output_ref {
        validate_text(output_ref, 16 * 1024)?;
    }
    if let Some(error_code) = &command.error_code {
        validate_text(error_code, 256)?;
    }
    if command.terminal_at < command.started_at {
        return Err(invalid_data());
    }
    Ok(())
}

fn validate_query(query: &ConversationQuery) -> Result<(), RepositoryError> {
    validate_text(&query.conversation_id, 256)?;
    validate_text(&query.tenant_id, 256)?;
    validate_text(&query.user_id, 256)
}

fn validate_retention(retention: &BoundedRetention) -> Result<i64, RepositoryError> {
    if retention.limit == 0 || retention.limit > 10_000 {
        return Err(invalid_data());
    }
    Ok(i64::from(retention.limit))
}

fn decode_owner_lease(
    row: &sqlx::postgres::PgRow,
) -> Result<RuntimeInstanceLease, RepositoryError> {
    Ok(RuntimeInstanceLease {
        owner: RuntimeOwner {
            instance_id: row.try_get("instance_id").map_err(|_| invalid_data())?,
            owner_epoch: row.try_get("owner_epoch").map_err(|_| invalid_data())?,
        },
        endpoint: row.try_get("endpoint").map_err(|_| invalid_data())?,
        lease_expires_at: row
            .try_get("lease_expires_at")
            .map_err(|_| invalid_data())?,
        started_at: row.try_get("started_at").map_err(|_| invalid_data())?,
    })
}

fn decode_admission(row: &sqlx::postgres::PgRow) -> Result<TerminalRunAdmission, RepositoryError> {
    let conversation_id = row
        .try_get::<Option<String>, _>("conversation_id")
        .map_err(|_| invalid_data())?;
    let user_message_id = row
        .try_get::<Option<String>, _>("user_message_id")
        .map_err(|_| invalid_data())?;
    let conversation = match (conversation_id, user_message_id) {
        (Some(conversation_id), Some(user_message_id)) => Some(AdmissionConversation {
            conversation_id,
            user_message_id,
        }),
        (None, None) => None,
        _ => return Err(invalid_data()),
    };
    Ok(TerminalRunAdmission {
        run_id: RunId::new(
            row.try_get::<String, _>("run_id")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        tenant_id: row.try_get("tenant_id").map_err(|_| invalid_data())?,
        request_id: row.try_get("request_id").map_err(|_| invalid_data())?,
        agent_id: row.try_get("agent_id").map_err(|_| invalid_data())?,
        definition_revision_id: DefinitionRevisionId::new(
            row.try_get::<String, _>("definition_revision_id")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        deployment_revision_id: DeploymentRevisionId::new(
            row.try_get::<String, _>("deployment_revision_id")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        conversation,
        input_ref: row.try_get("input_ref").map_err(|_| invalid_data())?,
        input_hash: ContentHash::parse(
            row.try_get::<String, _>("input_hash")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        selected_context_hash: row
            .try_get::<Option<String>, _>("selected_context_hash")
            .map_err(|_| invalid_data())?
            .map(ContentHash::parse)
            .transpose()
            .map_err(|_| invalid_data())?,
        owner: RuntimeOwner {
            instance_id: row
                .try_get("owner_instance_id")
                .map_err(|_| invalid_data())?,
            owner_epoch: row.try_get("owner_epoch").map_err(|_| invalid_data())?,
        },
        accepted_at: row.try_get("accepted_at").map_err(|_| invalid_data())?,
    })
}

fn decode_result(row: &sqlx::postgres::PgRow) -> Result<TerminalRunResult, RepositoryError> {
    Ok(TerminalRunResult {
        run_id: RunId::new(
            row.try_get::<String, _>("run_id")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        terminal_state: parse_terminal_state(
            &row.try_get::<String, _>("terminal_state")
                .map_err(|_| invalid_data())?,
        )?,
        response_id: row.try_get("response_id").map_err(|_| invalid_data())?,
        output_ref: row.try_get("output_ref").map_err(|_| invalid_data())?,
        output_hash: row
            .try_get::<Option<String>, _>("output_hash")
            .map_err(|_| invalid_data())?
            .map(ContentHash::parse)
            .transpose()
            .map_err(|_| invalid_data())?,
        error_code: row.try_get("error_code").map_err(|_| invalid_data())?,
        usage_json: row.try_get("usage_json").map_err(|_| invalid_data())?,
        started_at: row.try_get("started_at").map_err(|_| invalid_data())?,
        terminal_at: row.try_get("terminal_at").map_err(|_| invalid_data())?,
    })
}

fn decode_conversation(row: &sqlx::postgres::PgRow) -> Result<Conversation, RepositoryError> {
    let persistence_mode = match row
        .try_get::<String, _>("persistence_mode")
        .map_err(|_| invalid_data())?
        .as_str()
    {
        "full" => insight_engine::PersistenceMode::Full,
        "terminal_only" => insight_engine::PersistenceMode::TerminalOnly,
        _ => return Err(invalid_data()),
    };
    Ok(Conversation {
        conversation_id: row.try_get("conversation_id").map_err(|_| invalid_data())?,
        tenant_id: row.try_get("tenant_id").map_err(|_| invalid_data())?,
        user_id: row.try_get("user_id").map_err(|_| invalid_data())?,
        agent_id: row.try_get("agent_id").map_err(|_| invalid_data())?,
        persistence_mode,
        deployment_revision_id: DeploymentRevisionId::new(
            row.try_get::<String, _>("deployment_revision_id")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        created_at: row.try_get("created_at").map_err(|_| invalid_data())?,
        archived_at: row.try_get("archived_at").map_err(|_| invalid_data())?,
    })
}

fn decode_message(row: &sqlx::postgres::PgRow) -> Result<ConversationMessage, RepositoryError> {
    let inline = row
        .try_get::<Option<Value>, _>("content_inline")
        .map_err(|_| invalid_data())?;
    let reference = row
        .try_get::<Option<String>, _>("content_ref")
        .map_err(|_| invalid_data())?;
    let content = match (inline, reference) {
        (Some(value), None) => ConversationContent::Inline(value),
        (None, Some(reference)) => ConversationContent::Ref(reference),
        _ => return Err(invalid_data()),
    };
    Ok(ConversationMessage {
        message_id: row.try_get("message_id").map_err(|_| invalid_data())?,
        conversation_id: row.try_get("conversation_id").map_err(|_| invalid_data())?,
        message_order: row.try_get("message_order").map_err(|_| invalid_data())?,
        role: parse_conversation_role(
            &row.try_get::<String, _>("role")
                .map_err(|_| invalid_data())?,
        )?,
        run_id: row
            .try_get::<Option<String>, _>("run_id")
            .map_err(|_| invalid_data())?
            .map(RunId::new)
            .transpose()
            .map_err(|_| invalid_data())?,
        content,
        content_hash: ContentHash::parse(
            row.try_get::<String, _>("content_hash")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        created_at: row.try_get("created_at").map_err(|_| invalid_data())?,
    })
}

fn decode_summary(row: &sqlx::postgres::PgRow) -> Result<ConversationSummary, RepositoryError> {
    Ok(ConversationSummary {
        conversation_id: row.try_get("conversation_id").map_err(|_| invalid_data())?,
        through_message_order: row
            .try_get("through_message_order")
            .map_err(|_| invalid_data())?,
        summary_ref: row.try_get("summary_ref").map_err(|_| invalid_data())?,
        summary_hash: ContentHash::parse(
            row.try_get::<String, _>("summary_hash")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        model_revision: row.try_get("model_revision").map_err(|_| invalid_data())?,
        created_at: row.try_get("created_at").map_err(|_| invalid_data())?,
    })
}

fn decode_deletion_job(
    row: &sqlx::postgres::PgRow,
) -> Result<TerminalContentDeletionJob, RepositoryError> {
    Ok(TerminalContentDeletionJob {
        deletion_job_id: row.try_get("deletion_job_id").map_err(|_| invalid_data())?,
        tenant_id: row.try_get("tenant_id").map_err(|_| invalid_data())?,
        content_ref: row.try_get("content_ref").map_err(|_| invalid_data())?,
        content_hash: ContentHash::parse(
            row.try_get::<String, _>("content_hash")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        source_kind: parse_content_deletion_source_kind(
            &row.try_get::<String, _>("source_kind")
                .map_err(|_| invalid_data())?,
        )?,
        source_id: row.try_get("source_id").map_err(|_| invalid_data())?,
        attempts: u64::try_from(
            row.try_get::<i64, _>("attempts")
                .map_err(|_| invalid_data())?,
        )
        .map_err(|_| invalid_data())?,
        available_at: row.try_get("available_at").map_err(|_| invalid_data())?,
        created_at: row.try_get("created_at").map_err(|_| invalid_data())?,
    })
}

async fn enqueue_deletion_intents(
    executor: &mut Transaction<'_, Postgres>,
    intents: Vec<DeletionIntent>,
    created_at: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    for intent in intents {
        validate_text(&intent.tenant_id, 256)?;
        validate_text(&intent.content_ref, 16 * 1024)?;
        validate_text(&intent.source_id, 256)?;
        let content_hash = serde_json::from_str::<ArtifactRef>(&intent.content_ref)
            .ok()
            .filter(|artifact| artifact.media_type() == Some(TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE))
            .map(|artifact| artifact.content_hash().clone())
            .unwrap_or_else(|| ContentHash::from_bytes(intent.content_ref.as_bytes()));
        sqlx::query(
            "INSERT INTO terminal_content_deletion_jobs (
                 deletion_job_id,tenant_id,content_ref,content_hash,source_kind,source_id,
                 available_at,created_at
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$7)
             ON CONFLICT(tenant_id,content_ref,source_kind,source_id) DO NOTHING",
        )
        .bind(format!("terminal_delete_{}", Uuid::new_v4().simple()))
        .bind(intent.tenant_id)
        .bind(intent.content_ref)
        .bind(content_hash.as_str())
        .bind(content_deletion_source_kind_as_str(intent.source_kind))
        .bind(intent.source_id)
        .bind(created_at)
        .execute(&mut **executor)
        .await
        .map_err(RepositoryError::storage)?;
    }
    Ok(())
}

fn result_matches(stored: &TerminalRunResult, command: &NewTerminalRunResult) -> bool {
    stored.run_id == command.run_id
        && stored.terminal_state == command.terminal_state
        && stored.response_id == command.response_id
        && stored.output_ref == command.output_ref
        && stored.output_hash == command.output_hash
        && stored.error_code == command.error_code
        && stored.usage_json == command.usage_json
        && stored.started_at == database_time(command.started_at)
        && stored.terminal_at == database_time(command.terminal_at)
}

fn admission_intent_matches(
    stored: &TerminalRunAdmission,
    command: &NewTerminalRunAdmission,
) -> bool {
    stored.tenant_id == command.tenant_id
        && stored.request_id == command.request_id
        && stored.agent_id == command.agent_id
        && stored.definition_revision_id == command.definition_revision_id
        && stored.deployment_revision_id == command.deployment_revision_id
        && stored
            .conversation
            .as_ref()
            .map(|conversation| conversation.conversation_id.as_str())
            == command
                .conversation
                .as_ref()
                .map(|conversation| conversation.conversation_id.as_str())
        && stored.input_ref == command.input_ref
        && stored.input_hash == command.input_hash
        && stored.selected_context_hash == command.selected_context_hash
}

fn conversation_admission_intent_matches(
    stored: &TerminalRunAdmission,
    command: &NewTerminalRunAdmission,
) -> bool {
    stored.tenant_id == command.tenant_id
        && stored.request_id == command.request_id
        && stored.agent_id == command.agent_id
        && stored.definition_revision_id == command.definition_revision_id
        && stored.deployment_revision_id == command.deployment_revision_id
        && stored
            .conversation
            .as_ref()
            .map(|conversation| conversation.conversation_id.as_str())
            == command
                .conversation
                .as_ref()
                .map(|conversation| conversation.conversation_id.as_str())
}

fn message_intent_matches(stored: &ConversationMessage, command: &NewConversationMessage) -> bool {
    stored.role == ConversationRole::User
        && stored.content == command.content
        && stored.content_hash == command.content_hash
}

fn assistant_intent_matches(
    stored: &ConversationMessage,
    command: &NewConversationMessage,
) -> bool {
    stored.role == ConversationRole::Assistant
        && stored.content == command.content
        && stored.content_hash == command.content_hash
}

async fn load_admission_by_request(
    executor: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    request_id: &str,
) -> Result<Option<TerminalRunAdmission>, RepositoryError> {
    sqlx::query(
        "SELECT run_id,tenant_id,request_id,agent_id,definition_revision_id,
                deployment_revision_id,conversation_id,user_message_id,input_ref,input_hash,
                selected_context_hash,owner_instance_id,owner_epoch,accepted_at
         FROM terminal_run_admissions
         WHERE tenant_id=$1 AND request_id=$2",
    )
    .bind(tenant_id)
    .bind(request_id)
    .fetch_optional(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?
    .map(|row| decode_admission(&row))
    .transpose()
}

async fn load_result(
    executor: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<Option<TerminalRunResult>, RepositoryError> {
    sqlx::query(
        "SELECT run_id,terminal_state,response_id,output_ref,output_hash,error_code,
                usage_json,started_at,terminal_at
         FROM terminal_run_results WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_optional(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?
    .map(|row| decode_result(&row))
    .transpose()
}

async fn require_active_owner(
    executor: &mut Transaction<'_, Postgres>,
    owner: &RuntimeOwner,
) -> Result<(), RepositoryError> {
    let active = sqlx::query_scalar::<_, bool>(
        "SELECT TRUE FROM terminal_runtime_instances
         WHERE instance_id=$1 AND owner_epoch=$2
           AND lease_expires_at>clock_timestamp()
           AND started_at>=pg_postmaster_start_time()
         FOR SHARE",
    )
    .bind(&owner.instance_id)
    .bind(owner.owner_epoch)
    .fetch_optional(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?
    .unwrap_or(false);
    if active {
        Ok(())
    } else {
        Err(lease_lost())
    }
}

async fn check_active_owner(
    executor: &mut Transaction<'_, Postgres>,
    owner: &RuntimeOwner,
) -> Result<(), RepositoryError> {
    let active = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
             SELECT 1 FROM terminal_runtime_instances
             WHERE instance_id=$1 AND owner_epoch=$2
               AND lease_expires_at>clock_timestamp()
               AND started_at>=pg_postmaster_start_time()
         )",
    )
    .bind(&owner.instance_id)
    .bind(owner.owner_epoch)
    .fetch_one(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?;
    if active {
        Ok(())
    } else {
        Err(lease_lost())
    }
}

async fn insert_admission(
    executor: &mut Transaction<'_, Postgres>,
    command: &NewTerminalRunAdmission,
) -> Result<bool, RepositoryError> {
    let (conversation_id, user_message_id) = command
        .conversation
        .as_ref()
        .map(|conversation| {
            (
                Some(conversation.conversation_id.as_str()),
                Some(conversation.user_message_id.as_str()),
            )
        })
        .unwrap_or((None, None));
    let inserted = sqlx::query(
        "INSERT INTO terminal_run_admissions (
             run_id,tenant_id,request_id,agent_id,definition_revision_id,
             deployment_revision_id,conversation_id,user_message_id,input_ref,input_hash,
             selected_context_hash,owner_instance_id,owner_epoch,accepted_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
         ON CONFLICT DO NOTHING",
    )
    .bind(command.run_id.as_str())
    .bind(&command.tenant_id)
    .bind(&command.request_id)
    .bind(&command.agent_id)
    .bind(command.definition_revision_id.as_str())
    .bind(command.deployment_revision_id.as_str())
    .bind(conversation_id)
    .bind(user_message_id)
    .bind(&command.input_ref)
    .bind(command.input_hash.as_str())
    .bind(
        command
            .selected_context_hash
            .as_ref()
            .map(ContentHash::as_str),
    )
    .bind(&command.owner.instance_id)
    .bind(command.owner.owner_epoch)
    .bind(database_time(command.accepted_at))
    .execute(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected()
        == 1;
    Ok(inserted)
}

async fn insert_result(
    executor: &mut Transaction<'_, Postgres>,
    command: &NewTerminalRunResult,
) -> Result<bool, RepositoryError> {
    let inserted = sqlx::query(
        "INSERT INTO terminal_run_results (
             run_id,terminal_state,response_id,output_ref,output_hash,error_code,
             usage_json,started_at,terminal_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
         ON CONFLICT DO NOTHING",
    )
    .bind(command.run_id.as_str())
    .bind(terminal_state_as_str(command.terminal_state))
    .bind(&command.response_id)
    .bind(&command.output_ref)
    .bind(command.output_hash.as_ref().map(ContentHash::as_str))
    .bind(&command.error_code)
    .bind(&command.usage_json)
    .bind(database_time(command.started_at))
    .bind(database_time(command.terminal_at))
    .execute(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected()
        == 1;
    Ok(inserted)
}

async fn require_conversation(
    executor: &mut Transaction<'_, Postgres>,
    query: &ConversationQuery,
    lock: bool,
) -> Result<Option<Conversation>, RepositoryError> {
    let sql = if lock {
        "SELECT conversation_id,tenant_id,user_id,agent_id,persistence_mode,
                deployment_revision_id,created_at,archived_at
         FROM conversations
         WHERE conversation_id=$1 AND tenant_id=$2 AND user_id=$3
         FOR UPDATE"
    } else {
        "SELECT conversation_id,tenant_id,user_id,agent_id,persistence_mode,
                deployment_revision_id,created_at,archived_at
         FROM conversations
         WHERE conversation_id=$1 AND tenant_id=$2 AND user_id=$3"
    };
    sqlx::query(sql)
        .bind(&query.conversation_id)
        .bind(&query.tenant_id)
        .bind(&query.user_id)
        .fetch_optional(&mut **executor)
        .await
        .map_err(RepositoryError::storage)?
        .map(|row| decode_conversation(&row))
        .transpose()
}

fn split_content(content: &ConversationContent) -> (Option<&Value>, Option<&str>) {
    match content {
        ConversationContent::Inline(value) => (Some(value), None),
        ConversationContent::Ref(reference) => (None, Some(reference)),
    }
}

async fn insert_message(
    executor: &mut Transaction<'_, Postgres>,
    conversation_id: &str,
    run_id: &RunId,
    role: ConversationRole,
    message: &NewConversationMessage,
) -> Result<ConversationMessage, RepositoryError> {
    validate_text(&message.message_id, 256)?;
    if let ConversationContent::Ref(reference) = &message.content {
        validate_text(reference, 16 * 1024)?;
    }
    let (inline, reference) = split_content(&message.content);
    // PostgreSQL sequence caches are reserved per backend. Different pool
    // connections can therefore consume cached ranges out of insertion order.
    // Allocate the per-Conversation ordinal under a transaction-scoped lock
    // instead, without updating a hot Conversation head row. The cached
    // sequence remains the direct-SQL default required by the Schema contract.
    sqlx::query(
        "SELECT pg_advisory_xact_lock(
             hashtextextended($1, 6075990668891974961::bigint)
         )",
    )
    .bind(conversation_id)
    .execute(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?;
    let message_order = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(message_order),0)+1
         FROM conversation_messages
         WHERE conversation_id=$1",
    )
    .bind(conversation_id)
    .fetch_one(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?;
    let row = sqlx::query(
        "INSERT INTO conversation_messages (
             message_id,conversation_id,message_order,role,run_id,content_inline,
             content_ref,content_hash,created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
         RETURNING message_id,conversation_id,message_order,role,run_id,content_inline,
                   content_ref,content_hash,created_at",
    )
    .bind(&message.message_id)
    .bind(conversation_id)
    .bind(message_order)
    .bind(conversation_role_as_str(role))
    .bind(run_id.as_str())
    .bind(inline)
    .bind(reference)
    .bind(message.content_hash.as_str())
    .bind(database_time(message.created_at))
    .fetch_one(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?;
    decode_message(&row)
}

async fn load_message(
    executor: &mut Transaction<'_, Postgres>,
    conversation_id: &str,
    message_id: &str,
) -> Result<Option<ConversationMessage>, RepositoryError> {
    sqlx::query(
        "SELECT message_id,conversation_id,message_order,role,run_id,content_inline,
                content_ref,content_hash,created_at
         FROM conversation_messages
         WHERE conversation_id=$1 AND message_id=$2",
    )
    .bind(conversation_id)
    .bind(message_id)
    .fetch_optional(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?
    .map(|row| decode_message(&row))
    .transpose()
}

async fn load_assistant_message(
    executor: &mut Transaction<'_, Postgres>,
    conversation_id: &str,
    run_id: &RunId,
) -> Result<Option<ConversationMessage>, RepositoryError> {
    sqlx::query(
        "SELECT message_id,conversation_id,message_order,role,run_id,content_inline,
                content_ref,content_hash,created_at
         FROM conversation_messages
         WHERE conversation_id=$1 AND run_id=$2 AND role='assistant'",
    )
    .bind(conversation_id)
    .bind(run_id.as_str())
    .fetch_optional(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?
    .map(|row| decode_message(&row))
    .transpose()
}

async fn load_summary(
    executor: &mut Transaction<'_, Postgres>,
    conversation_id: &str,
    through_message_order: i64,
) -> Result<Option<ConversationSummary>, RepositoryError> {
    sqlx::query(
        "SELECT conversation_id,through_message_order,summary_ref,summary_hash,
                model_revision,created_at
         FROM conversation_summaries
         WHERE conversation_id=$1 AND through_message_order=$2",
    )
    .bind(conversation_id)
    .bind(through_message_order)
    .fetch_optional(&mut **executor)
    .await
    .map_err(RepositoryError::storage)?
    .map(|row| decode_summary(&row))
    .transpose()
}

#[async_trait]
impl TerminalRunStore for PostgresDurableRepository {
    async fn register_runtime_instance(
        &self,
        mut command: RegisterRuntimeInstance,
    ) -> Result<RuntimeInstanceLease, RepositoryError> {
        validate_lease(&command)?;
        command.lease_expires_at = database_time(command.lease_expires_at);
        command.started_at = database_time(command.started_at);
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        sqlx::query("SELECT pg_advisory_xact_lock(5282809774080937265::bigint)")
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        sqlx::query(
            "DELETE FROM terminal_runtime_instances
             WHERE lease_expires_at<=clock_timestamp()
                OR started_at<pg_postmaster_start_time()",
        )
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let another_owner = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1 FROM terminal_runtime_instances
                 WHERE instance_id<>$1 AND lease_expires_at>clock_timestamp()
             )",
        )
        .bind(&command.owner.instance_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if another_owner {
            return Err(lease_lost());
        }
        let row = sqlx::query(
            "INSERT INTO terminal_runtime_instances (
                 instance_id,owner_epoch,endpoint,lease_expires_at,started_at
             ) VALUES ($1,$2,$3,$4,$5)
             ON CONFLICT(instance_id) DO UPDATE SET
                 owner_epoch=EXCLUDED.owner_epoch,
                 endpoint=EXCLUDED.endpoint,
                 lease_expires_at=EXCLUDED.lease_expires_at,
                 started_at=EXCLUDED.started_at
             WHERE terminal_runtime_instances.owner_epoch < EXCLUDED.owner_epoch
                OR (
                    terminal_runtime_instances.owner_epoch = EXCLUDED.owner_epoch
                    AND terminal_runtime_instances.started_at = EXCLUDED.started_at
                    AND terminal_runtime_instances.lease_expires_at <= EXCLUDED.lease_expires_at
                )
             RETURNING instance_id,owner_epoch,endpoint,lease_expires_at,started_at",
        )
        .bind(&command.owner.instance_id)
        .bind(command.owner.owner_epoch)
        .bind(&command.endpoint)
        .bind(command.lease_expires_at)
        .bind(command.started_at)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        match row {
            Some(row) => {
                let lease = decode_owner_lease(&row)?;
                transaction
                    .commit()
                    .await
                    .map_err(RepositoryError::storage)?;
                Ok(lease)
            }
            None => Err(lease_lost()),
        }
    }

    async fn heartbeat_runtime_instance(
        &self,
        command: HeartbeatRuntimeInstance,
    ) -> Result<OwnerLeaseHeartbeat, RepositoryError> {
        validate_owner(&command.owner)?;
        let row = sqlx::query(
            "UPDATE terminal_runtime_instances
             SET lease_expires_at=$3
             WHERE instance_id=$1 AND owner_epoch=$2
               AND lease_expires_at>clock_timestamp()
               AND started_at>=pg_postmaster_start_time()
               AND $3>clock_timestamp()
             RETURNING instance_id,owner_epoch,endpoint,lease_expires_at,started_at",
        )
        .bind(&command.owner.instance_id)
        .bind(command.owner.owner_epoch)
        .bind(database_time(command.lease_expires_at))
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(match row {
            Some(row) => OwnerLeaseHeartbeat::Renewed {
                lease: decode_owner_lease(&row)?,
            },
            None => OwnerLeaseHeartbeat::Lost,
        })
    }

    async fn check_runtime_owner(
        &self,
        query: OwnerLeaseQuery,
    ) -> Result<OwnerLeaseStatus, RepositoryError> {
        validate_owner(&query.owner)?;
        let row = sqlx::query(
            "SELECT instance_id,owner_epoch,endpoint,lease_expires_at,started_at
             FROM terminal_runtime_instances
             WHERE instance_id=$1 AND owner_epoch=$2
               AND started_at>=pg_postmaster_start_time()",
        )
        .bind(&query.owner.instance_id)
        .bind(query.owner.owner_epoch)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            return Ok(OwnerLeaseStatus::Missing);
        };
        let lease = decode_owner_lease(&row)?;
        if lease.lease_expires_at > database_time(query.observed_at) {
            Ok(OwnerLeaseStatus::Active { lease })
        } else {
            Ok(OwnerLeaseStatus::Expired)
        }
    }

    async fn unregister_runtime_instance(
        &self,
        owner: RuntimeOwner,
    ) -> Result<bool, RepositoryError> {
        validate_owner(&owner)?;
        Ok(sqlx::query(
            "DELETE FROM terminal_runtime_instances
             WHERE instance_id=$1 AND owner_epoch=$2",
        )
        .bind(&owner.instance_id)
        .bind(owner.owner_epoch)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected()
            == 1)
    }

    async fn admit_terminal_run(
        &self,
        command: NewTerminalRunAdmission,
    ) -> Result<AdmissionOutcome, RepositoryError> {
        validate_admission(&command)?;
        if command.conversation.is_some() {
            return Err(invalid_data());
        }
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(admission) =
            load_admission_by_request(&mut transaction, &command.tenant_id, &command.request_id)
                .await?
        {
            if !admission_intent_matches(&admission, &command) {
                return Err(constraint_conflict());
            }
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(AdmissionOutcome {
                admission,
                replayed: true,
            });
        }
        require_active_owner(&mut transaction, &command.owner).await?;
        let inserted = insert_admission(&mut transaction, &command).await?;
        let admission =
            load_admission_by_request(&mut transaction, &command.tenant_id, &command.request_id)
                .await?
                .ok_or_else(constraint_conflict)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(AdmissionOutcome {
            admission,
            replayed: !inserted,
        })
    }

    async fn get_terminal_run(
        &self,
        query: TerminalRunQuery,
    ) -> Result<Option<TerminalRunView>, RepositoryError> {
        validate_text(&query.tenant_id, 256)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let admission_exists = sqlx::query_scalar::<_, bool>(
            "SELECT TRUE
             FROM terminal_run_admissions
             WHERE run_id=$1 AND tenant_id=$2
             FOR SHARE",
        )
        .bind(query.run_id.as_str())
        .bind(&query.tenant_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .unwrap_or(false);
        if !admission_exists {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(None);
        }
        let row = sqlx::query(
            "SELECT
                 a.run_id,a.tenant_id,a.request_id,a.agent_id,a.definition_revision_id,
                 a.deployment_revision_id,a.conversation_id,a.user_message_id,a.input_ref,
                 a.input_hash,a.selected_context_hash,a.owner_instance_id,a.owner_epoch,
                 a.accepted_at,
                 r.run_id AS result_run_id,r.terminal_state,r.response_id,r.output_ref,
                 r.output_hash,r.error_code,r.usage_json,r.started_at,r.terminal_at,
                 i.lease_expires_at AS owner_lease_expires_at,
                 c.conversation_id AS live_conversation_id
             FROM terminal_run_admissions a
             LEFT JOIN terminal_run_results r ON r.run_id=a.run_id
             LEFT JOIN terminal_runtime_instances i
               ON i.instance_id=a.owner_instance_id AND i.owner_epoch=a.owner_epoch
              AND i.started_at>=pg_postmaster_start_time()
             LEFT JOIN conversations c ON c.conversation_id=a.conversation_id
             WHERE a.run_id=$1 AND a.tenant_id=$2",
        )
        .bind(query.run_id.as_str())
        .bind(&query.tenant_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(row) = row else {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(None);
        };
        let mut admission = decode_admission(&row)?;
        let mut result = if row
            .try_get::<Option<String>, _>("result_run_id")
            .map_err(|_| invalid_data())?
            .is_some()
        {
            Some(decode_result(&row)?)
        } else {
            None
        };
        let privacy_deleted = admission.conversation.is_some()
            && row
                .try_get::<Option<String>, _>("live_conversation_id")
                .map_err(|_| invalid_data())?
                .is_none();
        if privacy_deleted {
            admission.input_ref = None;
            if let Some(result) = &mut result {
                result.output_ref = None;
                result.output_hash = None;
            }
        }
        let state = match &result {
            Some(result) => result.terminal_state.into(),
            None => {
                let active = row
                    .try_get::<Option<DateTime<Utc>>, _>("owner_lease_expires_at")
                    .map_err(|_| invalid_data())?
                    .is_some_and(|expires_at| expires_at > database_time(query.observed_at));
                if active {
                    TerminalRunDerivedState::Active
                } else {
                    TerminalRunDerivedState::Interrupted
                }
            }
        };
        let view = TerminalRunView {
            admission,
            result,
            state,
        };
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(Some(view))
    }

    async fn get_terminal_run_by_request(
        &self,
        query: TerminalRunRequestQuery,
    ) -> Result<Option<TerminalRunView>, RepositoryError> {
        validate_text(&query.tenant_id, 256)?;
        validate_text(&query.request_id, 256)?;
        let run_id = sqlx::query_scalar::<_, String>(
            "SELECT run_id FROM terminal_run_admissions
             WHERE tenant_id=$1 AND request_id=$2",
        )
        .bind(&query.tenant_id)
        .bind(&query.request_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(run_id) = run_id else {
            return Ok(None);
        };
        self.get_terminal_run(TerminalRunQuery {
            tenant_id: query.tenant_id,
            run_id: RunId::new(run_id).map_err(|_| invalid_data())?,
            observed_at: query.observed_at,
        })
        .await
    }

    async fn commit_terminal_result(
        &self,
        command: NewTerminalRunResult,
    ) -> Result<TerminalCommitOutcome, RepositoryError> {
        validate_result(&command)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let admission_row = sqlx::query(
            "SELECT tenant_id,owner_instance_id,owner_epoch,conversation_id
             FROM terminal_run_admissions WHERE run_id=$1
             FOR UPDATE",
        )
        .bind(command.run_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(run_not_found)?;
        if admission_row
            .try_get::<Option<String>, _>("conversation_id")
            .map_err(|_| invalid_data())?
            .is_some()
        {
            return Err(invalid_data());
        }
        if let Some(result) = load_result(&mut transaction, &command.run_id).await? {
            if !result_matches(&result, &command) {
                return Err(constraint_conflict());
            }
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(TerminalCommitOutcome {
                result,
                replayed: true,
            });
        }
        let admission_owner = (
            admission_row
                .try_get::<String, _>("owner_instance_id")
                .map_err(|_| invalid_data())?,
            admission_row
                .try_get::<i64, _>("owner_epoch")
                .map_err(|_| invalid_data())?,
        );
        if admission_owner != (command.owner.instance_id.clone(), command.owner.owner_epoch) {
            return Err(lease_lost());
        }
        check_active_owner(&mut transaction, &command.owner).await?;
        if let Some(reference) = command.output_ref.as_deref() {
            super::staging_postgres::consume_terminal_artifact_stage(
                &mut transaction,
                &admission_row
                    .try_get::<String, _>("tenant_id")
                    .map_err(|_| invalid_data())?,
                reference,
                TerminalArtifactSourceKind::RunOutput,
                command.run_id.as_str(),
            )
            .await?;
        }
        check_active_owner(&mut transaction, &command.owner).await?;
        let inserted = insert_result(&mut transaction, &command).await?;
        require_active_owner(&mut transaction, &command.owner).await?;
        let result = load_result(&mut transaction, &command.run_id)
            .await?
            .ok_or_else(constraint_conflict)?;
        if !result_matches(&result, &command) {
            return Err(constraint_conflict());
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(TerminalCommitOutcome {
            result,
            replayed: !inserted,
        })
    }

    async fn delete_terminal_runs_before(
        &self,
        retention: BoundedRetention,
    ) -> Result<RetentionDeleteOutcome, RepositoryError> {
        let limit = validate_retention(&retention)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let candidates = sqlx::query(
            "SELECT a.run_id,a.tenant_id,a.input_ref,r.output_ref
             FROM terminal_run_admissions a
             LEFT JOIN terminal_run_results r ON r.run_id=a.run_id
             WHERE a.accepted_at<$1
               AND (
                   r.run_id IS NOT NULL
                   OR NOT EXISTS(
                       SELECT 1 FROM terminal_runtime_instances i
                       WHERE i.instance_id=a.owner_instance_id
                         AND i.owner_epoch=a.owner_epoch
                         AND i.lease_expires_at>clock_timestamp()
                         AND i.started_at>=pg_postmaster_start_time()
                   )
               )
             ORDER BY a.accepted_at,a.run_id
             FOR UPDATE OF a SKIP LOCKED
             LIMIT $2",
        )
        .bind(database_time(retention.before))
        .bind(limit)
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let mut ids = Vec::with_capacity(candidates.len());
        let mut input_refs = Vec::new();
        let mut output_refs = Vec::new();
        let mut deletion_intents = Vec::new();
        for row in &candidates {
            let run_id = row
                .try_get::<String, _>("run_id")
                .map_err(|_| invalid_data())?;
            let tenant_id = row
                .try_get::<String, _>("tenant_id")
                .map_err(|_| invalid_data())?;
            ids.push(run_id.clone());
            if let Some(reference) = row
                .try_get::<Option<String>, _>("input_ref")
                .map_err(|_| invalid_data())?
            {
                input_refs.push(reference.clone());
                deletion_intents.push(DeletionIntent {
                    tenant_id: tenant_id.clone(),
                    content_ref: reference,
                    source_kind: ContentDeletionSourceKind::TerminalRunRetention,
                    source_id: run_id.clone(),
                });
            }
            if let Some(reference) = row
                .try_get::<Option<String>, _>("output_ref")
                .map_err(|_| invalid_data())?
            {
                output_refs.push(reference.clone());
                deletion_intents.push(DeletionIntent {
                    tenant_id,
                    content_ref: reference,
                    source_kind: ContentDeletionSourceKind::TerminalRunRetention,
                    source_id: run_id,
                });
            }
        }
        enqueue_deletion_intents(
            &mut transaction,
            deletion_intents,
            database_time(Utc::now()),
        )
        .await?;
        if !ids.is_empty() {
            sqlx::query("DELETE FROM terminal_run_admissions WHERE run_id=ANY($1)")
                .bind(&ids)
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        input_refs.sort();
        input_refs.dedup();
        output_refs.sort();
        output_refs.dedup();
        Ok(RetentionDeleteOutcome {
            deleted: u64::try_from(ids.len()).map_err(|_| invalid_data())?,
            input_refs,
            output_refs,
        })
    }
}

async fn lock_content_deletion_fence(
    transaction: &mut Transaction<'_, Postgres>,
    content_hash: &str,
) -> Result<(), RepositoryError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 812493764366122121::bigint))")
        .bind(content_hash)
        .execute(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    Ok(())
}

async fn content_deletion_authority_exists(
    transaction: &mut Transaction<'_, Postgres>,
    content_ref: &str,
    content_hash: &str,
) -> Result<bool, RepositoryError> {
    sqlx::query_scalar::<_, bool>(
        "SELECT
           EXISTS(
                SELECT 1 FROM terminal_run_admissions admission
                LEFT JOIN conversations conversation
                  ON conversation.conversation_id=admission.conversation_id
                WHERE admission.input_ref=$1
                  AND (admission.conversation_id IS NULL
                       OR conversation.conversation_id IS NOT NULL)
           )
           OR EXISTS(
                SELECT 1
                FROM terminal_run_results result
                JOIN terminal_run_admissions admission ON admission.run_id=result.run_id
                LEFT JOIN conversations conversation
                  ON conversation.conversation_id=admission.conversation_id
                WHERE result.output_ref=$1
                  AND (admission.conversation_id IS NULL
                       OR conversation.conversation_id IS NOT NULL)
           )
           OR EXISTS(SELECT 1 FROM conversation_messages WHERE content_ref=$1)
           OR EXISTS(SELECT 1 FROM conversation_summaries WHERE summary_ref=$1)
           OR EXISTS(
                SELECT 1 FROM terminal_artifact_staging WHERE content_hash=$2
           )
           OR EXISTS(
                SELECT 1 FROM artifacts
                WHERE content_hash=$2 AND artifact_state<>'deleted'
           )",
    )
    .bind(content_ref)
    .bind(content_hash)
    .fetch_one(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)
}

#[async_trait]
impl TerminalContentDeletionStore for PostgresDurableRepository {
    async fn claim_content_deletion_jobs(
        &self,
        command: ClaimContentDeletionJobs,
    ) -> Result<Vec<TerminalContentDeletionClaim>, RepositoryError> {
        validate_text(&command.claimed_by, 256)?;
        if command.limit == 0
            || command.limit > 1_000
            || command.claim_expires_at <= command.observed_at
        {
            return Err(invalid_data());
        }
        let observed_at = database_time(command.observed_at);
        let claim_expires_at = database_time(command.claim_expires_at);
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        // Serialize bounded claim batches so transactions that contain
        // multiple identical content hashes cannot acquire per-hash advisory
        // locks in opposing orders.
        sqlx::query("SELECT pg_advisory_xact_lock(756293764366122121::bigint)")
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "SELECT deletion_job_id,content_ref,content_hash
             FROM terminal_content_deletion_jobs
             WHERE (job_state='pending' AND available_at<=$1)
                OR (job_state='claimed' AND claim_expires_at<=$1)
             ORDER BY
                 CASE WHEN job_state='pending' THEN available_at ELSE claim_expires_at END,
                 created_at,deletion_job_id
             FOR UPDATE SKIP LOCKED
             LIMIT $2",
        )
        .bind(observed_at)
        .bind(i64::from(command.limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let deletion_job_id = row
                .try_get::<String, _>("deletion_job_id")
                .map_err(|_| invalid_data())?;
            let content_ref = row
                .try_get::<String, _>("content_ref")
                .map_err(|_| invalid_data())?;
            let Some(content_hash) = row
                .try_get::<Option<String>, _>("content_hash")
                .map_err(|_| invalid_data())?
            else {
                sqlx::query("DELETE FROM terminal_content_deletion_jobs WHERE deletion_job_id=$1")
                    .bind(&deletion_job_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(RepositoryError::storage)?;
                continue;
            };
            lock_content_deletion_fence(&mut transaction, &content_hash).await?;
            if content_deletion_authority_exists(&mut transaction, &content_ref, &content_hash)
                .await?
            {
                sqlx::query("DELETE FROM terminal_content_deletion_jobs WHERE deletion_job_id=$1")
                    .bind(&deletion_job_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(RepositoryError::storage)?;
                continue;
            }
            let claim_token = format!("delete_claim_{}", Uuid::new_v4().simple());
            let claimed = sqlx::query(
                "UPDATE terminal_content_deletion_jobs
                 SET job_state='claimed',claim_token=$2,claimed_by=$3,
                     claim_expires_at=$4,attempts=attempts+1
                 WHERE deletion_job_id=$1
                 RETURNING deletion_job_id,tenant_id,content_ref,content_hash,source_kind,source_id,
                           attempts,available_at,created_at",
            )
            .bind(&deletion_job_id)
            .bind(&claim_token)
            .bind(&command.claimed_by)
            .bind(claim_expires_at)
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            claims.push(TerminalContentDeletionClaim {
                job: decode_deletion_job(&claimed)?,
                claim_token,
            });
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn ack_content_deletion_job(
        &self,
        command: AckContentDeletionJob,
    ) -> Result<bool, RepositoryError> {
        validate_text(&command.deletion_job_id, 256)?;
        validate_text(&command.claim_token, 256)?;
        Ok(sqlx::query(
            "DELETE FROM terminal_content_deletion_jobs
             WHERE deletion_job_id=$1 AND job_state='claimed' AND claim_token=$2",
        )
        .bind(&command.deletion_job_id)
        .bind(&command.claim_token)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected()
            == 1)
    }
}

#[async_trait]
impl ConversationStore for PostgresDurableRepository {
    async fn create_conversation(
        &self,
        command: NewConversation,
    ) -> Result<CreateConversationOutcome, RepositoryError> {
        validate_text(&command.conversation_id, 256)?;
        validate_text(&command.tenant_id, 256)?;
        validate_text(&command.user_id, 256)?;
        validate_text(&command.agent_id, 256)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        sqlx::query(
            "SELECT pg_advisory_xact_lock(
                 hashtextextended($1, 6075990668891974961::bigint)
             )",
        )
        .bind(&command.conversation_id)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1 FROM conversation_tombstones WHERE conversation_id=$1
             )",
        )
        .bind(&command.conversation_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        {
            return Err(conversation_ownership_mismatch());
        }
        let inserted = sqlx::query(
            "INSERT INTO conversations (
                 conversation_id,tenant_id,user_id,agent_id,persistence_mode,
                 deployment_revision_id,created_at
             ) VALUES ($1,$2,$3,$4,$5,$6,$7)
             ON CONFLICT DO NOTHING",
        )
        .bind(&command.conversation_id)
        .bind(&command.tenant_id)
        .bind(&command.user_id)
        .bind(&command.agent_id)
        .bind(command.persistence_mode.as_str())
        .bind(command.deployment_revision_id.as_str())
        .bind(database_time(command.created_at))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected()
            == 1;
        let row = sqlx::query(
            "SELECT conversation_id,tenant_id,user_id,agent_id,persistence_mode,
                    deployment_revision_id,created_at,archived_at
             FROM conversations WHERE conversation_id=$1",
        )
        .bind(&command.conversation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(conversation_ownership_mismatch)?;
        let conversation = decode_conversation(&row)?;
        if conversation.tenant_id != command.tenant_id
            || conversation.user_id != command.user_id
            || conversation.agent_id != command.agent_id
            || conversation.persistence_mode != command.persistence_mode
            || conversation.deployment_revision_id != command.deployment_revision_id
        {
            return Err(conversation_ownership_mismatch());
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(CreateConversationOutcome {
            conversation,
            replayed: !inserted,
        })
    }

    async fn get_conversation(
        &self,
        query: ConversationQuery,
    ) -> Result<Option<Conversation>, RepositoryError> {
        validate_query(&query)?;
        sqlx::query(
            "SELECT conversation_id,tenant_id,user_id,agent_id,persistence_mode,
                    deployment_revision_id,created_at,archived_at
             FROM conversations
             WHERE conversation_id=$1 AND tenant_id=$2 AND user_id=$3",
        )
        .bind(&query.conversation_id)
        .bind(&query.tenant_id)
        .bind(&query.user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .map(|row| decode_conversation(&row))
        .transpose()
    }

    async fn get_terminal_conversation_turn(
        &self,
        query: FullConversationTurnQuery,
    ) -> Result<Option<ConversationTurnOutcome>, RepositoryError> {
        validate_text(&query.tenant_id, 256)?;
        validate_text(&query.request_id, 256)?;
        validate_text(&query.conversation_id, 256)?;
        validate_text(&query.user_id, 256)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let Some(admission) =
            load_admission_by_request(&mut transaction, &query.tenant_id, &query.request_id)
                .await?
        else {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(None);
        };
        let conversation = admission
            .conversation
            .as_ref()
            .filter(|conversation| conversation.conversation_id == query.conversation_id)
            .ok_or_else(constraint_conflict)?;
        if require_conversation(
            &mut transaction,
            &ConversationQuery {
                conversation_id: query.conversation_id.clone(),
                tenant_id: query.tenant_id.clone(),
                user_id: query.user_id,
            },
            false,
        )
        .await?
        .is_none()
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(None);
        }
        let user_message = load_message(
            &mut transaction,
            &query.conversation_id,
            &conversation.user_message_id,
        )
        .await?
        .ok_or_else(invalid_data)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(Some(ConversationTurnOutcome {
            admission,
            user_message,
            replayed: true,
        }))
    }

    async fn get_full_conversation_turn(
        &self,
        query: FullConversationTurnQuery,
    ) -> Result<Option<FullConversationTurn>, RepositoryError> {
        validate_text(&query.tenant_id, 256)?;
        validate_text(&query.request_id, 256)?;
        validate_text(&query.conversation_id, 256)?;
        validate_text(&query.user_id, 256)?;
        let row = sqlx::query(
            "SELECT t.run_id,m.message_id,m.conversation_id,m.message_order,m.role,m.run_id,
                    m.content_inline,m.content_ref,m.content_hash,m.created_at
             FROM full_conversation_turns t
             JOIN conversation_messages m
               ON m.conversation_id=t.conversation_id
              AND m.message_id=t.user_message_id
             WHERE t.tenant_id=$1 AND t.request_id=$2
               AND t.conversation_id=$3 AND t.user_id=$4",
        )
        .bind(&query.tenant_id)
        .bind(&query.request_id)
        .bind(&query.conversation_id)
        .bind(&query.user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| {
            Ok(FullConversationTurn {
                run_id: RunId::new(
                    row.try_get::<String, _>("run_id")
                        .map_err(|_| invalid_data())?,
                )
                .map_err(|_| invalid_data())?,
                user_message: decode_message(&row)?,
            })
        })
        .transpose()
    }

    async fn full_conversation_run_tenant(
        &self,
        run_id: &RunId,
    ) -> Result<Option<String>, RepositoryError> {
        sqlx::query_scalar(
            "WITH RECURSIVE ancestry(run_id) AS (
                 SELECT $1::text
                 UNION
                 SELECT lineage.source_run_id
                 FROM run_recovery_lineage lineage
                 JOIN ancestry ON ancestry.run_id=lineage.run_id
             )
             SELECT turn.tenant_id
             FROM ancestry
             JOIN full_conversation_turns turn ON turn.run_id=ancestry.run_id
             LIMIT 1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)
    }

    async fn full_conversation_run_user(
        &self,
        run_id: &RunId,
    ) -> Result<Option<String>, RepositoryError> {
        sqlx::query_scalar(
            "WITH RECURSIVE ancestry(run_id) AS (
                 SELECT $1::text
                 UNION
                 SELECT lineage.source_run_id
                 FROM run_recovery_lineage lineage
                 JOIN ancestry ON ancestry.run_id=lineage.run_id
             )
             SELECT turn.user_id
             FROM ancestry
             JOIN full_conversation_turns turn ON turn.run_id=ancestry.run_id
             LIMIT 1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)
    }

    async fn full_conversation_run_conversation_id(
        &self,
        run_id: &RunId,
    ) -> Result<Option<String>, RepositoryError> {
        sqlx::query_scalar(
            "WITH RECURSIVE ancestry(run_id) AS (
                 SELECT $1::text
                 UNION
                 SELECT lineage.source_run_id
                 FROM run_recovery_lineage lineage
                 JOIN ancestry ON ancestry.run_id=lineage.run_id
             )
             SELECT turn.conversation_id
             FROM ancestry
             JOIN full_conversation_turns turn ON turn.run_id=ancestry.run_id
             LIMIT 1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)
    }

    async fn full_conversation_run_is_deleted(
        &self,
        run_id: &RunId,
    ) -> Result<Option<bool>, RepositoryError> {
        sqlx::query_scalar(
            "WITH RECURSIVE ancestry(run_id) AS (
                 SELECT $1::text
                 UNION
                 SELECT lineage.source_run_id
                 FROM run_recovery_lineage lineage
                 JOIN ancestry ON ancestry.run_id=lineage.run_id
             )
             SELECT c.conversation_id IS NULL
             FROM ancestry
             JOIN full_conversation_turns t ON t.run_id=ancestry.run_id
             LEFT JOIN conversations c ON c.conversation_id=t.conversation_id
             LIMIT 1",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)
    }

    async fn list_full_conversation_run_ids(
        &self,
        query: ConversationQuery,
    ) -> Result<Vec<RunId>, RepositoryError> {
        validate_query(&query)?;
        sqlx::query_scalar::<_, String>(
            "WITH RECURSIVE family(run_id) AS (
                 SELECT turn.run_id
                 FROM full_conversation_turns turn
                 WHERE turn.conversation_id=$1
                   AND turn.tenant_id=$2
                   AND turn.user_id=$3
                 UNION
                 SELECT lineage.run_id
                 FROM run_recovery_lineage lineage
                 JOIN family ON family.run_id=lineage.source_run_id
             )
             SELECT run_id FROM family ORDER BY run_id",
        )
        .bind(&query.conversation_id)
        .bind(&query.tenant_id)
        .bind(&query.user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .into_iter()
        .map(|run_id| RunId::new(run_id).map_err(|_| invalid_data()))
        .collect()
    }

    async fn create_conversation_turn(
        &self,
        command: NewConversationTurn,
    ) -> Result<ConversationTurnOutcome, RepositoryError> {
        validate_admission(&command.admission)?;
        validate_text(&command.user_id, 256)?;
        validate_text(&command.message.message_id, 256)?;
        let requested_conversation = command
            .admission
            .conversation
            .as_ref()
            .ok_or_else(invalid_data)?;
        let query = ConversationQuery {
            conversation_id: requested_conversation.conversation_id.clone(),
            tenant_id: command.admission.tenant_id.clone(),
            user_id: command.user_id.clone(),
        };
        validate_query(&query)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(admission) = load_admission_by_request(
            &mut transaction,
            &command.admission.tenant_id,
            &command.admission.request_id,
        )
        .await?
        {
            if !conversation_admission_intent_matches(&admission, &command.admission) {
                return Err(constraint_conflict());
            }
            let conversation = admission
                .conversation
                .as_ref()
                .filter(|conversation| conversation.conversation_id == query.conversation_id)
                .ok_or_else(constraint_conflict)?;
            let stored_conversation = require_conversation(&mut transaction, &query, false)
                .await?
                .ok_or_else(conversation_ownership_mismatch)?;
            if stored_conversation.agent_id != admission.agent_id {
                return Err(conversation_ownership_mismatch());
            }
            let user_message = load_message(
                &mut transaction,
                &query.conversation_id,
                &conversation.user_message_id,
            )
            .await?
            .ok_or_else(invalid_data)?;
            if !message_intent_matches(&user_message, &command.message) {
                return Err(constraint_conflict());
            }
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(ConversationTurnOutcome {
                admission,
                user_message,
                replayed: true,
            });
        }
        let conversation = require_conversation(&mut transaction, &query, true)
            .await?
            .ok_or_else(conversation_ownership_mismatch)?;
        if conversation.agent_id != command.admission.agent_id {
            return Err(conversation_ownership_mismatch());
        }
        if conversation.archived_at.is_some() {
            return Err(conversation_archived());
        }
        require_active_owner(&mut transaction, &command.admission.owner).await?;
        if let ConversationContent::Ref(reference) = &command.message.content {
            super::staging_postgres::consume_terminal_artifact_stage(
                &mut transaction,
                &command.admission.tenant_id,
                reference,
                TerminalArtifactSourceKind::UserMessage,
                &command.message.message_id,
            )
            .await?;
        }
        let inserted = insert_admission(&mut transaction, &command.admission).await?;
        if !inserted {
            let admission = load_admission_by_request(
                &mut transaction,
                &command.admission.tenant_id,
                &command.admission.request_id,
            )
            .await?
            .ok_or_else(constraint_conflict)?;
            if !conversation_admission_intent_matches(&admission, &command.admission) {
                return Err(constraint_conflict());
            }
            let conversation = admission
                .conversation
                .as_ref()
                .filter(|candidate| candidate.conversation_id == query.conversation_id)
                .ok_or_else(constraint_conflict)?;
            let user_message = load_message(
                &mut transaction,
                &query.conversation_id,
                &conversation.user_message_id,
            )
            .await?
            .ok_or_else(invalid_data)?;
            if !message_intent_matches(&user_message, &command.message) {
                return Err(constraint_conflict());
            }
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(ConversationTurnOutcome {
                admission,
                user_message,
                replayed: true,
            });
        }
        if requested_conversation.user_message_id != command.message.message_id {
            return Err(invalid_data());
        }
        let user_message = insert_message(
            &mut transaction,
            &query.conversation_id,
            &command.admission.run_id,
            ConversationRole::User,
            &command.message,
        )
        .await?;
        let admission = load_admission_by_request(
            &mut transaction,
            &command.admission.tenant_id,
            &command.admission.request_id,
        )
        .await?
        .ok_or_else(constraint_conflict)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(ConversationTurnOutcome {
            admission,
            user_message,
            replayed: false,
        })
    }

    async fn commit_conversation_turn(
        &self,
        command: CommitConversationTurn,
    ) -> Result<ConversationTerminalCommitOutcome, RepositoryError> {
        validate_result(&command.result)?;
        validate_text(&command.assistant_message.message_id, 256)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let admission_row = sqlx::query(
            "SELECT tenant_id,owner_instance_id,owner_epoch,conversation_id
             FROM terminal_run_admissions WHERE run_id=$1
             FOR UPDATE",
        )
        .bind(command.result.run_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(run_not_found)?;
        let conversation_id = admission_row
            .try_get::<Option<String>, _>("conversation_id")
            .map_err(|_| invalid_data())?
            .ok_or_else(invalid_data)?;
        if let Some(result) = load_result(&mut transaction, &command.result.run_id).await? {
            if !result_matches(&result, &command.result) {
                return Err(constraint_conflict());
            }
            let assistant_message =
                load_assistant_message(&mut transaction, &conversation_id, &command.result.run_id)
                    .await?
                    .ok_or_else(invalid_data)?;
            if !assistant_intent_matches(&assistant_message, &command.assistant_message) {
                return Err(constraint_conflict());
            }
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(ConversationTerminalCommitOutcome {
                result,
                assistant_message,
                replayed: true,
            });
        }
        let owner = RuntimeOwner {
            instance_id: admission_row
                .try_get("owner_instance_id")
                .map_err(|_| invalid_data())?,
            owner_epoch: admission_row
                .try_get("owner_epoch")
                .map_err(|_| invalid_data())?,
        };
        if owner != command.result.owner {
            return Err(lease_lost());
        }
        check_active_owner(&mut transaction, &command.result.owner).await?;
        let tenant_id = admission_row
            .try_get::<String, _>("tenant_id")
            .map_err(|_| invalid_data())?;
        if let Some(reference) = command.result.output_ref.as_deref() {
            super::staging_postgres::consume_terminal_artifact_stage(
                &mut transaction,
                &tenant_id,
                reference,
                TerminalArtifactSourceKind::RunOutput,
                command.result.run_id.as_str(),
            )
            .await?;
        }
        if let ConversationContent::Ref(reference) = &command.assistant_message.content {
            super::staging_postgres::consume_terminal_artifact_stage(
                &mut transaction,
                &tenant_id,
                reference,
                TerminalArtifactSourceKind::AssistantMessage,
                &command.assistant_message.message_id,
            )
            .await?;
        }
        let assistant_message = insert_message(
            &mut transaction,
            &conversation_id,
            &command.result.run_id,
            ConversationRole::Assistant,
            &command.assistant_message,
        )
        .await?;
        check_active_owner(&mut transaction, &command.result.owner).await?;
        let inserted = insert_result(&mut transaction, &command.result).await?;
        require_active_owner(&mut transaction, &command.result.owner).await?;
        let result = load_result(&mut transaction, &command.result.run_id)
            .await?
            .ok_or_else(constraint_conflict)?;
        if !result_matches(&result, &command.result) {
            return Err(constraint_conflict());
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(ConversationTerminalCommitOutcome {
            result,
            assistant_message,
            replayed: !inserted,
        })
    }

    async fn page_conversation_messages(
        &self,
        query: MessagePageQuery,
    ) -> Result<Option<ConversationMessagePage>, RepositoryError> {
        validate_query(&query.conversation)?;
        if query.limit == 0 || query.limit > 200 {
            return Err(invalid_data());
        }
        if let Some(cursor) = &query.before {
            validate_text(&cursor.message_id, 256)?;
            if cursor.message_order < 1 {
                return Err(invalid_data());
            }
        }
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if require_conversation(&mut transaction, &query.conversation, false)
            .await?
            .is_none()
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(None);
        }
        let fetch_limit = i64::from(query.limit) + 1;
        let rows =
            match &query.before {
                Some(cursor) => sqlx::query(
                    "SELECT message_id,conversation_id,message_order,role,run_id,content_inline,
                            content_ref,content_hash,created_at
                     FROM conversation_messages
                     WHERE conversation_id=$1
                       AND (message_order,message_id)<($2,$3)
                     ORDER BY message_order DESC,message_id DESC
                     LIMIT $4",
                )
                .bind(&query.conversation.conversation_id)
                .bind(cursor.message_order)
                .bind(&cursor.message_id)
                .bind(fetch_limit)
                .fetch_all(&mut *transaction)
                .await,
                None => sqlx::query(
                    "SELECT message_id,conversation_id,message_order,role,run_id,content_inline,
                            content_ref,content_hash,created_at
                     FROM conversation_messages
                     WHERE conversation_id=$1
                     ORDER BY message_order DESC,message_id DESC
                     LIMIT $2",
                )
                .bind(&query.conversation.conversation_id)
                .bind(fetch_limit)
                .fetch_all(&mut *transaction)
                .await,
            }
            .map_err(RepositoryError::storage)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        let mut messages = rows
            .iter()
            .map(decode_message)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = messages.len() > usize::try_from(query.limit).map_err(|_| invalid_data())?;
        if has_more {
            messages.pop();
        }
        let next_cursor =
            has_more
                .then(|| messages.last())
                .flatten()
                .map(|message| MessageCursor {
                    message_order: message.message_order,
                    message_id: message.message_id.clone(),
                });
        Ok(Some(ConversationMessagePage {
            messages,
            next_cursor,
        }))
    }

    async fn archive_conversation(
        &self,
        query: ConversationQuery,
        archived_at: DateTime<Utc>,
    ) -> Result<ArchiveOutcome, RepositoryError> {
        validate_query(&query)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let Some(conversation) = require_conversation(&mut transaction, &query, true).await? else {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(ArchiveOutcome::NotFound);
        };
        let changed = conversation.archived_at.is_none();
        if changed {
            sqlx::query(
                "UPDATE conversations SET archived_at=$2
                 WHERE conversation_id=$1 AND archived_at IS NULL",
            )
            .bind(&query.conversation_id)
            .bind(database_time(archived_at))
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        let row = sqlx::query(
            "SELECT conversation_id,tenant_id,user_id,agent_id,persistence_mode,
                    deployment_revision_id,created_at,archived_at
             FROM conversations WHERE conversation_id=$1",
        )
        .bind(&query.conversation_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let conversation = decode_conversation(&row)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(ArchiveOutcome::Archived {
            conversation,
            changed,
        })
    }

    async fn delete_conversation(
        &self,
        query: ConversationQuery,
    ) -> Result<PrivacyDeleteOutcome, RepositoryError> {
        validate_query(&query)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        sqlx::query(
            "SELECT pg_advisory_xact_lock(
                 hashtextextended($1, 6075990668891974961::bigint)
             )",
        )
        .bind(&query.conversation_id)
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if require_conversation(&mut transaction, &query, true)
            .await?
            .is_none()
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(PrivacyDeleteOutcome::NotFound);
        }
        let active_full_run = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 WITH RECURSIVE family(run_id) AS (
                     SELECT turn.run_id
                     FROM full_conversation_turns turn
                     WHERE turn.conversation_id=$1
                       AND turn.tenant_id=$2
                       AND turn.user_id=$3
                     UNION
                     SELECT lineage.run_id
                     FROM run_recovery_lineage lineage
                     JOIN family ON family.run_id=lineage.source_run_id
                 )
                 SELECT 1
                 FROM family
                 JOIN workflow_runs run ON run.run_id=family.run_id
                 WHERE run.lifecycle NOT IN (
                     'succeeded','failed','cancelled','interrupted','timed_out'
                 )
             )",
        )
        .bind(&query.conversation_id)
        .bind(&query.tenant_id)
        .bind(&query.user_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if active_full_run {
            return Err(constraint_conflict());
        }
        let mut content_refs = sqlx::query_scalar::<_, String>(
            "SELECT content_ref FROM conversation_messages
             WHERE conversation_id=$1 AND content_ref IS NOT NULL
             ORDER BY message_order",
        )
        .bind(&query.conversation_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        content_refs.extend(
            sqlx::query_scalar::<_, String>(
                "SELECT input_ref AS object_ref
                 FROM terminal_run_admissions
                 WHERE conversation_id=$1 AND input_ref IS NOT NULL
                 UNION
                 SELECT r.output_ref AS object_ref
                 FROM terminal_run_admissions a
                 JOIN terminal_run_results r ON r.run_id=a.run_id
                 WHERE a.conversation_id=$1 AND r.output_ref IS NOT NULL",
            )
            .bind(&query.conversation_id)
            .fetch_all(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?,
        );
        let mut summary_refs = sqlx::query_scalar::<_, String>(
            "SELECT summary_ref FROM conversation_summaries
             WHERE conversation_id=$1 ORDER BY through_message_order",
        )
        .bind(&query.conversation_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        enqueue_deletion_intents(
            &mut transaction,
            content_refs
                .iter()
                .chain(summary_refs.iter())
                .cloned()
                .map(|content_ref| DeletionIntent {
                    tenant_id: query.tenant_id.clone(),
                    content_ref,
                    source_kind: ContentDeletionSourceKind::ConversationPrivacy,
                    source_id: query.conversation_id.clone(),
                })
                .collect(),
            database_time(Utc::now()),
        )
        .await?;
        sqlx::query(
            "INSERT INTO conversation_tombstones(conversation_id,deleted_at)
             VALUES ($1,$2)
             ON CONFLICT(conversation_id) DO NOTHING",
        )
        .bind(&query.conversation_id)
        .bind(database_time(Utc::now()))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        sqlx::query("DELETE FROM conversations WHERE conversation_id=$1")
            .bind(&query.conversation_id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        content_refs.sort();
        content_refs.dedup();
        summary_refs.sort();
        summary_refs.dedup();
        Ok(PrivacyDeleteOutcome::Deleted {
            content: DeletedConversationContent {
                content_refs,
                summary_refs,
            },
        })
    }

    async fn put_conversation_summary(
        &self,
        summary: NewConversationSummary,
    ) -> Result<SummaryOutcome, RepositoryError> {
        validate_query(&summary.conversation)?;
        validate_text(&summary.summary_ref, 16 * 1024)?;
        validate_text(&summary.model_revision, 256)?;
        if summary.through_message_order < 1 {
            return Err(invalid_data());
        }
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if require_conversation(&mut transaction, &summary.conversation, false)
            .await?
            .is_none()
        {
            return Err(conversation_ownership_mismatch());
        }
        let boundary_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1 FROM conversation_messages
                 WHERE conversation_id=$1 AND message_order=$2
             )",
        )
        .bind(&summary.conversation.conversation_id)
        .bind(summary.through_message_order)
        .fetch_one(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        if !boundary_exists {
            return Err(invalid_data());
        }
        super::staging_postgres::consume_terminal_artifact_stage(
            &mut transaction,
            &summary.conversation.tenant_id,
            &summary.summary_ref,
            TerminalArtifactSourceKind::ConversationSummary,
            &format!(
                "{}:{}",
                summary.conversation.conversation_id, summary.through_message_order
            ),
        )
        .await?;
        let inserted = sqlx::query(
            "INSERT INTO conversation_summaries (
                 conversation_id,through_message_order,summary_ref,summary_hash,
                 model_revision,created_at
             ) VALUES ($1,$2,$3,$4,$5,$6)
             ON CONFLICT DO NOTHING",
        )
        .bind(&summary.conversation.conversation_id)
        .bind(summary.through_message_order)
        .bind(&summary.summary_ref)
        .bind(summary.summary_hash.as_str())
        .bind(&summary.model_revision)
        .bind(database_time(summary.created_at))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected()
            == 1;
        let stored = load_summary(
            &mut transaction,
            &summary.conversation.conversation_id,
            summary.through_message_order,
        )
        .await?
        .ok_or_else(invalid_data)?;
        if stored.summary_ref != summary.summary_ref
            || stored.summary_hash != summary.summary_hash
            || stored.model_revision != summary.model_revision
        {
            return Err(constraint_conflict());
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(SummaryOutcome {
            summary: stored,
            replayed: !inserted,
        })
    }

    async fn try_claim_conversation_summary_job(
        &self,
        command: ClaimConversationSummaryJob,
    ) -> Result<bool, RepositoryError> {
        validate_query(&command.conversation)?;
        validate_text(&command.claim_token, 256)?;
        validate_text(&command.claimed_by, 256)?;
        if command.claim_expires_at <= command.created_at {
            return Err(invalid_data());
        }
        Ok(sqlx::query(
            "INSERT INTO conversation_summary_jobs(
                 conversation_id,claim_token,claimed_by,claim_expires_at,created_at
             )
             SELECT conversation_id,$4,$5,$6,$7
             FROM conversations
             WHERE conversation_id=$1 AND tenant_id=$2 AND user_id=$3
             ON CONFLICT(conversation_id) DO UPDATE
             SET claim_token=EXCLUDED.claim_token,
                 claimed_by=EXCLUDED.claimed_by,
                 claim_expires_at=EXCLUDED.claim_expires_at,
                 created_at=EXCLUDED.created_at
             WHERE conversation_summary_jobs.claim_expires_at<=clock_timestamp()
                OR (
                    conversation_summary_jobs.claim_token=EXCLUDED.claim_token
                    AND conversation_summary_jobs.claimed_by=EXCLUDED.claimed_by
                )",
        )
        .bind(&command.conversation.conversation_id)
        .bind(&command.conversation.tenant_id)
        .bind(&command.conversation.user_id)
        .bind(&command.claim_token)
        .bind(&command.claimed_by)
        .bind(database_time(command.claim_expires_at))
        .bind(database_time(command.created_at))
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected()
            == 1)
    }

    async fn release_conversation_summary_job(
        &self,
        command: ReleaseConversationSummaryJob,
    ) -> Result<bool, RepositoryError> {
        validate_text(&command.conversation_id, 256)?;
        validate_text(&command.claim_token, 256)?;
        validate_text(&command.claimed_by, 256)?;
        Ok(sqlx::query(
            "DELETE FROM conversation_summary_jobs
             WHERE conversation_id=$1 AND claim_token=$2 AND claimed_by=$3",
        )
        .bind(command.conversation_id)
        .bind(command.claim_token)
        .bind(command.claimed_by)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected()
            == 1)
    }

    async fn latest_conversation_summary(
        &self,
        query: ConversationQuery,
    ) -> Result<Option<ConversationSummary>, RepositoryError> {
        validate_query(&query)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if require_conversation(&mut transaction, &query, false)
            .await?
            .is_none()
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(None);
        }
        let summary = sqlx::query(
            "SELECT conversation_id,through_message_order,summary_ref,summary_hash,
                    model_revision,created_at
             FROM conversation_summaries
             WHERE conversation_id=$1
             ORDER BY through_message_order DESC
             LIMIT 1",
        )
        .bind(&query.conversation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .map(|row| decode_summary(&row))
        .transpose()?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(summary)
    }

    async fn load_conversation_context(
        &self,
        query: ConversationContextQuery,
    ) -> Result<Option<ConversationContext>, RepositoryError> {
        validate_query(&query.conversation)?;
        if query.recent_message_limit == 0 || query.recent_message_limit > 200 {
            return Err(invalid_data());
        }
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if require_conversation(&mut transaction, &query.conversation, false)
            .await?
            .is_none()
        {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(None);
        }
        let summary = sqlx::query(
            "SELECT conversation_id,through_message_order,summary_ref,summary_hash,
                    model_revision,created_at
             FROM conversation_summaries
             WHERE conversation_id=$1
             ORDER BY through_message_order DESC
             LIMIT 1",
        )
        .bind(&query.conversation.conversation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .map(|row| decode_summary(&row))
        .transpose()?;
        let boundary = summary
            .as_ref()
            .map_or(0, |summary| summary.through_message_order);
        let rows = sqlx::query(
            "SELECT message_id,conversation_id,message_order,role,run_id,content_inline,
                    content_ref,content_hash,created_at
             FROM conversation_messages
             WHERE conversation_id=$1 AND message_order>$2
             ORDER BY message_order DESC
             LIMIT $3",
        )
        .bind(&query.conversation.conversation_id)
        .bind(boundary)
        .bind(i64::from(query.recent_message_limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        let mut messages = rows
            .iter()
            .map(decode_message)
            .collect::<Result<Vec<_>, _>>()?;
        messages.reverse();
        Ok(Some(ConversationContext { summary, messages }))
    }

    async fn delete_conversations_before(
        &self,
        retention: BoundedRetention,
    ) -> Result<ConversationRetentionOutcome, RepositoryError> {
        let limit = validate_retention(&retention)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let candidates = sqlx::query_as::<_, (String, String)>(
            "SELECT conversation_id,tenant_id
             FROM conversations
             WHERE created_at<$1
             ORDER BY created_at,conversation_id
             LIMIT $2",
        )
        .bind(database_time(retention.before))
        .bind(limit)
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let mut conversations = Vec::with_capacity(candidates.len());
        for (conversation_id, _) in candidates {
            sqlx::query(
                "SELECT pg_advisory_xact_lock(
                     hashtextextended($1, 6075990668891974961::bigint)
                 )",
            )
            .bind(&conversation_id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if let Some(conversation) = sqlx::query_as::<_, (String, String)>(
                "SELECT conversation_id,tenant_id
                 FROM conversations
                 WHERE conversation_id=$1 AND created_at<$2
                   AND NOT EXISTS (
                       SELECT 1
                       FROM terminal_run_admissions admission
                       JOIN terminal_runtime_instances owner
                         ON owner.instance_id=admission.owner_instance_id
                        AND owner.owner_epoch=admission.owner_epoch
                        AND owner.started_at>=pg_postmaster_start_time()
                       LEFT JOIN terminal_run_results result
                         ON result.run_id=admission.run_id
                       WHERE admission.conversation_id=conversations.conversation_id
                         AND result.run_id IS NULL
                         AND owner.lease_expires_at>clock_timestamp()
                   )
                   AND NOT EXISTS (
                       WITH RECURSIVE family(run_id) AS (
                           SELECT turn.run_id
                           FROM full_conversation_turns turn
                           WHERE turn.conversation_id=conversations.conversation_id
                           UNION
                           SELECT lineage.run_id
                           FROM run_recovery_lineage lineage
                           JOIN family ON family.run_id=lineage.source_run_id
                       )
                       SELECT 1
                       FROM family
                       JOIN workflow_runs run ON run.run_id=family.run_id
                       WHERE run.lifecycle NOT IN (
                           'succeeded','failed','cancelled','interrupted','timed_out'
                       )
                   )
                 FOR UPDATE",
            )
            .bind(&conversation_id)
            .bind(database_time(retention.before))
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            {
                conversations.push(conversation);
            }
        }
        let ids = conversations
            .iter()
            .map(|(conversation_id, _)| conversation_id.clone())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            transaction
                .commit()
                .await
                .map_err(RepositoryError::storage)?;
            return Ok(ConversationRetentionOutcome {
                deleted: 0,
                content_refs: Vec::new(),
                summary_refs: Vec::new(),
            });
        }
        let tenant_by_conversation = conversations.into_iter().collect::<HashMap<_, _>>();
        let content_rows = sqlx::query_as::<_, (String, String)>(
            "SELECT conversation_id,content_ref AS object_ref FROM conversation_messages
             WHERE conversation_id=ANY($1) AND content_ref IS NOT NULL
             UNION
             SELECT conversation_id,input_ref AS object_ref FROM terminal_run_admissions
             WHERE conversation_id=ANY($1) AND input_ref IS NOT NULL
             UNION
             SELECT a.conversation_id,r.output_ref AS object_ref
             FROM terminal_run_admissions a
             JOIN terminal_run_results r ON r.run_id=a.run_id
             WHERE a.conversation_id=ANY($1) AND r.output_ref IS NOT NULL
             ORDER BY 1,2",
        )
        .bind(&ids)
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let summary_rows = sqlx::query_as::<_, (String, String)>(
            "SELECT conversation_id,summary_ref FROM conversation_summaries
             WHERE conversation_id=ANY($1)
             ORDER BY conversation_id,through_message_order",
        )
        .bind(&ids)
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let mut deletion_intents = Vec::with_capacity(content_rows.len() + summary_rows.len());
        for (conversation_id, content_ref) in content_rows.iter().chain(summary_rows.iter()) {
            deletion_intents.push(DeletionIntent {
                tenant_id: tenant_by_conversation
                    .get(conversation_id)
                    .ok_or_else(invalid_data)?
                    .clone(),
                content_ref: content_ref.clone(),
                source_kind: ContentDeletionSourceKind::ConversationRetention,
                source_id: conversation_id.clone(),
            });
        }
        enqueue_deletion_intents(
            &mut transaction,
            deletion_intents,
            database_time(Utc::now()),
        )
        .await?;
        sqlx::query(
            "INSERT INTO conversation_tombstones(conversation_id,deleted_at)
             SELECT unnest($1::text[]),$2
             ON CONFLICT(conversation_id) DO NOTHING",
        )
        .bind(&ids)
        .bind(database_time(Utc::now()))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let deleted = sqlx::query("DELETE FROM conversations WHERE conversation_id=ANY($1)")
            .bind(&ids)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        let mut content_refs = content_rows
            .into_iter()
            .map(|(_, content_ref)| content_ref)
            .collect::<Vec<_>>();
        let mut summary_refs = summary_rows
            .into_iter()
            .map(|(_, summary_ref)| summary_ref)
            .collect::<Vec<_>>();
        content_refs.sort();
        content_refs.dedup();
        summary_refs.sort();
        summary_refs.dedup();
        Ok(ConversationRetentionOutcome {
            deleted,
            content_refs,
            summary_refs,
        })
    }
}

#[allow(dead_code)]
fn _pool_is_send_sync(_: &PgPool) {}
