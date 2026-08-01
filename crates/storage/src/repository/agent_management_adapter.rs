use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgRow, AssertSqlSafe, Postgres, Row, Sqlite, Transaction};
use uuid::Uuid;

use insight_durable::{
    ActivateManagedAgentDeploymentCommand, AgentAuthoringMode, AgentDebugRuntimeCount,
    AgentDebugSession, AgentDebugStatus, AgentDefinitionRevision, AgentDeploymentResolution,
    AgentDeploymentRevision, AgentLifecycle, AgentManagementConflict,
    AgentManagementDurableRepository, AgentManagementOperationCount, AgentManagementPage,
    AgentManagementRuntimeStats, AgentManagementWriteError, AgentMutationMetadata,
    AgentMutationReceipt, AgentOperationStatus, AgentStoredDraft, AgentStoredDraftView,
    AgentValidationReport, ArchiveAgentCommand, CancelAgentDebugSessionCommand,
    CompleteAgentDebugSessionCommand, CreateAgentCommand, CreateAgentDebugSessionCommand,
    CreateAgentResolutionCommand, CreateAgentValidationCommand, DeactivateManagedAgentCommand,
    DeleteAgentCommand, InstallAgentDeploymentCommand, ManagedAgent, PublicationHead,
    PublicationOrigin, PublishAgentDefinitionCommand, RecordAgentManagementRejectionCommand,
    ReplaceAgentDraftCommand, ReplaceAgentDraftViewCommand, RepositoryError, RestoreAgentCommand,
    UpdateAgentLabelsCommand,
};

use super::model::VersionedPlanAdapter as _;
use super::{
    database_time,
    postgres::{
        begin_write_transaction, install_plan as install_postgres_plan,
        PlanInstallScope as PostgresPlanInstallScope,
    },
    sqlite::{install_plan as install_sqlite_plan, PlanInstallScope as SqlitePlanInstallScope},
    PostgresDurableRepository, RepositoryErrorExt as _, SqliteDurableRepository,
};

fn storage(error: sqlx::Error) -> AgentManagementWriteError {
    AgentManagementWriteError::Repository(RepositoryError::storage(error))
}

fn invalid_data() -> RepositoryError {
    RepositoryError::invalid_data()
}

fn decode_created_cursor(
    cursor: Option<&str>,
) -> Result<Option<(DateTime<Utc>, String)>, RepositoryError> {
    cursor
        .map(|cursor| {
            let (created_at, stable_id) = cursor.split_once('|').ok_or_else(invalid_data)?;
            if stable_id.is_empty() || stable_id.contains('|') {
                return Err(invalid_data());
            }
            let created_at = DateTime::parse_from_rfc3339(created_at)
                .map_err(|_| invalid_data())?
                .with_timezone(&Utc);
            Ok((created_at, stable_id.to_owned()))
        })
        .transpose()
}

fn encode_created_cursor(created_at: DateTime<Utc>, stable_id: &str) -> String {
    format!("{}|{stable_id}", created_at.to_rfc3339())
}

fn encode_json(value: &impl Serialize) -> Result<String, RepositoryError> {
    serde_jcs::to_string(value).map_err(|_| invalid_data())
}

fn decode_json(value: &str) -> Result<Value, RepositoryError> {
    serde_json::from_str(value).map_err(|_| invalid_data())
}

fn prefixed_sha256(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut output = String::from("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn collect_activation_dependencies(
    value: &Value,
    providers: &mut BTreeMap<String, String>,
    mcp_servers: &mut BTreeSet<String>,
) -> Result<(), RepositoryError> {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_activation_dependencies(value, providers, mcp_servers)?;
            }
        }
        Value::Object(object) => {
            if let (Some(provider_id), Some(revision_id)) = (
                object.get("provider_id").and_then(Value::as_str),
                object.get("provider_revision_id").and_then(Value::as_str),
            ) {
                if providers
                    .insert(provider_id.to_owned(), revision_id.to_owned())
                    .is_some_and(|existing| existing != revision_id)
                {
                    return Err(invalid_data());
                }
            }
            if let Some(action_id) = object.get("action_id").and_then(Value::as_str) {
                if let Some(server_id) = action_id
                    .strip_prefix("mcp.")
                    .and_then(|value| value.split('.').next())
                    .filter(|value| !value.is_empty())
                {
                    mcp_servers.insert(server_id.to_owned());
                }
            }
            for value in object.values() {
                collect_activation_dependencies(value, providers, mcp_servers)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn publication_origin(value: &str) -> Result<PublicationOrigin, RepositoryError> {
    match value {
        "built_in" => Ok(PublicationOrigin::BuiltIn),
        "graph" => Ok(PublicationOrigin::Graph),
        "managed" => Ok(PublicationOrigin::Managed),
        _ => Err(invalid_data()),
    }
}

fn debug_draft_pin(source: &Value) -> Option<(u64, &str)> {
    let selection = source.get("selection")?;
    (selection.get("type")?.as_str()? == "draft").then_some((
        selection.get("draft_version")?.as_u64()?,
        source.get("source_author_hash")?.as_str()?,
    ))
}

fn u64_to_i64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| invalid_data())
}

fn i64_to_u64(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| invalid_data())
}

fn authoring_mode(value: &str) -> Result<AgentAuthoringMode, RepositoryError> {
    match value {
        "yaml_package" => Ok(AgentAuthoringMode::YamlPackage),
        "graph" => Ok(AgentAuthoringMode::Graph),
        _ => Err(invalid_data()),
    }
}

fn lifecycle(value: &str) -> Result<AgentLifecycle, RepositoryError> {
    match value {
        "editable" => Ok(AgentLifecycle::Editable),
        "archived" => Ok(AgentLifecycle::Archived),
        _ => Err(invalid_data()),
    }
}

fn operation_status(value: &str) -> Result<AgentOperationStatus, RepositoryError> {
    match value {
        "queued" => Ok(AgentOperationStatus::Queued),
        "running" => Ok(AgentOperationStatus::Running),
        "succeeded" => Ok(AgentOperationStatus::Succeeded),
        "failed" => Ok(AgentOperationStatus::Failed),
        "cancelled" => Ok(AgentOperationStatus::Cancelled),
        _ => Err(invalid_data()),
    }
}

fn debug_status(value: &str) -> Result<AgentDebugStatus, RepositoryError> {
    match value {
        "queued" => Ok(AgentDebugStatus::Queued),
        "running" => Ok(AgentDebugStatus::Running),
        "succeeded" => Ok(AgentDebugStatus::Succeeded),
        "failed" => Ok(AgentDebugStatus::Failed),
        "cancelled" => Ok(AgentDebugStatus::Cancelled),
        "expired" => Ok(AgentDebugStatus::Expired),
        _ => Err(invalid_data()),
    }
}

fn agent_etag(version: u64) -> String {
    format!("\"agent-{version}\"")
}

fn draft_etag(version: u64) -> String {
    format!("\"draft-{version}\"")
}

fn view_etag(version: u64) -> String {
    format!("\"view-{version}\"")
}

fn sqlite_agent(row: &sqlx::sqlite::SqliteRow) -> Result<ManagedAgent, RepositoryError> {
    let labels: String = row.try_get("labels").map_err(RepositoryError::storage)?;
    let archived: Option<String> = row
        .try_get("archived_publication_head")
        .map_err(RepositoryError::storage)?;
    Ok(ManagedAgent {
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        authoring_mode: authoring_mode(
            row.try_get::<String, _>("authoring_mode")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        labels: decode_json(&labels)?,
        lifecycle: lifecycle(
            row.try_get::<String, _>("lifecycle")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        entity_version: i64_to_u64(
            row.try_get("entity_version")
                .map_err(RepositoryError::storage)?,
        )?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        active_definition_revision_id: row
            .try_get("active_definition_revision_id")
            .map_err(RepositoryError::storage)?,
        active_deployment_revision_id: row
            .try_get("active_deployment_revision_id")
            .map_err(RepositoryError::storage)?,
        archived_publication_head: archived
            .map(|value| serde_json::from_str(&value).map_err(|_| invalid_data()))
            .transpose()?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        updated_at: row
            .try_get("updated_at")
            .map_err(RepositoryError::storage)?,
    })
}

fn sqlite_draft(row: &sqlx::sqlite::SqliteRow) -> Result<AgentStoredDraft, RepositoryError> {
    let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
    Ok(AgentStoredDraft {
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        author_hash: row
            .try_get("author_hash")
            .map_err(RepositoryError::storage)?,
        document: decode_json(&document)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        updated_at: row
            .try_get("updated_at")
            .map_err(RepositoryError::storage)?,
    })
}

fn sqlite_validation(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<AgentValidationReport, RepositoryError> {
    let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
    Ok(AgentValidationReport {
        validation_id: row
            .try_get("validation_id")
            .map_err(RepositoryError::storage)?,
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        author_hash: row
            .try_get("author_hash")
            .map_err(RepositoryError::storage)?,
        policy_digest: row
            .try_get("policy_digest")
            .map_err(RepositoryError::storage)?,
        status: operation_status(
            row.try_get::<String, _>("operation_status")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        semantic_hash: row
            .try_get("semantic_hash")
            .map_err(RepositoryError::storage)?,
        report_hash: row
            .try_get("report_hash")
            .map_err(RepositoryError::storage)?,
        document: decode_json(&document)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

fn sqlite_resolution(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<AgentDeploymentResolution, RepositoryError> {
    let json_column = |name| -> Result<Value, RepositoryError> {
        decode_json(
            &row.try_get::<String, _>(name)
                .map_err(RepositoryError::storage)?,
        )
    };
    Ok(AgentDeploymentResolution {
        resolution_id: row
            .try_get("resolution_id")
            .map_err(RepositoryError::storage)?,
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        definition_revision_id: row
            .try_get("definition_revision_id")
            .map_err(RepositoryError::storage)?,
        status: operation_status(
            row.try_get::<String, _>("operation_status")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        catalog_snapshot_hash: row
            .try_get("catalog_snapshot_hash")
            .map_err(RepositoryError::storage)?,
        resolution_hash: row
            .try_get("resolution_hash")
            .map_err(RepositoryError::storage)?,
        resolved_bindings: json_column("resolved_bindings")?,
        worker_contracts: json_column("worker_contracts")?,
        dependency_heads: json_column("dependency_heads")?,
        risks: json_column("risks")?,
        expires_at: row
            .try_get("expires_at")
            .map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

fn sqlite_debug(row: &sqlx::sqlite::SqliteRow) -> Result<AgentDebugSession, RepositoryError> {
    let source: String = row.try_get("source").map_err(RepositoryError::storage)?;
    Ok(AgentDebugSession {
        debug_session_id: row
            .try_get("debug_session_id")
            .map_err(RepositoryError::storage)?,
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        source: decode_json(&source)?,
        source_hash: row
            .try_get("source_hash")
            .map_err(RepositoryError::storage)?,
        execution_profile_id: row
            .try_get("execution_profile_id")
            .map_err(RepositoryError::storage)?,
        profile_mode: row
            .try_get("profile_mode")
            .map_err(RepositoryError::storage)?,
        status: debug_status(
            row.try_get::<String, _>("session_status")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        definition_revision_id: row
            .try_get("definition_revision_id")
            .map_err(RepositoryError::storage)?,
        deployment_revision_id: row
            .try_get("deployment_revision_id")
            .map_err(RepositoryError::storage)?,
        run_id: row.try_get("run_id").map_err(RepositoryError::storage)?,
        failure_code: row
            .try_get("failure_code")
            .map_err(RepositoryError::storage)?,
        expires_at: row
            .try_get("expires_at")
            .map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        finished_at: row
            .try_get("finished_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

async fn sqlite_begin(
    repository: &SqliteDurableRepository,
) -> Result<Transaction<'_, Sqlite>, AgentManagementWriteError> {
    repository
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(storage)
}

async fn sqlite_validate_activation_dependencies(
    transaction: &mut Transaction<'_, Sqlite>,
    resolved_bindings: &Value,
    dependency_heads: &Value,
) -> Result<(), AgentManagementWriteError> {
    let mut providers = BTreeMap::new();
    let mut mcp_servers = BTreeSet::new();
    collect_activation_dependencies(resolved_bindings, &mut providers, &mut mcp_servers)?;
    for (provider_id, revision_id) in providers {
        let state: Option<String> = sqlx::query_scalar(
            "SELECT p.operational_state FROM managed_providers p
             JOIN provider_revisions r ON r.provider_id=p.provider_id
             WHERE p.provider_id=? AND r.revision_id=?",
        )
        .bind(provider_id)
        .bind(revision_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage)?;
        if state.as_deref() != Some("enabled") {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
    }
    for server_id in mcp_servers {
        let state: Option<String> =
            sqlx::query_scalar("SELECT server_state FROM mcp_managed_servers WHERE server_id=?")
                .bind(server_id)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(storage)?;
        if state.as_deref() != Some("active") {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
    }
    let mut expected: Vec<PublicationHead> =
        serde_json::from_value(dependency_heads.clone()).map_err(|_| invalid_data())?;
    expected.sort_by(|left, right| left.agent_id().cmp(right.agent_id()));
    for expected in expected {
        let row = sqlx::query(
            "SELECT agent_id,definition_id,definition_revision_id,deployment_revision_id,
                    publication_origin FROM agent_publication_heads WHERE agent_id=?",
        )
        .bind(expected.agent_id())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage)?;
        let actual = row
            .as_ref()
            .map(|row| {
                insight_durable::AgentDeploymentTarget::new(
                    row.try_get("agent_id").map_err(RepositoryError::storage)?,
                    row.try_get("definition_id")
                        .map_err(RepositoryError::storage)?,
                    insight_engine::DefinitionRevisionId::new(
                        row.try_get::<String, _>("definition_revision_id")
                            .map_err(RepositoryError::storage)?,
                    )
                    .map_err(|_| invalid_data())?,
                    insight_engine::DeploymentRevisionId::new(
                        row.try_get::<String, _>("deployment_revision_id")
                            .map_err(RepositoryError::storage)?,
                    )
                    .map_err(|_| invalid_data())?,
                    publication_origin(
                        &row.try_get::<String, _>("publication_origin")
                            .map_err(RepositoryError::storage)?,
                    )?,
                )
                .map(|target| target.publication_head())?
            })
            .transpose()?;
        if actual.as_ref() != Some(&expected) {
            return Err(conflict(AgentManagementConflict::DependencyHeadChanged));
        }
    }
    Ok(())
}

async fn sqlite_replay(
    transaction: &mut Transaction<'_, Sqlite>,
    metadata: &AgentMutationMetadata,
) -> Result<Option<AgentMutationReceipt>, AgentManagementWriteError> {
    let row = sqlx::query(
        "SELECT request_hash,response_status,response_json,response_etag
         FROM agent_management_requests
         WHERE operator_id=? AND method=? AND canonical_path=? AND request_id=?",
    )
    .bind(&metadata.operator_id)
    .bind(&metadata.method)
    .bind(&metadata.canonical_path)
    .bind(&metadata.request_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage)?;
    let Some(row) = row else { return Ok(None) };
    if row.try_get::<String, _>("request_hash").map_err(storage)? != metadata.request_hash {
        return Err(AgentManagementWriteError::Conflict(
            AgentManagementConflict::IdempotencyKeyReused,
        ));
    }
    let response: String = row.try_get("response_json").map_err(storage)?;
    Ok(Some(AgentMutationReceipt {
        replayed: true,
        status: u16::try_from(row.try_get::<i64, _>("response_status").map_err(storage)?)
            .map_err(|_| AgentManagementWriteError::Repository(invalid_data()))?,
        response: decode_json(&response)?,
        etag: row.try_get("response_etag").map_err(storage)?,
    }))
}

struct SqliteFinalize<'a> {
    event_kind: &'a str,
    agent_id: &'a str,
    subject_id: &'a str,
    before_hash: Option<&'a str>,
    after_hash: Option<&'a str>,
    status: u16,
    response: Value,
    etag: Option<String>,
}

async fn sqlite_finalize(
    transaction: &mut Transaction<'_, Sqlite>,
    metadata: &AgentMutationMetadata,
    value: SqliteFinalize<'_>,
) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
    sqlx::query(
        "INSERT INTO agent_management_requests(
           operator_id,method,canonical_path,request_id,request_hash,response_status,
           response_json,response_etag,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
    )
    .bind(&metadata.operator_id)
    .bind(&metadata.method)
    .bind(&metadata.canonical_path)
    .bind(&metadata.request_id)
    .bind(&metadata.request_hash)
    .bind(i64::from(value.status))
    .bind(encode_json(&value.response)?)
    .bind(&value.etag)
    .bind(database_time(metadata.now))
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    sqlx::query(
        "INSERT INTO agent_management_audit_events(
           event_kind,agent_id,subject_id,actor_id,capability,request_id_hash,before_hash,
           after_hash,result_code,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
    )
    .bind(value.event_kind)
    .bind(value.agent_id)
    .bind(value.subject_id)
    .bind(&metadata.operator_id)
    .bind(&metadata.capability)
    .bind(prefixed_sha256(metadata.request_id.as_bytes()))
    .bind(value.before_hash)
    .bind(value.after_hash)
    .bind(if value.status < 300 {
        "accepted"
    } else {
        "rejected"
    })
    .bind(database_time(metadata.now))
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    sqlx::query(
        "INSERT INTO agent_management_outbox(
           event_id,event_kind,agent_id,subject_id,safe_payload,created_at,delivered_at)
         VALUES(?,?,?,?,?,?,NULL)",
    )
    .bind(format!("aout_{}", Uuid::new_v4().simple()))
    .bind(value.event_kind)
    .bind(value.agent_id)
    .bind(value.subject_id)
    .bind(encode_json(&json!({
        "agent_id": value.agent_id,
        "subject_id": value.subject_id,
        "result_code": if value.status < 300 { "accepted" } else { "rejected" }
    }))?)
    .bind(database_time(metadata.now))
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    Ok(AgentMutationReceipt {
        replayed: false,
        status: value.status,
        response: value.response,
        etag: value.etag,
    })
}

fn conflict(value: AgentManagementConflict) -> AgentManagementWriteError {
    AgentManagementWriteError::Conflict(value)
}

fn sqlite_agent_select() -> &'static str {
    "SELECT agent_id,authoring_mode,labels,lifecycle,entity_version,draft_version,
            active_definition_revision_id,active_deployment_revision_id,
            archived_publication_head,created_at,updated_at FROM managed_agents"
}

fn sqlite_validation_select() -> &'static str {
    "SELECT validation_id,agent_id,draft_version,author_hash,policy_digest,
            operation_status,semantic_hash,report_hash,document,created_at,created_by
     FROM agent_validations"
}

fn sqlite_resolution_select() -> &'static str {
    "SELECT resolution_id,agent_id,definition_revision_id,operation_status,
            catalog_snapshot_hash,resolution_hash,resolved_bindings,worker_contracts,
            dependency_heads,risks,expires_at,created_at,created_by
     FROM agent_deployment_resolutions"
}

fn sqlite_debug_select() -> &'static str {
    "SELECT debug_session_id,agent_id,source,source_hash,execution_profile_id,
            profile_mode,session_status,definition_revision_id,deployment_revision_id,
            run_id,failure_code,expires_at,created_at,finished_at,created_by
     FROM agent_debug_sessions"
}

fn sqlite_definition(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<AgentDefinitionRevision, RepositoryError> {
    let json_column = |name| -> Result<Value, RepositoryError> {
        decode_json(
            &row.try_get::<String, _>(name)
                .map_err(RepositoryError::storage)?,
        )
    };
    Ok(AgentDefinitionRevision {
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        definition_id: row
            .try_get("definition_id")
            .map_err(RepositoryError::storage)?,
        definition_revision_id: row
            .try_get("definition_revision_id")
            .map_err(RepositoryError::storage)?,
        revision_number: i64_to_u64(
            row.try_get("revision_number")
                .map_err(RepositoryError::storage)?,
        )?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        validation_id: row
            .try_get("validation_id")
            .map_err(RepositoryError::storage)?,
        author_hash: row
            .try_get("author_hash")
            .map_err(RepositoryError::storage)?,
        semantic_hash: row.try_get("plan_hash").map_err(RepositoryError::storage)?,
        compiler_version: row
            .try_get("compiler_version")
            .map_err(RepositoryError::storage)?,
        expression_engine_version: row
            .try_get("expression_engine_version")
            .map_err(RepositoryError::storage)?,
        author_document: json_column("author_document")?,
        canonical_plan: json_column("canonical_plan")?,
        descriptor_contracts: json_column("descriptor_contracts")?,
        display_name: row
            .try_get("display_name")
            .map_err(RepositoryError::storage)?,
        public_description: row
            .try_get("public_description")
            .map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

fn sqlite_definition_select() -> &'static str {
    "SELECT p.agent_id,p.definition_id,p.definition_revision_id,p.revision_number,
            p.source_draft_version,p.validation_id,p.author_hash,p.created_at,p.created_by,
            r.plan_hash,r.compiler_version,r.expression_engine_version,r.author_document,
            r.canonical_plan,r.descriptor_contracts,m.display_name,m.public_description
     FROM agent_definition_publications p
     JOIN workflow_definition_revisions r
       ON r.definition_id=p.definition_id
      AND r.definition_revision_id=p.definition_revision_id
     JOIN workflow_definition_public_metadata m
       ON m.definition_id=p.definition_id
      AND m.definition_revision_id=p.definition_revision_id"
}

fn sqlite_deployment(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<AgentDeploymentRevision, RepositoryError> {
    Ok(AgentDeploymentRevision {
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        definition_id: row
            .try_get("definition_id")
            .map_err(RepositoryError::storage)?,
        definition_revision_id: row
            .try_get("definition_revision_id")
            .map_err(RepositoryError::storage)?,
        deployment_revision_id: row
            .try_get("deployment_revision_id")
            .map_err(RepositoryError::storage)?,
        resolution_id: row
            .try_get("resolution_id")
            .map_err(RepositoryError::storage)?,
        plan_hash: row.try_get("plan_hash").map_err(RepositoryError::storage)?,
        binding_hash: row
            .try_get("binding_hash")
            .map_err(RepositoryError::storage)?,
        resolved_bindings: decode_json(
            &row.try_get::<String, _>("resolved_bindings")
                .map_err(RepositoryError::storage)?,
        )?,
        worker_contracts: decode_json(
            &row.try_get::<String, _>("worker_contracts")
                .map_err(RepositoryError::storage)?,
        )?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

fn sqlite_deployment_select() -> &'static str {
    "SELECT p.agent_id,p.definition_id,p.definition_revision_id,p.deployment_revision_id,
            p.resolution_id,p.created_at,p.created_by,x.plan_hash,x.binding_hash,
            x.resolved_bindings,x.worker_contracts
     FROM agent_deployment_publications p
     JOIN deployment_revisions x
       ON x.definition_id=p.definition_id
      AND x.deployment_revision_id=p.deployment_revision_id"
}

fn postgres_agent(row: &PgRow) -> Result<ManagedAgent, RepositoryError> {
    let archived: Option<Value> = row
        .try_get("archived_publication_head")
        .map_err(RepositoryError::storage)?;
    Ok(ManagedAgent {
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        authoring_mode: authoring_mode(
            row.try_get::<String, _>("authoring_mode")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        labels: row.try_get("labels").map_err(RepositoryError::storage)?,
        lifecycle: lifecycle(
            row.try_get::<String, _>("lifecycle")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        entity_version: i64_to_u64(
            row.try_get("entity_version")
                .map_err(RepositoryError::storage)?,
        )?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        active_definition_revision_id: row
            .try_get("active_definition_revision_id")
            .map_err(RepositoryError::storage)?,
        active_deployment_revision_id: row
            .try_get("active_deployment_revision_id")
            .map_err(RepositoryError::storage)?,
        archived_publication_head: archived
            .map(|value| serde_json::from_value(value).map_err(|_| invalid_data()))
            .transpose()?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        updated_at: row
            .try_get("updated_at")
            .map_err(RepositoryError::storage)?,
    })
}

fn postgres_draft(row: &PgRow) -> Result<AgentStoredDraft, RepositoryError> {
    Ok(AgentStoredDraft {
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        author_hash: row
            .try_get("author_hash")
            .map_err(RepositoryError::storage)?,
        document: row.try_get("document").map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        updated_at: row
            .try_get("updated_at")
            .map_err(RepositoryError::storage)?,
    })
}

fn postgres_validation(row: &PgRow) -> Result<AgentValidationReport, RepositoryError> {
    Ok(AgentValidationReport {
        validation_id: row
            .try_get("validation_id")
            .map_err(RepositoryError::storage)?,
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        author_hash: row
            .try_get("author_hash")
            .map_err(RepositoryError::storage)?,
        policy_digest: row
            .try_get("policy_digest")
            .map_err(RepositoryError::storage)?,
        status: operation_status(
            row.try_get::<String, _>("operation_status")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        semantic_hash: row
            .try_get("semantic_hash")
            .map_err(RepositoryError::storage)?,
        report_hash: row
            .try_get("report_hash")
            .map_err(RepositoryError::storage)?,
        document: row.try_get("document").map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

fn postgres_resolution(row: &PgRow) -> Result<AgentDeploymentResolution, RepositoryError> {
    Ok(AgentDeploymentResolution {
        resolution_id: row
            .try_get("resolution_id")
            .map_err(RepositoryError::storage)?,
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        definition_revision_id: row
            .try_get("definition_revision_id")
            .map_err(RepositoryError::storage)?,
        status: operation_status(
            row.try_get::<String, _>("operation_status")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        catalog_snapshot_hash: row
            .try_get("catalog_snapshot_hash")
            .map_err(RepositoryError::storage)?,
        resolution_hash: row
            .try_get("resolution_hash")
            .map_err(RepositoryError::storage)?,
        resolved_bindings: row
            .try_get("resolved_bindings")
            .map_err(RepositoryError::storage)?,
        worker_contracts: row
            .try_get("worker_contracts")
            .map_err(RepositoryError::storage)?,
        dependency_heads: row
            .try_get("dependency_heads")
            .map_err(RepositoryError::storage)?,
        risks: row.try_get("risks").map_err(RepositoryError::storage)?,
        expires_at: row
            .try_get("expires_at")
            .map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

fn postgres_definition(row: &PgRow) -> Result<AgentDefinitionRevision, RepositoryError> {
    Ok(AgentDefinitionRevision {
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        definition_id: row
            .try_get("definition_id")
            .map_err(RepositoryError::storage)?,
        definition_revision_id: row
            .try_get("definition_revision_id")
            .map_err(RepositoryError::storage)?,
        revision_number: i64_to_u64(
            row.try_get("revision_number")
                .map_err(RepositoryError::storage)?,
        )?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        validation_id: row
            .try_get("validation_id")
            .map_err(RepositoryError::storage)?,
        author_hash: row
            .try_get("author_hash")
            .map_err(RepositoryError::storage)?,
        semantic_hash: row.try_get("plan_hash").map_err(RepositoryError::storage)?,
        compiler_version: row
            .try_get("compiler_version")
            .map_err(RepositoryError::storage)?,
        expression_engine_version: row
            .try_get("expression_engine_version")
            .map_err(RepositoryError::storage)?,
        author_document: row
            .try_get("author_document")
            .map_err(RepositoryError::storage)?,
        canonical_plan: row
            .try_get("canonical_plan")
            .map_err(RepositoryError::storage)?,
        descriptor_contracts: row
            .try_get("descriptor_contracts")
            .map_err(RepositoryError::storage)?,
        display_name: row
            .try_get("display_name")
            .map_err(RepositoryError::storage)?,
        public_description: row
            .try_get("public_description")
            .map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

fn postgres_deployment(row: &PgRow) -> Result<AgentDeploymentRevision, RepositoryError> {
    Ok(AgentDeploymentRevision {
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        definition_id: row
            .try_get("definition_id")
            .map_err(RepositoryError::storage)?,
        definition_revision_id: row
            .try_get("definition_revision_id")
            .map_err(RepositoryError::storage)?,
        deployment_revision_id: row
            .try_get("deployment_revision_id")
            .map_err(RepositoryError::storage)?,
        resolution_id: row
            .try_get("resolution_id")
            .map_err(RepositoryError::storage)?,
        plan_hash: row.try_get("plan_hash").map_err(RepositoryError::storage)?,
        binding_hash: row
            .try_get("binding_hash")
            .map_err(RepositoryError::storage)?,
        resolved_bindings: row
            .try_get("resolved_bindings")
            .map_err(RepositoryError::storage)?,
        worker_contracts: row
            .try_get("worker_contracts")
            .map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

fn postgres_debug(row: &PgRow) -> Result<AgentDebugSession, RepositoryError> {
    Ok(AgentDebugSession {
        debug_session_id: row
            .try_get("debug_session_id")
            .map_err(RepositoryError::storage)?,
        agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
        source: row.try_get("source").map_err(RepositoryError::storage)?,
        source_hash: row
            .try_get("source_hash")
            .map_err(RepositoryError::storage)?,
        execution_profile_id: row
            .try_get("execution_profile_id")
            .map_err(RepositoryError::storage)?,
        profile_mode: row
            .try_get("profile_mode")
            .map_err(RepositoryError::storage)?,
        status: debug_status(
            row.try_get::<String, _>("session_status")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        definition_revision_id: row
            .try_get("definition_revision_id")
            .map_err(RepositoryError::storage)?,
        deployment_revision_id: row
            .try_get("deployment_revision_id")
            .map_err(RepositoryError::storage)?,
        run_id: row.try_get("run_id").map_err(RepositoryError::storage)?,
        failure_code: row
            .try_get("failure_code")
            .map_err(RepositoryError::storage)?,
        expires_at: row
            .try_get("expires_at")
            .map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        finished_at: row
            .try_get("finished_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

async fn postgres_replay(
    transaction: &mut Transaction<'_, Postgres>,
    metadata: &AgentMutationMetadata,
) -> Result<Option<AgentMutationReceipt>, AgentManagementWriteError> {
    let row = sqlx::query(
        "SELECT request_hash,response_status,response_json,response_etag
         FROM agent_management_requests
         WHERE operator_id=$1 AND method=$2 AND canonical_path=$3 AND request_id=$4",
    )
    .bind(&metadata.operator_id)
    .bind(&metadata.method)
    .bind(&metadata.canonical_path)
    .bind(&metadata.request_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage)?;
    let Some(row) = row else { return Ok(None) };
    if row.try_get::<String, _>("request_hash").map_err(storage)? != metadata.request_hash {
        return Err(conflict(AgentManagementConflict::IdempotencyKeyReused));
    }
    Ok(Some(AgentMutationReceipt {
        replayed: true,
        status: u16::try_from(row.try_get::<i32, _>("response_status").map_err(storage)?)
            .map_err(|_| AgentManagementWriteError::Repository(invalid_data()))?,
        response: row.try_get("response_json").map_err(storage)?,
        etag: row.try_get("response_etag").map_err(storage)?,
    }))
}

async fn postgres_validate_activation_dependencies(
    transaction: &mut Transaction<'_, Postgres>,
    resolved_bindings: &Value,
    dependency_heads: &Value,
) -> Result<(), AgentManagementWriteError> {
    let mut providers = BTreeMap::new();
    let mut mcp_servers = BTreeSet::new();
    collect_activation_dependencies(resolved_bindings, &mut providers, &mut mcp_servers)?;
    for (provider_id, revision_id) in providers {
        let state: Option<String> = sqlx::query_scalar(
            "SELECT p.operational_state FROM managed_providers p
             JOIN provider_revisions r ON r.provider_id=p.provider_id
             WHERE p.provider_id=$1 AND r.revision_id=$2 FOR SHARE OF p,r",
        )
        .bind(provider_id)
        .bind(revision_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage)?;
        if state.as_deref() != Some("enabled") {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
    }
    for server_id in mcp_servers {
        let state: Option<String> = sqlx::query_scalar(
            "SELECT server_state FROM mcp_managed_servers WHERE server_id=$1 FOR SHARE",
        )
        .bind(server_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage)?;
        if state.as_deref() != Some("active") {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
    }
    let mut expected: Vec<PublicationHead> =
        serde_json::from_value(dependency_heads.clone()).map_err(|_| invalid_data())?;
    expected.sort_by(|left, right| left.agent_id().cmp(right.agent_id()));
    for expected in expected {
        let row = sqlx::query(
            "SELECT agent_id,definition_id,definition_revision_id,deployment_revision_id,
                    publication_origin FROM agent_publication_heads
             WHERE agent_id=$1 FOR SHARE",
        )
        .bind(expected.agent_id())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage)?;
        let actual = row
            .as_ref()
            .map(|row| {
                insight_durable::AgentDeploymentTarget::new(
                    row.try_get("agent_id").map_err(RepositoryError::storage)?,
                    row.try_get("definition_id")
                        .map_err(RepositoryError::storage)?,
                    insight_engine::DefinitionRevisionId::new(
                        row.try_get::<String, _>("definition_revision_id")
                            .map_err(RepositoryError::storage)?,
                    )
                    .map_err(|_| invalid_data())?,
                    insight_engine::DeploymentRevisionId::new(
                        row.try_get::<String, _>("deployment_revision_id")
                            .map_err(RepositoryError::storage)?,
                    )
                    .map_err(|_| invalid_data())?,
                    publication_origin(
                        &row.try_get::<String, _>("publication_origin")
                            .map_err(RepositoryError::storage)?,
                    )?,
                )
                .map(|target| target.publication_head())?
            })
            .transpose()?;
        if actual.as_ref() != Some(&expected) {
            return Err(conflict(AgentManagementConflict::DependencyHeadChanged));
        }
    }
    Ok(())
}

struct PostgresFinalize<'a> {
    event_kind: &'a str,
    agent_id: &'a str,
    subject_id: &'a str,
    before_hash: Option<&'a str>,
    after_hash: Option<&'a str>,
    status: u16,
    response: Value,
    etag: Option<String>,
}

async fn postgres_finalize(
    transaction: &mut Transaction<'_, Postgres>,
    metadata: &AgentMutationMetadata,
    value: PostgresFinalize<'_>,
) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
    sqlx::query(
        "INSERT INTO agent_management_requests(
           operator_id,method,canonical_path,request_id,request_hash,response_status,
           response_json,response_etag,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
    )
    .bind(&metadata.operator_id)
    .bind(&metadata.method)
    .bind(&metadata.canonical_path)
    .bind(&metadata.request_id)
    .bind(&metadata.request_hash)
    .bind(i32::from(value.status))
    .bind(&value.response)
    .bind(&value.etag)
    .bind(metadata.now)
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    sqlx::query(
        "INSERT INTO agent_management_audit_events(
           event_kind,agent_id,subject_id,actor_id,capability,request_id_hash,before_hash,
           after_hash,result_code,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
    )
    .bind(value.event_kind)
    .bind(value.agent_id)
    .bind(value.subject_id)
    .bind(&metadata.operator_id)
    .bind(&metadata.capability)
    .bind(prefixed_sha256(metadata.request_id.as_bytes()))
    .bind(value.before_hash)
    .bind(value.after_hash)
    .bind(if value.status < 300 {
        "accepted"
    } else {
        "rejected"
    })
    .bind(metadata.now)
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    sqlx::query(
        "INSERT INTO agent_management_outbox(
           event_id,event_kind,agent_id,subject_id,safe_payload,created_at,delivered_at)
         VALUES($1,$2,$3,$4,$5,$6,NULL)",
    )
    .bind(format!("aout_{}", Uuid::new_v4().simple()))
    .bind(value.event_kind)
    .bind(value.agent_id)
    .bind(value.subject_id)
    .bind(json!({
        "agent_id":value.agent_id,
        "subject_id":value.subject_id,
        "result_code":if value.status < 300 { "accepted" } else { "rejected" }
    }))
    .bind(metadata.now)
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    Ok(AgentMutationReceipt {
        replayed: false,
        status: value.status,
        response: value.response,
        etag: value.etag,
    })
}

#[async_trait]
impl AgentManagementDurableRepository for SqliteDurableRepository {
    async fn record_agent_management_rejection(
        &self,
        command: RecordAgentManagementRejectionCommand,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            "INSERT INTO agent_management_audit_events(
               event_kind,agent_id,subject_id,actor_id,capability,request_id_hash,before_hash,
               after_hash,result_code,created_at) VALUES('agent.request_rejected',?,?,?,?,?,NULL,NULL,?,?)",
        )
        .bind(command.agent_id)
        .bind(command.subject_id)
        .bind(command.actor_id)
        .bind(command.capability)
        .bind(prefixed_sha256(command.request_id.as_bytes()))
        .bind(command.result_code)
        .bind(database_time(command.now))
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(())
    }

    async fn replay_agent_mutation(
        &self,
        metadata: &AgentMutationMetadata,
    ) -> Result<Option<AgentMutationReceipt>, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        let receipt = sqlite_replay(&mut transaction, metadata).await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn create_agent(
        &self,
        command: CreateAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        if sqlx::query("SELECT 1 FROM managed_agents WHERE agent_id=?")
            .bind(&command.agent_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?
            .is_some()
        {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO managed_agents(
               agent_id,authoring_mode,labels,lifecycle,entity_version,draft_version,
               active_definition_revision_id,active_deployment_revision_id,
               archived_publication_head,created_at,updated_at)
             VALUES(?,?,?,'editable',1,1,NULL,NULL,NULL,?,?)",
        )
        .bind(&command.agent_id)
        .bind(command.authoring_mode.as_str())
        .bind(encode_json(&command.labels)?)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO agent_drafts(
               agent_id,draft_version,author_hash,document,created_at,updated_at)
             VALUES(?,1,?,?,?,?)",
        )
        .bind(&command.agent_id)
        .bind(&command.author_hash)
        .bind(encode_json(&command.draft_document)?)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let agent = ManagedAgent {
            agent_id: command.agent_id.clone(),
            authoring_mode: command.authoring_mode,
            labels: command.labels,
            lifecycle: AgentLifecycle::Editable,
            entity_version: 1,
            draft_version: 1,
            active_definition_revision_id: None,
            active_deployment_revision_id: None,
            archived_publication_head: None,
            created_at: now,
            updated_at: now,
        };
        let response = serde_json::to_value(&agent).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.created",
                agent_id: &agent.agent_id,
                subject_id: &agent.agent_id,
                before_hash: None,
                after_hash: Some(&command.author_hash),
                status: 201,
                response,
                etag: Some(agent_etag(1)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_agent(&self, agent_id: &str) -> Result<Option<ManagedAgent>, RepositoryError> {
        sqlx::query(AssertSqlSafe(format!(
            "{} WHERE agent_id=?",
            sqlite_agent_select()
        )))
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_agent)
        .transpose()
    }

    async fn list_agents(
        &self,
        lifecycle_filter: Option<AgentLifecycle>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<ManagedAgent>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT agent_id,authoring_mode,labels,lifecycle,entity_version,draft_version,
                    active_definition_revision_id,active_deployment_revision_id,
                    archived_publication_head,created_at,updated_at
             FROM managed_agents
             WHERE (? IS NULL OR lifecycle=?)
               AND (? IS NULL OR created_at>? OR (created_at=? AND agent_id>?))
             ORDER BY created_at,agent_id LIMIT ?",
        )
        .bind(lifecycle_filter.map(AgentLifecycle::as_str))
        .bind(lifecycle_filter.map(AgentLifecycle::as_str))
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(sqlite_agent)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.agent_id))
        } else {
            None
        };
        Ok(AgentManagementPage { items, next_cursor })
    }

    async fn update_agent_labels(
        &self,
        command: UpdateAgentLabelsCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT entity_version,labels FROM managed_agents WHERE agent_id=?")
            .bind(&command.agent_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?
            .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("entity_version").map_err(storage)?)?;
        if actual != command.expected_entity_version {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let before: String = row.try_get("labels").map_err(storage)?;
        let before_hash = prefixed_sha256(before.as_bytes());
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        let now = database_time(command.metadata.now);
        let labels = encode_json(&command.labels)?;
        sqlx::query(
            "UPDATE managed_agents SET labels=?,entity_version=?,updated_at=?
             WHERE agent_id=? AND entity_version=?",
        )
        .bind(&labels)
        .bind(u64_to_i64(next)?)
        .bind(now)
        .bind(&command.agent_id)
        .bind(u64_to_i64(actual)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response =
            json!({"agent_id":command.agent_id,"labels":command.labels,"entity_version":next});
        let after_hash = prefixed_sha256(labels.as_bytes());
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.labels.updated",
                agent_id: &command.agent_id,
                subject_id: &command.agent_id,
                before_hash: Some(&before_hash),
                after_hash: Some(&after_hash),
                status: 200,
                response,
                etag: Some(agent_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn delete_agent(
        &self,
        command: DeleteAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT entity_version,active_deployment_revision_id FROM managed_agents WHERE agent_id=?",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if i64_to_u64(row.try_get("entity_version").map_err(storage)?)?
            != command.expected_entity_version
        {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let active: Option<String> = row
            .try_get("active_deployment_revision_id")
            .map_err(storage)?;
        let references: i64 = sqlx::query_scalar(
            "SELECT
               (SELECT COUNT(*) FROM agent_definition_publications WHERE agent_id=?) +
               (SELECT COUNT(*) FROM agent_deployment_publications WHERE agent_id=?) +
               (SELECT COUNT(*) FROM agent_debug_sessions WHERE agent_id=?) +
               (SELECT COUNT(*) FROM workflow_runs r JOIN workflow_definitions d
                  ON d.definition_id=r.definition_id WHERE d.agent_id=?)",
        )
        .bind(&command.agent_id)
        .bind(&command.agent_id)
        .bind(&command.agent_id)
        .bind(&command.agent_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        if active.is_some() || references != 0 {
            return Err(conflict(AgentManagementConflict::Referenced));
        }
        sqlx::query("DELETE FROM managed_agents WHERE agent_id=?")
            .bind(&command.agent_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        let response = json!({"agent_id":command.agent_id,"deleted":true});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.deleted",
                agent_id: &command.agent_id,
                subject_id: &command.agent_id,
                before_hash: None,
                after_hash: None,
                status: 200,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_agent_draft(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentStoredDraft>, RepositoryError> {
        sqlx::query(
            "SELECT agent_id,draft_version,author_hash,document,created_at,updated_at
             FROM agent_drafts WHERE agent_id=?",
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_draft)
        .transpose()
    }

    async fn replace_agent_draft(
        &self,
        command: ReplaceAgentDraftCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT a.lifecycle,d.draft_version,d.author_hash,d.created_at
             FROM managed_agents a JOIN agent_drafts d USING(agent_id) WHERE a.agent_id=?",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if row.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        if actual != command.expected_draft_version {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let before_hash: String = row.try_get("author_hash").map_err(storage)?;
        let created_at: DateTime<Utc> = row.try_get("created_at").map_err(storage)?;
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "UPDATE agent_drafts SET draft_version=?,author_hash=?,document=?,updated_at=?
             WHERE agent_id=? AND draft_version=?",
        )
        .bind(u64_to_i64(next)?)
        .bind(&command.author_hash)
        .bind(encode_json(&command.draft_document)?)
        .bind(now)
        .bind(&command.agent_id)
        .bind(u64_to_i64(actual)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query("UPDATE managed_agents SET draft_version=?,updated_at=? WHERE agent_id=?")
            .bind(u64_to_i64(next)?)
            .bind(now)
            .bind(&command.agent_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        let draft = AgentStoredDraft {
            agent_id: command.agent_id.clone(),
            draft_version: next,
            author_hash: command.author_hash.clone(),
            document: command.draft_document,
            created_at,
            updated_at: now,
        };
        let response = serde_json::to_value(&draft).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.draft.replaced",
                agent_id: &draft.agent_id,
                subject_id: &draft.agent_id,
                before_hash: Some(&before_hash),
                after_hash: Some(&draft.author_hash),
                status: 200,
                response,
                etag: Some(draft_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_agent_draft_view(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentStoredDraftView>, RepositoryError> {
        let row = sqlx::query(
            "SELECT agent_id,view_version,document,updated_at
             FROM agent_draft_views WHERE agent_id=?",
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.map(|row| {
            let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
            Ok(AgentStoredDraftView {
                agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
                view_version: i64_to_u64(
                    row.try_get("view_version")
                        .map_err(RepositoryError::storage)?,
                )?,
                document: decode_json(&document)?,
                updated_at: row
                    .try_get("updated_at")
                    .map_err(RepositoryError::storage)?,
            })
        })
        .transpose()
    }

    async fn replace_agent_draft_view(
        &self,
        command: ReplaceAgentDraftViewCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let agent =
            sqlx::query("SELECT lifecycle,authoring_mode FROM managed_agents WHERE agent_id=?")
                .bind(&command.agent_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage)?
                .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if agent.try_get::<String, _>("lifecycle").map_err(storage)? != "editable"
            || agent
                .try_get::<String, _>("authoring_mode")
                .map_err(storage)?
                != "graph"
        {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let existing =
            sqlx::query("SELECT view_version,document FROM agent_draft_views WHERE agent_id=?")
                .bind(&command.agent_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage)?;
        let actual = match existing.as_ref() {
            Some(row) => {
                let raw: i64 = row.try_get("view_version").map_err(storage)?;
                i64_to_u64(raw).map_err(AgentManagementWriteError::from)?
            }
            None => 0,
        };
        if actual != command.expected_view_version {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        let document = encode_json(&command.document)?;
        let before_hash = existing
            .as_ref()
            .map(|row| {
                row.try_get::<String, _>("document")
                    .map(|value| prefixed_sha256(value.as_bytes()))
                    .map_err(storage)
            })
            .transpose()?;
        let after_hash = prefixed_sha256(document.as_bytes());
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO agent_draft_views(agent_id,view_version,document,updated_at)
             VALUES(?,?,?,?) ON CONFLICT(agent_id) DO UPDATE SET
             view_version=excluded.view_version,document=excluded.document,updated_at=excluded.updated_at",
        )
        .bind(&command.agent_id)
        .bind(u64_to_i64(next)?)
        .bind(&document)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let view = AgentStoredDraftView {
            agent_id: command.agent_id.clone(),
            view_version: next,
            document: command.document,
            updated_at: command.metadata.now,
        };
        let response = serde_json::to_value(&view).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.draft_view.replaced",
                agent_id: &command.agent_id,
                subject_id: &command.agent_id,
                before_hash: before_hash.as_deref(),
                after_hash: Some(&after_hash),
                status: 200,
                response,
                etag: Some(view_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn create_agent_validation(
        &self,
        command: CreateAgentValidationCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT a.lifecycle,d.draft_version,d.author_hash
             FROM managed_agents a JOIN agent_drafts d USING(agent_id) WHERE a.agent_id=?",
        )
        .bind(&command.report.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        let hash: String = row.try_get("author_hash").map_err(storage)?;
        if row.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        if actual != command.expected_draft_version
            || hash != command.expected_author_hash
            || command.report.draft_version != actual
            || command.report.author_hash != hash
        {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        sqlx::query(
            "INSERT INTO agent_validations(
               validation_id,agent_id,draft_version,author_hash,policy_digest,
               operation_status,semantic_hash,report_hash,document,created_at,created_by)
             VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&command.report.validation_id)
        .bind(&command.report.agent_id)
        .bind(u64_to_i64(command.report.draft_version)?)
        .bind(&command.report.author_hash)
        .bind(&command.report.policy_digest)
        .bind(command.report.status.as_str())
        .bind(&command.report.semantic_hash)
        .bind(&command.report.report_hash)
        .bind(encode_json(&command.report.document)?)
        .bind(database_time(command.report.created_at))
        .bind(&command.report.created_by)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(&command.report).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.validation.created",
                agent_id: &command.report.agent_id,
                subject_id: &command.report.validation_id,
                before_hash: None,
                after_hash: Some(&command.report.report_hash),
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_agent_validation(
        &self,
        agent_id: &str,
        validation_id: &str,
    ) -> Result<Option<AgentValidationReport>, RepositoryError> {
        sqlx::query(AssertSqlSafe(format!(
            "{} WHERE agent_id=? AND validation_id=?",
            sqlite_validation_select()
        )))
        .bind(agent_id)
        .bind(validation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_validation)
        .transpose()
    }

    async fn publish_agent_definition(
        &self,
        command: PublishAgentDefinitionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT a.lifecycle,d.draft_version,d.author_hash,v.operation_status,
                    v.draft_version AS validation_draft_version,
                    v.author_hash AS validation_author_hash,v.policy_digest,v.semantic_hash
             FROM managed_agents a
             JOIN agent_drafts d USING(agent_id)
             JOIN agent_validations v ON v.agent_id=a.agent_id AND v.validation_id=?
             WHERE a.agent_id=?",
        )
        .bind(&command.validation_id)
        .bind(command.plan.agent_id())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if row.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let draft_version = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        let validation_draft =
            i64_to_u64(row.try_get("validation_draft_version").map_err(storage)?)?;
        let author_hash: String = row.try_get("author_hash").map_err(storage)?;
        let validation_author: String = row.try_get("validation_author_hash").map_err(storage)?;
        let policy: String = row.try_get("policy_digest").map_err(storage)?;
        let semantic: Option<String> = row.try_get("semantic_hash").map_err(storage)?;
        if draft_version != command.expected_draft_version
            || validation_draft != draft_version
            || validation_author != author_hash
            || policy != command.validation_policy_digest
        {
            return Err(conflict(AgentManagementConflict::ValidationStale));
        }
        if row
            .try_get::<String, _>("operation_status")
            .map_err(storage)?
            != "succeeded"
            || semantic.as_deref() != Some(command.plan.plan_hash().as_str())
        {
            return Err(conflict(AgentManagementConflict::ValidationFailed));
        }
        install_sqlite_plan(
            &mut transaction,
            &command.plan,
            SqlitePlanInstallScope::Definition,
        )
        .await?;
        let revision_number: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision_number),0)+1
             FROM agent_definition_publications WHERE agent_id=?",
        )
        .bind(command.plan.agent_id())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO agent_definition_publications(
               agent_id,definition_id,definition_revision_id,revision_number,
               source_draft_version,validation_id,author_hash,created_at,created_by)
             VALUES(?,?,?,?,?,?,?,?,?)",
        )
        .bind(command.plan.agent_id())
        .bind(command.plan.definition_id())
        .bind(command.plan.definition_revision_id().as_str())
        .bind(revision_number)
        .bind(u64_to_i64(draft_version)?)
        .bind(&command.validation_id)
        .bind(&author_hash)
        .bind(now)
        .bind(&command.metadata.operator_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({
            "agent_id":command.plan.agent_id(),
            "definition_revision_id":command.plan.definition_revision_id(),
            "revision_number":revision_number,
            "semantic_hash":command.plan.plan_hash()
        });
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.definition.published",
                agent_id: command.plan.agent_id(),
                subject_id: command.plan.definition_revision_id().as_str(),
                before_hash: Some(&author_hash),
                after_hash: Some(command.plan.plan_hash().as_str()),
                status: 201,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_agent_definition(
        &self,
        agent_id: &str,
        definition_revision_id: &str,
    ) -> Result<Option<AgentDefinitionRevision>, RepositoryError> {
        sqlx::query(AssertSqlSafe(format!(
            "{} WHERE p.agent_id=? AND p.definition_revision_id=?",
            sqlite_definition_select()
        )))
        .bind(agent_id)
        .bind(definition_revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_definition)
        .transpose()
    }

    async fn list_agent_definitions(
        &self,
        agent_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<AgentDefinitionRevision>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT p.agent_id,p.definition_id,p.definition_revision_id,p.revision_number,
                    p.source_draft_version,p.validation_id,p.author_hash,p.created_at,p.created_by,
                    r.plan_hash,r.compiler_version,r.expression_engine_version,r.author_document,
                    r.canonical_plan,r.descriptor_contracts,m.display_name,m.public_description
             FROM agent_definition_publications p
             JOIN workflow_definition_revisions r
               ON r.definition_id=p.definition_id AND r.definition_revision_id=p.definition_revision_id
             JOIN workflow_definition_public_metadata m
               ON m.definition_id=p.definition_id AND m.definition_revision_id=p.definition_revision_id
             WHERE p.agent_id=?
               AND (? IS NULL OR p.created_at>? OR
                    (p.created_at=? AND p.definition_revision_id>?))
             ORDER BY p.created_at,p.definition_revision_id LIMIT ?",
        )
        .bind(agent_id)
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(sqlite_definition)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.definition_revision_id))
        } else {
            None
        };
        Ok(AgentManagementPage { items, next_cursor })
    }

    async fn create_agent_deployment_resolution(
        &self,
        command: CreateAgentResolutionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let editable: Option<String> = sqlx::query_scalar(
            "SELECT a.lifecycle FROM managed_agents a
             JOIN agent_definition_publications p ON p.agent_id=a.agent_id
             WHERE a.agent_id=? AND p.definition_revision_id=?",
        )
        .bind(&command.resolution.agent_id)
        .bind(&command.resolution.definition_revision_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?;
        match editable.as_deref() {
            None => return Err(conflict(AgentManagementConflict::NotFound)),
            Some("editable") => {}
            Some(_) => return Err(conflict(AgentManagementConflict::ForbiddenState)),
        }
        sqlx::query(
            "INSERT INTO agent_deployment_resolutions(
               resolution_id,agent_id,definition_revision_id,operation_status,
               catalog_snapshot_hash,resolution_hash,resolved_bindings,worker_contracts,
               dependency_heads,risks,expires_at,created_at,created_by)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&command.resolution.resolution_id)
        .bind(&command.resolution.agent_id)
        .bind(&command.resolution.definition_revision_id)
        .bind(command.resolution.status.as_str())
        .bind(&command.resolution.catalog_snapshot_hash)
        .bind(&command.resolution.resolution_hash)
        .bind(encode_json(&command.resolution.resolved_bindings)?)
        .bind(encode_json(&command.resolution.worker_contracts)?)
        .bind(encode_json(&command.resolution.dependency_heads)?)
        .bind(encode_json(&command.resolution.risks)?)
        .bind(database_time(command.resolution.expires_at))
        .bind(database_time(command.resolution.created_at))
        .bind(&command.resolution.created_by)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(&command.resolution).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.deployment_resolution.created",
                agent_id: &command.resolution.agent_id,
                subject_id: &command.resolution.resolution_id,
                before_hash: None,
                after_hash: Some(&command.resolution.resolution_hash),
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_agent_deployment_resolution(
        &self,
        agent_id: &str,
        resolution_id: &str,
    ) -> Result<Option<AgentDeploymentResolution>, RepositoryError> {
        sqlx::query(AssertSqlSafe(format!(
            "{} WHERE agent_id=? AND resolution_id=?",
            sqlite_resolution_select()
        )))
        .bind(agent_id)
        .bind(resolution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_resolution)
        .transpose()
    }

    async fn install_agent_deployment(
        &self,
        command: InstallAgentDeploymentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT a.lifecycle,r.definition_revision_id,r.operation_status,r.resolution_hash,
                    r.resolved_bindings,r.worker_contracts,r.dependency_heads,r.expires_at
             FROM managed_agents a JOIN agent_deployment_resolutions r USING(agent_id)
             WHERE a.agent_id=? AND r.resolution_id=?",
        )
        .bind(command.plan.agent_id())
        .bind(&command.resolution_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if row.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        if row
            .try_get::<String, _>("operation_status")
            .map_err(storage)?
            != "succeeded"
        {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let expires_at: DateTime<Utc> = row.try_get("expires_at").map_err(storage)?;
        if expires_at <= command.metadata.now {
            return Err(conflict(AgentManagementConflict::ResolutionExpired));
        }
        let resolution_hash: String = row.try_get("resolution_hash").map_err(storage)?;
        let definition_revision_id: String =
            row.try_get("definition_revision_id").map_err(storage)?;
        let bindings: String = row.try_get("resolved_bindings").map_err(storage)?;
        let workers: String = row.try_get("worker_contracts").map_err(storage)?;
        let heads: String = row.try_get("dependency_heads").map_err(storage)?;
        if resolution_hash != command.expected_resolution_hash
            || definition_revision_id != command.plan.definition_revision_id().as_str()
            || decode_json(&bindings)? != *command.plan.resolved_bindings()
            || decode_json(&workers)? != *command.plan.worker_contracts()
            || decode_json(&heads)? != command.expected_dependency_heads
        {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let expected_heads: Vec<PublicationHead> =
            serde_json::from_value(command.expected_dependency_heads.clone())
                .map_err(|_| invalid_data())?;
        for expected in expected_heads {
            let actual = sqlx::query(
                "SELECT agent_id,definition_id,definition_revision_id,deployment_revision_id,
                        publication_origin FROM agent_publication_heads WHERE agent_id=?",
            )
            .bind(expected.agent_id())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?;
            let actual = actual
                .as_ref()
                .map(|row| {
                    insight_durable::AgentDeploymentTarget::new(
                        row.try_get("agent_id").map_err(RepositoryError::storage)?,
                        row.try_get("definition_id")
                            .map_err(RepositoryError::storage)?,
                        insight_engine::DefinitionRevisionId::new(
                            row.try_get::<String, _>("definition_revision_id")
                                .map_err(RepositoryError::storage)?,
                        )
                        .map_err(|_| invalid_data())?,
                        insight_engine::DeploymentRevisionId::new(
                            row.try_get::<String, _>("deployment_revision_id")
                                .map_err(RepositoryError::storage)?,
                        )
                        .map_err(|_| invalid_data())?,
                        match row
                            .try_get::<String, _>("publication_origin")
                            .map_err(RepositoryError::storage)?
                            .as_str()
                        {
                            "built_in" => PublicationOrigin::BuiltIn,
                            "graph" => PublicationOrigin::Graph,
                            "managed" => PublicationOrigin::Managed,
                            _ => return Err(invalid_data()),
                        },
                    )?
                    .publication_head()
                })
                .transpose()?;
            if actual.as_ref() != Some(&expected) {
                return Err(conflict(AgentManagementConflict::DependencyHeadChanged));
            }
        }
        install_sqlite_plan(
            &mut transaction,
            &command.plan,
            SqlitePlanInstallScope::Deployment,
        )
        .await?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO agent_deployment_publications(
               agent_id,definition_id,definition_revision_id,deployment_revision_id,
               resolution_id,created_at,created_by) VALUES(?,?,?,?,?,?,?)",
        )
        .bind(command.plan.agent_id())
        .bind(command.plan.definition_id())
        .bind(command.plan.definition_revision_id().as_str())
        .bind(command.plan.deployment_revision_id().as_str())
        .bind(&command.resolution_id)
        .bind(now)
        .bind(&command.metadata.operator_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({
            "agent_id":command.plan.agent_id(),
            "definition_revision_id":command.plan.definition_revision_id(),
            "deployment_revision_id":command.plan.deployment_revision_id(),
            "binding_hash":command.plan.binding_hash(),
            "resolution_id":command.resolution_id
        });
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.deployment.installed",
                agent_id: command.plan.agent_id(),
                subject_id: command.plan.deployment_revision_id().as_str(),
                before_hash: Some(&resolution_hash),
                after_hash: Some(command.plan.binding_hash().as_str()),
                status: 201,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_agent_deployment(
        &self,
        agent_id: &str,
        deployment_revision_id: &str,
    ) -> Result<Option<AgentDeploymentRevision>, RepositoryError> {
        sqlx::query(AssertSqlSafe(format!(
            "{} WHERE p.agent_id=? AND p.deployment_revision_id=?",
            sqlite_deployment_select()
        )))
        .bind(agent_id)
        .bind(deployment_revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_deployment)
        .transpose()
    }

    async fn list_agent_deployments(
        &self,
        agent_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<AgentDeploymentRevision>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT p.agent_id,p.definition_id,p.definition_revision_id,p.deployment_revision_id,
                    p.resolution_id,p.created_at,p.created_by,x.plan_hash,x.binding_hash,
                    x.resolved_bindings,x.worker_contracts
             FROM agent_deployment_publications p
             JOIN deployment_revisions x
               ON x.definition_id=p.definition_id AND x.deployment_revision_id=p.deployment_revision_id
             WHERE p.agent_id=?
               AND (? IS NULL OR p.created_at>? OR
                    (p.created_at=? AND p.deployment_revision_id>?))
             ORDER BY p.created_at,p.deployment_revision_id LIMIT ?",
        )
        .bind(agent_id)
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(sqlite_deployment)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.deployment_revision_id))
        } else {
            None
        };
        Ok(AgentManagementPage { items, next_cursor })
    }

    async fn activate_managed_agent_deployment(
        &self,
        command: ActivateManagedAgentDeploymentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let agent = sqlx::query(
            "SELECT lifecycle,entity_version,active_definition_revision_id,
                    active_deployment_revision_id FROM managed_agents WHERE agent_id=?",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if agent.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let version = i64_to_u64(agent.try_get("entity_version").map_err(storage)?)?;
        if version != command.expected_entity_version {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let target = sqlx::query(
            "SELECT p.definition_id,p.definition_revision_id,x.resolved_bindings,
                    r.dependency_heads
             FROM agent_deployment_publications p
             JOIN deployment_revisions x
               ON x.definition_id=p.definition_id AND x.deployment_revision_id=p.deployment_revision_id
             JOIN agent_deployment_resolutions r ON r.resolution_id=p.resolution_id
             WHERE p.agent_id=? AND p.deployment_revision_id=?",
        )
        .bind(&command.agent_id)
        .bind(&command.deployment_revision_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        let definition_id: String = target.try_get("definition_id").map_err(storage)?;
        let definition_revision_id: String =
            target.try_get("definition_revision_id").map_err(storage)?;
        let resolved_bindings = decode_json(
            &target
                .try_get::<String, _>("resolved_bindings")
                .map_err(storage)?,
        )?;
        let dependency_heads = decode_json(
            &target
                .try_get::<String, _>("dependency_heads")
                .map_err(storage)?,
        )?;
        sqlite_validate_activation_dependencies(
            &mut transaction,
            &resolved_bindings,
            &dependency_heads,
        )
        .await?;
        let current_route: Option<(String, String)> = sqlx::query_as(
            "SELECT definition_revision_id,deployment_revision_id
             FROM agent_publication_heads WHERE agent_id=?",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?;
        let entity_definition: Option<String> = agent
            .try_get("active_definition_revision_id")
            .map_err(storage)?;
        let entity_deployment: Option<String> = agent
            .try_get("active_deployment_revision_id")
            .map_err(storage)?;
        if current_route != entity_definition.clone().zip(entity_deployment.clone()) {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        sqlx::query(
            "INSERT INTO agent_publication_heads(
               agent_id,definition_id,definition_revision_id,deployment_revision_id,
               publication_origin,updated_at) VALUES(?,?,?,?,'managed',?)
             ON CONFLICT(agent_id) DO UPDATE SET
               definition_id=excluded.definition_id,
               definition_revision_id=excluded.definition_revision_id,
               deployment_revision_id=excluded.deployment_revision_id,
               publication_origin='managed',updated_at=excluded.updated_at",
        )
        .bind(&command.agent_id)
        .bind(&definition_id)
        .bind(&definition_revision_id)
        .bind(&command.deployment_revision_id)
        .bind(database_time(command.metadata.now))
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let next = version.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query(
            "UPDATE managed_agents SET active_definition_revision_id=?,
               active_deployment_revision_id=?,entity_version=?,
               archived_publication_head=NULL,updated_at=?
             WHERE agent_id=? AND entity_version=?",
        )
        .bind(&definition_revision_id)
        .bind(&command.deployment_revision_id)
        .bind(u64_to_i64(next)?)
        .bind(database_time(command.metadata.now))
        .bind(&command.agent_id)
        .bind(u64_to_i64(version)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({
            "agent_id":command.agent_id,
            "definition_revision_id":definition_revision_id,
            "deployment_revision_id":command.deployment_revision_id,
            "entity_version":next
        });
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.deployment.activated",
                agent_id: &command.agent_id,
                subject_id: &command.deployment_revision_id,
                before_hash: entity_deployment.as_deref(),
                after_hash: Some(&command.deployment_revision_id),
                status: 200,
                response,
                etag: Some(agent_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn deactivate_managed_agent(
        &self,
        command: DeactivateManagedAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        sqlite_deactivate_or_archive(
            self,
            command.metadata,
            command.agent_id,
            command.expected_entity_version,
            false,
        )
        .await
    }

    async fn archive_agent(
        &self,
        command: ArchiveAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        sqlite_deactivate_or_archive(
            self,
            command.metadata,
            command.agent_id,
            command.expected_entity_version,
            true,
        )
        .await
    }

    async fn restore_agent(
        &self,
        command: RestoreAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row =
            sqlx::query("SELECT lifecycle,entity_version FROM managed_agents WHERE agent_id=?")
                .bind(&command.agent_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage)?
                .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        let version = i64_to_u64(row.try_get("entity_version").map_err(storage)?)?;
        if version != command.expected_entity_version {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        if row.try_get::<String, _>("lifecycle").map_err(storage)? != "archived" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let next = version.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query(
            "UPDATE managed_agents SET lifecycle='editable',entity_version=?,
               active_definition_revision_id=NULL,active_deployment_revision_id=NULL,
               updated_at=? WHERE agent_id=? AND entity_version=?",
        )
        .bind(u64_to_i64(next)?)
        .bind(database_time(command.metadata.now))
        .bind(&command.agent_id)
        .bind(u64_to_i64(version)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response =
            json!({"agent_id":command.agent_id,"lifecycle":"editable","entity_version":next});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.restored",
                agent_id: &command.agent_id,
                subject_id: &command.agent_id,
                before_hash: None,
                after_hash: None,
                status: 200,
                response,
                etag: Some(agent_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn create_agent_debug_session(
        &self,
        command: CreateAgentDebugSessionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let lifecycle: Option<String> =
            sqlx::query_scalar("SELECT lifecycle FROM managed_agents WHERE agent_id=?")
                .bind(&command.session.agent_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage)?;
        match lifecycle.as_deref() {
            None => return Err(conflict(AgentManagementConflict::NotFound)),
            Some("editable") => {}
            Some(_) => return Err(conflict(AgentManagementConflict::ForbiddenState)),
        }
        if let Some((expected_version, expected_hash)) = debug_draft_pin(&command.session.source) {
            let draft: Option<(i64, String)> = sqlx::query_as(
                "SELECT draft_version,author_hash FROM agent_drafts WHERE agent_id=?",
            )
            .bind(&command.session.agent_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?;
            if draft
                .and_then(|(version, hash)| i64_to_u64(version).ok().map(|version| (version, hash)))
                .as_ref()
                .is_none_or(|(version, hash)| *version != expected_version || hash != expected_hash)
            {
                return Err(conflict(AgentManagementConflict::PreconditionFailed));
            }
        }
        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_debug_sessions
             WHERE agent_id=? AND session_status IN('queued','running')",
        )
        .bind(&command.session.agent_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        if active >= i64::from(command.max_active_sessions) {
            return Err(conflict(AgentManagementConflict::CapacityExceeded));
        }
        sqlx::query(
            "INSERT INTO agent_debug_sessions(
               debug_session_id,agent_id,source,source_hash,execution_profile_id,
               profile_mode,session_status,definition_revision_id,deployment_revision_id,
               run_id,failure_code,expires_at,created_at,finished_at,created_by)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&command.session.debug_session_id)
        .bind(&command.session.agent_id)
        .bind(encode_json(&command.session.source)?)
        .bind(&command.session.source_hash)
        .bind(&command.session.execution_profile_id)
        .bind(&command.session.profile_mode)
        .bind(command.session.status.as_str())
        .bind(&command.session.definition_revision_id)
        .bind(&command.session.deployment_revision_id)
        .bind(&command.session.run_id)
        .bind(&command.session.failure_code)
        .bind(database_time(command.session.expires_at))
        .bind(database_time(command.session.created_at))
        .bind(command.session.finished_at.map(database_time))
        .bind(&command.session.created_by)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO agent_debug_content_retention(debug_session_id,retain_until,content_deleted_at)
             VALUES(?,?,NULL)",
        )
        .bind(&command.session.debug_session_id)
        .bind(database_time(command.retain_until))
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(&command.session).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.debug.created",
                agent_id: &command.session.agent_id,
                subject_id: &command.session.debug_session_id,
                before_hash: None,
                after_hash: Some(&command.session.source_hash),
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_agent_debug_session(
        &self,
        agent_id: &str,
        debug_session_id: &str,
    ) -> Result<Option<AgentDebugSession>, RepositoryError> {
        sqlx::query(AssertSqlSafe(format!(
            "{} WHERE agent_id=? AND debug_session_id=?",
            sqlite_debug_select()
        )))
        .bind(agent_id)
        .bind(debug_session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_debug)
        .transpose()
    }

    async fn list_agent_debug_sessions(
        &self,
        agent_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<AgentDebugSession>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT debug_session_id,agent_id,source,source_hash,execution_profile_id,
                    profile_mode,session_status,definition_revision_id,deployment_revision_id,
                    run_id,failure_code,expires_at,created_at,finished_at,created_by
             FROM agent_debug_sessions
             WHERE agent_id=?
               AND (? IS NULL OR created_at>? OR (created_at=? AND debug_session_id>?))
             ORDER BY created_at,debug_session_id LIMIT ?",
        )
        .bind(agent_id)
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(sqlite_debug)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.debug_session_id))
        } else {
            None
        };
        Ok(AgentManagementPage { items, next_cursor })
    }

    async fn complete_agent_debug_session(
        &self,
        command: CompleteAgentDebugSessionCommand,
    ) -> Result<(), AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(plan) = command.plan.as_ref() {
            install_sqlite_plan(&mut transaction, plan, SqlitePlanInstallScope::All).await?;
        }
        let finished_at = (!matches!(
            command.status,
            AgentDebugStatus::Queued | AgentDebugStatus::Running
        ))
        .then(|| database_time(command.now));
        let updated = sqlx::query(
            "UPDATE agent_debug_sessions SET session_status=?,definition_revision_id=?,
               deployment_revision_id=?,run_id=?,failure_code=?,finished_at=?
             WHERE debug_session_id=? AND session_status IN('queued','running')",
        )
        .bind(command.status.as_str())
        .bind(command.definition_revision_id)
        .bind(command.deployment_revision_id)
        .bind(command.run_id)
        .bind(command.failure_code)
        .bind(finished_at)
        .bind(command.debug_session_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?
        .rows_affected();
        if updated != 1 {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        transaction.commit().await.map_err(storage)?;
        Ok(())
    }

    async fn cancel_agent_debug_session(
        &self,
        command: CancelAgentDebugSessionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT session_status FROM agent_debug_sessions
             WHERE agent_id=? AND debug_session_id=?",
        )
        .bind(&command.agent_id)
        .bind(&command.debug_session_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        let status: String = row.try_get("session_status").map_err(storage)?;
        if matches!(status.as_str(), "queued" | "running") {
            sqlx::query(
                "UPDATE agent_debug_sessions SET session_status='cancelled',finished_at=?
                 WHERE debug_session_id=?",
            )
            .bind(database_time(command.metadata.now))
            .bind(&command.debug_session_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        } else if status != "cancelled" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let response = json!({"agent_id":command.agent_id,"debug_session_id":command.debug_session_id,"status":"cancelled"});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "agent.debug.cancelled",
                agent_id: &command.agent_id,
                subject_id: &command.debug_session_id,
                before_hash: None,
                after_hash: None,
                status: 200,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn cleanup_expired_agent_debug_sessions(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError> {
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(RepositoryError::storage)?;
        let ids = sqlx::query_scalar::<_, String>(
            "SELECT debug_session_id FROM agent_debug_sessions
             WHERE expires_at<=? AND session_status IN('queued','running')
             ORDER BY expires_at,debug_session_id LIMIT ?",
        )
        .bind(database_time(now))
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        for id in &ids {
            sqlx::query(
                "UPDATE agent_debug_sessions SET session_status='expired',finished_at=?
                 WHERE debug_session_id=? AND session_status IN('queued','running')",
            )
            .bind(database_time(now))
            .bind(id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        let redact_ids = sqlx::query_scalar::<_, String>(
            "SELECT debug_session_id FROM agent_debug_content_retention
             WHERE retain_until<=? AND content_deleted_at IS NULL
             ORDER BY retain_until,debug_session_id LIMIT ?",
        )
        .bind(database_time(now))
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        for id in &redact_ids {
            sqlx::query(
                "UPDATE agent_debug_sessions SET source=json('{\"content_deleted\":true}')
                 WHERE debug_session_id=?",
            )
            .bind(id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "UPDATE agent_management_requests
                 SET response_json=json_set(response_json,'$.source',json('{\"content_deleted\":true}'))
                 WHERE json_extract(response_json,'$.debug_session_id')=?",
            )
            .bind(id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "UPDATE agent_debug_content_retention SET content_deleted_at=?
                 WHERE debug_session_id=? AND content_deleted_at IS NULL",
            )
            .bind(database_time(now))
            .bind(id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok((ids.len() + redact_ids.len()) as u64)
    }

    async fn load_agent_management_runtime_stats(
        &self,
    ) -> Result<AgentManagementRuntimeStats, RepositoryError> {
        let summary = sqlx::query(
            "SELECT
               (SELECT COUNT(*) FROM agent_drafts) drafts_current,
               (SELECT COUNT(*) FROM agent_validations WHERE operation_status IN('queued','running')) validations_pending,
               (SELECT COUNT(*) FROM agent_deployment_resolutions WHERE operation_status IN('queued','running')) deployment_resolutions_pending,
               (SELECT COUNT(*) FROM managed_agents WHERE lifecycle='editable' AND active_deployment_revision_id IS NOT NULL) active_agents,
               (SELECT COUNT(*) FROM managed_agents WHERE lifecycle='archived') archived_agents",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let debug_sessions = sqlx::query(
            "SELECT session_status,profile_mode,COUNT(*) count
             FROM agent_debug_sessions GROUP BY session_status,profile_mode
             ORDER BY session_status,profile_mode",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .iter()
        .map(|row| {
            Ok(AgentDebugRuntimeCount {
                state: debug_status(
                    &row.try_get::<String, _>("session_status")
                        .map_err(RepositoryError::storage)?,
                )?,
                profile_mode: row
                    .try_get("profile_mode")
                    .map_err(RepositoryError::storage)?,
                count: i64_to_u64(row.try_get("count").map_err(RepositoryError::storage)?)?,
            })
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
        let operations = sqlx::query(
            "SELECT event_kind,result_code,COUNT(*) count
             FROM agent_management_audit_events GROUP BY event_kind,result_code
             ORDER BY event_kind,result_code",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .iter()
        .map(|row| {
            Ok(AgentManagementOperationCount {
                operation: row
                    .try_get("event_kind")
                    .map_err(RepositoryError::storage)?,
                outcome: row
                    .try_get("result_code")
                    .map_err(RepositoryError::storage)?,
                count: i64_to_u64(row.try_get("count").map_err(RepositoryError::storage)?)?,
            })
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
        Ok(AgentManagementRuntimeStats {
            drafts_current: i64_to_u64(
                summary
                    .try_get("drafts_current")
                    .map_err(RepositoryError::storage)?,
            )?,
            validations_pending: i64_to_u64(
                summary
                    .try_get("validations_pending")
                    .map_err(RepositoryError::storage)?,
            )?,
            deployment_resolutions_pending: i64_to_u64(
                summary
                    .try_get("deployment_resolutions_pending")
                    .map_err(RepositoryError::storage)?,
            )?,
            active_agents: i64_to_u64(
                summary
                    .try_get("active_agents")
                    .map_err(RepositoryError::storage)?,
            )?,
            archived_agents: i64_to_u64(
                summary
                    .try_get("archived_agents")
                    .map_err(RepositoryError::storage)?,
            )?,
            debug_sessions,
            operations,
        })
    }
}

async fn sqlite_deactivate_or_archive(
    repository: &SqliteDurableRepository,
    metadata: AgentMutationMetadata,
    agent_id: String,
    expected_entity_version: u64,
    archive: bool,
) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
    let mut transaction = sqlite_begin(repository).await?;
    if let Some(receipt) = sqlite_replay(&mut transaction, &metadata).await? {
        transaction.commit().await.map_err(storage)?;
        return Ok(receipt);
    }
    let row = sqlx::query(
        "SELECT lifecycle,entity_version,active_definition_revision_id,
                active_deployment_revision_id FROM managed_agents WHERE agent_id=?",
    )
    .bind(&agent_id)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(storage)?
    .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
    let version = i64_to_u64(row.try_get("entity_version").map_err(storage)?)?;
    if version != expected_entity_version {
        return Err(conflict(AgentManagementConflict::PreconditionFailed));
    }
    if row.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
        return Err(conflict(AgentManagementConflict::ForbiddenState));
    }
    let active_definition: Option<String> = row
        .try_get("active_definition_revision_id")
        .map_err(storage)?;
    let active_deployment: Option<String> = row
        .try_get("active_deployment_revision_id")
        .map_err(storage)?;
    let publication = match active_definition.as_ref().zip(active_deployment.as_ref()) {
        Some((definition_revision_id, deployment_revision_id)) => {
            let head = sqlx::query(
                "SELECT definition_id,publication_origin FROM agent_publication_heads
                 WHERE agent_id=? AND definition_revision_id=? AND deployment_revision_id=?",
            )
            .bind(&agent_id)
            .bind(definition_revision_id)
            .bind(deployment_revision_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?
            .ok_or_else(|| conflict(AgentManagementConflict::PreconditionFailed))?;
            let origin = match head
                .try_get::<String, _>("publication_origin")
                .map_err(storage)?
                .as_str()
            {
                "managed" => PublicationOrigin::Managed,
                _ => return Err(conflict(AgentManagementConflict::PreconditionFailed)),
            };
            Some(
                insight_durable::AgentDeploymentTarget::new(
                    agent_id.clone(),
                    head.try_get("definition_id").map_err(storage)?,
                    insight_engine::DefinitionRevisionId::new(definition_revision_id.clone())
                        .map_err(|_| invalid_data())?,
                    insight_engine::DeploymentRevisionId::new(deployment_revision_id.clone())
                        .map_err(|_| invalid_data())?,
                    origin,
                )?
                .publication_head()?,
            )
        }
        None if active_definition.is_none() && active_deployment.is_none() => None,
        None => return Err(conflict(AgentManagementConflict::PreconditionFailed)),
    };
    sqlx::query("DELETE FROM agent_publication_heads WHERE agent_id=?")
        .bind(&agent_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
    let next = version.checked_add(1).ok_or_else(invalid_data)?;
    sqlx::query(
        "UPDATE managed_agents SET lifecycle=?,entity_version=?,
           active_definition_revision_id=NULL,active_deployment_revision_id=NULL,
           archived_publication_head=?,updated_at=? WHERE agent_id=? AND entity_version=?",
    )
    .bind(if archive { "archived" } else { "editable" })
    .bind(u64_to_i64(next)?)
    .bind(if archive {
        publication.as_ref().map(encode_json).transpose()?
    } else {
        None
    })
    .bind(database_time(metadata.now))
    .bind(&agent_id)
    .bind(u64_to_i64(version)?)
    .execute(&mut *transaction)
    .await
    .map_err(storage)?;
    let response = json!({
        "agent_id":agent_id,
        "lifecycle":if archive { "archived" } else { "editable" },
        "active_deployment_revision_id":null,
        "entity_version":next
    });
    let receipt = sqlite_finalize(
        &mut transaction,
        &metadata,
        SqliteFinalize {
            event_kind: if archive {
                "agent.archived"
            } else {
                "agent.deactivated"
            },
            agent_id: &agent_id,
            subject_id: &agent_id,
            before_hash: active_deployment.as_deref(),
            after_hash: None,
            status: 200,
            response,
            etag: Some(agent_etag(next)),
        },
    )
    .await?;
    transaction.commit().await.map_err(storage)?;
    Ok(receipt)
}

#[async_trait]
impl AgentManagementDurableRepository for PostgresDurableRepository {
    async fn record_agent_management_rejection(
        &self,
        command: RecordAgentManagementRejectionCommand,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            "INSERT INTO agent_management_audit_events(
               event_kind,agent_id,subject_id,actor_id,capability,request_id_hash,before_hash,
               after_hash,result_code,created_at)
             VALUES('agent.request_rejected',$1,$2,$3,$4,$5,NULL,NULL,$6,$7)",
        )
        .bind(command.agent_id)
        .bind(command.subject_id)
        .bind(command.actor_id)
        .bind(command.capability)
        .bind(prefixed_sha256(command.request_id.as_bytes()))
        .bind(command.result_code)
        .bind(command.now)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(())
    }

    async fn replay_agent_mutation(
        &self,
        metadata: &AgentMutationMetadata,
    ) -> Result<Option<AgentMutationReceipt>, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        let receipt = postgres_replay(&mut transaction, metadata).await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn create_agent(
        &self,
        command: CreateAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        if sqlx::query("SELECT 1 FROM managed_agents WHERE agent_id=$1 FOR UPDATE")
            .bind(&command.agent_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?
            .is_some()
        {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        sqlx::query(
            "INSERT INTO managed_agents(
               agent_id,authoring_mode,labels,lifecycle,entity_version,draft_version,
               active_definition_revision_id,active_deployment_revision_id,
               archived_publication_head,created_at,updated_at)
             VALUES($1,$2,$3,'editable',1,1,NULL,NULL,NULL,$4,$4)",
        )
        .bind(&command.agent_id)
        .bind(command.authoring_mode.as_str())
        .bind(&command.labels)
        .bind(command.metadata.now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO agent_drafts(
               agent_id,draft_version,author_hash,document,created_at,updated_at)
             VALUES($1,1,$2,$3,$4,$4)",
        )
        .bind(&command.agent_id)
        .bind(&command.author_hash)
        .bind(&command.draft_document)
        .bind(command.metadata.now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let agent = ManagedAgent {
            agent_id: command.agent_id.clone(),
            authoring_mode: command.authoring_mode,
            labels: command.labels,
            lifecycle: AgentLifecycle::Editable,
            entity_version: 1,
            draft_version: 1,
            active_definition_revision_id: None,
            active_deployment_revision_id: None,
            archived_publication_head: None,
            created_at: command.metadata.now,
            updated_at: command.metadata.now,
        };
        let response = serde_json::to_value(&agent).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.created",
                agent_id: &agent.agent_id,
                subject_id: &agent.agent_id,
                before_hash: None,
                after_hash: Some(&command.author_hash),
                status: 201,
                response,
                etag: Some(agent_etag(1)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn get_agent(&self, agent_id: &str) -> Result<Option<ManagedAgent>, RepositoryError> {
        sqlx::query(
            "SELECT agent_id,authoring_mode,labels,lifecycle,entity_version,draft_version,
                    active_definition_revision_id,active_deployment_revision_id,
                    archived_publication_head,created_at,updated_at
             FROM managed_agents WHERE agent_id=$1",
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_agent)
        .transpose()
    }
    async fn list_agents(
        &self,
        lifecycle_filter: Option<AgentLifecycle>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<ManagedAgent>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT agent_id,authoring_mode,labels,lifecycle,entity_version,draft_version,
                    active_definition_revision_id,active_deployment_revision_id,
                    archived_publication_head,created_at,updated_at
             FROM managed_agents
             WHERE ($1::text IS NULL OR lifecycle=$1)
               AND ($2::timestamptz IS NULL OR created_at>$2 OR
                    (created_at=$2 AND agent_id>$3))
             ORDER BY created_at,agent_id LIMIT $4",
        )
        .bind(lifecycle_filter.map(AgentLifecycle::as_str))
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(postgres_agent)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.agent_id))
        } else {
            None
        };
        Ok(AgentManagementPage { items, next_cursor })
    }
    async fn update_agent_labels(
        &self,
        command: UpdateAgentLabelsCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT entity_version,labels FROM managed_agents WHERE agent_id=$1 FOR UPDATE",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("entity_version").map_err(storage)?)?;
        if actual != command.expected_entity_version {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let before: Value = row.try_get("labels").map_err(storage)?;
        let before_hash = prefixed_sha256(&serde_jcs::to_vec(&before).map_err(|_| invalid_data())?);
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query(
            "UPDATE managed_agents SET labels=$1,entity_version=$2,updated_at=$3
             WHERE agent_id=$4 AND entity_version=$5",
        )
        .bind(&command.labels)
        .bind(u64_to_i64(next)?)
        .bind(command.metadata.now)
        .bind(&command.agent_id)
        .bind(u64_to_i64(actual)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let after_hash =
            prefixed_sha256(&serde_jcs::to_vec(&command.labels).map_err(|_| invalid_data())?);
        let response =
            json!({"agent_id":command.agent_id,"labels":command.labels,"entity_version":next});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.labels.updated",
                agent_id: &command.agent_id,
                subject_id: &command.agent_id,
                before_hash: Some(&before_hash),
                after_hash: Some(&after_hash),
                status: 200,
                response,
                etag: Some(agent_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn delete_agent(
        &self,
        command: DeleteAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT entity_version,active_deployment_revision_id
             FROM managed_agents WHERE agent_id=$1 FOR UPDATE",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if i64_to_u64(row.try_get("entity_version").map_err(storage)?)?
            != command.expected_entity_version
        {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let active: Option<String> = row
            .try_get("active_deployment_revision_id")
            .map_err(storage)?;
        let references: i64 = sqlx::query_scalar(
            "SELECT
               (SELECT COUNT(*) FROM agent_definition_publications WHERE agent_id=$1) +
               (SELECT COUNT(*) FROM agent_deployment_publications WHERE agent_id=$1) +
               (SELECT COUNT(*) FROM agent_debug_sessions WHERE agent_id=$1) +
               (SELECT COUNT(*) FROM workflow_runs r JOIN workflow_definitions d
                  ON d.definition_id=r.definition_id WHERE d.agent_id=$1)",
        )
        .bind(&command.agent_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        if active.is_some() || references != 0 {
            return Err(conflict(AgentManagementConflict::Referenced));
        }
        sqlx::query("DELETE FROM managed_agents WHERE agent_id=$1")
            .bind(&command.agent_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        let response = json!({"agent_id":command.agent_id,"deleted":true});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.deleted",
                agent_id: &command.agent_id,
                subject_id: &command.agent_id,
                before_hash: None,
                after_hash: None,
                status: 200,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn get_agent_draft(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentStoredDraft>, RepositoryError> {
        sqlx::query(
            "SELECT agent_id,draft_version,author_hash,document,created_at,updated_at
             FROM agent_drafts WHERE agent_id=$1",
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_draft)
        .transpose()
    }
    async fn replace_agent_draft(
        &self,
        command: ReplaceAgentDraftCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT a.lifecycle,d.draft_version,d.author_hash,d.created_at
             FROM managed_agents a JOIN agent_drafts d USING(agent_id)
             WHERE a.agent_id=$1 FOR UPDATE OF a,d",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if row.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        if actual != command.expected_draft_version {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let before_hash: String = row.try_get("author_hash").map_err(storage)?;
        let created_at: DateTime<Utc> = row.try_get("created_at").map_err(storage)?;
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query(
            "UPDATE agent_drafts SET draft_version=$1,author_hash=$2,document=$3,updated_at=$4
             WHERE agent_id=$5 AND draft_version=$6",
        )
        .bind(u64_to_i64(next)?)
        .bind(&command.author_hash)
        .bind(&command.draft_document)
        .bind(command.metadata.now)
        .bind(&command.agent_id)
        .bind(u64_to_i64(actual)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query("UPDATE managed_agents SET draft_version=$1,updated_at=$2 WHERE agent_id=$3")
            .bind(u64_to_i64(next)?)
            .bind(command.metadata.now)
            .bind(&command.agent_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        let draft = AgentStoredDraft {
            agent_id: command.agent_id.clone(),
            draft_version: next,
            author_hash: command.author_hash.clone(),
            document: command.draft_document,
            created_at,
            updated_at: command.metadata.now,
        };
        let response = serde_json::to_value(&draft).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.draft.replaced",
                agent_id: &draft.agent_id,
                subject_id: &draft.agent_id,
                before_hash: Some(&before_hash),
                after_hash: Some(&draft.author_hash),
                status: 200,
                response,
                etag: Some(draft_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn get_agent_draft_view(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentStoredDraftView>, RepositoryError> {
        sqlx::query(
            "SELECT agent_id,view_version,document,updated_at
             FROM agent_draft_views WHERE agent_id=$1",
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .map(|row| {
            Ok(AgentStoredDraftView {
                agent_id: row.try_get("agent_id").map_err(RepositoryError::storage)?,
                view_version: i64_to_u64(
                    row.try_get("view_version")
                        .map_err(RepositoryError::storage)?,
                )?,
                document: row.try_get("document").map_err(RepositoryError::storage)?,
                updated_at: row
                    .try_get("updated_at")
                    .map_err(RepositoryError::storage)?,
            })
        })
        .transpose()
    }
    async fn replace_agent_draft_view(
        &self,
        command: ReplaceAgentDraftViewCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let agent = sqlx::query(
            "SELECT lifecycle,authoring_mode FROM managed_agents WHERE agent_id=$1 FOR UPDATE",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if agent.try_get::<String, _>("lifecycle").map_err(storage)? != "editable"
            || agent
                .try_get::<String, _>("authoring_mode")
                .map_err(storage)?
                != "graph"
        {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let existing = sqlx::query(
            "SELECT view_version,document FROM agent_draft_views WHERE agent_id=$1 FOR UPDATE",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?;
        let actual = match existing.as_ref() {
            Some(row) => {
                let raw: i64 = row.try_get("view_version").map_err(storage)?;
                i64_to_u64(raw).map_err(AgentManagementWriteError::from)?
            }
            None => 0,
        };
        if actual != command.expected_view_version {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        let before_hash = existing
            .as_ref()
            .map(|row| {
                row.try_get::<Value, _>("document")
                    .map_err(storage)
                    .and_then(|value| encode_json(&value).map_err(AgentManagementWriteError::from))
                    .map(|value| prefixed_sha256(value.as_bytes()))
            })
            .transpose()?;
        let encoded = encode_json(&command.document)?;
        let after_hash = prefixed_sha256(encoded.as_bytes());
        sqlx::query(
            "INSERT INTO agent_draft_views(agent_id,view_version,document,updated_at)
             VALUES($1,$2,$3,$4) ON CONFLICT(agent_id) DO UPDATE SET
             view_version=EXCLUDED.view_version,document=EXCLUDED.document,updated_at=EXCLUDED.updated_at",
        )
        .bind(&command.agent_id)
        .bind(u64_to_i64(next)?)
        .bind(&command.document)
        .bind(command.metadata.now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let view = AgentStoredDraftView {
            agent_id: command.agent_id.clone(),
            view_version: next,
            document: command.document,
            updated_at: command.metadata.now,
        };
        let response = serde_json::to_value(&view).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.draft_view.replaced",
                agent_id: &command.agent_id,
                subject_id: &command.agent_id,
                before_hash: before_hash.as_deref(),
                after_hash: Some(&after_hash),
                status: 200,
                response,
                etag: Some(view_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn create_agent_validation(
        &self,
        command: CreateAgentValidationCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT a.lifecycle,d.draft_version,d.author_hash
             FROM managed_agents a JOIN agent_drafts d USING(agent_id)
             WHERE a.agent_id=$1 FOR UPDATE OF a,d",
        )
        .bind(&command.report.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        let hash: String = row.try_get("author_hash").map_err(storage)?;
        if row.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        if actual != command.expected_draft_version
            || hash != command.expected_author_hash
            || command.report.draft_version != actual
            || command.report.author_hash != hash
        {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        sqlx::query(
            "INSERT INTO agent_validations(
               validation_id,agent_id,draft_version,author_hash,policy_digest,
               operation_status,semantic_hash,report_hash,document,created_at,created_by)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(&command.report.validation_id)
        .bind(&command.report.agent_id)
        .bind(u64_to_i64(command.report.draft_version)?)
        .bind(&command.report.author_hash)
        .bind(&command.report.policy_digest)
        .bind(command.report.status.as_str())
        .bind(&command.report.semantic_hash)
        .bind(&command.report.report_hash)
        .bind(&command.report.document)
        .bind(command.report.created_at)
        .bind(&command.report.created_by)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(&command.report).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.validation.created",
                agent_id: &command.report.agent_id,
                subject_id: &command.report.validation_id,
                before_hash: None,
                after_hash: Some(&command.report.report_hash),
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn get_agent_validation(
        &self,
        agent_id: &str,
        validation_id: &str,
    ) -> Result<Option<AgentValidationReport>, RepositoryError> {
        sqlx::query(
            "SELECT validation_id,agent_id,draft_version,author_hash,policy_digest,
                    operation_status,semantic_hash,report_hash,document,created_at,created_by
             FROM agent_validations WHERE agent_id=$1 AND validation_id=$2",
        )
        .bind(agent_id)
        .bind(validation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_validation)
        .transpose()
    }
    async fn publish_agent_definition(
        &self,
        command: PublishAgentDefinitionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT a.lifecycle,d.draft_version,d.author_hash,v.operation_status,
                    v.draft_version AS validation_draft_version,
                    v.author_hash AS validation_author_hash,v.policy_digest,v.semantic_hash
             FROM managed_agents a
             JOIN agent_drafts d USING(agent_id)
             JOIN agent_validations v ON v.agent_id=a.agent_id AND v.validation_id=$1
             WHERE a.agent_id=$2 FOR UPDATE OF a,d",
        )
        .bind(&command.validation_id)
        .bind(command.plan.agent_id())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if row.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let draft_version = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        let validation_draft =
            i64_to_u64(row.try_get("validation_draft_version").map_err(storage)?)?;
        let author_hash: String = row.try_get("author_hash").map_err(storage)?;
        let validation_author: String = row.try_get("validation_author_hash").map_err(storage)?;
        let policy: String = row.try_get("policy_digest").map_err(storage)?;
        let semantic: Option<String> = row.try_get("semantic_hash").map_err(storage)?;
        if draft_version != command.expected_draft_version
            || validation_draft != draft_version
            || validation_author != author_hash
            || policy != command.validation_policy_digest
        {
            return Err(conflict(AgentManagementConflict::ValidationStale));
        }
        if row
            .try_get::<String, _>("operation_status")
            .map_err(storage)?
            != "succeeded"
            || semantic.as_deref() != Some(command.plan.plan_hash().as_str())
        {
            return Err(conflict(AgentManagementConflict::ValidationFailed));
        }
        install_postgres_plan(
            &mut transaction,
            &command.plan,
            PostgresPlanInstallScope::Definition,
        )
        .await?;
        let revision_number: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision_number),0)+1
             FROM agent_definition_publications WHERE agent_id=$1",
        )
        .bind(command.plan.agent_id())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO agent_definition_publications(
               agent_id,definition_id,definition_revision_id,revision_number,
               source_draft_version,validation_id,author_hash,created_at,created_by)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        )
        .bind(command.plan.agent_id())
        .bind(command.plan.definition_id())
        .bind(command.plan.definition_revision_id().as_str())
        .bind(revision_number)
        .bind(u64_to_i64(draft_version)?)
        .bind(&command.validation_id)
        .bind(&author_hash)
        .bind(command.metadata.now)
        .bind(&command.metadata.operator_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({
            "agent_id":command.plan.agent_id(),
            "definition_revision_id":command.plan.definition_revision_id(),
            "revision_number":revision_number,
            "semantic_hash":command.plan.plan_hash()
        });
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.definition.published",
                agent_id: command.plan.agent_id(),
                subject_id: command.plan.definition_revision_id().as_str(),
                before_hash: Some(&author_hash),
                after_hash: Some(command.plan.plan_hash().as_str()),
                status: 201,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn get_agent_definition(
        &self,
        agent_id: &str,
        definition_revision_id: &str,
    ) -> Result<Option<AgentDefinitionRevision>, RepositoryError> {
        sqlx::query(
            "SELECT p.agent_id,p.definition_id,p.definition_revision_id,p.revision_number,
                    p.source_draft_version,p.validation_id,p.author_hash,p.created_at,p.created_by,
                    r.plan_hash,r.compiler_version,r.expression_engine_version,r.author_document,
                    r.canonical_plan,r.descriptor_contracts,m.display_name,m.public_description
             FROM agent_definition_publications p
             JOIN workflow_definition_revisions r
               ON r.definition_id=p.definition_id AND r.definition_revision_id=p.definition_revision_id
             JOIN workflow_definition_public_metadata m
               ON m.definition_id=p.definition_id AND m.definition_revision_id=p.definition_revision_id
             WHERE p.agent_id=$1 AND p.definition_revision_id=$2",
        )
        .bind(agent_id)
        .bind(definition_revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_definition)
        .transpose()
    }
    async fn list_agent_definitions(
        &self,
        agent_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<AgentDefinitionRevision>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT p.agent_id,p.definition_id,p.definition_revision_id,p.revision_number,
                    p.source_draft_version,p.validation_id,p.author_hash,p.created_at,p.created_by,
                    r.plan_hash,r.compiler_version,r.expression_engine_version,r.author_document,
                    r.canonical_plan,r.descriptor_contracts,m.display_name,m.public_description
             FROM agent_definition_publications p
             JOIN workflow_definition_revisions r
               ON r.definition_id=p.definition_id AND r.definition_revision_id=p.definition_revision_id
             JOIN workflow_definition_public_metadata m
               ON m.definition_id=p.definition_id AND m.definition_revision_id=p.definition_revision_id
             WHERE p.agent_id=$1
               AND ($2::timestamptz IS NULL OR p.created_at>$2 OR
                    (p.created_at=$2 AND p.definition_revision_id>$3))
             ORDER BY p.created_at,p.definition_revision_id LIMIT $4",
        )
        .bind(agent_id)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(postgres_definition)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.definition_revision_id))
        } else {
            None
        };
        Ok(AgentManagementPage { items, next_cursor })
    }
    async fn create_agent_deployment_resolution(
        &self,
        command: CreateAgentResolutionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let editable: Option<String> = sqlx::query_scalar(
            "SELECT a.lifecycle FROM managed_agents a
             JOIN agent_definition_publications p ON p.agent_id=a.agent_id
             WHERE a.agent_id=$1 AND p.definition_revision_id=$2 FOR UPDATE OF a",
        )
        .bind(&command.resolution.agent_id)
        .bind(&command.resolution.definition_revision_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?;
        match editable.as_deref() {
            None => return Err(conflict(AgentManagementConflict::NotFound)),
            Some("editable") => {}
            Some(_) => return Err(conflict(AgentManagementConflict::ForbiddenState)),
        }
        sqlx::query(
            "INSERT INTO agent_deployment_resolutions(
               resolution_id,agent_id,definition_revision_id,operation_status,
               catalog_snapshot_hash,resolution_hash,resolved_bindings,worker_contracts,
               dependency_heads,risks,expires_at,created_at,created_by)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
        )
        .bind(&command.resolution.resolution_id)
        .bind(&command.resolution.agent_id)
        .bind(&command.resolution.definition_revision_id)
        .bind(command.resolution.status.as_str())
        .bind(&command.resolution.catalog_snapshot_hash)
        .bind(&command.resolution.resolution_hash)
        .bind(&command.resolution.resolved_bindings)
        .bind(&command.resolution.worker_contracts)
        .bind(&command.resolution.dependency_heads)
        .bind(&command.resolution.risks)
        .bind(command.resolution.expires_at)
        .bind(command.resolution.created_at)
        .bind(&command.resolution.created_by)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(&command.resolution).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.deployment_resolution.created",
                agent_id: &command.resolution.agent_id,
                subject_id: &command.resolution.resolution_id,
                before_hash: None,
                after_hash: Some(&command.resolution.resolution_hash),
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn get_agent_deployment_resolution(
        &self,
        agent_id: &str,
        resolution_id: &str,
    ) -> Result<Option<AgentDeploymentResolution>, RepositoryError> {
        sqlx::query(
            "SELECT resolution_id,agent_id,definition_revision_id,operation_status,
                    catalog_snapshot_hash,resolution_hash,resolved_bindings,worker_contracts,
                    dependency_heads,risks,expires_at,created_at,created_by
             FROM agent_deployment_resolutions WHERE agent_id=$1 AND resolution_id=$2",
        )
        .bind(agent_id)
        .bind(resolution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_resolution)
        .transpose()
    }
    async fn install_agent_deployment(
        &self,
        command: InstallAgentDeploymentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT a.lifecycle,r.definition_revision_id,r.operation_status,r.resolution_hash,
                    r.resolved_bindings,r.worker_contracts,r.dependency_heads,r.expires_at
             FROM managed_agents a JOIN agent_deployment_resolutions r USING(agent_id)
             WHERE a.agent_id=$1 AND r.resolution_id=$2 FOR UPDATE OF a",
        )
        .bind(command.plan.agent_id())
        .bind(&command.resolution_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if row.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        if row
            .try_get::<String, _>("operation_status")
            .map_err(storage)?
            != "succeeded"
        {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let expires_at: DateTime<Utc> = row.try_get("expires_at").map_err(storage)?;
        if expires_at <= command.metadata.now {
            return Err(conflict(AgentManagementConflict::ResolutionExpired));
        }
        let resolution_hash: String = row.try_get("resolution_hash").map_err(storage)?;
        let definition_revision_id: String =
            row.try_get("definition_revision_id").map_err(storage)?;
        let bindings: Value = row.try_get("resolved_bindings").map_err(storage)?;
        let workers: Value = row.try_get("worker_contracts").map_err(storage)?;
        let heads: Value = row.try_get("dependency_heads").map_err(storage)?;
        if resolution_hash != command.expected_resolution_hash
            || definition_revision_id != command.plan.definition_revision_id().as_str()
            || bindings != *command.plan.resolved_bindings()
            || workers != *command.plan.worker_contracts()
            || heads != command.expected_dependency_heads
        {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let mut expected_heads: Vec<PublicationHead> =
            serde_json::from_value(command.expected_dependency_heads.clone())
                .map_err(|_| invalid_data())?;
        expected_heads.sort_by(|left, right| left.agent_id().cmp(right.agent_id()));
        for expected in expected_heads {
            let actual = sqlx::query(
                "SELECT agent_id,definition_id,definition_revision_id,deployment_revision_id,
                        publication_origin FROM agent_publication_heads
                 WHERE agent_id=$1 FOR SHARE",
            )
            .bind(expected.agent_id())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?;
            let actual = actual
                .as_ref()
                .map(|row| {
                    insight_durable::AgentDeploymentTarget::new(
                        row.try_get("agent_id").map_err(RepositoryError::storage)?,
                        row.try_get("definition_id")
                            .map_err(RepositoryError::storage)?,
                        insight_engine::DefinitionRevisionId::new(
                            row.try_get::<String, _>("definition_revision_id")
                                .map_err(RepositoryError::storage)?,
                        )
                        .map_err(|_| invalid_data())?,
                        insight_engine::DeploymentRevisionId::new(
                            row.try_get::<String, _>("deployment_revision_id")
                                .map_err(RepositoryError::storage)?,
                        )
                        .map_err(|_| invalid_data())?,
                        match row
                            .try_get::<String, _>("publication_origin")
                            .map_err(RepositoryError::storage)?
                            .as_str()
                        {
                            "built_in" => PublicationOrigin::BuiltIn,
                            "graph" => PublicationOrigin::Graph,
                            "managed" => PublicationOrigin::Managed,
                            _ => return Err(invalid_data()),
                        },
                    )?
                    .publication_head()
                })
                .transpose()?;
            if actual.as_ref() != Some(&expected) {
                return Err(conflict(AgentManagementConflict::DependencyHeadChanged));
            }
        }
        install_postgres_plan(
            &mut transaction,
            &command.plan,
            PostgresPlanInstallScope::Deployment,
        )
        .await?;
        sqlx::query(
            "INSERT INTO agent_deployment_publications(
               agent_id,definition_id,definition_revision_id,deployment_revision_id,
               resolution_id,created_at,created_by) VALUES($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(command.plan.agent_id())
        .bind(command.plan.definition_id())
        .bind(command.plan.definition_revision_id().as_str())
        .bind(command.plan.deployment_revision_id().as_str())
        .bind(&command.resolution_id)
        .bind(command.metadata.now)
        .bind(&command.metadata.operator_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({
            "agent_id":command.plan.agent_id(),
            "definition_revision_id":command.plan.definition_revision_id(),
            "deployment_revision_id":command.plan.deployment_revision_id(),
            "binding_hash":command.plan.binding_hash(),
            "resolution_id":command.resolution_id
        });
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.deployment.installed",
                agent_id: command.plan.agent_id(),
                subject_id: command.plan.deployment_revision_id().as_str(),
                before_hash: Some(&resolution_hash),
                after_hash: Some(command.plan.binding_hash().as_str()),
                status: 201,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn get_agent_deployment(
        &self,
        agent_id: &str,
        deployment_revision_id: &str,
    ) -> Result<Option<AgentDeploymentRevision>, RepositoryError> {
        sqlx::query(
            "SELECT p.agent_id,p.definition_id,p.definition_revision_id,p.deployment_revision_id,
                    p.resolution_id,p.created_at,p.created_by,x.plan_hash,x.binding_hash,
                    x.resolved_bindings,x.worker_contracts
             FROM agent_deployment_publications p
             JOIN deployment_revisions x
               ON x.definition_id=p.definition_id AND x.deployment_revision_id=p.deployment_revision_id
             WHERE p.agent_id=$1 AND p.deployment_revision_id=$2",
        )
        .bind(agent_id)
        .bind(deployment_revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_deployment)
        .transpose()
    }
    async fn list_agent_deployments(
        &self,
        agent_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<AgentDeploymentRevision>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT p.agent_id,p.definition_id,p.definition_revision_id,p.deployment_revision_id,
                    p.resolution_id,p.created_at,p.created_by,x.plan_hash,x.binding_hash,
                    x.resolved_bindings,x.worker_contracts
             FROM agent_deployment_publications p
             JOIN deployment_revisions x
               ON x.definition_id=p.definition_id AND x.deployment_revision_id=p.deployment_revision_id
             WHERE p.agent_id=$1
               AND ($2::timestamptz IS NULL OR p.created_at>$2 OR
                    (p.created_at=$2 AND p.deployment_revision_id>$3))
             ORDER BY p.created_at,p.deployment_revision_id LIMIT $4",
        )
        .bind(agent_id)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(postgres_deployment)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.deployment_revision_id))
        } else {
            None
        };
        Ok(AgentManagementPage { items, next_cursor })
    }
    async fn activate_managed_agent_deployment(
        &self,
        command: ActivateManagedAgentDeploymentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let agent = sqlx::query(
            "SELECT lifecycle,entity_version,active_definition_revision_id,
                    active_deployment_revision_id FROM managed_agents
             WHERE agent_id=$1 FOR UPDATE",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        if agent.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let version = i64_to_u64(agent.try_get("entity_version").map_err(storage)?)?;
        if version != command.expected_entity_version {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        let target = sqlx::query(
            "SELECT p.definition_id,p.definition_revision_id,x.resolved_bindings,
                    r.dependency_heads
             FROM agent_deployment_publications p
             JOIN deployment_revisions x
               ON x.definition_id=p.definition_id AND x.deployment_revision_id=p.deployment_revision_id
             JOIN agent_deployment_resolutions r ON r.resolution_id=p.resolution_id
             WHERE p.agent_id=$1 AND p.deployment_revision_id=$2",
        )
        .bind(&command.agent_id)
        .bind(&command.deployment_revision_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        let definition_id: String = target.try_get("definition_id").map_err(storage)?;
        let definition_revision_id: String =
            target.try_get("definition_revision_id").map_err(storage)?;
        let resolved_bindings: Value = target.try_get("resolved_bindings").map_err(storage)?;
        let dependency_heads: Value = target.try_get("dependency_heads").map_err(storage)?;
        postgres_validate_activation_dependencies(
            &mut transaction,
            &resolved_bindings,
            &dependency_heads,
        )
        .await?;
        let current_route: Option<(String, String)> = sqlx::query_as(
            "SELECT definition_revision_id,deployment_revision_id
             FROM agent_publication_heads WHERE agent_id=$1 FOR UPDATE",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?;
        let entity_definition: Option<String> = agent
            .try_get("active_definition_revision_id")
            .map_err(storage)?;
        let entity_deployment: Option<String> = agent
            .try_get("active_deployment_revision_id")
            .map_err(storage)?;
        if current_route != entity_definition.clone().zip(entity_deployment.clone()) {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        sqlx::query(
            "INSERT INTO agent_publication_heads(
               agent_id,definition_id,definition_revision_id,deployment_revision_id,
               publication_origin,updated_at) VALUES($1,$2,$3,$4,'managed',$5)
             ON CONFLICT(agent_id) DO UPDATE SET definition_id=EXCLUDED.definition_id,
               definition_revision_id=EXCLUDED.definition_revision_id,
               deployment_revision_id=EXCLUDED.deployment_revision_id,
               publication_origin='managed',updated_at=EXCLUDED.updated_at",
        )
        .bind(&command.agent_id)
        .bind(&definition_id)
        .bind(&definition_revision_id)
        .bind(&command.deployment_revision_id)
        .bind(command.metadata.now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let next = version.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query(
            "UPDATE managed_agents SET active_definition_revision_id=$1,
               active_deployment_revision_id=$2,entity_version=$3,
               archived_publication_head=NULL,updated_at=$4
             WHERE agent_id=$5 AND entity_version=$6",
        )
        .bind(&definition_revision_id)
        .bind(&command.deployment_revision_id)
        .bind(u64_to_i64(next)?)
        .bind(command.metadata.now)
        .bind(&command.agent_id)
        .bind(u64_to_i64(version)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({
            "agent_id":command.agent_id,
            "definition_revision_id":definition_revision_id,
            "deployment_revision_id":command.deployment_revision_id,
            "entity_version":next
        });
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.deployment.activated",
                agent_id: &command.agent_id,
                subject_id: &command.deployment_revision_id,
                before_hash: entity_deployment.as_deref(),
                after_hash: Some(&command.deployment_revision_id),
                status: 200,
                response,
                etag: Some(agent_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn deactivate_managed_agent(
        &self,
        command: DeactivateManagedAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        postgres_deactivate_or_archive(
            self,
            command.metadata,
            command.agent_id,
            command.expected_entity_version,
            false,
        )
        .await
    }
    async fn archive_agent(
        &self,
        command: ArchiveAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        postgres_deactivate_or_archive(
            self,
            command.metadata,
            command.agent_id,
            command.expected_entity_version,
            true,
        )
        .await
    }
    async fn restore_agent(
        &self,
        command: RestoreAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT lifecycle,entity_version FROM managed_agents WHERE agent_id=$1 FOR UPDATE",
        )
        .bind(&command.agent_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        let version = i64_to_u64(row.try_get("entity_version").map_err(storage)?)?;
        if version != command.expected_entity_version {
            return Err(conflict(AgentManagementConflict::PreconditionFailed));
        }
        if row.try_get::<String, _>("lifecycle").map_err(storage)? != "archived" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let next = version.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query(
            "UPDATE managed_agents SET lifecycle='editable',entity_version=$1,
               active_definition_revision_id=NULL,active_deployment_revision_id=NULL,
               updated_at=$2 WHERE agent_id=$3 AND entity_version=$4",
        )
        .bind(u64_to_i64(next)?)
        .bind(command.metadata.now)
        .bind(&command.agent_id)
        .bind(u64_to_i64(version)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response =
            json!({"agent_id":command.agent_id,"lifecycle":"editable","entity_version":next});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.restored",
                agent_id: &command.agent_id,
                subject_id: &command.agent_id,
                before_hash: None,
                after_hash: None,
                status: 200,
                response,
                etag: Some(agent_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn create_agent_debug_session(
        &self,
        command: CreateAgentDebugSessionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let lifecycle: Option<String> =
            sqlx::query_scalar("SELECT lifecycle FROM managed_agents WHERE agent_id=$1 FOR UPDATE")
                .bind(&command.session.agent_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage)?;
        match lifecycle.as_deref() {
            None => return Err(conflict(AgentManagementConflict::NotFound)),
            Some("editable") => {}
            Some(_) => return Err(conflict(AgentManagementConflict::ForbiddenState)),
        }
        if let Some((expected_version, expected_hash)) = debug_draft_pin(&command.session.source) {
            let draft: Option<(i64, String)> = sqlx::query_as(
                "SELECT draft_version,author_hash FROM agent_drafts WHERE agent_id=$1 FOR SHARE",
            )
            .bind(&command.session.agent_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?;
            if draft
                .and_then(|(version, hash)| i64_to_u64(version).ok().map(|version| (version, hash)))
                .as_ref()
                .is_none_or(|(version, hash)| *version != expected_version || hash != expected_hash)
            {
                return Err(conflict(AgentManagementConflict::PreconditionFailed));
            }
        }
        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_debug_sessions
             WHERE agent_id=$1 AND session_status IN('queued','running')",
        )
        .bind(&command.session.agent_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        if active >= i64::from(command.max_active_sessions) {
            return Err(conflict(AgentManagementConflict::CapacityExceeded));
        }
        sqlx::query(
            "INSERT INTO agent_debug_sessions(
               debug_session_id,agent_id,source,source_hash,execution_profile_id,
               profile_mode,session_status,definition_revision_id,deployment_revision_id,
               run_id,failure_code,expires_at,created_at,finished_at,created_by)
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        )
        .bind(&command.session.debug_session_id)
        .bind(&command.session.agent_id)
        .bind(&command.session.source)
        .bind(&command.session.source_hash)
        .bind(&command.session.execution_profile_id)
        .bind(&command.session.profile_mode)
        .bind(command.session.status.as_str())
        .bind(&command.session.definition_revision_id)
        .bind(&command.session.deployment_revision_id)
        .bind(&command.session.run_id)
        .bind(&command.session.failure_code)
        .bind(command.session.expires_at)
        .bind(command.session.created_at)
        .bind(command.session.finished_at)
        .bind(&command.session.created_by)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO agent_debug_content_retention(debug_session_id,retain_until,content_deleted_at)
             VALUES($1,$2,NULL)",
        )
        .bind(&command.session.debug_session_id)
        .bind(command.retain_until)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(&command.session).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.debug.created",
                agent_id: &command.session.agent_id,
                subject_id: &command.session.debug_session_id,
                before_hash: None,
                after_hash: Some(&command.session.source_hash),
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn get_agent_debug_session(
        &self,
        agent_id: &str,
        debug_session_id: &str,
    ) -> Result<Option<AgentDebugSession>, RepositoryError> {
        sqlx::query(
            "SELECT debug_session_id,agent_id,source,source_hash,execution_profile_id,
                    profile_mode,session_status,definition_revision_id,deployment_revision_id,
                    run_id,failure_code,expires_at,created_at,finished_at,created_by
             FROM agent_debug_sessions WHERE agent_id=$1 AND debug_session_id=$2",
        )
        .bind(agent_id)
        .bind(debug_session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_debug)
        .transpose()
    }
    async fn list_agent_debug_sessions(
        &self,
        agent_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<AgentDebugSession>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT debug_session_id,agent_id,source,source_hash,execution_profile_id,
                    profile_mode,session_status,definition_revision_id,deployment_revision_id,
                    run_id,failure_code,expires_at,created_at,finished_at,created_by
             FROM agent_debug_sessions
             WHERE agent_id=$1
               AND ($2::timestamptz IS NULL OR created_at>$2 OR
                    (created_at=$2 AND debug_session_id>$3))
             ORDER BY created_at,debug_session_id LIMIT $4",
        )
        .bind(agent_id)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(postgres_debug)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.debug_session_id))
        } else {
            None
        };
        Ok(AgentManagementPage { items, next_cursor })
    }
    async fn complete_agent_debug_session(
        &self,
        command: CompleteAgentDebugSessionCommand,
    ) -> Result<(), AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(plan) = command.plan.as_ref() {
            install_postgres_plan(&mut transaction, plan, PostgresPlanInstallScope::All).await?;
        }
        let finished_at = (!matches!(
            command.status,
            AgentDebugStatus::Queued | AgentDebugStatus::Running
        ))
        .then_some(command.now);
        let updated = sqlx::query(
            "UPDATE agent_debug_sessions SET session_status=$1,definition_revision_id=$2,
               deployment_revision_id=$3,run_id=$4,failure_code=$5,finished_at=$6
             WHERE debug_session_id=$7 AND session_status IN('queued','running')",
        )
        .bind(command.status.as_str())
        .bind(command.definition_revision_id)
        .bind(command.deployment_revision_id)
        .bind(command.run_id)
        .bind(command.failure_code)
        .bind(finished_at)
        .bind(command.debug_session_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?
        .rows_affected();
        if updated != 1 {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        transaction.commit().await.map_err(storage)?;
        Ok(())
    }
    async fn cancel_agent_debug_session(
        &self,
        command: CancelAgentDebugSessionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT session_status FROM agent_debug_sessions
             WHERE agent_id=$1 AND debug_session_id=$2 FOR UPDATE",
        )
        .bind(&command.agent_id)
        .bind(&command.debug_session_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
        let status: String = row.try_get("session_status").map_err(storage)?;
        if matches!(status.as_str(), "queued" | "running") {
            sqlx::query(
                "UPDATE agent_debug_sessions SET session_status='cancelled',finished_at=$1
                 WHERE debug_session_id=$2",
            )
            .bind(command.metadata.now)
            .bind(&command.debug_session_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        } else if status != "cancelled" {
            return Err(conflict(AgentManagementConflict::ForbiddenState));
        }
        let response = json!({"agent_id":command.agent_id,"debug_session_id":command.debug_session_id,"status":"cancelled"});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "agent.debug.cancelled",
                agent_id: &command.agent_id,
                subject_id: &command.debug_session_id,
                before_hash: None,
                after_hash: None,
                status: 200,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }
    async fn cleanup_expired_agent_debug_sessions(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError> {
        let mut transaction = begin_write_transaction(&self.pool).await?;
        let ids = sqlx::query_scalar::<_, String>(
            "SELECT debug_session_id FROM agent_debug_sessions
             WHERE expires_at<=$1 AND session_status IN('queued','running')
             ORDER BY expires_at,debug_session_id FOR UPDATE SKIP LOCKED LIMIT $2",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        for id in &ids {
            sqlx::query(
                "UPDATE agent_debug_sessions SET session_status='expired',finished_at=$1
                 WHERE debug_session_id=$2 AND session_status IN('queued','running')",
            )
            .bind(now)
            .bind(id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        let redact_ids = sqlx::query_scalar::<_, String>(
            "SELECT debug_session_id FROM agent_debug_content_retention
             WHERE retain_until<=$1 AND content_deleted_at IS NULL
             ORDER BY retain_until,debug_session_id FOR UPDATE SKIP LOCKED LIMIT $2",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        for id in &redact_ids {
            sqlx::query(
                "UPDATE agent_debug_sessions SET source='{\"content_deleted\":true}'::jsonb
                 WHERE debug_session_id=$1",
            )
            .bind(id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "UPDATE agent_management_requests
                 SET response_json=jsonb_set(response_json,'{source}','{\"content_deleted\":true}'::jsonb,true)
                 WHERE response_json->>'debug_session_id'=$1",
            )
            .bind(id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "UPDATE agent_debug_content_retention SET content_deleted_at=$1
                 WHERE debug_session_id=$2 AND content_deleted_at IS NULL",
            )
            .bind(now)
            .bind(id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok((ids.len() + redact_ids.len()) as u64)
    }

    async fn load_agent_management_runtime_stats(
        &self,
    ) -> Result<AgentManagementRuntimeStats, RepositoryError> {
        let summary = sqlx::query(
            "SELECT
               (SELECT COUNT(*) FROM agent_drafts) drafts_current,
               (SELECT COUNT(*) FROM agent_validations WHERE operation_status IN('queued','running')) validations_pending,
               (SELECT COUNT(*) FROM agent_deployment_resolutions WHERE operation_status IN('queued','running')) deployment_resolutions_pending,
               (SELECT COUNT(*) FROM managed_agents WHERE lifecycle='editable' AND active_deployment_revision_id IS NOT NULL) active_agents,
               (SELECT COUNT(*) FROM managed_agents WHERE lifecycle='archived') archived_agents",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let debug_sessions = sqlx::query(
            "SELECT session_status,profile_mode,COUNT(*) count
             FROM agent_debug_sessions GROUP BY session_status,profile_mode
             ORDER BY session_status,profile_mode",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .iter()
        .map(|row| {
            Ok(AgentDebugRuntimeCount {
                state: debug_status(
                    &row.try_get::<String, _>("session_status")
                        .map_err(RepositoryError::storage)?,
                )?,
                profile_mode: row
                    .try_get("profile_mode")
                    .map_err(RepositoryError::storage)?,
                count: i64_to_u64(row.try_get("count").map_err(RepositoryError::storage)?)?,
            })
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
        let operations = sqlx::query(
            "SELECT event_kind,result_code,COUNT(*) count
             FROM agent_management_audit_events GROUP BY event_kind,result_code
             ORDER BY event_kind,result_code",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .iter()
        .map(|row| {
            Ok(AgentManagementOperationCount {
                operation: row
                    .try_get("event_kind")
                    .map_err(RepositoryError::storage)?,
                outcome: row
                    .try_get("result_code")
                    .map_err(RepositoryError::storage)?,
                count: i64_to_u64(row.try_get("count").map_err(RepositoryError::storage)?)?,
            })
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
        Ok(AgentManagementRuntimeStats {
            drafts_current: i64_to_u64(
                summary
                    .try_get("drafts_current")
                    .map_err(RepositoryError::storage)?,
            )?,
            validations_pending: i64_to_u64(
                summary
                    .try_get("validations_pending")
                    .map_err(RepositoryError::storage)?,
            )?,
            deployment_resolutions_pending: i64_to_u64(
                summary
                    .try_get("deployment_resolutions_pending")
                    .map_err(RepositoryError::storage)?,
            )?,
            active_agents: i64_to_u64(
                summary
                    .try_get("active_agents")
                    .map_err(RepositoryError::storage)?,
            )?,
            archived_agents: i64_to_u64(
                summary
                    .try_get("archived_agents")
                    .map_err(RepositoryError::storage)?,
            )?,
            debug_sessions,
            operations,
        })
    }
}

async fn postgres_deactivate_or_archive(
    repository: &PostgresDurableRepository,
    metadata: AgentMutationMetadata,
    agent_id: String,
    expected_entity_version: u64,
    archive: bool,
) -> Result<AgentMutationReceipt, AgentManagementWriteError> {
    let mut transaction = begin_write_transaction(&repository.pool).await?;
    if let Some(receipt) = postgres_replay(&mut transaction, &metadata).await? {
        transaction.commit().await.map_err(storage)?;
        return Ok(receipt);
    }
    let row = sqlx::query(
        "SELECT lifecycle,entity_version,active_definition_revision_id,
                active_deployment_revision_id FROM managed_agents
         WHERE agent_id=$1 FOR UPDATE",
    )
    .bind(&agent_id)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(storage)?
    .ok_or_else(|| conflict(AgentManagementConflict::NotFound))?;
    let version = i64_to_u64(row.try_get("entity_version").map_err(storage)?)?;
    if version != expected_entity_version {
        return Err(conflict(AgentManagementConflict::PreconditionFailed));
    }
    if row.try_get::<String, _>("lifecycle").map_err(storage)? != "editable" {
        return Err(conflict(AgentManagementConflict::ForbiddenState));
    }
    let active_definition: Option<String> = row
        .try_get("active_definition_revision_id")
        .map_err(storage)?;
    let active_deployment: Option<String> = row
        .try_get("active_deployment_revision_id")
        .map_err(storage)?;
    let publication = match active_definition.as_ref().zip(active_deployment.as_ref()) {
        Some((definition_revision_id, deployment_revision_id)) => {
            let head = sqlx::query(
                "SELECT definition_id,publication_origin FROM agent_publication_heads
                 WHERE agent_id=$1 AND definition_revision_id=$2
                   AND deployment_revision_id=$3 FOR UPDATE",
            )
            .bind(&agent_id)
            .bind(definition_revision_id)
            .bind(deployment_revision_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?
            .ok_or_else(|| conflict(AgentManagementConflict::PreconditionFailed))?;
            if head
                .try_get::<String, _>("publication_origin")
                .map_err(storage)?
                != "managed"
            {
                return Err(conflict(AgentManagementConflict::PreconditionFailed));
            }
            Some(
                insight_durable::AgentDeploymentTarget::new(
                    agent_id.clone(),
                    head.try_get("definition_id").map_err(storage)?,
                    insight_engine::DefinitionRevisionId::new(definition_revision_id.clone())
                        .map_err(|_| invalid_data())?,
                    insight_engine::DeploymentRevisionId::new(deployment_revision_id.clone())
                        .map_err(|_| invalid_data())?,
                    PublicationOrigin::Managed,
                )?
                .publication_head()?,
            )
        }
        None if active_definition.is_none() && active_deployment.is_none() => None,
        None => return Err(conflict(AgentManagementConflict::PreconditionFailed)),
    };
    sqlx::query("DELETE FROM agent_publication_heads WHERE agent_id=$1")
        .bind(&agent_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
    let next = version.checked_add(1).ok_or_else(invalid_data)?;
    let archived = if archive {
        publication
            .as_ref()
            .map(|value| serde_json::to_value(value).map_err(|_| invalid_data()))
            .transpose()?
    } else {
        None
    };
    sqlx::query(
        "UPDATE managed_agents SET lifecycle=$1,entity_version=$2,
           active_definition_revision_id=NULL,active_deployment_revision_id=NULL,
           archived_publication_head=$3,updated_at=$4
         WHERE agent_id=$5 AND entity_version=$6",
    )
    .bind(if archive { "archived" } else { "editable" })
    .bind(u64_to_i64(next)?)
    .bind(archived)
    .bind(metadata.now)
    .bind(&agent_id)
    .bind(u64_to_i64(version)?)
    .execute(&mut *transaction)
    .await
    .map_err(storage)?;
    let response = json!({
        "agent_id":agent_id,
        "lifecycle":if archive { "archived" } else { "editable" },
        "active_deployment_revision_id":null,
        "entity_version":next
    });
    let receipt = postgres_finalize(
        &mut transaction,
        &metadata,
        PostgresFinalize {
            event_kind: if archive {
                "agent.archived"
            } else {
                "agent.deactivated"
            },
            agent_id: &agent_id,
            subject_id: &agent_id,
            before_hash: active_deployment.as_deref(),
            after_hash: None,
            status: 200,
            response,
            etag: Some(agent_etag(next)),
        },
    )
    .await?;
    transaction.commit().await.map_err(storage)?;
    Ok(receipt)
}
