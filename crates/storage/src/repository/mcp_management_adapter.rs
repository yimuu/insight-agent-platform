use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row, Sqlite, Transaction};
use uuid::Uuid;

use insight_durable::{
    ActivateMcpRevisionCommand, CancelMcpDiscoveryCommand, ClaimMcpDiscoveriesCommand,
    CompleteMcpDiscoveryCommand, CompleteMcpDiscoveryResult, CreateMcpDiscoveryCommand,
    CreateMcpManifestCommand, CreateMcpServerCommand, CreateMcpValidationCommand,
    DeleteMcpServerCommand, DisableMcpServerCommand, MarkMcpDiscoveryStaleCommand,
    McpDiscoveryClaim, McpDiscoveryFailure, McpDiscoveryOperation, McpDiscoverySnapshot,
    McpDiscoveryStatus, McpManagedServer, McpManagedServerState, McpManagementConflict,
    McpManagementDurableRepository, McpManagementPage, McpManagementRuntimeStats,
    McpManagementWriteError, McpMutationMetadata, McpMutationReceipt, McpServerFence,
    McpServerRevision, McpSignedManifest, McpStoredDraft, McpValidationReport,
    PublishMcpRevisionCommand, RecordMcpManagementRejectionCommand, ReplaceMcpDraftCommand,
    RepositoryError, RetireMcpServerCommand,
};

use super::{
    database_time, PostgresDurableRepository, RepositoryErrorExt as _, SqliteDurableRepository,
};

fn storage(error: sqlx::Error) -> McpManagementWriteError {
    McpManagementWriteError::Repository(RepositoryError::storage(error))
}

fn invalid_data() -> RepositoryError {
    RepositoryError::invalid_data()
}

fn encode_json(value: &impl Serialize) -> Result<String, RepositoryError> {
    serde_jcs::to_string(value).map_err(|_| invalid_data())
}

fn decode_json(value: &str) -> Result<Value, RepositoryError> {
    serde_json::from_str(value).map_err(|_| invalid_data())
}

fn u64_to_i64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| invalid_data())
}

fn i64_to_u64(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| invalid_data())
}

fn i64_to_u32(value: i64) -> Result<u32, RepositoryError> {
    u32::try_from(value).map_err(|_| invalid_data())
}

fn state(value: &str) -> Result<McpManagedServerState, RepositoryError> {
    match value {
        "draft" => Ok(McpManagedServerState::Draft),
        "active" => Ok(McpManagedServerState::Active),
        "disabled" => Ok(McpManagedServerState::Disabled),
        "retired" => Ok(McpManagedServerState::Retired),
        _ => Err(invalid_data()),
    }
}

fn discovery_status(value: &str) -> Result<McpDiscoveryStatus, RepositoryError> {
    match value {
        "pending" => Ok(McpDiscoveryStatus::Pending),
        "running" => Ok(McpDiscoveryStatus::Running),
        "succeeded" => Ok(McpDiscoveryStatus::Succeeded),
        "failed" => Ok(McpDiscoveryStatus::Failed),
        "cancelled" => Ok(McpDiscoveryStatus::Cancelled),
        _ => Err(invalid_data()),
    }
}

fn server_etag(version: u64) -> String {
    format!("\"server-{version}\"")
}

fn draft_etag(version: u64) -> String {
    format!("\"draft-{version}\"")
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

fn canonical_hash(value: &Value) -> Result<String, RepositoryError> {
    serde_jcs::to_vec(value)
        .map(|bytes| prefixed_sha256(&bytes))
        .map_err(|_| invalid_data())
}

#[derive(Clone)]
struct DiscoveryToolIndex {
    remote_name: String,
    schema_hash: String,
    document: Value,
}

#[derive(Clone)]
struct NamedDocumentIndex {
    kind: &'static str,
    identity: String,
    document: Value,
}

#[derive(Clone)]
struct RevisionToolIndex {
    remote_name: String,
    alias: String,
    action_id: String,
    binding_hash: String,
    document: Value,
}

#[derive(Clone, Default)]
struct DiscoveryIndexes {
    tools: Vec<DiscoveryToolIndex>,
    resources: Vec<NamedDocumentIndex>,
    prompts: Vec<NamedDocumentIndex>,
}

#[derive(Clone, Default)]
struct RevisionIndexes {
    tools: Vec<RevisionToolIndex>,
    resources: Vec<NamedDocumentIndex>,
    prompts: Vec<NamedDocumentIndex>,
}

fn bounded_identity(value: Option<&Value>, max: usize) -> Result<String, ()> {
    value
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
        })
        .map(str::to_owned)
        .ok_or(())
}

fn canonical_sha256(value: Option<&Value>) -> Result<String, ()> {
    bounded_identity(value, 71).and_then(|value| {
        (value.len() == 71
            && value.starts_with("sha256:")
            && value[7..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        .then_some(value)
        .ok_or(())
    })
}

fn object_array<'a>(document: &'a Value, pointer: &str) -> Result<&'a [Value], ()> {
    match document.pointer(pointer) {
        None => Ok(&[]),
        Some(Value::Array(items)) if items.iter().all(Value::is_object) => Ok(items),
        Some(_) => Err(()),
    }
}

fn discovery_indexes(document: &Value) -> Result<DiscoveryIndexes, ()> {
    let mut indexes = DiscoveryIndexes::default();
    let mut tool_names = BTreeSet::new();
    for tool in object_array(document, "/tools")? {
        let remote_name = bounded_identity(tool.get("remote").or_else(|| tool.get("name")), 128)?;
        if !tool_names.insert(remote_name.clone()) {
            return Err(());
        }
        indexes.tools.push(DiscoveryToolIndex {
            remote_name,
            schema_hash: canonical_sha256(tool.get("schema_hash"))?,
            document: tool.clone(),
        });
    }
    for (pointer, kind, identity_fields) in [
        ("/resources", "resource", ["uri", "uri"]),
        (
            "/resource_templates",
            "template",
            ["uriTemplate", "uri_template"],
        ),
    ] {
        let mut identities = BTreeSet::new();
        for item in object_array(document, pointer)? {
            let identity = bounded_identity(
                item.get(identity_fields[0])
                    .or_else(|| item.get(identity_fields[1])),
                2048,
            )?;
            if !identities.insert(identity.clone()) {
                return Err(());
            }
            indexes.resources.push(NamedDocumentIndex {
                kind,
                identity,
                document: item.clone(),
            });
        }
    }
    let mut prompt_names = BTreeSet::new();
    for prompt in object_array(document, "/prompts")? {
        let identity = bounded_identity(prompt.get("name"), 128)?;
        if !prompt_names.insert(identity.clone()) {
            return Err(());
        }
        indexes.prompts.push(NamedDocumentIndex {
            kind: "prompt",
            identity,
            document: prompt.clone(),
        });
    }
    Ok(indexes)
}

fn revision_indexes(document: &Value) -> Result<RevisionIndexes, ()> {
    let mut indexes = RevisionIndexes::default();
    let mut remotes = BTreeSet::new();
    let mut aliases = BTreeSet::new();
    let mut action_ids = BTreeSet::new();
    for tool in object_array(document, "/bindings/tools")? {
        let import = tool.get("import").and_then(Value::as_object).ok_or(())?;
        let remote_name = bounded_identity(import.get("remote"), 128)?;
        let alias = bounded_identity(import.get("as"), 128)?;
        let action_id = bounded_identity(tool.get("action_id"), 128)?;
        if !remotes.insert(remote_name.clone())
            || !aliases.insert(alias.clone())
            || !action_ids.insert(action_id.clone())
        {
            return Err(());
        }
        indexes.tools.push(RevisionToolIndex {
            remote_name,
            alias,
            action_id,
            binding_hash: canonical_sha256(tool.get("tool_binding_hash"))?,
            document: tool.clone(),
        });
    }
    for (pointer, kind, identity_fields) in [
        (
            "/bindings/resources/policies",
            "policy",
            ["uri_pattern", "uri_pattern"],
        ),
        ("/bindings/resources/items", "resource", ["uri", "uri"]),
        (
            "/bindings/resources/templates",
            "template",
            ["uriTemplate", "uri_template"],
        ),
    ] {
        let mut identities = BTreeSet::new();
        for item in object_array(document, pointer)? {
            let identity = bounded_identity(
                item.get(identity_fields[0])
                    .or_else(|| item.get(identity_fields[1])),
                2048,
            )?;
            if !identities.insert(identity.clone()) {
                return Err(());
            }
            indexes.resources.push(NamedDocumentIndex {
                kind,
                identity,
                document: item.clone(),
            });
        }
    }
    let mut prompt_names = BTreeSet::new();
    for prompt in object_array(document, "/bindings/prompts/policies")? {
        let identity = bounded_identity(prompt.get("remote"), 128)?;
        if !prompt_names.insert(identity.clone()) {
            return Err(());
        }
        indexes.prompts.push(NamedDocumentIndex {
            kind: "prompt",
            identity,
            document: prompt.clone(),
        });
    }
    Ok(indexes)
}

fn sqlite_server(row: &sqlx::sqlite::SqliteRow) -> Result<McpManagedServer, RepositoryError> {
    Ok(McpManagedServer {
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        display_name: row
            .try_get("display_name")
            .map_err(RepositoryError::storage)?,
        state: state(
            row.try_get::<String, _>("server_state")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        server_version: i64_to_u64(
            row.try_get("server_version")
                .map_err(RepositoryError::storage)?,
        )?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        active_revision_id: row
            .try_get("active_revision_id")
            .map_err(RepositoryError::storage)?,
        disable_fence: i64_to_u64(
            row.try_get("disable_fence")
                .map_err(RepositoryError::storage)?,
        )?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        updated_at: row
            .try_get("updated_at")
            .map_err(RepositoryError::storage)?,
    })
}

fn sqlite_draft(row: &sqlx::sqlite::SqliteRow) -> Result<McpStoredDraft, RepositoryError> {
    let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
    Ok(McpStoredDraft {
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        discovery_input_hash: row
            .try_get("discovery_input_hash")
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

fn sqlite_manifest(row: &sqlx::sqlite::SqliteRow) -> Result<McpSignedManifest, RepositoryError> {
    Ok(McpSignedManifest {
        manifest_id: row
            .try_get("manifest_id")
            .map_err(RepositoryError::storage)?,
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        format: row
            .try_get("manifest_format")
            .map_err(RepositoryError::storage)?,
        key_id: row.try_get("key_id").map_err(RepositoryError::storage)?,
        payload: row.try_get("payload").map_err(RepositoryError::storage)?,
        signature: row.try_get("signature").map_err(RepositoryError::storage)?,
        content_hash: row
            .try_get("content_hash")
            .map_err(RepositoryError::storage)?,
        issued_at: row.try_get("issued_at").map_err(RepositoryError::storage)?,
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

fn sqlite_discovery(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<McpDiscoveryOperation, RepositoryError> {
    let failure_code: Option<String> = row
        .try_get("failure_code")
        .map_err(RepositoryError::storage)?;
    let failure = match failure_code {
        Some(code) => Some(McpDiscoveryFailure {
            code,
            stage: row
                .try_get::<Option<String>, _>("failure_stage")
                .map_err(RepositoryError::storage)?
                .ok_or_else(invalid_data)?,
            retryable: row
                .try_get::<Option<i64>, _>("failure_retryable")
                .map_err(RepositoryError::storage)?
                .ok_or_else(invalid_data)?
                != 0,
            correlation_id: row
                .try_get("failure_correlation_id")
                .map_err(RepositoryError::storage)?,
        }),
        None => None,
    };
    Ok(McpDiscoveryOperation {
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        discovery_input_hash: row
            .try_get("discovery_input_hash")
            .map_err(RepositoryError::storage)?,
        status: discovery_status(
            row.try_get::<String, _>("discovery_status")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        cancel_requested: row
            .try_get::<i64, _>("cancel_requested")
            .map_err(RepositoryError::storage)?
            != 0,
        attempts: i64_to_u32(row.try_get("attempts").map_err(RepositoryError::storage)?)?,
        claimed_by: row
            .try_get("claimed_by")
            .map_err(RepositoryError::storage)?,
        claim_token: row
            .try_get("claim_token")
            .map_err(RepositoryError::storage)?,
        claim_expires_at: row
            .try_get("claim_expires_at")
            .map_err(RepositoryError::storage)?,
        failure,
        stale: row
            .try_get::<i64, _>("stale")
            .map_err(RepositoryError::storage)?
            != 0,
        stale_reason: row
            .try_get("stale_reason")
            .map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        started_at: row
            .try_get("started_at")
            .map_err(RepositoryError::storage)?,
        finished_at: row
            .try_get("finished_at")
            .map_err(RepositoryError::storage)?,
    })
}

fn sqlite_snapshot(row: &sqlx::sqlite::SqliteRow) -> Result<McpDiscoverySnapshot, RepositoryError> {
    let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
    Ok(McpDiscoverySnapshot {
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        discovery_input_hash: row
            .try_get("discovery_input_hash")
            .map_err(RepositoryError::storage)?,
        catalog_fingerprint: row
            .try_get("catalog_fingerprint")
            .map_err(RepositoryError::storage)?,
        document: decode_json(&document)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
    })
}

fn sqlite_validation(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<McpValidationReport, RepositoryError> {
    let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
    Ok(McpValidationReport {
        validation_id: row
            .try_get("validation_id")
            .map_err(RepositoryError::storage)?,
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        report_hash: row
            .try_get("report_hash")
            .map_err(RepositoryError::storage)?,
        valid: row
            .try_get::<i64, _>("valid")
            .map_err(RepositoryError::storage)?
            != 0,
        document: decode_json(&document)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

fn sqlite_revision(row: &sqlx::sqlite::SqliteRow) -> Result<McpServerRevision, RepositoryError> {
    let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
    Ok(McpServerRevision {
        revision_id: row
            .try_get("revision_id")
            .map_err(RepositoryError::storage)?,
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        revision_number: i64_to_u64(
            row.try_get("revision_number")
                .map_err(RepositoryError::storage)?,
        )?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        validation_id: row
            .try_get("validation_id")
            .map_err(RepositoryError::storage)?,
        catalog_fingerprint: row
            .try_get("catalog_fingerprint")
            .map_err(RepositoryError::storage)?,
        revision_hash: row
            .try_get("revision_hash")
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

async fn sqlite_replay(
    transaction: &mut Transaction<'_, Sqlite>,
    metadata: &McpMutationMetadata,
) -> Result<Option<McpMutationReceipt>, McpManagementWriteError> {
    let row = sqlx::query(
        "SELECT request_hash,response_status,response_json,response_etag
         FROM mcp_management_requests
         WHERE operator_id=? AND method=? AND canonical_path=? AND request_id=?",
    )
    .bind(&metadata.operator_id)
    .bind(&metadata.method)
    .bind(&metadata.canonical_path)
    .bind(&metadata.request_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let existing_hash: String = row.try_get("request_hash").map_err(storage)?;
    if existing_hash != metadata.request_hash {
        return Err(McpManagementWriteError::Conflict(
            McpManagementConflict::IdempotencyKeyReused,
        ));
    }
    let response: String = row.try_get("response_json").map_err(storage)?;
    Ok(Some(McpMutationReceipt {
        replayed: true,
        status: u16::try_from(row.try_get::<i64, _>("response_status").map_err(storage)?)
            .map_err(|_| McpManagementWriteError::Repository(invalid_data()))?,
        response: decode_json(&response)?,
        etag: row.try_get("response_etag").map_err(storage)?,
    }))
}

struct SqliteFinalize<'a> {
    event_kind: &'a str,
    server_id: Option<&'a str>,
    subject_id: Option<&'a str>,
    before_hash: Option<&'a str>,
    after_hash: Option<&'a str>,
    result_code: &'a str,
    status: u16,
    response: Value,
    etag: Option<String>,
}

async fn sqlite_finalize(
    transaction: &mut Transaction<'_, Sqlite>,
    metadata: &McpMutationMetadata,
    finalization: SqliteFinalize<'_>,
) -> Result<McpMutationReceipt, McpManagementWriteError> {
    let response_json = encode_json(&finalization.response)?;
    sqlx::query(
        "INSERT INTO mcp_management_requests(
           operator_id,method,canonical_path,request_id,request_hash,response_status,
           response_json,response_etag,created_at
         ) VALUES(?,?,?,?,?,?,?,?,?)",
    )
    .bind(&metadata.operator_id)
    .bind(&metadata.method)
    .bind(&metadata.canonical_path)
    .bind(&metadata.request_id)
    .bind(&metadata.request_hash)
    .bind(i64::from(finalization.status))
    .bind(response_json)
    .bind(&finalization.etag)
    .bind(database_time(metadata.now))
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    sqlx::query(
        "INSERT INTO mcp_management_audit_events(
           event_kind,server_id,subject_id,actor_id,request_id_hash,before_hash,
           after_hash,result_code,created_at
         ) VALUES(?,?,?,?,?,?,?,?,?)",
    )
    .bind(finalization.event_kind)
    .bind(finalization.server_id)
    .bind(finalization.subject_id)
    .bind(&metadata.operator_id)
    .bind(prefixed_sha256(metadata.request_id.as_bytes()))
    .bind(finalization.before_hash)
    .bind(finalization.after_hash)
    .bind(finalization.result_code)
    .bind(database_time(metadata.now))
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    if let (Some(server_id), Some(subject_id)) = (finalization.server_id, finalization.subject_id) {
        sqlx::query(
            "INSERT INTO mcp_management_outbox(
               event_id,event_kind,server_id,subject_id,safe_payload,created_at,delivered_at
             ) VALUES(?,?,?,?,?,?,NULL)",
        )
        .bind(format!("mout_{}", Uuid::new_v4().simple()))
        .bind(finalization.event_kind)
        .bind(server_id)
        .bind(subject_id)
        .bind(encode_json(&json!({
            "server_id": server_id,
            "subject_id": subject_id,
            "result_code": finalization.result_code,
        }))?)
        .bind(database_time(metadata.now))
        .execute(&mut **transaction)
        .await
        .map_err(storage)?;
    }
    Ok(McpMutationReceipt {
        replayed: false,
        status: finalization.status,
        response: finalization.response,
        etag: finalization.etag,
    })
}

async fn sqlite_begin(
    repository: &SqliteDurableRepository,
) -> Result<Transaction<'_, Sqlite>, McpManagementWriteError> {
    repository
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(storage)
}

#[async_trait]
impl McpManagementDurableRepository for SqliteDurableRepository {
    async fn record_mcp_management_rejection(
        &self,
        command: RecordMcpManagementRejectionCommand,
    ) -> Result<(), RepositoryError> {
        let _writer = self.writer.lock().await;
        sqlx::query(
            "INSERT INTO mcp_management_audit_events(
               event_kind,server_id,subject_id,actor_id,request_id_hash,
               result_code,created_at
             ) VALUES('mcp.management.rejected',?,?,?,?,?,?)",
        )
        .bind(command.server_id)
        .bind(command.subject_id)
        .bind(command.actor_id)
        .bind(prefixed_sha256(command.request_id.as_bytes()))
        .bind(command.result_code)
        .bind(database_time(command.now))
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(())
    }

    async fn load_mcp_management_runtime_stats(
        &self,
    ) -> Result<McpManagementRuntimeStats, RepositoryError> {
        let row = sqlx::query(
            "SELECT
               (SELECT COUNT(*) FROM mcp_discovery_operations WHERE discovery_status='pending') AS pending_discoveries,
               (SELECT COUNT(*) FROM mcp_discovery_operations WHERE discovery_status='running') AS running_discoveries,
               (SELECT MIN(created_at) FROM mcp_discovery_operations WHERE discovery_status IN('pending','running')) AS oldest_open_discovery_at,
               (SELECT COUNT(*) FROM mcp_managed_servers WHERE server_state='active') AS active_servers,
               (SELECT COUNT(*) FROM mcp_managed_servers WHERE server_state='disabled') AS disabled_servers,
               (SELECT COUNT(DISTINCT s.server_id)
                  FROM mcp_managed_servers s
                  JOIN mcp_server_revisions r ON r.revision_id=s.active_revision_id
                  JOIN mcp_discovery_operations d ON d.discovery_id=r.discovery_id
                 WHERE d.stale=1) AS stale_servers",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(McpManagementRuntimeStats {
            pending_discoveries: i64_to_u64(
                row.try_get("pending_discoveries")
                    .map_err(RepositoryError::storage)?,
            )?,
            running_discoveries: i64_to_u64(
                row.try_get("running_discoveries")
                    .map_err(RepositoryError::storage)?,
            )?,
            oldest_open_discovery_at: row
                .try_get("oldest_open_discovery_at")
                .map_err(RepositoryError::storage)?,
            active_servers: i64_to_u64(
                row.try_get("active_servers")
                    .map_err(RepositoryError::storage)?,
            )?,
            disabled_servers: i64_to_u64(
                row.try_get("disabled_servers")
                    .map_err(RepositoryError::storage)?,
            )?,
            stale_servers: i64_to_u64(
                row.try_get("stale_servers")
                    .map_err(RepositoryError::storage)?,
            )?,
        })
    }

    async fn replay_mcp_mutation(
        &self,
        metadata: &McpMutationMetadata,
    ) -> Result<Option<McpMutationReceipt>, McpManagementWriteError> {
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        let receipt = sqlite_replay(&mut tx, metadata).await?;
        tx.rollback().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn create_mcp_server(
        &self,
        command: CreateMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        if sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_managed_servers WHERE server_id=?",
        )
        .bind(&command.server_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?
            != 0
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO mcp_managed_servers(
               server_id,display_name,server_state,server_version,draft_version,
               active_revision_id,disable_fence,created_at,updated_at
             ) VALUES(?,?,'draft',1,1,NULL,0,?,?)",
        )
        .bind(&command.server_id)
        .bind(&command.display_name)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO mcp_server_drafts(
               server_id,draft_version,discovery_input_hash,document,created_at,updated_at
             ) VALUES(?,1,?,?,?,?)",
        )
        .bind(&command.server_id)
        .bind(&command.discovery_input_hash)
        .bind(encode_json(&command.draft_document)?)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let server = McpManagedServer {
            server_id: command.server_id.clone(),
            display_name: command.display_name,
            state: McpManagedServerState::Draft,
            server_version: 1,
            draft_version: 1,
            active_revision_id: None,
            disable_fence: 0,
            created_at: now,
            updated_at: now,
        };
        let draft = McpStoredDraft {
            server_id: command.server_id.clone(),
            draft_version: 1,
            discovery_input_hash: command.discovery_input_hash.clone(),
            document: command.draft_document,
            created_at: now,
            updated_at: now,
        };
        let response = serde_json::to_value(json!({"server":server,"draft":draft}))
            .map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut tx,
            &command.metadata,
            SqliteFinalize {
                event_kind: "mcp.server.created",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.server_id),
                before_hash: None,
                after_hash: Some(&command.discovery_input_hash),
                result_code: "created",
                status: 201,
                response,
                etag: Some(server_etag(1)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn replace_mcp_draft(
        &self,
        command: ReplaceMcpDraftCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT s.server_state,s.draft_version,d.discovery_input_hash,d.created_at
             FROM mcp_managed_servers s JOIN mcp_server_drafts d USING(server_id)
             WHERE s.server_id=?",
        )
        .bind(&command.server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        if row.try_get::<String, _>("server_state").map_err(storage)? == "retired" {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        let current = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        if current != command.expected_draft_version {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        let previous_hash: String = row.try_get("discovery_input_hash").map_err(storage)?;
        let created_at: DateTime<Utc> = row.try_get("created_at").map_err(storage)?;
        let next = current
            .checked_add(1)
            .ok_or_else(|| McpManagementWriteError::Repository(invalid_data()))?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "UPDATE mcp_server_drafts SET
               draft_version=?,discovery_input_hash=?,document=?,updated_at=?
             WHERE server_id=? AND draft_version=?",
        )
        .bind(u64_to_i64(next)?)
        .bind(&command.discovery_input_hash)
        .bind(encode_json(&command.draft_document)?)
        .bind(now)
        .bind(&command.server_id)
        .bind(u64_to_i64(current)?)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        sqlx::query(
            "UPDATE mcp_managed_servers SET draft_version=?,updated_at=? WHERE server_id=?",
        )
        .bind(u64_to_i64(next)?)
        .bind(now)
        .bind(&command.server_id)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        if previous_hash != command.discovery_input_hash {
            sqlx::query(
                "UPDATE mcp_discovery_operations
                 SET stale=1,stale_reason='draft_discovery_input_changed'
                 WHERE server_id=? AND discovery_status='succeeded' AND stale=0",
            )
            .bind(&command.server_id)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        let draft = McpStoredDraft {
            server_id: command.server_id.clone(),
            draft_version: next,
            discovery_input_hash: command.discovery_input_hash.clone(),
            document: command.draft_document,
            created_at,
            updated_at: now,
        };
        let response = serde_json::to_value(&draft).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut tx,
            &command.metadata,
            SqliteFinalize {
                event_kind: "mcp.draft.replaced",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.server_id),
                before_hash: Some(&previous_hash),
                after_hash: Some(&command.discovery_input_hash),
                result_code: "updated",
                status: 200,
                response,
                etag: Some(draft_etag(next)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn delete_mcp_server(
        &self,
        command: DeleteMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT server_state,server_version FROM mcp_managed_servers WHERE server_id=?",
        )
        .bind(&command.server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        let version = i64_to_u64(row.try_get("server_version").map_err(storage)?)?;
        if version != command.expected_server_version {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        if row.try_get::<String, _>("server_state").map_err(storage)? != "draft"
            || sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM mcp_server_revisions WHERE server_id=?",
            )
            .bind(&command.server_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?
                != 0
            || sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM mcp_discovery_operations
                 WHERE server_id=? AND discovery_status IN('pending','running')",
            )
            .bind(&command.server_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?
                != 0
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::Referenced,
            ));
        }
        sqlx::query("DELETE FROM mcp_managed_servers WHERE server_id=?")
            .bind(&command.server_id)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        let response = json!({"server_id":command.server_id,"deleted":true});
        let receipt = sqlite_finalize(
            &mut tx,
            &command.metadata,
            SqliteFinalize {
                event_kind: "mcp.server.deleted",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.server_id),
                before_hash: None,
                after_hash: None,
                result_code: "deleted",
                status: 200,
                response,
                etag: None,
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_mcp_server(
        &self,
        server_id: &str,
    ) -> Result<Option<McpManagedServer>, RepositoryError> {
        sqlx::query(
            "SELECT server_id,display_name,server_state,server_version,draft_version,
                    active_revision_id,disable_fence,created_at,updated_at
             FROM mcp_managed_servers WHERE server_id=?",
        )
        .bind(server_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_server)
        .transpose()
    }

    async fn get_mcp_draft(
        &self,
        server_id: &str,
    ) -> Result<Option<McpStoredDraft>, RepositoryError> {
        sqlx::query(
            "SELECT server_id,draft_version,discovery_input_hash,document,created_at,updated_at
             FROM mcp_server_drafts WHERE server_id=?",
        )
        .bind(server_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_draft)
        .transpose()
    }

    async fn list_mcp_servers(
        &self,
        requested_state: Option<McpManagedServerState>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpManagedServer>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT server_id,display_name,server_state,server_version,draft_version,
                    active_revision_id,disable_fence,created_at,updated_at
             FROM mcp_managed_servers
             WHERE (? IS NULL OR server_state=?) AND (? IS NULL OR server_id>?)
             ORDER BY server_id LIMIT ?",
        )
        .bind(requested_state.map(McpManagedServerState::as_str))
        .bind(requested_state.map(McpManagedServerState::as_str))
        .bind(cursor)
        .bind(cursor)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(sqlite_server)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor =
            (items.len() > limit as usize).then(|| items.remove(limit as usize).server_id);
        Ok(McpManagementPage { items, next_cursor })
    }

    async fn create_mcp_manifest(
        &self,
        command: CreateMcpManifestCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let server_state = sqlx::query_scalar::<_, String>(
            "SELECT server_state FROM mcp_managed_servers WHERE server_id=?",
        )
        .bind(&command.manifest.server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        if server_state == "retired" {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        let manifest = &command.manifest;
        sqlx::query(
            "INSERT INTO mcp_signed_manifests(
               manifest_id,server_id,manifest_format,key_id,payload,signature,content_hash,
               issued_at,expires_at,created_at,created_by
             ) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&manifest.manifest_id)
        .bind(&manifest.server_id)
        .bind(&manifest.format)
        .bind(&manifest.key_id)
        .bind(&manifest.payload)
        .bind(&manifest.signature)
        .bind(&manifest.content_hash)
        .bind(database_time(manifest.issued_at))
        .bind(database_time(manifest.expires_at))
        .bind(database_time(manifest.created_at))
        .bind(&manifest.created_by)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(manifest).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut tx,
            &command.metadata,
            SqliteFinalize {
                event_kind: "mcp.manifest.created",
                server_id: Some(&manifest.server_id),
                subject_id: Some(&manifest.manifest_id),
                before_hash: None,
                after_hash: Some(&manifest.content_hash),
                result_code: "created",
                status: 201,
                response,
                etag: Some(format!("\"manifest-{}\"", manifest.content_hash)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_mcp_manifest(
        &self,
        server_id: &str,
        manifest_id: &str,
    ) -> Result<Option<McpSignedManifest>, RepositoryError> {
        sqlx::query(
            "SELECT manifest_id,server_id,manifest_format,key_id,payload,signature,content_hash,
                    issued_at,expires_at,created_at,created_by
             FROM mcp_signed_manifests WHERE server_id=? AND manifest_id=?",
        )
        .bind(server_id)
        .bind(manifest_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_manifest)
        .transpose()
    }

    async fn list_mcp_manifests(
        &self,
        server_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpSignedManifest>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT manifest_id,server_id,manifest_format,key_id,payload,signature,content_hash,
                    issued_at,expires_at,created_at,created_by
             FROM mcp_signed_manifests
             WHERE server_id=? AND (? IS NULL OR manifest_id>?)
             ORDER BY manifest_id LIMIT ?",
        )
        .bind(server_id)
        .bind(cursor)
        .bind(cursor)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(sqlite_manifest)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor =
            (items.len() > limit as usize).then(|| items.remove(limit as usize).manifest_id);
        Ok(McpManagementPage { items, next_cursor })
    }

    async fn create_mcp_discovery(
        &self,
        command: CreateMcpDiscoveryCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT s.server_state,d.draft_version,d.discovery_input_hash,d.document
             FROM mcp_managed_servers s JOIN mcp_server_drafts d USING(server_id)
             WHERE s.server_id=?",
        )
        .bind(&command.server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        if row.try_get::<String, _>("server_state").map_err(storage)? == "retired" {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        if i64_to_u64(row.try_get("draft_version").map_err(storage)?)?
            != command.expected_draft_version
            || row
                .try_get::<String, _>("discovery_input_hash")
                .map_err(storage)?
                != command.discovery_input_hash
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        let active = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_discovery_operations
             WHERE discovery_status IN('pending','running')",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;
        if active >= i64::from(command.max_pending_discoveries) {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::CapacityExceeded,
            ));
        }
        let draft_document: String = row.try_get("document").map_err(storage)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO mcp_discovery_operations(
               discovery_id,server_id,source_draft_version,discovery_input_hash,draft_document,
               discovery_status,cancel_requested,attempts,stale,created_at
             ) VALUES(?,?,?,?,?,'pending',0,0,0,?)",
        )
        .bind(&command.discovery_id)
        .bind(&command.server_id)
        .bind(u64_to_i64(command.expected_draft_version)?)
        .bind(&command.discovery_input_hash)
        .bind(draft_document)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let operation = McpDiscoveryOperation {
            discovery_id: command.discovery_id.clone(),
            server_id: command.server_id.clone(),
            source_draft_version: command.expected_draft_version,
            discovery_input_hash: command.discovery_input_hash.clone(),
            status: McpDiscoveryStatus::Pending,
            cancel_requested: false,
            attempts: 0,
            claimed_by: None,
            claim_token: None,
            claim_expires_at: None,
            failure: None,
            stale: false,
            stale_reason: None,
            created_at: now,
            started_at: None,
            finished_at: None,
        };
        let response = serde_json::to_value(&operation).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut tx,
            &command.metadata,
            SqliteFinalize {
                event_kind: "mcp.discovery.requested",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.discovery_id),
                before_hash: None,
                after_hash: Some(&command.discovery_input_hash),
                result_code: "pending",
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn cancel_mcp_discovery(
        &self,
        command: CancelMcpDiscoveryCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT * FROM mcp_discovery_operations WHERE server_id=? AND discovery_id=?",
        )
        .bind(&command.server_id)
        .bind(&command.discovery_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        let current = discovery_status(
            row.try_get::<String, _>("discovery_status")
                .map_err(storage)?
                .as_str(),
        )?;
        let now = database_time(command.metadata.now);
        if current == McpDiscoveryStatus::Pending {
            sqlx::query(
                "UPDATE mcp_discovery_operations SET
                   discovery_status='cancelled',cancel_requested=1,finished_at=?
                 WHERE discovery_id=? AND discovery_status='pending'",
            )
            .bind(now)
            .bind(&command.discovery_id)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        } else if current == McpDiscoveryStatus::Running {
            sqlx::query(
                "UPDATE mcp_discovery_operations SET cancel_requested=1 WHERE discovery_id=?",
            )
            .bind(&command.discovery_id)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        let updated = sqlx::query("SELECT * FROM mcp_discovery_operations WHERE discovery_id=?")
            .bind(&command.discovery_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
        let operation = sqlite_discovery(&updated)?;
        let response = serde_json::to_value(&operation).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut tx,
            &command.metadata,
            SqliteFinalize {
                event_kind: "mcp.discovery.cancel_requested",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.discovery_id),
                before_hash: None,
                after_hash: None,
                result_code: operation.status.as_str(),
                status: 200,
                response,
                etag: None,
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_mcp_discovery(
        &self,
        server_id: &str,
        discovery_id: &str,
    ) -> Result<Option<McpDiscoveryOperation>, RepositoryError> {
        sqlx::query("SELECT * FROM mcp_discovery_operations WHERE server_id=? AND discovery_id=?")
            .bind(server_id)
            .bind(discovery_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(RepositoryError::storage)?
            .as_ref()
            .map(sqlite_discovery)
            .transpose()
    }

    async fn get_mcp_discovery_snapshot(
        &self,
        server_id: &str,
        discovery_id: &str,
    ) -> Result<Option<McpDiscoverySnapshot>, RepositoryError> {
        sqlx::query(
            "SELECT discovery_id,server_id,source_draft_version,discovery_input_hash,
                    catalog_fingerprint,document,created_at
             FROM mcp_discovery_snapshots WHERE server_id=? AND discovery_id=?",
        )
        .bind(server_id)
        .bind(discovery_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_snapshot)
        .transpose()
    }

    async fn list_mcp_discoveries(
        &self,
        server_id: &str,
        requested_status: Option<McpDiscoveryStatus>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpDiscoveryOperation>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT * FROM mcp_discovery_operations
             WHERE server_id=? AND (? IS NULL OR discovery_status=?)
               AND (? IS NULL OR discovery_id>?)
             ORDER BY discovery_id LIMIT ?",
        )
        .bind(server_id)
        .bind(requested_status.map(McpDiscoveryStatus::as_str))
        .bind(requested_status.map(McpDiscoveryStatus::as_str))
        .bind(cursor)
        .bind(cursor)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(sqlite_discovery)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor =
            (items.len() > limit as usize).then(|| items.remove(limit as usize).discovery_id);
        Ok(McpManagementPage { items, next_cursor })
    }

    async fn claim_mcp_discoveries(
        &self,
        command: ClaimMcpDiscoveriesCommand,
    ) -> Result<Vec<McpDiscoveryClaim>, RepositoryError> {
        let _writer = self.writer.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "SELECT discovery_id FROM mcp_discovery_operations
             WHERE cancel_requested=0 AND (
               discovery_status='pending'
               OR (discovery_status='running' AND claim_expires_at<=?)
             )
             ORDER BY created_at,discovery_id LIMIT ?",
        )
        .bind(database_time(command.now))
        .bind(i64::from(command.limit))
        .fetch_all(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let discovery_id: String = row
                .try_get("discovery_id")
                .map_err(RepositoryError::storage)?;
            let claim_token = format!("mclaim_{}", Uuid::new_v4().simple());
            let updated = sqlx::query(
                "UPDATE mcp_discovery_operations SET
                   discovery_status='running',claimed_by=?,claim_token=?,claim_expires_at=?,
                   attempts=attempts+1,started_at=COALESCE(started_at,?)
                 WHERE discovery_id=? AND cancel_requested=0 AND (
                   discovery_status='pending'
                   OR (discovery_status='running' AND claim_expires_at<=?)
                 )",
            )
            .bind(&command.worker_id)
            .bind(&claim_token)
            .bind(database_time(command.lease_expires_at))
            .bind(database_time(command.now))
            .bind(&discovery_id)
            .bind(database_time(command.now))
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
            if updated.rows_affected() == 0 {
                continue;
            }
            let claimed = sqlx::query(
                "SELECT * FROM mcp_discovery_operations WHERE discovery_id=? AND claim_token=?",
            )
            .bind(&discovery_id)
            .bind(&claim_token)
            .fetch_one(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
            let draft_document: String = claimed
                .try_get("draft_document")
                .map_err(RepositoryError::storage)?;
            claims.push(McpDiscoveryClaim {
                operation: sqlite_discovery(&claimed)?,
                claim_token,
                draft_document: decode_json(&draft_document)?,
            });
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn complete_mcp_discovery(
        &self,
        command: CompleteMcpDiscoveryCommand,
    ) -> Result<(), McpManagementWriteError> {
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        let row = sqlx::query(
            "SELECT * FROM mcp_discovery_operations
             WHERE discovery_id=? AND discovery_status='running' AND claim_token=?",
        )
        .bind(&command.discovery_id)
        .bind(&command.claim_token)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::FenceLost,
        ))?;
        let server_id: String = row.try_get("server_id").map_err(storage)?;
        let source_draft_version: i64 = row.try_get("source_draft_version").map_err(storage)?;
        let discovery_input_hash: String = row.try_get("discovery_input_hash").map_err(storage)?;
        let cancelled = row.try_get::<i64, _>("cancel_requested").map_err(storage)? != 0;
        let now = database_time(command.now);
        let result = if cancelled {
            CompleteMcpDiscoveryResult::Cancelled
        } else {
            command.result
        };
        let (event_kind, result_code) = match result {
            CompleteMcpDiscoveryResult::Succeeded {
                catalog_fingerprint,
                snapshot_document,
            } => {
                if canonical_hash(&snapshot_document)? != catalog_fingerprint {
                    return Err(McpManagementWriteError::Conflict(
                        McpManagementConflict::ValidationFailed,
                    ));
                }
                let indexes = discovery_indexes(&snapshot_document).map_err(|_| {
                    McpManagementWriteError::Conflict(McpManagementConflict::ValidationFailed)
                })?;
                sqlx::query(
                    "INSERT INTO mcp_discovery_snapshots(
                       discovery_id,server_id,source_draft_version,discovery_input_hash,
                       catalog_fingerprint,document,created_at
                     ) VALUES(?,?,?,?,?,?,?)",
                )
                .bind(&command.discovery_id)
                .bind(&server_id)
                .bind(source_draft_version)
                .bind(&discovery_input_hash)
                .bind(&catalog_fingerprint)
                .bind(encode_json(&snapshot_document)?)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                for (ordinal, tool) in indexes.tools.iter().enumerate() {
                    sqlx::query(
                        "INSERT INTO mcp_discovery_tools(
                           discovery_id,ordinal,remote_name,schema_hash,document
                         ) VALUES(?,?,?,?,?)",
                    )
                    .bind(&command.discovery_id)
                    .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
                    .bind(&tool.remote_name)
                    .bind(&tool.schema_hash)
                    .bind(encode_json(&tool.document)?)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                }
                for (ordinal, resource) in indexes.resources.iter().enumerate() {
                    sqlx::query(
                        "INSERT INTO mcp_discovery_resources(
                           discovery_id,candidate_kind,ordinal,resource_identity,document
                         ) VALUES(?,?,?,?,?)",
                    )
                    .bind(&command.discovery_id)
                    .bind(resource.kind)
                    .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
                    .bind(&resource.identity)
                    .bind(encode_json(&resource.document)?)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                }
                for (ordinal, prompt) in indexes.prompts.iter().enumerate() {
                    sqlx::query(
                        "INSERT INTO mcp_discovery_prompts(
                           discovery_id,ordinal,remote_name,document
                         ) VALUES(?,?,?,?)",
                    )
                    .bind(&command.discovery_id)
                    .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
                    .bind(&prompt.identity)
                    .bind(encode_json(&prompt.document)?)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                }
                sqlx::query(
                    "UPDATE mcp_discovery_operations SET
                       discovery_status='succeeded',claimed_by=NULL,claim_token=NULL,
                       claim_expires_at=NULL,finished_at=? WHERE discovery_id=?",
                )
                .bind(now)
                .bind(&command.discovery_id)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                ("mcp.discovery.succeeded", "succeeded")
            }
            CompleteMcpDiscoveryResult::Failed(failure) => {
                sqlx::query(
                    "UPDATE mcp_discovery_operations SET
                       discovery_status='failed',claimed_by=NULL,claim_token=NULL,
                       claim_expires_at=NULL,failure_code=?,failure_stage=?,failure_retryable=?,
                       failure_correlation_id=?,finished_at=? WHERE discovery_id=?",
                )
                .bind(&failure.code)
                .bind(&failure.stage)
                .bind(if failure.retryable { 1_i64 } else { 0_i64 })
                .bind(&failure.correlation_id)
                .bind(now)
                .bind(&command.discovery_id)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                ("mcp.discovery.failed", "failed")
            }
            CompleteMcpDiscoveryResult::Cancelled => {
                sqlx::query(
                    "UPDATE mcp_discovery_operations SET
                       discovery_status='cancelled',cancel_requested=1,claimed_by=NULL,
                       claim_token=NULL,claim_expires_at=NULL,finished_at=? WHERE discovery_id=?",
                )
                .bind(now)
                .bind(&command.discovery_id)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                ("mcp.discovery.cancelled", "cancelled")
            }
        };
        sqlx::query(
            "INSERT INTO mcp_management_audit_events(
               event_kind,server_id,subject_id,actor_id,request_id_hash,result_code,created_at
             ) VALUES(?,?,?,'discovery-worker',?,?,?)",
        )
        .bind(event_kind)
        .bind(&server_id)
        .bind(&command.discovery_id)
        .bind(prefixed_sha256(command.claim_token.as_bytes()))
        .bind(result_code)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO mcp_management_outbox(
               event_id,event_kind,server_id,subject_id,safe_payload,created_at
             ) VALUES(?,?,?,?,?,?)",
        )
        .bind(format!("mout_{}", Uuid::new_v4().simple()))
        .bind(event_kind)
        .bind(&server_id)
        .bind(&command.discovery_id)
        .bind(encode_json(&json!({
            "server_id":server_id,
            "discovery_id":command.discovery_id,
            "result_code":result_code
        }))?)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(())
    }

    async fn mark_mcp_discovery_stale(
        &self,
        command: MarkMcpDiscoveryStaleCommand,
    ) -> Result<u64, RepositoryError> {
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let result = sqlx::query(
            "UPDATE mcp_discovery_operations SET stale=1,stale_reason=?
             WHERE server_id=? AND discovery_status='succeeded' AND stale=0",
        )
        .bind(&command.reason_code)
        .bind(&command.server_id)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if result.rows_affected() > 0 {
            sqlx::query(
                "INSERT INTO mcp_management_outbox(
                   event_id,event_kind,server_id,subject_id,safe_payload,created_at
                 ) VALUES(?,'mcp.discovery.stale',?,?,?,?)",
            )
            .bind(format!("mout_{}", Uuid::new_v4().simple()))
            .bind(&command.server_id)
            .bind(&command.server_id)
            .bind(encode_json(&json!({
                "server_id":command.server_id,
                "reason_code":command.reason_code
            }))?)
            .bind(database_time(command.now))
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(result.rows_affected())
    }

    async fn cleanup_terminal_mcp_discoveries(
        &self,
        finished_before: DateTime<Utc>,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError> {
        if limit == 0 || limit > 1_000 {
            return Err(invalid_data());
        }
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let result = sqlx::query(
            "DELETE FROM mcp_discovery_operations
             WHERE discovery_id IN (
               SELECT discovery_id FROM mcp_discovery_operations
               WHERE discovery_status IN('failed','cancelled')
                 AND finished_at IS NOT NULL AND finished_at<?
               ORDER BY finished_at,discovery_id LIMIT ?
             )",
        )
        .bind(database_time(finished_before))
        .bind(i64::from(limit))
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let deleted = result.rows_affected();
        if deleted > 0 {
            let request_payload = encode_json(&json!({
                "finished_before":finished_before,
                "limit":limit
            }))?;
            let request_hash = prefixed_sha256(request_payload.as_bytes());
            sqlx::query(
                "INSERT INTO mcp_management_audit_events(
                   event_kind,server_id,subject_id,actor_id,request_id_hash,result_code,created_at
                 ) VALUES('mcp.discovery.retention',NULL,'terminal_discoveries','system',?,'deleted',?)",
            )
            .bind(&request_hash)
            .bind(database_time(now))
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "INSERT INTO mcp_management_outbox(
                   event_id,event_kind,server_id,subject_id,safe_payload,created_at
                 ) VALUES(?,'mcp.discovery.retention','system','terminal_discoveries',?,?)",
            )
            .bind(format!("mout_{}", Uuid::new_v4().simple()))
            .bind(encode_json(
                &json!({"deleted":deleted,"finished_before":finished_before}),
            )?)
            .bind(database_time(now))
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(deleted)
    }

    async fn create_mcp_validation(
        &self,
        command: CreateMcpValidationCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        if canonical_hash(&command.report.document)? != command.report.report_hash
            || command
                .report
                .document
                .get("valid")
                .and_then(Value::as_bool)
                != Some(command.report.valid)
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ValidationFailed,
            ));
        }
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT d.draft_version,d.discovery_input_hash,o.discovery_status,o.stale,
                    o.discovery_input_hash AS snapshot_input
             FROM mcp_server_drafts d
             JOIN mcp_discovery_operations o ON o.server_id=d.server_id
             WHERE d.server_id=? AND o.discovery_id=?",
        )
        .bind(&command.report.server_id)
        .bind(&command.report.discovery_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        if i64_to_u64(row.try_get("draft_version").map_err(storage)?)?
            != command.expected_draft_version
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        let input: String = row.try_get("discovery_input_hash").map_err(storage)?;
        let snapshot_input: String = row.try_get("snapshot_input").map_err(storage)?;
        if input != command.expected_discovery_input_hash
            || snapshot_input != command.expected_discovery_input_hash
            || row
                .try_get::<String, _>("discovery_status")
                .map_err(storage)?
                != "succeeded"
            || row.try_get::<i64, _>("stale").map_err(storage)? != 0
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::DiscoveryStale,
            ));
        }
        let report = &command.report;
        sqlx::query(
            "INSERT INTO mcp_validation_reports(
               validation_id,server_id,draft_version,discovery_id,report_hash,valid,
               document,created_at,created_by
             ) VALUES(?,?,?,?,?,?,?,?,?)",
        )
        .bind(&report.validation_id)
        .bind(&report.server_id)
        .bind(u64_to_i64(report.draft_version)?)
        .bind(&report.discovery_id)
        .bind(&report.report_hash)
        .bind(if report.valid { 1_i64 } else { 0_i64 })
        .bind(encode_json(&report.document)?)
        .bind(database_time(report.created_at))
        .bind(&report.created_by)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(report).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut tx,
            &command.metadata,
            SqliteFinalize {
                event_kind: "mcp.validation.created",
                server_id: Some(&report.server_id),
                subject_id: Some(&report.validation_id),
                before_hash: None,
                after_hash: Some(&report.report_hash),
                result_code: if report.valid { "valid" } else { "invalid" },
                status: 201,
                response,
                etag: Some(format!("\"validation-{}\"", report.report_hash)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_mcp_validation(
        &self,
        server_id: &str,
        validation_id: &str,
    ) -> Result<Option<McpValidationReport>, RepositoryError> {
        sqlx::query(
            "SELECT validation_id,server_id,draft_version,discovery_id,report_hash,valid,
                    document,created_at,created_by
             FROM mcp_validation_reports WHERE server_id=? AND validation_id=?",
        )
        .bind(server_id)
        .bind(validation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_validation)
        .transpose()
    }

    async fn publish_mcp_revision(
        &self,
        command: PublishMcpRevisionCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        if canonical_hash(&command.document)? != command.revision_hash {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ValidationFailed,
            ));
        }
        let indexes = revision_indexes(&command.document).map_err(|_| {
            McpManagementWriteError::Conflict(McpManagementConflict::ValidationFailed)
        })?;
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT s.server_state,d.draft_version,d.discovery_input_hash,
                    o.discovery_status,o.stale,o.discovery_input_hash AS snapshot_input,
                    x.catalog_fingerprint,v.draft_version AS validation_draft,
                    v.discovery_id AS validation_discovery,v.valid
             FROM mcp_managed_servers s
             JOIN mcp_server_drafts d USING(server_id)
             JOIN mcp_discovery_operations o ON o.server_id=s.server_id AND o.discovery_id=?
             JOIN mcp_discovery_snapshots x ON x.discovery_id=o.discovery_id
             JOIN mcp_validation_reports v ON v.server_id=s.server_id AND v.validation_id=?
             WHERE s.server_id=?",
        )
        .bind(&command.discovery_id)
        .bind(&command.validation_id)
        .bind(&command.server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        if row.try_get::<String, _>("server_state").map_err(storage)? == "retired" {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        if i64_to_u64(row.try_get("draft_version").map_err(storage)?)?
            != command.expected_draft_version
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        let draft_input: String = row.try_get("discovery_input_hash").map_err(storage)?;
        let snapshot_input: String = row.try_get("snapshot_input").map_err(storage)?;
        if draft_input != snapshot_input
            || row
                .try_get::<String, _>("discovery_status")
                .map_err(storage)?
                != "succeeded"
            || row.try_get::<i64, _>("stale").map_err(storage)? != 0
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::DiscoveryStale,
            ));
        }
        if i64_to_u64(row.try_get("validation_draft").map_err(storage)?)?
            != command.expected_draft_version
            || row
                .try_get::<String, _>("validation_discovery")
                .map_err(storage)?
                != command.discovery_id
            || row.try_get::<i64, _>("valid").map_err(storage)? == 0
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ValidationFailed,
            ));
        }
        let catalog_fingerprint: String = row.try_get("catalog_fingerprint").map_err(storage)?;
        let revision_number = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(revision_number),0)+1 FROM mcp_server_revisions WHERE server_id=?",
        )
        .bind(&command.server_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO mcp_server_revisions(
               revision_id,server_id,revision_number,source_draft_version,discovery_id,
               validation_id,catalog_fingerprint,revision_hash,document,created_at,created_by
             ) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&command.revision_id)
        .bind(&command.server_id)
        .bind(revision_number)
        .bind(u64_to_i64(command.expected_draft_version)?)
        .bind(&command.discovery_id)
        .bind(&command.validation_id)
        .bind(&catalog_fingerprint)
        .bind(&command.revision_hash)
        .bind(encode_json(&command.document)?)
        .bind(now)
        .bind(&command.metadata.operator_id)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        for (ordinal, tool) in indexes.tools.iter().enumerate() {
            sqlx::query(
                "INSERT INTO mcp_revision_tools(
                   revision_id,ordinal,remote_name,alias,action_id,binding_hash,document
                 ) VALUES(?,?,?,?,?,?,?)",
            )
            .bind(&command.revision_id)
            .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
            .bind(&tool.remote_name)
            .bind(&tool.alias)
            .bind(&tool.action_id)
            .bind(&tool.binding_hash)
            .bind(encode_json(&tool.document)?)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        for (ordinal, resource) in indexes.resources.iter().enumerate() {
            sqlx::query(
                "INSERT INTO mcp_revision_resources(
                   revision_id,binding_kind,ordinal,resource_identity,document
                 ) VALUES(?,?,?,?,?)",
            )
            .bind(&command.revision_id)
            .bind(resource.kind)
            .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
            .bind(&resource.identity)
            .bind(encode_json(&resource.document)?)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        for (ordinal, prompt) in indexes.prompts.iter().enumerate() {
            sqlx::query(
                "INSERT INTO mcp_revision_prompts(
                   revision_id,ordinal,remote_name,document
                 ) VALUES(?,?,?,?)",
            )
            .bind(&command.revision_id)
            .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
            .bind(&prompt.identity)
            .bind(encode_json(&prompt.document)?)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        let revision = McpServerRevision {
            revision_id: command.revision_id.clone(),
            server_id: command.server_id.clone(),
            revision_number: i64_to_u64(revision_number)?,
            source_draft_version: command.expected_draft_version,
            discovery_id: command.discovery_id,
            validation_id: command.validation_id,
            catalog_fingerprint,
            revision_hash: command.revision_hash.clone(),
            document: command.document,
            created_at: now,
            created_by: command.metadata.operator_id.clone(),
        };
        let response = serde_json::to_value(&revision).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut tx,
            &command.metadata,
            SqliteFinalize {
                event_kind: "mcp.revision.published",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.revision_id),
                before_hash: None,
                after_hash: Some(&command.revision_hash),
                result_code: "published",
                status: 201,
                response,
                etag: Some(format!("\"revision-{}\"", command.revision_hash)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn activate_mcp_revision(
        &self,
        command: ActivateMcpRevisionCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        if command.readiness_expires_at < command.metadata.now {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let server_row = sqlx::query("SELECT * FROM mcp_managed_servers WHERE server_id=?")
            .bind(&command.server_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?
            .ok_or(McpManagementWriteError::Conflict(
                McpManagementConflict::NotFound,
            ))?;
        let mut server = sqlite_server(&server_row)?;
        if server.server_version != command.expected_server_version {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        if server.state == McpManagedServerState::Retired {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        if sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_server_revisions WHERE server_id=? AND revision_id=?",
        )
        .bind(&command.server_id)
        .bind(&command.revision_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?
            == 0
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::NotFound,
            ));
        }
        let previous_revision = server.active_revision_id.clone();
        server.server_version = server
            .server_version
            .checked_add(1)
            .ok_or_else(|| McpManagementWriteError::Repository(invalid_data()))?;
        server.state = McpManagedServerState::Active;
        server.active_revision_id = Some(command.revision_id.clone());
        server.updated_at = database_time(command.metadata.now);
        sqlx::query(
            "UPDATE mcp_managed_servers SET
               server_state='active',active_revision_id=?,server_version=?,updated_at=?
             WHERE server_id=? AND server_version=?",
        )
        .bind(&command.revision_id)
        .bind(u64_to_i64(server.server_version)?)
        .bind(server.updated_at)
        .bind(&command.server_id)
        .bind(u64_to_i64(command.expected_server_version)?)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(&server).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut tx,
            &command.metadata,
            SqliteFinalize {
                event_kind: "mcp.revision.activated",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.revision_id),
                before_hash: previous_revision.as_deref(),
                after_hash: Some(&command.revision_id),
                result_code: "active",
                status: 200,
                response,
                etag: Some(server_etag(server.server_version)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn disable_mcp_server(
        &self,
        command: DisableMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        self.sqlite_change_server_state(
            command.metadata,
            &command.server_id,
            command.expected_server_version,
            McpManagedServerState::Disabled,
            None,
        )
        .await
    }

    async fn retire_mcp_server(
        &self,
        command: RetireMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        self.sqlite_change_server_state(
            command.metadata,
            &command.server_id,
            command.expected_server_version,
            McpManagedServerState::Retired,
            Some(&command.reason_code),
        )
        .await
    }

    async fn get_mcp_revision(
        &self,
        server_id: &str,
        revision_id: &str,
    ) -> Result<Option<McpServerRevision>, RepositoryError> {
        sqlx::query(
            "SELECT revision_id,server_id,revision_number,source_draft_version,discovery_id,
                    validation_id,catalog_fingerprint,revision_hash,document,created_at,created_by
             FROM mcp_server_revisions WHERE server_id=? AND revision_id=?",
        )
        .bind(server_id)
        .bind(revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_revision)
        .transpose()
    }

    async fn list_mcp_revisions(
        &self,
        server_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpServerRevision>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT revision_id,server_id,revision_number,source_draft_version,discovery_id,
                    validation_id,catalog_fingerprint,revision_hash,document,created_at,created_by
             FROM mcp_server_revisions
             WHERE server_id=? AND (? IS NULL OR revision_id>?)
             ORDER BY revision_id LIMIT ?",
        )
        .bind(server_id)
        .bind(cursor)
        .bind(cursor)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(sqlite_revision)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor =
            (items.len() > limit as usize).then(|| items.remove(limit as usize).revision_id);
        Ok(McpManagementPage { items, next_cursor })
    }

    async fn load_active_mcp_revisions(
        &self,
    ) -> Result<Vec<(McpManagedServer, McpServerRevision)>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT s.server_id AS s_server_id,s.display_name,s.server_state,s.server_version,
                    s.draft_version,s.active_revision_id,s.disable_fence,s.created_at AS s_created_at,
                    s.updated_at AS s_updated_at,r.*
             FROM mcp_managed_servers s JOIN mcp_server_revisions r
               ON r.server_id=s.server_id AND r.revision_id=s.active_revision_id
             WHERE s.server_state='active' ORDER BY s.server_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.iter()
            .map(|row| {
                let server = McpManagedServer {
                    server_id: row
                        .try_get("s_server_id")
                        .map_err(RepositoryError::storage)?,
                    display_name: row
                        .try_get("display_name")
                        .map_err(RepositoryError::storage)?,
                    state: state(
                        row.try_get::<String, _>("server_state")
                            .map_err(RepositoryError::storage)?
                            .as_str(),
                    )?,
                    server_version: i64_to_u64(
                        row.try_get("server_version")
                            .map_err(RepositoryError::storage)?,
                    )?,
                    draft_version: i64_to_u64(
                        row.try_get("draft_version")
                            .map_err(RepositoryError::storage)?,
                    )?,
                    active_revision_id: row
                        .try_get("active_revision_id")
                        .map_err(RepositoryError::storage)?,
                    disable_fence: i64_to_u64(
                        row.try_get("disable_fence")
                            .map_err(RepositoryError::storage)?,
                    )?,
                    created_at: row
                        .try_get("s_created_at")
                        .map_err(RepositoryError::storage)?,
                    updated_at: row
                        .try_get("s_updated_at")
                        .map_err(RepositoryError::storage)?,
                };
                Ok((server, sqlite_revision(row)?))
            })
            .collect()
    }

    async fn load_mcp_server_fence(
        &self,
        server_id: &str,
    ) -> Result<Option<McpServerFence>, RepositoryError> {
        let server = self.get_mcp_server(server_id).await?;
        Ok(server.map(|server| McpServerFence {
            server_id: server.server_id,
            state: server.state,
            active_revision_id: server.active_revision_id,
            disable_fence: server.disable_fence,
        }))
    }
}

impl SqliteDurableRepository {
    async fn sqlite_change_server_state(
        &self,
        metadata: McpMutationMetadata,
        server_id: &str,
        expected_server_version: u64,
        target: McpManagedServerState,
        reason_code: Option<&str>,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let _writer = self.writer.lock().await;
        let mut tx = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut tx, &metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT * FROM mcp_managed_servers WHERE server_id=?")
            .bind(server_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?
            .ok_or(McpManagementWriteError::Conflict(
                McpManagementConflict::NotFound,
            ))?;
        let mut server = sqlite_server(&row)?;
        if server.server_version != expected_server_version {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        match target {
            McpManagedServerState::Disabled
                if server.active_revision_id.is_none()
                    || server.state == McpManagedServerState::Retired =>
            {
                return Err(McpManagementWriteError::Conflict(
                    McpManagementConflict::ForbiddenState,
                ));
            }
            McpManagedServerState::Retired if server.state == McpManagedServerState::Retired => {
                return Err(McpManagementWriteError::Conflict(
                    McpManagementConflict::ForbiddenState,
                ));
            }
            McpManagedServerState::Disabled | McpManagedServerState::Retired => {}
            McpManagedServerState::Draft | McpManagedServerState::Active => {
                return Err(McpManagementWriteError::Conflict(
                    McpManagementConflict::ForbiddenState,
                ));
            }
        }
        server.server_version = server
            .server_version
            .checked_add(1)
            .ok_or_else(|| McpManagementWriteError::Repository(invalid_data()))?;
        server.disable_fence = server
            .disable_fence
            .checked_add(1)
            .ok_or_else(|| McpManagementWriteError::Repository(invalid_data()))?;
        server.state = target;
        server.updated_at = database_time(metadata.now);
        sqlx::query(
            "UPDATE mcp_managed_servers SET
               server_state=?,server_version=?,disable_fence=?,updated_at=?
             WHERE server_id=? AND server_version=?",
        )
        .bind(target.as_str())
        .bind(u64_to_i64(server.server_version)?)
        .bind(u64_to_i64(server.disable_fence)?)
        .bind(server.updated_at)
        .bind(server_id)
        .bind(u64_to_i64(expected_server_version)?)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(&server).map_err(|_| invalid_data())?;
        let event_kind = if target == McpManagedServerState::Disabled {
            "mcp.server.disabled"
        } else {
            "mcp.server.retired"
        };
        let receipt = sqlite_finalize(
            &mut tx,
            &metadata,
            SqliteFinalize {
                event_kind,
                server_id: Some(server_id),
                subject_id: Some(server_id),
                before_hash: None,
                after_hash: reason_code,
                result_code: target.as_str(),
                status: 200,
                response,
                etag: Some(server_etag(server.server_version)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
}

// PostgreSQL uses the same logical contract. Its implementation follows below
// so both backends are kept in one reviewable state-machine adapter.

fn postgres_server(row: &sqlx::postgres::PgRow) -> Result<McpManagedServer, RepositoryError> {
    Ok(McpManagedServer {
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        display_name: row
            .try_get("display_name")
            .map_err(RepositoryError::storage)?,
        state: state(
            row.try_get::<String, _>("server_state")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        server_version: i64_to_u64(
            row.try_get("server_version")
                .map_err(RepositoryError::storage)?,
        )?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        active_revision_id: row
            .try_get("active_revision_id")
            .map_err(RepositoryError::storage)?,
        disable_fence: i64_to_u64(
            row.try_get("disable_fence")
                .map_err(RepositoryError::storage)?,
        )?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        updated_at: row
            .try_get("updated_at")
            .map_err(RepositoryError::storage)?,
    })
}

fn postgres_draft(row: &sqlx::postgres::PgRow) -> Result<McpStoredDraft, RepositoryError> {
    Ok(McpStoredDraft {
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        discovery_input_hash: row
            .try_get("discovery_input_hash")
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

fn postgres_manifest(row: &sqlx::postgres::PgRow) -> Result<McpSignedManifest, RepositoryError> {
    Ok(McpSignedManifest {
        manifest_id: row
            .try_get("manifest_id")
            .map_err(RepositoryError::storage)?,
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        format: row
            .try_get("manifest_format")
            .map_err(RepositoryError::storage)?,
        key_id: row.try_get("key_id").map_err(RepositoryError::storage)?,
        payload: row.try_get("payload").map_err(RepositoryError::storage)?,
        signature: row.try_get("signature").map_err(RepositoryError::storage)?,
        content_hash: row
            .try_get("content_hash")
            .map_err(RepositoryError::storage)?,
        issued_at: row.try_get("issued_at").map_err(RepositoryError::storage)?,
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

fn postgres_discovery(
    row: &sqlx::postgres::PgRow,
) -> Result<McpDiscoveryOperation, RepositoryError> {
    let failure_code: Option<String> = row
        .try_get("failure_code")
        .map_err(RepositoryError::storage)?;
    let failure = match failure_code {
        Some(code) => Some(McpDiscoveryFailure {
            code,
            stage: row
                .try_get::<Option<String>, _>("failure_stage")
                .map_err(RepositoryError::storage)?
                .ok_or_else(invalid_data)?,
            retryable: row
                .try_get::<Option<bool>, _>("failure_retryable")
                .map_err(RepositoryError::storage)?
                .ok_or_else(invalid_data)?,
            correlation_id: row
                .try_get("failure_correlation_id")
                .map_err(RepositoryError::storage)?,
        }),
        None => None,
    };
    Ok(McpDiscoveryOperation {
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        discovery_input_hash: row
            .try_get("discovery_input_hash")
            .map_err(RepositoryError::storage)?,
        status: discovery_status(
            row.try_get::<String, _>("discovery_status")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        cancel_requested: row
            .try_get("cancel_requested")
            .map_err(RepositoryError::storage)?,
        attempts: i64_to_u32(row.try_get("attempts").map_err(RepositoryError::storage)?)?,
        claimed_by: row
            .try_get("claimed_by")
            .map_err(RepositoryError::storage)?,
        claim_token: row
            .try_get("claim_token")
            .map_err(RepositoryError::storage)?,
        claim_expires_at: row
            .try_get("claim_expires_at")
            .map_err(RepositoryError::storage)?,
        failure,
        stale: row.try_get("stale").map_err(RepositoryError::storage)?,
        stale_reason: row
            .try_get("stale_reason")
            .map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        started_at: row
            .try_get("started_at")
            .map_err(RepositoryError::storage)?,
        finished_at: row
            .try_get("finished_at")
            .map_err(RepositoryError::storage)?,
    })
}

fn postgres_snapshot(row: &sqlx::postgres::PgRow) -> Result<McpDiscoverySnapshot, RepositoryError> {
    Ok(McpDiscoverySnapshot {
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        discovery_input_hash: row
            .try_get("discovery_input_hash")
            .map_err(RepositoryError::storage)?,
        catalog_fingerprint: row
            .try_get("catalog_fingerprint")
            .map_err(RepositoryError::storage)?,
        document: row.try_get("document").map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
    })
}

fn postgres_validation(
    row: &sqlx::postgres::PgRow,
) -> Result<McpValidationReport, RepositoryError> {
    Ok(McpValidationReport {
        validation_id: row
            .try_get("validation_id")
            .map_err(RepositoryError::storage)?,
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        report_hash: row
            .try_get("report_hash")
            .map_err(RepositoryError::storage)?,
        valid: row.try_get("valid").map_err(RepositoryError::storage)?,
        document: row.try_get("document").map_err(RepositoryError::storage)?,
        created_at: row
            .try_get("created_at")
            .map_err(RepositoryError::storage)?,
        created_by: row
            .try_get("created_by")
            .map_err(RepositoryError::storage)?,
    })
}

fn postgres_revision(row: &sqlx::postgres::PgRow) -> Result<McpServerRevision, RepositoryError> {
    Ok(McpServerRevision {
        revision_id: row
            .try_get("revision_id")
            .map_err(RepositoryError::storage)?,
        server_id: row.try_get("server_id").map_err(RepositoryError::storage)?,
        revision_number: i64_to_u64(
            row.try_get("revision_number")
                .map_err(RepositoryError::storage)?,
        )?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        validation_id: row
            .try_get("validation_id")
            .map_err(RepositoryError::storage)?,
        catalog_fingerprint: row
            .try_get("catalog_fingerprint")
            .map_err(RepositoryError::storage)?,
        revision_hash: row
            .try_get("revision_hash")
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

async fn postgres_replay(
    transaction: &mut Transaction<'_, Postgres>,
    metadata: &McpMutationMetadata,
) -> Result<Option<McpMutationReceipt>, McpManagementWriteError> {
    let row = sqlx::query(
        "SELECT request_hash,response_status,response_json,response_etag
         FROM mcp_management_requests
         WHERE operator_id=$1 AND method=$2 AND canonical_path=$3 AND request_id=$4",
    )
    .bind(&metadata.operator_id)
    .bind(&metadata.method)
    .bind(&metadata.canonical_path)
    .bind(&metadata.request_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.try_get::<String, _>("request_hash").map_err(storage)? != metadata.request_hash {
        return Err(McpManagementWriteError::Conflict(
            McpManagementConflict::IdempotencyKeyReused,
        ));
    }
    Ok(Some(McpMutationReceipt {
        replayed: true,
        status: u16::try_from(row.try_get::<i32, _>("response_status").map_err(storage)?)
            .map_err(|_| McpManagementWriteError::Repository(invalid_data()))?,
        response: row.try_get("response_json").map_err(storage)?,
        etag: row.try_get("response_etag").map_err(storage)?,
    }))
}

struct PostgresFinalize<'a> {
    event_kind: &'a str,
    server_id: Option<&'a str>,
    subject_id: Option<&'a str>,
    before_hash: Option<&'a str>,
    after_hash: Option<&'a str>,
    result_code: &'a str,
    status: u16,
    response: Value,
    etag: Option<String>,
}

async fn postgres_finalize(
    transaction: &mut Transaction<'_, Postgres>,
    metadata: &McpMutationMetadata,
    finalization: PostgresFinalize<'_>,
) -> Result<McpMutationReceipt, McpManagementWriteError> {
    sqlx::query(
        "INSERT INTO mcp_management_requests(
           operator_id,method,canonical_path,request_id,request_hash,response_status,
           response_json,response_etag,created_at
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
    )
    .bind(&metadata.operator_id)
    .bind(&metadata.method)
    .bind(&metadata.canonical_path)
    .bind(&metadata.request_id)
    .bind(&metadata.request_hash)
    .bind(i32::from(finalization.status))
    .bind(&finalization.response)
    .bind(&finalization.etag)
    .bind(database_time(metadata.now))
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    sqlx::query(
        "INSERT INTO mcp_management_audit_events(
           event_kind,server_id,subject_id,actor_id,request_id_hash,before_hash,
           after_hash,result_code,created_at
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
    )
    .bind(finalization.event_kind)
    .bind(finalization.server_id)
    .bind(finalization.subject_id)
    .bind(&metadata.operator_id)
    .bind(prefixed_sha256(metadata.request_id.as_bytes()))
    .bind(finalization.before_hash)
    .bind(finalization.after_hash)
    .bind(finalization.result_code)
    .bind(database_time(metadata.now))
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    if let (Some(server_id), Some(subject_id)) = (finalization.server_id, finalization.subject_id) {
        sqlx::query(
            "INSERT INTO mcp_management_outbox(
               event_id,event_kind,server_id,subject_id,safe_payload,created_at,delivered_at
             ) VALUES($1,$2,$3,$4,$5,$6,NULL)",
        )
        .bind(format!("mout_{}", Uuid::new_v4().simple()))
        .bind(finalization.event_kind)
        .bind(server_id)
        .bind(subject_id)
        .bind(json!({
            "server_id":server_id,
            "subject_id":subject_id,
            "result_code":finalization.result_code,
        }))
        .bind(database_time(metadata.now))
        .execute(&mut **transaction)
        .await
        .map_err(storage)?;
    }
    Ok(McpMutationReceipt {
        replayed: false,
        status: finalization.status,
        response: finalization.response,
        etag: finalization.etag,
    })
}

#[async_trait]
impl McpManagementDurableRepository for PostgresDurableRepository {
    async fn record_mcp_management_rejection(
        &self,
        command: RecordMcpManagementRejectionCommand,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            "INSERT INTO mcp_management_audit_events(
               event_kind,server_id,subject_id,actor_id,request_id_hash,
               result_code,created_at
             ) VALUES('mcp.management.rejected',$1,$2,$3,$4,$5,$6)",
        )
        .bind(command.server_id)
        .bind(command.subject_id)
        .bind(command.actor_id)
        .bind(prefixed_sha256(command.request_id.as_bytes()))
        .bind(command.result_code)
        .bind(database_time(command.now))
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(())
    }

    async fn load_mcp_management_runtime_stats(
        &self,
    ) -> Result<McpManagementRuntimeStats, RepositoryError> {
        let row = sqlx::query(
            "SELECT
               (SELECT COUNT(*) FROM mcp_discovery_operations WHERE discovery_status='pending') AS pending_discoveries,
               (SELECT COUNT(*) FROM mcp_discovery_operations WHERE discovery_status='running') AS running_discoveries,
               (SELECT MIN(created_at) FROM mcp_discovery_operations WHERE discovery_status IN('pending','running')) AS oldest_open_discovery_at,
               (SELECT COUNT(*) FROM mcp_managed_servers WHERE server_state='active') AS active_servers,
               (SELECT COUNT(*) FROM mcp_managed_servers WHERE server_state='disabled') AS disabled_servers,
               (SELECT COUNT(DISTINCT s.server_id)
                  FROM mcp_managed_servers s
                  JOIN mcp_server_revisions r ON r.revision_id=s.active_revision_id
                  JOIN mcp_discovery_operations d ON d.discovery_id=r.discovery_id
                 WHERE d.stale=TRUE) AS stale_servers",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(McpManagementRuntimeStats {
            pending_discoveries: i64_to_u64(
                row.try_get("pending_discoveries")
                    .map_err(RepositoryError::storage)?,
            )?,
            running_discoveries: i64_to_u64(
                row.try_get("running_discoveries")
                    .map_err(RepositoryError::storage)?,
            )?,
            oldest_open_discovery_at: row
                .try_get("oldest_open_discovery_at")
                .map_err(RepositoryError::storage)?,
            active_servers: i64_to_u64(
                row.try_get("active_servers")
                    .map_err(RepositoryError::storage)?,
            )?,
            disabled_servers: i64_to_u64(
                row.try_get("disabled_servers")
                    .map_err(RepositoryError::storage)?,
            )?,
            stale_servers: i64_to_u64(
                row.try_get("stale_servers")
                    .map_err(RepositoryError::storage)?,
            )?,
        })
    }

    async fn replay_mcp_mutation(
        &self,
        metadata: &McpMutationMetadata,
    ) -> Result<Option<McpMutationReceipt>, McpManagementWriteError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let receipt = postgres_replay(&mut tx, metadata).await?;
        tx.rollback().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn create_mcp_server(
        &self,
        command: CreateMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        if sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_managed_servers WHERE server_id=$1",
        )
        .bind(&command.server_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?
            != 0
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO mcp_managed_servers(
               server_id,display_name,server_state,server_version,draft_version,
               active_revision_id,disable_fence,created_at,updated_at
             ) VALUES($1,$2,'draft',1,1,NULL,0,$3,$3)",
        )
        .bind(&command.server_id)
        .bind(&command.display_name)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO mcp_server_drafts(
               server_id,draft_version,discovery_input_hash,document,created_at,updated_at
             ) VALUES($1,1,$2,$3,$4,$4)",
        )
        .bind(&command.server_id)
        .bind(&command.discovery_input_hash)
        .bind(&command.draft_document)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let server = McpManagedServer {
            server_id: command.server_id.clone(),
            display_name: command.display_name,
            state: McpManagedServerState::Draft,
            server_version: 1,
            draft_version: 1,
            active_revision_id: None,
            disable_fence: 0,
            created_at: now,
            updated_at: now,
        };
        let draft = McpStoredDraft {
            server_id: command.server_id.clone(),
            draft_version: 1,
            discovery_input_hash: command.discovery_input_hash.clone(),
            document: command.draft_document,
            created_at: now,
            updated_at: now,
        };
        let receipt = postgres_finalize(
            &mut tx,
            &command.metadata,
            PostgresFinalize {
                event_kind: "mcp.server.created",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.server_id),
                before_hash: None,
                after_hash: Some(&command.discovery_input_hash),
                result_code: "created",
                status: 201,
                response: json!({"server":server,"draft":draft}),
                etag: Some(server_etag(1)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn replace_mcp_draft(
        &self,
        command: ReplaceMcpDraftCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT s.server_state,s.draft_version,d.discovery_input_hash,d.created_at
             FROM mcp_managed_servers s JOIN mcp_server_drafts d USING(server_id)
             WHERE s.server_id=$1 FOR UPDATE OF s,d",
        )
        .bind(&command.server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        if row.try_get::<String, _>("server_state").map_err(storage)? == "retired" {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        let current = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        if current != command.expected_draft_version {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        let previous_hash: String = row.try_get("discovery_input_hash").map_err(storage)?;
        let created_at: DateTime<Utc> = row.try_get("created_at").map_err(storage)?;
        let next = current
            .checked_add(1)
            .ok_or_else(|| McpManagementWriteError::Repository(invalid_data()))?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "UPDATE mcp_server_drafts SET
               draft_version=$1,discovery_input_hash=$2,document=$3,updated_at=$4
             WHERE server_id=$5 AND draft_version=$6",
        )
        .bind(u64_to_i64(next)?)
        .bind(&command.discovery_input_hash)
        .bind(&command.draft_document)
        .bind(now)
        .bind(&command.server_id)
        .bind(u64_to_i64(current)?)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        sqlx::query(
            "UPDATE mcp_managed_servers SET draft_version=$1,updated_at=$2 WHERE server_id=$3",
        )
        .bind(u64_to_i64(next)?)
        .bind(now)
        .bind(&command.server_id)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        if previous_hash != command.discovery_input_hash {
            sqlx::query(
                "UPDATE mcp_discovery_operations
                 SET stale=true,stale_reason='draft_discovery_input_changed'
                 WHERE server_id=$1 AND discovery_status='succeeded' AND NOT stale",
            )
            .bind(&command.server_id)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        let draft = McpStoredDraft {
            server_id: command.server_id.clone(),
            draft_version: next,
            discovery_input_hash: command.discovery_input_hash.clone(),
            document: command.draft_document,
            created_at,
            updated_at: now,
        };
        let response = serde_json::to_value(&draft).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut tx,
            &command.metadata,
            PostgresFinalize {
                event_kind: "mcp.draft.replaced",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.server_id),
                before_hash: Some(&previous_hash),
                after_hash: Some(&command.discovery_input_hash),
                result_code: "updated",
                status: 200,
                response,
                etag: Some(draft_etag(next)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn delete_mcp_server(
        &self,
        command: DeleteMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT server_state,server_version FROM mcp_managed_servers
             WHERE server_id=$1 FOR UPDATE",
        )
        .bind(&command.server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        let version = i64_to_u64(row.try_get("server_version").map_err(storage)?)?;
        if version != command.expected_server_version {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        if row.try_get::<String, _>("server_state").map_err(storage)? != "draft"
            || sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM mcp_server_revisions WHERE server_id=$1",
            )
            .bind(&command.server_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?
                != 0
            || sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM mcp_discovery_operations
                 WHERE server_id=$1 AND discovery_status IN('pending','running')",
            )
            .bind(&command.server_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?
                != 0
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::Referenced,
            ));
        }
        sqlx::query("DELETE FROM mcp_managed_servers WHERE server_id=$1")
            .bind(&command.server_id)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        let receipt = postgres_finalize(
            &mut tx,
            &command.metadata,
            PostgresFinalize {
                event_kind: "mcp.server.deleted",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.server_id),
                before_hash: None,
                after_hash: None,
                result_code: "deleted",
                status: 200,
                response: json!({"server_id":command.server_id,"deleted":true}),
                etag: None,
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_mcp_server(
        &self,
        server_id: &str,
    ) -> Result<Option<McpManagedServer>, RepositoryError> {
        sqlx::query(
            "SELECT server_id,display_name,server_state,server_version,draft_version,
                    active_revision_id,disable_fence,created_at,updated_at
             FROM mcp_managed_servers WHERE server_id=$1",
        )
        .bind(server_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_server)
        .transpose()
    }

    async fn get_mcp_draft(
        &self,
        server_id: &str,
    ) -> Result<Option<McpStoredDraft>, RepositoryError> {
        sqlx::query(
            "SELECT server_id,draft_version,discovery_input_hash,document,created_at,updated_at
             FROM mcp_server_drafts WHERE server_id=$1",
        )
        .bind(server_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_draft)
        .transpose()
    }

    async fn list_mcp_servers(
        &self,
        requested_state: Option<McpManagedServerState>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpManagedServer>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT server_id,display_name,server_state,server_version,draft_version,
                    active_revision_id,disable_fence,created_at,updated_at
             FROM mcp_managed_servers
             WHERE ($1::text IS NULL OR server_state=$1)
               AND ($2::text IS NULL OR server_id>$2)
             ORDER BY server_id LIMIT $3",
        )
        .bind(requested_state.map(McpManagedServerState::as_str))
        .bind(cursor)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(postgres_server)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor =
            (items.len() > limit as usize).then(|| items.remove(limit as usize).server_id);
        Ok(McpManagementPage { items, next_cursor })
    }

    async fn create_mcp_manifest(
        &self,
        command: CreateMcpManifestCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let server_state = sqlx::query_scalar::<_, String>(
            "SELECT server_state FROM mcp_managed_servers WHERE server_id=$1 FOR UPDATE",
        )
        .bind(&command.manifest.server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        if server_state == "retired" {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        let manifest = &command.manifest;
        sqlx::query(
            "INSERT INTO mcp_signed_manifests(
               manifest_id,server_id,manifest_format,key_id,payload,signature,content_hash,
               issued_at,expires_at,created_at,created_by
             ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(&manifest.manifest_id)
        .bind(&manifest.server_id)
        .bind(&manifest.format)
        .bind(&manifest.key_id)
        .bind(&manifest.payload)
        .bind(&manifest.signature)
        .bind(&manifest.content_hash)
        .bind(database_time(manifest.issued_at))
        .bind(database_time(manifest.expires_at))
        .bind(database_time(manifest.created_at))
        .bind(&manifest.created_by)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(manifest).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut tx,
            &command.metadata,
            PostgresFinalize {
                event_kind: "mcp.manifest.created",
                server_id: Some(&manifest.server_id),
                subject_id: Some(&manifest.manifest_id),
                before_hash: None,
                after_hash: Some(&manifest.content_hash),
                result_code: "created",
                status: 201,
                response,
                etag: Some(format!("\"manifest-{}\"", manifest.content_hash)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_mcp_manifest(
        &self,
        server_id: &str,
        manifest_id: &str,
    ) -> Result<Option<McpSignedManifest>, RepositoryError> {
        sqlx::query(
            "SELECT manifest_id,server_id,manifest_format,key_id,payload,signature,content_hash,
                    issued_at,expires_at,created_at,created_by
             FROM mcp_signed_manifests WHERE server_id=$1 AND manifest_id=$2",
        )
        .bind(server_id)
        .bind(manifest_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_manifest)
        .transpose()
    }

    async fn list_mcp_manifests(
        &self,
        server_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpSignedManifest>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT manifest_id,server_id,manifest_format,key_id,payload,signature,content_hash,
                    issued_at,expires_at,created_at,created_by
             FROM mcp_signed_manifests
             WHERE server_id=$1 AND ($2::text IS NULL OR manifest_id>$2)
             ORDER BY manifest_id LIMIT $3",
        )
        .bind(server_id)
        .bind(cursor)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(postgres_manifest)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor =
            (items.len() > limit as usize).then(|| items.remove(limit as usize).manifest_id);
        Ok(McpManagementPage { items, next_cursor })
    }
    async fn create_mcp_discovery(
        &self,
        command: CreateMcpDiscoveryCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        // Serialize the installation-wide capacity check without keeping a
        // network transaction open. The advisory key is scoped to this
        // control-plane class and released at commit.
        sqlx::query("SELECT pg_advisory_xact_lock(4932620935061048145::bigint)")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        let row = sqlx::query(
            "SELECT s.server_state,d.draft_version,d.discovery_input_hash,d.document
             FROM mcp_managed_servers s JOIN mcp_server_drafts d USING(server_id)
             WHERE s.server_id=$1 FOR UPDATE OF s,d",
        )
        .bind(&command.server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        if row.try_get::<String, _>("server_state").map_err(storage)? == "retired" {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        if i64_to_u64(row.try_get("draft_version").map_err(storage)?)?
            != command.expected_draft_version
            || row
                .try_get::<String, _>("discovery_input_hash")
                .map_err(storage)?
                != command.discovery_input_hash
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        let active = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_discovery_operations
             WHERE discovery_status IN('pending','running')",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;
        if active >= i64::from(command.max_pending_discoveries) {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::CapacityExceeded,
            ));
        }
        let draft_document: Value = row.try_get("document").map_err(storage)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO mcp_discovery_operations(
               discovery_id,server_id,source_draft_version,discovery_input_hash,draft_document,
               discovery_status,cancel_requested,attempts,stale,created_at
             ) VALUES($1,$2,$3,$4,$5,'pending',false,0,false,$6)",
        )
        .bind(&command.discovery_id)
        .bind(&command.server_id)
        .bind(u64_to_i64(command.expected_draft_version)?)
        .bind(&command.discovery_input_hash)
        .bind(draft_document)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let operation = McpDiscoveryOperation {
            discovery_id: command.discovery_id.clone(),
            server_id: command.server_id.clone(),
            source_draft_version: command.expected_draft_version,
            discovery_input_hash: command.discovery_input_hash.clone(),
            status: McpDiscoveryStatus::Pending,
            cancel_requested: false,
            attempts: 0,
            claimed_by: None,
            claim_token: None,
            claim_expires_at: None,
            failure: None,
            stale: false,
            stale_reason: None,
            created_at: now,
            started_at: None,
            finished_at: None,
        };
        let response = serde_json::to_value(&operation).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut tx,
            &command.metadata,
            PostgresFinalize {
                event_kind: "mcp.discovery.requested",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.discovery_id),
                before_hash: None,
                after_hash: Some(&command.discovery_input_hash),
                result_code: "pending",
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn cancel_mcp_discovery(
        &self,
        command: CancelMcpDiscoveryCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT * FROM mcp_discovery_operations
             WHERE server_id=$1 AND discovery_id=$2 FOR UPDATE",
        )
        .bind(&command.server_id)
        .bind(&command.discovery_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        let current = discovery_status(
            row.try_get::<String, _>("discovery_status")
                .map_err(storage)?
                .as_str(),
        )?;
        let now = database_time(command.metadata.now);
        if current == McpDiscoveryStatus::Pending {
            sqlx::query(
                "UPDATE mcp_discovery_operations SET
                   discovery_status='cancelled',cancel_requested=true,finished_at=$1
                 WHERE discovery_id=$2 AND discovery_status='pending'",
            )
            .bind(now)
            .bind(&command.discovery_id)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        } else if current == McpDiscoveryStatus::Running {
            sqlx::query(
                "UPDATE mcp_discovery_operations SET cancel_requested=true WHERE discovery_id=$1",
            )
            .bind(&command.discovery_id)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        let updated = sqlx::query("SELECT * FROM mcp_discovery_operations WHERE discovery_id=$1")
            .bind(&command.discovery_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
        let operation = postgres_discovery(&updated)?;
        let response = serde_json::to_value(&operation).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut tx,
            &command.metadata,
            PostgresFinalize {
                event_kind: "mcp.discovery.cancel_requested",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.discovery_id),
                before_hash: None,
                after_hash: None,
                result_code: operation.status.as_str(),
                status: 200,
                response,
                etag: None,
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_mcp_discovery(
        &self,
        server_id: &str,
        discovery_id: &str,
    ) -> Result<Option<McpDiscoveryOperation>, RepositoryError> {
        sqlx::query("SELECT * FROM mcp_discovery_operations WHERE server_id=$1 AND discovery_id=$2")
            .bind(server_id)
            .bind(discovery_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(RepositoryError::storage)?
            .as_ref()
            .map(postgres_discovery)
            .transpose()
    }

    async fn get_mcp_discovery_snapshot(
        &self,
        server_id: &str,
        discovery_id: &str,
    ) -> Result<Option<McpDiscoverySnapshot>, RepositoryError> {
        sqlx::query(
            "SELECT discovery_id,server_id,source_draft_version,discovery_input_hash,
                    catalog_fingerprint,document,created_at
             FROM mcp_discovery_snapshots WHERE server_id=$1 AND discovery_id=$2",
        )
        .bind(server_id)
        .bind(discovery_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_snapshot)
        .transpose()
    }

    async fn list_mcp_discoveries(
        &self,
        server_id: &str,
        requested_status: Option<McpDiscoveryStatus>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpDiscoveryOperation>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT * FROM mcp_discovery_operations
             WHERE server_id=$1 AND ($2::text IS NULL OR discovery_status=$2)
               AND ($3::text IS NULL OR discovery_id>$3)
             ORDER BY discovery_id LIMIT $4",
        )
        .bind(server_id)
        .bind(requested_status.map(McpDiscoveryStatus::as_str))
        .bind(cursor)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(postgres_discovery)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor =
            (items.len() > limit as usize).then(|| items.remove(limit as usize).discovery_id);
        Ok(McpManagementPage { items, next_cursor })
    }

    async fn claim_mcp_discoveries(
        &self,
        command: ClaimMcpDiscoveriesCommand,
    ) -> Result<Vec<McpDiscoveryClaim>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "SELECT discovery_id FROM mcp_discovery_operations
             WHERE NOT cancel_requested AND (
               discovery_status='pending'
               OR (discovery_status='running' AND claim_expires_at<=$1)
             )
             ORDER BY created_at,discovery_id
             FOR UPDATE SKIP LOCKED LIMIT $2",
        )
        .bind(database_time(command.now))
        .bind(i64::from(command.limit))
        .fetch_all(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let discovery_id: String = row
                .try_get("discovery_id")
                .map_err(RepositoryError::storage)?;
            let claim_token = format!("mclaim_{}", Uuid::new_v4().simple());
            let claimed = sqlx::query(
                "UPDATE mcp_discovery_operations SET
                   discovery_status='running',claimed_by=$1,claim_token=$2,claim_expires_at=$3,
                   attempts=attempts+1,started_at=COALESCE(started_at,$4)
                 WHERE discovery_id=$5
                 RETURNING *",
            )
            .bind(&command.worker_id)
            .bind(&claim_token)
            .bind(database_time(command.lease_expires_at))
            .bind(database_time(command.now))
            .bind(&discovery_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
            claims.push(McpDiscoveryClaim {
                operation: postgres_discovery(&claimed)?,
                claim_token,
                draft_document: claimed
                    .try_get("draft_document")
                    .map_err(RepositoryError::storage)?,
            });
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn complete_mcp_discovery(
        &self,
        command: CompleteMcpDiscoveryCommand,
    ) -> Result<(), McpManagementWriteError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let row = sqlx::query(
            "SELECT * FROM mcp_discovery_operations
             WHERE discovery_id=$1 AND discovery_status='running' AND claim_token=$2
             FOR UPDATE",
        )
        .bind(&command.discovery_id)
        .bind(&command.claim_token)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::FenceLost,
        ))?;
        let server_id: String = row.try_get("server_id").map_err(storage)?;
        let source_draft_version: i64 = row.try_get("source_draft_version").map_err(storage)?;
        let discovery_input_hash: String = row.try_get("discovery_input_hash").map_err(storage)?;
        let cancelled: bool = row.try_get("cancel_requested").map_err(storage)?;
        let now = database_time(command.now);
        let result = if cancelled {
            CompleteMcpDiscoveryResult::Cancelled
        } else {
            command.result
        };
        let (event_kind, result_code) = match result {
            CompleteMcpDiscoveryResult::Succeeded {
                catalog_fingerprint,
                snapshot_document,
            } => {
                if canonical_hash(&snapshot_document)? != catalog_fingerprint {
                    return Err(McpManagementWriteError::Conflict(
                        McpManagementConflict::ValidationFailed,
                    ));
                }
                let indexes = discovery_indexes(&snapshot_document).map_err(|_| {
                    McpManagementWriteError::Conflict(McpManagementConflict::ValidationFailed)
                })?;
                sqlx::query(
                    "INSERT INTO mcp_discovery_snapshots(
                       discovery_id,server_id,source_draft_version,discovery_input_hash,
                       catalog_fingerprint,document,created_at
                     ) VALUES($1,$2,$3,$4,$5,$6,$7)",
                )
                .bind(&command.discovery_id)
                .bind(&server_id)
                .bind(source_draft_version)
                .bind(&discovery_input_hash)
                .bind(&catalog_fingerprint)
                .bind(snapshot_document)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                for (ordinal, tool) in indexes.tools.iter().enumerate() {
                    sqlx::query(
                        "INSERT INTO mcp_discovery_tools(
                           discovery_id,ordinal,remote_name,schema_hash,document
                         ) VALUES($1,$2,$3,$4,$5)",
                    )
                    .bind(&command.discovery_id)
                    .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
                    .bind(&tool.remote_name)
                    .bind(&tool.schema_hash)
                    .bind(&tool.document)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                }
                for (ordinal, resource) in indexes.resources.iter().enumerate() {
                    sqlx::query(
                        "INSERT INTO mcp_discovery_resources(
                           discovery_id,candidate_kind,ordinal,resource_identity,document
                         ) VALUES($1,$2,$3,$4,$5)",
                    )
                    .bind(&command.discovery_id)
                    .bind(resource.kind)
                    .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
                    .bind(&resource.identity)
                    .bind(&resource.document)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                }
                for (ordinal, prompt) in indexes.prompts.iter().enumerate() {
                    sqlx::query(
                        "INSERT INTO mcp_discovery_prompts(
                           discovery_id,ordinal,remote_name,document
                         ) VALUES($1,$2,$3,$4)",
                    )
                    .bind(&command.discovery_id)
                    .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
                    .bind(&prompt.identity)
                    .bind(&prompt.document)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                }
                sqlx::query(
                    "UPDATE mcp_discovery_operations SET
                       discovery_status='succeeded',claimed_by=NULL,claim_token=NULL,
                       claim_expires_at=NULL,finished_at=$1 WHERE discovery_id=$2",
                )
                .bind(now)
                .bind(&command.discovery_id)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                ("mcp.discovery.succeeded", "succeeded")
            }
            CompleteMcpDiscoveryResult::Failed(failure) => {
                sqlx::query(
                    "UPDATE mcp_discovery_operations SET
                       discovery_status='failed',claimed_by=NULL,claim_token=NULL,
                       claim_expires_at=NULL,failure_code=$1,failure_stage=$2,
                       failure_retryable=$3,failure_correlation_id=$4,finished_at=$5
                     WHERE discovery_id=$6",
                )
                .bind(&failure.code)
                .bind(&failure.stage)
                .bind(failure.retryable)
                .bind(&failure.correlation_id)
                .bind(now)
                .bind(&command.discovery_id)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                ("mcp.discovery.failed", "failed")
            }
            CompleteMcpDiscoveryResult::Cancelled => {
                sqlx::query(
                    "UPDATE mcp_discovery_operations SET
                       discovery_status='cancelled',cancel_requested=true,claimed_by=NULL,
                       claim_token=NULL,claim_expires_at=NULL,finished_at=$1 WHERE discovery_id=$2",
                )
                .bind(now)
                .bind(&command.discovery_id)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                ("mcp.discovery.cancelled", "cancelled")
            }
        };
        sqlx::query(
            "INSERT INTO mcp_management_audit_events(
               event_kind,server_id,subject_id,actor_id,request_id_hash,result_code,created_at
             ) VALUES($1,$2,$3,'discovery-worker',$4,$5,$6)",
        )
        .bind(event_kind)
        .bind(&server_id)
        .bind(&command.discovery_id)
        .bind(prefixed_sha256(command.claim_token.as_bytes()))
        .bind(result_code)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO mcp_management_outbox(
               event_id,event_kind,server_id,subject_id,safe_payload,created_at
             ) VALUES($1,$2,$3,$4,$5,$6)",
        )
        .bind(format!("mout_{}", Uuid::new_v4().simple()))
        .bind(event_kind)
        .bind(&server_id)
        .bind(&command.discovery_id)
        .bind(json!({
            "server_id":server_id,
            "discovery_id":command.discovery_id,
            "result_code":result_code
        }))
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(())
    }

    async fn mark_mcp_discovery_stale(
        &self,
        command: MarkMcpDiscoveryStaleCommand,
    ) -> Result<u64, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let result = sqlx::query(
            "UPDATE mcp_discovery_operations SET stale=true,stale_reason=$1
             WHERE server_id=$2 AND discovery_status='succeeded' AND NOT stale",
        )
        .bind(&command.reason_code)
        .bind(&command.server_id)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if result.rows_affected() > 0 {
            sqlx::query(
                "INSERT INTO mcp_management_outbox(
                   event_id,event_kind,server_id,subject_id,safe_payload,created_at
                 ) VALUES($1,'mcp.discovery.stale',$2,$2,$3,$4)",
            )
            .bind(format!("mout_{}", Uuid::new_v4().simple()))
            .bind(&command.server_id)
            .bind(json!({
                "server_id":command.server_id,
                "reason_code":command.reason_code
            }))
            .bind(database_time(command.now))
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(result.rows_affected())
    }

    async fn cleanup_terminal_mcp_discoveries(
        &self,
        finished_before: DateTime<Utc>,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError> {
        if limit == 0 || limit > 1_000 {
            return Err(invalid_data());
        }
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let result = sqlx::query(
            "DELETE FROM mcp_discovery_operations
             WHERE discovery_id IN (
               SELECT discovery_id FROM mcp_discovery_operations
               WHERE discovery_status IN('failed','cancelled')
                 AND finished_at IS NOT NULL AND finished_at<$1
               ORDER BY finished_at,discovery_id LIMIT $2
               FOR UPDATE SKIP LOCKED
             )",
        )
        .bind(database_time(finished_before))
        .bind(i64::from(limit))
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let deleted = result.rows_affected();
        if deleted > 0 {
            let request_payload = encode_json(&json!({
                "finished_before":finished_before,
                "limit":limit
            }))?;
            let request_hash = prefixed_sha256(request_payload.as_bytes());
            sqlx::query(
                "INSERT INTO mcp_management_audit_events(
                   event_kind,server_id,subject_id,actor_id,request_id_hash,result_code,created_at
                 ) VALUES('mcp.discovery.retention',NULL,'terminal_discoveries','system',$1,'deleted',$2)",
            )
            .bind(&request_hash)
            .bind(database_time(now))
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
            sqlx::query(
                "INSERT INTO mcp_management_outbox(
                   event_id,event_kind,server_id,subject_id,safe_payload,created_at
                 ) VALUES($1,'mcp.discovery.retention','system','terminal_discoveries',$2,$3)",
            )
            .bind(format!("mout_{}", Uuid::new_v4().simple()))
            .bind(json!({"deleted":deleted,"finished_before":finished_before}))
            .bind(database_time(now))
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(deleted)
    }
    async fn create_mcp_validation(
        &self,
        command: CreateMcpValidationCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        if canonical_hash(&command.report.document)? != command.report.report_hash
            || command
                .report
                .document
                .get("valid")
                .and_then(Value::as_bool)
                != Some(command.report.valid)
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ValidationFailed,
            ));
        }
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT d.draft_version,d.discovery_input_hash,o.discovery_status,o.stale,
                    o.discovery_input_hash AS snapshot_input
             FROM mcp_server_drafts d
             JOIN mcp_discovery_operations o ON o.server_id=d.server_id
             WHERE d.server_id=$1 AND o.discovery_id=$2 FOR UPDATE OF d,o",
        )
        .bind(&command.report.server_id)
        .bind(&command.report.discovery_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        if i64_to_u64(row.try_get("draft_version").map_err(storage)?)?
            != command.expected_draft_version
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        let input: String = row.try_get("discovery_input_hash").map_err(storage)?;
        let snapshot_input: String = row.try_get("snapshot_input").map_err(storage)?;
        if input != command.expected_discovery_input_hash
            || snapshot_input != command.expected_discovery_input_hash
            || row
                .try_get::<String, _>("discovery_status")
                .map_err(storage)?
                != "succeeded"
            || row.try_get::<bool, _>("stale").map_err(storage)?
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::DiscoveryStale,
            ));
        }
        let report = &command.report;
        sqlx::query(
            "INSERT INTO mcp_validation_reports(
               validation_id,server_id,draft_version,discovery_id,report_hash,valid,
               document,created_at,created_by
             ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        )
        .bind(&report.validation_id)
        .bind(&report.server_id)
        .bind(u64_to_i64(report.draft_version)?)
        .bind(&report.discovery_id)
        .bind(&report.report_hash)
        .bind(report.valid)
        .bind(&report.document)
        .bind(database_time(report.created_at))
        .bind(&report.created_by)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(report).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut tx,
            &command.metadata,
            PostgresFinalize {
                event_kind: "mcp.validation.created",
                server_id: Some(&report.server_id),
                subject_id: Some(&report.validation_id),
                before_hash: None,
                after_hash: Some(&report.report_hash),
                result_code: if report.valid { "valid" } else { "invalid" },
                status: 201,
                response,
                etag: Some(format!("\"validation-{}\"", report.report_hash)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_mcp_validation(
        &self,
        server_id: &str,
        validation_id: &str,
    ) -> Result<Option<McpValidationReport>, RepositoryError> {
        sqlx::query(
            "SELECT validation_id,server_id,draft_version,discovery_id,report_hash,valid,
                    document,created_at,created_by
             FROM mcp_validation_reports WHERE server_id=$1 AND validation_id=$2",
        )
        .bind(server_id)
        .bind(validation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_validation)
        .transpose()
    }

    async fn publish_mcp_revision(
        &self,
        command: PublishMcpRevisionCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        if canonical_hash(&command.document)? != command.revision_hash {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ValidationFailed,
            ));
        }
        let indexes = revision_indexes(&command.document).map_err(|_| {
            McpManagementWriteError::Conflict(McpManagementConflict::ValidationFailed)
        })?;
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT s.server_state,d.draft_version,d.discovery_input_hash,
                    o.discovery_status,o.stale,o.discovery_input_hash AS snapshot_input,
                    x.catalog_fingerprint,v.draft_version AS validation_draft,
                    v.discovery_id AS validation_discovery,v.valid
             FROM mcp_managed_servers s
             JOIN mcp_server_drafts d USING(server_id)
             JOIN mcp_discovery_operations o ON o.server_id=s.server_id AND o.discovery_id=$1
             JOIN mcp_discovery_snapshots x ON x.discovery_id=o.discovery_id
             JOIN mcp_validation_reports v ON v.server_id=s.server_id AND v.validation_id=$2
             WHERE s.server_id=$3 FOR UPDATE OF s,d,o",
        )
        .bind(&command.discovery_id)
        .bind(&command.validation_id)
        .bind(&command.server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?
        .ok_or(McpManagementWriteError::Conflict(
            McpManagementConflict::NotFound,
        ))?;
        if row.try_get::<String, _>("server_state").map_err(storage)? == "retired" {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        if i64_to_u64(row.try_get("draft_version").map_err(storage)?)?
            != command.expected_draft_version
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        let draft_input: String = row.try_get("discovery_input_hash").map_err(storage)?;
        let snapshot_input: String = row.try_get("snapshot_input").map_err(storage)?;
        if draft_input != snapshot_input
            || row
                .try_get::<String, _>("discovery_status")
                .map_err(storage)?
                != "succeeded"
            || row.try_get::<bool, _>("stale").map_err(storage)?
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::DiscoveryStale,
            ));
        }
        if i64_to_u64(row.try_get("validation_draft").map_err(storage)?)?
            != command.expected_draft_version
            || row
                .try_get::<String, _>("validation_discovery")
                .map_err(storage)?
                != command.discovery_id
            || !row.try_get::<bool, _>("valid").map_err(storage)?
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ValidationFailed,
            ));
        }
        let catalog_fingerprint: String = row.try_get("catalog_fingerprint").map_err(storage)?;
        let revision_number = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(revision_number),0)+1 FROM mcp_server_revisions WHERE server_id=$1",
        )
        .bind(&command.server_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO mcp_server_revisions(
               revision_id,server_id,revision_number,source_draft_version,discovery_id,
               validation_id,catalog_fingerprint,revision_hash,document,created_at,created_by
             ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(&command.revision_id)
        .bind(&command.server_id)
        .bind(revision_number)
        .bind(u64_to_i64(command.expected_draft_version)?)
        .bind(&command.discovery_id)
        .bind(&command.validation_id)
        .bind(&catalog_fingerprint)
        .bind(&command.revision_hash)
        .bind(&command.document)
        .bind(now)
        .bind(&command.metadata.operator_id)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        for (ordinal, tool) in indexes.tools.iter().enumerate() {
            sqlx::query(
                "INSERT INTO mcp_revision_tools(
                   revision_id,ordinal,remote_name,alias,action_id,binding_hash,document
                 ) VALUES($1,$2,$3,$4,$5,$6,$7)",
            )
            .bind(&command.revision_id)
            .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
            .bind(&tool.remote_name)
            .bind(&tool.alias)
            .bind(&tool.action_id)
            .bind(&tool.binding_hash)
            .bind(&tool.document)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        for (ordinal, resource) in indexes.resources.iter().enumerate() {
            sqlx::query(
                "INSERT INTO mcp_revision_resources(
                   revision_id,binding_kind,ordinal,resource_identity,document
                 ) VALUES($1,$2,$3,$4,$5)",
            )
            .bind(&command.revision_id)
            .bind(resource.kind)
            .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
            .bind(&resource.identity)
            .bind(&resource.document)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        for (ordinal, prompt) in indexes.prompts.iter().enumerate() {
            sqlx::query(
                "INSERT INTO mcp_revision_prompts(
                   revision_id,ordinal,remote_name,document
                 ) VALUES($1,$2,$3,$4)",
            )
            .bind(&command.revision_id)
            .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
            .bind(&prompt.identity)
            .bind(&prompt.document)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        let revision = McpServerRevision {
            revision_id: command.revision_id.clone(),
            server_id: command.server_id.clone(),
            revision_number: i64_to_u64(revision_number)?,
            source_draft_version: command.expected_draft_version,
            discovery_id: command.discovery_id,
            validation_id: command.validation_id,
            catalog_fingerprint,
            revision_hash: command.revision_hash.clone(),
            document: command.document,
            created_at: now,
            created_by: command.metadata.operator_id.clone(),
        };
        let response = serde_json::to_value(&revision).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut tx,
            &command.metadata,
            PostgresFinalize {
                event_kind: "mcp.revision.published",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.revision_id),
                before_hash: None,
                after_hash: Some(&command.revision_hash),
                result_code: "published",
                status: 201,
                response,
                etag: Some(format!("\"revision-{}\"", command.revision_hash)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn activate_mcp_revision(
        &self,
        command: ActivateMcpRevisionCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        if command.readiness_expires_at < command.metadata.now {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut tx, &command.metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT * FROM mcp_managed_servers WHERE server_id=$1 FOR UPDATE")
            .bind(&command.server_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?
            .ok_or(McpManagementWriteError::Conflict(
                McpManagementConflict::NotFound,
            ))?;
        let mut server = postgres_server(&row)?;
        if server.server_version != command.expected_server_version {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        if server.state == McpManagedServerState::Retired {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::ForbiddenState,
            ));
        }
        if sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_server_revisions WHERE server_id=$1 AND revision_id=$2",
        )
        .bind(&command.server_id)
        .bind(&command.revision_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?
            == 0
        {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::NotFound,
            ));
        }
        let previous_revision = server.active_revision_id.clone();
        server.server_version = server
            .server_version
            .checked_add(1)
            .ok_or_else(|| McpManagementWriteError::Repository(invalid_data()))?;
        server.state = McpManagedServerState::Active;
        server.active_revision_id = Some(command.revision_id.clone());
        server.updated_at = database_time(command.metadata.now);
        sqlx::query(
            "UPDATE mcp_managed_servers SET
               server_state='active',active_revision_id=$1,server_version=$2,updated_at=$3
             WHERE server_id=$4 AND server_version=$5",
        )
        .bind(&command.revision_id)
        .bind(u64_to_i64(server.server_version)?)
        .bind(server.updated_at)
        .bind(&command.server_id)
        .bind(u64_to_i64(command.expected_server_version)?)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(&server).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut tx,
            &command.metadata,
            PostgresFinalize {
                event_kind: "mcp.revision.activated",
                server_id: Some(&command.server_id),
                subject_id: Some(&command.revision_id),
                before_hash: previous_revision.as_deref(),
                after_hash: Some(&command.revision_id),
                result_code: "active",
                status: 200,
                response,
                etag: Some(server_etag(server.server_version)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn disable_mcp_server(
        &self,
        command: DisableMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        self.postgres_change_server_state(
            command.metadata,
            &command.server_id,
            command.expected_server_version,
            McpManagedServerState::Disabled,
            None,
        )
        .await
    }

    async fn retire_mcp_server(
        &self,
        command: RetireMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        self.postgres_change_server_state(
            command.metadata,
            &command.server_id,
            command.expected_server_version,
            McpManagedServerState::Retired,
            Some(&command.reason_code),
        )
        .await
    }

    async fn get_mcp_revision(
        &self,
        server_id: &str,
        revision_id: &str,
    ) -> Result<Option<McpServerRevision>, RepositoryError> {
        sqlx::query(
            "SELECT revision_id,server_id,revision_number,source_draft_version,discovery_id,
                    validation_id,catalog_fingerprint,revision_hash,document,created_at,created_by
             FROM mcp_server_revisions WHERE server_id=$1 AND revision_id=$2",
        )
        .bind(server_id)
        .bind(revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_revision)
        .transpose()
    }

    async fn list_mcp_revisions(
        &self,
        server_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpServerRevision>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT revision_id,server_id,revision_number,source_draft_version,discovery_id,
                    validation_id,catalog_fingerprint,revision_hash,document,created_at,created_by
             FROM mcp_server_revisions
             WHERE server_id=$1 AND ($2::text IS NULL OR revision_id>$2)
             ORDER BY revision_id LIMIT $3",
        )
        .bind(server_id)
        .bind(cursor)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(postgres_revision)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor =
            (items.len() > limit as usize).then(|| items.remove(limit as usize).revision_id);
        Ok(McpManagementPage { items, next_cursor })
    }

    async fn load_active_mcp_revisions(
        &self,
    ) -> Result<Vec<(McpManagedServer, McpServerRevision)>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT s.server_id AS s_server_id,s.display_name,s.server_state,s.server_version,
                    s.draft_version,s.active_revision_id,s.disable_fence,s.created_at AS s_created_at,
                    s.updated_at AS s_updated_at,r.*
             FROM mcp_managed_servers s JOIN mcp_server_revisions r
               ON r.server_id=s.server_id AND r.revision_id=s.active_revision_id
             WHERE s.server_state='active' ORDER BY s.server_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.iter()
            .map(|row| {
                let server = McpManagedServer {
                    server_id: row
                        .try_get("s_server_id")
                        .map_err(RepositoryError::storage)?,
                    display_name: row
                        .try_get("display_name")
                        .map_err(RepositoryError::storage)?,
                    state: state(
                        row.try_get::<String, _>("server_state")
                            .map_err(RepositoryError::storage)?
                            .as_str(),
                    )?,
                    server_version: i64_to_u64(
                        row.try_get("server_version")
                            .map_err(RepositoryError::storage)?,
                    )?,
                    draft_version: i64_to_u64(
                        row.try_get("draft_version")
                            .map_err(RepositoryError::storage)?,
                    )?,
                    active_revision_id: row
                        .try_get("active_revision_id")
                        .map_err(RepositoryError::storage)?,
                    disable_fence: i64_to_u64(
                        row.try_get("disable_fence")
                            .map_err(RepositoryError::storage)?,
                    )?,
                    created_at: row
                        .try_get("s_created_at")
                        .map_err(RepositoryError::storage)?,
                    updated_at: row
                        .try_get("s_updated_at")
                        .map_err(RepositoryError::storage)?,
                };
                Ok((server, postgres_revision(row)?))
            })
            .collect()
    }

    async fn load_mcp_server_fence(
        &self,
        server_id: &str,
    ) -> Result<Option<McpServerFence>, RepositoryError> {
        let server = self.get_mcp_server(server_id).await?;
        Ok(server.map(|server| McpServerFence {
            server_id: server.server_id,
            state: server.state,
            active_revision_id: server.active_revision_id,
            disable_fence: server.disable_fence,
        }))
    }
}

impl PostgresDurableRepository {
    async fn postgres_change_server_state(
        &self,
        metadata: McpMutationMetadata,
        server_id: &str,
        expected_server_version: u64,
        target: McpManagedServerState,
        reason_code: Option<&str>,
    ) -> Result<McpMutationReceipt, McpManagementWriteError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut tx, &metadata).await? {
            tx.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT * FROM mcp_managed_servers WHERE server_id=$1 FOR UPDATE")
            .bind(server_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?
            .ok_or(McpManagementWriteError::Conflict(
                McpManagementConflict::NotFound,
            ))?;
        let mut server = postgres_server(&row)?;
        if server.server_version != expected_server_version {
            return Err(McpManagementWriteError::Conflict(
                McpManagementConflict::PreconditionFailed,
            ));
        }
        match target {
            McpManagedServerState::Disabled
                if server.active_revision_id.is_none()
                    || server.state == McpManagedServerState::Retired =>
            {
                return Err(McpManagementWriteError::Conflict(
                    McpManagementConflict::ForbiddenState,
                ));
            }
            McpManagedServerState::Retired if server.state == McpManagedServerState::Retired => {
                return Err(McpManagementWriteError::Conflict(
                    McpManagementConflict::ForbiddenState,
                ));
            }
            McpManagedServerState::Disabled | McpManagedServerState::Retired => {}
            McpManagedServerState::Draft | McpManagedServerState::Active => {
                return Err(McpManagementWriteError::Conflict(
                    McpManagementConflict::ForbiddenState,
                ));
            }
        }
        server.server_version = server
            .server_version
            .checked_add(1)
            .ok_or_else(|| McpManagementWriteError::Repository(invalid_data()))?;
        server.disable_fence = server
            .disable_fence
            .checked_add(1)
            .ok_or_else(|| McpManagementWriteError::Repository(invalid_data()))?;
        server.state = target;
        server.updated_at = database_time(metadata.now);
        sqlx::query(
            "UPDATE mcp_managed_servers SET
               server_state=$1,server_version=$2,disable_fence=$3,updated_at=$4
             WHERE server_id=$5 AND server_version=$6",
        )
        .bind(target.as_str())
        .bind(u64_to_i64(server.server_version)?)
        .bind(u64_to_i64(server.disable_fence)?)
        .bind(server.updated_at)
        .bind(server_id)
        .bind(u64_to_i64(expected_server_version)?)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let response = serde_json::to_value(&server).map_err(|_| invalid_data())?;
        let event_kind = if target == McpManagedServerState::Disabled {
            "mcp.server.disabled"
        } else {
            "mcp.server.retired"
        };
        let receipt = postgres_finalize(
            &mut tx,
            &metadata,
            PostgresFinalize {
                event_kind,
                server_id: Some(server_id),
                subject_id: Some(server_id),
                before_hash: None,
                after_hash: reason_code,
                result_code: target.as_str(),
                status: 200,
                response,
                etag: Some(server_etag(server.server_version)),
            },
        )
        .await?;
        tx.commit().await.map_err(storage)?;
        Ok(receipt)
    }
}
