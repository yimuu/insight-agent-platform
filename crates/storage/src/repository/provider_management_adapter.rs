use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgListener, AssertSqlSafe, Row, Sqlite, Transaction};
use uuid::Uuid;

use insight_durable::{
    ActivateProviderRevisionCommand, CancelProviderOperationCommand,
    ClaimProviderOperationsCommand, CompleteProviderConnectionTestCommand,
    CompleteProviderConnectionTestResult, CompleteProviderDiscoveryCommand,
    CompleteProviderDiscoveryResult, CreateProviderCommand, CreateProviderConnectionTestCommand,
    CreateProviderDiscoveryCommand, CreateProviderValidationCommand,
    DeactivateProviderRevisionCommand, DeleteProviderCommand, ManagedProvider,
    ProviderConnectionTest, ProviderConnectionTestClaim, ProviderConnectionTestMode,
    ProviderConnectionTestRuntimeCount, ProviderDiscoveryClaim, ProviderDiscoveryOperation,
    ProviderDiscoverySnapshot, ProviderFence, ProviderLegacyModelBinding,
    ProviderManagementConflict, ProviderManagementDurableRepository,
    ProviderManagementNotificationStream, ProviderManagementOperationCount, ProviderManagementPage,
    ProviderManagementRuntimeStats, ProviderManagementWriteError, ProviderModelCandidate,
    ProviderMutationMetadata, ProviderMutationReceipt, ProviderOperationFailure,
    ProviderOperationStatus, ProviderOperationalState, ProviderRevision, ProviderStoredDraft,
    ProviderValidationReport, PublishProviderRevisionCommand,
    RecordProviderManagementRejectionCommand, ReplaceProviderDraftCommand, RepositoryError,
    ResumeProviderCommand, RetireProviderCommand, SuspendProviderCommand,
    PROVIDER_MANAGEMENT_NOTIFY_CHANNEL_PREFIX,
};

use super::{
    database_time, PostgresDurableRepository, RepositoryErrorExt as _, SqliteDurableRepository,
};

const MANAGEMENT_OUTBOX_MAX_ROWS: i64 = 4_096;

fn storage(error: sqlx::Error) -> ProviderManagementWriteError {
    ProviderManagementWriteError::Repository(RepositoryError::storage(error))
}

fn invalid_data() -> RepositoryError {
    RepositoryError::invalid_data()
}

struct PostgresProviderManagementNotificationStream {
    listener: PgListener,
}

#[async_trait]
impl ProviderManagementNotificationStream for PostgresProviderManagementNotificationStream {
    async fn recv(&mut self) -> Result<(), RepositoryError> {
        self.listener
            .try_recv()
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(RepositoryError::storage_unavailable)?;
        Ok(())
    }
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

fn u64_to_i64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| invalid_data())
}

fn i64_to_u64(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| invalid_data())
}

fn i64_to_u32(value: i64) -> Result<u32, RepositoryError> {
    u32::try_from(value).map_err(|_| invalid_data())
}

fn operational_state(value: &str) -> Result<ProviderOperationalState, RepositoryError> {
    match value {
        "enabled" => Ok(ProviderOperationalState::Enabled),
        "suspended" => Ok(ProviderOperationalState::Suspended),
        "retired" => Ok(ProviderOperationalState::Retired),
        _ => Err(invalid_data()),
    }
}

fn operation_status(value: &str) -> Result<ProviderOperationStatus, RepositoryError> {
    match value {
        "pending" => Ok(ProviderOperationStatus::Pending),
        "running" => Ok(ProviderOperationStatus::Running),
        "succeeded" => Ok(ProviderOperationStatus::Succeeded),
        "failed" => Ok(ProviderOperationStatus::Failed),
        "cancelled" => Ok(ProviderOperationStatus::Cancelled),
        _ => Err(invalid_data()),
    }
}

fn test_mode(value: &str) -> Result<ProviderConnectionTestMode, RepositoryError> {
    match value {
        "metadata" => Ok(ProviderConnectionTestMode::Metadata),
        "canary" => Ok(ProviderConnectionTestMode::Canary),
        "capability_probe" => Ok(ProviderConnectionTestMode::CapabilityProbe),
        _ => Err(invalid_data()),
    }
}

fn provider_etag(version: u64) -> String {
    format!("\"provider-{version}\"")
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

fn model_documents(value: &Value) -> Result<Vec<(String, String, Value)>, RepositoryError> {
    let models = value
        .get("models")
        .and_then(Value::as_array)
        .ok_or_else(invalid_data)?;
    let mut names = BTreeSet::new();
    models
        .iter()
        .map(|model| {
            let model_id = model
                .get("id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(invalid_data)?
                .to_owned();
            if !names.insert(model_id.clone()) {
                return Err(invalid_data());
            }
            let fingerprint = canonical_hash(model)?;
            Ok((model_id, fingerprint, model.clone()))
        })
        .collect()
}

fn sqlite_discovery_select() -> &'static str {
    "SELECT discovery_id,provider_id,source_draft_version,provider_input_hash,
            operation_status,cancel_requested,attempts,claimed_by,claim_token,claim_expires_at,
            failure_code,failure_stage,failure_retryable,failure_correlation_id,stale,
            stale_reason,created_at,started_at,finished_at
     FROM provider_discovery_operations"
}

fn sqlite_test_select() -> &'static str {
    "SELECT test_id,provider_id,source_draft_version,provider_input_hash,test_mode,
            operation_status,cancel_requested,attempts,claimed_by,claim_token,claim_expires_at,
            failure_code,failure_stage,failure_retryable,failure_correlation_id,result_hash,
            result_document,created_at,started_at,finished_at
     FROM provider_connection_tests"
}

fn sqlite_provider(row: &sqlx::sqlite::SqliteRow) -> Result<ManagedProvider, RepositoryError> {
    Ok(ManagedProvider {
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        display_name: row
            .try_get("display_name")
            .map_err(RepositoryError::storage)?,
        adapter_type: row
            .try_get("adapter_type")
            .map_err(RepositoryError::storage)?,
        operational_state: operational_state(
            row.try_get::<String, _>("operational_state")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        provider_version: i64_to_u64(
            row.try_get("provider_version")
                .map_err(RepositoryError::storage)?,
        )?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        active_revision_id: row
            .try_get("active_revision_id")
            .map_err(RepositoryError::storage)?,
        suspension_fence: i64_to_u64(
            row.try_get("suspension_fence")
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

fn sqlite_draft(row: &sqlx::sqlite::SqliteRow) -> Result<ProviderStoredDraft, RepositoryError> {
    let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
    Ok(ProviderStoredDraft {
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_input_hash: row
            .try_get("provider_input_hash")
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

fn sqlite_failure(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<Option<ProviderOperationFailure>, RepositoryError> {
    let failure_code: Option<String> = row
        .try_get("failure_code")
        .map_err(RepositoryError::storage)?;
    match failure_code {
        Some(code) => Ok(Some(ProviderOperationFailure {
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
        })),
        None => Ok(None),
    }
}

fn sqlite_discovery(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<ProviderDiscoveryOperation, RepositoryError> {
    Ok(ProviderDiscoveryOperation {
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_input_hash: row
            .try_get("provider_input_hash")
            .map_err(RepositoryError::storage)?,
        status: operation_status(
            row.try_get::<String, _>("operation_status")
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
        failure: sqlite_failure(row)?,
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

fn sqlite_snapshot(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<ProviderDiscoverySnapshot, RepositoryError> {
    let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
    Ok(ProviderDiscoverySnapshot {
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_input_hash: row
            .try_get("provider_input_hash")
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

fn sqlite_test(row: &sqlx::sqlite::SqliteRow) -> Result<ProviderConnectionTest, RepositoryError> {
    let result_document: Option<String> = row
        .try_get("result_document")
        .map_err(RepositoryError::storage)?;
    Ok(ProviderConnectionTest {
        test_id: row.try_get("test_id").map_err(RepositoryError::storage)?,
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_input_hash: row
            .try_get("provider_input_hash")
            .map_err(RepositoryError::storage)?,
        mode: test_mode(
            row.try_get::<String, _>("test_mode")
                .map_err(RepositoryError::storage)?
                .as_str(),
        )?,
        status: operation_status(
            row.try_get::<String, _>("operation_status")
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
        failure: sqlite_failure(row)?,
        result_hash: row
            .try_get("result_hash")
            .map_err(RepositoryError::storage)?,
        result: result_document.as_deref().map(decode_json).transpose()?,
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

fn sqlite_validation(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<ProviderValidationReport, RepositoryError> {
    let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
    Ok(ProviderValidationReport {
        validation_id: row
            .try_get("validation_id")
            .map_err(RepositoryError::storage)?,
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_input_hash: row
            .try_get("provider_input_hash")
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

fn sqlite_revision(row: &sqlx::sqlite::SqliteRow) -> Result<ProviderRevision, RepositoryError> {
    let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
    Ok(ProviderRevision {
        revision_id: row
            .try_get("revision_id")
            .map_err(RepositoryError::storage)?,
        provider_id: row
            .try_get("provider_id")
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
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        connection_test_id: row
            .try_get("connection_test_id")
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
    metadata: &ProviderMutationMetadata,
) -> Result<Option<ProviderMutationReceipt>, ProviderManagementWriteError> {
    let row = sqlx::query(
        "SELECT request_hash,response_status,response_json,response_etag
         FROM provider_management_requests
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
    if row.try_get::<String, _>("request_hash").map_err(storage)? != metadata.request_hash {
        return Err(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::IdempotencyKeyReused,
        ));
    }
    let response: String = row.try_get("response_json").map_err(storage)?;
    Ok(Some(ProviderMutationReceipt {
        replayed: true,
        status: u16::try_from(row.try_get::<i64, _>("response_status").map_err(storage)?)
            .map_err(|_| ProviderManagementWriteError::Repository(invalid_data()))?,
        response: decode_json(&response)?,
        etag: row.try_get("response_etag").map_err(storage)?,
    }))
}

struct SqliteFinalize<'a> {
    event_kind: &'a str,
    provider_id: Option<&'a str>,
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
    metadata: &ProviderMutationMetadata,
    finalization: SqliteFinalize<'_>,
) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
    sqlx::query(
        "INSERT INTO provider_management_requests(
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
    .bind(encode_json(&finalization.response)?)
    .bind(&finalization.etag)
    .bind(database_time(metadata.now))
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    sqlx::query(
        "INSERT INTO provider_management_audit_events(
           event_kind,provider_id,subject_id,actor_id,capability,request_id_hash,before_hash,
           after_hash,result_code,created_at
         ) VALUES(?,?,?,?,?,?,?,?,?,?)",
    )
    .bind(finalization.event_kind)
    .bind(finalization.provider_id)
    .bind(finalization.subject_id)
    .bind(&metadata.operator_id)
    .bind(&metadata.capability)
    .bind(prefixed_sha256(metadata.request_id.as_bytes()))
    .bind(finalization.before_hash)
    .bind(finalization.after_hash)
    .bind(finalization.result_code)
    .bind(database_time(metadata.now))
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    if let (Some(provider_id), Some(subject_id)) =
        (finalization.provider_id, finalization.subject_id)
    {
        sqlx::query(
            "INSERT INTO provider_management_outbox(
               event_id,event_kind,provider_id,subject_id,safe_payload,created_at,delivered_at
             ) VALUES(?,?,?,?,?,?,NULL)",
        )
        .bind(format!("pout_{}", Uuid::new_v4().simple()))
        .bind(finalization.event_kind)
        .bind(provider_id)
        .bind(subject_id)
        .bind(encode_json(&json!({
            "provider_id": provider_id,
            "subject_id": subject_id,
            "result_code": finalization.result_code,
        }))?)
        .bind(database_time(metadata.now))
        .execute(&mut **transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "DELETE FROM provider_management_outbox WHERE event_id IN (
               SELECT event_id FROM provider_management_outbox
               ORDER BY created_at DESC,event_id DESC LIMIT -1 OFFSET ?
             )",
        )
        .bind(MANAGEMENT_OUTBOX_MAX_ROWS)
        .execute(&mut **transaction)
        .await
        .map_err(storage)?;
    }
    Ok(ProviderMutationReceipt {
        replayed: false,
        status: finalization.status,
        response: finalization.response,
        etag: finalization.etag,
    })
}

async fn sqlite_begin(
    repository: &SqliteDurableRepository,
) -> Result<Transaction<'_, Sqlite>, ProviderManagementWriteError> {
    repository
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(storage)
}

#[async_trait]
impl ProviderManagementDurableRepository for SqliteDurableRepository {
    async fn record_provider_management_rejection(
        &self,
        command: RecordProviderManagementRejectionCommand,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            "INSERT INTO provider_management_audit_events(
               event_kind,provider_id,subject_id,actor_id,capability,request_id_hash,before_hash,
               after_hash,result_code,created_at)
             VALUES('provider.request_rejected',?,?,?,?,?,NULL,NULL,?,?)",
        )
        .bind(command.provider_id)
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

    async fn replay_provider_mutation(
        &self,
        metadata: &ProviderMutationMetadata,
    ) -> Result<Option<ProviderMutationReceipt>, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        let receipt = sqlite_replay(&mut transaction, metadata).await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn create_provider(
        &self,
        command: CreateProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        if sqlx::query("SELECT 1 FROM managed_providers WHERE provider_id=?")
            .bind(&command.provider_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?
            .is_some()
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO managed_providers(
               provider_id,display_name,adapter_type,operational_state,provider_version,
               draft_version,active_revision_id,suspension_fence,created_at,updated_at
             ) VALUES(?,?,?,'enabled',1,1,NULL,0,?,?)",
        )
        .bind(&command.provider_id)
        .bind(&command.display_name)
        .bind(&command.adapter_type)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO provider_drafts(
               provider_id,draft_version,provider_input_hash,document,created_at,updated_at
             ) VALUES(?,1,?,?,?,?)",
        )
        .bind(&command.provider_id)
        .bind(&command.provider_input_hash)
        .bind(encode_json(&command.draft_document)?)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let provider = ManagedProvider {
            provider_id: command.provider_id.clone(),
            display_name: command.display_name,
            adapter_type: command.adapter_type,
            operational_state: ProviderOperationalState::Enabled,
            provider_version: 1,
            draft_version: 1,
            active_revision_id: None,
            suspension_fence: 0,
            created_at: now,
            updated_at: now,
        };
        let response = serde_json::to_value(&provider).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.created",
                provider_id: Some(&provider.provider_id),
                subject_id: Some(&provider.provider_id),
                before_hash: None,
                after_hash: Some(&command.provider_input_hash),
                result_code: "created",
                status: 201,
                response,
                etag: Some(provider_etag(1)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn replace_provider_draft(
        &self,
        command: ReplaceProviderDraftCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT p.operational_state,d.draft_version,d.provider_input_hash,d.created_at
             FROM managed_providers p JOIN provider_drafts d USING(provider_id)
             WHERE p.provider_id=?",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        if operational_state(
            row.try_get::<String, _>("operational_state")
                .map_err(storage)?
                .as_str(),
        )? == ProviderOperationalState::Retired
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        if actual != command.expected_draft_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let before_hash: String = row.try_get("provider_input_hash").map_err(storage)?;
        let created_at: DateTime<Utc> = row.try_get("created_at").map_err(storage)?;
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "UPDATE provider_drafts
             SET draft_version=?,provider_input_hash=?,document=?,updated_at=?
             WHERE provider_id=? AND draft_version=?",
        )
        .bind(u64_to_i64(next)?)
        .bind(&command.provider_input_hash)
        .bind(encode_json(&command.draft_document)?)
        .bind(now)
        .bind(&command.provider_id)
        .bind(u64_to_i64(actual)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "UPDATE managed_providers SET draft_version=?,updated_at=? WHERE provider_id=?",
        )
        .bind(u64_to_i64(next)?)
        .bind(now)
        .bind(&command.provider_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "UPDATE provider_discovery_operations
             SET stale=1,stale_reason='draft_changed'
             WHERE provider_id=? AND operation_status='succeeded' AND stale=0",
        )
        .bind(&command.provider_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let draft = ProviderStoredDraft {
            provider_id: command.provider_id.clone(),
            draft_version: next,
            provider_input_hash: command.provider_input_hash.clone(),
            document: command.draft_document,
            created_at,
            updated_at: now,
        };
        let response = serde_json::to_value(&draft).map_err(|_| invalid_data())?;
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.draft.replaced",
                provider_id: Some(&draft.provider_id),
                subject_id: Some(&draft.provider_id),
                before_hash: Some(&before_hash),
                after_hash: Some(&draft.provider_input_hash),
                result_code: "updated",
                status: 200,
                response,
                etag: Some(draft_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn delete_provider(
        &self,
        command: DeleteProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT provider_version,active_revision_id,operational_state
             FROM managed_providers WHERE provider_id=?",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        if i64_to_u64(row.try_get("provider_version").map_err(storage)?)?
            != command.expected_provider_version
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let active: Option<String> = row.try_get("active_revision_id").map_err(storage)?;
        let revision_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_revisions WHERE provider_id=?")
                .bind(&command.provider_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(storage)?;
        let open_operations: i64 = sqlx::query_scalar(
            "SELECT
               (SELECT COUNT(*) FROM provider_discovery_operations
                WHERE provider_id=? AND operation_status IN('pending','running')) +
               (SELECT COUNT(*) FROM provider_connection_tests
                WHERE provider_id=? AND operation_status IN('pending','running'))",
        )
        .bind(&command.provider_id)
        .bind(&command.provider_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        if active.is_some() || revision_count != 0 || open_operations != 0 {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::Referenced,
            ));
        }
        sqlx::query("DELETE FROM managed_providers WHERE provider_id=?")
            .bind(&command.provider_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"deleted":true});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.deleted",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.provider_id),
                before_hash: None,
                after_hash: None,
                result_code: "deleted",
                status: 200,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_provider(
        &self,
        provider_id: &str,
    ) -> Result<Option<ManagedProvider>, RepositoryError> {
        sqlx::query(
            "SELECT provider_id,display_name,adapter_type,operational_state,provider_version,
                    draft_version,active_revision_id,suspension_fence,created_at,updated_at
             FROM managed_providers WHERE provider_id=?",
        )
        .bind(provider_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_provider)
        .transpose()
    }

    async fn get_provider_draft(
        &self,
        provider_id: &str,
    ) -> Result<Option<ProviderStoredDraft>, RepositoryError> {
        sqlx::query(
            "SELECT provider_id,draft_version,provider_input_hash,document,created_at,updated_at
             FROM provider_drafts WHERE provider_id=?",
        )
        .bind(provider_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_draft)
        .transpose()
    }

    async fn list_providers(
        &self,
        state: Option<ProviderOperationalState>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ProviderManagementPage<ManagedProvider>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT provider_id,display_name,adapter_type,operational_state,provider_version,
                    draft_version,active_revision_id,suspension_fence,created_at,updated_at
             FROM managed_providers
             WHERE (? IS NULL OR operational_state=?)
               AND (? IS NULL OR created_at>? OR (created_at=? AND provider_id>?))
             ORDER BY created_at,provider_id LIMIT ?",
        )
        .bind(state.map(ProviderOperationalState::as_str))
        .bind(state.map(ProviderOperationalState::as_str))
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
            .map(sqlite_provider)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.provider_id))
        } else {
            None
        };
        Ok(ProviderManagementPage { items, next_cursor })
    }

    async fn create_provider_discovery(
        &self,
        command: CreateProviderDiscoveryCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT p.operational_state,d.draft_version,d.provider_input_hash,d.document
             FROM managed_providers p JOIN provider_drafts d USING(provider_id)
             WHERE p.provider_id=?",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            == "retired"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        let input_hash: String = row.try_get("provider_input_hash").map_err(storage)?;
        if actual != command.expected_draft_version || input_hash != command.provider_input_hash {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_discovery_operations
             WHERE provider_id=? AND operation_status IN('pending','running')",
        )
        .bind(&command.provider_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        if pending >= i64::from(command.max_pending_operations) {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::CapacityExceeded,
            ));
        }
        let document: String = row.try_get("document").map_err(storage)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO provider_discovery_operations(
               discovery_id,provider_id,source_draft_version,provider_input_hash,draft_document,
               operation_status,cancel_requested,attempts,stale,created_at
             ) VALUES(?,?,?,?,?,'pending',0,0,0,?)",
        )
        .bind(&command.discovery_id)
        .bind(&command.provider_id)
        .bind(u64_to_i64(actual)?)
        .bind(&input_hash)
        .bind(document)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({"discovery_id":command.discovery_id,"provider_id":command.provider_id,"status":"pending"});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.discovery.created",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.discovery_id),
                before_hash: None,
                after_hash: Some(&input_hash),
                result_code: "accepted",
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn cancel_provider_discovery(
        &self,
        command: CancelProviderOperationCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT operation_status FROM provider_discovery_operations
             WHERE provider_id=? AND discovery_id=?",
        )
        .bind(&command.provider_id)
        .bind(&command.operation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        let status: String = row.try_get("operation_status").map_err(storage)?;
        let now = database_time(command.metadata.now);
        match status.as_str() {
            "pending" => {
                sqlx::query(
                    "UPDATE provider_discovery_operations
                     SET operation_status='cancelled',cancel_requested=1,finished_at=?
                     WHERE discovery_id=? AND operation_status='pending'",
                )
                .bind(now)
                .bind(&command.operation_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage)?;
            }
            "running" => {
                sqlx::query(
                    "UPDATE provider_discovery_operations SET cancel_requested=1
                     WHERE discovery_id=? AND operation_status='running'",
                )
                .bind(&command.operation_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage)?;
            }
            _ => {}
        }
        let response = json!({"discovery_id":command.operation_id,"cancel_requested":true});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.discovery.cancel_requested",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.operation_id),
                before_hash: None,
                after_hash: None,
                result_code: "cancel_requested",
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_provider_discovery(
        &self,
        provider_id: &str,
        discovery_id: &str,
    ) -> Result<Option<ProviderDiscoveryOperation>, RepositoryError> {
        let query = format!(
            "{} WHERE provider_id=? AND discovery_id=?",
            sqlite_discovery_select()
        );
        sqlx::query(AssertSqlSafe(query.as_str()))
            .bind(provider_id)
            .bind(discovery_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(RepositoryError::storage)?
            .as_ref()
            .map(sqlite_discovery)
            .transpose()
    }

    async fn get_provider_discovery_snapshot(
        &self,
        provider_id: &str,
        discovery_id: &str,
    ) -> Result<Option<ProviderDiscoverySnapshot>, RepositoryError> {
        sqlx::query(
            "SELECT discovery_id,provider_id,source_draft_version,provider_input_hash,
                    catalog_fingerprint,document,created_at
             FROM provider_discovery_snapshots WHERE provider_id=? AND discovery_id=?",
        )
        .bind(provider_id)
        .bind(discovery_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_snapshot)
        .transpose()
    }

    async fn list_provider_model_candidates(
        &self,
        provider_id: &str,
        discovery_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ProviderManagementPage<ProviderModelCandidate>, RepositoryError> {
        let ordinal = cursor
            .map(str::parse::<i64>)
            .transpose()
            .map_err(|_| invalid_data())?;
        let rows = sqlx::query(
            "SELECT c.discovery_id,c.ordinal,c.model_id,c.candidate_fingerprint,c.document
             FROM provider_model_candidates c JOIN provider_discovery_snapshots s USING(discovery_id)
             WHERE s.provider_id=? AND c.discovery_id=? AND (? IS NULL OR c.ordinal>?)
             ORDER BY c.ordinal LIMIT ?",
        )
        .bind(provider_id)
        .bind(discovery_id)
        .bind(ordinal)
        .bind(ordinal)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(|row| {
                let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
                Ok(ProviderModelCandidate {
                    discovery_id: row
                        .try_get("discovery_id")
                        .map_err(RepositoryError::storage)?,
                    ordinal: i64_to_u32(row.try_get("ordinal").map_err(RepositoryError::storage)?)?,
                    model_id: row.try_get("model_id").map_err(RepositoryError::storage)?,
                    candidate_fingerprint: row
                        .try_get("candidate_fingerprint")
                        .map_err(RepositoryError::storage)?,
                    document: decode_json(&document)?,
                })
            })
            .collect::<Result<Vec<_>, RepositoryError>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items.last().map(|item| item.ordinal.to_string())
        } else {
            None
        };
        Ok(ProviderManagementPage { items, next_cursor })
    }

    async fn claim_provider_discoveries(
        &self,
        command: ClaimProviderOperationsCommand,
    ) -> Result<Vec<ProviderDiscoveryClaim>, RepositoryError> {
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "SELECT discovery_id FROM provider_discovery_operations
             WHERE (operation_status='pending'
                    OR (operation_status='running' AND claim_expires_at<=?))
               AND cancel_requested=0
             ORDER BY created_at,discovery_id LIMIT ?",
        )
        .bind(database_time(command.now))
        .bind(i64::from(command.limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let discovery_id: String = row
                .try_get("discovery_id")
                .map_err(RepositoryError::storage)?;
            let token = format!("pdc_{}", Uuid::new_v4().simple());
            let result = sqlx::query(
                "UPDATE provider_discovery_operations
                 SET operation_status='running',attempts=attempts+1,claimed_by=?,claim_token=?,
                     claim_expires_at=?,started_at=COALESCE(started_at,?)
                 WHERE discovery_id=? AND cancel_requested=0
                   AND (operation_status='pending'
                        OR (operation_status='running' AND claim_expires_at<=?))",
            )
            .bind(&command.worker_id)
            .bind(&token)
            .bind(database_time(command.lease_expires_at))
            .bind(database_time(command.now))
            .bind(&discovery_id)
            .bind(database_time(command.now))
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if result.rows_affected() == 0 {
                continue;
            }
            let query = format!(
                "SELECT q.* FROM ({}) q WHERE q.discovery_id=?",
                sqlite_discovery_select()
            );
            // The operation decoder intentionally excludes the captured secret-bearing Draft.
            let operation_row = sqlx::query(AssertSqlSafe(query.as_str()))
                .bind(&discovery_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
            let draft_document: String = sqlx::query_scalar(
                "SELECT draft_document FROM provider_discovery_operations WHERE discovery_id=?",
            )
            .bind(&discovery_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            claims.push(ProviderDiscoveryClaim {
                operation: sqlite_discovery(&operation_row)?,
                claim_token: token,
                draft_document: decode_json(&draft_document)?,
            });
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn complete_provider_discovery(
        &self,
        command: CompleteProviderDiscoveryCommand,
    ) -> Result<(), ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        let row = sqlx::query(
            "SELECT provider_id,source_draft_version,provider_input_hash,cancel_requested
             FROM provider_discovery_operations
             WHERE discovery_id=? AND operation_status='running' AND claim_token=?",
        )
        .bind(&command.discovery_id)
        .bind(&command.claim_token)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::FenceLost,
        ))?;
        let provider_id: String = row.try_get("provider_id").map_err(storage)?;
        let source_draft_version: i64 = row.try_get("source_draft_version").map_err(storage)?;
        let input_hash: String = row.try_get("provider_input_hash").map_err(storage)?;
        let cancel_requested = row.try_get::<i64, _>("cancel_requested").map_err(storage)? != 0;
        let now = database_time(command.now);
        let result = if cancel_requested {
            CompleteProviderDiscoveryResult::Cancelled
        } else {
            command.result
        };
        match result {
            CompleteProviderDiscoveryResult::Succeeded {
                catalog_fingerprint,
                snapshot_document,
            } => {
                if canonical_hash(&snapshot_document)? != catalog_fingerprint {
                    return Err(ProviderManagementWriteError::Conflict(
                        ProviderManagementConflict::CandidateMismatch,
                    ));
                }
                let models = model_documents(&snapshot_document).map_err(|_| {
                    ProviderManagementWriteError::Conflict(
                        ProviderManagementConflict::CandidateMismatch,
                    )
                })?;
                sqlx::query(
                    "INSERT INTO provider_discovery_snapshots(
                       discovery_id,provider_id,source_draft_version,provider_input_hash,
                       catalog_fingerprint,document,created_at
                     ) VALUES(?,?,?,?,?,?,?)",
                )
                .bind(&command.discovery_id)
                .bind(&provider_id)
                .bind(source_draft_version)
                .bind(&input_hash)
                .bind(&catalog_fingerprint)
                .bind(encode_json(&snapshot_document)?)
                .bind(now)
                .execute(&mut *transaction)
                .await
                .map_err(storage)?;
                for (ordinal, (model_id, fingerprint, document)) in models.iter().enumerate() {
                    sqlx::query(
                        "INSERT INTO provider_model_candidates(
                           discovery_id,ordinal,model_id,candidate_fingerprint,document
                         ) VALUES(?,?,?,?,?)",
                    )
                    .bind(&command.discovery_id)
                    .bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
                    .bind(model_id)
                    .bind(fingerprint)
                    .bind(encode_json(document)?)
                    .execute(&mut *transaction)
                    .await
                    .map_err(storage)?;
                }
                sqlx::query(
                    "UPDATE provider_discovery_operations
                     SET operation_status='succeeded',claimed_by=NULL,claim_token=NULL,
                         claim_expires_at=NULL,finished_at=? WHERE discovery_id=?",
                )
                .bind(now)
                .bind(&command.discovery_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage)?;
            }
            CompleteProviderDiscoveryResult::Failed(failure) => {
                sqlx::query(
                    "UPDATE provider_discovery_operations
                     SET operation_status='failed',claimed_by=NULL,claim_token=NULL,
                         claim_expires_at=NULL,failure_code=?,failure_stage=?,failure_retryable=?,
                         failure_correlation_id=?,finished_at=? WHERE discovery_id=?",
                )
                .bind(failure.code)
                .bind(failure.stage)
                .bind(failure.retryable)
                .bind(failure.correlation_id)
                .bind(now)
                .bind(&command.discovery_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage)?;
            }
            CompleteProviderDiscoveryResult::Cancelled => {
                sqlx::query(
                    "UPDATE provider_discovery_operations
                     SET operation_status='cancelled',cancel_requested=1,claimed_by=NULL,
                         claim_token=NULL,claim_expires_at=NULL,finished_at=? WHERE discovery_id=?",
                )
                .bind(now)
                .bind(&command.discovery_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage)?;
            }
        }
        transaction.commit().await.map_err(storage)?;
        Ok(())
    }

    async fn create_provider_connection_test(
        &self,
        command: CreateProviderConnectionTestCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT p.operational_state,d.draft_version,d.provider_input_hash,d.document
             FROM managed_providers p JOIN provider_drafts d USING(provider_id)
             WHERE p.provider_id=?",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            == "retired"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        let input_hash: String = row.try_get("provider_input_hash").map_err(storage)?;
        if actual != command.expected_draft_version || input_hash != command.provider_input_hash {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_connection_tests
             WHERE provider_id=? AND operation_status IN('pending','running')",
        )
        .bind(&command.provider_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        if pending >= i64::from(command.max_pending_operations) {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::CapacityExceeded,
            ));
        }
        let document: String = row.try_get("document").map_err(storage)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO provider_connection_tests(
               test_id,provider_id,source_draft_version,provider_input_hash,draft_document,
               test_mode,operation_status,cancel_requested,attempts,created_at
             ) VALUES(?,?,?,?,?,?,'pending',0,0,?)",
        )
        .bind(&command.test_id)
        .bind(&command.provider_id)
        .bind(u64_to_i64(actual)?)
        .bind(&input_hash)
        .bind(document)
        .bind(command.mode.as_str())
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response =
            json!({"test_id":command.test_id,"provider_id":command.provider_id,"status":"pending"});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.connection_test.created",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.test_id),
                before_hash: None,
                after_hash: Some(&input_hash),
                result_code: "accepted",
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn cancel_provider_connection_test(
        &self,
        command: CancelProviderOperationCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let status: String = sqlx::query_scalar(
            "SELECT operation_status FROM provider_connection_tests
             WHERE provider_id=? AND test_id=?",
        )
        .bind(&command.provider_id)
        .bind(&command.operation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        let now = database_time(command.metadata.now);
        if status == "pending" {
            sqlx::query(
                "UPDATE provider_connection_tests
                 SET operation_status='cancelled',cancel_requested=1,finished_at=?
                 WHERE test_id=? AND operation_status='pending'",
            )
            .bind(now)
            .bind(&command.operation_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        } else if status == "running" {
            sqlx::query(
                "UPDATE provider_connection_tests SET cancel_requested=1
                 WHERE test_id=? AND operation_status='running'",
            )
            .bind(&command.operation_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        }
        let response = json!({"test_id":command.operation_id,"cancel_requested":true});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.connection_test.cancel_requested",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.operation_id),
                before_hash: None,
                after_hash: None,
                result_code: "cancel_requested",
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_provider_connection_test(
        &self,
        provider_id: &str,
        test_id: &str,
    ) -> Result<Option<ProviderConnectionTest>, RepositoryError> {
        let query = format!("{} WHERE provider_id=? AND test_id=?", sqlite_test_select());
        sqlx::query(AssertSqlSafe(query.as_str()))
            .bind(provider_id)
            .bind(test_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(RepositoryError::storage)?
            .as_ref()
            .map(sqlite_test)
            .transpose()
    }

    async fn claim_provider_connection_tests(
        &self,
        command: ClaimProviderOperationsCommand,
    ) -> Result<Vec<ProviderConnectionTestClaim>, RepositoryError> {
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "SELECT test_id FROM provider_connection_tests
             WHERE (operation_status='pending'
                    OR (operation_status='running' AND claim_expires_at<=?))
               AND cancel_requested=0
             ORDER BY created_at,test_id LIMIT ?",
        )
        .bind(database_time(command.now))
        .bind(i64::from(command.limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let test_id: String = row.try_get("test_id").map_err(RepositoryError::storage)?;
            let token = format!("pct_{}", Uuid::new_v4().simple());
            let updated = sqlx::query(
                "UPDATE provider_connection_tests
                 SET operation_status='running',attempts=attempts+1,claimed_by=?,claim_token=?,
                     claim_expires_at=?,started_at=COALESCE(started_at,?)
                 WHERE test_id=? AND cancel_requested=0
                   AND (operation_status='pending'
                        OR (operation_status='running' AND claim_expires_at<=?))",
            )
            .bind(&command.worker_id)
            .bind(&token)
            .bind(database_time(command.lease_expires_at))
            .bind(database_time(command.now))
            .bind(&test_id)
            .bind(database_time(command.now))
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if updated.rows_affected() == 0 {
                continue;
            }
            let query = format!(
                "SELECT q.* FROM ({}) q WHERE q.test_id=?",
                sqlite_test_select()
            );
            let operation_row = sqlx::query(AssertSqlSafe(query.as_str()))
                .bind(&test_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
            let draft_document: String = sqlx::query_scalar(
                "SELECT draft_document FROM provider_connection_tests WHERE test_id=?",
            )
            .bind(&test_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            claims.push(ProviderConnectionTestClaim {
                operation: sqlite_test(&operation_row)?,
                claim_token: token,
                draft_document: decode_json(&draft_document)?,
            });
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn complete_provider_connection_test(
        &self,
        command: CompleteProviderConnectionTestCommand,
    ) -> Result<(), ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        let cancel_requested: i64 = sqlx::query_scalar(
            "SELECT cancel_requested FROM provider_connection_tests
             WHERE test_id=? AND operation_status='running' AND claim_token=?",
        )
        .bind(&command.test_id)
        .bind(&command.claim_token)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::FenceLost,
        ))?;
        let result = if cancel_requested != 0 {
            CompleteProviderConnectionTestResult::Cancelled
        } else {
            command.result
        };
        let now = database_time(command.now);
        match result {
            CompleteProviderConnectionTestResult::Succeeded {
                result_hash,
                result,
            } => {
                if canonical_hash(&result)? != result_hash {
                    return Err(ProviderManagementWriteError::Conflict(
                        ProviderManagementConflict::CandidateMismatch,
                    ));
                }
                sqlx::query(
                    "UPDATE provider_connection_tests
                     SET operation_status='succeeded',claimed_by=NULL,claim_token=NULL,
                         claim_expires_at=NULL,result_hash=?,result_document=?,finished_at=?
                     WHERE test_id=?",
                )
                .bind(result_hash)
                .bind(encode_json(&result)?)
                .bind(now)
                .bind(&command.test_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage)?;
            }
            CompleteProviderConnectionTestResult::Failed(failure) => {
                sqlx::query(
                    "UPDATE provider_connection_tests
                     SET operation_status='failed',claimed_by=NULL,claim_token=NULL,
                         claim_expires_at=NULL,failure_code=?,failure_stage=?,failure_retryable=?,
                         failure_correlation_id=?,finished_at=? WHERE test_id=?",
                )
                .bind(failure.code)
                .bind(failure.stage)
                .bind(failure.retryable)
                .bind(failure.correlation_id)
                .bind(now)
                .bind(&command.test_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage)?;
            }
            CompleteProviderConnectionTestResult::Cancelled => {
                sqlx::query(
                    "UPDATE provider_connection_tests
                     SET operation_status='cancelled',cancel_requested=1,claimed_by=NULL,
                         claim_token=NULL,claim_expires_at=NULL,finished_at=? WHERE test_id=?",
                )
                .bind(now)
                .bind(&command.test_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage)?;
            }
        }
        transaction.commit().await.map_err(storage)?;
        Ok(())
    }

    async fn create_provider_validation(
        &self,
        command: CreateProviderValidationCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT p.operational_state,d.draft_version,d.provider_input_hash
             FROM managed_providers p JOIN provider_drafts d USING(provider_id)
             WHERE p.provider_id=?",
        )
        .bind(&command.report.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            == "retired"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let draft_version = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        let input_hash: String = row.try_get("provider_input_hash").map_err(storage)?;
        if draft_version != command.expected_draft_version
            || draft_version != command.report.draft_version
            || input_hash != command.expected_provider_input_hash
            || input_hash != command.report.provider_input_hash
            || canonical_hash(&command.report.document)? != command.report.report_hash
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        sqlx::query(
            "INSERT INTO provider_validation_reports(
               validation_id,provider_id,draft_version,provider_input_hash,report_hash,valid,
               document,created_at,created_by
             ) VALUES(?,?,?,?,?,?,?,?,?)",
        )
        .bind(&command.report.validation_id)
        .bind(&command.report.provider_id)
        .bind(u64_to_i64(command.report.draft_version)?)
        .bind(&command.report.provider_input_hash)
        .bind(&command.report.report_hash)
        .bind(command.report.valid)
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
                event_kind: "provider.validation.created",
                provider_id: Some(&command.report.provider_id),
                subject_id: Some(&command.report.validation_id),
                before_hash: None,
                after_hash: Some(&command.report.report_hash),
                result_code: if command.report.valid {
                    "valid"
                } else {
                    "invalid"
                },
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_provider_validation(
        &self,
        provider_id: &str,
        validation_id: &str,
    ) -> Result<Option<ProviderValidationReport>, RepositoryError> {
        sqlx::query(
            "SELECT validation_id,provider_id,draft_version,provider_input_hash,report_hash,
                    valid,document,created_at,created_by
             FROM provider_validation_reports WHERE provider_id=? AND validation_id=?",
        )
        .bind(provider_id)
        .bind(validation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_validation)
        .transpose()
    }

    async fn publish_provider_revision(
        &self,
        command: PublishProviderRevisionCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let provider = sqlx::query(
            "SELECT p.operational_state,d.draft_version,d.provider_input_hash
             FROM managed_providers p JOIN provider_drafts d USING(provider_id)
             WHERE p.provider_id=?",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        let state: String = provider.try_get("operational_state").map_err(storage)?;
        let draft_version = i64_to_u64(provider.try_get("draft_version").map_err(storage)?)?;
        let input_hash: String = provider.try_get("provider_input_hash").map_err(storage)?;
        if state == "retired" {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        if draft_version != command.expected_draft_version
            || canonical_hash(&command.document)? != command.revision_hash
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let validation = sqlx::query(
            "SELECT draft_version,provider_input_hash,valid FROM provider_validation_reports
             WHERE provider_id=? AND validation_id=?",
        )
        .bind(&command.provider_id)
        .bind(&command.validation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::ValidationFailed,
        ))?;
        if validation
            .try_get::<i64, _>("draft_version")
            .map_err(storage)?
            != u64_to_i64(draft_version)?
            || validation
                .try_get::<String, _>("provider_input_hash")
                .map_err(storage)?
                != input_hash
            || validation.try_get::<i64, _>("valid").map_err(storage)? == 0
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ValidationFailed,
            ));
        }
        if let Some(discovery_id) = &command.discovery_id {
            let valid: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM provider_discovery_operations o
                 JOIN provider_discovery_snapshots s USING(discovery_id)
                 WHERE o.provider_id=? AND o.discovery_id=? AND o.operation_status='succeeded'
                   AND o.stale=0 AND o.source_draft_version=? AND o.provider_input_hash=?",
            )
            .bind(&command.provider_id)
            .bind(discovery_id)
            .bind(u64_to_i64(draft_version)?)
            .bind(&input_hash)
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage)?;
            if valid != 1 {
                return Err(ProviderManagementWriteError::Conflict(
                    ProviderManagementConflict::OperationStale,
                ));
            }
        }
        if let Some(test_id) = &command.connection_test_id {
            let valid: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM provider_connection_tests
                 WHERE provider_id=? AND test_id=? AND operation_status='succeeded'
                   AND source_draft_version=? AND provider_input_hash=?",
            )
            .bind(&command.provider_id)
            .bind(test_id)
            .bind(u64_to_i64(draft_version)?)
            .bind(&input_hash)
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage)?;
            if valid != 1 {
                return Err(ProviderManagementWriteError::Conflict(
                    ProviderManagementConflict::OperationStale,
                ));
            }
        }
        let models = model_documents(&command.document).map_err(|_| {
            ProviderManagementWriteError::Conflict(ProviderManagementConflict::CandidateMismatch)
        })?;
        let revision_number: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision_number),0)+1 FROM provider_revisions WHERE provider_id=?",
        )
        .bind(&command.provider_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO provider_revisions(
               revision_id,provider_id,revision_number,source_draft_version,validation_id,
               discovery_id,connection_test_id,revision_hash,document,created_at,created_by
             ) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&command.revision_id)
        .bind(&command.provider_id)
        .bind(revision_number)
        .bind(u64_to_i64(draft_version)?)
        .bind(&command.validation_id)
        .bind(&command.discovery_id)
        .bind(&command.connection_test_id)
        .bind(&command.revision_hash)
        .bind(encode_json(&command.document)?)
        .bind(now)
        .bind(&command.metadata.operator_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        for (ordinal, (model_id, capability_hash, document)) in models.iter().enumerate() {
            sqlx::query(
                "INSERT INTO provider_revision_models(revision_id,ordinal,model_id,capability_hash,document)
                 VALUES(?,?,?,?,?)",
            )
            .bind(&command.revision_id).bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
            .bind(model_id).bind(capability_hash).bind(encode_json(document)?)
            .execute(&mut *transaction).await.map_err(storage)?;
        }
        let response = json!({"revision_id":command.revision_id,"provider_id":command.provider_id,"revision_number":revision_number,"revision_hash":command.revision_hash});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.revision.published",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.revision_id),
                before_hash: Some(&input_hash),
                after_hash: Some(&command.revision_hash),
                result_code: "published",
                status: 201,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_provider_revision(
        &self,
        provider_id: &str,
        revision_id: &str,
    ) -> Result<Option<ProviderRevision>, RepositoryError> {
        sqlx::query(
            "SELECT revision_id,provider_id,revision_number,source_draft_version,validation_id,
                    discovery_id,connection_test_id,revision_hash,document,created_at,created_by
             FROM provider_revisions WHERE provider_id=? AND revision_id=?",
        )
        .bind(provider_id)
        .bind(revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(sqlite_revision)
        .transpose()
    }

    async fn list_provider_revisions(
        &self,
        provider_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ProviderManagementPage<ProviderRevision>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT revision_id,provider_id,revision_number,source_draft_version,validation_id,
                    discovery_id,connection_test_id,revision_hash,document,created_at,created_by
             FROM provider_revisions WHERE provider_id=?
               AND (? IS NULL OR created_at>? OR (created_at=? AND revision_id>?))
             ORDER BY created_at,revision_id LIMIT ?",
        )
        .bind(provider_id)
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
            .map(sqlite_revision)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.revision_id))
        } else {
            None
        };
        Ok(ProviderManagementPage { items, next_cursor })
    }

    async fn activate_provider_revision(
        &self,
        command: ActivateProviderRevisionCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT operational_state,provider_version,active_revision_id
             FROM managed_providers WHERE provider_id=?",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        let state: String = row.try_get("operational_state").map_err(storage)?;
        let actual = i64_to_u64(row.try_get("provider_version").map_err(storage)?)?;
        if actual != command.expected_provider_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        if state != "enabled" {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let revision_hash: String = sqlx::query_scalar(
            "SELECT revision_hash FROM provider_revisions WHERE provider_id=? AND revision_id=?",
        )
        .bind(&command.provider_id)
        .bind(&command.revision_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        let old: Option<String> = row.try_get("active_revision_id").map_err(storage)?;
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query(
            "UPDATE managed_providers SET active_revision_id=?,provider_version=?,updated_at=?
             WHERE provider_id=? AND provider_version=?",
        )
        .bind(&command.revision_id)
        .bind(u64_to_i64(next)?)
        .bind(database_time(command.metadata.now))
        .bind(&command.provider_id)
        .bind(u64_to_i64(actual)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"active_revision_id":command.revision_id,"provider_version":next});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.revision.activated",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.revision_id),
                before_hash: old.as_deref(),
                after_hash: Some(&revision_hash),
                result_code: "activated",
                status: 200,
                response,
                etag: Some(provider_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn deactivate_provider_revision(
        &self,
        command: DeactivateProviderRevisionCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT operational_state,provider_version,active_revision_id
             FROM managed_providers WHERE provider_id=?",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        let actual = i64_to_u64(row.try_get("provider_version").map_err(storage)?)?;
        let active: Option<String> = row.try_get("active_revision_id").map_err(storage)?;
        if actual != command.expected_provider_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            != "enabled"
            || active.is_none()
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query(
            "UPDATE managed_providers SET active_revision_id=NULL,provider_version=?,updated_at=?
             WHERE provider_id=? AND provider_version=?",
        )
        .bind(u64_to_i64(next)?)
        .bind(database_time(command.metadata.now))
        .bind(&command.provider_id)
        .bind(u64_to_i64(actual)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"active_revision_id":null,"provider_version":next});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.revision.deactivated",
                provider_id: Some(&command.provider_id),
                subject_id: active.as_deref(),
                before_hash: active.as_deref(),
                after_hash: None,
                result_code: "deactivated",
                status: 200,
                response,
                etag: Some(provider_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn suspend_provider(
        &self,
        command: SuspendProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT operational_state,provider_version,suspension_fence FROM managed_providers WHERE provider_id=?",
        )
        .bind(&command.provider_id).fetch_optional(&mut *transaction).await.map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(ProviderManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("provider_version").map_err(storage)?)?;
        let fence = i64_to_u64(row.try_get("suspension_fence").map_err(storage)?)?;
        if actual != command.expected_provider_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            != "enabled"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        let next_fence = fence.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query(
            "UPDATE managed_providers SET operational_state='suspended',provider_version=?,
                    suspension_fence=?,updated_at=? WHERE provider_id=? AND provider_version=?",
        )
        .bind(u64_to_i64(next)?)
        .bind(u64_to_i64(next_fence)?)
        .bind(database_time(command.metadata.now))
        .bind(&command.provider_id)
        .bind(u64_to_i64(actual)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"operational_state":"suspended","provider_version":next,"suspension_fence":next_fence,"reason_code":command.reason_code});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.suspended",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.provider_id),
                before_hash: None,
                after_hash: None,
                result_code: "suspended",
                status: 200,
                response,
                etag: Some(provider_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn resume_provider(
        &self,
        command: ResumeProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT operational_state,provider_version FROM managed_providers WHERE provider_id=?",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        let actual = i64_to_u64(row.try_get("provider_version").map_err(storage)?)?;
        if actual != command.expected_provider_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            != "suspended"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query("UPDATE managed_providers SET operational_state='enabled',provider_version=?,updated_at=? WHERE provider_id=? AND provider_version=?")
            .bind(u64_to_i64(next)?).bind(database_time(command.metadata.now)).bind(&command.provider_id).bind(u64_to_i64(actual)?)
            .execute(&mut *transaction).await.map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"operational_state":"enabled","provider_version":next});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.resumed",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.provider_id),
                before_hash: None,
                after_hash: None,
                result_code: "resumed",
                status: 200,
                response,
                etag: Some(provider_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn retire_provider(
        &self,
        command: RetireProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = sqlite_begin(self).await?;
        if let Some(receipt) = sqlite_replay(&mut transaction, &command.metadata).await? {
            transaction.commit().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT operational_state,provider_version,suspension_fence FROM managed_providers WHERE provider_id=?")
            .bind(&command.provider_id).fetch_optional(&mut *transaction).await.map_err(storage)?
            .ok_or(ProviderManagementWriteError::Conflict(ProviderManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("provider_version").map_err(storage)?)?;
        let fence = i64_to_u64(row.try_get("suspension_fence").map_err(storage)?)?;
        if actual != command.expected_provider_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            == "retired"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        let next_fence = fence.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query("UPDATE managed_providers SET operational_state='retired',active_revision_id=NULL,provider_version=?,suspension_fence=?,updated_at=? WHERE provider_id=? AND provider_version=?")
            .bind(u64_to_i64(next)?).bind(u64_to_i64(next_fence)?).bind(database_time(command.metadata.now))
            .bind(&command.provider_id).bind(u64_to_i64(actual)?).execute(&mut *transaction).await.map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"operational_state":"retired","provider_version":next,"suspension_fence":next_fence,"reason_code":command.reason_code});
        let receipt = sqlite_finalize(
            &mut transaction,
            &command.metadata,
            SqliteFinalize {
                event_kind: "provider.retired",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.provider_id),
                before_hash: None,
                after_hash: None,
                result_code: "retired",
                status: 200,
                response,
                etag: Some(provider_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn load_active_provider_revisions(
        &self,
    ) -> Result<Vec<(ManagedProvider, ProviderRevision)>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT p.provider_id,p.display_name,p.adapter_type,p.operational_state,p.provider_version,
                    p.draft_version,p.active_revision_id,p.suspension_fence,p.created_at,p.updated_at,
                    r.revision_id,r.provider_id AS revision_provider_id,r.revision_number,
                    r.source_draft_version,r.validation_id,r.discovery_id,r.connection_test_id,
                    r.revision_hash,r.document,r.created_at AS revision_created_at,r.created_by
             FROM managed_providers p JOIN provider_revisions r ON r.revision_id=p.active_revision_id
             WHERE p.operational_state='enabled' ORDER BY p.provider_id",
        )
        .fetch_all(&self.pool).await.map_err(RepositoryError::storage)?;
        rows.iter()
            .map(|row| {
                let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
                let provider = sqlite_provider(row)?;
                let revision = ProviderRevision {
                    revision_id: row
                        .try_get("revision_id")
                        .map_err(RepositoryError::storage)?,
                    provider_id: row
                        .try_get("revision_provider_id")
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
                    discovery_id: row
                        .try_get("discovery_id")
                        .map_err(RepositoryError::storage)?,
                    connection_test_id: row
                        .try_get("connection_test_id")
                        .map_err(RepositoryError::storage)?,
                    revision_hash: row
                        .try_get("revision_hash")
                        .map_err(RepositoryError::storage)?,
                    document: decode_json(&document)?,
                    created_at: row
                        .try_get("revision_created_at")
                        .map_err(RepositoryError::storage)?,
                    created_by: row
                        .try_get("created_by")
                        .map_err(RepositoryError::storage)?,
                };
                Ok((provider, revision))
            })
            .collect()
    }

    async fn load_provider_revision_archive(
        &self,
    ) -> Result<Vec<(ManagedProvider, ProviderRevision)>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT p.provider_id,p.display_name,p.adapter_type,p.operational_state,p.provider_version,
                    p.draft_version,p.active_revision_id,p.suspension_fence,p.created_at,p.updated_at,
                    r.revision_id,r.provider_id AS revision_provider_id,r.revision_number,
                    r.source_draft_version,r.validation_id,r.discovery_id,r.connection_test_id,
                    r.revision_hash,r.document,r.created_at AS revision_created_at,r.created_by
             FROM managed_providers p JOIN provider_revisions r ON r.provider_id=p.provider_id
             ORDER BY p.provider_id,r.revision_number",
        ).fetch_all(&self.pool).await.map_err(RepositoryError::storage)?;
        rows.iter()
            .map(|row| {
                let document: String = row.try_get("document").map_err(RepositoryError::storage)?;
                Ok((
                    sqlite_provider(row)?,
                    ProviderRevision {
                        revision_id: row
                            .try_get("revision_id")
                            .map_err(RepositoryError::storage)?,
                        provider_id: row
                            .try_get("revision_provider_id")
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
                        discovery_id: row
                            .try_get("discovery_id")
                            .map_err(RepositoryError::storage)?,
                        connection_test_id: row
                            .try_get("connection_test_id")
                            .map_err(RepositoryError::storage)?,
                        revision_hash: row
                            .try_get("revision_hash")
                            .map_err(RepositoryError::storage)?,
                        document: decode_json(&document)?,
                        created_at: row
                            .try_get("revision_created_at")
                            .map_err(RepositoryError::storage)?,
                        created_by: row
                            .try_get("created_by")
                            .map_err(RepositoryError::storage)?,
                    },
                ))
            })
            .collect()
    }

    async fn load_provider_legacy_model_bindings(
        &self,
        revision_id: &str,
    ) -> Result<Vec<ProviderLegacyModelBinding>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT revision_id,provider_id,model_id,legacy_binding_hash,
                    legacy_binding_evidence,source_definition_id,source_deployment_revision_id,created_at
             FROM provider_revision_legacy_model_bindings
             WHERE revision_id=? ORDER BY model_id,legacy_binding_hash",
        )
        .bind(revision_id)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.iter()
            .map(|row| {
                Ok(ProviderLegacyModelBinding {
                    revision_id: row
                        .try_get("revision_id")
                        .map_err(RepositoryError::storage)?,
                    provider_id: row
                        .try_get("provider_id")
                        .map_err(RepositoryError::storage)?,
                    model_id: row.try_get("model_id").map_err(RepositoryError::storage)?,
                    legacy_binding_hash: row
                        .try_get("legacy_binding_hash")
                        .map_err(RepositoryError::storage)?,
                    legacy_binding_evidence: decode_json(
                        &row.try_get::<String, _>("legacy_binding_evidence")
                            .map_err(RepositoryError::storage)?,
                    )?,
                    source_definition_id: row
                        .try_get("source_definition_id")
                        .map_err(RepositoryError::storage)?,
                    source_deployment_revision_id: row
                        .try_get("source_deployment_revision_id")
                        .map_err(RepositoryError::storage)?,
                    created_at: row
                        .try_get("created_at")
                        .map_err(RepositoryError::storage)?,
                })
            })
            .collect()
    }

    async fn load_provider_fence(
        &self,
        provider_id: &str,
    ) -> Result<Option<ProviderFence>, RepositoryError> {
        let row = sqlx::query("SELECT provider_id,operational_state,active_revision_id,suspension_fence FROM managed_providers WHERE provider_id=?")
            .bind(provider_id).fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?;
        row.as_ref()
            .map(|row| {
                Ok(ProviderFence {
                    provider_id: row
                        .try_get("provider_id")
                        .map_err(RepositoryError::storage)?,
                    operational_state: operational_state(
                        &row.try_get::<String, _>("operational_state")
                            .map_err(RepositoryError::storage)?,
                    )?,
                    active_revision_id: row
                        .try_get("active_revision_id")
                        .map_err(RepositoryError::storage)?,
                    suspension_fence: i64_to_u64(
                        row.try_get("suspension_fence")
                            .map_err(RepositoryError::storage)?,
                    )?,
                })
            })
            .transpose()
    }

    async fn load_provider_management_runtime_stats(
        &self,
    ) -> Result<ProviderManagementRuntimeStats, RepositoryError> {
        let row = sqlx::query(
            "SELECT
               (SELECT COUNT(*) FROM provider_discovery_operations WHERE operation_status='pending') pending_discoveries,
               (SELECT COUNT(*) FROM provider_discovery_operations WHERE operation_status='running') running_discoveries,
               (SELECT COUNT(*) FROM provider_connection_tests WHERE operation_status='pending') pending_connection_tests,
               (SELECT COUNT(*) FROM provider_connection_tests WHERE operation_status='running') running_connection_tests,
               (SELECT COUNT(*) FROM managed_providers WHERE operational_state='enabled' AND active_revision_id IS NOT NULL) active_providers,
               (SELECT COUNT(*) FROM managed_providers WHERE operational_state='suspended') suspended_providers,
               (SELECT COUNT(*) FROM managed_providers WHERE operational_state='enabled') enabled_providers,
               (SELECT COUNT(*) FROM managed_providers WHERE operational_state='retired') retired_providers",
        ).fetch_one(&self.pool).await.map_err(RepositoryError::storage)?;
        let connection_tests = sqlx::query(
            "SELECT test_mode,operation_status,COUNT(*) count FROM provider_connection_tests
             GROUP BY test_mode,operation_status ORDER BY test_mode,operation_status",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .iter()
        .map(|row| {
            Ok(ProviderConnectionTestRuntimeCount {
                mode: test_mode(
                    &row.try_get::<String, _>("test_mode")
                        .map_err(RepositoryError::storage)?,
                )?,
                outcome: operation_status(
                    &row.try_get::<String, _>("operation_status")
                        .map_err(RepositoryError::storage)?,
                )?,
                count: i64_to_u64(row.try_get("count").map_err(RepositoryError::storage)?)?,
            })
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
        let operations = sqlx::query(
            "SELECT event_kind,result_code,COUNT(*) count FROM provider_management_audit_events
             GROUP BY event_kind,result_code ORDER BY event_kind,result_code",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .iter()
        .map(|row| {
            Ok(ProviderManagementOperationCount {
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
        Ok(ProviderManagementRuntimeStats {
            pending_discoveries: i64_to_u64(
                row.try_get("pending_discoveries")
                    .map_err(RepositoryError::storage)?,
            )?,
            running_discoveries: i64_to_u64(
                row.try_get("running_discoveries")
                    .map_err(RepositoryError::storage)?,
            )?,
            pending_connection_tests: i64_to_u64(
                row.try_get("pending_connection_tests")
                    .map_err(RepositoryError::storage)?,
            )?,
            running_connection_tests: i64_to_u64(
                row.try_get("running_connection_tests")
                    .map_err(RepositoryError::storage)?,
            )?,
            active_providers: i64_to_u64(
                row.try_get("active_providers")
                    .map_err(RepositoryError::storage)?,
            )?,
            suspended_providers: i64_to_u64(
                row.try_get("suspended_providers")
                    .map_err(RepositoryError::storage)?,
            )?,
            enabled_providers: i64_to_u64(
                row.try_get("enabled_providers")
                    .map_err(RepositoryError::storage)?,
            )?,
            retired_providers: i64_to_u64(
                row.try_get("retired_providers")
                    .map_err(RepositoryError::storage)?,
            )?,
            connection_tests,
            operations,
        })
    }

    async fn cleanup_terminal_provider_operations(
        &self,
        finished_before: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError> {
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(RepositoryError::storage)?;
        let discoveries = sqlx::query(
            "DELETE FROM provider_discovery_operations WHERE discovery_id IN(
               SELECT o.discovery_id FROM provider_discovery_operations o
               LEFT JOIN provider_discovery_snapshots s USING(discovery_id)
               LEFT JOIN provider_revisions r ON r.discovery_id=s.discovery_id
               WHERE o.operation_status IN('failed','cancelled','succeeded') AND o.finished_at<?
                 AND r.revision_id IS NULL ORDER BY o.finished_at,o.discovery_id LIMIT ?
             )",
        )
        .bind(database_time(finished_before))
        .bind(i64::from(limit))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        let remaining = u64::from(limit).saturating_sub(discoveries);
        let tests = if remaining == 0 {
            0
        } else {
            sqlx::query(
                "DELETE FROM provider_connection_tests WHERE test_id IN(
                   SELECT t.test_id FROM provider_connection_tests t
                   LEFT JOIN provider_revisions r ON r.connection_test_id=t.test_id
                   WHERE t.operation_status IN('failed','cancelled','succeeded') AND t.finished_at<?
                     AND r.revision_id IS NULL ORDER BY t.finished_at,t.test_id LIMIT ?
                 )",
            )
            .bind(database_time(finished_before))
            .bind(u64_to_i64(remaining)?)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected()
        };
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(discoveries + tests)
    }
}

fn postgres_provider(row: &sqlx::postgres::PgRow) -> Result<ManagedProvider, RepositoryError> {
    Ok(ManagedProvider {
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        display_name: row
            .try_get("display_name")
            .map_err(RepositoryError::storage)?,
        adapter_type: row
            .try_get("adapter_type")
            .map_err(RepositoryError::storage)?,
        operational_state: operational_state(
            &row.try_get::<String, _>("operational_state")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_version: i64_to_u64(
            row.try_get("provider_version")
                .map_err(RepositoryError::storage)?,
        )?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        active_revision_id: row
            .try_get("active_revision_id")
            .map_err(RepositoryError::storage)?,
        suspension_fence: i64_to_u64(
            row.try_get("suspension_fence")
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

fn postgres_draft(row: &sqlx::postgres::PgRow) -> Result<ProviderStoredDraft, RepositoryError> {
    Ok(ProviderStoredDraft {
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_input_hash: row
            .try_get("provider_input_hash")
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

fn postgres_failure(
    row: &sqlx::postgres::PgRow,
) -> Result<Option<ProviderOperationFailure>, RepositoryError> {
    let code: Option<String> = row
        .try_get("failure_code")
        .map_err(RepositoryError::storage)?;
    code.map(|code| {
        Ok(ProviderOperationFailure {
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
        })
    })
    .transpose()
}

fn postgres_discovery(
    row: &sqlx::postgres::PgRow,
) -> Result<ProviderDiscoveryOperation, RepositoryError> {
    Ok(ProviderDiscoveryOperation {
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_input_hash: row
            .try_get("provider_input_hash")
            .map_err(RepositoryError::storage)?,
        status: operation_status(
            &row.try_get::<String, _>("operation_status")
                .map_err(RepositoryError::storage)?,
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
        failure: postgres_failure(row)?,
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

fn postgres_snapshot(
    row: &sqlx::postgres::PgRow,
) -> Result<ProviderDiscoverySnapshot, RepositoryError> {
    Ok(ProviderDiscoverySnapshot {
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_input_hash: row
            .try_get("provider_input_hash")
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

fn postgres_test(row: &sqlx::postgres::PgRow) -> Result<ProviderConnectionTest, RepositoryError> {
    Ok(ProviderConnectionTest {
        test_id: row.try_get("test_id").map_err(RepositoryError::storage)?,
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        source_draft_version: i64_to_u64(
            row.try_get("source_draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_input_hash: row
            .try_get("provider_input_hash")
            .map_err(RepositoryError::storage)?,
        mode: test_mode(
            &row.try_get::<String, _>("test_mode")
                .map_err(RepositoryError::storage)?,
        )?,
        status: operation_status(
            &row.try_get::<String, _>("operation_status")
                .map_err(RepositoryError::storage)?,
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
        failure: postgres_failure(row)?,
        result_hash: row
            .try_get("result_hash")
            .map_err(RepositoryError::storage)?,
        result: row
            .try_get("result_document")
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

fn postgres_validation(
    row: &sqlx::postgres::PgRow,
) -> Result<ProviderValidationReport, RepositoryError> {
    Ok(ProviderValidationReport {
        validation_id: row
            .try_get("validation_id")
            .map_err(RepositoryError::storage)?,
        provider_id: row
            .try_get("provider_id")
            .map_err(RepositoryError::storage)?,
        draft_version: i64_to_u64(
            row.try_get("draft_version")
                .map_err(RepositoryError::storage)?,
        )?,
        provider_input_hash: row
            .try_get("provider_input_hash")
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

fn postgres_revision(row: &sqlx::postgres::PgRow) -> Result<ProviderRevision, RepositoryError> {
    Ok(ProviderRevision {
        revision_id: row
            .try_get("revision_id")
            .map_err(RepositoryError::storage)?,
        provider_id: row
            .try_get("provider_id")
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
        discovery_id: row
            .try_get("discovery_id")
            .map_err(RepositoryError::storage)?,
        connection_test_id: row
            .try_get("connection_test_id")
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
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    metadata: &ProviderMutationMetadata,
) -> Result<Option<ProviderMutationReceipt>, ProviderManagementWriteError> {
    let row = sqlx::query(
        "SELECT request_hash,response_status,response_json,response_etag
         FROM provider_management_requests
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
        return Err(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::IdempotencyKeyReused,
        ));
    }
    Ok(Some(ProviderMutationReceipt {
        replayed: true,
        status: u16::try_from(row.try_get::<i32, _>("response_status").map_err(storage)?)
            .map_err(|_| ProviderManagementWriteError::Repository(invalid_data()))?,
        response: row.try_get("response_json").map_err(storage)?,
        etag: row.try_get("response_etag").map_err(storage)?,
    }))
}

struct PostgresFinalize<'a> {
    event_kind: &'a str,
    provider_id: Option<&'a str>,
    subject_id: Option<&'a str>,
    before_hash: Option<&'a str>,
    after_hash: Option<&'a str>,
    result_code: &'a str,
    status: u16,
    response: Value,
    etag: Option<String>,
}

async fn postgres_finalize(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    metadata: &ProviderMutationMetadata,
    finalization: PostgresFinalize<'_>,
) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
    sqlx::query(
        "INSERT INTO provider_management_requests(
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
        "INSERT INTO provider_management_audit_events(
           event_kind,provider_id,subject_id,actor_id,capability,request_id_hash,before_hash,
           after_hash,result_code,created_at
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
    )
    .bind(finalization.event_kind)
    .bind(finalization.provider_id)
    .bind(finalization.subject_id)
    .bind(&metadata.operator_id)
    .bind(&metadata.capability)
    .bind(prefixed_sha256(metadata.request_id.as_bytes()))
    .bind(finalization.before_hash)
    .bind(finalization.after_hash)
    .bind(finalization.result_code)
    .bind(database_time(metadata.now))
    .execute(&mut **transaction)
    .await
    .map_err(storage)?;
    if let (Some(provider_id), Some(subject_id)) =
        (finalization.provider_id, finalization.subject_id)
    {
        sqlx::query(
            "SELECT pg_advisory_xact_lock(
               hashtextextended(current_schema() || ':provider_management_outbox',0)
             )",
        )
        .execute(&mut **transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO provider_management_outbox(
               event_id,event_kind,provider_id,subject_id,safe_payload,created_at,delivered_at
             ) VALUES($1,$2,$3,$4,$5,$6,NULL)",
        )
        .bind(format!("pout_{}", Uuid::new_v4().simple())).bind(finalization.event_kind)
        .bind(provider_id).bind(subject_id)
        .bind(json!({"provider_id":provider_id,"subject_id":subject_id,"result_code":finalization.result_code}))
        .bind(database_time(metadata.now)).execute(&mut **transaction).await.map_err(storage)?;
        sqlx::query(
            "DELETE FROM provider_management_outbox WHERE event_id IN (
               SELECT event_id FROM provider_management_outbox
               ORDER BY created_at DESC,event_id DESC OFFSET $1
             )",
        )
        .bind(MANAGEMENT_OUTBOX_MAX_ROWS)
        .execute(&mut **transaction)
        .await
        .map_err(storage)?;
    }
    Ok(ProviderMutationReceipt {
        replayed: false,
        status: finalization.status,
        response: finalization.response,
        etag: finalization.etag,
    })
}

#[async_trait]
impl ProviderManagementDurableRepository for PostgresDurableRepository {
    async fn open_provider_management_notification_stream(
        &self,
    ) -> Result<Option<Box<dyn ProviderManagementNotificationStream>>, RepositoryError> {
        let schema_oid = sqlx::query_scalar::<_, i64>(
            "SELECT oid::bigint FROM pg_catalog.pg_namespace WHERE nspname=current_schema()",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .ok_or_else(RepositoryError::schema_not_initialized)?;
        let channel = format!("{PROVIDER_MANAGEMENT_NOTIFY_CHANNEL_PREFIX}{schema_oid}");
        let mut listener = PgListener::connect_with(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        listener.eager_reconnect(false);
        listener
            .listen(&channel)
            .await
            .map_err(RepositoryError::storage)?;
        Ok(Some(Box::new(
            PostgresProviderManagementNotificationStream { listener },
        )))
    }

    async fn record_provider_management_rejection(
        &self,
        command: RecordProviderManagementRejectionCommand,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            "INSERT INTO provider_management_audit_events(
               event_kind,provider_id,subject_id,actor_id,capability,request_id_hash,before_hash,
               after_hash,result_code,created_at)
             VALUES('provider.request_rejected',$1,$2,$3,$4,$5,NULL,NULL,$6,$7)",
        )
        .bind(command.provider_id)
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

    async fn replay_provider_mutation(
        &self,
        metadata: &ProviderMutationMetadata,
    ) -> Result<Option<ProviderMutationReceipt>, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        let result = postgres_replay(&mut transaction, metadata).await?;
        transaction.rollback().await.map_err(storage)?;
        Ok(result)
    }

    async fn create_provider(
        &self,
        command: CreateProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        if sqlx::query("SELECT 1 FROM managed_providers WHERE provider_id=$1 FOR UPDATE")
            .bind(&command.provider_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage)?
            .is_some()
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::Referenced,
            ));
        }
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO managed_providers(
               provider_id,display_name,adapter_type,operational_state,provider_version,
               draft_version,active_revision_id,suspension_fence,created_at,updated_at
             ) VALUES($1,$2,$3,'enabled',1,1,NULL,0,$4,$4)",
        )
        .bind(&command.provider_id)
        .bind(&command.display_name)
        .bind(&command.adapter_type)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "INSERT INTO provider_drafts(
               provider_id,draft_version,provider_input_hash,document,created_at,updated_at
             ) VALUES($1,1,$2,$3,$4,$4)",
        )
        .bind(&command.provider_id)
        .bind(&command.provider_input_hash)
        .bind(&command.draft_document)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let provider = ManagedProvider {
            provider_id: command.provider_id.clone(),
            display_name: command.display_name,
            adapter_type: command.adapter_type,
            operational_state: ProviderOperationalState::Enabled,
            provider_version: 1,
            draft_version: 1,
            active_revision_id: None,
            suspension_fence: 0,
            created_at: now,
            updated_at: now,
        };
        let response = serde_json::to_value(&provider).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.created",
                provider_id: Some(&provider.provider_id),
                subject_id: Some(&provider.provider_id),
                before_hash: None,
                after_hash: Some(&command.provider_input_hash),
                result_code: "created",
                status: 201,
                response,
                etag: Some(provider_etag(1)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn replace_provider_draft(
        &self,
        command: ReplaceProviderDraftCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT p.operational_state,d.draft_version,d.provider_input_hash,d.created_at
             FROM managed_providers p JOIN provider_drafts d USING(provider_id)
             WHERE p.provider_id=$1 FOR UPDATE OF p,d",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            == "retired"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        if actual != command.expected_draft_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let before_hash: String = row.try_get("provider_input_hash").map_err(storage)?;
        let created_at = row.try_get("created_at").map_err(storage)?;
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "UPDATE provider_drafts SET draft_version=$1,provider_input_hash=$2,document=$3,updated_at=$4
             WHERE provider_id=$5 AND draft_version=$6",
        )
        .bind(u64_to_i64(next)?).bind(&command.provider_input_hash).bind(&command.draft_document)
        .bind(now).bind(&command.provider_id).bind(u64_to_i64(actual)?)
        .execute(&mut *transaction).await.map_err(storage)?;
        sqlx::query(
            "UPDATE managed_providers SET draft_version=$1,updated_at=$2 WHERE provider_id=$3",
        )
        .bind(u64_to_i64(next)?)
        .bind(now)
        .bind(&command.provider_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "UPDATE provider_discovery_operations SET stale=TRUE,stale_reason='draft_changed'
             WHERE provider_id=$1 AND operation_status='succeeded' AND NOT stale",
        )
        .bind(&command.provider_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let draft = ProviderStoredDraft {
            provider_id: command.provider_id.clone(),
            draft_version: next,
            provider_input_hash: command.provider_input_hash.clone(),
            document: command.draft_document,
            created_at,
            updated_at: now,
        };
        let response = serde_json::to_value(&draft).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.draft.replaced",
                provider_id: Some(&draft.provider_id),
                subject_id: Some(&draft.provider_id),
                before_hash: Some(&before_hash),
                after_hash: Some(&draft.provider_input_hash),
                result_code: "updated",
                status: 200,
                response,
                etag: Some(draft_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn delete_provider(
        &self,
        command: DeleteProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT provider_version,active_revision_id FROM managed_providers WHERE provider_id=$1 FOR UPDATE")
            .bind(&command.provider_id).fetch_optional(&mut *transaction).await.map_err(storage)?
            .ok_or(ProviderManagementWriteError::Conflict(ProviderManagementConflict::NotFound))?;
        if i64_to_u64(row.try_get("provider_version").map_err(storage)?)?
            != command.expected_provider_version
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let active: Option<String> = row.try_get("active_revision_id").map_err(storage)?;
        let revisions: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_revisions WHERE provider_id=$1")
                .bind(&command.provider_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(storage)?;
        let open: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM provider_discovery_operations WHERE provider_id=$1 AND operation_status IN('pending','running'))
                    +(SELECT COUNT(*) FROM provider_connection_tests WHERE provider_id=$1 AND operation_status IN('pending','running'))",
        ).bind(&command.provider_id).fetch_one(&mut *transaction).await.map_err(storage)?;
        if active.is_some() || revisions != 0 || open != 0 {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::Referenced,
            ));
        }
        sqlx::query("DELETE FROM managed_providers WHERE provider_id=$1")
            .bind(&command.provider_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"deleted":true});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.deleted",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.provider_id),
                before_hash: None,
                after_hash: None,
                result_code: "deleted",
                status: 200,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_provider(
        &self,
        provider_id: &str,
    ) -> Result<Option<ManagedProvider>, RepositoryError> {
        sqlx::query(
            "SELECT provider_id,display_name,adapter_type,operational_state,provider_version,
                    draft_version,active_revision_id,suspension_fence,created_at,updated_at
             FROM managed_providers WHERE provider_id=$1",
        )
        .bind(provider_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_provider)
        .transpose()
    }

    async fn get_provider_draft(
        &self,
        provider_id: &str,
    ) -> Result<Option<ProviderStoredDraft>, RepositoryError> {
        sqlx::query("SELECT provider_id,draft_version,provider_input_hash,document,created_at,updated_at FROM provider_drafts WHERE provider_id=$1")
            .bind(provider_id).fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?
            .as_ref().map(postgres_draft).transpose()
    }

    async fn list_providers(
        &self,
        state: Option<ProviderOperationalState>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ProviderManagementPage<ManagedProvider>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT provider_id,display_name,adapter_type,operational_state,provider_version,
                   draft_version,active_revision_id,suspension_fence,created_at,updated_at
            FROM managed_providers WHERE ($1::text IS NULL OR operational_state=$1)
              AND ($2::timestamptz IS NULL OR created_at>$2 OR
                   (created_at=$2 AND provider_id>$3))
            ORDER BY created_at,provider_id LIMIT $4",
        )
        .bind(state.map(ProviderOperationalState::as_str))
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(postgres_provider)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.provider_id))
        } else {
            None
        };
        Ok(ProviderManagementPage { items, next_cursor })
    }
    async fn create_provider_discovery(
        &self,
        command: CreateProviderDiscoveryCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT p.operational_state,d.draft_version,d.provider_input_hash,d.document
             FROM managed_providers p JOIN provider_drafts d USING(provider_id)
             WHERE p.provider_id=$1 FOR UPDATE OF p,d",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            == "retired"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        let input_hash: String = row.try_get("provider_input_hash").map_err(storage)?;
        if actual != command.expected_draft_version || input_hash != command.provider_input_hash {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_discovery_operations
             WHERE provider_id=$1 AND operation_status IN('pending','running')",
        )
        .bind(&command.provider_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage)?;
        if pending >= i64::from(command.max_pending_operations) {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::CapacityExceeded,
            ));
        }
        let document: Value = row.try_get("document").map_err(storage)?;
        sqlx::query(
            "INSERT INTO provider_discovery_operations(
               discovery_id,provider_id,source_draft_version,provider_input_hash,draft_document,
               operation_status,cancel_requested,attempts,stale,created_at
             ) VALUES($1,$2,$3,$4,$5,'pending',FALSE,0,FALSE,$6)",
        )
        .bind(&command.discovery_id)
        .bind(&command.provider_id)
        .bind(u64_to_i64(actual)?)
        .bind(&input_hash)
        .bind(document)
        .bind(database_time(command.metadata.now))
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        let response = json!({"discovery_id":command.discovery_id,"provider_id":command.provider_id,"status":"pending"});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.discovery.created",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.discovery_id),
                before_hash: None,
                after_hash: Some(&input_hash),
                result_code: "accepted",
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn cancel_provider_discovery(
        &self,
        command: CancelProviderOperationCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let status: String = sqlx::query_scalar(
            "SELECT operation_status FROM provider_discovery_operations
             WHERE provider_id=$1 AND discovery_id=$2 FOR UPDATE",
        )
        .bind(&command.provider_id)
        .bind(&command.operation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        if status == "pending" {
            sqlx::query("UPDATE provider_discovery_operations SET operation_status='cancelled',cancel_requested=TRUE,finished_at=$1 WHERE discovery_id=$2")
                .bind(database_time(command.metadata.now)).bind(&command.operation_id)
                .execute(&mut *transaction).await.map_err(storage)?;
        } else if status == "running" {
            sqlx::query("UPDATE provider_discovery_operations SET cancel_requested=TRUE WHERE discovery_id=$1")
                .bind(&command.operation_id).execute(&mut *transaction).await.map_err(storage)?;
        }
        let response = json!({"discovery_id":command.operation_id,"cancel_requested":true});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.discovery.cancel_requested",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.operation_id),
                before_hash: None,
                after_hash: None,
                result_code: "cancel_requested",
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_provider_discovery(
        &self,
        provider_id: &str,
        discovery_id: &str,
    ) -> Result<Option<ProviderDiscoveryOperation>, RepositoryError> {
        sqlx::query(
            "SELECT discovery_id,provider_id,source_draft_version,provider_input_hash,
                    operation_status,cancel_requested,attempts,claimed_by,claim_token,claim_expires_at,
                    failure_code,failure_stage,failure_retryable,failure_correlation_id,stale,
                    stale_reason,created_at,started_at,finished_at
             FROM provider_discovery_operations WHERE provider_id=$1 AND discovery_id=$2",
        ).bind(provider_id).bind(discovery_id).fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?
            .as_ref().map(postgres_discovery).transpose()
    }

    async fn get_provider_discovery_snapshot(
        &self,
        provider_id: &str,
        discovery_id: &str,
    ) -> Result<Option<ProviderDiscoverySnapshot>, RepositoryError> {
        sqlx::query(
            "SELECT discovery_id,provider_id,source_draft_version,provider_input_hash,
                    catalog_fingerprint,document,created_at
             FROM provider_discovery_snapshots WHERE provider_id=$1 AND discovery_id=$2",
        )
        .bind(provider_id)
        .bind(discovery_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_snapshot)
        .transpose()
    }

    async fn list_provider_model_candidates(
        &self,
        provider_id: &str,
        discovery_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ProviderManagementPage<ProviderModelCandidate>, RepositoryError> {
        let ordinal = cursor
            .map(str::parse::<i64>)
            .transpose()
            .map_err(|_| invalid_data())?;
        let rows = sqlx::query(
            "SELECT c.discovery_id,c.ordinal,c.model_id,c.candidate_fingerprint,c.document
             FROM provider_model_candidates c JOIN provider_discovery_snapshots s USING(discovery_id)
             WHERE s.provider_id=$1 AND c.discovery_id=$2 AND ($3::bigint IS NULL OR c.ordinal>$3)
             ORDER BY c.ordinal LIMIT $4",
        ).bind(provider_id).bind(discovery_id).bind(ordinal).bind(i64::from(limit.saturating_add(1)))
            .fetch_all(&self.pool).await.map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(|row| {
                Ok(ProviderModelCandidate {
                    discovery_id: row
                        .try_get("discovery_id")
                        .map_err(RepositoryError::storage)?,
                    ordinal: i64_to_u32(row.try_get("ordinal").map_err(RepositoryError::storage)?)?,
                    model_id: row.try_get("model_id").map_err(RepositoryError::storage)?,
                    candidate_fingerprint: row
                        .try_get("candidate_fingerprint")
                        .map_err(RepositoryError::storage)?,
                    document: row.try_get("document").map_err(RepositoryError::storage)?,
                })
            })
            .collect::<Result<Vec<_>, RepositoryError>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items.last().map(|item| item.ordinal.to_string())
        } else {
            None
        };
        Ok(ProviderManagementPage { items, next_cursor })
    }

    async fn claim_provider_discoveries(
        &self,
        command: ClaimProviderOperationsCommand,
    ) -> Result<Vec<ProviderDiscoveryClaim>, RepositoryError> {
        let rows = sqlx::query(
            "WITH candidates AS (
               SELECT discovery_id FROM provider_discovery_operations
               WHERE (operation_status='pending' OR (operation_status='running' AND claim_expires_at<=$1))
                 AND NOT cancel_requested ORDER BY created_at,discovery_id
               FOR UPDATE SKIP LOCKED LIMIT $2
             )
             UPDATE provider_discovery_operations o SET operation_status='running',attempts=o.attempts+1,
               claimed_by=$3,claim_token='pdc_'||replace(gen_random_uuid()::text,'-',''),claim_expires_at=$4,
               started_at=COALESCE(o.started_at,$1)
             FROM candidates c WHERE o.discovery_id=c.discovery_id
             RETURNING o.discovery_id,o.provider_id,o.source_draft_version,o.provider_input_hash,
               o.operation_status,o.cancel_requested,o.attempts,o.claimed_by,o.claim_token,o.claim_expires_at,
               o.failure_code,o.failure_stage,o.failure_retryable,o.failure_correlation_id,o.stale,
               o.stale_reason,o.created_at,o.started_at,o.finished_at,o.draft_document",
        ).bind(database_time(command.now)).bind(i64::from(command.limit)).bind(&command.worker_id)
            .bind(database_time(command.lease_expires_at)).fetch_all(&self.pool)
            .await.map_err(RepositoryError::storage)?;
        rows.iter()
            .map(|row| {
                Ok(ProviderDiscoveryClaim {
                    operation: postgres_discovery(row)?,
                    claim_token: row
                        .try_get::<String, _>("claim_token")
                        .map_err(RepositoryError::storage)?,
                    draft_document: row
                        .try_get("draft_document")
                        .map_err(RepositoryError::storage)?,
                })
            })
            .collect()
    }

    async fn complete_provider_discovery(
        &self,
        command: CompleteProviderDiscoveryCommand,
    ) -> Result<(), ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        let row = sqlx::query(
            "SELECT provider_id,source_draft_version,provider_input_hash,cancel_requested
             FROM provider_discovery_operations
             WHERE discovery_id=$1 AND operation_status='running' AND claim_token=$2 FOR UPDATE",
        )
        .bind(&command.discovery_id)
        .bind(&command.claim_token)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::FenceLost,
        ))?;
        let provider_id: String = row.try_get("provider_id").map_err(storage)?;
        let source_draft_version: i64 = row.try_get("source_draft_version").map_err(storage)?;
        let input_hash: String = row.try_get("provider_input_hash").map_err(storage)?;
        let result = if row
            .try_get::<bool, _>("cancel_requested")
            .map_err(storage)?
        {
            CompleteProviderDiscoveryResult::Cancelled
        } else {
            command.result
        };
        let now = database_time(command.now);
        match result {
            CompleteProviderDiscoveryResult::Succeeded {
                catalog_fingerprint,
                snapshot_document,
            } => {
                if canonical_hash(&snapshot_document)? != catalog_fingerprint {
                    return Err(ProviderManagementWriteError::Conflict(
                        ProviderManagementConflict::CandidateMismatch,
                    ));
                }
                let models = model_documents(&snapshot_document).map_err(|_| {
                    ProviderManagementWriteError::Conflict(
                        ProviderManagementConflict::CandidateMismatch,
                    )
                })?;
                sqlx::query(
                    "INSERT INTO provider_discovery_snapshots(
                       discovery_id,provider_id,source_draft_version,provider_input_hash,catalog_fingerprint,document,created_at
                     ) VALUES($1,$2,$3,$4,$5,$6,$7)",
                ).bind(&command.discovery_id).bind(&provider_id).bind(source_draft_version).bind(&input_hash)
                    .bind(&catalog_fingerprint).bind(&snapshot_document).bind(now)
                    .execute(&mut *transaction).await.map_err(storage)?;
                for (ordinal, (model_id, fingerprint, document)) in models.iter().enumerate() {
                    sqlx::query("INSERT INTO provider_model_candidates(discovery_id,ordinal,model_id,candidate_fingerprint,document) VALUES($1,$2,$3,$4,$5)")
                        .bind(&command.discovery_id).bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
                        .bind(model_id).bind(fingerprint).bind(document).execute(&mut *transaction).await.map_err(storage)?;
                }
                sqlx::query("UPDATE provider_discovery_operations SET operation_status='succeeded',claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,finished_at=$1 WHERE discovery_id=$2")
                    .bind(now).bind(&command.discovery_id).execute(&mut *transaction).await.map_err(storage)?;
            }
            CompleteProviderDiscoveryResult::Failed(failure) => {
                sqlx::query("UPDATE provider_discovery_operations SET operation_status='failed',claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,failure_code=$1,failure_stage=$2,failure_retryable=$3,failure_correlation_id=$4,finished_at=$5 WHERE discovery_id=$6")
                    .bind(failure.code).bind(failure.stage).bind(failure.retryable).bind(failure.correlation_id)
                    .bind(now).bind(&command.discovery_id).execute(&mut *transaction).await.map_err(storage)?;
            }
            CompleteProviderDiscoveryResult::Cancelled => {
                sqlx::query("UPDATE provider_discovery_operations SET operation_status='cancelled',cancel_requested=TRUE,claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,finished_at=$1 WHERE discovery_id=$2")
                    .bind(now).bind(&command.discovery_id).execute(&mut *transaction).await.map_err(storage)?;
            }
        }
        transaction.commit().await.map_err(storage)?;
        Ok(())
    }

    async fn create_provider_connection_test(
        &self,
        command: CreateProviderConnectionTestCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT p.operational_state,d.draft_version,d.provider_input_hash,d.document
             FROM managed_providers p JOIN provider_drafts d USING(provider_id)
             WHERE p.provider_id=$1 FOR UPDATE OF p,d",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            == "retired"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        let input_hash: String = row.try_get("provider_input_hash").map_err(storage)?;
        if actual != command.expected_draft_version || input_hash != command.provider_input_hash {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_connection_tests WHERE provider_id=$1 AND operation_status IN('pending','running')")
            .bind(&command.provider_id).fetch_one(&mut *transaction).await.map_err(storage)?;
        if pending >= i64::from(command.max_pending_operations) {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::CapacityExceeded,
            ));
        }
        let document: Value = row.try_get("document").map_err(storage)?;
        sqlx::query(
            "INSERT INTO provider_connection_tests(
               test_id,provider_id,source_draft_version,provider_input_hash,draft_document,test_mode,
               operation_status,cancel_requested,attempts,created_at
             ) VALUES($1,$2,$3,$4,$5,$6,'pending',FALSE,0,$7)",
        ).bind(&command.test_id).bind(&command.provider_id).bind(u64_to_i64(actual)?)
            .bind(&input_hash).bind(document).bind(command.mode.as_str()).bind(database_time(command.metadata.now))
            .execute(&mut *transaction).await.map_err(storage)?;
        let response =
            json!({"test_id":command.test_id,"provider_id":command.provider_id,"status":"pending"});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.connection_test.created",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.test_id),
                before_hash: None,
                after_hash: Some(&input_hash),
                result_code: "accepted",
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn cancel_provider_connection_test(
        &self,
        command: CancelProviderOperationCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let status: String = sqlx::query_scalar(
            "SELECT operation_status FROM provider_connection_tests WHERE provider_id=$1 AND test_id=$2 FOR UPDATE",
        ).bind(&command.provider_id).bind(&command.operation_id)
            .fetch_optional(&mut *transaction).await.map_err(storage)?
            .ok_or(ProviderManagementWriteError::Conflict(ProviderManagementConflict::NotFound))?;
        if status == "pending" {
            sqlx::query("UPDATE provider_connection_tests SET operation_status='cancelled',cancel_requested=TRUE,finished_at=$1 WHERE test_id=$2")
                .bind(database_time(command.metadata.now)).bind(&command.operation_id)
                .execute(&mut *transaction).await.map_err(storage)?;
        } else if status == "running" {
            sqlx::query(
                "UPDATE provider_connection_tests SET cancel_requested=TRUE WHERE test_id=$1",
            )
            .bind(&command.operation_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        }
        let response = json!({"test_id":command.operation_id,"cancel_requested":true});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.connection_test.cancel_requested",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.operation_id),
                before_hash: None,
                after_hash: None,
                result_code: "cancel_requested",
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_provider_connection_test(
        &self,
        provider_id: &str,
        test_id: &str,
    ) -> Result<Option<ProviderConnectionTest>, RepositoryError> {
        sqlx::query(
            "SELECT test_id,provider_id,source_draft_version,provider_input_hash,test_mode,
                    operation_status,cancel_requested,attempts,claimed_by,claim_token,claim_expires_at,
                    failure_code,failure_stage,failure_retryable,failure_correlation_id,result_hash,
                    result_document,created_at,started_at,finished_at
             FROM provider_connection_tests WHERE provider_id=$1 AND test_id=$2",
        ).bind(provider_id).bind(test_id).fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?
            .as_ref().map(postgres_test).transpose()
    }

    async fn claim_provider_connection_tests(
        &self,
        command: ClaimProviderOperationsCommand,
    ) -> Result<Vec<ProviderConnectionTestClaim>, RepositoryError> {
        let rows = sqlx::query(
            "WITH candidates AS (
               SELECT test_id FROM provider_connection_tests
               WHERE (operation_status='pending' OR (operation_status='running' AND claim_expires_at<=$1))
                 AND NOT cancel_requested ORDER BY created_at,test_id FOR UPDATE SKIP LOCKED LIMIT $2
             )
             UPDATE provider_connection_tests t SET operation_status='running',attempts=t.attempts+1,
               claimed_by=$3,claim_token='pct_'||replace(gen_random_uuid()::text,'-',''),claim_expires_at=$4,
               started_at=COALESCE(t.started_at,$1)
             FROM candidates c WHERE t.test_id=c.test_id
             RETURNING t.test_id,t.provider_id,t.source_draft_version,t.provider_input_hash,t.test_mode,
               t.operation_status,t.cancel_requested,t.attempts,t.claimed_by,t.claim_token,t.claim_expires_at,
               t.failure_code,t.failure_stage,t.failure_retryable,t.failure_correlation_id,t.result_hash,
               t.result_document,t.created_at,t.started_at,t.finished_at,t.draft_document",
        ).bind(database_time(command.now)).bind(i64::from(command.limit)).bind(&command.worker_id)
            .bind(database_time(command.lease_expires_at)).fetch_all(&self.pool)
            .await.map_err(RepositoryError::storage)?;
        rows.iter()
            .map(|row| {
                Ok(ProviderConnectionTestClaim {
                    operation: postgres_test(row)?,
                    claim_token: row
                        .try_get::<String, _>("claim_token")
                        .map_err(RepositoryError::storage)?,
                    draft_document: row
                        .try_get("draft_document")
                        .map_err(RepositoryError::storage)?,
                })
            })
            .collect()
    }

    async fn complete_provider_connection_test(
        &self,
        command: CompleteProviderConnectionTestCommand,
    ) -> Result<(), ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        let cancel: bool = sqlx::query_scalar(
            "SELECT cancel_requested FROM provider_connection_tests
             WHERE test_id=$1 AND operation_status='running' AND claim_token=$2 FOR UPDATE",
        )
        .bind(&command.test_id)
        .bind(&command.claim_token)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::FenceLost,
        ))?;
        let result = if cancel {
            CompleteProviderConnectionTestResult::Cancelled
        } else {
            command.result
        };
        let now = database_time(command.now);
        match result {
            CompleteProviderConnectionTestResult::Succeeded {
                result_hash,
                result,
            } => {
                if canonical_hash(&result)? != result_hash {
                    return Err(ProviderManagementWriteError::Conflict(
                        ProviderManagementConflict::CandidateMismatch,
                    ));
                }
                sqlx::query("UPDATE provider_connection_tests SET operation_status='succeeded',claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,result_hash=$1,result_document=$2,finished_at=$3 WHERE test_id=$4")
                    .bind(result_hash).bind(result).bind(now).bind(&command.test_id)
                    .execute(&mut *transaction).await.map_err(storage)?;
            }
            CompleteProviderConnectionTestResult::Failed(failure) => {
                sqlx::query("UPDATE provider_connection_tests SET operation_status='failed',claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,failure_code=$1,failure_stage=$2,failure_retryable=$3,failure_correlation_id=$4,finished_at=$5 WHERE test_id=$6")
                    .bind(failure.code).bind(failure.stage).bind(failure.retryable).bind(failure.correlation_id)
                    .bind(now).bind(&command.test_id).execute(&mut *transaction).await.map_err(storage)?;
            }
            CompleteProviderConnectionTestResult::Cancelled => {
                sqlx::query("UPDATE provider_connection_tests SET operation_status='cancelled',cancel_requested=TRUE,claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,finished_at=$1 WHERE test_id=$2")
                    .bind(now).bind(&command.test_id).execute(&mut *transaction).await.map_err(storage)?;
            }
        }
        transaction.commit().await.map_err(storage)?;
        Ok(())
    }

    async fn create_provider_validation(
        &self,
        command: CreateProviderValidationCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query(
            "SELECT p.operational_state,d.draft_version,d.provider_input_hash
             FROM managed_providers p JOIN provider_drafts d USING(provider_id)
             WHERE p.provider_id=$1 FOR UPDATE OF p,d",
        )
        .bind(&command.report.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        let actual = i64_to_u64(row.try_get("draft_version").map_err(storage)?)?;
        let input_hash: String = row.try_get("provider_input_hash").map_err(storage)?;
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            == "retired"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        if actual != command.expected_draft_version
            || actual != command.report.draft_version
            || input_hash != command.expected_provider_input_hash
            || input_hash != command.report.provider_input_hash
            || canonical_hash(&command.report.document)? != command.report.report_hash
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        sqlx::query(
            "INSERT INTO provider_validation_reports(
               validation_id,provider_id,draft_version,provider_input_hash,report_hash,valid,document,created_at,created_by
             ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        ).bind(&command.report.validation_id).bind(&command.report.provider_id)
            .bind(u64_to_i64(command.report.draft_version)?).bind(&command.report.provider_input_hash)
            .bind(&command.report.report_hash).bind(command.report.valid).bind(&command.report.document)
            .bind(database_time(command.report.created_at)).bind(&command.report.created_by)
            .execute(&mut *transaction).await.map_err(storage)?;
        let response = serde_json::to_value(&command.report).map_err(|_| invalid_data())?;
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.validation.created",
                provider_id: Some(&command.report.provider_id),
                subject_id: Some(&command.report.validation_id),
                before_hash: None,
                after_hash: Some(&command.report.report_hash),
                result_code: if command.report.valid {
                    "valid"
                } else {
                    "invalid"
                },
                status: 202,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_provider_validation(
        &self,
        provider_id: &str,
        validation_id: &str,
    ) -> Result<Option<ProviderValidationReport>, RepositoryError> {
        sqlx::query(
            "SELECT validation_id,provider_id,draft_version,provider_input_hash,report_hash,
                    valid,document,created_at,created_by
             FROM provider_validation_reports WHERE provider_id=$1 AND validation_id=$2",
        )
        .bind(provider_id)
        .bind(validation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_validation)
        .transpose()
    }

    async fn publish_provider_revision(
        &self,
        command: PublishProviderRevisionCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let provider = sqlx::query(
            "SELECT p.operational_state,d.draft_version,d.provider_input_hash
             FROM managed_providers p JOIN provider_drafts d USING(provider_id)
             WHERE p.provider_id=$1 FOR UPDATE OF p,d",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        let actual = i64_to_u64(provider.try_get("draft_version").map_err(storage)?)?;
        let input_hash: String = provider.try_get("provider_input_hash").map_err(storage)?;
        if provider
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            == "retired"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        if actual != command.expected_draft_version
            || canonical_hash(&command.document)? != command.revision_hash
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        let validation = sqlx::query(
            "SELECT draft_version,provider_input_hash,valid FROM provider_validation_reports
             WHERE provider_id=$1 AND validation_id=$2",
        )
        .bind(&command.provider_id)
        .bind(&command.validation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::ValidationFailed,
        ))?;
        if validation
            .try_get::<i64, _>("draft_version")
            .map_err(storage)?
            != u64_to_i64(actual)?
            || validation
                .try_get::<String, _>("provider_input_hash")
                .map_err(storage)?
                != input_hash
            || !validation.try_get::<bool, _>("valid").map_err(storage)?
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ValidationFailed,
            ));
        }
        if let Some(discovery_id) = &command.discovery_id {
            let valid: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM provider_discovery_operations o JOIN provider_discovery_snapshots s USING(discovery_id)
                 WHERE o.provider_id=$1 AND o.discovery_id=$2 AND o.operation_status='succeeded' AND NOT o.stale
                   AND o.source_draft_version=$3 AND o.provider_input_hash=$4",
            ).bind(&command.provider_id).bind(discovery_id).bind(u64_to_i64(actual)?).bind(&input_hash)
                .fetch_one(&mut *transaction).await.map_err(storage)?;
            if valid != 1 {
                return Err(ProviderManagementWriteError::Conflict(
                    ProviderManagementConflict::OperationStale,
                ));
            }
        }
        if let Some(test_id) = &command.connection_test_id {
            let valid: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM provider_connection_tests WHERE provider_id=$1 AND test_id=$2
                   AND operation_status='succeeded' AND source_draft_version=$3 AND provider_input_hash=$4",
            ).bind(&command.provider_id).bind(test_id).bind(u64_to_i64(actual)?).bind(&input_hash)
                .fetch_one(&mut *transaction).await.map_err(storage)?;
            if valid != 1 {
                return Err(ProviderManagementWriteError::Conflict(
                    ProviderManagementConflict::OperationStale,
                ));
            }
        }
        let models = model_documents(&command.document).map_err(|_| {
            ProviderManagementWriteError::Conflict(ProviderManagementConflict::CandidateMismatch)
        })?;
        let revision_number: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(revision_number),0)+1 FROM provider_revisions WHERE provider_id=$1")
            .bind(&command.provider_id).fetch_one(&mut *transaction).await.map_err(storage)?;
        let now = database_time(command.metadata.now);
        sqlx::query(
            "INSERT INTO provider_revisions(
               revision_id,provider_id,revision_number,source_draft_version,validation_id,discovery_id,
               connection_test_id,revision_hash,document,created_at,created_by
             ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        ).bind(&command.revision_id).bind(&command.provider_id).bind(revision_number).bind(u64_to_i64(actual)?)
            .bind(&command.validation_id).bind(&command.discovery_id).bind(&command.connection_test_id)
            .bind(&command.revision_hash).bind(&command.document).bind(now).bind(&command.metadata.operator_id)
            .execute(&mut *transaction).await.map_err(storage)?;
        for (ordinal, (model_id, capability_hash, document)) in models.iter().enumerate() {
            sqlx::query("INSERT INTO provider_revision_models(revision_id,ordinal,model_id,capability_hash,document) VALUES($1,$2,$3,$4,$5)")
                .bind(&command.revision_id).bind(i64::try_from(ordinal).map_err(|_| invalid_data())?)
                .bind(model_id).bind(capability_hash).bind(document)
                .execute(&mut *transaction).await.map_err(storage)?;
        }
        let response = json!({"revision_id":command.revision_id,"provider_id":command.provider_id,"revision_number":revision_number,"revision_hash":command.revision_hash});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.revision.published",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.revision_id),
                before_hash: Some(&input_hash),
                after_hash: Some(&command.revision_hash),
                result_code: "published",
                status: 201,
                response,
                etag: None,
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn get_provider_revision(
        &self,
        provider_id: &str,
        revision_id: &str,
    ) -> Result<Option<ProviderRevision>, RepositoryError> {
        sqlx::query(
            "SELECT revision_id,provider_id,revision_number,source_draft_version,validation_id,
                    discovery_id,connection_test_id,revision_hash,document,created_at,created_by
             FROM provider_revisions WHERE provider_id=$1 AND revision_id=$2",
        )
        .bind(provider_id)
        .bind(revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(postgres_revision)
        .transpose()
    }

    async fn list_provider_revisions(
        &self,
        provider_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ProviderManagementPage<ProviderRevision>, RepositoryError> {
        let cursor = decode_created_cursor(cursor)?;
        let cursor_created_at = cursor.as_ref().map(|(created_at, _)| created_at);
        let cursor_id = cursor.as_ref().map(|(_, stable_id)| stable_id.as_str());
        let rows = sqlx::query(
            "SELECT revision_id,provider_id,revision_number,source_draft_version,validation_id,
                    discovery_id,connection_test_id,revision_hash,document,created_at,created_by
             FROM provider_revisions WHERE provider_id=$1
               AND ($2::timestamptz IS NULL OR created_at>$2 OR
                    (created_at=$2 AND revision_id>$3))
             ORDER BY created_at,revision_id LIMIT $4",
        )
        .bind(provider_id)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.saturating_add(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        let mut items = rows
            .iter()
            .map(postgres_revision)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if items.len() > limit as usize {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_created_cursor(item.created_at, &item.revision_id))
        } else {
            None
        };
        Ok(ProviderManagementPage { items, next_cursor })
    }

    async fn activate_provider_revision(
        &self,
        command: ActivateProviderRevisionCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT operational_state,provider_version,active_revision_id FROM managed_providers WHERE provider_id=$1 FOR UPDATE")
            .bind(&command.provider_id).fetch_optional(&mut *transaction).await.map_err(storage)?
            .ok_or(ProviderManagementWriteError::Conflict(ProviderManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("provider_version").map_err(storage)?)?;
        if actual != command.expected_provider_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            != "enabled"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let revision_hash: String = sqlx::query_scalar(
            "SELECT revision_hash FROM provider_revisions WHERE provider_id=$1 AND revision_id=$2",
        )
        .bind(&command.provider_id)
        .bind(&command.revision_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage)?
        .ok_or(ProviderManagementWriteError::Conflict(
            ProviderManagementConflict::NotFound,
        ))?;
        let old: Option<String> = row.try_get("active_revision_id").map_err(storage)?;
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query("UPDATE managed_providers SET active_revision_id=$1,provider_version=$2,updated_at=$3 WHERE provider_id=$4 AND provider_version=$5")
            .bind(&command.revision_id).bind(u64_to_i64(next)?).bind(database_time(command.metadata.now))
            .bind(&command.provider_id).bind(u64_to_i64(actual)?).execute(&mut *transaction).await.map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"active_revision_id":command.revision_id,"provider_version":next});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.revision.activated",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.revision_id),
                before_hash: old.as_deref(),
                after_hash: Some(&revision_hash),
                result_code: "activated",
                status: 200,
                response,
                etag: Some(provider_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn deactivate_provider_revision(
        &self,
        command: DeactivateProviderRevisionCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT operational_state,provider_version,active_revision_id FROM managed_providers WHERE provider_id=$1 FOR UPDATE")
            .bind(&command.provider_id).fetch_optional(&mut *transaction).await.map_err(storage)?
            .ok_or(ProviderManagementWriteError::Conflict(ProviderManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("provider_version").map_err(storage)?)?;
        let active: Option<String> = row.try_get("active_revision_id").map_err(storage)?;
        if actual != command.expected_provider_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            != "enabled"
            || active.is_none()
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query("UPDATE managed_providers SET active_revision_id=NULL,provider_version=$1,updated_at=$2 WHERE provider_id=$3 AND provider_version=$4")
            .bind(u64_to_i64(next)?).bind(database_time(command.metadata.now)).bind(&command.provider_id).bind(u64_to_i64(actual)?)
            .execute(&mut *transaction).await.map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"active_revision_id":null,"provider_version":next});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.revision.deactivated",
                provider_id: Some(&command.provider_id),
                subject_id: active.as_deref(),
                before_hash: active.as_deref(),
                after_hash: None,
                result_code: "deactivated",
                status: 200,
                response,
                etag: Some(provider_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn suspend_provider(
        &self,
        command: SuspendProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT operational_state,provider_version,suspension_fence FROM managed_providers WHERE provider_id=$1 FOR UPDATE")
            .bind(&command.provider_id).fetch_optional(&mut *transaction).await.map_err(storage)?
            .ok_or(ProviderManagementWriteError::Conflict(ProviderManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("provider_version").map_err(storage)?)?;
        let fence = i64_to_u64(row.try_get("suspension_fence").map_err(storage)?)?;
        if actual != command.expected_provider_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            != "enabled"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        let next_fence = fence.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query("UPDATE managed_providers SET operational_state='suspended',provider_version=$1,suspension_fence=$2,updated_at=$3 WHERE provider_id=$4 AND provider_version=$5")
            .bind(u64_to_i64(next)?).bind(u64_to_i64(next_fence)?).bind(database_time(command.metadata.now))
            .bind(&command.provider_id).bind(u64_to_i64(actual)?).execute(&mut *transaction).await.map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"operational_state":"suspended","provider_version":next,"suspension_fence":next_fence,"reason_code":command.reason_code});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.suspended",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.provider_id),
                before_hash: None,
                after_hash: None,
                result_code: "suspended",
                status: 200,
                response,
                etag: Some(provider_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn resume_provider(
        &self,
        command: ResumeProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT operational_state,provider_version FROM managed_providers WHERE provider_id=$1 FOR UPDATE")
            .bind(&command.provider_id).fetch_optional(&mut *transaction).await.map_err(storage)?
            .ok_or(ProviderManagementWriteError::Conflict(ProviderManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("provider_version").map_err(storage)?)?;
        if actual != command.expected_provider_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            != "suspended"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query("UPDATE managed_providers SET operational_state='enabled',provider_version=$1,updated_at=$2 WHERE provider_id=$3 AND provider_version=$4")
            .bind(u64_to_i64(next)?).bind(database_time(command.metadata.now)).bind(&command.provider_id).bind(u64_to_i64(actual)?)
            .execute(&mut *transaction).await.map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"operational_state":"enabled","provider_version":next});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.resumed",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.provider_id),
                before_hash: None,
                after_hash: None,
                result_code: "resumed",
                status: 200,
                response,
                etag: Some(provider_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn retire_provider(
        &self,
        command: RetireProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        if let Some(receipt) = postgres_replay(&mut transaction, &command.metadata).await? {
            transaction.rollback().await.map_err(storage)?;
            return Ok(receipt);
        }
        let row = sqlx::query("SELECT operational_state,provider_version,suspension_fence FROM managed_providers WHERE provider_id=$1 FOR UPDATE")
            .bind(&command.provider_id).fetch_optional(&mut *transaction).await.map_err(storage)?
            .ok_or(ProviderManagementWriteError::Conflict(ProviderManagementConflict::NotFound))?;
        let actual = i64_to_u64(row.try_get("provider_version").map_err(storage)?)?;
        let fence = i64_to_u64(row.try_get("suspension_fence").map_err(storage)?)?;
        if actual != command.expected_provider_version {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::PreconditionFailed,
            ));
        }
        if row
            .try_get::<String, _>("operational_state")
            .map_err(storage)?
            == "retired"
        {
            return Err(ProviderManagementWriteError::Conflict(
                ProviderManagementConflict::ForbiddenState,
            ));
        }
        let next = actual.checked_add(1).ok_or_else(invalid_data)?;
        let next_fence = fence.checked_add(1).ok_or_else(invalid_data)?;
        sqlx::query("UPDATE managed_providers SET operational_state='retired',active_revision_id=NULL,provider_version=$1,suspension_fence=$2,updated_at=$3 WHERE provider_id=$4 AND provider_version=$5")
            .bind(u64_to_i64(next)?).bind(u64_to_i64(next_fence)?).bind(database_time(command.metadata.now))
            .bind(&command.provider_id).bind(u64_to_i64(actual)?).execute(&mut *transaction).await.map_err(storage)?;
        let response = json!({"provider_id":command.provider_id,"operational_state":"retired","provider_version":next,"suspension_fence":next_fence,"reason_code":command.reason_code});
        let receipt = postgres_finalize(
            &mut transaction,
            &command.metadata,
            PostgresFinalize {
                event_kind: "provider.retired",
                provider_id: Some(&command.provider_id),
                subject_id: Some(&command.provider_id),
                before_hash: None,
                after_hash: None,
                result_code: "retired",
                status: 200,
                response,
                etag: Some(provider_etag(next)),
            },
        )
        .await?;
        transaction.commit().await.map_err(storage)?;
        Ok(receipt)
    }

    async fn load_active_provider_revisions(
        &self,
    ) -> Result<Vec<(ManagedProvider, ProviderRevision)>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT p.provider_id,p.display_name,p.adapter_type,p.operational_state,p.provider_version,
                    p.draft_version,p.active_revision_id,p.suspension_fence,p.created_at,p.updated_at,
                    r.revision_id,r.provider_id AS revision_provider_id,r.revision_number,r.source_draft_version,
                    r.validation_id,r.discovery_id,r.connection_test_id,r.revision_hash,r.document,
                    r.created_at AS revision_created_at,r.created_by
             FROM managed_providers p JOIN provider_revisions r ON r.revision_id=p.active_revision_id
             WHERE p.operational_state='enabled' ORDER BY p.provider_id",
        ).fetch_all(&self.pool).await.map_err(RepositoryError::storage)?;
        rows.iter()
            .map(|row| {
                Ok((
                    postgres_provider(row)?,
                    ProviderRevision {
                        revision_id: row
                            .try_get("revision_id")
                            .map_err(RepositoryError::storage)?,
                        provider_id: row
                            .try_get("revision_provider_id")
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
                        discovery_id: row
                            .try_get("discovery_id")
                            .map_err(RepositoryError::storage)?,
                        connection_test_id: row
                            .try_get("connection_test_id")
                            .map_err(RepositoryError::storage)?,
                        revision_hash: row
                            .try_get("revision_hash")
                            .map_err(RepositoryError::storage)?,
                        document: row.try_get("document").map_err(RepositoryError::storage)?,
                        created_at: row
                            .try_get("revision_created_at")
                            .map_err(RepositoryError::storage)?,
                        created_by: row
                            .try_get("created_by")
                            .map_err(RepositoryError::storage)?,
                    },
                ))
            })
            .collect()
    }

    async fn load_provider_revision_archive(
        &self,
    ) -> Result<Vec<(ManagedProvider, ProviderRevision)>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT p.provider_id,p.display_name,p.adapter_type,p.operational_state,p.provider_version,
                    p.draft_version,p.active_revision_id,p.suspension_fence,p.created_at,p.updated_at,
                    r.revision_id,r.provider_id AS revision_provider_id,r.revision_number,r.source_draft_version,
                    r.validation_id,r.discovery_id,r.connection_test_id,r.revision_hash,r.document,
                    r.created_at AS revision_created_at,r.created_by
             FROM managed_providers p JOIN provider_revisions r ON r.provider_id=p.provider_id
             ORDER BY p.provider_id,r.revision_number",
        ).fetch_all(&self.pool).await.map_err(RepositoryError::storage)?;
        rows.iter()
            .map(|row| {
                Ok((
                    postgres_provider(row)?,
                    ProviderRevision {
                        revision_id: row
                            .try_get("revision_id")
                            .map_err(RepositoryError::storage)?,
                        provider_id: row
                            .try_get("revision_provider_id")
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
                        discovery_id: row
                            .try_get("discovery_id")
                            .map_err(RepositoryError::storage)?,
                        connection_test_id: row
                            .try_get("connection_test_id")
                            .map_err(RepositoryError::storage)?,
                        revision_hash: row
                            .try_get("revision_hash")
                            .map_err(RepositoryError::storage)?,
                        document: row.try_get("document").map_err(RepositoryError::storage)?,
                        created_at: row
                            .try_get("revision_created_at")
                            .map_err(RepositoryError::storage)?,
                        created_by: row
                            .try_get("created_by")
                            .map_err(RepositoryError::storage)?,
                    },
                ))
            })
            .collect()
    }

    async fn load_provider_legacy_model_bindings(
        &self,
        revision_id: &str,
    ) -> Result<Vec<ProviderLegacyModelBinding>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT revision_id,provider_id,model_id,legacy_binding_hash,
                    legacy_binding_evidence,source_definition_id,source_deployment_revision_id,created_at
             FROM provider_revision_legacy_model_bindings
             WHERE revision_id=$1 ORDER BY model_id,legacy_binding_hash",
        )
        .bind(revision_id)
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        rows.iter()
            .map(|row| {
                Ok(ProviderLegacyModelBinding {
                    revision_id: row
                        .try_get("revision_id")
                        .map_err(RepositoryError::storage)?,
                    provider_id: row
                        .try_get("provider_id")
                        .map_err(RepositoryError::storage)?,
                    model_id: row.try_get("model_id").map_err(RepositoryError::storage)?,
                    legacy_binding_hash: row
                        .try_get("legacy_binding_hash")
                        .map_err(RepositoryError::storage)?,
                    legacy_binding_evidence: row
                        .try_get("legacy_binding_evidence")
                        .map_err(RepositoryError::storage)?,
                    source_definition_id: row
                        .try_get("source_definition_id")
                        .map_err(RepositoryError::storage)?,
                    source_deployment_revision_id: row
                        .try_get("source_deployment_revision_id")
                        .map_err(RepositoryError::storage)?,
                    created_at: row
                        .try_get("created_at")
                        .map_err(RepositoryError::storage)?,
                })
            })
            .collect()
    }

    async fn load_provider_fence(
        &self,
        provider_id: &str,
    ) -> Result<Option<ProviderFence>, RepositoryError> {
        let row = sqlx::query("SELECT provider_id,operational_state,active_revision_id,suspension_fence FROM managed_providers WHERE provider_id=$1")
            .bind(provider_id).fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?;
        row.as_ref()
            .map(|row| {
                Ok(ProviderFence {
                    provider_id: row
                        .try_get("provider_id")
                        .map_err(RepositoryError::storage)?,
                    operational_state: operational_state(
                        &row.try_get::<String, _>("operational_state")
                            .map_err(RepositoryError::storage)?,
                    )?,
                    active_revision_id: row
                        .try_get("active_revision_id")
                        .map_err(RepositoryError::storage)?,
                    suspension_fence: i64_to_u64(
                        row.try_get("suspension_fence")
                            .map_err(RepositoryError::storage)?,
                    )?,
                })
            })
            .transpose()
    }

    async fn load_provider_management_runtime_stats(
        &self,
    ) -> Result<ProviderManagementRuntimeStats, RepositoryError> {
        let row = sqlx::query(
            "SELECT (SELECT COUNT(*) FROM provider_discovery_operations WHERE operation_status='pending') pending_discoveries,
                    (SELECT COUNT(*) FROM provider_discovery_operations WHERE operation_status='running') running_discoveries,
                    (SELECT COUNT(*) FROM provider_connection_tests WHERE operation_status='pending') pending_connection_tests,
                    (SELECT COUNT(*) FROM provider_connection_tests WHERE operation_status='running') running_connection_tests,
                    (SELECT COUNT(*) FROM managed_providers WHERE operational_state='enabled' AND active_revision_id IS NOT NULL) active_providers,
                    (SELECT COUNT(*) FROM managed_providers WHERE operational_state='suspended') suspended_providers,
                    (SELECT COUNT(*) FROM managed_providers WHERE operational_state='enabled') enabled_providers,
                    (SELECT COUNT(*) FROM managed_providers WHERE operational_state='retired') retired_providers",
        ).fetch_one(&self.pool).await.map_err(RepositoryError::storage)?;
        let connection_tests = sqlx::query(
            "SELECT test_mode,operation_status,COUNT(*) count FROM provider_connection_tests
             GROUP BY test_mode,operation_status ORDER BY test_mode,operation_status",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .iter()
        .map(|row| {
            Ok(ProviderConnectionTestRuntimeCount {
                mode: test_mode(
                    &row.try_get::<String, _>("test_mode")
                        .map_err(RepositoryError::storage)?,
                )?,
                outcome: operation_status(
                    &row.try_get::<String, _>("operation_status")
                        .map_err(RepositoryError::storage)?,
                )?,
                count: i64_to_u64(row.try_get("count").map_err(RepositoryError::storage)?)?,
            })
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
        let operations = sqlx::query(
            "SELECT event_kind,result_code,COUNT(*) count FROM provider_management_audit_events
             GROUP BY event_kind,result_code ORDER BY event_kind,result_code",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .iter()
        .map(|row| {
            Ok(ProviderManagementOperationCount {
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
        Ok(ProviderManagementRuntimeStats {
            pending_discoveries: i64_to_u64(
                row.try_get("pending_discoveries")
                    .map_err(RepositoryError::storage)?,
            )?,
            running_discoveries: i64_to_u64(
                row.try_get("running_discoveries")
                    .map_err(RepositoryError::storage)?,
            )?,
            pending_connection_tests: i64_to_u64(
                row.try_get("pending_connection_tests")
                    .map_err(RepositoryError::storage)?,
            )?,
            running_connection_tests: i64_to_u64(
                row.try_get("running_connection_tests")
                    .map_err(RepositoryError::storage)?,
            )?,
            active_providers: i64_to_u64(
                row.try_get("active_providers")
                    .map_err(RepositoryError::storage)?,
            )?,
            suspended_providers: i64_to_u64(
                row.try_get("suspended_providers")
                    .map_err(RepositoryError::storage)?,
            )?,
            enabled_providers: i64_to_u64(
                row.try_get("enabled_providers")
                    .map_err(RepositoryError::storage)?,
            )?,
            retired_providers: i64_to_u64(
                row.try_get("retired_providers")
                    .map_err(RepositoryError::storage)?,
            )?,
            connection_tests,
            operations,
        })
    }

    async fn cleanup_terminal_provider_operations(
        &self,
        finished_before: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let discoveries = sqlx::query(
            "DELETE FROM provider_discovery_operations WHERE discovery_id IN(
               SELECT o.discovery_id FROM provider_discovery_operations o
               LEFT JOIN provider_discovery_snapshots s USING(discovery_id)
               LEFT JOIN provider_revisions r ON r.discovery_id=s.discovery_id
               WHERE o.operation_status IN('failed','cancelled','succeeded') AND o.finished_at<$1
                 AND r.revision_id IS NULL ORDER BY o.finished_at,o.discovery_id LIMIT $2)",
        )
        .bind(database_time(finished_before))
        .bind(i64::from(limit))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        let remaining = u64::from(limit).saturating_sub(discoveries);
        let tests = if remaining == 0 {
            0
        } else {
            sqlx::query(
                "DELETE FROM provider_connection_tests WHERE test_id IN(
                   SELECT t.test_id FROM provider_connection_tests t LEFT JOIN provider_revisions r ON r.connection_test_id=t.test_id
                   WHERE t.operation_status IN('failed','cancelled','succeeded') AND t.finished_at<$1
                     AND r.revision_id IS NULL ORDER BY t.finished_at,t.test_id LIMIT $2)",
            ).bind(database_time(finished_before)).bind(u64_to_i64(remaining)?).execute(&mut *transaction)
                .await.map_err(RepositoryError::storage)?.rows_affected()
        };
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(discoveries + tests)
    }
}
