use super::*;
use insight_platform_artifacts::{
    validate_artifact_prepare_replay, ArtifactUploadCompletionReceiptV1,
    ArtifactUploadCompletionReplay, ArtifactUploadReplayIdentity,
};

fn required_receipt_authority(error: RepositoryError) -> RepositoryError {
    match error {
        RepositoryError::NotFound(_) => {
            RepositoryError::CorruptRow("Artifact upload Receipt authority is missing".to_owned())
        }
        other => other,
    }
}

async fn require_upload_replay_permission(
    transaction: &mut Transaction<'_, Postgres>,
    identity: &ArtifactUploadReplayIdentity,
) -> Result<(), RepositoryError> {
    identity.validate()?;
    let principal = load_current_principal_snapshot(
        transaction,
        &identity.tenant_id,
        &identity.principal_id,
        identity.principal_kind,
    )
    .await?;
    if !principal.permissions.contains(Permission::ArtifactWrite) {
        return Err(RepositoryError::PermissionDenied);
    }
    Ok(())
}

async fn read_upload_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    identity: &ArtifactUploadReplayIdentity,
    artifact_id: Option<&ResourceId>,
    operation: &str,
    lock: bool,
) -> Result<Option<PgRow>, RepositoryError> {
    let scope_kind = if artifact_id.is_some() {
        "artifact"
    } else {
        "artifact_collection"
    };
    let scope_id = artifact_id.unwrap_or(&identity.tenant_id);
    let sql = if lock {
        "SELECT request_digest, state, disposition, response_reference_id, payload_schema_version, payload, payload_digest
        FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = $2 AND scope_id = $3 AND dedupe_owner_id = $4
          AND operation = $5 AND idempotency_key_digest = $6 FOR UPDATE"
    } else {
        "SELECT request_digest, state, disposition, response_reference_id, payload_schema_version, payload, payload_digest
        FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = $2 AND scope_id = $3 AND dedupe_owner_id = $4
          AND operation = $5 AND idempotency_key_digest = $6"
    };
    let row = sqlx::query(sql)
        .bind(identity.tenant_id.to_string())
        .bind(scope_kind)
        .bind(scope_id.to_string())
        .bind(identity.principal_id.to_string())
        .bind(operation)
        .bind(identity.idempotency_key_digest.to_string())
        .fetch_optional(&mut **transaction)
        .await?;
    let Some(row) = row else { return Ok(None) };
    if row.try_get::<String, _>("request_digest")? != identity.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("Artifact upload Receipt"));
    }
    // Even safe projection reads must reject damaged persisted command evidence.
    payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    Ok(Some(row))
}

pub(super) async fn read_prepare_replay(
    transaction: &mut Transaction<'_, Postgres>,
    identity: &ArtifactUploadReplayIdentity,
) -> Result<Option<PreparedArtifact>, RepositoryError> {
    let Some(row) =
        read_upload_receipt(transaction, identity, None, "artifact.prepare", true).await?
    else {
        return Ok(None);
    };
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let result: ArtifactPrepareReceiptResult =
        decode_versioned_payload(&payload, "Artifact prepare Receipt")?;
    validate_artifact_prepare_receipt_result(&result)?;
    if row
        .try_get::<Option<String>, _>("response_reference_id")?
        .as_deref()
        != Some(result.artifact_id.to_string().as_str())
        || row.try_get::<Option<String>, _>("disposition")?.as_deref() != Some("prepared")
    {
        return Err(RepositoryError::CorruptRow(
            "Artifact prepare Receipt result differs".to_owned(),
        ));
    }
    load_live_prepare_replay(transaction, identity, &result)
        .await
        .map(Some)
}

pub(super) async fn load_live_prepare_replay(
    transaction: &mut Transaction<'_, Postgres>,
    identity: &ArtifactUploadReplayIdentity,
    result: &ArtifactPrepareReceiptResult,
) -> Result<PreparedArtifact, RepositoryError> {
    let state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.artifacts WHERE tenant_id = $1 AND artifact_id = $2 FOR UPDATE"
    ).bind(identity.tenant_id.to_string()).bind(result.artifact_id.to_string())
        .fetch_optional(&mut **transaction).await?.ok_or_else(|| RepositoryError::CorruptRow("Artifact prepare Receipt owner is missing".to_owned()))?;
    if state != ArtifactState::Staging.as_str() {
        return Err(RepositoryError::Conflict(
            "Artifact upload is no longer staging",
        ));
    }
    let prepared = load_artifact_bundle(
        transaction,
        &identity.tenant_id,
        &result.artifact_id,
        &result.blob_id,
        &result.upload_grant_id,
        &result.operation_id,
    )
    .await
    .map_err(required_receipt_authority)?;
    let now = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await?;
    validate_artifact_prepare_replay(&prepared, identity, now).map_err(|error| match error {
        ArtifactCommandError::InvalidTransition => {
            RepositoryError::Conflict("Artifact upload is no longer staging")
        }
        _ => RepositoryError::CorruptRow("Artifact prepare Receipt authority differs".to_owned()),
    })?;
    Ok(prepared)
}

pub(super) async fn read_completion_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    identity: &ArtifactUploadReplayIdentity,
    artifact_id: &ResourceId,
    lock: bool,
) -> Result<Option<ArtifactUploadCompletionReceiptV1>, RepositoryError> {
    let Some(row) = read_upload_receipt(
        transaction,
        identity,
        Some(artifact_id),
        "artifact.complete_upload",
        lock,
    )
    .await?
    else {
        return Ok(None);
    };
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let result: ArtifactUploadCompletionReceiptV1 =
        decode_versioned_payload(&payload, "Artifact complete Receipt")?;
    result.validate().map_err(|_| {
        RepositoryError::CorruptRow("Artifact complete Receipt is invalid".to_owned())
    })?;
    if result.artifact_id != *artifact_id
        || row
            .try_get::<Option<String>, _>("response_reference_id")?
            .as_deref()
            != Some(artifact_id.to_string().as_str())
        || row.try_get::<Option<String>, _>("disposition")?.as_deref() != Some("uploaded")
    {
        return Err(RepositoryError::CorruptRow(
            "Artifact complete Receipt result differs".to_owned(),
        ));
    }
    Ok(Some(result))
}

pub(super) async fn load_completed_upload_replay(
    transaction: &mut Transaction<'_, Postgres>,
    identity: &ArtifactUploadReplayIdentity,
    result: &ArtifactUploadCompletionReceiptV1,
) -> Result<CompletedArtifactUpload, RepositoryError> {
    let artifact = load_artifact_record(transaction, &identity.tenant_id, &result.artifact_id)
        .await
        .map_err(required_receipt_authority)?;
    // Current logical bytes are safe result metadata, never a target for the historical upload.
    let current_blob_id = artifact.blob_id.as_ref().ok_or_else(|| {
        RepositoryError::CorruptRow("Artifact complete result has no Blob".to_owned())
    })?;
    let bundle = load_artifact_bundle(
        transaction,
        &identity.tenant_id,
        &result.artifact_id,
        current_blob_id,
        &result.upload_grant_id,
        &result.operation_id,
    )
    .await
    .map_err(required_receipt_authority)?;
    if bundle.artifact.metadata.upload_operation_id()? != &result.operation_id
        || bundle.grant.snapshot.subject_principal_id != identity.principal_id
        || bundle.grant.snapshot.subject_principal_kind != identity.principal_kind
        || bundle.grant.snapshot.generation != result.grant_generation
        || bundle.grant.snapshot.token_digest != result.grant_token_digest
        || bundle.grant.snapshot.operation_id != result.operation_id
        || bundle.grant.snapshot.artifact_id != result.artifact_id
        || bundle.grant.snapshot.purpose != bundle.artifact.purpose
        || bundle.grant.snapshot.max_bytes != bundle.artifact.expected_size_bytes
        || bundle.grant.state != ArtifactLinkState::Consumed
        || bundle.artifact.version <= result.expected_artifact_version
    {
        return Err(RepositoryError::CorruptRow(
            "Artifact complete result evidence differs".to_owned(),
        ));
    }
    Ok(completed_upload(bundle))
}

/// The public preflight uses a read-only repeatable snapshot. A command transaction instead
/// stabilizes the logical Artifact before loading its current Blob across separate statements.
pub(super) async fn load_completed_upload_replay_locked(
    transaction: &mut Transaction<'_, Postgres>,
    identity: &ArtifactUploadReplayIdentity,
    result: &ArtifactUploadCompletionReceiptV1,
) -> Result<CompletedArtifactUpload, RepositoryError> {
    let found: Option<String> = sqlx::query_scalar(
        "SELECT artifact_id FROM insight_platform.artifacts WHERE tenant_id = $1 AND artifact_id = $2 FOR SHARE"
    ).bind(identity.tenant_id.to_string()).bind(result.artifact_id.to_string())
        .fetch_optional(&mut **transaction).await?;
    if found.is_none() {
        return Err(RepositoryError::CorruptRow(
            "Artifact complete Receipt owner is missing".to_owned(),
        ));
    }
    load_completed_upload_replay(transaction, identity, result).await
}

pub(super) async fn terminalize_upload_completion_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &CompleteArtifactUpload,
) -> Result<(), RepositoryError> {
    let result = ArtifactUploadCompletionReceiptV1::from_command(command);
    result.validate()?;
    let payload = TypedPayload::from_versioned(1, &result, 65_536)?;
    let count = sqlx::query(r#"
        UPDATE insight_platform.receipts SET payload_schema_version = $4, payload = $5, payload_digest = $6
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3 AND state = 'processing'
    "#).bind(command.audit.tenant_id.to_string()).bind(command.audit.receipt_id.to_string())
        .bind(command.audit.request_digest.to_string()).bind(payload.schema_version).bind(payload.value).bind(payload.digest)
        .execute(&mut **transaction).await?.rows_affected();
    ensure_one(count, "Artifact complete Receipt")?;
    terminalize_command_receipt(
        transaction,
        &command.audit,
        &command.artifact_id.to_string(),
        "uploaded",
    )
    .await
}

impl PgRepository {
    /// Observes existing authority only. A terminal prepare never yields an upload capability.
    pub async fn load_gateway_artifact_prepare_replay(
        &self,
        identity: ArtifactUploadReplayIdentity,
    ) -> Result<Option<PreparedArtifact>, RepositoryError> {
        identity.validate()?;
        let mut transaction = self.pool().begin().await?;
        require_upload_replay_permission(&mut transaction, &identity).await?;
        let replay = read_prepare_replay(&mut transaction, &identity).await?;
        transaction.commit().await?;
        Ok(replay)
    }

    /// Successful public complete and scan are one transaction; neither is reconstructed by I/O.
    pub async fn load_gateway_artifact_completion_replay(
        &self,
        query: ArtifactUploadCompletionReplay,
    ) -> Result<Option<CompletedArtifactUpload>, RepositoryError> {
        query.validate()?;
        let mut transaction = begin_read_only_repeatable(self.pool()).await?;
        require_upload_replay_permission(&mut transaction, &query.identity).await?;
        let completed =
            read_completion_receipt(&mut transaction, &query.identity, &query.artifact_id, false)
                .await?;
        let scan_identity = ArtifactUploadReplayIdentity {
            request_digest: query.scan_request_digest,
            ..query.identity.clone()
        };
        let scan = read_upload_receipt(
            &mut transaction,
            &scan_identity,
            Some(&query.artifact_id),
            "artifact.scan.schedule",
            false,
        )
        .await?;
        let result = match (completed, scan) {
            (None, None) => None,
            (Some(result), Some(scan)) => {
                if result.expected_artifact_version != query.expected_artifact_version
                    || result.grant_token_digest != query.grant_token_digest
                {
                    return Err(RepositoryError::IdempotencyConflict);
                }
                if scan
                    .try_get::<Option<String>, _>("response_reference_id")?
                    .as_deref()
                    != Some(result.operation_id.to_string().as_str())
                    || scan.try_get::<Option<String>, _>("disposition")?.as_deref()
                        != Some("scan_scheduled")
                {
                    return Err(RepositoryError::CorruptRow(
                        "Artifact scan Receipt result differs".to_owned(),
                    ));
                }
                let scan_payload =
                    payload_from_row(&scan, "payload_schema_version", "payload", "payload_digest")?;
                if scan_payload.schema_version != 1
                    || scan_payload.value
                        != serde_json::json!({
                            "schema_version": 1,
                            "operation": "artifact.scan.schedule",
                            "principal_id": query.identity.principal_id,
                            "scope_id": query.artifact_id,
                            "scope_kind": "artifact",
                        })
                {
                    return Err(RepositoryError::CorruptRow(
                        "Artifact scan Receipt identity differs".to_owned(),
                    ));
                }
                Some(
                    load_completed_upload_replay(&mut transaction, &query.identity, &result)
                        .await?,
                )
            }
            _ => {
                return Err(RepositoryError::Conflict(
                    "Artifact upload acceptance is incomplete",
                ))
            }
        };
        transaction.commit().await?;
        Ok(result)
    }
}
