use crate::repository::{
    append_command_event, append_scheduler_event, begin_read_only_repeatable,
    claim_command_receipt, decode_typed_payload, job_from_row, job_projection,
    load_current_principal_snapshot, load_job_for_update_by_text, payload_from_row,
    require_tenant_permission, safety_scan_cursor_from_row, safety_scan_page,
    terminalize_command_receipt, validate_safety_scan_request, JobRecord, PgRepository,
    RepositoryError, SafetyScanCursor, SafetyScanPage, SafetyScanShard, TypedPayload,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_artifacts::{
    decide_artifact_backend_failure, decide_commit_artifact_scan, decide_commit_blob_cleanup,
    decide_complete_artifact_deletion, decide_complete_upload, decide_create_artifact_provenance,
    decide_expired_artifact_attempt, decide_finalize_artifact, decide_mark_artifact_deletion,
    decide_place_artifact_hold, decide_release_artifact_hold, decide_release_artifact_reference,
    decide_schedule_artifact_rescan, decide_schedule_initial_scan, ArtifactBlobCleanupExecution,
    ArtifactBlobCleanupSnapshot, ArtifactBlobRecord, ArtifactCommandError, ArtifactCommandLimits,
    ArtifactDeleteObjectAuthority, ArtifactDeletionAdmissionFacts, ArtifactDeletionEvidence,
    ArtifactDeletionExecution, ArtifactDeletionJobSnapshot, ArtifactDeletionRecord,
    ArtifactGrantRecord, ArtifactHoldRecord, ArtifactHoldSnapshot, ArtifactJobPayload,
    ArtifactLinkState, ArtifactMetadataSnapshot, ArtifactObjectReadAuthority,
    ArtifactObjectReadAuthorityError, ArtifactOperationRecord, ArtifactProvenanceRecord,
    ArtifactProvenanceSnapshot, ArtifactRecord, ArtifactRecoveryParentAction,
    ArtifactReferenceRecord, ArtifactReferenceSnapshot, ArtifactScanDecision,
    ArtifactScanExecution, ArtifactScanKind, ArtifactScanObjectReadAuthority, ArtifactScanRequest,
    ArtifactScanWorkRecord, ArtifactStore, ArtifactTransaction, ArtifactWorkAuthority,
    ArtifactWorkError, ArtifactWorkerAudit, ArtifactWorkerOperationRecord,
    AuthorizedArtifactDeleteObject, AuthorizedArtifactScanObjectRead, CommitArtifactAttemptFailure,
    CommitArtifactBlobCleanup, CommitArtifactScanOutcome, CompleteArtifactDeletion,
    CompleteArtifactUpload, CompletedArtifactBlobCleanup, CompletedArtifactDeletion,
    CompletedArtifactUpload, CreateArtifactProvenance, DeleteArtifactBlobGeneration,
    EncryptedArtifactObjectReference, FinalizeArtifact, FinalizedArtifact,
    GatewayArtifactReadRequest, MarkArtifactDeletion, MarkedArtifactDeletion, PlaceArtifactHold,
    PrepareArtifact, PreparedArtifact, ReleaseArtifactHold, ReleaseArtifactReference,
    ScheduleArtifactRescan, ScheduleInitialArtifactScan, UploadGrantSnapshot,
};
use insight_platform_contracts::{
    ArtifactPurpose, ArtifactRef, ArtifactRetentionPolicy, ArtifactState, BlobIntegrityState,
    CommandOutcome, DataClassification, Effect, ExactVersionRef, JobState, Permission, PolicyKind,
    PrincipalKind, PublishedVersionPayload, ResourceDocument, ResourceId, ResourceKind,
    Sha256Digest, WorkClass,
};
use insight_platform_jobs::JobFence;
use insight_platform_tasks::{TaskDefinition, TaskPayload, TaskState};
use serde::de::DeserializeOwned;
use sqlx::{postgres::PgRow, Acquire, Postgres, Row, Transaction};
use std::{collections::BTreeSet, str::FromStr};

use crate::repository::ArtifactWorkerRole;

pub struct PgArtifactTransaction {
    transaction: Transaction<'static, Postgres>,
    limits: ArtifactCommandLimits,
}

/// Safe database projection consumed by the public Artifact Gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayArtifactSnapshot {
    pub artifact: ArtifactRecord,
    pub content: Option<ArtifactRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayArtifactDeletionTarget {
    pub artifact: ArtifactRecord,
    pub blob: ArtifactBlobRecord,
}

#[async_trait::async_trait]
impl ArtifactObjectReadAuthority<GatewayArtifactReadRequest> for PgRepository {
    async fn authorize_object_read(
        &self,
        request: &GatewayArtifactReadRequest,
    ) -> Result<
        insight_platform_artifacts::AuthorizedArtifactObjectRead,
        ArtifactObjectReadAuthorityError,
    > {
        request.validate_at(Utc::now())?;
        let mut transaction = begin_read_only_repeatable(self.pool())
            .await
            .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            &request.tenant_id,
            &request.principal_id,
            request.principal_kind,
        )
        .await
        .map_err(classify_gateway_artifact_read_error)?;
        if !principal.permissions.contains(Permission::ArtifactRead) {
            return Err(ArtifactObjectReadAuthorityError::Denied);
        }
        let row = sqlx::query(
            r#"
            SELECT artifact.version AS artifact_version,
                   artifact.classification, artifact.verified_media_type,
                   artifact.metadata_schema_version, artifact.metadata, artifact.metadata_digest,
                   blob.blob_id, blob.backend, blob.storage_binding_digest,
                   blob.object_reference_ciphertext, blob.object_generation,
                   blob.key_id, blob.encryption_domain_id, blob.content_digest,
                   blob.size_bytes, blob.version AS blob_version,
                   reference.artifact_link_id AS reference_id,
                   reference.version AS reference_version,
                   reference.link_key_digest AS reference_digest
            FROM insight_platform.artifacts AS artifact
            JOIN insight_platform.artifact_blobs AS blob
              ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
            JOIN LATERAL (
                SELECT artifact_link_id, version, link_key_digest
                FROM insight_platform.artifact_links
                WHERE tenant_id = artifact.tenant_id
                  AND target_artifact_id = artifact.artifact_id
                  AND link_kind = 'reference' AND state = 'active'
                  AND released_at IS NULL
                  AND (expires_at IS NULL OR expires_at > clock_timestamp())
                ORDER BY artifact_link_id
                LIMIT 1
            ) AS reference ON true
            WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
              AND artifact.state = 'ready' AND artifact.terminal_at IS NULL
              AND blob.state = 'verified' AND blob.deleted_at IS NULL
            "#,
        )
        .bind(request.tenant_id.to_string())
        .bind(request.artifact.artifact_id().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?
        .ok_or(ArtifactObjectReadAuthorityError::NotFound)?;
        let metadata = payload_from_row(
            &row,
            "metadata_schema_version",
            "metadata",
            "metadata_digest",
        )
        .map_err(classify_gateway_artifact_read_error)?;
        if row.try_get::<String, _>("classification").ok().as_deref()
            != Some(request.artifact.classification().as_str())
            || row
                .try_get::<Option<String>, _>("verified_media_type")
                .ok()
                .flatten()
                .as_deref()
                != Some(request.artifact.media_type())
            || row
                .try_get::<Option<String>, _>("content_digest")
                .ok()
                .flatten()
                .as_deref()
                != Some(request.artifact.content_digest().as_str())
            || row.try_get::<Option<i64>, _>("size_bytes").ok().flatten()
                != i64::try_from(request.artifact.byte_length()).ok()
            || metadata
                .value
                .get("display_name")
                .and_then(serde_json::Value::as_str)
                != request.artifact.display_name()
        {
            return Err(ArtifactObjectReadAuthorityError::InvalidEvidence);
        }
        let blob_id = parse_id(
            row.try_get("blob_id")
                .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
            "Artifact Blob",
        )
        .map_err(classify_gateway_artifact_read_error)?;
        let storage_binding_digest = parse_digest(
            row.try_get("storage_binding_digest")
                .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
            "Artifact storage binding",
        )
        .map_err(classify_gateway_artifact_read_error)?;
        let encryption_domain_id = parse_id(
            row.try_get("encryption_domain_id")
                .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
            "Artifact encryption domain",
        )
        .map_err(classify_gateway_artifact_read_error)?;
        let ciphertext: Vec<u8> = row
            .try_get("object_reference_ciphertext")
            .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?;
        let authorization_digest: Sha256Digest = insight_platform_contracts::canonical_digest(
            &serde_json::json!({
                "artifact": request.artifact,
                "artifact_version": row.try_get::<i64, _>("artifact_version").map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
                "blob_id": blob_id,
                "blob_version": row.try_get::<i64, _>("blob_version").map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
                "object_reference_ciphertext": ciphertext,
                "principal_binding_generation": principal.binding_generation,
                "principal_permissions_digest": principal.permissions_digest,
                "principal_id": request.principal_id,
                "reference_digest": row.try_get::<String, _>("reference_digest").map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
                "reference_id": row.try_get::<String, _>("reference_id").map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
                "reference_version": row.try_get::<i64, _>("reference_version").map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
                "request_digest": request.request_digest,
                "schema_version": 1,
                "tenant_id": request.tenant_id,
            }),
        )
        .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?
        .parse()
        .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?;
        let authorized = insight_platform_artifacts::AuthorizedArtifactObjectRead {
            tenant_id: request.tenant_id.clone(),
            blob_id,
            artifact: request.artifact.clone(),
            backend: row
                .try_get("backend")
                .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
            storage_binding_digest,
            encryption_domain_id,
            key_id: row
                .try_get("key_id")
                .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
            object_reference_ciphertext: EncryptedArtifactObjectReference::new(ciphertext)
                .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?,
            object_generation: row
                .try_get::<Option<String>, _>("object_generation")
                .map_err(|_| ArtifactObjectReadAuthorityError::InvalidEvidence)?
                .ok_or(ArtifactObjectReadAuthorityError::InvalidEvidence)?,
            authorization_digest,
        };
        authorized.validate()?;
        transaction
            .commit()
            .await
            .map_err(|_| ArtifactObjectReadAuthorityError::Unavailable)?;
        Ok(authorized)
    }
}

fn classify_gateway_artifact_read_error(
    error: RepositoryError,
) -> ArtifactObjectReadAuthorityError {
    match error {
        RepositoryError::Database(_) => ArtifactObjectReadAuthorityError::Unavailable,
        RepositoryError::NotFound(_) => ArtifactObjectReadAuthorityError::NotFound,
        RepositoryError::CorruptRow(_) => ArtifactObjectReadAuthorityError::InvalidEvidence,
        _ => ArtifactObjectReadAuthorityError::Denied,
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactRecoverySlot {
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}

impl ArtifactRecoverySlot {
    fn validate(&self) -> Result<(), RepositoryError> {
        if self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.event_id.to_string() == self.outbox_id.to_string()
        {
            return Err(RepositoryError::InvalidInput(
                "Artifact recovery mutation identity is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactExecutionSlot {
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub duplicate_blob_cleanup_job_id: ResourceId,
    pub receipt_expires_at: DateTime<Utc>,
}

impl ArtifactExecutionSlot {
    fn validate(&self) -> Result<(), RepositoryError> {
        if self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.duplicate_blob_cleanup_job_id.kind() != ResourceKind::Job
        {
            return Err(RepositoryError::InvalidInput(
                "Artifact execution slot identity is invalid".to_owned(),
            ));
        }
        let identities = [
            self.receipt_id.to_string(),
            self.event_id.to_string(),
            self.outbox_id.to_string(),
            self.duplicate_blob_cleanup_job_id.to_string(),
        ];
        if identities.iter().collect::<BTreeSet<_>>().len() != identities.len() {
            return Err(RepositoryError::InvalidInput(
                "Artifact execution slot identities must be unique".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartedArtifactExecution {
    Scan(ArtifactScanExecution),
    Deletion(ArtifactDeletionExecution),
    BlobCleanup(ArtifactBlobCleanupExecution),
}

#[derive(Debug, Clone)]
pub struct DriveExpiredArtifactJobs {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub slots: Vec<ArtifactRecoverySlot>,
}

impl DriveExpiredArtifactJobs {
    fn validate(&self, maximum_batch: u16, maximum_shards: u16) -> Result<(), RepositoryError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Job,
            self.limit,
            self.slots.len(),
            maximum_batch,
            maximum_shards,
        )?;
        let mut identities = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for identity in [&slot.event_id, &slot.outbox_id] {
                if !identities.insert(identity.to_string()) {
                    return Err(RepositoryError::InvalidInput(
                        "Artifact recovery identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveredArtifactJob {
    pub job: JobRecord,
    pub payload_kind: String,
    pub artifact_version: Option<u64>,
    pub blob_version: Option<u64>,
    pub operation_version: Option<u64>,
}

impl PgRepository {
    pub async fn load_gateway_artifact(
        &self,
        tenant_id: ResourceId,
        principal_id: ResourceId,
        principal_kind: PrincipalKind,
        artifact_id: ResourceId,
    ) -> Result<GatewayArtifactSnapshot, RepositoryError> {
        if tenant_id.kind() != ResourceKind::Tenant
            || principal_id.kind() != ResourceKind::Principal
            || artifact_id.kind() != ResourceKind::Artifact
        {
            return Err(RepositoryError::InvalidInput(
                "Artifact Gateway identity is invalid".to_owned(),
            ));
        }
        let mut transaction = begin_read_only_repeatable(self.pool()).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            &tenant_id,
            &principal_id,
            principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::ArtifactRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        let artifact = load_artifact_record(&mut transaction, &tenant_id, &artifact_id).await?;
        let content = if artifact.state == ArtifactState::Ready {
            let row = sqlx::query(
                r#"
                SELECT blob.content_digest, blob.size_bytes
                FROM insight_platform.artifact_blobs AS blob
                WHERE blob.tenant_id = $1 AND blob.blob_id = $2
                  AND blob.state = 'verified' AND blob.deleted_at IS NULL
                  AND EXISTS (
                    SELECT 1 FROM insight_platform.artifact_links AS reference
                    WHERE reference.tenant_id = $1
                      AND reference.target_artifact_id = $3
                      AND reference.link_kind = 'reference'
                      AND reference.state = 'active'
                      AND reference.released_at IS NULL
                      AND (reference.expires_at IS NULL OR reference.expires_at > clock_timestamp())
                  )
                "#,
            )
            .bind(tenant_id.to_string())
            .bind(
                artifact
                    .blob_id
                    .as_ref()
                    .ok_or_else(|| {
                        RepositoryError::CorruptRow("Ready Artifact has no Blob".to_owned())
                    })?
                    .to_string(),
            )
            .bind(artifact_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?;
            row.map(|row| artifact_ref_from_gateway_rows(&artifact, &row))
                .transpose()?
        } else {
            None
        };
        transaction.commit().await?;
        Ok(GatewayArtifactSnapshot { artifact, content })
    }

    pub async fn load_gateway_artifact_deletion_target(
        &self,
        tenant_id: ResourceId,
        principal_id: ResourceId,
        principal_kind: PrincipalKind,
        artifact_id: ResourceId,
    ) -> Result<GatewayArtifactDeletionTarget, RepositoryError> {
        if tenant_id.kind() != ResourceKind::Tenant
            || principal_id.kind() != ResourceKind::Principal
            || artifact_id.kind() != ResourceKind::Artifact
        {
            return Err(RepositoryError::InvalidInput(
                "Artifact Gateway deletion identity is invalid".to_owned(),
            ));
        }
        let mut transaction = begin_read_only_repeatable(self.pool()).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            &tenant_id,
            &principal_id,
            principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::ArtifactDelete) {
            return Err(RepositoryError::PermissionDenied);
        }
        let artifact = load_artifact_record(&mut transaction, &tenant_id, &artifact_id).await?;
        let blob_id = artifact
            .blob_id
            .as_ref()
            .ok_or(RepositoryError::NotFound("Artifact Blob"))?;
        let row = sqlx::query(
            r#"
            SELECT tenant_id, blob_id, backend, storage_binding_digest,
                   security_domain_digest, object_generation, encryption_domain_id,
                   content_digest, size_bytes, state, version
            FROM insight_platform.artifact_blobs
            WHERE tenant_id = $1 AND blob_id = $2
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(blob_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Artifact Blob"))?;
        let blob = blob_from_row(row)?;
        transaction.commit().await?;
        Ok(GatewayArtifactDeletionTarget { artifact, blob })
    }

    pub async fn begin_artifact_transaction(
        &self,
    ) -> Result<PgArtifactTransaction, RepositoryError> {
        Ok(PgArtifactTransaction {
            transaction: self.pool().begin().await?,
            limits: self.artifact_limits(),
        })
    }

    pub async fn load_started_artifact_execution(
        &self,
        role: ArtifactWorkerRole,
        tenant_id: ResourceId,
        job_id: ResourceId,
        fence: JobFence,
        slot: ArtifactExecutionSlot,
    ) -> Result<StartedArtifactExecution, RepositoryError> {
        slot.validate()?;
        if tenant_id.kind() != ResourceKind::Tenant || job_id.kind() != ResourceKind::Job {
            return Err(RepositoryError::InvalidInput(
                "Artifact execution target identity is invalid".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        if slot.receipt_expires_at <= database_now {
            return Err(RepositoryError::InvalidInput(
                "Artifact execution receipt expiry is invalid".to_owned(),
            ));
        }
        let current = load_job_for_update_by_text(
            &mut transaction,
            &tenant_id.to_string(),
            &job_id.to_string(),
        )
        .await?;
        if current.work_class != WorkClass::Artifact.as_str() {
            return Err(RepositoryError::NotFound("Artifact Job"));
        }
        let payload: ArtifactJobPayload = decode_typed_payload(&current.payload, "Artifact Job")?;
        let role_matches = matches!(
            (role, &payload),
            (
                ArtifactWorkerRole::DataWorker,
                ArtifactJobPayload::Scan { .. } | ArtifactJobPayload::Rescan { .. }
            ) | (
                ArtifactWorkerRole::Maintenance,
                ArtifactJobPayload::Delete { .. } | ArtifactJobPayload::BlobCleanup { .. }
            )
        );
        if !role_matches {
            return Err(RepositoryError::PermissionDenied);
        }
        let request_digest: Sha256Digest =
            insight_platform_contracts::canonical_digest(&serde_json::json!({
                "fence": fence,
                "job_id": job_id,
                "operation": "artifact.execute",
                "payload_digest": current.payload.digest,
                "schema_version": 1,
                "tenant_id": tenant_id,
            }))
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            .parse::<Sha256Digest>()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let idempotency_key_digest: Sha256Digest =
            insight_platform_contracts::canonical_digest(&serde_json::json!({
                "job_id": job_id,
                "lease_generation": fence.lease_generation,
                "operation": "artifact.attempt",
                "schema_version": 1,
                "tenant_id": tenant_id,
            }))
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            .parse::<Sha256Digest>()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let audit = ArtifactWorkerAudit {
            tenant_id: tenant_id.clone(),
            worker_process_generation_id: fence.worker_process_generation_id.clone(),
            receipt_id: slot.receipt_id,
            event_id: slot.event_id,
            outbox_id: slot.outbox_id,
            idempotency_key_digest,
            request_digest,
            receipt_expires_at: slot.receipt_expires_at,
        };
        let execution = match payload {
            ArtifactJobPayload::Scan { scan } | ArtifactJobPayload::Rescan { scan } => {
                let loaded = load_artifact_scan_work_inner(
                    &mut transaction,
                    &tenant_id,
                    &scan.artifact_id,
                    &scan.blob_id,
                    &scan.operation_id,
                    &job_id,
                    true,
                )
                .await?;
                require_artifact_job_fence(
                    &loaded,
                    &fence,
                    &audit.worker_process_generation_id,
                    database_now,
                )?;
                StartedArtifactExecution::Scan(ArtifactScanExecution {
                    audit,
                    scan_job_id: job_id,
                    fence,
                    operation_id: loaded.record.operation.operation_id.clone(),
                    artifact_id: loaded.record.artifact.artifact_id.clone(),
                    blob_id: loaded.record.blob.blob_id.clone(),
                    expected_artifact_version: loaded.record.artifact.version,
                    expected_blob_version: loaded.record.blob.version,
                    expected_operation_version: loaded.record.operation.version,
                    scan: loaded.record.scan,
                    duplicate_blob_cleanup_job_id: slot.duplicate_blob_cleanup_job_id,
                })
            }
            ArtifactJobPayload::Delete { deletion } => {
                require_current_job_fence(
                    &current,
                    &fence,
                    &audit.worker_process_generation_id,
                    database_now,
                )?;
                let marked = load_artifact_deletion(
                    &mut transaction,
                    &tenant_id,
                    &deletion.artifact_id,
                    &deletion.blob_id,
                    &deletion.operation_id,
                    &job_id,
                )
                .await?;
                StartedArtifactExecution::Deletion(ArtifactDeletionExecution {
                    audit,
                    deletion_job_id: job_id,
                    fence,
                    expected_artifact_version: marked.artifact.version,
                    expected_blob_version: marked.blob.version,
                    expected_operation_version: marked.deletion.operation_version,
                    deletion,
                })
            }
            ArtifactJobPayload::BlobCleanup { cleanup } => {
                let loaded = lock_blob_cleanup_work(
                    &mut transaction,
                    &tenant_id,
                    &cleanup.discarded_blob_id,
                    &job_id,
                )
                .await?;
                require_raw_artifact_job_fence(
                    &loaded.job_state,
                    loaded.job_version,
                    loaded.lease_epoch,
                    loaded.worker_id.as_deref(),
                    loaded.lease_token_digest.as_deref(),
                    loaded.lease_expires_at,
                    &fence,
                    &audit.worker_process_generation_id,
                    database_now,
                )?;
                StartedArtifactExecution::BlobCleanup(ArtifactBlobCleanupExecution {
                    audit,
                    cleanup_job_id: job_id,
                    fence,
                    expected_blob_version: loaded.blob.version,
                    cleanup,
                })
            }
        };
        transaction.commit().await?;
        Ok(execution)
    }

    pub async fn drive_expired_artifact_jobs(
        &self,
        command: DriveExpiredArtifactJobs,
    ) -> Result<SafetyScanPage<RecoveredArtifactJob>, RepositoryError> {
        command.validate(self.recovery_batch_limit(), self.recovery_shard_limit())?;
        let mut transaction = self.pool().begin().await?;
        let scan_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT job.*, job.lease_expires_at AS scan_sort_at
            FROM insight_platform.jobs AS job
            WHERE job.work_class = 'artifact'
              AND job.state IN ('leased', 'running', 'cancelling')
              AND job.lease_expires_at <= $1 AND job.terminal_at IS NULL
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $4) = $3
              AND (
                  $5::timestamptz IS NULL OR
                  (job.lease_expires_at, job.tenant_id, job.job_id) >
                      ($5::timestamptz, $6::text, $7::text)
              )
            ORDER BY job.lease_expires_at, job.tenant_id, job.job_id
            LIMIT $2
            "#,
        )
        .bind(scan_now)
        .bind(i64::from(command.limit))
        .bind(i64::from(command.shard.index))
        .bind(i64::from(command.shard.count))
        .bind(command.after.as_ref().map(|cursor| cursor.sort_at))
        .bind(
            command
                .after
                .as_ref()
                .map(|cursor| cursor.tenant_id.to_string()),
        )
        .bind(
            command
                .after
                .as_ref()
                .map(|cursor| cursor.item_id.to_string()),
        )
        .fetch_all(&mut *transaction)
        .await?;
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| safety_scan_cursor_from_row(row, "job_id", ResourceKind::Job))
            .transpose()?;
        let candidates = rows
            .into_iter()
            .map(job_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let mut recovered = Vec::with_capacity(candidates.len());
        for (observed, slot) in candidates.into_iter().zip(command.slots.iter()) {
            let tenant_id: ResourceId = observed.tenant_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?;
            let job_id: ResourceId = observed.job_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?;
            let current = load_job_for_update_by_text(
                &mut transaction,
                &observed.tenant_id,
                &observed.job_id,
            )
            .await?;
            if current.version != observed.version
                || current.state != observed.state
                || current.lease_epoch != observed.lease_epoch
                || current.lease_expires_at != observed.lease_expires_at
                || current.payload.digest != observed.payload.digest
            {
                return Err(RepositoryError::Conflict("expired Artifact Job"));
            }
            let payload: ArtifactJobPayload =
                decode_typed_payload(&current.payload, "Artifact Job")?;
            let owner_id: ResourceId = current.owner_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?;
            payload
                .validate_for_owner(&owner_id)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let parents = lock_artifact_recovery_parents(
                &mut transaction,
                &tenant_id,
                &job_id,
                &current,
                &payload,
            )
            .await?;
            let recovery_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
                .fetch_one(&mut *transaction)
                .await?;
            let projection = job_projection(&current)?;
            let decision = decide_expired_artifact_attempt(
                &projection,
                &payload,
                u64::try_from(current.version).map_err(|_| {
                    RepositoryError::CorruptRow("negative Artifact Job version".to_owned())
                })?,
                u64::try_from(current.lease_epoch).map_err(|_| {
                    RepositoryError::CorruptRow("negative Artifact Job lease generation".to_owned())
                })?,
                recovery_now,
            )?;
            let mut parent_versions = persist_artifact_recovery_parent_action(
                &mut transaction,
                &payload,
                &parents,
                decision.parent_action,
                recovery_now,
            )
            .await?;
            let job = persist_recovered_artifact_job(
                &mut transaction,
                &current,
                &decision.job,
                recovery_now,
                None,
            )
            .await?;
            if parents.operation_id.as_ref() == Some(&job_id) {
                parent_versions.operation_version =
                    Some(parse_u64(job.version, "Artifact Operation Job version")?);
            }
            append_scheduler_event(
                &mut transaction,
                &job.tenant_id,
                &slot.event_id,
                &slot.outbox_id,
                "job",
                &job.job_id,
                job.version,
                None,
                artifact_recovery_event_type(&job.state)?,
                &TypedPayload::new(
                    1,
                    &serde_json::json!({
                        "artifact_version": parent_versions.artifact_version,
                        "blob_version": parent_versions.blob_version,
                        "job_state": job.state,
                        "lease_generation": current.lease_epoch,
                        "operation_version": parent_versions.operation_version,
                        "owner_id": current.owner_id,
                        "owner_kind": current.owner_kind,
                        "payload_kind": payload.kind_name(),
                    }),
                )?,
            )
            .await?;
            recovered.push(RecoveredArtifactJob {
                job,
                payload_kind: payload.kind_name().to_owned(),
                artifact_version: parent_versions.artifact_version,
                blob_version: parent_versions.blob_version,
                operation_version: parent_versions.operation_version,
            });
        }
        transaction.commit().await?;
        Ok(safety_scan_page(
            recovered,
            scanned_count,
            command.limit,
            last_cursor,
        ))
    }
}

impl ArtifactStore for PgRepository {
    type Error = RepositoryError;
    type Transaction<'a>
        = PgArtifactTransaction
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        self.begin_artifact_transaction().await
    }
}

impl ArtifactWorkAuthority for PgRepository {
    type Error = RepositoryError;

    async fn commit_attempt_failure(
        &self,
        command: CommitArtifactAttemptFailure,
    ) -> Result<CommandOutcome<()>, Self::Error> {
        commit_artifact_attempt_failure(self, command).await
    }

    async fn commit_scan_outcome(
        &self,
        command: CommitArtifactScanOutcome,
    ) -> Result<CommandOutcome<ArtifactScanWorkRecord>, Self::Error> {
        let mut transaction = self.begin_artifact_transaction().await?;
        let outcome = ArtifactTransaction::commit_scan_outcome(&mut transaction, command).await?;
        ArtifactTransaction::commit(transaction).await?;
        Ok(outcome)
    }

    async fn commit_blob_cleanup(
        &self,
        command: CommitArtifactBlobCleanup,
    ) -> Result<CommandOutcome<CompletedArtifactBlobCleanup>, Self::Error> {
        let mut transaction = self.begin_artifact_transaction().await?;
        let outcome = ArtifactTransaction::commit_blob_cleanup(&mut transaction, command).await?;
        ArtifactTransaction::commit(transaction).await?;
        Ok(outcome)
    }

    async fn commit_deletion(
        &self,
        command: CompleteArtifactDeletion,
    ) -> Result<CommandOutcome<CompletedArtifactDeletion>, Self::Error> {
        let mut transaction = self.begin_artifact_transaction().await?;
        let outcome = ArtifactTransaction::complete_deletion(&mut transaction, command).await?;
        ArtifactTransaction::commit(transaction).await?;
        Ok(outcome)
    }
}

#[async_trait::async_trait]
impl ArtifactScanObjectReadAuthority<ArtifactScanRequest> for PgRepository {
    async fn authorize_scan_object_read(
        &self,
        request: &ArtifactScanRequest,
    ) -> Result<AuthorizedArtifactScanObjectRead, ArtifactObjectReadAuthorityError> {
        authorize_artifact_scan_object(self, request)
            .await
            .map_err(map_scan_object_authority_error)
    }
}

async fn authorize_artifact_scan_object(
    repository: &PgRepository,
    request: &ArtifactScanRequest,
) -> Result<AuthorizedArtifactScanObjectRead, RepositoryError> {
    if request.tenant_id.kind() != ResourceKind::Tenant
        || request.job_id.kind() != ResourceKind::Job
    {
        return Err(RepositoryError::PermissionDenied);
    }
    request
        .job
        .validate()
        .map_err(|_| RepositoryError::PermissionDenied)?;
    let mut transaction = begin_read_only_repeatable(repository.pool()).await?;
    let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *transaction)
        .await?;
    let row = sqlx::query(
        r#"
        SELECT
            job.state AS job_state, job.version AS job_version,
            job.lease_epoch, job.worker_id, job.lease_token_digest, job.lease_expires_at,
            job.owner_kind, job.owner_id, job.invocation_id,
            job.payload_schema_version AS job_payload_schema_version,
            job.payload AS job_payload, job.payload_digest AS job_payload_digest,
            artifact.state AS artifact_state, artifact.version AS artifact_version,
            artifact.expected_size_bytes, artifact.expected_digest,
            artifact.declared_media_type,
            blob.backend, blob.storage_binding_digest, blob.object_reference_ciphertext,
            blob.key_id, blob.encryption_domain_id, blob.object_generation,
            blob.state AS blob_state, blob.version AS blob_version
        FROM insight_platform.jobs AS job
        JOIN insight_platform.artifacts AS artifact
          ON artifact.tenant_id = job.tenant_id AND artifact.artifact_id = $3
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
        WHERE job.tenant_id = $1 AND job.job_id = $2 AND job.work_class = 'artifact'
        "#,
    )
    .bind(request.tenant_id.to_string())
    .bind(request.job_id.to_string())
    .bind(request.job.artifact_id.to_string())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact scan object authority"))?;
    let typed = payload_from_row(
        &row,
        "job_payload_schema_version",
        "job_payload",
        "job_payload_digest",
    )?;
    let payload: ArtifactJobPayload = decode_typed_payload(&typed, "Artifact Job")?;
    let persisted_scan = match payload {
        ArtifactJobPayload::Scan { scan } | ArtifactJobPayload::Rescan { scan } => scan,
        _ => return Err(RepositoryError::PermissionDenied),
    };
    let state = row
        .try_get::<String, _>("job_state")?
        .parse::<JobState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    require_raw_artifact_job_fence(
        &state,
        parse_u64(row.try_get("job_version")?, "Artifact scan Job version")?,
        parse_u64(row.try_get("lease_epoch")?, "Artifact scan lease epoch")?,
        row.try_get::<Option<String>, _>("worker_id")?.as_deref(),
        row.try_get::<Option<String>, _>("lease_token_digest")?
            .as_deref(),
        row.try_get("lease_expires_at")?,
        &request.fence,
        &request.fence.worker_process_generation_id,
        database_now,
    )?;
    let expected_artifact_state = match request.job.scan_kind {
        ArtifactScanKind::Initial => ArtifactState::Verifying,
        ArtifactScanKind::Rescan => ArtifactState::Quarantined,
    };
    let expected_blob_state = match request.job.scan_kind {
        ArtifactScanKind::Initial => BlobIntegrityState::Staging,
        ArtifactScanKind::Rescan => BlobIntegrityState::Verified,
    };
    let valid = persisted_scan == request.job
        && row.try_get::<String, _>("owner_kind")? == "job"
        && row.try_get::<String, _>("owner_id")? == request.job.operation_id.to_string()
        && row.try_get::<Option<String>, _>("invocation_id")?.is_none()
        && row
            .try_get::<String, _>("artifact_state")?
            .parse::<ArtifactState>()
            .is_ok_and(|value| value == expected_artifact_state)
        && parse_u64(
            row.try_get("artifact_version")?,
            "Artifact scan Artifact version",
        )? == request.job.expected_artifact_version
        && row
            .try_get::<String, _>("blob_state")?
            .parse::<BlobIntegrityState>()
            .is_ok_and(|value| value == expected_blob_state)
        && parse_u64(row.try_get("blob_version")?, "Artifact scan Blob version")?
            == request.job.expected_blob_version
        && row
            .try_get::<Option<String>, _>("object_generation")?
            .as_deref()
            == Some(request.job.object_generation.as_str());
    if !valid {
        return Err(RepositoryError::PermissionDenied);
    }
    let expected_size = parse_u64(
        row.try_get("expected_size_bytes")?,
        "Artifact expected scan bytes",
    )?;
    let authorization_digest: Sha256Digest =
        insight_platform_contracts::canonical_digest(&serde_json::json!({
            "artifact_id": request.job.artifact_id,
            "blob_id": request.job.blob_id,
            "fence": request.fence,
            "job_id": request.job_id,
            "job_payload_digest": typed.digest,
            "object_generation": request.job.object_generation,
            "schema_version": 1,
            "tenant_id": request.tenant_id,
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse::<Sha256Digest>()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let projection = AuthorizedArtifactScanObjectRead {
        tenant_id: request.tenant_id.clone(),
        artifact_id: request.job.artifact_id.clone(),
        blob_id: request.job.blob_id.clone(),
        backend: row.try_get("backend")?,
        storage_binding_digest: row
            .try_get::<String, _>("storage_binding_digest")?
            .parse::<Sha256Digest>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        encryption_domain_id: row
            .try_get::<String, _>("encryption_domain_id")?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?,
        key_id: row.try_get("key_id")?,
        object_reference_ciphertext: EncryptedArtifactObjectReference::new(
            row.try_get("object_reference_ciphertext")?,
        )
        .map_err(|_| RepositoryError::CorruptRow("Artifact object ciphertext".to_owned()))?,
        object_generation: request.job.object_generation.clone(),
        maximum_bytes: expected_size,
        expected_digest: row
            .try_get::<Option<String>, _>("expected_digest")?
            .map(|value| value.parse::<Sha256Digest>())
            .transpose()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        declared_media_type: row.try_get("declared_media_type")?,
        authorization_digest,
    };
    projection
        .validate()
        .map_err(|_| RepositoryError::CorruptRow("Artifact scan projection".to_owned()))?;
    transaction.commit().await?;
    Ok(projection)
}

fn map_scan_object_authority_error(error: RepositoryError) -> ArtifactObjectReadAuthorityError {
    match error {
        RepositoryError::Database(_) => ArtifactObjectReadAuthorityError::Unavailable,
        RepositoryError::NotFound(_) => ArtifactObjectReadAuthorityError::NotFound,
        RepositoryError::PermissionDenied | RepositoryError::StaleFence => {
            ArtifactObjectReadAuthorityError::Denied
        }
        _ => ArtifactObjectReadAuthorityError::InvalidEvidence,
    }
}

#[async_trait::async_trait]
impl ArtifactDeleteObjectAuthority<DeleteArtifactBlobGeneration> for PgRepository {
    async fn authorize_delete_object(
        &self,
        request: &DeleteArtifactBlobGeneration,
    ) -> Result<AuthorizedArtifactDeleteObject, ArtifactObjectReadAuthorityError> {
        authorize_artifact_delete_object(self, request)
            .await
            .map_err(map_scan_object_authority_error)
    }
}

async fn authorize_artifact_delete_object(
    repository: &PgRepository,
    request: &DeleteArtifactBlobGeneration,
) -> Result<AuthorizedArtifactDeleteObject, RepositoryError> {
    if request.tenant_id.kind() != ResourceKind::Tenant
        || request.job_id.kind() != ResourceKind::Job
        || request.blob_id.kind() != ResourceKind::InternalBlob
        || request.object_generation.is_empty()
    {
        return Err(RepositoryError::PermissionDenied);
    }
    let mut transaction = begin_read_only_repeatable(repository.pool()).await?;
    let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *transaction)
        .await?;
    let row = sqlx::query(
        r#"
        SELECT
            job.state AS job_state, job.version AS job_version,
            job.lease_epoch, job.worker_id, job.lease_token_digest, job.lease_expires_at,
            job.owner_id,
            job.payload_schema_version AS job_payload_schema_version,
            job.payload AS job_payload, job.payload_digest AS job_payload_digest,
            blob.backend, blob.storage_binding_digest, blob.object_reference_ciphertext,
            blob.key_id, blob.encryption_domain_id, blob.object_generation,
            blob.state AS blob_state, blob.version AS blob_version
        FROM insight_platform.jobs AS job
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = job.tenant_id AND blob.blob_id = $3
        WHERE job.tenant_id = $1 AND job.job_id = $2 AND job.work_class = 'artifact'
        "#,
    )
    .bind(request.tenant_id.to_string())
    .bind(request.job_id.to_string())
    .bind(request.blob_id.to_string())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "Artifact deletion object authority",
    ))?;
    let typed = payload_from_row(
        &row,
        "job_payload_schema_version",
        "job_payload",
        "job_payload_digest",
    )?;
    let payload: ArtifactJobPayload = decode_typed_payload(&typed, "Artifact Job")?;
    let (payload_blob_id, payload_generation, payload_blob_version) = match &payload {
        ArtifactJobPayload::Delete { deletion } => match &deletion.mode {
            insight_platform_artifacts::ArtifactDeletionMode::BlobGeneration {
                object_generation,
            } => (
                &deletion.blob_id,
                object_generation,
                deletion.expected_blob_version,
            ),
            insight_platform_artifacts::ArtifactDeletionMode::ArtifactOnly { .. } => {
                return Err(RepositoryError::PermissionDenied)
            }
        },
        ArtifactJobPayload::BlobCleanup { cleanup } => (
            &cleanup.discarded_blob_id,
            &cleanup.object_generation,
            cleanup.expected_blob_version,
        ),
        _ => return Err(RepositoryError::PermissionDenied),
    };
    let owner_id: ResourceId = row.try_get::<String, _>("owner_id")?.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    payload
        .validate_for_owner(&owner_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let state = row
        .try_get::<String, _>("job_state")?
        .parse::<JobState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    require_raw_artifact_job_fence(
        &state,
        parse_u64(row.try_get("job_version")?, "Artifact deletion Job version")?,
        parse_u64(row.try_get("lease_epoch")?, "Artifact deletion lease epoch")?,
        row.try_get::<Option<String>, _>("worker_id")?.as_deref(),
        row.try_get::<Option<String>, _>("lease_token_digest")?
            .as_deref(),
        row.try_get("lease_expires_at")?,
        &request.fence,
        &request.fence.worker_process_generation_id,
        database_now,
    )?;
    let valid = payload_blob_id == &request.blob_id
        && payload_generation == &request.object_generation
        && row
            .try_get::<String, _>("blob_state")?
            .parse::<BlobIntegrityState>()
            .is_ok_and(|value| value == BlobIntegrityState::Deleting)
        && parse_u64(
            row.try_get("blob_version")?,
            "Artifact deletion Blob version",
        )? == payload_blob_version
        && row
            .try_get::<Option<String>, _>("object_generation")?
            .as_deref()
            == Some(request.object_generation.as_str());
    if !valid {
        return Err(RepositoryError::PermissionDenied);
    }
    let authorization_digest: Sha256Digest =
        insight_platform_contracts::canonical_digest(&serde_json::json!({
            "blob_id": request.blob_id,
            "fence": request.fence,
            "job_id": request.job_id,
            "job_payload_digest": typed.digest,
            "object_generation": request.object_generation,
            "operation": "artifact.delete_generation",
            "schema_version": 1,
            "tenant_id": request.tenant_id,
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse::<Sha256Digest>()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let projection = AuthorizedArtifactDeleteObject {
        tenant_id: request.tenant_id.clone(),
        blob_id: request.blob_id.clone(),
        backend: row.try_get("backend")?,
        storage_binding_digest: row
            .try_get::<String, _>("storage_binding_digest")?
            .parse::<Sha256Digest>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        encryption_domain_id: row
            .try_get::<String, _>("encryption_domain_id")?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?,
        key_id: row.try_get("key_id")?,
        object_reference_ciphertext: EncryptedArtifactObjectReference::new(
            row.try_get("object_reference_ciphertext")?,
        )
        .map_err(|_| RepositoryError::CorruptRow("Artifact object ciphertext".to_owned()))?,
        object_generation: request.object_generation.clone(),
        authorization_digest,
    };
    projection
        .validate()
        .map_err(|_| RepositoryError::CorruptRow("Artifact deletion projection".to_owned()))?;
    transaction.commit().await?;
    Ok(projection)
}

async fn commit_artifact_attempt_failure(
    repository: &PgRepository,
    command: CommitArtifactAttemptFailure,
) -> Result<CommandOutcome<()>, RepositoryError> {
    command.validate_at(Utc::now())?;
    let mut transaction = repository.pool().begin().await?;
    let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *transaction)
        .await?;
    command.validate_at(database_now)?;
    let receipt_payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "failure": command.failure,
            "fence": {
                "expected_version": command.fence.expected_version,
                "lease_generation": command.fence.lease_generation,
                "lease_token_digest": command.fence.token_digest,
                "worker_process_generation_id": command.fence.worker_process_generation_id,
            },
            "job_id": command.job_id,
        }),
        65_536,
    )?;
    if claim_artifact_worker_receipt(
        &mut transaction,
        &command.audit,
        &command.job_id,
        "artifact.attempt.fail",
        &receipt_payload,
    )
    .await?
    {
        transaction.commit().await?;
        return Ok(CommandOutcome::Replayed(()));
    }
    let current = load_job_for_update_by_text(
        &mut transaction,
        &command.audit.tenant_id.to_string(),
        &command.job_id.to_string(),
    )
    .await?;
    let payload: ArtifactJobPayload = decode_typed_payload(&current.payload, "Artifact Job")?;
    let owner_id: ResourceId = current.owner_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    payload
        .validate_for_owner(&owner_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let parents = lock_artifact_recovery_parents(
        &mut transaction,
        &command.audit.tenant_id,
        &command.job_id,
        &current,
        &payload,
    )
    .await?;
    let decision = decide_artifact_backend_failure(
        &job_projection(&current)?,
        &payload,
        &command,
        database_now,
    )?;
    let mut parent_versions = persist_artifact_recovery_parent_action(
        &mut transaction,
        &payload,
        &parents,
        decision.parent_action,
        database_now,
    )
    .await?;
    let job = persist_recovered_artifact_job(
        &mut transaction,
        &current,
        &decision.job,
        database_now,
        Some(&command.fence),
    )
    .await?;
    if parents.operation_id.as_ref() == Some(&command.job_id) {
        parent_versions.operation_version =
            Some(parse_u64(job.version, "Artifact Operation Job version")?);
    }
    append_artifact_worker_event(
        &mut transaction,
        &command.audit,
        "job",
        &job.job_id,
        job.version,
        artifact_recovery_event_type(&job.state)?,
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "artifact_version": parent_versions.artifact_version,
                "blob_version": parent_versions.blob_version,
                "job_state": job.state,
                "operation_version": parent_versions.operation_version,
                "payload_kind": payload.kind_name(),
                "reason_class": command.failure.reason_class,
                "retryable": command.failure.retryable,
            }),
        )?,
    )
    .await?;
    terminalize_artifact_worker_receipt(
        &mut transaction,
        &command.audit,
        &command.job_id,
        &job.state,
        &command.job_id,
    )
    .await?;
    transaction.commit().await?;
    Ok(CommandOutcome::Applied(()))
}

#[derive(Debug, Clone)]
struct LockedArtifactRecoveryParents {
    tenant_id: ResourceId,
    artifact_id: Option<ResourceId>,
    artifact_state: Option<ArtifactState>,
    artifact_version: Option<u64>,
    blob_state: BlobIntegrityState,
    blob_version: u64,
    operation_id: Option<ResourceId>,
    operation_state: Option<JobState>,
    operation_version: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct ArtifactRecoveryParentVersions {
    artifact_version: Option<u64>,
    blob_version: Option<u64>,
    operation_version: Option<u64>,
}

async fn lock_artifact_recovery_parents(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    current_job: &JobRecord,
    payload: &ArtifactJobPayload,
) -> Result<LockedArtifactRecoveryParents, RepositoryError> {
    match payload {
        ArtifactJobPayload::Scan { scan } | ArtifactJobPayload::Rescan { scan } => {
            let _ = load_artifact_scan_work_inner(
                transaction,
                tenant_id,
                &scan.artifact_id,
                &scan.blob_id,
                &scan.operation_id,
                job_id,
                true,
            )
            .await?;
            lock_artifact_and_blob(transaction, tenant_id, &scan.artifact_id, &scan.blob_id)
                .await?;
            let locked = load_artifact_scan_work_inner(
                transaction,
                tenant_id,
                &scan.artifact_id,
                &scan.blob_id,
                &scan.operation_id,
                job_id,
                true,
            )
            .await?;
            if locked.record.scan_job_version
                != u64::try_from(current_job.version).map_err(|_| {
                    RepositoryError::CorruptRow("negative Artifact Job version".to_owned())
                })?
                || locked.lease_epoch
                    != u64::try_from(current_job.lease_epoch).map_err(|_| {
                        RepositoryError::CorruptRow(
                            "negative Artifact Job lease generation".to_owned(),
                        )
                    })?
                || locked.record.operation.state != JobState::Running
                || locked.record.artifact.state
                    != match scan.scan_kind {
                        ArtifactScanKind::Initial => ArtifactState::Verifying,
                        ArtifactScanKind::Rescan => ArtifactState::Quarantined,
                    }
                || locked.record.blob.object_generation.as_deref()
                    != Some(scan.object_generation.as_str())
                || locked.record.blob.state
                    != match scan.scan_kind {
                        ArtifactScanKind::Initial => BlobIntegrityState::Staging,
                        ArtifactScanKind::Rescan => BlobIntegrityState::Verified,
                    }
            {
                return Err(RepositoryError::Conflict(
                    "expired Artifact scan parent authority",
                ));
            }
            Ok(LockedArtifactRecoveryParents {
                tenant_id: tenant_id.clone(),
                artifact_id: Some(locked.record.artifact.artifact_id),
                artifact_state: Some(locked.record.artifact.state),
                artifact_version: Some(locked.record.artifact.version),
                blob_state: locked.record.blob.state,
                blob_version: locked.record.blob.version,
                operation_id: Some(locked.record.operation.operation_id),
                operation_state: Some(locked.record.operation.state),
                operation_version: Some(locked.record.operation.version),
            })
        }
        ArtifactJobPayload::Delete { deletion } => {
            let (blob, _) =
                lock_blob_and_aliases(transaction, tenant_id, &deletion.blob_id).await?;
            sqlx::query(
                r#"
                SELECT job_id FROM insight_platform.jobs
                WHERE tenant_id = $1 AND job_id = $2
                  AND work_class = 'artifact'
                  AND owner_kind = 'artifact' AND owner_id = $3
                FOR UPDATE
                "#,
            )
            .bind(tenant_id.to_string())
            .bind(deletion.operation_id.to_string())
            .bind(deletion.artifact_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::NotFound("Artifact deletion Operation Job"))?;
            let marked = load_artifact_deletion(
                transaction,
                tenant_id,
                &deletion.artifact_id,
                &deletion.blob_id,
                &deletion.operation_id,
                job_id,
            )
            .await?;
            if marked.deletion.mode != deletion.mode
                || marked.artifact.version != deletion.expected_artifact_version
                || marked.blob.version != deletion.expected_blob_version
                || marked.artifact.state != ArtifactState::Deleting
                || marked.deletion.operation_state != JobState::Running
                || blob.version != marked.blob.version
                || blob.state != marked.blob.state
            {
                return Err(RepositoryError::Conflict(
                    "expired Artifact deletion parent authority",
                ));
            }
            match &deletion.mode {
                insight_platform_artifacts::ArtifactDeletionMode::ArtifactOnly { .. } => {
                    if blob.state == BlobIntegrityState::Deleting {
                        return Err(RepositoryError::Conflict(
                            "Artifact-only deletion unexpectedly owns Blob deletion",
                        ));
                    }
                }
                insight_platform_artifacts::ArtifactDeletionMode::BlobGeneration {
                    object_generation,
                } => {
                    if blob.state != BlobIntegrityState::Deleting
                        || blob.object_generation.as_deref() != Some(object_generation.as_str())
                    {
                        return Err(RepositoryError::Conflict(
                            "physical Artifact deletion Blob authority",
                        ));
                    }
                }
            }
            Ok(LockedArtifactRecoveryParents {
                tenant_id: tenant_id.clone(),
                artifact_id: Some(marked.artifact.artifact_id),
                artifact_state: Some(marked.artifact.state),
                artifact_version: Some(marked.artifact.version),
                blob_state: marked.blob.state,
                blob_version: marked.blob.version,
                operation_id: Some(marked.deletion.operation_id),
                operation_state: Some(marked.deletion.operation_state),
                operation_version: Some(marked.deletion.operation_version),
            })
        }
        ArtifactJobPayload::BlobCleanup { cleanup } => {
            let locked =
                lock_blob_cleanup_work(transaction, tenant_id, &cleanup.discarded_blob_id, job_id)
                    .await?;
            if locked.cleanup != *cleanup
                || locked.job_version
                    != u64::try_from(current_job.version).map_err(|_| {
                        RepositoryError::CorruptRow("negative Artifact Job version".to_owned())
                    })?
                || locked.lease_epoch
                    != u64::try_from(current_job.lease_epoch).map_err(|_| {
                        RepositoryError::CorruptRow(
                            "negative Artifact Job lease generation".to_owned(),
                        )
                    })?
                || locked.blob.version != cleanup.expected_blob_version
                || locked.blob.state != BlobIntegrityState::Deleting
                || locked.blob.object_generation.as_deref()
                    != Some(cleanup.object_generation.as_str())
            {
                return Err(RepositoryError::Conflict(
                    "expired Artifact Blob cleanup parent authority",
                ));
            }
            Ok(LockedArtifactRecoveryParents {
                tenant_id: tenant_id.clone(),
                artifact_id: None,
                artifact_state: None,
                artifact_version: None,
                blob_state: locked.blob.state,
                blob_version: locked.blob.version,
                operation_id: None,
                operation_state: None,
                operation_version: None,
            })
        }
    }
}

async fn persist_artifact_recovery_parent_action(
    transaction: &mut Transaction<'_, Postgres>,
    payload: &ArtifactJobPayload,
    parents: &LockedArtifactRecoveryParents,
    action: ArtifactRecoveryParentAction,
    database_now: DateTime<Utc>,
) -> Result<ArtifactRecoveryParentVersions, RepositoryError> {
    let mut artifact_version = parents.artifact_version;
    let operation_version = parents.operation_version;
    match action {
        ArtifactRecoveryParentAction::None => {}
        ArtifactRecoveryParentAction::Scan {
            artifact_state,
            operation_state,
        } => {
            if !matches!(
                payload,
                ArtifactJobPayload::Scan { .. } | ArtifactJobPayload::Rescan { .. }
            ) || artifact_state != ArtifactState::Quarantined
                || parents.operation_state != Some(JobState::Running)
            {
                return Err(RepositoryError::InvalidInput(
                    "Artifact scan recovery parent decision is invalid".to_owned(),
                ));
            }
            let artifact_id = parents
                .artifact_id
                .as_ref()
                .ok_or_else(|| RepositoryError::CorruptRow("missing Artifact parent".to_owned()))?;
            let current_artifact_state = parents.artifact_state.ok_or_else(|| {
                RepositoryError::CorruptRow("missing Artifact parent state".to_owned())
            })?;
            let current_artifact_version = parents.artifact_version.ok_or_else(|| {
                RepositoryError::CorruptRow("missing Artifact parent version".to_owned())
            })?;
            if current_artifact_state != artifact_state {
                if !current_artifact_state.can_transition_to(artifact_state) {
                    return Err(RepositoryError::Conflict(
                        "Artifact scan recovery transition",
                    ));
                }
                let next = current_artifact_version.checked_add(1).ok_or_else(|| {
                    RepositoryError::InvalidInput("Artifact version overflowed".to_owned())
                })?;
                ensure_one(
                    sqlx::query(
                        r#"
                        UPDATE insight_platform.artifacts
                        SET state = $4, version = $5, updated_at = $6
                        WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
                        "#,
                    )
                    .bind(parents.tenant_id.to_string())
                    .bind(artifact_id.to_string())
                    .bind(to_i64(current_artifact_version, "Artifact version")?)
                    .bind(artifact_state.as_str())
                    .bind(to_i64(next, "Artifact version")?)
                    .bind(database_now)
                    .execute(&mut **transaction)
                    .await?
                    .rows_affected(),
                    "Artifact scan recovery",
                )?;
                artifact_version = Some(next);
            }
            if operation_state != JobState::Failed {
                return Err(RepositoryError::InvalidInput(
                    "Artifact scan recovery Operation decision is invalid".to_owned(),
                ));
            }
        }
        ArtifactRecoveryParentAction::Deletion { operation_state } => {
            if !matches!(payload, ArtifactJobPayload::Delete { .. })
                || parents.artifact_state != Some(ArtifactState::Deleting)
                || parents.operation_state != Some(JobState::Running)
            {
                return Err(RepositoryError::InvalidInput(
                    "Artifact deletion recovery parent decision is invalid".to_owned(),
                ));
            }
            if operation_state != JobState::Failed {
                return Err(RepositoryError::InvalidInput(
                    "Artifact deletion recovery Operation decision is invalid".to_owned(),
                ));
            }
        }
        ArtifactRecoveryParentAction::BlobCleanupReconciliation => {
            if !matches!(payload, ArtifactJobPayload::BlobCleanup { .. })
                || parents.blob_state != BlobIntegrityState::Deleting
            {
                return Err(RepositoryError::InvalidInput(
                    "Artifact Blob cleanup recovery parent decision is invalid".to_owned(),
                ));
            }
        }
    }
    Ok(ArtifactRecoveryParentVersions {
        artifact_version,
        blob_version: Some(parents.blob_version),
        operation_version,
    })
}

async fn persist_recovered_artifact_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    next: &insight_platform_jobs::JobProjection,
    database_now: DateTime<Utc>,
    required_fence: Option<&JobFence>,
) -> Result<JobRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $6, version = $7, worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL, scheduled_at = $8,
            retry_at = $9,
            started_at = CASE WHEN $6 IN ('ready', 'retry_scheduled') THEN NULL ELSE started_at END,
            terminal_at = CASE WHEN $6 IN ('failed', 'timed_out') THEN $10 ELSE NULL END,
            updated_at = $10
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND state = $4 AND lease_epoch = $5
          AND ($11::text IS NULL OR worker_id = $11)
          AND ($12::text IS NULL OR lease_token_digest = $12)
        RETURNING *
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.job_id)
    .bind(current.version)
    .bind(&current.state)
    .bind(current.lease_epoch)
    .bind(next.state.as_str())
    .bind(to_i64(next.version, "Artifact Job version")?)
    .bind(next.scheduled_at)
    .bind(next.retry_at)
    .bind(database_now)
    .bind(required_fence.map(|fence| fence.worker_process_generation_id.to_string()))
    .bind(required_fence.map(|fence| fence.token_digest.to_string()))
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("expired Artifact Job"))?;
    job_from_row(row)
}

fn artifact_recovery_event_type(state: &str) -> Result<&'static str, RepositoryError> {
    match state {
        "ready" => Ok("artifact.job_lease_released"),
        "retry_scheduled" => Ok("artifact.retry_scheduled"),
        "reconciliation_required" => Ok("artifact.reconciliation_required"),
        "failed" => Ok("artifact.failed"),
        "timed_out" => Ok("artifact.timed_out"),
        _ => Err(RepositoryError::CorruptRow(
            "Artifact recovery produced an unsupported Job state".to_owned(),
        )),
    }
}

impl ArtifactTransaction for PgArtifactTransaction {
    type Error = RepositoryError;

    async fn prepare_artifact(
        &mut self,
        command: PrepareArtifact,
    ) -> Result<CommandOutcome<PreparedArtifact>, Self::Error> {
        command.validate_at(Utc::now(), self.limits)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now, self.limits)?;

        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            "artifact.prepare",
        )
        .await?
        {
            let prepared = load_artifact_bundle(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_id,
                &command.blob_id,
                &command.upload_grant_id,
                &command.operation_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(prepared));
        }

        require_tenant_permission(&mut transaction, &command.audit, Permission::ArtifactWrite)
            .await?;
        require_retention_policy(
            &mut transaction,
            &command.audit.tenant_id,
            &command.retention_policy_revision_id,
            command.retain_until,
            database_now,
        )
        .await?;
        reserve_staging_quota(&mut transaction, &command).await?;

        let metadata = command.metadata_snapshot()?;
        let operation = command.operation_snapshot();
        let grant = command.upload_grant_snapshot()?;
        let security_domain_digest = command.blob_security_domain().canonical_digest()?;
        let metadata_payload = TypedPayload::from_versioned(1, &metadata, 262_144)?;
        let operation_payload = TypedPayload::from_versioned(1, &operation, 1_048_576)?;
        let grant_payload = TypedPayload::from_versioned(1, &grant, 262_144)?;
        let expected_size = i64::try_from(command.expected_size_bytes).map_err(|_| {
            RepositoryError::InvalidInput("Artifact size exceeds PostgreSQL bigint".to_owned())
        })?;

        sqlx::query(
            r#"
            INSERT INTO insight_platform.artifact_blobs (
                tenant_id, blob_id, backend, storage_binding_digest,
                security_domain_digest, object_reference_ciphertext, key_id,
                encryption_domain_id, state
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'staging')
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.blob_id.to_string())
        .bind(&command.storage_backend)
        .bind(command.storage_binding_digest.to_string())
        .bind(security_domain_digest.to_string())
        .bind(&command.object_reference_ciphertext)
        .bind(&command.key_id)
        .bind(command.encryption_domain_id.to_string())
        .execute(&mut *transaction)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO insight_platform.artifacts (
                tenant_id, artifact_id, blob_id, purpose, classification,
                expected_size_bytes, expected_digest, declared_media_type,
                verified_media_type, state, metadata_schema_version, metadata,
                metadata_digest, retention_policy_revision_id, retain_until, created_by
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, 'staging',
                      $9, $10, $11, $12, $13, $14)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.artifact_id.to_string())
        .bind(command.blob_id.to_string())
        .bind(command.purpose.as_str())
        .bind(command.classification.as_str())
        .bind(expected_size)
        .bind(command.expected_digest.as_ref().map(ToString::to_string))
        .bind(&command.declared_media_type)
        .bind(metadata_payload.schema_version)
        .bind(&metadata_payload.value)
        .bind(&metadata_payload.digest)
        .bind(command.retention_policy_revision_id.to_string())
        .bind(command.retain_until)
        .bind(command.audit.principal_id.to_string())
        .execute(&mut *transaction)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, work_class, owner_kind, owner_id, state,
                attempt_limit, scheduled_at, deadline, request_digest,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, 'artifact', 'artifact', $3, 'waiting',
                      1, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(command.artifact_id.to_string())
        .bind(database_now)
        .bind(command.operation_deadline)
        .bind(command.audit.request_digest.to_string())
        .bind(operation_payload.schema_version)
        .bind(&operation_payload.value)
        .bind(&operation_payload.digest)
        .execute(&mut *transaction)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO insight_platform.artifact_links (
                tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
                target_artifact_id, link_key_digest, state, payload_schema_version,
                payload, payload_digest, expires_at
            ) VALUES ($1, $2, 'grant', 'job', $3,
                      $4, $5, 'active', $6, $7, $8, $9)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.upload_grant_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(command.artifact_id.to_string())
        .bind(grant.link_key_digest()?.to_string())
        .bind(grant_payload.schema_version)
        .bind(&grant_payload.value)
        .bind(&grant_payload.digest)
        .bind(command.grant_expires_at)
        .execute(&mut *transaction)
        .await?;

        append_command_event(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            1,
            "artifact.prepared",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "artifact_grant_id": command.upload_grant_id,
                    "blob_id": command.blob_id,
                    "expected_size_bytes": command.expected_size_bytes,
                    "operation_job_id": command.operation_id,
                    "retention_policy_revision_id": command.retention_policy_revision_id,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.artifact_id.to_string(),
            "prepared",
        )
        .await?;

        let prepared = load_artifact_bundle(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.upload_grant_id,
            &command.operation_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(prepared))
    }

    async fn complete_upload(
        &mut self,
        command: CompleteArtifactUpload,
    ) -> Result<CommandOutcome<CompletedArtifactUpload>, Self::Error> {
        command.validate_at(Utc::now(), self.limits)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now, self.limits)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            "artifact.complete_upload",
        )
        .await?
        {
            let bundle = load_artifact_bundle(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_id,
                &command.blob_id,
                &command.upload_grant_id,
                &command.operation_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(completed_upload(bundle)));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::ArtifactWrite)
            .await?;
        lock_upload_bundle(&mut transaction, &command).await?;
        let current = load_artifact_bundle(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.upload_grant_id,
            &command.operation_id,
        )
        .await?;
        let decision = decide_complete_upload(
            &current.artifact,
            &current.blob,
            &current.grant,
            &current.operation,
            &command,
            database_now,
        )?;

        let artifact_version = to_i64(decision.artifact_version, "Artifact version")?;
        let blob_version = to_i64(decision.blob_version, "Blob version")?;
        let grant_version = to_i64(decision.grant_version, "ArtifactGrant version")?;
        let operation_version = to_i64(decision.operation_version, "Operation Job version")?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifacts
                SET state = $4, version = $5, updated_at = $6
                WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.artifact_id.to_string())
            .bind(to_i64(
                command.expected_artifact_version,
                "Artifact version",
            )?)
            .bind(decision.artifact_state.as_str())
            .bind(artifact_version)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact",
        )?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifact_blobs
                SET object_generation = $4, version = $5, updated_at = $6
                WHERE tenant_id = $1 AND blob_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.blob_id.to_string())
            .bind(to_i64(command.expected_blob_version, "Blob version")?)
            .bind(&command.object_generation)
            .bind(blob_version)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact Blob",
        )?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifact_links
                SET state = $4, version = $5, released_at = $6, updated_at = $6
                WHERE tenant_id = $1 AND artifact_link_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.upload_grant_id.to_string())
            .bind(to_i64(
                command.expected_grant_version,
                "ArtifactGrant version",
            )?)
            .bind(decision.grant_state.as_str())
            .bind(grant_version)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "ArtifactGrant",
        )?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.jobs
                SET state = $4, version = $5, started_at = $6, updated_at = $6
                WHERE tenant_id = $1 AND job_id = $2 AND version = $3
                  AND work_class = 'artifact' AND owner_kind = 'artifact'
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.operation_id.to_string())
            .bind(to_i64(
                command.expected_operation_version,
                "Operation Job version",
            )?)
            .bind(decision.operation_state.as_str())
            .bind(operation_version)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact upload Operation Job",
        )?;

        append_command_event(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            artifact_version,
            "artifact.uploaded",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "backend_evidence_digest": command.backend_evidence_digest,
                    "operation_job_id": command.operation_id,
                    "observed_size_bytes": command.observed_size_bytes,
                    "state": decision.artifact_state,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.artifact_id.to_string(),
            "uploaded",
        )
        .await?;
        let bundle = load_artifact_bundle(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.upload_grant_id,
            &command.operation_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(completed_upload(bundle)))
    }

    async fn schedule_initial_scan(
        &mut self,
        command: ScheduleInitialArtifactScan,
    ) -> Result<CommandOutcome<ArtifactScanWorkRecord>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            "artifact.scan.schedule",
        )
        .await?
        {
            let current = load_artifact_scan_work(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_id,
                &command.blob_id,
                &command.operation_id,
                &command.scan_job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(current));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::ArtifactWrite)
            .await?;
        require_exact_artifact_scan_policy(
            &mut transaction,
            &command.audit.tenant_id,
            &command.scan_policy_revision,
        )
        .await?;
        lock_verification_bundle(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.operation_id,
        )
        .await?;
        let current = load_verification_records(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.operation_id,
        )
        .await?;
        let decision = decide_schedule_initial_scan(
            &current.artifact,
            &current.blob,
            &current.operation,
            &command,
            database_now,
        )?;
        let artifact_version = to_i64(decision.artifact_version, "Artifact version")?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifacts
                SET state = 'verifying', version = $4, updated_at = $5
                WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.artifact_id.to_string())
            .bind(to_i64(
                command.expected_artifact_version,
                "Artifact version",
            )?)
            .bind(artifact_version)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact scan scheduling",
        )?;
        let job = ArtifactJobPayload::Scan {
            scan: decision.job.clone(),
        };
        job.validate_for_owner(&command.artifact_id)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let payload = TypedPayload::new(1, &job)?;
        let attempt_limit = i32::try_from(self.limits.maximum_job_attempts()).map_err(|_| {
            RepositoryError::InvalidInput("Artifact scan attempt limit exceeds integer".to_owned())
        })?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.jobs
                SET state = 'ready', version = version + 1, attempt_limit = $4,
                    scheduled_at = $5, deadline = $6, request_digest = $7,
                    payload_schema_version = $8, payload = $9, payload_digest = $10,
                    updated_at = $5
                WHERE tenant_id = $1 AND job_id = $2 AND owner_id = $3
                  AND work_class = 'artifact' AND owner_kind = 'artifact'
                  AND state = 'waiting' AND version = $11
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.operation_id.to_string())
            .bind(command.artifact_id.to_string())
            .bind(attempt_limit)
            .bind(database_now)
            .bind(command.deadline)
            .bind(command.audit.request_digest.to_string())
            .bind(payload.schema_version)
            .bind(&payload.value)
            .bind(&payload.digest)
            .bind(to_i64(
                command.expected_operation_version,
                "Artifact verify Job version",
            )?)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact verify Job wake",
        )?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            artifact_version,
            "artifact.scan_scheduled",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "operation_job_id": command.operation_id,
                    "scan_job_id": command.operation_id,
                    "scan_kind": ArtifactScanKind::Initial,
                    "state": decision.artifact_state,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.scan_job_id.to_string(),
            "scan_scheduled",
        )
        .await?;
        let current = load_artifact_scan_work(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.operation_id,
            &command.scan_job_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(current))
    }

    async fn schedule_rescan(
        &mut self,
        command: ScheduleArtifactRescan,
    ) -> Result<CommandOutcome<ArtifactScanWorkRecord>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            "artifact.rescan.schedule",
        )
        .await?
        {
            let current = load_artifact_scan_work(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_id,
                &command.blob_id,
                &command.rescan_operation_id,
                &command.rescan_job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(current));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::ArtifactWrite)
            .await?;
        require_exact_artifact_scan_policy(
            &mut transaction,
            &command.audit.tenant_id,
            &command.scan_policy_revision,
        )
        .await?;
        lock_artifact_and_blob(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
        )
        .await?;
        let artifact = load_artifact_record(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
        )
        .await?;
        let blob =
            load_artifact_blob_record(&mut transaction, &command.audit.tenant_id, &command.blob_id)
                .await?;
        let decision = decide_schedule_artifact_rescan(&artifact, &blob, &command, database_now)?;
        insert_artifact_scan_job(
            &mut transaction,
            ArtifactScanJobInsert {
                tenant_id: &command.audit.tenant_id,
                job_id: &command.rescan_job_id,
                artifact_id: &command.artifact_id,
                job: ArtifactJobPayload::Rescan {
                    scan: decision.job.clone(),
                },
                request_digest: &command.audit.request_digest,
                deadline: command.deadline,
                database_now,
                limits: self.limits,
            },
        )
        .await?;
        let artifact_version = to_i64(decision.artifact_version, "Artifact version")?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifacts
                SET state = 'quarantined', version = $4, updated_at = $5
                WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.artifact_id.to_string())
            .bind(to_i64(
                command.expected_artifact_version,
                "Artifact version",
            )?)
            .bind(artifact_version)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact rescan scheduling",
        )?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            artifact_version,
            "artifact.quarantined",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "operation_job_id": command.rescan_operation_id,
                    "reason_class": "rescan_pending",
                    "scan_job_id": command.rescan_job_id,
                    "state": decision.artifact_state,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.rescan_job_id.to_string(),
            "rescan_scheduled",
        )
        .await?;
        let current = load_artifact_scan_work(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.rescan_operation_id,
            &command.rescan_job_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(current))
    }

    async fn commit_scan_outcome(
        &mut self,
        command: CommitArtifactScanOutcome,
    ) -> Result<CommandOutcome<ArtifactScanWorkRecord>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        let receipt_payload = artifact_scan_worker_receipt_payload(&command)?;
        if claim_artifact_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.scan_job_id,
            "artifact.scan.commit",
            &receipt_payload,
        )
        .await?
        {
            let current = load_artifact_scan_work(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_id,
                &command.blob_id,
                &command.operation_id,
                &command.scan_job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(current));
        }
        let locked = load_artifact_scan_work_inner(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.operation_id,
            &command.scan_job_id,
            true,
        )
        .await?;
        require_artifact_job_fence(
            &locked,
            &command.fence,
            &command.audit.worker_process_generation_id,
            database_now,
        )?;
        let current = locked.record;
        let reusable_blob = if current.scan.scan_kind == ArtifactScanKind::Initial {
            lock_and_load_reusable_blob(
                &mut transaction,
                &current.blob,
                &command.evidence.content_digest,
                command.evidence.size_bytes,
            )
            .await?
        } else {
            None
        };
        let decision = decide_commit_artifact_scan(
            &current.artifact,
            &current.blob,
            &current.operation,
            reusable_blob.as_ref(),
            &current.scan,
            &command,
            database_now,
        )?;
        persist_artifact_scan_decision(
            &mut transaction,
            &current,
            &command,
            &decision,
            database_now,
            self.limits,
        )
        .await?;
        let artifact_version = to_i64(decision.artifact_version, "Artifact version")?;
        let event_type = match decision.artifact_state {
            ArtifactState::Verified => "artifact.verified",
            ArtifactState::Ready => "artifact.ready",
            ArtifactState::Quarantined => "artifact.quarantined",
            ArtifactState::Rejected => "artifact.rejected",
            ArtifactState::Corrupt => "artifact.corrupt",
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Artifact scan decision produced an unsupported state".to_owned(),
                ))
            }
        };
        append_artifact_worker_event(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            artifact_version,
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "active_blob_id": decision.artifact_blob_id,
                    "duplicate_blob_cleanup_job_id": decision
                        .duplicate_blob_cleanup
                        .as_ref()
                        .map(|_| &command.duplicate_blob_cleanup_job_id),
                    "operation_job_id": command.operation_id,
                    "reason_class": command.evidence.reason_class,
                    "scan_job_id": command.scan_job_id,
                    "scan_kind": current.scan.scan_kind,
                    "state": decision.artifact_state,
                    "verified_media_type": command.evidence.verified_media_type,
                }),
            )?,
        )
        .await?;
        terminalize_artifact_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.scan_job_id,
            command.evidence.disposition.as_str(),
            &command.artifact_id,
        )
        .await?;
        let completed = load_artifact_scan_work(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.operation_id,
            &command.scan_job_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(completed))
    }

    async fn commit_blob_cleanup(
        &mut self,
        command: CommitArtifactBlobCleanup,
    ) -> Result<CommandOutcome<CompletedArtifactBlobCleanup>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "cleanup_job_id": command.cleanup_job_id,
                "discarded_blob_id": command.discarded_blob_id,
                "evidence": command.evidence,
                "fence": {
                    "expected_version": command.fence.expected_version,
                    "lease_generation": command.fence.lease_generation,
                    "lease_token_digest": command.fence.token_digest,
                    "worker_process_generation_id": command.fence.worker_process_generation_id,
                },
            }),
            65_536,
        )?;
        if claim_artifact_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.cleanup_job_id,
            "artifact.blob_cleanup.commit",
            &receipt_payload,
        )
        .await?
        {
            let completed = load_completed_blob_cleanup(
                &mut transaction,
                &command.audit.tenant_id,
                &command.discarded_blob_id,
                &command.cleanup_job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(completed));
        }
        let cleanup_work = lock_blob_cleanup_work(
            &mut transaction,
            &command.audit.tenant_id,
            &command.discarded_blob_id,
            &command.cleanup_job_id,
        )
        .await?;
        require_raw_artifact_job_fence(
            &cleanup_work.job_state,
            cleanup_work.job_version,
            cleanup_work.lease_epoch,
            cleanup_work.worker_id.as_deref(),
            cleanup_work.lease_token_digest.as_deref(),
            cleanup_work.lease_expires_at,
            &command.fence,
            &command.audit.worker_process_generation_id,
            database_now,
        )?;
        let (blob_state, blob_version) = decide_commit_blob_cleanup(
            &cleanup_work.blob,
            &cleanup_work.cleanup,
            &command,
            database_now,
        )?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifact_blobs
                SET state = $4, version = $5, deleted_at = $6, updated_at = $6
                WHERE tenant_id = $1 AND blob_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.discarded_blob_id.to_string())
            .bind(to_i64(
                command.expected_blob_version,
                "Artifact Blob version",
            )?)
            .bind(blob_state.as_str())
            .bind(to_i64(blob_version, "Artifact Blob version")?)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact Blob cleanup",
        )?;
        complete_artifact_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.cleanup_job_id,
            &command.fence,
            &command.evidence.backend_receipt_digest,
            database_now,
        )
        .await?;
        append_artifact_worker_event(
            &mut transaction,
            &command.audit,
            "artifact_blob",
            &command.discarded_blob_id.to_string(),
            to_i64(blob_version, "Artifact Blob version")?,
            "artifact.blob_deleted",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "cleanup_job_id": command.cleanup_job_id,
                    "state": blob_state,
                }),
            )?,
        )
        .await?;
        terminalize_artifact_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.cleanup_job_id,
            "deleted",
            &command.discarded_blob_id,
        )
        .await?;
        let completed = load_completed_blob_cleanup(
            &mut transaction,
            &command.audit.tenant_id,
            &command.discarded_blob_id,
            &command.cleanup_job_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(completed))
    }

    async fn place_hold(
        &mut self,
        command: PlaceArtifactHold,
    ) -> Result<CommandOutcome<ArtifactHoldRecord>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "artifact_link",
            &command.artifact_hold_id.to_string(),
            "artifact.hold.place",
        )
        .await?
        {
            let hold = load_artifact_hold(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_hold_id,
                &command.artifact_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(hold));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::ArtifactHold)
            .await?;
        lock_artifacts(
            &mut transaction,
            &command.audit.tenant_id,
            &[&command.artifact_id],
        )
        .await?;
        let artifact = load_artifact_record(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
        )
        .await?;
        require_artifact_link_capacity(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            self.limits.maximum_links_per_artifact(),
        )
        .await?;
        let snapshot = decide_place_artifact_hold(&artifact, &command, database_now)?;
        let payload = TypedPayload::from_versioned(1, &snapshot, 262_144)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.artifact_links (
                tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
                target_artifact_id, link_key_digest, state, payload_schema_version,
                payload, payload_digest, expires_at
            ) VALUES ($1, $2, 'hold', 'principal', $3, $4, $5, 'active',
                      $6, $7, $8, $9)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.artifact_hold_id.to_string())
        .bind(command.audit.principal_id.to_string())
        .bind(command.artifact_id.to_string())
        .bind(snapshot.link_key_digest()?.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(command.expires_at)
        .execute(&mut *transaction)
        .await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "artifact_link",
            &command.artifact_hold_id.to_string(),
            1,
            "artifact.hold_placed",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "artifact_id": command.artifact_id,
                    "hold_kind": command.hold_kind,
                    "reason_class": command.reason_class,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.artifact_hold_id.to_string(),
            "active",
        )
        .await?;
        let hold = load_artifact_hold(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_hold_id,
            &command.artifact_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(hold))
    }

    async fn release_hold(
        &mut self,
        command: ReleaseArtifactHold,
    ) -> Result<CommandOutcome<ArtifactHoldRecord>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "artifact_link",
            &command.artifact_hold_id.to_string(),
            "artifact.hold.release",
        )
        .await?
        {
            let hold = load_artifact_hold(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_hold_id,
                &command.artifact_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(hold));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::ArtifactHold)
            .await?;
        sqlx::query(
            "SELECT artifact_link_id FROM insight_platform.artifact_links WHERE tenant_id = $1 AND artifact_link_id = $2 FOR UPDATE",
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.artifact_hold_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Artifact hold"))?;
        let current = load_artifact_hold(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_hold_id,
            &command.artifact_id,
        )
        .await?;
        let (state, version) = decide_release_artifact_hold(&current, &command)?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifact_links
                SET state = $4, version = $5, released_at = $6, updated_at = $6
                WHERE tenant_id = $1 AND artifact_link_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.artifact_hold_id.to_string())
            .bind(to_i64(
                command.expected_hold_version,
                "Artifact hold version",
            )?)
            .bind(state.as_str())
            .bind(to_i64(version, "Artifact hold version")?)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact hold",
        )?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "artifact_link",
            &command.artifact_hold_id.to_string(),
            to_i64(version, "Artifact hold version")?,
            "artifact.hold_released",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "artifact_id": command.artifact_id,
                    "evidence_digest": command.evidence_digest,
                    "reason_class": command.reason_class,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.artifact_hold_id.to_string(),
            "released",
        )
        .await?;
        let hold = load_artifact_hold(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_hold_id,
            &command.artifact_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(hold))
    }

    async fn create_provenance(
        &mut self,
        command: CreateArtifactProvenance,
    ) -> Result<CommandOutcome<ArtifactProvenanceRecord>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "artifact_link",
            &command.provenance_link_id.to_string(),
            "artifact.provenance.create",
        )
        .await?
        {
            let provenance = load_artifact_provenance(
                &mut transaction,
                &command.audit.tenant_id,
                &command.provenance_link_id,
                &command.source_artifact_id,
                &command.derived_artifact_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(provenance));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::ArtifactWrite)
            .await?;
        lock_artifacts(
            &mut transaction,
            &command.audit.tenant_id,
            &[&command.source_artifact_id, &command.derived_artifact_id],
        )
        .await?;
        let source = load_artifact_record(
            &mut transaction,
            &command.audit.tenant_id,
            &command.source_artifact_id,
        )
        .await?;
        let derived = load_artifact_record(
            &mut transaction,
            &command.audit.tenant_id,
            &command.derived_artifact_id,
        )
        .await?;
        for artifact_id in [&command.source_artifact_id, &command.derived_artifact_id] {
            require_artifact_link_capacity(
                &mut transaction,
                &command.audit.tenant_id,
                artifact_id,
                self.limits.maximum_links_per_artifact(),
            )
            .await?;
        }
        require_acyclic_provenance(
            &mut transaction,
            &command.audit.tenant_id,
            &command.source_artifact_id,
            &command.derived_artifact_id,
            self.limits.maximum_provenance_depth(),
        )
        .await?;
        let snapshot = decide_create_artifact_provenance(&source, &derived, &command)?;
        let payload = TypedPayload::from_versioned(1, &snapshot, 262_144)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.artifact_links (
                tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
                source_artifact_id, target_artifact_id, link_key_digest, state,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, 'provenance', 'artifact_producer', $3,
                      $4, $5, $6, 'active', $7, $8, $9)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.provenance_link_id.to_string())
        .bind(command.producer_owner_id.to_string())
        .bind(command.source_artifact_id.to_string())
        .bind(command.derived_artifact_id.to_string())
        .bind(snapshot.link_key_digest()?.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .execute(&mut *transaction)
        .await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "artifact_link",
            &command.provenance_link_id.to_string(),
            1,
            "artifact.provenance_created",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "derived_artifact_id": command.derived_artifact_id,
                    "source_artifact_id": command.source_artifact_id,
                    "transformation_deployment_id": command.transformation_deployment_id,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.provenance_link_id.to_string(),
            "active",
        )
        .await?;
        let provenance = load_artifact_provenance(
            &mut transaction,
            &command.audit.tenant_id,
            &command.provenance_link_id,
            &command.source_artifact_id,
            &command.derived_artifact_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(provenance))
    }

    async fn release_reference(
        &mut self,
        command: ReleaseArtifactReference,
    ) -> Result<CommandOutcome<ArtifactReferenceRecord>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "artifact_link",
            &command.artifact_reference_id.to_string(),
            "artifact.reference.release",
        )
        .await?
        {
            let reference = load_artifact_reference(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_reference_id,
                &command.artifact_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(reference));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::ArtifactWrite)
            .await?;
        sqlx::query(
            "SELECT artifact_link_id FROM insight_platform.artifact_links WHERE tenant_id = $1 AND artifact_link_id = $2 FOR UPDATE",
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.artifact_reference_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Artifact reference"))?;
        let current = load_artifact_reference(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_reference_id,
            &command.artifact_id,
        )
        .await?;
        let (state, version) = decide_release_artifact_reference(&current, &command)?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifact_links
                SET state = $4, version = $5, released_at = $6, updated_at = $6
                WHERE tenant_id = $1 AND artifact_link_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.artifact_reference_id.to_string())
            .bind(to_i64(
                command.expected_reference_version,
                "Artifact reference version",
            )?)
            .bind(state.as_str())
            .bind(to_i64(version, "Artifact reference version")?)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact reference",
        )?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "artifact_link",
            &command.artifact_reference_id.to_string(),
            to_i64(version, "Artifact reference version")?,
            "artifact.reference_released",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "artifact_id": command.artifact_id,
                    "reason_class": command.reason_class,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.artifact_reference_id.to_string(),
            "released",
        )
        .await?;
        let reference = load_artifact_reference(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_reference_id,
            &command.artifact_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(reference))
    }

    async fn mark_deletion(
        &mut self,
        command: MarkArtifactDeletion,
    ) -> Result<CommandOutcome<MarkedArtifactDeletion>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            "artifact.delete.mark",
        )
        .await?
        {
            let marked = load_artifact_deletion(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_id,
                &command.blob_id,
                &command.deletion_operation_id,
                &command.deletion_job_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(marked));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::ArtifactDelete)
            .await?;
        let (blob, aliases) =
            lock_blob_and_aliases(&mut transaction, &command.audit.tenant_id, &command.blob_id)
                .await?;
        let artifact = aliases
            .iter()
            .find(|artifact| artifact.artifact_id == command.artifact_id)
            .cloned()
            .ok_or(RepositoryError::NotFound("Artifact deletion target"))?;
        if aliases.iter().any(|alias| {
            alias.artifact_id != command.artifact_id && alias.state == ArtifactState::Deleting
        }) {
            return Err(RepositoryError::Conflict(
                "another same-Blob Artifact deletion is active",
            ));
        }
        let policy = load_retention_policy(
            &mut transaction,
            &command.audit.tenant_id,
            &artifact.retention_policy_revision_id,
        )
        .await?;
        let approval_satisfied =
            require_deletion_approval(&mut transaction, &command, &artifact, &policy).await?;
        let (live_reference_count, active_hold_count, provenance_count) =
            artifact_deletion_link_facts(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_id,
                database_now,
            )
            .await?;
        let live_alias = aliases.iter().find(|alias| {
            alias.artifact_id != command.artifact_id && alias.state != ArtifactState::Deleted
        });
        let decision = decide_mark_artifact_deletion(
            &artifact,
            &blob,
            live_alias,
            ArtifactDeletionAdmissionFacts {
                approval_required: policy.delete_requires_approval,
                approval_satisfied,
                gc_grace_seconds: policy.gc_grace_seconds,
                live_reference_count,
                active_hold_count,
                blocking_provenance_count: if policy.retain_provenance_sources {
                    provenance_count
                } else {
                    0
                },
            },
            &command,
            database_now,
        )?;
        let artifact_job = ArtifactJobPayload::Delete {
            deletion: decision.job.clone(),
        };
        artifact_job
            .validate_for_owner(&command.artifact_id)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let job_payload = TypedPayload::new(1, &artifact_job)?;
        let artifact_version = to_i64(decision.artifact_version, "Artifact version")?;
        let blob_version = to_i64(decision.blob_version, "Blob version")?;
        if decision.blob_version != blob.version {
            ensure_one(
                sqlx::query(
                    r#"
                    UPDATE insight_platform.artifact_blobs
                    SET state = $4, version = $5, updated_at = $6
                    WHERE tenant_id = $1 AND blob_id = $2 AND version = $3
                    "#,
                )
                .bind(command.audit.tenant_id.to_string())
                .bind(command.blob_id.to_string())
                .bind(to_i64(command.expected_blob_version, "Blob version")?)
                .bind(decision.blob_state.as_str())
                .bind(blob_version)
                .bind(database_now)
                .execute(&mut *transaction)
                .await?
                .rows_affected(),
                "Artifact Blob deletion admission",
            )?;
        }
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifacts
                SET state = $4, version = $5, updated_at = $6
                WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.artifact_id.to_string())
            .bind(to_i64(
                command.expected_artifact_version,
                "Artifact version",
            )?)
            .bind(decision.artifact_state.as_str())
            .bind(artifact_version)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact deletion admission",
        )?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, work_class, owner_kind, owner_id, state,
                attempt_limit, scheduled_at, deadline, request_digest,
                payload_schema_version, payload, payload_digest, created_at, updated_at
            ) VALUES ($1, $2, 'artifact', 'artifact', $3, 'ready',
                      $4, $5, $6, $7, $8, $9, $10, $5, $5)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.deletion_operation_id.to_string())
        .bind(command.artifact_id.to_string())
        .bind(
            i32::try_from(self.limits.maximum_job_attempts()).map_err(|_| {
                RepositoryError::InvalidInput(
                    "Artifact deletion attempt limit exceeds integer".to_owned(),
                )
            })?,
        )
        .bind(database_now)
        .bind(command.deadline)
        .bind(command.audit.request_digest.to_string())
        .bind(job_payload.schema_version)
        .bind(&job_payload.value)
        .bind(&job_payload.digest)
        .execute(&mut *transaction)
        .await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            artifact_version,
            "artifact.deletion_marked",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "deletion_job_id": command.deletion_job_id,
                    "deletion_operation_id": command.deletion_operation_id,
                    "mode": decision.mode,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.artifact_id.to_string(),
            "deleting",
        )
        .await?;
        let marked = load_artifact_deletion(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.deletion_operation_id,
            &command.deletion_job_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(marked))
    }

    async fn complete_deletion(
        &mut self,
        command: CompleteArtifactDeletion,
    ) -> Result<CommandOutcome<CompletedArtifactDeletion>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "artifact_id": command.artifact_id,
                "blob_id": command.blob_id,
                "deletion_operation_id": command.deletion_operation_id,
                "evidence": command.evidence,
                "fence": {
                    "expected_version": command.fence.expected_version,
                    "lease_generation": command.fence.lease_generation,
                    "lease_token_digest": command.fence.token_digest,
                    "worker_process_generation_id": command.fence.worker_process_generation_id,
                },
            }),
            262_144,
        )?;
        if claim_artifact_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.deletion_job_id,
            "artifact.delete.complete",
            &receipt_payload,
        )
        .await?
        {
            let completed = load_artifact_deletion(
                &mut transaction,
                &command.audit.tenant_id,
                &command.artifact_id,
                &command.blob_id,
                &command.deletion_operation_id,
                &command.deletion_job_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(CompletedArtifactDeletion {
                artifact: completed.artifact,
                blob: completed.blob,
                deletion: completed.deletion,
            }));
        }
        let job = lock_deletion_job(&mut transaction, &command, database_now).await?;
        let (blob, aliases) =
            lock_blob_and_aliases(&mut transaction, &command.audit.tenant_id, &command.blob_id)
                .await?;
        let marked = load_artifact_deletion(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.deletion_operation_id,
            &command.deletion_job_id,
        )
        .await?;
        if job.snapshot.mode != marked.deletion.mode {
            return Err(RepositoryError::CorruptRow(
                "Artifact deletion Job and Operation disagree on mode".to_owned(),
            ));
        }
        let alias_witness = match &command.evidence {
            ArtifactDeletionEvidence::ArtifactOnly {
                alias_artifact_id, ..
            } => aliases
                .iter()
                .find(|artifact| &artifact.artifact_id == alias_artifact_id),
            ArtifactDeletionEvidence::BlobGeneration { .. } => aliases.iter().find(|artifact| {
                artifact.artifact_id != command.artifact_id
                    && artifact.state != ArtifactState::Deleted
            }),
        };
        let decision = decide_complete_artifact_deletion(
            &marked.artifact,
            &blob,
            &marked.deletion,
            alias_witness,
            &command,
        )?;
        let artifact_version = to_i64(decision.artifact_version, "Artifact version")?;
        let blob_version = to_i64(decision.blob_version, "Blob version")?;
        let operation_version = to_i64(
            decision.operation_version,
            "Artifact deletion Operation Job version",
        )?;
        let evidence_payload = TypedPayload::new(1, &command.evidence)?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifacts
                SET state = $4, version = $5, terminal_at = $6, updated_at = $6
                WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.artifact_id.to_string())
            .bind(to_i64(
                command.expected_artifact_version,
                "Artifact version",
            )?)
            .bind(decision.artifact_state.as_str())
            .bind(artifact_version)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact deletion completion",
        )?;
        if decision.blob_version != blob.version {
            ensure_one(
                sqlx::query(
                    r#"
                    UPDATE insight_platform.artifact_blobs
                    SET state = $4, version = $5, deleted_at = $6, updated_at = $6
                    WHERE tenant_id = $1 AND blob_id = $2 AND version = $3
                    "#,
                )
                .bind(command.audit.tenant_id.to_string())
                .bind(command.blob_id.to_string())
                .bind(to_i64(command.expected_blob_version, "Blob version")?)
                .bind(decision.blob_state.as_str())
                .bind(blob_version)
                .bind(database_now)
                .execute(&mut *transaction)
                .await?
                .rows_affected(),
                "Artifact Blob deletion completion",
            )?;
        }
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.jobs
                SET state = $7, version = $8,
                    result_digest = $9, worker_id = NULL, lease_token_digest = NULL,
                    lease_expires_at = NULL, heartbeat_at = NULL,
                    terminal_at = $10, updated_at = $10
                WHERE tenant_id = $1 AND job_id = $2 AND version = $3
                  AND state = 'running' AND worker_id = $4 AND lease_epoch = $5
                  AND lease_token_digest = $6 AND invocation_id IS NULL
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.deletion_job_id.to_string())
            .bind(to_i64(
                command.fence.expected_version,
                "Artifact deletion Job version",
            )?)
            .bind(command.fence.worker_process_generation_id.to_string())
            .bind(to_i64(
                command.fence.lease_generation,
                "Artifact deletion Job epoch",
            )?)
            .bind(command.fence.token_digest.to_string())
            .bind(decision.operation_state.as_str())
            .bind(operation_version)
            .bind(&evidence_payload.digest)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact deletion Job fence",
        )?;
        append_artifact_worker_event(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            artifact_version,
            "artifact.deleted",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "deletion_job_id": command.deletion_job_id,
                    "deletion_operation_id": command.deletion_operation_id,
                    "evidence_digest": evidence_payload.digest,
                    "mode": marked.deletion.mode,
                }),
            )?,
        )
        .await?;
        terminalize_artifact_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.deletion_job_id,
            "deleted",
            &command.artifact_id,
        )
        .await?;
        let completed = load_artifact_deletion(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.deletion_operation_id,
            &command.deletion_job_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(CompletedArtifactDeletion {
            artifact: completed.artifact,
            blob: completed.blob,
            deletion: completed.deletion,
        }))
    }

    async fn finalize_artifact(
        &mut self,
        command: FinalizeArtifact,
    ) -> Result<CommandOutcome<FinalizedArtifact>, Self::Error> {
        command.validate_at(Utc::now(), self.limits)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now, self.limits)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            "artifact.finalize",
        )
        .await?
        {
            let finalized = load_finalized_artifact(&mut transaction, &command).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(finalized));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::ArtifactWrite)
            .await?;
        let quota = lock_staging_quota(&mut transaction, &command).await?;
        lock_upload_bundle_for_finalize(&mut transaction, &command).await?;
        let current = load_artifact_bundle(
            &mut transaction,
            &command.audit.tenant_id,
            &command.artifact_id,
            &command.blob_id,
            &command.upload_grant_id,
            &command.operation_id,
        )
        .await?;
        require_retention_policy(
            &mut transaction,
            &command.audit.tenant_id,
            &current.artifact.retention_policy_revision_id,
            current.artifact.retain_until,
            database_now,
        )
        .await?;
        let decision = decide_finalize_artifact(
            &current.artifact,
            &current.blob,
            &current.grant,
            &current.operation,
            &command,
        )?;
        settle_locked_staging_quota(&mut transaction, &command, quota).await?;
        let reference_payload = TypedPayload::from_versioned(1, &decision.reference, 262_144)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.artifact_links (
                tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
                target_artifact_id, link_key_digest, state, payload_schema_version,
                payload, payload_digest
            ) VALUES ($1, $2, 'reference', 'job', $3,
                      $4, $5, 'active', $6, $7, $8)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.artifact_reference_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(command.artifact_id.to_string())
        .bind(decision.reference.link_key_digest()?.to_string())
        .bind(reference_payload.schema_version)
        .bind(&reference_payload.value)
        .bind(&reference_payload.digest)
        .execute(&mut *transaction)
        .await?;
        let artifact_version = to_i64(decision.artifact_version, "Artifact version")?;
        let operation_version = to_i64(decision.operation_version, "Operation Job version")?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifacts
                SET state = $4, version = $5, updated_at = $6
                WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.artifact_id.to_string())
            .bind(to_i64(
                command.expected_artifact_version,
                "Artifact version",
            )?)
            .bind(decision.artifact_state.as_str())
            .bind(artifact_version)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact",
        )?;
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.jobs
                SET state = $4, version = $5, terminal_at = $6, updated_at = $6
                WHERE tenant_id = $1 AND job_id = $2 AND version = $3
                  AND work_class = 'artifact' AND owner_kind = 'artifact'
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.operation_id.to_string())
            .bind(to_i64(
                command.expected_operation_version,
                "Operation Job version",
            )?)
            .bind(decision.operation_state.as_str())
            .bind(operation_version)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected(),
            "Artifact upload Operation Job",
        )?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "artifact",
            &command.artifact_id.to_string(),
            artifact_version,
            "artifact.ready",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "artifact_reference_id": command.artifact_reference_id,
                    "operation_job_id": command.operation_id,
                    "purpose": current.artifact.purpose,
                    "state": decision.artifact_state,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.artifact_id.to_string(),
            "ready",
        )
        .await?;
        let finalized = load_finalized_artifact(&mut transaction, &command).await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(finalized))
    }

    async fn commit(self) -> Result<(), Self::Error> {
        self.transaction.commit().await?;
        Ok(())
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

impl From<ArtifactCommandError> for RepositoryError {
    fn from(failure: ArtifactCommandError) -> Self {
        match failure {
            ArtifactCommandError::StaleVersion => Self::Conflict("Artifact aggregate version"),
            ArtifactCommandError::InvalidTransition => Self::Conflict("Artifact transition"),
            ArtifactCommandError::GrantRejected => Self::PermissionDenied,
            _ => Self::InvalidInput(failure.to_string()),
        }
    }
}

impl From<ArtifactWorkError> for RepositoryError {
    fn from(failure: ArtifactWorkError) -> Self {
        match failure {
            ArtifactWorkError::InvalidTransition => Self::Conflict("Artifact work transition"),
            ArtifactWorkError::EvidenceMismatch => Self::StaleFence,
            _ => Self::InvalidInput(failure.to_string()),
        }
    }
}

async fn require_exact_artifact_scan_policy(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &ExactVersionRef,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT version.payload_schema_version, version.payload, version.payload_digest
        FROM insight_platform.resource_versions AS version
        JOIN insight_platform.resources AS resource
          ON resource.tenant_id = version.tenant_id
         AND resource.resource_id = version.resource_id
        WHERE version.tenant_id = $1 AND version.resource_version_id = $2
          AND version.resource_version_kind = 'policy_revision'
          AND version.content_digest = $3
          AND resource.resource_kind = 'policy'
          AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
        FOR SHARE OF version, resource
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(exact.revision_id.to_string())
    .bind(exact.semantic_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "active exact Artifact scan Policy Revision",
    ))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let published: PublishedVersionPayload =
        decode_typed_payload(&payload, "Artifact scan Policy Revision")?;
    published
        .validate_for(
            insight_platform_contracts::RegistryResourceKind::Policy,
            &exact.revision_id,
        )
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    match published.document {
        ResourceDocument::Policy(policy) if policy.policy_kind == PolicyKind::ArtifactIo => Ok(()),
        ResourceDocument::Policy(_) => Err(RepositoryError::InvalidInput(
            "Artifact scan Policy must have policy_kind artifact_io".to_owned(),
        )),
        _ => Err(RepositoryError::CorruptRow(
            "Artifact scan Policy Revision contains the wrong document".to_owned(),
        )),
    }
}

async fn claim_artifact_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &ArtifactWorkerAudit,
    job_id: &ResourceId,
    operation: &str,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
            payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, $5, $6, $7,
                  'processing', $8, $9, $10, $11)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(job_id.to_string())
    .bind(audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let row = sqlx::query(
        r#"
        SELECT request_digest, state, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = $4 AND idempotency_key_digest = $5
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(job_id.to_string())
    .bind(audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != audit.request_digest.to_string()
        || row.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("Artifact worker receipt"));
    }
    Ok(true)
}

async fn append_artifact_worker_event(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &ArtifactWorkerAudit,
    aggregate_kind: &str,
    aggregate_id: &str,
    aggregate_version: i64,
    event_type: &str,
    payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    append_scheduler_event(
        transaction,
        &audit.tenant_id.to_string(),
        &audit.event_id,
        &audit.outbox_id,
        aggregate_kind,
        aggregate_id,
        aggregate_version,
        None,
        event_type,
        payload,
    )
    .await
}

async fn terminalize_artifact_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &ArtifactWorkerAudit,
    job_id: &ResourceId,
    disposition: &str,
    response_reference_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_id = $6 AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(disposition)
    .bind(response_reference_id.to_string())
    .bind(job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("Artifact worker receipt"));
    }
    Ok(())
}

async fn require_retention_policy(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    retention_policy_revision_id: &ResourceId,
    retain_until: DateTime<Utc>,
    database_now: DateTime<Utc>,
) -> Result<ArtifactRetentionPolicy, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT version.payload_schema_version, version.payload, version.payload_digest
        FROM insight_platform.resource_versions AS version
        JOIN insight_platform.resources AS resource
          ON resource.tenant_id = version.tenant_id
         AND resource.resource_id = version.resource_id
        WHERE version.tenant_id = $1 AND version.resource_version_id = $2
          AND version.resource_version_kind = 'policy_revision'
          AND resource.resource_kind = 'policy'
          AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(retention_policy_revision_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "enabled Artifact retention Policy Revision",
    ))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let published: PublishedVersionPayload =
        decode_typed_payload(&payload, "Artifact retention Policy Revision")?;
    published
        .validate_for(
            insight_platform_contracts::RegistryResourceKind::Policy,
            retention_policy_revision_id,
        )
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ResourceDocument::Policy(policy_spec) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "retention revision is not a Policy document".to_owned(),
        ));
    };
    if policy_spec.policy_kind != PolicyKind::Retention {
        return Err(RepositoryError::InvalidInput(
            "Artifact retention revision has the wrong PolicyKind".to_owned(),
        ));
    }
    let retention = policy_spec.retention.ok_or_else(|| {
        RepositoryError::CorruptRow("Retention Policy has no closed policy document".to_owned())
    })?;
    retention
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let minimum_seconds = i64::try_from(retention.minimum_retention_seconds).map_err(|_| {
        RepositoryError::CorruptRow("Retention duration exceeds clock representation".to_owned())
    })?;
    let minimum_retain_until = database_now
        .checked_add_signed(Duration::seconds(minimum_seconds))
        .ok_or_else(|| {
            RepositoryError::InvalidInput("Artifact retention deadline overflows".to_owned())
        })?;
    if retain_until < minimum_retain_until {
        return Err(RepositoryError::InvalidInput(
            "Artifact retain_until violates the exact Retention Policy Revision".to_owned(),
        ));
    }
    Ok(retention)
}

async fn load_retention_policy(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    retention_policy_revision_id: &ResourceId,
) -> Result<ArtifactRetentionPolicy, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT version.payload_schema_version, version.payload, version.payload_digest
        FROM insight_platform.resource_versions AS version
        JOIN insight_platform.resources AS resource
          ON resource.tenant_id = version.tenant_id
         AND resource.resource_id = version.resource_id
        WHERE version.tenant_id = $1 AND version.resource_version_id = $2
          AND version.resource_version_kind = 'policy_revision'
          AND resource.resource_kind = 'policy'
          AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(retention_policy_revision_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "enabled Artifact retention Policy Revision",
    ))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let published: PublishedVersionPayload =
        decode_typed_payload(&payload, "Artifact retention Policy Revision")?;
    published
        .validate_for(
            insight_platform_contracts::RegistryResourceKind::Policy,
            retention_policy_revision_id,
        )
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ResourceDocument::Policy(policy_spec) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "retention revision is not a Policy document".to_owned(),
        ));
    };
    if policy_spec.policy_kind != PolicyKind::Retention {
        return Err(RepositoryError::InvalidInput(
            "Artifact retention revision has the wrong PolicyKind".to_owned(),
        ));
    }
    let retention = policy_spec.retention.ok_or_else(|| {
        RepositoryError::CorruptRow("Retention Policy has no closed policy document".to_owned())
    })?;
    retention
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(retention)
}

async fn lock_blob_and_aliases(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    blob_id: &ResourceId,
) -> Result<(ArtifactBlobRecord, Vec<ArtifactRecord>), RepositoryError> {
    let blob = sqlx::query(
        r#"
        SELECT tenant_id, blob_id, backend, storage_binding_digest,
               security_domain_digest, object_generation, encryption_domain_id,
               content_digest, size_bytes, state, version
        FROM insight_platform.artifact_blobs
        WHERE tenant_id = $1 AND blob_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact Blob"))?;
    let rows = sqlx::query(
        r#"
        SELECT tenant_id, artifact_id, blob_id, purpose, classification,
               expected_size_bytes, expected_digest, declared_media_type,
               verified_media_type, state, version, metadata_schema_version,
               metadata, metadata_digest, retention_policy_revision_id,
               retain_until, created_by, created_at, updated_at, terminal_at
        FROM insight_platform.artifacts
        WHERE tenant_id = $1 AND blob_id = $2
        ORDER BY artifact_id
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    let aliases = rows
        .into_iter()
        .map(artifact_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((blob_from_row(blob)?, aliases))
}

async fn artifact_deletion_link_facts(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<(u64, u64, u64), RepositoryError> {
    let (references, holds, provenance): (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          count(*) FILTER (WHERE link_kind NOT IN ('hold', 'provenance')),
          count(*) FILTER (WHERE link_kind = 'hold'),
          count(*) FILTER (WHERE link_kind = 'provenance')
        FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND state = 'active' AND released_at IS NULL
          AND (source_artifact_id = $2 OR target_artifact_id = $2)
          AND (expires_at IS NULL OR expires_at > $3)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .bind(database_now)
    .fetch_one(&mut **transaction)
    .await?;
    Ok((
        parse_u64(references, "Artifact live reference count")?,
        parse_u64(holds, "Artifact active hold count")?,
        parse_u64(provenance, "Artifact provenance count")?,
    ))
}

async fn require_deletion_approval(
    transaction: &mut Transaction<'_, Postgres>,
    command: &MarkArtifactDeletion,
    artifact: &ArtifactRecord,
    policy: &ArtifactRetentionPolicy,
) -> Result<bool, RepositoryError> {
    if !policy.delete_requires_approval {
        return Ok(false);
    }
    let approval_task_id = command.approval_task_id.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput("Artifact deletion requires approval".to_owned())
    })?;
    let row = sqlx::query(
        r#"
        SELECT state, payload_schema_version, payload, payload_digest
        FROM insight_platform.tasks
        WHERE tenant_id = $1 AND task_id = $2 AND task_kind = 'approval'
          AND owner_kind = 'artifact' AND owner_id = $3
          AND state = 'approved' AND responded_at IS NOT NULL
        FOR SHARE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(approval_task_id.to_string())
    .bind(command.artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("approved Artifact deletion Task"))?;
    let state: String = row.try_get("state")?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let payload: TaskPayload = decode_typed_payload(&payload, "Artifact deletion approval")?;
    payload
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let TaskDefinition::Approval {
        owner_version,
        owner_snapshot_digest,
        effect,
        input_digest,
        policy_revision_id,
        ..
    } = &payload.definition
    else {
        return Err(RepositoryError::CorruptRow(
            "Artifact deletion Task is not an approval".to_owned(),
        ));
    };
    let approved = state == TaskState::Approved.as_str()
        && *owner_version == artifact.version
        && owner_snapshot_digest == &command.audit.request_digest
        && *effect == Effect::Irreversible
        && input_digest == &command.audit.request_digest
        && policy_revision_id == &artifact.retention_policy_revision_id
        && payload.resolution.as_ref().is_some_and(|resolution| {
            resolution.state == TaskState::Approved && resolution.principal.is_some()
        });
    if !approved {
        return Err(RepositoryError::InvalidInput(
            "Artifact deletion approval does not bind the exact request and policy".to_owned(),
        ));
    }
    Ok(true)
}

struct LockedDeletionJob {
    snapshot: ArtifactDeletionJobSnapshot,
}

async fn lock_deletion_job(
    transaction: &mut Transaction<'_, Postgres>,
    command: &CompleteArtifactDeletion,
    database_now: DateTime<Utc>,
) -> Result<LockedDeletionJob, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, job_id, owner_kind, owner_id, invocation_id, state,
               version, lease_epoch, worker_id, lease_token_digest, lease_expires_at,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.deletion_job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact deletion Job"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let payload: ArtifactJobPayload = decode_typed_payload(&payload, "Artifact Job")?;
    payload
        .validate_for_owner(&command.artifact_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ArtifactJobPayload::Delete { deletion: snapshot } = payload else {
        return Err(RepositoryError::CorruptRow(
            "Artifact deletion Job has the wrong payload variant".to_owned(),
        ));
    };
    let valid = row.try_get::<String, _>("tenant_id")? == command.audit.tenant_id.to_string()
        && row.try_get::<String, _>("job_id")? == command.deletion_job_id.to_string()
        && row.try_get::<String, _>("owner_kind")? == "artifact"
        && row.try_get::<String, _>("owner_id")? == command.artifact_id.to_string()
        && row.try_get::<Option<String>, _>("invocation_id")?.is_none()
        && row.try_get::<String, _>("state")? == "running"
        && row.try_get::<i64, _>("version")?
            == to_i64(
                command.fence.expected_version,
                "Artifact deletion Job version",
            )?
        && row.try_get::<i64, _>("lease_epoch")?
            == to_i64(
                command.fence.lease_generation,
                "Artifact deletion Job epoch",
            )?
        && row.try_get::<Option<String>, _>("worker_id")?
            == Some(command.fence.worker_process_generation_id.to_string())
        && row.try_get::<Option<String>, _>("lease_token_digest")?
            == Some(command.fence.token_digest.to_string())
        && row
            .try_get::<Option<DateTime<Utc>>, _>("lease_expires_at")?
            .is_some_and(|expires_at| expires_at > database_now)
        && snapshot.operation_id == command.deletion_operation_id
        && snapshot.artifact_id == command.artifact_id
        && snapshot.blob_id == command.blob_id
        && snapshot.expected_artifact_version == command.expected_artifact_version
        && snapshot.expected_blob_version == command.expected_blob_version
        && command.expected_operation_version == command.fence.expected_version;
    if !valid {
        return Err(RepositoryError::StaleFence);
    }
    Ok(LockedDeletionJob { snapshot })
}

async fn load_artifact_deletion(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
    operation_id: &ResourceId,
    job_id: &ResourceId,
) -> Result<MarkedArtifactDeletion, RepositoryError> {
    let artifact = load_artifact_record(transaction, tenant_id, artifact_id).await?;
    if artifact.blob_id.as_ref() != Some(blob_id) {
        return Err(RepositoryError::NotFound("Artifact deletion Blob binding"));
    }
    let blob = sqlx::query(
        r#"
        SELECT tenant_id, blob_id, backend, storage_binding_digest,
               security_domain_digest, object_generation, encryption_domain_id,
               content_digest, size_bytes, state, version
        FROM insight_platform.artifact_blobs
        WHERE tenant_id = $1 AND blob_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact deletion Blob"))?;
    let operation = sqlx::query(
        r#"
        SELECT state, version, deadline, payload_schema_version, payload, payload_digest
        FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2
          AND work_class = 'artifact'
          AND owner_kind = 'artifact' AND owner_id = $3
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(operation_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact deletion Operation Job"))?;
    let job_payload = payload_from_row(
        &operation,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let job_payload: ArtifactJobPayload = decode_typed_payload(&job_payload, "Artifact Job")?;
    job_payload
        .validate_for_owner(artifact_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ArtifactJobPayload::Delete {
        deletion: job_snapshot,
    } = job_payload
    else {
        return Err(RepositoryError::CorruptRow(
            "Artifact deletion Job has the wrong payload variant".to_owned(),
        ));
    };
    if operation_id != job_id
        || job_snapshot.operation_id != *operation_id
        || job_snapshot.artifact_id != *artifact_id
        || job_snapshot.blob_id != *blob_id
    {
        return Err(RepositoryError::CorruptRow(
            "Artifact deletion authority rows disagree".to_owned(),
        ));
    }
    Ok(MarkedArtifactDeletion {
        artifact,
        blob: blob_from_row(blob)?,
        deletion: ArtifactDeletionRecord {
            tenant_id: tenant_id.clone(),
            operation_id: operation_id.clone(),
            operation_state: operation
                .try_get::<String, _>("state")?
                .parse::<JobState>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
            operation_version: parse_u64(
                operation.try_get("version")?,
                "Artifact deletion Operation Job version",
            )?,
            job_id: job_id.clone(),
            artifact_id: artifact_id.clone(),
            blob_id: blob_id.clone(),
            mode: job_snapshot.mode,
            deadline: operation.try_get("deadline")?,
        },
    })
}

async fn load_artifact_blob_record(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    blob_id: &ResourceId,
) -> Result<ArtifactBlobRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, blob_id, backend, storage_binding_digest,
               security_domain_digest, object_generation, encryption_domain_id,
               content_digest, size_bytes, state, version
        FROM insight_platform.artifact_blobs
        WHERE tenant_id = $1 AND blob_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact Blob"))?;
    blob_from_row(row)
}

async fn lock_artifact_and_blob(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let artifact_blob_id: Option<String> = sqlx::query_scalar(
        r#"
        SELECT blob_id FROM insight_platform.artifacts
        WHERE tenant_id = $1 AND artifact_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact"))?;
    if artifact_blob_id != Some(blob_id.to_string()) {
        return Err(RepositoryError::Conflict("Artifact Blob binding"));
    }
    let found: Option<String> = sqlx::query_scalar(
        r#"
        SELECT blob_id FROM insight_platform.artifact_blobs
        WHERE tenant_id = $1 AND blob_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if found.is_none() {
        return Err(RepositoryError::NotFound("Artifact Blob"));
    }
    Ok(())
}

struct ArtifactScanJobInsert<'a> {
    tenant_id: &'a ResourceId,
    job_id: &'a ResourceId,
    artifact_id: &'a ResourceId,
    job: ArtifactJobPayload,
    request_digest: &'a Sha256Digest,
    deadline: DateTime<Utc>,
    database_now: DateTime<Utc>,
    limits: ArtifactCommandLimits,
}

async fn insert_artifact_scan_job(
    transaction: &mut Transaction<'_, Postgres>,
    insert: ArtifactScanJobInsert<'_>,
) -> Result<(), RepositoryError> {
    insert
        .job
        .validate_for_owner(insert.artifact_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let payload = TypedPayload::new(1, &insert.job)?;
    let attempt_limit = i32::try_from(insert.limits.maximum_job_attempts()).map_err(|_| {
        RepositoryError::InvalidInput("Artifact scan attempt limit exceeds integer".to_owned())
    })?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
            tenant_id, job_id, work_class, owner_kind, owner_id,
            state, attempt_limit, scheduled_at, deadline, request_digest,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'artifact', 'artifact', $3,
                  'ready', $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(insert.tenant_id.to_string())
    .bind(insert.job_id.to_string())
    .bind(insert.artifact_id.to_string())
    .bind(attempt_limit)
    .bind(insert.database_now)
    .bind(insert.deadline)
    .bind(insert.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

struct LoadedArtifactScanWork {
    record: ArtifactScanWorkRecord,
    lease_epoch: u64,
    worker_id: Option<String>,
    lease_token_digest: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
}

#[allow(clippy::too_many_arguments)]
async fn load_artifact_scan_work(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
    expected_candidate_blob_id: &ResourceId,
    operation_id: &ResourceId,
    job_id: &ResourceId,
    for_update: bool,
) -> Result<ArtifactScanWorkRecord, RepositoryError> {
    Ok(load_artifact_scan_work_inner(
        transaction,
        tenant_id,
        artifact_id,
        expected_candidate_blob_id,
        operation_id,
        job_id,
        for_update,
    )
    .await?
    .record)
}

#[allow(clippy::too_many_arguments)]
async fn load_artifact_scan_work_inner(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
    expected_candidate_blob_id: &ResourceId,
    operation_id: &ResourceId,
    job_id: &ResourceId,
    for_update: bool,
) -> Result<LoadedArtifactScanWork, RepositoryError> {
    let job_sql = if for_update {
        r#"
        SELECT state, version, lease_epoch, worker_id, lease_token_digest, lease_expires_at,
               owner_kind, owner_id, invocation_id,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2 AND work_class = 'artifact'
        FOR UPDATE
        "#
    } else {
        r#"
        SELECT state, version, lease_epoch, worker_id, lease_token_digest, lease_expires_at,
               owner_kind, owner_id, invocation_id,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2 AND work_class = 'artifact'
        "#
    };
    let job_row = sqlx::query(job_sql)
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Artifact scan Job"))?;
    let job_payload = payload_from_row(
        &job_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let payload: ArtifactJobPayload = decode_typed_payload(&job_payload, "Artifact Job")?;
    payload
        .validate_for_owner(artifact_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let scan = match payload {
        ArtifactJobPayload::Scan { scan } => scan,
        ArtifactJobPayload::Rescan { scan } => scan,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Artifact scan Job has the wrong payload variant".to_owned(),
            ))
        }
    };
    if job_row.try_get::<String, _>("owner_kind")? != "artifact"
        || job_row.try_get::<String, _>("owner_id")? != artifact_id.to_string()
        || job_row
            .try_get::<Option<String>, _>("invocation_id")?
            .is_some()
        || scan.operation_id != *operation_id
        || scan.artifact_id != *artifact_id
        || scan.blob_id != *expected_candidate_blob_id
    {
        return Err(RepositoryError::CorruptRow(
            "Artifact scan Job identity disagrees with its owner".to_owned(),
        ));
    }

    let artifact = load_artifact_record(transaction, tenant_id, artifact_id).await?;
    let active_blob_id = artifact
        .blob_id
        .as_ref()
        .ok_or_else(|| RepositoryError::CorruptRow("Artifact has no active Blob".to_owned()))?;
    let blob = load_artifact_blob_record(transaction, tenant_id, active_blob_id).await?;
    let job_state = job_row
        .try_get::<String, _>("state")?
        .parse::<JobState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let operation = ArtifactWorkerOperationRecord {
        tenant_id: tenant_id.clone(),
        operation_id: operation_id.clone(),
        state: job_row
            .try_get::<String, _>("state")?
            .parse::<JobState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(
            job_row.try_get("version")?,
            "Artifact scan Operation Job version",
        )?,
        scan_kind: scan.scan_kind,
    };
    if matches!(
        job_state,
        JobState::Ready | JobState::Leased | JobState::Running | JobState::RetryScheduled
    ) && (artifact.version != scan.expected_artifact_version
        || blob.version != scan.expected_blob_version)
    {
        return Err(RepositoryError::CorruptRow(
            "Artifact scan Job parent versions disagree with current authority".to_owned(),
        ));
    }
    Ok(LoadedArtifactScanWork {
        record: ArtifactScanWorkRecord {
            artifact,
            blob,
            operation,
            scan_job_id: job_id.clone(),
            scan_job_state: job_state,
            scan_job_version: parse_u64(job_row.try_get("version")?, "Artifact scan Job version")?,
            scan,
        },
        lease_epoch: parse_u64(job_row.try_get("lease_epoch")?, "Artifact scan lease epoch")?,
        worker_id: job_row.try_get("worker_id")?,
        lease_token_digest: job_row.try_get("lease_token_digest")?,
        lease_expires_at: job_row.try_get("lease_expires_at")?,
    })
}

fn require_artifact_job_fence(
    current: &LoadedArtifactScanWork,
    fence: &insight_platform_jobs::JobFence,
    audit_worker_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    require_raw_artifact_job_fence(
        &current.record.scan_job_state,
        current.record.scan_job_version,
        current.lease_epoch,
        current.worker_id.as_deref(),
        current.lease_token_digest.as_deref(),
        current.lease_expires_at,
        fence,
        audit_worker_id,
        database_now,
    )
}

fn require_current_job_fence(
    current: &JobRecord,
    fence: &JobFence,
    audit_worker_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let state = current
        .state
        .parse::<JobState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    require_raw_artifact_job_fence(
        &state,
        parse_u64(current.version, "Artifact Job version")?,
        parse_u64(current.lease_epoch, "Artifact Job lease epoch")?,
        current.worker_id.as_deref(),
        current.lease_token_digest.as_deref(),
        current.lease_expires_at,
        fence,
        audit_worker_id,
        database_now,
    )
}

#[allow(clippy::too_many_arguments)]
fn require_raw_artifact_job_fence(
    state: &JobState,
    version: u64,
    lease_epoch: u64,
    worker_id: Option<&str>,
    lease_token_digest: Option<&str>,
    lease_expires_at: Option<DateTime<Utc>>,
    fence: &insight_platform_jobs::JobFence,
    audit_worker_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let fenced_worker_id = fence.worker_process_generation_id.to_string();
    if *state != JobState::Running
        || version != fence.expected_version
        || lease_epoch != fence.lease_generation
        || worker_id != Some(fenced_worker_id.as_str())
        || lease_token_digest != Some(fence.token_digest.as_str())
        || &fence.worker_process_generation_id != audit_worker_id
        || lease_expires_at.is_none_or(|expires_at| expires_at <= database_now)
    {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

fn artifact_scan_worker_receipt_payload(
    command: &CommitArtifactScanOutcome,
) -> Result<TypedPayload, RepositoryError> {
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "artifact_id": command.artifact_id,
            "blob_id": command.blob_id,
            "evidence_digest": command.evidence.canonical_digest,
            "fence": {
                "expected_version": command.fence.expected_version,
                "lease_generation": command.fence.lease_generation,
                "lease_token_digest": command.fence.token_digest,
                "worker_process_generation_id": command.fence.worker_process_generation_id,
            },
            "operation_id": command.operation_id,
            "scan_job_id": command.scan_job_id,
        }),
        65_536,
    )
}

async fn persist_artifact_scan_decision(
    transaction: &mut Transaction<'_, Postgres>,
    current: &ArtifactScanWorkRecord,
    command: &CommitArtifactScanOutcome,
    decision: &ArtifactScanDecision,
    database_now: DateTime<Utc>,
    limits: ArtifactCommandLimits,
) -> Result<(), RepositoryError> {
    let blob_version = to_i64(decision.blob_version, "Artifact Blob version")?;
    if current.scan.scan_kind == ArtifactScanKind::Initial {
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifact_blobs
                SET content_digest = $5, size_bytes = $6, state = $7,
                    version = $8,
                    verified_at = CASE WHEN $7 = 'verified' THEN $9 ELSE NULL END,
                    updated_at = $9
                WHERE tenant_id = $1 AND blob_id = $2 AND version = $3
                  AND object_generation = $4
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.blob_id.to_string())
            .bind(to_i64(
                command.expected_blob_version,
                "Artifact Blob version",
            )?)
            .bind(&command.evidence.object_generation)
            .bind(command.evidence.content_digest.to_string())
            .bind(to_i64(command.evidence.size_bytes, "Artifact Blob size")?)
            .bind(decision.blob_state.as_str())
            .bind(blob_version)
            .bind(database_now)
            .execute(&mut **transaction)
            .await?
            .rows_affected(),
            "Artifact scan Blob",
        )?;
    } else if decision.blob_version != current.blob.version {
        ensure_one(
            sqlx::query(
                r#"
                UPDATE insight_platform.artifact_blobs
                SET state = $4, version = $5, updated_at = $6
                WHERE tenant_id = $1 AND blob_id = $2 AND version = $3
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.blob_id.to_string())
            .bind(to_i64(
                command.expected_blob_version,
                "Artifact Blob version",
            )?)
            .bind(decision.blob_state.as_str())
            .bind(blob_version)
            .bind(database_now)
            .execute(&mut **transaction)
            .await?
            .rows_affected(),
            "Artifact rescan Blob",
        )?;
    }
    let metadata = TypedPayload::from_versioned(1, &decision.metadata, 262_144)?;
    ensure_one(
        sqlx::query(
            r#"
            UPDATE insight_platform.artifacts
            SET blob_id = $4, verified_media_type = $5, state = $6, version = $7,
                metadata_schema_version = $8, metadata = $9, metadata_digest = $10,
                terminal_at = CASE WHEN $6 = 'rejected' THEN $11 ELSE terminal_at END,
                updated_at = $11
            WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.artifact_id.to_string())
        .bind(to_i64(
            command.expected_artifact_version,
            "Artifact version",
        )?)
        .bind(decision.artifact_blob_id.to_string())
        .bind(&command.evidence.verified_media_type)
        .bind(decision.artifact_state.as_str())
        .bind(to_i64(decision.artifact_version, "Artifact version")?)
        .bind(metadata.schema_version)
        .bind(&metadata.value)
        .bind(&metadata.digest)
        .bind(database_now)
        .execute(&mut **transaction)
        .await?
        .rows_affected(),
        "Artifact scan outcome",
    )?;
    ensure_one(
        sqlx::query(
            r#"
            UPDATE insight_platform.jobs
            SET state = $4, version = $5,
                result_digest = CASE WHEN $4 IN ('succeeded', 'failed') THEN $7 ELSE NULL END,
                worker_id = NULL, lease_token_digest = NULL, lease_expires_at = NULL,
                heartbeat_at = NULL,
                terminal_at = CASE WHEN $4 IN ('succeeded', 'failed') THEN $6 ELSE NULL END,
                updated_at = $6
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND work_class = 'artifact' AND owner_kind = 'artifact'
              AND state = 'running' AND worker_id = $8 AND lease_epoch = $9
              AND lease_token_digest = $10
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.operation_id.to_string())
        .bind(to_i64(
            command.expected_operation_version,
            "Artifact scan Operation Job version",
        )?)
        .bind(decision.operation_state.as_str())
        .bind(to_i64(
            decision.operation_version,
            "Artifact scan Operation Job version",
        )?)
        .bind(database_now)
        .bind(command.evidence.canonical_digest.to_string())
        .bind(command.fence.worker_process_generation_id.to_string())
        .bind(to_i64(
            command.fence.lease_generation,
            "Artifact verify Job lease generation",
        )?)
        .bind(command.fence.token_digest.to_string())
        .execute(&mut **transaction)
        .await?
        .rows_affected(),
        "Artifact scan Operation Job",
    )?;
    if let Some(cleanup) = decision.duplicate_blob_cleanup.as_ref() {
        insert_scan_duplicate_blob_cleanup_job(transaction, command, cleanup, database_now, limits)
            .await?;
    }
    Ok(())
}

async fn complete_artifact_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    fence: &insight_platform_jobs::JobFence,
    result_digest: &Sha256Digest,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    ensure_one(
        sqlx::query(
            r#"
            UPDATE insight_platform.jobs
            SET state = 'succeeded', version = version + 1, result_digest = $7,
                worker_id = NULL, lease_token_digest = NULL, lease_expires_at = NULL,
                heartbeat_at = NULL, terminal_at = $8, updated_at = $8
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND state = 'running' AND worker_id = $4 AND lease_epoch = $5
              AND lease_token_digest = $6
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .bind(to_i64(fence.expected_version, "Artifact Job version")?)
        .bind(fence.worker_process_generation_id.to_string())
        .bind(to_i64(
            fence.lease_generation,
            "Artifact Job lease generation",
        )?)
        .bind(fence.token_digest.to_string())
        .bind(result_digest.to_string())
        .bind(database_now)
        .execute(&mut **transaction)
        .await?
        .rows_affected(),
        "Artifact Job fence",
    )
}

async fn insert_scan_duplicate_blob_cleanup_job(
    transaction: &mut Transaction<'_, Postgres>,
    command: &CommitArtifactScanOutcome,
    cleanup: &ArtifactBlobCleanupSnapshot,
    database_now: DateTime<Utc>,
    limits: ArtifactCommandLimits,
) -> Result<(), RepositoryError> {
    let artifact_job = ArtifactJobPayload::BlobCleanup {
        cleanup: cleanup.clone(),
    };
    artifact_job
        .validate_for_owner(&cleanup.discarded_blob_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let payload = TypedPayload::new(1, &artifact_job)?;
    let attempt_limit = i32::try_from(limits.maximum_job_attempts()).map_err(|_| {
        RepositoryError::InvalidInput("Artifact cleanup attempt limit exceeds integer".to_owned())
    })?;
    let cleanup_deadline = database_now
        .checked_add_signed(Duration::seconds(limits.maximum_staging_seconds()))
        .ok_or_else(|| {
            RepositoryError::InvalidInput("Artifact cleanup deadline overflows".to_owned())
        })?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
            tenant_id, job_id, work_class, owner_kind, owner_id,
            state, attempt_limit, scheduled_at, deadline, request_digest,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'artifact', 'internal_blob', $3,
                  'ready', $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.duplicate_blob_cleanup_job_id.to_string())
    .bind(command.blob_id.to_string())
    .bind(attempt_limit)
    .bind(database_now)
    .bind(cleanup_deadline)
    .bind(command.audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

struct LockedBlobCleanupWork {
    blob: ArtifactBlobRecord,
    cleanup: ArtifactBlobCleanupSnapshot,
    job_state: JobState,
    job_version: u64,
    lease_epoch: u64,
    worker_id: Option<String>,
    lease_token_digest: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
}

async fn lock_blob_cleanup_work(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    blob_id: &ResourceId,
    job_id: &ResourceId,
) -> Result<LockedBlobCleanupWork, RepositoryError> {
    let job = sqlx::query(
        r#"
        SELECT state, version, lease_epoch, worker_id, lease_token_digest, lease_expires_at,
               owner_kind, owner_id, payload_schema_version, payload, payload_digest
        FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2 AND work_class = 'artifact'
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact Blob cleanup Job"))?;
    let payload = payload_from_row(&job, "payload_schema_version", "payload", "payload_digest")?;
    let payload: ArtifactJobPayload = decode_typed_payload(&payload, "Artifact Job")?;
    payload
        .validate_for_owner(blob_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ArtifactJobPayload::BlobCleanup { cleanup } = payload else {
        return Err(RepositoryError::CorruptRow(
            "Artifact Blob cleanup Job has the wrong payload variant".to_owned(),
        ));
    };
    if job.try_get::<String, _>("owner_kind")? != "internal_blob"
        || job.try_get::<String, _>("owner_id")? != blob_id.to_string()
        || cleanup.discarded_blob_id != *blob_id
    {
        return Err(RepositoryError::CorruptRow(
            "Artifact Blob cleanup Job has the wrong owner".to_owned(),
        ));
    }
    let blob_row = sqlx::query(
        r#"
        SELECT tenant_id, blob_id, backend, storage_binding_digest,
               security_domain_digest, object_generation, encryption_domain_id,
               content_digest, size_bytes, state, version
        FROM insight_platform.artifact_blobs
        WHERE tenant_id = $1 AND blob_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact Blob cleanup target"))?;
    Ok(LockedBlobCleanupWork {
        blob: blob_from_row(blob_row)?,
        cleanup,
        job_state: job
            .try_get::<String, _>("state")?
            .parse::<JobState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        job_version: parse_u64(job.try_get("version")?, "Artifact cleanup Job version")?,
        lease_epoch: parse_u64(job.try_get("lease_epoch")?, "Artifact cleanup lease epoch")?,
        worker_id: job.try_get("worker_id")?,
        lease_token_digest: job.try_get("lease_token_digest")?,
        lease_expires_at: job.try_get("lease_expires_at")?,
    })
}

async fn load_completed_blob_cleanup(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    blob_id: &ResourceId,
    job_id: &ResourceId,
    for_update: bool,
) -> Result<CompletedArtifactBlobCleanup, RepositoryError> {
    let blob = load_artifact_blob_record(transaction, tenant_id, blob_id).await?;
    let job_sql = if for_update {
        "SELECT state, version, owner_kind, owner_id, payload_schema_version, payload, payload_digest FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 AND work_class = 'artifact' FOR UPDATE"
    } else {
        "SELECT state, version, owner_kind, owner_id, payload_schema_version, payload, payload_digest FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 AND work_class = 'artifact'"
    };
    let job = sqlx::query(job_sql)
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Artifact Blob cleanup Job"))?;
    let payload = payload_from_row(&job, "payload_schema_version", "payload", "payload_digest")?;
    let payload: ArtifactJobPayload = decode_typed_payload(&payload, "Artifact Job")?;
    payload
        .validate_for_owner(blob_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ArtifactJobPayload::BlobCleanup { cleanup } = payload else {
        return Err(RepositoryError::CorruptRow(
            "Artifact Blob cleanup Job has the wrong payload variant".to_owned(),
        ));
    };
    if job.try_get::<String, _>("owner_kind")? != "internal_blob"
        || job.try_get::<String, _>("owner_id")? != blob_id.to_string()
        || cleanup.discarded_blob_id != *blob_id
    {
        return Err(RepositoryError::CorruptRow(
            "Artifact Blob cleanup Job has the wrong owner".to_owned(),
        ));
    }
    Ok(CompletedArtifactBlobCleanup {
        blob,
        cleanup_job_id: job_id.clone(),
        cleanup_job_state: job
            .try_get::<String, _>("state")?
            .parse::<JobState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        cleanup_job_version: parse_u64(
            job.try_get("version")?,
            "Artifact Blob cleanup Job version",
        )?,
    })
}

async fn lock_and_load_reusable_blob(
    transaction: &mut Transaction<'_, Postgres>,
    candidate: &ArtifactBlobRecord,
    content_digest: &Sha256Digest,
    size_bytes: u64,
) -> Result<Option<ArtifactBlobRecord>, RepositoryError> {
    let lock_key = format!(
        "{}|{}|{}|{}|{}|{}",
        candidate.tenant_id,
        candidate.backend,
        candidate.storage_binding_digest,
        candidate.encryption_domain_id,
        candidate.security_domain_digest,
        content_digest,
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(lock_key)
        .execute(&mut **transaction)
        .await?;
    let row = sqlx::query(
        r#"
        SELECT tenant_id, blob_id, backend, storage_binding_digest,
               security_domain_digest, object_generation, encryption_domain_id,
               content_digest, size_bytes, state, version
        FROM insight_platform.artifact_blobs
        WHERE tenant_id = $1 AND backend = $2 AND storage_binding_digest = $3
          AND encryption_domain_id = $4 AND security_domain_digest = $5
          AND content_digest = $6 AND state = 'verified' AND deleted_at IS NULL
          AND blob_id <> $7
        ORDER BY blob_id
        FOR UPDATE
        LIMIT 1
        "#,
    )
    .bind(candidate.tenant_id.to_string())
    .bind(&candidate.backend)
    .bind(candidate.storage_binding_digest.to_string())
    .bind(candidate.encryption_domain_id.to_string())
    .bind(candidate.security_domain_digest.to_string())
    .bind(content_digest.to_string())
    .bind(candidate.blob_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let reusable = blob_from_row(row)?;
    if reusable.size_bytes != Some(size_bytes) {
        return Err(RepositoryError::CorruptRow(
            "same-digest Artifact Blob has a different byte length".to_owned(),
        ));
    }
    Ok(Some(reusable))
}

async fn lock_artifacts(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_ids: &[&ResourceId],
) -> Result<(), RepositoryError> {
    let artifact_ids = artifact_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT artifact_id FROM insight_platform.artifacts
        WHERE tenant_id = $1 AND artifact_id = ANY($2)
        ORDER BY artifact_id
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(&artifact_ids)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != artifact_ids.len() {
        return Err(RepositoryError::NotFound("Artifact link endpoint"));
    }
    Ok(())
}

async fn load_artifact_record(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
) -> Result<ArtifactRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, artifact_id, blob_id, purpose, classification,
               expected_size_bytes, expected_digest, declared_media_type,
               verified_media_type, state, version, metadata_schema_version,
               metadata, metadata_digest, retention_policy_revision_id,
               retain_until, created_by, created_at, updated_at, terminal_at
        FROM insight_platform.artifacts
        WHERE tenant_id = $1 AND artifact_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact"))?;
    artifact_from_row(row)
}

fn artifact_ref_from_gateway_rows(
    artifact: &ArtifactRecord,
    blob: &PgRow,
) -> Result<ArtifactRef, RepositoryError> {
    let content_digest = blob
        .try_get::<Option<String>, _>("content_digest")?
        .ok_or_else(|| RepositoryError::CorruptRow("Ready Blob has no digest".to_owned()))?;
    let size_bytes = blob
        .try_get::<Option<i64>, _>("size_bytes")?
        .ok_or_else(|| RepositoryError::CorruptRow("Ready Blob has no size".to_owned()))?;
    ArtifactRef::new(
        artifact.artifact_id.clone(),
        parse_digest(content_digest, "Artifact content digest")?,
        parse_u64(size_bytes, "Artifact content size")?,
        artifact.verified_media_type.clone().ok_or_else(|| {
            RepositoryError::CorruptRow("Ready Artifact has no media type".to_owned())
        })?,
        artifact.classification,
        artifact.metadata.display_name.clone(),
    )
    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
}

async fn require_artifact_link_capacity(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
    maximum: u64,
) -> Result<(), RepositoryError> {
    let count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND released_at IS NULL
          AND (source_artifact_id = $2 OR target_artifact_id = $2)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    let maximum = i64::try_from(maximum).map_err(|_| {
        RepositoryError::InvalidInput("Artifact link limit exceeds bigint".to_owned())
    })?;
    if count >= maximum {
        return Err(RepositoryError::QuotaExceeded);
    }
    Ok(())
}

async fn require_acyclic_provenance(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    source_artifact_id: &ResourceId,
    derived_artifact_id: &ResourceId,
    maximum_depth: u64,
) -> Result<(), RepositoryError> {
    let maximum_depth = i64::try_from(maximum_depth).map_err(|_| {
        RepositoryError::InvalidInput("Artifact provenance depth exceeds bigint".to_owned())
    })?;
    let (cycle, truncated): (bool, bool) = sqlx::query_as(
        r#"
        WITH RECURSIVE walk(current_id, depth, path) AS (
            SELECT $3::text, 0::bigint, ARRAY[$3::text]
            UNION ALL
            SELECT link.target_artifact_id, walk.depth + 1,
                   walk.path || link.target_artifact_id
            FROM walk
            JOIN insight_platform.artifact_links AS link
              ON link.tenant_id = $1
             AND link.link_kind = 'provenance'
             AND link.state = 'active' AND link.released_at IS NULL
             AND link.source_artifact_id = walk.current_id
            WHERE walk.depth < $4
              AND NOT link.target_artifact_id = ANY(walk.path)
        )
        SELECT
          EXISTS(SELECT 1 FROM walk WHERE current_id = $2 AND depth > 0),
          EXISTS(
            SELECT 1 FROM walk
            JOIN insight_platform.artifact_links AS link
              ON link.tenant_id = $1
             AND link.link_kind = 'provenance'
             AND link.state = 'active' AND link.released_at IS NULL
             AND link.source_artifact_id = walk.current_id
            WHERE walk.depth = $4
          )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(source_artifact_id.to_string())
    .bind(derived_artifact_id.to_string())
    .bind(maximum_depth)
    .fetch_one(&mut **transaction)
    .await?;
    if cycle || truncated {
        return Err(RepositoryError::InvalidInput(
            "Artifact provenance would create a cycle or exceed the bounded depth".to_owned(),
        ));
    }
    Ok(())
}

async fn load_artifact_hold(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_hold_id: &ResourceId,
    artifact_id: &ResourceId,
) -> Result<ArtifactHoldRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, artifact_link_id, target_artifact_id, state, version,
               link_key_digest, payload_schema_version, payload, payload_digest,
               created_at, released_at
        FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND artifact_link_id = $2
          AND link_kind = 'hold' AND owner_kind = 'principal'
          AND target_artifact_id = $3 AND source_artifact_id IS NULL
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_hold_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact hold"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    Ok(ArtifactHoldRecord {
        tenant_id: parse_id(row.try_get("tenant_id")?, "Artifact hold tenant")?,
        artifact_hold_id: parse_id(row.try_get("artifact_link_id")?, "Artifact hold")?,
        artifact_id: parse_id(row.try_get("target_artifact_id")?, "held Artifact")?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<ArtifactLinkState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "Artifact hold version")?,
        snapshot: decode_versioned_payload::<ArtifactHoldSnapshot>(&payload, "Artifact hold")?,
        link_key_digest: parse_digest(row.try_get("link_key_digest")?, "Artifact hold link key")?,
        created_at: row.try_get("created_at")?,
        released_at: row.try_get("released_at")?,
    })
}

async fn load_artifact_provenance(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    provenance_link_id: &ResourceId,
    source_artifact_id: &ResourceId,
    derived_artifact_id: &ResourceId,
) -> Result<ArtifactProvenanceRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, artifact_link_id, source_artifact_id, target_artifact_id,
               state, version, link_key_digest, payload_schema_version, payload,
               payload_digest, created_at
        FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND artifact_link_id = $2
          AND link_kind = 'provenance' AND owner_kind = 'artifact_producer'
          AND source_artifact_id = $3 AND target_artifact_id = $4
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(provenance_link_id.to_string())
    .bind(source_artifact_id.to_string())
    .bind(derived_artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact provenance"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    Ok(ArtifactProvenanceRecord {
        tenant_id: parse_id(row.try_get("tenant_id")?, "Artifact provenance tenant")?,
        provenance_link_id: parse_id(row.try_get("artifact_link_id")?, "Artifact provenance")?,
        source_artifact_id: parse_id(row.try_get("source_artifact_id")?, "source Artifact")?,
        derived_artifact_id: parse_id(row.try_get("target_artifact_id")?, "derived Artifact")?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<ArtifactLinkState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "Artifact provenance version")?,
        snapshot: decode_versioned_payload::<ArtifactProvenanceSnapshot>(
            &payload,
            "Artifact provenance",
        )?,
        link_key_digest: parse_digest(
            row.try_get("link_key_digest")?,
            "Artifact provenance link key",
        )?,
        created_at: row.try_get("created_at")?,
    })
}

async fn load_artifact_reference(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_reference_id: &ResourceId,
    artifact_id: &ResourceId,
) -> Result<ArtifactReferenceRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, artifact_link_id, target_artifact_id, state, version,
               link_key_digest, payload_schema_version, payload, payload_digest,
               created_at, released_at
        FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND artifact_link_id = $2
          AND link_kind = 'reference' AND target_artifact_id = $3
          AND source_artifact_id IS NULL
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_reference_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact reference"))?;
    reference_from_row(row)
}

async fn reserve_staging_quota(
    transaction: &mut Transaction<'_, Postgres>,
    command: &PrepareArtifact,
) -> Result<(), RepositoryError> {
    let amount = i64::try_from(command.expected_size_bytes).map_err(|_| {
        RepositoryError::InvalidInput("Artifact size exceeds PostgreSQL bigint".to_owned())
    })?;
    let account = sqlx::query(
        r#"
        SELECT scope_kind, scope_id, work_class, metric, limit_value,
               reserved_value, used_value, version
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND quota_account_id = $2
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.quota_account_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact staging quota account"))?;
    let scope_kind: String = account.try_get("scope_kind")?;
    let scope_id: String = account.try_get("scope_id")?;
    let work_class: String = account.try_get("work_class")?;
    let metric: String = account.try_get("metric")?;
    if scope_kind != "tenant"
        || scope_id != command.audit.tenant_id.to_string()
        || work_class != "artifact"
        || metric != "artifact.staging_bytes"
    {
        return Err(RepositoryError::InvalidInput(
            "quota account is not the tenant Artifact staging authority".to_owned(),
        ));
    }
    let limit_value: i64 = account.try_get("limit_value")?;
    let reserved_value: i64 = account.try_get("reserved_value")?;
    let used_value: i64 = account.try_get("used_value")?;
    let version: i64 = account.try_get("version")?;
    if reserved_value
        .checked_add(used_value)
        .and_then(|current| current.checked_add(amount))
        .is_none_or(|next| next > limit_value)
    {
        return Err(RepositoryError::QuotaExceeded);
    }
    if amount == 0 {
        return Ok(());
    }
    let next_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.quota_accounts
        SET reserved_value = reserved_value + $4, version = version + 1,
            updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
        RETURNING version
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.quota_account_id.to_string())
    .bind(version)
    .bind(amount)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Artifact staging quota account"))?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_ledger (
            tenant_id, quota_entry_id, quota_account_id, correlation_id,
            entry_kind, reserved_amount, used_amount, account_version, request_digest
        ) VALUES ($1, $2, $3, $4, 'reserve', $5, 0, $6, $7)
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.quota_entry_id.to_string())
    .bind(command.quota_account_id.to_string())
    .bind(command.artifact_id.to_string())
    .bind(amount)
    .bind(next_version)
    .bind(command.audit.request_digest.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn load_artifact_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
    upload_grant_id: &ResourceId,
    operation_id: &ResourceId,
) -> Result<PreparedArtifact, RepositoryError> {
    let artifact = sqlx::query(
        r#"
        SELECT tenant_id, artifact_id, blob_id, purpose, classification,
               expected_size_bytes, expected_digest, declared_media_type,
               verified_media_type, state, version, metadata_schema_version,
               metadata, metadata_digest, retention_policy_revision_id,
               retain_until, created_by, created_at, updated_at, terminal_at
        FROM insight_platform.artifacts
        WHERE tenant_id = $1 AND artifact_id = $2 AND blob_id = $3
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .bind(blob_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("prepared Artifact"))?;
    let blob = sqlx::query(
        r#"
        SELECT tenant_id, blob_id, backend, storage_binding_digest,
               security_domain_digest, object_generation, encryption_domain_id, content_digest,
               size_bytes, state, version
        FROM insight_platform.artifact_blobs
        WHERE tenant_id = $1 AND blob_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("prepared Artifact Blob"))?;
    let grant = sqlx::query(
        r#"
        SELECT tenant_id, artifact_link_id, target_artifact_id, state, version,
               link_key_digest, payload_schema_version, payload, payload_digest,
               created_at, released_at
        FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND artifact_link_id = $2
          AND link_kind = 'grant' AND owner_kind = 'job'
          AND owner_id = $3 AND target_artifact_id = $4
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(upload_grant_id.to_string())
    .bind(operation_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact upload grant"))?;
    let operation = sqlx::query(
        r#"
        SELECT tenant_id, job_id, state, version, payload_schema_version,
               payload, payload_digest, deadline, created_at
        FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2 AND work_class = 'artifact'
          AND owner_kind = 'artifact' AND owner_id = $3
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(operation_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact upload Job"))?;

    let artifact = artifact_from_row(artifact)?;
    Ok(PreparedArtifact {
        operation: operation_from_row(operation, Some(&artifact))?,
        artifact,
        blob: blob_from_row(blob)?,
        grant: grant_from_row(grant)?,
    })
}

fn artifact_from_row(row: PgRow) -> Result<ArtifactRecord, RepositoryError> {
    let metadata = payload_from_row(
        &row,
        "metadata_schema_version",
        "metadata",
        "metadata_digest",
    )?;
    let record = ArtifactRecord {
        tenant_id: parse_id(row.try_get("tenant_id")?, "Artifact tenant")?,
        artifact_id: parse_id(row.try_get("artifact_id")?, "Artifact")?,
        blob_id: row
            .try_get::<Option<String>, _>("blob_id")?
            .map(|value| parse_id(value, "Artifact Blob"))
            .transpose()?,
        purpose: row
            .try_get::<String, _>("purpose")?
            .parse::<ArtifactPurpose>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        classification: DataClassification::from_str(&row.try_get::<String, _>("classification")?)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        expected_size_bytes: parse_u64(row.try_get("expected_size_bytes")?, "Artifact size")?,
        expected_digest: row
            .try_get::<Option<String>, _>("expected_digest")?
            .map(|value| parse_digest(value, "Artifact expected digest"))
            .transpose()?,
        declared_media_type: row.try_get("declared_media_type")?,
        verified_media_type: row.try_get("verified_media_type")?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<ArtifactState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "Artifact version")?,
        metadata: decode_versioned_payload::<ArtifactMetadataSnapshot>(
            &metadata,
            "Artifact metadata",
        )?,
        retention_policy_revision_id: parse_id(
            row.try_get("retention_policy_revision_id")?,
            "Artifact retention Policy Revision",
        )?,
        retain_until: row.try_get("retain_until")?,
        created_by: parse_id(row.try_get("created_by")?, "Artifact creator")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        terminal_at: row.try_get("terminal_at")?,
    };
    record
        .metadata
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(record)
}

fn blob_from_row(row: PgRow) -> Result<ArtifactBlobRecord, RepositoryError> {
    Ok(ArtifactBlobRecord {
        tenant_id: parse_id(row.try_get("tenant_id")?, "Blob tenant")?,
        blob_id: parse_id(row.try_get("blob_id")?, "Blob")?,
        backend: row.try_get("backend")?,
        storage_binding_digest: parse_digest(
            row.try_get("storage_binding_digest")?,
            "Blob storage binding",
        )?,
        security_domain_digest: parse_digest(
            row.try_get("security_domain_digest")?,
            "Blob security domain",
        )?,
        object_generation: row.try_get("object_generation")?,
        encryption_domain_id: parse_id(
            row.try_get("encryption_domain_id")?,
            "Blob encryption domain",
        )?,
        content_digest: row
            .try_get::<Option<String>, _>("content_digest")?
            .map(|value| parse_digest(value, "Blob content digest"))
            .transpose()?,
        size_bytes: row
            .try_get::<Option<i64>, _>("size_bytes")?
            .map(|value| parse_u64(value, "Blob size"))
            .transpose()?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<BlobIntegrityState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "Blob version")?,
    })
}

fn grant_from_row(row: PgRow) -> Result<ArtifactGrantRecord, RepositoryError> {
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    Ok(ArtifactGrantRecord {
        tenant_id: parse_id(row.try_get("tenant_id")?, "ArtifactGrant tenant")?,
        upload_grant_id: parse_id(row.try_get("artifact_link_id")?, "ArtifactGrant")?,
        artifact_id: parse_id(row.try_get("target_artifact_id")?, "granted Artifact")?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<ArtifactLinkState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "ArtifactGrant version")?,
        snapshot: decode_versioned_payload::<UploadGrantSnapshot>(&payload, "ArtifactGrant")?,
        link_key_digest: parse_digest(row.try_get("link_key_digest")?, "ArtifactGrant key")?,
        created_at: row.try_get("created_at")?,
    })
}

fn operation_from_row(
    row: PgRow,
    artifact: Option<&ArtifactRecord>,
) -> Result<ArtifactOperationRecord, RepositoryError> {
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let snapshot = match decode_versioned_payload(&payload, "Artifact upload Job") {
        Ok(snapshot) => snapshot,
        Err(_) => {
            let payload: ArtifactJobPayload =
                decode_typed_payload(&payload, "Artifact verify Job")?;
            let ArtifactJobPayload::Scan { scan } = payload else {
                return Err(RepositoryError::CorruptRow(
                    "Artifact upload Job has the wrong payload variant".to_owned(),
                ));
            };
            let artifact = artifact.ok_or_else(|| {
                RepositoryError::CorruptRow(
                    "Artifact verify Job cannot reconstruct its public upload target".to_owned(),
                )
            })?;
            if scan.artifact_id != artifact.artifact_id {
                return Err(RepositoryError::CorruptRow(
                    "Artifact verify Job has the wrong target".to_owned(),
                ));
            }
            insight_platform_artifacts::ArtifactUploadOperationSnapshot {
                schema_version: 1,
                artifact_id: artifact.artifact_id.clone(),
                purpose: artifact.purpose,
                expected_size_bytes: artifact.expected_size_bytes,
                expected_digest: artifact.expected_digest.clone(),
                retention_policy_revision_id: artifact.retention_policy_revision_id.clone(),
            }
        }
    };
    Ok(ArtifactOperationRecord {
        tenant_id: parse_id(row.try_get("tenant_id")?, "Artifact operation tenant")?,
        operation_id: parse_id(row.try_get("job_id")?, "Artifact operation Job")?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<JobState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "Artifact operation Job version")?,
        snapshot,
        deadline: row.try_get("deadline")?,
        created_at: row.try_get("created_at")?,
    })
}

fn parse_id(value: String, kind: &str) -> Result<ResourceId, RepositoryError> {
    value
        .parse()
        .map_err(|failure| RepositoryError::CorruptRow(format!("{kind}: {failure}")))
}

fn parse_digest(value: String, kind: &str) -> Result<Sha256Digest, RepositoryError> {
    value
        .parse()
        .map_err(|failure| RepositoryError::CorruptRow(format!("{kind}: {failure}")))
}

fn parse_u64(value: i64, kind: &str) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::CorruptRow(format!("negative {kind}")))
}

fn decode_versioned_payload<T: DeserializeOwned>(
    payload: &TypedPayload,
    kind: &str,
) -> Result<T, RepositoryError> {
    serde_json::from_value(payload.value.clone())
        .map_err(|failure| RepositoryError::CorruptRow(format!("{kind}: {failure}")))
}

async fn lock_upload_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    command: &CompleteArtifactUpload,
) -> Result<(), RepositoryError> {
    for (query, identity, kind) in [
        (
            "SELECT artifact_id FROM insight_platform.artifacts WHERE tenant_id = $1 AND artifact_id = $2 FOR UPDATE",
            command.artifact_id.to_string(),
            "Artifact",
        ),
        (
            "SELECT blob_id FROM insight_platform.artifact_blobs WHERE tenant_id = $1 AND blob_id = $2 FOR UPDATE",
            command.blob_id.to_string(),
            "Artifact Blob",
        ),
        (
            "SELECT artifact_link_id FROM insight_platform.artifact_links WHERE tenant_id = $1 AND artifact_link_id = $2 FOR UPDATE",
            command.upload_grant_id.to_string(),
            "ArtifactGrant",
        ),
        (
            "SELECT job_id FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 AND work_class = 'artifact' AND owner_kind = 'artifact' FOR UPDATE",
            command.operation_id.to_string(),
            "Artifact upload Operation Job",
        ),
    ] {
        let found: Option<String> = sqlx::query_scalar(query)
            .bind(command.audit.tenant_id.to_string())
            .bind(identity)
            .fetch_optional(&mut **transaction)
            .await?;
        if found.is_none() {
            return Err(RepositoryError::NotFound(kind));
        }
    }
    Ok(())
}

fn completed_upload(bundle: PreparedArtifact) -> CompletedArtifactUpload {
    CompletedArtifactUpload {
        artifact: bundle.artifact,
        blob: bundle.blob,
        grant: bundle.grant,
        operation: bundle.operation,
    }
}

fn to_i64(value: u64, kind: &str) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::InvalidInput(format!("{kind} exceeds PostgreSQL bigint")))
}

fn ensure_one(rows_affected: u64, kind: &'static str) -> Result<(), RepositoryError> {
    if rows_affected == 1 {
        Ok(())
    } else {
        Err(RepositoryError::Conflict(kind))
    }
}

struct VerificationRecords {
    artifact: ArtifactRecord,
    blob: ArtifactBlobRecord,
    operation: ArtifactOperationRecord,
}

async fn load_verification_records(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
    operation_id: &ResourceId,
) -> Result<VerificationRecords, RepositoryError> {
    let artifact = sqlx::query(
        r#"
        SELECT tenant_id, artifact_id, blob_id, purpose, classification,
               expected_size_bytes, expected_digest, declared_media_type,
               verified_media_type, state, version, metadata_schema_version,
               metadata, metadata_digest, retention_policy_revision_id,
               retain_until, created_by, created_at, updated_at, terminal_at
        FROM insight_platform.artifacts
        WHERE tenant_id = $1 AND artifact_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact verification target"))?;
    let artifact = artifact_from_row(artifact)?;
    let blob_id = artifact
        .blob_id
        .as_ref()
        .ok_or_else(|| RepositoryError::CorruptRow("Artifact has no Blob".to_owned()))?;
    let blob = sqlx::query(
        r#"
        SELECT tenant_id, blob_id, backend, storage_binding_digest,
               security_domain_digest, object_generation, encryption_domain_id, content_digest,
               size_bytes, state, version
        FROM insight_platform.artifact_blobs
        WHERE tenant_id = $1 AND blob_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact verification Blob"))?;
    let operation = sqlx::query(
        r#"
        SELECT tenant_id, job_id, state, version, payload_schema_version,
               payload, payload_digest, deadline, created_at
        FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2 AND work_class = 'artifact'
          AND owner_kind = 'artifact' AND owner_id = $3
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(operation_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact verification Job"))?;
    let operation = operation_from_row(operation, Some(&artifact))?;
    Ok(VerificationRecords {
        artifact,
        blob: blob_from_row(blob)?,
        operation,
    })
}

async fn lock_verification_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
    operation_id: &ResourceId,
) -> Result<(), RepositoryError> {
    for (query, identity, kind) in [
        (
            "SELECT artifact_id FROM insight_platform.artifacts WHERE tenant_id = $1 AND artifact_id = $2 FOR UPDATE",
            artifact_id.to_string(),
            "Artifact",
        ),
        (
            "SELECT blob_id FROM insight_platform.artifact_blobs WHERE tenant_id = $1 AND blob_id = $2 FOR UPDATE",
            blob_id.to_string(),
            "Artifact Blob",
        ),
        (
            "SELECT job_id FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 AND work_class = 'artifact' AND owner_kind = 'artifact' FOR UPDATE",
            operation_id.to_string(),
            "Artifact upload Operation Job",
        ),
    ] {
        let found: Option<String> = sqlx::query_scalar(query)
            .bind(tenant_id.to_string())
            .bind(identity)
            .fetch_optional(&mut **transaction)
            .await?;
        if found.is_none() {
            return Err(RepositoryError::NotFound(kind));
        }
    }
    Ok(())
}

struct LockedStagingQuota {
    amount: i64,
    account_version: i64,
}

async fn lock_staging_quota(
    transaction: &mut Transaction<'_, Postgres>,
    command: &FinalizeArtifact,
) -> Result<LockedStagingQuota, RepositoryError> {
    let account = sqlx::query(
        r#"
        SELECT scope_kind, scope_id, work_class, metric, reserved_value,
               used_value, version
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND quota_account_id = $2
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.quota_account_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact staging quota account"))?;
    let scope_kind: String = account.try_get("scope_kind")?;
    let scope_id: String = account.try_get("scope_id")?;
    let work_class: String = account.try_get("work_class")?;
    let metric: String = account.try_get("metric")?;
    let reserved_value: i64 = account.try_get("reserved_value")?;
    let account_version: i64 = account.try_get("version")?;
    let expected_version = to_i64(
        command.expected_quota_account_version,
        "Artifact staging quota account version",
    )?;
    if scope_kind != "tenant"
        || scope_id != command.audit.tenant_id.to_string()
        || work_class != "artifact"
        || metric != "artifact.staging_bytes"
    {
        return Err(RepositoryError::InvalidInput(
            "quota account is not the tenant Artifact staging authority".to_owned(),
        ));
    }
    if account_version != expected_version {
        return Err(RepositoryError::Conflict(
            "Artifact staging quota account version",
        ));
    }
    let amount = to_i64(command.size_bytes, "Artifact staging quota amount")?;
    if reserved_value < amount {
        return Err(RepositoryError::Conflict(
            "Artifact staging quota reservation",
        ));
    }
    if amount > 0 {
        let reserved_amount: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT reserved_amount
            FROM insight_platform.quota_ledger
            WHERE tenant_id = $1 AND quota_account_id = $2
              AND correlation_id = $3 AND entry_kind = 'reserve'
            FOR SHARE
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.quota_account_id.to_string())
        .bind(command.artifact_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?;
        if reserved_amount != Some(amount) {
            return Err(RepositoryError::Conflict(
                "Artifact staging quota reservation",
            ));
        }
    }
    Ok(LockedStagingQuota {
        amount,
        account_version,
    })
}

async fn settle_locked_staging_quota(
    transaction: &mut Transaction<'_, Postgres>,
    command: &FinalizeArtifact,
    locked: LockedStagingQuota,
) -> Result<(), RepositoryError> {
    if locked.amount == 0 {
        return Ok(());
    }
    let next_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.quota_accounts
        SET reserved_value = reserved_value - $4, version = version + 1,
            updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
          AND reserved_value >= $4
        RETURNING version
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.quota_account_id.to_string())
    .bind(locked.account_version)
    .bind(locked.amount)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Artifact staging quota account"))?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_ledger (
            tenant_id, quota_entry_id, quota_account_id, correlation_id,
            entry_kind, reserved_amount, used_amount, account_version, request_digest
        ) VALUES ($1, $2, $3, $4, 'settle', $5, 0, $6, $7)
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.quota_settle_entry_id.to_string())
    .bind(command.quota_account_id.to_string())
    .bind(command.artifact_id.to_string())
    .bind(locked.amount)
    .bind(next_version)
    .bind(command.audit.request_digest.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn lock_upload_bundle_for_finalize(
    transaction: &mut Transaction<'_, Postgres>,
    command: &FinalizeArtifact,
) -> Result<(), RepositoryError> {
    for (query, identity, kind) in [
        (
            "SELECT artifact_id FROM insight_platform.artifacts WHERE tenant_id = $1 AND artifact_id = $2 FOR UPDATE",
            command.artifact_id.to_string(),
            "Artifact",
        ),
        (
            "SELECT blob_id FROM insight_platform.artifact_blobs WHERE tenant_id = $1 AND blob_id = $2 FOR UPDATE",
            command.blob_id.to_string(),
            "Artifact Blob",
        ),
        (
            "SELECT artifact_link_id FROM insight_platform.artifact_links WHERE tenant_id = $1 AND artifact_link_id = $2 FOR UPDATE",
            command.upload_grant_id.to_string(),
            "ArtifactGrant",
        ),
        (
            "SELECT job_id FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 AND work_class = 'artifact' AND owner_kind = 'artifact' FOR UPDATE",
            command.operation_id.to_string(),
            "Artifact upload Operation Job",
        ),
    ] {
        let found: Option<String> = sqlx::query_scalar(query)
            .bind(command.audit.tenant_id.to_string())
            .bind(identity)
            .fetch_optional(&mut **transaction)
            .await?;
        if found.is_none() {
            return Err(RepositoryError::NotFound(kind));
        }
    }
    Ok(())
}

async fn load_finalized_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    command: &FinalizeArtifact,
) -> Result<FinalizedArtifact, RepositoryError> {
    let records = load_verification_records(
        transaction,
        &command.audit.tenant_id,
        &command.artifact_id,
        &command.operation_id,
    )
    .await?;
    if records.artifact.state != ArtifactState::Ready
        || records.blob.state != BlobIntegrityState::Verified
        || records.operation.state != JobState::Succeeded
    {
        return Err(RepositoryError::CorruptRow(
            "finalized Artifact aggregate is not terminal-ready".to_owned(),
        ));
    }
    let row = sqlx::query(
        r#"
        SELECT tenant_id, artifact_link_id, target_artifact_id, state, version,
               link_key_digest, payload_schema_version, payload, payload_digest,
               created_at, released_at
        FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND artifact_link_id = $2
          AND link_kind = 'reference' AND owner_kind = 'job'
          AND owner_id = $3 AND target_artifact_id = $4
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.artifact_reference_id.to_string())
    .bind(command.operation_id.to_string())
    .bind(command.artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Artifact owner Reference"))?;
    let reference = reference_from_row(row)?;
    let content_digest = records.blob.content_digest.clone().ok_or_else(|| {
        RepositoryError::CorruptRow("Ready Artifact Blob has no content digest".to_owned())
    })?;
    let size_bytes = records.blob.size_bytes.ok_or_else(|| {
        RepositoryError::CorruptRow("Ready Artifact Blob has no byte length".to_owned())
    })?;
    let media_type = records
        .artifact
        .verified_media_type
        .clone()
        .ok_or_else(|| {
            RepositoryError::CorruptRow("Ready Artifact has no verified media".to_owned())
        })?;
    let artifact_ref = ArtifactRef::new(
        records.artifact.artifact_id.clone(),
        content_digest,
        size_bytes,
        media_type,
        records.artifact.classification,
        records.artifact.metadata.display_name.clone(),
    )
    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(FinalizedArtifact {
        artifact: records.artifact,
        blob: records.blob,
        operation: records.operation,
        reference,
        artifact_ref,
    })
}

fn reference_from_row(row: PgRow) -> Result<ArtifactReferenceRecord, RepositoryError> {
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    Ok(ArtifactReferenceRecord {
        tenant_id: parse_id(row.try_get("tenant_id")?, "ArtifactReference tenant")?,
        artifact_reference_id: parse_id(row.try_get("artifact_link_id")?, "ArtifactReference")?,
        artifact_id: parse_id(row.try_get("target_artifact_id")?, "referenced Artifact")?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<ArtifactLinkState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "ArtifactReference version")?,
        snapshot: decode_versioned_payload::<ArtifactReferenceSnapshot>(
            &payload,
            "ArtifactReference",
        )?,
        link_key_digest: parse_digest(row.try_get("link_key_digest")?, "ArtifactReference key")?,
        created_at: row.try_get("created_at")?,
        released_at: row.try_get("released_at")?,
    })
}
