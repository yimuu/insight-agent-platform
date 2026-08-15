use chrono::{Duration, Utc};
use insight_platform_artifacts::{
    ArtifactBackendFailure, ArtifactBlobBackend, ArtifactBlobCleanupExecution,
    ArtifactBlobDeletionEvidence, ArtifactDeletionEvidence, ArtifactDeletionExecution,
    ArtifactDeletionJobSnapshot, ArtifactDeletionMode, ArtifactHoldKind, ArtifactHoldRecord,
    ArtifactJobPayload, ArtifactLinkState, ArtifactProvenanceRecord, ArtifactReferenceRecord,
    ArtifactScanDisposition, ArtifactScanEvidence, ArtifactScanEvidenceDraft,
    ArtifactScanExecution, ArtifactScanRequest, ArtifactScanWorkRecord, ArtifactScanner,
    ArtifactTransaction, ArtifactWorkerAudit, ArtifactWorkerService, CommitArtifactBlobCleanup,
    CommitArtifactScanOutcome, CompleteArtifactDeletion, CompleteArtifactUpload,
    CompletedArtifactDeletion, CompletedArtifactUpload, CreateArtifactProvenance,
    DeleteArtifactBlobGeneration, FinalizeArtifact, FinalizedArtifact, MarkArtifactDeletion,
    MarkedArtifactDeletion, PlaceArtifactHold, PrepareArtifact, PreparedArtifact,
    ReleaseArtifactHold, ReleaseArtifactReference, ScheduleArtifactRescan,
    ScheduleInitialArtifactScan,
};
use insight_platform_contracts::{
    ArtifactPurpose, ArtifactRef, ArtifactReferenceKind, ArtifactRetentionPolicy, ArtifactState,
    BlobIntegrityState, CommandAudit, CommandOutcome, DataClassification, Effect, ExactVersionRef,
    ManagementOperationState, Permission, PermissionSet, PolicyKind, PolicyResourceSpec,
    PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot, PublishedVersionPayload,
    ResourceDocument, ResourceDraftPayload, ResourceId, ResourceKind, Sha256Digest, TenantConfig,
    TenantPrincipalPayload, ValidationSummary,
};
use insight_platform_jobs::JobFence as DomainJobFence;
use insight_platform_postgres::{
    artifact_repository::{ArtifactRecoverySlot, DriveExpiredArtifactJobs},
    repository::{
        ClaimJobs, JobFence, NewPrincipal, NewQuotaAccount, NewTenant, NewTenantPrincipal,
        PgRepository, RepositoryError, SafetyScanShard, TypedPayload,
    },
    verify_schema,
};
use insight_platform_tasks::{TaskDefinition, TaskPayload, TaskResolution, TaskState};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool};

struct FixtureScanner {
    evidence: Result<ArtifactScanEvidence, ArtifactBackendFailure>,
}

impl ArtifactScanner for FixtureScanner {
    async fn scan(
        &self,
        request: ArtifactScanRequest,
    ) -> Result<ArtifactScanEvidence, ArtifactBackendFailure> {
        let evidence = self.evidence.clone()?;
        assert_eq!(request.tenant_id.kind(), ResourceKind::Tenant);
        assert_eq!(request.job_id, evidence.scan_job_id);
        assert_eq!(request.job.scan_kind, evidence.scan_kind);
        Ok(evidence)
    }
}

struct FixtureBlobBackend {
    evidence: Result<ArtifactBlobDeletionEvidence, ArtifactBackendFailure>,
}

impl ArtifactBlobBackend for FixtureBlobBackend {
    async fn delete_generation(
        &self,
        request: DeleteArtifactBlobGeneration,
    ) -> Result<ArtifactBlobDeletionEvidence, ArtifactBackendFailure> {
        let evidence = self.evidence.clone()?;
        assert_eq!(request.object_generation, evidence.object_generation);
        Ok(evidence)
    }
}

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1c7-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn audit(
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    base: u16,
    idempotency: char,
    request: char,
) -> CommandAudit {
    CommandAudit {
        tenant_id: tenant_id.clone(),
        principal_id: principal_id.clone(),
        principal_kind: PrincipalKind::TenantAdmin,
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(2),
    }
}

fn artifact_worker_audit(
    tenant_id: &ResourceId,
    worker_process_generation_id: &ResourceId,
    base: u16,
    idempotency: char,
    request: char,
) -> ArtifactWorkerAudit {
    ArtifactWorkerAudit {
        tenant_id: tenant_id.clone(),
        worker_process_generation_id: worker_process_generation_id.clone(),
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(2),
    }
}

fn artifact_domain_fence(fence: &JobFence) -> DomainJobFence {
    DomainJobFence {
        expected_version: u64::try_from(fence.expected_job_version).unwrap(),
        worker_process_generation_id: fence.worker_id.clone(),
        lease_generation: u64::try_from(fence.lease_epoch).unwrap(),
        token_digest: fence.lease_token_digest.clone(),
    }
}

fn command(
    tenant_id: ResourceId,
    principal_id: ResourceId,
    retention_policy_revision_id: ResourceId,
    quota_account_id: ResourceId,
    base: u16,
    expected_size_bytes: u64,
    request_digest: Sha256Digest,
) -> PrepareArtifact {
    let now = Utc::now();
    PrepareArtifact {
        audit: CommandAudit {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::TenantAdmin,
            receipt_id: id(ResourceKind::Receipt, base),
            event_id: id(ResourceKind::Event, base + 1),
            outbox_id: id(ResourceKind::OutboxEvent, base + 2),
            idempotency_key_digest: digest('a'),
            request_digest,
            receipt_expires_at: now + Duration::hours(2),
        },
        operation_id: id(ResourceKind::ManagementOperation, base + 3),
        artifact_id: id(ResourceKind::Artifact, base + 4),
        blob_id: id(ResourceKind::InternalBlob, base + 5),
        upload_grant_id: id(ResourceKind::ArtifactGrant, base + 6),
        quota_account_id,
        quota_entry_id: id(ResourceKind::QuotaLedgerEntry, base + 7),
        purpose: ArtifactPurpose::RunInput,
        classification: DataClassification::Internal,
        expected_size_bytes,
        expected_digest: None,
        declared_media_type: None,
        retention_policy_revision_id,
        retain_until: now + Duration::days(2),
        operation_deadline: now + Duration::hours(1),
        grant_expires_at: now + Duration::minutes(30),
        grant_token_digest: digest('c'),
        storage_backend: "s3".to_owned(),
        storage_binding_digest: digest('d'),
        object_reference_ciphertext: vec![7, 8, 9],
        key_id: "artifact-kek-v1".to_owned(),
        encryption_domain_id: id(ResourceKind::EncryptionDomain, base + 8),
        display_name: Some(format!("input-{base:04x}.json")),
    }
}

async fn execute_prepare(
    repository: &PgRepository,
    command: PrepareArtifact,
) -> Result<CommandOutcome<PreparedArtifact>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.prepare_artifact(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

fn complete_command(
    prepared: &PrepareArtifact,
    base: u16,
    idempotency: char,
    request: char,
) -> CompleteArtifactUpload {
    CompleteArtifactUpload {
        audit: CommandAudit {
            tenant_id: prepared.audit.tenant_id.clone(),
            principal_id: prepared.audit.principal_id.clone(),
            principal_kind: prepared.audit.principal_kind,
            receipt_id: id(ResourceKind::Receipt, base),
            event_id: id(ResourceKind::Event, base + 1),
            outbox_id: id(ResourceKind::OutboxEvent, base + 2),
            idempotency_key_digest: digest(idempotency),
            request_digest: digest(request),
            receipt_expires_at: Utc::now() + Duration::hours(2),
        },
        operation_id: prepared.operation_id.clone(),
        artifact_id: prepared.artifact_id.clone(),
        blob_id: prepared.blob_id.clone(),
        upload_grant_id: prepared.upload_grant_id.clone(),
        expected_artifact_version: 1,
        expected_blob_version: 1,
        expected_operation_version: 1,
        expected_grant_version: 1,
        grant_generation: 1,
        grant_token_digest: prepared.grant_token_digest.clone(),
        object_generation: "s3-version-0001".to_owned(),
        observed_size_bytes: prepared.expected_size_bytes,
        backend_evidence_digest: digest('9'),
    }
}

async fn execute_complete(
    repository: &PgRepository,
    command: CompleteArtifactUpload,
) -> Result<CommandOutcome<CompletedArtifactUpload>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.complete_upload(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn exact_policy_ref(
    pool: &PgPool,
    tenant_id: &ResourceId,
    revision_id: &ResourceId,
) -> ExactVersionRef {
    let content_digest: String = sqlx::query_scalar(
        r#"
        SELECT content_digest FROM insight_platform.resource_versions
        WHERE tenant_id = $1 AND resource_version_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(revision_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    ExactVersionRef::new(revision_id.clone(), content_digest.parse().unwrap()).unwrap()
}

fn schedule_initial_scan_command(
    prepared: &PrepareArtifact,
    scan_policy_revision: ExactVersionRef,
    base: u16,
) -> ScheduleInitialArtifactScan {
    ScheduleInitialArtifactScan {
        audit: audit(
            &prepared.audit.tenant_id,
            &prepared.audit.principal_id,
            base,
            '6',
            'a',
        ),
        scan_job_id: id(ResourceKind::Job, base + 3),
        operation_id: prepared.operation_id.clone(),
        artifact_id: prepared.artifact_id.clone(),
        blob_id: prepared.blob_id.clone(),
        expected_artifact_version: 2,
        expected_blob_version: 2,
        expected_operation_version: 2,
        scan_policy_revision,
        scanner_contract_digest: digest('4'),
        ruleset_digest: digest('5'),
        evidence_ttl_milliseconds: 60_000,
        retry_backoff_milliseconds: 100,
        deadline: Utc::now() + Duration::hours(1),
    }
}

async fn execute_schedule_initial_scan(
    repository: &PgRepository,
    command: ScheduleInitialArtifactScan,
) -> Result<CommandOutcome<ArtifactScanWorkRecord>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.schedule_initial_scan(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

struct ScanFinding {
    content_digest: Sha256Digest,
    disposition: ArtifactScanDisposition,
    reason_class: Option<String>,
}

impl ScanFinding {
    fn verified(content_digest: Sha256Digest) -> Self {
        Self {
            content_digest,
            disposition: ArtifactScanDisposition::Verified,
            reason_class: None,
        }
    }

    fn corrupt(content_digest: Sha256Digest, reason_class: &str) -> Self {
        Self {
            content_digest,
            disposition: ArtifactScanDisposition::Corrupt,
            reason_class: Some(reason_class.to_owned()),
        }
    }
}

fn commit_scan_command(
    prepared: &PrepareArtifact,
    scheduled: &ArtifactScanWorkRecord,
    worker_id: &ResourceId,
    fence: &JobFence,
    base: u16,
    finding: ScanFinding,
) -> CommitArtifactScanOutcome {
    let observed_at = Utc::now();
    let evidence = ArtifactScanEvidenceDraft {
        schema_version: 1,
        scan_kind: scheduled.scan.scan_kind,
        scan_job_id: scheduled.scan_job_id.clone(),
        scan_policy_revision: scheduled.scan.scan_policy_revision.clone(),
        scanner_contract_digest: scheduled.scan.scanner_contract_digest.clone(),
        ruleset_digest: scheduled.scan.ruleset_digest.clone(),
        object_generation: scheduled.scan.object_generation.clone(),
        content_digest: finding.content_digest,
        size_bytes: prepared.expected_size_bytes,
        verified_media_type: "application/json".to_owned(),
        disposition: finding.disposition,
        reason_class: finding.reason_class,
        observed_at,
        expires_at: observed_at
            + Duration::milliseconds(
                i64::try_from(scheduled.scan.evidence_ttl_milliseconds).unwrap(),
            ),
    }
    .seal()
    .unwrap();
    CommitArtifactScanOutcome {
        audit: artifact_worker_audit(&prepared.audit.tenant_id, worker_id, base, '7', 'b'),
        scan_job_id: scheduled.scan_job_id.clone(),
        fence: artifact_domain_fence(fence),
        operation_id: scheduled.operation.operation_id.clone(),
        artifact_id: prepared.artifact_id.clone(),
        blob_id: prepared.blob_id.clone(),
        expected_artifact_version: scheduled.artifact.version,
        expected_blob_version: scheduled.scan.expected_blob_version,
        expected_operation_version: scheduled.operation.version,
        evidence,
        duplicate_blob_cleanup_job_id: id(ResourceKind::Job, base + 3),
    }
}

fn scan_execution(
    scheduled: &ArtifactScanWorkRecord,
    command: &CommitArtifactScanOutcome,
) -> ArtifactScanExecution {
    ArtifactScanExecution {
        audit: command.audit.clone(),
        scan_job_id: command.scan_job_id.clone(),
        fence: command.fence.clone(),
        scan: scheduled.scan.clone(),
        operation_id: command.operation_id.clone(),
        artifact_id: command.artifact_id.clone(),
        blob_id: command.blob_id.clone(),
        expected_artifact_version: command.expected_artifact_version,
        expected_blob_version: command.expected_blob_version,
        expected_operation_version: command.expected_operation_version,
        duplicate_blob_cleanup_job_id: command.duplicate_blob_cleanup_job_id.clone(),
    }
}

fn deletion_execution(
    deletion: &insight_platform_artifacts::ArtifactDeletionRecord,
    command: &CompleteArtifactDeletion,
) -> ArtifactDeletionExecution {
    ArtifactDeletionExecution {
        audit: command.audit.clone(),
        deletion_job_id: command.deletion_job_id.clone(),
        fence: command.fence.clone(),
        deletion: ArtifactDeletionJobSnapshot {
            schema_version: 1,
            operation_id: deletion.operation_id.clone(),
            artifact_id: deletion.artifact_id.clone(),
            blob_id: deletion.blob_id.clone(),
            mode: deletion.mode.clone(),
            expected_artifact_version: command.expected_artifact_version,
            expected_blob_version: command.expected_blob_version,
            expected_operation_version: command.expected_operation_version,
            retry_backoff_milliseconds: 100,
        },
        expected_artifact_version: command.expected_artifact_version,
        expected_blob_version: command.expected_blob_version,
        expected_operation_version: command.expected_operation_version,
    }
}

async fn execute_commit_scan(
    repository: &PgRepository,
    command: CommitArtifactScanOutcome,
) -> Result<CommandOutcome<ArtifactScanWorkRecord>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.commit_scan_outcome(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_schedule_rescan(
    repository: &PgRepository,
    command: ScheduleArtifactRescan,
) -> Result<CommandOutcome<ArtifactScanWorkRecord>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.schedule_rescan(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

fn finalize_command(
    prepared: &PrepareArtifact,
    quota_account_id: ResourceId,
    base: u16,
) -> FinalizeArtifact {
    FinalizeArtifact {
        audit: CommandAudit {
            tenant_id: prepared.audit.tenant_id.clone(),
            principal_id: prepared.audit.principal_id.clone(),
            principal_kind: prepared.audit.principal_kind,
            receipt_id: id(ResourceKind::Receipt, base),
            event_id: id(ResourceKind::Event, base + 1),
            outbox_id: id(ResourceKind::OutboxEvent, base + 2),
            idempotency_key_digest: digest('8'),
            request_digest: digest('d'),
            receipt_expires_at: Utc::now() + Duration::hours(2),
        },
        operation_id: prepared.operation_id.clone(),
        artifact_id: prepared.artifact_id.clone(),
        blob_id: prepared.blob_id.clone(),
        upload_grant_id: prepared.upload_grant_id.clone(),
        artifact_reference_id: id(ResourceKind::ArtifactLink, base + 3),
        quota_account_id,
        quota_settle_entry_id: id(ResourceKind::QuotaLedgerEntry, base + 4),
        expected_artifact_version: 4,
        expected_blob_version: 3,
        expected_operation_version: 3,
        expected_grant_version: 2,
        expected_quota_account_version: 2,
        grant_generation: 1,
        object_generation: "s3-version-0001".to_owned(),
        content_digest: digest('8'),
        size_bytes: prepared.expected_size_bytes,
        verified_media_type: "application/json".to_owned(),
        reference_kind: ArtifactReferenceKind::Input,
    }
}

async fn execute_finalize(
    repository: &PgRepository,
    command: FinalizeArtifact,
) -> Result<CommandOutcome<FinalizedArtifact>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.finalize_artifact(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_place_hold(
    repository: &PgRepository,
    command: PlaceArtifactHold,
) -> Result<CommandOutcome<ArtifactHoldRecord>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.place_hold(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_release_hold(
    repository: &PgRepository,
    command: ReleaseArtifactHold,
) -> Result<CommandOutcome<ArtifactHoldRecord>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.release_hold(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_create_provenance(
    repository: &PgRepository,
    command: CreateArtifactProvenance,
) -> Result<CommandOutcome<ArtifactProvenanceRecord>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.create_provenance(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_release_reference(
    repository: &PgRepository,
    command: ReleaseArtifactReference,
) -> Result<CommandOutcome<ArtifactReferenceRecord>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.release_reference(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_mark_deletion(
    repository: &PgRepository,
    command: MarkArtifactDeletion,
) -> Result<CommandOutcome<MarkedArtifactDeletion>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.mark_deletion(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_complete_deletion(
    repository: &PgRepository,
    command: CompleteArtifactDeletion,
) -> Result<CommandOutcome<CompletedArtifactDeletion>, RepositoryError> {
    let mut transaction = repository.begin_artifact_transaction().await?;
    match transaction.complete_deletion(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn seed_approved_deletion_task(
    pool: &PgPool,
    command: &MarkArtifactDeletion,
    retention_policy_revision_id: &ResourceId,
) {
    let permissions = PermissionSet::new(vec![
        Permission::ApprovalRespond,
        Permission::ArtifactDelete,
        Permission::ArtifactHold,
        Permission::ArtifactWrite,
    ])
    .unwrap();
    let principal = PrincipalSnapshot::build(
        command.audit.tenant_id.clone(),
        command.audit.principal_id.clone(),
        command.audit.principal_kind,
        permissions,
        1,
        1,
        1,
    )
    .unwrap();
    let payload = TaskPayload {
        definition: TaskDefinition::Approval {
            owner_version: command.expected_artifact_version,
            owner_snapshot_digest: command.audit.request_digest.clone(),
            effect: Effect::Irreversible,
            input_digest: command.audit.request_digest.clone(),
            policy_revision_id: retention_policy_revision_id.clone(),
            approver_rule_digest: digest('e'),
            safe_prompt_key: "artifact_delete".to_owned(),
        },
        created_by: principal.clone(),
        resolution: Some(TaskResolution {
            state: TaskState::Approved,
            principal: Some(principal),
            response_value_id: None,
            response_schema_digest: None,
        }),
    };
    payload.validate().unwrap();
    let payload = TypedPayload::new(1, &payload).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.tasks (
            tenant_id, task_id, task_kind, owner_kind, owner_id, state,
            principal_snapshot_schema_version, payload_schema_version, payload,
            payload_digest, deadline, responded_at, created_at, updated_at
        ) VALUES ($1, $2, 'approval', 'artifact', $3, 'approved',
                  1, $4, $5, $6, statement_timestamp() + interval '1 hour',
                  statement_timestamp(), statement_timestamp(), statement_timestamp())
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.approval_task_id.as_ref().unwrap().to_string())
    .bind(command.artifact_id.to_string())
    .bind(payload.schema_version)
    .bind(payload.value)
    .bind(payload.digest)
    .execute(pool)
    .await
    .unwrap();
}

async fn claim_and_start_artifact_job(
    repository: &PgRepository,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    worker_id: &ResourceId,
) -> JobFence {
    let tokens = "0123456789abcdef".chars().map(digest).collect::<Vec<_>>();
    let claimed = repository
        .claim_jobs(ClaimJobs {
            work_class: "artifact".to_owned(),
            worker_id: worker_id.clone(),
            limit: u16::try_from(tokens.len()).unwrap(),
            lease_milliseconds: 120_000,
            lease_token_digests: tokens,
        })
        .await
        .unwrap();
    let claimed = claimed
        .into_iter()
        .find(|job| job.tenant_id == tenant_id.to_string() && job.job_id == job_id.to_string())
        .expect("deletion Job must be included in the bounded Artifact claim");
    let lease_token_digest = claimed
        .lease_token_digest
        .as_ref()
        .unwrap()
        .parse::<Sha256Digest>()
        .unwrap();
    let started = repository
        .start_job(JobFence {
            tenant_id: tenant_id.to_string(),
            job_id: job_id.to_string(),
            worker_id: worker_id.clone(),
            lease_epoch: claimed.lease_epoch,
            expected_job_version: claimed.version,
            lease_token_digest: lease_token_digest.clone(),
        })
        .await
        .unwrap();
    JobFence {
        tenant_id: tenant_id.to_string(),
        job_id: job_id.to_string(),
        worker_id: worker_id.clone(),
        lease_epoch: started.lease_epoch,
        expected_job_version: started.version,
        lease_token_digest,
    }
}

async fn seed_retention_root(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    base: u16,
) -> (ResourceId, ResourceId) {
    let policy_id = id(ResourceKind::Policy, base);
    let policy_revision_id = id(ResourceKind::PolicyRevision, base + 1);
    let artifact_id = id(ResourceKind::Artifact, base + 2);
    let blob_id = id(ResourceKind::InternalBlob, base + 3);
    let artifact_ref = ArtifactRef::new(
        artifact_id.clone(),
        digest('1'),
        64,
        "application/json",
        DataClassification::Internal,
        Some("built-in-retention-policy.json".to_owned()),
    )
    .unwrap();
    let retention = ArtifactRetentionPolicy {
        version: 1,
        minimum_retention_seconds: 3_600,
        gc_grace_seconds: 86_400,
        tombstone_retention_seconds: 2_592_000,
        retain_provenance_sources: true,
        delete_requires_approval: true,
    };
    let document = ResourceDocument::Policy(PolicyResourceSpec {
        authoring_package: insight_platform_contracts::AuthoringPackage {
            artifact: artifact_ref,
            manifest_digest: digest('2'),
        },
        contract_digest: digest('3'),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: PolicyKind::Retention,
        rules_digest: retention.canonical_digest().unwrap(),
        scheduling: None,
        retention: Some(retention),
        mcp_protocol: None,
        mcp_auth: None,
        sandbox_isolation: None,
        sandbox_resource: None,
        sandbox_network: None,
        sandbox_artifact_io: None,
        model_output_artifact_io: None,
        sandbox_secret_resolution: None,
    });
    let resource_payload = TypedPayload::new(
        1,
        &ResourceDraftPayload {
            display_name: "Built-in Artifact retention".to_owned(),
            document: document.clone(),
            validation: None,
        },
    )
    .unwrap();
    let version_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document,
            validation: ValidationSummary {
                validator_digest: digest('4'),
                validated_draft_digest: digest('5'),
                dependency_closure_digest: digest('6'),
                security_evidence_digest: digest('7'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let metadata = TypedPayload::new(1, &json!({"root": "tenant_retention"})).unwrap();
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'policy', 'active', 'enabled', $3, $4, $5)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(policy_id.to_string())
    .bind(resource_payload.schema_version)
    .bind(&resource_payload.value)
    .bind(&resource_payload.digest)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest,
            security_domain_digest, object_reference_ciphertext, object_generation, key_id,
            encryption_domain_id, content_digest, size_bytes, state, verified_at
        ) VALUES ($1, $2, 'builtin', $3, $4, $5, 'release-generation-1',
                  'release-key', $6, $7, 64, 'verified', clock_timestamp())
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .bind(digest('8').to_string())
    .bind(digest('9').to_string())
    .bind(vec![1_u8])
    .bind(id(ResourceKind::EncryptionDomain, base + 4).to_string())
    .bind(digest('1').to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'authoring_document', 'internal', 64, $4,
                  'application/json', 'application/json', 'ready', $5, $6, $7,
                  $8, $9, $10)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .bind(blob_id.to_string())
    .bind(digest('1').to_string())
    .bind(metadata.schema_version)
    .bind(&metadata.value)
    .bind(&metadata.digest)
    .bind(policy_revision_id.to_string())
    .bind(Utc::now() + Duration::days(365))
    .bind(principal_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resource_versions (
            tenant_id, resource_version_id, resource_id, resource_version_kind,
            revision_no, content_digest, artifact_id, payload_schema_version,
            payload, payload_digest, created_by
        ) VALUES ($1, $2, $3, 'policy_revision', 1, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(policy_revision_id.to_string())
    .bind(policy_id.to_string())
    .bind(&version_payload.digest)
    .bind(artifact_id.to_string())
    .bind(version_payload.schema_version)
    .bind(&version_payload.value)
    .bind(&version_payload.digest)
    .bind(principal_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        r#"
        UPDATE insight_platform.resources
        SET active_version_id = $3, version = version + 1,
            updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND resource_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(policy_id.to_string())
    .bind(policy_revision_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    (policy_revision_id, artifact_id)
}

async fn seed_artifact_io_policy(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    authoring_artifact_id: &ResourceId,
    base: u16,
) -> ResourceId {
    let policy_id = id(ResourceKind::Policy, base);
    let policy_revision_id = id(ResourceKind::PolicyRevision, base + 1);
    let artifact_ref = ArtifactRef::new(
        authoring_artifact_id.clone(),
        digest('1'),
        64,
        "application/json",
        DataClassification::Internal,
        Some("built-in-artifact-io-policy.json".to_owned()),
    )
    .unwrap();
    let document = ResourceDocument::Policy(PolicyResourceSpec {
        authoring_package: insight_platform_contracts::AuthoringPackage {
            artifact: artifact_ref,
            manifest_digest: digest('a'),
        },
        contract_digest: digest('b'),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: PolicyKind::ArtifactIo,
        rules_digest: digest('c'),
        scheduling: None,
        retention: None,
        mcp_protocol: None,
        mcp_auth: None,
        sandbox_isolation: None,
        sandbox_resource: None,
        sandbox_network: None,
        sandbox_artifact_io: None,
        model_output_artifact_io: None,
        sandbox_secret_resolution: None,
    });
    let resource_payload = TypedPayload::new(
        1,
        &ResourceDraftPayload {
            display_name: "Built-in Artifact I/O policy".to_owned(),
            document: document.clone(),
            validation: None,
        },
    )
    .unwrap();
    let version_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document,
            validation: ValidationSummary {
                validator_digest: digest('d'),
                validated_draft_digest: digest('e'),
                dependency_closure_digest: digest('f'),
                security_evidence_digest: digest('0'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'policy', 'active', 'enabled', $3, $4, $5)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(policy_id.to_string())
    .bind(resource_payload.schema_version)
    .bind(&resource_payload.value)
    .bind(&resource_payload.digest)
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resource_versions (
            tenant_id, resource_version_id, resource_id, resource_version_kind,
            revision_no, content_digest, artifact_id, payload_schema_version,
            payload, payload_digest, created_by
        ) VALUES ($1, $2, $3, 'policy_revision', 1, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(policy_revision_id.to_string())
    .bind(policy_id.to_string())
    .bind(&version_payload.digest)
    .bind(authoring_artifact_id.to_string())
    .bind(version_payload.schema_version)
    .bind(&version_payload.value)
    .bind(&version_payload.digest)
    .bind(principal_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query(
        r#"
        UPDATE insight_platform.resources
        SET active_version_id = $3, version = version + 1,
            updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND resource_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(policy_id.to_string())
    .bind(policy_revision_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    policy_revision_id
}

#[test]
fn artifact_upload_lifecycle_is_atomic_tenant_scoped_ready_only_and_replayable() {
    std::thread::Builder::new()
        .name("phase3-artifact-fixture".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(artifact_upload_lifecycle_fixture());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn artifact_upload_lifecycle_fixture() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let tenant_a = id(ResourceKind::Tenant, 0x0100);
    let tenant_b = id(ResourceKind::Tenant, 0x0101);
    let allowed_principal = id(ResourceKind::Principal, 0x0102);
    let denied_principal = id(ResourceKind::Principal, 0x0103);
    for tenant_id in [&tenant_a, &tenant_b] {
        repository
            .create_tenant(NewTenant {
                tenant_id: tenant_id.to_string(),
                state: "active".to_owned(),
                config: TenantConfig {
                    scheduling_policy: None,
                },
            })
            .await
            .unwrap();
    }
    for (principal_id, marker) in [(&allowed_principal, '9'), (&denied_principal, 'a')] {
        repository
            .create_principal(NewPrincipal {
                principal_id: principal_id.clone(),
                authentication_authority_digest: digest(marker),
                subject_digest: digest(if marker == '9' { 'b' } else { 'c' }),
                installation_bindings: PrincipalBindingsPayload {
                    installation_bindings: vec![],
                },
            })
            .await
            .unwrap();
    }
    for tenant_id in [&tenant_a, &tenant_b] {
        repository
            .bind_tenant_principal(NewTenantPrincipal {
                tenant_id: tenant_id.clone(),
                principal_id: allowed_principal.clone(),
                principal_kind: PrincipalKind::TenantAdmin,
                payload: TenantPrincipalPayload {
                    permissions: PermissionSet::new(vec![
                        Permission::ApprovalRespond,
                        Permission::ArtifactDelete,
                        Permission::ArtifactWrite,
                        Permission::ArtifactHold,
                    ])
                    .unwrap(),
                },
            })
            .await
            .unwrap();
    }
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant_a.clone(),
            principal_id: denied_principal.clone(),
            principal_kind: PrincipalKind::TenantAdmin,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::ArtifactRead]).unwrap(),
            },
        })
        .await
        .unwrap();
    let (retention_a, policy_authoring_artifact) =
        seed_retention_root(&pool, &tenant_a, &allowed_principal, 0x0200).await;
    let artifact_io_policy = seed_artifact_io_policy(
        &pool,
        &tenant_a,
        &allowed_principal,
        &policy_authoring_artifact,
        0x0210,
    )
    .await;
    let scan_policy_a = exact_policy_ref(&pool, &tenant_a, &artifact_io_policy).await;
    let wrong_scan_policy = exact_policy_ref(&pool, &tenant_a, &retention_a).await;
    let quota_a = id(ResourceKind::QuotaAccount, 0x0300);
    let quota_b = id(ResourceKind::QuotaAccount, 0x0301);
    for (tenant_id, quota_id) in [(&tenant_a, &quota_a), (&tenant_b, &quota_b)] {
        repository
            .create_quota_account(NewQuotaAccount {
                tenant_id: tenant_id.to_string(),
                quota_account_id: quota_id.to_string(),
                scope_kind: "tenant".to_owned(),
                scope_id: tenant_id.to_string(),
                work_class: "artifact".to_owned(),
                metric: "artifact.staging_bytes".to_owned(),
                limit_value: 4_096,
                payload: TypedPayload::new(1, &json!({"profile": "phase3-fixture"})).unwrap(),
            })
            .await
            .unwrap();
    }

    let prepared_command = command(
        tenant_a.clone(),
        allowed_principal.clone(),
        retention_a.clone(),
        quota_a.clone(),
        0x1000,
        1_024,
        digest('d'),
    );
    let applied = execute_prepare(&repository, prepared_command.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(prepared) = applied else {
        panic!("first prepare must apply");
    };
    assert_eq!(prepared.artifact.expected_digest, None);
    assert_eq!(prepared.artifact.declared_media_type, None);
    assert_eq!(prepared.artifact.verified_media_type, None);
    assert_eq!(prepared.blob.content_digest, None);
    assert_eq!(prepared.blob.size_bytes, None);
    assert_eq!(prepared.blob.object_generation, None);
    assert_eq!(prepared.grant.snapshot.token_digest, digest('c'));
    let verified_without_facts = sqlx::query(
        "UPDATE insight_platform.artifact_blobs SET state = 'verified' WHERE tenant_id = $1 AND blob_id = $2",
    )
    .bind(tenant_a.to_string())
    .bind(prepared_command.blob_id.to_string())
    .execute(&pool)
    .await;
    assert!(verified_without_facts.is_err());
    let ready_without_media = sqlx::query(
        "UPDATE insight_platform.artifacts SET state = 'ready' WHERE tenant_id = $1 AND artifact_id = $2",
    )
    .bind(tenant_a.to_string())
    .bind(prepared_command.artifact_id.to_string())
    .execute(&pool)
    .await;
    assert!(ready_without_media.is_err());

    let replay = execute_prepare(&repository, prepared_command.clone())
        .await
        .unwrap();
    let CommandOutcome::Replayed(replayed) = replay else {
        panic!("exact prepare retry must replay");
    };
    assert_eq!(replayed, prepared);
    let counts: (i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.artifacts WHERE tenant_id = $1 AND artifact_id = $2),
          (SELECT count(*) FROM insight_platform.artifact_blobs WHERE tenant_id = $1 AND blob_id = $3),
          (SELECT count(*) FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $4),
          (SELECT count(*) FROM insight_platform.artifact_links WHERE tenant_id = $1 AND artifact_link_id = $5),
          (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND scope_id = $2 AND operation = 'artifact.prepare'),
          (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND aggregate_id = $2 AND event_type = 'artifact.prepared'),
          (SELECT count(*) FROM insight_platform.outbox_events AS outbox JOIN insight_platform.events AS event ON event.event_id = outbox.event_id WHERE outbox.tenant_id = $1 AND event.aggregate_id = $2),
          (SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'reserve')
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(prepared_command.artifact_id.to_string())
    .bind(prepared_command.blob_id.to_string())
    .bind(prepared_command.operation_id.to_string())
    .bind(prepared_command.upload_grant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (1, 1, 1, 1, 1, 1, 1, 1));
    let reserved: i64 = sqlx::query_scalar(
        "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(tenant_a.to_string())
    .bind(quota_a.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(reserved, 1_024);
    let persisted_grant: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.artifact_links WHERE tenant_id = $1 AND artifact_link_id = $2",
    )
    .bind(tenant_a.to_string())
    .bind(prepared_command.upload_grant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(persisted_grant["token_digest"], json!(digest('c')));
    assert!(persisted_grant.get("token").is_none());
    assert!(persisted_grant.get("presigned_url").is_none());

    let mut conflict = prepared_command.clone();
    conflict.audit.request_digest = digest('e');
    assert!(matches!(
        execute_prepare(&repository, conflict).await,
        Err(RepositoryError::IdempotencyConflict)
    ));

    let mut invalid_grant = complete_command(&prepared_command, 0x1050, 'b', '4');
    invalid_grant.grant_token_digest = digest('0');
    assert!(matches!(
        execute_complete(&repository, invalid_grant).await,
        Err(RepositoryError::PermissionDenied)
    ));
    let mut wrong_size = complete_command(&prepared_command, 0x1060, 'c', '5');
    wrong_size.observed_size_bytes = 1_023;
    assert!(matches!(
        execute_complete(&repository, wrong_size).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let completed_command = complete_command(&prepared_command, 0x1070, 'd', '6');
    let completed = execute_complete(&repository, completed_command.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(completed) = completed else {
        panic!("first complete-upload command must apply");
    };
    assert_eq!(completed.artifact.state, ArtifactState::Uploaded);
    assert_eq!(completed.artifact.version, 2);
    assert_eq!(
        completed.blob.object_generation.as_deref(),
        Some("s3-version-0001")
    );
    assert_eq!(completed.blob.version, 2);
    assert_eq!(completed.grant.state, ArtifactLinkState::Consumed);
    assert_eq!(completed.grant.version, 2);
    assert_eq!(completed.operation.state, ManagementOperationState::Running);
    assert_eq!(completed.operation.version, 2);
    let replayed_completion = execute_complete(&repository, completed_command.clone())
        .await
        .unwrap();
    assert_eq!(
        replayed_completion,
        CommandOutcome::Replayed(completed.clone())
    );
    let mut completion_conflict = completed_command.clone();
    completion_conflict.audit.request_digest = digest('7');
    assert!(matches!(
        execute_complete(&repository, completion_conflict).await,
        Err(RepositoryError::IdempotencyConflict)
    ));
    let mut stale_completion = complete_command(&prepared_command, 0x1080, 'e', '8');
    stale_completion.expected_artifact_version = 1;
    assert!(matches!(
        execute_complete(&repository, stale_completion).await,
        Err(RepositoryError::Conflict(_))
    ));
    let completion_counts: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND scope_id = $2),
          (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND aggregate_id = $2),
          (SELECT count(*) FROM insight_platform.outbox_events AS outbox JOIN insight_platform.events AS event ON event.event_id = outbox.event_id WHERE outbox.tenant_id = $1 AND event.aggregate_id = $2)
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(prepared_command.artifact_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completion_counts, (2, 2, 2));

    let wrong_policy_schedule =
        schedule_initial_scan_command(&prepared_command, wrong_scan_policy, 0x1088);
    assert!(matches!(
        execute_schedule_initial_scan(&repository, wrong_policy_schedule).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let schedule_scan =
        schedule_initial_scan_command(&prepared_command, scan_policy_a.clone(), 0x1090);
    let started = execute_schedule_initial_scan(&repository, schedule_scan.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(started) = started else {
        panic!("first scan scheduling command must apply");
    };
    assert_eq!(started.artifact.state, ArtifactState::Verifying);
    assert_eq!(started.artifact.version, 3);
    assert_eq!(started.blob.version, 2);
    assert_eq!(started.operation.version, 2);
    assert_eq!(started.scan_job_state.as_str(), "ready");
    assert_eq!(
        execute_schedule_initial_scan(&repository, schedule_scan)
            .await
            .unwrap(),
        CommandOutcome::Replayed(started.clone())
    );

    let scan_worker = id(ResourceKind::WorkerProcessGeneration, 0x10a8);
    let expired_scan_fence =
        claim_and_start_artifact_job(&repository, &tenant_a, &started.scan_job_id, &scan_worker)
            .await;
    sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET heartbeat_at = clock_timestamp() - interval '2 seconds',
            lease_expires_at = clock_timestamp() - interval '1 second'
        WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(started.scan_job_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let recovered_scan = repository
        .drive_expired_artifact_jobs(DriveExpiredArtifactJobs {
            shard: SafetyScanShard::whole(),
            after: None,
            limit: 1,
            slots: vec![ArtifactRecoverySlot {
                event_id: id(ResourceKind::Event, 0x10b0),
                outbox_id: id(ResourceKind::OutboxEvent, 0x10b1),
            }],
        })
        .await
        .unwrap();
    assert_eq!(recovered_scan.records.len(), 1);
    assert_eq!(recovered_scan.records[0].job.state, "retry_scheduled");
    assert_eq!(
        recovered_scan.records[0].artifact_version,
        Some(started.artifact.version)
    );
    assert_eq!(
        recovered_scan.records[0].operation_version,
        Some(started.operation.version)
    );
    let stale_completion = commit_scan_command(
        &prepared_command,
        &started,
        &scan_worker,
        &expired_scan_fence,
        0x10c0,
        ScanFinding::verified(digest('8')),
    );
    assert!(matches!(
        execute_commit_scan(&repository, stale_completion).await,
        Err(RepositoryError::StaleFence)
    ));
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    let scan_retry_worker = id(ResourceKind::WorkerProcessGeneration, 0x10d8);
    let scan_fence = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &started.scan_job_id,
        &scan_retry_worker,
    )
    .await;
    let complete_scan = commit_scan_command(
        &prepared_command,
        &started,
        &scan_retry_worker,
        &scan_fence,
        0x10e0,
        ScanFinding::verified(digest('8')),
    );
    let scan_service = ArtifactWorkerService::new(
        FixtureScanner {
            evidence: Ok(complete_scan.evidence.clone()),
        },
        FixtureBlobBackend {
            evidence: Err(ArtifactBackendFailure {
                retryable: false,
                reason_class: "unused".to_owned(),
            }),
        },
        repository.clone(),
    );
    let verified = scan_service
        .execute_scan(scan_execution(&started, &complete_scan), Utc::now())
        .await
        .unwrap();
    let CommandOutcome::Applied(verified) = verified else {
        panic!("first fenced scan outcome must apply");
    };
    assert_eq!(verified.artifact.state, ArtifactState::Verified);
    assert_eq!(verified.artifact.version, 4);
    assert_eq!(
        verified.artifact.verified_media_type.as_deref(),
        Some("application/json")
    );
    assert_eq!(verified.blob.state, BlobIntegrityState::Verified);
    assert_eq!(verified.blob.version, 3);
    assert_eq!(verified.blob.content_digest, Some(digest('8')));
    assert_eq!(verified.blob.size_bytes, Some(1_024));
    assert_eq!(verified.operation.state, ManagementOperationState::Running);
    assert_eq!(verified.operation.version, 3);
    assert_eq!(verified.scan_job_state.as_str(), "succeeded");
    assert_eq!(
        verified
            .artifact
            .metadata
            .current_verification
            .as_ref()
            .map(|current| &current.scan_job_id),
        Some(&verified.scan_job_id)
    );
    assert_eq!(
        scan_service
            .execute_scan(scan_execution(&started, &complete_scan), Utc::now())
            .await
            .unwrap(),
        CommandOutcome::Replayed(verified)
    );
    let verification_counts: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND scope_id = $2),
          (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND aggregate_id = $2),
          (SELECT count(*) FROM insight_platform.outbox_events AS outbox JOIN insight_platform.events AS event ON event.event_id = outbox.event_id WHERE outbox.tenant_id = $1 AND event.aggregate_id = $2)
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(prepared_command.artifact_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(verification_counts, (3, 4, 4));

    let finalized_command = finalize_command(&prepared_command, quota_a.clone(), 0x10b0);
    let mut forged_finalization = finalized_command.clone();
    forged_finalization.audit.idempotency_key_digest = digest('9');
    forged_finalization.audit.request_digest = digest('e');
    forged_finalization.content_digest = digest('0');
    assert!(matches!(
        execute_finalize(&repository, forged_finalization).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let finalized = execute_finalize(&repository, finalized_command.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(finalized) = finalized else {
        panic!("first finalize command must apply");
    };
    assert_eq!(finalized.artifact.state, ArtifactState::Ready);
    assert_eq!(finalized.artifact.version, 5);
    assert_eq!(
        finalized.operation.state,
        ManagementOperationState::Succeeded
    );
    assert_eq!(finalized.operation.version, 4);
    assert_eq!(finalized.reference.state, ArtifactLinkState::Active);
    assert_eq!(
        finalized.reference.snapshot.reference_kind,
        ArtifactReferenceKind::Input
    );
    assert_eq!(
        finalized.artifact_ref.artifact_id(),
        &prepared_command.artifact_id
    );
    assert_eq!(finalized.artifact_ref.content_digest(), &digest('8'));
    assert_eq!(finalized.artifact_ref.byte_length(), 1_024);
    assert_eq!(
        execute_finalize(&repository, finalized_command.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(finalized)
    );
    let quota_after_finalize: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT account.reserved_value, account.used_value, account.version,
               (SELECT count(*) FROM insight_platform.quota_ledger AS ledger
                WHERE ledger.tenant_id = account.tenant_id
                  AND ledger.quota_account_id = account.quota_account_id
                  AND ledger.correlation_id = $3)
        FROM insight_platform.quota_accounts AS account
        WHERE account.tenant_id = $1 AND account.quota_account_id = $2
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(quota_a.to_string())
    .bind(prepared_command.artifact_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(quota_after_finalize, (0, 0, 3, 2));
    let final_counts: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND scope_id = $2),
          (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND aggregate_id = $2),
          (SELECT count(*) FROM insight_platform.outbox_events AS outbox JOIN insight_platform.events AS event ON event.event_id = outbox.event_id WHERE outbox.tenant_id = $1 AND event.aggregate_id = $2),
          (SELECT count(*) FROM insight_platform.artifact_links WHERE tenant_id = $1 AND target_artifact_id = $2 AND link_kind = 'reference')
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(prepared_command.artifact_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(final_counts, (4, 5, 5, 1));

    let mut duplicate_command = command(
        tenant_a.clone(),
        allowed_principal.clone(),
        retention_a.clone(),
        quota_a.clone(),
        0x1500,
        1_024,
        digest('4'),
    );
    duplicate_command.encryption_domain_id = prepared_command.encryption_domain_id.clone();
    let CommandOutcome::Applied(_) = execute_prepare(&repository, duplicate_command.clone())
        .await
        .unwrap()
    else {
        panic!("duplicate-content prepare must apply before dedupe lookup");
    };
    let CommandOutcome::Applied(_) = execute_complete(
        &repository,
        complete_command(&duplicate_command, 0x1510, 'e', '5'),
    )
    .await
    .unwrap() else {
        panic!("duplicate-content upload must apply");
    };
    let duplicate_schedule =
        schedule_initial_scan_command(&duplicate_command, scan_policy_a.clone(), 0x1520);
    let CommandOutcome::Applied(duplicate_scan) =
        execute_schedule_initial_scan(&repository, duplicate_schedule)
            .await
            .unwrap()
    else {
        panic!("duplicate-content scan must be scheduled");
    };
    let duplicate_worker = id(ResourceKind::WorkerProcessGeneration, 0x153f);
    let duplicate_fence = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &duplicate_scan.scan_job_id,
        &duplicate_worker,
    )
    .await;
    let duplicate_verification = commit_scan_command(
        &duplicate_command,
        &duplicate_scan,
        &duplicate_worker,
        &duplicate_fence,
        0x1530,
        ScanFinding::verified(digest('8')),
    );
    let duplicate_verified = execute_commit_scan(&repository, duplicate_verification.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(duplicate_verified) = duplicate_verified else {
        panic!("duplicate-content verification must apply");
    };
    assert_eq!(duplicate_verified.artifact.state, ArtifactState::Verified);
    assert_eq!(
        duplicate_verified.artifact.blob_id,
        Some(prepared_command.blob_id.clone())
    );
    assert_eq!(&duplicate_verified.blob.blob_id, &prepared_command.blob_id);
    assert_eq!(duplicate_verified.blob.state, BlobIntegrityState::Verified);
    assert_eq!(
        execute_commit_scan(&repository, duplicate_verification)
            .await
            .unwrap(),
        CommandOutcome::Replayed(duplicate_verified.clone())
    );
    let duplicate_cleanup: (String, i64, String, serde_json::Value) = sqlx::query_as(
        r#"
        SELECT blob.state, blob.version, job.state, job.payload
        FROM insight_platform.artifact_blobs AS blob
        JOIN insight_platform.jobs AS job
          ON job.tenant_id = blob.tenant_id
         AND job.owner_kind = 'internal_blob' AND job.owner_id = blob.blob_id
        WHERE blob.tenant_id = $1 AND blob.blob_id = $2
          AND job.job_id = $3
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(duplicate_command.blob_id.to_string())
    .bind(id(ResourceKind::Job, 0x1533).to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(duplicate_cleanup.0, "deleting");
    assert_eq!(duplicate_cleanup.1, 3);
    assert_eq!(duplicate_cleanup.2, "ready");
    assert_eq!(
        duplicate_cleanup.3["cleanup"]["replacement_blob_id"],
        json!(prepared_command.blob_id.clone())
    );
    let cleanup_worker = id(ResourceKind::WorkerProcessGeneration, 0x154f);
    let cleanup_job_id = id(ResourceKind::Job, 0x1533);
    let cleanup_fence =
        claim_and_start_artifact_job(&repository, &tenant_a, &cleanup_job_id, &cleanup_worker)
            .await;
    let cleanup_command = CommitArtifactBlobCleanup {
        audit: artifact_worker_audit(&tenant_a, &cleanup_worker, 0x1550, 'c', 'd'),
        cleanup_job_id: cleanup_job_id.clone(),
        fence: artifact_domain_fence(&cleanup_fence),
        discarded_blob_id: duplicate_command.blob_id.clone(),
        expected_blob_version: 3,
        evidence: ArtifactBlobDeletionEvidence {
            schema_version: 1,
            object_generation: "s3-version-0001".to_owned(),
            backend_receipt_digest: digest('e'),
            absence_evidence_digest: digest('f'),
            observed_at: Utc::now(),
        },
    };
    let mut cleanup_payload = duplicate_cleanup.3.clone();
    cleanup_payload
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    let cleanup = match serde_json::from_value::<ArtifactJobPayload>(cleanup_payload).unwrap() {
        ArtifactJobPayload::BlobCleanup { cleanup } => cleanup,
        _ => panic!("duplicate candidate must create a blob_cleanup Job"),
    };
    let cleanup_service = ArtifactWorkerService::new(
        FixtureScanner {
            evidence: Err(ArtifactBackendFailure {
                retryable: false,
                reason_class: "unused".to_owned(),
            }),
        },
        FixtureBlobBackend {
            evidence: Ok(cleanup_command.evidence.clone()),
        },
        repository.clone(),
    );
    let cleanup_execution = ArtifactBlobCleanupExecution {
        audit: cleanup_command.audit.clone(),
        cleanup_job_id: cleanup_command.cleanup_job_id.clone(),
        fence: cleanup_command.fence.clone(),
        cleanup,
        expected_blob_version: cleanup_command.expected_blob_version,
    };
    let cleaned = cleanup_service
        .execute_blob_cleanup(cleanup_execution.clone(), Utc::now())
        .await
        .unwrap();
    let CommandOutcome::Applied(cleaned) = cleaned else {
        panic!("exact duplicate Blob cleanup must apply");
    };
    assert_eq!(cleaned.blob.state, BlobIntegrityState::Deleted);
    assert_eq!(cleaned.blob.version, 4);
    assert_eq!(cleaned.cleanup_job_state.as_str(), "succeeded");
    assert_eq!(
        cleanup_service
            .execute_blob_cleanup(cleanup_execution, Utc::now())
            .await
            .unwrap(),
        CommandOutcome::Replayed(cleaned)
    );
    let mut duplicate_finalize = finalize_command(&duplicate_command, quota_a.clone(), 0x1540);
    duplicate_finalize.blob_id = duplicate_verified.blob.blob_id.clone();
    duplicate_finalize.expected_quota_account_version = 4;
    let CommandOutcome::Applied(duplicate_finalized) =
        execute_finalize(&repository, duplicate_finalize)
            .await
            .unwrap()
    else {
        panic!("duplicate-content Artifact must finalize against the shared Blob");
    };
    assert_eq!(duplicate_finalized.artifact.state, ArtifactState::Ready);
    assert_eq!(
        duplicate_finalized.artifact.blob_id,
        Some(prepared_command.blob_id.clone())
    );
    let verified_blob_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM insight_platform.artifact_blobs
        WHERE tenant_id = $1 AND backend = 's3' AND content_digest = $2
          AND state = 'verified' AND deleted_at IS NULL
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(digest('8').to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(verified_blob_count, 1);

    let mut concurrent_a = command(
        tenant_a.clone(),
        allowed_principal.clone(),
        retention_a.clone(),
        quota_a.clone(),
        0x1600,
        1_024,
        digest('6'),
    );
    concurrent_a.encryption_domain_id = prepared_command.encryption_domain_id.clone();
    let mut concurrent_b = command(
        tenant_a.clone(),
        allowed_principal.clone(),
        retention_a.clone(),
        quota_a.clone(),
        0x1700,
        1_024,
        digest('7'),
    );
    concurrent_b.encryption_domain_id = prepared_command.encryption_domain_id.clone();
    for candidate in [&concurrent_a, &concurrent_b] {
        assert!(matches!(
            execute_prepare(&repository, candidate.clone())
                .await
                .unwrap(),
            CommandOutcome::Applied(_)
        ));
    }
    assert!(matches!(
        execute_complete(
            &repository,
            complete_command(&concurrent_a, 0x1610, '1', '2')
        )
        .await
        .unwrap(),
        CommandOutcome::Applied(_)
    ));
    assert!(matches!(
        execute_complete(
            &repository,
            complete_command(&concurrent_b, 0x1710, '3', '4')
        )
        .await
        .unwrap(),
        CommandOutcome::Applied(_)
    ));
    let CommandOutcome::Applied(concurrent_scan_a) = execute_schedule_initial_scan(
        &repository,
        schedule_initial_scan_command(&concurrent_a, scan_policy_a.clone(), 0x1620),
    )
    .await
    .unwrap() else {
        panic!("first concurrent scan must schedule");
    };
    let concurrent_worker_a = id(ResourceKind::WorkerProcessGeneration, 0x163f);
    let concurrent_fence_a = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &concurrent_scan_a.scan_job_id,
        &concurrent_worker_a,
    )
    .await;
    let CommandOutcome::Applied(concurrent_scan_b) = execute_schedule_initial_scan(
        &repository,
        schedule_initial_scan_command(&concurrent_b, scan_policy_a.clone(), 0x1720),
    )
    .await
    .unwrap() else {
        panic!("second concurrent scan must schedule");
    };
    let concurrent_worker_b = id(ResourceKind::WorkerProcessGeneration, 0x173f);
    let concurrent_fence_b = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &concurrent_scan_b.scan_job_id,
        &concurrent_worker_b,
    )
    .await;
    let verify_a = commit_scan_command(
        &concurrent_a,
        &concurrent_scan_a,
        &concurrent_worker_a,
        &concurrent_fence_a,
        0x1630,
        ScanFinding::verified(digest('a')),
    );
    let verify_b = commit_scan_command(
        &concurrent_b,
        &concurrent_scan_b,
        &concurrent_worker_b,
        &concurrent_fence_b,
        0x1730,
        ScanFinding::verified(digest('a')),
    );
    let (verified_a, verified_b) = tokio::join!(
        execute_commit_scan(&repository, verify_a),
        execute_commit_scan(&repository, verify_b),
    );
    let CommandOutcome::Applied(verified_a) = verified_a.unwrap() else {
        panic!("first concurrent verification must apply");
    };
    let CommandOutcome::Applied(verified_b) = verified_b.unwrap() else {
        panic!("second concurrent verification must apply");
    };
    assert_eq!(verified_a.blob.blob_id, verified_b.blob.blob_id);
    let concurrent_shared_blob_id = verified_a.blob.blob_id.clone();
    let concurrent_blob_closure: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          count(*) FILTER (WHERE state = 'verified'),
          count(*) FILTER (WHERE state = 'deleting'),
          (SELECT count(*) FROM insight_platform.jobs
           WHERE tenant_id = $1 AND owner_kind = 'internal_blob'
             AND owner_id IN ($3, $4) AND state = 'ready')
        FROM insight_platform.artifact_blobs
        WHERE tenant_id = $1 AND content_digest = $2
          AND blob_id IN ($3, $4)
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(digest('a').to_string())
    .bind(concurrent_a.blob_id.to_string())
    .bind(concurrent_b.blob_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(concurrent_blob_closure, (1, 1, 1));
    let mut finalize_a = finalize_command(&concurrent_a, quota_a.clone(), 0x1640);
    finalize_a.blob_id = concurrent_shared_blob_id.clone();
    finalize_a.content_digest = digest('a');
    finalize_a.expected_quota_account_version = 7;
    assert!(matches!(
        execute_finalize(&repository, finalize_a).await.unwrap(),
        CommandOutcome::Applied(_)
    ));
    let mut finalize_b = finalize_command(&concurrent_b, quota_a.clone(), 0x1740);
    finalize_b.blob_id = concurrent_shared_blob_id.clone();
    finalize_b.content_digest = digest('a');
    finalize_b.expected_quota_account_version = 8;
    assert!(matches!(
        execute_finalize(&repository, finalize_b).await.unwrap(),
        CommandOutcome::Applied(_)
    ));

    let isolated_domain = command(
        tenant_a.clone(),
        allowed_principal.clone(),
        retention_a.clone(),
        quota_a.clone(),
        0x1800,
        1_024,
        digest('9'),
    );
    assert!(matches!(
        execute_prepare(&repository, isolated_domain.clone())
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    assert!(matches!(
        execute_complete(
            &repository,
            complete_command(&isolated_domain, 0x1810, '5', '6')
        )
        .await
        .unwrap(),
        CommandOutcome::Applied(_)
    ));
    let CommandOutcome::Applied(isolated_scan) = execute_schedule_initial_scan(
        &repository,
        schedule_initial_scan_command(&isolated_domain, scan_policy_a.clone(), 0x1820),
    )
    .await
    .unwrap() else {
        panic!("isolated-domain scan must schedule");
    };
    let isolated_worker = id(ResourceKind::WorkerProcessGeneration, 0x183f);
    let isolated_fence = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &isolated_scan.scan_job_id,
        &isolated_worker,
    )
    .await;
    let isolated_verified = execute_commit_scan(
        &repository,
        commit_scan_command(
            &isolated_domain,
            &isolated_scan,
            &isolated_worker,
            &isolated_fence,
            0x1830,
            ScanFinding::verified(digest('8')),
        ),
    )
    .await
    .unwrap();
    let CommandOutcome::Applied(isolated_verified) = isolated_verified else {
        panic!("same content in another security domain must verify independently");
    };
    assert_eq!(&isolated_verified.blob.blob_id, &isolated_domain.blob_id);
    let isolated_shape: (i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.artifact_blobs
           WHERE tenant_id = $1 AND content_digest = $2 AND state = 'verified'),
          (SELECT count(*) FROM insight_platform.jobs
           WHERE tenant_id = $1 AND job_id = $3)
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(digest('8').to_string())
    .bind(id(ResourceKind::Job, 0x1833).to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(isolated_shape, (2, 0));
    let mut isolated_finalize = finalize_command(&isolated_domain, quota_a.clone(), 0x1840);
    isolated_finalize.expected_quota_account_version = 10;
    assert!(matches!(
        execute_finalize(&repository, isolated_finalize)
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));

    let hold_command = PlaceArtifactHold {
        audit: CommandAudit {
            tenant_id: tenant_a.clone(),
            principal_id: allowed_principal.clone(),
            principal_kind: PrincipalKind::TenantAdmin,
            receipt_id: id(ResourceKind::Receipt, 0x1900),
            event_id: id(ResourceKind::Event, 0x1901),
            outbox_id: id(ResourceKind::OutboxEvent, 0x1902),
            idempotency_key_digest: digest('1'),
            request_digest: digest('2'),
            receipt_expires_at: Utc::now() + Duration::hours(2),
        },
        artifact_hold_id: id(ResourceKind::ArtifactLink, 0x1903),
        artifact_id: prepared_command.artifact_id.clone(),
        expected_artifact_version: 5,
        hold_kind: ArtifactHoldKind::Legal,
        reason_class: "litigation".to_owned(),
        evidence_digest: digest('3'),
        expires_at: None,
    };
    let placed_hold = execute_place_hold(&repository, hold_command.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(placed_hold) = placed_hold else {
        panic!("legal hold must be placed atomically");
    };
    assert_eq!(placed_hold.state, ArtifactLinkState::Active);
    assert_eq!(placed_hold.snapshot.hold_kind, ArtifactHoldKind::Legal);
    assert_eq!(
        execute_place_hold(&repository, hold_command.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(placed_hold.clone())
    );
    let release_command = ReleaseArtifactHold {
        audit: CommandAudit {
            tenant_id: tenant_a.clone(),
            principal_id: allowed_principal.clone(),
            principal_kind: PrincipalKind::TenantAdmin,
            receipt_id: id(ResourceKind::Receipt, 0x1910),
            event_id: id(ResourceKind::Event, 0x1911),
            outbox_id: id(ResourceKind::OutboxEvent, 0x1912),
            idempotency_key_digest: digest('4'),
            request_digest: digest('5'),
            receipt_expires_at: Utc::now() + Duration::hours(2),
        },
        artifact_hold_id: hold_command.artifact_hold_id.clone(),
        artifact_id: prepared_command.artifact_id.clone(),
        expected_hold_version: 1,
        reason_class: "matter_closed".to_owned(),
        evidence_digest: digest('6'),
    };
    let released_hold = execute_release_hold(&repository, release_command.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(released_hold) = released_hold else {
        panic!("legal hold release must apply");
    };
    assert_eq!(released_hold.state, ArtifactLinkState::Released);
    assert_eq!(released_hold.version, 2);
    assert!(released_hold.released_at.is_some());
    assert_eq!(
        execute_release_hold(&repository, release_command)
            .await
            .unwrap(),
        CommandOutcome::Replayed(released_hold)
    );

    let provenance_command = CreateArtifactProvenance {
        audit: CommandAudit {
            tenant_id: tenant_a.clone(),
            principal_id: allowed_principal.clone(),
            principal_kind: PrincipalKind::TenantAdmin,
            receipt_id: id(ResourceKind::Receipt, 0x1920),
            event_id: id(ResourceKind::Event, 0x1921),
            outbox_id: id(ResourceKind::OutboxEvent, 0x1922),
            idempotency_key_digest: digest('7'),
            request_digest: digest('8'),
            receipt_expires_at: Utc::now() + Duration::hours(2),
        },
        provenance_link_id: id(ResourceKind::ArtifactLink, 0x1923),
        source_artifact_id: prepared_command.artifact_id.clone(),
        derived_artifact_id: duplicate_command.artifact_id.clone(),
        transformation_deployment_id: id(ResourceKind::CapabilityDeployment, 0x1924),
        producer_owner_id: prepared_command.operation_id.clone(),
        expected_source_version: 5,
        expected_derived_version: 5,
        parameters_digest: digest('9'),
    };
    let provenance = execute_create_provenance(&repository, provenance_command.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(provenance) = provenance else {
        panic!("provenance edge must apply");
    };
    assert_eq!(
        provenance.snapshot.source_artifact_id,
        prepared_command.artifact_id
    );
    assert_eq!(
        provenance.snapshot.derived_artifact_id,
        duplicate_command.artifact_id
    );
    assert_eq!(
        execute_create_provenance(&repository, provenance_command)
            .await
            .unwrap(),
        CommandOutcome::Replayed(provenance)
    );
    let cyclic_provenance = CreateArtifactProvenance {
        audit: CommandAudit {
            tenant_id: tenant_a.clone(),
            principal_id: allowed_principal.clone(),
            principal_kind: PrincipalKind::TenantAdmin,
            receipt_id: id(ResourceKind::Receipt, 0x1930),
            event_id: id(ResourceKind::Event, 0x1931),
            outbox_id: id(ResourceKind::OutboxEvent, 0x1932),
            idempotency_key_digest: digest('a'),
            request_digest: digest('b'),
            receipt_expires_at: Utc::now() + Duration::hours(2),
        },
        provenance_link_id: id(ResourceKind::ArtifactLink, 0x1933),
        source_artifact_id: duplicate_command.artifact_id.clone(),
        derived_artifact_id: prepared_command.artifact_id.clone(),
        transformation_deployment_id: id(ResourceKind::CapabilityDeployment, 0x1934),
        producer_owner_id: duplicate_command.operation_id.clone(),
        expected_source_version: 5,
        expected_derived_version: 5,
        parameters_digest: digest('c'),
    };
    assert!(matches!(
        execute_create_provenance(&repository, cyclic_provenance).await,
        Err(RepositoryError::InvalidInput(_))
    ));

    let rescan_command = ScheduleArtifactRescan {
        audit: audit(&tenant_a, &allowed_principal, 0x1940, 'd', 'e'),
        rescan_operation_id: id(ResourceKind::ManagementOperation, 0x1943),
        rescan_job_id: id(ResourceKind::Job, 0x1944),
        artifact_id: prepared_command.artifact_id.clone(),
        blob_id: prepared_command.blob_id.clone(),
        expected_artifact_version: 5,
        expected_blob_version: 3,
        scan_policy_revision: scan_policy_a.clone(),
        scanner_contract_digest: digest('4'),
        ruleset_digest: digest('5'),
        evidence_ttl_milliseconds: 60_000,
        retry_backoff_milliseconds: 100,
        deadline: Utc::now() + Duration::hours(1),
    };
    let rescan = execute_schedule_rescan(&repository, rescan_command.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(rescan) = rescan else {
        panic!("Artifact rescan scheduling must apply");
    };
    assert_eq!(rescan.artifact.state, ArtifactState::Quarantined);
    assert_eq!(rescan.artifact.version, 6);
    assert_eq!(rescan.operation.state, ManagementOperationState::Running);
    assert_eq!(rescan.scan_job_state.as_str(), "ready");
    assert_eq!(
        execute_schedule_rescan(&repository, rescan_command)
            .await
            .unwrap(),
        CommandOutcome::Replayed(rescan.clone())
    );
    let rescan_worker = id(ResourceKind::WorkerProcessGeneration, 0x1950);
    let rescan_fence =
        claim_and_start_artifact_job(&repository, &tenant_a, &rescan.scan_job_id, &rescan_worker)
            .await;
    let rescan_completion = commit_scan_command(
        &prepared_command,
        &rescan,
        &rescan_worker,
        &rescan_fence,
        0x1960,
        ScanFinding::verified(digest('8')),
    );
    let rescanned = execute_commit_scan(&repository, rescan_completion.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(rescanned) = rescanned else {
        panic!("exact rescan outcome must apply");
    };
    assert_eq!(rescanned.artifact.state, ArtifactState::Ready);
    assert_eq!(rescanned.artifact.version, 7);
    assert_eq!(rescanned.blob.version, 3);
    assert_eq!(
        rescanned.operation.state,
        ManagementOperationState::Succeeded
    );
    assert_eq!(rescanned.operation.version, 2);
    assert_eq!(
        execute_commit_scan(&repository, rescan_completion)
            .await
            .unwrap(),
        CommandOutcome::Replayed(rescanned)
    );

    let corruption_rescan_command = ScheduleArtifactRescan {
        audit: audit(&tenant_a, &allowed_principal, 0x1970, '1', '2'),
        rescan_operation_id: id(ResourceKind::ManagementOperation, 0x1973),
        rescan_job_id: id(ResourceKind::Job, 0x1974),
        artifact_id: prepared_command.artifact_id.clone(),
        blob_id: prepared_command.blob_id.clone(),
        expected_artifact_version: 7,
        expected_blob_version: 3,
        scan_policy_revision: scan_policy_a.clone(),
        scanner_contract_digest: digest('4'),
        ruleset_digest: digest('5'),
        evidence_ttl_milliseconds: 60_000,
        retry_backoff_milliseconds: 100,
        deadline: Utc::now() + Duration::hours(1),
    };
    let CommandOutcome::Applied(corruption_rescan) =
        execute_schedule_rescan(&repository, corruption_rescan_command)
            .await
            .unwrap()
    else {
        panic!("corruption rescan scheduling must apply");
    };
    let corruption_worker = id(ResourceKind::WorkerProcessGeneration, 0x1980);
    let corruption_fence = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &corruption_rescan.scan_job_id,
        &corruption_worker,
    )
    .await;
    let corruption_completion = commit_scan_command(
        &prepared_command,
        &corruption_rescan,
        &corruption_worker,
        &corruption_fence,
        0x1990,
        ScanFinding::corrupt(digest('8'), "scanner_integrity_failure"),
    );
    let CommandOutcome::Applied(corrupted) =
        execute_commit_scan(&repository, corruption_completion)
            .await
            .unwrap()
    else {
        panic!("fenced corruption outcome must apply");
    };
    assert_eq!(corrupted.artifact.state, ArtifactState::Corrupt);
    assert_eq!(corrupted.artifact.version, 9);
    assert_eq!(corrupted.blob.state, BlobIntegrityState::Corrupt);
    assert_eq!(corrupted.blob.version, 4);
    let readable_shared_aliases: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM insight_platform.artifacts AS artifact
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
        WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
          AND artifact.state = 'ready' AND artifact.terminal_at IS NULL
          AND blob.state = 'verified' AND blob.deleted_at IS NULL
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(duplicate_command.artifact_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(readable_shared_aliases, 0);

    sqlx::query(
        r#"
        UPDATE insight_platform.artifacts
        SET created_at = clock_timestamp() - interval '4 days',
            retain_until = clock_timestamp() - interval '2 days',
            updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND artifact_id IN ($2, $3)
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(concurrent_a.artifact_id.to_string())
    .bind(concurrent_b.artifact_id.to_string())
    .execute(&pool)
    .await
    .unwrap();

    let mark_shared = MarkArtifactDeletion {
        audit: audit(&tenant_a, &allowed_principal, 0x1a20, '1', '2'),
        deletion_operation_id: id(ResourceKind::ManagementOperation, 0x1a23),
        deletion_job_id: id(ResourceKind::Job, 0x1a24),
        artifact_id: concurrent_a.artifact_id.clone(),
        blob_id: concurrent_shared_blob_id.clone(),
        expected_artifact_version: 5,
        expected_blob_version: 3,
        approval_task_id: Some(id(ResourceKind::ApprovalTask, 0x1a25)),
        retry_backoff_milliseconds: 100,
        deadline: Utc::now() + Duration::hours(1),
    };
    seed_approved_deletion_task(&pool, &mark_shared, &retention_a).await;
    let mut wrong_approval_binding = mark_shared.clone();
    wrong_approval_binding.audit = audit(&tenant_a, &allowed_principal, 0x1a18, 'f', '0');
    assert!(matches!(
        execute_mark_deletion(&repository, wrong_approval_binding).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    assert!(matches!(
        execute_mark_deletion(&repository, mark_shared.clone()).await,
        Err(RepositoryError::InvalidInput(_))
    ));

    let release_a = ReleaseArtifactReference {
        audit: audit(&tenant_a, &allowed_principal, 0x1a00, '3', '4'),
        artifact_reference_id: id(ResourceKind::ArtifactLink, 0x1643),
        artifact_id: concurrent_a.artifact_id.clone(),
        expected_reference_version: 1,
        reason_class: "retention_elapsed".to_owned(),
    };
    let released_a = execute_release_reference(&repository, release_a.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(released_a) = released_a else {
        panic!("first shared-Blob reference release must apply");
    };
    assert_eq!(released_a.state, ArtifactLinkState::Released);
    assert_eq!(released_a.version, 2);
    assert_eq!(
        execute_release_reference(&repository, release_a)
            .await
            .unwrap(),
        CommandOutcome::Replayed(released_a)
    );
    let release_b = ReleaseArtifactReference {
        audit: audit(&tenant_a, &allowed_principal, 0x1a10, '5', '6'),
        artifact_reference_id: id(ResourceKind::ArtifactLink, 0x1743),
        artifact_id: concurrent_b.artifact_id.clone(),
        expected_reference_version: 1,
        reason_class: "retention_elapsed".to_owned(),
    };
    assert!(matches!(
        execute_release_reference(&repository, release_b.clone())
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    assert!(matches!(
        execute_release_reference(&repository, release_b)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));

    let marked_shared = execute_mark_deletion(&repository, mark_shared.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(marked_shared) = marked_shared else {
        panic!("shared-Blob Artifact deletion mark must apply");
    };
    assert_eq!(marked_shared.artifact.state, ArtifactState::Deleting);
    assert_eq!(marked_shared.artifact.version, 6);
    assert_eq!(marked_shared.blob.state, BlobIntegrityState::Verified);
    assert_eq!(marked_shared.blob.version, 3);
    let (alias_artifact_id, alias_artifact_version) = match &marked_shared.deletion.mode {
        ArtifactDeletionMode::ArtifactOnly {
            alias_artifact_id,
            alias_artifact_version,
        } => (alias_artifact_id.clone(), *alias_artifact_version),
        ArtifactDeletionMode::BlobGeneration { .. } => {
            panic!("first shared-Blob deletion must preserve the Blob")
        }
    };
    assert_eq!(alias_artifact_id, concurrent_b.artifact_id);
    assert_eq!(alias_artifact_version, 5);
    assert_eq!(
        execute_mark_deletion(&repository, mark_shared)
            .await
            .unwrap(),
        CommandOutcome::Replayed(marked_shared.clone())
    );

    let deletion_worker = id(ResourceKind::WorkerProcessGeneration, 0x1a60);
    let shared_fence = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &marked_shared.deletion.job_id,
        &deletion_worker,
    )
    .await;
    let wrong_shared_completion = CompleteArtifactDeletion {
        audit: artifact_worker_audit(&tenant_a, &deletion_worker, 0x1a30, '7', '8'),
        deletion_operation_id: marked_shared.deletion.operation_id.clone(),
        deletion_job_id: marked_shared.deletion.job_id.clone(),
        artifact_id: concurrent_a.artifact_id.clone(),
        blob_id: concurrent_shared_blob_id.clone(),
        expected_artifact_version: 6,
        expected_blob_version: 3,
        expected_operation_version: 1,
        fence: artifact_domain_fence(&shared_fence),
        evidence: ArtifactDeletionEvidence::ArtifactOnly {
            alias_artifact_id: alias_artifact_id.clone(),
            alias_artifact_version: alias_artifact_version + 1,
        },
    };
    assert!(matches!(
        execute_complete_deletion(&repository, wrong_shared_completion).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let complete_shared = CompleteArtifactDeletion {
        audit: artifact_worker_audit(&tenant_a, &deletion_worker, 0x1a38, '9', 'a'),
        deletion_operation_id: marked_shared.deletion.operation_id.clone(),
        deletion_job_id: marked_shared.deletion.job_id.clone(),
        artifact_id: concurrent_a.artifact_id.clone(),
        blob_id: concurrent_shared_blob_id.clone(),
        expected_artifact_version: 6,
        expected_blob_version: 3,
        expected_operation_version: 1,
        fence: artifact_domain_fence(&shared_fence),
        evidence: ArtifactDeletionEvidence::ArtifactOnly {
            alias_artifact_id,
            alias_artifact_version,
        },
    };
    let shared_deletion_service = ArtifactWorkerService::new(
        FixtureScanner {
            evidence: Err(ArtifactBackendFailure {
                retryable: false,
                reason_class: "unused".to_owned(),
            }),
        },
        FixtureBlobBackend {
            evidence: Err(ArtifactBackendFailure {
                retryable: false,
                reason_class: "must_not_call".to_owned(),
            }),
        },
        repository.clone(),
    );
    let shared_deletion_execution = deletion_execution(&marked_shared.deletion, &complete_shared);
    let completed_shared = shared_deletion_service
        .execute_deletion(shared_deletion_execution.clone(), Utc::now())
        .await
        .unwrap();
    let CommandOutcome::Applied(completed_shared) = completed_shared else {
        panic!("shared-Blob Artifact-only deletion must apply");
    };
    assert_eq!(completed_shared.artifact.state, ArtifactState::Deleted);
    assert_eq!(completed_shared.artifact.version, 7);
    assert_eq!(completed_shared.blob.state, BlobIntegrityState::Verified);
    assert_eq!(completed_shared.blob.version, 3);
    assert_eq!(
        shared_deletion_service
            .execute_deletion(shared_deletion_execution, Utc::now())
            .await
            .unwrap(),
        CommandOutcome::Replayed(completed_shared)
    );

    let mark_physical = MarkArtifactDeletion {
        audit: audit(&tenant_a, &allowed_principal, 0x1a40, 'b', 'c'),
        deletion_operation_id: id(ResourceKind::ManagementOperation, 0x1a43),
        deletion_job_id: id(ResourceKind::Job, 0x1a44),
        artifact_id: concurrent_b.artifact_id.clone(),
        blob_id: concurrent_shared_blob_id.clone(),
        expected_artifact_version: 5,
        expected_blob_version: 3,
        approval_task_id: Some(id(ResourceKind::ApprovalTask, 0x1a45)),
        retry_backoff_milliseconds: 100,
        deadline: Utc::now() + Duration::hours(1),
    };
    seed_approved_deletion_task(&pool, &mark_physical, &retention_a).await;
    let marked_physical = execute_mark_deletion(&repository, mark_physical.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(marked_physical) = marked_physical else {
        panic!("last shared-Blob alias deletion mark must apply");
    };
    assert_eq!(marked_physical.artifact.state, ArtifactState::Deleting);
    assert_eq!(marked_physical.blob.state, BlobIntegrityState::Deleting);
    assert_eq!(marked_physical.blob.version, 4);
    assert_eq!(
        marked_physical.deletion.mode,
        ArtifactDeletionMode::BlobGeneration {
            object_generation: "s3-version-0001".to_owned(),
        }
    );
    assert_eq!(
        execute_mark_deletion(&repository, mark_physical)
            .await
            .unwrap(),
        CommandOutcome::Replayed(marked_physical.clone())
    );

    let physical_fence = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &marked_physical.deletion.job_id,
        &deletion_worker,
    )
    .await;
    let wrong_physical_completion = CompleteArtifactDeletion {
        audit: artifact_worker_audit(&tenant_a, &deletion_worker, 0x1a50, 'd', 'e'),
        deletion_operation_id: marked_physical.deletion.operation_id.clone(),
        deletion_job_id: marked_physical.deletion.job_id.clone(),
        artifact_id: concurrent_b.artifact_id.clone(),
        blob_id: concurrent_shared_blob_id.clone(),
        expected_artifact_version: 6,
        expected_blob_version: 4,
        expected_operation_version: 1,
        fence: artifact_domain_fence(&physical_fence),
        evidence: ArtifactDeletionEvidence::BlobGeneration {
            object_generation: "wrong-generation".to_owned(),
            backend_receipt_digest: digest('f'),
            absence_evidence_digest: digest('0'),
        },
    };
    assert!(matches!(
        execute_complete_deletion(&repository, wrong_physical_completion).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let physical_completion = CompleteArtifactDeletion {
        audit: artifact_worker_audit(&tenant_a, &deletion_worker, 0x1a58, '1', '2'),
        deletion_operation_id: marked_physical.deletion.operation_id.clone(),
        deletion_job_id: marked_physical.deletion.job_id.clone(),
        artifact_id: concurrent_b.artifact_id.clone(),
        blob_id: concurrent_shared_blob_id.clone(),
        expected_artifact_version: 6,
        expected_blob_version: 4,
        expected_operation_version: 1,
        fence: artifact_domain_fence(&physical_fence),
        evidence: ArtifactDeletionEvidence::BlobGeneration {
            object_generation: "s3-version-0001".to_owned(),
            backend_receipt_digest: digest('f'),
            absence_evidence_digest: digest('0'),
        },
    };
    let physical_backend_evidence = ArtifactBlobDeletionEvidence {
        schema_version: 1,
        object_generation: "s3-version-0001".to_owned(),
        backend_receipt_digest: digest('f'),
        absence_evidence_digest: digest('0'),
        observed_at: Utc::now(),
    };
    let physical_deletion_service = ArtifactWorkerService::new(
        FixtureScanner {
            evidence: Err(ArtifactBackendFailure {
                retryable: false,
                reason_class: "unused".to_owned(),
            }),
        },
        FixtureBlobBackend {
            evidence: Ok(physical_backend_evidence),
        },
        repository.clone(),
    );
    let physical_deletion_execution =
        deletion_execution(&marked_physical.deletion, &physical_completion);
    let completed_physical = physical_deletion_service
        .execute_deletion(physical_deletion_execution.clone(), Utc::now())
        .await
        .unwrap();
    let CommandOutcome::Applied(completed_physical) = completed_physical else {
        panic!("exact Blob generation deletion must apply");
    };
    assert_eq!(completed_physical.artifact.state, ArtifactState::Deleted);
    assert_eq!(completed_physical.artifact.version, 7);
    assert_eq!(completed_physical.blob.state, BlobIntegrityState::Deleted);
    assert_eq!(completed_physical.blob.version, 5);
    assert_eq!(
        physical_deletion_service
            .execute_deletion(physical_deletion_execution, Utc::now())
            .await
            .unwrap(),
        CommandOutcome::Replayed(completed_physical)
    );
    let deletion_closure: (String, String, String, i64, bool, i64, i64, i64) = sqlx::query_as(
        r#"
            SELECT
              (SELECT state FROM insight_platform.artifacts
               WHERE tenant_id = $1 AND artifact_id = $2),
              (SELECT state FROM insight_platform.artifacts
               WHERE tenant_id = $1 AND artifact_id = $3),
              blob.state, blob.version, blob.deleted_at IS NOT NULL,
              (SELECT count(*) FROM insight_platform.jobs
               WHERE tenant_id = $1 AND job_id IN ($5, $6) AND state = 'succeeded'),
              (SELECT count(*) FROM insight_platform.invocations
               WHERE tenant_id = $1 AND invocation_id IN ($7, $8) AND state = 'succeeded'),
              (SELECT count(*) FROM insight_platform.events AS event
               JOIN insight_platform.outbox_events AS outbox
                 ON outbox.tenant_id = event.tenant_id AND outbox.event_id = event.event_id
               WHERE event.tenant_id = $1 AND event.event_type = 'artifact.deleted'
                 AND event.aggregate_id IN ($2, $3))
            FROM insight_platform.artifact_blobs AS blob
            WHERE blob.tenant_id = $1 AND blob.blob_id = $4
            "#,
    )
    .bind(tenant_a.to_string())
    .bind(concurrent_a.artifact_id.to_string())
    .bind(concurrent_b.artifact_id.to_string())
    .bind(concurrent_shared_blob_id.to_string())
    .bind(marked_shared.deletion.job_id.to_string())
    .bind(marked_physical.deletion.job_id.to_string())
    .bind(marked_shared.deletion.operation_id.to_string())
    .bind(marked_physical.deletion.operation_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        deletion_closure,
        (
            "deleted".to_owned(),
            "deleted".to_owned(),
            "deleted".to_owned(),
            5,
            true,
            2,
            2,
            2,
        )
    );

    let recovery_artifact = command(
        tenant_a.clone(),
        allowed_principal.clone(),
        retention_a.clone(),
        quota_a.clone(),
        0x1b00,
        128,
        digest('5'),
    );
    assert!(matches!(
        execute_prepare(&repository, recovery_artifact.clone())
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    assert!(matches!(
        execute_complete(
            &repository,
            complete_command(&recovery_artifact, 0x1b10, '6', '7')
        )
        .await
        .unwrap(),
        CommandOutcome::Applied(_)
    ));
    let CommandOutcome::Applied(recovery_scan) = execute_schedule_initial_scan(
        &repository,
        schedule_initial_scan_command(&recovery_artifact, scan_policy_a.clone(), 0x1b20),
    )
    .await
    .unwrap() else {
        panic!("recovery Artifact scan must schedule");
    };
    let recovery_scan_worker = id(ResourceKind::WorkerProcessGeneration, 0x1b3f);
    let recovery_scan_fence = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &recovery_scan.scan_job_id,
        &recovery_scan_worker,
    )
    .await;
    let failed_scan_template = commit_scan_command(
        &recovery_artifact,
        &recovery_scan,
        &recovery_scan_worker,
        &recovery_scan_fence,
        0x1b30,
        ScanFinding::verified(digest('3')),
    );
    let failing_scan_service = ArtifactWorkerService::new(
        FixtureScanner {
            evidence: Err(ArtifactBackendFailure {
                retryable: true,
                reason_class: "scanner_timeout".to_owned(),
            }),
        },
        FixtureBlobBackend {
            evidence: Err(ArtifactBackendFailure {
                retryable: false,
                reason_class: "unused".to_owned(),
            }),
        },
        repository.clone(),
    );
    assert!(matches!(
        failing_scan_service
            .execute_scan(
                scan_execution(&recovery_scan, &failed_scan_template),
                Utc::now(),
            )
            .await,
        Err(
            insight_platform_artifacts::ArtifactWorkerExecutionError::Backend(
                ArtifactBackendFailure {
                    retryable: true,
                    reason_class,
                }
            )
        ) if reason_class == "scanner_timeout"
    ));
    let failed_scan_closure: (String, String, String, i64, i64) = sqlx::query_as(
        r#"
        SELECT job.state, artifact.state, operation.state,
               (SELECT count(*) FROM insight_platform.receipts
                WHERE tenant_id = $1 AND scope_kind = 'job' AND scope_id = $2
                  AND receipt_kind = 'job_commit' AND operation = 'artifact.attempt.fail'
                  AND state = 'succeeded'),
               (SELECT count(*) FROM insight_platform.events AS event
                JOIN insight_platform.outbox_events AS outbox
                  ON outbox.tenant_id = event.tenant_id AND outbox.event_id = event.event_id
                WHERE event.tenant_id = $1 AND event.aggregate_id = $2
                  AND event.event_type = 'artifact.retry_scheduled')
        FROM insight_platform.jobs AS job
        JOIN insight_platform.artifacts AS artifact
          ON artifact.tenant_id = job.tenant_id AND artifact.artifact_id = $3
        JOIN insight_platform.invocations AS operation
          ON operation.tenant_id = job.tenant_id AND operation.invocation_id = $4
        WHERE job.tenant_id = $1 AND job.job_id = $2
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(recovery_scan.scan_job_id.to_string())
    .bind(recovery_artifact.artifact_id.to_string())
    .bind(recovery_scan.operation.operation_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        failed_scan_closure,
        (
            "retry_scheduled".to_owned(),
            "verifying".to_owned(),
            "running".to_owned(),
            1,
            1,
        )
    );
    let mut stale_failed_scan = failed_scan_template;
    stale_failed_scan.audit =
        artifact_worker_audit(&tenant_a, &recovery_scan_worker, 0x1ba0, '4', '5');
    assert!(matches!(
        execute_commit_scan(&repository, stale_failed_scan).await,
        Err(RepositoryError::StaleFence)
    ));
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    let recovery_retry_worker = id(ResourceKind::WorkerProcessGeneration, 0x1bb0);
    let recovery_retry_fence = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &recovery_scan.scan_job_id,
        &recovery_retry_worker,
    )
    .await;
    let recovery_success = commit_scan_command(
        &recovery_artifact,
        &recovery_scan,
        &recovery_retry_worker,
        &recovery_retry_fence,
        0x1bc0,
        ScanFinding::verified(digest('3')),
    );
    let successful_scan_service = ArtifactWorkerService::new(
        FixtureScanner {
            evidence: Ok(recovery_success.evidence.clone()),
        },
        FixtureBlobBackend {
            evidence: Err(ArtifactBackendFailure {
                retryable: false,
                reason_class: "unused".to_owned(),
            }),
        },
        repository.clone(),
    );
    let CommandOutcome::Applied(recovery_verified) = successful_scan_service
        .execute_scan(
            scan_execution(&recovery_scan, &recovery_success),
            Utc::now(),
        )
        .await
        .unwrap()
    else {
        panic!("recovery Artifact verification must apply");
    };
    assert_eq!(recovery_verified.artifact.state, ArtifactState::Verified);
    let current_quota_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(tenant_a.to_string())
    .bind(quota_a.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut recovery_finalize = finalize_command(&recovery_artifact, quota_a.clone(), 0x1b40);
    recovery_finalize.content_digest = digest('3');
    recovery_finalize.expected_quota_account_version =
        u64::try_from(current_quota_version).unwrap();
    let recovery_reference_id = recovery_finalize.artifact_reference_id.clone();
    let CommandOutcome::Applied(recovery_ready) = execute_finalize(&repository, recovery_finalize)
        .await
        .unwrap()
    else {
        panic!("recovery Artifact finalize must apply");
    };
    assert_eq!(recovery_ready.artifact.state, ArtifactState::Ready);
    assert!(matches!(
        execute_release_reference(
            &repository,
            ReleaseArtifactReference {
                audit: audit(&tenant_a, &allowed_principal, 0x1b50, '8', '9'),
                artifact_reference_id: recovery_reference_id,
                artifact_id: recovery_artifact.artifact_id.clone(),
                expected_reference_version: 1,
                reason_class: "retention_elapsed".to_owned(),
            },
        )
        .await
        .unwrap(),
        CommandOutcome::Applied(_)
    ));
    sqlx::query(
        r#"
        UPDATE insight_platform.artifacts
        SET created_at = clock_timestamp() - interval '3 days',
            retain_until = clock_timestamp() - interval '2 days',
            updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND artifact_id = $2
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(recovery_artifact.artifact_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let recovery_mark = MarkArtifactDeletion {
        audit: audit(&tenant_a, &allowed_principal, 0x1b60, 'a', 'b'),
        deletion_operation_id: id(ResourceKind::ManagementOperation, 0x1b63),
        deletion_job_id: id(ResourceKind::Job, 0x1b64),
        artifact_id: recovery_artifact.artifact_id.clone(),
        blob_id: recovery_artifact.blob_id.clone(),
        expected_artifact_version: 5,
        expected_blob_version: 3,
        approval_task_id: Some(id(ResourceKind::ApprovalTask, 0x1b65)),
        retry_backoff_milliseconds: 100,
        deadline: Utc::now() + Duration::hours(1),
    };
    seed_approved_deletion_task(&pool, &recovery_mark, &retention_a).await;
    let CommandOutcome::Applied(recovery_marked) =
        execute_mark_deletion(&repository, recovery_mark)
            .await
            .unwrap()
    else {
        panic!("recovery Artifact deletion must mark");
    };
    assert_eq!(
        recovery_marked.deletion.mode,
        ArtifactDeletionMode::BlobGeneration {
            object_generation: "s3-version-0001".to_owned(),
        }
    );
    let uncertain_delete_worker = id(ResourceKind::WorkerProcessGeneration, 0x1b70);
    let uncertain_delete_fence = claim_and_start_artifact_job(
        &repository,
        &tenant_a,
        &recovery_marked.deletion.job_id,
        &uncertain_delete_worker,
    )
    .await;
    let failure_template = CompleteArtifactDeletion {
        audit: artifact_worker_audit(&tenant_a, &uncertain_delete_worker, 0x1b80, 'c', 'd'),
        deletion_operation_id: recovery_marked.deletion.operation_id.clone(),
        deletion_job_id: recovery_marked.deletion.job_id.clone(),
        artifact_id: recovery_artifact.artifact_id.clone(),
        blob_id: recovery_artifact.blob_id.clone(),
        expected_artifact_version: 6,
        expected_blob_version: 4,
        expected_operation_version: 1,
        fence: artifact_domain_fence(&uncertain_delete_fence),
        evidence: ArtifactDeletionEvidence::BlobGeneration {
            object_generation: "s3-version-0001".to_owned(),
            backend_receipt_digest: digest('e'),
            absence_evidence_digest: digest('f'),
        },
    };
    let uncertain_service = ArtifactWorkerService::new(
        FixtureScanner {
            evidence: Err(ArtifactBackendFailure {
                retryable: false,
                reason_class: "unused".to_owned(),
            }),
        },
        FixtureBlobBackend {
            evidence: Err(ArtifactBackendFailure {
                retryable: true,
                reason_class: "object_store_timeout".to_owned(),
            }),
        },
        repository.clone(),
    );
    let failure = uncertain_service
        .execute_deletion(
            deletion_execution(&recovery_marked.deletion, &failure_template),
            Utc::now(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        failure,
        insight_platform_artifacts::ArtifactWorkerExecutionError::Backend(
            ArtifactBackendFailure {
                retryable: true,
                reason_class,
            }
        ) if reason_class == "object_store_timeout"
    ));
    let recovery_closure: (String, String, String, i64) = sqlx::query_as(
        r#"
        SELECT artifact.state, blob.state, operation.state,
               (SELECT count(*) FROM insight_platform.events AS event
                JOIN insight_platform.outbox_events AS outbox
                  ON outbox.tenant_id = event.tenant_id AND outbox.event_id = event.event_id
                WHERE event.tenant_id = $1 AND event.aggregate_id = $4
                  AND event.event_type = 'artifact.reconciliation_required')
        FROM insight_platform.artifacts AS artifact
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
        JOIN insight_platform.invocations AS operation
          ON operation.tenant_id = artifact.tenant_id AND operation.invocation_id = $3
        WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
        "#,
    )
    .bind(tenant_a.to_string())
    .bind(recovery_artifact.artifact_id.to_string())
    .bind(recovery_marked.deletion.operation_id.to_string())
    .bind(recovery_marked.deletion.job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        recovery_closure,
        (
            "deleting".to_owned(),
            "deleting".to_owned(),
            "failed".to_owned(),
            1,
        )
    );
    let stale_uncertain_completion = CompleteArtifactDeletion {
        audit: artifact_worker_audit(&tenant_a, &uncertain_delete_worker, 0x1b90, 'e', 'f'),
        deletion_operation_id: recovery_marked.deletion.operation_id.clone(),
        deletion_job_id: recovery_marked.deletion.job_id.clone(),
        artifact_id: recovery_artifact.artifact_id.clone(),
        blob_id: recovery_artifact.blob_id.clone(),
        expected_artifact_version: 6,
        expected_blob_version: 4,
        expected_operation_version: 1,
        fence: artifact_domain_fence(&uncertain_delete_fence),
        evidence: ArtifactDeletionEvidence::BlobGeneration {
            object_generation: "s3-version-0001".to_owned(),
            backend_receipt_digest: digest('e'),
            absence_evidence_digest: digest('f'),
        },
    };
    assert!(matches!(
        execute_complete_deletion(&repository, stale_uncertain_completion).await,
        Err(RepositoryError::StaleFence)
    ));

    let denied = command(
        tenant_a.clone(),
        denied_principal,
        retention_a.clone(),
        quota_a.clone(),
        0x1100,
        128,
        digest('f'),
    );
    assert!(matches!(
        execute_prepare(&repository, denied.clone()).await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert_eq!(
        artifact_count(&pool, &tenant_a, &denied.artifact_id).await,
        0
    );

    let cross_tenant = command(
        tenant_b.clone(),
        allowed_principal.clone(),
        retention_a.clone(),
        quota_b,
        0x1200,
        128,
        digest('1'),
    );
    assert!(matches!(
        execute_prepare(&repository, cross_tenant.clone()).await,
        Err(RepositoryError::NotFound(_))
    ));
    assert_eq!(
        artifact_count(&pool, &tenant_b, &cross_tenant.artifact_id).await,
        0
    );

    let over_quota = command(
        tenant_a.clone(),
        allowed_principal.clone(),
        retention_a.clone(),
        quota_a.clone(),
        0x1300,
        4_097,
        digest('2'),
    );
    assert!(matches!(
        execute_prepare(&repository, over_quota.clone()).await,
        Err(RepositoryError::QuotaExceeded)
    ));
    assert_eq!(
        artifact_count(&pool, &tenant_a, &over_quota.artifact_id).await,
        0
    );

    let rolled_back = command(
        tenant_a.clone(),
        allowed_principal,
        retention_a,
        quota_a.clone(),
        0x1400,
        256,
        digest('3'),
    );
    let mut transaction = repository.begin_artifact_transaction().await.unwrap();
    assert!(matches!(
        transaction.prepare_artifact(rolled_back.clone()).await,
        Ok(CommandOutcome::Applied(_))
    ));
    transaction.rollback().await.unwrap();
    assert_eq!(
        artifact_count(&pool, &tenant_a, &rolled_back.artifact_id).await,
        0
    );
    let reserved_after_rollback: i64 = sqlx::query_scalar(
        "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(tenant_a.to_string())
    .bind(quota_a.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(reserved_after_rollback, 0);
}

async fn artifact_count(pool: &PgPool, tenant_id: &ResourceId, artifact_id: &ResourceId) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.artifacts WHERE tenant_id = $1 AND artifact_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}
