use crate::artifact_repository::{
    load_internal_artifact_admission_authority, reserve_internal_artifact_staging_quota,
    InternalArtifactAdmissionAuthority,
};
use crate::repository::{
    append_scheduler_event_with_trace, decode_deployment_closure, decode_typed_payload,
    decode_versioned_payload, job_from_row, job_projection, load_deployment, payload_from_row,
    require_ready_run_artifact, require_tenant_permission, terminalize_command_receipt,
    validate_deployment_closure_exists, JobRecord, PgRepository, RepositoryError, TypedPayload,
};
use chrono::{DateTime, Utc};
use insight_platform_artifacts::{
    ArtifactAwaitingStageSnapshot, ArtifactJobPayload, ArtifactMetadataSnapshot,
    ArtifactScanDisposition,
};
use insight_platform_context::{
    CommitContextDatasetBuild, ContextDatasetArtifactPreallocation, ContextDatasetArtifactStages,
    ContextDatasetBuildJobPayload, ContextDatasetRootPayload, FailContextDatasetBuildVerification,
    ParkContextDatasetBuildVerification, RequestContextDatasetBuild,
    CONTEXT_DATASET_INDEX_MANIFEST_MEDIA_TYPE, CONTEXT_DATASET_VALIDATION_EVIDENCE_MEDIA_TYPE,
    MAX_CONTEXT_DATASET_ARTIFACT_BYTES,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, ArtifactPurpose, ArtifactRef,
    AuthoringPackage, CommandOutcome, ContextDatasetResourceSpec, DataClassification,
    DeploymentClosure, JobState, Permission, PublishedVersionPayload, ResourceDocument, ResourceId,
    ResourceKind, ValidationSummary,
};
use insight_platform_jobs::{
    decide_terminal as decide_job_terminal, decide_wait as decide_job_wait,
    decide_wake as decide_job_wake, JobFence, WakeContract, WakeKind, WakeSource,
};
use sqlx::{Postgres, Row, Transaction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedContextDatasetBuild {
    pub payload: ContextDatasetBuildJobPayload,
    pub index_manifest: ArtifactRef,
    pub validation_evidence: ArtifactRef,
}

impl PgRepository {
    pub async fn request_context_dataset_build(
        &self,
        command: RequestContextDatasetBuild,
    ) -> Result<CommandOutcome<JobRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.pool().begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        if let Some(job_id) = claim_dataset_build_receipt(&mut transaction, &command).await? {
            let job =
                load_dataset_build_job(&mut transaction, &command.audit.tenant_id, &job_id).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(job));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::ContextWrite)
            .await?;
        let deployment = load_deployment(
            &mut transaction,
            &command.audit.tenant_id,
            &command.context_deployment.deployment_id,
        )
        .await?;
        if deployment.resource_id != command.context_resource_id.to_string()
            || deployment.bindings.digest
                != command.context_deployment.deployment_digest.to_string()
        {
            return Err(RepositoryError::NotFound("Context Deployment"));
        }
        let closure = match decode_deployment_closure(&deployment.bindings)? {
            DeploymentClosure::ContextSourceInterface(closure) => closure,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Context Deployment contains the wrong closure".to_owned(),
                ));
            }
        };
        validate_deployment_closure_exists(
            &mut transaction,
            &command.audit.tenant_id,
            &DeploymentClosure::ContextSourceInterface(closure.clone()),
        )
        .await?;
        let implementation = crate::invocation_repository::load_enabled_exact_published_version(
            &mut transaction,
            &command.audit.tenant_id,
            &closure.implementation,
            insight_platform_contracts::RegistryResourceKind::ContextSourceImplementation,
        )
        .await?;
        let ResourceDocument::ContextSourceImplementation(implementation) = implementation.document
        else {
            return Err(RepositoryError::CorruptRow(
                "Context Implementation revision contains the wrong document".to_owned(),
            ));
        };
        let (expected_dataset_version, expected_active_generation_id) =
            lock_dataset_build_target(&mut transaction, &command).await?;
        let active_build: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM insight_platform.jobs
                WHERE tenant_id = $1 AND work_class = 'context'
                  AND owner_kind = 'context_dataset' AND owner_id = $2
                  AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out')
            )
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.dataset_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if active_build {
            return Err(RepositoryError::Conflict("Context Dataset build"));
        }
        let artifact_authority =
            load_internal_artifact_admission_authority(&mut transaction, &command.audit.tenant_id)
                .await?;
        let minimum_retention_seconds = i64::try_from(
            artifact_authority
                .retention_policy
                .minimum_retention_seconds,
        )
        .map_err(|_| {
            RepositoryError::CorruptRow(
                "Artifact retention duration exceeds clock representation".to_owned(),
            )
        })?;
        let retain_until = command.deadline.max(
            database_now
                .checked_add_signed(chrono::Duration::seconds(minimum_retention_seconds))
                .ok_or_else(|| {
                    RepositoryError::InvalidInput(
                        "Artifact retention deadline overflows".to_owned(),
                    )
                })?,
        );
        let maximum_bytes = MAX_CONTEXT_DATASET_ARTIFACT_BYTES.min(
            checked_in_hard_limit_profile()
                .artifact
                .single_bytes
                .q1_default,
        );
        if maximum_bytes == 0 {
            return Err(RepositoryError::CorruptRow(
                "Context Dataset Artifact limit is zero".to_owned(),
            ));
        }
        for allocation in [
            &command.artifact_preallocations.index_manifest,
            &command.artifact_preallocations.validation_evidence,
        ] {
            reserve_internal_artifact_staging_quota(
                &mut transaction,
                &command.audit.tenant_id,
                &artifact_authority.quota_account_id,
                &allocation.quota_entry_id,
                &allocation.artifact_id,
                maximum_bytes,
                &command.audit.request_digest,
            )
            .await?;
        }
        let artifact_stages = ContextDatasetArtifactStages {
            schema_version: 1,
            index_manifest: context_dataset_artifact_stage(
                &artifact_authority,
                &command.artifact_preallocations.index_manifest,
                &command.job_id,
                CONTEXT_DATASET_INDEX_MANIFEST_MEDIA_TYPE,
                maximum_bytes,
                retain_until,
                command.deadline,
            ),
            validation_evidence: context_dataset_artifact_stage(
                &artifact_authority,
                &command.artifact_preallocations.validation_evidence,
                &command.job_id,
                CONTEXT_DATASET_VALIDATION_EVIDENCE_MEDIA_TYPE,
                maximum_bytes,
                retain_until,
                command.deadline,
            ),
        };
        let payload = ContextDatasetBuildJobPayload::from_request(
            &command,
            &closure,
            &implementation,
            expected_dataset_version,
            expected_active_generation_id,
            artifact_stages.clone(),
        )?;
        let typed = TypedPayload::from_versioned(1, &payload, 1_048_576)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, state,
                attempt_limit, scheduled_at, deadline, priority, request_digest,
                payload_schema_version, payload, payload_digest, created_at, updated_at,
                trace_id
            ) VALUES ($1, $2, 'context_dataset_build', 'context', 'context_dataset', $3, 'ready',
                      $4, $5, $6, 0, $7, $8, $9, $10, $5, $5, $11)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .bind(command.dataset_id.to_string())
        .bind(i32::from(command.attempt_limit))
        .bind(database_now)
        .bind(command.deadline)
        .bind(command.audit.request_digest.to_string())
        .bind(typed.schema_version)
        .bind(&typed.value)
        .bind(&typed.digest)
        .bind(command.audit.trace.trace_id.to_string())
        .execute(&mut *transaction)
        .await?;
        let artifact_attempt_limit = i32::try_from(
            checked_in_hard_limit_profile()
                .run_scheduler
                .attempts_per_work
                .q1_default,
        )
        .map_err(|_| {
            RepositoryError::InvalidInput(
                "Artifact verification attempt limit exceeds integer".to_owned(),
            )
        })?;
        for (allocation, stage) in [
            (
                &command.artifact_preallocations.index_manifest,
                &artifact_stages.index_manifest,
            ),
            (
                &command.artifact_preallocations.validation_evidence,
                &artifact_stages.validation_evidence,
            ),
        ] {
            insert_context_dataset_artifact_job(
                &mut transaction,
                &command,
                allocation,
                stage,
                artifact_attempt_limit,
                database_now,
            )
            .await?;
        }
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id.to_string(),
            "scheduled",
        )
        .await?;
        let job =
            load_dataset_build_job(&mut transaction, &command.audit.tenant_id, &command.job_id)
                .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(job))
    }

    pub async fn commit_context_dataset_build(
        &self,
        command: CommitContextDatasetBuild,
    ) -> Result<JobRecord, RepositoryError> {
        command.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let row = sqlx::query(
            "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR UPDATE",
        )
        .bind(command.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Context Dataset build Job"))?;
        let current = job_from_row(row)?;
        let owner: ResourceId = current
            .owner_id
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("Dataset Job owner is invalid".to_owned()))?;
        let payload: ContextDatasetBuildJobPayload =
            decode_versioned_payload(&current.payload, "Context Dataset build Job")?;
        payload.validate_for_owner(&owner)?;
        if owner != command.dataset_id
            || payload.job_id != command.job_id
            || payload.artifact_preallocations.generation_id != command.generation_id
            || payload.artifact_preallocations.index_manifest.artifact_id
                != *command.generation.index_manifest.artifact_id()
            || payload
                .artifact_preallocations
                .validation_evidence
                .artifact_id
                != *command.generation.validation_evidence.artifact_id()
            || payload.context_deployment != command.generation.context_deployment
            || payload.parser_profile != command.generation.parser_profile
            || payload.chunker_profile != command.generation.chunker_profile
            || payload.embedding_model_deployment != command.generation.embedding_model_deployment
            || payload.ranking_profile != command.generation.ranking_profile
            || current.lease_token_digest.as_deref() != Some(command.lease_token_digest.as_str())
        {
            return Err(RepositoryError::Conflict("Context Dataset build closure"));
        }
        let next = decide_job_terminal(
            &job_projection(&current)?,
            &command.fence,
            database_now,
            JobState::Succeeded,
        )?;
        let finalized_index = finalize_verified_context_dataset_artifact(
            &mut transaction,
            &command.tenant_id,
            &payload.artifact_preallocations.index_manifest,
            &payload.artifact_stages.index_manifest,
            &current.request_digest,
            database_now,
        )
        .await?;
        let finalized_validation = finalize_verified_context_dataset_artifact(
            &mut transaction,
            &command.tenant_id,
            &payload.artifact_preallocations.validation_evidence,
            &payload.artifact_stages.validation_evidence,
            &current.request_digest,
            database_now,
        )
        .await?;
        if finalized_index != command.generation.index_manifest
            || finalized_validation != command.generation.validation_evidence
        {
            return Err(RepositoryError::Conflict(
                "Context Dataset finalized Artifact closure",
            ));
        }
        require_ready_run_artifact(
            &mut transaction,
            &command.tenant_id,
            &command.generation.index_manifest,
        )
        .await?;
        require_ready_run_artifact(
            &mut transaction,
            &command.tenant_id,
            &command.generation.validation_evidence,
        )
        .await?;
        let generation_value = serde_json::to_value(&command.generation)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let generation_digest: insight_platform_contracts::Sha256Digest =
            canonical_digest(&generation_value)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
                .parse()
                .map_err(|failure: insight_platform_contracts::NominalTypeError| {
                    RepositoryError::InvalidInput(failure.to_string())
                })?;
        let document = ResourceDocument::ContextDataset(ContextDatasetResourceSpec {
            authoring_package: AuthoringPackage {
                artifact: command.generation.index_manifest.clone(),
                manifest_digest: command.generation.source_manifest_digest.clone(),
            },
            contract_digest: generation_digest.clone(),
            dependency_versions: vec![
                command.generation.parser_profile.clone(),
                command.generation.chunker_profile.clone(),
                command.generation.ranking_profile.clone(),
            ],
            policy_versions: vec![payload.data_policy.clone()],
            generation: command.generation.clone(),
        });
        document
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let document_digest: insight_platform_contracts::Sha256Digest = canonical_digest(
            &serde_json::to_value(&document)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
        )
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::NominalTypeError| {
            RepositoryError::InvalidInput(failure.to_string())
        })?;
        let published = PublishedVersionPayload {
            document,
            validation: ValidationSummary {
                validator_digest: command
                    .generation
                    .validation_evidence
                    .content_digest()
                    .clone(),
                validated_draft_digest: generation_digest,
                dependency_closure_digest: current.payload.digest.parse().map_err(|_| {
                    RepositoryError::CorruptRow(
                        "Dataset build payload digest is invalid".to_owned(),
                    )
                })?,
                security_evidence_digest: command
                    .generation
                    .validation_evidence
                    .content_digest()
                    .clone(),
                warnings: Vec::new(),
            },
        };
        let published_payload = TypedPayload::with_limit(1, &published, 1_048_576)?;
        let root_payload = ContextDatasetRootPayload {
            schema_version: 1,
            dataset_id: command.dataset_id.clone(),
            context_deployment: payload.context_deployment.clone(),
        };
        root_payload.validate()?;
        let root_typed = TypedPayload::from_versioned(1, &root_payload, 262_144)?;
        let revision_no = if let Some(expected_version) = payload.expected_dataset_version {
            let active_generation = payload
                .expected_active_generation_id
                .as_ref()
                .ok_or(RepositoryError::Conflict("Dataset active generation fence"))?;
            let affected = sqlx::query(
                r#"
                UPDATE insight_platform.resources
                SET version = version + 1, updated_at = $5
                WHERE tenant_id = $1 AND resource_id = $2 AND version = $3
                  AND active_version_id = $4 AND resource_kind = 'context_dataset'
                "#,
            )
            .bind(command.tenant_id.to_string())
            .bind(command.dataset_id.to_string())
            .bind(i64::try_from(expected_version).map_err(|_| {
                RepositoryError::InvalidInput("Dataset version exceeds bigint".to_owned())
            })?)
            .bind(active_generation.to_string())
            .bind(database_now)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if affected != 1 {
                return Err(RepositoryError::StaleFence);
            }
            sqlx::query_scalar::<_, i64>(
                "SELECT COALESCE(max(revision_no), 0) + 1 FROM insight_platform.resource_versions WHERE tenant_id = $1 AND resource_id = $2",
            )
            .bind(command.tenant_id.to_string())
            .bind(command.dataset_id.to_string())
            .fetch_one(&mut *transaction)
            .await?
        } else {
            sqlx::query(
                r#"
                INSERT INTO insight_platform.resources (
                    tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                    draft_generation, version, payload_schema_version, payload, payload_digest,
                    created_at, updated_at
                ) VALUES ($1, $2, 'context_dataset', 'active', 'enabled', 1, 1,
                          $3, $4, $5, $6, $6)
                "#,
            )
            .bind(command.tenant_id.to_string())
            .bind(command.dataset_id.to_string())
            .bind(root_typed.schema_version)
            .bind(&root_typed.value)
            .bind(&root_typed.digest)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?;
            1
        };
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resource_versions (
                tenant_id, resource_version_id, resource_id, resource_version_kind,
                revision_no, content_digest, artifact_id, payload_schema_version,
                payload, payload_digest, created_by, created_at
            ) VALUES ($1, $2, $3, 'dataset_generation', $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.generation_id.to_string())
        .bind(command.dataset_id.to_string())
        .bind(revision_no)
        .bind(document_digest.to_string())
        .bind(command.generation.index_manifest.artifact_id().to_string())
        .bind(published_payload.schema_version)
        .bind(&published_payload.value)
        .bind(&published_payload.digest)
        .bind(command.fence.worker_process_generation_id.to_string())
        .bind(database_now)
        .execute(&mut *transaction)
        .await?;
        let expected_previous = payload
            .expected_active_generation_id
            .as_ref()
            .map(ToString::to_string);
        let affected = sqlx::query(
            r#"
            UPDATE insight_platform.resources
            SET active_version_id = $3, updated_at = $4
            WHERE tenant_id = $1 AND resource_id = $2
              AND active_version_id IS NOT DISTINCT FROM $5
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.dataset_id.to_string())
        .bind(command.generation_id.to_string())
        .bind(database_now)
        .bind(expected_previous)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(RepositoryError::StaleFence);
        }
        let result_digest = document_digest.to_string();
        let row =
            sqlx::query(
                r#"
            UPDATE insight_platform.jobs
            SET state = 'succeeded', version = $4, result_digest = $5,
                worker_id = NULL, lease_token_digest = NULL, lease_expires_at = NULL,
                heartbeat_at = NULL, terminal_at = $6, updated_at = $6
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
            RETURNING *
            "#,
            )
            .bind(command.tenant_id.to_string())
            .bind(command.job_id.to_string())
            .bind(current.version)
            .bind(i64::try_from(next.version).map_err(|_| {
                RepositoryError::InvalidInput("Job version exceeds bigint".to_owned())
            })?)
            .bind(&result_digest)
            .bind(database_now)
            .fetch_one(&mut *transaction)
            .await?;
        let event_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "context_deployment": payload.context_deployment,
                "dataset_id": command.dataset_id,
                "generation_id": command.generation_id,
                "job_id": command.job_id,
                "result_digest": result_digest,
            }),
        )?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.events (
                tenant_id, event_id, aggregate_kind, aggregate_id, aggregate_version,
                trace_id, event_type, visibility, payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, 'context_dataset', $3, $4,
                      $5, 'context.dataset_generation_created', 'internal', $6, $7, $8)
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.event_id.to_string())
        .bind(command.dataset_id.to_string())
        .bind(revision_no)
        .bind(current.trace.trace_id.to_string())
        .bind(event_payload.schema_version)
        .bind(&event_payload.value)
        .bind(&event_payload.digest)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO insight_platform.outbox_events (tenant_id, outbox_id, event_id, trace_id) VALUES ($1, $2, $3, $4)",
        )
        .bind(command.tenant_id.to_string())
        .bind(command.outbox_id.to_string())
        .bind(command.event_id.to_string())
        .bind(current.trace.trace_id.to_string())
        .execute(&mut *transaction)
        .await?;
        let record = job_from_row(row)?;
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn park_context_dataset_build_for_verification(
        &self,
        command: ParkContextDatasetBuildVerification,
    ) -> Result<JobRecord, RepositoryError> {
        command.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let row = sqlx::query(
            "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR UPDATE",
        )
        .bind(command.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Context Dataset build Job"))?;
        let current = job_from_row(row)?;
        let owner: ResourceId = current
            .owner_id
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("Dataset Job owner is invalid".to_owned()))?;
        let mut payload: ContextDatasetBuildJobPayload =
            decode_versioned_payload(&current.payload, "Context Dataset build Job")?;
        payload.validate_for_owner(&owner)?;
        if owner != command.dataset_id || payload.job_id != command.job_id {
            return Err(RepositoryError::Conflict(
                "Context Dataset verification closure",
            ));
        }
        let verification_states = sqlx::query(
            r#"
            SELECT job_id, state FROM insight_platform.jobs
            WHERE tenant_id = $1 AND work_class = 'artifact' AND job_kind = 'artifact_scan'
              AND owner_kind = 'artifact' AND job_id = ANY($2)
            ORDER BY job_id
            FOR SHARE
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(vec![
            payload
                .artifact_preallocations
                .index_manifest
                .verification_job_id
                .to_string(),
            payload
                .artifact_preallocations
                .validation_evidence
                .verification_job_id
                .to_string(),
        ])
        .fetch_all(&mut *transaction)
        .await?;
        if verification_states.len() != 2 {
            return Err(RepositoryError::CorruptRow(
                "Context Dataset Artifact verification closure is incomplete".to_owned(),
            ));
        }
        let states = verification_states
            .iter()
            .map(|row| row.try_get::<String, _>("state"))
            .collect::<Result<Vec<_>, _>>()?;
        if states.iter().all(|state| state == "succeeded") {
            transaction.commit().await?;
            return Ok(current);
        }
        if states
            .iter()
            .any(|state| matches!(state.as_str(), "failed" | "cancelled" | "timed_out"))
        {
            return Err(RepositoryError::Conflict(
                "Context Dataset Artifact verification failed",
            ));
        }
        let staged_artifact_count: i64 = sqlx::query_scalar(
            r#"
            SELECT count(*) FROM insight_platform.artifacts
            WHERE tenant_id = $1 AND artifact_id = ANY($2)
              AND state IN ('verifying', 'verified', 'quarantined', 'rejected', 'ready')
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(vec![
            payload
                .artifact_preallocations
                .index_manifest
                .artifact_id
                .to_string(),
            payload
                .artifact_preallocations
                .validation_evidence
                .artifact_id
                .to_string(),
        ])
        .fetch_one(&mut *transaction)
        .await?;
        if staged_artifact_count != 2 {
            return Err(RepositoryError::Conflict(
                "Context Dataset Artifact has not been staged",
            ));
        }
        let wake = WakeContract {
            kind: WakeKind::RemoteInvocation,
            generation: command.fence.lease_generation,
            accepted_sources: vec![WakeSource::Signal, WakeSource::Timeout],
            expected_response_schema_digest: None,
            opaque_state_digest: Some(current.payload.digest.parse().map_err(|_| {
                RepositoryError::CorruptRow(
                    "Context Dataset build payload digest is invalid".to_owned(),
                )
            })?),
            next_poll_at: None,
            poll_count: 0,
            poll_limit: 0,
            callback_binding_digest: None,
            deadline: current.deadline,
        };
        let next = decide_job_wait(
            &job_projection(&current)?,
            &command.fence,
            database_now,
            wake.clone(),
        )?;
        payload.wake_contract = Some(wake.clone());
        payload.validate_for_owner(&owner)?;
        let next_payload = TypedPayload::from_versioned(1, &payload, 1_048_576)?;
        let next_version = i64::try_from(next.version).map_err(|_| {
            RepositoryError::InvalidInput("Context Dataset Job version exceeds bigint".to_owned())
        })?;
        let wake_generation = i64::try_from(wake.generation).map_err(|_| {
            RepositoryError::InvalidInput(
                "Context Dataset wake generation exceeds bigint".to_owned(),
            )
        })?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.jobs
            SET state = 'waiting', version = $4, worker_id = NULL,
                lease_token_digest = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
                retry_at = NULL, wake_kind = $5, wake_state = 'pending',
                wake_generation = $6, updated_at = $7,
                payload_schema_version = $8, payload = $9, payload_digest = $10
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND work_class = 'context' AND job_kind = 'context_dataset_build'
              AND owner_kind = 'context_dataset' AND state = 'running'
            RETURNING *
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .bind(current.version)
        .bind(next_version)
        .bind(wake.kind.as_str())
        .bind(wake_generation)
        .bind(database_now)
        .bind(next_payload.schema_version)
        .bind(&next_payload.value)
        .bind(&next_payload.digest)
        .fetch_one(&mut *transaction)
        .await?;
        append_scheduler_event_with_trace(
            &mut transaction,
            current.trace,
            &command.tenant_id.to_string(),
            &command.event_id,
            &command.outbox_id,
            "job",
            &command.job_id.to_string(),
            next_version,
            None,
            "context.dataset_verification_pending",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "index_verification_job_id": payload.artifact_preallocations.index_manifest.verification_job_id,
                    "job_id": command.job_id,
                    "validation_verification_job_id": payload.artifact_preallocations.validation_evidence.verification_job_id,
                }),
            )?,
        )
        .await?;
        let record = job_from_row(row)?;
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn resolve_context_dataset_build(
        &self,
        tenant_id: ResourceId,
        job_id: ResourceId,
        fence: JobFence,
    ) -> Result<ResolvedContextDatasetBuild, RepositoryError> {
        if tenant_id.kind() != ResourceKind::Tenant || job_id.kind() != ResourceKind::Job {
            return Err(RepositoryError::InvalidInput(
                "Context Dataset build identity is invalid".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let row = sqlx::query(
            "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR UPDATE",
        )
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Context Dataset build Job"))?;
        let job = job_from_row(row)?;
        let owner: ResourceId = job
            .owner_id
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("Dataset Job owner is invalid".to_owned()))?;
        let payload: ContextDatasetBuildJobPayload =
            decode_versioned_payload(&job.payload, "Context Dataset build Job")?;
        payload.validate_for_owner(&owner)?;
        require_context_dataset_job_fence(&job, &fence, database_now)?;
        let index_manifest = load_verified_context_dataset_artifact(
            &mut transaction,
            &tenant_id,
            &payload.artifact_preallocations.index_manifest,
            &payload.artifact_stages.index_manifest,
        )
        .await?;
        let validation_evidence = load_verified_context_dataset_artifact(
            &mut transaction,
            &tenant_id,
            &payload.artifact_preallocations.validation_evidence,
            &payload.artifact_stages.validation_evidence,
        )
        .await?;
        transaction.commit().await?;
        Ok(ResolvedContextDatasetBuild {
            payload,
            index_manifest,
            validation_evidence,
        })
    }

    pub async fn fail_context_dataset_build_verification(
        &self,
        command: FailContextDatasetBuildVerification,
    ) -> Result<JobRecord, RepositoryError> {
        command.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let row = sqlx::query(
            "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR UPDATE",
        )
        .bind(command.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Context Dataset build Job"))?;
        let current = job_from_row(row)?;
        let owner: ResourceId = current
            .owner_id
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("Dataset Job owner is invalid".to_owned()))?;
        let payload: ContextDatasetBuildJobPayload =
            decode_versioned_payload(&current.payload, "Context Dataset build Job")?;
        payload.validate_for_owner(&owner)?;
        if owner != command.dataset_id || payload.job_id != command.job_id {
            return Err(RepositoryError::Conflict(
                "Context Dataset verification failure closure",
            ));
        }
        require_context_dataset_job_fence(&current, &command.fence, database_now)?;
        let mut terminals = Vec::with_capacity(2);
        for (allocation, stage) in [
            (
                &payload.artifact_preallocations.index_manifest,
                &payload.artifact_stages.index_manifest,
            ),
            (
                &payload.artifact_preallocations.validation_evidence,
                &payload.artifact_stages.validation_evidence,
            ),
        ] {
            terminals.push(
                load_terminal_context_dataset_artifact(
                    &mut transaction,
                    &command.tenant_id,
                    allocation,
                    stage,
                )
                .await?,
            );
        }
        let failure_code = terminals
            .iter()
            .find_map(|terminal| terminal.failure_code(database_now))
            .ok_or(RepositoryError::Conflict(
                "Context Dataset verification has no terminal failure",
            ))?;
        for ((allocation, stage), terminal) in [
            (
                &payload.artifact_preallocations.index_manifest,
                &payload.artifact_stages.index_manifest,
            ),
            (
                &payload.artifact_preallocations.validation_evidence,
                &payload.artifact_stages.validation_evidence,
            ),
        ]
        .into_iter()
        .zip(&terminals)
        {
            settle_context_dataset_artifact_quota(
                &mut transaction,
                &command.tenant_id,
                allocation,
                stage,
                terminal.size_bytes,
                &current.request_digest,
                database_now,
            )
            .await?;
            finalize_failed_context_dataset_artifact(
                &mut transaction,
                &command.tenant_id,
                allocation,
                terminal,
                database_now,
            )
            .await?;
        }
        let result_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "failure_code": failure_code,
                "index_verification_evidence_digest": terminals[0].evidence_digest,
                "validation_verification_evidence_digest": terminals[1].evidence_digest,
            }),
        )?;
        let next = decide_job_terminal(
            &job_projection(&current)?,
            &command.fence,
            database_now,
            JobState::Failed,
        )?;
        let next_version = i64::try_from(next.version).map_err(|_| {
            RepositoryError::InvalidInput("Context Dataset Job version exceeds bigint".to_owned())
        })?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.jobs
            SET state = 'failed', version = $4, result_digest = $5,
                worker_id = NULL, lease_token_digest = NULL, lease_expires_at = NULL,
                heartbeat_at = NULL, terminal_at = $6, updated_at = $6
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND work_class = 'context' AND job_kind = 'context_dataset_build'
              AND owner_kind = 'context_dataset' AND state = 'running'
            RETURNING *
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .bind(current.version)
        .bind(next_version)
        .bind(&result_payload.digest)
        .bind(database_now)
        .fetch_one(&mut *transaction)
        .await?;
        append_scheduler_event_with_trace(
            &mut transaction,
            current.trace,
            &command.tenant_id.to_string(),
            &command.event_id,
            &command.outbox_id,
            "job",
            &command.job_id.to_string(),
            next_version,
            None,
            "context.dataset_build_failed",
            &result_payload,
        )
        .await?;
        let record = job_from_row(row)?;
        transaction.commit().await?;
        Ok(record)
    }
}

struct TerminalContextDatasetArtifact {
    artifact_state: String,
    artifact_version: i64,
    verification_state: String,
    verification_version: i64,
    disposition: ArtifactScanDisposition,
    evidence_digest: insight_platform_contracts::Sha256Digest,
    expires_at: DateTime<Utc>,
    size_bytes: u64,
}

impl TerminalContextDatasetArtifact {
    fn failure_code(&self, database_now: DateTime<Utc>) -> Option<&'static str> {
        if self.expires_at <= database_now {
            return Some("context_dataset_verification_expired");
        }
        match self.disposition {
            ArtifactScanDisposition::Verified => None,
            ArtifactScanDisposition::Quarantined => Some("context_dataset_artifact_quarantined"),
            ArtifactScanDisposition::Rejected => Some("context_dataset_artifact_rejected"),
            ArtifactScanDisposition::Corrupt => Some("context_dataset_artifact_corrupt"),
        }
    }
}

fn context_dataset_artifact_stage(
    authority: &InternalArtifactAdmissionAuthority,
    allocation: &ContextDatasetArtifactPreallocation,
    producer_job_id: &ResourceId,
    media_type: &str,
    maximum_bytes: u64,
    retain_until: DateTime<Utc>,
    deadline: DateTime<Utc>,
) -> ArtifactAwaitingStageSnapshot {
    ArtifactAwaitingStageSnapshot {
        schema_version: 1,
        producer_job_id: producer_job_id.clone(),
        artifact_id: allocation.artifact_id.clone(),
        blob_id: allocation.blob_id.clone(),
        quota_account_id: authority.quota_account_id.clone(),
        quota_entry_id: allocation.quota_entry_id.clone(),
        purpose: ArtifactPurpose::ContextDerived,
        classification: DataClassification::Internal,
        maximum_bytes,
        declared_media_type: media_type.to_owned(),
        retention_policy_revision: authority.retention_policy_revision.clone(),
        artifact_io_policy_revision: authority.artifact_io_policy_revision.clone(),
        scanner_contract_digest: authority.artifact_io_policy.scanner_contract_digest.clone(),
        ruleset_digest: authority.artifact_io_rules_digest.clone(),
        evidence_ttl_milliseconds: authority
            .artifact_io_policy
            .verification_evidence_ttl_milliseconds,
        retry_backoff_milliseconds: authority
            .artifact_io_policy
            .verification_retry_backoff_milliseconds,
        write_storage_binding_digest: authority
            .artifact_io_policy
            .write_storage_binding_digest
            .clone(),
        encryption_domain_id: authority.artifact_io_policy.encryption_domain_id.clone(),
        retain_until,
        deadline,
    }
}

async fn insert_context_dataset_artifact_job(
    transaction: &mut Transaction<'_, Postgres>,
    command: &RequestContextDatasetBuild,
    allocation: &ContextDatasetArtifactPreallocation,
    stage: &ArtifactAwaitingStageSnapshot,
    attempt_limit: i32,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let payload = ArtifactJobPayload::AwaitingStage {
        stage: stage.clone(),
    };
    payload
        .validate_for_owner(&allocation.artifact_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let typed = TypedPayload::new(1, &payload)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
            tenant_id, job_id, job_kind, work_class, owner_kind, owner_id,
            state, attempt_limit, scheduled_at, deadline, priority, request_digest,
            payload_schema_version, payload, payload_digest, created_at, updated_at,
            trace_id
        ) VALUES ($1, $2, 'artifact_scan', 'artifact', 'artifact', $3,
                  'waiting', $4, $5, $6, 0, $7, $8, $9, $10, $5, $5, $11)
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(allocation.verification_job_id.to_string())
    .bind(allocation.artifact_id.to_string())
    .bind(attempt_limit)
    .bind(database_now)
    .bind(command.deadline)
    .bind(command.audit.request_digest.to_string())
    .bind(typed.schema_version)
    .bind(&typed.value)
    .bind(&typed.digest)
    .bind(command.audit.trace.trace_id.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) struct ContextDatasetArtifactStageAuthority {
    pub trace: insight_platform_contracts::TraceIdentityV1,
    pub principal_id: ResourceId,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn require_context_dataset_artifact_stage_authority(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    producer_job_id: &ResourceId,
    producer_fence: &JobFence,
    verification_job_id: &ResourceId,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<ContextDatasetArtifactStageAuthority, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT * FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2
          AND work_class = 'context' AND job_kind = 'context_dataset_build'
          AND owner_kind = 'context_dataset'
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(producer_job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Context Dataset build Job"))?;
    let job = job_from_row(row)?;
    let owner: ResourceId = job
        .owner_id
        .parse()
        .map_err(|_| RepositoryError::CorruptRow("Dataset Job owner is invalid".to_owned()))?;
    let payload: ContextDatasetBuildJobPayload =
        decode_versioned_payload(&job.payload, "Context Dataset build Job")?;
    payload.validate_for_owner(&owner)?;
    let expected_version =
        i64::try_from(producer_fence.expected_version).map_err(|_| RepositoryError::StaleFence)?;
    let expected_lease =
        i64::try_from(producer_fence.lease_generation).map_err(|_| RepositoryError::StaleFence)?;
    let expected_worker_id = producer_fence.worker_process_generation_id.to_string();
    let allocation_matches = [
        &payload.artifact_preallocations.index_manifest,
        &payload.artifact_preallocations.validation_evidence,
    ]
    .iter()
    .any(|allocation| {
        allocation.verification_job_id == *verification_job_id
            && allocation.artifact_id == *artifact_id
            && allocation.blob_id == *blob_id
    });
    if job.state != JobState::Running.to_string()
        || job.version != expected_version
        || job.worker_id.as_deref() != Some(expected_worker_id.as_str())
        || job.lease_epoch != expected_lease
        || job.lease_token_digest.as_deref() != Some(producer_fence.token_digest.as_str())
        || job
            .lease_expires_at
            .is_none_or(|deadline| deadline < database_now)
        || job.deadline < database_now
        || !allocation_matches
    {
        return Err(RepositoryError::StaleFence);
    }
    Ok(ContextDatasetArtifactStageAuthority {
        trace: job.trace,
        principal_id: payload.principal_id,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn wake_context_dataset_build_after_artifact_verification(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    producer_job_id: &ResourceId,
    verification_job_id: &ResourceId,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT * FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2
          AND work_class = 'context' AND job_kind = 'context_dataset_build'
          AND owner_kind = 'context_dataset'
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(producer_job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Context Dataset build Job"))?;
    let job = job_from_row(row)?;
    let owner: ResourceId = job
        .owner_id
        .parse()
        .map_err(|_| RepositoryError::CorruptRow("Dataset Job owner is invalid".to_owned()))?;
    let mut payload: ContextDatasetBuildJobPayload =
        decode_versioned_payload(&job.payload, "Context Dataset build Job")?;
    payload.validate_for_owner(&owner)?;
    let allocations = [
        &payload.artifact_preallocations.index_manifest,
        &payload.artifact_preallocations.validation_evidence,
    ];
    let observed_allocation = allocations
        .iter()
        .find(|allocation| allocation.verification_job_id == *verification_job_id)
        .ok_or(RepositoryError::Conflict(
            "Context Dataset verification Artifact",
        ))?;
    if observed_allocation.artifact_id != *artifact_id || observed_allocation.blob_id != *blob_id {
        return Err(RepositoryError::Conflict(
            "Context Dataset verification Artifact closure",
        ));
    }
    let terminal_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM insight_platform.artifacts
        WHERE tenant_id = $1 AND artifact_id = ANY($2)
          AND state IN ('verified', 'quarantined', 'rejected')
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(vec![
        payload
            .artifact_preallocations
            .index_manifest
            .artifact_id
            .to_string(),
        payload
            .artifact_preallocations
            .validation_evidence
            .artifact_id
            .to_string(),
    ])
    .fetch_one(&mut **transaction)
    .await?;
    if terminal_count != 2 || job.state == JobState::Running.to_string() {
        return Ok(());
    }
    if job.state != JobState::Waiting.to_string()
        || job.wake_kind.as_deref() != Some(WakeKind::RemoteInvocation.as_str())
        || job.wake_state.as_deref() != Some("pending")
        || job.wake_generation <= 0
    {
        return Err(RepositoryError::Conflict(
            "Context Dataset verification wake authority",
        ));
    }
    let current = job_projection(&job)?;
    let next = decide_job_wake(
        &current,
        u64::try_from(job.wake_generation).map_err(|_| {
            RepositoryError::CorruptRow("Context Dataset wake generation is invalid".to_owned())
        })?,
        WakeSource::Signal,
        database_now,
    )?;
    let next_version = i64::try_from(next.version).map_err(|_| {
        RepositoryError::InvalidInput("Context Dataset Job version exceeds bigint".to_owned())
    })?;
    payload.wake_contract = None;
    payload.validate_for_owner(&owner)?;
    let next_payload = TypedPayload::from_versioned(1, &payload, 1_048_576)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'ready', version = $4, scheduled_at = $5,
            wake_kind = NULL, wake_state = NULL, wake_generation = 0, updated_at = $5,
            payload_schema_version = $6, payload = $7, payload_digest = $8
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'context' AND job_kind = 'context_dataset_build'
          AND owner_kind = 'context_dataset' AND state = 'waiting'
          AND wake_kind = 'remote_invocation' AND wake_state = 'pending'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(producer_job_id.to_string())
    .bind(job.version)
    .bind(next_version)
    .bind(database_now)
    .bind(next_payload.schema_version)
    .bind(&next_payload.value)
    .bind(&next_payload.digest)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict(
            "Context Dataset verification wake CAS",
        ));
    }
    let event_id = ResourceId::from_uuid_v7(ResourceKind::Event, uuid::Uuid::now_v7())
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let outbox_id = ResourceId::from_uuid_v7(ResourceKind::OutboxEvent, uuid::Uuid::now_v7())
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    append_scheduler_event_with_trace(
        transaction,
        job.trace,
        &tenant_id.to_string(),
        &event_id,
        &outbox_id,
        "job",
        &producer_job_id.to_string(),
        next_version,
        None,
        "context.dataset_verification_woken",
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "job_id": producer_job_id,
                "verification_job_id": verification_job_id,
            }),
        )?,
    )
    .await
}

fn require_context_dataset_job_fence(
    job: &JobRecord,
    fence: &JobFence,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let expected_version =
        i64::try_from(fence.expected_version).map_err(|_| RepositoryError::StaleFence)?;
    let expected_lease =
        i64::try_from(fence.lease_generation).map_err(|_| RepositoryError::StaleFence)?;
    let expected_worker_id = fence.worker_process_generation_id.to_string();
    if job.state != JobState::Running.to_string()
        || job.version != expected_version
        || job.worker_id.as_deref() != Some(expected_worker_id.as_str())
        || job.lease_epoch != expected_lease
        || job.lease_token_digest.as_deref() != Some(fence.token_digest.as_str())
        || job
            .lease_expires_at
            .is_none_or(|deadline| deadline < database_now)
        || job.deadline < database_now
    {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

async fn load_terminal_context_dataset_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    allocation: &ContextDatasetArtifactPreallocation,
    stage: &ArtifactAwaitingStageSnapshot,
) -> Result<TerminalContextDatasetArtifact, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT artifact.purpose, artifact.classification, artifact.expected_size_bytes,
               artifact.expected_digest, artifact.declared_media_type,
               artifact.verified_media_type, artifact.state AS artifact_state,
               artifact.version AS artifact_version, artifact.metadata_schema_version,
               artifact.metadata, artifact.metadata_digest,
               blob.content_digest, blob.size_bytes, blob.state AS blob_state,
               verification.state AS verification_state,
               verification.version AS verification_version,
               verification.result_digest AS verification_result_digest
        FROM insight_platform.artifacts AS artifact
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
        JOIN insight_platform.jobs AS verification
          ON verification.tenant_id = artifact.tenant_id
         AND verification.job_id = $3 AND verification.owner_id = artifact.artifact_id
        WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
          AND verification.work_class = 'artifact'
          AND verification.job_kind = 'artifact_scan'
          AND verification.owner_kind = 'artifact'
        FOR SHARE OF artifact, blob, verification
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(allocation.artifact_id.to_string())
    .bind(allocation.verification_job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "terminal Context Dataset Artifact",
    ))?;
    let metadata_payload = TypedPayload {
        schema_version: row.try_get("metadata_schema_version")?,
        value: row.try_get("metadata")?,
        digest: row.try_get("metadata_digest")?,
    };
    let metadata: ArtifactMetadataSnapshot =
        decode_versioned_payload(&metadata_payload, "Context Dataset Artifact metadata")?;
    metadata
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let verification = metadata
        .current_verification
        .as_ref()
        .ok_or(RepositoryError::Conflict(
            "Context Dataset verification evidence",
        ))?;
    let content_digest = row
        .try_get::<Option<String>, _>("content_digest")?
        .ok_or(RepositoryError::Conflict(
            "Context Dataset Artifact content digest",
        ))?
        .parse::<insight_platform_contracts::Sha256Digest>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let size_bytes = u64::try_from(
        row.try_get::<Option<i64>, _>("size_bytes")?
            .ok_or(RepositoryError::Conflict("Context Dataset Artifact size"))?,
    )
    .map_err(|_| RepositoryError::CorruptRow("negative Artifact size".to_owned()))?;
    let media_type = row
        .try_get::<Option<String>, _>("verified_media_type")?
        .ok_or(RepositoryError::Conflict(
            "Context Dataset verified media type",
        ))?;
    let artifact_state = row.try_get::<String, _>("artifact_state")?;
    let verification_state = row.try_get::<String, _>("verification_state")?;
    let verification_result_digest =
        row.try_get::<Option<String>, _>("verification_result_digest")?;
    let expected_artifact_state = match verification.disposition {
        ArtifactScanDisposition::Verified => "verified",
        ArtifactScanDisposition::Quarantined | ArtifactScanDisposition::Corrupt => "quarantined",
        ArtifactScanDisposition::Rejected => "rejected",
    };
    let expected_verification_state = match verification.disposition {
        ArtifactScanDisposition::Rejected => "failed",
        ArtifactScanDisposition::Verified
        | ArtifactScanDisposition::Quarantined
        | ArtifactScanDisposition::Corrupt => "waiting",
    };
    if row.try_get::<String, _>("purpose")? != ArtifactPurpose::ContextDerived.as_str()
        || row.try_get::<String, _>("classification")? != DataClassification::Internal.as_str()
        || row.try_get::<String, _>("blob_state")? != "verified"
        || artifact_state != expected_artifact_state
        || verification_state != expected_verification_state
        || row.try_get::<i64, _>("expected_size_bytes")?
            != i64::try_from(size_bytes).unwrap_or(i64::MAX)
        || row.try_get::<Option<String>, _>("expected_digest")? != Some(content_digest.to_string())
        || row.try_get::<Option<String>, _>("declared_media_type")?
            != Some(stage.declared_media_type.clone())
        || media_type != stage.declared_media_type
        || verification.scan_job_id != allocation.verification_job_id
        || verification.content_digest != content_digest
        || verification.size_bytes != size_bytes
        || verification.verified_media_type != media_type
        || (verification_state == "failed")
            != verification_result_digest
                .as_deref()
                .is_some_and(|digest| digest == verification.evidence_digest.to_string())
    {
        return Err(RepositoryError::Conflict(
            "Context Dataset terminal verification evidence drift",
        ));
    }
    Ok(TerminalContextDatasetArtifact {
        artifact_state,
        artifact_version: row.try_get("artifact_version")?,
        verification_state,
        verification_version: row.try_get("verification_version")?,
        disposition: verification.disposition,
        evidence_digest: verification.evidence_digest.clone(),
        expires_at: verification.expires_at,
        size_bytes,
    })
}

async fn finalize_failed_context_dataset_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    allocation: &ContextDatasetArtifactPreallocation,
    terminal: &TerminalContextDatasetArtifact,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if terminal.artifact_state == "verified" {
        let affected = sqlx::query(
            r#"
            UPDATE insight_platform.artifacts
            SET state = 'quarantined', version = version + 1, updated_at = $5
            WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
              AND state = $4
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(allocation.artifact_id.to_string())
        .bind(terminal.artifact_version)
        .bind(&terminal.artifact_state)
        .bind(database_now)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(RepositoryError::Conflict(
                "Context Dataset failed Artifact quarantine CAS",
            ));
        }
    }
    if terminal.verification_state == "waiting" {
        let affected = sqlx::query(
            r#"
            UPDATE insight_platform.jobs
            SET state = 'failed', version = version + 1, result_digest = $4,
                terminal_at = $5, updated_at = $5
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND work_class = 'artifact' AND job_kind = 'artifact_scan'
              AND owner_kind = 'artifact' AND state = 'waiting'
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(allocation.verification_job_id.to_string())
        .bind(terminal.verification_version)
        .bind(terminal.evidence_digest.to_string())
        .bind(database_now)
        .execute(&mut **transaction)
        .await?
        .rows_affected();
        if affected != 1 {
            return Err(RepositoryError::Conflict(
                "Context Dataset failed verification Job CAS",
            ));
        }
    }
    Ok(())
}

async fn load_verified_context_dataset_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    allocation: &ContextDatasetArtifactPreallocation,
    stage: &ArtifactAwaitingStageSnapshot,
) -> Result<ArtifactRef, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT artifact.purpose, artifact.classification, artifact.expected_size_bytes,
               artifact.expected_digest, artifact.declared_media_type,
               artifact.verified_media_type, artifact.state AS artifact_state,
               artifact.metadata_schema_version, artifact.metadata, artifact.metadata_digest,
               blob.content_digest, blob.size_bytes, blob.state AS blob_state,
               verification.state AS verification_state
        FROM insight_platform.artifacts AS artifact
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
        JOIN insight_platform.jobs AS verification
          ON verification.tenant_id = artifact.tenant_id
         AND verification.job_id = $3 AND verification.owner_id = artifact.artifact_id
        WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
          AND verification.work_class = 'artifact'
          AND verification.job_kind = 'artifact_scan'
          AND verification.owner_kind = 'artifact'
        FOR SHARE OF artifact, blob, verification
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(allocation.artifact_id.to_string())
    .bind(allocation.verification_job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "verified Context Dataset Artifact",
    ))?;
    let metadata_payload = TypedPayload {
        schema_version: row.try_get("metadata_schema_version")?,
        value: row.try_get("metadata")?,
        digest: row.try_get("metadata_digest")?,
    };
    let metadata: ArtifactMetadataSnapshot =
        decode_versioned_payload(&metadata_payload, "Context Dataset Artifact metadata")?;
    metadata
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let verification = metadata
        .current_verification
        .as_ref()
        .ok_or(RepositoryError::Conflict(
            "Context Dataset verification evidence",
        ))?;
    let content_digest = row
        .try_get::<Option<String>, _>("content_digest")?
        .ok_or(RepositoryError::Conflict(
            "Context Dataset Artifact content digest",
        ))?
        .parse::<insight_platform_contracts::Sha256Digest>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let size_bytes = u64::try_from(
        row.try_get::<Option<i64>, _>("size_bytes")?
            .ok_or(RepositoryError::Conflict("Context Dataset Artifact size"))?,
    )
    .map_err(|_| RepositoryError::CorruptRow("negative Artifact size".to_owned()))?;
    let media_type = row
        .try_get::<Option<String>, _>("verified_media_type")?
        .ok_or(RepositoryError::Conflict(
            "Context Dataset verified media type",
        ))?;
    if row.try_get::<String, _>("purpose")? != ArtifactPurpose::ContextDerived.as_str()
        || row.try_get::<String, _>("classification")? != DataClassification::Internal.as_str()
        || row.try_get::<String, _>("artifact_state")? != "verified"
        || row.try_get::<String, _>("blob_state")? != "verified"
        || row.try_get::<String, _>("verification_state")? != "waiting"
        || row.try_get::<i64, _>("expected_size_bytes")?
            != i64::try_from(size_bytes).unwrap_or(i64::MAX)
        || row.try_get::<Option<String>, _>("expected_digest")? != Some(content_digest.to_string())
        || row.try_get::<Option<String>, _>("declared_media_type")?
            != Some(stage.declared_media_type.clone())
        || media_type != stage.declared_media_type
        || verification.disposition != ArtifactScanDisposition::Verified
        || verification.scan_job_id != allocation.verification_job_id
        || verification.content_digest != content_digest
        || verification.size_bytes != size_bytes
        || verification.verified_media_type != media_type
    {
        return Err(RepositoryError::Conflict(
            "Context Dataset verification evidence drift",
        ));
    }
    ArtifactRef::new(
        allocation.artifact_id.clone(),
        content_digest,
        size_bytes,
        media_type,
        DataClassification::Internal,
        metadata.display_name,
    )
    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
}

async fn finalize_verified_context_dataset_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    allocation: &ContextDatasetArtifactPreallocation,
    stage: &ArtifactAwaitingStageSnapshot,
    request_digest: &str,
    database_now: DateTime<Utc>,
) -> Result<ArtifactRef, RepositoryError> {
    let artifact =
        load_verified_context_dataset_artifact(transaction, tenant_id, allocation, stage).await?;
    let row = sqlx::query(
        r#"
        SELECT artifact.version AS artifact_version,
               artifact.metadata_schema_version, artifact.metadata, artifact.metadata_digest,
               verification.version AS verification_version
        FROM insight_platform.artifacts AS artifact
        JOIN insight_platform.jobs AS verification
          ON verification.tenant_id = artifact.tenant_id
         AND verification.job_id = $3 AND verification.owner_id = artifact.artifact_id
        WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
          AND artifact.state = 'verified' AND verification.state = 'waiting'
        FOR UPDATE OF artifact, verification
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(allocation.artifact_id.to_string())
    .bind(allocation.verification_job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "Context Dataset Artifact finalize authority",
    ))?;
    let metadata_payload = TypedPayload {
        schema_version: row.try_get("metadata_schema_version")?,
        value: row.try_get("metadata")?,
        digest: row.try_get("metadata_digest")?,
    };
    let metadata: ArtifactMetadataSnapshot =
        decode_versioned_payload(&metadata_payload, "Context Dataset Artifact metadata")?;
    let evidence_digest = metadata
        .current_verification
        .as_ref()
        .ok_or(RepositoryError::Conflict(
            "Context Dataset verification evidence",
        ))?
        .evidence_digest
        .clone();
    settle_context_dataset_artifact_quota(
        transaction,
        tenant_id,
        allocation,
        stage,
        artifact.byte_length(),
        request_digest,
        database_now,
    )
    .await?;
    let artifact_version: i64 = row.try_get("artifact_version")?;
    let artifact_affected = sqlx::query(
        r#"
        UPDATE insight_platform.artifacts
        SET state = 'ready', version = $4, updated_at = $5
        WHERE tenant_id = $1 AND artifact_id = $2 AND version = $3
          AND state = 'verified'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(allocation.artifact_id.to_string())
    .bind(artifact_version)
    .bind(
        artifact_version
            .checked_add(1)
            .ok_or(RepositoryError::Conflict(
                "Context Dataset Artifact version",
            ))?,
    )
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if artifact_affected != 1 {
        return Err(RepositoryError::Conflict(
            "Context Dataset Artifact ready CAS",
        ));
    }
    let verification_version: i64 = row.try_get("verification_version")?;
    let verification_affected = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'succeeded', version = $4, result_digest = $5,
            terminal_at = $6, updated_at = $6
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND work_class = 'artifact' AND job_kind = 'artifact_scan'
          AND owner_kind = 'artifact' AND state = 'waiting'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(allocation.verification_job_id.to_string())
    .bind(verification_version)
    .bind(
        verification_version
            .checked_add(1)
            .ok_or(RepositoryError::Conflict(
                "Context Dataset verification Job version",
            ))?,
    )
    .bind(evidence_digest.to_string())
    .bind(database_now)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if verification_affected != 1 {
        return Err(RepositoryError::Conflict(
            "Context Dataset verification Job finalize CAS",
        ));
    }
    Ok(artifact)
}

#[allow(clippy::too_many_arguments)]
async fn settle_context_dataset_artifact_quota(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    allocation: &ContextDatasetArtifactPreallocation,
    stage: &ArtifactAwaitingStageSnapshot,
    actual_amount: u64,
    request_digest: &str,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let actual_amount = i64::try_from(actual_amount).map_err(|_| {
        RepositoryError::InvalidInput("Context Dataset Artifact size exceeds bigint".to_owned())
    })?;
    let reservation_amount = i64::try_from(stage.maximum_bytes).map_err(|_| {
        RepositoryError::CorruptRow(
            "Context Dataset Artifact reservation exceeds bigint".to_owned(),
        )
    })?;
    if actual_amount > reservation_amount {
        return Err(RepositoryError::Conflict(
            "Context Dataset Artifact quota amount",
        ));
    }
    let account = sqlx::query(
        r#"
        SELECT scope_kind, scope_id, work_class, metric, reserved_value, version
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND quota_account_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(stage.quota_account_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "Context Dataset Artifact quota account",
    ))?;
    if account.try_get::<String, _>("scope_kind")? != "tenant"
        || account.try_get::<String, _>("scope_id")? != tenant_id.to_string()
        || account.try_get::<String, _>("work_class")? != "artifact"
        || account.try_get::<String, _>("metric")? != "artifact.staging_bytes"
        || account.try_get::<i64, _>("reserved_value")? < reservation_amount
    {
        return Err(RepositoryError::Conflict(
            "Context Dataset Artifact quota authority",
        ));
    }
    let reserved: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT reserved_amount FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND quota_entry_id = $2 AND quota_account_id = $3
          AND correlation_id = $4 AND entry_kind = 'reserve'
        FOR SHARE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(allocation.quota_entry_id.to_string())
    .bind(stage.quota_account_id.to_string())
    .bind(allocation.artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if reserved != Some(reservation_amount) {
        return Err(RepositoryError::Conflict(
            "Context Dataset Artifact quota reservation",
        ));
    }
    let account_version: i64 = account.try_get("version")?;
    let next_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.quota_accounts
        SET reserved_value = reserved_value - $4, version = version + 1, updated_at = $5
        WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
          AND reserved_value >= $4
        RETURNING version
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(stage.quota_account_id.to_string())
    .bind(account_version)
    .bind(reservation_amount)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "Context Dataset Artifact quota account",
    ))?;
    let settlement_id =
        ResourceId::from_uuid_v7(ResourceKind::QuotaLedgerEntry, uuid::Uuid::now_v7())
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_ledger (
            tenant_id, quota_entry_id, quota_account_id, correlation_id,
            entry_kind, reserved_amount, used_amount, account_version, request_digest
        ) VALUES ($1, $2, $3, $4, 'settle', $5, 0, $6, $7)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(settlement_id.to_string())
    .bind(stage.quota_account_id.to_string())
    .bind(allocation.artifact_id.to_string())
    .bind(reservation_amount)
    .bind(next_version)
    .bind(request_digest)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn lock_dataset_build_target(
    transaction: &mut Transaction<'_, Postgres>,
    command: &RequestContextDatasetBuild,
) -> Result<(Option<u64>, Option<ResourceId>), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT version, resource_kind, active_version_id
        FROM insight_platform.resources
        WHERE tenant_id = $1 AND resource_id = $2
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.dataset_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Ok((None, None));
    };
    if row.try_get::<String, _>("resource_kind")? != "context_dataset" {
        return Err(RepositoryError::NotFound("Context Dataset"));
    }
    let version = u64::try_from(row.try_get::<i64, _>("version")?)
        .map_err(|_| RepositoryError::CorruptRow("Dataset version is invalid".to_owned()))?;
    let active: String = row
        .try_get::<Option<String>, _>("active_version_id")?
        .ok_or_else(|| {
            RepositoryError::CorruptRow("Dataset has no active generation".to_owned())
        })?;
    let active_id: ResourceId = active
        .parse()
        .map_err(|_| RepositoryError::CorruptRow("Dataset generation ID is invalid".to_owned()))?;
    if active_id.kind() != ResourceKind::DatasetGeneration {
        return Err(RepositoryError::CorruptRow(
            "Dataset active head has the wrong kind".to_owned(),
        ));
    }
    let version_row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.resource_versions
        WHERE tenant_id = $1 AND resource_id = $2 AND resource_version_id = $3
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.dataset_id.to_string())
    .bind(active_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    let typed = payload_from_row(
        &version_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let published: PublishedVersionPayload =
        decode_typed_payload(&typed, "Context Dataset generation")?;
    let ResourceDocument::ContextDataset(dataset) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Dataset generation contains the wrong document".to_owned(),
        ));
    };
    if dataset.generation.context_deployment != command.context_deployment {
        return Err(RepositoryError::Conflict(
            "Context Dataset Deployment ownership",
        ));
    }
    Ok((Some(version), Some(active_id)))
}

async fn claim_dataset_build_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &RequestContextDatasetBuild,
) -> Result<Option<ResourceId>, RepositoryError> {
    let scope_id = command.context_deployment.deployment_id.to_string();
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "operation": "context.dataset.build",
            "principal_id": command.audit.principal_id,
            "scope_id": scope_id,
            "scope_kind": "context_deployment",
        }),
        65_536,
    )?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'command', 'context_deployment', $3, $4,
                  'context.dataset.build', $5, $6, 'processing', $7, $8, $9, $10)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(command.audit.receipt_id.to_string())
    .bind(&scope_id)
    .bind(command.audit.principal_id.to_string())
    .bind(command.audit.idempotency_key_digest.to_string())
    .bind(command.audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(None);
    }
    let row = sqlx::query(
        r#"
        SELECT request_digest, state, response_reference_id
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = 'context_deployment' AND scope_id = $2
          AND dedupe_owner_id = $3 AND operation = 'context.dataset.build'
          AND idempotency_key_digest = $4
        FOR UPDATE
        "#,
    )
    .bind(command.audit.tenant_id.to_string())
    .bind(&scope_id)
    .bind(command.audit.principal_id.to_string())
    .bind(command.audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != command.audit.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("Context Dataset build Receipt"));
    }
    let job_id: ResourceId = row
        .try_get::<Option<String>, _>("response_reference_id")?
        .ok_or_else(|| {
            RepositoryError::CorruptRow("Dataset build Receipt has no result".to_owned())
        })?
        .parse()
        .map_err(|_| {
            RepositoryError::CorruptRow("Dataset build Receipt result is invalid".to_owned())
        })?;
    if job_id.kind() != ResourceKind::Job {
        return Err(RepositoryError::CorruptRow(
            "Dataset build Receipt result has the wrong kind".to_owned(),
        ));
    }
    Ok(Some(job_id))
}

async fn load_dataset_build_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
) -> Result<JobRecord, RepositoryError> {
    let row =
        sqlx::query("SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2")
            .bind(tenant_id.to_string())
            .bind(job_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::NotFound("Context Dataset build Job"))?;
    let job = job_from_row(row)?;
    if job.work_class != "context" || job.owner_kind != "context_dataset" {
        return Err(RepositoryError::CorruptRow(
            "Dataset build Receipt references the wrong Job".to_owned(),
        ));
    }
    let owner: ResourceId = job
        .owner_id
        .parse()
        .map_err(|_| RepositoryError::CorruptRow("Dataset Job owner is invalid".to_owned()))?;
    let payload: ContextDatasetBuildJobPayload =
        decode_versioned_payload(&job.payload, "Context Dataset build Job")?;
    payload.validate_for_owner(&owner)?;
    Ok(job)
}
