use crate::repository::{
    decode_deployment_closure, decode_versioned_payload, job_from_row, job_projection,
    load_deployment, payload_from_row, require_ready_run_artifact, require_tenant_permission,
    terminalize_command_receipt, validate_deployment_closure_exists, JobRecord, PgRepository,
    RepositoryError, TypedPayload,
};
use chrono::{DateTime, Utc};
use insight_platform_context::{
    CommitContextDatasetBuild, ContextDatasetBuildJobPayload, ContextDatasetRootPayload,
    RequestContextDatasetBuild,
};
use insight_platform_contracts::{
    canonical_digest, AuthoringPackage, CommandOutcome, ContextDatasetResourceSpec,
    DeploymentClosure, JobState, Permission, PublishedVersionPayload, ResourceDocument, ResourceId,
    ResourceKind, ValidationSummary,
};
use insight_platform_jobs::decide_terminal as decide_job_terminal;
use sqlx::{Postgres, Row, Transaction};

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
        let payload = ContextDatasetBuildJobPayload::from_request(
            &command,
            &closure,
            expected_dataset_version,
            expected_active_generation_id,
        )?;
        let typed = TypedPayload::from_versioned(1, &payload, 1_048_576)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, work_class, owner_kind, owner_id, state,
                attempt_limit, scheduled_at, deadline, priority, request_digest,
                payload_schema_version, payload, payload_digest, created_at, updated_at
            ) VALUES ($1, $2, 'context', 'context_dataset', $3, 'ready',
                      $4, $5, $6, 0, $7, $8, $9, $10, $5, $5)
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
        .execute(&mut *transaction)
        .await?;
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
                event_type, visibility, payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, 'context_dataset', $3, $4,
                      'context.dataset_generation_created', 'internal', $5, $6, $7)
            "#,
        )
        .bind(command.tenant_id.to_string())
        .bind(command.event_id.to_string())
        .bind(command.dataset_id.to_string())
        .bind(revision_no)
        .bind(event_payload.schema_version)
        .bind(&event_payload.value)
        .bind(&event_payload.digest)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO insight_platform.outbox_events (tenant_id, outbox_id, event_id) VALUES ($1, $2, $3)",
        )
        .bind(command.tenant_id.to_string())
        .bind(command.outbox_id.to_string())
        .bind(command.event_id.to_string())
        .execute(&mut *transaction)
        .await?;
        let record = job_from_row(row)?;
        transaction.commit().await?;
        Ok(record)
    }
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
        decode_versioned_payload(&typed, "Context Dataset generation")?;
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
