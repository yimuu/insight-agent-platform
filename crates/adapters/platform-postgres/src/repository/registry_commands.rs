//! Registry commands share the same PostgreSQL transaction authority.
use super::*;

fn resource_version_insert_error(error: sqlx::Error) -> RepositoryError {
    if error.as_database_error().is_some_and(|database| {
        database.code().as_deref() == Some("23505")
            && matches!(
                database.constraint(),
                Some(
                    "resource_versions_pkey"
                        | "resource_versions_resource_id_uq"
                        | "resource_versions_revision_uq"
                )
            )
    }) {
        RepositoryError::Conflict("Resource version identity or revision")
    } else {
        error.into()
    }
}

impl PgRegistryTransaction {
    pub async fn create_resource_draft(
        &mut self,
        command: CreateResourceDraft,
    ) -> Result<CommandOutcome<ResourceRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(
            &mut transaction,
            &command.audit,
            write_permission(command.draft.document.kind()),
        )
        .await?;
        if let Some(resource_id) =
            claim_resource_create_receipt(&mut transaction, &command.audit).await?
        {
            let record =
                load_resource(&mut transaction, &command.audit.tenant_id, &resource_id).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        crate::outbox_repository::require_new_work_capacity(
            &mut transaction,
            self.outbox_admission_backlog,
        )
        .await?;
        require_ready_authoring_artifact(
            &mut transaction,
            &command.audit.tenant_id,
            command.draft.document.authoring_package(),
        )
        .await?;
        let payload = TypedPayload::new(1, &command.draft)?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.resources (
                tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, $3, 'active', 'enabled', $4, $5, $6)
            RETURNING tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                      draft_generation, active_version_id, active_deployment_id, version,
                      payload_schema_version, payload, payload_digest, created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.resource_id.to_string())
        .bind(command.draft.document.kind().as_str())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .fetch_one(&mut *transaction)
        .await?;
        let record = resource_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "resource",
            &record.resource_id,
            record.version,
            "resource.draft_created",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "draft_digest": record.payload.digest,
                    "resource_kind": record.resource_kind,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.resource_id,
            "created",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn update_resource_draft(
        &mut self,
        command: UpdateResourceDraft,
    ) -> Result<CommandOutcome<ResourceRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(
            &mut transaction,
            &command.audit,
            write_permission(command.draft.document.kind()),
        )
        .await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "resource",
            &command.resource_id.to_string(),
            "resource.update_draft",
        )
        .await?
        {
            let result = load_resource_projection_receipt(
                &mut transaction,
                &command.audit,
                &command.resource_id,
                "resource.update_draft",
                "resource update Receipt result",
            )
            .await?;
            let record = result.into_record()?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        crate::outbox_repository::require_new_work_capacity(
            &mut transaction,
            self.outbox_admission_backlog,
        )
        .await?;
        require_ready_authoring_artifact(
            &mut transaction,
            &command.audit.tenant_id,
            command.draft.document.authoring_package(),
        )
        .await?;
        let current = load_resource_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.resource_id,
        )
        .await?;
        if current.version != command.expected_resource_version
            || current.lifecycle_state == "retired"
            || current.resource_kind != command.draft.document.kind().as_str()
        {
            return Err(RepositoryError::Conflict("resource"));
        }
        validate_resource_draft_replacement(
            &decode_resource_draft(&current.payload)?,
            &command.draft,
        )?;
        let payload = TypedPayload::new(1, &command.draft)?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.resources
            SET payload_schema_version = $4, payload = $5, payload_digest = $6,
                draft_generation = draft_generation + 1, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND resource_id = $2 AND version = $3
              AND lifecycle_state <> 'retired' AND resource_kind = $7
            RETURNING tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                      draft_generation, active_version_id, active_deployment_id, version,
                      payload_schema_version, payload, payload_digest, created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.resource_id.to_string())
        .bind(command.expected_resource_version)
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(command.draft.document.kind().as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("resource"))?;
        let record = resource_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "resource",
            &record.resource_id,
            record.version,
            "resource.draft_updated",
            &TypedPayload::new(
                1,
                &serde_json::json!({"draft_digest": record.payload.digest}),
            )?,
        )
        .await?;
        terminalize_resource_projection_receipt(
            &mut transaction,
            &command.audit,
            &record,
            "updated",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn record_resource_validation(
        &mut self,
        command: RecordResourceValidation,
    ) -> Result<CommandOutcome<ResourceRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let locked = load_resource_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.resource_id,
        )
        .await?;
        let kind = locked
            .resource_kind
            .parse::<RegistryResourceKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        require_tenant_permission(&mut transaction, &command.audit, write_permission(kind)).await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "resource",
            &command.resource_id.to_string(),
            "resource.record_validation",
        )
        .await?
        {
            let record = load_resource(
                &mut transaction,
                &command.audit.tenant_id,
                &command.resource_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let mut draft = decode_resource_draft(&locked.payload)?;
        let document_digest = draft
            .document_digest()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if locked.version != command.expected_resource_version
            || document_digest != command.expected_draft_digest
            || locked.lifecycle_state == "retired"
        {
            return Err(RepositoryError::Conflict("resource"));
        }
        draft.validation = Some(command.validation.clone());
        let payload = TypedPayload::new(1, &draft)?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.resources
            SET payload_schema_version = $4, payload = $5, payload_digest = $6,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND resource_id = $2 AND version = $3
            RETURNING tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                      draft_generation, active_version_id, active_deployment_id, version,
                      payload_schema_version, payload, payload_digest, created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.resource_id.to_string())
        .bind(command.expected_resource_version)
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .fetch_one(&mut *transaction)
        .await?;
        let record = resource_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "resource",
            &record.resource_id,
            record.version,
            "resource.validation_recorded",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "validated_draft_digest": command.validation.validated_draft_digest,
                    "validator_digest": command.validation.validator_digest,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.resource_id,
            "validated",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn request_resource_validation(
        &mut self,
        command: RequestResourceValidation,
    ) -> Result<CommandOutcome<RegistryValidationAccepted>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let locked = load_resource_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.resource_id,
        )
        .await?;
        let kind = locked
            .resource_kind
            .parse::<RegistryResourceKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        require_tenant_permission(&mut transaction, &command.audit, write_permission(kind)).await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "resource",
            &command.resource_id.to_string(),
            "resource.validate",
        )
        .await?
        {
            let accepted = load_registry_validation_receipt(
                &mut transaction,
                &command.audit,
                &command.resource_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(accepted));
        }
        crate::outbox_repository::require_new_work_capacity(
            &mut transaction,
            self.outbox_admission_backlog,
        )
        .await?;
        let draft = decode_resource_draft(&locked.payload)?;
        let document_digest = draft
            .document_digest()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if locked.version != command.expected_resource_version
            || locked.lifecycle_state == "retired"
        {
            return Err(RepositoryError::Conflict("resource"));
        }
        if kind == RegistryResourceKind::Agent
            && command.compilation_input.as_ref().is_none_or(|input| {
                &input.source_bundle_digest
                    != draft.document.authoring_package().artifact.content_digest()
            })
        {
            return Err(RepositoryError::InvalidInput(
                "Agent authoring recompile required".to_owned(),
            ));
        }
        let validation_job =
            RegistryValidationJobPayload::from_request(&command, kind, document_digest)?;
        let payload = TypedPayload::from_versioned(2, &validation_job, 262_144)?;
        let requirement = crate::execution_requirements::StoredExecutionRequirement::new(
            &validation_job.execution_requirement,
        )?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'registry_validation', 'registry_validation', 'job', $3, $4, 'ready', $5, GREATEST($6, clock_timestamp()), $7, 0, $8, $9, $10, $11, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), $12, $13, $14
            )
            RETURNING *
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.job_id.to_string())
        .bind(command.job_id.to_string())
        .bind(command.audit.trace.trace_id.to_string())
        .bind(command.attempt_limit)
        .bind(command.scheduled_at)
        .bind(command.deadline)
        .bind(command.audit.request_digest.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(requirement.version)
        .bind(&requirement.value)
        .bind(&requirement.digest)
        .fetch_one(&mut *transaction)
        .await?;
        let job = job_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "job",
            &command.job_id.to_string(),
            1,
            "resource.validation_requested",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "draft_digest": validation_job.draft_digest,
                    "job_id": command.job_id,
                    "resource_id": command.resource_id,
                }),
            )?,
        )
        .await?;
        let accepted = RegistryValidationAccepted { job };
        terminalize_registry_validation_receipt(
            &mut transaction,
            &command.audit,
            &command.resource_id,
            &accepted,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(accepted))
    }

    pub async fn publish_resource_versions(
        &mut self,
        command: PublishResourceVersions,
    ) -> Result<CommandOutcome<PublishedResource>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let locked = load_resource_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.resource_id,
        )
        .await?;
        let kind = locked
            .resource_kind
            .parse::<RegistryResourceKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        require_tenant_permission(&mut transaction, &command.audit, publish_permission(kind))
            .await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "resource",
            &command.resource_id.to_string(),
            "resource.publish",
        )
        .await?
        {
            let published = load_resource_publish_receipt(
                &mut transaction,
                &command.audit,
                &command.resource_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(published));
        }
        crate::outbox_repository::require_new_work_capacity(
            &mut transaction,
            self.outbox_admission_backlog,
        )
        .await?;
        let draft = decode_resource_draft(&locked.payload)?;
        let document_digest = draft
            .document_digest()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if locked.version != command.expected_resource_version
            || document_digest != command.expected_draft_digest
            || locked.lifecycle_state != "active"
        {
            return Err(RepositoryError::Conflict("resource"));
        }
        let Some(validation) = draft.validation.as_ref() else {
            return Err(RepositoryError::InvalidInput(
                "resource draft has no validation summary".to_owned(),
            ));
        };
        if validation.validated_draft_digest != command.expected_draft_digest {
            return Err(RepositoryError::Conflict("resource validation"));
        }
        require_ready_authoring_artifact(
            &mut transaction,
            &command.audit.tenant_id,
            draft.document.authoring_package(),
        )
        .await?;
        require_ready_typed_plan_artifact(
            &mut transaction,
            &command.audit.tenant_id,
            &draft.document,
        )
        .await?;
        require_ready_sandbox_runtime_bundle(
            &mut transaction,
            &command.audit.tenant_id,
            &draft.document,
        )
        .await?;
        let document_refs = draft.document.exact_version_refs();
        validate_exact_version_refs_exist(
            &mut transaction,
            &command.audit.tenant_id,
            &document_refs,
        )
        .await?;
        validate_publish_batch(kind, &command.versions)?;
        let mut records = Vec::with_capacity(command.versions.len());
        for version in &command.versions {
            version
                .payload
                .validate_for(kind, &version.resource_version_id)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            require_version_content_contract(version)?;
            let published_document = serde_json::to_value(&version.payload.document)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            let published_document_digest = canonical_digest(&published_document)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            if published_document_digest != command.expected_draft_digest.as_str() {
                return Err(RepositoryError::InvalidInput(
                    "published document does not match the validated draft".to_owned(),
                ));
            }
            if version.payload.validation != *validation {
                return Err(RepositoryError::InvalidInput(
                    "published validation does not match the validated draft".to_owned(),
                ));
            }
            require_ready_sandbox_runtime_bundle(
                &mut transaction,
                &command.audit.tenant_id,
                &version.payload.document,
            )
            .await?;
            let payload = TypedPayload::new(1, &version.payload)?;
            let artifact_id = version.artifact_id.as_ref().map(ToString::to_string);
            let row = sqlx::query(
                r#"
                INSERT INTO insight_platform.resource_versions (
                    tenant_id, resource_version_id, resource_id, resource_version_kind,
                    revision_no, content_digest, artifact_id, payload_schema_version,
                    payload, payload_digest, created_by
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING tenant_id, resource_version_id, resource_id, resource_version_kind,
                          revision_no, content_digest, artifact_id, payload_schema_version,
                          payload, payload_digest, created_by, created_at
                "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(version.resource_version_id.to_string())
            .bind(command.resource_id.to_string())
            .bind(version.resource_version_id.kind().descriptor().name)
            .bind(version.revision_no)
            .bind(version.content_digest.to_string())
            .bind(artifact_id)
            .bind(payload.schema_version)
            .bind(&payload.value)
            .bind(&payload.digest)
            .bind(command.audit.principal_id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(resource_version_insert_error)?;
            records.push(resource_version_from_row(row)?);
        }
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.resources
            SET version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND resource_id = $2 AND version = $3
              AND lifecycle_state = 'active'
            RETURNING tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                      draft_generation, active_version_id, active_deployment_id, version,
                      payload_schema_version, payload, payload_digest, created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.resource_id.to_string())
        .bind(command.expected_resource_version)
        .fetch_one(&mut *transaction)
        .await?;
        let resource = resource_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "resource",
            &resource.resource_id,
            resource.version,
            "resource.published",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "draft_digest": command.expected_draft_digest,
                    "resource_version_ids": records.iter().map(|record| &record.resource_version_id).collect::<Vec<_>>(),
                }),
            )?,
        )
        .await?;
        let published = PublishedResource {
            resource,
            versions: records,
        };
        terminalize_resource_publish_receipt(
            &mut transaction,
            &command.audit,
            &published,
            &draft.display_name,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(published))
    }

    pub async fn create_deployment(
        &mut self,
        command: CreateDeployment,
    ) -> Result<CommandOutcome<DeploymentRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let locked = load_resource_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.resource_id,
        )
        .await?;
        let kind = locked
            .resource_kind
            .parse::<RegistryResourceKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        require_tenant_permission(&mut transaction, &command.audit, deploy_permission(kind))
            .await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "resource",
            &command.resource_id.to_string(),
            "resource.deploy",
        )
        .await?
        {
            let deployment_id = load_command_receipt_response_reference(
                &mut transaction,
                &command.audit,
                "resource",
                &command.resource_id.to_string(),
                "resource.deploy",
            )
            .await?
            .parse::<ResourceId>()
            .map_err(|_| {
                RepositoryError::CorruptRow(
                    "deployment Receipt response reference is invalid".to_owned(),
                )
            })?;
            if deployment_id.kind() != command.deployment_id.kind() {
                return Err(RepositoryError::CorruptRow(
                    "deployment Receipt response kind is invalid".to_owned(),
                ));
            }
            let record =
                load_deployment(&mut transaction, &command.audit.tenant_id, &deployment_id).await?;
            if record.resource_id != command.resource_id.to_string() {
                return Err(RepositoryError::CorruptRow(
                    "deployment Receipt response has the wrong parent".to_owned(),
                ));
            }
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        crate::outbox_repository::require_new_work_capacity(
            &mut transaction,
            self.outbox_admission_backlog,
        )
        .await?;
        if locked.version != command.expected_resource_version || locked.lifecycle_state != "active"
        {
            return Err(RepositoryError::Conflict("resource"));
        }
        if command.closure.resource_kind() != kind
            || kind.deployment_kind() != Some(command.deployment_id.kind())
            || !kind.allows_version_kind(command.resource_version_id.kind())
        {
            return Err(RepositoryError::InvalidInput(
                "deployment kind does not match its resource".to_owned(),
            ));
        }
        if let DeploymentClosure::Agent(closure) = &command.closure {
            validate_agent_deployment_publish_pair(
                &mut transaction,
                &command.audit.tenant_id,
                &command.resource_id,
                closure,
            )
            .await?;
            for slot in &closure.slots {
                if let FrozenSlotTarget::Context { binding } = &slot.target {
                    if binding.owner_agent_deployment_id != command.deployment_id {
                        return Err(RepositoryError::InvalidInput(
                            "Context binding owner differs from Agent Deployment".to_owned(),
                        ));
                    }
                }
            }
        }
        validate_deployment_closure_exists(
            &mut transaction,
            &command.audit.tenant_id,
            &command.closure,
        )
        .await?;
        validate_exact_secret_bindings_at_creation(
            &mut transaction,
            &command.audit.tenant_id,
            command.closure.secret_bindings(),
        )
        .await?;
        let bindings = TypedPayload::new(1, &command.closure)?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.deployments (
                tenant_id, deployment_id, resource_id, resource_version_id, environment,
                bindings_digest, payload_schema_version, bindings, created_by
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING tenant_id, deployment_id, resource_id, resource_version_id, environment,
                      bindings_digest, payload_schema_version, bindings, created_by, created_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.deployment_id.to_string())
        .bind(command.resource_id.to_string())
        .bind(command.resource_version_id.to_string())
        .bind(&command.environment)
        .bind(&bindings.digest)
        .bind(bindings.schema_version)
        .bind(&bindings.value)
        .bind(command.audit.principal_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        let record = deployment_from_row(row)?;
        let new_version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.resources
            SET version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND resource_id = $2 AND version = $3
            RETURNING version
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.resource_id.to_string())
        .bind(command.expected_resource_version)
        .fetch_one(&mut *transaction)
        .await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "resource",
            &command.resource_id.to_string(),
            new_version,
            "resource.deployed",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "deployment_digest": record.bindings.digest,
                    "deployment_id": record.deployment_id,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.deployment_id,
            "deployed",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn activate_resource(
        &mut self,
        command: ActivateResource,
    ) -> Result<CommandOutcome<ResourceRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let locked = load_resource_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.resource_id,
        )
        .await?;
        let kind = locked
            .resource_kind
            .parse::<RegistryResourceKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        require_tenant_permission(&mut transaction, &command.audit, activate_permission(kind))
            .await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "resource",
            &command.resource_id.to_string(),
            "resource.activate",
        )
        .await?
        {
            let result = load_resource_projection_receipt(
                &mut transaction,
                &command.audit,
                &command.resource_id,
                "resource.activate",
                "resource activation Receipt result",
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(result.into_record()?));
        }
        let activates_deployment = matches!(&command.target, ActiveTarget::Deployment { .. });
        if locked.version != command.expected_resource_version
            || locked.lifecycle_state != "active"
            || (locked.gate_state != AdministrativeGate::Enabled.as_str() && !activates_deployment)
        {
            return Err(RepositoryError::Conflict("resource"));
        }
        let (version_id, deployment_id) = resolve_activation_target(
            &mut transaction,
            &command.audit.tenant_id,
            &command.resource_id,
            kind,
            &command.target,
        )
        .await?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.resources
            SET active_version_id = $4, active_deployment_id = $5,
                gate_state = $6,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND resource_id = $2 AND version = $3
            RETURNING tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                      draft_generation, active_version_id, active_deployment_id, version,
                      payload_schema_version, payload, payload_digest, created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.resource_id.to_string())
        .bind(command.expected_resource_version)
        .bind(version_id)
        .bind(deployment_id)
        .bind(if activates_deployment {
            AdministrativeGate::Enabled.as_str()
        } else {
            locked.gate_state.as_str()
        })
        .fetch_one(&mut *transaction)
        .await?;
        let record = resource_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "resource",
            &record.resource_id,
            record.version,
            "resource.activated",
            &TypedPayload::new(1, &command.target)?,
        )
        .await?;
        terminalize_resource_projection_receipt(
            &mut transaction,
            &command.audit,
            &record,
            "activated",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn suspend_resource_deployment(
        &mut self,
        command: SuspendResourceDeployment,
    ) -> Result<CommandOutcome<ResourceRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let locked = load_resource_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.resource_id,
        )
        .await?;
        let kind = locked
            .resource_kind
            .parse::<RegistryResourceKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        require_tenant_permission(&mut transaction, &command.audit, activate_permission(kind))
            .await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "resource",
            &command.resource_id.to_string(),
            "resource.suspend_deployment",
        )
        .await?
        {
            let result = load_resource_projection_receipt(
                &mut transaction,
                &command.audit,
                &command.resource_id,
                "resource.suspend_deployment",
                "resource deployment suspension Receipt result",
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(result.into_record()?));
        }
        if kind.deployment_kind() != Some(command.deployment_id.kind()) {
            return Err(RepositoryError::InvalidInput(
                "suspended Deployment kind does not match its Resource".to_owned(),
            ));
        }
        let deployment_id = command.deployment_id.to_string();
        if locked.version != command.expected_resource_version
            || locked.lifecycle_state != EntityLifecycle::Active.as_str()
            || locked.gate_state != AdministrativeGate::Enabled.as_str()
            || locked.active_deployment_id.as_deref() != Some(deployment_id.as_str())
        {
            return Err(RepositoryError::Conflict("resource active deployment"));
        }
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.resources
            SET gate_state = 'suspended', version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND resource_id = $2 AND version = $3
            RETURNING tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                      draft_generation, active_version_id, active_deployment_id, version,
                      payload_schema_version, payload, payload_digest, created_at, updated_at
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.resource_id.to_string())
        .bind(command.expected_resource_version)
        .fetch_one(&mut *transaction)
        .await?;
        let record = resource_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "resource",
            &record.resource_id,
            record.version,
            "resource.deployment_suspended",
            &TypedPayload::new(
                1,
                &serde_json::json!({"deployment_id": command.deployment_id}),
            )?,
        )
        .await?;
        terminalize_resource_projection_receipt(
            &mut transaction,
            &command.audit,
            &record,
            "suspended",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn transition_resource_lifecycle(
        &mut self,
        command: TransitionResourceLifecycle,
    ) -> Result<CommandOutcome<ResourceRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        self.transition_resource_control(
            command.audit,
            command.resource_id,
            command.expected_resource_version,
            Some(command.target),
            None,
            "resource.lifecycle_transitioned",
        )
        .await
    }

    pub async fn set_resource_gate(
        &mut self,
        command: SetResourceGate,
    ) -> Result<CommandOutcome<ResourceRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        self.transition_resource_control(
            command.audit,
            command.resource_id,
            command.expected_resource_version,
            None,
            Some(command.target),
            "resource.gate_changed",
        )
        .await
    }

    async fn transition_resource_control(
        &mut self,
        audit: CommandAudit,
        resource_id: ResourceId,
        expected_resource_version: i64,
        lifecycle: Option<EntityLifecycle>,
        gate: Option<AdministrativeGate>,
        event_type: &'static str,
    ) -> Result<CommandOutcome<ResourceRecord>, RepositoryError> {
        let operation = if lifecycle.is_some() {
            "resource.transition_lifecycle"
        } else {
            "resource.set_gate"
        };
        let mut transaction = self.transaction.begin().await?;
        let locked =
            load_resource_for_update(&mut transaction, &audit.tenant_id, &resource_id).await?;
        let kind = locked
            .resource_kind
            .parse::<RegistryResourceKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        require_tenant_permission(&mut transaction, &audit, activate_permission(kind)).await?;
        if claim_command_receipt(
            &mut transaction,
            &audit,
            "resource",
            &resource_id.to_string(),
            operation,
        )
        .await?
        {
            let record = load_resource(&mut transaction, &audit.tenant_id, &resource_id).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        if locked.version != expected_resource_version {
            return Err(RepositoryError::Conflict("resource"));
        }
        let current_lifecycle = locked
            .lifecycle_state
            .parse::<EntityLifecycle>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let current_gate = locked
            .gate_state
            .parse::<AdministrativeGate>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let next_lifecycle = lifecycle.unwrap_or(current_lifecycle);
        let next_gate = gate.unwrap_or(current_gate);
        if lifecycle.is_some() && !current_lifecycle.can_transition_to(next_lifecycle) {
            return Err(RepositoryError::InvalidInput(
                "resource lifecycle transition is not allowed".to_owned(),
            ));
        }
        if gate.is_some() && !current_gate.can_transition_to(next_gate) {
            return Err(RepositoryError::InvalidInput(
                "resource gate transition is not allowed".to_owned(),
            ));
        }
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.resources
            SET lifecycle_state = $4, gate_state = $5,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND resource_id = $2 AND version = $3
            RETURNING tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                      draft_generation, active_version_id, active_deployment_id, version,
                      payload_schema_version, payload, payload_digest, created_at, updated_at
            "#,
        )
        .bind(audit.tenant_id.to_string())
        .bind(resource_id.to_string())
        .bind(expected_resource_version)
        .bind(next_lifecycle.as_str())
        .bind(next_gate.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        let record = resource_from_row(row)?;
        append_command_event(
            &mut transaction,
            &audit,
            "resource",
            &record.resource_id,
            record.version,
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "gate": next_gate,
                    "lifecycle": next_lifecycle,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(&mut transaction, &audit, &record.resource_id, "updated")
            .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn commit(self) -> Result<(), RepositoryError> {
        self.transaction.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), RepositoryError> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

impl RegistryTransaction for PgRegistryTransaction {
    type Error = RepositoryError;
    type ResourceRecord = ResourceRecord;
    type PublishedResource = PublishedResource;
    type DeploymentRecord = DeploymentRecord;
    type ValidationJob = RegistryValidationAccepted;

    async fn create_resource_draft(
        &mut self,
        command: CreateResourceDraft,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error> {
        PgRegistryTransaction::create_resource_draft(self, command).await
    }

    async fn update_resource_draft(
        &mut self,
        command: UpdateResourceDraft,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error> {
        PgRegistryTransaction::update_resource_draft(self, command).await
    }

    async fn request_resource_validation(
        &mut self,
        command: RequestResourceValidation,
    ) -> Result<CommandOutcome<Self::ValidationJob>, Self::Error> {
        PgRegistryTransaction::request_resource_validation(self, command).await
    }

    async fn record_resource_validation(
        &mut self,
        command: RecordResourceValidation,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error> {
        PgRegistryTransaction::record_resource_validation(self, command).await
    }

    async fn publish_resource_versions(
        &mut self,
        command: PublishResourceVersions,
    ) -> Result<CommandOutcome<Self::PublishedResource>, Self::Error> {
        PgRegistryTransaction::publish_resource_versions(self, command).await
    }

    async fn create_deployment(
        &mut self,
        command: CreateDeployment,
    ) -> Result<CommandOutcome<Self::DeploymentRecord>, Self::Error> {
        PgRegistryTransaction::create_deployment(self, command).await
    }

    async fn activate_resource(
        &mut self,
        command: ActivateResource,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error> {
        PgRegistryTransaction::activate_resource(self, command).await
    }

    async fn suspend_resource_deployment(
        &mut self,
        command: SuspendResourceDeployment,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error> {
        PgRegistryTransaction::suspend_resource_deployment(self, command).await
    }

    async fn transition_resource_lifecycle(
        &mut self,
        command: TransitionResourceLifecycle,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error> {
        PgRegistryTransaction::transition_resource_lifecycle(self, command).await
    }

    async fn set_resource_gate(
        &mut self,
        command: SetResourceGate,
    ) -> Result<CommandOutcome<Self::ResourceRecord>, Self::Error> {
        PgRegistryTransaction::set_resource_gate(self, command).await
    }

    async fn commit(self) -> Result<(), Self::Error> {
        PgRegistryTransaction::commit(self).await
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        PgRegistryTransaction::rollback(self).await
    }
}

impl RegistryStore for PgRepository {
    type Error = RepositoryError;
    type Transaction<'a> = PgRegistryTransaction;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        self.begin_registry_transaction().await
    }
}
