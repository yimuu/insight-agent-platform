//! Run commands reuse the caller-owned transaction and the shared lock/Receipt helpers.
use super::*;

impl PgRunTransaction {
    pub async fn admit_run(
        &mut self,
        command: AdmitRun,
    ) -> Result<CommandOutcome<RunRecord>, RepositoryError> {
        command.validate_shape()?;
        let mut transaction = self.transaction.begin().await?;
        let principal =
            require_tenant_permission(&mut transaction, &command.audit, Permission::AgentRun)
                .await?;
        if let Some(record) = read_run_admission_receipt(
            &mut transaction,
            &command.audit,
            &command.admission_scope_id,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        // A concurrent command may have won after the empty locked read. Preserve its result.
        if let Some(record) = claim_run_admission_receipt(
            &mut transaction,
            &command.audit,
            &command.admission_scope_id,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        if command.admission_scope_id.kind() == ResourceKind::Agent {
            let resolved = &command.bindings.agent;
            // The resource row is the one active-head authority. Activation uses the same row lock.
            let row=sqlx::query("SELECT resource.active_deployment_id,deployment.bindings_digest FROM insight_platform.resources resource LEFT JOIN insight_platform.deployments deployment ON deployment.tenant_id=resource.tenant_id AND deployment.deployment_id=resource.active_deployment_id AND deployment.resource_id=resource.resource_id WHERE resource.tenant_id=$1 AND resource.resource_id=$2 AND resource.resource_kind='agent' AND resource.lifecycle_state='active' AND resource.gate_state='enabled' FOR SHARE OF resource")
                .bind(command.audit.tenant_id.to_string()).bind(command.admission_scope_id.to_string()).fetch_optional(&mut *transaction).await?.ok_or(RepositoryError::Conflict("active Agent deployment"))?;
            if row
                .try_get::<Option<String>, _>("active_deployment_id")?
                .as_deref()
                != Some(resolved.deployment_id.to_string().as_str())
                || row
                    .try_get::<Option<String>, _>("bindings_digest")?
                    .as_deref()
                    != Some(resolved.deployment_digest.as_str())
                || command
                    .expected_agent_deployment
                    .as_ref()
                    .is_some_and(|expected| expected != resolved)
            {
                return Err(RepositoryError::Conflict("active Agent deployment"));
            }
        }
        crate::outbox_repository::require_new_work_capacity(
            &mut transaction,
            self.outbox_admission_backlog,
        )
        .await?;
        if command.bindings.principal != principal {
            return Err(RepositoryError::InvalidInput(
                "run principal snapshot does not match the authorized binding".to_owned(),
            ));
        }
        validate_run_bindings_exist(
            &mut transaction,
            &command.audit.tenant_id,
            &command.bindings,
        )
        .await?;
        if let ValueRef::Artifact { artifact } = &command.input.value {
            require_ready_run_artifact(&mut transaction, &command.audit.tenant_id, artifact)
                .await?;
        }

        let execution = crate::execution_requirements::published_program_requirement(
            &mut transaction,
            &command.audit.tenant_id,
            &command.bindings,
        )
        .await?;
        let bindings = TypedPayload::from_versioned(1, &command.bindings, 1_048_576)?;
        let current_snapshot = command.initial_current_snapshot();
        current_snapshot.validate(&command.run_id)?;
        let current = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.runs (
                tenant_id, run_id, root_run_id, parent_run_id, parent_node_id,
                agent_deployment_id, principal_id, trace_id, state,
                bindings_schema_version, bindings, bindings_digest,
                current_schema_version, current_payload, current_payload_digest,
                depth, deadline, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, $2, NULL, NULL, $3, $4, $5, 'queued',
                $6, $7, $8, $9, $10, $11, 0, $12, $13, $14, $15
            )
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.run_id.to_string())
        .bind(command.agent_deployment_id.to_string())
        .bind(command.audit.principal_id.to_string())
        .bind(command.audit.trace.trace_id.to_string())
        .bind(bindings.schema_version)
        .bind(&bindings.value)
        .bind(command.bindings.canonical_digest.to_string())
        .bind(current.schema_version)
        .bind(&current.value)
        .bind(&current.digest)
        .bind(command.deadline)
        .bind(execution.version).bind(&execution.value).bind(&execution.digest)
        .execute(&mut *transaction)
        .await?;

        let scope_payload = TypedPayload::new(
            1,
            &StoredRootScopePayload {
                root_run_id: command.run_id.clone(),
                environment: root_scope_environment(&command.input, self.scope_environment_limits)?,
            },
        )?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.run_nodes (
                tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
                plan_node_key, activation_ordinal, related_run_id, logical_key,
                node_kind, state, payload_schema_version, payload, payload_digest, deadline
            ) VALUES (
                $1, $2, $3, NULL, 'scope_instance', $2,
                NULL, NULL, NULL, 'scope:root', 'root', 'open', $4, $5, $6, $7
            )
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.root_scope_id.to_string())
        .bind(command.run_id.to_string())
        .bind(scope_payload.schema_version)
        .bind(&scope_payload.value)
        .bind(&scope_payload.digest)
        .bind(command.deadline)
        .execute(&mut *transaction)
        .await?;

        let node_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "plan_node_key": command.entry_plan_node_key,
                "required_control_tokens": ["root"],
            }),
        )?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.run_nodes (
                tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
                plan_node_key, activation_ordinal, related_run_id, logical_key,
                node_kind, state, enqueue_round,
                payload_schema_version, payload, payload_digest, deadline
            ) VALUES (
                $1, $2, $3, NULL, 'node_execution', $4,
                $5, 1, NULL, $6, $7, 'ready', 0, $8, $9, $10, $11
            )
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.entry_node_execution_id.to_string())
        .bind(command.run_id.to_string())
        .bind(command.root_scope_id.to_string())
        .bind(command.entry_plan_node_key.as_str())
        .bind(format!("entry:{}:1", command.entry_plan_node_key.as_str()))
        .bind(command.entry_node_kind.as_str())
        .bind(node_payload.schema_version)
        .bind(&node_payload.value)
        .bind(&node_payload.digest)
        .bind(command.deadline)
        .execute(&mut *transaction)
        .await?;

        let (inline_value, artifact_id) = match &command.input.value {
            ValueRef::Inline { value } => (Some(value), None),
            ValueRef::Artifact { artifact } => (None, Some(artifact.artifact_id().to_string())),
        };
        sqlx::query(
            r#"
            INSERT INTO insight_platform.run_values (
                tenant_id, value_id, run_id, node_id, value_kind, classification,
                schema_digest, content_digest, inline_value, artifact_id
            ) VALUES ($1, $2, $3, NULL, 'run_input', $4, $5, $6, $7, $8)
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.input.value_id.to_string())
        .bind(command.run_id.to_string())
        .bind(command.input.classification.as_str())
        .bind(command.input.schema_digest.to_string())
        .bind(command.input.content_digest.to_string())
        .bind(inline_value)
        .bind(artifact_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE insight_platform.runs SET input_value_id = $3 WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.run_id.to_string())
        .bind(command.input.value_id.to_string())
        .execute(&mut *transaction)
        .await?;

        let job_payload = command.orchestration_job_payload().to_payload()?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, clock_timestamp(), $6, 0, $7, $8, $9, $10, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            )
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.orchestration_job_id.to_string())
        .bind(command.entry_node_execution_id.to_string())
        .bind(command.run_id.to_string())
        .bind(i32::from(command.attempt_limit))
        .bind(command.deadline)
        .bind(command.audit.request_digest.to_string())
        .bind(job_payload.schema_version)
        .bind(&job_payload.value)
        .bind(&job_payload.digest)
        .execute(&mut *transaction)
        .await?;

        let admitted =
            load_run(&mut transaction, &command.audit.tenant_id, &command.run_id).await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "run",
            &admitted.run_id,
            admitted.version,
            "run.admitted",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "agent_deployment_id": admitted.agent_deployment_id,
                    "bindings_digest": admitted.bindings.canonical_digest,
                    "input_value_id": admitted.input_value_id,
                }),
            )?,
        )
        .await?;
        let record = load_run(&mut transaction, &command.audit.tenant_id, &command.run_id).await?;
        terminalize_run_admission_receipt(&mut transaction, &command.audit, &record).await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn set_run_pause(
        &mut self,
        command: SetRunPause,
    ) -> Result<CommandOutcome<RunRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let SetRunPause {
            audit,
            run_id,
            expected_run_version,
            expected_pause_generation,
            requested,
        } = command;
        self.apply_run_control(
            audit,
            run_id,
            expected_run_version,
            "run.pause",
            move |record, _database_now, _principal| {
                decide_pause(
                    &record.current.control,
                    expected_pause_generation,
                    requested,
                )
            },
        )
        .await
    }

    pub async fn request_run_cancel(
        &mut self,
        command: RequestRunCancel,
    ) -> Result<CommandOutcome<RunRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let RequestRunCancel {
            audit,
            run_id,
            expected_run_version,
            expected_cancel_generation,
            reason_code,
        } = command;
        self.apply_run_control(
            audit,
            run_id,
            expected_run_version,
            "run.cancel",
            move |record, database_now, principal| {
                decide_cancel(
                    &record.current.control,
                    expected_cancel_generation,
                    database_now,
                    reason_code,
                    principal.clone(),
                )
            },
        )
        .await
    }

    pub async fn observe_run_timeout(
        &mut self,
        command: ObserveRunTimeout,
    ) -> Result<CommandOutcome<RunRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let ObserveRunTimeout {
            audit,
            run_id,
            expected_run_version,
            expected_timeout_generation,
        } = command;
        self.apply_run_control(
            audit,
            run_id,
            expected_run_version,
            "run.timeout",
            move |record, database_now, _principal| {
                decide_timeout(
                    &record.current.control,
                    expected_timeout_generation,
                    database_now,
                    record.deadline,
                    record.state.clone(),
                    u64::try_from(record.version)
                        .map_err(|_| OrchestratorError::InvalidRunControl)?,
                )
            },
        )
        .await
    }

    async fn apply_run_control<F>(
        &mut self,
        audit: CommandAudit,
        run_id: ResourceId,
        expected_run_version: i64,
        operation: &'static str,
        decide: F,
    ) -> Result<CommandOutcome<RunRecord>, RepositoryError>
    where
        F: FnOnce(
            &RunRecord,
            DateTime<Utc>,
            &PrincipalSnapshot,
        ) -> Result<ControlDecision, OrchestratorError>,
    {
        let mut transaction = self.transaction.begin().await?;
        let principal =
            require_tenant_permission(&mut transaction, &audit, Permission::RuntimeControl).await?;
        if claim_command_receipt(
            &mut transaction,
            &audit,
            "run",
            &run_id.to_string(),
            operation,
        )
        .await?
        {
            let record = load_run(&mut transaction, &audit.tenant_id, &run_id).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_run_for_update(&mut transaction, &audit.tenant_id, &run_id).await?;
        if current.version != expected_run_version || current.terminal_at.is_some() {
            return Err(RepositoryError::Conflict("run"));
        }
        let current_state = current
            .state
            .parse::<RunState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        match operation {
            "run.pause"
                if !matches!(
                    current_state,
                    RunState::Queued | RunState::Running | RunState::Waiting
                ) =>
            {
                return Err(RepositoryError::Conflict("run pause state"));
            }
            "run.cancel"
                if !matches!(
                    current_state,
                    RunState::Queued | RunState::Running | RunState::Waiting | RunState::Cancelling
                ) =>
            {
                return Err(RepositoryError::Conflict("run cancel state"));
            }
            "run.timeout" if !current_state.can_transition_to(RunState::TimedOut) => {
                return Err(RepositoryError::Conflict("run timeout state"));
            }
            _ => {}
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let decision = decide(&current, database_now, &principal)?;
        let (next_control, disposition, event_type, next_state, terminal) = match decision {
            ControlDecision::Unchanged(snapshot) => (
                snapshot,
                "unchanged",
                format!("{operation}_unchanged"),
                current.state.clone(),
                false,
            ),
            ControlDecision::Updated(snapshot) => {
                let (state, terminal, suffix) = match operation {
                    "run.pause" if snapshot.pause_requested => {
                        (current.state.clone(), false, "requested")
                    }
                    "run.pause" => (current.state.clone(), false, "resumed"),
                    "run.cancel" => ("cancelling".to_owned(), false, "requested"),
                    "run.timeout" => (current.state.clone(), false, "observed"),
                    _ => {
                        return Err(RepositoryError::InvalidInput(
                            "unknown run control operation".to_owned(),
                        ))
                    }
                };
                (
                    snapshot,
                    "updated",
                    format!("{operation}_{suffix}"),
                    state,
                    terminal,
                )
            }
        };
        let mut next_current = current.current.clone();
        next_current.control = next_control;
        next_current.validate(&run_id)?;
        let payload = TypedPayload::from_versioned(1, &next_current, 1_048_576)?;
        let changed = disposition == "updated";
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.runs
            SET state = $4, current_schema_version = $5, current_payload = $6,
                current_payload_digest = $7,
                pause_generation = $8, cancel_generation = $9, timeout_generation = $10,
                version = version + CASE WHEN $11 THEN 1 ELSE 0 END,
                terminal_at = CASE WHEN $12 THEN clock_timestamp() ELSE terminal_at END,
                updated_at = CASE WHEN $11 THEN clock_timestamp() ELSE updated_at END
            WHERE tenant_id = $1 AND run_id = $2 AND version = $3 AND terminal_at IS NULL
            RETURNING *
            "#,
        )
        .bind(audit.tenant_id.to_string())
        .bind(run_id.to_string())
        .bind(expected_run_version)
        .bind(next_state)
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(
            i64::try_from(next_current.control.pause_generation).map_err(|_| {
                RepositoryError::InvalidInput("pause generation exceeds bigint".to_owned())
            })?,
        )
        .bind(
            i64::try_from(next_current.control.cancel_generation).map_err(|_| {
                RepositoryError::InvalidInput("cancel generation exceeds bigint".to_owned())
            })?,
        )
        .bind(
            i64::try_from(next_current.control.timeout_generation).map_err(|_| {
                RepositoryError::InvalidInput("timeout generation exceeds bigint".to_owned())
            })?,
        )
        .bind(changed)
        .bind(terminal)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("run"))?;
        let record = run_from_row(row)?;
        append_command_event_version(
            &mut transaction,
            &audit,
            "run",
            &record.run_id,
            changed.then_some(record.version),
            &event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "cancel_generation": record.cancel_generation,
                    "pause_generation": record.pause_generation,
                    "state": record.state,
                    "timeout_generation": record.timeout_generation,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(&mut transaction, &audit, &record.run_id, disposition).await?;
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

impl RunTransaction for PgRunTransaction {
    type Error = RepositoryError;
    type RunRecord = RunRecord;

    async fn admit_run(
        &mut self,
        command: AdmitRun,
    ) -> Result<CommandOutcome<Self::RunRecord>, Self::Error> {
        PgRunTransaction::admit_run(self, command).await
    }

    async fn set_run_pause(
        &mut self,
        command: SetRunPause,
    ) -> Result<CommandOutcome<Self::RunRecord>, Self::Error> {
        PgRunTransaction::set_run_pause(self, command).await
    }

    async fn request_run_cancel(
        &mut self,
        command: RequestRunCancel,
    ) -> Result<CommandOutcome<Self::RunRecord>, Self::Error> {
        PgRunTransaction::request_run_cancel(self, command).await
    }

    async fn observe_run_timeout(
        &mut self,
        command: ObserveRunTimeout,
    ) -> Result<CommandOutcome<Self::RunRecord>, Self::Error> {
        PgRunTransaction::observe_run_timeout(self, command).await
    }

    async fn commit(self) -> Result<(), Self::Error> {
        PgRunTransaction::commit(self).await
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        PgRunTransaction::rollback(self).await
    }
}

impl RunStore for PgRepository {
    type Error = RepositoryError;
    type Transaction<'a> = PgRunTransaction;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        self.begin_run_transaction().await
    }
}
