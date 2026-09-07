//! PostgreSQL task commands. Shared locks and atomicity remain in this adapter.
use super::*;

impl PgSchedulerTransaction {
    pub async fn defer_orchestration_to_task(
        &mut self,
        command: DeferOrchestrationToTask,
    ) -> Result<CommandOutcome<DeferredOrchestrationTask>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let plan_digest = command.plan.canonical_digest(self.plan_limits)?;
        let mut transaction = self.transaction.begin().await?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "task_id": command.task_id,
                "task_kind": command.definition.task_kind().as_str(),
                "task_deadline": command.task_deadline,
            }),
            65_536,
        )?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            "orchestration.task.defer",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let deferred = load_deferred_orchestration_task(
                &mut transaction,
                &command.fence.tenant_id,
                &command.fence.job_id,
                &command.task_id.to_string(),
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(deferred));
        }
        let observed = load_job_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        require_orchestration_job(&observed)?;
        let quota_accounts = lock_job_quota_bundle(
            &mut transaction,
            &observed,
            &command.mutations.quota_entry_ids,
        )
        .await?;
        let parents = lock_running_orchestration_job_parents(&mut transaction, &observed).await?;
        require_exact_runtime_plan(&mut transaction, &parents.run, &command.plan, &plan_digest)
            .await?;
        let source_node =
            load_controller_source_node(&mut transaction, &observed, &parents, &command.plan)
                .await?;
        let RuntimeNode::HumanTask {
            definition,
            response,
            timeout_milliseconds,
            ..
        } = command.plan.node(&source_node.plan_node_key)?
        else {
            return Err(RepositoryError::Conflict(
                "orchestration Task exact Plan node",
            ));
        };
        let exact_definition = runtime_human_task_definition(definition);
        if command.definition != exact_definition
            || command.response_schema_digest.as_ref() != Some(response.schema_digest())
        {
            return Err(RepositoryError::Conflict(
                "orchestration Task exact Plan contract",
            ));
        }
        let current = load_job_for_update_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        if current.version != observed.version
            || current.payload.digest != observed.payload.digest
            || current.quota_reservation_id != observed.quota_reservation_id
            || command.task_deadline > current.deadline
            || command.task_deadline > parents.run.deadline
        {
            return Err(RepositoryError::Conflict("orchestration Task deferral"));
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        if command.task_deadline <= database_now {
            return Err(RepositoryError::Conflict("Task deadline"));
        }
        let maximum_deadline = database_now
            + Duration::milliseconds(i64::try_from(*timeout_milliseconds).map_err(|_| {
                RepositoryError::InvalidInput("Task timeout exceeds i64".to_owned())
            })?);
        if command.task_deadline > maximum_deadline {
            return Err(RepositoryError::Conflict("Task exact Plan deadline"));
        }
        let next_job = decide_job_terminal(
            &job_projection(&current)?,
            &domain_job_fence(&command.fence)?,
            database_now,
            JobState::Succeeded,
        )?;
        let payload_version = 2;
        let task_payload = TaskPayload {
            response_schema: Some(
                insight_platform_contracts::ClosedJsonSchema::try_from(
                    command
                        .plan
                        .schema_documents
                        .get(response.schema_digest())
                        .cloned()
                        .ok_or_else(|| {
                            RepositoryError::InvalidInput(
                                "Task response schema is absent from the exact Plan".to_owned(),
                            )
                        })?,
                )
                .map_err(|_| {
                    RepositoryError::InvalidInput(
                        "Task response schema must be an object".to_owned(),
                    )
                })?,
            ),
            eligibility_rule: Some(definition.eligibility_rule().cloned().ok_or_else(|| {
                RepositoryError::InvalidInput(
                    "Task eligibility rule is absent from the exact Plan".to_owned(),
                )
            })?),
            definition: command.definition.clone(),
            created_by: parents.run.bindings.principal.clone(),
            resolution: None,
        };
        task_payload
            .validate_for_version(payload_version)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let task_payload =
            TypedPayload::with_limit(payload_version as i32, &task_payload, 262_144)?;
        settle_locked_job_quota_bundle(
            &mut transaction,
            &current,
            &quota_accounts,
            &command.mutations.quota_entry_ids,
            &command.request_digest,
        )
        .await?;
        let mut deferred = mutate_deferred_orchestration_task(
            &mut transaction,
            &current,
            &next_job,
            &parents,
            &source_node,
            &command,
            &task_payload,
            database_now,
        )
        .await?;
        deferred.settled_quota_account_ids = quota_accounts
            .iter()
            .map(|account| account.quota_account_id.clone())
            .collect();
        append_deferred_orchestration_task_events(
            &mut transaction,
            &deferred,
            &command.mutations,
            &receipt_payload,
        )
        .await?;
        terminalize_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            &command.request_digest,
            "task_pending",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(deferred))
    }

    pub async fn resolve_orchestration_task(
        &mut self,
        command: ResolveOrchestrationTask,
    ) -> Result<CommandOutcome<ResolvedOrchestrationTask>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let task_id = command.task_id.to_string();
        let observed = load_task_by_text(
            &mut transaction,
            &command.audit.tenant_id.to_string(),
            &task_id,
        )
        .await?;
        let principal = require_tenant_permission(
            &mut transaction,
            &command.audit,
            observed.task_kind.responder_permission(),
        )
        .await?;
        if !insight_platform_tasks::is_eligible_responder(&task_projection(&observed)?, &principal)?
        {
            return Err(RepositoryError::PermissionDenied);
        }
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "interaction",
            &task_id,
            "interaction.respond",
        )
        .await?
        {
            let resume_job_id = load_command_receipt_response_reference(
                &mut transaction,
                &command.audit,
                "interaction",
                &task_id,
                "interaction.respond",
            )
            .await?;
            let resolved = load_resolved_orchestration_task(
                &mut transaction,
                &command.audit.tenant_id.to_string(),
                &task_id,
                &resume_job_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(resolved));
        }
        if observed.task_kind == TaskKind::Approval {
            return Err(RepositoryError::Conflict(
                "approval Task requires its Invocation owner adapter",
            ));
        }
        if observed.state != TaskState::Pending || observed.responded_at.is_some() {
            return Err(RepositoryError::Conflict("Task first-winner"));
        }
        let run_id: ResourceId = observed
            .run_id
            .as_deref()
            .ok_or_else(|| RepositoryError::CorruptRow("Task has no Run owner".to_owned()))?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
        let node_id = observed
            .node_id
            .as_deref()
            .ok_or_else(|| RepositoryError::CorruptRow("Task has no Node owner".to_owned()))?;
        if observed.owner_kind != "node_execution" || observed.owner_id != node_id {
            return Err(RepositoryError::CorruptRow(
                "Task owner does not match its Node".to_owned(),
            ));
        }
        let run = load_run_for_update(&mut transaction, &command.audit.tenant_id, &run_id).await?;
        let node = sqlx::query(
            r#"
            SELECT version, state, scope_id, plan_node_key,
                   payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
              AND record_kind = 'node_execution'
            FOR UPDATE
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(node_id)
        .bind(run_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Task owner Node"))?;
        let current =
            load_task_for_update(&mut transaction, &command.audit.tenant_id, &command.task_id)
                .await?;
        if current.state != TaskState::Pending || current.responded_at.is_some() {
            return Err(RepositoryError::Conflict("Task first-winner"));
        }
        if run.terminal_at.is_some()
            || !matches!(run.state.as_str(), "waiting" | "running")
            || run.current.control.cancel_requested_at.is_some()
            || run.current.control.timeout_requested_at.is_some()
            || node.try_get::<String, _>("state")? != "waiting"
        {
            return Err(RepositoryError::Conflict("Task owner wake"));
        }
        if current.version
            != i64::try_from(command.expected_task_version).map_err(|_| {
                RepositoryError::InvalidInput("Task version exceeds bigint".to_owned())
            })?
            || current.generation
                != i64::try_from(command.expected_generation).map_err(|_| {
                    RepositoryError::InvalidInput("Task generation exceeds bigint".to_owned())
                })?
            || current.payload.digest != observed.payload.digest
        {
            return Err(RepositoryError::Conflict("Task first-winner"));
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let projection = task_projection(&current)?;
        if !insight_platform_tasks::is_eligible_responder(&projection, &principal)? {
            return Err(RepositoryError::PermissionDenied);
        }
        if let Some(response) = &command.response {
            if projection.payload_schema_version == 2 {
                if !insight_platform_contracts::permits_content_disclosure(
                    &principal,
                    insight_platform_contracts::ExecutionAuthorizationPurpose::ContentDisclosure,
                ) {
                    return Err(RepositoryError::PermissionDenied);
                }
                let schema = projection.payload.response_schema.as_ref().ok_or_else(|| {
                    RepositoryError::CorruptRow("Task frozen response schema is missing".to_owned())
                })?;
                if response.schema_digest != schema.canonical_digest
                    || !command.validated_input.as_ref().is_some_and(|evidence| {
                        evidence.matches_response(
                            &schema.canonical_digest,
                            &response.content_digest,
                            &response.value,
                        )
                    })
                {
                    return Err(RepositoryError::InvalidInput(
                        "invalid_task_input".to_owned(),
                    ));
                }
            } else if command.validated_input.is_some() {
                return Err(RepositoryError::InvalidInput(
                    "invalid_task_input".to_owned(),
                ));
            }
        } else if command.validated_input.is_some() {
            return Err(RepositoryError::InvalidInput(
                "invalid_task_input".to_owned(),
            ));
        }
        let next = decide_task_resolution(
            &projection,
            DomainResolveTask {
                expected_generation: command.expected_generation,
                expected_version: command.expected_task_version,
                target: command.target,
                principal: (command.target != TaskState::Cancelled).then_some(principal),
                response_value_id: command
                    .response
                    .as_ref()
                    .map(|response| response.value_id.clone()),
                response_schema_digest: command
                    .response
                    .as_ref()
                    .map(|response| response.schema_digest.clone()),
            },
            database_now,
        )?;
        let node_payload =
            payload_from_row(&node, "payload_schema_version", "payload", "payload_digest")?;
        let mut stored_wait: StoredHumanTaskWaitPayload =
            decode_typed_payload(&node_payload, "HumanTask wait Node")?;
        let stored_plan_node_key = PlanNodeKey::new(node.try_get("plan_node_key")?)?;
        if stored_wait.task_id != command.task_id
            || stored_wait.plan_node_key != stored_plan_node_key
            || stored_wait.resolution.is_some()
        {
            return Err(RepositoryError::Conflict("HumanTask wait owner evidence"));
        }
        let outcome = match next.state {
            TaskState::Responded => DurableWaitOutcome::Succeeded,
            TaskState::Declined => DurableWaitOutcome::Declined,
            TaskState::Cancelled => DurableWaitOutcome::Cancelled,
            TaskState::Expired => DurableWaitOutcome::TimedOut,
            _ => return Err(RepositoryError::Conflict("HumanTask resolution state")),
        };
        let response_reference = command.response.as_ref().map(|response| ExactRunValueRef {
            value_id: response.value_id.clone(),
            schema_digest: response.schema_digest.clone(),
            content_digest: response.content_digest.clone(),
        });
        if matches!(outcome, DurableWaitOutcome::Succeeded) != response_reference.is_some()
            || response_reference.as_ref().is_some_and(|response| {
                response.schema_digest != *stored_wait.response_port.schema_digest()
            })
        {
            return Err(RepositoryError::Conflict(
                "HumanTask response owner evidence",
            ));
        }
        if let Some(response) = &command.response {
            insert_task_response_value(&mut transaction, &current, &run, node_id, response).await?;
            bind_run_value_to_scope(
                &mut transaction,
                &current.tenant_id,
                &run.run_id,
                node.try_get("scope_id")?,
                &stored_wait.response_port,
                response_reference
                    .as_ref()
                    .expect("response reference exists after response insertion"),
                self.scope_environment_limits,
            )
            .await?;
        }
        stored_wait.resolution = Some(StoredHumanTaskResolution {
            outcome,
            response: response_reference,
        });
        let resolved_node_payload = TypedPayload::with_limit(1, &stored_wait, 262_144)?;
        let source_job =
            load_task_source_orchestration_job(&mut transaction, &current.tenant_id, node_id)
                .await?;
        if source_job.deadline <= database_now || source_job.deadline != run.deadline {
            return Err(RepositoryError::Conflict("Task resume deadline"));
        }
        let resolved = mutate_resolved_orchestration_task(
            &mut transaction,
            &current,
            &next,
            &run,
            node.try_get("version")?,
            &source_job,
            &resolved_node_payload,
            &command.resume_job_id,
            &command.resume_request_digest,
            database_now,
        )
        .await?;
        append_resolved_orchestration_task_events(&mut transaction, &resolved, &command).await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &resolved.job.job_id,
            resolved.task.state.as_str(),
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(resolved))
    }

    pub async fn drive_expired_orchestration_tasks(
        &mut self,
        command: DriveExpiredOrchestrationTasks,
    ) -> Result<SafetyScanPage<ResolvedOrchestrationTask>, RepositoryError> {
        command.validate(self.recovery_batch_limit, self.recovery_shard_limit)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let candidates = sqlx::query(
            r#"
            SELECT task.tenant_id, task.task_id, task.run_id, task.node_id,
                   task.deadline AS scan_sort_at
            FROM insight_platform.tasks AS task
            JOIN insight_platform.runs AS run
              ON run.tenant_id = task.tenant_id AND run.run_id = task.run_id
            JOIN insight_platform.run_nodes AS node
              ON node.tenant_id = task.tenant_id AND node.node_id = task.node_id
            WHERE task.owner_kind = 'node_execution'
              AND task.task_kind <> 'approval'
              AND task.state = 'pending' AND task.responded_at IS NULL
              AND task.deadline <= $1
              AND run.state IN ('waiting', 'running') AND run.terminal_at IS NULL
              AND run.deadline > $1
              AND run.current_payload #> '{control,cancel_requested_at}' = 'null'::jsonb
              AND run.current_payload #> '{control,timeout_requested_at}' = 'null'::jsonb
              AND node.record_kind = 'node_execution' AND node.state = 'waiting'
              AND node.terminal_at IS NULL
              AND mod(('x' || right(task.task_id, 8))::bit(32)::bigint, $4) = $3
              AND (
                  $5::timestamptz IS NULL OR
                  (task.deadline, task.tenant_id, task.task_id) >
                      ($5::timestamptz, $6::text, $7::text)
              )
            ORDER BY task.deadline, task.tenant_id, task.task_id
            LIMIT $2
            "#,
        )
        .bind(database_now)
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
        let scanned_count = candidates.len();
        let last_cursor = candidates
            .last()
            .map(|row| safety_scan_cursor_from_row(row, "task_id", ResourceKind::Interaction))
            .transpose()?;
        let mut diagnostics = Vec::new();
        let mut expired = Vec::with_capacity(candidates.len());
        for (candidate, slot) in candidates.into_iter().zip(command.slots.iter()) {
            let mut object_transaction = transaction.begin().await?;
            let object_result: Result<(), RepositoryError> = async {
                let tenant_id: ResourceId = candidate
                    .try_get::<String, _>("tenant_id")?
                    .parse()
                    .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    })?;
                let run_id: ResourceId = candidate
                    .try_get::<Option<String>, _>("run_id")?
                    .ok_or_else(|| RepositoryError::CorruptRow("Task has no Run".to_owned()))?
                    .parse()
                    .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    })?;
                let task_id: ResourceId = candidate
                    .try_get::<String, _>("task_id")?
                    .parse()
                    .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    })?;
                let node_id = candidate
                    .try_get::<Option<String>, _>("node_id")?
                    .ok_or_else(|| RepositoryError::CorruptRow("Task has no Node".to_owned()))?;
                let run = load_run_for_update(&mut object_transaction, &tenant_id, &run_id).await?;
                let node = sqlx::query(
                    r#"
                SELECT version, state, plan_node_key,
                       payload_schema_version, payload, payload_digest
                FROM insight_platform.run_nodes
                WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
                  AND record_kind = 'node_execution'
                FOR UPDATE
                "#,
                )
                .bind(tenant_id.to_string())
                .bind(&node_id)
                .bind(run_id.to_string())
                .fetch_optional(&mut *object_transaction)
                .await?
                .ok_or(RepositoryError::NotFound("Task owner Node"))?;
                let task =
                    load_task_for_update(&mut object_transaction, &tenant_id, &task_id).await?;
                if task.task_kind == TaskKind::Approval
                    || task.owner_kind != "node_execution"
                    || task.owner_id != node_id
                    || task.run_id.as_deref() != Some(run.run_id.as_str())
                    || task.node_id.as_deref() != Some(node_id.as_str())
                    || task.state != TaskState::Pending
                    || task.responded_at.is_some()
                    || task.deadline > database_now
                    || run.terminal_at.is_some()
                    || !matches!(run.state.as_str(), "waiting" | "running")
                    || run.deadline <= database_now
                    || run.current.control.cancel_requested_at.is_some()
                    || run.current.control.timeout_requested_at.is_some()
                    || node.try_get::<String, _>("state")? != "waiting"
                {
                    return Ok(());
                }
                let next = decide_task_resolution(
                    &crate::recovery_isolation::addressed(
                        task_projection(&task),
                        &tenant_id,
                        &task_id,
                        insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
                    )?,
                    DomainResolveTask {
                        expected_generation: u64::try_from(task.generation).map_err(|_| {
                            RepositoryError::CorruptRow(
                                "Task generation is outside the domain bound".to_owned(),
                            )
                        })?,
                        expected_version: u64::try_from(task.version).map_err(|_| {
                            RepositoryError::CorruptRow(
                                "Task version is outside the domain bound".to_owned(),
                            )
                        })?,
                        target: TaskState::Expired,
                        principal: None,
                        response_value_id: None,
                        response_schema_digest: None,
                    },
                    database_now,
                )?;
                let source_job = load_task_source_orchestration_job(
                    &mut object_transaction,
                    &task.tenant_id,
                    &node_id,
                )
                .await?;
                if source_job.deadline <= database_now || source_job.deadline != run.deadline {
                    return Ok(());
                }
                let node_payload =
                    payload_from_row(&node, "payload_schema_version", "payload", "payload_digest")?;
                let mut stored_wait: StoredHumanTaskWaitPayload =
                    crate::recovery_isolation::addressed(
                        decode_typed_payload(&node_payload, "expiring HumanTask wait Node"),
                        &tenant_id,
                        &task_id,
                        insight_platform_jobs::store::SafetyScanPhase::OwnerDecode,
                    )?;
                if stored_wait.task_id != task_id
                    || stored_wait.plan_node_key
                        != PlanNodeKey::new(node.try_get("plan_node_key")?)?
                    || stored_wait.resolution.is_some()
                {
                    return Err(RepositoryError::Conflict(
                        "expiring HumanTask wait owner evidence",
                    ));
                }
                stored_wait.resolution = Some(StoredHumanTaskResolution {
                    outcome: DurableWaitOutcome::TimedOut,
                    response: None,
                });
                let resolved_node_payload = TypedPayload::with_limit(1, &stored_wait, 262_144)?;
                let produced = async {
                    let resolved = mutate_resolved_orchestration_task(
                        &mut object_transaction,
                        &task,
                        &next,
                        &run,
                        node.try_get("version")?,
                        &source_job,
                        &resolved_node_payload,
                        &slot.resume_job_id,
                        &slot.resume_request_digest,
                        database_now,
                    )
                    .await?;
                    append_expired_orchestration_task_events(
                        &mut object_transaction,
                        &resolved,
                        slot,
                    )
                    .await?;
                    Ok(resolved)
                }
                .await;
                expired.push(crate::recovery_isolation::produced(produced)?);
                Ok(())
            }
            .await;
            match object_result {
                Ok(()) => object_transaction.commit().await?,
                Err(RepositoryError::InvalidPersistedObject(diagnostic)) => {
                    object_transaction.rollback().await?;
                    diagnostics.push(diagnostic);
                }
                Err(error) => {
                    object_transaction.rollback().await?;
                    return Err(error);
                }
            }
        }
        transaction.commit().await?;
        Ok(
            safety_scan_page(expired, scanned_count, command.limit, last_cursor)
                .with_diagnostics(diagnostics),
        )
    }
}
