//! PostgreSQL model commands. Shared locks and atomicity remain in this adapter.
use super::*;

fn require_model_response_schema(
    plan: &RuntimePlan,
    node_key: &PlanNodeKey,
    response: &insight_platform_models::ModelResponseContract,
) -> Result<(), RepositoryError> {
    let expected = plan.model_response_schema(node_key)?;
    if response.output_schema_digest != expected.canonical_digest
        || response.structured_schema.as_ref() != Some(&expected)
    {
        return Err(RepositoryError::Conflict("Model node response schema"));
    }
    Ok(())
}

impl PgSchedulerTransaction {
    pub async fn dispatch_model_tool_capabilities(
        &mut self,
        command: DispatchModelToolCapabilities,
    ) -> Result<CommandOutcome<DispatchedModelToolCapabilities>, RepositoryError> {
        command.validate_at(Utc::now(), self.plan_limits)?;
        let plan_digest = command.plan.canonical_digest(self.plan_limits)?;
        let mut transaction = self.transaction.begin().await?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "model_turn_id": command.continuation.model_turn_id,
                "response_digest": command.continuation.response_digest,
                "tool_intent_count": command.continuation.tool_intent_count,
            }),
            65_536,
        )?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.source_mutations.receipt_id,
            "orchestration.model_tools.dispatch",
            &command.request_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let source_job = load_job_by_text(
                &mut transaction,
                &command.fence.tenant_id,
                &command.fence.job_id,
            )
            .await?;
            let tenant_id: ResourceId = command.fence.tenant_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?;
            let run_id: ResourceId = source_job
                .run_id
                .as_deref()
                .ok_or_else(|| RepositoryError::CorruptRow("Model tool Run".to_owned()))?
                .parse()
                .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                })?;
            let run = load_run(&mut transaction, &tenant_id, &run_id).await?;
            let node_id = source_job
                .node_id
                .clone()
                .ok_or_else(|| RepositoryError::CorruptRow("Model tool Node".to_owned()))?;
            let node_version: i64 = sqlx::query_scalar(
                "SELECT version FROM insight_platform.run_nodes WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3 AND record_kind = 'node_execution'",
            )
            .bind(&command.fence.tenant_id)
            .bind(run_id.to_string())
            .bind(&node_id)
            .fetch_one(&mut *transaction)
            .await?;
            let mut invocations = Vec::with_capacity(command.calls.len());
            let mut capability_jobs = Vec::new();
            for call in &command.calls {
                let invocation = crate::invocation_repository::load_capability_invocation(
                    &mut transaction,
                    &tenant_id,
                    &call.invocation_id,
                    false,
                )
                .await?;
                if let Ok(job) = load_job_by_text(
                    &mut transaction,
                    &command.fence.tenant_id,
                    &call.capability_job_id.to_string(),
                )
                .await
                {
                    capability_jobs.push(job);
                }
                invocations.push(invocation);
            }
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(DispatchedModelToolCapabilities {
                run,
                node_id,
                node_version,
                source_job,
                invocations,
                capability_jobs,
            }));
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
            &command.source_mutations.quota_entry_ids,
        )
        .await?;
        let parents = lock_running_orchestration_job_parents(&mut transaction, &observed).await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        if model_run_convergence_goal(&parents.run, database_now)?.is_some() {
            return Err(RepositoryError::Conflict(
                "Model business admission after Run control",
            ));
        }

        require_exact_runtime_plan(&mut transaction, &parents.run, &command.plan, &plan_digest)
            .await?;
        let source_node =
            load_controller_source_node(&mut transaction, &observed, &parents, &command.plan)
                .await?;
        let RuntimeNode::ModelLoop {
            capability_slot_ids,
            maximum_rounds,
            maximum_capability_calls,
            maximum_parallel_calls_per_round,
            token_budget,
            ..
        } = command.plan.node(&source_node.plan_node_key)?
        else {
            return Err(RepositoryError::Conflict("Model tool exact Plan node"));
        };
        let node_payload_row = sqlx::query(
            r#"
            SELECT payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'running'
            "#,
        )
        .bind(&parents.run.tenant_id)
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Model tool running Node"))?;
        let node_payload = payload_from_row(
            &node_payload_row,
            "payload_schema_version",
            "payload",
            "payload_digest",
        )?;
        let wait: StoredModelToolContinuationWaitPayload =
            decode_typed_payload(&node_payload, "Model tool continuation")?;
        let job_payload: OrchestrationJobPayload =
            decode_orchestration_job_payload(&observed.payload)?;
        if job_payload.model_tool_continuation.as_ref() != Some(&command.continuation)
            || wait.plan_node_key != source_node.plan_node_key
            || wait.plan_digest != plan_digest
            || wait.model_turn_id != command.continuation.model_turn_id
            || wait.response_value_id != command.continuation.response_value_id
            || wait.response_digest != command.continuation.response_digest
            || wait.round_ordinal != command.continuation.round_ordinal
            || wait.tool_intent_count != command.continuation.tool_intent_count
            || wait.maximum_rounds != *maximum_rounds
            || wait.maximum_capability_calls != *maximum_capability_calls
            || wait.maximum_parallel_calls_per_round != *maximum_parallel_calls_per_round
            || wait.token_budget == 0
            || wait.token_budget > *token_budget
            || wait.total_capability_calls > *maximum_capability_calls
        {
            return Err(RepositoryError::Conflict(
                "Model tool continuation evidence",
            ));
        }
        let response_row = sqlx::query(
            r#"
            SELECT inline_value, content_digest FROM insight_platform.run_values
            WHERE tenant_id = $1 AND run_id = $2 AND value_id = $3 AND node_id = $4
              AND value_kind = 'model_response' AND artifact_id IS NULL
            "#,
        )
        .bind(&parents.run.tenant_id)
        .bind(&parents.run.run_id)
        .bind(command.continuation.response_value_id.to_string())
        .bind(&parents.node_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Model tool response"))?;
        let response_value: serde_json::Value = response_row.try_get("inline_value")?;
        let response: insight_platform_models::CanonicalModelResponse =
            serde_json::from_value(response_value.clone()).map_err(|failure| {
                RepositoryError::CorruptRow(format!("Model tool response: {failure}"))
            })?;
        if response.tool_intents.len() != command.calls.len()
            || response_row.try_get::<String, _>("content_digest")?
                != command.continuation.response_digest.to_string()
            || canonical_digest(&response_value)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
                != command.continuation.response_digest.to_string()
        {
            return Err(RepositoryError::Conflict("Model tool response binding"));
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let tenant_id: ResourceId = parents.run.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let run_id: ResourceId = parents.run.run_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let node_id: ResourceId = parents.node_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let principal = parents.run.bindings.principal.clone();
        let by_call = response
            .tool_intents
            .iter()
            .map(|intent| (intent.call_id.as_str(), intent))
            .collect::<BTreeMap<_, _>>();
        let mut invocations = Vec::with_capacity(command.calls.len());
        let mut capability_jobs = Vec::new();
        let mut stored_calls = Vec::with_capacity(command.calls.len());
        for call in &command.calls {
            let intent = by_call
                .get(call.call_id.as_str())
                .ok_or(RepositoryError::Conflict("Model tool call ID"))?;
            if intent.projected_tool_name != call.projected_tool_name
                || intent.arguments != call.arguments
                || canonical_digest(&serde_json::json!({"call_id": call.call_id}))
                    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
                    != call.call_id_digest.to_string()
                || !capability_slot_ids.contains(&call.slot_id)
            {
                return Err(RepositoryError::Conflict("Model tool call evidence"));
            }
            let frozen_slot = parents
                .run
                .bindings
                .slots
                .iter()
                .find(|slot| slot.slot_id == call.slot_id)
                .ok_or(RepositoryError::NotFound("Model tool frozen slot"))?;
            let FrozenSlotTarget::Capability { candidates, .. } = &frozen_slot.target else {
                return Err(RepositoryError::Conflict("Model tool frozen slot kind"));
            };
            if candidates.get(usize::from(call.selected_candidate_ordinal))
                != Some(&call.selected_deployment)
            {
                return Err(RepositoryError::Conflict("Model tool selected deployment"));
            }
            sqlx::query(
                r#"
                INSERT INTO insight_platform.run_values (
                    tenant_id, value_id, run_id, node_id, value_kind, classification,
                    schema_digest, content_digest, inline_value, created_at
                ) VALUES ($1, $2, $3, $4, 'capability_input', $5, $6, $7, $8, $9)
                "#,
            )
            .bind(tenant_id.to_string())
            .bind(call.input_value_id.to_string())
            .bind(run_id.to_string())
            .bind(node_id.to_string())
            .bind(DataClassification::Internal.as_str())
            .bind(call.arguments.schema_digest.to_string())
            .bind(call.arguments.canonical_digest.to_string())
            .bind(&call.arguments.value)
            .bind(database_now)
            .execute(&mut *transaction)
            .await?;
            let admit_digest: Sha256Digest = canonical_digest(&serde_json::json!({
                "call_id_digest": call.call_id_digest,
                "input_content_digest": call.arguments.canonical_digest,
                "invocation_id": call.invocation_id,
                "model_turn_id": command.continuation.model_turn_id,
                "operation": "model_tool.capability.admit",
            }))
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            .parse()
            .map_err(|_| RepositoryError::InvalidInput("Model tool admit digest".to_owned()))?;
            let admitted =
                match crate::invocation_repository::admit_capability_invocation_in_transaction(
                    &mut transaction,
                    AdmitCapabilityInvocation {
                        audit: CommandAudit {
                            trace: parents.run.trace,
                            tenant_id: tenant_id.clone(),
                            principal_id: principal.principal_id.clone(),
                            principal_kind: principal.principal_kind,
                            receipt_id: call.mutations.admit_receipt_id.clone(),
                            event_id: call.mutations.admit_event_id.clone(),
                            outbox_id: call.mutations.admit_outbox_id.clone(),
                            idempotency_key_digest: call.idempotency_key_digest.clone(),
                            request_digest: admit_digest,
                            receipt_expires_at: command.receipt_expires_at,
                        },
                        invocation_id: call.invocation_id.clone(),
                        run_id: run_id.clone(),
                        node_execution_id: node_id.clone(),
                        expected_run_version: u64::try_from(parents.run.version).map_err(|_| {
                            RepositoryError::CorruptRow("Model tool Run version".to_owned())
                        })?,
                        expected_node_version: u64::try_from(parents.node_version).map_err(
                            |_| RepositoryError::CorruptRow("Model tool Node version".to_owned()),
                        )?,
                        slot_id: call.slot_id.clone(),
                        input_value_id: call.input_value_id.clone(),
                        input_artifact_link_id: None,
                        origin: InvocationOrigin::ModelToolCall {
                            model_turn_id: command.continuation.model_turn_id.clone(),
                            model_call_id_digest: call.call_id_digest.clone(),
                        },
                        selected_candidate_ordinal: call.selected_candidate_ordinal,
                        selector_input_digest: call.call_id_digest.clone(),
                        policy_decisions: call.policy_decisions.clone(),
                        approval_task_id: call.approval_task_id.clone(),
                        requested_attempt_limit: call.requested_attempt_limit,
                        requested_retry_backoff_milliseconds: call
                            .requested_retry_backoff_milliseconds,
                        mcp_runtime: call.mcp_runtime.clone(),
                    },
                    self.invocation_limits,
                    self.context_query_limits,
                    database_now,
                )
                .await?
                {
                    CommandOutcome::Applied(record) | CommandOutcome::Replayed(record) => record,
                };
            let invocation = if admitted.state == InvocationState::Ready {
                let prepare_digest: Sha256Digest = canonical_digest(&serde_json::json!({
                    "capability_job_id": call.capability_job_id,
                    "invocation_id": call.invocation_id,
                    "operation": "model_tool.capability.prepare",
                }))
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
                .parse()
                .map_err(|_| {
                    RepositoryError::InvalidInput("Model tool prepare digest".to_owned())
                })?;
                let prepared = match crate::capability_execution_repository::prepare_capability_dispatch_in_transaction(
                    &mut transaction,
                    PrepareCapabilityDispatch {
                        audit: CommandAudit {
                            trace: parents.run.trace,
                            tenant_id: tenant_id.clone(),
                            principal_id: principal.principal_id.clone(),
                            principal_kind: principal.principal_kind,
                            receipt_id: call.mutations.prepare_receipt_id.clone(),
                            event_id: call.mutations.prepare_event_id.clone(),
                            outbox_id: call.mutations.prepare_outbox_id.clone(),
                            idempotency_key_digest: call.idempotency_key_digest.clone(),
                            request_digest: prepare_digest,
                            receipt_expires_at: command.receipt_expires_at,
                        },
                        invocation_id: call.invocation_id.clone(),
                        expected_invocation_version: admitted.version,
                        job_id: call.capability_job_id.clone(),
                        scheduled_at: database_now,
                    },
                    database_now,
                ).await? {
                    CommandOutcome::Applied(record) | CommandOutcome::Replayed(record) => record,
                };
                capability_jobs.push(prepared.job);
                prepared.invocation
            } else if admitted.state == InvocationState::AwaitingApproval {
                admitted
            } else {
                return Err(RepositoryError::Conflict("Model tool Invocation state"));
            };
            stored_calls.push(StoredModelToolCallWait {
                call_id: call.call_id.clone(),
                call_id_digest: call.call_id_digest.clone(),
                invocation_id: call.invocation_id.clone(),
                capability_job_id: call.capability_job_id.clone(),
                input_value_id: call.input_value_id.clone(),
                result: None,
                sibling_cancel_event_id: call.mutations.sibling_cancel_event_id.clone(),
                sibling_cancel_outbox_id: call.mutations.sibling_cancel_outbox_id.clone(),
            });
            invocations.push(invocation);
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
        {
            return Err(RepositoryError::Conflict("Model tool source Job"));
        }
        let next_job = decide_job_terminal(
            &job_projection(&current)?,
            &domain_job_fence(&command.fence)?,
            database_now,
            JobState::Succeeded,
        )?;
        settle_locked_job_quota_bundle(
            &mut transaction,
            &current,
            &quota_accounts,
            &command.source_mutations.quota_entry_ids,
            &command.request_digest,
        )
        .await?;
        let batch_payload = TypedPayload::with_limit(
            1,
            &StoredModelToolBatchWaitPayload {
                continuation: wait,
                calls: stored_calls,
            },
            262_144,
        )?;
        let node_version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.run_nodes
            SET state = 'waiting', version = version + 1, payload_schema_version = $4,
                payload = $5, payload_digest = $6, updated_at = $7
            WHERE tenant_id = $1 AND node_id = $2 AND version = $3
              AND record_kind = 'node_execution' AND state = 'running'
              AND terminal_at IS NULL
            RETURNING version
            "#,
        )
        .bind(&parents.run.tenant_id)
        .bind(&parents.node_id)
        .bind(parents.node_version)
        .bind(batch_payload.schema_version)
        .bind(&batch_payload.value)
        .bind(&batch_payload.digest)
        .bind(database_now)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Model tool Node wait"))?;
        let source_job =
            job_from_row(
                sqlx::query(
                    r#"
                UPDATE insight_platform.jobs
                SET state = 'succeeded', version = $4, result_digest = $5,
                    worker_id = NULL, lease_token_digest = NULL,
                    lease_expires_at = NULL, heartbeat_at = NULL,
                    terminal_at = $6, updated_at = $6
                WHERE tenant_id = $1 AND job_id = $2 AND version = $3
                  AND state = 'running'
                RETURNING *
                "#,
                )
                .bind(&current.tenant_id)
                .bind(&current.job_id)
                .bind(current.version)
                .bind(i64::try_from(next_job.version).map_err(|_| {
                    RepositoryError::InvalidInput("Model tool Job version".to_owned())
                })?)
                .bind(&receipt_payload.digest)
                .bind(database_now)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(RepositoryError::Conflict("Model tool source Job terminal"))?,
            )?;
        let mut current_snapshot = parents.run.current.clone();
        if parents.run.active_work_count == 1 {
            current_snapshot.waiting_reason = Some("model_tools".to_owned());
        }
        current_snapshot.validate(&run_id)?;
        let current_payload = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
        let run = run_from_row(
            sqlx::query(
                r#"
                UPDATE insight_platform.runs
                SET state = CASE WHEN active_work_count = 1 THEN 'waiting' ELSE state END,
                    version = version + 1, active_work_count = active_work_count - 1,
                    current_schema_version = $4, current_payload = $5,
                    current_payload_digest = $6, updated_at = $7
                WHERE tenant_id = $1 AND run_id = $2 AND version = $3
                  AND state = 'running' AND active_work_count > 0 AND terminal_at IS NULL
                RETURNING *
                "#,
            )
            .bind(&parents.run.tenant_id)
            .bind(&parents.run.run_id)
            .bind(parents.run.version)
            .bind(current_payload.schema_version)
            .bind(&current_payload.value)
            .bind(&current_payload.digest)
            .bind(database_now)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(RepositoryError::Conflict("Model tool Run wait"))?,
        )?;
        let evidence = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "invocation_ids": invocations.iter().map(|record| &record.invocation_id).collect::<Vec<_>>(),
                "model_turn_id": command.continuation.model_turn_id,
                "tool_intent_count": command.continuation.tool_intent_count,
            }),
            65_536,
        )?;
        append_scheduler_event(
            &mut transaction,
            &parents.run.tenant_id,
            &command.source_mutations.run_event_id,
            &command.source_mutations.run_outbox_id,
            "run",
            &parents.run.run_id,
            run.version,
            Some(&parents.run.run_id),
            "run.model_tools_waiting",
            &evidence,
        )
        .await?;
        append_scheduler_event(
            &mut transaction,
            &parents.run.tenant_id,
            &command.source_mutations.node_event_id,
            &command.source_mutations.node_outbox_id,
            "node_execution",
            &parents.node_id,
            node_version,
            Some(&parents.run.run_id),
            "node.model_tools_waiting",
            &evidence,
        )
        .await?;
        append_scheduler_event(
            &mut transaction,
            &parents.run.tenant_id,
            &command.source_mutations.job_event_id,
            &command.source_mutations.job_outbox_id,
            "job",
            &source_job.job_id,
            source_job.version,
            Some(&parents.run.run_id),
            "job.succeeded",
            &evidence,
        )
        .await?;
        terminalize_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.source_mutations.receipt_id,
            &command.request_digest,
            "model_tools_waiting",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(DispatchedModelToolCapabilities {
            run,
            node_id: parents.node_id,
            node_version,
            source_job,
            invocations,
            capability_jobs,
        }))
    }

    pub async fn defer_orchestration_to_model_turn(
        &mut self,
        command: DeferOrchestrationToModelTurn,
    ) -> Result<CommandOutcome<DeferredOrchestrationModelTurn>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let plan_digest = command.plan.canonical_digest(self.plan_limits)?;
        let mut transaction = self.transaction.begin().await?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "model_job_id": command.model_job_id,
                "model_turn_id": command.model_turn_id,
                "request_digest": command.request.content_digest,
                "selection_evidence_digest": command.selection_evidence.canonical_digest,
            }),
            65_536,
        )?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.source.receipt_id,
            "orchestration.model.defer",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let turn = crate::model_turn_repository::load_model_turn_for_replay(
                &mut transaction,
                &command.fence.tenant_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?,
                &command.model_turn_id,
                self.model_turn_limits,
            )
            .await?;
            let model_job = load_job_by_text(
                &mut transaction,
                &command.fence.tenant_id,
                &command.model_job_id.to_string(),
            )
            .await?;
            let source_job = load_job_by_text(
                &mut transaction,
                &command.fence.tenant_id,
                &command.fence.job_id,
            )
            .await?;
            let run_id: ResourceId = source_job
                .run_id
                .as_deref()
                .ok_or_else(|| RepositoryError::CorruptRow("Model source Run".to_owned()))?
                .parse()
                .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                })?;
            let tenant_id: ResourceId = command.fence.tenant_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?;
            let run = load_run(&mut transaction, &tenant_id, &run_id).await?;
            let node_id = source_job
                .node_id
                .clone()
                .ok_or_else(|| RepositoryError::CorruptRow("Model source Node".to_owned()))?;
            let node_version: i64 = sqlx::query_scalar(
                "SELECT version FROM insight_platform.run_nodes WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3 AND record_kind = 'node_execution'",
            )
            .bind(&command.fence.tenant_id)
            .bind(run_id.to_string())
            .bind(&node_id)
            .fetch_one(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(DeferredOrchestrationModelTurn {
                run,
                node_id,
                node_version,
                source_job,
                turn,
                model_job,
                settled_quota_account_ids: Vec::new(),
            }));
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
            &command.mutations.source.quota_entry_ids,
        )
        .await?;
        let parents = lock_running_orchestration_job_parents(&mut transaction, &observed).await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        if model_run_convergence_goal(&parents.run, database_now)?.is_some() {
            return Err(RepositoryError::Conflict(
                "Model business admission after Run control",
            ));
        }

        require_exact_runtime_plan(&mut transaction, &parents.run, &command.plan, &plan_digest)
            .await?;
        let source_node =
            load_controller_source_node(&mut transaction, &observed, &parents, &command.plan)
                .await?;
        let runtime_node = command.plan.node(&source_node.plan_node_key)?;
        let RuntimeNode::ModelLoop {
            model_slot_id,
            output,
            maximum_rounds,
            maximum_capability_calls,
            maximum_parallel_calls_per_round,
            token_budget,
            resume,
            ..
        } = runtime_node
        else {
            return Err(RepositoryError::Conflict(
                "orchestration Model exact Plan node",
            ));
        };
        let selected_ordinal = require_exact_model_candidate_selection(
            &mut transaction,
            &parents,
            runtime_node,
            &command,
            self.scope_environment_limits,
        )
        .await?;
        require_model_response_schema(
            &command.plan,
            &source_node.plan_node_key,
            &command.request.request.response_contract,
        )?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let tenant_id: ResourceId = parents.run.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let run_id: ResourceId = parents.run.run_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let node_id: ResourceId = parents.node_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let scope_id: ResourceId = parents.scope_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let principal = parents.run.bindings.principal.clone();
        let create_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "model_turn_id": command.model_turn_id,
            "operation": "model.create",
            "plan_digest": plan_digest,
            "request_digest": command.request.content_digest,
            "selection_evidence_digest": command.selection_evidence.canonical_digest,
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse()
        .map_err(|_| RepositoryError::InvalidInput("Model create digest".to_owned()))?;
        let mut model_transaction =
            crate::model_turn_repository::PgModelTurnTransaction::from_transaction(
                transaction.begin().await?,
                self.model_turn_limits,
                self.scope_environment_limits,
            );
        let turn = match model_transaction
            .create_model_turn(CreateModelTurn {
                audit: CommandAudit {
                    trace: parents.run.trace,
                    tenant_id: tenant_id.clone(),
                    principal_id: principal.principal_id.clone(),
                    principal_kind: principal.principal_kind,
                    receipt_id: command.mutations.model_create_receipt_id.clone(),
                    event_id: command.mutations.model_create_event_id.clone(),
                    outbox_id: command.mutations.model_create_outbox_id.clone(),
                    idempotency_key_digest: command.idempotency_key_digest.clone(),
                    request_digest: create_digest,
                    receipt_expires_at: command.receipt_expires_at,
                },
                model_turn_id: command.model_turn_id.clone(),
                run_id: run_id.clone(),
                node_execution_id: node_id.clone(),
                scope_instance_id: scope_id,
                expected_run_version: u64::try_from(parents.run.version).map_err(|_| {
                    RepositoryError::CorruptRow("Run version is negative".to_owned())
                })?,
                expected_node_version: u64::try_from(parents.node_version).map_err(|_| {
                    RepositoryError::CorruptRow("Node version is negative".to_owned())
                })?,
                round_ordinal: 1,
                slot_id: model_slot_id.clone(),
                selected_candidate_ordinal: selected_ordinal,
                selector_input_digest: command.selection_evidence.canonical_digest.clone(),
                request: command.request.clone(),
                tool_slots: command.tool_slots.clone(),
                requested_attempt_limit: command.requested_attempt_limit,
                cost_ceiling_microunits: command.cost_ceiling_microunits,
            })
            .await?
        {
            CommandOutcome::Applied(turn) | CommandOutcome::Replayed(turn) => turn,
        };
        let prepare_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "expected_turn_version": turn.version,
            "model_job_id": command.model_job_id,
            "model_turn_id": command.model_turn_id,
            "operation": "model.dispatch.prepare",
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse()
        .map_err(|_| RepositoryError::InvalidInput("Model prepare digest".to_owned()))?;
        let prepared = match model_transaction
            .prepare_model_dispatch(PrepareModelDispatch {
                audit: CommandAudit {
                    trace: parents.run.trace,
                    tenant_id: tenant_id.clone(),
                    principal_id: principal.principal_id,
                    principal_kind: principal.principal_kind,
                    receipt_id: command.mutations.model_prepare_receipt_id.clone(),
                    event_id: command.mutations.model_prepare_event_id.clone(),
                    outbox_id: command.mutations.model_prepare_outbox_id.clone(),
                    idempotency_key_digest: command.idempotency_key_digest.clone(),
                    request_digest: prepare_digest,
                    receipt_expires_at: command.receipt_expires_at,
                },
                model_turn_id: command.model_turn_id.clone(),
                expected_turn_version: turn.version,
                job_id: command.model_job_id.clone(),
                scheduled_at: database_now,
            })
            .await?
        {
            CommandOutcome::Applied(prepared) | CommandOutcome::Replayed(prepared) => prepared,
        };
        model_transaction.commit().await?;
        let current = load_job_for_update_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        if current.version != observed.version
            || current.payload.digest != observed.payload.digest
            || current.quota_reservation_id != observed.quota_reservation_id
        {
            return Err(RepositoryError::Conflict("Model source Job"));
        }
        let next_job = decide_job_terminal(
            &job_projection(&current)?,
            &domain_job_fence(&command.fence)?,
            database_now,
            JobState::Succeeded,
        )?;
        settle_locked_job_quota_bundle(
            &mut transaction,
            &current,
            &quota_accounts,
            &command.mutations.source.quota_entry_ids,
            &command.request_digest,
        )
        .await?;
        let orchestration_payload: OrchestrationJobPayload =
            decode_orchestration_job_payload(&current.payload)?;
        let wait_payload = TypedPayload::with_limit(
            1,
            &StoredModelTurnWaitPayload {
                plan_node_key: source_node.plan_node_key,
                plan_digest,
                source_orchestration_job_id: command.fence.job_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?,
                model_turn_id: command.model_turn_id.clone(),
                model_job_id: command.model_job_id.clone(),
                output_port: output.clone(),
                resume_plan_node_key: resume.clone(),
                resume_node_kind: command.plan.node(resume)?.kind(),
                root_scope_id: orchestration_payload.root_scope_id,
                continuation_attempt_limit: current.attempt_limit,
                retry_backoff_milliseconds: orchestration_payload.retry_backoff_milliseconds,
                priority: current.priority,
                deadline: current.deadline.min(parents.run.deadline),
                round_ordinal: 1,
                maximum_rounds: *maximum_rounds,
                total_capability_calls: 0,
                maximum_capability_calls: *maximum_capability_calls,
                maximum_parallel_calls_per_round: *maximum_parallel_calls_per_round,
                token_budget: *token_budget,
            },
            262_144,
        )?;
        let mut deferred = mutate_deferred_orchestration_model_turn(
            &mut transaction,
            &current,
            &next_job,
            &parents,
            &prepared,
            &wait_payload,
            database_now,
        )
        .await?;
        deferred.settled_quota_account_ids = quota_accounts
            .iter()
            .map(|account| account.quota_account_id.clone())
            .collect();
        append_deferred_orchestration_model_events(
            &mut transaction,
            &deferred,
            &command.mutations.source,
        )
        .await?;
        terminalize_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.source.receipt_id,
            &command.request_digest,
            "model_ready",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(deferred))
    }

    pub async fn continue_model_tool_results_to_model_turn(
        &mut self,
        command: ContinueModelToolResultsToModelTurn,
    ) -> Result<CommandOutcome<DeferredOrchestrationModelTurn>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let plan_digest = command.plan.canonical_digest(self.plan_limits)?;
        let mut transaction = self.transaction.begin().await?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "model_job_id": command.model_job_id,
                "model_turn_id": command.model_turn_id,
                "previous_model_turn_id": command.continuation.model_turn_id,
                "request_digest": command.request.content_digest,
                "result_count": command.continuation.results.len(),
            }),
            65_536,
        )?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.source.receipt_id,
            "orchestration.model_tools.continue",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let tenant_id: ResourceId = command.fence.tenant_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?;
            let turn = crate::model_turn_repository::load_model_turn_for_replay(
                &mut transaction,
                &tenant_id,
                &command.model_turn_id,
                self.model_turn_limits,
            )
            .await?;
            let source_job = load_job_by_text(
                &mut transaction,
                &command.fence.tenant_id,
                &command.fence.job_id,
            )
            .await?;
            let model_job = load_job_by_text(
                &mut transaction,
                &command.fence.tenant_id,
                &command.model_job_id.to_string(),
            )
            .await?;
            let run_id: ResourceId = source_job
                .run_id
                .as_deref()
                .ok_or_else(|| RepositoryError::CorruptRow("Model continuation Run".to_owned()))?
                .parse()
                .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                })?;
            let run = load_run(&mut transaction, &tenant_id, &run_id).await?;
            let node_id = source_job
                .node_id
                .clone()
                .ok_or_else(|| RepositoryError::CorruptRow("Model continuation Node".to_owned()))?;
            let node_version = sqlx::query_scalar(
                "SELECT version FROM insight_platform.run_nodes WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3 AND record_kind = 'node_execution'",
            )
            .bind(&command.fence.tenant_id)
            .bind(run_id.to_string())
            .bind(&node_id)
            .fetch_one(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(DeferredOrchestrationModelTurn {
                run,
                node_id,
                node_version,
                source_job,
                turn,
                model_job,
                settled_quota_account_ids: Vec::new(),
            }));
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
            &command.mutations.source.quota_entry_ids,
        )
        .await?;
        let parents = lock_running_orchestration_job_parents(&mut transaction, &observed).await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        if model_run_convergence_goal(&parents.run, database_now)?.is_some() {
            return Err(RepositoryError::Conflict(
                "Model business admission after Run control",
            ));
        }

        require_exact_runtime_plan(&mut transaction, &parents.run, &command.plan, &plan_digest)
            .await?;
        let node_row = sqlx::query(
            r#"
            SELECT plan_node_key, node_kind, payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'running'
            "#,
        )
        .bind(&parents.run.tenant_id)
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("running Model tool result Node"))?;
        let plan_node_key = PlanNodeKey::new(node_row.try_get("plan_node_key")?)?;
        let RuntimeNode::ModelLoop {
            model_slot_id,
            output,
            maximum_rounds,
            maximum_capability_calls,
            maximum_parallel_calls_per_round,
            resume,
            ..
        } = command.plan.node(&plan_node_key)?
        else {
            return Err(RepositoryError::Conflict("Model tool result Plan node"));
        };
        if node_row.try_get::<String, _>("node_kind")? != PlanNodeKind::ModelLoop.as_str() {
            return Err(RepositoryError::Conflict("Model tool result Node kind"));
        }
        let node_payload = payload_from_row(
            &node_row,
            "payload_schema_version",
            "payload",
            "payload_digest",
        )?;
        let batch: StoredModelToolBatchWaitPayload =
            decode_typed_payload(&node_payload, "Model tool result batch")?;
        let expected_results = batch
            .calls
            .iter()
            .map(|call| {
                call.result.clone().ok_or(RepositoryError::Conflict(
                    "incomplete Model tool result batch",
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if batch.continuation.plan_node_key != plan_node_key
            || batch.continuation.plan_digest != plan_digest
            || batch.continuation.model_turn_id != command.continuation.model_turn_id
            || batch.continuation.response_value_id != command.continuation.response_value_id
            || batch.continuation.response_digest != command.continuation.response_digest
            || batch.continuation.round_ordinal != command.continuation.round_ordinal
            || batch.continuation.tool_intent_count != command.continuation.tool_intent_count
            || batch.continuation.maximum_rounds != *maximum_rounds
            || batch.continuation.maximum_capability_calls != *maximum_capability_calls
            || batch.continuation.maximum_parallel_calls_per_round
                != *maximum_parallel_calls_per_round
            || expected_results != command.continuation.results
        {
            return Err(RepositoryError::Conflict("Model tool result frozen batch"));
        }
        let previous_turn = crate::model_turn_repository::load_model_turn_for_replay(
            &mut transaction,
            &command.fence.tenant_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?,
            &command.continuation.model_turn_id,
            self.model_turn_limits,
        )
        .await?;
        if previous_turn.state != insight_platform_contracts::ModelTurnState::Succeeded
            || previous_turn.round_ordinal != command.continuation.round_ordinal
            || previous_turn.run_id.to_string() != parents.run.run_id
            || previous_turn.node_execution_id.to_string() != parents.node_id
            || previous_turn.output_value_id.as_ref()
                != Some(&command.continuation.response_value_id)
            || previous_turn.payload.admission.slot_id != *model_slot_id
        {
            return Err(RepositoryError::Conflict("previous Model tool turn"));
        }
        let next_round = command
            .continuation
            .round_ordinal
            .checked_add(1)
            .ok_or_else(|| RepositoryError::Conflict("Model round overflow"))?;
        let previous_request = load_canonical_model_request(
            &mut transaction,
            &previous_turn.tenant_id,
            &previous_turn.run_id,
            &previous_turn.node_execution_id,
            &previous_turn.request_value_id,
        )
        .await?;
        require_model_response_schema(
            &command.plan,
            &plan_node_key,
            &command.request.request.response_contract,
        )?;
        if next_round > *maximum_rounds
            || command.request.request.response_contract != previous_request.response_contract
            || command.request.request.model_turn_id != command.model_turn_id
            || command
                .request
                .request
                .input_token_estimate
                .checked_add(u64::from(command.request.request.max_output_tokens))
                .is_none_or(|required| required > batch.continuation.token_budget)
            || command.request.request.tools != previous_request.tools
            || !command
                .request
                .request
                .messages
                .starts_with(&previous_request.messages)
            || !canonical_request_contains_exact_tool_results(
                &command.request.request,
                &command.continuation.results,
                previous_request.messages.len(),
            )
        {
            return Err(RepositoryError::Conflict("next Model request continuation"));
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let tenant_id: ResourceId = parents.run.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let run_id: ResourceId = parents.run.run_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let node_id: ResourceId = parents.node_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let scope_id: ResourceId = parents.scope_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let principal = parents.run.bindings.principal.clone();
        let mut model_transaction =
            crate::model_turn_repository::PgModelTurnTransaction::from_transaction(
                transaction.begin().await?,
                self.model_turn_limits,
                self.scope_environment_limits,
            );
        let turn = match model_transaction
            .create_model_turn(CreateModelTurn {
                audit: CommandAudit {
                    trace: parents.run.trace,
                    tenant_id: tenant_id.clone(),
                    principal_id: principal.principal_id.clone(),
                    principal_kind: principal.principal_kind,
                    receipt_id: command.mutations.model_create_receipt_id.clone(),
                    event_id: command.mutations.model_create_event_id.clone(),
                    outbox_id: command.mutations.model_create_outbox_id.clone(),
                    idempotency_key_digest: command.idempotency_key_digest.clone(),
                    request_digest: command.request_digest.clone(),
                    receipt_expires_at: command.receipt_expires_at,
                },
                model_turn_id: command.model_turn_id.clone(),
                run_id: run_id.clone(),
                node_execution_id: node_id.clone(),
                scope_instance_id: scope_id,
                expected_run_version: u64::try_from(parents.run.version)
                    .map_err(|_| RepositoryError::CorruptRow("Run version".to_owned()))?,
                expected_node_version: u64::try_from(parents.node_version)
                    .map_err(|_| RepositoryError::CorruptRow("Node version".to_owned()))?,
                round_ordinal: next_round,
                slot_id: previous_turn.payload.admission.slot_id.clone(),
                selected_candidate_ordinal: previous_turn
                    .payload
                    .admission
                    .selection_evidence
                    .selected_candidate_ordinal,
                selector_input_digest: previous_turn
                    .payload
                    .admission
                    .selection_evidence
                    .selector_input_digest
                    .clone(),
                request: command.request.clone(),
                tool_slots: previous_turn.payload.admission.tool_slots.clone(),
                requested_attempt_limit: command.requested_attempt_limit,
                cost_ceiling_microunits: command.cost_ceiling_microunits,
            })
            .await?
        {
            CommandOutcome::Applied(turn) | CommandOutcome::Replayed(turn) => turn,
        };
        let prepared = match model_transaction
            .prepare_model_dispatch(PrepareModelDispatch {
                audit: CommandAudit {
                    trace: parents.run.trace,
                    tenant_id: tenant_id.clone(),
                    principal_id: principal.principal_id,
                    principal_kind: principal.principal_kind,
                    receipt_id: command.mutations.model_prepare_receipt_id.clone(),
                    event_id: command.mutations.model_prepare_event_id.clone(),
                    outbox_id: command.mutations.model_prepare_outbox_id.clone(),
                    idempotency_key_digest: command.idempotency_key_digest.clone(),
                    request_digest: command.request_digest.clone(),
                    receipt_expires_at: command.receipt_expires_at,
                },
                model_turn_id: command.model_turn_id.clone(),
                expected_turn_version: turn.version,
                job_id: command.model_job_id.clone(),
                scheduled_at: database_now,
            })
            .await?
        {
            CommandOutcome::Applied(prepared) | CommandOutcome::Replayed(prepared) => prepared,
        };
        model_transaction.commit().await?;
        let current = load_job_for_update_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        if current.version != observed.version
            || current.payload.digest != observed.payload.digest
            || current.quota_reservation_id != observed.quota_reservation_id
        {
            return Err(RepositoryError::Conflict("Model continuation source Job"));
        }
        let next_job = decide_job_terminal(
            &job_projection(&current)?,
            &domain_job_fence(&command.fence)?,
            database_now,
            JobState::Succeeded,
        )?;
        settle_locked_job_quota_bundle(
            &mut transaction,
            &current,
            &quota_accounts,
            &command.mutations.source.quota_entry_ids,
            &command.request_digest,
        )
        .await?;
        let orchestration_payload: OrchestrationJobPayload =
            decode_orchestration_job_payload(&current.payload)?;
        let wait_payload = TypedPayload::with_limit(
            1,
            &StoredModelTurnWaitPayload {
                plan_node_key,
                plan_digest,
                source_orchestration_job_id: command.fence.job_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?,
                model_turn_id: command.model_turn_id.clone(),
                model_job_id: command.model_job_id.clone(),
                output_port: output.clone(),
                resume_plan_node_key: resume.clone(),
                resume_node_kind: command.plan.node(resume)?.kind(),
                root_scope_id: orchestration_payload.root_scope_id,
                continuation_attempt_limit: current.attempt_limit,
                retry_backoff_milliseconds: orchestration_payload.retry_backoff_milliseconds,
                priority: current.priority,
                deadline: current.deadline.min(parents.run.deadline),
                round_ordinal: next_round,
                maximum_rounds: *maximum_rounds,
                total_capability_calls: batch.continuation.total_capability_calls,
                maximum_capability_calls: *maximum_capability_calls,
                maximum_parallel_calls_per_round: *maximum_parallel_calls_per_round,
                token_budget: batch.continuation.token_budget,
            },
            262_144,
        )?;
        let mut deferred = mutate_deferred_orchestration_model_turn(
            &mut transaction,
            &current,
            &next_job,
            &parents,
            &prepared,
            &wait_payload,
            database_now,
        )
        .await?;
        deferred.settled_quota_account_ids = quota_accounts
            .iter()
            .map(|account| account.quota_account_id.clone())
            .collect();
        append_deferred_orchestration_model_events(
            &mut transaction,
            &deferred,
            &command.mutations.source,
        )
        .await?;
        terminalize_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.source.receipt_id,
            &command.request_digest,
            "model_ready",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(deferred))
    }
}
