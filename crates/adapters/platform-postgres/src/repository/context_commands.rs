//! PostgreSQL context commands. Shared locks and atomicity remain in this adapter.
use super::*;

impl PgSchedulerTransaction {
    pub async fn defer_orchestration_to_context_query(
        &mut self,
        command: DeferOrchestrationToContextQuery,
    ) -> Result<CommandOutcome<DeferredOrchestrationContextQuery>, RepositoryError> {
        use crate::context_query_repository::PgContextQueryTransaction;

        command.validate_at(Utc::now(), self.expression_inline_limits)?;
        let plan_digest = command.plan.canonical_digest(self.plan_limits)?;
        let mut transaction = self.transaction.begin().await?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "context_job_id": command.context_job_id,
                "context_query_id": command.context_query_id,
                "input_value_id": command.input.run_value_id,
            }),
            65_536,
        )?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.source.receipt_id,
            "orchestration.context.defer",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let deferred = load_deferred_orchestration_context_query(
                &mut transaction,
                &command.fence,
                &command.context_query_id,
                &command.context_job_id,
                self.context_query_limits,
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
            &command.mutations.source.quota_entry_ids,
        )
        .await?;
        let parents = lock_running_orchestration_job_parents(&mut transaction, &observed).await?;
        require_exact_runtime_plan(&mut transaction, &parents.run, &command.plan, &plan_digest)
            .await?;
        let source_node =
            load_controller_source_node(&mut transaction, &observed, &parents, &command.plan)
                .await?;
        let RuntimeNode::ContextQuery {
            context_slot_id,
            request,
            result,
            maximum_items,
            resume,
        } = command.plan.node(&source_node.plan_node_key)?
        else {
            return Err(RepositoryError::Conflict(
                "orchestration Context exact Plan node",
            ));
        };
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
        let scope_id: ResourceId = source_node.scope_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let environments = load_scope_environment_chain(
            &mut transaction,
            &tenant_id,
            &run_id,
            &scope_id,
            self.scope_environment_limits,
        )
        .await?;
        let references = insight_platform_orchestrator::resolve_scope_inputs(
            std::slice::from_ref(request),
            &environments,
            self.scope_environment_limits,
        )?;
        let mut resolved = load_resolved_expression_values(
            &mut transaction,
            &tenant_id,
            &run_id,
            vec![request.clone()],
            references,
        )
        .await?;
        let exact_input = resolved.pop().ok_or_else(|| {
            RepositoryError::CorruptRow("Context input RunValue missing".to_owned())
        })?;
        if exact_input != command.input
            || command.materialized_input.schema_digest != exact_input.schema_digest
            || command.materialized_input.canonical_digest != exact_input.content_digest
            || matches!(&exact_input.value, ValueRef::Inline { value } if value != &command.materialized_input.value)
        {
            return Err(RepositoryError::Conflict(
                "Context materialized input evidence",
            ));
        }
        let slot = parents
            .run
            .bindings
            .slots
            .iter()
            .find(|slot| slot.slot_id == *context_slot_id)
            .ok_or(RepositoryError::NotFound("frozen Context slot"))?;
        if command
            .plan
            .dependency_slots
            .get(context_slot_id)
            .is_none_or(|plan_slot| plan_slot.requirement_digest != slot.requirement_digest)
            || !matches!(slot.target, FrozenSlotTarget::Context { .. })
        {
            return Err(RepositoryError::Conflict("Context Plan frozen requirement"));
        }
        let canonical_input = canonical_json(&command.materialized_input.value)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let query_bytes = u32::try_from(canonical_input.len()).map_err(|_| {
            RepositoryError::InvalidInput("Context query bytes exceed u32".to_owned())
        })?;
        let normalized_filter_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "filter": null,
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse()
        .map_err(|_| RepositoryError::InvalidInput("Context filter digest".to_owned()))?;
        let storage = match &exact_input.value {
            ValueRef::Inline { .. } => InvocationValueStorage::Inline,
            ValueRef::Artifact { artifact } => InvocationValueStorage::Artifact {
                artifact: artifact.clone(),
            },
        };
        let exact_reference = ExactInvocationValueRef {
            schema_version: 1,
            value_id: exact_input.run_value_id.clone(),
            run_id: run_id.clone(),
            producing_node_id: exact_input.producing_node_id.clone(),
            value_kind: exact_input.value_kind.clone(),
            classification: exact_input.classification,
            schema_digest: exact_input.schema_digest.clone(),
            content_digest: exact_input.content_digest.clone(),
            storage,
        };
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let principal = &parents.run.bindings.principal;
        let create_request_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "context_query_id": command.context_query_id,
            "input_content_digest": exact_reference.content_digest,
            "node_execution_id": parents.node_id,
            "operation": "context.create",
            "plan_digest": plan_digest,
            "slot_id": context_slot_id,
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse()
        .map_err(|_| RepositoryError::InvalidInput("Context create digest".to_owned()))?;
        let create_audit = CommandAudit {
            trace: parents.run.trace,
            tenant_id: tenant_id.clone(),
            principal_id: principal.principal_id.clone(),
            principal_kind: principal.principal_kind,
            receipt_id: command.mutations.context_create_receipt_id.clone(),
            event_id: command.mutations.context_create_event_id.clone(),
            outbox_id: command.mutations.context_create_outbox_id.clone(),
            idempotency_key_digest: command.idempotency_key_digest.clone(),
            request_digest: create_request_digest,
            receipt_expires_at: command.receipt_expires_at,
        };
        let create = CreateContextQuery {
            audit: create_audit,
            context_query_id: command.context_query_id.clone(),
            run_id: run_id.clone(),
            node_execution_id: parents.node_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?,
            expected_run_version: u64::try_from(parents.run.version)
                .map_err(|_| RepositoryError::CorruptRow("Run version is negative".to_owned()))?,
            expected_node_version: u64::try_from(parents.node_version)
                .map_err(|_| RepositoryError::CorruptRow("Node version is negative".to_owned()))?,
            slot_id: context_slot_id.clone(),
            request: ContextQueryRequest {
                schema_version: 1,
                input: exact_reference,
                input_artifact_link_id: command.input_artifact_link_id.clone(),
                normalized_query_digest: command.materialized_input.canonical_digest.clone(),
                normalized_filter_digest,
                requested_projection: Vec::new(),
                query_bytes,
                filter_bytes: 0,
                page_size: *maximum_items,
                page_ordinal: 0,
                cursor_digest: None,
            },
            requested_attempt_limit: self.context_query_limits.maximum_attempts(),
            result_byte_ceiling: self.context_query_limits.maximum_result_bytes(),
        };
        let query = match PgContextQueryTransaction::create_context_query_in_transaction(
            &mut transaction,
            create,
            self.context_query_limits,
        )
        .await?
        {
            CommandOutcome::Applied(query) | CommandOutcome::Replayed(query) => query,
        };
        let prepare_request_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "context_job_id": command.context_job_id,
            "context_query_id": command.context_query_id,
            "expected_query_version": query.version,
            "operation": "context.dispatch.prepare",
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse()
        .map_err(|_| RepositoryError::InvalidInput("Context prepare digest".to_owned()))?;
        let prepare = PrepareContextDispatch {
            audit: CommandAudit {
                trace: parents.run.trace,
                tenant_id: tenant_id.clone(),
                principal_id: principal.principal_id.clone(),
                principal_kind: principal.principal_kind,
                receipt_id: command.mutations.context_prepare_receipt_id.clone(),
                event_id: command.mutations.context_prepare_event_id.clone(),
                outbox_id: command.mutations.context_prepare_outbox_id.clone(),
                idempotency_key_digest: command.idempotency_key_digest.clone(),
                request_digest: prepare_request_digest,
                receipt_expires_at: command.receipt_expires_at,
            },
            context_query_id: command.context_query_id.clone(),
            expected_query_version: query.version,
            job_id: command.context_job_id.clone(),
            scheduled_at: database_now,
        };
        let prepared = match PgContextQueryTransaction::prepare_context_dispatch_in_transaction(
            &mut transaction,
            prepare,
            self.context_query_limits,
        )
        .await?
        {
            CommandOutcome::Applied(prepared) | CommandOutcome::Replayed(prepared) => prepared,
        };
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
            return Err(RepositoryError::Conflict("Context source Job"));
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
        let wait_payload = TypedPayload::with_limit(
            1,
            &StoredContextQueryWaitPayload {
                plan_node_key: source_node.plan_node_key,
                plan_digest,
                source_orchestration_job_id: command.fence.job_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?,
                context_query_id: command.context_query_id.clone(),
                context_job_id: command.context_job_id.clone(),
                result_port: result.clone(),
                resume_plan_node_key: resume.clone(),
                resume_node_kind: command.plan.node(resume)?.kind(),
                root_scope_id: decode_orchestration_job_payload(&current.payload)?.root_scope_id,
                continuation_attempt_limit: current.attempt_limit,
                retry_backoff_milliseconds: decode_orchestration_job_payload(&current.payload)?
                    .retry_backoff_milliseconds,
                priority: current.priority,
                deadline: current.deadline.min(parents.run.deadline),
            },
            262_144,
        )?;
        let mut deferred = mutate_deferred_orchestration_context_query(
            &mut transaction,
            &current,
            &next_job,
            &parents,
            &prepared.query,
            &prepared.job,
            &wait_payload,
            database_now,
        )
        .await?;
        deferred.settled_quota_account_ids = quota_accounts
            .iter()
            .map(|account| account.quota_account_id.clone())
            .collect();
        append_deferred_orchestration_context_events(
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
            "context_ready",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(deferred))
    }
}
