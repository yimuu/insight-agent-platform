//! PostgreSQL capability commands. Shared locks and atomicity remain in this adapter.
use super::*;

impl PgSchedulerTransaction {
    pub async fn defer_orchestration_to_capability_invocation(
        &mut self,
        command: DeferOrchestrationToCapabilityInvocation,
    ) -> Result<CommandOutcome<DeferredOrchestrationCapabilityInvocation>, RepositoryError> {
        command.validate_at(Utc::now(), self.expression_inline_limits)?;
        let plan_digest = command.plan.canonical_digest(self.plan_limits)?;
        let mut transaction = self.transaction.begin().await?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "capability_job_id": command.capability_job_id,
                "input_value_id": command.input.run_value_id,
                "invocation_id": command.invocation_id,
                "selection_evidence_digest": command.selection_evidence.canonical_digest,
            }),
            65_536,
        )?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.source.receipt_id,
            "orchestration.capability.defer",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let deferred = load_deferred_orchestration_capability_invocation(
                &mut transaction,
                &command.fence,
                &command.invocation_id,
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
        let RuntimeNode::CapabilityCall {
            capability_slot_id,
            output,
            attempt_limit,
            retry_backoff_milliseconds,
            resume,
            ..
        } = command.plan.node(&source_node.plan_node_key)?
        else {
            return Err(RepositoryError::Conflict(
                "orchestration Capability exact Plan node",
            ));
        };
        let selected_ordinal = require_exact_capability_candidate_selection(
            &mut transaction,
            &parents,
            command.plan.node(&source_node.plan_node_key)?,
            &command,
            self.scope_environment_limits,
        )
        .await?;
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
        let admit_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "input_content_digest": command.input.content_digest,
            "invocation_id": command.invocation_id,
            "operation": "capability.admit",
            "plan_digest": plan_digest,
            "selection_evidence_digest": command.selection_evidence.canonical_digest,
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse()
        .map_err(|_| RepositoryError::InvalidInput("Capability admission digest".to_owned()))?;
        let admitted =
            match crate::invocation_repository::admit_capability_invocation_in_transaction(
                &mut transaction,
                AdmitCapabilityInvocation {
                    audit: CommandAudit {
                        trace: parents.run.trace,
                        tenant_id: tenant_id.clone(),
                        principal_id: principal.principal_id.clone(),
                        principal_kind: principal.principal_kind,
                        receipt_id: command.mutations.invocation_admit_receipt_id.clone(),
                        event_id: command.mutations.invocation_admit_event_id.clone(),
                        outbox_id: command.mutations.invocation_admit_outbox_id.clone(),
                        idempotency_key_digest: command.idempotency_key_digest.clone(),
                        request_digest: admit_digest,
                        receipt_expires_at: command.receipt_expires_at,
                    },
                    invocation_id: command.invocation_id.clone(),
                    run_id: run_id.clone(),
                    node_execution_id: node_id.clone(),
                    expected_run_version: u64::try_from(parents.run.version).map_err(|_| {
                        RepositoryError::CorruptRow("Run version is negative".to_owned())
                    })?,
                    expected_node_version: u64::try_from(parents.node_version).map_err(|_| {
                        RepositoryError::CorruptRow("Node version is negative".to_owned())
                    })?,
                    slot_id: capability_slot_id.clone(),
                    input_value_id: command.input.run_value_id.clone(),
                    input_artifact_link_id: command.input_artifact_link_id.clone(),
                    origin: InvocationOrigin::PlanNode {
                        node_execution_id: node_id,
                    },
                    selected_candidate_ordinal: selected_ordinal,
                    selector_input_digest: command.selection_evidence.canonical_digest.clone(),
                    policy_decisions: command.policy_decisions.clone(),
                    approval_task_id: command.approval_task_id.clone(),
                    requested_attempt_limit: u32::from(*attempt_limit),
                    requested_retry_backoff_milliseconds: *retry_backoff_milliseconds,
                    mcp_runtime: command.mcp_runtime.clone(),
                },
                self.invocation_limits,
                self.context_query_limits,
                database_now,
            )
            .await?
            {
                CommandOutcome::Applied(invocation) | CommandOutcome::Replayed(invocation) => {
                    invocation
                }
            };
        let sandbox_backend = admitted.payload.admission.backend_kind
            == insight_platform_contracts::CapabilityBackendKind::Sandbox;
        if sandbox_backend != command.sandbox_submission.is_some() {
            return Err(RepositoryError::Conflict(
                "Sandbox Capability admission configuration",
            ));
        }
        let (invocation, capability_job) = if admitted.state == InvocationState::Ready
            && sandbox_backend
        {
            let submission =
                command
                    .sandbox_submission
                    .as_ref()
                    .ok_or(RepositoryError::Conflict(
                        "Sandbox Capability admission configuration",
                    ))?;
            match crate::sandbox_repository::accept_sandbox_capability_in_transaction(
                &mut transaction,
                &admitted,
                &command.capability_job_id,
                submission,
                database_now,
            )
            .await?
            {
                CommandOutcome::Applied(_) | CommandOutcome::Replayed(_) => {}
            }
            let invocation = crate::invocation_repository::load_capability_invocation(
                &mut transaction,
                &tenant_id,
                &command.invocation_id,
                false,
            )
            .await?;
            let job = load_job_by_text(
                &mut transaction,
                &command.fence.tenant_id,
                &command.capability_job_id.to_string(),
            )
            .await?;
            (invocation, Some(job))
        } else if admitted.state == InvocationState::Ready {
            let prepare_digest: Sha256Digest = canonical_digest(&serde_json::json!({
                "capability_job_id": command.capability_job_id,
                "expected_invocation_version": admitted.version,
                "invocation_id": command.invocation_id,
                "operation": "capability.dispatch.prepare",
            }))
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            .parse()
            .map_err(|_| RepositoryError::InvalidInput("Capability prepare digest".to_owned()))?;
            let prepared = match crate::capability_execution_repository::prepare_capability_dispatch_in_transaction(
                &mut transaction,
                PrepareCapabilityDispatch {
                    audit: CommandAudit {
                        trace: parents.run.trace,
                        tenant_id: tenant_id.clone(),
                        principal_id: principal.principal_id.clone(),
                        principal_kind: principal.principal_kind,
                        receipt_id: command.mutations.invocation_prepare_receipt_id.clone(),
                        event_id: command.mutations.invocation_prepare_event_id.clone(),
                        outbox_id: command.mutations.invocation_prepare_outbox_id.clone(),
                        idempotency_key_digest: command.idempotency_key_digest.clone(),
                        request_digest: prepare_digest,
                        receipt_expires_at: command.receipt_expires_at,
                    },
                    invocation_id: command.invocation_id.clone(),
                    expected_invocation_version: admitted.version,
                    job_id: command.capability_job_id.clone(),
                    scheduled_at: database_now,
                },
                database_now,
            )
            .await?
            {
                CommandOutcome::Applied(prepared) | CommandOutcome::Replayed(prepared) => prepared,
            };
            (prepared.invocation, Some(prepared.job))
        } else if admitted.state == InvocationState::AwaitingApproval {
            (admitted, None)
        } else {
            return Err(RepositoryError::Conflict(
                "Capability admission owner state",
            ));
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
            return Err(RepositoryError::Conflict("Capability source Job"));
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
            &StoredCapabilityInvocationWaitPayload {
                plan_node_key: source_node.plan_node_key,
                plan_digest,
                source_orchestration_job_id: command.fence.job_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?,
                invocation_id: command.invocation_id.clone(),
                capability_job_id: Some(command.capability_job_id.clone()),
                output_port: output.clone(),
                resume_plan_node_key: resume.clone(),
                resume_node_kind: command.plan.node(resume)?.kind(),
                root_scope_id: orchestration_payload.root_scope_id,
                continuation_attempt_limit: current.attempt_limit,
                retry_backoff_milliseconds: orchestration_payload.retry_backoff_milliseconds,
                priority: current.priority,
                deadline: current.deadline.min(parents.run.deadline),
            },
            262_144,
        )?;
        let mut deferred = mutate_deferred_orchestration_capability_invocation(
            &mut transaction,
            &current,
            &next_job,
            &parents,
            &invocation,
            capability_job.as_ref(),
            &wait_payload,
            database_now,
        )
        .await?;
        deferred.settled_quota_account_ids = quota_accounts
            .iter()
            .map(|account| account.quota_account_id.clone())
            .collect();
        append_deferred_orchestration_capability_events(
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
            if capability_job.is_some() {
                "capability_ready"
            } else {
                "capability_approval_pending"
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(deferred))
    }
}
