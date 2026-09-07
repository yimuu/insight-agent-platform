//! PostgreSQL controller commands. Shared locks and atomicity remain in this adapter.
use super::*;

impl PgSchedulerTransaction {
    pub async fn apply_orchestration_controller_step(
        &mut self,
        command: ApplyOrchestrationControllerStep,
    ) -> Result<CommandOutcome<AppliedOrchestrationControllerStep>, RepositoryError> {
        self.apply_orchestration_controller_step_internal(command, None)
            .await
    }

    pub async fn apply_derived_expression_controller_step(
        &mut self,
        command: ApplyDerivedExpressionControllerStep,
    ) -> Result<CommandOutcome<AppliedOrchestrationControllerStep>, RepositoryError> {
        command.validate_at(Utc::now(), self.plan_limits)?;
        let ApplyDerivedExpressionControllerStep {
            step,
            materialized_inputs,
            evaluation,
            output_value_ids,
        } = command;
        self.apply_orchestration_controller_step_internal(
            step,
            Some(DerivedExpressionCommitEvidence {
                materialized_inputs,
                evaluation,
                output_value_ids,
            }),
        )
        .await
    }

    async fn apply_orchestration_controller_step_internal(
        &mut self,
        command: ApplyOrchestrationControllerStep,
        derived: Option<DerivedExpressionCommitEvidence>,
    ) -> Result<CommandOutcome<AppliedOrchestrationControllerStep>, RepositoryError> {
        command.validate_at(Utc::now(), self.plan_limits)?;
        let plan_digest = command
            .plan
            .canonical_digest(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "activations": command
                    .mutations
                    .activations
                    .iter()
                    .map(|slot| serde_json::json!({
                        "job_event_id": slot.job_event_id,
                        "job_outbox_id": slot.job_outbox_id,
                        "node_event_id": slot.node_event_id,
                        "node_execution_id": slot.node_execution_id,
                        "node_outbox_id": slot.node_outbox_id,
                        "orchestration_job_id": slot.orchestration_job_id,
                        "scope": slot.scope.as_ref().map(|scope| serde_json::json!({
                            "scope_event_id": scope.scope_event_id,
                            "scope_instance_id": scope.scope_instance_id,
                            "scope_outbox_id": scope.scope_outbox_id,
                        })),
                    }))
                    .collect::<Vec<_>>(),
                "controller_event_ids": {
                    "job_event_id": command.mutations.job_event_id,
                    "job_outbox_id": command.mutations.job_outbox_id,
                    "node_event_id": command.mutations.node_event_id,
                    "node_outbox_id": command.mutations.node_outbox_id,
                    "run_event_id": command.mutations.run_event_id,
                    "run_outbox_id": command.mutations.run_outbox_id,
                },
                "observation": command.observation,
                "observation_evidence_digest": derived
                    .as_ref()
                    .map(|evidence| &evidence.evaluation.evidence.canonical_digest),
                "output_value_ids": derived
                    .as_ref()
                    .map(|evidence| &evidence.output_value_ids),
                "pending_nodes": command
                    .mutations
                    .pending_nodes
                    .iter()
                    .map(|slot| serde_json::json!({
                        "node_event_id": slot.node_event_id,
                        "node_execution_id": slot.node_execution_id,
                        "node_outbox_id": slot.node_outbox_id,
                    }))
                    .collect::<Vec<_>>(),
                "pending_wake": command
                    .mutations
                    .pending_wake
                    .as_ref()
                    .map(|slot| serde_json::json!({
                        "job_event_id": slot.job_event_id,
                        "job_outbox_id": slot.job_outbox_id,
                        "node_event_id": slot.node_event_id,
                        "node_outbox_id": slot.node_outbox_id,
                        "orchestration_job_id": slot.orchestration_job_id,
                        "request_digest": slot.request_digest,
                    })),
                "plan_digest": plan_digest,
                "quota_entry_ids": command.mutations.quota_entry_ids,
                "receipt_id": command.mutations.receipt_id,
                "remainder_cancellations": command
                    .mutations
                    .remainder_cancellations
                    .iter()
                    .map(|slot| serde_json::json!({
                        "expected_scope_id": slot.expected_scope_id,
                        "job_terminal_event_id": slot.job_terminal_event_id,
                        "job_terminal_outbox_id": slot.job_terminal_outbox_id,
                        "node_cancelling_event_id": slot.node_cancelling_event_id,
                        "node_cancelling_outbox_id": slot.node_cancelling_outbox_id,
                        "node_terminal_event_id": slot.node_terminal_event_id,
                        "node_terminal_outbox_id": slot.node_terminal_outbox_id,
                        "quota_entry_ids": slot.quota_entry_ids,
                        "scope_closing_event_id": slot.scope_closing_event_id,
                        "scope_closing_outbox_id": slot.scope_closing_outbox_id,
                        "scope_terminal_event_id": slot.scope_terminal_event_id,
                        "scope_terminal_outbox_id": slot.scope_terminal_outbox_id,
                    }))
                    .collect::<Vec<_>>(),
                "structural_exit": command
                    .mutations
                    .structural_exit
                    .as_ref()
                    .map(|slot| serde_json::json!({
                        "scope_closing_event_id": slot.scope_closing_event_id,
                        "scope_closing_outbox_id": slot.scope_closing_outbox_id,
                        "scope_terminal_event_id": slot.scope_terminal_event_id,
                        "scope_terminal_outbox_id": slot.scope_terminal_outbox_id,
                        "loop_rollover": slot.loop_rollover.as_ref().map(|rollover| serde_json::json!({
                            "carried_value_ids": rollover.carried_value_ids,
                            "scope_event_id": rollover.scope.scope_event_id,
                            "scope_instance_id": rollover.scope.scope_instance_id,
                            "scope_outbox_id": rollover.scope.scope_outbox_id,
                        })),
                    })),
            }),
            262_144,
        )?;
        let mut transaction = self.transaction.begin().await?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            "orchestration.controller.step",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let applied = load_applied_orchestration_controller_step(
                &mut transaction,
                &command.fence,
                &command.mutations,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(applied));
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
        let mut parents =
            lock_running_orchestration_job_parents(&mut transaction, &observed).await?;
        require_exact_runtime_plan(&mut transaction, &parents.run, &command.plan, &plan_digest)
            .await?;
        let source_node =
            load_controller_source_node(&mut transaction, &observed, &parents, &command.plan)
                .await?;
        let runtime_node = command.plan.node(&source_node.plan_node_key)?;
        if let Some(evidence) = &derived {
            command
                .plan
                .validate_node_literal_instances(&source_node.plan_node_key)?;
            for input in &evidence.materialized_inputs {
                command
                    .plan
                    .validate_value_instance(&input.value.schema_digest, &input.value.value)?;
            }
            for output in &evidence.evaluation.outputs {
                command
                    .plan
                    .validate_value_instance(&output.value.schema_digest, &output.value.value)?;
            }
        }
        require_exact_controller_observation(
            &mut transaction,
            &observed,
            &parents,
            &source_node,
            runtime_node,
            &command.observation,
            CompletionValidation {
                plan: &command.plan,
                context: self.context_query_limits,
                model: self.model_turn_limits,
                scope: self.scope_environment_limits,
            },
        )
        .await?;
        if let Some(derived) = &derived {
            require_exact_derived_expression_evidence(
                &mut transaction,
                &observed,
                &parents,
                &source_node,
                runtime_node,
                &command.observation,
                derived,
                self.plan_limits.expression,
                self.scope_environment_limits,
            )
            .await?;
        }
        let decision = decide_controller(runtime_node, &command.observation)?;
        let shape = derive_controller_step_shape(
            &mut transaction,
            &observed,
            &parents,
            &source_node,
            &command.plan,
            runtime_node,
            &decision,
            self.plan_limits.maximum_fan_out,
        )
        .await?;
        shape.validate_slots(&command.mutations)?;
        let derived_scope_bindings = if let Some(derived) = &derived {
            let committed = commit_derived_expression_values(
                &mut transaction,
                &observed,
                &parents,
                &source_node,
                runtime_node,
                &shape,
                &command.mutations,
                derived,
                self.expression_inline_limits,
                self.scope_environment_limits,
            )
            .await?;
            parents.scope_version = committed.source_scope_version;
            committed.new_scope_bindings
        } else {
            Vec::new()
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
            return Err(RepositoryError::Conflict("orchestration controller Job"));
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
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
            &command.mutations.quota_entry_ids,
            &command.request_digest,
        )
        .await?;
        let mut applied = mutate_orchestration_controller_step(
            &mut transaction,
            &current,
            &next_job,
            &parents,
            &source_node,
            &shape,
            &command.mutations,
            &command.plan,
            &command.request_digest,
            self.scope_environment_limits,
            &derived_scope_bindings,
            database_now,
        )
        .await?;
        applied.settled_quota_account_ids = quota_accounts
            .iter()
            .map(|account| account.quota_account_id.clone())
            .collect();
        append_orchestration_controller_events(
            &mut transaction,
            &applied,
            &command.mutations,
            &plan_digest,
            &receipt_payload,
        )
        .await?;
        terminalize_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            &command.request_digest,
            "controller_step_applied",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(applied))
    }

    /// Commits a Return or Raise only from the exact terminal port frozen in the bound Plan.
    /// The supplied body is materialization evidence, never terminal authority: the transaction
    /// re-resolves the current lexical Scope and revalidates the immutable RunValue and Interface.
    pub async fn commit_plan_terminal(
        &mut self,
        command: CommitPlanTerminal,
    ) -> Result<CommandOutcome<CompletedOrchestrationRun>, RepositoryError> {
        command.validate_at(Utc::now(), self.plan_limits)?;
        let plan_digest = command
            .plan
            .canonical_digest(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "classification": command.value.classification,
                "content_digest": command.value.content_digest,
                "plan_digest": plan_digest,
                "schema_digest": command.value.schema_digest,
                "value_id": command.value.value_id,
            }),
            65_536,
        )?;
        let mut transaction = self.transaction.begin().await?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            "orchestration.plan_terminal.commit",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let completed = load_completed_orchestration_run(
                &mut transaction,
                &command.fence.tenant_id,
                &command.fence.job_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(completed));
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
        let parents = lock_terminal_orchestration_parents(&mut transaction, &observed).await?;
        require_exact_runtime_plan(&mut transaction, &parents.run, &command.plan, &plan_digest)
            .await?;
        let source_node =
            load_controller_source_node(&mut transaction, &observed, &parents, &command.plan)
                .await?;
        let runtime_node = command.plan.node(&source_node.plan_node_key)?;
        let interface = load_exact_agent_interface_spec(&mut transaction, &parents.run).await?;
        let (terminal_state, terminal_port, schema, terminal_failure) = match runtime_node {
            insight_platform_plan::RuntimeNode::Return { value } => (
                OrchestrationRunTerminalState::Succeeded,
                value,
                &interface.output_schema,
                None,
            ),
            insight_platform_plan::RuntimeNode::Raise { failure } => {
                let failure_value: Failure = serde_json::from_value(command.value.body.clone())
                    .map_err(|_| {
                        RepositoryError::InvalidInput(
                            "Raise terminal body is not a safe Failure".to_owned(),
                        )
                    })?;
                failure_value
                    .validate(1_024)
                    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
                require_failure_references(&mut transaction, &parents.run, &failure_value).await?;
                (
                    OrchestrationRunTerminalState::Failed,
                    failure,
                    &interface.error_schema,
                    Some(failure_value),
                )
            }
            _ => {
                return Err(RepositoryError::Conflict(
                    "running orchestration Node is not a Plan terminal",
                ))
            }
        };
        let terminal_value_id = require_exact_terminal_value(
            &mut transaction,
            &parents.run,
            &parents.scope_id,
            terminal_port,
            schema,
            &command.value,
            self.scope_environment_limits,
        )
        .await?;
        let result_payload = if let Some(failure) = &terminal_failure {
            TypedPayload::with_limit(1, failure, 65_536)?
        } else {
            TypedPayload::with_limit(
                1,
                &serde_json::json!({
                    "classification": command.value.classification,
                    "content_digest": command.value.content_digest,
                    "schema_digest": command.value.schema_digest,
                    "value_id": terminal_value_id,
                }),
                65_536,
            )?
        };
        let result_digest: Sha256Digest = result_payload
            .digest
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("terminal result digest".to_owned()))?;
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
            return Err(RepositoryError::Conflict("orchestration Job"));
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let next = decide_job_terminal(
            &job_projection(&current)?,
            &domain_job_fence(&command.fence)?,
            database_now,
            terminal_state.job_state(),
        )?;
        settle_locked_job_quota_bundle(
            &mut transaction,
            &current,
            &quota_accounts,
            &command.mutations.quota_entry_ids,
            &command.request_digest,
        )
        .await?;
        let output_value_id = (terminal_state == OrchestrationRunTerminalState::Succeeded)
            .then_some(terminal_value_id.as_str());
        let mut completed = mutate_terminal_orchestration_run(
            &mut transaction,
            &current,
            &next,
            &parents,
            terminal_state,
            &result_digest,
            output_value_id,
            terminal_failure.as_ref(),
            database_now,
        )
        .await?;
        completed.settled_quota_account_ids = quota_accounts
            .iter()
            .map(|account| account.quota_account_id.clone())
            .collect();
        append_orchestration_terminal_events(
            &mut transaction,
            &completed,
            terminal_state,
            &command.mutations,
            &result_payload,
        )
        .await?;
        terminalize_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            &command.request_digest,
            terminal_state.as_str(),
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(completed))
    }
}
