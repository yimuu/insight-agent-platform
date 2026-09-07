//! PostgreSQL child run commands. Shared locks and atomicity remain in this adapter.
use super::*;

impl PgSchedulerTransaction {
    pub async fn defer_orchestration_to_child_run(
        &mut self,
        command: DeferOrchestrationToChildRun,
    ) -> Result<CommandOutcome<DeferredOrchestrationChildRun>, RepositoryError> {
        command.validate_at(Utc::now(), self.expression_inline_limits)?;
        let plan_digest = command.plan.canonical_digest(self.plan_limits)?;
        let mut transaction = self.transaction.begin().await?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "child_link_id": command.child_link_id,
                "child_run_id": command.child_run_id,
                "input_digest": command.input.content_digest,
                "selected_child_deployment": command.selected_child_deployment,
                "selection_evidence_digest": command.selection_evidence.canonical_digest,
                "slot_id": command.slot_id,
                "budget": command.budget,
            }),
            65_536,
        )?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            "orchestration.child.defer",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let deferred = load_deferred_orchestration_child_run(
                &mut transaction,
                &command.fence.tenant_id,
                &command.fence.job_id,
                &command.child_link_id.to_string(),
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
        let parent_run_id: ResourceId = observed
            .run_id
            .as_deref()
            .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Run".to_owned()))?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
        let tenant_id: ResourceId = command.fence.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let observed_parent = load_run(&mut transaction, &tenant_id, &parent_run_id).await?;
        let root_run_id: ResourceId = observed_parent.root_run_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let (quota_accounts, quota_already_settled) =
            lock_job_quota_bundle_state(&mut transaction, &observed).await?;

        let locked_root = if root_run_id == parent_run_id {
            None
        } else {
            Some(load_run_for_update(&mut transaction, &tenant_id, &root_run_id).await?)
        };
        let locked_parent =
            load_run_for_update(&mut transaction, &tenant_id, &parent_run_id).await?;
        if locked_parent.root_run_id != root_run_id.to_string() {
            return Err(RepositoryError::Conflict("orchestration child parent Run"));
        }

        if let Some(existing) = load_child_run_link_by_logical_key(
            &mut transaction,
            &command.fence.tenant_id,
            &locked_parent.run_id,
            &command.logical_key,
        )
        .await?
        {
            require_same_child_run_request(
                &mut transaction,
                &existing,
                &observed,
                &locked_parent,
                &command,
                &plan_digest,
            )
            .await?;
            if !quota_already_settled {
                return Err(RepositoryError::Conflict(
                    "orchestration child logical replay quota",
                ));
            }
            let deferred = load_deferred_orchestration_child_run(
                &mut transaction,
                &command.fence.tenant_id,
                &command.fence.job_id,
                &existing.child_link_id,
            )
            .await?;
            terminalize_job_mutation_receipt_with_reference(
                &mut transaction,
                &command.fence,
                &command.mutations.receipt_id,
                &command.request_digest,
                "child_run_existing",
                &existing.child_link_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(deferred));
        }
        if quota_already_settled {
            return Err(RepositoryError::Conflict(
                "orchestration child quota settlement",
            ));
        }
        if locked_parent.version != observed_parent.version {
            return Err(RepositoryError::Conflict("orchestration child parent Run"));
        }

        let parents = lock_running_orchestration_job_parents(&mut transaction, &observed).await?;
        let current = load_job_for_update_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        if current.version != observed.version
            || current.payload.digest != observed.payload.digest
            || current.quota_reservation_id != observed.quota_reservation_id
            || parents.run.version != locked_parent.version
        {
            return Err(RepositoryError::Conflict("orchestration child deferral"));
        }
        require_exact_runtime_plan(&mut transaction, &parents.run, &command.plan, &plan_digest)
            .await?;
        let principal = &parents.run.bindings.principal;
        let current_principal = load_current_principal_snapshot(
            &mut transaction,
            &tenant_id,
            &principal.principal_id,
            principal.principal_kind,
        )
        .await?;
        if !current_principal.permissions.contains(Permission::AgentRun) {
            return Err(RepositoryError::PermissionDenied);
        }
        let source_node =
            load_controller_source_node(&mut transaction, &observed, &parents, &command.plan)
                .await?;
        let runtime_node = command.plan.node(&source_node.plan_node_key)?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let root_snapshot = locked_root.as_ref().unwrap_or(&parents.run);
        let root_state = root_snapshot
            .state
            .parse::<RunState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if root_snapshot.terminal_at.is_some()
            || !matches!(root_state, RunState::Running | RunState::Waiting)
            || root_snapshot.deadline <= database_now
            || root_snapshot.current.control.pause_requested
            || root_snapshot.current.control.cancel_requested_at.is_some()
            || root_snapshot.current.control.timeout_requested_at.is_some()
        {
            return Err(RepositoryError::Conflict("orchestration child root Run"));
        }
        require_exact_child_candidate_selection(
            &mut transaction,
            &parents,
            runtime_node,
            &command,
            self.scope_environment_limits,
        )
        .await?;
        let RuntimeNode::ChildAgentCall { output, resume, .. } = runtime_node else {
            return Err(RepositoryError::Conflict(
                "orchestration child exact Plan node",
            ));
        };
        let source_payload: OrchestrationJobPayload =
            decode_orchestration_job_payload(&current.payload)?;
        let child_wait = StoredChildRunWaitPayload {
            plan_node_key: source_node.plan_node_key.clone(),
            plan_digest: plan_digest.clone(),
            source_orchestration_job_id: current.job_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?,
            child_link_id: command.child_link_id.clone(),
            child_run_id: command.child_run_id.clone(),
            output_port: output.clone(),
            resume_plan_node_key: resume.clone(),
            resume_node_kind: command.plan.node(resume)?.kind(),
            root_scope_id: source_payload.root_scope_id,
            continuation_attempt_limit: current.attempt_limit,
            retry_backoff_milliseconds: source_payload.retry_backoff_milliseconds,
            priority: current.priority,
            deadline: current.deadline.min(parents.run.deadline),
        };
        let child_deployment = require_enabled_exact_agent_deployment(
            &mut transaction,
            &tenant_id,
            &command.selected_child_deployment,
        )
        .await?;
        let child_closure = match decode_deployment_closure(&child_deployment.bindings)? {
            DeploymentClosure::Agent(closure) => closure,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "child Agent Deployment has a non-Agent closure".to_owned(),
                ))
            }
        };
        let child_entry_plan_node_key = PlanNodeKey::new(child_closure.entry_node_id.clone())?;
        let child_entry_node_kind = child_closure.entry_node_kind;
        validate_deployment_closure_exists(
            &mut transaction,
            &tenant_id,
            &DeploymentClosure::Agent(child_closure.clone()),
        )
        .await?;
        let child_bindings = RunBindingsSnapshot::build(
            command.selected_child_deployment.clone(),
            parents.run.bindings.principal.clone(),
            &child_closure,
        )
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        require_child_input_schema(
            &mut transaction,
            &tenant_id,
            &child_closure.interface,
            &command.input.schema_digest,
        )
        .await?;
        require_child_input_sources(
            &mut transaction,
            &tenant_id,
            &parent_run_id,
            &command.source_value_ids,
            command.input.classification,
        )
        .await?;
        if let ValueRef::Artifact { artifact } = &command.input.value {
            require_ready_run_artifact(&mut transaction, &tenant_id, artifact).await?;
        }

        let parent_descendant_count = u32::try_from(
            locked_root
                .as_ref()
                .unwrap_or(&parents.run)
                .descendant_count,
        )
        .map_err(|_| RepositoryError::CorruptRow("negative root descendant count".to_owned()))?;
        let (parent_delegated_budget, parent_delegated_descendant_count) = if let Some(
            parent_link_id,
        ) =
            parents.run.current.ancestry.parent_child_link_id.as_ref()
        {
            let parent_link = load_child_run_link_by_id(
                &mut transaction,
                &command.fence.tenant_id,
                &parent_link_id.to_string(),
            )
            .await?;
            if parent_link.child_run_id != parents.run.run_id || parent_link.state.is_terminal() {
                return Err(RepositoryError::Conflict(
                    "orchestration parent ChildRunLink budget",
                ));
            }
            (
                Some(parent_link.payload.budget),
                count_run_descendants(&mut transaction, &tenant_id, &parent_run_id).await?,
            )
        } else {
            (None, 0)
        };
        let child_execution = crate::execution_requirements::published_program_requirement(
            &mut transaction,
            &tenant_id,
            &child_bindings,
        )
        .await?;
        // This is the single admission instant, after the current owner and exact dependency
        // facts are locked and validated. Every created execution row uses this same timestamp.
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let inherited_deadline = parents
            .run
            .deadline
            .min(current.deadline)
            .min(root_snapshot.deadline);
        let inherited_deadline = parent_delegated_budget
            .as_ref()
            .map_or(inherited_deadline, |budget| {
                inherited_deadline.min(budget.deadline)
            });
        if inherited_deadline <= database_now {
            return Err(RepositoryError::LeaseExpired);
        }
        if root_snapshot.current.failure.is_some()
            || parents.run.current.failure.is_some()
            || root_snapshot.current.control.pause_requested
            || parents.run.current.control.pause_requested
            || root_snapshot.current.control.cancel_requested_at.is_some()
            || root_snapshot.current.control.timeout_requested_at.is_some()
            || parents.run.current.control.cancel_requested_at.is_some()
            || parents.run.current.control.timeout_requested_at.is_some()
        {
            return Err(RepositoryError::Conflict(
                "orchestration child controlled Run",
            ));
        }
        let next_parent_job = decide_job_terminal(
            &job_projection(&current)?,
            &domain_job_fence(&command.fence)?,
            database_now,
            JobState::Succeeded,
        )?;
        let child_budget = insight_platform_orchestrator::derive_child_budget(
            &command.budget,
            database_now,
            inherited_deadline,
        )?;
        let child_ancestry = prepare_child_run(PrepareChildRun {
            parent_run_id: parent_run_id.clone(),
            parent_node_execution_id: observed
                .node_id
                .as_deref()
                .ok_or_else(|| {
                    RepositoryError::CorruptRow("orchestration Job has no Node".to_owned())
                })?
                .parse()
                .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                })?,
            child_link_id: command.child_link_id.clone(),
            child_run_id: command.child_run_id.clone(),
            child_agent_deployment: command.selected_child_deployment.clone(),
            parent_ancestry: parents.run.current.ancestry.clone(),
            parent_deadline: parents.run.deadline,
            parent_delegated_budget,
            parent_delegated_descendant_count,
            parent_descendant_count,
            maximum_depth: MAX_CHILD_RUN_DEPTH,
            maximum_descendants: MAX_DESCENDANT_RUNS,
            budget: child_budget.clone(),
        })
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let parent_attempt_ordinal = u16::try_from(current.attempt_no).map_err(|_| {
            RepositoryError::CorruptRow("orchestration attempt exceeds u16".to_owned())
        })?;
        let child_link_payload = ChildRunLinkPayload {
            parent_attempt_ordinal,
            child_agent_deployment: command.selected_child_deployment.clone(),
            input_digest: command.input.content_digest.clone(),
            cancellation_policy: command.cancellation_policy,
            budget: child_budget.clone(),
        };
        child_link_payload
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;

        settle_locked_job_quota_bundle(
            &mut transaction,
            &current,
            &quota_accounts,
            &command.mutations.quota_entry_ids,
            &command.request_digest,
        )
        .await?;
        let mut deferred = mutate_deferred_orchestration_child_run(
            &mut transaction,
            &current,
            &next_parent_job,
            &parents,
            locked_root.as_ref(),
            &command,
            &child_bindings,
            &child_execution,
            &child_budget,
            &child_ancestry,
            &child_link_payload,
            &child_wait,
            &child_entry_plan_node_key,
            child_entry_node_kind,
            self.scope_environment_limits,
            database_now,
        )
        .await?;
        deferred.settled_quota_account_ids = quota_accounts
            .iter()
            .map(|account| account.quota_account_id.clone())
            .collect();
        append_deferred_orchestration_child_events(
            &mut transaction,
            &deferred,
            &command.mutations,
            &receipt_payload,
        )
        .await?;
        terminalize_job_mutation_receipt_with_reference(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            &command.request_digest,
            "child_run_created",
            &deferred.child_link.child_link_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(deferred))
    }

    pub async fn drive_terminal_child_runs(
        &mut self,
        command: DriveTerminalChildRuns,
    ) -> Result<SafetyScanPage<ResolvedOrchestrationChildRun>, RepositoryError> {
        command.validate(self.recovery_batch_limit)?;
        let mut transaction = self.transaction.begin().await?;
        let rows = sqlx::query(
            r#"
            SELECT link.tenant_id, link.node_id AS child_link_id,
                   link.run_id AS parent_run_id, link.related_run_id AS child_run_id,
                   parent.root_run_id, child.terminal_at AS scan_sort_at
            FROM insight_platform.run_nodes AS link
            JOIN insight_platform.runs AS parent
              ON parent.tenant_id = link.tenant_id AND parent.run_id = link.run_id
            JOIN insight_platform.runs AS child
              ON child.tenant_id = link.tenant_id AND child.run_id = link.related_run_id
            JOIN insight_platform.run_nodes AS node
              ON node.tenant_id = link.tenant_id AND node.node_id = link.parent_node_id
            WHERE link.record_kind = 'child_run_link'
              AND link.state IN ('running', 'waiting', 'cancelling')
              AND link.terminal_at IS NULL
              AND child.state IN ('succeeded', 'failed', 'cancelled', 'timed_out')
              AND child.terminal_at IS NOT NULL
              AND parent.state IN ('running', 'waiting', 'cancelling')
              AND parent.terminal_at IS NULL
              AND node.record_kind = 'node_execution' AND node.state = 'waiting'
              AND node.terminal_at IS NULL
            AND ($2::timestamptz IS NULL OR (child.terminal_at,link.tenant_id,link.node_id)>($2,$3::text,$4::text))
            ORDER BY child.terminal_at, link.tenant_id, link.node_id
            LIMIT $1
            "#,
        )
        .bind(i64::from(command.limit))
        .bind(command.after.as_ref().map(|c|c.sort_at))
        .bind(command.after.as_ref().map(|c|c.tenant_id.to_string()))
        .bind(command.after.as_ref().map(|c|c.item_id.to_string()))
        .fetch_all(&mut *transaction)
        .await?;
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| {
                safety_scan_cursor_from_row(row, "child_link_id", ResourceKind::ChildRunLink)
            })
            .transpose()?;
        let mut diagnostics = Vec::new();
        let candidates = rows
            .into_iter()
            .map(|row| {
                Ok(TerminalChildRunCandidate {
                    tenant_id: row.try_get("tenant_id")?,
                    child_link_id: row.try_get("child_link_id")?,
                    parent_run_id: row.try_get("parent_run_id")?,
                    child_run_id: row.try_get("child_run_id")?,
                    root_run_id: row.try_get("root_run_id")?,
                })
            })
            .collect::<Result<Vec<_>, RepositoryError>>()?;
        let mut resolved = Vec::with_capacity(candidates.len());
        for (candidate, slot) in candidates.into_iter().zip(command.slots.iter()) {
            let mut object_transaction = transaction.begin().await?;
            let result: Result<(), RepositoryError> = async {
                let tenant_id: ResourceId = candidate.tenant_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?;
                let root_run_id: ResourceId = candidate.root_run_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?;
                let parent_run_id: ResourceId = candidate.parent_run_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?;
                let child_run_id: ResourceId = candidate.child_run_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?;
                if root_run_id != parent_run_id {
                    load_run_for_update(&mut object_transaction, &tenant_id, &root_run_id).await?;
                }
                let parent_run =
                    load_run_for_update(&mut object_transaction, &tenant_id, &parent_run_id)
                        .await?;
                let child_run =
                    load_run_for_update(&mut object_transaction, &tenant_id, &child_run_id).await?;
                let child_link = load_child_run_link_for_update(
                    &mut object_transaction,
                    &candidate.tenant_id,
                    &candidate.child_link_id,
                )
                .await?;
                if child_link.parent_run_id != parent_run.run_id
                    || child_link.child_run_id != child_run.run_id
                    || child_run.root_run_id != root_run_id.to_string()
                    || child_run.parent_run_id.as_deref() != Some(parent_run.run_id.as_str())
                    || child_run.parent_node_id.as_deref()
                        != Some(child_link.parent_node_execution_id.as_str())
                {
                    return Err(RepositoryError::Conflict("terminal child Run relation"));
                }
                let (parent_node_version, parent_scope_id) = lock_waiting_child_parent_node(
                    &mut object_transaction,
                    &candidate.tenant_id,
                    &parent_run.run_id,
                    &child_link.parent_node_execution_id,
                )
                .await?;
                let source_job = load_task_source_orchestration_job(
                    &mut object_transaction,
                    &candidate.tenant_id,
                    &child_link.parent_node_execution_id,
                )
                .await?;
                let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
                    .fetch_one(&mut *object_transaction)
                    .await?;
                let child_state = child_run
                    .state
                    .parse::<RunState>()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
                let next_link = decide_child_link_terminal(
                    &child_link_projection(&child_link)?,
                    u64::try_from(child_link.generation).map_err(|_| {
                        RepositoryError::CorruptRow("negative ChildRunLink generation".to_owned())
                    })?,
                    u64::try_from(child_link.version).map_err(|_| {
                        RepositoryError::CorruptRow("negative ChildRunLink version".to_owned())
                    })?,
                    child_state,
                    database_now,
                )?;
                let converging = parent_run.state == RunState::Cancelling.as_str()
                    || parent_run.current.control.cancel_requested_at.is_some()
                    || parent_run.current.control.timeout_requested_at.is_some()
                    || parent_run.deadline <= database_now;
                let produced = async {
                    let result = if converging {
                        mutate_terminal_child_run(
                            &mut object_transaction,
                            &parent_run,
                            parent_node_version,
                            &parent_scope_id,
                            &source_job,
                            &child_run,
                            &child_link,
                            &next_link,
                            slot,
                            true,
                            database_now,
                        )
                        .await?
                    } else {
                        settle_terminal_child_run(
                            &mut object_transaction,
                            &parent_run,
                            &child_run,
                            &child_link,
                            &next_link,
                            slot,
                            self.scope_environment_limits,
                            database_now,
                        )
                        .await?
                    };
                    append_terminal_child_run_events(
                        &mut object_transaction,
                        &result,
                        slot,
                        converging,
                    )
                    .await?;
                    Ok(result)
                }
                .await;
                resolved.push(crate::recovery_isolation::produced(produced)?);
                Ok(())
            }
            .await;
            match result {
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
            safety_scan_page(resolved, scanned_count, command.limit, last_cursor)
                .with_diagnostics(diagnostics),
        )
    }

    pub async fn drive_child_run_cancellations(
        &mut self,
        command: DriveChildRunCancellations,
    ) -> Result<SafetyScanPage<CancellingOrchestrationChildRun>, RepositoryError> {
        command.validate(self.recovery_batch_limit)?;
        let mut transaction = self.transaction.begin().await?;
        let rows = sqlx::query(
            r#"
            SELECT link.tenant_id, link.node_id AS child_link_id,
                   link.run_id AS parent_run_id, link.related_run_id AS child_run_id,
                   parent.root_run_id, link.created_at AS scan_sort_at
            FROM insight_platform.run_nodes AS link
            JOIN insight_platform.runs AS parent
              ON parent.tenant_id = link.tenant_id AND parent.run_id = link.run_id
            JOIN insight_platform.runs AS child
              ON child.tenant_id = link.tenant_id AND child.run_id = link.related_run_id
            JOIN insight_platform.run_nodes AS node
              ON node.tenant_id = link.tenant_id AND node.node_id = link.parent_node_id
            WHERE link.record_kind = 'child_run_link'
              AND link.state IN ('running', 'waiting') AND link.terminal_at IS NULL
              AND parent.state = 'cancelling' AND parent.terminal_at IS NULL
              AND (parent.current_payload #> '{control,cancel_requested_at}' <> 'null'::jsonb
                OR parent.current_payload #> '{control,timeout_requested_at}' <> 'null'::jsonb
                OR parent.current_payload -> 'failure' <> 'null'::jsonb)
              AND child.state IN ('queued', 'running', 'waiting', 'cancelling')
              AND child.terminal_at IS NULL
              AND node.record_kind = 'node_execution' AND node.state = 'waiting'
              AND node.terminal_at IS NULL
            AND ($2::timestamptz IS NULL OR (link.created_at,link.tenant_id,link.node_id)>($2,$3::text,$4::text))
            ORDER BY link.created_at, link.tenant_id, link.node_id
            LIMIT $1
            "#,
        )
        .bind(i64::from(command.limit))
        .bind(command.after.as_ref().map(|c|c.sort_at))
        .bind(command.after.as_ref().map(|c|c.tenant_id.to_string()))
        .bind(command.after.as_ref().map(|c|c.item_id.to_string()))
        .fetch_all(&mut *transaction)
        .await?;
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| {
                safety_scan_cursor_from_row(row, "child_link_id", ResourceKind::ChildRunLink)
            })
            .transpose()?;
        let mut diagnostics = Vec::new();
        let candidates = rows
            .into_iter()
            .map(|row| {
                Ok(TerminalChildRunCandidate {
                    tenant_id: row.try_get("tenant_id")?,
                    child_link_id: row.try_get("child_link_id")?,
                    parent_run_id: row.try_get("parent_run_id")?,
                    child_run_id: row.try_get("child_run_id")?,
                    root_run_id: row.try_get("root_run_id")?,
                })
            })
            .collect::<Result<Vec<_>, RepositoryError>>()?;
        let mut cancelling = Vec::with_capacity(candidates.len());
        for (candidate, slot) in candidates.into_iter().zip(command.slots.iter()) {
            let mut object_transaction = transaction.begin().await?;
            let result: Result<(), RepositoryError> = async {
                let tenant_id: ResourceId = candidate.tenant_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?;
                let root_run_id: ResourceId = candidate.root_run_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?;
                let parent_run_id: ResourceId = candidate.parent_run_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?;
                let child_run_id: ResourceId = candidate.child_run_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?;
                if root_run_id != parent_run_id {
                    load_run_for_update(&mut object_transaction, &tenant_id, &root_run_id).await?;
                }
                let parent_run =
                    load_run_for_update(&mut object_transaction, &tenant_id, &parent_run_id)
                        .await?;
                let child_run =
                    load_run_for_update(&mut object_transaction, &tenant_id, &child_run_id).await?;
                let child_link = load_child_run_link_for_update(
                    &mut object_transaction,
                    &candidate.tenant_id,
                    &candidate.child_link_id,
                )
                .await?;
                if parent_run.state != RunState::Cancelling.as_str()
                    || child_link.parent_run_id != parent_run.run_id
                    || child_link.child_run_id != child_run.run_id
                    || child_run.root_run_id != root_run_id.to_string()
                    || child_run.parent_run_id.as_deref() != Some(parent_run.run_id.as_str())
                {
                    return Err(RepositoryError::Conflict("child Run cancellation relation"));
                }
                let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
                    .fetch_one(&mut *object_transaction)
                    .await?;
                let next_link = decide_child_link_cancel(
                    &child_link_projection(&child_link)?,
                    u64::try_from(child_link.generation).map_err(|_| {
                        RepositoryError::CorruptRow("negative ChildRunLink generation".to_owned())
                    })?,
                    u64::try_from(child_link.version).map_err(|_| {
                        RepositoryError::CorruptRow("negative ChildRunLink version".to_owned())
                    })?,
                )?;
                let Some(next_child_current) =
                    insight_platform_orchestrator::propagate_parent_convergence(
                        parent_run.deadline,
                        &parent_run.current,
                        child_run.state.parse().map_err(
                            |e: insight_platform_contracts::state::StateParseError| {
                                RepositoryError::CorruptRow(e.to_string())
                            },
                        )?,
                        u64::try_from(child_run.version)
                            .map_err(|_| RepositoryError::CorruptRow("child Run version".into()))?,
                        child_run.deadline,
                        &child_run.current,
                        database_now,
                    )?
                else {
                    return Ok(());
                };
                let produced = async {
                    let result = mutate_cancelling_child_run(
                        &mut object_transaction,
                        &parent_run,
                        &child_run,
                        &child_link,
                        &next_link,
                        next_child_current,
                        database_now,
                    )
                    .await?;
                    append_cancelling_child_run_events(&mut object_transaction, &result, slot)
                        .await?;
                    Ok(result)
                }
                .await;
                cancelling.push(crate::recovery_isolation::produced(produced)?);
                Ok(())
            }
            .await;
            match result {
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
            safety_scan_page(cancelling, scanned_count, command.limit, last_cursor)
                .with_diagnostics(diagnostics),
        )
    }
}
