//! PostgreSQL controller queries. Shared locks and atomicity remain in this adapter.
use super::*;

impl PgRepository {
    /// Loads the immutable value authorities needed by one expression controller. The returned
    /// Artifact references are intentionally not materialized here: object I/O occurs outside the
    /// database transaction and the eventual controller commit revalidates these exact facts.
    pub async fn load_expression_controller_facts(
        &self,
        fence: &JobFence,
        plan: &RuntimePlan,
    ) -> Result<ExpressionControllerFacts, RepositoryError> {
        fence.validate()?;
        plan.validate(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let plan_digest = plan
            .canonical_digest(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let job = load_job_by_text(&mut transaction, &fence.tenant_id, &fence.job_id).await?;
        require_orchestration_job(&job)?;
        require_exact_running_job_fence(&job, fence, database_now)?;
        let run_id: ResourceId = job
            .run_id
            .as_deref()
            .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Run".to_owned()))?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
        let tenant_id: ResourceId = fence.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let run = load_run(&mut transaction, &tenant_id, &run_id).await?;
        require_exact_runtime_plan(&mut transaction, &run, plan, &plan_digest).await?;
        let node_id = job.node_id.as_deref().ok_or_else(|| {
            RepositoryError::CorruptRow("orchestration Job has no Node".to_owned())
        })?;
        let node_row = sqlx::query(
            r#"
            SELECT node_id, plan_node_key, node_kind, scope_id, version,
                   parent_node_id, payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'running'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(run_id.to_string())
        .bind(node_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "running expression controller Node",
        ))?;
        let plan_node_key = PlanNodeKey::new(node_row.try_get("plan_node_key")?)?;
        let node = plan.node(&plan_node_key)?;
        let node_kind: PlanNodeKind = node_row
            .try_get::<String, _>("node_kind")?
            .parse::<PlanNodeKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if node.kind() != node_kind {
            return Err(RepositoryError::Conflict(
                "expression controller Plan binding",
            ));
        }
        let required_ports = required_expression_inputs(node)?;
        let scope_id: ResourceId = node_row.try_get::<String, _>("scope_id")?.parse().map_err(
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
        let value_refs = insight_platform_orchestrator::resolve_scope_inputs(
            &required_ports,
            &environments,
            self.scope_environment_limits,
        )?;
        let inputs = load_resolved_expression_values(
            &mut transaction,
            &tenant_id,
            &run_id,
            required_ports,
            value_refs,
        )
        .await?;
        let loop_iteration = expression_loop_iteration(node, &node_row)?;
        transaction.commit().await?;
        Ok(ExpressionControllerFacts {
            node_execution_id: node_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?,
            node_execution_version: node_row.try_get("version")?,
            plan_node_key,
            loop_iteration,
            inputs,
        })
    }

    /// Loads either immutable expression inputs or a controller observation reconstructed from
    /// committed scope state. Callers never choose the phase: the frozen Node payload and Plan do.
    pub async fn load_controller_facts(
        &self,
        fence: &JobFence,
        plan: &RuntimePlan,
    ) -> Result<ControllerFacts, RepositoryError> {
        fence.validate()?;
        plan.validate(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let plan_digest = plan
            .canonical_digest(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let job = load_job_by_text(&mut transaction, &fence.tenant_id, &fence.job_id).await?;
        require_orchestration_job(&job)?;
        require_exact_running_job_fence(&job, fence, database_now)?;
        let run_id: ResourceId = job
            .run_id
            .as_deref()
            .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Run".to_owned()))?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
        let tenant_id: ResourceId = fence.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let run = load_run(&mut transaction, &tenant_id, &run_id).await?;
        require_exact_runtime_plan(&mut transaction, &run, plan, &plan_digest).await?;
        let node_id = job.node_id.as_deref().ok_or_else(|| {
            RepositoryError::CorruptRow("orchestration Job has no Node".to_owned())
        })?;
        let row = sqlx::query(
            r#"
            SELECT node_id, plan_node_key, node_kind, scope_id, version,
                   parent_node_id, payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'running'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(run_id.to_string())
        .bind(node_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("running controller Node"))?;
        let plan_node_key = PlanNodeKey::new(row.try_get("plan_node_key")?)?;
        let runtime_node = plan.node(&plan_node_key)?;
        let node_kind = row
            .try_get::<String, _>("node_kind")?
            .parse::<PlanNodeKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if runtime_node.kind() != node_kind {
            return Err(RepositoryError::Conflict("controller Plan binding"));
        }
        let identity = ControllerFactIdentity {
            node_execution_id: node_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?,
            node_execution_version: row.try_get("version")?,
            plan_node_key,
        };
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let job_payload = decode_orchestration_job_payload(&job.payload)?;
        if job_payload.external_leaf_completion.is_some() {
            let source = ControllerSourceNode {
                plan_node_key: identity.plan_node_key.clone(),
                node_kind,
                scope_id: row.try_get("scope_id")?,
                version: identity.node_execution_version,
            };
            require_committed_external_leaf_completion(
                &mut transaction,
                &job,
                &run,
                &source,
                runtime_node,
                CompletionValidation {
                    plan,
                    context: self.context_query_limits,
                    model: self.model_turn_limits,
                    scope: self.scope_environment_limits,
                },
            )
            .await?;
            transaction.commit().await?;
            return Ok(ControllerFacts::Committed {
                identity,
                observation: ControllerObservation::ExternalLeafCompleted,
            });
        }
        if let insight_platform_plan::RuntimeNode::ChildAgentCall {
            child_agent_slot_id,
            input,
            candidate_route,
            ..
        } = runtime_node
        {
            let plan_slot = plan
                .dependency_slots
                .get(child_agent_slot_id)
                .ok_or(RepositoryError::Conflict("child Plan dependency slot"))?;
            let frozen_slot = run
                .bindings
                .slots
                .iter()
                .find(|slot| slot.slot_id == *child_agent_slot_id)
                .ok_or(RepositoryError::NotFound("frozen child Agent slot"))?;
            if plan_slot.requirement_digest != frozen_slot.requirement_digest {
                return Err(RepositoryError::Conflict("child Plan frozen requirement"));
            }
            let FrozenSlotTarget::ChildAgent {
                candidates,
                selection_policy,
            } = &frozen_slot.target
            else {
                return Err(RepositoryError::Conflict("frozen child Agent slot kind"));
            };
            let selection_document =
                load_exact_frozen_selection_policy(&mut transaction, &run, selection_policy, false)
                    .await?;
            let scope_id: ResourceId = row.try_get::<String, _>("scope_id")?.parse().map_err(
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
            let ports = std::iter::once(input.clone())
                .chain(candidate_route.iter().cloned())
                .collect::<Vec<_>>();
            let references = insight_platform_orchestrator::resolve_scope_inputs(
                &ports,
                &environments,
                self.scope_environment_limits,
            )?;
            let mut values = load_resolved_expression_values(
                &mut transaction,
                &tenant_id,
                &run_id,
                ports,
                references,
            )
            .await?;
            if values.is_empty() || values.len() > 2 {
                return Err(RepositoryError::CorruptRow(
                    "child dispatch input arity".to_owned(),
                ));
            }
            let input = values.remove(0);
            let route = values.pop();
            transaction.commit().await?;
            return Ok(ControllerFacts::ChildAgentDispatch(Box::new(
                ChildAgentDispatchFacts {
                    identity,
                    input,
                    route,
                    selection_policy: selection_policy.clone(),
                    selection_document,
                    candidates: candidates.clone(),
                },
            )));
        }
        if let insight_platform_plan::RuntimeNode::CapabilityCall {
            capability_slot_id,
            input,
            candidate_route,
            ..
        } = runtime_node
        {
            let plan_slot = plan
                .dependency_slots
                .get(capability_slot_id)
                .ok_or(RepositoryError::Conflict("Capability Plan dependency slot"))?;
            let frozen_slot = run
                .bindings
                .slots
                .iter()
                .find(|slot| slot.slot_id == *capability_slot_id)
                .ok_or(RepositoryError::NotFound("frozen Capability slot"))?;
            if plan_slot.requirement_digest != frozen_slot.requirement_digest {
                return Err(RepositoryError::Conflict(
                    "Capability Plan frozen requirement",
                ));
            }
            let FrozenSlotTarget::Capability {
                candidates,
                selection_policy,
                ..
            } = &frozen_slot.target
            else {
                return Err(RepositoryError::Conflict("frozen Capability slot kind"));
            };
            let selection_document =
                load_exact_frozen_selection_policy(&mut transaction, &run, selection_policy, false)
                    .await?;
            let scope_id: ResourceId = row.try_get::<String, _>("scope_id")?.parse().map_err(
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
            let ports = std::iter::once(input.clone())
                .chain(candidate_route.iter().cloned())
                .collect::<Vec<_>>();
            let references = insight_platform_orchestrator::resolve_scope_inputs(
                &ports,
                &environments,
                self.scope_environment_limits,
            )?;
            let mut values = load_resolved_expression_values(
                &mut transaction,
                &tenant_id,
                &run_id,
                ports,
                references,
            )
            .await?;
            if values.is_empty() || values.len() > 2 {
                return Err(RepositoryError::CorruptRow(
                    "Capability dispatch input arity".to_owned(),
                ));
            }
            let input = values.remove(0);
            let route = values.pop();
            transaction.commit().await?;
            return Ok(ControllerFacts::CapabilityDispatch(Box::new(
                CapabilityDispatchFacts {
                    identity,
                    input,
                    route,
                    selection_policy: selection_policy.clone(),
                    selection_document,
                    candidates: candidates.clone(),
                },
            )));
        }
        if let insight_platform_plan::RuntimeNode::ContextQuery {
            context_slot_id,
            request,
            ..
        } = runtime_node
        {
            let plan_slot = plan
                .dependency_slots
                .get(context_slot_id)
                .ok_or(RepositoryError::Conflict("Context Plan dependency slot"))?;
            let frozen_slot = run
                .bindings
                .slots
                .iter()
                .find(|slot| slot.slot_id == *context_slot_id)
                .ok_or(RepositoryError::NotFound("frozen Context slot"))?;
            if plan_slot.requirement_digest != frozen_slot.requirement_digest {
                return Err(RepositoryError::Conflict("Context Plan frozen requirement"));
            }
            if !matches!(frozen_slot.target, FrozenSlotTarget::Context { .. }) {
                return Err(RepositoryError::Conflict("frozen Context slot kind"));
            }
            let scope_id: ResourceId = row.try_get::<String, _>("scope_id")?.parse().map_err(
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
            let mut values = load_resolved_expression_values(
                &mut transaction,
                &tenant_id,
                &run_id,
                vec![request.clone()],
                references,
            )
            .await?;
            let input = values.pop().ok_or_else(|| {
                RepositoryError::CorruptRow("Context dispatch input missing".to_owned())
            })?;
            transaction.commit().await?;
            return Ok(ControllerFacts::ContextDispatch(Box::new(
                ContextDispatchFacts { identity, input },
            )));
        }
        if let insight_platform_plan::RuntimeNode::ModelLoop {
            model_slot_id,
            skill_slot_ids,
            capability_slot_ids,
            input,
            model_route,
            ..
        } = runtime_node
        {
            let plan_slot = plan
                .dependency_slots
                .get(model_slot_id)
                .ok_or(RepositoryError::Conflict("Model Plan dependency slot"))?;
            let frozen_slot = run
                .bindings
                .slots
                .iter()
                .find(|slot| slot.slot_id == *model_slot_id)
                .ok_or(RepositoryError::NotFound("frozen Model slot"))?;
            if plan_slot.requirement_digest != frozen_slot.requirement_digest {
                return Err(RepositoryError::Conflict("Model Plan frozen requirement"));
            }
            let FrozenSlotTarget::Model {
                candidates,
                selection_policy,
            } = &frozen_slot.target
            else {
                return Err(RepositoryError::Conflict("frozen Model slot kind"));
            };
            let selection_document =
                load_exact_frozen_selection_policy(&mut transaction, &run, selection_policy, false)
                    .await?;
            let mut tool_slots =
                Vec::with_capacity(skill_slot_ids.len() + capability_slot_ids.len());
            for (slot_id, expected_kind) in skill_slot_ids
                .iter()
                .map(|slot| (slot, "skill"))
                .chain(capability_slot_ids.iter().map(|slot| (slot, "capability")))
            {
                let plan_tool_slot = plan
                    .dependency_slots
                    .get(slot_id)
                    .ok_or(RepositoryError::Conflict("Model tool Plan dependency slot"))?;
                let frozen_tool_slot = run
                    .bindings
                    .slots
                    .iter()
                    .find(|slot| slot.slot_id == *slot_id)
                    .ok_or(RepositoryError::NotFound("frozen Model tool slot"))?;
                let kind_matches = matches!(
                    (&frozen_tool_slot.target, expected_kind),
                    (FrozenSlotTarget::Skill { .. }, "skill")
                        | (FrozenSlotTarget::Capability { .. }, "capability")
                );
                if plan_tool_slot.requirement_digest != frozen_tool_slot.requirement_digest
                    || !kind_matches
                {
                    return Err(RepositoryError::Conflict("Model tool frozen requirement"));
                }
                tool_slots.push(frozen_tool_slot.clone());
            }
            let scope_id: ResourceId = row.try_get::<String, _>("scope_id")?.parse().map_err(
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
            let ports = std::iter::once(input.clone())
                .chain(model_route.iter().cloned())
                .collect::<Vec<_>>();
            let references = insight_platform_orchestrator::resolve_scope_inputs(
                &ports,
                &environments,
                self.scope_environment_limits,
            )?;
            let mut values = load_resolved_expression_values(
                &mut transaction,
                &tenant_id,
                &run_id,
                ports,
                references,
            )
            .await?;
            if values.is_empty() || values.len() > 2 {
                return Err(RepositoryError::CorruptRow(
                    "Model dispatch input arity".to_owned(),
                ));
            }
            let input = values.remove(0);
            let route = values.pop();
            transaction.commit().await?;
            return Ok(ControllerFacts::ModelDispatch(Box::new(
                ModelDispatchFacts {
                    identity,
                    input,
                    route,
                    selection_policy: selection_policy.clone(),
                    selection_document,
                    candidates: candidates.clone(),
                    tool_slots,
                },
            )));
        }
        let observation = match runtime_node {
            insight_platform_plan::RuntimeNode::Map { .. }
                if payload.value.get("wait").is_some() =>
            {
                let pending: StoredPendingControllerNodePayload =
                    decode_typed_payload(&payload, "running Map controller")?;
                match &pending.wait {
                    StoredControllerWait::MapSettlement {
                        admitted_item_count,
                        ..
                    } => Some(
                        load_map_settlement_observation(
                            &mut transaction,
                            &job.tenant_id,
                            &run.run_id,
                            &pending,
                            *admitted_item_count,
                        )
                        .await?,
                    ),
                    StoredControllerWait::MapAdmission { .. } => None,
                    _ => return Err(RepositoryError::Conflict("Map controller wait kind")),
                }
            }
            insight_platform_plan::RuntimeNode::Join { .. } => Some(
                load_join_observation(&mut transaction, &job.tenant_id, &run.run_id, &payload)
                    .await?,
            ),
            insight_platform_plan::RuntimeNode::ErrorBoundary { .. } => {
                Some(ControllerObservation::ErrorBoundary { failure_code: None })
            }
            insight_platform_plan::RuntimeNode::HumanTask {
                definition,
                response,
                ..
            } => {
                if payload.value.get("task_id").is_none() {
                    Some(ControllerObservation::None)
                } else {
                    let wait: StoredHumanTaskWaitPayload =
                        decode_typed_payload(&payload, "running HumanTask wait Node")?;
                    if wait.plan_node_key != identity.plan_node_key
                        || wait.response_port != *response
                        || wait.task_id.kind()
                            != runtime_human_task_definition(definition)
                                .task_kind()
                                .task_id_kind()
                    {
                        return Err(RepositoryError::Conflict(
                            "HumanTask wait observation contract",
                        ));
                    }
                    Some(ControllerObservation::DurableWait {
                        wait_kind: DurableWaitKind::HumanTask,
                        outcome: wait
                            .resolution
                            .ok_or(RepositoryError::Conflict(
                                "unresolved HumanTask wait was claimed",
                            ))?
                            .outcome,
                    })
                }
            }
            insight_platform_plan::RuntimeNode::TimerWait { .. } => {
                if payload.value.get("due_at").is_none() {
                    Some(ControllerObservation::None)
                } else {
                    let wait: StoredTimerWaitPayload =
                        decode_typed_payload(&payload, "running Timer wait Node")?;
                    if wait.plan_node_key != identity.plan_node_key {
                        return Err(RepositoryError::Conflict("Timer wait observation contract"));
                    }
                    Some(ControllerObservation::DurableWait {
                        wait_kind: DurableWaitKind::Timer,
                        outcome: wait.resolution.ok_or(RepositoryError::Conflict(
                            "unresolved Timer wait was claimed",
                        ))?,
                    })
                }
            }
            insight_platform_plan::RuntimeNode::SignalWait {
                signal_key,
                payload: payload_port,
                ..
            } => {
                if payload.value.get("signal_key").is_none() {
                    Some(ControllerObservation::None)
                } else {
                    let wait: StoredSignalWaitPayload =
                        decode_typed_payload(&payload, "running Signal wait Node")?;
                    if wait.plan_node_key != identity.plan_node_key
                        || wait.signal_key != *signal_key
                        || wait.payload_port != *payload_port
                    {
                        return Err(RepositoryError::Conflict(
                            "Signal wait observation contract",
                        ));
                    }
                    Some(ControllerObservation::DurableWait {
                        wait_kind: DurableWaitKind::Signal,
                        outcome: wait
                            .resolution
                            .ok_or(RepositoryError::Conflict(
                                "unresolved Signal wait was claimed",
                            ))?
                            .outcome,
                    })
                }
            }
            insight_platform_plan::RuntimeNode::Start { .. }
            | insight_platform_plan::RuntimeNode::Fork { .. }
            | insight_platform_plan::RuntimeNode::ModelLoop { .. }
            | insight_platform_plan::RuntimeNode::CapabilityCall { .. }
            | insight_platform_plan::RuntimeNode::ContextQuery { .. }
            | insight_platform_plan::RuntimeNode::Return { .. }
            | insight_platform_plan::RuntimeNode::Raise { .. } => Some(ControllerObservation::None),
            _ => None,
        };
        transaction.rollback().await?;
        if let Some(observation) = observation {
            Ok(ControllerFacts::Committed {
                identity,
                observation,
            })
        } else {
            Ok(ControllerFacts::Expression(
                self.load_expression_controller_facts(fence, plan).await?,
            ))
        }
    }

    pub async fn load_model_tool_continuation_facts(
        &self,
        fence: &JobFence,
        plan: &RuntimePlan,
        continuation: &insight_platform_orchestrator::ModelToolContinuation,
    ) -> Result<ModelToolContinuationFacts, RepositoryError> {
        fence.validate()?;
        continuation
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        plan.validate(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let plan_digest = plan.canonical_digest(self.plan_limits)?;
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let job = load_job_by_text(&mut transaction, &fence.tenant_id, &fence.job_id).await?;
        require_orchestration_job(&job)?;
        require_exact_running_job_fence(&job, fence, database_now)?;
        let job_payload: OrchestrationJobPayload = decode_orchestration_job_payload(&job.payload)?;
        if job_payload.model_tool_continuation.as_ref() != Some(continuation) {
            return Err(RepositoryError::Conflict(
                "Model tool continuation Job payload",
            ));
        }
        let tenant_id: ResourceId = fence.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let run_id: ResourceId = job
            .run_id
            .as_deref()
            .ok_or_else(|| RepositoryError::CorruptRow("Model tool Run missing".to_owned()))?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
        let node_execution_id: ResourceId = job
            .node_id
            .as_deref()
            .ok_or_else(|| RepositoryError::CorruptRow("Model tool Node missing".to_owned()))?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
        if job_payload.node_execution_id != node_execution_id {
            return Err(RepositoryError::Conflict(
                "Model tool continuation Job owner",
            ));
        }
        let run = load_run(&mut transaction, &tenant_id, &run_id).await?;
        require_exact_runtime_plan(&mut transaction, &run, plan, &plan_digest).await?;
        let node_row = sqlx::query(
            r#"
            SELECT plan_node_key, node_kind, state, version,
                   payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution'
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(run_id.to_string())
        .bind(node_execution_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Model tool continuation Node"))?;
        let plan_node_key = PlanNodeKey::new(node_row.try_get("plan_node_key")?)?;
        let RuntimeNode::ModelLoop {
            capability_slot_ids,
            maximum_rounds,
            maximum_capability_calls,
            maximum_parallel_calls_per_round,
            token_budget,
            ..
        } = plan.node(&plan_node_key)?
        else {
            return Err(RepositoryError::Conflict(
                "Model tool continuation Plan node",
            ));
        };
        let stored_payload = payload_from_row(
            &node_row,
            "payload_schema_version",
            "payload",
            "payload_digest",
        )?;
        let wait: StoredModelToolContinuationWaitPayload =
            decode_typed_payload(&stored_payload, "Model tool continuation Node")?;
        if node_row.try_get::<String, _>("state")? != NodeExecutionState::Running.as_str()
            || node_row.try_get::<String, _>("node_kind")? != PlanNodeKind::ModelLoop.as_str()
            || wait.plan_node_key != plan_node_key
            || wait.plan_digest != plan_digest
            || wait.model_turn_id != continuation.model_turn_id
            || wait.response_value_id != continuation.response_value_id
            || wait.response_digest != continuation.response_digest
            || wait.round_ordinal != continuation.round_ordinal
            || wait.tool_intent_count != continuation.tool_intent_count
            || wait.maximum_rounds != *maximum_rounds
            || wait.maximum_capability_calls != *maximum_capability_calls
            || wait.maximum_parallel_calls_per_round != *maximum_parallel_calls_per_round
            || wait.token_budget == 0
            || wait.token_budget > *token_budget
            || wait.total_capability_calls > *maximum_capability_calls
        {
            return Err(RepositoryError::Conflict(
                "Model tool continuation frozen wait",
            ));
        }
        let turn = crate::model_turn_repository::load_model_turn_for_replay(
            &mut transaction,
            &tenant_id,
            &continuation.model_turn_id,
            self.model_turn_limits,
        )
        .await?;
        if turn.state != insight_platform_contracts::ModelTurnState::Succeeded
            || turn.run_id != run_id
            || turn.node_execution_id != node_execution_id
            || turn.round_ordinal != continuation.round_ordinal
            || turn.output_value_id.as_ref() != Some(&continuation.response_value_id)
        {
            return Err(RepositoryError::Conflict(
                "Model tool continuation ModelTurn",
            ));
        }
        let request_row = sqlx::query(
            r#"
            SELECT classification, schema_digest, inline_value, content_digest
            FROM insight_platform.run_values
            WHERE tenant_id = $1 AND run_id = $2 AND value_id = $3
              AND node_id = $4 AND value_kind = 'model_request' AND artifact_id IS NULL
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(run_id.to_string())
        .bind(turn.request_value_id.to_string())
        .bind(node_execution_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Model tool request RunValue"))?;
        let request_value: serde_json::Value = request_row.try_get("inline_value")?;
        let request: insight_platform_models::CanonicalModelRequest =
            serde_json::from_value(request_value.clone()).map_err(|failure| {
                RepositoryError::CorruptRow(format!("Model tool request: {failure}"))
            })?;
        if request.model_turn_id != turn.model_turn_id
            || request_row.try_get::<String, _>("content_digest")?
                != canonical_digest(&request_value)
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
        {
            return Err(RepositoryError::Conflict("Model tool request digest"));
        }
        let response_row = sqlx::query(
            r#"
            SELECT inline_value, content_digest FROM insight_platform.run_values
            WHERE tenant_id = $1 AND run_id = $2 AND value_id = $3
              AND node_id = $4 AND value_kind = 'model_response' AND artifact_id IS NULL
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(run_id.to_string())
        .bind(continuation.response_value_id.to_string())
        .bind(node_execution_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Model tool response RunValue"))?;
        let response_value: serde_json::Value = response_row.try_get("inline_value")?;
        let response: insight_platform_models::CanonicalModelResponse =
            serde_json::from_value(response_value.clone()).map_err(|failure| {
                RepositoryError::CorruptRow(format!("Model tool response: {failure}"))
            })?;
        if response.tool_intents.len() != usize::from(continuation.tool_intent_count)
            || response_row.try_get::<String, _>("content_digest")?
                != continuation.response_digest.to_string()
            || canonical_digest(&response_value)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
                != continuation.response_digest.to_string()
        {
            return Err(RepositoryError::Conflict("Model tool response digest"));
        }
        let projections = request
            .tools
            .iter()
            .map(|projection| (projection.projected_name.as_str(), projection))
            .collect::<BTreeMap<_, _>>();
        let mut calls = Vec::with_capacity(response.tool_intents.len());
        for intent in response.tool_intents {
            let projection = projections
                .get(intent.projected_tool_name.as_str())
                .ok_or(RepositoryError::Conflict("Model tool projection"))?;
            let mut matches = Vec::new();
            for slot_id in capability_slot_ids {
                let plan_slot = plan
                    .dependency_slots
                    .get(slot_id)
                    .ok_or(RepositoryError::Conflict("Model tool Plan slot"))?;
                let frozen_slot = run
                    .bindings
                    .slots
                    .iter()
                    .find(|slot| slot.slot_id == *slot_id)
                    .ok_or(RepositoryError::NotFound("frozen Model Capability slot"))?;
                if plan_slot.requirement_digest != frozen_slot.requirement_digest {
                    return Err(RepositoryError::Conflict("Model tool frozen requirement"));
                }
                let FrozenSlotTarget::Capability { candidates, .. } = &frozen_slot.target else {
                    return Err(RepositoryError::Conflict("Model tool slot kind"));
                };
                if let Some(ordinal) = candidates
                    .iter()
                    .position(|candidate| candidate == &projection.capability_deployment)
                {
                    matches.push((
                        slot_id.clone(),
                        ordinal,
                        projection.capability_deployment.clone(),
                    ));
                }
            }
            let [(slot_id, ordinal, selected_deployment)] = matches.as_slice() else {
                return Err(RepositoryError::Conflict(
                    "Model tool projection must map to one frozen slot",
                ));
            };
            calls.push(ModelToolIntentDispatchFact {
                call_id: intent.call_id,
                projected_tool_name: intent.projected_tool_name,
                arguments: intent.arguments,
                slot_id: slot_id.clone(),
                selected_candidate_ordinal: u16::try_from(*ordinal).map_err(|_| {
                    RepositoryError::CorruptRow("Model tool candidate ordinal".to_owned())
                })?,
                selected_deployment: selected_deployment.clone(),
            });
        }
        transaction.commit().await?;
        Ok(ModelToolContinuationFacts {
            tenant_id,
            run_id,
            node_execution_id,
            node_execution_version: u64::try_from(node_row.try_get::<i64, _>("version")?)
                .map_err(|_| RepositoryError::CorruptRow("Model tool Node version".to_owned()))?,
            model_turn_id: continuation.model_turn_id.clone(),
            round_ordinal: continuation.round_ordinal,
            calls,
        })
    }

    pub async fn load_model_tool_result_continuation_facts(
        &self,
        fence: &JobFence,
        plan: &RuntimePlan,
        continuation: &insight_platform_orchestrator::ModelToolContinuation,
    ) -> Result<ModelToolResultContinuationFacts, RepositoryError> {
        fence.validate()?;
        continuation
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        if continuation.results.is_empty() {
            return Err(RepositoryError::InvalidInput(
                "Model tool result continuation is empty".to_owned(),
            ));
        }
        let plan_digest = plan.canonical_digest(self.plan_limits)?;
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let job = load_job_by_text(&mut transaction, &fence.tenant_id, &fence.job_id).await?;
        require_orchestration_job(&job)?;
        require_exact_running_job_fence(&job, fence, database_now)?;
        let job_payload: OrchestrationJobPayload = decode_orchestration_job_payload(&job.payload)?;
        if job_payload.model_tool_continuation.as_ref() != Some(continuation) {
            return Err(RepositoryError::Conflict(
                "Model tool result continuation Job payload",
            ));
        }
        let tenant_id: ResourceId = fence.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let run_id: ResourceId = job
            .run_id
            .as_deref()
            .ok_or_else(|| RepositoryError::CorruptRow("Model tool result Run".to_owned()))?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
        let node_execution_id: ResourceId = job
            .node_id
            .as_deref()
            .ok_or_else(|| RepositoryError::CorruptRow("Model tool result Node".to_owned()))?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
        let run = load_run(&mut transaction, &tenant_id, &run_id).await?;
        require_exact_runtime_plan(&mut transaction, &run, plan, &plan_digest).await?;
        let node_row = sqlx::query(
            r#"
            SELECT plan_node_key, node_kind, state, version,
                   payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution'
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(run_id.to_string())
        .bind(node_execution_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "Model tool result continuation Node",
        ))?;
        let plan_node_key = PlanNodeKey::new(node_row.try_get("plan_node_key")?)?;
        let RuntimeNode::ModelLoop { .. } = plan.node(&plan_node_key)? else {
            return Err(RepositoryError::Conflict(
                "Model tool result continuation Plan node",
            ));
        };
        if node_row.try_get::<String, _>("state")? != NodeExecutionState::Running.as_str()
            || node_row.try_get::<String, _>("node_kind")? != PlanNodeKind::ModelLoop.as_str()
        {
            return Err(RepositoryError::Conflict(
                "Model tool result continuation Node state",
            ));
        }
        let stored_payload = payload_from_row(
            &node_row,
            "payload_schema_version",
            "payload",
            "payload_digest",
        )?;
        let wait: StoredModelToolBatchWaitPayload =
            decode_typed_payload(&stored_payload, "Model tool result batch")?;
        let expected_results = wait
            .calls
            .iter()
            .map(|call| {
                call.result.clone().ok_or(RepositoryError::Conflict(
                    "incomplete Model tool result batch",
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if wait.continuation.plan_node_key != plan_node_key
            || wait.continuation.plan_digest != plan_digest
            || wait.continuation.model_turn_id != continuation.model_turn_id
            || wait.continuation.response_value_id != continuation.response_value_id
            || wait.continuation.response_digest != continuation.response_digest
            || wait.continuation.round_ordinal != continuation.round_ordinal
            || wait.continuation.tool_intent_count != continuation.tool_intent_count
            || expected_results != continuation.results
        {
            return Err(RepositoryError::Conflict(
                "Model tool result continuation frozen batch",
            ));
        }
        let turn = crate::model_turn_repository::load_model_turn_for_replay(
            &mut transaction,
            &tenant_id,
            &continuation.model_turn_id,
            self.model_turn_limits,
        )
        .await?;
        if turn.state != insight_platform_contracts::ModelTurnState::Succeeded
            || turn.run_id != run_id
            || turn.node_execution_id != node_execution_id
            || turn.round_ordinal != continuation.round_ordinal
            || turn.output_value_id.as_ref() != Some(&continuation.response_value_id)
        {
            return Err(RepositoryError::Conflict(
                "Model tool result continuation ModelTurn",
            ));
        }
        let request_row = sqlx::query(
            r#"
            SELECT classification, schema_digest, inline_value, content_digest
            FROM insight_platform.run_values
            WHERE tenant_id = $1 AND run_id = $2 AND value_id = $3
              AND node_id = $4 AND value_kind = 'model_request' AND artifact_id IS NULL
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(run_id.to_string())
        .bind(turn.request_value_id.to_string())
        .bind(node_execution_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("previous Model request RunValue"))?;
        let request_value: Value = request_row.try_get("inline_value")?;
        let previous_request_document: insight_platform_models::CanonicalModelRequest =
            serde_json::from_value(request_value.clone()).map_err(|failure| {
                RepositoryError::CorruptRow(format!("previous Model request: {failure}"))
            })?;
        let request_content_digest: Sha256Digest = request_row
            .try_get::<String, _>("content_digest")?
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("previous request digest".to_owned()))?;
        if previous_request_document.model_turn_id != continuation.model_turn_id
            || request_row.try_get::<String, _>("content_digest")?
                != canonical_digest(&request_value)
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
        {
            return Err(RepositoryError::Conflict("previous Model request digest"));
        }
        let previous_request = insight_platform_models::ModelRequestValue {
            value_id: turn.request_value_id.clone(),
            classification: request_row
                .try_get::<String, _>("classification")?
                .parse::<DataClassification>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
            schema_digest: request_row
                .try_get::<String, _>("schema_digest")?
                .parse()
                .map_err(|_| RepositoryError::CorruptRow("previous request schema".to_owned()))?,
            content_digest: request_content_digest,
            value: ValueRef::Inline {
                value: request_value,
            },
            request: previous_request_document,
        };
        let mut results = Vec::with_capacity(continuation.results.len());
        for exact in &continuation.results {
            let row = sqlx::query(
                r#"
                SELECT value.classification, value.schema_digest, value.content_digest,
                       value.inline_value, value.artifact_id,
                       artifact.state AS artifact_state,
                       artifact.terminal_at AS artifact_terminal_at,
                       artifact.classification AS artifact_classification,
                       artifact.verified_media_type, blob.state AS blob_state,
                       blob.deleted_at AS blob_deleted_at,
                       blob.content_digest AS blob_content_digest, blob.size_bytes
                FROM insight_platform.run_values AS value
                LEFT JOIN insight_platform.artifacts AS artifact
                  ON artifact.tenant_id = value.tenant_id AND artifact.artifact_id = value.artifact_id
                LEFT JOIN insight_platform.artifact_blobs AS blob
                  ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
                WHERE value.tenant_id = $1 AND value.run_id = $2 AND value.value_id = $3
                  AND value.node_id = $4 AND value.value_kind = 'capability_output'
                "#,
            )
            .bind(tenant_id.to_string())
            .bind(run_id.to_string())
            .bind(exact.output_value_id.to_string())
            .bind(node_execution_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(RepositoryError::Conflict("Model tool output RunValue"))?;
            let classification = row
                .try_get::<String, _>("classification")?
                .parse::<DataClassification>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let schema_digest: Sha256Digest = row
                .try_get::<String, _>("schema_digest")?
                .parse()
                .map_err(|_| RepositoryError::CorruptRow("tool result schema digest".to_owned()))?;
            let content_digest: Sha256Digest = row
                .try_get::<String, _>("content_digest")?
                .parse()
                .map_err(|_| {
                    RepositoryError::CorruptRow("tool result content digest".to_owned())
                })?;
            if classification != exact.classification
                || schema_digest != exact.schema_digest
                || content_digest != exact.content_digest
            {
                return Err(RepositoryError::Conflict("Model tool result evidence"));
            }
            let inline_value: Option<Value> = row.try_get("inline_value")?;
            let artifact_id: Option<String> = row.try_get("artifact_id")?;
            let value = match (inline_value, artifact_id) {
                (Some(value), None)
                    if canonical_digest(&value)
                        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
                        == content_digest.to_string() =>
                {
                    ValueRef::Inline { value }
                }
                (None, Some(artifact_id))
                    if row
                        .try_get::<Option<String>, _>("artifact_state")?
                        .as_deref()
                        == Some("ready")
                        && row
                            .try_get::<Option<DateTime<Utc>>, _>("artifact_terminal_at")?
                            .is_none()
                        && row.try_get::<Option<String>, _>("blob_state")?.as_deref()
                            == Some("verified")
                        && row
                            .try_get::<Option<DateTime<Utc>>, _>("blob_deleted_at")?
                            .is_none()
                        && row
                            .try_get::<Option<String>, _>("blob_content_digest")?
                            .as_deref()
                            == Some(content_digest.as_str())
                        && row
                            .try_get::<Option<String>, _>("artifact_classification")?
                            .as_deref()
                            == Some(classification.as_str()) =>
                {
                    ValueRef::Artifact {
                        artifact: ArtifactRef::new(
                            artifact_id.parse().map_err(|_| {
                                RepositoryError::CorruptRow("tool result Artifact ID".to_owned())
                            })?,
                            content_digest.clone(),
                            u64::try_from(row.try_get::<i64, _>("size_bytes")?).map_err(|_| {
                                RepositoryError::CorruptRow("tool result Artifact size".to_owned())
                            })?,
                            row.try_get::<String, _>("verified_media_type")?,
                            classification,
                            None,
                        )
                        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                    }
                }
                _ => return Err(RepositoryError::Conflict("Model tool result storage")),
            };
            results.push(insight_platform_models::ModelToolResult {
                call_id: exact.call_id.clone(),
                invocation_id: exact.invocation_id.clone(),
                output_value_id: exact.output_value_id.clone(),
                output_schema_digest: exact.schema_digest.clone(),
                content_digest: exact.content_digest.clone(),
                classification,
                value,
            });
        }
        transaction.commit().await?;
        Ok(ModelToolResultContinuationFacts {
            tenant_id,
            run_id,
            node_execution_id,
            node_execution_version: u64::try_from(node_row.try_get::<i64, _>("version")?)
                .map_err(|_| RepositoryError::CorruptRow("Model tool Node version".to_owned()))?,
            previous_request,
            results,
            requested_attempt_limit: turn.payload.admission.attempt_limit,
            cost_ceiling_microunits: turn.payload.admission.quota_ceiling.cost_microunits,
        })
    }

    /// Resolves the one immutable RunValue selected by the currently running Return/Raise node.
    /// Artifact-backed values remain references here so the Scheduler can materialize them outside
    /// the database transaction; `commit_plan_terminal` repeats every authority check.
    pub async fn load_plan_terminal_value(
        &self,
        fence: &JobFence,
        plan: &RuntimePlan,
    ) -> Result<ResolvedExpressionInput, RepositoryError> {
        fence.validate()?;
        plan.validate(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let plan_digest = plan
            .canonical_digest(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let job = load_job_by_text(&mut transaction, &fence.tenant_id, &fence.job_id).await?;
        require_orchestration_job(&job)?;
        require_exact_running_job_fence(&job, fence, database_now)?;
        let run_id: ResourceId = job
            .run_id
            .as_deref()
            .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Run".to_owned()))?
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
        let tenant_id: ResourceId = fence.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        let run = load_run(&mut transaction, &tenant_id, &run_id).await?;
        require_exact_runtime_plan(&mut transaction, &run, plan, &plan_digest).await?;
        let node_id = job.node_id.as_deref().ok_or_else(|| {
            RepositoryError::CorruptRow("orchestration Job has no Node".to_owned())
        })?;
        let row = sqlx::query(
            r#"
            SELECT plan_node_key, node_kind, scope_id
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'running'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(run_id.to_string())
        .bind(node_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("running terminal Node"))?;
        let plan_node_key = PlanNodeKey::new(row.try_get("plan_node_key")?)?;
        let node = plan.node(&plan_node_key)?;
        let node_kind: PlanNodeKind = row
            .try_get::<String, _>("node_kind")?
            .parse::<PlanNodeKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if node.kind() != node_kind {
            return Err(RepositoryError::Conflict("terminal Plan binding"));
        }
        let port = match node {
            insight_platform_plan::RuntimeNode::Return { value } => value.clone(),
            insight_platform_plan::RuntimeNode::Raise { failure } => failure.clone(),
            _ => return Err(RepositoryError::Conflict("running Node is not terminal")),
        };
        let scope_id: ResourceId = row.try_get::<String, _>("scope_id")?.parse().map_err(
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
            std::slice::from_ref(&port),
            &environments,
            self.scope_environment_limits,
        )?;
        let mut values = load_resolved_expression_values(
            &mut transaction,
            &tenant_id,
            &run_id,
            vec![port],
            references,
        )
        .await?;
        let value = values
            .pop()
            .ok_or_else(|| RepositoryError::CorruptRow("terminal RunValue missing".to_owned()))?;
        transaction.commit().await?;
        Ok(value)
    }

    /// Plans the exact identity-slot shape for one controller commit from the same durable facts
    /// that the owner transaction will revalidate. This method never allocates identities and
    /// never mutates business state; callers may only use the returned bounded shape to allocate
    /// opaque IDs before calling the fenced commit API.
    pub async fn load_controller_mutation_requirements(
        &self,
        fence: &JobFence,
        plan: &RuntimePlan,
        observation: &ControllerObservation,
    ) -> Result<ControllerMutationRequirements, RepositoryError> {
        fence.validate()?;
        plan.validate(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let plan_digest = plan
            .canonical_digest(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let observed =
            load_job_for_update_by_text(&mut transaction, &fence.tenant_id, &fence.job_id).await?;
        require_orchestration_job(&observed)?;
        require_exact_running_job_fence(&observed, fence, database_now)?;
        let parents = lock_running_orchestration_job_parents(&mut transaction, &observed).await?;
        require_exact_runtime_plan(&mut transaction, &parents.run, plan, &plan_digest).await?;
        let source_node =
            load_controller_source_node(&mut transaction, &observed, &parents, plan).await?;
        let runtime_node = plan.node(&source_node.plan_node_key)?;
        require_exact_controller_observation(
            &mut transaction,
            &observed,
            &parents,
            &source_node,
            runtime_node,
            observation,
            CompletionValidation {
                plan,
                context: self.context_query_limits,
                model: self.model_turn_limits,
                scope: self.scope_environment_limits,
            },
        )
        .await?;
        let decision = decide_controller(runtime_node, observation)?;
        let shape = derive_controller_step_shape(
            &mut transaction,
            &observed,
            &parents,
            &source_node,
            plan,
            runtime_node,
            &decision,
            self.plan_limits.maximum_fan_out,
        )
        .await?;
        let mut requirements = shape.mutation_requirements(runtime_node)?;
        if let ControllerStepShape::LoopIterationExit {
            loop_plan_node_key, ..
        } = &shape
        {
            let insight_platform_plan::RuntimeNode::Loop { carried_ports, .. } =
                plan.node(loop_plan_node_key)?
            else {
                return Err(RepositoryError::Conflict("Loop continuation Plan node"));
            };
            requirements.structural_exit = ControllerStructuralRequirement::LoopRollover {
                carried_value_count: carried_ports.len(),
            };
        }
        requirements.remainder_cancellation_scope_ids = load_controller_remainder_requirements(
            &mut transaction,
            &observed,
            &parents,
            plan,
            &shape,
            ChildOutcome::Succeeded,
        )
        .await?;
        transaction.rollback().await?;
        Ok(requirements)
    }

    /// Plans the exact bounded mutation shape for a controller-derived terminal failure. The
    /// failure decision, ErrorBoundary route, structured exit and active sibling set all come
    /// from the same locked durable facts that the owner transaction revalidates.
    pub async fn load_controller_failure_mutation_requirements(
        &self,
        fence: &JobFence,
        plan: &RuntimePlan,
        observation: &ControllerObservation,
    ) -> Result<ControllerMutationRequirements, RepositoryError> {
        fence.validate()?;
        plan.validate(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let plan_digest = plan
            .canonical_digest(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let observed =
            load_job_for_update_by_text(&mut transaction, &fence.tenant_id, &fence.job_id).await?;
        require_orchestration_job(&observed)?;
        require_exact_running_job_fence(&observed, fence, database_now)?;
        let parents = lock_running_orchestration_job_parents(&mut transaction, &observed).await?;
        require_exact_runtime_plan(&mut transaction, &parents.run, plan, &plan_digest).await?;
        let source_node =
            load_controller_source_node(&mut transaction, &observed, &parents, plan).await?;
        let runtime_node = plan.node(&source_node.plan_node_key)?;
        require_exact_controller_observation(
            &mut transaction,
            &observed,
            &parents,
            &source_node,
            runtime_node,
            observation,
            CompletionValidation {
                plan,
                context: self.context_query_limits,
                model: self.model_turn_limits,
                scope: self.scope_environment_limits,
            },
        )
        .await?;
        let cause = OrchestrationFailureCause::Controller {
            observation: observation.clone(),
        };
        let (failure, _) = derive_orchestration_failure(&cause, runtime_node)?;
        let error_route = find_matching_error_boundary(
            &mut transaction,
            &parents.run,
            &parents.node_id,
            plan,
            &failure,
            self.plan_limits.maximum_nodes,
        )
        .await?;
        let shape = if error_route.is_none() {
            if let Some(exit) =
                load_parallel_leg_exit(&mut transaction, &observed, &parents, &source_node, None)
                    .await?
            {
                Some(exit)
            } else {
                load_map_item_exit(&mut transaction, &observed, &parents, &source_node, None)
                    .await?
            }
        } else {
            None
        };
        let remainder_cancellation_scope_ids = if let Some(shape) = &shape {
            load_controller_remainder_requirements(
                &mut transaction,
                &observed,
                &parents,
                plan,
                shape,
                ChildOutcome::Failed,
            )
            .await?
        } else {
            Vec::new()
        };
        let requirements = if error_route.is_some() {
            ControllerMutationRequirements {
                activation_scopes: vec![false],
                pending_node_count: 0,
                structural_exit: ControllerStructuralRequirement::None,
                pending_wake: false,
                remainder_cancellation_scope_ids,
            }
        } else {
            ControllerMutationRequirements {
                activation_scopes: Vec::new(),
                pending_node_count: 0,
                structural_exit: ControllerStructuralRequirement::Close,
                pending_wake: shape.is_some(),
                remainder_cancellation_scope_ids,
            }
        };
        transaction.rollback().await?;
        Ok(requirements)
    }

    /// Plans the exact mutation shape for a typed failure. External-leaf convergence must bind the
    /// failure already committed in the Job payload; a controller admission rejection instead
    /// proves an untouched initial Job payload and is closed again by `derive_orchestration_failure`.
    pub async fn load_failure_mutation_requirements(
        &self,
        fence: &JobFence,
        plan: &RuntimePlan,
        cause: &OrchestrationFailureCause,
    ) -> Result<ControllerMutationRequirements, RepositoryError> {
        fence.validate()?;
        cause.validate()?;
        plan.validate(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let plan_digest = plan.canonical_digest(self.plan_limits)?;
        let mut transaction = self.pool.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let observed =
            load_job_for_update_by_text(&mut transaction, &fence.tenant_id, &fence.job_id).await?;
        require_orchestration_job(&observed)?;
        require_exact_running_job_fence(&observed, fence, database_now)?;
        let payload: OrchestrationJobPayload = decode_orchestration_job_payload(&observed.payload)?;
        if let OrchestrationFailureCause::Committed { failure } = cause {
            if payload.convergence_failure.as_ref() != Some(failure) {
                return Err(RepositoryError::Conflict("failure convergence Job payload"));
            }
        } else if payload.wake_contract.is_some()
            || payload.convergence_failure.is_some()
            || payload.model_tool_continuation.is_some()
            || payload.external_leaf_completion.is_some()
        {
            return Err(RepositoryError::Conflict(
                "admission failure orchestration Job payload",
            ));
        }
        let parents = lock_running_orchestration_job_parents(&mut transaction, &observed).await?;
        require_exact_runtime_plan(&mut transaction, &parents.run, plan, &plan_digest).await?;
        let source_node =
            load_controller_source_node(&mut transaction, &observed, &parents, plan).await?;
        let runtime_node = plan.node(&source_node.plan_node_key)?;
        let (derived_failure, _) = derive_orchestration_failure(cause, runtime_node)?;
        require_failure_references(&mut transaction, &parents.run, &derived_failure).await?;
        let error_route = find_matching_error_boundary(
            &mut transaction,
            &parents.run,
            &parents.node_id,
            plan,
            &derived_failure,
            self.plan_limits.maximum_nodes,
        )
        .await?;
        let shape = if error_route.is_none() {
            if let Some(exit) =
                load_parallel_leg_exit(&mut transaction, &observed, &parents, &source_node, None)
                    .await?
            {
                Some(exit)
            } else {
                load_map_item_exit(&mut transaction, &observed, &parents, &source_node, None)
                    .await?
            }
        } else {
            None
        };
        let remainder_cancellation_scope_ids = if let Some(shape) = &shape {
            load_controller_remainder_requirements(
                &mut transaction,
                &observed,
                &parents,
                plan,
                shape,
                ChildOutcome::Failed,
            )
            .await?
        } else {
            Vec::new()
        };
        let requirements = if error_route.is_some() {
            ControllerMutationRequirements {
                activation_scopes: vec![false],
                pending_node_count: 0,
                structural_exit: ControllerStructuralRequirement::None,
                pending_wake: false,
                remainder_cancellation_scope_ids,
            }
        } else {
            ControllerMutationRequirements {
                activation_scopes: Vec::new(),
                pending_node_count: 0,
                structural_exit: ControllerStructuralRequirement::Close,
                pending_wake: shape.is_some(),
                remainder_cancellation_scope_ids,
            }
        };
        transaction.rollback().await?;
        Ok(requirements)
    }
}
