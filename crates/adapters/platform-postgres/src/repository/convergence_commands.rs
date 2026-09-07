//! Each bounded Run step has its own transaction: temporary blockers cannot poison a page.
use super::*;
use crate::transaction_retry::{is_retryable_postgres_transaction_abort, MAXIMUM_ATTEMPTS};
use insight_platform_orchestrator::{
    convergence_member_states, decide_run_convergence, RunConvergenceGoal,
};

impl PgRepository {
    pub async fn drive_orchestration_convergence(
        &self,
        command: DriveOrchestrationConvergence,
    ) -> Result<SafetyScanPage<ConvergedOrchestrationRun>, RepositoryError> {
        command.validate(self.recovery_batch_limit, self.recovery_shard_limit)?;
        // No business transaction spans this candidate read and multiple Run steps.
        let rows = sqlx::query(
            r#"
            SELECT run.tenant_id, run.run_id, run.deadline AS scan_sort_at
            FROM insight_platform.runs run
            WHERE run.terminal_at IS NULL
              AND run.state IN ('queued','running','waiting','cancelling')
              AND (run.current_payload #> '{control,cancel_requested_at}' <> 'null'::jsonb
                OR run.current_payload #> '{control,timeout_requested_at}' <> 'null'::jsonb
                OR run.current_payload -> 'failure' <> 'null'::jsonb
                OR run.deadline <= clock_timestamp()
                OR EXISTS (SELECT 1 FROM insight_platform.jobs job
                    WHERE job.tenant_id=run.tenant_id AND job.run_id=run.run_id
                      AND job.work_class='orchestration' AND job.terminal_at IS NULL
                      AND (job.deadline <= clock_timestamp() OR
                        (job.state='running' AND job.attempt_no >= job.attempt_limit
                         AND job.lease_expires_at <= clock_timestamp()))))
              AND mod(('x' || right(run.run_id,8))::bit(32)::bigint,$2)=$1
              AND ($3::timestamptz IS NULL OR (run.deadline,run.tenant_id,run.run_id)>
                    ($3::timestamptz,$4::text,$5::text))
            ORDER BY run.deadline,run.tenant_id,run.run_id LIMIT $6
        "#,
        )
        .bind(i64::from(command.shard.index))
        .bind(i64::from(command.shard.count))
        .bind(command.after.as_ref().map(|c| c.sort_at))
        .bind(command.after.as_ref().map(|c| c.tenant_id.to_string()))
        .bind(command.after.as_ref().map(|c| c.item_id.to_string()))
        .bind(i64::from(command.limit))
        .fetch_all(&self.pool)
        .await?;
        let count = rows.len();
        let last = rows
            .last()
            .map(|r| safety_scan_cursor_from_row(r, "run_id", ResourceKind::Run))
            .transpose()?;
        let mut results = Vec::with_capacity(count);
        let mut diagnostics = Vec::new();
        for (row, slot) in rows.into_iter().zip(&command.slots) {
            let tenant: String = row.try_get("tenant_id")?;
            let run: String = row.try_get("run_id")?;
            for attempt in 0..MAXIMUM_ATTEMPTS {
                let mut tx = self.pool.begin().await?;
                sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    .execute(&mut *tx)
                    .await?;
                let result = converge_run_step(
                    &mut tx,
                    &tenant,
                    &run,
                    slot,
                    self.context_query_limits,
                    self.model_turn_limits,
                )
                .await;
                let failure = match result {
                    Ok(result) => match tx.commit().await {
                        Ok(()) => {
                            if let Some(result) = result {
                                results.push(result);
                            }
                            break;
                        }
                        Err(error) => RepositoryError::Database(error),
                    },
                    Err(error) => {
                        tx.rollback().await?;
                        error
                    }
                };
                if let RepositoryError::InvalidPersistedObject(diagnostic) = failure {
                    diagnostics.push(diagnostic);
                    break;
                }
                if !is_retryable_postgres_transaction_abort(&failure) {
                    return Err(failure);
                }
                if attempt + 1 == MAXIMUM_ATTEMPTS {
                    // Explicit abort: no state or event committed and no Job attempt consumed.
                    // The ordinary next sweep retries this Run; already committed Runs survive.
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1_u64 << attempt)).await;
            }
        }
        Ok(safety_scan_page(results, count, command.limit, last).with_diagnostics(diagnostics))
    }
}

async fn converge_run_step(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &str,
    run_id: &str,
    slot: &OrchestrationConvergenceSlot,
    context_limits: ContextQueryLimits,
    model_limits: ModelTurnLimits,
) -> Result<Option<ConvergedOrchestrationRun>, RepositoryError> {
    // Select one domain owner before taking any lock: each helper retains the same
    // quota -> Run -> Node -> domain -> Job order and revalidates the Run generation.
    if let Some(owner) = sqlx::query(
        r#"
        SELECT invocation.invocation_id,invocation.invocation_kind,run.version AS run_version
        FROM insight_platform.invocations invocation JOIN insight_platform.runs run
          ON run.tenant_id=invocation.tenant_id AND run.run_id=invocation.run_id
        WHERE invocation.tenant_id=$1 AND invocation.run_id=$2 AND invocation.terminal_at IS NULL
          AND invocation.invocation_kind IN ('context','model','capability')
          AND invocation.state NOT IN ('cancelling','reconciliation_required')
          AND run.state='cancelling' AND run.terminal_at IS NULL
        ORDER BY invocation.created_at,invocation.invocation_id LIMIT 1
    "#,
    )
    .bind(tenant)
    .bind(run_id)
    .fetch_optional(&mut **tx)
    .await?
    {
        let tenant_id = parse_id(tenant)?;
        let run_id = parse_id(run_id)?;
        let owner_id = parse_id(&owner.try_get::<String, _>("invocation_id")?)?;
        let version = owner.try_get("run_version")?;
        let result = match owner.try_get::<String, _>("invocation_kind")?.as_str() {
            "context" => {
                crate::context_query_repository::converge_context_for_run(
                    tx,
                    &tenant_id,
                    &run_id,
                    &owner_id,
                    version,
                    slot,
                    context_limits,
                )
                .await?
            }
            "model" => {
                crate::model_turn_repository::converge_model_for_run(
                    tx,
                    &tenant_id,
                    &run_id,
                    &owner_id,
                    version,
                    slot,
                    model_limits,
                )
                .await?
            }
            "capability" => {
                crate::capability_execution_repository::converge_capability_for_run(
                    tx, &tenant_id, &run_id, &owner_id, version, slot,
                )
                .await?
            }
            _ => unreachable!("closed SQL domain candidate"),
        };
        if let Some((run, state)) = result {
            let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
                .fetch_one(&mut **tx)
                .await?;
            let goal = decide_run_convergence(
                RunState::Cancelling,
                u64::try_from(run.version)
                    .map_err(|_| RepositoryError::CorruptRow("Run version".into()))?,
                run.deadline,
                &run.current,
                false,
                now,
            )?
            .ok_or_else(|| {
                RepositoryError::CorruptRow("controlled domain lost Run intent".into())
            })?;
            return Ok(Some(progress(
                run,
                &goal,
                OrchestrationConvergenceStep::Domain {
                    owner_id: owner_id.to_string(),
                    state,
                },
            )));
        }
        return Ok(None);
    }
    // Observe one eligible Job before locking quota. Its exact version/fence is checked again
    // after quota -> Run -> Node -> Job locks; no reverse quota acquisition is permitted.
    let job=sqlx::query(r#"
        SELECT job.* FROM insight_platform.jobs job
        JOIN insight_platform.run_nodes node ON node.tenant_id=job.tenant_id AND node.node_id=job.node_id
        WHERE job.tenant_id=$1 AND job.run_id=$2 AND job.work_class='orchestration'
          AND job.owner_kind='node_execution' AND job.owner_id=node.node_id
          AND job.terminal_at IS NULL AND node.terminal_at IS NULL
          AND NOT EXISTS (SELECT 1 FROM insight_platform.invocations i WHERE i.tenant_id=$1
              AND i.run_id=$2 AND i.node_id=node.node_id AND i.terminal_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM insight_platform.run_nodes link WHERE link.tenant_id=$1
              AND link.run_id=$2 AND link.record_kind='child_run_link'
              AND link.parent_node_id=node.node_id AND link.terminal_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM insight_platform.tasks task WHERE task.tenant_id=$1
              AND task.run_id=$2 AND task.node_id=node.node_id AND task.state='pending')
          AND NOT EXISTS (SELECT 1 FROM insight_platform.jobs effect WHERE effect.tenant_id=$1
              AND effect.run_id=$2 AND effect.node_id=node.node_id AND effect.work_class<>'orchestration'
              AND effect.terminal_at IS NULL)
        ORDER BY (job.state='running' AND job.attempt_no>=job.attempt_limit
            AND job.lease_expires_at<=clock_timestamp()) DESC,job.created_at,job.job_id LIMIT 1
    "#).bind(tenant).bind(run_id).fetch_optional(&mut **tx).await?.map(persisted_job_from_row).transpose()?;
    let quota = if let Some(job) = job.as_ref().filter(|j| j.lease_expires_at.is_some()) {
        lock_job_quota_bundle(tx, job, &slot.quota_entry_ids).await?
    } else {
        Vec::new()
    };
    let run = load_run_for_update(tx, &parse_id(tenant)?, &parse_id(run_id)?).await?;
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **tx)
        .await?;
    let exhausted = job.as_ref().is_some_and(|j| {
        j.state == "running"
            && j.attempt_no >= j.attempt_limit
            && j.lease_expires_at.is_some_and(|expiry| expiry <= now)
    });
    let deadline = job
        .as_ref()
        .map_or(run.deadline, |j| run.deadline.min(j.deadline));
    let Some(goal) = decide_run_convergence(
        run.state
            .parse()
            .map_err(|e: insight_platform_contracts::state::StateParseError| {
                RepositoryError::CorruptRow(e.to_string())
            })?,
        u64::try_from(run.version)
            .map_err(|_| RepositoryError::CorruptRow("negative Run version".into()))?,
        deadline,
        &run.current,
        exhausted,
        now,
    )?
    else {
        return Ok(None);
    };
    if run.state != "cancelling"
        || run.current.control != goal.control
        || run.current.failure != goal.failure
    {
        let next = update_run_step(tx, &run, &goal, 0, false, now).await?;
        emit(
            tx,
            &next,
            slot,
            "run",
            &next.run_id,
            next.version,
            "run.convergence_observed",
            &goal,
            &next.state,
        )
        .await?;
        return Ok(Some(progress(
            next,
            &goal,
            OrchestrationConvergenceStep::ControlObserved,
        )));
    }
    // Task first-winner runs before closing its owner. Domain-owned approvals and OAuth
    // remain untouched; their pending tasks and cleanup Jobs continue to block terminality.
    if let Some(task_row) = sqlx::query(
        r#"
        SELECT task.task_id,task.node_id FROM insight_platform.tasks task
        WHERE task.tenant_id=$1 AND task.run_id=$2 AND task.owner_kind='node_execution'
          AND task.owner_id=task.node_id AND task.state='pending'
          AND task.task_kind <> 'approval'
        ORDER BY task.created_at,task.task_id LIMIT 1
    "#,
    )
    .bind(tenant)
    .bind(run_id)
    .fetch_optional(&mut **tx)
    .await?
    {
        let node_id: String = task_row.try_get("node_id")?;
        sqlx::query("SELECT node_id FROM insight_platform.run_nodes WHERE tenant_id=$1 AND node_id=$2 AND run_id=$3 FOR UPDATE")
            .bind(tenant).bind(&node_id).bind(run_id).fetch_one(&mut **tx).await?;
        let task = load_task_for_update(
            tx,
            &parse_id(tenant)?,
            &parse_id(&task_row.try_get::<String, _>("task_id")?)?,
        )
        .await?;
        let current = crate::recovery_isolation::addressed(
            task_projection(&task),
            &parse_id(tenant)?,
            &parse_id(&task.task_id)?,
            insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
        )?;
        let next = decide_task_resolution(
            &current,
            DomainResolveTask {
                expected_generation: current.generation,
                expected_version: current.version,
                target: TaskState::Cancelled,
                principal: None,
                response_value_id: None,
                response_schema_digest: None,
            },
            now,
        )?;
        let payload = TypedPayload::new(
            i32::try_from(next.payload_schema_version)
                .map_err(|_| RepositoryError::CorruptRow("Task schema version".into()))?,
            &next.payload,
        )?;
        let version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.tasks SET state='cancelled',version=version+1,
                payload_schema_version=$4,payload=$5,payload_digest=$6,responded_at=$7,updated_at=$7
            WHERE tenant_id=$1 AND task_id=$2 AND version=$3 AND state='pending' RETURNING version
        "#,
        )
        .bind(tenant)
        .bind(&task.task_id)
        .bind(task.version)
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(now)
        .fetch_one(&mut **tx)
        .await?;
        emit(
            tx,
            &run,
            slot,
            "task",
            &task.task_id,
            version,
            "task.cancelled_by_run",
            &goal,
            "cancelled",
        )
        .await?;
        return Ok(Some(progress(
            run,
            &goal,
            OrchestrationConvergenceStep::Task {
                task_id: task.task_id,
                task_version: version,
            },
        )));
    }
    if let Some(observed) = job {
        return converge_job_node(tx, run, goal, observed, quota, slot, now, exhausted)
            .await
            .map(Some);
    }
    // Pending joins and controller nodes can exist without any live Job. Close one only
    // after all external owners attached to that Node have reached their own terminal fact.
    let node=sqlx::query(r#"
        SELECT node.node_id,node.version,node.state FROM insight_platform.run_nodes node
        WHERE node.tenant_id=$1 AND node.run_id=$2 AND node.record_kind='node_execution'
          AND node.terminal_at IS NULL
          AND NOT EXISTS (SELECT 1 FROM insight_platform.jobs j WHERE j.tenant_id=$1 AND j.run_id=$2
              AND j.node_id=node.node_id AND j.terminal_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM insight_platform.invocations i WHERE i.tenant_id=$1 AND i.run_id=$2
              AND i.node_id=node.node_id AND i.terminal_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM insight_platform.run_nodes link WHERE link.tenant_id=$1
              AND link.run_id=$2 AND link.record_kind='child_run_link'
              AND link.parent_node_id=node.node_id AND link.terminal_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM insight_platform.tasks t WHERE t.tenant_id=$1 AND t.run_id=$2
              AND t.node_id=node.node_id AND (t.state='pending' OR EXISTS (
                SELECT 1 FROM insight_platform.jobs cleanup WHERE cleanup.tenant_id=t.tenant_id
                  AND cleanup.job_id=t.current_cleanup_job_id AND cleanup.terminal_at IS NULL)))
        ORDER BY node.created_at,node.node_id LIMIT 1 FOR UPDATE OF node
    "#).bind(tenant).bind(run_id).fetch_optional(&mut **tx).await?;
    if let Some(node) = node {
        let node_id: String = node.try_get("node_id")?;
        let version = close_node(
            tx,
            &run,
            &node_id,
            node.try_get("version")?,
            &node.try_get::<String, _>("state")?,
            convergence_member_states(&goal, false).1,
            slot,
            &goal,
            now,
        )
        .await?;
        return Ok(Some(progress(
            run,
            &goal,
            OrchestrationConvergenceStep::PendingNode {
                node_id,
                node_version: version,
            },
        )));
    }
    let scope=sqlx::query(r#"
        SELECT scope.node_id,scope.version,scope.state FROM insight_platform.run_nodes scope
        WHERE scope.tenant_id=$1 AND scope.run_id=$2 AND scope.record_kind='scope_instance'
          AND scope.terminal_at IS NULL
          AND NOT EXISTS (SELECT 1 FROM insight_platform.run_nodes member WHERE member.tenant_id=$1
              AND member.run_id=$2 AND member.scope_id=scope.node_id
              AND member.record_kind<>'scope_instance' AND member.terminal_at IS NULL)
          AND NOT EXISTS (SELECT 1 FROM insight_platform.run_nodes nested
              JOIN insight_platform.run_nodes owner ON owner.tenant_id=nested.tenant_id
                  AND owner.node_id=nested.parent_node_id
              WHERE nested.tenant_id=$1 AND nested.run_id=$2 AND nested.record_kind='scope_instance'
                AND nested.node_id<>scope.node_id AND owner.scope_id=scope.node_id AND nested.terminal_at IS NULL)
        ORDER BY scope.created_at DESC,scope.node_id LIMIT 1 FOR UPDATE OF scope
    "#).bind(tenant).bind(run_id).fetch_optional(&mut **tx).await?;
    if let Some(scope) = scope {
        let scope_id: String = scope.try_get("node_id")?;
        let mut version: i64 = scope.try_get("version")?;
        let state: ScopeState = scope.try_get::<String, _>("state")?.parse().map_err(
            |e: insight_platform_contracts::state::StateParseError| {
                RepositoryError::CorruptRow(e.to_string())
            },
        )?;
        if state == ScopeState::Open {
            version=sqlx::query_scalar("UPDATE insight_platform.run_nodes SET state='closing',version=version+1,updated_at=$4 WHERE tenant_id=$1 AND node_id=$2 AND version=$3 RETURNING version")
                .bind(tenant).bind(&scope_id).bind(version).bind(now).fetch_one(&mut **tx).await?;
            emit(
                tx,
                &run,
                slot,
                "scope_closing",
                &scope_id,
                version,
                "scope.closing",
                &goal,
                "closing",
            )
            .await?;
        } else if state != ScopeState::Closing {
            return Err(RepositoryError::CorruptRow(
                "nonterminal Scope state".into(),
            ));
        }
        version=sqlx::query_scalar("UPDATE insight_platform.run_nodes SET state=$4,version=version+1,terminal_at=$5,updated_at=$5 WHERE tenant_id=$1 AND node_id=$2 AND version=$3 AND state='closing' RETURNING version")
            .bind(tenant).bind(&scope_id).bind(version).bind(convergence_member_states(&goal,false).2.as_str()).bind(now).fetch_one(&mut **tx).await?;
        emit(
            tx,
            &run,
            slot,
            "scope",
            &scope_id,
            version,
            "scope.terminal_converged",
            &goal,
            convergence_member_states(&goal, false).2.as_str(),
        )
        .await?;
        return Ok(Some(progress(
            run,
            &goal,
            OrchestrationConvergenceStep::Scope {
                scope_id,
                scope_version: version,
            },
        )));
    }
    let blocked:bool=sqlx::query_scalar(r#"
        SELECT EXISTS (SELECT 1 FROM insight_platform.run_nodes WHERE tenant_id=$1 AND run_id=$2 AND terminal_at IS NULL)
          OR EXISTS (SELECT 1 FROM insight_platform.jobs WHERE tenant_id=$1 AND run_id=$2 AND terminal_at IS NULL)
          OR EXISTS (SELECT 1 FROM insight_platform.invocations WHERE tenant_id=$1 AND run_id=$2 AND terminal_at IS NULL)
          OR EXISTS (SELECT 1 FROM insight_platform.tasks t WHERE t.tenant_id=$1 AND t.run_id=$2
              AND (t.state='pending' OR EXISTS (SELECT 1 FROM insight_platform.jobs cleanup
                WHERE cleanup.tenant_id=t.tenant_id AND cleanup.job_id=t.current_cleanup_job_id AND cleanup.terminal_at IS NULL)))
          OR EXISTS (SELECT 1 FROM insight_platform.runs child WHERE child.tenant_id=$1
              AND child.parent_run_id=$2 AND child.terminal_at IS NULL)
    "#).bind(tenant).bind(run_id).fetch_one(&mut **tx).await?;
    if blocked || run.active_work_count != 0 {
        return Ok(Some(progress(
            run,
            &goal,
            OrchestrationConvergenceStep::NotReady,
        )));
    }
    let run = update_run_step(tx, &run, &goal, 0, true, now).await?;
    emit(
        tx,
        &run,
        slot,
        "run",
        &run.run_id,
        run.version,
        "run.terminal_converged",
        &goal,
        &run.state,
    )
    .await?;
    Ok(Some(progress(
        run,
        &goal,
        OrchestrationConvergenceStep::RunTerminal,
    )))
}

fn parse_id(value: &str) -> Result<ResourceId, RepositoryError> {
    value
        .parse()
        .map_err(|error: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(error.to_string())
        })
}
fn progress(
    run: RunRecord,
    goal: &RunConvergenceGoal,
    step: OrchestrationConvergenceStep,
) -> ConvergedOrchestrationRun {
    ConvergedOrchestrationRun {
        run,
        reason: goal.reason,
        step,
    }
}

#[allow(clippy::too_many_arguments)]
async fn converge_job_node(
    tx: &mut Transaction<'_, Postgres>,
    run: RunRecord,
    goal: RunConvergenceGoal,
    observed: JobRecord,
    quota: Vec<QuotaAccountRecord>,
    slot: &OrchestrationConvergenceSlot,
    now: DateTime<Utc>,
    exhausted: bool,
) -> Result<ConvergedOrchestrationRun, RepositoryError> {
    require_orchestration_job(&observed)?;
    let node_id = observed
        .node_id
        .as_ref()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Node missing".into()))?;
    let node=sqlx::query("SELECT state,version FROM insight_platform.run_nodes WHERE tenant_id=$1 AND node_id=$2 AND run_id=$3 AND record_kind='node_execution' FOR UPDATE")
        .bind(&run.tenant_id).bind(node_id).bind(&run.run_id).fetch_one(&mut **tx).await?;
    let current = load_job_for_update_by_text(tx, &run.tenant_id, &observed.job_id).await?;
    if current.version != observed.version
        || current.state != observed.state
        || current.lease_epoch != observed.lease_epoch
        || current.payload.digest != observed.payload.digest
        || current.quota_reservation_id != observed.quota_reservation_id
    {
        return Ok(progress(run, &goal, OrchestrationConvergenceStep::NotReady));
    }
    let expected = orchestration_node_state_for_job_state(&current.state)?;
    if node.try_get::<String, _>("state")? != expected {
        return Err(RepositoryError::CorruptRow(
            "orchestration Job and Node states disagree".into(),
        ));
    }
    let (target_job, target_node, _) = convergence_member_states(&goal, exhausted);
    let next = decide_job_owner_terminal(&job_projection(&current)?, target_job)?;
    let result = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "job_id":current.job_id,"lease_generation":current.lease_epoch,
            "reason":goal.reason.as_str(),"terminal_state":target_job.as_str(),
        }),
        65_536,
    )?;
    let active = current.lease_expires_at.is_some();
    if active {
        settle_locked_job_quota_bundle(
            tx,
            &current,
            &quota,
            &slot.quota_entry_ids,
            &result
                .digest
                .parse::<Sha256Digest>()
                .map_err(|e| RepositoryError::CorruptRow(e.to_string()))?,
        )
        .await?;
    }
    let payload = orchestration_job_payload_with_wake(&current, None)?;
    let row=sqlx::query(r#"
        UPDATE insight_platform.jobs SET state=$4,version=$5,result_digest=$6,
            worker_id=NULL,lease_token_digest=NULL,lease_expires_at=NULL,heartbeat_at=NULL,retry_at=NULL,
            wake_kind=NULL,wake_state=NULL,wake_generation=0,
            payload_schema_version=$7,payload=$8,payload_digest=$9,terminal_at=$10,updated_at=$10
        WHERE tenant_id=$1 AND job_id=$2 AND version=$3 AND terminal_at IS NULL RETURNING *
    "#).bind(&run.tenant_id).bind(&current.job_id).bind(current.version).bind(target_job.as_str())
        .bind(i64::try_from(next.version).map_err(|_|RepositoryError::CorruptRow("Job version overflow".into()))?)
        .bind(&result.digest).bind(payload.schema_version).bind(&payload.value).bind(&payload.digest).bind(now)
        .fetch_one(&mut **tx).await?;
    let job = job_from_row(row)?;
    let node_version = close_node(
        tx,
        &run,
        node_id,
        node.try_get("version")?,
        expected,
        target_node,
        slot,
        &goal,
        now,
    )
    .await?;
    let run = update_run_step(tx, &run, &goal, i32::from(active), false, now).await?;
    emit(
        tx,
        &run,
        slot,
        "job",
        &job.job_id,
        job.version,
        "job.terminal_converged",
        &goal,
        &job.state,
    )
    .await?;
    emit(
        tx,
        &run,
        slot,
        "run",
        &run.run_id,
        run.version,
        "run.convergence_progressed",
        &goal,
        &run.state,
    )
    .await?;
    Ok(progress(
        run,
        &goal,
        OrchestrationConvergenceStep::JobNode {
            job: Box::new(job),
            node_id: node_id.clone(),
            node_version,
            settled_quota_account_ids: quota.into_iter().map(|a| a.quota_account_id).collect(),
        },
    ))
}

#[allow(clippy::too_many_arguments)]
async fn close_node(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
    node_id: &str,
    mut version: i64,
    state: &str,
    target: NodeExecutionState,
    slot: &OrchestrationConvergenceSlot,
    goal: &RunConvergenceGoal,
    now: DateTime<Utc>,
) -> Result<i64, RepositoryError> {
    let mut state: NodeExecutionState =
        state
            .parse()
            .map_err(|e: insight_platform_contracts::state::StateParseError| {
                RepositoryError::CorruptRow(e.to_string())
            })?;
    if state == NodeExecutionState::Running && target == NodeExecutionState::Cancelled {
        version=sqlx::query_scalar("UPDATE insight_platform.run_nodes SET state='cancelling',version=version+1,retry_at=NULL,updated_at=$4 WHERE tenant_id=$1 AND node_id=$2 AND version=$3 RETURNING version")
            .bind(&run.tenant_id).bind(node_id).bind(version).bind(now).fetch_one(&mut **tx).await?;
        emit(
            tx,
            run,
            slot,
            "node_cancelling",
            node_id,
            version,
            "node.cancelling",
            goal,
            "cancelling",
        )
        .await?;
        state = NodeExecutionState::Cancelling;
    }
    if !state.can_transition_to(target) {
        return Err(RepositoryError::CorruptRow(
            "invalid convergence Node transition".into(),
        ));
    }
    version=sqlx::query_scalar("UPDATE insight_platform.run_nodes SET state=$4,version=version+1,retry_at=NULL,terminal_at=$5,updated_at=$5 WHERE tenant_id=$1 AND node_id=$2 AND version=$3 AND terminal_at IS NULL RETURNING version")
        .bind(&run.tenant_id).bind(node_id).bind(version).bind(target.as_str()).bind(now).fetch_one(&mut **tx).await?;
    emit(
        tx,
        run,
        slot,
        "node",
        node_id,
        version,
        "node.terminal_converged",
        goal,
        target.as_str(),
    )
    .await?;
    Ok(version)
}

pub(super) async fn update_run_step(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
    goal: &RunConvergenceGoal,
    release: i32,
    terminal: bool,
    now: DateTime<Utc>,
) -> Result<RunRecord, RepositoryError> {
    let mut current = run.current.clone();
    current.control = goal.control.clone();
    current.failure = goal.failure.clone();
    current.output_value_id = None;
    if terminal {
        current.waiting_reason = None;
    }
    current.validate(&parse_id(&run.run_id)?)?;
    let payload = TypedPayload::from_versioned(1, &current, 1_048_576)?;
    let target = if terminal {
        goal.terminal_state
    } else {
        RunState::Cancelling
    };
    let state: RunState =
        run.state
            .parse()
            .map_err(|e: insight_platform_contracts::state::StateParseError| {
                RepositoryError::CorruptRow(e.to_string())
            })?;
    if state != target && !state.can_transition_to(target) {
        return Err(RepositoryError::CorruptRow(
            "invalid Run convergence transition".into(),
        ));
    }
    let row=sqlx::query(r#"
        UPDATE insight_platform.runs SET state=$4,version=version+1,active_work_count=active_work_count-$5,
            current_schema_version=$6,current_payload=$7,current_payload_digest=$8,timeout_generation=$9,
            output_value_id=NULL,terminal_at=$10,updated_at=$11
        WHERE tenant_id=$1 AND run_id=$2 AND version=$3 AND terminal_at IS NULL
          AND active_work_count >= $5 RETURNING *
    "#).bind(&run.tenant_id).bind(&run.run_id).bind(run.version).bind(target.as_str()).bind(release)
        .bind(payload.schema_version).bind(&payload.value).bind(&payload.digest)
        .bind(i64::try_from(goal.control.timeout_generation).map_err(|_|RepositoryError::CorruptRow("timeout generation overflow".into()))?)
        .bind(terminal.then_some(now)).bind(now).fetch_one(&mut **tx).await?;
    run_from_row(row)
}

#[allow(clippy::too_many_arguments)]
async fn emit(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
    slot: &OrchestrationConvergenceSlot,
    kind: &str,
    id: &str,
    version: i64,
    event: &str,
    goal: &RunConvergenceGoal,
    actual_state: &str,
) -> Result<(), RepositoryError> {
    let (aggregate, event_id, outbox_id) = match kind {
        "run" => ("run", &slot.run_event_id, &slot.run_outbox_id),
        "job" => ("job", &slot.job_event_id, &slot.job_outbox_id),
        "scope_closing" => (
            "scope_instance",
            &slot.scope_closing_event_id,
            &slot.scope_closing_outbox_id,
        ),
        "scope" => (
            "scope_instance",
            &slot.scope_terminal_event_id,
            &slot.scope_terminal_outbox_id,
        ),
        "node_cancelling" => (
            "node_execution",
            &slot.node_cancelling_event_id,
            &slot.node_cancelling_outbox_id,
        ),
        "task" => ("interaction", &slot.node_event_id, &slot.node_outbox_id),
        _ => ("node_execution", &slot.node_event_id, &slot.node_outbox_id),
    };
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "reason":goal.reason.as_str(),"terminal_state":actual_state,
            "cancel_generation":run.current.control.cancel_generation,
            "timeout_generation":run.current.control.timeout_generation,
        }),
        65_536,
    )?;
    append_scheduler_event(
        tx,
        &run.tenant_id,
        event_id,
        outbox_id,
        aggregate,
        id,
        version,
        Some(&run.run_id),
        event,
        &payload,
    )
    .await
}
