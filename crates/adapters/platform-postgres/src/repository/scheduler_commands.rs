//! PostgreSQL scheduler commands. Shared locks and atomicity remain in this adapter.
use super::*;

enum PartitionClaimError {
    SharedAuthority,
    Fatal(RepositoryError),
}
impl PartitionClaimError {
    fn shared(error: RepositoryError) -> Self {
        match error {
            RepositoryError::CorruptRow(_) => Self::SharedAuthority,
            other => Self::Fatal(other),
        }
    }
}
impl From<RepositoryError> for PartitionClaimError {
    fn from(error: RepositoryError) -> Self {
        Self::Fatal(error)
    }
}
impl From<sqlx::Error> for PartitionClaimError {
    fn from(error: sqlx::Error) -> Self {
        Self::Fatal(error.into())
    }
}
impl From<insight_platform_jobs::JobError> for PartitionClaimError {
    fn from(error: insight_platform_jobs::JobError) -> Self {
        Self::Fatal(error.into())
    }
}

impl PgSchedulerTransaction {
    pub async fn claim_orchestration_jobs(
        &mut self,
        command: ClaimOrchestrationJobs,
    ) -> Result<Vec<ClaimedOrchestrationJob>, RepositoryError> {
        command.validate(self.limits)?;
        let hints = self
            .orchestration_partition_hints
            .as_ref()
            .ok_or_else(|| {
                RepositoryError::InvalidInput(
                    "orchestration claim requires its dedicated transaction".into(),
                )
            })?
            .clone();
        for hint in hints {
            match self
                .claim_orchestration_partition(command.clone(), hint)
                .await
            {
                Ok(result) => return Ok(result),
                Err(PartitionClaimError::SharedAuthority) => tracing::warn!(
                    work_class = "orchestration",
                    partition = hint.0,
                    phase = "shared_authority",
                    code = "invalid_shared_authority",
                    "invalid scheduling partition retained"
                ),
                Err(PartitionClaimError::Fatal(error)) => return Err(error),
            }
        }
        Ok(Vec::new())
    }

    async fn claim_orchestration_partition(
        &mut self,
        command: ClaimOrchestrationJobs,
        hint: insight_platform_contracts::SchedulerPartitionId,
    ) -> Result<Vec<ClaimedOrchestrationJob>, PartitionClaimError> {
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let Some(partition) = crate::partition_scheduler::lock_exact_partition(
            &mut transaction,
            WorkClass::Orchestration,
            hint,
        )
        .await
        .map_err(PartitionClaimError::shared)?
        else {
            transaction.commit().await?;
            return Ok(Vec::new());
        };
        let window = crate::partition_scheduler::lock_tenant_window(
            &mut transaction,
            &partition,
            self.limits.maximum_tenants,
            self.limits.maximum_deficit,
        )
        .await
        .map_err(PartitionClaimError::shared)?;
        let mut diagnostics = Vec::new();
        let mut pages = BTreeMap::new();
        let mut candidate_ids = Vec::new();
        for tenant in &window.tenants {
            let page = crate::partition_scheduler::scan_job_cohort(
                &mut transaction,
                tenant,
                self.limits.maximum_window_per_tenant,
            )
            .await
            .map_err(PartitionClaimError::shared)?;
            diagnostics.extend(page.diagnostics.iter().cloned());
            candidate_ids.extend(page.jobs.iter().map(|job| job.job_id.clone()));
            pages.insert(tenant.state.tenant_id.clone(), page);
        }
        let candidates = enumerate_orchestration_candidates(
            &mut transaction,
            database_now,
            &candidate_ids,
            &command.worker_manifest.execution_capabilities,
            self.limits,
            &mut diagnostics,
            self.scope_environment_limits,
        )
        .await?;
        let mut policies = BTreeMap::new();
        for tenant in &window.tenants {
            if candidates.iter().any(|candidate| {
                candidate.job.tenant_id == tenant.state.tenant_id.to_string()
                    && decode_orchestration_job_payload(&candidate.job.payload).is_ok_and(
                        |payload| {
                            payload.scheduling_lane()
                                == insight_platform_contracts::SchedulingLane::Business
                        },
                    )
            }) && matches!(
                tenant.state.policy,
                insight_platform_contracts::SchedulingPolicyBinding::Bound { .. }
            ) {
                policies.insert(
                    tenant.state.tenant_id.clone(),
                    load_tenant_scheduling_policy(&mut transaction, &tenant.state.tenant_id)
                        .await
                        .map_err(PartitionClaimError::shared)?,
                );
            }
        }
        // Lock the entire bounded candidate account set before selection. Saturated
        // Jobs are skipped against one shadow balance; only admitted work reserves.
        let quota_accounts = lock_orchestration_quota_accounts(&mut transaction, &candidates)
            .await
            .map_err(PartitionClaimError::shared)?;
        let available_quota = quota_accounts
            .values()
            .map(|account| {
                let available = account
                    .limit_value
                    .checked_sub(account.used_value)
                    .and_then(|value| value.checked_sub(account.reserved_value))
                    .filter(|value| *value >= 0)
                    .ok_or(RepositoryError::QuotaExceeded)?;
                let id = account
                    .quota_account_id
                    .parse::<ResourceId>()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
                Ok((id, available as u64))
            })
            .collect::<Result<BTreeMap<_, _>, RepositoryError>>()?;
        let candidate_by_job = candidates
            .into_iter()
            .map(|candidate| (candidate.job.job_id.clone(), candidate))
            .collect::<BTreeMap<_, _>>();
        let mut visits = Vec::new();
        for tenant in &window.tenants {
            let policy = policies.remove(&tenant.state.tenant_id);
            let page = pages
                .remove(&tenant.state.tenant_id)
                .ok_or_else(|| RepositoryError::CorruptRow("missing Job cohort page".into()))?;
            let mut admissions = Vec::new();
            for job in &page.jobs {
                let candidate = candidate_by_job.get(&job.job_id);
                let costs = match candidate {
                    Some(candidate) => quota_account_ids_for_candidate(candidate, &quota_accounts)?
                        .into_iter()
                        .map(|id| {
                            Ok(insight_platform_scheduler::partitioned::QuotaCost {
                                account_id: id.parse::<ResourceId>().map_err(|failure| {
                                    RepositoryError::CorruptRow(failure.to_string())
                                })?,
                                amount: 1,
                            })
                        })
                        .collect::<Result<Vec<_>, RepositoryError>>()?,
                    None => Vec::new(),
                };
                admissions.push(
                    insight_platform_scheduler::partitioned::AdmissionCandidate {
                        job_id: job
                            .job_id
                            .parse::<ResourceId>()
                            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                        mode: if has_started_worker_attempt(job) {
                            insight_platform_contracts::ClaimMode::Continuation
                        } else {
                            insight_platform_contracts::ClaimMode::NewAttempt
                        },
                        lane: candidate
                            .map(|candidate| {
                                decode_orchestration_job_payload(&candidate.job.payload)
                                    .map(|payload| payload.scheduling_lane())
                            })
                            .transpose()?
                            .unwrap_or(insight_platform_contracts::SchedulingLane::Business),
                        currently_eligible: candidate.is_some(),
                        quota_costs: costs,
                    },
                );
            }
            visits.push(insight_platform_scheduler::partitioned::LockedTenantVisit {
                state: tenant.state.clone(),
                business_policy: policy,
                candidates: admissions,
                next_job_sweep: page.next_sweep,
            });
        }
        let decision = insight_platform_scheduler::partitioned::select_admissible_partition_batch(
            &partition.state,
            &visits,
            &available_quota,
            insight_platform_scheduler::partitioned::PartitionSchedulerLimits {
                maximum_deficit: self.limits.maximum_deficit,
                maximum_tenant_window: usize::from(self.limits.maximum_tenants),
                maximum_candidates_per_tenant: usize::from(self.limits.maximum_window_per_tenant),
                maximum_claims: usize::from(self.limits.maximum_batch),
                maximum_control_claims_per_tenant: 1,
                maximum_quota_lines_per_candidate: MAX_ORCHESTRATION_QUOTA_LINES,
            },
            usize::from(command.limit),
            window.range_exhausted,
        )
        .map_err(|failure| {
            RepositoryError::InvalidInput(format!("partition admission: {failure:?}"))
        })?;
        for diagnostic in &diagnostics {
            crate::recovery_isolation::observe(diagnostic);
        }
        if decision.admitted_job_ids.is_empty() {
            crate::partition_scheduler::persist_admission(
                &mut transaction,
                &partition,
                &window.tenants,
                &decision,
            )
            .await?;
            transaction.commit().await?;
            return Ok(Vec::new());
        }
        let mut selected = Vec::with_capacity(decision.admitted_job_ids.len());
        for (job_id, slot) in decision.admitted_job_ids.iter().zip(command.slots.iter()) {
            let candidate = candidate_by_job
                .get(&job_id.to_string())
                .ok_or_else(|| {
                    RepositoryError::CorruptRow("selected Job is outside cohort".into())
                })?
                .clone();
            selected.push((candidate, slot));
        }
        selected.sort_by(|(left, _), (right, _)| left.lock_key().cmp(&right.lock_key()));
        let locked_parents =
            lock_orchestration_parents(&mut transaction, &selected, database_now).await?;
        let locked_jobs = lock_selected_jobs(&mut transaction, &selected).await?;

        let mut decisions = BTreeMap::new();
        for (candidate, slot) in &selected {
            let locked = locked_jobs
                .get(&candidate.job.job_id)
                .ok_or_else(|| RepositoryError::Conflict("selected orchestration Job"))?;
            if locked.version != candidate.job.version
                || locked.state != candidate.job.state
                || locked.payload.digest != candidate.job.payload.digest
                || !command
                    .worker_manifest
                    .execution_capabilities
                    .supports(&locked.execution_requirement)
            {
                return Err(RepositoryError::Conflict("selected orchestration Job").into());
            }
            let policy = LeasePolicy {
                requested_milliseconds: u64::try_from(command.lease_milliseconds)
                    .map_err(|_| RepositoryError::InvalidInput("negative Job lease".to_owned()))?,
                hard_maximum_milliseconds: u64::try_from(MAX_JOB_LEASE_MILLISECONDS)
                    .expect("positive Job lease hard maximum"),
            };
            let next = if has_started_worker_attempt(locked) {
                decide_job_claim_continuation(
                    &job_projection(locked)?,
                    database_now,
                    command.worker_id.clone(),
                    slot.lease_token_digest.clone(),
                    policy,
                )?
            } else {
                decide_job_claim(
                    &job_projection(locked)?,
                    database_now,
                    command.worker_id.clone(),
                    slot.lease_token_digest.clone(),
                    policy,
                )?
            };
            decisions.insert(candidate.job.job_id.clone(), next);
        }

        let quota_lines = reserve_orchestration_quota_bundles(
            &mut transaction,
            &selected,
            &quota_accounts,
            &decisions,
        )
        .await?;
        let parent_versions = mutate_orchestration_parents(
            &mut transaction,
            &selected,
            &locked_parents,
            database_now,
        )
        .await?;
        let mut claimed = mutate_claimed_jobs(
            &mut transaction,
            &selected,
            &decisions,
            &quota_lines,
            &command.worker_manifest.worker_build_digest,
        )
        .await?;
        append_orchestration_claim_events(
            &mut transaction,
            &selected,
            &claimed,
            &parent_versions,
            &quota_lines,
        )
        .await?;
        crate::partition_scheduler::persist_admission(
            &mut transaction,
            &partition,
            &window.tenants,
            &decision,
        )
        .await?;

        for record in &mut claimed {
            let (run_version, node_version) = parent_versions
                .get(&record.job.job_id)
                .copied()
                .ok_or_else(|| RepositoryError::CorruptRow("missing parent version".to_owned()))?;
            record.run_version = run_version;
            record.node_version = node_version;
        }
        transaction.commit().await?;
        Ok(claimed)
    }

    pub async fn start_orchestration_job(
        &mut self,
        command: StartOrchestrationJob,
    ) -> Result<CommandOutcome<JobRecord>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let receipt_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "job_id": command.fence.job_id,
                "lease_generation": command.fence.lease_epoch,
                "worker_process_generation_id": command.fence.worker_id,
            }),
        )?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.receipt_id,
            "orchestration.job.start",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let record = load_job_for_update_by_text(
                &mut transaction,
                &command.fence.tenant_id,
                &command.fence.job_id,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }

        let observed = load_job_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        require_orchestration_job(&observed)?;
        let parents = lock_orchestration_job_parents(&mut transaction, &observed, "ready").await?;
        let current = load_job_for_update_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        if current.version != observed.version || current.payload.digest != observed.payload.digest
        {
            return Err(RepositoryError::Conflict("orchestration Job"));
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let next = if has_started_worker_attempt(&current) {
            decide_job_resume(
                &job_projection(&current)?,
                &domain_job_fence(&command.fence)?,
                database_now,
            )?
        } else {
            decide_job_start(
                &job_projection(&current)?,
                &domain_job_fence(&command.fence)?,
                database_now,
            )?
        };
        let row =
            sqlx::query(
                r#"
            UPDATE insight_platform.jobs
            SET state = 'running', version = $4, attempt_no = $5,
                started_at = COALESCE(started_at, $6), updated_at = $6
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND state = 'leased'
            RETURNING *
            "#,
            )
            .bind(&current.tenant_id)
            .bind(&current.job_id)
            .bind(current.version)
            .bind(i64::try_from(next.version).map_err(|_| {
                RepositoryError::InvalidInput("Job version exceeds bigint".to_owned())
            })?)
            .bind(i32::try_from(next.attempt_count).map_err(|_| {
                RepositoryError::InvalidInput("Job attempt count exceeds integer".to_owned())
            })?)
            .bind(database_now)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(RepositoryError::Conflict("orchestration Job"))?;
        let record = job_from_row(row)?;
        let node_version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.run_nodes
            SET state = 'running', version = version + 1,
                started_at = COALESCE(started_at, $4), updated_at = $4
            WHERE tenant_id = $1 AND node_id = $2 AND version = $3
              AND record_kind = 'node_execution' AND state = 'ready'
              AND terminal_at IS NULL
            RETURNING version
            "#,
        )
        .bind(&record.tenant_id)
        .bind(&parents.node_id)
        .bind(parents.node_version)
        .bind(database_now)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("orchestration Node start"))?;
        let event_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "attempt_count": record.attempt_no,
                "lease_generation": record.lease_epoch,
                "worker_process_generation_id": record.worker_id,
            }),
        )?;
        append_scheduler_event(
            &mut transaction,
            &record.tenant_id,
            &command.node_event_id,
            &command.node_outbox_id,
            "node_execution",
            &parents.node_id,
            node_version,
            record.run_id.as_deref(),
            "node.started",
            &event_payload,
        )
        .await?;
        append_scheduler_event(
            &mut transaction,
            &record.tenant_id,
            &command.job_event_id,
            &command.job_outbox_id,
            "job",
            &record.job_id,
            record.version,
            record.run_id.as_deref(),
            "job.started",
            &event_payload,
        )
        .await?;
        terminalize_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.receipt_id,
            &command.request_digest,
            "started",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }

    pub async fn heartbeat_orchestration_job(
        &mut self,
        command: HeartbeatJob,
    ) -> Result<JobRecord, RepositoryError> {
        command.fence.validate()?;
        if command.lease_milliseconds <= 0
            || command.lease_milliseconds > MAX_JOB_LEASE_MILLISECONDS
        {
            return Err(RepositoryError::InvalidInput(
                "lease duration is outside the platform bound".to_owned(),
            ));
        }
        let mut transaction = self.transaction.begin().await?;
        let current = load_job_for_update_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        require_orchestration_job(&current)?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let next = decide_job_heartbeat(
            &job_projection(&current)?,
            &domain_job_fence(&command.fence)?,
            database_now,
            LeasePolicy {
                requested_milliseconds: u64::try_from(command.lease_milliseconds)
                    .map_err(|_| RepositoryError::InvalidInput("negative Job lease".to_owned()))?,
                hard_maximum_milliseconds: u64::try_from(MAX_JOB_LEASE_MILLISECONDS)
                    .expect("positive Job lease hard maximum"),
            },
        )?;
        let lease = next.lease.as_ref().ok_or_else(|| {
            RepositoryError::CorruptRow("heartbeat decision removed Job lease".to_owned())
        })?;
        let row =
            sqlx::query(
                r#"
            UPDATE insight_platform.jobs
            SET version = $4, heartbeat_at = $5, lease_expires_at = $6, updated_at = $5
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
            RETURNING *
            "#,
            )
            .bind(&current.tenant_id)
            .bind(&current.job_id)
            .bind(current.version)
            .bind(i64::try_from(next.version).map_err(|_| {
                RepositoryError::InvalidInput("Job version exceeds bigint".to_owned())
            })?)
            .bind(lease.heartbeat_at)
            .bind(lease.expires_at)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(RepositoryError::Conflict("orchestration Job"))?;
        let record = job_from_row(row)?;
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn yield_orchestration_job(
        &mut self,
        command: YieldOrchestrationJob,
    ) -> Result<CommandOutcome<YieldedOrchestrationJob>, RepositoryError> {
        command.validate_at(Utc::now(), self.plan_limits)?;
        let plan_digest = match &command.outcome {
            OrchestrationYield::TimerWait { plan } | OrchestrationYield::SignalWait { plan } => {
                Some(plan.canonical_digest(self.plan_limits)?)
            }
            OrchestrationYield::Retry { .. } => None,
        };
        let mut transaction = self.transaction.begin().await?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "kind": match &command.outcome {
                    OrchestrationYield::TimerWait { .. } => "timer_wait",
                    OrchestrationYield::SignalWait { .. } => "signal_wait",
                    OrchestrationYield::Retry { .. } => "retry",
                },
                "plan_digest": plan_digest,
            }),
            65_536,
        )?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            "orchestration.job.yield",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let yielded = load_yielded_orchestration_job(
                &mut transaction,
                &command.fence.tenant_id,
                &command.fence.job_id,
                command.outcome.state(),
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(yielded));
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
        let wait_source = if let OrchestrationYield::TimerWait { plan }
        | OrchestrationYield::SignalWait { plan } = &command.outcome
        {
            let digest = plan_digest.as_ref().ok_or_else(|| {
                RepositoryError::CorruptRow("Timer Plan digest missing".to_owned())
            })?;
            require_exact_runtime_plan(&mut transaction, &parents.run, plan, digest).await?;
            let source =
                load_controller_source_node(&mut transaction, &observed, &parents, plan).await?;
            let node = plan.node(&source.plan_node_key)?.clone();
            let kind_matches = matches!(
                (&command.outcome, &node),
                (
                    OrchestrationYield::TimerWait { .. },
                    RuntimeNode::TimerWait { .. }
                ) | (
                    OrchestrationYield::SignalWait { .. },
                    RuntimeNode::SignalWait { .. }
                )
            );
            if !kind_matches {
                return Err(RepositoryError::Conflict(
                    "orchestration durable wait exact Plan node",
                ));
            }
            Some((source, node))
        } else {
            None
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
            return Err(RepositoryError::Conflict("orchestration Job"));
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let (next, wake_contract, wait_due_at, node_payload) = match &command.outcome {
            OrchestrationYield::TimerWait { .. } => {
                let (source, node) = wait_source.as_ref().ok_or_else(|| {
                    RepositoryError::CorruptRow("Timer Plan source missing".to_owned())
                })?;
                let RuntimeNode::TimerWait {
                    delay_milliseconds, ..
                } = node
                else {
                    return Err(RepositoryError::CorruptRow(
                        "Timer Plan source kind changed".to_owned(),
                    ));
                };
                let timer_deadline = database_now
                    + Duration::milliseconds(i64::try_from(*delay_milliseconds).map_err(|_| {
                        RepositoryError::InvalidInput("Timer delay exceeds i64".to_owned())
                    })?);
                let wake_contract = WakeContract {
                    kind: insight_platform_jobs::WakeKind::Timer,
                    generation: u64::try_from(command.fence.lease_epoch).map_err(|_| {
                        RepositoryError::InvalidInput("Timer wake generation".to_owned())
                    })?,
                    accepted_sources: vec![WakeSource::Timer],
                    expected_response_schema_digest: None,
                    opaque_state_digest: None,
                    next_poll_at: None,
                    poll_count: 0,
                    poll_limit: 0,
                    callback_binding_digest: None,
                    deadline: current.deadline.min(parents.run.deadline),
                };
                let next = decide_job_wait(
                    &job_projection(&current)?,
                    &domain_job_fence(&command.fence)?,
                    database_now,
                    wake_contract.clone(),
                )?;
                let payload = TypedPayload::with_limit(
                    1,
                    &StoredTimerWaitPayload {
                        plan_node_key: source.plan_node_key.clone(),
                        due_at: timer_deadline,
                        resolution: None,
                    },
                    262_144,
                )?;
                let wait_due_at = timer_deadline
                    .min(current.deadline)
                    .min(parents.run.deadline);
                (next, Some(wake_contract), Some(wait_due_at), Some(payload))
            }
            OrchestrationYield::SignalWait { .. } => {
                let (source, node) = wait_source.as_ref().ok_or_else(|| {
                    RepositoryError::CorruptRow("Signal Plan source missing".to_owned())
                })?;
                let RuntimeNode::SignalWait {
                    signal_key,
                    payload,
                    timeout_milliseconds,
                    ..
                } = node
                else {
                    return Err(RepositoryError::CorruptRow(
                        "Signal Plan source kind changed".to_owned(),
                    ));
                };
                let signal_key_digest: Sha256Digest = canonical_digest(&serde_json::json!({
                    "signal_key": signal_key,
                }))
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
                .parse()
                .map_err(
                    |failure: insight_platform_contracts::NominalTypeError| {
                        RepositoryError::InvalidInput(failure.to_string())
                    },
                )?;
                let wake_contract = WakeContract {
                    kind: insight_platform_jobs::WakeKind::Signal,
                    generation: u64::try_from(command.fence.lease_epoch).map_err(|_| {
                        RepositoryError::InvalidInput("Signal wake generation".to_owned())
                    })?,
                    accepted_sources: vec![WakeSource::Signal, WakeSource::Timeout],
                    expected_response_schema_digest: payload
                        .as_ref()
                        .map(|port| port.schema_digest().clone()),
                    opaque_state_digest: Some(signal_key_digest),
                    next_poll_at: None,
                    poll_count: 0,
                    poll_limit: 0,
                    callback_binding_digest: None,
                    deadline: (database_now
                        + Duration::milliseconds(i64::try_from(*timeout_milliseconds).map_err(
                            |_| {
                                RepositoryError::InvalidInput(
                                    "Signal timeout exceeds i64".to_owned(),
                                )
                            },
                        )?))
                    .min(current.deadline)
                    .min(parents.run.deadline),
                };
                let next = decide_job_wait(
                    &job_projection(&current)?,
                    &domain_job_fence(&command.fence)?,
                    database_now,
                    wake_contract.clone(),
                )?;
                let payload = TypedPayload::with_limit(
                    1,
                    &StoredSignalWaitPayload {
                        plan_node_key: source.plan_node_key.clone(),
                        signal_key: signal_key.clone(),
                        payload_port: payload.clone(),
                        resolution: None,
                    },
                    262_144,
                )?;
                let wait_due_at = wake_contract.deadline;
                (next, Some(wake_contract), Some(wait_due_at), Some(payload))
            }
            OrchestrationYield::Retry { retry_at } => (
                decide_job_retry(
                    &job_projection(&current)?,
                    &domain_job_fence(&command.fence)?,
                    database_now,
                    *retry_at,
                )?,
                None,
                None,
                None,
            ),
        };
        settle_locked_job_quota_bundle(
            &mut transaction,
            &current,
            &quota_accounts,
            &command.mutations.quota_entry_ids,
            &command.request_digest,
        )
        .await?;
        let mut yielded = mutate_yielded_orchestration_job(
            &mut transaction,
            &current,
            &next,
            &parents,
            wake_contract,
            wait_due_at,
            node_payload.as_ref(),
            database_now,
        )
        .await?;
        yielded.settled_quota_account_ids = quota_accounts
            .iter()
            .map(|account| account.quota_account_id.clone())
            .collect();
        append_orchestration_yield_events(
            &mut transaction,
            &yielded,
            &command.mutations,
            &receipt_payload,
        )
        .await?;
        terminalize_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            &command.request_digest,
            command.outcome.state().as_str(),
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(yielded))
    }

    pub async fn fail_orchestration_job(
        &mut self,
        command: FailOrchestrationJob,
    ) -> Result<CommandOutcome<FailedOrchestrationJob>, RepositoryError> {
        command.validate_at(Utc::now(), self.plan_limits)?;
        let plan_digest = command
            .plan
            .canonical_digest(self.plan_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let receipt_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "cause": command.cause,
                "plan_digest": plan_digest,
            }),
            262_144,
        )?;
        let mut transaction = self.transaction.begin().await?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            "orchestration.job.fail",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            let failed = load_failed_orchestration_job(
                &mut transaction,
                &command.fence,
                &command.plan,
                &plan_digest,
                &command.cause,
                &command.mutations,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(failed));
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
        let runtime_node = command.plan.node(&source_node.plan_node_key)?;
        if let OrchestrationFailureCause::Committed { failure } = &command.cause {
            let payload: OrchestrationJobPayload =
                decode_orchestration_job_payload(&observed.payload)?;
            if matches!(
                runtime_node,
                RuntimeNode::ModelLoop { .. }
                    | RuntimeNode::CapabilityCall { .. }
                    | RuntimeNode::ContextQuery { .. }
            ) && payload.convergence_failure.as_ref() != Some(failure)
            {
                return Err(RepositoryError::Conflict(
                    "committed failure convergence evidence",
                ));
            }
        } else if let OrchestrationFailureCause::Controller { observation } = &command.cause {
            require_exact_controller_observation(
                &mut transaction,
                &observed,
                &parents,
                &source_node,
                runtime_node,
                observation,
                CompletionValidation {
                    plan: &command.plan,
                    context: self.context_query_limits,
                    model: self.model_turn_limits,
                    scope: self.scope_environment_limits,
                },
            )
            .await?;
            if let Some(derived) = &command.derived_expression {
                require_exact_derived_expression_evidence(
                    &mut transaction,
                    &observed,
                    &parents,
                    &source_node,
                    runtime_node,
                    observation,
                    &DerivedExpressionCommitEvidence {
                        materialized_inputs: derived.materialized_inputs.clone(),
                        evaluation: derived.evaluation.clone(),
                        output_value_ids: Vec::new(),
                    },
                    self.plan_limits.expression,
                    self.scope_environment_limits,
                )
                .await?;
            }
        }
        let (failure, controller_code) =
            derive_orchestration_failure(&command.cause, runtime_node)?;
        require_failure_references(&mut transaction, &parents.run, &failure).await?;
        let error_route = find_matching_error_boundary(
            &mut transaction,
            &parents.run,
            &parents.node_id,
            &command.plan,
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
        validate_failure_mutation_shape(&command.mutations, shape.is_some(), error_route.as_ref())?;

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
            return Err(RepositoryError::Conflict("orchestration failed Job"));
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let next_job = decide_job_terminal(
            &job_projection(&current)?,
            &domain_job_fence(&command.fence)?,
            database_now,
            JobState::Failed,
        )?;
        settle_locked_job_quota_bundle(
            &mut transaction,
            &current,
            &quota_accounts,
            &command.mutations.quota_entry_ids,
            &command.request_digest,
        )
        .await?;
        let failure_payload = TypedPayload::with_limit(1, &failure, 65_536)?;
        let mut failed = mutate_failed_orchestration_job(
            &mut transaction,
            &current,
            &next_job,
            &parents,
            &source_node,
            shape.as_ref(),
            error_route.as_ref(),
            &command.mutations,
            &command.plan,
            failure,
            controller_code,
            &failure_payload,
            &command.request_digest,
            database_now,
        )
        .await?;
        failed.settled_quota_account_ids = quota_accounts
            .iter()
            .map(|account| account.quota_account_id.clone())
            .collect();
        append_failed_orchestration_events(
            &mut transaction,
            &failed,
            &command.mutations,
            &plan_digest,
            &failure_payload,
        )
        .await?;
        terminalize_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.mutations.receipt_id,
            &command.request_digest,
            "orchestration_job_failed",
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(failed))
    }

    pub async fn wake_orchestration_job(
        &mut self,
        command: WakeOrchestrationJob,
    ) -> Result<CommandOutcome<WokenOrchestrationJob>, RepositoryError> {
        command.validate_at(Utc::now(), self.expression_inline_limits)?;
        let tenant_id = command.tenant_id.to_string();
        let job_id = command.job_id.to_string();
        let mut transaction = self.transaction.begin().await?;
        let signal_principal = match command.signal_authority.as_ref() {
            Some(authority) => {
                let snapshot = load_current_principal_snapshot(
                    &mut transaction,
                    &authority.tenant_id,
                    &authority.principal_id,
                    authority.principal_kind,
                )
                .await?;
                if !snapshot.permissions.contains(Permission::AgentRun) {
                    return Err(RepositoryError::PermissionDenied);
                }
                Some(snapshot)
            }
            None => None,
        };
        let signal_payload_evidence = command.signal_payload.as_ref().map(|payload| {
            serde_json::json!({
                "classification": payload.classification.as_str(),
                "content_digest": payload.content_digest,
                "schema_digest": payload.schema_digest,
                "value_id": payload.value_id,
            })
        });
        let receipt_payload = TypedPayload::with_limit(
            1,
            &OrchestrationWakeReceiptPayload {
                expected_job_version: command.expected_job_version,
                expected_wake_generation: command.expected_wake_generation,
                job_id: command.job_id.clone(),
                source: command.source.as_str().to_owned(),
                signal_key: command.signal_key.clone(),
                signal_payload_evidence,
                signal_principal,
            },
            65_536,
        )?;
        if claim_job_wake_receipt(&mut transaction, &command, &receipt_payload).await? {
            let woken = load_woken_orchestration_job(&mut transaction, &tenant_id, &job_id).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(woken));
        }
        let observed = load_job_by_text(&mut transaction, &tenant_id, &job_id).await?;
        require_orchestration_job(&observed)?;
        let signal_run_id = command
            .signal_authority
            .as_ref()
            .map(|authority| authority.run_id.to_string());
        if signal_run_id
            .as_ref()
            .is_some_and(|run_id| observed.run_id.as_ref() != Some(run_id))
        {
            return Err(RepositoryError::NotFound("Run signal owner"));
        }
        if observed.state != JobState::Waiting.as_str()
            || observed.version != command.expected_job_version
            || observed.wake_generation
                != i64::try_from(command.expected_wake_generation).unwrap_or_default()
        {
            return Err(RepositoryError::Conflict("orchestration wake first-winner"));
        }
        let parents = lock_waiting_orchestration_job_parents(&mut transaction, &observed).await?;
        let current = load_job_for_update_by_text(&mut transaction, &tenant_id, &job_id).await?;
        if current.version != command.expected_job_version
            || current.version != observed.version
            || current.payload.digest != observed.payload.digest
        {
            return Err(RepositoryError::Conflict("waiting orchestration Job"));
        }
        let node_row = sqlx::query(
            r#"
            SELECT plan_node_key, scope_id, payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'waiting'
            FOR UPDATE
            "#,
        )
        .bind(&observed.tenant_id)
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::Conflict("waiting durable-wait Node"))?;
        let node_payload = payload_from_row(
            &node_row,
            "payload_schema_version",
            "payload",
            "payload_digest",
        )?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let stored_plan_node_key = PlanNodeKey::new(node_row.try_get("plan_node_key")?)?;
        let current_wake = job_projection(&current)?.wake.ok_or_else(|| {
            RepositoryError::CorruptRow("waiting Job has no WakeContract".to_owned())
        })?;
        let resolved_node_payload = match current.wake_kind.as_deref() {
            Some("timer") => {
                let mut timer: StoredTimerWaitPayload =
                    decode_typed_payload(&node_payload, "waiting Timer Node")?;
                if command.source != WakeSource::Timer
                    || command.signal_key.is_some()
                    || command.signal_payload.is_some()
                    || timer.plan_node_key != stored_plan_node_key
                    || timer.resolution.is_some()
                {
                    return Err(RepositoryError::Conflict("Timer wake owner evidence"));
                }
                if database_now < timer.due_at {
                    return Err(RepositoryError::Conflict("Timer is not due"));
                }
                timer.resolution = Some(DurableWaitOutcome::Succeeded);
                TypedPayload::with_limit(1, &timer, 262_144)?
            }
            Some("signal") => {
                let mut signal: StoredSignalWaitPayload =
                    decode_typed_payload(&node_payload, "waiting Signal Node")?;
                let signal_arrived = command.source == WakeSource::Signal
                    && command.signal_key.as_deref() == Some(signal.signal_key.as_str());
                let signal_timed_out = command.source == WakeSource::Timeout
                    && command.signal_key.is_none()
                    && command.signal_payload.is_none()
                    && database_now >= current_wake.deadline;
                if (!signal_arrived && !signal_timed_out)
                    || signal.plan_node_key != stored_plan_node_key
                    || signal.resolution.is_some()
                    || (signal_arrived
                        && signal.payload_port.is_some() != command.signal_payload.is_some())
                {
                    return Err(RepositoryError::Conflict("Signal wake owner evidence"));
                }
                let payload_reference =
                    command
                        .signal_payload
                        .as_ref()
                        .map(|payload| ExactRunValueRef {
                            value_id: payload.value_id.clone(),
                            schema_digest: payload.schema_digest.clone(),
                            content_digest: payload.content_digest.clone(),
                        });
                if signal
                    .payload_port
                    .as_ref()
                    .zip(payload_reference.as_ref())
                    .is_some_and(|(port, payload)| payload.schema_digest != *port.schema_digest())
                {
                    return Err(RepositoryError::Conflict("Signal payload schema evidence"));
                }
                if let (Some(port), Some(payload), Some(reference)) = (
                    signal.payload_port.as_ref(),
                    command.signal_payload.as_ref(),
                    payload_reference.as_ref(),
                ) {
                    insert_signal_payload_value(
                        &mut transaction,
                        &current.tenant_id,
                        &parents.run.run_id,
                        &parents.node_id,
                        payload,
                    )
                    .await?;
                    bind_run_value_to_scope(
                        &mut transaction,
                        &current.tenant_id,
                        &parents.run.run_id,
                        node_row.try_get("scope_id")?,
                        port,
                        reference,
                        self.scope_environment_limits,
                    )
                    .await?;
                }
                signal.resolution = Some(StoredSignalResolution {
                    outcome: if signal_timed_out {
                        DurableWaitOutcome::TimedOut
                    } else {
                        DurableWaitOutcome::Succeeded
                    },
                    payload: payload_reference,
                });
                TypedPayload::with_limit(1, &signal, 262_144)?
            }
            _ => {
                return Err(RepositoryError::Conflict(
                    "unsupported orchestration wake kind",
                ))
            }
        };
        let next = decide_job_wake(
            &job_projection(&current)?,
            command.expected_wake_generation,
            command.source,
            database_now,
        )?;
        let woken = mutate_woken_orchestration_job(
            &mut transaction,
            &current,
            &next,
            &parents,
            &resolved_node_payload,
            database_now,
        )
        .await?;
        append_orchestration_wake_events(
            &mut transaction,
            &woken,
            &command.mutations,
            &receipt_payload,
        )
        .await?;
        terminalize_job_wake_receipt(&mut transaction, &command, "consumed").await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(woken))
    }

    pub async fn drive_due_orchestration_retries(
        &mut self,
        command: DriveDueOrchestrationRetries,
    ) -> Result<SafetyScanPage<PromotedOrchestrationRetry>, RepositoryError> {
        command.validate(self.recovery_batch_limit, self.recovery_shard_limit)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT job.*, node.version AS retry_node_version, job.retry_at AS scan_sort_at
            FROM insight_platform.jobs AS job
            JOIN insight_platform.runs AS run
              ON run.tenant_id = job.tenant_id AND run.run_id = job.run_id
            JOIN insight_platform.run_nodes AS node
              ON node.tenant_id = job.tenant_id AND node.node_id = job.node_id
            JOIN insight_platform.run_nodes AS scope
              ON scope.tenant_id = node.tenant_id AND scope.node_id = node.scope_id
            WHERE job.work_class = 'orchestration'
              AND job.owner_kind = 'node_execution' AND job.owner_id = node.node_id
              AND job.state = 'retry_scheduled' AND job.retry_at <= $1
              AND job.terminal_at IS NULL AND job.worker_id IS NULL
              AND job.attempt_no < job.attempt_limit AND job.deadline > $1
              AND run.state = 'running' AND run.terminal_at IS NULL AND run.deadline > $1
              AND run.current_payload #>> '{control,pause_requested}' = 'false'
              AND run.current_payload #> '{control,cancel_requested_at}' = 'null'::jsonb
              AND run.current_payload #> '{control,timeout_requested_at}' = 'null'::jsonb
              AND node.record_kind = 'node_execution' AND node.state = 'retry_scheduled'
              AND node.retry_at = job.retry_at AND node.terminal_at IS NULL
              AND scope.record_kind = 'scope_instance' AND scope.state = 'open'
              AND scope.terminal_at IS NULL
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $4) = $3
              AND (
                  $5::timestamptz IS NULL OR
                  (job.retry_at, job.tenant_id, job.job_id) >
                      ($5::timestamptz, $6::text, $7::text)
              )
            ORDER BY job.retry_at, job.tenant_id, job.job_id
            LIMIT $2
            FOR UPDATE OF run, node, scope, job SKIP LOCKED
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
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| safety_scan_cursor_from_row(row, "job_id", ResourceKind::Job))
            .transpose()?;
        let mut diagnostics = Vec::new();
        let mut promoted = Vec::with_capacity(rows.len());
        for (row, slot) in rows.into_iter().zip(command.slots.iter()) {
            let mut object_transaction = transaction.begin().await?;
            let object_result: Result<(), RepositoryError> = async {
                let node_version: i64 = row.try_get("retry_node_version")?;
                let current = crate::repository::persisted_job_from_row(row)?;
                require_orchestration_job(&current)?;
                let next = decide_job_retry_due(
                    &crate::recovery_isolation::job(
                        job_projection(&current),
                        &current,
                        insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
                    )?,
                    database_now,
                )?;
                let job = sqlx::query(
                    r#"
                UPDATE insight_platform.jobs
                SET state = 'ready', version = $4, scheduled_at = $5, retry_at = NULL,
                    updated_at = $5
                WHERE tenant_id = $1 AND job_id = $2 AND version = $3
                  AND state = 'retry_scheduled' AND worker_id IS NULL
                RETURNING *
                "#,
                )
                .bind(&current.tenant_id)
                .bind(&current.job_id)
                .bind(current.version)
                .bind(i64::try_from(next.version).map_err(|_| {
                    RepositoryError::InvalidInput("Job version exceeds bigint".to_owned())
                })?)
                .bind(database_now)
                .fetch_optional(&mut *object_transaction)
                .await?
                .ok_or(RepositoryError::Conflict("due orchestration Job retry"))?;
                let job = job_from_row(job)?;
                let node_id = current.node_id.as_deref().ok_or_else(|| {
                    RepositoryError::CorruptRow("orchestration Job has no Node".to_owned())
                })?;
                let next_node_version: i64 = sqlx::query_scalar(
                    r#"
                UPDATE insight_platform.run_nodes
                SET state = 'ready', version = version + 1, retry_at = NULL,
                    updated_at = $4
                WHERE tenant_id = $1 AND node_id = $2 AND version = $3
                  AND record_kind = 'node_execution' AND state = 'retry_scheduled'
                  AND terminal_at IS NULL
                RETURNING version
                "#,
                )
                .bind(&current.tenant_id)
                .bind(node_id)
                .bind(node_version)
                .bind(database_now)
                .fetch_optional(&mut *object_transaction)
                .await?
                .ok_or(RepositoryError::Conflict("due orchestration Node retry"))?;
                let payload = TypedPayload::with_limit(
                    1,
                    &serde_json::json!({
                        "job_id": job.job_id,
                        "lease_generation": job.lease_epoch,
                        "retry_observed_at": database_now,
                    }),
                    65_536,
                )?;
                let run_id = job.run_id.as_deref().ok_or_else(|| {
                    RepositoryError::CorruptRow("orchestration Job has no Run".to_owned())
                })?;
                append_scheduler_event(
                    &mut object_transaction,
                    &job.tenant_id,
                    &slot.node_event_id,
                    &slot.node_outbox_id,
                    "node_execution",
                    node_id,
                    next_node_version,
                    Some(run_id),
                    "node.retry_ready",
                    &payload,
                )
                .await?;
                append_scheduler_event(
                    &mut object_transaction,
                    &job.tenant_id,
                    &slot.job_event_id,
                    &slot.job_outbox_id,
                    "job",
                    &job.job_id,
                    job.version,
                    Some(run_id),
                    "job.retry_ready",
                    &payload,
                )
                .await?;
                promoted.push(PromotedOrchestrationRetry {
                    job,
                    node_id: node_id.to_owned(),
                    node_version: next_node_version,
                });
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
            safety_scan_page(promoted, scanned_count, command.limit, last_cursor)
                .with_diagnostics(diagnostics),
        )
    }

    pub async fn drive_expired_orchestration_jobs(
        &mut self,
        command: DriveExpiredOrchestrationJobs,
    ) -> Result<SafetyScanPage<RecoveredOrchestrationJob>, RepositoryError> {
        command.validate(self.recovery_batch_limit, self.recovery_shard_limit)?;
        let mut transaction = self.transaction.begin().await?;
        let scan_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT job.*, job.lease_expires_at AS scan_sort_at
            FROM insight_platform.jobs AS job
            JOIN insight_platform.runs AS run
              ON run.tenant_id = job.tenant_id AND run.run_id = job.run_id
            JOIN insight_platform.run_nodes AS node
              ON node.tenant_id = job.tenant_id AND node.node_id = job.node_id
            JOIN insight_platform.run_nodes AS scope
              ON scope.tenant_id = node.tenant_id AND scope.node_id = node.scope_id
            WHERE job.work_class = 'orchestration'
              AND job.owner_kind = 'node_execution' AND job.owner_id = node.node_id
              AND job.state IN ('leased', 'running')
              AND job.lease_expires_at <= $1 AND job.terminal_at IS NULL
              AND job.attempt_no < job.attempt_limit AND job.deadline > $1 + interval '60 seconds'
              AND run.state = 'running' AND run.terminal_at IS NULL
              AND run.deadline > $1 + interval '60 seconds'
              AND run.current_payload #> '{control,cancel_requested_at}' = 'null'::jsonb
              AND run.current_payload #> '{control,timeout_requested_at}' = 'null'::jsonb
              AND node.record_kind = 'node_execution'
              AND (
                  (job.state = 'leased' AND node.state = 'ready') OR
                  (job.state = 'running' AND node.state = 'running')
              )
              AND node.terminal_at IS NULL
              AND scope.record_kind = 'scope_instance' AND scope.state = 'open'
              AND scope.terminal_at IS NULL
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $4) = $3
              AND (
                  $5::timestamptz IS NULL OR
                  (job.lease_expires_at, job.tenant_id, job.job_id) >
                      ($5::timestamptz, $6::text, $7::text)
              )
            ORDER BY job.lease_expires_at, job.tenant_id, job.job_id
            LIMIT $2
            "#,
        )
        .bind(scan_now)
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
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| safety_scan_cursor_from_row(row, "job_id", ResourceKind::Job))
            .transpose()?;
        let candidates = rows;
        let mut diagnostics = Vec::new();
        let mut recovered = Vec::with_capacity(candidates.len());
        for (row, slot) in candidates.into_iter().zip(command.slots.iter()) {
            let mut object_transaction = transaction.begin().await?;
            let object_result: Result<(), RepositoryError> = async {
                let observed = crate::repository::persisted_job_from_row(row)?;
                let quota_accounts = lock_job_quota_bundle(
                    &mut object_transaction,
                    &observed,
                    &slot.quota_entry_ids,
                )
                .await?;
                let expected_node_state = if observed.state == JobState::Leased.as_str() {
                    "ready"
                } else {
                    "running"
                };
                let parents = lock_orchestration_job_parents(
                    &mut object_transaction,
                    &observed,
                    expected_node_state,
                )
                .await?;
                let current = load_job_for_update_by_text(
                    &mut object_transaction,
                    &observed.tenant_id,
                    &observed.job_id,
                )
                .await?;
                if current.version != observed.version
                    || current.lease_epoch != observed.lease_epoch
                    || current.lease_expires_at != observed.lease_expires_at
                    || current.payload.digest != observed.payload.digest
                    || current.quota_reservation_id != observed.quota_reservation_id
                {
                    return Err(RepositoryError::Conflict("expired orchestration Job"));
                }
                let recovery_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
                    .fetch_one(&mut *object_transaction)
                    .await?;
                let payload: OrchestrationJobPayload = crate::recovery_isolation::job(
                    decode_orchestration_job_payload(&current.payload),
                    &current,
                    insight_platform_jobs::store::SafetyScanPhase::OwnerDecode,
                )?;
                payload
                    .validate()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
                let (target, retry_at) = if current.state == JobState::Leased.as_str() {
                    (JobState::Ready, None)
                } else {
                    let retry_at = recovery_now
                        .checked_add_signed(Duration::milliseconds(
                            i64::try_from(payload.retry_backoff_milliseconds).map_err(|_| {
                                RepositoryError::CorruptRow(
                                    "retry backoff exceeds signed milliseconds".to_owned(),
                                )
                            })?,
                        ))
                        .ok_or_else(|| {
                            RepositoryError::InvalidInput("retry deadline overflowed".to_owned())
                        })?;
                    (JobState::RetryScheduled, Some(retry_at))
                };
                let next = decide_expired_job_lease(
                    &crate::recovery_isolation::job(
                        job_projection(&current),
                        &current,
                        insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
                    )?,
                    u64::try_from(current.version).map_err(|_| {
                        RepositoryError::CorruptRow("negative Job version".to_owned())
                    })?,
                    u64::try_from(current.lease_epoch).map_err(|_| {
                        RepositoryError::CorruptRow("negative Job lease generation".to_owned())
                    })?,
                    recovery_now,
                    target,
                    retry_at,
                )?;
                settle_locked_job_quota_bundle(
                    &mut object_transaction,
                    &current,
                    &quota_accounts,
                    &slot.quota_entry_ids,
                    &current
                        .request_digest
                        .parse::<Sha256Digest>()
                        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                )
                .await?;
                let mut record = mutate_recovered_orchestration_job(
                    &mut object_transaction,
                    &current,
                    &next,
                    &parents,
                    recovery_now,
                )
                .await?;
                record.settled_quota_account_ids = quota_accounts
                    .iter()
                    .map(|account| account.quota_account_id.clone())
                    .collect();
                append_orchestration_recovery_events(&mut object_transaction, &record, slot)
                    .await?;
                recovered.push(record);
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
            safety_scan_page(recovered, scanned_count, command.limit, last_cursor)
                .with_diagnostics(diagnostics),
        )
    }

    pub async fn drive_due_orchestration_waits(
        &mut self,
        command: DriveDueOrchestrationWaits,
    ) -> Result<SafetyScanPage<WokenOrchestrationJob>, RepositoryError> {
        command.validate(self.recovery_batch_limit, self.recovery_shard_limit)?;
        let mut scan = self.transaction.begin().await?;
        let scan_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *scan)
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT job.*, job.scheduled_at AS scan_sort_at
            FROM insight_platform.jobs AS job
            JOIN insight_platform.runs AS run
              ON run.tenant_id = job.tenant_id AND run.run_id = job.run_id
            JOIN insight_platform.run_nodes AS node
              ON node.tenant_id = job.tenant_id AND node.node_id = job.node_id
            JOIN insight_platform.run_nodes AS scope
              ON scope.tenant_id = node.tenant_id AND scope.node_id = node.scope_id
            WHERE job.work_class = 'orchestration'
              AND job.owner_kind = 'node_execution' AND job.owner_id = node.node_id
              AND job.state = 'waiting' AND job.terminal_at IS NULL
              AND job.worker_id IS NULL AND job.wake_state = 'pending'
              AND job.wake_kind IN ('timer', 'signal')
              AND job.scheduled_at <= $1 AND job.deadline > $1
              AND run.state IN ('running', 'waiting') AND run.terminal_at IS NULL
              AND run.deadline > $1
              AND run.current_payload #> '{control,cancel_requested_at}' = 'null'::jsonb
              AND run.current_payload #> '{control,timeout_requested_at}' = 'null'::jsonb
              AND node.record_kind = 'node_execution' AND node.state = 'waiting'
              AND node.terminal_at IS NULL
              AND scope.record_kind = 'scope_instance' AND scope.state = 'open'
              AND scope.terminal_at IS NULL
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $4) = $3
              AND (
                  $5::timestamptz IS NULL OR
                  (job.scheduled_at, job.tenant_id, job.job_id) >
                      ($5::timestamptz, $6::text, $7::text)
              )
            ORDER BY job.scheduled_at, job.tenant_id, job.job_id
            LIMIT $2
            "#,
        )
        .bind(scan_now)
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
        .fetch_all(&mut *scan)
        .await?;
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| safety_scan_cursor_from_row(row, "job_id", ResourceKind::Job))
            .transpose()?;
        let candidates = rows;
        scan.commit().await?;

        let mut diagnostics = Vec::new();
        let mut woken = Vec::with_capacity(candidates.len());
        for (row, slot) in candidates.into_iter().zip(command.slots.into_iter()) {
            let result: Result<(), RepositoryError> = async {
                let candidate = crate::repository::persisted_job_from_row(row)?;
                let tenant_id: ResourceId = candidate.tenant_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?;
                let job_id: ResourceId = candidate.job_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?;
                let source = match candidate.wake_kind.as_deref() {
                    Some("timer") => WakeSource::Timer,
                    Some("signal") => WakeSource::Timeout,
                    _ => {
                        return Err(RepositoryError::CorruptRow(
                            "due orchestration wait kind changed".to_owned(),
                        ))
                    }
                };
                let outcome = self
                    .wake_orchestration_job(WakeOrchestrationJob {
                        tenant_id,
                        job_id,
                        expected_job_version: candidate.version,
                        expected_wake_generation: u64::try_from(candidate.wake_generation)
                            .map_err(|_| {
                                RepositoryError::CorruptRow("negative wake generation".to_owned())
                            })?,
                        source,
                        signal_key: None,
                        signal_payload: None,
                        idempotency_key_digest: slot.idempotency_key_digest,
                        request_digest: slot.request_digest,
                        receipt_expires_at: scan_now + Duration::hours(1),
                        signal_authority: None,
                        mutations: slot.mutations,
                    })
                    .await?;
                woken.push(match outcome {
                    CommandOutcome::Applied(record) | CommandOutcome::Replayed(record) => record,
                });
                Ok(())
            }
            .await;
            crate::recovery_isolation::collect(result, &mut diagnostics)?;
        }
        Ok(
            safety_scan_page(woken, scanned_count, command.limit, last_cursor)
                .with_diagnostics(diagnostics),
        )
    }
}
