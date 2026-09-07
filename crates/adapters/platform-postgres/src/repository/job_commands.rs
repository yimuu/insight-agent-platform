//! PostgreSQL job commands. Shared locks and atomicity remain in this adapter.
use super::*;
use insight_platform_jobs::store::SafetyScanPhase;

impl PgRepository {
    pub async fn create_job(&self, command: NewJob) -> Result<JobRecord, RepositoryError> {
        command.validate()?;
        let execution = crate::execution_requirements::StoredExecutionRequirement::new(
            &command.execution_requirement,
        )?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, invocation_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, effect_key_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'ready', $11, GREATEST($12, clock_timestamp()), $13, $14, $15, $16, $17, $18, $19, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), $20, $21, $22
            )
            RETURNING *
            "#,
        )
        .bind(command.tenant_id)
        .bind(command.job_id)
        .bind(command.job_kind)
        .bind(command.work_class)
        .bind(command.owner_kind)
        .bind(command.owner_id)
        .bind(command.trace_id.to_string())
        .bind(command.invocation_id)
        .bind(command.run_id)
        .bind(command.node_id)
        .bind(command.attempt_limit)
        .bind(command.scheduled_at)
        .bind(command.deadline)
        .bind(scheduler_priority_to_database(command.priority))
        .bind(command.request_digest)
        .bind(command.effect_key_digest)
        .bind(command.payload.schema_version)
        .bind(command.payload.value)
        .bind(command.payload.digest)
        .bind(execution.version).bind(execution.value).bind(execution.digest)
        .fetch_one(&self.pool)
        .await?;
        job_from_row(row)
    }

    pub async fn claim_jobs(&self, command: ClaimJobs) -> Result<Vec<JobRecord>, RepositoryError> {
        command.validate()?;
        self.claim_jobs_filtered(command, None, None, None).await
    }

    /// Claims RegistryValidation work only for tenants where the configured validator is an
    /// active service identity with the exact write permission required by the Job payload.
    /// The terminal commit repeats the authorization check and remains the semantic authority;
    /// this predicate prevents an ineligible process from stealing another validator's lease.
    pub async fn claim_registry_validation_jobs(
        &self,
        command: ClaimJobs,
        validator_principal_id: &ResourceId,
    ) -> Result<Vec<JobRecord>, RepositoryError> {
        if command.work_class != WorkClass::RegistryValidation.as_str()
            || validator_principal_id.kind() != ResourceKind::Principal
        {
            return Err(RepositoryError::InvalidInput(
                "RegistryValidation claims require a validator Principal".to_owned(),
            ));
        }
        validate_claim_manifest(&command.worker_manifest, WorkClass::RegistryValidation)?;
        validate_claim_bounds(
            &command.worker_id,
            command.limit,
            command.lease_milliseconds,
            &command.lease_token_digests,
        )?;
        self.claim_jobs_filtered(command, None, Some(validator_principal_id), None)
            .await
    }

    /// Claims Artifact work through a closed physical-role lane. The typed Job-kind predicate is part of
    /// the same `FOR UPDATE SKIP LOCKED` transaction as the lease, so one Artifact role cannot
    /// temporarily steal another role's work and starve its dedicated queue.
    pub async fn claim_artifact_jobs(
        &self,
        command: ClaimArtifactJobs,
    ) -> Result<Vec<JobRecord>, RepositoryError> {
        command.validate()?;
        let job_kinds = command.role.job_kinds();
        self.claim_jobs_filtered(
            ClaimJobs {
                work_class: WorkClass::Artifact.as_str().to_owned(),
                worker_manifest: command.worker_manifest,
                worker_id: command.worker_id,
                limit: command.limit,
                lease_milliseconds: command.lease_milliseconds,
                lease_token_digests: command.lease_token_digests,
            },
            Some(job_kinds),
            None,
            None,
        )
        .await
    }

    /// Claims only durable MCP discovery work. Logical subscription Jobs share the MCP work
    /// class and owner kind, so the Job-kind predicate must participate in the locking query.
    pub async fn claim_mcp_discovery_jobs(
        &self,
        command: ClaimJobs,
    ) -> Result<Vec<JobRecord>, RepositoryError> {
        command.validate()?;
        if command.work_class != WorkClass::Mcp.as_str() {
            return Err(RepositoryError::InvalidInput(
                "MCP discovery claim requires the MCP work class".to_owned(),
            ));
        }
        self.claim_jobs_filtered(command, Some(&["mcp_discovery"]), None, None)
            .await
    }

    /// Claims only durable logical MCP subscription work. Discovery Jobs share the MCP work
    /// class and owner kind, so the Job-kind predicate must participate in the locking query.
    pub async fn claim_mcp_subscription_jobs(
        &self,
        command: ClaimJobs,
    ) -> Result<Vec<JobRecord>, RepositoryError> {
        command.validate()?;
        if command.work_class != WorkClass::Mcp.as_str() {
            return Err(RepositoryError::InvalidInput(
                "MCP subscription claim requires the MCP work class".to_owned(),
            ));
        }
        self.claim_jobs_filtered(command, Some(&["mcp_subscription"]), None, None)
            .await
    }

    /// Claims only Context Dataset build work. Dataset construction has an independent
    /// deployment, queue, connection pool, and capacity lane from online Context queries, so its
    /// Job-kind predicate must participate in the locking query rather than being filtered after
    /// a generic Context lease is acquired.
    pub async fn claim_context_dataset_build_jobs(
        &self,
        command: ClaimJobs,
    ) -> Result<Vec<JobRecord>, RepositoryError> {
        command.validate()?;
        if command.work_class != WorkClass::Context.as_str() {
            return Err(RepositoryError::InvalidInput(
                "Context Dataset build claim requires the Context work class".to_owned(),
            ));
        }
        self.claim_jobs_filtered(command, Some(&["context_dataset_build"]), None, None)
            .await
    }

    pub async fn claim_context_dataset_build_jobs_for_sources(
        &self,
        command: ClaimJobs,
        source_binding_digests: &[Sha256Digest],
    ) -> Result<Vec<JobRecord>, RepositoryError> {
        command.validate()?;
        if command.work_class != WorkClass::Context.as_str()
            || source_binding_digests.is_empty()
            || source_binding_digests.len() > 64
        {
            return Err(RepositoryError::InvalidInput(
                "Context Dataset source claim closure is invalid".to_owned(),
            ));
        }
        let mut unique = source_binding_digests
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        unique.sort();
        unique.dedup();
        if unique.len() != source_binding_digests.len() {
            return Err(RepositoryError::InvalidInput(
                "Context Dataset source claim closure contains duplicates".to_owned(),
            ));
        }
        self.claim_jobs_filtered(
            command,
            Some(&["context_dataset_build"]),
            None,
            Some(unique),
        )
        .await
    }

    async fn claim_jobs_filtered(
        &self,
        command: ClaimJobs,
        job_kinds: Option<&'static [&'static str]>,
        authorized_principal_id: Option<&ResourceId>,
        source_binding_digests: Option<Vec<String>>,
    ) -> Result<Vec<JobRecord>, RepositoryError> {
        let class = command
            .work_class
            .parse::<WorkClass>()
            .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
        validate_claim_manifest(&command.worker_manifest, class)?;
        let mut transaction = self.pool.begin().await?;
        let Some(admission) = crate::claim_admission::PreparedClaimAdmission::prepare(
            &mut transaction,
            class,
            DEFAULT_SCHEDULER_LIMITS,
        )
        .await?
        else {
            transaction.commit().await?;
            return Ok(Vec::new());
        };
        admission.observe_diagnostics();
        let mut candidate_ids = admission.candidate_ids();
        if let Some(principal_id) = authorized_principal_id {
            candidate_ids=sqlx::query_scalar("SELECT candidate.job_id FROM insight_platform.jobs candidate JOIN insight_platform.tenant_principals binding ON binding.tenant_id=candidate.tenant_id JOIN insight_platform.principals principal ON principal.principal_id=binding.principal_id WHERE candidate.job_id=ANY($1) AND binding.principal_id=$2 AND binding.principal_kind='service_identity' AND binding.state='active' AND principal.state='active' AND binding.permissions_schema_version=1 AND binding.permissions->'permissions' ? ($3::jsonb->>(candidate.payload->>'resource_kind'))")
                .bind(candidate_ids).bind(principal_id.to_string()).bind(registry_validation_write_permission_map()).fetch_all(&mut *transaction).await?;
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let candidates = sqlx::query(
            r#"
            SELECT *
            FROM insight_platform.jobs AS candidate
            WHERE candidate.work_class = $1
              AND candidate.job_id = ANY($6)
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements($7::jsonb) capability
                WHERE capability->>'family'=candidate.execution_requirement_family
                  AND CASE candidate.execution_requirement_family
                    WHEN 'program' THEN capability->>'program_semantic_identity'=candidate.execution_semantic_identity
                      AND (capability->>'ir_abi_version')::integer=candidate.execution_ir_abi_version
                    WHEN 'agent_compilation' THEN capability->>'compiler_semantic_identity'=candidate.execution_semantic_identity
                    WHEN 'domain_operation' THEN capability->>'operation_abi_identity'=candidate.execution_semantic_identity
                      AND (capability->>'adapter') IS NOT DISTINCT FROM candidate.execution_adapter_identity
                    ELSE false END
              )
              AND job_kind <> 'mcp_oauth_pkce_cleanup'
              AND state IN ('ready', 'retry_scheduled')
              AND terminal_at IS NULL
              AND worker_id IS NULL
              AND (
                  attempt_no < attempt_limit
                  OR (
                      state = 'ready'
                      AND attempt_no > 0
                      AND attempt_no <= attempt_limit
                      AND started_at IS NOT NULL
                  )
              )
              AND scheduled_at <= $3
              AND (retry_at IS NULL OR retry_at <= $3)
              AND deadline > $3
              AND ($4::text[] IS NULL OR job_kind = ANY($4::text[]))
              AND (
                $5::text[] IS NULL
                OR payload #>> '{source_binding,canonical_digest}' = ANY($5::text[])
              )
            ORDER BY priority DESC, COALESCE(retry_at, scheduled_at), job_id
            LIMIT $2
            "#,
        )
        .bind(&command.work_class)
        .bind(i64::from(DEFAULT_SCHEDULER_LIMITS.maximum_tenants)*i64::from(DEFAULT_SCHEDULER_LIMITS.maximum_window_per_tenant))
        .bind(database_now)
        .bind(job_kinds.map(|kinds| kinds.to_vec()))
        .bind(source_binding_digests)
        .bind(&candidate_ids)
        .bind(serde_json::to_value(&command.worker_manifest.execution_capabilities.capabilities).map_err(|error|RepositoryError::InvalidInput(error.to_string()))?)
        .fetch_all(&mut *transaction)
        .await?;
        let mut diagnostics = Vec::new();
        let mut eligible = BTreeMap::new();
        let mut by_id = BTreeMap::new();
        for row in candidates {
            let Some(job) =
                crate::recovery_isolation::collect(persisted_job_from_row(row), &mut diagnostics)?
            else {
                continue;
            };
            if crate::recovery_isolation::collect(
                validate_claimed_job_payload(&job),
                &mut diagnostics,
            )?
            .is_none()
            {
                continue;
            }
            if !command
                .worker_manifest
                .execution_capabilities
                .supports(&job.execution_requirement)
            {
                continue;
            }
            let lane = match job.job_kind.as_str() {
                "artifact_delete" | "artifact_blob_cleanup" => {
                    insight_platform_contracts::SchedulingLane::RestrictedControl
                }
                _ => insight_platform_contracts::SchedulingLane::Business,
            };
            eligible.insert(
                job.job_id.clone(),
                crate::claim_admission::EligibleClaim {
                    lane,
                    mode: if job.state == "ready" && has_started_worker_attempt(&job) {
                        insight_platform_contracts::ClaimMode::Continuation
                    } else {
                        insight_platform_contracts::ClaimMode::NewAttempt
                    },
                    quota_costs: Vec::new(),
                },
            );
            by_id.insert(job.job_id.clone(), job);
        }
        // Domain-specific quotas are retained by their owning operation/invocation.
        // Generic claims do not invent a second reservation for the same fact.
        let decision = admission.select(&eligible, &BTreeMap::new(), command.limit)?;
        let mut selected = decision
            .admitted_job_ids
            .iter()
            .zip(&command.lease_token_digests)
            .map(|(id, token)| (by_id[&id.to_string()].clone(), token))
            .collect::<Vec<_>>();
        selected.sort_by(|(left, _), (right, _)| {
            (&left.tenant_id, &left.job_id).cmp(&(&right.tenant_id, &right.job_id))
        });
        let mut claimed = Vec::with_capacity(selected.len());
        for (candidate, lease_token_digest) in selected {
            let mut object_transaction = transaction.begin().await?;
            let result: Result<(), RepositoryError> = async {
                let current = load_job_for_update_by_text(
                    &mut object_transaction,
                    &candidate.tenant_id,
                    &candidate.job_id,
                )
                .await?;
                if current.version != candidate.version
                    || current.payload.digest != candidate.payload.digest
                    || current.execution_requirement != candidate.execution_requirement
                {
                    return Err(RepositoryError::Conflict("partition-selected Job changed"));
                }
                validate_claimed_job_payload(&current)?;
                let current_projection = crate::recovery_isolation::job(
                    job_projection(&current),
                    &current,
                    insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
                )?;
                let lease_policy = LeasePolicy {
                    requested_milliseconds: u64::try_from(command.lease_milliseconds).map_err(
                        |_| RepositoryError::InvalidInput("negative Job lease".to_owned()),
                    )?,
                    hard_maximum_milliseconds: u64::try_from(MAX_JOB_LEASE_MILLISECONDS)
                        .expect("positive Job lease hard maximum"),
                };
                // `started_at` can be set by a synchronous pre-worker phase while the durable
                // worker attempt count is still zero (for example, Artifact upload completion
                // handing the same Operation Job to its scan worker). Only a previously consumed
                // worker attempt is a continuation; otherwise this must take the normal first-claim
                // transition which increments `attempt_no` on start.
                let is_continuation = current.state == JobState::Ready.as_str()
                    && has_started_worker_attempt(&current);
                let next = if is_continuation {
                    decide_job_claim_continuation(
                        &current_projection,
                        database_now,
                        command.worker_id.clone(),
                        lease_token_digest.clone(),
                        lease_policy,
                    )?
                } else {
                    decide_job_claim(
                        &current_projection,
                        database_now,
                        command.worker_id.clone(),
                        lease_token_digest.clone(),
                        lease_policy,
                    )?
                };
                let lease = next.lease.as_ref().ok_or_else(|| {
                    RepositoryError::InvalidInput(
                        "claim decision did not create a lease".to_owned(),
                    )
                })?;
                let row = sqlx::query(
                    r#"
                UPDATE insight_platform.jobs
                SET state = $4, version = $5, attempt_no = $6, lease_epoch = $7,
                    worker_id = $8, lease_token_digest = $9,
                    lease_expires_at = $10, heartbeat_at = $11, retry_at = NULL,
                    started_at = CASE
                        WHEN state = 'retry_scheduled' THEN NULL
                        ELSE started_at
                    END,
                    updated_at = $11, attempt_build_digest = $12
                WHERE tenant_id = $1 AND job_id = $2 AND version = $3
                  AND state IN ('ready', 'retry_scheduled') AND worker_id IS NULL
                RETURNING *
                "#,
                )
                .bind(&current.tenant_id)
                .bind(&current.job_id)
                .bind(current.version)
                .bind(next.state.as_str())
                .bind(i64::try_from(next.version).map_err(|_| {
                    RepositoryError::InvalidInput("Job version exceeds bigint".to_owned())
                })?)
                .bind(i32::try_from(next.attempt_count).map_err(|_| {
                    RepositoryError::InvalidInput("Job attempt count exceeds integer".to_owned())
                })?)
                .bind(i64::try_from(next.lease_generation).map_err(|_| {
                    RepositoryError::InvalidInput("Job lease generation exceeds bigint".to_owned())
                })?)
                .bind(lease.worker_process_generation_id.to_string())
                .bind(lease.token_digest.to_string())
                .bind(lease.expires_at)
                .bind(lease.heartbeat_at)
                .bind(command.worker_manifest.worker_build_digest.to_string())
                .fetch_optional(&mut *object_transaction)
                .await?;
                if let Some(row) = row {
                    claimed.push(job_from_row(row)?);
                }
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
        let decision = admission.settle_actual(
            &eligible,
            &BTreeMap::new(),
            command.limit,
            claimed.iter().map(|record| record.job_id.clone()),
        )?;
        admission.persist(&mut transaction, &decision).await?;
        for diagnostic in &diagnostics {
            crate::recovery_isolation::observe(diagnostic);
        }
        transaction.commit().await?;
        Ok(claimed)
    }

    pub async fn start_job(&self, command: JobFence) -> Result<JobRecord, RepositoryError> {
        self.start_job_filtered(command, None).await
    }

    /// Starts only a previously claimed Context Dataset build Job. Keeping this check inside the
    /// same row-locking transaction prevents a Dataset Builder from starting online query or
    /// subscription work even when handed a syntactically valid foreign fence.
    pub async fn start_context_dataset_build_job(
        &self,
        command: JobFence,
    ) -> Result<JobRecord, RepositoryError> {
        self.start_job_filtered(command, Some(JobKind::ContextDatasetBuild))
            .await
    }

    async fn start_job_filtered(
        &self,
        command: JobFence,
        expected_job_kind: Option<JobKind>,
    ) -> Result<JobRecord, RepositoryError> {
        command.validate()?;
        let mut transaction = self.pool.begin().await?;
        let current =
            load_job_for_update_by_text(&mut transaction, &command.tenant_id, &command.job_id)
                .await?;
        if let Some(expected) = expected_job_kind {
            if current.work_class != WorkClass::Context.as_str()
                || current.job_kind != expected.as_str()
                || current.owner_kind != ResourceKind::ContextDataset.descriptor().name
            {
                return Err(RepositoryError::InvalidInput(
                    "Job is not a Context Dataset build work item".to_owned(),
                ));
            }
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let next = if has_started_worker_attempt(&current) {
            decide_job_resume(
                &job_projection(&current)?,
                &domain_job_fence(&command)?,
                database_now,
            )?
        } else {
            decide_job_start(
                &job_projection(&current)?,
                &domain_job_fence(&command)?,
                database_now,
            )?
        };
        let row =
            sqlx::query(
                r#"
            UPDATE insight_platform.jobs
            SET state = $4, version = $5, attempt_no = $6,
                started_at = COALESCE(started_at, $7),
                updated_at = $7
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
            RETURNING *
            "#,
            )
            .bind(&command.tenant_id)
            .bind(&command.job_id)
            .bind(current.version)
            .bind(next.state.as_str())
            .bind(i64::try_from(next.version).map_err(|_| {
                RepositoryError::InvalidInput("Job version exceeds bigint".to_owned())
            })?)
            .bind(i32::try_from(next.attempt_count).map_err(|_| {
                RepositoryError::InvalidInput("Job attempt count exceeds integer".to_owned())
            })?)
            .bind(database_now)
            .fetch_one(&mut *transaction)
            .await?;
        let record = job_from_row(row)?;
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn heartbeat_job(&self, command: HeartbeatJob) -> Result<JobRecord, RepositoryError> {
        command.fence.validate()?;
        if command.lease_milliseconds <= 0
            || command.lease_milliseconds > MAX_JOB_LEASE_MILLISECONDS
        {
            return Err(RepositoryError::InvalidInput(
                "lease duration is outside the platform bound".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let current = load_job_for_update_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
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
            RepositoryError::InvalidInput("heartbeat decision removed the Job lease".to_owned())
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
            .bind(&command.fence.tenant_id)
            .bind(&command.fence.job_id)
            .bind(current.version)
            .bind(i64::try_from(next.version).map_err(|_| {
                RepositoryError::InvalidInput("Job version exceeds bigint".to_owned())
            })?)
            .bind(lease.heartbeat_at)
            .bind(lease.expires_at)
            .fetch_one(&mut *transaction)
            .await?;
        let record = job_from_row(row)?;
        transaction.commit().await?;
        Ok(record)
    }

    /// Bounded owner recovery for RegistryValidation leases. Validation is read-only until its
    /// fenced terminal transaction, so an expired running attempt is safe to retry; no external
    /// effect reconciliation is required.
    pub async fn recover_expired_registry_validation_jobs(
        &self,
        after: Option<SafetyScanCursor>,
        limit: u16,
        retry_backoff_milliseconds: i64,
    ) -> Result<SafetyScanPage<ResourceId>, RepositoryError> {
        if limit == 0
            || limit > 256
            || retry_backoff_milliseconds <= 0
            || retry_backoff_milliseconds > 60_000
        {
            return Err(RepositoryError::InvalidInput(
                "RegistryValidation recovery bounds are invalid".to_owned(),
            ));
        }
        if let Some(cursor) = &after {
            cursor.validate(ResourceKind::Job)?;
        }
        let mut transaction = self.pool.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM insight_platform.jobs
            WHERE work_class = 'registry_validation'
              AND job_kind = 'registry_validation'
              AND owner_kind = 'job' AND owner_id = job_id
              AND state IN ('leased', 'running')
              AND terminal_at IS NULL AND lease_expires_at <= $1
              AND ($3::timestamptz IS NULL OR (lease_expires_at,tenant_id,job_id) > ($3,$4,$5))
            ORDER BY lease_expires_at, tenant_id, job_id
            FOR UPDATE SKIP LOCKED
            LIMIT $2
            "#,
        )
        .bind(database_now)
        .bind(i64::from(limit))
        .bind(after.as_ref().map(|cursor| cursor.sort_at))
        .bind(after.as_ref().map(|cursor| cursor.tenant_id.to_string()))
        .bind(after.as_ref().map(|cursor| cursor.item_id.to_string()))
        .fetch_all(&mut *transaction)
        .await?;
        let exhausted = rows.len() < usize::from(limit);
        let mut records = Vec::new();
        let mut diagnostics = Vec::new();
        let mut next_cursor = None;
        for row in rows {
            let diagnostic = crate::recovery_isolation::identity(
                &row,
                "job_id",
                ResourceKind::Job,
                SafetyScanPhase::JobDecode,
            )?;
            next_cursor = Some(SafetyScanCursor {
                sort_at: row.try_get("lease_expires_at")?,
                tenant_id: diagnostic.tenant_id.clone(),
                item_id: diagnostic.item_id.clone(),
            });
            let mut candidate = transaction.begin().await?;
            let outcome: Result<Option<ResourceId>, RepositoryError> = async {
                let current = crate::recovery_isolation::persisted(job_from_row(row), &diagnostic)?;
                let projection = crate::recovery_isolation::job(
                    (|| {
                        let payload: RegistryValidationJobPayload =
                            decode_versioned_payload(&current.payload, "Registry validation Job")?;
                        payload.validate_for_owner(&diagnostic.item_id)?;
                        if payload.execution_requirement != current.execution_requirement {
                            return Err(RepositoryError::CorruptRow(
                                "Registry validation recovery execution requirement disagrees"
                                    .into(),
                            ));
                        }
                        job_projection(&current)
                    })(),
                    &current,
                    SafetyScanPhase::OwnerValidation,
                )?;
                let retry_at = database_now
                    .checked_add_signed(Duration::milliseconds(retry_backoff_milliseconds))
                    .ok_or_else(|| {
                        RepositoryError::InvalidInput(
                            "RegistryValidation retry time overflowed".to_owned(),
                        )
                    })?;
                let (target, retry_at) = if database_now >= projection.deadline {
                    (JobState::TimedOut, None)
                } else if projection.state == JobState::Leased {
                    (JobState::Ready, None)
                } else if projection.attempt_count < projection.attempt_limit
                    && retry_at < projection.deadline
                {
                    (JobState::RetryScheduled, Some(retry_at))
                } else if projection.attempt_count >= projection.attempt_limit {
                    (JobState::Failed, None)
                } else {
                    // A valid live deadline cannot be terminalized early. The remaining
                    // window cannot fit the required backoff; revisit at its real deadline.
                    return Ok(None);
                };
                let next = decide_expired_job_lease(
                    &projection,
                    u64::try_from(current.version).map_err(|_| {
                        RepositoryError::CorruptRow(
                            "negative RegistryValidation Job version".to_owned(),
                        )
                    })?,
                    u64::try_from(current.lease_epoch).map_err(|_| {
                        RepositoryError::CorruptRow(
                            "negative RegistryValidation lease generation".to_owned(),
                        )
                    })?,
                    database_now,
                    target,
                    retry_at,
                )?;
                let terminal_at = matches!(
                    next.state,
                    JobState::Failed | JobState::TimedOut | JobState::ReconciliationRequired
                )
                .then_some(database_now);
                let result_digest = terminal_at
                    .map(|_| {
                        canonical_digest(&serde_json::json!({
                            "job_id": current.job_id,
                            "reason": "registry_validation_attempts_exhausted_or_timed_out",
                            "schema_version": 1,
                            "state": next.state,
                        }))
                    })
                    .transpose()
                    .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
                let affected = sqlx::query(
                    r#"
                UPDATE insight_platform.jobs
                SET state = $4, version = $5, worker_id = NULL,
                    lease_token_digest = NULL, lease_expires_at = NULL,
                    heartbeat_at = NULL, retry_at = $6, result_digest = $7,
                    terminal_at = $8, updated_at = $9
                WHERE tenant_id = $1 AND job_id = $2 AND version = $3
                  AND work_class = 'registry_validation'
                  AND job_kind = 'registry_validation'
                  AND state IN ('leased', 'running')
                "#,
                )
                .bind(&current.tenant_id)
                .bind(&current.job_id)
                .bind(current.version)
                .bind(next.state.as_str())
                .bind(i64::try_from(next.version).map_err(|_| {
                    RepositoryError::InvalidInput(
                        "RegistryValidation Job version exceeds bigint".to_owned(),
                    )
                })?)
                .bind(next.retry_at)
                .bind(result_digest)
                .bind(terminal_at)
                .bind(database_now)
                .execute(&mut *candidate)
                .await?
                .rows_affected();
                if affected != 1 {
                    return Err(RepositoryError::Conflict("expired RegistryValidation Job"));
                }
                Ok(Some(diagnostic.item_id.clone()))
            }
            .await;
            match outcome {
                Ok(Some(id)) => {
                    candidate.commit().await?;
                    records.push(id);
                }
                Ok(None) => candidate.rollback().await?,
                Err(RepositoryError::InvalidPersistedObject(diagnostic)) => {
                    candidate.rollback().await?;
                    diagnostics.push(diagnostic);
                }
                Err(error) => return Err(error),
            }
        }
        transaction.commit().await?;
        Ok(SafetyScanPage {
            records,
            diagnostics,
            next_cursor: if exhausted { None } else { next_cursor },
            exhausted,
        })
    }

    /// Atomically records a RegistryValidation success and terminals its fenced Job.
    ///
    /// This intentionally does not use `commit_job`: a registry validation Job's payload is its
    /// public Operation target and therefore remains immutable after the result is committed.
    pub async fn commit_registry_validation(
        &self,
        command: CommitRegistryValidation,
    ) -> Result<RegistryValidationCommitOutcome, RepositoryError> {
        command.validate()?;
        let receipt_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "job_id": command.fence.job_id,
                "validator_principal_id": command.validator_principal_id,
            }),
        )?;
        let mut transaction = self.pool.begin().await?;
        if claim_job_mutation_receipt(
            &mut transaction,
            &command.fence,
            &command.receipt_id,
            "registry_validation.commit",
            &command.idempotency_key_digest,
            &command.request_digest,
            &receipt_payload,
            command.receipt_expires_at,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(RegistryValidationCommitOutcome::Replayed);
        }

        let observed = load_job_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        if observed.work_class != WorkClass::RegistryValidation.as_str()
            || observed.job_kind != JobKind::RegistryValidation.as_str()
            || observed.owner_kind != ResourceKind::Job.descriptor().name
        {
            return Err(RepositoryError::InvalidInput(
                "Job is not a RegistryValidation work item".to_owned(),
            ));
        }
        let owner_id = observed.owner_id.parse::<ResourceId>().map_err(|_| {
            RepositoryError::CorruptRow("Registry validation Job owner ID is invalid".to_owned())
        })?;
        let payload: RegistryValidationJobPayload =
            decode_versioned_payload(&observed.payload, "Registry validation Job")?;
        payload
            .validate_for_owner(&owner_id)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if payload.job_id.to_string() != observed.job_id {
            return Err(RepositoryError::CorruptRow(
                "Registry validation Job payload ID does not match its row".to_owned(),
            ));
        }
        let tenant_id = ResourceId::parse_expected(&observed.tenant_id, ResourceKind::Tenant)
            .map_err(|_| {
                RepositoryError::CorruptRow(
                    "Registry validation Job tenant ID is invalid".to_owned(),
                )
            })?;
        let locked =
            load_resource_for_update(&mut transaction, &tenant_id, &payload.resource_id).await?;
        let current = load_job_for_update_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        if current.payload.digest != observed.payload.digest
            || current.owner_id != observed.owner_id
        {
            return Err(RepositoryError::Conflict(
                "Registry validation frozen input",
            ));
        }
        let completion_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        crate::execution_authorization::authorize_restricted_job_completion(
            &current,
            &command.fence,
            completion_now,
        )?;
        let kind = locked
            .resource_kind
            .parse::<RegistryResourceKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if kind != payload.resource_kind
            || locked.resource_id != payload.resource_id.to_string()
            || locked.version
                != i64::try_from(payload.expected_resource_version).map_err(|_| {
                    RepositoryError::CorruptRow(
                        "Registry validation Job expected resource version is invalid".to_owned(),
                    )
                })?
            || locked.lifecycle_state == "retired"
        {
            return Err(RepositoryError::Conflict("registry validation authority"));
        }
        let mut draft = decode_resource_draft(&locked.payload)?;
        require_ready_authoring_artifact(
            &mut transaction,
            &tenant_id,
            draft.document.authoring_package(),
        )
        .await?;
        require_ready_typed_plan_artifact(&mut transaction, &tenant_id, &draft.document).await?;
        require_ready_sandbox_runtime_bundle(&mut transaction, &tenant_id, &draft.document).await?;
        let exact_references = draft.document.exact_version_refs();
        validate_exact_version_refs_exist(&mut transaction, &tenant_id, &exact_references).await?;
        if let Some(compilation) = &command.compilation {
            for submitted in &compilation.deployment_features {
                let actual = crate::agent_feature_repository::derive_agent_deployment_features(
                    &mut transaction,
                    &tenant_id,
                    &submitted.deployment,
                )
                .await?;
                if actual != *submitted {
                    return Err(RepositoryError::Conflict(
                        "Agent deployment feature evidence",
                    ));
                }
            }
        }
        let summary = build_registry_validation_summary(
            &payload,
            &draft,
            &command.validator_digest,
            &command.validation_profile_digest,
            command.compilation.as_ref(),
        )
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        draft.validation = Some(summary.clone());
        let resource_payload = TypedPayload::new(1, &draft)?;
        let resource_row = sqlx::query(
            r#"
            UPDATE insight_platform.resources
            SET payload_schema_version = $4, payload = $5, payload_digest = $6,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND resource_id = $2 AND version = $3
            RETURNING tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                      draft_generation, active_version_id, active_deployment_id, version,
                      payload_schema_version, payload, payload_digest, created_at, updated_at
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(payload.resource_id.to_string())
        .bind(locked.version)
        .bind(resource_payload.schema_version)
        .bind(&resource_payload.value)
        .bind(&resource_payload.digest)
        .fetch_one(&mut *transaction)
        .await?;
        let resource = resource_from_row(resource_row)?;
        let result_digest = canonical_digest(
            &serde_json::to_value(&summary)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
        )
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let next = decide_job_terminal(
            &job_projection(&current)?,
            &domain_job_fence(&command.fence)?,
            database_now,
            JobState::Succeeded,
        )?;
        let job_row =
            sqlx::query(
                r#"
            UPDATE insight_platform.jobs
            SET state = $4, version = $5, result_digest = $6,
                worker_id = NULL, lease_token_digest = NULL,
                lease_expires_at = NULL, heartbeat_at = NULL,
                terminal_at = $7, updated_at = $7
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
            RETURNING *
            "#,
            )
            .bind(&command.fence.tenant_id)
            .bind(&command.fence.job_id)
            .bind(current.version)
            .bind(next.state.as_str())
            .bind(i64::try_from(next.version).map_err(|_| {
                RepositoryError::InvalidInput("Job version exceeds bigint".to_owned())
            })?)
            .bind(&result_digest)
            .bind(database_now)
            .fetch_one(&mut *transaction)
            .await?;
        let job = job_from_row(job_row)?;
        let resource_event_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "job_id": job.job_id,
                "validated_draft_digest": summary.validated_draft_digest,
                "validator_digest": summary.validator_digest,
            }),
        )?;
        let job_event_payload = TypedPayload::new(
            1,
            &serde_json::json!({
                "resource_id": resource.resource_id,
                "result_digest": result_digest,
            }),
        )?;
        insert_registry_validation_event(
            &mut transaction,
            &tenant_id,
            &command.resource_event_id,
            "resource",
            &resource.resource_id,
            resource.version,
            &job.trace,
            "resource.validation_recorded",
            &resource_event_payload,
        )
        .await?;
        insert_registry_validation_outbox(
            &mut transaction,
            &tenant_id,
            &command.resource_outbox_id,
            &command.resource_event_id,
            &job.trace,
        )
        .await?;
        insert_registry_validation_event(
            &mut transaction,
            &tenant_id,
            &command.job_event_id,
            "job",
            &job.job_id,
            job.version,
            &job.trace,
            "job.registry_validation_succeeded",
            &job_event_payload,
        )
        .await?;
        insert_registry_validation_outbox(
            &mut transaction,
            &tenant_id,
            &command.job_outbox_id,
            &command.job_event_id,
            &job.trace,
        )
        .await?;
        terminalize_job_mutation_receipt_with_reference(
            &mut transaction,
            &command.fence,
            &command.receipt_id,
            &command.request_digest,
            "committed",
            &resource.resource_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(RegistryValidationCommitOutcome::Committed {
            job: Box::new(job),
            resource: Box::new(resource),
        })
    }

    pub async fn commit_job(
        &self,
        command: CommitJob,
    ) -> Result<JobCommitOutcome, RepositoryError> {
        command.validate()?;
        let mut transaction = self.pool.begin().await?;
        let inserted_receipt = sqlx::query(
            r#"
            INSERT INTO insight_platform.receipts (
                tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
                dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
                payload_schema_version, payload, payload_digest, expires_at
            ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, 'job.commit', $5, $6,
                      'processing', $7, $8, $9, $10)
            ON CONFLICT (
                tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
                operation, idempotency_key_digest
            )
            DO NOTHING
            RETURNING receipt_id
            "#,
        )
        .bind(&command.fence.tenant_id)
        .bind(&command.receipt_id)
        .bind(&command.fence.job_id)
        .bind(command.fence.worker_id.to_string())
        .bind(&command.idempotency_key_digest)
        .bind(&command.request_digest)
        .bind(command.receipt_payload.schema_version)
        .bind(&command.receipt_payload.value)
        .bind(&command.receipt_payload.digest)
        .bind(command.receipt_expires_at)
        .fetch_optional(&mut *transaction)
        .await?;

        if inserted_receipt.is_none() {
            let existing = sqlx::query(
                r#"
                SELECT request_digest, state, disposition
                FROM insight_platform.receipts
                WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
                  AND scope_kind = 'job' AND scope_id = $2
                  AND dedupe_owner_id = $3 AND operation = 'job.commit'
                  AND idempotency_key_digest = $4
                FOR UPDATE
                "#,
            )
            .bind(&command.fence.tenant_id)
            .bind(&command.fence.job_id)
            .bind(command.fence.worker_id.to_string())
            .bind(&command.idempotency_key_digest)
            .fetch_one(&mut *transaction)
            .await?;
            let request_digest: String = existing.try_get("request_digest")?;
            if request_digest != command.request_digest {
                return Err(RepositoryError::IdempotencyConflict);
            }
            let state: String = existing.try_get("state")?;
            if state == "succeeded" {
                let disposition: Option<String> = existing.try_get("disposition")?;
                transaction.commit().await?;
                return Ok(JobCommitOutcome::Replayed { disposition });
            }
            return Err(RepositoryError::Conflict("job commit receipt"));
        }

        let current = load_job_for_update_by_text(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
        )
        .await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let next = decide_job_terminal(
            &job_projection(&current)?,
            &domain_job_fence(&command.fence)?,
            database_now,
            command.terminal_state.job_state(),
        )?;
        let row =
            sqlx::query(
                r#"
            UPDATE insight_platform.jobs
            SET state = $4, version = $5, result_digest = $6,
                payload_schema_version = $7, payload = $8, payload_digest = $9,
                worker_id = NULL, lease_token_digest = NULL,
                lease_expires_at = NULL, heartbeat_at = NULL,
                terminal_at = $10, updated_at = $10
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
            RETURNING *
            "#,
            )
            .bind(&command.fence.tenant_id)
            .bind(&command.fence.job_id)
            .bind(current.version)
            .bind(next.state.as_str())
            .bind(i64::try_from(next.version).map_err(|_| {
                RepositoryError::InvalidInput("Job version exceeds bigint".to_owned())
            })?)
            .bind(&command.result_digest)
            .bind(command.result_payload.schema_version)
            .bind(&command.result_payload.value)
            .bind(&command.result_payload.digest)
            .bind(database_now)
            .fetch_one(&mut *transaction)
            .await?;
        let job = job_from_row(row)?;

        sqlx::query(
            r#"
            INSERT INTO insight_platform.events (
                tenant_id, event_id, aggregate_kind, aggregate_id, aggregate_version,
                trace_id, run_id, event_type, visibility, payload_schema_version, payload,
                payload_digest
            ) VALUES ($1, $2, 'job', $3, $4, $5, $6, $7, 'internal', $8, $9, $10)
            "#,
        )
        .bind(&command.fence.tenant_id)
        .bind(&command.event_id)
        .bind(&command.fence.job_id)
        .bind(job.version)
        .bind(job.trace.trace_id.to_string())
        .bind(&job.run_id)
        .bind(&command.event_type)
        .bind(command.event_payload.schema_version)
        .bind(&command.event_payload.value)
        .bind(&command.event_payload.digest)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.outbox_events (
                tenant_id, outbox_id, event_id, trace_id
            ) VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(&command.fence.tenant_id)
        .bind(&command.outbox_id)
        .bind(&command.event_id)
        .bind(job.trace.trace_id.to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE insight_platform.receipts
            SET state = 'succeeded', disposition = 'committed', response_reference_id = $4,
                completed_at = clock_timestamp()
            WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
            "#,
        )
        .bind(&command.fence.tenant_id)
        .bind(&command.receipt_id)
        .bind(&command.request_digest)
        .bind(&command.fence.job_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(JobCommitOutcome::Committed(Box::new(job)))
    }
}
