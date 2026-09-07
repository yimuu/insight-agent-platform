//! Task-owned PKCE cleanup using the shared Job lease and scheduler authority.
use crate::{
    execution_requirements::StoredExecutionRequirement,
    repository::{
        job_from_row, job_projection, load_job_for_update_by_text, load_task_for_update,
        task_projection, PgRepository, RepositoryError, DEFAULT_SCHEDULER_LIMITS,
    },
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    JobKind, JobState, ResourceId, ResourceKind, Sha256Digest, TraceIdentityV1, TypedPayload,
    WorkClass,
};
use insight_platform_jobs::store::JobRecord;
use insight_platform_jobs::{self as jobs, JobProjection, LeasePolicy};
use insight_platform_mcp_host::*;
use insight_platform_tasks::{TaskDefinition, TaskProjection, TaskState};
use sqlx::{Acquire, Postgres, Transaction};
use std::collections::{BTreeMap, BTreeSet};
fn invalid(error: impl ToString) -> RepositoryError {
    RepositoryError::CorruptRow(error.to_string())
}
fn delivery(error: RepositoryError) -> McpOAuthPkceCleanupDeliveryError {
    match error {
        RepositoryError::Database(_) => McpOAuthPkceCleanupDeliveryError::Unavailable,
        _ => McpOAuthPkceCleanupDeliveryError::CorruptJob,
    }
}
fn decode(job: &JobRecord) -> Result<McpOAuthPkceCleanupJobPayload, RepositoryError> {
    crate::recovery_isolation::job(
        decode_inner(job),
        job,
        insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
    )
}
fn decode_inner(job: &JobRecord) -> Result<McpOAuthPkceCleanupJobPayload, RepositoryError> {
    let payload: McpOAuthPkceCleanupJobPayload =
        serde_json::from_value(job.payload.value.clone()).map_err(invalid)?;
    payload.validate().map_err(invalid)?;
    if job.job_kind != JobKind::McpOAuthPkceCleanup.as_str()
        || job.work_class != WorkClass::Recovery.as_str()
        || job.owner_id != payload.task_id.to_string()
        || job.tenant_id != payload.tenant_id.to_string()
        || job.execution_requirement != payload.execution_requirement().map_err(invalid)?
    {
        return Err(invalid("cleanup Job owner/requirement mismatch"));
    }
    Ok(payload)
}
fn cause(task: &TaskProjection) -> Result<McpOAuthPkceCleanupCause, RepositoryError> {
    match task.state {
        TaskState::Responded => Ok(McpOAuthPkceCleanupCause::Authorized),
        TaskState::Declined => Ok(McpOAuthPkceCleanupCause::Declined),
        TaskState::Expired => Ok(McpOAuthPkceCleanupCause::Expired),
        _ => Err(invalid("cleanup owner Task is not terminal OAuth")),
    }
}
/// The caller holds the Task root. All mutations of this chain use that same
/// root, so reverse-chain Job locks cannot race a successor insertion.
/// Returns current-to-initial Jobs, including the completed proof when present.
pub(crate) async fn load_cleanup_recovery_chain(
    tx: &mut Transaction<'_, Postgres>,
    task: &TaskProjection,
) -> Result<Vec<JobRecord>, RepositoryError> {
    let pointer: Option<String> = sqlx::query_scalar(
        "SELECT current_cleanup_job_id FROM insight_platform.tasks WHERE tenant_id=$1 AND task_id=$2",
    )
    .bind(task.tenant_id.to_string())
    .bind(task.task_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    let TaskDefinition::McpOAuthAuthorization { binding, .. } = &task.payload.definition else {
        return Err(invalid("cleanup chain owner is not OAuth Task"));
    };
    let mut next = pointer.ok_or_else(|| invalid("cleanup chain current Job missing"))?;
    let mut seen = BTreeSet::new();
    let mut chain = Vec::with_capacity(MCP_OAUTH_CLEANUP_MAX_CHAIN_JOBS);
    // The extra read distinguishes a full allowed chain from an oversized one.
    for _ in 0..=MCP_OAUTH_CLEANUP_MAX_CHAIN_JOBS {
        if !seen.insert(next.clone()) {
            return Err(invalid("cleanup predecessor chain contains a cycle"));
        }
        let job = load_job_for_update_by_text(tx, &task.tenant_id.to_string(), &next)
            .await
            .map_err(|error| match error {
                RepositoryError::NotFound(_) => invalid("cleanup predecessor Job missing"),
                other => other,
            })?;
        let payload = decode(&job)?;
        if payload.task_id != task.task_id
            || payload.task_generation != task.generation
            || payload.cause != cause(task)?
            || payload.hint.secret_binding_id != binding.pkce_secret_binding.secret_binding_id
            || payload.hint.binding_generation != binding.pkce_secret_binding.binding_generation
            || job.owner_kind != "interaction"
            || (!chain.is_empty()
                && (!matches!(job.state.as_str(), "failed" | "cancelled" | "timed_out")
                    || job.terminal_at.is_none()
                    || payload.deletion_proof.is_some()))
        {
            return Err(invalid(
                "cleanup predecessor ownership or terminal evidence invalid",
            ));
        }
        let predecessor = payload.predecessor_job_id;
        chain.push(job);
        if chain.len() > MCP_OAUTH_CLEANUP_MAX_CHAIN_JOBS {
            return Err(invalid("cleanup predecessor chain exceeds recovery budget"));
        }
        match predecessor {
            Some(id) => next = id.to_string(),
            None => {
                let identity = McpOAuthCleanupChainIdentity {
                    tenant_id: task.tenant_id.clone(),
                    task_id: task.task_id.clone(),
                    task_generation: task.generation,
                    current_job_id: chain[0].job_id.parse().map_err(invalid)?,
                    cause: cause(task)?,
                    hint: McpOAuthPkceCleanupHint {
                        schema_version: 1,
                        secret_binding_id: binding.pkce_secret_binding.secret_binding_id.clone(),
                        binding_generation: binding.pkce_secret_binding.binding_generation,
                    },
                };
                let metadata = chain
                    .iter()
                    .map(|job| {
                        Ok(McpOAuthCleanupChainJob {
                            job_id: job.job_id.parse().map_err(invalid)?,
                            state: job.state.parse().map_err(invalid)?,
                            created_at: job.created_at,
                            terminal_at: job.terminal_at,
                            deadline: job.deadline,
                            payload_digest: job.payload.digest.parse().map_err(invalid)?,
                            payload: decode(job)?,
                        })
                    })
                    .collect::<Result<Vec<_>, RepositoryError>>()?;
                validate_mcp_oauth_cleanup_chain(&identity, &metadata).map_err(invalid)?;
                return Ok(chain);
            }
        }
    }
    Err(invalid("cleanup predecessor chain is incomplete"))
}
/// Caller holds the Task root; no scheduling row is created or reset here.
/// Enrollment provisioned the closed Recovery work class before this Task existed.
pub(crate) struct CleanupJobCreation<'a> {
    pub event_id: &'a ResourceId,
    pub trace: TraceIdentityV1,
    pub job_id: &'a ResourceId,
    pub predecessor: Option<(ResourceId, Sha256Digest)>,
    pub attempt_limit: u32,
    pub now: DateTime<Utc>,
}
pub(crate) async fn create_cleanup_job(
    tx: &mut Transaction<'_, Postgres>,
    task: &TaskProjection,
    creation: CleanupJobCreation<'_>,
) -> Result<JobRecord, RepositoryError> {
    let CleanupJobCreation {
        event_id,
        trace,
        job_id,
        predecessor,
        attempt_limit,
        now,
    } = creation;
    task.validate()?;
    if job_id.kind() != ResourceKind::Job
        || !(1..=MCP_OAUTH_CLEANUP_ATTEMPT_LIMIT).contains(&attempt_limit)
    {
        return Err(invalid("cleanup Job identity or attempt budget invalid"));
    }
    let TaskDefinition::McpOAuthAuthorization { binding, .. } = &task.payload.definition else {
        return Err(invalid("cleanup owner is not OAuth Task"));
    };
    let hint = McpOAuthPkceCleanupHint {
        schema_version: 1,
        secret_binding_id: binding.pkce_secret_binding.secret_binding_id.clone(),
        binding_generation: binding.pkce_secret_binding.binding_generation,
    };
    let payload = McpOAuthPkceCleanupJobPayload {
        schema_version: 1,
        tenant_id: task.tenant_id.clone(),
        task_id: task.task_id.clone(),
        task_generation: task.generation,
        cause: cause(task)?,
        deletion_effect_identity: McpOAuthPkceCleanupJobPayload::effect_identity(
            &task.tenant_id,
            &task.task_id,
            task.generation,
            &hint,
        )
        .map_err(invalid)?,
        hint,
        source_event_id: event_id.clone(),
        predecessor_job_id: predecessor.as_ref().map(|value| value.0.clone()),
        recovery_evidence_digest: predecessor.as_ref().map(|value| value.1.clone()),
        deletion_proof: None,
    };
    let requirement =
        StoredExecutionRequirement::new(&payload.execution_requirement().map_err(invalid)?)?;
    let stored = TypedPayload::from_versioned(1, &payload, 65_536)?;
    let row=sqlx::query("INSERT INTO insight_platform.jobs (tenant_id,job_id,job_kind,work_class,owner_kind,owner_id,trace_id,state,attempt_limit,scheduled_at,deadline,priority,request_digest,effect_key_digest,payload_schema_version,payload,payload_digest,scheduler_partition_id,execution_requirement_version,execution_requirement,execution_requirement_digest) VALUES ($1,$2,'mcp_oauth_pkce_cleanup','recovery','interaction',$3,$4,'ready',$5,$6,$7,2,$8,$9,1,$10,$8,(SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1),$11,$12,$13) RETURNING *")
        .bind(task.tenant_id.to_string()).bind(job_id.to_string()).bind(task.task_id.to_string()).bind(trace.trace_id.to_string()).bind(attempt_limit as i32).bind(now).bind(now+Duration::seconds(MCP_OAUTH_CLEANUP_DEADLINE_SECONDS))
        .bind(&stored.digest).bind(payload.deletion_effect_identity.to_string()).bind(&stored.value).bind(requirement.version).bind(requirement.value).bind(requirement.digest).fetch_one(&mut **tx).await?;
    let previous = predecessor.as_ref().map(|value| value.0.to_string());
    let affected=sqlx::query("UPDATE insight_platform.tasks SET current_cleanup_job_id=$4 WHERE tenant_id=$1 AND task_id=$2 AND generation=$3 AND current_cleanup_job_id IS NOT DISTINCT FROM $5")
        .bind(task.tenant_id.to_string()).bind(task.task_id.to_string()).bind(task.generation as i64).bind(job_id.to_string()).bind(previous).execute(&mut **tx).await?.rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("Task cleanup pointer"));
    }
    job_from_row(row)
}
async fn current_task(
    tx: &mut Transaction<'_, Postgres>,
    payload: &McpOAuthPkceCleanupJobPayload,
    job_id: &str,
) -> Result<TaskProjection, RepositoryError> {
    let row = load_task_for_update(tx, &payload.tenant_id, &payload.task_id).await?;
    let task = task_projection(&row)?;
    let pointer:Option<String>=sqlx::query_scalar("SELECT current_cleanup_job_id FROM insight_platform.tasks WHERE tenant_id=$1 AND task_id=$2").bind(payload.tenant_id.to_string()).bind(payload.task_id.to_string()).fetch_one(&mut **tx).await?;
    let TaskDefinition::McpOAuthAuthorization { binding, .. } = &task.payload.definition else {
        return Err(RepositoryError::StaleFence);
    };
    if pointer.as_deref() != Some(job_id)
        || task.generation != payload.task_generation
        || cause(&task)? != payload.cause
        || binding.pkce_secret_binding.secret_binding_id != payload.hint.secret_binding_id
        || binding.pkce_secret_binding.binding_generation != payload.hint.binding_generation
    {
        return Err(RepositoryError::StaleFence);
    }
    Ok(task)
}
async fn write_projection(
    tx: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    next: &JobProjection,
    payload: &McpOAuthPkceCleanupJobPayload,
    now: DateTime<Utc>,
    build: Option<&Sha256Digest>,
) -> Result<JobRecord, RepositoryError> {
    payload.validate().map_err(invalid)?;
    next.validate()?;
    let stored = TypedPayload::from_versioned(1, payload, 65_536)?;
    let terminal = matches!(
        next.state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
    );
    let row=sqlx::query("UPDATE insight_platform.jobs SET state=$4,version=$5,attempt_no=$6,lease_epoch=$7,worker_id=$8,lease_token_digest=$9,lease_expires_at=$10,heartbeat_at=$11,scheduled_at=$12,retry_at=$13,payload_schema_version=1,payload=$14,payload_digest=$15,result_digest=CASE WHEN $16 THEN $15 ELSE result_digest END,started_at=CASE WHEN $4='running' THEN COALESCE(started_at,$17) WHEN $4='retry_scheduled' THEN NULL ELSE started_at END,terminal_at=CASE WHEN $16 THEN $17 ELSE terminal_at END,updated_at=$17,attempt_build_digest=COALESCE($18,attempt_build_digest) WHERE tenant_id=$1 AND job_id=$2 AND version=$3 RETURNING *")
        .bind(&current.tenant_id).bind(&current.job_id).bind(current.version).bind(next.state.as_str()).bind(next.version as i64).bind(next.attempt_count as i32).bind(next.lease_generation as i64)
        .bind(next.lease.as_ref().map(|lease|lease.worker_process_generation_id.to_string())).bind(next.lease.as_ref().map(|lease|lease.token_digest.to_string()))
        .bind(next.lease.as_ref().map(|lease|lease.expires_at)).bind(next.lease.as_ref().map(|lease|lease.heartbeat_at)).bind(next.scheduled_at).bind(next.retry_at).bind(stored.value).bind(stored.digest).bind(terminal).bind(now).bind(build.map(ToString::to_string))
        .fetch_optional(&mut **tx).await?.ok_or(RepositoryError::StaleFence)?;
    job_from_row(row)
}
async fn append_cleanup_event(
    tx: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    payload: &McpOAuthPkceCleanupJobPayload,
    kind: &str,
    failure_code: Option<&str>,
) -> Result<(), RepositoryError> {
    let event =
        ResourceId::from_uuid_v7(ResourceKind::Event, uuid::Uuid::now_v7()).map_err(invalid)?;
    let outbox = ResourceId::from_uuid_v7(ResourceKind::OutboxEvent, uuid::Uuid::now_v7())
        .map_err(invalid)?;
    crate::repository::append_scheduler_event_with_trace(tx,job.trace,&job.tenant_id,&event,&outbox,"job",&job.job_id,job.version,None,kind,
        &TypedPayload::new(1,&serde_json::json!({"task_id":payload.task_id,"task_generation":payload.task_generation,"deletion_effect_identity":payload.deletion_effect_identity,"state":job.state,"attempt_no":job.attempt_no,"lease_generation":job.lease_epoch,"worker_build_digest":job.attempt_build_digest,"deletion_proof":payload.deletion_proof,"failure_code":failure_code}))?).await
}
#[async_trait]
impl McpOAuthPkceCleanupJobs for PgRepository {
    async fn expire_due_mcp_oauth_tasks(
        &self,
        command: DriveExpiredMcpOAuthTasks,
    ) -> Result<
        insight_platform_jobs::store::SafetyScanPage<ResourceId>,
        McpOAuthPkceCleanupDeliveryError,
    > {
        let page = self
            .drive_expired_mcp_oauth_tasks(command)
            .await
            .map_err(delivery)?;
        let mut records = Vec::with_capacity(page.records.len());
        for task in page.records {
            records.push(
                task.task_id
                    .parse()
                    .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptJob)?,
            );
        }
        Ok(insight_platform_jobs::store::SafetyScanPage {
            records,
            diagnostics: page.diagnostics,
            next_cursor: page.next_cursor,
            exhausted: page.exhausted,
        })
    }
    async fn claim_due_mcp_oauth_pkce_cleanups(
        &self,
        command: ClaimDueMcpOAuthPkceCleanups,
    ) -> Result<Vec<ClaimedMcpOAuthPkceCleanup>, McpOAuthPkceCleanupDeliveryError> {
        command.validate(64, 120000)?;
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::Unavailable)?;
        let result:Result<Vec<ClaimedMcpOAuthPkceCleanup>,RepositoryError>=async {
            let Some(admission)=crate::claim_admission::PreparedClaimAdmission::prepare(&mut tx,WorkClass::Recovery,DEFAULT_SCHEDULER_LIMITS).await? else{return Ok(Vec::new())};
            let now:DateTime<Utc>=sqlx::query_scalar("SELECT clock_timestamp()").fetch_one(&mut *tx).await?;
            admission.observe_diagnostics();
            for observed in admission.candidate_jobs().filter(|job| job.job_kind == JobKind::McpOAuthPkceCleanup.as_str()) {
                if observed.deadline <= now || (matches!(observed.state.as_str(),"leased"|"running") && observed.lease_expires_at.is_some_and(|expiry|expiry<=now)) {
                    let mut object_tx=tx.begin().await?;
                    match recover_expired_cleanup_candidate(&mut object_tx,observed).await {
                        Ok(())=>object_tx.commit().await?,
                        Err(RepositoryError::InvalidPersistedObject(diagnostic))=>{object_tx.rollback().await?;crate::recovery_isolation::observe(&diagnostic);},
                        Err(RepositoryError::StaleFence|RepositoryError::LeaseExpired|RepositoryError::Conflict(_))=>{object_tx.rollback().await?;},
                        Err(error)=>{object_tx.rollback().await?;return Err(error);}
                    }
                }
            }
            let rows=sqlx::query("SELECT * FROM insight_platform.jobs WHERE job_id=ANY($1) AND job_kind='mcp_oauth_pkce_cleanup' AND terminal_at IS NULL AND state IN ('ready','retry_scheduled') AND worker_id IS NULL AND scheduled_at<=$2 AND COALESCE(retry_at,scheduled_at)<=$2 AND deadline>$2 AND attempt_no<attempt_limit ORDER BY tenant_id,owner_id,job_id")
                .bind(admission.candidate_ids()).bind(now).fetch_all(&mut *tx).await?;
            let mut eligible=BTreeMap::new();let mut candidates=BTreeMap::new();let mut diagnostics=Vec::new();
            for row in rows {
                let Some(job)=crate::recovery_isolation::collect(crate::repository::persisted_job_from_row(row),&mut diagnostics)? else {continue};
                let Some(payload)=crate::recovery_isolation::collect(decode(&job),&mut diagnostics)? else {continue};
                if command.worker_manifest.execution_capabilities.supports(&job.execution_requirement) && payload.deletion_proof.is_none() {
                    eligible.insert(job.job_id.clone(),crate::claim_admission::EligibleClaim{lane:insight_platform_contracts::SchedulingLane::RestrictedControl,mode:insight_platform_contracts::ClaimMode::NewAttempt,quota_costs:Vec::new()});candidates.insert(job.job_id.clone(),job);
                }
            }
            let decision=admission.select(&eligible,&BTreeMap::new(),command.maximum_claims)?;
            let mut selected=decision.admitted_job_ids.iter().zip(&command.lease_token_digests).map(|(id,token)|(candidates[&id.to_string()].clone(),token)).collect::<Vec<_>>();
            selected.sort_by(|(a,_),(b,_)|(&a.tenant_id,&a.owner_id,&a.job_id).cmp(&(&b.tenant_id,&b.owner_id,&b.job_id)));
            let mut claims=Vec::new();
            for (observed,token) in selected {
                let mut object_tx=tx.begin().await?;
                let result:Result<(),RepositoryError>=async {let payload=decode(&observed)?;current_task(&mut object_tx,&payload,&observed.job_id).await?;
                let current=load_job_for_update_by_text(&mut object_tx,&observed.tenant_id,&observed.job_id).await?;if current.version!=observed.version{return Err(RepositoryError::StaleFence)}
                let leased=jobs::decide_claim(&crate::recovery_isolation::job(job_projection(&current), &current, insight_platform_jobs::store::SafetyScanPhase::OwnerValidation)?,now,command.claim_owner.clone(),token.clone(),LeasePolicy{requested_milliseconds:command.lease_milliseconds,hard_maximum_milliseconds:120000})?;
                let fence=jobs::JobFence{expected_version:leased.version,worker_process_generation_id:command.claim_owner.clone(),lease_generation:leased.lease_generation,token_digest:token.clone()};
                let running=jobs::decide_start(&leased,&fence,now)?;
                let record=write_projection(&mut object_tx,&current,&running,&payload,now,Some(&command.worker_manifest.worker_build_digest)).await?;
                append_cleanup_event(&mut object_tx,&record,&payload,"mcp.pkce.cleanup_started",None).await?;
                claims.push(ClaimedMcpOAuthPkceCleanup{event_id:payload.source_event_id.clone(),attempt_no:running.attempt_count,trace:record.trace,request:McpOAuthPkceCleanupRequest{cleanup_job_id:record.job_id.parse().map_err(invalid)?,task_generation:payload.task_generation,deletion_effect_identity:payload.deletion_effect_identity.clone(),fence:jobs::JobFence{expected_version:running.version,..fence},tenant_id:payload.tenant_id,task_id:payload.task_id,cause:payload.cause,hint:payload.hint}});
                    Ok(())
                }.await;
                match result {
                    Ok(())=>object_tx.commit().await?,
                    Err(RepositoryError::InvalidPersistedObject(diagnostic))=>{object_tx.rollback().await?;diagnostics.push(diagnostic);},
                    Err(error)=>{object_tx.rollback().await?;return Err(error);}
                }
            }
            let decision=admission.settle_actual(&eligible,&BTreeMap::new(),command.maximum_claims,claims.iter().map(|claim|claim.request.cleanup_job_id.to_string()))?;
            admission.persist(&mut tx,&decision).await?;
            for diagnostic in &diagnostics {crate::recovery_isolation::observe(diagnostic);}
            Ok(claims)
        }.await;
        match result {
            Ok(claims) => {
                tx.commit()
                    .await
                    .map_err(|_| McpOAuthPkceCleanupDeliveryError::Unavailable)?;
                Ok(claims)
            }
            Err(error) => Err(delivery(error)),
        }
    }
    async fn settle_mcp_oauth_pkce_cleanup(
        &self,
        claim: &ClaimedMcpOAuthPkceCleanup,
        settlement: McpOAuthPkceCleanupSettlement,
    ) -> Result<bool, McpOAuthPkceCleanupDeliveryError> {
        claim.validate()?;
        if settlement == McpOAuthPkceCleanupSettlement::Stale {
            return Ok(false);
        }
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::Unavailable)?;
        let result: Result<(), RepositoryError> = async {
            let observed = crate::repository::load_job_by_text(
                &mut tx,
                &claim.request.tenant_id.to_string(),
                &claim.request.cleanup_job_id.to_string(),
            )
            .await?;
            let mut payload = decode(&observed)?;
            current_task(&mut tx, &payload, &observed.job_id).await?;
            let current =
                load_job_for_update_by_text(&mut tx, &observed.tenant_id, &observed.job_id).await?;
            let now = sqlx::query_scalar("SELECT clock_timestamp()")
                .fetch_one(&mut *tx)
                .await?;
            let projection = crate::recovery_isolation::job(
                job_projection(&current),
                &current,
                insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
            )?;
            crate::execution_authorization::authorize_restricted_job_completion(
                &current,
                &insight_platform_jobs::store::JobCommandFence {
                    tenant_id: current.tenant_id.clone(),
                    job_id: current.job_id.clone(),
                    worker_id: claim.request.fence.worker_process_generation_id.clone(),
                    lease_epoch: claim.request.fence.lease_generation as i64,
                    expected_job_version: claim.request.fence.expected_version as i64,
                    lease_token_digest: claim.request.fence.token_digest.clone(),
                },
                now,
            )?;
            if payload.deletion_effect_identity != claim.request.deletion_effect_identity {
                return Err(RepositoryError::StaleFence);
            }
            let failure_code = match settlement {
                McpOAuthPkceCleanupSettlement::Retry { failure_code, .. }
                | McpOAuthPkceCleanupSettlement::DeadLetter { failure_code } => Some(failure_code),
                _ => None,
            };
            if failure_code.is_some_and(|code| {
                !matches!(
                    code,
                    "mcp_oauth_pkce_cleanup_authority_unavailable"
                        | "mcp_oauth_pkce_cleanup_secret_rejected"
                        | "mcp_oauth_pkce_cleanup_secret_unavailable"
                        | "mcp_oauth_pkce_cleanup_outcome_uncertain"
                        | "mcp_oauth_pkce_cleanup_hint_invalid"
                        | "mcp_oauth_pkce_cleanup_envelope_invalid"
                )
            }) {
                return Err(RepositoryError::InvalidInput(
                    "unknown cleanup failure code".into(),
                ));
            }
            let next = match settlement {
                McpOAuthPkceCleanupSettlement::Completed { proof } => {
                    payload.deletion_proof = Some(proof);
                    jobs::decide_terminal(
                        &projection,
                        &claim.request.fence,
                        now,
                        JobState::Succeeded,
                    )?
                }
                McpOAuthPkceCleanupSettlement::Retry {
                    delay_milliseconds, ..
                } if (1..=3600000).contains(&delay_milliseconds)
                    && projection.attempt_count < projection.attempt_limit
                    && now + Duration::milliseconds(delay_milliseconds as i64)
                        < projection.deadline =>
                {
                    jobs::decide_retry(
                        &projection,
                        &claim.request.fence,
                        now,
                        now + Duration::milliseconds(delay_milliseconds as i64),
                    )?
                }
                McpOAuthPkceCleanupSettlement::Retry { .. }
                | McpOAuthPkceCleanupSettlement::DeadLetter { .. } => {
                    jobs::decide_terminal(&projection, &claim.request.fence, now, JobState::Failed)?
                }
                McpOAuthPkceCleanupSettlement::Stale => return Err(RepositoryError::StaleFence),
            };
            let updated = write_projection(&mut tx, &current, &next, &payload, now, None).await?;
            append_cleanup_event(
                &mut tx,
                &updated,
                &payload,
                "mcp.pkce.cleanup_settled",
                failure_code,
            )
            .await?;
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                tx.commit()
                    .await
                    .map_err(|_| McpOAuthPkceCleanupDeliveryError::Unavailable)?;
                Ok(true)
            }
            Err(RepositoryError::StaleFence | RepositoryError::LeaseExpired) => Ok(false),
            Err(error) => Err(delivery(error)),
        }
    }
}
async fn recover_expired_cleanup_candidate(
    tx: &mut Transaction<'_, Postgres>,
    observed: &JobRecord,
) -> Result<(), RepositoryError> {
    let payload = decode(observed)?;
    current_task(tx, &payload, &observed.job_id).await?;
    let current = load_job_for_update_by_text(tx, &observed.tenant_id, &observed.job_id).await?;
    if current.version != observed.version {
        return Ok(());
    }
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **tx)
        .await?;
    let projection = crate::recovery_isolation::job(
        job_projection(&current),
        &current,
        insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
    )?;
    let next = if projection.lease.is_some() {
        let target = if now >= projection.deadline {
            JobState::TimedOut
        } else if projection.state == JobState::Leased {
            JobState::Ready
        } else if projection.attempt_count >= projection.attempt_limit {
            JobState::Failed
        } else {
            JobState::RetryScheduled
        };
        jobs::decide_expired_lease(
            &projection,
            projection.version,
            projection.lease_generation,
            now,
            target,
            (target == JobState::RetryScheduled).then_some(now + Duration::milliseconds(1)),
        )?
    } else {
        jobs::decide_owner_terminal(&projection, JobState::TimedOut)?
    };
    let updated = write_projection(tx, &current, &next, &payload, now, None).await?;
    append_cleanup_event(
        tx,
        &updated,
        &payload,
        "mcp.pkce.cleanup_lease_recovered",
        None,
    )
    .await?;
    Ok(())
}

#[async_trait]
impl McpOAuthPkceCleanupAuthority for PgRepository {
    async fn authorize_cleanup(
        &self,
        request: &McpOAuthPkceCleanupRequest,
    ) -> Result<AuthorizedMcpOAuthPkceCleanup, McpOAuthPkceCleanupAuthorityError> {
        request
            .validate()
            .map_err(|_| McpOAuthPkceCleanupAuthorityError::StaleOrNotFound)?;
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|_| McpOAuthPkceCleanupAuthorityError::Unavailable)?;
        let result: Result<AuthorizedMcpOAuthPkceCleanup, RepositoryError> = async {
            let observed = crate::repository::load_job_by_text(
                &mut tx,
                &request.tenant_id.to_string(),
                &request.cleanup_job_id.to_string(),
            )
            .await?;
            let payload = decode(&observed)?;
            let task = current_task(&mut tx, &payload, &observed.job_id).await?;
            let current =
                load_job_for_update_by_text(&mut tx, &observed.tenant_id, &observed.job_id).await?;
            let now = sqlx::query_scalar("SELECT clock_timestamp()")
                .fetch_one(&mut *tx)
                .await?;
            crate::execution_authorization::authorize_restricted_job_completion(
                &current,
                &insight_platform_jobs::store::JobCommandFence {
                    tenant_id: current.tenant_id.clone(),
                    job_id: current.job_id.clone(),
                    worker_id: request.fence.worker_process_generation_id.clone(),
                    lease_epoch: request.fence.lease_generation as i64,
                    expected_job_version: request.fence.expected_version as i64,
                    lease_token_digest: request.fence.token_digest.clone(),
                },
                now,
            )?;
            if payload.task_generation != request.task_generation
                || payload.deletion_effect_identity != request.deletion_effect_identity
                || payload.hint != request.hint
                || payload.cause != request.cause
                || payload.deletion_proof.is_some()
            {
                return Err(RepositoryError::StaleFence);
            }
            let TaskDefinition::McpOAuthAuthorization { binding, .. } = task.payload.definition
            else {
                return Err(RepositoryError::StaleFence);
            };
            // Cleanup addresses the already frozen pinned generation, including
            // after principal or SecretBinding revocation. It cannot resolve a
            // current business secret or choose a replacement generation.
            let authorization = AuthorizedMcpOAuthPkceCleanup {
                tenant_id: request.tenant_id.clone(),
                task_id: request.task_id.clone(),
                secret_binding: *binding.pkce_secret_binding,
            };
            authorization.validate_for(request).map_err(invalid)?;
            Ok(authorization)
        }
        .await;
        match result {
            Ok(value) => {
                tx.commit()
                    .await
                    .map_err(|_| McpOAuthPkceCleanupAuthorityError::Unavailable)?;
                Ok(value)
            }
            Err(RepositoryError::Database(_) | RepositoryError::CorruptRow(_)) => {
                Err(McpOAuthPkceCleanupAuthorityError::Unavailable)
            }
            Err(_) => Err(McpOAuthPkceCleanupAuthorityError::StaleOrNotFound),
        }
    }
}
impl PgRepository {
    pub async fn recover_mcp_oauth_pkce_cleanup(
        &self,
        command: RecoverMcpOAuthPkceCleanup,
    ) -> Result<insight_platform_contracts::CommandOutcome<JobRecord>, RepositoryError> {
        let mut tx = self.pool().begin().await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        command
            .validate_at(now)
            .map_err(|error| RepositoryError::InvalidInput(error.into()))?;
        crate::repository::require_tenant_permission(
            &mut tx,
            &command.audit,
            insight_platform_contracts::Permission::McpCleanupRecover,
        )
        .await?;
        let task_record =
            load_task_for_update(&mut tx, &command.audit.tenant_id, &command.task_id).await?;
        let task = task_projection(&task_record)?;
        if crate::repository::claim_command_receipt(
            &mut tx,
            &command.audit,
            "mcp_oauth_task",
            &command.task_id.to_string(),
            "mcp.pkce.cleanup.recover",
        )
        .await?
        {
            let original_job_id = crate::repository::load_command_receipt_response_reference(
                &mut tx,
                &command.audit,
                "mcp_oauth_task",
                &command.task_id.to_string(),
                "mcp.pkce.cleanup.recover",
            )
            .await?;
            let job = crate::repository::load_job_by_text(
                &mut tx,
                &command.audit.tenant_id.to_string(),
                &original_job_id,
            )
            .await?;
            let payload = decode(&job)?;
            if payload.predecessor_job_id.as_ref() != Some(&command.previous_job_id)
                || payload.recovery_evidence_digest.as_ref()
                    != Some(&command.recovery_evidence_digest)
            {
                return Err(RepositoryError::Conflict("cleanup recovery replay"));
            }
            tx.commit().await?;
            return Ok(insight_platform_contracts::CommandOutcome::Replayed(job));
        }
        if task.generation != command.expected_task_generation
            || task.version != command.expected_task_version
        {
            return Err(RepositoryError::Conflict("cleanup recovery Task"));
        }
        let chain = load_cleanup_recovery_chain(&mut tx, &task).await?;
        let previous = chain
            .first()
            .ok_or_else(|| invalid("cleanup chain is empty"))?;
        if previous.job_id != command.previous_job_id.to_string() {
            return Err(RepositoryError::Conflict("Task cleanup pointer"));
        }
        let payload = decode(previous)?;
        if !matches!(
            previous.state.as_str(),
            "failed" | "cancelled" | "timed_out"
        ) || previous.terminal_at.is_none()
            || payload.deletion_proof.is_some()
            || payload.task_id != task.task_id
            || payload.task_generation != task.generation
        {
            return Err(RepositoryError::Conflict("cleanup recovery predecessor"));
        }
        if chain.len() > MCP_OAUTH_CLEANUP_RECOVERY_LIMIT {
            return Err(RepositoryError::Conflict(
                "cleanup recovery budget exhausted",
            ));
        }
        let job = create_cleanup_job(
            &mut tx,
            &task,
            CleanupJobCreation {
                event_id: &command.audit.event_id,
                trace: command.audit.trace,
                job_id: &command.new_job_id,
                predecessor: Some((
                    command.previous_job_id.clone(),
                    command.recovery_evidence_digest.clone(),
                )),
                attempt_limit: command.attempt_limit,
                now,
            },
        )
        .await?;
        if decode(&job)?.deletion_effect_identity != payload.deletion_effect_identity {
            return Err(invalid("cleanup recovery changed stable effect"));
        }
        let changed=sqlx::query("UPDATE insight_platform.tasks SET version=version+1,updated_at=$4 WHERE tenant_id=$1 AND task_id=$2 AND version=$3")
            .bind(task.tenant_id.to_string()).bind(task.task_id.to_string()).bind(task.version as i64).bind(now).execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            return Err(RepositoryError::Conflict("cleanup recovery Task version"));
        }
        crate::repository::append_command_event(&mut tx,&command.audit,"mcp_oauth_task",&task.task_id.to_string(),task.version as i64+1,"mcp.pkce.cleanup_recovered",&TypedPayload::new(1,&serde_json::json!({"previous_job_id":command.previous_job_id,"new_job_id":command.new_job_id,"task_generation":task.generation,"deletion_effect_identity":payload.deletion_effect_identity,"recovery_evidence_digest":command.recovery_evidence_digest,"attempt_limit":command.attempt_limit}))?).await?;
        crate::repository::terminalize_command_receipt(
            &mut tx,
            &command.audit,
            &job.job_id,
            "recovered",
        )
        .await?;
        tx.commit().await?;
        Ok(insight_platform_contracts::CommandOutcome::Applied(job))
    }
}
