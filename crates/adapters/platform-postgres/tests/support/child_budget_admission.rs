//! Real Child admission: current authorization, nested pause and a delayed 100 ms transaction.
use super::*;
use insight_platform_orchestrator::store::DeferredOrchestrationChildRun;
use std::time::{Duration as StdDuration, Instant};

fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
fn fresh_digest() -> Sha256Digest {
    canonical_digest(&json!(uuid::Uuid::now_v7()))
        .unwrap()
        .parse()
        .unwrap()
}
fn fresh_audit() -> CommandAudit {
    CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: id(TENANT_ID),
        principal_id: id(PRINCIPAL_ID),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: fresh(ResourceKind::Receipt),
        event_id: fresh(ResourceKind::Event),
        outbox_id: fresh(ResourceKind::OutboxEvent),
        idempotency_key_digest: fresh_digest(),
        request_digest: fresh_digest(),
        receipt_expires_at: Utc::now() + Duration::hours(1),
    }
}

struct Agent {
    bindings: RunBindingsSnapshot,
    plan: RuntimePlan,
    resource_id: ResourceId,
}

/// Clone real frozen definitions under new identities; the ordinary fixture's Plan stays intact.
async fn clone_agent(
    pool: &PgPool,
    base: &RunBindingsSnapshot,
    candidate: ExactDeploymentRef,
    milliseconds: u64,
    descendants: u32,
) -> Agent {
    let mut plan = runtime_plan();
    let entry = PlanNodeKey::new("child_call".to_owned()).unwrap();
    plan.entry_node_id = entry.clone();
    let RuntimeNode::ChildAgentCall { budget, .. } = plan.nodes.get_mut(&entry).unwrap() else {
        panic!("child entry")
    };
    budget.maximum_duration_milliseconds = milliseconds;
    budget.maximum_descendant_runs = descendants;
    let plan_digest = plan.canonical_digest(plan_limits()).unwrap();
    let plan_bytes = canonical_json(&serde_json::to_value(&plan).unwrap()).unwrap();
    let resource_id = fresh(ResourceKind::Agent);
    let interface_id = fresh(ResourceKind::AgentInterfaceRevision);
    let plan_id = fresh(ResourceKind::AgentPlanRevision);
    let deployment_id = fresh(ResourceKind::AgentDeployment);
    let artifact_id = fresh(ResourceKind::Artifact);
    let blob_id = fresh(ResourceKind::InternalBlob);
    let mut value: Value = sqlx::query_scalar("SELECT payload FROM insight_platform.resource_versions WHERE tenant_id=$1 AND resource_version_id=$2")
        .bind(TENANT_ID).bind(AGENT_PLAN_ID).fetch_one(pool).await.unwrap();
    value.as_object_mut().unwrap().remove("schema_version");
    let mut published: PublishedVersionPayload = serde_json::from_value(value).unwrap();
    let ResourceDocument::Agent(document) = &mut published.document else {
        panic!("Agent owner")
    };
    document.typed_plan_artifact_id = artifact_id.clone();
    document.typed_plan_digest = plan_digest.clone();
    published.validation.program_requirement = Some(
        insight_platform_plan::execution::program_execution_requirement(plan_digest.clone(), 6)
            .unwrap(),
    );
    let payload = TypedPayload::new(1, &published).unwrap();
    let interface_digest: Sha256Digest =
        canonical_digest(&serde_json::to_value(&published.document).unwrap())
            .unwrap()
            .parse()
            .unwrap();
    sqlx::query("INSERT INTO insight_platform.artifact_blobs (tenant_id,blob_id,backend,storage_binding_digest,security_domain_digest,object_reference_ciphertext,object_generation,key_id,encryption_domain_id,content_digest,size_bytes,state,verified_at,created_at,updated_at) SELECT tenant_id,$2,backend,storage_binding_digest,security_domain_digest,object_reference_ciphertext,object_generation,key_id,encryption_domain_id,$3,$4,state,statement_timestamp(),statement_timestamp(),statement_timestamp() FROM insight_platform.artifact_blobs WHERE tenant_id=$1 AND blob_id=$5")
        .bind(TENANT_ID).bind(blob_id.to_string()).bind(plan_digest.to_string()).bind(plan_bytes.len() as i64).bind(TYPED_PLAN_BLOB_ID).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.artifacts (tenant_id,artifact_id,blob_id,purpose,classification,expected_size_bytes,expected_digest,declared_media_type,verified_media_type,state,metadata_schema_version,metadata,metadata_digest,retention_policy_revision_id,retain_until,created_by) SELECT tenant_id,$2,$3,purpose,classification,$4,$5,declared_media_type,verified_media_type,state,metadata_schema_version,metadata,metadata_digest,retention_policy_revision_id,retain_until,created_by FROM insight_platform.artifacts WHERE tenant_id=$1 AND artifact_id=$6")
        .bind(TENANT_ID).bind(artifact_id.to_string()).bind(blob_id.to_string()).bind(plan_bytes.len() as i64).bind(plan_digest.to_string()).bind(TYPED_PLAN_ARTIFACT_ID).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.resources (tenant_id,resource_id,resource_kind,lifecycle_state,gate_state,payload_schema_version,payload,payload_digest) SELECT tenant_id,$2,resource_kind,lifecycle_state,gate_state,payload_schema_version,payload,payload_digest FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=$3")
        .bind(TENANT_ID).bind(resource_id.to_string()).bind(AGENT_ID).execute(pool).await.unwrap();
    for (revision, kind, digest) in [
        (&interface_id, "agent_interface_revision", &interface_digest),
        (&plan_id, "agent_plan_revision", &plan_digest),
    ] {
        sqlx::query("INSERT INTO insight_platform.resource_versions (tenant_id,resource_version_id,resource_id,resource_version_kind,revision_no,content_digest,payload_schema_version,payload,payload_digest,created_by,artifact_id) VALUES ($1,$2,$3,$4,1,$5,$6,$7,$8,$9,$10)")
            .bind(TENANT_ID).bind(revision.to_string()).bind(resource_id.to_string()).bind(kind).bind(digest.to_string()).bind(payload.schema_version).bind(&payload.value).bind(&payload.digest).bind(PRINCIPAL_ID).bind(artifact_id.to_string()).execute(pool).await.unwrap();
    }
    let mut closure_value: Value = sqlx::query_scalar(
        "SELECT bindings FROM insight_platform.deployments WHERE tenant_id=$1 AND deployment_id=$2",
    )
    .bind(TENANT_ID)
    .bind(AGENT_DEPLOYMENT_ID)
    .fetch_one(pool)
    .await
    .unwrap();
    closure_value
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    let DeploymentClosure::Agent(mut closure) = serde_json::from_value(closure_value).unwrap()
    else {
        panic!("Agent closure")
    };
    closure.interface = ExactVersionRef::new(interface_id, interface_digest).unwrap();
    closure.plan = ExactVersionRef::new(plan_id.clone(), plan_digest).unwrap();
    closure.entry_node_id = entry.as_str().to_owned();
    closure.entry_node_kind = PlanNodeKind::ChildAgentCall;
    let slot = closure
        .slots
        .iter_mut()
        .find(|slot| slot.slot_id == "child_worker")
        .unwrap();
    let FrozenSlotTarget::ChildAgent { candidates, .. } = &mut slot.target else {
        panic!("child slot")
    };
    *candidates = vec![candidate];
    slot.binding_digest = canonical_digest(&serde_json::to_value(&slot.target).unwrap())
        .unwrap()
        .parse()
        .unwrap();
    let payload = TypedPayload::new(1, &DeploymentClosure::Agent(closure.clone())).unwrap();
    sqlx::query("INSERT INTO insight_platform.deployments (tenant_id,deployment_id,resource_id,resource_version_id,environment,bindings_digest,payload_schema_version,bindings,created_by) VALUES ($1,$2,$3,$4,'test',$5,$6,$7,$8)")
        .bind(TENANT_ID).bind(deployment_id.to_string()).bind(resource_id.to_string()).bind(plan_id.to_string()).bind(&payload.digest).bind(payload.schema_version).bind(&payload.value).bind(PRINCIPAL_ID).execute(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2")
        .bind(TENANT_ID).bind(resource_id.to_string()).bind(deployment_id.to_string()).execute(pool).await.unwrap();
    let bindings = RunBindingsSnapshot::build(
        ExactDeploymentRef::new(deployment_id, payload.digest.parse().unwrap()).unwrap(),
        base.principal.clone(),
        &closure,
    )
    .unwrap();
    Agent {
        bindings,
        plan,
        resource_id,
    }
}

async fn start(repo: &PgRepository, pool: &PgPool, job_id: &str) -> JobFence {
    isolate_ready_orchestration_job(pool, job_id).await;
    let token = fresh_digest();
    let claim = ClaimOrchestrationJobs {
        worker_manifest: support::orchestration(),
        worker_id: id(WORKER_D_ID),
        limit: 1,
        lease_milliseconds: 30_000,
        slots: vec![OrchestrationClaimSlot {
            lease_token_digest: token.clone(),
            quota_reservation_id: fresh(ResourceKind::UsageReservation),
            quota_entry_ids: (0..4)
                .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
                .collect(),
            run_event_id: fresh(ResourceKind::Event),
            run_outbox_id: fresh(ResourceKind::OutboxEvent),
            node_event_id: fresh(ResourceKind::Event),
            node_outbox_id: fresh(ResourceKind::OutboxEvent),
            job_event_id: fresh(ResourceKind::Event),
            job_outbox_id: fresh(ResourceKind::OutboxEvent),
        }],
    };
    let (tx, claimed) = begin_orchestration_claim_fixture(repo, claim)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.job_id, job_id);
    let command = StartOrchestrationJob {
        fence: JobFence {
            tenant_id: TENANT_ID.into(),
            job_id: job_id.into(),
            worker_id: id(WORKER_D_ID),
            lease_epoch: claimed[0].job.lease_epoch,
            expected_job_version: claimed[0].job.version,
            lease_token_digest: token,
        },
        receipt_id: fresh(ResourceKind::Receipt),
        idempotency_key_digest: fresh_digest(),
        request_digest: fresh_digest(),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: fresh(ResourceKind::Event),
        job_outbox_id: fresh(ResourceKind::OutboxEvent),
        node_event_id: fresh(ResourceKind::Event),
        node_outbox_id: fresh(ResourceKind::OutboxEvent),
    };
    let mut tx = repo.begin_scheduler_transaction().await.unwrap();
    let CommandOutcome::Applied(started) =
        tx.start_orchestration_job(command.clone()).await.unwrap()
    else {
        panic!("new start")
    };
    tx.commit().await.unwrap();
    JobFence {
        expected_job_version: started.version,
        ..command.fence
    }
}

fn defer(agent: &Agent, fence: JobFence, input_id: ResourceId) -> DeferOrchestrationToChildRun {
    let slot = agent
        .bindings
        .slots
        .iter()
        .find(|slot| slot.slot_id == "child_worker")
        .unwrap();
    let FrozenSlotTarget::ChildAgent {
        candidates,
        selection_policy,
    } = &slot.target
    else {
        panic!("child slot")
    };
    let evidence = derive_candidate_selection(
        "child_worker",
        selection_policy,
        &CandidateSelectionPolicyDocument {
            schema_version: 1,
            mode: CandidateSelectionMode::OnlyCandidate,
            route_schema_digest: None,
        },
        candidates,
        None,
    )
    .unwrap();
    let RuntimeNode::ChildAgentCall { budget, .. } =
        agent.plan.node(&agent.plan.entry_node_id).unwrap()
    else {
        panic!("child entry")
    };
    DeferOrchestrationToChildRun {
        fence,
        plan: agent.plan.clone(),
        slot_id: "child_worker".into(),
        selected_child_deployment: evidence.selected_deployment.clone(),
        selection_evidence: evidence,
        materialized_route: None,
        child_link_id: fresh(ResourceKind::ChildRunLink),
        child_run_id: fresh(ResourceKind::Run),
        child_root_scope_id: fresh(ResourceKind::ScopeInstance),
        child_entry_node_execution_id: fresh(ResourceKind::NodeExecution),
        child_orchestration_job_id: fresh(ResourceKind::Job),
        input: RunInputValue {
            value_id: fresh(ResourceKind::RunValue),
            classification: DataClassification::Internal,
            schema_digest: agent_schema().canonical_digest,
            content_digest: canonical_digest(&json!({"question":"select one"}))
                .unwrap()
                .parse()
                .unwrap(),
            value: ValueRef::Inline {
                value: json!({"question":"select one"}),
            },
        },
        source_value_ids: vec![input_id],
        budget: budget.clone(),
        cancellation_policy: ChildCancellationPolicy::CascadeAndWait,
        logical_key: format!("short-budget-{}", uuid::Uuid::now_v7()),
        child_attempt_limit: 3,
        child_retry_backoff_milliseconds: 100,
        idempotency_key_digest: fresh_digest(),
        request_digest: fresh_digest(),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        mutations: DeferOrchestrationChildMutationIds {
            receipt_id: fresh(ResourceKind::Receipt),
            quota_entry_ids: (0..4)
                .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
                .collect(),
            root_run_event_id: fresh(ResourceKind::Event),
            root_run_outbox_id: fresh(ResourceKind::OutboxEvent),
            parent_run_event_id: fresh(ResourceKind::Event),
            parent_run_outbox_id: fresh(ResourceKind::OutboxEvent),
            parent_node_event_id: fresh(ResourceKind::Event),
            parent_node_outbox_id: fresh(ResourceKind::OutboxEvent),
            parent_job_event_id: fresh(ResourceKind::Event),
            parent_job_outbox_id: fresh(ResourceKind::OutboxEvent),
            child_link_event_id: fresh(ResourceKind::Event),
            child_link_outbox_id: fresh(ResourceKind::OutboxEvent),
            child_run_event_id: fresh(ResourceKind::Event),
            child_run_outbox_id: fresh(ResourceKind::OutboxEvent),
            child_job_event_id: fresh(ResourceKind::Event),
            child_job_outbox_id: fresh(ResourceKind::OutboxEvent),
        },
    }
}

async fn apply(
    repo: &PgRepository,
    command: DeferOrchestrationToChildRun,
) -> DeferredOrchestrationChildRun {
    let mut tx = repo.begin_scheduler_transaction().await.unwrap();
    let CommandOutcome::Applied(result) = Box::pin(tx.defer_orchestration_to_child_run(command))
        .await
        .unwrap()
    else {
        panic!("fresh admission")
    };
    tx.commit().await.unwrap();
    result
}
async fn counts(pool: &PgPool) -> (i64, i64, i64, i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM insight_platform.runs),(SELECT count(*) FROM insight_platform.jobs),(SELECT count(*) FROM insight_platform.receipts),(SELECT count(*) FROM insight_platform.events),(SELECT count(*) FROM insight_platform.outbox_events),(SELECT count(*) FROM insight_platform.quota_ledger)").fetch_one(pool).await.unwrap()
}
async fn owner_state(pool: &PgPool, job: &str) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('run_version',run.version,'run_state',run.state,'active',run.active_work_count,'node_version',node.version,'node_state',node.state,'job_version',job.version,'job_state',job.state,'reservation',job.quota_reservation_id,'lease_epoch',job.lease_epoch,'quota_version',quota.version,'reserved',quota.reserved_value,'used',quota.used_value) FROM insight_platform.jobs job JOIN insight_platform.runs run ON run.tenant_id=job.tenant_id AND run.run_id=job.run_id JOIN insight_platform.run_nodes node ON node.tenant_id=job.tenant_id AND node.node_id=job.node_id JOIN insight_platform.quota_accounts quota ON quota.tenant_id=job.tenant_id AND quota.quota_account_id=$3 WHERE job.tenant_id=$1 AND job.job_id=$2")
        .bind(TENANT_ID).bind(job).bind(QUOTA_ACCOUNT_ID).fetch_one(pool).await.unwrap()
}
async fn permissions(repo: &PgRepository, pool: &PgPool, permissions: PermissionSet) {
    let (generation,version):(i64,i64)=sqlx::query_as("SELECT generation,version FROM insight_platform.tenant_principals WHERE tenant_id=$1 AND principal_id=$2 AND principal_kind='agent_runner'").bind(TENANT_ID).bind(PRINCIPAL_ID).fetch_one(pool).await.unwrap();
    let mut tx = repo.begin_security_transaction().await.unwrap();
    tx.update_tenant_principal_permissions(
        insight_platform_security::UpdateTenantPrincipalPermissions {
            audit: fresh_audit(),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::AgentRunner,
            expected_generation: generation,
            expected_version: version,
            permissions,
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}
async fn pause(repo: &PgRepository, pool: &PgPool, run: &str, requested: bool) {
    let (version,generation):(i64,i64)=sqlx::query_as("SELECT version,pause_generation FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2").bind(TENANT_ID).bind(run).fetch_one(pool).await.unwrap();
    let mut tx = repo.begin_run_transaction().await.unwrap();
    tx.set_run_pause(SetRunPause {
        audit: fresh_audit(),
        run_id: id(run),
        expected_run_version: version,
        expected_pause_generation: generation as u64,
        requested,
    })
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

pub(super) async fn verify(repo: &PgRepository, pool: &PgPool, base: &RunBindingsSnapshot) {
    Box::pin(verify_inner(repo, pool, base)).await;
}

async fn verify_inner(repo: &PgRepository, pool: &PgPool, base: &RunBindingsSnapshot) {
    let candidate = base
        .slots
        .iter()
        .find_map(|slot| match &slot.target {
            FrozenSlotTarget::ChildAgent { candidates, .. } => candidates.first().cloned(),
            _ => None,
        })
        .unwrap();
    let middle = clone_agent(pool, base, candidate, 100, 1).await;
    let outer = clone_agent(pool, base, middle.bindings.agent.clone(), 30_000, 8).await;
    let root_id = fresh(ResourceKind::Run);
    let job_id = fresh(ResourceKind::Job);
    let input_id = fresh(ResourceKind::RunValue);
    let input = json!({"question":"select one"});
    let mut tx = repo.begin_run_transaction().await.unwrap();
    tx.admit_run(AdmitRun {
        expected_agent_deployment: None,
        audit: fresh_audit(),
        admission_scope_id: outer.resource_id.clone(),
        run_id: root_id.clone(),
        agent_deployment_id: outer.bindings.agent.deployment_id.clone(),
        root_scope_id: fresh(ResourceKind::ScopeInstance),
        entry_node_execution_id: fresh(ResourceKind::NodeExecution),
        orchestration_job_id: job_id.clone(),
        entry_plan_node_key: outer.plan.entry_node_id.clone(),
        entry_node_kind: PlanNodeKind::ChildAgentCall,
        bindings: outer.bindings.clone(),
        input: RunInputValue {
            value_id: input_id.clone(),
            classification: DataClassification::Internal,
            schema_digest: agent_schema().canonical_digest,
            content_digest: canonical_digest(&input).unwrap().parse().unwrap(),
            value: ValueRef::Inline { value: input },
        },
        deadline: Utc::now() + Duration::minutes(2),
        inline_limits: JsonLimits::CONTRACT_FIXTURE,
        attempt_limit: 3,
        retry_backoff_milliseconds: 100,
    })
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let outer_command = defer(
        &outer,
        start(repo, pool, &job_id.to_string()).await,
        input_id,
    );
    let original = base.principal.permissions.clone();
    permissions(
        repo,
        pool,
        PermissionSet::new(
            original
                .iter()
                .filter(|permission| *permission != Permission::AgentRun)
                .collect(),
        )
        .unwrap(),
    )
    .await;
    let before_owner = owner_state(pool, &outer_command.fence.job_id).await;
    let before = counts(pool).await;
    let mut tx = repo.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        Box::pin(tx.defer_orchestration_to_child_run(outer_command.clone())).await,
        Err(RepositoryError::PermissionDenied)
    ));
    tx.rollback().await.unwrap();
    assert_eq!(counts(pool).await, before);
    assert_eq!(
        owner_state(pool, &outer_command.fence.job_id).await,
        before_owner
    );
    permissions(repo, pool, original).await;
    let outer_result = Box::pin(apply(repo, outer_command)).await;
    let middle_run = outer_result.child_run.run_id.clone();
    let command = defer(
        &middle,
        start(repo, pool, &outer_result.child_job.job_id).await,
        outer_result
            .child_run
            .input_value_id
            .as_deref()
            .unwrap()
            .parse()
            .unwrap(),
    );
    pause(repo, pool, &middle_run, true).await;
    assert_ne!(middle_run, root_id.to_string());
    let root_paused:bool=sqlx::query_scalar("SELECT (current_payload->'control'->>'pause_requested')::boolean FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2").bind(TENANT_ID).bind(root_id.to_string()).fetch_one(pool).await.unwrap();
    assert!(!root_paused);
    let before_owner = owner_state(pool, &command.fence.job_id).await;
    let before = counts(pool).await;
    let mut tx = repo.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        Box::pin(tx.defer_orchestration_to_child_run(command.clone())).await,
        Err(RepositoryError::Conflict(
            "orchestration child controlled Run"
        ))
    ));
    tx.rollback().await.unwrap();
    assert_eq!(counts(pool).await, before);
    assert_eq!(owner_state(pool, &command.fence.job_id).await, before_owner);
    pause(repo, pool, &middle_run, false).await;

    let mut blocker = pool.begin().await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    sqlx::query("LOCK TABLE insight_platform.jobs IN SHARE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let task_repo = repo.clone();
    let task_command = command.clone();
    let pending = tokio::spawn(async move { Box::pin(apply(&task_repo, task_command)).await });
    let wait_until = Instant::now() + StdDuration::from_secs(5);
    loop {
        let blocked:bool=sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%UPDATE insight_platform.jobs%')").bind(blocker_pid).fetch_one(pool).await.unwrap();
        if blocked {
            break;
        }
        assert!(
            Instant::now() < wait_until,
            "Child admission did not reach its post-anchor Job UPDATE"
        );
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    tokio::time::sleep(StdDuration::from_millis(150)).await;
    blocker.commit().await.unwrap();
    let result = tokio::time::timeout(StdDuration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        result.child_run.deadline,
        result.child_run.created_at + Duration::milliseconds(100)
    );
    assert!(
        result.child_run.deadline
            < sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()")
                .fetch_one(pool)
                .await
                .unwrap()
    );
    assert_eq!(result.parent_job.attempt_no, 1);
    assert_eq!(result.parent_run.active_work_count, 0);
    assert_eq!(result.child_job.attempt_no, 0);
    let timestamps:Vec<(DateTime<Utc>,DateTime<Utc>)>=sqlx::query_as("SELECT created_at,updated_at FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2 UNION ALL SELECT created_at,updated_at FROM insight_platform.run_nodes WHERE tenant_id=$1 AND node_id=ANY($3) UNION ALL SELECT created_at,updated_at FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$4 UNION ALL SELECT created_at,created_at FROM insight_platform.run_values WHERE tenant_id=$1 AND value_id=$5")
        .bind(TENANT_ID).bind(&result.child_run.run_id).bind(vec![command.child_root_scope_id.to_string(),command.child_entry_node_execution_id.to_string(),command.child_link_id.to_string()]).bind(&result.child_job.job_id).bind(command.input.value_id.to_string()).fetch_all(pool).await.unwrap();
    assert_eq!(timestamps.len(), 6);
    assert!(timestamps
        .iter()
        .all(|(created, updated)| *created == result.child_run.created_at && created == updated));
    let before = counts(pool).await;
    let mut tx = repo.begin_scheduler_transaction().await.unwrap();
    let CommandOutcome::Replayed(replayed) =
        Box::pin(tx.defer_orchestration_to_child_run(command.clone()))
            .await
            .unwrap()
    else {
        panic!("same Receipt")
    };
    tx.commit().await.unwrap();
    assert_eq!(replayed.child_run.deadline, result.child_run.deadline);
    assert_eq!(counts(pool).await, before);
    let mut altered = command;
    altered.budget.maximum_duration_milliseconds += 1;
    let mut tx = repo.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        Box::pin(tx.defer_orchestration_to_child_run(altered)).await,
        Err(RepositoryError::IdempotencyConflict)
    ));
    tx.rollback().await.unwrap();
    assert_eq!(counts(pool).await, before);
    let mut cursor = None;
    let until = Instant::now() + StdDuration::from_secs(10);
    loop {
        let state: String = sqlx::query_scalar(
            "SELECT state FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
        )
        .bind(TENANT_ID)
        .bind(&result.child_run.run_id)
        .fetch_one(pool)
        .await
        .unwrap();
        if state == "timed_out" {
            break;
        }
        assert!(Instant::now() < until, "expired child did not converge");
        let slot = || OrchestrationConvergenceSlot {
            quota_entry_ids: (0..4)
                .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
                .collect(),
            run_event_id: fresh(ResourceKind::Event),
            run_outbox_id: fresh(ResourceKind::OutboxEvent),
            node_event_id: fresh(ResourceKind::Event),
            node_outbox_id: fresh(ResourceKind::OutboxEvent),
            node_cancelling_event_id: fresh(ResourceKind::Event),
            node_cancelling_outbox_id: fresh(ResourceKind::OutboxEvent),
            scope_closing_event_id: fresh(ResourceKind::Event),
            scope_closing_outbox_id: fresh(ResourceKind::OutboxEvent),
            scope_terminal_event_id: fresh(ResourceKind::Event),
            scope_terminal_outbox_id: fresh(ResourceKind::OutboxEvent),
            job_event_id: fresh(ResourceKind::Event),
            job_outbox_id: fresh(ResourceKind::OutboxEvent),
        };
        let page = repo
            .drive_orchestration_convergence(DriveOrchestrationConvergence {
                shard: SafetyScanShard::whole(),
                after: cursor,
                limit: 16,
                slots: (0..16).map(|_| slot()).collect(),
            })
            .await
            .unwrap();
        cursor = page.next_cursor;
    }
    let (state, attempt): (String, i32) = sqlx::query_as(
        "SELECT state,attempt_no FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(TENANT_ID)
    .bind(&result.child_job.job_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!((state.as_str(), attempt), ("timed_out", 0));
    let effects: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.invocations WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(TENANT_ID)
    .bind(&result.child_run.run_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(effects, 0);
}
