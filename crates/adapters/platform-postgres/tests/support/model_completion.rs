//! The Model response envelope and the structural result are distinct durable values.
use super::*;
use insight_platform_jobs::store::JobRecord;
use insight_platform_orchestrator::store::{
    ApplyOrchestrationControllerStep, CommitPlanTerminal, ControllerActivationSlot,
    ControllerFacts, ControllerStepMutationIds, MaterializedTerminalValue,
    OrchestrationTerminalMutationIds,
};
use insight_platform_orchestrator::{decide_controller, ControllerDecision, ControllerObservation};

fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
async fn start(repository: &PgRepository, expected: &str) -> (RepositoryJobFence, JobRecord) {
    let worker = fresh(ResourceKind::WorkerProcessGeneration);
    let token = named_digest(&worker.to_string());
    let mut tx = repository
        .begin_orchestration_claim_transaction()
        .await
        .unwrap();
    let mut claims = tx
        .claim_orchestration_jobs_fixture(ClaimOrchestrationJobs {
            worker_manifest: support::orchestration(),
            worker_id: worker.clone(),
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
        })
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(claims.len(), 1);
    let claim = claims.pop().unwrap();
    assert_eq!(claim.job.job_id, expected);
    let fence = RepositoryJobFence {
        tenant_id: claim.job.tenant_id.clone(),
        job_id: claim.job.job_id.clone(),
        worker_id: worker,
        lease_epoch: claim.job.lease_epoch,
        expected_job_version: claim.job.version,
        lease_token_digest: token,
    };
    let request = StartOrchestrationJob {
        fence: fence.clone(),
        receipt_id: fresh(ResourceKind::Receipt),
        idempotency_key_digest: named_digest(&fresh(ResourceKind::Receipt).to_string()),
        request_digest: named_digest("completion proof start"),
        receipt_expires_at: Utc::now() + Duration::hours(1),
        job_event_id: fresh(ResourceKind::Event),
        job_outbox_id: fresh(ResourceKind::OutboxEvent),
        node_event_id: fresh(ResourceKind::Event),
        node_outbox_id: fresh(ResourceKind::OutboxEvent),
    };
    let mut tx = repository.begin_scheduler_transaction().await.unwrap();
    let CommandOutcome::Applied(job) = tx.start_orchestration_job(request).await.unwrap() else {
        panic!("fresh start")
    };
    tx.commit().await.unwrap();
    (
        RepositoryJobFence {
            expected_job_version: job.version,
            ..fence
        },
        job,
    )
}

pub(super) async fn assert_structured_completion_proof(
    pool: &PgPool,
    repository: &PgRepository,
    original: &Fixture,
) {
    let mut fixture = original.clone();
    fixture.run_id = fresh(ResourceKind::Run);
    fixture.scope_id = fresh(ResourceKind::ScopeInstance);
    let input = fresh(ResourceKind::RunValue);
    let current = RunCurrentSnapshot::initial(
        fixture.run_id.clone(),
        id(ResourceKind::AgentDeployment, 0x3a),
        input.clone(),
    );
    let current = TypedPayload::from_versioned(1, &current, 1_048_576).unwrap();
    sqlx::query(r#"INSERT INTO insight_platform.runs(tenant_id,run_id,root_run_id,agent_deployment_id,principal_id,state,version,bindings_schema_version,bindings,bindings_digest,current_schema_version,current_payload,current_payload_digest,deadline,started_at,created_at,updated_at,trace_id,execution_requirement_version,execution_requirement,execution_requirement_digest)
        SELECT tenant_id,$3,$3,agent_deployment_id,principal_id,'running',1,bindings_schema_version,bindings,bindings_digest,$4,$5,$6,deadline,statement_timestamp(),statement_timestamp(),statement_timestamp(),trace_id,execution_requirement_version,execution_requirement,execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2"#)
        .bind(original.tenant_id.to_string()).bind(original.run_id.to_string()).bind(fixture.run_id.to_string()).bind(current.schema_version).bind(&current.value).bind(&current.digest).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.run_values(tenant_id,value_id,run_id,value_kind,classification,schema_digest,content_digest,inline_value) SELECT tenant_id,$3,$4,value_kind,classification,schema_digest,content_digest,inline_value FROM insight_platform.run_values WHERE tenant_id=$1 AND value_id=$2")
        .bind(original.tenant_id.to_string()).bind(id(ResourceKind::RunValue,0x54).to_string()).bind(input.to_string()).bind(fixture.run_id.to_string()).execute(pool).await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET input_value_id=$3 WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(input.to_string())
    .execute(pool)
    .await
    .unwrap();
    let scope = TypedPayload::new(1, &json!({"root_run_id":fixture.run_id})).unwrap();
    sqlx::query("INSERT INTO insight_platform.run_nodes(tenant_id,node_id,run_id,record_kind,scope_id,logical_key,node_kind,state,generation,version,payload_schema_version,payload,payload_digest,deadline,started_at,created_at,updated_at) VALUES($1,$2,$3,'scope_instance',$2,'root','root','open',1,1,$4,$5,$6,$7,statement_timestamp(),statement_timestamp(),statement_timestamp())")
        .bind(fixture.tenant_id.to_string()).bind(fixture.scope_id.to_string()).bind(fixture.run_id.to_string()).bind(scope.schema_version).bind(&scope.value).bind(&scope.digest).bind(fixture.deadline).execute(pool).await.unwrap();
    let (_, command) = seed_running_model_orchestration(pool, repository, &fixture, 0x6100).await;
    let request = command.request.request.clone();
    let mut tx = repository.begin_scheduler_transaction().await.unwrap();
    let CommandOutcome::Applied(deferred) =
        tx.defer_orchestration_to_model_turn(command).await.unwrap()
    else {
        panic!("fresh defer")
    };
    tx.commit().await.unwrap();
    let worker = fresh(ResourceKind::WorkerProcessGeneration);
    let resume = resume_mutations(0x6300);
    let mut claims = repository
        .claim_model_jobs_current_rounds(ClaimModelJobs {
            worker_manifest: production_model_worker_manifest(),
            worker_process_generation_id: worker.clone(),
            worker_manifest_digest: production_model_worker_manifest_digest(),
            limit: 1,
            lease_milliseconds: 30_000,
            slots: vec![ModelClaimSlot {
                lease_token_digest: named_digest("structured-proof lease"),
                usage_reservation_id: fresh(ResourceKind::UsageReservation),
                quota_entry_ids: (0..4)
                    .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
                    .collect(),
                event_id: fresh(ResourceKind::Event),
                outbox_id: fresh(ResourceKind::OutboxEvent),
                resume_mutations: Some(resume.clone()),
                failure_mutations: Some(failure_mutations(0x6500)),
                tool_continuation_mutations: Some(
                    insight_platform_models::ModelToolContinuationMutationIds {
                        continuation_job_id: fresh(ResourceKind::Job),
                        run_event_id: fresh(ResourceKind::Event),
                        run_outbox_id: fresh(ResourceKind::OutboxEvent),
                        node_event_id: fresh(ResourceKind::Event),
                        node_outbox_id: fresh(ResourceKind::OutboxEvent),
                        continuation_job_event_id: fresh(ResourceKind::Event),
                        continuation_job_outbox_id: fresh(ResourceKind::OutboxEvent),
                    },
                ),
            }],
        })
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    let claim = claims.pop().unwrap();
    assert_eq!(claim.turn.model_turn_id, deferred.turn.model_turn_id);
    let response = output(
        &fixture,
        final_response(&fixture),
        fresh(ResourceKind::RunValue),
    );
    let structured_id = response.structured_output_value_id.clone().unwrap();
    let response_id = response.value_id.clone();
    let outcome = CommitModelOutcome {
        audit: worker_audit(&fixture.tenant_id, &worker, 0x6400, 'a', 'b'),
        model_turn_id: claim.turn.model_turn_id.clone(),
        job_id: claim.job.job_id.parse().unwrap(),
        expected_turn_version: claim.turn.version,
        fence: claim.fence,
        usage_reservation_id: claim.usage_reservation_id,
        quota_entry_ids: (0..4)
            .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
            .collect(),
        request,
        outcome: ModelDispatchOutcome::Succeeded(Box::new(response)),
        resume_mutations: Some(resume.clone()),
        failure_mutations: None,
        tool_continuation_mutations: None,
    };
    let CommandOutcome::Applied(completed) = execute_outcome(repository, outcome).await.unwrap()
    else {
        panic!("fresh result")
    };
    assert_eq!(completed.turn.output_value_id, Some(response_id.clone()));
    assert_ne!(response_id, structured_id);
    let (fence, job) = start(repository, &resume.continuation_job_id.to_string()).await;
    let facts = repository
        .load_controller_facts(&fence, &fixture.runtime_plan)
        .await
        .unwrap();
    let ControllerFacts::Committed {
        identity,
        observation,
    } = facts
    else {
        panic!("completed facts")
    };
    assert_eq!(observation, ControllerObservation::ExternalLeafCompleted);
    assert!(matches!(
        decide_controller(
            fixture.runtime_plan.node(&identity.plan_node_key).unwrap(),
            &observation
        )
        .unwrap(),
        ControllerDecision::CompleteNode { .. }
    ));
    let original_payload = OrchestrationJobPayload::from_payload(&job.payload).unwrap();
    let mut forged = original_payload.clone();
    forged
        .external_leaf_completion
        .as_mut()
        .unwrap()
        .output
        .value_id = response_id;
    let forged = forged.to_payload().unwrap();
    sqlx::query("UPDATE insight_platform.jobs SET payload=$3,payload_digest=$4 WHERE tenant_id=$1 AND job_id=$2").bind(&fence.tenant_id).bind(&fence.job_id).bind(&forged.value).bind(&forged.digest).execute(pool).await.unwrap();
    assert!(
        repository
            .load_controller_facts(&fence, &fixture.runtime_plan)
            .await
            .is_err(),
        "response envelope is not the structural result"
    );
    sqlx::query("UPDATE insight_platform.jobs SET payload=$3,payload_digest=$4 WHERE tenant_id=$1 AND job_id=$2").bind(&fence.tenant_id).bind(&fence.job_id).bind(&job.payload.value).bind(&job.payload.digest).execute(pool).await.unwrap();
    let original_row:(String,serde_json::Value)=sqlx::query_as("SELECT node_id,inline_value FROM insight_platform.run_values WHERE tenant_id=$1 AND value_id=$2").bind(&fence.tenant_id).bind(structured_id.to_string()).fetch_one(pool).await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.run_values SET node_id=$3 WHERE tenant_id=$1 AND value_id=$2",
    )
    .bind(&fence.tenant_id)
    .bind(structured_id.to_string())
    .bind(original.primary_node_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    assert!(
        repository
            .load_controller_facts(&fence, &fixture.runtime_plan)
            .await
            .is_err(),
        "another Node cannot provide completion proof"
    );
    sqlx::query("UPDATE insight_platform.run_values SET node_id=$3,inline_value=$4 WHERE tenant_id=$1 AND value_id=$2").bind(&fence.tenant_id).bind(structured_id.to_string()).bind(&original_row.0).bind(json!({"answer":"tampered"})).execute(pool).await.unwrap();
    assert!(
        repository
            .load_controller_facts(&fence, &fixture.runtime_plan)
            .await
            .is_err(),
        "matching metadata cannot authorize different structured body"
    );
    sqlx::query(
        "UPDATE insight_platform.run_values SET inline_value=$3 WHERE tenant_id=$1 AND value_id=$2",
    )
    .bind(&fence.tenant_id)
    .bind(structured_id.to_string())
    .bind(&original_row.1)
    .execute(pool)
    .await
    .unwrap();
    let next_job = fresh(ResourceKind::Job);
    let command = ApplyOrchestrationControllerStep {
        fence,
        plan: fixture.runtime_plan.clone(),
        observation,
        idempotency_key_digest: named_digest("model completed controller"),
        request_digest: named_digest("model completed controller request"),
        receipt_expires_at: fixture.deadline,
        mutations: ControllerStepMutationIds {
            receipt_id: fresh(ResourceKind::Receipt),
            quota_entry_ids: (0..4)
                .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
                .collect(),
            run_event_id: fresh(ResourceKind::Event),
            run_outbox_id: fresh(ResourceKind::OutboxEvent),
            node_event_id: fresh(ResourceKind::Event),
            node_outbox_id: fresh(ResourceKind::OutboxEvent),
            job_event_id: fresh(ResourceKind::Event),
            job_outbox_id: fresh(ResourceKind::OutboxEvent),
            activations: vec![ControllerActivationSlot {
                node_execution_id: fresh(ResourceKind::NodeExecution),
                orchestration_job_id: next_job.clone(),
                scope: None,
                node_event_id: fresh(ResourceKind::Event),
                node_outbox_id: fresh(ResourceKind::OutboxEvent),
                job_event_id: fresh(ResourceKind::Event),
                job_outbox_id: fresh(ResourceKind::OutboxEvent),
            }],
            pending_nodes: vec![],
            structural_exit: None,
            pending_wake: None,
            remainder_cancellations: vec![],
        },
    };
    sqlx::query(
        "UPDATE insight_platform.run_values SET inline_value=$3 WHERE tenant_id=$1 AND value_id=$2",
    )
    .bind(&command.fence.tenant_id)
    .bind(structured_id.to_string())
    .bind(json!({"answer":"tampered-after-facts"}))
    .execute(pool)
    .await
    .unwrap();
    let mut rejected = repository.begin_scheduler_transaction().await.unwrap();
    assert!(
        rejected
            .apply_orchestration_controller_step(command.clone())
            .await
            .is_err(),
        "commit independently checks physical result proof after facts were read"
    );
    rejected.rollback().await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.run_values SET inline_value=$3 WHERE tenant_id=$1 AND value_id=$2",
    )
    .bind(&command.fence.tenant_id)
    .bind(structured_id.to_string())
    .bind(&original_row.1)
    .execute(pool)
    .await
    .unwrap();
    let mut tx = repository.begin_scheduler_transaction().await.unwrap();
    let CommandOutcome::Applied(step) = tx
        .apply_orchestration_controller_step(command)
        .await
        .unwrap()
    else {
        panic!("fresh controller")
    };
    tx.commit().await.unwrap();
    assert_eq!(step.activations[0].plan_node_key.as_str(), "finish");
    let (fence, _) = start(repository, &next_job.to_string()).await;
    let resolved = repository
        .load_plan_terminal_value(&fence, &fixture.runtime_plan)
        .await
        .unwrap();
    assert_eq!(resolved.run_value_id, structured_id);
    let ValueRef::Inline { value: body } = resolved.value else {
        panic!("structured value inline")
    };
    assert_eq!(body, json!({"answer":"done"}));
    let terminal = CommitPlanTerminal {
        fence,
        plan: fixture.runtime_plan.clone(),
        value: MaterializedTerminalValue {
            value_id: resolved.run_value_id,
            classification: resolved.classification,
            schema_digest: resolved.schema_digest,
            content_digest: resolved.content_digest,
            body,
        },
        idempotency_key_digest: named_digest("model return"),
        request_digest: named_digest("model return request"),
        receipt_expires_at: fixture.deadline,
        mutations: OrchestrationTerminalMutationIds {
            receipt_id: fresh(ResourceKind::Receipt),
            quota_entry_ids: (0..4)
                .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
                .collect(),
            run_event_id: fresh(ResourceKind::Event),
            run_outbox_id: fresh(ResourceKind::OutboxEvent),
            node_event_id: fresh(ResourceKind::Event),
            node_outbox_id: fresh(ResourceKind::OutboxEvent),
            scope_closing_event_id: fresh(ResourceKind::Event),
            scope_closing_outbox_id: fresh(ResourceKind::OutboxEvent),
            scope_terminal_event_id: fresh(ResourceKind::Event),
            scope_terminal_outbox_id: fresh(ResourceKind::OutboxEvent),
            job_event_id: fresh(ResourceKind::Event),
            job_outbox_id: fresh(ResourceKind::OutboxEvent),
        },
    };
    let mut tx = repository.begin_scheduler_transaction().await.unwrap();
    let CommandOutcome::Applied(done) = tx.commit_plan_terminal(terminal).await.unwrap() else {
        panic!("fresh Return")
    };
    tx.commit().await.unwrap();
    assert_eq!(done.run.state, "succeeded");
    assert_eq!(done.run.active_work_count, 0);
    assert_eq!(done.run.current.output_value_id, Some(structured_id));
    model_public_events::assert_projection(
        pool,
        repository,
        &fixture,
        &[
            "model.started",
            "model.completed",
            "run.completed",
            "node.completed",
        ],
    )
    .await;
}
