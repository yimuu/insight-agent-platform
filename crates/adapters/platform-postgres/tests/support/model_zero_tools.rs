//! Text-only controller admission uses real immutable revisions and a separate Run.
use super::*;
use insight_platform_artifacts::{
    SchedulerSkillPackageReadError, SchedulerSkillPackageReadRequest, SchedulerSkillPackageReader,
};
use insight_platform_contracts::ClosedValueSchema;
use insight_platform_postgres::controller_admission::PostgresControllerModelAdmissionProvider;
use insight_platform_runtime::{
    ControllerModelAdmissionProvider, ControllerModelAdmissionRequest,
    ControllerRunValueReadContext, DurablePlanDriverError,
};
use serde_json::Value;

struct NoSkillReads;
#[async_trait::async_trait]
impl SchedulerSkillPackageReader for NoSkillReads {
    async fn read_exact(
        &self,
        _: SchedulerSkillPackageReadRequest,
    ) -> Result<Vec<u8>, SchedulerSkillPackageReadError> {
        panic!("text-only admission must not materialize a Skill");
    }
}

pub(super) async fn verify(pool: &PgPool, repository: &PgRepository, original: &Fixture) {
    let facts = repository
        .load_exact_controller_model_assembly_facts(
            &original.tenant_id,
            &original.run_id,
            "primary_model",
            &original.model_deployment,
            std::slice::from_ref(&original.tool_slot_binding),
        )
        .await
        .unwrap();
    let mut profile = facts.profile.clone();
    profile.tools = ModelToolContract {
        supported: false,
        parallel: false,
        maximum_tools: 0,
        maximum_calls_per_turn: 0,
        maximum_argument_bytes: 0,
    };
    profile.limits.maximum_tools = 0;
    profile.limits.maximum_parallel_tool_calls = 0;
    profile.limits.maximum_rounds = 1;
    profile.structured_output.native = false;
    profile.usage.provider_reports_usage = false;
    let mut fixture = original.clone();
    fixture.profile_revision = ExactVersionRef::new(
        id(ResourceKind::ModelProfileRevision, 0x9000),
        canonical_digest(&serde_json::to_value(&profile).unwrap())
            .unwrap()
            .parse()
            .unwrap(),
    )
    .unwrap();
    insert_version(
        pool,
        &fixture.tenant_id,
        &fixture.profile_resource_id,
        RegistryResourceKind::ModelProfile,
        &fixture.profile_revision,
        99,
        &fixture.principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::ModelProfile(Box::new(profile.clone())),
            validation: validation(),
        },
    )
    .await;
    fixture.model_closure.profile_revision = fixture.profile_revision.clone();
    let model_payload = TypedPayload::new(
        1,
        &DeploymentClosure::ModelProfile(fixture.model_closure.clone()),
    )
    .unwrap();
    fixture.model_deployment = ExactDeploymentRef::new(
        id(ResourceKind::ModelDeployment, 0x9001),
        model_payload.digest.parse().unwrap(),
    )
    .unwrap();
    insert_deployment(
        pool,
        &fixture.tenant_id,
        &fixture.model_deployment.deployment_id,
        &fixture.profile_resource_id,
        &fixture.profile_revision.revision_id,
        &fixture.principal_id,
        &model_payload,
    )
    .await;
    model_quota::provision_additional(repository, &fixture.tenant_id, &fixture.model_deployment)
        .await;

    let agent_output_schema = facts.agent.output_schema.clone();
    fixture.output_schema=ClosedJsonSchema::build(json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,
        "required":["answer"],"properties":{"answer":{"type":"string","minLength":0,"maxLength":64,"x-platform-max-bytes":256}}
    })).unwrap();
    assert_ne!(
        fixture.output_schema.canonical_digest,
        agent_output_schema.canonical_digest
    );
    let response_value_schema = ClosedValueSchema::try_from(fixture.output_schema.clone()).unwrap();
    fixture.runtime_plan.schema_documents.insert(
        response_value_schema.canonical_digest.clone(),
        response_value_schema,
    );
    fixture.runtime_plan.dependency_slots.remove("search");
    let key = PlanNodeKey::new("model".into()).unwrap();
    let node = fixture.runtime_plan.nodes.get_mut(&key).unwrap();
    let RuntimeNode::ModelLoop {
        capability_slot_ids,
        maximum_rounds,
        maximum_capability_calls,
        maximum_parallel_calls_per_round,
        output,
        resume,
        ..
    } = node
    else {
        panic!("model node")
    };
    capability_slot_ids.clear();
    *maximum_rounds = 1;
    *maximum_capability_calls = 0;
    *maximum_parallel_calls_per_round = 0;
    let ExactDataPortRef::NodeOutput { schema_digest, .. } = output else {
        panic!("model output")
    };
    *schema_digest = fixture.output_schema.canonical_digest.clone();
    let new_output = output.clone();
    let adapt = PlanNodeKey::new("adapt".into()).unwrap();
    let finish = resume.clone();
    *resume = adapt.clone();
    let final_output = ExactDataPortRef::NodeOutput {
        producer_node_id: adapt.clone(),
        port_id: DataPortKey::new("final".into()).unwrap(),
        schema_digest: agent_output_schema.canonical_digest.clone(),
    };
    for node in fixture.runtime_plan.nodes.values_mut() {
        if let RuntimeNode::Return { value } = node {
            *value = final_output.clone();
        }
    }
    fixture.runtime_plan.nodes.insert(
        adapt,
        RuntimeNode::Compute {
            assignments: vec![insight_platform_plan::PortAssignment {
                output_port: final_output,
                expression: insight_platform_plan::TypedExpressionProgram::build(
                    vec![new_output.clone()],
                    vec![insight_platform_plan::TypedInstruction::LoadPort { port: new_output }],
                    agent_output_schema.canonical_digest.clone(),
                    insight_platform_plan::ExpressionLimits::ABSOLUTE,
                )
                .unwrap(),
            }],
            next: finish,
        },
    );
    let plan_limits = PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
    fixture.runtime_plan.validate(plan_limits).unwrap();
    let plan_digest = fixture.runtime_plan.canonical_digest(plan_limits).unwrap();
    let plan_bytes = canonical_json(&serde_json::to_value(&fixture.runtime_plan).unwrap()).unwrap();
    fixture.agent_closure.plan = ExactVersionRef::new(
        id(ResourceKind::AgentPlanRevision, 0x9002),
        plan_digest.clone(),
    )
    .unwrap();
    fixture.agent_closure.interface = version(ResourceKind::AgentInterfaceRevision, 0x9003, '8');
    fixture
        .agent_closure
        .slots
        .retain(|slot| slot.slot_id == "primary_model");
    let FrozenSlotTarget::Model { candidates, .. } = &mut fixture.agent_closure.slots[0].target
    else {
        panic!("model slot")
    };
    *candidates = vec![fixture.model_deployment.clone()];
    fixture.agent_closure.slots[0].binding_digest = fixture.agent_closure.slots[0]
        .expected_binding_digest()
        .unwrap();
    let plan_artifact = ArtifactRef::new(
        id(ResourceKind::Artifact, 0x9004),
        plan_digest.clone(),
        plan_bytes.len() as u64,
        "application/json",
        DataClassification::Internal,
        Some("zero-tools-plan.json".into()),
    )
    .unwrap();
    let mut agent = facts.agent;
    agent.typed_plan_digest = plan_digest.clone();
    agent.typed_plan_artifact_id = plan_artifact.artifact_id().clone();
    for exact in [
        &fixture.agent_closure.interface,
        &fixture.agent_closure.plan,
    ] {
        insert_version(
            pool,
            &fixture.tenant_id,
            &fixture.agent_resource_id,
            RegistryResourceKind::Agent,
            exact,
            99,
            &fixture.principal_id,
            PublishedVersionPayload {
                document: ResourceDocument::Agent(agent.clone()),
                validation: validation(),
            },
        )
        .await;
    }
    insert_ready_artifact(
        pool,
        &fixture.tenant_id,
        &fixture.principal_id,
        &fixture.invocation_policy.revision_id,
        &plan_artifact,
        0x9010,
    )
    .await;
    sqlx::query("UPDATE insight_platform.artifacts SET purpose='typed_plan' WHERE tenant_id=$1 AND artifact_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(plan_artifact.artifact_id().to_string()).execute(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.resource_versions SET artifact_id=$3 WHERE tenant_id=$1 AND resource_version_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(fixture.agent_closure.plan.revision_id.to_string()).bind(plan_artifact.artifact_id().to_string()).execute(pool).await.unwrap();
    let agent_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(fixture.agent_closure.clone())).unwrap();
    let deployment = ExactDeploymentRef::new(
        id(ResourceKind::AgentDeployment, 0x9005),
        agent_payload.digest.parse().unwrap(),
    )
    .unwrap();
    insert_deployment(
        pool,
        &fixture.tenant_id,
        &deployment.deployment_id,
        &fixture.agent_resource_id,
        &fixture.agent_closure.plan.revision_id,
        &fixture.principal_id,
        &agent_payload,
    )
    .await;
    fixture.run_id = id(ResourceKind::Run, 0x9006);
    fixture.scope_id = id(ResourceKind::ScopeInstance, 0x9007);
    let input_id = id(ResourceKind::RunValue, 0x9008);
    let current = TypedPayload::from_versioned(
        1,
        &RunCurrentSnapshot::initial(
            fixture.run_id.clone(),
            deployment.deployment_id.clone(),
            input_id.clone(),
        ),
        1_048_576,
    )
    .unwrap();
    let bindings = RunBindingsSnapshot::build(
        deployment.clone(),
        facts.run_bindings.principal,
        &fixture.agent_closure,
    )
    .unwrap();
    let bindings_payload = TypedPayload::from_versioned(1, &bindings, 1_048_576).unwrap();
    let requirement =
        insight_platform_plan::execution::program_execution_requirement(plan_digest, 6).unwrap();
    sqlx::query(r#"INSERT INTO insight_platform.runs(tenant_id,run_id,root_run_id,agent_deployment_id,principal_id,state,version,bindings_schema_version,bindings,bindings_digest,current_schema_version,current_payload,current_payload_digest,deadline,started_at,created_at,updated_at,trace_id,execution_requirement_version,execution_requirement,execution_requirement_digest)
        VALUES($1,$2,$2,$3,$4,'running',1,$5,$6,$7,$8,$9,$10,$11,statement_timestamp(),statement_timestamp(),statement_timestamp(),'0123456789abcdef0123456789abcdef',1,$12,$13)"#)
        .bind(fixture.tenant_id.to_string()).bind(fixture.run_id.to_string()).bind(deployment.deployment_id.to_string()).bind(fixture.principal_id.to_string())
        .bind(bindings_payload.schema_version).bind(&bindings_payload.value).bind(bindings.canonical_digest.to_string())
        .bind(current.schema_version).bind(&current.value).bind(&current.digest).bind(fixture.deadline)
        .bind(serde_json::to_value(&requirement).unwrap()).bind(requirement.canonical_digest().unwrap().to_string()).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.run_values(tenant_id,value_id,run_id,value_kind,classification,schema_digest,content_digest,inline_value) SELECT tenant_id,$3,$4,value_kind,classification,schema_digest,content_digest,inline_value FROM insight_platform.run_values WHERE tenant_id=$1 AND value_id=$2")
        .bind(original.tenant_id.to_string()).bind(id(ResourceKind::RunValue,0x54).to_string()).bind(input_id.to_string()).bind(fixture.run_id.to_string()).execute(pool).await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET input_value_id=$3 WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(input_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let scope = TypedPayload::new(1, &json!({"root_run_id":fixture.run_id})).unwrap();
    sqlx::query("INSERT INTO insight_platform.run_nodes(tenant_id,node_id,run_id,record_kind,scope_id,logical_key,node_kind,state,generation,version,payload_schema_version,payload,payload_digest,deadline,started_at,created_at,updated_at) VALUES($1,$2,$3,'scope_instance',$2,'root','root','open',1,1,$4,$5,$6,$7,statement_timestamp(),statement_timestamp(),statement_timestamp())")
        .bind(fixture.tenant_id.to_string()).bind(fixture.scope_id.to_string()).bind(fixture.run_id.to_string()).bind(scope.schema_version).bind(&scope.value).bind(&scope.digest).bind(fixture.deadline).execute(pool).await.unwrap();
    let has_quota: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND quota_account_id=$2)")
        .bind(fixture.tenant_id.to_string()).bind(id(ResourceKind::QuotaAccount,0x801).to_string()).fetch_one(pool).await.unwrap();
    if !has_quota {
        repository
            .create_quota_account(NewQuotaAccount {
                tenant_id: fixture.tenant_id.to_string(),
                quota_account_id: id(ResourceKind::QuotaAccount, 0x801).to_string(),
                scope_kind: "tenant".into(),
                scope_id: fixture.tenant_id.to_string(),
                work_class: "orchestration".into(),
                metric: "concurrent_jobs".into(),
                limit_value: 4,
                payload: TypedPayload::new(1, &json!({"fixture":"model-owner"})).unwrap(),
            })
            .await
            .unwrap();
    }
    let (fence, mut command) =
        seed_running_model_orchestration(pool, repository, &fixture, 0x9100).await;
    let provider = PostgresControllerModelAdmissionProvider::new(
        repository.clone(),
        Arc::new(repository.clone()),
        Arc::new(NoSkillReads),
        Arc::new(NoSkillReads),
    );
    let request = ControllerModelAdmissionRequest {
        lease: ControllerRunValueReadContext {
            tenant_id: fixture.tenant_id.clone(),
            run_id: fixture.run_id.clone(),
            orchestration_job_id: fence.job_id.parse().unwrap(),
            worker_process_generation_id: fence.worker_id.clone(),
            lease_generation: fence.lease_epoch as u64,
            lease_token_digest: fence.lease_token_digest.clone(),
            deadline: fixture.deadline,
        },
        tenant_id: fixture.tenant_id.clone(),
        run_id: fixture.run_id.clone(),
        node_execution_id: id(ResourceKind::NodeExecution, 0x9100),
        model_turn_id: command.model_turn_id.clone(),
        request_value_id: command.request.value_id.clone(),
        selected_deployment: fixture.model_deployment.clone(),
        model_slot_id: "primary_model".into(),
        plan_node_key: key.clone(),
        plan_node: fixture.runtime_plan.nodes[&key].clone(),
        response_schema: fixture.runtime_plan.model_response_schema(&key).unwrap(),
        input: command.input.clone(),
        input_value: ClosedJsonValue::build(
            command.input.schema_digest.clone(),
            json!({"prompt":"fixture"}),
        )
        .unwrap(),
        tool_slots: vec![],
        maximum_rounds: 1,
        maximum_capability_calls: 0,
        maximum_parallel_calls_per_round: 0,
        token_budget: 4096,
        deadline: fixture.deadline,
    };
    let before = support::fixture_durable_counts(pool, &fixture.tenant_id).await;
    let decision = provider
        .assemble(request.clone())
        .await
        .expect("actual PostgreSQL text-only controller admission");
    assert!(decision.request.request.tools.is_empty());
    assert_eq!(
        decision
            .request
            .request
            .response_contract
            .output_schema_digest,
        fixture.output_schema.canonical_digest
    );
    assert_eq!(
        decision
            .request
            .request
            .response_contract
            .structured_schema
            .as_ref(),
        Some(&fixture.output_schema)
    );
    for schema in [agent_output_schema.clone(), {
        let mut tampered = fixture.output_schema.clone();
        tampered.schema["required"] = json!([]);
        tampered
    }] {
        let mut rejected = request.clone();
        rejected.response_schema = schema;
        assert!(matches!(
            provider.assemble(rejected).await,
            Err(DurablePlanDriverError::InvariantViolation)
        ));
    }
    assert!(
        !decision
            .request
            .request
            .response_contract
            .allow_tool_intents
    );
    for variant in 0..4 {
        let mut rejected = request.clone();
        if let RuntimeNode::ModelLoop {
            capability_slot_ids,
            maximum_capability_calls,
            maximum_parallel_calls_per_round,
            ..
        } = &mut rejected.plan_node
        {
            match variant {
                0 => {
                    capability_slot_ids.push("search".into());
                    rejected.tool_slots.push(original.tool_slot_binding.clone());
                }
                1 => {
                    *maximum_capability_calls = 1;
                    *maximum_parallel_calls_per_round = 1;
                    rejected.maximum_capability_calls = 1;
                    rejected.maximum_parallel_calls_per_round = 1;
                }
                2 => {
                    *maximum_capability_calls = 1;
                    rejected.maximum_capability_calls = 1;
                }
                _ => {
                    rejected.maximum_capability_calls = 1;
                }
            }
        }
        assert!(matches!(
            provider.assemble(rejected).await,
            Err(DurablePlanDriverError::InvariantViolation)
        ));
    }
    assert_eq!(
        support::fixture_durable_counts(pool, &fixture.tenant_id).await,
        before
    );
    final_response(&fixture)
        .validate_for(
            &decision.request.request,
            &facts.provider,
            &profile,
            ModelTurnLimits::from_profile(&checked_in_hard_limit_profile()).unwrap(),
        )
        .expect("valid text-only response before unexpected-tool negative");
    assert!(tool_response(&fixture, json!({"query":"forbidden"}))
        .validate_for(
            &decision.request.request,
            &facts.provider,
            &profile,
            ModelTurnLimits::from_profile(&checked_in_hard_limit_profile()).unwrap()
        )
        .is_err());
    command.request = decision.request;
    command.tool_slots.clear();
    command.requested_attempt_limit = decision.requested_attempt_limit;
    command.cost_ceiling_microunits = decision.cost_ceiling_microunits;
    let before_bad = support::fixture_durable_counts(pool, &fixture.tenant_id).await;
    let quotas_before:Value=sqlx::query_scalar("SELECT coalesce(jsonb_agg(to_jsonb(q) ORDER BY quota_account_id),'[]'::jsonb) FROM insight_platform.quota_accounts q WHERE tenant_id=$1")
        .bind(fixture.tenant_id.to_string()).fetch_one(pool).await.unwrap();
    for wrong in [None, Some(agent_output_schema)] {
        let mut forged = command.clone();
        let response = &mut forged.request.request.response_contract;
        if let Some(schema) = &wrong {
            response.output_schema_digest = schema.canonical_digest.clone();
        }
        response.structured_schema = wrong;
        let value = serde_json::to_value(&forged.request.request).unwrap();
        forged.request.content_digest = canonical_digest(&value).unwrap().parse().unwrap();
        forged.request.value = ValueRef::Inline { value };
        let mut tx = repository.begin_scheduler_transaction().await.unwrap();
        let failure = tx
            .defer_orchestration_to_model_turn(forged)
            .await
            .expect_err("forged node contract must fail");
        assert!(
            matches!(
                failure,
                RepositoryError::Conflict("Model node response schema")
            ),
            "unexpected rejection: {failure:?}"
        );
        tx.commit().await.unwrap();
        assert_eq!(
            support::fixture_durable_counts(pool, &fixture.tenant_id).await,
            before_bad
        );
        let quotas_after:Value=sqlx::query_scalar("SELECT coalesce(jsonb_agg(to_jsonb(q) ORDER BY quota_account_id),'[]'::jsonb) FROM insight_platform.quota_accounts q WHERE tenant_id=$1")
            .bind(fixture.tenant_id.to_string()).fetch_one(pool).await.unwrap();
        assert_eq!(quotas_after, quotas_before);
    }
    let mut tx = repository.begin_scheduler_transaction().await.unwrap();
    let CommandOutcome::Applied(deferred) = tx
        .defer_orchestration_to_model_turn(command.clone())
        .await
        .unwrap()
    else {
        panic!("fresh defer")
    };
    tx.commit().await.unwrap();
    assert_eq!(deferred.turn.state, ModelTurnState::Ready);
    assert_eq!(deferred.model_job.state, "ready");
    let wait: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.run_nodes WHERE tenant_id=$1 AND node_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::NodeExecution, 0x9100).to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    // The ModelTurn owner embeds its controller continuation with the original zero pair.
    assert_eq!(wait["maximum_capability_calls"], 0);
    assert_eq!(wait["maximum_parallel_calls_per_round"], 0);
    assert_eq!(
        wait["model_turn_id"],
        deferred.turn.model_turn_id.to_string()
    );
    let mut tx = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        tx.defer_orchestration_to_model_turn(command).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));
    tx.commit().await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.invocations WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[async_trait::async_trait]
impl insight_platform_artifacts::SchedulerRunValueReader for NoSkillReads {
    async fn read_exact(
        &self,
        _: insight_platform_artifacts::SchedulerRunValueReadRequest,
    ) -> Result<Vec<u8>, insight_platform_artifacts::SchedulerRunValueReadError> {
        panic!("no conversation Artifact in this fixture")
    }
}
