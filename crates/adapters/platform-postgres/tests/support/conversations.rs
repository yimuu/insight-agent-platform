//! Physical conversation transactions and provider-wire history qualification.
use super::*;
use insight_platform_postgres::repository::ConversationReadScope;
fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
fn command_audit(f: &Fixture) -> CommandAudit {
    let mut a = audit(&f.tenant_id, &f.principal_id, 0xfe00, 'c', 'd');
    a.receipt_id = fresh(ResourceKind::Receipt);
    a.event_id = fresh(ResourceKind::Event);
    a.outbox_id = fresh(ResourceKind::OutboxEvent);
    a.idempotency_key_digest =
        insight_platform_contracts::canonical_digest(&json!({"key":a.receipt_id}))
            .unwrap()
            .parse()
            .unwrap();
    a.request_digest = a.idempotency_key_digest.clone();
    a
}
pub(super) async fn verify(pool: &PgPool, repo: &PgRepository, f: &Fixture) {
    let mut closure = f.agent_closure.clone();
    for binding in closure
        .policies
        .iter_mut()
        .chain(std::iter::once(&mut closure.execution_profile))
    {
        let payload = TypedPayload::new(
            1,
            &DeploymentClosure::Policy(PolicyDeploymentClosure {
                policy_revision: binding.revision.clone(),
                applicability_digest: digest('a'),
                qualification_evidence: authoring(0xb9, '9').artifact,
            }),
        )
        .unwrap();
        let deployment = fresh(ResourceKind::PolicyDeployment);
        insert_deployment(
            pool,
            &f.tenant_id,
            &deployment,
            &f.policy_resource_id,
            &binding.revision.revision_id,
            &f.principal_id,
            &payload,
        )
        .await;
        binding.deployment =
            ExactDeploymentRef::new(deployment, payload.digest.parse().unwrap()).unwrap();
    }
    // Model fixture policies previously needed no physical evidence; root admission does.
    let policies: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT bindings FROM insight_platform.deployments WHERE tenant_id=$1")
            .bind(f.tenant_id.to_string())
            .fetch_all(pool)
            .await
            .unwrap();
    let mut blob_base = 0xee00;
    for mut value in policies {
        value.as_object_mut().unwrap().remove("schema_version");
        if let Ok(DeploymentClosure::Policy(policy)) =
            serde_json::from_value::<DeploymentClosure>(value)
        {
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.artifacts WHERE tenant_id=$1 AND artifact_id=$2)").bind(f.tenant_id.to_string()).bind(policy.qualification_evidence.artifact_id().to_string()).fetch_one(pool).await.unwrap();
            if !exists {
                insert_ready_artifact(
                    pool,
                    &f.tenant_id,
                    &f.principal_id,
                    &f.invocation_policy.revision_id,
                    &policy.qualification_evidence,
                    blob_base,
                )
                .await;
                blob_base += 1;
            }
        }
    }
    let payload = TypedPayload::new(1, &DeploymentClosure::Agent(closure.clone())).unwrap();
    let deployment = fresh(ResourceKind::AgentDeployment);
    insert_deployment(
        pool,
        &f.tenant_id,
        &deployment,
        &f.agent_resource_id,
        &closure.plan.revision_id,
        &f.principal_id,
        &payload,
    )
    .await;
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2").bind(f.tenant_id.to_string()).bind(f.agent_resource_id.to_string()).bind(deployment.to_string()).execute(pool).await.unwrap();

    let permissions = PermissionSet::new(vec![
        Permission::AgentRun,
        Permission::CapabilityInvoke,
        Permission::ModelDeploy,
        Permission::ModelInvoke,
        Permission::RuntimeControl,
        Permission::RuntimeRead,
        Permission::TenantManage,
        Permission::ArtifactRead,
    ])
    .unwrap();
    let payload = TypedPayload::new(
        1,
        &TenantPrincipalPayload {
            permissions: permissions.clone(),
        },
    )
    .unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4,version=2 WHERE tenant_id=$1 AND principal_id=$2").bind(f.tenant_id.to_string()).bind(f.principal_id.to_string()).bind(payload.value).bind(payload.digest).execute(pool).await.unwrap();
    let scope = ConversationReadScope {
        tenant_id: f.tenant_id.clone(),
        principal_id: f.principal_id.clone(),
        principal_kind: PrincipalKind::AgentRunner,
    };
    let create = command_audit(f);
    let cid = fresh(ResourceKind::Conversation);
    let CommandOutcome::Applied(conversation) = repo
        .create_conversation(
            create.clone(),
            cid.clone(),
            f.agent_resource_id.clone(),
            "Two rounds".into(),
        )
        .await
        .unwrap()
    else {
        panic!()
    };
    let CommandOutcome::Replayed(replay) = repo
        .create_conversation(
            create,
            fresh(ResourceKind::Conversation),
            f.agent_resource_id.clone(),
            "Two rounds".into(),
        )
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(replay.conversation_id, cid);
    let target = repo
        .resolve_root_run_target(&f.tenant_id, &f.agent_resource_id)
        .await
        .unwrap();
    let principal = PrincipalSnapshot::build(
        f.tenant_id.clone(),
        f.principal_id.clone(),
        PrincipalKind::AgentRunner,
        permissions,
        1,
        1,
        2,
    )
    .unwrap();
    let bindings = RunBindingsSnapshot::build_with_context_dataset_views(
        target.agent.clone(),
        principal,
        &target.closure,
        target.context_dataset_views,
    )
    .unwrap();
    let make = |text: &str| {
        let value = json!({conversation.input_field.clone():text});
        AdmitRun {
            audit: command_audit(f),
            expected_agent_deployment: Some(target.agent.clone()),
            admission_scope_id: f.agent_resource_id.clone(),
            run_id: fresh(ResourceKind::Run),
            agent_deployment_id: target.agent.deployment_id.clone(),
            root_scope_id: fresh(ResourceKind::ScopeInstance),
            entry_node_execution_id: fresh(ResourceKind::NodeExecution),
            orchestration_job_id: fresh(ResourceKind::Job),
            entry_plan_node_key: PlanNodeKey::new(target.closure.entry_node_id.clone()).unwrap(),
            entry_node_kind: target.closure.entry_node_kind,
            bindings: bindings.clone(),
            input: RunInputValue {
                value_id: fresh(ResourceKind::RunValue),
                classification: DataClassification::Internal,
                schema_digest: conversation.input_schema_digest.clone(),
                content_digest: insight_platform_contracts::canonical_digest(&value)
                    .unwrap()
                    .parse()
                    .unwrap(),
                value: ValueRef::Inline { value },
            },
            deadline: Utc::now() + Duration::minutes(5),
            inline_limits: JsonLimits::CONTRACT_FIXTURE,
            attempt_limit: 3,
            retry_backoff_milliseconds: 100,
        }
    };
    let first = make("Remember cobalt");
    let mut tx = repo.begin_run_transaction().await.unwrap();
    let CommandOutcome::Applied(turn) = tx
        .admit_conversation_turn(&cid, 1, first.clone())
        .await
        .unwrap()
    else {
        panic!()
    };
    tx.commit().await.unwrap();
    let mut tx = repo.begin_run_transaction().await.unwrap();
    let CommandOutcome::Replayed(replayed) = tx
        .admit_conversation_turn(&cid, 1, first.clone())
        .await
        .unwrap()
    else {
        panic!()
    };
    tx.commit().await.unwrap();
    assert_eq!(turn, replayed);
    let rejected = make("busy");
    let mut tx = repo.begin_run_transaction().await.unwrap();
    assert!(tx
        .admit_conversation_turn(&cid, 2, rejected.clone())
        .await
        .is_err());
    tx.commit().await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_id=$2",
    )
    .bind(f.tenant_id.to_string())
    .bind(rejected.audit.receipt_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(count, 0, "caught error cannot commit a Receipt");
    let wire = Arc::new(ConversationWire::default());
    let answer = provider_round(pool, repo, f, &first.run_id, wire.clone()).await;
    assert_eq!(answer, json!({"answer":"I remember cobalt"}));
    let output = fresh(ResourceKind::RunValue);
    sqlx::query("INSERT INTO insight_platform.run_values(tenant_id,value_id,run_id,value_kind,classification,schema_digest,content_digest,inline_value) VALUES($1,$2,$3,'run_output','internal',$4,$5,$6)").bind(f.tenant_id.to_string()).bind(output.to_string()).bind(first.run_id.to_string()).bind(f.output_schema.canonical_digest.to_string()).bind(insight_platform_contracts::canonical_digest(&answer).unwrap()).bind(answer).execute(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.runs SET state='succeeded',terminal_at=clock_timestamp(),output_value_id=$3 WHERE tenant_id=$1 AND run_id=$2").bind(f.tenant_id.to_string()).bind(first.run_id.to_string()).bind(output.to_string()).execute(pool).await.unwrap();
    let second = make("What color?");
    let third = make("Concurrent message");
    let submit = |command: AdmitRun| {
        let repo = repo.clone();
        let cid = cid.clone();
        async move {
            let mut tx = repo.begin_run_transaction().await.unwrap();
            let outcome = tx.admit_conversation_turn(&cid, 2, command).await;
            match tx.commit().await {
                Ok(()) => outcome,
                Err(error) => Err(error),
            }
        }
    };
    let (a, b) = tokio::join!(submit(second), submit(third));
    assert_ne!(a.is_ok(), b.is_ok());
    let applied = a.or(b).unwrap();
    let CommandOutcome::Applied(second_turn) = applied else {
        panic!()
    };
    let history = repo
        .load_conversation_history_for_run(&f.tenant_id, &second_turn.run_id)
        .await
        .unwrap();
    let second_answer = provider_round(pool, repo, f, &second_turn.run_id, wire.clone()).await;
    assert_eq!(second_answer, json!({"answer":"cobalt"}));
    assert_eq!(wire.requests.lock().unwrap().len(), 2);
    assert_eq!(history.len(), 2);
    assert!(history[0].user);
    assert!(!history[1].user);
    assert_eq!(
        history[1]
            .clone()
            .into_block(history[1].inline.clone().unwrap())
            .unwrap()
            .text,
        "I remember cobalt"
    );
    assert_eq!(
        repo.read_conversation(&scope, &cid)
            .await
            .unwrap()
            .turn_count,
        2
    );
    assert_eq!(
        repo.list_conversation_turns(&scope, &cid, Utc::now(), 0, 50)
            .await
            .unwrap()
            .len(),
        2
    );
    let held: serde_json::Value =
        sqlx::query_scalar("SELECT insight_platform.history_lock_run($1,$2)")
            .bind(f.tenant_id.to_string())
            .bind(first.run_id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
    assert!(held["hold_count"].as_i64().unwrap() > 0);
    let replay = repo
        .read_conversation_turn_replay(
            &scope,
            &cid,
            &first.audit.idempotency_key_digest,
            &first.audit.request_digest,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.run_id, first.run_id);
    // Changing active deployment does not rewrite a conversation or break an accepted Receipt replay.
    sqlx::query("UPDATE insight_platform.runs SET state='cancelled',terminal_at=clock_timestamp() WHERE tenant_id=$1 AND run_id=$2").bind(f.tenant_id.to_string()).bind(second_turn.run_id.to_string()).execute(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=NULL WHERE tenant_id=$1 AND resource_id=$2").bind(f.tenant_id.to_string()).bind(f.agent_resource_id.to_string()).execute(pool).await.unwrap();
    let mut tx = repo.begin_run_transaction().await.unwrap();
    assert!(tx
        .admit_conversation_turn(&cid, 3, make("new deployment required"))
        .await
        .is_err());
    tx.commit().await.unwrap();
    assert_eq!(
        repo.read_conversation_turn_replay(
            &scope,
            &cid,
            &first.audit.idempotency_key_digest,
            &first.audit.request_digest
        )
        .await
        .unwrap()
        .unwrap()
        .run_id,
        first.run_id
    );
    assert_eq!(
        repo.read_conversation(&scope, &cid)
            .await
            .unwrap()
            .turn_count,
        2
    );
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2").bind(f.tenant_id.to_string()).bind(f.agent_resource_id.to_string()).bind(target.agent.deployment_id.to_string()).execute(pool).await.unwrap();
}

#[derive(Default)]
struct ConversationWire {
    requests: std::sync::Mutex<Vec<serde_json::Value>>,
}
#[async_trait::async_trait]
impl ModelProviderWireConnector for ConversationWire {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
        assert_eq!(request.protocol, ModelProviderWireProtocol::OpenAiResponses);
        let mut requests = self.requests.lock().unwrap();
        let input = request.request_body["input"].as_array().unwrap();
        let dialogue: Vec<_> = input.iter().filter(|m| m["role"] != "developer").collect();
        let answer = if requests.is_empty() {
            assert_eq!(dialogue.len(), 1);
            assert_eq!(dialogue[0]["role"], "user");
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(
                    dialogue[0]["content"][0]["text"].as_str().unwrap()
                )
                .unwrap(),
                json!({"prompt":"Remember cobalt"})
            );
            "I remember cobalt"
        } else {
            assert_eq!(requests.len(), 1, "exactly two provider requests");
            assert_eq!(dialogue.len(), 3);
            assert_eq!(dialogue[0]["role"], "user");
            assert_eq!(dialogue[0]["content"][0]["type"], "input_text");
            assert_eq!(dialogue[0]["content"][0]["text"], "Remember cobalt");
            assert_eq!(dialogue[1]["role"], "assistant");
            assert_eq!(dialogue[1]["content"][0]["type"], "output_text");
            assert_eq!(dialogue[1]["content"][0]["text"], "I remember cobalt");
            assert_eq!(dialogue[2]["role"], "user");
            let current: serde_json::Value =
                serde_json::from_str(dialogue[2]["content"][0]["text"].as_str().unwrap()).unwrap();
            assert!(
                current == json!({"prompt":"What color?"})
                    || current == json!({"prompt":"Concurrent message"})
            );
            "cobalt"
        };
        requests.push(request.request_body.clone());
        Ok(Box::pin(futures::stream::iter([Ok(
            ModelProviderWireEvent {
                event_name: "response.completed".into(),
                data: json!({"type":"response.completed","response":{
                    "status":"completed","model":"fixture-model-2026-08",
                    "output":[{"id":"msg_conversation","type":"message","status":"completed","role":"assistant",
                    "content":[{"type":"output_text","text":json!({"answer":answer}).to_string()}]}],
                    "usage":{"input_tokens":50,"output_tokens":10,"total_tokens":60}
                }}),
            },
        )])))
    }
    async fn cancel(
        &self,
        _: ModelProviderWireProtocol,
        _: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Accepted)
    }
}

// Uses the production assembler and adapter; only the network connector is a mock.
// Successful RunValue persistence below remains an explicit terminal fixture, not a Worker test.
async fn provider_round(
    pool: &PgPool,
    repo: &PgRepository,
    f: &Fixture,
    run_id: &ResourceId,
    wire: Arc<ConversationWire>,
) -> serde_json::Value {
    use futures::StreamExt;
    use insight_platform_model_adapters::{ModelProviderAdapter, OpenAiResponsesAdapter};
    use insight_platform_models::execution::ModelAdapterExecutionRequest;
    use insight_platform_models::{
        assemble_prompt_messages, ModelQuotaCeiling, NormalizedModelDelta, PromptAssemblyBlock,
        PromptAssemblyPhase,
    };
    let facts = repo
        .load_exact_controller_model_assembly_facts(
            &f.tenant_id,
            run_id,
            "primary_model",
            &f.model_deployment,
            std::slice::from_ref(&f.tool_slot_binding),
        )
        .await
        .unwrap();
    let mut blocks = Vec::new();
    for (phase, text) in [
        (PromptAssemblyPhase::PlatformSafety, "Answer safely."),
        (PromptAssemblyPhase::AgentContract, "Answer as JSON."),
        (
            PromptAssemblyPhase::PlanNodeInstruction,
            "Use conversation context.",
        ),
    ] {
        blocks.push(PromptAssemblyBlock {
            phase,
            history_role: None,
            ordinal: 0,
            source_kind: "fixture_instruction".into(),
            source_id: format!("{phase:?}"),
            source_digest: digest('a'),
            classification: DataClassification::Internal,
            byte_budget: 1024,
            token_budget: 1024,
            text: text.into(),
        });
    }
    for value in repo
        .load_conversation_history_for_run(&f.tenant_id, run_id)
        .await
        .unwrap()
    {
        let inline = value
            .inline
            .clone()
            .expect("this fixture stores inline history");
        blocks.push(value.into_block(inline).unwrap());
    }
    let current: serde_json::Value = sqlx::query_scalar("SELECT v.inline_value FROM insight_platform.runs r JOIN insight_platform.run_values v ON v.tenant_id=r.tenant_id AND v.value_id=r.input_value_id WHERE r.tenant_id=$1 AND r.run_id=$2")
        .bind(f.tenant_id.to_string()).bind(run_id.to_string()).fetch_one(pool).await.unwrap();
    blocks.push(PromptAssemblyBlock {
        phase: PromptAssemblyPhase::UserInput,
        history_role: None,
        ordinal: 0,
        source_kind: "run_input".into(),
        source_id: run_id.to_string(),
        source_digest: canonical_digest(&current).unwrap().parse().unwrap(),
        classification: DataClassification::Internal,
        byte_budget: 16384,
        token_budget: 16384,
        text: current.to_string(),
    });
    let assembled = assemble_prompt_messages(blocks, 262144, 65536).unwrap();
    let mut canonical = command_for_node(f, &f.primary_node_id, 0xef00)
        .request
        .request;
    canonical.messages = assembled.messages;
    canonical.source_map_digest = assembled.source_map_digest;
    canonical.input_token_estimate = assembled.total_estimated_tokens;
    canonical.classification = assembled.classification;
    canonical.tools.clear();
    canonical.response_contract.allow_tool_intents = false;
    let descriptor = (&facts.provider.installed_adapter).into();
    let adapter = OpenAiResponsesAdapter::new(descriptor, wire).unwrap();
    let execution = ModelAdapterExecutionRequest {
        schema_version: 1,
        tenant_id: f.tenant_id.clone(),
        run_id: run_id.clone(),
        model_turn_id: canonical.model_turn_id.clone(),
        job_id: fresh(ResourceKind::Job),
        worker_process_generation_id: fresh(ResourceKind::WorkerProcessGeneration),
        worker_manifest_digest: facts
            .provider
            .installed_adapter
            .worker_manifest_digest
            .clone(),
        attempt_no: 1,
        attempt_limit: 1,
        lease_generation: 1,
        admission_digest: digest('a'),
        request_digest: canonical_digest(&serde_json::to_value(&canonical).unwrap())
            .unwrap()
            .parse()
            .unwrap(),
        quota_ceiling: ModelQuotaCeiling {
            concurrent_units: 1,
            requests: 1,
            tokens: 65536,
            cost_microunits: 10000,
        },
        model_deployment: f.model_deployment.clone(),
        model_closure: f.model_closure.clone(),
        profile_revision: f.profile_revision.clone(),
        provider_deployment: f.model_closure.provider_deployment.clone(),
        provider_closure: f.provider_closure.clone(),
        provider_revision: f.provider_revision.clone(),
        provider: facts.provider,
        profile: Box::new(facts.profile),
        request: Box::new(canonical),
    };
    let mut stream = adapter.invoke(execution).await.unwrap();
    let mut answer = None;
    while let Some(frame) = stream.next().await {
        if let NormalizedModelDelta::Terminal(response) = frame.unwrap().delta {
            answer = Some(response.structured_output.unwrap().value);
            break;
        }
    }
    answer.expect("provider returned validated structured output")
}
