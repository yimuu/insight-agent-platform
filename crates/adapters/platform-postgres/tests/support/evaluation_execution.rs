//! Actual generated evaluation Plan execution through the existing coordinator.
#[path = "fixture_directory.rs"]
mod fixture_directory;
use super::*;
use insight_platform_agent_compiler::{evaluation::*, *};
use insight_platform_contracts::{ClosedJsonValue, ClosedValueSchema};
use insight_platform_plan::{PortAssignment, TypedExpressionProgram, TypedInstruction};
use insight_platform_postgres::repository::RepositoryError;

fn body_artifact(value: &serde_json::Value) -> ArtifactRef {
    ArtifactRef::new(
        fresh_id(ResourceKind::Artifact),
        canonical_digest(value).unwrap().parse().unwrap(),
        canonical_json(value).unwrap().len() as u64,
        "application/json",
        DataClassification::Internal,
        None,
    )
    .unwrap()
}
fn compiler_profile(base: &RunBindingsSnapshot) -> AgentCompilerProfile {
    AgentCompilerProfile {
        default_deadline_seconds: 120,
        default_environment: "test".into(),
        policy_versions: vec![base.execution_profile.revision.clone()],
        deployment_policies: vec![base.execution_profile.clone()],
        execution_profile: base.execution_profile.clone(),
        model_loop: ModelLoopCompilerLimits {
            maximum_rounds: 1,
            maximum_capability_calls: 1,
            maximum_parallel_calls_per_round: 1,
            token_budget: 1000,
        },
    }
}
fn compile_source(
    name: &str,
    input: &ClosedJsonSchema,
    output: &ClosedJsonSchema,
    plan: RuntimePlan,
    profile: AgentCompilerProfile,
) -> AgentCompilationV1 {
    let manifest = json!({"apiVersion":"insight.platform/v1","kind":"Agent","metadata":{"name":name},"spec":{"execution":{"kind":"full_plan","plan":"plan.json"},"input":{"schema":"input.json","classification":"internal"},"output":{"schema":"output.json"}}});
    let bundle = AgentSourceBundleV1 {
        schema_version: 1,
        compiler_semantic_identity: compiler_semantic_identity(),
        compile_policy_inputs_digest: AgentSourceBundleV1::policy_digest(&profile),
        sources: AgentSourceFilesV1 {
            manifest_path: "agent.json".into(),
            files: BTreeMap::from([
                (
                    "agent.json".into(),
                    serde_json::to_string(&manifest).unwrap(),
                ),
                (
                    "input.json".into(),
                    serde_json::to_string(&input.schema).unwrap(),
                ),
                (
                    "output.json".into(),
                    serde_json::to_string(&output.schema).unwrap(),
                ),
                ("plan.json".into(), serde_json::to_string(&plan).unwrap()),
            ]),
        },
        profile,
        bindings: Default::default(),
    };
    let AgentCompileResponseV1::Compiled { compilation } = compile_source_bundle(bundle) else {
        panic!("actual fixture compilation rejected")
    };
    *compilation
}
struct InstalledAgent {
    exact: ExactDeploymentRef,
    closure: AgentDeploymentClosure,
    compilation: AgentCompilationV1,
}
async fn install_agent(
    repo: &PgRepository,
    compilation: AgentCompilationV1,
    plans: &mut BTreeMap<ResourceId, Vec<u8>>,
) -> InstalledAgent {
    let authority = |intent: &ArtifactIntent| ArtifactAuthority {
        purpose: intent.purpose,
        state: insight_platform_contracts::ArtifactState::Ready,
        artifact: ArtifactRef::new(
            fresh_id(ResourceKind::Artifact),
            intent.content_digest.clone(),
            intent.byte_length,
            intent.media_type.clone(),
            intent.classification,
            intent.display_name.clone(),
        )
        .unwrap(),
    };
    let source = authority(&compilation.compiled.resource_intent.authoring_artifact);
    let typed = authority(&compilation.compiled.resource_intent.typed_plan_artifact);
    for (a, purpose) in [(&source, "source_package"), (&typed, "typed_plan")] {
        insert_ready_artifact(
            repo.pool(),
            &id(TENANT_ID),
            &id(PRINCIPAL_ID),
            &id(POLICY_REVISION_ID),
            &a.artifact,
            purpose,
        )
        .await;
    }
    let document = compilation.materialize(&source, &typed).unwrap();
    let proof = validate_frozen_agent_artifacts(
        &document,
        &source,
        &compilation.source_bundle_bytes,
        &typed,
        &compilation.compiled.typed_plan_bytes,
    )
    .unwrap();
    let agent = fresh_id(ResourceKind::Agent);
    let interface = fresh_id(ResourceKind::AgentInterfaceRevision);
    let plan_id = fresh_id(ResourceKind::AgentPlanRevision);
    let deployment = fresh_id(ResourceKind::AgentDeployment);
    let published = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document,
            validation: ValidationSummary {
                program_requirement: Some(proof.program_requirement),
                validator_digest: fresh_digest(),
                validated_draft_digest: fresh_digest(),
                dependency_closure_digest: fresh_digest(),
                security_evidence_digest: fresh_digest(),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let empty = TypedPayload::empty(1).unwrap();
    sqlx::query("INSERT INTO insight_platform.resources(tenant_id,resource_id,resource_kind,lifecycle_state,gate_state,payload_schema_version,payload,payload_digest) VALUES($1,$2,'agent','active','enabled',$3,$4,$5)").bind(TENANT_ID).bind(agent.to_string()).bind(empty.schema_version).bind(empty.value).bind(empty.digest).execute(repo.pool()).await.unwrap();
    for (revision, kind, digest, artifact) in [
        (
            &interface,
            "agent_interface_revision",
            &compilation.compiled.resource_intent.contract_digest,
            None,
        ),
        (
            &plan_id,
            "agent_plan_revision",
            &compilation.compiled.typed_plan_digest,
            Some(typed.artifact.artifact_id().to_string()),
        ),
    ] {
        sqlx::query("INSERT INTO insight_platform.resource_versions(tenant_id,resource_version_id,resource_id,resource_version_kind,revision_no,content_digest,artifact_id,payload_schema_version,payload,payload_digest,created_by) VALUES($1,$2,$3,$4,1,$5,$6,$7,$8,$9,$10)").bind(TENANT_ID).bind(revision.to_string()).bind(agent.to_string()).bind(kind).bind(digest.to_string()).bind(artifact).bind(published.schema_version).bind(&published.value).bind(&published.digest).bind(PRINCIPAL_ID).execute(repo.pool()).await.unwrap();
    }
    let intent = &compilation.compiled.deployment_intent;
    let closure = AgentDeploymentClosure {
        interface: ExactVersionRef::new(
            interface,
            compilation.compiled.resource_intent.contract_digest.clone(),
        )
        .unwrap(),
        plan: ExactVersionRef::new(
            plan_id.clone(),
            compilation.compiled.typed_plan_digest.clone(),
        )
        .unwrap(),
        entry_node_id: intent.entry_node_id.clone(),
        entry_node_kind: intent.entry_node_kind,
        slots: intent
            .slots
            .iter()
            .cloned()
            .map(|slot| {
                slot.materialize(&deployment, &mut std::iter::empty())
                    .unwrap()
            })
            .collect(),
        policies: intent.policies.clone(),
        execution_profile: intent.execution_profile.clone(),
    };
    let payload = TypedPayload::new(1, &DeploymentClosure::Agent(closure.clone())).unwrap();
    sqlx::query("INSERT INTO insight_platform.deployments(tenant_id,deployment_id,resource_id,resource_version_id,environment,bindings_digest,payload_schema_version,bindings,created_by) VALUES($1,$2,$3,$4,'test',$5,$6,$7,$8)").bind(TENANT_ID).bind(deployment.to_string()).bind(agent.to_string()).bind(plan_id.to_string()).bind(&payload.digest).bind(payload.schema_version).bind(payload.value).bind(PRINCIPAL_ID).execute(repo.pool()).await.unwrap();
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2").bind(TENANT_ID).bind(agent.to_string()).bind(deployment.to_string()).execute(repo.pool()).await.unwrap();
    plans.insert(
        typed.artifact.artifact_id().clone(),
        compilation.compiled.typed_plan_bytes.clone(),
    );
    InstalledAgent {
        exact: ExactDeploymentRef::new(deployment, payload.digest.parse().unwrap()).unwrap(),
        closure,
        compilation,
    }
}
fn evaluator_plan(input: &ClosedJsonSchema, output: &ClosedJsonSchema) -> RuntimePlan {
    let entry = PlanNodeKey::new("metric".into()).unwrap();
    let finish = PlanNodeKey::new("finish".into()).unwrap();
    let port = ExactDataPortRef::NodeOutput {
        producer_node_id: entry.clone(),
        port_id: DataPortKey::new("score".into()).unwrap(),
        schema_digest: output.canonical_digest.clone(),
    };
    RuntimePlan {
        plan_version: 6,
        interface_contract_digest: agent_interface_contract_digest(input, output).unwrap(),
        entry_node_id: entry.clone(),
        dependency_slots: BTreeMap::new(),
        schema_documents: BTreeMap::from([
            (
                input.canonical_digest.clone(),
                ClosedValueSchema::try_from(input.clone()).unwrap(),
            ),
            (
                output.canonical_digest.clone(),
                ClosedValueSchema::try_from(output.clone()).unwrap(),
            ),
        ]),
        nodes: BTreeMap::from([
            (
                entry,
                RuntimeNode::Compute {
                    assignments: vec![PortAssignment {
                        output_port: port.clone(),
                        expression: TypedExpressionProgram::build(
                            vec![],
                            vec![TypedInstruction::Literal {
                                value: ClosedJsonValue::build(
                                    output.canonical_digest.clone(),
                                    json!({"score":1}),
                                )
                                .unwrap(),
                            }],
                            output.canonical_digest.clone(),
                            ExpressionLimits::ABSOLUTE,
                        )
                        .unwrap(),
                    }],
                    next: finish.clone(),
                },
            ),
            (finish, RuntimeNode::Return { value: port }),
        ]),
    }
}
fn evaluation_admission_command(
    base: &RunBindingsSnapshot,
    agent: &InstalledAgent,
    input: serde_json::Value,
) -> AdmitRun {
    let run = fresh_id(ResourceKind::Run);
    let bindings =
        RunBindingsSnapshot::build(agent.exact.clone(), base.principal.clone(), &agent.closure)
            .unwrap();
    AdmitRun {
        expected_agent_deployment: None,
        audit: fresh_audit(&id(TENANT_ID), &id(PRINCIPAL_ID)),
        admission_scope_id: agent.exact.deployment_id.clone(),
        run_id: run.clone(),
        agent_deployment_id: agent.exact.deployment_id.clone(),
        root_scope_id: fresh_id(ResourceKind::ScopeInstance),
        entry_node_execution_id: fresh_id(ResourceKind::NodeExecution),
        orchestration_job_id: fresh_id(ResourceKind::Job),
        entry_plan_node_key: PlanNodeKey::new(agent.closure.entry_node_id.clone()).unwrap(),
        entry_node_kind: agent.closure.entry_node_kind,
        bindings,
        input: RunInputValue {
            value_id: fresh_id(ResourceKind::RunValue),
            classification: DataClassification::Internal,
            schema_digest: agent
                .compilation
                .compiled
                .resource_intent
                .input_schema
                .canonical_digest
                .clone(),
            content_digest: canonical_digest(&input).unwrap().parse().unwrap(),
            value: ValueRef::Inline { value: input },
        },
        deadline: Utc::now() + ChronoDuration::seconds(90),
        inline_limits: JsonLimits::CONTRACT_FIXTURE,
        attempt_limit: 3,
        retry_backoff_milliseconds: 25,
    }
}
async fn admit_evaluation(
    repo: &PgRepository,
    base: &RunBindingsSnapshot,
    agent: &InstalledAgent,
    input: serde_json::Value,
) -> ResourceId {
    let command = evaluation_admission_command(base, agent, input);
    let run = command.run_id.clone();
    let mut tx = repo.begin_run_transaction().await.unwrap();
    tx.admit_run(command).await.unwrap();
    tx.commit().await.unwrap();
    run
}
async fn assert_active_condition_and_frozen_definition(
    repo: &PgRepository,
    base: &RunBindingsSnapshot,
    agent: &InstalledAgent,
) {
    let resource:String=sqlx::query_scalar("SELECT resource_id FROM insight_platform.deployments WHERE tenant_id=$1 AND deployment_id=$2").bind(TENANT_ID).bind(agent.exact.deployment_id.to_string()).fetch_one(repo.pool()).await.unwrap();
    // Head fixtures exercise admission, not the independently tested activation command.
    sqlx::query("UPDATE insight_platform.resources SET active_version_id=NULL,active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2").bind(TENANT_ID).bind(&resource).bind(agent.exact.deployment_id.to_string()).execute(repo.pool()).await.unwrap();
    let mut command = evaluation_admission_command(
        base,
        agent,
        json!({"samples":{"one":{"input":{"question":"exact active"}}}}),
    );
    command.admission_scope_id = resource.parse().unwrap();
    command.expected_agent_deployment = Some(agent.exact.clone());
    command.deadline = Utc::now() + ChronoDuration::milliseconds(500);
    command.audit.receipt_expires_at = Utc::now() + ChronoDuration::milliseconds(500);
    command.audit.request_digest = canonical_digest(
        &json!({"expected":command.expected_agent_deployment,"input":command.input.content_digest}),
    )
    .unwrap()
    .parse()
    .unwrap();
    let mut tx = repo.begin_run_transaction().await.unwrap();
    assert!(matches!(
        tx.admit_run(command.clone()).await.unwrap(),
        CommandOutcome::Applied(_)
    ));
    tx.commit().await.unwrap();
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=NULL WHERE tenant_id=$1 AND resource_id=$2").bind(TENANT_ID).bind(&resource).execute(repo.pool()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(550)).await;
    let mut tx = repo.begin_run_transaction().await.unwrap();
    let replay = tx.admit_run(command.clone()).await.unwrap();
    assert!(
        matches!(replay,CommandOutcome::Replayed(ref record) if record.run_id==command.run_id.to_string())
    );
    tx.commit().await.unwrap();
    let before:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM insight_platform.runs WHERE tenant_id=$1),(SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1)").bind(TENANT_ID).fetch_one(repo.pool()).await.unwrap();
    let mut fresh = command.clone();
    fresh.audit = fresh_audit(&id(TENANT_ID), &id(PRINCIPAL_ID));
    fresh.run_id = fresh_id(ResourceKind::Run);
    fresh.deadline = Utc::now() + ChronoDuration::seconds(10);
    let mut tx = repo.begin_run_transaction().await.unwrap();
    assert!(matches!(
        tx.admit_run(fresh).await,
        Err(RepositoryError::Conflict(_))
    ));
    drop(tx);
    let mut implicit = command.clone();
    implicit.expected_agent_deployment = None;
    implicit.audit = fresh_audit(&id(TENANT_ID), &id(PRINCIPAL_ID));
    implicit.run_id = fresh_id(ResourceKind::Run);
    implicit.deadline = Utc::now() + ChronoDuration::seconds(10);
    let mut tx = repo.begin_run_transaction().await.unwrap();
    assert!(
        matches!(
            tx.admit_run(implicit).await,
            Err(RepositoryError::Conflict(_))
        ),
        "submit-time active mode cannot admit a stale resolved closure"
    );
    drop(tx);
    let mut expired = command.clone();
    expired.audit = fresh_audit(&id(TENANT_ID), &id(PRINCIPAL_ID));
    expired.run_id = fresh_id(ResourceKind::Run);
    let mut tx = repo.begin_run_transaction().await.unwrap();
    assert!(matches!(
        tx.admit_run(expired).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    drop(tx);
    // expires_at is the retention lower bound; a retained Receipt continues replaying.
    let retained_expired:bool=sqlx::query_scalar("SELECT expires_at<clock_timestamp() AND expires_at>created_at FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_id=$2").bind(TENANT_ID).bind(command.audit.receipt_id.to_string()).fetch_one(repo.pool()).await.unwrap();
    assert!(retained_expired);
    let mut tx = repo.begin_run_transaction().await.unwrap();
    assert!(matches!(
        tx.admit_run(command.clone()).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));
    tx.commit().await.unwrap();
    let mut different = command.clone();
    different.expected_agent_deployment = None;
    different.audit.request_digest = fresh_digest();
    let mut tx = repo.begin_run_transaction().await.unwrap();
    assert!(matches!(
        tx.admit_run(different).await,
        Err(RepositoryError::IdempotencyConflict)
    ));
    drop(tx);
    let after:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM insight_platform.runs WHERE tenant_id=$1),(SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1)").bind(TENANT_ID).fetch_one(repo.pool()).await.unwrap();
    assert_eq!(
        before, after,
        "head or Receipt conflict cannot leave Run/Receipt writes"
    );
    assert!(matches!(
        repo.read_run_definition_for_principal(
            &id(TENANT_ID),
            &id(PRINCIPAL_ID),
            PrincipalKind::AgentRunner,
            &command.run_id
        )
        .await,
        Err(RepositoryError::PermissionDenied)
    ));
    let reader = fresh_id(ResourceKind::Principal);
    repo.create_principal(NewPrincipal {
        principal_id: reader.clone(),
        authentication_authority_digest: fresh_digest(),
        subject_digest: fresh_digest(),
        installation_bindings: PrincipalBindingsPayload {
            installation_bindings: vec![],
        },
    })
    .await
    .unwrap();
    repo.bind_tenant_principal(NewTenantPrincipal {
        tenant_id: id(TENANT_ID),
        principal_id: reader.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        payload: TenantPrincipalPayload {
            permissions: PermissionSet::new(vec![Permission::RuntimeRead]).unwrap(),
        },
    })
    .await
    .unwrap();
    let definition = repo
        .read_run_definition_for_principal(
            &id(TENANT_ID),
            &reader,
            PrincipalKind::AgentRunner,
            &command.run_id,
        )
        .await
        .unwrap();
    assert_eq!(definition.agent_deployment, agent.exact);
    assert_eq!(definition.plan, agent.closure.plan);
    assert_eq!(definition.agent_id.to_string(), resource);
}

#[test]
#[ignore = "subprocess entry selected by the evaluation parent test"]
fn evaluation_worker_process_entry() {
    assert_eq!(
        std::env::var("PLATFORM_EVALUATION_WORKER").as_deref(),
        Ok("1"),
        "only the parent evaluation test launches this subprocess"
    );
    let url =
        std::env::var("PLATFORM_TEST_EVALUATION_DATABASE_URL").expect("dedicated evaluation DB");
    let plans: BTreeMap<ResourceId, Vec<u8>> = serde_json::from_slice(
        &std::fs::read(std::env::var("PLATFORM_EVALUATION_PLANS_FILE").unwrap()).unwrap(),
    )
    .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let mut profile = checked_in_hard_limit_profile();
        profile.run_scheduler.lease_milliseconds.q1_default = 2000;
        profile.run_scheduler.heartbeat_milliseconds.q1_default = 100;
        profile.validate().unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .unwrap();
        let repo = PgRepository::with_hard_limit_profile(pool, &profile).unwrap();
        let materializer = Arc::new(
            SchedulerPlanMaterializer::new(
                Arc::new(repo.clone()),
                Arc::new(StaticTypedPlanReader { plans }),
                SchedulerPlanMaterializerConfig {
                    request_timeout: Duration::from_secs(1),
                    maximum_bytes: 1048576,
                    json_limits: JsonLimits::CONTRACT_FIXTURE,
                    plan_limits: PlanLimits::from_profile(&profile).unwrap(),
                },
            )
            .unwrap(),
        );
        let values = Arc::new(
            SchedulerControllerRunValueMaterializer::new(
                Arc::new(RecordingRunValueResolver {
                    repository: repo.clone(),
                    calls: Arc::new(AtomicU64::new(0)),
                    last_error: Arc::new(Mutex::new(None)),
                    last_request: Arc::new(Mutex::new(None)),
                }),
                Arc::new(StaticRunValueReader {
                    bytes: vec![],
                    calls: Arc::new(AtomicU64::new(0)),
                }),
                Duration::from_secs(1),
            )
            .unwrap(),
        );
        let durable = Arc::new(
            PostgresDurablePlanGenerationStore::new(
                repo.clone(),
                values,
                Arc::new(UuidCoordinatorIdentityFactory),
                Arc::new(EmptyCapabilityAdmissionProvider),
                Arc::new(EmptyModelAdmissionProvider),
                Duration::from_millis(25),
            )
            .unwrap(),
        );
        let handler = Arc::new(MaterializingOrchestrationJobHandler::new(
            materializer,
            Arc::new(ExactPlanGenerationDriver::new(durable, ExpressionLimits::ABSOLUTE).unwrap()),
        ));
        let executor = Arc::new(
            LeaseFencedOrchestrationExecutor::new(
                Arc::new(repo.clone()),
                handler,
                Arc::new(UuidCoordinatorIdentityFactory),
                OrchestrationExecutorConfig::from_profile(
                    &profile,
                    OrchestrationExecutorTiming {
                        heartbeat_jitter: Duration::ZERO,
                        store_retry_backoff: Duration::from_millis(10),
                    },
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let executable = std::fs::read(std::env::current_exe().unwrap()).unwrap();
        use sha2::{Digest as _, Sha256};
        let build: Sha256Digest = format!(
            "sha256:{}",
            Sha256::digest(executable)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        )
        .parse()
        .unwrap();
        let pools = LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: insight_platform_contracts::WORKER_MANIFEST_VERSION,
                worker_role: "orchestration-evaluation-fixture".into(),
                work_class: WorkClass::Orchestration,
                adapter_runtime_digest: build.clone(),
                worker_build_digest: build,
                execution_capabilities:
                    insight_platform_plan::execution::program_execution_capabilities(),
                protocol_version: 1,
                max_concurrency: 4,
                critical_control_reserved_slots: 1,
            },
            fresh_id(ResourceKind::WorkerProcessGeneration),
        )
        .unwrap();
        let coordinator = WorkCoordinator::new(
            Arc::new(repo.clone()),
            executor,
            Arc::new(UuidCoordinatorIdentityFactory),
            pools.clone(),
            OrchestrationCoordinatorConfig::from_profile(
                &profile,
                CoordinatorTiming {
                    coalesce_window: Duration::from_millis(1),
                    safety_scan_interval: Duration::from_millis(2),
                    safety_scan_jitter: Duration::ZERO,
                    claim_failure_backoff: Duration::from_millis(2),
                    drain_grace: Duration::from_secs(1),
                },
            )
            .unwrap(),
        )
        .unwrap()
        .spawn();
        let safety = OrchestrationSafetyDriver::new(
            Arc::new(repo),
            Arc::new(UuidCoordinatorIdentityFactory),
            pools,
            OrchestrationSafetyConfig::from_profile(
                &profile,
                SafetyScanShard::whole(),
                SafetyDriverTiming {
                    scan_interval: Duration::from_millis(20),
                    scan_jitter: Duration::ZERO,
                    failure_backoff: Duration::from_millis(10),
                },
            )
            .unwrap(),
        )
        .unwrap()
        .spawn();
        tokio::time::sleep(Duration::from_secs(120)).await;
        coordinator.shutdown().await.unwrap();
        safety.shutdown().await.unwrap();
    });
}
struct TestWorker(std::process::Child);
impl Drop for TestWorker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn spawn_worker(url: &str, plans: &std::path::Path) -> TestWorker {
    TestWorker(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "--ignored",
                "evaluation_execution::evaluation_worker_process_entry",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("PLATFORM_EVALUATION_WORKER", "1")
            .env("PLATFORM_TEST_EVALUATION_DATABASE_URL", url)
            .env("PLATFORM_EVALUATION_PLANS_FILE", plans)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}
async fn wait_for(
    repo: &PgRepository,
    run: &ResourceId,
    worker: &mut TestWorker,
    terminal: bool,
) -> String {
    let until = Instant::now() + Duration::from_secs(45);
    loop {
        assert!(
            worker.0.try_wait().unwrap().is_none(),
            "evaluation worker exited early"
        );
        let state: String = sqlx::query_scalar(
            "SELECT state FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
        )
        .bind(TENANT_ID)
        .bind(run.to_string())
        .fetch_one(repo.pool())
        .await
        .unwrap();
        let done = if terminal {
            matches!(
                state.as_str(),
                "succeeded" | "failed" | "cancelled" | "timed_out"
            )
        } else {
            sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM insight_platform.run_nodes n JOIN insight_platform.runs r ON r.tenant_id=n.tenant_id AND r.run_id=n.run_id WHERE r.tenant_id=$1 AND r.parent_run_id=$2 AND n.node_kind='timer_wait' AND n.state='waiting')").bind(TENANT_ID).bind(run.to_string()).fetch_one(repo.pool()).await.unwrap()
        };
        if done {
            return state;
        }
        if Instant::now() > until
            || (!terminal && matches!(state.as_str(), "failed" | "timed_out" | "cancelled"))
        {
            let states: Vec<(String, String, i32)> = sqlx::query_as(
                "SELECT job_id,state,attempt_no FROM insight_platform.jobs WHERE tenant_id=$1",
            )
            .bind(TENANT_ID)
            .fetch_all(repo.pool())
            .await
            .unwrap();
            panic!("evaluation run timeout {run} state={state} jobs={states:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
#[test]
fn generated_evaluation_survives_process_restart_and_preserves_cancel_and_budget_outcomes() {
    let url=std::env::var("PLATFORM_TEST_EVALUATION_DATABASE_URL").expect("PLATFORM_TEST_EVALUATION_DATABASE_URL must point to an exclusively provisioned fresh current-schema PostgreSQL database");
    tokio::runtime::Runtime::new().unwrap().block_on(async{
        let pool=PgPoolOptions::new().max_connections(8).connect(&url).await.unwrap();verify_schema(&pool).await.unwrap();let repo=PgRepository::new(pool);
        let base=seed_authorities(&repo).await;let profile=compiler_profile(&base);let input_schema=agent_schema();let mut plans=BTreeMap::new();
        let mut subject_plan=child_fixture_plan();subject_plan.interface_contract_digest=agent_interface_contract_digest(&input_schema,&input_schema).unwrap();
        let subject=install_agent(&repo,compile_source("evaluation-subject",&input_schema,&input_schema,subject_plan,profile.clone()),&mut plans).await;
        let selection_payload:serde_json::Value=sqlx::query_scalar("SELECT bindings FROM insight_platform.deployments WHERE tenant_id=$1 AND deployment_id=$2").bind(TENANT_ID).bind(SELECTION_POLICY_DEPLOYMENT_ID).fetch_one(repo.pool()).await.unwrap();
        let mut selection_document=selection_payload.clone();selection_document.as_object_mut().unwrap().remove("schema_version");
        let DeploymentClosure::Policy(selection_closure)=serde_json::from_value(selection_document).unwrap()else{panic!("selection owner")};
        let selection=ExactPolicyBinding{deployment:ExactDeploymentRef::new(id(SELECTION_POLICY_DEPLOYMENT_ID),canonical_digest(&selection_payload).unwrap().parse().unwrap()).unwrap(),revision:selection_closure.policy_revision};
        let sample=json!({"question":"evaluate exactly this input"});let sample_ref=body_artifact(&sample);
        insert_ready_artifact(repo.pool(),&id(TENANT_ID),&id(PRINCIPAL_ID),&id(POLICY_REVISION_ID),&sample_ref,"run_input").await;
        let metric=ClosedJsonSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":{"score":{"type":"integer","minimum":0,"maximum":1}},"required":["score"]})).unwrap();
        let manifest=EvaluationManifestV1{schema_version:1,dataset_id:"real-pg-evaluation".into(),samples:vec![EvaluationSampleV1{sample_id:"one".into(),input:sample_ref,input_schema_digest:input_schema.canonical_digest.clone(),expected:None,expected_schema_digest:None}],repetitions:2,subject:subject.exact.clone(),evaluator:ExactDeploymentRef::new(fresh_id(ResourceKind::AgentDeployment),fresh_digest()).unwrap(),metric_schema:metric.clone()};
        let mut request=EvaluationPlanRequestV1{deployment_features:Vec::new(),schema_version:1,name:"evaluation-parent".into(),display_name:"Actual PostgreSQL evaluation".into(),manifest_artifact:body_artifact(&serde_json::to_value(&manifest).unwrap()),manifest,subject_input_schema:input_schema.clone(),subject_output_schema:input_schema.clone(),expected_schema:None,subject_selection_policy:selection.clone(),evaluator_selection_policy:selection,child_budget:ChildBudgetLimit{maximum_duration_milliseconds:30000,maximum_model_tokens:1000,maximum_capability_calls:4,maximum_artifact_bytes:65536,maximum_descendant_runs:1},profile:profile.clone()};
        let evaluator_schema=evaluation_evaluator_input_schema(&input_schema,&input_schema,None).unwrap();
        let evaluator=install_agent(&repo,compile_source("evaluation-metrics",&evaluator_schema,&metric,evaluator_plan(&evaluator_schema,&metric),profile),&mut plans).await;
        request.manifest.evaluator=evaluator.exact;request.manifest_artifact=body_artifact(&serde_json::to_value(&request.manifest).unwrap());
        // This fixture obtains features from real immutable Registry rows using current query permissions.
        let reader=fresh_id(ResourceKind::Principal);
        repo.create_principal(NewPrincipal{principal_id:reader.clone(),authentication_authority_digest:fresh_digest(),subject_digest:fresh_digest(),installation_bindings:PrincipalBindingsPayload{installation_bindings:vec![]}}).await.unwrap();
        repo.bind_tenant_principal(NewTenantPrincipal{tenant_id:id(TENANT_ID),principal_id:reader.clone(),principal_kind:PrincipalKind::AgentRunner,payload:TenantPrincipalPayload{permissions:PermissionSet::new(vec![Permission::AgentRead,Permission::PolicyRead]).unwrap()}}).await.unwrap();
        let feature_query=insight_platform_registry::authoring::exact_feature_request(&evaluation_dependency_bindings(&request).unwrap()).unwrap().unwrap();
        let actual_features=repo.resolve_agent_authoring_bindings(&id(TENANT_ID),&reader,PrincipalKind::AgentRunner,&feature_query).await.unwrap();
        request.deployment_features=insight_platform_registry::authoring::resolved_feature_evidence(&actual_features,&feature_query).unwrap();
        let generated=compile_evaluation_plan(request.clone()).unwrap();assert_eq!(generated.evaluator_input_schema,evaluator_schema);
        insert_ready_artifact(repo.pool(),&id(TENANT_ID),&id(PRINCIPAL_ID),&id(POLICY_REVISION_ID),&request.manifest_artifact,"export").await;
        let AgentCompileResponseV1::Compiled{compilation}=compile_source_bundle(generated.source_bundle)else{panic!("generated parent rejected")};
        let parent=install_agent(&repo,*compilation,&mut plans).await;
        let input=json!({"samples":{"one":{"input":sample}}});
        let first=admit_evaluation(&repo,&base,&parent,input.clone()).await;
        let _files = fixture_directory::FixtureDirectory::new("evaluation");
        let plans_file = _files.path().join("plans.json");std::fs::write(&plans_file,serde_json::to_vec(&plans).unwrap()).unwrap();
        let mut worker=spawn_worker(&url,&plans_file);wait_for(&repo,&first,&mut worker,false).await;
        let completed_controllers: Vec<i32> = sqlx::query_scalar("SELECT attempt_no FROM insight_platform.jobs WHERE tenant_id=$1 AND run_id=$2 AND state='succeeded'").bind(TENANT_ID).bind(first.to_string()).fetch_all(repo.pool()).await.unwrap();
        assert!(!completed_controllers.is_empty(), "the parent has claimed its first controller");
        assert!(completed_controllers.iter().all(|attempt| *attempt == 1), "transaction retries do not spend business attempts");
        let failed_jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.jobs WHERE tenant_id=$1 AND state='failed'").bind(TENANT_ID).fetch_one(repo.pool()).await.unwrap();
        assert_eq!(failed_jobs, 0, "the initial concurrent controllers do not fail");
        let before:Vec<String>=sqlx::query_scalar("SELECT run_id FROM insight_platform.runs WHERE tenant_id=$1 AND parent_run_id=$2 ORDER BY run_id").bind(TENANT_ID).bind(first.to_string()).fetch_all(repo.pool()).await.unwrap();assert!(!before.is_empty());drop(worker);
        let mut worker=spawn_worker(&url,&plans_file);assert_eq!(wait_for(&repo,&first,&mut worker,true).await,"succeeded");
        let after:Vec<String>=sqlx::query_scalar("SELECT run_id FROM insight_platform.runs WHERE tenant_id=$1 AND parent_run_id=$2 ORDER BY run_id").bind(TENANT_ID).bind(first.to_string()).fetch_all(repo.pool()).await.unwrap();assert_eq!(after.len(),4);assert!(before.iter().all(|id|after.contains(id)));
        let links:Vec<(String,String,String)>=sqlx::query_as("SELECT n.plan_node_key,r.state,r.run_id FROM insight_platform.runs r JOIN insight_platform.run_nodes n ON n.tenant_id=r.tenant_id AND n.node_id=r.parent_node_id WHERE r.tenant_id=$1 AND r.parent_run_id=$2").bind(TENANT_ID).bind(first.to_string()).fetch_all(repo.pool()).await.unwrap();let keys=links.iter().map(|v|v.0.clone()).collect::<BTreeSet<_>>();assert_eq!(keys.len(),4);for trial in request.manifest.trials().unwrap(){assert!(keys.contains(trial_subject_node(&trial).as_str()));assert!(keys.contains(trial_evaluator_node(&trial).as_str()));}assert!(links.iter().all(|(_,state,_)|state=="succeeded"));
        assert_public_evaluation_reads(&repo,&first).await;
        let cancelled=admit_evaluation(&repo,&base,&parent,input.clone()).await;wait_for(&repo,&cancelled,&mut worker,false).await;
        let (version,generation):(i64,i64)=sqlx::query_as("SELECT version,cancel_generation FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2").bind(TENANT_ID).bind(cancelled.to_string()).fetch_one(repo.pool()).await.unwrap();let mut tx=repo.begin_run_transaction().await.unwrap();tx.request_run_cancel(insight_platform_orchestrator::RequestRunCancel{audit:fresh_audit(&id(TENANT_ID),&id(PRINCIPAL_ID)),run_id:cancelled.clone(),expected_run_version:version,expected_cancel_generation:generation as u64,reason_code:"evaluation_fixture_cancel".into()}).await.unwrap();tx.commit().await.unwrap();assert_eq!(wait_for(&repo,&cancelled,&mut worker,true).await,"cancelled");
        drop(worker);
        request.name="evaluation-budget-parent".into();request.child_budget.maximum_duration_milliseconds=100;
        let limited=compile_evaluation_plan(request.clone()).unwrap();let AgentCompileResponseV1::Compiled{compilation}=compile_source_bundle(limited.source_bundle)else{panic!("budget parent rejected")};let limited_parent=install_agent(&repo,*compilation,&mut plans).await;std::fs::write(&plans_file,serde_json::to_vec(&plans).unwrap()).unwrap();
        let limited_run=admit_evaluation(&repo,&base,&limited_parent,input).await;let mut worker=spawn_worker(&url,&plans_file);assert_eq!(wait_for(&repo,&limited_run,&mut worker,true).await,"succeeded");
        let outcomes:Vec<(String,String)>=sqlx::query_as("SELECT n.plan_node_key,r.state FROM insight_platform.runs r JOIN insight_platform.run_nodes n ON n.tenant_id=r.tenant_id AND n.node_id=r.parent_node_id WHERE r.tenant_id=$1 AND r.parent_run_id=$2").bind(TENANT_ID).bind(limited_run.to_string()).fetch_all(repo.pool()).await.unwrap();assert_eq!(outcomes.len(),2);assert!(outcomes.iter().all(|(key,state)|key.starts_with("subject-")&&state=="timed_out"));
        drop(worker);assert_active_condition_and_frozen_definition(&repo,&base,&parent).await;let _=std::fs::remove_file(plans_file);
    });
}
async fn assert_public_evaluation_reads(repo: &PgRepository, parent: &ResourceId) {
    use insight_platform_orchestrator::store::{ChildRunLinksQuery, RunValuesQuery};
    let principal = fresh_id(ResourceKind::Principal);
    repo.create_principal(NewPrincipal {
        principal_id: principal.clone(),
        authentication_authority_digest: fresh_digest(),
        subject_digest: fresh_digest(),
        installation_bindings: PrincipalBindingsPayload {
            installation_bindings: vec![],
        },
    })
    .await
    .unwrap();
    repo.bind_tenant_principal(NewTenantPrincipal {
        tenant_id: id(TENANT_ID),
        principal_id: principal.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        payload: TenantPrincipalPayload {
            permissions: PermissionSet::new(vec![
                Permission::RuntimeRead,
                Permission::ArtifactRead,
            ])
            .unwrap(),
        },
    })
    .await
    .unwrap();
    let query = ChildRunLinksQuery {
        tenant_id: id(TENANT_ID),
        principal_id: principal.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        parent_run_id: parent.clone(),
        parent_node_id: None,
        page_size: 1,
        snapshot_at: None,
        boundary: None,
    };
    let mut next = query.clone();
    let mut seen = BTreeSet::new();
    loop {
        let page = repo
            .list_child_runs_for_principal(next.clone())
            .await
            .unwrap();
        assert_eq!(page.items.len(), 1);
        for item in page.items {
            item.validate_for(parent, None).unwrap();
            assert!(seen.insert(item.child_run_id));
        }
        if let Some(boundary) = page.next_boundary {
            next.boundary = Some(boundary);
            next.snapshot_at = Some(page.snapshot_at)
        } else {
            break;
        }
    }
    assert_eq!(seen.len(), 4);
    let values = repo
        .list_run_values_for_principal(RunValuesQuery {
            tenant_id: id(TENANT_ID),
            principal_id: principal.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            run_id: parent.clone(),
            node_id: None,
            page_size: 50,
            snapshot_at: None,
            boundary: None,
        })
        .await
        .unwrap();
    assert!(!values.items.is_empty());
    let result = repo
        .read_run_result_for_principal(
            &id(TENANT_ID),
            &principal,
            PrincipalKind::AgentRunner,
            parent,
        )
        .await
        .unwrap();
    let value: ResourceId = result.value_id.clone();
    assert_eq!(
        repo.read_run_value_content_for_principal(
            &id(TENANT_ID),
            &principal,
            PrincipalKind::AgentRunner,
            parent,
            &value
        )
        .await
        .unwrap(),
        result
    );
    // Revoke only disclosure. Safe metadata remains available and both body routes reject.
    let payload = TypedPayload::new(
        1,
        &TenantPrincipalPayload {
            permissions: PermissionSet::new(vec![Permission::RuntimeRead]).unwrap(),
        },
    )
    .unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4,version=version+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND principal_id=$2").bind(TENANT_ID).bind(principal.to_string()).bind(payload.value).bind(payload.digest).execute(repo.pool()).await.unwrap();
    assert!(repo
        .list_child_runs_for_principal(query.clone())
        .await
        .is_ok());
    assert!(repo
        .read_run_value_metadata_for_principal(
            &id(TENANT_ID),
            &principal,
            PrincipalKind::AgentRunner,
            parent,
            &value
        )
        .await
        .is_ok());
    assert!(matches!(
        repo.read_run_result_for_principal(
            &id(TENANT_ID),
            &principal,
            PrincipalKind::AgentRunner,
            parent
        )
        .await,
        Err(insight_platform_postgres::repository::RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repo.read_run_value_content_for_principal(
            &id(TENANT_ID),
            &principal,
            PrincipalKind::AgentRunner,
            parent,
            &value
        )
        .await,
        Err(insight_platform_postgres::repository::RepositoryError::PermissionDenied)
    ));
    sqlx::query("UPDATE insight_platform.tenant_principals SET state='revoked',version=version+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND principal_id=$2").bind(TENANT_ID).bind(principal.to_string()).execute(repo.pool()).await.unwrap();
    assert!(matches!(
        repo.list_child_runs_for_principal(query).await,
        Err(insight_platform_postgres::repository::RepositoryError::PermissionDenied)
    ));
}
