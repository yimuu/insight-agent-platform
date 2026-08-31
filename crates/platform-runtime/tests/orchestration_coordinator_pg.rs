use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_artifacts::{
    ArtifactObjectReadAuthorityError, SchedulerRunValueLease, SchedulerRunValueReadError,
    SchedulerRunValueReadRequest, SchedulerRunValueReader, SchedulerRunValueRequestResolver,
    SchedulerTypedPlanReadError, SchedulerTypedPlanReadRequest, SchedulerTypedPlanReader,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, checked_in_hard_limit_profile, AgentDeploymentClosure,
    AgentResourceSpec, ArtifactRef, AuthoringPackage, CandidateSelectionMode,
    CandidateSelectionPolicyDocument, ClosedJsonSchema, CommandAudit, CommandOutcome,
    DataClassification, DeploymentClosure, ExactDeploymentRef, ExactPolicyBinding, ExactVersionRef,
    FrozenSlotBinding, FrozenSlotTarget, InteractionKind, JsonLimits, Permission, PermissionSet,
    PlanNodeKind, PolicyDeploymentClosure, PolicyKind, PolicyResourceSpec,
    PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot, PublishedVersionPayload,
    ResourceDocument, ResourceId, ResourceKind, RunBindingsSnapshot, SchedulingPolicyDocument,
    Sha256Digest, TenantConfig, TenantPrincipalPayload, ValidationSummary, ValueRef, WorkClass,
    WorkerManifest,
};
use insight_platform_invocations::InvocationPolicyDecisionBundle;
use insight_platform_jobs::WakeSource;
use insight_platform_orchestrator::{
    AdmitRun, ChildBudgetLimit, ChildCancellationPolicy, DataPortKey, ExactDataPortRef,
    ExpressionLimits, HumanTaskDefinition, PlanLimits, PlanNodeKey, RunInputValue,
    RuntimeDependencyKind, RuntimeDependencySlot, RuntimeNode, RuntimePlan,
};
use insight_platform_postgres::{
    repository::{
        NewPrincipal, NewQuotaAccount, NewTenant, NewTenantPrincipal, OrchestrationSignalAuthority,
        OrchestrationWakeMutationIds, OrchestrationYield, OrchestrationYieldMutationIds,
        PgRepository, ResolveOrchestrationSignalTarget, ResolveOrchestrationTask,
        ResolveOrchestrationTaskMutationIds, RunRecord, SafetyScanShard, TypedPayload,
        WakeOrchestrationJob, YieldOrchestrationJob, MAX_ORCHESTRATION_QUOTA_LINES,
    },
    verify_schema,
};
use insight_platform_runtime::postgres::{
    PostgresConnectionBulkheadConfig, PostgresConnectionBulkheads,
};
use insight_platform_runtime::{
    ActiveOrchestrationJob, ControllerCapabilityAdmissionDecision,
    ControllerCapabilityAdmissionProvider, ControllerCapabilityAdmissionRequest,
    ControllerModelAdmissionDecision, ControllerModelAdmissionProvider,
    ControllerModelAdmissionRequest, CoordinatorIdentityFactory, CoordinatorTiming,
    DurablePlanDriverError, ExactPlanGenerationDriver, ExecutionDisposition,
    GenerationHandlerDisposition, GenerationHandlerError, GenerationHandoffReason,
    LeaseFencedOrchestrationExecutor, MaterializingOrchestrationJobHandler,
    OrchestrationCoordinatorConfig, OrchestrationExecutorConfig, OrchestrationExecutorTiming,
    OrchestrationJobExecutor, OrchestrationSafetyConfig, OrchestrationSafetyDriver,
    PostgresDurablePlanGenerationStore, SafetyDriverTiming,
    SchedulerControllerRunValueMaterializer, SchedulerPlanMaterializer,
    SchedulerPlanMaterializerConfig, StartedOrchestrationJob, StartedOrchestrationJobHandler,
    UuidCoordinatorIdentityFactory, WorkCoordinator,
};
use insight_platform_security::BindTenantSchedulingPolicy;
use insight_platform_tasks::TaskState;
use insight_platform_worker::{ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPools};
use serde_json::json;
use sqlx::{pool::PoolConnection, postgres::PgPoolOptions, Postgres, Row};
use std::{
    collections::{BTreeMap, BTreeSet},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

const TENANT_ID: &str = "ten_0198f1c5-0787-75e1-a9e8-d95ca0f36001";
const PRINCIPAL_ID: &str = "prn_0198f1c5-0787-75e1-a9e8-d95ca0f36002";
const POLICY_ID: &str = "pol_0198f1c5-0787-75e1-a9e8-d95ca0f36003";
const POLICY_REVISION_ID: &str = "prev_0198f1c5-0787-75e1-a9e8-d95ca0f36004";
const POLICY_DEPLOYMENT_ID: &str = "pdep_0198f1c5-0787-75e1-a9e8-d95ca0f36011";
const AGENT_ID: &str = "agt_0198f1c5-0787-75e1-a9e8-d95ca0f36005";
const INTERFACE_ID: &str = "aif_0198f1c5-0787-75e1-a9e8-d95ca0f36006";
const PLAN_ID: &str = "arev_0198f1c5-0787-75e1-a9e8-d95ca0f36007";
const DEPLOYMENT_ID: &str = "adep_0198f1c5-0787-75e1-a9e8-d95ca0f36008";
const WORKER_ID: &str = "wrk_0198f1c5-0787-75e1-a9e8-d95ca0f36009";
const QUOTA_ACCOUNT_ID: &str = "qac_0198f1c5-0787-75e1-a9e8-d95ca0f3600a";
const RUN_ID: &str = "run_0198f1c5-0787-75e1-a9e8-d95ca0f3600b";
const SCOPE_ID: &str = "scp_0198f1c5-0787-75e1-a9e8-d95ca0f3600c";
const NODE_ID: &str = "nod_0198f1c5-0787-75e1-a9e8-d95ca0f3600d";
const JOB_ID: &str = "job_0198f1c5-0787-75e1-a9e8-d95ca0f3600e";
const VALUE_ID: &str = "val_0198f1c5-0787-75e1-a9e8-d95ca0f3600f";
const TYPED_PLAN_ARTIFACT_ID: &str = "art_0198f1c5-0787-75e1-a9e8-d95ca0f36012";
const INPUT_ARTIFACT_ID: &str = "art_0198f1c5-0787-75e1-a9e8-d95ca0f36013";
const CHILD_AGENT_ID: &str = "agt_0198f1c5-0787-75e1-a9e8-d95ca0f36014";
const CHILD_INTERFACE_ID: &str = "aif_0198f1c5-0787-75e1-a9e8-d95ca0f36015";
const CHILD_PLAN_ID: &str = "arev_0198f1c5-0787-75e1-a9e8-d95ca0f36016";
const CHILD_DEPLOYMENT_ID: &str = "adep_0198f1c5-0787-75e1-a9e8-d95ca0f36017";
const CHILD_PLAN_ARTIFACT_ID: &str = "art_0198f1c5-0787-75e1-a9e8-d95ca0f36018";
const SELECTION_POLICY_ID: &str = "pol_0198f1c5-0787-75e1-a9e8-d95ca0f36019";
const SELECTION_POLICY_REVISION_ID: &str = "prev_0198f1c5-0787-75e1-a9e8-d95ca0f3601a";
const SELECTION_POLICY_DEPLOYMENT_ID: &str = "pdep_0198f1c5-0787-75e1-a9e8-d95ca0f3601b";

struct EmptyCapabilityAdmissionProvider;

#[async_trait]
impl ControllerCapabilityAdmissionProvider for EmptyCapabilityAdmissionProvider {
    async fn decide(
        &self,
        _request: ControllerCapabilityAdmissionRequest,
    ) -> Result<
        ControllerCapabilityAdmissionDecision,
        insight_platform_runtime::DurablePlanDriverError,
    > {
        Ok(ControllerCapabilityAdmissionDecision {
            policies: InvocationPolicyDecisionBundle::build(Vec::new(), None).unwrap(),
            mcp_runtime: None,
            sandbox: None,
        })
    }
}

struct EmptyModelAdmissionProvider;

#[async_trait]
impl ControllerModelAdmissionProvider for EmptyModelAdmissionProvider {
    async fn assemble(
        &self,
        _request: ControllerModelAdmissionRequest,
    ) -> Result<ControllerModelAdmissionDecision, DurablePlanDriverError> {
        Err(DurablePlanDriverError::InvariantViolation)
    }
}

fn id(value: &str) -> ResourceId {
    value.parse().unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn agent_schema() -> ClosedJsonSchema {
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "minLength": 0,
                "maxLength": 1024,
                "x-platform-max-bytes": 4096
            }
        },
        "required": ["question"],
        "additionalProperties": false
    }))
    .unwrap()
}

fn fixture_plan() -> RuntimePlan {
    let entry = PlanNodeKey::new("entry".to_owned()).unwrap();
    let signal = PlanNodeKey::new("signal".to_owned()).unwrap();
    let task = PlanNodeKey::new("task".to_owned()).unwrap();
    let child = PlanNodeKey::new("child".to_owned()).unwrap();
    let finish = PlanNodeKey::new("finish".to_owned()).unwrap();
    RuntimePlan {
        plan_version: 5,
        interface_contract_digest: digest('5'),
        entry_node_id: entry.clone(),
        dependency_slots: BTreeMap::from([(
            "child_worker".to_owned(),
            RuntimeDependencySlot {
                kind: RuntimeDependencyKind::ChildAgent,
                requirement_digest: digest('c'),
            },
        )]),
        nodes: BTreeMap::from([
            (
                entry,
                RuntimeNode::TimerWait {
                    delay_milliseconds: 3_000,
                    resume: signal.clone(),
                },
            ),
            (
                signal,
                RuntimeNode::SignalWait {
                    signal_key: "release".to_owned(),
                    payload: None,
                    timeout_milliseconds: 60_000,
                    resume: task.clone(),
                },
            ),
            (
                task.clone(),
                RuntimeNode::HumanTask {
                    definition: HumanTaskDefinition::Interaction {
                        interaction_kind: InteractionKind::Form,
                        eligible_principal_rule_digest: digest('6'),
                        safe_prompt_key: "provide_input".to_owned(),
                    },
                    response: ExactDataPortRef::NodeOutput {
                        producer_node_id: task,
                        port_id: DataPortKey::new("response".to_owned()).unwrap(),
                        schema_digest: agent_schema().canonical_digest,
                    },
                    timeout_milliseconds: 60_000,
                    resume: child.clone(),
                },
            ),
            (
                child.clone(),
                RuntimeNode::ChildAgentCall {
                    child_agent_slot_id: "child_worker".to_owned(),
                    input: ExactDataPortRef::RunInput {
                        schema_digest: agent_schema().canonical_digest,
                    },
                    candidate_route: None,
                    output: ExactDataPortRef::NodeOutput {
                        producer_node_id: child,
                        port_id: DataPortKey::new("response".to_owned()).unwrap(),
                        schema_digest: agent_schema().canonical_digest,
                    },
                    budget: ChildBudgetLimit {
                        maximum_duration_milliseconds: 60_000,
                        maximum_model_tokens: 1_000,
                        maximum_capability_calls: 10,
                        maximum_artifact_bytes: 1_048_576,
                        maximum_descendant_runs: 2,
                    },
                    cancellation_policy: ChildCancellationPolicy::CascadeAndWait,
                    attempt_limit: 3,
                    retry_backoff_milliseconds: 100,
                    resume: finish.clone(),
                },
            ),
            (
                finish,
                RuntimeNode::Return {
                    value: ExactDataPortRef::RunInput {
                        schema_digest: agent_schema().canonical_digest,
                    },
                },
            ),
        ]),
    }
}

fn child_fixture_plan() -> RuntimePlan {
    let entry = PlanNodeKey::new("entry".to_owned()).unwrap();
    let finish = PlanNodeKey::new("finish".to_owned()).unwrap();
    RuntimePlan {
        plan_version: 5,
        interface_contract_digest: digest('5'),
        entry_node_id: entry.clone(),
        dependency_slots: BTreeMap::new(),
        nodes: BTreeMap::from([
            (
                entry,
                RuntimeNode::TimerWait {
                    delay_milliseconds: 1_000,
                    resume: finish.clone(),
                },
            ),
            (
                finish,
                RuntimeNode::Return {
                    value: ExactDataPortRef::RunInput {
                        schema_digest: agent_schema().canonical_digest,
                    },
                },
            ),
        ]),
    }
}

struct StaticTypedPlanReader {
    plans: BTreeMap<ResourceId, Vec<u8>>,
}

#[async_trait]
impl SchedulerTypedPlanReader for StaticTypedPlanReader {
    async fn read_exact(
        &self,
        request: SchedulerTypedPlanReadRequest,
    ) -> Result<Vec<u8>, SchedulerTypedPlanReadError> {
        let bytes = self
            .plans
            .get(request.artifact.artifact_id())
            .ok_or(SchedulerTypedPlanReadError::Denied)?;
        if request.artifact.byte_length() != u64::try_from(bytes.len()).unwrap()
            || request.artifact.content_digest()
                != &canonical_digest(&serde_json::from_slice::<serde_json::Value>(bytes).unwrap())
                    .unwrap()
                    .parse()
                    .unwrap()
        {
            return Err(SchedulerTypedPlanReadError::Integrity);
        }
        Ok(bytes.clone())
    }
}

struct StaticRunValueReader {
    bytes: Vec<u8>,
    calls: Arc<AtomicU64>,
}

#[async_trait]
impl SchedulerRunValueReader for StaticRunValueReader {
    async fn read_exact(
        &self,
        request: SchedulerRunValueReadRequest,
    ) -> Result<Vec<u8>, SchedulerRunValueReadError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if request.run_value_id.kind() != ResourceKind::RunValue
            || request.schema_digest != agent_schema().canonical_digest
            || request.artifact.artifact_id() != &id(INPUT_ARTIFACT_ID)
            || request.artifact.byte_length() != u64::try_from(self.bytes.len()).unwrap()
            || request.artifact.content_digest()
                != &canonical_digest(
                    &serde_json::from_slice::<serde_json::Value>(&self.bytes).unwrap(),
                )
                .unwrap()
                .parse()
                .unwrap()
        {
            return Err(SchedulerRunValueReadError::Integrity);
        }
        Ok(self.bytes.clone())
    }
}

struct RecordingRunValueResolver {
    repository: PgRepository,
    calls: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<ArtifactObjectReadAuthorityError>>>,
    last_request: Arc<Mutex<Option<SchedulerRunValueReadRequest>>>,
}

#[async_trait]
impl SchedulerRunValueRequestResolver for RecordingRunValueResolver {
    async fn resolve_run_value_read(
        &self,
        lease: SchedulerRunValueLease,
    ) -> Result<SchedulerRunValueReadRequest, ArtifactObjectReadAuthorityError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let result = self.repository.resolve_run_value_read(lease).await;
        *self.last_error.lock().unwrap() = result.as_ref().err().copied();
        *self.last_request.lock().unwrap() = result.as_ref().ok().cloned();
        result
    }
}

async fn seed_policy_deployment(
    pool: &sqlx::PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    policy_id: &ResourceId,
    deployment_id: ResourceId,
    revision: ExactVersionRef,
    qualification_evidence: ArtifactRef,
) -> ExactDeploymentRef {
    let bindings = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: revision.clone(),
            applicability_digest: digest('0'),
            qualification_evidence,
        }),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id, environment,
            bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(deployment_id.to_string())
    .bind(policy_id.to_string())
    .bind(revision.revision_id.to_string())
    .bind(&bindings.digest)
    .bind(bindings.schema_version)
    .bind(&bindings.value)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_version_id = NULL, active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(policy_id.to_string())
    .bind(deployment_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    ExactDeploymentRef::new(deployment_id, bindings.digest.parse().unwrap()).unwrap()
}

async fn insert_ready_artifact(
    pool: &sqlx::PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    retention_policy_revision_id: &ResourceId,
    artifact: &ArtifactRef,
    purpose: &str,
) {
    let now: chrono::DateTime<Utc> = sqlx::query_scalar("SELECT statement_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    let blob_id = fresh_id(ResourceKind::InternalBlob);
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest, security_domain_digest,
            object_reference_ciphertext, object_generation, key_id, encryption_domain_id,
            content_digest, size_bytes, state, version, verified_at, created_at, updated_at
        ) VALUES ($1, $2, 'fixture-runtime', $3, $4, $5, 'generation-1', 'fixture-key', $6,
                  $7, $8, 'verified', 1, $9, $9, $9)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .bind(fresh_digest().to_string())
    .bind(fresh_digest().to_string())
    .bind(vec![1_u8, 2, 3])
    .bind(fresh_id(ResourceKind::EncryptionDomain).to_string())
    .bind(artifact.content_digest().to_string())
    .bind(i64::try_from(artifact.byte_length()).unwrap())
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    let metadata = TypedPayload::new(
        1,
        &json!({
            "display_name": artifact.display_name(),
            "fixture": "orchestration-capacity"
        }),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification, expected_size_bytes,
            expected_digest, declared_media_type, verified_media_type, state, version,
            metadata_schema_version, metadata, metadata_digest, retention_policy_revision_id,
            retain_until, created_by, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, 'ready', 1,
                  $9, $10, $11, $12, $13, $14, $15, $15)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .bind(blob_id.to_string())
    .bind(purpose)
    .bind(artifact.classification().as_str())
    .bind(i64::try_from(artifact.byte_length()).unwrap())
    .bind(artifact.content_digest().to_string())
    .bind(artifact.media_type())
    .bind(metadata.schema_version)
    .bind(&metadata.value)
    .bind(&metadata.digest)
    .bind(retention_policy_revision_id.to_string())
    .bind(now + ChronoDuration::days(1))
    .bind(principal_id.to_string())
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
}

fn audit(suffix: &str) -> CommandAudit {
    CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: id(TENANT_ID),
        principal_id: id(PRINCIPAL_ID),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: id(&format!("rcp_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix}")),
        event_id: id(&format!("evt_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix}")),
        outbox_id: id(&format!("obx_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix}")),
        idempotency_key_digest: digest('a'),
        request_digest: digest('b'),
        receipt_expires_at: Utc::now() + ChronoDuration::hours(1),
    }
}

struct PostgresHandoffHandler {
    repository: PgRepository,
    running: AtomicBool,
}

impl PostgresHandoffHandler {
    fn new(repository: PgRepository) -> Self {
        Self {
            repository,
            running: AtomicBool::new(false),
        }
    }

    async fn wait_running(&self) {
        while !self.running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    }
}

#[async_trait]
impl StartedOrchestrationJobHandler for PostgresHandoffHandler {
    type Outcome = ();

    async fn run(
        &self,
        job: StartedOrchestrationJob,
    ) -> Result<Self::Outcome, GenerationHandlerError> {
        assert_eq!(job.started().job_id, JOB_ID);
        assert_eq!(job.started().lease_epoch, 1);
        assert_eq!(job.started().state, "running");
        self.running.store(true, Ordering::Release);
        std::future::pending().await
    }

    async fn commit(
        &self,
        _job: &StartedOrchestrationJob,
        _fence: insight_platform_postgres::repository::JobFence,
        _outcome: Self::Outcome,
    ) -> GenerationHandlerDisposition {
        GenerationHandlerDisposition::NotCommitted
    }

    async fn handoff(
        &self,
        job: &StartedOrchestrationJob,
        fence: insight_platform_postgres::repository::JobFence,
        reason: GenerationHandoffReason,
    ) -> GenerationHandlerDisposition {
        assert_eq!(reason, GenerationHandoffReason::Shutdown);
        let identities = UuidCoordinatorIdentityFactory;
        let new_id = |kind| identities.new_resource_id(kind).unwrap();
        let command = YieldOrchestrationJob {
            fence,
            outcome: OrchestrationYield::Retry {
                retry_at: Utc::now() + ChronoDuration::milliseconds(100),
            },
            idempotency_key_digest: identities.new_lease_token_digest().unwrap(),
            request_digest: digest('f'),
            receipt_expires_at: job.started().deadline,
            mutations: OrchestrationYieldMutationIds {
                receipt_id: new_id(insight_platform_contracts::ResourceKind::Receipt),
                quota_entry_ids: (0..MAX_ORCHESTRATION_QUOTA_LINES)
                    .map(|_| new_id(insight_platform_contracts::ResourceKind::QuotaLedgerEntry))
                    .collect(),
                run_event_id: new_id(insight_platform_contracts::ResourceKind::Event),
                run_outbox_id: new_id(insight_platform_contracts::ResourceKind::OutboxEvent),
                node_event_id: new_id(insight_platform_contracts::ResourceKind::Event),
                node_outbox_id: new_id(insight_platform_contracts::ResourceKind::OutboxEvent),
                job_event_id: new_id(insight_platform_contracts::ResourceKind::Event),
                job_outbox_id: new_id(insight_platform_contracts::ResourceKind::OutboxEvent),
            },
        };
        let Ok(mut transaction) = self.repository.begin_scheduler_transaction().await else {
            return GenerationHandlerDisposition::NotCommitted;
        };
        match transaction.yield_orchestration_job(command).await {
            Ok(_) => {
                if transaction.commit().await.is_ok() {
                    GenerationHandlerDisposition::Committed
                } else {
                    GenerationHandlerDisposition::NotCommitted
                }
            }
            Err(_) => {
                let _ = transaction.rollback().await;
                GenerationHandlerDisposition::NotCommitted
            }
        }
    }
}

#[test]
fn wait_recovery_worker_process_entry() {
    if std::env::var("PLATFORM_WAIT_RECOVERY_CHILD").as_deref() != Ok("1") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL").unwrap();
        let mut profile = checked_in_hard_limit_profile();
        profile.control_data.recovery_batch.q1_default = 1;
        profile.validate().unwrap();
        let process_generation_id = fresh_id(ResourceKind::WorkerProcessGeneration);
        let bulkheads = PostgresConnectionBulkheads::connect(
            &database_url,
            PostgresConnectionBulkheadConfig {
                worker_role: format!("orch.timer-{}", process_generation_id.uuid()),
                business_max_connections: 1,
                critical_control_reserved_connections: 2,
                process_connection_budget: 3,
                acquire_timeout: Duration::from_secs(2),
                statement_timeout: Duration::from_secs(30),
                idle_timeout: Some(Duration::from_secs(30)),
                max_lifetime: Some(Duration::from_secs(300)),
            },
            &profile,
        )
        .await
        .unwrap();
        verify_schema(bulkheads.business_pool()).await.unwrap();
        let pools = LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: 1,
                worker_role: "orchestration.timer-recovery".to_owned(),
                work_class: WorkClass::Orchestration,
                adapter_runtime_digest: digest('e'),
                protocol_version: 1,
                max_concurrency: 1,
                critical_control_reserved_slots: 1,
            },
            process_generation_id,
        )
        .unwrap();
        let plan = fixture_plan();
        let plan_bytes = canonical_json(&serde_json::to_value(&plan).unwrap()).unwrap();
        let child_plan_bytes =
            canonical_json(&serde_json::to_value(child_fixture_plan()).unwrap()).unwrap();
        let plan_materializer = Arc::new(
            SchedulerPlanMaterializer::new(
                Arc::new(bulkheads.critical_control_repository()),
                Arc::new(StaticTypedPlanReader {
                    plans: BTreeMap::from([
                        (id(TYPED_PLAN_ARTIFACT_ID), plan_bytes),
                        (id(CHILD_PLAN_ARTIFACT_ID), child_plan_bytes),
                    ]),
                }),
                SchedulerPlanMaterializerConfig {
                    request_timeout: Duration::from_secs(1),
                    maximum_bytes: 64 * 1024,
                    json_limits: JsonLimits::CONTRACT_FIXTURE,
                    plan_limits: PlanLimits::from_profile(&profile).unwrap(),
                },
            )
            .unwrap(),
        );
        let durable_store = Arc::new(
            PostgresDurablePlanGenerationStore::new(
                bulkheads.critical_control_repository(),
                Arc::new(
                    SchedulerControllerRunValueMaterializer::new(
                        Arc::new(RecordingRunValueResolver {
                            repository: bulkheads.critical_control_repository(),
                            calls: Arc::new(AtomicU64::new(0)),
                            last_error: Arc::new(Mutex::new(None)),
                            last_request: Arc::new(Mutex::new(None)),
                        }),
                        Arc::new(StaticRunValueReader {
                            bytes: canonical_json(&json!({"question": "coordinator"})).unwrap(),
                            calls: Arc::new(AtomicU64::new(0)),
                        }),
                        Duration::from_secs(1),
                    )
                    .unwrap(),
                ),
                Arc::new(UuidCoordinatorIdentityFactory),
                Arc::new(EmptyCapabilityAdmissionProvider),
                Arc::new(EmptyModelAdmissionProvider),
                Duration::from_millis(50),
            )
            .unwrap(),
        );
        let handler = Arc::new(MaterializingOrchestrationJobHandler::new(
            plan_materializer,
            Arc::new(
                ExactPlanGenerationDriver::new(durable_store, ExpressionLimits::ABSOLUTE).unwrap(),
            ),
        ));
        let executor = Arc::new(
            LeaseFencedOrchestrationExecutor::new(
                Arc::new(bulkheads.business_repository()),
                handler,
                Arc::new(UuidCoordinatorIdentityFactory),
                OrchestrationExecutorConfig::from_profile(
                    &profile,
                    OrchestrationExecutorTiming {
                        heartbeat_jitter: Duration::from_millis(10),
                        store_retry_backoff: Duration::from_millis(10),
                    },
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let coordinator = WorkCoordinator::new(
            Arc::new(bulkheads.business_repository()),
            executor,
            Arc::new(UuidCoordinatorIdentityFactory),
            pools.clone(),
            OrchestrationCoordinatorConfig::from_profile(
                &profile,
                CoordinatorTiming {
                    coalesce_window: Duration::from_millis(2),
                    safety_scan_interval: Duration::from_millis(100),
                    safety_scan_jitter: Duration::ZERO,
                    claim_failure_backoff: Duration::from_millis(5),
                    drain_grace: Duration::from_secs(1),
                },
            )
            .unwrap(),
        )
        .unwrap()
        .spawn();
        let safety = OrchestrationSafetyDriver::new(
            Arc::new(bulkheads.critical_control_repository()),
            Arc::new(UuidCoordinatorIdentityFactory),
            pools,
            OrchestrationSafetyConfig::from_profile(
                &profile,
                SafetyScanShard::whole(),
                SafetyDriverTiming {
                    scan_interval: Duration::from_millis(100),
                    scan_jitter: Duration::ZERO,
                    failure_backoff: Duration::from_millis(10),
                },
            )
            .unwrap(),
        )
        .unwrap()
        .spawn();

        let terminal = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let states = sqlx::query_as::<_, (String, String)>(
                    r#"
                    SELECT job.state, run.state
                    FROM insight_platform.jobs AS job
                    JOIN insight_platform.runs AS run
                      ON run.tenant_id = job.tenant_id AND run.run_id = job.run_id
                    WHERE job.tenant_id = $1 AND job.job_id = $2
                    "#,
                )
                .bind(TENANT_ID)
                .bind(JOB_ID)
                .fetch_one(bulkheads.critical_control_pool())
                .await
                .unwrap();
                if states == ("succeeded".to_owned(), "succeeded".to_owned()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if terminal.is_err() {
            panic!(
                "wait recovery process did not reach the durable terminal state: safety={:?}",
                safety.snapshot()
            );
        }
        coordinator.shutdown().await.unwrap();
        safety.shutdown().await.unwrap();
        bulkheads.close().await;
        println!("WAIT_RECOVERY_CHILD_RESULT terminal=succeeded");
    });
}

#[test]
fn real_postgres_coordinator_claims_with_physical_and_connection_bulkheads() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
            eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture skipped");
            return;
        };
        let _fixture_lock = acquire_phase2_fixture_lock(&database_url).await;
        let mut profile = checked_in_hard_limit_profile();
        profile.run_scheduler.heartbeat_milliseconds.q1_default = 500;
        profile.run_scheduler.lease_milliseconds.q1_default = 3_000;
        profile.control_data.recovery_batch.q1_default = 1;
        profile.validate().unwrap();
        let connection_config = PostgresConnectionBulkheadConfig {
            worker_role: "orchestration.pg-fixture".to_owned(),
            business_max_connections: 1,
            critical_control_reserved_connections: 2,
            process_connection_budget: 3,
            acquire_timeout: Duration::from_secs(2),
            statement_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(30)),
            max_lifetime: Some(Duration::from_secs(300)),
        };
        let bulkheads =
            PostgresConnectionBulkheads::connect(&database_url, connection_config, &profile)
                .await
                .unwrap();
        verify_schema(bulkheads.business_pool()).await.unwrap();
        let repository = bulkheads.business_repository();
        let bindings = seed_authorities(&repository).await;
        let admitted = admit_run(&repository, bindings).await;
        assert_eq!(admitted.state, "queued");

        let worker_manifest = WorkerManifest {
            manifest_version: 1,
            worker_role: "orchestration.pg-fixture".to_owned(),
            work_class: WorkClass::Orchestration,
            adapter_runtime_digest: digest('e'),
            protocol_version: 1,
            max_concurrency: 1,
            critical_control_reserved_slots: 1,
        };
        let pools = LocalWorkerPools::new(worker_manifest, id(WORKER_ID)).unwrap();
        let handler = Arc::new(PostgresHandoffHandler::new(
            bulkheads.critical_control_repository(),
        ));
        let executor = Arc::new(
            LeaseFencedOrchestrationExecutor::new(
                Arc::new(repository.clone()),
                Arc::clone(&handler),
                Arc::new(UuidCoordinatorIdentityFactory),
                OrchestrationExecutorConfig::from_profile(
                    &profile,
                    OrchestrationExecutorTiming {
                        heartbeat_jitter: Duration::from_millis(10),
                        store_retry_backoff: Duration::from_millis(10),
                    },
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let coordinator_config = OrchestrationCoordinatorConfig::from_profile(
            &profile,
            CoordinatorTiming {
                coalesce_window: Duration::from_millis(2),
                safety_scan_interval: Duration::from_millis(200),
                safety_scan_jitter: Duration::from_millis(5),
                claim_failure_backoff: Duration::from_millis(5),
                drain_grace: Duration::from_secs(1),
            },
        )
        .unwrap();
        let running = WorkCoordinator::new(
            Arc::new(repository),
            Arc::clone(&executor),
            Arc::new(UuidCoordinatorIdentityFactory),
            pools.clone(),
            coordinator_config,
        )
        .unwrap()
        .spawn();
        tokio::time::timeout(Duration::from_secs(2), handler.wait_running())
            .await
            .expect("real PostgreSQL Job was not started");
        tokio::time::sleep(Duration::from_millis(650)).await;
        assert_eq!(running.snapshot().jobs_claimed, 1);
        assert_eq!(running.snapshot().active_jobs, 1);

        let business_application: String =
            sqlx::query_scalar("SELECT current_setting('application_name')")
                .fetch_one(bulkheads.business_pool())
                .await
                .unwrap();
        let critical_application: String =
            sqlx::query_scalar("SELECT current_setting('application_name')")
                .fetch_one(bulkheads.critical_control_pool())
                .await
                .unwrap();
        assert_eq!(business_application, "orchestration.pg-fixture.business");
        assert_eq!(
            critical_application,
            "orchestration.pg-fixture.critical-control"
        );

        let _business_saturation = bulkheads.business_pool().acquire().await.unwrap();
        let claimed = sqlx::query(
            r#"
            SELECT job.state, job.worker_id, job.lease_epoch, job.attempt_no,
                   job.version, job.heartbeat_at,
                   run.active_work_count, quota.reserved_value
            FROM insight_platform.jobs AS job
            JOIN insight_platform.runs AS run
              ON run.tenant_id = job.tenant_id AND run.run_id = job.run_id
            JOIN insight_platform.quota_accounts AS quota
              ON quota.tenant_id = job.tenant_id AND quota.quota_account_id = $3
            WHERE job.tenant_id = $1 AND job.job_id = $2
            "#,
        )
        .bind(TENANT_ID)
        .bind(JOB_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(bulkheads.critical_control_pool())
        .await
        .expect("critical-control reserve was blocked by business saturation");
        assert_eq!(claimed.try_get::<String, _>("state").unwrap(), "running");
        assert_eq!(
            claimed.try_get::<String, _>("worker_id").unwrap(),
            WORKER_ID
        );
        assert_eq!(claimed.try_get::<i64, _>("lease_epoch").unwrap(), 1);
        assert_eq!(claimed.try_get::<i32, _>("attempt_no").unwrap(), 1);
        assert!(claimed.try_get::<i64, _>("version").unwrap() >= 4);
        assert!(claimed
            .try_get::<chrono::DateTime<Utc>, _>("heartbeat_at")
            .is_ok());
        assert_eq!(claimed.try_get::<i32, _>("active_work_count").unwrap(), 1);
        assert_eq!(claimed.try_get::<i64, _>("reserved_value").unwrap(), 1);

        let exit = running.shutdown().await.unwrap();
        assert_eq!(exit.abandoned_on_drain_timeout, 0);
        assert_eq!(exit.snapshot.settled_generations, 1);
        let state_after_drain = sqlx::query(
            r#"
            SELECT job.state, run.active_work_count, quota.reserved_value
            FROM insight_platform.jobs AS job
            JOIN insight_platform.runs AS run
              ON run.tenant_id = job.tenant_id AND run.run_id = job.run_id
            JOIN insight_platform.quota_accounts AS quota
              ON quota.tenant_id = job.tenant_id AND quota.quota_account_id = $3
            WHERE job.tenant_id = $1 AND job.job_id = $2
            "#,
        )
        .bind(TENANT_ID)
        .bind(JOB_ID)
        .bind(QUOTA_ACCOUNT_ID)
        .fetch_one(bulkheads.critical_control_pool())
        .await
        .unwrap();
        assert_eq!(
            state_after_drain.try_get::<String, _>("state").unwrap(),
            "retry_scheduled"
        );
        assert_eq!(
            state_after_drain
                .try_get::<i32, _>("active_work_count")
                .unwrap(),
            0
        );
        assert_eq!(
            state_after_drain
                .try_get::<i64, _>("reserved_value")
                .unwrap(),
            0
        );
        let local_business_reservation = pools
            .reserve_claim_capacity(
                WorkClass::Orchestration,
                1,
                ClaimBatchHardLimit::from_profile(&profile).unwrap(),
            )
            .unwrap()
            .unwrap();
        let local_business_saturation = local_business_reservation
            .bind_claimed_jobs(vec![ClaimedJobIdentity {
                job_id: id(JOB_ID),
                lease_generation: 1,
            }])
            .unwrap();
        assert_eq!(pools.snapshot().business_available, 0);
        let safety = OrchestrationSafetyDriver::new(
            Arc::new(bulkheads.critical_control_repository()),
            Arc::new(UuidCoordinatorIdentityFactory),
            pools.clone(),
            OrchestrationSafetyConfig::from_profile(
                &profile,
                SafetyScanShard::whole(),
                SafetyDriverTiming {
                    scan_interval: Duration::from_millis(100),
                    scan_jitter: Duration::ZERO,
                    failure_backoff: Duration::from_millis(10),
                },
            )
            .unwrap(),
        )
        .unwrap()
        .spawn();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state: String = sqlx::query_scalar(
                    "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
                )
                .bind(TENANT_ID)
                .bind(JOB_ID)
                .fetch_one(bulkheads.critical_control_pool())
                .await
                .unwrap();
                if state == "ready" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("critical-control safety driver did not promote the handed-off retry");
        assert!(safety.snapshot().mutations >= 1);
        assert!(safety.snapshot().scan_attempts >= 2);
        drop(local_business_saturation);
        drop(_business_saturation);
        let safety_exit = safety.shutdown().await.unwrap();
        assert!(safety_exit.mutations >= 1);

        let executable = std::env::current_exe().unwrap();
        let spawn_wait_worker = || {
            Command::new(&executable)
                .arg("--exact")
                .arg("wait_recovery_worker_process_entry")
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env("PLATFORM_WAIT_RECOVERY_CHILD", "1")
                .env("PLATFORM_TEST_DATABASE_URL", &database_url)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        };
        let mut crash_child = spawn_wait_worker();
        let park_deadline = Instant::now() + Duration::from_secs(5);
        let scheduled_at = loop {
            if crash_child.try_wait().unwrap().is_some() {
                let failed = crash_child.wait_with_output().unwrap();
                panic!(
                    "first Timer worker exited before durable park: stdout={} stderr={}",
                    String::from_utf8_lossy(&failed.stdout),
                    String::from_utf8_lossy(&failed.stderr),
                );
            }
            let parked = sqlx::query_as::<_, (String, Option<chrono::DateTime<Utc>>, String)>(
                r#"
                SELECT job.state, job.scheduled_at, node.state
                FROM insight_platform.jobs AS job
                JOIN insight_platform.run_nodes AS node
                  ON node.tenant_id = job.tenant_id
                 AND node.node_id = job.node_id
                WHERE job.tenant_id = $1 AND job.job_id = $2
                "#,
            )
            .bind(TENANT_ID)
            .bind(JOB_ID)
            .fetch_one(bulkheads.critical_control_pool())
            .await
            .unwrap();
            if parked.0 == "waiting" && parked.2 == "waiting" {
                break parked.1.expect("Timer wait must own a database deadline");
            }
            if Instant::now() >= park_deadline {
                crash_child.kill().unwrap();
                let failed = crash_child.wait_with_output().unwrap();
                panic!(
                    "first process did not durably park the Timer: stdout={} stderr={}",
                    String::from_utf8_lossy(&failed.stdout),
                    String::from_utf8_lossy(&failed.stderr),
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(scheduled_at > Utc::now());
        crash_child.kill().unwrap();
        let crashed = crash_child.wait_with_output().unwrap();
        assert!(!crashed.status.success());

        let until_due = scheduled_at.signed_duration_since(Utc::now());
        if let Ok(until_due) = until_due.to_std() {
            tokio::time::sleep(until_due + Duration::from_millis(100)).await;
        }
        let mut signal_child = spawn_wait_worker();
        let signal_park_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if signal_child.try_wait().unwrap().is_some() {
                let failed = signal_child.wait_with_output().unwrap();
                panic!(
                    "replacement worker exited before durable Signal park: stdout={} stderr={}",
                    String::from_utf8_lossy(&failed.stdout),
                    String::from_utf8_lossy(&failed.stderr),
                );
            }
            let signal_parked: Option<(String, String)> = sqlx::query_as(
                r#"
                SELECT job.state, node.state
                FROM insight_platform.jobs AS job
                JOIN insight_platform.run_nodes AS node
                  ON node.tenant_id = job.tenant_id AND node.node_id = job.node_id
                WHERE job.tenant_id = $1 AND job.run_id = $2
                  AND job.wake_kind = 'signal' AND node.plan_node_key = 'signal'
                "#,
            )
            .bind(TENANT_ID)
            .bind(RUN_ID)
            .fetch_optional(bulkheads.critical_control_pool())
            .await
            .unwrap();
            if signal_parked.as_ref().is_some_and(|states| {
                states == &("waiting".to_owned(), "waiting".to_owned())
            }) {
                break;
            }
            if Instant::now() >= signal_park_deadline {
                signal_child.kill().unwrap();
                let failed = signal_child.wait_with_output().unwrap();
                panic!(
                    "replacement worker did not durably park the Signal: stdout={} stderr={}",
                    String::from_utf8_lossy(&failed.stdout),
                    String::from_utf8_lossy(&failed.stderr),
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        signal_child.kill().unwrap();
        let signal_crashed = signal_child.wait_with_output().unwrap();
        assert!(!signal_crashed.status.success());

        let idempotency_key_digest = fresh_digest();
        let request_digest = fresh_digest();
        let target = bulkheads
            .critical_control_repository()
            .resolve_signal_wake_target_for_principal(&ResolveOrchestrationSignalTarget {
                tenant_id: id(TENANT_ID),
                principal_id: id(PRINCIPAL_ID),
                principal_kind: PrincipalKind::AgentRunner,
                run_id: id(RUN_ID),
                signal_key: "release".to_owned(),
                idempotency_key_digest: idempotency_key_digest.clone(),
                request_digest: request_digest.clone(),
            })
            .await
            .unwrap();
        let identities = UuidCoordinatorIdentityFactory;
        let new_id = |kind| identities.new_resource_id(kind).unwrap();
        let mut signal_transaction = bulkheads
            .critical_control_repository()
            .begin_scheduler_transaction()
            .await
            .unwrap();
        signal_transaction
            .wake_orchestration_job(WakeOrchestrationJob {
                tenant_id: id(TENANT_ID),
                job_id: target.job_id,
                expected_job_version: target.job_version,
                expected_wake_generation: target.wake_generation,
                source: WakeSource::Signal,
                signal_key: Some("release".to_owned()),
                signal_payload: None,
                idempotency_key_digest: idempotency_key_digest.clone(),
                request_digest: request_digest.clone(),
                receipt_expires_at: Utc::now() + ChronoDuration::hours(1),
                signal_authority: Some(OrchestrationSignalAuthority {
                    tenant_id: id(TENANT_ID),
                    principal_id: id(PRINCIPAL_ID),
                    principal_kind: PrincipalKind::AgentRunner,
                    run_id: id(RUN_ID),
                    idempotency_key_digest,
                    request_digest,
                }),
                mutations: OrchestrationWakeMutationIds {
                    receipt_id: new_id(ResourceKind::Receipt),
                    run_event_id: new_id(ResourceKind::Event),
                    run_outbox_id: new_id(ResourceKind::OutboxEvent),
                    node_event_id: new_id(ResourceKind::Event),
                    node_outbox_id: new_id(ResourceKind::OutboxEvent),
                    job_event_id: new_id(ResourceKind::Event),
                    job_outbox_id: new_id(ResourceKind::OutboxEvent),
                },
            })
            .await
            .unwrap();
        signal_transaction.commit().await.unwrap();

        let mut task_child = spawn_wait_worker();
        let task_park_deadline = Instant::now() + Duration::from_secs(10);
        let (task_id, task_generation, task_version) = loop {
            if task_child.try_wait().unwrap().is_some() {
                let failed = task_child.wait_with_output().unwrap();
                panic!(
                    "Signal recovery worker exited before durable HumanTask park: stdout={} stderr={}",
                    String::from_utf8_lossy(&failed.stdout),
                    String::from_utf8_lossy(&failed.stderr),
                );
            }
            let task: Option<(String, i64, i64)> = sqlx::query_as(
                r#"
                SELECT task_id, generation, version
                FROM insight_platform.tasks
                WHERE tenant_id = $1 AND run_id = $2
                  AND task_kind = 'interaction_form' AND state = 'pending'
                "#,
            )
            .bind(TENANT_ID)
            .bind(RUN_ID)
            .fetch_optional(bulkheads.critical_control_pool())
            .await
            .unwrap();
            if let Some(task) = task {
                break task;
            }
            if Instant::now() >= task_park_deadline {
                task_child.kill().unwrap();
                let failed = task_child.wait_with_output().unwrap();
                panic!(
                    "Signal recovery worker did not durably park the HumanTask: stdout={} stderr={}",
                    String::from_utf8_lossy(&failed.stdout),
                    String::from_utf8_lossy(&failed.stderr),
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        task_child.kill().unwrap();
        let task_crashed = task_child.wait_with_output().unwrap();
        assert!(!task_crashed.status.success());

        let task_input = json!({"question": "approved"});
        let task_idempotency_digest = fresh_digest();
        let task_request_digest = fresh_digest();
        let task_response = RunInputValue {
            value_id: new_id(ResourceKind::RunValue),
            classification: DataClassification::Internal,
            schema_digest: agent_schema().canonical_digest,
            content_digest: canonical_digest(&task_input).unwrap().parse().unwrap(),
            value: ValueRef::Inline { value: task_input },
        };
        let mut task_transaction = bulkheads
            .critical_control_repository()
            .begin_scheduler_transaction()
            .await
            .unwrap();
        task_transaction
            .resolve_orchestration_task(ResolveOrchestrationTask {
                audit: CommandAudit {
                    trace: insight_platform_contracts::TraceIdentityV1::generate(),
                    tenant_id: id(TENANT_ID),
                    principal_id: id(PRINCIPAL_ID),
                    principal_kind: PrincipalKind::AgentRunner,
                    receipt_id: new_id(ResourceKind::Receipt),
                    event_id: new_id(ResourceKind::Event),
                    outbox_id: new_id(ResourceKind::OutboxEvent),
                    idempotency_key_digest: task_idempotency_digest,
                    request_digest: task_request_digest.clone(),
                    receipt_expires_at: Utc::now() + ChronoDuration::hours(1),
                },
                task_id: id(&task_id),
                expected_generation: u64::try_from(task_generation).unwrap(),
                expected_task_version: u64::try_from(task_version).unwrap(),
                target: TaskState::Responded,
                response: Some(task_response),
                resume_job_id: new_id(ResourceKind::Job),
                resume_request_digest: task_request_digest,
                mutations: ResolveOrchestrationTaskMutationIds {
                    run_event_id: new_id(ResourceKind::Event),
                    run_outbox_id: new_id(ResourceKind::OutboxEvent),
                    node_event_id: new_id(ResourceKind::Event),
                    node_outbox_id: new_id(ResourceKind::OutboxEvent),
                    job_event_id: new_id(ResourceKind::Event),
                    job_outbox_id: new_id(ResourceKind::OutboxEvent),
                },
            })
            .await
            .unwrap();
        task_transaction.commit().await.unwrap();

        let mut child_run_worker = spawn_wait_worker();
        let child_park_deadline = Instant::now() + Duration::from_secs(10);
        let (child_run_id, child_due_at) = loop {
            if child_run_worker.try_wait().unwrap().is_some() {
                let failed = child_run_worker.wait_with_output().unwrap();
                panic!(
                    "HumanTask recovery worker exited before durable child Timer park: stdout={} stderr={}",
                    String::from_utf8_lossy(&failed.stdout),
                    String::from_utf8_lossy(&failed.stderr),
                );
            }
            let child: Option<(String, chrono::DateTime<Utc>)> = sqlx::query_as(
                r#"
                SELECT child.run_id, job.scheduled_at
                FROM insight_platform.run_nodes AS link
                JOIN insight_platform.runs AS child
                  ON child.tenant_id = link.tenant_id AND child.run_id = link.related_run_id
                JOIN insight_platform.jobs AS job
                  ON job.tenant_id = child.tenant_id AND job.run_id = child.run_id
                WHERE link.tenant_id = $1 AND link.run_id = $2
                  AND link.record_kind = 'child_run_link'
                  AND job.wake_kind = 'timer' AND job.state = 'waiting'
                "#,
            )
            .bind(TENANT_ID)
            .bind(RUN_ID)
            .fetch_optional(bulkheads.critical_control_pool())
            .await
            .unwrap();
            if let Some(child) = child {
                break child;
            }
            if Instant::now() >= child_park_deadline {
                child_run_worker.kill().unwrap();
                let failed = child_run_worker.wait_with_output().unwrap();
                panic!(
                    "HumanTask recovery worker did not durably park the child Run: stdout={} stderr={}",
                    String::from_utf8_lossy(&failed.stdout),
                    String::from_utf8_lossy(&failed.stderr),
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        child_run_worker.kill().unwrap();
        let child_worker_crashed = child_run_worker.wait_with_output().unwrap();
        assert!(!child_worker_crashed.status.success());
        let child_until_due = child_due_at.signed_duration_since(Utc::now());
        if let Ok(child_until_due) = child_until_due.to_std() {
            tokio::time::sleep(child_until_due + Duration::from_millis(100)).await;
        }

        let final_child = spawn_wait_worker();
        let recovered = tokio::time::timeout(
            Duration::from_secs(25),
            tokio::task::spawn_blocking(move || final_child.wait_with_output()),
        )
        .await
        .expect("final HumanTask recovery worker process did not exit")
        .unwrap()
        .unwrap();
        assert!(
            recovered.status.success(),
            "replacement Timer worker failed: stdout={} stderr={}",
            String::from_utf8_lossy(&recovered.stdout),
            String::from_utf8_lossy(&recovered.stderr),
        );
        assert!(
            String::from_utf8_lossy(&recovered.stdout)
                .contains("WAIT_RECOVERY_CHILD_RESULT terminal=succeeded")
        );
        let activated = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT plan_node_key, state
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND plan_node_key = 'finish'
              AND record_kind = 'node_execution'
            "#,
        )
        .bind(TENANT_ID)
        .bind(RUN_ID)
        .fetch_one(bulkheads.critical_control_pool())
        .await
        .unwrap();
        assert_eq!(activated.0, "finish");
        assert_eq!(activated.1, "succeeded");
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT output_value_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
            )
            .bind(TENANT_ID)
            .bind(RUN_ID)
            .fetch_one(bulkheads.critical_control_pool())
            .await
            .unwrap()
            .as_deref(),
            Some(VALUE_ID)
        );
        let finish_count: i64 = sqlx::query_scalar(
            r#"
            SELECT count(*)
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND plan_node_key = 'finish'
              AND record_kind = 'node_execution' AND state = 'succeeded'
            "#,
        )
        .bind(TENANT_ID)
        .bind(RUN_ID)
        .fetch_one(bulkheads.critical_control_pool())
        .await
        .unwrap();
        assert_eq!(finish_count, 1);
        let task_terminal = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT state, response_value_id FROM insight_platform.tasks WHERE tenant_id = $1 AND task_id = $2",
        )
        .bind(TENANT_ID)
        .bind(&task_id)
        .fetch_one(bulkheads.critical_control_pool())
        .await
        .unwrap();
        assert_eq!(task_terminal.0, "responded");
        assert!(task_terminal.1.is_some());
        let child_terminal: (String, String) = sqlx::query_as(
            r#"
            SELECT child.state, link.state
            FROM insight_platform.run_nodes AS link
            JOIN insight_platform.runs AS child
              ON child.tenant_id = link.tenant_id AND child.run_id = link.related_run_id
            WHERE link.tenant_id = $1 AND link.run_id = $2
              AND link.record_kind = 'child_run_link' AND child.run_id = $3
            "#,
        )
        .bind(TENANT_ID)
        .bind(RUN_ID)
        .bind(&child_run_id)
        .fetch_one(bulkheads.critical_control_pool())
        .await
        .unwrap();
        assert_eq!(child_terminal, ("succeeded".to_owned(), "succeeded".to_owned()));
        bulkheads.close().await;
    });
}

async fn seed_authorities(repository: &PgRepository) -> RunBindingsSnapshot {
    repository
        .create_tenant(NewTenant {
            tenant_id: TENANT_ID.to_owned(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    repository
        .create_principal(NewPrincipal {
            principal_id: id(PRINCIPAL_ID),
            authentication_authority_digest: fresh_digest(),
            subject_digest: fresh_digest(),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: id(TENANT_ID),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![
                    Permission::AgentRun,
                    Permission::InteractionRespond,
                    Permission::RuntimeControl,
                    Permission::TenantManage,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();

    let pool = repository.pool();
    let empty = TypedPayload::empty(1).unwrap();
    for (resource_id, resource_kind) in [
        (POLICY_ID, "policy"),
        (SELECTION_POLICY_ID, "policy"),
        (AGENT_ID, "agent"),
        (CHILD_AGENT_ID, "agent"),
    ] {
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resources (
                tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, $3, 'active', 'enabled', $4, $5, $6)
            "#,
        )
        .bind(TENANT_ID)
        .bind(resource_id)
        .bind(resource_kind)
        .bind(empty.schema_version)
        .bind(&empty.value)
        .bind(&empty.digest)
        .execute(pool)
        .await
        .unwrap();
    }

    let scheduling = SchedulingPolicyDocument {
        version: 1,
        weight: 1,
        burst: 1,
        aging_rounds: 2,
    };
    let policy_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                authoring_package: AuthoringPackage {
                    artifact: ArtifactRef::new(
                        id("art_0198f1c5-0787-75e1-a9e8-d95ca0f36010"),
                        digest('3'),
                        1,
                        "application/json",
                        DataClassification::Internal,
                        Some("scheduling-policy.json".to_owned()),
                    )
                    .unwrap(),
                    manifest_digest: digest('4'),
                },
                contract_digest: digest('5'),
                dependency_versions: vec![],
                policy_versions: vec![],
                policy_kind: PolicyKind::Scheduling,
                rules_digest: scheduling.canonical_digest().unwrap(),
                selection: None,
                scheduling: Some(scheduling),
                retention: None,
                model_safety: None,
                model_budget: None,
                model_public_projection: None,
                mcp_protocol: None,
                mcp_auth: None,
                sandbox_isolation: None,
                sandbox_resource: None,
                sandbox_network: None,
                sandbox_artifact_io: None,
                sandbox_secret_resolution: None,
            })),
            validation: ValidationSummary {
                validator_digest: digest('6'),
                validated_draft_digest: digest('7'),
                dependency_closure_digest: digest('8'),
                security_evidence_digest: digest('9'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let selection = CandidateSelectionPolicyDocument {
        schema_version: 1,
        mode: CandidateSelectionMode::OnlyCandidate,
        route_schema_digest: None,
    };
    let selection_policy_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                authoring_package: AuthoringPackage {
                    artifact: ArtifactRef::new(
                        id("art_0198f1c5-0787-75e1-a9e8-d95ca0f36010"),
                        digest('3'),
                        1,
                        "application/json",
                        DataClassification::Internal,
                        Some("selection-policy.json".to_owned()),
                    )
                    .unwrap(),
                    manifest_digest: digest('4'),
                },
                contract_digest: digest('5'),
                dependency_versions: vec![],
                policy_versions: vec![],
                policy_kind: PolicyKind::Selection,
                rules_digest: selection.canonical_digest().unwrap(),
                selection: Some(selection),
                scheduling: None,
                retention: None,
                model_safety: None,
                model_budget: None,
                model_public_projection: None,
                mcp_protocol: None,
                mcp_auth: None,
                sandbox_isolation: None,
                sandbox_resource: None,
                sandbox_network: None,
                sandbox_artifact_io: None,
                sandbox_secret_resolution: None,
            })),
            validation: ValidationSummary {
                validator_digest: digest('6'),
                validated_draft_digest: digest('7'),
                dependency_closure_digest: digest('8'),
                security_evidence_digest: digest('9'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let plan = fixture_plan();
    let plan_limits = PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
    let typed_plan_bytes = canonical_json(&serde_json::to_value(&plan).unwrap()).unwrap();
    let typed_plan_digest = plan.canonical_digest(plan_limits).unwrap();
    let typed_plan_artifact = ArtifactRef::new(
        id(TYPED_PLAN_ARTIFACT_ID),
        typed_plan_digest.clone(),
        u64::try_from(typed_plan_bytes.len()).unwrap(),
        "application/json",
        DataClassification::Internal,
        Some("typed-plan.json".to_owned()),
    )
    .unwrap();
    let agent_plan_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Agent(AgentResourceSpec {
                authoring_name: "coordinator-agent".to_owned(),
                required_features: vec![],
                authoring_package: AuthoringPackage {
                    artifact: typed_plan_artifact.clone(),
                    manifest_digest: digest('4'),
                },
                contract_digest: digest('5'),
                dependency_versions: vec![],
                policy_versions: vec![],
                author_instructions: None,
                input_schema: agent_schema(),
                output_schema: agent_schema(),
                error_schema: agent_schema(),
                typed_plan_artifact_id: id(TYPED_PLAN_ARTIFACT_ID),
                typed_plan_digest: typed_plan_digest.clone(),
            }),
            validation: ValidationSummary {
                validator_digest: digest('6'),
                validated_draft_digest: digest('7'),
                dependency_closure_digest: digest('8'),
                security_evidence_digest: digest('9'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let child_plan = child_fixture_plan();
    let child_plan_bytes = canonical_json(&serde_json::to_value(&child_plan).unwrap()).unwrap();
    let child_plan_digest = child_plan.canonical_digest(plan_limits).unwrap();
    let child_plan_artifact = ArtifactRef::new(
        id(CHILD_PLAN_ARTIFACT_ID),
        child_plan_digest.clone(),
        u64::try_from(child_plan_bytes.len()).unwrap(),
        "application/json",
        DataClassification::Internal,
        Some("child-typed-plan.json".to_owned()),
    )
    .unwrap();
    let child_plan_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Agent(AgentResourceSpec {
                authoring_name: "coordinator-child-agent".to_owned(),
                required_features: vec![],
                authoring_package: AuthoringPackage {
                    artifact: child_plan_artifact.clone(),
                    manifest_digest: digest('4'),
                },
                contract_digest: digest('5'),
                dependency_versions: vec![],
                policy_versions: vec![],
                author_instructions: None,
                input_schema: agent_schema(),
                output_schema: agent_schema(),
                error_schema: agent_schema(),
                typed_plan_artifact_id: id(CHILD_PLAN_ARTIFACT_ID),
                typed_plan_digest: child_plan_digest.clone(),
            }),
            validation: ValidationSummary {
                validator_digest: digest('6'),
                validated_draft_digest: digest('7'),
                dependency_closure_digest: digest('8'),
                security_evidence_digest: digest('9'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    for (version_id, resource_id, kind, content_digest, payload) in [
        (
            POLICY_REVISION_ID,
            POLICY_ID,
            "policy_revision",
            digest('a'),
            &policy_payload,
        ),
        (
            SELECTION_POLICY_REVISION_ID,
            SELECTION_POLICY_ID,
            "policy_revision",
            digest('d'),
            &selection_policy_payload,
        ),
        (
            INTERFACE_ID,
            AGENT_ID,
            "agent_interface_revision",
            digest('b'),
            &agent_plan_payload,
        ),
        (
            PLAN_ID,
            AGENT_ID,
            "agent_plan_revision",
            typed_plan_digest.clone(),
            &agent_plan_payload,
        ),
        (
            CHILD_INTERFACE_ID,
            CHILD_AGENT_ID,
            "agent_interface_revision",
            digest('e'),
            &child_plan_payload,
        ),
        (
            CHILD_PLAN_ID,
            CHILD_AGENT_ID,
            "agent_plan_revision",
            child_plan_digest.clone(),
            &child_plan_payload,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resource_versions (
                tenant_id, resource_version_id, resource_id, resource_version_kind,
                revision_no, content_digest, payload_schema_version, payload,
                payload_digest, created_by
            ) VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(TENANT_ID)
        .bind(version_id)
        .bind(resource_id)
        .bind(kind)
        .bind(content_digest.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(PRINCIPAL_ID)
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "UPDATE insight_platform.resources SET active_version_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(TENANT_ID)
    .bind(POLICY_ID)
    .bind(POLICY_REVISION_ID)
    .execute(pool)
    .await
    .unwrap();
    let qualification_evidence = ArtifactRef::new(
        id("art_0198f1c5-0787-75e1-a9e8-d95ca0f36010"),
        digest('3'),
        1,
        "application/json",
        DataClassification::Internal,
        Some("scheduling-policy.json".to_owned()),
    )
    .unwrap();
    insert_ready_artifact(
        pool,
        &id(TENANT_ID),
        &id(PRINCIPAL_ID),
        &id(POLICY_REVISION_ID),
        &qualification_evidence,
        "qualification",
    )
    .await;
    insert_ready_artifact(
        pool,
        &id(TENANT_ID),
        &id(PRINCIPAL_ID),
        &id(POLICY_REVISION_ID),
        &typed_plan_artifact,
        "typed_plan",
    )
    .await;
    insert_ready_artifact(
        pool,
        &id(TENANT_ID),
        &id(PRINCIPAL_ID),
        &id(POLICY_REVISION_ID),
        &child_plan_artifact,
        "typed_plan",
    )
    .await;
    sqlx::query(
        "UPDATE insight_platform.resource_versions SET artifact_id = $3 WHERE tenant_id = $1 AND resource_version_id = $2",
    )
    .bind(TENANT_ID)
    .bind(PLAN_ID)
    .bind(TYPED_PLAN_ARTIFACT_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resource_versions SET artifact_id = $3 WHERE tenant_id = $1 AND resource_version_id = $2",
    )
    .bind(TENANT_ID)
    .bind(CHILD_PLAN_ID)
    .bind(CHILD_PLAN_ARTIFACT_ID)
    .execute(pool)
    .await
    .unwrap();
    let policy = ExactVersionRef::new(id(POLICY_REVISION_ID), digest('a')).unwrap();
    let policy_deployment = seed_policy_deployment(
        pool,
        &id(TENANT_ID),
        &id(PRINCIPAL_ID),
        &id(POLICY_ID),
        id(POLICY_DEPLOYMENT_ID),
        policy.clone(),
        qualification_evidence.clone(),
    )
    .await;
    let policy_binding = ExactPolicyBinding {
        deployment: policy_deployment.clone(),
        revision: policy.clone(),
    };
    let selection_revision =
        ExactVersionRef::new(id(SELECTION_POLICY_REVISION_ID), digest('d')).unwrap();
    let selection_deployment_payload = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: selection_revision.clone(),
            applicability_digest: digest('f'),
            qualification_evidence: qualification_evidence.clone(),
        }),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id, environment,
            bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(TENANT_ID)
    .bind(SELECTION_POLICY_DEPLOYMENT_ID)
    .bind(SELECTION_POLICY_ID)
    .bind(SELECTION_POLICY_REVISION_ID)
    .bind(&selection_deployment_payload.digest)
    .bind(selection_deployment_payload.schema_version)
    .bind(&selection_deployment_payload.value)
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    let selection_binding = ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(
            id(SELECTION_POLICY_DEPLOYMENT_ID),
            selection_deployment_payload.digest.parse().unwrap(),
        )
        .unwrap(),
        revision: selection_revision,
    };
    let child_closure = AgentDeploymentClosure {
        interface: ExactVersionRef::new(id(CHILD_INTERFACE_ID), digest('e')).unwrap(),
        plan: ExactVersionRef::new(id(CHILD_PLAN_ID), child_plan_digest).unwrap(),
        entry_node_id: "entry".to_owned(),
        entry_node_kind: PlanNodeKind::TimerWait,
        slots: vec![],
        policies: vec![policy_binding.clone()],
        execution_profile: policy_binding.clone(),
    };
    let child_deployment_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(child_closure)).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id, environment,
            bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(TENANT_ID)
    .bind(CHILD_DEPLOYMENT_ID)
    .bind(CHILD_AGENT_ID)
    .bind(CHILD_PLAN_ID)
    .bind(&child_deployment_payload.digest)
    .bind(child_deployment_payload.schema_version)
    .bind(&child_deployment_payload.value)
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    let child_deployment = ExactDeploymentRef::new(
        id(CHILD_DEPLOYMENT_ID),
        child_deployment_payload.digest.parse().unwrap(),
    )
    .unwrap();
    let closure = AgentDeploymentClosure {
        interface: ExactVersionRef::new(id(INTERFACE_ID), digest('b')).unwrap(),
        plan: ExactVersionRef::new(id(PLAN_ID), typed_plan_digest).unwrap(),
        entry_node_id: "entry".to_owned(),
        entry_node_kind: insight_platform_contracts::PlanNodeKind::TimerWait,
        slots: vec![FrozenSlotBinding {
            slot_id: "child_worker".to_owned(),
            requirement_digest: digest('c'),
            target: FrozenSlotTarget::ChildAgent {
                candidates: vec![child_deployment],
                selection_policy: selection_binding,
            },
            binding_digest: digest('2'),
        }],
        policies: vec![policy_binding.clone()],
        execution_profile: policy_binding,
    };
    let deployment_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(closure.clone())).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id, environment,
            bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(TENANT_ID)
    .bind(DEPLOYMENT_ID)
    .bind(AGENT_ID)
    .bind(PLAN_ID)
    .bind(&deployment_payload.digest)
    .bind(deployment_payload.schema_version)
    .bind(&deployment_payload.value)
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(TENANT_ID)
    .bind(CHILD_AGENT_ID)
    .bind(CHILD_DEPLOYMENT_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(TENANT_ID)
    .bind(AGENT_ID)
    .bind(DEPLOYMENT_ID)
    .execute(pool)
    .await
    .unwrap();

    let mut security = repository.begin_security_transaction().await.unwrap();
    security
        .bind_tenant_scheduling_policy(BindTenantSchedulingPolicy {
            audit: audit("6011"),
            expected_tenant_version: 1,
            policy: policy_deployment,
        })
        .await
        .unwrap();
    security.commit().await.unwrap();
    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: TENANT_ID.to_owned(),
            quota_account_id: QUOTA_ACCOUNT_ID.to_owned(),
            scope_kind: "tenant".to_owned(),
            scope_id: TENANT_ID.to_owned(),
            work_class: WorkClass::Orchestration.as_str().to_owned(),
            metric: "concurrent_jobs".to_owned(),
            limit_value: 1,
            payload: TypedPayload::empty(1).unwrap(),
        })
        .await
        .unwrap();

    RunBindingsSnapshot::build(
        ExactDeploymentRef::new(
            id(DEPLOYMENT_ID),
            deployment_payload.digest.parse().unwrap(),
        )
        .unwrap(),
        PrincipalSnapshot::build(
            id(TENANT_ID),
            id(PRINCIPAL_ID),
            PrincipalKind::AgentRunner,
            PermissionSet::new(vec![
                Permission::AgentRun,
                Permission::InteractionRespond,
                Permission::RuntimeControl,
                Permission::TenantManage,
            ])
            .unwrap(),
            1,
            1,
            1,
        )
        .unwrap(),
        &closure,
    )
    .unwrap()
}

async fn admit_run(repository: &PgRepository, bindings: RunBindingsSnapshot) -> RunRecord {
    let input = json!({"question": "coordinator"});
    let input_bytes = canonical_json(&input).unwrap();
    let input_digest: Sha256Digest = canonical_digest(&input).unwrap().parse().unwrap();
    let input_artifact = ArtifactRef::new(
        id(INPUT_ARTIFACT_ID),
        input_digest.clone(),
        u64::try_from(input_bytes.len()).unwrap(),
        "application/json",
        DataClassification::Internal,
        None,
    )
    .unwrap();
    insert_ready_artifact(
        repository.pool(),
        &id(TENANT_ID),
        &id(PRINCIPAL_ID),
        &id(POLICY_REVISION_ID),
        &input_artifact,
        "run_input",
    )
    .await;
    let command = AdmitRun {
        audit: audit("6021"),
        admission_scope_id: id(DEPLOYMENT_ID),
        run_id: id(RUN_ID),
        agent_deployment_id: id(DEPLOYMENT_ID),
        root_scope_id: id(SCOPE_ID),
        entry_node_execution_id: id(NODE_ID),
        orchestration_job_id: id(JOB_ID),
        entry_plan_node_key: PlanNodeKey::new("entry".to_owned()).unwrap(),
        entry_node_kind: PlanNodeKind::TimerWait,
        bindings,
        input: RunInputValue {
            value_id: id(VALUE_ID),
            classification: DataClassification::Internal,
            schema_digest: agent_schema().canonical_digest,
            content_digest: input_digest,
            value: ValueRef::Artifact {
                artifact: input_artifact,
            },
        },
        deadline: Utc::now() + ChronoDuration::minutes(5),
        inline_limits: JsonLimits::CONTRACT_FIXTURE,
        attempt_limit: 3,
        retry_backoff_milliseconds: 100,
    };
    let mut transaction = repository.begin_run_transaction().await.unwrap();
    let outcome = transaction.admit_run(command).await.unwrap();
    transaction.commit().await.unwrap();
    match outcome {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh fixture unexpectedly replayed Run admission"),
    }
}

const PHASE2_TENANT_COUNT: usize = 5;
const PHASE2_RUNS_PER_TENANT: usize = 10;
const PHASE2_CHILD_COUNT: usize = 4;
const PHASE2_BARRIER_CLASS: i32 = 24_002;
const PHASE2_BARRIER_OBJECT: i32 = 50;
const PHASE2_FIXTURE_LOCK_OBJECT: i32 = 51;

async fn acquire_phase2_fixture_lock(database_url: &str) -> PoolConnection<Postgres> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .unwrap();
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1, $2)")
        .bind(PHASE2_BARRIER_CLASS)
        .bind(PHASE2_FIXTURE_LOCK_OBJECT)
        .execute(&mut *connection)
        .await
        .unwrap();
    connection
}

struct RecordingClaimExecutor {
    claimed: AtomicU64,
}

impl RecordingClaimExecutor {
    fn new() -> Self {
        Self {
            claimed: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl OrchestrationJobExecutor for RecordingClaimExecutor {
    async fn execute(&self, _job: ActiveOrchestrationJob) -> ExecutionDisposition {
        self.claimed.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(25)).await;
        ExecutionDisposition::GenerationAbandoned
    }
}

#[test]
fn phase2_claim_worker_process_entry() {
    if std::env::var("PLATFORM_PHASE2_CLAIM_CHILD").as_deref() != Ok("1") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL").unwrap();
        let tenant_ids = std::env::var("PLATFORM_PHASE2_TENANT_IDS")
            .expect("PLATFORM_PHASE2_TENANT_IDS is required for a claim child")
            .split(',')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert_eq!(tenant_ids.len(), PHASE2_TENANT_COUNT);
        let profile = checked_in_hard_limit_profile();
        let worker_id = fresh_id(ResourceKind::WorkerProcessGeneration);
        let bulkheads = PostgresConnectionBulkheads::connect(
            &database_url,
            PostgresConnectionBulkheadConfig {
                worker_role: format!("orchestration.q1-{}", worker_id.uuid()),
                business_max_connections: 2,
                critical_control_reserved_connections: 1,
                process_connection_budget: 3,
                acquire_timeout: Duration::from_secs(5),
                statement_timeout: Duration::from_secs(30),
                idle_timeout: Some(Duration::from_secs(30)),
                max_lifetime: Some(Duration::from_secs(300)),
            },
            &profile,
        )
        .await
        .unwrap();

        let mut barrier = bulkheads.critical_control_pool().acquire().await.unwrap();
        sqlx::query("SELECT pg_advisory_lock_shared($1, $2)")
            .bind(PHASE2_BARRIER_CLASS)
            .bind(PHASE2_BARRIER_OBJECT)
            .execute(&mut *barrier)
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_unlock_shared($1, $2)")
            .bind(PHASE2_BARRIER_CLASS)
            .bind(PHASE2_BARRIER_OBJECT)
            .execute(&mut *barrier)
            .await
            .unwrap();
        drop(barrier);

        let pools = LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: 1,
                worker_role: "orchestration.q1".to_owned(),
                work_class: WorkClass::Orchestration,
                adapter_runtime_digest: digest('e'),
                protocol_version: 1,
                max_concurrency: 2,
                critical_control_reserved_slots: 1,
            },
            worker_id.clone(),
        )
        .unwrap();
        let executor = Arc::new(RecordingClaimExecutor::new());
        let running = WorkCoordinator::new(
            Arc::new(bulkheads.business_repository()),
            Arc::clone(&executor),
            Arc::new(UuidCoordinatorIdentityFactory),
            pools,
            OrchestrationCoordinatorConfig::from_profile(
                &profile,
                CoordinatorTiming {
                    coalesce_window: Duration::from_millis(1),
                    safety_scan_interval: Duration::from_millis(25),
                    safety_scan_jitter: Duration::ZERO,
                    claim_failure_backoff: Duration::from_millis(2),
                    drain_grace: Duration::from_secs(2),
                },
            )
            .unwrap(),
        )
        .unwrap()
        .spawn();

        tokio::time::timeout(Duration::from_secs(20), async {
            let mut consecutive_empty_observations = 0_u8;
            loop {
                let ready: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM insight_platform.jobs WHERE work_class = 'orchestration' AND state = 'ready' AND tenant_id = ANY($1)",
                )
                .bind(&tenant_ids)
                .fetch_one(bulkheads.critical_control_pool())
                .await
                .unwrap();
                if ready == 0 {
                    consecutive_empty_observations += 1;
                    if consecutive_empty_observations == 3 {
                        break;
                    }
                } else {
                    consecutive_empty_observations = 0;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("multi-process claim fixture did not drain the ready set");

        let exit = running.shutdown().await.unwrap();
        assert_eq!(exit.abandoned_on_drain_timeout, 0);
        println!(
            "PHASE2_CHILD_RESULT worker_id={} claimed={}",
            worker_id,
            executor.claimed.load(Ordering::Relaxed)
        );
        bulkheads.close().await;
    });
}

#[test]
fn q1_fifty_runs_use_multiple_processes_and_preserve_database_fairness() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
            eprintln!("PLATFORM_TEST_DATABASE_URL is unset; Phase 2 Q1 fixture skipped");
            return;
        };
        if std::env::var("PLATFORM_PHASE2_CLAIM_CHILD").as_deref() == Ok("1") {
            return;
        }
        let _fixture_lock = acquire_phase2_fixture_lock(&database_url).await;

        let profile = checked_in_hard_limit_profile();
        let bulkheads = PostgresConnectionBulkheads::connect(
            &database_url,
            PostgresConnectionBulkheadConfig {
                worker_role: "orchestration.q1-parent".to_owned(),
                business_max_connections: 12,
                critical_control_reserved_connections: 2,
                process_connection_budget: 14,
                acquire_timeout: Duration::from_secs(5),
                statement_timeout: Duration::from_secs(30),
                idle_timeout: Some(Duration::from_secs(30)),
                max_lifetime: Some(Duration::from_secs(300)),
            },
            &profile,
        )
        .await
        .unwrap();
        verify_schema(bulkheads.business_pool()).await.unwrap();
        let repository = bulkheads.business_repository();
        let mut tenants = BTreeSet::new();
        for _ in 0..PHASE2_TENANT_COUNT {
            let (tenant_id, bindings) = seed_capacity_tenant(&repository).await;
            assert!(tenants.insert(tenant_id.to_string()));
            for _ in 0..PHASE2_RUNS_PER_TENANT {
                admit_capacity_run(&repository, &tenant_id, bindings.clone()).await;
            }
        }

        let total_runs = i64::try_from(PHASE2_TENANT_COUNT * PHASE2_RUNS_PER_TENANT).unwrap();
        let tenant_ids = tenants.iter().cloned().collect::<Vec<_>>();
        let ready_before: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM insight_platform.jobs WHERE work_class = 'orchestration' AND state = 'ready' AND tenant_id = ANY($1)",
        )
        .bind(&tenant_ids)
        .fetch_one(bulkheads.business_pool())
        .await
        .unwrap();
        assert_eq!(ready_before, total_runs);

        let mut barrier = bulkheads.critical_control_pool().acquire().await.unwrap();
        sqlx::query("SELECT pg_advisory_lock($1, $2)")
            .bind(PHASE2_BARRIER_CLASS)
            .bind(PHASE2_BARRIER_OBJECT)
            .execute(&mut *barrier)
            .await
            .unwrap();

        let executable = std::env::current_exe().unwrap();
        let child_tenant_ids = tenant_ids.join(",");
        let mut children = Vec::with_capacity(PHASE2_CHILD_COUNT);
        for ordinal in 0..PHASE2_CHILD_COUNT {
            let child = Command::new(&executable)
                .arg("--exact")
                .arg("phase2_claim_worker_process_entry")
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env("PLATFORM_PHASE2_CLAIM_CHILD", "1")
                .env("PLATFORM_PHASE2_CHILD_ORDINAL", ordinal.to_string())
                .env("PLATFORM_PHASE2_TENANT_IDS", &child_tenant_ids)
                .env("PLATFORM_TEST_DATABASE_URL", &database_url)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            children.push(child);
        }

        tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                let waiters: i64 = sqlx::query_scalar(
                    r#"
                    SELECT count(*)
                    FROM pg_locks
                    WHERE locktype = 'advisory'
                      AND classid = $1::oid AND objid = $2::oid
                      AND mode = 'ShareLock' AND NOT granted
                    "#,
                )
                .bind(PHASE2_BARRIER_CLASS)
                .bind(PHASE2_BARRIER_OBJECT)
                .fetch_one(bulkheads.business_pool())
                .await
                .unwrap();
                if waiters == i64::try_from(PHASE2_CHILD_COUNT).unwrap() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("claim worker processes did not reach the PostgreSQL start barrier");
        let unlocked: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1, $2)")
            .bind(PHASE2_BARRIER_CLASS)
            .bind(PHASE2_BARRIER_OBJECT)
            .fetch_one(&mut *barrier)
            .await
            .unwrap();
        assert!(unlocked);
        drop(barrier);

        let mut waits = tokio::task::JoinSet::new();
        for child in children {
            waits.spawn(async move {
                tokio::task::spawn_blocking(move || child.wait_with_output())
                    .await
                    .unwrap()
            });
        }
        let outputs = tokio::time::timeout(Duration::from_secs(60), async {
            let mut outputs = Vec::with_capacity(PHASE2_CHILD_COUNT);
            while let Some(joined) = waits.join_next().await {
                outputs.push(joined.unwrap().unwrap());
            }
            outputs
        })
        .await
        .expect("claim worker processes did not finish");
        assert_eq!(outputs.len(), PHASE2_CHILD_COUNT);
        for output in outputs {
            assert!(
                output.status.success(),
                "claim child failed: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("PHASE2_CHILD_RESULT"));
        }

        let leased: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM insight_platform.jobs WHERE work_class = 'orchestration' AND state = 'leased' AND tenant_id = ANY($1)",
        )
        .bind(&tenant_ids)
        .fetch_one(bulkheads.business_pool())
        .await
        .unwrap();
        let distinct_workers: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT worker_id) FROM insight_platform.jobs WHERE work_class = 'orchestration' AND state = 'leased' AND tenant_id = ANY($1)",
        )
        .bind(&tenant_ids)
        .fetch_one(bulkheads.business_pool())
        .await
        .unwrap();
        assert_eq!(leased, total_runs);
        assert!(distinct_workers >= 2, "one process monopolized every claim");

        let active_work: i64 = sqlx::query_scalar(
            "SELECT COALESCE(sum(active_work_count), 0)::bigint FROM insight_platform.runs WHERE tenant_id = ANY($1)",
        )
        .bind(&tenant_ids)
        .fetch_one(bulkheads.business_pool())
        .await
        .unwrap();
        let reserved: i64 = sqlx::query_scalar(
            "SELECT COALESCE(sum(reserved_value), 0)::bigint FROM insight_platform.quota_accounts WHERE work_class = 'orchestration' AND tenant_id = ANY($1)",
        )
        .bind(&tenant_ids)
        .fetch_one(bulkheads.business_pool())
        .await
        .unwrap();
        assert_eq!(active_work, total_runs);
        assert_eq!(reserved, total_runs);

        let scheduler_payload: serde_json::Value = sqlx::query_scalar(
            "SELECT payload FROM insight_platform.scheduler_state WHERE work_class = 'orchestration'",
        )
        .fetch_one(bulkheads.business_pool())
        .await
        .unwrap();
        let tenant_states = scheduler_payload["tenants"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|tenant| {
                tenant["tenant_id"]
                    .as_str()
                    .is_some_and(|tenant_id| tenants.contains(tenant_id))
            })
            .collect::<Vec<_>>();
        assert_eq!(tenant_states.len(), PHASE2_TENANT_COUNT);
        assert!(tenant_states.into_iter().all(|tenant| {
            tenant["successful_claims"].as_u64()
                == Some(u64::try_from(PHASE2_RUNS_PER_TENANT).unwrap())
        }));

        assert_cross_work_class_database_sli(&database_url, &profile).await;
        bulkheads.close().await;
    });
}

async fn assert_cross_work_class_database_sli(
    database_url: &str,
    profile: &insight_platform_contracts::HardLimitProfile,
) {
    let config = |worker_role: &str| PostgresConnectionBulkheadConfig {
        worker_role: worker_role.to_owned(),
        business_max_connections: 1,
        critical_control_reserved_connections: 1,
        process_connection_budget: 2,
        acquire_timeout: Duration::from_millis(100),
        statement_timeout: Duration::from_secs(2),
        idle_timeout: Some(Duration::from_secs(30)),
        max_lifetime: Some(Duration::from_secs(300)),
    };
    let saturated = PostgresConnectionBulkheads::connect(
        database_url,
        config("orchestration.q1-saturated"),
        profile,
    )
    .await
    .unwrap();
    let independent =
        PostgresConnectionBulkheads::connect(database_url, config("sandbox.q1-probe"), profile)
            .await
            .unwrap();
    let held_business_connection = saturated.business_pool().acquire().await.unwrap();
    assert!(tokio::time::timeout(
        Duration::from_millis(25),
        saturated.business_pool().acquire()
    )
    .await
    .is_err());

    let mut latencies = Vec::with_capacity(20);
    for _ in 0..20 {
        let started = Instant::now();
        let value: i32 = tokio::time::timeout(
            Duration::from_millis(250),
            sqlx::query_scalar("SELECT 1").fetch_one(independent.business_pool()),
        )
        .await
        .expect("independent WorkClass probe exceeded the Q1 admission p95 threshold")
        .unwrap();
        assert_eq!(value, 1);
        latencies.push(started.elapsed());
    }
    latencies.sort_unstable();
    assert!(latencies[18] <= Duration::from_millis(250));

    let control_value: i32 = tokio::time::timeout(
        Duration::from_millis(250),
        sqlx::query_scalar("SELECT 1").fetch_one(saturated.critical_control_pool()),
    )
    .await
    .expect("critical-control reserve exceeded the Q1 admission p95 threshold")
    .unwrap();
    assert_eq!(control_value, 1);
    drop(held_business_connection);
    saturated.close().await;
    independent.close().await;
}

async fn seed_capacity_tenant(repository: &PgRepository) -> (ResourceId, RunBindingsSnapshot) {
    let tenant_id = fresh_id(ResourceKind::Tenant);
    let principal_id = fresh_id(ResourceKind::Principal);
    let policy_id = fresh_id(ResourceKind::Policy);
    let policy_revision_id = fresh_id(ResourceKind::PolicyRevision);
    let agent_id = fresh_id(ResourceKind::Agent);
    let interface_id = fresh_id(ResourceKind::AgentInterfaceRevision);
    let plan_id = fresh_id(ResourceKind::AgentPlanRevision);
    let deployment_id = fresh_id(ResourceKind::AgentDeployment);
    let quota_account_id = fresh_id(ResourceKind::QuotaAccount);

    repository
        .create_tenant(NewTenant {
            tenant_id: tenant_id.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    repository
        .create_principal(NewPrincipal {
            principal_id: principal_id.clone(),
            authentication_authority_digest: digest('1'),
            subject_digest: fresh_digest(),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    let permissions = PermissionSet::new(vec![
        Permission::AgentRun,
        Permission::RuntimeControl,
        Permission::TenantManage,
    ])
    .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant_id.clone(),
            principal_id: principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: permissions.clone(),
            },
        })
        .await
        .unwrap();

    let pool = repository.pool();
    let empty = TypedPayload::empty(1).unwrap();
    for (resource_id, resource_kind) in [(&policy_id, "policy"), (&agent_id, "agent")] {
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resources (
                tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, $3, 'active', 'enabled', $4, $5, $6)
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(resource_id.to_string())
        .bind(resource_kind)
        .bind(empty.schema_version)
        .bind(&empty.value)
        .bind(&empty.digest)
        .execute(pool)
        .await
        .unwrap();
    }

    let scheduling = SchedulingPolicyDocument {
        version: 1,
        weight: 1,
        burst: 1,
        aging_rounds: 2,
    };
    let qualification_evidence = ArtifactRef::new(
        fresh_id(ResourceKind::Artifact),
        digest('3'),
        1,
        "application/json",
        DataClassification::Internal,
        Some("scheduling-policy.json".to_owned()),
    )
    .unwrap();
    let policy_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                authoring_package: AuthoringPackage {
                    artifact: qualification_evidence.clone(),
                    manifest_digest: digest('4'),
                },
                contract_digest: digest('5'),
                dependency_versions: vec![],
                policy_versions: vec![],
                policy_kind: PolicyKind::Scheduling,
                rules_digest: scheduling.canonical_digest().unwrap(),
                selection: None,
                scheduling: Some(scheduling),
                retention: None,
                model_safety: None,
                model_budget: None,
                model_public_projection: None,
                mcp_protocol: None,
                mcp_auth: None,
                sandbox_isolation: None,
                sandbox_resource: None,
                sandbox_network: None,
                sandbox_artifact_io: None,
                sandbox_secret_resolution: None,
            })),
            validation: ValidationSummary {
                validator_digest: digest('6'),
                validated_draft_digest: digest('7'),
                dependency_closure_digest: digest('8'),
                security_evidence_digest: digest('9'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    for (version_id, resource_id, kind, content_digest, payload) in [
        (
            &policy_revision_id,
            &policy_id,
            "policy_revision",
            digest('a'),
            &policy_payload,
        ),
        (
            &interface_id,
            &agent_id,
            "agent_interface_revision",
            digest('b'),
            &empty,
        ),
        (
            &plan_id,
            &agent_id,
            "agent_plan_revision",
            digest('c'),
            &empty,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resource_versions (
                tenant_id, resource_version_id, resource_id, resource_version_kind,
                revision_no, content_digest, payload_schema_version, payload,
                payload_digest, created_by
            ) VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(version_id.to_string())
        .bind(resource_id.to_string())
        .bind(kind)
        .bind(content_digest.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(principal_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "UPDATE insight_platform.resources SET active_version_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(policy_id.to_string())
    .bind(policy_revision_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    insert_ready_artifact(
        pool,
        &tenant_id,
        &principal_id,
        &policy_revision_id,
        &qualification_evidence,
        "qualification",
    )
    .await;

    let policy = ExactVersionRef::new(policy_revision_id.clone(), digest('a')).unwrap();
    let policy_deployment = seed_policy_deployment(
        pool,
        &tenant_id,
        &principal_id,
        &policy_id,
        fresh_id(ResourceKind::PolicyDeployment),
        policy.clone(),
        qualification_evidence,
    )
    .await;
    let policy_binding = ExactPolicyBinding {
        deployment: policy_deployment.clone(),
        revision: policy.clone(),
    };
    let closure = AgentDeploymentClosure {
        interface: ExactVersionRef::new(interface_id, digest('b')).unwrap(),
        plan: ExactVersionRef::new(plan_id.clone(), digest('c')).unwrap(),
        entry_node_id: "start".to_owned(),
        entry_node_kind: insight_platform_contracts::PlanNodeKind::Start,
        slots: vec![],
        policies: vec![policy_binding.clone()],
        execution_profile: policy_binding,
    };
    let deployment_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(closure.clone())).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id, environment,
            bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(deployment_id.to_string())
    .bind(agent_id.to_string())
    .bind(plan_id.to_string())
    .bind(&deployment_payload.digest)
    .bind(deployment_payload.schema_version)
    .bind(&deployment_payload.value)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(agent_id.to_string())
    .bind(deployment_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let mut security = repository.begin_security_transaction().await.unwrap();
    security
        .bind_tenant_scheduling_policy(BindTenantSchedulingPolicy {
            audit: fresh_audit(&tenant_id, &principal_id),
            expected_tenant_version: 1,
            policy: policy_deployment,
        })
        .await
        .unwrap();
    security.commit().await.unwrap();
    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: tenant_id.to_string(),
            quota_account_id: quota_account_id.to_string(),
            scope_kind: "tenant".to_owned(),
            scope_id: tenant_id.to_string(),
            work_class: WorkClass::Orchestration.as_str().to_owned(),
            metric: "concurrent_jobs".to_owned(),
            limit_value: i64::try_from(PHASE2_RUNS_PER_TENANT).unwrap(),
            payload: TypedPayload::empty(1).unwrap(),
        })
        .await
        .unwrap();

    let bindings = RunBindingsSnapshot::build(
        ExactDeploymentRef::new(deployment_id, deployment_payload.digest.parse().unwrap()).unwrap(),
        PrincipalSnapshot::build(
            tenant_id.clone(),
            principal_id,
            PrincipalKind::AgentRunner,
            permissions,
            1,
            1,
            1,
        )
        .unwrap(),
        &closure,
    )
    .unwrap();
    (tenant_id, bindings)
}

async fn admit_capacity_run(
    repository: &PgRepository,
    tenant_id: &ResourceId,
    bindings: RunBindingsSnapshot,
) {
    let principal_id = bindings.principal.principal_id.clone();
    let input = json!({"question": "phase2-capacity"});
    let command = AdmitRun {
        audit: fresh_audit(tenant_id, &principal_id),
        admission_scope_id: bindings.agent.deployment_id.clone(),
        run_id: fresh_id(ResourceKind::Run),
        agent_deployment_id: bindings.agent.deployment_id.clone(),
        root_scope_id: fresh_id(ResourceKind::ScopeInstance),
        entry_node_execution_id: fresh_id(ResourceKind::NodeExecution),
        orchestration_job_id: fresh_id(ResourceKind::Job),
        entry_plan_node_key: PlanNodeKey::new("entry".to_owned()).unwrap(),
        entry_node_kind: PlanNodeKind::Start,
        bindings,
        input: RunInputValue {
            value_id: fresh_id(ResourceKind::RunValue),
            classification: DataClassification::Internal,
            schema_digest: digest('d'),
            content_digest: canonical_digest(&input).unwrap().parse().unwrap(),
            value: ValueRef::Inline { value: input },
        },
        deadline: Utc::now() + ChronoDuration::minutes(5),
        inline_limits: JsonLimits::CONTRACT_FIXTURE,
        attempt_limit: 3,
        retry_backoff_milliseconds: 100,
    };
    let mut transaction = repository.begin_run_transaction().await.unwrap();
    assert!(matches!(
        transaction.admit_run(command).await.unwrap(),
        CommandOutcome::Applied(_)
    ));
    transaction.commit().await.unwrap();
}

fn fresh_id(kind: ResourceKind) -> ResourceId {
    UuidCoordinatorIdentityFactory
        .new_resource_id(kind)
        .unwrap()
}

fn fresh_digest() -> Sha256Digest {
    UuidCoordinatorIdentityFactory
        .new_lease_token_digest()
        .unwrap()
}

fn fresh_audit(tenant_id: &ResourceId, principal_id: &ResourceId) -> CommandAudit {
    CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: tenant_id.clone(),
        principal_id: principal_id.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: fresh_id(ResourceKind::Receipt),
        event_id: fresh_id(ResourceKind::Event),
        outbox_id: fresh_id(ResourceKind::OutboxEvent),
        idempotency_key_digest: fresh_digest(),
        request_digest: fresh_digest(),
        receipt_expires_at: Utc::now() + ChronoDuration::hours(1),
    }
}
