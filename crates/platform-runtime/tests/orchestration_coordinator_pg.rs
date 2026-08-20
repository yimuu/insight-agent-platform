use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, AgentDeploymentClosure, ArtifactRef,
    AuthoringPackage, CommandAudit, CommandOutcome, DataClassification, DeploymentClosure,
    ExactDeploymentRef, ExactVersionRef, JsonLimits, Permission, PermissionSet, PlanNodeKind,
    PolicyKind, PolicyResourceSpec, PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot,
    PublishedVersionPayload, ResourceDocument, ResourceId, ResourceKind, RunBindingsSnapshot,
    SchedulingPolicyDocument, Sha256Digest, TenantConfig, TenantPrincipalPayload,
    ValidationSummary, ValueRef, WorkClass, WorkerManifest,
};
use insight_platform_orchestrator::{AdmitRun, PlanNodeKey, RunInputValue};
use insight_platform_postgres::{
    repository::{
        NewPrincipal, NewQuotaAccount, NewTenant, NewTenantPrincipal, OrchestrationYield,
        OrchestrationYieldMutationIds, PgRepository, RunRecord, SafetyScanShard, TypedPayload,
        YieldOrchestrationJob, MAX_ORCHESTRATION_QUOTA_LINES,
    },
    verify_schema,
};
use insight_platform_runtime::postgres::{
    PostgresConnectionBulkheadConfig, PostgresConnectionBulkheads,
};
use insight_platform_runtime::{
    ActiveOrchestrationJob, CoordinatorIdentityFactory, CoordinatorTiming, ExecutionDisposition,
    GenerationHandlerDisposition, GenerationHandlerError, GenerationHandoffReason,
    LeaseFencedOrchestrationExecutor, OrchestrationCoordinatorConfig, OrchestrationExecutorConfig,
    OrchestrationExecutorTiming, OrchestrationJobExecutor, OrchestrationSafetyConfig,
    OrchestrationSafetyDriver, SafetyDriverTiming, StartedOrchestrationJob,
    StartedOrchestrationJobHandler, UuidCoordinatorIdentityFactory, WorkCoordinator,
};
use insight_platform_security::BindTenantSchedulingPolicy;
use insight_platform_worker::{ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPools};
use serde_json::json;
use sqlx::Row;
use std::{
    collections::BTreeSet,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const TENANT_ID: &str = "ten_0198f1c5-0787-75e1-a9e8-d95ca0f36001";
const PRINCIPAL_ID: &str = "prn_0198f1c5-0787-75e1-a9e8-d95ca0f36002";
const POLICY_ID: &str = "pol_0198f1c5-0787-75e1-a9e8-d95ca0f36003";
const POLICY_REVISION_ID: &str = "prev_0198f1c5-0787-75e1-a9e8-d95ca0f36004";
const AGENT_ID: &str = "agt_0198f1c5-0787-75e1-a9e8-d95ca0f36005";
const INTERFACE_ID: &str = "aif_0198f1c5-0787-75e1-a9e8-d95ca0f36006";
const PLAN_ID: &str = "arev_0198f1c5-0787-75e1-a9e8-d95ca0f36007";
const DEPLOYMENT_ID: &str = "adep_0198f1c5-0787-75e1-a9e8-d95ca0f36008";
const WORKER_ID: &str = "wrk_0198f1c5-0787-75e1-a9e8-d95ca0f36009";
const QUOTA_ACCOUNT_ID: &str = "qac_0198f1c5-0787-75e1-a9e8-d95ca0f3600a";
const RUN_ID: &str = "run_0198f1c5-0787-75e1-a9e8-d95ca0f3600b";
const SCOPE_ID: &str = "scp_0198f1c5-0787-75e1-a9e8-d95ca0f3600c";
const NODE_ID: &str = "nex_0198f1c5-0787-75e1-a9e8-d95ca0f3600d";
const JOB_ID: &str = "job_0198f1c5-0787-75e1-a9e8-d95ca0f3600e";
const VALUE_ID: &str = "rval_0198f1c5-0787-75e1-a9e8-d95ca0f3600f";

fn id(value: &str) -> ResourceId {
    value.parse().unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn audit(suffix: &str) -> CommandAudit {
    CommandAudit {
        tenant_id: id(TENANT_ID),
        principal_id: id(PRINCIPAL_ID),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: id(&format!("rcp_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix}")),
        event_id: id(&format!("evt_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix}")),
        outbox_id: id(&format!("out_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix}")),
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
        let mut profile = checked_in_hard_limit_profile();
        profile.run_scheduler.heartbeat_milliseconds.q1_default = 100;
        profile.run_scheduler.lease_milliseconds.q1_default = 500;
        profile.control_data.recovery_batch.q1_default = 1;
        profile.validate().unwrap();
        let connection_config = PostgresConnectionBulkheadConfig {
            worker_role: "orchestration.pg-fixture".to_owned(),
            business_max_connections: 1,
            critical_control_reserved_connections: 1,
            process_connection_budget: 2,
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
        tokio::time::sleep(Duration::from_millis(150)).await;
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
        let safety_exit = safety.shutdown().await.unwrap();
        assert!(safety_exit.mutations >= 1);
        assert!(safety_exit.scan_attempts >= 4);
        drop(local_business_saturation);
        drop(_business_saturation);
        bulkheads.close().await;
    });
}

async fn seed_authorities(repository: &PgRepository) -> RunBindingsSnapshot {
    repository
        .create_tenant(NewTenant {
            tenant_id: TENANT_ID.to_owned(),
            state: "active".to_owned(),
            config: TenantConfig {
                scheduling_policy: None,
            },
        })
        .await
        .unwrap();
    repository
        .create_principal(NewPrincipal {
            principal_id: id(PRINCIPAL_ID),
            authentication_authority_digest: digest('1'),
            subject_digest: digest('2'),
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
    for (resource_id, resource_kind) in [(POLICY_ID, "policy"), (AGENT_ID, "agent")] {
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
            document: ResourceDocument::Policy(PolicyResourceSpec {
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
                scheduling: Some(scheduling),
                retention: None,
                mcp_protocol: None,
                mcp_auth: None,
                sandbox_isolation: None,
                sandbox_resource: None,
                sandbox_network: None,
                sandbox_artifact_io: None,
                sandbox_secret_resolution: None,
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
    for (version_id, kind, content_digest, payload) in [
        (
            POLICY_REVISION_ID,
            "policy_revision",
            digest('a'),
            &policy_payload,
        ),
        (
            INTERFACE_ID,
            "agent_interface_revision",
            digest('b'),
            &empty,
        ),
        (PLAN_ID, "agent_plan_revision", digest('c'), &empty),
    ] {
        let resource_id = if version_id == POLICY_REVISION_ID {
            POLICY_ID
        } else {
            AGENT_ID
        };
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

    let policy = ExactVersionRef::new(id(POLICY_REVISION_ID), digest('a')).unwrap();
    let closure = AgentDeploymentClosure {
        interface: ExactVersionRef::new(id(INTERFACE_ID), digest('b')).unwrap(),
        plan: ExactVersionRef::new(id(PLAN_ID), digest('c')).unwrap(),
        slots: vec![],
        policies: vec![policy.clone()],
        execution_profile: policy.clone(),
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
            policy: policy.clone(),
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
    let command = AdmitRun {
        audit: audit("6021"),
        run_id: id(RUN_ID),
        agent_deployment_id: id(DEPLOYMENT_ID),
        root_scope_id: id(SCOPE_ID),
        entry_node_execution_id: id(NODE_ID),
        orchestration_job_id: id(JOB_ID),
        entry_plan_node_key: PlanNodeKey::new("entry".to_owned()).unwrap(),
        entry_node_kind: PlanNodeKind::Start,
        bindings,
        input: RunInputValue {
            value_id: id(VALUE_ID),
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
                    "SELECT count(*) FROM insight_platform.jobs WHERE work_class = 'orchestration' AND state = 'ready'",
                )
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
        let ready_before: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM insight_platform.jobs WHERE work_class = 'orchestration' AND state = 'ready'",
        )
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
        let mut children = Vec::with_capacity(PHASE2_CHILD_COUNT);
        for ordinal in 0..PHASE2_CHILD_COUNT {
            let child = Command::new(&executable)
                .arg("--exact")
                .arg("phase2_claim_worker_process_entry")
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env("PLATFORM_PHASE2_CLAIM_CHILD", "1")
                .env("PLATFORM_PHASE2_CHILD_ORDINAL", ordinal.to_string())
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
            "SELECT count(*) FROM insight_platform.jobs WHERE work_class = 'orchestration' AND state = 'leased'",
        )
        .fetch_one(bulkheads.business_pool())
        .await
        .unwrap();
        let distinct_workers: i64 = sqlx::query_scalar(
            "SELECT count(DISTINCT worker_id) FROM insight_platform.jobs WHERE work_class = 'orchestration' AND state = 'leased'",
        )
        .fetch_one(bulkheads.business_pool())
        .await
        .unwrap();
        assert_eq!(leased, total_runs);
        assert!(distinct_workers >= 2, "one process monopolized every claim");

        let active_work: i64 = sqlx::query_scalar(
            "SELECT COALESCE(sum(active_work_count), 0)::bigint FROM insight_platform.runs",
        )
        .fetch_one(bulkheads.business_pool())
        .await
        .unwrap();
        let reserved: i64 = sqlx::query_scalar(
            "SELECT COALESCE(sum(reserved_value), 0)::bigint FROM insight_platform.quota_accounts WHERE work_class = 'orchestration'",
        )
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
        let tenant_states = scheduler_payload["tenants"].as_array().unwrap();
        assert_eq!(tenant_states.len(), PHASE2_TENANT_COUNT);
        assert!(tenant_states.iter().all(|tenant| {
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
            config: TenantConfig {
                scheduling_policy: None,
            },
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
    let policy_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: ResourceDocument::Policy(PolicyResourceSpec {
                authoring_package: AuthoringPackage {
                    artifact: ArtifactRef::new(
                        fresh_id(ResourceKind::Artifact),
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
                scheduling: Some(scheduling),
                retention: None,
                mcp_protocol: None,
                mcp_auth: None,
                sandbox_isolation: None,
                sandbox_resource: None,
                sandbox_network: None,
                sandbox_artifact_io: None,
                sandbox_secret_resolution: None,
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

    let policy = ExactVersionRef::new(policy_revision_id.clone(), digest('a')).unwrap();
    let closure = AgentDeploymentClosure {
        interface: ExactVersionRef::new(interface_id, digest('b')).unwrap(),
        plan: ExactVersionRef::new(plan_id.clone(), digest('c')).unwrap(),
        slots: vec![],
        policies: vec![policy.clone()],
        execution_profile: policy.clone(),
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
            policy: policy.clone(),
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
