use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use insight_agent_platform::{
    dsl::{compile_source, CompileOptions},
    engine::{
        plan::{
            DescriptorConfigurationContract, DescriptorContract, DescriptorContractRegistry,
            DescriptorFieldContract, DescriptorValueSchema, LeafTaskKind, LinkedPlan, NodeKind,
            Plan, PortDirection, SubflowContractRegistry, VersionTag, WorkerContract,
            WorkerInputPortContract,
        },
        repository::{
            consume_scheduler_task_once, consume_scheduler_task_once_with_artifact_store,
            drive_scheduler_once, drive_scheduler_until_quiescent,
            AcknowledgeArtifactDeletionCommand, ArtifactDurableRepository,
            ClaimSchedulerRunCommand, CreateRunCommand, DurableRepository, FailOnceSchedulerCrash,
            FrozenSchedulerWorkerFailurePolicy, NoSchedulerCrash, OrphanSweepCommand,
            PlanInstallOutcome, PostgresDurableRepository, RepositoryError, SchedulerCrashInjector,
            SchedulerCrashPoint, SchedulerDriveOutcome, SchedulerDurableRepository,
            SchedulerLeaseRepository, SchedulerRecoveryOutcome, SchedulerTaskCommitOutcome,
            SchedulerTaskHeartbeatOutcome, SchedulerTaskOutcome, SchedulerTaskSuccess,
            SchedulerWorkerFailurePolicy, SchedulerWorkerPumpOutcome,
            TerminalSchedulerWorkerFailurePolicy, VersionedPlan,
        },
        ContentHash, DefinitionRevisionId, DeploymentRevisionId, EffectEvidence, EffectIdempotency,
        LeafTaskExecutor, LocalContentAddressedArtifactStore, RunId, RuntimeValue,
        SchedulerQuiescence, SchedulerTaskKind, TaskExecutionRequest, TaskExecutionResult,
        TransitionKey, TransitionOutcome, ValueRef, WorkerArtifactStore, WorkerCancellation,
        WorkerEffectClass, WorkerEffectPolicy, WorkerExecutionContext, WorkerExecutorRegistry,
        WorkerFailure, WorkerFailureClass,
    },
};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const LINEAR_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
output: string
workflow:
  steps:
    - id: answer
      type: action
      call: fixture.answer
      inputs:
        question: $question
      response: string
    - return: $answer
"#;

const LARGE_ARTIFACT_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
output: any
workflow:
  steps:
    - id: answer
      type: action
      call: fixture.large_artifact
      inputs:
        question: $question
      response: any
    - return: $answer
"#;

const PARALLEL_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
output: any
workflow:
  steps:
    - id: analyses
      settle: all_success
      parallel:
        technical:
          - id: technical_analysis
            type: action
            call: fixture.technical
            inputs: {question: $question}
            response: string
          - yield: $technical_analysis
        risk:
          - id: risk_analysis
            type: action
            call: fixture.risk
            inputs: {question: $question}
            response: string
          - yield: $risk_analysis
    - return: $analyses
"#;

const PARALLEL_FAILURE_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
output: any
workflow:
  steps:
    - id: work
      settle: all_success
      parallel:
        failing:
          - id: fail_task
            type: action
            call: fixture.failure
            response: string
          - yield: $fail_task
        sibling:
          - id: sibling_task
            type: action
            call: fixture.sibling
            response: string
          - yield: $sibling_task
    - return: $work
"#;

const PARALLEL_ALL_SETTLED_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
output: any
workflow:
  steps:
    - id: work
      settle: all_settled
      parallel:
        stable:
          - id: stable_task
            type: action
            call: fixture.stable
            response: string
          - yield: $stable_task
        risky:
          - id: risky_task
            type: action
            call: fixture.risky
            response: string
          - yield: $risky_task
    - return: $work
"#;

const NESTED_PARALLEL_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
output: any
workflow:
  steps:
    - id: outer
      settle: all_success
      parallel:
        nested:
          - id: inner
            settle: all_success
            parallel:
              left:
                - id: left_task
                  type: action
                  call: fixture.left
                  response: string
                - yield: $left_task
              right:
                - id: right_task
                  type: action
                  call: fixture.right
                  response: string
                - yield: $right_task
          - yield: $inner
        plain:
          - id: plain_task
            type: action
            call: fixture.plain
            response: string
          - yield: $plain_task
    - return: $outer
"#;

const BRANCH_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  ready: boolean
output: string
workflow:
  steps:
    - id: routed
      if: ready
      then:
        - id: selected_answer
          type: action
          call: fixture.answer
          response: string
        - yield: $selected_answer
      else:
        - id: fallback_answer
          type: action
          call: fixture.fallback
          response: string
        - yield: $fallback_answer
    - return: $routed
"#;

fn key(label: &str, run_id: &RunId) -> TransitionKey {
    TransitionKey::derive("scheduler.pg.integration.v1", &[label, run_id.as_str()]).unwrap()
}

fn crash_point_name(point: SchedulerCrashPoint) -> &'static str {
    match point {
        SchedulerCrashPoint::BeforeBranchCommit => "before_branch_commit",
        SchedulerCrashPoint::AfterBranchCommit => "after_branch_commit",
        SchedulerCrashPoint::BeforeJoinCommit => "before_join_commit",
        SchedulerCrashPoint::AfterJoinCommit => "after_join_commit",
        SchedulerCrashPoint::BeforeExternalCall => "before_external_call",
        SchedulerCrashPoint::AfterExternalCall => "after_external_call",
        SchedulerCrashPoint::BeforeResultCommit => "before_result_commit",
        SchedulerCrashPoint::AfterResultCommit => "after_result_commit",
        SchedulerCrashPoint::BeforeDownstreamAdmission => "before_downstream_admission",
        SchedulerCrashPoint::AfterDownstreamAdmission => "after_downstream_admission",
    }
}

fn parse_crash_point(name: &str) -> SchedulerCrashPoint {
    match name {
        "before_branch_commit" => SchedulerCrashPoint::BeforeBranchCommit,
        "after_branch_commit" => SchedulerCrashPoint::AfterBranchCommit,
        "before_join_commit" => SchedulerCrashPoint::BeforeJoinCommit,
        "after_join_commit" => SchedulerCrashPoint::AfterJoinCommit,
        "before_external_call" => SchedulerCrashPoint::BeforeExternalCall,
        "after_external_call" => SchedulerCrashPoint::AfterExternalCall,
        "before_result_commit" => SchedulerCrashPoint::BeforeResultCommit,
        "after_result_commit" => SchedulerCrashPoint::AfterResultCommit,
        "before_downstream_admission" => SchedulerCrashPoint::BeforeDownstreamAdmission,
        "after_downstream_admission" => SchedulerCrashPoint::AfterDownstreamAdmission,
        other => panic!("unknown real-process crash point: {other}"),
    }
}

/// This injector deliberately does not unwind. The parent integration test must
/// recover exclusively from PostgreSQL state; no in-process object or Drop
/// implementation gets an opportunity to repair the interrupted operation.
struct ExitProcessSchedulerCrash(SchedulerCrashPoint);

impl SchedulerCrashInjector for ExitProcessSchedulerCrash {
    fn hit(&self, point: SchedulerCrashPoint) -> Result<(), RepositoryError> {
        if point == self.0 {
            std::process::exit(86);
        }
        Ok(())
    }
}

fn compile_fixture_with_policy(
    source: &str,
    effect_policy: Option<&WorkerEffectPolicy>,
) -> (Plan, DescriptorContractRegistry) {
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("scheduler_pg_fixture_v1").unwrap(),
            "scheduler-pg-fixture.yaml",
            source,
        ),
    )
    .unwrap();
    let mut descriptors = DescriptorContractRegistry::new();
    for node in plan.nodes() {
        let NodeKind::ActionTask(descriptor) = node.kind() else {
            continue;
        };
        let inputs = plan
            .data_ports()
            .iter()
            .filter(|port| port.owner() == node.id() && port.direction() == PortDirection::Input)
            .map(|port| {
                (
                    port.name().clone(),
                    WorkerInputPortContract::new(port.value_type().clone(), port.required()),
                )
            })
            .collect();
        let outputs = plan
            .data_ports()
            .iter()
            .filter(|port| port.owner() == node.id() && port.direction() == PortDirection::Output)
            .map(|port| (port.name().clone(), port.value_type().clone()))
            .collect();
        let configuration = DescriptorConfigurationContract::closed(
            descriptor
                .public_configuration
                .keys()
                .map(|field| {
                    (
                        field.clone(),
                        DescriptorFieldContract::required(DescriptorValueSchema::Any),
                    )
                })
                .collect(),
            descriptor
                .secret_configuration
                .keys()
                .map(|field| (field.clone(), true))
                .collect(),
        );
        let mut worker = WorkerContract::new(
            LeafTaskKind::Action,
            VersionTag::new("worker-1").unwrap(),
            inputs,
            outputs,
        );
        if let Some(effect_policy) = effect_policy {
            worker = worker.with_effect_policy(effect_policy.clone());
        }
        descriptors
            .register(DescriptorContract::new(
                descriptor.implementation.clone(),
                descriptor.descriptor_version.clone(),
                configuration,
                worker,
            ))
            .unwrap();
    }
    (plan, descriptors)
}

fn compile_fixture(source: &str) -> (Plan, DescriptorContractRegistry) {
    compile_fixture_with_policy(source, None)
}

fn fixture_plan() -> (Plan, DescriptorContractRegistry) {
    compile_fixture(LINEAR_AGENT)
}

fn versioned(plan: &Plan) -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        "scheduler-pg-fixture",
        "scheduler-pg-agent",
        "PostgreSQL durable scheduler fixture",
        DeploymentRevisionId::new("scheduler_pg_deployment_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "structured"}),
        plan,
        json!({"fixture": "descriptor-v1"}),
        json!({}),
        json!({"fixture": "worker-1"}),
    )
    .unwrap()
}

async fn isolated_repository() -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("scheduler_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    Some((repository, control, admin, schema))
}

async fn activate_run(control: &PgPool, run_id: &RunId) {
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP WHERE run_id=$1 AND lifecycle='created'",
        )
        .bind(run_id.as_str())
        .execute(control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
}

#[derive(Clone)]
struct CountingExecutor {
    calls: Arc<AtomicUsize>,
}

struct SlowExecutor;

#[derive(Clone)]
enum FixtureBehavior {
    Success(RuntimeValue),
    Failure(WorkerFailure),
    Panic,
    SlowSuccess(RuntimeValue),
}

#[derive(Clone)]
struct BehaviorExecutor(FixtureBehavior);

#[derive(Clone)]
struct PostgresCountingExecutor {
    pool: PgPool,
}

#[derive(Clone)]
struct PostgresLargeArtifactExecutor {
    pool: PgPool,
}

fn large_artifact_value() -> serde_json::Value {
    json!({
        "kind": "large_artifact_fixture",
        "payload": "durable-artifact".repeat(512),
        "sequence": (0_u32..256).collect::<Vec<_>>(),
        "nested": {
            "survives": ["process", "crash", "restart"],
            "authoritative": true,
        },
    })
}

#[async_trait]
impl LeafTaskExecutor for PostgresCountingExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        sqlx::query(
            "INSERT INTO scheduler_real_crash_external_calls(run_id,task_id) VALUES ($1,$2)",
        )
        .bind(request.run_id().as_str())
        .bind(request.task_id().as_str())
        .execute(&self.pool)
        .await
        .expect("the PostgreSQL call ledger must survive the child process");
        let output = request.outputs().first().expect("fixture output");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("durable-answer")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

#[async_trait]
impl LeafTaskExecutor for PostgresLargeArtifactExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        sqlx::query(
            "INSERT INTO scheduler_real_crash_external_calls(run_id,task_id) VALUES ($1,$2)",
        )
        .bind(request.run_id().as_str())
        .bind(request.task_id().as_str())
        .execute(&self.pool)
        .await
        .expect("the PostgreSQL call ledger must survive the child process");
        let output = request.outputs().first().expect("fixture output");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(large_artifact_value()).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

#[async_trait]
impl LeafTaskExecutor for SlowExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let output = request.outputs().first().expect("fixture output");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("late-answer")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

#[async_trait]
impl LeafTaskExecutor for BehaviorExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let value = match &self.0 {
            FixtureBehavior::Success(value) => value.clone(),
            FixtureBehavior::Failure(failure) => return Err(failure.clone()),
            FixtureBehavior::Panic => panic!("fixture executor panic"),
            FixtureBehavior::SlowSuccess(value) => {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                value.clone()
            }
        };
        let output = request.outputs().first().expect("fixture output");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(output.port_id().clone(), value)]),
            EffectEvidence::Committed,
        ))
    }
}

#[async_trait]
impl LeafTaskExecutor for CountingExecutor {
    async fn execute(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        assert!(!context.fencing_token().is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        let output = request.outputs().first().expect("fixture output");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("durable-answer")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

async fn create_active_run(
    repository: &PostgresDurableRepository,
    control: &PgPool,
    plan: &VersionedPlan,
    run_id: RunId,
) {
    assert!(matches!(
        repository
            .create_run(
                key("create", &run_id),
                CreateRunCommand::new(run_id.clone(), plan, json!({"question": "why"})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    activate_run(control, &run_id).await;
}

async fn drive_to_task(
    repository: &PostgresDurableRepository,
    linked: &LinkedPlan<'_>,
    fence: &insight_agent_platform::engine::repository::FencedSchedulerRunCommand,
) {
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, linked, fence, &NoSchedulerCrash, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
}

fn real_crash_child_process(
    scoped_url: &str,
    run_id: &RunId,
    mode: &str,
    crash_point: SchedulerCrashPoint,
    artifact_root: Option<&Path>,
) {
    let mut child = Command::new(std::env::current_exe().expect("integration test executable"));
    child
        .arg("--exact")
        .arg("postgres_real_process_crash_child")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("REAL_CRASH_CHILD", "1")
        .env("REAL_CRASH_SCOPED_URL", scoped_url)
        .env("REAL_CRASH_RUN_ID", run_id.as_str())
        .env("REAL_CRASH_MODE", mode)
        .env("REAL_CRASH_POINT", crash_point_name(crash_point));
    if let Some(artifact_root) = artifact_root {
        child.env("REAL_CRASH_ARTIFACT_ROOT", artifact_root);
    }
    let output = child.output().expect("spawn the crash child process");
    assert_eq!(
        output.status.code(),
        Some(86),
        "child did not exit at {:?}; stdout={} stderr={}",
        crash_point,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn register_postgres_counting_worker(pool: &PgPool) -> WorkerExecutorRegistry {
    let mut registry = WorkerExecutorRegistry::new();
    registry
        .register(
            SchedulerTaskKind::Action,
            "fixture.answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            Arc::new(PostgresCountingExecutor { pool: pool.clone() }),
        )
        .unwrap();
    registry
}

fn register_postgres_large_artifact_worker(pool: &PgPool) -> WorkerExecutorRegistry {
    let mut registry = WorkerExecutorRegistry::new();
    registry
        .register(
            SchedulerTaskKind::Action,
            "fixture.large_artifact",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            Arc::new(PostgresLargeArtifactExecutor { pool: pool.clone() }),
        )
        .unwrap();
    registry
}

fn behavior_registry(entries: &[(&str, FixtureBehavior)]) -> WorkerExecutorRegistry {
    let mut registry = WorkerExecutorRegistry::new();
    for (implementation, behavior) in entries {
        registry
            .register(
                SchedulerTaskKind::Action,
                *implementation,
                VersionTag::new("1").unwrap(),
                VersionTag::new("worker-1").unwrap(),
                Arc::new(BehaviorExecutor(behavior.clone())),
            )
            .unwrap();
    }
    registry
}

async fn drain_scheduler_workers<P: SchedulerWorkerFailurePolicy>(
    repository: &PostgresDurableRepository,
    registry: &WorkerExecutorRegistry,
    failure_policy: &P,
    worker: &str,
) {
    loop {
        match consume_scheduler_task_once(
            repository,
            registry,
            failure_policy,
            worker,
            60,
            64,
            CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap()
        {
            SchedulerWorkerPumpOutcome::NoTask => break,
            SchedulerWorkerPumpOutcome::Committed { .. }
            | SchedulerWorkerPumpOutcome::AcknowledgedRecoveredResult => {}
            other => panic!("unexpected worker drain result: {other:?}"),
        }
    }
}

async fn drive_parallel_with_workers<P: SchedulerWorkerFailurePolicy>(
    repository: &PostgresDurableRepository,
    linked: &LinkedPlan<'_>,
    fence: &insight_agent_platform::engine::repository::FencedSchedulerRunCommand,
    registry: &WorkerExecutorRegistry,
    failure_policy: &P,
    worker: &str,
) -> SchedulerQuiescence {
    for _ in 0..64 {
        match drive_scheduler_until_quiescent(repository, linked, fence, &NoSchedulerCrash, 128)
            .await
            .unwrap()
        {
            SchedulerRecoveryOutcome::Quiescent(
                terminal @ (SchedulerQuiescence::RunSucceeded
                | SchedulerQuiescence::RunFailed
                | SchedulerQuiescence::RunCancelled),
            ) => return terminal,
            SchedulerRecoveryOutcome::Quiescent(_) => {
                drain_scheduler_workers(repository, registry, failure_policy, worker).await;
            }
            SchedulerRecoveryOutcome::ActionBudgetExhausted => {}
            SchedulerRecoveryOutcome::Fenced => panic!("scheduler fence was unexpectedly lost"),
        }
    }
    panic!("parallel scheduler did not reach a terminal quiescence")
}

async fn expire_scheduler_owner(control: &PgPool, run_id: &RunId) {
    sqlx::query(
        "UPDATE workflow_runs SET scheduler_lease_expires_at=CURRENT_TIMESTAMP - INTERVAL '1 second'
         WHERE run_id=$1 AND scheduler_lease_owner IS NOT NULL",
    )
    .bind(run_id.as_str())
    .execute(control)
    .await
    .unwrap();
}

async fn assert_one_durable_success(control: &PgPool, run_id: &RunId) {
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM public_event_outbox
             WHERE run_id=$1 AND event_kind='run.completed' AND is_terminal=TRUE",
        )
        .bind(run_id.as_str())
        .fetch_one(control)
        .await
        .unwrap(),
        1,
        "there must be exactly one durable terminal fact"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_attempts WHERE run_id=$1 AND lifecycle='succeeded'",
        )
        .bind(run_id.as_str())
        .fetch_one(control)
        .await
        .unwrap(),
        1,
        "recovery must leave one authoritative task result"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_outbox WHERE run_id=$1 AND task_state='acked'",
        )
        .bind(run_id.as_str())
        .fetch_one(control)
        .await
        .unwrap(),
        1,
        "the authoritative task result must be acknowledged exactly once"
    );
}

#[tokio::test]
async fn postgres_real_process_crash_child() {
    if std::env::var("REAL_CRASH_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let scoped_url = std::env::var("REAL_CRASH_SCOPED_URL").unwrap();
    let run_id = RunId::new(std::env::var("REAL_CRASH_RUN_ID").unwrap()).unwrap();
    let mode = std::env::var("REAL_CRASH_MODE").unwrap();
    let crash_point = parse_crash_point(&std::env::var("REAL_CRASH_POINT").unwrap());
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    let control = PgPoolOptions::new()
        .max_connections(2)
        .connect(&scoped_url)
        .await
        .unwrap();
    let crash = ExitProcessSchedulerCrash(crash_point);

    match mode.as_str() {
        "branch" => {
            let (plan, descriptors) = compile_fixture(BRANCH_AGENT);
            let subflows = SubflowContractRegistry::new();
            let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
            let lease = repository
                .claim_scheduler_run(
                    key("real-crash-child-claim", &run_id),
                    ClaimSchedulerRunCommand::new(
                        run_id.clone(),
                        format!("crash-child-{}", crash_point_name(crash_point)),
                        60,
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .committed_result()
                .cloned()
                .expect("child scheduler lease");
            let _ = drive_scheduler_until_quiescent(
                &repository,
                &linked,
                &lease.fence().unwrap(),
                &crash,
                64,
            )
            .await;
        }
        "result" => {
            let retry_policy = WorkerEffectPolicy::frozen(
                WorkerEffectClass::Mutating,
                EffectIdempotency::Idempotent,
                2,
                0,
                0,
                60_000,
                WorkerCancellation::Cooperative,
            )
            .unwrap();
            let (_plan, _descriptors) =
                compile_fixture_with_policy(LINEAR_AGENT, Some(&retry_policy));
            let registry = register_postgres_counting_worker(&control);
            let _ = consume_scheduler_task_once(
                &repository,
                &registry,
                &FrozenSchedulerWorkerFailurePolicy,
                "real-crash-child-worker",
                60,
                64,
                CancellationToken::new(),
                &crash,
            )
            .await;
        }
        "artifact_result" => {
            let registry = register_postgres_large_artifact_worker(&control);
            let store = LocalContentAddressedArtifactStore::open(
                PathBuf::from(std::env::var("REAL_CRASH_ARTIFACT_ROOT").unwrap()),
                1,
            )
            .await
            .unwrap();
            let _ = consume_scheduler_task_once_with_artifact_store(
                &repository,
                &registry,
                &FrozenSchedulerWorkerFailurePolicy,
                "real-crash-artifact-child-worker",
                60,
                64,
                CancellationToken::new(),
                &store,
                &crash,
            )
            .await;
        }
        other => panic!("unknown real-process crash mode: {other}"),
    }
    panic!("crash point {crash_point:?} was not reached");
}

#[tokio::test]
async fn postgres_real_process_crash_matrix_recovers_from_durable_state() {
    let Some((repository, control, _admin, schema)) = isolated_repository().await else {
        return;
    };
    let database_url = std::env::var("TEST_POSTGRES_URL").unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    sqlx::query(
        "CREATE TABLE scheduler_real_crash_external_calls (
            call_no BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            called_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&control)
    .await
    .unwrap();

    let (branch_plan, branch_descriptors) = compile_fixture(BRANCH_AGENT);
    let branch_subflows = SubflowContractRegistry::new();
    let linked_branch =
        LinkedPlan::link(&branch_plan, &branch_descriptors, &branch_subflows).unwrap();
    let branch_versioned = versioned(&branch_plan);
    repository
        .install_versioned_plan(&branch_versioned)
        .await
        .unwrap();
    let selected_node = branch_plan
        .nodes()
        .iter()
        .find(|node| {
            matches!(
                node.kind(),
                NodeKind::ActionTask(descriptor) if descriptor.implementation == "fixture.answer"
            )
        })
        .unwrap()
        .id()
        .clone();
    let unselected_node = branch_plan
        .nodes()
        .iter()
        .find(|node| {
            matches!(
                node.kind(),
                NodeKind::ActionTask(descriptor) if descriptor.implementation == "fixture.fallback"
            )
        })
        .unwrap()
        .id()
        .clone();

    for crash_point in [
        SchedulerCrashPoint::BeforeBranchCommit,
        SchedulerCrashPoint::AfterBranchCommit,
        SchedulerCrashPoint::BeforeDownstreamAdmission,
        SchedulerCrashPoint::AfterDownstreamAdmission,
    ] {
        let suffix = crash_point_name(crash_point);
        let run_id = RunId::new(format!("run_pg_real_{suffix}")).unwrap();
        assert!(matches!(
            repository
                .create_run(
                    key("real-crash-branch-create", &run_id),
                    CreateRunCommand::new(
                        run_id.clone(),
                        &branch_versioned,
                        json!({"ready": true}),
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        activate_run(&control, &run_id).await;

        real_crash_child_process(&scoped_url, &run_id, "branch", crash_point, None);

        // This is deliberately a new pool/repository and a higher lease epoch.
        // The child cannot share any in-memory scheduler state with recovery.
        let recovered_repository = PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap();
        expire_scheduler_owner(&control, &run_id).await;
        let replacement = recovered_repository
            .claim_scheduler_run(
                key("real-crash-parent-reclaim", &run_id),
                ClaimSchedulerRunCommand::new(run_id.clone(), format!("recovered-{suffix}"), 60)
                    .unwrap(),
            )
            .await
            .unwrap()
            .committed_result()
            .cloned()
            .expect("replacement scheduler lease");
        let fence = replacement.fence().unwrap();
        assert!(matches!(
            drive_scheduler_until_quiescent(
                &recovered_repository,
                &linked_branch,
                &fence,
                &NoSchedulerCrash,
                64,
            )
            .await
            .unwrap(),
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM node_activations WHERE run_id=$1 AND node_id=$2",
            )
            .bind(run_id.as_str())
            .bind(selected_node.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            1,
            "the selected branch activation is admitted exactly once"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM node_activations WHERE run_id=$1 AND node_id=$2",
            )
            .bind(run_id.as_str())
            .bind(unselected_node.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            0,
            "an unselected branch must never acquire an Activation"
        );

        let registry = register_postgres_counting_worker(&control);
        assert!(matches!(
            consume_scheduler_task_once(
                &recovered_repository,
                &registry,
                &TerminalSchedulerWorkerFailurePolicy,
                &format!("recovered-worker-{suffix}"),
                60,
                64,
                CancellationToken::new(),
                &NoSchedulerCrash,
            )
            .await
            .unwrap(),
            SchedulerWorkerPumpOutcome::Committed {
                acknowledged: true,
                ..
            }
        ));
        assert!(matches!(
            drive_scheduler_until_quiescent(
                &recovered_repository,
                &linked_branch,
                &fence,
                &NoSchedulerCrash,
                64,
            )
            .await
            .unwrap(),
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scheduler_real_crash_external_calls WHERE run_id=$1",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            1
        );
        assert_one_durable_success(&control, &run_id).await;
    }

    // The fixtures intentionally share one immutable definition revision but
    // have different Plan hashes, so exercise the worker crash half in its own
    // durable authority namespace just as a separate deployed definition would.
    let Some((repository, control, _result_admin, schema)) = isolated_repository().await else {
        return;
    };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    sqlx::query(
        "CREATE TABLE scheduler_real_crash_external_calls (
            call_no BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            called_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&control)
    .await
    .unwrap();

    let retry_policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::Mutating,
        EffectIdempotency::Idempotent,
        2,
        0,
        0,
        60_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let (result_plan, result_descriptors) =
        compile_fixture_with_policy(LINEAR_AGENT, Some(&retry_policy));
    let result_subflows = SubflowContractRegistry::new();
    let linked_result =
        LinkedPlan::link(&result_plan, &result_descriptors, &result_subflows).unwrap();
    let result_versioned = versioned(&result_plan);
    repository
        .install_versioned_plan(&result_versioned)
        .await
        .unwrap();

    for (crash_point, expected_external_calls) in [
        (SchedulerCrashPoint::BeforeExternalCall, 1_i64),
        (SchedulerCrashPoint::AfterExternalCall, 2_i64),
        (SchedulerCrashPoint::BeforeResultCommit, 2_i64),
        (SchedulerCrashPoint::AfterResultCommit, 1_i64),
    ] {
        let suffix = crash_point_name(crash_point);
        let run_id = RunId::new(format!("run_pg_real_{suffix}")).unwrap();
        create_active_run(&repository, &control, &result_versioned, run_id.clone()).await;
        let initial_lease = repository
            .claim_scheduler_run(
                key("real-crash-result-claim", &run_id),
                ClaimSchedulerRunCommand::new(
                    run_id.clone(),
                    format!("result-preparer-{suffix}"),
                    60,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .committed_result()
            .cloned()
            .unwrap();
        drive_to_task(&repository, &linked_result, &initial_lease.fence().unwrap()).await;

        real_crash_child_process(&scoped_url, &run_id, "result", crash_point, None);

        let recovered_repository = PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap();
        expire_scheduler_owner(&control, &run_id).await;
        let replacement = recovered_repository
            .claim_scheduler_run(
                key("real-crash-result-reclaim", &run_id),
                ClaimSchedulerRunCommand::new(
                    run_id.clone(),
                    format!("result-recovered-{suffix}"),
                    60,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .committed_result()
            .cloned()
            .expect("replacement scheduler lease");

        if crash_point != SchedulerCrashPoint::AfterResultCommit {
            sqlx::query(
                "UPDATE task_outbox SET claim_expires_at=CURRENT_TIMESTAMP - INTERVAL '1 second'
                 WHERE run_id=$1 AND task_state='claimed'",
            )
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap();
        }
        let registry = register_postgres_counting_worker(&control);
        let recovered = consume_scheduler_task_once(
            &recovered_repository,
            &registry,
            &FrozenSchedulerWorkerFailurePolicy,
            &format!("result-recovered-worker-{suffix}"),
            60,
            64,
            CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap();
        if crash_point == SchedulerCrashPoint::AfterResultCommit {
            assert_eq!(
                recovered,
                SchedulerWorkerPumpOutcome::AcknowledgedRecoveredResult
            );
        } else {
            assert!(matches!(
                recovered,
                SchedulerWorkerPumpOutcome::Committed {
                    acknowledged: false,
                    ..
                }
            ));
            assert!(matches!(
                consume_scheduler_task_once(
                    &recovered_repository,
                    &registry,
                    &FrozenSchedulerWorkerFailurePolicy,
                    &format!("result-ack-worker-{suffix}"),
                    60,
                    64,
                    CancellationToken::new(),
                    &NoSchedulerCrash,
                )
                .await
                .unwrap(),
                SchedulerWorkerPumpOutcome::Committed {
                    acknowledged: true,
                    ..
                }
            ));
        }
        assert!(matches!(
            drive_scheduler_until_quiescent(
                &recovered_repository,
                &linked_result,
                &replacement.fence().unwrap(),
                &NoSchedulerCrash,
                64,
            )
            .await
            .unwrap(),
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scheduler_real_crash_external_calls WHERE run_id=$1",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            expected_external_calls,
            "external calls must match the frozen idempotency contract at {crash_point:?}"
        );
        assert_one_durable_success(&control, &run_id).await;
    }
}

#[tokio::test]
async fn postgres_large_artifact_before_result_commit_honors_grace_then_is_reclaimed() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let database_url = std::env::var("TEST_POSTGRES_URL").unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    sqlx::query(
        "CREATE TABLE scheduler_real_crash_external_calls (
            call_no BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            called_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&control)
    .await
    .unwrap();

    let retry_policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::Mutating,
        EffectIdempotency::Idempotent,
        2,
        0,
        0,
        60_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let (plan, descriptors) =
        compile_fixture_with_policy(LARGE_ARTIFACT_AGENT, Some(&retry_policy));
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let run_id = RunId::new("run_pg_real_large_artifact_before_result_commit").unwrap();
    create_active_run(&repository, &control, &versioned, run_id.clone()).await;
    let initial_lease = repository
        .claim_scheduler_run(
            key("real-crash-artifact-before-claim", &run_id),
            ClaimSchedulerRunCommand::new(
                run_id.clone(),
                "real-crash-artifact-before-preparer",
                60,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .expect("initial scheduler lease");
    drive_to_task(&repository, &linked, &initial_lease.fence().unwrap()).await;

    let artifact_directory = tempfile::tempdir().unwrap();
    let artifact_root = artifact_directory.path().join("objects");
    real_crash_child_process(
        &scoped_url,
        &run_id,
        "artifact_result",
        SchedulerCrashPoint::BeforeResultCommit,
        Some(&artifact_root),
    );

    drop(repository);
    control.close().await;
    let recovered_repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    let recovered_control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let recovered_store = LocalContentAddressedArtifactStore::open(artifact_root.clone(), 1)
        .await
        .unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scheduler_values WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&recovered_control)
            .await
            .unwrap(),
        0,
        "BeforeResultCommit must leave no globally visible scheduler value"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_occurrence_values WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&recovered_control)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_attempts
             WHERE run_id=$1 AND output_artifact_id IS NOT NULL",
        )
        .bind(run_id.as_str())
        .fetch_one(&recovered_control)
        .await
        .unwrap(),
        0,
        "an uploaded object is not a committed Attempt output"
    );
    let artifact = sqlx::query_as::<_, (String, String, String, bool, bool, bool)>(
        "SELECT artifact_id,artifact_state,content_hash,
                verified_at IS NOT NULL,referenced_at IS NULL,
                retain_until > CURRENT_TIMESTAMP
         FROM artifacts WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_one(&recovered_control)
    .await
    .unwrap();
    assert_eq!(artifact.1, "verified");
    assert!(artifact.3, "the object upload must have been verified");
    assert!(
        artifact.4,
        "the uncommitted object must remain unreferenced"
    );
    assert!(
        artifact.5,
        "the staging grace must be fixed by database time"
    );
    let hash = artifact.2.strip_prefix("sha256:").unwrap();
    let object_path = artifact_root.join(&hash[..2]).join(hash);
    assert!(object_path.is_file());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_real_crash_external_calls WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&recovered_control)
        .await
        .unwrap(),
        1
    );

    // Make age independently eligible while preserving the authoritative
    // retain_until grace. The first sweep must still refuse the object.
    sqlx::query(
        "UPDATE artifacts SET created_at=CURRENT_TIMESTAMP-INTERVAL '10 minutes'
         WHERE run_id=$1 AND artifact_id=$2",
    )
    .bind(run_id.as_str())
    .bind(&artifact.0)
    .execute(&recovered_control)
    .await
    .unwrap();
    let grace_sweep = recovered_repository
        .sweep_orphan_artifacts(
            key("real-crash-artifact-before-grace-sweep", &run_id),
            OrphanSweepCommand::new(1, "real-crash-before-grace-sweeper", 30, 100).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        grace_sweep.committed_result().unwrap().claims().is_empty(),
        "orphan sweep must honor retain_until even when created_at is old"
    );
    assert!(object_path.is_file());

    // Advance only the database-owned grace boundary. The next sweep may now
    // claim exactly this verified-but-unreferenced object for deletion.
    sqlx::query(
        "UPDATE artifacts SET retain_until=CURRENT_TIMESTAMP-INTERVAL '1 second'
         WHERE run_id=$1 AND artifact_id=$2 AND artifact_state='verified'",
    )
    .bind(run_id.as_str())
    .bind(&artifact.0)
    .execute(&recovered_control)
    .await
    .unwrap();
    let expired_sweep = recovered_repository
        .sweep_orphan_artifacts(
            key("real-crash-artifact-before-expired-sweep", &run_id),
            OrphanSweepCommand::new(1, "real-crash-before-expired-sweeper", 30, 100).unwrap(),
        )
        .await
        .unwrap();
    let claims = expired_sweep.committed_result().unwrap().claims();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].artifact().artifact_id().as_str(), artifact.0);
    recovered_store
        .delete(claims[0].artifact(), claims[0].storage_locator())
        .await
        .unwrap();
    assert!(matches!(
        recovered_repository
            .acknowledge_artifact_deleted(AcknowledgeArtifactDeletionCommand::from_claim(
                &claims[0],
            ))
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(tokio::fs::metadata(&object_path).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT artifact_state FROM artifacts WHERE run_id=$1 AND artifact_id=$2",
        )
        .bind(run_id.as_str())
        .bind(&artifact.0)
        .fetch_one(&recovered_control)
        .await
        .unwrap(),
        "deleted"
    );

    recovered_control.close().await;
    drop(recovered_repository);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres_large_artifact_survives_real_process_result_commit_crash() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let database_url = std::env::var("TEST_POSTGRES_URL").unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    sqlx::query(
        "CREATE TABLE scheduler_real_crash_external_calls (
            call_no BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            called_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&control)
    .await
    .unwrap();

    let retry_policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::Mutating,
        EffectIdempotency::Idempotent,
        2,
        0,
        0,
        60_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let (plan, descriptors) =
        compile_fixture_with_policy(LARGE_ARTIFACT_AGENT, Some(&retry_policy));
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let action_node = plan
        .nodes()
        .iter()
        .find(|node| {
            matches!(
                node.kind(),
                NodeKind::ActionTask(descriptor)
                    if descriptor.implementation == "fixture.large_artifact"
            )
        })
        .expect("large artifact action node")
        .id()
        .clone();
    let action_output = plan
        .data_ports()
        .iter()
        .find(|port| port.owner() == &action_node && port.direction() == PortDirection::Output)
        .expect("large artifact action output")
        .id()
        .clone();

    let run_id = RunId::new("run_pg_real_large_artifact_after_result_commit").unwrap();
    create_active_run(&repository, &control, &versioned, run_id.clone()).await;
    let initial_lease = repository
        .claim_scheduler_run(
            key("real-crash-artifact-claim", &run_id),
            ClaimSchedulerRunCommand::new(run_id.clone(), "real-crash-artifact-preparer", 60)
                .unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .expect("initial scheduler lease");
    drive_to_task(&repository, &linked, &initial_lease.fence().unwrap()).await;

    let artifact_directory = tempfile::tempdir().unwrap();
    let artifact_root = artifact_directory.path().join("objects");
    real_crash_child_process(
        &scoped_url,
        &run_id,
        "artifact_result",
        SchedulerCrashPoint::AfterResultCommit,
        Some(&artifact_root),
    );

    // Drop every original parent-side handle. Recovery below must rebuild all
    // repository, pool, worker-store, and scheduler authority from durable state.
    drop(repository);
    control.close().await;
    let recovered_repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    let recovered_control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let recovered_store = LocalContentAddressedArtifactStore::open(artifact_root.clone(), 1)
        .await
        .unwrap();

    let stored = recovered_repository
        .load_scheduler_value(&run_id, &action_output)
        .await
        .unwrap()
        .expect("the committed action output must survive process exit");
    assert_eq!(stored.runtime_value().value(), &large_artifact_value());
    let artifact_ref = match stored.value_ref() {
        ValueRef::Artifact(artifact) => artifact.clone(),
        ValueRef::Inline(_) => panic!("large action output must be stored as an ArtifactRef"),
    };
    let artifact_rows = sqlx::query_as::<_, (String, String, i64, String, bool, bool)>(
        "SELECT artifact_id,content_hash,size_bytes,artifact_state,
                verified_at IS NOT NULL,referenced_at IS NOT NULL
         FROM artifacts WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_all(&recovered_control)
    .await
    .unwrap();
    assert_eq!(
        artifact_rows.len(),
        1,
        "one output must create exactly one artifact authority row"
    );
    let (artifact_id, content_hash, size_bytes, state, verified, referenced) = &artifact_rows[0];
    assert_eq!(state, "referenced");
    assert!(*verified && *referenced);
    assert_eq!(artifact_id, artifact_ref.artifact_id().as_str());
    assert_eq!(content_hash, artifact_ref.content_hash().as_str());
    assert_eq!(
        *size_bytes,
        i64::try_from(artifact_ref.size_bytes()).unwrap()
    );
    let hash = content_hash.strip_prefix("sha256:").unwrap();
    let object_path = artifact_root.join(&hash[..2]).join(hash);
    let object_bytes = tokio::fs::read(&object_path).await.unwrap();
    let expected_bytes = serde_jcs::to_vec(&large_artifact_value()).unwrap();
    assert_eq!(object_bytes, expected_bytes);
    assert_eq!(
        ContentHash::from_bytes(&object_bytes),
        *artifact_ref.content_hash()
    );
    assert_eq!(
        u64::try_from(object_bytes.len()).unwrap(),
        artifact_ref.size_bytes()
    );

    expire_scheduler_owner(&recovered_control, &run_id).await;
    let replacement = recovered_repository
        .claim_scheduler_run(
            key("real-crash-artifact-reclaim", &run_id),
            ClaimSchedulerRunCommand::new(
                run_id.clone(),
                "real-crash-artifact-recovered-scheduler",
                60,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .expect("replacement scheduler lease");
    let recovered_registry = register_postgres_large_artifact_worker(&recovered_control);
    assert_eq!(
        consume_scheduler_task_once_with_artifact_store(
            &recovered_repository,
            &recovered_registry,
            &FrozenSchedulerWorkerFailurePolicy,
            "real-crash-artifact-recovery-worker",
            60,
            64,
            CancellationToken::new(),
            &recovered_store,
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::AcknowledgedRecoveredResult
    );
    assert_eq!(
        consume_scheduler_task_once_with_artifact_store(
            &recovered_repository,
            &recovered_registry,
            &FrozenSchedulerWorkerFailurePolicy,
            "real-crash-artifact-second-recovery-worker",
            60,
            64,
            CancellationToken::new(),
            &recovered_store,
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::NoTask,
        "the recovered result must be acknowledged only once"
    );
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &recovered_repository,
            &linked,
            &replacement.fence().unwrap(),
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_real_crash_external_calls WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&recovered_control)
        .await
        .unwrap(),
        1,
        "ack recovery must not call the external worker again"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events
             WHERE run_id=$1 AND kind='projection.mutated'
               AND safe_payload->>'mutation'='task_acknowledged'",
        )
        .bind(run_id.as_str())
        .fetch_one(&recovered_control)
        .await
        .unwrap(),
        1,
        "one task acknowledgement must produce one durable fact"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events
             WHERE run_id=$1 AND kind='run.lifecycle_changed'
               AND safe_payload->>'lifecycle'='succeeded'",
        )
        .bind(run_id.as_str())
        .fetch_one(&recovered_control)
        .await
        .unwrap(),
        1,
        "scheduler completion must produce one internal terminal fact"
    );
    assert_one_durable_success(&recovered_control, &run_id).await;

    let sweep = recovered_repository
        .sweep_orphan_artifacts(
            key("real-crash-artifact-orphan-sweep", &run_id),
            OrphanSweepCommand::new(1, "real-crash-artifact-sweeper", 30, 100).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        sweep.committed_result().unwrap().claims().is_empty(),
        "orphan cleanup must not claim a referenced scheduler value"
    );
    assert_eq!(tokio::fs::read(&object_path).await.unwrap(), object_bytes);

    sqlx::query("UPDATE scheduler_values SET runtime_value=$1 WHERE run_id=$2 AND port_id=$3")
        .bind(
            serde_json::to_value(
                RuntimeValue::new(json!({"forged": "postgres-runtime-value"})).unwrap(),
            )
            .unwrap(),
        )
        .bind(run_id.as_str())
        .bind(action_output.as_str())
        .execute(&recovered_control)
        .await
        .unwrap();
    assert!(recovered_repository
        .load_scheduler_value(&run_id, &action_output)
        .await
        .is_err());
    assert!(recovered_repository
        .load_scheduler_facts(&run_id)
        .await
        .is_err());

    recovered_control.close().await;
    drop(recovered_repository);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres_branch_selection_atomically_correlates_consumes_and_admits() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let (plan, descriptors) = compile_fixture(BRANCH_AGENT);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new("run_scheduler_pg_atomic_branch").unwrap();
    assert!(matches!(
        repository
            .create_run(
                key("branch-create", &run_id),
                CreateRunCommand::new(run_id.clone(), &versioned, json!({"ready": true})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    activate_run(&control, &run_id).await;
    let lease = repository
        .claim_scheduler_run(
            key("branch-claim", &run_id),
            ClaimSchedulerRunCommand::new(run_id.clone(), "scheduler-branch", 60).unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap();
    let fence = lease.fence().unwrap();
    drive_to_task(&repository, &linked, &fence).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = WorkerExecutorRegistry::new();
    registry
        .register(
            SchedulerTaskKind::Action,
            "fixture.answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            Arc::new(CountingExecutor {
                calls: calls.clone(),
            }),
        )
        .unwrap();
    assert!(matches!(
        consume_scheduler_task_once(
            &repository,
            &registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "worker-branch",
            60,
            64,
            CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::Committed {
            acknowledged: true,
            ..
        }
    ));
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_checkpoints
             WHERE run_id=$1 AND checkpoint_kind='planned_action'
               AND fact_payload->'action'->>'kind'='select_branch_and_admit'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM control_tokens t
             JOIN node_activations a ON a.run_id=t.run_id
                AND a.activation_id=t.consumed_by_activation_id
             WHERE t.run_id=$1 AND t.branch_activation_id IS NOT NULL
               AND t.selected_branch_port_id IS NOT NULL
               AND jsonb_array_length(t.provenance_frames)=1
               AND t.provenance_frames->0->>'kind'='branch'
               AND t.token_state='consumed'
               AND t.consumed_by_transition_key=t.emitted_by_transition_key
               AND a.node_id='selected_answer'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1,
        "one transition must select, correlate, admit and consume into the chosen successor"
    );
}

#[tokio::test]
async fn postgres_branch_and_downstream_crash_boundaries_recover_exactly_once() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let (plan, descriptors) = compile_fixture(BRANCH_AGENT);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let selected_node = plan
        .nodes()
        .iter()
        .find(|node| {
            matches!(
                node.kind(),
                NodeKind::ActionTask(descriptor) if descriptor.implementation == "fixture.answer"
            )
        })
        .unwrap()
        .id()
        .clone();
    let unselected_node = plan
        .nodes()
        .iter()
        .find(|node| {
            matches!(
                node.kind(),
                NodeKind::ActionTask(descriptor) if descriptor.implementation == "fixture.fallback"
            )
        })
        .unwrap()
        .id()
        .clone();

    for (suffix, crash_point) in [
        ("before_branch", SchedulerCrashPoint::BeforeBranchCommit),
        ("after_branch", SchedulerCrashPoint::AfterBranchCommit),
        (
            "before_downstream",
            SchedulerCrashPoint::BeforeDownstreamAdmission,
        ),
        (
            "after_downstream",
            SchedulerCrashPoint::AfterDownstreamAdmission,
        ),
    ] {
        let run_id = RunId::new(format!("run_scheduler_pg_{suffix}")).unwrap();
        assert!(matches!(
            repository
                .create_run(
                    key("branch-crash-create", &run_id),
                    CreateRunCommand::new(run_id.clone(), &versioned, json!({"ready": true}))
                        .unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        activate_run(&control, &run_id).await;
        let fence = repository
            .claim_scheduler_run(
                key("branch-crash-claim", &run_id),
                ClaimSchedulerRunCommand::new(run_id.clone(), format!("scheduler-{suffix}"), 60)
                    .unwrap(),
            )
            .await
            .unwrap()
            .committed_result()
            .cloned()
            .unwrap()
            .fence()
            .unwrap();
        let crash = FailOnceSchedulerCrash::new(crash_point);
        assert_eq!(
            drive_scheduler_until_quiescent(&repository, &linked, &fence, &crash, 64)
                .await
                .unwrap_err()
                .code(),
            "ENGINE_REPOSITORY_SCHEDULER_CRASH_INJECTED"
        );
        assert!(crash.fired());
        assert!(matches!(
            drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
                .await
                .unwrap(),
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
        ));

        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scheduler_checkpoints
                 WHERE run_id=$1 AND checkpoint_kind='planned_action'
                   AND fact_payload->'action'->>'kind'='select_branch_and_admit'",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM node_activations WHERE run_id=$1 AND node_id=$2",
            )
            .bind(run_id.as_str())
            .bind(selected_node.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM node_activations WHERE run_id=$1 AND node_id=$2",
            )
            .bind(run_id.as_str())
            .bind(unselected_node.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            0
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = WorkerExecutorRegistry::new();
        registry
            .register(
                SchedulerTaskKind::Action,
                "fixture.answer",
                VersionTag::new("1").unwrap(),
                VersionTag::new("worker-1").unwrap(),
                Arc::new(CountingExecutor {
                    calls: calls.clone(),
                }),
            )
            .unwrap();
        assert!(matches!(
            consume_scheduler_task_once(
                &repository,
                &registry,
                &TerminalSchedulerWorkerFailurePolicy,
                &format!("worker-{suffix}"),
                60,
                64,
                CancellationToken::new(),
                &NoSchedulerCrash,
            )
            .await
            .unwrap(),
            SchedulerWorkerPumpOutcome::Committed {
                acknowledged: true,
                ..
            }
        ));
        assert!(matches!(
            drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
                .await
                .unwrap(),
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM public_event_outbox
                 WHERE run_id=$1 AND event_kind='run.completed' AND is_terminal=TRUE",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            1
        );
    }
}

#[tokio::test]
async fn postgres_parallel_fork_persists_exact_join_arrivals() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let (plan, descriptors) = compile_fixture(PARALLEL_AGENT);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new("run_scheduler_pg_parallel_arrivals").unwrap();
    create_active_run(&repository, &control, &versioned, run_id.clone()).await;
    let lease = repository
        .claim_scheduler_run(
            key("parallel-claim", &run_id),
            ClaimSchedulerRunCommand::new(run_id.clone(), "scheduler-parallel", 60).unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap();
    let fence = lease.fence().unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = WorkerExecutorRegistry::new();
    for implementation in ["fixture.technical", "fixture.risk"] {
        registry
            .register(
                SchedulerTaskKind::Action,
                implementation,
                VersionTag::new("1").unwrap(),
                VersionTag::new("worker-1").unwrap(),
                Arc::new(CountingExecutor {
                    calls: calls.clone(),
                }),
            )
            .unwrap();
    }

    let mut succeeded = false;
    for _ in 0..16 {
        match drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 128)
            .await
            .unwrap()
        {
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded) => {
                succeeded = true;
                break;
            }
            SchedulerRecoveryOutcome::Quiescent(
                SchedulerQuiescence::WaitingForTask { .. }
                | SchedulerQuiescence::WaitingForChildren { .. },
            ) => loop {
                match consume_scheduler_task_once(
                    &repository,
                    &registry,
                    &TerminalSchedulerWorkerFailurePolicy,
                    "worker-parallel",
                    60,
                    64,
                    CancellationToken::new(),
                    &NoSchedulerCrash,
                )
                .await
                .unwrap()
                {
                    SchedulerWorkerPumpOutcome::NoTask => break,
                    SchedulerWorkerPumpOutcome::Committed {
                        acknowledged: true, ..
                    }
                    | SchedulerWorkerPumpOutcome::AcknowledgedRecoveredResult => {}
                    outcome => panic!("unexpected parallel worker outcome: {outcome:?}"),
                }
            },
            outcome => panic!("unexpected parallel scheduler outcome: {outcome:?}"),
        }
    }
    assert!(succeeded, "parallel scheduler did not reach RunSucceeded");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let group = sqlx::query(
        "SELECT expected_legs,admitted_legs,settled_legs,group_state,join_activation_id
         FROM fork_groups WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(sqlx::Row::get::<i32, _>(&group, "expected_legs"), 2);
    assert_eq!(sqlx::Row::get::<i32, _>(&group, "admitted_legs"), 2);
    assert_eq!(sqlx::Row::get::<i32, _>(&group, "settled_legs"), 2);
    assert_eq!(
        sqlx::Row::get::<String, _>(&group, "group_state"),
        "settled"
    );
    assert!(sqlx::Row::get::<Option<String>, _>(&group, "join_activation_id").is_some());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_checkpoints
             WHERE run_id=$1 AND checkpoint_kind='planned_action'
               AND fact_payload->'action'->>'kind'='open_fork'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1,
        "the ordered Fork group and all leg admissions share one checkpoint"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM control_tokens
             WHERE run_id=$1 AND fork_group_id IS NOT NULL AND fork_leg_id IS NOT NULL
               AND jsonb_array_length(provenance_frames)=1
               AND provenance_frames->0->>'kind'='fork_leg'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scope_instances
             WHERE run_id=$1 AND scope_kind='parallel_leg'
               AND admitted_children=0 AND settled_children=0
               AND lifecycle='settled' AND admission_state='closed'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT admitted_children,settled_children FROM scope_instances
             WHERE run_id=$1 AND is_root=TRUE",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        (2, 2)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM fork_groups g
             JOIN node_activations a ON a.run_id=g.run_id
                AND a.activation_id=g.fork_activation_id
             WHERE g.run_id=$1 AND a.lifecycle='succeeded'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_activations WHERE run_id=$1
               AND lifecycle NOT IN ('succeeded','failed','cancelled','timed_out')",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0,
        "a successful Fork workflow cannot retain a live controller activation"
    );

    let arrivals = sqlx::query(
        "SELECT j.arrival_transition_key,j.arrival_event_id,j.value_payload_id,j.value_hash,
                c.transition_key AS checkpoint_transition,c.event_id AS checkpoint_event
         FROM join_arrivals j
         JOIN scheduler_checkpoints c ON c.run_id=j.run_id
              AND c.transition_key=j.arrival_transition_key
              AND c.event_id=j.arrival_event_id
         WHERE j.run_id=$1 ORDER BY j.leg_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(arrivals.len(), 2);
    for arrival in arrivals {
        assert_eq!(
            sqlx::Row::get::<String, _>(&arrival, "arrival_transition_key"),
            sqlx::Row::get::<String, _>(&arrival, "checkpoint_transition")
        );
        assert_eq!(
            sqlx::Row::get::<String, _>(&arrival, "arrival_event_id"),
            sqlx::Row::get::<String, _>(&arrival, "checkpoint_event")
        );
        assert!(sqlx::Row::get::<Option<String>, _>(&arrival, "value_payload_id").is_some());
        assert!(sqlx::Row::get::<Option<String>, _>(&arrival, "value_hash").is_some());
    }
}

#[tokio::test]
async fn postgres_all_success_cancels_and_fences_sibling_before_internal_failure() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let (plan, descriptors) = compile_fixture(PARALLEL_FAILURE_AGENT);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let run_id = RunId::new("run_scheduler_pg_parallel_failure_drain").unwrap();
    create_active_run(&repository, &control, &versioned, run_id.clone()).await;
    let fence = repository
        .claim_scheduler_run(
            key("parallel-failure-claim", &run_id),
            ClaimSchedulerRunCommand::new(run_id.clone(), "scheduler-parallel-failure", 60)
                .unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap()
        .fence()
        .unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 128)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(
            SchedulerQuiescence::WaitingForTask { .. }
                | SchedulerQuiescence::WaitingForChildren { .. }
        )
    ));
    let claims = repository
        .claim_scheduler_tasks("parallel-held-workers", 60, 2)
        .await
        .unwrap();
    assert_eq!(claims.len(), 2);
    let mut renewed = Vec::new();
    for claim in claims {
        assert!(matches!(
            repository
                .mark_scheduler_task_started(&claim)
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        renewed.push(
            match repository
                .heartbeat_scheduler_task(&claim, 60)
                .await
                .unwrap()
            {
                SchedulerTaskHeartbeatOutcome::Renewed(value) => value,
                SchedulerTaskHeartbeatOutcome::LeaseLost
                | SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(_) => {
                    panic!("fresh parallel worker claim lost its lease")
                }
            },
        );
    }
    let failing = renewed
        .iter()
        .find(|claim| claim.envelope().request().implementation() == "fixture.failure")
        .unwrap()
        .clone();
    let sibling = renewed
        .iter()
        .find(|claim| claim.envelope().request().implementation() == "fixture.sibling")
        .unwrap()
        .clone();
    let failure = WorkerFailure::new(
        WorkerFailureClass::InfrastructureFailure,
        "PROVIDER_UNAVAILABLE",
        false,
    )
    .unwrap();
    let outcome = SchedulerTaskOutcome::Failed(
        TerminalSchedulerWorkerFailurePolicy
            .freeze(&failing, &failure)
            .unwrap(),
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&failing, &outcome)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));

    let mut sibling_cancelled = false;
    for _ in 0..32 {
        match drive_scheduler_once(&repository, &linked, &fence, &NoSchedulerCrash)
            .await
            .unwrap()
        {
            SchedulerDriveOutcome::Applied(_) => {}
            other => panic!("parallel failure stopped before sibling cancellation: {other:?}"),
        }
        sibling_cancelled = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM task_outbox
             WHERE run_id=$1 AND task_id=$2 AND task_state='dead'
               AND last_error_code='SCOPE_CANCELLED')",
        )
        .bind(run_id.as_str())
        .bind(sibling.task_id().as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        if sibling_cancelled {
            break;
        }
    }
    assert!(
        sibling_cancelled,
        "fatal leg did not cancel its live sibling"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT lifecycle FROM workflow_runs WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        "active",
        "the Run must not fail until sibling cancellation/drain is durable"
    );
    assert_eq!(
        repository
            .heartbeat_scheduler_task(&sibling, 60)
            .await
            .unwrap(),
        SchedulerTaskHeartbeatOutcome::LeaseLost
    );
    let output = sibling.envelope().request().outputs().first().unwrap();
    let late_success = SchedulerTaskOutcome::Succeeded(
        SchedulerTaskSuccess::inline(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("late-sibling")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
        .unwrap(),
    );
    assert_eq!(
        repository
            .commit_scheduler_task_outcome(&sibling, &late_success)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict
    );
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 128)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunFailed)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events
             WHERE run_id=$1 AND kind='run.lifecycle_changed'
               AND safe_payload->>'lifecycle'='failed'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM join_arrivals
             WHERE run_id=$1 AND settlement_class='safe_failure'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0,
        "an infrastructure failure must remain internal"
    );
}

#[tokio::test]
async fn postgres_all_settled_collects_only_typed_safe_business_failures() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let effect_policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::ReadOnly,
        EffectIdempotency::Idempotent,
        1,
        0,
        0,
        1_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let (plan, descriptors) =
        compile_fixture_with_policy(PARALLEL_ALL_SETTLED_AGENT, Some(&effect_policy));
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let safe_error = RuntimeValue::new(json!({
        "kind": "safe_error",
        "code": "RISK_REJECTED",
        "message": "risk policy rejected the request"
    }))
    .unwrap();
    let cases = vec![
        (
            "safe",
            FixtureBehavior::Failure(
                WorkerFailure::safe_business("RISK_REJECTED", false, safe_error.clone()).unwrap(),
            ),
            true,
        ),
        (
            "infrastructure",
            FixtureBehavior::Failure(
                WorkerFailure::new(
                    WorkerFailureClass::InfrastructureFailure,
                    "PROVIDER_UNAVAILABLE",
                    false,
                )
                .unwrap(),
            ),
            false,
        ),
        ("panic", FixtureBehavior::Panic, false),
        (
            "timeout",
            FixtureBehavior::SlowSuccess(RuntimeValue::new(json!("too-late")).unwrap()),
            false,
        ),
        (
            "control",
            FixtureBehavior::Failure(
                WorkerFailure::new(
                    WorkerFailureClass::ControlTermination,
                    "SCOPE_CANCELLED",
                    false,
                )
                .unwrap(),
            ),
            false,
        ),
    ];

    for (case, risky, collectable) in cases {
        let run_id = RunId::new(format!("run_scheduler_pg_all_settled_{case}")).unwrap();
        create_active_run(&repository, &control, &versioned, run_id.clone()).await;
        let fence = repository
            .claim_scheduler_run(
                key("all-settled-claim", &run_id),
                ClaimSchedulerRunCommand::new(
                    run_id.clone(),
                    format!("scheduler-all-settled-{case}"),
                    60,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .committed_result()
            .cloned()
            .unwrap()
            .fence()
            .unwrap();
        let registry = behavior_registry(&[
            (
                "fixture.stable",
                FixtureBehavior::Success(RuntimeValue::new(json!("stable-result")).unwrap()),
            ),
            ("fixture.risky", risky),
        ]);
        let terminal = drive_parallel_with_workers(
            &repository,
            &linked,
            &fence,
            &registry,
            &FrozenSchedulerWorkerFailurePolicy,
            &format!("worker-all-settled-{case}"),
        )
        .await;
        assert_eq!(
            terminal,
            if collectable {
                SchedulerQuiescence::RunSucceeded
            } else {
                SchedulerQuiescence::RunFailed
            },
            "unexpected all_settled terminal for {case}"
        );
        let safe_arrivals = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM join_arrivals
             WHERE run_id=$1 AND settlement_class='safe_failure'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(safe_arrivals, i64::from(collectable), "case={case}");
        let completed_joins = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_checkpoints
             WHERE run_id=$1 AND fact_payload->'action'->>'kind'='complete_fork'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(completed_joins, i64::from(collectable), "case={case}");
        if collectable {
            let output = sqlx::query_scalar::<_, serde_json::Value>(
                "SELECT p.inline_value FROM workflow_runs r JOIN payloads p
                   ON p.run_id=r.run_id AND p.payload_id=r.output_payload_id
                 WHERE r.run_id=$1",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
            assert_eq!(
                output,
                json!({
                    "stable": {"kind": "ok", "value": "stable-result"},
                    "risky": {"kind": "error", "error": safe_error.value()}
                })
            );
        }
    }
}

#[tokio::test]
async fn postgres_nested_parallel_completes_after_repository_and_worker_runtime_rebuild() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let (plan, descriptors) = compile_fixture(NESTED_PARALLEL_AGENT);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let run_id = RunId::new("run_scheduler_pg_nested_parallel_rebuild").unwrap();
    create_active_run(&repository, &control, &versioned, run_id.clone()).await;
    let fence = repository
        .claim_scheduler_run(
            key("nested-parallel-claim", &run_id),
            ClaimSchedulerRunCommand::new(run_id.clone(), "scheduler-nested-before", 60).unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap()
        .fence()
        .unwrap();
    let before_registry = behavior_registry(&[
        (
            "fixture.left",
            FixtureBehavior::Success(RuntimeValue::new(json!("left")).unwrap()),
        ),
        (
            "fixture.right",
            FixtureBehavior::Success(RuntimeValue::new(json!("right")).unwrap()),
        ),
        (
            "fixture.plain",
            FixtureBehavior::Success(RuntimeValue::new(json!("plain")).unwrap()),
        ),
    ]);
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 128)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(
            SchedulerQuiescence::WaitingForTask { .. }
                | SchedulerQuiescence::WaitingForChildren { .. }
        )
    ));
    assert!(matches!(
        consume_scheduler_task_once(
            &repository,
            &before_registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "nested-worker-before",
            60,
            64,
            CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::Committed { .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT lifecycle FROM workflow_runs WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        "active"
    );

    drop(before_registry);
    drop(repository);
    let repository = PostgresDurableRepository::from_pool(control.clone());
    let after_registry = behavior_registry(&[
        (
            "fixture.left",
            FixtureBehavior::Success(RuntimeValue::new(json!("left")).unwrap()),
        ),
        (
            "fixture.right",
            FixtureBehavior::Success(RuntimeValue::new(json!("right")).unwrap()),
        ),
        (
            "fixture.plain",
            FixtureBehavior::Success(RuntimeValue::new(json!("plain")).unwrap()),
        ),
    ]);
    assert_eq!(
        drive_parallel_with_workers(
            &repository,
            &linked,
            &fence,
            &after_registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "nested-worker-after",
        )
        .await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM fork_groups
             WHERE run_id=$1 AND group_state='settled'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM join_arrivals WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_outbox WHERE run_id=$1 AND task_state='acked'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_checkpoints
             WHERE run_id=$1 AND fact_payload->'action'->>'kind'='complete_fork'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2
    );
}

#[tokio::test]
async fn postgres_join_before_and_after_commit_crashes_replay_idempotently() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let (plan, descriptors) = compile_fixture(PARALLEL_AGENT);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let registry = behavior_registry(&[
        (
            "fixture.technical",
            FixtureBehavior::Success(RuntimeValue::new(json!("technical")).unwrap()),
        ),
        (
            "fixture.risk",
            FixtureBehavior::Success(RuntimeValue::new(json!("risk")).unwrap()),
        ),
    ]);

    for (suffix, crash_point) in [
        ("before", SchedulerCrashPoint::BeforeJoinCommit),
        ("after", SchedulerCrashPoint::AfterJoinCommit),
    ] {
        let run_id = RunId::new(format!("run_scheduler_pg_join_crash_{suffix}")).unwrap();
        create_active_run(&repository, &control, &versioned, run_id.clone()).await;
        let fence = repository
            .claim_scheduler_run(
                key("join-crash-claim", &run_id),
                ClaimSchedulerRunCommand::new(
                    run_id.clone(),
                    format!("scheduler-join-{suffix}"),
                    60,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .committed_result()
            .cloned()
            .unwrap()
            .fence()
            .unwrap();
        let crash = FailOnceSchedulerCrash::new(crash_point);
        let mut crashed = false;
        for _ in 0..32 {
            match drive_scheduler_until_quiescent(&repository, &linked, &fence, &crash, 128).await {
                Err(error) => {
                    assert_eq!(error.code(), "ENGINE_REPOSITORY_SCHEDULER_CRASH_INJECTED");
                    crashed = true;
                    break;
                }
                Ok(SchedulerRecoveryOutcome::Quiescent(
                    SchedulerQuiescence::WaitingForTask { .. }
                    | SchedulerQuiescence::WaitingForChildren { .. },
                )) => {
                    drain_scheduler_workers(
                        &repository,
                        &registry,
                        &TerminalSchedulerWorkerFailurePolicy,
                        &format!("join-crash-worker-{suffix}"),
                    )
                    .await;
                }
                Ok(SchedulerRecoveryOutcome::ActionBudgetExhausted) => {}
                other => panic!("join crash point was bypassed: {other:?}"),
            }
        }
        assert!(crashed && crash.fired());

        let recovered = PostgresDurableRepository::from_pool(control.clone());
        assert_eq!(
            drive_parallel_with_workers(
                &recovered,
                &linked,
                &fence,
                &registry,
                &TerminalSchedulerWorkerFailurePolicy,
                &format!("join-recovered-worker-{suffix}"),
            )
            .await,
            SchedulerQuiescence::RunSucceeded
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scheduler_checkpoints
                 WHERE run_id=$1 AND fact_payload->'action'->>'kind'='complete_fork'",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            1,
            "Join completion must be exactly once after {crash_point:?}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM join_arrivals WHERE run_id=$1")
                .bind(run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM public_event_outbox
                 WHERE run_id=$1 AND event_kind='run.completed' AND is_terminal=TRUE",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            1
        );
    }
}

#[tokio::test]
async fn postgres_result_crash_boundaries_recover_without_duplicate_terminal_facts() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let retry_policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::Mutating,
        EffectIdempotency::Idempotent,
        2,
        0,
        0,
        60_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let (plan, descriptors) = compile_fixture_with_policy(LINEAR_AGENT, Some(&retry_policy));
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    for (suffix, crash_point, expected_calls) in [
        (
            "before_external",
            SchedulerCrashPoint::BeforeExternalCall,
            1_usize,
        ),
        (
            "after_external",
            SchedulerCrashPoint::AfterExternalCall,
            2_usize,
        ),
        (
            "before_result",
            SchedulerCrashPoint::BeforeResultCommit,
            2_usize,
        ),
        (
            "after_result",
            SchedulerCrashPoint::AfterResultCommit,
            1_usize,
        ),
    ] {
        let run_id = RunId::new(format!("run_scheduler_pg_result_{suffix}")).unwrap();
        create_active_run(&repository, &control, &versioned, run_id.clone()).await;
        let lease = repository
            .claim_scheduler_run(
                key("scheduler-claim", &run_id),
                ClaimSchedulerRunCommand::new(run_id.clone(), format!("scheduler-{suffix}"), 60)
                    .unwrap(),
            )
            .await
            .unwrap()
            .committed_result()
            .cloned()
            .unwrap();
        let fence = lease.fence().unwrap();
        drive_to_task(&repository, &linked, &fence).await;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = WorkerExecutorRegistry::new();
        registry
            .register(
                SchedulerTaskKind::Action,
                "fixture.answer",
                VersionTag::new("1").unwrap(),
                VersionTag::new("worker-1").unwrap(),
                Arc::new(CountingExecutor {
                    calls: calls.clone(),
                }),
            )
            .unwrap();
        let crash = FailOnceSchedulerCrash::new(crash_point);
        let error = consume_scheduler_task_once(
            &repository,
            &registry,
            &FrozenSchedulerWorkerFailurePolicy,
            &format!("worker-{suffix}-a"),
            60,
            64,
            CancellationToken::new(),
            &crash,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), "ENGINE_REPOSITORY_SCHEDULER_CRASH_INJECTED");

        if crash_point != SchedulerCrashPoint::AfterResultCommit {
            sqlx::query(
                "UPDATE task_outbox SET claim_expires_at=CURRENT_TIMESTAMP - INTERVAL '1 second'
                 WHERE run_id=$1 AND task_state='claimed'",
            )
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap();
        }
        let recovered = consume_scheduler_task_once(
            &repository,
            &registry,
            &FrozenSchedulerWorkerFailurePolicy,
            &format!("worker-{suffix}-b"),
            60,
            64,
            CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap();
        if crash_point == SchedulerCrashPoint::AfterResultCommit {
            assert_eq!(
                recovered,
                SchedulerWorkerPumpOutcome::AcknowledgedRecoveredResult
            );
        } else {
            assert!(matches!(
                recovered,
                SchedulerWorkerPumpOutcome::Committed {
                    acknowledged: false,
                    ..
                }
            ));
            assert!(matches!(
                consume_scheduler_task_once(
                    &repository,
                    &registry,
                    &FrozenSchedulerWorkerFailurePolicy,
                    &format!("worker-{suffix}-c"),
                    60,
                    64,
                    CancellationToken::new(),
                    &NoSchedulerCrash,
                )
                .await
                .unwrap(),
                SchedulerWorkerPumpOutcome::Committed {
                    acknowledged: true,
                    ..
                }
            ));
        }
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM node_attempts WHERE run_id=$1",)
                .bind(run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            if crash_point == SchedulerCrashPoint::AfterResultCommit {
                1
            } else {
                2
            }
        );
        assert!(matches!(
            drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 32,)
                .await
                .unwrap(),
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
        ));
        let terminal_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM public_event_outbox
             WHERE run_id=$1 AND event_kind='run.completed' AND is_terminal=TRUE",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(terminal_count, 1);
    }
}

#[tokio::test]
async fn postgres_frozen_worker_timeout_is_terminal_and_fences_late_success() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let timeout_policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::ReadOnly,
        EffectIdempotency::Idempotent,
        1,
        0,
        0,
        50,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let (plan, descriptors) = compile_fixture_with_policy(LINEAR_AGENT, Some(&timeout_policy));
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let run_id = RunId::new("run_scheduler_pg_worker_timeout").unwrap();
    create_active_run(&repository, &control, &versioned, run_id.clone()).await;
    let lease = repository
        .claim_scheduler_run(
            key("timeout-claim", &run_id),
            ClaimSchedulerRunCommand::new(run_id.clone(), "scheduler-timeout", 60).unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap();
    let fence = lease.fence().unwrap();
    drive_to_task(&repository, &linked, &fence).await;
    let mut registry = WorkerExecutorRegistry::new();
    registry
        .register(
            SchedulerTaskKind::Action,
            "fixture.answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            Arc::new(SlowExecutor),
        )
        .unwrap();
    assert!(matches!(
        consume_scheduler_task_once(
            &repository,
            &registry,
            &FrozenSchedulerWorkerFailurePolicy,
            "worker-timeout",
            60,
            1,
            CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::Committed {
            acknowledged: false,
            ..
        }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=$1 AND attempt_no=1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "timed_out"
    );
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunFailed)
    ));
}

#[tokio::test]
async fn postgres_retry_backoff_uses_database_time_and_rejects_corrupted_lineage() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::ReadOnly,
        EffectIdempotency::Idempotent,
        2,
        1_000,
        1_000,
        10_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let (plan, descriptors) = compile_fixture_with_policy(LINEAR_AGENT, Some(&policy));
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let run_id = RunId::new("run_scheduler_pg_retry_backoff_lineage").unwrap();
    create_active_run(&repository, &control, &versioned, run_id.clone()).await;
    let lease = repository
        .claim_scheduler_run(
            key("retry-backoff-claim", &run_id),
            ClaimSchedulerRunCommand::new(run_id.clone(), "scheduler-retry-backoff", 60).unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap();
    let fence = lease.fence().unwrap();
    drive_to_task(&repository, &linked, &fence).await;
    let initial_claim = repository
        .claim_scheduler_tasks("worker-retry-lineage-initial", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(matches!(
        repository
            .mark_scheduler_task_started(&initial_claim)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let initial_claim = match repository
        .heartbeat_scheduler_task(&initial_claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed) => renewed,
        other => panic!("fresh retry claim did not renew: {other:?}"),
    };
    let failure = WorkerFailure::new(
        WorkerFailureClass::InfrastructureFailure,
        "TRANSIENT_RETRY_BACKOFF",
        true,
    )
    .unwrap();
    let retry = SchedulerTaskOutcome::Failed(
        FrozenSchedulerWorkerFailurePolicy
            .freeze(&initial_claim, &failure)
            .unwrap(),
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&initial_claim, &retry)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));

    let (available_at, last_error_code, baseline_projection_version, baseline_publish_attempts) =
        sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, String, i64, i32)>(
            "SELECT available_at,last_error_code,projection_version,publish_attempts
             FROM task_outbox WHERE run_id=$1 AND task_state='pending'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(last_error_code, "TRANSIENT_RETRY_BACKOFF");
    assert!(repository
        .claim_scheduler_tasks("worker-retry-lineage-too-early", 60, 1)
        .await
        .unwrap()
        .is_empty());

    sqlx::query(
        "UPDATE task_outbox SET available_at=clock_timestamp()-INTERVAL '1 second'
         WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(repository
        .claim_scheduler_tasks("worker-retry-lineage-forged-time", 60, 1)
        .await
        .is_err());
    sqlx::query("UPDATE task_outbox SET available_at=$1 WHERE run_id=$2")
        .bind(available_at)
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    sqlx::query("UPDATE task_outbox SET last_error_code='FORGED_RETRY_ERROR' WHERE run_id=$1")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(repository
        .claim_scheduler_tasks("worker-retry-lineage-forged-error", 60, 1)
        .await
        .is_err());
    sqlx::query("UPDATE task_outbox SET last_error_code=$1 WHERE run_id=$2")
        .bind(&last_error_code)
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();

    let (retry_transition, retry_payload) = sqlx::query_as::<_, (String, serde_json::Value)>(
        "SELECT transition_key,fact_payload FROM scheduler_checkpoints
             WHERE run_id=$1 AND checkpoint_kind='task_retry_scheduled'",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(
        retry_payload["next_envelope"]["attempt_no"],
        json!(2),
        "the retry checkpoint must literally bind the complete next envelope",
    );
    sqlx::query(
        "UPDATE scheduler_checkpoints
         SET fact_payload=jsonb_set(fact_payload,'{next_envelope,attempt_no}','999'::jsonb)
         WHERE run_id=$1 AND checkpoint_kind='task_retry_scheduled'",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(repository
        .claim_scheduler_tasks("worker-retry-lineage-forged-checkpoint", 60, 1)
        .await
        .is_err());
    sqlx::query(
        "UPDATE scheduler_checkpoints SET fact_payload=$1
         WHERE run_id=$2 AND checkpoint_kind='task_retry_scheduled'",
    )
    .bind(&retry_payload)
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();

    let original_event_kind = sqlx::query_scalar::<_, String>(
        "SELECT kind FROM execution_events WHERE run_id=$1 AND transition_key=$2",
    )
    .bind(run_id.as_str())
    .bind(&retry_transition)
    .fetch_one(&control)
    .await
    .unwrap();
    assert!(
        sqlx::query(
            "UPDATE execution_events SET kind='forged.retry.event'
         WHERE run_id=$1 AND transition_key=$2",
        )
        .bind(run_id.as_str())
        .bind(&retry_transition)
        .execute(&control)
        .await
        .is_err(),
        "the immutable retry event authority must reject tampering before claim"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT kind FROM execution_events WHERE run_id=$1 AND transition_key=$2",
        )
        .bind(run_id.as_str())
        .bind(&retry_transition)
        .fetch_one(&control)
        .await
        .unwrap(),
        original_event_kind,
    );

    assert_eq!(
        sqlx::query_as::<_, (String, i64, i32)>(
            "SELECT task_state,projection_version,publish_attempts
             FROM task_outbox WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        (
            "pending".to_owned(),
            baseline_projection_version,
            baseline_publish_attempts,
        ),
        "every forged claim transaction must roll back completely",
    );
    let retried = repository
        .claim_scheduler_tasks("worker-retry-lineage-authorized", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(retried.envelope().attempt_no().get(), 2);
    assert_eq!(retried.envelope().lease_epoch().get(), 2);
    assert_ne!(
        retried.envelope().fencing_token(),
        initial_claim.envelope().fencing_token()
    );
}

#[tokio::test]
async fn postgres_scheduler_lease_race_expires_and_fences_the_zombie() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let (plan, descriptors) = fixture_plan();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let run_id = RunId::new("run_scheduler_pg_lease_race").unwrap();
    create_active_run(&repository, &control, &versioned, run_id.clone()).await;

    let left_repository = repository.clone();
    let right_repository = repository.clone();
    let left_run = run_id.clone();
    let right_run = run_id.clone();
    let (left, right) = tokio::join!(
        left_repository.claim_scheduler_run(
            key("lease-left", &left_run),
            ClaimSchedulerRunCommand::new(left_run, "scheduler-left", 60).unwrap(),
        ),
        right_repository.claim_scheduler_run(
            key("lease-right", &right_run),
            ClaimSchedulerRunCommand::new(right_run, "scheduler-right", 60).unwrap(),
        ),
    );
    let (winner, loser) = match (left.unwrap(), right.unwrap()) {
        (TransitionOutcome::Committed { result }, loser) => (result, loser),
        (loser, TransitionOutcome::Committed { result }) => (result, loser),
        outcomes => panic!("expected one scheduler winner: {outcomes:?}"),
    };
    assert_eq!(loser, TransitionOutcome::StateConflict);
    let zombie_fence = winner.fence().unwrap();
    sqlx::query(
        "UPDATE workflow_runs SET scheduler_lease_expires_at=CURRENT_TIMESTAMP - INTERVAL '1 second'
         WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    let replacement = repository
        .claim_scheduler_run(
            key("lease-replacement", &run_id),
            ClaimSchedulerRunCommand::new(run_id.clone(), "scheduler-replacement", 60).unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap();
    assert!(replacement.lease_epoch() > winner.lease_epoch());
    assert_eq!(
        drive_scheduler_once(&repository, &linked, &zombie_fence, &NoSchedulerCrash)
            .await
            .unwrap(),
        SchedulerDriveOutcome::Fenced
    );
    assert!(matches!(
        drive_scheduler_once(
            &repository,
            &linked,
            &replacement.fence().unwrap(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerDriveOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn postgres_cancel_run_fences_a_claimed_worker_and_allows_pre_admission_cancel() {
    let Some((repository, control, _admin, _schema)) = isolated_repository().await else {
        return;
    };
    let (plan, descriptors) = fixture_plan();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();

    // Cancellation is Run-level and remains valid when the deterministic
    // entry activation has never been admitted.
    let empty_run = RunId::new("run_scheduler_pg_cancel_before_admit").unwrap();
    create_active_run(&repository, &control, &versioned, empty_run.clone()).await;
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
            termination_intent_reason='cancelled',termination_intent_transition_key=$1,
            termination_intent_at=CURRENT_TIMESTAMP WHERE run_id=$2",
    )
    .bind(key("cancel-intent-empty", &empty_run).as_str())
    .bind(empty_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    let empty_lease = repository
        .claim_scheduler_run(
            key("cancel-claim-empty", &empty_run),
            ClaimSchedulerRunCommand::new(empty_run.clone(), "scheduler-cancel-empty", 60).unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &repository,
            &linked,
            &empty_lease.fence().unwrap(),
            &NoSchedulerCrash,
            8,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunCancelled)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT lifecycle FROM workflow_runs WHERE run_id=$1")
            .bind(empty_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        "cancelled"
    );

    // Once a task has been claimed, cancellation atomically closes its task
    // and attempt. The old worker claim cannot publish a late value.
    let active_run = RunId::new("run_scheduler_pg_cancel_claimed_worker").unwrap();
    create_active_run(&repository, &control, &versioned, active_run.clone()).await;
    let active_lease = repository
        .claim_scheduler_run(
            key("cancel-claim-active", &active_run),
            ClaimSchedulerRunCommand::new(active_run.clone(), "scheduler-cancel-active", 60)
                .unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap();
    let active_fence = active_lease.fence().unwrap();
    drive_to_task(&repository, &linked, &active_fence).await;
    let claim = repository
        .claim_scheduler_tasks("worker-cancel-zombie", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let started_claim = claim.clone();
    let start_receipt = match repository
        .mark_scheduler_task_started(&started_claim)
        .await
        .unwrap()
    {
        TransitionOutcome::Committed { result } => result,
        other => panic!("initial task start did not commit: {other:?}"),
    };
    let claim = match repository
        .heartbeat_scheduler_task(&started_claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed) => {
            assert!(renewed.task_projection_version() > claim.task_projection_version());
            renewed
        }
        SchedulerTaskHeartbeatOutcome::LeaseLost
        | SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(_) => {
            panic!("fresh worker heartbeat lost its lease")
        }
    };
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
            termination_intent_reason='cancelled',termination_intent_transition_key=$1,
            termination_intent_at=CURRENT_TIMESTAMP WHERE run_id=$2",
    )
    .bind(key("cancel-intent-active", &active_run).as_str())
    .bind(active_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .heartbeat_scheduler_task(&claim, 60)
            .await
            .unwrap(),
        SchedulerTaskHeartbeatOutcome::LeaseLost,
        "terminating Run must cancel the physical worker authority"
    );
    assert!(matches!(
        drive_scheduler_once(&repository, &linked, &active_fence, &NoSchedulerCrash)
            .await
            .unwrap(),
        SchedulerDriveOutcome::Applied(_)
    ));
    assert_eq!(
        repository
            .mark_scheduler_task_started(&started_claim)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay {
            authoritative: start_receipt,
        },
        "terminal projection changes must not hide the immutable start receipt"
    );
    let output = claim.envelope().request().outputs().first().unwrap();
    let late = SchedulerTaskOutcome::Succeeded(
        SchedulerTaskSuccess::inline(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("too-late")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
        .unwrap(),
    );
    assert_eq!(
        repository
            .commit_scheduler_task_outcome(&claim, &late)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT lifecycle FROM workflow_runs WHERE run_id=$1")
            .bind(active_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        "cancelled"
    );
}
