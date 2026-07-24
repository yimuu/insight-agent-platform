use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use insight_agent_platform::{
    dsl::{compile_source, CompileOptions},
    engine::{
        control::{ControlEmissionSlot, ControlFrame},
        plan::{
            DescriptorConfigurationContract, DescriptorContract, DescriptorContractRegistry,
            DescriptorFieldContract, DescriptorValueSchema, LeafTaskKind, LinkedPlan, NodeKind,
            Plan, PlanIndex, PlanInputContract, PlanProperty, PlanType, PortDirection,
            SubflowContractRegistry, SubflowInterfaceContract, VersionTag, WorkerContract,
            WorkerInputPortContract,
        },
        repository::{
            consume_scheduler_task_once, drive_scheduler_until_quiescent, ClaimSchedulerRunCommand,
            CreateReuseCandidateCommand, CreateRunCommand, DurableRepository,
            FencedSchedulerRunCommand, MigrationMappingCompatibility, NoSchedulerCrash,
            PlanInstallOutcome, PostgresDurableRepository, RecoveryDurableRepository,
            RecoveryRevisionSpec, RedriveRunCommand, ReuseCompatibility, SchedulerLeaseRepository,
            SchedulerRecoveryOutcome, SchedulerWorkerPumpOutcome, SqliteDurableRepository,
            TerminalSchedulerWorkerFailurePolicy, VersionedPlan, REPOSITORY_REDRIVE_REQUIRES_FORK,
        },
        scheduler::{
            DeterministicIds, LogicalOccurrence, ReuseAdmissionContract, SchedulerAction,
            SchedulerIntent,
        },
        ActivationId, ContentHash, ControlTokenProvenance, DefinitionRevisionId,
        DeploymentRevisionId, EffectEvidence, EffectId, EffectIdempotency, ExecutionRevisionPin,
        LeafTaskExecutor, MigrationNodeMapping, NodeId, PortId, RunId, RuntimeValue,
        SchedulerQuiescence, SchedulerTaskKind, ScopeInstanceId, TaskExecutionRequest,
        TaskExecutionResult, TransitionKey, TransitionOutcome, WorkerCancellation,
        WorkerEffectClass, WorkerEffectPolicy, WorkerExecutionContext, WorkerExecutorRegistry,
        WorkerFailure, WorkerFailureClass,
    },
};
use serde_json::json;
use sqlx::{
    postgres::PgPoolOptions, sqlite::SqliteConnectOptions, AssertSqlSafe, PgPool, Row, SqlitePool,
};
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
      call: fixture.recovery_answer
      inputs:
        question: $question
      response: string
    - return: $answer
"#;

const DEPENDENCY_CHAIN_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
output: string
workflow:
  steps:
    - id: first
      type: action
      call: fixture.recovery_first
      inputs:
        question: $question
      response: string
    - id: second
      type: action
      call: fixture.recovery_second
      inputs:
        question: $first
      response: string
    - return: $second
"#;

const DYNAMIC_MAP_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  items: string[]
output: string[]
workflow:
  steps:
    - id: rendered
      map:
        items: $items
        as: item
        steps:
          - id: render_item
            type: action
            call: fixture.dynamic_map
            inputs: {item: $item}
            response: string
          - yield: $render_item
    - return: $rendered
"#;

const DYNAMIC_LOOP_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {seed: string}
output: string
workflow:
  steps:
    - id: reasoning
      loop:
        initial: $seed
        as: state
        until: false
        max_iterations: 1
        steps:
          - id: next_state
            type: action
            call: fixture.dynamic_loop
            inputs: {state: $state}
            response: string
          - continue: $next_state
    - return: $reasoning
"#;

fn key(label: &str, run_id: &RunId) -> TransitionKey {
    TransitionKey::derive("scheduler.recovery.e2e.v1", &[label, run_id.as_str()]).unwrap()
}

fn compile_fixture_with_policy(
    policy: Option<&WorkerEffectPolicy>,
) -> (Plan, DescriptorContractRegistry) {
    compile_source_fixture(
        LINEAR_AGENT,
        "scheduler_recovery_fixture_v1",
        "scheduler-recovery-fixture.yaml",
        policy,
    )
}

fn compile_source_fixture(
    source: &str,
    revision: &str,
    file_name: &str,
    policy: Option<&WorkerEffectPolicy>,
) -> (Plan, DescriptorContractRegistry) {
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new(revision).unwrap(),
            file_name,
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
        let worker = WorkerContract::new(
            LeafTaskKind::Action,
            VersionTag::new("worker-1").unwrap(),
            inputs,
            outputs,
        );
        let worker = match policy {
            Some(policy) => worker.with_effect_policy(policy.clone()),
            None => worker,
        };
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

fn compile_fixture() -> (Plan, DescriptorContractRegistry) {
    compile_fixture_with_policy(None)
}

fn compile_dependency_chain_fixture() -> (Plan, DescriptorContractRegistry) {
    compile_source_fixture(
        DEPENDENCY_CHAIN_AGENT,
        "scheduler_recovery_dependency_chain_v1",
        "scheduler-recovery-dependency-chain.yaml",
        None,
    )
}

fn compile_dynamic_map_fixture() -> (Plan, DescriptorContractRegistry) {
    compile_source_fixture(
        DYNAMIC_MAP_AGENT,
        "scheduler_recovery_dynamic_map_v1",
        "scheduler-recovery-dynamic-map.yaml",
        None,
    )
}

fn compile_dynamic_loop_fixture() -> (Plan, DescriptorContractRegistry) {
    compile_source_fixture(
        DYNAMIC_LOOP_AGENT,
        "scheduler_recovery_dynamic_loop_v1",
        "scheduler-recovery-dynamic-loop.yaml",
        None,
    )
}

fn versioned(plan: &Plan) -> VersionedPlan {
    versioned_named(
        plan,
        "scheduler-recovery-fixture",
        "scheduler_recovery_deployment_v1",
    )
}

fn versioned_named(plan: &Plan, definition_id: &str, deployment_id: &str) -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        definition_id,
        "scheduler-recovery-agent",
        "Scheduler recovery fixture",
        DeploymentRevisionId::new(deployment_id).unwrap(),
        "expression-3.0.0",
        json!({"format": "dsl"}),
        plan,
        json!({"fixture": "descriptor-v1"}),
        json!({}),
        json!({"fixture": "worker-1"}),
    )
    .unwrap()
}

fn compile_subflow_fixture() -> (
    Plan,
    VersionedPlan,
    Plan,
    VersionedPlan,
    SubflowContractRegistry,
) {
    let child_source = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - return: $question
"#;
    let (child_plan, _) = compile_source_fixture(
        child_source,
        "scheduler_recovery_subflow_child_v1",
        "scheduler-recovery-subflow-child.yaml",
        None,
    );
    let child = versioned_named(
        &child_plan,
        "scheduler-recovery-subflow-child",
        "scheduler_recovery_subflow_child_deployment_v1",
    );
    let parent_source = format!(
        r#"api_version: insight.agent/v1
kind: agent
inputs: {{question: string}}
output: string
workflow:
  steps:
    - id: child
      type: call
      definition_revision: {}
      interface_version: child-v1
      input: {{question: $question}}
      response: string
    - return: $child
"#,
        child.definition_revision_id().as_str()
    );
    let (parent_plan, _) = compile_source_fixture(
        &parent_source,
        "scheduler_recovery_subflow_parent_v1",
        "scheduler-recovery-subflow-parent.yaml",
        None,
    );
    let _unpinned_parent_fixture = versioned_named(
        &parent_plan,
        "scheduler-recovery-subflow-parent",
        "scheduler_recovery_subflow_parent_deployment_v1",
    );
    let index = PlanIndex::new(&parent_plan).unwrap();
    let call = parent_plan
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::SubflowCall(_)))
        .unwrap();
    let NodeKind::SubflowCall(descriptor) = call.kind() else {
        unreachable!()
    };
    let parent = VersionedPlan::from_verified_plan(
        "scheduler-recovery-subflow-parent",
        "scheduler-recovery-agent",
        "Scheduler recovery fixture",
        DeploymentRevisionId::new("scheduler_recovery_subflow_parent_deployment_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "dsl"}),
        &parent_plan,
        json!({"fixture": "descriptor-v1"}),
        json!([{
            "node_id": call.id(),
            "binding": {
                "adapter": "durable_subflow",
                "definition_revision_id": child.definition_revision_id(),
                "deployment_revision_id": child.deployment_revision_id(),
                "plan_hash": child.plan_hash(),
                "binding_hash": child.binding_hash(),
                "interface_version": descriptor.interface_version,
            }
        }]),
        json!({"fixture": "worker-1"}),
    )
    .unwrap();
    let input_contract = PlanInputContract::new(PlanType::Object {
        properties: descriptor
            .inputs
            .iter()
            .map(|(name, id)| {
                let port = index.data_port(id).unwrap();
                (
                    name.as_str().to_owned(),
                    PlanProperty::new(port.value_type().clone(), port.required()).unwrap(),
                )
            })
            .collect(),
        additional_properties: None,
    });
    let outputs = index
        .data_outputs(call.id())
        .iter()
        .map(|id| {
            let port = index.data_port(id).unwrap();
            (port.name().clone(), port.value_type().clone())
        })
        .collect::<BTreeMap<_, _>>();
    let mut subflows = SubflowContractRegistry::new();
    subflows
        .register(SubflowInterfaceContract::new(
            ExecutionRevisionPin::new(
                child.definition_revision_id().clone(),
                child.deployment_revision_id().clone(),
                child.plan_hash().clone(),
                child.binding_hash().clone(),
            ),
            VersionTag::new("child-v1").unwrap(),
            input_contract,
            outputs,
            parent_plan.metadata().error_type().clone(),
        ))
        .unwrap();
    (child_plan, child, parent_plan, parent, subflows)
}

fn recovery_revision(versioned: &VersionedPlan) -> RecoveryRevisionSpec {
    RecoveryRevisionSpec::new(
        versioned.definition_id(),
        ExecutionRevisionPin::new(
            versioned.definition_revision_id().clone(),
            versioned.deployment_revision_id().clone(),
            versioned.plan_hash().clone(),
            versioned.binding_hash().clone(),
        ),
    )
    .unwrap()
}

fn identity_migration_mapping_for_node(
    plan: &Plan,
    node_id: &str,
) -> MigrationMappingCompatibility {
    let node = NodeId::new(node_id).unwrap();
    let ports = plan
        .data_ports()
        .iter()
        .filter(|port| port.owner() == &node)
        .map(|port| (port.id().clone(), port.id().clone()))
        .collect();
    let source = MigrationNodeMapping::new(
        node.clone(),
        node,
        ports,
        ContentHash::from_bytes(b"identity-mapping-input"),
        ContentHash::from_bytes(b"identity-mapping-output"),
        ContentHash::from_bytes(b"identity-mapping-effect"),
        false,
        false,
    );
    MigrationMappingCompatibility::new(source.clone(), source).unwrap()
}

#[derive(Clone)]
struct CountingExecutor {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LeafTaskExecutor for CountingExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let output = request.outputs().first().expect("fixture output");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("durable-recovery-answer")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

fn workers(calls: Arc<AtomicUsize>) -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Action,
            "fixture.recovery_answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            Arc::new(CountingExecutor { calls }),
        )
        .unwrap();
    workers
}

fn dependency_chain_workers(calls: Arc<AtomicUsize>) -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    for implementation in ["fixture.recovery_first", "fixture.recovery_second"] {
        workers
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
    workers
}

fn dynamic_worker(implementation: &str, calls: Arc<AtomicUsize>) -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Action,
            implementation,
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            Arc::new(CountingExecutor { calls }),
        )
        .unwrap();
    workers
}

#[derive(Clone)]
struct FailOnceThenSucceedExecutor {
    calls: Arc<AtomicUsize>,
    effect_ids: Arc<Mutex<Vec<EffectId>>>,
}

#[async_trait]
impl LeafTaskExecutor for FailOnceThenSucceedExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        self.effect_ids
            .lock()
            .unwrap()
            .push(request.effect_id().clone());
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(WorkerFailure::new(
                WorkerFailureClass::EffectOutcomeUnknown,
                "PROVIDER_OUTCOME_UNKNOWN",
                false,
            )
            .unwrap());
        }
        let output = request.outputs().first().expect("fixture output");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("durable-redrive-answer")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

fn fail_once_workers(
    calls: Arc<AtomicUsize>,
    effect_ids: Arc<Mutex<Vec<EffectId>>>,
) -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Action,
            "fixture.recovery_answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            Arc::new(FailOnceThenSucceedExecutor { calls, effect_ids }),
        )
        .unwrap();
    workers
}

#[derive(Clone)]
struct TerminalFailureExecutor(WorkerFailureClass);

#[async_trait]
impl LeafTaskExecutor for TerminalFailureExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        _request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let code = match self.0 {
            WorkerFailureClass::EffectOutcomeUnknown => "PROVIDER_OUTCOME_UNKNOWN",
            WorkerFailureClass::InfrastructureFailure => "PROVIDER_FAILED_AFTER_START",
            _ => panic!("unsupported redrive safety fixture failure class"),
        };
        Err(WorkerFailure::new(self.0, code, false).unwrap())
    }
}

fn terminal_failure_workers(class: WorkerFailureClass) -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Action,
            "fixture.recovery_answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            Arc::new(TerminalFailureExecutor(class)),
        )
        .unwrap();
    workers
}

fn idempotent_policy() -> WorkerEffectPolicy {
    WorkerEffectPolicy::frozen(
        WorkerEffectClass::Mutating,
        EffectIdempotency::Idempotent,
        1,
        0,
        0,
        60_000,
        WorkerCancellation::LeaseOnly,
    )
    .unwrap()
}

fn non_idempotent_policy() -> WorkerEffectPolicy {
    WorkerEffectPolicy::frozen(
        WorkerEffectClass::Mutating,
        EffectIdempotency::NonIdempotent,
        1,
        0,
        0,
        60_000,
        WorkerCancellation::LeaseOnly,
    )
    .unwrap()
}

async fn repository_and_control(
    name: &str,
) -> (
    tempfile::TempDir,
    std::path::PathBuf,
    SqliteDurableRepository,
    SqlitePool,
) {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join(format!("{name}.sqlite"));
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    (directory, database, repository, control)
}

async fn activate_and_claim_sqlite(
    _repository: &SqliteDurableRepository,
    control: &SqlitePool,
    run_id: &RunId,
    owner: &str,
) -> FencedSchedulerRunCommand {
    let fence = format!("{owner}-fence");
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
                    scheduler_lease_epoch=1,scheduler_lease_owner=?,scheduler_fencing_token=?,
                    scheduler_lease_expires_at=datetime('now','+1 hour'),
                    scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND lifecycle='created'",
        )
        .bind(owner)
        .bind(&fence)
        .bind(run_id.as_str())
        .execute(control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    FencedSchedulerRunCommand::new(run_id.clone(), owner, 1, fence).unwrap()
}

async fn execute_one_task_sqlite(
    repository: &SqliteDurableRepository,
    workers: &WorkerExecutorRegistry,
) {
    assert!(matches!(
        consume_scheduler_task_once(
            repository,
            workers,
            &TerminalSchedulerWorkerFailurePolicy,
            "scheduler-recovery-worker",
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

async fn drive_source_sqlite(
    repository: &SqliteDurableRepository,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    workers: &WorkerExecutorRegistry,
) {
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, linked, fence, &NoSchedulerCrash, 64)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    execute_one_task_sqlite(repository, workers).await;
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, linked, fence, &NoSchedulerCrash, 64)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
}

async fn drive_chain_to_success_sqlite(
    repository: &SqliteDurableRepository,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    workers: &WorkerExecutorRegistry,
) {
    for _ in 0..8 {
        match drive_scheduler_until_quiescent(repository, linked, fence, &NoSchedulerCrash, 64)
            .await
            .unwrap()
        {
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. }) => {
                execute_one_task_sqlite(repository, workers).await;
            }
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForChildren {
                ..
            }) => {
                let _ = consume_scheduler_task_once(
                    repository,
                    workers,
                    &TerminalSchedulerWorkerFailurePolicy,
                    "scheduler-dynamic-child-settlement",
                    60,
                    64,
                    CancellationToken::new(),
                    &NoSchedulerCrash,
                )
                .await
                .unwrap();
            }
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded) => return,
            other => panic!("dependency chain stopped unexpectedly: {other:?}"),
        }
    }
    panic!("dependency chain did not reach success");
}

async fn drive_dynamic_terminal_sqlite(
    repository: &SqliteDurableRepository,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    workers: &WorkerExecutorRegistry,
) -> SchedulerQuiescence {
    for _ in 0..64 {
        match drive_scheduler_until_quiescent(repository, linked, fence, &NoSchedulerCrash, 64)
            .await
            .unwrap()
        {
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. }) => {
                execute_one_task_sqlite(repository, workers).await;
            }
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForChildren {
                ..
            }) => {
                let _ = consume_scheduler_task_once(
                    repository,
                    workers,
                    &TerminalSchedulerWorkerFailurePolicy,
                    "scheduler-dynamic-child-settlement",
                    60,
                    64,
                    CancellationToken::new(),
                    &NoSchedulerCrash,
                )
                .await
                .unwrap();
            }
            SchedulerRecoveryOutcome::Quiescent(terminal @ SchedulerQuiescence::RunSucceeded)
            | SchedulerRecoveryOutcome::Quiescent(terminal @ SchedulerQuiescence::RunFailed) => {
                return terminal;
            }
            other => panic!("dynamic fixture stopped unexpectedly: {other:?}"),
        }
    }
    panic!("dynamic fixture did not terminalize");
}

#[derive(Clone)]
struct SourceReuseFacts {
    projection_version: u64,
    activation_id: ActivationId,
    node_id: NodeId,
    stable_activation_key: String,
    output_hash: ContentHash,
    effect_id: EffectId,
    provenance: ControlTokenProvenance,
    contract: ReuseAdmissionContract,
}

async fn source_reuse_facts_sqlite(control: &SqlitePool, run_id: &RunId) -> SourceReuseFacts {
    let activation = sqlx::query(
        "SELECT activation_id,node_id,stable_activation_key,output_value_hash,effect_id
         FROM node_activations WHERE run_id=? AND execution_kind='worker' AND lifecycle='succeeded'",
    )
    .bind(run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap();
    let activation_id = ActivationId::new(activation.get::<String, _>("activation_id")).unwrap();
    let node_id = NodeId::new(activation.get::<String, _>("node_id")).unwrap();
    let stable_activation_key = activation.get::<String, _>("stable_activation_key");
    let output_hash = ContentHash::parse(activation.get::<String, _>("output_value_hash")).unwrap();
    let effect_id = EffectId::new(activation.get::<String, _>("effect_id")).unwrap();

    let token = sqlx::query(
        "SELECT source_port_id,current_scope_instance_id,emission_slot,provenance_frames
         FROM control_tokens WHERE run_id=? AND source_activation_id=?",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_one(control)
    .await
    .unwrap();
    let _scheduler_emission_slot = token.get::<String, _>("emission_slot");
    let frames: Vec<ControlFrame> =
        serde_json::from_str(&token.get::<String, _>("provenance_frames")).unwrap();
    let provenance = ControlTokenProvenance::new(
        run_id.clone(),
        activation_id.clone(),
        PortId::new(token.get::<String, _>("source_port_id")).unwrap(),
        ControlEmissionSlot::ActivationOutput,
        ScopeInstanceId::new(token.get::<String, _>("current_scope_instance_id")).unwrap(),
        frames,
    )
    .unwrap();

    let intents = sqlx::query_scalar::<_, String>(
        "SELECT fact_payload FROM scheduler_checkpoints
         WHERE run_id=? AND checkpoint_kind='planned_action' ORDER BY created_at",
    )
    .bind(run_id.as_str())
    .fetch_all(control)
    .await
    .unwrap();
    let dispatch = intents
        .iter()
        .filter_map(|payload| serde_json::from_str::<SchedulerIntent>(payload).ok())
        .find(|intent| {
            matches!(
                intent.action(),
                SchedulerAction::DispatchTask { activation_id: id, .. } if id == &activation_id
            )
        })
        .expect("source DispatchTask checkpoint");
    let SchedulerAction::DispatchTask {
        task_kind,
        implementation,
        descriptor_version,
        worker_version,
        effect_policy,
        deployment_binding,
        public_configuration,
        secret_configuration,
        inputs,
        outputs,
        ..
    } = dispatch.action()
    else {
        unreachable!()
    };
    let contract = ReuseAdmissionContract::from_task_parts(
        *task_kind,
        implementation,
        descriptor_version,
        worker_version,
        effect_policy,
        deployment_binding,
        public_configuration,
        secret_configuration,
        inputs,
        outputs,
        None,
    )
    .unwrap();
    let projection_version =
        sqlx::query_scalar::<_, i64>("SELECT projection_version FROM workflow_runs WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(control)
            .await
            .unwrap() as u64;
    SourceReuseFacts {
        projection_version,
        activation_id,
        node_id,
        stable_activation_key,
        output_hash,
        effect_id,
        provenance,
        contract,
    }
}

fn candidate(
    target_run_id: &RunId,
    source_run_id: &RunId,
    versioned: &VersionedPlan,
    source: &SourceReuseFacts,
    candidate_id: &str,
    compatibility: ReuseCompatibility,
) -> CreateReuseCandidateCommand {
    CreateReuseCandidateCommand::new(
        target_run_id.clone(),
        candidate_id,
        ScopeInstanceId::root(),
        source.node_id.clone(),
        source.stable_activation_key.clone(),
        source_run_id.clone(),
        source.activation_id.clone(),
        source.provenance.clone(),
        versioned.definition_revision_id().clone(),
        versioned.deployment_revision_id().clone(),
        versioned.plan_hash().clone(),
        versioned.binding_hash().clone(),
        source.output_hash.clone(),
        source.effect_id.clone(),
        compatibility,
    )
    .unwrap()
}

fn assert_dynamic_candidate_scope_rederivation(
    source_scope: &str,
    first: &[CreateReuseCandidateCommand],
    replay: &[CreateReuseCandidateCommand],
    other_target: &[CreateReuseCandidateCommand],
) {
    assert_eq!(first.len(), 1);
    assert_eq!(replay.len(), 1);
    assert_eq!(other_target.len(), 1);
    let first_scope = first[0].target_scope_instance_id();
    assert_ne!(first_scope, &ScopeInstanceId::root());
    assert_ne!(first_scope.as_str(), source_scope);
    assert_eq!(first_scope, replay[0].target_scope_instance_id());
    assert_ne!(first_scope, other_target[0].target_scope_instance_id());
    assert_eq!(
        first[0].stable_activation_key(),
        replay[0].stable_activation_key(),
        "exact derivation replay must preserve the stable occurrence"
    );
}

fn assert_subflow_invocation_scope_rederivation(
    parent_plan: &Plan,
    source_scope: &str,
    static_scope: &str,
    occurrence: &LogicalOccurrence,
    target_run: &RunId,
) {
    let scope = parent_plan
        .scopes()
        .iter()
        .find(|scope| scope.id().as_str() == static_scope)
        .expect("pinned Subflow invocation scope remains in the parent Plan");
    let scoped_occurrence = occurrence
        .child(format!("scope:{}", scope.id().as_str()))
        .unwrap();
    let expected = DeterministicIds::new(target_run, parent_plan.semantic_hash())
        .scope_instance(scope.id(), &scoped_occurrence)
        .unwrap();
    let replay = DeterministicIds::new(target_run, parent_plan.semantic_hash())
        .scope_instance(scope.id(), &scoped_occurrence)
        .unwrap();
    assert_ne!(source_scope, "root");
    assert_ne!(expected.as_str(), source_scope);
    assert_eq!(expected, replay);
}

#[tokio::test]
async fn sqlite_redrive_resolves_reuse_at_exact_admission_and_rejects_incompatible_hint() {
    let (_directory, database, repository, control) =
        repository_and_control("scheduler-recovery").await;
    let (plan, descriptors) = compile_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let workers = workers(calls.clone());
    let source_run_id = RunId::new("run_recovery_source").unwrap();
    assert!(matches!(
        repository
            .create_run(
                key("create-source", &source_run_id),
                CreateRunCommand::new(
                    source_run_id.clone(),
                    &versioned,
                    json!({"question": "recover me"}),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let source_fence =
        activate_and_claim_sqlite(&repository, &control, &source_run_id, "scheduler-source").await;
    drive_source_sqlite(&repository, &linked, &source_fence, &workers).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let source = source_reuse_facts_sqlite(&control, &source_run_id).await;

    let reused_run_id = RunId::new("run_recovery_reused").unwrap();
    let exact_candidate = candidate(
        &reused_run_id,
        &source_run_id,
        &versioned,
        &source,
        "candidate-exact",
        ReuseCompatibility::from_admission_contract(&source.contract),
    );
    assert!(matches!(
        repository
            .redrive_run(
                key("redrive-exact", &source_run_id),
                RedriveRunCommand::new(
                    source_run_id.clone(),
                    reused_run_id.clone(),
                    source.projection_version,
                    vec![exact_candidate],
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let reused_fence =
        activate_and_claim_sqlite(&repository, &control, &reused_run_id, "scheduler-reused").await;
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &repository,
            &linked,
            &reused_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "reuse cannot call a worker"
    );
    let exact = sqlx::query(
        "SELECT candidate_state,rejection_reason,materialized_activation_id
         FROM run_reuse_candidates WHERE run_id=? AND candidate_id='candidate-exact'",
    )
    .bind(reused_run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(exact.get::<String, _>("candidate_state"), "materialized");
    assert_eq!(exact.get::<Option<String>, _>("rejection_reason"), None);
    assert!(exact
        .get::<Option<String>, _>("materialized_activation_id")
        .is_some());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_activations
             WHERE run_id=? AND reused_from_run_id=? AND reused_from_activation_id=?",
        )
        .bind(reused_run_id.as_str())
        .bind(source_run_id.as_str())
        .bind(source.activation_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT (SELECT COUNT(*) FROM node_attempts WHERE run_id=?) +
                    (SELECT COUNT(*) FROM task_outbox WHERE run_id=?)",
        )
        .bind(reused_run_id.as_str())
        .bind(reused_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0,
        "materialized reuse has no worker attempt or outbox task"
    );

    drop(repository);
    let restarted = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(&restarted, &linked, &reused_fence, &NoSchedulerCrash, 16,)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let rejected_run_id = RunId::new("run_recovery_rejected").unwrap();
    let incompatible = ReuseCompatibility::new(
        source.contract.node_config_hash().clone(),
        ContentHash::from_bytes(b"incompatible descriptor"),
        source.contract.input_value_hash().clone(),
        source.contract.output_schema_hash().clone(),
        source.contract.effect_policy_hash().clone(),
        source.contract.data_dependencies_hash().clone(),
    );
    let rejected_candidate = candidate(
        &rejected_run_id,
        &source_run_id,
        &versioned,
        &source,
        "candidate-incompatible",
        incompatible,
    );
    assert!(matches!(
        restarted
            .redrive_run(
                key("redrive-incompatible", &source_run_id),
                RedriveRunCommand::new(
                    source_run_id.clone(),
                    rejected_run_id.clone(),
                    source.projection_version,
                    vec![rejected_candidate],
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let rejected_fence =
        activate_and_claim_sqlite(&restarted, &control, &rejected_run_id, "scheduler-rejected")
            .await;
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &restarted,
            &linked,
            &rejected_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    execute_one_task_sqlite(&restarted, &workers).await;
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &restarted,
            &linked,
            &rejected_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let rejected = sqlx::query(
        "SELECT candidate_state,rejection_reason,materialized_activation_id
         FROM run_reuse_candidates WHERE run_id=? AND candidate_id='candidate-incompatible'",
    )
    .bind(rejected_run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(rejected.get::<String, _>("candidate_state"), "rejected");
    assert_eq!(
        rejected
            .get::<Option<String>, _>("rejection_reason")
            .as_deref(),
        Some("target_contract_mismatch")
    );
    assert_eq!(
        rejected.get::<Option<String>, _>("materialized_activation_id"),
        None
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_attempts WHERE run_id=? AND lifecycle='succeeded'",
        )
        .bind(rejected_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1,
        "a rejected hint falls back to ordinary execution"
    );
}

#[tokio::test]
async fn sqlite_reuse_dependency_closure_rejects_b_after_a_fallback_and_materializes_closed_chain()
{
    let (_directory, _database, repository, control) =
        repository_and_control("scheduler-reuse-dependency-closure").await;
    let (plan, descriptors) = compile_dependency_chain_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let workers = dependency_chain_workers(calls.clone());

    let source_run = RunId::new("run_dependency_closure_source").unwrap();
    repository
        .create_run(
            key("dependency-source-create", &source_run),
            CreateRunCommand::new(
                source_run.clone(),
                &versioned,
                json!({"question": "dependency closure"}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let source_fence = activate_and_claim_sqlite(
        &repository,
        &control,
        &source_run,
        "scheduler-dependency-source",
    )
    .await;
    drive_chain_to_success_sqlite(&repository, &linked, &source_fence, &workers).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let source_version = repository
        .load_run(&source_run)
        .await
        .unwrap()
        .unwrap()
        .projection_version();

    let filtered_target = RunId::new("run_dependency_closure_filtered_at_derivation").unwrap();
    let filtered = repository
        .derive_migration_reuse_candidates(
            &source_run,
            &filtered_target,
            source_version,
            &recovery_revision(&versioned),
            &[identity_migration_mapping_for_node(&plan, "second")],
        )
        .await
        .unwrap()
        .expect("mapped derivation is authoritative");
    assert!(
        filtered.is_empty(),
        "B must be removed at derivation when its mapped contract excludes source A"
    );

    let rejected_target = RunId::new("run_dependency_closure_rejected").unwrap();
    let rejected_candidates = repository
        .derive_compatible_reuse_candidates(
            &source_run,
            &rejected_target,
            source_version,
            &recovery_revision(&versioned),
            None,
        )
        .await
        .unwrap()
        .expect("closed source chain is derivable");
    assert_eq!(rejected_candidates.len(), 2);
    let first = rejected_candidates
        .iter()
        .find(|candidate| candidate.target_node_id().as_str() == "first")
        .expect("first candidate");
    let second = rejected_candidates
        .iter()
        .find(|candidate| candidate.target_node_id().as_str() == "second")
        .expect("second candidate");
    assert!(first.source_data_dependencies().is_empty());
    assert_eq!(
        second.source_data_dependencies(),
        &std::collections::BTreeSet::from([first.source_activation_id().clone()])
    );
    repository
        .redrive_run(
            key("dependency-redrive-rejected", &source_run),
            RedriveRunCommand::new(
                source_run.clone(),
                rejected_target.clone(),
                source_version,
                rejected_candidates,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE run_reuse_candidates SET descriptor_hash=?
         WHERE run_id=? AND target_node_id='first'",
    )
    .bind(ContentHash::from_bytes(b"force upstream fallback").as_str())
    .bind(rejected_target.as_str())
    .execute(&control)
    .await
    .unwrap();
    let rejected_fence = activate_and_claim_sqlite(
        &repository,
        &control,
        &rejected_target,
        "scheduler-dependency-rejected",
    )
    .await;
    drive_chain_to_success_sqlite(&repository, &linked, &rejected_fence, &workers).await;
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    let decisions = sqlx::query_as::<_, (String, String, Option<String>)>(
        "SELECT target_node_id,candidate_state,rejection_reason
         FROM run_reuse_candidates WHERE run_id=? ORDER BY target_node_id",
    )
    .bind(rejected_target.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(
        decisions,
        vec![
            (
                "first".to_owned(),
                "rejected".to_owned(),
                Some("target_contract_mismatch".to_owned()),
            ),
            (
                "second".to_owned(),
                "rejected".to_owned(),
                Some("dependency_closure_open".to_owned()),
            ),
        ]
    );

    let closed_target = RunId::new("run_dependency_closure_materialized").unwrap();
    let closed_candidates = repository
        .derive_compatible_reuse_candidates(
            &source_run,
            &closed_target,
            source_version,
            &recovery_revision(&versioned),
            None,
        )
        .await
        .unwrap()
        .expect("closed candidate chain");
    repository
        .redrive_run(
            key("dependency-redrive-closed", &source_run),
            RedriveRunCommand::new(
                source_run.clone(),
                closed_target.clone(),
                source_version,
                closed_candidates,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let closed_fence = activate_and_claim_sqlite(
        &repository,
        &control,
        &closed_target,
        "scheduler-dependency-closed",
    )
    .await;
    drive_chain_to_success_sqlite(&repository, &linked, &closed_fence, &workers).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        4,
        "closed reuse runs no workers"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM run_reuse_candidates
             WHERE run_id=? AND candidate_state='materialized'",
        )
        .bind(closed_target.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2
    );
}

#[tokio::test]
async fn sqlite_repository_derives_real_map_and_loop_candidates_into_target_dynamic_scopes() {
    let (_directory, _database, repository, control) =
        repository_and_control("scheduler-dynamic-reuse").await;

    let (map_plan, map_descriptors) = compile_dynamic_map_fixture();
    let map_subflows = SubflowContractRegistry::new();
    let map_linked = LinkedPlan::link(&map_plan, &map_descriptors, &map_subflows).unwrap();
    let map_versioned = versioned_named(
        &map_plan,
        "scheduler-recovery-dynamic-map",
        "scheduler_recovery_dynamic_map_deployment_v1",
    );
    repository
        .install_versioned_plan(&map_versioned)
        .await
        .unwrap();
    let map_calls = Arc::new(AtomicUsize::new(0));
    let map_workers = dynamic_worker("fixture.dynamic_map", map_calls.clone());
    let map_source = RunId::new("run_dynamic_map_source").unwrap();
    repository
        .create_run(
            key("dynamic-map-create", &map_source),
            CreateRunCommand::new(
                map_source.clone(),
                &map_versioned,
                json!({"items": ["one"]}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let map_fence =
        activate_and_claim_sqlite(&repository, &control, &map_source, "scheduler-dynamic-map")
            .await;
    assert_eq!(
        drive_dynamic_terminal_sqlite(&repository, &map_linked, &map_fence, &map_workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_eq!(map_calls.load(Ordering::SeqCst), 1);
    let map_source_scope = sqlx::query_scalar::<_, String>(
        "SELECT scope_instance_id FROM node_activations
         WHERE run_id=? AND execution_kind='worker' AND lifecycle='succeeded'",
    )
    .bind(map_source.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_ne!(map_source_scope, "root");
    let map_version = repository
        .load_run(&map_source)
        .await
        .unwrap()
        .unwrap()
        .projection_version();
    let map_target = RunId::new("run_dynamic_map_target").unwrap();
    let map_other_target = RunId::new("run_dynamic_map_other_target").unwrap();
    let map_first = repository
        .derive_compatible_reuse_candidates(
            &map_source,
            &map_target,
            map_version,
            &recovery_revision(&map_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let map_replay = repository
        .derive_compatible_reuse_candidates(
            &map_source,
            &map_target,
            map_version,
            &recovery_revision(&map_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let map_other = repository
        .derive_compatible_reuse_candidates(
            &map_source,
            &map_other_target,
            map_version,
            &recovery_revision(&map_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_dynamic_candidate_scope_rederivation(
        &map_source_scope,
        &map_first,
        &map_replay,
        &map_other,
    );

    let (loop_plan, loop_descriptors) = compile_dynamic_loop_fixture();
    let loop_subflows = SubflowContractRegistry::new();
    let loop_linked = LinkedPlan::link(&loop_plan, &loop_descriptors, &loop_subflows).unwrap();
    let loop_versioned = versioned_named(
        &loop_plan,
        "scheduler-recovery-dynamic-loop",
        "scheduler_recovery_dynamic_loop_deployment_v1",
    );
    repository
        .install_versioned_plan(&loop_versioned)
        .await
        .unwrap();
    let loop_calls = Arc::new(AtomicUsize::new(0));
    let loop_workers = dynamic_worker("fixture.dynamic_loop", loop_calls.clone());
    let loop_source = RunId::new("run_dynamic_loop_source").unwrap();
    repository
        .create_run(
            key("dynamic-loop-create", &loop_source),
            CreateRunCommand::new(loop_source.clone(), &loop_versioned, json!({"seed": "s0"}))
                .unwrap(),
        )
        .await
        .unwrap();
    let loop_fence = activate_and_claim_sqlite(
        &repository,
        &control,
        &loop_source,
        "scheduler-dynamic-loop",
    )
    .await;
    assert_eq!(
        drive_dynamic_terminal_sqlite(&repository, &loop_linked, &loop_fence, &loop_workers).await,
        SchedulerQuiescence::RunFailed
    );
    assert_eq!(loop_calls.load(Ordering::SeqCst), 1);
    let loop_source_scope = sqlx::query_scalar::<_, String>(
        "SELECT scope_instance_id FROM node_activations
         WHERE run_id=? AND execution_kind='worker' AND lifecycle='succeeded'",
    )
    .bind(loop_source.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_ne!(loop_source_scope, "root");
    let loop_version = repository
        .load_run(&loop_source)
        .await
        .unwrap()
        .unwrap()
        .projection_version();
    let loop_target = RunId::new("run_dynamic_loop_target").unwrap();
    let loop_other_target = RunId::new("run_dynamic_loop_other_target").unwrap();
    let loop_first = repository
        .derive_compatible_reuse_candidates(
            &loop_source,
            &loop_target,
            loop_version,
            &recovery_revision(&loop_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let loop_replay = repository
        .derive_compatible_reuse_candidates(
            &loop_source,
            &loop_target,
            loop_version,
            &recovery_revision(&loop_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let loop_other = repository
        .derive_compatible_reuse_candidates(
            &loop_source,
            &loop_other_target,
            loop_version,
            &recovery_revision(&loop_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_dynamic_candidate_scope_rederivation(
        &loop_source_scope,
        &loop_first,
        &loop_replay,
        &loop_other,
    );
}

#[tokio::test]
async fn sqlite_subflow_invocation_scope_rederives_but_parent_call_is_not_a_reuse_candidate() {
    let (_directory, _database, repository, control) =
        repository_and_control("scheduler-subflow-recovery-scope").await;
    let (_child_plan, child, parent_plan, parent, subflows) = compile_subflow_fixture();
    repository.install_versioned_plan(&child).await.unwrap();
    repository.install_versioned_plan(&parent).await.unwrap();
    let descriptors = DescriptorContractRegistry::new();
    let linked = LinkedPlan::link(&parent_plan, &descriptors, &subflows).unwrap();
    let parent_run = RunId::new("run_subflow_recovery_parent").unwrap();
    repository
        .create_run(
            key("subflow-parent-create", &parent_run),
            CreateRunCommand::new(
                parent_run.clone(),
                &parent,
                json!({"question": "pinned child"}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let fence = activate_and_claim_sqlite(
        &repository,
        &control,
        &parent_run,
        "scheduler-subflow-parent",
    )
    .await;
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForChildRun { .. })
    ));
    let invocation = sqlx::query(
        "SELECT child_run_id,occurrence_key,invocation_scope_instance_id,static_scope_id
         FROM scheduler_subflow_invocations WHERE run_id=?",
    )
    .bind(parent_run.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let child_run = invocation.get::<String, _>("child_run_id");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lineage_kind FROM workflow_runs
             WHERE run_id=? AND parent_run_id=?",
        )
        .bind(&child_run)
        .bind(parent_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "subflow"
    );
    let occurrence: LogicalOccurrence =
        serde_json::from_str(&invocation.get::<String, _>("occurrence_key")).unwrap();
    let target_run = RunId::new("run_subflow_recovery_target").unwrap();
    assert_subflow_invocation_scope_rederivation(
        &parent_plan,
        &invocation.get::<String, _>("invocation_scope_instance_id"),
        &invocation.get::<String, _>("static_scope_id"),
        &occurrence,
        &target_run,
    );

    // SubflowCall is scheduler-native and its invocation scope may not contain
    // parent-Plan worker nodes. It therefore never becomes a reuse Candidate;
    // the pinned child Run has its own lineage and is recovered/redriven as an
    // independent Run authority.
    let parent_version = repository
        .load_run(&parent_run)
        .await
        .unwrap()
        .unwrap()
        .projection_version();
    let candidates = repository
        .derive_compatible_reuse_candidates(
            &parent_run,
            &target_run,
            parent_version,
            &recovery_revision(&parent),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(candidates.is_empty());
}

#[tokio::test]
async fn sqlite_redrive_restarts_and_retries_idempotent_unknown_effect_with_inherited_id() {
    let (_directory, database, repository, control) =
        repository_and_control("scheduler-redrive-effect").await;
    let policy = idempotent_policy();
    let (plan, descriptors) = compile_fixture_with_policy(Some(&policy));
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let effect_ids = Arc::new(Mutex::new(Vec::new()));
    let workers = fail_once_workers(calls.clone(), effect_ids.clone());

    let source_run_id = RunId::new("run_redrive_effect_source").unwrap();
    repository
        .create_run(
            key("create-effect-source", &source_run_id),
            CreateRunCommand::new(
                source_run_id.clone(),
                &versioned,
                json!({"question": "retry the exact provider operation"}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let source_fence = activate_and_claim_sqlite(
        &repository,
        &control,
        &source_run_id,
        "scheduler-effect-source",
    )
    .await;
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &repository,
            &linked,
            &source_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    assert!(matches!(
        consume_scheduler_task_once(
            &repository,
            &workers,
            &TerminalSchedulerWorkerFailurePolicy,
            "scheduler-redrive-effect-worker",
            60,
            64,
            CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::Committed { .. }
    ));
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &repository,
            &linked,
            &source_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunFailed)
    ));
    let source = sqlx::query(
        "SELECT a.effect_id,r.projection_version
         FROM node_activations a JOIN workflow_runs r ON r.run_id=a.run_id
         WHERE a.run_id=? AND a.execution_kind='worker' AND a.lifecycle='failed'
           AND a.effect_evidence='unknown' AND a.effect_idempotency='idempotent'",
    )
    .bind(source_run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let source_effect_id = EffectId::new(source.get::<String, _>("effect_id")).unwrap();
    let source_version = u64::try_from(source.get::<i64, _>("projection_version")).unwrap();

    let target_run_id = RunId::new("run_redrive_effect_target").unwrap();
    assert!(matches!(
        repository
            .redrive_run(
                key("redrive-effect", &source_run_id),
                RedriveRunCommand::new(
                    source_run_id.clone(),
                    target_run_id.clone(),
                    source_version,
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let target_fence = activate_and_claim_sqlite(
        &repository,
        &control,
        &target_run_id,
        "scheduler-effect-target",
    )
    .await;

    // Recovery roots, not process memory, carry the provider operation key.
    drop(repository);
    let restarted = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(&restarted, &linked, &target_fence, &NoSchedulerCrash, 64,)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    let target_effect_id = sqlx::query_scalar::<_, String>(
        "SELECT effect_id FROM node_activations
         WHERE run_id=? AND execution_kind='worker' AND lifecycle IN ('ready','leased','running')",
    )
    .bind(target_run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(target_effect_id, source_effect_id.as_str());
    execute_one_task_sqlite(&restarted, &workers).await;
    assert!(matches!(
        drive_scheduler_until_quiescent(&restarted, &linked, &target_fence, &NoSchedulerCrash, 64,)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        effect_ids.lock().unwrap().as_slice(),
        &[source_effect_id.clone(), source_effect_id]
    );
}

#[tokio::test]
async fn sqlite_redrive_requires_fork_for_non_idempotent_unknown_or_started_effects() {
    let (_directory, _database, repository, control) =
        repository_and_control("scheduler-redrive-blocked-effect").await;
    let policy = non_idempotent_policy();
    let (plan, descriptors) = compile_fixture_with_policy(Some(&policy));
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();

    for (suffix, class, expected_evidence) in [
        (
            "unknown",
            WorkerFailureClass::EffectOutcomeUnknown,
            "unknown",
        ),
        (
            "started",
            WorkerFailureClass::InfrastructureFailure,
            "started",
        ),
    ] {
        let source_run_id = RunId::new(format!("run_redrive_blocked_sqlite_{suffix}")).unwrap();
        repository
            .create_run(
                key("create-blocked-effect", &source_run_id),
                CreateRunCommand::new(
                    source_run_id.clone(),
                    &versioned,
                    json!({"question": "unsafe provider effect"}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let fence = activate_and_claim_sqlite(
            &repository,
            &control,
            &source_run_id,
            &format!("scheduler-blocked-{suffix}"),
        )
        .await;
        assert!(matches!(
            drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
                .await
                .unwrap(),
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
        ));
        assert!(matches!(
            consume_scheduler_task_once(
                &repository,
                &terminal_failure_workers(class),
                &TerminalSchedulerWorkerFailurePolicy,
                &format!("worker-blocked-{suffix}"),
                60,
                64,
                CancellationToken::new(),
                &NoSchedulerCrash,
            )
            .await
            .unwrap(),
            SchedulerWorkerPumpOutcome::Committed { .. }
        ));
        assert!(matches!(
            drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
                .await
                .unwrap(),
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunFailed)
        ));
        let source = sqlx::query_as::<_, (i64, String, String)>(
            "SELECT r.projection_version,a.effect_evidence,a.effect_idempotency
             FROM workflow_runs r JOIN node_activations a ON a.run_id=r.run_id
             WHERE r.run_id=? AND a.execution_kind='worker'",
        )
        .bind(source_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(source.1, expected_evidence);
        assert_eq!(source.2, "non_idempotent");
        let target_run_id =
            RunId::new(format!("run_redrive_blocked_sqlite_target_{suffix}")).unwrap();
        let error = repository
            .redrive_run(
                key("redrive-blocked-effect", &source_run_id),
                RedriveRunCommand::new(
                    source_run_id.clone(),
                    target_run_id.clone(),
                    u64::try_from(source.0).unwrap(),
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), REPOSITORY_REDRIVE_REQUIRES_FORK);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workflow_runs WHERE run_id=?")
                .bind(target_run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM node_attempts WHERE run_id=?")
                .bind(target_run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM recovery_effect_roots WHERE run_id=?",
            )
            .bind(target_run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM node_attempts WHERE run_id=?")
                .bind(source_run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            1,
            "blocked Redrive must not add another source Attempt"
        );
    }
}

async fn isolated_postgres() -> Option<(PostgresDurableRepository, PgPool, PgPool, String, String)>
{
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("scheduler_recovery_{}", Uuid::new_v4().simple());
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
    Some((repository, control, admin, schema, scoped_url))
}

async fn activate_and_claim_postgres(
    repository: &PostgresDurableRepository,
    control: &PgPool,
    run_id: &RunId,
    owner: &str,
) -> FencedSchedulerRunCommand {
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
                    updated_at=CURRENT_TIMESTAMP
             WHERE run_id=$1 AND lifecycle='created'",
        )
        .bind(run_id.as_str())
        .execute(control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    repository
        .claim_scheduler_run(
            key("claim", run_id),
            ClaimSchedulerRunCommand::new(run_id.clone(), owner, 60).unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .expect("new PostgreSQL scheduler lease")
        .fence()
        .unwrap()
}

async fn execute_one_task_postgres(
    repository: &PostgresDurableRepository,
    workers: &WorkerExecutorRegistry,
) {
    assert!(matches!(
        consume_scheduler_task_once(
            repository,
            workers,
            &TerminalSchedulerWorkerFailurePolicy,
            "scheduler-recovery-pg-worker",
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

async fn drive_chain_to_success_postgres(
    repository: &PostgresDurableRepository,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    workers: &WorkerExecutorRegistry,
) {
    for _ in 0..8 {
        match drive_scheduler_until_quiescent(repository, linked, fence, &NoSchedulerCrash, 64)
            .await
            .unwrap()
        {
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. }) => {
                execute_one_task_postgres(repository, workers).await;
            }
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForChildren {
                ..
            }) => {
                let _ = consume_scheduler_task_once(
                    repository,
                    workers,
                    &TerminalSchedulerWorkerFailurePolicy,
                    "scheduler-dynamic-pg-child-settlement",
                    60,
                    64,
                    CancellationToken::new(),
                    &NoSchedulerCrash,
                )
                .await
                .unwrap();
            }
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded) => return,
            other => panic!("PostgreSQL dependency chain stopped unexpectedly: {other:?}"),
        }
    }
    panic!("PostgreSQL dependency chain did not reach success");
}

async fn drive_dynamic_terminal_postgres(
    repository: &PostgresDurableRepository,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    workers: &WorkerExecutorRegistry,
) -> SchedulerQuiescence {
    for _ in 0..64 {
        match drive_scheduler_until_quiescent(repository, linked, fence, &NoSchedulerCrash, 64)
            .await
            .unwrap()
        {
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. }) => {
                execute_one_task_postgres(repository, workers).await;
            }
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForChildren {
                ..
            }) => {
                let _ = consume_scheduler_task_once(
                    repository,
                    workers,
                    &TerminalSchedulerWorkerFailurePolicy,
                    "scheduler-dynamic-pg-child-settlement",
                    60,
                    64,
                    CancellationToken::new(),
                    &NoSchedulerCrash,
                )
                .await
                .unwrap();
            }
            SchedulerRecoveryOutcome::Quiescent(terminal @ SchedulerQuiescence::RunSucceeded)
            | SchedulerRecoveryOutcome::Quiescent(terminal @ SchedulerQuiescence::RunFailed) => {
                return terminal;
            }
            other => panic!("PostgreSQL dynamic fixture stopped unexpectedly: {other:?}"),
        }
    }
    panic!("PostgreSQL dynamic fixture did not terminalize");
}

async fn source_reuse_facts_postgres(control: &PgPool, run_id: &RunId) -> SourceReuseFacts {
    let activation = sqlx::query(
        "SELECT activation_id,node_id,stable_activation_key,output_value_hash,effect_id
         FROM node_activations WHERE run_id=$1 AND execution_kind='worker' AND lifecycle='succeeded'",
    )
    .bind(run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap();
    let activation_id = ActivationId::new(activation.get::<String, _>("activation_id")).unwrap();
    let node_id = NodeId::new(activation.get::<String, _>("node_id")).unwrap();
    let stable_activation_key = activation.get::<String, _>("stable_activation_key");
    let output_hash = ContentHash::parse(activation.get::<String, _>("output_value_hash")).unwrap();
    let effect_id = EffectId::new(activation.get::<String, _>("effect_id")).unwrap();

    let token = sqlx::query(
        "SELECT source_port_id,current_scope_instance_id,provenance_frames
         FROM control_tokens WHERE run_id=$1 AND source_activation_id=$2",
    )
    .bind(run_id.as_str())
    .bind(activation_id.as_str())
    .fetch_one(control)
    .await
    .unwrap();
    let frames: Vec<ControlFrame> =
        serde_json::from_value(token.get::<serde_json::Value, _>("provenance_frames")).unwrap();
    let provenance = ControlTokenProvenance::new(
        run_id.clone(),
        activation_id.clone(),
        PortId::new(token.get::<String, _>("source_port_id")).unwrap(),
        ControlEmissionSlot::ActivationOutput,
        ScopeInstanceId::new(token.get::<String, _>("current_scope_instance_id")).unwrap(),
        frames,
    )
    .unwrap();

    let intents = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT fact_payload FROM scheduler_checkpoints
         WHERE run_id=$1 AND checkpoint_kind='planned_action' ORDER BY created_at",
    )
    .bind(run_id.as_str())
    .fetch_all(control)
    .await
    .unwrap();
    let dispatch = intents
        .into_iter()
        .filter_map(|payload| serde_json::from_value::<SchedulerIntent>(payload).ok())
        .find(|intent| {
            matches!(
                intent.action(),
                SchedulerAction::DispatchTask { activation_id: id, .. } if id == &activation_id
            )
        })
        .expect("source PostgreSQL DispatchTask checkpoint");
    let SchedulerAction::DispatchTask {
        task_kind,
        implementation,
        descriptor_version,
        worker_version,
        effect_policy,
        deployment_binding,
        public_configuration,
        secret_configuration,
        inputs,
        outputs,
        ..
    } = dispatch.action()
    else {
        unreachable!()
    };
    let contract = ReuseAdmissionContract::from_task_parts(
        *task_kind,
        implementation,
        descriptor_version,
        worker_version,
        effect_policy,
        deployment_binding,
        public_configuration,
        secret_configuration,
        inputs,
        outputs,
        None,
    )
    .unwrap();
    let projection_version = sqlx::query_scalar::<_, i64>(
        "SELECT projection_version FROM workflow_runs WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap() as u64;
    SourceReuseFacts {
        projection_version,
        activation_id,
        node_id,
        stable_activation_key,
        output_hash,
        effect_id,
        provenance,
        contract,
    }
}

#[tokio::test]
async fn postgres_redrive_resolves_reuse_at_exact_admission_and_rejects_incompatible_hint() {
    let Some((repository, control, _admin, _schema, scoped_url)) = isolated_postgres().await else {
        return;
    };
    let (plan, descriptors) = compile_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let workers = workers(calls.clone());
    let source_run_id = RunId::new("run_recovery_pg_source").unwrap();
    assert!(matches!(
        repository
            .create_run(
                key("create-source", &source_run_id),
                CreateRunCommand::new(
                    source_run_id.clone(),
                    &versioned,
                    json!({"question": "recover me"}),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let source_fence =
        activate_and_claim_postgres(&repository, &control, &source_run_id, "scheduler-pg-source")
            .await;
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &repository,
            &linked,
            &source_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    execute_one_task_postgres(&repository, &workers).await;
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &repository,
            &linked,
            &source_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let source = source_reuse_facts_postgres(&control, &source_run_id).await;

    let reused_run_id = RunId::new("run_recovery_pg_reused").unwrap();
    let exact_candidate = candidate(
        &reused_run_id,
        &source_run_id,
        &versioned,
        &source,
        "candidate-pg-exact",
        ReuseCompatibility::from_admission_contract(&source.contract),
    );
    assert!(matches!(
        repository
            .redrive_run(
                key("redrive-exact", &source_run_id),
                RedriveRunCommand::new(
                    source_run_id.clone(),
                    reused_run_id.clone(),
                    source.projection_version,
                    vec![exact_candidate],
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let reused_fence =
        activate_and_claim_postgres(&repository, &control, &reused_run_id, "scheduler-pg-reused")
            .await;
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &repository,
            &linked,
            &reused_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_as::<_, (String, Option<String>, i64, i64)>(
            "SELECT c.candidate_state,c.rejection_reason,
                    (SELECT COUNT(*) FROM node_attempts WHERE run_id=$1),
                    (SELECT COUNT(*) FROM task_outbox WHERE run_id=$1)
             FROM run_reuse_candidates c WHERE c.run_id=$1 AND c.candidate_id='candidate-pg-exact'",
        )
        .bind(reused_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        ("materialized".to_owned(), None, 0, 0)
    );

    drop(repository);
    let restarted = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(&restarted, &linked, &reused_fence, &NoSchedulerCrash, 16,)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));

    let rejected_run_id = RunId::new("run_recovery_pg_rejected").unwrap();
    let incompatible = ReuseCompatibility::new(
        source.contract.node_config_hash().clone(),
        ContentHash::from_bytes(b"incompatible pg descriptor"),
        source.contract.input_value_hash().clone(),
        source.contract.output_schema_hash().clone(),
        source.contract.effect_policy_hash().clone(),
        source.contract.data_dependencies_hash().clone(),
    );
    let rejected_candidate = candidate(
        &rejected_run_id,
        &source_run_id,
        &versioned,
        &source,
        "candidate-pg-incompatible",
        incompatible,
    );
    assert!(matches!(
        restarted
            .redrive_run(
                key("redrive-incompatible", &source_run_id),
                RedriveRunCommand::new(
                    source_run_id.clone(),
                    rejected_run_id.clone(),
                    source.projection_version,
                    vec![rejected_candidate],
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let rejected_fence = activate_and_claim_postgres(
        &restarted,
        &control,
        &rejected_run_id,
        "scheduler-pg-rejected",
    )
    .await;
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &restarted,
            &linked,
            &rejected_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    execute_one_task_postgres(&restarted, &workers).await;
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &restarted,
            &linked,
            &rejected_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        sqlx::query_as::<_, (String, Option<String>, i64)>(
            "SELECT c.candidate_state,c.rejection_reason,
                    (SELECT COUNT(*) FROM node_attempts
                     WHERE run_id=$1 AND lifecycle='succeeded')
             FROM run_reuse_candidates c
             WHERE c.run_id=$1 AND c.candidate_id='candidate-pg-incompatible'",
        )
        .bind(rejected_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        (
            "rejected".to_owned(),
            Some("target_contract_mismatch".to_owned()),
            1,
        )
    );
}

#[tokio::test]
async fn postgres_reuse_dependency_closure_rejects_b_after_a_fallback_and_materializes_closed_chain(
) {
    let Some((repository, control, _admin, _schema, _scoped_url)) = isolated_postgres().await
    else {
        return;
    };
    let (plan, descriptors) = compile_dependency_chain_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let workers = dependency_chain_workers(calls.clone());

    let source_run = RunId::new("run_dependency_closure_pg_source").unwrap();
    repository
        .create_run(
            key("dependency-source-create", &source_run),
            CreateRunCommand::new(
                source_run.clone(),
                &versioned,
                json!({"question": "dependency closure"}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let source_fence = activate_and_claim_postgres(
        &repository,
        &control,
        &source_run,
        "scheduler-dependency-pg-source",
    )
    .await;
    drive_chain_to_success_postgres(&repository, &linked, &source_fence, &workers).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let source_version = repository
        .load_run(&source_run)
        .await
        .unwrap()
        .unwrap()
        .projection_version();

    let filtered_target = RunId::new("run_dependency_closure_pg_filtered_at_derivation").unwrap();
    let filtered = repository
        .derive_migration_reuse_candidates(
            &source_run,
            &filtered_target,
            source_version,
            &recovery_revision(&versioned),
            &[identity_migration_mapping_for_node(&plan, "second")],
        )
        .await
        .unwrap()
        .expect("PostgreSQL mapped derivation is authoritative");
    assert!(
        filtered.is_empty(),
        "PostgreSQL B must be removed when mapped derivation excludes source A"
    );

    let rejected_target = RunId::new("run_dependency_closure_pg_rejected").unwrap();
    let rejected_candidates = repository
        .derive_compatible_reuse_candidates(
            &source_run,
            &rejected_target,
            source_version,
            &recovery_revision(&versioned),
            None,
        )
        .await
        .unwrap()
        .expect("PostgreSQL closed source chain is derivable");
    assert_eq!(rejected_candidates.len(), 2);
    let first = rejected_candidates
        .iter()
        .find(|candidate| candidate.target_node_id().as_str() == "first")
        .expect("first candidate");
    let second = rejected_candidates
        .iter()
        .find(|candidate| candidate.target_node_id().as_str() == "second")
        .expect("second candidate");
    assert!(first.source_data_dependencies().is_empty());
    assert_eq!(
        second.source_data_dependencies(),
        &std::collections::BTreeSet::from([first.source_activation_id().clone()])
    );
    repository
        .redrive_run(
            key("dependency-redrive-rejected", &source_run),
            RedriveRunCommand::new(
                source_run.clone(),
                rejected_target.clone(),
                source_version,
                rejected_candidates,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE run_reuse_candidates SET descriptor_hash=$1
         WHERE run_id=$2 AND target_node_id='first'",
    )
    .bind(ContentHash::from_bytes(b"force upstream fallback").as_str())
    .bind(rejected_target.as_str())
    .execute(&control)
    .await
    .unwrap();
    let rejected_fence = activate_and_claim_postgres(
        &repository,
        &control,
        &rejected_target,
        "scheduler-dependency-pg-rejected",
    )
    .await;
    drive_chain_to_success_postgres(&repository, &linked, &rejected_fence, &workers).await;
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    let decisions = sqlx::query_as::<_, (String, String, Option<String>)>(
        "SELECT target_node_id,candidate_state,rejection_reason
         FROM run_reuse_candidates WHERE run_id=$1 ORDER BY target_node_id",
    )
    .bind(rejected_target.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(
        decisions,
        vec![
            (
                "first".to_owned(),
                "rejected".to_owned(),
                Some("target_contract_mismatch".to_owned()),
            ),
            (
                "second".to_owned(),
                "rejected".to_owned(),
                Some("dependency_closure_open".to_owned()),
            ),
        ]
    );

    let closed_target = RunId::new("run_dependency_closure_pg_materialized").unwrap();
    let closed_candidates = repository
        .derive_compatible_reuse_candidates(
            &source_run,
            &closed_target,
            source_version,
            &recovery_revision(&versioned),
            None,
        )
        .await
        .unwrap()
        .expect("PostgreSQL closed candidate chain");
    repository
        .redrive_run(
            key("dependency-redrive-closed", &source_run),
            RedriveRunCommand::new(
                source_run.clone(),
                closed_target.clone(),
                source_version,
                closed_candidates,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let closed_fence = activate_and_claim_postgres(
        &repository,
        &control,
        &closed_target,
        "scheduler-dependency-pg-closed",
    )
    .await;
    drive_chain_to_success_postgres(&repository, &linked, &closed_fence, &workers).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        4,
        "closed reuse runs no workers"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM run_reuse_candidates
             WHERE run_id=$1 AND candidate_state='materialized'",
        )
        .bind(closed_target.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2
    );
}

#[tokio::test]
async fn postgres_repository_derives_real_map_and_loop_candidates_into_target_dynamic_scopes() {
    let Some((repository, control, _admin, _schema, _scoped_url)) = isolated_postgres().await
    else {
        return;
    };

    let (map_plan, map_descriptors) = compile_dynamic_map_fixture();
    let map_subflows = SubflowContractRegistry::new();
    let map_linked = LinkedPlan::link(&map_plan, &map_descriptors, &map_subflows).unwrap();
    let map_versioned = versioned_named(
        &map_plan,
        "scheduler-recovery-dynamic-map-pg",
        "scheduler_recovery_dynamic_map_pg_deployment_v1",
    );
    repository
        .install_versioned_plan(&map_versioned)
        .await
        .unwrap();
    let map_calls = Arc::new(AtomicUsize::new(0));
    let map_workers = dynamic_worker("fixture.dynamic_map", map_calls.clone());
    let map_source = RunId::new("run_dynamic_map_pg_source").unwrap();
    repository
        .create_run(
            key("dynamic-map-create", &map_source),
            CreateRunCommand::new(
                map_source.clone(),
                &map_versioned,
                json!({"items": ["one"]}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let map_fence = activate_and_claim_postgres(
        &repository,
        &control,
        &map_source,
        "scheduler-dynamic-map-pg",
    )
    .await;
    assert_eq!(
        drive_dynamic_terminal_postgres(&repository, &map_linked, &map_fence, &map_workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_eq!(map_calls.load(Ordering::SeqCst), 1);
    let map_source_scope = sqlx::query_scalar::<_, String>(
        "SELECT scope_instance_id FROM node_activations
         WHERE run_id=$1 AND execution_kind='worker' AND lifecycle='succeeded'",
    )
    .bind(map_source.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_ne!(map_source_scope, "root");
    let map_version = repository
        .load_run(&map_source)
        .await
        .unwrap()
        .unwrap()
        .projection_version();
    let map_target = RunId::new("run_dynamic_map_pg_target").unwrap();
    let map_other_target = RunId::new("run_dynamic_map_pg_other_target").unwrap();
    let map_first = repository
        .derive_compatible_reuse_candidates(
            &map_source,
            &map_target,
            map_version,
            &recovery_revision(&map_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let map_replay = repository
        .derive_compatible_reuse_candidates(
            &map_source,
            &map_target,
            map_version,
            &recovery_revision(&map_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let map_other = repository
        .derive_compatible_reuse_candidates(
            &map_source,
            &map_other_target,
            map_version,
            &recovery_revision(&map_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_dynamic_candidate_scope_rederivation(
        &map_source_scope,
        &map_first,
        &map_replay,
        &map_other,
    );

    let (loop_plan, loop_descriptors) = compile_dynamic_loop_fixture();
    let loop_subflows = SubflowContractRegistry::new();
    let loop_linked = LinkedPlan::link(&loop_plan, &loop_descriptors, &loop_subflows).unwrap();
    let loop_versioned = versioned_named(
        &loop_plan,
        "scheduler-recovery-dynamic-loop-pg",
        "scheduler_recovery_dynamic_loop_pg_deployment_v1",
    );
    repository
        .install_versioned_plan(&loop_versioned)
        .await
        .unwrap();
    let loop_calls = Arc::new(AtomicUsize::new(0));
    let loop_workers = dynamic_worker("fixture.dynamic_loop", loop_calls.clone());
    let loop_source = RunId::new("run_dynamic_loop_pg_source").unwrap();
    repository
        .create_run(
            key("dynamic-loop-create", &loop_source),
            CreateRunCommand::new(loop_source.clone(), &loop_versioned, json!({"seed": "s0"}))
                .unwrap(),
        )
        .await
        .unwrap();
    let loop_fence = activate_and_claim_postgres(
        &repository,
        &control,
        &loop_source,
        "scheduler-dynamic-loop-pg",
    )
    .await;
    assert_eq!(
        drive_dynamic_terminal_postgres(&repository, &loop_linked, &loop_fence, &loop_workers)
            .await,
        SchedulerQuiescence::RunFailed
    );
    assert_eq!(loop_calls.load(Ordering::SeqCst), 1);
    let loop_source_scope = sqlx::query_scalar::<_, String>(
        "SELECT scope_instance_id FROM node_activations
         WHERE run_id=$1 AND execution_kind='worker' AND lifecycle='succeeded'",
    )
    .bind(loop_source.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_ne!(loop_source_scope, "root");
    let loop_version = repository
        .load_run(&loop_source)
        .await
        .unwrap()
        .unwrap()
        .projection_version();
    let loop_target = RunId::new("run_dynamic_loop_pg_target").unwrap();
    let loop_other_target = RunId::new("run_dynamic_loop_pg_other_target").unwrap();
    let loop_first = repository
        .derive_compatible_reuse_candidates(
            &loop_source,
            &loop_target,
            loop_version,
            &recovery_revision(&loop_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let loop_replay = repository
        .derive_compatible_reuse_candidates(
            &loop_source,
            &loop_target,
            loop_version,
            &recovery_revision(&loop_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let loop_other = repository
        .derive_compatible_reuse_candidates(
            &loop_source,
            &loop_other_target,
            loop_version,
            &recovery_revision(&loop_versioned),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_dynamic_candidate_scope_rederivation(
        &loop_source_scope,
        &loop_first,
        &loop_replay,
        &loop_other,
    );
}

#[tokio::test]
async fn postgres_subflow_invocation_scope_rederives_but_parent_call_is_not_a_reuse_candidate() {
    let Some((repository, control, _admin, _schema, _scoped_url)) = isolated_postgres().await
    else {
        return;
    };
    let (_child_plan, child, parent_plan, parent, subflows) = compile_subflow_fixture();
    repository.install_versioned_plan(&child).await.unwrap();
    repository.install_versioned_plan(&parent).await.unwrap();
    let descriptors = DescriptorContractRegistry::new();
    let linked = LinkedPlan::link(&parent_plan, &descriptors, &subflows).unwrap();
    let parent_run = RunId::new("run_subflow_recovery_pg_parent").unwrap();
    repository
        .create_run(
            key("subflow-parent-create", &parent_run),
            CreateRunCommand::new(
                parent_run.clone(),
                &parent,
                json!({"question": "pinned child"}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let fence = activate_and_claim_postgres(
        &repository,
        &control,
        &parent_run,
        "scheduler-subflow-parent-pg",
    )
    .await;
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForChildRun { .. })
    ));
    let invocation = sqlx::query(
        "SELECT child_run_id,occurrence_key,invocation_scope_instance_id,static_scope_id
         FROM scheduler_subflow_invocations WHERE run_id=$1",
    )
    .bind(parent_run.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let child_run = invocation.get::<String, _>("child_run_id");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lineage_kind FROM workflow_runs
             WHERE run_id=$1 AND parent_run_id=$2",
        )
        .bind(&child_run)
        .bind(parent_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "subflow"
    );
    let occurrence: LogicalOccurrence =
        serde_json::from_value(invocation.get::<serde_json::Value, _>("occurrence_key")).unwrap();
    let target_run = RunId::new("run_subflow_recovery_pg_target").unwrap();
    assert_subflow_invocation_scope_rederivation(
        &parent_plan,
        &invocation.get::<String, _>("invocation_scope_instance_id"),
        &invocation.get::<String, _>("static_scope_id"),
        &occurrence,
        &target_run,
    );

    // A parent SubflowCall is scheduler-native, while the pinned child Run is
    // a separate lineage authority. Reuse derivation must not manufacture a
    // worker Candidate for the parent invocation projection.
    let parent_version = repository
        .load_run(&parent_run)
        .await
        .unwrap()
        .unwrap()
        .projection_version();
    let candidates = repository
        .derive_compatible_reuse_candidates(
            &parent_run,
            &target_run,
            parent_version,
            &recovery_revision(&parent),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(candidates.is_empty());
}

#[tokio::test]
async fn postgres_redrive_restarts_and_retries_idempotent_unknown_effect_with_inherited_id() {
    let Some((repository, control, _admin, _schema, scoped_url)) = isolated_postgres().await else {
        return;
    };
    let policy = idempotent_policy();
    let (plan, descriptors) = compile_fixture_with_policy(Some(&policy));
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let effect_ids = Arc::new(Mutex::new(Vec::new()));
    let workers = fail_once_workers(calls.clone(), effect_ids.clone());

    let source_run_id = RunId::new("run_redrive_effect_pg_source").unwrap();
    repository
        .create_run(
            key("create-effect-source", &source_run_id),
            CreateRunCommand::new(
                source_run_id.clone(),
                &versioned,
                json!({"question": "retry the exact provider operation"}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let source_fence = activate_and_claim_postgres(
        &repository,
        &control,
        &source_run_id,
        "scheduler-effect-pg-source",
    )
    .await;
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &repository,
            &linked,
            &source_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    assert!(matches!(
        consume_scheduler_task_once(
            &repository,
            &workers,
            &TerminalSchedulerWorkerFailurePolicy,
            "scheduler-redrive-effect-pg-worker",
            60,
            64,
            CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::Committed { .. }
    ));
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &repository,
            &linked,
            &source_fence,
            &NoSchedulerCrash,
            64,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunFailed)
    ));
    let source = sqlx::query(
        "SELECT a.effect_id,r.projection_version
         FROM node_activations a JOIN workflow_runs r ON r.run_id=a.run_id
         WHERE a.run_id=$1 AND a.execution_kind='worker' AND a.lifecycle='failed'
           AND a.effect_evidence='unknown' AND a.effect_idempotency='idempotent'",
    )
    .bind(source_run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let source_effect_id = EffectId::new(source.get::<String, _>("effect_id")).unwrap();
    let source_version = u64::try_from(source.get::<i64, _>("projection_version")).unwrap();

    let target_run_id = RunId::new("run_redrive_effect_pg_target").unwrap();
    assert!(matches!(
        repository
            .redrive_run(
                key("redrive-effect", &source_run_id),
                RedriveRunCommand::new(
                    source_run_id.clone(),
                    target_run_id.clone(),
                    source_version,
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let target_fence = activate_and_claim_postgres(
        &repository,
        &control,
        &target_run_id,
        "scheduler-effect-pg-target",
    )
    .await;

    drop(repository);
    let restarted = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(&restarted, &linked, &target_fence, &NoSchedulerCrash, 64,)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    let target_effect_id = sqlx::query_scalar::<_, String>(
        "SELECT effect_id FROM node_activations
         WHERE run_id=$1 AND execution_kind='worker'
           AND lifecycle IN ('ready','leased','running')",
    )
    .bind(target_run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(target_effect_id, source_effect_id.as_str());
    execute_one_task_postgres(&restarted, &workers).await;
    assert!(matches!(
        drive_scheduler_until_quiescent(&restarted, &linked, &target_fence, &NoSchedulerCrash, 64,)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        effect_ids.lock().unwrap().as_slice(),
        &[source_effect_id.clone(), source_effect_id]
    );
}

#[tokio::test]
async fn postgres_redrive_requires_fork_for_non_idempotent_unknown_or_started_effects() {
    let Some((repository, control, _admin, _schema, _scoped_url)) = isolated_postgres().await
    else {
        return;
    };
    let policy = non_idempotent_policy();
    let (plan, descriptors) = compile_fixture_with_policy(Some(&policy));
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let versioned = versioned(&plan);
    repository.install_versioned_plan(&versioned).await.unwrap();

    for (suffix, class, expected_evidence) in [
        (
            "unknown",
            WorkerFailureClass::EffectOutcomeUnknown,
            "unknown",
        ),
        (
            "started",
            WorkerFailureClass::InfrastructureFailure,
            "started",
        ),
    ] {
        let source_run_id = RunId::new(format!("run_redrive_blocked_pg_{suffix}")).unwrap();
        repository
            .create_run(
                key("create-blocked-effect", &source_run_id),
                CreateRunCommand::new(
                    source_run_id.clone(),
                    &versioned,
                    json!({"question": "unsafe provider effect"}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let fence = activate_and_claim_postgres(
            &repository,
            &control,
            &source_run_id,
            &format!("scheduler-blocked-pg-{suffix}"),
        )
        .await;
        assert!(matches!(
            drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
                .await
                .unwrap(),
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
        ));
        assert!(matches!(
            consume_scheduler_task_once(
                &repository,
                &terminal_failure_workers(class),
                &TerminalSchedulerWorkerFailurePolicy,
                &format!("worker-blocked-pg-{suffix}"),
                60,
                64,
                CancellationToken::new(),
                &NoSchedulerCrash,
            )
            .await
            .unwrap(),
            SchedulerWorkerPumpOutcome::Committed { .. }
        ));
        assert!(matches!(
            drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
                .await
                .unwrap(),
            SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunFailed)
        ));
        let source = sqlx::query_as::<_, (i64, String, String)>(
            "SELECT r.projection_version,a.effect_evidence,a.effect_idempotency
             FROM workflow_runs r JOIN node_activations a ON a.run_id=r.run_id
             WHERE r.run_id=$1 AND a.execution_kind='worker'",
        )
        .bind(source_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(source.1, expected_evidence);
        assert_eq!(source.2, "non_idempotent");
        let target_run_id = RunId::new(format!("run_redrive_blocked_pg_target_{suffix}")).unwrap();
        let error = repository
            .redrive_run(
                key("redrive-blocked-effect", &source_run_id),
                RedriveRunCommand::new(
                    source_run_id.clone(),
                    target_run_id.clone(),
                    u64::try_from(source.0).unwrap(),
                    vec![],
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), REPOSITORY_REDRIVE_REQUIRES_FORK);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workflow_runs WHERE run_id=$1")
                .bind(target_run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM node_attempts WHERE run_id=$1")
                .bind(target_run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM recovery_effect_roots WHERE run_id=$1",
            )
            .bind(target_run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM node_attempts WHERE run_id=$1")
                .bind(source_run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            1,
            "blocked Redrive must not add another source Attempt"
        );
    }
}
