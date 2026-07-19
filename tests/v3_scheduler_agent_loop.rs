use std::{collections::BTreeMap, sync::Arc};

use insight_agent_platform::{
    dsl::v3::{compile_source, CompileOptions},
    engine::{
        plan::{
            DescriptorConfigurationContract, DescriptorContract, DescriptorContractRegistry,
            DescriptorFieldContract, DescriptorValue, DescriptorValueSchema, LeafTaskKind,
            LinkedPlan, NodeKind, Plan, PlanIndex, SubflowContractRegistry, VersionTag,
            WorkerContract, WorkerInputPortContract,
        },
        repository::{
            consume_scheduler_task_once, drive_scheduler_once, CreateRunCommand, DurableRepository,
            FencedSchedulerRunCommand, NoSchedulerCrash, PlanInstallOutcome,
            PostgresDurableRepository, SchedulerDriveOutcome, SchedulerDurableRepository,
            SqliteDurableRepository, TerminalSchedulerWorkerFailurePolicy, VersionedPlan,
        },
        DefinitionRevisionId, DeploymentRevisionId, EffectEvidence, LeafTaskExecutor, RunId,
        RunTerminalFact, RuntimeValue, SchedulerQuiescence, SchedulerTaskKind,
        TaskExecutionRequest, TaskExecutionResult, TransitionKey, TransitionOutcome,
        WorkerExecutionContext, WorkerExecutorRegistry, WorkerFailure,
    },
};
use serde_json::{json, Value};
use sqlx::{
    postgres::PgPoolOptions, sqlite::SqliteConnectOptions, AssertSqlSafe, PgPool, SqlitePool,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const AGENT_LOOP: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {seed: string}
output: string
workflow:
  steps:
    - id: reasoning
      agent_loop:
        initial: $seed
        as: state
        until: false
        max_iterations: 2
        steps:
          - id: next_state
            type: tool
            tool: fixture.next
            arguments: {state: $state}
            response: string
          - continue: $next_state
    - return: $reasoning
"#;

const KEYED_MAP: &str = r#"api_version: insight.agent/v3
kind: agent
types:
  Item:
    fields: {id: string, text: string}
inputs:
  items: Item[]
output: string[]
workflow:
  steps:
    - id: rendered
      map:
        items: $items
        key: id
        max_concurrency: 2
        steps:
          - id: render
            type: tool
            tool: fixture.next
            arguments: {item: $item}
            response: string
          - yield: $render
    - return: $rendered
"#;

const ORDINAL_MAP: &str = r#"api_version: insight.agent/v3
kind: agent
types:
  Item:
    fields: {id: string, text: string}
inputs:
  items: Item[]
output: string[]
workflow:
  steps:
    - id: rendered
      map:
        items: $items
        max_concurrency: 2
        steps:
          - id: render
            type: tool
            tool: fixture.next
            arguments: {item: $item}
            response: string
          - yield: $render
    - return: $rendered
"#;

fn version(value: &str) -> VersionTag {
    VersionTag::new(value).unwrap()
}

fn descriptor_schema(value: &DescriptorValue) -> DescriptorValueSchema {
    match value {
        DescriptorValue::Null => DescriptorValueSchema::Null,
        DescriptorValue::Boolean(_) => DescriptorValueSchema::Boolean,
        DescriptorValue::Integer(_) => DescriptorValueSchema::Integer,
        DescriptorValue::Number(_) => DescriptorValueSchema::Number,
        DescriptorValue::String(_) => DescriptorValueSchema::String,
        DescriptorValue::Array(values) => DescriptorValueSchema::Array(Box::new(
            values
                .first()
                .map(descriptor_schema)
                .unwrap_or(DescriptorValueSchema::Any),
        )),
        DescriptorValue::Object(values) => DescriptorValueSchema::Object(
            values
                .iter()
                .map(|(name, value)| {
                    (
                        name.clone(),
                        DescriptorFieldContract::required(descriptor_schema(value)),
                    )
                })
                .collect(),
        ),
    }
}

fn fixture_for(source: &str, revision: &str) -> (Plan, DescriptorContractRegistry) {
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new(revision).unwrap(),
            "agent-loop.yaml",
            source,
        ),
    )
    .unwrap();
    let index = PlanIndex::new(&plan).unwrap();
    let mut descriptors = DescriptorContractRegistry::new();
    for node in plan.nodes() {
        let NodeKind::ToolTask(descriptor) = node.kind() else {
            continue;
        };
        let public_fields = descriptor
            .public_configuration
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    DescriptorFieldContract::required(descriptor_schema(value)),
                )
            })
            .collect();
        let inputs = index
            .data_inputs(node.id())
            .iter()
            .map(|id| {
                let port = index.data_port(id).unwrap();
                (
                    port.name().clone(),
                    WorkerInputPortContract::new(port.value_type().clone(), port.required()),
                )
            })
            .collect();
        let outputs = index
            .data_outputs(node.id())
            .iter()
            .map(|id| {
                let port = index.data_port(id).unwrap();
                (port.name().clone(), port.value_type().clone())
            })
            .collect();
        descriptors
            .register(DescriptorContract::new(
                descriptor.implementation.clone(),
                descriptor.descriptor_version.clone(),
                DescriptorConfigurationContract::closed(public_fields, BTreeMap::new()),
                WorkerContract::new(LeafTaskKind::Tool, version("worker-1"), inputs, outputs),
            ))
            .unwrap();
    }
    (plan, descriptors)
}

fn fixture() -> (Plan, DescriptorContractRegistry) {
    fixture_for(AGENT_LOOP, "agent_loop_revision_v1")
}

fn deployed(plan: &Plan) -> VersionedPlan {
    deployed_for(plan, "agent_loop")
}

fn deployed_for(plan: &Plan, label: &str) -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        format!("{label}-definition"),
        format!("{label}-agent"),
        "Agent loop durable gate",
        DeploymentRevisionId::new(format!("{label}_deployment_v1")).unwrap(),
        "expression-3.0.0",
        json!({"format": "dsl-v3"}),
        plan,
        json!({}),
        json!({}),
        json!({"worker": "worker-1"}),
    )
    .unwrap()
}

#[derive(Clone)]
struct NextTurnExecutor;

#[async_trait::async_trait]
impl LeafTaskExecutor for NextTurnExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let input = request
            .inputs()
            .first()
            .map(|input| input.value().value())
            .expect("loop/Map input");
        let value = if let Some(state) = input.as_str() {
            Value::String(format!("{state}+"))
        } else {
            input.get("text").cloned().expect("Map item text input")
        };
        Ok(TaskExecutionResult::new(
            request
                .outputs()
                .iter()
                .map(|output| {
                    (
                        output.port_id().clone(),
                        RuntimeValue::new(value.clone()).unwrap(),
                    )
                })
                .collect(),
            EffectEvidence::Committed,
        ))
    }
}

fn workers(plan: &Plan) -> WorkerExecutorRegistry {
    let descriptor = plan
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::ToolTask(value) => Some(value),
            _ => None,
        })
        .unwrap();
    let mut registry = WorkerExecutorRegistry::new();
    registry
        .register(
            SchedulerTaskKind::Tool,
            descriptor.implementation.clone(),
            descriptor.descriptor_version.clone(),
            version("worker-1"),
            Arc::new(NextTurnExecutor),
        )
        .unwrap();
    registry
}

async fn drive<R: SchedulerDurableRepository + ?Sized>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    workers: &WorkerExecutorRegistry,
) -> SchedulerQuiescence {
    for step in 0..256 {
        match drive_scheduler_once(repository, linked, fence, &NoSchedulerCrash)
            .await
            .unwrap()
        {
            SchedulerDriveOutcome::Applied(_) => {}
            SchedulerDriveOutcome::Quiescent(
                terminal @ (SchedulerQuiescence::RunSucceeded
                | SchedulerQuiescence::RunFailed
                | SchedulerQuiescence::RunCancelled),
            ) => return terminal,
            SchedulerDriveOutcome::Quiescent(
                SchedulerQuiescence::WaitingForTask { .. }
                | SchedulerQuiescence::WaitingForChildren { .. },
            ) => {
                consume_scheduler_task_once(
                    repository,
                    workers,
                    &TerminalSchedulerWorkerFailurePolicy,
                    "agent-loop-worker",
                    60,
                    64,
                    CancellationToken::new(),
                    &NoSchedulerCrash,
                )
                .await
                .unwrap();
            }
            other => panic!("unexpected AgentLoop step {step}: {other:?}"),
        }
    }
    panic!("AgentLoop exhausted scheduler action budget");
}

async fn assert_sqlite_projection(pool: &SqlitePool, run_id: &RunId) {
    let scopes = sqlx::query_as::<_, (String, String, String, String)>(
        "SELECT scope_instance_id,scope_kind,stable_dynamic_key,lifecycle
         FROM scope_instances WHERE run_id=? AND scope_kind='agent_loop_turn'
         ORDER BY stable_dynamic_key",
    )
    .bind(run_id.as_str())
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(scopes.len(), 2);
    assert_eq!(scopes[0].2, "agent_loop:0");
    assert_eq!(scopes[1].2, "agent_loop:1");
    assert!(scopes
        .iter()
        .all(|scope| scope.1 == "agent_loop_turn" && scope.3 == "settled"));
    assert_ne!(scopes[0].0, scopes[1].0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scope_instances WHERE run_id=? AND scope_kind='loop_iteration'",
        )
        .bind(run_id.as_str())
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
    let root = sqlx::query_as::<_, (String, String, i64, i64)>(
        "SELECT lifecycle,admission_state,admitted_children,settled_children
         FROM scope_instances WHERE run_id=? AND is_root=1",
    )
    .bind(run_id.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        (&root.0, &root.1),
        (&"settled".to_owned(), &"closed".to_owned())
    );
    assert_eq!(root.2, 2);
    assert_eq!(root.2, root.3);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_activations WHERE run_id=? AND lifecycle IN
               ('created','ready','leased','running','retry_wait','waiting','terminating')",
        )
        .bind(run_id.as_str())
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
    let occurrences = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT a.stable_activation_key FROM node_activations a
         JOIN scope_instances s ON s.run_id=a.run_id AND s.scope_instance_id=a.scope_instance_id
         WHERE a.run_id=? AND s.scope_kind='agent_loop_turn' ORDER BY a.stable_activation_key",
    )
    .bind(run_id.as_str())
    .fetch_all(pool)
    .await
    .unwrap();
    assert!(occurrences.len() >= 2);
    assert!(occurrences
        .iter()
        .any(|value| value.contains("agent_loop_turn:0")));
    assert!(occurrences
        .iter()
        .any(|value| value.contains("agent_loop_turn:1")));
}

#[tokio::test]
async fn sqlite_agent_loop_turns_are_distinct_durable_scopes() {
    let (plan, descriptors) = fixture();
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let deployed = deployed(&plan);
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("agent-loop.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new("run_sqlite_agent_loop").unwrap();
    assert!(matches!(
        repository
            .create_run(
                TransitionKey::derive("agent-loop.sqlite", &["create"]).unwrap(),
                CreateRunCommand::new(run_id.clone(), &deployed, json!({"seed": "s0"})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='agent-loop-test',scheduler_fencing_token='agent-loop-fence',
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    let fence =
        FencedSchedulerRunCommand::new(run_id.clone(), "agent-loop-test", 1, "agent-loop-fence")
            .unwrap();
    assert_eq!(
        drive(&repository, &linked, &fence, &workers(&plan)).await,
        SchedulerQuiescence::RunFailed
    );
    assert_sqlite_projection(&control, &run_id).await;
    assert_eq!(
        drive(&repository, &linked, &fence, &workers(&plan)).await,
        SchedulerQuiescence::RunFailed
    );
    assert_sqlite_projection(&control, &run_id).await;
}

#[tokio::test]
async fn sqlite_workflow_loop_retains_loop_iteration_scope_kind() {
    let source = AGENT_LOOP.replacen("agent_loop:", "loop:", 1);
    let (plan, descriptors) = fixture_for(&source, "workflow_loop_revision_v1");
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let deployed = deployed(&plan);
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("workflow-loop.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new("run_sqlite_workflow_loop").unwrap();
    assert!(matches!(
        repository
            .create_run(
                TransitionKey::derive("workflow-loop.sqlite", &["create"]).unwrap(),
                CreateRunCommand::new(run_id.clone(), &deployed, json!({"seed": "s0"})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='workflow-loop-test',scheduler_fencing_token='workflow-loop-fence',
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    let fence = FencedSchedulerRunCommand::new(
        run_id.clone(),
        "workflow-loop-test",
        1,
        "workflow-loop-fence",
    )
    .unwrap();
    assert_eq!(
        drive(&repository, &linked, &fence, &workers(&plan)).await,
        SchedulerQuiescence::RunFailed
    );
    let scopes = sqlx::query_as::<_, (String, String, String)>(
        "SELECT scope_kind,stable_dynamic_key,lifecycle FROM scope_instances
         WHERE run_id=? AND is_root=0 ORDER BY stable_dynamic_key",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(
        scopes,
        vec![
            ("loop_iteration".into(), "loop:0".into(), "settled".into()),
            ("loop_iteration".into(), "loop:1".into(), "settled".into()),
        ]
    );
}

#[tokio::test]
async fn sqlite_agent_loop_completion_and_cancellation_drain_turn_scopes() {
    let source = AGENT_LOOP
        .replace("max_iterations: 2", "max_iterations: 3")
        .replace(
            "          - continue: $next_state",
            "          - break: $next_state",
        );
    let (plan, descriptors) = fixture_for(&source, "agent_loop_terminal_revision_v1");
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let deployed = deployed(&plan);
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("agent-loop-terminal.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();

    let success_run = RunId::new("run_sqlite_agent_loop_complete").unwrap();
    assert!(matches!(
        repository
            .create_run(
                TransitionKey::derive("agent-loop.complete", &["create"]).unwrap(),
                CreateRunCommand::new(success_run.clone(), &deployed, json!({"seed": "s0"}),)
                    .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='complete-test',scheduler_fencing_token='complete-fence',
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(success_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    let success_fence =
        FencedSchedulerRunCommand::new(success_run.clone(), "complete-test", 1, "complete-fence")
            .unwrap();
    assert_eq!(
        drive(&repository, &linked, &success_fence, &workers(&plan)).await,
        SchedulerQuiescence::RunSucceeded
    );

    let cancel_run = RunId::new("run_sqlite_agent_loop_cancel").unwrap();
    assert!(matches!(
        repository
            .create_run(
                TransitionKey::derive("agent-loop.cancel", &["create"]).unwrap(),
                CreateRunCommand::new(cancel_run.clone(), &deployed, json!({"seed": "s0"}))
                    .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='cancel-test',scheduler_fencing_token='cancel-fence',
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(cancel_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    let cancel_fence =
        FencedSchedulerRunCommand::new(cancel_run.clone(), "cancel-test", 1, "cancel-fence")
            .unwrap();
    loop {
        match drive_scheduler_once(&repository, &linked, &cancel_fence, &NoSchedulerCrash)
            .await
            .unwrap()
        {
            SchedulerDriveOutcome::Applied(_) => {}
            SchedulerDriveOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. }) => break,
            other => panic!("AgentLoop cancellation fixture did not reach a task: {other:?}"),
        }
    }
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
                termination_intent_reason='cancelled',termination_intent_transition_key=?,
                termination_intent_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
                updated_at=CURRENT_TIMESTAMP WHERE run_id=? AND lifecycle='active'",
        )
        .bind(
            TransitionKey::derive("agent-loop.cancel", &["request"])
                .unwrap()
                .as_str(),
        )
        .bind(cancel_run.as_str())
        .execute(&control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    loop {
        match drive_scheduler_once(&repository, &linked, &cancel_fence, &NoSchedulerCrash)
            .await
            .unwrap()
        {
            SchedulerDriveOutcome::Applied(_) => {}
            SchedulerDriveOutcome::Quiescent(SchedulerQuiescence::RunCancelled) => break,
            other => panic!("unexpected AgentLoop cancellation decision: {other:?}"),
        }
    }

    for (run_id, scope_lifecycle, root_lifecycle) in [
        (&success_run, "settled", "settled"),
        (&cancel_run, "cancelled", "cancelled"),
    ] {
        let turn = sqlx::query_as::<_, (String, String, String)>(
            "SELECT scope_kind,lifecycle,admission_state FROM scope_instances
             WHERE run_id=? AND scope_kind='agent_loop_turn'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(
            turn,
            (
                "agent_loop_turn".into(),
                scope_lifecycle.into(),
                "closed".into()
            )
        );
        let root = sqlx::query_as::<_, (String, String, i64, i64)>(
            "SELECT lifecycle,admission_state,admitted_children,settled_children
             FROM scope_instances WHERE run_id=? AND is_root=1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(root.0, root_lifecycle);
        assert_eq!(root.1, "closed");
        assert_eq!(root.2, 1);
        assert_eq!(root.2, root.3);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM node_activations WHERE run_id=? AND lifecycle IN
                   ('created','ready','leased','running','retry_wait','waiting','terminating')",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            0
        );
    }
}

async fn create_and_lease_sqlite_run(
    repository: &SqliteDurableRepository,
    control: &SqlitePool,
    deployed: &VersionedPlan,
    run_id: &RunId,
    input: Value,
) -> FencedSchedulerRunCommand {
    assert!(matches!(
        repository.install_versioned_plan(deployed).await.unwrap(),
        PlanInstallOutcome::Installed | PlanInstallOutcome::AlreadyInstalled
    ));
    assert!(matches!(
        repository
            .create_run(
                TransitionKey::derive("scheduler.sqlite.ordinal-map.create.v1", &[run_id.as_str()])
                    .unwrap(),
                CreateRunCommand::new(run_id.clone(), deployed, input).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let owner = format!("{}-owner", run_id.as_str());
    let fence = format!("{}-fence", run_id.as_str());
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
                scheduler_lease_epoch=1,scheduler_lease_owner=?,scheduler_fencing_token=?,
                scheduler_lease_expires_at=datetime('now','+1 hour'),
                scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND lifecycle='created'",
        )
        .bind(&owner)
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

async fn map_scopes_sqlite(pool: &SqlitePool, run_id: &RunId) -> Vec<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT scope_instance_id,stable_dynamic_key FROM scope_instances
         WHERE run_id=? AND scope_kind='map_item' ORDER BY stable_dynamic_key",
    )
    .bind(run_id.as_str())
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn sqlite_ordinal_map_preserves_order_accepts_duplicate_values_and_replays_identity() {
    let (plan, descriptors) = fixture_for(ORDINAL_MAP, "sqlite_ordinal_map_revision_v1");
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let deployed = deployed_for(&plan, "sqlite_ordinal_map");
    let workers = workers(&plan);
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("ordinal-map.sqlite");
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

    let ordered = RunId::new("run_sqlite_ordinal_map_ordered").unwrap();
    let ordered_fence = create_and_lease_sqlite_run(
        &repository,
        &control,
        &deployed,
        &ordered,
        json!({"items": [
            {"id": "same", "text": "second"},
            {"id": "same", "text": "first"}
        ]}),
    )
    .await;
    assert_eq!(
        drive(&repository, &linked, &ordered_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    let facts = repository.load_scheduler_facts(&ordered).await.unwrap();
    assert!(matches!(
        facts.terminal(),
        Some(RunTerminalFact::Succeeded(value))
            if value.value() == &json!(["second", "first"])
    ));
    let durable_scopes = map_scopes_sqlite(&control, &ordered).await;
    assert_eq!(
        durable_scopes
            .iter()
            .map(|scope| scope.1.as_str())
            .collect::<Vec<_>>(),
        vec!["ordinal:0", "ordinal:1"]
    );
    assert_ne!(durable_scopes[0].0, durable_scopes[1].0);

    drop(repository);
    control.close().await;
    let restarted = SqliteDurableRepository::connect_path(&database)
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
    assert_eq!(
        drive(&restarted, &linked, &ordered_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded,
        "terminal replay after process restart must not respawn ordinal items"
    );
    assert_eq!(
        map_scopes_sqlite(&control, &ordered).await,
        durable_scopes,
        "ordinal scope identities must be byte-stable after restart"
    );

    let empty = RunId::new("run_sqlite_ordinal_map_empty").unwrap();
    let empty_fence = create_and_lease_sqlite_run(
        &restarted,
        &control,
        &deployed,
        &empty,
        json!({"items": []}),
    )
    .await;
    assert_eq!(
        drive(&restarted, &linked, &empty_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    let empty_facts = restarted.load_scheduler_facts(&empty).await.unwrap();
    assert!(matches!(
        empty_facts.terminal(),
        Some(RunTerminalFact::Succeeded(value)) if value.value() == &json!([])
    ));
    assert!(map_scopes_sqlite(&control, &empty).await.is_empty());
    assert_eq!(
        drive(&restarted, &linked, &empty_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded
    );
}

async fn isolated_postgres() -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("V3_TEST_POSTGRES_URL").ok()?;
    let schema = format!("agent_loop_{}", Uuid::new_v4().simple());
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

async fn assert_postgres_projection(pool: &PgPool, run_id: &RunId) {
    let scopes = sqlx::query_as::<_, (String, String, String, String)>(
        "SELECT scope_instance_id,scope_kind,stable_dynamic_key,lifecycle
         FROM scope_instances WHERE run_id=$1 AND scope_kind='agent_loop_turn'
         ORDER BY stable_dynamic_key",
    )
    .bind(run_id.as_str())
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(scopes.len(), 2);
    assert_eq!(scopes[0].2, "agent_loop:0");
    assert_eq!(scopes[1].2, "agent_loop:1");
    assert!(scopes
        .iter()
        .all(|scope| scope.1 == "agent_loop_turn" && scope.3 == "settled"));
    assert_ne!(scopes[0].0, scopes[1].0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scope_instances WHERE run_id=$1 AND scope_kind='loop_iteration'",
        )
        .bind(run_id.as_str())
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
    let root = sqlx::query_as::<_, (String, String, i64, i64)>(
        "SELECT lifecycle,admission_state,admitted_children,settled_children
         FROM scope_instances WHERE run_id=$1 AND is_root=TRUE",
    )
    .bind(run_id.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        (&root.0, &root.1),
        (&"settled".to_owned(), &"closed".to_owned())
    );
    assert_eq!(root.2, 2);
    assert_eq!(root.2, root.3);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_activations WHERE run_id=$1 AND lifecycle IN
               ('created','ready','leased','running','retry_wait','waiting','terminating')",
        )
        .bind(run_id.as_str())
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
    let occurrences = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT stable_activation_key FROM node_activations a
         JOIN scope_instances s ON s.run_id=a.run_id AND s.scope_instance_id=a.scope_instance_id
         WHERE a.run_id=$1 AND s.scope_kind='agent_loop_turn' ORDER BY stable_activation_key",
    )
    .bind(run_id.as_str())
    .fetch_all(pool)
    .await
    .unwrap();
    assert!(occurrences.len() >= 2);
    assert!(occurrences
        .iter()
        .any(|value| value.contains("agent_loop_turn:0")));
    assert!(occurrences
        .iter()
        .any(|value| value.contains("agent_loop_turn:1")));
}

#[tokio::test]
async fn postgres_agent_loop_turns_are_distinct_durable_scopes() {
    let Some((repository, control, admin, schema)) = isolated_postgres().await else {
        return;
    };
    let (plan, descriptors) = fixture();
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let deployed = deployed(&plan);
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new("run_postgres_agent_loop").unwrap();
    assert!(matches!(
        repository
            .create_run(
                TransitionKey::derive("agent-loop.postgres", &["create"]).unwrap(),
                CreateRunCommand::new(run_id.clone(), &deployed, json!({"seed": "s0"})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='agent-loop-test',scheduler_fencing_token='agent-loop-fence',
            scheduler_lease_expires_at=CURRENT_TIMESTAMP + INTERVAL '1 hour',
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    let fence =
        FencedSchedulerRunCommand::new(run_id.clone(), "agent-loop-test", 1, "agent-loop-fence")
            .unwrap();
    assert_eq!(
        drive(&repository, &linked, &fence, &workers(&plan)).await,
        SchedulerQuiescence::RunFailed
    );
    assert_postgres_projection(&control, &run_id).await;
    assert_eq!(
        drive(&repository, &linked, &fence, &workers(&plan)).await,
        SchedulerQuiescence::RunFailed
    );
    assert_postgres_projection(&control, &run_id).await;

    drop(repository);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

async fn create_and_lease_postgres_run(
    repository: &PostgresDurableRepository,
    control: &PgPool,
    deployed: &VersionedPlan,
    run_id: &RunId,
    input: Value,
) -> FencedSchedulerRunCommand {
    assert!(matches!(
        repository.install_versioned_plan(deployed).await.unwrap(),
        PlanInstallOutcome::Installed | PlanInstallOutcome::AlreadyInstalled
    ));
    assert!(matches!(
        repository
            .create_run(
                TransitionKey::derive("scheduler.pg16.exit-gate.create.v1", &[run_id.as_str()])
                    .unwrap(),
                CreateRunCommand::new(run_id.clone(), deployed, input).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let owner = format!("{}-owner", run_id.as_str());
    let fence = format!("{}-fence", run_id.as_str());
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
                scheduler_lease_epoch=1,scheduler_lease_owner=$1,scheduler_fencing_token=$2,
                scheduler_lease_expires_at=CURRENT_TIMESTAMP + INTERVAL '1 hour',
                scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=$3 AND lifecycle='created'",
        )
        .bind(&owner)
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

async fn assert_one_postgres_terminal(pool: &PgPool, run_id: &RunId, lifecycle: &str) {
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM workflow_runs WHERE run_id=$1 AND terminal_event_id IS NOT NULL",
        )
        .bind(run_id.as_str())
        .fetch_one(pool)
        .await
        .unwrap(),
        lifecycle
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events e
             JOIN workflow_runs r ON r.run_id=e.run_id AND r.terminal_event_id=e.event_id
             WHERE e.run_id=$1 AND e.kind='run.lifecycle_changed'",
        )
        .bind(run_id.as_str())
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
}

async fn map_scopes_postgres(pool: &PgPool, run_id: &RunId) -> Vec<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT scope_instance_id,stable_dynamic_key FROM scope_instances
         WHERE run_id=$1 AND scope_kind='map_item' ORDER BY stable_dynamic_key",
    )
    .bind(run_id.as_str())
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn postgres16_keyed_map_preserves_order_handles_empty_and_fails_closed_on_duplicates() {
    let Some((repository, control, admin, schema)) = isolated_postgres().await else {
        return;
    };
    let (plan, descriptors) = fixture_for(KEYED_MAP, "pg16_keyed_map_revision_v1");
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let deployed = deployed_for(&plan, "pg16_keyed_map");
    let workers = workers(&plan);

    let ordered = RunId::new("run_pg16_keyed_map_ordered").unwrap();
    let ordered_fence = create_and_lease_postgres_run(
        &repository,
        &control,
        &deployed,
        &ordered,
        json!({"items": [
            {"id": "key:x", "text": "second"},
            {"id": "x", "text": "first"}
        ]}),
    )
    .await;
    assert_eq!(
        drive(&repository, &linked, &ordered_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    let ordered_facts = repository.load_scheduler_facts(&ordered).await.unwrap();
    assert!(matches!(
        ordered_facts.terminal(),
        Some(RunTerminalFact::Succeeded(value))
            if value.value() == &json!(["second", "first"])
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scope_instances
             WHERE run_id=$1 AND scope_kind='map_item' AND lifecycle='settled'",
        )
        .bind(ordered.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2
    );
    let keyed_scopes = map_scopes_postgres(&control, &ordered).await;
    assert_eq!(
        keyed_scopes
            .iter()
            .map(|scope| scope.1.as_str())
            .collect::<Vec<_>>(),
        vec!["key:key:x", "key:x"],
        "business keys are encoded exactly once and cannot collide with the key namespace"
    );
    assert_ne!(keyed_scopes[0].0, keyed_scopes[1].0);
    assert_eq!(
        drive(&repository, &linked, &ordered_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded,
        "replay after the durable terminal must not spawn Map items again"
    );
    assert_one_postgres_terminal(&control, &ordered, "succeeded").await;

    let empty = RunId::new("run_pg16_keyed_map_empty").unwrap();
    let empty_fence = create_and_lease_postgres_run(
        &repository,
        &control,
        &deployed,
        &empty,
        json!({"items": []}),
    )
    .await;
    assert_eq!(
        drive(&repository, &linked, &empty_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    let empty_facts = repository.load_scheduler_facts(&empty).await.unwrap();
    assert!(matches!(
        empty_facts.terminal(),
        Some(RunTerminalFact::Succeeded(value)) if value.value() == &json!([])
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scope_instances WHERE run_id=$1 AND scope_kind='map_item'",
        )
        .bind(empty.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0,
        "an empty Map succeeds without fabricating an item scope"
    );
    assert_one_postgres_terminal(&control, &empty, "succeeded").await;

    let duplicate = RunId::new("run_pg16_keyed_map_duplicate").unwrap();
    let duplicate_fence = create_and_lease_postgres_run(
        &repository,
        &control,
        &deployed,
        &duplicate,
        json!({"items": [
            {"id": "same", "text": "one"},
            {"id": "same", "text": "two"}
        ]}),
    )
    .await;
    assert_eq!(
        drive(&repository, &linked, &duplicate_fence, &workers).await,
        SchedulerQuiescence::RunFailed
    );
    assert_eq!(
        drive(&repository, &linked, &duplicate_fence, &workers).await,
        SchedulerQuiescence::RunFailed,
        "the closed planning failure is exactly replayable"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_checkpoints WHERE run_id=$1
               AND fact_payload->'action'->>'kind'='fail_run_planning'",
        )
        .bind(duplicate.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scope_instances WHERE run_id=$1 AND scope_kind='map_item'",
        )
        .bind(duplicate.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0,
        "duplicate dynamic keys fail before partial item admission"
    );
    assert_one_postgres_terminal(&control, &duplicate, "failed").await;

    drop(repository);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres16_ordinal_map_preserves_order_accepts_duplicate_values_and_replays_identity() {
    let Some((repository, control, admin, schema)) = isolated_postgres().await else {
        return;
    };
    let (plan, descriptors) = fixture_for(ORDINAL_MAP, "pg16_ordinal_map_revision_v1");
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let deployed = deployed_for(&plan, "pg16_ordinal_map");
    let workers = workers(&plan);

    let ordered = RunId::new("run_pg16_ordinal_map_ordered").unwrap();
    let ordered_fence = create_and_lease_postgres_run(
        &repository,
        &control,
        &deployed,
        &ordered,
        json!({"items": [
            {"id": "same", "text": "second"},
            {"id": "same", "text": "first"}
        ]}),
    )
    .await;
    assert_eq!(
        drive(&repository, &linked, &ordered_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    let ordered_facts = repository.load_scheduler_facts(&ordered).await.unwrap();
    assert!(matches!(
        ordered_facts.terminal(),
        Some(RunTerminalFact::Succeeded(value))
            if value.value() == &json!(["second", "first"])
    ));
    let durable_scopes = map_scopes_postgres(&control, &ordered).await;
    assert_eq!(
        durable_scopes
            .iter()
            .map(|scope| scope.1.as_str())
            .collect::<Vec<_>>(),
        vec!["ordinal:0", "ordinal:1"]
    );
    assert_ne!(durable_scopes[0].0, durable_scopes[1].0);

    drop(repository);
    let restarted = PostgresDurableRepository::from_pool(control.clone());
    assert_eq!(
        drive(&restarted, &linked, &ordered_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded,
        "durable terminal replay must not respawn ordinal Map items"
    );
    assert_eq!(
        map_scopes_postgres(&control, &ordered).await,
        durable_scopes,
        "ordinal scope identities must be byte-stable across repository reconstruction"
    );
    assert_one_postgres_terminal(&control, &ordered, "succeeded").await;

    let empty = RunId::new("run_pg16_ordinal_map_empty").unwrap();
    let empty_fence = create_and_lease_postgres_run(
        &restarted,
        &control,
        &deployed,
        &empty,
        json!({"items": []}),
    )
    .await;
    assert_eq!(
        drive(&restarted, &linked, &empty_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    let empty_facts = restarted.load_scheduler_facts(&empty).await.unwrap();
    assert!(matches!(
        empty_facts.terminal(),
        Some(RunTerminalFact::Succeeded(value)) if value.value() == &json!([])
    ));
    assert!(map_scopes_postgres(&control, &empty).await.is_empty());
    assert_eq!(
        drive(&restarted, &linked, &empty_fence, &workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_one_postgres_terminal(&control, &empty, "succeeded").await;

    drop(restarted);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn postgres16_workflow_loop_iterations_are_durable_for_budget_and_break_replay() {
    let Some((repository, control, admin, schema)) = isolated_postgres().await else {
        return;
    };
    let budget_source = AGENT_LOOP.replacen("agent_loop:", "loop:", 1);
    let (budget_plan, budget_descriptors) =
        fixture_for(&budget_source, "pg16_workflow_loop_budget_revision_v1");
    let subflows = SubflowContractRegistry::new();
    let budget_linked = LinkedPlan::link(&budget_plan, &budget_descriptors, &subflows).unwrap();
    let budget_deployed = deployed_for(&budget_plan, "pg16_workflow_loop_budget");
    let budget_workers = workers(&budget_plan);
    let budget_run = RunId::new("run_pg16_workflow_loop_budget").unwrap();
    let budget_fence = create_and_lease_postgres_run(
        &repository,
        &control,
        &budget_deployed,
        &budget_run,
        json!({"seed": "s0"}),
    )
    .await;
    assert_eq!(
        drive(&repository, &budget_linked, &budget_fence, &budget_workers).await,
        SchedulerQuiescence::RunFailed
    );
    let scopes = sqlx::query_as::<_, (String, String, String)>(
        "SELECT scope_instance_id,stable_dynamic_key,lifecycle FROM scope_instances
         WHERE run_id=$1 AND scope_kind='loop_iteration' ORDER BY stable_dynamic_key",
    )
    .bind(budget_run.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(scopes.len(), 2);
    assert_eq!(scopes[0].1, "loop:0");
    assert_eq!(scopes[1].1, "loop:1");
    assert_ne!(scopes[0].0, scopes[1].0);
    assert!(scopes.iter().all(|scope| scope.2 == "settled"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(DISTINCT a.activation_id) FROM node_activations a
             JOIN scope_instances s ON s.run_id=a.run_id AND s.scope_instance_id=a.scope_instance_id
             WHERE a.run_id=$1 AND s.scope_kind='loop_iteration'",
        )
        .bind(budget_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2,
        "each workflow Loop iteration owns a distinct durable Activation"
    );
    assert_eq!(
        drive(&repository, &budget_linked, &budget_fence, &budget_workers).await,
        SchedulerQuiescence::RunFailed
    );
    assert_one_postgres_terminal(&control, &budget_run, "failed").await;

    let break_source = budget_source
        .replace("max_iterations: 2", "max_iterations: 3")
        .replace(
            "          - continue: $next_state",
            "          - break: $next_state",
        );
    let (break_plan, break_descriptors) =
        fixture_for(&break_source, "pg16_workflow_loop_break_revision_v1");
    let break_linked = LinkedPlan::link(&break_plan, &break_descriptors, &subflows).unwrap();
    let break_deployed = deployed_for(&break_plan, "pg16_workflow_loop_break");
    let break_workers = workers(&break_plan);
    let break_run = RunId::new("run_pg16_workflow_loop_break").unwrap();
    let break_fence = create_and_lease_postgres_run(
        &repository,
        &control,
        &break_deployed,
        &break_run,
        json!({"seed": "s0"}),
    )
    .await;
    assert_eq!(
        drive(&repository, &break_linked, &break_fence, &break_workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String, String)>(
            "SELECT scope_kind,stable_dynamic_key,lifecycle FROM scope_instances
             WHERE run_id=$1 AND is_root=FALSE",
        )
        .bind(break_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        ("loop_iteration".into(), "loop:0".into(), "settled".into())
    );
    assert_eq!(
        drive(&repository, &break_linked, &break_fence, &break_workers).await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_one_postgres_terminal(&control, &break_run, "succeeded").await;

    drop(repository);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
