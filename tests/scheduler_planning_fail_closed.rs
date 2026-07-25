#[path = "support/database.rs"]
mod database;

use std::collections::BTreeMap;

use insight_agent_platform::{
    dsl::{compile_source, CompileOptions},
    engine::{
        plan::{
            DescriptorConfigurationContract, DescriptorContract, DescriptorContractRegistry,
            DescriptorFieldContract, DescriptorValue, DescriptorValueSchema, LeafTaskKind,
            LinkedPlan, NodeKind, Plan, PlanIndex, SubflowContractRegistry, VersionTag,
            WorkerContract, WorkerInputPortContract,
        },
        repository::{
            drive_scheduler_once, CreateRunCommand, DurableRepository, FencedSchedulerRunCommand,
            NoSchedulerCrash, PlanInstallOutcome, PostgresDurableRepository, SchedulerDriveOutcome,
            SchedulerDurableRepository, SqliteDurableRepository, VersionedPlan,
        },
        DefinitionRevisionId, DeploymentRevisionId, RunId, SchedulerPlanner,
        SchedulerPlanningFailure, SchedulerQuiescence, TransitionKey, TransitionOutcome,
    },
};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, sqlite::SqliteConnectOptions, AssertSqlSafe, PgPool, Row};
use uuid::Uuid;

const INVALID_RAISE_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  code: string
output: string
workflow:
  steps:
    - raise:
        kind: safe_error
        code: $code
        message: rejected
"#;

const DUPLICATE_MAP_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
types:
  Item:
    fields: {id: string, text: string}
inputs:
  items: Item[]
output: string[]
workflow:
  steps:
    - id: copied
      map:
        items: $items
        key: id
        as: item
        max_concurrency: 2
        steps:
          - id: rendered_item
            if: "true"
            then:
              - yield: copied
            else:
              - yield: skipped
          - yield: $rendered_item
    - return: $copied
"#;

const CORRUPT_FACT_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: echo
      type: action
      call: fixture.echo
      inputs: {question: $question}
      response: string
    - return: $echo
"#;

const CORRUPT_WAIT_FACT_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: pause
      wait: {duration_ms: 600000}
    - return: completed
"#;

#[derive(Clone, Copy)]
enum FailureCase {
    InvalidRaise,
    DuplicateMapKey,
    CorruptFact,
    CorruptWaitFact,
}

impl FailureCase {
    fn label(self) -> &'static str {
        match self {
            Self::InvalidRaise => "invalid_raise",
            Self::DuplicateMapKey => "duplicate_map_key",
            Self::CorruptFact => "corrupt_fact",
            Self::CorruptWaitFact => "corrupt_wait_fact",
        }
    }

    fn source(self) -> &'static str {
        match self {
            Self::InvalidRaise => INVALID_RAISE_AGENT,
            Self::DuplicateMapKey => DUPLICATE_MAP_AGENT,
            Self::CorruptFact => CORRUPT_FACT_AGENT,
            Self::CorruptWaitFact => CORRUPT_WAIT_FACT_AGENT,
        }
    }

    fn input(self) -> Value {
        match self {
            Self::InvalidRaise => json!({"code": "not_a_public_code"}),
            Self::DuplicateMapKey => json!({
                "items": [
                    {"id": "same", "text": "one"},
                    {"id": "same", "text": "two"}
                ]
            }),
            Self::CorruptFact => json!({"question": "hello"}),
            Self::CorruptWaitFact => json!({}),
        }
    }

    fn failure(self) -> SchedulerPlanningFailure {
        match self {
            Self::InvalidRaise => SchedulerPlanningFailure::ValueTypeMismatch,
            Self::DuplicateMapKey => SchedulerPlanningFailure::DynamicKeyDuplicate,
            Self::CorruptFact | Self::CorruptWaitFact => SchedulerPlanningFailure::FactInconsistent,
        }
    }
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

fn descriptors(plan: &Plan) -> DescriptorContractRegistry {
    let index = PlanIndex::new(plan).unwrap();
    let mut registry = DescriptorContractRegistry::new();
    for node in plan.nodes() {
        let (kind, descriptor) = match node.kind() {
            NodeKind::LlmTask(value) => (LeafTaskKind::Llm, value),
            NodeKind::ActionTask(value) => (LeafTaskKind::Action, value),
            NodeKind::HttpTask(value) => (LeafTaskKind::Http, value),
            NodeKind::ToolTask(value) => (LeafTaskKind::Tool, value),
            _ => continue,
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
        registry
            .register(DescriptorContract::new(
                descriptor.implementation.clone(),
                descriptor.descriptor_version.clone(),
                DescriptorConfigurationContract::closed(public_fields, BTreeMap::new()),
                WorkerContract::new(
                    kind,
                    VersionTag::new("planning-worker-v1").unwrap(),
                    inputs,
                    outputs,
                ),
            ))
            .unwrap();
    }
    registry
}

fn transition(label: &str, run_id: &RunId) -> TransitionKey {
    TransitionKey::derive(
        "scheduler.planning-fail-closed.test.v1",
        &[label, run_id.as_str()],
    )
    .unwrap()
}

fn compile(case: FailureCase) -> Plan {
    compile_source(
        case.source(),
        CompileOptions::new(
            DefinitionRevisionId::new(format!("planning_{}_revision", case.label())).unwrap(),
            format!("{}.yaml", case.label()),
            case.source(),
        ),
    )
    .unwrap()
}

fn versioned(case: FailureCase, plan: &Plan) -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        format!("planning_{}_definition", case.label()),
        format!("planning_{}_agent", case.label()),
        format!("planning_{}_agent", case.label()),
        DeploymentRevisionId::new(format!("planning_{}_deployment", case.label())).unwrap(),
        "expression-3.0.0",
        json!({"format": "planning-fail-closed-test"}),
        plan,
        json!({}),
        json!({"case": case.label()}),
        json!({}),
    )
    .unwrap()
}

async fn drive_to_failed<R: SchedulerDurableRepository + ?Sized>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
) {
    for _ in 0..32 {
        match drive_scheduler_once(repository, linked, fence, &NoSchedulerCrash)
            .await
            .unwrap()
        {
            SchedulerDriveOutcome::Applied(_) => {}
            SchedulerDriveOutcome::Quiescent(SchedulerQuiescence::RunFailed) => return,
            outcome => panic!("unexpected planning failure drive outcome: {outcome:?}"),
        }
    }
    panic!("planning failure did not reach its durable terminal")
}

async fn create_sqlite_run(
    repository: &SqliteDurableRepository,
    deployed: &VersionedPlan,
    run_id: &RunId,
    input: Value,
) {
    assert_eq!(
        repository.install_versioned_plan(deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    assert!(matches!(
        repository
            .create_run(
                transition("create", run_id),
                CreateRunCommand::new(run_id.clone(), deployed, input).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
}

async fn lease_sqlite(pool: &sqlx::SqlitePool, run_id: &RunId) -> FencedSchedulerRunCommand {
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
            scheduler_lease_epoch=1,scheduler_lease_owner='planning-test',
            scheduler_fencing_token='planning-fence',
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND lifecycle='created'",
    )
    .bind(run_id.as_str())
    .execute(pool)
    .await
    .unwrap();
    FencedSchedulerRunCommand::new(run_id.clone(), "planning-test", 1, "planning-fence").unwrap()
}

async fn reach_corruptible_state<R: SchedulerDurableRepository + ?Sized>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    case: FailureCase,
) {
    for _ in 0..16 {
        match drive_scheduler_once(repository, linked, fence, &NoSchedulerCrash)
            .await
            .unwrap()
        {
            SchedulerDriveOutcome::Applied(_) => {}
            SchedulerDriveOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
                if matches!(case, FailureCase::CorruptFact) =>
            {
                return;
            }
            SchedulerDriveOutcome::Quiescent(SchedulerQuiescence::WaitingForWait { .. })
                if matches!(case, FailureCase::CorruptWaitFact) =>
            {
                return;
            }
            outcome => panic!("unexpected corruptible-state outcome: {outcome:?}"),
        }
    }
    panic!("fixture did not reach a corruptible scheduler state")
}

async fn corrupt_sqlite_fact(pool: &sqlx::SqlitePool, run_id: &RunId, case: FailureCase) {
    let rows = sqlx::query(
        "SELECT checkpoint_id,fact_payload FROM scheduler_checkpoints
         WHERE run_id=? AND checkpoint_kind='planned_action'
         ORDER BY scheduler_projection_version",
    )
    .bind(run_id.as_str())
    .fetch_all(pool)
    .await
    .unwrap();
    let expected_kind = match case {
        FailureCase::CorruptFact => "dispatch_task",
        FailureCase::CorruptWaitFact => "register_wait",
        _ => panic!("case has no corrupt fact fixture"),
    };
    let (checkpoint_id, mut payload) = rows
        .into_iter()
        .find_map(|row| {
            let payload =
                serde_json::from_str::<Value>(&row.get::<String, _>("fact_payload")).unwrap();
            (payload["action"]["kind"] == expected_kind)
                .then(|| (row.get::<String, _>("checkpoint_id"), payload))
        })
        .unwrap();
    match case {
        FailureCase::CorruptFact => {
            payload["action"]["task_id"] = json!(format!("task_{}", "0".repeat(64)));
        }
        FailureCase::CorruptWaitFact => {
            // Keep valid JSON while making the persisted intent undecodable;
            // drive_scheduler_once must still terminalize from the minimal Run
            // projection instead of returning a permanent repository hot loop.
            payload["schema_version"] = json!(999);
        }
        _ => unreachable!(),
    }
    sqlx::query(
        "UPDATE scheduler_checkpoints SET fact_payload=?
         WHERE run_id=? AND checkpoint_id=?",
    )
    .bind(serde_json::to_string(&payload).unwrap())
    .bind(run_id.as_str())
    .bind(checkpoint_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn assert_sqlite_closed_world(pool: &sqlx::SqlitePool, run_id: &RunId, case: FailureCase) {
    let run = sqlx::query(
        "SELECT lifecycle,admission_state,error_code,projection_version
         FROM workflow_runs WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(run.get::<String, _>("lifecycle"), "failed");
    assert_eq!(run.get::<String, _>("admission_state"), "closed");
    assert_eq!(
        run.get::<String, _>("error_code"),
        case.failure().internal_code()
    );
    let root = sqlx::query(
        "SELECT lifecycle,admission_state,admitted_children,settled_children
         FROM scope_instances WHERE run_id=? AND is_root=1",
    )
    .bind(run_id.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(root.get::<String, _>("lifecycle"), "cancelled");
    assert_eq!(root.get::<String, _>("admission_state"), "closed");
    assert_eq!(
        root.get::<i64, _>("admitted_children"),
        root.get::<i64, _>("settled_children")
    );
    let live = sqlx::query_scalar::<_, i64>(
        "SELECT
            (SELECT COUNT(*) FROM scope_instances WHERE run_id=?
               AND lifecycle IN ('active','settling'))
          + (SELECT COUNT(*) FROM node_activations WHERE run_id=?
               AND lifecycle IN ('created','ready','leased','running','retry_wait','waiting','terminating'))
          + (SELECT COUNT(*) FROM node_attempts WHERE run_id=?
               AND lifecycle IN ('created','leased','running'))
          + (SELECT COUNT(*) FROM task_outbox WHERE run_id=?
               AND task_state IN ('pending','claimed','published'))
          + (SELECT COUNT(*) FROM timers WHERE run_id=? AND timer_state='scheduled')
          + (SELECT COUNT(*) FROM scheduler_wait_registrations WHERE run_id=?
               AND winner_kind IS NULL)
          + (SELECT COUNT(*) FROM control_tokens WHERE run_id=? AND token_state='available')",
    )
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(live, 0);
    if matches!(case, FailureCase::CorruptFact) {
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT task_state FROM task_outbox WHERE run_id=?",)
                .bind(run_id.as_str())
                .fetch_one(pool)
                .await
                .unwrap(),
            "dead"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT lifecycle FROM node_attempts WHERE run_id=?",)
                .bind(run_id.as_str())
                .fetch_one(pool)
                .await
                .unwrap(),
            "cancelled"
        );
    }
    if matches!(case, FailureCase::CorruptWaitFact) {
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT timer_state FROM timers WHERE run_id=?",)
                .bind(run_id.as_str())
                .fetch_one(pool)
                .await
                .unwrap(),
            "cancelled"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT winner_kind FROM scheduler_wait_registrations WHERE run_id=?",
            )
            .bind(run_id.as_str())
            .fetch_one(pool)
            .await
            .unwrap(),
            "cancelled"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_checkpoints WHERE run_id=?
               AND json_extract(fact_payload,'$.action.kind')='fail_run_planning'",
        )
        .bind(run_id.as_str())
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM public_event_outbox WHERE run_id=? AND is_terminal=1",
        )
        .bind(run_id.as_str())
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events e
             JOIN workflow_runs r ON r.run_id=e.run_id AND r.terminal_event_id=e.event_id
             WHERE e.run_id=? AND e.kind='run.lifecycle_changed'",
        )
        .bind(run_id.as_str())
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
    let safe = sqlx::query_scalar::<_, String>(
        "SELECT safe_envelope FROM public_event_outbox WHERE run_id=? AND is_terminal=1",
    )
    .bind(run_id.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(safe.contains(case.failure().public_code()));
    assert!(!safe.contains("ENGINE_SCHEDULER_"));
}

#[tokio::test]
async fn sqlite_planner_failures_commit_one_closed_terminal_and_exact_replay() {
    for case in [
        FailureCase::InvalidRaise,
        FailureCase::DuplicateMapKey,
        FailureCase::CorruptFact,
        FailureCase::CorruptWaitFact,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(format!("{}.sqlite", case.label()));
        database::provision_sqlite_database(&path).await;
        let repository = SqliteDurableRepository::connect_path(&path).await.unwrap();
        let control = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .foreign_keys(true),
            )
            .await
            .unwrap();
        let plan = compile(case);
        let deployed = versioned(case, &plan);
        let descriptors = descriptors(&plan);
        let subflows = SubflowContractRegistry::new();
        let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
        let run_id = RunId::new(format!("run_sqlite_planning_{}", case.label())).unwrap();
        create_sqlite_run(&repository, &deployed, &run_id, case.input()).await;
        let fence = lease_sqlite(&control, &run_id).await;
        if matches!(
            case,
            FailureCase::CorruptFact | FailureCase::CorruptWaitFact
        ) {
            reach_corruptible_state(&repository, &linked, &fence, case).await;
            corrupt_sqlite_fact(&control, &run_id, case).await;
        }
        drive_to_failed(&repository, &linked, &fence).await;
        assert_sqlite_closed_world(&control, &run_id, case).await;

        let projection = repository.load_run(&run_id).await.unwrap().unwrap();
        let replay = SchedulerPlanner::new(&linked)
            .fail_closed_action_at(&run_id, projection.projection_version(), case.failure())
            .unwrap();
        assert!(matches!(
            repository
                .commit_scheduler_action(&fence, &replay)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        assert_sqlite_closed_world(&control, &run_id, case).await;
    }
}

async fn isolated_postgres() -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("planning_fail_closed_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let version = sqlx::query_scalar::<_, String>("SHOW server_version_num")
        .fetch_one(&admin)
        .await
        .unwrap()
        .parse::<i32>()
        .unwrap();
    assert!(version >= 160_000, "planner gate requires PostgreSQL 16+");
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped)
        .await
        .unwrap();
    database::provision_postgres_schema(&control).await;
    let repository = PostgresDurableRepository::connect(&scoped).await.unwrap();
    Some((repository, control, admin, schema))
}

async fn lease_postgres(pool: &PgPool, run_id: &RunId) -> FencedSchedulerRunCommand {
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
            scheduler_lease_epoch=1,scheduler_lease_owner='planning-test',
            scheduler_fencing_token='planning-fence',
            scheduler_lease_expires_at=CURRENT_TIMESTAMP+INTERVAL '1 hour',
            scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
         WHERE run_id=$1 AND lifecycle='created'",
    )
    .bind(run_id.as_str())
    .execute(pool)
    .await
    .unwrap();
    FencedSchedulerRunCommand::new(run_id.clone(), "planning-test", 1, "planning-fence").unwrap()
}

async fn corrupt_postgres_fact(pool: &PgPool, run_id: &RunId, case: FailureCase) {
    let rows = sqlx::query(
        "SELECT checkpoint_id,fact_payload FROM scheduler_checkpoints
         WHERE run_id=$1 AND checkpoint_kind='planned_action'
         ORDER BY scheduler_projection_version",
    )
    .bind(run_id.as_str())
    .fetch_all(pool)
    .await
    .unwrap();
    let expected_kind = match case {
        FailureCase::CorruptFact => "dispatch_task",
        FailureCase::CorruptWaitFact => "register_wait",
        _ => panic!("case has no corrupt fact fixture"),
    };
    let (checkpoint_id, mut payload) = rows
        .into_iter()
        .find_map(|row| {
            let payload = row.get::<Value, _>("fact_payload");
            (payload["action"]["kind"] == expected_kind)
                .then(|| (row.get::<String, _>("checkpoint_id"), payload))
        })
        .unwrap();
    match case {
        FailureCase::CorruptFact => {
            payload["action"]["task_id"] = json!(format!("task_{}", "0".repeat(64)));
        }
        FailureCase::CorruptWaitFact => {
            payload["schema_version"] = json!(999);
        }
        _ => unreachable!(),
    }
    sqlx::query(
        "UPDATE scheduler_checkpoints SET fact_payload=$1
         WHERE run_id=$2 AND checkpoint_id=$3",
    )
    .bind(payload)
    .bind(run_id.as_str())
    .bind(checkpoint_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn assert_postgres_closed_world(pool: &PgPool, run_id: &RunId, case: FailureCase) {
    let run = sqlx::query(
        "SELECT lifecycle,admission_state,error_code FROM workflow_runs WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(run.get::<String, _>("lifecycle"), "failed");
    assert_eq!(run.get::<String, _>("admission_state"), "closed");
    assert_eq!(
        run.get::<String, _>("error_code"),
        case.failure().internal_code()
    );
    let root = sqlx::query(
        "SELECT lifecycle,admission_state,admitted_children,settled_children
         FROM scope_instances WHERE run_id=$1 AND is_root",
    )
    .bind(run_id.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(root.get::<String, _>("lifecycle"), "cancelled");
    assert_eq!(root.get::<String, _>("admission_state"), "closed");
    assert_eq!(
        root.get::<i64, _>("admitted_children"),
        root.get::<i64, _>("settled_children")
    );
    let live = sqlx::query_scalar::<_, i64>(
        "SELECT
            (SELECT COUNT(*) FROM scope_instances WHERE run_id=$1
               AND lifecycle IN ('active','settling'))
          + (SELECT COUNT(*) FROM node_activations WHERE run_id=$1
               AND lifecycle IN ('created','ready','leased','running','retry_wait','waiting','terminating'))
          + (SELECT COUNT(*) FROM node_attempts WHERE run_id=$1
               AND lifecycle IN ('created','leased','running'))
          + (SELECT COUNT(*) FROM task_outbox WHERE run_id=$1
               AND task_state IN ('pending','claimed','published'))
          + (SELECT COUNT(*) FROM timers WHERE run_id=$1 AND timer_state='scheduled')
          + (SELECT COUNT(*) FROM scheduler_wait_registrations WHERE run_id=$1
               AND winner_kind IS NULL)
          + (SELECT COUNT(*) FROM control_tokens WHERE run_id=$1 AND token_state='available')",
    )
    .bind(run_id.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(live, 0);
    if matches!(case, FailureCase::CorruptFact) {
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT task_state FROM task_outbox WHERE run_id=$1",)
                .bind(run_id.as_str())
                .fetch_one(pool)
                .await
                .unwrap(),
            "dead"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT lifecycle FROM node_attempts WHERE run_id=$1",)
                .bind(run_id.as_str())
                .fetch_one(pool)
                .await
                .unwrap(),
            "cancelled"
        );
    }
    if matches!(case, FailureCase::CorruptWaitFact) {
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT timer_state FROM timers WHERE run_id=$1",)
                .bind(run_id.as_str())
                .fetch_one(pool)
                .await
                .unwrap(),
            "cancelled"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT winner_kind FROM scheduler_wait_registrations WHERE run_id=$1",
            )
            .bind(run_id.as_str())
            .fetch_one(pool)
            .await
            .unwrap(),
            "cancelled"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_checkpoints WHERE run_id=$1
               AND fact_payload->'action'->>'kind'='fail_run_planning'",
        )
        .bind(run_id.as_str())
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
    let safe = sqlx::query_scalar::<_, Value>(
        "SELECT safe_envelope FROM public_event_outbox WHERE run_id=$1 AND is_terminal",
    )
    .bind(run_id.as_str())
    .fetch_one(pool)
    .await
    .unwrap();
    let safe = serde_json::to_string(&safe).unwrap();
    assert!(safe.contains(case.failure().public_code()));
    assert!(!safe.contains("ENGINE_SCHEDULER_"));
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

#[tokio::test]
async fn postgres16_planner_failures_commit_one_closed_terminal_and_exact_replay() {
    let Some((repository, control, admin, schema)) = isolated_postgres().await else {
        return;
    };
    for case in [
        FailureCase::InvalidRaise,
        FailureCase::DuplicateMapKey,
        FailureCase::CorruptFact,
        FailureCase::CorruptWaitFact,
    ] {
        let plan = compile(case);
        let deployed = versioned(case, &plan);
        assert_eq!(
            repository.install_versioned_plan(&deployed).await.unwrap(),
            PlanInstallOutcome::Installed
        );
        let descriptors = descriptors(&plan);
        let subflows = SubflowContractRegistry::new();
        let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
        let run_id = RunId::new(format!("run_pg_planning_{}", case.label())).unwrap();
        assert!(matches!(
            repository
                .create_run(
                    transition("create", &run_id),
                    CreateRunCommand::new(run_id.clone(), &deployed, case.input()).unwrap(),
                )
                .await
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        let fence = lease_postgres(&control, &run_id).await;
        if matches!(
            case,
            FailureCase::CorruptFact | FailureCase::CorruptWaitFact
        ) {
            reach_corruptible_state(&repository, &linked, &fence, case).await;
            corrupt_postgres_fact(&control, &run_id, case).await;
        }
        drive_to_failed(&repository, &linked, &fence).await;
        assert_postgres_closed_world(&control, &run_id, case).await;
        let projection = repository.load_run(&run_id).await.unwrap().unwrap();
        let replay = SchedulerPlanner::new(&linked)
            .fail_closed_action_at(&run_id, projection.projection_version(), case.failure())
            .unwrap();
        assert!(matches!(
            repository
                .commit_scheduler_action(&fence, &replay)
                .await
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        assert_postgres_closed_world(&control, &run_id, case).await;
    }
    drop(repository);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
