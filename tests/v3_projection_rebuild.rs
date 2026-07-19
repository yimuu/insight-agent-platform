//! Full projection repair coverage for scheduler-owned materializations.
//!
//! The immutable checkpoint ledger is the value authority. SQL identity and
//! writable columns remain repository-owned through a closed registry.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use insight_agent_platform::{
    dsl::v3::{compile_source, CompileOptions},
    engine::{
        plan::{
            DescriptorConfigurationContract, DescriptorContract, DescriptorContractRegistry,
            DescriptorFieldContract, DescriptorValueSchema, LeafTaskKind, LinkedPlan, NodeKind,
            Plan, PortDirection, SubflowContractRegistry, VersionTag, WorkerContract,
            WorkerInputPortContract,
        },
        repository::{
            consume_scheduler_task_once, drive_scheduler_once, CreateRunCommand, DurableRepository,
            FencedSchedulerRunCommand, NoSchedulerCrash, PlanInstallOutcome,
            PostgresDurableRepository, ProjectionAudit, ProjectionDurableRepository,
            ProjectionRepairReceipt, ProjectionSubject, ProjectionSubjectKind,
            SchedulerDriveOutcome, SchedulerDurableRepository, SqliteDurableRepository,
            TerminalSchedulerWorkerFailurePolicy, VersionedPlan, REPOSITORY_DATA_INVALID,
        },
        DefinitionRevisionId, DeploymentRevisionId, EffectEvidence, LeafTaskExecutor, RunId,
        RuntimeValue, SchedulerQuiescence, SchedulerTaskKind, TaskExecutionRequest,
        TaskExecutionResult, TransitionKey, TransitionOutcome, WorkerExecutionContext,
        WorkerExecutorRegistry, WorkerFailure,
    },
};
use serde_json::json;
use sqlx::{
    postgres::PgPoolOptions, sqlite::SqliteConnectOptions, AssertSqlSafe, PgPool, SqlitePool,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const PARALLEL_AGENT: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {question: string}
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

fn key(label: &str, run_id: &RunId) -> TransitionKey {
    TransitionKey::derive("projection.rebuild.e2e.v1", &[label, run_id.as_str()]).unwrap()
}

fn fixture() -> (Plan, DescriptorContractRegistry) {
    let plan = compile_source(
        PARALLEL_AGENT,
        CompileOptions::new(
            DefinitionRevisionId::new("projection_rebuild_v1").unwrap(),
            "projection-rebuild.yaml",
            PARALLEL_AGENT,
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
            BTreeMap::new(),
        );
        descriptors
            .register(DescriptorContract::new(
                descriptor.implementation.clone(),
                descriptor.descriptor_version.clone(),
                configuration,
                WorkerContract::new(
                    LeafTaskKind::Action,
                    VersionTag::new("worker-1").unwrap(),
                    inputs,
                    outputs,
                ),
            ))
            .unwrap();
    }
    (plan, descriptors)
}

fn versioned(plan: &Plan) -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        "projection-rebuild",
        "projection-rebuild-agent",
        "Projection rebuild fixture",
        DeploymentRevisionId::new("projection_rebuild_deployment_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "structured-v3"}),
        plan,
        json!({}),
        json!({}),
        json!({"worker": "worker-1"}),
    )
    .unwrap()
}

#[derive(Clone)]
struct AnswerExecutor;

#[async_trait]
impl LeafTaskExecutor for AnswerExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        Ok(TaskExecutionResult::new(
            request
                .outputs()
                .iter()
                .map(|output| {
                    (
                        output.port_id().clone(),
                        RuntimeValue::new(json!(request.implementation())).unwrap(),
                    )
                })
                .collect(),
            EffectEvidence::Committed,
        ))
    }
}

fn workers() -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    let executor: Arc<dyn LeafTaskExecutor> = Arc::new(AnswerExecutor);
    for implementation in ["fixture.technical", "fixture.risk"] {
        workers
            .register(
                SchedulerTaskKind::Action,
                implementation,
                VersionTag::new("1").unwrap(),
                VersionTag::new("worker-1").unwrap(),
                executor.clone(),
            )
            .unwrap();
    }
    workers
}

async fn create_run<R: DurableRepository + ?Sized>(
    repository: &R,
    deployed: &VersionedPlan,
    run_id: &RunId,
) {
    assert_eq!(
        repository.install_versioned_plan(deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    assert!(matches!(
        repository
            .create_run(
                key("create", run_id),
                CreateRunCommand::new(run_id.clone(), deployed, json!({"question": "why"}))
                    .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
}

async fn drive_to_success<R>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
) where
    R: SchedulerDurableRepository + ?Sized,
{
    let workers = workers();
    for _ in 0..256 {
        match drive_scheduler_once(repository, linked, fence, &NoSchedulerCrash)
            .await
            .unwrap()
        {
            SchedulerDriveOutcome::Applied(_) => {}
            SchedulerDriveOutcome::Quiescent(SchedulerQuiescence::RunSucceeded) => return,
            SchedulerDriveOutcome::Quiescent(
                SchedulerQuiescence::WaitingForTask { .. }
                | SchedulerQuiescence::WaitingForChildren { .. },
            ) => {
                consume_scheduler_task_once(
                    repository,
                    &workers,
                    &TerminalSchedulerWorkerFailurePolicy,
                    "projection-repair-worker",
                    60,
                    64,
                    CancellationToken::new(),
                    &NoSchedulerCrash,
                )
                .await
                .unwrap();
            }
            outcome => panic!("unexpected projection fixture outcome: {outcome:?}"),
        }
    }
    panic!("projection fixture exhausted scheduler budget")
}

async fn sqlite_repository() -> (tempfile::TempDir, SqliteDurableRepository, SqlitePool) {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("projection-rebuild.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    (directory, repository, control)
}

async fn activate_sqlite(control: &SqlitePool, run_id: &RunId) -> FencedSchedulerRunCommand {
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
            scheduler_lease_epoch=1,scheduler_lease_owner='projection-repair',
            scheduler_fencing_token='projection-repair-fence',
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .execute(control)
    .await
    .unwrap();
    FencedSchedulerRunCommand::new(
        run_id.clone(),
        "projection-repair",
        1,
        "projection-repair-fence",
    )
    .unwrap()
}

#[tokio::test]
async fn sqlite_repairs_scheduler_data_control_fork_and_join_projections() {
    let (_directory, repository, control) = sqlite_repository().await;
    let (plan, descriptors) = fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let deployed = versioned(&plan);
    let run_id = RunId::new("run_sqlite_projection_full_repair").unwrap();
    create_run(&repository, &deployed, &run_id).await;
    let fence = activate_sqlite(&control, &run_id).await;
    drive_to_success(&repository, &linked, &fence).await;

    let task_authority_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_outbox WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    assert!(task_authority_count > 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM projection_checkpoints
             WHERE run_id=? AND subject_kind IN ('signal','task_outbox','human_work_item')",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0
    );
    sqlx::query("UPDATE task_outbox SET last_error_code='authority-marker' WHERE run_id=?")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    repository.repair_all_projections(&run_id).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_outbox WHERE run_id=? AND last_error_code='authority-marker'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        task_authority_count,
        "repair must not update task delivery authorities"
    );

    let scheduler_id: String = sqlx::query_scalar(
        "SELECT checkpoint_id FROM scheduler_checkpoints WHERE run_id=? ORDER BY created_at LIMIT 1",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let data_id: String = sqlx::query_scalar(
        "SELECT port_id FROM scheduler_values WHERE run_id=? ORDER BY port_id LIMIT 1",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let control_id: String = sqlx::query_scalar(
        "SELECT 'token:' || token_id FROM control_tokens WHERE run_id=? ORDER BY token_id LIMIT 1",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let fork_id: String = sqlx::query_scalar(
        "SELECT 'group:' || fork_group_id FROM fork_groups WHERE run_id=? ORDER BY fork_group_id LIMIT 1",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let join_id: String = sqlx::query_scalar(
        "SELECT 'arrival:' || lower(hex(CAST(join_activation_id AS BLOB))) || ':' ||
                lower(hex(CAST(fork_group_id AS BLOB))) || ':' ||
                lower(hex(CAST(leg_id AS BLOB))) FROM join_arrivals
         WHERE run_id=? ORDER BY fork_group_id,leg_id LIMIT 1",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();

    let cases = [
        (
            ProjectionSubject::new(ProjectionSubjectKind::Scheduler, scheduler_id).unwrap(),
            "UPDATE scheduler_checkpoints SET scheduler_projection_version=scheduler_projection_version+100 WHERE run_id=?",
        ),
        (
            ProjectionSubject::new(ProjectionSubjectKind::DataValue, data_id).unwrap(),
            "UPDATE scheduler_values SET runtime_value=json('\"corrupt\"') WHERE run_id=?",
        ),
        (
            ProjectionSubject::new(ProjectionSubjectKind::Control, control_id).unwrap(),
            "UPDATE control_tokens SET current_port_id='corrupt_port' WHERE run_id=?",
        ),
        (
            ProjectionSubject::new(ProjectionSubjectKind::Fork, fork_id).unwrap(),
            "UPDATE fork_groups SET projection_version=projection_version+100 WHERE run_id=?",
        ),
        (
            ProjectionSubject::new(ProjectionSubjectKind::Join, join_id).unwrap(),
            "UPDATE join_arrivals SET settlement_class='safe_failure' WHERE run_id=?",
        ),
    ];

    for (subject, corruption) in &cases {
        assert!(repository
            .audit_projection(&run_id, subject)
            .await
            .unwrap()
            .is_match());
        sqlx::query(*corruption)
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap();
        assert!(matches!(
            repository.audit_projection(&run_id, subject).await.unwrap(),
            ProjectionAudit::Mismatch { .. }
        ));
        assert!(repository
            .repair_projection(&run_id, subject)
            .await
            .unwrap()
            .repaired());
        assert!(repository
            .audit_projection(&run_id, subject)
            .await
            .unwrap()
            .is_match());
    }

    // Checkpoint tables are disposable indexes: remove them, corrupt the
    // materialized projection again, and rebuild from execution_events alone.
    sqlx::query("DELETE FROM projection_checkpoints WHERE run_id=?")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("DELETE FROM projection_checkpoint_batches WHERE run_id=?")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    let mut fault = control.acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys=OFF")
        .execute(&mut *fault)
        .await
        .unwrap();
    let deletes = [
        "DELETE FROM scheduler_checkpoints WHERE run_id=? AND checkpoint_id=?",
        "DELETE FROM scheduler_values WHERE run_id=? AND port_id=?",
        "DELETE FROM control_tokens WHERE run_id=? AND ('token:' || token_id)=?",
        "DELETE FROM fork_groups WHERE run_id=? AND ('group:' || fork_group_id)=?",
        "DELETE FROM join_arrivals WHERE run_id=? AND
           ('arrival:' || lower(hex(CAST(join_activation_id AS BLOB))) || ':' ||
            lower(hex(CAST(fork_group_id AS BLOB))) || ':' ||
            lower(hex(CAST(leg_id AS BLOB))))=?",
    ];
    for ((subject, _), delete) in cases.iter().zip(deletes) {
        assert_eq!(
            sqlx::query(delete)
                .bind(run_id.as_str())
                .bind(subject.subject_id())
                .execute(&mut *fault)
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        assert!(repository
            .repair_projection(&run_id, subject)
            .await
            .unwrap()
            .repaired());
        assert!(repository
            .audit_projection(&run_id, subject)
            .await
            .unwrap()
            .is_match());
    }

    // A whole rebuildable graph can be restored without callers guessing a
    // subject order. Task delivery authority is intentionally deleted in the
    // same fault but remains absent after repair.
    for table in [
        "join_arrivals",
        "fork_legs",
        "control_tokens",
        "fork_groups",
        "task_outbox",
        "scheduler_occurrence_values",
        "scheduler_values",
        "scheduler_checkpoints",
        "node_attempts",
        "node_activations",
        "scope_instances",
        "workflow_runs",
    ] {
        let query = format!("DELETE FROM {table} WHERE run_id=?");
        sqlx::query(AssertSqlSafe(query))
            .bind(run_id.as_str())
            .execute(&mut *fault)
            .await
            .unwrap();
    }
    drop(fault);
    let receipts = repository.repair_all_projections(&run_id).await.unwrap();
    assert!(receipts.iter().any(ProjectionRepairReceipt::repaired));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_outbox WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
        "repair must not recreate a deleted task delivery authority"
    );
    for subject in cases.iter().map(|(subject, _)| subject) {
        assert!(repository
            .audit_projection(&run_id, subject)
            .await
            .unwrap()
            .is_match());
    }

    // The ledger envelope is write-once. For the corruption-path test, remove
    // only the guard as an explicit fault injection and forge its manifest.
    assert!(sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=json_set(projection_ledger_batch,'$.manifest_hash','forged')
         WHERE run_id=? AND projection_ledger_batch IS NOT NULL",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .is_err());
    sqlx::query("DROP TRIGGER execution_event_projection_ledger_immutable")
        .execute(&control)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE execution_events SET projection_ledger_batch=json_set(projection_ledger_batch,'$.manifest_hash','forged')
         WHERE run_id=? AND event_id=(
           SELECT e.event_id FROM execution_events e, json_each(e.projection_ledger_batch,'$.subjects') s
           WHERE e.run_id=? AND json_extract(s.value,'$.subject_kind')='scheduler'
             AND json_extract(s.value,'$.subject_id')=?
           ORDER BY e.seq DESC LIMIT 1
         )",
    )
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(cases[0].0.subject_id())
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .repair_projection(&run_id, &cases[0].0)
            .await
            .unwrap_err()
            .code(),
        REPOSITORY_DATA_INVALID
    );
}

async fn postgres_repository() -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("V3_TEST_POSTGRES_URL").ok()?;
    let schema = format!("projection_rebuild_{}", Uuid::new_v4().simple());
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
        .max_connections(8)
        .connect(&scoped_url)
        .await
        .unwrap();
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    Some((repository, control, admin, schema))
}

async fn activate_postgres(control: &PgPool, run_id: &RunId) -> FencedSchedulerRunCommand {
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
            scheduler_lease_epoch=1,scheduler_lease_owner='projection-repair',
            scheduler_fencing_token='projection-repair-fence',
            scheduler_lease_expires_at=CURRENT_TIMESTAMP + INTERVAL '1 hour',
            scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .execute(control)
    .await
    .unwrap();
    FencedSchedulerRunCommand::new(
        run_id.clone(),
        "projection-repair",
        1,
        "projection-repair-fence",
    )
    .unwrap()
}

#[tokio::test]
async fn postgres_projection_repair_serializes_concurrent_writers() {
    let Some((repository, control, admin, schema)) = postgres_repository().await else {
        return;
    };
    let (plan, descriptors) = fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let deployed = versioned(&plan);
    let run_id = RunId::new("run_postgres_projection_concurrent_repair").unwrap();
    create_run(&repository, &deployed, &run_id).await;
    let fence = activate_postgres(&control, &run_id).await;
    drive_to_success(&repository, &linked, &fence).await;

    let task_authority_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_outbox WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    assert!(task_authority_count > 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM projection_checkpoints
             WHERE run_id=$1 AND subject_kind IN ('signal','task_outbox','human_work_item')",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0
    );
    sqlx::query("UPDATE task_outbox SET last_error_code='authority-marker' WHERE run_id=$1")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    repository.repair_all_projections(&run_id).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_outbox WHERE run_id=$1 AND last_error_code='authority-marker'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        task_authority_count,
        "repair must not update task delivery authorities"
    );

    let subject_id: String = sqlx::query_scalar(
        "SELECT 'group:' || fork_group_id FROM fork_groups WHERE run_id=$1 ORDER BY fork_group_id LIMIT 1",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let subject = ProjectionSubject::new(ProjectionSubjectKind::Fork, subject_id).unwrap();
    sqlx::query("UPDATE fork_groups SET projection_version=projection_version+100 WHERE run_id=$1")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(matches!(
        repository
            .audit_projection(&run_id, &subject)
            .await
            .unwrap(),
        ProjectionAudit::Mismatch { .. }
    ));

    let left_repository = repository.clone();
    let right_repository = repository.clone();
    let left_run = run_id.clone();
    let right_run = run_id.clone();
    let left_subject = subject.clone();
    let right_subject = subject.clone();
    let (left, right) = tokio::join!(
        left_repository.repair_projection(&left_run, &left_subject),
        right_repository.repair_projection(&right_run, &right_subject),
    );
    let repaired = u8::from(left.unwrap().repaired()) + u8::from(right.unwrap().repaired());
    assert_eq!(repaired, 1);
    assert!(repository
        .audit_projection(&run_id, &subject)
        .await
        .unwrap()
        .is_match());

    sqlx::query("DELETE FROM projection_checkpoints WHERE run_id=$1")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("DELETE FROM projection_checkpoint_batches WHERE run_id=$1")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("UPDATE fork_groups SET projection_version=projection_version+100 WHERE run_id=$1")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(repository
        .repair_projection(&run_id, &subject)
        .await
        .unwrap()
        .repaired());
    assert!(repository
        .audit_projection(&run_id, &subject)
        .await
        .unwrap()
        .is_match());

    let missing_subject_id: String = sqlx::query_scalar(
        "SELECT 'arrival:' || encode(convert_to(join_activation_id,'UTF8'),'hex') || ':' ||
                encode(convert_to(fork_group_id,'UTF8'),'hex') || ':' ||
                encode(convert_to(leg_id,'UTF8'),'hex')
         FROM join_arrivals WHERE run_id=$1 ORDER BY fork_group_id,leg_id LIMIT 1",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let missing_subject =
        ProjectionSubject::new(ProjectionSubjectKind::Join, missing_subject_id).unwrap();
    assert_eq!(
        sqlx::query(
            "DELETE FROM join_arrivals WHERE run_id=$1 AND
               ('arrival:' || encode(convert_to(join_activation_id,'UTF8'),'hex') || ':' ||
                encode(convert_to(fork_group_id,'UTF8'),'hex') || ':' ||
                encode(convert_to(leg_id,'UTF8'),'hex'))=$2",
        )
        .bind(run_id.as_str())
        .bind(missing_subject.subject_id())
        .execute(&control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    let left_repository = repository.clone();
    let right_repository = repository.clone();
    let left_run = run_id.clone();
    let right_run = run_id.clone();
    let left_subject = missing_subject.clone();
    let right_subject = missing_subject.clone();
    let (left, right) = tokio::join!(
        left_repository.repair_projection(&left_run, &left_subject),
        right_repository.repair_projection(&right_run, &right_subject),
    );
    let repaired = u8::from(left.unwrap().repaired()) + u8::from(right.unwrap().repaired());
    assert_eq!(repaired, 1);
    assert!(repository
        .audit_projection(&run_id, &missing_subject)
        .await
        .unwrap()
        .is_match());

    assert_eq!(
        sqlx::query("DELETE FROM task_outbox WHERE run_id=$1")
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap()
            .rows_affected(),
        u64::try_from(task_authority_count).unwrap()
    );
    repository.repair_all_projections(&run_id).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_outbox WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
        "repair must not recreate a deleted task delivery authority"
    );

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}
