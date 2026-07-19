use std::collections::BTreeMap;

use crate::{
    dsl::v3::{compile_source, CompileOptions},
    engine::{
        plan::{
            DescriptorConfigurationContract, DescriptorContract, DescriptorContractRegistry,
            DescriptorFieldContract, DescriptorValueSchema, LeafTaskKind, LinkedPlan, NodeKind,
            Plan, PortDirection, SubflowContractRegistry, VersionTag, WorkerContract,
            WorkerInputPortContract,
        },
        DefinitionRevisionId, DeploymentRevisionId, EffectEvidence, EffectIdempotency, RunId,
        RuntimeValue, SchedulerQuiescence, TaskExecutionResult, TransitionKey, TransitionOutcome,
        WorkerCancellation, WorkerEffectClass, WorkerEffectPolicy,
    },
};
use serde_json::json;
use sqlx::{
    postgres::PgPoolOptions, sqlite::SqliteConnectOptions, AssertSqlSafe, PgPool, SqlitePool,
};
use uuid::Uuid;

use super::{
    drive_scheduler_until_quiescent, ClaimSchedulerRunCommand, CreateRunCommand, DurableRepository,
    FencedSchedulerRunCommand, NoSchedulerCrash, PlanInstallOutcome, PostgresDurableRepository,
    SchedulerDurableRepository, SchedulerLeaseRepository, SchedulerRecoveryOutcome,
    SchedulerTaskClaim, SchedulerTaskCommitOutcome, SchedulerTaskFailure,
    SchedulerTaskHeartbeatOutcome, SchedulerTaskOutcome, SchedulerTaskSuccess,
    SqliteDurableRepository, VersionedPlan,
};

const DEADLINE_AGENT: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: answer
      type: action
      call: fixture.deadline
      response: string
    - return: $answer
"#;

fn key(label: &str, run_id: &RunId) -> TransitionKey {
    TransitionKey::derive(
        "scheduler.deadline.authority.test.v1",
        &[label, run_id.as_str()],
    )
    .unwrap()
}

fn deadline_fixture() -> (Plan, DescriptorContractRegistry, VersionedPlan) {
    let policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::ReadOnly,
        EffectIdempotency::Idempotent,
        1,
        0,
        0,
        2_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let plan = compile_source(
        DEADLINE_AGENT,
        CompileOptions::new(
            DefinitionRevisionId::new("scheduler_deadline_authority_v1").unwrap(),
            "scheduler-deadline-authority.yaml",
            DEADLINE_AGENT,
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
                )
                .with_effect_policy(policy.clone()),
            ))
            .unwrap();
    }
    let versioned = VersionedPlan::from_verified_plan(
        "scheduler-deadline-authority",
        "scheduler-deadline-agent",
        "Scheduler deadline authority fixture",
        DeploymentRevisionId::new("scheduler_deadline_authority_deployment_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "structured-v3"}),
        &plan,
        json!({"fixture": "descriptor-v1"}),
        json!({}),
        json!({"fixture": "worker-1"}),
    )
    .unwrap();
    (plan, descriptors, versioned)
}

fn success_for(claim: &SchedulerTaskClaim) -> SchedulerTaskOutcome {
    let output = claim
        .envelope()
        .request()
        .outputs()
        .first()
        .expect("deadline fixture output");
    SchedulerTaskOutcome::Succeeded(
        SchedulerTaskSuccess::inline(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("late-success")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
        .unwrap(),
    )
}

async fn prepare_sqlite_task(
    repository: &SqliteDurableRepository,
    control: &SqlitePool,
    versioned: &VersionedPlan,
    linked: &LinkedPlan<'_>,
    run_id: &RunId,
) -> SchedulerTaskClaim {
    assert!(matches!(
        repository
            .create_run(
                key("create", run_id),
                CreateRunCommand::new(run_id.clone(), versioned, json!({})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let owner = format!("scheduler-{}", run_id.as_str());
    let fencing_token = format!("fence-{}", run_id.as_str());
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
                    scheduler_lease_epoch=1,scheduler_lease_owner=?,
                    scheduler_fencing_token=?,
                    scheduler_lease_expires_at=datetime('now','+1 hour'),
                    scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND lifecycle='created'",
        )
        .bind(&owner)
        .bind(&fencing_token)
        .bind(run_id.as_str())
        .execute(control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    let fence = FencedSchedulerRunCommand::new(run_id.clone(), owner, 1, fencing_token).unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, linked, &fence, &NoSchedulerCrash, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    let claim = repository
        .claim_scheduler_tasks(&format!("worker-{}", run_id.as_str()), 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(matches!(
        repository
            .mark_scheduler_task_started(&claim)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    match repository
        .heartbeat_scheduler_task(&claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed) => renewed,
        other => panic!("fresh SQLite deadline claim did not renew: {other:?}"),
    }
}

async fn prepare_postgres_task(
    repository: &PostgresDurableRepository,
    control: &PgPool,
    versioned: &VersionedPlan,
    linked: &LinkedPlan<'_>,
    run_id: &RunId,
) -> SchedulerTaskClaim {
    assert!(matches!(
        repository
            .create_run(
                key("create", run_id),
                CreateRunCommand::new(run_id.clone(), versioned, json!({})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
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
    let lease = repository
        .claim_scheduler_run(
            key("scheduler-claim", run_id),
            ClaimSchedulerRunCommand::new(
                run_id.clone(),
                format!("scheduler-{}", run_id.as_str()),
                60,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap();
    let fence = lease.fence().unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, linked, &fence, &NoSchedulerCrash, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    let claim = repository
        .claim_scheduler_tasks(&format!("worker-{}", run_id.as_str()), 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(matches!(
        repository
            .mark_scheduler_task_started(&claim)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    match repository
        .heartbeat_scheduler_task(&claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed) => renewed,
        other => panic!("fresh PostgreSQL deadline claim did not renew: {other:?}"),
    }
}

async fn sqlite_snapshot(
    control: &SqlitePool,
    run_id: &RunId,
) -> (i64, i64, i64, i64, i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT o.projection_version,a.projection_version,v.projection_version,
                r.projection_version,
                (SELECT COUNT(*) FROM execution_events WHERE run_id=?),
                (SELECT COUNT(*) FROM scheduler_checkpoints WHERE run_id=?),
                (SELECT COUNT(*) FROM scheduler_values WHERE run_id=?),
                (SELECT COUNT(*) FROM public_event_outbox WHERE run_id=?)
         FROM task_outbox o
         JOIN node_attempts a ON a.run_id=o.run_id AND a.activation_id=o.activation_id
           AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
         JOIN node_activations v ON v.run_id=o.run_id AND v.activation_id=o.activation_id
         JOIN workflow_runs r ON r.run_id=o.run_id
         WHERE o.run_id=?",
    )
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap()
}

async fn postgres_snapshot(
    control: &PgPool,
    run_id: &RunId,
) -> (i64, i64, i64, i64, i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT o.projection_version,a.projection_version,v.projection_version,
                r.projection_version,
                (SELECT COUNT(*) FROM execution_events WHERE run_id=$1),
                (SELECT COUNT(*) FROM scheduler_checkpoints WHERE run_id=$1),
                (SELECT COUNT(*) FROM scheduler_values WHERE run_id=$1),
                (SELECT COUNT(*) FROM public_event_outbox WHERE run_id=$1)
         FROM task_outbox o
         JOIN node_attempts a ON a.run_id=o.run_id AND a.activation_id=o.activation_id
           AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
         JOIN node_activations v ON v.run_id=o.run_id AND v.activation_id=o.activation_id
         JOIN workflow_runs r ON r.run_id=o.run_id
         WHERE o.run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap()
}

#[tokio::test]
async fn sqlite_deadline_authority_rejects_premature_and_lost_leases_and_commits_only_db_authorized_timeout(
) {
    let (plan, descriptors, versioned) = deadline_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("scheduler-deadline-authority.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();

    let premature_run = RunId::new("run_sqlite_deadline_premature").unwrap();
    let premature =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &premature_run).await;
    let premature_snapshot = sqlite_snapshot(&control, &premature_run).await;
    let premature_timeout = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_runtime_deadline(&premature).unwrap(),
    );
    assert!(repository
        .commit_scheduler_task_outcome(&premature, &premature_timeout)
        .await
        .is_err());
    assert_eq!(
        sqlite_snapshot(&control, &premature_run).await,
        premature_snapshot,
        "a private timeout before the database deadline must change no authority",
    );

    let late_run = RunId::new("run_sqlite_deadline_late_success").unwrap();
    let late = prepare_sqlite_task(&repository, &control, &versioned, &linked, &late_run).await;
    let lost_run = RunId::new("run_sqlite_deadline_lease_lost").unwrap();
    let lost = prepare_sqlite_task(&repository, &control, &versioned, &linked, &lost_run).await;
    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;

    let before_late = sqlite_snapshot(&control, &late_run).await;
    assert_eq!(
        repository
            .commit_scheduler_task_outcome(&late, &success_for(&late))
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::OperationDeadlineElapsed,
    );
    assert_eq!(
        sqlite_snapshot(&control, &late_run).await,
        before_late,
        "ordinary late success must roll back event, checkpoint, value, public event, and projections",
    );
    let authorized = match repository
        .heartbeat_scheduler_task(&late, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(renewed) => renewed,
        other => panic!("database did not authorize the elapsed deadline: {other:?}"),
    };
    let authorized_timeout = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_runtime_deadline(&authorized).unwrap(),
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&authorized, &authorized_timeout)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=? AND attempt_no=1",
        )
        .bind(late_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "timed_out",
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scheduler_values WHERE run_id=?")
            .bind(late_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
    );

    sqlx::query(
        "UPDATE task_outbox SET claim_expires_at=datetime('now','-1 second') WHERE run_id=?",
    )
    .bind(lost_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE node_attempts SET lease_expires_at=datetime('now','-1 second') WHERE run_id=?",
    )
    .bind(lost_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .heartbeat_scheduler_task(&lost, 60)
            .await
            .unwrap(),
        SchedulerTaskHeartbeatOutcome::LeaseLost,
    );
    let lost_snapshot = sqlite_snapshot(&control, &lost_run).await;
    let unauthorized_timeout =
        SchedulerTaskOutcome::Failed(SchedulerTaskFailure::from_runtime_deadline(&lost).unwrap());
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&lost, &unauthorized_timeout)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict | SchedulerTaskCommitOutcome::StaleLease
    ));
    assert_eq!(sqlite_snapshot(&control, &lost_run).await, lost_snapshot);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=? AND attempt_no=1",
        )
        .bind(lost_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "running",
    );
}

#[tokio::test]
async fn postgres_deadline_authority_matches_sqlite_contract() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("scheduler_deadline_v3_{}", Uuid::new_v4().simple());
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
    let (plan, descriptors, versioned) = deadline_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    let premature_run = RunId::new("run_pg_deadline_premature").unwrap();
    let premature =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &premature_run).await;
    let premature_snapshot = postgres_snapshot(&control, &premature_run).await;
    let premature_timeout = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_runtime_deadline(&premature).unwrap(),
    );
    assert!(repository
        .commit_scheduler_task_outcome(&premature, &premature_timeout)
        .await
        .is_err());
    assert_eq!(
        postgres_snapshot(&control, &premature_run).await,
        premature_snapshot,
    );

    let late_run = RunId::new("run_pg_deadline_late_success").unwrap();
    let late = prepare_postgres_task(&repository, &control, &versioned, &linked, &late_run).await;
    let lost_run = RunId::new("run_pg_deadline_lease_lost").unwrap();
    let lost = prepare_postgres_task(&repository, &control, &versioned, &linked, &lost_run).await;
    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;

    let before_late = postgres_snapshot(&control, &late_run).await;
    assert_eq!(
        repository
            .commit_scheduler_task_outcome(&late, &success_for(&late))
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::OperationDeadlineElapsed,
    );
    assert_eq!(postgres_snapshot(&control, &late_run).await, before_late,);
    let authorized = match repository
        .heartbeat_scheduler_task(&late, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(renewed) => renewed,
        other => panic!("database did not authorize the elapsed deadline: {other:?}"),
    };
    let authorized_timeout = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_runtime_deadline(&authorized).unwrap(),
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&authorized, &authorized_timeout)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=$1 AND attempt_no=1",
        )
        .bind(late_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "timed_out",
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scheduler_values WHERE run_id=$1")
            .bind(late_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
    );

    let expired_at = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        "SELECT clock_timestamp()-INTERVAL '1 second'",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query("UPDATE task_outbox SET claim_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(lost_run.as_str())
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("UPDATE node_attempts SET lease_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(lost_run.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert_eq!(
        repository
            .heartbeat_scheduler_task(&lost, 60)
            .await
            .unwrap(),
        SchedulerTaskHeartbeatOutcome::LeaseLost,
    );
    let lost_snapshot = postgres_snapshot(&control, &lost_run).await;
    let unauthorized_timeout =
        SchedulerTaskOutcome::Failed(SchedulerTaskFailure::from_runtime_deadline(&lost).unwrap());
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&lost, &unauthorized_timeout)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict | SchedulerTaskCommitOutcome::StaleLease
    ));
    assert_eq!(postgres_snapshot(&control, &lost_run).await, lost_snapshot);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=$1 AND attempt_no=1",
        )
        .bind(lost_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "running",
    );

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}
