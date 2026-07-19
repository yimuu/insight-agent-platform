use std::collections::BTreeMap;

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
            drive_scheduler_until_quiescent, CreateRunCommand, DurableRepository,
            FencedSchedulerRunCommand, FrozenSchedulerWorkerFailurePolicy, NoSchedulerCrash,
            PlanInstallOutcome, PostgresDurableRepository, SchedulerDurableRepository,
            SchedulerRecoveryOutcome, SchedulerTaskCommitOutcome, SchedulerTaskHeartbeatOutcome,
            SchedulerTaskOutcome, SchedulerTaskSuccess, SchedulerWorkerFailurePolicy,
            SqliteDurableRepository, VersionedPlan,
        },
        DefinitionRevisionId, DeploymentRevisionId, EffectEvidence, RunId, RuntimeValue,
        SchedulerQuiescence, TaskAdmissionClass, TaskExecutionResult, TransitionKey,
        TransitionOutcome, WorkerFailure, WorkerFailureClass,
    },
};
use serde_json::json;
use sqlx::{
    postgres::PgPoolOptions, sqlite::SqliteConnectOptions, AssertSqlSafe, PgPool, SqlitePool,
};
use uuid::Uuid;

const FINALIZER_ACTION_AGENT: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - id: protected_task
          type: action
          call: fixture.protected
          response: string
      catch:
        safe_business_failure:
          as: failure
          steps:
            - return: caught
      finally:
        - id: final_task
          type: action
          call: fixture.finalizer
          response: string
    - return: done
"#;

fn key(label: &str, run_id: &RunId) -> TransitionKey {
    TransitionKey::derive(
        "scheduler.task.admission.integration.v1",
        &[label, run_id.as_str()],
    )
    .unwrap()
}

fn finalizer_plan() -> (Plan, DescriptorContractRegistry) {
    let plan = compile_source(
        FINALIZER_ACTION_AGENT,
        CompileOptions::new(
            DefinitionRevisionId::new("task_admission_finalizer_v1").unwrap(),
            "task-admission-finalizer.yaml",
            FINALIZER_ACTION_AGENT,
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
                ),
            ))
            .unwrap();
    }
    (plan, descriptors)
}

fn versioned(plan: &Plan, deployment: &str) -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        "task-admission-finalizer",
        "task-admission-agent",
        "Termination task admission fixture",
        DeploymentRevisionId::new(deployment).unwrap(),
        "expression-3.0.0",
        json!({"format": "structured-v3"}),
        plan,
        json!({"fixture": "descriptor-v1"}),
        json!({}),
        json!({"fixture": "worker-1"}),
    )
    .unwrap()
}

async fn wait_for_task<R: SchedulerDurableRepository + ?Sized>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
) {
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, linked, fence, &NoSchedulerCrash, 64)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
}

fn success_for(
    claim: &insight_agent_platform::engine::repository::SchedulerTaskClaim,
) -> SchedulerTaskOutcome {
    let output = claim
        .envelope()
        .request()
        .outputs()
        .first()
        .expect("fixture action output");
    SchedulerTaskOutcome::Succeeded(
        SchedulerTaskSuccess::inline(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("cleanup-complete")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
        .unwrap(),
    )
}

fn retry_outcome(
    claim: &insight_agent_platform::engine::repository::SchedulerTaskClaim,
) -> SchedulerTaskOutcome {
    let failure = WorkerFailure::new(
        WorkerFailureClass::InfrastructureFailure,
        "TRANSIENT_PROVIDER_FAILURE",
        true,
    )
    .unwrap();
    SchedulerTaskOutcome::Failed(
        FrozenSchedulerWorkerFailurePolicy
            .freeze(claim, &failure)
            .unwrap(),
    )
}

#[tokio::test]
async fn sqlite_termination_closes_normal_tasks_but_executes_structural_finalizer_tasks() {
    let (plan, descriptors) = finalizer_plan();
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let deployed = versioned(&plan, "task_admission_sqlite_v1");
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("task-admission.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();

    let run_id = RunId::new("run_task_admission_sqlite_finalizer").unwrap();
    assert!(matches!(
        repository
            .create_run(
                key("create-finalizer", &run_id),
                CreateRunCommand::new(run_id.clone(), &deployed, json!({})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='scheduler-admission',scheduler_fencing_token='fence-admission',
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    let fence =
        FencedSchedulerRunCommand::new(run_id.clone(), "scheduler-admission", 1, "fence-admission")
            .unwrap();
    wait_for_task(&repository, &linked, &fence).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT json_extract(task_envelope,'$.request.admission_class')
             FROM task_outbox WHERE run_id=? AND task_state='pending'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "normal"
    );
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
            termination_intent_reason='cancelled',termination_intent_transition_key=?,
            termination_intent_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(key("cancel-finalizer", &run_id).as_str())
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(repository
        .claim_scheduler_tasks("worker-must-not-claim-normal", 60, 8)
        .await
        .unwrap()
        .is_empty());

    wait_for_task(&repository, &linked, &fence).await;
    let states = sqlx::query_as::<_, (String, String)>(
        "SELECT task_state,json_extract(task_envelope,'$.request.admission_class')
         FROM task_outbox WHERE run_id=?
         ORDER BY json_extract(task_envelope,'$.request.admission_class')",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(
        states,
        vec![
            ("dead".into(), "normal".into()),
            ("pending".into(), "termination_finalizer".into())
        ]
    );
    let finalizer = repository
        .claim_scheduler_tasks("worker-finalizer", 60, 8)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        finalizer.envelope().request().admission_class(),
        TaskAdmissionClass::TerminationFinalizer
    );
    assert!(matches!(
        repository
            .mark_scheduler_task_started(&finalizer)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let finalizer = match repository
        .heartbeat_scheduler_task(&finalizer, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(value) => value,
        SchedulerTaskHeartbeatOutcome::LeaseLost
        | SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(_) => {
            panic!("finalizer lost terminating authority")
        }
    };
    let finalizer_outcome = success_for(&finalizer);
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&finalizer, &finalizer_outcome)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let output_port = finalizer.envelope().request().outputs()[0]
        .port_id()
        .clone();
    let original_runtime = sqlx::query_scalar::<_, String>(
        "SELECT runtime_value FROM scheduler_values WHERE run_id=? AND port_id=?",
    )
    .bind(run_id.as_str())
    .bind(output_port.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query("UPDATE scheduler_values SET runtime_value=? WHERE run_id=? AND port_id=?")
        .bind(serde_json::to_string(&RuntimeValue::new(json!("forged")).unwrap()).unwrap())
        .bind(run_id.as_str())
        .bind(output_port.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(repository
        .commit_scheduler_task_outcome(&finalizer, &finalizer_outcome)
        .await
        .is_err());
    sqlx::query("UPDATE scheduler_values SET runtime_value=? WHERE run_id=? AND port_id=?")
        .bind(original_runtime)
        .bind(run_id.as_str())
        .bind(output_port.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunCancelled)
    ));

    let guarded_run = RunId::new("run_task_admission_sqlite_claim_guard").unwrap();
    repository
        .create_run(
            key("create-guard", &guarded_run),
            CreateRunCommand::new(guarded_run.clone(), &deployed, json!({})).unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='scheduler-guard',scheduler_fencing_token='fence-guard',
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(guarded_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    let guarded_fence =
        FencedSchedulerRunCommand::new(guarded_run.clone(), "scheduler-guard", 1, "fence-guard")
            .unwrap();
    wait_for_task(&repository, &linked, &guarded_fence).await;
    let normal_claim = repository
        .claim_scheduler_tasks("worker-normal-snapshot", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        normal_claim.envelope().request().admission_class(),
        TaskAdmissionClass::Normal
    );
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
            termination_intent_reason='cancelled',termination_intent_transition_key=?,
            termination_intent_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(key("cancel-guard", &guarded_run).as_str())
    .bind(guarded_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_outbox SET task_envelope=json_set(task_envelope,
            '$.request.admission_class','termination_finalizer')
         WHERE run_id=? AND task_id=?",
    )
    .bind(guarded_run.as_str())
    .bind(normal_claim.task_id().as_str())
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .mark_scheduler_task_started(&normal_claim)
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    assert_eq!(
        repository
            .heartbeat_scheduler_task(&normal_claim, 60)
            .await
            .unwrap(),
        SchedulerTaskHeartbeatOutcome::LeaseLost
    );
    assert_eq!(
        repository
            .commit_scheduler_task_outcome(&normal_claim, &retry_outcome(&normal_claim))
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_attempts WHERE run_id=? AND activation_id=?",
        )
        .bind(guarded_run.as_str())
        .bind(normal_claim.activation_id().as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
}

async fn isolated_postgres() -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("V3_TEST_POSTGRES_URL").ok()?;
    let schema = format!("task_admission_{}", Uuid::new_v4().simple());
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

#[tokio::test]
async fn postgres_termination_closes_normal_tasks_but_executes_structural_finalizer_tasks() {
    let Some((repository, control, admin, schema)) = isolated_postgres().await else {
        return;
    };
    let (plan, descriptors) = finalizer_plan();
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let deployed = versioned(&plan, "task_admission_postgres_v1");
    repository.install_versioned_plan(&deployed).await.unwrap();
    let run_id = RunId::new("run_task_admission_postgres_finalizer").unwrap();
    repository
        .create_run(
            key("create-finalizer", &run_id),
            CreateRunCommand::new(run_id.clone(), &deployed, json!({})).unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='scheduler-admission',scheduler_fencing_token='fence-admission',
            scheduler_lease_expires_at=CURRENT_TIMESTAMP+INTERVAL '1 hour',
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    let fence =
        FencedSchedulerRunCommand::new(run_id.clone(), "scheduler-admission", 1, "fence-admission")
            .unwrap();
    wait_for_task(&repository, &linked, &fence).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT task_envelope #>> '{request,admission_class}'
             FROM task_outbox WHERE run_id=$1 AND task_state='pending'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "normal"
    );
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
            termination_intent_reason='cancelled',termination_intent_transition_key=$1,
            termination_intent_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP WHERE run_id=$2",
    )
    .bind(key("cancel-finalizer", &run_id).as_str())
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(repository
        .claim_scheduler_tasks("worker-must-not-claim-normal", 60, 8)
        .await
        .unwrap()
        .is_empty());
    wait_for_task(&repository, &linked, &fence).await;
    let states = sqlx::query_as::<_, (String, String)>(
        "SELECT task_state,task_envelope #>> '{request,admission_class}'
         FROM task_outbox WHERE run_id=$1
         ORDER BY task_envelope #>> '{request,admission_class}'",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(
        states,
        vec![
            ("dead".into(), "normal".into()),
            ("pending".into(), "termination_finalizer".into())
        ]
    );
    let finalizer = repository
        .claim_scheduler_tasks("worker-finalizer", 60, 8)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        finalizer.envelope().request().admission_class(),
        TaskAdmissionClass::TerminationFinalizer
    );
    assert!(matches!(
        repository
            .mark_scheduler_task_started(&finalizer)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let finalizer = match repository
        .heartbeat_scheduler_task(&finalizer, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(value) => value,
        SchedulerTaskHeartbeatOutcome::LeaseLost
        | SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(_) => {
            panic!("finalizer lost terminating authority")
        }
    };
    let finalizer_outcome = success_for(&finalizer);
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&finalizer, &finalizer_outcome)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let output_port = finalizer.envelope().request().outputs()[0]
        .port_id()
        .clone();
    let original_runtime = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT runtime_value FROM scheduler_values WHERE run_id=$1 AND port_id=$2",
    )
    .bind(run_id.as_str())
    .bind(output_port.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query("UPDATE scheduler_values SET runtime_value=$1 WHERE run_id=$2 AND port_id=$3")
        .bind(serde_json::to_value(RuntimeValue::new(json!("forged")).unwrap()).unwrap())
        .bind(run_id.as_str())
        .bind(output_port.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(repository
        .commit_scheduler_task_outcome(&finalizer, &finalizer_outcome)
        .await
        .is_err());
    sqlx::query("UPDATE scheduler_values SET runtime_value=$1 WHERE run_id=$2 AND port_id=$3")
        .bind(original_runtime)
        .bind(run_id.as_str())
        .bind(output_port.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunCancelled)
    ));

    let guarded_run = RunId::new("run_task_admission_postgres_claim_guard").unwrap();
    repository
        .create_run(
            key("create-guard", &guarded_run),
            CreateRunCommand::new(guarded_run.clone(), &deployed, json!({})).unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='scheduler-guard',scheduler_fencing_token='fence-guard',
            scheduler_lease_expires_at=CURRENT_TIMESTAMP+INTERVAL '1 hour',
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=$1",
    )
    .bind(guarded_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    let guarded_fence =
        FencedSchedulerRunCommand::new(guarded_run.clone(), "scheduler-guard", 1, "fence-guard")
            .unwrap();
    wait_for_task(&repository, &linked, &guarded_fence).await;
    let normal_claim = repository
        .claim_scheduler_tasks("worker-normal-snapshot", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
            termination_intent_reason='cancelled',termination_intent_transition_key=$1,
            termination_intent_at=CURRENT_TIMESTAMP,projection_version=projection_version+1,
            updated_at=CURRENT_TIMESTAMP WHERE run_id=$2",
    )
    .bind(key("cancel-guard", &guarded_run).as_str())
    .bind(guarded_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_outbox SET task_envelope=jsonb_set(task_envelope,
            '{request,admission_class}','\"termination_finalizer\"'::jsonb)
         WHERE run_id=$1 AND task_id=$2",
    )
    .bind(guarded_run.as_str())
    .bind(normal_claim.task_id().as_str())
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .mark_scheduler_task_started(&normal_claim)
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    assert_eq!(
        repository
            .heartbeat_scheduler_task(&normal_claim, 60)
            .await
            .unwrap(),
        SchedulerTaskHeartbeatOutcome::LeaseLost
    );
    assert_eq!(
        repository
            .commit_scheduler_task_outcome(&normal_claim, &retry_outcome(&normal_claim))
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_attempts WHERE run_id=$1 AND activation_id=$2",
        )
        .bind(guarded_run.as_str())
        .bind(normal_claim.activation_id().as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );

    drop(repository);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
