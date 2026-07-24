use std::{collections::BTreeMap, time::Duration};

use insight_agent_platform::{
    dsl::{compile_source, CompileOptions},
    engine::{
        plan::{
            DescriptorContractRegistry, LinkedPlan, NodeKind, Plan, PlanBuilder, PlanIndex, Policy,
            PolicyId, PolicyKind, SubflowContractRegistry, SubflowInterfaceContract, TimeoutPolicy,
            VersionTag,
        },
        repository::{
            drive_scheduler_once, ActivationDurableRepository, ClaimHumanWorkItemCommand,
            CompleteHumanWorkItemCommand, CreateRunCommand, DurableRepository,
            FencedSchedulerRunCommand, FireTimerCommand, HumanTaskDurableRepository,
            HumanTaskPrincipal, HumanWorkItemId, HumanWorkItemState, NoSchedulerCrash,
            PlanInstallOutcome, PostgresDurableRepository, ProjectionDurableRepository,
            ReceiveSignalCommand, ResolveSignalCommand, RuntimeIngressDurableRepository,
            SchedulerDriveOutcome, SchedulerDurableRepository, TimerFireAuthority, VersionedPlan,
        },
        DefinitionRevisionId, DeploymentRevisionId, ExecutionRevisionPin, IntentHash,
        PlannedSchedulerAction, RunId, RunLifecycle, RunTerminalFact, RuntimeValue,
        SchedulerAction, SchedulerDecision, SchedulerPlanner, SchedulerQuiescence, TransitionKey,
        TransitionOutcome,
    },
};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use uuid::Uuid;

const WAIT_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: approval
      wait:
        signal: review
        response: string
    - return: $approval
"#;

const FINALIZER_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - id: protected_wait
          wait:
            signal: continue
            response: string
      catch:
        safe_business_failure:
          as: failure
          steps:
            - id: recovery_pause
              wait: {duration_ms: 0}
      finally:
        - id: final_audit
          wait: {duration_ms: 0}
    - return: done
"#;

const FINALIZER_SUBFLOW_PARENT_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - id: protected_wait
          wait:
            signal: continue
            response: string
      catch:
        safe_business_failure:
          as: failure
          steps:
            - id: recovery_pause
              wait: {duration_ms: 0}
      finally:
        - id: final_child
          type: call
          definition_revision: finalizer_child_revision_v1
          interface_version: child-v1
          input: {question: finalizer-cleanup}
          response: string
    - return: unreachable
"#;

const HUMAN_TASK_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
types:
  Approval:
    fields:
      decision: {type: string, enum: [approved, rejected]}
      comment: string
inputs: {}
output: Approval
workflow:
  steps:
    - id: review
      human_task:
        signal: medical_review
        request: {kind: medical_report, report_id: pg-report-1}
        response: Approval
        candidate_groups: [medical-reviewers]
        claim_lease_ms: 60000
    - return: $review
"#;

const PARENT_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: child
      type: call
      definition_revision: child_revision_v1
      interface_version: child-v1
      input: {question: $question}
      response: string
    - return: $child
"#;

const DEADLINE_PARENT_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: child
      type: call
      definition_revision: child_revision_v1
      interface_version: child-v1
      timeout_ms: 300000
      input: {question: $question}
      response: string
    - return: $child
"#;

const CHILD_V1_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - return: $question
"#;

const CHILD_V2_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - return: wrong-revision
"#;

const OPTIONAL_SUBFLOW_PARENT_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
  note: {type: string, optional: true}
output: string
workflow:
  steps:
    - id: child
      type: call
      definition_revision: child_normalized_input_revision_v1
      interface_version: child-v1
      input:
        question: $question
        note: $note
      response: string
    - return: $child
"#;

const DEFAULTED_SUBFLOW_CHILD_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
  note: {type: string, optional: true}
  tone: {type: string, default: concise}
output: string
workflow:
  steps:
    - return: $question
"#;

const AUTHORED_RAISE_CATCH_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
errors:
  rejected: {category: workflow, code: REJECTED, public_message: rejected}
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - raise: rejected
      catch:
        safe_business_failure:
          as: failure
          steps:
            - return: caught
"#;

const TRY_RETURN_FINALLY_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - return: protected-result
      catch:
        safe_business_failure:
          as: failure
          steps:
            - return: catch-must-not-run
      finally:
        - id: cleanup
          if: "true"
          then:
            - yield: cleaned
          else:
            - yield: cleaned
"#;

const CATCH_RETURN_FINALLY_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
errors:
  rejected: {category: workflow, code: REJECTED, public_message: rejected}
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - raise: rejected
      catch:
        safe_business_failure:
          as: failure
          steps:
            - return: recovered-result
      finally:
        - id: cleanup
          if: "true"
          then:
            - yield: cleaned
          else:
            - yield: cleaned
"#;

const FINALIZER_RAISE_PRECEDENCE_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
errors:
  finalizer_rejected:
    category: workflow
    code: FINALIZER_REJECTED
    public_message: finalizer rejected
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - return: pending-result
      catch:
        safe_business_failure:
          as: failure
          steps:
            - return: catch-must-not-run
      finally:
        - raise: finalizer_rejected
"#;

const FINALIZER_RETURN_PRECEDENCE_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - return: pending-result
      catch:
        safe_business_failure:
          as: failure
          steps:
            - return: catch-must-not-run
      finally:
        - return: finalizer-result
"#;

fn key(label: &str, run_id: &RunId) -> TransitionKey {
    TransitionKey::derive(
        "scheduler.postgres.boundary.e2e.v1",
        &[label, run_id.as_str()],
    )
    .unwrap()
}

fn compile(revision: &str, source: &str) -> Plan {
    compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new(revision).unwrap(),
            format!("{revision}.yaml"),
            source,
        ),
    )
    .unwrap()
}

fn wait_plan() -> Plan {
    let compiled = compile("wait_revision_v1", WAIT_AGENT);
    let wait_node = compiled
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::WaitSignal(_)))
        .unwrap()
        .id()
        .clone();
    let policy_id = PolicyId::new("approval_timeout").unwrap();
    let mut source_map = compiled.source_map().clone();
    source_map.insert_policy(
        policy_id.clone(),
        compiled.source_map().node(&wait_node).unwrap().clone(),
    );
    let mut builder = PlanBuilder::from_verified_plan(&compiled).unwrap();
    builder.set_source_map(source_map);
    builder.add_policy(Policy::new(
        policy_id,
        wait_node,
        PolicyKind::Timeout(TimeoutPolicy { timeout_ms: 1 }),
    ));
    builder.build().unwrap()
}

fn versioned(
    definition_id: &str,
    agent_id: &str,
    deployment_revision: &str,
    plan: &Plan,
) -> VersionedPlan {
    VersionedPlan::from_verified_plan(
        definition_id,
        agent_id,
        agent_id,
        DeploymentRevisionId::new(deployment_revision).unwrap(),
        "expression-3.0.0",
        json!({"format": "scheduler-postgres-boundary-e2e"}),
        plan,
        json!({}),
        json!({"deployment": deployment_revision}),
        json!({}),
    )
    .unwrap()
}

fn subflow_parent_versioned(
    definition_id: &str,
    agent_id: &str,
    deployment_revision: &str,
    parent: &Plan,
    child: &VersionedPlan,
) -> VersionedPlan {
    let node = parent
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::SubflowCall(_)))
        .unwrap();
    let NodeKind::SubflowCall(descriptor) = node.kind() else {
        unreachable!()
    };
    VersionedPlan::from_verified_plan(
        definition_id,
        agent_id,
        agent_id,
        DeploymentRevisionId::new(deployment_revision).unwrap(),
        "expression-3.0.0",
        json!({"format": "scheduler-postgres-boundary-e2e"}),
        parent,
        json!({}),
        json!([{
            "node_id": node.id(),
            "binding": {
                "adapter": "durable_subflow",
                "definition_revision_id": child.definition_revision_id(),
                "deployment_revision_id": child.deployment_revision_id(),
                "plan_hash": child.plan_hash(),
                "binding_hash": child.binding_hash(),
                "interface_version": descriptor.interface_version,
            }
        }]),
        json!({}),
    )
    .unwrap()
}

async fn isolated_repository() -> Option<(PostgresDurableRepository, PgPool, PgPool, String)> {
    let database_url = std::env::var("TEST_POSTGRES_URL").ok()?;
    let schema = format!("scheduler_boundaries_{}", Uuid::new_v4().simple());
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

async fn cleanup(
    repository: PostgresDurableRepository,
    control: PgPool,
    admin: PgPool,
    schema: String,
) {
    drop(repository);
    control.close().await;
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

async fn create_run(
    repository: &PostgresDurableRepository,
    deployed: &VersionedPlan,
    run_id: &RunId,
    input: serde_json::Value,
) {
    assert!(matches!(
        repository
            .create_run(
                key("create", run_id),
                CreateRunCommand::new(run_id.clone(), deployed, input).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
}

async fn lease_run(
    control: &PgPool,
    run_id: &RunId,
    owner: &str,
    token: &str,
) -> FencedSchedulerRunCommand {
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs
             SET lifecycle=CASE WHEN lifecycle='created' THEN 'active' ELSE lifecycle END,
                 started_at=COALESCE(started_at,CURRENT_TIMESTAMP),
                 scheduler_lease_epoch=1,scheduler_lease_owner=$1,scheduler_fencing_token=$2,
                 scheduler_lease_expires_at=CURRENT_TIMESTAMP+INTERVAL '1 hour',
                 scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=$3 AND lifecycle IN ('created','active','waiting','terminating')",
        )
        .bind(owner)
        .bind(token)
        .bind(run_id.as_str())
        .execute(control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    FencedSchedulerRunCommand::new(run_id.clone(), owner, 1, token).unwrap()
}

async fn drive(
    repository: &PostgresDurableRepository,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
) -> SchedulerQuiescence {
    for step in 0..128 {
        match drive_scheduler_once(repository, linked, fence, &NoSchedulerCrash).await {
            Ok(SchedulerDriveOutcome::Applied(_)) => {}
            Ok(SchedulerDriveOutcome::Quiescent(outcome)) => return outcome,
            Ok(SchedulerDriveOutcome::Fenced) => panic!("scheduler was fenced"),
            Err(error) => {
                let facts = repository
                    .load_scheduler_facts(fence.run_id())
                    .await
                    .unwrap();
                let decision = SchedulerPlanner::new(linked).plan(&facts);
                panic!(
                    "durable boundary step {step} failed: {error:?}; next decision={decision:?}"
                );
            }
        }
    }
    panic!("scheduler exhausted the test action budget")
}

#[tokio::test]
async fn postgres_authored_try_exits_catch_finalize_and_apply_precedence_durably() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    for (suffix, source, expected_output, expected_error, expects_finalizer) in [
        (
            "raise_catch",
            AUTHORED_RAISE_CATCH_AGENT,
            Some("caught"),
            None,
            false,
        ),
        (
            "try_return",
            TRY_RETURN_FINALLY_AGENT,
            Some("protected-result"),
            None,
            true,
        ),
        (
            "catch_return",
            CATCH_RETURN_FINALLY_AGENT,
            Some("recovered-result"),
            None,
            true,
        ),
        (
            "finalizer_raise",
            FINALIZER_RAISE_PRECEDENCE_AGENT,
            None,
            Some("FINALIZER_REJECTED"),
            true,
        ),
        (
            "finalizer_return",
            FINALIZER_RETURN_PRECEDENCE_AGENT,
            Some("finalizer-result"),
            None,
            true,
        ),
    ] {
        let revision = format!("pg_authored_{suffix}_revision_v1");
        let plan = compile(&revision, source);
        let descriptors = DescriptorContractRegistry::new();
        let subflows = SubflowContractRegistry::new();
        let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
        let deployed = versioned(
            &format!("pg_authored_{suffix}_definition"),
            &format!("pg_authored_{suffix}_agent"),
            &format!("pg_authored_{suffix}_deployment_v1"),
            &plan,
        );
        repository.install_versioned_plan(&deployed).await.unwrap();
        let run_id = RunId::new(format!("run_pg_authored_{suffix}")).unwrap();
        create_run(&repository, &deployed, &run_id, json!({})).await;
        let fence = lease_run(
            &control,
            &run_id,
            &format!("pg-authored-{suffix}-owner"),
            &format!("pg-authored-{suffix}-fence"),
        )
        .await;
        let terminal = drive(&repository, &linked, &fence).await;
        let facts = repository.load_scheduler_facts(&run_id).await.unwrap();
        if let Some(output) = expected_output {
            assert_eq!(terminal, SchedulerQuiescence::RunSucceeded);
            assert_eq!(
                facts.terminal(),
                Some(&RunTerminalFact::Succeeded(
                    RuntimeValue::new(json!(output)).unwrap()
                ))
            );
        } else {
            assert_eq!(terminal, SchedulerQuiescence::RunFailed);
            assert!(matches!(
                facts.terminal(),
                Some(RunTerminalFact::Failed(error))
                    if error.value()["code"] == json!(expected_error.unwrap())
            ));
        }
        if expects_finalizer {
            let phases = sqlx::query_scalar::<_, String>(
                "SELECT fact_payload::text FROM scheduler_checkpoints
                 WHERE run_id=$1 AND checkpoint_kind='planned_action'
                   AND fact_payload::text LIKE '%transition_error_boundary%'
                 ORDER BY scheduler_projection_version",
            )
            .bind(run_id.as_str())
            .fetch_all(&control)
            .await
            .unwrap()
            .join("\n");
            assert!(phases.contains("\"phase\": \"finalizer\""));
            assert!(phases.contains("\"phase\": \"completed\""));
        }
    }
    cleanup(repository, control, admin, schema).await;
}

#[tokio::test]
async fn postgres_timeout_runs_durable_finalizer_before_terminal() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let plan = compile("pg_finalizer_revision_v1", FINALIZER_AGENT);
    let linked = LinkedPlan::link(
        &plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    let deployed = versioned(
        "pg_finalizer_definition",
        "pg_finalizer_agent",
        "pg_finalizer_deployment_v1",
        &plan,
    );
    repository.install_versioned_plan(&deployed).await.unwrap();
    let run_id = RunId::new("run_pg_timeout_finalizer").unwrap();
    create_run(&repository, &deployed, &run_id, json!({})).await;
    let fence = lease_run(
        &control,
        &run_id,
        "pg-finalizer-owner",
        "pg-finalizer-fence",
    )
    .await;
    assert!(matches!(
        drive(&repository, &linked, &fence).await,
        SchedulerQuiescence::WaitingForWait { .. }
    ));
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs
             SET lifecycle='terminating',admission_state='draining',
                 termination_intent_reason='timed_out',termination_intent_transition_key=$1,
                 termination_intent_at=clock_timestamp(),
                 projection_version=projection_version+1,updated_at=clock_timestamp()
             WHERE run_id=$2 AND lifecycle='active' AND termination_intent_reason IS NULL",
        )
        .bind(key("timeout-finalizer", &run_id).as_str())
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    let SchedulerQuiescence::WaitingForWait { wait_id, .. } =
        drive(&repository, &linked, &fence).await
    else {
        panic!("PostgreSQL finalizer did not reach its durable timer")
    };
    let timer_id = repository
        .load_scheduler_facts(&run_id)
        .await
        .unwrap()
        .waits()
        .get(&wait_id)
        .unwrap()
        .timer_id()
        .unwrap()
        .clone();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let timer_state = sqlx::query_as::<_, (String, String, bool)>(
        "SELECT m.timer_state,a.lifecycle,m.deadline_at <= clock_timestamp()
         FROM timers m JOIN node_activations a
           ON a.run_id=m.run_id AND a.activation_id=m.activation_id
         WHERE m.run_id=$1 AND m.timer_id=$2",
    )
    .bind(run_id.as_str())
    .bind(timer_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(timer_state, ("scheduled".into(), "waiting".into(), true));
    let timer_result = repository
        .fire_timer(
            key("fire-finalizer", &run_id),
            FireTimerCommand::new(run_id.clone(), timer_id, None),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            timer_result,
            TransitionOutcome::Committed {
                result: TimerFireAuthority::WaitResolved(_)
            }
        ),
        "unexpected finalizer timer result: {timer_result:?}"
    );
    assert_eq!(
        drive(&repository, &linked, &fence).await,
        SchedulerQuiescence::RunFailed
    );
    assert_eq!(
        repository
            .load_run(&run_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::TimedOut
    );
    let phases = sqlx::query_scalar::<_, String>(
        "SELECT fact_payload::text FROM scheduler_checkpoints
         WHERE run_id=$1 AND checkpoint_kind='planned_action'
           AND fact_payload::text LIKE '%transition_error_boundary%'
         ORDER BY scheduler_projection_version",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap()
    .join("\n");
    assert!(phases.contains("\"phase\": \"finalizer\""));
    assert!(phases.contains("\"phase\": \"completed\""));
    assert!(phases.contains("\"reason\": \"timed_out\""));
    cleanup(repository, control, admin, schema).await;
}

#[tokio::test]
async fn postgres_timeout_allows_finalizer_subflow_then_replays_original_terminal() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let parent_plan = compile(
        "finalizer_subflow_parent_revision_v1",
        FINALIZER_SUBFLOW_PARENT_AGENT,
    );
    let child_plan = compile("finalizer_child_revision_v1", CHILD_V1_AGENT);
    let _unpinned_parent_fixture = versioned(
        "finalizer_subflow_parent_definition",
        "finalizer_subflow_parent_agent",
        "finalizer_subflow_parent_deployment_v1",
        &parent_plan,
    );
    let child_versioned = versioned(
        "finalizer_child_definition",
        "finalizer_child_agent",
        "finalizer_child_deployment_v1",
        &child_plan,
    );
    let parent_versioned = subflow_parent_versioned(
        "finalizer_subflow_parent_definition",
        "finalizer_subflow_parent_agent",
        "finalizer_subflow_parent_deployment_v1",
        &parent_plan,
        &child_versioned,
    );
    let subflows = subflow_contract(&parent_plan, &child_plan, &child_versioned);
    let parent_linked =
        LinkedPlan::link(&parent_plan, &DescriptorContractRegistry::new(), &subflows).unwrap();
    let child_linked = LinkedPlan::link(
        &child_plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    for revision in [&child_versioned, &parent_versioned] {
        assert_eq!(
            repository.install_versioned_plan(revision).await.unwrap(),
            PlanInstallOutcome::Installed
        );
    }

    let parent_run = RunId::new("run_pg_timeout_finalizer_subflow").unwrap();
    create_run(&repository, &parent_versioned, &parent_run, json!({})).await;
    let parent_fence = lease_run(
        &control,
        &parent_run,
        "pg-finalizer-subflow-parent",
        "pg-finalizer-subflow-parent-fence",
    )
    .await;
    assert!(matches!(
        drive(&repository, &parent_linked, &parent_fence).await,
        SchedulerQuiescence::WaitingForWait { .. }
    ));
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs
             SET lifecycle='terminating',admission_state='draining',
                 termination_intent_reason='timed_out',termination_intent_transition_key=$1,
                 termination_intent_at=clock_timestamp(),
                 projection_version=projection_version+1,updated_at=clock_timestamp()
             WHERE run_id=$2 AND lifecycle='active' AND termination_intent_reason IS NULL",
        )
        .bind(key("timeout-finalizer-subflow", &parent_run).as_str())
        .bind(parent_run.as_str())
        .execute(&control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    let SchedulerQuiescence::WaitingForChildRun { child_run_id, .. } =
        drive(&repository, &parent_linked, &parent_fence).await
    else {
        panic!("finalizer did not reach its authored child Run")
    };
    assert_eq!(
        repository
            .load_run(&child_run_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::Created
    );
    assert!(!repository
        .load_scheduler_facts(&parent_run)
        .await
        .unwrap()
        .child_cancellation_requests()
        .contains(&child_run_id));

    let child_fence = lease_run(
        &control,
        &child_run_id,
        "pg-finalizer-subflow-child",
        "pg-finalizer-subflow-child-fence",
    )
    .await;
    assert_eq!(
        drive(&repository, &child_linked, &child_fence).await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_eq!(
        drive(&repository, &parent_linked, &parent_fence).await,
        SchedulerQuiescence::RunFailed
    );
    assert_eq!(
        repository
            .load_run(&parent_run)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::TimedOut
    );
    assert_eq!(
        repository
            .load_run(&child_run_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::Succeeded
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT invocation_state FROM scheduler_subflow_invocations
             WHERE run_id=$1 AND child_run_id=$2",
        )
        .bind(parent_run.as_str())
        .bind(child_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "completed"
    );
    let facts = repository.load_scheduler_facts(&parent_run).await.unwrap();
    assert_eq!(facts.terminal(), Some(&RunTerminalFact::TimedOut));
    for _ in 0..2 {
        assert!(matches!(
            SchedulerPlanner::new(&parent_linked).plan(&facts).unwrap(),
            SchedulerDecision::Quiescent(SchedulerQuiescence::RunFailed)
        ));
    }
    cleanup(repository, control, admin, schema).await;
}

#[tokio::test]
async fn postgres_human_task_yaml_lowers_to_typed_durable_signal_and_succeeds() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let plan = compile("human_task_revision_v1", HUMAN_TASK_AGENT);
    assert!(plan.nodes().iter().any(|node| matches!(
        node.kind(),
        NodeKind::HumanTask(descriptor)
            if descriptor.completion_signal == "medical_review"
                && descriptor.response_type == *plan.metadata().output_type()
    )));
    let linked = LinkedPlan::link(
        &plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    let deployed = versioned(
        "human_task_definition",
        "human_task_agent",
        "human_task_deployment_v1",
        &plan,
    );
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    let run_id = RunId::new("run_pg_human_task_approval").unwrap();
    create_run(&repository, &deployed, &run_id, json!({})).await;
    let fence = lease_run(
        &control,
        &run_id,
        "pg-human-task-scheduler",
        "pg-human-task-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &fence).await
    else {
        panic!("human_task did not reach its durable signal wait")
    };
    let registration = repository
        .load_scheduler_facts(&run_id)
        .await
        .unwrap()
        .waits()
        .get(&wait_id)
        .unwrap()
        .clone();
    assert!(registration.timer_id().is_none());
    let signal_id = registration.signal_id().unwrap().clone();
    let approval = json!({"decision": "approved", "comment": "reviewed"});
    let principal =
        HumanTaskPrincipal::new("pg-reviewer", vec!["medical-reviewers".to_owned()]).unwrap();
    let item = repository
        .list_human_work_items(&principal, 10)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        item.request(),
        &json!({"kind": "medical_report", "report_id": "pg-report-1"})
    );
    let item_id = HumanWorkItemId::new(item.work_item_id().as_str()).unwrap();
    let claim = match repository
        .claim_human_work_item(
            ClaimHumanWorkItemCommand::new(item_id.clone(), principal.clone(), "pg-human-claim")
                .unwrap(),
        )
        .await
        .unwrap()
    {
        TransitionOutcome::Committed { result } => result,
        other => panic!("unexpected claim outcome: {other:?}"),
    };
    let completion = match repository
        .complete_human_work_item(
            CompleteHumanWorkItemCommand::new(
                item_id.clone(),
                principal.clone(),
                "pg-human-complete",
                claim.claim_fence(),
                approval.clone(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        TransitionOutcome::Committed { result } => result,
        other => panic!("unexpected completion reservation: {other:?}"),
    };
    assert_eq!(completion.signal_id(), &signal_id);
    assert!(matches!(
        repository
            .receive_signal(
                ReceiveSignalCommand::new(
                    run_id.clone(),
                    signal_id.clone(),
                    completion.message_id(),
                    "medical_review",
                    activation_id.clone(),
                    approval.clone(),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let activation = repository
        .load_activation(&run_id, &activation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        repository
            .resolve_wait_signal(
                key("resolve-human-task", &run_id),
                ResolveSignalCommand::new(
                    run_id.clone(),
                    activation_id,
                    signal_id,
                    activation.projection_version(),
                ),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert_eq!(
        drive(&repository, &linked, &fence).await,
        SchedulerQuiescence::RunSucceeded
    );
    // Deterministic resolve->terminal->finalize race: the Run terminal trigger
    // recognizes the consumed reserved completion and projects completed,
    // never cancelled.
    assert_eq!(
        repository
            .load_human_work_item(&item_id)
            .await
            .unwrap()
            .unwrap()
            .state(),
        HumanWorkItemState::Completed
    );
    assert!(repository
        .finalize_human_work_item(&item_id, "pg-human-complete")
        .await
        .unwrap());
    let facts = repository.load_scheduler_facts(&run_id).await.unwrap();
    assert_eq!(
        facts.terminal(),
        Some(&RunTerminalFact::Succeeded(
            RuntimeValue::new(approval).unwrap()
        ))
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT winner_kind FROM scheduler_wait_registrations WHERE run_id=$1 AND wait_id=$2",
        )
        .bind(run_id.as_str())
        .bind(wait_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "signal"
    );
    let authority_signal_id: String =
        sqlx::query_scalar("SELECT signal_id FROM signals_inbox WHERE run_id=$1")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
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
    sqlx::query("UPDATE human_work_items SET request_value=$1 WHERE work_item_id=$2")
        .bind(json!({"authority_marker": true}))
        .bind(item_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("UPDATE signals_inbox SET signal_name=$1 WHERE signal_id=$2")
        .bind("authority-marker")
        .bind(&authority_signal_id)
        .execute(&control)
        .await
        .unwrap();
    repository.repair_all_projections(&run_id).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT request_value FROM human_work_items WHERE work_item_id=$1",
        )
        .bind(item_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        json!({"authority_marker": true}),
        "repair must not update human-task claim/completion authority"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT signal_name FROM signals_inbox WHERE signal_id=$1",
        )
        .bind(&authority_signal_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        "authority-marker",
        "repair must not update signal receipt authority"
    );
    assert_eq!(
        sqlx::query("DELETE FROM human_work_items WHERE work_item_id=$1")
            .bind(item_id.as_str())
            .execute(&control)
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    assert_eq!(
        sqlx::query("DELETE FROM signals_inbox WHERE signal_id=$1")
            .bind(&authority_signal_id)
            .execute(&control)
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    repository.repair_all_projections(&run_id).await.unwrap();
    assert!(repository
        .load_human_work_item(&item_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM signals_inbox WHERE signal_id=$1")
            .bind(&authority_signal_id)
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
        "repair must not recreate a deleted signal receipt authority"
    );
    cleanup(repository, control, admin, schema).await;
}

#[tokio::test]
async fn postgres_human_claim_lease_reopens_for_authorized_peer_and_fences_stale_owner() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let plan = compile("human_task_claim_lease_revision", HUMAN_TASK_AGENT);
    let linked = LinkedPlan::link(
        &plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    let deployed = versioned(
        "human_claim_lease_definition",
        "human_claim_lease_agent",
        "human_claim_lease_deployment",
        &plan,
    );
    repository.install_versioned_plan(&deployed).await.unwrap();
    let run_id = RunId::new("run_pg_human_claim_lease").unwrap();
    create_run(&repository, &deployed, &run_id, json!({})).await;
    let fence = lease_run(
        &control,
        &run_id,
        "pg-human-claim-lease-scheduler",
        "pg-human-claim-lease-fence",
    )
    .await;
    assert!(matches!(
        drive(&repository, &linked, &fence).await,
        SchedulerQuiescence::WaitingForWait { .. }
    ));
    let alice = HumanTaskPrincipal::new("alice", vec!["medical-reviewers".to_owned()]).unwrap();
    let bob = HumanTaskPrincipal::new("bob", vec!["medical-reviewers".to_owned()]).unwrap();
    let id = repository
        .list_human_work_items(&alice, 10)
        .await
        .unwrap()
        .pop()
        .unwrap()
        .work_item_id()
        .clone();
    let alice_command =
        ClaimHumanWorkItemCommand::new(id.clone(), alice.clone(), "alice-claim").unwrap();
    let bob_command = ClaimHumanWorkItemCommand::new(id.clone(), bob.clone(), "bob-claim").unwrap();
    let (alice_result, bob_result) = tokio::join!(
        repository.claim_human_work_item(alice_command),
        repository.claim_human_work_item(bob_command),
    );
    let (winning_claim, winner, loser, loser_request_id) =
        match (alice_result.unwrap(), bob_result.unwrap()) {
            (TransitionOutcome::Committed { result }, TransitionOutcome::StateConflict) => {
                (result, alice, bob, "bob-reclaim")
            }
            (TransitionOutcome::StateConflict, TransitionOutcome::Committed { result }) => {
                (result, bob, alice, "alice-reclaim")
            }
            other => panic!("concurrent claims must have exactly one winner: {other:?}"),
        };
    assert_eq!(
        sqlx::query(
            "UPDATE human_work_items
             SET claim_expires_at=clock_timestamp() - INTERVAL '1 millisecond'
             WHERE work_item_id=$1 AND work_state='claimed' AND claimed_by=$2",
        )
        .bind(id.as_str())
        .bind(winner.identity())
        .execute(&control)
        .await
        .unwrap()
        .rows_affected(),
        1,
        "the winning claim must still be authoritative before expiry"
    );
    let visible = repository.list_human_work_items(&loser, 10).await.unwrap();
    assert_eq!(
        visible.len(),
        1,
        "list must sweep an expired claim before LIMIT"
    );
    let successor_claim = match repository
        .claim_human_work_item(
            ClaimHumanWorkItemCommand::new(id.clone(), loser, loser_request_id).unwrap(),
        )
        .await
        .unwrap()
    {
        TransitionOutcome::Committed { result } => result,
        other => panic!("unexpected successor reclaim: {other:?}"),
    };
    assert!(successor_claim.claim_fence() > winning_claim.claim_fence());
    assert!(matches!(
        repository
            .complete_human_work_item(
                CompleteHumanWorkItemCommand::new(
                    id,
                    winner,
                    "stale-winner-complete",
                    winning_claim.claim_fence(),
                    json!({"decision": "approved", "comment": "stale"}),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    ));
    cleanup(repository, control, admin, schema).await;
}

#[tokio::test]
async fn postgres_wait_signal_and_timeout_share_one_durable_first_winner() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let plan = wait_plan();
    let linked = LinkedPlan::link(
        &plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    let deployed = versioned("wait_definition", "wait_agent", "wait_deployment_v1", &plan);
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    let signal_run = RunId::new("run_pg_wait_signal_wins").unwrap();
    create_run(&repository, &deployed, &signal_run, json!({})).await;
    let signal_fence = lease_run(
        &control,
        &signal_run,
        "pg-wait-signal-scheduler",
        "pg-wait-signal-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &signal_fence).await
    else {
        panic!("signal fixture did not reach its durable wait")
    };
    let deadline_delta_ms: i64 = sqlx::query_scalar(
        "SELECT due_at_ms - FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
         FROM scheduler_wait_registrations WHERE run_id=$1 AND wait_id=$2",
    )
    .bind(signal_run.as_str())
    .bind(wait_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert!(
        (-2_000..=1_001).contains(&deadline_delta_ms),
        "wait timeout must derive from the PostgreSQL clock, delta={deadline_delta_ms}ms"
    );
    let registration = repository
        .load_scheduler_facts(&signal_run)
        .await
        .unwrap()
        .waits()
        .get(&wait_id)
        .unwrap()
        .clone();
    let signal_id = registration.signal_id().unwrap().clone();
    let timer_id = registration.timer_id().unwrap().clone();
    assert!(matches!(
        repository
            .receive_signal(
                ReceiveSignalCommand::new(
                    signal_run.clone(),
                    signal_id.clone(),
                    "message-pg-signal-wins",
                    "review",
                    activation_id.clone(),
                    json!("approved"),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let activation = repository
        .load_activation(&signal_run, &activation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        repository
            .resolve_wait_signal(
                key("resolve-signal", &signal_run),
                ResolveSignalCommand::new(
                    signal_run.clone(),
                    activation_id.clone(),
                    signal_id.clone(),
                    activation.projection_version(),
                ),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let late_timer_key = key("late-timer", &signal_run);
    let late_timer_command = FireTimerCommand::new(signal_run.clone(), timer_id.clone(), None);
    assert_eq!(
        repository
            .fire_timer(late_timer_key.clone(), late_timer_command.clone())
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    assert_eq!(
        repository
            .fire_timer(late_timer_key, late_timer_command)
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events WHERE run_id=$1 AND kind='timer.late'",
        )
        .bind(signal_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        drive(&repository, &linked, &signal_fence).await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT winner_kind FROM scheduler_wait_registrations WHERE run_id=$1",
        )
        .bind(signal_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "signal"
    );

    let timer_run = RunId::new("run_pg_wait_timer_wins").unwrap();
    create_run(&repository, &deployed, &timer_run, json!({})).await;
    let timer_fence = lease_run(
        &control,
        &timer_run,
        "pg-wait-timer-scheduler",
        "pg-wait-timer-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &timer_fence).await
    else {
        panic!("timer fixture did not reach its durable wait")
    };
    let registration = repository
        .load_scheduler_facts(&timer_run)
        .await
        .unwrap()
        .waits()
        .get(&wait_id)
        .unwrap()
        .clone();
    let signal_id = registration.signal_id().unwrap().clone();
    let timer_id = registration.timer_id().unwrap().clone();
    assert!(matches!(
        repository
            .receive_signal(
                ReceiveSignalCommand::new(
                    timer_run.clone(),
                    signal_id.clone(),
                    "message-pg-timer-wins",
                    "review",
                    activation_id.clone(),
                    json!("too-late"),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let activation = repository
        .load_activation(&timer_run, &activation_id)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert!(matches!(
        repository
            .fire_timer(
                key("resolve-timer", &timer_run),
                FireTimerCommand::new(timer_run.clone(), timer_id, None),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let late_signal_key = key("late-signal", &timer_run);
    let late_signal_command = ResolveSignalCommand::new(
        timer_run.clone(),
        activation_id,
        signal_id,
        activation.projection_version(),
    );
    assert_eq!(
        repository
            .resolve_wait_signal(late_signal_key.clone(), late_signal_command.clone())
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    assert_eq!(
        repository
            .resolve_wait_signal(late_signal_key, late_signal_command)
            .await
            .unwrap(),
        TransitionOutcome::StateConflict
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events WHERE run_id=$1 AND kind='signal.late'",
        )
        .bind(timer_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        drive(&repository, &linked, &timer_fence).await,
        SchedulerQuiescence::RunFailed
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT winner_kind FROM scheduler_wait_registrations WHERE run_id=$1",
        )
        .bind(timer_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "timer"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT signal_state FROM signals_inbox WHERE run_id=$1 AND signal_id=$2",
        )
        .bind(timer_run.as_str())
        .bind(registration.signal_id().unwrap().as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "rejected"
    );
    assert_eq!(
        repository
            .load_run(&timer_run)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::Failed
    );

    let race_run = RunId::new("run_pg_wait_concurrent_first_winner").unwrap();
    create_run(&repository, &deployed, &race_run, json!({})).await;
    let race_fence = lease_run(
        &control,
        &race_run,
        "pg-wait-race-scheduler",
        "pg-wait-race-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &race_fence).await
    else {
        panic!("race fixture did not reach its durable wait")
    };
    let registration = repository
        .load_scheduler_facts(&race_run)
        .await
        .unwrap()
        .waits()
        .get(&wait_id)
        .unwrap()
        .clone();
    let signal_id = registration.signal_id().unwrap().clone();
    let timer_id = registration.timer_id().unwrap().clone();
    repository
        .receive_signal(
            ReceiveSignalCommand::new(
                race_run.clone(),
                signal_id.clone(),
                "message-pg-concurrent-race",
                "review",
                activation_id.clone(),
                json!("race"),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let version = repository
        .load_activation(&race_run, &activation_id)
        .await
        .unwrap()
        .unwrap()
        .projection_version();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let signal_key = key("concurrent-signal", &race_run);
    let signal_command =
        ResolveSignalCommand::new(race_run.clone(), activation_id.clone(), signal_id, version);
    let timer_key = key("concurrent-timer", &race_run);
    let timer_command = FireTimerCommand::new(race_run.clone(), timer_id, None);
    let (signal, timer) = tokio::join!(
        repository.resolve_wait_signal(signal_key.clone(), signal_command.clone()),
        repository.fire_timer(timer_key.clone(), timer_command.clone()),
    );
    let signal_won = matches!(signal.unwrap(), TransitionOutcome::Committed { .. });
    let timer_won = matches!(timer.unwrap(), TransitionOutcome::Committed { .. });
    assert_ne!(signal_won, timer_won);
    if signal_won {
        assert_eq!(
            repository
                .fire_timer(timer_key, timer_command)
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
    } else {
        assert_eq!(
            repository
                .resolve_wait_signal(signal_key, signal_command)
                .await
                .unwrap(),
            TransitionOutcome::StateConflict
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events
             WHERE run_id=$1 AND kind IN ('signal.late','timer.late')",
        )
        .bind(race_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );

    cleanup(repository, control, admin, schema).await;
}

#[tokio::test]
async fn postgres_late_audit_reconciles_atomically_after_fault_and_restart() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let plan = wait_plan();
    let linked = LinkedPlan::link(
        &plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    let deployed = versioned(
        "pg_wait_late_atomic_definition",
        "pg_wait_late_atomic_agent",
        "pg_wait_late_atomic_deployment_v1",
        &plan,
    );
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    // Crash point 1: the signal winner is durable but the timer-loser pump is
    // gone. A BEFORE trigger faults the reconciler before timer.late exists.
    let signal_run = RunId::new("run_pg_timer_late_reconcile_fault").unwrap();
    create_run(&repository, &deployed, &signal_run, json!({})).await;
    let signal_fence = lease_run(
        &control,
        &signal_run,
        "pg-late-reconcile-signal-scheduler",
        "pg-late-reconcile-signal-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &signal_fence).await
    else {
        panic!("timer-late reconcile fixture did not reach its durable wait")
    };
    let registration = repository
        .load_scheduler_facts(&signal_run)
        .await
        .unwrap()
        .waits()
        .get(&wait_id)
        .unwrap()
        .clone();
    let signal_id = registration.signal_id().unwrap().clone();
    assert!(matches!(
        repository
            .receive_signal(
                ReceiveSignalCommand::new(
                    signal_run.clone(),
                    signal_id.clone(),
                    "message-pg-timer-late-reconcile",
                    "review",
                    activation_id.clone(),
                    json!("approved"),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let activation_version = repository
        .load_activation(&signal_run, &activation_id)
        .await
        .unwrap()
        .unwrap()
        .projection_version();
    assert!(matches!(
        repository
            .resolve_wait_signal(
                key("resolve-signal-before-pg-timer-late-fault", &signal_run),
                ResolveSignalCommand::new(
                    signal_run.clone(),
                    activation_id.clone(),
                    signal_id,
                    activation_version,
                ),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    tokio::time::sleep(Duration::from_millis(5)).await;
    let signal_winner_event: String = sqlx::query_scalar(
        "SELECT consumed_event_id FROM signals_inbox
         WHERE run_id=$1 AND target_activation_id=$2",
    )
    .bind(signal_run.as_str())
    .bind(activation_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let next_before_timer_late: i64 =
        sqlx::query_scalar("SELECT next_event_seq FROM workflow_runs WHERE run_id=$1")
            .bind(signal_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    sqlx::query(
        "CREATE FUNCTION fail_timer_late_before_insert_fn() RETURNS trigger
         LANGUAGE plpgsql AS $body$
         BEGIN
           RAISE EXCEPTION 'fault before timer late commit';
         END
         $body$",
    )
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_timer_late_before_insert
         BEFORE INSERT ON execution_events
         FOR EACH ROW WHEN (NEW.kind='timer.late')
         EXECUTE FUNCTION fail_timer_late_before_insert_fn()",
    )
    .execute(&control)
    .await
    .unwrap();
    assert!(repository.reconcile_wait_late_audits(128).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events WHERE run_id=$1 AND kind='timer.late'",
        )
        .bind(signal_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_event_seq FROM workflow_runs WHERE run_id=$1",)
            .bind(signal_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        next_before_timer_late
    );
    sqlx::query("DROP TRIGGER fail_timer_late_before_insert ON execution_events")
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION fail_timer_late_before_insert_fn()")
        .execute(&control)
        .await
        .unwrap();
    drop(repository);
    let reconciler_a = PostgresDurableRepository::from_pool(control.clone());
    let reconciler_b = PostgresDurableRepository::from_pool(control.clone());
    let (appended_a, appended_b) = tokio::join!(
        reconciler_a.reconcile_wait_late_audits(128),
        reconciler_b.reconcile_wait_late_audits(128),
    );
    assert_eq!(appended_a.unwrap() + appended_b.unwrap(), 1);
    drop(reconciler_a);
    drop(reconciler_b);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT causation_event_id FROM execution_events
             WHERE run_id=$1 AND kind='timer.late'",
        )
        .bind(signal_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        signal_winner_event
    );
    // Crash point 3: reconstruct immediately after the successful commit.
    // The stable internal transition identity makes the next pump a no-op.
    let repository = PostgresDurableRepository::from_pool(control.clone());
    assert_eq!(repository.reconcile_wait_late_audits(128).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events WHERE run_id=$1 AND kind='timer.late'",
        )
        .bind(signal_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );

    // Crash point 2, opposite direction: timer is durable and its transaction
    // has rejected the received signal. The AFTER trigger faults after the
    // signal.late row and sequence are staged but before their transaction can
    // commit, so restart must observe neither and reconcile without ingress.
    let timer_run = RunId::new("run_pg_signal_late_reconcile_fault").unwrap();
    create_run(&repository, &deployed, &timer_run, json!({})).await;
    let timer_fence = lease_run(
        &control,
        &timer_run,
        "pg-late-reconcile-timer-scheduler",
        "pg-late-reconcile-timer-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &timer_fence).await
    else {
        panic!("signal-late reconcile fixture did not reach its durable wait")
    };
    let registration = repository
        .load_scheduler_facts(&timer_run)
        .await
        .unwrap()
        .waits()
        .get(&wait_id)
        .unwrap()
        .clone();
    let signal_id = registration.signal_id().unwrap().clone();
    let timer_id = registration.timer_id().unwrap().clone();
    assert!(matches!(
        repository
            .receive_signal(
                ReceiveSignalCommand::new(
                    timer_run.clone(),
                    signal_id,
                    "message-pg-signal-late-reconcile",
                    "review",
                    activation_id.clone(),
                    json!("too-late"),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(matches!(
        repository
            .fire_timer(
                key("resolve-timer-before-pg-signal-late-fault", &timer_run),
                FireTimerCommand::new(timer_run.clone(), timer_id, None),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let timer_winner_event: String = sqlx::query_scalar(
        "SELECT fired_event_id FROM timers
         WHERE run_id=$1 AND activation_id=$2 AND timer_kind='wait'",
    )
    .bind(timer_run.as_str())
    .bind(activation_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let next_before_signal_late: i64 =
        sqlx::query_scalar("SELECT next_event_seq FROM workflow_runs WHERE run_id=$1")
            .bind(timer_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    sqlx::query(
        "CREATE FUNCTION fail_signal_late_after_insert_fn() RETURNS trigger
         LANGUAGE plpgsql AS $body$
         BEGIN
           RAISE EXCEPTION 'fault after signal late insert before commit';
         END
         $body$",
    )
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_signal_late_after_insert
         AFTER INSERT ON execution_events
         FOR EACH ROW WHEN (NEW.kind='signal.late')
         EXECUTE FUNCTION fail_signal_late_after_insert_fn()",
    )
    .execute(&control)
    .await
    .unwrap();
    assert!(repository.reconcile_wait_late_audits(128).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events WHERE run_id=$1 AND kind='signal.late'",
        )
        .bind(timer_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_event_seq FROM workflow_runs WHERE run_id=$1",)
            .bind(timer_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        next_before_signal_late
    );
    sqlx::query("DROP TRIGGER fail_signal_late_after_insert ON execution_events")
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION fail_signal_late_after_insert_fn()")
        .execute(&control)
        .await
        .unwrap();
    drop(repository);
    let repository = PostgresDurableRepository::from_pool(control.clone());
    assert_eq!(repository.reconcile_wait_late_audits(128).await.unwrap(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT causation_event_id FROM execution_events
             WHERE run_id=$1 AND kind='signal.late'",
        )
        .bind(timer_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        timer_winner_event
    );
    drop(repository);
    let repository = PostgresDurableRepository::from_pool(control.clone());
    assert_eq!(repository.reconcile_wait_late_audits(128).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events WHERE run_id=$1 AND kind='signal.late'",
        )
        .bind(timer_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );

    cleanup(repository, control, admin, schema).await;
}

fn subflow_contract(
    parent: &Plan,
    child_plan: &Plan,
    child: &VersionedPlan,
) -> SubflowContractRegistry {
    let index = PlanIndex::new(parent).unwrap();
    let call = parent
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::SubflowCall(_)))
        .unwrap();
    let outputs = index
        .data_outputs(call.id())
        .iter()
        .map(|id| {
            let port = index.data_port(id).unwrap();
            (port.name().clone(), port.value_type().clone())
        })
        .collect::<BTreeMap<_, _>>();
    let mut registry = SubflowContractRegistry::new();
    registry
        .register(SubflowInterfaceContract::new(
            ExecutionRevisionPin::new(
                child.definition_revision_id().clone(),
                child.deployment_revision_id().clone(),
                child.plan_hash().clone(),
                child.binding_hash().clone(),
            ),
            VersionTag::new("child-v1").unwrap(),
            child_plan.metadata().input_contract().clone(),
            outputs,
            parent.metadata().error_type().clone(),
        ))
        .unwrap();
    registry
}

async fn start_parent_and_child(
    repository: &PostgresDurableRepository,
    control: &PgPool,
    parent_versioned: &VersionedPlan,
    parent_linked: &LinkedPlan<'_>,
    parent_run: &RunId,
    owner: &str,
    token: &str,
) -> (FencedSchedulerRunCommand, RunId) {
    let input = json!({"question": "exact child"});
    let runtime_input = RuntimeValue::new(input.clone()).unwrap();
    let expected_input = parent_linked
        .index()
        .metadata()
        .input_contract()
        .run_type()
        .unwrap();
    assert!(
        runtime_input.matches(&expected_input),
        "runtime input type {:?} does not match parent input type {:?}",
        runtime_input.value_type(),
        expected_input,
    );
    create_run(repository, parent_versioned, parent_run, input).await;
    let fence = lease_run(control, parent_run, owner, token).await;
    let SchedulerQuiescence::WaitingForChildRun { child_run_id, .. } =
        drive(repository, parent_linked, &fence).await
    else {
        panic!("parent did not reach its durable child wait")
    };
    (fence, child_run_id)
}

async fn start_parent_and_child_with_timeout(
    repository: &PostgresDurableRepository,
    control: &PgPool,
    parent_versioned: &VersionedPlan,
    parent_linked: &LinkedPlan<'_>,
    parent_run: &RunId,
    timeout: Duration,
) -> RunId {
    let input = json!({"question": "deadline child"});
    assert!(matches!(
        repository
            .create_run(
                key("create-with-deadline", parent_run),
                CreateRunCommand::new(parent_run.clone(), parent_versioned, input)
                    .unwrap()
                    .with_run_timeout(timeout)
                    .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let fence = lease_run(
        control,
        parent_run,
        "deadline-parent",
        "deadline-parent-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForChildRun { child_run_id, .. } =
        drive(repository, parent_linked, &fence).await
    else {
        panic!("parent did not reach its durable child wait")
    };
    child_run_id
}

async fn assert_subflow_parent_drained(
    control: &PgPool,
    parent_run_id: &RunId,
    invocation_scope_lifecycle: &str,
    root_lifecycle: &str,
) {
    let invocation = sqlx::query_as::<_, (String, String, String)>(
        "SELECT i.invocation_state,s.lifecycle,s.admission_state
         FROM scheduler_subflow_invocations i
         JOIN scope_instances s ON s.run_id=i.run_id
              AND s.scope_instance_id=i.invocation_scope_instance_id
         WHERE i.run_id=$1",
    )
    .bind(parent_run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap();
    assert_eq!(invocation.0, "completed");
    assert_eq!(invocation.1, invocation_scope_lifecycle);
    assert_eq!(invocation.2, "closed");

    let root = sqlx::query_as::<_, (String, String, i64, i64)>(
        "SELECT lifecycle,admission_state,admitted_children,settled_children
         FROM scope_instances WHERE run_id=$1 AND is_root=TRUE",
    )
    .bind(parent_run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap();
    assert_eq!(root.0, root_lifecycle);
    assert_eq!(root.1, "closed");
    assert_eq!(root.2, root.3);
    assert_eq!(root.2, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_activations WHERE run_id=$1 AND lifecycle IN
               ('created','ready','leased','running','retry_wait','waiting','terminating')",
        )
        .bind(parent_run_id.as_str())
        .fetch_one(control)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn postgres_subflow_admission_rederives_normalized_input_and_call_contract() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let parent_plan = compile(
        "parent_normalized_input_revision_v1",
        OPTIONAL_SUBFLOW_PARENT_AGENT,
    );
    let child_plan = compile(
        "child_normalized_input_revision_v1",
        DEFAULTED_SUBFLOW_CHILD_AGENT,
    );
    let _unpinned_parent_fixture = versioned(
        "parent_normalized_input_definition",
        "parent_normalized_input_agent",
        "parent_normalized_input_deployment_v1",
        &parent_plan,
    );
    let child_versioned = versioned(
        "child_normalized_input_definition",
        "child_normalized_input_agent",
        "child_normalized_input_deployment_v1",
        &child_plan,
    );
    let parent_versioned = subflow_parent_versioned(
        "parent_normalized_input_definition",
        "parent_normalized_input_agent",
        "parent_normalized_input_deployment_v1",
        &parent_plan,
        &child_versioned,
    );
    let subflows = subflow_contract(&parent_plan, &child_plan, &child_versioned);
    let linked =
        LinkedPlan::link(&parent_plan, &DescriptorContractRegistry::new(), &subflows).unwrap();
    for revision in [&child_versioned, &parent_versioned] {
        repository.install_versioned_plan(revision).await.unwrap();
    }

    let parent_run = RunId::new("run_pg_subflow_normalized_input_parent").unwrap();
    create_run(
        &repository,
        &parent_versioned,
        &parent_run,
        json!({"question": "exact child"}),
    )
    .await;
    let fence = lease_run(
        &control,
        &parent_run,
        "pg-normalized-input-parent",
        "pg-normalized-input-parent-fence",
    )
    .await;
    let planner = SchedulerPlanner::new(&linked);
    let start_subflow = loop {
        let facts = repository.load_scheduler_facts(&parent_run).await.unwrap();
        let SchedulerDecision::Action(action) = planner.plan(&facts).unwrap() else {
            panic!("parent quiesced before planning its child")
        };
        if matches!(
            action.intent().action(),
            SchedulerAction::StartSubflow { .. }
        ) {
            break action;
        }
        repository
            .commit_scheduler_action(&fence, &action)
            .await
            .unwrap();
    };

    let valid_wire = serde_json::to_value(start_subflow.as_ref()).unwrap();
    let mut tampered = Vec::new();

    let mut missing_default = valid_wire.clone();
    missing_default["intent"]["action"]["run_input"] =
        serde_json::to_value(RuntimeValue::new(json!({"question": "exact child"})).unwrap())
            .unwrap();
    tampered.push(("missing child default", missing_default));

    let mut substituted_default = valid_wire.clone();
    substituted_default["intent"]["action"]["run_input"] = serde_json::to_value(
        RuntimeValue::new(json!({"question": "exact child", "tone": "verbose"})).unwrap(),
    )
    .unwrap();
    tampered.push((
        "type-correct child default substitution",
        substituted_default,
    ));

    let mut injected_optional = valid_wire.clone();
    injected_optional["intent"]["action"]["run_input"] = serde_json::to_value(
        RuntimeValue::new(
            json!({"question": "exact child", "note": "injected", "tone": "concise"}),
        )
        .unwrap(),
    )
    .unwrap();
    tampered.push(("unbound optional injection", injected_optional));

    let mut wrong_child_run = valid_wire.clone();
    let actual_child_run = wrong_child_run["intent"]["action"]["invocation"]["child_run_id"]
        .as_str()
        .unwrap();
    let replacement = if actual_child_run.ends_with('0') {
        '1'
    } else {
        '0'
    };
    wrong_child_run["intent"]["action"]["invocation"]["child_run_id"] = json!(format!(
        "{}{}",
        &actual_child_run[..actual_child_run.len() - 1],
        replacement
    ));
    tampered.push(("wrong deterministic child run", wrong_child_run));

    let mut wrong_invocation_scope = valid_wire.clone();
    let actual_scope = wrong_invocation_scope["intent"]["action"]["invocation"]
        ["invocation_scope_instance_id"]
        .as_str()
        .unwrap();
    let replacement = if actual_scope.ends_with('0') {
        '1'
    } else {
        '0'
    };
    wrong_invocation_scope["intent"]["action"]["invocation"]["invocation_scope_instance_id"] = json!(
        format!("{}{}", &actual_scope[..actual_scope.len() - 1], replacement)
    );
    tampered.push((
        "wrong deterministic invocation scope",
        wrong_invocation_scope,
    ));

    let mut wrong_interface = valid_wire.clone();
    wrong_interface["intent"]["action"]["interface_version"] = json!("forged-v1");
    tampered.push(("wrong interface", wrong_interface));

    let mut wrong_timeout = valid_wire.clone();
    wrong_timeout["intent"]["action"]["timeout_ms"] = json!(1);
    tampered.push(("wrong timeout", wrong_timeout));

    let mut wrong_outputs = valid_wire;
    wrong_outputs["intent"]["action"]["outputs"][0]["name"] = json!("forged_result");
    tampered.push(("wrong outputs", wrong_outputs));

    for (case, mut wire) in tampered {
        wire["intent_hash"] =
            serde_json::to_value(IntentHash::from_serializable(&wire["intent"]).unwrap()).unwrap();
        let action = serde_json::from_value::<PlannedSchedulerAction>(wire).unwrap();
        let error = repository
            .commit_scheduler_action(&fence, &action)
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            insight_agent_platform::engine::repository::REPOSITORY_DATA_INVALID,
            "{case}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM workflow_runs WHERE parent_run_id=$1"
            )
            .bind(parent_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            0,
            "{case}"
        );
    }

    let SchedulerQuiescence::WaitingForChildRun { child_run_id, .. } =
        drive(&repository, &linked, &fence).await
    else {
        panic!("parent did not reach its durable child wait")
    };
    let stored_input = sqlx::query_scalar::<_, Value>(
        "SELECT p.inline_value FROM workflow_runs r
         JOIN payloads p ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
         WHERE r.run_id=$1",
    )
    .bind(child_run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(
        stored_input,
        json!({"question": "exact child", "tone": "concise"})
    );

    cleanup(repository, control, admin, schema).await;
}

#[tokio::test]
async fn postgres_subflow_deadline_uses_child_policy_parent_cap_and_survives_restart() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let parent_plan = compile("deadline_parent_revision_v1", DEADLINE_PARENT_AGENT);
    let child_plan = compile("child_revision_v1", CHILD_V1_AGENT);
    let _unpinned_parent_fixture = versioned(
        "deadline_parent_definition",
        "deadline_parent_agent",
        "deadline_parent_deployment_v1",
        &parent_plan,
    );
    let child_versioned = versioned(
        "deadline_child_definition",
        "deadline_child_agent",
        "deadline_child_deployment_v1",
        &child_plan,
    );
    let parent_versioned = subflow_parent_versioned(
        "deadline_parent_definition",
        "deadline_parent_agent",
        "deadline_parent_deployment_v1",
        &parent_plan,
        &child_versioned,
    );
    let subflows = subflow_contract(&parent_plan, &child_plan, &child_versioned);
    let linked =
        LinkedPlan::link(&parent_plan, &DescriptorContractRegistry::new(), &subflows).unwrap();
    for revision in [&child_versioned, &parent_versioned] {
        repository.install_versioned_plan(revision).await.unwrap();
    }

    let policy_parent = RunId::new("run_pg_subflow_deadline_policy_parent").unwrap();
    let policy_child = start_parent_and_child_with_timeout(
        &repository,
        &control,
        &parent_versioned,
        &linked,
        &policy_parent,
        Duration::from_secs(10 * 60),
    )
    .await;
    let policy = sqlx::query_as::<_, (bool, bool, bool)>(
        "SELECT child.deadline_at < parent.deadline_at,
                child.deadline_at > clock_timestamp(),
                (event.safe_payload->>'run_deadline_at')::timestamptz = child.deadline_at
         FROM workflow_runs child
         JOIN workflow_runs parent ON parent.run_id=$1
         JOIN execution_events event ON event.run_id=child.run_id AND event.kind='run.created'
         WHERE child.run_id=$2",
    )
    .bind(policy_parent.as_str())
    .bind(policy_child.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(policy, (true, true, true));

    let capped_parent = RunId::new("run_pg_subflow_deadline_capped_parent").unwrap();
    let capped_child = start_parent_and_child_with_timeout(
        &repository,
        &control,
        &parent_versioned,
        &linked,
        &capped_parent,
        Duration::from_secs(2 * 60),
    )
    .await;
    let capped_deadline = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        "SELECT child.deadline_at FROM workflow_runs child
         JOIN workflow_runs parent ON parent.run_id=$1
         WHERE child.run_id=$2 AND child.deadline_at=parent.deadline_at",
    )
    .bind(capped_parent.as_str())
    .bind(capped_child.as_str())
    .fetch_one(&control)
    .await
    .unwrap();

    let restarted = PostgresDurableRepository::from_pool(control.clone());
    assert!(restarted.load_run(&capped_child).await.unwrap().is_some());
    assert_eq!(
        sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            "SELECT deadline_at FROM workflow_runs WHERE run_id=$1",
        )
        .bind(capped_child.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        capped_deadline
    );
    drop(restarted);
    cleanup(repository, control, admin, schema).await;
}

#[tokio::test]
async fn postgres_subflow_pins_exact_revision_and_isolates_parent_child_terminals_and_cancel() {
    let Some((repository, control, admin, schema)) = isolated_repository().await else {
        return;
    };
    let parent_plan = compile("parent_revision_v1", PARENT_AGENT);
    let child_v1_plan = compile("child_revision_v1", CHILD_V1_AGENT);
    let child_v2_plan = compile("child_revision_v2", CHILD_V2_AGENT);
    let _unpinned_parent_fixture = versioned(
        "parent_definition",
        "parent_agent",
        "parent_deployment_v1",
        &parent_plan,
    );
    let child_v1 = versioned(
        "child_definition",
        "child_agent",
        "child_deployment_v1",
        &child_v1_plan,
    );
    let child_v2 = versioned(
        "child_definition",
        "child_agent",
        "child_deployment_v2",
        &child_v2_plan,
    );
    let parent_versioned = subflow_parent_versioned(
        "parent_definition",
        "parent_agent",
        "parent_deployment_v1",
        &parent_plan,
        &child_v1,
    );
    let subflows = subflow_contract(&parent_plan, &child_v1_plan, &child_v1);
    let parent_linked =
        LinkedPlan::link(&parent_plan, &DescriptorContractRegistry::new(), &subflows).unwrap();
    let child_linked = LinkedPlan::link(
        &child_v1_plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    for revision in [&child_v1, &child_v2, &parent_versioned] {
        assert_eq!(
            repository.install_versioned_plan(revision).await.unwrap(),
            PlanInstallOutcome::Installed
        );
    }

    let success_parent = RunId::new("run_pg_subflow_parent_success").unwrap();
    let (success_parent_fence, success_child) = start_parent_and_child(
        &repository,
        &control,
        &parent_versioned,
        &parent_linked,
        &success_parent,
        "pg-parent-success-scheduler",
        "pg-parent-success-fence",
    )
    .await;
    let pinned = sqlx::query_as::<_, (String, String, String, String)>(
        "SELECT definition_revision_id,deployment_revision_id,plan_hash,binding_hash
         FROM workflow_runs WHERE run_id=$1",
    )
    .bind(success_child.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(pinned.0, child_v1.definition_revision_id().as_str());
    assert_eq!(pinned.1, child_v1.deployment_revision_id().as_str());
    assert_eq!(pinned.2, child_v1.plan_hash().as_str());
    assert_eq!(pinned.3, child_v1.binding_hash().as_str());
    assert_ne!(pinned.0, child_v2.definition_revision_id().as_str());
    assert_ne!(pinned.1, child_v2.deployment_revision_id().as_str());

    let success_child_fence = lease_run(
        &control,
        &success_child,
        "pg-child-success-scheduler",
        "pg-child-success-fence",
    )
    .await;
    assert_eq!(
        drive(&repository, &child_linked, &success_child_fence).await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_eq!(
        repository
            .load_run(&success_parent)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::Active
    );
    assert_eq!(
        drive(&repository, &parent_linked, &success_parent_fence).await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_subflow_parent_drained(&control, &success_parent, "settled", "settled").await;

    let cancel_parent = RunId::new("run_pg_subflow_parent_cancel").unwrap();
    let (cancel_parent_fence, cancel_child) = start_parent_and_child(
        &repository,
        &control,
        &parent_versioned,
        &parent_linked,
        &cancel_parent,
        "pg-parent-cancel-scheduler",
        "pg-parent-cancel-fence",
    )
    .await;
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs
             SET lifecycle='terminating',admission_state='draining',
                 termination_intent_reason='cancelled',termination_intent_transition_key=$1,
                 termination_intent_at=CURRENT_TIMESTAMP,
                 projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=$2 AND lifecycle='active'",
        )
        .bind(key("cancel-parent", &cancel_parent).as_str())
        .bind(cancel_parent.as_str())
        .execute(&control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    assert!(matches!(
        drive(&repository, &parent_linked, &cancel_parent_fence).await,
        SchedulerQuiescence::WaitingForChildRun { .. }
    ));
    assert_eq!(
        repository
            .load_run(&cancel_child)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::Terminating
    );
    assert_eq!(
        repository
            .load_run(&cancel_parent)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::Terminating
    );

    let cancel_child_fence = lease_run(
        &control,
        &cancel_child,
        "pg-child-cancel-scheduler",
        "pg-child-cancel-fence",
    )
    .await;
    assert_eq!(
        drive(&repository, &child_linked, &cancel_child_fence).await,
        SchedulerQuiescence::RunCancelled
    );
    assert_eq!(
        repository
            .load_run(&cancel_parent)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::Terminating
    );
    assert_eq!(
        drive(&repository, &parent_linked, &cancel_parent_fence).await,
        SchedulerQuiescence::RunCancelled
    );
    assert_eq!(
        repository
            .load_run(&cancel_child)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::Cancelled
    );
    assert_subflow_parent_drained(&control, &cancel_parent, "cancelled", "cancelled").await;

    cleanup(repository, control, admin, schema).await;
}
