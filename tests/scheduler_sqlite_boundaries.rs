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
            drive_scheduler_once, ActivationDurableRepository, CreateRunCommand, DurableRepository,
            FencedSchedulerRunCommand, FireTimerCommand, NoSchedulerCrash, PlanInstallOutcome,
            ReceiveSignalCommand, ResolveSignalCommand, RuntimeIngressDurableRepository,
            SchedulerDriveOutcome, SchedulerDurableRepository, SqliteDurableRepository,
            TimerFireAuthority, VersionedPlan,
        },
        DefinitionRevisionId, DeploymentRevisionId, ExecutionRevisionPin, IntentHash,
        PlannedSchedulerAction, RunId, RunLifecycle, RunTerminalFact, RuntimeValue,
        SchedulerAction, SchedulerDecision, SchedulerPlanner, SchedulerQuiescence, TransitionKey,
        TransitionOutcome,
    },
};
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};

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

const TIMER_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: pause
      wait: {duration_ms: 1}
    - return: timer-done
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

const FAILURE_PARENT_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: child
      type: call
      definition_revision: child_failure_revision_v1
      interface_version: child-v1
      input: {question: $question}
      response: string
    - return: $child
"#;

const FAILURE_CHILD_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
errors:
  rejected:
    category: workflow
    code: CHILD_REJECTED
    public_message: child rejected the request
inputs: {question: string}
output: string
workflow:
  steps:
    - raise: rejected
"#;

const AUTHORED_RAISE_CATCH_AGENT: &str = r#"api_version: insight.agent/v1
kind: agent
errors:
  rejected:
    category: workflow
    code: REJECTED
    public_message: rejected
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
  rejected:
    category: workflow
    code: REJECTED
    public_message: rejected
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
        "scheduler.sqlite.boundary.e2e.v1",
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
        json!({"format": "scheduler-sqlite-boundary-e2e"}),
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
        json!({"format": "scheduler-sqlite-boundary-e2e"}),
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

async fn repository_and_control(
    name: &str,
) -> (tempfile::TempDir, SqliteDurableRepository, SqlitePool) {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join(format!("{name}.sqlite"));
    database::provision_sqlite_database(&database).await;
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    (directory, repository, control)
}

async fn create_run(
    repository: &SqliteDurableRepository,
    versioned: &VersionedPlan,
    run_id: &RunId,
    input: serde_json::Value,
) {
    assert!(matches!(
        repository
            .create_run(
                key("create", run_id),
                CreateRunCommand::new(run_id.clone(), versioned, input).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
}

async fn lease_run(
    control: &SqlitePool,
    run_id: &RunId,
    owner: &str,
    token: &str,
) -> FencedSchedulerRunCommand {
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs
             SET lifecycle=CASE WHEN lifecycle='created' THEN 'active' ELSE lifecycle END,
                 started_at=COALESCE(started_at,CURRENT_TIMESTAMP),
                 scheduler_lease_epoch=1,scheduler_lease_owner=?,scheduler_fencing_token=?,
                 scheduler_lease_expires_at=datetime('now','+1 hour'),
                 scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND lifecycle IN ('created','active','waiting','terminating')",
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
    repository: &SqliteDurableRepository,
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
                    "durable boundary step {step} failed: {error:?}; next decision={decision:?}; run_input={:?}; expected_input={:?}",
                    facts.run_input(),
                    linked
                        .index()
                        .metadata()
                        .input_contract()
                        .run_type()
                        .unwrap(),
                );
            }
        }
    }
    panic!("scheduler exhausted the test action budget")
}

#[tokio::test]
async fn sqlite_authored_try_exits_catch_finalize_and_apply_precedence_durably() {
    let (_directory, repository, control) =
        repository_and_control("authored-structured-exits").await;
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
        let revision = format!("sqlite_authored_{suffix}_revision_v1");
        let plan = compile(&revision, source);
        let descriptors = DescriptorContractRegistry::new();
        let subflows = SubflowContractRegistry::new();
        let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
        let deployed = versioned(
            &format!("sqlite_authored_{suffix}_definition"),
            &format!("sqlite_authored_{suffix}_agent"),
            &format!("sqlite_authored_{suffix}_deployment_v1"),
            &plan,
        );
        repository.install_versioned_plan(&deployed).await.unwrap();
        let run_id = RunId::new(format!("run_sqlite_authored_{suffix}")).unwrap();
        create_run(&repository, &deployed, &run_id, json!({})).await;
        let fence = lease_run(
            &control,
            &run_id,
            &format!("sqlite-authored-{suffix}-owner"),
            &format!("sqlite-authored-{suffix}-fence"),
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
                "SELECT fact_payload FROM scheduler_checkpoints
                 WHERE run_id=? AND checkpoint_kind='planned_action'
                   AND fact_payload LIKE '%transition_error_boundary%'
                 ORDER BY scheduler_projection_version",
            )
            .bind(run_id.as_str())
            .fetch_all(&control)
            .await
            .unwrap()
            .join("\n");
            assert!(phases.contains("\"phase\":\"finalizer\""));
            assert!(phases.contains("\"phase\":\"completed\""));
        }
    }
}

#[tokio::test]
async fn sqlite_control_termination_runs_durable_finalizer_before_exact_terminal() {
    let (_directory, repository, control) =
        repository_and_control("control-termination-finalizer").await;
    let plan = compile("finalizer_revision_v1", FINALIZER_AGENT);
    let linked = LinkedPlan::link(
        &plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    let deployed = versioned(
        "finalizer_definition",
        "finalizer_agent",
        "finalizer_deployment_v1",
        &plan,
    );
    repository.install_versioned_plan(&deployed).await.unwrap();

    for (suffix, reason, lifecycle, expected) in [
        (
            "cancel",
            "cancelled",
            RunLifecycle::Cancelled,
            SchedulerQuiescence::RunCancelled,
        ),
        (
            "timeout",
            "timed_out",
            RunLifecycle::TimedOut,
            SchedulerQuiescence::RunFailed,
        ),
        (
            "interrupt",
            "interrupted",
            RunLifecycle::Interrupted,
            SchedulerQuiescence::RunFailed,
        ),
    ] {
        let run_id = RunId::new(format!("run_sqlite_finalizer_{suffix}")).unwrap();
        create_run(&repository, &deployed, &run_id, json!({})).await;
        let fence = lease_run(
            &control,
            &run_id,
            &format!("finalizer-owner-{suffix}"),
            &format!("finalizer-fence-{suffix}"),
        )
        .await;
        assert!(matches!(
            drive(&repository, &linked, &fence).await,
            SchedulerQuiescence::WaitingForWait { .. }
        ));

        let intent_key = key(&format!("terminate-{suffix}"), &run_id);
        assert_eq!(
            sqlx::query(
                "UPDATE workflow_runs
                 SET lifecycle='terminating',admission_state='draining',
                     termination_intent_reason=?,termination_intent_transition_key=?,
                     termination_intent_at=CURRENT_TIMESTAMP,
                     projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND lifecycle='active' AND termination_intent_reason IS NULL",
            )
            .bind(reason)
            .bind(intent_key.as_str())
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
            panic!("termination finalizer did not reach its durable timer")
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
        assert!(matches!(
            repository
                .fire_timer(
                    key(&format!("fire-finalizer-{suffix}"), &run_id),
                    FireTimerCommand::new(run_id.clone(), timer_id, None),
                )
                .await
                .unwrap(),
            TransitionOutcome::Committed {
                result: TimerFireAuthority::WaitResolved(_)
            }
        ));
        assert_eq!(drive(&repository, &linked, &fence).await, expected);
        assert_eq!(
            repository
                .load_run(&run_id)
                .await
                .unwrap()
                .unwrap()
                .lifecycle(),
            lifecycle
        );
        assert_eq!(drive(&repository, &linked, &fence).await, expected);

        let phases = sqlx::query_scalar::<_, String>(
            "SELECT fact_payload FROM scheduler_checkpoints
             WHERE run_id=? AND checkpoint_kind='planned_action'
               AND fact_payload LIKE '%transition_error_boundary%'
             ORDER BY scheduler_projection_version",
        )
        .bind(run_id.as_str())
        .fetch_all(&control)
        .await
        .unwrap()
        .join("\n");
        assert!(phases.contains("\"phase\":\"finalizer\""));
        assert!(phases.contains("\"phase\":\"completed\""));
        assert!(phases.contains(&format!("\"reason\":\"{reason}\"")));
        let finalizer_activations: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM node_activations WHERE run_id=? AND node_id='final_audit'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(finalizer_activations, 1);
    }
}

#[tokio::test]
async fn sqlite_timeout_allows_finalizer_subflow_then_replays_original_terminal() {
    let (_directory, repository, control) =
        repository_and_control("timeout-finalizer-subflow").await;
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

    let parent_run = RunId::new("run_sqlite_timeout_finalizer_subflow").unwrap();
    create_run(&repository, &parent_versioned, &parent_run, json!({})).await;
    let parent_fence = lease_run(
        &control,
        &parent_run,
        "sqlite-finalizer-subflow-parent",
        "sqlite-finalizer-subflow-parent-fence",
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
                 termination_intent_reason='timed_out',termination_intent_transition_key=?,
                 termination_intent_at=CURRENT_TIMESTAMP,
                 projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND lifecycle='active' AND termination_intent_reason IS NULL",
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
        "sqlite-finalizer-subflow-child",
        "sqlite-finalizer-subflow-child-fence",
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
             WHERE run_id=? AND child_run_id=?",
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
        assert_eq!(
            drive(&repository, &parent_linked, &parent_fence).await,
            SchedulerQuiescence::RunFailed
        );
    }
}

#[tokio::test]
async fn sqlite_wait_signal_and_timeout_share_one_durable_first_winner() {
    let plan = wait_plan();
    let linked = LinkedPlan::link(
        &plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    let deployed = versioned("wait_definition", "wait_agent", "wait_deployment_v1", &plan);
    let (_directory, repository, control) = repository_and_control("wait-first-winner").await;
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    // Signal commits first; the already-due timer is atomically cancelled.
    let signal_run = RunId::new("run_wait_signal_wins").unwrap();
    create_run(&repository, &deployed, &signal_run, json!({})).await;
    let signal_fence = lease_run(
        &control,
        &signal_run,
        "wait-signal-scheduler",
        "wait-signal-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &signal_fence).await
    else {
        panic!("signal fixture did not reach its durable wait")
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
    let timer_id = registration.timer_id().unwrap().clone();
    assert!(matches!(
        repository
            .receive_signal(
                ReceiveSignalCommand::new(
                    signal_run.clone(),
                    signal_id.clone(),
                    "message-signal-wins",
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
            "SELECT COUNT(*) FROM execution_events WHERE run_id=? AND kind='timer.late'",
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
            "SELECT winner_kind FROM scheduler_wait_registrations WHERE run_id=?",
        )
        .bind(signal_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "signal"
    );

    // A pending signal does not reserve victory: the due timer can commit
    // first, after which the signal resolution is fenced by activation CAS.
    let timer_run = RunId::new("run_wait_timer_wins").unwrap();
    create_run(&repository, &deployed, &timer_run, json!({})).await;
    let timer_fence = lease_run(
        &control,
        &timer_run,
        "wait-timer-scheduler",
        "wait-timer-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &timer_fence).await
    else {
        panic!("timer fixture did not reach its durable wait")
    };
    let deadline_delta_ms: i64 = sqlx::query_scalar(
        "SELECT due_at_ms - CAST(strftime('%s','now') AS INTEGER) * 1000
         FROM scheduler_wait_registrations WHERE run_id=? AND wait_id=?",
    )
    .bind(timer_run.as_str())
    .bind(wait_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert!(
        (-2_000..=1_001).contains(&deadline_delta_ms),
        "timer deadline must derive from the SQLite clock, delta={deadline_delta_ms}ms"
    );
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
                    "message-timer-wins",
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
        TransitionOutcome::Committed {
            result: TimerFireAuthority::ActivationTimedOut { .. }
        }
    ));
    let late_signal_key = key("late-signal", &timer_run);
    let late_signal_command = ResolveSignalCommand::new(
        timer_run.clone(),
        activation_id.clone(),
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
            "SELECT COUNT(*) FROM execution_events WHERE run_id=? AND kind='signal.late'",
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
            "SELECT winner_kind FROM scheduler_wait_registrations WHERE run_id=?",
        )
        .bind(timer_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "timer"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_activations WHERE run_id=? AND activation_id=?",
        )
        .bind(timer_run.as_str())
        .bind(activation_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "timed_out"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT signal_state FROM signals_inbox WHERE run_id=? AND signal_id=?",
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

    // Race both contenders against the same durable wait. The losing request
    // commits exactly one independent late audit event and exact replay stays
    // a no-op StateConflict.
    let race_run = RunId::new("run_wait_concurrent_first_winner").unwrap();
    create_run(&repository, &deployed, &race_run, json!({})).await;
    let race_fence = lease_run(
        &control,
        &race_run,
        "wait-race-scheduler",
        "wait-race-fence",
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
                "message-concurrent-race",
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
             WHERE run_id=? AND kind IN ('signal.late','timer.late')",
        )
        .bind(race_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn sqlite_late_audit_is_atomic_across_fault_and_repository_restart() {
    let plan = wait_plan();
    let linked = LinkedPlan::link(
        &plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    let deployed = versioned(
        "wait_late_atomic_definition",
        "wait_late_atomic_agent",
        "wait_late_atomic_deployment_v1",
        &plan,
    );
    let (directory, repository, control) =
        repository_and_control("wait-late-audit-atomicity").await;
    let database = directory.path().join("wait-late-audit-atomicity.sqlite");
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    // Signal is already the durable winner. Fault before the timer.late row is
    // inserted: the losing command must not consume a sequence or leave a
    // partial transition. A reconstructed repository can retry it exactly.
    let signal_run = RunId::new("run_wait_timer_late_atomic_fault").unwrap();
    create_run(&repository, &deployed, &signal_run, json!({})).await;
    let signal_fence = lease_run(
        &control,
        &signal_run,
        "late-atomic-signal-scheduler",
        "late-atomic-signal-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &signal_fence).await
    else {
        panic!("timer-late fixture did not reach its durable wait")
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
                    "message-timer-late-atomic",
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
                key("resolve-signal-before-timer-late-fault", &signal_run),
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
    let signal_winner_event: String = sqlx::query_scalar(
        "SELECT consumed_event_id FROM signals_inbox
         WHERE run_id=? AND target_activation_id=?",
    )
    .bind(signal_run.as_str())
    .bind(activation_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let next_before_timer_late: i64 =
        sqlx::query_scalar("SELECT next_event_seq FROM workflow_runs WHERE run_id=?")
            .bind(signal_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_timer_late_before_insert
         BEFORE INSERT ON execution_events
         WHEN NEW.kind='timer.late'
         BEGIN
           SELECT RAISE(ABORT, 'fault before timer late commit');
         END",
    )
    .execute(&control)
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(repository.reconcile_wait_late_audits(128).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events WHERE run_id=? AND kind='timer.late'",
        )
        .bind(signal_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_event_seq FROM workflow_runs WHERE run_id=?",)
            .bind(signal_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        next_before_timer_late
    );
    sqlx::query("DROP TRIGGER fail_timer_late_before_insert")
        .execute(&control)
        .await
        .unwrap();
    drop(repository);
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(repository.reconcile_wait_late_audits(128).await.unwrap(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT causation_event_id FROM execution_events
             WHERE run_id=? AND kind='timer.late'",
        )
        .bind(signal_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        signal_winner_event
    );
    drop(repository);
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(repository.reconcile_wait_late_audits(128).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events WHERE run_id=? AND kind='timer.late'",
        )
        .bind(signal_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );

    // Timer is the durable winner. The AFTER trigger fails after signal.late
    // and its sequence were staged but before commit; both must roll back.
    // Reopening after the successful retry also models a crash immediately
    // after commit and proves exact replay remains count=1.
    let timer_run = RunId::new("run_wait_signal_late_atomic_fault").unwrap();
    create_run(&repository, &deployed, &timer_run, json!({})).await;
    let timer_fence = lease_run(
        &control,
        &timer_run,
        "late-atomic-timer-scheduler",
        "late-atomic-timer-fence",
    )
    .await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &timer_fence).await
    else {
        panic!("signal-late fixture did not reach its durable wait")
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
                    "message-signal-late-atomic",
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
                key("resolve-timer-before-signal-late-fault", &timer_run),
                FireTimerCommand::new(timer_run.clone(), timer_id, None),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed {
            result: TimerFireAuthority::ActivationTimedOut { .. }
        }
    ));
    let timer_winner_event: String = sqlx::query_scalar(
        "SELECT fired_event_id FROM timers
         WHERE run_id=? AND activation_id=? AND timer_kind='wait'",
    )
    .bind(timer_run.as_str())
    .bind(activation_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let next_before_signal_late: i64 =
        sqlx::query_scalar("SELECT next_event_seq FROM workflow_runs WHERE run_id=?")
            .bind(timer_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_signal_late_after_insert
         AFTER INSERT ON execution_events
         WHEN NEW.kind='signal.late'
         BEGIN
           SELECT RAISE(ABORT, 'fault after signal late insert before commit');
         END",
    )
    .execute(&control)
    .await
    .unwrap();
    assert!(repository.reconcile_wait_late_audits(128).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events WHERE run_id=? AND kind='signal.late'",
        )
        .bind(timer_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT next_event_seq FROM workflow_runs WHERE run_id=?",)
            .bind(timer_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        next_before_signal_late
    );
    sqlx::query("DROP TRIGGER fail_signal_late_after_insert")
        .execute(&control)
        .await
        .unwrap();
    drop(repository);
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(repository.reconcile_wait_late_audits(128).await.unwrap(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT causation_event_id FROM execution_events
             WHERE run_id=? AND kind='signal.late'",
        )
        .bind(timer_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        timer_winner_event
    );
    drop(repository);
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(repository.reconcile_wait_late_audits(128).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_events WHERE run_id=? AND kind='signal.late'",
        )
        .bind(timer_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn sqlite_timer_only_wait_remains_a_successful_control_delay() {
    let plan = compile("timer_revision_v1", TIMER_AGENT);
    let linked = LinkedPlan::link(
        &plan,
        &DescriptorContractRegistry::new(),
        &SubflowContractRegistry::new(),
    )
    .unwrap();
    let deployed = versioned(
        "timer_definition",
        "timer_agent",
        "timer_deployment_v1",
        &plan,
    );
    let (_directory, repository, control) = repository_and_control("timer-only-success").await;
    assert_eq!(
        repository.install_versioned_plan(&deployed).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    let run_id = RunId::new("run_timer_only_success").unwrap();
    create_run(&repository, &deployed, &run_id, json!({})).await;
    let fence = lease_run(&control, &run_id, "timer-scheduler", "timer-fence").await;
    let SchedulerQuiescence::WaitingForWait {
        wait_id,
        activation_id,
    } = drive(&repository, &linked, &fence).await
    else {
        panic!("timer fixture did not reach its durable wait")
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
    assert!(matches!(
        repository
            .fire_timer(
                key("timer-only-fire", &run_id),
                FireTimerCommand::new(run_id.clone(), timer_id, None),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed {
            result: TimerFireAuthority::WaitResolved(_)
        }
    ));
    assert_eq!(
        drive(&repository, &linked, &fence).await,
        SchedulerQuiescence::RunSucceeded
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_activations WHERE run_id=? AND activation_id=?",
        )
        .bind(run_id.as_str())
        .bind(activation_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "succeeded"
    );
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
    repository: &SqliteDurableRepository,
    control: &SqlitePool,
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
    repository: &SqliteDurableRepository,
    control: &SqlitePool,
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
    control: &SqlitePool,
    parent_run_id: &RunId,
    invocation_scope_lifecycle: &str,
    root_lifecycle: &str,
) {
    let invocation = sqlx::query_as::<_, (String, String, String)>(
        "SELECT i.invocation_state,s.lifecycle,s.admission_state
         FROM scheduler_subflow_invocations i
         JOIN scope_instances s ON s.run_id=i.run_id
              AND s.scope_instance_id=i.invocation_scope_instance_id
         WHERE i.run_id=?",
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
         FROM scope_instances WHERE run_id=? AND is_root=1",
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
            "SELECT COUNT(*) FROM node_activations WHERE run_id=? AND lifecycle IN
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
async fn sqlite_subflow_persists_normalized_child_input_without_collapsing_absence() {
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
    let (_directory, repository, control) =
        repository_and_control("subflow-normalized-input").await;
    for revision in [&child_versioned, &parent_versioned] {
        repository.install_versioned_plan(revision).await.unwrap();
    }

    let parent_run = RunId::new("run_subflow_normalized_input_parent").unwrap();
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
        "normalized-input-parent",
        "normalized-input-parent-fence",
    )
    .await;

    // Stop immediately before child creation, then prove the repository does
    // not trust a well-formed scheduler intent whose input skipped planner
    // normalization. The failed transaction must leave the original valid
    // action retryable under the same deterministic checkpoint.
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
                "SELECT COUNT(*) FROM workflow_runs WHERE parent_run_id=?"
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
    let stored_input = sqlx::query_scalar::<_, String>(
        "SELECT p.inline_value FROM workflow_runs r
         JOIN payloads p ON p.run_id=r.run_id AND p.payload_id=r.input_payload_id
         WHERE r.run_id=?",
    )
    .bind(child_run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&stored_input).unwrap(),
        json!({"question": "exact child", "tone": "concise"})
    );
}

#[tokio::test]
async fn sqlite_subflow_deadline_uses_child_policy_parent_cap_and_survives_restart() {
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
    let (directory, repository, control) = repository_and_control("subflow-deadlines").await;
    for revision in [&child_versioned, &parent_versioned] {
        repository.install_versioned_plan(revision).await.unwrap();
    }

    let policy_parent = RunId::new("run_subflow_deadline_policy_parent").unwrap();
    let policy_child = start_parent_and_child_with_timeout(
        &repository,
        &control,
        &parent_versioned,
        &linked,
        &policy_parent,
        Duration::from_secs(10 * 60),
    )
    .await;
    let policy = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT julianday(child.deadline_at) < julianday(parent.deadline_at),
                julianday(child.deadline_at) > julianday('now'),
                julianday(json_extract(event.safe_payload,'$.run_deadline_at')) =
                    julianday(child.deadline_at)
         FROM workflow_runs child
         JOIN workflow_runs parent ON parent.run_id=?
         JOIN execution_events event ON event.run_id=child.run_id AND event.kind='run.created'
         WHERE child.run_id=?",
    )
    .bind(policy_parent.as_str())
    .bind(policy_child.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(policy, (1, 1, 1));

    let capped_parent = RunId::new("run_subflow_deadline_capped_parent").unwrap();
    let capped_child = start_parent_and_child_with_timeout(
        &repository,
        &control,
        &parent_versioned,
        &linked,
        &capped_parent,
        Duration::from_secs(2 * 60),
    )
    .await;
    let capped_deadline = sqlx::query_scalar::<_, String>(
        "SELECT child.deadline_at FROM workflow_runs child
         JOIN workflow_runs parent ON parent.run_id=?
         WHERE child.run_id=? AND child.deadline_at=parent.deadline_at",
    )
    .bind(capped_parent.as_str())
    .bind(capped_child.as_str())
    .fetch_one(&control)
    .await
    .unwrap();

    let database = directory.path().join("subflow-deadlines.sqlite");
    control.close().await;
    drop(repository);
    let restarted = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert!(restarted.load_run(&capped_child).await.unwrap().is_some());
    let restarted_control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT deadline_at FROM workflow_runs WHERE run_id=?")
            .bind(capped_child.as_str())
            .fetch_one(&restarted_control)
            .await
            .unwrap(),
        capped_deadline
    );
}

#[tokio::test]
async fn sqlite_subflow_pins_exact_revision_and_isolates_parent_child_terminals_and_cancel() {
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
    let (_directory, repository, control) = repository_and_control("subflow-boundaries").await;
    for revision in [&child_v1, &child_v2, &parent_versioned] {
        assert_eq!(
            repository.install_versioned_plan(revision).await.unwrap(),
            PlanInstallOutcome::Installed
        );
    }

    // Child success is durable but does not mutate the parent terminal until
    // the parent scheduler observes and consumes that exact child outcome.
    let success_parent = RunId::new("run_subflow_parent_success").unwrap();
    let (success_parent_fence, success_child) = start_parent_and_child(
        &repository,
        &control,
        &parent_versioned,
        &parent_linked,
        &success_parent,
        "parent-success-scheduler",
        "parent-success-fence",
    )
    .await;
    let pinned = sqlx::query_as::<_, (String, String, String, String)>(
        "SELECT definition_revision_id,deployment_revision_id,plan_hash,binding_hash
         FROM workflow_runs WHERE run_id=?",
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
        "child-success-scheduler",
        "child-success-fence",
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

    // Parent cancellation first requests child cancellation. The child and
    // parent each require their own terminal scheduler commit.
    let cancel_parent = RunId::new("run_subflow_parent_cancel").unwrap();
    let (cancel_parent_fence, cancel_child) = start_parent_and_child(
        &repository,
        &control,
        &parent_versioned,
        &parent_linked,
        &cancel_parent,
        "parent-cancel-scheduler",
        "parent-cancel-fence",
    )
    .await;
    let cancel_child_fence = lease_run(
        &control,
        &cancel_child,
        "child-cancel-scheduler",
        "child-cancel-fence",
    )
    .await;
    assert!(matches!(
        drive_scheduler_once(
            &repository,
            &child_linked,
            &cancel_child_fence,
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerDriveOutcome::Applied(_)
    ));
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs
             SET lifecycle='terminating',admission_state='draining',
                 termination_intent_reason='cancelled',termination_intent_transition_key=?,
                 termination_intent_at=CURRENT_TIMESTAMP,
                 projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND lifecycle='active'",
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
}

#[tokio::test]
async fn sqlite_subflow_failure_requires_explicit_invocation_settlement() {
    let parent_plan = compile("failure_parent_revision_v1", FAILURE_PARENT_AGENT);
    let child_plan = compile("child_failure_revision_v1", FAILURE_CHILD_AGENT);
    let _unpinned_parent_fixture = versioned(
        "failure_parent_definition",
        "failure_parent_agent",
        "failure_parent_deployment_v1",
        &parent_plan,
    );
    let child_versioned = versioned(
        "failure_child_definition",
        "failure_child_agent",
        "failure_child_deployment_v1",
        &child_plan,
    );
    let parent_versioned = subflow_parent_versioned(
        "failure_parent_definition",
        "failure_parent_agent",
        "failure_parent_deployment_v1",
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
    let (_directory, repository, control) = repository_and_control("subflow-failure").await;
    for revision in [&child_versioned, &parent_versioned] {
        assert_eq!(
            repository.install_versioned_plan(revision).await.unwrap(),
            PlanInstallOutcome::Installed
        );
    }

    let parent_run = RunId::new("run_subflow_parent_failure").unwrap();
    let (parent_fence, child_run) = start_parent_and_child(
        &repository,
        &control,
        &parent_versioned,
        &parent_linked,
        &parent_run,
        "parent-failure-scheduler",
        "parent-failure-fence",
    )
    .await;
    let child_fence = lease_run(
        &control,
        &child_run,
        "child-failure-scheduler",
        "child-failure-fence",
    )
    .await;
    assert_eq!(
        drive(&repository, &child_linked, &child_fence).await,
        SchedulerQuiescence::RunFailed
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT invocation_state FROM scheduler_subflow_invocations WHERE run_id=?",
        )
        .bind(parent_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "started"
    );
    assert_eq!(
        drive(&repository, &parent_linked, &parent_fence).await,
        SchedulerQuiescence::RunFailed
    );
    assert_subflow_parent_drained(&control, &parent_run, "settled", "settled").await;
}
#[path = "support/database.rs"]
mod database;
