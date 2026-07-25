use std::{collections::BTreeMap, time::Duration};

use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool, Row, SqlitePool};
use uuid::Uuid;

use insight_dsl::{compile_source, CompileOptions};
use insight_durable::scheduler_repository::adapter::SchedulerTaskFailureAdapter as _;
use insight_engine::plan::PortDirection;
use insight_engine::resource_policy::RetrievalPublicPolicy;
use insight_engine::retrieval::{
    deterministic_retrieval_id, FrozenRetrievalTarget, RetrievalCompletion,
};
use insight_engine::worker::{TaskExecutionResult, WorkerFailure, WorkerFailureClass};
use insight_engine::{
    ArtifactId, ArtifactRef, ContentHash, DefinitionRevisionId, DeploymentRevisionId,
    DescriptorConfigurationContract, DescriptorContract, DescriptorContractRegistry,
    DescriptorFieldContract, DescriptorValueSchema, EffectEvidence, EffectIdempotency,
    LeafTaskKind, LinkedPlan, NodeKind, Plan, PlanError, RunId, RunLifecycle, RuntimeValue,
    SchedulerDecision, SchedulerPlanner, SchedulerPlanningFailure, SchedulerQuiescence,
    SubflowContractRegistry, TransitionKey, TransitionOutcome, VersionTag, WorkerCancellation,
    WorkerContract, WorkerEffectClass, WorkerEffectPolicy, WorkerInputPortContract,
};

use super::{
    ArtifactDurableRepository, ClaimSchedulerRunCommand, CreateRunCommand, DurableRepository,
    FencedSchedulerRunCommand, PlanInstallOutcome, PostgresDurableRepository, RepositoryError,
    RepositoryErrorExt as _, SchedulerDurableRepository, SchedulerFailureDisposition,
    SchedulerLeaseRepository, SchedulerTaskClaim, SchedulerTaskCommitOutcome, SchedulerTaskFailure,
    SchedulerTaskHeartbeatOutcome, SchedulerTaskOutcome, SchedulerTaskSuccess,
    SqliteDurableRepository, StageArtifactCommand, StorageLocator, VerifyArtifactCommand,
    VersionedPlan, REPOSITORY_DATA_INVALID,
};

const SOURCE: &str = r#"api_version: insight.agent/v1
kind: agent
types:
  SearchOutput:
    fields:
      answer: string
inputs:
  question: string
output: SearchOutput
workflow:
  steps:
    - type: retrieval
      id: search
      retrieval: fixture.search
      publish: true
      inputs:
        query: $question
      response: SearchOutput
    - return: $search
"#;

const FAIL_AFTER_RETRIEVAL_SOURCE: &str = r#"api_version: insight.agent/v1
kind: agent
types:
  SearchOutput:
    fields:
      answer: string
inputs:
  question: string
output: SearchOutput
workflow:
  steps:
    - type: retrieval
      id: search
      retrieval: fixture.search
      publish: true
      inputs:
        query: $question
      response: SearchOutput
    - type: action
      id: fail_after_search
      call: fixture.fail_after_search
      response: string
    - return: $search
"#;

#[derive(Debug, Clone, Copy)]
enum PublicMode {
    Private,
    Query,
    Result,
    Both,
    Artifact,
}

fn public_policy(mode: PublicMode) -> RetrievalPublicPolicy {
    let result_schema = match mode {
        PublicMode::Private | PublicMode::Query => None,
        PublicMode::Result | PublicMode::Both => Some(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {},
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "title": {"type": "string"},
                "metadata": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            },
            "required": ["id"],
            "additionalProperties": false
        })),
        PublicMode::Artifact => Some(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {},
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "metadata": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                },
                "artifact": {
                    "type": "object",
                    "properties": {
                        "artifact_id": {"type": "string"},
                        "content_hash": {"type": "string"},
                        "size_bytes": {"type": "integer", "minimum": 0},
                        "media_type": {"type": ["string", "null"]}
                    },
                    "required": ["artifact_id", "content_hash", "size_bytes", "media_type"],
                    "additionalProperties": false
                }
            },
            "required": ["id", "metadata", "artifact"],
            "additionalProperties": false
        })),
    };
    RetrievalPublicPolicy {
        query: matches!(
            mode,
            PublicMode::Query | PublicMode::Both | PublicMode::Artifact
        ),
        result_schema,
    }
}

struct TestDeployment {
    plan: Plan,
    descriptors: DescriptorContractRegistry,
    versioned_plan: VersionedPlan,
}

impl TestDeployment {
    fn plan(&self) -> &Plan {
        &self.plan
    }

    fn versioned_plan(&self) -> &VersionedPlan {
        &self.versioned_plan
    }

    fn linked_plan(&self) -> Result<LinkedPlan<'_>, PlanError> {
        LinkedPlan::link(
            &self.plan,
            &self.descriptors,
            &SubflowContractRegistry::new(),
        )
    }
}

fn deployed(mode: PublicMode, timeout: Duration, suffix: &str) -> TestDeployment {
    deployed_from_source(mode, timeout, suffix, SOURCE)
}

fn deployed_from_source(
    mode: PublicMode,
    timeout: Duration,
    suffix: &str,
    source: &str,
) -> TestDeployment {
    let definition = format!("retrieval_durable_{suffix}");
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new(definition.clone()).unwrap(),
            "retrieval-durable.yaml",
            source,
        ),
    )
    .unwrap();
    let timeout_ms = u64::try_from(timeout.as_millis()).unwrap();
    let policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::ReadOnly,
        EffectIdempotency::Idempotent,
        1,
        0,
        0,
        timeout_ms,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let retrieval_public = public_policy(mode);
    let retrieval_binding = json!({
        "adapter": "native_retrieval",
        "retrieval_id": "fixture.search",
        "retrieval_version": "1.0.0",
        "descriptor_hash": "a".repeat(64),
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": false
        },
        "output_schema": {
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
            "additionalProperties": false
        },
        "query_field": "query",
        "effect": "read_only",
        "idempotency": "idempotent",
        "cancellation": "cooperative",
        "required_capabilities": [],
        "publish": true,
        "public": retrieval_public.clone(),
        "effective_public_policy": retrieval_public,
    });
    let mut descriptors = DescriptorContractRegistry::new();
    for node in plan.nodes() {
        let (kind, descriptor, binding) = match node.kind() {
            NodeKind::RetrievalTask(descriptor) => (
                LeafTaskKind::Retrieval,
                descriptor,
                retrieval_binding.clone(),
            ),
            NodeKind::ActionTask(descriptor) => (LeafTaskKind::Action, descriptor, json!({})),
            _ => continue,
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
            .register_for_node(
                node.id().clone(),
                DescriptorContract::new(
                    descriptor.implementation.clone(),
                    descriptor.descriptor_version.clone(),
                    configuration,
                    WorkerContract::new(kind, VersionTag::new("1.0.0").unwrap(), inputs, outputs)
                        .with_effect_policy(policy.clone()),
                )
                .with_deployment_binding(binding)
                .unwrap(),
            )
            .unwrap();
    }
    let versioned_plan = VersionedPlan::from_verified_plan(
        format!("retrieval-{suffix}"),
        format!("retrieval-{suffix}"),
        "retrieval durable fixture",
        DeploymentRevisionId::new(format!("retrieval_deployment_{suffix}")).unwrap(),
        "expression-3.0.0",
        json!({"format": "structured", "source": source}),
        &plan,
        json!({"fixture": "descriptor-v1"}),
        json!({"fixture": "retrieval-binding-v1"}),
        json!({"fixture": "worker-v1"}),
    )
    .unwrap();
    TestDeployment {
        plan,
        descriptors,
        versioned_plan,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SchedulerDriveOutcome {
    Applied,
    Quiescent(SchedulerQuiescence),
    Fenced,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SchedulerRecoveryOutcome {
    Quiescent(SchedulerQuiescence),
    Fenced,
    ActionBudgetExhausted,
}

async fn drive_scheduler_once<R>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
) -> Result<SchedulerDriveOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
{
    let planner = SchedulerPlanner::new(linked);
    let decision = match repository.load_scheduler_facts(fence.run_id()).await {
        Ok(facts) => match planner.plan(&facts) {
            Ok(decision) => decision,
            Err(error) => SchedulerDecision::Action(Box::new(
                planner
                    .fail_closed_action(&facts, &error)
                    .map_err(|_| RepositoryError::invalid_data())?,
            )),
        },
        Err(error) if error.code() == REPOSITORY_DATA_INVALID => {
            let projection = repository
                .load_run(fence.run_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            if projection.lifecycle().is_terminal() {
                let quiescence = match projection.lifecycle() {
                    RunLifecycle::Succeeded => SchedulerQuiescence::RunSucceeded,
                    RunLifecycle::Cancelled => SchedulerQuiescence::RunCancelled,
                    _ => SchedulerQuiescence::RunFailed,
                };
                return Ok(SchedulerDriveOutcome::Quiescent(quiescence));
            }
            SchedulerDecision::Action(Box::new(
                planner
                    .fail_closed_action_at(
                        fence.run_id(),
                        projection.projection_version(),
                        SchedulerPlanningFailure::FactInconsistent,
                    )
                    .map_err(|_| RepositoryError::invalid_data())?,
            ))
        }
        Err(error) => return Err(error),
    };
    let SchedulerDecision::Action(action) = decision else {
        let SchedulerDecision::Quiescent(quiescence) = decision else {
            unreachable!("SchedulerDecision is closed")
        };
        return Ok(SchedulerDriveOutcome::Quiescent(quiescence));
    };
    match repository.commit_scheduler_action(fence, &action).await? {
        TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {
            Ok(SchedulerDriveOutcome::Applied)
        }
        TransitionOutcome::StaleLease | TransitionOutcome::StateConflict => {
            Ok(SchedulerDriveOutcome::Fenced)
        }
    }
}

async fn drive_scheduler_until_quiescent<R>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    max_actions: u32,
) -> Result<SchedulerRecoveryOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
{
    if max_actions == 0 {
        return Err(RepositoryError::invalid_configuration());
    }
    for _ in 0..max_actions {
        match drive_scheduler_once(repository, linked, fence).await? {
            SchedulerDriveOutcome::Applied => {}
            SchedulerDriveOutcome::Quiescent(quiescence) => {
                return Ok(SchedulerRecoveryOutcome::Quiescent(quiescence));
            }
            SchedulerDriveOutcome::Fenced => return Ok(SchedulerRecoveryOutcome::Fenced),
        }
    }
    Ok(SchedulerRecoveryOutcome::ActionBudgetExhausted)
}

fn transition(label: &str, run_id: &RunId) -> TransitionKey {
    TransitionKey::derive("retrieval.durable.test.v1", &[label, run_id.as_str()]).unwrap()
}

async fn prepare_sqlite(
    repository: &SqliteDurableRepository,
    control: &SqlitePool,
    deployment: &TestDeployment,
    run_id: &RunId,
) -> (SchedulerTaskClaim, FencedSchedulerRunCommand) {
    let fence = prepare_sqlite_waiting(repository, control, deployment, run_id).await;
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
    let claim = match repository
        .heartbeat_scheduler_task(&claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(claim) => claim,
        other => panic!("retrieval SQLite claim did not renew: {other:?}"),
    };
    (claim, fence)
}

async fn prepare_sqlite_waiting(
    repository: &SqliteDurableRepository,
    control: &SqlitePool,
    deployment: &TestDeployment,
    run_id: &RunId,
) -> FencedSchedulerRunCommand {
    assert_eq!(
        repository
            .install_versioned_plan(deployment.versioned_plan())
            .await
            .unwrap(),
        PlanInstallOutcome::Installed
    );
    assert!(matches!(
        repository
            .create_run(
                transition("create", run_id),
                CreateRunCommand::new(
                    run_id.clone(),
                    deployment.versioned_plan(),
                    json!({"question": "WBC"}),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let owner = format!("scheduler-{}", run_id.as_str());
    let fence_token = format!("fence-{}", run_id.as_str());
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
                scheduler_lease_epoch=1,scheduler_lease_owner=?,scheduler_fencing_token=?,
                scheduler_lease_expires_at=datetime('now','+1 hour'),
                scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND lifecycle='created'",
    )
    .bind(&owner)
    .bind(&fence_token)
    .bind(run_id.as_str())
    .execute(control)
    .await
    .unwrap();
    let fence = FencedSchedulerRunCommand::new(run_id.clone(), owner, 1, fence_token).unwrap();
    let linked = deployment.linked_plan().unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, &linked, &fence, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    fence
}

async fn prepare_postgres(
    repository: &PostgresDurableRepository,
    control: &PgPool,
    deployment: &TestDeployment,
    run_id: &RunId,
) -> (SchedulerTaskClaim, FencedSchedulerRunCommand) {
    assert_eq!(
        repository
            .install_versioned_plan(deployment.versioned_plan())
            .await
            .unwrap(),
        PlanInstallOutcome::Installed
    );
    assert!(matches!(
        repository
            .create_run(
                transition("create", run_id),
                CreateRunCommand::new(
                    run_id.clone(),
                    deployment.versioned_plan(),
                    json!({"question": "WBC"}),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP WHERE run_id=$1 AND lifecycle='created'",
    )
    .bind(run_id.as_str())
    .execute(control)
    .await
    .unwrap();
    let lease = repository
        .claim_scheduler_run(
            transition("scheduler-claim", run_id),
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
    let linked = deployment.linked_plan().unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, &linked, &fence, 32)
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
    let claim = match repository
        .heartbeat_scheduler_task(&claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(claim) => claim,
        other => panic!("retrieval PostgreSQL claim did not renew: {other:?}"),
    };
    (claim, fence)
}

fn success(claim: &SchedulerTaskClaim, query: &str, title: &str) -> SchedulerTaskOutcome {
    let request = claim.envelope().request();
    let target =
        FrozenRetrievalTarget::from_deployment_binding(request.deployment_binding()).unwrap();
    let retrieval_id = deterministic_retrieval_id(claim.run_id(), claim.activation_id());
    let candidate = json!([{"id": "doc_1", "title": title, "metadata": {}}]);
    let public = target
        .public_projection()
        .unwrap()
        .project_validated_completed(retrieval_id, &json!({"query": query}), Some(&candidate))
        .unwrap();
    let completion = RetrievalCompletion::new(public).unwrap();
    let output = request.outputs().first().unwrap();
    SchedulerTaskOutcome::Succeeded(
        SchedulerTaskSuccess::inline(
            TaskExecutionResult::new(
                std::collections::BTreeMap::from([(
                    output.port_id().clone(),
                    RuntimeValue::new(json!({"answer": "safe"})).unwrap(),
                )]),
                EffectEvidence::Committed,
            )
            .with_retrieval_completion(completion),
        )
        .unwrap(),
    )
}

fn artifact_success(claim: &SchedulerTaskClaim, artifact: &ArtifactRef) -> SchedulerTaskOutcome {
    let request = claim.envelope().request();
    let target =
        FrozenRetrievalTarget::from_deployment_binding(request.deployment_binding()).unwrap();
    let retrieval_id = deterministic_retrieval_id(claim.run_id(), claim.activation_id());
    let candidate = json!([{
        "id": "doc_artifact",
        "metadata": {},
        "artifact": serde_json::to_value(artifact).unwrap(),
    }]);
    let public = target
        .public_projection()
        .unwrap()
        .project_validated_completed(retrieval_id, &json!({"query": "WBC"}), Some(&candidate))
        .unwrap();
    let output = request.outputs().first().unwrap();
    SchedulerTaskOutcome::Succeeded(
        SchedulerTaskSuccess::inline(
            TaskExecutionResult::new(
                std::collections::BTreeMap::from([(
                    output.port_id().clone(),
                    RuntimeValue::new(json!({"answer": "safe"})).unwrap(),
                )]),
                EffectEvidence::Committed,
            )
            .with_retrieval_completion(RetrievalCompletion::new(public).unwrap()),
        )
        .unwrap(),
    )
}

fn artifact(label: &str, bytes: &[u8]) -> ArtifactRef {
    ArtifactRef::new(
        ArtifactId::new(format!("artifact_{label}")).unwrap(),
        ContentHash::from_bytes(bytes),
        u64::try_from(bytes.len()).unwrap(),
        Some("application/octet-stream".to_owned()),
    )
    .unwrap()
}

async fn stage_and_verify_sqlite_artifact(
    repository: &SqliteDurableRepository,
    run_id: &RunId,
    artifact: &ArtifactRef,
) {
    let locator = StorageLocator::new(format!(
        "content-addressed:v1/sha256/{}",
        artifact.artifact_id().as_str()
    ))
    .unwrap();
    assert!(matches!(
        repository
            .stage_artifact(StageArtifactCommand::new(
                run_id.clone(),
                artifact.clone(),
                locator,
                None,
            ))
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .verify_artifact(VerifyArtifactCommand::new(
                run_id.clone(),
                artifact.artifact_id().clone(),
                artifact.content_hash().clone(),
                artifact.size_bytes(),
            ))
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
}

#[tokio::test]
async fn sqlite_retrieval_success_is_atomic_race_safe_and_exactly_replayable() {
    let repository = SqliteDurableRepository::in_memory().await.unwrap();
    let control = repository.pool.clone();
    let deployment = deployed(PublicMode::Both, Duration::from_secs(30), "sqlite_race");
    let run_id = RunId::new("run_retrieval_sqlite_race").unwrap();
    let (claim, _) = prepare_sqlite(&repository, &control, &deployment, &run_id).await;
    let outcome = success(&claim, "WBC", "White blood cell guidance");

    let left_repository = repository.clone();
    let left_claim = claim.clone();
    let left_outcome = outcome.clone();
    let right_repository = repository.clone();
    let right_claim = claim.clone();
    let right_outcome = outcome.clone();
    let (left, right) = tokio::join!(
        left_repository.commit_scheduler_task_outcome(&left_claim, &left_outcome),
        right_repository.commit_scheduler_task_outcome(&right_claim, &right_outcome),
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, SchedulerTaskCommitOutcome::Committed { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, SchedulerTaskCommitOutcome::ExactReplay { .. }))
            .count(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_retrieval_publications WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );
    let row = sqlx::query(
        "SELECT effective_public_policy,public_projection FROM workflow_retrieval_publications
         WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let projection: Value =
        serde_json::from_str(row.try_get("public_projection").unwrap()).unwrap();
    assert_eq!(projection["query"], json!("WBC"));
    assert_eq!(
        projection["results"][0]["title"],
        json!("White blood cell guidance")
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&claim, &outcome)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::ExactReplay { .. }
    ));
    assert!(repository
        .commit_scheduler_task_outcome(&claim, &success(&claim, "WBC", "changed"))
        .await
        .is_err());
}

#[tokio::test]
async fn sqlite_private_retrieval_records_no_query_or_candidate_and_deadline_records_no_row() {
    let private_repository = SqliteDurableRepository::in_memory().await.unwrap();
    let private_control = private_repository.pool.clone();
    let private_deployment = deployed(
        PublicMode::Private,
        Duration::from_secs(30),
        "sqlite_private",
    );
    let private_run = RunId::new("run_retrieval_sqlite_private").unwrap();
    let (private_claim, _) = prepare_sqlite(
        &private_repository,
        &private_control,
        &private_deployment,
        &private_run,
    )
    .await;
    assert!(matches!(
        private_repository
            .commit_scheduler_task_outcome(
                &private_claim,
                &success(&private_claim, "secret-query", "secret-candidate"),
            )
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let private_row =
        sqlx::query("SELECT public_projection FROM workflow_retrieval_publications WHERE run_id=?")
            .bind(private_run.as_str())
            .fetch_one(&private_control)
            .await
            .unwrap();
    assert!(private_row
        .try_get::<Option<String>, _>("public_projection")
        .unwrap()
        .is_none());

    let deadline_repository = SqliteDurableRepository::in_memory().await.unwrap();
    let deadline_control = deadline_repository.pool.clone();
    let deadline_deployment = deployed(PublicMode::Both, Duration::from_secs(2), "sqlite_deadline");
    let deadline_run = RunId::new("run_retrieval_sqlite_deadline").unwrap();
    let (deadline_claim, _) = prepare_sqlite(
        &deadline_repository,
        &deadline_control,
        &deadline_deployment,
        &deadline_run,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    assert!(matches!(
        deadline_repository
            .commit_scheduler_task_outcome(
                &deadline_claim,
                &success(&deadline_claim, "WBC", "late"),
            )
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::OperationDeadlineElapsed
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_retrieval_publications WHERE run_id=?",
        )
        .bind(deadline_run.as_str())
        .fetch_one(&deadline_control)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_terminal_failure_preserves_an_earlier_committed_retrieval() {
    let repository = SqliteDurableRepository::in_memory().await.unwrap();
    let control = repository.pool.clone();
    let deployment = deployed_from_source(
        PublicMode::Both,
        Duration::from_secs(30),
        "sqlite_later_failure",
        FAIL_AFTER_RETRIEVAL_SOURCE,
    );
    let run_id = RunId::new("run_retrieval_sqlite_later_failure").unwrap();
    let (retrieval_claim, fence) =
        prepare_sqlite(&repository, &control, &deployment, &run_id).await;
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(
                &retrieval_claim,
                &success(&retrieval_claim, "WBC", "durably retained"),
            )
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(repository
        .acknowledge_scheduler_task(&retrieval_claim)
        .await
        .unwrap());

    let linked = deployment.linked_plan().unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    let downstream_claim = repository
        .claim_scheduler_tasks("retrieval-downstream-failure-worker", 60, 1)
        .await
        .unwrap()
        .pop()
        .expect("downstream Action must be claimable");
    assert!(matches!(
        repository
            .mark_scheduler_task_started(&downstream_claim)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let downstream_claim = match repository
        .heartbeat_scheduler_task(&downstream_claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(claim) => claim,
        other => panic!("downstream Action claim did not renew: {other:?}"),
    };
    let failure = WorkerFailure::new(
        WorkerFailureClass::InfrastructureFailure,
        "DOWNSTREAM_FAILURE",
        false,
    )
    .unwrap();
    let outcome = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_worker_failure(
            &downstream_claim,
            &failure,
            EffectEvidence::Started,
            SchedulerFailureDisposition::Terminal,
        )
        .unwrap(),
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&downstream_claim, &outcome)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunFailed)
    ));

    let snapshot = repository
        .load_response_snapshot(&run_id)
        .await
        .unwrap()
        .expect("failed Run must have a terminal response snapshot");
    assert_eq!(snapshot.workflow()["error"]["code"], "DOWNSTREAM_FAILURE");
    assert_eq!(
        snapshot.workflow()["retrievals"].as_array().unwrap().len(),
        1
    );
    assert_eq!(snapshot.workflow()["retrievals"][0]["query"], "WBC");
    assert_eq!(
        snapshot.workflow()["retrievals"][0]["results"][0]["title"],
        "durably retained"
    );
}

#[tokio::test]
async fn sqlite_retrieval_artifact_is_exact_run_scoped_and_retained_atomically_at_terminal() {
    let repository = SqliteDurableRepository::in_memory().await.unwrap();
    let control = repository.pool.clone();
    let deployment = deployed(
        PublicMode::Artifact,
        Duration::from_secs(30),
        "sqlite_artifact",
    );
    let run_id = RunId::new("run_retrieval_sqlite_artifact").unwrap();
    let (claim, fence) = prepare_sqlite(&repository, &control, &deployment, &run_id).await;
    let valid = artifact("retrieval_valid", b"retrieval valid artifact");
    stage_and_verify_sqlite_artifact(&repository, &run_id, &valid).await;

    // A verified-but-unreferenced ArtifactRef is not public retention
    // authority. The whole task-success transaction must roll back.
    assert!(repository
        .commit_scheduler_task_outcome(&claim, &artifact_success(&claim, &valid))
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_retrieval_publications WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT task_state FROM task_outbox WHERE run_id=? AND task_id=?",
        )
        .bind(run_id.as_str())
        .bind(claim.task_id().as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "claimed"
    );

    sqlx::query(
        "UPDATE artifacts SET artifact_state='referenced',referenced_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND artifact_id=? AND artifact_state='verified'",
    )
    .bind(run_id.as_str())
    .bind(valid.artifact_id().as_str())
    .execute(&control)
    .await
    .unwrap();

    let cross_run = RunId::new("run_retrieval_sqlite_artifact_other").unwrap();
    assert!(matches!(
        repository
            .create_run(
                transition("artifact-other-create", &cross_run),
                CreateRunCommand::new(
                    cross_run.clone(),
                    deployment.versioned_plan(),
                    json!({"question": "other"}),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let other_artifact = artifact("retrieval_other", b"other run artifact");
    stage_and_verify_sqlite_artifact(&repository, &cross_run, &other_artifact).await;
    sqlx::query(
        "UPDATE artifacts SET artifact_state='referenced',referenced_at=CURRENT_TIMESTAMP
         WHERE run_id=? AND artifact_id=? AND artifact_state='verified'",
    )
    .bind(cross_run.as_str())
    .bind(other_artifact.artifact_id().as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(repository
        .commit_scheduler_task_outcome(&claim, &artifact_success(&claim, &other_artifact),)
        .await
        .is_err());

    let forged = ArtifactRef::new(
        valid.artifact_id().clone(),
        valid.content_hash().clone(),
        valid.size_bytes() + 1,
        valid.media_type().map(str::to_owned),
    )
    .unwrap();
    assert!(repository
        .commit_scheduler_task_outcome(&claim, &artifact_success(&claim, &forged))
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_retrieval_publications WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0
    );

    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&claim, &artifact_success(&claim, &valid))
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(repository.acknowledge_scheduler_task(&claim).await.unwrap());
    let linked = deployment.linked_plan().unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &fence, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));

    let snapshot = repository
        .load_response_snapshot(&run_id)
        .await
        .unwrap()
        .expect("successful Run must have a terminal response snapshot");
    assert_eq!(
        snapshot.workflow()["retrievals"][0]["results"][0]["artifact"]["artifact_id"],
        valid.artifact_id().as_str()
    );
    let retention = sqlx::query(
        "SELECT artifact_count,registration_kind FROM artifact_retention_releases WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(retention.try_get::<i64, _>("artifact_count").unwrap(), 1);
    assert_eq!(
        retention.try_get::<String, _>("registration_kind").unwrap(),
        "terminal_atomic"
    );
    assert!(sqlx::query_scalar::<_, Option<String>>(
        "SELECT retain_until FROM artifacts WHERE run_id=? AND artifact_id=?",
    )
    .bind(run_id.as_str())
    .bind(valid.artifact_id().as_str())
    .fetch_one(&control)
    .await
    .unwrap()
    .is_some());
}

#[tokio::test]
async fn postgres_retrieval_success_race_replay_and_deadline_match_sqlite() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("retrieval_publication_{}", Uuid::new_v4().simple());
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
    super::schema_contract::provision_postgres_for_test(&control).await;
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();

    let deployment = deployed(PublicMode::Both, Duration::from_secs(30), "pg_race");
    let run_id = RunId::new("run_retrieval_pg_race").unwrap();
    let (claim, fence) = prepare_postgres(&repository, &control, &deployment, &run_id).await;
    let outcome = success(&claim, "WBC", "PostgreSQL source");
    let (left, right) = tokio::join!(
        repository.commit_scheduler_task_outcome(&claim, &outcome),
        repository.commit_scheduler_task_outcome(&claim, &outcome),
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, SchedulerTaskCommitOutcome::Committed { .. }))
            .count(),
        1
    );
    assert!(repository.acknowledge_scheduler_task(&claim).await.unwrap());
    assert!(matches!(
        drive_scheduler_until_quiescent(
            &repository,
            &deployment.linked_plan().unwrap(),
            &fence,
            32,
        )
        .await
        .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)
    ));
    let snapshot = repository
        .load_response_snapshot(&run_id)
        .await
        .unwrap()
        .expect("PostgreSQL successful Run must expose its durable response snapshot");
    assert_eq!(
        snapshot.workflow()["retrievals"].as_array().unwrap().len(),
        1
    );
    assert_eq!(snapshot.workflow()["retrievals"][0]["query"], "WBC");
    assert_eq!(
        snapshot.workflow()["retrievals"][0]["results"][0]["title"],
        "PostgreSQL source"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, SchedulerTaskCommitOutcome::ExactReplay { .. }))
            .count(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_retrieval_publications WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1
    );

    let deadline_deployment = deployed(PublicMode::Both, Duration::from_secs(2), "pg_deadline");
    let deadline_run = RunId::new("run_retrieval_pg_deadline").unwrap();
    let (deadline_claim, _deadline_fence) =
        prepare_postgres(&repository, &control, &deadline_deployment, &deadline_run).await;
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(
                &deadline_claim,
                &success(&deadline_claim, "WBC", "late"),
            )
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::OperationDeadlineElapsed
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_retrieval_publications WHERE run_id=$1",
        )
        .bind(deadline_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0
    );

    control.close().await;
    drop(repository);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[test]
fn all_four_frozen_publication_modes_are_independent_and_private_is_noninspecting() {
    for (mode, expect_query, expect_result) in [
        (PublicMode::Private, false, false),
        (PublicMode::Query, true, false),
        (PublicMode::Result, false, true),
        (PublicMode::Both, true, true),
    ] {
        let deployment = deployed(mode, Duration::from_secs(30), &format!("mode_{mode:?}"));
        let node = deployment
            .plan()
            .nodes()
            .iter()
            .find(|node| matches!(node.kind(), NodeKind::RetrievalTask(_)))
            .unwrap();
        let linked = deployment.linked_plan().unwrap();
        let target = FrozenRetrievalTarget::from_deployment_binding(
            linked.descriptor(node.id()).unwrap().deployment_binding(),
        )
        .unwrap();
        let projection = target.public_projection().unwrap();
        assert_eq!(projection.query_authorized(), expect_query);
        assert_eq!(projection.result_authorized(), expect_result);
        if matches!(mode, PublicMode::Private) {
            assert!(projection
                .project_validated_completed(
                    "ret_private",
                    &json!("invalid-private-input"),
                    Some(&json!("invalid-private-candidate")),
                )
                .unwrap()
                .is_none());
        }
    }
}
