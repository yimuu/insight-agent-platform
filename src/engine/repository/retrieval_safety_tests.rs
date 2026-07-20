use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool, Row, SqlitePool};
use uuid::Uuid;

use crate::{
    catalog_v3::{DeployedV3Agent, ProductionLeafDeploymentResolver, PublishedV3Agent},
    dsl::v3::{CompileOptions, GraphAuthorDocument, GraphDocumentId},
    engine::{
        deterministic_retrieval_id, production_worker_registry_with_retrievals, ArtifactId,
        ArtifactRef, ContentHash, EffectEvidence, FrozenRetrievalTarget, RetrievalCompletion,
        RuntimeValue, SchedulerQuiescence, TaskExecutionResult, TransitionKey, TransitionOutcome,
        WorkerFailure, WorkerFailureClass,
    },
    resources::{
        actions::{
            Action, ActionContext, ActionDescriptor, ActionRegistry, CancellationClass,
            EffectClass, IdempotencyClass,
        },
        models::ModelRegistry,
        retrievals::{
            Retrieval, RetrievalContext, RetrievalDescriptor, RetrievalExecutionResult,
            RetrievalPublicPolicy, RetrievalRegistry,
        },
    },
    runtime::{
        InMemoryLiveResponseBroker, LiveResponseBroker, LiveResponseDelivery, ResponseStreamEvent,
        ResponseStreamEventType, RunError,
    },
};
use tokio_util::sync::CancellationToken;

use super::{
    consume_scheduler_task_once_with_retrieval_observer, drive_scheduler_until_quiescent,
    ArtifactDurableRepository, ClaimSchedulerRunCommand, CreateRunCommand, DurableRepository,
    FailOnceSchedulerCrash, FencedSchedulerRunCommand, FrozenSchedulerWorkerFailurePolicy,
    NoSchedulerCrash, PlanInstallOutcome, PostgresDurableRepository, SchedulerCrashPoint,
    SchedulerDurableRepository, SchedulerLeaseRepository, SchedulerRecoveryOutcome,
    SchedulerTaskClaim, SchedulerTaskCommitOutcome, SchedulerTaskHeartbeatOutcome,
    SchedulerTaskOutcome, SchedulerTaskSuccess, SchedulerWorkerFailurePolicy,
    SchedulerWorkerPumpOutcome, SqliteDurableRepository, StageArtifactCommand, StorageLocator,
    TerminalSchedulerWorkerFailurePolicy, VerifyArtifactCommand,
};

const SOURCE: &str = r#"api_version: insight.agent/v3
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

const FAIL_AFTER_RETRIEVAL_SOURCE: &str = r#"api_version: insight.agent/v3
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

#[derive(Clone)]
struct FixtureRetrieval {
    mode: PublicMode,
}

#[derive(Clone)]
struct FixtureFailAction;

#[async_trait]
impl Action for FixtureFailAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: "fixture.fail_after_search",
            version: "1.0.0",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            output_schema: json!({"type": "string"}),
            effect: EffectClass::ReadOnly,
            idempotency: IdempotencyClass::Idempotent,
            cancellation: CancellationClass::Cooperative,
            required_capabilities: BTreeSet::new(),
        }
    }

    async fn call(&self, _input: Value, _context: ActionContext) -> Result<Value, RunError> {
        unreachable!("repository test commits the downstream failure directly")
    }
}

#[async_trait]
impl Retrieval for FixtureRetrieval {
    fn descriptor(&self) -> RetrievalDescriptor {
        RetrievalDescriptor {
            id: "fixture.search",
            version: "1.0.0",
            input_schema: json!({
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"],
                "additionalProperties": false
            }),
            query_field: "query",
            effect: EffectClass::ReadOnly,
            idempotency: IdempotencyClass::Idempotent,
            cancellation: CancellationClass::Cooperative,
            required_capabilities: BTreeSet::new(),
        }
    }

    fn public_policy(&self) -> RetrievalPublicPolicy {
        let result_schema = match self.mode {
            PublicMode::Private | PublicMode::Query => None,
            PublicMode::Result | PublicMode::Both => Some(json!({
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
                self.mode,
                PublicMode::Query | PublicMode::Both | PublicMode::Artifact
            ),
            result_schema,
        }
    }

    async fn retrieve(
        &self,
        _input: Value,
        _context: RetrievalContext,
    ) -> Result<RetrievalExecutionResult, RunError> {
        Ok(RetrievalExecutionResult::new(
            json!({"answer": "safe"}),
            Some(json!([{
                "id": "doc_live",
                "title": "live durable result",
                "metadata": {}
            }])),
        ))
    }
}

fn deployed(mode: PublicMode, timeout: Duration, suffix: &str) -> DeployedV3Agent {
    deployed_from_source(mode, timeout, suffix, SOURCE)
}

fn deployed_from_source(
    mode: PublicMode,
    timeout: Duration,
    suffix: &str,
    source: &str,
) -> DeployedV3Agent {
    let retrievals = retrieval_registry(mode);
    let definition = format!("retrieval_durable_{suffix}");
    let options = CompileOptions::new(
        crate::engine::DefinitionRevisionId::new(definition.clone()).unwrap(),
        "retrieval-durable.yaml",
        source,
    );
    let graph = GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new(format!("retrieval_graph_{suffix}")).unwrap(),
        source,
        options,
    )
    .unwrap();
    let published = Arc::new(
        PublishedV3Agent::from_verified_graph(
            format!("retrieval-{suffix}"),
            format!("retrieval-{suffix}"),
            "retrieval durable fixture",
            graph,
        )
        .unwrap(),
    );
    let models = ModelRegistry::default();
    let mut actions = ActionRegistry::default();
    actions.register(FixtureFailAction).unwrap();
    let resolver = ProductionLeafDeploymentResolver::new(&models, &actions)
        .with_retrievals(&retrievals)
        .with_operation_timeout(timeout)
        .unwrap();
    DeployedV3Agent::publish(
        published,
        &resolver,
        crate::engine::SubflowContractRegistry::new(),
    )
    .unwrap()
}

fn retrieval_registry(mode: PublicMode) -> RetrievalRegistry {
    let mut retrievals = RetrievalRegistry::default();
    retrievals.register(FixtureRetrieval { mode }).unwrap();
    retrievals
}

fn transition(label: &str, run_id: &crate::engine::RunId) -> TransitionKey {
    TransitionKey::derive("retrieval.durable.test.v1", &[label, run_id.as_str()]).unwrap()
}

async fn prepare_sqlite(
    repository: &SqliteDurableRepository,
    control: &SqlitePool,
    deployment: &DeployedV3Agent,
    run_id: &crate::engine::RunId,
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
    deployment: &DeployedV3Agent,
    run_id: &crate::engine::RunId,
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
        drive_scheduler_until_quiescent(repository, &linked, &fence, &NoSchedulerCrash, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    fence
}

async fn prepare_postgres(
    repository: &PostgresDurableRepository,
    control: &PgPool,
    deployment: &DeployedV3Agent,
    run_id: &crate::engine::RunId,
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
        drive_scheduler_until_quiescent(repository, &linked, &fence, &NoSchedulerCrash, 32,)
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
    run_id: &crate::engine::RunId,
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
    let run_id = crate::engine::RunId::new("run_retrieval_sqlite_race").unwrap();
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
    let private_run = crate::engine::RunId::new("run_retrieval_sqlite_private").unwrap();
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
    let deadline_run = crate::engine::RunId::new("run_retrieval_sqlite_deadline").unwrap();
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
    let run_id = crate::engine::RunId::new("run_retrieval_sqlite_later_failure").unwrap();
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
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 32)
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
        TerminalSchedulerWorkerFailurePolicy
            .freeze(&downstream_claim, &failure)
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
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 32)
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
    let run_id = crate::engine::RunId::new("run_retrieval_sqlite_artifact").unwrap();
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

    let cross_run = crate::engine::RunId::new("run_retrieval_sqlite_artifact_other").unwrap();
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
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 32)
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
async fn sqlite_live_retrieval_is_post_commit_best_effort_and_never_a_standard_tool_event() {
    let retrievals = retrieval_registry(PublicMode::Both);
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let workers =
        production_worker_registry_with_retrievals(&models, &actions, &retrievals).unwrap();

    let repository = SqliteDurableRepository::in_memory().await.unwrap();
    let control = repository.pool.clone();
    let deployment = deployed(PublicMode::Both, Duration::from_secs(30), "sqlite_live");
    let run_id = crate::engine::RunId::new("run_retrieval_sqlite_live").unwrap();
    let _fence = prepare_sqlite_waiting(&repository, &control, &deployment, &run_id).await;
    let broker = InMemoryLiveResponseBroker::new(16, 4).unwrap();
    let mut subscriber = broker.subscribe(run_id.clone()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), subscriber.recv())
            .await
            .is_err()
    );

    assert!(matches!(
        consume_scheduler_task_once_with_retrieval_observer(
            &repository,
            &workers,
            &FrozenSchedulerWorkerFailurePolicy,
            "retrieval-live-worker",
            60,
            1,
            CancellationToken::new(),
            &broker,
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::Committed {
            acknowledged: true,
            ..
        }
    ));
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
    let LiveResponseDelivery::Publication(publication) =
        tokio::time::timeout(Duration::from_secs(1), subscriber.recv())
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("expected one Retrieval live publication")
    };
    assert_eq!(
        publication.payload_type(),
        ResponseStreamEventType::WorkflowRetrievalCompleted
    );
    let ResponseStreamEvent::WorkflowRetrievalCompleted { query, results, .. } =
        publication.into_public_event(1)
    else {
        panic!("Retrieval must not masquerade as a function, tool, or file-search event")
    };
    assert_eq!(query.as_deref(), Some("WBC"));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].title(), Some("live durable result"));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), subscriber.recv())
            .await
            .is_err()
    );

    // Freeze the real commit/publication boundary: a crash after the SQL
    // commit but before observation delivery leaves one durable row and no
    // live event. Recovery acknowledges the exact result without replaying a
    // best-effort observation.
    let crashed_repository = SqliteDurableRepository::in_memory().await.unwrap();
    let crashed_control = crashed_repository.pool.clone();
    let crashed_deployment = deployed(
        PublicMode::Both,
        Duration::from_secs(30),
        "sqlite_live_crash",
    );
    let crashed_run = crate::engine::RunId::new("run_retrieval_sqlite_live_crash").unwrap();
    let _crashed_fence = prepare_sqlite_waiting(
        &crashed_repository,
        &crashed_control,
        &crashed_deployment,
        &crashed_run,
    )
    .await;
    let crashed_broker = InMemoryLiveResponseBroker::new(16, 4).unwrap();
    let mut crashed_subscriber = crashed_broker.subscribe(crashed_run.clone()).await.unwrap();
    let crash = FailOnceSchedulerCrash::new(SchedulerCrashPoint::AfterResultCommit);
    let error = consume_scheduler_task_once_with_retrieval_observer(
        &crashed_repository,
        &workers,
        &FrozenSchedulerWorkerFailurePolicy,
        "retrieval-live-crash-worker",
        60,
        1,
        CancellationToken::new(),
        &crashed_broker,
        &crash,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), "ENGINE_REPOSITORY_SCHEDULER_CRASH_INJECTED");
    assert!(crash.fired());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_retrieval_publications WHERE run_id=?",
        )
        .bind(crashed_run.as_str())
        .fetch_one(&crashed_control)
        .await
        .unwrap(),
        1
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(25), crashed_subscriber.recv())
            .await
            .is_err()
    );
    assert_eq!(
        consume_scheduler_task_once_with_retrieval_observer(
            &crashed_repository,
            &workers,
            &FrozenSchedulerWorkerFailurePolicy,
            "retrieval-live-recovery-worker",
            60,
            1,
            CancellationToken::new(),
            &crashed_broker,
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::AcknowledgedRecoveredResult
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(25), crashed_subscriber.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn postgres_retrieval_success_race_replay_and_deadline_match_sqlite() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
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
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();

    let deployment = deployed(PublicMode::Both, Duration::from_secs(30), "pg_race");
    let run_id = crate::engine::RunId::new("run_retrieval_pg_race").unwrap();
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
            &NoSchedulerCrash,
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
    let deadline_run = crate::engine::RunId::new("run_retrieval_pg_deadline").unwrap();
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
            .published()
            .plan()
            .nodes()
            .iter()
            .find(|node| matches!(node.kind(), crate::engine::NodeKind::RetrievalTask(_)))
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
