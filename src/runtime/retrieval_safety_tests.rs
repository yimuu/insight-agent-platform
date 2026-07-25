use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use tokio_util::sync::CancellationToken;

use crate::{
    catalog::{DeployedAgent, ProductionLeafDeploymentResolver, PublishedAgent},
    dsl::{CompileOptions, GraphAuthorDocument, GraphDocumentId},
    engine::{
        production_worker_registry_with_retrievals, DefinitionRevisionId, RunId,
        SchedulerQuiescence, SubflowContractRegistry, TransitionKey, TransitionOutcome,
    },
    resources::{
        actions::ActionRegistry,
        actions::{CancellationClass, EffectClass, IdempotencyClass},
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

use crate::engine::repository::{
    consume_scheduler_task_once_with_retrieval_observer, drive_scheduler_until_quiescent,
    CreateRunCommand, DurableRepository, FailOnceSchedulerCrash, FencedSchedulerRunCommand,
    FrozenSchedulerWorkerFailurePolicy, NoSchedulerCrash, PlanInstallOutcome, SchedulerCrashPoint,
    SchedulerRecoveryOutcome, SchedulerWorkerPumpOutcome, SqliteDurableRepository,
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

#[derive(Clone)]
struct FixtureRetrieval;

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
        RetrievalPublicPolicy {
            query: true,
            result_schema: Some(json!({
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

fn retrieval_registry() -> RetrievalRegistry {
    let mut retrievals = RetrievalRegistry::default();
    retrievals.register(FixtureRetrieval).unwrap();
    retrievals
}

fn deployed(suffix: &str) -> DeployedAgent {
    let retrievals = retrieval_registry();
    let source_revision = format!("retrieval_runtime_{suffix}");
    let graph = GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new(format!("retrieval_runtime_graph_{suffix}")).unwrap(),
        SOURCE,
        CompileOptions::new(
            DefinitionRevisionId::new(source_revision).unwrap(),
            "retrieval-runtime.yaml",
            SOURCE,
        ),
    )
    .unwrap();
    let published = Arc::new(
        PublishedAgent::from_verified_graph(
            format!("retrieval-runtime-{suffix}"),
            format!("retrieval-runtime-{suffix}"),
            "retrieval runtime observation fixture",
            graph,
        )
        .unwrap(),
    );
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let resolver = ProductionLeafDeploymentResolver::new(&models, &actions)
        .with_retrievals(&retrievals)
        .with_operation_timeout(Duration::from_secs(30))
        .unwrap();
    DeployedAgent::publish(published, &resolver, SubflowContractRegistry::new()).unwrap()
}

fn transition(label: &str, run_id: &RunId) -> TransitionKey {
    TransitionKey::derive("retrieval.runtime.test.v1", &[label, run_id.as_str()]).unwrap()
}

async fn sqlite_fixture(label: &str) -> (tempfile::TempDir, SqliteDurableRepository, SqlitePool) {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join(format!("{label}.sqlite"));
    let repository = crate::test_database::provisioned_sqlite_repository(&database).await;
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    (directory, repository, control)
}

async fn prepare_sqlite_waiting(
    repository: &SqliteDurableRepository,
    control: &SqlitePool,
    deployment: &DeployedAgent,
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
        drive_scheduler_until_quiescent(repository, &linked, &fence, &NoSchedulerCrash, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    fence
}

#[tokio::test]
async fn sqlite_live_retrieval_is_post_commit_best_effort_and_never_a_standard_tool_event() {
    let retrievals = retrieval_registry();
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let workers =
        production_worker_registry_with_retrievals(&models, &actions, &retrievals).unwrap();

    let (_directory, repository, control) = sqlite_fixture("retrieval_live").await;
    let deployment = deployed("sqlite_live");
    let run_id = RunId::new("run_retrieval_sqlite_live").unwrap();
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

    // A crash after the SQL commit but before observation delivery leaves one
    // durable row and no live event. Recovery acknowledges the exact result
    // without replaying the best-effort observation.
    let (_crashed_directory, crashed_repository, crashed_control) =
        sqlite_fixture("retrieval_live_crash").await;
    let crashed_deployment = deployed("sqlite_live_crash");
    let crashed_run = RunId::new("run_retrieval_sqlite_live_crash").unwrap();
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
