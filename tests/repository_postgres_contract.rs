use std::{sync::Arc, time::Duration};

use insight_agent_platform::{
    catalog::{compile_agent_dir, DeployedAgent, LeafDeploymentResolver, ResolvedLeafDeployment},
    dsl::CompileError,
    engine::{
        plan::LeafTaskDescriptor, repository::SqliteDurableRepository, AdmissionState,
        ExecutionEventContext, ExecutionEventPayload, IntentHash, LeafTaskKind,
        PendingExecutionEvent, PublicEventPayload, RunId, RunLifecycle, SubflowContractRegistry,
        TerminationReason, VersionTag, WorkerExecutorRegistry,
    },
    runtime::{
        DeployedAgentCatalog, ProductionRunRepository, RequestMetadata, RunService,
        RunServiceConfig,
    },
};
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, SqlitePool};

struct NoLeafResolver;

impl LeafDeploymentResolver for NoLeafResolver {
    fn resolve_leaf(
        &self,
        _kind: LeafTaskKind,
        _descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        ResolvedLeafDeployment::new(VersionTag::new("unused-worker").unwrap(), json!({}))
    }
}

#[derive(serde::Serialize)]
struct TypedPublicEventIntent<'a> {
    payload: &'a PublicEventPayload,
}

#[derive(serde::Serialize)]
struct TypedRunTransitionIntent<'a> {
    run_id: &'a RunId,
    expected_projection_version: u64,
    expected_lifecycle: RunLifecycle,
    expected_admission: AdmissionState,
    next_lifecycle: RunLifecycle,
    next_admission: AdmissionState,
    termination_intent_reason: Option<TerminationReason>,
    terminal: Option<()>,
    event: &'a PendingExecutionEvent,
    public_event: Option<TypedPublicEventIntent<'a>>,
}

#[tokio::test]
async fn run_transition_intent_serialization_keeps_closed_typed_events() {
    let temporary = tempfile::tempdir().unwrap();
    let agent = temporary.path().join("typed-intent-agent");
    std::fs::create_dir_all(&agent).unwrap();
    std::fs::write(
        agent.join("agent.yaml"),
        r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: repository_typed_intent
  name: Repository typed intent
  description: Freezes the closed transition intent wire contract.
inputs:
  question: string
output: string
workflow:
  steps:
    - return: $question
"#,
    )
    .unwrap();
    let published = Arc::new(compile_agent_dir(&agent).unwrap());
    let deployed = Arc::new(
        DeployedAgent::publish(published, &NoLeafResolver, SubflowContractRegistry::new()).unwrap(),
    );

    let database = temporary.path().join("typed-intent.sqlite");
    let repository = Arc::new(
        SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap(),
    );
    let service = RunService::start(
        DeployedAgentCatalog::new(vec![deployed]).unwrap(),
        repository as Arc<dyn ProductionRunRepository>,
        WorkerExecutorRegistry::new(),
        RunServiceConfig::single_process_development(8, 1, 1, 32),
    )
    .await
    .unwrap();
    let run = service
        .create_detached(
            "repository_typed_intent",
            json!({"question": "typed"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    service.shutdown(Duration::from_secs(1)).await.unwrap();

    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(false)
            .disable_statement_logging(),
    )
    .await
    .unwrap();
    let (stored_intent_hash, stored_execution_payload) = sqlx::query_as::<_, (String, String)>(
        "SELECT intent_hash,safe_payload FROM execution_events
             WHERE run_id=? AND kind='run.lifecycle_changed'
               AND json_extract(safe_payload,'$.lifecycle')='active'",
    )
    .bind(&run.run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    let stored_public_envelope = sqlx::query_scalar::<_, String>(
        "SELECT safe_envelope FROM public_event_outbox
         WHERE run_id=? AND event_kind='run.started'",
    )
    .bind(&run.run_id)
    .fetch_one(&control)
    .await
    .unwrap();

    let run_id = RunId::new(run.run_id).unwrap();
    let event = PendingExecutionEvent::new(
        ExecutionEventContext::for_run(run_id.clone()),
        ExecutionEventPayload::RunLifecycleChanged {
            lifecycle: RunLifecycle::Active,
        },
    )
    .unwrap();
    let public_payload = PublicEventPayload::RunStarted;
    let typed_command = TypedRunTransitionIntent {
        run_id: &run_id,
        expected_projection_version: 0,
        expected_lifecycle: RunLifecycle::Created,
        expected_admission: AdmissionState::Open,
        next_lifecycle: RunLifecycle::Active,
        next_admission: AdmissionState::Open,
        termination_intent_reason: None,
        terminal: None,
        event: &event,
        public_event: Some(TypedPublicEventIntent {
            payload: &public_payload,
        }),
    };
    let typed_command_wire = serde_json::to_value(&typed_command).unwrap();

    assert_eq!(
        IntentHash::from_serializable(&typed_command)
            .unwrap()
            .as_str(),
        stored_intent_hash.as_str(),
        "the committed RunTransitionCommand hash must cover a PendingExecutionEvent and a typed PublicEventPayload"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&stored_execution_payload).unwrap(),
        typed_command_wire["event"]["payload"]
    );
    assert_eq!(
        serde_json::from_str::<Value>(&stored_public_envelope).unwrap()["payload"],
        typed_command_wire["public_event"]["payload"]
    );
    assert!(typed_command_wire.get("safe_event_payload").is_none());
    assert!(typed_command_wire["public_event"]
        .get("event_kind")
        .is_none());

    let mut raw_event_fallback = typed_command_wire.clone();
    let typed_event = raw_event_fallback
        .as_object_mut()
        .unwrap()
        .remove("event")
        .unwrap();
    raw_event_fallback["safe_event_payload"] = typed_event["payload"].clone();
    assert_ne!(
        IntentHash::from_serializable(&raw_event_fallback)
            .unwrap()
            .as_str(),
        stored_intent_hash.as_str(),
        "a raw safe_event_payload fallback must not reproduce the committed intent"
    );

    let mut string_kind_fallback = typed_command_wire;
    string_kind_fallback["public_event"] = json!({"event_kind": "run.started"});
    assert_ne!(
        IntentHash::from_serializable(&string_kind_fallback)
            .unwrap()
            .as_str(),
        stored_intent_hash.as_str(),
        "a String event kind fallback must not reproduce the committed intent"
    );
}
