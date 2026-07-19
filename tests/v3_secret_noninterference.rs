use std::{collections::BTreeMap, fs, sync::Arc, time::Duration};

use async_trait::async_trait;
use insight_agent_platform::{
    catalog_v3::{compile_v3_agent_dir, DeployedV3Agent, ProductionLeafDeploymentResolver},
    dsl::v3::{GraphAuthorDocument, GraphDocumentId, TraceOverlay},
    engine::{
        plan::{PlanIndex, VersionTag},
        repository::SqliteDurableRepository,
        EffectEvidence, LeafTaskExecutor, RunId, RuntimeValue, SchedulerTaskKind,
        SubflowContractRegistry, TaskExecutionRequest, TaskExecutionResult, WorkerExecutionContext,
        WorkerExecutorRegistry, WorkerFailure,
    },
    resources::{actions::ActionRegistry, config::load_model_registry_with_env},
    runtime::{
        DeployedAgentCatalog, ProductionRunRepository, RequestMetadata, RunService,
        RunServiceConfig,
    },
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const SECRET_SENTINEL: &str = "v3-secret-sentinel-MUST-NEVER-PERSIST-7c6a1f";

struct FixtureExecutor;

#[async_trait]
impl LeafTaskExecutor for FixtureExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let output = request.outputs().first().expect("fixture output contract");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("safe response")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

#[tokio::test]
async fn configured_secret_never_enters_plan_ledger_graph_trace_or_diagnostics() {
    let directory = tempfile::tempdir().unwrap();
    let model_config = directory.path().join("models.yaml");
    fs::write(
        &model_config,
        r#"version: 1
models:
  primary:
    type: open_ai_chat
    base_url: https://models.example.test/v1
    model: example-chat
    api_key_env: MODEL_API_KEY
    capabilities: []
    connect_timeout: 2s
    request_timeout: 30s
"#,
    )
    .unwrap();
    let models = load_model_registry_with_env(&model_config, |name| {
        (name == "MODEL_API_KEY").then(|| SECRET_SENTINEL.to_owned())
    })
    .unwrap();

    let agent_dir = directory.path().join("secret_fixture");
    fs::create_dir_all(&agent_dir).unwrap();
    fs::write(
        agent_dir.join("agent.yaml"),
        r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: secret_fixture
  name: Secret fixture
  description: Verifies configured-secret non-interference.
inputs: {question: string}
output: string
workflow:
  steps:
    - type: llm
      id: answer
      model: primary
      messages:
        - role: user
          content:
            - text: $question
      response: string
    - return: $answer
"#,
    )
    .unwrap();
    let published = Arc::new(compile_v3_agent_dir(&agent_dir).unwrap());
    let actions = ActionRegistry::default();
    let resolver = ProductionLeafDeploymentResolver::new(&models, &actions);
    let deployed = Arc::new(
        DeployedV3Agent::publish(
            Arc::clone(&published),
            &resolver,
            SubflowContractRegistry::new(),
        )
        .unwrap(),
    );

    let graph = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("secret_fixture_graph").unwrap(),
        published.plan().clone(),
    )
    .unwrap();
    let diagnostic_surface = format!("{deployed:?} {:?}", deployed.risk_diagnostics());
    for encoded in [
        serde_json::to_vec(deployed.versioned_plan()).unwrap(),
        graph.encode_json().unwrap(),
        diagnostic_surface.into_bytes(),
    ] {
        assert!(!String::from_utf8_lossy(&encoded).contains(SECRET_SENTINEL));
    }

    let database = directory.path().join("secret-ledger.sqlite");
    let repository = Arc::new(
        SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap(),
    );
    let index = PlanIndex::new(published.plan()).unwrap();
    let leaf = published
        .plan()
        .nodes()
        .iter()
        .find_map(|node| index.leaf_descriptor(node.id()))
        .unwrap();
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Llm,
            leaf.descriptor().implementation.clone(),
            leaf.descriptor().descriptor_version.clone(),
            VersionTag::new("openai-chat-adapter-1.0.0").unwrap(),
            Arc::new(FixtureExecutor),
        )
        .unwrap();
    let service = RunService::start(
        DeployedAgentCatalog::new(vec![Arc::clone(&deployed)]).unwrap(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers,
        {
            let mut config = RunServiceConfig::single_process_development(2, 1, 1, 32);
            config.scheduler_lease_seconds = 30;
            config.task_claim_seconds = 30;
            config.scheduler_action_budget = 128;
            config.pump_interval = Duration::from_millis(5);
            config.run_timeout = Duration::from_secs(30);
            config.artifact_orphan_retention = Duration::from_secs(60);
            config.artifact_reference_retention = Duration::from_secs(60);
            config.artifact_gc_interval = Duration::from_secs(60);
            config.artifact_deletion_claim_seconds = 30;
            config.public_event_nonterminal_retention = Duration::from_secs(60);
            config.public_event_prune_interval = Duration::from_secs(60);
            config
        },
    )
    .await
    .unwrap();
    let mut attached = service
        .create_attached(
            "secret_fixture",
            json!({"question": "ordinary public input"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let run_id = RunId::new(attached.run_id.clone()).unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), attached.subscription.recv())
            .await
            .expect("public event stream stalled")
            .unwrap();
        assert!(!serde_json::to_string(&event)
            .unwrap()
            .contains(SECRET_SENTINEL));
        if matches!(
            event.event_type,
            insight_agent_platform::events::protocol::RunEventType::RunCompleted
                | insight_agent_platform::events::protocol::RunEventType::RunFailed
                | insight_agent_platform::events::protocol::RunEventType::RunCancelled
                | insight_agent_platform::events::protocol::RunEventType::RunInterrupted
        ) {
            break;
        }
    }
    let trace = TraceOverlay::new(graph.document_id().clone(), run_id);
    assert!(!String::from_utf8_lossy(&trace.encode_json().unwrap()).contains(SECRET_SENTINEL));
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(service);
    drop(repository);
    let database_bytes = fs::read(database).unwrap();
    assert!(!String::from_utf8_lossy(&database_bytes).contains(SECRET_SENTINEL));
}
