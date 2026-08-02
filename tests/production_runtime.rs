#[path = "support/database.rs"]
mod database;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use insight_agent_platform::{
    catalog::{
        compile_agent_dir, DeployedAgent, LeafDeploymentResolver, ProductionLeafDeploymentResolver,
        ResolvedLeafDeployment,
    },
    dsl::{CompileError, GraphAuthorDocument, GraphDocumentId},
    engine::{
        plan::LeafTaskDescriptor,
        production_worker_registry, production_worker_registry_with_retrievals,
        repository::{
            CreateRunCommand, DurableRepository, SchedulerDurableRepository, VersionedPlan,
            PUBLIC_EVENT_NOTIFY_CHANNEL,
        },
        scheduler::TaskOutcomeFact,
        ContentHash, DeploymentRevisionId, EffectEvidence, LeafTaskExecutor, LeafTaskKind,
        LocalContentAddressedArtifactStore, RunId, RuntimeValue, SchedulerTaskKind,
        SubflowContractRegistry, TaskExecutionRequest, TaskExecutionResult, TransitionKey,
        TransitionOutcome, VersionTag, WorkerArtifactStore, WorkerExecutionContext,
        WorkerExecutorRegistry, WorkerFailure, WorkerFailureClass,
    },
    history::types::{RunAttachment, RunStatus},
    resources::{
        actions::{ActionRegistry, CancellationClass, EffectClass, IdempotencyClass},
        models::{
            ChatFinishReason, ChatModel, ChatRequest, ChatResponse, ChatStream, ModelCapability,
            ModelDeploymentIdentity, ModelRegistry, ModelRequestCapability, ModelSelector,
        },
        retrievals::{
            Retrieval, RetrievalContext, RetrievalDescriptor, RetrievalExecutionResult,
            RetrievalPublicPolicy, RetrievalRegistry,
        },
    },
    runtime::{
        DeployedAgentCatalog, ProductionRunRepository, RequestMetadata, RunError, RunService,
        RunServiceConfig, RunStreamDeploymentTopology,
    },
};
use insight_api::v1::{build_router, ApiAuth, ApiState};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const WORKER_VERSION: &str = "fixture-worker-1";
const PUBLIC_RETRIEVAL_AGENT_ID: &str = "public_retrieval_production_gate";
const CLEAN_CUTOVER_AGENT_ID: &str = "llm_descriptor_clean_cutover";
const CLEAN_CUTOVER_MODEL_WORKER_VERSION: &str = "clean-cutover-model-worker-v2";
const CLEAN_CUTOVER_SOURCE: &str = r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: llm_descriptor_clean_cutover
  name: LLM descriptor clean cutover
  description: Proves persisted descriptor versions never inherit new defaults.
inputs:
  question: string
output: string
workflow:
  steps:
    - id: answer
      type: llm
      model:
        provider: fixture
        id: clean-cutover-model
      stream: false
      publish: false
      messages:
        - role: user
          content:
            - text: $question
      parameters: {}
      response: string
    - return: $answer
"#;

struct FixtureResolver;

impl LeafDeploymentResolver for FixtureResolver {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        ResolvedLeafDeployment::new(
            VersionTag::new(WORKER_VERSION).unwrap(),
            json!({
                "kind": kind.name(),
                "implementation": descriptor.implementation,
            }),
        )
    }
}

struct FixtureExecutor;

#[derive(Clone)]
struct PublicRetrievalFixture;

#[async_trait]
impl Retrieval for PublicRetrievalFixture {
    fn descriptor(&self) -> RetrievalDescriptor {
        RetrievalDescriptor {
            id: "fixture.public_search".to_owned(),
            version: "1.0.0".to_owned(),
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
            query_field: "query".to_owned(),
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
        _input: serde_json::Value,
        _context: RetrievalContext,
    ) -> Result<RetrievalExecutionResult, insight_agent_platform::runtime::RunError> {
        Ok(RetrievalExecutionResult::new(
            json!({"answer": "retrieved"}),
            Some(json!([{"id": "doc_1", "metadata": {}}])),
        ))
    }
}

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
                RuntimeValue::new(json!("answered")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

struct LiveLlmFixtureExecutor;

#[derive(Debug, Clone)]
struct CleanCutoverCountingModel {
    provider_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ChatModel for CleanCutoverCountingModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
        BTreeSet::from([ModelRequestCapability::Complete])
    }

    fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
        if parameters.is_object() {
            Ok(())
        } else {
            Err(CompileError::new(
                "MODEL_PARAMETERS_INVALID",
                "clean-cutover fixture parameters must be an object",
            ))
        }
    }

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse, RunError> {
        self.provider_calls.fetch_add(1, Ordering::SeqCst);
        Ok(ChatResponse {
            text: "descriptor v2 resumed".to_owned(),
            tool_calls: Vec::new(),
            finish_reason: ChatFinishReason::Stop,
            usage: None,
        })
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        self.provider_calls.fetch_add(1, Ordering::SeqCst);
        Err(RunError::operation(
            "UNEXPECTED_STREAMING_REQUEST",
            "the clean-cutover fixture only permits its explicit complete request mode",
        ))
    }
}

#[async_trait]
impl LeafTaskExecutor for LiveLlmFixtureExecutor {
    fn live_run_stream_capable(&self) -> bool {
        true
    }

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
                RuntimeValue::new(json!("streamed answer")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

struct PanicsOnceExecutor {
    calls: AtomicUsize,
}

#[async_trait]
impl LeafTaskExecutor for PanicsOnceExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("executor panic payload must never escape the worker boundary");
        }
        let output = request.outputs().first().expect("fixture output contract");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("recovered worker pump")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

fn deployed_catalog() -> (DeployedAgentCatalog, String) {
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dsl/runtime_agent");
    let published = Arc::new(compile_agent_dir(&directory).unwrap());
    let deployed = Arc::new(
        DeployedAgent::publish(published, &FixtureResolver, SubflowContractRegistry::new())
            .unwrap(),
    );
    let revision = deployed.deployment_revision_id().as_str().to_owned();
    (DeployedAgentCatalog::new(vec![deployed]).unwrap(), revision)
}

fn workers() -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Action,
            "fixture.answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new(WORKER_VERSION).unwrap(),
            Arc::new(FixtureExecutor),
        )
        .unwrap();
    workers
}

fn public_streaming_catalog() -> DeployedAgentCatalog {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("agent.yaml"),
        r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: public_streaming_production_gate
  name: Public streaming production gate
  description: Verifies the production shared-broker deployment fence.
inputs:
  question:
    type: string
types:
  SearchOutput:
    fields:
      answer: string
output: SearchOutput
workflow:
  steps:
    - id: answer
      type: llm
      model:
        provider: fixture
        id: fixture-model
      stream: true
      publish: true
      messages:
        - role: user
          content:
            - text: $question
      response: SearchOutput
    - return: $answer
"#,
    )
    .unwrap();
    let published = Arc::new(compile_agent_dir(directory.path()).unwrap());
    let deployed = Arc::new(
        DeployedAgent::publish(published, &FixtureResolver, SubflowContractRegistry::new())
            .unwrap(),
    );
    DeployedAgentCatalog::new(vec![deployed]).unwrap()
}

fn public_streaming_workers() -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Llm,
            "core.llm",
            VersionTag::new("2").unwrap(),
            VersionTag::new(WORKER_VERSION).unwrap(),
            Arc::new(LiveLlmFixtureExecutor),
        )
        .unwrap();
    workers
}

fn clean_cutover_fixture(provider_calls: Arc<AtomicUsize>) -> (Arc<DeployedAgent>, ModelRegistry) {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("agent.yaml"), CLEAN_CUTOVER_SOURCE).unwrap();
    let mut models = ModelRegistry::default();
    models
        .register_versioned(
            ModelSelector::new("fixture", "clean-cutover-model").unwrap(),
            ModelDeploymentIdentity::new(
                CLEAN_CUTOVER_MODEL_WORKER_VERSION,
                json!({
                    "adapter": "clean-cutover-fixture",
                    "provider_model": "complete-only-v2"
                }),
            )
            .unwrap(),
            CleanCutoverCountingModel { provider_calls },
        )
        .unwrap();
    let published = Arc::new(compile_agent_dir(directory.path()).unwrap());
    let deployment = Arc::new(
        DeployedAgent::publish(
            published,
            &ProductionLeafDeploymentResolver::new(&models, &ActionRegistry::default()),
            SubflowContractRegistry::new(),
        )
        .unwrap(),
    );
    (deployment, models)
}

fn clean_cutover_catalog(deployment: &Arc<DeployedAgent>) -> DeployedAgentCatalog {
    DeployedAgentCatalog::new(vec![Arc::clone(deployment)]).unwrap()
}

fn legacy_llm_descriptor_v1(deployment: &DeployedAgent) -> VersionedPlan {
    let graph = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("llm_descriptor_v1_archive").unwrap(),
        deployment.published().plan().clone(),
    )
    .unwrap();
    let mut graph_wire = serde_json::to_value(graph).unwrap();
    let llm = graph_wire["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["kind"]["kind"] == json!("llm_task"))
        .expect("clean-cutover fixture contains one LLM node");
    llm["kind"]["descriptor"]["descriptor_version"] = json!("1");
    let configuration = llm["kind"]["descriptor"]["public_configuration"]
        .as_object_mut()
        .unwrap();
    for field in ["stream", "publish", "tools", "tool_choice", "tool_limits"] {
        configuration.remove(field);
    }
    let legacy_graph = GraphAuthorDocument::decode_json(
        &serde_json::to_vec(&graph_wire).expect("legacy graph serializes"),
    )
    .expect("the generic Plan contract retains versioned opaque leaf descriptors");

    let current = serde_json::to_value(deployment.versioned_plan()).unwrap();
    let mut descriptor_contracts = current["descriptor_contracts"].clone();
    let descriptor = descriptor_contracts
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|document| document["node_id"] == json!("answer"))
        .expect("clean-cutover fixture descriptor contract exists");
    descriptor["descriptor_version"] = json!("1");
    let configuration = descriptor["public_configuration"].as_object_mut().unwrap();
    for field in ["stream", "publish", "tools", "tool_choice", "tool_limits"] {
        configuration.remove(field);
    }

    let mut resolved_bindings = current["resolved_bindings"].clone();
    let binding = resolved_bindings
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|document| document["node_id"] == json!("answer"))
        .expect("clean-cutover fixture binding exists")["binding"]
        .as_object_mut()
        .unwrap();
    for field in [
        "request_mode",
        "request_capabilities",
        "tool_choice",
        "tool_limits",
        "tools",
    ] {
        binding.remove(field);
    }
    let worker_contracts = current["worker_contracts"].clone();
    let binding_projection = json!({
        "schema_version": 1,
        "resolved_bindings": &resolved_bindings,
        "worker_contracts": &worker_contracts,
    });
    let binding_hash = ContentHash::from_bytes(
        &serde_jcs::to_vec(&binding_projection).expect("legacy binding canonicalizes"),
    );
    let deployment_identity = json!({
        "domain": "insight-agent/deployment-revision/v1",
        "definition_revision_id": legacy_graph.plan().metadata().definition_revision_id(),
        "plan_hash": legacy_graph.plan().semantic_hash(),
        "binding_hash": &binding_hash,
    });
    let deployment_hash = ContentHash::from_bytes(
        &serde_jcs::to_vec(&deployment_identity).expect("legacy deployment identity canonicalizes"),
    );
    let deployment_revision_id = DeploymentRevisionId::new(format!(
        "deployrev_{}",
        deployment_hash.as_str().trim_start_matches("sha256:")
    ))
    .unwrap();

    VersionedPlan::from_verified_graph(
        deployment.versioned_plan().definition_id(),
        deployment.versioned_plan().agent_id(),
        deployment.versioned_plan().display_name(),
        deployment_revision_id,
        current["expression_engine_version"].as_str().unwrap(),
        &legacy_graph,
        descriptor_contracts,
        resolved_bindings,
        worker_contracts,
    )
    .unwrap()
}

fn public_retrieval_streaming_fixture() -> (DeployedAgentCatalog, WorkerExecutorRegistry) {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("agent.yaml"),
        format!(
            r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: {PUBLIC_RETRIEVAL_AGENT_ID}
  name: Public Retrieval production gate
  description: Verifies first-class Retrieval live-response gates.
inputs:
  question:
    type: string
types:
  SearchOutput:
    fields:
      answer: string
output: SearchOutput
workflow:
  steps:
    - id: search
      type: retrieval
      retrieval: fixture.public_search
      publish: true
      inputs:
        query: $question
      response: SearchOutput
    - return: $search
"#
        ),
    )
    .unwrap();

    let retrievals = RetrievalRegistry::default();
    retrievals.register(PublicRetrievalFixture).unwrap();
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let published = Arc::new(compile_agent_dir(directory.path()).unwrap());
    let resolver =
        ProductionLeafDeploymentResolver::new(&models, &actions).with_retrievals(&retrievals);
    let deployed = Arc::new(
        DeployedAgent::publish(published, &resolver, SubflowContractRegistry::new()).unwrap(),
    );
    let workers =
        production_worker_registry_with_retrievals(&models, &actions, &retrievals).unwrap();
    (DeployedAgentCatalog::new(vec![deployed]).unwrap(), workers)
}

fn panics_once_workers() -> WorkerExecutorRegistry {
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Action,
            "fixture.answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new(WORKER_VERSION).unwrap(),
            Arc::new(PanicsOnceExecutor {
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
    workers
}

fn config(pump_interval: Duration) -> RunServiceConfig {
    let mut config = RunServiceConfig::single_process_development(16, 4, 2, 64);
    config.scheduler_lease_seconds = 30;
    config.task_claim_seconds = 30;
    config.scheduler_action_budget = 128;
    config.pump_interval = pump_interval;
    config.run_timeout = Duration::from_secs(60);
    config.artifact_orphan_retention = Duration::from_secs(60);
    config.artifact_reference_retention = Duration::from_secs(60);
    config.artifact_gc_interval = Duration::from_secs(60);
    config.artifact_deletion_claim_seconds = 30;
    config.public_event_nonterminal_retention = Duration::from_secs(60);
    config.public_event_prune_interval = Duration::from_secs(60);
    config
}

fn production_config(pump_interval: Duration) -> RunServiceConfig {
    let mut config = RunServiceConfig::production(16, 4, 2, 64);
    config.scheduler_lease_seconds = 30;
    config.task_claim_seconds = 30;
    config.scheduler_action_budget = 128;
    config.pump_interval = pump_interval;
    config.run_timeout = Duration::from_secs(60);
    config.artifact_orphan_retention = Duration::from_secs(60);
    config.artifact_reference_retention = Duration::from_secs(60);
    config.artifact_gc_interval = Duration::from_secs(60);
    config.artifact_deletion_claim_seconds = 30;
    config.public_event_nonterminal_retention = Duration::from_secs(60);
    config.public_event_prune_interval = Duration::from_secs(60);
    config
}

async fn wait_for_terminal(service: &RunService, run_id: &str) {
    let timeout = Duration::from_secs(30);
    let deadline = Instant::now() + timeout;
    loop {
        let record = service.get_run(run_id).await.unwrap();
        if record.status().is_terminal() {
            assert_eq!(record.status(), RunStatus::Completed);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Run did not become terminal within {timeout:?}; last status was {:?}",
            record.status()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn production_sqlite_is_rejected_at_the_library_boundary_before_catalog_writes() {
    let (agents, _) = deployed_catalog();
    let (_database, repository) = database::temporary_sqlite_repository().await;
    let repository = Arc::new(repository);

    let error = RunService::start(
        agents.clone(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        RunServiceConfig::production(16, 4, 2, 64),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), "PLATFORM_PRODUCTION_REQUIRES_POSTGRES");
    let stored = repository.load_versioned_plan_catalog().await.unwrap();
    assert!(stored.plans().is_empty());
    assert!(stored.heads().is_empty());

    let service = RunService::start(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let stored = repository.load_versioned_plan_catalog().await.unwrap();
    assert_eq!(stored.plans().len(), 1);
    assert_eq!(stored.heads().len(), 1);
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn retrieval_only_public_streaming_needs_no_llm_worker_at_start_or_admission() {
    let (agents, retrieval_workers) = public_retrieval_streaming_fixture();
    assert!(
        !retrieval_workers.supports_public_llm_response(),
        "an empty ModelRegistry must not manufacture a live LLM worker"
    );
    let (_database, repository) = database::temporary_sqlite_repository().await;
    let repository = Arc::new(repository);
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        retrieval_workers,
        config(Duration::from_millis(2)),
    )
    .await
    .expect("Retrieval-only discovery must not require a live LLM worker at startup");

    let detached = service
        .create_detached(
            PUBLIC_RETRIEVAL_AGENT_ID,
            json!({"question": "WBC"}),
            RequestMetadata::default(),
        )
        .await
        .expect("Retrieval-only admission must not require a live LLM worker");
    wait_for_terminal(&service, &detached.run_id).await;
    let app = build_router(ApiState {
        service: service.clone(),
        auth: ApiAuth::disabled(),
        sse_keep_alive_interval: Duration::from_secs(30),
        readiness_probe_timeout: Duration::from_secs(1),
    });
    let response = app
        .oneshot(
            Request::post(format!(
                "/v1/agents/{PUBLIC_RETRIEVAL_AGENT_ID}/runs/stream"
            ))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"question":"WBC"}"#))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = tokio::time::timeout(
        Duration::from_secs(5),
        to_bytes(response.into_body(), 1 << 20),
    )
    .await
    .expect("Retrieval attached response did not reach terminal EOF")
    .unwrap();
    let raw = String::from_utf8(body.to_vec()).unwrap();
    let events = raw
        .split_terminator("\n\n")
        .filter_map(|frame| {
            let event_type = frame
                .lines()
                .find_map(|line| line.strip_prefix("event: "))?;
            let data = frame.lines().find_map(|line| line.strip_prefix("data: "))?;
            Some((
                event_type.to_owned(),
                serde_json::from_str::<serde_json::Value>(data).unwrap(),
            ))
        })
        .collect::<Vec<_>>();
    let retrieval_index = events
        .iter()
        .position(|(event_type, _)| event_type == "run.retrieval.completed")
        .unwrap_or_else(|| {
            panic!(
                "public Retrieval must emit its dedicated live event; observed {:?}; terminal error {:?}",
                events
                    .iter()
                    .map(|(event_type, _)| event_type)
                    .collect::<Vec<_>>(),
                events
                    .last()
                    .map(|(_, event)| event["run"]["error"]["code"].clone())
            )
        });
    let terminal_index = events.len() - 1;
    assert!(retrieval_index < terminal_index);
    assert_eq!(events[retrieval_index].1["query"], "WBC");
    assert_eq!(events[retrieval_index].1["results"][0]["id"], "doc_1");
    assert_eq!(events[terminal_index].0, "run.lifecycle.completed");
    assert_eq!(
        events[terminal_index].1["run"]["retrievals"][0]["query"],
        "WBC"
    );
    assert_eq!(
        events[terminal_index].1["run"]["retrievals"][0]["results"][0]["id"],
        "doc_1"
    );
    assert!(!raw.contains("response.file_search_call."));
    assert!(!raw.contains("response.function_call_arguments."));
    assert!(!raw.contains("workflow.tool."));
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn production_artifact_store_gate_precedes_publication_and_binds_shared_identity() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        eprintln!("skipping real PostgreSQL production Artifact-store gate test");
        return;
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("artifact_authority_{}", &suffix[..16]);
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    database::provision_postgres_url(&scoped_url).await;
    let runtime_url = format!("{scoped_url}&application_name=insight-agent-platform-runtime");
    let repository = Arc::new(
        insight_agent_platform::engine::repository::PostgresDurableRepository::connect(
            &runtime_url,
        )
        .await
        .unwrap(),
    );
    let missing = RunService::start(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap_err();
    assert_eq!(
        missing.code(),
        "PLATFORM_PRODUCTION_REQUIRES_ARTIFACT_STORE"
    );
    let stored = repository.load_versioned_plan_catalog().await.unwrap();
    assert!(stored.plans().is_empty());
    assert!(stored.heads().is_empty());

    let artifact_directory = tempfile::tempdir().unwrap();
    let local = Arc::new(
        LocalContentAddressedArtifactStore::open(artifact_directory.path().join("local"), 1)
            .await
            .unwrap(),
    );
    let local_error = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        local,
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap_err();
    assert_eq!(
        local_error.code(),
        "PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE"
    );
    let stored = repository.load_versioned_plan_catalog().await.unwrap();
    assert!(stored.plans().is_empty());
    assert!(stored.heads().is_empty());

    let shared_root = artifact_directory.path().join("shared");
    let shared = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(shared_root.clone(), 1, "production")
            .await
            .unwrap(),
    );
    let (retrieval_agents, retrieval_workers) = public_retrieval_streaming_fixture();
    let retrieval_broker_error = RunService::start_with_artifact_store(
        retrieval_agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        retrieval_workers,
        shared.clone(),
        production_config(Duration::from_secs(3_600))
            .with_run_stream_topology(RunStreamDeploymentTopology::Distributed),
    )
    .await
    .unwrap_err();
    assert_eq!(
        retrieval_broker_error.code(),
        "PLATFORM_DISTRIBUTED_REQUIRES_SHARED_LIVE_RUN_STREAM_BROKER",
        "all public sources require a shared broker in distributed topology, even without an LLM source"
    );
    let stored = repository.load_versioned_plan_catalog().await.unwrap();
    assert!(stored.plans().is_empty());
    assert!(stored.heads().is_empty());

    let broker_error = RunService::start_with_artifact_store(
        public_streaming_catalog(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        public_streaming_workers(),
        shared.clone(),
        production_config(Duration::from_secs(3_600))
            .with_run_stream_topology(RunStreamDeploymentTopology::Distributed),
    )
    .await
    .unwrap_err();
    assert_eq!(
        broker_error.code(),
        "PLATFORM_DISTRIBUTED_REQUIRES_SHARED_LIVE_RUN_STREAM_BROKER"
    );
    let stored = repository.load_versioned_plan_catalog().await.unwrap();
    assert!(stored.plans().is_empty());
    assert!(stored.heads().is_empty());

    let first = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        shared.clone(),
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let same_root = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(
            shared_root.join("..").join("shared"),
            1,
            "production",
        )
        .await
        .unwrap(),
    );
    let second = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        same_root,
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let before_conflict = repository.load_versioned_plan_catalog().await.unwrap();

    let different_root = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(
            artifact_directory.path().join("different"),
            1,
            "production",
        )
        .await
        .unwrap(),
    );
    assert_ne!(
        shared.deployment_contract().store_id(),
        different_root.deployment_contract().store_id()
    );
    let conflict = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        different_root,
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap_err();
    assert_eq!(
        conflict.code(),
        "PLATFORM_ARTIFACT_STORE_AUTHORITY_CONFLICT"
    );
    let after_conflict = repository.load_versioned_plan_catalog().await.unwrap();
    assert_eq!(after_conflict.plans().len(), before_conflict.plans().len());
    assert_eq!(after_conflict.heads().len(), before_conflict.heads().len());

    second.shutdown(Duration::from_secs(1)).await.unwrap();
    first.shutdown(Duration::from_secs(1)).await.unwrap();

    // Start with no public sources, then simulate another runtime publishing a
    // Retrieval-only revision. Admission must still enforce the shared-broker
    // gate without inventing a live LLM worker requirement.
    let (retrieval_catalog, mut mixed_workers) = public_retrieval_streaming_fixture();
    let retrieval_deployment = retrieval_catalog.get(PUBLIC_RETRIEVAL_AGENT_ID).unwrap();
    mixed_workers
        .register(
            SchedulerTaskKind::Action,
            "fixture.answer",
            VersionTag::new("1").unwrap(),
            VersionTag::new(WORKER_VERSION).unwrap(),
            Arc::new(FixtureExecutor),
        )
        .unwrap();
    let admission_service = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        mixed_workers,
        shared,
        production_config(Duration::from_secs(3_600))
            .with_run_stream_topology(RunStreamDeploymentTopology::Distributed),
    )
    .await
    .unwrap();
    repository
        .publish_builtin_versioned_plan(retrieval_deployment.versioned_plan())
        .await
        .unwrap();
    let admission_error = admission_service
        .create_detached(
            PUBLIC_RETRIEVAL_AGENT_ID,
            json!({"question": "WBC"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        admission_error.code(),
        "PLATFORM_DISTRIBUTED_REQUIRES_SHARED_LIVE_RUN_STREAM_BROKER"
    );
    admission_service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();

    drop(repository);
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn executor_panic_is_durable_and_does_not_kill_the_only_worker_pump() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory
        .path()
        .join("runtime-terminal-task-failure.sqlite");
    database::provision_sqlite_database(&database).await;
    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let mut service_config = config(Duration::from_millis(5));
    service_config.max_concurrent_operations = 1;
    service_config.max_concurrent_operations_per_run = 1;
    let service = RunService::start(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        panics_once_workers(),
        service_config,
    )
    .await
    .unwrap();

    let mut first = service
        .create_attached(
            "runtime_fixture",
            json!({"question": "panic once"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let record = service.get_run(&first.run_id).await.unwrap();
        if record.status().is_terminal() {
            assert_eq!(record.status(), RunStatus::Failed);
            let insight_agent_platform::history::types::RunLifecycle::Failed { error } =
                record.lifecycle
            else {
                unreachable!("terminal status was checked above")
            };
            assert_eq!(error.code, "SCHEDULER_INTERNAL_FAILURE");
            break;
        }
        assert!(Instant::now() < deadline, "panicked task was stranded");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let run_id = insight_agent_platform::engine::RunId::new(first.run_id.clone()).unwrap();
    let facts = repository.load_scheduler_facts(&run_id).await.unwrap();
    let failure = facts
        .task_outcomes()
        .values()
        .find_map(|outcome| match outcome {
            TaskOutcomeFact::Failed { failure } => Some(failure),
            TaskOutcomeFact::Succeeded { .. } => None,
        })
        .expect("panic must commit one durable task failure");
    assert_eq!(failure.code(), "WORKER_EXECUTOR_PANICKED");
    assert_eq!(failure.class(), WorkerFailureClass::EffectOutcomeUnknown);

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let public_failures = sqlx::query_as::<_, (String, String, i64, i64)>(
        "SELECT outbox.event_kind,outbox.causation_event_id,event.seq,outbox.public_ordinal
         FROM public_event_outbox outbox
         JOIN execution_events event
           ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
         WHERE outbox.run_id=? AND outbox.event_kind IN ('operation.failed','run.failed')
         ORDER BY event.seq,outbox.public_ordinal",
    )
    .bind(&first.run_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(public_failures.len(), 2, "{public_failures:?}");
    assert_eq!(public_failures[0].0, "operation.failed");
    assert_eq!(public_failures[1].0, "run.failed");
    assert_ne!(public_failures[0].1, public_failures[1].1);
    assert!(public_failures[0].2 < public_failures[1].2);
    assert_eq!((public_failures[0].3, public_failures[1].3), (40, 50));
    let terminal_authority = sqlx::query_as::<_, (String, i64, i64, i64, i64)>(
        "SELECT rr.registration_kind,rr.artifact_count,
                CASE WHEN rr.event_id=r.terminal_event_id THEN 1 ELSE 0 END,
                (SELECT COUNT(*) FROM run_stream_snapshots s WHERE s.run_id=r.run_id),
                (SELECT COUNT(*) FROM public_event_outbox o
                 WHERE o.run_id=r.run_id AND o.is_terminal=1)
         FROM workflow_runs r
         JOIN artifact_retention_releases rr ON rr.run_id=r.run_id
         WHERE r.run_id=?",
    )
    .bind(&first.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        terminal_authority,
        ("terminal_atomic".to_owned(), 0, 1, 1, 1)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_runs r
             LEFT JOIN artifact_retention_releases rr ON rr.run_id=r.run_id
             WHERE r.run_id=? AND rr.run_id IS NULL",
        )
        .bind(&first.run_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "failed terminal Run must not require the compensation scanner"
    );

    use insight_agent_platform::events::protocol::RunEventType;
    let mut public_sequence = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), first.subscription.recv())
            .await
            .unwrap()
            .unwrap();
        let terminal = event.event_type == RunEventType::RunFailed;
        public_sequence.push(event.event_type);
        if terminal {
            break;
        }
    }
    assert_eq!(
        public_sequence,
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::OperationStarted,
            RunEventType::OperationFailed,
            RunEventType::RunFailed,
        ]
    );
    assert_eq!(
        first.subscription.recv().await.unwrap_err().code(),
        "SUBSCRIPTION_TERMINAL"
    );

    // With one configured worker, success here proves the same supervised
    // pump survived the panic and retained runtime capacity.
    let second = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "worker still alive"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_terminal(&service, &second.run_id).await;
    pool.close().await;
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn sqlite_restart_resumes_nonterminal_run_and_preserves_public_identity() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime.sqlite");
    database::provision_sqlite_database(&database).await;
    let (agents, revision) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let first = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let created = first
        .create_detached(
            "runtime_fixture",
            json!({"question": "resume me"}),
            RequestMetadata {
                request_id: Some("request-restart-1".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(created.status(), RunStatus::Created);
    assert_eq!(created.request_id, "request-restart-1");
    assert_eq!(created.attachment, RunAttachment::Detached);
    assert_eq!(created.agent_version, revision);
    let run_id = created.run_id.clone();
    first.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(first);

    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let second = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    wait_for_terminal(&second, &run_id).await;
    let completed = second.get_run(&run_id).await.unwrap();
    assert_eq!(completed.request_id, "request-restart-1");
    assert_eq!(completed.attachment, RunAttachment::Detached);
    assert_eq!(completed.agent_version, revision);
    let execution_graph = second.execution_graph(&run_id).await.unwrap();
    assert!(!execution_graph.nodes().is_empty());
    let trace = second.trace_overlay(&run_id).await.unwrap();
    assert_eq!(trace.graph_document_id(), execution_graph.document_id());
    assert!(!trace.activations().is_empty());
    second.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn sqlite_restart_rejects_llm_descriptor_v1_without_applying_v2_defaults_and_recovers_v2() {
    let directory = tempfile::tempdir().unwrap();
    let legacy_database = directory.path().join("llm-descriptor-v1.sqlite");
    let current_database = directory.path().join("llm-descriptor-v2.sqlite");
    database::provision_sqlite_database(&legacy_database).await;
    database::provision_sqlite_database(&current_database).await;
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let (deployment, models) = clean_cutover_fixture(Arc::clone(&provider_calls));
    let legacy_plan = legacy_llm_descriptor_v1(&deployment);
    let legacy_run_id = RunId::new("run_llm_descriptor_v1_nonterminal").unwrap();

    let legacy_repository =
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &legacy_database,
        )
        .await
        .unwrap();
    legacy_repository
        .publish_builtin_versioned_plan(&legacy_plan)
        .await
        .unwrap();
    assert!(matches!(
        legacy_repository
            .create_run(
                TransitionKey::derive("clean-cutover.legacy-create", &[legacy_run_id.as_str()])
                    .unwrap(),
                CreateRunCommand::new(
                    legacy_run_id.clone(),
                    &legacy_plan,
                    json!({"question": "must never reach a provider"}),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let stored_legacy = legacy_repository
        .load_versioned_plan_catalog()
        .await
        .unwrap();
    assert_eq!(stored_legacy.plans(), std::slice::from_ref(&legacy_plan));
    let legacy_wire = serde_json::to_value(&stored_legacy.plans()[0]).unwrap();
    let legacy_llm = legacy_wire["canonical_plan"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["kind"]["kind"] == json!("llm_task"))
        .unwrap();
    assert_eq!(
        legacy_llm["kind"]["descriptor"]["descriptor_version"],
        json!("1")
    );
    let legacy_configuration = legacy_llm["kind"]["descriptor"]["public_configuration"]
        .as_object()
        .unwrap();
    assert!(!legacy_configuration.contains_key("stream"));
    assert!(!legacy_configuration.contains_key("publish"));
    assert!(!legacy_repository
        .load_run(&legacy_run_id)
        .await
        .unwrap()
        .unwrap()
        .lifecycle()
        .is_terminal());
    drop(legacy_repository);

    let legacy_restart_repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &legacy_database,
        )
        .await
        .unwrap(),
    );
    let legacy_restart_error = RunService::start(
        DeployedAgentCatalog::default(),
        legacy_restart_repository.clone() as Arc<dyn ProductionRunRepository>,
        production_worker_registry(&models, &ActionRegistry::default()).unwrap(),
        config(Duration::from_millis(5)),
    )
    .await
    .unwrap_err();
    assert_eq!(legacy_restart_error.code(), "STORED_DEPLOYMENT_INVALID");
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        legacy_restart_repository
            .load_versioned_plan_catalog()
            .await
            .unwrap()
            .plans(),
        &[legacy_plan]
    );
    assert!(!legacy_restart_repository
        .load_run(&legacy_run_id)
        .await
        .unwrap()
        .unwrap()
        .lifecycle()
        .is_terminal());
    drop(legacy_restart_repository);

    let current_repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &current_database,
        )
        .await
        .unwrap(),
    );
    let first = RunService::start(
        clean_cutover_catalog(&deployment),
        current_repository as Arc<dyn ProductionRunRepository>,
        production_worker_registry(&models, &ActionRegistry::default()).unwrap(),
        config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let current = first
        .create_detached(
            CLEAN_CUTOVER_AGENT_ID,
            json!({"question": "resume the exact descriptor v2 plan"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(current.status(), RunStatus::Created);
    let current_run_id = current.run_id.clone();
    first.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(first);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);

    let current_repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &current_database,
        )
        .await
        .unwrap(),
    );
    let stored_current = current_repository
        .load_versioned_plan_catalog()
        .await
        .unwrap();
    let current_wire = serde_json::to_value(&stored_current.plans()[0]).unwrap();
    let current_llm = current_wire["canonical_plan"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["kind"]["kind"] == json!("llm_task"))
        .unwrap();
    assert_eq!(
        current_llm["kind"]["descriptor"]["descriptor_version"],
        json!("2")
    );
    assert_eq!(
        current_llm["kind"]["descriptor"]["public_configuration"]["stream"],
        json!({"type": "boolean", "value": false})
    );
    assert_eq!(
        current_llm["kind"]["descriptor"]["public_configuration"]["publish"],
        json!({"type": "boolean", "value": false})
    );
    let second = RunService::start(
        DeployedAgentCatalog::default(),
        current_repository as Arc<dyn ProductionRunRepository>,
        production_worker_registry(&models, &ActionRegistry::default()).unwrap(),
        config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    wait_for_terminal(&second, &current_run_id).await;
    assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
    second.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn attached_subscription_delivers_one_terminal_event_then_reaches_eof() {
    let (agents, revision) = deployed_catalog();
    let (_database, repository) = database::temporary_sqlite_repository().await;
    let repository = Arc::new(repository);
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    let mut attached = service
        .create_attached(
            "runtime_fixture",
            json!({"question": "public-event-secret-marker"}),
            RequestMetadata {
                request_id: Some("request-stream-1".to_owned()),
            },
        )
        .await
        .unwrap();
    let mut events = Vec::new();
    for _ in 0..16 {
        let event = tokio::time::timeout(Duration::from_secs(2), attached.subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.request_id, "request-stream-1");
        assert_eq!(event.agent_version, revision);
        let encoded = serde_json::to_string(&event).unwrap();
        assert!(!encoded.contains("public-event-secret-marker"));
        assert!(!encoded.contains("answered"));
        let terminal = matches!(
            event.event_type,
            insight_agent_platform::events::protocol::RunEventType::RunCompleted
                | insight_agent_platform::events::protocol::RunEventType::RunFailed
                | insight_agent_platform::events::protocol::RunEventType::RunCancelled
                | insight_agent_platform::events::protocol::RunEventType::RunInterrupted
        );
        events.push(event);
        if terminal {
            break;
        }
    }
    use insight_agent_platform::events::protocol::RunEventType;
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::OperationStarted,
            RunEventType::OperationCompleted,
            RunEventType::RunCompleted,
        ]
    );
    assert_eq!(events.last().unwrap().code, "OK");
    assert_eq!(
        attached.subscription.recv().await.unwrap_err().code(),
        "SUBSCRIPTION_TERMINAL"
    );
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn postgres_attached_public_lifecycle_is_ordered_private_and_replay_idempotent() {
    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        return;
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("public_lifecycle_{}", &suffix[..16]);
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    let version: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(&admin)
        .await
        .unwrap();
    assert!((160_000..170_000).contains(&version.parse::<u32>().unwrap()));
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    database::provision_postgres_url(&scoped_url).await;
    let runtime_url = format!("{scoped_url}&application_name=insight-agent-platform-runtime");
    let repository = Arc::new(
        insight_agent_platform::engine::repository::PostgresDurableRepository::connect(
            &runtime_url,
        )
        .await
        .unwrap(),
    );
    let control = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&scoped_url)
        .await
        .unwrap();
    let mut public_listener = sqlx::postgres::PgListener::connect_with(&control)
        .await
        .unwrap();
    public_listener
        .listen(PUBLIC_EVENT_NOTIFY_CHANNEL)
        .await
        .unwrap();

    let (agents, revision) = deployed_catalog();
    let artifact_directory = tempfile::tempdir().unwrap();
    let artifact_store = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(
            artifact_directory.path().join("objects"),
            1,
            "public_lifecycle",
        )
        .await
        .unwrap(),
    );
    let service = RunService::start_with_artifact_store(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        artifact_store.clone(),
        production_config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    let mut attached = service
        .create_attached(
            "runtime_fixture",
            json!({"question": "postgres-public-secret-marker"}),
            RequestMetadata {
                request_id: Some("request-postgres-public-lifecycle".to_owned()),
            },
        )
        .await
        .unwrap();
    use insight_agent_platform::events::protocol::RunEventType;
    let mut live_kinds = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), attached.subscription.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.agent_version, revision);
        let encoded = serde_json::to_string(&event).unwrap();
        assert!(!encoded.contains("postgres-public-secret-marker"));
        assert!(!encoded.contains("answered"));
        let terminal = event.event_type == RunEventType::RunCompleted;
        live_kinds.push(event.event_type);
        if terminal {
            break;
        }
    }
    assert_eq!(
        live_kinds,
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::OperationStarted,
            RunEventType::OperationCompleted,
            RunEventType::RunCompleted,
        ]
    );
    assert_eq!(
        attached.subscription.recv().await.unwrap_err().code(),
        "SUBSCRIPTION_TERMINAL"
    );
    let rows = sqlx::query_as::<_, (String, String, String, i64, i32)>(
        "SELECT outbox.event_kind,outbox.public_event_id,outbox.safe_envelope::text,
                event.seq,outbox.public_ordinal
         FROM public_event_outbox outbox
         JOIN execution_events event
           ON event.run_id=outbox.run_id AND event.event_id=outbox.causation_event_id
         WHERE outbox.run_id=$1
         ORDER BY event.seq,outbox.public_ordinal",
    )
    .bind(&attached.run_id)
    .fetch_all(&control)
    .await
    .unwrap();
    let notification_deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    loop {
        match tokio::time::timeout_at(notification_deadline, public_listener.recv()).await {
            Ok(Ok(notification)) => assert!(
                !rows
                    .iter()
                    .any(|row| row.1.as_str() == notification.payload()),
                "runtime publication must use local delivery without a per-event transactional NOTIFY"
            ),
            Ok(Err(error)) => panic!("public-event listener failed: {error}"),
            Err(_) => break,
        }
    }
    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
        vec![
            "run.created",
            "run.started",
            "operation.started",
            "operation.completed",
            "run.completed",
        ]
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.1.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        rows.len()
    );
    assert!(rows.iter().all(|row| {
        !row.2.contains("postgres-public-secret-marker") && !row.2.contains("answered")
    }));
    let authoritative_ids = rows.iter().map(|row| row.1.clone()).collect::<Vec<_>>();

    service.reconcile_startup().await.unwrap();
    let replay_ids = sqlx::query_scalar::<_, String>(
        "SELECT public_event_id FROM public_event_outbox WHERE run_id=$1
         ORDER BY public_event_id",
    )
    .bind(&attached.run_id)
    .fetch_all(&control)
    .await
    .unwrap();
    let mut expected_ids = authoritative_ids;
    expected_ids.sort();
    assert_eq!(replay_ids, expected_ids);

    let succeeded_authority = sqlx::query_as::<_, (String, i64, i64, f64, i64, i64, i64)>(
        "SELECT rr.registration_kind,rr.artifact_count,
                r.artifact_reference_retention_seconds,
                EXTRACT(EPOCH FROM (rr.retain_until-r.terminal_at))::double precision,
                (SELECT COUNT(*) FROM run_stream_snapshots s WHERE s.run_id=r.run_id),
                (SELECT COUNT(*) FROM public_event_outbox o
                 WHERE o.run_id=r.run_id AND o.is_terminal),
                (CASE WHEN rr.event_id=r.terminal_event_id THEN 1 ELSE 0 END)::bigint
         FROM workflow_runs r
         JOIN artifact_retention_releases rr ON rr.run_id=r.run_id
         WHERE r.run_id=$1",
    )
    .bind(&attached.run_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(succeeded_authority.0, "terminal_atomic");
    assert!(
        succeeded_authority.1 > 0,
        "PostgreSQL success fixture must exercise N referenced Artifacts"
    );
    assert_eq!(succeeded_authority.2, 60);
    assert!((succeeded_authority.3 - 60.0).abs() < 0.01);
    assert_eq!(
        (
            succeeded_authority.4,
            succeeded_authority.5,
            succeeded_authority.6
        ),
        (1, 1, 1)
    );

    let publisher_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let published = service
            .prometheus_metrics()
            .lines()
            .find_map(|line| {
                line.strip_prefix(
                    "insight_runtime_notification_publisher_total{backend=\"postgres\",outcome=\"published\"} ",
                )
            })
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        if published > 0 {
            break;
        }
        assert!(
            Instant::now() < publisher_deadline,
            "process-coalesced PostgreSQL work hint was not published"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    service.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(service);

    let failing = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        panics_once_workers(),
        artifact_store.clone(),
        production_config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    let failed = failing
        .create_detached(
            "runtime_fixture",
            json!({"question": "postgres atomic terminal failure"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let record = failing.get_run(&failed.run_id).await.unwrap();
        if record.status().is_terminal() {
            assert_eq!(record.status(), RunStatus::Failed);
            break;
        }
        assert!(Instant::now() < deadline, "PostgreSQL Run did not fail");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    failing.shutdown(Duration::from_secs(2)).await.unwrap();

    let cancelling = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        artifact_store,
        production_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let cancel_target = cancelling
        .create_detached(
            "runtime_fixture",
            json!({"question": "postgres atomic terminal cancellation"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        cancelling
            .cancel(&cancel_target.run_id)
            .await
            .unwrap()
            .status(),
        RunStatus::Cancelled
    );
    cancelling.shutdown(Duration::from_secs(2)).await.unwrap();

    let terminal_rows = sqlx::query_as::<_, (String, String, String, i64, i64, i64, i64)>(
        "SELECT r.run_id,r.lifecycle,rr.registration_kind,rr.artifact_count,
                (SELECT COUNT(*) FROM run_stream_snapshots s WHERE s.run_id=r.run_id),
                (SELECT COUNT(*) FROM public_event_outbox o
                 WHERE o.run_id=r.run_id AND o.is_terminal),
                (CASE WHEN rr.event_id=r.terminal_event_id THEN 1 ELSE 0 END)::bigint
         FROM workflow_runs r
         JOIN artifact_retention_releases rr ON rr.run_id=r.run_id
         WHERE r.run_id=ANY($1)
         ORDER BY r.run_id",
    )
    .bind(vec![
        attached.run_id.clone(),
        failed.run_id.clone(),
        cancel_target.run_id.clone(),
    ])
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(terminal_rows.len(), 3);
    assert!(terminal_rows
        .iter()
        .all(|row| { row.2 == "terminal_atomic" && row.4 == 1 && row.5 == 1 && row.6 == 1 }));
    assert!(terminal_rows
        .iter()
        .any(|row| row.1 == "succeeded" && row.3 > 0));
    assert!(terminal_rows
        .iter()
        .any(|row| row.1 == "failed" && row.3 == 0));
    assert!(terminal_rows
        .iter()
        .any(|row| row.1 == "cancelled" && row.3 == 0));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_runs r
             LEFT JOIN artifact_retention_releases rr ON rr.run_id=r.run_id
             WHERE r.run_id=ANY($1) AND rr.run_id IS NULL",
        )
        .bind(vec![
            attached.run_id.clone(),
            failed.run_id.clone(),
            cancel_target.run_id.clone(),
        ])
        .fetch_one(&control)
        .await
        .unwrap(),
        0
    );

    drop(repository);
    drop(public_listener);
    control.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn cancel_is_durable_and_idempotently_visible_through_get() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime-cancel-retention.sqlite");
    database::provision_sqlite_database(&database).await;
    let (agents, revision) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let service = RunService::start(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let created = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "cancel me"}),
            RequestMetadata {
                request_id: Some("request-cancel-1".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(created.status(), RunStatus::Created);
    let cancelled = service.cancel(&created.run_id).await.unwrap();
    assert_eq!(cancelled.status(), RunStatus::Cancelled);
    assert_eq!(cancelled.request_id, "request-cancel-1");
    assert_eq!(cancelled.agent_version, revision);
    assert_eq!(
        service.get_run(&created.run_id).await.unwrap().status(),
        RunStatus::Cancelled
    );
    assert_eq!(
        service.cancel(&created.run_id).await.unwrap().status(),
        RunStatus::Cancelled
    );
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_as::<_, (String, i64, i64, i64)>(
            "SELECT rr.registration_kind,rr.artifact_count,
                    (SELECT COUNT(*) FROM run_stream_snapshots s WHERE s.run_id=r.run_id),
                    (SELECT COUNT(*) FROM public_event_outbox o
                     WHERE o.run_id=r.run_id AND o.is_terminal=1)
             FROM workflow_runs r
             JOIN artifact_retention_releases rr ON rr.run_id=r.run_id
             WHERE r.run_id=?",
        )
        .bind(&created.run_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        ("terminal_atomic".to_owned(), 0, 1, 1)
    );
    pool.close().await;
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn terminal_artifact_retention_abort_rolls_back_and_replays_the_whole_terminal_bundle() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory
        .path()
        .join("runtime-terminal-retention-abort.sqlite");
    database::provision_sqlite_database(&database).await;
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER fail_terminal_atomic_retention
         AFTER INSERT ON artifact_retention_releases
         WHEN NEW.registration_kind='terminal_atomic'
         BEGIN SELECT RAISE(ABORT, 'forced terminal retention abort'); END",
    )
    .execute(&control)
    .await
    .unwrap();
    let service = RunService::start(
        deployed_catalog().0,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    let created = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "force atomic terminal rollback"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let succeeded_activations = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_activations WHERE run_id=? AND lifecycle='succeeded'",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap();
        if succeeded_activations > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Run never reached the forced terminal transaction"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Give at least one terminal retry time to reach the aborting statement.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64, i64)>(
            "SELECT CASE WHEN terminal_at IS NULL THEN 0 ELSE 1 END,
                    (SELECT COUNT(*) FROM run_stream_snapshots s WHERE s.run_id=r.run_id),
                    (SELECT COUNT(*) FROM public_event_outbox o
                     WHERE o.run_id=r.run_id AND o.is_terminal=1),
                    (SELECT COUNT(*) FROM artifact_retention_releases rr
                     WHERE rr.run_id=r.run_id)
             FROM workflow_runs r WHERE r.run_id=?",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        (0, 0, 0, 0),
        "an abort at retention registration must roll back every terminal authority"
    );

    sqlx::query("DROP TRIGGER fail_terminal_atomic_retention")
        .execute(&control)
        .await
        .unwrap();
    wait_for_terminal(&service, &created.run_id).await;
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64, i64, String, i64)>(
            "SELECT CASE WHEN r.terminal_at IS NULL THEN 0 ELSE 1 END,
                    (SELECT COUNT(*) FROM run_stream_snapshots s WHERE s.run_id=r.run_id),
                    (SELECT COUNT(*) FROM public_event_outbox o
                     WHERE o.run_id=r.run_id AND o.is_terminal=1),
                    (SELECT COUNT(*) FROM artifact_retention_releases rr2
                     WHERE rr2.run_id=r.run_id),
                    rr.registration_kind,
                    CASE WHEN rr.event_id=r.terminal_event_id THEN 1 ELSE 0 END
             FROM workflow_runs r
             JOIN artifact_retention_releases rr ON rr.run_id=r.run_id
             WHERE r.run_id=?",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap(),
        (1, 1, 1, 1, "terminal_atomic".to_owned(), 1)
    );
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    control.close().await;
}

#[tokio::test]
async fn max_concurrent_runs_bounds_nonterminal_runs_and_reopens_after_drain() {
    let (agents, _) = deployed_catalog();
    let (_database, repository) = database::temporary_sqlite_repository().await;
    let repository = Arc::new(repository);
    let mut service_config = config(Duration::from_secs(3_600));
    service_config.max_concurrent_runs = 1;
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        service_config,
    )
    .await
    .unwrap();

    let first = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "hold the only slot"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), RunStatus::Created);

    let capacity = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "must wait"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(capacity.code(), "RUN_CAPACITY_EXCEEDED");
    let metrics = service.prometheus_metrics();
    assert!(metrics.contains("insight_runtime_admission_total{outcome=\"accepted\"} 1"));
    assert!(metrics.contains("insight_runtime_admission_total{outcome=\"capacity_rejected\"} 1"));
    assert!(metrics.contains("insight_runtime_active_runs{lifecycle_class=\"nonterminal\"} 1"));

    assert_eq!(
        service.cancel(&first.run_id).await.unwrap().status(),
        RunStatus::Cancelled
    );
    let admitted = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "slot reopened"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(admitted.status(), RunStatus::Created);

    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn production_public_event_pruner_keeps_terminal_and_expires_nonterminal_rows() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime-public-retention.sqlite");
    database::provision_sqlite_database(&database).await;
    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let mut service_config = config(Duration::from_millis(5));
    service_config.public_event_nonterminal_retention = Duration::from_secs(1);
    service_config.public_event_prune_interval = Duration::from_millis(5);
    let service = RunService::start(
        agents,
        repository as Arc<dyn ProductionRunRepository>,
        workers(),
        service_config,
    )
    .await
    .unwrap();
    let created = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "exercise public retention"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_terminal(&service, &created.run_id).await;

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let publication_deadline = Instant::now() + Duration::from_secs(5);
    let initial_nonterminal = loop {
        let (terminal, published_nonterminal, total_nonterminal, missing_deadline) =
            sqlx::query_as::<_, (i64, i64, i64, i64)>(
                "SELECT
                    SUM(CASE WHEN is_terminal=1 AND publish_state='published'
                        AND retain_until IS NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN is_terminal=0 AND publish_state='published'
                        THEN 1 ELSE 0 END),
                    SUM(CASE WHEN is_terminal=0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN is_terminal=0 AND publish_state='published'
                        AND retain_until IS NULL THEN 1 ELSE 0 END)
                 FROM public_event_outbox WHERE run_id=?",
            )
            .bind(&created.run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        if terminal == 1 && published_nonterminal > 0 && published_nonterminal == total_nonterminal
        {
            assert_eq!(missing_deadline, 0);
            break total_nonterminal;
        }
        assert!(
            Instant::now() < publication_deadline,
            "public outbox did not reach its published retention boundary"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(initial_nonterminal > 0);

    let prune_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (terminal, nonterminal) = sqlx::query_as::<_, (i64, i64)>(
            "SELECT
                SUM(CASE WHEN is_terminal=1 AND publish_state='published' THEN 1 ELSE 0 END),
                SUM(CASE WHEN is_terminal=0 THEN 1 ELSE 0 END)
             FROM public_event_outbox WHERE run_id=?",
        )
        .bind(&created.run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(terminal, 1, "terminal public authority must remain durable");
        if nonterminal == 0 {
            break;
        }
        assert!(
            Instant::now() < prune_deadline,
            "expired nonterminal public events were not pruned"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    pool.close().await;
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn production_worker_externalizes_large_output_and_commits_reference() {
    use insight_agent_platform::engine::{
        repository::{ArtifactDurableRepository, ReleaseRunArtifactRetentionCommand},
        RunId, TransitionKey, TransitionOutcome,
    };

    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime-artifact.sqlite");
    database::provision_sqlite_database(&database).await;
    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let object_root = directory.path().join("objects");
    let store = Arc::new(
        LocalContentAddressedArtifactStore::open(object_root.clone(), 1)
            .await
            .unwrap(),
    );
    let mut service_config = config(Duration::from_millis(5));
    service_config.artifact_gc_interval = Duration::from_millis(5);
    service_config.artifact_reference_retention = Duration::from_secs(60);
    let service = RunService::start_with_artifact_store(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        store.clone(),
        service_config,
    )
    .await
    .unwrap();
    let created = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "store the answer"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_terminal(&service, &created.run_id).await;

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let content_hash = loop {
        let row = sqlx::query_as::<_, (String, String, i64, Option<String>, i64, Option<f64>)>(
            "SELECT a.artifact_state,a.content_hash,
                    (SELECT COUNT(*) FROM artifact_retention_releases rr WHERE rr.run_id=a.run_id),
                    (SELECT registration_kind FROM artifact_retention_releases rr
                     WHERE rr.run_id=a.run_id),
                    r.artifact_reference_retention_seconds,
                    (SELECT (julianday(rr.retain_until)-julianday(r.terminal_at))*86400.0
                     FROM artifact_retention_releases rr WHERE rr.run_id=a.run_id)
             FROM artifacts a JOIN workflow_runs r ON r.run_id=a.run_id WHERE a.run_id=?",
        )
        .bind(&created.run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        if row.0 == "referenced" && row.2 == 1 {
            assert_eq!(row.3.as_deref(), Some("terminal_atomic"));
            assert_eq!(row.4, 60, "Run admission must freeze service retention");
            assert!(
                row.5.is_some_and(|seconds| (seconds - 60.0).abs() < 0.01),
                "retention deadline must derive from durable terminal_at"
            );
            break row.1;
        }
        assert!(
            Instant::now() < deadline,
            "terminal Artifact retention was not registered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let hash = content_hash.strip_prefix("sha256:").unwrap();
    let object_path = object_root.join(&hash[..2]).join(hash);
    assert!(object_path.exists());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT artifact_state FROM artifacts WHERE run_id=?")
            .bind(&created.run_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "referenced",
        "a caller clock must not bypass the database retention deadline"
    );

    let run_id = RunId::new(created.run_id.clone()).unwrap();
    assert!(repository
        .list_unreleased_terminal_artifact_runs(10)
        .await
        .unwrap()
        .is_empty());
    let event_count_before_replay =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_events WHERE run_id=?")
            .bind(&created.run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let release_key = TransitionKey::derive(
        "production.run-service",
        &["artifact.retention.release", run_id.as_str()],
    )
    .unwrap();
    assert!(matches!(
        repository
            .release_run_artifact_retention(
                release_key.clone(),
                ReleaseRunArtifactRetentionCommand::new(run_id.clone(), 60).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_events WHERE run_id=?")
            .bind(&created.run_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        event_count_before_replay,
        "legacy compensation replay must not append an event for atomic rows"
    );
    assert!(matches!(
        repository
            .release_run_artifact_retention(
                release_key.clone(),
                ReleaseRunArtifactRetentionCommand::new(run_id.clone(), 61).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay { .. }
    ));
    // Simulate a pre-022 terminal Run whose compensating row was never
    // registered. The recovery scanner and legacy command remain supported.
    sqlx::query("DROP TRIGGER artifact_retention_release_delete_forbidden")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM artifact_retention_releases WHERE run_id=?")
        .bind(&created.run_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE artifacts SET retain_until=NULL WHERE run_id=?")
        .bind(&created.run_id)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        repository
            .list_unreleased_terminal_artifact_runs(10)
            .await
            .unwrap(),
        vec![run_id.clone()]
    );
    let compensation = repository
        .release_run_artifact_retention(
            release_key,
            ReleaseRunArtifactRetentionCommand::new(run_id, 61).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            compensation,
            TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. }
        ),
        "legacy compensation did not converge: {compensation:?}"
    );
    let legacy_registration = sqlx::query_as::<_, (String, f64)>(
        "SELECT rr.registration_kind,
                (julianday(rr.retain_until)-julianday(r.terminal_at))*86400.0
         FROM artifact_retention_releases rr
         JOIN workflow_runs r ON r.run_id=rr.run_id WHERE rr.run_id=?",
    )
    .bind(&created.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(legacy_registration.0, "legacy");
    assert!(
        (legacy_registration.1 - 60.0).abs() < 0.01,
        "legacy compensation must use the frozen Run policy, not caller value 61"
    );
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT
                (SELECT COUNT(*) FROM scheduler_values
                 WHERE run_id=? AND storage_kind='artifact') +
                (SELECT COUNT(*) FROM scheduler_occurrence_values
                 WHERE run_id=? AND storage_kind='artifact')",
        )
        .bind(&created.run_id)
        .bind(&created.run_id)
        .fetch_one(&pool)
        .await
        .unwrap()
            > 0
    );

    sqlx::query(
        "UPDATE artifact_retention_releases
         SET retain_until=STRFTIME('%Y-%m-%dT%H:%M:%fZ','now','-1 second')
         WHERE run_id=?",
    )
    .bind(&created.run_id)
    .execute(&pool)
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let state =
            sqlx::query_scalar::<_, String>("SELECT artifact_state FROM artifacts WHERE run_id=?")
                .bind(&created.run_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        if state == "deleted" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "referenced Artifact was not collected after its durable deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!object_path.exists());
    pool.close().await;
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn production_artifact_gc_deletes_verified_unreferenced_object() {
    use insight_agent_platform::engine::repository::{
        ArtifactDurableRepository, StageArtifactCommand, VerifyArtifactCommand,
    };

    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime-artifact-gc.sqlite");
    database::provision_sqlite_database(&database).await;
    let (agents, _) = deployed_catalog();
    let repository = Arc::new(
        insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
            &database,
        )
        .await
        .unwrap(),
    );
    let object_root = directory.path().join("objects");
    let store = Arc::new(
        LocalContentAddressedArtifactStore::open(object_root.clone(), 1)
            .await
            .unwrap(),
    );
    let mut service_config = config(Duration::from_millis(5));
    service_config.artifact_orphan_retention = Duration::from_secs(1);
    service_config.artifact_gc_interval = Duration::from_millis(5);
    let service = RunService::start_with_artifact_store(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        store.clone(),
        service_config,
    )
    .await
    .unwrap();
    let run = service
        .create_detached(
            "runtime_fixture",
            json!({"question": "create a GC authority Run"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_terminal(&service, &run.run_id).await;
    let run_id = insight_agent_platform::engine::RunId::new(run.run_id.clone()).unwrap();

    let bytes = b"verified-but-never-referenced";
    let artifact = store
        .artifact_for_bytes(bytes, Some("application/octet-stream".to_owned()))
        .unwrap();
    let locator = store.storage_locator(&artifact).unwrap();
    let hash_hex = artifact
        .content_hash()
        .as_str()
        .strip_prefix("sha256:")
        .unwrap();
    let object_path = object_root.join(&hash_hex[..2]).join(hash_hex);
    repository
        .stage_artifact(StageArtifactCommand::new(
            run_id.clone(),
            artifact.clone(),
            locator,
            None,
        ))
        .await
        .unwrap();
    let (hash, size) = store.put_and_verify(&artifact, bytes).await.unwrap();
    repository
        .verify_artifact(VerifyArtifactCommand::new(
            run_id,
            artifact.artifact_id().clone(),
            hash,
            size,
        ))
        .await
        .unwrap();

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let state = sqlx::query_scalar::<_, String>(
            "SELECT artifact_state FROM artifacts WHERE run_id=? AND artifact_id=?",
        )
        .bind(&run.run_id)
        .bind(artifact.artifact_id().as_str())
        .fetch_one(&pool)
        .await
        .unwrap();
        if state == "deleted" {
            break;
        }
        assert!(Instant::now() < deadline, "artifact GC did not settle");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!object_path.exists());
    pool.close().await;
    service.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn postgres_background_pumps_externalize_prune_and_gc_across_shared_store_restart() {
    use insight_agent_platform::engine::repository::{
        ArtifactDurableRepository, StageArtifactCommand, VerifyArtifactCommand,
    };

    let Ok(database_url) = std::env::var("TEST_POSTGRES_URL") else {
        eprintln!("skipping real PostgreSQL Artifact/public-event background-pump test");
        return;
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("background_artifact_{}", &suffix[..16]);
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    database::provision_postgres_url(&scoped_url).await;
    let repository = Arc::new(
        insight_agent_platform::engine::repository::PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let control = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();

    let artifact_directory = tempfile::tempdir().unwrap();
    let object_root = artifact_directory.path().join("objects");
    let store = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(
            object_root.clone(),
            1,
            "pg_background_pumps",
        )
        .await
        .unwrap(),
    );
    let mut first_config = production_config(Duration::from_millis(5));
    first_config.artifact_gc_interval = Duration::from_millis(5);
    first_config.public_event_nonterminal_retention = Duration::from_secs(1);
    first_config.public_event_prune_interval = Duration::from_secs(3_600);
    let first = RunService::start_with_artifact_store(
        deployed_catalog().0,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        store.clone(),
        first_config,
    )
    .await
    .unwrap();
    let created = first
        .create_detached(
            "runtime_fixture",
            json!({"question": "postgres background Artifact evidence"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    wait_for_terminal(&first, &created.run_id).await;

    let deadline = Instant::now() + Duration::from_secs(10);
    let referenced_hash = loop {
        let artifact =
            sqlx::query_as::<_, (String, String, i64, Option<String>, i64, Option<f64>)>(
                "SELECT a.artifact_state,a.content_hash,
                    (SELECT COUNT(*) FROM artifact_retention_releases rr
                     WHERE rr.run_id=a.run_id),
                    (SELECT registration_kind FROM artifact_retention_releases rr
                     WHERE rr.run_id=a.run_id),
                    r.artifact_reference_retention_seconds,
                    (SELECT EXTRACT(EPOCH FROM (rr.retain_until-r.terminal_at))::double precision
                     FROM artifact_retention_releases rr WHERE rr.run_id=a.run_id)
             FROM artifacts a JOIN workflow_runs r ON r.run_id=a.run_id WHERE a.run_id=$1",
            )
            .bind(&created.run_id)
            .fetch_one(&control)
            .await
            .unwrap();
        let public = sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT
                COUNT(*) FILTER (WHERE is_terminal AND publish_state='published'),
                COUNT(*) FILTER (WHERE NOT is_terminal),
                COUNT(*) FILTER (WHERE NOT is_terminal AND publish_state='published')
             FROM public_event_outbox WHERE run_id=$1",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap();
        if artifact.0 == "referenced"
            && artifact.2 == 1
            && public.0 == 1
            && public.1 > 0
            && public.1 == public.2
        {
            assert_eq!(artifact.3.as_deref(), Some("terminal_atomic"));
            assert_eq!(artifact.4, 60);
            assert!(artifact
                .5
                .is_some_and(|seconds| (seconds - 60.0).abs() < 0.01));
            break artifact.1;
        }
        assert!(
            Instant::now() < deadline,
            "PostgreSQL background pumps did not publish and reference terminal output"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let referenced_hex = referenced_hash.strip_prefix("sha256:").unwrap();
    let referenced_path = object_root.join(&referenced_hex[..2]).join(referenced_hex);
    assert!(referenced_path.is_file());
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT
                (SELECT COUNT(*) FROM scheduler_values
                 WHERE run_id=$1 AND storage_kind='artifact') +
                (SELECT COUNT(*) FROM scheduler_occurrence_values
                 WHERE run_id=$1 AND storage_kind='artifact')",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap()
            > 0
    );

    first.shutdown(Duration::from_secs(1)).await.unwrap();

    let restarted_repository = Arc::new(
        insight_agent_platform::engine::repository::PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    let restarted_store = Arc::new(
        LocalContentAddressedArtifactStore::open_shared(
            object_root.join("..").join("objects"),
            1,
            "pg_background_pumps",
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        store.deployment_contract(),
        restarted_store.deployment_contract()
    );
    let mut restarted_config = production_config(Duration::from_millis(5));
    restarted_config.artifact_orphan_retention = Duration::from_secs(1);
    restarted_config.artifact_gc_interval = Duration::from_millis(5);
    restarted_config.public_event_nonterminal_retention = Duration::from_secs(1);
    restarted_config.public_event_prune_interval = Duration::from_millis(5);
    let restarted = RunService::start_with_artifact_store(
        deployed_catalog().0,
        restarted_repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(),
        restarted_store.clone(),
        restarted_config,
    )
    .await
    .unwrap();

    let prune_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (terminal, nonterminal) = sqlx::query_as::<_, (i64, i64)>(
            "SELECT
                COUNT(*) FILTER (WHERE is_terminal AND publish_state='published'),
                COUNT(*) FILTER (WHERE NOT is_terminal)
             FROM public_event_outbox WHERE run_id=$1",
        )
        .bind(&created.run_id)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(terminal, 1, "terminal public authority must remain durable");
        if nonterminal == 0 {
            break;
        }
        assert!(
            Instant::now() < prune_deadline,
            "PostgreSQL nonterminal public events were not pruned after restart"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(referenced_path.is_file());

    let run_id = insight_agent_platform::engine::RunId::new(created.run_id.clone()).unwrap();
    let orphan_bytes = b"postgres-verified-but-never-referenced";
    let orphan = restarted_store
        .artifact_for_bytes(orphan_bytes, Some("application/octet-stream".to_owned()))
        .unwrap();
    let orphan_locator = restarted_store.storage_locator(&orphan).unwrap();
    let orphan_hex = orphan
        .content_hash()
        .as_str()
        .strip_prefix("sha256:")
        .unwrap();
    let orphan_path = object_root.join(&orphan_hex[..2]).join(orphan_hex);
    restarted_repository
        .stage_artifact(StageArtifactCommand::new(
            run_id,
            orphan.clone(),
            orphan_locator,
            None,
        ))
        .await
        .unwrap();
    let (orphan_hash, orphan_size) = restarted_store
        .put_and_verify(&orphan, orphan_bytes)
        .await
        .unwrap();
    restarted_repository
        .verify_artifact(VerifyArtifactCommand::new(
            insight_agent_platform::engine::RunId::new(created.run_id.clone()).unwrap(),
            orphan.artifact_id().clone(),
            orphan_hash,
            orphan_size,
        ))
        .await
        .unwrap();
    assert!(orphan_path.is_file());
    sqlx::query(
        "UPDATE artifacts
         SET created_at=CURRENT_TIMESTAMP-INTERVAL '10 seconds'
         WHERE run_id=$1 AND artifact_id=$2",
    )
    .bind(&created.run_id)
    .bind(orphan.artifact_id().as_str())
    .execute(&control)
    .await
    .unwrap();

    let gc_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let state = sqlx::query_scalar::<_, String>(
            "SELECT artifact_state FROM artifacts WHERE run_id=$1 AND artifact_id=$2",
        )
        .bind(&created.run_id)
        .bind(orphan.artifact_id().as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        if state == "deleted" {
            break;
        }
        assert!(
            Instant::now() < gc_deadline,
            "PostgreSQL background Artifact GC did not delete the orphan"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!orphan_path.exists());

    sqlx::query(
        "UPDATE artifact_retention_releases
         SET retain_until=CURRENT_TIMESTAMP-INTERVAL '1 second'
         WHERE run_id=$1",
    )
    .bind(&created.run_id)
    .execute(&control)
    .await
    .unwrap();
    let referenced_gc_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let state = sqlx::query_scalar::<_, String>(
            "SELECT artifact_state FROM artifacts
             WHERE run_id=$1 AND content_hash=$2",
        )
        .bind(&created.run_id)
        .bind(&referenced_hash)
        .fetch_one(&control)
        .await
        .unwrap();
        if state == "deleted" {
            break;
        }
        assert!(
            Instant::now() < referenced_gc_deadline,
            "PostgreSQL referenced Artifact was not collected after its durable deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!referenced_path.exists());

    restarted.shutdown(Duration::from_secs(1)).await.unwrap();
    control.close().await;
    drop(restarted_repository);
    drop(repository);
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
