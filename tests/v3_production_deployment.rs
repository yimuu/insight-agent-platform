use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    catalog_v3::{
        compile_v3_agent_dir, DeployedV3Agent, ProductionLeafDeploymentResolver,
        VersionedLeafAdapterIdentity, VersionedLeafAdapterRegistration,
        VersionedLeafAdapterRegistry,
    },
    dsl::CompileError,
    engine::{
        plan::DescriptorValue, production_worker_registry_with_leaf_adapters,
        repository::PostgresDurableRepository, EffectEvidence, EffectIdempotency, LeafTaskExecutor,
        LeafTaskKind, LocalContentAddressedArtifactStore, RuntimeValue, SchedulerTaskKind,
        SubflowContractRegistry, TaskExecutionRequest, TaskExecutionResult, V3LlmTaskExecutor,
        VersionTag, WorkerCancellation, WorkerEffectClass, WorkerEffectPolicy,
        WorkerExecutionContext, WorkerExecutorRegistry, WorkerFailure,
    },
    history::types::{RunLifecycle, RunStatus},
    resources::{
        actions::ActionRegistry,
        models::{
            ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability,
            ModelDeploymentIdentity, ModelRegistry,
        },
    },
    runtime::{
        DeployedAgentCatalog, ProductionRunRepository, RequestMetadata, RunError, RunService,
        RunServiceConfig,
    },
};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn version(value: &str) -> VersionTag {
    VersionTag::new(value).unwrap()
}

fn service_config(pump_interval: Duration) -> RunServiceConfig {
    let mut config = RunServiceConfig::production(16, 4, 2, 64)
        .with_run_timeout(Duration::from_secs(60))
        .with_artifact_gc(
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(60),
            30,
        );
    config.pump_interval = pump_interval;
    config
}

async fn shared_artifact_store(
    root: std::path::PathBuf,
    namespace: &str,
) -> Arc<LocalContentAddressedArtifactStore> {
    Arc::new(
        LocalContentAddressedArtifactStore::open_shared(root, 64 * 1024, namespace)
            .await
            .unwrap(),
    )
}

async fn postgres_repository(
    database_url: &str,
    prefix: &str,
) -> (sqlx::PgPool, String, Arc<PostgresDurableRepository>) {
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(database_url)
        .await
        .unwrap();
    let schema = format!("{prefix}_{}", Uuid::new_v4().simple());
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository = Arc::new(
        PostgresDurableRepository::connect(&scoped_url)
            .await
            .unwrap(),
    );
    repository.initialize_schema().await.unwrap();
    (admin, schema, repository)
}

async fn drop_postgres_schema(admin: sqlx::PgPool, schema: String) {
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

async fn wait_for_completed(service: &RunService, run_id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let record = service.get_run(run_id).await.unwrap();
        match record.lifecycle {
            RunLifecycle::Completed { output } => return output.data,
            RunLifecycle::Failed { error } => panic!("Run failed: {error:?}"),
            RunLifecycle::Cancelled { error } | RunLifecycle::Interrupted { error } => {
                panic!("Run stopped: {error:?}")
            }
            RunLifecycle::Created | RunLifecycle::Running => {}
        }
        assert!(Instant::now() < deadline, "Run did not complete");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[derive(Debug, Clone)]
struct FixedModel {
    response: &'static str,
}

#[async_trait]
impl ChatModel for FixedModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
        if parameters.is_object() {
            Ok(())
        } else {
            Err(CompileError::new(
                "MODEL_PARAMETERS_INVALID",
                "fixture parameters must be an object",
            ))
        }
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::iter([Ok(ChatChunk {
            text: self.response.to_owned(),
            finish_reason: Some("stop".to_owned()),
            usage: None,
        })])))
    }
}

fn model_registry(
    worker_version: &str,
    provider_model: &str,
    response: &'static str,
) -> ModelRegistry {
    let mut models = ModelRegistry::default();
    models
        .register_versioned(
            "answer_model",
            ModelDeploymentIdentity::new(
                worker_version,
                json!({
                    "adapter": "fixture_chat",
                    "provider_model": provider_model,
                    "protocol": "fixture-v1",
                }),
            )
            .unwrap(),
            FixedModel { response },
        )
        .unwrap();
    models
}

fn register_model_worker(
    workers: &mut WorkerExecutorRegistry,
    models: &ModelRegistry,
    worker_version: &str,
) {
    workers
        .register(
            SchedulerTaskKind::Llm,
            "core.llm",
            version("2"),
            version(worker_version),
            Arc::new(V3LlmTaskExecutor::new(models.clone())),
        )
        .unwrap();
}

#[tokio::test]
async fn postgres_restart_keeps_old_alias_model_and_worker_in_deployment_archive() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let temporary = tempfile::tempdir().unwrap();
    let agent_root = temporary.path().join("versioned_agent");
    std::fs::create_dir_all(&agent_root).unwrap();
    std::fs::write(
        agent_root.join("agent.yaml"),
        r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: versioned_agent
  name: Versioned agent
  description: Deployment archive fixture.
inputs:
  question:
    type: string
    default: default question after restart
  note:
    type: string
    optional: true
output: string
workflow:
  steps:
    - id: answer
      type: llm
      model: answer_model
      messages:
        - role: user
          content:
            - text: $question
      response: string
    - return: $answer
"#,
    )
    .unwrap();
    let published = Arc::new(compile_v3_agent_dir(&agent_root).unwrap());
    let old_models = model_registry("model-worker-1", "provider-model-v1", "old-model-answer");
    let new_models = model_registry("model-worker-2", "provider-model-v2", "new-model-answer");
    let actions = ActionRegistry::default();
    let old = Arc::new(
        DeployedV3Agent::publish(
            Arc::clone(&published),
            &ProductionLeafDeploymentResolver::new(&old_models, &actions),
            SubflowContractRegistry::new(),
        )
        .unwrap(),
    );
    let current = Arc::new(
        DeployedV3Agent::publish(
            published,
            &ProductionLeafDeploymentResolver::new(&new_models, &actions),
            SubflowContractRegistry::new(),
        )
        .unwrap(),
    );
    assert_ne!(
        old.deployment_revision_id(),
        current.deployment_revision_id()
    );

    let (admin, schema, repository) =
        postgres_repository(&database_url, "v3_deployment_archive").await;
    let artifact_store =
        shared_artifact_store(temporary.path().join("artifacts"), "v3-deployment-archive").await;
    let mut old_workers = WorkerExecutorRegistry::new();
    register_model_worker(&mut old_workers, &old_models, "model-worker-1");
    let first = RunService::start_with_artifact_store(
        DeployedAgentCatalog::new(vec![Arc::clone(&old)]).unwrap(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        old_workers,
        artifact_store.clone(),
        service_config(Duration::from_secs(3_600)),
    )
    .await
    .unwrap();
    let old_run = first
        .create_detached(
            "versioned_agent",
            json!({"question": "which model?"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(old_run.status(), RunStatus::Running);
    assert_eq!(old_run.agent_version, old.deployment_revision_id().as_str());
    first.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(first);

    let agents = DeployedAgentCatalog::new(vec![Arc::clone(&current)]).unwrap();
    assert_eq!(
        agents
            .get("versioned_agent")
            .unwrap()
            .deployment_revision_id(),
        current.deployment_revision_id()
    );
    assert!(agents
        .get_deployment(old.deployment_revision_id().as_str())
        .is_none());
    assert_eq!(agents.archived_deployments().count(), 1);
    let mut all_workers = WorkerExecutorRegistry::new();
    register_model_worker(&mut all_workers, &old_models, "model-worker-1");
    register_model_worker(&mut all_workers, &new_models, "model-worker-2");
    let second = RunService::start_with_artifact_store(
        agents,
        repository.clone() as Arc<dyn ProductionRunRepository>,
        all_workers,
        artifact_store,
        service_config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    assert!(second
        .agents()
        .get_deployment(old.deployment_revision_id().as_str())
        .is_some());
    assert_eq!(second.agents().archived_deployments().count(), 2);
    let new_run = second
        .create_detached(
            "versioned_agent",
            json!({"question": "which model now?"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        new_run.agent_version,
        current.deployment_revision_id().as_str()
    );
    assert_eq!(
        wait_for_completed(&second, &old_run.run_id).await,
        json!("old-model-answer")
    );
    assert_eq!(
        wait_for_completed(&second, &new_run.run_id).await,
        json!("new-model-answer")
    );
    let restored_old_run = second
        .create_detached_from_deployment(
            "versioned_agent",
            old.deployment_revision_id().as_str(),
            json!({}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        wait_for_completed(&second, &restored_old_run.run_id).await,
        json!("old-model-answer")
    );
    let old_record = second.get_run(&old_run.run_id).await.unwrap();
    assert_eq!(
        old_record.agent_version,
        old.deployment_revision_id().as_str()
    );
    let execution_graph = second.execution_graph(&old_run.run_id).await.unwrap();
    assert!(!execution_graph.nodes().is_empty());
    let trace = second.trace_overlay(&old_run.run_id).await.unwrap();
    assert_eq!(trace.graph_document_id(), execution_graph.document_id());
    assert!(!trace.activations().is_empty());

    second.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(second);
    drop(repository);
    drop_postgres_schema(admin, schema).await;
}

#[derive(Debug)]
struct BoundValueExecutor {
    kind: SchedulerTaskKind,
    result_prefix: &'static str,
}

#[async_trait]
impl LeafTaskExecutor for BoundValueExecutor {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        assert_eq!(request.task_kind(), self.kind);
        let output = request.outputs().first().unwrap();
        let suffix = request
            .inputs()
            .first()
            .and_then(|input| input.value().value().as_str())
            .unwrap_or("none");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!(format!("{}:{suffix}", self.result_prefix))).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
    }
}

fn external_policy(effect_class: WorkerEffectClass) -> WorkerEffectPolicy {
    WorkerEffectPolicy::frozen(
        effect_class,
        EffectIdempotency::Idempotent,
        1,
        0,
        0,
        60_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap()
}

#[tokio::test]
async fn postgres_production_resolver_publishes_validated_http_and_tool_workers() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let temporary = tempfile::tempdir().unwrap();
    let agent_root = temporary.path().join("external_leaf_agent");
    std::fs::create_dir_all(&agent_root).unwrap();
    std::fs::write(
        agent_root.join("agent.yaml"),
        r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: external_leaf_agent
  name: External leaf agent
  description: Versioned HTTP and Tool adapter fixture.
inputs:
  url: string
output: string
workflow:
  steps:
    - id: fetch
      type: http
      method: GET
      url: $url
      response: string
    - id: transform
      type: tool
      tool: fixture.upper
      arguments:
        value: $fetch
      response: string
    - return: $transform
"#,
    )
    .unwrap();
    let published = Arc::new(compile_v3_agent_dir(&agent_root).unwrap());
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let without_adapters = DeployedV3Agent::publish(
        Arc::clone(&published),
        &ProductionLeafDeploymentResolver::new(&models, &actions),
        SubflowContractRegistry::new(),
    )
    .unwrap_err();
    assert_eq!(without_adapters.code(), "LEAF_DEPLOYMENT_NOT_REGISTERED");

    let http_validations = Arc::new(AtomicUsize::new(0));
    let tool_validations = Arc::new(AtomicUsize::new(0));
    let mut adapters = VersionedLeafAdapterRegistry::new();
    let validations = Arc::clone(&http_validations);
    adapters
        .register(
            VersionedLeafAdapterRegistration::new(
                VersionedLeafAdapterIdentity::new(
                    LeafTaskKind::Http,
                    "core.http",
                    version("1"),
                    version("http-adapter-1"),
                    version("http-worker-1"),
                )
                .unwrap(),
                external_policy(WorkerEffectClass::ReadOnly),
                json!({"transport_policy": "fixture-restricted-http-v1"}),
                Arc::new(
                    move |descriptor: &insight_agent_platform::engine::plan::LeafTaskDescriptor| {
                        validations.fetch_add(1, Ordering::SeqCst);
                        match descriptor.public_configuration.get("method") {
                            Some(DescriptorValue::String(method)) if method == "GET" => Ok(()),
                            _ => Err(CompileError::new(
                                "HTTP_DESCRIPTOR_UNSUPPORTED",
                                "fixture HTTP adapter supports only GET",
                            )),
                        }
                    },
                ),
                Arc::new(BoundValueExecutor {
                    kind: SchedulerTaskKind::Http,
                    result_prefix: "http",
                }),
            )
            .unwrap(),
        )
        .unwrap();
    let validations = Arc::clone(&tool_validations);
    adapters
        .register(
            VersionedLeafAdapterRegistration::new(
                VersionedLeafAdapterIdentity::new(
                    LeafTaskKind::Tool,
                    "core.tool",
                    version("1"),
                    version("tool-adapter-1"),
                    version("tool-worker-1"),
                )
                .unwrap(),
                external_policy(WorkerEffectClass::Pure),
                json!({"tool_protocol": "fixture-tool-v1"}),
                Arc::new(
                    move |descriptor: &insight_agent_platform::engine::plan::LeafTaskDescriptor| {
                        validations.fetch_add(1, Ordering::SeqCst);
                        match descriptor.public_configuration.get("tool") {
                            Some(DescriptorValue::String(tool)) if tool == "fixture.upper" => {
                                Ok(())
                            }
                            _ => Err(CompileError::new(
                                "TOOL_DESCRIPTOR_UNSUPPORTED",
                                "fixture Tool adapter supports only fixture.upper",
                            )),
                        }
                    },
                ),
                Arc::new(BoundValueExecutor {
                    kind: SchedulerTaskKind::Tool,
                    result_prefix: "tool",
                }),
            )
            .unwrap(),
        )
        .unwrap();

    let deployed = Arc::new(
        DeployedV3Agent::publish(
            published,
            &ProductionLeafDeploymentResolver::new(&models, &actions)
                .with_external_leaf_adapters(&adapters),
            SubflowContractRegistry::new(),
        )
        .unwrap(),
    );
    assert_eq!(http_validations.load(Ordering::SeqCst), 1);
    assert_eq!(tool_validations.load(Ordering::SeqCst), 1);

    let rejected_root = temporary.path().join("rejected_external_leaf_agent");
    std::fs::create_dir_all(&rejected_root).unwrap();
    let rejected_source = std::fs::read_to_string(agent_root.join("agent.yaml"))
        .unwrap()
        .replace(
            "id: external_leaf_agent",
            "id: rejected_external_leaf_agent",
        )
        .replace("method: GET", "method: POST");
    std::fs::write(rejected_root.join("agent.yaml"), rejected_source).unwrap();
    let rejected = DeployedV3Agent::publish(
        Arc::new(compile_v3_agent_dir(&rejected_root).unwrap()),
        &ProductionLeafDeploymentResolver::new(&models, &actions)
            .with_external_leaf_adapters(&adapters),
        SubflowContractRegistry::new(),
    )
    .unwrap_err();
    assert_eq!(rejected.code(), "HTTP_DESCRIPTOR_UNSUPPORTED");
    assert_eq!(http_validations.load(Ordering::SeqCst), 2);
    assert_eq!(tool_validations.load(Ordering::SeqCst), 1);

    let workers =
        production_worker_registry_with_leaf_adapters(&models, &actions, &adapters).unwrap();
    let (admin, schema, repository) = postgres_repository(&database_url, "v3_external_leaf").await;
    let artifact_store =
        shared_artifact_store(temporary.path().join("artifacts"), "v3-external-leaf").await;
    let service = RunService::start_with_artifact_store(
        DeployedAgentCatalog::new(vec![deployed]).unwrap(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers,
        artifact_store,
        service_config(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    let run = service
        .create_detached(
            "external_leaf_agent",
            json!({"url": "https://example.test/report"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        wait_for_completed(&service, &run.run_id).await,
        json!("tool:http:https://example.test/report")
    );

    service.shutdown(Duration::from_secs(2)).await.unwrap();
    drop(service);
    drop(repository);
    drop_postgres_schema(admin, schema).await;
}
