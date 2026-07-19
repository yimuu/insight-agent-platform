use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    catalog_v3::{compile_v3_agent_dir, DeployedV3Agent, ProductionLeafDeploymentResolver},
    dsl::CompileError,
    engine::{
        repository::SqliteDurableRepository, SchedulerTaskKind, SubflowContractRegistry,
        V3LlmTaskExecutor, VersionTag, WorkerExecutorRegistry,
    },
    history::types::RunLifecycle,
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

#[derive(Debug, Clone)]
struct ChunkedModel;

#[async_trait]
impl ChatModel for ChunkedModel {
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
        Ok(Box::pin(stream::iter([
            Ok(ChatChunk {
                text: "durable ".to_owned(),
                finish_reason: None,
                usage: None,
            }),
            Ok(ChatChunk {
                text: "after ".to_owned(),
                finish_reason: None,
                usage: None,
            }),
            Ok(ChatChunk {
                text: "stream loss".to_owned(),
                finish_reason: Some("stop".to_owned()),
                usage: None,
            }),
        ])))
    }
}

fn models() -> ModelRegistry {
    let mut models = ModelRegistry::default();
    models
        .register_versioned(
            "answer_model",
            ModelDeploymentIdentity::new(
                "token-observer-worker-1",
                json!({"adapter": "chunked-fixture", "provider_model": "fixed"}),
            )
            .unwrap(),
            ChunkedModel,
        )
        .unwrap();
    models
}

fn workers(
    models: &ModelRegistry,
    observer: Option<
        tokio::sync::broadcast::Sender<insight_agent_platform::engine::LlmTokenObservation>,
    >,
) -> WorkerExecutorRegistry {
    let executor = V3LlmTaskExecutor::new(models.clone());
    let executor = match observer {
        Some(observer) => executor.with_token_observer(observer),
        None => executor,
    };
    let mut workers = WorkerExecutorRegistry::new();
    workers
        .register(
            SchedulerTaskKind::Llm,
            "core.llm",
            VersionTag::new("1").unwrap(),
            VersionTag::new("token-observer-worker-1").unwrap(),
            Arc::new(executor),
        )
        .unwrap();
    workers
}

fn config() -> RunServiceConfig {
    let mut config = RunServiceConfig::single_process_development(8, 2, 1, 16);
    config.pump_interval = Duration::from_millis(5);
    config.run_timeout = Duration::from_secs(30);
    config.artifact_orphan_retention = Duration::from_secs(60);
    config.artifact_reference_retention = Duration::from_secs(60);
    config.artifact_gc_interval = Duration::from_secs(60);
    config.artifact_deletion_claim_seconds = 30;
    config.public_event_nonterminal_retention = Duration::from_secs(60);
    config.public_event_prune_interval = Duration::from_secs(60);
    config
}

async fn completed_output(service: &RunService, run_id: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let record = service.get_run(run_id).await.unwrap();
        match record.lifecycle {
            RunLifecycle::Completed { output } => return output.data,
            RunLifecycle::Failed { error } => panic!("run failed: {error:?}"),
            RunLifecycle::Cancelled { error } | RunLifecycle::Interrupted { error } => {
                panic!("run stopped: {error:?}")
            }
            RunLifecycle::Created | RunLifecycle::Running => {}
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "run did not complete"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn losing_the_transient_token_stream_does_not_change_the_restart_durable_result() {
    let temporary = tempfile::tempdir().unwrap();
    let agent_root = temporary.path().join("token_stream_agent");
    std::fs::create_dir_all(&agent_root).unwrap();
    std::fs::write(
        agent_root.join("agent.yaml"),
        r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: token_stream_agent
  name: Token stream agent
  description: Token observations are not execution facts.
inputs:
  question:
    type: string
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

    let models = models();
    let actions = ActionRegistry::default();
    let published = Arc::new(compile_v3_agent_dir(&agent_root).unwrap());
    let deployed = Arc::new(
        DeployedV3Agent::publish(
            published,
            &ProductionLeafDeploymentResolver::new(&models, &actions),
            SubflowContractRegistry::new(),
        )
        .unwrap(),
    );
    let database = temporary.path().join("durable.sqlite");
    let repository = Arc::new(
        SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap(),
    );
    let (observer, receiver) = tokio::sync::broadcast::channel(1);
    drop(receiver);
    let service = RunService::start(
        DeployedAgentCatalog::new(vec![Arc::clone(&deployed)]).unwrap(),
        repository.clone() as Arc<dyn ProductionRunRepository>,
        workers(&models, Some(observer)),
        config(),
    )
    .await
    .unwrap();
    let run = service
        .create_detached(
            "token_stream_agent",
            json!({"question": "does observation loss stop execution?"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        completed_output(&service, &run.run_id).await,
        json!("durable after stream loss")
    );
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    drop(service);
    drop(repository);

    // Reopen the store to prove the answer came from the durable Run result,
    // not from the lost process-local observation channel.
    let repository = Arc::new(
        SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap(),
    );
    let restarted = RunService::start(
        DeployedAgentCatalog::new(vec![deployed]).unwrap(),
        repository as Arc<dyn ProductionRunRepository>,
        workers(&models, None),
        config(),
    )
    .await
    .unwrap();
    assert_eq!(
        completed_output(&restarted, &run.run_id).await,
        json!("durable after stream loss")
    );
    restarted.shutdown(Duration::from_secs(1)).await.unwrap();
}
