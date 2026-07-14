use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    dsl::compiler::{AgentCompiler, CompileLimits},
    events::hub::{EventHub, EventHubConfig},
    history::{repository::RunRepository, sqlite::SqliteRunRepository, types::RunStatus},
    nodes::default_node_registries,
    resources::{
        actions::ActionRegistry,
        models::{ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry},
    },
    runtime::{CompiledAgentRegistry, RequestMetadata, RunError, RunService, RunServiceConfig},
};
use serde_json::{json, Value};
use tempfile::tempdir;

#[derive(Clone)]
struct ScenarioModel {
    chunks: Vec<ChatChunk>,
    max_accumulated_text_bytes: usize,
}

impl std::fmt::Debug for ScenarioModel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ScenarioModel").finish()
    }
}

#[async_trait]
impl ChatModel for ScenarioModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn validate_parameters(
        &self,
        _parameters: &Value,
    ) -> Result<(), insight_agent_platform::dsl::CompileError> {
        Ok(())
    }

    fn max_accumulated_text_bytes(&self) -> usize {
        self.max_accumulated_text_bytes
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::iter(
            self.chunks.clone().into_iter().map(Ok),
        )))
    }
}

fn write_agent(root: &Path, id: &str, model: &str) {
    let directory = root.join(id);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("agent.yaml"),
        format!(
            r#"version: 1
id: {id}
name: {id}
input:
  schema:
    type: object
    additionalProperties: false
entry: answer
nodes:
  answer:
    type: core.chat
    next: result
    config:
      model: {model}
      messages:
        - role: user
          content: Hi
  result:
    type: core.end
    config:
      outcome: success
      data: {{ok: true}}
"#
        ),
    )
    .unwrap();
}

struct Fixture {
    _root: tempfile::TempDir,
    service: RunService,
}

async fn fixture() -> Fixture {
    let root = tempdir().unwrap();
    write_agent(root.path(), "too-large", "too_large_model");
    write_agent(root.path(), "success", "success_model");

    let mut models = ModelRegistry::default();
    models
        .register(
            "too_large_model",
            ScenarioModel {
                chunks: vec![
                    ChatChunk {
                        text: "ok".to_string(),
                        finish_reason: None,
                        usage: None,
                    },
                    ChatChunk {
                        text: "capacity-release-secret".to_string(),
                        finish_reason: Some("stop".to_string()),
                        usage: None,
                    },
                ],
                max_accumulated_text_bytes: 2,
            },
        )
        .unwrap();
    models
        .register(
            "success_model",
            ScenarioModel {
                chunks: vec![ChatChunk {
                    text: "done".to_string(),
                    finish_reason: Some("stop".to_string()),
                    usage: None,
                }],
                max_accumulated_text_bytes: 16,
            },
        )
        .unwrap();

    let (node_types, executors) = default_node_registries().unwrap();
    let compiler = AgentCompiler::new(
        node_types,
        models,
        ActionRegistry::default(),
        Duration::from_secs(5),
        CompileLimits {
            max_fork_branches: 8,
        },
    );
    let agents = ["too-large", "success"]
        .into_iter()
        .map(|id| Arc::new(compiler.compile_dir(&root.path().join(id)).unwrap()))
        .collect();
    let agents = CompiledAgentRegistry::new(agents).unwrap();

    let database_path = root.path().join("history.sqlite3");
    let repository = Arc::new(
        SqliteRunRepository::connect_path(&database_path)
            .await
            .unwrap(),
    );
    let repository_trait: Arc<dyn RunRepository> = repository;
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let service = RunService::new(
        agents,
        executors,
        repository_trait,
        events,
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 1,
            max_parallel_branches_per_run: 1,
            run_timeout: Duration::from_secs(5),
        },
    )
    .unwrap();
    Fixture {
        _root: root,
        service,
    }
}

async fn wait_for_status(service: &RunService, run_id: &str, expected: RunStatus) {
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let record = service.get_run(run_id).await.unwrap();
            if record.status == expected {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    result.unwrap();
}

#[tokio::test]
async fn bounded_chat_failure_releases_capacity_for_later_runs() {
    let fixture = fixture().await;
    let service = &fixture.service;

    let failed = service
        .create_detached("too-large", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(service, &failed.run_id, RunStatus::Failed).await;
    let failed_record = service.get_run(&failed.run_id).await.unwrap();
    assert_eq!(
        failed_record.error_code.as_deref(),
        Some("MODEL_RESPONSE_TOO_LARGE")
    );
    assert_eq!(
        failed_record.error_message.as_deref(),
        Some("chat provider response exceeded the configured size limit")
    );
    assert!(
        !format!("{failed_record:?}").contains("capacity-release-secret"),
        "bounded failure leaked oversized content: {failed_record:?}"
    );

    let success = service
        .create_detached("success", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(service, &success.run_id, RunStatus::Completed).await;
    let success_record = service.get_run(&success.run_id).await.unwrap();
    assert_eq!(success_record.status, RunStatus::Completed);

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}
