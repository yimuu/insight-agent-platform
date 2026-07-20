use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use futures::{stream, StreamExt};
use insight_agent_platform::{
    api::formal::{build_router, ApiAuth, FormalApiState},
    catalog_v3::{
        compile_v3_agent_dir, DeployedV3Agent, LeafDeploymentResolver,
        OwnedProductionLeafDeploymentResolver, ResolvedLeafDeployment,
    },
    dsl::CompileError,
    engine::{
        plan::LeafTaskDescriptor, production_worker_registry_with_live_response, EffectIdempotency,
        LeafTaskKind, LocalContentAddressedArtifactStore, SubflowContractRegistry,
        WorkerCancellation, WorkerEffectClass, WorkerEffectPolicy,
    },
    resources::{
        actions::{
            Action, ActionContext, ActionDescriptor, ActionRegistry, CancellationClass,
            EffectClass, IdempotencyClass, ToolPublicArguments, ToolPublicPolicy,
        },
        models::{
            ChatChunk, ChatEvent, ChatEventStream, ChatFinishReason, ChatInputTokensDetails,
            ChatModel, ChatOutputTokensDetails, ChatRequest, ChatResponse, ChatStream,
            ChatToolCallDelta, ChatUsage, ModelCapability, ModelDeploymentIdentity, ModelRegistry,
            ModelRequestCapability,
        },
    },
    runtime::{
        DeployedAgentCatalog, InMemoryLiveResponseBroker, LiveResponseBroker,
        ProductionRunRepository, ResponseStreamEvent, RunError, RunService, RunServiceConfig,
    },
};
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};
use tower::ServiceExt;

const AGENT_ID: &str = "public_llm_retry";
const FIRST_ATTEMPT_TEXT: &str = "discarded-first-attempt";
const FINAL_TEXT: &str = "authoritative-second-attempt";
const CALL_ID: &str = "call_retry_probe";
const TOOL_ARGUMENT_FRAGMENT: &str = r#"{"probe":"part"#;

#[derive(Debug, Clone, Default)]
struct RetryModelState {
    calls: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
struct RetryOnceStreamingModel {
    state: RetryModelState,
}

impl RetryOnceStreamingModel {
    fn usage() -> ChatUsage {
        ChatUsage {
            input_tokens: Some(5),
            input_tokens_details: Some(ChatInputTokensDetails {
                cached_tokens: Some(1),
            }),
            output_tokens: Some(3),
            output_tokens_details: Some(ChatOutputTokensDetails {
                reasoning_tokens: Some(0),
            }),
            total_tokens: Some(8),
        }
    }
}

#[async_trait]
impl ChatModel for RetryOnceStreamingModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
        BTreeSet::from([ModelRequestCapability::Streaming])
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

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse, RunError> {
        Ok(ChatResponse {
            text: FINAL_TEXT.to_owned(),
            tool_calls: Vec::new(),
            finish_reason: ChatFinishReason::Stop,
            usage: Some(Self::usage()),
        })
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::empty::<Result<ChatChunk, RunError>>()))
    }

    async fn stream_chat_events(&self, _request: ChatRequest) -> Result<ChatEventStream, RunError> {
        let attempt = self.state.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let events = if attempt == 1 {
            vec![
                Ok(ChatEvent {
                    text_delta: FIRST_ATTEMPT_TEXT.to_owned(),
                    ..ChatEvent::default()
                }),
                Ok(ChatEvent {
                    tool_call_deltas: vec![ChatToolCallDelta {
                        index: 0,
                        id: Some(CALL_ID.to_owned()),
                        name: Some("retry_probe".to_owned()),
                        arguments_delta: TOOL_ARGUMENT_FRAGMENT.to_owned(),
                    }],
                    ..ChatEvent::default()
                }),
                Err(RunError::infrastructure(
                    "FIXTURE_PROVIDER_INTERRUPTED",
                    "fixture provider interrupted after provisional output",
                )),
            ]
        } else {
            vec![
                Ok(ChatEvent {
                    text_delta: FINAL_TEXT.to_owned(),
                    ..ChatEvent::default()
                }),
                Ok(ChatEvent {
                    finish_reason: Some(ChatFinishReason::Stop),
                    usage: Some(Self::usage()),
                    ..ChatEvent::default()
                }),
            ]
        };
        Ok(Box::pin(stream::iter(events).then(|event| async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            event
        })))
    }
}

#[derive(Clone)]
struct NeverCalledRetryProbe;

#[async_trait]
impl Action for NeverCalledRetryProbe {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: "retry_probe",
            version: "1.0.0",
            input_schema: json!({
                "type": "object",
                "properties": {"probe": {"type": "string"}},
                "required": ["probe"],
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "properties": {"ok": {"type": "boolean"}},
                "required": ["ok"],
                "additionalProperties": false
            }),
            effect: EffectClass::ReadOnly,
            idempotency: IdempotencyClass::Idempotent,
            cancellation: CancellationClass::Cooperative,
            required_capabilities: BTreeSet::new(),
        }
    }

    fn public_policy(&self) -> ToolPublicPolicy {
        ToolPublicPolicy {
            call: true,
            arguments: ToolPublicArguments::All,
            result_schema: None,
        }
    }

    async fn call(&self, _input: Value, _context: ActionContext) -> Result<Value, RunError> {
        Err(RunError::infrastructure(
            "FIXTURE_TOOL_MUST_NOT_RUN",
            "an incomplete provider tool call must never execute",
        ))
    }
}

#[derive(Clone)]
struct RetryPolicyResolver {
    inner: OwnedProductionLeafDeploymentResolver,
}

// Structured-author syntax cannot yet attach RetryPolicy to an LLM node. This
// fixture therefore freezes the policy at the real publication resolver
// boundary; every retry decision and all item allocation still go through the
// production scheduler and repository paths.
impl LeafDeploymentResolver for RetryPolicyResolver {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        let resolved = self.inner.resolve_leaf(kind, descriptor)?;
        if kind != LeafTaskKind::Llm {
            return Ok(resolved);
        }
        let retry_policy = WorkerEffectPolicy::frozen(
            WorkerEffectClass::Pure,
            EffectIdempotency::Idempotent,
            2,
            0,
            0,
            60_000,
            WorkerCancellation::Cooperative,
        )
        .map_err(|_| {
            CompileError::new(
                "WORKER_EFFECT_POLICY_INVALID",
                "fixture retry policy must be valid",
            )
        })?;
        Ok(resolved.with_effect_policy(retry_policy))
    }
}

struct Fixture {
    app: Router,
    service: RunService,
    database: PathBuf,
    model: RetryModelState,
    _temporary: tempfile::TempDir,
}

impl Fixture {
    async fn start() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().to_path_buf();
        let agent_root = root.join(AGENT_ID);
        std::fs::create_dir_all(&agent_root).unwrap();
        std::fs::write(
            agent_root.join("agent.yaml"),
            format!(
                r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: {AGENT_ID}
  name: Public LLM retry
  description: Exercises one public retry after provisional model output.
inputs: {{}}
output: string
workflow:
  steps:
    - id: answer
      type: llm
      model: retry_fixture
      stream: true
      publish: true
      messages:
        - role: user
          content:
            - text: retry once
      tools: [retry_probe]
      tool_choice: auto
      response: string
    - return: $answer
"#
            ),
        )
        .unwrap();

        let model = RetryModelState::default();
        let mut models = ModelRegistry::default();
        models
            .register_versioned(
                "retry_fixture",
                ModelDeploymentIdentity::new(
                    "public-retry-model-v1",
                    json!({"adapter": "retry-once", "provider_model": "fixture"}),
                )
                .unwrap(),
                RetryOnceStreamingModel {
                    state: model.clone(),
                },
            )
            .unwrap();
        let mut actions = ActionRegistry::default();
        actions.register(NeverCalledRetryProbe).unwrap();

        let resolver = RetryPolicyResolver {
            inner: OwnedProductionLeafDeploymentResolver::new(&models, &actions)
                .with_llm_tool_continuation_capability(),
        };
        let published = Arc::new(compile_v3_agent_dir(&agent_root).unwrap());
        let deployed = Arc::new(
            DeployedV3Agent::publish(published, &resolver, SubflowContractRegistry::new()).unwrap(),
        );
        let broker = Arc::new(InMemoryLiveResponseBroker::new(64, 16).unwrap());
        let workers = production_worker_registry_with_live_response(
            &models,
            &actions,
            Arc::clone(&broker) as Arc<dyn LiveResponseBroker>,
        )
        .unwrap();
        let database = root.join("durable.sqlite");
        let repository = Arc::new(
            insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
                &database,
            )
            .await
            .unwrap(),
        );
        let artifact_store = Arc::new(
            LocalContentAddressedArtifactStore::open(root.join("artifacts"), 1024)
                .await
                .unwrap(),
        );
        let mut config = RunServiceConfig::single_process_development(8, 4, 2, 32);
        config.pump_interval = Duration::from_millis(2);
        config.run_timeout = Duration::from_secs(10);
        config.terminal_barrier_timeout = Duration::from_millis(250);
        config.outbound_write_timeout = Duration::from_secs(2);
        config.artifact_gc_interval = Duration::from_secs(60);
        config.public_event_prune_interval = Duration::from_secs(60);
        let service = RunService::start_with_artifact_store_graph_publication_and_live_response(
            DeployedAgentCatalog::new(vec![deployed]).unwrap(),
            repository as Arc<dyn ProductionRunRepository>,
            workers,
            broker as Arc<dyn LiveResponseBroker>,
            artifact_store,
            Arc::new(resolver),
            config,
        )
        .await
        .unwrap();
        let app = build_router(FormalApiState {
            service: service.clone(),
            auth: ApiAuth::disabled(),
            sse_keep_alive_interval: Duration::from_secs(30),
            readiness_probe_timeout: Duration::from_secs(1),
        });
        Self {
            app,
            service,
            database,
            model,
            _temporary: temporary,
        }
    }
}

fn decode_sse(raw: &str) -> Vec<Value> {
    raw.split_terminator("\n\n")
        .filter_map(|frame| {
            let event_name = frame
                .lines()
                .find_map(|line| line.strip_prefix("event: "))?;
            let data = frame
                .lines()
                .find_map(|line| line.strip_prefix("data: "))
                .expect("every named SSE frame has data");
            let value: Value = serde_json::from_str(data).unwrap();
            assert_eq!(value["type"], event_name);
            serde_json::from_value::<ResponseStreamEvent>(value.clone()).unwrap();
            Some(value)
        })
        .collect()
}

async fn terminal_manifest(database: &PathBuf, run_id: &str) -> Value {
    let pool = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let row = sqlx::query(
        "SELECT public_item_manifest,response_payload,workflow_payload \
         FROM response_snapshots WHERE run_id=?",
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let manifest = serde_json::from_str(&row.get::<String, _>("public_item_manifest")).unwrap();
    let response = row.get::<String, _>("response_payload");
    let workflow = row.get::<String, _>("workflow_payload");
    assert!(!response.contains(FIRST_ATTEMPT_TEXT));
    assert!(!workflow.contains(FIRST_ATTEMPT_TEXT));
    let attempts = sqlx::query(
        "SELECT attempt_no,lifecycle,failure_code FROM node_attempts \
         WHERE run_id=? ORDER BY attempt_no",
    )
    .bind(run_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].get::<i64, _>("attempt_no"), 1);
    assert_eq!(attempts[0].get::<String, _>("lifecycle"), "failed");
    assert_eq!(
        attempts[0]
            .get::<Option<String>, _>("failure_code")
            .as_deref(),
        Some("V3_LLM_PROVIDER_FAILED")
    );
    assert_eq!(attempts[1].get::<i64, _>("attempt_no"), 2);
    assert_eq!(attempts[1].get::<String, _>("lifecycle"), "succeeded");
    assert_eq!(attempts[1].get::<Option<String>, _>("failure_code"), None);
    pool.close().await;
    manifest
}

#[tokio::test]
async fn attached_public_llm_retry_closes_old_items_and_appends_a_new_terminal_item() {
    let fixture = Fixture::start().await;
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post(format!("/v1/agents/{AGENT_ID}/runs/stream"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
    let run_id = response.headers()["x-run-id"].to_str().unwrap().to_owned();
    let bytes = tokio::time::timeout(
        Duration::from_secs(15),
        to_bytes(response.into_body(), 1 << 20),
    )
    .await
    .expect("retrying attached Run must reach terminal EOF")
    .unwrap();
    let raw = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(raw.ends_with("\n\n"));
    let events = decode_sse(&raw);
    assert!(!events.is_empty());
    for (sequence, event) in events.iter().enumerate() {
        assert_eq!(event["sequence_number"], sequence as u64);
    }
    assert_eq!(events[0]["type"], "response.created");
    assert_eq!(events[1]["type"], "response.in_progress");
    let terminal = events.last().unwrap();
    assert_eq!(terminal["type"], "response.completed");
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(
                    event["type"].as_str(),
                    Some("response.completed")
                        | Some("response.failed")
                        | Some("workflow.response.timed_out")
                        | Some("workflow.response.cancelled")
                        | Some("workflow.response.interrupted")
                )
            })
            .count(),
        1
    );
    assert_eq!(fixture.model.calls.load(Ordering::SeqCst), 2);

    let added = events
        .iter()
        .filter(|event| event["type"] == "response.output_item.added")
        .collect::<Vec<_>>();
    assert_eq!(
        added.len(),
        3,
        "two old items and one retry item are expected"
    );
    assert_eq!(added[0]["item"]["type"], "message");
    assert_eq!(added[1]["item"]["type"], "function_call");
    assert_eq!(added[2]["item"]["type"], "message");
    assert_eq!(added[0]["output_index"], 0);
    assert_eq!(added[1]["output_index"], 1);
    assert_eq!(added[2]["output_index"], 2);
    let first_message_id = added[0]["item"]["id"].as_str().unwrap();
    let failed_function_id = added[1]["item"]["id"].as_str().unwrap();
    let retry_message_id = added[2]["item"]["id"].as_str().unwrap();
    assert_ne!(retry_message_id, first_message_id);
    assert_ne!(retry_message_id, failed_function_id);

    let first_text_delta_index = events
        .iter()
        .position(|event| {
            event["type"] == "response.output_text.delta"
                && event["item_id"] == first_message_id
                && event["delta"] == FIRST_ATTEMPT_TEXT
        })
        .expect("the first Attempt body must be visible before failure");
    let provisional_arguments_index = events
        .iter()
        .position(|event| {
            event["type"] == "response.function_call_arguments.delta"
                && event["item_id"] == failed_function_id
                && event["delta"] == TOOL_ARGUMENT_FRAGMENT
        })
        .expect("the first Attempt function fragment must be visible before failure");

    let failed_function_done_index = events
        .iter()
        .position(|event| {
            event["type"] == "response.output_item.done"
                && event["item"]["id"] == failed_function_id
        })
        .expect("failed function item must close online");
    let failed_function_done = &events[failed_function_done_index];
    assert_eq!(failed_function_done["output_index"], 1);
    assert_eq!(failed_function_done["item"]["status"], "incomplete");
    assert_eq!(failed_function_done["item"]["arguments"], "");
    assert!(provisional_arguments_index < failed_function_done_index);
    assert!(events.iter().all(|event| {
        !(event["type"] == "response.function_call_arguments.done"
            && event["item_id"] == failed_function_id)
    }));
    let first_message_done_index = events
        .iter()
        .position(|event| {
            event["type"] == "response.output_item.done" && event["item"]["id"] == first_message_id
        })
        .expect("failed message item must close online");
    let first_message_done = &events[first_message_done_index];
    assert_eq!(first_message_done["item"]["status"], "incomplete");
    assert!(first_message_done["item"]["content"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(first_text_delta_index < first_message_done_index);

    let retry_added_index = events
        .iter()
        .position(|event| {
            event["type"] == "response.output_item.added" && event["item"]["id"] == retry_message_id
        })
        .unwrap();
    assert!(failed_function_done_index < retry_added_index);
    assert!(first_message_done_index < retry_added_index);
    let retry_done = events
        .iter()
        .find(|event| {
            event["type"] == "response.output_item.done" && event["item"]["id"] == retry_message_id
        })
        .expect("the retry item must complete online");
    assert_eq!(retry_done["output_index"], 2);
    assert_eq!(retry_done["item"]["status"], "completed");
    assert_eq!(retry_done["item"]["content"][0]["text"], FINAL_TEXT);

    let output = terminal["response"]["output"].as_array().unwrap();
    assert_eq!(output.len(), 3);
    assert_eq!(output[0]["id"], first_message_id);
    assert_eq!(output[0]["status"], "incomplete");
    assert!(output[0]["content"].as_array().unwrap().is_empty());
    assert_eq!(output[1]["id"], failed_function_id);
    assert_eq!(output[1]["status"], "incomplete");
    assert_eq!(output[1]["arguments"], "");
    assert_eq!(output[2]["id"], retry_message_id);
    assert_eq!(output[2]["status"], "completed");
    assert_eq!(output[2]["content"][0]["text"], FINAL_TEXT);
    assert_eq!(terminal["workflow"]["result"], FINAL_TEXT);
    assert!(!serde_json::to_string(terminal)
        .unwrap()
        .contains(FIRST_ATTEMPT_TEXT));

    let manifest = terminal_manifest(&fixture.database, &run_id).await;
    let manifest = manifest.as_array().unwrap();
    assert_eq!(manifest.len(), 3);
    assert_eq!(manifest[0]["attempt_no"], 1);
    assert_eq!(manifest[0]["output_index"], 0);
    assert_eq!(manifest[0]["item_id"], first_message_id);
    assert_eq!(manifest[0]["kind"], "message");
    assert_eq!(manifest[0]["status"], "incomplete");
    assert_eq!(manifest[1]["attempt_no"], 1);
    assert_eq!(manifest[1]["output_index"], 1);
    assert_eq!(manifest[1]["item_id"], failed_function_id);
    assert_eq!(manifest[1]["kind"], "function_call");
    assert_eq!(manifest[1]["status"], "incomplete");
    assert_eq!(manifest[2]["attempt_no"], 2);
    assert_eq!(manifest[2]["output_index"], 2);
    assert_eq!(manifest[2]["item_id"], retry_message_id);
    assert_eq!(manifest[2]["kind"], "message");
    assert_eq!(manifest[2]["status"], "completed");

    fixture
        .service
        .shutdown(Duration::from_secs(2))
        .await
        .unwrap();
}
