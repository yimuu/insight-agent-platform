use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
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
    catalog::{
        compile_agent_dir, DeployedAgent, DeploymentPersistencePolicy, LeafDeploymentResolver,
        OwnedProductionLeafDeploymentResolver, ResolvedLeafDeployment,
    },
    dsl::CompileError,
    engine::{
        plan::LeafTaskDescriptor, production_worker_registry_with_live_run_stream, LeafTaskKind,
        LocalContentAddressedArtifactStore, SubflowContractRegistry, WorkerEffectPolicy,
    },
    resources::{
        actions::{
            Action, ActionContext, ActionDescriptor, ActionRegistry, CancellationClass,
            EffectClass, IdempotencyClass, ToolPublicArguments, ToolPublicPolicy,
        },
        models::{
            ChatChunk, ChatEvent, ChatEventStream, ChatFinishReason, ChatInputTokensDetails,
            ChatMessage, ChatModel, ChatOutputTokensDetails, ChatRequest, ChatResponse, ChatStream,
            ChatToolCall, ChatToolCallDelta, ChatToolChoice, ChatUsage, ModelCapability,
            ModelDeploymentIdentity, ModelRegistry, ModelRequestCapability, ModelSelector,
        },
    },
    runtime::{
        DeployedAgentCatalog, InMemoryLiveRunStreamBroker, LiveRunStreamBroker,
        ProductionRunRepository, RunError, RunService, RunServiceConfig, RunStreamEvent,
        TerminalOnlyRunConfig, TerminalOnlyStore,
    },
};
use insight_api::v1::{build_router, ApiAuth, ApiState};
use serde_json::{json, Value};
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};
use tower::ServiceExt;

const AGENT_ID: &str = "durable_tool_continuation";
const TERMINAL_AGENT_ID: &str = "terminal_tool_continuation";
const MODEL_WORKER_VERSION: &str = "durable-tool-continuation-model-v1";
const CALL_ID: &str = "call_lookup_1";
const TOOL_ARGUMENTS: &str = r#"{"query":"durable"}"#;
const FINAL_TEXT: &str = "final answer: 42";
const SERVER_ONLY_SECRET_SENTINEL: &str = "server-only-sentinel-never-persist";

#[derive(Debug, Clone, Default)]
struct ModelState {
    first_round_calls: Arc<AtomicUsize>,
    first_round_finished: Arc<AtomicBool>,
    continuation_calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<ChatRequest>>>,
}

#[derive(Debug, Clone)]
struct DeterministicContinuationModel {
    state: ModelState,
}

impl DeterministicContinuationModel {
    fn first_usage() -> ChatUsage {
        ChatUsage {
            input_tokens: Some(10),
            input_tokens_details: Some(ChatInputTokensDetails {
                cached_tokens: Some(1),
            }),
            output_tokens: Some(2),
            output_tokens_details: Some(ChatOutputTokensDetails {
                reasoning_tokens: Some(0),
            }),
            total_tokens: Some(12),
        }
    }

    fn continuation_usage() -> ChatUsage {
        ChatUsage {
            input_tokens: Some(20),
            input_tokens_details: Some(ChatInputTokensDetails {
                cached_tokens: Some(2),
            }),
            output_tokens: Some(4),
            output_tokens_details: Some(ChatOutputTokensDetails {
                reasoning_tokens: Some(1),
            }),
            total_tokens: Some(24),
        }
    }
}

#[async_trait]
impl ChatModel for DeterministicContinuationModel {
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

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, RunError> {
        let is_continuation = request
            .messages
            .iter()
            .any(|message| matches!(message, ChatMessage::Tool { .. }));
        self.state.requests.lock().unwrap().push(request);
        if is_continuation {
            self.state.continuation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ChatResponse {
                text: FINAL_TEXT.to_owned(),
                tool_calls: Vec::new(),
                finish_reason: ChatFinishReason::Stop,
                usage: Some(Self::continuation_usage()),
            })
        } else {
            self.state.first_round_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ChatResponse {
                text: String::new(),
                tool_calls: vec![ChatToolCall {
                    index: 0,
                    id: CALL_ID.to_owned(),
                    name: "lookup".to_owned(),
                    arguments: TOOL_ARGUMENTS.to_owned(),
                }],
                finish_reason: ChatFinishReason::ToolCalls,
                usage: Some(Self::first_usage()),
            })
        }
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::empty::<Result<ChatChunk, RunError>>()))
    }

    async fn stream_chat_events(&self, request: ChatRequest) -> Result<ChatEventStream, RunError> {
        let is_continuation = request
            .messages
            .iter()
            .any(|message| matches!(message, ChatMessage::Tool { .. }));
        self.state.requests.lock().unwrap().push(request);
        let events = if is_continuation {
            self.state.continuation_calls.fetch_add(1, Ordering::SeqCst);
            vec![
                Ok(ChatEvent {
                    text_delta: "final answer: ".to_owned(),
                    ..ChatEvent::default()
                }),
                Ok(ChatEvent {
                    text_delta: "42".to_owned(),
                    ..ChatEvent::default()
                }),
                Ok(ChatEvent {
                    finish_reason: Some(ChatFinishReason::Stop),
                    usage: Some(Self::continuation_usage()),
                    ..ChatEvent::default()
                }),
            ]
        } else {
            self.state.first_round_calls.fetch_add(1, Ordering::SeqCst);
            vec![
                Ok(ChatEvent {
                    tool_call_deltas: vec![ChatToolCallDelta {
                        index: 0,
                        id: Some(CALL_ID.to_owned()),
                        name: Some("lookup".to_owned()),
                        arguments_delta: "{\"query\":\"".to_owned(),
                    }],
                    ..ChatEvent::default()
                }),
                Ok(ChatEvent {
                    tool_call_deltas: vec![ChatToolCallDelta {
                        index: 0,
                        id: None,
                        name: None,
                        arguments_delta: "dur".to_owned(),
                    }],
                    ..ChatEvent::default()
                }),
                Ok(ChatEvent {
                    tool_call_deltas: vec![ChatToolCallDelta {
                        index: 0,
                        id: None,
                        name: None,
                        arguments_delta: r#"able"}"#.to_owned(),
                    }],
                    ..ChatEvent::default()
                }),
                Ok(ChatEvent {
                    finish_reason: Some(ChatFinishReason::ToolCalls),
                    usage: Some(Self::first_usage()),
                    ..ChatEvent::default()
                }),
            ]
        };
        let state = self.state.clone();
        Ok(Box::pin(stream::iter(events).enumerate().then(
            move |(index, event)| {
                let state = state.clone();
                async move {
                    let delay = if !is_continuation && index == 1 {
                        Duration::from_millis(150)
                    } else {
                        Duration::from_millis(5)
                    };
                    tokio::time::sleep(delay).await;
                    if !is_continuation
                        && matches!(
                            &event,
                            Ok(ChatEvent {
                                finish_reason: Some(ChatFinishReason::ToolCalls),
                                ..
                            })
                        )
                    {
                        state.first_round_finished.store(true, Ordering::SeqCst);
                    }
                    event
                }
            },
        )))
    }
}

// Intentionally not `Debug`: this test record receives process-local injected
// values and models the production rule that protected invocation data must
// not become incidental diagnostics.
#[derive(Clone, PartialEq)]
struct ActionCall {
    input: Value,
    run_id: String,
    operation_id: String,
    attempt: u32,
    idempotency_key: String,
}

#[derive(Clone, Default)]
struct ActionState {
    calls: Arc<AtomicUsize>,
    records: Arc<Mutex<Vec<ActionCall>>>,
    retry_first: Arc<AtomicBool>,
}

#[derive(Clone)]
struct RecordingLookupAction {
    state: ActionState,
}

#[async_trait]
impl Action for RecordingLookupAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: "lookup".to_owned(),
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
            progress_schema: Some(json!({
                "type": "object",
                "properties": {
                    "stage": {"type": "string", "maxLength": 32},
                    "completed": {"type": "integer", "minimum": 0, "maximum": 2}
                },
                "required": ["stage", "completed"],
                "additionalProperties": false
            })),
            result_schema: Some(json!({
                "type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"],
                "additionalProperties": false
            })),
        }
    }

    fn inject_model_tool_input(
        &self,
        mut model_visible_input: Value,
        _context: &ActionContext,
    ) -> Result<Value, RunError> {
        model_visible_input
            .as_object_mut()
            .expect("frozen model-visible input is a closed object")
            .insert(
                "server_token".to_owned(),
                Value::String(SERVER_ONLY_SECRET_SENTINEL.to_owned()),
            );
        Ok(model_visible_input)
    }

    async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
        let attempt = self.state.calls.fetch_add(1, Ordering::SeqCst) + 1;
        assert_eq!(
            context
                .publish_progress(json!({"stage": "lookup", "completed": 1}))
                .await
                .unwrap(),
            insight_agent_platform::resources::actions::ActionProgressDisposition::Published
        );
        assert_eq!(
            context
                .publish_progress(json!({"stage": "lookup", "completed": 2}))
                .await
                .unwrap(),
            insight_agent_platform::resources::actions::ActionProgressDisposition::Published
        );
        self.state.records.lock().unwrap().push(ActionCall {
            input,
            run_id: context.run_id,
            operation_id: context.operation_id,
            attempt: context.attempt,
            idempotency_key: context.idempotency_key,
        });
        if attempt == 1 && self.state.retry_first.load(Ordering::SeqCst) {
            return Err(RunError::infrastructure(
                "FIXTURE_ACTION_RETRY",
                "the fixture retries the first Action attempt",
            ));
        }
        Ok(json!({"answer": "42"}))
    }
}

#[derive(Clone)]
struct RetryingToolResolver {
    inner: OwnedProductionLeafDeploymentResolver,
}

impl LeafDeploymentResolver for RetryingToolResolver {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        let resolved = self.inner.resolve_leaf(kind, descriptor)?;
        if kind != LeafTaskKind::Llm {
            return Ok(resolved);
        }
        let mut binding = resolved.binding_evidence().clone();
        for tool in binding
            .get_mut("tools")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| {
                CompileError::new(
                    "FIXTURE_TOOL_BINDING_INVALID",
                    "fixture LLM binding must contain tools",
                )
            })?
        {
            let policy = serde_json::from_value::<WorkerEffectPolicy>(
                tool.get("effect_policy").cloned().ok_or_else(|| {
                    CompileError::new(
                        "FIXTURE_TOOL_BINDING_INVALID",
                        "fixture tool binding must contain an effect policy",
                    )
                })?,
            )
            .map_err(|_| {
                CompileError::new(
                    "FIXTURE_TOOL_BINDING_INVALID",
                    "fixture tool effect policy must be valid",
                )
            })?;
            let retrying = WorkerEffectPolicy::frozen(
                policy.effect_class(),
                policy.effect_idempotency(),
                2,
                20,
                20,
                policy.timeout_ms(),
                policy.cancellation(),
            )
            .map_err(|_| {
                CompileError::new(
                    "FIXTURE_TOOL_BINDING_INVALID",
                    "fixture retry policy must be valid",
                )
            })?;
            tool.as_object_mut()
                .expect("tool binding is a closed object")
                .insert(
                    "effect_policy".to_owned(),
                    serde_json::to_value(retrying).expect("effect policy serializes"),
                );
        }
        ResolvedLeafDeployment::new(resolved.worker_version().clone(), binding)
            .map(|rebuilt| rebuilt.with_effect_policy(resolved.effect_policy().clone()))
    }
}

struct Fixture {
    app: Router,
    service: RunService,
    database: PathBuf,
    model: ModelState,
    action: ActionState,
    _temporary: tempfile::TempDir,
}

impl Fixture {
    async fn start() -> Self {
        Self::start_with_action_retry(true).await
    }

    async fn start_with_action_retry(retry_first: bool) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().to_path_buf();
        let agent_root = root.join(AGENT_ID);
        std::fs::create_dir_all(&agent_root).unwrap();
        std::fs::write(
            agent_root.join("agent.yaml"),
            format!(
                r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: {AGENT_ID}
  name: Durable tool continuation
  description: Exercises one complete model-tool-continuation chain.
inputs: {{}}
output: string
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
            - text: use the lookup tool
      tools: [lookup]
      tool_choice: lookup
      tool_limits:
        max_rounds: 2
        max_calls: 2
      response: string
    - return: $answer
"#
            ),
        )
        .unwrap();
        let terminal_agent_root = root.join(TERMINAL_AGENT_ID);
        std::fs::create_dir_all(&terminal_agent_root).unwrap();
        std::fs::write(
            terminal_agent_root.join("agent.yaml"),
            format!(
                r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: {TERMINAL_AGENT_ID}
  name: Terminal tool continuation
  description: Exercises terminal-only model-tool progress over real HTTP SSE.
execution:
  persistence_mode: terminal_only
inputs: {{}}
output: string
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
            - text: use the lookup tool
      tools: [lookup]
      tool_choice: lookup
      tool_limits:
        max_rounds: 2
        max_calls: 2
      response: string
    - return: $answer
"#
            ),
        )
        .unwrap();

        let model = ModelState::default();
        let mut models = ModelRegistry::default();
        models
            .register_versioned(
                ModelSelector::new("fixture", "fixture-model").unwrap(),
                ModelDeploymentIdentity::new(
                    MODEL_WORKER_VERSION,
                    json!({"adapter": "deterministic-continuation", "provider_model": "fixed"}),
                )
                .unwrap(),
                DeterministicContinuationModel {
                    state: model.clone(),
                },
            )
            .unwrap();
        let action = ActionState::default();
        action.retry_first.store(retry_first, Ordering::SeqCst);
        let mut actions = ActionRegistry::default();
        actions
            .register(RecordingLookupAction {
                state: action.clone(),
            })
            .unwrap();

        // The continuation capability is deliberately enabled only in this
        // fixture. Production publication remains fail-closed by default.
        let base_resolver = OwnedProductionLeafDeploymentResolver::new(&models, &actions)
            .with_llm_tool_continuation_capability()
            .with_operation_timeout(Duration::from_secs(2))
            .unwrap();
        let resolver = RetryingToolResolver {
            inner: base_resolver.clone(),
        };
        let published = Arc::new(compile_agent_dir(&agent_root).unwrap());
        let full_deployed = Arc::new(
            DeployedAgent::publish(published, &resolver, SubflowContractRegistry::new()).unwrap(),
        );
        let terminal_published = Arc::new(compile_agent_dir(&terminal_agent_root).unwrap());
        let terminal_deployed = Arc::new(
            DeployedAgent::publish_with_persistence_policy(
                terminal_published,
                &base_resolver,
                SubflowContractRegistry::new(),
                DeploymentPersistencePolicy::terminal_only(false, Duration::from_secs(10)).unwrap(),
            )
            .unwrap(),
        );

        let broker = Arc::new(InMemoryLiveRunStreamBroker::new(64, 16).unwrap());
        let workers = production_worker_registry_with_live_run_stream(
            &models,
            &actions,
            Arc::clone(&broker) as Arc<dyn LiveRunStreamBroker>,
        )
        .unwrap();
        let database = root.join("durable.sqlite");
        database::provision_sqlite_database(&database).await;
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
        let service = RunService::start_with_artifact_store_graph_publication_and_live_run_stream(
            DeployedAgentCatalog::new(vec![full_deployed, terminal_deployed]).unwrap(),
            repository.clone() as Arc<dyn ProductionRunRepository>,
            workers,
            broker as Arc<dyn LiveRunStreamBroker>,
            artifact_store,
            Arc::new(resolver),
            config,
        )
        .await
        .unwrap();
        service
            .enable_terminal_only(
                repository as Arc<dyn TerminalOnlyStore>,
                "http://terminal-tool.test".to_owned(),
                TerminalOnlyRunConfig {
                    owner_lease: Duration::from_secs(30),
                    owner_heartbeat: Duration::from_secs(5),
                    terminal_commit_retry: Duration::from_secs(1),
                    run_timeout: Duration::from_secs(10),
                    max_concurrent_runs: 4,
                    conversations_enabled: false,
                    inline_content_max_bytes: 4_096,
                    message_page_size_default: 20,
                    message_page_size_max: 100,
                    recent_context_messages: 8,
                    summary_trigger_messages: 100,
                    summary_trigger_tokens: 100_000,
                    run_retention: Duration::from_secs(24 * 60 * 60),
                    conversation_retention: Duration::from_secs(24 * 60 * 60),
                    terminal_barrier_timeout: Duration::from_secs(2),
                    outbound_write_timeout: Duration::from_secs(2),
                },
            )
            .await
            .unwrap();
        let app = build_router(ApiState {
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
            action,
            _temporary: temporary,
        }
    }
}

struct Transcript {
    run_id: String,
    raw_sse: String,
    events: Vec<Value>,
}

impl Transcript {
    fn terminal(&self) -> &Value {
        self.events.last().unwrap()
    }

    fn event_types(&self) -> Vec<&str> {
        self.events
            .iter()
            .map(|event| event["type"].as_str().unwrap())
            .collect()
    }
}

async fn attached_transcript(app: &Router, model: &ModelState, agent_id: &str) -> Transcript {
    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/agents/{agent_id}/runs/stream"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(
                    "x-request-id",
                    format!("request-tool-continuation-{agent_id}"),
                )
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
    assert!(!response.headers().contains_key("x-response-id"));
    let mut body = response.into_body().into_data_stream();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
            assert!(bytes.len() <= 1 << 20, "response exceeded test bound");
            let observed = String::from_utf8_lossy(&bytes);
            if observed.contains("event: run.output.function_call.arguments.delta") {
                assert!(
                    !model.first_round_finished.load(Ordering::SeqCst),
                    "the first function-argument delta must precede Provider completion"
                );
                assert!(
                    !observed.contains("event: run.lifecycle.completed"),
                    "the first function-argument delta must precede Run terminal"
                );
                return;
            }
        }
        panic!("response reached EOF before the first function-argument delta");
    })
    .await
    .expect("the first function-argument delta was not delivered promptly");
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
            assert!(bytes.len() <= 1 << 20, "response exceeded test bound");
        }
    })
    .await
    .expect("durable continuation did not reach terminal EOF");
    let raw = String::from_utf8(bytes).unwrap();
    assert!(raw.ends_with("\n\n"));
    assert!(!raw.contains("output_bytes"));
    assert!(!raw.contains(SERVER_ONLY_SECRET_SENTINEL));
    assert!(raw.lines().all(|line| !line.starts_with("id:")));

    let mut typed = Vec::new();
    let mut events = Vec::new();
    for frame in raw.split_terminator("\n\n") {
        let Some(event_name) = frame.lines().find_map(|line| line.strip_prefix("event: ")) else {
            continue;
        };
        let data = frame
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .expect("every named SSE event has data");
        let value: Value = serde_json::from_str(data).unwrap();
        assert_eq!(value["type"], event_name);
        assert_no_private_runtime_keys(&value);
        typed.push(serde_json::from_value::<RunStreamEvent>(value.clone()).unwrap());
        events.push(value);
    }
    assert!(!events.is_empty());
    for (sequence, event) in typed.iter().enumerate() {
        assert_eq!(event.sequence_number(), sequence as u64);
    }
    assert_eq!(events[0]["type"], "run.lifecycle.created");
    assert_eq!(events[1]["type"], "run.lifecycle.running");
    assert_eq!(events[0]["run"]["id"], run_id);
    assert_eq!(events[0]["run"]["status"], "created");
    assert!(typed.last().unwrap().is_run_terminal());
    assert_eq!(
        typed.iter().filter(|event| event.is_run_terminal()).count(),
        1
    );

    Transcript {
        run_id,
        raw_sse: raw,
        events,
    }
}

fn assert_no_private_runtime_keys(value: &Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                assert!(
                    !matches!(key.as_str(), "operation" | "output_bytes"),
                    "private runtime field leaked: {key}"
                );
                assert_no_private_runtime_keys(value);
            }
        }
        Value::Array(values) => values.iter().for_each(assert_no_private_runtime_keys),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

async fn get_run(app: &Router, run_id: &str) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/v1/runs/{run_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn assert_no_server_secret(label: &str, value: &str) {
    assert!(
        !value.contains(SERVER_ONLY_SECRET_SENTINEL),
        "server-only model-tool value leaked into {label}"
    );
}

fn assert_no_secret_in_public_surfaces(transcript: &Transcript, run: &Value) {
    assert_no_server_secret("raw SSE", &transcript.raw_sse);
    assert_no_server_secret(
        "decoded response events",
        &serde_json::to_string(&transcript.events).unwrap(),
    );
    assert_no_server_secret(
        "terminal response",
        &serde_json::to_string(transcript.terminal()).unwrap(),
    );
    assert_no_server_secret("GET Run", &serde_json::to_string(run).unwrap());
}

async fn assert_no_secret_in_durable_state(database: &PathBuf, run_id: &str) {
    let pool = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(database)
            .foreign_keys(true),
    )
    .await
    .unwrap();

    // Every replayable body which can carry model arguments, worker input,
    // public events, terminal state, or a diagnostic is scanned independently.
    // Failure messages name only the table/row and never echo a leaked value.
    let probes = [
        (
            "model_tool_calls",
            "SELECT arguments || coalesce(result_json,'') || \
                    coalesce(action_input_schema,'') || coalesce(action_output_schema,'') || \
                    coalesce(action_deployment_binding,'') || \
                    coalesce(effective_public_policy,'') || coalesce(failure_code,'') || \
                    coalesce(last_failure_code,'') \
             FROM model_tool_calls WHERE run_id=? ORDER BY model_call_no,call_index",
        ),
        (
            "model_tool_call_batches",
            "SELECT coalesce(assistant_content,'') \
             FROM model_tool_call_batches WHERE run_id=? ORDER BY model_call_no",
        ),
        (
            "task_outbox",
            "SELECT task_envelope FROM task_outbox WHERE run_id=? ORDER BY task_id",
        ),
        (
            "execution_events",
            "SELECT safe_payload FROM execution_events WHERE run_id=? ORDER BY seq",
        ),
        (
            "scheduler_checkpoints",
            "SELECT fact_payload FROM scheduler_checkpoints \
             WHERE run_id=? ORDER BY scheduler_projection_version,checkpoint_id",
        ),
        (
            "scheduler_values",
            "SELECT runtime_value || value_ref || declared_type FROM scheduler_values \
             WHERE run_id=? ORDER BY port_id",
        ),
        (
            "public_event_outbox",
            "SELECT safe_envelope FROM public_event_outbox \
             WHERE run_id=? ORDER BY public_ordinal",
        ),
        (
            "response_public_items",
            "SELECT coalesce(safe_item,'') FROM response_public_items \
             WHERE run_id=? ORDER BY output_index",
        ),
        (
            "run_stream_snapshots",
            "SELECT run_payload || public_item_manifest \
             FROM run_stream_snapshots WHERE run_id=?",
        ),
        (
            "model_call_usage",
            "SELECT coalesce(usage,'') FROM model_call_usage \
             WHERE run_id=? ORDER BY model_call_no",
        ),
        (
            "payloads",
            "SELECT coalesce(inline_value,'') FROM payloads \
             WHERE run_id=? ORDER BY payload_id",
        ),
        (
            "node_attempts",
            "SELECT coalesce(failure_code,'') FROM node_attempts \
             WHERE run_id=? ORDER BY activation_id,attempt_no",
        ),
        (
            "workflow_runs",
            "SELECT coalesce(error_code,'') FROM workflow_runs WHERE run_id=?",
        ),
    ];
    for (table, query) in probes {
        let values = sqlx::query_scalar::<_, String>(query)
            .bind(run_id)
            .fetch_all(&pool)
            .await
            .unwrap();
        for (index, value) in values.iter().enumerate() {
            assert_no_server_secret(&format!("{table} row {index}"), value);
        }
    }

    pool.close().await;
}

fn assert_provider_requests(state: &ModelState) {
    assert_eq!(state.first_round_calls.load(Ordering::SeqCst), 1);
    assert!(state.first_round_finished.load(Ordering::SeqCst));
    assert_eq!(state.continuation_calls.load(Ordering::SeqCst), 1);
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert_no_server_secret("Provider request Debug", &format!("{request:?}"));
        assert_eq!(request.parameters, json!({}));
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, "lookup");
        assert_eq!(
            request.tool_choice,
            ChatToolChoice::Named("lookup".to_owned())
        );
    }
    assert_eq!(requests[0].messages.len(), 1);
    assert!(matches!(requests[0].messages[0], ChatMessage::User { .. }));

    assert_eq!(requests[1].messages.len(), 3);
    assert!(matches!(requests[1].messages[0], ChatMessage::User { .. }));
    match &requests[1].messages[1] {
        ChatMessage::Assistant {
            content: None,
            tool_calls,
        } => assert_eq!(
            tool_calls,
            &[ChatToolCall {
                index: 0,
                id: CALL_ID.to_owned(),
                name: "lookup".to_owned(),
                arguments: TOOL_ARGUMENTS.to_owned(),
            }]
        ),
        message => panic!("expected exact assistant tool-call checkpoint, got {message:?}"),
    }
    match &requests[1].messages[2] {
        ChatMessage::Tool {
            tool_call_id,
            content,
        } => {
            assert_eq!(tool_call_id, CALL_ID);
            assert_eq!(content, r#"{"answer":"42"}"#);
            assert_eq!(
                serde_json::from_str::<Value>(content).unwrap(),
                json!({"answer": "42"})
            );
        }
        message => panic!("expected typed tool result, got {message:?}"),
    }
}

fn assert_public_events(transcript: &Transcript) {
    let types = transcript.event_types();
    let function_start = transcript
        .events
        .iter()
        .position(|event| {
            event["type"] == "run.output.item.added" && event["item"]["type"] == "function_call"
        })
        .expect("standard function-call item was not published");
    assert_eq!(
        &types[function_start..function_start + 6],
        &[
            "run.output.item.added",
            "run.output.function_call.arguments.delta",
            "run.output.function_call.arguments.delta",
            "run.output.function_call.arguments.delta",
            "run.output.function_call.arguments.done",
            "run.output.item.done",
        ]
    );
    let frames = &transcript.events[function_start..function_start + 6];
    let item_id = frames[0]["item"]["id"].as_str().unwrap();
    let output_index = frames[0]["output_index"].as_u64().unwrap();
    assert_eq!(frames[0]["item"]["status"], "in_progress");
    assert_eq!(frames[0]["item"]["call_id"], CALL_ID);
    assert_eq!(frames[0]["item"]["name"], "lookup");
    assert_eq!(frames[0]["item"]["arguments"], "");
    for frame in &frames[1..5] {
        assert_eq!(frame["item_id"], item_id);
        assert_eq!(frame["output_index"], output_index);
    }
    assert_eq!(frames[5]["output_index"], output_index);
    assert_eq!(frames[1]["delta"], "{\"query\":\"");
    assert_eq!(frames[2]["delta"], "dur");
    assert_eq!(frames[3]["delta"], r#"able"}"#);
    assert_eq!(frames[4]["name"], "lookup");
    assert_eq!(frames[4]["arguments"], TOOL_ARGUMENTS);
    assert_eq!(frames[5]["item"]["status"], "completed");
    assert_eq!(frames[5]["item"]["id"], item_id);
    assert_eq!(frames[5]["item"]["arguments"], TOOL_ARGUMENTS);

    let started = transcript
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| event["type"] == "run.tool.started")
        .collect::<Vec<_>>();
    let completed = transcript
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| event["type"] == "run.tool.completed")
        .collect::<Vec<_>>();
    let failed = transcript
        .events
        .iter()
        .filter(|event| event["type"] == "run.tool.failed")
        .collect::<Vec<_>>();
    let progress = transcript
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| event["type"] == "run.tool.progress")
        .collect::<Vec<_>>();
    assert_eq!(started.len(), 2);
    assert_eq!(progress.len(), 4);
    assert_eq!(completed.len(), 1);
    assert!(
        failed.is_empty(),
        "retryable attempts are not public terminals"
    );
    assert!(function_start + 5 < started[0].0);
    assert!(started[0].0 < progress[0].0);
    assert!(progress[0].0 < progress[1].0);
    assert!(progress[1].0 < started[1].0);
    assert!(started[1].0 < progress[2].0);
    assert!(progress[2].0 < progress[3].0);
    assert!(progress[3].0 < completed[0].0);
    for (index, (_, event)) in started.iter().enumerate() {
        assert_eq!(event["call_id"], CALL_ID, "started attempt {index}");
        assert_eq!(event["tool_name"], "lookup", "started attempt {index}");
        assert_eq!(
            event["arguments"],
            json!({"query": "durable"}),
            "started attempt {index}"
        );
    }
    for (index, (_, event)) in progress.iter().enumerate() {
        assert_eq!(
            event["content"],
            json!([{
                "type": "output_json",
                "json": {"stage": "lookup", "completed": index % 2 + 1}
            }])
        );
    }
    assert_eq!(completed[0].1["call_id"], CALL_ID);
    assert_eq!(completed[0].1["tool_name"], "lookup");
    assert!(
        completed[0].1["duration_ms"].as_u64().unwrap() >= 20,
        "logical duration must include retry backoff"
    );
    assert_eq!(
        completed[0].1["content"],
        json!([{"type": "output_json", "json": {"answer": "42"}}])
    );

    let terminal = transcript.terminal();
    assert_eq!(
        terminal["type"], "run.lifecycle.completed",
        "unexpected terminal envelope: {terminal}"
    );
    assert_eq!(terminal["run"]["id"], transcript.run_id);
    assert_eq!(terminal["run"]["status"], "completed");
    assert_eq!(terminal["run"]["result"], FINAL_TEXT);
    assert_eq!(terminal["run"]["usage_status"], "complete");
    assert_eq!(
        terminal["run"]["usage"],
        json!({
            "input_tokens": 30,
            "input_tokens_details": {"cached_tokens": 3},
            "output_tokens": 6,
            "output_tokens_details": {"reasoning_tokens": 1},
            "total_tokens": 36
        })
    );
    assert_eq!(
        terminal["run"]["tool_results"],
        json!([{
            "call_id": CALL_ID,
            "tool_name": "lookup",
            "content": [{"type": "output_json", "json": {"answer": "42"}}]
        }])
    );
    assert!(terminal["run"]["retrievals"].as_array().unwrap().is_empty());
    let output = terminal["run"]["output"].as_array().unwrap();
    assert_eq!(
        output.len(),
        2,
        "tool-only model round must not leave an empty message item: {output:?}",
    );
    assert_eq!(output[0]["type"], "function_call");
    assert_eq!(output[0]["status"], "completed");
    assert_eq!(output[0]["call_id"], CALL_ID);
    assert_eq!(output[0]["name"], "lookup");
    assert_eq!(output[0]["arguments"], TOOL_ARGUMENTS);
    assert_eq!(output[1]["type"], "message");
    assert_eq!(output[1]["status"], "completed");
    assert_eq!(output[1]["role"], "assistant");
    assert_eq!(
        output[1]["content"],
        json!([{
            "type": "output_text",
            "text": FINAL_TEXT,
            "annotations": []
        }])
    );
    assert!(output.iter().all(|item| item["status"] != "incomplete"));

    let message_additions = transcript
        .events
        .iter()
        .filter(|event| {
            event["type"] == "run.output.item.added" && event["item"]["type"] == "message"
        })
        .collect::<Vec<_>>();
    assert_eq!(message_additions.len(), 1);
    assert_eq!(message_additions[0]["output_index"], 1);
}

async fn assert_durable_authority(
    database: &PathBuf,
    transcript: &Transcript,
    action: &ActionState,
) {
    let pool = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(database)
            .foreign_keys(true),
    )
    .await
    .unwrap();

    let attempts = sqlx::query(
        "SELECT activation_id,attempt_no,lifecycle,effect_evidence,failure_code \
         FROM node_attempts WHERE run_id=? ORDER BY activation_id,attempt_no",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    let event_rows = sqlx::query(
        "SELECT seq,kind,safe_payload FROM execution_events WHERE run_id=? ORDER BY seq",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    let event_trace = event_rows
        .iter()
        .map(|row| {
            (
                row.get::<i64, _>("seq"),
                row.get::<String, _>("kind"),
                serde_json::from_str::<Value>(&row.get::<String, _>("safe_payload")).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let checkpoint_trace = sqlx::query_scalar::<_, String>(
        "SELECT json_object(\
             'checkpoint_kind',checkpoint_kind,\
             'transition_key',transition_key,\
             'scheduler_projection_version',scheduler_projection_version,\
             'fact_payload',json(fact_payload)) \
         FROM scheduler_checkpoints WHERE run_id=? \
         ORDER BY scheduler_projection_version,checkpoint_id",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|value| serde_json::from_str::<Value>(&value).unwrap())
    .collect::<Vec<_>>();
    let task_trace = sqlx::query_scalar::<_, String>(
        "SELECT json_object(\
             'task_id',task_id,'attempt_no',attempt_no,'lease_epoch',lease_epoch,\
             'task_state',task_state,'claim_mode',claim_mode,'claimed_by',claimed_by,\
             'claim_token',claim_token,'claim_expires_at',claim_expires_at,\
             'last_error_code',last_error_code,'projection_version',projection_version) \
         FROM task_outbox WHERE run_id=? ORDER BY task_id",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|value| serde_json::from_str::<Value>(&value).unwrap())
    .collect::<Vec<_>>();
    let activation_trace = sqlx::query_scalar::<_, String>(
        "SELECT json_object(\
             'activation_id',activation_id,'lifecycle',lifecycle,\
             'current_attempt_no',current_attempt_no,'current_lease_epoch',current_lease_epoch,\
             'current_fencing_token',current_fencing_token,\
             'winning_attempt_no',winning_attempt_no,'projection_version',projection_version) \
         FROM node_activations WHERE run_id=? ORDER BY activation_id",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|value| serde_json::from_str::<Value>(&value).unwrap())
    .collect::<Vec<_>>();
    let usage_trace = sqlx::query_scalar::<_, String>(
        "SELECT json_object(\
             'model_call_no',model_call_no,'call_status',call_status,\
             'finish_reason',finish_reason,'usage_complete',usage_complete) \
         FROM model_call_usage WHERE run_id=? ORDER BY model_call_no",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|value| serde_json::from_str::<Value>(&value).unwrap())
    .collect::<Vec<_>>();
    let batch_trace = sqlx::query_scalar::<_, String>(
        "SELECT json_object(\
             'model_call_no',model_call_no,'execution_status',execution_status,\
             'continuation_status',continuation_status,'parent_task_id',parent_task_id,\
             'parent_claimed_by',parent_claimed_by,'parent_claim_token',parent_claim_token,\
             'parent_claim_expires_at',parent_claim_expires_at,\
             'parent_operation_deadline',parent_operation_deadline) \
         FROM model_tool_call_batches WHERE run_id=? ORDER BY model_call_no",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|value| serde_json::from_str::<Value>(&value).unwrap())
    .collect::<Vec<_>>();
    let call_trace = sqlx::query_scalar::<_, String>(
        "SELECT json_object(\
             'model_call_no',model_call_no,'call_index',call_index,'call_id',call_id,\
             'call_status',call_status,'tool_attempt_no',tool_attempt_no,\
             'effect_evidence',effect_evidence,'claim_owner',claim_owner,\
             'claim_token',claim_token,'failure_code',failure_code,\
             'result_present',result_json IS NOT NULL) \
         FROM model_tool_calls WHERE run_id=? ORDER BY model_call_no,call_index",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|value| serde_json::from_str::<Value>(&value).unwrap())
    .collect::<Vec<_>>();
    let diagnostic = json!({
        "events": event_trace,
        "scheduler_checkpoints": checkpoint_trace,
        "task_outbox": task_trace,
        "node_activations": activation_trace,
        "model_call_usage": usage_trace,
        "model_tool_call_batches": batch_trace,
        "model_tool_calls": call_trace,
    });
    assert_eq!(attempts.len(), 1);
    let attempt = &attempts[0];
    assert_eq!(
        attempt.get::<String, _>("lifecycle"),
        "succeeded",
        "parent Attempt did not commit the continuation result: activation={}, attempt={}, \
         effect_evidence={}, failure_code={:?}, facts={diagnostic:#}",
        attempt.get::<String, _>("activation_id"),
        attempt.get::<i64, _>("attempt_no"),
        attempt.get::<String, _>("effect_evidence"),
        attempt.get::<Option<String>, _>("failure_code")
    );

    let usage_rows = sqlx::query(
        "SELECT model_call_no,call_status,finish_reason,usage,usage_complete \
         FROM model_call_usage WHERE run_id=? ORDER BY model_call_no",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(usage_rows.len(), 2);
    for (index, expected_reason) in ["tool_calls", "stop"].iter().enumerate() {
        assert_eq!(
            usage_rows[index].get::<i64, _>("model_call_no"),
            index as i64 + 1
        );
        assert_eq!(
            usage_rows[index].get::<String, _>("call_status"),
            "completed"
        );
        assert_eq!(
            usage_rows[index].get::<String, _>("finish_reason"),
            *expected_reason
        );
        assert_eq!(usage_rows[index].get::<i64, _>("usage_complete"), 1);
    }
    assert_eq!(
        serde_json::from_str::<Value>(&usage_rows[0].get::<String, _>("usage")).unwrap(),
        json!({
            "input_tokens": 10,
            "input_tokens_details": {"cached_tokens": 1},
            "output_tokens": 2,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 12
        })
    );
    assert_eq!(
        serde_json::from_str::<Value>(&usage_rows[1].get::<String, _>("usage")).unwrap(),
        json!({
            "input_tokens": 20,
            "input_tokens_details": {"cached_tokens": 2},
            "output_tokens": 4,
            "output_tokens_details": {"reasoning_tokens": 1},
            "total_tokens": 24
        })
    );

    let tool = sqlx::query(
        "SELECT call_id,call_status,tool_attempt_no,tool_task_id,effect_id,result_json,\
                started_at,completed_at \
         FROM model_tool_calls WHERE run_id=?",
    )
    .bind(&transcript.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(tool.get::<String, _>("call_id"), CALL_ID);
    assert_eq!(tool.get::<String, _>("call_status"), "succeeded");
    assert_eq!(tool.get::<i64, _>("tool_attempt_no"), 2);
    assert_eq!(
        serde_json::from_str::<Value>(&tool.get::<String, _>("result_json")).unwrap(),
        json!({"answer": "42"})
    );
    let tool_task_id = tool.get::<String, _>("tool_task_id");
    let effect_id = tool.get::<String, _>("effect_id");
    assert!(effect_id.starts_with("effect_"));
    let started_at =
        chrono::DateTime::parse_from_rfc3339(&tool.get::<String, _>("started_at")).unwrap();
    let completed_at =
        chrono::DateTime::parse_from_rfc3339(&tool.get::<String, _>("completed_at")).unwrap();
    let durable_duration_ms = completed_at
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(0) as u64;
    let public_duration_ms = transcript
        .events
        .iter()
        .find(|event| event["type"] == "run.tool.completed")
        .unwrap()["duration_ms"]
        .as_u64()
        .unwrap();
    assert_eq!(public_duration_ms, durable_duration_ms);

    assert_eq!(action.calls.load(Ordering::SeqCst), 2);
    let records = {
        let records = action.records.lock().unwrap();
        assert_eq!(records.len(), 2);
        records.clone()
    };
    for (index, record) in records.iter().enumerate() {
        assert_eq!(
            record.input,
            json!({
                "query": "durable",
                "server_token": SERVER_ONLY_SECRET_SENTINEL
            })
        );
        assert_eq!(record.run_id, transcript.run_id);
        assert_eq!(
            record.operation_id,
            format!(
                "model_tool_{}",
                tool_task_id
                    .strip_prefix("task_")
                    .expect("durable tool task uses its closed task namespace")
            )
        );
        assert_eq!(record.attempt, index as u32 + 1);
        assert_eq!(record.idempotency_key, effect_id);
    }

    let function_item = sqlx::query(
        "SELECT item_id,item_status,seal_index,safe_item FROM response_public_items \
         WHERE run_id=? AND item_kind='function_call'",
    )
    .bind(&transcript.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(function_item.get::<String, _>("item_status"), "completed");
    assert_eq!(function_item.get::<i64, _>("seal_index"), 5);
    let safe_item =
        serde_json::from_str::<Value>(&function_item.get::<String, _>("safe_item")).unwrap();
    assert_eq!(safe_item["id"], function_item.get::<String, _>("item_id"));
    assert_eq!(safe_item["type"], "function_call");
    assert_eq!(safe_item["status"], "completed");
    assert_eq!(safe_item["call_id"], CALL_ID);
    assert_eq!(safe_item["name"], "lookup");
    assert_eq!(safe_item["arguments"], TOOL_ARGUMENTS);

    let public_items = sqlx::query(
        "SELECT output_index,item_kind,item_status,safe_item FROM response_public_items \
         WHERE run_id=? ORDER BY output_index",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(public_items.len(), 2);
    assert_eq!(public_items[0].get::<i64, _>("output_index"), 0);
    assert_eq!(
        public_items[0].get::<String, _>("item_kind"),
        "function_call"
    );
    assert_eq!(public_items[0].get::<String, _>("item_status"), "completed");
    assert_eq!(public_items[1].get::<i64, _>("output_index"), 1);
    assert_eq!(public_items[1].get::<String, _>("item_kind"), "message");
    assert_eq!(public_items[1].get::<String, _>("item_status"), "completed");
    let final_message =
        serde_json::from_str::<Value>(&public_items[1].get::<String, _>("safe_item")).unwrap();
    assert_eq!(final_message["content"][0]["text"], FINAL_TEXT);

    pool.close().await;
}

#[tokio::test]
async fn attached_run_executes_one_durable_tool_round_and_publishes_the_sealed_contract() {
    let fixture = Fixture::start().await;
    let transcript = attached_transcript(&fixture.app, &fixture.model, AGENT_ID).await;

    let run = get_run(&fixture.app, &transcript.run_id).await;
    fixture
        .service
        .shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    assert_durable_authority(&fixture.database, &transcript, &fixture.action).await;
    assert_no_secret_in_durable_state(&fixture.database, &transcript.run_id).await;

    assert_provider_requests(&fixture.model);
    assert_public_events(&transcript);
    assert_no_secret_in_public_surfaces(&transcript, &run);
    assert_eq!(run["data"]["status"], "completed");
    assert!(run["data"].get("response_id").is_none());
    assert_eq!(run["data"]["output"]["data"], FINAL_TEXT);
    assert_eq!(
        run["data"]["output"]["data"],
        transcript.terminal()["run"]["result"]
    );
}

#[tokio::test]
async fn terminal_only_attached_run_uses_the_same_progress_wire_and_persists_calibration() {
    let fixture = Fixture::start_with_action_retry(false).await;
    let transcript = attached_transcript(&fixture.app, &fixture.model, TERMINAL_AGENT_ID).await;

    let started = transcript
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| event["type"] == "run.tool.started")
        .collect::<Vec<_>>();
    let progress = transcript
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| event["type"] == "run.tool.progress")
        .collect::<Vec<_>>();
    let completed = transcript
        .events
        .iter()
        .enumerate()
        .filter(|(_, event)| event["type"] == "run.tool.completed")
        .collect::<Vec<_>>();
    assert_eq!(started.len(), 1);
    assert_eq!(progress.len(), 2);
    assert_eq!(completed.len(), 1);
    assert!(started[0].0 < progress[0].0);
    assert!(progress[0].0 < progress[1].0);
    assert!(progress[1].0 < completed[0].0);
    assert!(completed[0].1["duration_ms"].as_u64().is_some());
    assert_eq!(
        completed[0].1["content"],
        json!([{"type": "output_json", "json": {"answer": "42"}}])
    );
    assert_eq!(
        progress
            .iter()
            .map(|(_, event)| event["content"][0]["json"]["completed"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(transcript
        .events
        .iter()
        .any(|event| event["type"] == "run.output.function_call.arguments.done"));
    assert!(transcript
        .events
        .iter()
        .any(|event| event["type"] == "run.output.text.delta"));

    let terminal = transcript.terminal();
    assert_eq!(terminal["type"], "run.lifecycle.completed");
    assert_eq!(terminal["run"]["result"], FINAL_TEXT);
    assert_eq!(
        terminal["run"]["tool_results"],
        json!([{
            "call_id": CALL_ID,
            "tool_name": "lookup",
            "content": [{"type": "output_json", "json": {"answer": "42"}}]
        }])
    );
    assert_eq!(
        terminal["run"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "id",
            "interactions",
            "object",
            "output",
            "retrievals",
            "result",
            "status",
            "tool_results",
            "usage",
            "usage_status"
        ])
    );
    assert_eq!(terminal["run"]["interactions"], json!([]));
    assert_eq!(fixture.action.calls.load(Ordering::SeqCst), 1);

    fixture
        .service
        .shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    let pool = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&fixture.database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let stored = sqlx::query(
        "SELECT terminal_state,tool_results_json
         FROM terminal_run_results WHERE run_id=?",
    )
    .bind(&transcript.run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored.get::<String, _>("terminal_state"), "succeeded");
    assert_eq!(
        serde_json::from_str::<Value>(&stored.get::<String, _>("tool_results_json")).unwrap(),
        terminal["run"]["tool_results"]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM terminal_run_admissions WHERE run_id=?")
            .bind(&transcript.run_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM terminal_run_results WHERE run_id=?")
            .bind(&transcript.run_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let full_runtime_rows = sqlx::query_as::<_, (String, i64)>(
        "WITH selected(run_id) AS (VALUES (?))
         SELECT 'execution_events',COUNT(*) FROM execution_events
           WHERE run_id=(SELECT run_id FROM selected)
         UNION ALL SELECT 'public_event_outbox',COUNT(*) FROM public_event_outbox
           WHERE run_id=(SELECT run_id FROM selected)
         UNION ALL SELECT 'response_public_items',COUNT(*) FROM response_public_items
           WHERE run_id=(SELECT run_id FROM selected)
         UNION ALL SELECT 'model_call_usage',COUNT(*) FROM model_call_usage
           WHERE run_id=(SELECT run_id FROM selected)
         UNION ALL SELECT 'model_tool_call_batches',COUNT(*) FROM model_tool_call_batches
           WHERE run_id=(SELECT run_id FROM selected)
         UNION ALL SELECT 'model_tool_calls',COUNT(*) FROM model_tool_calls
           WHERE run_id=(SELECT run_id FROM selected)
         UNION ALL SELECT 'run_stream_snapshots',COUNT(*) FROM run_stream_snapshots
           WHERE run_id=(SELECT run_id FROM selected)",
    )
    .bind(&transcript.run_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    for (table, rows) in full_runtime_rows {
        assert_eq!(
            rows, 0,
            "terminal-only output deltas and tool progress must not add {table} writes"
        );
    }
    pool.close().await;
}
#[path = "support/database.rs"]
mod database;
