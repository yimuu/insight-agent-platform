use std::{
    collections::{BTreeMap, BTreeSet},
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
    catalog_v3::{
        compile_v3_agent_dir, DeployedV3Agent, LeafDeploymentResolver,
        ProductionLeafDeploymentResolver, ResolvedLeafDeployment,
    },
    dsl::CompileError,
    engine::{
        plan::LeafTaskDescriptor, production_worker_registry_with_live_response,
        LocalContentAddressedArtifactStore, RunId, SubflowContractRegistry, VersionTag,
    },
    history::types::RunStatus,
    resources::{
        actions::ActionRegistry,
        models::{
            ChatChunk, ChatEvent, ChatEventStream, ChatFinishReason, ChatInputTokensDetails,
            ChatModel, ChatOutputTokensDetails, ChatRequest, ChatResponse, ChatStream, ChatUsage,
            ModelCapability, ModelDeploymentIdentity, ModelRegistry, ModelRequestCapability,
        },
    },
    runtime::{
        DeployedAgentCatalog, InMemoryLiveResponseBroker, LiveResponseBroker,
        LiveResponseBrokerCapability, LiveResponseBrokerError, LiveResponseCloseOutcome,
        LiveResponsePublication, LiveResponsePublishOutcome, LiveResponseSeal,
        LiveResponseSubscriber, ProductionRunRepository, ResponseStreamEvent, RunError, RunService,
        RunServiceConfig,
    },
};
use insight_api::v1::{build_router, ApiAuth, ApiState};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tower::ServiceExt;

const MODEL_WORKER_VERSION: &str = "response-stream-fixture-worker-1";
const FAILURE_QUESTION: &str = "fixture terminal failure";
const BACKPRESSURE_QUESTION: &str = "emit many frames for an unread response";
const TCP_BACKPRESSURE_QUESTION: &str = "fill a bounded real tcp response for an unread client";
const TCP_BACKPRESSURE_DELTA_BYTES: usize = 1;
const TCP_BACKPRESSURE_EVENT_COUNT: usize = 2 * 1_024;
const PARALLEL_LEFT_MARKER: &str = "parallel isolation left";
const PARALLEL_RIGHT_MARKER: &str = "parallel isolation right";
const LOOP_MARKER: &str = "loop isolation state";
const LOOP_INITIAL_STATE: &str = "loop-zero";
const LOOP_FIRST_STATE: &str = "loop-one";
const LOOP_SECOND_STATE: &str = "loop-two";

#[derive(Debug, Clone, Default)]
struct ModeCounters {
    complete: Arc<AtomicUsize>,
    streaming: Arc<AtomicUsize>,
    streaming_finished: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
struct DualModeFixtureModel {
    counters: ModeCounters,
}

#[derive(Debug, Clone, Default)]
struct IsolationModelState {
    parallel_arrivals: Arc<AtomicUsize>,
    parallel_active: Arc<AtomicUsize>,
    parallel_max_active: Arc<AtomicUsize>,
    parallel_ready: Arc<Notify>,
    loop_calls: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
struct IsolationFixtureModel {
    state: IsolationModelState,
}

impl IsolationFixtureModel {
    fn request_text(request: &ChatRequest) -> String {
        serde_json::to_string(&request.messages).expect("fixture chat messages serialize")
    }

    fn usage() -> ChatUsage {
        DualModeFixtureModel::usage()
    }

    fn two_chunk_stream(
        first: &'static str,
        second: &'static str,
        first_delay: Duration,
        second_delay: Duration,
    ) -> ChatEventStream {
        Box::pin(
            stream::iter([(first_delay, first, false), (second_delay, second, true)]).then(
                |(delay, text, final_event)| async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    Ok(ChatEvent {
                        text_delta: text.to_owned(),
                        finish_reason: final_event.then_some(ChatFinishReason::Stop),
                        usage: final_event.then(Self::usage),
                        ..ChatEvent::default()
                    })
                },
            ),
        )
    }

    async fn rendezvous_parallel_calls(&self) -> Result<(), RunError> {
        let active = self.state.parallel_active.fetch_add(1, Ordering::SeqCst) + 1;
        self.state
            .parallel_max_active
            .fetch_max(active, Ordering::SeqCst);
        let arrivals = self.state.parallel_arrivals.fetch_add(1, Ordering::SeqCst) + 1;
        if arrivals >= 2 {
            self.state.parallel_ready.notify_waiters();
        }

        let ready = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self.state.parallel_arrivals.load(Ordering::SeqCst) >= 2 {
                    break;
                }
                let notified = self.state.parallel_ready.notified();
                if self.state.parallel_arrivals.load(Ordering::SeqCst) >= 2 {
                    break;
                }
                notified.await;
            }
        })
        .await;
        self.state.parallel_active.fetch_sub(1, Ordering::SeqCst);
        ready.map_err(|_| {
            RunError::infrastructure(
                "FIXTURE_PARALLEL_RENDEZVOUS_TIMEOUT",
                "parallel LLM calls did not overlap at the Provider boundary",
            )
        })
    }
}

#[async_trait]
impl ChatModel for IsolationFixtureModel {
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
        Err(RunError::infrastructure(
            "FIXTURE_UNEXPECTED_COMPLETE_REQUEST",
            "isolation fixture only accepts streaming requests",
        ))
    }

    async fn stream_chat_events(&self, request: ChatRequest) -> Result<ChatEventStream, RunError> {
        let request = Self::request_text(&request);
        if request.contains(PARALLEL_LEFT_MARKER) {
            self.rendezvous_parallel_calls().await?;
            return Ok(Self::two_chunk_stream(
                "left-",
                "complete",
                Duration::ZERO,
                Duration::from_millis(200),
            ));
        }
        if request.contains(PARALLEL_RIGHT_MARKER) {
            self.rendezvous_parallel_calls().await?;
            return Ok(Self::two_chunk_stream(
                "right-",
                "complete",
                Duration::from_millis(80),
                Duration::from_millis(220),
            ));
        }
        if request.contains(LOOP_MARKER) {
            self.state.loop_calls.fetch_add(1, Ordering::SeqCst);
            if request.contains(LOOP_INITIAL_STATE) {
                return Ok(Self::two_chunk_stream(
                    "loop-",
                    "one",
                    Duration::ZERO,
                    Duration::from_millis(20),
                ));
            }
            if request.contains(LOOP_FIRST_STATE) {
                return Ok(Self::two_chunk_stream(
                    "loop-",
                    "two",
                    Duration::ZERO,
                    Duration::from_millis(20),
                ));
            }
        }
        Err(RunError::infrastructure(
            "FIXTURE_ISOLATION_REQUEST_INVALID",
            "isolation fixture received an unknown request",
        ))
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Err(RunError::infrastructure(
            "FIXTURE_LEGACY_STREAM_UNEXPECTED",
            "isolation fixture requires typed chat events",
        ))
    }
}

impl DualModeFixtureModel {
    fn usage() -> ChatUsage {
        ChatUsage {
            input_tokens: Some(3),
            input_tokens_details: Some(ChatInputTokensDetails {
                cached_tokens: Some(1),
            }),
            output_tokens: Some(2),
            output_tokens_details: Some(ChatOutputTokensDetails {
                reasoning_tokens: Some(0),
            }),
            total_tokens: Some(5),
        }
    }

    fn fails(request: &ChatRequest) -> bool {
        serde_json::to_string(&request.messages)
            .expect("fixture chat messages serialize")
            .contains(FAILURE_QUESTION)
    }

    fn exercises_backpressure(request: &ChatRequest) -> bool {
        serde_json::to_string(&request.messages)
            .expect("fixture chat messages serialize")
            .contains(BACKPRESSURE_QUESTION)
    }

    fn exercises_tcp_backpressure(request: &ChatRequest) -> bool {
        serde_json::to_string(&request.messages)
            .expect("fixture chat messages serialize")
            .contains(TCP_BACKPRESSURE_QUESTION)
    }
}

#[async_trait]
impl ChatModel for DualModeFixtureModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::from([ModelCapability::Vision])
    }

    fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
        BTreeSet::from([
            ModelRequestCapability::Complete,
            ModelRequestCapability::Streaming,
        ])
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
        self.counters.complete.fetch_add(1, Ordering::SeqCst);
        if Self::fails(&request) {
            return Ok(ChatResponse {
                text: String::new(),
                tool_calls: Vec::new(),
                finish_reason: ChatFinishReason::Length,
                usage: None,
            });
        }
        Ok(ChatResponse {
            text: "fixture answer".to_owned(),
            tool_calls: Vec::new(),
            finish_reason: ChatFinishReason::Stop,
            usage: Some(Self::usage()),
        })
    }

    async fn stream_chat_events(&self, request: ChatRequest) -> Result<ChatEventStream, RunError> {
        self.counters.streaming.fetch_add(1, Ordering::SeqCst);
        if Self::fails(&request) {
            return Ok(Box::pin(stream::iter([Ok(ChatEvent {
                finish_reason: Some(ChatFinishReason::Length),
                ..ChatEvent::default()
            })])));
        }
        let finished = Arc::clone(&self.counters.streaming_finished);
        if Self::exercises_tcp_backpressure(&request) {
            return Ok(Box::pin(stream::unfold(0_usize, move |state| {
                let finished = Arc::clone(&finished);
                async move {
                    if state >= TCP_BACKPRESSURE_EVENT_COUNT {
                        return None;
                    }
                    if state % 8 == 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    } else {
                        tokio::task::yield_now().await;
                    }
                    let final_event = state + 1 == TCP_BACKPRESSURE_EVENT_COUNT;
                    if final_event {
                        finished.fetch_add(1, Ordering::SeqCst);
                    }
                    Some((
                        Ok(ChatEvent {
                            text_delta: "x".repeat(TCP_BACKPRESSURE_DELTA_BYTES),
                            finish_reason: final_event.then_some(ChatFinishReason::Stop),
                            usage: final_event.then(Self::usage),
                            ..ChatEvent::default()
                        }),
                        state + 1,
                    ))
                }
            })));
        }
        if Self::exercises_backpressure(&request) {
            return Ok(Box::pin(stream::unfold(0_u8, move |state| {
                let finished = Arc::clone(&finished);
                async move {
                    if state >= 40 {
                        return None;
                    }
                    let final_event = state == 39;
                    if final_event {
                        finished.fetch_add(1, Ordering::SeqCst);
                    }
                    Some((
                        Ok(ChatEvent {
                            text_delta: "x".to_owned(),
                            finish_reason: final_event.then_some(ChatFinishReason::Stop),
                            usage: final_event.then(Self::usage),
                            ..ChatEvent::default()
                        }),
                        state + 1,
                    ))
                }
            })));
        }
        Ok(Box::pin(stream::unfold(0_u8, move |state| {
            let finished = Arc::clone(&finished);
            async move {
                match state {
                    0 => Some((
                        Ok(ChatEvent {
                            text_delta: "fixture ".to_owned(),
                            ..ChatEvent::default()
                        }),
                        1,
                    )),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        finished.fetch_add(1, Ordering::SeqCst);
                        Some((
                            Ok(ChatEvent {
                                text_delta: "answer".to_owned(),
                                finish_reason: Some(ChatFinishReason::Stop),
                                usage: Some(Self::usage()),
                                ..ChatEvent::default()
                            }),
                            2,
                        ))
                    }
                    _ => None,
                }
            }
        })))
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::iter([Ok(ChatChunk {
            text: "fixture answer".to_owned(),
            finish_reason: Some("stop".to_owned()),
            usage: Some(json!({
                "input_tokens": 3,
                "input_tokens_details": {"cached_tokens": 1},
                "output_tokens": 2,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 5
            })),
        })])))
    }
}

struct GraphPublicationResolver;

impl LeafDeploymentResolver for GraphPublicationResolver {
    fn resolve_leaf(
        &self,
        kind: insight_agent_platform::engine::LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        let worker_version = match kind {
            insight_agent_platform::engine::LeafTaskKind::Llm => MODEL_WORKER_VERSION,
            _ => "response-stream-graph-fixture-1",
        };
        ResolvedLeafDeployment::new(
            VersionTag::new(worker_version).unwrap(),
            json!({
                "kind": kind.name(),
                "implementation": descriptor.implementation,
                "fixture": "response-stream"
            }),
        )
    }
}

#[derive(Clone)]
struct DropSealAndCloseBroker {
    inner: InMemoryLiveResponseBroker,
}

#[async_trait]
impl LiveResponseBroker for DropSealAndCloseBroker {
    fn deployment_capability(&self) -> LiveResponseBrokerCapability {
        self.inner.deployment_capability()
    }

    async fn check_readiness(
        &self,
        readiness_timeout: Duration,
    ) -> Result<(), LiveResponseBrokerError> {
        self.inner.check_readiness(readiness_timeout).await
    }

    async fn shutdown(&self, grace: Duration) -> Result<(), LiveResponseBrokerError> {
        self.inner.shutdown(grace).await
    }

    async fn subscribe(
        &self,
        run_id: RunId,
    ) -> Result<Box<dyn LiveResponseSubscriber>, LiveResponseBrokerError> {
        self.inner.subscribe(run_id).await
    }

    fn publish(&self, publication: LiveResponsePublication) -> LiveResponsePublishOutcome {
        self.inner.publish(publication)
    }

    fn seal(&self, _seal: LiveResponseSeal) -> LiveResponsePublishOutcome {
        LiveResponsePublishOutcome::SealEnqueued
    }

    fn close_run(&self, _run_id: &RunId) -> LiveResponseCloseOutcome {
        LiveResponseCloseOutcome::default()
    }
}

struct Fixture {
    app: Router,
    service: RunService,
    counters: ModeCounters,
    isolation: IsolationModelState,
    repository: Arc<dyn ProductionRunRepository>,
    _temporary: tempfile::TempDir,
}

impl Fixture {
    async fn start() -> Self {
        Self::start_with_run_timeout(Duration::from_secs(10)).await
    }

    async fn start_with_run_timeout(run_timeout: Duration) -> Self {
        Self::start_with_timeouts(run_timeout, Duration::from_secs(2)).await
    }

    async fn start_with_timeouts(run_timeout: Duration, outbound_write_timeout: Duration) -> Self {
        let broker: Arc<dyn LiveResponseBroker> =
            Arc::new(InMemoryLiveResponseBroker::new(64, 16).unwrap());
        Self::start_with_broker_and_timeouts(run_timeout, outbound_write_timeout, broker).await
    }

    async fn start_with_broker_and_timeouts(
        run_timeout: Duration,
        outbound_write_timeout: Duration,
        broker: Arc<dyn LiveResponseBroker>,
    ) -> Self {
        Self::start_with_broker_and_response_timeouts(
            run_timeout,
            outbound_write_timeout,
            Duration::from_millis(100),
            broker,
        )
        .await
    }

    async fn start_with_broker_and_response_timeouts(
        run_timeout: Duration,
        outbound_write_timeout: Duration,
        terminal_barrier_timeout: Duration,
        broker: Arc<dyn LiveResponseBroker>,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().to_path_buf();
        let counters = ModeCounters::default();
        let isolation = IsolationModelState::default();
        let mut models = ModelRegistry::default();
        models
            .register_versioned(
                "fixture_model",
                ModelDeploymentIdentity::new(
                    MODEL_WORKER_VERSION,
                    json!({"adapter": "dual-mode-fixture", "provider_model": "fixed"}),
                )
                .unwrap(),
                DualModeFixtureModel {
                    counters: counters.clone(),
                },
            )
            .unwrap();
        models
            .register_versioned(
                "vision_chat",
                ModelDeploymentIdentity::new(
                    MODEL_WORKER_VERSION,
                    json!({"adapter": "dual-mode-fixture", "provider_model": "vision-fixed"}),
                )
                .unwrap(),
                DualModeFixtureModel {
                    counters: counters.clone(),
                },
            )
            .unwrap();
        models
            .register_versioned(
                "isolation_model",
                ModelDeploymentIdentity::new(
                    MODEL_WORKER_VERSION,
                    json!({"adapter": "stream-isolation-fixture", "provider_model": "fixed"}),
                )
                .unwrap(),
                IsolationFixtureModel {
                    state: isolation.clone(),
                },
            )
            .unwrap();
        let actions = ActionRegistry::default();
        let publication_resolver = ProductionLeafDeploymentResolver::new(&models, &actions);
        let mut deployed = Vec::new();
        for (stream_enabled, publish_enabled) in
            [(true, true), (true, false), (false, true), (false, false)]
        {
            let agent_id = agent_id(stream_enabled, publish_enabled);
            let directory = root.join(agent_id);
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(
                directory.join("agent.yaml"),
                format!(
                    r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: {agent_id}
  name: Response stream fixture
  description: Exercises one stream and publish combination.
inputs:
  question:
    type: string
output: string
workflow:
  steps:
    - id: answer
      type: llm
      model: fixture_model
      stream: {stream_enabled}
      publish: {publish_enabled}
      messages:
        - role: user
          content:
            - text: $question
      response: string
    - return: $answer
"#
                ),
            )
            .unwrap();
            let published = Arc::new(compile_v3_agent_dir(&directory).unwrap());
            deployed.push(Arc::new(
                DeployedV3Agent::publish(
                    published,
                    &publication_resolver,
                    SubflowContractRegistry::new(),
                )
                .unwrap(),
            ));
        }
        let parallel_directory = root.join("response_stream_parallel_isolation");
        std::fs::create_dir_all(&parallel_directory).unwrap();
        std::fs::write(
            parallel_directory.join("agent.yaml"),
            r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: response_stream_parallel_isolation
  name: Parallel response stream isolation fixture
  description: Interleaves two public LLM response items across parallel legs.
inputs: {}
output: any
workflow:
  steps:
    - id: answers
      settle: all_success
      parallel:
        left:
          - id: parallel_left_answer
            type: llm
            model: isolation_model
            stream: true
            publish: true
            messages:
              - role: user
                content:
                  - text: parallel isolation left
            response: string
          - yield: $parallel_left_answer
        right:
          - id: parallel_right_answer
            type: llm
            model: isolation_model
            stream: true
            publish: true
            messages:
              - role: user
                content:
                  - text: parallel isolation right
            response: string
          - yield: $parallel_right_answer
    - return: $answers
"#,
        )
        .unwrap();
        let parallel = Arc::new(compile_v3_agent_dir(&parallel_directory).unwrap());
        deployed.push(Arc::new(
            DeployedV3Agent::publish(
                parallel,
                &publication_resolver,
                SubflowContractRegistry::new(),
            )
            .unwrap(),
        ));

        let loop_directory = root.join("response_stream_loop_isolation");
        std::fs::create_dir_all(&loop_directory).unwrap();
        std::fs::write(
            loop_directory.join("agent.yaml"),
            r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: response_stream_loop_isolation
  name: Loop response stream isolation fixture
  description: Reuses one public LLM node across two durable loop occurrences.
inputs:
  seed: string
output: string
workflow:
  steps:
    - id: repeated
      loop:
        initial: $seed
        as: state
        until: false
        max_iterations: 2
        steps:
          - id: loop_answer
            type: llm
            model: isolation_model
            stream: true
            publish: true
            messages:
              - role: user
                content:
                  - text: loop isolation state
                  - text: $state
            response: string
          - continue: $loop_answer
    - return: $repeated
"#,
        )
        .unwrap();
        let loop_agent = Arc::new(compile_v3_agent_dir(&loop_directory).unwrap());
        deployed.push(Arc::new(
            DeployedV3Agent::publish(
                loop_agent,
                &publication_resolver,
                SubflowContractRegistry::new(),
            )
            .unwrap(),
        ));
        let wait_directory = root.join("response_stream_wait");
        std::fs::create_dir_all(&wait_directory).unwrap();
        std::fs::write(
            wait_directory.join("agent.yaml"),
            r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: response_stream_wait
  name: Response stream wait fixture
  description: Waits on a durable signal for terminal lifecycle tests.
inputs: {}
output: string
workflow:
  steps:
    - id: gate
      wait: {signal: continue, response: string}
    - return: $gate
"#,
        )
        .unwrap();
        let wait = Arc::new(compile_v3_agent_dir(&wait_directory).unwrap());
        deployed.push(Arc::new(
            DeployedV3Agent::publish(wait, &publication_resolver, SubflowContractRegistry::new())
                .unwrap(),
        ));
        for (agent_id, wait_after_publication) in [
            ("response_stream_backpressure_wait", true),
            ("response_stream_backpressure_complete", false),
        ] {
            let directory = root.join(agent_id);
            std::fs::create_dir_all(&directory).unwrap();
            let mut steps = String::new();
            for index in 0..1 {
                steps.push_str(&format!(
                    r#"    - id: answer_{index}
      type: llm
      model: fixture_model
      stream: true
      publish: true
      messages:
        - role: user
          content:
            - text: $question
      response: string
"#
                ));
            }
            if wait_after_publication {
                steps.push_str(
                    r#"    - id: gate
      wait: {signal: continue, response: string}
    - return: $gate
"#,
                );
            } else {
                steps.push_str("    - return: $answer_0\n");
            }
            std::fs::write(
                directory.join("agent.yaml"),
                format!(
                    r#"api_version: insight.agent/v3
kind: agent
metadata:
  id: {agent_id}
  name: Response stream outbound backpressure fixture
  description: Produces enough public frames to fill an unread HTTP response.
inputs:
  question:
    type: string
output: string
workflow:
  steps:
{steps}"#
                ),
            )
            .unwrap();
            let published = Arc::new(compile_v3_agent_dir(&directory).unwrap());
            deployed.push(Arc::new(
                DeployedV3Agent::publish(
                    published,
                    &publication_resolver,
                    SubflowContractRegistry::new(),
                )
                .unwrap(),
            ));
        }
        let medical = Arc::new(
            compile_v3_agent_dir(
                &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("agents/medical_report_interpreter"),
            )
            .unwrap(),
        );
        deployed.push(Arc::new(
            DeployedV3Agent::publish(
                medical,
                &publication_resolver,
                SubflowContractRegistry::new(),
            )
            .unwrap(),
        ));

        let workers =
            production_worker_registry_with_live_response(&models, &actions, Arc::clone(&broker))
                .unwrap();
        let repository: Arc<dyn ProductionRunRepository> = Arc::new(
            insight_agent_platform::engine::repository::SqliteDurableRepository::connect_path(
                &root.join("response-stream.sqlite"),
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
        config.run_timeout = run_timeout;
        config.terminal_barrier_timeout = terminal_barrier_timeout;
        config.outbound_write_timeout = outbound_write_timeout;
        config.artifact_gc_interval = Duration::from_secs(60);
        config.public_event_prune_interval = Duration::from_secs(60);
        let service = RunService::start_with_artifact_store_graph_publication_and_live_response(
            DeployedAgentCatalog::new(deployed).unwrap(),
            Arc::clone(&repository),
            workers,
            broker,
            artifact_store,
            Arc::new(GraphPublicationResolver),
            config,
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
            counters,
            isolation,
            repository,
            _temporary: temporary,
        }
    }
}

struct Transcript {
    run_id: String,
    response_id: String,
    values: Vec<Value>,
}

impl Transcript {
    fn event_types(&self) -> Vec<&str> {
        self.values
            .iter()
            .map(|event| event["type"].as_str().unwrap())
            .collect()
    }

    fn deltas(&self) -> Vec<&str> {
        self.values
            .iter()
            .filter(|event| event["type"] == "response.output_text.delta")
            .map(|event| event["delta"].as_str().unwrap())
            .collect()
    }

    fn terminal(&self) -> &Value {
        self.values.last().unwrap()
    }
}

fn reconstructed_public_text_items(
    transcript: &Transcript,
) -> (BTreeMap<String, u64>, BTreeMap<String, String>) {
    let items = transcript
        .values
        .iter()
        .filter(|event| {
            event["type"] == "response.output_item.added" && event["item"]["type"] == "message"
        })
        .map(|event| {
            (
                event["item"]["id"].as_str().unwrap().to_owned(),
                event["output_index"].as_u64().unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut text = items
        .keys()
        .map(|item_id| (item_id.clone(), String::new()))
        .collect::<BTreeMap<_, _>>();
    for event in transcript
        .values
        .iter()
        .filter(|event| event["type"] == "response.output_text.delta")
    {
        let item_id = event["item_id"].as_str().unwrap();
        assert_eq!(
            items.get(item_id),
            event["output_index"].as_u64().as_ref(),
            "every delta must retain its item's durable output index"
        );
        text.get_mut(item_id)
            .expect("delta must reference an added item")
            .push_str(event["delta"].as_str().unwrap());
    }
    (items, text)
}

fn assert_terminal_output_matches_reconstructed_text(
    transcript: &Transcript,
    items: &BTreeMap<String, u64>,
    text: &BTreeMap<String, String>,
) {
    let output = transcript.terminal()["response"]["output"]
        .as_array()
        .unwrap();
    assert_eq!(output.len(), items.len());
    for (output_index, item) in output.iter().enumerate() {
        let item_id = item["id"].as_str().unwrap();
        assert_eq!(items.get(item_id), Some(&(output_index as u64)));
        assert_eq!(item["status"], "completed");
        assert_eq!(
            item["content"][0]["text"].as_str().unwrap(),
            text.get(item_id).unwrap()
        );
    }
}

fn agent_id(stream_enabled: bool, publish_enabled: bool) -> &'static str {
    match (stream_enabled, publish_enabled) {
        (true, true) => "response_stream_tt",
        (true, false) => "response_stream_tf",
        (false, true) => "response_stream_ft",
        (false, false) => "response_stream_ff",
    }
}

async fn attached_transcript(app: &Router, agent_id: &str) -> Transcript {
    attached_transcript_with_body(
        app,
        agent_id,
        &format!("request-{agent_id}"),
        json!({"question": "show the response"}),
    )
    .await
}

async fn attached_transcript_with_body(
    app: &Router,
    agent_id: &str,
    request_id: &str,
    input: Value,
) -> Transcript {
    let response = start_attached_response(app, agent_id, request_id, input).await;
    consume_attached_response(response).await
}

async fn start_attached_response(
    app: &Router,
    agent_id: &str,
    request_id: &str,
    input: Value,
) -> axum::response::Response {
    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/v1/agents/{agent_id}/runs/stream"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-request-id", request_id)
                .body(Body::from(serde_json::to_vec(&input).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
    assert_eq!(
        response.headers()[header::CACHE_CONTROL].to_str().unwrap(),
        "no-store, no-transform"
    );
    assert_eq!(response.headers()["x-accel-buffering"], "no");
    response
}

async fn consume_attached_response(response: axum::response::Response) -> Transcript {
    let run_id = response.headers()["x-run-id"].to_str().unwrap().to_owned();
    let response_id = response.headers()["x-response-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let body = tokio::time::timeout(
        Duration::from_secs(5),
        to_bytes(response.into_body(), 1 << 20),
    )
    .await
    .expect("attached response did not reach terminal EOF")
    .unwrap();
    parse_attached_transcript(run_id, response_id, body.to_vec())
}

async fn consume_attached_response_asserting_live_delta(
    response: axum::response::Response,
    counters: &ModeCounters,
) -> Transcript {
    let run_id = response.headers()["x-run-id"].to_str().unwrap().to_owned();
    let response_id = response.headers()["x-response-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let completed_before = counters.streaming_finished.load(Ordering::SeqCst);
    let mut body = response.into_body().into_data_stream();
    let mut bytes = Vec::new();

    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
            assert!(
                bytes.len() <= 1 << 20,
                "attached response exceeded test bound"
            );
            let observed = String::from_utf8_lossy(&bytes);
            if observed.contains("event: response.output_text.delta") {
                assert_eq!(
                    counters.streaming_finished.load(Ordering::SeqCst),
                    completed_before,
                    "the first medical body delta must precede its model-call completion"
                );
                assert!(
                    !observed.contains("event: response.completed"),
                    "the first medical body delta must precede Run terminal"
                );
                return;
            }
        }
        panic!(
            "medical response reached EOF before its first body delta: {}",
            String::from_utf8_lossy(&bytes)
        );
    })
    .await
    .expect("the first medical body delta was not delivered promptly");

    // The fixture permits a Run to execute for up to 10 seconds. Under the
    // full CI suite, concurrent streaming fixtures can delay terminal
    // propagation beyond the shorter focused-test timing.
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
            assert!(
                bytes.len() <= 1 << 20,
                "attached response exceeded test bound"
            );
        }
    })
    .await
    .expect("medical response did not reach terminal EOF");

    parse_attached_transcript(run_id, response_id, bytes)
}

fn parse_attached_transcript(run_id: String, response_id: String, body: Vec<u8>) -> Transcript {
    let raw = String::from_utf8(body).unwrap();
    assert!(raw.lines().all(|line| !line.starts_with("id:")));
    assert!(!raw.contains("output_bytes"));
    assert!(
        raw.ends_with("\n\n"),
        "the terminal frame must be followed by EOF"
    );

    let mut events: Vec<ResponseStreamEvent> = Vec::new();
    let mut values = Vec::new();
    let frames = raw
        .split_terminator("\n\n")
        .filter(|frame| !frame.trim().is_empty())
        .collect::<Vec<_>>();
    for frame in &frames {
        let Some(event_name) = frame.lines().find_map(|line| line.strip_prefix("event: ")) else {
            continue;
        };
        let data = frame
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .expect("every named SSE event has data");
        let value: Value = serde_json::from_str(data).unwrap();
        assert_eq!(value["type"], event_name);
        assert!(!event_name.starts_with("run."));
        assert!(!event_name.starts_with("operation."));
        assert_no_private_runtime_keys(&value);
        events.push(serde_json::from_value(value.clone()).unwrap());
        values.push(value);
    }
    assert!(!events.is_empty());
    for (expected, event) in events.iter().enumerate() {
        assert_eq!(event.sequence_number(), expected as u64);
    }
    assert_eq!(values[0]["type"], "response.created");
    assert_eq!(values[1]["type"], "response.in_progress");
    assert_eq!(values[0]["response"]["id"], response_id);
    assert_eq!(values[1]["response"]["id"], response_id);
    assert!(events.last().unwrap().is_terminal());
    assert_eq!(
        events.iter().filter(|event| event.is_terminal()).count(),
        1,
        "a response stream must contain exactly one terminal event"
    );
    let final_frame_event = frames
        .last()
        .and_then(|frame| frame.lines().find_map(|line| line.strip_prefix("event: ")))
        .expect("the frame immediately before EOF must be a named terminal event");
    assert_eq!(final_frame_event, values.last().unwrap()["type"]);

    Transcript {
        run_id,
        response_id,
        values,
    }
}

fn assert_no_private_runtime_keys(value: &Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                assert!(
                    !matches!(key.as_str(), "run" | "operation" | "output_bytes"),
                    "private runtime field leaked into response stream: {key}"
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
    let status = response.status();
    let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected GET Run response: {}",
        String::from_utf8_lossy(&body)
    );
    serde_json::from_slice(&body).unwrap()
}

async fn wait_for_run_status(
    app: &Router,
    run_id: &str,
    expected: &str,
    timeout: Duration,
) -> Value {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let run = get_run(app, run_id).await;
        if run["data"]["status"] == expected {
            return run;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Run {run_id} did not reach {expected} before the bounded deadline: {run}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_run_status_through_transient_unavailability(
    app: &Router,
    run_id: &str,
    expected: &str,
    timeout: Duration,
) -> Value {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/v1/runs/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        match status {
            StatusCode::OK if value["data"]["status"] == expected => return value,
            StatusCode::OK | StatusCode::SERVICE_UNAVAILABLE => {}
            status => panic!("unexpected GET Run status {status}: {value}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Run {run_id} did not reach {expected} through transient unavailability: {value}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn assert_terminal_matches_get(app: &Router, transcript: &Transcript) {
    let run = get_run(app, &transcript.run_id).await;
    assert_eq!(run["data"]["status"], "completed");
    assert_eq!(run["data"]["response_id"], transcript.response_id);
    assert_eq!(
        run["data"]["response_id"],
        transcript.terminal()["response"]["id"]
    );
    assert_eq!(
        transcript.terminal()["workflow"]["result"],
        run["data"]["output"]["data"]
    );
    assert_eq!(run["data"]["output"]["data"], "fixture answer");
}

fn object_keys(value: &Value) -> BTreeSet<&str> {
    value
        .as_object()
        .expect("terminal contract member must be an object")
        .keys()
        .map(String::as_str)
        .collect()
}

fn assert_terminal_union(
    transcript: &Transcript,
    event_type: &str,
    response_status: &str,
    workflow_keys: BTreeSet<&str>,
) {
    let terminal = transcript.terminal();
    assert_eq!(
        terminal["type"], event_type,
        "unexpected terminal envelope: {terminal}"
    );
    assert_eq!(terminal["response"]["status"], response_status);
    assert_eq!(terminal["response"]["id"], transcript.response_id);
    assert_eq!(terminal["response"]["object"], "response");
    assert_eq!(terminal["workflow"]["run_id"], transcript.run_id);
    assert_eq!(
        object_keys(terminal),
        BTreeSet::from(["response", "sequence_number", "type", "workflow"])
    );
    assert_eq!(
        object_keys(&terminal["response"]),
        BTreeSet::from(["error", "id", "object", "output", "status", "usage"])
    );
    assert_eq!(object_keys(&terminal["workflow"]), workflow_keys);
    assert!(terminal["workflow"]["tool_results"].is_array());
    assert!(terminal["workflow"]["retrievals"].is_array());
    assert!(matches!(
        terminal["workflow"]["usage_status"].as_str(),
        Some("complete" | "partial" | "unavailable")
    ));
}

#[tokio::test]
async fn closed_or_unread_http_clients_cancel_within_bounds_without_rewriting_terminal_authority() {
    let outbound_write_timeout = Duration::from_secs(1);
    let fixture =
        Fixture::start_with_timeouts(Duration::from_secs(10), outbound_write_timeout).await;

    let waiting_response = start_attached_response(
        &fixture.app,
        "response_stream_backpressure_wait",
        "request-unread-client-nonterminal",
        json!({"question": BACKPRESSURE_QUESTION}),
    )
    .await;
    let waiting_run_id = waiting_response.headers()["x-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let waiting = wait_for_run_status(
        &fixture.app,
        &waiting_run_id,
        "cancelled",
        Duration::from_secs(4),
    )
    .await;
    assert_eq!(waiting["data"]["status"], "cancelled");
    tokio::time::timeout(
        Duration::from_secs(1),
        to_bytes(waiting_response.into_body(), 1 << 20),
    )
    .await
    .expect("the unread nonterminal HTTP response did not close after its write deadline")
    .unwrap();

    let closed_response = start_attached_response(
        &fixture.app,
        "response_stream_wait",
        "request-closed-client-nonterminal",
        json!({}),
    )
    .await;
    let closed_run_id = closed_response.headers()["x-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    drop(closed_response);
    let closed = wait_for_run_status(
        &fixture.app,
        &closed_run_id,
        "cancelled",
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(closed["data"]["status"], "cancelled");

    // Keep the nonterminal cancellation checks fast, but give this branch a
    // separate transport deadline so durable completion wins before the
    // deliberately unread response becomes unwritable on a contended runner.
    let completed_outbound_write_timeout = Duration::from_secs(5);
    let completed_fixture =
        Fixture::start_with_timeouts(Duration::from_secs(10), completed_outbound_write_timeout)
            .await;
    let completed_response = start_attached_response(
        &completed_fixture.app,
        "response_stream_backpressure_complete",
        "request-unread-client-terminal",
        json!({"question": BACKPRESSURE_QUESTION}),
    )
    .await;
    let completed_run_id = completed_response.headers()["x-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let completed = wait_for_run_status(
        &completed_fixture.app,
        &completed_run_id,
        "completed",
        Duration::from_secs(8),
    )
    .await;
    assert_eq!(completed["data"]["output"]["data"], "x".repeat(40));
    tokio::time::sleep(completed_outbound_write_timeout + Duration::from_secs(1)).await;
    tokio::time::timeout(
        Duration::from_secs(1),
        to_bytes(completed_response.into_body(), 1 << 20),
    )
    .await
    .expect("the unread terminal HTTP response did not close after its write deadline")
    .unwrap();
    let recovered = get_run(&completed_fixture.app, &completed_run_id).await;
    assert_eq!(recovered["data"]["status"], "completed");
    assert_eq!(recovered["data"]["output"], completed["data"]["output"]);

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
    completed_fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_real_tcp_client_close_reaches_the_attached_cancellation_contract() {
    let fixture =
        Fixture::start_with_timeouts(Duration::from_secs(10), Duration::from_millis(250)).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = fixture.app.clone();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(
            b"POST /v1/agents/response_stream_wait/runs/stream HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\n{}",
        )
        .await
        .unwrap();
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut chunk = [0_u8; 1024];
        while !received.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = client.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0, "server closed before returning SSE headers");
            received.extend_from_slice(&chunk[..count]);
        }
    })
    .await
    .expect("real TCP client did not receive SSE headers");
    let headers = String::from_utf8_lossy(&received);
    let run_id = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("x-run-id")
                .then(|| value.trim().to_owned())
        })
        .expect("SSE response must identify the attached Run");

    drop(client);
    let cancelled =
        wait_for_run_status(&fixture.app, &run_id, "cancelled", Duration::from_secs(2)).await;
    assert_eq!(cancelled["data"]["status"], "cancelled");

    server.abort();
    let _ = server.await;
    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_real_tcp_client_that_stops_reading_hits_the_outbound_deadline_and_cancels() {
    let outbound_write_timeout = Duration::from_secs(1);
    let fixture =
        Fixture::start_with_timeouts(Duration::from_secs(10), outbound_write_timeout).await;

    // Keep the test finite and below the production live-response byte policy:
    // 2,048 tiny provider deltas carry only 2 KiB of final text while their
    // SSE envelopes provide enough transport volume to fill a deliberately
    // small test-listener send buffer and client receive buffer. Every frame,
    // the single item, and the Run remain comfortably below the configured
    // 4 KiB / 4 MiB / 16 MiB live limits.
    const {
        assert!(TCP_BACKPRESSURE_DELTA_BYTES < 4 * 1_024);
        assert!(TCP_BACKPRESSURE_DELTA_BYTES * TCP_BACKPRESSURE_EVENT_COUNT < 4 * 1_024 * 1_024);
    }
    let server_socket = tokio::net::TcpSocket::new_v4().unwrap();
    server_socket.set_reuseaddr(true).unwrap();
    server_socket.set_send_buffer_size(4 * 1_024).unwrap();
    server_socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = server_socket.listen(128).unwrap();
    let address = listener.local_addr().unwrap();
    let app = fixture.app.clone();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client_socket = tokio::net::TcpSocket::new_v4().unwrap();
    client_socket.set_recv_buffer_size(1_024).unwrap();
    let mut client = client_socket.connect(address).await.unwrap();
    let body = serde_json::to_vec(&json!({"question": TCP_BACKPRESSURE_QUESTION})).unwrap();
    let request_head = format!(
        "POST /v1/agents/response_stream_backpressure_wait/runs/stream HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    client.write_all(request_head.as_bytes()).await.unwrap();
    client.write_all(&body).await.unwrap();

    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut chunk = [0_u8; 512];
        while !received.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = client.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0, "server closed before returning SSE headers");
            received.extend_from_slice(&chunk[..count]);
            assert!(
                received.len() <= 16 * 1_024,
                "HTTP headers were not bounded"
            );
        }
    })
    .await
    .expect("real TCP unread client did not receive SSE headers");
    let headers = String::from_utf8_lossy(&received);
    let run_id = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("x-run-id")
                .then(|| value.trim().to_owned())
        })
        .expect("SSE response must identify the attached Run");

    // The socket deliberately remains open and is not read while the bounded
    // dispatcher deadline expires. Cancellation must win well before the
    // independent 10-second Run timeout.
    tokio::time::sleep(outbound_write_timeout.saturating_mul(3)).await;
    assert_eq!(
        fixture.counters.streaming_finished.load(Ordering::SeqCst),
        1,
        "the finite high-volume model stream must finish before transport cancellation"
    );

    // Reading resumes only after cancellation so the kernel can flush its
    // already-bounded backlog. `Connection: close` then makes dispatcher/body
    // closure observable as TCP EOF without allocating an unbounded buffer.
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut drained = 0_usize;
        let mut chunk = [0_u8; 4 * 1_024];
        loop {
            let count = client.read(&mut chunk).await.unwrap();
            if count == 0 {
                break;
            }
            drained += count;
            assert!(
                drained <= 4 * 1_024 * 1_024,
                "closed SSE transport exceeded the bounded drain allowance"
            );
        }
    })
    .await
    .expect("real TCP response did not reach EOF after its outbound write deadline");
    server.abort();
    let _ = server.await;
    let cancelled = wait_for_run_status_through_transient_unavailability(
        &fixture.app,
        &run_id,
        "cancelled",
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(cancelled["data"]["status"], "cancelled");
    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn terminal_lifecycle_matrix_is_closed_redacted_unique_and_immediately_eof() {
    let fixture = Fixture::start_with_run_timeout(Duration::from_secs(10)).await;

    let succeeded = attached_transcript_with_body(
        &fixture.app,
        agent_id(false, true),
        "request-terminal-succeeded",
        json!({"question": "show the response"}),
    )
    .await;
    assert_terminal_union(
        &succeeded,
        "response.completed",
        "completed",
        BTreeSet::from([
            "result",
            "retrievals",
            "run_id",
            "tool_results",
            "usage_status",
        ]),
    );
    assert_eq!(succeeded.terminal()["workflow"]["result"], "fixture answer");
    assert!(succeeded.terminal()["response"]["error"].is_null());

    let failed = attached_transcript_with_body(
        &fixture.app,
        agent_id(false, true),
        "request-terminal-failed",
        json!({"question": FAILURE_QUESTION}),
    )
    .await;
    assert_terminal_union(
        &failed,
        "response.failed",
        "failed",
        BTreeSet::from([
            "error",
            "retrievals",
            "run_id",
            "tool_results",
            "usage_status",
        ]),
    );
    assert_eq!(
        failed.terminal()["workflow"]["error"]["message"],
        "run failed"
    );
    assert_eq!(
        failed.terminal()["response"]["error"]["code"],
        failed.terminal()["workflow"]["error"]["code"]
    );
    assert_eq!(
        failed.terminal()["response"]["error"]["message"],
        "run failed"
    );
    assert!(failed.terminal()["response"]["error"]["param"].is_null());
    assert!(!failed.terminal().to_string().contains(FAILURE_QUESTION));

    let timeout_fixture = Fixture::start_with_run_timeout(Duration::from_millis(500)).await;
    let timed_out = attached_transcript_with_body(
        &timeout_fixture.app,
        "response_stream_wait",
        "request-terminal-timed-out",
        json!({}),
    )
    .await;
    assert_terminal_union(
        &timed_out,
        "workflow.response.timed_out",
        "failed",
        BTreeSet::from([
            "error",
            "retrievals",
            "run_id",
            "tool_results",
            "usage_status",
        ]),
    );
    assert_eq!(
        timed_out.terminal()["workflow"]["error"]["code"],
        "RUN_TIMEOUT"
    );
    assert_eq!(
        timed_out.terminal()["response"]["error"]["code"],
        "RUN_TIMEOUT"
    );

    let cancelled_response = start_attached_response(
        &fixture.app,
        "response_stream_wait",
        "request-terminal-cancelled",
        json!({}),
    )
    .await;
    let cancelled_run_id = cancelled_response.headers()["x-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        fixture
            .service
            .cancel(&cancelled_run_id)
            .await
            .unwrap()
            .status(),
        RunStatus::Cancelled
    );
    let cancelled = consume_attached_response(cancelled_response).await;
    assert_terminal_union(
        &cancelled,
        "workflow.response.cancelled",
        "cancelled",
        BTreeSet::from([
            "reason",
            "retrievals",
            "run_id",
            "tool_results",
            "usage_status",
        ]),
    );
    assert_eq!(cancelled.terminal()["workflow"]["reason"], "cancelled");
    assert!(cancelled.terminal()["response"]["error"].is_null());

    let interrupted_response = start_attached_response(
        &fixture.app,
        "response_stream_wait",
        "request-terminal-interrupted",
        json!({}),
    )
    .await;
    let interrupted_run_id = interrupted_response.headers()["x-run-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        fixture
            .service
            .interrupt(&interrupted_run_id)
            .await
            .unwrap()
            .status(),
        RunStatus::Interrupted
    );
    assert_eq!(
        fixture
            .service
            .cancel(&interrupted_run_id)
            .await
            .unwrap()
            .status(),
        RunStatus::Interrupted,
        "a later cancellation must not rewrite the winning terminal outcome"
    );
    let interrupted = consume_attached_response(interrupted_response).await;
    assert_terminal_union(
        &interrupted,
        "workflow.response.interrupted",
        "incomplete",
        BTreeSet::from([
            "reason",
            "retrievals",
            "run_id",
            "tool_results",
            "usage_status",
        ]),
    );
    assert_eq!(interrupted.terminal()["workflow"]["reason"], "interrupted");
    assert!(interrupted.terminal()["response"]["error"].is_null());

    for (transcript, get_status) in [
        (&succeeded, "completed"),
        (&failed, "failed"),
        (&cancelled, "cancelled"),
        (&interrupted, "interrupted"),
    ] {
        let run = get_run(&fixture.app, &transcript.run_id).await;
        assert_eq!(run["data"]["status"], get_status);
        assert_eq!(run["data"]["response_id"], transcript.response_id);
    }
    let timed_out_run = get_run(&timeout_fixture.app, &timed_out.run_id).await;
    assert_eq!(timed_out_run["data"]["status"], "failed");
    assert_eq!(timed_out_run["data"]["response_id"], timed_out.response_id);

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
    timeout_fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn checked_in_medical_agent_streams_three_initial_items_and_one_follow_up_item() {
    // This test asserts the complete publication lifecycle for every medical
    // item. Give the finite workflow enough broker capacity and terminal
    // barrier time that unrelated workflow observations cannot turn the
    // assertion into a bounded-loss test under a contended CI scheduler;
    // dedicated tests below cover gaps.
    let broker: Arc<dyn LiveResponseBroker> =
        Arc::new(InMemoryLiveResponseBroker::new(512, 64).unwrap());
    let fixture = Fixture::start_with_broker_and_response_timeouts(
        Duration::from_secs(10),
        Duration::from_secs(2),
        Duration::from_secs(5),
        broker,
    )
    .await;
    let initial_response = start_attached_response(
        &fixture.app,
        "medical_report_interpreter",
        "request-medical-initial",
        json!({
            "report_text": "白细胞 12×10^9/L，参考范围 4-10×10^9/L。",
            "messages": [],
            "question": "请解读这份报告"
        }),
    )
    .await;
    let initial =
        consume_attached_response_asserting_live_delta(initial_response, &fixture.counters).await;
    let initial_items = initial
        .values
        .iter()
        .filter(|event| event["type"] == "response.output_item.added")
        .collect::<Vec<_>>();
    assert_eq!(initial_items.len(), 3);
    assert_eq!(
        initial_items
            .iter()
            .map(|event| event["output_index"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(initial.terminal()["workflow"]["result"]["mode"], "initial");
    assert_eq!(
        initial.terminal()["workflow"]["result"]["answer"],
        "fixture answer\n\nfixture answer\n\nfixture answer"
    );
    let initial_run = get_run(&fixture.app, &initial.run_id).await;
    assert_eq!(
        initial_run["data"]["output"]["data"],
        initial.terminal()["workflow"]["result"]
    );
    assert_eq!(initial_run["data"]["response_id"], initial.response_id);

    let follow_up_response = start_attached_response(
        &fixture.app,
        "medical_report_interpreter",
        "request-medical-follow-up",
        json!({
            "report_text": "白细胞 12×10^9/L，参考范围 4-10×10^9/L。",
            "messages": [{
                "role": "user",
                "content": [{"text": "刚才的白细胞结果偏高"}]
            }],
            "question": "需要马上去急诊吗？"
        }),
    )
    .await;
    let follow_up =
        consume_attached_response_asserting_live_delta(follow_up_response, &fixture.counters).await;
    assert_eq!(
        follow_up
            .values
            .iter()
            .filter(|event| event["type"] == "response.output_item.added")
            .count(),
        1
    );
    assert_eq!(
        follow_up.terminal()["workflow"]["result"],
        json!({
            "mode": "follow_up",
            "answer": "fixture answer",
            "abnormal_indicators": "",
            "comprehensive_interpretation": "",
            "health_advice": ""
        })
    );
    let follow_up_run = get_run(&fixture.app, &follow_up.run_id).await;
    assert_eq!(
        follow_up_run["data"]["output"]["data"],
        follow_up.terminal()["workflow"]["result"]
    );
    assert_eq!(follow_up_run["data"]["response_id"], follow_up.response_id);

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn first_public_delta_arrives_before_the_model_call_finishes() {
    let fixture = Fixture::start().await;
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/v1/agents/response_stream_tt/runs/stream")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-request-id", "request-live-before-finish")
                .body(Body::from(r#"{"question":"show the response"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let mut observed = String::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(chunk) = body.next().await {
            let chunk = chunk.unwrap();
            observed.push_str(std::str::from_utf8(&chunk).unwrap());
            if observed.contains("event: response.output_text.delta") {
                return;
            }
        }
        panic!("response body reached EOF before the first text delta: {observed}");
    })
    .await
    .expect("the first text delta was not delivered promptly");
    assert_eq!(
        fixture.counters.streaming_finished.load(Ordering::SeqCst),
        0,
        "the public delta must be observable before Provider completion"
    );

    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(chunk) = body.next().await {
            let chunk = chunk.unwrap();
            observed.push_str(std::str::from_utf8(&chunk).unwrap());
        }
    })
    .await
    .expect("the response did not reach terminal EOF");
    assert_eq!(
        fixture.counters.streaming_finished.load(Ordering::SeqCst),
        1
    );
    assert!(observed.contains("event: response.completed"));

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn parallel_public_llm_deltas_interleave_without_crossing_item_identity() {
    let fixture = Fixture::start().await;
    let transcript = attached_transcript_with_body(
        &fixture.app,
        "response_stream_parallel_isolation",
        "request-parallel-stream-isolation",
        json!({}),
    )
    .await;

    assert_eq!(
        fixture.isolation.parallel_max_active.load(Ordering::SeqCst),
        2,
        "the fixture rendezvous proves both Provider calls were in flight together"
    );
    assert_eq!(transcript.terminal()["type"], "response.completed");

    let deltas = transcript
        .values
        .iter()
        .filter(|event| event["type"] == "response.output_text.delta")
        .collect::<Vec<_>>();
    assert_eq!(
        deltas
            .iter()
            .map(|event| event["delta"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["left-", "right-", "complete", "complete"],
        "the synchronized fixture must put both items on the wire before either can finish"
    );
    let delta_item_ids = deltas
        .iter()
        .map(|event| event["item_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_ne!(delta_item_ids[0], delta_item_ids[1]);
    assert_eq!(delta_item_ids[0], delta_item_ids[2]);
    assert_eq!(delta_item_ids[1], delta_item_ids[3]);

    let (items, text) = reconstructed_public_text_items(&transcript);
    assert_eq!(items.len(), 2);
    assert_eq!(
        items.values().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1])
    );
    assert_eq!(
        text.values().map(String::as_str).collect::<BTreeSet<_>>(),
        BTreeSet::from(["left-complete", "right-complete"])
    );
    assert_terminal_output_matches_reconstructed_text(&transcript, &items, &text);
    assert_eq!(
        transcript.terminal()["workflow"]["result"],
        json!({"left": "left-complete", "right": "right-complete"})
    );

    let snapshot = fixture
        .repository
        .load_response_snapshot(&RunId::new(transcript.run_id.clone()).unwrap())
        .await
        .unwrap()
        .unwrap();
    let manifest = snapshot.public_item_manifest().as_array().unwrap();
    assert_eq!(manifest.len(), 2);
    assert_eq!(
        manifest
            .iter()
            .map(|item| item["item_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>(),
        items.keys().map(String::as_str).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        manifest
            .iter()
            .map(|item| item["output_index"].as_u64().unwrap())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1])
    );
    assert_eq!(
        manifest
            .iter()
            .map(|item| item["node_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["parallel_left_answer", "parallel_right_answer"])
    );

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn workflow_loop_public_llm_occurrences_append_distinct_items_without_mixing_deltas() {
    let fixture = Fixture::start().await;
    let transcript = attached_transcript_with_body(
        &fixture.app,
        "response_stream_loop_isolation",
        "request-loop-stream-isolation",
        json!({"seed": LOOP_INITIAL_STATE}),
    )
    .await;

    assert_eq!(fixture.isolation.loop_calls.load(Ordering::SeqCst), 2);
    assert_eq!(transcript.terminal()["type"], "response.failed");

    let deltas = transcript
        .values
        .iter()
        .filter(|event| event["type"] == "response.output_text.delta")
        .collect::<Vec<_>>();
    assert_eq!(
        deltas
            .iter()
            .map(|event| event["delta"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["loop-", "one", "loop-", "two"]
    );
    let delta_item_ids = deltas
        .iter()
        .map(|event| event["item_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(delta_item_ids[0], delta_item_ids[1]);
    assert_eq!(delta_item_ids[2], delta_item_ids[3]);
    assert_ne!(delta_item_ids[0], delta_item_ids[2]);

    let (items, text) = reconstructed_public_text_items(&transcript);
    assert_eq!(items.len(), 2);
    assert_eq!(items.get(delta_item_ids[0]), Some(&0));
    assert_eq!(items.get(delta_item_ids[2]), Some(&1));
    assert_eq!(text.get(delta_item_ids[0]).unwrap(), LOOP_FIRST_STATE);
    assert_eq!(text.get(delta_item_ids[2]).unwrap(), LOOP_SECOND_STATE);
    assert_terminal_output_matches_reconstructed_text(&transcript, &items, &text);

    let snapshot = fixture
        .repository
        .load_response_snapshot(&RunId::new(transcript.run_id.clone()).unwrap())
        .await
        .unwrap()
        .unwrap();
    let manifest = snapshot.public_item_manifest().as_array().unwrap();
    assert_eq!(manifest.len(), 2);
    assert_eq!(manifest[0]["item_id"], delta_item_ids[0]);
    assert_eq!(manifest[1]["item_id"], delta_item_ids[2]);
    assert_eq!(manifest[0]["output_index"], 0);
    assert_eq!(manifest[1]["output_index"], 1);
    assert_eq!(manifest[0]["node_id"], "loop_answer");
    assert_eq!(manifest[1]["node_id"], "loop_answer");
    assert_eq!(manifest[0]["attempt_no"], 1);
    assert_eq!(manifest[1]["attempt_no"], 1);
    assert_ne!(manifest[0]["activation_id"], manifest[1]["activation_id"]);
    assert_eq!(manifest[0]["status"], "completed");
    assert_eq!(manifest[1]["status"], "completed");

    let run = get_run(&fixture.app, &transcript.run_id).await;
    assert_eq!(run["data"]["status"], "failed");
    assert_eq!(run["data"]["response_id"], transcript.response_id);

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn terminal_barrier_times_out_to_an_unknown_tail_gap_after_the_last_live_watermark() {
    let broker: Arc<dyn LiveResponseBroker> = Arc::new(DropSealAndCloseBroker {
        inner: InMemoryLiveResponseBroker::new(64, 16).unwrap(),
    });
    let fixture = Fixture::start_with_broker_and_timeouts(
        Duration::from_secs(10),
        Duration::from_secs(2),
        broker,
    )
    .await;

    let transcript = attached_transcript(&fixture.app, "response_stream_tt").await;
    let gap_index = transcript
        .values
        .iter()
        .position(|event| event["type"] == "workflow.stream.gap")
        .expect("a missing seal must be calibrated with an unknown-tail gap");
    let gap = &transcript.values[gap_index];
    assert_eq!(gap_index + 1, transcript.values.len() - 1);
    assert_eq!(gap["unknown_tail"], true);
    assert!(gap["missing_to"].is_null());
    assert_eq!(gap["action"], "discard_provisional_item");
    assert!(
        gap["missing_from"].as_u64().unwrap() > 0,
        "the dispatcher must continue after its observed live watermark, not restart at zero"
    );
    assert_eq!(transcript.terminal()["type"], "response.completed");
    assert_terminal_matches_get(&fixture.app, &transcript).await;

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn public_sse_protocol_covers_all_stream_publish_combinations() {
    let fixture = Fixture::start().await;

    let streaming_public = attached_transcript(&fixture.app, agent_id(true, true)).await;
    assert_eq!(
        streaming_public.event_types(),
        vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ],
        "unexpected stream: {}",
        serde_json::to_string_pretty(&streaming_public.values).unwrap()
    );
    assert_eq!(streaming_public.deltas(), vec!["fixture ", "answer"]);
    assert_terminal_matches_get(&fixture.app, &streaming_public).await;

    let streaming_private = attached_transcript(&fixture.app, agent_id(true, false)).await;
    assert_eq!(
        streaming_private.event_types(),
        vec![
            "response.created",
            "response.in_progress",
            "response.completed"
        ]
    );
    assert!(streaming_private.deltas().is_empty());
    assert_eq!(
        streaming_private.terminal()["response"]["output"],
        json!([])
    );
    assert_terminal_matches_get(&fixture.app, &streaming_private).await;

    let complete_public = attached_transcript(&fixture.app, agent_id(false, true)).await;
    assert_eq!(
        complete_public.event_types(),
        vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );
    assert_eq!(complete_public.deltas(), vec!["fixture answer"]);
    assert_terminal_matches_get(&fixture.app, &complete_public).await;

    let complete_private = attached_transcript(&fixture.app, agent_id(false, false)).await;
    assert_eq!(
        complete_private.event_types(),
        vec![
            "response.created",
            "response.in_progress",
            "response.completed"
        ]
    );
    assert!(complete_private.deltas().is_empty());
    assert_eq!(complete_private.terminal()["response"]["output"], json!([]));
    assert_terminal_matches_get(&fixture.app, &complete_private).await;

    assert_eq!(fixture.counters.streaming.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.counters.complete.load(Ordering::SeqCst), 2);
    for transcript in [
        streaming_public,
        streaming_private,
        complete_public,
        complete_private,
    ] {
        assert_eq!(
            transcript.response_id,
            transcript.terminal()["response"]["id"]
        );
        assert_eq!(
            transcript.terminal()["response"]["usage"]["input_tokens"],
            3
        );
        assert_eq!(
            transcript.terminal()["response"]["usage"]["output_tokens"],
            2
        );
        assert_eq!(
            transcript.terminal()["response"]["usage"]["total_tokens"],
            5
        );
        assert_eq!(
            transcript.terminal()["workflow"]["usage_status"],
            "complete"
        );
    }

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn artifact_read_route_requires_auth_and_hides_handle_and_run_misses() {
    let fixture = Fixture::start().await;
    let transcript = attached_transcript(&fixture.app, agent_id(false, false)).await;
    let protected = build_router(ApiState {
        service: fixture.service.clone(),
        auth: ApiAuth::bearer_token("artifact-reader-token"),
        sse_keep_alive_interval: Duration::from_secs(30),
        readiness_probe_timeout: Duration::from_secs(1),
    });

    let unauthorized = protected
        .clone()
        .oneshot(
            Request::get(format!(
                "/v1/runs/{}/artifacts/artifact_missing",
                transcript.run_id
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let mut misses = Vec::new();
    for uri in [
        format!("/v1/runs/{}/artifacts/artifact_missing", transcript.run_id),
        "/v1/runs/run_missing/artifacts/artifact_missing".to_owned(),
        format!(
            "/v1/runs/{}/artifacts/invalid%20artifact",
            transcript.run_id
        ),
    ] {
        let response = protected
            .clone()
            .oneshot(
                Request::get(uri)
                    .header(header::AUTHORIZATION, "Bearer artifact-reader-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        misses.push(
            serde_json::from_slice::<Value>(
                &to_bytes(response.into_body(), 1 << 20).await.unwrap(),
            )
            .unwrap(),
        );
    }
    assert!(misses.iter().all(|body| body
        == &json!({
            "code": "ARTIFACT_NOT_FOUND",
            "message": "artifact not found",
            "data": {}
        })));

    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
}
