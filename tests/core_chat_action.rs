use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::stream;
use handlebars::Handlebars;
use insight_agent_platform::{
    dsl::{
        compiled::{CompiledNode, NodeCompilation, NodeControl, NodeOutcome, NodeTransition},
        compiler::CompileContext,
        EmitPolicy,
    },
    nodes::{
        action::ActionNode,
        chat::ChatNode,
        registry::{NodeExecutor, NodeType},
    },
    resources::{
        actions::{Action, ActionContext, ActionDescriptor, ActionRegistry},
        models::{
            ChatChunk, ChatModel, ChatRequest, ChatRole, ChatStream, ModelCapability, ModelRegistry,
        },
    },
    runtime::{stop_pair, ExecutionControl, RunContext, RunError, RunMetadata, StopReason},
};
use serde_json::{json, Value};
use tokio::sync::Notify;

#[derive(Clone)]
struct RecordingModel {
    requests: Arc<Mutex<Vec<ChatRequest>>>,
    capabilities: BTreeSet<ModelCapability>,
}

impl RecordingModel {
    fn text_only() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            capabilities: BTreeSet::new(),
        }
    }

    fn vision() -> Self {
        Self {
            capabilities: BTreeSet::from([ModelCapability::Vision]),
            ..Self::text_only()
        }
    }
}

impl fmt::Debug for RecordingModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("RecordingModel").finish()
    }
}

#[async_trait]
impl ChatModel for RecordingModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        self.capabilities.clone()
    }

    fn validate_parameters(
        &self,
        parameters: &Value,
    ) -> Result<(), insight_agent_platform::dsl::CompileError> {
        if parameters.get("invalid") == Some(&Value::Bool(true)) {
            return Err(insight_agent_platform::dsl::CompileError::new(
                "MODEL_PARAMETERS_INVALID",
                "invalid recording-model parameters",
            ));
        }
        Ok(())
    }

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        self.requests.lock().unwrap().push(request);
        Ok(Box::pin(stream::iter(vec![
            Ok(ChatChunk {
                text: "Hel".to_string(),
                finish_reason: None,
                usage: None,
            }),
            Ok(ChatChunk {
                text: "lo".to_string(),
                finish_reason: Some("stop".to_string()),
                usage: Some(json!({"output_tokens":2})),
            }),
        ])))
    }
}

#[derive(Clone)]
struct LimitModel {
    chunks: Vec<ChatChunk>,
    max_accumulated_text_bytes: usize,
}

impl fmt::Debug for LimitModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("LimitModel").finish()
    }
}

#[async_trait]
impl ChatModel for LimitModel {
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

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        assert_eq!(request.messages.len(), 1);
        Ok(Box::pin(stream::iter(
            self.chunks.clone().into_iter().map(Ok),
        )))
    }
}

fn context(input: Value) -> RunContext {
    RunContext::new(
        RunMetadata {
            run_id: "run_test".to_string(),
            request_id: "req_test".to_string(),
            agent_id: "agent_test".to_string(),
            agent_version: "sha256:test".to_string(),
            started_at: chrono::Utc::now(),
        },
        input,
    )
}

fn compiled_node(
    id: &str,
    kind: &str,
    emit: EmitPolicy,
    compilation: NodeCompilation,
) -> CompiledNode {
    CompiledNode {
        id: id.to_string(),
        kind: kind.to_string(),
        next: Some("done".to_string()),
        emit,
        timeout: Duration::from_secs(1),
        body: compilation.body,
        edges: compilation.edges,
        references: compilation.references,
        terminal: compilation.terminal,
        control: NodeControl::Ordinary,
    }
}

fn capturing_control() -> (ExecutionControl, Arc<Mutex<Vec<String>>>) {
    let emitted = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&emitted);
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), move |content| {
        let captured = Arc::clone(&captured);
        async move {
            captured.lock().unwrap().push(content);
            Ok(())
        }
    });
    (control, emitted)
}

fn compile_chat_with_parts(
    parts: Value,
) -> (
    CompiledNode,
    Arc<Handlebars<'static>>,
    Arc<Mutex<Vec<ChatRequest>>>,
) {
    let model = RecordingModel::vision();
    let requests = Arc::clone(&model.requests);
    let mut models = ModelRegistry::default();
    models.register("primary", model).unwrap();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ChatNode
        .compile(
            "answer",
            json!({
                "model":"primary",
                "messages":[{"role":"user", "content":parts}]
            }),
            &mut compile_context,
        )
        .unwrap();
    (
        compiled_node("answer", "core.chat", EmitPolicy::None, compilation),
        Arc::new(compile_context.into_templates()),
        requests,
    )
}

#[tokio::test]
async fn optional_image_parts_omit_missing_empty_and_blank_values() {
    for input in [
        json!({}),
        json!({"image_url":""}),
        json!({"image_url":"   "}),
    ] {
        let (node, templates, requests) = compile_chat_with_parts(json!([
            {"type":"text", "text":"question"},
            {"type":"image_url", "optional":true,
             "image_url":{"url":"{{ input.image_url }}"}}
        ]));
        let context = context(input).with_templates(templates);
        let (control, _) = capturing_control();
        ChatNode.execute(&node, &context, &control).await.unwrap();
        let request = requests.lock().unwrap().pop().unwrap();
        assert_eq!(request.messages[0].text(), Some("question"));
        assert!(request.messages[0].image_urls().is_empty());
    }
}

#[tokio::test]
async fn optional_image_parts_preserve_non_blank_urls() {
    for url in [
        "http://example.test/report.png",
        "https://example.test/report.png",
        "data:image/png;base64,AA==",
    ] {
        let (node, templates, requests) = compile_chat_with_parts(json!([
            {"type":"text", "text":"question"},
            {"type":"image_url", "optional":true,
             "image_url":{"url":"{{ input.image_url }}"}}
        ]));
        let context = context(json!({"image_url":url})).with_templates(templates);
        let (control, _) = capturing_control();
        ChatNode.execute(&node, &context, &control).await.unwrap();
        let request = requests.lock().unwrap().pop().unwrap();
        assert_eq!(request.messages[0].image_urls(), vec![url]);
    }
}

#[tokio::test]
async fn required_image_parts_still_fail_for_missing_values() {
    let (node, templates, requests) = compile_chat_with_parts(json!([
        {"type":"text", "text":"question"},
        {"type":"image_url", "image_url":{"url":"{{ input.image_url }}"}}
    ]));
    let context = context(json!({})).with_templates(templates);
    let (control, _) = capturing_control();
    let error = ChatNode
        .execute(&node, &context, &control)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "TEMPLATE_RENDER_FAILED");
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn optional_image_parts_preserve_non_missing_render_errors() {
    let (node, templates, requests) = compile_chat_with_parts(json!([
        {"type":"text", "text":"question"},
        {"type":"image_url", "optional":true,
         "image_url":{"url":"{{#if}}{{/if}}"}}
    ]));
    let context = context(json!({})).with_templates(templates);
    let (control, _) = capturing_control();
    let error = ChatNode
        .execute(&node, &context, &control)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "TEMPLATE_RENDER_FAILED");
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn optional_image_parts_reject_messages_emptied_by_filtering() {
    let (node, templates, requests) = compile_chat_with_parts(json!([
        {"type":"image_url", "optional":true,
         "image_url":{"url":"{{ input.image_url }}"}}
    ]));
    let context = context(json!({})).with_templates(templates);
    let (control, _) = capturing_control();
    let error = ChatNode
        .execute(&node, &context, &control)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "CHAT_CONTENT_PARTS_EMPTY");
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn chat_renders_multimodal_messages_streams_and_normalizes_output() {
    let model = RecordingModel::vision();
    let requests = Arc::clone(&model.requests);
    let mut models = ModelRegistry::default();
    models.register("primary", model).unwrap();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ChatNode
        .compile(
            "answer",
            json!({
                "model": "primary",
                "messages": [
                    {"role":"system", "content":"Be concise."},
                    {"role":"user", "content":[
                        {"type":"text", "text":"{{ input.question }}"},
                        {"type":"image_url", "image_url":{"url":"{{ input.image_url }}"}}
                    ]}
                ],
                "parameters": {"temperature":0}
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("answer", "core.chat", EmitPolicy::Content, compilation);
    let context = context(json!({
        "question":"A&B",
        "image_url":"https://example.test/report.png"
    }))
    .with_templates(Arc::new(compile_context.into_templates()));
    let (control, emitted) = capturing_control();

    let outcome = ChatNode.execute(&node, &context, &control).await.unwrap();

    let request = requests.lock().unwrap().pop().unwrap();
    assert_eq!(request.messages[0].role, ChatRole::System);
    assert_eq!(request.messages[1].text(), Some("A&B"));
    assert_eq!(
        request.messages[1].image_urls(),
        vec!["https://example.test/report.png"]
    );
    assert_eq!(request.parameters, json!({"temperature":0}));
    assert_eq!(
        outcome,
        NodeOutcome {
            output: json!({
                "text":"Hello",
                "finish_reason":"stop",
                "usage":{"output_tokens":2}
            }),
            transition: NodeTransition::Next,
        }
    );
    assert_eq!(*emitted.lock().unwrap(), vec!["Hel", "lo"]);
}

#[tokio::test]
async fn chat_allows_accumulated_text_at_exact_model_limit() {
    let mut models = ModelRegistry::default();
    models
        .register(
            "limited",
            LimitModel {
                chunks: vec![
                    ChatChunk {
                        text: "ab".to_string(),
                        finish_reason: None,
                        usage: None,
                    },
                    ChatChunk {
                        text: "c".to_string(),
                        finish_reason: Some("stop".to_string()),
                        usage: None,
                    },
                ],
                max_accumulated_text_bytes: 3,
            },
        )
        .unwrap();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ChatNode
        .compile(
            "answer",
            json!({
                "model":"limited",
                "messages":[{"role":"user", "content":"Hi"}]
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("answer", "core.chat", EmitPolicy::Content, compilation);
    let context = context(json!({})).with_templates(Arc::new(compile_context.into_templates()));
    let (control, emitted) = capturing_control();

    let outcome = ChatNode.execute(&node, &context, &control).await.unwrap();

    assert_eq!(outcome.output["text"], "abc");
    assert_eq!(
        *emitted.lock().unwrap(),
        vec!["ab".to_string(), "c".to_string()]
    );
}

#[tokio::test]
async fn chat_rejects_accumulated_text_before_appending_or_emitting_over_limit_chunk() {
    const OVER_LIMIT_SECRET: &str = "accumulated-text-secret";
    let mut models = ModelRegistry::default();
    models
        .register(
            "limited",
            LimitModel {
                chunks: vec![
                    ChatChunk {
                        text: "ok".to_string(),
                        finish_reason: None,
                        usage: None,
                    },
                    ChatChunk {
                        text: OVER_LIMIT_SECRET.to_string(),
                        finish_reason: Some("stop".to_string()),
                        usage: None,
                    },
                ],
                max_accumulated_text_bytes: 2,
            },
        )
        .unwrap();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ChatNode
        .compile(
            "answer",
            json!({
                "model":"limited",
                "messages":[{"role":"user", "content":"Hi"}]
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("answer", "core.chat", EmitPolicy::Content, compilation);
    let context = context(json!({})).with_templates(Arc::new(compile_context.into_templates()));
    let (control, emitted) = capturing_control();

    let error = ChatNode
        .execute(&node, &context, &control)
        .await
        .unwrap_err();

    assert_eq!(error.code(), "MODEL_RESPONSE_TOO_LARGE");
    assert_eq!(
        error.message(),
        "chat provider response exceeded the configured size limit"
    );
    assert!(!format!("{error:?} {error}").contains(OVER_LIMIT_SECRET));
    assert_eq!(*emitted.lock().unwrap(), vec!["ok".to_string()]);
}

#[test]
fn chat_rejects_unknown_models_invalid_parameters_messages_roles_and_vision() {
    let actions = ActionRegistry::default();

    let models = ModelRegistry::default();
    let mut context = CompileContext::new(&models, &actions);
    assert_compile_error(
        ChatNode.compile(
            "chat",
            json!({"model":"missing", "messages":[{"role":"user", "content":"hi"}]}),
            &mut context,
        ),
        "MODEL_NOT_FOUND",
    );

    let mut models = ModelRegistry::default();
    models
        .register("text", RecordingModel::text_only())
        .unwrap();
    let mut context = CompileContext::new(&models, &actions);
    assert_compile_error(
        ChatNode.compile(
            "chat",
            json!({
                "model":"text",
                "messages":[{"role":"user", "content":"hi"}],
                "parameters":{"invalid":true}
            }),
            &mut context,
        ),
        "MODEL_PARAMETERS_INVALID",
    );
    assert_compile_error(
        ChatNode.compile("chat", json!({"model":"text", "messages":[]}), &mut context),
        "CHAT_MESSAGES_REQUIRED",
    );
    assert_compile_error(
        ChatNode.compile(
            "chat",
            json!({"model":"text", "messages":[{"role":"tool", "content":"hi"}]}),
            &mut context,
        ),
        "NODE_CONFIG_INVALID",
    );
    assert_compile_error(
        ChatNode.compile(
            "chat",
            json!({
                "model":"text",
                "messages":[{"role":"user", "content":[
                    {"type":"image_url", "image_url":{"url":"https://example.test/a.png"}}
                ]}]
            }),
            &mut context,
        ),
        "MODEL_CAPABILITY_REQUIRED",
    );
    assert_compile_error(
        ChatNode.compile(
            "chat",
            json!({"model":"text", "messages":[{"role":"user", "content":[
                {"type":"image_url", "optional":true,
                 "image_url":{"url":"{{ input.image_url }}"}}
            ]}]}),
            &mut context,
        ),
        "MODEL_CAPABILITY_REQUIRED",
    );
    assert_compile_error(
        ChatNode.compile(
            "chat",
            json!({"model":"text", "messages":[{"role":"user", "content":[
                {"type":"text", "optional":true, "text":"hello"}
            ]}]}),
            &mut context,
        ),
        "NODE_CONFIG_INVALID",
    );
}

fn assert_compile_error(
    result: Result<NodeCompilation, insight_agent_platform::dsl::CompileError>,
    expected_code: &str,
) {
    let error = result.err().expect("node compilation must fail");
    assert_eq!(error.code(), expected_code, "{error}");
}

#[derive(Clone)]
struct EchoAction {
    calls: Arc<Mutex<Vec<Value>>>,
    streams_content: bool,
    invalid_output: bool,
}

#[async_trait]
impl Action for EchoAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            name: if self.invalid_output {
                "bad_echo"
            } else {
                "echo"
            },
            input_schema: json!({
                "type":"object",
                "required":["payload"],
                "properties":{"payload":{"type":"object"}},
                "additionalProperties":false
            }),
            output_schema: json!({
                "type":"object",
                "required":["echoed"],
                "properties":{"echoed":{"type":"object"}},
                "additionalProperties":false
            }),
            idempotent: true,
            streams_content: self.streams_content,
        }
    }

    async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
        self.calls.lock().unwrap().push(input.clone());
        if self.streams_content {
            context.control.emit_content("one").await?;
            context.control.emit_content("two").await?;
        }
        if self.invalid_output {
            Ok(json!({"echoed":input["payload"]["secret"].clone()}))
        } else {
            Ok(json!({"echoed":input["payload"].clone()}))
        }
    }
}

#[test]
fn action_compile_validates_literal_input_against_registered_schema() {
    const STATIC_SECRET: &str = "static-action-input-never-expose";

    let mut actions = ActionRegistry::default();
    actions
        .register(EchoAction {
            calls: Arc::new(Mutex::new(Vec::new())),
            streams_content: false,
            invalid_output: false,
        })
        .unwrap();
    let models = ModelRegistry::default();
    let mut context = CompileContext::new(&models, &actions);

    let error = ActionNode
        .compile(
            "echo",
            json!({
                "action": "echo",
                "input": {"payload": STATIC_SECRET}
            }),
            &mut context,
        )
        .err()
        .expect("static schema-invalid action input must fail compilation");

    assert_eq!(error.code(), "ACTION_INPUT_INVALID");
    assert_eq!(error.to_string(), "action input validation failed");
    assert!(!format!("{error:?} {error}").contains(STATIC_SECRET));
}

#[test]
fn action_compile_allows_valid_literal_and_dynamic_inputs() {
    let mut actions = ActionRegistry::default();
    actions
        .register(EchoAction {
            calls: Arc::new(Mutex::new(Vec::new())),
            streams_content: false,
            invalid_output: false,
        })
        .unwrap();
    let models = ModelRegistry::default();

    let mut literal_context = CompileContext::new(&models, &actions);
    ActionNode
        .compile(
            "literal",
            json!({
                "action": "echo",
                "input": {"payload": {"text": "static"}}
            }),
            &mut literal_context,
        )
        .unwrap();

    let mut dynamic_context = CompileContext::new(&models, &actions);
    ActionNode
        .compile(
            "dynamic",
            json!({
                "action": "echo",
                "input": {"payload": "{{ input.payload }}"}
            }),
            &mut dynamic_context,
        )
        .unwrap();
}

#[tokio::test]
async fn action_renders_recursive_input_validates_and_returns_output_unchanged() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut actions = ActionRegistry::default();
    actions
        .register(EchoAction {
            calls: Arc::clone(&calls),
            streams_content: false,
            invalid_output: false,
        })
        .unwrap();
    let models = ModelRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ActionNode
        .compile(
            "echo",
            json!({
                "action":"echo",
                "input":{"payload":{"text":"{{ input.text }}", "items":["{{ input.text }}", 2]}}
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("echo", "core.action", EmitPolicy::None, compilation);
    let context =
        context(json!({"text":"A&B"})).with_templates(Arc::new(compile_context.into_templates()));
    let (control, _) = capturing_control();

    let outcome = ActionNode.execute(&node, &context, &control).await.unwrap();

    let expected = json!({"payload":{"text":"A&B", "items":["A&B", 2]}});
    assert_eq!(*calls.lock().unwrap(), vec![expected.clone()]);
    assert_eq!(
        outcome.output,
        json!({"echoed":expected["payload"].clone()})
    );
    assert_eq!(outcome.transition, NodeTransition::Next);
}

#[tokio::test]
async fn action_validation_errors_are_fixed_and_instance_free() {
    const INPUT_SECRET: &str = "rendered-input-never-expose";
    const OUTPUT_SECRET: &str = "returned-output-never-expose";

    let valid_calls = Arc::new(Mutex::new(Vec::new()));
    let mut actions = ActionRegistry::default();
    actions
        .register(EchoAction {
            calls: Arc::clone(&valid_calls),
            streams_content: false,
            invalid_output: false,
        })
        .unwrap();
    actions
        .register(EchoAction {
            calls: Arc::new(Mutex::new(Vec::new())),
            streams_content: false,
            invalid_output: true,
        })
        .unwrap();
    let models = ModelRegistry::default();

    let mut compile_context = CompileContext::new(&models, &actions);
    let invalid_input = ActionNode
        .compile(
            "echo",
            json!({"action":"echo", "input":{"payload":"{{ input.secret }}"}}),
            &mut compile_context,
        )
        .unwrap();
    let invalid_input_node = compiled_node("echo", "core.action", EmitPolicy::None, invalid_input);
    let invalid_input_context = context(json!({"secret":INPUT_SECRET}))
        .with_templates(Arc::new(compile_context.into_templates()));
    let (control, _) = capturing_control();
    let input_error = ActionNode
        .execute(&invalid_input_node, &invalid_input_context, &control)
        .await
        .unwrap_err();
    assert_eq!(input_error.code(), "ACTION_INPUT_INVALID");
    assert_eq!(input_error.message(), "action input validation failed");
    assert!(!format!("{input_error:?}").contains(INPUT_SECRET));
    assert!(valid_calls.lock().unwrap().is_empty());

    let mut compile_context = CompileContext::new(&models, &actions);
    let invalid_output = ActionNode
        .compile(
            "bad",
            json!({
                "action":"bad_echo",
                "input":{"payload":{"secret":"{{ input.secret }}"}}
            }),
            &mut compile_context,
        )
        .unwrap();
    let invalid_output_node = compiled_node("bad", "core.action", EmitPolicy::None, invalid_output);
    let invalid_output_context = context(json!({"secret":OUTPUT_SECRET}))
        .with_templates(Arc::new(compile_context.into_templates()));
    let output_error = ActionNode
        .execute(&invalid_output_node, &invalid_output_context, &control)
        .await
        .unwrap_err();
    assert_eq!(output_error.code(), "ACTION_OUTPUT_INVALID");
    assert_eq!(output_error.message(), "action output validation failed");
    assert!(!format!("{output_error:?}").contains(OUTPUT_SECRET));
}

#[tokio::test]
async fn streaming_action_forwards_content_when_declared_and_enabled() {
    let mut actions = ActionRegistry::default();
    actions
        .register(EchoAction {
            calls: Arc::new(Mutex::new(Vec::new())),
            streams_content: true,
            invalid_output: false,
        })
        .unwrap();
    let models = ModelRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ActionNode
        .compile(
            "echo",
            json!({"action":"echo", "input":{"payload":{}}}),
            &mut compile_context,
        )
        .unwrap();
    assert!(compilation.envelope.allows_content_emit);
    let node = compiled_node("echo", "core.action", EmitPolicy::Content, compilation);
    let context = context(json!({})).with_templates(Arc::new(compile_context.into_templates()));
    let (control, emitted) = capturing_control();

    ActionNode.execute(&node, &context, &control).await.unwrap();

    assert_eq!(*emitted.lock().unwrap(), vec!["one", "two"]);
}

#[test]
fn non_streaming_action_disallows_content_emission_at_compile_time() {
    let mut actions = ActionRegistry::default();
    actions
        .register(EchoAction {
            calls: Arc::new(Mutex::new(Vec::new())),
            streams_content: false,
            invalid_output: false,
        })
        .unwrap();
    let models = ModelRegistry::default();
    let mut context = CompileContext::new(&models, &actions);
    let compilation = ActionNode
        .compile(
            "echo",
            json!({"action":"echo", "input":{"payload":{}}}),
            &mut context,
        )
        .unwrap();

    assert!(!compilation.envelope.allows_content_emit);
}

struct WaitingAction {
    started: Arc<Notify>,
}

#[async_trait]
impl Action for WaitingAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            name: "waiting",
            input_schema: json!({"type":"object"}),
            output_schema: json!({"type":"object"}),
            idempotent: false,
            streams_content: false,
        }
    }

    async fn call(&self, _input: Value, context: ActionContext) -> Result<Value, RunError> {
        self.started.notify_one();
        context.control.stopped().await;
        Err(RunError::stopped(context.control.stop_reason().unwrap()))
    }
}

#[tokio::test]
async fn action_execution_propagates_cancellation() {
    let started = Arc::new(Notify::new());
    let mut actions = ActionRegistry::default();
    actions
        .register(WaitingAction {
            started: Arc::clone(&started),
        })
        .unwrap();
    let models = ModelRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ActionNode
        .compile(
            "wait",
            json!({"action":"waiting", "input":{}}),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("wait", "core.action", EmitPolicy::None, compilation);
    let context = context(json!({})).with_templates(Arc::new(compile_context.into_templates()));
    let (controller, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    let execution = ActionNode.execute(&node, &context, &control);
    let cancellation = async {
        started.notified().await;
        controller.request(StopReason::Cancelled);
    };
    let (result, ()) = tokio::join!(execution, cancellation);

    assert_eq!(result.unwrap_err().code(), "RUN_CANCELLED");
}
