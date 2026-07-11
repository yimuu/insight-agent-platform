use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    dsl::CompileError,
    resources::{
        actions::{Action, ActionContext, ActionDescriptor, ActionRegistry},
        models::{ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry},
    },
    runtime::{stop_pair, ExecutionControl, RunError, StopReason},
};
use serde_json::{json, Value};

#[derive(Debug, Default)]
struct FakeChatModel;

#[async_trait]
impl ChatModel for FakeChatModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::from([ModelCapability::Vision])
    }

    fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
        if parameters
            .get("temperature")
            .and_then(Value::as_f64)
            .is_some()
        {
            Ok(())
        } else {
            Err(CompileError::new(
                "MODEL_PARAMETERS_INVALID",
                "temperature is required",
            ))
        }
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::iter([Ok(ChatChunk {
            text: "ok".to_string(),
            finish_reason: Some("stop".to_string()),
            usage: None,
        })])))
    }
}

struct EchoAction;

#[async_trait]
impl Action for EchoAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            name: "echo",
            input_schema: json!({
                "type": "object",
                "required": ["text"],
                "additionalProperties": false,
                "properties": {"text": {"type": "string"}}
            }),
            output_schema: json!({
                "type": "object",
                "required": ["text"],
                "additionalProperties": false,
                "properties": {"text": {"type": "string"}}
            }),
            idempotent: true,
            streams_content: false,
        }
    }

    async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
        Ok(input)
    }
}

struct InvalidOutputAction;

#[async_trait]
impl Action for InvalidOutputAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            name: "invalid_output",
            input_schema: json!({"type":"object"}),
            output_schema: json!({"type":"string"}),
            idempotent: false,
            streams_content: false,
        }
    }

    async fn call(&self, _input: Value, _context: ActionContext) -> Result<Value, RunError> {
        Ok(json!({"secret":"never-expose-output"}))
    }
}

struct SchemaMatrixAction;

#[async_trait]
impl Action for SchemaMatrixAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            name: "schema_matrix",
            input_schema: json!({
                "type":"object",
                "required":["typed","sized","patterned","selected","choice"],
                "additionalProperties":false,
                "properties":{
                    "typed":{"type":"integer"},
                    "sized":{"type":"string","maxLength":4},
                    "patterned":{"type":"string","pattern":"^allowed$"},
                    "selected":{"enum":["allowed"]},
                    "choice":{"oneOf":[{"type":"integer"},{"const":"allowed"}]}
                }
            }),
            output_schema: json!({"type":"object"}),
            idempotent: true,
            streams_content: false,
        }
    }

    async fn call(&self, _input: Value, _context: ActionContext) -> Result<Value, RunError> {
        Ok(json!({}))
    }
}

fn test_control() -> ExecutionControl {
    let (_, signal) = stop_pair();
    ExecutionControl::new(signal, Duration::from_secs(5), |_content| async { Ok(()) })
}

fn test_action_context() -> ActionContext {
    ActionContext::new("run_test", "node_test", test_control())
}

#[test]
fn registries_reject_duplicate_aliases() {
    let mut models = ModelRegistry::default();
    models.register("default_chat", FakeChatModel).unwrap();
    assert_eq!(
        models
            .register("default_chat", FakeChatModel)
            .unwrap_err()
            .code(),
        "DUPLICATE_MODEL"
    );

    let mut actions = ActionRegistry::default();
    actions.register(EchoAction).unwrap();
    assert_eq!(
        actions.register(EchoAction).unwrap_err().code(),
        "DUPLICATE_ACTION"
    );
}

#[tokio::test]
async fn action_registry_validates_input_and_output_without_exposing_instances() {
    const INPUT_SECRET: &str = "never-expose-input";
    const OUTPUT_SECRET: &str = "never-expose-output";

    let mut registry = ActionRegistry::default();
    registry.register(EchoAction).unwrap();
    registry.register(InvalidOutputAction).unwrap();

    let echo = registry.resolve("echo").unwrap();
    assert_eq!(echo.descriptor().name, "echo");
    assert!(echo.descriptor().idempotent);

    let input_error = echo
        .validate_input(&json!({"text":{"secret":INPUT_SECRET}}))
        .unwrap_err();
    assert_eq!(input_error.code(), "ACTION_INPUT_INVALID");
    assert_eq!(input_error.message(), "action input validation failed");
    assert!(!input_error.to_string().contains(INPUT_SECRET));
    assert!(!format!("{input_error:?}").contains(INPUT_SECRET));

    assert_eq!(
        echo.call(json!({"text":"hi"}), test_action_context())
            .await
            .unwrap(),
        json!({"text":"hi"})
    );

    let invalid = registry.resolve("invalid_output").unwrap();
    let output_error = invalid
        .call(json!({}), test_action_context())
        .await
        .unwrap_err();
    assert_eq!(output_error.code(), "ACTION_OUTPUT_INVALID");
    assert_eq!(output_error.message(), "action output validation failed");
    assert!(!output_error.to_string().contains(OUTPUT_SECRET));
    assert!(!format!("{output_error:?}").contains(OUTPUT_SECRET));
}

#[test]
fn action_validation_keyword_matrix_never_exposes_values() {
    const SECRET: &str = "keyword-matrix-never-expose";
    let mut registry = ActionRegistry::default();
    registry.register(SchemaMatrixAction).unwrap();
    let action = registry.resolve("schema_matrix").unwrap();

    let cases = [
        json!({"typed":SECRET,"sized":"okay","patterned":"allowed","selected":"allowed","choice":1}),
        json!({"typed":1,"sized":SECRET,"patterned":"allowed","selected":"allowed","choice":1}),
        json!({"typed":1,"sized":"okay","patterned":SECRET,"selected":"allowed","choice":1}),
        json!({"typed":1,"sized":"okay","patterned":"allowed","selected":SECRET,"choice":1}),
        json!({"typed":1,"sized":"okay","patterned":"allowed","selected":"allowed","choice":SECRET}),
    ];

    for input in cases {
        let error = action.validate_input(&input).unwrap_err();
        assert_eq!(error.code(), "ACTION_INPUT_INVALID");
        assert_eq!(error.message(), "action input validation failed");
        assert!(!format!("{error:?}").contains(SECRET));
    }
}

#[test]
fn model_registry_exposes_capabilities_and_validates_parameters() {
    let mut registry = ModelRegistry::default();
    registry.register("default_chat", FakeChatModel).unwrap();

    let model = registry.resolve("default_chat").unwrap();
    assert!(model.capabilities().contains(&ModelCapability::Vision));
    assert!(model
        .validate_parameters(&json!({"temperature": 0.2}))
        .is_ok());
    assert_eq!(
        model.validate_parameters(&json!({})).unwrap_err().code(),
        "MODEL_PARAMETERS_INVALID"
    );
    assert_eq!(
        registry.resolve("missing").unwrap_err().code(),
        "MODEL_NOT_FOUND"
    );
}

#[tokio::test]
async fn execution_control_preserves_stop_reason_and_emits_content() {
    let emitted = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let output = Arc::clone(&emitted);
    let (controller, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(5), move |content| {
        let output = Arc::clone(&output);
        async move {
            output.lock().await.push(content);
            Ok(())
        }
    });

    control.emit_content("hello").await.unwrap();
    assert_eq!(*emitted.lock().await, vec!["hello"]);
    assert!(controller.request(StopReason::Interrupted));
    control.stopped().await;
    assert_eq!(control.stop_reason(), Some(StopReason::Interrupted));
    assert!(!controller.request(StopReason::Cancelled));
    assert_eq!(control.stop_reason(), Some(StopReason::Interrupted));
}
