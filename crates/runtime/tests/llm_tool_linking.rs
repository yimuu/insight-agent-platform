use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use futures::stream;
use insight_engine::{author::CompileError, execution::RunError, NodeId, SubflowContractRegistry};
use insight_resources::{
    actions::{
        Action, ActionContext, ActionDescriptor, ActionModelTool, ActionRegistry,
        CancellationClass, EffectClass, IdempotencyClass, ToolPublicArguments, ToolPublicPolicy,
    },
    builtin_actions::builtin_action_registry,
    models::{
        ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelDeploymentIdentity,
        ModelRegistry, ModelRequestCapability, ModelSelector,
    },
};
use insight_runtime::catalog::{
    compile_agent_dir, DeployedAgent, ProductionLeafDeploymentResolver,
    LLM_TOOL_CONTINUATION_CAPABILITY,
};
use serde_json::{json, Value};

#[allow(unused_macros)]
#[path = "../../../tests/support/workspace_assets.rs"]
mod workspace_assets;

#[derive(Debug, Clone)]
struct ModeModel {
    capabilities: BTreeSet<ModelCapability>,
    request_capabilities: BTreeSet<ModelRequestCapability>,
}

#[async_trait]
impl ChatModel for ModeModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        self.capabilities.clone()
    }

    fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
        self.request_capabilities.clone()
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
        Ok(Box::pin(stream::empty::<Result<ChatChunk, RunError>>()))
    }
}

#[derive(Clone)]
struct LookupAction {
    public: ToolPublicPolicy,
}

#[async_trait]
impl Action for LookupAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: "lookup".to_owned(),
            version: "2.1.0".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "tenant_scope": {"type": "string"}
                },
                "required": ["query", "tenant_scope"],
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "properties": {
                    "answer": {"type": "string"},
                    "internal_trace": {"type": "string"}
                },
                "required": ["answer", "internal_trace"],
                "additionalProperties": false
            }),
            effect: EffectClass::ReadOnly,
            idempotency: IdempotencyClass::Idempotent,
            cancellation: CancellationClass::Cooperative,
            required_capabilities: BTreeSet::new(),
        }
    }

    fn public_policy(&self) -> ToolPublicPolicy {
        self.public.clone()
    }

    async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
        Ok(input)
    }
}

#[derive(Clone)]
struct AliasedLookupAction;

#[async_trait]
impl Action for AliasedLookupAction {
    fn descriptor(&self) -> ActionDescriptor {
        LookupAction {
            public: ToolPublicPolicy::private(),
        }
        .descriptor()
    }

    fn model_tool(&self) -> ActionModelTool {
        ActionModelTool::new("remote_search")
            .with_title("Repository search")
            .with_description("Search the publication-frozen remote repository catalog.")
    }

    async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
        Ok(input)
    }
}

fn model_registry(capabilities: BTreeSet<ModelRequestCapability>) -> ModelRegistry {
    let mut models = ModelRegistry::default();
    models
        .register_versioned(
            ModelSelector::new("fixture", "fixture-model").unwrap(),
            ModelDeploymentIdentity::new(
                "fixture-model-worker-v2",
                json!({"adapter": "fixture", "provider_model": "fixture-model"}),
            )
            .unwrap(),
            ModeModel {
                capabilities: BTreeSet::new(),
                request_capabilities: capabilities,
            },
        )
        .unwrap();
    models
}

fn action_registry(public: ToolPublicPolicy) -> ActionRegistry {
    let mut actions = ActionRegistry::default();
    actions.register(LookupAction { public }).unwrap();
    actions
}

fn compile_agent(
    stream: bool,
    publish: bool,
    tools: &str,
) -> Arc<insight_runtime::catalog::PublishedAgent> {
    let directory = tempfile::tempdir().unwrap();
    let source = format!(
        r#"api_version: insight.agent/v1
kind: agent
metadata:
  id: tool_link
  name: Tool link
  description: Tool linker fixture.
inputs: {{}}
output: string
workflow:
  steps:
    - id: answer
      type: llm
      model:
        provider: fixture
        id: fixture-model
      messages: [{{role: user, content: [{{text: hello}}]}}]
      stream: {stream}
      publish: {publish}
      tools: {tools}
      tool_choice: lookup
      response: string
    - return: $answer
"#
    );
    std::fs::write(directory.path().join("agent.yaml"), source).unwrap();
    Arc::new(compile_agent_dir(directory.path()).unwrap())
}

fn publish(
    stream: bool,
    publish_output: bool,
    tools: &str,
    models: &ModelRegistry,
    actions: &ActionRegistry,
) -> Result<DeployedAgent, CompileError> {
    DeployedAgent::publish(
        compile_agent(stream, publish_output, tools),
        &ProductionLeafDeploymentResolver::new(models, actions),
        SubflowContractRegistry::new(),
    )
}

fn publish_with_tool_continuation(
    stream: bool,
    publish_output: bool,
    tools: &str,
    models: &ModelRegistry,
    actions: &ActionRegistry,
) -> Result<DeployedAgent, CompileError> {
    DeployedAgent::publish(
        compile_agent(stream, publish_output, tools),
        &ProductionLeafDeploymentResolver::new(models, actions)
            .with_llm_tool_continuation_capability(),
        SubflowContractRegistry::new(),
    )
}

fn llm_binding(deployment: &DeployedAgent) -> Value {
    let wire = serde_json::to_value(deployment.versioned_plan()).unwrap();
    wire["resolved_bindings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["node_id"] == json!("answer"))
        .unwrap()["binding"]
        .clone()
}

fn public_policy() -> ToolPublicPolicy {
    ToolPublicPolicy {
        call: true,
        arguments: ToolPublicArguments::Fields(BTreeSet::from(["query".to_owned()])),
        progress_schema: None,
        result_schema: Some(json!({
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
            "additionalProperties": false
        })),
    }
}

#[test]
fn linker_resolves_whitelist_request_mode_and_freezes_effective_public_policy() {
    let models = model_registry(BTreeSet::from([
        ModelRequestCapability::Complete,
        ModelRequestCapability::Streaming,
    ]));
    let actions = action_registry(public_policy());
    assert_eq!(
        publish(false, true, "[lookup]", &models, &actions)
            .unwrap_err()
            .code(),
        "LLM_TOOL_CONTINUATION_UNAVAILABLE",
        "the production resolver must fail closed until continuation is explicitly enabled"
    );

    let deployment =
        publish_with_tool_continuation(false, true, "[lookup]", &models, &actions).unwrap();
    let binding = llm_binding(&deployment);
    let linked = deployment.linked_plan().unwrap();
    assert_eq!(
        linked
            .descriptor(&NodeId::new("answer").unwrap())
            .unwrap()
            .deployment_binding(),
        &binding,
        "workers receive the exact frozen deployment binding, not a fresh registry lookup"
    );

    assert_eq!(binding["request_mode"], json!("complete_request"));
    assert_eq!(
        binding["request_capabilities"],
        json!(["complete_request", "streaming_request"])
    );
    assert_eq!(binding["tool_choice"], json!("lookup"));
    assert_eq!(
        binding["runtime_capabilities"],
        json!([LLM_TOOL_CONTINUATION_CAPABILITY]),
        "the explicit resolver grant is frozen into deployment identity"
    );
    assert_eq!(binding["tools"].as_array().unwrap().len(), 1);
    let tool = &binding["tools"][0];
    assert_eq!(tool["name"], json!("lookup"));
    assert_eq!(tool["action_version"], json!("2.1.0"));
    assert_eq!(tool["input_schema"]["additionalProperties"], false);
    assert_eq!(tool["output_schema"]["additionalProperties"], false);
    assert_eq!(tool["effect"], json!("read_only"));
    assert_eq!(tool["idempotency"], json!("idempotent"));
    assert_eq!(tool["cancellation"], json!("cooperative"));
    assert_eq!(tool["effect_policy"]["max_attempts"], json!(1));
    assert_eq!(
        binding["tool_limits"],
        json!({"max_rounds": 8, "max_calls": 32})
    );
    assert_eq!(tool["public_policy"]["arguments"], json!(["query"]));
    assert_eq!(
        tool["effective_public_policy"], tool["public_policy"],
        "publish=true applies the frozen tool-side policy"
    );
    assert!(tool["descriptor_hash"]
        .as_str()
        .is_some_and(|hash| hash.len() == 64));

    let private_deployment =
        publish_with_tool_continuation(false, false, "[lookup]", &models, &actions).unwrap();
    let private_tool = llm_binding(&private_deployment)["tools"][0].clone();
    assert_eq!(private_tool["public_policy"]["call"], true);
    assert_eq!(
        private_tool["effective_public_policy"],
        json!({"call": false, "arguments": "private", "result": null})
    );
    assert_ne!(
        deployment.deployment_revision_id(),
        private_deployment.deployment_revision_id(),
        "the Agent-side publish decision participates in frozen deployment identity"
    );
}

#[test]
fn linker_keeps_action_identity_separate_from_model_presentation() {
    let models = model_registry(BTreeSet::from([ModelRequestCapability::Complete]));
    let mut actions = ActionRegistry::default();
    actions.register(AliasedLookupAction).unwrap();

    let deployment =
        publish_with_tool_continuation(false, false, "[lookup]", &models, &actions).unwrap();
    let binding = llm_binding(&deployment);
    let tool = &binding["tools"][0];

    assert_eq!(tool["action_id"], "lookup");
    assert_eq!(tool["name"], "remote_search");
    assert_eq!(tool["title"], "Repository search");
    assert_eq!(
        tool["description"],
        "Search the publication-frozen remote repository catalog."
    );
    assert_eq!(
        binding["tool_choice"], "remote_search",
        "named DSL choices are translated from Action identity to model alias"
    );
}

#[test]
fn checked_in_multi_tool_agent_links_four_provider_safe_actions() {
    let mut models = ModelRegistry::default();
    models
        .register_versioned(
            ModelSelector::new("dashscope-cn", "qwen3.6-flash").unwrap(),
            ModelDeploymentIdentity::new(
                "fixture-model-worker-v2",
                json!({"adapter": "fixture", "provider_model": "fixture-model"}),
            )
            .unwrap(),
            ModeModel {
                capabilities: BTreeSet::new(),
                request_capabilities: BTreeSet::from([
                    ModelRequestCapability::Complete,
                    ModelRequestCapability::Streaming,
                ]),
            },
        )
        .unwrap();
    let actions = builtin_action_registry(
        &[
            "current_time".to_owned(),
            "text_metrics".to_owned(),
            "integer_calculator".to_owned(),
            "text_replace".to_owned(),
        ],
        None,
    )
    .unwrap();
    let agent_root = workspace_assets::workspace_path("agents/tool_assistant");
    let published = Arc::new(compile_agent_dir(&agent_root).unwrap());
    let deployment = DeployedAgent::publish(
        published,
        &ProductionLeafDeploymentResolver::new(&models, &actions)
            .with_llm_tool_continuation_capability()
            .with_llm_tool_limits(16, 64)
            .unwrap(),
        SubflowContractRegistry::new(),
    )
    .unwrap();
    let binding = llm_binding(&deployment);
    let names = binding["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        names,
        vec![
            "current_time",
            "text_metrics",
            "integer_calculator",
            "text_replace"
        ]
    );
    assert!(names.iter().all(|name| {
        !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    }));
    for tool in binding["tools"].as_array().unwrap() {
        let name = tool["name"].as_str().unwrap();
        let policy = tool["effective_public_policy"].as_object().unwrap();
        assert_eq!(policy["call"], true, "{name} must publish status");
        assert_eq!(
            policy["arguments"], "private",
            "{name} must keep arguments private"
        );
        if name == "text_replace" {
            assert_eq!(
                policy["result"],
                Value::Null,
                "text_replace must remain status-only"
            );
        } else {
            assert!(
                policy["result"].is_object(),
                "{name} must publish a closed safe result"
            );
            assert_eq!(policy["result"]["additionalProperties"], false);
        }
    }
}

#[test]
fn linker_rejects_image_messages_for_a_text_only_model() {
    let published = Arc::new(
        compile_agent_dir(&workspace_assets::workspace_path(
            "agents/medical_report_interpreter",
        ))
        .unwrap(),
    );
    let actions = ActionRegistry::default();

    for (capabilities, expected_error) in [
        (BTreeSet::new(), Some("LLM_VISION_BINDING_REQUIRED")),
        (BTreeSet::from([ModelCapability::Vision]), None),
    ] {
        let mut models = ModelRegistry::default();
        models
            .register_versioned(
                ModelSelector::new("dashscope-cn", "qwen-vl-plus").unwrap(),
                ModelDeploymentIdentity::new(
                    "fixture-model-worker-v2",
                    json!({"adapter": "fixture", "provider_model": "qwen-vl-plus"}),
                )
                .unwrap(),
                ModeModel {
                    capabilities,
                    request_capabilities: BTreeSet::from([
                        ModelRequestCapability::Complete,
                        ModelRequestCapability::Streaming,
                    ]),
                },
            )
            .unwrap();
        let result = DeployedAgent::publish(
            Arc::clone(&published),
            &ProductionLeafDeploymentResolver::new(&models, &actions),
            SubflowContractRegistry::new(),
        );
        match expected_error {
            Some(code) => assert_eq!(result.unwrap_err().code(), code),
            None => assert!(result.is_ok()),
        }
    }
}

#[test]
fn linker_rejects_missing_tools_invalid_public_escalation_and_request_mode_mismatch() {
    let both_modes = model_registry(BTreeSet::from([
        ModelRequestCapability::Complete,
        ModelRequestCapability::Streaming,
    ]));
    let no_actions = ActionRegistry::default();
    assert_eq!(
        publish_with_tool_continuation(false, true, "[lookup]", &both_modes, &no_actions)
            .unwrap_err()
            .code(),
        "LLM_TOOL_NOT_FOUND"
    );

    let invalid_actions = action_registry(ToolPublicPolicy {
        call: false,
        arguments: ToolPublicArguments::All,
        progress_schema: None,
        result_schema: None,
    });
    assert_eq!(
        publish_with_tool_continuation(false, true, "[lookup]", &both_modes, &invalid_actions)
            .unwrap_err()
            .code(),
        "ACTION_PUBLIC_POLICY_INVALID"
    );

    let streaming_only = model_registry(BTreeSet::from([ModelRequestCapability::Streaming]));
    let actions = action_registry(ToolPublicPolicy::private());
    assert_eq!(
        publish_with_tool_continuation(false, false, "[lookup]", &streaming_only, &actions)
            .unwrap_err()
            .code(),
        "LLM_REQUEST_MODE_UNSUPPORTED"
    );

    let complete_only = model_registry(BTreeSet::from([ModelRequestCapability::Complete]));
    assert_eq!(
        publish_with_tool_continuation(true, false, "[lookup]", &complete_only, &actions)
            .unwrap_err()
            .code(),
        "LLM_REQUEST_MODE_UNSUPPORTED"
    );
}
