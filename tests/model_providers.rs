use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use futures::{stream, StreamExt};
use serde_json::json;

use insight_agent_platform::{
    agent::{
        config::{AgentConfig, InputConfig, ModelConfig, StepConfig, StepKind},
        loader::{load_agents, LoadedAgent},
    },
    error::AppError,
    model::{
        providers::{
            validate_agent_models, ModelFeature, ModelProviderCatalog, ModelProviderConfig,
            ModelProviderKind, ModelProviderModelConfig, ModelProvidersConfig, ModelRouter,
            ModelType,
        },
        types::{ChatRequest, ChatStream, ModelClient},
    },
};

#[test]
fn parses_dify_style_provider_model_groups() {
    let yaml = r#"
default_provider: dashscope
providers:
  dashscope:
    kind: openai_compatible
    base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    api_key_env: OPENAI_API_KEY
    defaults:
      llm: qwen3.6-flash
      text_embedding: text-embedding-v4
      speech2text: paraformer-realtime-v2
    models:
      llm:
        qwen3.6-flash: {}
        qwen-vl-plus:
          features: [vision]
      text_embedding:
        text-embedding-v4: {}
      speech2text:
        paraformer-realtime-v2: {}
"#;

    let config: ModelProvidersConfig = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(config.default_provider.as_deref(), Some("dashscope"));
    let dashscope = config.providers.get("dashscope").unwrap();
    assert_eq!(dashscope.kind, ModelProviderKind::OpenAiCompatible);
    assert_eq!(dashscope.defaults[&ModelType::Llm], "qwen3.6-flash");
    assert!(dashscope.models[&ModelType::Llm]["qwen3.6-flash"]
        .features
        .is_empty());
    assert!(dashscope.models[&ModelType::Llm]["qwen-vl-plus"]
        .features
        .contains(&ModelFeature::Vision));
    assert!(dashscope.models[&ModelType::TextEmbedding].contains_key("text-embedding-v4"));
    assert!(dashscope.models[&ModelType::Speech2Text].contains_key("paraformer-realtime-v2"));
}

#[test]
fn repository_agents_validate_against_repository_model_config() {
    let yaml = std::fs::read_to_string("config/models.yaml").unwrap();
    let config: ModelProvidersConfig = serde_yaml::from_str(&yaml).unwrap();
    let catalog = ModelProviderCatalog::new(config).unwrap();
    let agents = load_agents("agents").unwrap();

    validate_agent_models(&agents, &catalog).unwrap();
}

#[derive(Clone)]
struct NamedFakeBackend {
    name: &'static str,
    calls: Arc<std::sync::Mutex<Vec<ChatRequest>>>,
}

impl NamedFakeBackend {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            calls: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl ModelClient for NamedFakeBackend {
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, AppError> {
        self.calls.lock().unwrap().push(request);
        Ok(Box::pin(stream::iter(vec![Ok(self.name.to_string())])))
    }
}

#[tokio::test]
async fn model_router_dispatches_by_provider_id() {
    let dashscope = NamedFakeBackend::new("dashscope");
    let openai = NamedFakeBackend::new("openai");
    let router = ModelRouter::from_clients(
        "dashscope",
        BTreeMap::from([
            ("dashscope".to_string(), dashscope.clone()),
            ("openai".to_string(), openai.clone()),
        ]),
    );

    let mut stream = router
        .stream_chat(ChatRequest {
            provider: "openai".to_string(),
            model_type: ModelType::Llm,
            model: "gpt-test".to_string(),
            messages: vec![],
            temperature: None,
            max_tokens: None,
        })
        .await
        .unwrap();

    assert_eq!(stream.next().await.unwrap().unwrap(), "openai");
    assert!(dashscope.calls.lock().unwrap().is_empty());
    assert_eq!(openai.calls.lock().unwrap().len(), 1);
}

#[test]
fn validates_agents_against_provider_model_whitelist_and_vision_capability() {
    let catalog = ModelProviderCatalog::new(ModelProvidersConfig {
        default_provider: Some("dashscope".to_string()),
        providers: BTreeMap::from([(
            "dashscope".to_string(),
            ModelProviderConfig {
                kind: ModelProviderKind::OpenAiCompatible,
                base_url: "https://dashscope.aliyuncs.com/compatible-mode/v1".to_string(),
                api_key_env: Some("OPENAI_API_KEY".to_string()),
                auth: None,
                defaults: BTreeMap::from([(ModelType::Llm, "qwen3.6-flash".to_string())]),
                models: BTreeMap::from([(
                    ModelType::Llm,
                    BTreeMap::from([
                        (
                            "qwen3.6-flash".to_string(),
                            ModelProviderModelConfig::default(),
                        ),
                        (
                            "qwen-vl-plus".to_string(),
                            ModelProviderModelConfig {
                                features: vec![ModelFeature::Vision],
                            },
                        ),
                    ]),
                )]),
            },
        )]),
    })
    .unwrap();

    let agents = vec![LoadedAgent {
        root: "agents/test".into(),
        prompts: Default::default(),
        config: AgentConfig {
            id: "medical".to_string(),
            name: "Medical".to_string(),
            description: String::new(),
            model: ModelConfig {
                provider: "dashscope".to_string(),
                model_type: ModelType::Llm,
                model: Some("qwen3.6-flash".to_string()),
                temperature: None,
                max_tokens: None,
                options: serde_json::Value::Null,
            },
            input: InputConfig {
                schema: json!({"type":"object"}),
            },
            prompts: Default::default(),
            steps: vec![StepConfig {
                id: "vision".to_string(),
                kind: StepKind::Llm,
                prompt_ref: None,
                prompt: Some("read".to_string()),
                system_prompt_ref: None,
                system_prompt: None,
                image_input: Some("input.images".to_string()),
                stream: true,
                tool: None,
                args: serde_json::Value::Null,
            }],
        },
    }];

    let err = validate_agent_models(&agents, &catalog)
        .unwrap_err()
        .to_string();
    assert!(err.contains("requires vision feature"));
}
