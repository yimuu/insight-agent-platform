use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    agent::{config::StepKind, loader::LoadedAgent},
    error::AppError,
    model::{
        openai::OpenAiModelClient,
        types::{ChatRequest, ChatStream, ModelClient},
    },
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelProvidersConfig {
    #[serde(default)]
    pub default_provider: Option<String>,
    pub providers: BTreeMap<String, ModelProviderConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelProviderConfig {
    pub kind: ModelProviderKind,
    pub base_url: String,
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub auth: Option<ModelProviderAuth>,
    #[serde(default)]
    pub defaults: BTreeMap<ModelType, String>,
    #[serde(default)]
    pub models: BTreeMap<ModelType, BTreeMap<String, ModelProviderModelConfig>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelProviderKind {
    #[serde(rename = "openai_compatible")]
    OpenAiCompatible,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelProviderAuth {
    None,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ModelProviderModelConfig {
    #[serde(default)]
    pub features: Vec<ModelFeature>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ModelType {
    #[default]
    Llm,
    #[serde(rename = "text_embedding", alias = "text-embedding")]
    TextEmbedding,
    Rerank,
    #[serde(rename = "speech2text", alias = "speech2_text")]
    Speech2Text,
    Tts,
    Moderation,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelFeature {
    Vision,
}

#[derive(Debug, Clone)]
pub struct ModelProviderCatalog {
    config: ModelProvidersConfig,
}

impl ModelProviderCatalog {
    pub fn new(config: ModelProvidersConfig) -> Result<Self, AppError> {
        if config.providers.is_empty() {
            return Err(AppError::Config(
                "model providers config requires at least one provider".to_string(),
            ));
        }

        if let Some(default_provider) = &config.default_provider {
            if !config.providers.contains_key(default_provider) {
                return Err(AppError::Config(format!(
                    "default model provider '{default_provider}' is not configured"
                )));
            }
        }

        for (provider_id, provider) in &config.providers {
            if provider.base_url.trim().is_empty() {
                return Err(AppError::Config(format!(
                    "model provider '{provider_id}' requires base_url"
                )));
            }
            if provider.models.is_empty() {
                return Err(AppError::Config(format!(
                    "model provider '{provider_id}' requires at least one model"
                )));
            }
            for (model_type, default_model) in &provider.defaults {
                if default_model.trim().is_empty() {
                    return Err(AppError::Config(format!(
                        "model provider '{provider_id}' default for '{model_type:?}' is empty"
                    )));
                }
                let Some(models) = provider.models.get(model_type) else {
                    return Err(AppError::Config(format!(
                        "model provider '{provider_id}' default for '{model_type:?}' has no model group"
                    )));
                };
                if !models.contains_key(default_model) {
                    return Err(AppError::Config(format!(
                        "model provider '{provider_id}' default model '{default_model}' is not in '{model_type:?}' models"
                    )));
                }
            }
            if provider.auth != Some(ModelProviderAuth::None) && provider.api_key_env.is_none() {
                return Err(AppError::Config(format!(
                    "model provider '{provider_id}' requires api_key_env unless auth is none"
                )));
            }
        }

        Ok(Self { config })
    }

    pub fn resolve_model<'a>(
        &'a self,
        provider_id: &str,
        model_type: ModelType,
        model: Option<&'a str>,
    ) -> Result<(&'a str, &'a ModelProviderModelConfig), AppError> {
        let provider = self.config.providers.get(provider_id).ok_or_else(|| {
            AppError::Config(format!("model provider '{provider_id}' is not configured"))
        })?;
        let model_id = match model {
            Some(model) => model,
            None => provider.defaults.get(&model_type).ok_or_else(|| {
                AppError::Config(format!(
                    "model provider '{provider_id}' has no default for '{model_type:?}'"
                ))
            })?,
        };
        let models = provider.models.get(&model_type).ok_or_else(|| {
            AppError::Config(format!(
                "model provider '{provider_id}' has no '{model_type:?}' models"
            ))
        })?;
        let model_config = models.get(model_id).ok_or_else(|| {
            AppError::Config(format!(
                "model '{model_id}' is not configured as '{model_type:?}' for provider '{provider_id}'"
            ))
        })?;
        Ok((model_id, model_config))
    }
}

pub fn validate_agent_models(
    agents: &[LoadedAgent],
    catalog: &ModelProviderCatalog,
) -> Result<(), AppError> {
    for agent in agents {
        let (model_id, model_config) = catalog.resolve_model(
            &agent.config.model.provider,
            agent.config.model.model_type,
            agent.config.model.model.as_deref(),
        )?;
        if agent.config.model.model_type != ModelType::Llm {
            return Err(AppError::Config(format!(
                "agent '{}' requires model type llm for llm steps",
                agent.config.id
            )));
        }
        let uses_images = agent.config.steps.iter().any(|step| {
            step.kind == StepKind::Llm && step.image_input.as_deref() == Some("input.images")
        });

        if uses_images && !model_config.features.contains(&ModelFeature::Vision) {
            return Err(AppError::Config(format!(
                "agent '{}' model '{}' requires vision feature because it uses image_input",
                agent.config.id, model_id
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ModelRouter<M: ModelClient> {
    default_provider: String,
    providers: Arc<BTreeMap<String, M>>,
}

impl<M: ModelClient> ModelRouter<M> {
    pub fn from_clients(
        default_provider: impl Into<String>,
        providers: BTreeMap<String, M>,
    ) -> Self {
        Self {
            default_provider: default_provider.into(),
            providers: Arc::new(providers),
        }
    }
}

#[async_trait]
impl<M: ModelClient> ModelClient for ModelRouter<M> {
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, AppError> {
        let provider_id = if request.provider.is_empty() {
            &self.default_provider
        } else {
            &request.provider
        };
        tracing::debug!(
            provider = %provider_id,
            model_type = ?request.model_type,
            model = %request.model,
            messages_count = request.messages.len(),
            "routing model request"
        );
        let provider = self.providers.get(provider_id).ok_or_else(|| {
            tracing::error!(provider = %provider_id, "model provider route missing");
            AppError::Config(format!("model provider '{provider_id}' is not configured"))
        })?;
        provider.stream_chat(request).await
    }
}

impl ModelRouter<OpenAiModelClient> {
    pub fn from_openai_compatible_config(config: &ModelProvidersConfig) -> Result<Self, AppError> {
        let catalog = ModelProviderCatalog::new(config.clone())?;
        let default_provider = config
            .default_provider
            .clone()
            .unwrap_or_else(|| config.providers.keys().next().cloned().unwrap_or_default());
        let mut providers = BTreeMap::new();

        for (provider_id, provider) in &config.providers {
            match provider.kind {
                ModelProviderKind::OpenAiCompatible => {
                    let model_groups = provider
                        .models
                        .keys()
                        .map(|model_type| format!("{model_type:?}"))
                        .collect::<Vec<_>>();
                    tracing::info!(
                        provider = %provider_id,
                        kind = ?provider.kind,
                        base_url = %provider.base_url,
                        auth = ?provider.auth,
                        api_key_env = ?provider.api_key_env,
                        model_groups = ?model_groups,
                        "registering model provider"
                    );
                    let api_key = match provider.auth {
                        Some(ModelProviderAuth::None) => None,
                        None => {
                            let env_name = provider.api_key_env.as_ref().ok_or_else(|| {
                                AppError::Config(format!(
                                    "model provider '{provider_id}' requires api_key_env"
                                ))
                            })?;
                            let value = std::env::var(env_name).map_err(|_| {
                                AppError::Config(format!(
                                    "environment variable '{env_name}' is required for model provider '{provider_id}'"
                                ))
                            })?;
                            if value.trim().is_empty() {
                                return Err(AppError::Config(format!(
                                    "environment variable '{env_name}' is required for model provider '{provider_id}'"
                                )));
                            }
                            Some(value)
                        }
                    };
                    providers.insert(
                        provider_id.clone(),
                        OpenAiModelClient::new_with_optional_api_key(
                            api_key,
                            provider.base_url.clone(),
                            provider
                                .defaults
                                .get(&ModelType::Llm)
                                .cloned()
                                .unwrap_or_default(),
                        ),
                    );
                }
            }
        }

        let router = Self::from_clients(default_provider, providers);
        let _ = catalog;
        Ok(router)
    }
}
