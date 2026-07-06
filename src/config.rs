use std::{collections::BTreeMap, env, fs, net::SocketAddr, path::PathBuf};

use crate::{
    error::AppError,
    model::providers::{
        ModelProviderConfig, ModelProviderKind, ModelProviderModelConfig, ModelProvidersConfig,
        ModelType,
    },
};

#[derive(Debug, Clone)]
pub struct PlatformConfig {
    pub bind_addr: SocketAddr,
    pub agents_dir: PathBuf,
    pub run_history_db: PathBuf,
    pub model_providers: ModelProvidersConfig,
}

impl PlatformConfig {
    pub fn from_env() -> Result<Self, AppError> {
        let bind_addr = env::var("BIND_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
            .parse()
            .map_err(|err| AppError::Config(format!("invalid BIND_ADDR: {err}")))?;

        let agents_dir = env::var("AGENTS_DIR").unwrap_or_else(|_| "agents".to_string());
        let run_history_db =
            env::var("RUN_HISTORY_DB").unwrap_or_else(|_| "data/run_history.sqlite3".to_string());
        let model_providers = load_model_providers_config()?;

        Ok(Self {
            bind_addr,
            agents_dir: PathBuf::from(agents_dir),
            run_history_db: PathBuf::from(run_history_db),
            model_providers,
        })
    }
}

fn load_model_providers_config() -> Result<ModelProvidersConfig, AppError> {
    let path =
        env::var("MODEL_PROVIDERS_CONFIG").unwrap_or_else(|_| "config/models.yaml".to_string());
    let path = PathBuf::from(path);
    if path.exists() {
        let yaml = fs::read_to_string(&path).map_err(|err| {
            AppError::Config(format!(
                "failed to read model providers config '{}': {err}",
                path.display()
            ))
        })?;
        return serde_yaml::from_str(&yaml).map_err(|err| {
            AppError::Config(format!(
                "invalid model providers config '{}': {err}",
                path.display()
            ))
        });
    }

    let base_url = env::var("OPENAI_BASE_URL")
        .unwrap_or_else(|_| "https://dashscope.aliyuncs.com/compatible-mode/v1".to_string());
    let default_model =
        env::var("OPENAI_DEFAULT_MODEL").unwrap_or_else(|_| "qwen3.6-flash".to_string());
    Ok(ModelProvidersConfig {
        default_provider: Some("openai_compatible".to_string()),
        providers: BTreeMap::from([(
            "openai_compatible".to_string(),
            ModelProviderConfig {
                kind: ModelProviderKind::OpenAiCompatible,
                base_url,
                api_key_env: Some("OPENAI_API_KEY".to_string()),
                auth: None,
                defaults: BTreeMap::from([(ModelType::Llm, default_model.clone())]),
                models: BTreeMap::from([(
                    ModelType::Llm,
                    BTreeMap::from([(default_model, ModelProviderModelConfig::default())]),
                )]),
            },
        )]),
    })
}
