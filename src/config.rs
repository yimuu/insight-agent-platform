use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::agent::loader::LoadedAgent;
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
    pub internal_auth_token: Option<String>,
    pub agent_exposure: AgentExposureConfig,
    pub model_providers: ModelProvidersConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AgentExposureConfig {
    #[serde(default)]
    pub default_enabled: bool,
    #[serde(default)]
    pub default_public: bool,
    #[serde(default)]
    pub exposure: BTreeMap<String, AgentExposure>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AgentExposure {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub public: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PlatformYaml {
    #[serde(default)]
    bind_addr: Option<String>,
    #[serde(default)]
    auth: PlatformAuthYaml,
    #[serde(default)]
    agents: PlatformAgentsYaml,
    #[serde(default)]
    history: PlatformHistoryYaml,
    #[serde(default)]
    model_providers_config: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PlatformAuthYaml {
    #[serde(default)]
    internal_token_env: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PlatformAgentsYaml {
    #[serde(default)]
    directory: Option<PathBuf>,
    #[serde(default)]
    default_enabled: bool,
    #[serde(default)]
    default_public: bool,
    #[serde(default)]
    exposure: BTreeMap<String, AgentExposure>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PlatformHistoryYaml {
    #[serde(default)]
    db: Option<PathBuf>,
}

impl PlatformConfig {
    pub fn from_env() -> Result<Self, AppError> {
        let platform_config_path =
            env::var("PLATFORM_CONFIG").unwrap_or_else(|_| "config/platform.yaml".to_string());
        let yaml = load_platform_yaml(Path::new(&platform_config_path))?;

        let bind_addr = env::var("BIND_ADDR")
            .ok()
            .or(yaml.bind_addr)
            .unwrap_or_else(|| "127.0.0.1:3000".to_string())
            .parse()
            .map_err(|err| AppError::Config(format!("invalid BIND_ADDR: {err}")))?;

        let agents_dir = env::var("AGENTS_DIR")
            .ok()
            .map(PathBuf::from)
            .or(yaml.agents.directory)
            .unwrap_or_else(|| PathBuf::from("agents"));
        let run_history_db = env::var("RUN_HISTORY_DB")
            .ok()
            .map(PathBuf::from)
            .or(yaml.history.db)
            .unwrap_or_else(|| PathBuf::from("data/run_history.sqlite3"));
        let internal_auth_token = load_internal_auth_token(yaml.auth.internal_token_env)?;
        let model_providers = load_model_providers_config(yaml.model_providers_config)?;

        Ok(Self {
            bind_addr,
            agents_dir,
            run_history_db,
            internal_auth_token,
            agent_exposure: AgentExposureConfig {
                default_enabled: yaml.agents.default_enabled,
                default_public: yaml.agents.default_public,
                exposure: yaml.agents.exposure,
            },
            model_providers,
        })
    }

    pub fn filter_enabled_agents(
        &self,
        agents: Vec<LoadedAgent>,
    ) -> Result<Vec<LoadedAgent>, AppError> {
        filter_enabled_agents(agents, &self.agent_exposure)
    }
}

fn load_internal_auth_token(token_env: Option<String>) -> Result<Option<String>, AppError> {
    let Some(token_env) = token_env else {
        return Ok(None);
    };
    let token = env::var(&token_env).map_err(|_| {
        AppError::Config(format!(
            "environment variable '{token_env}' is required for internal runtime auth"
        ))
    })?;
    if token.trim().is_empty() {
        return Err(AppError::Config(format!(
            "environment variable '{token_env}' for internal runtime auth must not be empty"
        )));
    }
    Ok(Some(token))
}

fn load_platform_yaml(path: &Path) -> Result<PlatformYaml, AppError> {
    if path.exists() {
        let yaml = fs::read_to_string(&path).map_err(|err| {
            AppError::Config(format!(
                "failed to read platform config '{}': {err}",
                path.display()
            ))
        })?;
        return serde_yaml::from_str(&yaml).map_err(|err| {
            AppError::Config(format!(
                "invalid platform config '{}': {err}",
                path.display()
            ))
        });
    }
    Ok(PlatformYaml {
        agents: PlatformAgentsYaml {
            default_enabled: true,
            default_public: true,
            ..Default::default()
        },
        ..Default::default()
    })
}

fn load_model_providers_config(
    config_path: Option<PathBuf>,
) -> Result<ModelProvidersConfig, AppError> {
    let path = env::var("MODEL_PROVIDERS_CONFIG")
        .ok()
        .map(PathBuf::from)
        .or(config_path)
        .unwrap_or_else(|| PathBuf::from("config/models.yaml"));
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

fn filter_enabled_agents(
    agents: Vec<LoadedAgent>,
    exposure: &AgentExposureConfig,
) -> Result<Vec<LoadedAgent>, AppError> {
    let loaded_ids = agents
        .iter()
        .map(|agent| agent.config.id.clone())
        .collect::<BTreeSet<_>>();
    for (agent_id, policy) in &exposure.exposure {
        if policy.enabled && !loaded_ids.contains(agent_id) {
            return Err(AppError::Config(format!(
                "enabled agent '{agent_id}' was not found"
            )));
        }
    }

    Ok(agents
        .into_iter()
        .filter(|agent| {
            exposure
                .exposure
                .get(&agent.config.id)
                .map(|policy| policy.enabled)
                .unwrap_or(exposure.default_enabled)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::agent::{
        config::{AgentConfig, InputConfig, ModelConfig},
        loader::LoadedAgent,
    };

    use super::{
        filter_enabled_agents, load_internal_auth_token, AgentExposure, AgentExposureConfig,
    };

    fn loaded_agent(id: &str) -> LoadedAgent {
        LoadedAgent {
            root: PathBuf::from(format!("agents/{id}")),
            prompts: Default::default(),
            config: AgentConfig {
                id: id.to_string(),
                name: id.to_string(),
                description: String::new(),
                model: ModelConfig {
                    provider: "openai_compatible".to_string(),
                    model_type: Default::default(),
                    model: Some("fake".to_string()),
                    temperature: None,
                    max_tokens: None,
                    options: serde_json::Value::Null,
                },
                input: InputConfig {
                    schema: serde_json::json!({"type":"object"}),
                },
                prompts: Default::default(),
                steps: Vec::new(),
            },
        }
    }

    #[test]
    fn filters_agents_by_exposure_config() {
        let filtered = filter_enabled_agents(
            vec![
                loaded_agent("medical_report_interpreter"),
                loaded_agent("code_node_demo"),
            ],
            &AgentExposureConfig {
                default_enabled: false,
                default_public: false,
                exposure: [(
                    "medical_report_interpreter".to_string(),
                    AgentExposure {
                        enabled: true,
                        public: true,
                    },
                )]
                .into_iter()
                .collect(),
            },
        )
        .unwrap();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].config.id, "medical_report_interpreter");
    }

    #[test]
    fn rejects_enabled_agent_that_was_not_loaded() {
        let err = filter_enabled_agents(
            vec![loaded_agent("medical_report_interpreter")],
            &AgentExposureConfig {
                default_enabled: false,
                default_public: false,
                exposure: [(
                    "missing".to_string(),
                    AgentExposure {
                        enabled: true,
                        public: true,
                    },
                )]
                .into_iter()
                .collect(),
            },
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("enabled agent 'missing' was not found"));
    }

    #[test]
    fn loads_internal_auth_token_from_environment() {
        std::env::set_var("INSIGHT_TEST_INTERNAL_AUTH_TOKEN", "secret");

        let token =
            load_internal_auth_token(Some("INSIGHT_TEST_INTERNAL_AUTH_TOKEN".to_string())).unwrap();

        assert_eq!(token.as_deref(), Some("secret"));
    }
}
