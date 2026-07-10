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
    pub history: HistoryConfig,
    pub internal_auth_token: Option<String>,
    pub agent_exposure: AgentExposureConfig,
    pub model_providers: ModelProvidersConfig,
}

#[derive(Debug, Clone)]
pub struct HistoryConfig {
    pub provider: HistoryProvider,
    pub database_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryProvider {
    Sqlite,
    Postgres,
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
    provider: Option<HistoryProvider>,
    #[serde(default)]
    database_url: Option<String>,
    #[serde(default)]
    database_url_env: Option<String>,
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
        let history = resolve_history_config(yaml.history, |name| env::var(name).ok())?;
        let internal_auth_token = load_internal_auth_token(yaml.auth.internal_token_env)?;
        let model_providers = load_model_providers_config(yaml.model_providers_config)?;

        Ok(Self {
            bind_addr,
            agents_dir,
            history,
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
        let yaml = fs::read_to_string(path).map_err(|err| {
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

fn resolve_history_config(
    yaml: PlatformHistoryYaml,
    get_env: impl Fn(&str) -> Option<String>,
) -> Result<HistoryConfig, AppError> {
    if let Some(database_url) = get_env("RUN_HISTORY_DATABASE_URL") {
        return history_config_from_url(yaml.provider, database_url, "RUN_HISTORY_DATABASE_URL");
    }

    if let Some(database_url_env) = yaml.database_url_env {
        let database_url = get_env(&database_url_env).ok_or_else(|| {
            AppError::Config(format!(
                "environment variable '{database_url_env}' is required for run history"
            ))
        })?;
        return history_config_from_url(yaml.provider, database_url, &database_url_env);
    }

    if let Some(database_url) = yaml.database_url {
        return history_config_from_url(yaml.provider, database_url, "history.database_url");
    }

    let db_path = get_env("RUN_HISTORY_DB").map(PathBuf::from).or(yaml.db);
    if let Some(db_path) = db_path {
        if yaml.provider == Some(HistoryProvider::Postgres) {
            return Err(AppError::Config(
                "history.db and RUN_HISTORY_DB can only be used with sqlite history".to_string(),
            ));
        }
        return Ok(HistoryConfig {
            provider: HistoryProvider::Sqlite,
            database_url: sqlite_database_url_from_path(&db_path),
        });
    }

    if yaml.provider == Some(HistoryProvider::Postgres) {
        return Err(AppError::Config(
            "postgres run history requires history.database_url or history.database_url_env"
                .to_string(),
        ));
    }

    Ok(HistoryConfig {
        provider: HistoryProvider::Sqlite,
        database_url: sqlite_database_url_from_path(Path::new("data/run_history.sqlite3")),
    })
}

fn history_config_from_url(
    provider: Option<HistoryProvider>,
    database_url: String,
    source: &str,
) -> Result<HistoryConfig, AppError> {
    let database_url = non_empty_history_url(database_url, source)?;
    let provider = provider.unwrap_or_else(|| infer_history_provider(&database_url));
    let database_url = normalize_history_database_url(provider, &database_url)?;
    Ok(HistoryConfig {
        provider,
        database_url,
    })
}

fn non_empty_history_url(database_url: String, source: &str) -> Result<String, AppError> {
    let database_url = database_url.trim().to_string();
    if database_url.is_empty() {
        return Err(AppError::Config(format!(
            "{source} for run history must not be empty"
        )));
    }
    Ok(database_url)
}

fn infer_history_provider(database_url: &str) -> HistoryProvider {
    if is_postgres_url(database_url) {
        HistoryProvider::Postgres
    } else {
        HistoryProvider::Sqlite
    }
}

fn normalize_history_database_url(
    provider: HistoryProvider,
    database_url: &str,
) -> Result<String, AppError> {
    match provider {
        HistoryProvider::Sqlite => {
            if database_url.starts_with("sqlite:") {
                Ok(database_url.to_string())
            } else {
                Ok(sqlite_database_url_from_path(Path::new(database_url)))
            }
        }
        HistoryProvider::Postgres => {
            if is_postgres_url(database_url) {
                Ok(database_url.to_string())
            } else {
                Err(AppError::Config(
                    "postgres run history requires a postgres:// or postgresql:// database_url"
                        .to_string(),
                ))
            }
        }
    }
}

fn is_postgres_url(database_url: &str) -> bool {
    database_url.starts_with("postgres://") || database_url.starts_with("postgresql://")
}

fn sqlite_database_url_from_path(path: &Path) -> String {
    format!("sqlite://{}", path.display())
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
        filter_enabled_agents, load_internal_auth_token, resolve_history_config, AgentExposure,
        AgentExposureConfig, HistoryProvider, PlatformHistoryYaml,
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

    #[test]
    fn resolves_legacy_history_db_as_sqlite_url() {
        let history = resolve_history_config(
            PlatformHistoryYaml {
                db: Some(PathBuf::from("data/custom.sqlite3")),
                ..Default::default()
            },
            |_| None,
        )
        .unwrap();

        assert_eq!(history.provider, HistoryProvider::Sqlite);
        assert_eq!(history.database_url, "sqlite://data/custom.sqlite3");
    }

    #[test]
    fn resolves_history_database_url_and_infers_postgres_provider() {
        let history = resolve_history_config(
            PlatformHistoryYaml {
                database_url: Some("postgres://user:pass@localhost/insight".to_string()),
                ..Default::default()
            },
            |_| None,
        )
        .unwrap();

        assert_eq!(history.provider, HistoryProvider::Postgres);
        assert_eq!(
            history.database_url,
            "postgres://user:pass@localhost/insight"
        );
    }

    #[test]
    fn resolves_history_database_url_from_named_environment() {
        let history = resolve_history_config(
            PlatformHistoryYaml {
                provider: Some(HistoryProvider::Sqlite),
                database_url_env: Some("INSIGHT_HISTORY_URL".to_string()),
                ..Default::default()
            },
            |name| (name == "INSIGHT_HISTORY_URL").then(|| "sqlite::memory:".to_string()),
        )
        .unwrap();

        assert_eq!(history.provider, HistoryProvider::Sqlite);
        assert_eq!(history.database_url, "sqlite::memory:");
    }
}
