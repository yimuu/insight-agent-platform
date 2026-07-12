use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::Path,
    time::Duration,
};

use serde::Deserialize;

use super::{
    models::{ModelCapability, ModelRegistry},
    openai_chat::{OpenAiChatLimits, OpenAiChatModel, OpenAiTransportPolicy},
};

pub fn load_model_registry(path: &Path) -> Result<ModelRegistry, ResourceConfigError> {
    load_model_registry_with_env(path, |name| env::var(name).ok())
}

pub fn load_model_registry_with_env(
    path: &Path,
    get_env: impl Fn(&str) -> Option<String>,
) -> Result<ModelRegistry, ResourceConfigError> {
    let yaml = fs::read_to_string(path).map_err(|_| {
        ResourceConfigError::new(
            "MODEL_CONFIG_READ_FAILED",
            format!("failed to read model config '{}'", path.display()),
        )
    })?;
    let raw: ModelResourcesYaml =
        crate::yaml::from_str(&yaml, crate::yaml::YamlSurface::ModelResources).map_err(
            |error| {
                ResourceConfigError::new(
                    "MODEL_CONFIG_INVALID",
                    format!("invalid model config: {error}"),
                )
            },
        )?;
    if raw.version != 1 {
        return Err(ResourceConfigError::new(
            "MODEL_CONFIG_VERSION_UNSUPPORTED",
            format!(
                "unsupported model config version {}; expected 1",
                raw.version
            ),
        ));
    }
    if raw.models.is_empty() {
        return Err(ResourceConfigError::new(
            "MODEL_CONFIG_INVALID",
            "model config must define at least one model alias",
        ));
    }

    let mut registry = ModelRegistry::default();
    for (alias, model) in raw.models {
        match model {
            ModelYaml::OpenAiChat {
                base_url,
                model,
                api_key_env,
                capabilities,
                connect_timeout,
                request_timeout,
                limits,
                transport,
            } => {
                let api_key = api_key_env
                    .map(|name| required_secret(&name, &get_env))
                    .transpose()?;
                let capabilities = capabilities
                    .into_iter()
                    .map(|capability| match capability {
                        CapabilityYaml::Vision => ModelCapability::Vision,
                    })
                    .collect::<BTreeSet<_>>();
                let limits = limits.unwrap_or_default().resolve()?;
                let transport_policy = transport.unwrap_or_default().plaintext_http.into();
                let model = OpenAiChatModel::new_with_limits_and_transport_policy(
                    api_key,
                    base_url,
                    model,
                    capabilities,
                    positive_duration(&connect_timeout, "connect_timeout")?,
                    positive_duration(&request_timeout, "request_timeout")?,
                    limits,
                    transport_policy,
                )
                .map_err(|error| {
                    ResourceConfigError::new(error.code(), format!("model '{alias}': {error}"))
                })?;
                registry
                    .register(alias, model)
                    .map_err(|error| ResourceConfigError::new(error.code(), error.to_string()))?;
            }
        }
    }
    Ok(registry)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelResourcesYaml {
    version: u32,
    models: BTreeMap<String, ModelYaml>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ModelYaml {
    OpenAiChat {
        base_url: String,
        model: String,
        api_key_env: Option<String>,
        #[serde(default)]
        capabilities: BTreeSet<CapabilityYaml>,
        connect_timeout: String,
        request_timeout: String,
        limits: Option<OpenAiChatLimitsYaml>,
        transport: Option<OpenAiTransportYaml>,
    },
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenAiChatLimitsYaml {
    max_upstream_bytes: Option<usize>,
    max_buffered_line_bytes: Option<usize>,
    max_event_payload_bytes: Option<usize>,
    max_chunk_text_bytes: Option<usize>,
    max_usage_json_bytes: Option<usize>,
    max_accumulated_text_bytes: Option<usize>,
}

impl OpenAiChatLimitsYaml {
    fn resolve(self) -> Result<OpenAiChatLimits, ResourceConfigError> {
        let defaults = OpenAiChatLimits::default();
        OpenAiChatLimits {
            max_upstream_bytes: self
                .max_upstream_bytes
                .unwrap_or(defaults.max_upstream_bytes),
            max_buffered_line_bytes: self
                .max_buffered_line_bytes
                .unwrap_or(defaults.max_buffered_line_bytes),
            max_event_payload_bytes: self
                .max_event_payload_bytes
                .unwrap_or(defaults.max_event_payload_bytes),
            max_chunk_text_bytes: self
                .max_chunk_text_bytes
                .unwrap_or(defaults.max_chunk_text_bytes),
            max_usage_json_bytes: self
                .max_usage_json_bytes
                .unwrap_or(defaults.max_usage_json_bytes),
            max_accumulated_text_bytes: self
                .max_accumulated_text_bytes
                .unwrap_or(defaults.max_accumulated_text_bytes),
        }
        .validate()
        .map_err(|error| ResourceConfigError::new(error.code(), error.to_string()))
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenAiTransportYaml {
    #[serde(default)]
    plaintext_http: PlaintextHttpYaml,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PlaintextHttpYaml {
    #[default]
    Disabled,
    Loopback,
    TrustedPrivate,
}

impl From<PlaintextHttpYaml> for OpenAiTransportPolicy {
    fn from(value: PlaintextHttpYaml) -> Self {
        match value {
            PlaintextHttpYaml::Disabled => Self::HttpsOnly,
            PlaintextHttpYaml::Loopback => Self::AllowLoopbackHttp,
            PlaintextHttpYaml::TrustedPrivate => Self::AllowTrustedPrivateHttp,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CapabilityYaml {
    Vision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceConfigError {
    code: &'static str,
    message: String,
}

impl ResourceConfigError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl std::fmt::Display for ResourceConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ResourceConfigError {}

fn required_secret(
    name: &str,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<String, ResourceConfigError> {
    if name.trim().is_empty() {
        return Err(ResourceConfigError::new(
            "MODEL_CONFIG_INVALID",
            "model secret environment variable name must not be empty",
        ));
    }
    let value = get_env(name).ok_or_else(|| {
        ResourceConfigError::new(
            "MODEL_SECRET_MISSING",
            format!("required environment variable '{name}' is missing"),
        )
    })?;
    if value.trim().is_empty() {
        return Err(ResourceConfigError::new(
            "MODEL_SECRET_EMPTY",
            format!("required environment variable '{name}' is empty"),
        ));
    }
    Ok(value)
}

fn positive_duration(value: &str, field: &str) -> Result<Duration, ResourceConfigError> {
    let duration = humantime::parse_duration(value).map_err(|_| {
        ResourceConfigError::new(
            "MODEL_CONFIG_INVALID",
            format!("model {field} must be a valid positive duration"),
        )
    })?;
    if duration.is_zero() {
        return Err(ResourceConfigError::new(
            "MODEL_CONFIG_INVALID",
            format!("model {field} must be greater than zero"),
        ));
    }
    Ok(duration)
}
