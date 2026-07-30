use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Write as _,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{
    ModelInputModality, ModelPolicyConfig, NativeStructuredOutput, ProviderExtensionSource,
    ProviderModelProfileConfig, ProviderTransportPolicy, ProvidersConfig,
};

use super::{
    models::{
        ModelCapability, ModelDeploymentIdentity, ModelRegistry, ModelRequestCapability,
        ModelSelector,
    },
    openai_chat::{OpenAiChatLimits, OpenAiChatModel, OpenAiTransportPolicy},
};

const BUILTIN_PROVIDER_CATALOG: &str = include_str!("../../catalog/provider-catalog.yaml");
const OPENAI_CHAT_ADAPTER_VERSION: &str = "2.0.0";
const OPENAI_CHAT_WORKER_VERSION: &str = "openai-chat-adapter-2.0.0";

pub fn load_model_registry(
    providers: &ProvidersConfig,
    model_policy: Option<&ModelPolicyConfig>,
) -> Result<ModelRegistry, ResourceConfigError> {
    load_model_registry_with_env(providers, model_policy, |name| env::var(name).ok())
}

pub fn load_model_registry_with_env(
    providers: &ProvidersConfig,
    model_policy: Option<&ModelPolicyConfig>,
    get_env: impl Fn(&str) -> Option<String>,
) -> Result<ModelRegistry, ResourceConfigError> {
    let catalog: ProviderCatalogYaml = crate::yaml::from_str(
        BUILTIN_PROVIDER_CATALOG,
        crate::yaml::YamlSurface::ProviderCatalog,
    )
    .map_err(|error| {
        ResourceConfigError::new(
            "PROVIDER_CATALOG_INVALID",
            format!("built-in provider catalog is invalid: {error}"),
        )
    })?;
    if catalog.catalog_version != 1 {
        return Err(ResourceConfigError::new(
            "PROVIDER_CATALOG_VERSION_UNSUPPORTED",
            format!(
                "unsupported provider catalog version {}; expected 1",
                catalog.catalog_version
            ),
        ));
    }
    if catalog.providers.is_empty() {
        return Err(ResourceConfigError::new(
            "PROVIDER_CATALOG_INVALID",
            "built-in provider catalog must define at least one provider route",
        ));
    }
    let catalog_digest = canonical_digest(&catalog, "PROVIDER_CATALOG_INVALID")?;
    let mut routes = resolve_builtin_routes(&catalog)?;
    apply_provider_extensions(&mut routes, providers)?;
    validate_model_policy(&routes, model_policy)?;

    let allowed = model_policy.map(|policy| &policy.allow);
    let mut registry = ModelRegistry::default();
    for (route_id, route) in routes {
        let credential_value = route.credential_env.as_deref().and_then(&get_env);
        let api_key = credential_value
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned);
        for (model_id, profile) in route.models {
            if allowed.is_some_and(|allowed| {
                !allowed
                    .iter()
                    .any(|selector| selector.provider == route_id && selector.id == model_id)
            }) {
                continue;
            }
            let selector = ModelSelector::new(route_id.clone(), model_id.clone())
                .map_err(resource_compile_error)?;
            let capabilities = model_capabilities(&profile);
            let request_capabilities = BTreeSet::from([
                ModelRequestCapability::Complete,
                ModelRequestCapability::Streaming,
            ]);
            let limits = OpenAiChatLimits::default();
            let transport: OpenAiTransportPolicy = route.transport.into();
            let model = OpenAiChatModel::new_with_limits_and_transport_policy(
                api_key.clone(),
                route.endpoint.clone(),
                model_id.clone(),
                capabilities.clone(),
                route.connect_timeout,
                route.request_timeout,
                limits,
                transport,
            )
            .map_err(resource_compile_error)?;
            let endpoint_identity = sanitized_endpoint_identity(&route.endpoint)?;
            let extension_digest = route.extension_digest.as_deref();
            let capability_names = capabilities
                .iter()
                .copied()
                .map(ModelCapability::as_str)
                .collect::<Vec<_>>();
            let request_capability_names = request_capabilities
                .iter()
                .copied()
                .map(ModelRequestCapability::as_str)
                .collect::<Vec<_>>();
            let input = profile
                .input
                .iter()
                .map(|input| match input {
                    ModelInputModality::Text => "text",
                    ModelInputModality::Image => "image",
                })
                .collect::<Vec<_>>();
            let native_structured_output =
                profile.native_structured_output.map(|mode| match mode {
                    NativeStructuredOutput::JsonObject => "json_object",
                    NativeStructuredOutput::JsonSchema => "json_schema",
                });
            let deployment = ModelDeploymentIdentity::new(
                OPENAI_CHAT_WORKER_VERSION,
                serde_json::json!({
                    "adapter": "openai_chat",
                    "adapter_version": OPENAI_CHAT_ADAPTER_VERSION,
                    "provider_route": route_id,
                    "model_id": model_id,
                    "endpoint_identity": endpoint_identity,
                    "credential_env": route.credential_env,
                    "catalog_version": catalog.catalog_version,
                    "catalog_digest": catalog_digest,
                    "provider_source": route.source,
                    "provider_extension_digest": extension_digest,
                    "input": input,
                    "native_structured_output": native_structured_output,
                    "capabilities": capability_names,
                    "request_capabilities": request_capability_names,
                    "connect_timeout_ms": route.connect_timeout.as_millis().to_string(),
                    "request_timeout_ms": route.request_timeout.as_millis().to_string(),
                    "limits": {
                        "max_upstream_bytes": limits.max_upstream_bytes,
                        "max_buffered_line_bytes": limits.max_buffered_line_bytes,
                        "max_event_payload_bytes": limits.max_event_payload_bytes,
                        "max_chunk_text_bytes": limits.max_chunk_text_bytes,
                        "max_usage_json_bytes": limits.max_usage_json_bytes,
                        "max_accumulated_text_bytes": limits.max_accumulated_text_bytes,
                    },
                    "transport": transport.as_str(),
                    "output_policy": "prompt-local-validation-with-optional-native-v1",
                }),
            )
            .map_err(resource_compile_error)?;
            registry
                .register_provider_model(
                    selector,
                    deployment,
                    route.credential_env.clone(),
                    credential_value.as_deref(),
                    model,
                )
                .map_err(resource_compile_error)?;
        }
    }
    Ok(registry)
}

#[derive(Debug, Clone)]
struct ResolvedProviderRoute {
    endpoint: String,
    credential_env: Option<String>,
    models: BTreeMap<String, ProviderModelProfileConfig>,
    connect_timeout: std::time::Duration,
    request_timeout: std::time::Duration,
    transport: ProviderTransportPolicy,
    source: String,
    extension_digest: Option<String>,
}

fn resolve_builtin_routes(
    catalog: &ProviderCatalogYaml,
) -> Result<BTreeMap<String, ResolvedProviderRoute>, ResourceConfigError> {
    catalog
        .providers
        .iter()
        .map(|(route_id, route)| {
            let selector_probe =
                ModelSelector::new(route_id.clone(), "__catalog_probe__".to_owned())
                    .map_err(resource_compile_error)?;
            debug_assert_eq!(selector_probe.provider(), route_id);
            if route.adapter != ProviderAdapterYaml::OpenAiChat {
                return Err(ResourceConfigError::new(
                    "PROVIDER_CATALOG_INVALID",
                    format!("provider route '{route_id}' uses an unsupported adapter"),
                ));
            }
            if route.credential.r#type != ProviderCredentialTypeYaml::Bearer
                || !valid_environment_variable_name(&route.credential.env)
            {
                return Err(ResourceConfigError::new(
                    "PROVIDER_CATALOG_INVALID",
                    format!("provider route '{route_id}' has invalid credentials"),
                ));
            }
            let models = route
                .models
                .iter()
                .map(|(model_id, profile)| {
                    ModelSelector::new(route_id.clone(), model_id.clone())
                        .map_err(resource_compile_error)?;
                    let profile = resolve_builtin_profile(profile);
                    if profile.input.contains(&ModelInputModality::Image)
                        && !profile.input.contains(&ModelInputModality::Text)
                    {
                        return Err(ResourceConfigError::new(
                            "PROVIDER_CATALOG_INVALID",
                            format!(
                                "provider route '{route_id}' model '{model_id}' image input requires text input"
                            ),
                        ));
                    }
                    Ok((model_id.clone(), profile))
                })
                .collect::<Result<BTreeMap<_, _>, ResourceConfigError>>()?;
            if models.is_empty() {
                return Err(ResourceConfigError::new(
                    "PROVIDER_CATALOG_INVALID",
                    format!("provider route '{route_id}' has no models"),
                ));
            }
            Ok((
                route_id.clone(),
                ResolvedProviderRoute {
                    endpoint: route.endpoint.clone(),
                    credential_env: Some(route.credential.env.clone()),
                    models,
                    connect_timeout: std::time::Duration::from_secs(5),
                    request_timeout: std::time::Duration::from_secs(120),
                    transport: ProviderTransportPolicy::HttpsOnly,
                    source: "builtin_catalog".to_owned(),
                    extension_digest: None,
                },
            ))
        })
        .collect()
}

fn resolve_builtin_profile(profile: &ProviderModelProfileYaml) -> ProviderModelProfileConfig {
    let input = if profile.input.is_empty() {
        BTreeSet::from([ModelInputModality::Text])
    } else {
        profile
            .input
            .iter()
            .map(|input| match input {
                ModelInputModalityYaml::Text => ModelInputModality::Text,
                ModelInputModalityYaml::Image => ModelInputModality::Image,
            })
            .collect()
    };
    ProviderModelProfileConfig {
        input,
        native_structured_output: profile.native_structured_output.map(|mode| match mode {
            NativeStructuredOutputYaml::JsonObject => NativeStructuredOutput::JsonObject,
            NativeStructuredOutputYaml::JsonSchema => NativeStructuredOutput::JsonSchema,
        }),
    }
}

fn apply_provider_extensions(
    routes: &mut BTreeMap<String, ResolvedProviderRoute>,
    providers: &ProvidersConfig,
) -> Result<(), ResourceConfigError> {
    let builtin_route_ids = routes.keys().cloned().collect::<BTreeSet<_>>();
    for (route_id, extension) in &providers.extensions {
        validate_provider_extension(route_id, extension)?;
        if routes.contains_key(route_id) {
            return Err(ResourceConfigError::new(
                "PROVIDER_ROUTE_CONFLICT",
                format!("provider route '{route_id}' conflicts with a built-in route"),
            ));
        }
        let extension_digest = canonical_digest(
            &provider_extension_evidence(route_id, extension),
            "PROVIDER_EXTENSION_INVALID",
        )?;
        let mut route = match &extension.source {
            ProviderExtensionSource::Extends { provider } => {
                if !builtin_route_ids.contains(provider) {
                    return Err(ResourceConfigError::new(
                        "PROVIDER_PARENT_NOT_FOUND",
                        format!(
                            "provider route '{route_id}' extends unknown built-in route '{provider}'"
                        ),
                    ));
                }
                routes
                    .get(provider)
                    .expect("validated built-in provider route exists")
                    .clone()
            }
            ProviderExtensionSource::OpenAiCompatible => ResolvedProviderRoute {
                endpoint: extension.endpoint.clone().ok_or_else(|| {
                    ResourceConfigError::new(
                        "PROVIDER_EXTENSION_INVALID",
                        format!("custom provider route '{route_id}' requires endpoint"),
                    )
                })?,
                credential_env: extension.credential_env.clone(),
                models: BTreeMap::new(),
                connect_timeout: extension.connect_timeout,
                request_timeout: extension.request_timeout,
                transport: extension.transport,
                source: "operator_extension".to_owned(),
                extension_digest: None,
            },
        };
        if let Some(endpoint) = &extension.endpoint {
            route.endpoint = endpoint.clone();
        }
        if let Some(credential_env) = &extension.credential_env {
            route.credential_env = Some(credential_env.clone());
        }
        route.connect_timeout = extension.connect_timeout;
        route.request_timeout = extension.request_timeout;
        route.transport = extension.transport;
        for (model_id, profile) in &extension.models {
            if route
                .models
                .insert(model_id.clone(), profile.clone())
                .is_some()
            {
                return Err(ResourceConfigError::new(
                    "PROVIDER_MODEL_CONFLICT",
                    format!(
                        "provider route '{route_id}' attempts to override inherited model '{model_id}'"
                    ),
                ));
            }
        }
        route.source = match &extension.source {
            ProviderExtensionSource::Extends { provider } => {
                format!("extension_of:{provider}")
            }
            ProviderExtensionSource::OpenAiCompatible => "operator_extension".to_owned(),
        };
        route.extension_digest = Some(extension_digest);
        routes.insert(route_id.clone(), route);
    }
    Ok(())
}

fn validate_provider_extension(
    route_id: &str,
    extension: &crate::config::ProviderExtensionConfig,
) -> Result<(), ResourceConfigError> {
    ModelSelector::new(route_id.to_owned(), "__extension_probe__".to_owned())
        .map_err(resource_compile_error)?;
    if extension.connect_timeout.is_zero() || extension.request_timeout.is_zero() {
        return Err(ResourceConfigError::new(
            "PROVIDER_EXTENSION_INVALID",
            format!("provider route '{route_id}' timeouts must be positive"),
        ));
    }
    match &extension.source {
        ProviderExtensionSource::Extends { .. } if extension.endpoint.is_some() => {
            return Err(ResourceConfigError::new(
                "PROVIDER_EXTENSION_INVALID",
                format!(
                    "inherited provider route '{route_id}' cannot override its built-in endpoint"
                ),
            ));
        }
        ProviderExtensionSource::OpenAiCompatible => {
            if extension.endpoint.is_none()
                || extension.credential_env.is_none()
                || extension.models.is_empty()
            {
                return Err(ResourceConfigError::new(
                    "PROVIDER_EXTENSION_INVALID",
                    format!(
                        "custom provider route '{route_id}' requires endpoint, credential, and at least one model"
                    ),
                ));
            }
        }
        ProviderExtensionSource::Extends { .. } => {}
    }
    if let Some(endpoint) = &extension.endpoint {
        sanitized_endpoint_identity(endpoint)?;
    }
    if extension
        .credential_env
        .as_deref()
        .is_some_and(|name| !valid_environment_variable_name(name))
    {
        return Err(ResourceConfigError::new(
            "PROVIDER_EXTENSION_INVALID",
            format!("provider route '{route_id}' credential environment variable is invalid"),
        ));
    }
    for (model_id, profile) in &extension.models {
        ModelSelector::new(route_id.to_owned(), model_id.clone())
            .map_err(resource_compile_error)?;
        if profile.input.contains(&ModelInputModality::Image)
            && !profile.input.contains(&ModelInputModality::Text)
        {
            return Err(ResourceConfigError::new(
                "PROVIDER_EXTENSION_INVALID",
                format!(
                    "provider route '{route_id}' model '{model_id}' image input requires text input"
                ),
            ));
        }
    }
    Ok(())
}

fn validate_model_policy(
    routes: &BTreeMap<String, ResolvedProviderRoute>,
    policy: Option<&ModelPolicyConfig>,
) -> Result<(), ResourceConfigError> {
    let Some(policy) = policy else {
        return Ok(());
    };
    for selector in &policy.allow {
        let Some(route) = routes.get(&selector.provider) else {
            return Err(ResourceConfigError::new(
                "MODEL_POLICY_PROVIDER_NOT_FOUND",
                format!(
                    "model policy references unknown provider route '{}'",
                    selector.provider
                ),
            ));
        };
        if !route.models.contains_key(&selector.id) {
            return Err(ResourceConfigError::new(
                "MODEL_POLICY_MODEL_NOT_FOUND",
                format!(
                    "model policy references unknown model '{}/{}'",
                    selector.provider, selector.id
                ),
            ));
        }
    }
    Ok(())
}

fn model_capabilities(profile: &ProviderModelProfileConfig) -> BTreeSet<ModelCapability> {
    let mut capabilities = BTreeSet::new();
    if profile.input.contains(&ModelInputModality::Image) {
        capabilities.insert(ModelCapability::Vision);
    }
    match profile.native_structured_output {
        Some(NativeStructuredOutput::JsonObject) => {
            capabilities.insert(ModelCapability::JsonObjectOutput);
        }
        Some(NativeStructuredOutput::JsonSchema) => {
            capabilities.insert(ModelCapability::JsonSchemaOutput);
        }
        None => {}
    }
    capabilities
}

fn provider_extension_evidence(
    route_id: &str,
    extension: &crate::config::ProviderExtensionConfig,
) -> serde_json::Value {
    let source = match &extension.source {
        ProviderExtensionSource::Extends { provider } => {
            serde_json::json!({"extends": provider})
        }
        ProviderExtensionSource::OpenAiCompatible => {
            serde_json::json!({"type": "openai_compatible"})
        }
    };
    let models = extension
        .models
        .iter()
        .map(|(id, profile)| {
            let input = profile
                .input
                .iter()
                .map(|input| match input {
                    ModelInputModality::Text => "text",
                    ModelInputModality::Image => "image",
                })
                .collect::<Vec<_>>();
            let native = profile.native_structured_output.map(|mode| match mode {
                NativeStructuredOutput::JsonObject => "json_object",
                NativeStructuredOutput::JsonSchema => "json_schema",
            });
            (
                id.clone(),
                serde_json::json!({"input": input, "native_structured_output": native}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::json!({
        "route_id": route_id,
        "source": source,
        "endpoint": extension.endpoint,
        "credential_env": extension.credential_env,
        "models": models,
        "connect_timeout_ms": extension.connect_timeout.as_millis().to_string(),
        "request_timeout_ms": extension.request_timeout.as_millis().to_string(),
        "transport": match extension.transport {
            ProviderTransportPolicy::HttpsOnly => "https_only",
            ProviderTransportPolicy::AllowLoopbackHttp => "allow_loopback_http",
            ProviderTransportPolicy::AllowTrustedPrivateHttp => "allow_trusted_private_http",
        },
    })
}

fn sanitized_endpoint_identity(endpoint: &str) -> Result<String, ResourceConfigError> {
    let endpoint = reqwest::Url::parse(endpoint).map_err(|_| {
        ResourceConfigError::new("PROVIDER_ENDPOINT_INVALID", "provider endpoint is invalid")
    })?;
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err(ResourceConfigError::new(
            "PROVIDER_ENDPOINT_INVALID",
            "provider endpoint must not include user information",
        ));
    }
    if endpoint.query().is_some() || endpoint.fragment().is_some() {
        return Err(ResourceConfigError::new(
            "PROVIDER_ENDPOINT_INVALID",
            "provider endpoint must not include a query or fragment",
        ));
    }
    Ok(endpoint.to_string().trim_end_matches('/').to_owned())
}

fn valid_environment_variable_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_uppercase() || byte == b'_' || (byte.is_ascii_digit() && index > 0)
        })
}

fn canonical_digest(
    value: &impl Serialize,
    code: &'static str,
) -> Result<String, ResourceConfigError> {
    let bytes = serde_jcs::to_vec(value).map_err(|_| {
        ResourceConfigError::new(code, "provider configuration cannot be canonicalized")
    })?;
    let digest = Sha256::digest(bytes);
    let mut rendered = String::with_capacity("sha256:".len() + digest.len() * 2);
    rendered.push_str("sha256:");
    for byte in digest {
        write!(&mut rendered, "{byte:02x}").expect("writing a digest into String cannot fail");
    }
    Ok(rendered)
}

fn resource_compile_error(error: insight_dsl::CompileError) -> ResourceConfigError {
    ResourceConfigError::new(error.code(), error.to_string())
}

impl From<ProviderTransportPolicy> for OpenAiTransportPolicy {
    fn from(value: ProviderTransportPolicy) -> Self {
        match value {
            ProviderTransportPolicy::HttpsOnly => Self::HttpsOnly,
            ProviderTransportPolicy::AllowLoopbackHttp => Self::AllowLoopbackHttp,
            ProviderTransportPolicy::AllowTrustedPrivateHttp => Self::AllowTrustedPrivateHttp,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCatalogYaml {
    catalog_version: u32,
    providers: BTreeMap<String, ProviderRouteYaml>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderRouteYaml {
    adapter: ProviderAdapterYaml,
    endpoint: String,
    credential: ProviderCredentialYaml,
    models: BTreeMap<String, ProviderModelProfileYaml>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProviderAdapterYaml {
    OpenAiChat,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCredentialYaml {
    r#type: ProviderCredentialTypeYaml,
    env: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProviderCredentialTypeYaml {
    Bearer,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderModelProfileYaml {
    #[serde(default)]
    input: BTreeSet<ModelInputModalityYaml>,
    #[serde(default)]
    native_structured_output: Option<NativeStructuredOutputYaml>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum ModelInputModalityYaml {
    Text,
    Image,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum NativeStructuredOutputYaml {
    JsonObject,
    JsonSchema,
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
