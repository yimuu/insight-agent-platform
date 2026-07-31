use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::Url;
use serde::Deserialize;

use super::{
    positive_duration, required_secret, resolve_path, DeploymentMode, PlatformConfigError,
    SecretString,
};

pub const MCP_MODERN_PROTOCOL_VERSION: &str = "2026-07-28";
pub const MCP_LEGACY_PROTOCOL_VERSION: &str = "2025-11-25";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpConfig {
    pub version: u32,
    pub protocol: McpProtocolConfig,
    pub client: McpClientConfig,
    pub server: McpServerConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpProtocolConfig {
    pub preferred: String,
    pub legacy_fallback: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpClientConfig {
    pub enabled: bool,
    pub servers: BTreeMap<String, McpClientServerConfig>,
    pub secret_encryption: Option<McpSecretEncryptionConfig>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct McpSecretEncryptionConfig {
    pub active_key_version: String,
    pub keyring: SecretString,
}

impl std::fmt::Debug for McpSecretEncryptionConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpSecretEncryptionConfig")
            .field("active_key_version", &self.active_key_version)
            .field("keyring", &"REDACTED")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpClientServerConfig {
    pub transport: McpClientTransportConfig,
    pub discovery: McpDiscoveryConfig,
    pub authorization: McpClientAuthorizationConfig,
    pub imports: McpImportConfig,
    pub limits: McpLimitsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpClientTransportConfig {
    StreamableHttp {
        endpoint: String,
        allow_plaintext_loopback: bool,
        connect_timeout: Duration,
        request_timeout: Duration,
    },
    Stdio {
        executable: PathBuf,
        args: Vec<String>,
        working_directory: PathBuf,
        environment: BTreeMap<String, String>,
        isolation_profile: Option<String>,
        startup_timeout: Duration,
        request_timeout: Duration,
        shutdown_timeout: Duration,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpDiscoveryConfig {
    LiveServiceAccount,
    SignedManifest {
        path: PathBuf,
        trust_keys_environment: String,
        trust_keys: SecretString,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub enum McpClientAuthorizationConfig {
    None,
    BearerEnv {
        environment: String,
        token: SecretString,
    },
    OauthUser {
        scopes: BTreeSet<String>,
        client_id: String,
        redirect_uri: String,
    },
}

impl std::fmt::Debug for McpClientAuthorizationConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => formatter.write_str("None"),
            Self::BearerEnv { environment, .. } => formatter
                .debug_struct("BearerEnv")
                .field("environment", environment)
                .field("token", &"REDACTED")
                .finish(),
            Self::OauthUser {
                scopes,
                client_id,
                redirect_uri,
            } => formatter
                .debug_struct("OauthUser")
                .field("scopes", scopes)
                .field("client_id", client_id)
                .field("redirect_uri", redirect_uri)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct McpImportConfig {
    pub tools: Vec<McpToolImportConfig>,
    pub resources: Vec<String>,
    pub prompts: Vec<McpPromptImportConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpPromptImportConfig {
    pub remote_name: String,
    pub allow_user_invocation: bool,
    pub definition_arguments: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpEffectConfig {
    Pure,
    ReadOnly,
    Mutating,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpIdempotencyConfig {
    Idempotent,
    NonIdempotent,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpCancellationConfig {
    NotSupported,
    Cooperative,
    Immediate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpApprovalConfig {
    Never,
    ModelToolOnly,
    Mutating,
    Always,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpInteractionConfig {
    Denied,
    Allowed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpDescriptionSourceConfig {
    Remote,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpToolImportConfig {
    pub remote_name: String,
    pub alias: String,
    pub title: Option<String>,
    pub description_source: McpDescriptionSourceConfig,
    pub description_override: Option<String>,
    pub effect: McpEffectConfig,
    pub idempotency: McpIdempotencyConfig,
    pub cancellation: McpCancellationConfig,
    pub required_capabilities: BTreeSet<String>,
    pub approval: McpApprovalConfig,
    pub input_required: McpInteractionConfig,
    pub tasks: McpInteractionConfig,
    pub terminal_only_compatible: bool,
    pub public_call: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpLimitsConfig {
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_sse_line_bytes: usize,
    pub max_sse_event_bytes: usize,
    pub max_content_items: usize,
    pub max_catalog_items: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub authorization: McpServerAuthorizationConfig,
    pub exports: McpExportConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpServerAuthorizationConfig {
    Disabled,
    BearerCompatible,
    OauthResourceServer {
        resource: String,
        authorization_servers: Vec<String>,
        required_scopes: BTreeSet<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct McpExportConfig {
    pub agents: Vec<McpAgentExportConfig>,
    pub actions: Vec<McpActionExportConfig>,
    pub resources: Vec<McpResourceExportConfig>,
    pub prompts: Vec<McpPromptExportConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAgentExportConfig {
    pub agent: String,
    pub alias: String,
    pub execution: McpExportExecutionConfig,
    pub input_required: McpInteractionConfig,
    pub required_scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpActionExportConfig {
    pub action: String,
    pub alias: String,
    pub required_scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpResourceExportConfig {
    pub uri: String,
    pub name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub mime_type: String,
    pub text: String,
    pub required_scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpPromptExportConfig {
    pub name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub arguments: Vec<McpPromptArgumentExportConfig>,
    pub messages: Vec<McpPromptMessageExportConfig>,
    pub completions: BTreeMap<String, Vec<String>>,
    pub required_scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpPromptArgumentExportConfig {
    pub name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpPromptMessageExportConfig {
    pub role: McpPromptRoleConfig,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpPromptRoleConfig {
    Assistant,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpExportExecutionConfig {
    Synchronous,
    TaskPreferred,
    TaskRequired,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct McpYaml {
    version: u32,
    protocol: McpProtocolYaml,
    client: McpClientYaml,
    server: McpServerYaml,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpProtocolYaml {
    preferred: String,
    #[serde(default)]
    legacy_fallback: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpClientYaml {
    enabled: bool,
    #[serde(default)]
    servers: BTreeMap<String, McpClientServerYaml>,
    #[serde(default)]
    secret_encryption: Option<McpSecretEncryptionYaml>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpSecretEncryptionYaml {
    active_key_version: String,
    keyring_env: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpClientServerYaml {
    transport: McpClientTransportYaml,
    discovery: McpDiscoveryYaml,
    authorization: McpClientAuthorizationYaml,
    #[serde(default)]
    imports: McpImportsYaml,
    #[serde(default)]
    limits: McpLimitsYaml,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum McpClientTransportYaml {
    StreamableHttp {
        endpoint: String,
        #[serde(default)]
        allow_plaintext_loopback: bool,
        #[serde(default = "default_connect_timeout")]
        connect_timeout: String,
        #[serde(default = "default_request_timeout")]
        request_timeout: String,
    },
    Stdio {
        executable: PathBuf,
        #[serde(default)]
        args: Vec<String>,
        working_directory: PathBuf,
        #[serde(default)]
        environment: BTreeMap<String, McpSecretReferenceYaml>,
        #[serde(default)]
        isolation: Option<McpIsolationYaml>,
        #[serde(default = "default_startup_timeout")]
        startup_timeout: String,
        #[serde(default = "default_request_timeout")]
        request_timeout: String,
        #[serde(default = "default_shutdown_timeout")]
        shutdown_timeout: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum McpDiscoveryYaml {
    LiveServiceAccount,
    SignedManifest {
        path: PathBuf,
        trust_keys_env: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum McpClientAuthorizationYaml {
    None,
    BearerEnv {
        token_env: String,
    },
    OauthUser {
        scopes: Vec<String>,
        client_id: String,
        redirect_uri: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpSecretReferenceYaml {
    secret_env: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpIsolationYaml {
    profile: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpImportsYaml {
    #[serde(default)]
    tools: Vec<McpToolImportYaml>,
    #[serde(default)]
    resources: McpAllowYaml,
    #[serde(default)]
    prompts: McpPromptAllowYaml,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpAllowYaml {
    #[serde(default)]
    allow: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpPromptAllowYaml {
    #[serde(default)]
    allow: Vec<McpPromptImportYaml>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum McpPromptImportYaml {
    Name(String),
    Detailed(McpPromptImportDetailYaml),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpPromptImportDetailYaml {
    remote: String,
    #[serde(default = "default_true")]
    user_invocation: bool,
    #[serde(default)]
    definition_arguments: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpToolImportYaml {
    remote: String,
    #[serde(rename = "as")]
    alias: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<McpDescriptionYaml>,
    effect: McpEffectConfig,
    idempotency: McpIdempotencyConfig,
    cancellation: McpCancellationConfig,
    #[serde(default)]
    required_capabilities: Vec<String>,
    approval: McpApprovalConfig,
    input_required: McpInteractionConfig,
    tasks: McpInteractionConfig,
    #[serde(default)]
    terminal_only_compatible: bool,
    #[serde(default)]
    public: McpPublicToolYaml,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum McpDescriptionYaml {
    Source(McpDescriptionSourceConfig),
    Override { r#override: String },
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpPublicToolYaml {
    #[serde(default)]
    call: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpLimitsYaml {
    #[serde(default = "default_max_request_bytes")]
    max_request_bytes: usize,
    #[serde(default = "default_max_response_bytes")]
    max_response_bytes: usize,
    #[serde(default = "default_max_sse_line_bytes")]
    max_sse_line_bytes: usize,
    #[serde(default = "default_max_sse_event_bytes")]
    max_sse_event_bytes: usize,
    #[serde(default = "default_max_content_items")]
    max_content_items: usize,
    #[serde(default = "default_max_catalog_items")]
    max_catalog_items: usize,
}

impl Default for McpLimitsYaml {
    fn default() -> Self {
        Self {
            max_request_bytes: default_max_request_bytes(),
            max_response_bytes: default_max_response_bytes(),
            max_sse_line_bytes: default_max_sse_line_bytes(),
            max_sse_event_bytes: default_max_sse_event_bytes(),
            max_content_items: default_max_content_items(),
            max_catalog_items: default_max_catalog_items(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpServerYaml {
    enabled: bool,
    #[serde(default = "default_server_endpoint")]
    endpoint: String,
    authorization: McpServerAuthorizationYaml,
    #[serde(default)]
    exports: McpExportsYaml,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum McpServerAuthorizationYaml {
    Disabled,
    BearerCompatible,
    OauthResourceServer {
        resource: String,
        authorization_servers: Vec<String>,
        required_scopes: Vec<String>,
    },
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpExportsYaml {
    #[serde(default)]
    agents: Vec<McpAgentExportYaml>,
    #[serde(default)]
    actions: Vec<McpActionExportYaml>,
    #[serde(default)]
    resources: Vec<McpResourceExportYaml>,
    #[serde(default)]
    prompts: Vec<McpPromptExportYaml>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpAgentExportYaml {
    agent: String,
    #[serde(rename = "as")]
    alias: String,
    execution: McpExportExecutionConfig,
    input_required: McpInteractionConfig,
    #[serde(default)]
    required_scope: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpActionExportYaml {
    action: String,
    #[serde(rename = "as")]
    alias: String,
    #[serde(default)]
    required_scope: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpResourceExportYaml {
    uri: String,
    name: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    mime_type: String,
    text: String,
    #[serde(default)]
    required_scope: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpPromptExportYaml {
    name: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    arguments: Vec<McpPromptArgumentExportYaml>,
    messages: Vec<McpPromptMessageExportYaml>,
    #[serde(default)]
    completions: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    required_scope: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpPromptArgumentExportYaml {
    name: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    required: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpPromptMessageExportYaml {
    role: McpPromptRoleConfig,
    text: String,
}

pub(crate) fn resolve_mcp(
    raw: Option<McpYaml>,
    config_parent: &Path,
    deployment_mode: DeploymentMode,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<McpConfig, PlatformConfigError> {
    let Some(raw) = raw else {
        return Ok(disabled_mcp_config());
    };
    if raw.version != 1 {
        return Err(mcp_error("mcp.version must be 1"));
    }
    if raw.protocol.preferred != MCP_MODERN_PROTOCOL_VERSION
        || raw
            .protocol
            .legacy_fallback
            .iter()
            .any(|version| version != MCP_LEGACY_PROTOCOL_VERSION)
        || has_duplicates(&raw.protocol.legacy_fallback)
    {
        return Err(mcp_error("mcp.protocol contains an unsupported version"));
    }
    if !raw.client.enabled && !raw.client.servers.is_empty() {
        return Err(mcp_error(
            "mcp.client.servers must be empty when the client is disabled",
        ));
    }
    let servers = raw
        .client
        .servers
        .into_iter()
        .map(|(server_id, server)| {
            validate_server_id(&server_id)?;
            resolve_client_server(&server_id, server, config_parent, deployment_mode, get_env)
                .map(|server| (server_id, server))
        })
        .collect::<Result<BTreeMap<_, _>, PlatformConfigError>>()?;
    let secret_encryption = raw
        .client
        .secret_encryption
        .map(|encryption| {
            let active_key_version = encryption.active_key_version.trim().to_owned();
            let keyring = required_secret(&encryption.keyring_env, get_env)?;
            insight_storage::mcp_secret::McpSecretEncryptionKeyring::from_secret_json(
                active_key_version.clone(),
                keyring.expose(),
            )
            .map_err(|_| mcp_error("MCP secret encryption keyring is invalid"))?;
            Ok(McpSecretEncryptionConfig {
                active_key_version,
                keyring,
            })
        })
        .transpose()?;
    let requires_secret_encryption = servers.values().any(|server| {
        matches!(
            server.authorization,
            McpClientAuthorizationConfig::OauthUser { .. }
        ) || server.imports.tools.iter().any(|tool| {
            tool.input_required == McpInteractionConfig::Allowed
                || tool.tasks == McpInteractionConfig::Allowed
                || tool.approval != McpApprovalConfig::Never
        })
    });
    if requires_secret_encryption && secret_encryption.is_none() {
        return Err(mcp_error(
            "MCP OAuth and interactive tools require client.secret_encryption",
        ));
    }
    let server = resolve_server(raw.server, deployment_mode)?;
    Ok(McpConfig {
        version: 1,
        protocol: McpProtocolConfig {
            preferred: raw.protocol.preferred,
            legacy_fallback: raw.protocol.legacy_fallback,
        },
        client: McpClientConfig {
            enabled: raw.client.enabled,
            servers,
            secret_encryption,
        },
        server,
    })
}

fn resolve_client_server(
    server_id: &str,
    raw: McpClientServerYaml,
    config_parent: &Path,
    deployment_mode: DeploymentMode,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<McpClientServerConfig, PlatformConfigError> {
    let transport = match raw.transport {
        McpClientTransportYaml::StreamableHttp {
            endpoint,
            allow_plaintext_loopback,
            connect_timeout,
            request_timeout,
        } => {
            validate_http_endpoint(&endpoint, allow_plaintext_loopback, deployment_mode)?;
            McpClientTransportConfig::StreamableHttp {
                endpoint,
                allow_plaintext_loopback,
                connect_timeout: positive_duration(
                    &connect_timeout,
                    &format!("mcp.client.servers.{server_id}.transport.connect_timeout"),
                )?,
                request_timeout: positive_duration(
                    &request_timeout,
                    &format!("mcp.client.servers.{server_id}.transport.request_timeout"),
                )?,
            }
        }
        McpClientTransportYaml::Stdio {
            executable,
            args,
            working_directory,
            environment,
            isolation,
            startup_timeout,
            request_timeout,
            shutdown_timeout,
        } => {
            if !executable.is_absolute()
                || args.iter().any(|value| {
                    value.is_empty()
                        || value.len() > 16 * 1024
                        || value.chars().any(char::is_control)
                })
                || (deployment_mode == DeploymentMode::Production && isolation.is_none())
            {
                return Err(mcp_error(
                    "MCP stdio requires an absolute executable, safe argv, and production isolation",
                ));
            }
            let environment = environment
                .into_iter()
                .map(|(name, reference)| {
                    if !valid_environment_name(&name) {
                        return Err(mcp_error("MCP stdio environment variable name is invalid"));
                    }
                    required_secret(&reference.secret_env, get_env)?;
                    Ok((name, reference.secret_env))
                })
                .collect::<Result<BTreeMap<_, _>, PlatformConfigError>>()?;
            McpClientTransportConfig::Stdio {
                executable,
                args,
                working_directory: resolve_path(config_parent, &working_directory),
                environment,
                isolation_profile: isolation.map(|value| value.profile),
                startup_timeout: positive_duration(
                    &startup_timeout,
                    &format!("mcp.client.servers.{server_id}.transport.startup_timeout"),
                )?,
                request_timeout: positive_duration(
                    &request_timeout,
                    &format!("mcp.client.servers.{server_id}.transport.request_timeout"),
                )?,
                shutdown_timeout: positive_duration(
                    &shutdown_timeout,
                    &format!("mcp.client.servers.{server_id}.transport.shutdown_timeout"),
                )?,
            }
        }
    };
    let authorization = match raw.authorization {
        McpClientAuthorizationYaml::None => McpClientAuthorizationConfig::None,
        McpClientAuthorizationYaml::BearerEnv { token_env } => {
            let token = required_secret(&token_env, get_env)?;
            McpClientAuthorizationConfig::BearerEnv {
                environment: token_env,
                token,
            }
        }
        McpClientAuthorizationYaml::OauthUser {
            scopes,
            client_id,
            redirect_uri,
        } => {
            let scopes = unique_nonempty(scopes, "MCP OAuth scope")?;
            insight_mcp::OAuthClientRegistration::new(
                client_id.clone(),
                redirect_uri.clone(),
                scopes.iter().cloned().collect(),
            )
            .map_err(|_| mcp_error("MCP OAuth client registration is invalid"))?;
            McpClientAuthorizationConfig::OauthUser {
                scopes,
                client_id,
                redirect_uri,
            }
        }
    };
    if matches!(transport, McpClientTransportConfig::Stdio { .. })
        && matches!(
            authorization,
            McpClientAuthorizationConfig::OauthUser { .. }
        )
    {
        return Err(mcp_error(
            "oauth_user is only valid for Streamable HTTP transports",
        ));
    }
    if matches!(raw.discovery, McpDiscoveryYaml::LiveServiceAccount)
        && matches!(
            authorization,
            McpClientAuthorizationConfig::OauthUser { .. }
        )
    {
        return Err(mcp_error(
            "oauth_user cannot be used for publication discovery",
        ));
    }
    let discovery = match raw.discovery {
        McpDiscoveryYaml::LiveServiceAccount => McpDiscoveryConfig::LiveServiceAccount,
        McpDiscoveryYaml::SignedManifest {
            path,
            trust_keys_env,
        } => McpDiscoveryConfig::SignedManifest {
            path: resolve_path(config_parent, &path),
            trust_keys: required_secret(&trust_keys_env, get_env)?,
            trust_keys_environment: trust_keys_env,
        },
    };
    let imports = resolve_imports(raw.imports)?;
    if matches!(
        authorization,
        McpClientAuthorizationConfig::OauthUser { .. }
    ) && imports
        .tools
        .iter()
        .any(|tool| tool.terminal_only_compatible)
    {
        return Err(mcp_error(
            "OAuth user MCP tools cannot be terminal-only compatible",
        ));
    }
    if matches!(
        authorization,
        McpClientAuthorizationConfig::OauthUser { .. }
    ) && imports
        .prompts
        .iter()
        .any(|prompt| prompt.definition_arguments.is_some())
    {
        return Err(mcp_error(
            "OAuth user MCP prompts cannot be frozen without an authoring credential",
        ));
    }
    let limits = resolve_limits(raw.limits)?;
    Ok(McpClientServerConfig {
        transport,
        discovery,
        authorization,
        imports,
        limits,
    })
}

fn resolve_imports(raw: McpImportsYaml) -> Result<McpImportConfig, PlatformConfigError> {
    let mut remote_names = BTreeSet::new();
    let mut aliases = BTreeSet::new();
    let tools = raw
        .tools
        .into_iter()
        .map(|tool| {
            if !valid_protocol_name(&tool.remote)
                || !valid_protocol_name(&tool.alias)
                || !remote_names.insert(tool.remote.clone())
                || !aliases.insert(tool.alias.clone())
                || tool
                    .title
                    .as_ref()
                    .is_some_and(|value| !valid_text(value, 512))
            {
                return Err(mcp_error(
                    "MCP tool import identity is invalid or duplicated",
                ));
            }
            let (description_source, description_override) = match tool.description {
                None | Some(McpDescriptionYaml::Source(McpDescriptionSourceConfig::Remote)) => {
                    (McpDescriptionSourceConfig::Remote, None)
                }
                Some(McpDescriptionYaml::Source(McpDescriptionSourceConfig::Disabled)) => {
                    (McpDescriptionSourceConfig::Disabled, None)
                }
                Some(McpDescriptionYaml::Override { r#override }) => {
                    if !valid_text(&r#override, 4 * 1024) {
                        return Err(mcp_error("MCP tool description override is invalid"));
                    }
                    (McpDescriptionSourceConfig::Disabled, Some(r#override))
                }
            };
            if tool.terminal_only_compatible
                && (tool.input_required == McpInteractionConfig::Allowed
                    || tool.tasks == McpInteractionConfig::Allowed
                    || tool.approval != McpApprovalConfig::Never)
            {
                return Err(mcp_error(
                    "terminal-only MCP tools cannot require interaction, approval, or Tasks",
                ));
            }
            Ok(McpToolImportConfig {
                remote_name: tool.remote,
                alias: tool.alias,
                title: tool.title,
                description_source,
                description_override,
                effect: tool.effect,
                idempotency: tool.idempotency,
                cancellation: tool.cancellation,
                required_capabilities: unique_nonempty(
                    tool.required_capabilities,
                    "MCP required capability",
                )?,
                approval: tool.approval,
                input_required: tool.input_required,
                tasks: tool.tasks,
                terminal_only_compatible: tool.terminal_only_compatible,
                public_call: tool.public.call,
            })
        })
        .collect::<Result<Vec<_>, PlatformConfigError>>()?;
    let mut prompt_names = BTreeSet::new();
    let prompts = raw
        .prompts
        .allow
        .into_iter()
        .map(|prompt| {
            let (remote_name, allow_user_invocation, definition_arguments) = match prompt {
                McpPromptImportYaml::Name(remote_name) => (remote_name, true, None),
                McpPromptImportYaml::Detailed(detail) => (
                    detail.remote,
                    detail.user_invocation,
                    detail.definition_arguments,
                ),
            };
            if !valid_protocol_name(&remote_name)
                || !prompt_names.insert(remote_name.clone())
                || (!allow_user_invocation && definition_arguments.is_none())
                || definition_arguments.as_ref().is_some_and(|arguments| {
                    arguments.len() > 128
                        || arguments.iter().any(|(name, value)| {
                            !valid_protocol_name(name)
                                || value.len() > 8 * 1024
                                || value.chars().any(char::is_control)
                        })
                })
            {
                return Err(mcp_error("MCP prompt import is invalid or duplicated"));
            }
            Ok(McpPromptImportConfig {
                remote_name,
                allow_user_invocation,
                definition_arguments,
            })
        })
        .collect::<Result<Vec<_>, PlatformConfigError>>()?;
    Ok(McpImportConfig {
        tools,
        resources: unique_nonempty(raw.resources.allow, "MCP resource import")?
            .into_iter()
            .collect(),
        prompts,
    })
}

fn resolve_limits(raw: McpLimitsYaml) -> Result<McpLimitsConfig, PlatformConfigError> {
    if raw.max_request_bytes == 0
        || raw.max_response_bytes == 0
        || raw.max_sse_line_bytes == 0
        || raw.max_sse_event_bytes == 0
        || raw.max_content_items == 0
        || raw.max_catalog_items == 0
        || raw.max_sse_line_bytes > raw.max_sse_event_bytes
        || raw.max_sse_event_bytes > raw.max_response_bytes
        || raw.max_request_bytes > 64 * 1024 * 1024
        || raw.max_response_bytes > 256 * 1024 * 1024
        || raw.max_catalog_items > 100_000
    {
        return Err(mcp_error("MCP limits are zero, inconsistent, or unbounded"));
    }
    Ok(McpLimitsConfig {
        max_request_bytes: raw.max_request_bytes,
        max_response_bytes: raw.max_response_bytes,
        max_sse_line_bytes: raw.max_sse_line_bytes,
        max_sse_event_bytes: raw.max_sse_event_bytes,
        max_content_items: raw.max_content_items,
        max_catalog_items: raw.max_catalog_items,
    })
}

fn resolve_server(
    raw: McpServerYaml,
    deployment_mode: DeploymentMode,
) -> Result<McpServerConfig, PlatformConfigError> {
    if !raw.endpoint.starts_with('/')
        || raw.endpoint.contains('?')
        || raw.endpoint.contains('#')
        || raw.endpoint.len() > 256
    {
        return Err(mcp_error(
            "MCP server endpoint must be a bounded absolute path",
        ));
    }
    let authorization = match raw.authorization {
        McpServerAuthorizationYaml::Disabled => {
            if raw.enabled && deployment_mode == DeploymentMode::Production {
                return Err(mcp_error(
                    "production MCP server cannot use disabled authorization",
                ));
            }
            McpServerAuthorizationConfig::Disabled
        }
        McpServerAuthorizationYaml::BearerCompatible => {
            McpServerAuthorizationConfig::BearerCompatible
        }
        McpServerAuthorizationYaml::OauthResourceServer {
            resource,
            authorization_servers,
            required_scopes,
        } => {
            validate_https_url(&resource)?;
            let required_scopes = unique_nonempty(required_scopes, "MCP server OAuth scope")?;
            if authorization_servers.is_empty()
                || has_duplicates(&authorization_servers)
                || required_scopes.is_empty()
                || required_scopes
                    .iter()
                    .any(|scope| !valid_oauth_scope(scope))
                || authorization_servers
                    .iter()
                    .any(|value| validate_https_url(value).is_err())
            {
                return Err(mcp_error("MCP authorization server metadata is invalid"));
            }
            McpServerAuthorizationConfig::OauthResourceServer {
                resource,
                authorization_servers,
                required_scopes,
            }
        }
    };
    let mut aliases = BTreeSet::new();
    let agents = raw
        .exports
        .agents
        .into_iter()
        .map(|export| {
            if !valid_protocol_name(&export.agent)
                || !valid_protocol_name(&export.alias)
                || !aliases.insert(export.alias.clone())
            {
                return Err(mcp_error("MCP Agent export is invalid or duplicated"));
            }
            Ok(McpAgentExportConfig {
                agent: export.agent,
                alias: export.alias,
                execution: export.execution,
                input_required: export.input_required,
                required_scope: validate_export_scope(
                    &authorization,
                    export.required_scope,
                    "MCP Agent export",
                )?,
            })
        })
        .collect::<Result<Vec<_>, PlatformConfigError>>()?;
    let actions = raw
        .exports
        .actions
        .into_iter()
        .map(|export| {
            if !valid_protocol_name(&export.action)
                || !valid_protocol_name(&export.alias)
                || !aliases.insert(export.alias.clone())
            {
                return Err(mcp_error("MCP Action export is invalid or duplicated"));
            }
            Ok(McpActionExportConfig {
                action: export.action,
                alias: export.alias,
                required_scope: validate_export_scope(
                    &authorization,
                    export.required_scope,
                    "MCP Action export",
                )?,
            })
        })
        .collect::<Result<Vec<_>, PlatformConfigError>>()?;
    let mut resource_uris = BTreeSet::new();
    let mut resource_names = BTreeSet::new();
    let resources = raw
        .exports
        .resources
        .into_iter()
        .map(|export| {
            let uri = Url::parse(&export.uri)
                .map_err(|_| mcp_error("MCP resource export URI is invalid"))?;
            if export.uri.len() > 8 * 1024
                || uri.scheme().is_empty()
                || matches!(uri.scheme(), "file" | "data" | "javascript")
                || uri.fragment().is_some()
                || !valid_protocol_name(&export.name)
                || !resource_uris.insert(export.uri.clone())
                || !resource_names.insert(export.name.clone())
                || !valid_optional_text(export.title.as_deref(), 256)
                || !valid_optional_text(export.description.as_deref(), 4 * 1024)
                || !valid_mime_type(&export.mime_type)
                || !valid_text(&export.text, 1024 * 1024)
            {
                return Err(mcp_error("MCP resource export is invalid or duplicated"));
            }
            Ok(McpResourceExportConfig {
                uri: export.uri,
                name: export.name,
                title: export.title,
                description: export.description,
                mime_type: export.mime_type,
                text: export.text,
                required_scope: validate_export_scope(
                    &authorization,
                    export.required_scope,
                    "MCP resource export",
                )?,
            })
        })
        .collect::<Result<Vec<_>, PlatformConfigError>>()?;
    let mut prompt_names = BTreeSet::new();
    let prompts = raw
        .exports
        .prompts
        .into_iter()
        .map(|export| {
            if !valid_protocol_name(&export.name)
                || !prompt_names.insert(export.name.clone())
                || !valid_optional_text(export.title.as_deref(), 256)
                || !valid_optional_text(export.description.as_deref(), 4 * 1024)
                || export.arguments.len() > 32
                || export.messages.is_empty()
                || export.messages.len() > 64
            {
                return Err(mcp_error("MCP prompt export is invalid or duplicated"));
            }
            let mut argument_names = BTreeSet::new();
            let arguments = export
                .arguments
                .into_iter()
                .map(|argument| {
                    if !valid_protocol_name(&argument.name)
                        || !argument_names.insert(argument.name.clone())
                        || !valid_optional_text(argument.title.as_deref(), 256)
                        || !valid_optional_text(argument.description.as_deref(), 4 * 1024)
                    {
                        return Err(mcp_error("MCP prompt argument is invalid or duplicated"));
                    }
                    Ok(McpPromptArgumentExportConfig {
                        name: argument.name,
                        title: argument.title,
                        description: argument.description,
                        required: argument.required,
                    })
                })
                .collect::<Result<Vec<_>, PlatformConfigError>>()?;
            let messages = export
                .messages
                .into_iter()
                .map(|message| {
                    if !valid_text(&message.text, 64 * 1024)
                        || !valid_prompt_template(&message.text, &argument_names)
                    {
                        return Err(mcp_error("MCP prompt message template is invalid"));
                    }
                    Ok(McpPromptMessageExportConfig {
                        role: message.role,
                        text: message.text,
                    })
                })
                .collect::<Result<Vec<_>, PlatformConfigError>>()?;
            if export.completions.len() > argument_names.len()
                || export.completions.iter().any(|(name, values)| {
                    !argument_names.contains(name)
                        || values.len() > 100
                        || values.is_empty()
                        || values.iter().any(|value| !valid_text(value, 512))
                        || has_duplicates(values)
                })
            {
                return Err(mcp_error("MCP prompt completions are invalid"));
            }
            Ok(McpPromptExportConfig {
                name: export.name,
                title: export.title,
                description: export.description,
                arguments,
                messages,
                completions: export.completions,
                required_scope: validate_export_scope(
                    &authorization,
                    export.required_scope,
                    "MCP prompt export",
                )?,
            })
        })
        .collect::<Result<Vec<_>, PlatformConfigError>>()?;
    Ok(McpServerConfig {
        enabled: raw.enabled,
        endpoint: raw.endpoint,
        authorization,
        exports: McpExportConfig {
            agents,
            actions,
            resources,
            prompts,
        },
    })
}

fn disabled_mcp_config() -> McpConfig {
    McpConfig {
        version: 1,
        protocol: McpProtocolConfig {
            preferred: MCP_MODERN_PROTOCOL_VERSION.to_owned(),
            legacy_fallback: Vec::new(),
        },
        client: McpClientConfig {
            enabled: false,
            servers: BTreeMap::new(),
            secret_encryption: None,
        },
        server: McpServerConfig {
            enabled: false,
            endpoint: default_server_endpoint(),
            authorization: McpServerAuthorizationConfig::Disabled,
            exports: McpExportConfig::default(),
        },
    }
}

fn validate_server_id(value: &str) -> Result<(), PlatformConfigError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || value.starts_with('-')
        || value.ends_with('-')
    {
        return Err(mcp_error("MCP server_id is invalid"));
    }
    Ok(())
}

fn validate_http_endpoint(
    value: &str,
    allow_plaintext_loopback: bool,
    deployment_mode: DeploymentMode,
) -> Result<(), PlatformConfigError> {
    let url = Url::parse(value).map_err(|_| mcp_error("MCP HTTP endpoint is invalid"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
    {
        return Err(mcp_error(
            "MCP HTTP endpoint cannot contain userinfo, query, or fragment",
        ));
    }
    match url.scheme() {
        "https" => Ok(()),
        "http"
            if deployment_mode == DeploymentMode::SingleProcessDevelopment
                && allow_plaintext_loopback
                && url.host_str().is_some_and(is_loopback_host) =>
        {
            Ok(())
        }
        _ => Err(mcp_error(
            "MCP HTTP endpoint must use HTTPS except explicit development loopback",
        )),
    }
}

fn validate_https_url(value: &str) -> Result<(), PlatformConfigError> {
    let url = Url::parse(value).map_err(|_| mcp_error("MCP HTTPS URL is invalid"))?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(mcp_error("MCP URL must be a credential-free HTTPS URL"));
    }
    Ok(())
}

fn is_loopback_host(value: &str) -> bool {
    value == "localhost"
        || value
            .parse::<std::net::IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false)
}

fn valid_protocol_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn validate_export_scope(
    authorization: &McpServerAuthorizationConfig,
    required_scope: Option<String>,
    field: &str,
) -> Result<Option<String>, PlatformConfigError> {
    match (authorization, required_scope) {
        (
            McpServerAuthorizationConfig::OauthResourceServer {
                required_scopes, ..
            },
            Some(scope),
        ) if valid_oauth_scope(&scope) && required_scopes.contains(&scope) => Ok(Some(scope)),
        (McpServerAuthorizationConfig::OauthResourceServer { .. }, _) => Err(mcp_error(format!(
            "{field} must declare one configured OAuth required_scope"
        ))),
        (_, None) => Ok(None),
        (_, Some(_)) => Err(mcp_error(format!(
            "{field} required_scope needs OAuth resource-server authorization"
        ))),
    }
}

fn valid_optional_text(value: Option<&str>, max_bytes: usize) -> bool {
    value.is_none_or(|value| valid_text(value, max_bytes))
}

fn valid_mime_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 127
        && value.is_ascii()
        && value.contains('/')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'!' | b'#'..=b'~'))
        && !value.chars().any(char::is_whitespace)
}

fn valid_prompt_template(value: &str, arguments: &BTreeSet<String>) -> bool {
    let mut remaining = value;
    while let Some(start) = remaining.find("{{") {
        let Some(end) = remaining[start + 2..].find("}}") else {
            return false;
        };
        let name = &remaining[start + 2..start + 2 + end];
        if !arguments.contains(name) {
            return false;
        }
        remaining = &remaining[start + 2 + end + 2..];
    }
    !remaining.contains("}}")
}

fn valid_environment_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_uppercase() || (index > 0 && byte.is_ascii_digit())
        })
}

fn valid_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && !value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
}

fn unique_nonempty(
    values: Vec<String>,
    field: &str,
) -> Result<BTreeSet<String>, PlatformConfigError> {
    let mut unique = BTreeSet::new();
    for value in values {
        if !valid_text(&value, 512) || !unique.insert(value) {
            return Err(mcp_error(format!(
                "{field} is empty, invalid, or duplicated"
            )));
        }
    }
    Ok(unique)
}

fn has_duplicates(values: &[String]) -> bool {
    values.iter().collect::<BTreeSet<_>>().len() != values.len()
}

fn valid_oauth_scope(value: &str) -> bool {
    value.len() <= 256
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
}

fn mcp_error(message: impl Into<String>) -> PlatformConfigError {
    PlatformConfigError::new("PLATFORM_MCP_INVALID", message)
}

fn default_connect_timeout() -> String {
    "5s".to_owned()
}

fn default_startup_timeout() -> String {
    "10s".to_owned()
}

fn default_request_timeout() -> String {
    "2m".to_owned()
}

fn default_shutdown_timeout() -> String {
    "10s".to_owned()
}

fn default_server_endpoint() -> String {
    "/mcp".to_owned()
}

const fn default_true() -> bool {
    true
}

const fn default_max_request_bytes() -> usize {
    1024 * 1024
}

const fn default_max_response_bytes() -> usize {
    16 * 1024 * 1024
}

const fn default_max_sse_line_bytes() -> usize {
    64 * 1024
}

const fn default_max_sse_event_bytes() -> usize {
    1024 * 1024
}

const fn default_max_content_items() -> usize {
    128
}

const fn default_max_catalog_items() -> usize {
    4_096
}
