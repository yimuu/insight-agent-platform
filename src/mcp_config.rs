use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use base64::{engine::general_purpose, Engine as _};
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
    pub management_api: McpManagementApiConfig,
    pub secret_encryption: Option<McpSecretEncryptionConfig>,
    pub secret_resolver: McpSecretResolverConfig,
    pub signed_manifest_trust: McpSignedManifestTrustConfig,
    pub network_policy: McpNetworkPolicyConfig,
    pub stdio_launch_profiles: BTreeMap<String, McpStdioLaunchProfileConfig>,
    pub default_limits: McpClientDefaultLimitsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpManagementApiConfig {
    pub enabled: bool,
    pub discovery_workers: u32,
    pub max_pending_discoveries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct McpSecretResolverConfig {
    pub allowed_names: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSignedManifestTrustConfig {
    pub max_validity: Duration,
    pub trusted_signers: BTreeMap<String, McpManifestTrustedSignerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpManifestTrustedSignerConfig {
    pub key_id: String,
    pub algorithm: String,
    pub public_key_file: PathBuf,
    pub public_key: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpNetworkPolicyConfig {
    pub allow_loopback_development: bool,
    pub allow_private_networks: bool,
    pub allow_redirects: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpStdioLaunchProfileConfig {
    pub executable: PathBuf,
    pub fixed_args: Vec<String>,
    pub working_directory: PathBuf,
    pub allowed_parameters: BTreeSet<String>,
    pub secret_environment: BTreeMap<String, String>,
    pub isolation_profile: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpClientDefaultLimitsConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub limits: McpLimitsConfig,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpInteractionConfig {
    Denied,
    Allowed,
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
    management_api: McpManagementApiYaml,
    #[serde(default)]
    secret_encryption: Option<McpSecretEncryptionYaml>,
    #[serde(default)]
    secret_resolver: McpSecretResolverYaml,
    #[serde(default)]
    signed_manifest_trust: McpSignedManifestTrustYaml,
    #[serde(default)]
    network_policy: McpNetworkPolicyYaml,
    #[serde(default)]
    stdio_launch_profiles: BTreeMap<String, McpStdioLaunchProfileYaml>,
    #[serde(default)]
    default_limits: McpClientDefaultLimitsYaml,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpManagementApiYaml {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_discovery_workers")]
    discovery_workers: u32,
    #[serde(default = "default_max_pending_discoveries")]
    max_pending_discoveries: u32,
}

impl Default for McpManagementApiYaml {
    fn default() -> Self {
        Self {
            enabled: false,
            discovery_workers: default_discovery_workers(),
            max_pending_discoveries: default_max_pending_discoveries(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum McpSecretResolverYaml {
    #[default]
    Disabled,
    EnvironmentReference {
        #[serde(default)]
        allowed_names: Vec<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpSignedManifestTrustYaml {
    #[serde(default = "default_manifest_max_validity")]
    max_validity: String,
    #[serde(default)]
    trusted_signers: Vec<McpManifestTrustedSignerYaml>,
}

impl Default for McpSignedManifestTrustYaml {
    fn default() -> Self {
        Self {
            max_validity: default_manifest_max_validity(),
            trusted_signers: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpManifestTrustedSignerYaml {
    key_id: String,
    algorithm: String,
    public_key_file: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpNetworkPolicyYaml {
    #[serde(default)]
    allow_loopback_development: bool,
    #[serde(default)]
    allow_private_networks: bool,
    #[serde(default)]
    allow_redirects: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpStdioLaunchProfileYaml {
    executable: PathBuf,
    #[serde(default)]
    fixed_args: Vec<String>,
    working_directory: PathBuf,
    #[serde(default)]
    allowed_parameters: Vec<String>,
    #[serde(default)]
    secret_environment: BTreeMap<String, String>,
    isolation: McpIsolationYaml,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpClientDefaultLimitsYaml {
    #[serde(default = "default_connect_timeout")]
    connect_timeout: String,
    #[serde(default = "default_request_timeout")]
    request_timeout: String,
    #[serde(flatten)]
    limits: McpLimitsYaml,
}

impl Default for McpClientDefaultLimitsYaml {
    fn default() -> Self {
        Self {
            connect_timeout: default_connect_timeout(),
            request_timeout: default_request_timeout(),
            limits: McpLimitsYaml::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpSecretEncryptionYaml {
    active_key_version: String,
    keyring_env: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpIsolationYaml {
    profile: String,
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
    if raw.version != 2 {
        return Err(mcp_error("mcp.version must be 2"));
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
    if !raw.client.enabled && raw.client.management_api.enabled {
        return Err(mcp_error(
            "mcp.client.management_api must be disabled when the client is disabled",
        ));
    }
    if raw.client.management_api.discovery_workers == 0
        || raw.client.management_api.discovery_workers > 64
        || raw.client.management_api.max_pending_discoveries == 0
        || raw.client.management_api.max_pending_discoveries > 10_000
    {
        return Err(mcp_error("MCP management worker limits are invalid"));
    }
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
    if raw.client.management_api.enabled && secret_encryption.is_none() {
        return Err(mcp_error(
            "enabled MCP management API requires client.secret_encryption",
        ));
    }

    let secret_resolver = match raw.client.secret_resolver {
        McpSecretResolverYaml::Disabled => McpSecretResolverConfig::default(),
        McpSecretResolverYaml::EnvironmentReference { allowed_names } => {
            let allowed_names = unique_nonempty(allowed_names, "MCP secret resolver name")?;
            if allowed_names
                .iter()
                .any(|name| !valid_environment_name(name))
            {
                return Err(mcp_error("MCP secret resolver name is invalid"));
            }
            McpSecretResolverConfig { allowed_names }
        }
    };
    if raw.client.management_api.enabled && secret_resolver.allowed_names.is_empty() {
        return Err(mcp_error(
            "enabled MCP management API requires a non-empty secret resolver policy",
        ));
    }

    let max_validity = positive_duration(
        &raw.client.signed_manifest_trust.max_validity,
        "mcp.client.signed_manifest_trust.max_validity",
    )?;
    if max_validity > Duration::from_secs(30 * 24 * 60 * 60) {
        return Err(mcp_error(
            "MCP signed manifest maximum validity is unbounded",
        ));
    }
    let mut trusted_signers = BTreeMap::new();
    for signer in raw.client.signed_manifest_trust.trusted_signers {
        let key_id = signer.key_id.trim().to_owned();
        if !valid_protocol_name(&key_id)
            || signer.algorithm != "ed25519"
            || trusted_signers.contains_key(&key_id)
        {
            return Err(mcp_error(
                "MCP signed manifest signer is invalid or duplicated",
            ));
        }
        let public_key_file = resolve_path(config_parent, &signer.public_key_file);
        if deployment_mode == DeploymentMode::Production && !public_key_file.is_absolute() {
            return Err(mcp_error("production MCP signer key path must be absolute"));
        }
        let encoded = fs::read(&public_key_file)
            .map_err(|_| mcp_error("MCP signed manifest public key cannot be read"))?;
        let public_key = decode_ed25519_public_key(&encoded)?;
        trusted_signers.insert(
            key_id.clone(),
            McpManifestTrustedSignerConfig {
                key_id,
                algorithm: signer.algorithm,
                public_key_file,
                public_key,
            },
        );
    }

    let mut stdio_launch_profiles = BTreeMap::new();
    for (profile_id, profile) in raw.client.stdio_launch_profiles {
        if !valid_protocol_name(&profile_id)
            || !profile.executable.is_absolute()
            || profile.fixed_args.len() > 128
            || profile.fixed_args.iter().any(|value| !valid_argv(value))
            || profile.isolation.profile.trim().is_empty()
        {
            return Err(mcp_error("MCP stdio launch profile is invalid"));
        }
        let allowed_parameters =
            unique_nonempty(profile.allowed_parameters, "MCP stdio allowed parameter")?;
        if allowed_parameters
            .iter()
            .any(|value| !valid_protocol_name(value))
        {
            return Err(mcp_error("MCP stdio allowed parameter is invalid"));
        }
        let working_directory = resolve_path(config_parent, &profile.working_directory);
        if deployment_mode == DeploymentMode::Production && !working_directory.is_absolute() {
            return Err(mcp_error(
                "production MCP stdio working directory must be absolute",
            ));
        }
        for (environment, secret_ref) in &profile.secret_environment {
            if !valid_environment_name(environment)
                || !valid_environment_name(secret_ref)
                || !secret_resolver.allowed_names.contains(secret_ref)
            {
                return Err(mcp_error(
                    "MCP stdio secret environment is outside the resolver policy",
                ));
            }
        }
        stdio_launch_profiles.insert(
            profile_id,
            McpStdioLaunchProfileConfig {
                executable: profile.executable,
                fixed_args: profile.fixed_args,
                working_directory,
                allowed_parameters,
                secret_environment: profile.secret_environment,
                isolation_profile: profile.isolation.profile,
            },
        );
    }
    let default_limits = McpClientDefaultLimitsConfig {
        connect_timeout: positive_duration(
            &raw.client.default_limits.connect_timeout,
            "mcp.client.default_limits.connect_timeout",
        )?,
        request_timeout: positive_duration(
            &raw.client.default_limits.request_timeout,
            "mcp.client.default_limits.request_timeout",
        )?,
        limits: resolve_limits(raw.client.default_limits.limits)?,
    };
    if raw.client.network_policy.allow_loopback_development
        && deployment_mode != DeploymentMode::SingleProcessDevelopment
    {
        return Err(mcp_error(
            "MCP plaintext loopback is limited to single-process development",
        ));
    }
    if raw.client.network_policy.allow_private_networks || raw.client.network_policy.allow_redirects
    {
        return Err(mcp_error(
            "MCP private-network and redirect exceptions are not supported",
        ));
    }
    let server = resolve_server(raw.server, deployment_mode)?;
    Ok(McpConfig {
        version: 2,
        protocol: McpProtocolConfig {
            preferred: raw.protocol.preferred,
            legacy_fallback: raw.protocol.legacy_fallback,
        },
        client: McpClientConfig {
            enabled: raw.client.enabled,
            management_api: McpManagementApiConfig {
                enabled: raw.client.management_api.enabled,
                discovery_workers: raw.client.management_api.discovery_workers,
                max_pending_discoveries: raw.client.management_api.max_pending_discoveries,
            },
            secret_encryption,
            secret_resolver,
            signed_manifest_trust: McpSignedManifestTrustConfig {
                max_validity,
                trusted_signers,
            },
            network_policy: McpNetworkPolicyConfig {
                allow_loopback_development: raw.client.network_policy.allow_loopback_development,
                allow_private_networks: raw.client.network_policy.allow_private_networks,
                allow_redirects: raw.client.network_policy.allow_redirects,
            },
            stdio_launch_profiles,
            default_limits,
        },
        server,
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
        version: 2,
        protocol: McpProtocolConfig {
            preferred: MCP_MODERN_PROTOCOL_VERSION.to_owned(),
            legacy_fallback: Vec::new(),
        },
        client: McpClientConfig {
            enabled: false,
            management_api: McpManagementApiConfig {
                enabled: false,
                discovery_workers: default_discovery_workers(),
                max_pending_discoveries: default_max_pending_discoveries(),
            },
            secret_encryption: None,
            secret_resolver: McpSecretResolverConfig::default(),
            signed_manifest_trust: McpSignedManifestTrustConfig {
                max_validity: Duration::from_secs(24 * 60 * 60),
                trusted_signers: BTreeMap::new(),
            },
            network_policy: McpNetworkPolicyConfig {
                allow_loopback_development: false,
                allow_private_networks: false,
                allow_redirects: false,
            },
            stdio_launch_profiles: BTreeMap::new(),
            default_limits: McpClientDefaultLimitsConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(120),
                limits: McpLimitsConfig {
                    max_request_bytes: default_max_request_bytes(),
                    max_response_bytes: default_max_response_bytes(),
                    max_sse_line_bytes: default_max_sse_line_bytes(),
                    max_sse_event_bytes: default_max_sse_event_bytes(),
                    max_content_items: default_max_content_items(),
                    max_catalog_items: default_max_catalog_items(),
                },
            },
        },
        server: McpServerConfig {
            enabled: false,
            endpoint: default_server_endpoint(),
            authorization: McpServerAuthorizationConfig::Disabled,
            exports: McpExportConfig::default(),
        },
    }
}

#[cfg(any())]
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

#[cfg(any())]
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

#[cfg(any())]
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

fn valid_argv(value: &str) -> bool {
    !value.is_empty() && value.len() <= 16 * 1024 && !value.chars().any(char::is_control)
}

fn decode_ed25519_public_key(encoded: &[u8]) -> Result<Vec<u8>, PlatformConfigError> {
    if encoded.len() == 32 {
        return Ok(encoded.to_vec());
    }
    let text = std::str::from_utf8(encoded)
        .map_err(|_| mcp_error("MCP Ed25519 public key is not valid UTF-8 or raw bytes"))?
        .trim();
    let decoded = general_purpose::STANDARD
        .decode(text)
        .or_else(|_| general_purpose::URL_SAFE_NO_PAD.decode(text))
        .map_err(|_| mcp_error("MCP Ed25519 public key must be raw or base64"))?;
    if decoded.len() != 32 {
        return Err(mcp_error("MCP Ed25519 public key must contain 32 bytes"));
    }
    Ok(decoded)
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

const fn default_discovery_workers() -> u32 {
    4
}

const fn default_max_pending_discoveries() -> u32 {
    128
}

fn default_manifest_max_validity() -> String {
    "24h".to_owned()
}

#[cfg(any())]
fn default_startup_timeout() -> String {
    "10s".to_owned()
}

fn default_request_timeout() -> String {
    "2m".to_owned()
}

#[cfg(any())]
fn default_shutdown_timeout() -> String {
    "10s".to_owned()
}

fn default_server_endpoint() -> String {
    "/mcp".to_owned()
}

#[cfg(any())]
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
