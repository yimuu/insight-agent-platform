//! Strict platform configuration for the durable runtime.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use insight_engine::PersistenceMode;
use insight_storage::postgres_config::PostgresHistoryUrlError;
use serde::Deserialize;

#[path = "mcp_config.rs"]
mod mcp_config;
pub use mcp_config::*;

#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("REDACTED")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthConfig {
    Disabled,
    Bearer { token: SecretString },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementOperatorCredentialConfig {
    identity: String,
    token: SecretString,
    capabilities: BTreeSet<String>,
}

impl ManagementOperatorCredentialConfig {
    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn token(&self) -> &SecretString {
        &self.token
    }

    pub fn capabilities(&self) -> &BTreeSet<String> {
        &self.capabilities
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebugExecutionProfileMode {
    Sandbox,
    Live,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugExecutionProfileConfig {
    pub mode: DebugExecutionProfileMode,
    pub max_concurrent_sessions: u32,
    pub session_timeout: Duration,
    pub retention: Duration,
    pub allow_external_actions: bool,
    pub allow_live_provider_credentials: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementLimitsConfig {
    pub max_agent_draft_bytes: usize,
    pub max_agent_prompt_files: usize,
    pub max_provider_models: usize,
    pub max_pending_operations: u32,
    pub operation_retention_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementConfig {
    pub enabled: bool,
    pub operator_credentials: Vec<ManagementOperatorCredentialConfig>,
    pub provider_secret_resolver_allowed_names: BTreeSet<String>,
    pub limits: ManagementLimitsConfig,
    pub debug_execution_profiles: BTreeMap<String, DebugExecutionProfileConfig>,
}

impl AuthConfig {
    pub fn bearer_token(&self) -> Option<&str> {
        match self {
            Self::Disabled => None,
            Self::Bearer { token } => Some(token.expose()),
        }
    }
}

/// One environment-backed credential accepted by the HumanTask API.
///
/// The bearer token is deliberately retained as [`SecretString`], so derived
/// platform configuration can be inspected without disclosing credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HumanTaskCredentialConfig {
    identity: String,
    groups: BTreeSet<String>,
    token: SecretString,
}

impl HumanTaskCredentialConfig {
    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn groups(&self) -> &BTreeSet<String> {
        &self.groups
    }

    pub fn token(&self) -> &SecretString {
        &self.token
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentsConfig {
    pub directory: PathBuf,
    pub enabled: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ModelInputModality {
    Text,
    Image,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderModelProfileConfig {
    pub input: BTreeSet<ModelInputModality>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderTransportPolicy {
    HttpsOnly,
    AllowLoopbackHttp,
    AllowTrustedPrivateHttp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderExtensionSource {
    Extends { provider: String },
    OpenAiCompatible,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderExtensionConfig {
    pub source: ProviderExtensionSource,
    pub endpoint: Option<String>,
    pub credential_env: Option<String>,
    pub models: BTreeMap<String, ProviderModelProfileConfig>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub transport: ProviderTransportPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ModelSelectorConfig {
    pub provider: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModelPolicyConfig {
    pub allow: BTreeSet<ModelSelectorConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProvidersConfig {
    pub extensions: BTreeMap<String, ProviderExtensionConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpGetResourceConfig {
    pub timeout: Duration,
    pub max_bytes: usize,
    pub allowlist: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionsConfig {
    pub enabled: BTreeSet<String>,
    pub http_get: Option<HttpGetResourceConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryConfig {
    Sqlite {
        path: PathBuf,
    },
    Postgres {
        database_url: SecretString,
        max_connections: u32,
    },
}

/// Declares the durability and ownership contract expected by this process.
///
/// SQLite remains useful for a single local process, but it is deliberately
/// not presented as a production or multi-runtime store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    Production,
    SingleProcessDevelopment,
}

/// Transport used for live-only response publications.
///
/// This is deliberately separate from the durable history store: response
/// deltas are bounded observations and never become recovery authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveRunStreamBrokerProvider {
    InProcess,
    PostgresNotify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunStreamConfig {
    pub broker: LiveRunStreamBrokerProvider,
    pub body_queue_capacity: usize,
    pub control_queue_capacity: usize,
    pub max_frame_bytes: usize,
    pub max_item_bytes: usize,
    pub max_run_bytes: usize,
    pub terminal_barrier_timeout: Duration,
    pub outbound_write_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStoreProvider {
    LocalFilesystem,
    SharedFilesystem,
}

/// External object-store policy for large durable worker values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactsConfig {
    pub provider: ArtifactStoreProvider,
    pub namespace: Option<String>,
    pub directory: PathBuf,
    pub inline_threshold_bytes: usize,
    pub tenant_encryption: Option<TenantArtifactEncryptionConfig>,
    /// Maximum bytes returned by one authorized Artifact read.
    pub max_read_bytes: usize,
    pub orphan_retention: Duration,
    /// Audit/recovery hold for Artifacts that became part of a durable Run.
    pub reference_retention: Duration,
    pub gc_interval: Duration,
    pub deletion_claim_seconds: u32,
}

/// Secret-backed, versioned tenant keyring for scoped Artifact envelopes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantArtifactEncryptionConfig {
    pub active_key_version: String,
    pub keyring: SecretString,
}

impl HistoryConfig {
    pub fn database_url(&self) -> Option<&str> {
        match self {
            Self::Sqlite { .. } => None,
            Self::Postgres { database_url, .. } => Some(database_url.expose()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub active_poll_interval: Duration,
    pub idle_poll_min_interval: Duration,
    pub idle_poll_max_interval: Duration,
    pub safety_poll_interval: Duration,
    pub claim_batch_size: u32,
    pub notification_reconnect_interval: Duration,
}

/// Process-level settings for the best-effort terminal-only execution path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalOnlyRuntimeConfig {
    pub enabled: bool,
    pub owner_lease: Duration,
    pub owner_heartbeat: Duration,
    pub terminal_commit_retry: Duration,
    pub run_retention_days: u32,
    pub allow_volatile_waits: bool,
    pub max_concurrent_runs: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Fallback for author documents that do not declare an immutable
    /// Deployment Revision persistence policy. This remains `full` until a
    /// separate rollout decision changes it.
    pub default_persistence_mode: PersistenceMode,
    pub terminal_only: TerminalOnlyRuntimeConfig,
    pub max_concurrent_runs: usize,
    pub max_concurrent_operations: usize,
    pub max_concurrent_operations_per_run: usize,
    pub operation_timeout: Duration,
    pub run_timeout: Duration,
    pub sse_keep_alive_interval: Duration,
    pub subscriber_capacity: usize,
    pub scheduler: SchedulerConfig,
    pub run_stream: RunStreamConfig,
    /// Platform hard bounds. Author-level LLM tool budgets must not exceed
    /// these values when a Deployment Revision is linked.
    pub max_llm_tool_rounds: u32,
    pub max_llm_tool_calls: u32,
    /// Retention of already-published nonterminal public events. Terminal
    /// public events remain durable and are not governed by this setting.
    pub public_event_retention: Duration,
    pub public_event_prune_interval: Duration,
    pub readiness_probe_timeout: Duration,
    pub shutdown_grace_period: Duration,
    pub shutdown_hard_deadline: Duration,
}

/// Persistent Conversation storage and context-selection policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConversationsConfig {
    pub enabled: bool,
    pub inline_content_max_bytes: usize,
    pub message_page_size_default: usize,
    pub message_page_size_max: usize,
    pub summary_trigger_messages: usize,
    pub summary_trigger_tokens: usize,
    pub recent_context_messages: usize,
    pub retention_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformConfig {
    pub deployment_mode: DeploymentMode,
    pub bind_addr: SocketAddr,
    pub auth: AuthConfig,
    pub human_task_credentials: Vec<HumanTaskCredentialConfig>,
    pub management: ManagementConfig,
    pub agents: AgentsConfig,
    pub providers: ProvidersConfig,
    pub model_policy: Option<ModelPolicyConfig>,
    pub actions: ActionsConfig,
    pub history: HistoryConfig,
    pub artifacts: ArtifactsConfig,
    pub runtime: RuntimeConfig,
    pub conversations: ConversationsConfig,
    pub mcp: McpConfig,
}

impl PlatformConfig {
    pub fn from_env() -> Result<Self, PlatformConfigError> {
        let path = env::var("PLATFORM_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("config/platform.yaml"));
        Self::load_with_env(&path, |name| env::var(name).ok())
    }

    pub fn load(path: &Path) -> Result<Self, PlatformConfigError> {
        Self::load_with_env(path, |name| env::var(name).ok())
    }

    pub fn load_with_env(
        path: &Path,
        get_env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, PlatformConfigError> {
        let path = absolute_path(path)?;
        if !path.is_file() {
            return Err(PlatformConfigError::new(
                "PLATFORM_CONFIG_NOT_FOUND",
                format!("platform config '{}' was not found", path.display()),
            ));
        }
        let yaml = fs::read_to_string(&path).map_err(|_| {
            PlatformConfigError::new(
                "PLATFORM_CONFIG_READ_FAILED",
                format!("failed to read platform config '{}'", path.display()),
            )
        })?;
        let mut raw: PlatformYaml =
            crate::yaml::from_str(&yaml, crate::yaml::YamlSurface::Platform).map_err(|error| {
                PlatformConfigError::new(
                    "PLATFORM_CONFIG_INVALID",
                    format!("invalid platform config: {error}"),
                )
            })?;
        if raw.version != 1 {
            return Err(PlatformConfigError::new(
                "PLATFORM_VERSION_UNSUPPORTED",
                format!(
                    "unsupported platform config version {}; expected 1",
                    raw.version
                ),
            ));
        }
        apply_persistence_environment(&mut raw, &get_env)?;
        let parent = path.parent().ok_or_else(|| {
            PlatformConfigError::new(
                "PLATFORM_CONFIG_INVALID",
                "platform config has no parent directory",
            )
        })?;
        let bind_addr = raw.bind_addr.parse().map_err(|_| {
            PlatformConfigError::new(
                "PLATFORM_CONFIG_INVALID",
                "bind_addr must be a valid socket address",
            )
        })?;
        let (auth, human_task_credentials) = resolve_auth(raw.auth, &get_env)?;
        let management = resolve_management(raw.management, raw.deployment_mode, &get_env)?;
        let agents = AgentsConfig {
            directory: resolve_path(parent, &raw.agents.directory),
            enabled: unique_names(raw.agents.enabled, "agents.enabled")?,
        };
        let providers = resolve_providers(raw.providers)?;
        let model_policy = resolve_model_policy(raw.model_policy)?;
        let actions = resolve_actions(raw.actions)?;
        let history = resolve_history(parent, raw.history, &get_env)?;
        if raw.deployment_mode == DeploymentMode::Production
            && matches!(history, HistoryConfig::Sqlite { .. })
        {
            return Err(PlatformConfigError::new(
                "PLATFORM_PRODUCTION_REQUIRES_POSTGRES",
                "deployment_mode=production requires history.provider=postgres",
            ));
        }
        let artifacts = resolve_artifacts(parent, raw.artifacts, raw.deployment_mode, &get_env)?;
        let runtime = resolve_runtime(raw.runtime, raw.deployment_mode)?;
        let conversations = resolve_conversations(raw.conversations)?;
        let mcp = resolve_mcp(raw.mcp, parent, raw.deployment_mode, &get_env)?;
        if mcp.client.management_api.enabled && !management.enabled {
            return Err(PlatformConfigError::new(
                "PLATFORM_MANAGEMENT_REQUIRED",
                "MCP management routes require management.enabled=true",
            ));
        }
        Ok(Self {
            deployment_mode: raw.deployment_mode,
            bind_addr,
            auth,
            human_task_credentials,
            management,
            agents,
            providers,
            model_policy,
            actions,
            history,
            artifacts,
            runtime,
            conversations,
            mcp,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformConfigError {
    code: &'static str,
    message: String,
}

impl PlatformConfigError {
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

impl std::fmt::Display for PlatformConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PlatformConfigError {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlatformYaml {
    version: u32,
    deployment_mode: DeploymentMode,
    bind_addr: String,
    auth: AuthYaml,
    #[serde(default)]
    management: Option<ManagementYaml>,
    agents: AgentsYaml,
    #[serde(default)]
    providers: BTreeMap<String, ProviderExtensionYaml>,
    #[serde(default)]
    model_policy: Option<ModelPolicyYaml>,
    actions: ActionsYaml,
    history: HistoryYaml,
    #[serde(default)]
    artifacts: Option<ArtifactsYaml>,
    runtime: RuntimeYaml,
    #[serde(default)]
    conversations: Option<ConversationsYaml>,
    #[serde(default)]
    mcp: Option<McpYaml>,
}

impl<'de> Deserialize<'de> for DeploymentMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match String::deserialize(deserializer)?.as_str() {
            "production" => Ok(Self::Production),
            "single_process_development" => Ok(Self::SingleProcessDevelopment),
            _ => Err(serde::de::Error::custom(
                "deployment_mode must be production or single_process_development",
            )),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum AuthYaml {
    Disabled {
        #[serde(default)]
        human_task_credentials: Vec<HumanTaskCredentialYaml>,
    },
    BearerEnv {
        token_env: String,
        #[serde(default)]
        human_task_credentials: Vec<HumanTaskCredentialYaml>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagementYaml {
    version: u32,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    operator_credentials: Vec<ManagementOperatorCredentialYaml>,
    #[serde(default)]
    provider_secret_resolver: ProviderSecretResolverYaml,
    #[serde(default)]
    limits: Option<ManagementLimitsYaml>,
    #[serde(default)]
    debug_execution_profiles: BTreeMap<String, DebugExecutionProfileYaml>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ProviderSecretResolverYaml {
    #[default]
    Disabled,
    EnvironmentReference {
        allowed_names: Vec<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagementOperatorCredentialYaml {
    identity: String,
    token_env: String,
    capabilities: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagementLimitsYaml {
    #[serde(default = "default_max_agent_draft_bytes")]
    max_agent_draft_bytes: usize,
    #[serde(default = "default_max_agent_prompt_files")]
    max_agent_prompt_files: usize,
    #[serde(default = "default_max_provider_models")]
    max_provider_models: usize,
    #[serde(default = "default_max_pending_operations")]
    max_pending_operations: u32,
    #[serde(default = "default_operation_retention_days")]
    operation_retention_days: u32,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DebugExecutionProfileModeYaml {
    Sandbox,
    Live,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DebugExecutionProfileYaml {
    mode: DebugExecutionProfileModeYaml,
    #[serde(default = "default_debug_max_concurrent_sessions")]
    max_concurrent_sessions: u32,
    #[serde(default = "default_debug_session_timeout")]
    session_timeout: String,
    #[serde(default = "default_debug_retention")]
    retention: String,
    #[serde(default)]
    allow_external_actions: bool,
    #[serde(default)]
    allow_live_provider_credentials: bool,
}

const fn default_max_agent_draft_bytes() -> usize {
    4 * 1_024 * 1_024
}

const fn default_max_agent_prompt_files() -> usize {
    128
}

const fn default_max_provider_models() -> usize {
    4_096
}

const fn default_max_pending_operations() -> u32 {
    256
}

const fn default_operation_retention_days() -> u32 {
    30
}

const fn default_debug_max_concurrent_sessions() -> u32 {
    4
}

fn default_debug_session_timeout() -> String {
    "10m".to_owned()
}

fn default_debug_retention() -> String {
    "24h".to_owned()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HumanTaskCredentialYaml {
    identity: String,
    #[serde(default)]
    groups: Vec<String>,
    token_env: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentsYaml {
    directory: PathBuf,
    #[serde(default)]
    enabled: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderExtensionYaml {
    #[serde(default)]
    extends: Option<String>,
    #[serde(default, rename = "type")]
    provider_type: Option<ProviderTypeYaml>,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    credential: Option<ProviderCredentialYaml>,
    #[serde(default)]
    models: BTreeMap<String, ProviderModelProfileYaml>,
    #[serde(default)]
    connect_timeout: Option<String>,
    #[serde(default)]
    request_timeout: Option<String>,
    #[serde(default)]
    transport: Option<ProviderTransportYaml>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProviderTypeYaml {
    OpenAiCompatible,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCredentialYaml {
    #[serde(default)]
    r#type: Option<ProviderCredentialTypeYaml>,
    env: String,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProviderCredentialTypeYaml {
    Bearer,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderModelProfileYaml {
    #[serde(default)]
    input: BTreeSet<ModelInputModalityYaml>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum ModelInputModalityYaml {
    Text,
    Image,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderTransportYaml {
    #[serde(default)]
    plaintext_http: ProviderPlaintextHttpYaml,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProviderPlaintextHttpYaml {
    #[default]
    Disabled,
    Loopback,
    TrustedPrivate,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelPolicyYaml {
    allow: Vec<ModelSelectorYaml>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelSelectorYaml {
    provider: String,
    id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActionsYaml {
    #[serde(default)]
    enabled: Vec<String>,
    http_get: Option<HttpGetYaml>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpGetYaml {
    timeout: String,
    max_bytes: usize,
    allowlist: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
enum HistoryYaml {
    Sqlite {
        path: PathBuf,
    },
    Postgres {
        database_url_env: String,
        #[serde(default = "default_postgres_pool_connections")]
        max_connections: u32,
    },
}

const fn default_postgres_pool_connections() -> u32 {
    10
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactsYaml {
    provider: ArtifactStoreProvider,
    #[serde(default)]
    namespace: Option<String>,
    directory: PathBuf,
    inline_threshold_bytes: usize,
    #[serde(default)]
    tenant_encryption: Option<TenantArtifactEncryptionYaml>,
    #[serde(default)]
    max_read_bytes: Option<usize>,
    orphan_retention: String,
    #[serde(default)]
    reference_retention: Option<String>,
    gc_interval: String,
    deletion_claim_seconds: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TenantArtifactEncryptionYaml {
    active_key_version: String,
    keyring_env: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeYaml {
    #[serde(default)]
    default_persistence_mode: PersistenceMode,
    #[serde(default)]
    terminal_only: Option<TerminalOnlyRuntimeYaml>,
    max_concurrent_runs: usize,
    max_concurrent_operations: usize,
    max_concurrent_operations_per_run: usize,
    operation_timeout: String,
    run_timeout: String,
    sse_keep_alive_interval: String,
    subscriber_capacity: usize,
    #[serde(default)]
    scheduler: Option<SchedulerYaml>,
    #[serde(default)]
    run_stream: Option<RunStreamYaml>,
    #[serde(default)]
    max_llm_tool_rounds: Option<u32>,
    #[serde(default)]
    max_llm_tool_calls: Option<u32>,
    #[serde(default)]
    public_event_retention: Option<String>,
    #[serde(default)]
    public_event_prune_interval: Option<String>,
    readiness_probe_timeout: Option<String>,
    shutdown_grace_period: Option<String>,
    shutdown_hard_deadline: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalOnlyRuntimeYaml {
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default = "default_owner_lease_seconds")]
    owner_lease_seconds: u64,
    #[serde(default = "default_owner_heartbeat_seconds")]
    owner_heartbeat_seconds: u64,
    #[serde(default = "default_terminal_commit_retry_seconds")]
    terminal_commit_retry_seconds: u64,
    #[serde(default = "default_terminal_run_retention_days")]
    run_retention_days: u32,
    #[serde(default)]
    allow_volatile_waits: bool,
    #[serde(default = "default_terminal_max_concurrent_runs")]
    max_concurrent_runs: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConversationsYaml {
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default = "default_inline_content_max_bytes")]
    inline_content_max_bytes: usize,
    #[serde(default = "default_message_page_size")]
    message_page_size_default: usize,
    #[serde(default = "default_message_page_size_max")]
    message_page_size_max: usize,
    #[serde(default = "default_summary_trigger_messages")]
    summary_trigger_messages: usize,
    #[serde(default = "default_summary_trigger_tokens")]
    summary_trigger_tokens: usize,
    #[serde(default = "default_recent_context_messages")]
    recent_context_messages: usize,
    #[serde(default = "default_conversation_retention_days")]
    retention_days: u32,
}

const fn default_true() -> bool {
    true
}

const fn default_owner_lease_seconds() -> u64 {
    30
}

const fn default_owner_heartbeat_seconds() -> u64 {
    10
}

const fn default_terminal_commit_retry_seconds() -> u64 {
    10
}

const fn default_terminal_run_retention_days() -> u32 {
    30
}

const fn default_terminal_max_concurrent_runs() -> usize {
    50
}

const fn default_inline_content_max_bytes() -> usize {
    8_192
}

const fn default_message_page_size() -> usize {
    50
}

const fn default_message_page_size_max() -> usize {
    200
}

const fn default_summary_trigger_messages() -> usize {
    30
}

const fn default_summary_trigger_tokens() -> usize {
    24_000
}

const fn default_recent_context_messages() -> usize {
    20
}

const fn default_conversation_retention_days() -> u32 {
    90
}

fn default_terminal_only_yaml() -> TerminalOnlyRuntimeYaml {
    TerminalOnlyRuntimeYaml {
        enabled: true,
        owner_lease_seconds: default_owner_lease_seconds(),
        owner_heartbeat_seconds: default_owner_heartbeat_seconds(),
        terminal_commit_retry_seconds: default_terminal_commit_retry_seconds(),
        run_retention_days: default_terminal_run_retention_days(),
        allow_volatile_waits: false,
        max_concurrent_runs: default_terminal_max_concurrent_runs(),
    }
}

fn default_conversations_yaml() -> ConversationsYaml {
    ConversationsYaml {
        enabled: true,
        inline_content_max_bytes: default_inline_content_max_bytes(),
        message_page_size_default: default_message_page_size(),
        message_page_size_max: default_message_page_size_max(),
        summary_trigger_messages: default_summary_trigger_messages(),
        summary_trigger_tokens: default_summary_trigger_tokens(),
        recent_context_messages: default_recent_context_messages(),
        retention_days: default_conversation_retention_days(),
    }
}

fn apply_persistence_environment(
    raw: &mut PlatformYaml,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<(), PlatformConfigError> {
    if let Some(value) = get_env("INSIGHT_RUNTIME__DEFAULT_PERSISTENCE_MODE") {
        raw.runtime.default_persistence_mode = match value.as_str() {
            "full" => PersistenceMode::Full,
            "terminal_only" => PersistenceMode::TerminalOnly,
            _ => {
                return Err(runtime_environment_error(
                    "INSIGHT_RUNTIME__DEFAULT_PERSISTENCE_MODE",
                    "full or terminal_only",
                ))
            }
        };
    }

    let terminal_only = raw
        .runtime
        .terminal_only
        .get_or_insert_with(default_terminal_only_yaml);
    if let Some(value) = get_env("INSIGHT_RUNTIME__TERMINAL_ONLY__ENABLED") {
        terminal_only.enabled =
            parse_runtime_environment_bool("INSIGHT_RUNTIME__TERMINAL_ONLY__ENABLED", &value)?;
    }
    if let Some(value) = get_env("INSIGHT_RUNTIME__TERMINAL_ONLY__OWNER_LEASE_SECONDS") {
        terminal_only.owner_lease_seconds = parse_runtime_environment_number(
            "INSIGHT_RUNTIME__TERMINAL_ONLY__OWNER_LEASE_SECONDS",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_RUNTIME__TERMINAL_ONLY__OWNER_HEARTBEAT_SECONDS") {
        terminal_only.owner_heartbeat_seconds = parse_runtime_environment_number(
            "INSIGHT_RUNTIME__TERMINAL_ONLY__OWNER_HEARTBEAT_SECONDS",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_RUNTIME__TERMINAL_ONLY__TERMINAL_COMMIT_RETRY_SECONDS") {
        terminal_only.terminal_commit_retry_seconds = parse_runtime_environment_number(
            "INSIGHT_RUNTIME__TERMINAL_ONLY__TERMINAL_COMMIT_RETRY_SECONDS",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_RUNTIME__TERMINAL_ONLY__RUN_RETENTION_DAYS") {
        terminal_only.run_retention_days = parse_runtime_environment_number(
            "INSIGHT_RUNTIME__TERMINAL_ONLY__RUN_RETENTION_DAYS",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_RUNTIME__TERMINAL_ONLY__ALLOW_VOLATILE_WAITS") {
        terminal_only.allow_volatile_waits = parse_runtime_environment_bool(
            "INSIGHT_RUNTIME__TERMINAL_ONLY__ALLOW_VOLATILE_WAITS",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_RUNTIME__TERMINAL_ONLY__MAX_CONCURRENT_RUNS") {
        terminal_only.max_concurrent_runs = parse_runtime_environment_number(
            "INSIGHT_RUNTIME__TERMINAL_ONLY__MAX_CONCURRENT_RUNS",
            &value,
        )?;
    }

    let conversations = raw
        .conversations
        .get_or_insert_with(default_conversations_yaml);
    if let Some(value) = get_env("INSIGHT_CONVERSATIONS__ENABLED") {
        conversations.enabled =
            parse_conversation_environment_bool("INSIGHT_CONVERSATIONS__ENABLED", &value)?;
    }
    if let Some(value) = get_env("INSIGHT_CONVERSATIONS__INLINE_CONTENT_MAX_BYTES") {
        conversations.inline_content_max_bytes = parse_conversation_environment_number(
            "INSIGHT_CONVERSATIONS__INLINE_CONTENT_MAX_BYTES",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_CONVERSATIONS__MESSAGE_PAGE_SIZE_DEFAULT") {
        conversations.message_page_size_default = parse_conversation_environment_number(
            "INSIGHT_CONVERSATIONS__MESSAGE_PAGE_SIZE_DEFAULT",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_CONVERSATIONS__MESSAGE_PAGE_SIZE_MAX") {
        conversations.message_page_size_max = parse_conversation_environment_number(
            "INSIGHT_CONVERSATIONS__MESSAGE_PAGE_SIZE_MAX",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_CONVERSATIONS__SUMMARY_TRIGGER_MESSAGES") {
        conversations.summary_trigger_messages = parse_conversation_environment_number(
            "INSIGHT_CONVERSATIONS__SUMMARY_TRIGGER_MESSAGES",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_CONVERSATIONS__SUMMARY_TRIGGER_TOKENS") {
        conversations.summary_trigger_tokens = parse_conversation_environment_number(
            "INSIGHT_CONVERSATIONS__SUMMARY_TRIGGER_TOKENS",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_CONVERSATIONS__RECENT_CONTEXT_MESSAGES") {
        conversations.recent_context_messages = parse_conversation_environment_number(
            "INSIGHT_CONVERSATIONS__RECENT_CONTEXT_MESSAGES",
            &value,
        )?;
    }
    if let Some(value) = get_env("INSIGHT_CONVERSATIONS__RETENTION_DAYS") {
        conversations.retention_days =
            parse_conversation_environment_number("INSIGHT_CONVERSATIONS__RETENTION_DAYS", &value)?;
    }
    Ok(())
}

fn parse_runtime_environment_bool(name: &str, value: &str) -> Result<bool, PlatformConfigError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(runtime_environment_error(name, "true or false")),
    }
}

fn parse_conversation_environment_bool(
    name: &str,
    value: &str,
) -> Result<bool, PlatformConfigError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(conversation_environment_error(name, "true or false")),
    }
}

fn parse_runtime_environment_number<T>(name: &str, value: &str) -> Result<T, PlatformConfigError>
where
    T: std::str::FromStr,
{
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(runtime_environment_error(
            name,
            "an unsigned base-10 integer",
        ));
    }
    value
        .parse()
        .map_err(|_| runtime_environment_error(name, "an unsigned base-10 integer"))
}

fn parse_conversation_environment_number<T>(
    name: &str,
    value: &str,
) -> Result<T, PlatformConfigError>
where
    T: std::str::FromStr,
{
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(conversation_environment_error(
            name,
            "an unsigned base-10 integer",
        ));
    }
    value
        .parse()
        .map_err(|_| conversation_environment_error(name, "an unsigned base-10 integer"))
}

fn runtime_environment_error(name: &str, expected: &str) -> PlatformConfigError {
    runtime_error(format!("environment variable '{name}' must be {expected}"))
}

fn conversation_environment_error(name: &str, expected: &str) -> PlatformConfigError {
    conversation_error(format!("environment variable '{name}' must be {expected}"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerYaml {
    active_poll_interval: String,
    idle_poll_min_interval: String,
    idle_poll_max_interval: String,
    safety_poll_interval: String,
    claim_batch_size: u32,
    notification_reconnect_interval: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunStreamYaml {
    broker: LiveRunStreamBrokerProvider,
    body_queue_capacity: usize,
    control_queue_capacity: usize,
    max_frame_bytes: usize,
    max_item_bytes: usize,
    max_run_bytes: usize,
    terminal_barrier_timeout: String,
    outbound_write_timeout: String,
}

fn resolve_auth(
    auth: AuthYaml,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<(AuthConfig, Vec<HumanTaskCredentialConfig>), PlatformConfigError> {
    let (auth, raw_human_task_credentials) = match auth {
        AuthYaml::Disabled {
            human_task_credentials,
        } => (AuthConfig::Disabled, human_task_credentials),
        AuthYaml::BearerEnv {
            token_env,
            human_task_credentials,
        } => {
            let token = required_secret(&token_env, get_env)?;
            (AuthConfig::Bearer { token }, human_task_credentials)
        }
    };
    let human_task_credentials =
        resolve_human_task_credentials(raw_human_task_credentials, get_env)?;
    if auth.bearer_token().is_some_and(|base_token| {
        human_task_credentials
            .iter()
            .any(|credential| credential.token().expose() == base_token)
    }) {
        return Err(PlatformConfigError::new(
            "PLATFORM_CONFIG_INVALID",
            "platform and HumanTask bearer credentials must be distinct",
        ));
    }
    Ok((auth, human_task_credentials))
}

fn resolve_management(
    raw: Option<ManagementYaml>,
    _deployment_mode: DeploymentMode,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<ManagementConfig, PlatformConfigError> {
    let Some(raw) = raw else {
        return Ok(ManagementConfig {
            enabled: false,
            operator_credentials: Vec::new(),
            provider_secret_resolver_allowed_names: BTreeSet::new(),
            limits: ManagementLimitsConfig {
                max_agent_draft_bytes: default_max_agent_draft_bytes(),
                max_agent_prompt_files: default_max_agent_prompt_files(),
                max_provider_models: default_max_provider_models(),
                max_pending_operations: default_max_pending_operations(),
                operation_retention_days: default_operation_retention_days(),
            },
            debug_execution_profiles: BTreeMap::new(),
        });
    };
    if raw.version != 1 {
        return Err(PlatformConfigError::new(
            "PLATFORM_MANAGEMENT_VERSION_UNSUPPORTED",
            "management.version must be 1",
        ));
    }
    let allowed_capabilities = BTreeSet::from([
        "agent.read",
        "agent.write",
        "agent.validate",
        "agent.publish",
        "agent.deploy",
        "agent.activate",
        "agent.archive",
        "agent.debug.sandbox",
        "agent.debug.live",
        "provider.read",
        "provider.write",
        "provider.discover",
        "provider.test",
        "provider.publish",
        "provider.activate",
        "provider.suspend",
        "provider.retire",
        "mcp.server.read",
        "mcp.server.write",
        "mcp.server.discover",
        "mcp.server.publish",
    ]);
    let mut identities = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut operator_credentials = Vec::with_capacity(raw.operator_credentials.len());
    for credential in raw.operator_credentials {
        let identity = credential.identity.trim().to_owned();
        let capabilities = unique_names(
            credential.capabilities,
            "management.operator_credentials.capabilities",
        )?;
        if !valid_human_principal_label(&identity)
            || !identities.insert(identity.clone())
            || capabilities.is_empty()
            || capabilities
                .iter()
                .any(|capability| !allowed_capabilities.contains(capability.as_str()))
        {
            return Err(PlatformConfigError::new(
                "PLATFORM_MANAGEMENT_INVALID",
                "management Operator identity or capability set is invalid",
            ));
        }
        let token = required_secret(&credential.token_env, get_env)?;
        if !tokens.insert(token.expose().to_owned()) {
            return Err(PlatformConfigError::new(
                "PLATFORM_MANAGEMENT_INVALID",
                "management Operator bearer credentials must be unique",
            ));
        }
        operator_credentials.push(ManagementOperatorCredentialConfig {
            identity,
            token,
            capabilities,
        });
    }
    if raw.enabled && operator_credentials.is_empty() {
        return Err(PlatformConfigError::new(
            "PLATFORM_MANAGEMENT_INVALID",
            "enabled management requires at least one Operator credential",
        ));
    }
    let provider_secret_resolver_allowed_names = match raw.provider_secret_resolver {
        ProviderSecretResolverYaml::Disabled => BTreeSet::new(),
        ProviderSecretResolverYaml::EnvironmentReference { allowed_names } => {
            let names = unique_names(
                allowed_names,
                "management.provider_secret_resolver.allowed_names",
            )?;
            if names.is_empty()
                || names.iter().any(|name| {
                    name.len() > 256
                        || !name.bytes().enumerate().all(|(index, byte)| {
                            byte.is_ascii_uppercase()
                                || byte == b'_'
                                || (byte.is_ascii_digit() && index > 0)
                        })
                })
            {
                return Err(PlatformConfigError::new(
                    "PLATFORM_MANAGEMENT_INVALID",
                    "management Provider secret resolver policy is invalid",
                ));
            }
            names
        }
    };
    let limits = raw.limits.unwrap_or(ManagementLimitsYaml {
        max_agent_draft_bytes: default_max_agent_draft_bytes(),
        max_agent_prompt_files: default_max_agent_prompt_files(),
        max_provider_models: default_max_provider_models(),
        max_pending_operations: default_max_pending_operations(),
        operation_retention_days: default_operation_retention_days(),
    });
    if limits.max_agent_draft_bytes == 0
        || limits.max_agent_draft_bytes > 64 * 1_024 * 1_024
        || limits.max_agent_prompt_files == 0
        || limits.max_agent_prompt_files > 4_096
        || limits.max_provider_models == 0
        || limits.max_provider_models > 65_536
        || limits.max_pending_operations == 0
        || limits.max_pending_operations > 10_000
        || limits.operation_retention_days == 0
        || limits.operation_retention_days > 3_650
    {
        return Err(PlatformConfigError::new(
            "PLATFORM_MANAGEMENT_INVALID",
            "management limits are outside supported bounds",
        ));
    }
    let mut debug_execution_profiles = BTreeMap::new();
    for (profile_id, profile) in raw.debug_execution_profiles {
        if !valid_human_principal_label(&profile_id)
            || profile.max_concurrent_sessions == 0
            || profile.max_concurrent_sessions > 1_024
        {
            return Err(PlatformConfigError::new(
                "PLATFORM_MANAGEMENT_INVALID",
                "management debug execution profile is invalid",
            ));
        }
        let mode = match profile.mode {
            DebugExecutionProfileModeYaml::Sandbox => DebugExecutionProfileMode::Sandbox,
            DebugExecutionProfileModeYaml::Live => DebugExecutionProfileMode::Live,
        };
        if mode == DebugExecutionProfileMode::Sandbox
            && (profile.allow_external_actions || profile.allow_live_provider_credentials)
        {
            return Err(PlatformConfigError::new(
                "PLATFORM_MANAGEMENT_INVALID",
                "sandbox debug profiles cannot allow live external effects or credentials",
            ));
        }
        let session_timeout = positive_duration(
            &profile.session_timeout,
            "management.debug_execution_profiles.session_timeout",
        )?;
        let retention = positive_duration(
            &profile.retention,
            "management.debug_execution_profiles.retention",
        )?;
        if retention < session_timeout {
            return Err(PlatformConfigError::new(
                "PLATFORM_MANAGEMENT_INVALID",
                "Debug content retention cannot be shorter than the Session timeout",
            ));
        }
        debug_execution_profiles.insert(
            profile_id,
            DebugExecutionProfileConfig {
                mode,
                max_concurrent_sessions: profile.max_concurrent_sessions,
                session_timeout,
                retention,
                allow_external_actions: profile.allow_external_actions,
                allow_live_provider_credentials: profile.allow_live_provider_credentials,
            },
        );
    }
    Ok(ManagementConfig {
        enabled: raw.enabled,
        operator_credentials,
        provider_secret_resolver_allowed_names,
        limits: ManagementLimitsConfig {
            max_agent_draft_bytes: limits.max_agent_draft_bytes,
            max_agent_prompt_files: limits.max_agent_prompt_files,
            max_provider_models: limits.max_provider_models,
            max_pending_operations: limits.max_pending_operations,
            operation_retention_days: limits.operation_retention_days,
        },
        debug_execution_profiles,
    })
}

fn resolve_human_task_credentials(
    credentials: Vec<HumanTaskCredentialYaml>,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<Vec<HumanTaskCredentialConfig>, PlatformConfigError> {
    let mut identities = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut resolved = Vec::with_capacity(credentials.len());

    for credential in credentials {
        let identity = credential.identity.trim().to_owned();
        if !valid_human_principal_label(&identity) || !identities.insert(identity.clone()) {
            return Err(PlatformConfigError::new(
                "PLATFORM_CONFIG_INVALID",
                "auth.human_task_credentials contains an invalid or duplicate identity",
            ));
        }
        let groups = unique_names(credential.groups, "auth.human_task_credentials.groups")?;
        if groups
            .iter()
            .any(|group| !valid_human_principal_label(group))
        {
            return Err(PlatformConfigError::new(
                "PLATFORM_CONFIG_INVALID",
                "auth.human_task_credentials contains an invalid group",
            ));
        }
        let token = required_secret(&credential.token_env, get_env)?;
        if !tokens.insert(token.expose().to_owned()) {
            return Err(PlatformConfigError::new(
                "PLATFORM_CONFIG_INVALID",
                "auth.human_task_credentials contains duplicate bearer credentials",
            ));
        }
        resolved.push(HumanTaskCredentialConfig {
            identity,
            groups,
            token,
        });
    }

    Ok(resolved)
}

fn valid_human_principal_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
}

fn resolve_actions(raw: ActionsYaml) -> Result<ActionsConfig, PlatformConfigError> {
    let enabled = unique_names(raw.enabled, "actions.enabled")?;
    let http_get = raw
        .http_get
        .map(|http| {
            let timeout = positive_duration(&http.timeout, "actions.http_get.timeout")?;
            if http.max_bytes == 0 {
                return Err(runtime_error(
                    "actions.http_get.max_bytes must be greater than zero",
                ));
            }
            let allowlist = unique_names(http.allowlist, "actions.http_get.allowlist")?;
            if allowlist.is_empty() {
                return Err(PlatformConfigError::new(
                    "PLATFORM_CONFIG_INVALID",
                    "actions.http_get.allowlist must not be empty",
                ));
            }
            Ok(HttpGetResourceConfig {
                timeout,
                max_bytes: http.max_bytes,
                allowlist,
            })
        })
        .transpose()?;
    if enabled.contains("http_get") && http_get.is_none() {
        return Err(PlatformConfigError::new(
            "PLATFORM_CONFIG_INVALID",
            "http_get is enabled but actions.http_get is missing",
        ));
    }
    Ok(ActionsConfig { enabled, http_get })
}

fn resolve_providers(
    raw: BTreeMap<String, ProviderExtensionYaml>,
) -> Result<ProvidersConfig, PlatformConfigError> {
    let mut extensions = BTreeMap::new();
    for (id, extension) in raw {
        validate_provider_route_id(&id, "providers route id")?;
        let source = match (extension.extends, extension.provider_type) {
            (Some(provider), None) => {
                validate_provider_route_id(&provider, "providers.extends")?;
                ProviderExtensionSource::Extends { provider }
            }
            (None, Some(ProviderTypeYaml::OpenAiCompatible)) => {
                ProviderExtensionSource::OpenAiCompatible
            }
            _ => {
                return Err(PlatformConfigError::new(
                    "PLATFORM_PROVIDER_INVALID",
                    format!("provider route '{id}' must declare exactly one of extends or type"),
                ))
            }
        };
        if matches!(&source, ProviderExtensionSource::Extends { .. })
            && extension.endpoint.is_some()
        {
            return Err(PlatformConfigError::new(
                "PLATFORM_PROVIDER_INVALID",
                format!("inherited provider route '{id}' cannot override its built-in endpoint"),
            ));
        }
        if matches!(&source, ProviderExtensionSource::OpenAiCompatible)
            && extension.endpoint.is_none()
        {
            return Err(PlatformConfigError::new(
                "PLATFORM_PROVIDER_INVALID",
                format!("custom provider route '{id}' requires endpoint"),
            ));
        }
        if extension
            .endpoint
            .as_deref()
            .is_some_and(|endpoint| endpoint.trim().is_empty())
        {
            return Err(PlatformConfigError::new(
                "PLATFORM_PROVIDER_INVALID",
                format!("provider route '{id}' endpoint must not be empty"),
            ));
        }
        let credential_env = extension
            .credential
            .map(|credential| {
                if matches!(&source, ProviderExtensionSource::OpenAiCompatible)
                    && credential.r#type != Some(ProviderCredentialTypeYaml::Bearer)
                {
                    return Err(PlatformConfigError::new(
                        "PLATFORM_PROVIDER_INVALID",
                        format!("custom provider route '{id}' requires credential.type=bearer"),
                    ));
                }
                if credential.env.trim().is_empty()
                    || credential.env.len() > 256
                    || !credential.env.bytes().enumerate().all(|(index, byte)| {
                        byte.is_ascii_uppercase()
                            || byte == b'_'
                            || (byte.is_ascii_digit() && index > 0)
                    })
                {
                    return Err(PlatformConfigError::new(
                        "PLATFORM_PROVIDER_INVALID",
                        format!("provider route '{id}' credential.env is invalid"),
                    ));
                }
                Ok(credential.env)
            })
            .transpose()?;
        if matches!(&source, ProviderExtensionSource::OpenAiCompatible) && credential_env.is_none()
        {
            return Err(PlatformConfigError::new(
                "PLATFORM_PROVIDER_INVALID",
                format!("custom provider route '{id}' requires credential"),
            ));
        }
        if matches!(&source, ProviderExtensionSource::OpenAiCompatible)
            && extension.models.is_empty()
        {
            return Err(PlatformConfigError::new(
                "PLATFORM_PROVIDER_INVALID",
                format!("custom provider route '{id}' requires at least one model"),
            ));
        }
        let models = extension
            .models
            .into_iter()
            .map(|(model_id, profile)| {
                validate_provider_model_id(&model_id, "providers model id")?;
                let input = if profile.input.is_empty() {
                    BTreeSet::from([ModelInputModality::Text])
                } else {
                    profile
                        .input
                        .into_iter()
                        .map(|input| match input {
                            ModelInputModalityYaml::Text => ModelInputModality::Text,
                            ModelInputModalityYaml::Image => ModelInputModality::Image,
                        })
                        .collect()
                };
                if input.contains(&ModelInputModality::Image)
                    && !input.contains(&ModelInputModality::Text)
                {
                    return Err(PlatformConfigError::new(
                        "PLATFORM_PROVIDER_INVALID",
                        format!(
                            "provider route '{id}' model '{model_id}' image input requires text input"
                        ),
                    ));
                }
                Ok((model_id, ProviderModelProfileConfig { input }))
            })
            .collect::<Result<BTreeMap<_, _>, PlatformConfigError>>()?;
        let transport = match extension.transport.unwrap_or_default().plaintext_http {
            ProviderPlaintextHttpYaml::Disabled => ProviderTransportPolicy::HttpsOnly,
            ProviderPlaintextHttpYaml::Loopback => ProviderTransportPolicy::AllowLoopbackHttp,
            ProviderPlaintextHttpYaml::TrustedPrivate => {
                ProviderTransportPolicy::AllowTrustedPrivateHttp
            }
        };
        extensions.insert(
            id,
            ProviderExtensionConfig {
                source,
                endpoint: extension.endpoint,
                credential_env,
                models,
                connect_timeout: extension
                    .connect_timeout
                    .as_deref()
                    .map(|value| positive_duration(value, "providers.connect_timeout"))
                    .transpose()?
                    .unwrap_or(Duration::from_secs(5)),
                request_timeout: extension
                    .request_timeout
                    .as_deref()
                    .map(|value| positive_duration(value, "providers.request_timeout"))
                    .transpose()?
                    .unwrap_or(Duration::from_secs(120)),
                transport,
            },
        );
    }
    Ok(ProvidersConfig { extensions })
}

fn resolve_model_policy(
    raw: Option<ModelPolicyYaml>,
) -> Result<Option<ModelPolicyConfig>, PlatformConfigError> {
    raw.map(|policy| {
        if policy.allow.is_empty() {
            return Err(PlatformConfigError::new(
                "PLATFORM_MODEL_POLICY_INVALID",
                "model_policy.allow must not be empty",
            ));
        }
        let mut allow = BTreeSet::new();
        for selector in policy.allow {
            validate_provider_route_id(&selector.provider, "model_policy.allow.provider")?;
            validate_provider_model_id(&selector.id, "model_policy.allow.id")?;
            if !allow.insert(ModelSelectorConfig {
                provider: selector.provider,
                id: selector.id,
            }) {
                return Err(PlatformConfigError::new(
                    "PLATFORM_MODEL_POLICY_INVALID",
                    "model_policy.allow contains a duplicate selector",
                ));
            }
        }
        Ok(ModelPolicyConfig { allow })
    })
    .transpose()
}

fn validate_provider_route_id(value: &str, field: &str) -> Result<(), PlatformConfigError> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || (byte.is_ascii_digit() && index > 0)
                || (byte == b'-' && index > 0)
        })
        || value.ends_with('-')
    {
        return Err(PlatformConfigError::new(
            "PLATFORM_PROVIDER_INVALID",
            format!("{field} must be a bounded lowercase route identifier"),
        ));
    }
    Ok(())
}

fn validate_provider_model_id(value: &str, field: &str) -> Result<(), PlatformConfigError> {
    if value.trim().is_empty()
        || value.len() > 512
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(PlatformConfigError::new(
            "PLATFORM_PROVIDER_INVALID",
            format!("{field} must be a bounded non-empty opaque identifier"),
        ));
    }
    Ok(())
}

fn resolve_history(
    parent: &Path,
    history: HistoryYaml,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<HistoryConfig, PlatformConfigError> {
    match history {
        HistoryYaml::Sqlite { path } => Ok(HistoryConfig::Sqlite {
            path: resolve_path(parent, &path),
        }),
        HistoryYaml::Postgres {
            database_url_env,
            max_connections,
        } => {
            if !(4..=256).contains(&max_connections) {
                return Err(PlatformConfigError::new(
                    "PLATFORM_CONFIG_INVALID",
                    "history.max_connections must be between 4 and 256",
                ));
            }
            let database_url = required_secret(&database_url_env, get_env)?;
            validate_postgres_history_url(database_url.expose())?;
            Ok(HistoryConfig::Postgres {
                database_url,
                max_connections,
            })
        }
    }
}

fn resolve_artifacts(
    parent: &Path,
    artifacts: Option<ArtifactsYaml>,
    deployment_mode: DeploymentMode,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<ArtifactsConfig, PlatformConfigError> {
    let artifacts = match artifacts {
        Some(artifacts) => artifacts,
        None if deployment_mode == DeploymentMode::Production => {
            return Err(PlatformConfigError::new(
                "PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE",
                "deployment_mode=production requires artifacts.provider=shared_filesystem",
            ));
        }
        None => ArtifactsYaml {
            provider: ArtifactStoreProvider::LocalFilesystem,
            namespace: None,
            directory: PathBuf::from("../data/artifacts"),
            inline_threshold_bytes: 64 * 1024,
            tenant_encryption: None,
            max_read_bytes: Some(64 * 1024 * 1024),
            orphan_retention: "24h".to_owned(),
            reference_retention: Some("30d".to_owned()),
            gc_interval: "1m".to_owned(),
            deletion_claim_seconds: 60,
        },
    };
    let namespace_valid = artifacts.namespace.as_deref().is_some_and(|namespace| {
        !namespace.is_empty()
            && namespace.len() <= 128
            && namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    });
    match artifacts.provider {
        ArtifactStoreProvider::LocalFilesystem if artifacts.namespace.is_some() => {
            return Err(PlatformConfigError::new(
                "PLATFORM_ARTIFACTS_INVALID",
                "artifacts.namespace is forbidden for local_filesystem",
            ));
        }
        ArtifactStoreProvider::SharedFilesystem if !namespace_valid => {
            return Err(PlatformConfigError::new(
                "PLATFORM_ARTIFACTS_INVALID",
                "shared_filesystem requires a valid artifacts.namespace",
            ));
        }
        _ => {}
    }
    if deployment_mode == DeploymentMode::Production
        && artifacts.provider != ArtifactStoreProvider::SharedFilesystem
    {
        return Err(PlatformConfigError::new(
            "PLATFORM_PRODUCTION_REQUIRES_SHARED_ARTIFACT_STORE",
            "deployment_mode=production requires artifacts.provider=shared_filesystem",
        ));
    }
    let max_read_bytes = artifacts.max_read_bytes.unwrap_or(64 * 1024 * 1024);
    if artifacts.inline_threshold_bytes == 0
        || artifacts.inline_threshold_bytes > 64 * 1024 * 1024
        || max_read_bytes == 0
        || max_read_bytes > 256 * 1024 * 1024
        || artifacts.deletion_claim_seconds == 0
        || artifacts.deletion_claim_seconds > 3_600
    {
        return Err(PlatformConfigError::new(
            "PLATFORM_ARTIFACTS_INVALID",
            "artifact thresholds and deletion claim must be positive and bounded",
        ));
    }
    let reference_retention = positive_artifact_duration(
        artifacts.reference_retention.as_deref().unwrap_or("30d"),
        "artifacts.reference_retention",
    )?;
    if reference_retention < Duration::from_secs(1)
        || reference_retention > Duration::from_secs(10 * 365 * 24 * 60 * 60)
    {
        return Err(PlatformConfigError::new(
            "PLATFORM_ARTIFACTS_INVALID",
            "artifact reference retention must be between one second and ten years",
        ));
    }
    let tenant_encryption = artifacts
        .tenant_encryption
        .map(|encryption| {
            let active_key_version = encryption.active_key_version.trim().to_owned();
            if active_key_version.is_empty()
                || active_key_version.len() > 64
                || !active_key_version
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            {
                return Err(PlatformConfigError::new(
                    "PLATFORM_ARTIFACTS_INVALID",
                    "artifacts.tenant_encryption.active_key_version is invalid",
                ));
            }
            let keyring = required_secret(&encryption.keyring_env, get_env)?;
            insight_storage::artifact_store::TenantArtifactEncryptionKeyring::from_secret_json(
                active_key_version.clone(),
                keyring.expose(),
            )
            .map_err(|_| {
                PlatformConfigError::new(
                    "PLATFORM_ARTIFACTS_INVALID",
                    "artifacts tenant encryption keyring is invalid",
                )
            })?;
            Ok(TenantArtifactEncryptionConfig {
                active_key_version,
                keyring,
            })
        })
        .transpose()?;
    Ok(ArtifactsConfig {
        provider: artifacts.provider,
        namespace: artifacts.namespace,
        directory: resolve_path(parent, &artifacts.directory),
        inline_threshold_bytes: artifacts.inline_threshold_bytes,
        tenant_encryption,
        max_read_bytes,
        orphan_retention: positive_artifact_duration(
            &artifacts.orphan_retention,
            "artifacts.orphan_retention",
        )?,
        reference_retention,
        gc_interval: positive_artifact_duration(&artifacts.gc_interval, "artifacts.gc_interval")?,
        deletion_claim_seconds: artifacts.deletion_claim_seconds,
    })
}

fn validate_postgres_history_url(database_url: &str) -> Result<(), PlatformConfigError> {
    insight_storage::postgres_config::validate_postgres_history_url(database_url).map_err(
        |error| match error {
            PostgresHistoryUrlError::InvalidUrl => PlatformConfigError::new(
            "PLATFORM_CONFIG_INVALID",
            "PostgreSQL history URL must be a valid postgres:// or postgresql:// URL",
            ),
            PostgresHistoryUrlError::RemoteTlsRequired => PlatformConfigError::new(
                "PLATFORM_CONFIG_INVALID",
                "remote PostgreSQL history URL must set sslmode=verify-full; plaintext is allowed only for exact loopback or Unix socket development URLs",
            ),
        },
    )
}

fn resolve_runtime(
    raw: RuntimeYaml,
    deployment_mode: DeploymentMode,
) -> Result<RuntimeConfig, PlatformConfigError> {
    let terminal_only = resolve_terminal_only(raw.terminal_only)?;
    let capacities = [
        raw.max_concurrent_runs,
        raw.max_concurrent_operations,
        raw.max_concurrent_operations_per_run,
        raw.subscriber_capacity,
    ];
    if capacities.contains(&0) {
        return Err(runtime_error("runtime capacities must be positive"));
    }
    let readiness_probe_timeout = optional_positive_duration(
        raw.readiness_probe_timeout.as_deref(),
        Duration::from_secs(2),
        "runtime.readiness_probe_timeout",
    )?;
    let shutdown_grace_period = optional_positive_duration(
        raw.shutdown_grace_period.as_deref(),
        Duration::from_secs(30),
        "runtime.shutdown_grace_period",
    )?;
    let shutdown_hard_deadline = optional_positive_duration(
        raw.shutdown_hard_deadline.as_deref(),
        Duration::from_secs(35),
        "runtime.shutdown_hard_deadline",
    )?;
    let public_event_retention = optional_positive_duration(
        raw.public_event_retention.as_deref(),
        Duration::from_secs(24 * 60 * 60),
        "runtime.public_event_retention",
    )?;
    if public_event_retention < Duration::from_secs(1)
        || public_event_retention > Duration::from_secs(10 * 365 * 24 * 60 * 60)
    {
        return Err(runtime_error(
            "runtime.public_event_retention must be between one second and ten years",
        ));
    }
    let public_event_prune_interval = optional_positive_duration(
        raw.public_event_prune_interval.as_deref(),
        Duration::from_secs(60),
        "runtime.public_event_prune_interval",
    )?;
    if shutdown_hard_deadline <= shutdown_grace_period {
        return Err(runtime_error(
            "runtime.shutdown_hard_deadline must be greater than runtime.shutdown_grace_period",
        ));
    }
    let run_stream = resolve_run_stream(raw.run_stream, deployment_mode)?;
    let scheduler = resolve_scheduler(raw.scheduler)?;
    let max_llm_tool_rounds = raw.max_llm_tool_rounds.unwrap_or(16);
    let max_llm_tool_calls = raw.max_llm_tool_calls.unwrap_or(64);
    if max_llm_tool_rounds == 0
        || max_llm_tool_rounds > 128
        || max_llm_tool_calls == 0
        || max_llm_tool_calls > 1_024
        || max_llm_tool_calls < max_llm_tool_rounds
    {
        return Err(runtime_error("runtime LLM tool hard limits are invalid"));
    }
    Ok(RuntimeConfig {
        default_persistence_mode: raw.default_persistence_mode,
        terminal_only,
        max_concurrent_runs: raw.max_concurrent_runs,
        max_concurrent_operations: raw.max_concurrent_operations,
        max_concurrent_operations_per_run: raw.max_concurrent_operations_per_run,
        operation_timeout: positive_duration(&raw.operation_timeout, "runtime.operation_timeout")?,
        run_timeout: positive_duration(&raw.run_timeout, "runtime.run_timeout")?,
        sse_keep_alive_interval: positive_duration(
            &raw.sse_keep_alive_interval,
            "runtime.sse_keep_alive_interval",
        )?,
        subscriber_capacity: raw.subscriber_capacity,
        scheduler,
        run_stream,
        max_llm_tool_rounds,
        max_llm_tool_calls,
        public_event_retention,
        public_event_prune_interval,
        readiness_probe_timeout,
        shutdown_grace_period,
        shutdown_hard_deadline,
    })
}

fn resolve_terminal_only(
    raw: Option<TerminalOnlyRuntimeYaml>,
) -> Result<TerminalOnlyRuntimeConfig, PlatformConfigError> {
    let raw = raw.unwrap_or_else(default_terminal_only_yaml);
    if !(3..=300).contains(&raw.owner_lease_seconds) {
        return Err(runtime_error(
            "runtime.terminal_only.owner_lease_seconds must be between 3 and 300",
        ));
    }
    if !(1..=100).contains(&raw.owner_heartbeat_seconds)
        || raw.owner_heartbeat_seconds > raw.owner_lease_seconds / 3
    {
        return Err(runtime_error(
            "runtime.terminal_only.owner_heartbeat_seconds must be positive and no greater than one third of owner_lease_seconds",
        ));
    }
    if !(1..=300).contains(&raw.terminal_commit_retry_seconds) {
        return Err(runtime_error(
            "runtime.terminal_only.terminal_commit_retry_seconds must be between 1 and 300",
        ));
    }
    if !(1..=3_650).contains(&raw.run_retention_days) {
        return Err(runtime_error(
            "runtime.terminal_only.run_retention_days must be between 1 and 3650",
        ));
    }
    if !(1..=10_000).contains(&raw.max_concurrent_runs) {
        return Err(runtime_error(
            "runtime.terminal_only.max_concurrent_runs must be between 1 and 10000",
        ));
    }
    Ok(TerminalOnlyRuntimeConfig {
        enabled: raw.enabled,
        owner_lease: Duration::from_secs(raw.owner_lease_seconds),
        owner_heartbeat: Duration::from_secs(raw.owner_heartbeat_seconds),
        terminal_commit_retry: Duration::from_secs(raw.terminal_commit_retry_seconds),
        run_retention_days: raw.run_retention_days,
        allow_volatile_waits: raw.allow_volatile_waits,
        max_concurrent_runs: raw.max_concurrent_runs,
    })
}

fn resolve_conversations(
    raw: Option<ConversationsYaml>,
) -> Result<ConversationsConfig, PlatformConfigError> {
    let raw = raw.unwrap_or_else(default_conversations_yaml);
    if !(256..=1_048_576).contains(&raw.inline_content_max_bytes) {
        return Err(conversation_error(
            "conversations.inline_content_max_bytes must be between 256 and 1048576",
        ));
    }
    if !(1..=200).contains(&raw.message_page_size_max)
        || !(1..=raw.message_page_size_max).contains(&raw.message_page_size_default)
    {
        return Err(conversation_error(
            "conversation page sizes must be positive, bounded by 200, and default must not exceed max",
        ));
    }
    if !(2..=10_000).contains(&raw.summary_trigger_messages) {
        return Err(conversation_error(
            "conversations.summary_trigger_messages must be between 2 and 10000",
        ));
    }
    if !(256..=1_000_000).contains(&raw.summary_trigger_tokens) {
        return Err(conversation_error(
            "conversations.summary_trigger_tokens must be between 256 and 1000000",
        ));
    }
    if !(1..=200).contains(&raw.recent_context_messages)
        || raw.recent_context_messages > raw.summary_trigger_messages
    {
        return Err(conversation_error(
            "conversations.recent_context_messages must be between 1 and 200 and no greater than summary_trigger_messages",
        ));
    }
    if !(1..=3_650).contains(&raw.retention_days) {
        return Err(conversation_error(
            "conversations.retention_days must be between 1 and 3650",
        ));
    }
    Ok(ConversationsConfig {
        enabled: raw.enabled,
        inline_content_max_bytes: raw.inline_content_max_bytes,
        message_page_size_default: raw.message_page_size_default,
        message_page_size_max: raw.message_page_size_max,
        summary_trigger_messages: raw.summary_trigger_messages,
        summary_trigger_tokens: raw.summary_trigger_tokens,
        recent_context_messages: raw.recent_context_messages,
        retention_days: raw.retention_days,
    })
}

fn resolve_scheduler(raw: Option<SchedulerYaml>) -> Result<SchedulerConfig, PlatformConfigError> {
    let Some(raw) = raw else {
        return Ok(SchedulerConfig {
            active_poll_interval: Duration::from_millis(25),
            idle_poll_min_interval: Duration::from_millis(100),
            idle_poll_max_interval: Duration::from_secs(2),
            safety_poll_interval: Duration::from_secs(5),
            claim_batch_size: 8,
            notification_reconnect_interval: Duration::from_millis(250),
        });
    };
    let scheduler = SchedulerConfig {
        active_poll_interval: positive_duration(
            &raw.active_poll_interval,
            "runtime.scheduler.active_poll_interval",
        )?,
        idle_poll_min_interval: positive_duration(
            &raw.idle_poll_min_interval,
            "runtime.scheduler.idle_poll_min_interval",
        )?,
        idle_poll_max_interval: positive_duration(
            &raw.idle_poll_max_interval,
            "runtime.scheduler.idle_poll_max_interval",
        )?,
        safety_poll_interval: positive_duration(
            &raw.safety_poll_interval,
            "runtime.scheduler.safety_poll_interval",
        )?,
        claim_batch_size: raw.claim_batch_size,
        notification_reconnect_interval: positive_duration(
            &raw.notification_reconnect_interval,
            "runtime.scheduler.notification_reconnect_interval",
        )?,
    };
    if scheduler.active_poll_interval > scheduler.idle_poll_min_interval
        || scheduler.idle_poll_min_interval > scheduler.idle_poll_max_interval
        || scheduler.idle_poll_max_interval > scheduler.safety_poll_interval
        || !(1..=256).contains(&scheduler.claim_batch_size)
    {
        return Err(runtime_error(
            "runtime.scheduler intervals or claim_batch_size are invalid",
        ));
    }
    Ok(scheduler)
}

fn resolve_run_stream(
    raw: Option<RunStreamYaml>,
    deployment_mode: DeploymentMode,
) -> Result<RunStreamConfig, PlatformConfigError> {
    let Some(raw) = raw else {
        return Ok(RunStreamConfig {
            broker: match deployment_mode {
                DeploymentMode::Production => LiveRunStreamBrokerProvider::PostgresNotify,
                DeploymentMode::SingleProcessDevelopment => LiveRunStreamBrokerProvider::InProcess,
            },
            body_queue_capacity: 256,
            control_queue_capacity: 32,
            // PostgreSQL NOTIFY has an 8 KiB payload ceiling. Keep enough
            // headroom for the closed publication envelope.
            max_frame_bytes: 4 * 1_024,
            max_item_bytes: 4 * 1_024 * 1_024,
            max_run_bytes: 16 * 1_024 * 1_024,
            terminal_barrier_timeout: Duration::from_secs(2),
            outbound_write_timeout: Duration::from_secs(10),
        });
    };
    let capacities = [raw.body_queue_capacity, raw.control_queue_capacity];
    if capacities.contains(&0)
        || raw.max_frame_bytes < 256
        || raw.max_frame_bytes > 64 * 1_024
        || raw.max_item_bytes < raw.max_frame_bytes
        || raw.max_run_bytes < raw.max_item_bytes
        || raw.max_run_bytes > 256 * 1_024 * 1_024
        || (raw.broker == LiveRunStreamBrokerProvider::PostgresNotify
            && raw.max_frame_bytes > 4 * 1_024)
    {
        return Err(runtime_error("runtime.run_stream bounds are invalid"));
    }
    Ok(RunStreamConfig {
        broker: raw.broker,
        body_queue_capacity: raw.body_queue_capacity,
        control_queue_capacity: raw.control_queue_capacity,
        max_frame_bytes: raw.max_frame_bytes,
        max_item_bytes: raw.max_item_bytes,
        max_run_bytes: raw.max_run_bytes,
        terminal_barrier_timeout: positive_duration(
            &raw.terminal_barrier_timeout,
            "runtime.run_stream.terminal_barrier_timeout",
        )?,
        outbound_write_timeout: positive_duration(
            &raw.outbound_write_timeout,
            "runtime.run_stream.outbound_write_timeout",
        )?,
    })
}

fn optional_positive_duration(
    value: Option<&str>,
    default: Duration,
    field: &str,
) -> Result<Duration, PlatformConfigError> {
    value.map_or(Ok(default), |value| positive_duration(value, field))
}

fn positive_duration(value: &str, field: &str) -> Result<Duration, PlatformConfigError> {
    let duration = humantime::parse_duration(value)
        .map_err(|_| runtime_error(format!("{field} must be a valid positive duration")))?;
    if duration.is_zero() {
        return Err(runtime_error(format!("{field} must be greater than zero")));
    }
    Ok(duration)
}

fn positive_artifact_duration(value: &str, field: &str) -> Result<Duration, PlatformConfigError> {
    let duration = humantime::parse_duration(value).map_err(|_| {
        PlatformConfigError::new(
            "PLATFORM_ARTIFACTS_INVALID",
            format!("{field} must be a valid positive duration"),
        )
    })?;
    if duration.is_zero() {
        return Err(PlatformConfigError::new(
            "PLATFORM_ARTIFACTS_INVALID",
            format!("{field} must be greater than zero"),
        ));
    }
    Ok(duration)
}

fn unique_names(values: Vec<String>, field: &str) -> Result<BTreeSet<String>, PlatformConfigError> {
    let mut names = BTreeSet::new();
    for value in values {
        let value = value.trim().to_string();
        if value.is_empty() || !names.insert(value.clone()) {
            return Err(PlatformConfigError::new(
                "PLATFORM_CONFIG_INVALID",
                format!("{field} contains an empty or duplicate value"),
            ));
        }
    }
    Ok(names)
}

fn required_secret(
    name: &str,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<SecretString, PlatformConfigError> {
    if name.trim().is_empty() {
        return Err(PlatformConfigError::new(
            "PLATFORM_CONFIG_INVALID",
            "secret environment variable name must not be empty",
        ));
    }
    let value = get_env(name).ok_or_else(|| {
        PlatformConfigError::new(
            "PLATFORM_SECRET_MISSING",
            format!("required environment variable '{name}' is missing"),
        )
    })?;
    if value.trim().is_empty() {
        return Err(PlatformConfigError::new(
            "PLATFORM_SECRET_EMPTY",
            format!("required environment variable '{name}' is empty"),
        ));
    }
    Ok(SecretString(value))
}

fn runtime_error(message: impl Into<String>) -> PlatformConfigError {
    PlatformConfigError::new("PLATFORM_RUNTIME_INVALID", message)
}

fn conversation_error(message: impl Into<String>) -> PlatformConfigError {
    PlatformConfigError::new("PLATFORM_CONVERSATIONS_INVALID", message)
}

fn absolute_path(path: &Path) -> Result<PathBuf, PlatformConfigError> {
    if path.is_absolute() {
        Ok(normalize_path(path))
    } else {
        let current = env::current_dir().map_err(|_| {
            PlatformConfigError::new(
                "PLATFORM_CONFIG_READ_FAILED",
                "failed to resolve current directory",
            )
        })?;
        Ok(normalize_path(&current.join(path)))
    }
}

fn resolve_path(parent: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        normalize_path(path)
    } else {
        normalize_path(&parent.join(path))
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::Normal(value) => normalized.push(value),
        }
    }
    normalized
}
