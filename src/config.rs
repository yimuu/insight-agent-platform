//! Strict platform configuration for the durable v3 runtime.

use std::{
    collections::BTreeSet,
    env, fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use serde::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgSslMode};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelsConfig {
    pub config: PathBuf,
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
    Sqlite { path: PathBuf },
    Postgres { database_url: SecretString },
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
pub enum LiveResponseBrokerProvider {
    InProcess,
    PostgresNotify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseStreamConfig {
    pub broker: LiveResponseBrokerProvider,
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
    /// Maximum bytes returned by one authorized Artifact read.
    pub max_read_bytes: usize,
    pub orphan_retention: Duration,
    /// Audit/recovery hold for Artifacts that became part of a durable Run.
    pub reference_retention: Duration,
    pub gc_interval: Duration,
    pub deletion_claim_seconds: u32,
}

impl HistoryConfig {
    pub fn database_url(&self) -> Option<&str> {
        match self {
            Self::Sqlite { .. } => None,
            Self::Postgres { database_url } => Some(database_url.expose()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub max_concurrent_runs: usize,
    pub max_concurrent_operations: usize,
    pub max_concurrent_operations_per_run: usize,
    pub operation_timeout: Duration,
    pub run_timeout: Duration,
    pub sse_keep_alive_interval: Duration,
    pub subscriber_capacity: usize,
    pub response_stream: ResponseStreamConfig,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformConfig {
    pub deployment_mode: DeploymentMode,
    pub bind_addr: SocketAddr,
    pub auth: AuthConfig,
    pub human_task_credentials: Vec<HumanTaskCredentialConfig>,
    pub agents: AgentsConfig,
    pub models: ModelsConfig,
    pub actions: ActionsConfig,
    pub history: HistoryConfig,
    pub artifacts: ArtifactsConfig,
    pub runtime: RuntimeConfig,
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
        let raw: PlatformYaml = crate::yaml::from_str(&yaml, crate::yaml::YamlSurface::Platform)
            .map_err(|error| {
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
        let agents = AgentsConfig {
            directory: resolve_path(parent, &raw.agents.directory),
            enabled: unique_names(raw.agents.enabled, "agents.enabled")?,
        };
        let models = ModelsConfig {
            config: resolve_path(parent, &raw.models.config),
        };
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
        let artifacts = resolve_artifacts(parent, raw.artifacts, raw.deployment_mode)?;
        let runtime = resolve_runtime(raw.runtime, raw.deployment_mode)?;
        Ok(Self {
            deployment_mode: raw.deployment_mode,
            bind_addr,
            auth,
            human_task_credentials,
            agents,
            models,
            actions,
            history,
            artifacts,
            runtime,
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
    agents: AgentsYaml,
    models: ModelsYaml,
    actions: ActionsYaml,
    history: HistoryYaml,
    #[serde(default)]
    artifacts: Option<ArtifactsYaml>,
    runtime: RuntimeYaml,
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
struct ModelsYaml {
    config: PathBuf,
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
    Sqlite { path: PathBuf },
    Postgres { database_url_env: String },
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
    max_read_bytes: Option<usize>,
    orphan_retention: String,
    #[serde(default)]
    reference_retention: Option<String>,
    gc_interval: String,
    deletion_claim_seconds: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeYaml {
    max_concurrent_runs: usize,
    max_concurrent_operations: usize,
    max_concurrent_operations_per_run: usize,
    operation_timeout: String,
    run_timeout: String,
    sse_keep_alive_interval: String,
    subscriber_capacity: usize,
    #[serde(default)]
    response_stream: Option<ResponseStreamYaml>,
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
struct ResponseStreamYaml {
    broker: LiveResponseBrokerProvider,
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

fn resolve_history(
    parent: &Path,
    history: HistoryYaml,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<HistoryConfig, PlatformConfigError> {
    match history {
        HistoryYaml::Sqlite { path } => Ok(HistoryConfig::Sqlite {
            path: resolve_path(parent, &path),
        }),
        HistoryYaml::Postgres { database_url_env } => {
            let database_url = required_secret(&database_url_env, get_env)?;
            validate_postgres_history_url(database_url.expose())?;
            Ok(HistoryConfig::Postgres { database_url })
        }
    }
}

fn resolve_artifacts(
    parent: &Path,
    artifacts: Option<ArtifactsYaml>,
    deployment_mode: DeploymentMode,
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
    Ok(ArtifactsConfig {
        provider: artifacts.provider,
        namespace: artifacts.namespace,
        directory: resolve_path(parent, &artifacts.directory),
        inline_threshold_bytes: artifacts.inline_threshold_bytes,
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
    if !raw_postgres_scheme_is_allowed(database_url) {
        return Err(PlatformConfigError::new(
            "PLATFORM_CONFIG_INVALID",
            "PostgreSQL history URL must be a valid postgres:// or postgresql:// URL",
        ));
    }

    let options = PgConnectOptions::from_str(database_url).map_err(|_| {
        PlatformConfigError::new(
            "PLATFORM_CONFIG_INVALID",
            "PostgreSQL history URL must be a valid postgres:// or postgresql:// URL",
        )
    })?;

    if matches!(options.get_ssl_mode(), PgSslMode::VerifyFull) {
        return Ok(());
    }
    if options.get_socket().is_some() || raw_postgres_host_is_exact_local(database_url) {
        return Ok(());
    }

    Err(PlatformConfigError::new(
        "PLATFORM_CONFIG_INVALID",
        "remote PostgreSQL history URL must set sslmode=verify-full; plaintext is allowed only for exact loopback or Unix socket development URLs",
    ))
}

fn raw_postgres_scheme_is_allowed(database_url: &str) -> bool {
    database_url.starts_with("postgres://") || database_url.starts_with("postgresql://")
}

fn raw_postgres_host_is_exact_local(database_url: &str) -> bool {
    last_query_value(database_url, &["host", "hostaddr"])
        .map(|host| exact_local_postgres_host(&host))
        .unwrap_or_else(|| {
            raw_authority_host(database_url)
                .as_deref()
                .map(exact_local_postgres_host)
                .unwrap_or(false)
        })
}

fn exact_local_postgres_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1")
}

fn last_query_value(database_url: &str, names: &[&str]) -> Option<String> {
    let query = raw_query(database_url)?;
    let mut value = None;

    for pair in query.split('&') {
        let (key, pair_value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_query_component(key);
        if names.contains(&key.as_str()) {
            value = Some(decode_query_component(pair_value));
        }
    }

    value
}

fn decode_query_component(component: &str) -> String {
    let bytes = component.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                if let (Some(high), Some(low)) =
                    (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
                {
                    decoded.push((high << 4) | low);
                    index += 3;
                } else {
                    decoded.push(bytes[index]);
                    index += 1;
                }
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn raw_query(database_url: &str) -> Option<&str> {
    let (_, rest) = database_url.split_once('?')?;
    Some(rest.split_once('#').map_or(rest, |(query, _)| query))
}

fn raw_authority_host(database_url: &str) -> Option<String> {
    let (_, rest) = database_url.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if host_port.starts_with('[') {
        let end = host_port.find(']')?;
        return Some(host_port[..=end].to_string());
    }
    Some(host_port.split(':').next().unwrap_or(host_port).to_string())
}

fn resolve_runtime(
    raw: RuntimeYaml,
    deployment_mode: DeploymentMode,
) -> Result<RuntimeConfig, PlatformConfigError> {
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
    let response_stream = resolve_response_stream(raw.response_stream, deployment_mode)?;
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
        response_stream,
        max_llm_tool_rounds,
        max_llm_tool_calls,
        public_event_retention,
        public_event_prune_interval,
        readiness_probe_timeout,
        shutdown_grace_period,
        shutdown_hard_deadline,
    })
}

fn resolve_response_stream(
    raw: Option<ResponseStreamYaml>,
    deployment_mode: DeploymentMode,
) -> Result<ResponseStreamConfig, PlatformConfigError> {
    let Some(raw) = raw else {
        return Ok(ResponseStreamConfig {
            broker: match deployment_mode {
                DeploymentMode::Production => LiveResponseBrokerProvider::PostgresNotify,
                DeploymentMode::SingleProcessDevelopment => LiveResponseBrokerProvider::InProcess,
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
        || (raw.broker == LiveResponseBrokerProvider::PostgresNotify
            && raw.max_frame_bytes > 4 * 1_024)
    {
        return Err(runtime_error("runtime.response_stream bounds are invalid"));
    }
    Ok(ResponseStreamConfig {
        broker: raw.broker,
        body_queue_capacity: raw.body_queue_capacity,
        control_queue_capacity: raw.control_queue_capacity,
        max_frame_bytes: raw.max_frame_bytes,
        max_item_bytes: raw.max_item_bytes,
        max_run_bytes: raw.max_run_bytes,
        terminal_barrier_timeout: positive_duration(
            &raw.terminal_barrier_timeout,
            "runtime.response_stream.terminal_barrier_timeout",
        )?,
        outbound_write_timeout: positive_duration(
            &raw.outbound_write_timeout,
            "runtime.response_stream.outbound_write_timeout",
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
