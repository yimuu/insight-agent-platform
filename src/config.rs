//! Strict platform configuration for the canonical v2 runtime.

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
    pub operation_cancel_grace_period: Duration,
    pub max_template_output_bytes: usize,
    pub run_timeout: Duration,
    pub sse_keep_alive_interval: Duration,
    pub subscriber_capacity: usize,
    pub journal_capacity: usize,
    pub journal_batch_size: usize,
    pub journal_operation_timeout: Duration,
    pub readiness_probe_timeout: Duration,
    pub shutdown_grace_period: Duration,
    pub shutdown_hard_deadline: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformConfig {
    pub bind_addr: SocketAddr,
    pub auth: AuthConfig,
    pub agents: AgentsConfig,
    pub models: ModelsConfig,
    pub actions: ActionsConfig,
    pub history: HistoryConfig,
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
        let auth = resolve_auth(raw.auth, &get_env)?;
        let agents = AgentsConfig {
            directory: resolve_path(parent, &raw.agents.directory),
            enabled: unique_names(raw.agents.enabled, "agents.enabled")?,
        };
        let models = ModelsConfig {
            config: resolve_path(parent, &raw.models.config),
        };
        let actions = resolve_actions(raw.actions)?;
        let history = resolve_history(parent, raw.history, &get_env)?;
        let runtime = resolve_runtime(raw.runtime)?;
        Ok(Self {
            bind_addr,
            auth,
            agents,
            models,
            actions,
            history,
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
    bind_addr: String,
    auth: AuthYaml,
    agents: AgentsYaml,
    models: ModelsYaml,
    actions: ActionsYaml,
    history: HistoryYaml,
    runtime: RuntimeYaml,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum AuthYaml {
    Disabled,
    BearerEnv { token_env: String },
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
struct RuntimeYaml {
    max_concurrent_runs: usize,
    max_concurrent_operations: usize,
    max_concurrent_operations_per_run: usize,
    operation_timeout: String,
    operation_cancel_grace_period: String,
    max_template_output_bytes: usize,
    run_timeout: String,
    sse_keep_alive_interval: String,
    subscriber_capacity: usize,
    journal_capacity: usize,
    journal_batch_size: usize,
    journal_operation_timeout: String,
    readiness_probe_timeout: Option<String>,
    shutdown_grace_period: Option<String>,
    shutdown_hard_deadline: Option<String>,
}

fn resolve_auth(
    auth: AuthYaml,
    get_env: &impl Fn(&str) -> Option<String>,
) -> Result<AuthConfig, PlatformConfigError> {
    match auth {
        AuthYaml::Disabled => Ok(AuthConfig::Disabled),
        AuthYaml::BearerEnv { token_env } => {
            let token = required_secret(&token_env, get_env)?;
            Ok(AuthConfig::Bearer { token })
        }
    }
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

fn resolve_runtime(raw: RuntimeYaml) -> Result<RuntimeConfig, PlatformConfigError> {
    let capacities = [
        raw.max_concurrent_runs,
        raw.max_concurrent_operations,
        raw.max_concurrent_operations_per_run,
        raw.max_template_output_bytes,
        raw.subscriber_capacity,
        raw.journal_capacity,
        raw.journal_batch_size,
    ];
    if capacities.contains(&0) || raw.journal_batch_size > raw.journal_capacity {
        return Err(runtime_error(
            "runtime capacities must be positive and batch size must not exceed journal capacity",
        ));
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
    if shutdown_hard_deadline <= shutdown_grace_period {
        return Err(runtime_error(
            "runtime.shutdown_hard_deadline must be greater than runtime.shutdown_grace_period",
        ));
    }
    Ok(RuntimeConfig {
        max_concurrent_runs: raw.max_concurrent_runs,
        max_concurrent_operations: raw.max_concurrent_operations,
        max_concurrent_operations_per_run: raw.max_concurrent_operations_per_run,
        operation_timeout: positive_duration(&raw.operation_timeout, "runtime.operation_timeout")?,
        operation_cancel_grace_period: positive_duration(
            &raw.operation_cancel_grace_period,
            "runtime.operation_cancel_grace_period",
        )?,
        max_template_output_bytes: raw.max_template_output_bytes,
        run_timeout: positive_duration(&raw.run_timeout, "runtime.run_timeout")?,
        sse_keep_alive_interval: positive_duration(
            &raw.sse_keep_alive_interval,
            "runtime.sse_keep_alive_interval",
        )?,
        subscriber_capacity: raw.subscriber_capacity,
        journal_capacity: raw.journal_capacity,
        journal_batch_size: raw.journal_batch_size,
        journal_operation_timeout: positive_duration(
            &raw.journal_operation_timeout,
            "runtime.journal_operation_timeout",
        )?,
        readiness_probe_timeout,
        shutdown_grace_period,
        shutdown_hard_deadline,
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
