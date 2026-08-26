//! Deployable public MCP OAuth callback ingress for the clean-cut Platform v1 candidate.
//!
//! The process owns only the bounded callback HTTP adapter and its PostgreSQL command port. Raw
//! authorization codes cross the exact MCP Host mTLS Egress RPC and never enter database rows,
//! events, logs or configuration.

mod dependency_observer;

use dependency_observer::install_callback_dependency_metrics;

use axum::{
    extract::{Extension, Request},
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use insight_platform_api::{
    build_mcp_oauth_callback_router, McpOAuthCallbackHttpState, SystemMcpOAuthCallbackClock,
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_egress_rpc::{EgressBrokerGrpcClient, EgressInternalRpcLimits};
use insight_platform_mcp_host::{
    AeadMcpOAuthStateCodec, McpOAuthCallbackIngress, McpOAuthCallbackIngressConfig,
    McpOAuthStateCodecConfig, McpOAuthStateKey, SensitiveMcpOAuthStateKey,
    UuidMcpOAuthCallbackIdentityFactory, MCP_OAUTH_STATE_KEY_BYTES,
};
use insight_platform_observability::{DependencyObservationMetrics, ProcessHttpMetrics};
use insight_platform_postgres::{
    dependency_health::run_postgres_health_sampler, repository::PgRepository, verify_schema,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error,
    fmt,
    io::Read as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use url::Url;
use uuid::Uuid;

const CONFIG_PATH_ENV: &str = "PLATFORM_CALLBACK_API_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_CALLBACK_API_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_CALLBACK_API_DATABASE_URL";
const EGRESS_CA_PATH_ENV: &str = "PLATFORM_CALLBACK_API_EGRESS_CA_PATH";
const EGRESS_CERT_PATH_ENV: &str = "PLATFORM_CALLBACK_API_EGRESS_CERT_PATH";
const EGRESS_KEY_PATH_ENV: &str = "PLATFORM_CALLBACK_API_EGRESS_KEY_PATH";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;
const OAUTH_STATE_KEY_DIRECTORY: &str = "/etc/insight/oauth-state-keys";
const CALLBACK_HTTP_OPERATIONS: &[&str] = &["live", "ready", "metrics", "oauth_callback", "other"];

fn callback_operation(path: &str) -> &'static str {
    match path {
        "/livez" => "live",
        "/readyz" => "ready",
        "/metrics" => "metrics",
        insight_platform_api::MCP_OAUTH_CALLBACK_PATH => "oauth_callback",
        _ => "other",
    }
}

fn install_callback_metrics(
    dependencies: Option<Arc<DependencyObservationMetrics>>,
) -> Arc<ProcessHttpMetrics> {
    let metrics = ProcessHttpMetrics::install("mcp-callback-api", CALLBACK_HTTP_OPERATIONS)
        .expect("static Callback API metric labels are valid");
    Arc::new(match dependencies {
        Some(dependencies) => metrics.with_dependency_observations(dependencies),
        None => metrics,
    })
}

fn build_process_router(callback: Router, metrics: Arc<ProcessHttpMetrics>) -> Router {
    Router::new()
        .route("/livez", get(callback_live))
        .route("/readyz", get(callback_ready))
        .route("/metrics", get(callback_metrics))
        .merge(callback)
        .layer(middleware::from_fn(observe_callback_request))
        .layer(Extension(metrics))
}

async fn callback_live() -> Response {
    callback_health("live")
}

async fn callback_ready(Extension(metrics): Extension<Arc<ProcessHttpMetrics>>) -> Response {
    if metrics.is_ready() {
        callback_health("ready")
    } else {
        callback_response(StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

async fn callback_metrics(Extension(metrics): Extension<Arc<ProcessHttpMetrics>>) -> Response {
    let mut response = metrics.render_prometheus().into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn observe_callback_request(request: Request, next: Next) -> Response {
    let metrics = request
        .extensions()
        .get::<Arc<ProcessHttpMetrics>>()
        .cloned()
        .expect("Callback API metrics Extension is installed by build_process_router");
    let operation = callback_operation(request.uri().path());
    let started = Instant::now();
    let response = next.run(request).await;
    metrics.observe(operation, response.status().as_u16(), started.elapsed());
    response
}

fn callback_health(state: &'static str) -> Response {
    callback_response(StatusCode::OK, state)
}

fn callback_response(status: StatusCode, body: &'static str) -> Response {
    let mut response = (status, body).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    listen_address: String,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    egress_endpoint: String,
    egress_tls_server_name: String,
    egress_connect_timeout_milliseconds: u64,
    egress_request_timeout_milliseconds: u64,
    maximum_rpc_metadata_bytes: usize,
    maximum_rpc_payload_bytes: usize,
    callback_binding_digest: Sha256Digest,
    callback_receipt_ttl_seconds: i64,
    oauth_state: OAuthStateProcessConfig,
    shutdown_grace_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct OAuthStateProcessConfig {
    active_key_id: String,
    maximum_lifetime_seconds: i64,
    clock_skew_seconds: i64,
    keys: Vec<OAuthStateKeyFile>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct OAuthStateKeyFile {
    key_id: String,
    key_material_digest: Sha256Digest,
    key_material_path: String,
}

impl OAuthStateProcessConfig {
    fn load(
        &self,
        callback_binding_digest: Sha256Digest,
    ) -> Result<AeadMcpOAuthStateCodec, ProcessError> {
        let keys = self
            .keys
            .iter()
            .map(|key| {
                let path = PathBuf::from(&key.key_material_path);
                if !valid_projected_key_path(&path) {
                    return Err(ProcessError::InvalidConfiguration);
                }
                let mut material = read_exact_projected_file(&path, MCP_OAUTH_STATE_KEY_BYTES)?;
                if raw_digest(&material)? != key.key_material_digest {
                    material.fill(0);
                    return Err(ProcessError::InvalidConfiguration);
                }
                Ok(McpOAuthStateKey {
                    key_id: key.key_id.clone(),
                    key_material: SensitiveMcpOAuthStateKey::new(material)
                        .map_err(|_| ProcessError::InvalidConfiguration)?,
                })
            })
            .collect::<Result<Vec<_>, ProcessError>>()?;
        AeadMcpOAuthStateCodec::new(
            McpOAuthStateCodecConfig {
                active_key_id: self.active_key_id.clone(),
                callback_binding_digest,
                maximum_lifetime_seconds: self.maximum_lifetime_seconds,
                clock_skew_seconds: self.clock_skew_seconds,
            },
            keys,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)
    }
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded_file(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 32,
                max_items_per_array: 32,
                max_properties_per_object: 64,
                max_string_bytes: 16_384,
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let expected: Sha256Digest = required(CONFIG_DIGEST_ENV)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let actual: Sha256Digest = canonical_digest(&value)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if actual != expected {
            return Err(ProcessError::InvalidConfiguration);
        }
        let config: Self =
            serde_json::from_value(value).map_err(|_| ProcessError::InvalidConfiguration)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ProcessError> {
        let listen: SocketAddr = self
            .listen_address
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let egress =
            Url::parse(&self.egress_endpoint).map_err(|_| ProcessError::InvalidConfiguration)?;
        if self.schema_version != 1
            || listen.port() == 0
            || !(2..=32).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
            || egress.scheme() != "https"
            || egress.host_str().is_none()
            || egress.port().is_none()
            || egress.username() != ""
            || egress.password().is_some()
            || egress.path() != "/"
            || egress.query().is_some()
            || egress.fragment().is_some()
            || !valid_tls_server_name(&self.egress_tls_server_name)
            || self.egress_connect_timeout_milliseconds == 0
            || self.egress_connect_timeout_milliseconds > 30_000
            || self.egress_request_timeout_milliseconds == 0
            || self.egress_request_timeout_milliseconds > 3_600_000
            || self.callback_receipt_ttl_seconds <= 0
            || self.callback_receipt_ttl_seconds > 86_400
            || self.shutdown_grace_milliseconds == 0
            || self.shutdown_grace_milliseconds > 60_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        EgressInternalRpcLimits::new(
            self.maximum_rpc_metadata_bytes,
            self.maximum_rpc_payload_bytes,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        self.oauth_state
            .load(self.callback_binding_digest.clone())?;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-callback-api failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ProcessError> {
    let config = ProcessConfig::load()?;
    let database_url = required(DATABASE_URL_ENV)?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&database_url)
        .await
        .map_err(|_| ProcessError::DatabaseUnavailable)?;
    verify_schema(&pool)
        .await
        .map_err(|_| ProcessError::SchemaMismatch)?;
    let database_health_pool = pool.clone();
    let repository = Arc::new(PgRepository::new(pool));
    let (dependency_metrics, postgres_observer, egress_observer) =
        install_callback_dependency_metrics().map_err(|_| ProcessError::InvalidConfiguration)?;

    let rpc_limits = EgressInternalRpcLimits::new(
        config.maximum_rpc_metadata_bytes,
        config.maximum_rpc_payload_bytes,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let egress_channel = connect_egress(&config).await?;
    let broker = Arc::new(EgressBrokerGrpcClient::new_with_observer(
        egress_channel,
        rpc_limits,
        egress_observer,
    ));
    let states = Arc::new(
        config
            .oauth_state
            .load(config.callback_binding_digest.clone())?,
    );
    let generation_id =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
    let application = Arc::new(
        McpOAuthCallbackIngress::new(
            McpOAuthCallbackIngressConfig {
                callback_ingress_generation_id: generation_id,
                callback_binding_digest: config.callback_binding_digest,
                receipt_ttl_seconds: config.callback_receipt_ttl_seconds,
            },
            states,
            Arc::new(UuidMcpOAuthCallbackIdentityFactory),
            repository,
            broker,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?,
    );
    let callback = build_mcp_oauth_callback_router(McpOAuthCallbackHttpState::new(
        application,
        Arc::new(SystemMcpOAuthCallbackClock),
    ));
    let metrics = install_callback_metrics(Some(dependency_metrics));
    let router = build_process_router(callback, Arc::clone(&metrics));
    let listener = tokio::net::TcpListener::bind(&config.listen_address)
        .await
        .map_err(|_| ProcessError::ServerFailure)?;
    metrics.mark_ready();
    let cancellation = CancellationToken::new();
    let server = axum::serve(listener, router)
        .with_graceful_shutdown(cancellation.child_token().cancelled_owned());
    let mut server = tokio::spawn(async move { server.await });
    let mut postgres_health = tokio::spawn(run_postgres_health_sampler(
        database_health_pool,
        postgres_observer,
        cancellation.child_token(),
    ));
    let shutdown_grace = Duration::from_millis(config.shutdown_grace_milliseconds);
    tokio::select! {
        _ = shutdown_signal() => {
            cancellation.cancel();
            let drained = tokio::time::timeout(shutdown_grace, async {
                let (server_result, postgres_result) = tokio::join!(
                    &mut server,
                    &mut postgres_health,
                );
                server_result.map_err(|_| ProcessError::ServerFailure)?
                    .map_err(|_| ProcessError::ServerFailure)?;
                postgres_result.map_err(|_| ProcessError::DependencyObserverFailure)
            }).await;
            match drained {
                Ok(result) => result,
                Err(_) => {
                    server.abort();
                    postgres_health.abort();
                    let _ = tokio::join!(server, postgres_health);
                    Err(ProcessError::ShutdownTimeout)
                }
            }
        }
        result = &mut server => {
            cancellation.cancel();
            let server_result = result.map_err(|_| ProcessError::ServerFailure)
                .and_then(|result| result.map_err(|_| ProcessError::ServerFailure));
            let postgres_result = postgres_health.await
                .map_err(|_| ProcessError::DependencyObserverFailure);
            server_result?;
            postgres_result?;
            Err(ProcessError::ServerFailure)
        }
        result = &mut postgres_health => {
            cancellation.cancel();
            let postgres_result = result.map_err(|_| ProcessError::DependencyObserverFailure);
            let server_result = server.await.map_err(|_| ProcessError::ServerFailure)
                .and_then(|result| result.map_err(|_| ProcessError::ServerFailure));
            postgres_result?;
            server_result?;
            Err(ProcessError::DependencyObserverFailure)
        }
    }
}

async fn connect_egress(config: &ProcessConfig) -> Result<tonic::transport::Channel, ProcessError> {
    let tls = ClientTlsConfig::new()
        .domain_name(config.egress_tls_server_name.clone())
        .ca_certificate(Certificate::from_pem(read_bounded_file(
            &required_absolute_path(EGRESS_CA_PATH_ENV)?,
            MAX_TLS_FILE_BYTES,
        )?))
        .identity(Identity::from_pem(
            read_bounded_file(
                &required_absolute_path(EGRESS_CERT_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
            read_bounded_file(
                &required_absolute_path(EGRESS_KEY_PATH_ENV)?,
                MAX_TLS_FILE_BYTES,
            )?,
        ));
    Endpoint::from_shared(config.egress_endpoint.clone())
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect_timeout(Duration::from_millis(
            config.egress_connect_timeout_milliseconds,
        ))
        .timeout(Duration::from_millis(
            config.egress_request_timeout_milliseconds,
        ))
        .tls_config(tls)
        .map_err(|_| ProcessError::InvalidConfiguration)?
        .connect()
        .await
        .map_err(|_| ProcessError::EgressUnavailable)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = terminate.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn valid_tls_server_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && !value.starts_with('.')
        && !value.ends_with('.')
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn required(name: &'static str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= 16_384)
        .ok_or(ProcessError::MissingConfiguration(name))
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn valid_projected_key_path(path: &Path) -> bool {
    path.is_absolute()
        && path.parent() == Some(Path::new(OAUTH_STATE_KEY_DIRECTORY))
        && path.file_name().is_some()
}

fn read_exact_projected_file(path: &Path, exact_size: usize) -> Result<Vec<u8>, ProcessError> {
    let mut bytes = read_opened_file(path, exact_size.saturating_add(1))?;
    if bytes.len() != exact_size {
        bytes.fill(0);
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

fn read_bounded_file(path: &Path, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let bytes = read_opened_file(path, maximum)?;
    if bytes.is_empty() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

fn read_opened_file(path: &Path, maximum: usize) -> Result<Vec<u8>, ProcessError> {
    let file = std::fs::File::open(path).map_err(|_| ProcessError::FileUnavailable)?;
    let metadata = file.metadata().map_err(|_| ProcessError::FileUnavailable)?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX)
    {
        return Err(ProcessError::InvalidConfiguration);
    }
    let mut bytes = Vec::with_capacity(maximum.min(64 * 1_024));
    let read_result = file
        .take(u64::try_from(maximum.saturating_add(1)).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes);
    if read_result.is_err() || bytes.is_empty() || bytes.len() > maximum {
        bytes.fill(0);
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

fn raw_digest(bytes: &[u8]) -> Result<Sha256Digest, ProcessError> {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").map_err(|_| ProcessError::InvalidConfiguration)?;
    }
    encoded
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)
}

#[derive(Debug)]
enum ProcessError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    FileUnavailable,
    DatabaseUnavailable,
    SchemaMismatch,
    EgressUnavailable,
    ServerFailure,
    DependencyObserverFailure,
    ShutdownTimeout,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => {
                write!(formatter, "required configuration {name} is missing")
            }
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::FileUnavailable => formatter.write_str("configuration file is unavailable"),
            Self::DatabaseUnavailable => formatter.write_str("database is unavailable"),
            Self::SchemaMismatch => formatter.write_str("database schema does not match candidate"),
            Self::EgressUnavailable => formatter.write_str("Egress Broker is unavailable"),
            Self::ServerFailure => formatter.write_str("callback HTTP server failed"),
            Self::DependencyObserverFailure => {
                formatter.write_str("callback dependency observer failed")
            }
            Self::ShutdownTimeout => formatter.write_str("graceful shutdown timed out"),
        }
    }
}

impl Error for ProcessError {}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get};
    use tower::ServiceExt;

    #[test]
    fn oauth_state_key_paths_are_confined() {
        assert!(valid_projected_key_path(Path::new(
            "/etc/insight/oauth-state-keys/current"
        )));
        assert!(!valid_projected_key_path(Path::new(
            "/etc/insight/oauth-state-keys/../other/current"
        )));
        assert!(!valid_projected_key_path(Path::new("current")));
    }

    #[test]
    fn tls_server_name_is_closed() {
        assert!(valid_tls_server_name("egress.platform-egress.svc"));
        assert!(!valid_tls_server_name("https://egress"));
        assert!(!valid_tls_server_name("egress/path"));
    }

    #[tokio::test]
    async fn process_routes_expose_health_and_bounded_metrics() {
        let metrics = install_callback_metrics(None);
        metrics.mark_ready();
        let router = build_process_router(
            Router::new().route(
                insight_platform_api::MCP_OAUTH_CALLBACK_PATH,
                get(|| async { StatusCode::BAD_REQUEST }),
            ),
            metrics,
        );
        let callback = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/mcp/oauth/callback?state=secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(callback.status(), StatusCode::BAD_REQUEST);
        let ready = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ready.status(), StatusCode::OK);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 65_536)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("component_role=\"mcp-callback-api\",operation=\"oauth_callback\",outcome=\"rejected\"} 1"));
        assert!(!body.contains("state=secret"));
    }
}
