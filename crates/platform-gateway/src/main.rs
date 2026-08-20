//! Deployable public Gateway for the clean-cut Platform `/v1` contract.

use async_trait::async_trait;
use axum::{
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use insight_platform_api::{
    authentication::{
        authenticate_public_request, AuthenticationError, ExternalPrincipalBindingAuthority,
        PublicAuthenticationState, SystemAuthenticationClock,
    },
    oidc::InstalledOidcVerifierConfig,
    operation::{
        build_operation_router, OperationApplication, OperationApplicationError,
        OperationHttpState, SystemOperationClock,
    },
    resource::{
        build_resource_router, resource_etag, CreateResourceIntent, ReadResourceIntent,
        ResourceApplication, ResourceApplicationError, ResourceHttpState, ResourceViewV1,
        SystemResourceClock,
    },
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, AdministrativeGate, CommandAudit, EntityLifecycle,
    JsonLimits, OperationViewV1, PrincipalKind, PrincipalSnapshot, ReadOperation, ResourceId,
    ResourceKind, Sha256Digest,
};
use insight_platform_postgres::{
    operation_repository::OperationReadError,
    repository::{PgRepository, RepositoryError},
    verify_schema,
};
use insight_platform_registry::CreateResourceDraft;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error,
    fmt,
    fs::File,
    io::Read as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const CONFIG_PATH_ENV: &str = "PLATFORM_GATEWAY_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_GATEWAY_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_GATEWAY_DATABASE_URL";
const MAX_CONFIG_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    schema_version: u32,
    listen_address: String,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    shutdown_grace_milliseconds: u64,
    oidc: InstalledOidcVerifierConfig,
}

impl ProcessConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded_file(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 16,
                max_items_per_array: 16,
                max_properties_per_object: 32,
                max_string_bytes: 524_288,
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
        if self.schema_version != 1
            || listen.port() == 0
            || !(2..=64).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0
            || self.database_acquire_timeout_milliseconds > 30_000
            || self.shutdown_grace_milliseconds == 0
            || self.shutdown_grace_milliseconds > 60_000
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PgPrincipalBindings(Arc<PgRepository>);

#[async_trait]
impl ExternalPrincipalBindingAuthority for PgPrincipalBindings {
    async fn resolve_external_principal(
        &self,
        tenant_id: ResourceId,
        authentication_authority_digest: Sha256Digest,
        subject_digest: Sha256Digest,
        asserted_principal_kind: PrincipalKind,
    ) -> Result<PrincipalSnapshot, AuthenticationError> {
        self.0
            .resolve_external_principal(
                tenant_id,
                authentication_authority_digest,
                subject_digest,
                asserted_principal_kind,
            )
            .await
            .map_err(|error| match error {
                RepositoryError::InvalidInput(_)
                | RepositoryError::NotFound(_)
                | RepositoryError::PermissionDenied => AuthenticationError::Unauthenticated,
                _ => AuthenticationError::Unavailable,
            })
    }
}

#[derive(Clone)]
struct PgOperations(Arc<PgRepository>);

#[async_trait]
impl OperationApplication for PgOperations {
    async fn read_operation(
        &self,
        request: ReadOperation,
    ) -> Result<OperationViewV1, OperationApplicationError> {
        self.0
            .read_public_operation(&request)
            .await
            .map_err(|error| match error {
                OperationReadError::InvalidRequest => OperationApplicationError::Invalid,
                OperationReadError::Denied => OperationApplicationError::Denied,
                OperationReadError::NotFound => OperationApplicationError::NotFound,
                OperationReadError::NotPublic => OperationApplicationError::NotPublic,
                OperationReadError::AuthorityUnavailable => OperationApplicationError::Unavailable,
                OperationReadError::CorruptAuthority => OperationApplicationError::Internal,
            })
    }
}

#[derive(Clone)]
struct PgResources(Arc<PgRepository>);

#[async_trait]
impl ResourceApplication for PgResources {
    async fn create_resource(
        &self,
        intent: CreateResourceIntent,
    ) -> Result<ResourceViewV1, ResourceApplicationError> {
        let now = chrono::Utc::now();
        if intent.deadline <= now {
            return Err(ResourceApplicationError::Unavailable);
        }
        let candidate_resource_id = new_id(intent.resource_kind.id_kind())?;
        let audit = CommandAudit {
            tenant_id: intent.principal.tenant_id,
            principal_id: intent.principal.principal_id,
            principal_kind: intent.principal.principal_kind,
            receipt_id: new_id(ResourceKind::Receipt)?,
            event_id: new_id(ResourceKind::Event)?,
            outbox_id: new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: intent.idempotency_key_digest,
            request_digest: intent.request_digest,
            receipt_expires_at: now + chrono::Duration::hours(24),
        };
        let draft = intent.draft;
        let mut transaction = self
            .0
            .begin_registry_transaction()
            .await
            .map_err(map_resource_repository_error)?;
        let outcome = transaction
            .create_resource_draft(CreateResourceDraft {
                audit,
                resource_id: candidate_resource_id,
                draft: draft.clone(),
            })
            .await
            .map_err(map_resource_repository_error)?;
        transaction
            .commit()
            .await
            .map_err(map_resource_repository_error)?;
        let record = match outcome {
            insight_platform_contracts::CommandOutcome::Applied(record)
            | insight_platform_contracts::CommandOutcome::Replayed(record) => record,
        };
        let resource_id: ResourceId = record
            .resource_id
            .parse()
            .map_err(|_| ResourceApplicationError::Internal)?;
        if resource_id.kind() != intent.resource_kind.id_kind() {
            return Err(ResourceApplicationError::Internal);
        }
        Ok(ResourceViewV1 {
            schema_version: 1,
            resource_id: resource_id.clone(),
            resource_kind: intent.resource_kind,
            lifecycle_state: EntityLifecycle::Active,
            gate_state: AdministrativeGate::Enabled,
            draft_generation: 1,
            version: 1,
            draft,
            etag: resource_etag(&resource_id, 1),
        })
    }

    async fn read_resource(
        &self,
        intent: ReadResourceIntent,
    ) -> Result<ResourceViewV1, ResourceApplicationError> {
        if intent.deadline <= chrono::Utc::now() {
            return Err(ResourceApplicationError::Unavailable);
        }
        let record = self
            .0
            .read_resource_for_principal(
                &intent.principal.tenant_id,
                &intent.principal.principal_id,
                intent.principal.principal_kind,
                intent.resource_kind,
                &intent.resource_id,
            )
            .await
            .map_err(map_resource_repository_error)?;
        if record.payload.schema_version != 1 {
            return Err(ResourceApplicationError::Internal);
        }
        let mut value = record.payload.value;
        let object = value
            .as_object_mut()
            .ok_or(ResourceApplicationError::Internal)?;
        object.remove("schema_version");
        let draft =
            serde_json::from_value(value).map_err(|_| ResourceApplicationError::Internal)?;
        let version =
            u64::try_from(record.version).map_err(|_| ResourceApplicationError::Internal)?;
        let draft_generation = u64::try_from(record.draft_generation)
            .map_err(|_| ResourceApplicationError::Internal)?;
        Ok(ResourceViewV1 {
            schema_version: 1,
            resource_id: intent.resource_id.clone(),
            resource_kind: intent.resource_kind,
            lifecycle_state: record
                .lifecycle_state
                .parse()
                .map_err(|_| ResourceApplicationError::Internal)?,
            gate_state: record
                .gate_state
                .parse()
                .map_err(|_| ResourceApplicationError::Internal)?,
            draft_generation,
            version,
            draft,
            etag: resource_etag(&intent.resource_id, version),
        })
    }
}

fn new_id(kind: ResourceKind) -> Result<ResourceId, ResourceApplicationError> {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7())
        .map_err(|_| ResourceApplicationError::Internal)
}

fn map_resource_repository_error(error: RepositoryError) -> ResourceApplicationError {
    match error {
        RepositoryError::InvalidInput(_) => ResourceApplicationError::Invalid,
        RepositoryError::NotFound(_) => ResourceApplicationError::NotFound,
        RepositoryError::Conflict(_) | RepositoryError::StaleFence => {
            ResourceApplicationError::Conflict
        }
        RepositoryError::PermissionDenied => ResourceApplicationError::Denied,
        RepositoryError::IdempotencyConflict => ResourceApplicationError::IdempotencyConflict,
        RepositoryError::Database(_) | RepositoryError::LeaseExpired => {
            ResourceApplicationError::Unavailable
        }
        RepositoryError::QuotaExceeded | RepositoryError::CorruptRow(_) => {
            ResourceApplicationError::Internal
        }
    }
}

fn build_router(
    repository: Arc<PgRepository>,
    verifier: insight_platform_api::oidc::InstalledOidcVerifier,
) -> Router {
    let authentication = PublicAuthenticationState::new(
        Arc::new(verifier),
        Arc::new(PgPrincipalBindings(repository.clone())),
        Arc::new(SystemAuthenticationClock),
    );
    let operation = build_operation_router(OperationHttpState::new(
        Arc::new(PgOperations(repository.clone())),
        Arc::new(SystemOperationClock),
    ));
    let resource = build_resource_router(ResourceHttpState::new(
        Arc::new(PgResources(repository)),
        Arc::new(SystemResourceClock),
    ));
    let protected = operation
        .merge(resource)
        .route_layer(middleware::from_fn_with_state(
            authentication,
            authenticate_public_request,
        ));
    Router::new()
        .route("/livez", get(live))
        .route("/readyz", get(ready))
        .merge(protected)
}

async fn live() -> Response {
    health("live")
}

async fn ready() -> Response {
    health("ready")
}

fn health(state: &'static str) -> Response {
    let mut response = (StatusCode::OK, state).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[derive(Debug)]
enum ProcessError {
    InvalidConfiguration,
    Io(std::io::Error),
    Database(sqlx::Error),
    Schema(insight_platform_postgres::AuthoritySchemaError),
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => formatter.write_str("configuration is invalid"),
            Self::Io(error) => write!(formatter, "I/O failed: {error}"),
            Self::Database(error) => write!(formatter, "database failed: {error}"),
            Self::Schema(error) => write!(formatter, "schema verification failed: {error}"),
        }
    }
}

impl Error for ProcessError {}

impl From<std::io::Error> for ProcessError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<sqlx::Error> for ProcessError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

fn required(name: &str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= 16_384)
        .ok_or(ProcessError::InvalidConfiguration)
}

fn required_absolute_path(name: &str) -> Result<PathBuf, ProcessError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded_file(path: &Path, maximum_bytes: usize) -> Result<Vec<u8>, ProcessError> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(u64::try_from(maximum_bytes).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > maximum_bytes {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

async fn shutdown_signal(grace: Duration) {
    let _ = tokio::signal::ctrl_c().await;
    tokio::time::sleep(grace.min(Duration::from_secs(1))).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = ProcessConfig::load()?;
    let verifier = config
        .oidc
        .clone()
        .install()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let database_url = required(DATABASE_URL_ENV)?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&database_url)
        .await?;
    verify_schema(&pool).await.map_err(ProcessError::Schema)?;
    let repository = Arc::new(PgRepository::new(pool));
    let listener = tokio::net::TcpListener::bind(&config.listen_address).await?;
    tracing::info!(listen_address = %config.listen_address, "public gateway ready");
    axum::serve(listener, build_router(repository, verifier))
        .with_graceful_shutdown(shutdown_signal(Duration::from_millis(
            config.shutdown_grace_milliseconds,
        )))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    fn oidc_config() -> InstalledOidcVerifierConfig {
        let keys = serde_json::json!({"keys": [{
            "kty": "RSA", "kid": "key-1", "use": "sig", "alg": "RS256",
            "n": "4KoeIFhx35ADyXYT0MpVCFDcWPKi1KUDxNTnPu1uubb9hqbnpgq68U8YQAGT1Dh1B4lyZmqUvYbGLNBj7CEcuJdms6JkohM50AdwBv6-TCy_uLpZzcUs8AGh8zFyVeyceX2CkZptlaP-362KPVB0tnvmjRVO2tJLiiqFBGqe9OKKGL-WevKFrUlSoaWTova7baKBBIMUx8GckC9NHvSj9oMbaaOTziTSOhonVnzHr1diFh5CbluUn3ef6KFcO8mssT-prqfqHYnNCEeLRsEUZT79oCVXb2H9RasBv7mU-FNPNwj8dcWcfUIV6ePEDjAGH-KU1eStSTYxeJEfbgW9zw",
            "e": "AQAB"
        }]});
        InstalledOidcVerifierConfig {
            issuer: "https://issuer.example".to_owned(),
            audience: "insight-platform-public".to_owned(),
            jwks_digest: canonical_digest(&keys).unwrap().parse().unwrap(),
            jwks: keys,
        }
    }

    #[test]
    fn process_config_rejects_unbounded_or_ambiguous_values() {
        let mut config = ProcessConfig {
            schema_version: 1,
            listen_address: "0.0.0.0:8080".to_owned(),
            database_max_connections: 8,
            database_acquire_timeout_milliseconds: 1_000,
            shutdown_grace_milliseconds: 5_000,
            oidc: oidc_config(),
        };
        assert!(config.validate().is_ok());
        config.database_max_connections = 1_000;
        assert!(matches!(
            config.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }

    #[tokio::test]
    async fn health_is_public_but_operation_routes_require_verified_authentication() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let router = build_router(
            Arc::new(PgRepository::new(pool)),
            oidc_config().install().unwrap(),
        );
        let live = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(live.status(), StatusCode::OK);
        assert_eq!(live.headers()[CACHE_CONTROL], "no-store");

        let operation = router
            .oneshot(
                Request::builder()
                    .uri("/v1/operations/job_0198f1cc-32e4-75e1-a9e8-d95ca0f80001")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(operation.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(operation.headers()[CACHE_CONTROL], "no-store");
    }
}
