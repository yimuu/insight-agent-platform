use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{
        header::{CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, ETAG},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use futures::Stream;
use insight_platform_api::artifact::{
    artifact_etag, view_from_record, ArtifactMutationAcceptedV1, CompleteArtifactUploadRequestV1,
    OpaqueUploadCompletionProof, PrepareArtifactUploadRequestV1, PrepareArtifactUploadResponseV1,
    SecretBearingUploadTargetV1,
};
use insight_platform_artifact_broker::{
    ArtifactBrokerLimits, ArtifactBrokerReadPermit, AwsArtifactProviderCatalog,
    AwsArtifactProviderCatalogConfig, AwsArtifactUploadProvider, AwsArtifactUploadRequest,
    BrokeredGatewayArtifactReader, GatewayArtifactReadError,
};
use insight_platform_artifacts::{
    ArtifactStore, ArtifactTransaction, CompleteArtifactUpload, GatewayArtifactReadRequest,
    MarkArtifactDeletion, PrepareArtifact, ScheduleInitialArtifactScan,
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, CommandAudit, CommandOutcome, JsonLimits, PrincipalKind,
    ResourceId, ResourceKind, Sha256Digest, UtcTimestamp,
};
use insight_platform_observability::{
    process_observability_router, ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS,
};
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
    RootCertStore, ServerConfig,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::{
    convert::Infallible,
    error::Error,
    fmt,
    io::Read as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tokio_util::sync::CancellationToken;
use x509_parser::{extensions::GeneralName, parse_x509_certificate};

#[path = "../capacity.rs"]
mod capacity;
use capacity::artifact_capacity_metric;

const CONFIG_PATH_ENV: &str = "PLATFORM_ARTIFACT_GATEWAY_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_ARTIFACT_GATEWAY_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_ARTIFACT_GATEWAY_DATABASE_URL";
const CLIENT_CA_PATH_ENV: &str = "PLATFORM_ARTIFACT_GATEWAY_CLIENT_CA_PATH";
const SERVER_CERT_PATH_ENV: &str = "PLATFORM_ARTIFACT_GATEWAY_CERT_PATH";
const SERVER_KEY_PATH_ENV: &str = "PLATFORM_ARTIFACT_GATEWAY_KEY_PATH";
const PUBLIC_GATEWAY_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/public-gateway";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_REQUEST_BYTES: usize = 262_144;
const MAX_GRANT_TOKEN_BYTES: usize = 1_024;
const MAX_TLS_FILE_BYTES: usize = 1_048_576;

struct ExactMtlsListener {
    tcp: tokio::net::TcpListener,
    acceptor: TlsAcceptor,
}

impl axum::serve::Listener for ExactMtlsListener {
    type Io = TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, address) = match self.tcp.accept().await {
                Ok(value) => value,
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            let tls = match self.acceptor.accept(stream).await {
                Ok(value) => value,
                Err(_) => continue,
            };
            let authorized = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certificates| certificates.first())
                .is_some_and(|certificate| {
                    has_exact_workload_identity(
                        certificate.as_ref(),
                        PUBLIC_GATEWAY_WORKLOAD_IDENTITY,
                    )
                });
            if authorized {
                return (tls, address);
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.tcp.local_addr()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayConfig {
    schema_version: u32,
    listen_address: String,
    observability_listen_address: String,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    artifact_provider_catalog: AwsArtifactProviderCatalogConfig,
    write_encryption_domain_id: ResourceId,
    scanner_contract_digest: Sha256Digest,
    scan_evidence_ttl_milliseconds: u64,
    scan_retry_backoff_milliseconds: u64,
    maximum_upload_target_seconds: u64,
    maximum_download_bytes: usize,
    maximum_download_in_flight: usize,
    download_timeout_milliseconds: u64,
    shutdown_grace_milliseconds: u64,
}

impl GatewayConfig {
    fn load() -> Result<Self, GatewayError> {
        let bytes = read_bounded(&absolute_path(CONFIG_PATH_ENV)?, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 32,
                max_items_per_array: 128,
                max_properties_per_object: 128,
                max_string_bytes: 16_384,
            },
        )
        .map_err(|_| GatewayError::InvalidConfiguration)?;
        let expected: Sha256Digest = required(CONFIG_DIGEST_ENV)?
            .parse()
            .map_err(|_| GatewayError::InvalidConfiguration)?;
        if canonical_digest(&value).ok().as_deref() != Some(expected.as_str()) {
            return Err(GatewayError::InvalidConfiguration);
        }
        let config: Self =
            serde_json::from_value(value).map_err(|_| GatewayError::InvalidConfiguration)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), GatewayError> {
        let listen: SocketAddr = self
            .listen_address
            .parse()
            .map_err(|_| GatewayError::InvalidConfiguration)?;
        let observability: SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| GatewayError::InvalidConfiguration)?;
        self.artifact_provider_catalog
            .validate()
            .map_err(|_| GatewayError::InvalidConfiguration)?;
        if self.schema_version != 1
            || listen.port() == 0
            || observability.port() == 0
            || observability == listen
            || !(2..=64).contains(&self.database_max_connections)
            || !(1..=30_000).contains(&self.database_acquire_timeout_milliseconds)
            || !(60..=3_600).contains(&self.maximum_upload_target_seconds)
            || self.write_encryption_domain_id.kind() != ResourceKind::EncryptionDomain
            || self.scan_evidence_ttl_milliseconds == 0
            || self.scan_evidence_ttl_milliseconds > 86_400_000
            || self.scan_retry_backoff_milliseconds == 0
            || self.scan_retry_backoff_milliseconds > 60_000
            || self.maximum_download_bytes == 0
            || self.maximum_download_bytes > 64 * 1024 * 1024
            || !(1..=4_096).contains(&self.maximum_download_in_flight)
            || !(1..=30_000).contains(&self.download_timeout_milliseconds)
            || !(1..=120_000).contains(&self.shutdown_grace_milliseconds)
        {
            return Err(GatewayError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct GatewayState {
    repository: Arc<PgRepository>,
    uploads: AwsArtifactUploadProvider,
    reader: Arc<BrokeredGatewayArtifactReader>,
    maximum_upload_target_seconds: u64,
    write_encryption_domain_id: ResourceId,
    scanner_contract_digest: Sha256Digest,
    scan_evidence_ttl_milliseconds: u64,
    scan_retry_backoff_milliseconds: u64,
    maximum_download_bytes: usize,
    download_timeout_milliseconds: u64,
}

#[derive(Clone)]
struct PrincipalHeaders {
    tenant_id: ResourceId,
    principal_id: ResourceId,
    principal_kind: PrincipalKind,
    idempotency_key_digest: Option<Sha256Digest>,
}

struct PermitResponseStream {
    bytes: Option<bytes::Bytes>,
    _permit: ArtifactBrokerReadPermit,
}

impl Stream for PermitResponseStream {
    type Item = Result<bytes::Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.bytes.take().map(Ok))
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-artifact-gateway failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), GatewayError> {
    let config = GatewayConfig::load()?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .connect(&required(DATABASE_URL_ENV)?)
        .await
        .map_err(|_| GatewayError::DatabaseUnavailable)?;
    verify_schema(&pool)
        .await
        .map_err(|_| GatewayError::SchemaMismatch)?;
    let providers = AwsArtifactProviderCatalog::install(config.artifact_provider_catalog)
        .await
        .map_err(|_| GatewayError::InvalidConfiguration)?;
    providers
        .check_readiness()
        .await
        .map_err(|_| GatewayError::ProviderUnavailable)?;
    let repository = Arc::new(PgRepository::new(pool));
    let (uploads, unsealer, stores) = providers.into_gateway_components();
    let reader = Arc::new(
        BrokeredGatewayArtifactReader::new(
            repository.clone(),
            unsealer,
            stores,
            ArtifactBrokerLimits {
                maximum_in_flight: config.maximum_download_in_flight,
                maximum_read_bytes: config.maximum_download_bytes,
                operation_timeout: Duration::from_millis(config.download_timeout_milliseconds),
            },
        )
        .map_err(|_| GatewayError::InvalidConfiguration)?,
    );
    let state = GatewayState {
        repository,
        uploads,
        reader: Arc::clone(&reader),
        maximum_upload_target_seconds: config.maximum_upload_target_seconds,
        write_encryption_domain_id: config.write_encryption_domain_id,
        scanner_contract_digest: config.scanner_contract_digest,
        scan_evidence_ttl_milliseconds: config.scan_evidence_ttl_milliseconds,
        scan_retry_backoff_milliseconds: config.scan_retry_backoff_milliseconds,
        maximum_download_bytes: config.maximum_download_bytes,
        download_timeout_milliseconds: config.download_timeout_milliseconds,
    };
    let app = Router::new()
        .route("/v1/artifacts:prepare-upload", post(prepare_upload))
        .route(
            "/v1/artifacts/{artifact_action}",
            get(get_artifact).post(mutate_artifact),
        )
        .route(
            "/v1/artifacts/{artifact_id}/content",
            get(get_artifact_content),
        )
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(state);
    let address: SocketAddr = config
        .listen_address
        .parse()
        .map_err(|_| GatewayError::InvalidConfiguration)?;
    let listener = install_mtls_listener(address).await?;
    let metrics = Arc::new(
        ProcessHttpMetrics::install_with_capacities(
            "artifact-gateway",
            PROCESS_OBSERVABILITY_OPERATIONS,
            vec![artifact_capacity_metric(
                "download",
                reader,
                BrokeredGatewayArtifactReader::capacity_snapshot,
            )],
        )
        .map_err(|_| GatewayError::InvalidConfiguration)?,
    );
    let observability_listener =
        tokio::net::TcpListener::bind(&config.observability_listen_address)
            .await
            .map_err(|_| GatewayError::ObservabilityUnavailable)?;
    let cancellation = CancellationToken::new();
    let http_cancellation = cancellation.child_token();
    let mut http = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(http_cancellation.cancelled_owned())
            .await
    });
    let observability_cancellation = cancellation.child_token();
    let router = process_observability_router(Arc::clone(&metrics));
    let mut observability = tokio::spawn(async move {
        axum::serve(observability_listener, router)
            .with_graceful_shutdown(observability_cancellation.cancelled_owned())
            .await
    });
    metrics.mark_ready();
    tokio::select! {
        _ = shutdown_signal() => cancellation.cancel(),
        result = &mut http => {
            cancellation.cancel();
            result.map_err(|_| GatewayError::HttpUnavailable)?
                .map_err(|_| GatewayError::HttpUnavailable)?;
            observability.await.map_err(|_| GatewayError::ObservabilityUnavailable)?
                .map_err(|_| GatewayError::ObservabilityUnavailable)?;
            return Err(GatewayError::HttpUnavailable);
        }
        result = &mut observability => {
            cancellation.cancel();
            result.map_err(|_| GatewayError::ObservabilityUnavailable)?
                .map_err(|_| GatewayError::ObservabilityUnavailable)?;
            http.await.map_err(|_| GatewayError::HttpUnavailable)?
                .map_err(|_| GatewayError::HttpUnavailable)?;
            return Err(GatewayError::ObservabilityUnavailable);
        }
    }
    tokio::time::timeout(
        Duration::from_millis(config.shutdown_grace_milliseconds),
        async {
            http.await
                .map_err(|_| GatewayError::HttpUnavailable)?
                .map_err(|_| GatewayError::HttpUnavailable)?;
            observability
                .await
                .map_err(|_| GatewayError::ObservabilityUnavailable)?
                .map_err(|_| GatewayError::ObservabilityUnavailable)
        },
    )
    .await
    .map_err(|_| GatewayError::ShutdownDeadlineExceeded)?
}

async fn install_mtls_listener(address: SocketAddr) -> Result<ExactMtlsListener, GatewayError> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let ca = read_bounded(&absolute_path(CLIENT_CA_PATH_ENV)?, MAX_TLS_FILE_BYTES)?;
    let certificate = read_bounded(&absolute_path(SERVER_CERT_PATH_ENV)?, MAX_TLS_FILE_BYTES)?;
    let key = read_bounded(&absolute_path(SERVER_KEY_PATH_ENV)?, MAX_TLS_FILE_BYTES)?;
    let mut roots = RootCertStore::empty();
    let mut ca_count = 0_usize;
    for certificate in CertificateDer::pem_slice_iter(&ca) {
        roots
            .add(certificate.map_err(|_| GatewayError::InvalidConfiguration)?)
            .map_err(|_| GatewayError::InvalidConfiguration)?;
        ca_count += 1;
    }
    if ca_count == 0 {
        return Err(GatewayError::InvalidConfiguration);
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|_| GatewayError::InvalidConfiguration)?;
    let certificate_chain = CertificateDer::pem_slice_iter(&certificate)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| GatewayError::InvalidConfiguration)?;
    if certificate_chain.is_empty() {
        return Err(GatewayError::InvalidConfiguration);
    }
    let private_key =
        PrivateKeyDer::from_pem_slice(&key).map_err(|_| GatewayError::InvalidConfiguration)?;
    let tls = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificate_chain, private_key)
        .map_err(|_| GatewayError::InvalidConfiguration)?;
    let tcp = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|_| GatewayError::HttpUnavailable)?;
    Ok(ExactMtlsListener {
        tcp,
        acceptor: TlsAcceptor::from(Arc::new(tls)),
    })
}

fn has_exact_workload_identity(certificate: &[u8], expected: &str) -> bool {
    let Ok((remainder, certificate)) = parse_x509_certificate(certificate) else {
        return false;
    };
    if !remainder.is_empty() {
        return false;
    }
    let Ok(Some(alternative_names)) = certificate.subject_alternative_name() else {
        return false;
    };
    let mut uris = alternative_names
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(*uri),
            _ => None,
        });
    uris.next() == Some(expected) && uris.next().is_none()
}

async fn prepare_upload(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<PrepareArtifactUploadRequestV1>,
) -> Response {
    match prepare_upload_inner(state, &headers, request).await {
        Ok(response) => no_store((StatusCode::CREATED, Json(response)).into_response()),
        Err(error) => problem(error),
    }
}

async fn prepare_upload_inner(
    state: GatewayState,
    headers: &HeaderMap,
    request: PrepareArtifactUploadRequestV1,
) -> Result<PrepareArtifactUploadResponseV1, HttpError> {
    request.validate().map_err(|_| HttpError::Invalid)?;
    let principal = mutation_principal(headers)?;
    let request_digest = request_digest("artifact.prepare_upload", &request)?;
    let now = Utc::now();
    let authority = state
        .repository
        .resolve_public_artifact_prepare_authority(
            principal.tenant_id.clone(),
            principal.principal_id.clone(),
            principal.principal_kind,
        )
        .await
        .map_err(map_repository_error)?;
    if request
        .declared_media_type
        .as_ref()
        .is_some_and(|media_type| {
            !authority
                .artifact_io_policy
                .allowed_input_media_types
                .iter()
                .any(|allowed| allowed == media_type)
        })
    {
        return Err(HttpError::Forbidden);
    }
    let target_seconds = state.maximum_upload_target_seconds;
    let target_duration =
        chrono::Duration::seconds(i64::try_from(target_seconds).map_err(|_| HttpError::Invalid)?);
    let operation_deadline = now + target_duration;
    let retention_seconds = authority
        .retention_policy
        .minimum_retention_seconds
        .max(target_seconds);
    let retain_until = now
        + chrono::Duration::seconds(
            i64::try_from(retention_seconds).map_err(|_| HttpError::Invalid)?,
        );
    let operation_id = server_id(ResourceKind::Job);
    let artifact_id = server_id(ResourceKind::Artifact);
    let blob_id = server_id(ResourceKind::InternalBlob);
    let upload_grant_id = server_id(ResourceKind::ArtifactGrant);
    let quota_entry_id = server_id(ResourceKind::QuotaLedgerEntry);
    let candidate_proof = completion_proof(&principal, &artifact_id, &upload_grant_id)?;
    let upload = state
        .uploads
        .prepare_upload(AwsArtifactUploadRequest {
            tenant_id: &principal.tenant_id,
            artifact_id: &artifact_id,
            blob_id: &blob_id,
            encryption_domain_id: &state.write_encryption_domain_id,
            expected_size_bytes: request.expected_size_bytes,
            declared_media_type: request.declared_media_type.as_deref(),
            expires_in: Duration::from_secs(target_seconds),
        })
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let token_digest = token_digest(candidate_proof.as_str())?;
    let command = PrepareArtifact {
        audit: audit(
            principal.clone(),
            server_id(ResourceKind::Receipt),
            server_id(ResourceKind::Event),
            server_id(ResourceKind::OutboxEvent),
            request_digest,
            operation_deadline,
        ),
        operation_id,
        artifact_id: artifact_id.clone(),
        blob_id: blob_id.clone(),
        upload_grant_id: upload_grant_id.clone(),
        quota_account_id: authority.quota_account_id,
        quota_entry_id,
        purpose: request.purpose,
        classification: request.classification,
        expected_size_bytes: request.expected_size_bytes,
        expected_digest: request.expected_digest,
        declared_media_type: request.declared_media_type,
        retention_policy_revision_id: authority.retention_policy_revision.revision_id,
        scan_policy_revision: authority.artifact_io_policy_revision,
        scanner_contract_digest: state.scanner_contract_digest.clone(),
        ruleset_digest: authority.artifact_io_rules_digest,
        evidence_ttl_milliseconds: state.scan_evidence_ttl_milliseconds,
        retry_backoff_milliseconds: state.scan_retry_backoff_milliseconds,
        retain_until,
        operation_deadline,
        grant_expires_at: operation_deadline,
        grant_token_digest: token_digest,
        storage_backend: upload.storage_backend.clone(),
        storage_binding_digest: upload.storage_binding_digest.clone(),
        object_reference_ciphertext: upload.object_reference_ciphertext.clone(),
        key_id: upload.key_id.clone(),
        encryption_domain_id: state.write_encryption_domain_id.clone(),
        display_name: request.display_name,
    };
    let mut transaction = state
        .repository
        .begin()
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let outcome = transaction
        .prepare_artifact(command)
        .await
        .map_err(map_repository_error)?;
    transaction
        .commit()
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let prepared = outcome_value(outcome);
    let upload = if prepared.artifact.artifact_id == artifact_id && prepared.blob.blob_id == blob_id
    {
        upload
    } else {
        state
            .uploads
            .prepare_upload(AwsArtifactUploadRequest {
                tenant_id: &principal.tenant_id,
                artifact_id: &prepared.artifact.artifact_id,
                blob_id: &prepared.blob.blob_id,
                encryption_domain_id: &state.write_encryption_domain_id,
                expected_size_bytes: prepared.artifact.expected_size_bytes,
                declared_media_type: prepared.artifact.declared_media_type.as_deref(),
                expires_in: Duration::from_secs(target_seconds),
            })
            .await
            .map_err(|_| HttpError::Unavailable)?
    };
    let completion_proof = completion_proof(
        &principal,
        &prepared.artifact.artifact_id,
        &prepared.grant.upload_grant_id,
    )?;
    let artifact_etag = artifact_etag(&prepared.artifact.artifact_id, prepared.artifact.version);
    let response = PrepareArtifactUploadResponseV1 {
        schema_version: 1,
        artifact_id: prepared.artifact.artifact_id,
        operation_id: prepared.operation.operation_id,
        upload_grant_id: prepared.grant.upload_grant_id,
        artifact_etag,
        upload_target: SecretBearingUploadTargetV1 {
            url: upload.upload_url,
            completion_proof,
        },
        upload_expires_at: UtcTimestamp::from_datetime(operation_deadline),
    };
    response.validate().map_err(|_| HttpError::Unavailable)?;
    Ok(response)
}

async fn mutate_artifact(
    State(state): State<GatewayState>,
    AxumPath(artifact_action): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(artifact_id) = artifact_action.strip_suffix(":complete-upload") {
        let Ok(artifact_id) = artifact_id.parse::<ResourceId>() else {
            return problem(HttpError::NotFound);
        };
        let Ok(request) = serde_json::from_slice::<CompleteArtifactUploadRequestV1>(&body) else {
            return problem(HttpError::Invalid);
        };
        return match complete_upload_inner(state, &headers, artifact_id, request).await {
            Ok(response) => no_store((StatusCode::ACCEPTED, Json(response)).into_response()),
            Err(error) => problem(error),
        };
    }
    if let Some(artifact_id) = artifact_action.strip_suffix(":delete") {
        let Ok(artifact_id) = artifact_id.parse::<ResourceId>() else {
            return problem(HttpError::NotFound);
        };
        if !body.is_empty() {
            return problem(HttpError::Invalid);
        }
        return match delete_artifact_inner(state, &headers, artifact_id).await {
            Ok(response) => no_store((StatusCode::ACCEPTED, Json(response)).into_response()),
            Err(error) => problem(error),
        };
    }
    problem(HttpError::NotFound)
}

async fn complete_upload_inner(
    state: GatewayState,
    headers: &HeaderMap,
    artifact_id: ResourceId,
    request: CompleteArtifactUploadRequestV1,
) -> Result<ArtifactMutationAcceptedV1, HttpError> {
    request.validate().map_err(|_| HttpError::Invalid)?;
    let principal = mutation_principal(headers)?;
    let expected_artifact_version = headers
        .get("x-insight-artifact-expected-version")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or(HttpError::Invalid)?;
    let complete_digest = request_digest("artifact.complete_upload", &request)?;
    let scan_digest = request_digest("artifact.schedule_scan", &request)?;
    let target = state
        .repository
        .load_gateway_artifact_upload_target(
            principal.tenant_id.clone(),
            principal.principal_id.clone(),
            principal.principal_kind,
            artifact_id,
        )
        .await
        .map_err(map_repository_error)?;
    if target.artifact.version != expected_artifact_version {
        return Err(HttpError::Conflict);
    }
    let grant_token_digest = token_digest(request.completion_proof.as_str())?;
    let observed = state
        .uploads
        .complete_current_upload(
            &principal.tenant_id,
            &target.artifact.artifact_id,
            &target.blob.blob_id,
            target.artifact.expected_size_bytes,
        )
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let mut transaction = state
        .repository
        .begin()
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let completed = transaction
        .complete_upload(CompleteArtifactUpload {
            audit: audit(
                principal.clone(),
                server_id(ResourceKind::Receipt),
                server_id(ResourceKind::Event),
                server_id(ResourceKind::OutboxEvent),
                complete_digest,
                target.operation.deadline,
            ),
            operation_id: target.operation.operation_id.clone(),
            artifact_id: target.artifact.artifact_id.clone(),
            blob_id: target.blob.blob_id.clone(),
            upload_grant_id: target.grant.upload_grant_id.clone(),
            expected_artifact_version: target.artifact.version,
            expected_blob_version: target.blob.version,
            expected_operation_version: target.operation.version,
            expected_grant_version: target.grant.version,
            grant_generation: target.grant.snapshot.generation,
            grant_token_digest,
            object_generation: observed.object_generation,
            observed_size_bytes: observed.observed_size_bytes,
            backend_evidence_digest: observed.backend_evidence_digest,
        })
        .await
        .map_err(map_repository_error)?;
    let completed = outcome_value(completed);
    let scan = transaction
        .schedule_initial_scan(ScheduleInitialArtifactScan {
            audit: audit(
                principal,
                server_id(ResourceKind::Receipt),
                server_id(ResourceKind::Event),
                server_id(ResourceKind::OutboxEvent),
                scan_digest,
                target.operation.deadline,
            ),
            scan_job_id: target.operation.operation_id.clone(),
            operation_id: target.operation.operation_id,
            artifact_id: target.artifact.artifact_id,
            blob_id: target.blob.blob_id,
            expected_artifact_version: completed.artifact.version,
            expected_blob_version: completed.blob.version,
            expected_operation_version: completed.operation.version,
            scan_policy_revision: target.operation.snapshot.scan_policy_revision,
            scanner_contract_digest: target.operation.snapshot.scanner_contract_digest,
            ruleset_digest: target.operation.snapshot.ruleset_digest,
            evidence_ttl_milliseconds: target.operation.snapshot.evidence_ttl_milliseconds,
            retry_backoff_milliseconds: target.operation.snapshot.retry_backoff_milliseconds,
            deadline: target.operation.deadline,
        })
        .await
        .map_err(map_repository_error)?;
    transaction
        .commit()
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let scan = outcome_value(scan);
    let artifact_etag = artifact_etag(&scan.artifact.artifact_id, scan.artifact.version);
    let response = ArtifactMutationAcceptedV1 {
        schema_version: 1,
        artifact_id: scan.artifact.artifact_id,
        artifact_etag,
        operation_id: scan.operation.operation_id,
    };
    response.validate().map_err(|_| HttpError::Unavailable)?;
    Ok(response)
}

async fn get_artifact(
    State(state): State<GatewayState>,
    AxumPath(artifact_id): AxumPath<ResourceId>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        let principal = read_principal(&headers)?;
        let snapshot = state
            .repository
            .load_gateway_artifact(
                principal.tenant_id,
                principal.principal_id,
                principal.principal_kind,
                artifact_id,
            )
            .await
            .map_err(map_repository_error)?;
        let response = view_from_record(snapshot.artifact, snapshot.content);
        response.validate().map_err(|_| HttpError::Unavailable)?;
        Ok::<_, HttpError>(response)
    }
    .await;
    match result {
        Ok(response) => {
            let etag = response.etag.clone();
            let mut response = no_store((StatusCode::OK, Json(response)).into_response());
            if let Ok(value) = HeaderValue::from_str(&etag) {
                response.headers_mut().insert(ETAG, value);
            }
            response
        }
        Err(error) => problem(error),
    }
}

async fn get_artifact_content(
    State(state): State<GatewayState>,
    AxumPath(artifact_id): AxumPath<ResourceId>,
    headers: HeaderMap,
) -> Response {
    match get_artifact_content_inner(state, &headers, artifact_id).await {
        Ok(response) => response,
        Err(error) => problem(error),
    }
}

async fn get_artifact_content_inner(
    state: GatewayState,
    headers: &HeaderMap,
    artifact_id: ResourceId,
) -> Result<Response, HttpError> {
    let principal = read_principal(headers)?;
    let snapshot = state
        .repository
        .load_gateway_artifact(
            principal.tenant_id.clone(),
            principal.principal_id.clone(),
            principal.principal_kind,
            artifact_id,
        )
        .await
        .map_err(map_repository_error)?;
    let artifact = snapshot.content.ok_or(HttpError::Conflict)?;
    let request = GatewayArtifactReadRequest {
        tenant_id: principal.tenant_id,
        principal_id: principal.principal_id,
        principal_kind: principal.principal_kind,
        request_digest: request_digest("artifact.read_content", &artifact)?,
        maximum_bytes: state.maximum_download_bytes,
        deadline: Utc::now()
            + chrono::Duration::milliseconds(
                i64::try_from(state.download_timeout_milliseconds)
                    .map_err(|_| HttpError::Invalid)?,
            ),
        artifact: artifact.clone(),
    };
    let read = state
        .reader
        .read(&request)
        .await
        .map_err(|error| match error {
            GatewayArtifactReadError::Unavailable => HttpError::Unavailable,
            GatewayArtifactReadError::Denied => HttpError::Forbidden,
            GatewayArtifactReadError::NotFound => HttpError::NotFound,
            GatewayArtifactReadError::TooLarge => HttpError::TooLarge,
            GatewayArtifactReadError::Integrity => HttpError::Conflict,
        })?;
    let (bytes, permit) = read.into_response_parts();
    let body = Body::from_stream(PermitResponseStream {
        bytes: Some(bytes::Bytes::from(bytes)),
        _permit: permit,
    });
    let mut response = no_store((StatusCode::OK, body).into_response());
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_str(artifact.media_type()).map_err(|_| HttpError::Conflict)?,
    );
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&artifact.byte_length().to_string())
            .map_err(|_| HttpError::Conflict)?,
    );
    response
        .headers_mut()
        .insert(CONTENT_DISPOSITION, HeaderValue::from_static("attachment"));
    response.headers_mut().insert(
        ETAG,
        HeaderValue::from_str(&format!("\"{}\"", artifact.content_digest().as_str()))
            .map_err(|_| HttpError::Conflict)?,
    );
    Ok(response)
}

async fn delete_artifact_inner(
    state: GatewayState,
    headers: &HeaderMap,
    artifact_id: ResourceId,
) -> Result<ArtifactMutationAcceptedV1, HttpError> {
    let principal = mutation_principal(headers)?;
    let expected_artifact_version = headers
        .get("x-insight-artifact-expected-version")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or(HttpError::Invalid)?;
    let request_digest = request_digest(
        "artifact.delete",
        &serde_json::json!({
            "artifact_id": artifact_id,
            "expected_artifact_version": expected_artifact_version,
            "schema_version": 1,
        }),
    )?;
    let target = state
        .repository
        .load_gateway_artifact_deletion_target(
            principal.tenant_id.clone(),
            principal.principal_id.clone(),
            principal.principal_kind,
            artifact_id.clone(),
        )
        .await
        .map_err(map_repository_error)?;
    if target.artifact.version != expected_artifact_version {
        return Err(HttpError::Conflict);
    }
    let now = Utc::now();
    let deadline = now + chrono::Duration::hours(1);
    let operation_id = server_id(ResourceKind::Job);
    let approval_task_id = target
        .retention_policy
        .delete_requires_approval
        .then(|| server_id(ResourceKind::ApprovalTask));
    let mut transaction = state
        .repository
        .begin()
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let marked = transaction
        .mark_deletion(MarkArtifactDeletion {
            audit: audit(
                principal,
                server_id(ResourceKind::Receipt),
                server_id(ResourceKind::Event),
                server_id(ResourceKind::OutboxEvent),
                request_digest,
                deadline,
            ),
            deletion_operation_id: operation_id.clone(),
            deletion_job_id: operation_id.clone(),
            artifact_id: artifact_id.clone(),
            blob_id: target.blob.blob_id,
            expected_artifact_version,
            expected_blob_version: target.blob.version,
            approval_task_id,
            retry_backoff_milliseconds: 1_000,
            deadline,
        })
        .await
        .map_err(map_repository_error)?;
    transaction
        .commit()
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let marked = outcome_value(marked);
    let response = ArtifactMutationAcceptedV1 {
        schema_version: 1,
        artifact_id: marked.artifact.artifact_id,
        artifact_etag: artifact_etag(&artifact_id, marked.artifact.version),
        operation_id: marked.deletion.operation_id,
    };
    response.validate().map_err(|_| HttpError::Unavailable)?;
    Ok(response)
}

fn read_principal(headers: &HeaderMap) -> Result<PrincipalHeaders, HttpError> {
    let value = |name: &'static str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty() && value.len() <= 512)
            .ok_or(HttpError::Unauthenticated)
    };
    Ok(PrincipalHeaders {
        tenant_id: value("x-insight-verified-tenant-id")?
            .parse()
            .map_err(|_| HttpError::Unauthenticated)?,
        principal_id: value("x-insight-verified-principal-id")?
            .parse()
            .map_err(|_| HttpError::Unauthenticated)?,
        principal_kind: PrincipalKind::from_str(value("x-insight-verified-principal-kind")?)
            .map_err(|_| HttpError::Unauthenticated)?,
        idempotency_key_digest: None,
    })
}

fn mutation_principal(headers: &HeaderMap) -> Result<PrincipalHeaders, HttpError> {
    let mut principal = read_principal(headers)?;
    let idempotency_key_digest = headers
        .get("x-insight-idempotency-key-digest")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<Sha256Digest>().ok())
        .ok_or(HttpError::Invalid)?;
    principal.idempotency_key_digest = Some(idempotency_key_digest);
    Ok(principal)
}

fn server_id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).expect("server-generated ID kind")
}

fn completion_proof(
    principal: &PrincipalHeaders,
    artifact_id: &ResourceId,
    upload_grant_id: &ResourceId,
) -> Result<OpaqueUploadCompletionProof, HttpError> {
    let idempotency_key_digest = principal
        .idempotency_key_digest
        .as_ref()
        .ok_or(HttpError::Invalid)?;
    let message = format!(
        "insight.platform/artifact-upload-completion/v1\0{}\0{}\0{}\0{}",
        principal.tenant_id, principal.principal_id, artifact_id, upload_grant_id
    );
    let key = ring::hmac::Key::new(
        ring::hmac::HMAC_SHA256,
        idempotency_key_digest.as_str().as_bytes(),
    );
    let tag = ring::hmac::sign(&key, message.as_bytes());
    OpaqueUploadCompletionProof::new(format!("v1.{}", lower_hex(tag.as_ref())))
        .map_err(|_| HttpError::Invalid)
}

fn audit(
    principal: PrincipalHeaders,
    receipt_id: ResourceId,
    event_id: ResourceId,
    outbox_id: ResourceId,
    request_digest: Sha256Digest,
    deadline: DateTime<Utc>,
) -> CommandAudit {
    CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: principal.tenant_id,
        principal_id: principal.principal_id,
        principal_kind: principal.principal_kind,
        receipt_id,
        event_id,
        outbox_id,
        idempotency_key_digest: principal
            .idempotency_key_digest
            .expect("mutation principal has an idempotency digest"),
        request_digest,
        receipt_expires_at: deadline,
    }
}

fn request_digest<T: Serialize>(operation: &str, request: &T) -> Result<Sha256Digest, HttpError> {
    canonical_digest(&serde_json::json!({
        "operation": operation,
        "request": request,
        "schema_version": 1,
    }))
    .map_err(|_| HttpError::Invalid)?
    .parse()
    .map_err(|_| HttpError::Invalid)
}

fn token_digest(value: &str) -> Result<Sha256Digest, HttpError> {
    if value.is_empty()
        || value.len() > MAX_GRANT_TOKEN_BYTES
        || !value.is_ascii()
        || value.chars().any(char::is_control)
    {
        return Err(HttpError::Invalid);
    }
    let digest = Sha256::digest(value.as_bytes());
    format!("sha256:{}", lower_hex(&digest))
        .parse()
        .map_err(|_| HttpError::Invalid)
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn outcome_value<T>(outcome: CommandOutcome<T>) -> T {
    match outcome {
        CommandOutcome::Applied(value) | CommandOutcome::Replayed(value) => value,
    }
}

fn map_repository_error(
    error: insight_platform_postgres::repository::RepositoryError,
) -> HttpError {
    use insight_platform_postgres::repository::RepositoryError;
    match error {
        RepositoryError::PermissionDenied => HttpError::Forbidden,
        RepositoryError::NotFound(_) => HttpError::NotFound,
        RepositoryError::Conflict(_) => HttpError::Conflict,
        RepositoryError::IdempotencyConflict => HttpError::IdempotencyConflict,
        RepositoryError::QuotaExceeded => HttpError::TooLarge,
        RepositoryError::InvalidInput(_) => HttpError::Invalid,
        _ => HttpError::Unavailable,
    }
}

#[derive(Debug, Clone, Copy)]
enum HttpError {
    Invalid,
    Unauthenticated,
    Forbidden,
    NotFound,
    Conflict,
    IdempotencyConflict,
    TooLarge,
    Unavailable,
}

fn problem(error: HttpError) -> Response {
    let (status, code) = match error {
        HttpError::Invalid => (StatusCode::BAD_REQUEST, "invalid_request"),
        HttpError::Unauthenticated => (StatusCode::UNAUTHORIZED, "authentication_required"),
        HttpError::Forbidden => (StatusCode::FORBIDDEN, "permission_denied"),
        HttpError::NotFound => (StatusCode::NOT_FOUND, "artifact_not_found"),
        HttpError::Conflict => (StatusCode::PRECONDITION_FAILED, "artifact_conflict"),
        HttpError::IdempotencyConflict => (StatusCode::CONFLICT, "idempotency_conflict"),
        HttpError::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "artifact_limit_exceeded"),
        HttpError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "artifact_unavailable"),
    };
    no_store(
        (
            status,
            Json(serde_json::json!({
                "type": format!("https://insight.platform/problems/{code}"),
                "title": code,
                "status": status.as_u16(),
            })),
        )
            .into_response(),
    )
}

fn no_store(mut response: Response<Body>) -> Response<Body> {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = async { if let Some(signal) = &mut terminate { signal.recv().await; } } => {},
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

fn required(name: &'static str) -> Result<String, GatewayError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= 16_384)
        .ok_or(GatewayError::MissingConfiguration(name))
}

fn absolute_path(name: &'static str) -> Result<PathBuf, GatewayError> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() || path == Path::new("/") {
        return Err(GatewayError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, GatewayError> {
    let file = std::fs::File::open(path).map_err(|_| GatewayError::FileUnavailable)?;
    let metadata = file.metadata().map_err(|_| GatewayError::FileUnavailable)?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX)
    {
        return Err(GatewayError::FileUnavailable);
    }
    let mut bytes = Vec::new();
    file.take(u64::try_from(maximum).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GatewayError::FileUnavailable)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(GatewayError::FileUnavailable);
    }
    Ok(bytes)
}

#[derive(Debug)]
enum GatewayError {
    MissingConfiguration(&'static str),
    InvalidConfiguration,
    FileUnavailable,
    DatabaseUnavailable,
    SchemaMismatch,
    ProviderUnavailable,
    HttpUnavailable,
    ObservabilityUnavailable,
    ShutdownDeadlineExceeded,
}

impl fmt::Display for GatewayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingConfiguration(name) => write!(formatter, "missing configuration {name}"),
            Self::InvalidConfiguration => formatter.write_str("invalid configuration"),
            Self::FileUnavailable => formatter.write_str("configuration file unavailable"),
            Self::DatabaseUnavailable => formatter.write_str("database unavailable"),
            Self::SchemaMismatch => formatter.write_str("database schema mismatch"),
            Self::ProviderUnavailable => formatter.write_str("Artifact provider unavailable"),
            Self::HttpUnavailable => formatter.write_str("Artifact Gateway HTTP unavailable"),
            Self::ObservabilityUnavailable => {
                formatter.write_str("Artifact Gateway observability unavailable")
            }
            Self::ShutdownDeadlineExceeded => {
                formatter.write_str("Artifact Gateway shutdown deadline exceeded")
            }
        }
    }
}

impl Error for GatewayError {}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, KeyPair, SanType};

    fn certificate_with_uris(uris: &[&str]) -> Vec<u8> {
        let mut parameters = CertificateParams::default();
        parameters.subject_alt_names = uris
            .iter()
            .map(|uri| SanType::URI((*uri).try_into().unwrap()))
            .collect();
        let key = KeyPair::generate().unwrap();
        parameters.self_signed(&key).unwrap().der().to_vec()
    }

    #[test]
    fn public_gateway_workload_identity_must_be_the_only_uri_san() {
        let exact = certificate_with_uris(&[PUBLIC_GATEWAY_WORKLOAD_IDENTITY]);
        assert!(has_exact_workload_identity(
            &exact,
            PUBLIC_GATEWAY_WORKLOAD_IDENTITY
        ));

        let wrong = certificate_with_uris(&["spiffe://insight.platform/workload/model-worker"]);
        assert!(!has_exact_workload_identity(
            &wrong,
            PUBLIC_GATEWAY_WORKLOAD_IDENTITY
        ));

        let ambiguous = certificate_with_uris(&[
            PUBLIC_GATEWAY_WORKLOAD_IDENTITY,
            "spiffe://insight.platform/workload/also-public-gateway",
        ]);
        assert!(!has_exact_workload_identity(
            &ambiguous,
            PUBLIC_GATEWAY_WORKLOAD_IDENTITY
        ));
    }

    #[test]
    fn forwarded_principal_assertion_is_closed_and_digest_only() {
        let tenant = ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7()).unwrap();
        let principal =
            ResourceId::from_uuid_v7(ResourceKind::Principal, uuid::Uuid::now_v7()).unwrap();
        let digest: Sha256Digest = format!("sha256:{}", "a".repeat(64)).parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-insight-verified-tenant-id",
            HeaderValue::from_str(&tenant.to_string()).unwrap(),
        );
        headers.insert(
            "x-insight-verified-principal-id",
            HeaderValue::from_str(&principal.to_string()).unwrap(),
        );
        headers.insert(
            "x-insight-verified-principal-kind",
            HeaderValue::from_static("agent_runner"),
        );
        headers.insert(
            "x-insight-idempotency-key-digest",
            HeaderValue::from_str(digest.as_str()).unwrap(),
        );
        let parsed = mutation_principal(&headers).unwrap();
        assert_eq!(parsed.tenant_id, tenant);
        assert_eq!(parsed.principal_id, principal);
        assert_eq!(parsed.idempotency_key_digest, Some(digest));

        let mut legacy = HeaderMap::new();
        legacy.insert(
            "x-platform-tenant-id",
            HeaderValue::from_str(&tenant.to_string()).unwrap(),
        );
        legacy.insert(
            "x-platform-principal-id",
            HeaderValue::from_str(&principal.to_string()).unwrap(),
        );
        legacy.insert(
            "x-platform-principal-kind",
            HeaderValue::from_static("agent_runner"),
        );
        legacy.insert("idempotency-key", HeaderValue::from_static("raw-secret"));
        assert!(matches!(
            mutation_principal(&legacy),
            Err(HttpError::Unauthenticated)
        ));
    }
}
