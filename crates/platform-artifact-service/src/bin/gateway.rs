use axum::{
    body::Body,
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
use insight_platform_artifact_broker::{
    ArtifactBrokerLimits, ArtifactBrokerReadPermit, AwsArtifactProviderCatalog,
    AwsArtifactProviderCatalogConfig, AwsArtifactUploadProvider, BrokeredGatewayArtifactReader,
    GatewayArtifactReadError,
};
use insight_platform_artifacts::{
    ArtifactStore, ArtifactTransaction, CompleteArtifactUpload, GatewayArtifactReadRequest,
    MarkArtifactDeletion, PrepareArtifact, ScheduleInitialArtifactScan,
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ArtifactPurpose, CommandAudit, CommandOutcome,
    DataClassification, ExactVersionRef, JsonLimits, PrincipalKind, ResourceId, Sha256Digest,
};
use insight_platform_postgres::{
    artifact_repository::GatewayArtifactSnapshot, repository::PgRepository, verify_schema,
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

const CONFIG_PATH_ENV: &str = "PLATFORM_ARTIFACT_GATEWAY_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_ARTIFACT_GATEWAY_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_ARTIFACT_GATEWAY_DATABASE_URL";
const MAX_CONFIG_BYTES: usize = 1_048_576;
const MAX_REQUEST_BYTES: usize = 262_144;
const MAX_GRANT_TOKEN_BYTES: usize = 1_024;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayConfig {
    schema_version: u32,
    listen_address: String,
    database_max_connections: u32,
    database_acquire_timeout_milliseconds: u64,
    artifact_provider_catalog: AwsArtifactProviderCatalogConfig,
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
        let _: SocketAddr = self
            .listen_address
            .parse()
            .map_err(|_| GatewayError::InvalidConfiguration)?;
        self.artifact_provider_catalog
            .validate()
            .map_err(|_| GatewayError::InvalidConfiguration)?;
        if self.schema_version != 1
            || !(2..=64).contains(&self.database_max_connections)
            || !(1..=30_000).contains(&self.database_acquire_timeout_milliseconds)
            || !(60..=3_600).contains(&self.maximum_upload_target_seconds)
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
    maximum_download_bytes: usize,
    download_timeout_milliseconds: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PrepareUploadRequest {
    operation_id: ResourceId,
    artifact_id: ResourceId,
    blob_id: ResourceId,
    upload_grant_id: ResourceId,
    quota_account_id: ResourceId,
    quota_entry_id: ResourceId,
    receipt_id: ResourceId,
    event_id: ResourceId,
    outbox_id: ResourceId,
    purpose: ArtifactPurpose,
    classification: DataClassification,
    expected_size_bytes: u64,
    expected_digest: Option<Sha256Digest>,
    declared_media_type: Option<String>,
    retention_policy_revision_id: ResourceId,
    encryption_domain_id: ResourceId,
    retain_until: DateTime<Utc>,
    operation_deadline: DateTime<Utc>,
    grant_expires_at: DateTime<Utc>,
    upload_grant_token: String,
    display_name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct PrepareUploadResponse {
    schema_version: u32,
    artifact_id: ResourceId,
    operation_id: ResourceId,
    upload_grant_id: ResourceId,
    artifact_version: u64,
    blob_version: u64,
    operation_version: u64,
    operation_state: String,
    grant_version: u64,
    upload_url: String,
    upload_expires_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CompleteUploadRequest {
    operation_id: ResourceId,
    artifact_id: ResourceId,
    blob_id: ResourceId,
    upload_grant_id: ResourceId,
    complete_receipt_id: ResourceId,
    complete_event_id: ResourceId,
    complete_outbox_id: ResourceId,
    scan_receipt_id: ResourceId,
    scan_event_id: ResourceId,
    scan_outbox_id: ResourceId,
    expected_artifact_version: u64,
    expected_blob_version: u64,
    expected_operation_version: u64,
    expected_grant_version: u64,
    grant_generation: u64,
    upload_grant_token: String,
    object_generation: String,
    expected_size_bytes: u64,
    scan_policy_revision: ExactVersionRef,
    scanner_contract_digest: Sha256Digest,
    ruleset_digest: Sha256Digest,
    evidence_ttl_milliseconds: u64,
    retry_backoff_milliseconds: u64,
    deadline: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct CompleteUploadResponse {
    schema_version: u32,
    artifact_id: ResourceId,
    operation_id: ResourceId,
    artifact_state: String,
    artifact_version: u64,
    operation_state: String,
    operation_version: u64,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ArtifactResponse {
    schema_version: u32,
    artifact_id: ResourceId,
    purpose: ArtifactPurpose,
    classification: DataClassification,
    state: String,
    version: u64,
    size_bytes: Option<u64>,
    content_digest: Option<Sha256Digest>,
    media_type: Option<String>,
    display_name: Option<String>,
    retain_until: DateTime<Utc>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DeleteArtifactRequest {
    deletion_operation_id: ResourceId,
    receipt_id: ResourceId,
    event_id: ResourceId,
    outbox_id: ResourceId,
    expected_artifact_version: u64,
    approval_task_id: Option<ResourceId>,
    retry_backoff_milliseconds: u64,
    deadline: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct DeleteArtifactResponse {
    schema_version: u32,
    artifact_id: ResourceId,
    artifact_state: String,
    artifact_version: u64,
    operation_id: ResourceId,
    operation_state: String,
    operation_version: u64,
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
    let reader = BrokeredGatewayArtifactReader::new(
        repository.clone(),
        unsealer,
        stores,
        ArtifactBrokerLimits {
            maximum_in_flight: config.maximum_download_in_flight,
            maximum_read_bytes: config.maximum_download_bytes,
            operation_timeout: Duration::from_millis(config.download_timeout_milliseconds),
        },
    )
    .map_err(|_| GatewayError::InvalidConfiguration)?;
    let state = GatewayState {
        repository,
        uploads,
        reader: Arc::new(reader),
        maximum_upload_target_seconds: config.maximum_upload_target_seconds,
        maximum_download_bytes: config.maximum_download_bytes,
        download_timeout_milliseconds: config.download_timeout_milliseconds,
    };
    let app = Router::new()
        .route("/readyz", get(ready))
        .route("/v1/artifacts:prepare-upload", post(prepare_upload))
        .route(
            "/v1/artifacts/{artifact_id}:complete-upload",
            post(complete_upload),
        )
        .route("/v1/artifacts/{artifact_id}", get(get_artifact))
        .route(
            "/v1/artifacts/{artifact_id}/content",
            get(get_artifact_content),
        )
        .route("/v1/artifacts/{artifact_id}:delete", post(delete_artifact))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(state);
    let address: SocketAddr = config
        .listen_address
        .parse()
        .map_err(|_| GatewayError::InvalidConfiguration)?;
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|_| GatewayError::HttpUnavailable)?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|_| GatewayError::HttpUnavailable)
}

async fn ready() -> Response {
    no_store((StatusCode::OK, "ready").into_response())
}

async fn prepare_upload(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<PrepareUploadRequest>,
) -> Response {
    match prepare_upload_inner(state, &headers, request).await {
        Ok(response) => no_store((StatusCode::CREATED, Json(response)).into_response()),
        Err(error) => problem(error),
    }
}

async fn prepare_upload_inner(
    state: GatewayState,
    headers: &HeaderMap,
    request: PrepareUploadRequest,
) -> Result<PrepareUploadResponse, HttpError> {
    let principal = mutation_principal(headers)?;
    let request_digest = request_digest("artifact.prepare_upload", &request)?;
    let now = Utc::now();
    let target_seconds = state.maximum_upload_target_seconds.min(
        u64::try_from((request.grant_expires_at - now).num_seconds())
            .map_err(|_| HttpError::Invalid)?,
    );
    if target_seconds == 0 {
        return Err(HttpError::Invalid);
    }
    let upload = state
        .uploads
        .prepare_upload(
            &principal.tenant_id,
            &request.artifact_id,
            &request.blob_id,
            &request.encryption_domain_id,
            request.expected_size_bytes,
            request.declared_media_type.as_deref(),
            Duration::from_secs(target_seconds),
        )
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let token_digest = token_digest(&request.upload_grant_token)?;
    let command = PrepareArtifact {
        audit: audit(
            principal,
            request.receipt_id,
            request.event_id,
            request.outbox_id,
            request_digest,
            request.operation_deadline,
        ),
        operation_id: request.operation_id,
        artifact_id: request.artifact_id,
        blob_id: request.blob_id,
        upload_grant_id: request.upload_grant_id,
        quota_account_id: request.quota_account_id,
        quota_entry_id: request.quota_entry_id,
        purpose: request.purpose,
        classification: request.classification,
        expected_size_bytes: request.expected_size_bytes,
        expected_digest: request.expected_digest,
        declared_media_type: request.declared_media_type,
        retention_policy_revision_id: request.retention_policy_revision_id,
        retain_until: request.retain_until,
        operation_deadline: request.operation_deadline,
        grant_expires_at: request.grant_expires_at,
        grant_token_digest: token_digest,
        storage_backend: upload.storage_backend,
        storage_binding_digest: upload.storage_binding_digest,
        object_reference_ciphertext: upload.object_reference_ciphertext,
        key_id: upload.key_id,
        encryption_domain_id: request.encryption_domain_id,
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
    Ok(PrepareUploadResponse {
        schema_version: 1,
        artifact_id: prepared.artifact.artifact_id,
        operation_id: prepared.operation.operation_id,
        upload_grant_id: prepared.grant.upload_grant_id,
        artifact_version: prepared.artifact.version,
        blob_version: prepared.blob.version,
        operation_version: prepared.operation.version,
        operation_state: prepared.operation.state.to_string(),
        grant_version: prepared.grant.version,
        upload_url: upload.upload_url,
        upload_expires_at: now
            + chrono::Duration::seconds(
                i64::try_from(target_seconds).map_err(|_| HttpError::Invalid)?,
            ),
    })
}

async fn complete_upload(
    State(state): State<GatewayState>,
    AxumPath(artifact_id): AxumPath<ResourceId>,
    headers: HeaderMap,
    Json(request): Json<CompleteUploadRequest>,
) -> Response {
    if artifact_id != request.artifact_id {
        return problem(HttpError::Invalid);
    }
    match complete_upload_inner(state, &headers, request).await {
        Ok(response) => no_store((StatusCode::ACCEPTED, Json(response)).into_response()),
        Err(error) => problem(error),
    }
}

async fn complete_upload_inner(
    state: GatewayState,
    headers: &HeaderMap,
    request: CompleteUploadRequest,
) -> Result<CompleteUploadResponse, HttpError> {
    let principal = mutation_principal(headers)?;
    let complete_digest = request_digest("artifact.complete_upload", &request)?;
    let scan_digest = request_digest("artifact.schedule_scan", &request)?;
    let observed = state
        .uploads
        .complete_upload(
            &principal.tenant_id,
            &request.artifact_id,
            &request.blob_id,
            &request.object_generation,
            request.expected_size_bytes,
        )
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let grant_token_digest = token_digest(&request.upload_grant_token)?;
    let mut transaction = state
        .repository
        .begin()
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let completed = transaction
        .complete_upload(CompleteArtifactUpload {
            audit: audit(
                principal.clone(),
                request.complete_receipt_id,
                request.complete_event_id,
                request.complete_outbox_id,
                complete_digest,
                request.deadline,
            ),
            operation_id: request.operation_id.clone(),
            artifact_id: request.artifact_id.clone(),
            blob_id: request.blob_id.clone(),
            upload_grant_id: request.upload_grant_id,
            expected_artifact_version: request.expected_artifact_version,
            expected_blob_version: request.expected_blob_version,
            expected_operation_version: request.expected_operation_version,
            expected_grant_version: request.expected_grant_version,
            grant_generation: request.grant_generation,
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
                request.scan_receipt_id,
                request.scan_event_id,
                request.scan_outbox_id,
                scan_digest,
                request.deadline,
            ),
            scan_job_id: request.operation_id.clone(),
            operation_id: request.operation_id,
            artifact_id: request.artifact_id,
            blob_id: request.blob_id,
            expected_artifact_version: completed.artifact.version,
            expected_blob_version: completed.blob.version,
            expected_operation_version: completed.operation.version,
            scan_policy_revision: request.scan_policy_revision,
            scanner_contract_digest: request.scanner_contract_digest,
            ruleset_digest: request.ruleset_digest,
            evidence_ttl_milliseconds: request.evidence_ttl_milliseconds,
            retry_backoff_milliseconds: request.retry_backoff_milliseconds,
            deadline: request.deadline,
        })
        .await
        .map_err(map_repository_error)?;
    transaction
        .commit()
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let scan = outcome_value(scan);
    Ok(CompleteUploadResponse {
        schema_version: 1,
        artifact_id: scan.artifact.artifact_id,
        operation_id: scan.operation.operation_id,
        artifact_state: scan.artifact.state.to_string(),
        artifact_version: scan.artifact.version,
        operation_state: scan.operation.state.to_string(),
        operation_version: scan.operation.version,
    })
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
        Ok::<_, HttpError>(artifact_response(snapshot))
    }
    .await;
    match result {
        Ok(response) => {
            let etag = format!("\"{}\"", response.version);
            let mut response = no_store((StatusCode::OK, Json(response)).into_response());
            if let Ok(value) = HeaderValue::from_str(&etag) {
                response.headers_mut().insert(ETAG, value);
            }
            response
        }
        Err(error) => problem(error),
    }
}

fn artifact_response(snapshot: GatewayArtifactSnapshot) -> ArtifactResponse {
    let content = snapshot.content.as_ref();
    ArtifactResponse {
        schema_version: 1,
        artifact_id: snapshot.artifact.artifact_id,
        purpose: snapshot.artifact.purpose,
        classification: snapshot.artifact.classification,
        state: snapshot.artifact.state.to_string(),
        version: snapshot.artifact.version,
        size_bytes: content.map(|content| content.byte_length()),
        content_digest: content.map(|content| content.content_digest().clone()),
        media_type: content.map(|content| content.media_type().to_owned()),
        display_name: snapshot.artifact.metadata.display_name,
        retain_until: snapshot.artifact.retain_until,
        created_at: snapshot.artifact.created_at,
        updated_at: snapshot.artifact.updated_at,
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

async fn delete_artifact(
    State(state): State<GatewayState>,
    AxumPath(artifact_id): AxumPath<ResourceId>,
    headers: HeaderMap,
    Json(request): Json<DeleteArtifactRequest>,
) -> Response {
    match delete_artifact_inner(state, &headers, artifact_id, request).await {
        Ok(response) => no_store((StatusCode::ACCEPTED, Json(response)).into_response()),
        Err(error) => problem(error),
    }
}

async fn delete_artifact_inner(
    state: GatewayState,
    headers: &HeaderMap,
    artifact_id: ResourceId,
    request: DeleteArtifactRequest,
) -> Result<DeleteArtifactResponse, HttpError> {
    let principal = mutation_principal(headers)?;
    let request_digest = request_digest("artifact.delete", &request)?;
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
    let mut transaction = state
        .repository
        .begin()
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let marked = transaction
        .mark_deletion(MarkArtifactDeletion {
            audit: audit(
                principal,
                request.receipt_id,
                request.event_id,
                request.outbox_id,
                request_digest,
                request.deadline,
            ),
            deletion_operation_id: request.deletion_operation_id.clone(),
            deletion_job_id: request.deletion_operation_id,
            artifact_id,
            blob_id: target.blob.blob_id,
            expected_artifact_version: request.expected_artifact_version,
            expected_blob_version: target.blob.version,
            approval_task_id: request.approval_task_id,
            retry_backoff_milliseconds: request.retry_backoff_milliseconds,
            deadline: request.deadline,
        })
        .await
        .map_err(map_repository_error)?;
    transaction
        .commit()
        .await
        .map_err(|_| HttpError::Unavailable)?;
    let marked = outcome_value(marked);
    Ok(DeleteArtifactResponse {
        schema_version: 1,
        artifact_id: marked.artifact.artifact_id,
        artifact_state: marked.artifact.state.to_string(),
        artifact_version: marked.artifact.version,
        operation_id: marked.deletion.operation_id,
        operation_state: marked.deletion.operation_state.to_string(),
        operation_version: marked.deletion.operation_version,
    })
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
        tenant_id: value("x-platform-tenant-id")?
            .parse()
            .map_err(|_| HttpError::Unauthenticated)?,
        principal_id: value("x-platform-principal-id")?
            .parse()
            .map_err(|_| HttpError::Unauthenticated)?,
        principal_kind: PrincipalKind::from_str(value("x-platform-principal-kind")?)
            .map_err(|_| HttpError::Unauthenticated)?,
        idempotency_key_digest: None,
    })
}

fn mutation_principal(headers: &HeaderMap) -> Result<PrincipalHeaders, HttpError> {
    let mut principal = read_principal(headers)?;
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 512)
        .ok_or(HttpError::Invalid)?;
    principal.idempotency_key_digest = Some(token_digest(idempotency_key)?);
    Ok(principal)
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
        RepositoryError::Conflict(_) | RepositoryError::IdempotencyConflict => HttpError::Conflict,
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
    TooLarge,
    Unavailable,
}

fn problem(error: HttpError) -> Response {
    let (status, code) = match error {
        HttpError::Invalid => (StatusCode::BAD_REQUEST, "invalid_request"),
        HttpError::Unauthenticated => (StatusCode::UNAUTHORIZED, "authentication_required"),
        HttpError::Forbidden => (StatusCode::FORBIDDEN, "permission_denied"),
        HttpError::NotFound => (StatusCode::NOT_FOUND, "artifact_not_found"),
        HttpError::Conflict => (StatusCode::CONFLICT, "artifact_conflict"),
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
        }
    }
}

impl Error for GatewayError {}
