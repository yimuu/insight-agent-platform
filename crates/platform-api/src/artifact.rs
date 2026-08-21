use crate::authentication::AuthenticatedPrincipal;
use async_trait::async_trait;
use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::{
        header::{
            CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, ETAG, IF_MATCH,
            LOCATION,
        },
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use futures::Stream;
use insight_platform_artifacts::ArtifactRecord;
use insight_platform_contracts::{
    parse_strict_json, ApiProblem, ApiProblemCode, ArtifactRef, ArtifactState, JsonLimits,
    ResourceId, ResourceKind, Sha256Digest, UtcTimestamp, MAX_FIELD_ERRORS, MAX_SAFE_TEXT_BYTES,
};
use serde::{Deserialize, Serialize};
use std::{fmt, pin::Pin, sync::Arc};

const MAX_PUBLIC_ARTIFACT_BYTES: u64 = 1_073_741_824;
const MAX_MEDIA_TYPE_BYTES: usize = 255;
const MAX_DISPLAY_NAME_BYTES: usize = 512;
const MAX_UPLOAD_URL_BYTES: usize = 8_192;
const MAX_COMPLETION_PROOF_BYTES: usize = 4_096;
const MAX_ARTIFACT_REQUEST_BYTES: usize = 262_144;
const MIN_IDEMPOTENCY_KEY_BYTES: usize = 32;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 512;
const ARTIFACT_COMMAND_DEADLINE_MILLISECONDS: i64 = 30_000;
const ARTIFACT_READ_DEADLINE_MILLISECONDS: i64 = 5_000;

#[derive(Clone, PartialEq, Eq)]
struct SecretIdempotencyKey(String);

impl SecretIdempotencyKey {
    fn new(value: impl Into<String>) -> Result<Self, ArtifactApplicationError> {
        let value = value.into();
        if value.len() < MIN_IDEMPOTENCY_KEY_BYTES
            || value.len() > MAX_IDEMPOTENCY_KEY_BYTES
            || !value.is_ascii()
            || value.chars().any(char::is_control)
        {
            return Err(ArtifactApplicationError::Invalid);
        }
        Ok(Self(value))
    }

    fn digest(&self) -> Result<Sha256Digest, ArtifactApplicationError> {
        let bytes = ring::digest::digest(&ring::digest::SHA256, self.0.as_bytes());
        let mut encoded = String::from("sha256:");
        for byte in bytes.as_ref() {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}").map_err(|_| ArtifactApplicationError::Internal)?;
        }
        encoded
            .parse()
            .map_err(|_| ArtifactApplicationError::Internal)
    }
}

impl fmt::Debug for SecretIdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretIdempotencyKey([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareArtifactUploadRequestV1 {
    pub schema_version: u32,
    pub purpose: insight_platform_contracts::ArtifactPurpose,
    pub classification: insight_platform_contracts::DataClassification,
    pub expected_size_bytes: u64,
    pub expected_digest: Option<insight_platform_contracts::Sha256Digest>,
    pub declared_media_type: Option<String>,
    pub display_name: Option<String>,
}

impl PrepareArtifactUploadRequestV1 {
    pub fn validate(&self) -> Result<(), ArtifactApplicationError> {
        if self.schema_version != 1
            || self.expected_size_bytes == 0
            || self.expected_size_bytes > MAX_PUBLIC_ARTIFACT_BYTES
            || self
                .declared_media_type
                .as_deref()
                .is_some_and(|value| !valid_media_type(value))
            || self
                .display_name
                .as_deref()
                .is_some_and(|value| !valid_safe_text(value, MAX_DISPLAY_NAME_BYTES))
        {
            return Err(ArtifactApplicationError::Invalid);
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpaqueUploadCompletionProof(String);

impl OpaqueUploadCompletionProof {
    pub fn new(value: impl Into<String>) -> Result<Self, ArtifactApplicationError> {
        let value = value.into();
        if !valid_bounded_ascii(&value, MAX_COMPLETION_PROOF_BYTES) {
            return Err(ArtifactApplicationError::Invalid);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), ArtifactApplicationError> {
        Self::new(self.0.clone()).map(|_| ())
    }
}

impl fmt::Debug for OpaqueUploadCompletionProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpaqueUploadCompletionProof([REDACTED])")
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretBearingUploadTargetV1 {
    pub url: String,
    pub completion_proof: OpaqueUploadCompletionProof,
}

impl SecretBearingUploadTargetV1 {
    fn validate(&self) -> Result<(), ArtifactApplicationError> {
        let parsed = url::Url::parse(&self.url).map_err(|_| ArtifactApplicationError::Internal)?;
        if self.url.len() > MAX_UPLOAD_URL_BYTES
            || parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || parsed.username() != ""
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(ArtifactApplicationError::Internal);
        }
        self.completion_proof
            .validate()
            .map_err(|_| ArtifactApplicationError::Internal)
    }
}

impl fmt::Debug for SecretBearingUploadTargetV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretBearingUploadTargetV1([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareArtifactUploadResponseV1 {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub operation_id: ResourceId,
    pub upload_grant_id: ResourceId,
    pub artifact_etag: String,
    pub upload_target: SecretBearingUploadTargetV1,
    pub upload_expires_at: UtcTimestamp,
}

impl PrepareArtifactUploadResponseV1 {
    pub fn validate(&self) -> Result<(), ArtifactApplicationError> {
        if self.schema_version != 1
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.operation_id.kind() != ResourceKind::Job
            || self.upload_grant_id.kind() != ResourceKind::ArtifactGrant
            || !valid_strong_etag(&self.artifact_etag)
        {
            return Err(ArtifactApplicationError::Internal);
        }
        self.upload_target.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteArtifactUploadRequestV1 {
    pub schema_version: u32,
    pub completion_proof: OpaqueUploadCompletionProof,
}

impl CompleteArtifactUploadRequestV1 {
    pub fn validate(&self) -> Result<(), ArtifactApplicationError> {
        if self.schema_version != 1 {
            return Err(ArtifactApplicationError::Invalid);
        }
        self.completion_proof.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactMutationAcceptedV1 {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub artifact_etag: String,
    pub operation_id: ResourceId,
}

impl ArtifactMutationAcceptedV1 {
    pub fn validate(&self) -> Result<(), ArtifactApplicationError> {
        if self.schema_version != 1
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.operation_id.kind() != ResourceKind::Job
            || !valid_strong_etag(&self.artifact_etag)
        {
            return Err(ArtifactApplicationError::Internal);
        }
        Ok(())
    }
}

fn valid_bounded_ascii(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.is_ascii()
        && !value.chars().any(char::is_control)
}

fn valid_media_type(value: &str) -> bool {
    if !valid_bounded_ascii(value, MAX_MEDIA_TYPE_BYTES) {
        return false;
    }
    let essence = value.split(';').next().unwrap_or_default().trim();
    let Some((top_level, subtype)) = essence.split_once('/') else {
        return false;
    };
    let valid_token = |token: &str| {
        !token.is_empty()
            && token.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
                    )
            })
    };
    valid_token(top_level) && valid_token(subtype)
}

fn valid_safe_text(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

fn valid_strong_etag(value: &str) -> bool {
    value.len() >= 3
        && value.len() <= 128
        && value.starts_with('"')
        && value.ends_with('"')
        && !value.starts_with("W/")
        && value[1..value.len() - 1]
            .chars()
            .all(|character| character.is_ascii_graphic() && character != '"')
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactViewV1 {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub purpose: insight_platform_contracts::ArtifactPurpose,
    pub classification: insight_platform_contracts::DataClassification,
    pub state: ArtifactState,
    pub version: u64,
    pub expected_size_bytes: u64,
    pub declared_media_type: Option<String>,
    pub verified_media_type: Option<String>,
    pub content: Option<ArtifactRef>,
    pub retain_until: UtcTimestamp,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub etag: String,
}

impl ArtifactViewV1 {
    pub fn validate(&self) -> Result<(), ArtifactApplicationError> {
        if self.schema_version != 1
            || self.artifact_id.kind() != ResourceKind::Artifact
            || self.version == 0
            || self.etag != artifact_etag(&self.artifact_id, self.version)
            || self.content.is_some() != (self.state == ArtifactState::Ready)
            || self.content.as_ref().is_some_and(|content| {
                content.artifact_id() != &self.artifact_id
                    || content.classification() != self.classification
            })
        {
            return Err(ArtifactApplicationError::Internal);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReadArtifactIntent {
    pub principal: AuthenticatedPrincipal,
    pub artifact_id: ResourceId,
    pub deadline: DateTime<Utc>,
}

pub struct ArtifactContentStreamV1 {
    pub stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
    pub content_length: u64,
    pub media_type: String,
    pub etag: String,
}

impl ArtifactContentStreamV1 {
    pub fn validate(&self) -> Result<(), ArtifactApplicationError> {
        if self.content_length == 0
            || self.content_length > MAX_PUBLIC_ARTIFACT_BYTES
            || !valid_media_type(&self.media_type)
            || !valid_strong_etag(&self.etag)
        {
            return Err(ArtifactApplicationError::Internal);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PrepareArtifactUploadIntent {
    pub principal: AuthenticatedPrincipal,
    pub request: PrepareArtifactUploadRequestV1,
    pub idempotency_key_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CompleteArtifactUploadIntent {
    pub principal: AuthenticatedPrincipal,
    pub artifact_id: ResourceId,
    pub expected_artifact_version: u64,
    pub request: CompleteArtifactUploadRequestV1,
    pub idempotency_key_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DeleteArtifactIntent {
    pub principal: AuthenticatedPrincipal,
    pub artifact_id: ResourceId,
    pub expected_artifact_version: u64,
    pub idempotency_key_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactApplicationError {
    Unauthenticated,
    Invalid,
    Denied,
    NotFound,
    Conflict,
    IdempotencyConflict,
    PreconditionRequired,
    TooLarge,
    Unavailable,
    Internal,
}

#[async_trait]
pub trait ArtifactApplication: Send + Sync {
    async fn prepare_artifact_upload(
        &self,
        intent: PrepareArtifactUploadIntent,
    ) -> Result<PrepareArtifactUploadResponseV1, ArtifactApplicationError>;

    async fn complete_artifact_upload(
        &self,
        intent: CompleteArtifactUploadIntent,
    ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError>;

    async fn delete_artifact(
        &self,
        intent: DeleteArtifactIntent,
    ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError>;

    async fn read_artifact(
        &self,
        intent: ReadArtifactIntent,
    ) -> Result<ArtifactViewV1, ArtifactApplicationError>;

    async fn read_artifact_content(
        &self,
        intent: ReadArtifactIntent,
    ) -> Result<ArtifactContentStreamV1, ArtifactApplicationError>;
}

pub trait ArtifactClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}
#[derive(Debug, Default)]
pub struct SystemArtifactClock;
impl ArtifactClock for SystemArtifactClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone)]
pub struct ArtifactHttpState {
    application: Arc<dyn ArtifactApplication>,
    clock: Arc<dyn ArtifactClock>,
}
impl ArtifactHttpState {
    pub fn new(application: Arc<dyn ArtifactApplication>, clock: Arc<dyn ArtifactClock>) -> Self {
        Self { application, clock }
    }
}

pub fn build_artifact_router(state: ArtifactHttpState) -> Router {
    Router::new()
        .route(
            "/v1/artifacts:prepare-upload",
            post(prepare_artifact_upload),
        )
        .route(
            "/v1/artifacts/{artifact_action}",
            get(read_artifact).post(mutate_artifact),
        )
        .route(
            "/v1/artifacts/{artifact_id}/content",
            get(read_artifact_content),
        )
        .layer(DefaultBodyLimit::max(MAX_ARTIFACT_REQUEST_BYTES))
        .with_state(state)
}

async fn prepare_artifact_upload(
    State(state): State<ArtifactHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    headers: HeaderMap,
    Json(request): Json<PrepareArtifactUploadRequestV1>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ArtifactApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ArtifactApplicationError::Unauthenticated);
    }
    if request.validate().is_err() {
        return problem(ArtifactApplicationError::Invalid);
    }
    let idempotency_key_digest = match idempotency_key_digest(&headers) {
        Ok(value) => value,
        Err(error) => return problem(error),
    };
    match state
        .application
        .prepare_artifact_upload(PrepareArtifactUploadIntent {
            principal,
            request,
            idempotency_key_digest,
            deadline: state.clock.now()
                + Duration::milliseconds(ARTIFACT_COMMAND_DEADLINE_MILLISECONDS),
        })
        .await
    {
        Ok(view) if view.validate().is_ok() => prepare_upload_response(view),
        Ok(_) => problem(ArtifactApplicationError::Internal),
        Err(error) => problem(error),
    }
}

async fn mutate_artifact(
    State(state): State<ArtifactHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(artifact_action): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ArtifactApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ArtifactApplicationError::Unauthenticated);
    }
    let (artifact_id, action) = if let Some(id) = artifact_action.strip_suffix(":complete-upload") {
        (id, "complete")
    } else if let Some(id) = artifact_action.strip_suffix(":delete") {
        (id, "delete")
    } else {
        return problem(ArtifactApplicationError::NotFound);
    };
    let artifact_id = match artifact_id.parse::<ResourceId>() {
        Ok(id) if id.kind() == ResourceKind::Artifact => id,
        _ => return problem(ArtifactApplicationError::NotFound),
    };
    let expected_artifact_version = match expected_artifact_version(&headers, &artifact_id) {
        Ok(value) => value,
        Err(error) => return problem(error),
    };
    let idempotency_key_digest = match idempotency_key_digest(&headers) {
        Ok(value) => value,
        Err(error) => return problem(error),
    };
    let deadline =
        state.clock.now() + Duration::milliseconds(ARTIFACT_COMMAND_DEADLINE_MILLISECONDS);
    let result = if action == "complete" {
        if !has_json_content_type(&headers) {
            return problem(ArtifactApplicationError::Invalid);
        }
        let Ok(value) = parse_strict_json(
            &body,
            JsonLimits {
                max_bytes: MAX_ARTIFACT_REQUEST_BYTES,
                max_depth: 8,
                max_properties_per_object: 16,
                max_items_per_array: 16,
                max_string_bytes: MAX_COMPLETION_PROOF_BYTES,
            },
        ) else {
            return problem(ArtifactApplicationError::Invalid);
        };
        let Ok(request) = serde_json::from_value::<CompleteArtifactUploadRequestV1>(value) else {
            return problem(ArtifactApplicationError::Invalid);
        };
        if request.validate().is_err() {
            return problem(ArtifactApplicationError::Invalid);
        }
        state
            .application
            .complete_artifact_upload(CompleteArtifactUploadIntent {
                principal,
                artifact_id,
                expected_artifact_version,
                request,
                idempotency_key_digest,
                deadline,
            })
            .await
    } else {
        if !body.is_empty() {
            return problem(ArtifactApplicationError::Invalid);
        }
        state
            .application
            .delete_artifact(DeleteArtifactIntent {
                principal,
                artifact_id,
                expected_artifact_version,
                idempotency_key_digest,
                deadline,
            })
            .await
    };
    match result {
        Ok(view) if view.validate().is_ok() => mutation_accepted_response(view),
        Ok(_) => problem(ArtifactApplicationError::Internal),
        Err(error) => problem(error),
    }
}

fn has_json_content_type(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let Some(value) = values.next().and_then(|value| value.to_str().ok()) else {
        return false;
    };
    if values.next().is_some() || value.contains(',') {
        return false;
    }
    let mut parts = value.split(';').map(str::trim);
    if !parts
        .next()
        .is_some_and(|media_type| media_type.eq_ignore_ascii_case("application/json"))
    {
        return false;
    }
    parts.all(|parameter| parameter.eq_ignore_ascii_case("charset=utf-8"))
}

async fn read_artifact(
    State(state): State<ArtifactHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(artifact_id): Path<String>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ArtifactApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ArtifactApplicationError::Unauthenticated);
    }
    let artifact_id = match artifact_id.parse::<ResourceId>() {
        Ok(id) if id.kind() == ResourceKind::Artifact => id,
        _ => return problem(ArtifactApplicationError::NotFound),
    };
    match state
        .application
        .read_artifact(ReadArtifactIntent {
            principal,
            artifact_id,
            deadline: state.clock.now()
                + Duration::milliseconds(ARTIFACT_READ_DEADLINE_MILLISECONDS),
        })
        .await
    {
        Ok(view) if view.validate().is_ok() => artifact_response(view),
        Ok(_) => problem(ArtifactApplicationError::Internal),
        Err(error) => problem(error),
    }
}

async fn read_artifact_content(
    State(state): State<ArtifactHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(artifact_id): Path<String>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ArtifactApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ArtifactApplicationError::Unauthenticated);
    }
    let artifact_id = match artifact_id.parse::<ResourceId>() {
        Ok(id) if id.kind() == ResourceKind::Artifact => id,
        _ => return problem(ArtifactApplicationError::NotFound),
    };
    match state
        .application
        .read_artifact_content(ReadArtifactIntent {
            principal,
            artifact_id,
            deadline: state.clock.now()
                + Duration::milliseconds(ARTIFACT_COMMAND_DEADLINE_MILLISECONDS),
        })
        .await
    {
        Ok(content) if content.validate().is_ok() => artifact_content_response(content),
        Ok(_) => problem(ArtifactApplicationError::Internal),
        Err(error) => problem(error),
    }
}

fn artifact_content_response(content: ArtifactContentStreamV1) -> Response {
    let (Ok(length), Ok(media_type), Ok(etag)) = (
        HeaderValue::from_str(&content.content_length.to_string()),
        HeaderValue::from_str(&content.media_type),
        HeaderValue::from_str(&content.etag),
    ) else {
        return problem(ArtifactApplicationError::Internal);
    };
    let mut response = (StatusCode::OK, Body::from_stream(content.stream)).into_response();
    response.headers_mut().insert(CONTENT_LENGTH, length);
    response.headers_mut().insert(CONTENT_TYPE, media_type);
    response
        .headers_mut()
        .insert(CONTENT_DISPOSITION, HeaderValue::from_static("attachment"));
    response.headers_mut().insert(ETAG, etag);
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
}

fn idempotency_key_digest(headers: &HeaderMap) -> Result<Sha256Digest, ArtifactApplicationError> {
    let mut values = headers.get_all("idempotency-key").iter();
    let value = values
        .next()
        .ok_or(ArtifactApplicationError::Invalid)?
        .to_str()
        .map_err(|_| ArtifactApplicationError::Invalid)?;
    if values.next().is_some() {
        return Err(ArtifactApplicationError::Invalid);
    }
    SecretIdempotencyKey::new(value)?.digest()
}

fn expected_artifact_version(
    headers: &HeaderMap,
    artifact_id: &ResourceId,
) -> Result<u64, ArtifactApplicationError> {
    let mut values = headers.get_all(IF_MATCH).iter();
    let value = values
        .next()
        .ok_or(ArtifactApplicationError::PreconditionRequired)?;
    if values.next().is_some() {
        return Err(ArtifactApplicationError::Invalid);
    }
    let value = value
        .to_str()
        .map_err(|_| ArtifactApplicationError::Invalid)?;
    let prefix = format!("\"{artifact_id}-");
    let version = value
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix('"'))
        .ok_or(ArtifactApplicationError::Invalid)?;
    if version.is_empty() || version.starts_with('0') {
        return Err(ArtifactApplicationError::Invalid);
    }
    version
        .parse()
        .map_err(|_| ArtifactApplicationError::Invalid)
}

fn prepare_upload_response(view: PrepareArtifactUploadResponseV1) -> Response {
    response_with_authority(
        StatusCode::CREATED,
        format!("/v1/artifacts/{}", view.artifact_id),
        view.artifact_etag.clone(),
        view,
    )
}

fn mutation_accepted_response(view: ArtifactMutationAcceptedV1) -> Response {
    response_with_authority(
        StatusCode::ACCEPTED,
        format!("/v1/operations/{}", view.operation_id),
        view.artifact_etag.clone(),
        view,
    )
}

fn response_with_authority<T: Serialize>(
    status: StatusCode,
    location: String,
    etag: String,
    body: T,
) -> Response {
    let (Ok(location), Ok(etag)) = (
        HeaderValue::from_str(&location),
        HeaderValue::from_str(&etag),
    ) else {
        return problem(ArtifactApplicationError::Internal);
    };
    let mut response = (status, Json(body)).into_response();
    response.headers_mut().insert(LOCATION, location);
    response.headers_mut().insert(ETAG, etag);
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
}

fn artifact_response(view: ArtifactViewV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(value) => value,
        Err(_) => return problem(ArtifactApplicationError::Internal),
    };
    let mut response = (StatusCode::OK, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
}
pub fn artifact_etag(id: &ResourceId, version: u64) -> String {
    format!("\"{id}-{version}\"")
}

fn problem(error: ArtifactApplicationError) -> Response {
    let (status, code, title, retryable) = match error {
        ArtifactApplicationError::Unauthenticated => (
            StatusCode::UNAUTHORIZED,
            ApiProblemCode::Unauthenticated,
            "Authentication is required.",
            false,
        ),
        ArtifactApplicationError::Invalid => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::InvalidRequest,
            "The Artifact request is invalid.",
            false,
        ),
        ArtifactApplicationError::Denied => (
            StatusCode::FORBIDDEN,
            ApiProblemCode::PermissionDenied,
            "The Artifact is not available to this principal.",
            false,
        ),
        ArtifactApplicationError::NotFound => (
            StatusCode::NOT_FOUND,
            ApiProblemCode::ResourceNotFound,
            "The Artifact was not found.",
            false,
        ),
        ArtifactApplicationError::Conflict => (
            StatusCode::PRECONDITION_FAILED,
            ApiProblemCode::PreconditionFailed,
            "The Artifact state conflicts with this request.",
            false,
        ),
        ArtifactApplicationError::PreconditionRequired => (
            StatusCode::PRECONDITION_REQUIRED,
            ApiProblemCode::PreconditionRequired,
            "A current Artifact If-Match precondition is required.",
            false,
        ),
        ArtifactApplicationError::IdempotencyConflict => (
            StatusCode::CONFLICT,
            ApiProblemCode::IdempotencyConflict,
            "The idempotency key was used for a different Artifact request.",
            false,
        ),
        ArtifactApplicationError::TooLarge => (
            StatusCode::PAYLOAD_TOO_LARGE,
            ApiProblemCode::QuotaExceeded,
            "The Artifact request exceeds the admitted quota.",
            false,
        ),
        ArtifactApplicationError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            ApiProblemCode::TemporarilyUnavailable,
            "The Artifact authority is temporarily unavailable.",
            true,
        ),
        ArtifactApplicationError::Internal => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiProblemCode::InternalError,
            "The Artifact response could not be projected.",
            false,
        ),
    };
    let request_id = ResourceId::from_uuid_v7(ResourceKind::ServerRequest, uuid::Uuid::now_v7())
        .expect("request ID");
    let problem = ApiProblem {
        type_uri: format!("https://insight.platform/problems/{}", code.as_str()),
        title: title.to_owned(),
        status: status.as_u16(),
        code,
        detail: None,
        request_id,
        retryable,
        retry_after_ms: retryable.then_some(1_000),
        field_errors: Vec::new(),
    };
    debug_assert!(problem
        .validate(MAX_SAFE_TEXT_BYTES, MAX_FIELD_ERRORS)
        .is_ok());
    let mut response = (status, Json(problem)).into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
}

pub fn view_from_record(record: ArtifactRecord, content: Option<ArtifactRef>) -> ArtifactViewV1 {
    let etag = artifact_etag(&record.artifact_id, record.version);
    ArtifactViewV1 {
        schema_version: 1,
        artifact_id: record.artifact_id,
        purpose: record.purpose,
        classification: record.classification,
        state: record.state,
        version: record.version,
        expected_size_bytes: record.expected_size_bytes,
        declared_media_type: record.declared_media_type,
        verified_media_type: record.verified_media_type,
        content,
        retain_until: UtcTimestamp::from_datetime(record.retain_until),
        created_at: UtcTimestamp::from_datetime(record.created_at),
        updated_at: UtcTimestamp::from_datetime(record.updated_at),
        etag,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use insight_platform_contracts::{
        ArtifactPurpose, AuthnStrength, DataClassification, Permission, PermissionSet,
        PrincipalKind, Sha256Digest,
    };
    use tower::ServiceExt;
    fn id(kind: ResourceKind, n: u16) -> ResourceId {
        format!(
            "{}_0198f1cc-32e4-75e1-a9e8-d95ca0f8{n:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }
    fn digest(c: char) -> Sha256Digest {
        format!("sha256:{}", c.to_string().repeat(64))
            .parse()
            .unwrap()
    }
    struct Clock(DateTime<Utc>);
    impl ArtifactClock for Clock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }
    struct App;
    #[async_trait]
    impl ArtifactApplication for App {
        async fn prepare_artifact_upload(
            &self,
            _intent: PrepareArtifactUploadIntent,
        ) -> Result<PrepareArtifactUploadResponseV1, ArtifactApplicationError> {
            Ok(PrepareArtifactUploadResponseV1 {
                schema_version: 1,
                artifact_id: id(ResourceKind::Artifact, 3),
                operation_id: id(ResourceKind::Job, 4),
                upload_grant_id: id(ResourceKind::ArtifactGrant, 5),
                artifact_etag: artifact_etag(&id(ResourceKind::Artifact, 3), 1),
                upload_target: SecretBearingUploadTargetV1 {
                    url: "https://uploads.example/artifact".to_owned(),
                    completion_proof: OpaqueUploadCompletionProof::new("v1.proof").unwrap(),
                },
                upload_expires_at: UtcTimestamp::from_datetime(Utc::now() + Duration::minutes(5)),
            })
        }

        async fn complete_artifact_upload(
            &self,
            intent: CompleteArtifactUploadIntent,
        ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError> {
            Ok(ArtifactMutationAcceptedV1 {
                schema_version: 1,
                artifact_id: intent.artifact_id.clone(),
                artifact_etag: artifact_etag(
                    &intent.artifact_id,
                    intent.expected_artifact_version + 2,
                ),
                operation_id: id(ResourceKind::Job, 4),
            })
        }

        async fn delete_artifact(
            &self,
            intent: DeleteArtifactIntent,
        ) -> Result<ArtifactMutationAcceptedV1, ArtifactApplicationError> {
            Ok(ArtifactMutationAcceptedV1 {
                schema_version: 1,
                artifact_id: intent.artifact_id.clone(),
                artifact_etag: artifact_etag(
                    &intent.artifact_id,
                    intent.expected_artifact_version + 1,
                ),
                operation_id: id(ResourceKind::Job, 6),
            })
        }

        async fn read_artifact(
            &self,
            intent: ReadArtifactIntent,
        ) -> Result<ArtifactViewV1, ArtifactApplicationError> {
            let now = UtcTimestamp::from_datetime(Utc::now());
            Ok(ArtifactViewV1 {
                schema_version: 1,
                artifact_id: intent.artifact_id.clone(),
                purpose: ArtifactPurpose::RunOutput,
                classification: DataClassification::Internal,
                state: ArtifactState::Ready,
                version: 2,
                expected_size_bytes: 4,
                declared_media_type: Some("text/plain".into()),
                verified_media_type: Some("text/plain".into()),
                content: Some(
                    ArtifactRef::new(
                        intent.artifact_id,
                        digest('a'),
                        4,
                        "text/plain",
                        DataClassification::Internal,
                        None,
                    )
                    .unwrap(),
                ),
                retain_until: now.clone(),
                created_at: now.clone(),
                updated_at: now,
                etag: artifact_etag(&id(ResourceKind::Artifact, 3), 2),
            })
        }

        async fn read_artifact_content(
            &self,
            _intent: ReadArtifactIntent,
        ) -> Result<ArtifactContentStreamV1, ArtifactApplicationError> {
            Ok(ArtifactContentStreamV1 {
                stream: Box::pin(futures::stream::once(async {
                    Ok(Bytes::from_static(b"test"))
                })),
                content_length: 4,
                media_type: "text/plain".to_owned(),
                etag: "\"sha256:test\"".to_owned(),
            })
        }
    }

    #[test]
    fn public_artifact_upload_contract_is_closed_bounded_and_secret_redacted() {
        let request = PrepareArtifactUploadRequestV1 {
            schema_version: 1,
            purpose: ArtifactPurpose::RunInput,
            classification: DataClassification::Internal,
            expected_size_bytes: 4,
            expected_digest: Some(digest('a')),
            declared_media_type: Some("text/plain; charset=utf-8".to_owned()),
            display_name: Some("input.txt".to_owned()),
        };
        assert!(request.validate().is_ok());
        let mut invalid = request.clone();
        invalid.expected_size_bytes = 0;
        assert_eq!(invalid.validate(), Err(ArtifactApplicationError::Invalid));
        let mut value = serde_json::to_value(&request).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("blob_id".to_owned(), serde_json::json!("forbidden"));
        assert!(serde_json::from_value::<PrepareArtifactUploadRequestV1>(value).is_err());

        let proof = OpaqueUploadCompletionProof::new("completion-secret").unwrap();
        let target = SecretBearingUploadTargetV1 {
            url: "https://uploads.example/artifact?signature=secret".to_owned(),
            completion_proof: proof,
        };
        let debug = format!("{target:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("completion-secret"));
        assert!(!debug.contains("signature=secret"));
        assert!(target.validate().is_ok());
    }

    #[test]
    fn public_artifact_upload_response_requires_nominal_ids_https_and_strong_etag() {
        let response = PrepareArtifactUploadResponseV1 {
            schema_version: 1,
            artifact_id: id(ResourceKind::Artifact, 3),
            operation_id: id(ResourceKind::Job, 4),
            upload_grant_id: id(ResourceKind::ArtifactGrant, 5),
            artifact_etag: "\"artifact-1\"".to_owned(),
            upload_target: SecretBearingUploadTargetV1 {
                url: "https://uploads.example/artifact".to_owned(),
                completion_proof: OpaqueUploadCompletionProof::new("proof").unwrap(),
            },
            upload_expires_at: UtcTimestamp::from_datetime(Utc::now() + Duration::minutes(5)),
        };
        assert!(response.validate().is_ok());
        let mut insecure = response.clone();
        insecure.upload_target.url = "http://uploads.example/artifact".to_owned();
        assert_eq!(insecure.validate(), Err(ArtifactApplicationError::Internal));
        let mut weak = response;
        weak.artifact_etag = "W/\"artifact-1\"".to_owned();
        assert_eq!(weak.validate(), Err(ArtifactApplicationError::Internal));
    }
    #[tokio::test]
    async fn artifact_read_is_nominal_and_no_store() {
        let now = Utc::now();
        let artifact_id = id(ResourceKind::Artifact, 3);
        let principal = AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant, 1),
            principal_id: id(ResourceKind::Principal, 2),
            principal_kind: PrincipalKind::AgentRunner,
            permissions: PermissionSet::new(vec![Permission::ArtifactRead]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: digest('b'),
            credential_expires_at: now + Duration::hours(1),
        };
        let response =
            build_artifact_router(ArtifactHttpState::new(Arc::new(App), Arc::new(Clock(now))))
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/artifacts/{artifact_id}"))
                        .extension(principal)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("etag").unwrap(),
            &artifact_etag(&artifact_id, 2)
        );
        assert_eq!(
            response.headers().get(CACHE_CONTROL).unwrap(),
            "no-store, private, max-age=0"
        );

        let content =
            build_artifact_router(ArtifactHttpState::new(Arc::new(App), Arc::new(Clock(now))))
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/artifacts/{artifact_id}/content"))
                        .extension(AuthenticatedPrincipal {
                            tenant_id: id(ResourceKind::Tenant, 1),
                            principal_id: id(ResourceKind::Principal, 2),
                            principal_kind: PrincipalKind::AgentRunner,
                            permissions: PermissionSet::new(vec![Permission::ArtifactRead])
                                .unwrap(),
                            authn_strength: AuthnStrength::MultiFactor,
                            principal_version: 1,
                            binding_generation: 1,
                            binding_version: 1,
                            credential_digest: digest('b'),
                            credential_expires_at: now + Duration::hours(1),
                        })
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        assert_eq!(content.status(), StatusCode::OK);
        assert_eq!(content.headers().get(CONTENT_LENGTH).unwrap(), "4");
        assert_eq!(content.headers().get(CONTENT_TYPE).unwrap(), "text/plain");
        assert_eq!(
            content.headers().get(CONTENT_DISPOSITION).unwrap(),
            "attachment"
        );
        assert_eq!(content.headers().get(ETAG).unwrap(), "\"sha256:test\"");
    }

    #[tokio::test]
    async fn artifact_upload_handlers_enforce_headers_and_project_authority() {
        let now = Utc::now();
        let artifact_id = id(ResourceKind::Artifact, 3);
        let principal = AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant, 1),
            principal_id: id(ResourceKind::Principal, 2),
            principal_kind: PrincipalKind::AgentRunner,
            permissions: PermissionSet::new(vec![Permission::ArtifactWrite]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: digest('b'),
            credential_expires_at: now + Duration::hours(1),
        };
        let router =
            build_artifact_router(ArtifactHttpState::new(Arc::new(App), Arc::new(Clock(now))));
        let prepare = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/artifacts:prepare-upload")
                    .header("content-type", "application/json")
                    .header(
                        "idempotency-key",
                        "artifact-upload-key-with-at-least-32-bytes",
                    )
                    .extension(principal.clone())
                    .body(Body::from(
                        serde_json::to_vec(&PrepareArtifactUploadRequestV1 {
                            schema_version: 1,
                            purpose: ArtifactPurpose::RunInput,
                            classification: DataClassification::Internal,
                            expected_size_bytes: 4,
                            expected_digest: Some(digest('a')),
                            declared_media_type: Some("text/plain".to_owned()),
                            display_name: Some("input.txt".to_owned()),
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(prepare.status(), StatusCode::CREATED);
        assert_eq!(
            prepare.headers().get(LOCATION).unwrap(),
            &format!("/v1/artifacts/{artifact_id}")
        );
        assert_eq!(
            prepare.headers().get(ETAG).unwrap(),
            &artifact_etag(&artifact_id, 1)
        );
        assert_eq!(
            prepare.headers().get(CACHE_CONTROL).unwrap(),
            "no-store, private, max-age=0"
        );

        let missing_precondition = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/artifacts/{artifact_id}:complete-upload"))
                    .header("content-type", "application/json")
                    .header(
                        "idempotency-key",
                        "artifact-complete-key-with-at-least-32-bytes",
                    )
                    .extension(principal.clone())
                    .body(Body::from(
                        serde_json::to_vec(&CompleteArtifactUploadRequestV1 {
                            schema_version: 1,
                            completion_proof: OpaqueUploadCompletionProof::new("v1.proof").unwrap(),
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            missing_precondition.status(),
            StatusCode::PRECONDITION_REQUIRED
        );

        let complete = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/artifacts/{artifact_id}:complete-upload"))
                    .header("content-type", "application/json")
                    .header(
                        "idempotency-key",
                        "artifact-complete-key-with-at-least-32-bytes",
                    )
                    .header(IF_MATCH, artifact_etag(&artifact_id, 1))
                    .extension(principal.clone())
                    .body(Body::from(
                        serde_json::to_vec(&CompleteArtifactUploadRequestV1 {
                            schema_version: 1,
                            completion_proof: OpaqueUploadCompletionProof::new("v1.proof").unwrap(),
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(complete.status(), StatusCode::ACCEPTED);
        assert_eq!(
            complete.headers().get(LOCATION).unwrap(),
            &format!("/v1/operations/{}", id(ResourceKind::Job, 4))
        );
        assert_eq!(
            complete.headers().get(ETAG).unwrap(),
            &artifact_etag(&artifact_id, 3)
        );

        let duplicate_json = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/artifacts/{artifact_id}:complete-upload"))
                    .header("content-type", "application/json")
                    .header(
                        "idempotency-key",
                        "artifact-duplicate-key-with-at-least-32-bytes",
                    )
                    .header(IF_MATCH, artifact_etag(&artifact_id, 1))
                    .extension(principal.clone())
                    .body(Body::from(
                        r#"{"schema_version":1,"schema_version":1,"completion_proof":"v1.proof"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(duplicate_json.status(), StatusCode::BAD_REQUEST);

        let wrong_content_type = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/artifacts/{artifact_id}:complete-upload"))
                    .header("content-type", "text/plain")
                    .header(
                        "idempotency-key",
                        "artifact-content-type-key-with-at-least-32-bytes",
                    )
                    .header(IF_MATCH, artifact_etag(&artifact_id, 1))
                    .extension(principal.clone())
                    .body(Body::from(
                        r#"{"schema_version":1,"completion_proof":"v1.proof"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_content_type.status(), StatusCode::BAD_REQUEST);

        let delete =
            build_artifact_router(ArtifactHttpState::new(Arc::new(App), Arc::new(Clock(now))))
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/v1/artifacts/{artifact_id}:delete"))
                        .header(
                            "idempotency-key",
                            "artifact-delete-key-with-at-least-32-bytes",
                        )
                        .header(IF_MATCH, artifact_etag(&artifact_id, 3))
                        .extension(AuthenticatedPrincipal {
                            tenant_id: id(ResourceKind::Tenant, 1),
                            principal_id: id(ResourceKind::Principal, 2),
                            principal_kind: PrincipalKind::AgentRunner,
                            permissions: PermissionSet::new(vec![Permission::ArtifactDelete])
                                .unwrap(),
                            authn_strength: AuthnStrength::MultiFactor,
                            principal_version: 1,
                            binding_generation: 1,
                            binding_version: 1,
                            credential_digest: digest('b'),
                            credential_expires_at: now + Duration::hours(1),
                        })
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        assert_eq!(delete.status(), StatusCode::ACCEPTED);
        assert_eq!(
            delete.headers().get(LOCATION).unwrap(),
            &format!("/v1/operations/{}", id(ResourceKind::Job, 6))
        );
    }
}
