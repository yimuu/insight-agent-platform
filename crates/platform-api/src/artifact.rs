use crate::authentication::AuthenticatedPrincipal;
use async_trait::async_trait;
use axum::{
    extract::{Extension, Path, State},
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_artifacts::ArtifactRecord;
use insight_platform_contracts::{
    ApiProblem, ApiProblemCode, ArtifactRef, ArtifactState, ResourceId, ResourceKind, UtcTimestamp,
    MAX_FIELD_ERRORS, MAX_SAFE_TEXT_BYTES,
};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

const MAX_PUBLIC_ARTIFACT_BYTES: u64 = 1_073_741_824;
const MAX_MEDIA_TYPE_BYTES: usize = 255;
const MAX_DISPLAY_NAME_BYTES: usize = 512;
const MAX_UPLOAD_URL_BYTES: usize = 8_192;
const MAX_COMPLETION_PROOF_BYTES: usize = 4_096;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactApplicationError {
    Unauthenticated,
    Invalid,
    Denied,
    NotFound,
    Unavailable,
    Internal,
}

#[async_trait]
pub trait ArtifactApplication: Send + Sync {
    async fn read_artifact(
        &self,
        intent: ReadArtifactIntent,
    ) -> Result<ArtifactViewV1, ArtifactApplicationError>;
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
        .route("/v1/artifacts/{artifact_id}", get(read_artifact))
        .with_state(state)
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
            deadline: state.clock.now() + Duration::seconds(5),
        })
        .await
    {
        Ok(view) if view.validate().is_ok() => artifact_response(view),
        Ok(_) => problem(ArtifactApplicationError::Internal),
        Err(error) => problem(error),
    }
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
    }
}
