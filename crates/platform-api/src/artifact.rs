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
use std::sync::Arc;

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
