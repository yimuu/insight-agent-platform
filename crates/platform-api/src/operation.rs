use async_trait::async_trait;
use axum::{
    extract::{Extension, Path, State},
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, ApiProblem, ApiProblemCode, OperationViewV1, PrincipalKind, ReadOperation,
    ResourceId, ResourceKind, Sha256Digest, MAX_FIELD_ERRORS, MAX_SAFE_TEXT_BYTES,
};
use serde::Serialize;
use std::sync::Arc;

pub use crate::authentication::AuthenticatedPrincipal;

pub const OPERATION_PATH: &str = "/v1/operations/{operation_id}";
const OPERATION_READ_DEADLINE_MILLISECONDS: i64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationApplicationError {
    Invalid,
    Unauthenticated,
    Denied,
    NotFound,
    NotPublic,
    Unavailable,
    Internal,
}

#[async_trait]
pub trait OperationApplication: Send + Sync {
    async fn read_operation(
        &self,
        request: ReadOperation,
    ) -> Result<OperationViewV1, OperationApplicationError>;
}

pub trait OperationClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemOperationClock;

impl OperationClock for SystemOperationClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone)]
pub struct OperationHttpState {
    application: Arc<dyn OperationApplication>,
    clock: Arc<dyn OperationClock>,
}

impl OperationHttpState {
    pub fn new(application: Arc<dyn OperationApplication>, clock: Arc<dyn OperationClock>) -> Self {
        Self { application, clock }
    }
}

pub fn build_operation_router(state: OperationHttpState) -> Router {
    Router::new()
        .route(OPERATION_PATH, get(read_operation))
        .with_state(state)
}

async fn read_operation(
    State(state): State<OperationHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(operation_id): Path<String>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(OperationApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(OperationApplicationError::Unauthenticated);
    }
    let operation_id = match ResourceId::parse_expected(&operation_id, ResourceKind::Job) {
        Ok(operation_id) => operation_id,
        Err(_) => return problem(OperationApplicationError::NotFound),
    };
    let now = state.clock.now();
    let request_digest = match operation_request_digest(&principal, &operation_id) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let request = ReadOperation {
        tenant_id: principal.tenant_id,
        principal_id: principal.principal_id,
        principal_kind: principal.principal_kind,
        operation_id,
        request_digest,
        deadline: now + Duration::milliseconds(OPERATION_READ_DEADLINE_MILLISECONDS),
    };
    match state.application.read_operation(request).await {
        Ok(view) if view.validate().is_ok() => {
            let etag = match HeaderValue::from_str(&view.etag) {
                Ok(etag) => etag,
                Err(_) => return problem(OperationApplicationError::Internal),
            };
            let mut response = (StatusCode::OK, Json(view)).into_response();
            response.headers_mut().insert("etag", etag);
            no_store(response)
        }
        Ok(_) => problem(OperationApplicationError::Internal),
        Err(error) => problem(error),
    }
}

#[derive(Serialize)]
struct OperationRequestIdentity<'a> {
    schema_version: u32,
    operation: &'static str,
    tenant_id: &'a ResourceId,
    principal_id: &'a ResourceId,
    principal_kind: PrincipalKind,
    credential_digest: &'a Sha256Digest,
    operation_id: &'a ResourceId,
}

fn operation_request_digest(
    principal: &AuthenticatedPrincipal,
    operation_id: &ResourceId,
) -> Result<Sha256Digest, OperationApplicationError> {
    let value = serde_json::to_value(OperationRequestIdentity {
        schema_version: 1,
        operation: "operation.read",
        tenant_id: &principal.tenant_id,
        principal_id: &principal.principal_id,
        principal_kind: principal.principal_kind,
        credential_digest: &principal.credential_digest,
        operation_id,
    })
    .map_err(|_| OperationApplicationError::Internal)?;
    canonical_digest(&value)
        .map_err(|_| OperationApplicationError::Internal)?
        .parse()
        .map_err(|_| OperationApplicationError::Internal)
}

fn problem(error: OperationApplicationError) -> Response {
    let (status, code, title, retry_after) = match error {
        OperationApplicationError::Invalid => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::InvalidRequest,
            "The request is invalid.",
            false,
        ),
        OperationApplicationError::Unauthenticated => (
            StatusCode::UNAUTHORIZED,
            ApiProblemCode::Unauthenticated,
            "Authentication is required.",
            false,
        ),
        OperationApplicationError::Denied => (
            StatusCode::FORBIDDEN,
            ApiProblemCode::PermissionDenied,
            "The operation is not permitted.",
            false,
        ),
        OperationApplicationError::NotFound | OperationApplicationError::NotPublic => (
            StatusCode::NOT_FOUND,
            ApiProblemCode::ResourceNotFound,
            "The operation was not found.",
            false,
        ),
        OperationApplicationError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            ApiProblemCode::TemporarilyUnavailable,
            "The operation service is temporarily unavailable.",
            true,
        ),
        OperationApplicationError::Internal => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiProblemCode::InternalError,
            "The operation could not be projected safely.",
            false,
        ),
    };
    let request_id = ResourceId::from_uuid_v7(ResourceKind::ServerRequest, uuid::Uuid::now_v7())
        .expect("UUID v7 generator must produce a valid server request identity");
    let body = ApiProblem {
        type_uri: format!("https://insight.platform/problems/{}", code.as_str()),
        title: title.to_owned(),
        status: status.as_u16(),
        code,
        detail: None,
        request_id,
        retryable: retry_after,
        retry_after_ms: retry_after.then_some(1_000),
        field_errors: Vec::new(),
    };
    debug_assert!(body.validate(MAX_SAFE_TEXT_BYTES, MAX_FIELD_ERRORS).is_ok());
    let body = Json(body);
    let mut response = (status, body).into_response();
    if retry_after {
        response
            .headers_mut()
            .insert("retry-after", HeaderValue::from_static("1"));
    }
    no_store(response)
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, http::Request};
    use insight_platform_contracts::{
        operation_etag, AuthnStrength, Permission, PermissionSet, PublicJobKind, PublicJobState,
        PublicJobTarget, UtcTimestamp,
    };
    use std::sync::Mutex;
    use tower::ServiceExt;

    struct FixedClock(DateTime<Utc>);

    impl OperationClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    struct FixtureApplication {
        requests: Mutex<Vec<ReadOperation>>,
        result: Result<OperationViewV1, OperationApplicationError>,
    }

    #[async_trait]
    impl OperationApplication for FixtureApplication {
        async fn read_operation(
            &self,
            request: ReadOperation,
        ) -> Result<OperationViewV1, OperationApplicationError> {
            self.requests.lock().unwrap().push(request);
            self.result.clone()
        }
    }

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1cc-32e4-75e1-a9e8-d95ca0f8{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn fixture() -> (
        Router,
        Arc<FixtureApplication>,
        AuthenticatedPrincipal,
        ResourceId,
    ) {
        let now = Utc::now();
        let operation_id = id(ResourceKind::Job, 4);
        let view = OperationViewV1 {
            operation_id: operation_id.clone(),
            tenant_id: id(ResourceKind::Tenant, 1),
            kind: PublicJobKind::ArtifactVerify,
            target: PublicJobTarget::Artifact {
                artifact_id: id(ResourceKind::Artifact, 5),
            },
            state: PublicJobState::Running,
            progress: None,
            result: None,
            error: None,
            created_at: UtcTimestamp::from_datetime(now),
            updated_at: UtcTimestamp::from_datetime(now),
            etag: operation_etag(&operation_id.to_string(), 2),
        };
        let application = Arc::new(FixtureApplication {
            requests: Mutex::new(Vec::new()),
            result: Ok(view),
        });
        let router = build_operation_router(OperationHttpState::new(
            application.clone(),
            Arc::new(FixedClock(now)),
        ));
        let principal = AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant, 1),
            principal_id: id(ResourceKind::Principal, 2),
            principal_kind: PrincipalKind::TenantAdmin,
            permissions: PermissionSet::new(vec![Permission::OperationRead]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: digest('a'),
            credential_expires_at: now + Duration::hours(1),
        };
        (router, application, principal, operation_id)
    }

    #[tokio::test]
    async fn operation_identity_comes_only_from_authenticated_extension() {
        let (router, application, principal, operation_id) = fixture();
        let response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/operations/{operation_id}"))
                    .header(
                        "x-platform-tenant-id",
                        id(ResourceKind::Tenant, 99).to_string(),
                    )
                    .extension(principal.clone())
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        let requests = application.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].tenant_id, principal.tenant_id);
        assert_eq!(requests[0].operation_id, operation_id);
    }

    #[tokio::test]
    async fn missing_authentication_and_wrong_id_kind_fail_before_authority() {
        let (router, application, principal, _) = fixture();
        let unauthenticated = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/operations/{}", id(ResourceKind::Job, 4)))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

        let wrong_kind = router
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/operations/{}", id(ResourceKind::Artifact, 4)))
                    .extension(principal)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_kind.status(), StatusCode::NOT_FOUND);
        assert!(application.requests.lock().unwrap().is_empty());
        let body = to_bytes(wrong_kind.into_body(), 2_048).await.unwrap();
        assert!(!body.is_empty());
    }
}
