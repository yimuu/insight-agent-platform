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
use insight_platform_contracts::{
    ApiProblem, ApiProblemCode, ResourceId, ResourceKind, RunState, UtcTimestamp, MAX_FIELD_ERRORS,
    MAX_SAFE_TEXT_BYTES,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const RUN_READ_DEADLINE_MILLISECONDS: i64 = 5_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewV1 {
    pub schema_version: u32,
    pub run_id: ResourceId,
    pub agent_deployment_id: ResourceId,
    pub state: RunState,
    pub version: u64,
    pub input_value_id: ResourceId,
    pub output_value_id: Option<ResourceId>,
    pub pause_generation: u64,
    pub cancel_generation: u64,
    pub deadline: UtcTimestamp,
    pub started_at: Option<UtcTimestamp>,
    pub terminal_at: Option<UtcTimestamp>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub etag: String,
}

impl RunViewV1 {
    pub fn validate(&self) -> Result<(), RunApplicationError> {
        if self.schema_version != 1
            || self.run_id.kind() != ResourceKind::Run
            || self.agent_deployment_id.kind() != ResourceKind::AgentDeployment
            || self.input_value_id.kind() != ResourceKind::RunValue
            || self
                .output_value_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::RunValue)
            || self.version == 0
            || self.etag != run_etag(&self.run_id, self.version)
        {
            return Err(RunApplicationError::Internal);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReadRunIntent {
    pub principal: AuthenticatedPrincipal,
    pub run_id: ResourceId,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunApplicationError {
    Unauthenticated,
    Denied,
    NotFound,
    Unavailable,
    Internal,
}

#[async_trait]
pub trait RunApplication: Send + Sync {
    async fn read_run(&self, intent: ReadRunIntent) -> Result<RunViewV1, RunApplicationError>;
}

pub trait RunClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemRunClock;

impl RunClock for SystemRunClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone)]
pub struct RunHttpState {
    application: Arc<dyn RunApplication>,
    clock: Arc<dyn RunClock>,
}

impl RunHttpState {
    pub fn new(application: Arc<dyn RunApplication>, clock: Arc<dyn RunClock>) -> Self {
        Self { application, clock }
    }
}

pub fn build_run_router(state: RunHttpState) -> Router {
    Router::new()
        .route("/v1/runs/{run_id}", get(read_run))
        .with_state(state)
}

async fn read_run(
    State(state): State<RunHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(run_id): Path<String>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(RunApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(RunApplicationError::Unauthenticated);
    }
    let run_id = match run_id.parse::<ResourceId>() {
        Ok(run_id) if run_id.kind() == ResourceKind::Run => run_id,
        _ => return problem(RunApplicationError::NotFound),
    };
    let intent = ReadRunIntent {
        principal,
        run_id,
        deadline: state.clock.now() + Duration::milliseconds(RUN_READ_DEADLINE_MILLISECONDS),
    };
    match state.application.read_run(intent).await {
        Ok(view) if view.validate().is_ok() => run_response(view),
        Ok(_) => problem(RunApplicationError::Internal),
        Err(error) => problem(error),
    }
}

fn run_response(view: RunViewV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(etag) => etag,
        Err(_) => return problem(RunApplicationError::Internal),
    };
    let mut response = (StatusCode::OK, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
}

pub fn run_etag(run_id: &ResourceId, version: u64) -> String {
    format!("\"{run_id}-{version}\"")
}

fn problem(error: RunApplicationError) -> Response {
    let (status, code, title, retryable) = match error {
        RunApplicationError::Unauthenticated => (
            StatusCode::UNAUTHORIZED,
            ApiProblemCode::Unauthenticated,
            "Authentication is required.",
            false,
        ),
        RunApplicationError::Denied => (
            StatusCode::FORBIDDEN,
            ApiProblemCode::PermissionDenied,
            "The Run operation is not permitted.",
            false,
        ),
        RunApplicationError::NotFound => (
            StatusCode::NOT_FOUND,
            ApiProblemCode::ResourceNotFound,
            "The Run was not found.",
            false,
        ),
        RunApplicationError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            ApiProblemCode::TemporarilyUnavailable,
            "The Run authority is temporarily unavailable.",
            true,
        ),
        RunApplicationError::Internal => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiProblemCode::InternalError,
            "The Run response could not be projected.",
            false,
        ),
    };
    let request_id = ResourceId::from_uuid_v7(ResourceKind::ServerRequest, uuid::Uuid::now_v7())
        .expect("UUID v7 generator must produce a valid server request identity");
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use insight_platform_contracts::{
        AuthnStrength, Permission, PermissionSet, PrincipalKind, Sha256Digest,
    };
    use tower::ServiceExt;

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

    fn principal(now: DateTime<Utc>) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant, 1),
            principal_id: id(ResourceKind::Principal, 2),
            principal_kind: PrincipalKind::AgentRunner,
            permissions: PermissionSet::new(vec![Permission::RuntimeRead]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: digest('a'),
            credential_expires_at: now + Duration::hours(1),
        }
    }

    struct FixedClock(DateTime<Utc>);

    impl RunClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    struct FixtureApplication;

    #[async_trait]
    impl RunApplication for FixtureApplication {
        async fn read_run(&self, intent: ReadRunIntent) -> Result<RunViewV1, RunApplicationError> {
            let now = UtcTimestamp::from_datetime(Utc::now());
            Ok(RunViewV1 {
                schema_version: 1,
                run_id: intent.run_id.clone(),
                agent_deployment_id: id(ResourceKind::AgentDeployment, 4),
                state: RunState::Queued,
                version: 3,
                input_value_id: id(ResourceKind::RunValue, 5),
                output_value_id: None,
                pause_generation: 0,
                cancel_generation: 0,
                deadline: now.clone(),
                started_at: None,
                terminal_at: None,
                created_at: now.clone(),
                updated_at: now,
                etag: run_etag(&intent.run_id, 3),
            })
        }
    }

    #[tokio::test]
    async fn run_read_requires_nominal_identity_and_returns_current_etag() {
        let now = Utc::now();
        let router = build_run_router(RunHttpState::new(
            Arc::new(FixtureApplication),
            Arc::new(FixedClock(now)),
        ));
        let run_id = id(ResourceKind::Run, 3);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/runs/{run_id}"))
                    .extension(principal(now))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["etag"], run_etag(&run_id, 3));
        assert_eq!(
            response.headers()[CACHE_CONTROL],
            "no-store, private, max-age=0"
        );

        let wrong_kind = router
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/runs/{}", id(ResourceKind::Job, 6)))
                    .extension(principal(now))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_kind.status(), StatusCode::NOT_FOUND);
    }
}
