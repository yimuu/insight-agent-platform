use crate::authentication::AuthenticatedPrincipal;
use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::{header::CACHE_CONTROL, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, ApiProblem, ApiProblemCode, DataClassification, ResourceId, ResourceKind,
    Sha256Digest, UtcTimestamp, ValueRef, MAX_FIELD_ERRORS, MAX_SAFE_TEXT_BYTES,
};
use insight_platform_tasks::{TaskKind, TaskState};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const TASK_READ_DEADLINE_MILLISECONDS: i64 = 5_000;
const TASK_COMMAND_DEADLINE_MILLISECONDS: i64 = 10_000;
const MAX_TASK_REQUEST_BYTES: usize = 65_536;
const IDEMPOTENCY_KEY: &str = "idempotency-key";
const IF_MATCH: &str = "if-match";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskActionV1 {
    SubmitInput,
    Approve,
    Reject,
    Cancel,
}

impl TaskActionV1 {
    pub const fn operation(self) -> &'static str {
        match self {
            Self::SubmitInput => "task.submit_input",
            Self::Approve => "task.approve",
            Self::Reject => "task.reject",
            Self::Cancel => "task.cancel",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitTaskInputV1 {
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub value: ValueRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskOwnerLinkV1 {
    Run { run_id: ResourceId },
    Invocation { invocation_id: ResourceId },
    Artifact { artifact_id: ResourceId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskViewV1 {
    pub schema_version: u32,
    pub task_id: ResourceId,
    pub task_kind: TaskKind,
    pub state: TaskState,
    pub generation: u64,
    pub version: u64,
    pub safe_prompt_key: String,
    pub response_schema_digest: Option<Sha256Digest>,
    pub owner: TaskOwnerLinkV1,
    pub deadline: UtcTimestamp,
    pub responded_at: Option<UtcTimestamp>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub etag: String,
}

impl TaskViewV1 {
    pub fn validate(&self) -> Result<(), TaskApplicationError> {
        if self.schema_version != 1
            || self.task_id.kind() != self.task_kind.task_id_kind()
            || self.generation == 0
            || self.version == 0
            || self.safe_prompt_key.is_empty()
            || self.safe_prompt_key.len() > 128
            || self.etag != task_etag(&self.task_id, self.version)
        {
            return Err(TaskApplicationError::Internal);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReadTaskIntent {
    pub principal: AuthenticatedPrincipal,
    pub task_id: ResourceId,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ResolveTaskIntent {
    pub principal: AuthenticatedPrincipal,
    pub task_id: ResourceId,
    pub expected_task_version: u64,
    pub action: TaskActionV1,
    pub input: Option<SubmitTaskInputV1>,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskApplicationError {
    Unauthenticated,
    Invalid,
    Denied,
    NotFound,
    Conflict,
    IdempotencyConflict,
    Unavailable,
    Internal,
}

#[async_trait]
pub trait TaskApplication: Send + Sync {
    async fn read_task(&self, intent: ReadTaskIntent) -> Result<TaskViewV1, TaskApplicationError>;
    async fn resolve_task(
        &self,
        _intent: ResolveTaskIntent,
    ) -> Result<TaskViewV1, TaskApplicationError> {
        Err(TaskApplicationError::Internal)
    }
}

pub trait TaskClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemTaskClock;

impl TaskClock for SystemTaskClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone)]
pub struct TaskHttpState {
    application: Arc<dyn TaskApplication>,
    clock: Arc<dyn TaskClock>,
}

impl TaskHttpState {
    pub fn new(application: Arc<dyn TaskApplication>, clock: Arc<dyn TaskClock>) -> Self {
        Self { application, clock }
    }
}

pub fn build_task_router(state: TaskHttpState) -> Router {
    Router::new()
        .route("/v1/tasks/{task_action}", get(read_task).post(resolve_task))
        .layer(DefaultBodyLimit::max(MAX_TASK_REQUEST_BYTES))
        .with_state(state)
}

async fn resolve_task(
    State(state): State<TaskHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(task_action): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(TaskApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(TaskApplicationError::Unauthenticated);
    }
    let (task_id, action) = if let Some(id) = task_action.strip_suffix(":submit-input") {
        (id, TaskActionV1::SubmitInput)
    } else if let Some(id) = task_action.strip_suffix(":approve") {
        (id, TaskActionV1::Approve)
    } else if let Some(id) = task_action.strip_suffix(":reject") {
        (id, TaskActionV1::Reject)
    } else if let Some(id) = task_action.strip_suffix(":cancel") {
        (id, TaskActionV1::Cancel)
    } else {
        return problem(TaskApplicationError::NotFound);
    };
    let task_id = match task_id.parse::<ResourceId>() {
        Ok(id)
            if matches!(
                id.kind(),
                ResourceKind::Interaction | ResourceKind::ApprovalTask
            ) =>
        {
            id
        }
        _ => return problem(TaskApplicationError::NotFound),
    };
    let input = if action == TaskActionV1::SubmitInput {
        match serde_json::from_slice::<SubmitTaskInputV1>(&body) {
            Ok(value) => Some(value),
            Err(_) => return problem(TaskApplicationError::Invalid),
        }
    } else if body.is_empty() {
        None
    } else {
        return problem(TaskApplicationError::Invalid);
    };
    let expected_task_version = match expected_task_version(&headers, &task_id) {
        Ok(value) => value,
        Err(error) => return problem(error),
    };
    let idempotency_key_digest =
        match task_idempotency_digest(&headers, &principal, &task_id, action) {
            Ok(value) => value,
            Err(error) => return problem(error),
        };
    let request_digest = match canonical_digest(&serde_json::json!({
        "action": action.operation(),
        "expected_task_version": expected_task_version,
        "idempotency_key_digest": idempotency_key_digest,
        "input": input,
        "principal_id": principal.principal_id,
        "schema_version": 1,
        "task_id": task_id,
        "tenant_id": principal.tenant_id,
    }))
    .ok()
    .and_then(|value| value.parse().ok())
    {
        Some(value) => value,
        None => return problem(TaskApplicationError::Invalid),
    };
    match state
        .application
        .resolve_task(ResolveTaskIntent {
            principal,
            task_id,
            expected_task_version,
            action,
            input,
            idempotency_key_digest,
            request_digest,
            deadline: state.clock.now()
                + Duration::milliseconds(TASK_COMMAND_DEADLINE_MILLISECONDS),
        })
        .await
    {
        Ok(view) if view.validate().is_ok() => task_response(view),
        Ok(_) => problem(TaskApplicationError::Internal),
        Err(error) => problem(error),
    }
}

fn expected_task_version(
    headers: &HeaderMap,
    task_id: &ResourceId,
) -> Result<u64, TaskApplicationError> {
    let mut values = headers.get_all(IF_MATCH).iter();
    let value = values.next().ok_or(TaskApplicationError::Invalid)?;
    if values.next().is_some() {
        return Err(TaskApplicationError::Invalid);
    }
    let value = value.to_str().map_err(|_| TaskApplicationError::Invalid)?;
    let prefix = format!("\"{task_id}-");
    let version = value
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix('"'))
        .ok_or(TaskApplicationError::Invalid)?;
    if version.is_empty() || version.starts_with('0') {
        return Err(TaskApplicationError::Invalid);
    }
    version.parse().map_err(|_| TaskApplicationError::Invalid)
}

fn task_idempotency_digest(
    headers: &HeaderMap,
    principal: &AuthenticatedPrincipal,
    task_id: &ResourceId,
    action: TaskActionV1,
) -> Result<Sha256Digest, TaskApplicationError> {
    let mut values = headers.get_all(IDEMPOTENCY_KEY).iter();
    let value = values.next().ok_or(TaskApplicationError::Invalid)?;
    if values.next().is_some() {
        return Err(TaskApplicationError::Invalid);
    }
    let key = value.to_str().map_err(|_| TaskApplicationError::Invalid)?;
    if key.is_empty()
        || key.len() > 255
        || !key.is_ascii()
        || key.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(TaskApplicationError::Invalid);
    }
    canonical_digest(&serde_json::json!({
        "key": key,
        "operation": action.operation(),
        "principal_id": principal.principal_id,
        "schema_version": 1,
        "task_id": task_id,
        "tenant_id": principal.tenant_id,
    }))
    .map_err(|_| TaskApplicationError::Invalid)?
    .parse()
    .map_err(|_| TaskApplicationError::Invalid)
}

async fn read_task(
    State(state): State<TaskHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(task_id): Path<String>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(TaskApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(TaskApplicationError::Unauthenticated);
    }
    let task_id = match task_id.parse::<ResourceId>() {
        Ok(id)
            if matches!(
                id.kind(),
                ResourceKind::Interaction | ResourceKind::ApprovalTask
            ) =>
        {
            id
        }
        _ => return problem(TaskApplicationError::NotFound),
    };
    match state
        .application
        .read_task(ReadTaskIntent {
            principal,
            task_id,
            deadline: state.clock.now() + Duration::milliseconds(TASK_READ_DEADLINE_MILLISECONDS),
        })
        .await
    {
        Ok(view) if view.validate().is_ok() => task_response(view),
        Ok(_) => problem(TaskApplicationError::Internal),
        Err(error) => problem(error),
    }
}

fn task_response(view: TaskViewV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(value) => value,
        Err(_) => return problem(TaskApplicationError::Internal),
    };
    let mut response = (StatusCode::OK, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
}

pub fn task_etag(task_id: &ResourceId, version: u64) -> String {
    format!("\"{task_id}-{version}\"")
}

fn problem(error: TaskApplicationError) -> Response {
    let (status, code, title, retryable) = match error {
        TaskApplicationError::Unauthenticated => (
            StatusCode::UNAUTHORIZED,
            ApiProblemCode::Unauthenticated,
            "Authentication is required.",
            false,
        ),
        TaskApplicationError::Invalid => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::InvalidRequest,
            "The Task request is invalid.",
            false,
        ),
        TaskApplicationError::Denied => (
            StatusCode::FORBIDDEN,
            ApiProblemCode::PermissionDenied,
            "The Task is not available to this principal.",
            false,
        ),
        TaskApplicationError::NotFound => (
            StatusCode::NOT_FOUND,
            ApiProblemCode::ResourceNotFound,
            "The Task was not found.",
            false,
        ),
        TaskApplicationError::Conflict => (
            StatusCode::CONFLICT,
            ApiProblemCode::InvalidStateTransition,
            "The Task changed or is no longer pending.",
            false,
        ),
        TaskApplicationError::IdempotencyConflict => (
            StatusCode::CONFLICT,
            ApiProblemCode::IdempotencyConflict,
            "The idempotency key was used for a different Task request.",
            false,
        ),
        TaskApplicationError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            ApiProblemCode::TemporarilyUnavailable,
            "The Task authority is temporarily unavailable.",
            true,
        ),
        TaskApplicationError::Internal => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiProblemCode::InternalError,
            "The Task response could not be projected.",
            false,
        ),
    };
    let request_id = ResourceId::from_uuid_v7(ResourceKind::ServerRequest, uuid::Uuid::now_v7())
        .expect("UUID v7 generator must produce a request ID");
    let problem = ApiProblem {
        type_uri: format!("https://insight.platform/problems/{}", code.as_str()),
        title: title.to_owned(),
        status: status.as_u16(),
        code,
        detail: None,
        request_id,
        trace_id: crate::trace::current_trace_id(),
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
    use insight_platform_contracts::{AuthnStrength, Permission, PermissionSet, PrincipalKind};
    use tower::ServiceExt;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1cc-32e4-75e1-a9e8-d95ca0f8{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn principal(now: DateTime<Utc>) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant, 1),
            principal_id: id(ResourceKind::Principal, 2),
            principal_kind: PrincipalKind::AgentRunner,
            permissions: PermissionSet::new(vec![Permission::InteractionRespond]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            credential_expires_at: now + Duration::hours(1),
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
        }
    }

    struct FixedClock(DateTime<Utc>);
    impl TaskClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    struct Fixture;
    #[async_trait]
    impl TaskApplication for Fixture {
        async fn read_task(
            &self,
            intent: ReadTaskIntent,
        ) -> Result<TaskViewV1, TaskApplicationError> {
            let now = UtcTimestamp::from_datetime(Utc::now());
            Ok(TaskViewV1 {
                schema_version: 1,
                task_id: intent.task_id.clone(),
                task_kind: TaskKind::InteractionForm,
                state: TaskState::Pending,
                generation: 1,
                version: 2,
                safe_prompt_key: "collect_profile".to_owned(),
                response_schema_digest: None,
                owner: TaskOwnerLinkV1::Run {
                    run_id: id(ResourceKind::Run, 4),
                },
                deadline: now.clone(),
                responded_at: None,
                created_at: now.clone(),
                updated_at: now,
                etag: task_etag(&intent.task_id, 2),
            })
        }

        async fn resolve_task(
            &self,
            intent: ResolveTaskIntent,
        ) -> Result<TaskViewV1, TaskApplicationError> {
            let now = UtcTimestamp::from_datetime(Utc::now());
            Ok(TaskViewV1 {
                schema_version: 1,
                task_id: intent.task_id.clone(),
                task_kind: TaskKind::InteractionForm,
                state: TaskState::Responded,
                generation: 2,
                version: 2,
                safe_prompt_key: "collect_profile".to_owned(),
                response_schema_digest: None,
                owner: TaskOwnerLinkV1::Run {
                    run_id: id(ResourceKind::Run, 4),
                },
                deadline: now.clone(),
                responded_at: Some(now.clone()),
                created_at: now.clone(),
                updated_at: now,
                etag: task_etag(&intent.task_id, 2),
            })
        }
    }

    #[tokio::test]
    async fn task_read_is_nominal_authorized_and_no_store() {
        let now = Utc::now();
        let task_id = id(ResourceKind::Interaction, 3);
        let response = build_task_router(TaskHttpState::new(
            Arc::new(Fixture),
            Arc::new(FixedClock(now)),
        ))
        .oneshot(
            Request::builder()
                .uri(format!("/v1/tasks/{task_id}"))
                .extension(principal(now))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("etag").unwrap(),
            &task_etag(&task_id, 2)
        );
    }

    #[tokio::test]
    async fn task_submit_requires_strong_etag_scoped_idempotency_and_closed_input() {
        let now = Utc::now();
        let task_id = id(ResourceKind::Interaction, 3);
        let body = serde_json::json!({
            "classification": "internal",
            "schema_digest": format!("sha256:{}", "b".repeat(64)),
            "value": {"kind": "inline", "value": {"name": "Ada"}}
        });
        let response = build_task_router(TaskHttpState::new(
            Arc::new(Fixture),
            Arc::new(FixedClock(now)),
        ))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/tasks/{task_id}:submit-input"))
                .header("if-match", task_etag(&task_id, 1))
                .header("idempotency-key", "task-submit-1")
                .extension(principal(now))
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("etag").unwrap(),
            &task_etag(&task_id, 2)
        );
    }
}
