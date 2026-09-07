use crate::authentication::AuthenticatedPrincipal;
use crate::product::{
    AuthorityListPage, ListCursorCodec, ListCursorContext, ListError, ListKeysetBoundary,
    ListPageV1, ListRoutePurpose, PRODUCT_LIST_CURSOR_TTL_SECONDS, PRODUCT_LIST_DEFAULT_PAGE_SIZE,
    PRODUCT_LIST_MAX_PAGE_SIZE,
};
use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{rejection::QueryRejection, DefaultBodyLimit, Extension, Path, Query, State},
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
use insight_platform_tasks::{TaskAction, TaskKind, TaskQueryPurpose, TaskState};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const TASK_READ_DEADLINE_MILLISECONDS: i64 = 5_000;
const TASK_COMMAND_DEADLINE_MILLISECONDS: i64 = 10_000;
const MAX_TASK_REQUEST_BYTES: usize = 65_536;
const IDEMPOTENCY_KEY: &str = "idempotency-key";
const IF_MATCH: &str = "if-match";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitTaskInputV1 {
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub value: ValueRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskOwnerLinkV2 {
    Run {
        run_id: ResourceId,
    },
    Invocation {
        invocation_id: ResourceId,
    },
    Artifact {
        artifact_id: ResourceId,
    },
    McpAuthorization {
        authorization_binding_id: ResourceId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskViewV2 {
    pub schema_version: u32,
    pub task_id: ResourceId,
    pub task_kind: TaskKind,
    pub state: TaskState,
    pub generation: u64,
    pub version: u64,
    pub safe_prompt_key: String,
    pub allowed_actions: Vec<TaskAction>,
    pub response_schema_digest: Option<Sha256Digest>,
    pub owner: TaskOwnerLinkV2,
    pub deadline: UtcTimestamp,
    pub responded_at: Option<UtcTimestamp>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub etag: String,
}

impl TaskViewV2 {
    pub fn validate(&self) -> Result<(), TaskApplicationError> {
        if self.schema_version != 2
            || self.task_id.kind() != self.task_kind.task_id_kind()
            || self.generation == 0
            || self.version == 0
            || self.version > insight_platform_contracts::MAX_SAFE_JSON_INTEGER
            || self.generation > insight_platform_contracts::MAX_SAFE_JSON_INTEGER
            || self.safe_prompt_key.is_empty()
            || self.safe_prompt_key.len() > 128
            || self.etag != task_etag(&self.task_id, self.version)
            || !valid_actions(&self.allowed_actions)
            || !match &self.owner {
                TaskOwnerLinkV2::Run { run_id } => run_id.kind() == ResourceKind::Run,
                TaskOwnerLinkV2::Invocation { invocation_id } => {
                    invocation_id.kind() == ResourceKind::CapabilityInvocation
                }
                TaskOwnerLinkV2::Artifact { artifact_id } => {
                    artifact_id.kind() == ResourceKind::Artifact
                }
                TaskOwnerLinkV2::McpAuthorization {
                    authorization_binding_id,
                } => authorization_binding_id.kind() == ResourceKind::McpAuthorizationBinding,
            }
            || self
                .allowed_actions
                .iter()
                .any(|action| action.target(self.task_kind).is_none())
            || (self.state.is_terminal() && !self.allowed_actions.is_empty())
        {
            return Err(TaskApplicationError::Internal);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReadTaskIntent {
    pub purpose: TaskQueryPurpose,
    pub principal: AuthenticatedPrincipal,
    pub task_id: ResourceId,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ResolveTaskIntent {
    pub principal: AuthenticatedPrincipal,
    pub task_id: ResourceId,
    pub expected_task_version: u64,
    pub action: TaskAction,
    pub input: Option<SubmitTaskInputV1>,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskApplicationError {
    FormUnavailable,
    InvalidTaskInput,
    CursorInvalid,
    CursorExpired,
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
    async fn list_tasks(
        &self,
        _intent: ListTasksIntent,
    ) -> Result<AuthorityListPage<TaskViewV2>, TaskApplicationError> {
        Err(TaskApplicationError::Unavailable)
    }
    async fn read_task_form(
        &self,
        _intent: ReadTaskIntent,
    ) -> Result<TaskFormV2, TaskApplicationError> {
        Err(TaskApplicationError::FormUnavailable)
    }

    async fn read_task(&self, intent: ReadTaskIntent) -> Result<TaskViewV2, TaskApplicationError>;
    async fn resolve_task(
        &self,
        _intent: ResolveTaskIntent,
    ) -> Result<TaskViewV2, TaskApplicationError> {
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
    list_cursor_codec: Option<Arc<dyn ListCursorCodec>>,
}

impl TaskHttpState {
    pub fn new(application: Arc<dyn TaskApplication>, clock: Arc<dyn TaskClock>) -> Self {
        Self {
            application,
            clock,
            list_cursor_codec: None,
        }
    }
}

impl TaskHttpState {
    pub fn with_list_cursor_codec(mut self, codec: Arc<dyn ListCursorCodec>) -> Self {
        self.list_cursor_codec = Some(codec);
        self
    }
}

pub fn build_task_router(state: TaskHttpState) -> Router {
    Router::new()
        .route("/v1/tasks", get(list_tasks))
        .route("/v1/tasks/{task_action}", get(read_task).post(resolve_task))
        .route("/v1/tasks/{task_action}/form", get(read_task_form))
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
        (id, TaskAction::SubmitInput)
    } else if let Some(id) = task_action.strip_suffix(":approve") {
        (id, TaskAction::Approve)
    } else if let Some(id) = task_action.strip_suffix(":reject") {
        (id, TaskAction::Reject)
    } else if let Some(id) = task_action.strip_suffix(":cancel") {
        (id, TaskAction::Cancel)
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
    let input = if action == TaskAction::SubmitInput {
        match insight_platform_contracts::parse_strict_json(
            &body,
            insight_platform_contracts::JsonLimits {
                max_bytes: MAX_TASK_REQUEST_BYTES,
                max_depth: 32,
                max_properties_per_object: 1024,
                max_items_per_array: 4096,
                max_string_bytes: MAX_TASK_REQUEST_BYTES,
            },
        )
        .ok()
        .and_then(|value| serde_json::from_value::<SubmitTaskInputV1>(value).ok())
        {
            Some(value) => Some(value),
            None => return problem(TaskApplicationError::InvalidTaskInput),
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
    action: TaskAction,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskReadQuery {
    purpose: Option<TaskQueryPurpose>,
}

fn valid_actions(actions: &[TaskAction]) -> bool {
    actions.len() <= 4
        && actions
            .iter()
            .enumerate()
            .all(|(i, action)| !actions[..i].contains(action))
}

async fn read_task(
    State(state): State<TaskHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(task_id): Path<String>,
    query: Result<Query<TaskReadQuery>, QueryRejection>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(TaskApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(TaskApplicationError::Unauthenticated);
    }
    let Ok(Query(query)) = query else {
        return problem(TaskApplicationError::Invalid);
    };
    let purpose = query.purpose.unwrap_or(TaskQueryPurpose::Respondable);
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
            purpose,
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

fn task_response(view: TaskViewV2) -> Response {
    versioned_task_response(&view, &view.etag)
}

fn versioned_task_response(value: impl Serialize, etag: &str) -> Response {
    let etag = match HeaderValue::from_str(etag) {
        Ok(value) => value,
        Err(_) => return problem(TaskApplicationError::Internal),
    };
    let mut response = no_store_json(value);
    response.headers_mut().insert("etag", etag);
    response
}

pub fn task_etag(task_id: &ResourceId, version: u64) -> String {
    format!("\"{task_id}-{version}\"")
}

fn problem(error: TaskApplicationError) -> Response {
    let (status, code, title, retryable) = match error {
        TaskApplicationError::FormUnavailable => (
            StatusCode::CONFLICT,
            ApiProblemCode::TaskFormUnavailable,
            "This Task has no frozen response form.",
            false,
        ),
        TaskApplicationError::InvalidTaskInput => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::InvalidTaskInput,
            "The response does not satisfy the Task input contract.",
            false,
        ),
        TaskApplicationError::CursorInvalid => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::CursorInvalid,
            "The Task cursor is invalid.",
            false,
        ),
        TaskApplicationError::CursorExpired => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::CursorExpired,
            "The Task cursor has expired.",
            false,
        ),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskInboxFiltersV1 {
    pub purpose: TaskQueryPurpose,
    pub state: Option<TaskState>,
    pub kind: Option<TaskKind>,
    pub run_id: Option<ResourceId>,
}
#[derive(Debug, Clone)]
pub struct ListTasksIntent {
    pub principal: AuthenticatedPrincipal,
    pub filters: TaskInboxFiltersV1,
    pub page_size: u16,
    pub snapshot_at: Option<DateTime<Utc>>,
    pub boundary: Option<ListKeysetBoundary>,
    pub deadline: DateTime<Utc>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskFormV2 {
    pub allowed_actions: Vec<TaskAction>,
    pub etag: String,
    pub safe_prompt_key: String,
    pub schema_version: u32,
    pub task_id: ResourceId,
    pub generation: u64,
    pub version: u64,
    pub response_schema: insight_platform_contracts::InteractionSchemaDocument,
    pub response_schema_digest: Sha256Digest,
}
impl TaskFormV2 {
    pub fn validate(&self) -> Result<(), TaskApplicationError> {
        if self.schema_version != 2
            || self.task_id.kind() != ResourceKind::Interaction
            || self.generation == 0
            || self.version == 0
            || self.version > insight_platform_contracts::MAX_SAFE_JSON_INTEGER
            || self.generation > insight_platform_contracts::MAX_SAFE_JSON_INTEGER
            || !valid_actions(&self.allowed_actions)
            || self.allowed_actions.contains(&TaskAction::Approve)
            || self.etag != task_etag(&self.task_id, self.version)
            || self.safe_prompt_key.is_empty()
            || self.safe_prompt_key.len() > 128
            || self.response_schema.validate().is_err()
            || self.response_schema.canonical_digest != self.response_schema_digest
        {
            return Err(TaskApplicationError::Internal);
        }
        Ok(())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskListQuery {
    purpose: Option<TaskQueryPurpose>,
    state: Option<TaskState>,
    kind: Option<TaskKind>,
    run_id: Option<ResourceId>,
    page_size: Option<u16>,
    cursor: Option<String>,
}
async fn list_tasks(
    State(state): State<TaskHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    query: Result<Query<TaskListQuery>, QueryRejection>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(TaskApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(TaskApplicationError::Unauthenticated);
    }
    let Ok(Query(query)) = query else {
        return problem(TaskApplicationError::Invalid);
    };
    let page_size = query.page_size.unwrap_or(PRODUCT_LIST_DEFAULT_PAGE_SIZE);
    if page_size == 0
        || page_size > PRODUCT_LIST_MAX_PAGE_SIZE
        || query
            .run_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::Run)
    {
        return problem(TaskApplicationError::Invalid);
    }
    let filters = TaskInboxFiltersV1 {
        purpose: query.purpose.unwrap_or(TaskQueryPurpose::Respondable),
        state: query.state,
        kind: query.kind,
        run_id: query.run_id,
    };
    let Some(filter_digest) = serde_json::to_value(&filters)
        .ok()
        .and_then(|value| canonical_digest(&value).ok())
        .and_then(|digest| digest.parse().ok())
    else {
        return problem(TaskApplicationError::Internal);
    };
    let context = ListCursorContext {
        purpose: ListRoutePurpose::Tasks,
        filter_digest,
        page_size,
    };
    let Some(codec) = &state.list_cursor_codec else {
        return problem(TaskApplicationError::Unavailable);
    };
    let now = state.clock.now();
    let (snapshot_at, boundary, expires_at) = match query.cursor {
        Some(cursor) => match codec.decode(&cursor, &principal, &context, now) {
            Ok(decoded) => (
                Some(decoded.snapshot_at),
                Some(decoded.boundary),
                decoded.expires_at,
            ),
            Err(ListError::Invalid) => return problem(TaskApplicationError::CursorInvalid),
            Err(ListError::Expired) => return problem(TaskApplicationError::CursorExpired),
        },
        None => (
            None,
            None,
            now + Duration::seconds(PRODUCT_LIST_CURSOR_TTL_SECONDS),
        ),
    };
    let page = match state
        .application
        .list_tasks(ListTasksIntent {
            principal: principal.clone(),
            filters,
            page_size,
            snapshot_at,
            boundary,
            deadline: now + Duration::milliseconds(TASK_READ_DEADLINE_MILLISECONDS),
        })
        .await
    {
        Ok(page) => page,
        Err(error) => return problem(error),
    };
    if page.items.len() > usize::from(page_size)
        || page.items.iter().any(|item| item.validate().is_err())
        || snapshot_at.is_some_and(|requested| requested != page.snapshot_at)
    {
        return problem(TaskApplicationError::Internal);
    }
    let next_cursor = match page.next_boundary {
        Some(boundary) => {
            match codec.encode(&principal, &context, page.snapshot_at, boundary, expires_at) {
                Ok(cursor) => Some(cursor),
                Err(_) => return problem(TaskApplicationError::Internal),
            }
        }
        None => None,
    };
    match ListPageV1::new(page.items, next_cursor) {
        Ok(page) => no_store_json(page),
        Err(_) => problem(TaskApplicationError::Internal),
    }
}
async fn read_task_form(
    State(state): State<TaskHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(task_id): Path<String>,
    query: Result<Query<TaskReadQuery>, QueryRejection>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(TaskApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(TaskApplicationError::Unauthenticated);
    }
    let Ok(Query(query)) = query else {
        return problem(TaskApplicationError::Invalid);
    };
    let purpose = query.purpose.unwrap_or(TaskQueryPurpose::Respondable);
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
    if purpose != TaskQueryPurpose::Respondable {
        return problem(TaskApplicationError::Invalid);
    }
    match state
        .application
        .read_task_form(ReadTaskIntent {
            purpose: TaskQueryPurpose::Respondable,
            principal,
            task_id,
            deadline: state.clock.now() + Duration::milliseconds(TASK_READ_DEADLINE_MILLISECONDS),
        })
        .await
    {
        Ok(form) if form.validate().is_ok() => versioned_task_response(&form, &form.etag),
        Ok(_) => problem(TaskApplicationError::Internal),
        Err(error) => problem(error),
    }
}
fn no_store_json(value: impl Serialize) -> Response {
    let mut response = (StatusCode::OK, Json(value)).into_response();
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
        ) -> Result<TaskViewV2, TaskApplicationError> {
            let now = UtcTimestamp::from_datetime(Utc::now());
            Ok(TaskViewV2 {
                schema_version: 2,
                allowed_actions: Vec::new(),
                task_id: intent.task_id.clone(),
                task_kind: TaskKind::InteractionForm,
                state: TaskState::Pending,
                generation: 1,
                version: 2,
                safe_prompt_key: "collect_profile".to_owned(),
                response_schema_digest: None,
                owner: TaskOwnerLinkV2::Run {
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
        ) -> Result<TaskViewV2, TaskApplicationError> {
            let now = UtcTimestamp::from_datetime(Utc::now());
            Ok(TaskViewV2 {
                schema_version: 2,
                allowed_actions: Vec::new(),
                task_id: intent.task_id.clone(),
                task_kind: TaskKind::InteractionForm,
                state: TaskState::Responded,
                generation: 2,
                version: 2,
                safe_prompt_key: "collect_profile".to_owned(),
                response_schema_digest: None,
                owner: TaskOwnerLinkV2::Run {
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
