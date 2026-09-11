//! Authenticated read-only authoring queries. POST resolution is classified by
//! this closed route, never by a caller-controlled mutation exemption.
use crate::{
    authentication::AuthenticatedPrincipal,
    product::*,
    task::{SystemTaskClock, TaskClock},
};
use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{rejection::QueryRejection, DefaultBodyLimit, Extension, Query, State},
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ApiProblem, ApiProblemCode, DependencySlotKind,
    JsonLimits, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_registry::authoring::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DiscoverAuthoringIntent {
    pub principal: AuthenticatedPrincipal,
    pub filters: AuthoringDependencyFiltersV1,
    pub page_size: u16,
    pub snapshot_at: Option<DateTime<Utc>>,
    pub boundary: Option<ListKeysetBoundary>,
    pub deadline: DateTime<Utc>,
}
#[derive(Debug, Clone)]
pub struct ResolveAuthoringIntent {
    pub principal: AuthenticatedPrincipal,
    pub request: ResolveAgentBindingsRequestV1,
    pub deadline: DateTime<Utc>,
}
#[async_trait]
pub trait AuthoringApplication: Send + Sync {
    async fn discover(
        &self,
        intent: DiscoverAuthoringIntent,
    ) -> Result<AuthorityListPage<AuthoringDependencyV1>, AuthoringQueryError>;
    async fn resolve(
        &self,
        intent: ResolveAuthoringIntent,
    ) -> Result<ResolveAgentBindingsResponseV1, AuthoringQueryError>;
}
#[derive(Clone)]
pub struct AuthoringHttpState {
    application: Arc<dyn AuthoringApplication>,
    codec: Arc<dyn ListCursorCodec>,
    clock: Arc<dyn TaskClock>,
}
impl AuthoringHttpState {
    pub fn new(
        application: Arc<dyn AuthoringApplication>,
        codec: Arc<dyn ListCursorCodec>,
    ) -> Self {
        Self {
            application,
            codec,
            clock: Arc::new(SystemTaskClock),
        }
    }
    pub fn with_clock(mut self, clock: Arc<dyn TaskClock>) -> Self {
        self.clock = clock;
        self
    }
}
pub fn build_authoring_router(state: AuthoringHttpState) -> Router {
    Router::new()
        .route(
            AuthoringQueryOperation::DiscoverDependencies.path(),
            get(discover),
        )
        .route(
            AuthoringQueryOperation::ResolveBindings.path(),
            post(resolve),
        )
        .layer(DefaultBodyLimit::max(MAX_AUTHORING_QUERY_BYTES))
        .with_state(state)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryQuery {
    kind: DependencySlotKind,
    environment: Option<String>,
    interface_contract_digest: Option<Sha256Digest>,
    page_size: Option<u16>,
    cursor: Option<String>,
}
async fn discover(
    State(state): State<AuthoringHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    query: Result<Query<DiscoveryQuery>, QueryRejection>,
) -> Response {
    let Some(Extension(principal)) = principal.filter(|p| p.0.validate().is_ok()) else {
        return problem(StatusCode::UNAUTHORIZED, ApiProblemCode::Unauthenticated);
    };
    let Ok(Query(query)) = query else {
        return query_problem(AuthoringQueryError::Invalid);
    };
    let filters = AuthoringDependencyFiltersV1 {
        kind: query.kind,
        environment: query.environment,
        interface_contract_digest: query.interface_contract_digest,
    };
    let page_size = query.page_size.unwrap_or(PRODUCT_LIST_DEFAULT_PAGE_SIZE);
    if filters.validate().is_err() || page_size == 0 || page_size > PRODUCT_LIST_MAX_PAGE_SIZE {
        return query_problem(AuthoringQueryError::Invalid);
    }
    let filter_digest = match serde_json::to_value(&filters)
        .ok()
        .and_then(|v| canonical_digest(&v).ok())
        .and_then(|v| v.parse().ok())
    {
        Some(v) => v,
        None => return query_problem(AuthoringQueryError::Unavailable),
    };
    let context = ListCursorContext {
        purpose: ListRoutePurpose::AuthoringDependencies,
        filter_digest,
        page_size,
    };
    let now = state.clock.now();
    let (snapshot_at, boundary, expires_at) = match query.cursor {
        Some(cursor) => match state.codec.decode(&cursor, &principal, &context, now) {
            Ok(value) => (
                Some(value.snapshot_at),
                Some(value.boundary),
                value.expires_at,
            ),
            Err(error) => {
                return problem(
                    StatusCode::BAD_REQUEST,
                    match error {
                        ListError::Expired => ApiProblemCode::CursorExpired,
                        ListError::Invalid => ApiProblemCode::CursorInvalid,
                    },
                )
            }
        },
        None => (
            None,
            None,
            now + Duration::seconds(PRODUCT_LIST_CURSOR_TTL_SECONDS),
        ),
    };
    let page = match state
        .application
        .discover(DiscoverAuthoringIntent {
            principal: principal.clone(),
            filters: filters.clone(),
            page_size,
            snapshot_at,
            boundary,
            deadline: now + Duration::seconds(5),
        })
        .await
    {
        Ok(page) => page,
        Err(error) => return query_problem(error),
    };
    if page.items.len() > usize::from(page_size)
        || page
            .items
            .iter()
            .any(|item| item.validate_for(&filters).is_err())
        || snapshot_at.is_some_and(|requested| requested != page.snapshot_at)
    {
        return query_problem(AuthoringQueryError::Unavailable);
    }
    let next_cursor = match page.next_boundary {
        Some(boundary) => {
            match state
                .codec
                .encode(&principal, &context, page.snapshot_at, boundary, expires_at)
            {
                Ok(cursor) => Some(cursor),
                Err(_) => return query_problem(AuthoringQueryError::Unavailable),
            }
        }
        None => None,
    };
    match ListPageV1::new(page.items, next_cursor) {
        Ok(page) => no_store(page),
        Err(_) => query_problem(AuthoringQueryError::Unavailable),
    }
}
async fn resolve(
    State(state): State<AuthoringHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    body: Bytes,
) -> Response {
    let Some(Extension(principal)) = principal.filter(|p| p.0.validate().is_ok()) else {
        return problem(StatusCode::UNAUTHORIZED, ApiProblemCode::Unauthenticated);
    };
    let request = match parse_strict_json(
        &body,
        JsonLimits {
            max_bytes: MAX_AUTHORING_QUERY_BYTES,
            max_depth: 32,
            max_properties_per_object: 128,
            max_items_per_array: 64,
            max_string_bytes: 4096,
        },
    )
    .ok()
    .and_then(|v| serde_json::from_value::<ResolveAgentBindingsRequestV1>(v).ok())
    {
        Some(request) if request.validate().is_ok() => request,
        _ => return query_problem(AuthoringQueryError::Invalid),
    };
    match state
        .application
        .resolve(ResolveAuthoringIntent {
            principal,
            request: request.clone(),
            deadline: state.clock.now() + Duration::seconds(5),
        })
        .await
    {
        Ok(response) if response.validate_for(&request).is_ok() => no_store(response),
        Ok(_) => query_problem(AuthoringQueryError::Unavailable),
        Err(error) => query_problem(error),
    }
}
fn no_store(value: impl Serialize) -> Response {
    let mut response = Json(value).into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
}
fn query_problem(error: AuthoringQueryError) -> Response {
    let (status, code) = match error {
        AuthoringQueryError::Invalid => (StatusCode::BAD_REQUEST, ApiProblemCode::InvalidRequest),
        AuthoringQueryError::Denied => (StatusCode::FORBIDDEN, ApiProblemCode::PermissionDenied),
        AuthoringQueryError::NotFound | AuthoringQueryError::DefaultNotConfigured => {
            (StatusCode::NOT_FOUND, ApiProblemCode::ResourceNotFound)
        }
        AuthoringQueryError::Disabled | AuthoringQueryError::ContractMismatch => {
            (StatusCode::CONFLICT, ApiProblemCode::InvalidStateTransition)
        }
        AuthoringQueryError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            ApiProblemCode::TemporarilyUnavailable,
        ),
    };
    problem(status, code)
}
fn problem(status: StatusCode, code: ApiProblemCode) -> Response {
    let retryable = status == StatusCode::SERVICE_UNAVAILABLE;
    let body = ApiProblem {
        type_uri: format!("https://insight.platform/problems/{}", code.as_str()),
        title: "The authoring query could not be completed.".to_owned(),
        status: status.as_u16(),
        code,
        detail: None,
        request_id: ResourceId::from_uuid_v7(ResourceKind::ServerRequest, uuid::Uuid::now_v7())
            .expect("request ID"),
        trace_id: crate::trace::current_trace_id(),
        retryable,
        retry_after_ms: retryable.then_some(1000),
        field_errors: Vec::new(),
    };
    let mut response = (status, Json(body)).into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
}
