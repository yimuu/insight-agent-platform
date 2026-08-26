use axum::{
    body::Body,
    extract::Request,
    http::{HeaderName, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use insight_platform_contracts::{
    ApiProblem, ApiProblemCode, ResourceId, ResourceKind, SpanId, TraceFlags, TraceId,
    TraceIdentityV1, W3cTraceParent, MAX_FIELD_ERRORS, MAX_SAFE_TEXT_BYTES,
};

pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const TRACESTATE_HEADER: &str = "tracestate";
pub const BAGGAGE_HEADER: &str = "baggage";
pub const TRACE_ID_HEADER: &str = "trace-id";

tokio::task_local! {
    static ACTIVE_PUBLIC_TRACE: PublicTraceContext;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicTraceContext {
    pub identity: TraceIdentityV1,
    pub gateway_span_id: SpanId,
    pub flags: TraceFlags,
}

impl PublicTraceContext {
    pub fn generate() -> Self {
        Self {
            identity: TraceIdentityV1::generate(),
            gateway_span_id: SpanId::new(),
            flags: TraceFlags::NotSampled,
        }
    }

    pub fn from_parent(parent: W3cTraceParent) -> Self {
        Self {
            identity: TraceIdentityV1::new(parent.trace_id),
            gateway_span_id: SpanId::new(),
            flags: parent.flags,
        }
    }

    pub const fn outbound_parent(self) -> W3cTraceParent {
        W3cTraceParent::new(self.identity.trace_id, self.gateway_span_id, self.flags)
    }
}

pub fn current_trace_context() -> PublicTraceContext {
    ACTIVE_PUBLIC_TRACE
        .try_with(|context| *context)
        .unwrap_or_else(|_| PublicTraceContext::generate())
}

pub fn current_trace_id() -> TraceId {
    current_trace_context().identity.trace_id
}

pub async fn establish_public_trace(mut request: Request, next: Next) -> Response {
    let context = match parse_public_trace(request.headers()) {
        Ok(context) => context,
        Err(()) => {
            let context = PublicTraceContext::generate();
            return ACTIVE_PUBLIC_TRACE
                .scope(context, async move {
                    with_trace_header(invalid_trace_problem(), context.identity.trace_id)
                })
                .await;
        }
    };
    request.extensions_mut().insert(context);
    ACTIVE_PUBLIC_TRACE
        .scope(context, async move {
            let response = next.run(request).await;
            with_trace_header(response, context.identity.trace_id)
        })
        .await
}

fn parse_public_trace(headers: &axum::http::HeaderMap) -> Result<PublicTraceContext, ()> {
    if headers.contains_key(TRACESTATE_HEADER) || headers.contains_key(BAGGAGE_HEADER) {
        return Err(());
    }
    let mut parents = headers.get_all(TRACEPARENT_HEADER).iter();
    let Some(parent) = parents.next() else {
        return Ok(PublicTraceContext::generate());
    };
    if parents.next().is_some() {
        return Err(());
    }
    let parent = parent
        .to_str()
        .map_err(|_| ())?
        .parse::<W3cTraceParent>()
        .map_err(|_| ())?;
    Ok(PublicTraceContext::from_parent(parent))
}

fn invalid_trace_problem() -> Response {
    let status = StatusCode::BAD_REQUEST;
    let code = ApiProblemCode::InvalidRequest;
    let request_id = ResourceId::from_uuid_v7(ResourceKind::ServerRequest, uuid::Uuid::now_v7())
        .expect("UUID v7 generator must produce a valid server request identity");
    let problem = ApiProblem {
        type_uri: format!("https://insight.platform/problems/{}", code.as_str()),
        title: "The trace context is invalid.".to_owned(),
        status: status.as_u16(),
        code,
        detail: None,
        request_id,
        trace_id: current_trace_id(),
        retryable: false,
        retry_after_ms: None,
        field_errors: Vec::new(),
    };
    debug_assert!(problem
        .validate(MAX_SAFE_TEXT_BYTES, MAX_FIELD_ERRORS)
        .is_ok());
    let mut response = (status, Json(problem)).into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    response
}

fn with_trace_header(mut response: Response, trace_id: TraceId) -> Response {
    let header_name = HeaderName::from_static(TRACE_ID_HEADER);
    let header_value = HeaderValue::from_str(&trace_id.to_string())
        .expect("canonical trace ID is always a valid header value");
    response.headers_mut().insert(header_name, header_value);
    response
}

pub fn trace_context(request: &axum::http::Request<Body>) -> Option<&PublicTraceContext> {
    request.extensions().get::<PublicTraceContext>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    fn traced_router() -> Router {
        Router::new()
            .route("/v1/test", get(|| async { StatusCode::NO_CONTENT }))
            .route("/v1/problem", get(|| async { invalid_trace_problem() }))
            .layer(axum::middleware::from_fn(establish_public_trace))
    }

    #[tokio::test]
    async fn accepts_exact_parent_and_returns_its_trace_id() {
        let trace_id = "0af7651916cd43dd8448eb211c80319c";
        let response = traced_router()
            .oneshot(
                Request::builder()
                    .uri("/v1/test")
                    .header(
                        TRACEPARENT_HEADER,
                        format!("00-{trace_id}-b7ad6b7169203331-01"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers()[TRACE_ID_HEADER], trace_id);
    }

    #[tokio::test]
    async fn generates_a_trace_when_parent_is_absent() {
        let response = traced_router()
            .oneshot(
                Request::builder()
                    .uri("/v1/test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(response.headers()[TRACE_ID_HEADER]
            .to_str()
            .unwrap()
            .parse::<TraceId>()
            .is_ok());
    }

    #[tokio::test]
    async fn problem_body_and_response_header_share_the_request_trace() {
        let trace_id = "0af7651916cd43dd8448eb211c80319c";
        let response = traced_router()
            .oneshot(
                Request::builder()
                    .uri("/v1/problem")
                    .header(
                        TRACEPARENT_HEADER,
                        format!("00-{trace_id}-b7ad6b7169203331-00"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()[TRACE_ID_HEADER], trace_id);
        let body = axum::body::to_bytes(response.into_body(), 4_096)
            .await
            .unwrap();
        let problem = serde_json::from_slice::<ApiProblem>(&body).unwrap();
        assert_eq!(problem.trace_id.to_string(), trace_id);
    }

    #[tokio::test]
    async fn rejects_malformed_parent_and_forbidden_context_headers() {
        for (name, value) in [
            (TRACEPARENT_HEADER, "00-bad-parent"),
            (TRACESTATE_HEADER, "vendor=value"),
            (BAGGAGE_HEADER, "tenant_id=forged"),
        ] {
            let response = traced_router()
                .oneshot(
                    Request::builder()
                        .uri("/v1/test")
                        .header(name, value)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(response.headers()[TRACE_ID_HEADER]
                .to_str()
                .unwrap()
                .parse::<TraceId>()
                .is_ok());
        }
    }
}
