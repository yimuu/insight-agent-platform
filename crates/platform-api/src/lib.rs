//! HTTP transport adapters for the clean-cut Platform v1 candidate.
//!
//! This crate owns protocol parsing and response hardening only. Domain state and callback
//! first-winner authority remain behind typed application ports.

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{
        header::{
            ALLOW, CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, PRAGMA, REFERRER_POLICY,
            RETRY_AFTER, X_CONTENT_TYPE_OPTIONS,
        },
        HeaderValue, Method, Request, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use chrono::{DateTime, Utc};
use insight_platform_mcp_host::{
    McpOAuthCallbackError, McpOAuthCallbackIngress, McpOAuthCallbackIngressOutcome,
    MAX_MCP_OAUTH_CALLBACK_QUERY_BYTES,
};
use std::sync::Arc;

pub mod authentication;
pub mod oidc;
pub mod operation;
pub mod resource;

pub const MCP_OAUTH_CALLBACK_PATH: &str = "/v1/mcp/oauth/callback";

const CALLBACK_COMPLETE_BODY: &str =
    "MCP authorization response received. You may close this window.";
const CALLBACK_REJECTED_BODY: &str = "The MCP authorization response could not be accepted.";
const CALLBACK_UNAVAILABLE_BODY: &str = "The MCP authorization service is temporarily unavailable.";
const CALLBACK_PENDING_BODY: &str = "The MCP authorization response is being processed.";
const CALLBACK_INTERNAL_BODY: &str = "The MCP authorization response could not be processed.";

#[async_trait]
pub trait McpOAuthCallbackApplication: Send + Sync {
    async fn handle_callback_query(
        &self,
        raw_query: &[u8],
        now: DateTime<Utc>,
    ) -> Result<McpOAuthCallbackIngressOutcome, McpOAuthCallbackError>;
}

#[async_trait]
impl McpOAuthCallbackApplication for McpOAuthCallbackIngress {
    async fn handle_callback_query(
        &self,
        raw_query: &[u8],
        now: DateTime<Utc>,
    ) -> Result<McpOAuthCallbackIngressOutcome, McpOAuthCallbackError> {
        self.handle_query(raw_query, now).await
    }
}

pub trait McpOAuthCallbackClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemMcpOAuthCallbackClock;

impl McpOAuthCallbackClock for SystemMcpOAuthCallbackClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone)]
pub struct McpOAuthCallbackHttpState {
    application: Arc<dyn McpOAuthCallbackApplication>,
    clock: Arc<dyn McpOAuthCallbackClock>,
}

impl McpOAuthCallbackHttpState {
    pub fn new(
        application: Arc<dyn McpOAuthCallbackApplication>,
        clock: Arc<dyn McpOAuthCallbackClock>,
    ) -> Self {
        Self { application, clock }
    }
}

pub fn build_mcp_oauth_callback_router(state: McpOAuthCallbackHttpState) -> Router {
    Router::new()
        .route(MCP_OAUTH_CALLBACK_PATH, any(handle_mcp_oauth_callback))
        .with_state(state)
}

async fn handle_mcp_oauth_callback(
    State(state): State<McpOAuthCallbackHttpState>,
    request: Request<Body>,
) -> Response {
    let (parts, body) = request.into_parts();
    if parts.method != Method::GET {
        let mut response = callback_response(
            StatusCode::METHOD_NOT_ALLOWED,
            CALLBACK_REJECTED_BODY,
            false,
        );
        response
            .headers_mut()
            .insert(ALLOW, HeaderValue::from_static("GET"));
        return response;
    }
    let Some(query) = parts.uri.query() else {
        return callback_response(StatusCode::BAD_REQUEST, CALLBACK_REJECTED_BODY, false);
    };
    if query.is_empty() || query.len() > MAX_MCP_OAUTH_CALLBACK_QUERY_BYTES {
        return callback_response(StatusCode::BAD_REQUEST, CALLBACK_REJECTED_BODY, false);
    }
    if to_bytes(body, 0).await.is_err() {
        return callback_response(StatusCode::BAD_REQUEST, CALLBACK_REJECTED_BODY, false);
    }
    match state
        .application
        .handle_callback_query(query.as_bytes(), state.clock.now())
        .await
    {
        Ok(outcome) if outcome.no_store => {
            callback_response(StatusCode::OK, CALLBACK_COMPLETE_BODY, false)
        }
        Ok(_) => callback_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            CALLBACK_INTERNAL_BODY,
            false,
        ),
        Err(McpOAuthCallbackError::Rejected(_)) => {
            callback_response(StatusCode::BAD_REQUEST, CALLBACK_REJECTED_BODY, false)
        }
        Err(McpOAuthCallbackError::TemporarilyUnavailable(_)) => callback_response(
            StatusCode::SERVICE_UNAVAILABLE,
            CALLBACK_UNAVAILABLE_BODY,
            true,
        ),
        Err(McpOAuthCallbackError::CommitUncertain(_)) => {
            callback_response(StatusCode::ACCEPTED, CALLBACK_PENDING_BODY, false)
        }
    }
}

fn callback_response(status: StatusCode, body: &'static str, retryable: bool) -> Response {
    let mut response = (status, body).into_response();
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(
        CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'none'; frame-ancestors 'none'; base-uri 'none'"),
    );
    if retryable {
        headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{header::ALLOW, Method};
    use insight_platform_mcp_host::McpOAuthCallbackCommitDisposition;
    use std::sync::Mutex;
    use tower::ServiceExt;

    struct FixedClock(DateTime<Utc>);

    impl McpOAuthCallbackClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    struct FixtureApplication {
        calls: Mutex<Vec<Vec<u8>>>,
        outcome: Result<McpOAuthCallbackIngressOutcome, McpOAuthCallbackError>,
    }

    #[async_trait]
    impl McpOAuthCallbackApplication for FixtureApplication {
        async fn handle_callback_query(
            &self,
            raw_query: &[u8],
            _now: DateTime<Utc>,
        ) -> Result<McpOAuthCallbackIngressOutcome, McpOAuthCallbackError> {
            self.calls.lock().unwrap().push(raw_query.to_vec());
            self.outcome
        }
    }

    fn state(
        outcome: Result<McpOAuthCallbackIngressOutcome, McpOAuthCallbackError>,
    ) -> (McpOAuthCallbackHttpState, Arc<FixtureApplication>) {
        let application = Arc::new(FixtureApplication {
            calls: Mutex::new(Vec::new()),
            outcome,
        });
        (
            McpOAuthCallbackHttpState::new(application.clone(), Arc::new(FixedClock(Utc::now()))),
            application,
        )
    }

    #[tokio::test]
    async fn callback_forwards_only_the_bounded_raw_query_and_returns_static_no_store_content() {
        let (state, application) = state(Ok(McpOAuthCallbackIngressOutcome {
            disposition: McpOAuthCallbackCommitDisposition::Authorized,
            no_store: true,
        }));
        let response = build_mcp_oauth_callback_router(state)
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "{MCP_OAUTH_CALLBACK_PATH}?state=secret-state&code=secret-code"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        assert_eq!(response.headers()[REFERRER_POLICY], "no-referrer");
        let body = to_bytes(response.into_body(), 1_024).await.unwrap();
        assert!(!body.windows(12).any(|window| window == b"secret-state"));
        assert!(!body.windows(11).any(|window| window == b"secret-code"));
        assert_eq!(
            application.calls.lock().unwrap().as_slice(),
            [b"state=secret-state&code=secret-code".to_vec()]
        );
    }

    #[tokio::test]
    async fn callback_rejects_a_body_and_non_get_method_before_application_dispatch() {
        let (state, application) = state(Ok(McpOAuthCallbackIngressOutcome {
            disposition: McpOAuthCallbackCommitDisposition::Authorized,
            no_store: true,
        }));
        let router = build_mcp_oauth_callback_router(state);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("{MCP_OAUTH_CALLBACK_PATH}?state=a&code=b"))
                    .body(Body::from("unexpected"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(application.calls.lock().unwrap().is_empty());

        for method in [Method::POST, Method::HEAD] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(format!("{MCP_OAUTH_CALLBACK_PATH}?state=a&code=b"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
            assert_eq!(response.headers()[ALLOW], "GET");
        }
        assert!(application.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn callback_rejects_an_oversized_query_before_application_dispatch() {
        let (state, application) = state(Ok(McpOAuthCallbackIngressOutcome {
            disposition: McpOAuthCallbackCommitDisposition::Authorized,
            no_store: true,
        }));
        let response = build_mcp_oauth_callback_router(state)
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "{MCP_OAUTH_CALLBACK_PATH}?state={}&code=b",
                        "a".repeat(MAX_MCP_OAUTH_CALLBACK_QUERY_BYTES)
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(application.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn callback_maps_safe_failure_classes_without_reflecting_internal_codes() {
        for (error, expected_status, retry_after) in [
            (
                McpOAuthCallbackError::Rejected("secret_rejected_detail"),
                StatusCode::BAD_REQUEST,
                false,
            ),
            (
                McpOAuthCallbackError::TemporarilyUnavailable("secret_dependency_detail"),
                StatusCode::SERVICE_UNAVAILABLE,
                true,
            ),
            (
                McpOAuthCallbackError::CommitUncertain("secret_uncertain_detail"),
                StatusCode::ACCEPTED,
                false,
            ),
        ] {
            let (state, _) = state(Err(error));
            let response = build_mcp_oauth_callback_router(state)
                .oneshot(
                    Request::builder()
                        .uri(format!("{MCP_OAUTH_CALLBACK_PATH}?state=a&code=b"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected_status);
            assert_eq!(response.headers().contains_key(RETRY_AFTER), retry_after);
            let body = to_bytes(response.into_body(), 1_024).await.unwrap();
            assert!(!body.windows(6).any(|window| window == b"secret"));
        }
    }
}
