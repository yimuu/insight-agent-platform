//! Lossy, authorized text observations. Durable history and final values retain their own ports.
use crate::{
    authentication::AuthenticatedPrincipal,
    run::{problem, RunApplicationError},
};
use async_trait::async_trait;
use axum::{
    extract::{Extension, Path, State},
    http::{HeaderMap, HeaderValue},
    response::{sse::Event, IntoResponse, Response, Sse},
    routing::get,
    Router,
};
use futures::{stream::BoxStream, StreamExt};
use insight_platform_contracts::{ResourceId, ResourceKind};
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc};

pub const LIVE_TEXT_MAX_FRAME_BYTES: usize = 65_536;
pub const LIVE_TEXT_MAX_PENDING_MESSAGES: usize = 128;
pub const LIVE_TEXT_MAX_PENDING_BYTES: usize = 1_048_576;
pub const LIVE_TEXT_MAX_CONNECTIONS: usize = 16;
pub const LIVE_TEXT_TRANSPORT_PENDING_MESSAGES: usize = 8;
pub const LIVE_TEXT_MAX_PRINCIPAL_CONNECTIONS: usize = 4;
pub const LIVE_TEXT_MAX_SECONDS: u64 = 300;
pub const LIVE_TEXT_AUTH_INTERVAL_MILLISECONDS: u64 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveTextFrameV1 {
    pub schema_version: u32,
    pub run_id: ResourceId,
    #[serde(flatten)]
    pub body: LiveTextBodyV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LiveTextBodyV1 {
    Opened {
        partial: bool,
    },
    Reset {
        model_turn_id: ResourceId,
        attempt_no: u32,
    },
    Text {
        model_turn_id: ResourceId,
        attempt_no: u32,
        text_sequence: u64,
        text: String,
    },
    Gap {
        reason: LiveTextGapReason,
    },
    Closed {
        reason: LiveTextCloseReason,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveTextGapReason {
    LateSubscription,
    SequenceGap,
    TransportReconnected,
    SlowConsumer,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveTextCloseReason {
    Terminal,
    Cancelled,
    Expired,
    AuthorizationChanged,
    Unavailable,
    DurationLimit,
}
impl LiveTextBodyV1 {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Opened { .. } => "opened",
            Self::Reset { .. } => "reset",
            Self::Text { .. } => "text",
            Self::Gap { .. } => "gap",
            Self::Closed { .. } => "closed",
        }
    }
}
pub type LiveTextStream = BoxStream<'static, LiveTextFrameV1>;
#[async_trait]
pub trait RunLiveApplication: Send + Sync {
    async fn open(
        &self,
        principal: AuthenticatedPrincipal,
        run_id: ResourceId,
    ) -> Result<LiveTextStream, RunApplicationError>;
}
pub fn build_run_live_router(application: Arc<dyn RunLiveApplication>) -> Router {
    Router::new()
        .route("/v1/runs/{run_id}/live-text", get(open))
        .with_state(application)
}
async fn open(
    State(application): State<Arc<dyn RunLiveApplication>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Path(run): Path<String>,
    headers: HeaderMap,
) -> Response {
    if headers.contains_key("last-event-id") {
        return problem(RunApplicationError::Invalid);
    }
    let Ok(run) = ResourceId::parse_expected(&run, ResourceKind::Run) else {
        return problem(RunApplicationError::Invalid);
    };
    let stream = match application.open(principal, run).await {
        Ok(stream) => stream,
        Err(error) => return problem(error),
    };
    let mut response = Sse::new(stream.map(|frame| {
        Ok::<_, Infallible>(
            Event::default()
                .event(frame.body.kind())
                .json_data(frame)
                .expect("typed text frame"),
        )
    }))
    .keep_alive(axum::response::sse::KeepAlive::new().interval(std::time::Duration::from_secs(10)))
    .into_response();
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn text_frame_is_explicitly_live_and_utf8_safe() {
        let frame = LiveTextFrameV1 {
            schema_version: 1,
            run_id: "run_0198f1cc-32e4-75e1-a9e8-d95ca0f80001".parse().unwrap(),
            body: LiveTextBodyV1::Text {
                model_turn_id: "mturn_0198f1cc-32e4-75e1-a9e8-d95ca0f80002"
                    .parse()
                    .unwrap(),
                attempt_no: 2,
                text_sequence: 4,
                text: "你好\n🌱".into(),
            },
        };
        let value = serde_json::to_value(&frame).unwrap();
        assert_eq!(value["kind"], "text");
        for private in [
            "event_id",
            "cursor",
            "job_id",
            "worker_process_generation_id",
            "lease_generation",
            "request_digest",
            "transport_sequence",
        ] {
            assert!(value.get(private).is_none());
        }
        assert_eq!(
            serde_json::from_value::<LiveTextFrameV1>(value).unwrap(),
            frame
        );
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use insight_platform_contracts::{AuthnStrength, Permission, PermissionSet, PrincipalKind};
    use tower::ServiceExt;
    struct Delayed;
    #[async_trait]
    impl RunLiveApplication for Delayed {
        async fn open(
            &self,
            _principal: AuthenticatedPrincipal,
            run_id: ResourceId,
        ) -> Result<LiveTextStream, RunApplicationError> {
            Ok(futures::stream::iter([LiveTextFrameV1 {
                schema_version: 1,
                run_id,
                body: LiveTextBodyV1::Opened { partial: true },
            }])
            .chain(futures::stream::pending())
            .boxed())
        }
    }
    fn principal() -> AuthenticatedPrincipal {
        let id = |kind: ResourceKind| {
            format!(
                "{}_0198f1cc-32e4-75e1-a9e8-d95ca0f80001",
                kind.descriptor().prefix
            )
            .parse()
            .unwrap()
        };
        AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant),
            principal_id: id(ResourceKind::Principal),
            principal_kind: PrincipalKind::AgentRunner,
            permissions: PermissionSet::new(vec![
                Permission::RuntimeRead,
                Permission::ArtifactRead,
            ])
            .unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            credential_expires_at: chrono::Utc::now() + chrono::Duration::minutes(5),
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
        }
    }
    #[tokio::test]
    async fn first_frame_is_not_buffered_until_stream_closes_and_has_no_replay_id() {
        let app = build_run_live_router(Arc::new(Delayed)).layer(Extension(principal()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/runs/run_0198f1cc-32e4-75e1-a9e8-d95ca0f80001/live-text")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["cache-control"], "no-store");
        let mut body = response.into_body().into_data_stream();
        let first = tokio::time::timeout(std::time::Duration::from_millis(100), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let text = std::str::from_utf8(&first).unwrap();
        assert!(text.contains("event: opened"));
        assert!(!text.contains("\nid:"));
    }
    #[tokio::test]
    async fn durable_cursor_cannot_be_used_for_lossy_text() {
        let app = build_run_live_router(Arc::new(Delayed)).layer(Extension(principal()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/runs/run_0198f1cc-32e4-75e1-a9e8-d95ca0f80001/live-text")
                    .header("last-event-id", "durable-cursor")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
