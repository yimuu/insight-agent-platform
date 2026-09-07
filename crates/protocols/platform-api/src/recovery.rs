//! Authenticated management commands over existing Run holds and Task cleanup authority.
use crate::{
    authentication::AuthenticatedPrincipal,
    run::{RunApplicationError, RunClock},
};
use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind, Sha256Digest,
    UtcTimestamp,
};
use insight_platform_orchestrator::history::RunHistoryHoldOutcome;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const MAX_RECOVERY_REQUEST_BYTES: usize = 4096;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaceHistoryHoldRequestV1 {
    pub schema_version: u32,
    pub run_id: ResourceId,
    pub expected_run_version: u64,
    pub reason_evidence_digest: Sha256Digest,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseHistoryHoldRequestV1 {
    pub schema_version: u32,
    pub run_id: ResourceId,
    pub expected_run_version: u64,
    pub hold_key: Sha256Digest,
    pub release_evidence_digest: Sha256Digest,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverPkceCleanupRequestV1 {
    pub schema_version: u32,
    pub task_id: ResourceId,
    pub expected_task_version: u64,
    pub expected_task_generation: u64,
    pub previous_job_id: ResourceId,
    pub attempt_limit: u32,
    pub recovery_evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone)]
pub enum RecoveryRequest {
    Place(PlaceHistoryHoldRequestV1),
    Release(ReleaseHistoryHoldRequestV1),
    Recover(RecoverPkceCleanupRequestV1),
}
impl RecoveryRequest {
    pub fn operation(&self) -> &'static str {
        match self {
            Self::Place(_) => "run.history_hold.place",
            Self::Release(_) => "run.history_hold.release",
            Self::Recover(_) => "mcp.pkce.cleanup.recover",
        }
    }
    pub fn target(&self) -> &ResourceId {
        match self {
            Self::Place(request) => &request.run_id,
            Self::Release(request) => &request.run_id,
            Self::Recover(request) => &request.task_id,
        }
    }
    pub fn validate(&self) -> Result<(), RunApplicationError> {
        let (schema, version, kind) = match self {
            Self::Place(request) => (
                request.schema_version,
                request.expected_run_version,
                ResourceKind::Run,
            ),
            Self::Release(request) => (
                request.schema_version,
                request.expected_run_version,
                ResourceKind::Run,
            ),
            Self::Recover(request) => {
                if request.previous_job_id.kind() != ResourceKind::Job
                    || request.expected_task_generation == 0
                    || request.expected_task_generation
                        > insight_platform_contracts::MAX_SAFE_JSON_INTEGER
                    || !(1..=insight_platform_mcp_host::MCP_OAUTH_CLEANUP_ATTEMPT_LIMIT)
                        .contains(&request.attempt_limit)
                {
                    return Err(RunApplicationError::Invalid);
                }
                (
                    request.schema_version,
                    request.expected_task_version,
                    ResourceKind::Interaction,
                )
            }
        };
        if schema != 1
            || version == 0
            || version > insight_platform_contracts::MAX_SAFE_JSON_INTEGER
            || self.target().kind() != kind
        {
            return Err(RunApplicationError::Invalid);
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct RecoveryIntent {
    pub principal: AuthenticatedPrincipal,
    pub request: RecoveryRequest,
    pub idempotency_key_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicHistoryHoldV1 {
    pub reason_evidence_digest: Sha256Digest,
    pub placed_by: ResourceId,
    pub placed_at: UtcTimestamp,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicHistoryHoldsV1 {
    pub schema_version: u32,
    pub holds: std::collections::BTreeMap<Sha256Digest, PublicHistoryHoldV1>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicHistoryHoldOutcomeV1 {
    pub run_id: ResourceId,
    pub run_version: u64,
    pub holds: PublicHistoryHoldsV1,
}
impl From<RunHistoryHoldOutcome> for PublicHistoryHoldOutcomeV1 {
    fn from(value: RunHistoryHoldOutcome) -> Self {
        Self {
            run_id: value.run_id,
            run_version: value.run_version,
            holds: PublicHistoryHoldsV1 {
                schema_version: value.holds.schema_version,
                holds: value
                    .holds
                    .holds
                    .into_iter()
                    .map(|(key, hold)| {
                        (
                            key,
                            PublicHistoryHoldV1 {
                                reason_evidence_digest: hold.reason_evidence_digest,
                                placed_by: hold.placed_by,
                                placed_at: UtcTimestamp::from_datetime(hold.placed_at),
                            },
                        )
                    })
                    .collect(),
            },
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecoveryResultV1 {
    /// The hold key is stable across retry; the Run projection reflects current authority.
    HistoryHold {
        schema_version: u32,
        hold_key: Sha256Digest,
        current: PublicHistoryHoldOutcomeV1,
    },
    PkceCleanup {
        schema_version: u32,
        task_id: ResourceId,
        cleanup_job_id: ResourceId,
    },
}
impl RecoveryResultV1 {
    pub fn validate_for(
        &self,
        request: &RecoveryRequest,
        now: DateTime<Utc>,
    ) -> Result<(), RunApplicationError> {
        if let (Self::HistoryHold { hold_key, .. }, RecoveryRequest::Release(request)) =
            (self, request)
        {
            if hold_key != &request.hold_key {
                return Err(RunApplicationError::Internal);
            }
        }
        match (self, request) {
            (
                Self::HistoryHold {
                    schema_version: 1,
                    current,
                    ..
                },
                RecoveryRequest::Place(_) | RecoveryRequest::Release(_),
            ) if &current.run_id == request.target()
                && current.run_version > 0
                && current.run_version <= insight_platform_contracts::MAX_SAFE_JSON_INTEGER
                && current.holds.schema_version == 1
                && current.holds.holds.len()
                    <= insight_platform_orchestrator::history::MAX_RUN_HISTORY_HOLDS
                && current.holds.holds.values().all(|hold| {
                    hold.placed_by.kind() == ResourceKind::Principal
                        && hold.placed_at.as_str() <= UtcTimestamp::from_datetime(now).as_str()
                }) =>
            {
                Ok(())
            }
            (
                Self::PkceCleanup {
                    schema_version: 1,
                    task_id,
                    cleanup_job_id,
                },
                RecoveryRequest::Recover(request),
            ) if task_id == &request.task_id
                && cleanup_job_id.kind() == ResourceKind::Job
                && cleanup_job_id != &request.previous_job_id =>
            {
                Ok(())
            }
            _ => Err(RunApplicationError::Internal),
        }
    }
}
#[async_trait]
pub trait RecoveryApplication: Send + Sync {
    async fn execute(
        &self,
        intent: RecoveryIntent,
    ) -> Result<RecoveryResultV1, RunApplicationError>;
}
#[derive(Clone)]
pub struct RecoveryHttpState {
    application: Arc<dyn RecoveryApplication>,
    clock: Arc<dyn RunClock>,
}
impl RecoveryHttpState {
    pub fn new(application: Arc<dyn RecoveryApplication>, clock: Arc<dyn RunClock>) -> Self {
        Self { application, clock }
    }
}
pub fn build_recovery_router(state: RecoveryHttpState) -> Router {
    Router::new()
        .route("/v1/recovery/{action}", post(execute))
        .layer(DefaultBodyLimit::max(MAX_RECOVERY_REQUEST_BYTES))
        .with_state(state)
}
pub fn parse_recovery_request(
    action: &str,
    body: &[u8],
) -> Result<RecoveryRequest, RunApplicationError> {
    let value = parse_strict_json(
        body,
        JsonLimits {
            max_bytes: MAX_RECOVERY_REQUEST_BYTES,
            max_depth: 4,
            max_properties_per_object: 16,
            max_items_per_array: 1,
            max_string_bytes: 255,
        },
    )
    .map_err(|_| RunApplicationError::Invalid)?;
    let request = match action {
        "run-history-holds:place" => RecoveryRequest::Place(
            serde_json::from_value(value).map_err(|_| RunApplicationError::Invalid)?,
        ),
        "run-history-holds:release" => RecoveryRequest::Release(
            serde_json::from_value(value).map_err(|_| RunApplicationError::Invalid)?,
        ),
        "mcp-pkce-cleanup:recover" => RecoveryRequest::Recover(
            serde_json::from_value(value).map_err(|_| RunApplicationError::Invalid)?,
        ),
        _ => return Err(RunApplicationError::NotFound),
    };
    request.validate()?;
    Ok(request)
}
async fn execute(
    State(state): State<RecoveryHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(action): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return crate::run::problem(RunApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return crate::run::problem(RunApplicationError::Unauthenticated);
    }
    let request = match parse_recovery_request(&action, &body) {
        Ok(value) => value,
        Err(error) => return crate::run::problem(error),
    };
    let mut keys = headers.get_all("idempotency-key").iter();
    let Some(key) = keys.next().and_then(|value| value.to_str().ok()) else {
        return crate::run::problem(RunApplicationError::Invalid);
    };
    if keys.next().is_some()
        || key.is_empty()
        || key.len() > 255
        || !key.is_ascii()
        || key.bytes().any(|byte| byte.is_ascii_control())
    {
        return crate::run::problem(RunApplicationError::Invalid);
    }
    let digest=canonical_digest(&serde_json::json!({"schema_version":1,"tenant_id":principal.tenant_id,"principal_id":principal.principal_id,"operation":request.operation(),"target":request.target(),"key":key})).ok().and_then(|value|value.parse().ok());
    let Some(idempotency_key_digest) = digest else {
        return crate::run::problem(RunApplicationError::Invalid);
    };
    let deadline = state.clock.now() + Duration::seconds(10);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        state.application.execute(RecoveryIntent {
            principal,
            request: request.clone(),
            idempotency_key_digest,
            deadline,
        }),
    )
    .await;
    match result {
        Ok(Ok(result)) if result.validate_for(&request, state.clock.now()).is_ok() => {
            let Some(etag) = serde_json::to_value(&result)
                .ok()
                .and_then(|value| canonical_digest(&value).ok())
                .and_then(|digest| HeaderValue::from_str(&format!("\"{digest}\"")).ok())
            else {
                return crate::run::problem(RunApplicationError::Internal);
            };
            let mut response = (StatusCode::OK, Json(result)).into_response();
            response.headers_mut().insert("etag", etag);
            response.headers_mut().insert(
                "cache-control",
                HeaderValue::from_static("no-store, private, max-age=0"),
            );
            response
        }
        Ok(Ok(_)) => crate::run::problem(RunApplicationError::Internal),
        Ok(Err(error)) => crate::run::problem(error),
        Err(_) => crate::run::problem(RunApplicationError::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use insight_platform_contracts::{
        AuthnStrength, PermissionSet, PrincipalKind, TraceIdentityV1,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;
    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }
    fn body() -> serde_json::Value {
        serde_json::json!({"schema_version":1,"run_id":id(ResourceKind::Run),"expected_run_version":1,"reason_evidence_digest":format!("sha256:{}","a".repeat(64))})
    }
    fn principal() -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant),
            principal_id: id(ResourceKind::Principal),
            principal_kind: PrincipalKind::AgentRunner,
            permissions: PermissionSet::new(vec![]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            credential_expires_at: Utc::now() + Duration::hours(1),
            trace: TraceIdentityV1::generate(),
        }
    }
    struct App(AtomicUsize);
    #[async_trait]
    impl RecoveryApplication for App {
        async fn execute(
            &self,
            _: RecoveryIntent,
        ) -> Result<RecoveryResultV1, RunApplicationError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(RunApplicationError::Denied)
        }
    }
    #[test]
    fn recovery_wire_rejects_ambiguous_unbounded_or_wrong_targets() {
        let valid = body();
        assert!(parse_recovery_request(
            "run-history-holds:place",
            &serde_json::to_vec(&valid).unwrap()
        )
        .is_ok());
        let mut wrong = valid.clone();
        wrong["expected_run_version"] =
            serde_json::json!(insight_platform_contracts::MAX_SAFE_JSON_INTEGER + 1);
        assert!(parse_recovery_request(
            "run-history-holds:place",
            &serde_json::to_vec(&wrong).unwrap()
        )
        .is_err());
        wrong = valid.clone();
        wrong["run_id"] = serde_json::json!(id(ResourceKind::Interaction));
        assert!(parse_recovery_request(
            "run-history-holds:place",
            &serde_json::to_vec(&wrong).unwrap()
        )
        .is_err());
        let duplicate =
            serde_json::to_string(&valid)
                .unwrap()
                .replacen('{', "{\"schema_version\":1,", 1);
        assert!(parse_recovery_request("run-history-holds:place", duplicate.as_bytes()).is_err());
        wrong = valid;
        wrong["new_job_id"] = serde_json::json!(id(ResourceKind::Job));
        assert!(parse_recovery_request(
            "run-history-holds:place",
            &serde_json::to_vec(&wrong).unwrap()
        )
        .is_err());
    }
    #[tokio::test]
    async fn authentication_and_idempotency_precede_command_dispatch() {
        let app = Arc::new(App(AtomicUsize::new(0)));
        let router = build_recovery_router(RecoveryHttpState::new(
            app.clone(),
            Arc::new(crate::run::SystemRunClock),
        ));
        let send = |body: serde_json::Value| {
            Request::builder()
                .method("POST")
                .uri("/v1/recovery/run-history-holds:place")
                .header("content-type", "application/json")
                .header("idempotency-key", "fixture")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap()
        };
        assert_eq!(
            router.clone().oneshot(send(body())).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(app.0.load(Ordering::SeqCst), 0);
        let principal = AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant),
            principal_id: id(ResourceKind::Principal),
            principal_kind: PrincipalKind::AgentRunner,
            permissions: PermissionSet::new(vec![]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            credential_expires_at: Utc::now() + Duration::hours(1),
            trace: TraceIdentityV1::generate(),
        };
        let router = router.layer(Extension(principal));
        let mut missing_key = send(body());
        missing_key.headers_mut().remove("idempotency-key");
        assert_eq!(
            router.clone().oneshot(missing_key).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(app.0.load(Ordering::SeqCst), 0);
        let denied = router.oneshot(send(body())).await.unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        assert_eq!(app.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            denied.headers()["cache-control"],
            "no-store, private, max-age=0"
        );
    }
    #[tokio::test]
    async fn successful_hold_uses_canonical_public_time_and_projection_etag() {
        struct HoldApp;
        #[async_trait]
        impl RecoveryApplication for HoldApp {
            async fn execute(
                &self,
                intent: RecoveryIntent,
            ) -> Result<RecoveryResultV1, RunApplicationError> {
                let RecoveryRequest::Place(request) = intent.request else {
                    return Err(RunApplicationError::Invalid);
                };
                let hold_key = intent.idempotency_key_digest;
                let mut holds = insight_platform_orchestrator::history::RunHistoryHolds::default();
                holds.holds.insert(
                    hold_key.clone(),
                    insight_platform_orchestrator::history::RunHistoryHold {
                        reason_evidence_digest: request.reason_evidence_digest,
                        placed_by: intent.principal.principal_id,
                        placed_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123456789Z")
                            .unwrap()
                            .with_timezone(&Utc),
                    },
                );
                Ok(RecoveryResultV1::HistoryHold {
                    schema_version: 1,
                    hold_key,
                    current: RunHistoryHoldOutcome {
                        run_id: request.run_id,
                        run_version: 2,
                        holds,
                    }
                    .into(),
                })
            }
        }
        let router = build_recovery_router(RecoveryHttpState::new(
            Arc::new(HoldApp),
            Arc::new(crate::run::SystemRunClock),
        ))
        .layer(Extension(principal()));
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/recovery/run-history-holds:place")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "stable-placement")
                    .body(Body::from(serde_json::to_vec(&body()).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["cache-control"],
            "no-store, private, max-age=0"
        );
        let etag = response.headers()["etag"].to_str().unwrap().to_owned();
        let bytes = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(etag, format!("\"{}\"", canonical_digest(&value).unwrap()));
        let hold = value["current"]["holds"]["holds"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap();
        assert_eq!(hold["placed_at"], "2026-01-01T00:00:00.123456Z");
        assert!(!String::from_utf8(bytes.to_vec())
            .unwrap()
            .contains("credential"));
    }
    #[test]
    fn release_cannot_accept_a_different_hold_identity() {
        let run_id = id(ResourceKind::Run);
        let key: Sha256Digest = format!("sha256:{}", "a".repeat(64)).parse().unwrap();
        let wrong: Sha256Digest = format!("sha256:{}", "b".repeat(64)).parse().unwrap();
        let request = RecoveryRequest::Release(ReleaseHistoryHoldRequestV1 {
            schema_version: 1,
            run_id: run_id.clone(),
            expected_run_version: 2,
            hold_key: key.clone(),
            release_evidence_digest: key,
        });
        let result = RecoveryResultV1::HistoryHold {
            schema_version: 1,
            hold_key: wrong,
            current: RunHistoryHoldOutcome {
                run_id,
                run_version: 3,
                holds: Default::default(),
            }
            .into(),
        };
        assert!(result.validate_for(&request, Utc::now()).is_err());
    }
}
