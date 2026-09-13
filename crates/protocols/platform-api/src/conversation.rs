//! Workspace conversation transport; persisted facts remain behind the application port.
use crate::{
    authentication::AuthenticatedPrincipal,
    product::*,
    run::{problem, RunApplicationError},
};
use async_trait::async_trait;
use axum::{
    extract::{
        rejection::{JsonRejection, QueryRejection},
        DefaultBodyLimit, Extension, Path, Query, State,
    },
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, validate_conversation_message, ConversationTurnViewV1, ConversationViewV1,
    ResourceId, ResourceKind, Sha256Digest, UtcTimestamp, MAX_CONVERSATION_TITLE_BYTES,
};
use serde::{Deserialize, Serialize};
use std::{future::Future, sync::Arc};

const CONVERSATION_OPERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

async fn bounded<T>(
    operation: impl Future<Output = Result<T, RunApplicationError>>,
) -> Result<T, RunApplicationError> {
    tokio::time::timeout(CONVERSATION_OPERATION_TIMEOUT, operation)
        .await
        .map_err(|_| RunApplicationError::Unavailable)?
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateConversationRequestV1 {
    pub schema_version: u32,
    pub agent_id: ResourceId,
    pub title: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendConversationRequestV1 {
    pub schema_version: u32,
    pub message: String,
    pub deadline: UtcTimestamp,
}
#[derive(Clone)]
pub struct ConversationCommand {
    pub principal: AuthenticatedPrincipal,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
}
#[derive(Clone)]
pub struct ConversationListIntent {
    pub principal: AuthenticatedPrincipal,
    pub conversation_id: Option<ResourceId>,
    pub agent_id: Option<ResourceId>,
    pub snapshot_at: Option<DateTime<Utc>>,
    pub boundary: Option<ListKeysetBoundary>,
    pub limit: u16,
}
#[async_trait]
pub trait ConversationApplication: Send + Sync {
    async fn create(
        &self,
        context: ConversationCommand,
        request: CreateConversationRequestV1,
    ) -> Result<ConversationViewV1, RunApplicationError>;
    async fn read(
        &self,
        principal: AuthenticatedPrincipal,
        id: ResourceId,
    ) -> Result<ConversationViewV1, RunApplicationError>;
    async fn list(
        &self,
        intent: ConversationListIntent,
    ) -> Result<AuthorityListPage<ConversationViewV1>, RunApplicationError>;
    async fn turns(
        &self,
        intent: ConversationListIntent,
    ) -> Result<AuthorityListPage<ConversationTurnViewV1>, RunApplicationError>;
    async fn send(
        &self,
        context: ConversationCommand,
        id: ResourceId,
        version: u64,
        request: SendConversationRequestV1,
    ) -> Result<ConversationTurnViewV1, RunApplicationError>;
}
#[derive(Clone)]
pub struct ConversationHttpState {
    pub application: Arc<dyn ConversationApplication>,
    pub cursors: Arc<dyn ListCursorCodec>,
}
pub fn build_conversation_router(state: ConversationHttpState) -> Router {
    Router::new()
        .route("/v1/conversations", get(list).post(create))
        .route("/v1/conversations/{id}", get(read))
        .route("/v1/conversations/{id}/turns", get(turns).post(send))
        .layer(DefaultBodyLimit::max(131072))
        .with_state(state)
}
pub fn conversation_etag(id: &ResourceId, version: u64) -> String {
    format!("\"{id}-v{version}\"")
}
fn response<T: Serialize>(status: StatusCode, data: T, etag: Option<String>) -> Response {
    let mut result = (status, Json(data)).into_response();
    result.headers_mut().insert(
        "cache-control",
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    if let Some(etag) = etag {
        if let Ok(v) = HeaderValue::from_str(&etag) {
            result.headers_mut().insert("etag", v);
        }
    }
    result
}
fn principal(
    value: Option<Extension<AuthenticatedPrincipal>>,
) -> Result<AuthenticatedPrincipal, RunApplicationError> {
    let Extension(p) = value.ok_or(RunApplicationError::Unauthenticated)?;
    p.validate()
        .map_err(|_| RunApplicationError::Unauthenticated)?;
    Ok(p)
}
fn command<T: Serialize>(
    p: AuthenticatedPrincipal,
    h: &HeaderMap,
    operation: &str,
    id: &ResourceId,
    request: &T,
    version: Option<u64>,
) -> Result<ConversationCommand, RunApplicationError> {
    let mut values = h.get_all("idempotency-key").iter();
    let key = values
        .next()
        .and_then(|h| h.to_str().ok())
        .filter(|s| !s.is_empty() && s.len() <= 255 && s.bytes().all(|b| (32..=126).contains(&b)))
        .ok_or(RunApplicationError::Invalid)?;
    if values.next().is_some() {
        return Err(RunApplicationError::Invalid);
    }
    let digest = |v: &serde_json::Value| {
        canonical_digest(v)
            .ok()
            .and_then(|s| s.parse::<Sha256Digest>().ok())
            .ok_or(RunApplicationError::Invalid)
    };
    let idempotency_key_digest = digest(
        &serde_json::json!({"tenant":p.tenant_id,"principal":p.principal_id,"operation":operation,"scope":id,"key":key}),
    )?;
    let request_digest = digest(
        &serde_json::json!({"schema_version":1,"key":idempotency_key_digest,"request":request,"version":version}),
    )?;
    Ok(ConversationCommand {
        principal: p,
        idempotency_key_digest,
        request_digest,
    })
}
async fn create(
    State(s): State<ConversationHttpState>,
    p: Option<Extension<AuthenticatedPrincipal>>,
    h: HeaderMap,
    body: Result<Json<CreateConversationRequestV1>, JsonRejection>,
) -> Response {
    let result = bounded(async {
        let p = principal(p)?;
        let Json(r) = body.map_err(|_| RunApplicationError::Invalid)?;
        if r.schema_version != 1
            || r.agent_id.kind() != ResourceKind::Agent
            || r.title.trim().is_empty()
            || r.title.len() > MAX_CONVERSATION_TITLE_BYTES
            || r.title.chars().any(char::is_control)
        {
            return Err(RunApplicationError::Invalid);
        }
        let scope = p.tenant_id.clone();
        let c = command(p, &h, "conversation.create", &scope, &r, None)?;
        s.application.create(c, r).await
    })
    .await;
    match result {
        Ok(v) => {
            let e = conversation_etag(&v.conversation_id, v.version);
            response(StatusCode::CREATED, v, Some(e))
        }
        Err(e) => problem(e),
    }
}
async fn read(
    State(s): State<ConversationHttpState>,
    p: Option<Extension<AuthenticatedPrincipal>>,
    Path(id): Path<String>,
) -> Response {
    let result = bounded(async {
        s.application
            .read(
                principal(p)?,
                ResourceId::parse_expected(&id, ResourceKind::Conversation)
                    .map_err(|_| RunApplicationError::NotFound)?,
            )
            .await
    })
    .await;
    match result {
        Ok(v) => {
            let e = conversation_etag(&v.conversation_id, v.version);
            response(StatusCode::OK, v, Some(e))
        }
        Err(e) => problem(e),
    }
}
fn expected_conversation_version(
    id: &ResourceId,
    headers: &HeaderMap,
) -> Result<u64, RunApplicationError> {
    let mut etags = headers.get_all("if-match").iter();
    let e = etags
        .next()
        .and_then(|x| x.to_str().ok())
        .ok_or(RunApplicationError::Invalid)?;
    if etags.next().is_some() {
        return Err(RunApplicationError::Invalid);
    }
    let prefix = format!("\"{id}-v");
    let version = e
        .strip_prefix(&prefix)
        .and_then(|x| x.strip_suffix('"'))
        .and_then(|x| x.parse::<u64>().ok())
        .filter(|x| *x > 0)
        .ok_or(RunApplicationError::Invalid)?;
    if conversation_etag(id, version) != e {
        return Err(RunApplicationError::Invalid);
    }
    Ok(version)
}

async fn send(
    State(s): State<ConversationHttpState>,
    p: Option<Extension<AuthenticatedPrincipal>>,
    Path(id): Path<String>,
    h: HeaderMap,
    body: Result<Json<SendConversationRequestV1>, JsonRejection>,
) -> Response {
    let result = bounded(async {
        let p = principal(p)?;
        let id = ResourceId::parse_expected(&id, ResourceKind::Conversation)
            .map_err(|_| RunApplicationError::NotFound)?;
        let Json(r) = body.map_err(|_| RunApplicationError::Invalid)?;
        if r.schema_version != 1 || !validate_conversation_message(&r.message) {
            return Err(RunApplicationError::Invalid);
        }
        let version = expected_conversation_version(&id, &h)?;
        let c = command(p, &h, "conversation.send", &id, &r, Some(version))?;
        s.application.send(c, id, version, r).await
    })
    .await;
    match result {
        Ok(v) => {
            let e = conversation_etag(&v.conversation_id, v.conversation_version);
            response(StatusCode::CREATED, v, Some(e))
        }
        Err(e) => problem(e),
    }
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListQuery {
    cursor: Option<String>,
    agent_id: Option<ResourceId>,
    limit: Option<u16>,
}
async fn list(
    State(s): State<ConversationHttpState>,
    p: Option<Extension<AuthenticatedPrincipal>>,
    q: Result<Query<ListQuery>, QueryRejection>,
) -> Response {
    list_inner(s, p, None, q).await
}
async fn turns(
    State(s): State<ConversationHttpState>,
    p: Option<Extension<AuthenticatedPrincipal>>,
    Path(id): Path<String>,
    q: Result<Query<ListQuery>, QueryRejection>,
) -> Response {
    let id = match ResourceId::parse_expected(&id, ResourceKind::Conversation) {
        Ok(v) => v,
        Err(_) => return problem(RunApplicationError::NotFound),
    };
    list_inner(s, p, Some(id), q).await
}
async fn list_inner(
    s: ConversationHttpState,
    p: Option<Extension<AuthenticatedPrincipal>>,
    id: Option<ResourceId>,
    q: Result<Query<ListQuery>, QueryRejection>,
) -> Response {
    let result = bounded(async {
        let p = principal(p)?;
        let Query(q) = q.map_err(|_| RunApplicationError::Invalid)?;
        let size = q.limit.unwrap_or(50);
        if size == 0 || size > 50
            || q.agent_id.as_ref().is_some_and(|id| id.kind() != ResourceKind::Agent)
            || id.is_some() && q.agent_id.is_some()
        {
            return Err(RunApplicationError::Invalid);
        }
        let context = ListCursorContext {
            purpose: if id.is_some() { ListRoutePurpose::ConversationTurns } else { ListRoutePurpose::Conversations },
            filter_digest: list_filter_digest(&serde_json::json!({"conversation_id":id,"agent_id":q.agent_id}))
                .map_err(|_| RunApplicationError::Invalid)?,
            page_size: size,
        };
        let now = Utc::now();
        let decoded = q.cursor.as_deref().map(|cursor| s.cursors.decode(cursor, &p, &context, now))
            .transpose().map_err(|error| match error {
                ListError::Invalid => RunApplicationError::CursorInvalid,
                ListError::Expired => RunApplicationError::CursorExpired,
            })?;
        let snapshot = decoded.as_ref().map(|value| value.snapshot_at);
        let expires = decoded.as_ref().map(|value| value.expires_at).unwrap_or(now + Duration::seconds(900));
        let intent = ConversationListIntent {
            principal: p.clone(), conversation_id: id.clone(), agent_id: q.agent_id,
            snapshot_at: snapshot, boundary: decoded.map(|value| value.boundary), limit: size,
        };
        let (items, at, boundary) = if id.is_some() {
            let page = s.application.turns(intent).await?;
            (serde_json::to_value(page.items), page.snapshot_at, page.next_boundary)
        } else {
            let page = s.application.list(intent).await?;
            (serde_json::to_value(page.items), page.snapshot_at, page.next_boundary)
        };
        if snapshot.is_some_and(|expected| expected != at) {
            return Err(RunApplicationError::Internal);
        }
        let cursor = boundary.map(|boundary| s.cursors.encode(&p, &context, at, boundary, expires))
            .transpose().map_err(|_| RunApplicationError::Internal)?;
        Ok(serde_json::json!({"schema_version":1,"items":items.map_err(|_|RunApplicationError::Internal)?,"next_cursor":cursor}))
    }).await;
    match result {
        Ok(v) => response(StatusCode::OK, v, None),
        Err(e) => problem(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        AuthnStrength, Permission, PermissionSet, PrincipalKind, TraceIdentityV1,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    use tower::ServiceExt;
    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }
    fn p() -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant),
            principal_id: id(ResourceKind::Principal),
            principal_kind: PrincipalKind::AgentRunner,
            permissions: PermissionSet::new(vec![Permission::RuntimeRead]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            credential_expires_at: Utc::now() + Duration::hours(1),
            trace: TraceIdentityV1::generate(),
        }
    }
    #[test]
    fn receipt_identity_is_stable_but_changed_version_or_body_is_a_different_request() {
        let p = p();
        let id = id(ResourceKind::Conversation);
        let mut h = HeaderMap::new();
        h.insert("idempotency-key", HeaderValue::from_static("same-command"));
        let a = command(p.clone(), &h, "conversation.send", &id, &"hello", Some(2)).unwrap();
        let b = command(p.clone(), &h, "conversation.send", &id, &"hello", Some(3)).unwrap();
        let c = command(p, &h, "conversation.send", &id, &"changed", Some(2)).unwrap();
        assert_eq!(a.idempotency_key_digest, b.idempotency_key_digest);
        assert_ne!(a.request_digest, b.request_digest);
        assert_ne!(a.request_digest, c.request_digest);
    }
    #[test]
    fn duplicate_or_oversized_receipt_headers_are_rejected() {
        let p = p();
        let id = id(ResourceKind::Conversation);
        let mut h = HeaderMap::new();
        h.append("idempotency-key", HeaderValue::from_static("a"));
        h.append("idempotency-key", HeaderValue::from_static("b"));
        assert!(command(p.clone(), &h, "send", &id, &"hi", Some(1)).is_err());
        h.clear();
        h.insert(
            "idempotency-key",
            HeaderValue::from_str(&"x".repeat(256)).unwrap(),
        );
        assert!(command(p, &h, "send", &id, &"hi", Some(1)).is_err());
    }

    #[test]
    fn if_match_requires_one_exact_strong_etag() {
        let id = id(ResourceKind::Conversation);
        let mut headers = HeaderMap::new();
        headers.insert(
            "if-match",
            HeaderValue::from_str(&conversation_etag(&id, 1)).unwrap(),
        );
        assert_eq!(expected_conversation_version(&id, &headers).unwrap(), 1);
        for value in [
            format!("\"{id}-v01\""),
            format!("\"{id}-v+1\""),
            format!("\"{id}-v0\""),
            format!("W/{}", conversation_etag(&id, 1)),
            "*".into(),
            format!(
                "{}, {}",
                conversation_etag(&id, 1),
                conversation_etag(&id, 2)
            ),
            conversation_etag(&self::id(ResourceKind::Conversation), 1),
        ] {
            headers.insert("if-match", HeaderValue::from_str(&value).unwrap());
            assert!(
                expected_conversation_version(&id, &headers).is_err(),
                "{value}"
            );
        }
        headers.insert(
            "if-match",
            HeaderValue::from_str(&conversation_etag(&id, 1)).unwrap(),
        );
        headers.append(
            "if-match",
            HeaderValue::from_str(&conversation_etag(&id, 1)).unwrap(),
        );
        assert!(expected_conversation_version(&id, &headers).is_err());
    }

    #[derive(Default)]
    struct ProbeApplication {
        block: bool,
        dropped: AtomicUsize,
        creates: Mutex<Vec<ConversationCommand>>,
    }
    impl ProbeApplication {
        async fn result<T>(&self) -> Result<T, RunApplicationError> {
            if self.block {
                struct Cancelled<'a>(&'a AtomicUsize);
                impl Drop for Cancelled<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_add(1, Ordering::SeqCst);
                    }
                }
                let _guard = Cancelled(&self.dropped);
                std::future::pending().await
            } else {
                Err(RunApplicationError::NotFound)
            }
        }
    }
    #[async_trait]
    impl ConversationApplication for ProbeApplication {
        async fn create(
            &self,
            c: ConversationCommand,
            _: CreateConversationRequestV1,
        ) -> Result<ConversationViewV1, RunApplicationError> {
            self.creates.lock().unwrap().push(c);
            self.result().await
        }
        async fn read(
            &self,
            _: AuthenticatedPrincipal,
            _: ResourceId,
        ) -> Result<ConversationViewV1, RunApplicationError> {
            self.result().await
        }
        async fn list(
            &self,
            _: ConversationListIntent,
        ) -> Result<AuthorityListPage<ConversationViewV1>, RunApplicationError> {
            self.result().await
        }
        async fn turns(
            &self,
            _: ConversationListIntent,
        ) -> Result<AuthorityListPage<ConversationTurnViewV1>, RunApplicationError> {
            self.result().await
        }
        async fn send(
            &self,
            _: ConversationCommand,
            _: ResourceId,
            _: u64,
            _: SendConversationRequestV1,
        ) -> Result<ConversationTurnViewV1, RunApplicationError> {
            self.result().await
        }
    }
    fn router(application: Arc<ProbeApplication>) -> Router {
        build_conversation_router(ConversationHttpState {
            application,
            cursors: Arc::new(HmacListCursorCodec::install(&[7; 32]).unwrap()),
        })
        .layer(Extension(p()))
    }
    fn request(
        method: &str,
        path: &str,
        body: serde_json::Value,
        etag: Option<String>,
    ) -> axum::http::Request<axum::body::Body> {
        let mut builder = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json")
            .header("idempotency-key", "same-command");
        if let Some(value) = etag {
            builder = builder.header("if-match", value);
        }
        builder
            .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }
    #[tokio::test]
    async fn creating_for_another_agent_preserves_receipt_key_but_changes_request_digest() {
        let application = Arc::new(ProbeApplication::default());
        let router = router(application.clone());
        for agent in [id(ResourceKind::Agent), id(ResourceKind::Agent)] {
            router
                .clone()
                .oneshot(request(
                    "POST",
                    "/v1/conversations",
                    serde_json::json!({"schema_version":1,"agent_id":agent,"title":"Debug"}),
                    None,
                ))
                .await
                .unwrap();
        }
        let commands = application.creates.lock().unwrap();
        assert_eq!(commands.len(), 2);
        assert_eq!(
            commands[0].idempotency_key_digest,
            commands[1].idempotency_key_digest
        );
        assert_ne!(commands[0].request_digest, commands[1].request_digest);
    }
    #[tokio::test(start_paused = true)]
    async fn every_conversation_route_times_out_and_drops_the_pending_operation() {
        let application = Arc::new(ProbeApplication {
            block: true,
            ..Default::default()
        });
        let router = router(application.clone());
        let conversation = id(ResourceKind::Conversation);
        let paths = [
            (
                "GET",
                "/v1/conversations".into(),
                serde_json::Value::Null,
                None,
            ),
            (
                "GET",
                format!("/v1/conversations/{conversation}"),
                serde_json::Value::Null,
                None,
            ),
            (
                "GET",
                format!("/v1/conversations/{conversation}/turns"),
                serde_json::Value::Null,
                None,
            ),
            (
                "POST",
                "/v1/conversations".into(),
                serde_json::json!({"schema_version":1,"agent_id":id(ResourceKind::Agent),"title":"Debug"}),
                None,
            ),
            (
                "POST",
                format!("/v1/conversations/{conversation}/turns"),
                serde_json::json!({"schema_version":1,"message":"Hello","deadline":UtcTimestamp::from_datetime(Utc::now()+Duration::hours(1))}),
                Some(conversation_etag(&conversation, 1)),
            ),
        ];
        for (index, (method, path, body, etag)) in paths.into_iter().enumerate() {
            let start = tokio::time::Instant::now();
            let response = router
                .clone()
                .oneshot(request(method, &path, body, etag))
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{method} {path}"
            );
            assert_eq!(start.elapsed(), CONVERSATION_OPERATION_TIMEOUT);
            assert_eq!(application.dropped.load(Ordering::SeqCst), index + 1);
        }
    }
}
