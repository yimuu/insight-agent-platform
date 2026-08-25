use crate::authentication::AuthenticatedPrincipal;
use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{rejection::JsonRejection, DefaultBodyLimit, Extension, Path, State},
    http::{header::CACHE_CONTROL, HeaderMap, HeaderValue, StatusCode},
    response::{sse::Event, IntoResponse, Response, Sse},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Duration, Utc};
use futures::stream;
use insight_platform_contracts::{
    canonical_digest, ApiProblem, ApiProblemCode, DataClassification, DurablePublicRunEventData,
    EventDurability, OpaqueRunEventCursor, PublicRunEvent, PublicRunEventSourceKind,
    PublicRunEventType, ResourceId, ResourceKind, RunState, Sha256Digest, UtcTimestamp, ValueRef,
    MAX_FIELD_ERRORS, MAX_SAFE_TEXT_BYTES,
};
use ring::hmac;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc};

const RUN_READ_DEADLINE_MILLISECONDS: i64 = 5_000;
const RUN_COMMAND_DEADLINE_MILLISECONDS: i64 = 10_000;
const IDEMPOTENCY_KEY: &str = "idempotency-key";
const MAX_RUN_REQUEST_BYTES: usize = 1_048_576;
const IF_MATCH: &str = "if-match";
const LAST_EVENT_ID: &str = "last-event-id";
const RUN_EVENT_PAGE_LIMIT: u16 = 128;
const RUN_EVENT_CURSOR_TTL_SECONDS: i64 = 900;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRunInputV1 {
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub value: ValueRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRunRequestV1 {
    pub agent_id: ResourceId,
    pub input: CreateRunInputV1,
    pub deadline: UtcTimestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalRunPayloadV1 {
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub value: ValueRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalRunRequestV1 {
    pub payload: Option<SignalRunPayloadV1>,
}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunResultViewV1 {
    pub schema_version: u32,
    pub run_id: ResourceId,
    pub value_id: ResourceId,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub value: ValueRef,
}

impl RunResultViewV1 {
    pub fn validate(&self) -> Result<(), RunApplicationError> {
        if self.schema_version != 1
            || self.run_id.kind() != ResourceKind::Run
            || self.value_id.kind() != ResourceKind::RunValue
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEventProjectionV1 {
    pub event_id: ResourceId,
    pub sequence: u64,
    pub event_type: PublicRunEventType,
    pub source_kind: PublicRunEventSourceKind,
    pub source_id: ResourceId,
    pub source_projection_version: u64,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ReadRunEventsIntent {
    pub principal: AuthenticatedPrincipal,
    pub run_id: ResourceId,
    pub after_sequence: u64,
    pub limit: u16,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunEventCursorError {
    Invalid,
    Expired,
}

pub trait RunEventCursorCodec: Send + Sync {
    fn encode(
        &self,
        principal: &AuthenticatedPrincipal,
        run_id: &ResourceId,
        sequence: u64,
        expires_at: DateTime<Utc>,
    ) -> Result<OpaqueRunEventCursor, RunEventCursorError>;

    fn decode(
        &self,
        cursor: &str,
        principal: &AuthenticatedPrincipal,
        run_id: &ResourceId,
        now: DateTime<Utc>,
    ) -> Result<u64, RunEventCursorError>;
}

#[derive(Debug, Clone)]
pub struct HmacRunEventCursorCodec {
    key: hmac::Key,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunEventCursorClaims {
    schema_version: u32,
    scope_digest: Sha256Digest,
    run_id: ResourceId,
    sequence: u64,
    expires_at_epoch_seconds: i64,
}

impl HmacRunEventCursorCodec {
    pub fn install(key: &[u8]) -> Result<Self, RunEventCursorError> {
        if !(32..=64).contains(&key.len()) {
            return Err(RunEventCursorError::Invalid);
        }
        Ok(Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, key),
        })
    }
}

impl RunEventCursorCodec for HmacRunEventCursorCodec {
    fn encode(
        &self,
        principal: &AuthenticatedPrincipal,
        run_id: &ResourceId,
        sequence: u64,
        expires_at: DateTime<Utc>,
    ) -> Result<OpaqueRunEventCursor, RunEventCursorError> {
        let claims = RunEventCursorClaims {
            schema_version: 1,
            scope_digest: run_event_scope_digest(principal)?,
            run_id: run_id.clone(),
            sequence,
            expires_at_epoch_seconds: expires_at.timestamp(),
        };
        let payload = serde_jcs::to_vec(&claims).map_err(|_| RunEventCursorError::Invalid)?;
        let signature = hmac::sign(&self.key, &payload);
        OpaqueRunEventCursor::new(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        ))
        .map_err(|_| RunEventCursorError::Invalid)
    }

    fn decode(
        &self,
        cursor: &str,
        principal: &AuthenticatedPrincipal,
        run_id: &ResourceId,
        now: DateTime<Utc>,
    ) -> Result<u64, RunEventCursorError> {
        let (payload, signature) = cursor.split_once('.').ok_or(RunEventCursorError::Invalid)?;
        if signature.contains('.') {
            return Err(RunEventCursorError::Invalid);
        }
        let payload = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| RunEventCursorError::Invalid)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| RunEventCursorError::Invalid)?;
        hmac::verify(&self.key, &payload, &signature).map_err(|_| RunEventCursorError::Invalid)?;
        let claims: RunEventCursorClaims =
            serde_json::from_slice(&payload).map_err(|_| RunEventCursorError::Invalid)?;
        if claims.schema_version != 1
            || claims.run_id != *run_id
            || claims.scope_digest != run_event_scope_digest(principal)?
            || claims.sequence == 0
        {
            return Err(RunEventCursorError::Invalid);
        }
        if claims.expires_at_epoch_seconds <= now.timestamp() {
            return Err(RunEventCursorError::Expired);
        }
        Ok(claims.sequence)
    }
}

fn run_event_scope_digest(
    principal: &AuthenticatedPrincipal,
) -> Result<Sha256Digest, RunEventCursorError> {
    canonical_digest(&serde_json::json!({
        "binding_generation": principal.binding_generation,
        "binding_version": principal.binding_version,
        "permissions": principal.permissions,
        "principal_id": principal.principal_id,
        "principal_kind": principal.principal_kind,
        "principal_version": principal.principal_version,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    }))
    .map_err(|_| RunEventCursorError::Invalid)?
    .parse()
    .map_err(|_| RunEventCursorError::Invalid)
}

#[derive(Debug, Clone)]
pub struct CreateRunIntent {
    pub principal: AuthenticatedPrincipal,
    pub request: CreateRunRequestV1,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ControlRunIntent {
    pub principal: AuthenticatedPrincipal,
    pub run_id: ResourceId,
    pub expected_run_version: u64,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SignalRunIntent {
    pub principal: AuthenticatedPrincipal,
    pub run_id: ResourceId,
    pub signal_key: String,
    pub request: SignalRunRequestV1,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunApplicationError {
    Unauthenticated,
    Invalid,
    Denied,
    NotFound,
    Conflict,
    IdempotencyConflict,
    NotTerminal,
    CursorInvalid,
    CursorExpired,
    Unavailable,
    Internal,
}

#[async_trait]
pub trait RunApplication: Send + Sync {
    async fn create_run(&self, _intent: CreateRunIntent) -> Result<RunViewV1, RunApplicationError> {
        Err(RunApplicationError::Internal)
    }
    async fn pause_run(&self, _intent: ControlRunIntent) -> Result<RunViewV1, RunApplicationError> {
        Err(RunApplicationError::Internal)
    }
    async fn resume_run(
        &self,
        _intent: ControlRunIntent,
    ) -> Result<RunViewV1, RunApplicationError> {
        Err(RunApplicationError::Internal)
    }
    async fn cancel_run(
        &self,
        _intent: ControlRunIntent,
    ) -> Result<RunViewV1, RunApplicationError> {
        Err(RunApplicationError::Internal)
    }
    async fn signal_run(&self, _intent: SignalRunIntent) -> Result<(), RunApplicationError> {
        Err(RunApplicationError::Internal)
    }
    async fn read_run_result(
        &self,
        _intent: ReadRunIntent,
    ) -> Result<RunResultViewV1, RunApplicationError> {
        Err(RunApplicationError::Internal)
    }
    async fn read_run_events(
        &self,
        _intent: ReadRunEventsIntent,
    ) -> Result<Vec<RunEventProjectionV1>, RunApplicationError> {
        Err(RunApplicationError::Internal)
    }
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
    event_cursor_codec: Option<Arc<dyn RunEventCursorCodec>>,
}

impl RunHttpState {
    pub fn new(application: Arc<dyn RunApplication>, clock: Arc<dyn RunClock>) -> Self {
        Self {
            application,
            clock,
            event_cursor_codec: None,
        }
    }

    pub fn with_event_cursor_codec(mut self, codec: Arc<dyn RunEventCursorCodec>) -> Self {
        self.event_cursor_codec = Some(codec);
        self
    }
}

pub fn build_run_router(state: RunHttpState) -> Router {
    Router::new()
        .route("/v1/runs", post(create_run))
        .route(
            "/v1/runs/{run_action}",
            get(read_run).post(control_run_action),
        )
        .route("/v1/runs/{run_id}/result", get(read_run_result))
        .route("/v1/runs/{run_id}/events", get(read_run_events))
        .route("/v1/runs/{run_id}/signals/{signal_key}", post(signal_run))
        .layer(DefaultBodyLimit::max(MAX_RUN_REQUEST_BYTES))
        .with_state(state)
}

async fn read_run_events(
    State(state): State<RunHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(RunApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(RunApplicationError::Unauthenticated);
    }
    let run_id = match run_id.parse::<ResourceId>() {
        Ok(id) if id.kind() == ResourceKind::Run => id,
        _ => return problem(RunApplicationError::NotFound),
    };
    let Some(codec) = state.event_cursor_codec.as_ref() else {
        return problem(RunApplicationError::Unavailable);
    };
    let now = state.clock.now();
    let after_sequence = match single_header(&headers, LAST_EVENT_ID) {
        Ok(None) => 0,
        Ok(Some(cursor)) => match codec.decode(cursor, &principal, &run_id, now) {
            Ok(sequence) => sequence,
            Err(RunEventCursorError::Invalid) => {
                return problem(RunApplicationError::CursorInvalid)
            }
            Err(RunEventCursorError::Expired) => {
                return problem(RunApplicationError::CursorExpired)
            }
        },
        Err(()) => return problem(RunApplicationError::CursorInvalid),
    };
    let projections = match state
        .application
        .read_run_events(ReadRunEventsIntent {
            principal: principal.clone(),
            run_id: run_id.clone(),
            after_sequence,
            limit: RUN_EVENT_PAGE_LIMIT,
            deadline: now + Duration::milliseconds(RUN_READ_DEADLINE_MILLISECONDS),
        })
        .await
    {
        Ok(events) => events,
        Err(error) => return problem(error),
    };
    let expires_at = now + Duration::seconds(RUN_EVENT_CURSOR_TTL_SECONDS);
    let mut encoded = Vec::with_capacity(projections.len());
    for projection in projections {
        let cursor = match codec.encode(&principal, &run_id, projection.sequence, expires_at) {
            Ok(cursor) => cursor,
            Err(_) => return problem(RunApplicationError::Internal),
        };
        let data = DurablePublicRunEventData {
            source_kind: projection.source_kind,
            source_id: projection.source_id,
            source_projection_version: projection.source_projection_version,
            safe_summary: None,
        };
        let event = PublicRunEvent {
            event_id: Some(projection.event_id),
            run_id: run_id.clone(),
            cursor: Some(cursor.clone()),
            sequence: Some(projection.sequence),
            schema_version: 1,
            event_type: projection.event_type,
            durability: EventDurability::Durable,
            occurred_at: UtcTimestamp::from_datetime(projection.occurred_at),
            data: match serde_json::to_value(data) {
                Ok(data) => data,
                Err(_) => return problem(RunApplicationError::Internal),
            },
        };
        if event.validate().is_err() {
            return problem(RunApplicationError::Internal);
        }
        let frame = match Event::default()
            .id(cursor.as_str())
            .event(event.event_type.as_str())
            .json_data(event)
        {
            Ok(frame) => frame,
            Err(_) => return problem(RunApplicationError::Internal),
        };
        encoded.push(Ok::<_, Infallible>(frame));
    }
    let mut response = Sse::new(stream::iter(encoded)).into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, ()> {
    let mut values = headers.get_all(name).iter();
    let first = match values.next() {
        Some(value) => value.to_str().map_err(|_| ())?,
        None => return Ok(None),
    };
    if values.next().is_some() || first.is_empty() || first.len() > 4_096 {
        return Err(());
    }
    Ok(Some(first))
}

async fn read_run_result(
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
        Ok(id) if id.kind() == ResourceKind::Run => id,
        _ => return problem(RunApplicationError::NotFound),
    };
    match state
        .application
        .read_run_result(ReadRunIntent {
            principal,
            run_id,
            deadline: state.clock.now() + Duration::milliseconds(RUN_READ_DEADLINE_MILLISECONDS),
        })
        .await
    {
        Ok(view) if view.validate().is_ok() => {
            let mut response = (StatusCode::OK, Json(view)).into_response();
            response.headers_mut().insert(
                CACHE_CONTROL,
                HeaderValue::from_static("no-store, private, max-age=0"),
            );
            response
        }
        Ok(_) => problem(RunApplicationError::Internal),
        Err(error) => problem(error),
    }
}

async fn signal_run(
    State(state): State<RunHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path((run_id, signal_key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Result<Json<SignalRunRequestV1>, JsonRejection>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(RunApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(RunApplicationError::Unauthenticated);
    }
    let run_id = match run_id.parse::<ResourceId>() {
        Ok(id) if id.kind() == ResourceKind::Run => id,
        _ => return problem(RunApplicationError::NotFound),
    };
    if !valid_signal_key(&signal_key) {
        return problem(RunApplicationError::NotFound);
    }
    let Json(request) = match body {
        Ok(request) => request,
        Err(_) => return problem(RunApplicationError::Invalid),
    };
    let idempotency_key_digest = match control_idempotency_digest(
        &headers,
        &principal,
        &run_id,
        &format!("run.signal.{signal_key}"),
    ) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let request_digest = match canonical_digest(&serde_json::json!({
        "idempotency_key_digest": idempotency_key_digest,
        "operation": "run.signal",
        "principal_id": principal.principal_id,
        "request": request,
        "run_id": run_id,
        "schema_version": 1,
        "signal_key": signal_key,
        "tenant_id": principal.tenant_id,
    }))
    .ok()
    .and_then(|value| value.parse().ok())
    {
        Some(digest) => digest,
        None => return problem(RunApplicationError::Invalid),
    };
    match state
        .application
        .signal_run(SignalRunIntent {
            principal,
            run_id,
            signal_key,
            request,
            idempotency_key_digest,
            request_digest,
            deadline: state.clock.now() + Duration::milliseconds(RUN_COMMAND_DEADLINE_MILLISECONDS),
        })
        .await
    {
        Ok(()) => {
            let mut response = StatusCode::NO_CONTENT.into_response();
            response.headers_mut().insert(
                CACHE_CONTROL,
                HeaderValue::from_static("no-store, private, max-age=0"),
            );
            response
        }
        Err(error) => problem(error),
    }
}

fn valid_signal_key(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

async fn control_run_action(
    State(state): State<RunHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(run_action): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !body.is_empty() {
        return problem(RunApplicationError::Invalid);
    }
    let Some(Extension(principal)) = principal else {
        return problem(RunApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(RunApplicationError::Unauthenticated);
    }
    let (run_id, operation) = if let Some(id) = run_action.strip_suffix(":pause") {
        (id, "run.pause")
    } else if let Some(id) = run_action.strip_suffix(":resume") {
        (id, "run.resume")
    } else if let Some(id) = run_action.strip_suffix(":cancel") {
        (id, "run.cancel")
    } else {
        return problem(RunApplicationError::NotFound);
    };
    let run_id = match run_id.parse::<ResourceId>() {
        Ok(id) if id.kind() == ResourceKind::Run => id,
        _ => return problem(RunApplicationError::NotFound),
    };
    let expected_run_version = match expected_run_version(&headers, &run_id) {
        Ok(version) => version,
        Err(error) => return problem(error),
    };
    let idempotency_key_digest =
        match control_idempotency_digest(&headers, &principal, &run_id, operation) {
            Ok(digest) => digest,
            Err(error) => return problem(error),
        };
    let request_digest = match canonical_digest(&serde_json::json!({
        "expected_run_version": expected_run_version,
        "idempotency_key_digest": idempotency_key_digest,
        "operation": operation,
        "principal_id": principal.principal_id,
        "run_id": run_id,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    }))
    .ok()
    .and_then(|value| value.parse().ok())
    {
        Some(digest) => digest,
        None => return problem(RunApplicationError::Invalid),
    };
    let intent = ControlRunIntent {
        principal,
        run_id,
        expected_run_version,
        idempotency_key_digest,
        request_digest,
        deadline: state.clock.now() + Duration::milliseconds(RUN_COMMAND_DEADLINE_MILLISECONDS),
    };
    let result = match operation {
        "run.pause" => state.application.pause_run(intent).await,
        "run.resume" => state.application.resume_run(intent).await,
        "run.cancel" => state.application.cancel_run(intent).await,
        _ => unreachable!(),
    };
    match result {
        Ok(view) if view.validate().is_ok() => run_response(view),
        Ok(_) => problem(RunApplicationError::Internal),
        Err(error) => problem(error),
    }
}

fn expected_run_version(
    headers: &HeaderMap,
    run_id: &ResourceId,
) -> Result<u64, RunApplicationError> {
    let mut values = headers.get_all(IF_MATCH).iter();
    let value = values.next().ok_or(RunApplicationError::Invalid)?;
    if values.next().is_some() {
        return Err(RunApplicationError::Invalid);
    }
    let value = value.to_str().map_err(|_| RunApplicationError::Invalid)?;
    let prefix = format!("\"{run_id}-");
    let version = value
        .strip_prefix(&prefix)
        .and_then(|v| v.strip_suffix('"'))
        .ok_or(RunApplicationError::Invalid)?;
    if version.is_empty() || version.starts_with('0') {
        return Err(RunApplicationError::Invalid);
    }
    version.parse().map_err(|_| RunApplicationError::Invalid)
}

fn control_idempotency_digest(
    headers: &HeaderMap,
    principal: &AuthenticatedPrincipal,
    run_id: &ResourceId,
    operation: &str,
) -> Result<Sha256Digest, RunApplicationError> {
    let mut values = headers.get_all(IDEMPOTENCY_KEY).iter();
    let value = values.next().ok_or(RunApplicationError::Invalid)?;
    if values.next().is_some() {
        return Err(RunApplicationError::Invalid);
    }
    let key = value.to_str().map_err(|_| RunApplicationError::Invalid)?;
    if key.is_empty()
        || key.len() > 255
        || !key.is_ascii()
        || key.bytes().any(|b| b.is_ascii_control())
    {
        return Err(RunApplicationError::Invalid);
    }
    canonical_digest(&serde_json::json!({
        "key": key,
        "operation": operation,
        "principal_id": principal.principal_id,
        "run_id": run_id,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    }))
    .map_err(|_| RunApplicationError::Invalid)?
    .parse()
    .map_err(|_| RunApplicationError::Invalid)
}

async fn create_run(
    State(state): State<RunHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    headers: HeaderMap,
    body: Result<Json<CreateRunRequestV1>, JsonRejection>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(RunApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(RunApplicationError::Unauthenticated);
    }
    let Json(request) = match body {
        Ok(body) if body.agent_id.kind() == ResourceKind::Agent => body,
        _ => return problem(RunApplicationError::Invalid),
    };
    let command_deadline =
        state.clock.now() + Duration::milliseconds(RUN_COMMAND_DEADLINE_MILLISECONDS);
    let requested_deadline = match DateTime::parse_from_rfc3339(request.deadline.as_str()) {
        Ok(value) => value.with_timezone(&Utc),
        Err(_) => return problem(RunApplicationError::Invalid),
    };
    if requested_deadline <= state.clock.now() {
        return problem(RunApplicationError::Invalid);
    }
    let idempotency_key_digest =
        match run_idempotency_digest(&headers, &principal, &request.agent_id) {
            Ok(digest) => digest,
            Err(error) => return problem(error),
        };
    let request_digest = match canonical_digest(&serde_json::json!({
        "idempotency_key_digest": idempotency_key_digest,
        "operation": "run.create",
        "principal_id": principal.principal_id,
        "request": request,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    }))
    .ok()
    .and_then(|value| value.parse().ok())
    {
        Some(digest) => digest,
        None => return problem(RunApplicationError::Invalid),
    };
    match state
        .application
        .create_run(CreateRunIntent {
            principal,
            request,
            idempotency_key_digest,
            request_digest,
            deadline: command_deadline,
        })
        .await
    {
        Ok(view) if view.validate().is_ok() => create_run_response(view),
        Ok(_) => problem(RunApplicationError::Internal),
        Err(error) => problem(error),
    }
}

fn run_idempotency_digest(
    headers: &HeaderMap,
    principal: &AuthenticatedPrincipal,
    agent_id: &ResourceId,
) -> Result<Sha256Digest, RunApplicationError> {
    let mut values = headers.get_all(IDEMPOTENCY_KEY).iter();
    let value = values.next().ok_or(RunApplicationError::Invalid)?;
    if values.next().is_some() {
        return Err(RunApplicationError::Invalid);
    }
    let key = value.to_str().map_err(|_| RunApplicationError::Invalid)?;
    if key.is_empty()
        || key.len() > 255
        || !key.is_ascii()
        || key.bytes().any(|b| b.is_ascii_control())
    {
        return Err(RunApplicationError::Invalid);
    }
    canonical_digest(&serde_json::json!({
        "agent_id": agent_id,
        "key": key,
        "operation": "run.create",
        "principal_id": principal.principal_id,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    }))
    .map_err(|_| RunApplicationError::Invalid)?
    .parse()
    .map_err(|_| RunApplicationError::Invalid)
}

fn create_run_response(view: RunViewV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(etag) => etag,
        Err(_) => return problem(RunApplicationError::Internal),
    };
    let location = match HeaderValue::from_str(&format!("/v1/runs/{}", view.run_id)) {
        Ok(location) => location,
        Err(_) => return problem(RunApplicationError::Internal),
    };
    let mut response = (StatusCode::CREATED, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    response.headers_mut().insert("location", location);
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
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
        RunApplicationError::Invalid => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::InvalidRequest,
            "The Run request is invalid.",
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
        RunApplicationError::Conflict => (
            StatusCode::CONFLICT,
            ApiProblemCode::InvalidStateTransition,
            "The Run request conflicts with current authority.",
            false,
        ),
        RunApplicationError::IdempotencyConflict => (
            StatusCode::CONFLICT,
            ApiProblemCode::IdempotencyConflict,
            "The idempotency key was used for a different Run request.",
            false,
        ),
        RunApplicationError::NotTerminal => (
            StatusCode::CONFLICT,
            ApiProblemCode::RunNotTerminal,
            "The Run is not terminal.",
            false,
        ),
        RunApplicationError::CursorInvalid => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::CursorInvalid,
            "The Run event cursor is invalid.",
            false,
        ),
        RunApplicationError::CursorExpired => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::CursorExpired,
            "The Run event cursor has expired.",
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
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
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
        async fn create_run(
            &self,
            _intent: CreateRunIntent,
        ) -> Result<RunViewV1, RunApplicationError> {
            let now = UtcTimestamp::from_datetime(Utc::now());
            let run_id = id(ResourceKind::Run, 30);
            Ok(RunViewV1 {
                schema_version: 1,
                run_id: run_id.clone(),
                agent_deployment_id: id(ResourceKind::AgentDeployment, 4),
                state: RunState::Queued,
                version: 1,
                input_value_id: id(ResourceKind::RunValue, 31),
                output_value_id: None,
                pause_generation: 0,
                cancel_generation: 0,
                deadline: now.clone(),
                started_at: None,
                terminal_at: None,
                created_at: now.clone(),
                updated_at: now,
                etag: run_etag(&run_id, 1),
            })
        }

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

        async fn pause_run(
            &self,
            intent: ControlRunIntent,
        ) -> Result<RunViewV1, RunApplicationError> {
            let now = UtcTimestamp::from_datetime(Utc::now());
            Ok(RunViewV1 {
                schema_version: 1,
                run_id: intent.run_id.clone(),
                agent_deployment_id: id(ResourceKind::AgentDeployment, 4),
                state: RunState::Queued,
                version: 4,
                input_value_id: id(ResourceKind::RunValue, 5),
                output_value_id: None,
                pause_generation: 1,
                cancel_generation: 0,
                deadline: now.clone(),
                started_at: None,
                terminal_at: None,
                created_at: now.clone(),
                updated_at: now,
                etag: run_etag(&intent.run_id, 4),
            })
        }

        async fn signal_run(&self, _intent: SignalRunIntent) -> Result<(), RunApplicationError> {
            Ok(())
        }

        async fn read_run_result(
            &self,
            intent: ReadRunIntent,
        ) -> Result<RunResultViewV1, RunApplicationError> {
            Ok(RunResultViewV1 {
                schema_version: 1,
                run_id: intent.run_id,
                value_id: id(ResourceKind::RunValue, 40),
                classification: DataClassification::Internal,
                schema_digest: digest('b'),
                content_digest: digest('c'),
                value: ValueRef::Inline {
                    value: serde_json::json!({"answer": "done"}),
                },
            })
        }

        async fn read_run_events(
            &self,
            intent: ReadRunEventsIntent,
        ) -> Result<Vec<RunEventProjectionV1>, RunApplicationError> {
            Ok((intent.after_sequence < 1)
                .then(|| RunEventProjectionV1 {
                    event_id: id(ResourceKind::Event, 41),
                    sequence: 1,
                    event_type: PublicRunEventType::RunQueued,
                    source_kind: PublicRunEventSourceKind::Run,
                    source_id: intent.run_id,
                    source_projection_version: 1,
                    occurred_at: Utc::now(),
                })
                .into_iter()
                .collect())
        }
    }

    #[test]
    fn run_event_cursor_is_scoped_signed_and_expiring() {
        let now = Utc::now();
        let codec = HmacRunEventCursorCodec::install(&[7_u8; 32]).unwrap();
        let run_id = id(ResourceKind::Run, 3);
        let authorized = principal(now);
        let cursor = codec
            .encode(&authorized, &run_id, 7, now + Duration::minutes(5))
            .unwrap();
        assert_eq!(
            codec.decode(cursor.as_str(), &authorized, &run_id, now),
            Ok(7)
        );

        let mut other_principal = authorized.clone();
        other_principal.principal_id = id(ResourceKind::Principal, 9);
        assert_eq!(
            codec.decode(cursor.as_str(), &other_principal, &run_id, now),
            Err(RunEventCursorError::Invalid)
        );
        assert_eq!(
            codec.decode(
                cursor.as_str(),
                &authorized,
                &run_id,
                now + Duration::minutes(6)
            ),
            Err(RunEventCursorError::Expired)
        );
        let mut tampered = cursor.as_str().as_bytes().to_vec();
        tampered[0] = if tampered[0] == b'A' { b'B' } else { b'A' };
        assert_eq!(
            codec.decode(
                std::str::from_utf8(&tampered).unwrap(),
                &authorized,
                &run_id,
                now
            ),
            Err(RunEventCursorError::Invalid)
        );
    }

    #[tokio::test]
    async fn run_events_emit_only_valid_durable_sse_projections() {
        let now = Utc::now();
        let codec: Arc<dyn RunEventCursorCodec> =
            Arc::new(HmacRunEventCursorCodec::install(&[7_u8; 32]).unwrap());
        let router = build_run_router(
            RunHttpState::new(Arc::new(FixtureApplication), Arc::new(FixedClock(now)))
                .with_event_cursor_codec(codec),
        );
        let run_id = id(ResourceKind::Run, 3);
        let response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/runs/{run_id}/events"))
                    .extension(principal(now))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[CACHE_CONTROL],
            "no-store, private, max-age=0"
        );
        assert!(response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream"));
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("event: run.queued"));
        assert!(body.contains(&format!("\"run_id\":\"{run_id}\"")));
        assert!(body.contains("\"source_kind\":\"run\""));
        assert!(!body.contains("payload"));
        assert!(!body.contains("principal"));
        assert!(body.lines().any(|line| line.starts_with("id: ")));
    }

    #[tokio::test]
    async fn run_create_requires_scoped_idempotency_and_returns_stable_location() {
        let now = Utc::now();
        let router = build_run_router(RunHttpState::new(
            Arc::new(FixtureApplication),
            Arc::new(FixedClock(now)),
        ));
        let request = serde_json::json!({
            "agent_id": id(ResourceKind::Agent, 20),
            "input": {
                "classification": "internal",
                "schema_digest": digest('b'),
                "value": {"kind": "inline", "value": {"question": "why"}}
            },
            "deadline": UtcTimestamp::from_datetime(now + Duration::minutes(5))
        });
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runs")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "run-create-1")
                    .extension(principal(now))
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers().get("location").unwrap(),
            &format!("/v1/runs/{}", id(ResourceKind::Run, 30))
        );
        assert_eq!(
            response.headers().get("etag").unwrap(),
            &run_etag(&id(ResourceKind::Run, 30), 1)
        );
    }

    #[tokio::test]
    async fn run_pause_requires_empty_body_strong_etag_and_idempotency() {
        let now = Utc::now();
        let router = build_run_router(RunHttpState::new(
            Arc::new(FixtureApplication),
            Arc::new(FixedClock(now)),
        ));
        let run_id = id(ResourceKind::Run, 3);
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/runs/{run_id}:pause"))
                    .header("if-match", run_etag(&run_id, 3))
                    .header("idempotency-key", "pause-1")
                    .extension(principal(now))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("etag").unwrap(),
            &run_etag(&run_id, 4)
        );
    }

    #[tokio::test]
    async fn run_result_returns_only_the_typed_terminal_value() {
        let now = Utc::now();
        let router = build_run_router(RunHttpState::new(
            Arc::new(FixtureApplication),
            Arc::new(FixedClock(now)),
        ));
        let run_id = id(ResourceKind::Run, 3);
        let response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/runs/{run_id}/result"))
                    .extension(principal(now))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CACHE_CONTROL).unwrap(),
            "no-store, private, max-age=0"
        );
    }

    #[tokio::test]
    async fn run_signal_requires_a_closed_typed_body_and_scoped_idempotency() {
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
                    .method("POST")
                    .uri(format!("/v1/runs/{run_id}/signals/release"))
                    .header("content-type", "application/json")
                    .header("idempotency-key", "release-1")
                    .extension(principal(now))
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "payload": {
                                "classification": "internal",
                                "schema_digest": digest('b'),
                                "value": {"kind": "inline", "value": {"released": true}}
                            }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers()[CACHE_CONTROL],
            "no-store, private, max-age=0"
        );

        let invalid = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/runs/{run_id}/signals/Release"))
                    .header("content-type", "application/json")
                    .header("idempotency-key", "release-2")
                    .extension(principal(now))
                    .body(Body::from(r#"{"payload":null}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::NOT_FOUND);
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
