//! Principal-scoped durable MCP interaction API.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{
        rejection::{JsonRejection, QueryRejection},
        Path, Query, State,
    },
    http::{HeaderMap, HeaderName, Request},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use insight_durable::{
    McpInteraction, McpInteractionDisposition, McpInteractionDurableRepository, McpInteractionId,
    McpInteractionListFilter, McpInteractionPrincipal, McpInteractionRequest, McpInteractionState,
    McpProtectedSecret, McpSecretProtector, McpSecretPurpose, McpSecretScope,
    ResolveMcpInteractionCommand,
};
use insight_engine::TransitionOutcome;
use insight_runtime::ServiceError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    auth::ApiAuth,
    response::{ApiError, ApiResponse},
};

const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const X_TENANT_ID: HeaderName = HeaderName::from_static("x-tenant-id");
const X_USER_ID: HeaderName = HeaderName::from_static("x-user-id");
const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 100;

#[derive(Clone)]
pub(crate) struct McpInteractionApiState {
    pub auth: ApiAuth,
    pub repository: Arc<dyn McpInteractionDurableRepository>,
    pub protector: Arc<dyn McpSecretProtector>,
}

pub(crate) fn build_mcp_interaction_router(state: McpInteractionApiState) -> Router {
    let state = Arc::new(state);
    let auth = state.auth.clone();
    Router::new()
        .route("/v1/mcp/interactions", get(list_interactions))
        .route(
            "/v1/mcp/interactions/{interaction_id}",
            get(get_interaction),
        )
        .route(
            "/v1/mcp/interactions/{interaction_id}/respond",
            post(respond_interaction),
        )
        .route(
            "/v1/mcp/interactions/{interaction_id}/open",
            post(open_url_interaction),
        )
        .route(
            "/v1/mcp/interactions/{interaction_id}/decline",
            post(decline_interaction),
        )
        .route(
            "/v1/mcp/interactions/{interaction_id}/cancel",
            post(cancel_interaction),
        )
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap, request: Request<Body>, next: Next| {
                let auth = auth.clone();
                async move {
                    if !auth.accepts(&headers) {
                        return Err(ApiError::unauthorized());
                    }
                    if ambiguous_identity_headers(&headers) {
                        return Err(ApiError::input_invalid());
                    }
                    Ok::<Response, ApiError>(next.run(request).await)
                }
            },
        ))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InteractionQuery {
    run_id: Option<String>,
    status: Option<String>,
    limit: Option<u32>,
    cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct InteractionPage {
    items: Vec<SafeInteractionDto>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct SafeInteractionDto {
    interaction_id: String,
    run_id: String,
    operation_id: String,
    server_id: String,
    source_kind: &'static str,
    mode: insight_durable::McpInteractionMode,
    state: McpInteractionState,
    outcome: Option<insight_durable::McpInteractionOutcome>,
    version: u64,
    request: Value,
    deadline: chrono::DateTime<Utc>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
    closed_at: Option<chrono::DateTime<Utc>>,
    detail_url: String,
}

impl TryFrom<&McpInteraction> for SafeInteractionDto {
    type Error = ApiError;

    fn try_from(interaction: &McpInteraction) -> Result<Self, Self::Error> {
        Ok(Self {
            interaction_id: interaction.interaction_id().as_str().to_owned(),
            run_id: interaction.run_id().to_owned(),
            operation_id: interaction.operation_id().to_owned(),
            server_id: interaction.server_id().to_owned(),
            source_kind: "mcp",
            mode: interaction.request().mode(),
            state: interaction.state(),
            outcome: interaction.outcome(),
            version: interaction.version(),
            request: serde_json::to_value(interaction.request())
                .map_err(|_| ApiError::internal())?,
            deadline: interaction.deadline(),
            created_at: interaction.created_at(),
            updated_at: interaction.updated_at(),
            closed_at: interaction.closed_at(),
            detail_url: format!(
                "/v1/mcp/interactions/{}",
                interaction.interaction_id().as_str()
            ),
        })
    }
}

async fn list_interactions(
    State(state): State<Arc<McpInteractionApiState>>,
    headers: HeaderMap,
    query: Result<Query<InteractionQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let Query(query) = query.map_err(|_| ApiError::input_invalid())?;
    let principal = interaction_principal(&headers)?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0
        || limit > MAX_LIMIT
        || query
            .run_id
            .as_ref()
            .is_some_and(|value| !valid_label(value))
        || query
            .cursor
            .as_ref()
            .is_some_and(|value| !valid_label(value))
    {
        return Err(ApiError::input_invalid());
    }
    let state_filter = query
        .status
        .as_deref()
        .map(parse_state)
        .transpose()
        .ok()
        .flatten();
    if query.status.is_some() && state_filter.is_none() {
        return Err(ApiError::input_invalid());
    }
    let items = state
        .repository
        .list_mcp_interactions(
            &principal,
            &McpInteractionListFilter {
                run_id: query.run_id,
                state: state_filter,
                after_interaction_id: query.cursor,
            },
            limit,
        )
        .await
        .map_err(|_| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?;
    let next_cursor = (items.len() == usize::try_from(limit).unwrap_or(usize::MAX))
        .then(|| {
            items
                .last()
                .map(|item| item.interaction_id().as_str().to_owned())
        })
        .flatten();
    let items = items
        .iter()
        .map(SafeInteractionDto::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(no_store(Json(ApiResponse::ok(InteractionPage {
        items,
        next_cursor,
    }))))
}

async fn get_interaction(
    State(state): State<Arc<McpInteractionApiState>>,
    Path(interaction_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = interaction_principal(&headers)?;
    let interaction = load_owned_interaction(&state, &principal, &interaction_id).await?;
    Ok(no_store(Json(ApiResponse::ok(
        SafeInteractionDto::try_from(&interaction)?,
    ))))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RespondRequest {
    version: u64,
    response: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveRequest {
    version: u64,
}

#[derive(Debug, Serialize)]
struct OpenUrlResponse {
    url: String,
    expires_at: chrono::DateTime<Utc>,
}

async fn open_url_interaction(
    State(state): State<Arc<McpInteractionApiState>>,
    Path(interaction_id): Path<String>,
    headers: HeaderMap,
    body: Result<Json<ResolveRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    required_request_id(&headers)?;
    let principal = interaction_principal(&headers)?;
    let interaction = load_owned_interaction(&state, &principal, &interaction_id).await?;
    let Json(body) = body.map_err(|_| ApiError::input_invalid())?;
    let McpInteractionRequest::Url {
        scheme, host, port, ..
    } = interaction.request()
    else {
        return Err(mcp_api_error("MCP_INTERACTION_MODE_INVALID"));
    };
    if interaction.state() != McpInteractionState::Requested
        || interaction.version() != body.version
        || interaction.deadline() <= Utc::now()
    {
        return Err(mcp_api_error("MCP_INTERACTION_CONFLICT"));
    }
    let secret = state
        .repository
        .load_mcp_interaction_secret(interaction.interaction_id())
        .await
        .map_err(|_| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?
        .ok_or_else(|| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?;
    let protected = McpProtectedSecret::new(secret.request_secret, secret.request_hash)
        .map_err(|_| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?;
    let scope = McpSecretScope::new(
        &principal,
        interaction.server_id(),
        interaction.interaction_id().as_str(),
        McpSecretPurpose::ElicitationRequest,
    )
    .map_err(|_| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?;
    let plaintext = state
        .protector
        .open(&scope, &protected)
        .map_err(|_| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?;
    let value: Value = serde_json::from_slice(&plaintext)
        .map_err(|_| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?;
    let url = value
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?;
    if !revealed_url_matches(url, scheme, host, *port) {
        return Err(mcp_api_error("MCP_INTERACTION_UNAVAILABLE"));
    }
    Ok(no_store(Json(ApiResponse::ok(OpenUrlResponse {
        url: url.to_owned(),
        expires_at: interaction.deadline(),
    }))))
}

fn revealed_url_matches(url: &str, scheme: &str, host: &str, port: Option<u16>) -> bool {
    reqwest::Url::parse(url).is_ok_and(|parsed| {
        parsed.scheme() == scheme
            && parsed.host_str() == Some(host)
            && parsed.port() == port
            && parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.fragment().is_none()
    })
}

async fn respond_interaction(
    State(state): State<Arc<McpInteractionApiState>>,
    Path(interaction_id): Path<String>,
    headers: HeaderMap,
    body: Result<Json<RespondRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let request_id = required_request_id(&headers)?;
    let principal = interaction_principal(&headers)?;
    let interaction = load_owned_interaction(&state, &principal, &interaction_id).await?;
    let Json(body) = body.map_err(|_| ApiError::input_invalid())?;
    interaction
        .request()
        .validate_response(&body.response)
        .map_err(|_| mcp_api_error("MCP_INTERACTION_RESPONSE_INVALID"))?;
    let plaintext = serde_jcs::to_vec(&body.response).map_err(|_| ApiError::input_invalid())?;
    let scope = McpSecretScope::new(
        &principal,
        interaction.server_id(),
        interaction.interaction_id().as_str(),
        McpSecretPurpose::ElicitationResponse,
    )
    .map_err(|_| ApiError::input_invalid())?;
    let protected = state
        .protector
        .seal(&scope, &plaintext)
        .map_err(|_| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?;
    let (ciphertext, content_hash) = protected.into_parts();
    let command = ResolveMcpInteractionCommand::new(
        &interaction,
        principal,
        request_id,
        body.version,
        McpInteractionDisposition::Accept,
        Some(&body.response),
        Some(ciphertext),
        Some(content_hash),
        Utc::now(),
    )
    .map_err(|_| mcp_api_error("MCP_INTERACTION_RESPONSE_INVALID"))?;
    resolve(&state, command).await
}

async fn decline_interaction(
    State(state): State<Arc<McpInteractionApiState>>,
    Path(interaction_id): Path<String>,
    headers: HeaderMap,
    body: Result<Json<ResolveRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    resolve_without_body(
        state,
        interaction_id,
        headers,
        body,
        McpInteractionDisposition::Decline,
    )
    .await
}

async fn cancel_interaction(
    State(state): State<Arc<McpInteractionApiState>>,
    Path(interaction_id): Path<String>,
    headers: HeaderMap,
    body: Result<Json<ResolveRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    resolve_without_body(
        state,
        interaction_id,
        headers,
        body,
        McpInteractionDisposition::Cancel,
    )
    .await
}

async fn resolve_without_body(
    state: Arc<McpInteractionApiState>,
    interaction_id: String,
    headers: HeaderMap,
    body: Result<Json<ResolveRequest>, JsonRejection>,
    disposition: McpInteractionDisposition,
) -> Result<Response, ApiError> {
    let request_id = required_request_id(&headers)?;
    let principal = interaction_principal(&headers)?;
    let interaction = load_owned_interaction(&state, &principal, &interaction_id).await?;
    let Json(body) = body.map_err(|_| ApiError::input_invalid())?;
    let command = ResolveMcpInteractionCommand::new(
        &interaction,
        principal,
        request_id,
        body.version,
        disposition,
        None,
        None,
        None,
        Utc::now(),
    )
    .map_err(|_| mcp_api_error("MCP_INTERACTION_CONFLICT"))?;
    resolve(&state, command).await
}

async fn resolve(
    state: &McpInteractionApiState,
    command: ResolveMcpInteractionCommand,
) -> Result<Response, ApiError> {
    match state
        .repository
        .resolve_mcp_interaction(command)
        .await
        .map_err(|_| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?
    {
        TransitionOutcome::Committed { result } => {
            if let Some(event) = result.outcome().map(|outcome| match outcome {
                insight_durable::McpInteractionOutcome::Accepted => {
                    insight_mcp::McpOperationalEvent::InteractionAccepted
                }
                insight_durable::McpInteractionOutcome::Declined => {
                    insight_mcp::McpOperationalEvent::InteractionDeclined
                }
                insight_durable::McpInteractionOutcome::Cancelled => {
                    insight_mcp::McpOperationalEvent::InteractionCancelled
                }
                insight_durable::McpInteractionOutcome::Expired => {
                    insight_mcp::McpOperationalEvent::InteractionExpired
                }
                insight_durable::McpInteractionOutcome::RunTerminal => {
                    insight_mcp::McpOperationalEvent::InteractionRunTerminal
                }
                insight_durable::McpInteractionOutcome::RetryCompleted => {
                    insight_mcp::McpOperationalEvent::InteractionRetryCompleted
                }
                insight_durable::McpInteractionOutcome::RetryFailed => {
                    insight_mcp::McpOperationalEvent::InteractionRetryFailed
                }
            }) {
                insight_mcp::record_operational_event(result.server_id(), event);
            }
            if result.state() == insight_durable::McpInteractionState::Closed {
                insight_mcp::adjust_operational_gauge(
                    result.server_id(),
                    insight_mcp::McpOperationalGauge::OpenInteractions,
                    -1,
                );
            }
            Ok(no_store(Json(ApiResponse::ok(
                SafeInteractionDto::try_from(&result)?,
            ))))
        }
        TransitionOutcome::ExactReplay {
            authoritative: result,
        } => Ok(no_store(Json(ApiResponse::ok(
            SafeInteractionDto::try_from(&result)?,
        )))),
        TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
            Err(mcp_api_error("MCP_INTERACTION_CONFLICT"))
        }
    }
}

async fn load_owned_interaction(
    state: &McpInteractionApiState,
    principal: &McpInteractionPrincipal,
    interaction_id: &str,
) -> Result<McpInteraction, ApiError> {
    let interaction_id =
        McpInteractionId::new(interaction_id).map_err(|_| ApiError::input_invalid())?;
    state
        .repository
        .load_mcp_interaction(&interaction_id)
        .await
        .map_err(|_| mcp_api_error("MCP_INTERACTION_UNAVAILABLE"))?
        .filter(|interaction| interaction.principal() == principal)
        .ok_or_else(|| mcp_api_error("MCP_INTERACTION_NOT_FOUND"))
}

fn interaction_principal(headers: &HeaderMap) -> Result<McpInteractionPrincipal, ApiError> {
    McpInteractionPrincipal::new(
        required_identity(headers, &X_TENANT_ID)?,
        required_identity(headers, &X_USER_ID)?,
    )
    .map_err(|_| ApiError::input_invalid())
}

fn required_request_id(headers: &HeaderMap) -> Result<String, ApiError> {
    required_identity(headers, &X_REQUEST_ID)
}

fn required_identity(headers: &HeaderMap, name: &HeaderName) -> Result<String, ApiError> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().ok_or_else(ApiError::input_invalid)?;
    if values.next().is_some() {
        return Err(ApiError::input_invalid());
    }
    let value = value
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|value| valid_label(value))
        .ok_or_else(ApiError::input_invalid)?;
    Ok(value.to_owned())
}

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.contains(',')
        && !value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
}

fn ambiguous_identity_headers(headers: &HeaderMap) -> bool {
    [&X_TENANT_ID, &X_USER_ID, &X_REQUEST_ID]
        .into_iter()
        .any(|name| {
            let mut values = headers.get_all(name).iter();
            values.next().is_some_and(|first| {
                values.next().is_some() || first.to_str().map_or(true, |value| value.contains(','))
            })
        })
}

fn parse_state(value: &str) -> Result<McpInteractionState, ()> {
    match value {
        "requested" => Ok(McpInteractionState::Requested),
        "responded" => Ok(McpInteractionState::Responded),
        "retrying" => Ok(McpInteractionState::Retrying),
        "closed" => Ok(McpInteractionState::Closed),
        _ => Err(()),
    }
}

fn mcp_api_error(code: &'static str) -> ApiError {
    ApiError::from(ServiceError::new(code, "MCP interaction request failed"))
}

fn no_store(response: impl IntoResponse) -> Response {
    let mut response = response.into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("private, no-store"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::revealed_url_matches;

    #[test]
    fn revealed_url_remains_bound_to_the_safe_interaction_authority() {
        assert!(revealed_url_matches(
            "https://login.example.test/authorize?state=opaque",
            "https",
            "login.example.test",
            None,
        ));
        assert!(!revealed_url_matches(
            "https://attacker.example.test/authorize",
            "https",
            "login.example.test",
            None,
        ));
        assert!(!revealed_url_matches(
            "https://login.example.test/authorize#token",
            "https",
            "login.example.test",
            None,
        ));
    }
}
