//! Principal-scoped MCP OAuth authorization and connection APIs.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use axum::{
    body::Body,
    extract::{rejection::QueryRejection, Path, Query, State},
    http::{HeaderMap, HeaderName, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use chrono::{Duration as ChronoDuration, Utc};
use insight_durable::{
    CompleteMcpOAuthCallbackCommand, ConsumeMcpOAuthTransactionCommand,
    CreateMcpOAuthTransactionCommand, McpInteractionPrincipal, McpOAuthDurableRepository,
    McpOAuthTransactionId, McpProtectedSecret, McpSecretProtector, McpSecretPurpose,
    McpSecretScope, StoreMcpOAuthCredentialCommand,
};
use insight_engine::{ContentHash, TransitionOutcome};
use insight_mcp::{OAuthClient, OAuthClientRegistration, OAuthPkceTransaction};
use serde::{Deserialize, Serialize};

use super::{
    auth::ApiAuth,
    response::{ApiError, ApiResponse},
};

const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const X_TENANT_ID: HeaderName = HeaderName::from_static("x-tenant-id");
const X_USER_ID: HeaderName = HeaderName::from_static("x-user-id");
const TRANSACTION_LIFETIME: ChronoDuration = ChronoDuration::minutes(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthServer {
    pub server_id: String,
    pub resource: String,
    pub client_id: String,
    pub redirect_uri: String,
    pub scopes: Vec<String>,
}

impl McpOAuthServer {
    pub fn new(
        server_id: impl Into<String>,
        resource: impl Into<String>,
        client_id: impl Into<String>,
        redirect_uri: impl Into<String>,
        scopes: Vec<String>,
    ) -> Result<Self, &'static str> {
        let server_id = server_id.into();
        let resource = resource.into();
        let client_id = client_id.into();
        let redirect_uri = redirect_uri.into();
        if !valid_label(&server_id)
            || OAuthClientRegistration::new(client_id.clone(), redirect_uri.clone(), scopes.clone())
                .is_err()
        {
            return Err("invalid MCP OAuth server registration");
        }
        Ok(Self {
            server_id,
            resource,
            client_id,
            redirect_uri,
            scopes,
        })
    }

    fn registration(&self) -> Result<OAuthClientRegistration, ApiError> {
        OAuthClientRegistration::new(
            self.client_id.clone(),
            self.redirect_uri.clone(),
            self.scopes.clone(),
        )
        .map_err(|_| oauth_error("MCP_OAUTH_CONFIGURATION_INVALID"))
    }
}

#[derive(Clone)]
pub struct McpOAuthApiState {
    pub auth: ApiAuth,
    pub repository: Arc<dyn McpOAuthDurableRepository>,
    pub protector: Arc<dyn McpSecretProtector>,
    pub servers: BTreeMap<String, McpOAuthServer>,
    pub client: OAuthClient,
}

impl McpOAuthApiState {
    pub fn new(
        auth: ApiAuth,
        repository: Arc<dyn McpOAuthDurableRepository>,
        protector: Arc<dyn McpSecretProtector>,
        servers: impl IntoIterator<Item = McpOAuthServer>,
        timeout: Duration,
    ) -> Result<Self, &'static str> {
        let mut indexed = BTreeMap::new();
        for server in servers {
            if indexed.insert(server.server_id.clone(), server).is_some() {
                return Err("duplicate MCP OAuth server");
            }
        }
        if indexed.is_empty() {
            return Err("MCP OAuth API requires at least one server");
        }
        let client = OAuthClient::new(timeout).map_err(|_| "invalid MCP OAuth timeout")?;
        Ok(Self {
            auth,
            repository,
            protector,
            servers: indexed,
            client,
        })
    }
}

pub fn build_mcp_oauth_router(state: McpOAuthApiState) -> Router {
    let state = Arc::new(state);
    let auth = state.auth.clone();
    let protected = Router::new()
        .route("/v1/mcp/servers/{server_id}/authorize", post(authorize))
        .route("/v1/mcp/connections", get(list_connections))
        .route("/v1/mcp/connections/{server_id}", delete(delete_connection))
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
        ));
    Router::new()
        .route("/v1/mcp/oauth/callback", get(callback))
        .merge(protected)
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct AuthorizationDto {
    server_id: String,
    authorization_url: String,
    expires_at: chrono::DateTime<Utc>,
}

async fn authorize(
    State(state): State<Arc<McpOAuthApiState>>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = principal(&headers)?;
    let server = state
        .servers
        .get(&server_id)
        .ok_or_else(|| oauth_error("MCP_SERVER_NOT_FOUND"))?;
    let protected = state
        .client
        .discover_protected_resource(&server.resource)
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_METADATA_UNAVAILABLE"))?;
    let issuer = protected
        .authorization_servers
        .first()
        .ok_or_else(|| oauth_error("MCP_OAUTH_METADATA_INVALID"))?;
    let authorization_server = state
        .client
        .discover_authorization_server(issuer)
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_METADATA_UNAVAILABLE"))?;
    let registration = server.registration()?;
    let transaction = state
        .client
        .begin_authorization(&protected, &authorization_server, &registration)
        .map_err(|_| oauth_error("MCP_OAUTH_AUTHORIZATION_INVALID"))?;
    let state_hash = hash(transaction.state().as_bytes());
    let transaction_id = McpOAuthTransactionId::new(format!("oauth_{state_hash}"))
        .map_err(|_| ApiError::internal())?;
    let now = Utc::now();
    let expires_at = now + TRANSACTION_LIFETIME;
    let scope = transaction_scope(&principal, server.server_id.as_str(), &transaction_id)?;
    let transaction_secret = serde_jcs::to_vec(&transaction).map_err(|_| ApiError::internal())?;
    let protected_secret = state
        .protector
        .seal(&scope, &transaction_secret)
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?;
    let (ciphertext, secret_hash) = protected_secret.into_parts();
    let command = CreateMcpOAuthTransactionCommand::new(
        transaction_id,
        principal,
        &server.server_id,
        transaction.issuer(),
        transaction.resource(),
        &server.client_id,
        &server.redirect_uri,
        server.scopes.clone(),
        state_hash,
        ciphertext,
        secret_hash,
        expires_at,
        now,
    )
    .map_err(|_| ApiError::internal())?;
    match state
        .repository
        .create_mcp_oauth_transaction(command)
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?
    {
        TransitionOutcome::Committed { .. } => {
            insight_mcp::record_operational_event(
                &server_id,
                insight_mcp::McpOperationalEvent::OAuthTransactionStarted,
            );
            Ok(no_store(Json(ApiResponse::ok(AuthorizationDto {
                server_id,
                authorization_url: transaction.authorization_url().to_owned(),
                expires_at,
            }))))
        }
        TransitionOutcome::ExactReplay { .. } => {
            Ok(no_store(Json(ApiResponse::ok(AuthorizationDto {
                server_id,
                authorization_url: transaction.authorization_url().to_owned(),
                expires_at,
            }))))
        }
        TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
            Err(oauth_error("MCP_OAUTH_CONFLICT"))
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallbackQuery {
    state: String,
    iss: String,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct CallbackDto {
    server_id: String,
    status: &'static str,
}

async fn callback(
    State(state): State<Arc<McpOAuthApiState>>,
    query: Result<Query<CallbackQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let Query(query) = query.map_err(|_| ApiError::input_invalid())?;
    if query.state.is_empty()
        || query.state.len() > 1024
        || query.iss.is_empty()
        || query.iss.len() > 8 * 1024
        || query
            .code
            .as_ref()
            .is_some_and(|code| code.is_empty() || code.len() > 16 * 1024)
        || query
            .error
            .as_ref()
            .is_some_and(|error| !valid_label(error))
        || query.code.is_some() == query.error.is_some()
    {
        return Err(ApiError::input_invalid());
    }
    let state_hash = hash(query.state.as_bytes());
    let transaction_id = McpOAuthTransactionId::new(format!("oauth_{state_hash}"))
        .map_err(|_| ApiError::internal())?;
    let transaction = state
        .repository
        .load_mcp_oauth_transaction(&transaction_id)
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?
        .ok_or_else(|| oauth_error("MCP_OAUTH_CALLBACK_INVALID"))?;
    let principal = transaction.principal().clone();
    if transaction.expires_at() <= Utc::now()
        || transaction.state_hash() != state_hash
        || transaction.state() != insight_durable::McpOAuthTransactionState::Pending
        || transaction.issuer() != query.iss
    {
        return Err(oauth_error("MCP_OAUTH_CALLBACK_INVALID"));
    }
    if query.error.is_some() {
        consume_transaction(
            &state,
            &transaction,
            principal,
            state_hash,
            "oauth-declined",
        )
        .await?;
        return Err(oauth_error("MCP_OAUTH_DECLINED"));
    }
    let server = state
        .servers
        .get(transaction.server_id())
        .filter(|server| {
            server.client_id == transaction.client_id()
                && server.redirect_uri == transaction.redirect_uri()
                && server.resource == transaction.resource()
                && server.scopes.as_slice() == transaction.scopes()
        })
        .ok_or_else(|| oauth_error("MCP_OAUTH_CONFIGURATION_CHANGED"))?;
    let encrypted = state
        .repository
        .load_mcp_oauth_transaction_secret(&transaction_id)
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?
        .ok_or_else(|| oauth_error("MCP_OAUTH_CALLBACK_INVALID"))?;
    let scope = transaction_scope(&principal, transaction.server_id(), &transaction_id)?;
    let plaintext = state
        .protector
        .open(
            &scope,
            &McpProtectedSecret::new(
                encrypted.transaction_secret,
                encrypted.transaction_secret_hash,
            )
            .map_err(|_| ApiError::internal())?,
        )
        .map_err(|_| oauth_error("MCP_OAUTH_CALLBACK_INVALID"))?;
    let pkce: OAuthPkceTransaction = serde_json::from_slice(&plaintext)
        .map_err(|_| oauth_error("MCP_OAUTH_CALLBACK_INVALID"))?;
    if pkce.issuer() != transaction.issuer()
        || pkce.resource() != transaction.resource()
        || pkce.state() != query.state
    {
        return Err(oauth_error("MCP_OAUTH_CALLBACK_INVALID"));
    }
    let authorization_server = state
        .client
        .discover_authorization_server(transaction.issuer())
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_METADATA_UNAVAILABLE"))?;
    let registration = server.registration()?;
    let token = state
        .client
        .exchange_code(
            &authorization_server,
            &registration,
            &pkce,
            &query.state,
            &query.iss,
            query.code.as_deref().expect("validated callback code"),
        )
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_TOKEN_EXCHANGE_FAILED"))?;
    let current = state
        .repository
        .load_mcp_oauth_credential(&principal, transaction.server_id())
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?;
    let expected_generation = current.as_ref().map(|credential| credential.generation());
    let generation = expected_generation.map_or(1, |generation| generation + 1);
    let authority_id = format!("credential-{generation}");
    let access = protect_token(
        &state,
        &principal,
        transaction.server_id(),
        &authority_id,
        McpSecretPurpose::AccessToken,
        token.access_token(),
    )?;
    let refresh = token
        .refresh_token()
        .map(|refresh| {
            protect_token(
                &state,
                &principal,
                transaction.server_id(),
                &authority_id,
                McpSecretPurpose::RefreshToken,
                refresh,
            )
        })
        .transpose()?;
    let now = Utc::now();
    let (access_ciphertext, access_hash) = access.into_parts();
    let (refresh_ciphertext, refresh_hash) = refresh
        .map(McpProtectedSecret::into_parts)
        .map_or((None, None), |(ciphertext, hash)| {
            (Some(ciphertext), Some(hash))
        });
    let credential = StoreMcpOAuthCredentialCommand::new(
        principal.clone(),
        transaction.server_id(),
        transaction.issuer(),
        transaction.client_id(),
        transaction.resource(),
        token.scopes().to_vec(),
        token.token_type(),
        generation,
        expected_generation,
        format!("oauth-callback-{}", transaction_id.as_str()),
        access_ciphertext,
        access_hash,
        refresh_ciphertext,
        refresh_hash,
        token
            .expires_in()
            .and_then(|duration| ChronoDuration::from_std(duration).ok())
            .map(|duration| now + duration),
        now,
    )
    .map_err(|_| ApiError::internal())?;
    let consume = ConsumeMcpOAuthTransactionCommand::new(
        transaction.transaction_id().clone(),
        principal,
        format!("oauth-complete-{}", transaction.transaction_id().as_str()),
        transaction.version(),
        state_hash,
        now,
    )
    .map_err(|_| ApiError::internal())?;
    let command = CompleteMcpOAuthCallbackCommand::new(consume, credential)
        .map_err(|_| ApiError::internal())?;
    match state
        .repository
        .complete_mcp_oauth_callback(command)
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?
    {
        TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => {}
        TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
            return Err(oauth_error("MCP_OAUTH_CONFLICT"));
        }
    }
    Ok(no_store(Json(ApiResponse::ok(CallbackDto {
        server_id: server.server_id.clone(),
        status: "authorized",
    }))))
}

async fn consume_transaction(
    state: &McpOAuthApiState,
    transaction: &insight_durable::McpOAuthTransaction,
    principal: McpInteractionPrincipal,
    state_hash: String,
    suffix: &str,
) -> Result<(), ApiError> {
    let command = ConsumeMcpOAuthTransactionCommand::new(
        transaction.transaction_id().clone(),
        principal,
        format!("{suffix}-{}", transaction.transaction_id().as_str()),
        transaction.version(),
        state_hash,
        Utc::now(),
    )
    .map_err(|_| ApiError::internal())?;
    match state
        .repository
        .consume_mcp_oauth_transaction(command)
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?
    {
        TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. } => Ok(()),
        TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
            Err(oauth_error("MCP_OAUTH_CALLBACK_INVALID"))
        }
    }
}

#[derive(Debug, Serialize)]
struct ConnectionDto {
    server_id: String,
    status: &'static str,
    issuer: Option<String>,
    resource: String,
    scopes: Vec<String>,
    generation: Option<u64>,
    access_expires_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
}

async fn list_connections(
    State(state): State<Arc<McpOAuthApiState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = principal(&headers)?;
    let mut items = Vec::with_capacity(state.servers.len());
    for server in state.servers.values() {
        let credential = state
            .repository
            .load_mcp_oauth_credential(&principal, &server.server_id)
            .await
            .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?;
        let active = credential
            .as_ref()
            .filter(|credential| credential.revoked_at().is_none());
        items.push(ConnectionDto {
            server_id: server.server_id.clone(),
            status: if active.is_some() {
                "authorized"
            } else {
                "authorization_required"
            },
            issuer: active.map(|credential| credential.issuer().to_owned()),
            resource: server.resource.clone(),
            scopes: active
                .map(|credential| credential.scopes().to_vec())
                .unwrap_or_else(|| server.scopes.clone()),
            generation: active.map(|credential| credential.generation()),
            access_expires_at: active.and_then(|credential| credential.access_expires_at()),
            updated_at: active.map(|credential| credential.updated_at()),
        });
    }
    Ok(no_store(Json(ApiResponse::ok(items))))
}

async fn delete_connection(
    State(state): State<Arc<McpOAuthApiState>>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request_id = required_identity(&headers, &X_REQUEST_ID)?;
    let principal = principal(&headers)?;
    let server = state
        .servers
        .get(&server_id)
        .ok_or_else(|| oauth_error("MCP_SERVER_NOT_FOUND"))?;
    let credential = state
        .repository
        .load_mcp_oauth_credential(&principal, &server_id)
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?
        .ok_or_else(|| oauth_error("MCP_OAUTH_CONNECTION_NOT_FOUND"))?;
    if credential.revoked_at().is_some() {
        return delete_local_connection(&state, &principal, server, server_id, &request_id).await;
    }
    let secret = state
        .repository
        .load_mcp_oauth_credential_secret(&principal, &server_id)
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?
        .ok_or_else(|| oauth_error("MCP_OAUTH_CONNECTION_NOT_FOUND"))?;
    let authority_id = format!("credential-{}", credential.generation());
    let access = open_token(
        &state,
        &principal,
        &server_id,
        &authority_id,
        McpSecretPurpose::AccessToken,
        secret.access_token,
        secret.access_token_hash,
    )
    .ok();
    let refresh = secret
        .refresh_token
        .zip(secret.refresh_token_hash)
        .and_then(|(ciphertext, hash)| {
            open_token(
                &state,
                &principal,
                &server_id,
                &authority_id,
                McpSecretPurpose::RefreshToken,
                ciphertext,
                hash,
            )
            .ok()
        });
    match (
        access.as_deref(),
        state
            .client
            .discover_authorization_server(credential.issuer())
            .await,
    ) {
        (Some(access), Ok(authorization_server))
            if authorization_server.revocation_endpoint.is_some() =>
        {
            let mut revoke_failed = false;
            if let Some(refresh) = refresh.as_deref() {
                revoke_failed |= state
                    .client
                    .revoke(
                        &authorization_server,
                        &server.client_id,
                        refresh,
                        "refresh_token",
                    )
                    .await
                    .is_err();
            }
            revoke_failed |= state
                .client
                .revoke(
                    &authorization_server,
                    &server.client_id,
                    access,
                    "access_token",
                )
                .await
                .is_err();
            if revoke_failed {
                tracing::warn!(
                    server_id = %server_id,
                    "MCP OAuth remote revocation failed before local privacy deletion"
                );
            }
        }
        (Some(_), Ok(_)) => {}
        _ => {
            tracing::warn!(
                server_id = %server_id,
                "MCP OAuth token or metadata was unavailable before local privacy deletion"
            );
        }
    }
    delete_local_connection(&state, &principal, server, server_id, &request_id).await
}

async fn delete_local_connection(
    state: &McpOAuthApiState,
    principal: &McpInteractionPrincipal,
    server: &McpOAuthServer,
    server_id: String,
    request_id: &str,
) -> Result<Response, ApiError> {
    match state
        .repository
        .delete_mcp_oauth_credential(principal, &server_id, request_id, Utc::now())
        .await
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?
    {
        TransitionOutcome::Committed { .. } => {
            insight_mcp::record_operational_event(
                &server_id,
                insight_mcp::McpOperationalEvent::OAuthRevoked,
            );
            Ok(no_store(Json(ApiResponse::ok(ConnectionDto {
                server_id,
                status: "authorization_required",
                issuer: None,
                resource: server.resource.clone(),
                scopes: server.scopes.clone(),
                generation: None,
                access_expires_at: None,
                updated_at: None,
            }))))
        }
        TransitionOutcome::ExactReplay { .. } => {
            Ok(no_store(Json(ApiResponse::ok(ConnectionDto {
                server_id,
                status: "authorization_required",
                issuer: None,
                resource: server.resource.clone(),
                scopes: server.scopes.clone(),
                generation: None,
                access_expires_at: None,
                updated_at: None,
            }))))
        }
        TransitionOutcome::StateConflict | TransitionOutcome::StaleLease => {
            Err(oauth_error("MCP_OAUTH_CONFLICT"))
        }
    }
}

fn protect_token(
    state: &McpOAuthApiState,
    principal: &McpInteractionPrincipal,
    server_id: &str,
    authority_id: &str,
    purpose: McpSecretPurpose,
    token: &str,
) -> Result<McpProtectedSecret, ApiError> {
    let scope = McpSecretScope::new(principal, server_id, authority_id, purpose)
        .map_err(|_| ApiError::internal())?;
    state
        .protector
        .seal(&scope, token.as_bytes())
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))
}

#[allow(clippy::too_many_arguments)]
fn open_token(
    state: &McpOAuthApiState,
    principal: &McpInteractionPrincipal,
    server_id: &str,
    authority_id: &str,
    purpose: McpSecretPurpose,
    ciphertext: insight_durable::McpSecretCiphertext,
    hash: String,
) -> Result<String, ApiError> {
    let scope = McpSecretScope::new(principal, server_id, authority_id, purpose)
        .map_err(|_| ApiError::internal())?;
    let plaintext = state
        .protector
        .open(
            &scope,
            &McpProtectedSecret::new(ciphertext, hash).map_err(|_| ApiError::internal())?,
        )
        .map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))?;
    String::from_utf8(plaintext).map_err(|_| oauth_error("MCP_OAUTH_UNAVAILABLE"))
}

fn transaction_scope(
    principal: &McpInteractionPrincipal,
    server_id: &str,
    transaction_id: &McpOAuthTransactionId,
) -> Result<McpSecretScope, ApiError> {
    McpSecretScope::new(
        principal,
        server_id,
        transaction_id.as_str(),
        McpSecretPurpose::OauthTransaction,
    )
    .map_err(|_| ApiError::internal())
}

fn hash(value: &[u8]) -> String {
    ContentHash::from_bytes(value).as_str().to_owned()
}

fn principal(headers: &HeaderMap) -> Result<McpInteractionPrincipal, ApiError> {
    McpInteractionPrincipal::new(
        required_identity(headers, &X_TENANT_ID)?,
        required_identity(headers, &X_USER_ID)?,
    )
    .map_err(|_| ApiError::input_invalid())
}

fn required_identity(headers: &HeaderMap, name: &HeaderName) -> Result<String, ApiError> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().ok_or_else(ApiError::input_invalid)?;
    if values.next().is_some() {
        return Err(ApiError::input_invalid());
    }
    value
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|value| valid_label(value))
        .map(str::to_owned)
        .ok_or_else(ApiError::input_invalid)
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

fn oauth_error(code: &'static str) -> ApiError {
    let status = match code {
        "MCP_SERVER_NOT_FOUND" | "MCP_OAUTH_CONNECTION_NOT_FOUND" => StatusCode::NOT_FOUND,
        "MCP_OAUTH_CONFLICT" => StatusCode::CONFLICT,
        "MCP_OAUTH_DECLINED" | "MCP_OAUTH_CALLBACK_INVALID" => StatusCode::BAD_REQUEST,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    ApiError::new(status, code, "MCP OAuth request failed")
}

fn no_store(response: impl IntoResponse) -> Response {
    let mut response = response.into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("private, no-store"),
    );
    response
}
