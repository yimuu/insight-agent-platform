//! Modern MCP Streamable HTTP endpoint adapter.

use std::{
    collections::BTreeSet,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body, Bytes},
    extract::State,
    http::{header, HeaderMap, HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use futures::StreamExt as _;
use insight_mcp::{
    build_pinned_http_client, McpRequestHeaders, McpServerBackend, McpServerDispatcher,
    McpServerReply, OAuthClient, OAuthProtectedResourceMetadata,
};
use jsonwebtoken::{
    decode, decode_header, get_current_timestamp,
    jwk::{Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse},
    DecodingKey, Validation,
};
use reqwest::Url;
use serde_json::Value;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;

use crate::v1::ApiAuth;

const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpHttpPrincipal {
    subject: String,
    authorization_scopes: Option<BTreeSet<String>>,
}

impl McpHttpPrincipal {
    pub fn new(subject: impl Into<String>) -> Option<Self> {
        let subject = subject.into();
        (!subject.is_empty() && subject.len() <= 512 && !subject.chars().any(char::is_control))
            .then_some(Self {
                subject,
                authorization_scopes: None,
            })
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    fn with_authorization_scopes(mut self, scopes: BTreeSet<String>) -> Self {
        self.authorization_scopes = Some(scopes);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpHttpAuthorizationError {
    MissingToken,
    InvalidToken,
    InsufficientScope,
    Unavailable,
}

#[async_trait]
pub trait McpHttpAuthorizer: Send + Sync {
    async fn authorize(
        &self,
        headers: &HeaderMap,
    ) -> Result<McpHttpPrincipal, McpHttpAuthorizationError>;

    fn challenge(&self, error: McpHttpAuthorizationError) -> String {
        match error {
            McpHttpAuthorizationError::InsufficientScope => {
                "Bearer error=\"insufficient_scope\"".to_owned()
            }
            McpHttpAuthorizationError::InvalidToken => "Bearer error=\"invalid_token\"".to_owned(),
            McpHttpAuthorizationError::MissingToken | McpHttpAuthorizationError::Unavailable => {
                "Bearer".to_owned()
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct DisabledMcpHttpAuthorizer;

#[async_trait]
impl McpHttpAuthorizer for DisabledMcpHttpAuthorizer {
    async fn authorize(
        &self,
        _headers: &HeaderMap,
    ) -> Result<McpHttpPrincipal, McpHttpAuthorizationError> {
        Ok(McpHttpPrincipal {
            subject: "default/service".to_owned(),
            authorization_scopes: None,
        })
    }
}

#[derive(Clone)]
pub struct ApiAuthMcpHttpAuthorizer {
    auth: ApiAuth,
}

impl ApiAuthMcpHttpAuthorizer {
    pub fn new(auth: ApiAuth) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl McpHttpAuthorizer for ApiAuthMcpHttpAuthorizer {
    async fn authorize(
        &self,
        headers: &HeaderMap,
    ) -> Result<McpHttpPrincipal, McpHttpAuthorizationError> {
        if !self.auth.accepts(headers) {
            return Err(McpHttpAuthorizationError::InvalidToken);
        }
        let tenant = optional_single_header(headers, "x-tenant-id")
            .map_err(|_| McpHttpAuthorizationError::InvalidToken)?
            .unwrap_or("default");
        let user = optional_single_header(headers, "x-user-id")
            .map_err(|_| McpHttpAuthorizationError::InvalidToken)?;
        let subject = user.map_or_else(|| tenant.to_owned(), |user| format!("{tenant}/{user}"));
        McpHttpPrincipal::new(subject).ok_or(McpHttpAuthorizationError::InvalidToken)
    }
}

impl fmt::Debug for ApiAuthMcpHttpAuthorizer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiAuthMcpHttpAuthorizer")
            .field("auth", &self.auth)
            .finish()
    }
}

pub struct StaticBearerMcpHttpAuthorizer {
    token: String,
    principal: McpHttpPrincipal,
}

impl StaticBearerMcpHttpAuthorizer {
    pub fn new(token: impl Into<String>, principal: McpHttpPrincipal) -> Option<Self> {
        let token = token.into();
        (!token.is_empty() && !token.chars().any(char::is_control))
            .then_some(Self { token, principal })
    }
}

#[async_trait]
impl McpHttpAuthorizer for StaticBearerMcpHttpAuthorizer {
    async fn authorize(
        &self,
        headers: &HeaderMap,
    ) -> Result<McpHttpPrincipal, McpHttpAuthorizationError> {
        let candidate = single_header(headers, header::AUTHORIZATION.as_str())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or(McpHttpAuthorizationError::MissingToken)?;
        constant_time_eq(candidate.as_bytes(), self.token.as_bytes())
            .then(|| self.principal.clone())
            .ok_or(McpHttpAuthorizationError::InvalidToken)
    }
}

impl fmt::Debug for StaticBearerMcpHttpAuthorizer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticBearerMcpHttpAuthorizer")
            .field("token", &"[REDACTED]")
            .field("principal", &self.principal)
            .finish()
    }
}

#[derive(Debug)]
struct CachedJwks {
    value: JwkSet,
    fetched_at: Instant,
}

#[derive(Debug)]
struct OAuthIssuerKeys {
    issuer: String,
    jwks_uri: Url,
    keys: RwLock<CachedJwks>,
}

pub struct OAuthResourceServerAuthorizer {
    resource: String,
    resource_metadata_url: String,
    required_scopes: BTreeSet<String>,
    issuers: Vec<Arc<OAuthIssuerKeys>>,
    http_timeout: Duration,
}

impl OAuthResourceServerAuthorizer {
    pub async fn discover(
        resource: impl Into<String>,
        authorization_servers: Vec<String>,
        required_scopes: BTreeSet<String>,
        timeout: Duration,
    ) -> Result<Self, &'static str> {
        let resource = resource.into();
        let resource_url = secure_remote_url(&resource)?;
        if resource_url.query().is_some() {
            return Err("invalid MCP OAuth resource");
        }
        if authorization_servers.is_empty()
            || authorization_servers.len() > 16
            || required_scopes.len() > 128
            || required_scopes.iter().any(|scope| !valid_scope(scope))
        {
            return Err("invalid MCP OAuth resource-server configuration");
        }
        let resource_metadata_url =
            protected_resource_metadata_url(&resource_url).ok_or("invalid MCP OAuth resource")?;
        let discovery =
            OAuthClient::new(timeout).map_err(|_| "invalid MCP OAuth discovery configuration")?;
        let mut issuers = Vec::with_capacity(authorization_servers.len());
        let mut seen = BTreeSet::new();
        for configured_issuer in authorization_servers {
            if !seen.insert(configured_issuer.clone()) {
                return Err("duplicate MCP OAuth authorization server");
            }
            secure_remote_url(&configured_issuer)?;
            let metadata = discovery
                .discover_authorization_server(&configured_issuer)
                .await
                .map_err(|_| "MCP OAuth authorization-server discovery failed")?;
            if metadata
                .extensions
                .get("protected_resources")
                .and_then(Value::as_array)
                .is_some_and(|resources| {
                    !resources
                        .iter()
                        .any(|candidate| candidate.as_str() == Some(resource.as_str()))
                })
            {
                return Err("MCP OAuth authorization server does not recognize the resource");
            }
            let jwks_uri = metadata
                .extensions
                .get("jwks_uri")
                .and_then(Value::as_str)
                .ok_or("MCP OAuth authorization server omits jwks_uri")
                .and_then(secure_remote_url)?;
            let value = fetch_jwks(timeout, jwks_uri.clone()).await?;
            issuers.push(Arc::new(OAuthIssuerKeys {
                issuer: metadata.issuer,
                jwks_uri,
                keys: RwLock::new(CachedJwks {
                    value,
                    fetched_at: Instant::now(),
                }),
            }));
        }
        Ok(Self {
            resource,
            resource_metadata_url,
            required_scopes,
            issuers,
            http_timeout: timeout,
        })
    }

    async fn verify_token(
        &self,
        token: &str,
    ) -> Result<McpHttpPrincipal, McpHttpAuthorizationError> {
        if token.is_empty() || token.len() > 128 * 1024 || token.chars().any(char::is_whitespace) {
            return Err(McpHttpAuthorizationError::InvalidToken);
        }
        let header = decode_header(token).map_err(|_| McpHttpAuthorizationError::InvalidToken)?;
        if !matches!(header.typ.as_deref(), Some("at+jwt" | "application/at+jwt"))
            || matches!(
                header.alg,
                jsonwebtoken::Algorithm::HS256
                    | jsonwebtoken::Algorithm::HS384
                    | jsonwebtoken::Algorithm::HS512
            )
        {
            return Err(McpHttpAuthorizationError::InvalidToken);
        }
        let kid = header
            .kid
            .as_deref()
            .filter(|kid| !kid.is_empty() && kid.len() <= 512 && !kid.chars().any(char::is_control))
            .ok_or(McpHttpAuthorizationError::InvalidToken)?;
        let mut unavailable = false;
        for issuer in &self.issuers {
            let jwk = match issuer.key(kid, self.http_timeout).await {
                Ok(Some(jwk)) => jwk,
                Ok(None) => continue,
                Err(()) => {
                    unavailable = true;
                    continue;
                }
            };
            if !jwk_is_valid_signature_key(&jwk, header.alg) {
                continue;
            }
            let key = match DecodingKey::from_jwk(&jwk) {
                Ok(key) => key,
                Err(_) => continue,
            };
            let mut validation = Validation::new(header.alg);
            validation.set_audience(&[&self.resource]);
            validation.set_issuer(&[&issuer.issuer]);
            validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
            validation.leeway = 30;
            validation.validate_nbf = true;
            let claims = match decode::<Value>(token, &key, &validation) {
                Ok(token) => token.claims,
                Err(_) => continue,
            };
            if !valid_access_token_claims(&claims, get_current_timestamp()) {
                continue;
            }
            let granted_scopes =
                access_token_scopes(&claims).ok_or(McpHttpAuthorizationError::InvalidToken)?;
            if !self
                .required_scopes
                .iter()
                .all(|scope| granted_scopes.contains(scope))
            {
                return Err(McpHttpAuthorizationError::InsufficientScope);
            }
            let tenant = claims
                .get("tenant_id")
                .or_else(|| claims.get("tid"))
                .and_then(Value::as_str)
                .filter(|value| valid_principal_component(value))
                .ok_or(McpHttpAuthorizationError::InvalidToken)?;
            let user = claims
                .get("user_id")
                .or_else(|| claims.get("sub"))
                .and_then(Value::as_str)
                .filter(|value| valid_principal_component(value))
                .ok_or(McpHttpAuthorizationError::InvalidToken)?;
            return McpHttpPrincipal::new(format!("{tenant}/{user}"))
                .map(|principal| principal.with_authorization_scopes(granted_scopes))
                .ok_or(McpHttpAuthorizationError::InvalidToken);
        }
        if unavailable {
            Err(McpHttpAuthorizationError::Unavailable)
        } else {
            Err(McpHttpAuthorizationError::InvalidToken)
        }
    }
}

impl fmt::Debug for OAuthResourceServerAuthorizer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthResourceServerAuthorizer")
            .field("resource", &self.resource)
            .field("required_scopes", &self.required_scopes)
            .field(
                "issuers",
                &self
                    .issuers
                    .iter()
                    .map(|issuer| issuer.issuer.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[async_trait]
impl McpHttpAuthorizer for OAuthResourceServerAuthorizer {
    async fn authorize(
        &self,
        headers: &HeaderMap,
    ) -> Result<McpHttpPrincipal, McpHttpAuthorizationError> {
        let authorization = single_header(headers, header::AUTHORIZATION.as_str())
            .ok_or(McpHttpAuthorizationError::MissingToken)?;
        let token = authorization
            .strip_prefix("Bearer ")
            .filter(|token| !token.is_empty())
            .ok_or(McpHttpAuthorizationError::InvalidToken)?;
        self.verify_token(token).await
    }

    fn challenge(&self, error: McpHttpAuthorizationError) -> String {
        let mut challenge = format!(
            "Bearer resource_metadata=\"{}\"",
            self.resource_metadata_url
        );
        match error {
            McpHttpAuthorizationError::InvalidToken => {
                challenge.push_str(", error=\"invalid_token\"");
            }
            McpHttpAuthorizationError::InsufficientScope => {
                challenge.push_str(", error=\"insufficient_scope\"");
                if !self.required_scopes.is_empty() {
                    challenge.push_str(", scope=\"");
                    challenge.push_str(
                        &self
                            .required_scopes
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(" "),
                    );
                    challenge.push('"');
                }
            }
            McpHttpAuthorizationError::MissingToken | McpHttpAuthorizationError::Unavailable => {}
        }
        challenge
    }
}

impl OAuthIssuerKeys {
    async fn key(
        &self,
        kid: &str,
        http_timeout: Duration,
    ) -> Result<Option<jsonwebtoken::jwk::Jwk>, ()> {
        if let Some(key) = self.keys.read().await.value.find(kid).cloned() {
            return Ok(Some(key));
        }
        {
            let cached = self.keys.read().await;
            if cached.fetched_at.elapsed() < Duration::from_secs(30) {
                return Ok(None);
            }
        }
        let refreshed = fetch_jwks(http_timeout, self.jwks_uri.clone())
            .await
            .map_err(|_| ())?;
        let key = refreshed.find(kid).cloned();
        *self.keys.write().await = CachedJwks {
            value: refreshed,
            fetched_at: Instant::now(),
        };
        Ok(key)
    }
}

pub fn oauth_protected_resource_metadata(
    resource: impl Into<String>,
    authorization_servers: Vec<String>,
    scopes_supported: BTreeSet<String>,
) -> Result<OAuthProtectedResourceMetadata, &'static str> {
    let resource = resource.into();
    if secure_remote_url(&resource)?.query().is_some() {
        return Err("invalid MCP protected resource");
    }
    if authorization_servers.is_empty()
        || authorization_servers.len() > 16
        || scopes_supported.len() > 128
        || scopes_supported.iter().any(|scope| !valid_scope(scope))
    {
        return Err("invalid MCP protected-resource metadata");
    }
    for issuer in &authorization_servers {
        secure_remote_url(issuer)?;
    }
    Ok(OAuthProtectedResourceMetadata {
        resource,
        authorization_servers,
        scopes_supported: scopes_supported.into_iter().collect(),
        bearer_methods_supported: vec!["header".to_owned()],
        extensions: Default::default(),
    })
}

pub fn build_mcp_protected_resource_metadata_router(
    metadata: OAuthProtectedResourceMetadata,
) -> Result<Router, &'static str> {
    let resource = secure_remote_url(&metadata.resource)?;
    let path =
        protected_resource_metadata_path(&resource).ok_or("invalid MCP protected resource path")?;
    Ok(Router::new().route(
        &path,
        get(move || {
            let metadata = metadata.clone();
            async move {
                let mut response = axum::Json(metadata).into_response();
                no_store(&mut response);
                response
            }
        }),
    ))
}

#[async_trait]
pub trait McpHttpService: Send + Sync {
    async fn dispatch(
        &self,
        body: &[u8],
        headers: &McpRequestHeaders,
        authorization_subject: String,
        authorization_scopes: Option<BTreeSet<String>>,
    ) -> McpServerReply;

    async fn open_subscription(
        &self,
        body: &[u8],
        headers: &McpRequestHeaders,
        authorization_subject: String,
        authorization_scopes: Option<BTreeSet<String>>,
    ) -> Result<
        tokio::sync::mpsc::Receiver<insight_mcp::JsonRpcNotification<Value>>,
        McpServerErrorMarker,
    >;
}

#[derive(Debug, Clone, Copy)]
pub struct McpServerErrorMarker;

#[async_trait]
impl<B> McpHttpService for McpServerDispatcher<B>
where
    B: McpServerBackend,
{
    async fn dispatch(
        &self,
        body: &[u8],
        headers: &McpRequestHeaders,
        authorization_subject: String,
        authorization_scopes: Option<BTreeSet<String>>,
    ) -> McpServerReply {
        self.dispatch_with_authorization(
            body,
            headers,
            Some(authorization_subject),
            authorization_scopes,
        )
        .await
    }

    async fn open_subscription(
        &self,
        body: &[u8],
        headers: &McpRequestHeaders,
        authorization_subject: String,
        authorization_scopes: Option<BTreeSet<String>>,
    ) -> Result<
        tokio::sync::mpsc::Receiver<insight_mcp::JsonRpcNotification<Value>>,
        McpServerErrorMarker,
    > {
        self.open_subscription_with_authorization(
            body,
            headers,
            Some(authorization_subject),
            authorization_scopes,
        )
        .await
        .map_err(|_| McpServerErrorMarker)
    }
}

#[derive(Clone)]
struct McpHttpState {
    service: Arc<dyn McpHttpService>,
    authorizer: Arc<dyn McpHttpAuthorizer>,
    max_body_bytes: usize,
}

pub fn build_mcp_server_router(
    endpoint: &str,
    service: Arc<dyn McpHttpService>,
    authorizer: Arc<dyn McpHttpAuthorizer>,
    max_body_bytes: Option<usize>,
) -> Result<Router, &'static str> {
    if !endpoint.starts_with('/')
        || endpoint.len() > 256
        || endpoint.contains('?')
        || endpoint.contains('#')
    {
        return Err("invalid MCP HTTP endpoint");
    }
    let max_body_bytes = max_body_bytes.unwrap_or(DEFAULT_MAX_BODY_BYTES);
    if max_body_bytes == 0 || max_body_bytes > 256 * 1024 * 1024 {
        return Err("invalid MCP HTTP body limit");
    }
    Ok(Router::new()
        .route(endpoint, post(handle_mcp))
        .with_state(Arc::new(McpHttpState {
            service,
            authorizer,
            max_body_bytes,
        })))
}

async fn handle_mcp(
    State(state): State<Arc<McpHttpState>>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Response {
    let principal = match state.authorizer.authorize(&headers).await {
        Ok(principal) => principal,
        Err(error) => return unauthorized(&state.authorizer.challenge(error)),
    };
    let request_headers = match protocol_headers(&headers) {
        Ok(headers) => headers,
        Err(()) => return bad_request(),
    };
    if !valid_content_negotiation(&headers) {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }
    let body = match to_bytes(request.into_body(), state.max_body_bytes).await {
        Ok(body) if !body.is_empty() => body,
        _ => {
            insight_mcp::record_operational_event(
                "platform",
                insight_mcp::McpOperationalEvent::BodyLimitRejected,
            );
            return StatusCode::PAYLOAD_TOO_LARGE.into_response();
        }
    };
    match state
        .service
        .dispatch(
            &body,
            &request_headers,
            principal.subject.clone(),
            principal.authorization_scopes.clone(),
        )
        .await
    {
        McpServerReply::Json(value) => {
            let mut response = axum::Json(value).into_response();
            no_store(&mut response);
            response
        }
        McpServerReply::SubscriptionAcknowledgement(notification) => {
            let acknowledgement = match encode_sse_notification(&notification) {
                Ok(data) => data,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            let receiver = match state
                .service
                .open_subscription(
                    &body,
                    &request_headers,
                    principal.subject,
                    principal.authorization_scopes,
                )
                .await
            {
                Ok(receiver) => receiver,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            let notifications = ReceiverStream::new(receiver)
                .map(|notification| encode_sse_notification(&notification).map(Bytes::from));
            let stream = futures::stream::once(async move {
                Ok::<Bytes, std::io::Error>(Bytes::from(acknowledgement))
            })
            .chain(notifications);
            let mut response = Body::from_stream(stream).into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream; charset=utf-8"),
            );
            no_store(&mut response);
            response
        }
    }
}

fn encode_sse_notification(
    notification: &insight_mcp::JsonRpcNotification<Value>,
) -> Result<String, std::io::Error> {
    serde_json::to_string(notification)
        .map(|data| format!("event: message\ndata: {data}\n\n"))
        .map_err(std::io::Error::other)
}

fn protocol_headers(headers: &HeaderMap) -> Result<McpRequestHeaders, ()> {
    let protocol_version = single_header(headers, "mcp-protocol-version").ok_or(())?;
    let method = single_header(headers, "mcp-method").ok_or(())?;
    let name = optional_single_header(headers, "mcp-name")?;
    Ok(McpRequestHeaders {
        protocol_version: protocol_version.to_owned(),
        method: method.to_owned(),
        name: name.map(str::to_owned),
    })
}

fn valid_content_negotiation(headers: &HeaderMap) -> bool {
    let content_type = single_header(headers, header::CONTENT_TYPE.as_str())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    let accept = single_header(headers, header::ACCEPT.as_str());
    content_type == Some("application/json")
        && accept.is_some_and(|value| {
            let values = value.split(',').map(str::trim).collect::<Vec<_>>();
            values.contains(&"application/json") && values.contains(&"text/event-stream")
        })
}

async fn fetch_jwks(timeout: Duration, url: Url) -> Result<JwkSet, &'static str> {
    const MAX_JWKS_BYTES: usize = 256 * 1024;
    let http = build_pinned_http_client(&url, timeout.min(Duration::from_secs(10)), timeout, false)
        .map_err(|_| "MCP OAuth JWKS endpoint violates network policy")?;
    let response = http
        .get(url)
        .header(header::ACCEPT.as_str(), "application/json")
        .send()
        .await
        .map_err(|_| "MCP OAuth JWKS request failed")?;
    if !response.status().is_success()
        || response
            .headers()
            .get(header::CONTENT_TYPE.as_str())
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            != Some("application/json")
    {
        return Err("MCP OAuth JWKS response is invalid");
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "MCP OAuth JWKS response is invalid")?;
        if body.len().saturating_add(chunk.len()) > MAX_JWKS_BYTES {
            return Err("MCP OAuth JWKS response is too large");
        }
        body.extend_from_slice(&chunk);
    }
    let keys: JwkSet =
        serde_json::from_slice(&body).map_err(|_| "MCP OAuth JWKS response is invalid")?;
    if keys.keys.is_empty() || keys.keys.len() > 64 {
        return Err("MCP OAuth JWKS response has an invalid key count");
    }
    Ok(keys)
}

fn secure_remote_url(value: &str) -> Result<Url, &'static str> {
    if value.is_empty() || value.len() > 8 * 1024 || value.chars().any(char::is_control) {
        return Err("invalid secure MCP OAuth URL");
    }
    let url = Url::parse(value).map_err(|_| "invalid secure MCP OAuth URL")?;
    if url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
        || url.scheme() != "https"
    {
        return Err("invalid secure MCP OAuth URL");
    }
    Ok(url)
}

fn protected_resource_metadata_path(resource: &Url) -> Option<String> {
    let suffix = resource.path().trim_start_matches('/');
    let path = if suffix.is_empty() {
        "/.well-known/oauth-protected-resource".to_owned()
    } else {
        format!("/.well-known/oauth-protected-resource/{suffix}")
    };
    (path.len() <= 8 * 1024).then_some(path)
}

fn protected_resource_metadata_url(resource: &Url) -> Option<String> {
    let mut metadata = resource.clone();
    metadata.set_path(&protected_resource_metadata_path(resource)?);
    metadata.set_query(None);
    metadata.set_fragment(None);
    Some(metadata.to_string())
}

fn access_token_scopes(claims: &Value) -> Option<BTreeSet<String>> {
    let mut scopes = BTreeSet::new();
    if let Some(scope) = claims.get("scope").and_then(Value::as_str) {
        for value in scope.split_ascii_whitespace() {
            if !valid_scope(value) || !scopes.insert(value.to_owned()) {
                return None;
            }
        }
    }
    if let Some(scp) = claims.get("scp") {
        match scp {
            Value::String(value) => {
                for value in value.split_ascii_whitespace() {
                    if !valid_scope(value) {
                        return None;
                    }
                    scopes.insert(value.to_owned());
                }
            }
            Value::Array(values) => {
                for value in values {
                    let value = value.as_str().filter(|value| valid_scope(value))?;
                    scopes.insert(value.to_owned());
                }
            }
            _ => return None,
        }
    }
    (scopes.len() <= 128).then_some(scopes)
}

fn jwk_is_valid_signature_key(jwk: &Jwk, algorithm: jsonwebtoken::Algorithm) -> bool {
    let common = &jwk.common;
    if common
        .key_algorithm
        .is_some_and(|declared| declared != KeyAlgorithm::from(algorithm))
        || common
            .public_key_use
            .as_ref()
            .is_some_and(|usage| usage != &PublicKeyUse::Signature)
        || common.key_operations.as_ref().is_some_and(|operations| {
            operations.is_empty()
                || !operations.contains(&KeyOperations::Verify)
                || operations.iter().any(|operation| {
                    !matches!(operation, KeyOperations::Verify | KeyOperations::Sign)
                })
        })
    {
        return false;
    }
    !(common.public_key_use.is_some() && common.key_operations.is_some())
}

fn valid_access_token_claims(claims: &Value, now: u64) -> bool {
    let Some(exp) = claims.get("exp").and_then(Value::as_u64) else {
        return false;
    };
    let Some(iat) = claims.get("iat").and_then(Value::as_u64) else {
        return false;
    };
    let valid_bounded_claim = |name: &str| {
        claims
            .get(name)
            .and_then(Value::as_str)
            .is_some_and(|value| {
                !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
            })
    };
    exp > iat
        && iat <= now.saturating_add(60)
        && valid_bounded_claim("client_id")
        && valid_bounded_claim("jti")
}

fn valid_scope(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
}

fn valid_principal_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.contains('/')
        && !value.chars().any(char::is_control)
}

fn optional_single_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, ()> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(());
    }
    first
        .map(|value| value.to_str().map_err(|_| ()))
        .transpose()
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    optional_single_header(headers, name).ok().flatten()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

fn unauthorized(challenge: &str) -> Response {
    let mut response = StatusCode::UNAUTHORIZED.into_response();
    if let Ok(challenge) = HeaderValue::from_str(challenge) {
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, challenge);
    }
    no_store(&mut response);
    response
}

fn bad_request() -> Response {
    let mut response = StatusCode::BAD_REQUEST.into_response();
    no_store(&mut response);
    response
}

fn no_store(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-transform"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use insight_mcp::MCP_PROTOCOL_VERSION;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair as _},
    };
    use serde_json::json;

    #[tokio::test]
    async fn bearer_authorizer_is_redacted_and_exact() {
        let authorizer = StaticBearerMcpHttpAuthorizer::new(
            "secret",
            McpHttpPrincipal::new("tenant/user").unwrap(),
        )
        .unwrap();
        assert!(!format!("{authorizer:?}").contains("secret"));
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret"),
        );
        assert_eq!(
            authorizer.authorize(&headers).await.unwrap().subject(),
            "tenant/user"
        );
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer other"),
        );
        assert!(authorizer.authorize(&headers).await.is_err());
    }

    #[test]
    fn protected_resource_metadata_path_preserves_resource_path() {
        let resource = Url::parse("https://agents.example.com/mcp").unwrap();
        assert_eq!(
            protected_resource_metadata_path(&resource).as_deref(),
            Some("/.well-known/oauth-protected-resource/mcp")
        );
        assert_eq!(
            protected_resource_metadata_url(&resource).as_deref(),
            Some("https://agents.example.com/.well-known/oauth-protected-resource/mcp")
        );
    }

    #[tokio::test]
    async fn oauth_resource_authorizer_verifies_signature_audience_scope_and_principal() {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let encoding = EncodingKey::from_ed_der(pkcs8.as_ref());
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let jwk = serde_json::from_value(json!({
            "kty": "OKP",
            "crv": "Ed25519",
            "x": URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref()),
            "alg": "EdDSA",
            "kid": "key-1",
            "use": "sig"
        }))
        .unwrap();
        let authorizer = OAuthResourceServerAuthorizer {
            resource: "https://agents.example.com/mcp".to_owned(),
            resource_metadata_url:
                "https://agents.example.com/.well-known/oauth-protected-resource/mcp".to_owned(),
            required_scopes: BTreeSet::from(["mcp.invoke".to_owned()]),
            issuers: vec![Arc::new(OAuthIssuerKeys {
                issuer: "https://issuer.example.com".to_owned(),
                jwks_uri: Url::parse("https://issuer.example.com/jwks").unwrap(),
                keys: RwLock::new(CachedJwks {
                    value: JwkSet { keys: vec![jwk] },
                    fetched_at: Instant::now(),
                }),
            })],
            http_timeout: Duration::from_secs(5),
        };
        let mut jwt_header = Header::new(Algorithm::EdDSA);
        jwt_header.kid = Some("key-1".to_owned());
        jwt_header.typ = Some("at+jwt".to_owned());
        let claims = serde_json::json!({
            "iss": "https://issuer.example.com",
            "sub": "user-a",
            "tenant_id": "tenant-a",
            "aud": "https://agents.example.com/mcp",
            "scope": "mcp.invoke extra",
            "iat": jsonwebtoken::get_current_timestamp(),
            "exp": jsonwebtoken::get_current_timestamp() + 300,
            "jti": "token-1",
            "client_id": "client-a"
        });
        let token = encode(&jwt_header, &claims, &encoding).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        assert_eq!(
            authorizer.authorize(&headers).await.unwrap().subject(),
            "tenant-a/user-a"
        );

        let insufficient = serde_json::json!({
            "iss": "https://issuer.example.com",
            "sub": "user-a",
            "tenant_id": "tenant-a",
            "aud": "https://agents.example.com/mcp",
            "scope": "other",
            "iat": jsonwebtoken::get_current_timestamp(),
            "exp": jsonwebtoken::get_current_timestamp() + 300,
            "jti": "token-2",
            "client_id": "client-a"
        });
        let token = encode(&jwt_header, &insufficient, &encoding).unwrap();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        assert_eq!(
            authorizer.authorize(&headers).await.unwrap_err(),
            McpHttpAuthorizationError::InsufficientScope
        );

        let missing_required_profile_claim = serde_json::json!({
            "iss": "https://issuer.example.com",
            "sub": "user-a",
            "tenant_id": "tenant-a",
            "aud": "https://agents.example.com/mcp",
            "scope": "mcp.invoke",
            "iat": jsonwebtoken::get_current_timestamp(),
            "exp": jsonwebtoken::get_current_timestamp() + 300,
            "client_id": "client-a"
        });
        let token = encode(&jwt_header, &missing_required_profile_claim, &encoding).unwrap();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        assert_eq!(
            authorizer.authorize(&headers).await.unwrap_err(),
            McpHttpAuthorizationError::InvalidToken
        );

        let future_issued_token = serde_json::json!({
            "iss": "https://issuer.example.com",
            "sub": "user-a",
            "tenant_id": "tenant-a",
            "aud": "https://agents.example.com/mcp",
            "scope": "mcp.invoke",
            "iat": jsonwebtoken::get_current_timestamp() + 120,
            "exp": jsonwebtoken::get_current_timestamp() + 300,
            "jti": "token-future",
            "client_id": "client-a"
        });
        let token = encode(&jwt_header, &future_issued_token, &encoding).unwrap();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        assert_eq!(
            authorizer.authorize(&headers).await.unwrap_err(),
            McpHttpAuthorizationError::InvalidToken
        );
    }

    #[test]
    fn protocol_headers_reject_duplicates() {
        let mut headers = HeaderMap::new();
        headers.append(
            "mcp-protocol-version",
            HeaderValue::from_static(MCP_PROTOCOL_VERSION),
        );
        headers.append(
            "mcp-protocol-version",
            HeaderValue::from_static(MCP_PROTOCOL_VERSION),
        );
        headers.insert("mcp-method", HeaderValue::from_static("server/discover"));
        assert!(protocol_headers(&headers).is_err());
    }
}
