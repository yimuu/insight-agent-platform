//! MCP HTTP Authorization discovery and authorization-code/PKCE client.

use std::{collections::BTreeMap, fmt, time::Duration};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures::StreamExt as _;
use reqwest::Url;
use ring::{
    digest,
    rand::{SecureRandom as _, SystemRandom},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use crate::build_pinned_http_client;

const MAX_METADATA_BYTES: usize = 256 * 1024;
const MAX_TOKEN_RESPONSE_BYTES: usize = 256 * 1024;
const MAX_EXTENSION_ENTRIES: usize = 64;
const MAX_SCOPE_COUNT: usize = 128;
const MAX_URL_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OAuthProtectedResourceMetadata {
    pub resource: String,
    pub authorization_servers: Vec<String>,
    #[serde(default)]
    pub scopes_supported: Vec<String>,
    #[serde(default)]
    pub bearer_methods_supported: Vec<String>,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OAuthAuthorizationServerMetadata {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    #[serde(default)]
    pub revocation_endpoint: Option<String>,
    #[serde(default)]
    pub scopes_supported: Vec<String>,
    #[serde(default)]
    pub response_types_supported: Vec<String>,
    #[serde(default)]
    pub code_challenge_methods_supported: Vec<String>,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthClientRegistration {
    pub client_id: String,
    pub redirect_uri: String,
    pub scopes: Vec<String>,
}

impl OAuthClientRegistration {
    pub fn new(
        client_id: impl Into<String>,
        redirect_uri: impl Into<String>,
        mut scopes: Vec<String>,
    ) -> Result<Self, OAuthError> {
        let client_id = client_id.into();
        let redirect_uri = redirect_uri.into();
        scopes.sort();
        scopes.dedup();
        if client_id.is_empty()
            || client_id.len() > 1024
            || !valid_redirect_uri(&redirect_uri)
            || !valid_scopes(&scopes)
        {
            return Err(OAuthError::Configuration);
        }
        Ok(Self {
            client_id,
            redirect_uri,
            scopes,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthPkceTransaction {
    authorization_url: String,
    state: String,
    code_verifier: String,
    issuer: String,
    resource: String,
}

impl OAuthPkceTransaction {
    pub fn authorization_url(&self) -> &str {
        &self.authorization_url
    }

    pub fn state(&self) -> &str {
        &self.state
    }

    pub fn code_verifier(&self) -> &str {
        &self.code_verifier
    }

    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    pub fn resource(&self) -> &str {
        &self.resource
    }
}

impl fmt::Debug for OAuthPkceTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthPkceTransaction")
            .field("authorization_url", &"[REDACTED]")
            .field("state", &"[REDACTED]")
            .field("code_verifier", &"[REDACTED]")
            .field("issuer", &self.issuer)
            .field("resource", &self.resource)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct OAuthTokenSet {
    access_token: String,
    refresh_token: Option<String>,
    token_type: String,
    expires_in: Option<Duration>,
    scopes: Vec<String>,
}

impl OAuthTokenSet {
    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    pub fn refresh_token(&self) -> Option<&str> {
        self.refresh_token.as_deref()
    }

    pub fn token_type(&self) -> &str {
        &self.token_type
    }

    pub fn expires_in(&self) -> Option<Duration> {
        self.expires_in
    }

    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }
}

impl fmt::Debug for OAuthTokenSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthTokenSet")
            .field("access_token", &"[REDACTED]")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("token_type", &self.token_type)
            .field("expires_in", &self.expires_in)
            .field("scopes", &self.scopes)
            .finish()
    }
}

#[derive(Clone)]
pub struct OAuthClient {
    timeout: Duration,
}

impl OAuthClient {
    pub fn new(timeout: Duration) -> Result<Self, OAuthError> {
        if timeout.is_zero() || timeout > Duration::from_secs(120) {
            return Err(OAuthError::Configuration);
        }
        Ok(Self { timeout })
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub async fn discover_protected_resource(
        &self,
        resource: &str,
    ) -> Result<OAuthProtectedResourceMetadata, OAuthError> {
        let resource_url = secure_url(resource)?;
        let metadata_url = well_known_url(&resource_url, "oauth-protected-resource")?;
        let metadata: OAuthProtectedResourceMetadata =
            self.get_json(metadata_url, MAX_METADATA_BYTES).await?;
        metadata.validate(resource)?;
        Ok(metadata)
    }

    pub async fn discover_authorization_server(
        &self,
        issuer: &str,
    ) -> Result<OAuthAuthorizationServerMetadata, OAuthError> {
        let issuer_url = secure_url(issuer)?;
        let metadata_url = well_known_url(&issuer_url, "oauth-authorization-server")?;
        let metadata: OAuthAuthorizationServerMetadata =
            self.get_json(metadata_url, MAX_METADATA_BYTES).await?;
        metadata.validate(issuer)?;
        Ok(metadata)
    }

    pub fn begin_authorization(
        &self,
        protected: &OAuthProtectedResourceMetadata,
        authorization_server: &OAuthAuthorizationServerMetadata,
        registration: &OAuthClientRegistration,
    ) -> Result<OAuthPkceTransaction, OAuthError> {
        protected.validate(&protected.resource)?;
        authorization_server.validate(&authorization_server.issuer)?;
        if !protected
            .authorization_servers
            .iter()
            .any(|issuer| issuer == &authorization_server.issuer)
            || !registration.scopes.iter().all(|scope| {
                protected.scopes_supported.is_empty() || protected.scopes_supported.contains(scope)
            })
            || !registration.scopes.iter().all(|scope| {
                authorization_server.scopes_supported.is_empty()
                    || authorization_server.scopes_supported.contains(scope)
            })
        {
            return Err(OAuthError::Scope);
        }
        let state = random_url_token()?;
        let code_verifier = random_url_token()?;
        let challenge =
            URL_SAFE_NO_PAD.encode(digest::digest(&digest::SHA256, code_verifier.as_bytes()));
        let mut authorization_url = secure_url(&authorization_server.authorization_endpoint)?;
        {
            let mut query = authorization_url.query_pairs_mut();
            query
                .clear()
                .append_pair("response_type", "code")
                .append_pair("client_id", &registration.client_id)
                .append_pair("redirect_uri", &registration.redirect_uri)
                .append_pair("state", &state)
                .append_pair("code_challenge", &challenge)
                .append_pair("code_challenge_method", "S256")
                .append_pair("resource", &protected.resource);
            if !registration.scopes.is_empty() {
                query.append_pair("scope", &registration.scopes.join(" "));
            }
        }
        Ok(OAuthPkceTransaction {
            authorization_url: authorization_url.to_string(),
            state,
            code_verifier,
            issuer: authorization_server.issuer.clone(),
            resource: protected.resource.clone(),
        })
    }

    pub async fn exchange_code(
        &self,
        authorization_server: &OAuthAuthorizationServerMetadata,
        registration: &OAuthClientRegistration,
        transaction: &OAuthPkceTransaction,
        callback_state: &str,
        callback_issuer: &str,
        code: &str,
    ) -> Result<OAuthTokenSet, OAuthError> {
        if callback_state.is_empty()
            || !constant_time_eq(callback_state.as_bytes(), transaction.state.as_bytes())
            || code.is_empty()
            || code.len() > 16 * 1024
            || authorization_server.issuer != transaction.issuer
            || callback_issuer != transaction.issuer
        {
            return Err(OAuthError::State);
        }
        self.token_request(
            &authorization_server.token_endpoint,
            &[
                ("grant_type", "authorization_code"),
                ("client_id", &registration.client_id),
                ("redirect_uri", &registration.redirect_uri),
                ("code", code),
                ("code_verifier", &transaction.code_verifier),
                ("resource", &transaction.resource),
            ],
            &registration.scopes,
        )
        .await
    }

    pub async fn refresh(
        &self,
        authorization_server: &OAuthAuthorizationServerMetadata,
        registration: &OAuthClientRegistration,
        resource: &str,
        refresh_token: &str,
    ) -> Result<OAuthTokenSet, OAuthError> {
        secure_url(resource)?;
        if refresh_token.is_empty() || refresh_token.len() > 64 * 1024 {
            return Err(OAuthError::Token);
        }
        self.token_request(
            &authorization_server.token_endpoint,
            &[
                ("grant_type", "refresh_token"),
                ("client_id", &registration.client_id),
                ("refresh_token", refresh_token),
                ("resource", resource),
            ],
            &registration.scopes,
        )
        .await
    }

    pub async fn revoke(
        &self,
        authorization_server: &OAuthAuthorizationServerMetadata,
        client_id: &str,
        token: &str,
        token_type_hint: &str,
    ) -> Result<(), OAuthError> {
        authorization_server.validate(&authorization_server.issuer)?;
        let endpoint = authorization_server
            .revocation_endpoint
            .as_deref()
            .ok_or(OAuthError::Configuration)
            .and_then(secure_url)?;
        if client_id.is_empty()
            || client_id.len() > 1024
            || token.is_empty()
            || token.len() > 64 * 1024
            || !matches!(token_type_hint, "access_token" | "refresh_token")
        {
            return Err(OAuthError::Configuration);
        }
        let http = self.http_for(&endpoint)?;
        let response = http
            .post(endpoint)
            .header(reqwest::header::ACCEPT, "application/json")
            .form(&[
                ("client_id", client_id),
                ("token", token),
                ("token_type_hint", token_type_hint),
            ])
            .send()
            .await
            .map_err(|_| OAuthError::Network)?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(OAuthError::Token)
        }
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        url: Url,
        max_bytes: usize,
    ) -> Result<T, OAuthError> {
        let http = self.http_for(&url)?;
        let response = http
            .get(url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| OAuthError::Network)?;
        if !response.status().is_success()
            || response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(';').next())
                .map(str::trim)
                != Some("application/json")
        {
            return Err(OAuthError::Metadata);
        }
        let bytes = read_limited_oauth_body(response, max_bytes).await?;
        serde_json::from_slice(&bytes).map_err(|_| OAuthError::Metadata)
    }

    async fn token_request(
        &self,
        endpoint: &str,
        fields: &[(&str, &str)],
        requested_scopes: &[String],
    ) -> Result<OAuthTokenSet, OAuthError> {
        let endpoint = secure_url(endpoint)?;
        let http = self.http_for(&endpoint)?;
        let response = http
            .post(endpoint)
            .header(reqwest::header::ACCEPT, "application/json")
            .form(fields)
            .send()
            .await
            .map_err(|_| OAuthError::Network)?;
        if !response.status().is_success()
            || response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(';').next())
                .map(str::trim)
                != Some("application/json")
        {
            return Err(OAuthError::Token);
        }
        let bytes = read_limited_oauth_body(response, MAX_TOKEN_RESPONSE_BYTES).await?;
        let wire: TokenResponse = serde_json::from_slice(&bytes).map_err(|_| OAuthError::Token)?;
        wire.into_token_set(requested_scopes)
    }

    fn http_for(&self, endpoint: &Url) -> Result<reqwest::Client, OAuthError> {
        build_pinned_http_client(
            endpoint,
            self.timeout.min(Duration::from_secs(10)),
            self.timeout,
            false,
        )
        .map_err(|_| OAuthError::Configuration)
    }
}

impl fmt::Debug for OAuthClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthClient")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl OAuthProtectedResourceMetadata {
    pub fn validate(&self, expected_resource: &str) -> Result<(), OAuthError> {
        secure_url(&self.resource)?;
        if self.resource != expected_resource
            || self.authorization_servers.is_empty()
            || self.authorization_servers.len() > 16
            || self
                .authorization_servers
                .iter()
                .any(|issuer| secure_url(issuer).is_err())
            || !valid_scopes(&self.scopes_supported)
            || self.extensions.len() > MAX_EXTENSION_ENTRIES
        {
            return Err(OAuthError::Metadata);
        }
        Ok(())
    }
}

impl OAuthAuthorizationServerMetadata {
    pub fn validate(&self, expected_issuer: &str) -> Result<(), OAuthError> {
        secure_url(&self.issuer)?;
        secure_url(&self.authorization_endpoint)?;
        secure_url(&self.token_endpoint)?;
        if self.issuer != expected_issuer
            || self
                .revocation_endpoint
                .as_ref()
                .is_some_and(|value| secure_url(value).is_err())
            || !self
                .response_types_supported
                .iter()
                .any(|value| value == "code")
            || !self
                .code_challenge_methods_supported
                .iter()
                .any(|value| value == "S256")
            || !valid_scopes(&self.scopes_supported)
            || self.extensions.len() > MAX_EXTENSION_ENTRIES
        {
            return Err(OAuthError::Metadata);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(flatten)]
    extensions: BTreeMap<String, Value>,
}

impl TokenResponse {
    fn into_token_set(self, requested_scopes: &[String]) -> Result<OAuthTokenSet, OAuthError> {
        let mut scopes = self
            .scope
            .map(|scope| scope.split_ascii_whitespace().map(str::to_owned).collect())
            .unwrap_or_else(|| requested_scopes.to_vec());
        scopes.sort();
        scopes.dedup();
        if self.access_token.is_empty()
            || self.access_token.len() > 64 * 1024
            || !self.token_type.eq_ignore_ascii_case("bearer")
            || self
                .refresh_token
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 64 * 1024)
            || !valid_scopes(&scopes)
            || !requested_scopes.iter().all(|scope| scopes.contains(scope))
            || self.extensions.len() > MAX_EXTENSION_ENTRIES
        {
            return Err(OAuthError::Token);
        }
        Ok(OAuthTokenSet {
            access_token: self.access_token,
            refresh_token: self.refresh_token,
            token_type: "Bearer".to_owned(),
            expires_in: self.expires_in.map(Duration::from_secs),
            scopes,
        })
    }
}

fn well_known_url(base: &Url, suffix: &str) -> Result<Url, OAuthError> {
    let path = base.path();
    let path = if path == "/" {
        format!("/.well-known/{suffix}")
    } else {
        format!("/.well-known/{suffix}{}", path.trim_end_matches('/'))
    };
    let mut url = base.clone();
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn secure_url(value: &str) -> Result<Url, OAuthError> {
    if value.is_empty() || value.len() > MAX_URL_BYTES {
        return Err(OAuthError::Configuration);
    }
    let url = Url::parse(value).map_err(|_| OAuthError::Configuration)?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
        || url.fragment().is_some()
        || url.query().is_some()
    {
        return Err(OAuthError::Configuration);
    }
    Ok(url)
}

async fn read_limited_oauth_body(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, OAuthError> {
    if response
        .content_length()
        .is_some_and(|length| usize::try_from(length).map_or(true, |length| length > max_bytes))
    {
        return Err(OAuthError::Limit);
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| OAuthError::Network)?;
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(OAuthError::Limit);
        }
        body.extend_from_slice(&chunk);
    }
    if body.is_empty() {
        return Err(OAuthError::Limit);
    }
    Ok(body)
}

fn valid_redirect_uri(value: &str) -> bool {
    Url::parse(value).is_ok_and(|url| {
        (url.scheme() == "https"
            || (url.scheme() == "http"
                && url
                    .host_str()
                    .is_some_and(|host| host == "127.0.0.1" || host == "::1")))
            && url.fragment().is_none()
            && url.username().is_empty()
            && url.password().is_none()
    })
}

fn valid_scopes(scopes: &[String]) -> bool {
    scopes.len() <= MAX_SCOPE_COUNT
        && scopes.iter().all(|scope| {
            !scope.is_empty()
                && scope.len() <= 256
                && scope
                    .bytes()
                    .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
        })
}

fn random_url_token() -> Result<String, OAuthError> {
    let mut bytes = [0_u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| OAuthError::Random)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthError {
    Configuration,
    Metadata,
    Scope,
    State,
    Token,
    Network,
    Limit,
    Random,
}

impl fmt::Display for OAuthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Configuration => "invalid OAuth configuration",
            Self::Metadata => "OAuth metadata is invalid",
            Self::Scope => "OAuth scope authorization failed",
            Self::State => "OAuth callback state validation failed",
            Self::Token => "OAuth token operation failed",
            Self::Network => "OAuth endpoint is unavailable",
            Self::Limit => "OAuth response exceeds configured limits",
            Self::Random => "OAuth secure random generation failed",
        })
    }
}

impl std::error::Error for OAuthError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn protected() -> OAuthProtectedResourceMetadata {
        OAuthProtectedResourceMetadata {
            resource: "https://mcp.example.test/mcp".to_owned(),
            authorization_servers: vec!["https://id.example.test".to_owned()],
            scopes_supported: vec!["repo.read".to_owned()],
            bearer_methods_supported: vec!["header".to_owned()],
            extensions: BTreeMap::new(),
        }
    }

    fn server() -> OAuthAuthorizationServerMetadata {
        OAuthAuthorizationServerMetadata {
            issuer: "https://id.example.test".to_owned(),
            authorization_endpoint: "https://id.example.test/authorize".to_owned(),
            token_endpoint: "https://id.example.test/token".to_owned(),
            revocation_endpoint: Some("https://id.example.test/revoke".to_owned()),
            scopes_supported: vec!["repo.read".to_owned()],
            response_types_supported: vec!["code".to_owned()],
            code_challenge_methods_supported: vec!["S256".to_owned()],
            extensions: BTreeMap::new(),
        }
    }

    #[test]
    fn authorization_uses_unique_pkce_and_redacts_authority() {
        let client = OAuthClient::new(Duration::from_secs(5)).unwrap();
        let registration = OAuthClientRegistration::new(
            "client",
            "https://app.example.test/oauth/callback",
            vec!["repo.read".to_owned()],
        )
        .unwrap();
        let first = client
            .begin_authorization(&protected(), &server(), &registration)
            .unwrap();
        let second = client
            .begin_authorization(&protected(), &server(), &registration)
            .unwrap();
        assert_ne!(first.state(), second.state());
        assert_ne!(first.code_verifier(), second.code_verifier());
        assert!(first
            .authorization_url()
            .contains("code_challenge_method=S256"));
        let debug = format!("{first:?}");
        assert!(!debug.contains(first.state()));
        assert!(!debug.contains(first.code_verifier()));
    }

    #[test]
    fn metadata_and_scope_fail_closed() {
        let client = OAuthClient::new(Duration::from_secs(5)).unwrap();
        let registration = OAuthClientRegistration::new(
            "client",
            "https://app.example.test/oauth/callback",
            vec!["repo.write".to_owned()],
        )
        .unwrap();
        assert_eq!(
            client.begin_authorization(&protected(), &server(), &registration),
            Err(OAuthError::Scope)
        );
        let mut invalid = server();
        invalid.code_challenge_methods_supported = vec!["plain".to_owned()];
        assert_eq!(
            invalid.validate("https://id.example.test"),
            Err(OAuthError::Metadata)
        );
        assert!(OAuthClientRegistration::new(
            "client",
            "https://app.example.test/callback#fragment",
            Vec::new()
        )
        .is_err());
    }

    #[test]
    fn token_debug_never_contains_tokens() {
        let token = OAuthTokenSet {
            access_token: "access-secret".to_owned(),
            refresh_token: Some("refresh-secret".to_owned()),
            token_type: "Bearer".to_owned(),
            expires_in: Some(Duration::from_secs(60)),
            scopes: vec!["repo.read".to_owned()],
        };
        let debug = format!("{token:?}");
        assert!(!debug.contains("access-secret"));
        assert!(!debug.contains("refresh-secret"));
    }
}
