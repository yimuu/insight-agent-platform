use super::{
    is_public_destination_ip, parse_endpoint_host, DnsResolutionError, EgressDnsResolver,
    ParsedEndpointHost, ResolvedSecretMaterial, SecretMaterialResolutionError,
    SecretMaterialResolver,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::StreamExt;
use insight_platform_contracts::{
    canonical_digest, CanonicalHttpEndpoint, ExactDeploymentRef, ExactSecretBindingRef,
    ExactVersionRef, McpAuthPolicyDocument, McpOAuthClientAuthenticationKind, ResourceId,
    ResourceKind, SecretPurpose, SecretResolutionPolicy, Sha256Digest,
};
use insight_platform_mcp_host::{
    AuthorizedMcpOAuthPkceCleanup, McpOAuthAuthorizedGrant, McpOAuthCredentialBroker,
    McpOAuthCredentialBrokerError, McpOAuthExchangeContract, McpOAuthPkceSecretCleaner,
    McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError, SensitiveMcpOAuthNonce,
    SensitiveOAuthValue,
};
use jsonwebtoken::{
    decode, decode_header,
    jwk::{Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse},
    Algorithm, DecodingKey, Validation,
};
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
};
use serde::{
    de::{self, Deserializer, MapAccess, Visitor},
    Deserialize, Serialize,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Write as _},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use url::Url;

pub const MAX_MCP_OAUTH_TOKEN_IN_FLIGHT_HARD: usize = 512;
pub const MAX_MCP_OAUTH_TOKEN_BYTES_HARD: usize = 65_536;
pub const MAX_MCP_OAUTH_TOKEN_LIFETIME_SECONDS_HARD: i64 = 2_592_000;
pub const MAX_INSTALLED_MCP_OAUTH_VERIFICATION_BINDINGS: usize = 64;
pub const MAX_MCP_OAUTH_VERIFICATION_KEYS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthEgressLimits {
    pub maximum_in_flight: usize,
    pub maximum_dns_answers: usize,
    pub maximum_secret_material_bytes: usize,
    pub maximum_token_lifetime_seconds: i64,
}

impl Default for McpOAuthEgressLimits {
    fn default() -> Self {
        Self {
            maximum_in_flight: 64,
            maximum_dns_answers: 16,
            maximum_secret_material_bytes: 8_192,
            maximum_token_lifetime_seconds: 86_400,
        }
    }
}

impl McpOAuthEgressLimits {
    pub fn validate(self) -> Result<(), McpOAuthEgressConfigurationError> {
        if self.maximum_in_flight == 0
            || self.maximum_in_flight > MAX_MCP_OAUTH_TOKEN_IN_FLIGHT_HARD
            || self.maximum_dns_answers == 0
            || self.maximum_dns_answers > super::MAX_DNS_ANSWERS_HARD
            || self.maximum_secret_material_bytes == 0
            || self.maximum_secret_material_bytes > super::MAX_SECRET_MATERIAL_BYTES_HARD
            || self.maximum_token_lifetime_seconds <= 0
            || self.maximum_token_lifetime_seconds > MAX_MCP_OAUTH_TOKEN_LIFETIME_SECONDS_HARD
        {
            return Err(McpOAuthEgressConfigurationError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthEgressConfigurationError {
    InvalidLimits,
    InvalidVerificationCatalog,
}

impl fmt::Display for McpOAuthEgressConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimits => "MCP OAuth egress limits are invalid",
            Self::InvalidVerificationCatalog => "MCP OAuth verification catalog is invalid",
        })
    }
}

impl std::error::Error for McpOAuthEgressConfigurationError {}

/// Closed asymmetric algorithms accepted for an installed OAuth verification key set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstalledMcpOAuthJwtAlgorithm {
    EdDsa,
    Es256,
    Rs256,
}

impl InstalledMcpOAuthJwtAlgorithm {
    const fn jsonwebtoken(self) -> Algorithm {
        match self {
            Self::EdDsa => Algorithm::EdDSA,
            Self::Es256 => Algorithm::ES256,
            Self::Rs256 => Algorithm::RS256,
        }
    }
}

/// CandidateManifest-installed verification material for one exact MCP Auth Policy revision.
///
/// JWKs are public material. Their canonical digest is frozen separately so a config renderer or
/// secret injector cannot silently alter the trust root while retaining the same Auth Policy.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledMcpOAuthVerificationBinding {
    pub schema_version: u32,
    pub auth_policy: ExactVersionRef,
    pub auth_profile: McpAuthPolicyDocument,
    pub algorithms: Vec<InstalledMcpOAuthJwtAlgorithm>,
    pub jwks: serde_json::Value,
    pub jwks_digest: Sha256Digest,
}

#[derive(Debug, Clone)]
struct ValidatedMcpOAuthVerificationBinding {
    config: InstalledMcpOAuthVerificationBinding,
    keys: JwkSet,
}

#[derive(Debug, Clone)]
pub struct InstalledMcpOAuthVerificationCatalog {
    bindings: Arc<BTreeMap<Sha256Digest, ValidatedMcpOAuthVerificationBinding>>,
}

impl InstalledMcpOAuthVerificationCatalog {
    pub fn new(
        bindings: Vec<InstalledMcpOAuthVerificationBinding>,
    ) -> Result<Self, McpOAuthEgressConfigurationError> {
        if bindings.is_empty() || bindings.len() > MAX_INSTALLED_MCP_OAUTH_VERIFICATION_BINDINGS {
            return Err(McpOAuthEgressConfigurationError::InvalidVerificationCatalog);
        }
        let mut installed = BTreeMap::new();
        for binding in bindings {
            let validated = validate_verification_binding(binding)?;
            if installed
                .insert(
                    validated.config.auth_policy.semantic_digest.clone(),
                    validated,
                )
                .is_some()
            {
                return Err(McpOAuthEgressConfigurationError::InvalidVerificationCatalog);
            }
        }
        Ok(Self {
            bindings: Arc::new(installed),
        })
    }

    fn resolve(
        &self,
        contract: &McpOAuthExchangeContract,
    ) -> Result<&ValidatedMcpOAuthVerificationBinding, McpOAuthCredentialBrokerError> {
        let binding = self
            .bindings
            .get(&contract.binding.auth_policy.semantic_digest)
            .ok_or(McpOAuthCredentialBrokerError::Rejected)?;
        if binding.config.auth_policy != contract.binding.auth_policy
            || binding.config.auth_profile != contract.auth_profile
        {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        Ok(binding)
    }
}

fn validate_verification_binding(
    binding: InstalledMcpOAuthVerificationBinding,
) -> Result<ValidatedMcpOAuthVerificationBinding, McpOAuthEgressConfigurationError> {
    let invalid = || McpOAuthEgressConfigurationError::InvalidVerificationCatalog;
    binding.auth_profile.validate().map_err(|_| invalid())?;
    if binding.schema_version != 1
        || binding.auth_policy.resource_kind != ResourceKind::PolicyRevision
        || binding.auth_policy.validate().is_err()
        || binding.auth_profile.canonical_digest().as_ref()
            != Ok(&binding.auth_policy.semantic_digest)
        || binding
            .auth_profile
            .allowed_scopes
            .binary_search_by(|scope| scope.as_str().cmp("openid"))
            .is_err()
        || binding.algorithms.is_empty()
        || !binding.algorithms.windows(2).all(|pair| pair[0] < pair[1])
        || canonical_digest(&binding.jwks)
            .ok()
            .and_then(|value| value.parse::<Sha256Digest>().ok())
            .as_ref()
            != Some(&binding.jwks_digest)
        || binding
            .jwks
            .as_object()
            .is_none_or(|object| object.len() != 1 || !object.contains_key("keys"))
    {
        return Err(invalid());
    }
    let keys: JwkSet = serde_json::from_value(binding.jwks.clone()).map_err(|_| invalid())?;
    if keys.keys.is_empty() || keys.keys.len() > MAX_MCP_OAUTH_VERIFICATION_KEYS {
        return Err(invalid());
    }
    let mut previous_kid: Option<&str> = None;
    for key in &keys.keys {
        let kid = key
            .common
            .key_id
            .as_deref()
            .filter(|value| stable_jwt_identifier(value))
            .ok_or_else(invalid)?;
        if previous_kid.is_some_and(|previous| previous >= kid) {
            return Err(invalid());
        }
        previous_kid = Some(kid);
        let algorithm = key
            .common
            .key_algorithm
            .and_then(installed_algorithm_from_key)
            .filter(|algorithm| binding.algorithms.binary_search(algorithm).is_ok())
            .ok_or_else(invalid)?;
        if !jwk_is_valid_signature_key(key, algorithm.jsonwebtoken())
            || DecodingKey::from_jwk(key).is_err()
        {
            return Err(invalid());
        }
    }
    Ok(ValidatedMcpOAuthVerificationBinding {
        config: binding,
        keys,
    })
}

fn installed_algorithm_from_key(value: KeyAlgorithm) -> Option<InstalledMcpOAuthJwtAlgorithm> {
    match value {
        KeyAlgorithm::EdDSA => Some(InstalledMcpOAuthJwtAlgorithm::EdDsa),
        KeyAlgorithm::ES256 => Some(InstalledMcpOAuthJwtAlgorithm::Es256),
        KeyAlgorithm::RS256 => Some(InstalledMcpOAuthJwtAlgorithm::Rs256),
        _ => None,
    }
}

/// Raw token material is non-clone, redacted and zeroed before its allocation is released.
pub struct SensitiveMcpOAuthToken(Vec<u8>);

impl SensitiveMcpOAuthToken {
    fn new(mut value: Vec<u8>) -> Result<Self, McpOAuthCredentialBrokerError> {
        if value.is_empty()
            || value.len() > MAX_MCP_OAUTH_TOKEN_BYTES_HARD
            || value.iter().any(|byte| byte.is_ascii_control())
        {
            value.fill(0);
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        Ok(Self(value))
    }

    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SensitiveMcpOAuthToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveMcpOAuthToken")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for SensitiveMcpOAuthToken {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

impl<'de> Deserialize<'de> for SensitiveMcpOAuthToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SensitiveTokenVisitor;

        impl Visitor<'_> for SensitiveTokenVisitor {
            type Value = SensitiveMcpOAuthToken;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded OAuth token string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                SensitiveMcpOAuthToken::new(value.as_bytes().to_vec())
                    .map_err(|_| E::custom("invalid OAuth token"))
            }

            fn visit_borrowed_str<E>(self, value: &'_ str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(value)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                SensitiveMcpOAuthToken::new(value.into_bytes())
                    .map_err(|_| E::custom("invalid OAuth token"))
            }
        }

        deserializer.deserialize_string(SensitiveTokenVisitor)
    }
}

/// Closed token response. Unknown/duplicate fields and non-Bearer token types are rejected.
pub struct McpOAuthTokenSet {
    pub access_token: SensitiveMcpOAuthToken,
    pub refresh_token: Option<SensitiveMcpOAuthToken>,
    pub id_token: Option<SensitiveMcpOAuthToken>,
    pub expires_in_seconds: i64,
    pub granted_scopes: Vec<String>,
}

impl fmt::Debug for McpOAuthTokenSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthTokenSet")
            .field("access_token", &self.access_token)
            .field("has_refresh_token", &self.refresh_token.is_some())
            .field("has_id_token", &self.id_token.is_some())
            .field("expires_in_seconds", &self.expires_in_seconds)
            .field("granted_scopes", &self.granted_scopes)
            .finish()
    }
}

impl<'de> Deserialize<'de> for McpOAuthTokenSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TokenSetVisitor;

        impl<'de> Visitor<'de> for TokenSetVisitor {
            type Value = McpOAuthTokenSet;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a closed OAuth token response")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut seen = BTreeSet::new();
                let mut access_token = None;
                let mut refresh_token = None;
                let mut id_token = None;
                let mut token_type = None::<String>;
                let mut expires_in_seconds = None;
                let mut scope = None::<String>;
                while let Some(key) = map.next_key::<String>()? {
                    if !seen.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate OAuth token response field"));
                    }
                    match key.as_str() {
                        "access_token" => access_token = Some(map.next_value()?),
                        "refresh_token" => refresh_token = Some(map.next_value()?),
                        "id_token" => id_token = Some(map.next_value()?),
                        "token_type" => token_type = Some(map.next_value()?),
                        "expires_in" => expires_in_seconds = Some(map.next_value()?),
                        "scope" => scope = Some(map.next_value()?),
                        _ => return Err(de::Error::custom("unknown OAuth token response field")),
                    }
                }
                let token_type =
                    token_type.ok_or_else(|| de::Error::missing_field("token_type"))?;
                if !token_type.eq_ignore_ascii_case("bearer") {
                    return Err(de::Error::custom("unsupported OAuth token type"));
                }
                let granted_scopes = scope
                    .map(|value| parse_scope_set(&value).map_err(de::Error::custom))
                    .transpose()?
                    .unwrap_or_default();
                Ok(McpOAuthTokenSet {
                    access_token: access_token
                        .ok_or_else(|| de::Error::missing_field("access_token"))?,
                    refresh_token,
                    id_token,
                    expires_in_seconds: expires_in_seconds
                        .ok_or_else(|| de::Error::missing_field("expires_in"))?,
                    granted_scopes,
                })
            }
        }

        deserializer.deserialize_map(TokenSetVisitor)
    }
}

fn parse_scope_set(value: &str) -> Result<Vec<String>, &'static str> {
    if value.is_empty() || value.len() > 16_384 {
        return Err("invalid OAuth scope response");
    }
    let mut scopes = value.split(' ').map(str::to_owned).collect::<Vec<String>>();
    scopes.sort();
    if scopes.iter().any(|scope| scope.is_empty())
        || !scopes.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err("invalid OAuth scope response");
    }
    Ok(scopes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedMcpOAuthToken {
    pub granted_scopes: Vec<String>,
    pub audience_identity_digest: Sha256Digest,
    pub issuer_identity_digest: Sha256Digest,
    pub subject_identity_digest: Sha256Digest,
    pub verification_evidence_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
    pub nonce_verified: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthTokenVerificationError {
    Rejected,
    TemporarilyUnavailable,
}

#[async_trait]
pub trait McpOAuthTokenVerifier: Send + Sync {
    async fn verify(
        &self,
        contract: &McpOAuthExchangeContract,
        tokens: &McpOAuthTokenSet,
        now: DateTime<Utc>,
    ) -> Result<VerifiedMcpOAuthToken, McpOAuthTokenVerificationError>;
}

/// Production verifier backed only by CandidateManifest-installed public JWK material.
///
/// Verification performs no network I/O. This is intentional: a one-time authorization code is
/// not consumed until the exact Auth Policy/key-set binding is already available locally.
#[derive(Clone)]
pub struct InstalledMcpOAuthJwtVerifier {
    catalog: InstalledMcpOAuthVerificationCatalog,
}

impl InstalledMcpOAuthJwtVerifier {
    pub fn new(catalog: InstalledMcpOAuthVerificationCatalog) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl McpOAuthTokenVerifier for InstalledMcpOAuthJwtVerifier {
    async fn verify(
        &self,
        contract: &McpOAuthExchangeContract,
        tokens: &McpOAuthTokenSet,
        now: DateTime<Utc>,
    ) -> Result<VerifiedMcpOAuthToken, McpOAuthTokenVerificationError> {
        contract
            .validate_at(now)
            .map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
        let installed = self
            .catalog
            .resolve(contract)
            .map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
        if contract
            .binding
            .requested_scopes
            .binary_search_by(|scope| scope.as_str().cmp("openid"))
            .is_err()
        {
            return Err(McpOAuthTokenVerificationError::Rejected);
        }
        let issuer = exact_endpoint_url(&contract.auth_profile.issuer.endpoint)
            .map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
        let audience = exact_endpoint_url(&contract.auth_profile.resource_indicator.endpoint)
            .map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
        let access = verify_installed_jwt(
            &tokens.access_token,
            JwtPurpose::AccessToken,
            issuer.as_str(),
            audience.as_str(),
            contract.auth_profile.maximum_clock_skew_seconds,
            installed,
            now,
        )?;
        let id_token = tokens
            .id_token
            .as_ref()
            .ok_or(McpOAuthTokenVerificationError::Rejected)?;
        let identity = verify_installed_jwt(
            id_token,
            JwtPurpose::IdToken,
            issuer.as_str(),
            &contract.auth_profile.client_id,
            contract.auth_profile.maximum_clock_skew_seconds,
            installed,
            now,
        )?;
        if access.subject != identity.subject {
            return Err(McpOAuthTokenVerificationError::Rejected);
        }
        let nonce = identity
            .claims
            .get("nonce")
            .and_then(serde_json::Value::as_str)
            .filter(|value| stable_jwt_identifier(value))
            .ok_or(McpOAuthTokenVerificationError::Rejected)?;
        let nonce = SensitiveMcpOAuthNonce::new(nonce.as_bytes().to_vec())
            .map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
        if insight_platform_mcp_host::mcp_oauth_nonce_digest(&nonce)
            .ok()
            .as_ref()
            != Some(&contract.binding.nonce_digest)
        {
            return Err(McpOAuthTokenVerificationError::Rejected);
        }
        let granted_scopes = jwt_scopes(&access.claims)?;
        let expires_at = DateTime::from_timestamp(
            i64::try_from(access.expires_at)
                .map_err(|_| McpOAuthTokenVerificationError::Rejected)?,
            0,
        )
        .ok_or(McpOAuthTokenVerificationError::Rejected)?;
        let subject_identity_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "domain": "mcp_oauth_subject_v1",
            "issuer_identity_digest": contract.auth_profile.issuer.endpoint_identity_digest,
            "subject": access.subject,
        }))
        .map_err(|_| McpOAuthTokenVerificationError::Rejected)?
        .parse()
        .map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
        let access_token_digest = sensitive_sha256(
            b"insight.platform/v1:mcp_oauth_access_token\0",
            tokens.access_token.expose(),
        )
        .map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
        let id_token_digest = sensitive_sha256(
            b"insight.platform/v1:mcp_oauth_id_token\0",
            id_token.expose(),
        )
        .map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
        let verification_evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "access_algorithm": access.algorithm,
            "access_kid": access.kid,
            "access_token_digest": access_token_digest,
            "auth_policy": contract.binding.auth_policy,
            "domain": "mcp_oauth_installed_jwt_verification_v1",
            "expires_at": expires_at,
            "id_algorithm": identity.algorithm,
            "id_kid": identity.kid,
            "id_token_digest": id_token_digest,
            "jwks_digest": installed.config.jwks_digest,
            "nonce_digest": contract.binding.nonce_digest,
            "subject_identity_digest": subject_identity_digest,
        }))
        .map_err(|_| McpOAuthTokenVerificationError::Rejected)?
        .parse()
        .map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
        Ok(VerifiedMcpOAuthToken {
            granted_scopes,
            audience_identity_digest: contract.binding.audience_identity_digest.clone(),
            issuer_identity_digest: contract
                .auth_profile
                .issuer
                .endpoint_identity_digest
                .clone(),
            subject_identity_digest,
            verification_evidence_digest,
            expires_at,
            nonce_verified: true,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum JwtPurpose {
    AccessToken,
    IdToken,
}

struct VerifiedJwt {
    claims: serde_json::Value,
    subject: String,
    expires_at: u64,
    kid: String,
    algorithm: InstalledMcpOAuthJwtAlgorithm,
}

fn verify_installed_jwt(
    token: &SensitiveMcpOAuthToken,
    purpose: JwtPurpose,
    issuer: &str,
    audience: &str,
    maximum_clock_skew_seconds: u16,
    installed: &ValidatedMcpOAuthVerificationBinding,
    now: DateTime<Utc>,
) -> Result<VerifiedJwt, McpOAuthTokenVerificationError> {
    let raw = std::str::from_utf8(token.expose())
        .map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
    let header = decode_header(raw).map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
    let valid_type = match purpose {
        JwtPurpose::AccessToken => {
            matches!(header.typ.as_deref(), Some("at+jwt" | "application/at+jwt"))
        }
        JwtPurpose::IdToken => header.typ.as_deref() == Some("JWT"),
    };
    let algorithm = installed_algorithm_from_jwt(header.alg)
        .filter(|algorithm| installed.config.algorithms.binary_search(algorithm).is_ok())
        .ok_or(McpOAuthTokenVerificationError::Rejected)?;
    let kid = header
        .kid
        .filter(|value| stable_jwt_identifier(value))
        .ok_or(McpOAuthTokenVerificationError::Rejected)?;
    if !valid_type {
        return Err(McpOAuthTokenVerificationError::Rejected);
    }
    let key = installed
        .keys
        .keys
        .iter()
        .find(|key| key.common.key_id.as_deref() == Some(kid.as_str()))
        .filter(|key| jwk_is_valid_signature_key(key, header.alg))
        .ok_or(McpOAuthTokenVerificationError::Rejected)?;
    let decoding_key =
        DecodingKey::from_jwk(key).map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
    let mut validation = Validation::new(header.alg);
    validation.set_audience(&[audience]);
    validation.set_issuer(&[issuer]);
    validation.set_required_spec_claims(&["aud", "exp", "iat", "iss", "sub"]);
    validation.leeway = u64::from(maximum_clock_skew_seconds);
    validation.validate_nbf = true;
    let claims = decode::<serde_json::Value>(raw, &decoding_key, &validation)
        .map_err(|_| McpOAuthTokenVerificationError::Rejected)?
        .claims;
    let expires_at = claims
        .get("exp")
        .and_then(serde_json::Value::as_u64)
        .ok_or(McpOAuthTokenVerificationError::Rejected)?;
    let issued_at = claims
        .get("iat")
        .and_then(serde_json::Value::as_u64)
        .ok_or(McpOAuthTokenVerificationError::Rejected)?;
    let now =
        u64::try_from(now.timestamp()).map_err(|_| McpOAuthTokenVerificationError::Rejected)?;
    let skew = u64::from(maximum_clock_skew_seconds);
    if expires_at <= issued_at
        || expires_at <= now
        || issued_at > now.saturating_add(skew)
        || expires_at > now.saturating_add(MAX_MCP_OAUTH_TOKEN_LIFETIME_SECONDS_HARD as u64)
    {
        return Err(McpOAuthTokenVerificationError::Rejected);
    }
    let subject = claims
        .get("sub")
        .and_then(serde_json::Value::as_str)
        .filter(|value| stable_jwt_identifier(value))
        .ok_or(McpOAuthTokenVerificationError::Rejected)?
        .to_owned();
    Ok(VerifiedJwt {
        claims,
        subject,
        expires_at,
        kid,
        algorithm,
    })
}

fn installed_algorithm_from_jwt(value: Algorithm) -> Option<InstalledMcpOAuthJwtAlgorithm> {
    match value {
        Algorithm::EdDSA => Some(InstalledMcpOAuthJwtAlgorithm::EdDsa),
        Algorithm::ES256 => Some(InstalledMcpOAuthJwtAlgorithm::Es256),
        Algorithm::RS256 => Some(InstalledMcpOAuthJwtAlgorithm::Rs256),
        _ => None,
    }
}

fn jwt_scopes(claims: &serde_json::Value) -> Result<Vec<String>, McpOAuthTokenVerificationError> {
    let mut scopes = BTreeSet::new();
    if let Some(scope) = claims.get("scope") {
        let scope = scope
            .as_str()
            .ok_or(McpOAuthTokenVerificationError::Rejected)?;
        for value in scope.split_ascii_whitespace() {
            if !valid_oauth_scope(value) || !scopes.insert(value.to_owned()) {
                return Err(McpOAuthTokenVerificationError::Rejected);
            }
        }
    }
    if let Some(scp) = claims.get("scp") {
        match scp {
            serde_json::Value::String(value) => {
                for value in value.split_ascii_whitespace() {
                    if !valid_oauth_scope(value) || !scopes.insert(value.to_owned()) {
                        return Err(McpOAuthTokenVerificationError::Rejected);
                    }
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    let value = value
                        .as_str()
                        .filter(|value| valid_oauth_scope(value))
                        .ok_or(McpOAuthTokenVerificationError::Rejected)?;
                    if !scopes.insert(value.to_owned()) {
                        return Err(McpOAuthTokenVerificationError::Rejected);
                    }
                }
            }
            _ => return Err(McpOAuthTokenVerificationError::Rejected),
        }
    }
    if scopes.is_empty() || scopes.len() > insight_platform_contracts::MAX_MCP_OAUTH_SCOPES {
        return Err(McpOAuthTokenVerificationError::Rejected);
    }
    Ok(scopes.into_iter().collect())
}

fn valid_oauth_scope(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_graphic() && !matches!(*byte, b'"' | b'\\'))
}

fn stable_jwt_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value.is_ascii()
        && !value.chars().any(char::is_control)
}

fn jwk_is_valid_signature_key(jwk: &Jwk, algorithm: Algorithm) -> bool {
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

/// Stable identity for the external prepared-token record. The authorization code itself never
/// leaves Egress; only its domain-separated digest participates in this identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthTokenPreparation {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub task_generation: u64,
    pub task_version: u64,
    pub authorization_binding_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub state_digest: Sha256Digest,
    pub authorization_code_digest: Sha256Digest,
    pub token_credential_purpose: SecretPurpose,
    pub token_secret_provider_id: ResourceId,
    pub requested_scopes: Vec<String>,
    pub audience_identity_digest: Sha256Digest,
    pub issuer_identity_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
    pub preparation_digest: Sha256Digest,
}

impl McpOAuthTokenPreparation {
    pub fn from_exchange(
        contract: &McpOAuthExchangeContract,
        authorization_code: &SensitiveOAuthValue,
    ) -> Result<Self, McpOAuthCredentialBrokerError> {
        let authorization_code_digest = sensitive_sha256(
            b"insight.platform/v1:mcp_oauth_authorization_code\0",
            authorization_code.as_bytes(),
        )?;
        let preparation_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "authorization_binding_id": contract.binding.authorization_binding_id,
            "authorization_code_digest": authorization_code_digest,
            "domain": "mcp_oauth_token_preparation_v1",
            "expires_at": contract.task_deadline,
            "mcp_deployment": contract.binding.mcp_deployment,
            "schema_version": 1,
            "state_digest": contract.binding.state_digest,
            "task_generation": contract.task_generation,
            "task_id": contract.task_id,
            "task_version": contract.task_version,
            "tenant_id": contract.tenant_id,
            "token_credential_purpose": contract.binding.token_credential_purpose,
            "token_secret_provider_id": contract.auth_profile.token_secret_provider_id,
            "requested_scopes": contract.binding.requested_scopes,
            "audience_identity_digest": contract.binding.audience_identity_digest,
            "issuer_identity_digest": contract.auth_profile.issuer.endpoint_identity_digest,
        }))
        .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?
        .parse()
        .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
        let preparation = Self {
            schema_version: 1,
            tenant_id: contract.tenant_id.clone(),
            task_id: contract.task_id.clone(),
            task_generation: contract.task_generation,
            task_version: contract.task_version,
            authorization_binding_id: contract.binding.authorization_binding_id.clone(),
            mcp_deployment: contract.binding.mcp_deployment.clone(),
            state_digest: contract.binding.state_digest.clone(),
            authorization_code_digest,
            token_credential_purpose: contract.binding.token_credential_purpose.clone(),
            token_secret_provider_id: contract.auth_profile.token_secret_provider_id.clone(),
            requested_scopes: contract.binding.requested_scopes.clone(),
            audience_identity_digest: contract.binding.audience_identity_digest.clone(),
            issuer_identity_digest: contract
                .auth_profile
                .issuer
                .endpoint_identity_digest
                .clone(),
            expires_at: contract.task_deadline,
            preparation_digest,
        };
        preparation.validate_for(contract)?;
        Ok(preparation)
    }

    fn validate_for(
        &self,
        contract: &McpOAuthExchangeContract,
    ) -> Result<(), McpOAuthCredentialBrokerError> {
        if self.schema_version != 1
            || self.tenant_id != contract.tenant_id
            || self.task_id != contract.task_id
            || self.task_generation != contract.task_generation
            || self.task_version != contract.task_version
            || self.authorization_binding_id != contract.binding.authorization_binding_id
            || self.mcp_deployment != contract.binding.mcp_deployment
            || self.state_digest != contract.binding.state_digest
            || self.token_credential_purpose != contract.binding.token_credential_purpose
            || self.token_secret_provider_id != contract.auth_profile.token_secret_provider_id
            || self.requested_scopes != contract.binding.requested_scopes
            || self.audience_identity_digest != contract.binding.audience_identity_digest
            || self.issuer_identity_digest != contract.auth_profile.issuer.endpoint_identity_digest
            || self.expires_at != contract.task_deadline
            || self.task_id.kind() != ResourceKind::Interaction
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
        {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        Ok(())
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpOAuthTokenStoreError> {
        let expected: Sha256Digest = canonical_digest(&serde_json::json!({
            "authorization_binding_id": self.authorization_binding_id,
            "authorization_code_digest": self.authorization_code_digest,
            "domain": "mcp_oauth_token_preparation_v1",
            "expires_at": self.expires_at,
            "mcp_deployment": self.mcp_deployment,
            "schema_version": self.schema_version,
            "state_digest": self.state_digest,
            "task_generation": self.task_generation,
            "task_id": self.task_id,
            "task_version": self.task_version,
            "tenant_id": self.tenant_id,
            "token_credential_purpose": self.token_credential_purpose,
            "token_secret_provider_id": self.token_secret_provider_id,
            "requested_scopes": self.requested_scopes,
            "audience_identity_digest": self.audience_identity_digest,
            "issuer_identity_digest": self.issuer_identity_digest,
        }))
        .map_err(|_| McpOAuthTokenStoreError::Rejected)?
        .parse()
        .map_err(|_| McpOAuthTokenStoreError::Rejected)?;
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.task_id.kind() != ResourceKind::Interaction
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.token_secret_provider_id.kind() != ResourceKind::SecretProvider
            || self.expires_at <= now
            || self.requested_scopes.is_empty()
            || self.requested_scopes.len() > insight_platform_contracts::MAX_MCP_OAUTH_SCOPES
            || !self
                .requested_scopes
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self.requested_scopes.iter().any(|scope| {
                let bytes = scope.as_bytes();
                bytes.is_empty()
                    || bytes.len() > 256
                    || !bytes
                        .iter()
                        .all(|byte| byte.is_ascii_graphic() && !matches!(*byte, b'"' | b'\\'))
            })
            || expected != self.preparation_digest
        {
            return Err(McpOAuthTokenStoreError::Rejected);
        }
        Ok(())
    }
}

/// Prepared token record returned by the trusted Secret Manager composition. Raw tokens remain in
/// the external store; this closed projection is sufficient to replay the Host grant safely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMcpOAuthTokenSecret {
    pub schema_version: u32,
    pub preparation_digest: Sha256Digest,
    pub token_secret_binding: ExactSecretBindingRef,
    pub granted_scopes: Vec<String>,
    pub audience_identity_digest: Sha256Digest,
    pub issuer_identity_digest: Sha256Digest,
    pub subject_identity_digest: Sha256Digest,
    pub verification_evidence_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
    pub storage_evidence_digest: Sha256Digest,
}

impl StoredMcpOAuthTokenSecret {
    pub fn validate_for_preparation(
        &self,
        preparation: &McpOAuthTokenPreparation,
        now: DateTime<Utc>,
    ) -> Result<(), McpOAuthTokenStoreError> {
        preparation.validate_at(now)?;
        if self.schema_version != 1
            || self.preparation_digest != preparation.preparation_digest
            || self.token_secret_binding.validate().is_err()
            || self.token_secret_binding.purpose != preparation.token_credential_purpose
            || self.token_secret_binding.provider_id != preparation.token_secret_provider_id
            || !matches!(
                &self.token_secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
            || self.granted_scopes.is_empty()
            || !self.granted_scopes.windows(2).all(|pair| pair[0] < pair[1])
            || !self
                .granted_scopes
                .iter()
                .all(|scope| preparation.requested_scopes.binary_search(scope).is_ok())
            || self.audience_identity_digest != preparation.audience_identity_digest
            || self.issuer_identity_digest != preparation.issuer_identity_digest
            || self.expires_at <= now
        {
            return Err(McpOAuthTokenStoreError::Rejected);
        }
        Ok(())
    }

    fn validate_for(
        &self,
        preparation: &McpOAuthTokenPreparation,
        contract: &McpOAuthExchangeContract,
        now: DateTime<Utc>,
    ) -> Result<(), McpOAuthCredentialBrokerError> {
        self.validate_for_preparation(preparation, now)
            .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
        if !contract.auth_profile.permits_scopes(&self.granted_scopes)
            || !self.granted_scopes.iter().all(|scope| {
                contract
                    .binding
                    .requested_scopes
                    .binary_search(scope)
                    .is_ok()
            })
            || self.audience_identity_digest != contract.binding.audience_identity_digest
            || self.expires_at <= now
        {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthTokenStoreError {
    Rejected,
    TemporarilyUnavailable,
    WriteUncertain,
}

/// Trusted Secret Manager composition. The implementation must use a short-lived prepared entry
/// and return only an active, pinned SecretBinding; raw tokens never cross into Host or PostgreSQL.
#[async_trait]
pub trait McpOAuthTokenStore: Send + Sync {
    /// Loads a prior prepared winner before the one-time authorization code is exchanged again.
    async fn load_prepared(
        &self,
        preparation: &McpOAuthTokenPreparation,
        now: DateTime<Utc>,
    ) -> Result<Option<StoredMcpOAuthTokenSecret>, McpOAuthTokenStoreError>;

    async fn store_prepared(
        &self,
        preparation: &McpOAuthTokenPreparation,
        tokens: &McpOAuthTokenSet,
        verified: &VerifiedMcpOAuthToken,
        now: DateTime<Utc>,
    ) -> Result<StoredMcpOAuthTokenSecret, McpOAuthTokenStoreError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactSecretVersionDeleteDisposition {
    Deleted,
    AlreadyAbsent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactSecretVersionDeleteError {
    Rejected,
    TemporarilyUnavailable,
    OutcomeUncertain,
}

/// External Secret Manager client port owned by the Egress role.
#[async_trait]
pub trait ExactSecretVersionDeleter: Send + Sync {
    async fn delete_exact_version(
        &self,
        tenant_id: &ResourceId,
        binding: &ExactSecretBindingRef,
    ) -> Result<ExactSecretVersionDeleteDisposition, ExactSecretVersionDeleteError>;
}

/// Adapter from the MCP cleanup contract to the role-scoped Secret Manager client.
pub struct BrokeredMcpOAuthPkceSecretCleaner {
    deleter: Arc<dyn ExactSecretVersionDeleter>,
}

impl BrokeredMcpOAuthPkceSecretCleaner {
    pub fn new(deleter: Arc<dyn ExactSecretVersionDeleter>) -> Self {
        Self { deleter }
    }
}

#[async_trait]
impl McpOAuthPkceSecretCleaner for BrokeredMcpOAuthPkceSecretCleaner {
    async fn delete_exact(
        &self,
        authorization: &AuthorizedMcpOAuthPkceCleanup,
    ) -> Result<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError> {
        if authorization.secret_binding.validate().is_err()
            || authorization.secret_binding.purpose.as_str()
                != insight_platform_mcp_host::MCP_OAUTH_PKCE_SECRET_PURPOSE
            || !matches!(
                authorization.secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
        {
            return Err(McpOAuthPkceSecretCleanupError::Rejected);
        }
        self.deleter
            .delete_exact_version(&authorization.tenant_id, &authorization.secret_binding)
            .await
            .map(|disposition| match disposition {
                ExactSecretVersionDeleteDisposition::Deleted => {
                    McpOAuthPkceSecretCleanupDisposition::Deleted
                }
                ExactSecretVersionDeleteDisposition::AlreadyAbsent => {
                    McpOAuthPkceSecretCleanupDisposition::AlreadyAbsent
                }
            })
            .map_err(|failure| match failure {
                ExactSecretVersionDeleteError::Rejected => McpOAuthPkceSecretCleanupError::Rejected,
                ExactSecretVersionDeleteError::TemporarilyUnavailable => {
                    McpOAuthPkceSecretCleanupError::TemporarilyUnavailable
                }
                ExactSecretVersionDeleteError::OutcomeUncertain => {
                    McpOAuthPkceSecretCleanupError::OutcomeUncertain
                }
            })
    }
}

struct SensitiveResponseBody(Vec<u8>);

impl Drop for SensitiveResponseBody {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

struct McpOAuthTokenHttpRequest {
    endpoint: CanonicalHttpEndpoint,
    url: Url,
    dns_host: String,
    addresses: Vec<SocketAddr>,
    headers: HeaderMap,
    body: Vec<u8>,
    connect_timeout: Duration,
    total_timeout: Duration,
    maximum_response_bytes: usize,
}

struct McpOAuthTokenHttpResponse {
    status_code: u16,
    content_type: String,
    body: SensitiveResponseBody,
}

#[async_trait]
trait McpOAuthTokenHttpTransport: Send + Sync {
    async fn exchange(
        &self,
        request: McpOAuthTokenHttpRequest,
    ) -> Result<McpOAuthTokenHttpResponse, McpOAuthCredentialBrokerError>;
}

struct ReqwestMcpOAuthTokenHttpTransport;

#[async_trait]
impl McpOAuthTokenHttpTransport for ReqwestMcpOAuthTokenHttpTransport {
    async fn exchange(
        &self,
        request: McpOAuthTokenHttpRequest,
    ) -> Result<McpOAuthTokenHttpResponse, McpOAuthCredentialBrokerError> {
        if request.endpoint.canonical_digest().is_err() {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .no_proxy()
            .https_only(true)
            .connect_timeout(request.connect_timeout)
            .timeout(request.total_timeout)
            .pool_max_idle_per_host(0)
            .resolve_to_addrs(&request.dns_host, &request.addresses)
            .build()
            .map_err(|_| McpOAuthCredentialBrokerError::TemporarilyUnavailable)?;
        let outbound = client
            .post(request.url)
            .headers(request.headers)
            .body(request.body)
            .build()
            .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
        let response = client
            .execute(outbound)
            .await
            .map_err(|_| McpOAuthCredentialBrokerError::ExchangeUncertain)?;
        if response.headers().get_all(CONTENT_TYPE).iter().count() != 1
            || response.headers().get_all(CONTENT_LENGTH).iter().count() > 1
            || response
                .headers()
                .get(CONTENT_ENCODING)
                .is_some_and(|value| value.as_bytes() != b"identity")
            || response
                .content_length()
                .is_some_and(|length| length > request.maximum_response_bytes as u64)
        {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .filter(|value| is_json_content_type(value))
            .ok_or(McpOAuthCredentialBrokerError::Rejected)?
            .to_owned();
        let status_code = response.status().as_u16();
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| McpOAuthCredentialBrokerError::ExchangeUncertain)?;
            if body.len().saturating_add(chunk.len()) > request.maximum_response_bytes {
                body.fill(0);
                return Err(McpOAuthCredentialBrokerError::Rejected);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(McpOAuthTokenHttpResponse {
            status_code,
            content_type,
            body: SensitiveResponseBody(body),
        })
    }
}

pub struct ReqwestMcpOAuthCredentialBroker {
    secrets: Arc<dyn SecretMaterialResolver>,
    dns: Arc<dyn EgressDnsResolver>,
    verification_catalog: InstalledMcpOAuthVerificationCatalog,
    verifier: Arc<dyn McpOAuthTokenVerifier>,
    token_store: Arc<dyn McpOAuthTokenStore>,
    transport: Arc<dyn McpOAuthTokenHttpTransport>,
    limits: McpOAuthEgressLimits,
    permits: Arc<Semaphore>,
}

impl ReqwestMcpOAuthCredentialBroker {
    pub fn new(
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        verification_catalog: InstalledMcpOAuthVerificationCatalog,
        verifier: Arc<dyn McpOAuthTokenVerifier>,
        token_store: Arc<dyn McpOAuthTokenStore>,
        limits: McpOAuthEgressLimits,
    ) -> Result<Self, McpOAuthEgressConfigurationError> {
        Self::with_transport(
            secrets,
            dns,
            verification_catalog,
            verifier,
            token_store,
            Arc::new(ReqwestMcpOAuthTokenHttpTransport),
            limits,
        )
    }

    fn with_transport(
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        verification_catalog: InstalledMcpOAuthVerificationCatalog,
        verifier: Arc<dyn McpOAuthTokenVerifier>,
        token_store: Arc<dyn McpOAuthTokenStore>,
        transport: Arc<dyn McpOAuthTokenHttpTransport>,
        limits: McpOAuthEgressLimits,
    ) -> Result<Self, McpOAuthEgressConfigurationError> {
        limits.validate()?;
        Ok(Self {
            secrets,
            dns,
            verification_catalog,
            verifier,
            token_store,
            transport,
            limits,
            permits: Arc::new(Semaphore::new(limits.maximum_in_flight)),
        })
    }

    async fn resolve_addresses(
        &self,
        endpoint: &CanonicalHttpEndpoint,
    ) -> Result<(String, Vec<SocketAddr>), McpOAuthCredentialBrokerError> {
        let host = parse_endpoint_host(&endpoint.host)
            .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
        let (dns_host, mut addresses) = match host {
            ParsedEndpointHost::Address(address) => (
                address.to_string(),
                vec![SocketAddr::new(address, endpoint.port)],
            ),
            ParsedEndpointHost::Name(host) => {
                let addresses = self.dns.resolve(&host, endpoint.port).await.map_err(
                    |failure| match failure {
                        DnsResolutionError::Unavailable => {
                            McpOAuthCredentialBrokerError::TemporarilyUnavailable
                        }
                        DnsResolutionError::NoAddresses | DnsResolutionError::TooManyAddresses => {
                            McpOAuthCredentialBrokerError::Rejected
                        }
                    },
                )?;
                (host, addresses)
            }
        };
        addresses.sort_unstable();
        addresses.dedup();
        if addresses.is_empty()
            || addresses.len() > self.limits.maximum_dns_answers
            || addresses.iter().any(|address| {
                address.port() != endpoint.port || !is_public_destination_ip(address.ip())
            })
        {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        Ok((dns_host, addresses))
    }

    async fn resolve_secret(
        &self,
        tenant_id: &ResourceId,
        binding: &ExactSecretBindingRef,
    ) -> Result<ResolvedSecretMaterial, McpOAuthCredentialBrokerError> {
        let resolved = self
            .secrets
            .resolve(tenant_id, binding)
            .await
            .map_err(|failure| match failure {
                SecretMaterialResolutionError::Unavailable => {
                    McpOAuthCredentialBrokerError::TemporarilyUnavailable
                }
                SecretMaterialResolutionError::NotFound
                | SecretMaterialResolutionError::Revoked
                | SecretMaterialResolutionError::InvalidEvidence => {
                    McpOAuthCredentialBrokerError::Rejected
                }
            })?;
        if !resolved.validate_for(binding, self.limits.maximum_secret_material_bytes) {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        Ok(resolved)
    }

    async fn build_request(
        &self,
        contract: &McpOAuthExchangeContract,
        authorization_code: &SensitiveOAuthValue,
        now: DateTime<Utc>,
    ) -> Result<McpOAuthTokenHttpRequest, McpOAuthCredentialBrokerError> {
        let endpoint = &contract.auth_profile.token_endpoint.endpoint;
        let (dns_host, addresses) = self.resolve_addresses(endpoint).await?;
        let pkce = self
            .resolve_secret(&contract.tenant_id, &contract.binding.pkce_secret_binding)
            .await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        let client_id_in_body = match contract.auth_profile.client_authentication {
            McpOAuthClientAuthenticationKind::None => true,
            McpOAuthClientAuthenticationKind::ClientSecretBasic => {
                let purpose = contract
                    .auth_profile
                    .client_credential_purpose
                    .as_ref()
                    .ok_or(McpOAuthCredentialBrokerError::Rejected)?;
                let binding = contract
                    .deployment_closure
                    .secret_bindings
                    .iter()
                    .find(|binding| &binding.purpose == purpose)
                    .ok_or(McpOAuthCredentialBrokerError::Rejected)?;
                let client_secret = self.resolve_secret(&contract.tenant_id, binding).await?;
                let credentials = basic_credentials(
                    contract.auth_profile.client_id.as_bytes(),
                    client_secret.material.as_bytes(),
                )?;
                let mut value = HeaderValue::from_bytes(&credentials)
                    .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
                value.set_sensitive(true);
                headers.insert(AUTHORIZATION, value);
                false
            }
        };
        let redirect_uri = exact_endpoint_url(&contract.auth_profile.redirect_uri.endpoint)?;
        let resource = exact_endpoint_url(&contract.auth_profile.resource_indicator.endpoint)?;
        let mut body = Vec::new();
        push_form_field(&mut body, b"grant_type", b"authorization_code");
        push_form_field(&mut body, b"code", authorization_code.as_bytes());
        push_form_field(&mut body, b"code_verifier", pkce.material.as_bytes());
        push_form_field(&mut body, b"redirect_uri", redirect_uri.as_str().as_bytes());
        push_form_field(&mut body, b"resource", resource.as_str().as_bytes());
        if client_id_in_body {
            push_form_field(
                &mut body,
                b"client_id",
                contract.auth_profile.client_id.as_bytes(),
            );
        }
        let url = exact_endpoint_url(endpoint)?;
        let remaining = (contract.task_deadline - now)
            .to_std()
            .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
        let total_timeout =
            Duration::from_millis(contract.auth_profile.total_timeout_milliseconds).min(remaining);
        let connect_timeout =
            Duration::from_millis(contract.auth_profile.connect_timeout_milliseconds)
                .min(total_timeout);
        if connect_timeout.is_zero() || total_timeout.is_zero() {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        Ok(McpOAuthTokenHttpRequest {
            endpoint: endpoint.clone(),
            url,
            dns_host,
            addresses,
            headers,
            body,
            connect_timeout,
            total_timeout,
            maximum_response_bytes: contract.auth_profile.maximum_token_response_bytes as usize,
        })
    }
}

#[async_trait]
impl McpOAuthCredentialBroker for ReqwestMcpOAuthCredentialBroker {
    async fn exchange_authorization_code(
        &self,
        contract: &McpOAuthExchangeContract,
        authorization_code: SensitiveOAuthValue,
        now: DateTime<Utc>,
    ) -> Result<McpOAuthAuthorizedGrant, McpOAuthCredentialBrokerError> {
        contract
            .validate_at(now)
            .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
        self.verification_catalog.resolve(contract)?;
        let _permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| McpOAuthCredentialBrokerError::TemporarilyUnavailable)?;
        let preparation = McpOAuthTokenPreparation::from_exchange(contract, &authorization_code)?;
        if let Some(stored) = self
            .token_store
            .load_prepared(&preparation, now)
            .await
            .map_err(map_token_store_error)?
        {
            return authorized_grant_from_stored(&preparation, contract, stored, now);
        }
        let request = self
            .build_request(contract, &authorization_code, now)
            .await?;
        let response = self.transport.exchange(request).await?;
        if response.status_code != 200 || !is_json_content_type(&response.content_type) {
            return Err(
                if response.status_code == 429 || response.status_code >= 500 {
                    McpOAuthCredentialBrokerError::ExchangeUncertain
                } else {
                    McpOAuthCredentialBrokerError::Rejected
                },
            );
        }
        let mut deserializer = serde_json::Deserializer::from_slice(&response.body.0);
        let mut tokens = McpOAuthTokenSet::deserialize(&mut deserializer)
            .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
        deserializer
            .end()
            .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
        if tokens.expires_in_seconds <= 0
            || tokens.expires_in_seconds > self.limits.maximum_token_lifetime_seconds
        {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        if tokens.granted_scopes.is_empty() {
            tokens.granted_scopes = contract.binding.requested_scopes.clone();
        }
        if !contract.auth_profile.permits_scopes(&tokens.granted_scopes)
            || !tokens.granted_scopes.iter().all(|scope| {
                contract
                    .binding
                    .requested_scopes
                    .binary_search(scope)
                    .is_ok()
            })
        {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        let verified = self
            .verifier
            .verify(contract, &tokens, now)
            .await
            .map_err(|failure| match failure {
                McpOAuthTokenVerificationError::Rejected => McpOAuthCredentialBrokerError::Rejected,
                McpOAuthTokenVerificationError::TemporarilyUnavailable => {
                    McpOAuthCredentialBrokerError::TemporarilyUnavailable
                }
            })?;
        validate_verified_token(contract, &tokens, &verified, now, self.limits)?;
        let stored = self
            .token_store
            .store_prepared(&preparation, &tokens, &verified, now)
            .await
            .map_err(map_token_store_error)?;
        if stored.granted_scopes != verified.granted_scopes
            || stored.audience_identity_digest != verified.audience_identity_digest
            || stored.issuer_identity_digest != verified.issuer_identity_digest
            || stored.subject_identity_digest != verified.subject_identity_digest
            || stored.verification_evidence_digest != verified.verification_evidence_digest
            || stored.expires_at != verified.expires_at
        {
            return Err(McpOAuthCredentialBrokerError::Rejected);
        }
        authorized_grant_from_stored(&preparation, contract, stored, now)
    }
}

fn authorized_grant_from_stored(
    preparation: &McpOAuthTokenPreparation,
    contract: &McpOAuthExchangeContract,
    stored: StoredMcpOAuthTokenSecret,
    now: DateTime<Utc>,
) -> Result<McpOAuthAuthorizedGrant, McpOAuthCredentialBrokerError> {
    stored.validate_for(preparation, contract, now)?;
    let exchange_evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "authorization_binding_id": contract.binding.authorization_binding_id,
            "issuer_identity_digest": stored.issuer_identity_digest,
            "preparation_digest": stored.preparation_digest,
            "storage_evidence_digest": stored.storage_evidence_digest,
            "token_endpoint_identity_digest": contract.auth_profile.token_endpoint.endpoint_identity_digest,
            "verification_evidence_digest": stored.verification_evidence_digest,
        }))
        .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?
        .parse()
        .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
    McpOAuthAuthorizedGrant::build(
        stored.granted_scopes,
        stored.token_secret_binding,
        stored.audience_identity_digest,
        stored.issuer_identity_digest,
        stored.subject_identity_digest,
        exchange_evidence_digest,
        stored.expires_at,
    )
    .map_err(|_| McpOAuthCredentialBrokerError::Rejected)
}

fn map_token_store_error(failure: McpOAuthTokenStoreError) -> McpOAuthCredentialBrokerError {
    match failure {
        McpOAuthTokenStoreError::Rejected => McpOAuthCredentialBrokerError::Rejected,
        McpOAuthTokenStoreError::TemporarilyUnavailable => {
            McpOAuthCredentialBrokerError::TemporarilyUnavailable
        }
        McpOAuthTokenStoreError::WriteUncertain => McpOAuthCredentialBrokerError::ExchangeUncertain,
    }
}

fn sensitive_sha256(
    domain: &[u8],
    value: &[u8],
) -> Result<Sha256Digest, McpOAuthCredentialBrokerError> {
    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    context.update(domain);
    context.update(value);
    let digest = context.finish();
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest.as_ref() {
        write!(&mut encoded, "{byte:02x}").map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
    }
    Sha256Digest::from_str(&encoded).map_err(|_| McpOAuthCredentialBrokerError::Rejected)
}

fn validate_verified_token(
    contract: &McpOAuthExchangeContract,
    tokens: &McpOAuthTokenSet,
    verified: &VerifiedMcpOAuthToken,
    now: DateTime<Utc>,
    limits: McpOAuthEgressLimits,
) -> Result<(), McpOAuthCredentialBrokerError> {
    let response_expiry = now
        .checked_add_signed(ChronoDuration::seconds(tokens.expires_in_seconds))
        .ok_or(McpOAuthCredentialBrokerError::Rejected)?;
    let maximum_expiry = now
        .checked_add_signed(ChronoDuration::seconds(
            limits.maximum_token_lifetime_seconds,
        ))
        .ok_or(McpOAuthCredentialBrokerError::Rejected)?;
    let skew = ChronoDuration::seconds(contract.auth_profile.maximum_clock_skew_seconds as i64);
    if !verified.nonce_verified
        || verified.granted_scopes != tokens.granted_scopes
        || verified.audience_identity_digest != contract.binding.audience_identity_digest
        || verified.issuer_identity_digest != contract.auth_profile.issuer.endpoint_identity_digest
        || verified.expires_at <= now
        || verified.expires_at > maximum_expiry
        || verified.expires_at > response_expiry + skew
        || !verified.granted_scopes.iter().all(|scope| {
            contract
                .binding
                .requested_scopes
                .binary_search(scope)
                .is_ok()
        })
    {
        return Err(McpOAuthCredentialBrokerError::Rejected);
    }
    Ok(())
}

fn exact_endpoint_url(
    endpoint: &CanonicalHttpEndpoint,
) -> Result<Url, McpOAuthCredentialBrokerError> {
    endpoint
        .validate()
        .map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
    let raw = format!(
        "https://{}:{}{}",
        endpoint.host, endpoint.port, endpoint.base_path
    );
    let url = Url::parse(&raw).map_err(|_| McpOAuthCredentialBrokerError::Rejected)?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host_str() != Some(endpoint.host.trim_matches(['[', ']']))
        || url.port_or_known_default() != Some(endpoint.port)
        || url.path() != endpoint.base_path
    {
        return Err(McpOAuthCredentialBrokerError::Rejected);
    }
    Ok(url)
}

fn is_json_content_type(value: &str) -> bool {
    let mut parts = value.split(';');
    if parts.next().map(str::trim) != Some("application/json") {
        return false;
    }
    parts.all(|part| part.trim().eq_ignore_ascii_case("charset=utf-8"))
}

fn basic_credentials(
    client_id: &[u8],
    client_secret: &[u8],
) -> Result<Vec<u8>, McpOAuthCredentialBrokerError> {
    if client_id.is_empty()
        || client_secret.is_empty()
        || client_id.iter().any(|byte| byte.is_ascii_control())
        || client_secret.iter().any(|byte| byte.is_ascii_control())
    {
        return Err(McpOAuthCredentialBrokerError::Rejected);
    }
    let mut source = Vec::new();
    percent_encode_into(&mut source, client_id);
    source.push(b':');
    percent_encode_into(&mut source, client_secret);
    let encoded = BASE64_STANDARD.encode(&source);
    source.fill(0);
    let mut header = b"Basic ".to_vec();
    header.extend_from_slice(encoded.as_bytes());
    Ok(header)
}

fn push_form_field(target: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    if !target.is_empty() {
        target.push(b'&');
    }
    percent_encode_into(target, name);
    target.push(b'=');
    percent_encode_into(target, value);
}

fn percent_encode_into(target: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_' | b'~') {
            target.push(*byte);
        } else {
            target.push(b'%');
            target.push(HEX[(byte >> 4) as usize]);
            target.push(HEX[(byte & 0x0f) as usize]);
        }
    }
}

const HEX: &[u8; 16] = b"0123456789ABCDEF";

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        ArtifactRef, CapabilityEndpointScheme, DataClassification, ExactDeploymentRef,
        ExactVersionRef, McpAuthPolicyDocument, McpDeploymentClosure, McpOAuthEndpoint,
        McpOAuthTaskBinding, McpServerExecutionContract, McpServerLimits, McpTransportBinding,
        McpTransportKind, ResourceKind, SecretPurpose,
    };
    use insight_platform_mcp_host::{
        mcp_oauth_nonce_digest, parse_mcp_oauth_callback_query, McpOAuthCallbackWireOutcome,
        MCP_OAUTH_PKCE_SECRET_PURPOSE,
    };
    use jsonwebtoken::{encode, EncodingKey, Header};
    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair},
    };
    use std::{
        net::Ipv4Addr,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        sync::Mutex,
    };

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), digest(character)).unwrap()
    }

    fn endpoint(host: &str, path: &str) -> McpOAuthEndpoint {
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: host.to_owned(),
            port: 443,
            base_path: path.to_owned(),
        };
        McpOAuthEndpoint {
            endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
            endpoint,
        }
    }

    fn secret_binding(suffix: u16, purpose: SecretPurpose, version: char) -> ExactSecretBindingRef {
        ExactSecretBindingRef::build(
            id(ResourceKind::SecretBinding, suffix),
            1,
            id(ResourceKind::SecretProvider, suffix + 1),
            purpose,
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: digest(version),
            },
        )
        .unwrap()
    }

    fn secret_binding_with_provider(
        suffix: u16,
        provider_id: ResourceId,
        purpose: SecretPurpose,
        version: char,
    ) -> ExactSecretBindingRef {
        ExactSecretBindingRef::build(
            id(ResourceKind::SecretBinding, suffix),
            1,
            provider_id,
            purpose,
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: digest(version),
            },
        )
        .unwrap()
    }

    fn server_limits() -> McpServerLimits {
        McpServerLimits {
            maximum_message_bytes: 8_192,
            maximum_response_bytes: 8_192,
            maximum_headers: 32,
            maximum_sse_event_bytes: 4_096,
            maximum_in_flight: 8,
            maximum_connections: 4,
            maximum_sessions: 4,
            maximum_session_milliseconds: 3_600_000,
            idle_timeout_milliseconds: 1_000,
            initialize_timeout_milliseconds: 5_000,
            request_timeout_milliseconds: 30_000,
            total_timeout_milliseconds: 60_000,
        }
    }

    fn contract(now: DateTime<Utc>) -> McpOAuthExchangeContract {
        let issuer = endpoint("auth.example.test", "/");
        let redirect_uri = endpoint("platform.example.test", "/v1/mcp/oauth/callback");
        let resource_indicator = endpoint("mcp.example.test", "/mcp");
        let auth_profile = McpAuthPolicyDocument {
            schema_version: 1,
            issuer,
            authorization_endpoint: endpoint("auth.example.test", "/oauth/authorize"),
            token_endpoint: endpoint("auth.example.test", "/oauth/token"),
            client_id: "insight-platform".to_owned(),
            client_authentication: McpOAuthClientAuthenticationKind::None,
            client_credential_purpose: None,
            pkce_secret_provider_id: id(ResourceKind::SecretProvider, 8),
            token_secret_provider_id: id(ResourceKind::SecretProvider, 9),
            redirect_uri: redirect_uri.clone(),
            resource_indicator: resource_indicator.clone(),
            allowed_scopes: vec![
                "openid".to_owned(),
                "tools.call".to_owned(),
                "tools.read".to_owned(),
            ],
            maximum_token_response_bytes: 65_536,
            connect_timeout_milliseconds: 5_000,
            total_timeout_milliseconds: 30_000,
            maximum_clock_skew_seconds: 60,
        };
        let auth_policy = ExactVersionRef::new(
            id(ResourceKind::PolicyRevision, 10),
            auth_profile.canonical_digest().unwrap(),
        )
        .unwrap();
        let server_revision = exact(ResourceKind::McpServerRevision, 11, '1');
        let protocol_policy = exact(ResourceKind::PolicyRevision, 12, '2');
        let network_policy = exact(ResourceKind::PolicyRevision, 13, '3');
        let tls_policy = exact(ResourceKind::PolicyRevision, 14, '4');
        let mcp_endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "mcp.example.test".to_owned(),
            port: 443,
            base_path: "/mcp".to_owned(),
        };
        let token_purpose: SecretPurpose = "mcp.oauth.token".parse().unwrap();
        let binding = McpOAuthTaskBinding {
            authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 15),
            mcp_deployment: ExactDeploymentRef::new(
                id(ResourceKind::McpDeployment, 16),
                digest('5'),
            )
            .unwrap(),
            auth_policy: auth_policy.clone(),
            principal_binding_generation: 1,
            audience_identity_digest: resource_indicator.endpoint_identity_digest.clone(),
            requested_scopes: vec![
                "openid".to_owned(),
                "tools.call".to_owned(),
                "tools.read".to_owned(),
            ],
            token_credential_purpose: token_purpose.clone(),
            state_digest: digest('6'),
            nonce_digest: digest('7'),
            callback_binding_digest: redirect_uri.endpoint_identity_digest.clone(),
            pkce_secret_binding: Box::new(secret_binding_with_provider(
                17,
                auth_profile.pkce_secret_provider_id.clone(),
                MCP_OAUTH_PKCE_SECRET_PURPOSE.parse().unwrap(),
                '8',
            )),
            expected_authorization_generation: None,
            expected_authorization_version: None,
        };
        let server = McpServerExecutionContract::build(
            server_revision.clone(),
            McpTransportKind::StreamableHttp,
            protocol_policy.clone(),
            vec![],
            Some(token_purpose),
            server_limits(),
        )
        .unwrap();
        let contract = McpOAuthExchangeContract {
            tenant_id: id(ResourceKind::Tenant, 19),
            task_id: id(ResourceKind::Interaction, 20),
            task_generation: 1,
            task_version: 1,
            task_deadline: now + ChronoDuration::minutes(5),
            binding,
            deployment_closure: McpDeploymentClosure {
                server_revision,
                server_identity_digest: resource_indicator.endpoint_identity_digest,
                transport: McpTransportBinding::StreamableHttp {
                    endpoint_identity_digest: mcp_endpoint.canonical_digest().unwrap(),
                    endpoint: mcp_endpoint,
                    network_policy,
                    tls_policy,
                },
                protocol_policy,
                trust_policy: exact(ResourceKind::PolicyRevision, 21, '9'),
                auth_policy: Some(auth_policy),
                secret_bindings: vec![],
                conformance_evidence: ArtifactRef::new(
                    id(ResourceKind::Artifact, 22),
                    digest('a'),
                    64,
                    "application/json",
                    DataClassification::Internal,
                    Some("mcp-oauth.json".to_owned()),
                )
                .unwrap(),
            },
            server,
            auth_profile,
        };
        contract.validate_at(now).unwrap();
        contract
    }

    fn verification_catalog(
        contract: &McpOAuthExchangeContract,
    ) -> InstalledMcpOAuthVerificationCatalog {
        verification_key_fixture(contract).0
    }

    fn verification_key_fixture(
        contract: &McpOAuthExchangeContract,
    ) -> (InstalledMcpOAuthVerificationCatalog, EncodingKey) {
        let (binding, encoding) = verification_binding_fixture(contract);
        (
            InstalledMcpOAuthVerificationCatalog::new(vec![binding]).unwrap(),
            encoding,
        )
    }

    fn verification_binding_fixture(
        contract: &McpOAuthExchangeContract,
    ) -> (InstalledMcpOAuthVerificationBinding, EncodingKey) {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let encoding = EncodingKey::from_ed_der(pkcs8.as_ref());
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let jwks = serde_json::json!({
            "keys": [{
                "alg": "EdDSA",
                "crv": "Ed25519",
                "kid": "key-1",
                "kty": "OKP",
                "use": "sig",
                "x": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                    key_pair.public_key().as_ref()
                ),
            }]
        });
        let jwks_digest = canonical_digest(&jwks).unwrap().parse().unwrap();
        (
            InstalledMcpOAuthVerificationBinding {
                schema_version: 1,
                auth_policy: contract.binding.auth_policy.clone(),
                auth_profile: contract.auth_profile.clone(),
                algorithms: vec![InstalledMcpOAuthJwtAlgorithm::EdDsa],
                jwks,
                jwks_digest,
            },
            encoding,
        )
    }

    struct FixtureSecrets {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl SecretMaterialResolver for FixtureSecrets {
        async fn resolve(
            &self,
            _tenant_id: &ResourceId,
            binding: &ExactSecretBindingRef,
        ) -> Result<ResolvedSecretMaterial, SecretMaterialResolutionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ResolvedSecretMaterial::new(
                binding.secret_binding_id.clone(),
                binding.provider_id.clone(),
                binding.purpose.clone(),
                binding.binding_generation,
                match &binding.resolution_policy {
                    SecretResolutionPolicy::Pinned {
                        opaque_version_identity_digest,
                    } => opaque_version_identity_digest.clone(),
                    SecretResolutionPolicy::FollowProviderRotation { .. } => digest('f'),
                },
                b"pkce-verifier".to_vec(),
            )
            .map_err(|_| SecretMaterialResolutionError::InvalidEvidence)
        }
    }

    struct FixtureDns {
        address: Ipv4Addr,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl EgressDnsResolver for FixtureDns {
        async fn resolve(
            &self,
            _host: &str,
            port: u16,
        ) -> Result<Vec<SocketAddr>, DnsResolutionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![SocketAddr::new(self.address.into(), port)])
        }
    }

    struct FixtureVerifier {
        called: AtomicBool,
    }

    #[async_trait]
    impl McpOAuthTokenVerifier for FixtureVerifier {
        async fn verify(
            &self,
            contract: &McpOAuthExchangeContract,
            tokens: &McpOAuthTokenSet,
            now: DateTime<Utc>,
        ) -> Result<VerifiedMcpOAuthToken, McpOAuthTokenVerificationError> {
            self.called.store(true, Ordering::SeqCst);
            assert_eq!(tokens.access_token.expose(), b"access-secret");
            Ok(VerifiedMcpOAuthToken {
                granted_scopes: tokens.granted_scopes.clone(),
                audience_identity_digest: contract.binding.audience_identity_digest.clone(),
                issuer_identity_digest: contract
                    .auth_profile
                    .issuer
                    .endpoint_identity_digest
                    .clone(),
                subject_identity_digest: digest('b'),
                verification_evidence_digest: digest('c'),
                expires_at: now + ChronoDuration::seconds(600),
                nonce_verified: true,
            })
        }
    }

    struct FixtureStore {
        load_calls: AtomicUsize,
        store_calls: AtomicUsize,
        prepared: Mutex<Option<StoredMcpOAuthTokenSecret>>,
    }

    #[async_trait]
    impl McpOAuthTokenStore for FixtureStore {
        async fn load_prepared(
            &self,
            _preparation: &McpOAuthTokenPreparation,
            _now: DateTime<Utc>,
        ) -> Result<Option<StoredMcpOAuthTokenSecret>, McpOAuthTokenStoreError> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.prepared.lock().unwrap().clone())
        }

        async fn store_prepared(
            &self,
            preparation: &McpOAuthTokenPreparation,
            tokens: &McpOAuthTokenSet,
            verified: &VerifiedMcpOAuthToken,
            _now: DateTime<Utc>,
        ) -> Result<StoredMcpOAuthTokenSecret, McpOAuthTokenStoreError> {
            self.store_calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                tokens.refresh_token.as_ref().unwrap().expose(),
                b"refresh-secret"
            );
            let stored = StoredMcpOAuthTokenSecret {
                schema_version: 1,
                preparation_digest: preparation.preparation_digest.clone(),
                token_secret_binding: secret_binding_with_provider(
                    30,
                    preparation.token_secret_provider_id.clone(),
                    preparation.token_credential_purpose.clone(),
                    'd',
                ),
                granted_scopes: verified.granted_scopes.clone(),
                audience_identity_digest: verified.audience_identity_digest.clone(),
                issuer_identity_digest: verified.issuer_identity_digest.clone(),
                subject_identity_digest: verified.subject_identity_digest.clone(),
                verification_evidence_digest: verified.verification_evidence_digest.clone(),
                expires_at: verified.expires_at,
                storage_evidence_digest: digest('e'),
            };
            *self.prepared.lock().unwrap() = Some(stored.clone());
            Ok(stored)
        }
    }

    struct FixtureExactSecretVersionDeleter {
        calls: AtomicUsize,
        result: Result<ExactSecretVersionDeleteDisposition, ExactSecretVersionDeleteError>,
    }

    #[async_trait]
    impl ExactSecretVersionDeleter for FixtureExactSecretVersionDeleter {
        async fn delete_exact_version(
            &self,
            tenant_id: &ResourceId,
            binding: &ExactSecretBindingRef,
        ) -> Result<ExactSecretVersionDeleteDisposition, ExactSecretVersionDeleteError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(tenant_id.kind(), ResourceKind::Tenant);
            assert_eq!(
                binding.purpose.as_str(),
                insight_platform_mcp_host::MCP_OAUTH_PKCE_SECRET_PURPOSE
            );
            assert!(matches!(
                binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            ));
            self.result
        }
    }

    struct FixtureOAuthTransport {
        called: AtomicUsize,
    }

    #[async_trait]
    impl McpOAuthTokenHttpTransport for FixtureOAuthTransport {
        async fn exchange(
            &self,
            request: McpOAuthTokenHttpRequest,
        ) -> Result<McpOAuthTokenHttpResponse, McpOAuthCredentialBrokerError> {
            self.called.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                request.url.as_str(),
                "https://auth.example.test/oauth/token"
            );
            assert_eq!(request.dns_host, "auth.example.test");
            assert_eq!(
                request.addresses,
                vec![SocketAddr::new(Ipv4Addr::new(8, 8, 8, 8).into(), 443)]
            );
            assert_eq!(request.headers.len(), 2);
            let form = std::str::from_utf8(&request.body).unwrap();
            assert!(form.contains("code=one-time-code"));
            assert!(form.contains("code_verifier=pkce-verifier"));
            assert!(form.contains("client_id=insight-platform"));
            Ok(McpOAuthTokenHttpResponse {
                status_code: 200,
                content_type: "application/json; charset=utf-8".to_owned(),
                body: SensitiveResponseBody(
                    br#"{"access_token":"access-secret","refresh_token":"refresh-secret","token_type":"Bearer","expires_in":600,"scope":"tools.call tools.read"}"#.to_vec(),
                ),
            })
        }
    }

    #[test]
    fn token_response_is_closed_duplicate_safe_and_redacted() {
        let valid = br#"{"access_token":"secret-access","refresh_token":"secret-refresh","token_type":"Bearer","expires_in":600,"scope":"a b"}"#;
        let parsed: McpOAuthTokenSet = serde_json::from_slice(valid).unwrap();
        assert_eq!(parsed.granted_scopes, vec!["a", "b"]);
        let diagnostic = format!("{parsed:?}");
        assert!(!diagnostic.contains("secret-access"));
        assert!(!diagnostic.contains("secret-refresh"));

        let duplicate = br#"{"access_token":"one","access_token":"two","token_type":"Bearer","expires_in":600}"#;
        assert!(serde_json::from_slice::<McpOAuthTokenSet>(duplicate).is_err());
        let unknown =
            br#"{"access_token":"one","token_type":"Bearer","expires_in":600,"extension":"no"}"#;
        assert!(serde_json::from_slice::<McpOAuthTokenSet>(unknown).is_err());
    }

    #[test]
    fn oauth_form_and_basic_auth_are_canonical_and_do_not_log_secret() {
        let mut form = Vec::new();
        push_form_field(&mut form, b"code", b"a+b/c");
        push_form_field(&mut form, b"resource", b"https://mcp.example/mcp");
        assert_eq!(
            std::str::from_utf8(&form).unwrap(),
            "code=a%2Bb%2Fc&resource=https%3A%2F%2Fmcp.example%2Fmcp"
        );
        let header = basic_credentials(b"client", b"s:ecret").unwrap();
        assert!(header.starts_with(b"Basic "));
        assert!(!header.windows(7).any(|window| window == b"s:ecret"));
    }

    #[tokio::test]
    async fn installed_jwks_verifier_binds_policy_access_token_id_token_and_nonce() {
        let now = Utc::now();
        let mut contract = contract(now);
        contract.auth_profile.allowed_scopes = vec![
            "openid".to_owned(),
            "tools.call".to_owned(),
            "tools.read".to_owned(),
        ];
        contract.binding.requested_scopes = contract.auth_profile.allowed_scopes.clone();
        let policy = ExactVersionRef::new(
            contract.binding.auth_policy.revision_id.clone(),
            contract.auth_profile.canonical_digest().unwrap(),
        )
        .unwrap();
        contract.binding.auth_policy = policy.clone();
        contract.deployment_closure.auth_policy = Some(policy);
        let nonce = SensitiveMcpOAuthNonce::new(b"n".repeat(43)).unwrap();
        contract.binding.nonce_digest = mcp_oauth_nonce_digest(&nonce).unwrap();
        contract.validate_at(now).unwrap();

        let (catalog, encoding) = verification_key_fixture(&contract);
        let verifier = InstalledMcpOAuthJwtVerifier::new(catalog);
        let mut access_header = Header::new(Algorithm::EdDSA);
        access_header.kid = Some("key-1".to_owned());
        access_header.typ = Some("at+jwt".to_owned());
        let mut id_header = Header::new(Algorithm::EdDSA);
        id_header.kid = Some("key-1".to_owned());
        id_header.typ = Some("JWT".to_owned());
        let issuer = exact_endpoint_url(&contract.auth_profile.issuer.endpoint).unwrap();
        let audience =
            exact_endpoint_url(&contract.auth_profile.resource_indicator.endpoint).unwrap();
        let issued_at = now.timestamp();
        let expires_at = issued_at + 300;
        let access = encode(
            &access_header,
            &serde_json::json!({
                "aud": audience.as_str(),
                "exp": expires_at,
                "iat": issued_at,
                "iss": issuer.as_str(),
                "scope": "openid tools.call tools.read",
                "sub": "principal-1",
            }),
            &encoding,
        )
        .unwrap();
        let identity = encode(
            &id_header,
            &serde_json::json!({
                "aud": contract.auth_profile.client_id,
                "exp": expires_at,
                "iat": issued_at,
                "iss": issuer.as_str(),
                "nonce": std::str::from_utf8(nonce.as_bytes()).unwrap(),
                "sub": "principal-1",
            }),
            &encoding,
        )
        .unwrap();
        let tokens = McpOAuthTokenSet {
            access_token: SensitiveMcpOAuthToken::new(access.into_bytes()).unwrap(),
            refresh_token: None,
            id_token: Some(SensitiveMcpOAuthToken::new(identity.into_bytes()).unwrap()),
            expires_in_seconds: 300,
            granted_scopes: contract.binding.requested_scopes.clone(),
        };
        let verified = verifier.verify(&contract, &tokens, now).await.unwrap();
        assert!(verified.nonce_verified);
        assert_eq!(verified.granted_scopes, contract.binding.requested_scopes);
        assert_eq!(verified.expires_at.timestamp(), expires_at);

        contract.binding.nonce_digest = digest('f');
        assert_eq!(
            verifier.verify(&contract, &tokens, now).await,
            Err(McpOAuthTokenVerificationError::Rejected)
        );
    }

    #[test]
    fn installed_jwks_catalog_rejects_drift_and_noncanonical_key_order() {
        let contract = contract(Utc::now());
        let (mut drifted, _) = verification_binding_fixture(&contract);
        drifted.auth_profile.client_id = "drifted-client".to_owned();
        assert!(matches!(
            InstalledMcpOAuthVerificationCatalog::new(vec![drifted]),
            Err(McpOAuthEgressConfigurationError::InvalidVerificationCatalog)
        ));

        let (mut unsorted, _) = verification_binding_fixture(&contract);
        let first = unsorted.jwks["keys"][0].clone();
        let mut second = first.clone();
        second["kid"] = serde_json::Value::String("key-0".to_owned());
        unsorted.jwks["keys"] = serde_json::json!([first, second]);
        unsorted.jwks_digest = canonical_digest(&unsorted.jwks).unwrap().parse().unwrap();
        assert!(matches!(
            InstalledMcpOAuthVerificationCatalog::new(vec![unsorted]),
            Err(McpOAuthEgressConfigurationError::InvalidVerificationCatalog)
        ));
    }

    #[tokio::test]
    async fn broker_rejects_uninstalled_auth_policy_before_any_side_effect() {
        let now = Utc::now();
        let installed_contract = contract(now);
        let mut request_contract = installed_contract.clone();
        request_contract.auth_profile.client_id = "another-client".to_owned();
        let policy = ExactVersionRef::new(
            request_contract.binding.auth_policy.revision_id.clone(),
            request_contract.auth_profile.canonical_digest().unwrap(),
        )
        .unwrap();
        request_contract.binding.auth_policy = policy.clone();
        request_contract.deployment_closure.auth_policy = Some(policy);
        request_contract.validate_at(now).unwrap();

        let verifier = Arc::new(FixtureVerifier {
            called: AtomicBool::new(false),
        });
        let store = Arc::new(FixtureStore {
            load_calls: AtomicUsize::new(0),
            store_calls: AtomicUsize::new(0),
            prepared: Mutex::new(None),
        });
        let transport = Arc::new(FixtureOAuthTransport {
            called: AtomicUsize::new(0),
        });
        let secrets = Arc::new(FixtureSecrets {
            calls: AtomicUsize::new(0),
        });
        let dns = Arc::new(FixtureDns {
            address: Ipv4Addr::new(8, 8, 8, 8),
            calls: AtomicUsize::new(0),
        });
        let broker = ReqwestMcpOAuthCredentialBroker::with_transport(
            secrets.clone(),
            dns.clone(),
            verification_catalog(&installed_contract),
            verifier.clone(),
            store.clone(),
            transport.clone(),
            McpOAuthEgressLimits::default(),
        )
        .unwrap();
        let parsed = parse_mcp_oauth_callback_query(b"state=s&code=one-time-code").unwrap();
        let McpOAuthCallbackWireOutcome::AuthorizationCode(code) = parsed.outcome else {
            panic!("fixture must contain a code");
        };

        assert_eq!(
            broker
                .exchange_authorization_code(&request_contract, code, now)
                .await,
            Err(McpOAuthCredentialBrokerError::Rejected)
        );
        assert_eq!(transport.called.load(Ordering::SeqCst), 0);
        assert_eq!(secrets.calls.load(Ordering::SeqCst), 0);
        assert_eq!(dns.calls.load(Ordering::SeqCst), 0);
        assert!(!verifier.called.load(Ordering::SeqCst));
        assert_eq!(store.load_calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.store_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn broker_pins_dns_verifies_before_store_and_returns_only_exact_secret_metadata() {
        let now = Utc::now();
        let contract = contract(now);
        let verifier = Arc::new(FixtureVerifier {
            called: AtomicBool::new(false),
        });
        let store = Arc::new(FixtureStore {
            load_calls: AtomicUsize::new(0),
            store_calls: AtomicUsize::new(0),
            prepared: Mutex::new(None),
        });
        let transport = Arc::new(FixtureOAuthTransport {
            called: AtomicUsize::new(0),
        });
        let secrets = Arc::new(FixtureSecrets {
            calls: AtomicUsize::new(0),
        });
        let dns = Arc::new(FixtureDns {
            address: Ipv4Addr::new(8, 8, 8, 8),
            calls: AtomicUsize::new(0),
        });
        let broker = ReqwestMcpOAuthCredentialBroker::with_transport(
            secrets.clone(),
            dns.clone(),
            verification_catalog(&contract),
            verifier.clone(),
            store.clone(),
            transport.clone(),
            McpOAuthEgressLimits::default(),
        )
        .unwrap();
        let parsed = parse_mcp_oauth_callback_query(b"state=s&code=one-time-code").unwrap();
        let McpOAuthCallbackWireOutcome::AuthorizationCode(code) = parsed.outcome else {
            panic!("fixture must contain a code");
        };
        let grant = broker
            .exchange_authorization_code(&contract, code, now)
            .await
            .unwrap();
        assert_eq!(transport.called.load(Ordering::SeqCst), 1);
        assert_eq!(secrets.calls.load(Ordering::SeqCst), 1);
        assert_eq!(dns.calls.load(Ordering::SeqCst), 1);
        assert!(verifier.called.load(Ordering::SeqCst));
        assert_eq!(store.load_calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.store_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            grant.token_secret_binding.purpose,
            contract.binding.token_credential_purpose
        );
        let diagnostic = format!("{grant:?}");
        assert!(!diagnostic.contains("access-secret"));
        assert!(!diagnostic.contains("refresh-secret"));

        let parsed = parse_mcp_oauth_callback_query(b"state=s&code=one-time-code").unwrap();
        let McpOAuthCallbackWireOutcome::AuthorizationCode(code) = parsed.outcome else {
            panic!("fixture must contain a code");
        };
        let replayed = broker
            .exchange_authorization_code(&contract, code, now)
            .await
            .unwrap();
        assert_eq!(replayed, grant);
        assert_eq!(transport.called.load(Ordering::SeqCst), 1);
        assert_eq!(secrets.calls.load(Ordering::SeqCst), 1);
        assert_eq!(dns.calls.load(Ordering::SeqCst), 1);
        assert!(verifier.called.load(Ordering::SeqCst));
        assert_eq!(store.load_calls.load(Ordering::SeqCst), 2);
        assert_eq!(store.store_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn broker_rejects_private_dns_before_code_dispatch_or_secret_store() {
        let now = Utc::now();
        let contract = contract(now);
        let verifier = Arc::new(FixtureVerifier {
            called: AtomicBool::new(false),
        });
        let store = Arc::new(FixtureStore {
            load_calls: AtomicUsize::new(0),
            store_calls: AtomicUsize::new(0),
            prepared: Mutex::new(None),
        });
        let transport = Arc::new(FixtureOAuthTransport {
            called: AtomicUsize::new(0),
        });
        let broker = ReqwestMcpOAuthCredentialBroker::with_transport(
            Arc::new(FixtureSecrets {
                calls: AtomicUsize::new(0),
            }),
            Arc::new(FixtureDns {
                address: Ipv4Addr::new(10, 0, 0, 8),
                calls: AtomicUsize::new(0),
            }),
            verification_catalog(&contract),
            verifier.clone(),
            store.clone(),
            transport.clone(),
            McpOAuthEgressLimits::default(),
        )
        .unwrap();
        let parsed = parse_mcp_oauth_callback_query(b"state=s&code=one-time-code").unwrap();
        let McpOAuthCallbackWireOutcome::AuthorizationCode(code) = parsed.outcome else {
            panic!("fixture must contain a code");
        };
        let failure = broker
            .exchange_authorization_code(&contract, code, now)
            .await
            .unwrap_err();
        assert_eq!(failure, McpOAuthCredentialBrokerError::Rejected);
        assert_eq!(transport.called.load(Ordering::SeqCst), 0);
        assert!(!verifier.called.load(Ordering::SeqCst));
        assert_eq!(store.load_calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.store_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn oauth_capacity_rejects_before_dns_or_code_dispatch() {
        let now = Utc::now();
        let contract = contract(now);
        let verifier = Arc::new(FixtureVerifier {
            called: AtomicBool::new(false),
        });
        let store = Arc::new(FixtureStore {
            load_calls: AtomicUsize::new(0),
            store_calls: AtomicUsize::new(0),
            prepared: Mutex::new(None),
        });
        let transport = Arc::new(FixtureOAuthTransport {
            called: AtomicUsize::new(0),
        });
        let broker = ReqwestMcpOAuthCredentialBroker::with_transport(
            Arc::new(FixtureSecrets {
                calls: AtomicUsize::new(0),
            }),
            Arc::new(FixtureDns {
                address: Ipv4Addr::new(8, 8, 8, 8),
                calls: AtomicUsize::new(0),
            }),
            verification_catalog(&contract),
            verifier.clone(),
            store.clone(),
            transport.clone(),
            McpOAuthEgressLimits {
                maximum_in_flight: 1,
                ..McpOAuthEgressLimits::default()
            },
        )
        .unwrap();
        let _occupied = broker.permits.clone().try_acquire_owned().unwrap();
        let parsed = parse_mcp_oauth_callback_query(b"state=s&code=one-time-code").unwrap();
        let McpOAuthCallbackWireOutcome::AuthorizationCode(code) = parsed.outcome else {
            panic!("fixture must contain a code");
        };
        let failure = broker
            .exchange_authorization_code(&contract, code, now)
            .await
            .unwrap_err();
        assert_eq!(
            failure,
            McpOAuthCredentialBrokerError::TemporarilyUnavailable
        );
        assert_eq!(transport.called.load(Ordering::SeqCst), 0);
        assert!(!verifier.called.load(Ordering::SeqCst));
        assert_eq!(store.load_calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.store_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn pkce_cleaner_forwards_only_the_authorized_exact_version() {
        let deleter = Arc::new(FixtureExactSecretVersionDeleter {
            calls: AtomicUsize::new(0),
            result: Ok(ExactSecretVersionDeleteDisposition::AlreadyAbsent),
        });
        let cleaner = BrokeredMcpOAuthPkceSecretCleaner::new(deleter.clone());
        let authorization = AuthorizedMcpOAuthPkceCleanup {
            tenant_id: id(ResourceKind::Tenant, 40),
            task_id: id(ResourceKind::Interaction, 41),
            secret_binding: secret_binding(42, MCP_OAUTH_PKCE_SECRET_PURPOSE.parse().unwrap(), 'f'),
        };

        assert_eq!(
            cleaner.delete_exact(&authorization).await.unwrap(),
            McpOAuthPkceSecretCleanupDisposition::AlreadyAbsent
        );
        assert_eq!(deleter.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pkce_cleaner_rejects_a_non_pkce_binding_before_secret_manager_dispatch() {
        let deleter = Arc::new(FixtureExactSecretVersionDeleter {
            calls: AtomicUsize::new(0),
            result: Ok(ExactSecretVersionDeleteDisposition::Deleted),
        });
        let cleaner = BrokeredMcpOAuthPkceSecretCleaner::new(deleter.clone());
        let authorization = AuthorizedMcpOAuthPkceCleanup {
            tenant_id: id(ResourceKind::Tenant, 43),
            task_id: id(ResourceKind::Interaction, 44),
            secret_binding: secret_binding(45, "mcp.oauth.client".parse().unwrap(), 'e'),
        };

        assert_eq!(
            cleaner.delete_exact(&authorization).await.unwrap_err(),
            McpOAuthPkceSecretCleanupError::Rejected
        );
        assert_eq!(deleter.calls.load(Ordering::SeqCst), 0);
    }
}
