use super::{digest, CompleteMcpOAuthCallback, McpHostError, McpOAuthAuthorizedGrant};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    exact_secret_binding_purposes_match, CanonicalHttpEndpoint, CapabilityEndpointScheme,
    McpAuthPolicyDocument, McpDeploymentClosure, McpOAuthClientAuthenticationKind,
    McpOAuthTaskBinding, McpServerExecutionContract, McpTransportKind, ResourceId, ResourceKind,
    Sha256Digest,
};

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, error::Error, fmt};
use url::Url;

pub const MAX_MCP_OAUTH_CALLBACK_QUERY_BYTES: usize = 8_192;
pub const MAX_MCP_OAUTH_STATE_BYTES: usize = 4_096;
pub const MAX_MCP_OAUTH_CODE_BYTES: usize = 8_192;
pub const MAX_MCP_OAUTH_ISSUER_BYTES: usize = 2_048;
pub const MAX_MCP_OAUTH_ERROR_BYTES: usize = 128;
pub const MAX_MCP_OAUTH_RECEIPT_TTL_SECONDS: i64 = 86_400;

/// Sensitive callback value that is redacted in diagnostics and zeroed on drop.
pub struct SensitiveOAuthValue(Vec<u8>);

impl SensitiveOAuthValue {
    pub fn from_decoded(
        mut value: Vec<u8>,
        maximum_bytes: usize,
    ) -> Result<Self, McpOAuthCallbackError> {
        if value.is_empty()
            || value.len() > maximum_bytes
            || !value.iter().all(u8::is_ascii_graphic)
        {
            value.fill(0);
            return Err(McpOAuthCallbackError::Rejected(
                "mcp_oauth_callback_value_invalid",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SensitiveOAuthValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveOAuthValue")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for SensitiveOAuthValue {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

pub enum McpOAuthCallbackWireOutcome {
    AuthorizationCode(SensitiveOAuthValue),
    Declined { safe_reason_code: &'static str },
}

pub struct ParsedMcpOAuthCallback {
    pub state: SensitiveOAuthValue,
    pub issuer: Option<SensitiveOAuthValue>,
    pub outcome: McpOAuthCallbackWireOutcome,
}

impl fmt::Debug for ParsedMcpOAuthCallback {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParsedMcpOAuthCallback")
            .field("state", &self.state)
            .field("issuer", &self.issuer)
            .field(
                "outcome",
                &match self.outcome {
                    McpOAuthCallbackWireOutcome::AuthorizationCode(_) => "authorization_code",
                    McpOAuthCallbackWireOutcome::Declined { .. } => "declined",
                },
            )
            .finish()
    }
}

/// Strict `application/x-www-form-urlencoded` callback query parser.
///
/// Duplicate and unknown fields fail closed. `error_description`, `error_uri`, tokens and arbitrary
/// extension fields are deliberately not accepted at this boundary.
pub fn parse_mcp_oauth_callback_query(
    raw_query: &[u8],
) -> Result<ParsedMcpOAuthCallback, McpOAuthCallbackError> {
    if raw_query.is_empty() || raw_query.len() > MAX_MCP_OAUTH_CALLBACK_QUERY_BYTES {
        return Err(McpOAuthCallbackError::Rejected(
            "mcp_oauth_callback_query_invalid",
        ));
    }
    let mut fields = BTreeMap::<String, Vec<u8>>::new();
    for pair in raw_query.split(|byte| *byte == b'&') {
        let Some(separator) = pair.iter().position(|byte| *byte == b'=') else {
            return Err(McpOAuthCallbackError::Rejected(
                "mcp_oauth_callback_query_invalid",
            ));
        };
        let key = strict_form_decode(&pair[..separator])?;
        let value = strict_form_decode(&pair[separator + 1..])?;
        let key = String::from_utf8(key)
            .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_callback_query_invalid"))?;
        if !matches!(key.as_str(), "code" | "error" | "iss" | "state")
            || fields.insert(key, value).is_some()
        {
            return Err(McpOAuthCallbackError::Rejected(
                "mcp_oauth_callback_query_invalid",
            ));
        }
    }

    let state = fields
        .remove("state")
        .ok_or(McpOAuthCallbackError::Rejected(
            "mcp_oauth_callback_state_missing",
        ))
        .and_then(|value| SensitiveOAuthValue::from_decoded(value, MAX_MCP_OAUTH_STATE_BYTES))?;
    let issuer = fields
        .remove("iss")
        .map(|value| SensitiveOAuthValue::from_decoded(value, MAX_MCP_OAUTH_ISSUER_BYTES))
        .transpose()?;
    let code = fields.remove("code");
    let error = fields.remove("error");
    let outcome = match (code, error) {
        (Some(code), None) => McpOAuthCallbackWireOutcome::AuthorizationCode(
            SensitiveOAuthValue::from_decoded(code, MAX_MCP_OAUTH_CODE_BYTES)?,
        ),
        (None, Some(error)) => {
            let error = SensitiveOAuthValue::from_decoded(error, MAX_MCP_OAUTH_ERROR_BYTES)?;
            McpOAuthCallbackWireOutcome::Declined {
                safe_reason_code: safe_oauth_error_code(error.as_bytes()),
            }
        }
        _ => {
            return Err(McpOAuthCallbackError::Rejected(
                "mcp_oauth_callback_outcome_invalid",
            ));
        }
    };
    Ok(ParsedMcpOAuthCallback {
        state,
        issuer,
        outcome,
    })
}

fn strict_form_decode(value: &[u8]) -> Result<Vec<u8>, McpOAuthCallbackError> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        match value[index] {
            b'%' => {
                if index + 2 >= value.len() {
                    return Err(McpOAuthCallbackError::Rejected(
                        "mcp_oauth_callback_query_invalid",
                    ));
                }
                let high = hex(value[index + 1]).ok_or(McpOAuthCallbackError::Rejected(
                    "mcp_oauth_callback_query_invalid",
                ))?;
                let low = hex(value[index + 2]).ok_or(McpOAuthCallbackError::Rejected(
                    "mcp_oauth_callback_query_invalid",
                ))?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            byte if byte.is_ascii() => {
                decoded.push(byte);
                index += 1;
            }
            _ => {
                return Err(McpOAuthCallbackError::Rejected(
                    "mcp_oauth_callback_query_invalid",
                ));
            }
        }
    }
    Ok(decoded)
}

fn hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn safe_oauth_error_code(value: &[u8]) -> &'static str {
    match value {
        b"access_denied" => "mcp_oauth_access_denied",
        b"interaction_required" | b"login_required" | b"consent_required" => {
            "mcp_oauth_interaction_incomplete"
        }
        _ => "mcp_oauth_authorization_failed",
    }
}

pub fn sensitive_digest(
    domain: &str,
    value: &SensitiveOAuthValue,
) -> Result<Sha256Digest, McpOAuthCallbackError> {
    #[derive(Serialize)]
    struct DigestInput<'a> {
        domain: &'a str,
        value: &'a [u8],
    }
    digest(&DigestInput {
        domain,
        value: value.as_bytes(),
    })
    .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_callback_digest_failed"))
}

pub fn mcp_oauth_state_digest(
    state: &SensitiveOAuthValue,
) -> Result<Sha256Digest, McpOAuthCallbackError> {
    sensitive_digest("mcp_oauth_state_v1", state)
}

pub fn mcp_oauth_issuer_identity_digest(
    issuer: &SensitiveOAuthValue,
) -> Result<Sha256Digest, McpOAuthCallbackError> {
    let value = std::str::from_utf8(issuer.as_bytes())
        .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_callback_issuer_invalid"))?;
    let parsed = Url::parse(value)
        .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_callback_issuer_invalid"))?;
    if parsed.scheme() != "https"
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(McpOAuthCallbackError::Rejected(
            "mcp_oauth_callback_issuer_invalid",
        ));
    }
    let host = parsed.host_str().ok_or(McpOAuthCallbackError::Rejected(
        "mcp_oauth_callback_issuer_invalid",
    ))?;
    let endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: host.to_owned(),
        port: parsed
            .port_or_known_default()
            .ok_or(McpOAuthCallbackError::Rejected(
                "mcp_oauth_callback_issuer_invalid",
            ))?,
        base_path: parsed.path().to_owned(),
    };
    endpoint
        .canonical_digest()
        .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_callback_issuer_invalid"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedMcpOAuthState {
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
}

impl AuthenticatedMcpOAuthState {
    pub fn validate(&self) -> Result<(), McpOAuthCallbackError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.task_id.kind() != ResourceKind::Interaction
        {
            return Err(McpOAuthCallbackError::Rejected(
                "mcp_oauth_callback_state_invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthCallbackCommitIds {
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}

impl McpOAuthCallbackCommitIds {
    pub fn validate(&self) -> Result<(), McpOAuthCallbackError> {
        if self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
        {
            return Err(McpOAuthCallbackError::Rejected(
                "mcp_oauth_callback_identity_invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthExchangeContract {
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub task_generation: u64,
    pub task_version: u64,
    pub task_deadline: DateTime<Utc>,
    pub binding: McpOAuthTaskBinding,
    pub deployment_closure: McpDeploymentClosure,
    pub server: McpServerExecutionContract,
    pub auth_profile: McpAuthPolicyDocument,
}

impl McpOAuthExchangeContract {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpOAuthCallbackError> {
        self.binding
            .validate()
            .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_contract_invalid"))?;
        self.deployment_closure
            .validate()
            .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_contract_invalid"))?;
        self.server
            .validate()
            .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_contract_invalid"))?;
        self.auth_profile
            .validate()
            .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_contract_invalid"))?;
        let profile_digest = self
            .auth_profile
            .canonical_digest()
            .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_contract_invalid"))?;
        let client_binding_is_exact = self
            .auth_profile
            .client_credential_purpose
            .as_ref()
            .is_none_or(|purpose| {
                self.deployment_closure
                    .secret_bindings
                    .iter()
                    .filter(|binding| &binding.purpose == purpose)
                    .count()
                    == 1
            });
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.task_id.kind() != ResourceKind::Interaction
            || self.task_generation == 0
            || self.task_version == 0
            || self.task_deadline <= now
            || self.server.transport != McpTransportKind::StreamableHttp
            || self.deployment_closure.transport.kind() != McpTransportKind::StreamableHttp
            || self.deployment_closure.server_revision != self.server.revision
            || self.deployment_closure.protocol_policy != self.server.protocol_policy
            || self.deployment_closure.auth_policy.as_ref() != Some(&self.binding.auth_policy)
            || self.binding.auth_policy.semantic_digest != profile_digest
            || self.binding.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.binding.audience_identity_digest
                != self.deployment_closure.server_identity_digest
            || self
                .auth_profile
                .resource_indicator
                .endpoint_identity_digest
                != self.binding.audience_identity_digest
            || self.auth_profile.redirect_uri.endpoint_identity_digest
                != self.binding.callback_binding_digest
            || !self
                .auth_profile
                .permits_scopes(&self.binding.requested_scopes)
            || self.server.authorization_credential_purpose.as_ref()
                != Some(&self.binding.token_credential_purpose)
            || self.binding.pkce_secret_binding.provider_id
                != self.auth_profile.pkce_secret_provider_id
            || self.auth_profile.client_credential_purpose.as_ref()
                == Some(&self.binding.token_credential_purpose)
            || !exact_secret_binding_purposes_match(
                &self.deployment_closure.secret_bindings,
                &self.server.deployment_credential_requirements,
            )
            || !client_binding_is_exact
            || (self.auth_profile.client_authentication == McpOAuthClientAuthenticationKind::None
                && self.auth_profile.client_credential_purpose.is_some())
        {
            return Err(McpOAuthCallbackError::Rejected(
                "mcp_oauth_contract_invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthCallbackCommitDisposition {
    Authorized,
    Declined,
    RejectedStale,
    Replayed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpOAuthCallbackCommitOutcome {
    pub disposition: McpOAuthCallbackCommitDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthCallbackAuthorityError {
    NotFoundOrChanged,
    Unavailable,
    CommitUncertain,
}

#[async_trait]
pub trait McpOAuthStateAuthenticator: Send + Sync {
    async fn authenticate(
        &self,
        state: &SensitiveOAuthValue,
        now: DateTime<Utc>,
    ) -> Result<AuthenticatedMcpOAuthState, McpOAuthCallbackAuthorityError>;
}

pub trait McpOAuthCallbackIdentityFactory: Send + Sync {
    fn next_ids(
        &self,
        tenant_id: &ResourceId,
        task_id: &ResourceId,
        request_digest: &Sha256Digest,
    ) -> Result<McpOAuthCallbackCommitIds, McpOAuthCallbackAuthorityError>;
}

#[async_trait]
pub trait McpOAuthCallbackAuthority: Send + Sync {
    async fn resolve_exchange_contract(
        &self,
        identity: &AuthenticatedMcpOAuthState,
    ) -> Result<McpOAuthExchangeContract, McpOAuthCallbackAuthorityError>;

    async fn commit_callback(
        &self,
        command: CompleteMcpOAuthCallback,
    ) -> Result<McpOAuthCallbackCommitOutcome, McpOAuthCallbackAuthorityError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthCredentialBrokerError {
    Rejected,
    TemporarilyUnavailable,
    ExchangeUncertain,
}

#[async_trait]
pub trait McpOAuthCredentialBroker: Send + Sync {
    async fn exchange_authorization_code(
        &self,
        contract: &McpOAuthExchangeContract,
        authorization_code: SensitiveOAuthValue,
        now: DateTime<Utc>,
    ) -> Result<McpOAuthAuthorizedGrant, McpOAuthCredentialBrokerError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthCallbackIngressConfig {
    pub callback_ingress_generation_id: ResourceId,
    pub callback_binding_digest: Sha256Digest,
    pub receipt_ttl_seconds: i64,
}

impl McpOAuthCallbackIngressConfig {
    pub fn validate(&self) -> Result<(), McpOAuthCallbackError> {
        if self.callback_ingress_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.receipt_ttl_seconds <= 0
            || self.receipt_ttl_seconds > MAX_MCP_OAUTH_RECEIPT_TTL_SECONDS
        {
            return Err(McpOAuthCallbackError::Rejected(
                "mcp_oauth_callback_configuration_invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpOAuthCallbackIngressOutcome {
    pub disposition: McpOAuthCallbackCommitDisposition,
    pub no_store: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthCallbackError {
    Rejected(&'static str),
    TemporarilyUnavailable(&'static str),
    CommitUncertain(&'static str),
}

impl McpOAuthCallbackError {
    pub const fn safe_code(self) -> &'static str {
        match self {
            Self::Rejected(code)
            | Self::TemporarilyUnavailable(code)
            | Self::CommitUncertain(code) => code,
        }
    }
}

impl fmt::Display for McpOAuthCallbackError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Rejected(_) => "MCP OAuth callback was rejected",
            Self::TemporarilyUnavailable(_) => "MCP OAuth callback dependency is unavailable",
            Self::CommitUncertain(_) => "MCP OAuth callback outcome is uncertain",
        })
    }
}

impl Error for McpOAuthCallbackError {}

impl From<McpHostError> for McpOAuthCallbackError {
    fn from(_: McpHostError) -> Self {
        Self::Rejected("mcp_oauth_callback_contract_invalid")
    }
}
