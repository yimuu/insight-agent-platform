use super::{
    digest, valid_code, BeginMcpOAuthAuthorization, McpOAuthReauthorizationFence,
    SensitiveOAuthValue, MAX_MCP_OAUTH_STATE_LIFETIME_SECONDS, MCP_OAUTH_PKCE_SECRET_PURPOSE,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    CommandAudit, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef,
    McpAuthPolicyDocument, PrincipalKind, ResourceId, ResourceKind, SecretPurpose,
    SecretResolutionPolicy, Sha256Digest,
};
use serde::Serialize;
use std::{error::Error, fmt};

pub const MAX_MCP_OAUTH_AUTHORIZATION_URL_BYTES: usize = 32_768;
pub const MAX_MCP_OAUTH_NONCE_BYTES: usize = 256;
pub const MAX_MCP_OAUTH_PKCE_CHALLENGE_BYTES: usize = 128;

/// Stable, caller-owned intent. Random state, nonce and PKCE material are deliberately absent.
#[derive(Debug, Clone)]
pub struct McpOAuthAuthorizationStartIntent {
    pub audit: CommandAudit,
    pub task_id: ResourceId,
    pub authorization_binding_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub expected_principal_binding_generation: u64,
    pub requested_scopes: Vec<String>,
    pub reauthorization: Option<McpOAuthReauthorizationFence>,
    pub safe_prompt_key: String,
    pub deadline: DateTime<Utc>,
}

impl McpOAuthAuthorizationStartIntent {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpOAuthAuthorizationStartError> {
        self.audit
            .validate_at(now)
            .map_err(|_| rejected("mcp_oauth_start_intent_invalid"))?;
        if self.task_id.kind() != ResourceKind::Interaction
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.audit.principal_kind == PrincipalKind::ServiceIdentity
            || self.expected_principal_binding_generation == 0
            || self.requested_scopes.is_empty()
            || self.requested_scopes.len() > super::MAX_MCP_AUTHORIZATION_SCOPES
            || !self
                .requested_scopes
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self
                .requested_scopes
                .iter()
                .any(|scope| !valid_oauth_scope(scope))
            || self
                .reauthorization
                .as_ref()
                .is_some_and(|fence| fence.validate().is_err())
            || !valid_code(&self.safe_prompt_key)
            || self.deadline <= now
            || self.audit.receipt_expires_at < self.deadline
            || self.deadline - now > Duration::seconds(MAX_MCP_OAUTH_STATE_LIFETIME_SECONDS)
        {
            return Err(rejected("mcp_oauth_start_intent_invalid"));
        }
        Ok(())
    }
}

/// Exact, non-secret registry facts resolved before preparing transient OAuth material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMcpOAuthAuthorizationStart {
    pub tenant_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub audience_identity_digest: Sha256Digest,
    pub token_credential_purpose: SecretPurpose,
    pub auth_policy: ExactVersionRef,
    pub auth_profile: McpAuthPolicyDocument,
}

impl ResolvedMcpOAuthAuthorizationStart {
    pub fn validate_for(
        &self,
        intent: &McpOAuthAuthorizationStartIntent,
        callback_binding_digest: &Sha256Digest,
    ) -> Result<(), McpOAuthAuthorizationStartError> {
        self.auth_profile
            .validate()
            .map_err(|_| rejected("mcp_oauth_start_contract_invalid"))?;
        let profile_digest = self
            .auth_profile
            .canonical_digest()
            .map_err(|_| rejected("mcp_oauth_start_contract_invalid"))?;
        if self.tenant_id != intent.audit.tenant_id
            || self.mcp_deployment != intent.mcp_deployment
            || self.auth_policy.resource_kind != ResourceKind::PolicyRevision
            || self.auth_policy.validate().is_err()
            || self.auth_policy.semantic_digest != profile_digest
            || self
                .auth_profile
                .resource_indicator
                .endpoint_identity_digest
                != self.audience_identity_digest
            || self.auth_profile.redirect_uri.endpoint_identity_digest != *callback_binding_digest
            || !self.auth_profile.permits_scopes(&intent.requested_scopes)
            || self.token_credential_purpose.as_str() == MCP_OAUTH_PKCE_SECRET_PURPOSE
            || self.auth_profile.client_credential_purpose.as_ref()
                == Some(&self.token_credential_purpose)
        {
            return Err(rejected("mcp_oauth_start_contract_invalid"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthAuthorizationPreparationRequest {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub authorization_binding_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub pkce_secret_provider_id: ResourceId,
    pub preparation_digest: Sha256Digest,
    pub callback_binding_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
}

impl McpOAuthAuthorizationPreparationRequest {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpOAuthAuthorizationStartError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.task_id.kind() != ResourceKind::Interaction
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.pkce_secret_provider_id.kind() != ResourceKind::SecretProvider
            || self.expires_at <= now
            || self.expires_at - now > Duration::seconds(MAX_MCP_OAUTH_STATE_LIFETIME_SECONDS)
        {
            return Err(rejected("mcp_oauth_start_preparation_invalid"));
        }
        Ok(())
    }
}

/// Authorization nonce returned only to the Host application service. It is redacted and zeroed.
pub struct SensitiveMcpOAuthNonce(Vec<u8>);

impl SensitiveMcpOAuthNonce {
    pub fn new(mut value: Vec<u8>) -> Result<Self, McpOAuthAuthorizationStartError> {
        if !(43..=MAX_MCP_OAUTH_NONCE_BYTES).contains(&value.len())
            || !value.iter().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_' | b'~')
            })
        {
            value.fill(0);
            return Err(rejected("mcp_oauth_start_nonce_invalid"));
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SensitiveMcpOAuthNonce {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveMcpOAuthNonce")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for SensitiveMcpOAuthNonce {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

pub struct PreparedMcpOAuthAuthorization {
    pub preparation_digest: Sha256Digest,
    pub state: SensitiveOAuthValue,
    pub nonce: SensitiveMcpOAuthNonce,
    pub pkce_challenge: String,
    pub pkce_secret_binding: ExactSecretBindingRef,
    pub storage_evidence_digest: Sha256Digest,
}

impl fmt::Debug for PreparedMcpOAuthAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedMcpOAuthAuthorization")
            .field("preparation_digest", &self.preparation_digest)
            .field("state", &self.state)
            .field("nonce", &self.nonce)
            .field("pkce_challenge", &"[REDACTED]")
            .field("pkce_secret_binding", &self.pkce_secret_binding)
            .field("storage_evidence_digest", &self.storage_evidence_digest)
            .finish()
    }
}

impl PreparedMcpOAuthAuthorization {
    pub fn validate_for(
        &self,
        request: &McpOAuthAuthorizationPreparationRequest,
    ) -> Result<(), McpOAuthAuthorizationStartError> {
        if self.preparation_digest != request.preparation_digest
            || self.state.as_bytes().is_empty()
            || self.nonce.as_bytes().is_empty()
            || !valid_pkce_challenge(&self.pkce_challenge)
            || self.pkce_secret_binding.validate().is_err()
            || self.pkce_secret_binding.purpose.as_str() != MCP_OAUTH_PKCE_SECRET_PURPOSE
            || !matches!(
                &self.pkce_secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
        {
            return Err(rejected("mcp_oauth_start_preparation_invalid"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthAuthorizationPreparationError {
    Rejected,
    TemporarilyUnavailable,
    WriteUncertain,
}

#[async_trait]
pub trait McpOAuthAuthorizationPreparationBroker: Send + Sync {
    async fn prepare_or_load(
        &self,
        request: &McpOAuthAuthorizationPreparationRequest,
        now: DateTime<Utc>,
    ) -> Result<PreparedMcpOAuthAuthorization, McpOAuthAuthorizationPreparationError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthAuthorizationStartCommitDisposition {
    Applied,
    Replayed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpOAuthAuthorizationStartCommitOutcome {
    pub disposition: McpOAuthAuthorizationStartCommitDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthAuthorizationStartAuthorityError {
    NotFoundOrChanged,
    Unavailable,
    CommitUncertain,
}

#[async_trait]
pub trait McpOAuthAuthorizationStartAuthority: Send + Sync {
    async fn resolve_authorization_start(
        &self,
        intent: &McpOAuthAuthorizationStartIntent,
        callback_binding_digest: &Sha256Digest,
    ) -> Result<ResolvedMcpOAuthAuthorizationStart, McpOAuthAuthorizationStartAuthorityError>;

    async fn commit_authorization_start(
        &self,
        command: BeginMcpOAuthAuthorization,
    ) -> Result<McpOAuthAuthorizationStartCommitOutcome, McpOAuthAuthorizationStartAuthorityError>;
}

/// Redacted, zero-on-drop authorization URL. Callers may expose it only in the intended response.
pub struct SensitiveMcpOAuthAuthorizationUrl(Vec<u8>);

impl SensitiveMcpOAuthAuthorizationUrl {
    pub fn new(value: String) -> Result<Self, McpOAuthAuthorizationStartError> {
        if value.is_empty()
            || value.len() > MAX_MCP_OAUTH_AUTHORIZATION_URL_BYTES
            || !value.is_ascii()
            || value.chars().any(char::is_control)
        {
            return Err(rejected("mcp_oauth_start_authorization_url_invalid"));
        }
        Ok(Self(value.into_bytes()))
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("validated OAuth authorization URL is ASCII")
    }
}

impl fmt::Debug for SensitiveMcpOAuthAuthorizationUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveMcpOAuthAuthorizationUrl")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for SensitiveMcpOAuthAuthorizationUrl {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

#[derive(Debug)]
pub struct McpOAuthAuthorizationStartOutcome {
    pub disposition: McpOAuthAuthorizationStartCommitDisposition,
    pub authorization_url: SensitiveMcpOAuthAuthorizationUrl,
    pub task_id: ResourceId,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthAuthorizationStartConfig {
    pub callback_binding_digest: Sha256Digest,
}

pub fn mcp_oauth_nonce_digest(
    nonce: &SensitiveMcpOAuthNonce,
) -> Result<Sha256Digest, McpOAuthAuthorizationStartError> {
    #[derive(Serialize)]
    struct DigestInput<'a> {
        domain: &'a str,
        value: &'a [u8],
    }
    digest(&DigestInput {
        domain: "mcp_oauth_nonce_v1",
        value: nonce.as_bytes(),
    })
    .map_err(|_| rejected("mcp_oauth_start_digest_failed"))
}

fn valid_oauth_scope(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= super::MAX_MCP_SCOPE_BYTES
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_graphic() && !matches!(*byte, b'"' | b'\\'))
}

fn valid_pkce_challenge(value: &str) -> bool {
    value.len() == 43
        && value.len() <= MAX_MCP_OAUTH_PKCE_CHALLENGE_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

pub const fn rejected(code: &'static str) -> McpOAuthAuthorizationStartError {
    McpOAuthAuthorizationStartError::Rejected(code)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthAuthorizationStartError {
    Rejected(&'static str),
    TemporarilyUnavailable(&'static str),
    CommitUncertain(&'static str),
}

impl McpOAuthAuthorizationStartError {
    pub const fn safe_code(self) -> &'static str {
        match self {
            Self::Rejected(code)
            | Self::TemporarilyUnavailable(code)
            | Self::CommitUncertain(code) => code,
        }
    }
}

impl fmt::Display for McpOAuthAuthorizationStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Rejected(_) => "MCP OAuth authorization start was rejected",
            Self::TemporarilyUnavailable(_) => {
                "MCP OAuth authorization start dependency is unavailable"
            }
            Self::CommitUncertain(_) => "MCP OAuth authorization start outcome is uncertain",
        })
    }
}

impl Error for McpOAuthAuthorizationStartError {}
