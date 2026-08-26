use super::{
    digest, mcp_oauth_state_digest, valid_code, BeginMcpOAuthAuthorization,
    McpOAuthReauthorizationFence, SensitiveOAuthValue, MAX_MCP_OAUTH_STATE_LIFETIME_SECONDS,
    MCP_OAUTH_PKCE_SECRET_PURPOSE,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    CommandAudit, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef,
    McpAuthPolicyDocument, McpOAuthEndpoint, PrincipalKind, ResourceId, ResourceKind,
    SecretPurpose, SecretResolutionPolicy, Sha256Digest,
};
use serde::Serialize;
use std::{error::Error, fmt, sync::Arc};
use url::Url;

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
    fn new(value: String) -> Result<Self, McpOAuthAuthorizationStartError> {
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

pub struct McpOAuthAuthorizationStartService {
    config: McpOAuthAuthorizationStartConfig,
    authority: Arc<dyn McpOAuthAuthorizationStartAuthority>,
    preparation: Arc<dyn McpOAuthAuthorizationPreparationBroker>,
}

impl McpOAuthAuthorizationStartService {
    pub fn new(
        config: McpOAuthAuthorizationStartConfig,
        authority: Arc<dyn McpOAuthAuthorizationStartAuthority>,
        preparation: Arc<dyn McpOAuthAuthorizationPreparationBroker>,
    ) -> Self {
        Self {
            config,
            authority,
            preparation,
        }
    }

    pub async fn start(
        &self,
        intent: McpOAuthAuthorizationStartIntent,
        now: DateTime<Utc>,
    ) -> Result<McpOAuthAuthorizationStartOutcome, McpOAuthAuthorizationStartError> {
        intent.validate_at(now)?;
        let resolved = self
            .authority
            .resolve_authorization_start(&intent, &self.config.callback_binding_digest)
            .await
            .map_err(map_authority_error)?;
        resolved.validate_for(&intent, &self.config.callback_binding_digest)?;
        let preparation_request = McpOAuthAuthorizationPreparationRequest {
            schema_version: 1,
            tenant_id: intent.audit.tenant_id.clone(),
            task_id: intent.task_id.clone(),
            authorization_binding_id: intent.authorization_binding_id.clone(),
            mcp_deployment: intent.mcp_deployment.clone(),
            pkce_secret_provider_id: resolved.auth_profile.pkce_secret_provider_id.clone(),
            preparation_digest: preparation_digest(
                &intent,
                &resolved,
                &self.config.callback_binding_digest,
            )?,
            callback_binding_digest: self.config.callback_binding_digest.clone(),
            expires_at: intent.deadline,
        };
        preparation_request.validate_at(now)?;
        let prepared = self
            .preparation
            .prepare_or_load(&preparation_request, now)
            .await
            .map_err(map_preparation_error)?;
        prepared.validate_for(&preparation_request)?;
        let state_digest = mcp_oauth_state_digest(&prepared.state)
            .map_err(|_| rejected("mcp_oauth_start_state_invalid"))?;
        let nonce_digest = mcp_oauth_nonce_digest(&prepared.nonce)?;
        let authorization_url =
            build_authorization_url(&resolved.auth_profile, &intent.requested_scopes, &prepared)?;
        let command = BeginMcpOAuthAuthorization {
            audit: intent.audit,
            task_id: intent.task_id.clone(),
            authorization_binding_id: intent.authorization_binding_id,
            mcp_deployment: intent.mcp_deployment,
            expected_principal_binding_generation: intent.expected_principal_binding_generation,
            requested_scopes: intent.requested_scopes,
            state_digest,
            nonce_digest,
            callback_binding_digest: self.config.callback_binding_digest.clone(),
            pkce_secret_binding: prepared.pkce_secret_binding,
            reauthorization: intent.reauthorization,
            safe_prompt_key: intent.safe_prompt_key,
            deadline: intent.deadline,
        };
        let outcome = self
            .authority
            .commit_authorization_start(command)
            .await
            .map_err(map_authority_error)?;
        Ok(McpOAuthAuthorizationStartOutcome {
            disposition: outcome.disposition,
            authorization_url,
            task_id: intent.task_id,
            deadline: intent.deadline,
        })
    }
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

fn preparation_digest(
    intent: &McpOAuthAuthorizationStartIntent,
    resolved: &ResolvedMcpOAuthAuthorizationStart,
    callback_binding_digest: &Sha256Digest,
) -> Result<Sha256Digest, McpOAuthAuthorizationStartError> {
    #[derive(Serialize)]
    struct PreparationDigestInput<'a> {
        schema_version: u32,
        tenant_id: &'a ResourceId,
        principal_id: &'a ResourceId,
        principal_kind: PrincipalKind,
        task_id: &'a ResourceId,
        authorization_binding_id: &'a ResourceId,
        mcp_deployment: &'a ExactDeploymentRef,
        expected_principal_binding_generation: u64,
        requested_scopes: &'a [String],
        auth_policy: &'a ExactVersionRef,
        audience_identity_digest: &'a Sha256Digest,
        token_credential_purpose: &'a SecretPurpose,
        pkce_secret_provider_id: &'a ResourceId,
        token_secret_provider_id: &'a ResourceId,
        callback_binding_digest: &'a Sha256Digest,
        idempotency_key_digest: &'a Sha256Digest,
        request_digest: &'a Sha256Digest,
        deadline: DateTime<Utc>,
    }
    digest(&PreparationDigestInput {
        schema_version: 1,
        tenant_id: &intent.audit.tenant_id,
        principal_id: &intent.audit.principal_id,
        principal_kind: intent.audit.principal_kind,
        task_id: &intent.task_id,
        authorization_binding_id: &intent.authorization_binding_id,
        mcp_deployment: &intent.mcp_deployment,
        expected_principal_binding_generation: intent.expected_principal_binding_generation,
        requested_scopes: &intent.requested_scopes,
        auth_policy: &resolved.auth_policy,
        audience_identity_digest: &resolved.audience_identity_digest,
        token_credential_purpose: &resolved.token_credential_purpose,
        pkce_secret_provider_id: &resolved.auth_profile.pkce_secret_provider_id,
        token_secret_provider_id: &resolved.auth_profile.token_secret_provider_id,
        callback_binding_digest,
        idempotency_key_digest: &intent.audit.idempotency_key_digest,
        request_digest: &intent.audit.request_digest,
        deadline: intent.deadline,
    })
    .map_err(|_| rejected("mcp_oauth_start_digest_failed"))
}

fn build_authorization_url(
    profile: &McpAuthPolicyDocument,
    requested_scopes: &[String],
    prepared: &PreparedMcpOAuthAuthorization,
) -> Result<SensitiveMcpOAuthAuthorizationUrl, McpOAuthAuthorizationStartError> {
    let mut authorization = endpoint_url(&profile.authorization_endpoint)?;
    let redirect = endpoint_url(&profile.redirect_uri)?.to_string();
    let resource = endpoint_url(&profile.resource_indicator)?.to_string();
    let state = std::str::from_utf8(prepared.state.as_bytes())
        .map_err(|_| rejected("mcp_oauth_start_state_invalid"))?;
    let nonce = std::str::from_utf8(prepared.nonce.as_bytes())
        .map_err(|_| rejected("mcp_oauth_start_nonce_invalid"))?;
    let scope = requested_scopes.join(" ");
    authorization
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &profile.client_id)
        .append_pair("redirect_uri", &redirect)
        .append_pair("scope", &scope)
        .append_pair("state", state)
        .append_pair("nonce", nonce)
        .append_pair("code_challenge", &prepared.pkce_challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("resource", &resource);
    SensitiveMcpOAuthAuthorizationUrl::new(authorization.to_string())
}

fn endpoint_url(endpoint: &McpOAuthEndpoint) -> Result<Url, McpOAuthAuthorizationStartError> {
    endpoint
        .validate()
        .map_err(|_| rejected("mcp_oauth_start_endpoint_invalid"))?;
    let raw = format!(
        "https://{}:{}{}",
        endpoint.endpoint.host, endpoint.endpoint.port, endpoint.endpoint.base_path
    );
    let url = Url::parse(&raw).map_err(|_| rejected("mcp_oauth_start_endpoint_invalid"))?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default() != Some(endpoint.endpoint.port)
    {
        return Err(rejected("mcp_oauth_start_endpoint_invalid"));
    }
    Ok(url)
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

fn map_authority_error(
    error: McpOAuthAuthorizationStartAuthorityError,
) -> McpOAuthAuthorizationStartError {
    match error {
        McpOAuthAuthorizationStartAuthorityError::NotFoundOrChanged => {
            rejected("mcp_oauth_start_not_found")
        }
        McpOAuthAuthorizationStartAuthorityError::Unavailable => {
            McpOAuthAuthorizationStartError::TemporarilyUnavailable(
                "mcp_oauth_start_authority_unavailable",
            )
        }
        McpOAuthAuthorizationStartAuthorityError::CommitUncertain => {
            McpOAuthAuthorizationStartError::CommitUncertain("mcp_oauth_start_commit_uncertain")
        }
    }
}

fn map_preparation_error(
    error: McpOAuthAuthorizationPreparationError,
) -> McpOAuthAuthorizationStartError {
    match error {
        McpOAuthAuthorizationPreparationError::Rejected => {
            rejected("mcp_oauth_start_preparation_rejected")
        }
        McpOAuthAuthorizationPreparationError::TemporarilyUnavailable => {
            McpOAuthAuthorizationStartError::TemporarilyUnavailable(
                "mcp_oauth_start_preparation_unavailable",
            )
        }
        McpOAuthAuthorizationPreparationError::WriteUncertain => {
            McpOAuthAuthorizationStartError::CommitUncertain(
                "mcp_oauth_start_preparation_uncertain",
            )
        }
    }
}

const fn rejected(code: &'static str) -> McpOAuthAuthorizationStartError {
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

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{CapabilityEndpointScheme, McpOAuthClientAuthenticationKind};
    use std::sync::Mutex;

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn sha(character: char) -> Sha256Digest {
        let hexadecimal = char::from_digit((character as u32) % 16, 16).unwrap();
        format!("sha256:{}", hexadecimal.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn endpoint(host: &str, path: &str) -> McpOAuthEndpoint {
        let endpoint = insight_platform_contracts::CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: host.to_owned(),
            port: 443,
            base_path: path.to_owned(),
        };
        let actual = endpoint.canonical_digest().unwrap();
        McpOAuthEndpoint {
            endpoint,
            endpoint_identity_digest: actual,
        }
    }

    fn exact_policy(profile: &McpAuthPolicyDocument) -> ExactVersionRef {
        ExactVersionRef::new(
            id("prev_0198f1c3-8f49-7c3e-b1f3-773c28367ba0"),
            profile.canonical_digest().unwrap(),
        )
        .unwrap()
    }

    fn deployment() -> ExactDeploymentRef {
        ExactDeploymentRef::new(id("mcdep_0198f1c3-8f49-7c3e-b1f3-773c28367ba1"), sha('d')).unwrap()
    }

    fn profile() -> McpAuthPolicyDocument {
        McpAuthPolicyDocument {
            schema_version: 1,
            issuer: endpoint("issuer.example", "/"),
            authorization_endpoint: endpoint("issuer.example", "/authorize"),
            token_endpoint: endpoint("issuer.example", "/token"),
            client_id: "platform-client".to_owned(),
            client_authentication: McpOAuthClientAuthenticationKind::None,
            client_credential_purpose: None,
            pkce_secret_provider_id: id("spr_0198f1c3-8f49-7c3e-b1f3-773c28367b9e"),
            token_secret_provider_id: id("spr_0198f1c3-8f49-7c3e-b1f3-773c28367b9f"),
            redirect_uri: endpoint("platform.example", "/v1/mcp/oauth/callback"),
            resource_indicator: endpoint("mcp.example", "/mcp"),
            allowed_scopes: vec!["read".to_owned(), "write".to_owned()],
            maximum_token_response_bytes: 65_536,
            connect_timeout_milliseconds: 1_000,
            total_timeout_milliseconds: 5_000,
            maximum_clock_skew_seconds: 30,
        }
    }

    fn audit(now: DateTime<Utc>) -> CommandAudit {
        CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: id("ten_0198f1c3-8f49-7c3e-b1f3-773c28367ba2"),
            principal_id: id("prn_0198f1c3-8f49-7c3e-b1f3-773c28367ba3"),
            principal_kind: PrincipalKind::AgentRunner,
            receipt_id: id("rcp_0198f1c3-8f49-7c3e-b1f3-773c28367ba4"),
            event_id: id("evt_0198f1c3-8f49-7c3e-b1f3-773c28367ba5"),
            outbox_id: id("obx_0198f1c3-8f49-7c3e-b1f3-773c28367ba6"),
            idempotency_key_digest: sha('i'),
            request_digest: sha('r'),
            receipt_expires_at: now + Duration::minutes(20),
        }
    }

    fn intent(now: DateTime<Utc>) -> McpOAuthAuthorizationStartIntent {
        McpOAuthAuthorizationStartIntent {
            audit: audit(now),
            task_id: id("int_0198f1c3-8f49-7c3e-b1f3-773c28367ba7"),
            authorization_binding_id: id("mab_0198f1c3-8f49-7c3e-b1f3-773c28367ba8"),
            mcp_deployment: deployment(),
            expected_principal_binding_generation: 7,
            requested_scopes: vec!["read".to_owned(), "write".to_owned()],
            reauthorization: None,
            safe_prompt_key: "mcp_oauth_authorize".to_owned(),
            deadline: now + Duration::minutes(10),
        }
    }

    fn pkce_binding() -> ExactSecretBindingRef {
        ExactSecretBindingRef::build(
            id("sbd_0198f1c3-8f49-7c3e-b1f3-773c28367ba9"),
            3,
            profile().pkce_secret_provider_id,
            MCP_OAUTH_PKCE_SECRET_PURPOSE.parse().unwrap(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: sha('v'),
            },
        )
        .unwrap()
    }

    struct Authority {
        resolved: ResolvedMcpOAuthAuthorizationStart,
        command: Mutex<Option<BeginMcpOAuthAuthorization>>,
    }

    #[async_trait]
    impl McpOAuthAuthorizationStartAuthority for Authority {
        async fn resolve_authorization_start(
            &self,
            _intent: &McpOAuthAuthorizationStartIntent,
            _callback_binding_digest: &Sha256Digest,
        ) -> Result<ResolvedMcpOAuthAuthorizationStart, McpOAuthAuthorizationStartAuthorityError>
        {
            Ok(self.resolved.clone())
        }

        async fn commit_authorization_start(
            &self,
            command: BeginMcpOAuthAuthorization,
        ) -> Result<McpOAuthAuthorizationStartCommitOutcome, McpOAuthAuthorizationStartAuthorityError>
        {
            *self.command.lock().unwrap() = Some(command);
            Ok(McpOAuthAuthorizationStartCommitOutcome {
                disposition: McpOAuthAuthorizationStartCommitDisposition::Applied,
            })
        }
    }

    struct Preparation;

    #[async_trait]
    impl McpOAuthAuthorizationPreparationBroker for Preparation {
        async fn prepare_or_load(
            &self,
            request: &McpOAuthAuthorizationPreparationRequest,
            _now: DateTime<Utc>,
        ) -> Result<PreparedMcpOAuthAuthorization, McpOAuthAuthorizationPreparationError> {
            Ok(PreparedMcpOAuthAuthorization {
                preparation_digest: request.preparation_digest.clone(),
                state: SensitiveOAuthValue::from_decoded(
                    b"sealed.state".to_vec(),
                    super::super::MAX_MCP_OAUTH_STATE_BYTES,
                )
                .unwrap(),
                nonce: SensitiveMcpOAuthNonce::new(b"n".repeat(43)).unwrap(),
                pkce_challenge: "a".repeat(43),
                pkce_secret_binding: pkce_binding(),
                storage_evidence_digest: sha('s'),
            })
        }
    }

    #[tokio::test]
    async fn start_builds_canonical_url_and_commits_only_digests_and_exact_secret() {
        let now = Utc::now();
        let profile = profile();
        let callback_digest = profile.redirect_uri.endpoint_identity_digest.clone();
        let authority = Arc::new(Authority {
            resolved: ResolvedMcpOAuthAuthorizationStart {
                tenant_id: audit(now).tenant_id,
                mcp_deployment: deployment(),
                audience_identity_digest: profile
                    .resource_indicator
                    .endpoint_identity_digest
                    .clone(),
                token_credential_purpose: "mcp.oauth.token".parse().unwrap(),
                auth_policy: exact_policy(&profile),
                auth_profile: profile,
            },
            command: Mutex::new(None),
        });
        let service = McpOAuthAuthorizationStartService::new(
            McpOAuthAuthorizationStartConfig {
                callback_binding_digest: callback_digest,
            },
            authority.clone(),
            Arc::new(Preparation),
        );

        let outcome = service.start(intent(now), now).await.unwrap();
        let url = Url::parse(outcome.authorization_url.as_str()).unwrap();
        let query = url.query_pairs().collect::<Vec<_>>();
        assert_eq!(url.path(), "/authorize");
        assert_eq!(query[0], ("response_type".into(), "code".into()));
        assert!(query
            .iter()
            .any(|pair| pair == &("state".into(), "sealed.state".into())));
        assert!(query
            .iter()
            .any(|pair| pair == &("nonce".into(), "n".repeat(43).into())));
        assert!(query
            .iter()
            .any(|pair| pair == &("code_challenge_method".into(), "S256".into())));
        let command = authority.command.lock().unwrap();
        let command = command.as_ref().unwrap();
        assert_eq!(command.pkce_secret_binding, pkce_binding());
        assert_ne!(command.state_digest, sha('r'));
        assert_ne!(command.nonce_digest, sha('r'));
        let debug = format!("{outcome:?}");
        assert!(!debug.contains("sealed.state"));
        assert!(!debug.contains(&"n".repeat(43)));
    }

    #[test]
    fn intent_rejects_unbounded_lifetime_and_service_principal() {
        let now = Utc::now();
        let mut invalid = intent(now);
        invalid.deadline = now + Duration::seconds(MAX_MCP_OAUTH_STATE_LIFETIME_SECONDS + 1);
        invalid.audit.receipt_expires_at = invalid.deadline;
        assert!(invalid.validate_at(now).is_err());
        invalid = intent(now);
        invalid.audit.principal_kind = PrincipalKind::ServiceIdentity;
        assert!(invalid.validate_at(now).is_err());
    }

    #[test]
    fn prepared_values_and_url_are_redacted() {
        let prepared = PreparedMcpOAuthAuthorization {
            preparation_digest: sha('a'),
            state: SensitiveOAuthValue::from_decoded(
                b"secret-state".to_vec(),
                super::super::MAX_MCP_OAUTH_STATE_BYTES,
            )
            .unwrap(),
            nonce: SensitiveMcpOAuthNonce::new(b"n".repeat(43)).unwrap(),
            pkce_challenge: "c".repeat(43),
            pkce_secret_binding: pkce_binding(),
            storage_evidence_digest: sha('b'),
        };
        let debug = format!("{prepared:?}");
        assert!(!debug.contains("secret-state"));
        assert!(!debug.contains(&"n".repeat(43)));
        assert!(!debug.contains(&"c".repeat(43)));
        let url = SensitiveMcpOAuthAuthorizationUrl::new(
            "https://issuer.example/authorize?state=secret-state".to_owned(),
        )
        .unwrap();
        assert!(!format!("{url:?}").contains("secret-state"));
    }
}
