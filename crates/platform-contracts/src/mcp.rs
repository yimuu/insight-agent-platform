use crate::{
    canonical_digest, validate_capability_credential_requirements, ArtifactRef,
    CanonicalHttpEndpoint, CapabilityEndpointScheme, ExactDeploymentRef, ExactSecretBindingRef,
    ExactVersionRef, McpOAuthClientAuthenticationKind, McpTransportKind, ResourceId, ResourceKind,
    SecretPurpose, SecretResolutionPolicy, Sha256Digest,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, error::Error, fmt};

pub const MCP_PROTOCOL_BASELINE: &str = "2025-11-25";
pub const MAX_MCP_PROTOCOL_VERSIONS: usize = 8;
pub const MAX_MCP_METHODS: usize = 32;
pub const MAX_MCP_MESSAGE_BYTES: u32 = 16 * 1_048_576;
pub const MAX_MCP_RESPONSE_BYTES: u32 = 64 * 1_048_576;
pub const MAX_MCP_SESSION_MILLISECONDS: u64 = 86_400_000;
pub const MAX_MCP_TIMEOUT_MILLISECONDS: u64 = 3_600_000;
pub const MAX_MCP_OAUTH_SCOPES: usize = 64;
pub const MAX_MCP_OAUTH_CLIENT_ID_BYTES: usize = 512;
pub const MAX_MCP_OAUTH_TOKEN_RESPONSE_BYTES: u32 = 1_048_576;
pub const MAX_MCP_OAUTH_TIMEOUT_MILLISECONDS: u64 = 120_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthEndpoint {
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
}

impl McpOAuthEndpoint {
    pub fn validate(&self) -> Result<(), McpContractError> {
        self.endpoint
            .validate()
            .map_err(|_| McpContractError::InvalidCredentials)?;
        if self.endpoint.scheme != CapabilityEndpointScheme::Https
            || self.endpoint.canonical_digest().as_ref() != Ok(&self.endpoint_identity_digest)
        {
            return Err(McpContractError::InvalidCredentials);
        }
        Ok(())
    }
}

/// Closed OAuth client and token-exchange profile owned by `PolicyKind::McpAuth`.
///
/// It contains no token, PKCE verifier, state, nonce or Secret Manager reference. Exact Secret
/// bindings remain in the Deployment or per-principal AuthorizationBinding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpAuthPolicyDocument {
    pub schema_version: u32,
    pub issuer: McpOAuthEndpoint,
    pub authorization_endpoint: McpOAuthEndpoint,
    pub token_endpoint: McpOAuthEndpoint,
    pub client_id: String,
    pub client_authentication: McpOAuthClientAuthenticationKind,
    pub client_credential_purpose: Option<SecretPurpose>,
    pub pkce_secret_provider_id: ResourceId,
    pub token_secret_provider_id: ResourceId,
    pub redirect_uri: McpOAuthEndpoint,
    pub resource_indicator: McpOAuthEndpoint,
    pub allowed_scopes: Vec<String>,
    pub maximum_token_response_bytes: u32,
    pub connect_timeout_milliseconds: u64,
    pub total_timeout_milliseconds: u64,
    pub maximum_clock_skew_seconds: u16,
}

impl McpAuthPolicyDocument {
    pub fn validate(&self) -> Result<(), McpContractError> {
        self.authorization_endpoint.validate()?;
        self.token_endpoint.validate()?;
        self.issuer.validate()?;
        self.redirect_uri.validate()?;
        self.resource_indicator.validate()?;
        let credential_shape_is_valid = matches!(
            (self.client_authentication, &self.client_credential_purpose),
            (McpOAuthClientAuthenticationKind::None, None)
                | (McpOAuthClientAuthenticationKind::ClientSecretBasic, Some(_))
        );
        if self.schema_version != 1
            || self.authorization_endpoint.endpoint_identity_digest
                == self.token_endpoint.endpoint_identity_digest
            || self.client_id.is_empty()
            || self.client_id.len() > MAX_MCP_OAUTH_CLIENT_ID_BYTES
            || self.client_id.chars().any(char::is_control)
            || !credential_shape_is_valid
            || self.pkce_secret_provider_id.kind() != ResourceKind::SecretProvider
            || self.token_secret_provider_id.kind() != ResourceKind::SecretProvider
            || self.allowed_scopes.is_empty()
            || self.allowed_scopes.len() > MAX_MCP_OAUTH_SCOPES
            || !self.allowed_scopes.windows(2).all(|pair| pair[0] < pair[1])
            || self
                .allowed_scopes
                .iter()
                .any(|scope| !valid_oauth_scope(scope))
            || self.maximum_token_response_bytes == 0
            || self.maximum_token_response_bytes > MAX_MCP_OAUTH_TOKEN_RESPONSE_BYTES
            || self.connect_timeout_milliseconds == 0
            || self.connect_timeout_milliseconds > self.total_timeout_milliseconds
            || self.total_timeout_milliseconds > MAX_MCP_OAUTH_TIMEOUT_MILLISECONDS
            || self.maximum_clock_skew_seconds > 300
        {
            return Err(McpContractError::InvalidCredentials);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, McpContractError> {
        self.validate()?;
        let value = serde_json::to_value(self).map_err(|_| McpContractError::Canonicalization)?;
        digest(&value)
    }

    pub fn permits_scopes(&self, scopes: &[String]) -> bool {
        !scopes.is_empty()
            && scopes.windows(2).all(|pair| pair[0] < pair[1])
            && scopes
                .iter()
                .all(|scope| self.allowed_scopes.binary_search(scope).is_ok())
    }
}

/// Immutable, non-secret OAuth interaction binding persisted inside the shared Task payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthTaskBinding {
    pub authorization_binding_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub auth_policy: ExactVersionRef,
    pub principal_binding_generation: u64,
    pub audience_identity_digest: Sha256Digest,
    pub requested_scopes: Vec<String>,
    pub token_credential_purpose: SecretPurpose,
    pub state_digest: Sha256Digest,
    pub nonce_digest: Sha256Digest,
    pub callback_binding_digest: Sha256Digest,
    pub pkce_secret_binding: Box<ExactSecretBindingRef>,
    pub expected_authorization_generation: Option<u64>,
    pub expected_authorization_version: Option<u64>,
}

impl McpOAuthTaskBinding {
    pub fn validate(&self) -> Result<(), McpContractError> {
        let reauthorization_fence_is_closed = matches!(
            (
                self.expected_authorization_generation,
                self.expected_authorization_version,
            ),
            (None, None) | (Some(1..), Some(1..))
        );
        if self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.auth_policy.resource_kind != ResourceKind::PolicyRevision
            || self.auth_policy.validate().is_err()
            || self.principal_binding_generation == 0
            || self.requested_scopes.is_empty()
            || self.requested_scopes.len() > MAX_MCP_OAUTH_SCOPES
            || !self
                .requested_scopes
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self
                .requested_scopes
                .iter()
                .any(|scope| !valid_oauth_scope(scope))
            || self.token_credential_purpose.as_str() == "mcp.oauth.pkce"
            || self.pkce_secret_binding.validate().is_err()
            || self.pkce_secret_binding.purpose.as_str() != "mcp.oauth.pkce"
            || !matches!(
                &self.pkce_secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
            || !reauthorization_fence_is_closed
        {
            return Err(McpContractError::InvalidCredentials);
        }
        Ok(())
    }
}

fn valid_oauth_scope(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 256
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_graphic() && !matches!(*byte, b'"' | b'\\'))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpExperimentalFeature {
    Tasks,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishedMcpMethod {
    ToolsCall,
    ResourcesList,
    ResourcesRead,
    PromptsGet,
    TasksGet,
    TasksResult,
    TasksCancel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpTransportFeatures {
    pub streamable_http_get: bool,
    pub streamable_http_sse: bool,
    pub resumable_stream: bool,
    pub session_affinity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpClientCapabilities {
    pub elicitation_form: bool,
    pub elicitation_url: bool,
    /// Experimental 2025-11-25 Task augmentation for server-to-client elicitation/create.
    pub tasks_elicitation_create: bool,
    pub sampling: bool,
    pub roots: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedMcpServerCapabilities {
    pub tools: bool,
    pub resources: bool,
    pub prompts: bool,
    pub logging: bool,
    pub tasks: bool,
    pub subscriptions: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpMethodLimits {
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
    pub maximum_metadata_entries: u16,
    pub maximum_progress_events: u32,
    pub maximum_pages: u16,
    pub minimum_poll_milliseconds: u64,
    pub maximum_poll_milliseconds: u64,
}

impl McpMethodLimits {
    fn validate(&self) -> Result<(), McpContractError> {
        if self.maximum_request_bytes == 0
            || self.maximum_request_bytes > MAX_MCP_MESSAGE_BYTES
            || self.maximum_response_bytes == 0
            || self.maximum_response_bytes > MAX_MCP_RESPONSE_BYTES
            || self.maximum_metadata_entries > 256
            || self.maximum_progress_events > 10_000
            || self.maximum_pages > 1_000
            || self.minimum_poll_milliseconds > self.maximum_poll_milliseconds
            || self.maximum_poll_milliseconds > MAX_MCP_TIMEOUT_MILLISECONDS
        {
            return Err(McpContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpMetadataPolicy {
    pub maximum_server_name_bytes: u16,
    pub maximum_server_version_bytes: u16,
    pub maximum_instruction_bytes: u32,
    pub maximum_object_name_bytes: u16,
    pub maximum_description_bytes: u32,
    pub maximum_icon_bytes: u32,
}

impl McpMetadataPolicy {
    fn validate(&self) -> Result<(), McpContractError> {
        if self.maximum_server_name_bytes == 0
            || self.maximum_server_name_bytes > 512
            || self.maximum_server_version_bytes == 0
            || self.maximum_server_version_bytes > 256
            || self.maximum_instruction_bytes > 65_536
            || self.maximum_object_name_bytes == 0
            || self.maximum_object_name_bytes > 512
            || self.maximum_description_bytes > 262_144
            || self.maximum_icon_bytes > 4 * 1_048_576
        {
            return Err(McpContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpProtocolPolicyDocument {
    pub schema_version: u32,
    pub offered_versions: Vec<String>,
    pub transport_features: McpTransportFeatures,
    pub client_capabilities: McpClientCapabilities,
    pub allowed_server_capabilities: AllowedMcpServerCapabilities,
    pub experimental_features: Vec<McpExperimentalFeature>,
    pub method_limits: BTreeMap<PublishedMcpMethod, McpMethodLimits>,
    pub metadata_policy: McpMetadataPolicy,
}

impl McpProtocolPolicyDocument {
    pub fn validate(&self) -> Result<(), McpContractError> {
        if self.schema_version != 1
            || self.offered_versions.is_empty()
            || self.offered_versions.len() > MAX_MCP_PROTOCOL_VERSIONS
            || !self
                .offered_versions
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self.offered_versions.iter().any(|value| !exact_date(value))
            || !self
                .experimental_features
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self.method_limits.is_empty()
            || self.method_limits.len() > MAX_MCP_METHODS
        {
            return Err(McpContractError::InvalidProtocolProfile);
        }
        self.metadata_policy.validate()?;
        for limits in self.method_limits.values() {
            limits.validate()?;
        }
        let tasks_enabled = self
            .experimental_features
            .contains(&McpExperimentalFeature::Tasks)
            && self.allowed_server_capabilities.tasks;
        if self.client_capabilities.tasks_elicitation_create
            && (!tasks_enabled
                || !(self.client_capabilities.elicitation_form
                    || self.client_capabilities.elicitation_url))
        {
            return Err(McpContractError::InvalidProtocolProfile);
        }
        if self.method_limits.keys().any(|method| match method {
            PublishedMcpMethod::ToolsCall => !self.allowed_server_capabilities.tools,
            PublishedMcpMethod::ResourcesList | PublishedMcpMethod::ResourcesRead => {
                !self.allowed_server_capabilities.resources
            }
            PublishedMcpMethod::PromptsGet => !self.allowed_server_capabilities.prompts,
            PublishedMcpMethod::TasksGet
            | PublishedMcpMethod::TasksResult
            | PublishedMcpMethod::TasksCancel => !tasks_enabled,
        }) {
            return Err(McpContractError::InvalidProtocolProfile);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, McpContractError> {
        self.validate()?;
        digest(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerLimits {
    pub maximum_message_bytes: u32,
    pub maximum_response_bytes: u32,
    pub maximum_headers: u16,
    pub maximum_sse_event_bytes: u32,
    pub maximum_in_flight: u16,
    pub maximum_connections: u16,
    pub maximum_sessions: u16,
    pub maximum_session_milliseconds: u64,
    pub idle_timeout_milliseconds: u64,
    pub initialize_timeout_milliseconds: u64,
    pub request_timeout_milliseconds: u64,
    pub total_timeout_milliseconds: u64,
}

impl McpServerLimits {
    pub fn validate(&self) -> Result<(), McpContractError> {
        if self.maximum_message_bytes == 0
            || self.maximum_message_bytes > MAX_MCP_MESSAGE_BYTES
            || self.maximum_response_bytes == 0
            || self.maximum_response_bytes > MAX_MCP_RESPONSE_BYTES
            || self.maximum_headers == 0
            || self.maximum_headers > 256
            || self.maximum_sse_event_bytes == 0
            || self.maximum_sse_event_bytes > MAX_MCP_MESSAGE_BYTES
            || self.maximum_in_flight == 0
            || self.maximum_connections == 0
            || self.maximum_sessions == 0
            || self.maximum_session_milliseconds == 0
            || self.maximum_session_milliseconds > MAX_MCP_SESSION_MILLISECONDS
            || self.idle_timeout_milliseconds == 0
            || self.initialize_timeout_milliseconds == 0
            || self.request_timeout_milliseconds == 0
            || self.total_timeout_milliseconds == 0
            || self.total_timeout_milliseconds > MAX_MCP_TIMEOUT_MILLISECONDS
            || self.idle_timeout_milliseconds >= self.total_timeout_milliseconds
            || self.initialize_timeout_milliseconds >= self.total_timeout_milliseconds
            || self.request_timeout_milliseconds > self.total_timeout_milliseconds
        {
            return Err(McpContractError::InvalidLimits);
        }
        Ok(())
    }
}

pub fn validate_mcp_server_contract(
    transport: McpTransportKind,
    protocol_policy: &ExactVersionRef,
    deployment_credential_requirements: &[SecretPurpose],
    authorization_credential_purpose: Option<&SecretPurpose>,
    limits: &McpServerLimits,
) -> Result<(), McpContractError> {
    protocol_policy
        .validate()
        .map_err(|_| McpContractError::InvalidServer)?;
    if protocol_policy.resource_kind != ResourceKind::PolicyRevision {
        return Err(McpContractError::InvalidServer);
    }
    validate_capability_credential_requirements(deployment_credential_requirements)
        .map_err(|_| McpContractError::InvalidCredentials)?;
    if authorization_credential_purpose.is_some_and(|purpose| {
        deployment_credential_requirements
            .binary_search(purpose)
            .is_ok()
    }) {
        return Err(McpContractError::InvalidCredentials);
    }
    limits.validate()?;
    match transport {
        McpTransportKind::StreamableHttp => Ok(()),
    }
}

/// Exact executable subset of an immutable MCP Server Revision.
///
/// The authoring package is intentionally not carried into a Host process. The repository derives
/// this closed snapshot from the exact `McpServerRevision` selected by the Deployment and verifies
/// its digest before handing it to the execution boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerExecutionContract {
    pub schema_version: u32,
    pub revision: ExactVersionRef,
    pub transport: McpTransportKind,
    pub protocol_policy: ExactVersionRef,
    pub deployment_credential_requirements: Vec<SecretPurpose>,
    pub authorization_credential_purpose: Option<SecretPurpose>,
    pub limits: McpServerLimits,
    pub canonical_digest: Sha256Digest,
}

impl McpServerExecutionContract {
    pub fn build(
        revision: ExactVersionRef,
        transport: McpTransportKind,
        protocol_policy: ExactVersionRef,
        deployment_credential_requirements: Vec<SecretPurpose>,
        authorization_credential_purpose: Option<SecretPurpose>,
        limits: McpServerLimits,
    ) -> Result<Self, McpContractError> {
        let mut contract = Self {
            schema_version: 1,
            revision,
            transport,
            protocol_policy,
            deployment_credential_requirements,
            authorization_credential_purpose,
            limits,
            canonical_digest: digest(&serde_json::json!({"empty": true}))?,
        };
        contract.validate_shape()?;
        contract.canonical_digest = digest_without_field(&contract, "canonical_digest")?;
        Ok(contract)
    }

    pub fn validate(&self) -> Result<(), McpContractError> {
        self.validate_shape()?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpContractError::InvalidServer);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), McpContractError> {
        if self.schema_version != 1
            || self.revision.resource_kind != ResourceKind::McpServerRevision
        {
            return Err(McpContractError::InvalidServer);
        }
        self.revision
            .validate()
            .map_err(|_| McpContractError::InvalidServer)?;
        validate_mcp_server_contract(
            self.transport,
            &self.protocol_policy,
            &self.deployment_credential_requirements,
            self.authorization_credential_purpose.as_ref(),
            &self.limits,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "binding",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum McpTransportBinding {
    StreamableHttp {
        endpoint: CanonicalHttpEndpoint,
        endpoint_identity_digest: Sha256Digest,
        network_policy: ExactVersionRef,
        tls_policy: ExactVersionRef,
    },
}

impl McpTransportBinding {
    pub const fn kind(&self) -> McpTransportKind {
        match self {
            Self::StreamableHttp { .. } => McpTransportKind::StreamableHttp,
        }
    }

    pub fn validate(&self) -> Result<(), McpContractError> {
        match self {
            Self::StreamableHttp {
                endpoint,
                endpoint_identity_digest,
                network_policy,
                tls_policy,
            } => {
                endpoint
                    .validate()
                    .map_err(|_| McpContractError::InvalidTransport)?;
                if endpoint.scheme != CapabilityEndpointScheme::Https
                    || endpoint.canonical_digest().as_ref() != Ok(endpoint_identity_digest)
                {
                    return Err(McpContractError::InvalidTransport);
                }
                validate_policy(network_policy)?;
                validate_policy(tls_policy)?;
                if network_policy == tls_policy {
                    return Err(McpContractError::InvalidTransport);
                }
                Ok(())
            }
        }
    }

    pub fn exact_version_refs(&self) -> Vec<&ExactVersionRef> {
        match self {
            Self::StreamableHttp {
                network_policy,
                tls_policy,
                ..
            } => vec![network_policy, tls_policy],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpNegotiatedCapabilities {
    pub tools: bool,
    pub resources: bool,
    pub prompts: bool,
    pub logging: bool,
    pub tasks: bool,
    pub tasks_list: bool,
    pub tasks_cancel: bool,
    pub tasks_tools_call: bool,
    pub elicitation: bool,
    pub sampling: bool,
    pub roots: bool,
    pub subscriptions: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoverySnapshot {
    pub schema_version: u32,
    pub snapshot_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub server_revision: ExactVersionRef,
    pub protocol_profile: ExactVersionRef,
    pub authorization_context_digest: Sha256Digest,
    pub negotiated_version: String,
    pub negotiated_capabilities: McpNegotiatedCapabilities,
    pub objects_artifact: ArtifactRef,
    pub objects_digest: Sha256Digest,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl McpDiscoverySnapshot {
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        snapshot_id: ResourceId,
        mcp_deployment: ExactDeploymentRef,
        server_revision: ExactVersionRef,
        protocol_profile: ExactVersionRef,
        authorization_context_digest: Sha256Digest,
        negotiated_version: String,
        negotiated_capabilities: McpNegotiatedCapabilities,
        objects_artifact: ArtifactRef,
        observed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, McpContractError> {
        let objects_digest = objects_artifact.content_digest().clone();
        let mut snapshot = Self {
            schema_version: 1,
            snapshot_id,
            mcp_deployment,
            server_revision,
            protocol_profile,
            authorization_context_digest,
            negotiated_version,
            negotiated_capabilities,
            objects_artifact,
            objects_digest,
            observed_at,
            expires_at,
            canonical_digest: digest(&serde_json::json!({"empty": true}))?,
        };
        snapshot.canonical_digest = digest_without_field(&snapshot, "canonical_digest")?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn validate(&self) -> Result<(), McpContractError> {
        if self.schema_version != 1
            || self.snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.server_revision.resource_kind != ResourceKind::McpServerRevision
            || self.protocol_profile.resource_kind != ResourceKind::PolicyRevision
            || !exact_date(&self.negotiated_version)
            || self.objects_artifact.validate().is_err()
            || self.objects_artifact.content_digest() != &self.objects_digest
            || self.expires_at <= self.observed_at
            || (!self.negotiated_capabilities.tasks
                && (self.negotiated_capabilities.tasks_list
                    || self.negotiated_capabilities.tasks_cancel
                    || self.negotiated_capabilities.tasks_tools_call))
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(McpContractError::InvalidDiscoverySnapshot);
        }
        Ok(())
    }
}

fn validate_exact(reference: &ExactVersionRef, kind: ResourceKind) -> Result<(), McpContractError> {
    reference
        .validate()
        .map_err(|_| McpContractError::InvalidTransport)?;
    if reference.resource_kind != kind {
        return Err(McpContractError::InvalidTransport);
    }
    Ok(())
}

fn validate_policy(reference: &ExactVersionRef) -> Result<(), McpContractError> {
    validate_exact(reference, ResourceKind::PolicyRevision)
}

fn exact_date(value: &str) -> bool {
    value.len() == 10 && NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}

fn digest<T: Serialize>(value: &T) -> Result<Sha256Digest, McpContractError> {
    let value = serde_json::to_value(value).map_err(|_| McpContractError::Canonicalization)?;
    canonical_digest(&value)
        .map_err(|_| McpContractError::Canonicalization)?
        .parse()
        .map_err(|_| McpContractError::Canonicalization)
}

fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, McpContractError> {
    let mut value = serde_json::to_value(value).map_err(|_| McpContractError::Canonicalization)?;
    value
        .as_object_mut()
        .ok_or(McpContractError::Canonicalization)?
        .remove(field)
        .ok_or(McpContractError::Canonicalization)?;
    digest(&value)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpContractError {
    InvalidProtocolProfile,
    InvalidLimits,
    InvalidServer,
    InvalidCredentials,
    InvalidTransport,
    InvalidDiscoverySnapshot,
    Canonicalization,
}

impl fmt::Display for McpContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidProtocolProfile => "MCP Protocol Policy is invalid",
            Self::InvalidLimits => "MCP limits are invalid",
            Self::InvalidServer => "MCP Server contract is invalid",
            Self::InvalidCredentials => "MCP credential requirements are invalid",
            Self::InvalidTransport => "MCP transport binding is invalid",
            Self::InvalidDiscoverySnapshot => "MCP Discovery Snapshot is invalid",
            Self::Canonicalization => "MCP contract cannot be canonicalized",
        })
    }
}

impl Error for McpContractError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataClassification;
    use chrono::Duration;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn sha(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), sha(character)).unwrap()
    }

    fn method_limits() -> McpMethodLimits {
        McpMethodLimits {
            maximum_request_bytes: 1_048_576,
            maximum_response_bytes: 1_048_576,
            maximum_metadata_entries: 32,
            maximum_progress_events: 128,
            maximum_pages: 32,
            minimum_poll_milliseconds: 500,
            maximum_poll_milliseconds: 30_000,
        }
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

    fn protocol() -> McpProtocolPolicyDocument {
        McpProtocolPolicyDocument {
            schema_version: 1,
            offered_versions: vec![MCP_PROTOCOL_BASELINE.to_owned()],
            transport_features: McpTransportFeatures {
                streamable_http_get: true,
                streamable_http_sse: true,
                resumable_stream: false,
                session_affinity: true,
            },
            client_capabilities: McpClientCapabilities {
                elicitation_form: true,
                elicitation_url: false,
                tasks_elicitation_create: false,
                sampling: false,
                roots: false,
            },
            allowed_server_capabilities: AllowedMcpServerCapabilities {
                tools: true,
                resources: false,
                prompts: false,
                logging: false,
                tasks: false,
                subscriptions: false,
            },
            experimental_features: vec![],
            method_limits: BTreeMap::from([(PublishedMcpMethod::ToolsCall, method_limits())]),
            metadata_policy: McpMetadataPolicy {
                maximum_server_name_bytes: 128,
                maximum_server_version_bytes: 64,
                maximum_instruction_bytes: 4_096,
                maximum_object_name_bytes: 128,
                maximum_description_bytes: 16_384,
                maximum_icon_bytes: 1_048_576,
            },
        }
    }

    fn oauth_endpoint(host: &str, path: &str) -> McpOAuthEndpoint {
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: host.to_owned(),
            port: 443,
            base_path: path.to_owned(),
        };
        let endpoint_identity_digest = endpoint.canonical_digest().unwrap();
        McpOAuthEndpoint {
            endpoint,
            endpoint_identity_digest,
        }
    }

    fn auth_profile() -> McpAuthPolicyDocument {
        McpAuthPolicyDocument {
            schema_version: 1,
            issuer: oauth_endpoint("auth.example.com", "/"),
            authorization_endpoint: oauth_endpoint("auth.example.com", "/oauth/authorize"),
            token_endpoint: oauth_endpoint("auth.example.com", "/oauth/token"),
            client_id: "insight-platform".to_owned(),
            client_authentication: McpOAuthClientAuthenticationKind::ClientSecretBasic,
            client_credential_purpose: Some("mcp.oauth.client".parse().unwrap()),
            pkce_secret_provider_id: id(ResourceKind::SecretProvider, 0x80),
            token_secret_provider_id: id(ResourceKind::SecretProvider, 0x81),
            redirect_uri: oauth_endpoint("platform.example.com", "/v1/mcp/oauth/callback"),
            resource_indicator: oauth_endpoint("mcp.example.com", "/mcp"),
            allowed_scopes: vec!["resources.read".to_owned(), "tools.call".to_owned()],
            maximum_token_response_bytes: 65_536,
            connect_timeout_milliseconds: 5_000,
            total_timeout_milliseconds: 30_000,
            maximum_clock_skew_seconds: 60,
        }
    }

    #[test]
    fn oauth_auth_profile_is_closed_scope_bounded_and_credential_role_exact() {
        let profile = auth_profile();
        profile.validate().unwrap();
        assert!(profile.permits_scopes(&["tools.call".to_owned()]));
        assert!(!profile.permits_scopes(&["tools.write".to_owned()]));

        let mut missing_client_secret_role = profile.clone();
        missing_client_secret_role.client_credential_purpose = None;
        assert_eq!(
            missing_client_secret_role.validate(),
            Err(McpContractError::InvalidCredentials)
        );

        let mut unsorted = profile;
        unsorted.allowed_scopes.reverse();
        assert_eq!(
            unsorted.validate(),
            Err(McpContractError::InvalidCredentials)
        );
    }

    #[test]
    fn protocol_profile_is_closed_and_tasks_require_the_exact_experimental_gate() {
        let profile = protocol();
        profile.validate().unwrap();
        profile.canonical_digest().unwrap();

        let mut task_without_gate = profile;
        task_without_gate
            .method_limits
            .insert(PublishedMcpMethod::TasksGet, method_limits());
        assert_eq!(
            task_without_gate.validate(),
            Err(McpContractError::InvalidProtocolProfile)
        );

        let mut wire = serde_json::to_value(protocol()).unwrap();
        wire.as_object_mut()
            .unwrap()
            .insert("latest".to_owned(), serde_json::json!(true));
        assert!(serde_json::from_value::<McpProtocolPolicyDocument>(wire).is_err());
    }

    #[test]
    fn resource_list_is_a_closed_resources_method_with_its_own_limits() {
        let mut profile = protocol();
        profile.allowed_server_capabilities.resources = true;
        profile
            .method_limits
            .insert(PublishedMcpMethod::ResourcesList, method_limits());
        profile.validate().unwrap();
        assert_eq!(
            serde_json::to_value(PublishedMcpMethod::ResourcesList).unwrap(),
            serde_json::json!("resources_list")
        );

        profile.allowed_server_capabilities.resources = false;
        assert_eq!(
            profile.validate(),
            Err(McpContractError::InvalidProtocolProfile)
        );
    }

    #[test]
    fn streamable_http_binding_requires_exact_https_endpoint_and_distinct_policies() {
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "mcp.example.test".to_owned(),
            port: 443,
            base_path: "/mcp".to_owned(),
        };
        let binding = McpTransportBinding::StreamableHttp {
            endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
            endpoint,
            network_policy: exact(ResourceKind::PolicyRevision, 1, '1'),
            tls_policy: exact(ResourceKind::PolicyRevision, 2, '2'),
        };
        binding.validate().unwrap();

        let mut insecure = binding;
        let McpTransportBinding::StreamableHttp { endpoint, .. } = &mut insecure;
        endpoint.scheme = CapabilityEndpointScheme::Http;
        assert_eq!(insecure.validate(), Err(McpContractError::InvalidTransport));
    }

    #[test]
    fn authorization_credentials_are_distinct_from_deployment_credentials() {
        let authorization = "mcp.oauth".parse::<SecretPurpose>().unwrap();
        assert_eq!(
            validate_mcp_server_contract(
                McpTransportKind::StreamableHttp,
                &exact(ResourceKind::PolicyRevision, 1, '1'),
                std::slice::from_ref(&authorization),
                Some(&authorization),
                &server_limits(),
            ),
            Err(McpContractError::InvalidCredentials)
        );

        validate_mcp_server_contract(
            McpTransportKind::StreamableHttp,
            &exact(ResourceKind::PolicyRevision, 1, '1'),
            &["mcp.client_secret".parse().unwrap()],
            Some(&authorization),
            &server_limits(),
        )
        .unwrap();
    }

    #[test]
    fn discovery_snapshot_freezes_exact_source_authorization_and_artifact() {
        let artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, 10),
            sha('a'),
            128,
            "application/json",
            DataClassification::Internal,
            Some("mcp-discovery.json".to_owned()),
        )
        .unwrap();
        let observed_at = Utc::now();
        let snapshot = McpDiscoverySnapshot::build(
            id(ResourceKind::McpDiscoverySnapshot, 11),
            ExactDeploymentRef::new(id(ResourceKind::McpDeployment, 12), sha('b')).unwrap(),
            exact(ResourceKind::McpServerRevision, 13, 'c'),
            exact(ResourceKind::PolicyRevision, 14, 'd'),
            sha('e'),
            MCP_PROTOCOL_BASELINE.to_owned(),
            McpNegotiatedCapabilities {
                tools: true,
                resources: false,
                prompts: false,
                logging: false,
                tasks: false,
                tasks_list: false,
                tasks_cancel: false,
                tasks_tools_call: false,
                elicitation: true,
                sampling: false,
                roots: false,
                subscriptions: false,
            },
            artifact,
            observed_at,
            observed_at + Duration::hours(1),
        )
        .unwrap();
        snapshot.validate().unwrap();

        let mut forged = snapshot;
        forged.authorization_context_digest = sha('f');
        assert_eq!(
            forged.validate(),
            Err(McpContractError::InvalidDiscoverySnapshot)
        );
    }
}
