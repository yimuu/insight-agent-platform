use super::{
    is_public_destination_ip, parse_endpoint_host, DnsResolutionError, EgressConfigurationError,
    EgressDnsResolver, ParsedEndpointHost, SecretMaterialResolutionError, SecretMaterialResolver,
    MAX_DNS_ANSWERS_HARD, MAX_EGRESS_IN_FLIGHT_HARD, MAX_SECRET_MATERIAL_BYTES_HARD,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt};
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, CanonicalHttpEndpoint,
    CapabilityEndpointScheme, ClosedJsonValue, ExactDeploymentRef, ExactVersionRef,
    InteractionSchemaDocument, JsonLimits, McpClientCapabilities, McpNegotiatedCapabilities,
    PublishedMcpMethod, ResourceId, ResourceKind, SecretPurpose, SecretResolutionPolicy,
    Sha256Digest, MAX_MCP_SESSION_MILLISECONDS,
};
use insight_platform_mcp_host::{
    EncryptedMcpState, EstablishedMcpSubscription, McpElicitationAction, McpElicitationResponse,
    McpOperationOutcome, McpRemoteTaskCancelOutcome, McpRemoteTaskLimits,
    McpResourceRefreshConnector, McpResourceRefreshTransportEvidence,
    McpResourceRefreshTransportRequest,
    McpStreamableHttpConnector, McpStreamableHttpRequest, McpStreamableHttpSubscriptionConnector,
    McpStreamableHttpSubscriptionNotification, McpStreamableHttpSubscriptionRequest,
    McpStreamableHttpSubscriptionSink, McpStreamableHttpSubscriptionSinkError,
    McpStreamableHttpSubscriptionTermination, McpSubscriptionActivation, McpTransportFailure,
    PreparedMcpSubscription, SafeMcpFailure, SensitiveMcpNotificationWire,
};
#[cfg(any())]
use insight_platform_sandbox::{
    ManagedMcpSandboxOpaqueSessionClaims, ManagedMcpSandboxSessionStateSealError,
    ManagedMcpSandboxSessionStateSealer,
};
use reqwest::{
    header::{
        HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH,
        CONTENT_TYPE, WWW_AUTHENTICATE,
    },
    Method, StatusCode,
};
use ring::{
    aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN},
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

pub const MAX_INSTALLED_MCP_HTTP_ENDPOINTS: usize = 4_096;
pub const MAX_MCP_TRUST_BUNDLE_BYTES: usize = 262_144;
pub const MAX_MCP_HTTP_REQUEST_BYTES_HARD: u32 = 1_048_576;
pub const MAX_MCP_HTTP_RESPONSE_BYTES_HARD: u32 = 16_777_216;
pub const MAX_MCP_HTTP_HEADERS_HARD: u16 = 256;
pub const MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD: u64 = 600_000;
const MAX_MCP_SESSION_ID_BYTES: usize = 1_024;
const MCP_SESSION_HEADER: &str = "mcp-session-id";
const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
const MCP_REMOTE_TASK_STATE_SCHEME: &str = "aes256_gcm_v1";
const MCP_REMOTE_TASK_STATE_AAD: &[u8] = b"insight.platform/v1/mcp-remote-task\0";
const MCP_REMOTE_TASK_KEY_BYTES: usize = 32;
const MAX_MCP_REMOTE_TASK_KEYS: usize = 4;
const MAX_MCP_REMOTE_TASK_KEY_ID_BYTES: usize = 64;
const MAX_MCP_REMOTE_TASK_ID_BYTES: usize = 1_024;
const MAX_MCP_ELICITATION_MESSAGE_BYTES: usize = 4_096;
const MCP_SUBSCRIPTION_STATE_SCHEME: &str = "aes256_gcm_v1";
const MCP_SUBSCRIPTION_STATE_AAD: &[u8] = b"insight.platform/v1/mcp-subscription-session\0";
#[cfg(any())]
const MANAGED_MCP_SANDBOX_SESSION_STATE_SCHEME: &str = "aes256_gcm_v1";
#[cfg(any())]
const MANAGED_MCP_SANDBOX_SESSION_STATE_AAD: &[u8] =
    b"insight.platform/v1/managed-mcp-sandbox-session\0";
const MCP_SUBSCRIPTION_STATE_KEY_BYTES: usize = 32;
const MAX_MCP_SUBSCRIPTION_STATE_KEYS: usize = 4;
const MAX_MCP_SUBSCRIPTION_KEY_ID_BYTES: usize = 64;
const LAST_EVENT_ID_HEADER: &str = "last-event-id";
const MAX_MCP_SSE_EVENT_ID_BYTES: usize = 1_024;
const DEFAULT_MCP_SSE_RETRY_MILLISECONDS: u64 = 3_000;
const MAX_MCP_SSE_RETRY_MILLISECONDS: u64 = 60_000;
const MAX_MCP_SSE_CONTROL_EVENTS: u16 = 16;
const MAX_MCP_SUBSCRIPTION_RECONNECTS_HARD: u32 = 10_000;
const MAX_MCP_SUBSCRIPTION_EVENTS_HARD: u64 = 10_000_000;

pub struct SensitiveMcpRemoteTaskStateKey(Vec<u8>);

impl SensitiveMcpRemoteTaskStateKey {
    pub fn new(material: Vec<u8>) -> Result<Self, EgressConfigurationError> {
        if material.len() != MCP_REMOTE_TASK_KEY_BYTES {
            return Err(EgressConfigurationError::InvalidCryptographicKey);
        }
        Ok(Self(material))
    }

    fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SensitiveMcpRemoteTaskStateKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveMcpRemoteTaskStateKey")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for SensitiveMcpRemoteTaskStateKey {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

#[derive(Debug)]
pub struct McpRemoteTaskStateKey {
    pub key_id: String,
    pub key_reference_digest: Sha256Digest,
    pub key_material: SensitiveMcpRemoteTaskStateKey,
}

struct InstalledMcpRemoteTaskStateKey {
    key_reference_digest: Sha256Digest,
    key: LessSafeKey,
}

pub struct AeadMcpRemoteTaskStateCodec {
    active_key_id: String,
    keys: BTreeMap<String, InstalledMcpRemoteTaskStateKey>,
    random: SystemRandom,
}

pub struct SensitiveMcpSubscriptionStateKey(Vec<u8>);

impl SensitiveMcpSubscriptionStateKey {
    pub fn new(material: Vec<u8>) -> Result<Self, EgressConfigurationError> {
        if material.len() != MCP_SUBSCRIPTION_STATE_KEY_BYTES {
            return Err(EgressConfigurationError::InvalidCryptographicKey);
        }
        Ok(Self(material))
    }

    fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SensitiveMcpSubscriptionStateKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveMcpSubscriptionStateKey")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for SensitiveMcpSubscriptionStateKey {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

#[derive(Debug)]
pub struct McpSubscriptionStateKey {
    pub key_id: String,
    pub key_reference_digest: Sha256Digest,
    pub key_material: SensitiveMcpSubscriptionStateKey,
}

pub struct AeadMcpSubscriptionStateCodec {
    active_key_id: String,
    keys: BTreeMap<String, InstalledMcpRemoteTaskStateKey>,
    random: SystemRandom,
}

impl AeadMcpSubscriptionStateCodec {
    pub fn new(
        active_key_id: String,
        keys: Vec<McpSubscriptionStateKey>,
    ) -> Result<Self, EgressConfigurationError> {
        if !valid_subscription_key_id(&active_key_id)
            || keys.is_empty()
            || keys.len() > MAX_MCP_SUBSCRIPTION_STATE_KEYS
        {
            return Err(EgressConfigurationError::InvalidCryptographicKey);
        }
        let mut installed = BTreeMap::new();
        for key in keys {
            if !valid_subscription_key_id(&key.key_id) {
                return Err(EgressConfigurationError::InvalidCryptographicKey);
            }
            let unbound = UnboundKey::new(&AES_256_GCM, key.key_material.expose())
                .map_err(|_| EgressConfigurationError::InvalidCryptographicKey)?;
            if installed
                .insert(
                    key.key_id,
                    InstalledMcpRemoteTaskStateKey {
                        key_reference_digest: key.key_reference_digest,
                        key: LessSafeKey::new(unbound),
                    },
                )
                .is_some()
            {
                return Err(EgressConfigurationError::InvalidCryptographicKey);
            }
        }
        if !installed.contains_key(&active_key_id) {
            return Err(EgressConfigurationError::InvalidCryptographicKey);
        }
        Ok(Self {
            active_key_id,
            keys: installed,
            random: SystemRandom::new(),
        })
    }

    fn seal(
        &self,
        claims: &McpSubscriptionStateClaims,
    ) -> Result<EncryptedMcpState, McpTransportFailure> {
        claims.validate()?;
        let value = serde_json::to_value(claims)
            .map_err(|_| rejected("mcp_egress_subscription_state_invalid"))?;
        let plaintext_digest = digest_value(&value)?;
        let mut plaintext = SensitiveRemoteTaskBuffer(
            canonical_json(&value)
                .map_err(|_| rejected("mcp_egress_subscription_state_invalid"))?,
        );
        let installed = self
            .keys
            .get(&self.active_key_id)
            .ok_or_else(|| rejected("mcp_egress_subscription_key_unavailable"))?;
        let mut nonce_bytes = [0_u8; NONCE_LEN];
        self.random
            .fill(&mut nonce_bytes)
            .map_err(|_| retryable("mcp_egress_random_unavailable"))?;
        installed
            .key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(subscription_state_aad(&self.active_key_id)),
                &mut plaintext.0,
            )
            .map_err(|_| retryable("mcp_egress_subscription_state_seal_unavailable"))?;
        let mut ciphertext = Vec::with_capacity(NONCE_LEN + plaintext.0.len());
        ciphertext.extend_from_slice(&nonce_bytes);
        ciphertext.extend_from_slice(&plaintext.0);
        let state = EncryptedMcpState {
            scheme: MCP_SUBSCRIPTION_STATE_SCHEME.to_owned(),
            ciphertext,
            key_id: self.active_key_id.clone(),
            key_reference_digest: installed.key_reference_digest.clone(),
            plaintext_digest,
        };
        state
            .validate()
            .map_err(|_| rejected("mcp_egress_subscription_state_invalid"))?;
        Ok(state)
    }

    #[cfg(any())]
    fn seal_managed_sandbox_session(
        &self,
        claims: &ManagedMcpSandboxOpaqueSessionClaims,
    ) -> Result<EncryptedMcpState, ManagedMcpSandboxSessionStateSealError> {
        claims
            .validate_shape(Utc::now())
            .map_err(|_| ManagedMcpSandboxSessionStateSealError::Denied)?;
        let value = serde_json::to_value(claims)
            .map_err(|_| ManagedMcpSandboxSessionStateSealError::Denied)?;
        let plaintext_digest =
            digest_value(&value).map_err(|_| ManagedMcpSandboxSessionStateSealError::Denied)?;
        if claims
            .canonical_digest()
            .map_err(|_| ManagedMcpSandboxSessionStateSealError::Denied)?
            != plaintext_digest
        {
            return Err(ManagedMcpSandboxSessionStateSealError::Denied);
        }
        let mut plaintext = SensitiveRemoteTaskBuffer(
            canonical_json(&value).map_err(|_| ManagedMcpSandboxSessionStateSealError::Denied)?,
        );
        let installed = self
            .keys
            .get(&self.active_key_id)
            .ok_or(ManagedMcpSandboxSessionStateSealError::Unavailable)?;
        let mut nonce_bytes = [0_u8; NONCE_LEN];
        self.random
            .fill(&mut nonce_bytes)
            .map_err(|_| ManagedMcpSandboxSessionStateSealError::Unavailable)?;
        installed
            .key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(managed_mcp_sandbox_session_state_aad(&self.active_key_id)),
                &mut plaintext.0,
            )
            .map_err(|_| ManagedMcpSandboxSessionStateSealError::Unavailable)?;
        let mut ciphertext = Vec::with_capacity(NONCE_LEN + plaintext.0.len());
        ciphertext.extend_from_slice(&nonce_bytes);
        ciphertext.extend_from_slice(&plaintext.0);
        let state = EncryptedMcpState {
            scheme: MANAGED_MCP_SANDBOX_SESSION_STATE_SCHEME.to_owned(),
            ciphertext,
            key_id: self.active_key_id.clone(),
            key_reference_digest: installed.key_reference_digest.clone(),
            plaintext_digest,
        };
        state
            .validate()
            .map_err(|_| ManagedMcpSandboxSessionStateSealError::Denied)?;
        Ok(state)
    }

    #[cfg(test)]
    fn open(
        &self,
        state: &EncryptedMcpState,
    ) -> Result<McpSubscriptionStateClaims, McpTransportFailure> {
        state
            .validate()
            .map_err(|_| rejected("mcp_egress_subscription_state_invalid"))?;
        let installed = self
            .keys
            .get(&state.key_id)
            .filter(|key| key.key_reference_digest == state.key_reference_digest)
            .ok_or_else(|| rejected("mcp_egress_subscription_key_unavailable"))?;
        if state.scheme != MCP_SUBSCRIPTION_STATE_SCHEME
            || state.ciphertext.len() <= NONCE_LEN + AES_256_GCM.tag_len()
        {
            return Err(rejected("mcp_egress_subscription_state_invalid"));
        }
        let nonce_bytes: [u8; NONCE_LEN] = state.ciphertext[..NONCE_LEN]
            .try_into()
            .map_err(|_| rejected("mcp_egress_subscription_state_invalid"))?;
        let mut plaintext = SensitiveRemoteTaskBuffer(state.ciphertext[NONCE_LEN..].to_vec());
        let opened = installed
            .key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(subscription_state_aad(&state.key_id)),
                &mut plaintext.0,
            )
            .map_err(|_| rejected("mcp_egress_subscription_state_invalid"))?;
        let claims: McpSubscriptionStateClaims = serde_json::from_slice(opened)
            .map_err(|_| rejected("mcp_egress_subscription_state_invalid"))?;
        claims.validate()?;
        let value = serde_json::to_value(&claims)
            .map_err(|_| rejected("mcp_egress_subscription_state_invalid"))?;
        if digest_value(&value)? != state.plaintext_digest {
            return Err(rejected("mcp_egress_subscription_state_invalid"));
        }
        Ok(claims)
    }
}

#[cfg(any())]
#[async_trait]
impl ManagedMcpSandboxSessionStateSealer for AeadMcpSubscriptionStateCodec {
    async fn seal_managed_mcp_sandbox_session_state(
        &self,
        claims: ManagedMcpSandboxOpaqueSessionClaims,
    ) -> Result<EncryptedMcpState, ManagedMcpSandboxSessionStateSealError> {
        self.seal_managed_sandbox_session(&claims)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpSubscriptionStateClaims {
    schema_version: u32,
    tenant_id: ResourceId,
    deployment: ExactDeploymentRef,
    authorization_binding_id: ResourceId,
    authorization_generation: u64,
    principal_binding_generation: u64,
    subscription_id: ResourceId,
    session_generation: u64,
    binding_digest: Sha256Digest,
    session_id: Option<Vec<u8>>,
    external_identity_digest: Sha256Digest,
    expires_at_epoch_milliseconds: i64,
}

impl McpSubscriptionStateClaims {
    fn validate(&self) -> Result<(), McpTransportFailure> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.deployment.resource_kind != ResourceKind::McpDeployment
            || self.deployment.validate().is_err()
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.authorization_generation == 0
            || self.principal_binding_generation == 0
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.session_generation == 0
            || self.session_id.as_deref().is_some_and(|value| {
                value.is_empty()
                    || value.len() > MAX_MCP_SESSION_ID_BYTES
                    || !value.iter().all(u8::is_ascii_graphic)
            })
            || self.expires_at_epoch_milliseconds <= 0
        {
            return Err(rejected("mcp_egress_subscription_state_invalid"));
        }
        Ok(())
    }
}

impl fmt::Debug for McpSubscriptionStateClaims {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpSubscriptionStateClaims")
            .field("schema_version", &self.schema_version)
            .field("tenant_id", &self.tenant_id)
            .field("deployment", &self.deployment)
            .field("authorization_binding_id", &self.authorization_binding_id)
            .field("authorization_generation", &self.authorization_generation)
            .field("subscription_id", &self.subscription_id)
            .field("session_generation", &self.session_generation)
            .field("session_id", &"[REDACTED]")
            .field("external_identity_digest", &self.external_identity_digest)
            .field(
                "expires_at_epoch_milliseconds",
                &self.expires_at_epoch_milliseconds,
            )
            .finish()
    }
}

impl Drop for McpSubscriptionStateClaims {
    fn drop(&mut self) {
        if let Some(session_id) = self.session_id.as_mut() {
            session_id.fill(0);
        }
    }
}

fn subscription_state_aad(key_id: &str) -> Vec<u8> {
    let mut aad = Vec::with_capacity(MCP_SUBSCRIPTION_STATE_AAD.len() + key_id.len());
    aad.extend_from_slice(MCP_SUBSCRIPTION_STATE_AAD);
    aad.extend_from_slice(key_id.as_bytes());
    aad
}

#[cfg(any())]
fn managed_mcp_sandbox_session_state_aad(key_id: &str) -> Vec<u8> {
    let mut aad = Vec::with_capacity(MANAGED_MCP_SANDBOX_SESSION_STATE_AAD.len() + key_id.len());
    aad.extend_from_slice(MANAGED_MCP_SANDBOX_SESSION_STATE_AAD);
    aad.extend_from_slice(key_id.as_bytes());
    aad
}

fn valid_subscription_key_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MCP_SUBSCRIPTION_KEY_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

impl AeadMcpRemoteTaskStateCodec {
    pub fn new(
        active_key_id: String,
        keys: Vec<McpRemoteTaskStateKey>,
    ) -> Result<Self, EgressConfigurationError> {
        if !valid_remote_task_key_id(&active_key_id)
            || keys.is_empty()
            || keys.len() > MAX_MCP_REMOTE_TASK_KEYS
        {
            return Err(EgressConfigurationError::InvalidCryptographicKey);
        }
        let mut installed = BTreeMap::new();
        for key in keys {
            if !valid_remote_task_key_id(&key.key_id) {
                return Err(EgressConfigurationError::InvalidCryptographicKey);
            }
            let unbound = UnboundKey::new(&AES_256_GCM, key.key_material.expose())
                .map_err(|_| EgressConfigurationError::InvalidCryptographicKey)?;
            if installed
                .insert(
                    key.key_id,
                    InstalledMcpRemoteTaskStateKey {
                        key_reference_digest: key.key_reference_digest,
                        key: LessSafeKey::new(unbound),
                    },
                )
                .is_some()
            {
                return Err(EgressConfigurationError::InvalidCryptographicKey);
            }
        }
        if !installed.contains_key(&active_key_id) {
            return Err(EgressConfigurationError::InvalidCryptographicKey);
        }
        Ok(Self {
            active_key_id,
            keys: installed,
            random: SystemRandom::new(),
        })
    }

    fn seal(
        &self,
        claims: &McpRemoteTaskStateClaims,
    ) -> Result<EncryptedMcpState, McpTransportFailure> {
        claims.validate()?;
        let claims_value = serde_json::to_value(claims)
            .map_err(|_| rejected("mcp_egress_remote_task_state_invalid"))?;
        let plaintext_digest = digest_value(&claims_value)?;
        let mut plaintext = SensitiveRemoteTaskBuffer(
            canonical_json(&claims_value)
                .map_err(|_| rejected("mcp_egress_remote_task_state_invalid"))?,
        );
        let installed = self
            .keys
            .get(&self.active_key_id)
            .ok_or_else(|| rejected("mcp_egress_remote_task_key_unavailable"))?;
        let mut nonce_bytes = [0_u8; NONCE_LEN];
        self.random
            .fill(&mut nonce_bytes)
            .map_err(|_| retryable("mcp_egress_random_unavailable"))?;
        installed
            .key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(remote_task_aad(&self.active_key_id)),
                &mut plaintext.0,
            )
            .map_err(|_| retryable("mcp_egress_remote_task_seal_unavailable"))?;
        let mut ciphertext = Vec::with_capacity(NONCE_LEN + plaintext.0.len());
        ciphertext.extend_from_slice(&nonce_bytes);
        ciphertext.extend_from_slice(&plaintext.0);
        let state = EncryptedMcpState {
            scheme: MCP_REMOTE_TASK_STATE_SCHEME.to_owned(),
            ciphertext,
            key_id: self.active_key_id.clone(),
            key_reference_digest: installed.key_reference_digest.clone(),
            plaintext_digest,
        };
        state
            .validate()
            .map_err(|_| rejected("mcp_egress_remote_task_state_invalid"))?;
        Ok(state)
    }

    fn open(
        &self,
        state: &EncryptedMcpState,
    ) -> Result<McpRemoteTaskStateClaims, McpTransportFailure> {
        state
            .validate()
            .map_err(|_| rejected("mcp_egress_remote_task_state_invalid"))?;
        let installed = self
            .keys
            .get(&state.key_id)
            .filter(|key| key.key_reference_digest == state.key_reference_digest)
            .ok_or_else(|| rejected("mcp_egress_remote_task_key_unavailable"))?;
        if state.scheme != MCP_REMOTE_TASK_STATE_SCHEME
            || state.ciphertext.len() <= NONCE_LEN + AES_256_GCM.tag_len()
        {
            return Err(rejected("mcp_egress_remote_task_state_invalid"));
        }
        let nonce_bytes: [u8; NONCE_LEN] = state.ciphertext[..NONCE_LEN]
            .try_into()
            .map_err(|_| rejected("mcp_egress_remote_task_state_invalid"))?;
        let mut plaintext = SensitiveRemoteTaskBuffer(state.ciphertext[NONCE_LEN..].to_vec());
        let opened = installed
            .key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(remote_task_aad(&state.key_id)),
                &mut plaintext.0,
            )
            .map_err(|_| rejected("mcp_egress_remote_task_state_invalid"))?;
        let claims: McpRemoteTaskStateClaims = serde_json::from_slice(opened)
            .map_err(|_| rejected("mcp_egress_remote_task_state_invalid"))?;
        claims.validate()?;
        let claims_value = serde_json::to_value(&claims)
            .map_err(|_| rejected("mcp_egress_remote_task_state_invalid"))?;
        if digest_value(&claims_value)? != state.plaintext_digest {
            return Err(rejected("mcp_egress_remote_task_state_invalid"));
        }
        Ok(claims)
    }
}

struct SensitiveRemoteTaskBuffer(Vec<u8>);

impl Drop for SensitiveRemoteTaskBuffer {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpRemoteTaskStateClaims {
    schema_version: u32,
    tenant_id: ResourceId,
    deployment: ExactDeploymentRef,
    authorization_binding_id: ResourceId,
    authorization_generation: u64,
    principal_binding_generation: u64,
    invocation_id: ResourceId,
    job_id: ResourceId,
    physical_attempt: u32,
    discovery_snapshot_id: ResourceId,
    discovery_snapshot_digest: Sha256Digest,
    protocol_policy: ExactVersionRef,
    operation_id: ResourceId,
    task_id: Vec<u8>,
    session_id: Option<Vec<u8>>,
    pending_elicitation: Option<PendingMcpElicitation>,
    external_identity_digest: Sha256Digest,
    deadline_epoch_milliseconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum McpJsonRpcRequestId {
    String(String),
    Integer(i64),
}

impl McpJsonRpcRequestId {
    fn parse(value: &Value) -> Result<Self, McpTransportFailure> {
        if let Some(value) = value.as_str() {
            if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
                return Err(rejected("mcp_egress_elicitation_request_id_invalid"));
            }
            return Ok(Self::String(value.to_owned()));
        }
        value
            .as_i64()
            .map(Self::Integer)
            .ok_or_else(|| rejected("mcp_egress_elicitation_request_id_invalid"))
    }

    fn value(&self) -> Value {
        match self {
            Self::String(value) => Value::String(value.clone()),
            Self::Integer(value) => Value::Number((*value).into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingMcpElicitation {
    request_id: McpJsonRpcRequestId,
    response_schema: InteractionSchemaDocument,
}

impl PendingMcpElicitation {
    fn validate(&self) -> Result<(), McpTransportFailure> {
        self.response_schema
            .validate()
            .map_err(|_| rejected("mcp_egress_elicitation_schema_invalid"))?;
        if self.response_schema.profile != insight_platform_contracts::MCP_FORM_SCHEMA_PROFILE_ID {
            return Err(rejected("mcp_egress_elicitation_schema_invalid"));
        }
        Ok(())
    }
}

impl fmt::Debug for McpRemoteTaskStateClaims {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpRemoteTaskStateClaims")
            .field("schema_version", &self.schema_version)
            .field("tenant_id", &self.tenant_id)
            .field("deployment", &self.deployment)
            .field("authorization_binding_id", &self.authorization_binding_id)
            .field("authorization_generation", &self.authorization_generation)
            .field(
                "principal_binding_generation",
                &self.principal_binding_generation,
            )
            .field("invocation_id", &self.invocation_id)
            .field("job_id", &self.job_id)
            .field("physical_attempt", &self.physical_attempt)
            .field("discovery_snapshot_id", &self.discovery_snapshot_id)
            .field("discovery_snapshot_digest", &self.discovery_snapshot_digest)
            .field("protocol_policy", &self.protocol_policy)
            .field("operation_id", &self.operation_id)
            .field("task_id", &"[REDACTED]")
            .field("session_id", &"[REDACTED]")
            .field(
                "pending_elicitation",
                &self.pending_elicitation.as_ref().map(|_| "[REDACTED]"),
            )
            .field("external_identity_digest", &self.external_identity_digest)
            .field(
                "deadline_epoch_milliseconds",
                &self.deadline_epoch_milliseconds,
            )
            .finish()
    }
}

impl McpRemoteTaskStateClaims {
    fn validate(&self) -> Result<(), McpTransportFailure> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.deployment.resource_kind != ResourceKind::McpDeployment
            || self.deployment.validate().is_err()
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.authorization_generation == 0
            || self.principal_binding_generation == 0
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.physical_attempt == 0
            || self.discovery_snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
            || self.protocol_policy.resource_kind != ResourceKind::PolicyRevision
            || self.protocol_policy.validate().is_err()
            || self.operation_id.kind() != ResourceKind::McpOperation
            || std::str::from_utf8(&self.task_id)
                .ok()
                .is_none_or(|value| !valid_remote_task_id(value))
            || self.session_id.as_deref().is_some_and(|value| {
                value.is_empty()
                    || value.len() > MAX_MCP_SESSION_ID_BYTES
                    || !value.iter().all(u8::is_ascii_graphic)
            })
            || self
                .pending_elicitation
                .as_ref()
                .is_some_and(|pending| pending.validate().is_err())
            || self.deadline_epoch_milliseconds <= 0
        {
            return Err(rejected("mcp_egress_remote_task_state_invalid"));
        }
        Ok(())
    }

    fn validate_for(&self, request: &McpStreamableHttpRequest) -> Result<(), McpTransportFailure> {
        self.validate()?;
        if self.tenant_id != request.tenant_id
            || self.deployment != request.deployment
            || self.authorization_binding_id != request.authorization_binding_id
            || self.authorization_generation != request.authorization_generation
            || self.principal_binding_generation != request.principal_binding_generation
            || self.invocation_id != request.invocation_id
            || self.job_id != request.job_id
            || self.physical_attempt != request.physical_attempt
            || self.discovery_snapshot_id != request.discovery_snapshot_id
            || self.discovery_snapshot_digest != request.discovery_snapshot_digest
            || self.protocol_policy != request.protocol_policy
            || self.operation_id != request.operation_id
            || self.deadline_epoch_milliseconds != request.deadline.timestamp_millis()
            || request.continuation.as_ref().is_none_or(|continuation| {
                continuation.external_identity_digest != self.external_identity_digest
            })
        {
            return Err(rejected("mcp_egress_remote_task_binding_changed"));
        }
        Ok(())
    }

    fn task_id(&self) -> Result<&str, McpTransportFailure> {
        std::str::from_utf8(&self.task_id)
            .map_err(|_| rejected("mcp_egress_remote_task_state_invalid"))
    }
}

impl Drop for McpRemoteTaskStateClaims {
    fn drop(&mut self) {
        self.task_id.fill(0);
        if let Some(session_id) = self.session_id.as_mut() {
            session_id.fill(0);
        }
    }
}

fn remote_task_aad(key_id: &str) -> Vec<u8> {
    let mut aad = Vec::with_capacity(MCP_REMOTE_TASK_STATE_AAD.len() + key_id.len());
    aad.extend_from_slice(MCP_REMOTE_TASK_STATE_AAD);
    aad.extend_from_slice(key_id.as_bytes());
    aad
}

fn valid_remote_task_key_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MCP_REMOTE_TASK_KEY_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn valid_remote_task_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MCP_REMOTE_TASK_ID_BYTES
        && !value.chars().any(char::is_control)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpStreamableHttpEgressLimits {
    pub maximum_in_flight: usize,
    pub maximum_dns_answers: usize,
    pub maximum_secret_material_bytes: usize,
    pub maximum_subscription_reconnects: u32,
    pub maximum_subscription_events_per_session: u64,
}

impl McpStreamableHttpEgressLimits {
    pub fn validate(self) -> Result<(), EgressConfigurationError> {
        if self.maximum_in_flight == 0
            || self.maximum_in_flight > MAX_EGRESS_IN_FLIGHT_HARD
            || self.maximum_dns_answers == 0
            || self.maximum_dns_answers > MAX_DNS_ANSWERS_HARD
            || self.maximum_secret_material_bytes == 0
            || self.maximum_secret_material_bytes > MAX_SECRET_MATERIAL_BYTES_HARD
            || self.maximum_subscription_reconnects == 0
            || self.maximum_subscription_reconnects > MAX_MCP_SUBSCRIPTION_RECONNECTS_HARD
            || self.maximum_subscription_events_per_session == 0
            || self.maximum_subscription_events_per_session > MAX_MCP_SUBSCRIPTION_EVENTS_HARD
        {
            return Err(EgressConfigurationError::InvalidLimits);
        }
        Ok(())
    }
}

impl Default for McpStreamableHttpEgressLimits {
    fn default() -> Self {
        Self {
            maximum_in_flight: 128,
            maximum_dns_answers: 16,
            maximum_secret_material_bytes: 8_192,
            maximum_subscription_reconnects: 128,
            maximum_subscription_events_per_session: 1_000_000,
        }
    }
}

/// Process-installed Streamable HTTP closure. Runtime callers may select only this exact identity;
/// they cannot submit an arbitrary URI, redirect, header or credential injection rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledMcpStreamableHttpEndpoint {
    pub schema_version: u32,
    pub deployment: ExactDeploymentRef,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub server_identity_digest: Sha256Digest,
    pub protocol_policy: ExactVersionRef,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub auth_policy: ExactVersionRef,
    pub token_credential_purpose: SecretPurpose,
    pub trusted_root_pem: String,
}

impl InstalledMcpStreamableHttpEndpoint {
    pub fn validate(&self) -> Result<(), EgressConfigurationError> {
        self.deployment
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        self.endpoint
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        if self.schema_version != 1
            || self.deployment.resource_kind != ResourceKind::McpDeployment
            || self.endpoint.scheme != CapabilityEndpointScheme::Https
            || self.endpoint.canonical_digest().as_ref() != Ok(&self.endpoint_identity_digest)
            || parse_endpoint_host(&self.endpoint.host).is_err()
            || !valid_mcp_path(&self.endpoint.base_path)
            || self.trusted_root_pem.is_empty()
            || self.trusted_root_pem.len() > MAX_MCP_TRUST_BUNDLE_BYTES
        {
            return Err(EgressConfigurationError::InvalidEndpoint);
        }
        let roots = reqwest::Certificate::from_pem_bundle(self.trusted_root_pem.as_bytes())
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        if roots.is_empty() {
            return Err(EgressConfigurationError::InvalidEndpoint);
        }
        let policies = [
            &self.protocol_policy,
            &self.network_policy,
            &self.tls_policy,
            &self.trust_policy,
            &self.auth_policy,
        ];
        let mut ids = BTreeSet::new();
        for policy in policies {
            policy
                .validate()
                .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
            if policy.resource_kind != ResourceKind::PolicyRevision
                || !ids.insert(policy.revision_id.clone())
            {
                return Err(EgressConfigurationError::InvalidEndpoint);
            }
        }
        mcp_endpoint_url(&self.endpoint)?;
        Ok(())
    }

    fn matches(&self, request: &McpStreamableHttpRequest) -> bool {
        self.deployment == request.deployment
            && self.endpoint == request.endpoint
            && self.endpoint_identity_digest == request.endpoint_identity_digest
            && self.server_identity_digest == request.server_identity_digest
            && self.protocol_policy == request.protocol_policy
            && self.network_policy == request.network_policy
            && self.tls_policy == request.tls_policy
            && self.trust_policy == request.trust_policy
            && request.auth_policy.as_ref() == Some(&self.auth_policy)
            && request.token_secret_binding.purpose == self.token_credential_purpose
    }

    fn matches_resource_refresh(&self, request: &McpResourceRefreshTransportRequest) -> bool {
        self.deployment == request.deployment
            && self.endpoint == request.endpoint
            && self.endpoint_identity_digest == request.endpoint_identity_digest
            && self.server_identity_digest == request.server_identity_digest
            && self.protocol_policy == request.protocol_policy
            && self.network_policy == request.network_policy
            && self.tls_policy == request.tls_policy
            && self.trust_policy == request.trust_policy
            && request.auth_policy.as_ref() == Some(&self.auth_policy)
            && request.token_secret_binding.purpose == self.token_credential_purpose
    }
}

#[derive(Debug, Clone)]
pub struct InstalledMcpStreamableHttpEndpointCatalog {
    entries: BTreeMap<(ResourceId, Sha256Digest), InstalledMcpStreamableHttpEndpoint>,
}

impl InstalledMcpStreamableHttpEndpointCatalog {
    pub fn new(
        entries: Vec<InstalledMcpStreamableHttpEndpoint>,
    ) -> Result<Self, EgressConfigurationError> {
        if entries.is_empty() || entries.len() > MAX_INSTALLED_MCP_HTTP_ENDPOINTS {
            return Err(EgressConfigurationError::InvalidEndpointCatalog);
        }
        let mut catalog = BTreeMap::new();
        for entry in entries {
            entry.validate()?;
            let key = (
                entry.deployment.deployment_id.clone(),
                entry.deployment.deployment_digest.clone(),
            );
            if catalog.insert(key, entry).is_some() {
                return Err(EgressConfigurationError::DuplicateEndpoint);
            }
        }
        Ok(Self { entries: catalog })
    }

    fn resolve(
        &self,
        request: &McpStreamableHttpRequest,
    ) -> Result<InstalledMcpStreamableHttpEndpoint, McpTransportFailure> {
        let key = (
            request.deployment.deployment_id.clone(),
            request.deployment.deployment_digest.clone(),
        );
        self.entries
            .get(&key)
            .filter(|entry| entry.matches(request))
            .cloned()
            .ok_or_else(|| rejected("mcp_egress_endpoint_not_installed"))
    }

    fn resolve_subscription(
        &self,
        request: &McpStreamableHttpSubscriptionRequest,
    ) -> Result<InstalledMcpStreamableHttpEndpoint, McpTransportFailure> {
        let key = (
            request.deployment.deployment_id.clone(),
            request.deployment.deployment_digest.clone(),
        );
        self.entries
            .get(&key)
            .filter(|entry| {
                entry.deployment == request.deployment
                    && entry.endpoint == request.endpoint
                    && entry.endpoint_identity_digest == request.endpoint_identity_digest
                    && entry.server_identity_digest == request.server_identity_digest
                    && entry.protocol_policy == request.protocol_policy
                    && entry.network_policy == request.network_policy
                    && entry.tls_policy == request.tls_policy
                    && entry.trust_policy == request.trust_policy
                    && request.auth_policy.as_ref() == Some(&entry.auth_policy)
                    && request.token_secret_binding.purpose == entry.token_credential_purpose
            })
            .cloned()
            .ok_or_else(|| rejected("mcp_egress_subscription_endpoint_not_installed"))
    }

    fn resolve_resource_refresh(
        &self,
        request: &McpResourceRefreshTransportRequest,
    ) -> Result<InstalledMcpStreamableHttpEndpoint, McpTransportFailure> {
        let key = (
            request.deployment.deployment_id.clone(),
            request.deployment.deployment_digest.clone(),
        );
        self.entries
            .get(&key)
            .filter(|entry| entry.matches_resource_refresh(request))
            .cloned()
            .ok_or_else(|| rejected("mcp_egress_resource_refresh_endpoint_not_installed"))
    }
}

struct SensitiveSessionId(Vec<u8>);

impl SensitiveSessionId {
    fn from_headers(headers: &HeaderMap) -> Result<Option<Self>, McpTransportFailure> {
        let values = headers.get_all(MCP_SESSION_HEADER);
        let mut iter = values.iter();
        let Some(value) = iter.next() else {
            return Ok(None);
        };
        if iter.next().is_some()
            || value.as_bytes().is_empty()
            || value.as_bytes().len() > MAX_MCP_SESSION_ID_BYTES
            || !value.as_bytes().iter().all(|byte| byte.is_ascii_graphic())
        {
            return Err(uncertain("mcp_egress_invalid_session", static_external()));
        }
        Ok(Some(Self(value.as_bytes().to_vec())))
    }

    fn header_value(&self) -> Result<HeaderValue, McpTransportFailure> {
        let mut value =
            HeaderValue::from_bytes(&self.0).map_err(|_| rejected("mcp_egress_invalid_session"))?;
        value.set_sensitive(true);
        Ok(value)
    }

    fn clone_sensitive(&self) -> Vec<u8> {
        self.0.clone()
    }

    fn from_sensitive_bytes(value: &[u8]) -> Result<Self, McpTransportFailure> {
        if value.is_empty()
            || value.len() > MAX_MCP_SESSION_ID_BYTES
            || !value.iter().all(u8::is_ascii_graphic)
        {
            return Err(rejected("mcp_egress_invalid_session"));
        }
        Ok(Self(value.to_vec()))
    }
}

impl Drop for SensitiveSessionId {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

struct PinnedMcpHttpRequest {
    method: Method,
    url: Url,
    dns_host: String,
    addresses: Vec<SocketAddr>,
    headers: HeaderMap,
    body: Vec<u8>,
    timeout: Duration,
    idle_timeout: Duration,
    maximum_response_bytes: usize,
    maximum_sse_event_bytes: usize,
    maximum_progress_events: u32,
    expected_response_id: Option<String>,
    allow_elicitation_request: bool,
    maximum_headers: usize,
    external_identity_digest: Sha256Digest,
    trusted_root_pem: String,
}

struct PinnedMcpHttpResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

struct PinnedMcpSseRequest {
    url: Url,
    dns_host: String,
    addresses: Vec<SocketAddr>,
    headers: HeaderMap,
    connect_timeout: Duration,
    total_timeout: Duration,
    maximum_headers: usize,
    external_identity_digest: Sha256Digest,
    trusted_root_pem: String,
}

struct PinnedMcpSseResponse {
    status: StatusCode,
    headers: HeaderMap,
    chunks: BoxStream<'static, Result<Vec<u8>, McpTransportFailure>>,
}

#[async_trait]
trait PinnedMcpHttpTransport: Send + Sync {
    async fn round_trip(
        &self,
        request: PinnedMcpHttpRequest,
    ) -> Result<PinnedMcpHttpResponse, McpTransportFailure>;

    async fn open_sse(
        &self,
        _request: PinnedMcpSseRequest,
    ) -> Result<PinnedMcpSseResponse, McpTransportFailure> {
        Err(rejected("mcp_egress_subscription_transport_unavailable"))
    }
}

#[derive(Default)]
struct ReqwestPinnedMcpHttpTransport;

#[async_trait]
impl PinnedMcpHttpTransport for ReqwestPinnedMcpHttpTransport {
    async fn round_trip(
        &self,
        request: PinnedMcpHttpRequest,
    ) -> Result<PinnedMcpHttpResponse, McpTransportFailure> {
        let roots = reqwest::Certificate::from_pem_bundle(request.trusted_root_pem.as_bytes())
            .map_err(|_| rejected("mcp_egress_trust_bundle_invalid"))?;
        if roots.is_empty() {
            return Err(rejected("mcp_egress_trust_bundle_invalid"));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .no_proxy()
            .https_only(true)
            .connect_timeout(request.timeout)
            .timeout(request.timeout)
            .pool_max_idle_per_host(0)
            .tls_certs_only(roots)
            .resolve_to_addrs(&request.dns_host, &request.addresses)
            .build()
            .map_err(|_| rejected("mcp_egress_client_build_failed"))?;
        let outbound = client
            .request(request.method, request.url)
            .headers(request.headers)
            .body(request.body)
            .build()
            .map_err(|_| rejected("mcp_egress_request_build_failed"))?;
        let response = client.execute(outbound).await.map_err(|_| {
            uncertain(
                "mcp_egress_transport_failed",
                request.external_identity_digest.clone(),
            )
        })?;
        if response.headers().len() > request.maximum_headers
            || response.headers().get_all(CONTENT_LENGTH).iter().count() > 1
            || response
                .headers()
                .get(CONTENT_ENCODING)
                .is_some_and(|value| value.as_bytes() != b"identity")
            || response
                .content_length()
                .is_some_and(|length| length > request.maximum_response_bytes as u64)
        {
            return Err(uncertain(
                "mcp_egress_invalid_response_metadata",
                request.external_identity_digest,
            ));
        }
        let status = response.status();
        let headers = response.headers().clone();
        let is_sse = content_type_is(&headers, "text/event-stream");
        let mut body = Vec::new();
        let mut received_bytes = 0_usize;
        let mut progress_events = 0_u32;
        let mut control_events = 0_u16;
        let mut stream = response.bytes_stream();
        loop {
            let next = tokio::time::timeout(request.idle_timeout, stream.next()).await;
            let bytes = match next {
                Ok(Some(Ok(bytes))) => bytes,
                Ok(Some(Err(_))) => {
                    return Err(uncertain(
                        "mcp_egress_response_stream_failed",
                        request.external_identity_digest,
                    ))
                }
                Ok(None) => break,
                Err(_) => {
                    return Err(uncertain(
                        "mcp_egress_idle_timeout",
                        request.external_identity_digest,
                    ))
                }
            };
            received_bytes = received_bytes.saturating_add(bytes.len());
            if received_bytes > request.maximum_response_bytes {
                return Err(uncertain(
                    "mcp_egress_response_too_large",
                    request.external_identity_digest,
                ));
            }
            body.extend_from_slice(&bytes);
            if is_sse {
                while let Some(event_end) = first_complete_sse_event_end(&body) {
                    let event = body[..event_end].to_vec();
                    body.drain(..event_end);
                    if event.len() > request.maximum_sse_event_bytes {
                        return Err(uncertain(
                            "mcp_egress_sse_event_too_large",
                            request.external_identity_digest,
                        ));
                    }
                    match classify_sse_event(
                        &event,
                        request.expected_response_id.as_deref(),
                        request.allow_elicitation_request,
                    )? {
                        McpSseEventDisposition::Selected => {
                            body = event;
                            return Ok(PinnedMcpHttpResponse {
                                status,
                                headers,
                                body,
                            });
                        }
                        McpSseEventDisposition::Progress => {
                            progress_events = progress_events.saturating_add(1);
                            if progress_events > request.maximum_progress_events {
                                return Err(uncertain(
                                    "mcp_egress_progress_limit_exceeded",
                                    request.external_identity_digest,
                                ));
                            }
                        }
                        McpSseEventDisposition::Control => {
                            control_events = control_events.saturating_add(1);
                            if control_events > MAX_MCP_SSE_CONTROL_EVENTS {
                                return Err(uncertain(
                                    "mcp_egress_sse_control_limit_exceeded",
                                    request.external_identity_digest,
                                ));
                            }
                        }
                    }
                }
                if body.len() > request.maximum_sse_event_bytes {
                    return Err(uncertain(
                        "mcp_egress_sse_event_too_large",
                        request.external_identity_digest,
                    ));
                }
            }
        }
        if is_sse && request.expected_response_id.is_some() {
            return Err(uncertain(
                "mcp_egress_sse_response_missing",
                request.external_identity_digest,
            ));
        }
        Ok(PinnedMcpHttpResponse {
            status,
            headers,
            body,
        })
    }

    async fn open_sse(
        &self,
        request: PinnedMcpSseRequest,
    ) -> Result<PinnedMcpSseResponse, McpTransportFailure> {
        let roots = reqwest::Certificate::from_pem_bundle(request.trusted_root_pem.as_bytes())
            .map_err(|_| rejected("mcp_egress_subscription_trust_bundle_invalid"))?;
        if roots.is_empty() {
            return Err(rejected("mcp_egress_subscription_trust_bundle_invalid"));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .no_proxy()
            .https_only(true)
            .connect_timeout(request.connect_timeout)
            .timeout(request.total_timeout)
            .pool_max_idle_per_host(0)
            .tls_certs_only(roots)
            .resolve_to_addrs(&request.dns_host, &request.addresses)
            .build()
            .map_err(|_| rejected("mcp_egress_subscription_client_build_failed"))?;
        let outbound = client
            .request(Method::GET, request.url)
            .headers(request.headers)
            .build()
            .map_err(|_| rejected("mcp_egress_subscription_request_build_failed"))?;
        let response = client.execute(outbound).await.map_err(|_| {
            uncertain(
                "mcp_egress_subscription_connect_failed",
                request.external_identity_digest.clone(),
            )
        })?;
        if response.headers().len() > request.maximum_headers
            || response.headers().get_all(CONTENT_LENGTH).iter().count() > 1
            || response
                .headers()
                .get(CONTENT_ENCODING)
                .is_some_and(|value| value.as_bytes() != b"identity")
        {
            return Err(uncertain(
                "mcp_egress_subscription_invalid_response_metadata",
                request.external_identity_digest,
            ));
        }
        let status = response.status();
        let headers = response.headers().clone();
        let external_identity = request.external_identity_digest;
        let chunks = response
            .bytes_stream()
            .map(move |chunk| {
                chunk.map(|bytes| bytes.to_vec()).map_err(|_| {
                    uncertain(
                        "mcp_egress_subscription_stream_failed",
                        external_identity.clone(),
                    )
                })
            })
            .boxed();
        Ok(PinnedMcpSseResponse {
            status,
            headers,
            chunks,
        })
    }
}

/// Production Streamable HTTP connector. Each operation establishes a fresh protocol session,
/// which avoids relying on process memory for correctness while the durable subscription path owns
/// long-lived sessions separately.
pub struct ReqwestMcpStreamableHttpConnector {
    catalog: InstalledMcpStreamableHttpEndpointCatalog,
    secrets: Arc<dyn SecretMaterialResolver>,
    dns: Arc<dyn EgressDnsResolver>,
    transport: Arc<dyn PinnedMcpHttpTransport>,
    remote_tasks: Arc<AeadMcpRemoteTaskStateCodec>,
    limits: McpStreamableHttpEgressLimits,
    permits: Arc<Semaphore>,
    #[cfg(any(test, feature = "protocol-fixtures"))]
    allow_loopback_for_protocol_fixture: bool,
}

impl ReqwestMcpStreamableHttpConnector {
    pub fn new(
        catalog: InstalledMcpStreamableHttpEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        remote_tasks: Arc<AeadMcpRemoteTaskStateCodec>,
        limits: McpStreamableHttpEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        Self::with_transport(
            catalog,
            secrets,
            dns,
            Arc::new(ReqwestPinnedMcpHttpTransport),
            remote_tasks,
            limits,
        )
    }

    fn with_transport(
        catalog: InstalledMcpStreamableHttpEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        transport: Arc<dyn PinnedMcpHttpTransport>,
        remote_tasks: Arc<AeadMcpRemoteTaskStateCodec>,
        limits: McpStreamableHttpEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        limits.validate()?;
        Ok(Self {
            catalog,
            secrets,
            dns,
            transport,
            remote_tasks,
            limits,
            permits: Arc::new(Semaphore::new(limits.maximum_in_flight)),
            #[cfg(any(test, feature = "protocol-fixtures"))]
            allow_loopback_for_protocol_fixture: false,
        })
    }

    /// Enables loopback only in explicit protocol-fixture builds. Production binaries do not
    /// enable this feature and retain the public-destination-only guard.
    #[cfg(any(test, feature = "protocol-fixtures"))]
    pub fn allow_loopback_for_protocol_fixture(mut self) -> Self {
        self.allow_loopback_for_protocol_fixture = true;
        self
    }

    fn destination_allowed(&self, address: &SocketAddr) -> bool {
        if is_public_destination_ip(address.ip()) {
            return true;
        }
        #[cfg(any(test, feature = "protocol-fixtures"))]
        if self.allow_loopback_for_protocol_fixture && address.ip().is_loopback() {
            return true;
        }
        false
    }

    async fn acquire(&self) -> Result<OwnedSemaphorePermit, McpTransportFailure> {
        self.permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| retryable("mcp_egress_capacity"))
    }

    async fn resolve_addresses(
        &self,
        endpoint: &CanonicalHttpEndpoint,
    ) -> Result<(String, Vec<SocketAddr>), McpTransportFailure> {
        let host = parse_endpoint_host(&endpoint.host)
            .map_err(|_| rejected("mcp_egress_invalid_endpoint"))?;
        let (dns_host, mut addresses) = match host {
            ParsedEndpointHost::Address(address) => (
                match address {
                    IpAddr::V4(address) => address.to_string(),
                    IpAddr::V6(address) => address.to_string(),
                },
                vec![SocketAddr::new(address, endpoint.port)],
            ),
            ParsedEndpointHost::Name(host) => {
                let resolved = self
                    .dns
                    .resolve(&host, endpoint.port)
                    .await
                    .map_err(|failure| match failure {
                        DnsResolutionError::Unavailable => retryable("mcp_egress_dns_unavailable"),
                        DnsResolutionError::NoAddresses | DnsResolutionError::TooManyAddresses => {
                            rejected("mcp_egress_dns_rejected")
                        }
                    })?;
                (host, resolved)
            }
        };
        addresses.sort_unstable();
        addresses.dedup();
        if addresses.is_empty()
            || addresses.len() > self.limits.maximum_dns_answers
            || addresses.iter().any(|address| {
                address.port() != endpoint.port || !self.destination_allowed(address)
            })
        {
            return Err(rejected("mcp_egress_destination_denied"));
        }
        Ok((dns_host, addresses))
    }

    async fn resolve_headers(
        &self,
        request: &McpStreamableHttpRequest,
    ) -> Result<HeaderMap, McpTransportFailure> {
        let resolved = self
            .secrets
            .resolve(&request.tenant_id, &request.token_secret_binding)
            .await
            .map_err(|failure| match failure {
                SecretMaterialResolutionError::Unavailable => {
                    retryable("mcp_egress_secret_unavailable")
                }
                SecretMaterialResolutionError::NotFound
                | SecretMaterialResolutionError::Revoked
                | SecretMaterialResolutionError::InvalidEvidence => {
                    rejected("mcp_egress_secret_rejected")
                }
            })?;
        if !resolved.validate_for(
            &request.token_secret_binding,
            self.limits.maximum_secret_material_bytes,
        ) {
            return Err(rejected("mcp_egress_secret_rejected"));
        }
        let mut bearer = Vec::with_capacity(7 + resolved.material.as_bytes().len());
        bearer.extend_from_slice(b"Bearer ");
        bearer.extend_from_slice(resolved.material.as_bytes());
        let parsed = HeaderValue::from_bytes(&bearer);
        bearer.fill(0);
        let mut authorization = parsed.map_err(|_| rejected("mcp_egress_secret_rejected"))?;
        authorization.set_sensitive(true);
        let protocol_version = HeaderValue::from_str(&request.protocol_version)
            .map_err(|_| rejected("mcp_egress_invalid_protocol_version"))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        headers.insert(AUTHORIZATION, authorization);
        headers.insert(MCP_PROTOCOL_VERSION_HEADER, protocol_version);
        Ok(headers)
    }

    async fn resolve_resource_refresh_headers(
        &self,
        request: &McpResourceRefreshTransportRequest,
    ) -> Result<HeaderMap, McpTransportFailure> {
        let resolved = self
            .secrets
            .resolve(&request.tenant_id, &request.token_secret_binding)
            .await
            .map_err(|failure| match failure {
                SecretMaterialResolutionError::Unavailable => {
                    retryable("mcp_egress_secret_unavailable")
                }
                SecretMaterialResolutionError::NotFound
                | SecretMaterialResolutionError::Revoked
                | SecretMaterialResolutionError::InvalidEvidence => {
                    rejected("mcp_egress_secret_rejected")
                }
            })?;
        if !resolved.validate_for(
            &request.token_secret_binding,
            self.limits.maximum_secret_material_bytes,
        ) {
            return Err(rejected("mcp_egress_secret_rejected"));
        }
        let mut bearer = Vec::with_capacity(7 + resolved.material.as_bytes().len());
        bearer.extend_from_slice(b"Bearer ");
        bearer.extend_from_slice(resolved.material.as_bytes());
        let parsed = HeaderValue::from_bytes(&bearer);
        bearer.fill(0);
        let mut authorization = parsed.map_err(|_| rejected("mcp_egress_secret_rejected"))?;
        authorization.set_sensitive(true);
        let protocol_version = HeaderValue::from_str(&request.protocol_version)
            .map_err(|_| rejected("mcp_egress_invalid_protocol_version"))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        headers.insert(AUTHORIZATION, authorization);
        headers.insert(MCP_PROTOCOL_VERSION_HEADER, protocol_version);
        Ok(headers)
    }

    #[allow(clippy::too_many_arguments)]
    async fn post(
        &self,
        entry: &InstalledMcpStreamableHttpEndpoint,
        dns_host: &str,
        addresses: &[SocketAddr],
        base_headers: &HeaderMap,
        session: Option<&SensitiveSessionId>,
        body: Vec<u8>,
        timeout: Duration,
        idle_timeout: Duration,
        maximum_response_bytes: usize,
        maximum_sse_event_bytes: usize,
        maximum_headers: usize,
        expected_response_id: Option<&str>,
        allow_elicitation_request: bool,
        maximum_progress_events: u32,
        external_identity_digest: Sha256Digest,
    ) -> Result<PinnedMcpHttpResponse, McpTransportFailure> {
        let mut headers = base_headers.clone();
        if let Some(session) = session {
            headers.insert(MCP_SESSION_HEADER, session.header_value()?);
        }
        self.transport
            .round_trip(PinnedMcpHttpRequest {
                method: Method::POST,
                url: mcp_endpoint_url(&entry.endpoint)
                    .map_err(|_| rejected("mcp_egress_invalid_endpoint"))?,
                dns_host: dns_host.to_owned(),
                addresses: addresses.to_vec(),
                headers,
                body,
                timeout,
                idle_timeout,
                maximum_response_bytes,
                maximum_sse_event_bytes,
                maximum_headers,
                maximum_progress_events,
                expected_response_id: expected_response_id.map(str::to_owned),
                allow_elicitation_request,
                external_identity_digest,
                trusted_root_pem: entry.trusted_root_pem.clone(),
            })
            .await
    }

    fn decode_initial_operation_response(
        &self,
        request: &McpStreamableHttpRequest,
        wire: Value,
        id: &str,
        session: Option<&SensitiveSessionId>,
        external_identity_digest: Sha256Digest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        let result = matching_result(&wire, id).ok_or_else(|| {
            if is_matching_jsonrpc_error(&wire, id) {
                McpTransportFailure::Permanent(safe_failure("mcp_egress_jsonrpc_error"))
            } else {
                uncertain(
                    "mcp_egress_invalid_jsonrpc_response",
                    external_identity_digest.clone(),
                )
            }
        })?;
        if !request.task_requested {
            return completed_operation(request, result, external_identity_digest);
        }
        let task = result
            .as_object()
            .and_then(|object| object.get("task"))
            .ok_or_else(|| {
                uncertain(
                    "mcp_egress_task_result_missing",
                    external_identity_digest.clone(),
                )
            })?;
        let task_state = parse_task_state(task)?;
        if task_state.status != "working" {
            return Err(uncertain(
                "mcp_egress_initial_task_not_working",
                external_identity_digest,
            ));
        }
        self.remote_wait(
            request,
            task_state,
            session.map(SensitiveSessionId::clone_sensitive),
            external_identity_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn poll_remote_task(
        &self,
        entry: &InstalledMcpStreamableHttpEndpoint,
        dns_host: &str,
        addresses: &[SocketAddr],
        headers: &HeaderMap,
        request: &McpStreamableHttpRequest,
        external_identity_digest: Sha256Digest,
        request_timeout: Duration,
        idle_timeout: Duration,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        let continuation = request
            .continuation
            .as_ref()
            .ok_or_else(|| rejected("mcp_egress_remote_task_state_missing"))?;
        let claims = self.remote_tasks.open(&continuation.encrypted_state)?;
        claims.validate_for(request)?;
        if let Some(response) = continuation.elicitation_response.as_ref() {
            return self
                .resume_remote_elicitation(
                    entry,
                    dns_host,
                    addresses,
                    headers,
                    request,
                    claims,
                    response,
                    request_timeout,
                    idle_timeout,
                )
                .await;
        }
        if claims.pending_elicitation.is_some() {
            return Err(rejected("mcp_egress_elicitation_response_missing"));
        }
        let task_limits = request
            .task_limits
            .ok_or_else(|| rejected("mcp_egress_task_limits_missing"))?;
        let operation_id = format!("{}:poll:{}", request.operation_id, continuation.poll_count);
        let body = canonical_json(&serde_json::json!({
            "id": operation_id,
            "jsonrpc": "2.0",
            "method": "tasks/get",
            "params": {"taskId": claims.task_id()?},
        }))
        .map_err(|_| rejected("mcp_egress_request_not_canonical"))?;
        if body.len() > task_limits.maximum_get_request_bytes as usize {
            return Err(rejected("mcp_egress_task_poll_request_too_large"));
        }
        let session = claims
            .session_id
            .as_deref()
            .map(SensitiveSessionId::from_sensitive_bytes)
            .transpose()?;
        let response = self
            .post(
                entry,
                dns_host,
                addresses,
                headers,
                session.as_ref(),
                body,
                request_timeout,
                idle_timeout,
                task_limits.maximum_get_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                Some(&operation_id),
                false,
                task_limits.maximum_get_progress_events,
                external_identity_digest.clone(),
            )
            .await?;
        if response.status == StatusCode::UNAUTHORIZED || response.status == StatusCode::FORBIDDEN {
            return Err(reauthorization(&response.headers));
        }
        let wire = response_json(
            &response,
            task_limits.maximum_get_response_bytes,
            &external_identity_digest,
        )?;
        self.decode_task_poll_response(
            entry,
            dns_host,
            addresses,
            headers,
            request,
            wire,
            &operation_id,
            external_identity_digest,
            request_timeout,
            idle_timeout,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn decode_task_poll_response(
        &self,
        entry: &InstalledMcpStreamableHttpEndpoint,
        dns_host: &str,
        addresses: &[SocketAddr],
        headers: &HeaderMap,
        request: &McpStreamableHttpRequest,
        wire: Value,
        id: &str,
        external_identity_digest: Sha256Digest,
        request_timeout: Duration,
        idle_timeout: Duration,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        let continuation = request
            .continuation
            .as_ref()
            .ok_or_else(|| rejected("mcp_egress_remote_task_state_missing"))?;
        let claims = self.remote_tasks.open(&continuation.encrypted_state)?;
        claims.validate_for(request)?;
        let task = matching_result(&wire, id).ok_or_else(|| {
            if is_matching_jsonrpc_error(&wire, id) {
                McpTransportFailure::Permanent(safe_failure("mcp_egress_task_poll_error"))
            } else {
                uncertain(
                    "mcp_egress_invalid_task_poll_response",
                    external_identity_digest.clone(),
                )
            }
        })?;
        let task_state = parse_task_state(task)?;
        if task_state.task_id.as_bytes() != claims.task_id {
            return Err(uncertain(
                "mcp_egress_remote_task_identity_changed",
                external_identity_digest,
            ));
        }
        match task_state.status.as_str() {
            "working" => self.remote_wait(
                request,
                task_state,
                claims.session_id.clone(),
                external_identity_digest,
            ),
            "completed" | "failed" | "cancelled" => {
                let task_limits = request
                    .task_limits
                    .ok_or_else(|| rejected("mcp_egress_task_limits_missing"))?;
                let result_id = format!(
                    "{}:result:{}",
                    request.operation_id, continuation.poll_count
                );
                let result_body = canonical_json(&serde_json::json!({
                    "id": result_id,
                    "jsonrpc": "2.0",
                    "method": "tasks/result",
                    "params": {"taskId": claims.task_id()?},
                }))
                .map_err(|_| rejected("mcp_egress_request_not_canonical"))?;
                if result_body.len() > task_limits.maximum_result_request_bytes as usize {
                    return Err(rejected("mcp_egress_task_result_request_too_large"));
                }
                let session = claims
                    .session_id
                    .as_deref()
                    .map(SensitiveSessionId::from_sensitive_bytes)
                    .transpose()?;
                let response = self
                    .post(
                        entry,
                        dns_host,
                        addresses,
                        headers,
                        session.as_ref(),
                        result_body,
                        request_timeout,
                        idle_timeout,
                        task_limits.maximum_result_response_bytes as usize,
                        request.maximum_sse_event_bytes as usize,
                        request.maximum_headers as usize,
                        Some(&result_id),
                        false,
                        task_limits.maximum_result_progress_events,
                        external_identity_digest.clone(),
                    )
                    .await?;
                if response.status == StatusCode::UNAUTHORIZED
                    || response.status == StatusCode::FORBIDDEN
                {
                    return Err(reauthorization(&response.headers));
                }
                let wire = response_json(
                    &response,
                    task_limits.maximum_result_response_bytes,
                    &external_identity_digest,
                )?;
                let result = matching_result(&wire, &result_id).ok_or_else(|| {
                    if is_matching_jsonrpc_error(&wire, &result_id) {
                        McpTransportFailure::Permanent(safe_failure("mcp_egress_task_result_error"))
                    } else {
                        uncertain(
                            "mcp_egress_invalid_task_result_response",
                            external_identity_digest.clone(),
                        )
                    }
                })?;
                validate_related_task(result, claims.task_id()?)?;
                completed_operation(request, result, external_identity_digest)
            }
            "input_required" => {
                let task_limits = request
                    .task_limits
                    .ok_or_else(|| rejected("mcp_egress_task_limits_missing"))?;
                let result_id = format!(
                    "{}:result:{}",
                    request.operation_id, continuation.poll_count
                );
                let result_body = canonical_json(&serde_json::json!({
                    "id": result_id,
                    "jsonrpc": "2.0",
                    "method": "tasks/result",
                    "params": {"taskId": claims.task_id()?},
                }))
                .map_err(|_| rejected("mcp_egress_request_not_canonical"))?;
                if result_body.len() > task_limits.maximum_result_request_bytes as usize {
                    return Err(rejected("mcp_egress_task_result_request_too_large"));
                }
                let session = claims
                    .session_id
                    .as_deref()
                    .map(SensitiveSessionId::from_sensitive_bytes)
                    .transpose()?;
                let response = self
                    .post(
                        entry,
                        dns_host,
                        addresses,
                        headers,
                        session.as_ref(),
                        result_body,
                        request_timeout,
                        idle_timeout,
                        task_limits.maximum_result_response_bytes as usize,
                        request.maximum_sse_event_bytes as usize,
                        request.maximum_headers as usize,
                        Some(&result_id),
                        true,
                        task_limits.maximum_result_progress_events,
                        external_identity_digest.clone(),
                    )
                    .await?;
                if response.status == StatusCode::UNAUTHORIZED
                    || response.status == StatusCode::FORBIDDEN
                {
                    return Err(reauthorization(&response.headers));
                }
                if !response.status.is_success() {
                    return Err(uncertain(
                        "mcp_egress_task_result_http_failure",
                        external_identity_digest,
                    ));
                }
                let wire = response_json(
                    &response,
                    task_limits.maximum_result_response_bytes,
                    &external_identity_digest,
                )?;
                if let Some(result) = matching_result(&wire, &result_id) {
                    validate_related_task(result, claims.task_id()?)?;
                    return completed_operation(request, result, external_identity_digest);
                }
                if is_matching_jsonrpc_error(&wire, &result_id) {
                    return Err(McpTransportFailure::Permanent(safe_failure(
                        "mcp_egress_task_result_error",
                    )));
                }
                let pending = parse_task_elicitation(&wire, claims.task_id()?)?;
                let response_schema = pending.response_schema.clone();
                let response_schema_digest = response_schema.canonical_digest.clone();
                let mut claims = claims;
                claims.pending_elicitation = Some(pending);
                Ok(McpOperationOutcome::InputRequired {
                    encrypted_state: self.remote_tasks.seal(&claims)?,
                    external_identity_digest: claims.external_identity_digest.clone(),
                    safe_prompt_key: "mcp_task_input_required".to_owned(),
                    response_schema,
                    response_schema_digest,
                    deadline: request.deadline,
                })
            }
            _ => Err(uncertain(
                "mcp_egress_task_status_invalid",
                external_identity_digest,
            )),
        }
    }

    fn remote_wait(
        &self,
        request: &McpStreamableHttpRequest,
        task: ParsedMcpTask,
        session_id: Option<Vec<u8>>,
        external_identity_digest: Sha256Digest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        let task_limits = request
            .task_limits
            .ok_or_else(|| rejected("mcp_egress_task_limits_missing"))?;
        let now = Utc::now();
        let poll_milliseconds = task
            .poll_interval_milliseconds
            .unwrap_or(task_limits.minimum_poll_milliseconds)
            .clamp(
                task_limits.minimum_poll_milliseconds,
                task_limits.maximum_poll_milliseconds,
            );
        let next_poll_at = now
            .checked_add_signed(chrono::Duration::milliseconds(
                i64::try_from(poll_milliseconds)
                    .map_err(|_| rejected("mcp_egress_task_poll_interval_invalid"))?,
            ))
            .ok_or_else(|| rejected("mcp_egress_task_poll_interval_invalid"))?;
        if next_poll_at >= request.deadline {
            return Err(uncertain(
                "mcp_egress_task_deadline_exhausted",
                external_identity_digest,
            ));
        }
        let remote_task_identity_digest = remote_task_identity(request, &task.task_id)?;
        let claims = McpRemoteTaskStateClaims {
            schema_version: 1,
            tenant_id: request.tenant_id.clone(),
            deployment: request.deployment.clone(),
            authorization_binding_id: request.authorization_binding_id.clone(),
            authorization_generation: request.authorization_generation,
            principal_binding_generation: request.principal_binding_generation,
            invocation_id: request.invocation_id.clone(),
            job_id: request.job_id.clone(),
            physical_attempt: request.physical_attempt,
            discovery_snapshot_id: request.discovery_snapshot_id.clone(),
            discovery_snapshot_digest: request.discovery_snapshot_digest.clone(),
            protocol_policy: request.protocol_policy.clone(),
            operation_id: request.operation_id.clone(),
            task_id: task.task_id.into_bytes(),
            session_id,
            pending_elicitation: None,
            external_identity_digest: remote_task_identity_digest.clone(),
            deadline_epoch_milliseconds: request.deadline.timestamp_millis(),
        };
        Ok(McpOperationOutcome::RemoteTask {
            encrypted_state: self.remote_tasks.seal(&claims)?,
            external_identity_digest: remote_task_identity_digest,
            next_poll_at,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn resume_remote_elicitation(
        &self,
        entry: &InstalledMcpStreamableHttpEndpoint,
        dns_host: &str,
        addresses: &[SocketAddr],
        headers: &HeaderMap,
        request: &McpStreamableHttpRequest,
        mut claims: McpRemoteTaskStateClaims,
        response: &McpElicitationResponse,
        request_timeout: Duration,
        idle_timeout: Duration,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        let pending = claims
            .pending_elicitation
            .as_ref()
            .ok_or_else(|| rejected("mcp_egress_elicitation_request_missing"))?;
        validate_elicitation_response(response, pending)?;
        let action = match response.action {
            McpElicitationAction::Accept => "accept",
            McpElicitationAction::Decline => "decline",
            McpElicitationAction::Cancel => "cancel",
        };
        let mut result = Map::new();
        result.insert("action".to_owned(), Value::String(action.to_owned()));
        if let Some(content) = response.content.as_ref() {
            result.insert("content".to_owned(), content.value.clone());
        }
        let body = canonical_json(&serde_json::json!({
            "id": pending.request_id.value(),
            "jsonrpc": "2.0",
            "result": result,
        }))
        .map_err(|_| rejected("mcp_egress_request_not_canonical"))?;
        if body.len() > request.maximum_request_bytes as usize {
            return Err(rejected("mcp_egress_elicitation_response_too_large"));
        }
        let session = claims
            .session_id
            .as_deref()
            .map(SensitiveSessionId::from_sensitive_bytes)
            .transpose()?;
        let acknowledged = self
            .post(
                entry,
                dns_host,
                addresses,
                headers,
                session.as_ref(),
                body,
                request_timeout,
                idle_timeout,
                request.maximum_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                None,
                false,
                0,
                claims.external_identity_digest.clone(),
            )
            .await?;
        if acknowledged.status == StatusCode::UNAUTHORIZED
            || acknowledged.status == StatusCode::FORBIDDEN
        {
            return Err(reauthorization(&acknowledged.headers));
        }
        if acknowledged.status != StatusCode::ACCEPTED || !acknowledged.body.is_empty() {
            return Err(uncertain(
                "mcp_egress_elicitation_response_not_accepted",
                claims.external_identity_digest.clone(),
            ));
        }
        claims.pending_elicitation = None;
        self.remote_wait_from_claims(request, claims)
    }

    fn remote_wait_from_claims(
        &self,
        request: &McpStreamableHttpRequest,
        claims: McpRemoteTaskStateClaims,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        let task_limits = request
            .task_limits
            .ok_or_else(|| rejected("mcp_egress_task_limits_missing"))?;
        let next_poll_at = Utc::now()
            .checked_add_signed(chrono::Duration::milliseconds(
                i64::try_from(task_limits.minimum_poll_milliseconds)
                    .map_err(|_| rejected("mcp_egress_task_poll_interval_invalid"))?,
            ))
            .ok_or_else(|| rejected("mcp_egress_task_poll_interval_invalid"))?;
        if next_poll_at >= request.deadline {
            return Err(uncertain(
                "mcp_egress_task_deadline_exhausted",
                claims.external_identity_digest.clone(),
            ));
        }
        Ok(McpOperationOutcome::RemoteTask {
            encrypted_state: self.remote_tasks.seal(&claims)?,
            external_identity_digest: claims.external_identity_digest.clone(),
            next_poll_at,
        })
    }
}

#[async_trait]
impl McpStreamableHttpConnector for ReqwestMcpStreamableHttpConnector {
    async fn execute(
        &self,
        request: McpStreamableHttpRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        validate_request(&request, Utc::now())?;
        let entry = self.catalog.resolve(&request)?;
        let _permit = self.acquire().await?;
        let (dns_host, addresses) = self.resolve_addresses(&entry.endpoint).await?;
        let headers = self.resolve_headers(&request).await?;
        let external_identity = external_identity(&request, &addresses)?;
        let now = Utc::now();
        let remaining = (request.transport_deadline - now)
            .to_std()
            .map_err(|_| rejected("mcp_egress_deadline_elapsed"))?;
        let initialize_timeout = remaining.min(Duration::from_millis(
            request.initialize_timeout_milliseconds,
        ));
        let request_timeout =
            remaining.min(Duration::from_millis(request.request_timeout_milliseconds));
        let idle_timeout = remaining.min(Duration::from_millis(request.idle_timeout_milliseconds));
        if initialize_timeout.is_zero() || request_timeout.is_zero() || idle_timeout.is_zero() {
            return Err(rejected("mcp_egress_deadline_elapsed"));
        }
        if request.continuation.is_some() {
            return self
                .poll_remote_task(
                    &entry,
                    &dns_host,
                    &addresses,
                    &headers,
                    &request,
                    external_identity,
                    request_timeout,
                    idle_timeout,
                )
                .await;
        }

        let initialize_id = format!("{}:initialize", request.operation_id);
        let initialize_body = canonical_json(&initialize_request(&request, &initialize_id))
            .map_err(|_| rejected("mcp_egress_request_not_canonical"))?;
        if initialize_body.len() > request.maximum_request_bytes as usize {
            return Err(rejected("mcp_egress_initialize_request_too_large"));
        }
        let initialize = self
            .post(
                &entry,
                &dns_host,
                &addresses,
                &headers,
                None,
                initialize_body,
                initialize_timeout,
                idle_timeout,
                request.maximum_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                Some(&initialize_id),
                false,
                0,
                external_identity.clone(),
            )
            .await?;
        if initialize.status == StatusCode::UNAUTHORIZED
            || initialize.status == StatusCode::FORBIDDEN
        {
            return Err(reauthorization(&initialize.headers));
        }
        require_success_status(initialize.status, &external_identity)?;
        let initialize_wire = response_json(
            &initialize,
            request.maximum_response_bytes,
            &external_identity,
        )?;
        validate_initialize_response(
            &initialize_wire,
            &initialize_id,
            &request.protocol_version,
            &request.negotiated_capabilities,
        )?;
        let session = SensitiveSessionId::from_headers(&initialize.headers)?;

        let initialized = canonical_json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }))
        .map_err(|_| rejected("mcp_egress_request_not_canonical"))?;
        if initialized.len() > request.maximum_request_bytes as usize {
            return Err(rejected("mcp_egress_initialized_request_too_large"));
        }
        let acknowledged = self
            .post(
                &entry,
                &dns_host,
                &addresses,
                &headers,
                session.as_ref(),
                initialized,
                initialize_timeout,
                idle_timeout,
                request.maximum_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                None,
                false,
                0,
                external_identity.clone(),
            )
            .await?;
        if acknowledged.status != StatusCode::ACCEPTED || !acknowledged.body.is_empty() {
            return Err(uncertain(
                "mcp_egress_initialized_not_accepted",
                external_identity,
            ));
        }

        let operation_id = request.operation_id.to_string();
        let mut operation_params = request.params.value.clone();
        if request.task_requested {
            let params = operation_params
                .as_object_mut()
                .ok_or_else(|| rejected("mcp_egress_invalid_task_request"))?;
            if params.contains_key("task") {
                return Err(rejected("mcp_egress_task_parameter_reserved"));
            }
            params.insert(
                "task".to_owned(),
                serde_json::json!({"ttl": task_ttl_milliseconds(&request)?}),
            );
        }
        let operation_body = canonical_json(&serde_json::json!({
            "id": operation_id,
            "jsonrpc": "2.0",
            "method": wire_method(request.method),
            "params": operation_params,
        }))
        .map_err(|_| rejected("mcp_egress_request_not_canonical"))?;
        if operation_body.len() > request.maximum_request_bytes as usize {
            return Err(rejected("mcp_egress_request_too_large"));
        }
        let operation = self
            .post(
                &entry,
                &dns_host,
                &addresses,
                &headers,
                session.as_ref(),
                operation_body,
                request_timeout,
                idle_timeout,
                request.maximum_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                Some(&operation_id),
                false,
                request.maximum_progress_events,
                external_identity.clone(),
            )
            .await?;
        if operation.status == StatusCode::UNAUTHORIZED || operation.status == StatusCode::FORBIDDEN
        {
            return Err(reauthorization(&operation.headers));
        }
        let wire = response_json(
            &operation,
            request.maximum_response_bytes,
            &external_identity,
        )?;
        if !operation.status.is_success() && !is_matching_jsonrpc_error(&wire, &operation_id) {
            return Err(uncertain("mcp_egress_http_failure", external_identity));
        }
        self.decode_initial_operation_response(
            &request,
            wire,
            &operation_id,
            session.as_ref(),
            external_identity,
        )
    }

    async fn cancel_remote_task(
        &self,
        request: McpStreamableHttpRequest,
    ) -> Result<McpRemoteTaskCancelOutcome, McpTransportFailure> {
        validate_request(&request, Utc::now())?;
        if !request.negotiated_capabilities.tasks_cancel || request.continuation.is_none() {
            return Err(rejected("mcp_egress_task_cancel_not_negotiated"));
        }
        let entry = self.catalog.resolve(&request)?;
        let _permit = self.acquire().await?;
        let (dns_host, addresses) = self.resolve_addresses(&entry.endpoint).await?;
        let headers = self.resolve_headers(&request).await?;
        let transport_identity = external_identity(&request, &addresses)?;
        let continuation = request
            .continuation
            .as_ref()
            .ok_or_else(|| rejected("mcp_egress_remote_task_state_missing"))?;
        let claims = self.remote_tasks.open(&continuation.encrypted_state)?;
        claims.validate_for(&request)?;
        let task_limits = request
            .task_limits
            .ok_or_else(|| rejected("mcp_egress_task_limits_missing"))?;
        let maximum_request_bytes = task_limits
            .maximum_cancel_request_bytes
            .ok_or_else(|| rejected("mcp_egress_task_cancel_limits_missing"))?;
        let maximum_response_bytes = task_limits
            .maximum_cancel_response_bytes
            .ok_or_else(|| rejected("mcp_egress_task_cancel_limits_missing"))?;
        let now = Utc::now();
        let remaining = (request.transport_deadline - now)
            .to_std()
            .map_err(|_| rejected("mcp_egress_deadline_elapsed"))?;
        let request_timeout =
            remaining.min(Duration::from_millis(request.request_timeout_milliseconds));
        let idle_timeout = remaining.min(Duration::from_millis(request.idle_timeout_milliseconds));
        if request_timeout.is_zero() || idle_timeout.is_zero() {
            return Err(rejected("mcp_egress_deadline_elapsed"));
        }
        let operation_id = format!(
            "{}:cancel:{}",
            request.operation_id, continuation.poll_count
        );
        let body = canonical_json(&serde_json::json!({
            "id": operation_id,
            "jsonrpc": "2.0",
            "method": "tasks/cancel",
            "params": {"taskId": claims.task_id()?},
        }))
        .map_err(|_| rejected("mcp_egress_request_not_canonical"))?;
        if body.len() > maximum_request_bytes as usize {
            return Err(rejected("mcp_egress_task_cancel_request_too_large"));
        }
        let session = claims
            .session_id
            .as_deref()
            .map(SensitiveSessionId::from_sensitive_bytes)
            .transpose()?;
        let response = self
            .post(
                &entry,
                &dns_host,
                &addresses,
                &headers,
                session.as_ref(),
                body,
                request_timeout,
                idle_timeout,
                maximum_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                Some(&operation_id),
                false,
                task_limits.maximum_cancel_progress_events.unwrap_or(0),
                transport_identity.clone(),
            )
            .await?;
        if response.status == StatusCode::UNAUTHORIZED || response.status == StatusCode::FORBIDDEN {
            return Err(reauthorization(&response.headers));
        }
        let wire = response_json(&response, maximum_response_bytes, &transport_identity)?;
        if !response.status.is_success() && !is_matching_jsonrpc_error(&wire, &operation_id) {
            return Err(uncertain(
                "mcp_egress_task_cancel_http_failure",
                claims.external_identity_digest.clone(),
            ));
        }
        let task = matching_result(&wire, &operation_id).ok_or_else(|| {
            if is_matching_jsonrpc_error(&wire, &operation_id) {
                McpTransportFailure::Permanent(safe_failure("mcp_egress_task_cancel_error"))
            } else {
                uncertain(
                    "mcp_egress_invalid_task_cancel_response",
                    claims.external_identity_digest.clone(),
                )
            }
        })?;
        let task_state = parse_task_state(task)?;
        if task_state.task_id.as_bytes() != claims.task_id || task_state.status != "cancelled" {
            return Err(uncertain(
                "mcp_egress_task_cancel_not_confirmed",
                claims.external_identity_digest.clone(),
            ));
        }
        Ok(McpRemoteTaskCancelOutcome::Accepted)
    }
}

#[async_trait]
impl McpResourceRefreshConnector for ReqwestMcpStreamableHttpConnector {
    async fn refresh_resources(
        &self,
        request: McpResourceRefreshTransportRequest,
    ) -> Result<McpResourceRefreshTransportEvidence, McpTransportFailure> {
        validate_resource_refresh_request(&request, Utc::now())?;
        let entry = self.catalog.resolve_resource_refresh(&request)?;
        let _permit = self.acquire().await?;
        let (dns_host, addresses) = self.resolve_addresses(&entry.endpoint).await?;
        let headers = self.resolve_resource_refresh_headers(&request).await?;
        let external_identity = resource_refresh_external_identity(&request, &addresses)?;
        let remaining = (request.deadline - Utc::now())
            .to_std()
            .map_err(|_| rejected("mcp_egress_resource_refresh_deadline_elapsed"))?;
        let initialize_timeout = remaining.min(Duration::from_millis(
            request.initialize_timeout_milliseconds,
        ));
        let request_timeout =
            remaining.min(Duration::from_millis(request.request_timeout_milliseconds));
        let idle_timeout = remaining.min(Duration::from_millis(request.idle_timeout_milliseconds));
        if initialize_timeout.is_zero() || request_timeout.is_zero() || idle_timeout.is_zero() {
            return Err(rejected("mcp_egress_resource_refresh_deadline_elapsed"));
        }

        let initialize_id = format!(
            "{}:{}:refresh:initialize",
            request.job_id, request.attempt_number
        );
        let initialize_body = canonical_json(&resource_refresh_initialize_request(
            &request,
            &initialize_id,
        ))
        .map_err(|_| rejected("mcp_egress_resource_refresh_request_not_canonical"))?;
        if initialize_body.len() > request.read_limits.maximum_request_bytes as usize {
            return Err(rejected(
                "mcp_egress_resource_refresh_initialize_request_too_large",
            ));
        }
        let initialize = self
            .post(
                &entry,
                &dns_host,
                &addresses,
                &headers,
                None,
                initialize_body,
                initialize_timeout,
                idle_timeout,
                request.read_limits.maximum_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                Some(&initialize_id),
                false,
                0,
                external_identity.clone(),
            )
            .await?;
        if initialize.status == StatusCode::UNAUTHORIZED
            || initialize.status == StatusCode::FORBIDDEN
        {
            return Err(reauthorization(&initialize.headers));
        }
        require_success_status(initialize.status, &external_identity)?;
        let initialize_wire = response_json(
            &initialize,
            request.read_limits.maximum_response_bytes,
            &external_identity,
        )?;
        validate_initialize_response(
            &initialize_wire,
            &initialize_id,
            &request.protocol_version,
            &request.negotiated_capabilities,
        )?;
        let session = SensitiveSessionId::from_headers(&initialize.headers)?;

        let initialized = canonical_json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }))
        .map_err(|_| rejected("mcp_egress_resource_refresh_request_not_canonical"))?;
        let initialize_request_limit = request
            .list_limits
            .map_or(request.read_limits.maximum_request_bytes, |limits| {
                limits
                    .maximum_request_bytes
                    .max(request.read_limits.maximum_request_bytes)
            });
        if initialized.len() > initialize_request_limit as usize {
            return Err(rejected(
                "mcp_egress_resource_refresh_initialized_request_too_large",
            ));
        }
        let acknowledged = self
            .post(
                &entry,
                &dns_host,
                &addresses,
                &headers,
                session.as_ref(),
                initialized,
                initialize_timeout,
                idle_timeout,
                request.read_limits.maximum_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                None,
                false,
                0,
                external_identity.clone(),
            )
            .await?;
        if acknowledged.status != StatusCode::ACCEPTED || !acknowledged.body.is_empty() {
            return Err(uncertain(
                "mcp_egress_resource_refresh_initialized_not_accepted",
                external_identity,
            ));
        }

        let mut response_digests = Vec::new();
        let mut byte_count = 0_u64;
        let mut listed_resource_count = 0_u32;
        let mut root_present = true;
        if let Some(list_limits) = request.list_limits {
            root_present = false;
            let mut cursor: Option<String> = None;
            for page in 0..list_limits.maximum_pages {
                let operation_id = format!(
                    "{}:{}:refresh:list:{}",
                    request.job_id,
                    request.attempt_number,
                    u32::from(page) + 1
                );
                let params = cursor
                    .as_ref()
                    .map_or_else(|| serde_json::json!({}), |cursor| serde_json::json!({"cursor": cursor}));
                let result = self
                    .resource_refresh_call(
                        &entry,
                        &dns_host,
                        &addresses,
                        &headers,
                        session.as_ref(),
                        &operation_id,
                        "resources/list",
                        params,
                        list_limits.maximum_request_bytes,
                        list_limits.maximum_response_bytes,
                        request_timeout,
                        idle_timeout,
                        request.maximum_sse_event_bytes,
                        request.maximum_headers,
                        external_identity.clone(),
                    )
                    .await?;
                let encoded = canonical_json(&result)
                    .map_err(|_| uncertain("mcp_egress_resource_list_invalid", external_identity.clone()))?;
                byte_count = byte_count.saturating_add(encoded.len() as u64);
                if byte_count > request.maximum_total_bytes {
                    return Err(uncertain(
                        "mcp_egress_resource_refresh_bytes_exceeded",
                        external_identity,
                    ));
                }
                response_digests.push(digest_value(&result)?);
                let object = result.as_object().ok_or_else(|| {
                    uncertain("mcp_egress_resource_list_invalid", external_identity.clone())
                })?;
                let resources = object
                    .get("resources")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        uncertain("mcp_egress_resource_list_invalid", external_identity.clone())
                    })?;
                listed_resource_count = listed_resource_count.saturating_add(
                    u32::try_from(resources.len()).unwrap_or(u32::MAX),
                );
                if listed_resource_count > request.maximum_resources {
                    return Err(uncertain(
                        "mcp_egress_resource_list_limit_exceeded",
                        external_identity,
                    ));
                }
                for resource in resources {
                    let uri = resource
                        .as_object()
                        .and_then(|resource| resource.get("uri"))
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            uncertain(
                                "mcp_egress_resource_list_invalid",
                                external_identity.clone(),
                            )
                        })?;
                    if uri == request.resource_uri {
                        root_present = true;
                    }
                }
                cursor = object
                    .get("nextCursor")
                    .map(|value| {
                        value
                            .as_str()
                            .filter(|cursor| {
                                !cursor.is_empty()
                                    && cursor.len() <= 2_048
                                    && !cursor.chars().any(char::is_control)
                            })
                            .map(str::to_owned)
                            .ok_or_else(|| {
                                uncertain(
                                    "mcp_egress_resource_list_cursor_invalid",
                                    external_identity.clone(),
                                )
                            })
                    })
                    .transpose()?;
                if cursor.is_none() {
                    break;
                }
                if page + 1 == list_limits.maximum_pages {
                    return Err(uncertain(
                        "mcp_egress_resource_list_page_limit_exceeded",
                        external_identity,
                    ));
                }
            }
        }

        let mut item_count = 0_u32;
        if root_present {
            let operation_id = format!(
                "{}:{}:refresh:read",
                request.job_id, request.attempt_number
            );
            let result = self
                .resource_refresh_call(
                    &entry,
                    &dns_host,
                    &addresses,
                    &headers,
                    session.as_ref(),
                    &operation_id,
                    "resources/read",
                    serde_json::json!({"uri": request.resource_uri}),
                    request.read_limits.maximum_request_bytes,
                    request.read_limits.maximum_response_bytes,
                    request_timeout,
                    idle_timeout,
                    request.maximum_sse_event_bytes,
                    request.maximum_headers,
                    external_identity.clone(),
                )
                .await?;
            let contents = result
                .as_object()
                .and_then(|object| object.get("contents"))
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    uncertain("mcp_egress_resource_read_invalid", external_identity.clone())
                })?;
            item_count = u32::try_from(contents.len()).map_err(|_| {
                uncertain(
                    "mcp_egress_resource_read_item_limit_exceeded",
                    external_identity.clone(),
                )
            })?;
            if item_count > request.maximum_items {
                return Err(uncertain(
                    "mcp_egress_resource_read_item_limit_exceeded",
                    external_identity,
                ));
            }
            let encoded = canonical_json(&result)
                .map_err(|_| uncertain("mcp_egress_resource_read_invalid", external_identity.clone()))?;
            byte_count = byte_count.saturating_add(encoded.len() as u64);
            if byte_count > request.maximum_total_bytes {
                return Err(uncertain(
                    "mcp_egress_resource_refresh_bytes_exceeded",
                    external_identity,
                ));
            }
            response_digests.push(digest_value(&result)?);
        }
        let resources = root_present
            .then(|| request.resource_uri.clone())
            .into_iter()
            .collect::<Vec<_>>();
        let observed_at = Utc::now();
        if observed_at > request.deadline {
            return Err(uncertain(
                "mcp_egress_resource_refresh_deadline_elapsed",
                external_identity,
            ));
        }
        Ok(McpResourceRefreshTransportEvidence {
            schema_version: request.schema_version,
            execution_identity_digest: request.execution_identity_digest,
            request_digest: request.request_digest,
            response_digest: digest_value(&serde_json::json!({
                "response_digests": response_digests,
                "schema_version": 1,
            }))?,
            resource_set_digest: digest_value(&serde_json::json!({
                "resources": resources,
                "schema_version": 1,
            }))?,
            resource_count: u32::from(root_present),
            item_count,
            byte_count,
            remote_revision: None,
            cursor: None,
            observed_at,
        })
    }
}

impl ReqwestMcpStreamableHttpConnector {
    #[allow(clippy::too_many_arguments)]
    async fn resource_refresh_call(
        &self,
        entry: &InstalledMcpStreamableHttpEndpoint,
        dns_host: &str,
        addresses: &[SocketAddr],
        headers: &HeaderMap,
        session: Option<&SensitiveSessionId>,
        operation_id: &str,
        method: &str,
        params: Value,
        maximum_request_bytes: u32,
        maximum_response_bytes: u32,
        request_timeout: Duration,
        idle_timeout: Duration,
        maximum_sse_event_bytes: u32,
        maximum_headers: u16,
        external_identity: Sha256Digest,
    ) -> Result<Value, McpTransportFailure> {
        let body = canonical_json(&serde_json::json!({
            "id": operation_id,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
        .map_err(|_| rejected("mcp_egress_resource_refresh_request_not_canonical"))?;
        if body.len() > maximum_request_bytes as usize {
            return Err(rejected("mcp_egress_resource_refresh_request_too_large"));
        }
        let response = self
            .post(
                entry,
                dns_host,
                addresses,
                headers,
                session,
                body,
                request_timeout,
                idle_timeout,
                maximum_response_bytes as usize,
                maximum_sse_event_bytes as usize,
                maximum_headers as usize,
                Some(operation_id),
                false,
                0,
                external_identity.clone(),
            )
            .await?;
        if response.status == StatusCode::UNAUTHORIZED || response.status == StatusCode::FORBIDDEN
        {
            return Err(reauthorization(&response.headers));
        }
        let wire = response_json(&response, maximum_response_bytes, &external_identity)?;
        if !response.status.is_success() && !is_matching_jsonrpc_error(&wire, operation_id) {
            return Err(uncertain(
                "mcp_egress_resource_refresh_http_failure",
                external_identity,
            ));
        }
        matching_result(&wire, operation_id).cloned().ok_or_else(|| {
            if is_matching_jsonrpc_error(&wire, operation_id) {
                McpTransportFailure::Permanent(safe_failure(
                    "mcp_egress_resource_refresh_jsonrpc_error",
                ))
            } else {
                uncertain(
                    "mcp_egress_resource_refresh_invalid_response",
                    external_identity,
                )
            }
        })
    }
}

/// Production connector for durable MCP Resource subscriptions. Protocol setup shares the same
/// exact endpoint, DNS, TLS and late-Secret boundary as ordinary operations, but long-lived GET
/// streams use an independent permit pool and only emit typed generation-bound observations.
pub struct ReqwestMcpStreamableHttpSubscriptionConnector {
    catalog: InstalledMcpStreamableHttpEndpointCatalog,
    secrets: Arc<dyn SecretMaterialResolver>,
    dns: Arc<dyn EgressDnsResolver>,
    transport: Arc<dyn PinnedMcpHttpTransport>,
    sessions: Arc<AeadMcpSubscriptionStateCodec>,
    sink: Arc<dyn McpStreamableHttpSubscriptionSink>,
    limits: McpStreamableHttpEgressLimits,
    permits: Arc<Semaphore>,
}

impl ReqwestMcpStreamableHttpSubscriptionConnector {
    pub fn new(
        catalog: InstalledMcpStreamableHttpEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        sessions: Arc<AeadMcpSubscriptionStateCodec>,
        sink: Arc<dyn McpStreamableHttpSubscriptionSink>,
        limits: McpStreamableHttpEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        Self::with_transport(
            catalog,
            secrets,
            dns,
            Arc::new(ReqwestPinnedMcpHttpTransport),
            sessions,
            sink,
            limits,
        )
    }

    fn with_transport(
        catalog: InstalledMcpStreamableHttpEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        transport: Arc<dyn PinnedMcpHttpTransport>,
        sessions: Arc<AeadMcpSubscriptionStateCodec>,
        sink: Arc<dyn McpStreamableHttpSubscriptionSink>,
        limits: McpStreamableHttpEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        limits.validate()?;
        Ok(Self {
            catalog,
            secrets,
            dns,
            transport,
            sessions,
            sink,
            limits,
            permits: Arc::new(Semaphore::new(limits.maximum_in_flight)),
        })
    }

    async fn resolve_addresses(
        &self,
        endpoint: &CanonicalHttpEndpoint,
    ) -> Result<(String, Vec<SocketAddr>), McpTransportFailure> {
        let host = parse_endpoint_host(&endpoint.host)
            .map_err(|_| rejected("mcp_egress_subscription_invalid_endpoint"))?;
        let (dns_host, mut addresses) = match host {
            ParsedEndpointHost::Address(address) => (
                address.to_string(),
                vec![SocketAddr::new(address, endpoint.port)],
            ),
            ParsedEndpointHost::Name(host) => {
                let resolved = self
                    .dns
                    .resolve(&host, endpoint.port)
                    .await
                    .map_err(|failure| match failure {
                        DnsResolutionError::Unavailable => {
                            retryable("mcp_egress_subscription_dns_unavailable")
                        }
                        DnsResolutionError::NoAddresses | DnsResolutionError::TooManyAddresses => {
                            rejected("mcp_egress_subscription_dns_rejected")
                        }
                    })?;
                (host, resolved)
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
            return Err(rejected("mcp_egress_subscription_destination_denied"));
        }
        Ok((dns_host, addresses))
    }

    async fn resolve_headers(
        &self,
        request: &McpStreamableHttpSubscriptionRequest,
    ) -> Result<HeaderMap, McpTransportFailure> {
        let resolved = self
            .secrets
            .resolve(&request.tenant_id, &request.token_secret_binding)
            .await
            .map_err(|failure| match failure {
                SecretMaterialResolutionError::Unavailable => {
                    retryable("mcp_egress_subscription_secret_unavailable")
                }
                SecretMaterialResolutionError::NotFound
                | SecretMaterialResolutionError::Revoked
                | SecretMaterialResolutionError::InvalidEvidence => {
                    rejected("mcp_egress_subscription_secret_rejected")
                }
            })?;
        if !resolved.validate_for(
            &request.token_secret_binding,
            self.limits.maximum_secret_material_bytes,
        ) {
            return Err(rejected("mcp_egress_subscription_secret_rejected"));
        }
        let mut bearer = Vec::with_capacity(7 + resolved.material.as_bytes().len());
        bearer.extend_from_slice(b"Bearer ");
        bearer.extend_from_slice(resolved.material.as_bytes());
        let parsed = HeaderValue::from_bytes(&bearer);
        bearer.fill(0);
        let mut authorization =
            parsed.map_err(|_| rejected("mcp_egress_subscription_secret_rejected"))?;
        authorization.set_sensitive(true);
        let protocol_version = HeaderValue::from_str(&request.protocol_version)
            .map_err(|_| rejected("mcp_egress_subscription_protocol_version_invalid"))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        headers.insert(AUTHORIZATION, authorization);
        headers.insert(MCP_PROTOCOL_VERSION_HEADER, protocol_version);
        Ok(headers)
    }

    #[allow(clippy::too_many_arguments)]
    async fn post(
        &self,
        entry: &InstalledMcpStreamableHttpEndpoint,
        dns_host: &str,
        addresses: &[SocketAddr],
        base_headers: &HeaderMap,
        session: Option<&SensitiveSessionId>,
        body: Vec<u8>,
        timeout: Duration,
        idle_timeout: Duration,
        maximum_response_bytes: usize,
        maximum_sse_event_bytes: usize,
        maximum_headers: usize,
        expected_response_id: Option<&str>,
        external_identity_digest: Sha256Digest,
    ) -> Result<PinnedMcpHttpResponse, McpTransportFailure> {
        let mut headers = base_headers.clone();
        if let Some(session) = session {
            headers.insert(MCP_SESSION_HEADER, session.header_value()?);
        }
        self.transport
            .round_trip(PinnedMcpHttpRequest {
                method: Method::POST,
                url: mcp_endpoint_url(&entry.endpoint)
                    .map_err(|_| rejected("mcp_egress_subscription_invalid_endpoint"))?,
                dns_host: dns_host.to_owned(),
                addresses: addresses.to_vec(),
                headers,
                body,
                timeout,
                idle_timeout,
                maximum_response_bytes,
                maximum_sse_event_bytes,
                maximum_headers,
                maximum_progress_events: 0,
                expected_response_id: expected_response_id.map(str::to_owned),
                allow_elicitation_request: false,
                external_identity_digest,
                trusted_root_pem: entry.trusted_root_pem.clone(),
            })
            .await
    }
}

#[async_trait]
impl McpStreamableHttpSubscriptionConnector for ReqwestMcpStreamableHttpSubscriptionConnector {
    async fn establish_subscription(
        &self,
        request: McpStreamableHttpSubscriptionRequest,
    ) -> Result<PreparedMcpSubscription, McpTransportFailure> {
        validate_subscription_request(&request, Utc::now())?;
        let entry = self.catalog.resolve_subscription(&request)?;
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| retryable("mcp_egress_subscription_capacity"))?;
        let (dns_host, addresses) = self.resolve_addresses(&entry.endpoint).await?;
        let headers = self.resolve_headers(&request).await?;
        let external_identity = subscription_external_identity(&request, &addresses)?;
        let now = Utc::now();
        let remaining = (request.deadline - now)
            .to_std()
            .map_err(|_| rejected("mcp_egress_subscription_deadline_elapsed"))?;
        let initialize_timeout = remaining.min(Duration::from_millis(
            request.initialize_timeout_milliseconds,
        ));
        let request_timeout =
            remaining.min(Duration::from_millis(request.request_timeout_milliseconds));
        let idle_timeout = remaining.min(Duration::from_millis(request.idle_timeout_milliseconds));
        if initialize_timeout.is_zero() || request_timeout.is_zero() || idle_timeout.is_zero() {
            return Err(rejected("mcp_egress_subscription_deadline_elapsed"));
        }

        let initialize_id = format!("{}:initialize", request.subscription_id);
        let initialize_body =
            canonical_json(&subscription_initialize_request(&request, &initialize_id))
                .map_err(|_| rejected("mcp_egress_subscription_request_not_canonical"))?;
        if initialize_body.len() > request.maximum_message_bytes as usize {
            return Err(rejected(
                "mcp_egress_subscription_initialize_request_too_large",
            ));
        }
        let initialize = self
            .post(
                &entry,
                &dns_host,
                &addresses,
                &headers,
                None,
                initialize_body,
                initialize_timeout,
                idle_timeout,
                request.maximum_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                Some(&initialize_id),
                external_identity.clone(),
            )
            .await?;
        if initialize.status == StatusCode::UNAUTHORIZED
            || initialize.status == StatusCode::FORBIDDEN
        {
            return Err(reauthorization(&initialize.headers));
        }
        require_success_status(initialize.status, &external_identity)?;
        let initialize_wire = response_json(
            &initialize,
            request.maximum_response_bytes,
            &external_identity,
        )?;
        validate_initialize_response(
            &initialize_wire,
            &initialize_id,
            &request.protocol_version,
            &request.negotiated_capabilities,
        )?;
        let session = SensitiveSessionId::from_headers(&initialize.headers)?;

        let initialized = canonical_json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }))
        .map_err(|_| rejected("mcp_egress_subscription_request_not_canonical"))?;
        let acknowledged = self
            .post(
                &entry,
                &dns_host,
                &addresses,
                &headers,
                session.as_ref(),
                initialized,
                initialize_timeout,
                idle_timeout,
                request.maximum_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                None,
                external_identity.clone(),
            )
            .await?;
        if acknowledged.status != StatusCode::ACCEPTED || !acknowledged.body.is_empty() {
            return Err(uncertain(
                "mcp_egress_subscription_initialized_not_accepted",
                external_identity,
            ));
        }

        let subscribe_id = format!("{}:subscribe", request.subscription_id);
        let subscribe_body = canonical_json(&serde_json::json!({
            "id": subscribe_id,
            "jsonrpc": "2.0",
            "method": "resources/subscribe",
            "params": {"uri": request.resource_uri},
        }))
        .map_err(|_| rejected("mcp_egress_subscription_request_not_canonical"))?;
        if subscribe_body.len() > request.maximum_message_bytes as usize {
            return Err(rejected("mcp_egress_subscribe_request_too_large"));
        }
        let subscribed = self
            .post(
                &entry,
                &dns_host,
                &addresses,
                &headers,
                session.as_ref(),
                subscribe_body,
                request_timeout,
                idle_timeout,
                request.maximum_response_bytes as usize,
                request.maximum_sse_event_bytes as usize,
                request.maximum_headers as usize,
                Some(&subscribe_id),
                external_identity.clone(),
            )
            .await?;
        if subscribed.status == StatusCode::UNAUTHORIZED
            || subscribed.status == StatusCode::FORBIDDEN
        {
            return Err(reauthorization(&subscribed.headers));
        }
        let subscribe_wire = response_json(
            &subscribed,
            request.maximum_response_bytes,
            &external_identity,
        )?;
        validate_empty_result(&subscribe_wire, &subscribe_id, &external_identity)?;

        let established_at = Utc::now();
        let maximum_expiry = established_at
            .checked_add_signed(chrono::Duration::milliseconds(
                i64::try_from(request.maximum_session_milliseconds)
                    .map_err(|_| rejected("mcp_egress_subscription_limits_invalid"))?,
            ))
            .ok_or_else(|| rejected("mcp_egress_subscription_limits_invalid"))?;
        let expires_at = request.deadline.min(maximum_expiry);
        let claims = McpSubscriptionStateClaims {
            schema_version: 1,
            tenant_id: request.tenant_id.clone(),
            deployment: request.deployment.clone(),
            authorization_binding_id: request.authorization_binding_id.clone(),
            authorization_generation: request.authorization_generation,
            principal_binding_generation: request.principal_binding_generation,
            subscription_id: request.subscription_id.clone(),
            session_generation: request.session_generation,
            binding_digest: request.binding_digest.clone(),
            session_id: session.as_ref().map(SensitiveSessionId::clone_sensitive),
            external_identity_digest: external_identity.clone(),
            expires_at_epoch_milliseconds: expires_at.timestamp_millis(),
        };
        let encrypted_opaque_session = self.sessions.seal(&claims)?;
        let evidence_digest = subscription_established_evidence(&request, &external_identity)?;
        let activation = ReqwestMcpSubscriptionActivation {
            request,
            entry,
            dns_host,
            addresses,
            headers,
            session,
            transport: Arc::clone(&self.transport),
            sink: Arc::clone(&self.sink),
            limits: self.limits,
            external_identity_digest: external_identity,
            _permit: permit,
        };
        Ok(PreparedMcpSubscription::new(
            EstablishedMcpSubscription {
                transport_kind: insight_platform_contracts::McpTransportKind::StreamableHttp,
                binding_digest: claims.binding_digest.clone(),
                encrypted_opaque_session,
                established_at,
                expires_at,
                evidence_digest,
            },
            Box::new(activation),
        ))
    }
}

struct ReqwestMcpSubscriptionActivation {
    request: McpStreamableHttpSubscriptionRequest,
    entry: InstalledMcpStreamableHttpEndpoint,
    dns_host: String,
    addresses: Vec<SocketAddr>,
    headers: HeaderMap,
    session: Option<SensitiveSessionId>,
    transport: Arc<dyn PinnedMcpHttpTransport>,
    sink: Arc<dyn McpStreamableHttpSubscriptionSink>,
    limits: McpStreamableHttpEgressLimits,
    external_identity_digest: Sha256Digest,
    _permit: OwnedSemaphorePermit,
}

#[async_trait]
impl McpSubscriptionActivation for ReqwestMcpSubscriptionActivation {
    async fn activate(self: Box<Self>) {
        tokio::spawn(async move {
            let result = self.drive().await;
            if let Err(failure) = result {
                let termination = McpStreamableHttpSubscriptionTermination {
                    tenant_id: self.request.tenant_id.clone(),
                    subscription_id: self.request.subscription_id.clone(),
                    authorization_generation: self.request.authorization_generation,
                    session_generation: self.request.session_generation,
                    worker_process_generation_id: self.request.worker_process_generation_id.clone(),
                    observed_at: Utc::now(),
                    failure,
                };
                let _ = self.sink.report_termination(termination).await;
            }
        });
    }
}

impl ReqwestMcpSubscriptionActivation {
    async fn drive(&self) -> Result<(), McpTransportFailure> {
        let mut last_event_id: Option<Vec<u8>> = None;
        let mut last_event_generation = 0_u64;
        let mut retry_milliseconds = DEFAULT_MCP_SSE_RETRY_MILLISECONDS;
        let mut reconnects = 0_u32;
        let mut events = 0_u64;
        loop {
            if Utc::now() >= self.request.deadline {
                return Err(uncertain(
                    "mcp_egress_subscription_session_expired",
                    self.external_identity_digest.clone(),
                ));
            }
            let result = self
                .consume_stream(last_event_id.as_deref(), last_event_generation, &mut events)
                .await?;
            if !result.all_data_events_resumable {
                return Err(uncertain(
                    "mcp_egress_subscription_stream_not_resumable",
                    self.external_identity_digest.clone(),
                ));
            }
            last_event_id = result.last_event_id;
            last_event_generation = result.last_event_generation;
            reconnects = reconnects.saturating_add(1);
            if reconnects > self.limits.maximum_subscription_reconnects {
                return Err(uncertain(
                    "mcp_egress_subscription_reconnect_limit_exceeded",
                    self.external_identity_digest.clone(),
                ));
            }
            if let Some(value) = result.retry_milliseconds {
                retry_milliseconds = value;
            }
            if last_event_id.is_none() {
                return Err(uncertain(
                    "mcp_egress_subscription_closed_without_event_id",
                    self.external_identity_digest.clone(),
                ));
            }
            let remaining = (self.request.deadline - Utc::now()).to_std().map_err(|_| {
                uncertain(
                    "mcp_egress_subscription_session_expired",
                    self.external_identity_digest.clone(),
                )
            })?;
            let delay = Duration::from_millis(retry_milliseconds).min(remaining);
            if delay.is_zero() || delay >= remaining {
                return Err(uncertain(
                    "mcp_egress_subscription_session_expired",
                    self.external_identity_digest.clone(),
                ));
            }
            tokio::time::sleep(delay).await;
        }
    }

    async fn consume_stream(
        &self,
        last_event_id: Option<&[u8]>,
        last_event_generation: u64,
        events: &mut u64,
    ) -> Result<ClosedMcpSubscriptionStream, McpTransportFailure> {
        let mut headers = self.headers.clone();
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        headers.remove(CONTENT_TYPE);
        if let Some(session) = &self.session {
            headers.insert(MCP_SESSION_HEADER, session.header_value()?);
        }
        if let Some(last_event_id) = last_event_id {
            let value = HeaderValue::from_bytes(last_event_id)
                .map_err(|_| rejected("mcp_egress_subscription_event_id_invalid"))?;
            headers.insert(LAST_EVENT_ID_HEADER, value);
        }
        let remaining = (self.request.deadline - Utc::now())
            .to_std()
            .map_err(|_| rejected("mcp_egress_subscription_deadline_elapsed"))?;
        let mut response = self
            .transport
            .open_sse(PinnedMcpSseRequest {
                url: mcp_endpoint_url(&self.entry.endpoint)
                    .map_err(|_| rejected("mcp_egress_subscription_invalid_endpoint"))?,
                dns_host: self.dns_host.clone(),
                addresses: self.addresses.clone(),
                headers,
                connect_timeout: remaining.min(Duration::from_millis(
                    self.request.initialize_timeout_milliseconds,
                )),
                total_timeout: remaining,
                maximum_headers: self.request.maximum_headers as usize,
                external_identity_digest: self.external_identity_digest.clone(),
                trusted_root_pem: self.entry.trusted_root_pem.clone(),
            })
            .await?;
        if response.status == StatusCode::UNAUTHORIZED || response.status == StatusCode::FORBIDDEN {
            return Err(reauthorization(&response.headers));
        }
        if response.status == StatusCode::NOT_FOUND && self.session.is_some() {
            return Err(uncertain(
                "mcp_egress_subscription_session_not_found",
                self.external_identity_digest.clone(),
            ));
        }
        if response.status == StatusCode::METHOD_NOT_ALLOWED {
            return Err(McpTransportFailure::Permanent(safe_failure(
                "mcp_egress_subscription_get_unsupported",
            )));
        }
        if !response.status.is_success() || !content_type_is(&response.headers, "text/event-stream")
        {
            return Err(uncertain(
                "mcp_egress_subscription_invalid_stream_response",
                self.external_identity_digest.clone(),
            ));
        }

        let mut buffer = Vec::new();
        let mut stream_last_event_id = last_event_id.map(ToOwned::to_owned);
        let mut stream_last_event_generation = last_event_generation;
        let mut retry_milliseconds = None;
        let mut all_data_events_resumable = true;
        loop {
            let next = tokio::time::timeout(
                Duration::from_millis(self.request.idle_timeout_milliseconds),
                response.chunks.next(),
            )
            .await;
            let chunk = match next {
                Ok(Some(Ok(chunk))) => chunk,
                Ok(Some(Err(failure))) => return Err(failure),
                Ok(None) => break,
                Err(_) => {
                    return Err(uncertain(
                        "mcp_egress_subscription_idle_timeout",
                        self.external_identity_digest.clone(),
                    ))
                }
            };
            buffer.extend_from_slice(&chunk);
            if buffer.len() > self.request.maximum_sse_event_bytes as usize {
                return Err(uncertain(
                    "mcp_egress_subscription_sse_event_too_large",
                    self.external_identity_digest.clone(),
                ));
            }
            while let Some(event_end) = first_complete_sse_event_end(&buffer) {
                let event = buffer[..event_end].to_vec();
                buffer.drain(..event_end);
                let parsed = parse_subscription_sse_event(
                    &event,
                    self.request.maximum_sse_event_bytes as usize,
                )?;
                if let Some(value) = parsed.retry_milliseconds {
                    retry_milliseconds = Some(value);
                }
                let Some(wire) = parsed.wire else {
                    if let Some(value) = parsed.event_id {
                        if stream_last_event_id.as_deref() != Some(value.as_slice()) {
                            stream_last_event_id = Some(value);
                        }
                    }
                    continue;
                };
                if parsed.event_id.is_none() {
                    all_data_events_resumable = false;
                }
                let event_generation = if parsed.event_id.as_deref()
                    == stream_last_event_id.as_deref()
                    && stream_last_event_generation != 0
                {
                    stream_last_event_generation
                } else {
                    *events = events.saturating_add(1);
                    *events
                };
                if *events > self.limits.maximum_subscription_events_per_session {
                    return Err(uncertain(
                        "mcp_egress_subscription_event_limit_exceeded",
                        self.external_identity_digest.clone(),
                    ));
                }
                if let Some(value) = parsed.event_id.as_ref() {
                    stream_last_event_id = Some(value.clone());
                    stream_last_event_generation = event_generation;
                }
                let event_key_digest = subscription_event_key(
                    &self.request,
                    parsed.event_id.as_deref(),
                    event_generation,
                )?;
                self.sink
                    .ingest_notification(McpStreamableHttpSubscriptionNotification {
                        tenant_id: self.request.tenant_id.clone(),
                        subscription_id: self.request.subscription_id.clone(),
                        authorization_generation: self.request.authorization_generation,
                        session_generation: self.request.session_generation,
                        event_generation,
                        event_key_digest,
                        wire: SensitiveMcpNotificationWire::new(wire)
                            .map_err(|_| rejected("mcp_egress_subscription_wire_invalid"))?,
                        received_at: Utc::now(),
                    })
                    .await
                    .map_err(map_subscription_sink_error)?;
            }
        }
        if !buffer.is_empty() {
            return Err(uncertain(
                "mcp_egress_subscription_incomplete_sse",
                self.external_identity_digest.clone(),
            ));
        }
        Ok(ClosedMcpSubscriptionStream {
            last_event_id: stream_last_event_id,
            last_event_generation: stream_last_event_generation,
            retry_milliseconds,
            all_data_events_resumable,
        })
    }
}

struct ClosedMcpSubscriptionStream {
    last_event_id: Option<Vec<u8>>,
    last_event_generation: u64,
    retry_milliseconds: Option<u64>,
    all_data_events_resumable: bool,
}

struct ParsedMcpSubscriptionSseEvent {
    event_id: Option<Vec<u8>>,
    retry_milliseconds: Option<u64>,
    wire: Option<Vec<u8>>,
}

fn parse_subscription_sse_event(
    event: &[u8],
    maximum_event_bytes: usize,
) -> Result<ParsedMcpSubscriptionSseEvent, McpTransportFailure> {
    if event.is_empty() || event.len() > maximum_event_bytes {
        return Err(rejected("mcp_egress_subscription_sse_event_invalid"));
    }
    let text = std::str::from_utf8(event)
        .map_err(|_| rejected("mcp_egress_subscription_sse_event_invalid"))?;
    let mut event_id = None;
    let mut retry_milliseconds = None;
    let mut data = Vec::new();
    let mut saw_comment = false;
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if line.starts_with(':') {
            saw_comment = true;
            continue;
        }
        if let Some(value) = line.strip_prefix("id:") {
            let value = value.strip_prefix(' ').unwrap_or(value).as_bytes();
            if event_id.is_some()
                || value.is_empty()
                || value.len() > MAX_MCP_SSE_EVENT_ID_BYTES
                || value.iter().any(|byte| matches!(*byte, b'\r' | b'\n' | 0))
            {
                return Err(rejected("mcp_egress_subscription_event_id_invalid"));
            }
            event_id = Some(value.to_vec());
            continue;
        }
        if let Some(value) = line.strip_prefix("retry:") {
            let value = value.strip_prefix(' ').unwrap_or(value);
            let parsed = value
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0 && *value <= MAX_MCP_SSE_RETRY_MILLISECONDS)
                .ok_or_else(|| rejected("mcp_egress_subscription_retry_invalid"))?;
            if retry_milliseconds.replace(parsed).is_some() {
                return Err(rejected("mcp_egress_subscription_retry_invalid"));
            }
            continue;
        }
        if line.starts_with("event:") {
            continue;
        }
        let Some(value) = line.strip_prefix("data:") else {
            return Err(rejected("mcp_egress_subscription_sse_event_invalid"));
        };
        if !data.is_empty() {
            data.push(b'\n');
        }
        data.extend_from_slice(value.strip_prefix(' ').unwrap_or(value).as_bytes());
    }
    let wire = (!data.is_empty()).then_some(data);
    if wire.is_none() && event_id.is_none() && retry_milliseconds.is_none() && !saw_comment {
        return Err(rejected("mcp_egress_subscription_sse_event_invalid"));
    }
    Ok(ParsedMcpSubscriptionSseEvent {
        event_id,
        retry_milliseconds,
        wire,
    })
}

fn map_subscription_sink_error(
    failure: McpStreamableHttpSubscriptionSinkError,
) -> McpTransportFailure {
    match failure {
        McpStreamableHttpSubscriptionSinkError::Rejected => {
            rejected("mcp_egress_subscription_notification_rejected")
        }
        McpStreamableHttpSubscriptionSinkError::Saturated => {
            retryable("mcp_egress_subscription_notification_saturated")
        }
        McpStreamableHttpSubscriptionSinkError::Unavailable => {
            retryable("mcp_egress_subscription_notification_unavailable")
        }
    }
}

fn validate_request(
    request: &McpStreamableHttpRequest,
    now: DateTime<Utc>,
) -> Result<(), McpTransportFailure> {
    let policies = [
        &request.protocol_policy,
        &request.network_policy,
        &request.tls_policy,
        &request.trust_policy,
    ];
    if request.tenant_id.kind() != ResourceKind::Tenant
        || request.deployment.resource_kind != ResourceKind::McpDeployment
        || request.deployment.validate().is_err()
        || request.endpoint.validate().is_err()
        || request.endpoint.scheme != CapabilityEndpointScheme::Https
        || request.endpoint.canonical_digest().as_ref() != Ok(&request.endpoint_identity_digest)
        || request.auth_policy.as_ref().is_none_or(|value| {
            value.resource_kind != ResourceKind::PolicyRevision || value.validate().is_err()
        })
        || policies.iter().any(|value| {
            value.resource_kind != ResourceKind::PolicyRevision || value.validate().is_err()
        })
        || request.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
        || request.authorization_generation == 0
        || request.principal_binding_generation == 0
        || request.token_secret_binding.validate().is_err()
        || !matches!(
            request.token_secret_binding.resolution_policy,
            SecretResolutionPolicy::Pinned { .. }
        )
        || request.protocol_version.len() != 10
        || request.protocol_version.chars().any(char::is_control)
        || request.operation_id.kind() != ResourceKind::McpOperation
        || request.invocation_id.kind() != ResourceKind::CapabilityInvocation
        || request.job_id.kind() != ResourceKind::Job
        || request.physical_attempt == 0
        || request.discovery_snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
        || request.params.validate().is_err()
        || !request.params.value.is_object()
        || (request.task_requested && request.continuation.is_some())
        || (request.task_requested && !request.negotiated_capabilities.tasks_tools_call)
        || request.continuation.as_ref().is_some_and(|continuation| {
            request.method != PublishedMcpMethod::ToolsCall
                || !request.negotiated_capabilities.tasks_tools_call
                || continuation.poll_count == 0
                || continuation.encrypted_state.validate().is_err()
                || continuation
                    .elicitation_response
                    .as_ref()
                    .is_some_and(|response| {
                        response.content.is_some()
                            != (response.action == McpElicitationAction::Accept)
                            || response
                                .content
                                .as_ref()
                                .is_some_and(|content| content.validate().is_err())
                            || !request.client_capabilities.elicitation_form
                            || !request.client_capabilities.tasks_elicitation_create
                            || !request.negotiated_capabilities.elicitation
                    })
        })
        || (request.task_requested || request.continuation.is_some())
            != request.task_limits.is_some()
        || request
            .task_limits
            .is_some_and(|limits| !valid_remote_task_limits(limits))
        || request.task_limits.is_some_and(|limits| {
            limits.maximum_cancel_request_bytes.is_some()
                != request.negotiated_capabilities.tasks_cancel
        })
        || !method_negotiated(request.method, &request.negotiated_capabilities)
        || request.deadline.timestamp_millis() <= 0
        || request.transport_deadline <= now
        || request.maximum_request_bytes == 0
        || request.maximum_request_bytes > MAX_MCP_HTTP_REQUEST_BYTES_HARD
        || request.maximum_response_bytes == 0
        || request.maximum_response_bytes > MAX_MCP_HTTP_RESPONSE_BYTES_HARD
        || request.maximum_headers == 0
        || request.maximum_headers > MAX_MCP_HTTP_HEADERS_HARD
        || request.maximum_sse_event_bytes == 0
        || request.maximum_sse_event_bytes > request.maximum_response_bytes
        || request.maximum_progress_events > 10_000
        || request.idle_timeout_milliseconds == 0
        || request.idle_timeout_milliseconds > MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD
        || request.initialize_timeout_milliseconds == 0
        || request.initialize_timeout_milliseconds > MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD
        || request.request_timeout_milliseconds == 0
        || request.request_timeout_milliseconds > MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD
    {
        return Err(rejected("mcp_egress_invalid_request"));
    }
    Ok(())
}

fn validate_resource_refresh_request(
    request: &McpResourceRefreshTransportRequest,
    now: DateTime<Utc>,
) -> Result<(), McpTransportFailure> {
    request
        .validate_at(now)
        .map_err(|_| rejected("mcp_egress_resource_refresh_request_invalid"))?;
    let limits = request
        .list_limits
        .into_iter()
        .chain(std::iter::once(request.read_limits));
    if request.endpoint.scheme != CapabilityEndpointScheme::Https
        || request.resource_uri_digest
            != canonical_digest(&Value::String(request.resource_uri.clone()))
                .ok()
                .and_then(|digest| digest.parse().ok())
                .ok_or_else(|| rejected("mcp_egress_resource_refresh_request_invalid"))?
        || limits.into_iter().any(|limits| {
            limits.maximum_request_bytes == 0
                || limits.maximum_request_bytes > MAX_MCP_HTTP_REQUEST_BYTES_HARD
                || limits.maximum_response_bytes == 0
                || limits.maximum_response_bytes > MAX_MCP_HTTP_RESPONSE_BYTES_HARD
                || limits.maximum_pages == 0
                || limits.maximum_pages > 1_000
        })
        || request.maximum_headers == 0
        || request.maximum_headers > MAX_MCP_HTTP_HEADERS_HARD
        || request.maximum_sse_event_bytes == 0
        || request.maximum_sse_event_bytes > MAX_MCP_HTTP_RESPONSE_BYTES_HARD
        || request.idle_timeout_milliseconds == 0
        || request.idle_timeout_milliseconds > MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD
        || request.initialize_timeout_milliseconds == 0
        || request.initialize_timeout_milliseconds > MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD
        || request.request_timeout_milliseconds == 0
        || request.request_timeout_milliseconds > MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD
    {
        return Err(rejected("mcp_egress_resource_refresh_request_invalid"));
    }
    Ok(())
}

fn validate_subscription_request(
    request: &McpStreamableHttpSubscriptionRequest,
    now: DateTime<Utc>,
) -> Result<(), McpTransportFailure> {
    let policies = [
        &request.protocol_policy,
        &request.network_policy,
        &request.tls_policy,
        &request.trust_policy,
    ];
    if request.tenant_id.kind() != ResourceKind::Tenant
        || request.deployment.resource_kind != ResourceKind::McpDeployment
        || request.deployment.validate().is_err()
        || request.endpoint.scheme != CapabilityEndpointScheme::Https
        || request.endpoint.canonical_digest().as_ref() != Ok(&request.endpoint_identity_digest)
        || request.auth_policy.as_ref().is_none_or(|value| {
            value.resource_kind != ResourceKind::PolicyRevision || value.validate().is_err()
        })
        || policies.iter().any(|value| {
            value.resource_kind != ResourceKind::PolicyRevision || value.validate().is_err()
        })
        || request.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
        || request.authorization_generation == 0
        || request.principal_binding_generation == 0
        || request.token_secret_binding.validate().is_err()
        || !matches!(
            request.token_secret_binding.resolution_policy,
            SecretResolutionPolicy::Pinned { .. }
        )
        || request.protocol_version.len() != 10
        || request.protocol_version.chars().any(char::is_control)
        || !request.negotiated_capabilities.resources
        || !request.negotiated_capabilities.subscriptions
        || request.subscription_id.kind() != ResourceKind::McpOperation
        || request.session_generation == 0
        || request.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
        || request.resource_uri.is_empty()
        || request.resource_uri.len() > 8_192
        || request.resource_uri.chars().any(char::is_control)
        || canonical_digest(&Value::String(request.resource_uri.clone()))
            .ok()
            .and_then(|value| value.parse::<Sha256Digest>().ok())
            .as_ref()
            != Some(&request.resource_uri_digest)
        || request.deadline <= now
        || request.maximum_message_bytes == 0
        || request.maximum_message_bytes > MAX_MCP_HTTP_REQUEST_BYTES_HARD
        || request.maximum_response_bytes == 0
        || request.maximum_response_bytes > MAX_MCP_HTTP_RESPONSE_BYTES_HARD
        || request.maximum_headers == 0
        || request.maximum_headers > MAX_MCP_HTTP_HEADERS_HARD
        || request.maximum_sse_event_bytes == 0
        || request.maximum_sse_event_bytes > request.maximum_message_bytes
        || request.idle_timeout_milliseconds == 0
        || request.idle_timeout_milliseconds > MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD
        || request.initialize_timeout_milliseconds == 0
        || request.initialize_timeout_milliseconds > MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD
        || request.request_timeout_milliseconds == 0
        || request.request_timeout_milliseconds > MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD
        || request.maximum_session_milliseconds == 0
        || request.maximum_session_milliseconds > MAX_MCP_SESSION_MILLISECONDS
    {
        return Err(rejected("mcp_egress_subscription_request_invalid"));
    }
    Ok(())
}

fn subscription_initialize_request(
    request: &McpStreamableHttpSubscriptionRequest,
    id: &str,
) -> Value {
    let mut capabilities = Map::new();
    if request.client_capabilities.elicitation_form || request.client_capabilities.elicitation_url {
        let mut elicitation = Map::new();
        if request.client_capabilities.elicitation_form {
            elicitation.insert("form".to_owned(), Value::Object(Map::new()));
        }
        if request.client_capabilities.elicitation_url {
            elicitation.insert("url".to_owned(), Value::Object(Map::new()));
        }
        capabilities.insert("elicitation".to_owned(), Value::Object(elicitation));
    }
    if request.client_capabilities.tasks_elicitation_create {
        capabilities.insert(
            "tasks".to_owned(),
            serde_json::json!({"requests": {"elicitation": {"create": {}}}}),
        );
    }
    if request.client_capabilities.sampling {
        capabilities.insert("sampling".to_owned(), Value::Object(Map::new()));
    }
    if request.client_capabilities.roots {
        capabilities.insert(
            "roots".to_owned(),
            serde_json::json!({"listChanged": false}),
        );
    }
    serde_json::json!({
        "id": id,
        "jsonrpc": "2.0",
        "method": "initialize",
        "params": {
            "capabilities": capabilities,
            "clientInfo": {"name": "insight-platform-mcp-host", "version": "1"},
            "protocolVersion": request.protocol_version,
        }
    })
}

fn validate_empty_result(
    wire: &Value,
    id: &str,
    external_identity: &Sha256Digest,
) -> Result<(), McpTransportFailure> {
    let Some(result) = matching_result(wire, id) else {
        if is_matching_jsonrpc_error(wire, id) {
            return Err(McpTransportFailure::Permanent(safe_failure(
                "mcp_egress_subscription_rejected",
            )));
        }
        return Err(uncertain(
            "mcp_egress_subscription_response_invalid",
            external_identity.clone(),
        ));
    };
    if !result.as_object().is_some_and(Map::is_empty) {
        return Err(uncertain(
            "mcp_egress_subscription_result_invalid",
            external_identity.clone(),
        ));
    }
    Ok(())
}

fn subscription_external_identity(
    request: &McpStreamableHttpSubscriptionRequest,
    addresses: &[SocketAddr],
) -> Result<Sha256Digest, McpTransportFailure> {
    digest_value(&serde_json::json!({
        "addresses": addresses.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "authorization_binding_id": request.authorization_binding_id,
        "authorization_generation": request.authorization_generation,
        "binding_digest": request.binding_digest,
        "deployment": request.deployment,
        "endpoint_identity_digest": request.endpoint_identity_digest,
        "schema_version": 1,
        "session_generation": request.session_generation,
        "subscription_id": request.subscription_id,
    }))
}

fn subscription_established_evidence(
    request: &McpStreamableHttpSubscriptionRequest,
    external_identity: &Sha256Digest,
) -> Result<Sha256Digest, McpTransportFailure> {
    digest_value(&serde_json::json!({
        "binding_digest": request.binding_digest,
        "external_identity_digest": external_identity,
        "protocol_version": request.protocol_version,
        "resource_uri_digest": request.resource_uri_digest,
        "schema_version": 1,
        "session_generation": request.session_generation,
        "subscription_id": request.subscription_id,
    }))
}

fn subscription_event_key(
    request: &McpStreamableHttpSubscriptionRequest,
    event_id: Option<&[u8]>,
    event_generation: u64,
) -> Result<Sha256Digest, McpTransportFailure> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let stable = event_id
        .map(|value| URL_SAFE_NO_PAD.encode(value))
        .unwrap_or_else(|| format!("generation:{event_generation}"));
    digest_value(&serde_json::json!({
        "authorization_generation": request.authorization_generation,
        "event_identity": stable,
        "schema_version": 1,
        "session_generation": request.session_generation,
        "subscription_id": request.subscription_id,
    }))
}

fn valid_remote_task_limits(limits: McpRemoteTaskLimits) -> bool {
    limits.maximum_get_request_bytes > 0
        && limits.maximum_get_request_bytes <= MAX_MCP_HTTP_REQUEST_BYTES_HARD
        && limits.maximum_get_response_bytes > 0
        && limits.maximum_get_response_bytes <= MAX_MCP_HTTP_RESPONSE_BYTES_HARD
        && limits.maximum_result_request_bytes > 0
        && limits.maximum_result_request_bytes <= MAX_MCP_HTTP_REQUEST_BYTES_HARD
        && limits.maximum_result_response_bytes > 0
        && limits.maximum_result_response_bytes <= MAX_MCP_HTTP_RESPONSE_BYTES_HARD
        && limits
            .maximum_cancel_request_bytes
            .is_some_and(|value| value > 0 && value <= MAX_MCP_HTTP_REQUEST_BYTES_HARD)
            == limits.maximum_cancel_response_bytes.is_some()
        && limits
            .maximum_cancel_response_bytes
            .is_none_or(|value| value > 0 && value <= MAX_MCP_HTTP_RESPONSE_BYTES_HARD)
        && limits.maximum_get_progress_events <= 10_000
        && limits.maximum_result_progress_events <= 10_000
        && limits.maximum_cancel_progress_events.is_some()
            == limits.maximum_cancel_request_bytes.is_some()
        && limits
            .maximum_cancel_progress_events
            .is_none_or(|value| value <= 10_000)
        && limits.minimum_poll_milliseconds > 0
        && limits.minimum_poll_milliseconds <= limits.maximum_poll_milliseconds
        && limits.maximum_poll_milliseconds <= MAX_MCP_HTTP_TIMEOUT_MILLISECONDS_HARD
}

fn initialize_request(request: &McpStreamableHttpRequest, id: &str) -> Value {
    let mut capabilities = Map::new();
    let clients: &McpClientCapabilities = &request.client_capabilities;
    if clients.elicitation_form || clients.elicitation_url {
        let mut elicitation = Map::new();
        if clients.elicitation_form {
            elicitation.insert("form".to_owned(), Value::Object(Map::new()));
        }
        if clients.elicitation_url {
            elicitation.insert("url".to_owned(), Value::Object(Map::new()));
        }
        capabilities.insert("elicitation".to_owned(), Value::Object(elicitation));
    }
    if clients.tasks_elicitation_create {
        capabilities.insert(
            "tasks".to_owned(),
            serde_json::json!({"requests": {"elicitation": {"create": {}}}}),
        );
    }
    if clients.sampling {
        capabilities.insert("sampling".to_owned(), Value::Object(Map::new()));
    }
    if clients.roots {
        capabilities.insert(
            "roots".to_owned(),
            serde_json::json!({"listChanged": false}),
        );
    }
    serde_json::json!({
        "id": id,
        "jsonrpc": "2.0",
        "method": "initialize",
        "params": {
            "capabilities": capabilities,
            "clientInfo": {"name": "insight-platform-mcp-host", "version": "1"},
            "protocolVersion": request.protocol_version,
        }
    })
}

fn resource_refresh_initialize_request(
    request: &McpResourceRefreshTransportRequest,
    id: &str,
) -> Value {
    let clients = &request.client_capabilities;
    let mut capabilities = Map::new();
    if clients.elicitation_form || clients.elicitation_url {
        let mut elicitation = Map::new();
        if clients.elicitation_form {
            elicitation.insert("form".to_owned(), Value::Object(Map::new()));
        }
        if clients.elicitation_url {
            elicitation.insert("url".to_owned(), Value::Object(Map::new()));
        }
        capabilities.insert("elicitation".to_owned(), Value::Object(elicitation));
    }
    if clients.tasks_elicitation_create {
        capabilities.insert(
            "tasks".to_owned(),
            serde_json::json!({"requests": {"elicitation": {"create": {}}}}),
        );
    }
    if clients.sampling {
        capabilities.insert("sampling".to_owned(), Value::Object(Map::new()));
    }
    if clients.roots {
        capabilities.insert(
            "roots".to_owned(),
            serde_json::json!({"listChanged": false}),
        );
    }
    serde_json::json!({
        "id": id,
        "jsonrpc": "2.0",
        "method": "initialize",
        "params": {
            "capabilities": capabilities,
            "clientInfo": {"name": "insight-platform-mcp-host", "version": "1"},
            "protocolVersion": request.protocol_version,
        }
    })
}

fn validate_initialize_response(
    wire: &Value,
    id: &str,
    protocol_version: &str,
    expected: &McpNegotiatedCapabilities,
) -> Result<(), McpTransportFailure> {
    let result = matching_result(wire, id)
        .ok_or_else(|| uncertain("mcp_egress_invalid_initialize", static_external()))?;
    let object = result
        .as_object()
        .ok_or_else(|| uncertain("mcp_egress_invalid_initialize", static_external()))?;
    if object.get("protocolVersion").and_then(Value::as_str) != Some(protocol_version) {
        return Err(uncertain(
            "mcp_egress_protocol_version_changed",
            static_external(),
        ));
    }
    let capabilities = object
        .get("capabilities")
        .and_then(Value::as_object)
        .ok_or_else(|| uncertain("mcp_egress_invalid_initialize", static_external()))?;
    let actual = |name: &str| capabilities.get(name).is_some_and(Value::is_object);
    let resources_subscribe = capabilities
        .get("resources")
        .and_then(Value::as_object)
        .and_then(|value| value.get("subscribe"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let tasks = capabilities.get("tasks").and_then(Value::as_object);
    let task_flag = |name: &str| {
        tasks
            .and_then(|value| value.get(name))
            .is_some_and(Value::is_object)
    };
    let tasks_tools_call = tasks
        .and_then(|value| value.get("requests"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("tools"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("call"))
        .is_some_and(Value::is_object);
    if expected.tools != actual("tools")
        || expected.resources != actual("resources")
        || expected.prompts != actual("prompts")
        || expected.logging != actual("logging")
        || expected.tasks != actual("tasks")
        || expected.tasks_list != task_flag("list")
        || expected.tasks_cancel != task_flag("cancel")
        || expected.tasks_tools_call != tasks_tools_call
        || expected.subscriptions != resources_subscribe
    {
        return Err(uncertain(
            "mcp_egress_capability_changed",
            static_external(),
        ));
    }
    Ok(())
}

fn completed_operation(
    request: &McpStreamableHttpRequest,
    result: &Value,
    external_identity_digest: Sha256Digest,
) -> Result<McpOperationOutcome, McpTransportFailure> {
    if result
        .as_object()
        .is_some_and(|object| object.contains_key("task"))
    {
        return Err(uncertain(
            "mcp_egress_unexpected_remote_task",
            external_identity_digest,
        ));
    }
    let closed = ClosedJsonValue::build(
        request.protocol_policy.semantic_digest.clone(),
        result.clone(),
    )
    .map_err(|_| {
        uncertain(
            "mcp_egress_invalid_result",
            external_identity_digest.clone(),
        )
    })?;
    let evidence_digest = digest_value(&serde_json::json!({
        "deployment": request.deployment,
        "operation_id": request.operation_id,
        "result_digest": closed.canonical_digest,
        "schema_version": 1,
    }))?;
    Ok(McpOperationOutcome::Completed {
        result: closed,
        evidence_digest,
    })
}

#[derive(Debug)]
struct ParsedMcpTask {
    task_id: String,
    status: String,
    poll_interval_milliseconds: Option<u64>,
}

fn parse_task_state(value: &Value) -> Result<ParsedMcpTask, McpTransportFailure> {
    let object = value
        .as_object()
        .ok_or_else(|| rejected("mcp_egress_task_state_invalid"))?;
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "taskId"
                | "status"
                | "statusMessage"
                | "createdAt"
                | "lastUpdatedAt"
                | "ttl"
                | "pollInterval"
        )
    }) || !object
        .get("ttl")
        .is_some_and(|value| value.is_null() || value.as_u64().is_some())
        || object
            .get("statusMessage")
            .is_some_and(|value| value.as_str().is_none())
    {
        return Err(rejected("mcp_egress_task_state_invalid"));
    }
    let created_at = object
        .get("createdAt")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .ok_or_else(|| rejected("mcp_egress_task_state_invalid"))?;
    let last_updated_at = object
        .get("lastUpdatedAt")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .ok_or_else(|| rejected("mcp_egress_task_state_invalid"))?;
    if last_updated_at < created_at {
        return Err(rejected("mcp_egress_task_state_invalid"));
    }
    let task_id = object
        .get("taskId")
        .and_then(Value::as_str)
        .filter(|value| valid_remote_task_id(value))
        .ok_or_else(|| rejected("mcp_egress_task_state_invalid"))?;
    let status = object
        .get("status")
        .and_then(Value::as_str)
        .filter(|value| {
            matches!(
                *value,
                "working" | "input_required" | "completed" | "failed" | "cancelled"
            )
        })
        .ok_or_else(|| rejected("mcp_egress_task_state_invalid"))?;
    let poll_interval_milliseconds = object
        .get("pollInterval")
        .map(|value| {
            value
                .as_u64()
                .filter(|value| *value > 0)
                .ok_or_else(|| rejected("mcp_egress_task_state_invalid"))
        })
        .transpose()?;
    Ok(ParsedMcpTask {
        task_id: task_id.to_owned(),
        status: status.to_owned(),
        poll_interval_milliseconds,
    })
}

fn validate_related_task(
    result: &Value,
    expected_task_id: &str,
) -> Result<(), McpTransportFailure> {
    let task_id = result
        .as_object()
        .and_then(|object| object.get("_meta"))
        .and_then(Value::as_object)
        .and_then(|meta| meta.get("io.modelcontextprotocol/related-task"))
        .and_then(Value::as_object)
        .and_then(|related| related.get("taskId"))
        .and_then(Value::as_str);
    if task_id != Some(expected_task_id) {
        return Err(rejected("mcp_egress_task_result_binding_invalid"));
    }
    Ok(())
}

fn parse_task_elicitation(
    wire: &Value,
    expected_task_id: &str,
) -> Result<PendingMcpElicitation, McpTransportFailure> {
    let object = wire
        .as_object()
        .ok_or_else(|| rejected("mcp_egress_elicitation_request_invalid"))?;
    if object.len() != 4
        || object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || object.get("method").and_then(Value::as_str) != Some("elicitation/create")
        || object.contains_key("result")
        || object.contains_key("error")
    {
        return Err(rejected("mcp_egress_elicitation_request_invalid"));
    }
    let request_id = object
        .get("id")
        .ok_or_else(|| rejected("mcp_egress_elicitation_request_id_invalid"))
        .and_then(McpJsonRpcRequestId::parse)?;
    let params = object
        .get("params")
        .and_then(Value::as_object)
        .ok_or_else(|| rejected("mcp_egress_elicitation_request_invalid"))?;
    if params.keys().any(|key| {
        !matches!(
            key.as_str(),
            "_meta" | "message" | "mode" | "requestedSchema"
        )
    }) || params
        .get("mode")
        .is_some_and(|mode| mode.as_str() != Some("form"))
    {
        return Err(rejected("mcp_egress_elicitation_request_invalid"));
    }
    let message = params
        .get("message")
        .and_then(Value::as_str)
        .filter(|message| {
            !message.is_empty()
                && message.len() <= MAX_MCP_ELICITATION_MESSAGE_BYTES
                && !message.chars().any(char::is_control)
        })
        .ok_or_else(|| rejected("mcp_egress_elicitation_message_invalid"))?;
    let _ = message;
    let related_task_id = params
        .get("_meta")
        .and_then(Value::as_object)
        .and_then(|meta| meta.get("io.modelcontextprotocol/related-task"))
        .and_then(Value::as_object)
        .and_then(|related| related.get("taskId"))
        .and_then(Value::as_str);
    if related_task_id != Some(expected_task_id) {
        return Err(rejected("mcp_egress_elicitation_task_binding_invalid"));
    }
    let response_schema = params
        .get("requestedSchema")
        .cloned()
        .ok_or_else(|| rejected("mcp_egress_elicitation_schema_invalid"))
        .and_then(|schema| {
            InteractionSchemaDocument::build_mcp_form(schema)
                .map_err(|_| rejected("mcp_egress_elicitation_schema_invalid"))
        })?;
    let pending = PendingMcpElicitation {
        request_id,
        response_schema,
    };
    pending.validate()?;
    Ok(pending)
}

fn validate_elicitation_response(
    response: &McpElicitationResponse,
    pending: &PendingMcpElicitation,
) -> Result<(), McpTransportFailure> {
    match response.action {
        McpElicitationAction::Accept => {
            let content = response
                .content
                .as_ref()
                .ok_or_else(|| rejected("mcp_egress_elicitation_response_invalid"))?;
            content
                .validate()
                .map_err(|_| rejected("mcp_egress_elicitation_response_invalid"))?;
            if content.schema_digest != pending.response_schema.canonical_digest
                || pending
                    .response_schema
                    .validate_mcp_form_instance(&content.value)
                    .is_err()
            {
                return Err(rejected("mcp_egress_elicitation_response_invalid"));
            }
        }
        McpElicitationAction::Decline | McpElicitationAction::Cancel => {
            if response.content.is_some() {
                return Err(rejected("mcp_egress_elicitation_response_invalid"));
            }
        }
    }
    Ok(())
}

fn task_ttl_milliseconds(request: &McpStreamableHttpRequest) -> Result<u64, McpTransportFailure> {
    u64::try_from((request.deadline - Utc::now()).num_milliseconds())
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| rejected("mcp_egress_deadline_elapsed"))
}

fn matching_result<'a>(wire: &'a Value, id: &str) -> Option<&'a Value> {
    let object = wire.as_object()?;
    if object.len() != 3
        || object.get("jsonrpc")?.as_str()? != "2.0"
        || object.get("id")?.as_str()? != id
        || object.contains_key("error")
    {
        return None;
    }
    object.get("result")
}

fn is_matching_jsonrpc_error(wire: &Value, id: &str) -> bool {
    let Some(object) = wire.as_object() else {
        return false;
    };
    if object.len() != 3
        || object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || object.get("id").and_then(Value::as_str) != Some(id)
        || object.contains_key("result")
    {
        return false;
    }
    let Some(error) = object.get("error").and_then(Value::as_object) else {
        return false;
    };
    error.len() >= 2
        && error.len() <= 3
        && error.get("code").and_then(Value::as_i64).is_some()
        && error.get("message").and_then(Value::as_str).is_some()
        && error
            .keys()
            .all(|key| matches!(key.as_str(), "code" | "message" | "data"))
}

fn response_json(
    response: &PinnedMcpHttpResponse,
    maximum_response_bytes: u32,
    external_identity_digest: &Sha256Digest,
) -> Result<Value, McpTransportFailure> {
    let bytes = if content_type_is(&response.headers, "application/json") {
        Cow::Borrowed(response.body.as_slice())
    } else if content_type_is(&response.headers, "text/event-stream") {
        Cow::Owned(sse_data(&response.body)?)
    } else {
        return Err(uncertain(
            "mcp_egress_invalid_content_type",
            external_identity_digest.clone(),
        ));
    };
    parse_strict_json(
        bytes.as_ref(),
        JsonLimits {
            max_bytes: maximum_response_bytes as usize,
            max_depth: 32,
            max_properties_per_object: 1_024,
            max_items_per_array: 4_096,
            max_string_bytes: maximum_response_bytes as usize,
        },
    )
    .map_err(|_| uncertain("mcp_egress_invalid_json", external_identity_digest.clone()))
}

fn sse_data(body: &[u8]) -> Result<Vec<u8>, McpTransportFailure> {
    let text = std::str::from_utf8(body)
        .map_err(|_| uncertain("mcp_egress_invalid_sse", static_external()))?;
    let event = text
        .split_once("\n\n")
        .map(|(event, _)| event)
        .or_else(|| text.split_once("\r\n\r\n").map(|(event, _)| event))
        .ok_or_else(|| uncertain("mcp_egress_incomplete_sse", static_external()))?;
    let mut data = Vec::new();
    for line in event.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty()
            || line.starts_with(':')
            || line.starts_with("event:")
            || line.starts_with("id:")
            || line.starts_with("retry:")
        {
            continue;
        }
        let Some(value) = line.strip_prefix("data:") else {
            return Err(uncertain("mcp_egress_invalid_sse", static_external()));
        };
        if !data.is_empty() {
            data.push(b'\n');
        }
        data.extend_from_slice(value.strip_prefix(' ').unwrap_or(value).as_bytes());
    }
    if data.is_empty() {
        Err(uncertain("mcp_egress_missing_sse_data", static_external()))
    } else {
        Ok(data)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum McpSseEventDisposition {
    Selected,
    Progress,
    Control,
}

fn classify_sse_event(
    event: &[u8],
    expected_response_id: Option<&str>,
    allow_elicitation_request: bool,
) -> Result<McpSseEventDisposition, McpTransportFailure> {
    if is_sse_control_event(event)? {
        return Ok(McpSseEventDisposition::Control);
    }
    let Some(expected_response_id) = expected_response_id else {
        return Ok(McpSseEventDisposition::Selected);
    };
    let data = sse_data(event)?;
    let wire = parse_strict_json(
        &data,
        JsonLimits {
            max_bytes: event.len(),
            max_depth: 32,
            max_properties_per_object: 1_024,
            max_items_per_array: 4_096,
            max_string_bytes: event.len(),
        },
    )
    .map_err(|_| uncertain("mcp_egress_invalid_json", static_external()))?;
    if matching_result(&wire, expected_response_id).is_some()
        || is_matching_jsonrpc_error(&wire, expected_response_id)
        || allow_elicitation_request && is_elicitation_request(&wire)
    {
        return Ok(McpSseEventDisposition::Selected);
    }
    if is_progress_notification(&wire) {
        return Ok(McpSseEventDisposition::Progress);
    }
    Err(uncertain(
        "mcp_egress_unexpected_sse_message",
        static_external(),
    ))
}

fn is_sse_control_event(event: &[u8]) -> Result<bool, McpTransportFailure> {
    let text = std::str::from_utf8(event)
        .map_err(|_| uncertain("mcp_egress_invalid_sse", static_external()))?;
    let mut has_control = false;
    let mut has_non_empty_data = false;
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') || line.starts_with("event:") {
            continue;
        }
        if line.starts_with("id:") || line.starts_with("retry:") {
            has_control = true;
            continue;
        }
        let Some(value) = line.strip_prefix("data:") else {
            return Err(uncertain("mcp_egress_invalid_sse", static_external()));
        };
        if !value.strip_prefix(' ').unwrap_or(value).is_empty() {
            has_non_empty_data = true;
        }
    }
    Ok(has_control && !has_non_empty_data)
}

fn is_elicitation_request(wire: &Value) -> bool {
    wire.as_object().is_some_and(|object| {
        object.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
            && object.get("method").and_then(Value::as_str) == Some("elicitation/create")
            && object.contains_key("id")
            && object.get("params").is_some_and(Value::is_object)
            && !object.contains_key("result")
            && !object.contains_key("error")
    })
}

fn is_progress_notification(wire: &Value) -> bool {
    let Some(object) = wire.as_object() else {
        return false;
    };
    if object.len() != 3
        || object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || object.get("method").and_then(Value::as_str) != Some("notifications/progress")
        || object.contains_key("id")
        || object.contains_key("result")
        || object.contains_key("error")
    {
        return false;
    }
    let Some(params) = object.get("params").and_then(Value::as_object) else {
        return false;
    };
    params.keys().all(|key| {
        matches!(
            key.as_str(),
            "_meta" | "progressToken" | "progress" | "total" | "message"
        )
    }) && params
        .get("progressToken")
        .is_some_and(|value| value.is_string() || value.is_number())
        && params.get("progress").is_some_and(Value::is_number)
        && params.get("total").is_none_or(|value| value.is_number())
        && params.get("message").is_none_or(Value::is_string)
}

fn first_complete_sse_event_end(bytes: &[u8]) -> Option<usize> {
    let lf = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| index + 2);
    let crlf = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4);
    match (lf, crlf) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn content_type_is(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case(expected))
}

fn require_success_status(
    status: StatusCode,
    external_identity: &Sha256Digest,
) -> Result<(), McpTransportFailure> {
    if status.is_success() {
        Ok(())
    } else {
        Err(uncertain(
            "mcp_egress_initialize_http_failure",
            external_identity.clone(),
        ))
    }
}

fn wire_method(method: PublishedMcpMethod) -> &'static str {
    match method {
        PublishedMcpMethod::ToolsCall => "tools/call",
        PublishedMcpMethod::ResourcesList => "resources/list",
        PublishedMcpMethod::ResourcesRead => "resources/read",
        PublishedMcpMethod::PromptsGet => "prompts/get",
        PublishedMcpMethod::TasksGet => "tasks/get",
        PublishedMcpMethod::TasksResult => "tasks/result",
        PublishedMcpMethod::TasksCancel => "tasks/cancel",
    }
}

fn method_negotiated(method: PublishedMcpMethod, capabilities: &McpNegotiatedCapabilities) -> bool {
    match method {
        PublishedMcpMethod::ToolsCall => capabilities.tools,
        PublishedMcpMethod::ResourcesList | PublishedMcpMethod::ResourcesRead => {
            capabilities.resources
        }
        PublishedMcpMethod::PromptsGet => capabilities.prompts,
        PublishedMcpMethod::TasksGet | PublishedMcpMethod::TasksResult => capabilities.tasks,
        PublishedMcpMethod::TasksCancel => capabilities.tasks_cancel,
    }
}

fn mcp_endpoint_url(endpoint: &CanonicalHttpEndpoint) -> Result<Url, EgressConfigurationError> {
    let raw = format!(
        "https://{}:{}{}",
        endpoint.host, endpoint.port, endpoint.base_path
    );
    let url = Url::parse(&raw).map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default() != Some(endpoint.port)
        || url.path() != endpoint.base_path
    {
        return Err(EgressConfigurationError::InvalidEndpoint);
    }
    Ok(url)
}

fn valid_mcp_path(path: &str) -> bool {
    path.starts_with('/')
        && path.len() <= 2_048
        && path.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~')
        })
}

fn external_identity(
    request: &McpStreamableHttpRequest,
    addresses: &[SocketAddr],
) -> Result<Sha256Digest, McpTransportFailure> {
    digest_value(&serde_json::json!({
        "addresses": addresses.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "authorization_binding_id": request.authorization_binding_id,
        "authorization_generation": request.authorization_generation,
        "deployment": request.deployment,
        "endpoint_identity_digest": request.endpoint_identity_digest,
        "idempotency_key_digest": request.idempotency_key_digest,
        "operation_id": request.operation_id,
        "schema_version": 1,
    }))
}

fn resource_refresh_external_identity(
    request: &McpResourceRefreshTransportRequest,
    addresses: &[SocketAddr],
) -> Result<Sha256Digest, McpTransportFailure> {
    digest_value(&serde_json::json!({
        "addresses": addresses.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "authorization_binding_id": request.authorization_binding_id,
        "authorization_generation": request.authorization_generation,
        "deployment": request.deployment,
        "endpoint_identity_digest": request.endpoint_identity_digest,
        "execution_identity_digest": request.execution_identity_digest,
        "job_id": request.job_id,
        "schema_version": 1,
        "subscription_id": request.subscription_id,
    }))
}

fn remote_task_identity(
    request: &McpStreamableHttpRequest,
    task_id: &str,
) -> Result<Sha256Digest, McpTransportFailure> {
    digest_value(&serde_json::json!({
        "authorization_binding_id": request.authorization_binding_id,
        "authorization_generation": request.authorization_generation,
        "deployment": request.deployment,
        "discovery_snapshot_digest": request.discovery_snapshot_digest,
        "discovery_snapshot_id": request.discovery_snapshot_id,
        "invocation_id": request.invocation_id,
        "job_id": request.job_id,
        "operation_id": request.operation_id,
        "physical_attempt": request.physical_attempt,
        "schema_version": 1,
        "task_id": task_id,
    }))
}

fn reauthorization(headers: &HeaderMap) -> McpTransportFailure {
    let challenge = headers
        .get(WWW_AUTHENTICATE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() <= 4_096 && !value.chars().any(char::is_control))
        .unwrap_or("missing");
    McpTransportFailure::ReauthorizationRequired {
        challenge_digest: digest_value(&serde_json::json!({
            "challenge": challenge,
            "schema_version": 1,
        }))
        .unwrap_or_else(|_| static_external()),
    }
}

fn rejected(code: &str) -> McpTransportFailure {
    McpTransportFailure::RejectedBeforeDispatch(safe_failure(code))
}

fn retryable(code: &str) -> McpTransportFailure {
    McpTransportFailure::RetryableBeforeDispatch(safe_failure(code))
}

fn uncertain(code: &str, external_identity_digest: Sha256Digest) -> McpTransportFailure {
    McpTransportFailure::PostDispatchUncertain {
        failure: safe_failure(code),
        external_identity_digest,
    }
}

fn safe_failure(code: &str) -> SafeMcpFailure {
    SafeMcpFailure {
        safe_code: code.to_owned(),
        safe_message: "MCP Streamable HTTP egress contract failed".to_owned(),
        evidence_digest: digest_value(&serde_json::json!({
            "domain": "mcp_streamable_http_egress",
            "safe_code": code,
            "schema_version": 1,
        }))
        .unwrap_or_else(|_| static_external()),
    }
}

fn digest_value(value: &Value) -> Result<Sha256Digest, McpTransportFailure> {
    canonical_digest(value)
        .map_err(|_| rejected("mcp_egress_canonicalization"))?
        .parse()
        .map_err(|_| rejected("mcp_egress_canonicalization"))
}

fn static_external() -> Sha256Digest {
    canonical_digest(&serde_json::json!({
        "domain": "mcp_streamable_http_egress",
        "schema_version": 1,
    }))
    .expect("static MCP egress evidence is canonical")
    .parse()
    .expect("canonical digest is SHA-256")
}

impl fmt::Debug for ReqwestMcpStreamableHttpConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReqwestMcpStreamableHttpConnector")
            .field("installed_endpoints", &self.catalog.entries.len())
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_context::ContextSubscriptionRefreshCause;
    use insight_platform_contracts::{SecretResolutionPolicy, MCP_PROTOCOL_BASELINE};
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, SanType,
    };
    use rustls::{pki_types::PrivatePkcs8KeyDer, ServerConfig};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio_rustls::TlsAcceptor;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f7{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn exact_version(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), digest(character)).unwrap()
    }

    fn exact_deployment(kind: ResourceKind, suffix: u16, character: char) -> ExactDeploymentRef {
        ExactDeploymentRef::new(id(kind, suffix), digest(character)).unwrap()
    }

    fn trusted_root_pem() -> String {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap())
            .unwrap()
            .pem()
    }

    async fn start_resource_refresh_https_fixture(
        expected_requests: usize,
    ) -> (
        SocketAddr,
        String,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
        let mut server_parameters = CertificateParams::default();
        server_parameters.subject_alt_names = vec![SanType::DnsName(
            "mcp.example.test".try_into().unwrap(),
        )];
        server_parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate().unwrap();
        let server_certificate = server_parameters.signed_by(&server_key, &ca).unwrap();
        let tls = Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![server_certificate.der().clone()],
                    PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
                )
                .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = Arc::clone(&calls);
        let task = tokio::spawn(async move {
            for _ in 0..expected_requests {
                let (stream, _) = listener.accept().await.unwrap();
                let Ok(mut stream) = TlsAcceptor::from(Arc::clone(&tls)).accept(stream).await else {
                    break;
                };
                let mut request = Vec::new();
                let header_end = loop {
                    let mut chunk = [0_u8; 1_024];
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&chunk[..read]);
                    if let Some(position) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                        break position + 4;
                    }
                };
                let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
                assert!(headers.starts_with("POST /mcp HTTP/1.1\r\n"), "{headers}");
                let lower_headers = headers.to_ascii_lowercase();
                assert!(lower_headers.contains("host: mcp.example.test:"));
                assert!(lower_headers.contains("authorization: bearer mcp-token-canary"));
                let content_length = lower_headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                while request.len() - header_end < content_length {
                    let mut chunk = [0_u8; 1_024];
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&chunk[..read]);
                }
                let body: Value = serde_json::from_slice(
                    &request[header_end..header_end + content_length],
                )
                .unwrap();
                observed_calls.fetch_add(1, Ordering::SeqCst);
                let method = body.get("method").and_then(Value::as_str).unwrap();
                let id = body.get("id").cloned();
                let (status, session, response_body) = match method {
                    "initialize" => (
                        "200 OK",
                        Some("refresh-session-canary"),
                        canonical_json(&serde_json::json!({
                            "id": id,
                            "jsonrpc": "2.0",
                            "result": {
                                "capabilities": {"resources": {}, "tools": {}},
                                "protocolVersion": MCP_PROTOCOL_BASELINE,
                                "serverInfo": {"name": "tls-fixture", "version": "1"}
                            }
                        }))
                        .unwrap(),
                    ),
                    "notifications/initialized" => ("202 Accepted", None, Vec::new()),
                    "resources/list" => (
                        "200 OK",
                        None,
                        canonical_json(&serde_json::json!({
                            "id": id,
                            "jsonrpc": "2.0",
                            "result": {"resources": [
                                {"name": "other", "uri": "mcp://untrusted.example/other"},
                                {"name": "root", "uri": "mcp://catalog.example/items/42"}
                            ]}
                        }))
                        .unwrap(),
                    ),
                    "resources/read" => (
                        "200 OK",
                        None,
                        canonical_json(&serde_json::json!({
                            "id": id,
                            "jsonrpc": "2.0",
                            "result": {"contents": [{
                                "mimeType": "text/plain",
                                "text": "TLS body canary",
                                "uri": "mcp://catalog.example/items/42"
                            }]}
                        }))
                        .unwrap(),
                    ),
                    _ => panic!("unexpected MCP method {method}"),
                };
                let session_header = session
                    .map(|value| format!("mcp-session-id: {value}\r\n"))
                    .unwrap_or_default();
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n{session_header}content-length: {}\r\nconnection: close\r\n\r\n",
                    response_body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(&response_body).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        (address, ca.pem(), calls, task)
    }

    struct FixtureSecrets {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl SecretMaterialResolver for FixtureSecrets {
        async fn resolve(
            &self,
            _tenant_id: &ResourceId,
            binding: &insight_platform_contracts::ExactSecretBindingRef,
        ) -> Result<super::super::ResolvedSecretMaterial, SecretMaterialResolutionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            super::super::ResolvedSecretMaterial::new(
                binding.secret_binding_id.clone(),
                binding.provider_id.clone(),
                binding.purpose.clone(),
                binding.binding_generation,
                digest('e'),
                b"mcp-token-canary".to_vec(),
            )
            .map_err(|_| SecretMaterialResolutionError::InvalidEvidence)
        }
    }

    struct FixtureDns {
        addresses: Vec<SocketAddr>,
    }

    #[async_trait]
    impl EgressDnsResolver for FixtureDns {
        async fn resolve(
            &self,
            _host: &str,
            _port: u16,
        ) -> Result<Vec<SocketAddr>, DnsResolutionError> {
            Ok(self.addresses.clone())
        }
    }

    struct ObservedRequest {
        body: Vec<u8>,
        session: Option<Vec<u8>>,
    }

    struct FixtureHttpTransport {
        responses: Mutex<Vec<PinnedMcpHttpResponse>>,
        observed: Mutex<Vec<ObservedRequest>>,
        calls: AtomicUsize,
    }

    struct SubscriptionFixtureHttpTransport {
        post: FixtureHttpTransport,
        sse_calls: AtomicUsize,
        observed_sse_sessions: Mutex<Vec<Option<Vec<u8>>>>,
        observed_last_event_ids: Mutex<Vec<Option<Vec<u8>>>>,
    }

    #[async_trait]
    impl PinnedMcpHttpTransport for SubscriptionFixtureHttpTransport {
        async fn round_trip(
            &self,
            request: PinnedMcpHttpRequest,
        ) -> Result<PinnedMcpHttpResponse, McpTransportFailure> {
            self.post.round_trip(request).await
        }

        async fn open_sse(
            &self,
            request: PinnedMcpSseRequest,
        ) -> Result<PinnedMcpSseResponse, McpTransportFailure> {
            self.sse_calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.url.as_str(), "https://mcp.example.com/mcp");
            assert_eq!(request.dns_host, "mcp.example.com");
            assert_eq!(
                request.headers.get(ACCEPT).unwrap().as_bytes(),
                b"text/event-stream"
            );
            assert!(request.headers.get(CONTENT_TYPE).is_none());
            self.observed_sse_sessions.lock().unwrap().push(
                request
                    .headers
                    .get(MCP_SESSION_HEADER)
                    .map(|value| value.as_bytes().to_vec()),
            );
            self.observed_last_event_ids.lock().unwrap().push(
                request
                    .headers
                    .get(LAST_EVENT_ID_HEADER)
                    .map(|value| value.as_bytes().to_vec()),
            );
            Ok(PinnedMcpSseResponse {
                status: StatusCode::OK,
                headers: headers(Some("text/event-stream"), None),
                chunks: futures::stream::iter(vec![Ok(
                    b"retry: 1\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/resources/list_changed\"}\n\n"
                        .to_vec(),
                )])
                .boxed(),
            })
        }
    }

    #[derive(Default)]
    struct RecordingSubscriptionSink {
        notifications: Mutex<Vec<McpStreamableHttpSubscriptionNotification>>,
        terminations: Mutex<Vec<McpStreamableHttpSubscriptionTermination>>,
        terminated: tokio::sync::Notify,
    }

    #[async_trait]
    impl McpStreamableHttpSubscriptionSink for RecordingSubscriptionSink {
        async fn ingest_notification(
            &self,
            notification: McpStreamableHttpSubscriptionNotification,
        ) -> Result<(), McpStreamableHttpSubscriptionSinkError> {
            self.notifications.lock().unwrap().push(notification);
            Ok(())
        }

        async fn report_termination(
            &self,
            termination: McpStreamableHttpSubscriptionTermination,
        ) -> Result<(), McpStreamableHttpSubscriptionSinkError> {
            self.terminations.lock().unwrap().push(termination);
            self.terminated.notify_one();
            Ok(())
        }
    }

    impl FixtureHttpTransport {
        fn new(mut responses: Vec<PinnedMcpHttpResponse>) -> Self {
            responses.reverse();
            Self {
                responses: Mutex::new(responses),
                observed: Mutex::new(Vec::new()),
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl PinnedMcpHttpTransport for FixtureHttpTransport {
        async fn round_trip(
            &self,
            request: PinnedMcpHttpRequest,
        ) -> Result<PinnedMcpHttpResponse, McpTransportFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.url.as_str(), "https://mcp.example.com/mcp");
            assert_eq!(request.dns_host, "mcp.example.com");
            assert_eq!(
                request.addresses,
                vec![SocketAddr::new(
                    IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 8, 8)),
                    443
                )]
            );
            assert_eq!(
                request.headers.get(AUTHORIZATION).unwrap().as_bytes(),
                b"Bearer mcp-token-canary"
            );
            assert!(request.headers.get(AUTHORIZATION).unwrap().is_sensitive());
            assert_eq!(
                request
                    .headers
                    .get(MCP_PROTOCOL_VERSION_HEADER)
                    .unwrap()
                    .as_bytes(),
                MCP_PROTOCOL_BASELINE.as_bytes()
            );
            let session = request
                .headers
                .get(MCP_SESSION_HEADER)
                .map(|value| value.as_bytes().to_vec());
            self.observed.lock().unwrap().push(ObservedRequest {
                body: request.body,
                session,
            });
            let mut response = self
                .responses
                .lock()
                .unwrap()
                .pop()
                .ok_or_else(|| rejected("fixture_response_missing"))?;
            if content_type_is(&response.headers, "text/event-stream") {
                let mut remaining = response.body;
                let mut progress_events = 0_u32;
                let mut control_events = 0_u16;
                loop {
                    let event_end = first_complete_sse_event_end(&remaining)
                        .ok_or_else(|| rejected("fixture_sse_incomplete"))?;
                    let event = remaining[..event_end].to_vec();
                    remaining.drain(..event_end);
                    match classify_sse_event(
                        &event,
                        request.expected_response_id.as_deref(),
                        request.allow_elicitation_request,
                    )? {
                        McpSseEventDisposition::Selected => {
                            response.body = event;
                            break;
                        }
                        McpSseEventDisposition::Progress => {
                            progress_events = progress_events.saturating_add(1);
                            if progress_events > request.maximum_progress_events {
                                return Err(rejected("fixture_progress_limit_exceeded"));
                            }
                        }
                        McpSseEventDisposition::Control => {
                            control_events = control_events.saturating_add(1);
                            if control_events > MAX_MCP_SSE_CONTROL_EVENTS {
                                return Err(rejected("fixture_control_limit_exceeded"));
                            }
                        }
                    }
                }
            }
            Ok(response)
        }
    }

    struct Fixture {
        entry: InstalledMcpStreamableHttpEndpoint,
        request: McpStreamableHttpRequest,
    }

    fn fixture() -> Fixture {
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "mcp.example.com".to_owned(),
            port: 443,
            base_path: "/mcp".to_owned(),
        };
        let deployment = exact_deployment(ResourceKind::McpDeployment, 1, '1');
        let protocol_policy = exact_version(ResourceKind::PolicyRevision, 2, '2');
        let network_policy = exact_version(ResourceKind::PolicyRevision, 3, '3');
        let tls_policy = exact_version(ResourceKind::PolicyRevision, 4, '4');
        let trust_policy = exact_version(ResourceKind::PolicyRevision, 5, '5');
        let auth_policy = exact_version(ResourceKind::PolicyRevision, 6, '6');
        let purpose: SecretPurpose = "mcp_access_token".parse().unwrap();
        let token_secret_binding = insight_platform_contracts::ExactSecretBindingRef::build(
            id(ResourceKind::SecretBinding, 7),
            3,
            id(ResourceKind::SecretProvider, 8),
            purpose.clone(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: digest('e'),
            },
        )
        .unwrap();
        let params = ClosedJsonValue::build(
            digest('9'),
            serde_json::json!({"arguments": {"city": "Shanghai"}, "name": "weather"}),
        )
        .unwrap();
        let endpoint_identity_digest = endpoint.canonical_digest().unwrap();
        Fixture {
            entry: InstalledMcpStreamableHttpEndpoint {
                schema_version: 1,
                deployment: deployment.clone(),
                endpoint: endpoint.clone(),
                endpoint_identity_digest: endpoint_identity_digest.clone(),
                server_identity_digest: digest('a'),
                protocol_policy: protocol_policy.clone(),
                network_policy: network_policy.clone(),
                tls_policy: tls_policy.clone(),
                trust_policy: trust_policy.clone(),
                auth_policy: auth_policy.clone(),
                token_credential_purpose: purpose,
                trusted_root_pem: trusted_root_pem(),
            },
            request: McpStreamableHttpRequest {
                tenant_id: id(ResourceKind::Tenant, 10),
                deployment,
                endpoint,
                endpoint_identity_digest,
                server_identity_digest: digest('a'),
                protocol_policy,
                network_policy,
                tls_policy,
                trust_policy,
                auth_policy: Some(auth_policy),
                authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 11),
                authorization_generation: 4,
                principal_binding_generation: 5,
                token_secret_binding,
                protocol_version: MCP_PROTOCOL_BASELINE.to_owned(),
                client_capabilities: McpClientCapabilities {
                    elicitation_form: false,
                    elicitation_url: false,
                    tasks_elicitation_create: false,
                    sampling: false,
                    roots: false,
                },
                negotiated_capabilities: McpNegotiatedCapabilities {
                    tools: true,
                    resources: false,
                    prompts: false,
                    logging: false,
                    tasks: false,
                    tasks_list: false,
                    tasks_cancel: false,
                    tasks_tools_call: false,
                    elicitation: false,
                    sampling: false,
                    roots: false,
                    subscriptions: false,
                },
                operation_id: id(ResourceKind::McpOperation, 12),
                invocation_id: id(ResourceKind::CapabilityInvocation, 13),
                job_id: id(ResourceKind::Job, 14),
                physical_attempt: 1,
                discovery_snapshot_id: id(ResourceKind::McpDiscoverySnapshot, 15),
                discovery_snapshot_digest: digest('f'),
                method: PublishedMcpMethod::ToolsCall,
                params,
                task_requested: false,
                continuation: None,
                task_limits: None,
                idempotency_key_digest: digest('b'),
                deadline: Utc::now() + chrono::Duration::seconds(30),
                transport_deadline: Utc::now() + chrono::Duration::seconds(30),
                maximum_request_bytes: 16_384,
                maximum_response_bytes: 16_384,
                maximum_headers: 32,
                maximum_sse_event_bytes: 8_192,
                maximum_progress_events: 16,
                idle_timeout_milliseconds: 2_000,
                initialize_timeout_milliseconds: 2_000,
                request_timeout_milliseconds: 5_000,
            },
        }
    }

    fn headers(content_type: Option<&'static str>, session: Option<&'static str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(content_type) = content_type {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        }
        if let Some(session) = session {
            headers.insert(MCP_SESSION_HEADER, HeaderValue::from_static(session));
        }
        headers
    }

    fn response(status: StatusCode, headers: HeaderMap, body: Vec<u8>) -> PinnedMcpHttpResponse {
        PinnedMcpHttpResponse {
            status,
            headers,
            body,
        }
    }

    fn initialized_responses(
        fixture: &Fixture,
        operation_body: Vec<u8>,
    ) -> Vec<PinnedMcpHttpResponse> {
        let initialize_id = format!("{}:initialize", fixture.request.operation_id);
        vec![
            response(
                StatusCode::OK,
                headers(Some("application/json"), Some("opaque-session-canary")),
                serde_json::to_vec(&serde_json::json!({
                    "id": initialize_id,
                    "jsonrpc": "2.0",
                    "result": {
                        "capabilities": {"tools": {}},
                        "protocolVersion": MCP_PROTOCOL_BASELINE,
                        "serverInfo": {"name": "fixture", "version": "1"}
                    }
                }))
                .unwrap(),
            ),
            response(StatusCode::ACCEPTED, HeaderMap::new(), Vec::new()),
            response(
                StatusCode::OK,
                headers(Some("text/event-stream; charset=utf-8"), None),
                operation_body,
            ),
        ]
    }

    fn enable_remote_tasks(request: &mut McpStreamableHttpRequest) {
        request.negotiated_capabilities.tasks = true;
        request.negotiated_capabilities.tasks_list = true;
        request.negotiated_capabilities.tasks_cancel = true;
        request.negotiated_capabilities.tasks_tools_call = true;
        request.task_requested = true;
        request.task_limits = Some(McpRemoteTaskLimits {
            maximum_get_request_bytes: 4_096,
            maximum_get_response_bytes: 8_192,
            maximum_result_request_bytes: 4_096,
            maximum_result_response_bytes: 16_384,
            maximum_cancel_request_bytes: Some(4_096),
            maximum_cancel_response_bytes: Some(8_192),
            maximum_get_progress_events: 16,
            maximum_result_progress_events: 16,
            maximum_cancel_progress_events: Some(16),
            minimum_poll_milliseconds: 1,
            maximum_poll_milliseconds: 10_000,
        });
    }

    fn enable_remote_task_elicitation(request: &mut McpStreamableHttpRequest) {
        enable_remote_tasks(request);
        request.client_capabilities.elicitation_form = true;
        request.client_capabilities.tasks_elicitation_create = true;
        request.negotiated_capabilities.elicitation = true;
    }

    fn task_capabilities() -> Value {
        serde_json::json!({
            "tools": {},
            "tasks": {
                "list": {},
                "cancel": {},
                "requests": {"tools": {"call": {}}}
            }
        })
    }

    fn initialized_task_responses(
        fixture: &Fixture,
        operation_body: Vec<u8>,
    ) -> Vec<PinnedMcpHttpResponse> {
        let initialize_id = format!("{}:initialize", fixture.request.operation_id);
        vec![
            response(
                StatusCode::OK,
                headers(Some("application/json"), Some("opaque-task-session-canary")),
                serde_json::to_vec(&serde_json::json!({
                    "id": initialize_id,
                    "jsonrpc": "2.0",
                    "result": {
                        "capabilities": task_capabilities(),
                        "protocolVersion": MCP_PROTOCOL_BASELINE,
                        "serverInfo": {"name": "fixture", "version": "1"}
                    }
                }))
                .unwrap(),
            ),
            response(StatusCode::ACCEPTED, HeaderMap::new(), Vec::new()),
            response(
                StatusCode::OK,
                headers(Some("application/json"), None),
                operation_body,
            ),
        ]
    }

    fn connector(
        fixture: &Fixture,
        secrets: Arc<FixtureSecrets>,
        dns: Arc<FixtureDns>,
        transport: Arc<FixtureHttpTransport>,
    ) -> ReqwestMcpStreamableHttpConnector {
        connector_with_codec(
            fixture,
            secrets,
            dns,
            transport,
            remote_task_codec("remote-task-key-1", '7', 7),
        )
    }

    fn remote_task_codec(
        active_key_id: &str,
        digest_character: char,
        key_byte: u8,
    ) -> Arc<AeadMcpRemoteTaskStateCodec> {
        Arc::new(
            AeadMcpRemoteTaskStateCodec::new(
                active_key_id.to_owned(),
                vec![McpRemoteTaskStateKey {
                    key_id: active_key_id.to_owned(),
                    key_reference_digest: digest(digest_character),
                    key_material: SensitiveMcpRemoteTaskStateKey::new(vec![key_byte; 32]).unwrap(),
                }],
            )
            .unwrap(),
        )
    }

    fn connector_with_codec(
        fixture: &Fixture,
        secrets: Arc<FixtureSecrets>,
        dns: Arc<FixtureDns>,
        transport: Arc<FixtureHttpTransport>,
        remote_tasks: Arc<AeadMcpRemoteTaskStateCodec>,
    ) -> ReqwestMcpStreamableHttpConnector {
        ReqwestMcpStreamableHttpConnector::with_transport(
            InstalledMcpStreamableHttpEndpointCatalog::new(vec![fixture.entry.clone()]).unwrap(),
            secrets,
            dns,
            transport,
            remote_tasks,
            McpStreamableHttpEgressLimits {
                maximum_in_flight: 2,
                maximum_dns_answers: 4,
                maximum_secret_material_bytes: 128,
                maximum_subscription_reconnects: 4,
                maximum_subscription_events_per_session: 128,
            },
        )
        .unwrap()
    }

    fn public_dns() -> Arc<FixtureDns> {
        Arc::new(FixtureDns {
            addresses: vec![SocketAddr::new(
                IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 8, 8)),
                443,
            )],
        })
    }

    fn secrets() -> Arc<FixtureSecrets> {
        Arc::new(FixtureSecrets {
            calls: AtomicUsize::new(0),
        })
    }

    fn resource_refresh_request(fixture: &Fixture) -> McpResourceRefreshTransportRequest {
        let resource_uri = "mcp://catalog.example/items/42".to_owned();
        McpResourceRefreshTransportRequest {
            schema_version: 1,
            tenant_id: fixture.request.tenant_id.clone(),
            deployment: fixture.request.deployment.clone(),
            endpoint: fixture.request.endpoint.clone(),
            endpoint_identity_digest: fixture.request.endpoint_identity_digest.clone(),
            server_identity_digest: fixture.request.server_identity_digest.clone(),
            protocol_policy: fixture.request.protocol_policy.clone(),
            network_policy: fixture.request.network_policy.clone(),
            tls_policy: fixture.request.tls_policy.clone(),
            trust_policy: fixture.request.trust_policy.clone(),
            auth_policy: fixture.request.auth_policy.clone(),
            authorization_binding_id: fixture.request.authorization_binding_id.clone(),
            authorization_generation: fixture.request.authorization_generation,
            principal_binding_generation: fixture.request.principal_binding_generation,
            token_secret_binding: fixture.request.token_secret_binding.clone(),
            protocol_version: fixture.request.protocol_version.clone(),
            client_capabilities: fixture.request.client_capabilities.clone(),
            negotiated_capabilities: McpNegotiatedCapabilities {
                resources: true,
                ..fixture.request.negotiated_capabilities.clone()
            },
            subscription_id: id(ResourceKind::McpOperation, 0x61),
            job_id: id(ResourceKind::Job, 0x62),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 0x63),
            lease_generation: 4,
            attempt_number: 2,
            discovery_snapshot_id: fixture.request.discovery_snapshot_id.clone(),
            discovery_snapshot_digest: fixture.request.discovery_snapshot_digest.clone(),
            execution_identity_digest: digest('c'),
            request_digest: digest('d'),
            resource_uri_digest: canonical_digest(&Value::String(resource_uri.clone()))
                .unwrap()
                .parse()
                .unwrap(),
            resource_uri,
            cause: ContextSubscriptionRefreshCause::FullReconcile {
                observed_subscription_version: 7,
            },
            deadline: Utc::now() + chrono::Duration::seconds(30),
            list_limits: Some(insight_platform_mcp_host::McpResourceRefreshMethodLimits {
                maximum_request_bytes: 4_096,
                maximum_response_bytes: 16_384,
                maximum_pages: 4,
            }),
            read_limits: insight_platform_mcp_host::McpResourceRefreshMethodLimits {
                maximum_request_bytes: 4_096,
                maximum_response_bytes: 16_384,
                maximum_pages: 1,
            },
            maximum_resources: 16,
            maximum_items: 32,
            maximum_total_bytes: 65_536,
            maximum_headers: 32,
            maximum_sse_event_bytes: 8_192,
            idle_timeout_milliseconds: 2_000,
            initialize_timeout_milliseconds: 2_000,
            request_timeout_milliseconds: 5_000,
        }
    }

    #[tokio::test]
    async fn full_resource_refresh_lists_then_reads_only_the_frozen_root() {
        let fixture = fixture();
        let request = resource_refresh_request(&fixture);
        let initialize_id = format!("{}:{}:refresh:initialize", request.job_id, request.attempt_number);
        let list_id = format!("{}:{}:refresh:list:1", request.job_id, request.attempt_number);
        let read_id = format!("{}:{}:refresh:read", request.job_id, request.attempt_number);
        let transport = Arc::new(FixtureHttpTransport::new(vec![
            response(
                StatusCode::OK,
                headers(Some("application/json"), Some("refresh-session-canary")),
                serde_json::to_vec(&serde_json::json!({
                    "id": initialize_id,
                    "jsonrpc": "2.0",
                    "result": {
                        "capabilities": {"resources": {}, "tools": {}},
                        "protocolVersion": MCP_PROTOCOL_BASELINE,
                        "serverInfo": {"name": "fixture", "version": "1"}
                    }
                }))
                .unwrap(),
            ),
            response(StatusCode::ACCEPTED, HeaderMap::new(), Vec::new()),
            response(
                StatusCode::OK,
                headers(Some("application/json"), None),
                serde_json::to_vec(&serde_json::json!({
                    "id": list_id,
                    "jsonrpc": "2.0",
                    "result": {"resources": [
                        {"name": "other", "uri": "mcp://untrusted.example/other"},
                        {"name": "root", "uri": request.resource_uri}
                    ]}
                }))
                .unwrap(),
            ),
            response(
                StatusCode::OK,
                headers(Some("application/json"), None),
                serde_json::to_vec(&serde_json::json!({
                    "id": read_id,
                    "jsonrpc": "2.0",
                    "result": {"contents": [
                        {"mimeType": "text/plain", "text": "secret body canary", "uri": request.resource_uri},
                        {"blob": "AAEC", "uri": request.resource_uri}
                    ]}
                }))
                .unwrap(),
            ),
        ]));
        let connector = connector(&fixture, secrets(), public_dns(), transport.clone());
        let evidence = connector.refresh_resources(request.clone()).await.unwrap();
        assert_eq!(evidence.execution_identity_digest, request.execution_identity_digest);
        assert_eq!(evidence.request_digest, request.request_digest);
        assert_eq!(evidence.resource_count, 1);
        assert_eq!(evidence.item_count, 2);
        assert!(evidence.byte_count > 0);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 4);
        let observed = transport.observed.lock().unwrap();
        let bodies = observed
            .iter()
            .map(|request| serde_json::from_slice::<Value>(&request.body).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(bodies[2].get("method").and_then(Value::as_str), Some("resources/list"));
        assert_eq!(bodies[3].get("method").and_then(Value::as_str), Some("resources/read"));
        assert_eq!(
            bodies[3]
                .pointer("/params/uri")
                .and_then(Value::as_str),
            Some(request.resource_uri.as_str())
        );
        assert!(observed.iter().all(|request| {
            !String::from_utf8_lossy(&request.body).contains("untrusted.example/other")
                || String::from_utf8_lossy(&request.body).contains("resources/list")
        }));
    }

    #[tokio::test]
    async fn resource_refresh_https_uses_only_the_installed_trust_bundle() {
        let (address, root_pem, calls, server) = start_resource_refresh_https_fixture(4).await;
        let mut fixture = fixture();
        fixture.entry.endpoint.host = "mcp.example.test".to_owned();
        fixture.entry.endpoint.port = address.port();
        fixture.entry.endpoint_identity_digest = fixture.entry.endpoint.canonical_digest().unwrap();
        fixture.entry.trusted_root_pem = root_pem;
        fixture.request.endpoint = fixture.entry.endpoint.clone();
        fixture.request.endpoint_identity_digest = fixture.entry.endpoint_identity_digest.clone();
        let request = resource_refresh_request(&fixture);
        let connector = ReqwestMcpStreamableHttpConnector::new(
            InstalledMcpStreamableHttpEndpointCatalog::new(vec![fixture.entry.clone()]).unwrap(),
            secrets(),
            Arc::new(FixtureDns {
                addresses: vec![address],
            }),
            remote_task_codec("tls-fixture-key", '7', 7),
            McpStreamableHttpEgressLimits::default(),
        )
        .unwrap()
        .allow_loopback_for_protocol_fixture();
        let evidence = connector.refresh_resources(request).await.unwrap();
        assert_eq!(evidence.resource_count, 1);
        assert_eq!(evidence.item_count, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        server.await.unwrap();

        let (address, _server_root, calls, server) = start_resource_refresh_https_fixture(1).await;
        let mut wrong = fixture;
        wrong.entry.endpoint.port = address.port();
        wrong.entry.endpoint_identity_digest = wrong.entry.endpoint.canonical_digest().unwrap();
        wrong.entry.trusted_root_pem = trusted_root_pem();
        wrong.request.endpoint = wrong.entry.endpoint.clone();
        wrong.request.endpoint_identity_digest = wrong.entry.endpoint_identity_digest.clone();
        let request = resource_refresh_request(&wrong);
        let connector = ReqwestMcpStreamableHttpConnector::new(
            InstalledMcpStreamableHttpEndpointCatalog::new(vec![wrong.entry.clone()]).unwrap(),
            secrets(),
            Arc::new(FixtureDns {
                addresses: vec![address],
            }),
            remote_task_codec("wrong-tls-fixture-key", '8', 8),
            McpStreamableHttpEgressLimits::default(),
        )
        .unwrap()
        .allow_loopback_for_protocol_fixture();
        assert!(connector.refresh_resources(request).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn connector_initializes_acknowledges_and_executes_with_one_pinned_session() {
        let fixture = fixture();
        let operation_id = fixture.request.operation_id.to_string();
        let event = format!(
            "data: {{\"jsonrpc\":\"2.0\",\"id\":\"{operation_id}\",\"result\":{{\"content\":[]}}}}\n\n"
        )
        .into_bytes();
        let transport = Arc::new(FixtureHttpTransport::new(initialized_responses(
            &fixture, event,
        )));
        let connector = connector(&fixture, secrets(), public_dns(), transport.clone());
        let outcome = connector.execute(fixture.request.clone()).await.unwrap();
        let McpOperationOutcome::Completed { ref result, .. } = outcome else {
            panic!("expected completed MCP operation")
        };
        assert_eq!(result.value, serde_json::json!({"content": []}));

        let observed = transport.observed.lock().unwrap();
        assert_eq!(observed.len(), 3);
        let initialize: Value = serde_json::from_slice(&observed[0].body).unwrap();
        assert_eq!(initialize["method"], "initialize");
        assert_eq!(
            initialize["params"]["protocolVersion"],
            MCP_PROTOCOL_BASELINE
        );
        assert!(observed[0].session.is_none());
        let initialized: Value = serde_json::from_slice(&observed[1].body).unwrap();
        assert_eq!(initialized["method"], "notifications/initialized");
        assert_eq!(
            observed[1].session.as_deref(),
            Some(b"opaque-session-canary".as_slice())
        );
        let operation: Value = serde_json::from_slice(&observed[2].body).unwrap();
        assert_eq!(operation["method"], "tools/call");
        assert_eq!(operation["params"]["name"], "weather");
        assert_eq!(
            observed[2].session.as_deref(),
            Some(b"opaque-session-canary".as_slice())
        );
        let debug = format!("{outcome:?}");
        assert!(!debug.contains("mcp-token-canary"));
        assert!(!debug.contains("opaque-session-canary"));
    }

    #[tokio::test]
    async fn mixed_public_private_dns_is_rejected_before_secret_or_transport() {
        let fixture = fixture();
        let secrets = secrets();
        let transport = Arc::new(FixtureHttpTransport::new(Vec::new()));
        let initial_connector = connector(
            &fixture,
            secrets.clone(),
            Arc::new(FixtureDns {
                addresses: vec![
                    SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 8, 8)), 443),
                    SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 443),
                ],
            }),
            transport.clone(),
        );
        let failure = initial_connector
            .execute(fixture.request)
            .await
            .unwrap_err();
        assert!(matches!(
            failure,
            McpTransportFailure::RejectedBeforeDispatch(_)
        ));
        assert_eq!(secrets.calls.load(Ordering::SeqCst), 0);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn authorization_challenge_is_digest_only_and_stops_before_operation() {
        let fixture = fixture();
        let mut challenge_headers = headers(Some("application/json"), None);
        challenge_headers.insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer scope=expanded-canary"),
        );
        let transport = Arc::new(FixtureHttpTransport::new(vec![response(
            StatusCode::UNAUTHORIZED,
            challenge_headers,
            Vec::new(),
        )]));
        let connector = connector(&fixture, secrets(), public_dns(), transport.clone());
        let failure = connector.execute(fixture.request).await.unwrap_err();
        assert!(matches!(
            failure,
            McpTransportFailure::ReauthorizationRequired { .. }
        ));
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        assert!(!format!("{failure:?}").contains("expanded-canary"));
    }

    #[tokio::test]
    async fn duplicate_jsonrpc_field_and_unsealed_remote_task_fail_closed() {
        let fixture = fixture();
        let operation_id = fixture.request.operation_id.to_string();
        let duplicate = format!(
            "data: {{\"jsonrpc\":\"2.0\",\"id\":\"{operation_id}\",\"id\":\"{operation_id}\",\"result\":{{}}}}\n\n"
        )
        .into_bytes();
        let duplicate_connector = connector(
            &fixture,
            secrets(),
            public_dns(),
            Arc::new(FixtureHttpTransport::new(initialized_responses(
                &fixture, duplicate,
            ))),
        );
        assert!(matches!(
            duplicate_connector
                .execute(fixture.request.clone())
                .await
                .unwrap_err(),
            McpTransportFailure::PostDispatchUncertain { .. }
        ));

        let task = format!(
            "data: {{\"jsonrpc\":\"2.0\",\"id\":\"{operation_id}\",\"result\":{{\"task\":{{\"taskId\":\"remote-secret-id\"}}}}}}\n\n"
        )
        .into_bytes();
        let task_connector = connector(
            &fixture,
            secrets(),
            public_dns(),
            Arc::new(FixtureHttpTransport::new(initialized_responses(
                &fixture, task,
            ))),
        );
        let failure = task_connector.execute(fixture.request).await.unwrap_err();
        assert!(matches!(
            failure,
            McpTransportFailure::PostDispatchUncertain { .. }
        ));
        assert!(!format!("{failure:?}").contains("remote-secret-id"));
    }

    #[tokio::test]
    async fn remote_task_is_sealed_polled_and_completed_on_the_original_session() {
        let mut task_fixture = fixture();
        enable_remote_tasks(&mut task_fixture.request);
        let operation_id = task_fixture.request.operation_id.to_string();
        let task_id = "remote-task-secret-canary";
        let initial = serde_json::to_vec(&serde_json::json!({
            "id": operation_id,
            "jsonrpc": "2.0",
            "result": {"task": {
                "taskId": task_id,
                "status": "working",
                "createdAt": "2026-08-12T00:00:00Z",
                "lastUpdatedAt": "2026-08-12T00:00:00Z",
                "ttl": 30000,
                "pollInterval": 1
            }}
        }))
        .unwrap();
        let initial_transport = Arc::new(FixtureHttpTransport::new(initialized_task_responses(
            &task_fixture,
            initial,
        )));
        let initial_connector = connector(
            &task_fixture,
            secrets(),
            public_dns(),
            initial_transport.clone(),
        );
        let first = initial_connector
            .execute(task_fixture.request.clone())
            .await
            .unwrap();
        let McpOperationOutcome::RemoteTask {
            encrypted_state,
            external_identity_digest,
            ..
        } = first
        else {
            panic!("expected remote Task")
        };
        assert!(!format!("{encrypted_state:?}").contains(task_id));
        {
            let initial_observed = initial_transport.observed.lock().unwrap();
            let tool: Value = serde_json::from_slice(&initial_observed[2].body).unwrap();
            assert!(tool["params"]["task"]["ttl"].as_u64().is_some());
        }

        task_fixture.request.task_requested = false;
        task_fixture.request.continuation =
            Some(insight_platform_mcp_host::McpOperationContinuation {
                encrypted_state,
                external_identity_digest,
                poll_count: 1,
                elicitation_response: None,
            });
        let poll_id = format!("{}:poll:1", task_fixture.request.operation_id);
        let result_id = format!("{}:result:1", task_fixture.request.operation_id);
        let completed = response(
            StatusCode::OK,
            headers(Some("application/json"), None),
            serde_json::to_vec(&serde_json::json!({
                "id": poll_id,
                "jsonrpc": "2.0",
                "result": {
                    "taskId": task_id,
                    "status": "completed",
                    "createdAt": "2026-08-12T00:00:00Z",
                    "lastUpdatedAt": "2026-08-12T00:00:01Z",
                    "ttl": 30000,
                    "pollInterval": 1
                }
            }))
            .unwrap(),
        );
        let result = response(
            StatusCode::OK,
            headers(Some("application/json"), None),
            serde_json::to_vec(&serde_json::json!({
                "id": result_id,
                "jsonrpc": "2.0",
                "result": {
                    "content": [{"type": "text", "text": "done"}],
                    "isError": false,
                    "_meta": {"io.modelcontextprotocol/related-task": {"taskId": task_id}}
                }
            }))
            .unwrap(),
        );
        let resume_transport = Arc::new(FixtureHttpTransport::new(vec![completed, result]));
        let resume_connector = connector(
            &task_fixture,
            secrets(),
            public_dns(),
            resume_transport.clone(),
        );
        let outcome = resume_connector
            .execute(task_fixture.request.clone())
            .await
            .unwrap();
        assert!(matches!(outcome, McpOperationOutcome::Completed { .. }));
        let observed = resume_transport.observed.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert_eq!(
            observed[0].session.as_deref(),
            Some(b"opaque-task-session-canary".as_slice())
        );
        assert_eq!(
            observed[1].session.as_deref(),
            Some(b"opaque-task-session-canary".as_slice())
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&observed[0].body).unwrap()["method"],
            "tasks/get"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&observed[1].body).unwrap()["method"],
            "tasks/result"
        );
    }

    #[tokio::test]
    async fn remote_task_elicitation_resumes_exact_request_task_schema_and_session() {
        let mut task_fixture = fixture();
        enable_remote_task_elicitation(&mut task_fixture.request);
        let operation_id = task_fixture.request.operation_id.to_string();
        let task_id = "remote-task-input-secret-canary";
        let initial = serde_json::to_vec(&serde_json::json!({
            "id": operation_id,
            "jsonrpc": "2.0",
            "result": {"task": {
                "taskId": task_id,
                "status": "working",
                "createdAt": "2026-08-12T00:00:00Z",
                "lastUpdatedAt": "2026-08-12T00:00:00Z",
                "ttl": 30000,
                "pollInterval": 1
            }}
        }))
        .unwrap();
        let initial_transport = Arc::new(FixtureHttpTransport::new(initialized_task_responses(
            &task_fixture,
            initial,
        )));
        let initial_connector = connector(
            &task_fixture,
            secrets(),
            public_dns(),
            initial_transport.clone(),
        );
        let McpOperationOutcome::RemoteTask {
            encrypted_state,
            external_identity_digest,
            ..
        } = initial_connector
            .execute(task_fixture.request.clone())
            .await
            .unwrap()
        else {
            panic!("expected remote Task")
        };
        let initialized: Value =
            serde_json::from_slice(&initial_transport.observed.lock().unwrap()[0].body).unwrap();
        assert!(initialized["params"]["capabilities"]["elicitation"]["form"].is_object());
        assert!(
            initialized["params"]["capabilities"]["tasks"]["requests"]["elicitation"]["create"]
                .is_object()
        );

        task_fixture.request.task_requested = false;
        task_fixture.request.continuation =
            Some(insight_platform_mcp_host::McpOperationContinuation {
                encrypted_state,
                external_identity_digest,
                poll_count: 1,
                elicitation_response: None,
            });
        let poll_id = format!("{}:poll:1", task_fixture.request.operation_id);
        let input_required = response(
            StatusCode::OK,
            headers(Some("application/json"), None),
            serde_json::to_vec(&serde_json::json!({
                "id": poll_id,
                "jsonrpc": "2.0",
                "result": {
                    "taskId": task_id,
                    "status": "input_required",
                    "createdAt": "2026-08-12T00:00:00Z",
                    "lastUpdatedAt": "2026-08-12T00:00:01Z",
                    "ttl": 30000,
                    "pollInterval": 1
                }
            }))
            .unwrap(),
        );
        let requested_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "city": {"type": "string", "minLength": 1, "maxLength": 64},
                "units": {"type": "string", "enum": ["celsius", "fahrenheit"]}
            },
            "required": ["city", "units"]
        });
        let elicitation_wire = serde_json::to_vec(&serde_json::json!({
            "id": 77,
            "jsonrpc": "2.0",
            "method": "elicitation/create",
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/related-task": {"taskId": task_id}
                },
                "mode": "form",
                "message": "Choose weather units",
                "requestedSchema": requested_schema
            }
        }))
        .unwrap();
        let mut elicitation_sse = b"data: ".to_vec();
        elicitation_sse.extend_from_slice(&elicitation_wire);
        elicitation_sse.extend_from_slice(b"\n\n");
        let elicitation = response(
            StatusCode::OK,
            headers(Some("text/event-stream"), None),
            elicitation_sse,
        );
        let input_transport =
            Arc::new(FixtureHttpTransport::new(vec![input_required, elicitation]));
        let input_connector = connector(
            &task_fixture,
            secrets(),
            public_dns(),
            input_transport.clone(),
        );
        let McpOperationOutcome::InputRequired {
            encrypted_state,
            external_identity_digest,
            safe_prompt_key,
            response_schema,
            response_schema_digest,
            ..
        } = input_connector
            .execute(task_fixture.request.clone())
            .await
            .unwrap()
        else {
            panic!("expected platform input request")
        };
        assert_eq!(safe_prompt_key, "mcp_task_input_required");
        assert_eq!(response_schema.schema, requested_schema);
        assert_eq!(response_schema.canonical_digest, response_schema_digest);
        {
            let observed = input_transport.observed.lock().unwrap();
            assert_eq!(observed.len(), 2);
            assert!(observed.iter().all(|request| {
                request.session.as_deref() == Some(b"opaque-task-session-canary".as_slice())
            }));
            assert_eq!(
                serde_json::from_slice::<Value>(&observed[1].body).unwrap()["method"],
                "tasks/result"
            );
        }

        for (action, expected_action) in [
            (McpElicitationAction::Accept, "accept"),
            (McpElicitationAction::Decline, "decline"),
            (McpElicitationAction::Cancel, "cancel"),
        ] {
            let content = (action == McpElicitationAction::Accept)
                .then(|| {
                    ClosedJsonValue::build(
                        response_schema_digest.clone(),
                        serde_json::json!({"city": "Shanghai", "units": "celsius"}),
                    )
                })
                .transpose()
                .unwrap();
            task_fixture.request.continuation =
                Some(insight_platform_mcp_host::McpOperationContinuation {
                    encrypted_state: encrypted_state.clone(),
                    external_identity_digest: external_identity_digest.clone(),
                    poll_count: 1,
                    elicitation_response: Some(McpElicitationResponse { action, content }),
                });
            let response_transport = Arc::new(FixtureHttpTransport::new(vec![response(
                StatusCode::ACCEPTED,
                HeaderMap::new(),
                Vec::new(),
            )]));
            let response_connector = connector(
                &task_fixture,
                secrets(),
                public_dns(),
                response_transport.clone(),
            );
            assert!(matches!(
                response_connector
                    .execute(task_fixture.request.clone())
                    .await
                    .unwrap(),
                McpOperationOutcome::RemoteTask { .. }
            ));
            let observed = response_transport.observed.lock().unwrap();
            assert_eq!(observed.len(), 1);
            assert_eq!(
                observed[0].session.as_deref(),
                Some(b"opaque-task-session-canary".as_slice())
            );
            let response: Value = serde_json::from_slice(&observed[0].body).unwrap();
            assert_eq!(response["id"], 77);
            assert_eq!(response["result"]["action"], expected_action);
            if action == McpElicitationAction::Accept {
                assert_eq!(response["result"]["content"]["city"], "Shanghai");
                assert_eq!(response["result"]["content"]["units"], "celsius");
            } else {
                assert!(response["result"].get("content").is_none());
            }
        }
    }

    #[tokio::test]
    async fn task_result_skips_bounded_progress_before_elicitation() {
        let mut task_fixture = fixture();
        enable_remote_task_elicitation(&mut task_fixture.request);
        let operation_id = task_fixture.request.operation_id.to_string();
        let task_id = "remote-task-progress-before-input";
        let initial = serde_json::to_vec(&serde_json::json!({
            "id": operation_id,
            "jsonrpc": "2.0",
            "result": {"task": {
                "taskId": task_id,
                "status": "working",
                "createdAt": "2026-08-12T00:00:00Z",
                "lastUpdatedAt": "2026-08-12T00:00:00Z",
                "ttl": 30000,
                "pollInterval": 1
            }}
        }))
        .unwrap();
        let initial_transport = Arc::new(FixtureHttpTransport::new(initialized_task_responses(
            &task_fixture,
            initial,
        )));
        let initial_connector =
            connector(&task_fixture, secrets(), public_dns(), initial_transport);
        let McpOperationOutcome::RemoteTask {
            encrypted_state,
            external_identity_digest,
            ..
        } = initial_connector
            .execute(task_fixture.request.clone())
            .await
            .unwrap()
        else {
            panic!("expected remote Task")
        };
        task_fixture.request.task_requested = false;
        task_fixture.request.continuation =
            Some(insight_platform_mcp_host::McpOperationContinuation {
                encrypted_state,
                external_identity_digest,
                poll_count: 1,
                elicitation_response: None,
            });
        let poll_id = format!("{}:poll:1", task_fixture.request.operation_id);
        let input_required = response(
            StatusCode::OK,
            headers(Some("application/json"), None),
            serde_json::to_vec(&serde_json::json!({
                "id": poll_id,
                "jsonrpc": "2.0",
                "result": {
                    "taskId": task_id,
                    "status": "input_required",
                    "createdAt": "2026-08-12T00:00:00Z",
                    "lastUpdatedAt": "2026-08-12T00:00:01Z",
                    "ttl": 30000,
                    "pollInterval": 1
                }
            }))
            .unwrap(),
        );
        let progress = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": {"progressToken": "poll-1", "progress": 1, "total": 2}
        });
        let elicitation = serde_json::json!({
            "id": "elicit-after-progress",
            "jsonrpc": "2.0",
            "method": "elicitation/create",
            "params": {
                "_meta": {"io.modelcontextprotocol/related-task": {"taskId": task_id}},
                "mode": "form",
                "message": "Choose a city",
                "requestedSchema": {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                    "properties": {"city": {"type": "string", "maxLength": 64}},
                    "required": ["city"]
                }
            }
        });
        let body = format!("data: {progress}\n\ndata: {elicitation}\n\n").into_bytes();
        let transport = Arc::new(FixtureHttpTransport::new(vec![
            input_required,
            response(
                StatusCode::OK,
                headers(Some("text/event-stream"), None),
                body,
            ),
        ]));
        let connector = connector(&task_fixture, secrets(), public_dns(), transport);
        assert!(matches!(
            connector.execute(task_fixture.request).await.unwrap(),
            McpOperationOutcome::InputRequired { .. }
        ));
    }

    #[test]
    fn task_elicitation_rejects_wrong_task_and_sensitive_form_schema() {
        let task_id = "remote-task-input";
        let wire = serde_json::json!({
            "id": "elicit-1",
            "jsonrpc": "2.0",
            "method": "elicitation/create",
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/related-task": {"taskId": "different-task"}
                },
                "mode": "form",
                "message": "Choose a city",
                "requestedSchema": {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                    "properties": {"city": {"type": "string", "maxLength": 64}},
                    "required": ["city"]
                }
            }
        });
        assert!(parse_task_elicitation(&wire, task_id).is_err());

        let mut sensitive = wire;
        sensitive["params"]["_meta"]["io.modelcontextprotocol/related-task"]["taskId"] =
            Value::String(task_id.to_owned());
        sensitive["params"]["requestedSchema"]["properties"] = serde_json::json!({
            "access_token": {"type": "string", "maxLength": 64}
        });
        sensitive["params"]["requestedSchema"]["required"] = serde_json::json!(["access_token"]);
        assert!(parse_task_elicitation(&sensitive, task_id).is_err());
    }

    #[tokio::test]
    async fn remote_task_cancel_uses_exact_task_and_original_session() {
        let mut task_fixture = fixture();
        enable_remote_tasks(&mut task_fixture.request);
        let task_id = "remote-task-cancel-secret-canary";
        let claims = McpRemoteTaskStateClaims {
            schema_version: 1,
            tenant_id: task_fixture.request.tenant_id.clone(),
            deployment: task_fixture.request.deployment.clone(),
            authorization_binding_id: task_fixture.request.authorization_binding_id.clone(),
            authorization_generation: task_fixture.request.authorization_generation,
            principal_binding_generation: task_fixture.request.principal_binding_generation,
            invocation_id: task_fixture.request.invocation_id.clone(),
            job_id: task_fixture.request.job_id.clone(),
            physical_attempt: task_fixture.request.physical_attempt,
            discovery_snapshot_id: task_fixture.request.discovery_snapshot_id.clone(),
            discovery_snapshot_digest: task_fixture.request.discovery_snapshot_digest.clone(),
            protocol_policy: task_fixture.request.protocol_policy.clone(),
            operation_id: task_fixture.request.operation_id.clone(),
            task_id: task_id.as_bytes().to_vec(),
            session_id: Some(b"opaque-task-cancel-session".to_vec()),
            pending_elicitation: None,
            external_identity_digest: remote_task_identity(&task_fixture.request, task_id).unwrap(),
            deadline_epoch_milliseconds: task_fixture.request.deadline.timestamp_millis(),
        };
        task_fixture.request.task_requested = false;
        task_fixture.request.continuation =
            Some(insight_platform_mcp_host::McpOperationContinuation {
                encrypted_state: remote_task_codec("remote-task-key-1", '7', 7)
                    .seal(&claims)
                    .unwrap(),
                external_identity_digest: claims.external_identity_digest.clone(),
                poll_count: 2,
                elicitation_response: None,
            });
        let cancel_id = format!("{}:cancel:2", task_fixture.request.operation_id);
        let transport = Arc::new(FixtureHttpTransport::new(vec![response(
            StatusCode::OK,
            headers(Some("application/json"), None),
            serde_json::to_vec(&serde_json::json!({
                "id": cancel_id,
                "jsonrpc": "2.0",
                "result": {
                    "taskId": task_id,
                    "status": "cancelled",
                    "createdAt": "2026-08-12T00:00:00Z",
                    "lastUpdatedAt": "2026-08-12T00:00:01Z",
                    "ttl": 30000,
                    "pollInterval": 1
                }
            }))
            .unwrap(),
        )]));
        let task_connector = connector(&task_fixture, secrets(), public_dns(), transport.clone());
        assert_eq!(
            task_connector
                .cancel_remote_task(task_fixture.request.clone())
                .await
                .unwrap(),
            McpRemoteTaskCancelOutcome::Accepted
        );
        {
            let observed = transport.observed.lock().unwrap();
            assert_eq!(observed.len(), 1);
            assert_eq!(
                observed[0].session.as_deref(),
                Some(b"opaque-task-cancel-session".as_slice())
            );
            let body: Value = serde_json::from_slice(&observed[0].body).unwrap();
            assert_eq!(body["method"], "tasks/cancel");
            assert_eq!(body["params"]["taskId"], task_id);
        }

        let mut drifted = task_fixture.request;
        drifted.authorization_generation += 1;
        let no_network = Arc::new(FixtureHttpTransport::new(Vec::new()));
        let clean_fixture = fixture();
        let task_connector = connector(&clean_fixture, secrets(), public_dns(), no_network.clone());
        assert!(matches!(
            task_connector
                .cancel_remote_task(drifted)
                .await
                .unwrap_err(),
            McpTransportFailure::RejectedBeforeDispatch(_)
        ));
        assert_eq!(no_network.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn remote_task_binding_key_and_ciphertext_drift_fail_before_poll() {
        let mut fixture = fixture();
        enable_remote_tasks(&mut fixture.request);
        let task_id = "remote-task-secret-canary";
        let claims = McpRemoteTaskStateClaims {
            schema_version: 1,
            tenant_id: fixture.request.tenant_id.clone(),
            deployment: fixture.request.deployment.clone(),
            authorization_binding_id: fixture.request.authorization_binding_id.clone(),
            authorization_generation: fixture.request.authorization_generation,
            principal_binding_generation: fixture.request.principal_binding_generation,
            invocation_id: fixture.request.invocation_id.clone(),
            job_id: fixture.request.job_id.clone(),
            physical_attempt: fixture.request.physical_attempt,
            discovery_snapshot_id: fixture.request.discovery_snapshot_id.clone(),
            discovery_snapshot_digest: fixture.request.discovery_snapshot_digest.clone(),
            protocol_policy: fixture.request.protocol_policy.clone(),
            operation_id: fixture.request.operation_id.clone(),
            task_id: task_id.as_bytes().to_vec(),
            session_id: Some(b"opaque-task-session-canary".to_vec()),
            pending_elicitation: None,
            external_identity_digest: remote_task_identity(&fixture.request, task_id).unwrap(),
            deadline_epoch_milliseconds: fixture.request.deadline.timestamp_millis(),
        };
        let codec = remote_task_codec("remote-task-key-1", '7', 7);
        let encrypted_state = codec.seal(&claims).unwrap();
        fixture.request.task_requested = false;
        fixture.request.continuation = Some(insight_platform_mcp_host::McpOperationContinuation {
            encrypted_state,
            external_identity_digest: claims.external_identity_digest.clone(),
            poll_count: 1,
            elicitation_response: None,
        });

        for mutate in ["binding", "ciphertext", "key"] {
            let mut request = fixture.request.clone();
            let selected_codec = match mutate {
                "binding" => {
                    request.authorization_generation += 1;
                    codec.clone()
                }
                "ciphertext" => {
                    request
                        .continuation
                        .as_mut()
                        .unwrap()
                        .encrypted_state
                        .ciphertext[NONCE_LEN] ^= 1;
                    codec.clone()
                }
                "key" => remote_task_codec("remote-task-key-2", '8', 8),
                _ => unreachable!(),
            };
            let transport = Arc::new(FixtureHttpTransport::new(Vec::new()));
            let connector = connector_with_codec(
                &fixture,
                secrets(),
                public_dns(),
                transport.clone(),
                selected_codec,
            );
            assert!(matches!(
                connector.execute(request).await.unwrap_err(),
                McpTransportFailure::RejectedBeforeDispatch(_)
            ));
            assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
        }
        assert!(!format!("{:?}", fixture.request.continuation).contains(task_id));
    }

    #[test]
    fn remote_task_key_rotation_reads_old_state_and_writes_only_with_the_active_key() {
        let mut fixture = fixture();
        enable_remote_tasks(&mut fixture.request);
        let task_id = "remote-task-rotation-canary";
        let claims = McpRemoteTaskStateClaims {
            schema_version: 1,
            tenant_id: fixture.request.tenant_id.clone(),
            deployment: fixture.request.deployment.clone(),
            authorization_binding_id: fixture.request.authorization_binding_id.clone(),
            authorization_generation: fixture.request.authorization_generation,
            principal_binding_generation: fixture.request.principal_binding_generation,
            invocation_id: fixture.request.invocation_id.clone(),
            job_id: fixture.request.job_id.clone(),
            physical_attempt: fixture.request.physical_attempt,
            discovery_snapshot_id: fixture.request.discovery_snapshot_id.clone(),
            discovery_snapshot_digest: fixture.request.discovery_snapshot_digest.clone(),
            protocol_policy: fixture.request.protocol_policy.clone(),
            operation_id: fixture.request.operation_id.clone(),
            task_id: task_id.as_bytes().to_vec(),
            session_id: Some(b"opaque-rotation-session".to_vec()),
            pending_elicitation: None,
            external_identity_digest: remote_task_identity(&fixture.request, task_id).unwrap(),
            deadline_epoch_milliseconds: fixture.request.deadline.timestamp_millis(),
        };
        let old = remote_task_codec("remote-task-key-old", '6', 6);
        let old_state = old.seal(&claims).unwrap();
        let rotated = AeadMcpRemoteTaskStateCodec::new(
            "remote-task-key-new".to_owned(),
            vec![
                McpRemoteTaskStateKey {
                    key_id: "remote-task-key-new".to_owned(),
                    key_reference_digest: digest('7'),
                    key_material: SensitiveMcpRemoteTaskStateKey::new(vec![7; 32]).unwrap(),
                },
                McpRemoteTaskStateKey {
                    key_id: "remote-task-key-old".to_owned(),
                    key_reference_digest: digest('6'),
                    key_material: SensitiveMcpRemoteTaskStateKey::new(vec![6; 32]).unwrap(),
                },
            ],
        )
        .unwrap();
        assert_eq!(rotated.open(&old_state).unwrap(), claims);
        let new_state = rotated.seal(&claims).unwrap();
        assert_eq!(new_state.key_id, "remote-task-key-new");
        assert_ne!(new_state.ciphertext, old_state.ciphertext);
        assert!(!format!("{claims:?}").contains(task_id));
    }

    #[test]
    fn remote_task_timestamps_are_rfc3339_and_monotonic() {
        let valid = serde_json::json!({
            "taskId": "task-1",
            "status": "working",
            "createdAt": "2026-08-12T00:00:00Z",
            "lastUpdatedAt": "2026-08-12T00:00:01Z",
            "ttl": null,
            "pollInterval": 100
        });
        assert!(parse_task_state(&valid).is_ok());
        for invalid in [
            serde_json::json!({
                "taskId": "task-1", "status": "working", "createdAt": "not-a-time",
                "lastUpdatedAt": "2026-08-12T00:00:01Z", "ttl": null
            }),
            serde_json::json!({
                "taskId": "task-1", "status": "working", "createdAt": "2026-08-12T00:00:02Z",
                "lastUpdatedAt": "2026-08-12T00:00:01Z", "ttl": 1000
            }),
        ] {
            assert!(matches!(
                parse_task_state(&invalid).unwrap_err(),
                McpTransportFailure::RejectedBeforeDispatch(_)
            ));
        }
    }

    #[test]
    fn endpoint_catalog_rejects_http_ambiguous_path_and_duplicate_deployment() {
        let fixture = fixture();
        let mut http = fixture.entry.clone();
        http.endpoint.scheme = CapabilityEndpointScheme::Http;
        http.endpoint_identity_digest = http.endpoint.canonical_digest().unwrap();
        assert_eq!(
            http.validate(),
            Err(EgressConfigurationError::InvalidEndpoint)
        );

        let mut ambiguous = fixture.entry.clone();
        ambiguous.endpoint.base_path = "/mcp/%2e%2e".to_owned();
        ambiguous.endpoint_identity_digest = ambiguous.endpoint.canonical_digest().unwrap();
        assert_eq!(
            ambiguous.validate(),
            Err(EgressConfigurationError::InvalidEndpoint)
        );
        assert_eq!(
            InstalledMcpStreamableHttpEndpointCatalog::new(vec![
                fixture.entry.clone(),
                fixture.entry
            ])
            .unwrap_err(),
            EgressConfigurationError::DuplicateEndpoint
        );
    }

    #[test]
    fn sse_parser_joins_standard_multiline_data_and_rejects_unknown_fields() {
        assert_eq!(
            sse_data(b"event: message\ndata: {\"a\":\ndata: 1}\n\n").unwrap(),
            b"{\"a\":\n1}".to_vec()
        );
        assert!(sse_data(b"unknown: value\ndata: {}\n\n").is_err());
    }

    #[test]
    fn subscription_sse_parser_is_bounded_and_preserves_resume_fields() {
        let parsed = parse_subscription_sse_event(
            b"id: event-7\nretry: 250\ndata: {\"jsonrpc\":\"2.0\",\ndata: \"method\":\"notifications/resources/list_changed\"}\n\n",
            4_096,
        )
        .unwrap();
        assert_eq!(parsed.event_id.as_deref(), Some(b"event-7".as_slice()));
        assert_eq!(parsed.retry_milliseconds, Some(250));
        assert_eq!(
            parsed.wire.as_deref(),
            Some(
                b"{\"jsonrpc\":\"2.0\",\n\"method\":\"notifications/resources/list_changed\"}"
                    .as_slice()
            )
        );
        assert!(parse_subscription_sse_event(b"id: one\nid: two\n\n", 4_096).is_err());
        assert!(parse_subscription_sse_event(b"retry: 999999\n\n", 4_096).is_err());
        assert!(parse_subscription_sse_event(b"data: {}\n\n", 4).is_err());
        assert!(parse_subscription_sse_event(b": heartbeat\n\n", 4_096).is_ok());
    }

    #[test]
    fn subscription_event_identity_is_stable_across_redelivery() {
        let fixture = fixture();
        let request = subscription_request(&fixture);
        let first = subscription_event_key(&request, Some(b"event-1"), 1).unwrap();
        let replay = subscription_event_key(&request, Some(b"event-1"), 99).unwrap();
        let next = subscription_event_key(&request, Some(b"event-2"), 2).unwrap();
        let anonymous_first = subscription_event_key(&request, None, 1).unwrap();
        let anonymous_next = subscription_event_key(&request, None, 2).unwrap();
        assert_eq!(first, replay);
        assert_ne!(first, next);
        assert_ne!(anonymous_first, anonymous_next);
    }

    #[tokio::test]
    async fn subscription_opens_stream_only_after_activation_and_reports_unresumable_close() {
        let fixture = fixture();
        let request = subscription_request(&fixture);
        let initialize_id = format!("{}:initialize", request.subscription_id);
        let subscribe_id = format!("{}:subscribe", request.subscription_id);
        let transport = Arc::new(SubscriptionFixtureHttpTransport {
            post: FixtureHttpTransport::new(vec![
                response(
                    StatusCode::OK,
                    headers(
                        Some("application/json"),
                        Some("opaque-subscription-session-canary"),
                    ),
                    serde_json::to_vec(&serde_json::json!({
                        "id": initialize_id,
                        "jsonrpc": "2.0",
                        "result": {
                            "capabilities": {
                                "resources": {"subscribe": true},
                                "tools": {}
                            },
                            "protocolVersion": MCP_PROTOCOL_BASELINE,
                            "serverInfo": {"name": "fixture", "version": "1"}
                        }
                    }))
                    .unwrap(),
                ),
                response(StatusCode::ACCEPTED, HeaderMap::new(), Vec::new()),
                response(
                    StatusCode::OK,
                    headers(Some("application/json"), None),
                    serde_json::to_vec(&serde_json::json!({
                        "id": subscribe_id,
                        "jsonrpc": "2.0",
                        "result": {}
                    }))
                    .unwrap(),
                ),
            ]),
            sse_calls: AtomicUsize::new(0),
            observed_sse_sessions: Mutex::new(Vec::new()),
            observed_last_event_ids: Mutex::new(Vec::new()),
        });
        let session_codec = Arc::new(
            AeadMcpSubscriptionStateCodec::new(
                "subscription-key".to_owned(),
                vec![McpSubscriptionStateKey {
                    key_id: "subscription-key".to_owned(),
                    key_reference_digest: digest('d'),
                    key_material: SensitiveMcpSubscriptionStateKey::new(vec![17; 32]).unwrap(),
                }],
            )
            .unwrap(),
        );
        let sink = Arc::new(RecordingSubscriptionSink::default());
        let connector = ReqwestMcpStreamableHttpSubscriptionConnector::with_transport(
            InstalledMcpStreamableHttpEndpointCatalog::new(vec![fixture.entry.clone()]).unwrap(),
            secrets(),
            public_dns(),
            transport.clone(),
            session_codec.clone(),
            sink.clone(),
            McpStreamableHttpEgressLimits {
                maximum_in_flight: 2,
                maximum_dns_answers: 4,
                maximum_secret_material_bytes: 128,
                maximum_subscription_reconnects: 4,
                maximum_subscription_events_per_session: 128,
            },
        )
        .unwrap();

        let prepared = connector
            .establish_subscription(request.clone())
            .await
            .unwrap();
        assert_eq!(transport.post.calls.load(Ordering::SeqCst), 3);
        assert_eq!(transport.sse_calls.load(Ordering::SeqCst), 0);
        let claims = session_codec
            .open(&prepared.established.encrypted_opaque_session)
            .unwrap();
        assert_eq!(claims.subscription_id, request.subscription_id);
        assert_eq!(claims.session_generation, request.session_generation);
        assert_eq!(
            claims.session_id.as_deref(),
            Some(b"opaque-subscription-session-canary".as_slice())
        );

        prepared.activate().await;
        tokio::time::timeout(Duration::from_secs(1), sink.terminated.notified())
            .await
            .unwrap();
        assert_eq!(transport.sse_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            transport.observed_sse_sessions.lock().unwrap().as_slice(),
            &[Some(b"opaque-subscription-session-canary".to_vec())]
        );
        assert_eq!(
            transport.observed_last_event_ids.lock().unwrap().as_slice(),
            &[None]
        );
        let notifications = sink.notifications.lock().unwrap();
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].subscription_id, request.subscription_id);
        assert_eq!(
            notifications[0].session_generation,
            request.session_generation
        );
        assert_eq!(notifications[0].event_generation, 1);
        drop(notifications);
        let terminations = sink.terminations.lock().unwrap();
        assert_eq!(terminations.len(), 1);
        assert_eq!(terminations[0].subscription_id, request.subscription_id);
        assert_eq!(
            terminations[0].worker_process_generation_id,
            request.worker_process_generation_id
        );
        assert!(matches!(
            &terminations[0].failure,
            McpTransportFailure::PostDispatchUncertain { failure, .. }
                if failure.safe_code == "mcp_egress_subscription_stream_not_resumable"
        ));
    }

    #[test]
    fn subscription_session_state_is_encrypted_and_generation_bound() {
        let codec = AeadMcpSubscriptionStateCodec::new(
            "subscription-key".to_owned(),
            vec![McpSubscriptionStateKey {
                key_id: "subscription-key".to_owned(),
                key_reference_digest: digest('d'),
                key_material: SensitiveMcpSubscriptionStateKey::new(vec![17; 32]).unwrap(),
            }],
        )
        .unwrap();
        let fixture = fixture();
        let request = subscription_request(&fixture);
        let claims = McpSubscriptionStateClaims {
            schema_version: 1,
            tenant_id: request.tenant_id,
            deployment: request.deployment,
            authorization_binding_id: request.authorization_binding_id,
            authorization_generation: request.authorization_generation,
            principal_binding_generation: request.principal_binding_generation,
            subscription_id: request.subscription_id,
            session_generation: request.session_generation,
            binding_digest: request.binding_digest,
            session_id: Some(b"opaque-subscription-session-canary".to_vec()),
            external_identity_digest: digest('e'),
            expires_at_epoch_milliseconds: request.deadline.timestamp_millis(),
        };
        let encrypted = codec.seal(&claims).unwrap();
        assert!(!encrypted
            .ciphertext
            .windows(b"opaque-subscription-session-canary".len())
            .any(|window| window == b"opaque-subscription-session-canary"));
        assert_eq!(codec.open(&encrypted).unwrap(), claims);
        let debug = format!("{claims:?}");
        assert!(!debug.contains("opaque-subscription-session-canary"));
    }

    #[cfg(any())]
    #[tokio::test]
    async fn managed_sandbox_session_state_uses_a_domain_separated_egress_key_boundary() {
        let codec = AeadMcpSubscriptionStateCodec::new(
            "subscription-key".to_owned(),
            vec![McpSubscriptionStateKey {
                key_id: "subscription-key".to_owned(),
                key_reference_digest: digest('d'),
                key_material: SensitiveMcpSubscriptionStateKey::new(vec![17; 32]).unwrap(),
            }],
        )
        .unwrap();
        let physical_job_id = id(ResourceKind::Job, 91);
        let mut identity = insight_platform_mcp_host::ManagedMcpSandboxSessionIdentity {
            schema_version: 1,
            tenant_id: id(ResourceKind::Tenant, 90),
            subscription_id: id(ResourceKind::McpOperation, 92),
            logical_job_id: id(ResourceKind::Job, 93),
            admitted_subscription_version: 1,
            admitted_logical_job_version: 2,
            session_generation: 3,
            sandbox_job_id: ResourceId::from_uuid_v7(ResourceKind::Job, physical_job_id.uuid())
                .unwrap(),
            physical_job_id,
            subscription_binding_digest: digest('a'),
            canonical_digest: digest('0'),
        };
        let mut identity_value = serde_json::to_value(&identity).unwrap();
        identity_value
            .as_object_mut()
            .unwrap()
            .remove("canonical_digest");
        identity.canonical_digest = canonical_digest(&identity_value).unwrap().parse().unwrap();
        let now = Utc::now();
        let claims = ManagedMcpSandboxOpaqueSessionClaims {
            schema_version: 1,
            identity,
            request_digest: digest('b'),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 94),
            provider_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 95),
            lease_generation: 4,
            sandbox_identity_digest: digest('c'),
            protocol_evidence_digest: digest('e'),
            established_at: now,
            expires_at: now + chrono::Duration::minutes(1),
        };
        let expected_digest = claims.canonical_digest().unwrap();
        let encrypted = codec
            .seal_managed_mcp_sandbox_session_state(claims)
            .await
            .unwrap();
        assert_eq!(encrypted.plaintext_digest, expected_digest);
        assert_eq!(encrypted.scheme, MANAGED_MCP_SANDBOX_SESSION_STATE_SCHEME);
        assert!(codec.open(&encrypted).is_err());
    }

    fn subscription_request(fixture: &Fixture) -> McpStreamableHttpSubscriptionRequest {
        let resource_uri = "mcp://catalog.example/items/42".to_owned();
        McpStreamableHttpSubscriptionRequest {
            tenant_id: fixture.request.tenant_id.clone(),
            deployment: fixture.request.deployment.clone(),
            endpoint: fixture.request.endpoint.clone(),
            endpoint_identity_digest: fixture.request.endpoint_identity_digest.clone(),
            server_identity_digest: fixture.request.server_identity_digest.clone(),
            protocol_policy: fixture.request.protocol_policy.clone(),
            network_policy: fixture.request.network_policy.clone(),
            tls_policy: fixture.request.tls_policy.clone(),
            trust_policy: fixture.request.trust_policy.clone(),
            auth_policy: fixture.request.auth_policy.clone(),
            authorization_binding_id: fixture.request.authorization_binding_id.clone(),
            authorization_generation: fixture.request.authorization_generation,
            principal_binding_generation: fixture.request.principal_binding_generation,
            token_secret_binding: fixture.request.token_secret_binding.clone(),
            protocol_version: fixture.request.protocol_version.clone(),
            client_capabilities: fixture.request.client_capabilities.clone(),
            negotiated_capabilities: McpNegotiatedCapabilities {
                resources: true,
                subscriptions: true,
                ..fixture.request.negotiated_capabilities.clone()
            },
            subscription_id: id(ResourceKind::McpOperation, 21),
            binding_digest: digest('c'),
            session_generation: 3,
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 22),
            resource_uri_digest: canonical_digest(&Value::String(resource_uri.clone()))
                .unwrap()
                .parse()
                .unwrap(),
            resource_uri,
            deadline: Utc::now() + chrono::Duration::minutes(5),
            maximum_message_bytes: 16_384,
            maximum_response_bytes: 16_384,
            maximum_headers: 32,
            maximum_sse_event_bytes: 8_192,
            idle_timeout_milliseconds: 2_000,
            initialize_timeout_milliseconds: 2_000,
            request_timeout_milliseconds: 5_000,
            maximum_session_milliseconds: 300_000,
        }
    }
}
