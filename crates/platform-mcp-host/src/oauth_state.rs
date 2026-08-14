use super::{
    AuthenticatedMcpOAuthState, McpOAuthCallbackAuthorityError, McpOAuthCallbackError,
    McpOAuthStateAuthenticator, SensitiveOAuthValue, MAX_MCP_OAUTH_STATE_BYTES,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{ResourceId, Sha256Digest};
use ring::{
    aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN},
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, sync::Arc};

pub const MCP_OAUTH_STATE_KEY_BYTES: usize = 32;
pub const MAX_MCP_OAUTH_STATE_KEYS: usize = 4;
pub const MAX_MCP_OAUTH_STATE_KEY_ID_BYTES: usize = 32;
pub const MAX_MCP_OAUTH_STATE_LIFETIME_SECONDS: i64 = 3_600;
pub const MAX_MCP_OAUTH_STATE_CLOCK_SKEW_SECONDS: i64 = 120;

const STATE_TOKEN_VERSION: &str = "s1";
const STATE_AAD_DOMAIN: &[u8] = b"insight.platform/v1/mcp-oauth-state\0";

/// Construction-only state-encryption key. The caller-provided allocation is zeroed after the
/// cryptographic key schedule has been created.
pub struct SensitiveMcpOAuthStateKey(Vec<u8>);

impl SensitiveMcpOAuthStateKey {
    pub fn new(material: Vec<u8>) -> Result<Self, McpOAuthStateConfigurationError> {
        if material.len() != MCP_OAUTH_STATE_KEY_BYTES {
            return Err(McpOAuthStateConfigurationError::InvalidKeyMaterial);
        }
        Ok(Self(material))
    }

    fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SensitiveMcpOAuthStateKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveMcpOAuthStateKey")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for SensitiveMcpOAuthStateKey {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

pub struct McpOAuthStateKey {
    pub key_id: String,
    pub key_material: SensitiveMcpOAuthStateKey,
}

impl fmt::Debug for McpOAuthStateKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthStateKey")
            .field("key_id", &self.key_id)
            .field("key_material", &self.key_material)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthStateCodecConfig {
    pub active_key_id: String,
    pub callback_binding_digest: Sha256Digest,
    pub maximum_lifetime_seconds: i64,
    pub clock_skew_seconds: i64,
}

impl McpOAuthStateCodecConfig {
    pub fn validate(&self) -> Result<(), McpOAuthStateConfigurationError> {
        if !valid_key_id(&self.active_key_id)
            || self.maximum_lifetime_seconds <= 0
            || self.maximum_lifetime_seconds > MAX_MCP_OAUTH_STATE_LIFETIME_SECONDS
            || self.clock_skew_seconds < 0
            || self.clock_skew_seconds > MAX_MCP_OAUTH_STATE_CLOCK_SKEW_SECONDS
        {
            return Err(McpOAuthStateConfigurationError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthStateConfigurationError {
    InvalidConfiguration,
    InvalidKeyMaterial,
    InvalidKeySet,
}

impl fmt::Display for McpOAuthStateConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "MCP OAuth state configuration is invalid",
            Self::InvalidKeyMaterial => "MCP OAuth state key material is invalid",
            Self::InvalidKeySet => "MCP OAuth state key set is invalid",
        })
    }
}

impl std::error::Error for McpOAuthStateConfigurationError {}

pub trait McpOAuthStateIssuer: Send + Sync {
    fn issue_state(
        &self,
        identity: &AuthenticatedMcpOAuthState,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<SensitiveOAuthValue, McpOAuthCallbackError>;
}

trait McpOAuthStateNonceSource: Send + Sync {
    fn fill(&self, nonce: &mut [u8]) -> Result<(), ()>;
}

struct SystemMcpOAuthStateNonceSource(SystemRandom);

impl Default for SystemMcpOAuthStateNonceSource {
    fn default() -> Self {
        Self(SystemRandom::new())
    }
}

impl McpOAuthStateNonceSource for SystemMcpOAuthStateNonceSource {
    fn fill(&self, nonce: &mut [u8]) -> Result<(), ()> {
        self.0.fill(nonce).map_err(|_| ())
    }
}

pub struct AeadMcpOAuthStateCodec {
    config: McpOAuthStateCodecConfig,
    keys: BTreeMap<String, LessSafeKey>,
    nonce_source: Arc<dyn McpOAuthStateNonceSource>,
}

impl AeadMcpOAuthStateCodec {
    pub fn new(
        config: McpOAuthStateCodecConfig,
        keys: Vec<McpOAuthStateKey>,
    ) -> Result<Self, McpOAuthStateConfigurationError> {
        Self::with_nonce_source(
            config,
            keys,
            Arc::new(SystemMcpOAuthStateNonceSource::default()),
        )
    }

    fn with_nonce_source(
        config: McpOAuthStateCodecConfig,
        keys: Vec<McpOAuthStateKey>,
        nonce_source: Arc<dyn McpOAuthStateNonceSource>,
    ) -> Result<Self, McpOAuthStateConfigurationError> {
        config.validate()?;
        if keys.is_empty() || keys.len() > MAX_MCP_OAUTH_STATE_KEYS {
            return Err(McpOAuthStateConfigurationError::InvalidKeySet);
        }
        let mut indexed = BTreeMap::new();
        for key in keys {
            if !valid_key_id(&key.key_id) {
                return Err(McpOAuthStateConfigurationError::InvalidKeySet);
            }
            let unbound = UnboundKey::new(&AES_256_GCM, key.key_material.expose())
                .map_err(|_| McpOAuthStateConfigurationError::InvalidKeyMaterial)?;
            if indexed
                .insert(key.key_id, LessSafeKey::new(unbound))
                .is_some()
            {
                return Err(McpOAuthStateConfigurationError::InvalidKeySet);
            }
        }
        if !indexed.contains_key(&config.active_key_id) {
            return Err(McpOAuthStateConfigurationError::InvalidKeySet);
        }
        Ok(Self {
            config,
            keys: indexed,
            nonce_source,
        })
    }

    fn aad(&self, key_id: &str) -> Vec<u8> {
        let mut aad = Vec::with_capacity(
            STATE_AAD_DOMAIN.len()
                + key_id.len()
                + self.config.callback_binding_digest.as_str().len()
                + 2,
        );
        aad.extend_from_slice(STATE_AAD_DOMAIN);
        aad.extend_from_slice(key_id.as_bytes());
        aad.push(0);
        aad.extend_from_slice(self.config.callback_binding_digest.as_str().as_bytes());
        aad
    }

    fn authenticate_state(
        &self,
        state: &SensitiveOAuthValue,
        now: DateTime<Utc>,
    ) -> Result<AuthenticatedMcpOAuthState, McpOAuthCallbackAuthorityError> {
        let raw = std::str::from_utf8(state.as_bytes())
            .map_err(|_| McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        let mut parts = raw.split('.');
        if parts.next() != Some(STATE_TOKEN_VERSION) {
            return Err(McpOAuthCallbackAuthorityError::NotFoundOrChanged);
        }
        let key_id = parts
            .next()
            .filter(|value| valid_key_id(value))
            .ok_or(McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        let encoded = parts
            .next()
            .filter(|value| !value.is_empty())
            .ok_or(McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        if parts.next().is_some() {
            return Err(McpOAuthCallbackAuthorityError::NotFoundOrChanged);
        }
        let key = self
            .keys
            .get(key_id)
            .ok_or(McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        if decoded.len() <= NONCE_LEN + AES_256_GCM.tag_len()
            || decoded.len() > MAX_MCP_OAUTH_STATE_BYTES
        {
            return Err(McpOAuthCallbackAuthorityError::NotFoundOrChanged);
        }
        let mut protected = SensitiveStateBuffer(decoded);
        let nonce_bytes: [u8; NONCE_LEN] = protected.0[..NONCE_LEN]
            .try_into()
            .map_err(|_| McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        protected.0.drain(..NONCE_LEN);
        let plaintext = key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(self.aad(key_id)),
                &mut protected.0,
            )
            .map_err(|_| McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        let claims = serde_json::from_slice::<McpOAuthStateClaims>(plaintext)
            .map_err(|_| McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        claims.validate(&self.config, now)
    }
}

impl McpOAuthStateIssuer for AeadMcpOAuthStateCodec {
    fn issue_state(
        &self,
        identity: &AuthenticatedMcpOAuthState,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<SensitiveOAuthValue, McpOAuthCallbackError> {
        identity.validate()?;
        let claims = McpOAuthStateClaims {
            schema_version: 1,
            tenant_id: identity.tenant_id.clone(),
            task_id: identity.task_id.clone(),
            callback_binding_digest: self.config.callback_binding_digest.clone(),
            issued_at_epoch_seconds: issued_at.timestamp(),
            expires_at_epoch_seconds: expires_at.timestamp(),
        };
        claims
            .validate(&self.config, issued_at)
            .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_state_issue_invalid"))?;
        let key =
            self.keys
                .get(&self.config.active_key_id)
                .ok_or(McpOAuthCallbackError::Rejected(
                    "mcp_oauth_state_key_unavailable",
                ))?;
        let mut nonce_bytes = [0_u8; NONCE_LEN];
        self.nonce_source.fill(&mut nonce_bytes).map_err(|_| {
            McpOAuthCallbackError::TemporarilyUnavailable("mcp_oauth_state_random_unavailable")
        })?;
        let mut protected = SensitiveStateBuffer(
            serde_json::to_vec(&claims)
                .map_err(|_| McpOAuthCallbackError::Rejected("mcp_oauth_state_issue_invalid"))?,
        );
        key.seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce_bytes),
            Aad::from(self.aad(&self.config.active_key_id)),
            &mut protected.0,
        )
        .map_err(|_| {
            McpOAuthCallbackError::TemporarilyUnavailable("mcp_oauth_state_seal_unavailable")
        })?;
        let mut envelope = Vec::with_capacity(NONCE_LEN + protected.0.len());
        envelope.extend_from_slice(&nonce_bytes);
        envelope.extend_from_slice(&protected.0);
        let encoded = URL_SAFE_NO_PAD.encode(envelope);
        let token = format!(
            "{STATE_TOKEN_VERSION}.{}.{encoded}",
            self.config.active_key_id
        )
        .into_bytes();
        SensitiveOAuthValue::from_decoded(token, MAX_MCP_OAUTH_STATE_BYTES)
    }
}

#[async_trait]
impl McpOAuthStateAuthenticator for AeadMcpOAuthStateCodec {
    async fn authenticate(
        &self,
        state: &SensitiveOAuthValue,
        now: DateTime<Utc>,
    ) -> Result<AuthenticatedMcpOAuthState, McpOAuthCallbackAuthorityError> {
        self.authenticate_state(state, now)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpOAuthStateClaims {
    schema_version: u32,
    tenant_id: ResourceId,
    task_id: ResourceId,
    callback_binding_digest: Sha256Digest,
    issued_at_epoch_seconds: i64,
    expires_at_epoch_seconds: i64,
}

impl McpOAuthStateClaims {
    fn validate(
        &self,
        config: &McpOAuthStateCodecConfig,
        now: DateTime<Utc>,
    ) -> Result<AuthenticatedMcpOAuthState, McpOAuthCallbackAuthorityError> {
        let identity = AuthenticatedMcpOAuthState {
            tenant_id: self.tenant_id.clone(),
            task_id: self.task_id.clone(),
        };
        identity
            .validate()
            .map_err(|_| McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        let issued_at = DateTime::from_timestamp(self.issued_at_epoch_seconds, 0)
            .ok_or(McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        let expires_at = DateTime::from_timestamp(self.expires_at_epoch_seconds, 0)
            .ok_or(McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        let maximum_expiry = issued_at
            .checked_add_signed(Duration::seconds(config.maximum_lifetime_seconds))
            .ok_or(McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        let maximum_issued_at = now
            .checked_add_signed(Duration::seconds(config.clock_skew_seconds))
            .ok_or(McpOAuthCallbackAuthorityError::NotFoundOrChanged)?;
        if self.schema_version != 1
            || self.callback_binding_digest != config.callback_binding_digest
            || issued_at > maximum_issued_at
            || expires_at <= issued_at
            || expires_at > maximum_expiry
            || now >= expires_at
        {
            return Err(McpOAuthCallbackAuthorityError::NotFoundOrChanged);
        }
        Ok(identity)
    }
}

struct SensitiveStateBuffer(Vec<u8>);

impl Drop for SensitiveStateBuffer {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

fn valid_key_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MCP_OAUTH_STATE_KEY_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{ResourceKind, Sha256Digest};

    struct FixedNonce([u8; NONCE_LEN]);

    impl McpOAuthStateNonceSource for FixedNonce {
        fn fill(&self, nonce: &mut [u8]) -> Result<(), ()> {
            nonce.copy_from_slice(&self.0);
            Ok(())
        }
    }

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn key(key_id: &str, byte: u8) -> McpOAuthStateKey {
        McpOAuthStateKey {
            key_id: key_id.to_owned(),
            key_material: SensitiveMcpOAuthStateKey::new(vec![byte; 32]).unwrap(),
        }
    }

    fn codec(active_key_id: &str, callback: char) -> AeadMcpOAuthStateCodec {
        AeadMcpOAuthStateCodec::with_nonce_source(
            McpOAuthStateCodecConfig {
                active_key_id: active_key_id.to_owned(),
                callback_binding_digest: digest(callback),
                maximum_lifetime_seconds: 600,
                clock_skew_seconds: 30,
            },
            vec![key("old", 7), key("new", 9)],
            Arc::new(FixedNonce([3; NONCE_LEN])),
        )
        .unwrap()
    }

    fn identity() -> AuthenticatedMcpOAuthState {
        AuthenticatedMcpOAuthState {
            tenant_id: id(ResourceKind::Tenant, 1),
            task_id: id(ResourceKind::Interaction, 2),
        }
    }

    #[tokio::test]
    async fn state_is_confidential_authenticated_and_callback_bound() {
        let now = Utc::now();
        let codec_instance = codec("new", 'a');
        let state = codec_instance
            .issue_state(&identity(), now, now + Duration::minutes(5))
            .unwrap();
        let visible = std::str::from_utf8(state.as_bytes()).unwrap();
        assert!(visible.starts_with("s1.new."));
        assert!(!visible.contains(&identity().tenant_id.to_string()));
        assert!(!visible.contains(&identity().task_id.to_string()));
        assert_eq!(
            codec_instance.authenticate(&state, now).await.unwrap(),
            identity()
        );

        let wrong_callback = codec("new", 'b');
        assert_eq!(
            wrong_callback.authenticate(&state, now).await.unwrap_err(),
            McpOAuthCallbackAuthorityError::NotFoundOrChanged
        );
    }

    #[tokio::test]
    async fn state_tampering_unknown_keys_and_expiry_fail_closed() {
        let now = Utc::now();
        let codec = codec("new", 'a');
        let state = codec
            .issue_state(&identity(), now, now + Duration::seconds(30))
            .unwrap();
        let mut tampered = state.as_bytes().to_vec();
        let replacement = if tampered.last() == Some(&b'A') {
            b'B'
        } else {
            b'A'
        };
        *tampered.last_mut().unwrap() = replacement;
        let tampered =
            SensitiveOAuthValue::from_decoded(tampered, MAX_MCP_OAUTH_STATE_BYTES).unwrap();
        assert!(codec.authenticate(&tampered, now).await.is_err());

        let unknown = SensitiveOAuthValue::from_decoded(
            std::str::from_utf8(state.as_bytes())
                .unwrap()
                .replacen("s1.new.", "s1.missing.", 1)
                .into_bytes(),
            MAX_MCP_OAUTH_STATE_BYTES,
        )
        .unwrap();
        assert!(codec.authenticate(&unknown, now).await.is_err());
        assert!(codec
            .authenticate(&state, now + Duration::seconds(30))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn rotation_issues_with_active_key_and_keeps_bounded_verification_keys() {
        let now = Utc::now();
        let old_codec = codec("old", 'a');
        let old_state = old_codec
            .issue_state(&identity(), now, now + Duration::minutes(5))
            .unwrap();
        let new_codec = codec("new", 'a');
        assert_eq!(
            new_codec.authenticate(&old_state, now).await.unwrap(),
            identity()
        );
        let new_state = new_codec
            .issue_state(&identity(), now, now + Duration::minutes(5))
            .unwrap();
        assert!(std::str::from_utf8(new_state.as_bytes())
            .unwrap()
            .starts_with("s1.new."));
    }
}
