//! Versioned envelope encryption for MCP interaction and OAuth secrets.

use std::{collections::BTreeMap, fmt, sync::Arc};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use insight_durable::{
    McpProtectedSecret, McpSecretCiphertext, McpSecretProtector, McpSecretScope, RepositoryError,
};
use ring::{
    aead::{self, Aad, LessSafeKey, Nonce, UnboundKey},
    digest, hkdf,
    rand::{SecureRandom as _, SystemRandom},
};

use crate::repository::RepositoryErrorExt as _;

const ENVELOPE_PREFIX: &str = "enc:v1:";
const NONCE_BYTES: usize = 12;
const MAX_KEY_VERSION_BYTES: usize = 64;
const MAX_SECRET_BYTES: usize = 192 * 1024;
const HKDF_SALT: &[u8] = b"insight-agent-platform/mcp-secret-encryption/v1";

#[derive(Clone)]
pub struct McpSecretEncryptionKeyring {
    inner: Arc<McpSecretEncryptionKeyringInner>,
}

struct McpSecretEncryptionKeyringInner {
    active_key_version: String,
    keys: BTreeMap<String, [u8; 32]>,
}

impl Drop for McpSecretEncryptionKeyringInner {
    fn drop(&mut self) {
        for key in self.keys.values_mut() {
            key.fill(0);
        }
    }
}

impl fmt::Debug for McpSecretEncryptionKeyring {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpSecretEncryptionKeyring")
            .field("active_key_version", &self.inner.active_key_version)
            .field("key_versions", &self.inner.keys.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl McpSecretEncryptionKeyring {
    pub fn from_secret_json(
        active_key_version: impl Into<String>,
        secret_json: &str,
    ) -> Result<Self, RepositoryError> {
        let active_key_version = active_key_version.into();
        if !valid_key_version(&active_key_version) {
            return Err(RepositoryError::invalid_configuration());
        }
        let encoded = serde_json::from_str::<BTreeMap<String, String>>(secret_json)
            .map_err(|_| RepositoryError::invalid_configuration())?;
        if encoded.is_empty() || encoded.len() > 32 {
            return Err(RepositoryError::invalid_configuration());
        }
        let mut keys = BTreeMap::new();
        for (version, encoded_key) in encoded {
            if !valid_key_version(&version)
                || keys.insert(version, decode_key(&encoded_key)?).is_some()
            {
                return Err(RepositoryError::invalid_configuration());
            }
        }
        if !keys.contains_key(&active_key_version) {
            return Err(RepositoryError::invalid_configuration());
        }
        Ok(Self {
            inner: Arc::new(McpSecretEncryptionKeyringInner {
                active_key_version,
                keys,
            }),
        })
    }

    pub fn active_key_version(&self) -> &str {
        &self.inner.active_key_version
    }

    fn key_for(
        &self,
        version: &str,
        scope: &McpSecretScope,
    ) -> Result<LessSafeKey, RepositoryError> {
        let master = self
            .inner
            .keys
            .get(version)
            .ok_or_else(RepositoryError::invalid_configuration)?;
        let tenant_hash = digest::digest(&digest::SHA256, scope.tenant_id().as_bytes());
        let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, HKDF_SALT);
        let pseudo_random_key = salt.extract(master);
        let info = [
            b"tenant:".as_slice(),
            tenant_hash.as_ref(),
            b":version:".as_slice(),
            version.as_bytes(),
        ];
        let output = pseudo_random_key
            .expand(&info, hkdf::HKDF_SHA256)
            .map_err(|_| RepositoryError::invalid_configuration())?;
        let mut derived = [0_u8; 32];
        output
            .fill(&mut derived)
            .map_err(|_| RepositoryError::invalid_configuration())?;
        let key = UnboundKey::new(&aead::AES_256_GCM, &derived)
            .map(LessSafeKey::new)
            .map_err(|_| RepositoryError::invalid_configuration());
        derived.fill(0);
        key
    }
}

impl McpSecretProtector for McpSecretEncryptionKeyring {
    fn seal(
        &self,
        scope: &McpSecretScope,
        plaintext: &[u8],
    ) -> Result<McpProtectedSecret, RepositoryError> {
        if plaintext.is_empty() || plaintext.len() > MAX_SECRET_BYTES {
            return Err(RepositoryError::invalid_data());
        }
        let aad = serde_jcs::to_vec(scope).map_err(|_| RepositoryError::canonicalization())?;
        let mut nonce_bytes = [0_u8; NONCE_BYTES];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .map_err(|_| RepositoryError::storage_unavailable())?;
        let mut sealed = plaintext.to_vec();
        self.key_for(&self.inner.active_key_version, scope)?
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(aad),
                &mut sealed,
            )
            .map_err(|_| RepositoryError::storage_unavailable())?;
        let mut payload = Vec::with_capacity(NONCE_BYTES + sealed.len());
        payload.extend_from_slice(&nonce_bytes);
        payload.extend_from_slice(&sealed);
        let ciphertext = McpSecretCiphertext::new(format!(
            "{ENVELOPE_PREFIX}{}:{}",
            self.inner.active_key_version,
            URL_SAFE_NO_PAD.encode(payload)
        ))?;
        McpProtectedSecret::new(ciphertext, sha256_hex(plaintext))
    }

    fn open(
        &self,
        scope: &McpSecretScope,
        protected: &McpProtectedSecret,
    ) -> Result<Vec<u8>, RepositoryError> {
        let encoded = protected
            .ciphertext()
            .expose_ciphertext()
            .strip_prefix(ENVELOPE_PREFIX)
            .ok_or_else(RepositoryError::invalid_data)?;
        let (version, payload) = encoded
            .split_once(':')
            .ok_or_else(RepositoryError::invalid_data)?;
        if !valid_key_version(version) {
            return Err(RepositoryError::invalid_data());
        }
        let payload = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| RepositoryError::invalid_data())?;
        if payload.len() <= NONCE_BYTES + aead::AES_256_GCM.tag_len()
            || payload.len() > MAX_SECRET_BYTES + NONCE_BYTES + aead::AES_256_GCM.tag_len()
        {
            return Err(RepositoryError::invalid_data());
        }
        let mut nonce_bytes = [0_u8; NONCE_BYTES];
        nonce_bytes.copy_from_slice(&payload[..NONCE_BYTES]);
        let mut sealed = payload[NONCE_BYTES..].to_vec();
        let aad = serde_jcs::to_vec(scope).map_err(|_| RepositoryError::canonicalization())?;
        let plaintext = self
            .key_for(version, scope)?
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(aad),
                &mut sealed,
            )
            .map_err(|_| RepositoryError::invalid_data())?
            .to_vec();
        if sha256_hex(&plaintext) != protected.content_hash() {
            return Err(RepositoryError::invalid_data());
        }
        Ok(plaintext)
    }
}

fn sha256_hex(value: &[u8]) -> String {
    digest::digest(&digest::SHA256, value)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn valid_key_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_KEY_VERSION_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn decode_key(value: &str) -> Result<[u8; 32], RepositoryError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RepositoryError::invalid_configuration());
    }
    let mut decoded = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (decode_hex_digit(chunk[0])? << 4) | decode_hex_digit(chunk[1])?;
    }
    Ok(decoded)
}

fn decode_hex_digit(value: u8) -> Result<u8, RepositoryError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(RepositoryError::invalid_configuration()),
    }
}

#[cfg(test)]
mod tests {
    use insight_durable::{
        McpInteractionPrincipal, McpSecretProtector as _, McpSecretPurpose, McpSecretScope,
    };

    use super::McpSecretEncryptionKeyring;

    #[test]
    fn envelope_is_random_scoped_and_redacted() {
        let keyring = McpSecretEncryptionKeyring::from_secret_json(
            "active",
            &format!(r#"{{"active":"{}"}}"#, "11".repeat(32)),
        )
        .unwrap();
        let principal = McpInteractionPrincipal::new("tenant-a", "user-a").unwrap();
        let scope = McpSecretScope::new(
            &principal,
            "server-a",
            "interaction-a",
            McpSecretPurpose::ElicitationResponse,
        )
        .unwrap();
        let first = keyring.seal(&scope, br#"{"answer":"private"}"#).unwrap();
        let second = keyring.seal(&scope, br#"{"answer":"private"}"#).unwrap();
        assert_ne!(
            first.ciphertext().expose_ciphertext(),
            second.ciphertext().expose_ciphertext()
        );
        assert!(!format!("{first:?}").contains("private"));
        assert_eq!(
            keyring.open(&scope, &first).unwrap(),
            br#"{"answer":"private"}"#
        );

        let other_scope = McpSecretScope::new(
            &McpInteractionPrincipal::new("tenant-b", "user-a").unwrap(),
            "server-a",
            "interaction-a",
            McpSecretPurpose::ElicitationResponse,
        )
        .unwrap();
        assert!(keyring.open(&other_scope, &first).is_err());
    }
}
