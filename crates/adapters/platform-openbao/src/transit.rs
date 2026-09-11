use crate::{client::encode, BaoClient, BaoError, SensitiveBytes, TransitBindingV1};
use base64::{engine::general_purpose::STANDARD, Engine};
use reqwest::Method;
use serde_json::Value;
use tokio::time::Instant;

const MAX_PLAINTEXT_BYTES: usize = 16 * 1024;
const MAX_CIPHERTEXT_BYTES: usize = 32 * 1024;
const MAX_AAD_BYTES: usize = 16 * 1024;

impl BaoClient {
    pub async fn check_transit(
        &self,
        binding: &TransitBindingV1,
        caller_deadline: Instant,
    ) -> Result<(), BaoError> {
        binding.validate_for(self.config())?;
        let deadline = self.deadline(caller_deadline)?;
        self.check_mount(
            &binding.mount,
            &binding.mount_accessor,
            "transit",
            None,
            deadline,
        )
        .await?;
        let metadata = self
            .authorized(
                Method::GET,
                &format!("{}/keys/{}", binding.mount, binding.name),
                None,
                false,
                deadline,
            )
            .await
            .map_err(|error| {
                if error == BaoError::NotFound {
                    BaoError::InvalidEvidence
                } else {
                    error
                }
            })?;
        check_key(&metadata.0, binding)
    }

    pub async fn encrypt(
        &self,
        binding: &TransitBindingV1,
        plaintext: &[u8],
        canonical_aad: &[u8],
        caller_deadline: Instant,
    ) -> Result<SensitiveBytes, BaoError> {
        check_input(plaintext, MAX_PLAINTEXT_BYTES, canonical_aad)?;
        let deadline = self.deadline(caller_deadline)?;
        self.check_transit(binding, deadline).await?;
        let body = encode(serde_json::json!({
            "plaintext": STANDARD.encode(plaintext),
            "associated_data": STANDARD.encode(canonical_aad),
            "key_version": binding.key_version,
        }))?;
        let response = self
            .authorized(
                Method::POST,
                &format!("{}/encrypt/{}", binding.mount, binding.name),
                Some(body),
                false,
                deadline,
            )
            .await?;
        let data = response.0.get("data").ok_or(BaoError::InvalidEvidence)?;
        let ciphertext = data
            .get("ciphertext")
            .and_then(Value::as_str)
            .ok_or(BaoError::InvalidEvidence)?;
        check_ciphertext(ciphertext.as_bytes(), binding.key_version)?;
        if data
            .get("key_version")
            .is_some_and(|version| version.as_u64() != Some(u64::from(binding.key_version)))
        {
            return Err(BaoError::InvalidEvidence);
        }
        SensitiveBytes::new(ciphertext.as_bytes().to_vec())
    }

    pub async fn decrypt(
        &self,
        binding: &TransitBindingV1,
        ciphertext: &[u8],
        canonical_aad: &[u8],
        caller_deadline: Instant,
    ) -> Result<SensitiveBytes, BaoError> {
        check_input(ciphertext, MAX_CIPHERTEXT_BYTES, canonical_aad)?;
        check_ciphertext(ciphertext, binding.key_version)?;
        let deadline = self.deadline(caller_deadline)?;
        self.check_transit(binding, deadline).await?;
        let body = encode(serde_json::json!({
            "ciphertext": std::str::from_utf8(ciphertext).map_err(|_| BaoError::InvalidEvidence)?,
            "associated_data": STANDARD.encode(canonical_aad),
        }))?;
        let response = self
            .authorized(
                Method::POST,
                &format!("{}/decrypt/{}", binding.mount, binding.name),
                Some(body),
                false,
                deadline,
            )
            .await?;
        let plaintext = response
            .0
            .pointer("/data/plaintext")
            .and_then(Value::as_str)
            .ok_or(BaoError::InvalidEvidence)?;
        let bytes = SensitiveBytes::new(
            STANDARD
                .decode(plaintext)
                .map_err(|_| BaoError::InvalidEvidence)?,
        )?;
        if bytes.as_bytes().len() > MAX_PLAINTEXT_BYTES {
            return Err(BaoError::InvalidEvidence);
        }
        Ok(bytes)
    }
}

fn check_input(bytes: &[u8], maximum: usize, aad: &[u8]) -> Result<(), BaoError> {
    if bytes.is_empty() || bytes.len() > maximum || aad.is_empty() || aad.len() > MAX_AAD_BYTES {
        Err(BaoError::InvalidConfig)
    } else {
        Ok(())
    }
}

fn check_ciphertext(bytes: &[u8], version: u32) -> Result<(), BaoError> {
    if bytes.len() > MAX_CIPHERTEXT_BYTES {
        return Err(BaoError::InvalidEvidence);
    }
    let value = std::str::from_utf8(bytes).map_err(|_| BaoError::InvalidEvidence)?;
    let payload = value
        .strip_prefix(&format!("vault:v{version}:"))
        .ok_or(BaoError::InvalidEvidence)?;
    let decoded = STANDARD
        .decode(payload)
        .map_err(|_| BaoError::InvalidEvidence)?;
    // AES-GCM ciphertext must include its random nonce and authentication tag.
    if decoded.len() <= 12 + 16 {
        return Err(BaoError::InvalidEvidence);
    }
    Ok(())
}

fn check_key(value: &Value, binding: &TransitBindingV1) -> Result<(), BaoError> {
    let data = value.get("data").ok_or(BaoError::InvalidEvidence)?;
    let version = u64::from(binding.key_version);
    let minimum_decrypt = data
        .get("min_decryption_version")
        .and_then(Value::as_u64)
        .ok_or(BaoError::InvalidEvidence)?;
    let minimum_encrypt = data
        .get("min_encryption_version")
        .and_then(Value::as_u64)
        .ok_or(BaoError::InvalidEvidence)?;
    let latest = data
        .get("latest_version")
        .and_then(Value::as_u64)
        .ok_or(BaoError::InvalidEvidence)?;
    if data.get("name").and_then(Value::as_str) != Some(binding.name.as_str())
        || data.get("type").and_then(Value::as_str) != Some("aes256-gcm96")
        || [
            "exportable",
            "allow_plaintext_backup",
            "deletion_allowed",
            "derived",
        ]
        .iter()
        .any(|field| data.get(field).and_then(Value::as_bool) != Some(false))
        || data
            .get("convergent_encryption")
            .is_some_and(|value| value.as_bool() != Some(false))
        || minimum_decrypt > version
        || minimum_encrypt > version
        || latest < version
        || data
            .get("keys")
            .and_then(Value::as_object)
            .is_none_or(|keys| !keys.contains_key(&binding.key_version.to_string()))
    {
        return Err(BaoError::InvalidEvidence);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ciphertext_requires_exact_canonical_version_and_authenticated_payload() {
        let payload = STANDARD.encode([7u8; 29]);
        assert_eq!(
            check_ciphertext(format!("vault:v1:{payload}").as_bytes(), 1),
            Ok(())
        );
        for ciphertext in [
            format!("vault:v2:{payload}"),
            format!("vault:v01:{payload}"),
            "plaintext".into(),
            "vault:v1:dGlueQ==".into(),
        ] {
            assert_eq!(
                check_ciphertext(ciphertext.as_bytes(), 1),
                Err(BaoError::InvalidEvidence)
            );
        }
    }
}
