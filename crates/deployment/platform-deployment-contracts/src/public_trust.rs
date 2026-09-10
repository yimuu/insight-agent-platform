//! Exact public certificate delivery; it is neither authentication nor endpoint qualification.
use crate::installation::{InstallationError, InstallationIdentityV1, InstallationInputV1};
use insight_platform_contracts::{parse_strict_json, JsonLimits, Sha256Digest};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

pub const MAXIMUM_PUBLIC_CA_BYTES: usize = 16_384;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationPublicTrustV1 {
    pub schema_version: u32,
    pub input_digest: Sha256Digest,
    pub identity_digest: Sha256Digest,
    pub certificate_pem: String,
    pub certificate_sha256: Sha256Digest,
}

impl InstallationPublicTrustV1 {
    pub fn decode(bytes: &[u8]) -> Result<Self, InstallationError> {
        let value = parse_strict_json(
            bytes,
            JsonLimits {
                max_bytes: 24_576,
                max_depth: 2,
                max_properties_per_object: 5,
                max_items_per_array: 1,
                max_string_bytes: MAXIMUM_PUBLIC_CA_BYTES,
            },
        )
        .map_err(|_| InstallationError::InvalidInput)?;
        let result: Self =
            serde_json::from_value(value).map_err(|_| InstallationError::InvalidInput)?;
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), InstallationError> {
        let certificate = self.certificate_pem.as_bytes();
        if self.schema_version != 1 || certificate.len() > MAXIMUM_PUBLIC_CA_BYTES {
            return Err(InstallationError::InvalidInput);
        }
        let body = self
            .certificate_pem
            .strip_prefix("-----BEGIN CERTIFICATE-----\n")
            .and_then(|value| value.strip_suffix("-----END CERTIFICATE-----\n"))
            .ok_or(InstallationError::CredentialInvalid)?;
        if body.is_empty()
            || !body.ends_with('\n')
            || body.lines().any(|line| line.is_empty() || line.len() > 64)
            || !body.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=' | b'\n')
            })
        {
            return Err(InstallationError::CredentialInvalid);
        }
        let encoded: String = body.lines().collect();
        let without_padding = encoded.trim_end_matches('=');
        if !encoded.len().is_multiple_of(4)
            || encoded.len() - without_padding.len() > 2
            || without_padding.is_empty()
            || without_padding.contains('=')
        {
            return Err(InstallationError::CredentialInvalid);
        }
        let observed = certificate_digest(certificate)?;
        if observed != self.certificate_sha256 {
            return Err(InstallationError::IdentityDrift);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
    ) -> Result<(), InstallationError> {
        self.validate()?;
        input.validate()?;
        identity.validate()?;
        if self.input_digest != input.digest()?
            || identity.input_digest != self.input_digest
            || self.identity_digest != identity.digest()?
            || self.certificate_sha256 != identity.certificate_authority_digest
        {
            return Err(InstallationError::IdentityDrift);
        }
        Ok(())
    }
}

fn certificate_digest(bytes: &[u8]) -> Result<Sha256Digest, InstallationError> {
    let hexadecimal: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("sha256:{hexadecimal}")
        .parse()
        .map_err(|_| InstallationError::InvalidInput)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn envelope() -> InstallationPublicTrustV1 {
        let certificate_pem =
            "-----BEGIN CERTIFICATE-----\nMAA=\n-----END CERTIFICATE-----\n".to_owned();
        InstallationPublicTrustV1 {
            schema_version: 1,
            input_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            identity_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
            certificate_sha256: certificate_digest(certificate_pem.as_bytes()).unwrap(),
            certificate_pem,
        }
    }
    #[test]
    fn only_bounded_current_public_certificate_envelopes_are_decoded() {
        let value = envelope();
        assert_eq!(
            InstallationPublicTrustV1::decode(&serde_json::to_vec(&value).unwrap()).unwrap(),
            value
        );
        let mut json = serde_json::to_value(&value).unwrap();
        json["issuer_private_key"] = serde_json::json!("private-canary");
        assert!(InstallationPublicTrustV1::decode(&serde_json::to_vec(&json).unwrap()).is_err());
        for certificate in [
            "-----BEGIN PRIVATE KEY-----\nMAA=\n-----END PRIVATE KEY-----\n".to_owned(),
            value.certificate_pem.repeat(2),
            "A".repeat(MAXIMUM_PUBLIC_CA_BYTES + 1),
        ] {
            let mut invalid = value.clone();
            invalid.certificate_pem = certificate;
            assert!(invalid.validate().is_err());
        }
        let mut invalid = value.clone();
        invalid.schema_version = 2;
        assert!(invalid.validate().is_err());
        invalid = value;
        invalid.certificate_sha256 = invalid.input_digest.clone();
        assert!(invalid.validate().is_err());
    }
}
