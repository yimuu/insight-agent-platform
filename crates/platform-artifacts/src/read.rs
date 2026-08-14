use async_trait::async_trait;
use insight_platform_contracts::{ArtifactRef, ResourceId, ResourceKind, Sha256Digest};
use std::fmt;

pub const MAX_ARTIFACT_OBJECT_REFERENCE_BYTES: usize = 16_384;
pub const MAX_ARTIFACT_OBJECT_GENERATION_BYTES: usize = 255;
pub const MAX_ARTIFACT_STORAGE_BACKEND_BYTES: usize = 64;
pub const MAX_ARTIFACT_KMS_KEY_ID_BYTES: usize = 255;

/// Envelope-encrypted physical object locator exposed only to the trusted Artifact Broker.
///
/// It is intentionally non-clone, redacted in diagnostics and zeroed on drop. Public Artifact
/// projections never contain this value.
pub struct EncryptedArtifactObjectReference(Vec<u8>);

impl EncryptedArtifactObjectReference {
    pub fn new(mut ciphertext: Vec<u8>) -> Result<Self, ArtifactObjectReadAuthorityError> {
        if ciphertext.is_empty() || ciphertext.len() > MAX_ARTIFACT_OBJECT_REFERENCE_BYTES {
            ciphertext.fill(0);
            return Err(ArtifactObjectReadAuthorityError::InvalidEvidence);
        }
        Ok(Self(ciphertext))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for EncryptedArtifactObjectReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncryptedArtifactObjectReference")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for EncryptedArtifactObjectReference {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// One non-persisted authorization winner for reading an exact physical Artifact generation.
///
/// The authority digest binds caller-specific authorization evidence. The object locator remains
/// encrypted; only the Broker's KMS adapter may expose it.
pub struct AuthorizedArtifactObjectRead {
    pub tenant_id: ResourceId,
    pub blob_id: ResourceId,
    pub artifact: ArtifactRef,
    pub backend: String,
    pub storage_binding_digest: Sha256Digest,
    pub encryption_domain_id: ResourceId,
    pub key_id: String,
    pub object_reference_ciphertext: EncryptedArtifactObjectReference,
    pub object_generation: String,
    pub authorization_digest: Sha256Digest,
}

impl AuthorizedArtifactObjectRead {
    pub fn validate(&self) -> Result<(), ArtifactObjectReadAuthorityError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.blob_id.kind() != ResourceKind::InternalBlob
            || self.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
            || self.artifact.validate().is_err()
            || !stable_code(&self.backend, MAX_ARTIFACT_STORAGE_BACKEND_BYTES)
            || self.key_id.is_empty()
            || self.key_id.len() > MAX_ARTIFACT_KMS_KEY_ID_BYTES
            || self.key_id.chars().any(char::is_control)
            || self.object_generation.is_empty()
            || self.object_generation.len() > MAX_ARTIFACT_OBJECT_GENERATION_BYTES
            || self.object_generation.chars().any(char::is_control)
        {
            return Err(ArtifactObjectReadAuthorityError::InvalidEvidence);
        }
        Ok(())
    }
}

impl fmt::Debug for AuthorizedArtifactObjectRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizedArtifactObjectRead")
            .field("tenant_id", &self.tenant_id)
            .field("blob_id", &self.blob_id)
            .field("artifact", &self.artifact)
            .field("backend", &self.backend)
            .field("storage_binding_digest", &self.storage_binding_digest)
            .field("encryption_domain_id", &self.encryption_domain_id)
            .field("key_id", &"[redacted]")
            .field(
                "object_reference_ciphertext",
                &self.object_reference_ciphertext,
            )
            .field("object_generation", &"[redacted]")
            .field("authorization_digest", &self.authorization_digest)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactObjectReadAuthorityError {
    Unavailable,
    Denied,
    NotFound,
    InvalidEvidence,
}

/// Read-only database authority. `R` is the consumer-specific closed authorization request.
#[async_trait]
pub trait ArtifactObjectReadAuthority<R>: Send + Sync {
    async fn authorize_object_read(
        &self,
        request: &R,
    ) -> Result<AuthorizedArtifactObjectRead, ArtifactObjectReadAuthorityError>;
}

fn stable_code(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().enumerate().all(|(index, byte)| match byte {
            b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => {
                index != 0 || byte.is_ascii_lowercase()
            }
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{DataClassification, ResourceKind};
    use uuid::Uuid;

    fn id(kind: ResourceKind, _suffix: u128) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    #[test]
    fn trusted_projection_is_valid_and_diagnostics_are_redacted() {
        let artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, 3),
            digest('a'),
            7,
            "application/json".to_owned(),
            DataClassification::Internal,
            None,
        )
        .unwrap();
        let projection = AuthorizedArtifactObjectRead {
            tenant_id: id(ResourceKind::Tenant, 1),
            blob_id: id(ResourceKind::InternalBlob, 2),
            artifact,
            backend: "s3".to_owned(),
            storage_binding_digest: digest('b'),
            encryption_domain_id: id(ResourceKind::EncryptionDomain, 4),
            key_id: "kms-key-canary".to_owned(),
            object_reference_ciphertext: EncryptedArtifactObjectReference::new(
                b"ciphertext-canary".to_vec(),
            )
            .unwrap(),
            object_generation: "version-canary".to_owned(),
            authorization_digest: digest('c'),
        };
        projection.validate().unwrap();
        let diagnostic = format!("{projection:?}");
        assert!(!diagnostic.contains("kms-key-canary"));
        assert!(!diagnostic.contains("ciphertext-canary"));
        assert!(!diagnostic.contains("version-canary"));
    }
}
