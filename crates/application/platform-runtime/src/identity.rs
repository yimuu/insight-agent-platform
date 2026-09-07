use insight_platform_contracts::{ResourceId, ResourceIdError, ResourceKind, Sha256Digest};
use sha2::{Digest as _, Sha256};
use std::{error::Error, fmt};
use uuid::Uuid;

pub trait CoordinatorIdentityFactory: Send + Sync {
    fn new_resource_id(&self, kind: ResourceKind) -> Result<ResourceId, IdentityFactoryError>;

    fn new_lease_token_digest(&self) -> Result<Sha256Digest, IdentityFactoryError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UuidCoordinatorIdentityFactory;

impl CoordinatorIdentityFactory for UuidCoordinatorIdentityFactory {
    fn new_resource_id(&self, kind: ResourceKind) -> Result<ResourceId, IdentityFactoryError> {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).map_err(IdentityFactoryError::ResourceId)
    }

    fn new_lease_token_digest(&self) -> Result<Sha256Digest, IdentityFactoryError> {
        let mut hasher = Sha256::new();
        hasher.update(b"insight.platform/v1/lease-token\0");
        hasher.update(Uuid::new_v4().as_bytes());
        format!("sha256:{}", lower_hex(&hasher.finalize()))
            .parse()
            .map_err(|_| IdentityFactoryError::Digest)
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Debug)]
pub enum IdentityFactoryError {
    ResourceId(ResourceIdError),
    Digest,
}

impl fmt::Display for IdentityFactoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceId(failure) => failure.fmt(formatter),
            Self::Digest => formatter.write_str("generated lease-token digest is invalid"),
        }
    }
}

impl Error for IdentityFactoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ResourceId(failure) => Some(failure),
            Self::Digest => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_factory_emits_typed_uuid_v7_and_unique_digests() {
        let factory = UuidCoordinatorIdentityFactory;
        let first = factory.new_resource_id(ResourceKind::Event).unwrap();
        let second = factory.new_resource_id(ResourceKind::Event).unwrap();
        assert_eq!(first.kind(), ResourceKind::Event);
        assert_ne!(first, second);
        assert_ne!(
            factory.new_lease_token_digest().unwrap(),
            factory.new_lease_token_digest().unwrap()
        );
    }
}
