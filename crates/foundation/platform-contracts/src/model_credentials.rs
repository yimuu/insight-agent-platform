//! Model API-key import metadata and transient sensitive material. The metadata digest identifies
//! an operation, never credential bytes; SecretBinding and the prepared provider version own state.
use crate::{
    canonical_digest, PrincipalKind, ResourceId, ResourceKind, SecretPurpose, Sha256Digest,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};
use uuid::Uuid;

pub const MODEL_API_KEY_PURPOSE: &str = "model_api_key";
pub const MAX_MODEL_API_KEY_BYTES: usize = 4096;
pub const MODEL_CREDENTIAL_IMPORT_AUTHORIZATION_SECONDS: i64 = 30;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ModelCredentialOperationId(Uuid);
impl TryFrom<String> for ModelCredentialOperationId {
    type Error = ModelCredentialImportError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let id = Uuid::parse_str(&value).map_err(|_| ModelCredentialImportError::Rejected)?;
        if !matches!(id.get_version_num(), 4 | 7)
            || id.get_variant() != uuid::Variant::RFC4122
            || id.to_string() != value
        {
            return Err(ModelCredentialImportError::Rejected);
        }
        Ok(Self(id))
    }
}
impl From<ModelCredentialOperationId> for String {
    fn from(value: ModelCredentialOperationId) -> Self {
        value.0.to_string()
    }
}
impl FromStr for ModelCredentialOperationId {
    type Err = ModelCredentialImportError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.to_owned().try_into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCredentialImportIdentityV1 {
    pub schema_version: u16,
    pub operation_id: ModelCredentialOperationId,
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub provider_id: ResourceId,
    pub purpose: SecretPurpose,
}
impl ModelCredentialImportIdentityV1 {
    pub fn validate(&self) -> bool {
        self.schema_version == 1
            && self.tenant_id.kind() == ResourceKind::Tenant
            && self.principal_id.kind() == ResourceKind::Principal
            && self.principal_kind != PrincipalKind::InstallationOperator
            && self.provider_id.kind() == ResourceKind::SecretProvider
            && self.purpose.as_str() == MODEL_API_KEY_PURPOSE
    }
    pub fn preparation_digest(&self) -> Result<Sha256Digest, ModelCredentialImportError> {
        if !self.validate() {
            return Err(ModelCredentialImportError::Rejected);
        }
        canonical_digest(
            &serde_json::json!({"domain":"model_credential_import_v1", "identity":self}),
        )
        .map_err(|_| ModelCredentialImportError::Rejected)?
        .parse()
        .map_err(|_| ModelCredentialImportError::Rejected)
    }
    pub fn secret_binding_id(&self) -> Result<ResourceId, ModelCredentialImportError> {
        let digest = self.preparation_digest()?;
        let hash = Sha256::digest(digest.as_str().as_bytes());
        let mut bytes = self.tenant_id.uuid().into_bytes();
        bytes[6..16].copy_from_slice(&hash[..10]);
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        ResourceId::from_uuid_v7(ResourceKind::SecretBinding, Uuid::from_bytes(bytes))
            .map_err(|_| ModelCredentialImportError::Rejected)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCredentialImportAuthorizationV1 {
    pub schema_version: u16,
    pub identity: ModelCredentialImportIdentityV1,
    pub deadline: DateTime<Utc>,
}
impl ModelCredentialImportAuthorizationV1 {
    pub fn validate_at(&self, now: DateTime<Utc>) -> bool {
        self.schema_version == 1
            && self.identity.validate()
            && self.deadline > now
            && self.deadline
                <= now + Duration::seconds(MODEL_CREDENTIAL_IMPORT_AUTHORIZATION_SECONDS)
    }
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ModelCredentialImportError> {
        serde_json::to_value(self)
            .ok()
            .and_then(|value| canonical_digest(&value).ok())
            .and_then(|value| value.parse().ok())
            .ok_or(ModelCredentialImportError::Rejected)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCredentialImportPermitV1 {
    pub schema_version: u16,
    pub request_digest: Sha256Digest,
    pub valid_until: DateTime<Utc>,
}
impl ModelCredentialImportPermitV1 {
    pub fn validate_for(
        &self,
        request: &ModelCredentialImportAuthorizationV1,
        now: DateTime<Utc>,
    ) -> bool {
        self.schema_version == 1
            && request.validate_at(now)
            && self.valid_until > now
            && self.valid_until <= request.deadline
            && request.canonical_digest().as_ref() == Ok(&self.request_digest)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCredentialImportError {
    Rejected,
    TemporarilyUnavailable,
    OutcomeUnknown,
}
impl fmt::Display for ModelCredentialImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Rejected => "model credential import rejected",
            Self::TemporarilyUnavailable => "model credential import temporarily unavailable",
            Self::OutcomeUnknown => "model credential import outcome unknown",
        })
    }
}
impl std::error::Error for ModelCredentialImportError {}

/// Deliberately not Serialize or Clone. Transport payloads are private and short lived.
pub struct SensitiveModelApiKey(Vec<u8>);
impl SensitiveModelApiKey {
    pub fn new(mut value: Vec<u8>) -> Result<Self, ModelCredentialImportError> {
        if value.is_empty()
            || value.len() > MAX_MODEL_API_KEY_BYTES
            || !value.iter().all(u8::is_ascii_graphic)
        {
            value.fill(0);
            return Err(ModelCredentialImportError::Rejected);
        }
        Ok(Self(value))
    }
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}
impl Drop for SensitiveModelApiKey {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}
impl fmt::Debug for SensitiveModelApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SensitiveModelApiKey")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for SensitiveModelApiKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value.into_bytes()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity() -> ModelCredentialImportIdentityV1 {
        ModelCredentialImportIdentityV1 {
            schema_version: 1,
            operation_id: "a8376371-3d45-4ef6-8c8c-eb1a895fa99c".parse().unwrap(),
            tenant_id: "ten_0198f1c3-8f49-7c3e-b1f3-773c28367d10".parse().unwrap(),
            principal_id: "prn_0198f1c3-8f49-7c3e-b1f3-773c28367d12".parse().unwrap(),
            principal_kind: PrincipalKind::TenantAdmin,
            provider_id: "spr_0198f1c3-8f49-7c3e-b1f3-773c28367d15".parse().unwrap(),
            purpose: MODEL_API_KEY_PURPOSE.parse().unwrap(),
        }
    }
    #[test]
    fn credential_import_metadata_and_sensitive_values_are_separate() {
        let identity = identity();
        let digest = identity.preparation_digest().unwrap();
        assert_eq!(
            identity.secret_binding_id().unwrap().kind(),
            ResourceKind::SecretBinding
        );
        let value = serde_json::to_value(&identity).unwrap();
        assert_eq!(
            serde_json::from_value::<ModelCredentialImportIdentityV1>(value.clone()).unwrap(),
            identity
        );
        let mut unknown = value;
        unknown["api_key"] = serde_json::json!("never-metadata");
        assert!(serde_json::from_value::<ModelCredentialImportIdentityV1>(unknown).is_err());
        let mut another = identity.clone();
        another.operation_id = "a8376371-3d45-4ef6-8c8c-eb1a895fa99d".parse().unwrap();
        assert_ne!(another.preparation_digest().unwrap(), digest);
        assert_ne!(
            another.secret_binding_id().unwrap(),
            identity.secret_binding_id().unwrap()
        );
        for changed in ["model.api_key", "mcp.oauth.pkce"] {
            let mut wrong = identity.clone();
            wrong.purpose = changed.parse().unwrap();
            assert!(!wrong.validate());
        }
        for value in [
            "",
            "A8376371-3d45-4ef6-8c8c-eb1a895fa99c",
            "a83763713d454ef68c8ceb1a895fa99c",
            "a8376371-3d45-1ef6-8c8c-eb1a895fa99c",
        ] {
            assert!(value.parse::<ModelCredentialOperationId>().is_err());
        }
        let key = SensitiveModelApiKey::new(b"secret-import-canary".to_vec()).unwrap();
        assert!(!format!("{key:?}").contains("secret-import-canary"));
        for value in [
            vec![],
            vec![b'a'; 4097],
            b"with space".to_vec(),
            vec![0xff],
            b"key\n".to_vec(),
        ] {
            assert!(SensitiveModelApiKey::new(value).is_err());
        }
    }
    #[test]
    fn import_permit_binds_every_metadata_field_and_never_extends_deadline() {
        let now = DateTime::parse_from_rfc3339("2026-09-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let request = ModelCredentialImportAuthorizationV1 {
            schema_version: 1,
            identity: identity(),
            deadline: now + Duration::seconds(30),
        };
        assert!(request.validate_at(now));
        let permit = ModelCredentialImportPermitV1 {
            schema_version: 1,
            request_digest: request.canonical_digest().unwrap(),
            valid_until: request.deadline,
        };
        assert!(permit.validate_for(&request, now));
        assert!(!permit.validate_for(&request, request.deadline));
        let mut changed = request.clone();
        changed.identity.principal_kind = PrincipalKind::AgentRunner;
        assert!(!permit.validate_for(&changed, now));
        changed = request.clone();
        changed.deadline += Duration::nanoseconds(1);
        assert!(!changed.validate_at(now));
        let mut extended = permit;
        extended.valid_until += Duration::nanoseconds(1);
        assert!(!extended.validate_for(&request, now));
    }
}
