//! One owner for prepared-secret payloads and exact authority context across physical stores.
use super::{SecretProviderPrepareError, SecretProviderResolveError, SecretReferenceSealError};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    parse_strict_json, ExactDeploymentRef, ExactSecretBindingRef, JsonLimits, ResourceId,
    ResourceKind, SecretPurpose, SecretResolutionPolicy, Sha256Digest,
};
use insight_platform_egress::{McpOAuthTokenPreparation, NewMcpOAuthTransientSecretBundle};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{collections::HashMap, fmt};
use uuid::Uuid;

pub(super) const MAX_PREPARED_SECRET_BYTES: usize = 64 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum PreparedSecretEnvelope {
    ModelCredential(PreparedModelCredential),
    McpOAuthPkce(PreparedMcpOAuthPkce),
    McpOAuthToken(PreparedMcpOAuthToken),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedModelCredential {
    pub(super) schema_version: u16,
    pub(super) identity: insight_platform_contracts::ModelCredentialImportIdentityV1,
    pub(super) api_key: SecretBytes,
}

pub(super) fn check_import_permit(
    import: Option<(
        &insight_platform_contracts::ModelCredentialImportAuthorizationV1,
        &insight_platform_contracts::ModelCredentialImportPermitV1,
    )>,
) -> Result<(), SecretProviderPrepareError> {
    if import.is_some_and(|(request, permit)| !permit.validate_for(request, Utc::now())) {
        return Err(SecretProviderPrepareError::Rejected);
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedMcpOAuthPkce {
    pub(super) schema_version: u32,
    pub(super) tenant_id: ResourceId,
    pub(super) task_id: ResourceId,
    pub(super) authorization_binding_id: ResourceId,
    pub(super) mcp_deployment: ExactDeploymentRef,
    pub(super) preparation_digest: Sha256Digest,
    pub(super) callback_binding_digest: Sha256Digest,
    pub(super) expires_at: DateTime<Utc>,
    pub(super) state: SecretBytes,
    pub(super) nonce: SecretBytes,
    pub(super) pkce_verifier: SecretBytes,
}

impl PreparedMcpOAuthPkce {
    pub(super) fn validate_for_transient(
        &self,
        candidate: &NewMcpOAuthTransientSecretBundle,
    ) -> Result<(), SecretProviderPrepareError> {
        if self.schema_version != 1
            || self.tenant_id != candidate.tenant_id
            || self.task_id != candidate.task_id
            || self.authorization_binding_id != candidate.authorization_binding_id
            || self.mcp_deployment != candidate.mcp_deployment
            || self.preparation_digest != candidate.preparation_digest
            || self.callback_binding_digest != candidate.callback_binding_digest
            || self.expires_at != candidate.expires_at
            || self.expires_at <= Utc::now()
        {
            return Err(SecretProviderPrepareError::Rejected);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedMcpOAuthToken {
    pub(super) schema_version: u32,
    pub(super) preparation_digest: Sha256Digest,
    pub(super) access_token: SecretBytes,
    pub(super) refresh_token: Option<SecretBytes>,
    pub(super) id_token: Option<SecretBytes>,
    pub(super) granted_scopes: Vec<String>,
    pub(super) audience_identity_digest: Sha256Digest,
    pub(super) issuer_identity_digest: Sha256Digest,
    pub(super) subject_identity_digest: Sha256Digest,
    pub(super) verification_evidence_digest: Sha256Digest,
    pub(super) expires_at: DateTime<Utc>,
}

impl PreparedMcpOAuthToken {
    pub(super) fn validate_for(
        &self,
        preparation: &McpOAuthTokenPreparation,
    ) -> Result<(), SecretProviderPrepareError> {
        if self.schema_version != 1
            || self.preparation_digest != preparation.preparation_digest
            || self.granted_scopes.is_empty()
            || !self.granted_scopes.windows(2).all(|pair| pair[0] < pair[1])
            || !self
                .granted_scopes
                .iter()
                .all(|scope| preparation.requested_scopes.binary_search(scope).is_ok())
            || self.audience_identity_digest != preparation.audience_identity_digest
            || self.issuer_identity_digest != preparation.issuer_identity_digest
            || self.expires_at <= Utc::now()
        {
            return Err(SecretProviderPrepareError::Rejected);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct SecretBytes(String);

impl SecretBytes {
    pub(super) fn encode(bytes: &[u8]) -> Self {
        Self(BASE64_STANDARD.encode(bytes))
    }

    pub(super) fn decode(&self) -> Result<Vec<u8>, SecretProviderResolveError> {
        let bytes = BASE64_STANDARD
            .decode(&self.0)
            .map_err(|_| SecretProviderResolveError::InvalidEvidence)?;
        if bytes.is_empty() || bytes.len() > insight_platform_egress::MAX_MCP_OAUTH_TOKEN_BYTES_HARD
        {
            return Err(SecretProviderResolveError::InvalidEvidence);
        }
        Ok(bytes)
    }
}

impl fmt::Debug for SecretBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretBytes")
            .field("encoded_byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        // SAFETY: the String remains valid UTF-8 after zeroing and is not observed again during
        // drop. This avoids a second allocation solely to clear provider-held sensitive text.
        unsafe { self.0.as_bytes_mut().fill(0) };
    }
}

pub(super) fn encode_prepared(
    envelope: &PreparedSecretEnvelope,
) -> Result<Vec<u8>, SecretProviderPrepareError> {
    let bytes = serde_jcs::to_vec(envelope).map_err(|_| SecretProviderPrepareError::Rejected)?;
    if bytes.is_empty() || bytes.len() > MAX_PREPARED_SECRET_BYTES {
        return Err(SecretProviderPrepareError::Rejected);
    }
    Ok(bytes)
}

pub(super) fn decode_prepared(
    bytes: &[u8],
) -> Result<PreparedSecretEnvelope, SecretProviderResolveError> {
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: MAX_PREPARED_SECRET_BYTES,
            max_depth: 16,
            max_items_per_array: insight_platform_contracts::MAX_MCP_OAUTH_SCOPES,
            max_properties_per_object: 32,
            max_string_bytes: insight_platform_egress::MAX_MCP_OAUTH_TOKEN_BYTES_HARD * 2,
        },
    )
    .map_err(|_| SecretProviderResolveError::InvalidEvidence)?;
    serde_json::from_value(value).map_err(|_| SecretProviderResolveError::InvalidEvidence)
}

pub(super) fn exact_binding(
    binding_id: ResourceId,
    provider_id: ResourceId,
    purpose: SecretPurpose,
    version_digest: Sha256Digest,
) -> Result<ExactSecretBindingRef, SecretProviderPrepareError> {
    ExactSecretBindingRef::build(
        binding_id,
        1,
        provider_id,
        purpose,
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: version_digest,
        },
    )
    .map_err(|_| SecretProviderPrepareError::Rejected)
}

pub(super) fn deterministic_binding_id(
    task_id: &ResourceId,
    preparation: &Sha256Digest,
) -> Result<ResourceId, SecretProviderPrepareError> {
    let hash = Sha256::digest(preparation.as_str().as_bytes());
    let mut bytes = task_id.uuid().into_bytes();
    bytes[6..16].copy_from_slice(&hash[..10]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    ResourceId::from_uuid_v7(ResourceKind::SecretBinding, Uuid::from_bytes(bytes))
        .map_err(|_| SecretProviderPrepareError::Rejected)
}

pub(super) fn validate_seal_identity(
    tenant_id: &ResourceId,
    secret_binding_id: &ResourceId,
    provider_id: &ResourceId,
    binding_generation: u64,
) -> Result<(), SecretReferenceSealError> {
    if tenant_id.kind() != ResourceKind::Tenant
        || secret_binding_id.kind() != ResourceKind::SecretBinding
        || provider_id.kind() != ResourceKind::SecretProvider
        || binding_generation == 0
    {
        return Err(SecretReferenceSealError::Rejected);
    }
    Ok(())
}

pub(super) fn reference_encryption_context(
    tenant_id: &ResourceId,
    secret_binding_id: &ResourceId,
    provider_id: &ResourceId,
    generation: u64,
    key_id: &str,
) -> HashMap<String, String> {
    HashMap::from([
        ("schema_version".to_owned(), "1".to_owned()),
        ("tenant_id".to_owned(), tenant_id.to_string()),
        (
            "secret_binding_id".to_owned(),
            secret_binding_id.to_string(),
        ),
        ("provider_id".to_owned(), provider_id.to_string()),
        ("binding_generation".to_owned(), generation.to_string()),
        ("key_id".to_owned(), key_id.to_owned()),
    ])
}
