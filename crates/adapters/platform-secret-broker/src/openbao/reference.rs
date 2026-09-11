use crate::{
    digest, provider_config::path_is_within, OpaqueSecretReference, OpenBaoSecretProviderConfigV1,
    SecretProviderResolveError as Failure, MAX_OPAQUE_SECRET_REFERENCE_BYTES,
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind,
    SecretResolutionPolicy, Sha256Digest,
};
use insight_platform_openbao::BaoSecretPath;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum MaterialKind {
    ModelCredential,
    McpOAuthPkce,
    McpOAuthToken,
}

/// This complete physical identity is encrypted before it crosses into durable business state.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OpenBaoOpaqueSecretReferenceV1 {
    pub(super) schema_version: u32,
    pub(super) provider_id: ResourceId,
    pub(super) provider_config_digest: Sha256Digest,
    pub(super) tenant_id: ResourceId,
    pub(super) kv_binding_digest: Sha256Digest,
    pub(super) relative_path: String,
    pub(super) version: u64,
    pub(super) material_kind: MaterialKind,
}

impl OpenBaoOpaqueSecretReferenceV1 {
    pub(super) fn new(
        config: &OpenBaoSecretProviderConfigV1,
        tenant_id: &ResourceId,
        preparation: &Sha256Digest,
        material_kind: MaterialKind,
    ) -> Result<Self, Failure> {
        let result = Self {
            schema_version: 1,
            provider_id: config.provider_id.clone(),
            provider_config_digest: config.provider_config_digest.clone(),
            tenant_id: tenant_id.clone(),
            kv_binding_digest: config.kv.identity_digest.clone(),
            relative_path: prepared_path(config, tenant_id, preparation)?
                .as_str()
                .to_owned(),
            version: 1,
            material_kind,
        };
        result.validate(config, tenant_id)?;
        Ok(result)
    }

    pub(super) fn encode(&self) -> Result<OpaqueSecretReference, Failure> {
        let bytes = serde_jcs::to_vec(self).map_err(|_| Failure::InvalidEvidence)?;
        OpaqueSecretReference::new(bytes).map_err(|_| Failure::InvalidEvidence)
    }

    pub(super) fn decode(
        reference: &OpaqueSecretReference,
        config: &OpenBaoSecretProviderConfigV1,
        tenant_id: &ResourceId,
    ) -> Result<Self, Failure> {
        let value = parse_strict_json(
            reference.expose(),
            JsonLimits {
                max_bytes: MAX_OPAQUE_SECRET_REFERENCE_BYTES,
                max_depth: 2,
                max_items_per_array: 1,
                max_properties_per_object: 9,
                max_string_bytes: 512,
            },
        )
        .map_err(|_| Failure::InvalidEvidence)?;
        let decoded: Self = serde_json::from_value(value).map_err(|_| Failure::InvalidEvidence)?;
        decoded.validate(config, tenant_id)?;
        Ok(decoded)
    }

    fn validate(
        &self,
        config: &OpenBaoSecretProviderConfigV1,
        tenant: &ResourceId,
    ) -> Result<(), Failure> {
        let prefix = format!("{}/{}", config.secret_path_prefix, tenant.uuid());
        let suffix = self.relative_path.strip_prefix(&format!("{prefix}/"));
        if self.schema_version != 1
            || tenant.kind() != ResourceKind::Tenant
            || self.tenant_id != *tenant
            || self.provider_id != config.provider_id
            || self.provider_config_digest != config.provider_config_digest
            || self.kv_binding_digest != config.kv.identity_digest
            || self.version != 1
            || BaoSecretPath::parse(&self.relative_path).is_err()
            || !path_is_within(&self.relative_path, &prefix)
            || !suffix.is_some_and(|hex| {
                hex.len() == 64
                    && hex
                        .bytes()
                        .all(|v| v.is_ascii_digit() || matches!(v, b'a'..=b'f'))
            })
        {
            return Err(Failure::Rejected);
        }
        Ok(())
    }

    pub(super) fn version_digest(&self) -> Result<Sha256Digest, Failure> {
        canonical_digest(&serde_json::json!({
            "domain": "openbao_secret_version_v1",
            "reference": self,
        }))
        .map_err(|_| Failure::InvalidEvidence)?
        .parse()
        .map_err(|_| Failure::InvalidEvidence)
    }

    pub(super) fn validate_policy(&self, policy: &SecretResolutionPolicy) -> Result<(), Failure> {
        match policy {
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest,
            } if *opaque_version_identity_digest == self.version_digest()? => Ok(()),
            _ => Err(Failure::Rejected),
        }
    }

    pub(super) fn evidence_digest(
        &self,
        preparation: &Sha256Digest,
    ) -> Result<Sha256Digest, Failure> {
        let encoded = self.encode()?;
        canonical_digest(&serde_json::json!({
            "domain": "openbao_prepared_secret_storage_v1",
            "schema_version": 1,
            "preparation_digest": preparation,
            "reference_digest": digest(encoded.expose()),
            "version_identity_digest": self.version_digest()?,
        }))
        .map_err(|_| Failure::InvalidEvidence)?
        .parse()
        .map_err(|_| Failure::InvalidEvidence)
    }
}

pub(super) fn prepared_path(
    config: &OpenBaoSecretProviderConfigV1,
    tenant: &ResourceId,
    preparation: &Sha256Digest,
) -> Result<BaoSecretPath, Failure> {
    if tenant.kind() != ResourceKind::Tenant {
        return Err(Failure::Rejected);
    }
    let hex = preparation
        .as_str()
        .strip_prefix("sha256:")
        .ok_or(Failure::Rejected)?;
    BaoSecretPath::parse(&format!(
        "{}/{}/{}",
        config.secret_path_prefix,
        tenant.uuid(),
        hex
    ))
    .map_err(|_| Failure::Rejected)
}
