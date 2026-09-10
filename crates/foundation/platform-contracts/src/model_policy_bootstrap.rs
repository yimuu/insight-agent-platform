//! Frozen one-shot model Policy identities and actual Artifact material. These values are inputs
//! to existing Registry/Artifact owners, never a second current-state database.
use crate::{
    canonical_digest, parse_strict_json, ExactVersionRef, JsonLimits, PolicyKind, ResourceId,
    ResourceKind, Sha256Digest, UtcTimestamp,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt};

pub const MAX_MODEL_POLICY_DECLARATION_BYTES: usize = 65_536;
pub const MAX_MODEL_POLICY_MATERIAL_BYTES: usize = 131_072;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelBootstrapPolicyRole {
    Protocol,
    Network,
    Tls,
    Trust,
    Data,
    Safety,
    Budget,
    PublicProjection,
    Selection,
    Execution,
}
impl ModelBootstrapPolicyRole {
    pub const ALL: [Self; 10] = [
        Self::Protocol,
        Self::Network,
        Self::Tls,
        Self::Trust,
        Self::Data,
        Self::Safety,
        Self::Budget,
        Self::PublicProjection,
        Self::Selection,
        Self::Execution,
    ];
    pub const fn policy_kind(self) -> PolicyKind {
        match self {
            Self::Protocol => PolicyKind::Protocol,
            Self::Network => PolicyKind::Network,
            Self::Tls => PolicyKind::Tls,
            Self::Trust => PolicyKind::Trust,
            Self::Data => PolicyKind::DataHandling,
            Self::Safety => PolicyKind::ModelSafety,
            Self::Budget => PolicyKind::Budget,
            Self::PublicProjection => PolicyKind::PublicProjection,
            Self::Selection => PolicyKind::Selection,
            Self::Execution => PolicyKind::Execution,
        }
    }
    pub const fn name(self) -> &'static str {
        match self {
            Self::Protocol => "protocol",
            Self::Network => "network",
            Self::Tls => "tls",
            Self::Trust => "trust",
            Self::Data => "data",
            Self::Safety => "safety",
            Self::Budget => "budget",
            Self::PublicProjection => "public_projection",
            Self::Selection => "selection",
            Self::Execution => "execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelBootstrapPolicyIdentityV1 {
    pub role: ModelBootstrapPolicyRole,
    pub resource_id: ResourceId,
    pub revision_id: ResourceId,
    pub deployment_id: ResourceId,
    pub artifact_reference_id: ResourceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelPolicyBootstrapSeedV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub installation_principal_id: ResourceId,
    pub created_by: ResourceId,
    pub request_id: ResourceId,
    pub environment: String,
    pub authoring_artifact_id: ResourceId,
    pub authoring_blob_id: ResourceId,
    pub model_quota_account_id: ResourceId,
    pub encryption_domain_id: ResourceId,
    pub retention_policy: ExactVersionRef,
    pub retain_until: UtcTimestamp,
    pub policies: [ModelBootstrapPolicyIdentityV1; 10],
}
impl ModelPolicyBootstrapSeedV1 {
    pub fn validate(&self) -> Result<(), ModelPolicyBootstrapError> {
        if self.schema_version != 1
            || self.environment.is_empty()
            || self.environment.len() > 64
            || !self
                .environment
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
            || self.retention_policy.resource_kind != ResourceKind::PolicyRevision
            || self.retention_policy.validate().is_err()
        {
            return Err(ModelPolicyBootstrapError);
        }
        let mut ids = BTreeSet::new();
        for (id, kind) in [
            (&self.tenant_id, ResourceKind::Tenant),
            (&self.installation_principal_id, ResourceKind::Principal),
            (&self.created_by, ResourceKind::Principal),
            (&self.request_id, ResourceKind::ServerRequest),
            (&self.authoring_artifact_id, ResourceKind::Artifact),
            (&self.authoring_blob_id, ResourceKind::InternalBlob),
            (&self.model_quota_account_id, ResourceKind::QuotaAccount),
            (&self.encryption_domain_id, ResourceKind::EncryptionDomain),
            (
                &self.retention_policy.revision_id,
                ResourceKind::PolicyRevision,
            ),
        ] {
            if id.kind() != kind || !ids.insert(id.clone()) {
                return Err(ModelPolicyBootstrapError);
            }
        }
        for (entry, role) in self.policies.iter().zip(ModelBootstrapPolicyRole::ALL) {
            if entry.role != role {
                return Err(ModelPolicyBootstrapError);
            }
            for (id, kind) in [
                (&entry.resource_id, ResourceKind::Policy),
                (&entry.revision_id, ResourceKind::PolicyRevision),
                (&entry.deployment_id, ResourceKind::PolicyDeployment),
                (&entry.artifact_reference_id, ResourceKind::ArtifactLink),
            ] {
                if id.kind() != kind || !ids.insert(id.clone()) {
                    return Err(ModelPolicyBootstrapError);
                }
            }
        }
        Ok(())
    }
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ModelPolicyBootstrapError> {
        self.validate()?;
        digest(self)
    }
    pub fn decode(
        bytes: &[u8],
        expected: &Sha256Digest,
    ) -> Result<Self, ModelPolicyBootstrapError> {
        let seed: Self = decode(bytes, MAX_MODEL_POLICY_DECLARATION_BYTES)?;
        if seed.canonical_digest()? != *expected {
            return Err(ModelPolicyBootstrapError);
        }
        Ok(seed)
    }
}

/// The trusted physical installer creates this only after exact generation HEAD+GET verifies the
/// staged bytes. PG validates its binding and persists it; PG does not claim to repeat provider IO.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelPolicyArtifactMaterialV1 {
    pub schema_version: u16,
    pub seed_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub size_bytes: u64,
    pub storage_backend: String,
    pub storage_binding_digest: Sha256Digest,
    pub object_reference_ciphertext: Vec<u8>,
    pub object_generation: String,
    pub key_id: String,
    pub backend_evidence_digest: Sha256Digest,
}
impl fmt::Debug for ModelPolicyArtifactMaterialV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelPolicyArtifactMaterialV1")
            .field("seed_digest", &self.seed_digest)
            .field("content_digest", &self.content_digest)
            .field("size_bytes", &self.size_bytes)
            .field(
                "encrypted_reference_bytes",
                &self.object_reference_ciphertext.len(),
            )
            .finish_non_exhaustive()
    }
}
impl ModelPolicyArtifactMaterialV1 {
    pub fn validate_for(
        &self,
        seed: &ModelPolicyBootstrapSeedV1,
        content_digest: &Sha256Digest,
        size_bytes: u64,
    ) -> Result<(), ModelPolicyBootstrapError> {
        if self.schema_version != 1
            || self.seed_digest != seed.canonical_digest()?
            || self.content_digest != *content_digest
            || self.size_bytes != size_bytes
            || !(1..=MAX_MODEL_POLICY_DECLARATION_BYTES as u64).contains(&size_bytes)
            || self.storage_backend != "s3"
            || !(1..=16_384).contains(&self.object_reference_ciphertext.len())
            || !bounded_graphic(&self.object_generation, 255)
            || !bounded_graphic(&self.key_id, 255)
        {
            return Err(ModelPolicyBootstrapError);
        }
        let expected = digest(
            &serde_json::json!({"schema_version":1,"kind":"s3_workload_stage",
            "tenant_id":seed.tenant_id,"artifact_id":seed.authoring_artifact_id,"blob_id":seed.authoring_blob_id,
            "object_generation":self.object_generation,"size_bytes":self.size_bytes,
            "storage_binding_digest":self.storage_binding_digest}),
        )?;
        if self.backend_evidence_digest != expected {
            return Err(ModelPolicyBootstrapError);
        }
        Ok(())
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ModelPolicyBootstrapError> {
        decode(bytes, MAX_MODEL_POLICY_MATERIAL_BYTES)
    }
}
fn bounded_graphic(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && value.bytes().all(|b| b.is_ascii_graphic())
}
fn digest(value: &impl Serialize) -> Result<Sha256Digest, ModelPolicyBootstrapError> {
    canonical_digest(&serde_json::to_value(value).map_err(|_| ModelPolicyBootstrapError)?)
        .map_err(|_| ModelPolicyBootstrapError)?
        .parse()
        .map_err(|_| ModelPolicyBootstrapError)
}
fn decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<T, ModelPolicyBootstrapError> {
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes,
            max_depth: 12,
            max_properties_per_object: 32,
            max_items_per_array: 16_384,
            max_string_bytes: 2048,
        },
    )
    .map_err(|_| ModelPolicyBootstrapError)?;
    serde_json::from_value(value).map_err(|_| ModelPolicyBootstrapError)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelPolicyBootstrapError;
impl fmt::Display for ModelPolicyBootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("model Policy bootstrap material is invalid")
    }
}
impl std::error::Error for ModelPolicyBootstrapError {}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(kind: ResourceKind, n: u16) -> ResourceId {
        ResourceId::from_uuid_v7(
            kind,
            uuid::Uuid::parse_str(&format!("0198f1c9-32e4-75e1-a9e8-d95ca0f4{n:04x}")).unwrap(),
        )
        .unwrap()
    }
    fn seed() -> ModelPolicyBootstrapSeedV1 {
        ModelPolicyBootstrapSeedV1 {
            schema_version: 1,
            tenant_id: id(ResourceKind::Tenant, 1),
            installation_principal_id: id(ResourceKind::Principal, 2),
            created_by: id(ResourceKind::Principal, 3),
            request_id: id(ResourceKind::ServerRequest, 4),
            environment: "development".into(),
            authoring_artifact_id: id(ResourceKind::Artifact, 5),
            authoring_blob_id: id(ResourceKind::InternalBlob, 6),
            model_quota_account_id: id(ResourceKind::QuotaAccount, 9),
            encryption_domain_id: id(ResourceKind::EncryptionDomain, 7),
            retention_policy: ExactVersionRef::new(
                id(ResourceKind::PolicyRevision, 8),
                format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            )
            .unwrap(),
            retain_until: "2027-01-01T00:00:00.000000Z".parse().unwrap(),
            policies: ModelBootstrapPolicyRole::ALL.map(|role| {
                let base = 100 + role as u16 * 4;
                ModelBootstrapPolicyIdentityV1 {
                    role,
                    resource_id: id(ResourceKind::Policy, base),
                    revision_id: id(ResourceKind::PolicyRevision, base + 1),
                    deployment_id: id(ResourceKind::PolicyDeployment, base + 2),
                    artifact_reference_id: id(ResourceKind::ArtifactLink, base + 3),
                }
            }),
        }
    }
    #[test]
    fn seed_requires_current_closed_unique_identity_and_exact_digest() {
        let seed = seed();
        let digest = seed.canonical_digest().unwrap();
        let bytes = serde_json::to_vec(&seed).unwrap();
        assert_eq!(
            ModelPolicyBootstrapSeedV1::decode(&bytes, &digest).unwrap(),
            seed
        );
        let mut wrong = seed.clone();
        wrong.policies[1].resource_id = wrong.policies[0].resource_id.clone();
        assert!(wrong.validate().is_err());
        let mut wrong = seed.clone();
        wrong.policies.swap(0, 1);
        assert!(wrong.validate().is_err());
        let mut wrong = seed.clone();
        wrong.policies[1].artifact_reference_id = wrong.policies[0].artifact_reference_id.clone();
        assert!(wrong.validate().is_err());
        let mut wrong = seed.clone();
        wrong.policies[1].artifact_reference_id = wrong.policies[0].revision_id.clone();
        assert!(wrong.validate().is_err());
        let mut wrong = seed.clone();
        wrong.tenant_id = wrong.created_by.clone();
        assert!(wrong.validate().is_err());
        let mut wrong = seed.clone();
        wrong.environment = "../development".into();
        assert!(wrong.validate().is_err());
        let mut value = serde_json::to_value(&seed).unwrap();
        value["schema_version"] = serde_json::json!(1.0);
        assert!(
            ModelPolicyBootstrapSeedV1::decode(&serde_json::to_vec(&value).unwrap(), &digest)
                .is_err()
        );
        value["schema_version"] = serde_json::json!(1);
        value["default_model"] = serde_json::Value::Null;
        assert!(
            ModelPolicyBootstrapSeedV1::decode(&serde_json::to_vec(&value).unwrap(), &digest)
                .is_err()
        );
        let duplicate =
            String::from_utf8(bytes)
                .unwrap()
                .replacen("{", "{\"schema_version\":1,", 1);
        assert!(ModelPolicyBootstrapSeedV1::decode(duplicate.as_bytes(), &digest).is_err());
        let mut changed = seed.clone();
        changed.request_id = id(ResourceKind::ServerRequest, 44);
        assert!(ModelPolicyBootstrapSeedV1::decode(
            &serde_json::to_vec(&changed).unwrap(),
            &digest
        )
        .is_err());
    }
    #[test]
    fn physical_material_checks_every_binding_without_claiming_provider_io() {
        let seed = seed();
        let content: Sha256Digest = format!("sha256:{}", "b".repeat(64)).parse().unwrap();
        let storage: Sha256Digest = format!("sha256:{}", "c".repeat(64)).parse().unwrap();
        let evidence=digest(&serde_json::json!({"schema_version":1,"kind":"s3_workload_stage","tenant_id":seed.tenant_id,
            "artifact_id":seed.authoring_artifact_id,"blob_id":seed.authoring_blob_id,"object_generation":"actual-version-shape",
            "size_bytes":100,"storage_binding_digest":storage})).unwrap();
        let material = ModelPolicyArtifactMaterialV1 {
            schema_version: 1,
            seed_digest: seed.canonical_digest().unwrap(),
            content_digest: content.clone(),
            size_bytes: 100,
            storage_backend: "s3".into(),
            storage_binding_digest: storage,
            object_reference_ciphertext: vec![1, 2, 3],
            object_generation: "actual-version-shape".into(),
            key_id: "fixture-key-id".into(),
            backend_evidence_digest: evidence,
        };
        material.validate_for(&seed, &content, 100).unwrap();
        assert_eq!(
            ModelPolicyArtifactMaterialV1::decode(&serde_json::to_vec(&material).unwrap()).unwrap(),
            material
        );
        assert!(!format!("{material:?}").contains("fixture-key-id"));
        assert!(material.validate_for(&seed, &content, 99).is_err());
        for change in 0..6 {
            let mut bad = material.clone();
            match change {
                0 => bad.object_generation = "other".into(),
                1 => bad.storage_backend = "builtin".into(),
                2 => bad.object_reference_ciphertext.clear(),
                3 => bad.object_reference_ciphertext = vec![0; 16_385],
                4 => bad.key_id = "bad\nkey".into(),
                _ => bad.seed_digest = content.clone(),
            };
            assert!(bad.validate_for(&seed, &content, 100).is_err());
        }
        assert!(ModelPolicyArtifactMaterialV1::decode(&vec![
            b' ';
            MAX_MODEL_POLICY_MATERIAL_BYTES + 1
        ])
        .is_err());
    }
}
