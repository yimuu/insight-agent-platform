//! Signed evidence for the single current schema and its physical provisioning executable.
use insight_platform_contracts::Sha256Digest;

pub const SCHEMA_RUNNER_BINARY: &str = "platform-schema";
pub const SCHEMA_RELEASE_FILES: &[&str] = &[
    "schema-contract.json",
    "schema-inventory.json",
    "schema.sql",
];

/// Every digest hashes the exact distributed file bytes with SHA-256. These are
/// artifact identities, not canonical JSON identities or business data versions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaExecutableEvidenceV1 {
    pub schema_version: u32,
    pub runtime_image_digest: Sha256Digest,
    pub runner_binary: String,
    pub runner_build_digest: Sha256Digest,
    pub schema_snapshot_digest: Sha256Digest,
    pub schema_inventory_digest: Sha256Digest,
    pub schema_contract_digest: Sha256Digest,
}

impl SchemaExecutableEvidenceV1 {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1 || self.runner_binary != SCHEMA_RUNNER_BINARY {
            return Err("schema executable evidence has an unsupported identity");
        }
        Ok(())
    }
}
