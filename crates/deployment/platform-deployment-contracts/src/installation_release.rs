//! Deployment-owned evidence for the explicitly authorized conversation upgrade.
use crate::installation::{InstallationError, InstallationIdentityV1, InstallationInputV1};
use insight_platform_contracts::{ResourceId, Sha256Digest};
use serde::{Deserialize, Serialize};

pub const RELEASE_FILE: &str = "release.json";
pub const UPGRADE_INTENT_FILE: &str = "conversation-upgrade.json";
pub const SOURCE_SCHEMA_VERSION: u32 = 16;
pub const TARGET_SCHEMA_VERSION: u32 = 17;
pub const SOURCE_INVENTORY_DIGEST: &str =
    "sha256:ac9da25876b552d2737afcc47ed65e1329860b9d4d0ef182f51a6fa4fa3da4e1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationReleaseV1 {
    pub schema_version: u32,
    pub installation_id: ResourceId,
    pub bootstrap_input_digest: Sha256Digest,
    pub bootstrap_identity_digest: Sha256Digest,
    pub from_package_digest: Sha256Digest,
    pub to_package_digest: Sha256Digest,
    pub from_schema_version: u32,
    pub to_schema_version: u32,
    pub from_inventory_digest: Sha256Digest,
    pub to_inventory_digest: Sha256Digest,
}

impl InstallationReleaseV1 {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, InstallationError> {
        insight_platform_contracts::canonical_digest(
            &serde_json::to_value(self).map_err(|_| InstallationError::InvalidInput)?,
        )
        .map_err(|_| InstallationError::InvalidInput)?
        .parse()
        .map_err(|_| InstallationError::InvalidInput)
    }
    pub fn validate_for(
        &self,
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
    ) -> Result<(), InstallationError> {
        input.validate()?;
        identity.validate()?;
        if self.schema_version != 1
            || self.installation_id != identity.installation_id
            || self.bootstrap_input_digest != input.digest()?
            || self.bootstrap_identity_digest != identity.digest()?
            || identity.input_digest != input.digest()?
            || self.from_package_digest != input.package_digest
            || self.from_package_digest == self.to_package_digest
            || self.from_schema_version != SOURCE_SCHEMA_VERSION
            || self.to_schema_version != TARGET_SCHEMA_VERSION
            || self.from_inventory_digest.as_str() != SOURCE_INVENTORY_DIGEST
            || self.from_inventory_digest == self.to_inventory_digest
        {
            return Err(InstallationError::IdentityDrift);
        }
        Ok(())
    }
}

/// Explicit same-schema package transition. ReleaseV1 itself remains bootstrap-to-current evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageRolloutIntentV1 {
    pub schema_version: u32,
    pub expected_previous_release_digest: Sha256Digest,
    pub previous_release: InstallationReleaseV1,
    pub target_release: InstallationReleaseV1,
}
impl PackageRolloutIntentV1 {
    pub fn validate_transition(&self) -> Result<(), InstallationError> {
        let mut target = self.previous_release.clone();
        target.to_package_digest = self.target_release.to_package_digest.clone();
        if self.schema_version != 1
            || self.expected_previous_release_digest != self.previous_release.canonical_digest()?
            || target != self.target_release
            || self.previous_release.to_package_digest == self.target_release.to_package_digest
        {
            return Err(InstallationError::IdentityDrift);
        }
        Ok(())
    }
    pub fn validate_for(
        &self,
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
    ) -> Result<(), InstallationError> {
        self.previous_release.validate_for(input, identity)?;
        self.target_release.validate_for(input, identity)?;
        self.validate_transition()
    }
    pub fn filename(target_release_digest: &Sha256Digest) -> String {
        format!(
            "package-rollout-{}.json",
            &target_release_digest.as_str()[7..]
        )
    }
}
