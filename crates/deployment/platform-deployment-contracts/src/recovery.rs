//! Offline recovery evidence. Validation proves consistency of signed declarations,
//! never the truth of a provider observation or permission to mutate business state.
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ArtifactRef, ExactSecretBindingRef, JsonLimits, ResourceId, ResourceKind,
    SecretResolutionPolicy, Sha256Digest, WorkerExecutionCapabilities,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const RECOVERY_MANIFEST_LIMITS: JsonLimits = JsonLimits {
    max_bytes: 2_097_152,
    max_depth: 24,
    max_properties_per_object: 64,
    max_items_per_array: 8192,
    max_string_bytes: 1024,
};
pub const RECOVERY_REPORT_LIMITS: JsonLimits = JsonLimits {
    max_bytes: 1_048_576,
    ..RECOVERY_MANIFEST_LIMITS
};
pub const RECOVERY_MAX_OBJECTS: usize = 4096;
pub const RECOVERY_MAX_KEYS: usize = 1024;
pub const RECOVERY_MAX_EFFECTS: usize = 4096;
pub const RECOVERY_MAX_EVIDENCE: usize = 8192;
pub const RECOVERY_SET_LIMITS: JsonLimits = JsonLimits {
    max_items_per_array: RECOVERY_MAX_EVIDENCE + 2,
    ..RECOVERY_MANIFEST_LIMITS
};
pub const RECOVERY_EVIDENCE_MAX_BYTES: u64 = 4_194_304;
pub const RECOVERY_SET_MAX_BYTES: u64 = 268_435_456;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgreSqlRecoveryPoint {
    pub instance_identity_digest: Sha256Digest,
    pub timeline: u32,
    pub wal_lsn: String,
    pub snapshot_digest: Sha256Digest,
    pub schema_snapshot_digest: Sha256Digest,
    pub captured_at: DateTime<Utc>,
    pub reference_inventory_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryObject {
    pub artifact: ArtifactRef,
    pub storage_binding_digest: Sha256Digest,
    /// Opaque provider generation identity, never an object URL or credential.
    pub object_generation_digest: Sha256Digest,
    pub required_key_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecoveryKey {
    Secret {
        binding: ExactSecretBindingRef,
    },
    Kms {
        encryption_domain_id: ResourceId,
        key_version_identity_digest: Sha256Digest,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryManifestV1 {
    pub schema_version: u32,
    pub created_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub database: PostgreSqlRecoveryPoint,
    pub objects: Vec<RecoveryObject>,
    pub required_keys: Vec<RecoveryKey>,
    pub potentially_affected_effects: Vec<Sha256Digest>,
    pub program_capabilities: WorkerExecutionCapabilities,
    pub runner_build_digest: Sha256Digest,
    pub package_set_digest: Sha256Digest,
    pub release_digest: Sha256Digest,
    pub recovery_tool_build_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryObservation {
    pub subject_digest: Sha256Digest,
    pub evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryBackupHold {
    pub subject_digest: Sha256Digest,
    pub protected_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPhase {
    OldWritersIsolated,
    OldIdentitiesRevoked,
    OldSessionsTerminated,
    CredentialsRotated,
    RestrictedEnvironmentInstalled,
    DatabaseStructureVerified,
    OwnerAndQuotaVerified,
    CleanupObligationsVerified,
    ReferenceInventoryComplete,
    SecretValidityRechecked,
    ConsumerWatermarksReconciled,
}
impl RecoveryPhase {
    pub const ALL: &'static [Self] = &[
        Self::OldWritersIsolated,
        Self::OldIdentitiesRevoked,
        Self::OldSessionsTerminated,
        Self::CredentialsRotated,
        Self::RestrictedEnvironmentInstalled,
        Self::DatabaseStructureVerified,
        Self::OwnerAndQuotaVerified,
        Self::CleanupObligationsVerified,
        Self::ReferenceInventoryComplete,
        Self::SecretValidityRechecked,
        Self::ConsumerWatermarksReconciled,
    ];
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPhaseObservation {
    pub phase: RecoveryPhase,
    pub verified: bool,
    pub evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecoveryEffectDisposition {
    VerifiedCompleted {
        evidence_digest: Sha256Digest,
    },
    VerifiedNotExecuted {
        evidence_digest: Sha256Digest,
    },
    Quarantined {
        evidence_digest: Sha256Digest,
        isolation_evidence_digest: Sha256Digest,
    },
    Unresolved {
        evidence_digest: Sha256Digest,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryEffectObservation {
    pub effect_identity_digest: Sha256Digest,
    pub disposition: RecoveryEffectDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryVerificationReportV1 {
    pub schema_version: u32,
    pub manifest_digest: Sha256Digest,
    pub verified_at: DateTime<Utc>,
    pub object_observations: Vec<RecoveryObservation>,
    pub key_observations: Vec<RecoveryObservation>,
    pub backup_holds: Vec<RecoveryBackupHold>,
    pub phases: Vec<RecoveryPhaseObservation>,
    pub effects: Vec<RecoveryEffectObservation>,
    /// Each unrestored effect must occur here and have an explicit isolation proof.
    pub unrecovered_effects: Vec<Sha256Digest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryError {
    Version,
    Bounds,
    Identity,
    Duplicate,
    Incomplete,
    Expired,
    ManifestMismatch,
    UnverifiedPhase,
    UnisolatedEffect,
}
impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "recovery_{self:?}")
    }
}
impl std::error::Error for RecoveryError {}

pub fn recovery_identity(value: &impl Serialize) -> Result<Sha256Digest, RecoveryError> {
    canonical_digest(&serde_json::to_value(value).map_err(|_| RecoveryError::Identity)?)
        .map_err(|_| RecoveryError::Identity)?
        .parse()
        .map_err(|_| RecoveryError::Identity)
}
fn identities<T: Serialize>(values: &[T]) -> Result<BTreeSet<Sha256Digest>, RecoveryError> {
    let values_out = values
        .iter()
        .map(recovery_identity)
        .collect::<Result<BTreeSet<_>, _>>()?;
    if values_out.len() != values.len() {
        return Err(RecoveryError::Duplicate);
    }
    Ok(values_out)
}
fn unique(values: &[Sha256Digest]) -> Result<BTreeSet<Sha256Digest>, RecoveryError> {
    let output = values.iter().cloned().collect::<BTreeSet<_>>();
    if output.len() != values.len() {
        return Err(RecoveryError::Duplicate);
    }
    Ok(output)
}
impl RecoveryManifestV1 {
    pub fn validate(&self, now: DateTime<Utc>) -> Result<(), RecoveryError> {
        if self.schema_version != 1 {
            return Err(RecoveryError::Version);
        }
        if self.objects.len() > RECOVERY_MAX_OBJECTS
            || self.required_keys.len() > RECOVERY_MAX_KEYS
            || self.potentially_affected_effects.len() > RECOVERY_MAX_EFFECTS
        {
            return Err(RecoveryError::Bounds);
        }
        if self.created_at < self.database.captured_at
            || now < self.created_at
            || now >= self.valid_until
            || self.valid_until <= self.created_at
        {
            return Err(RecoveryError::Expired);
        }
        let lsn = self.database.wal_lsn.split('/').collect::<Vec<_>>();
        if self.database.timeline == 0
            || lsn.len() != 2
            || lsn.iter().any(|part| {
                part.is_empty()
                    || part.len() > 8
                    || !part
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'A'..=b'F').contains(&byte))
            })
        {
            return Err(RecoveryError::Identity);
        }
        self.program_capabilities
            .validate()
            .map_err(|_| RecoveryError::Identity)?;
        if self.program_capabilities.capabilities.is_empty()
            || self.program_capabilities.capabilities.iter().any(|cap| {
                !matches!(
                    cap,
                    insight_platform_contracts::WorkerExecutionCapability::Program { .. }
                )
            })
        {
            return Err(RecoveryError::Identity);
        }
        let keys = identities(&self.required_keys)?;
        let kms_keys = self
            .required_keys
            .iter()
            .filter(|key| matches!(key, RecoveryKey::Kms { .. }))
            .map(recovery_identity)
            .collect::<Result<BTreeSet<_>, _>>()?;
        identities(&self.objects)?;
        let artifact_ids = self
            .objects
            .iter()
            .map(|object| object.artifact.artifact_id())
            .collect::<BTreeSet<_>>();
        if artifact_ids.len() != self.objects.len() {
            return Err(RecoveryError::Duplicate);
        }
        unique(&self.potentially_affected_effects)?;
        for key in &self.required_keys {
            match key {
                RecoveryKey::Secret { binding } => {
                    binding.validate().map_err(|_| RecoveryError::Identity)?;
                    if !matches!(
                        binding.resolution_policy,
                        SecretResolutionPolicy::Pinned { .. }
                    ) {
                        return Err(RecoveryError::Identity);
                    }
                }
                RecoveryKey::Kms {
                    encryption_domain_id,
                    ..
                } if encryption_domain_id.kind() != ResourceKind::EncryptionDomain => {
                    return Err(RecoveryError::Identity)
                }
                _ => {}
            }
        }
        for object in &self.objects {
            object
                .artifact
                .validate()
                .map_err(|_| RecoveryError::Identity)?;
            if !keys.contains(&object.required_key_digest)
                || !kms_keys.contains(&object.required_key_digest)
            {
                return Err(RecoveryError::Incomplete);
            }
        }
        Ok(())
    }
}
impl RecoveryVerificationReportV1 {
    /// Returns the complete bounded set of external evidence files that must be signed.
    pub fn validate_for(
        &self,
        manifest: &RecoveryManifestV1,
        now: DateTime<Utc>,
    ) -> Result<BTreeSet<Sha256Digest>, RecoveryError> {
        manifest.validate(now)?;
        if self.schema_version != 1 {
            return Err(RecoveryError::Version);
        }
        if self.manifest_digest != recovery_identity(manifest)? {
            return Err(RecoveryError::ManifestMismatch);
        }
        if self.verified_at < manifest.created_at || self.verified_at > now {
            return Err(RecoveryError::Expired);
        }
        if self.object_observations.len() > RECOVERY_MAX_OBJECTS
            || self.key_observations.len() > RECOVERY_MAX_KEYS
            || self.backup_holds.len() > RECOVERY_MAX_OBJECTS + RECOVERY_MAX_KEYS
            || self.effects.len() > RECOVERY_MAX_EFFECTS
            || self.unrecovered_effects.len() > RECOVERY_MAX_EFFECTS
            || self.phases.len() != RecoveryPhase::ALL.len()
        {
            return Err(RecoveryError::Bounds);
        }
        let objects = identities(&manifest.objects)?;
        let keys = identities(&manifest.required_keys)?;
        let mut evidence = BTreeSet::from([
            manifest.database.reference_inventory_digest.clone(),
            manifest.package_set_digest.clone(),
        ]);
        for (expected, observed) in [
            (&objects, &self.object_observations),
            (&keys, &self.key_observations),
        ] {
            let subjects = observed
                .iter()
                .map(|item| item.subject_digest.clone())
                .collect::<Vec<_>>();
            if &unique(&subjects)? != expected {
                return Err(RecoveryError::Incomplete);
            }
            evidence.extend(observed.iter().map(|item| item.evidence_digest.clone()));
        }
        let required_holds = objects.union(&keys).cloned().collect::<BTreeSet<_>>();
        let holds = self
            .backup_holds
            .iter()
            .map(|hold| hold.subject_digest.clone())
            .collect::<Vec<_>>();
        if unique(&holds)? != required_holds {
            return Err(RecoveryError::Incomplete);
        }
        for hold in &self.backup_holds {
            if hold.protected_from > manifest.database.captured_at
                || hold.expires_at < manifest.valid_until
                || hold.expires_at <= now
            {
                return Err(RecoveryError::Expired);
            }
            evidence.insert(hold.evidence_digest.clone());
        }
        let phases = self
            .phases
            .iter()
            .map(|phase| (phase.phase, phase))
            .collect::<BTreeMap<_, _>>();
        if phases.len() != self.phases.len()
            || phases.keys().copied().collect::<BTreeSet<_>>()
                != RecoveryPhase::ALL.iter().copied().collect()
        {
            return Err(RecoveryError::Incomplete);
        }
        for phase in &self.phases {
            if !phase.verified {
                return Err(RecoveryError::UnverifiedPhase);
            }
            evidence.insert(phase.evidence_digest.clone());
        }
        if unique(
            &self
                .effects
                .iter()
                .map(|effect| effect.effect_identity_digest.clone())
                .collect::<Vec<_>>(),
        )? != unique(&manifest.potentially_affected_effects)?
        {
            return Err(RecoveryError::Incomplete);
        }
        let mut quarantined = BTreeSet::new();
        for effect in &self.effects {
            match &effect.disposition {
                RecoveryEffectDisposition::VerifiedCompleted { evidence_digest }
                | RecoveryEffectDisposition::VerifiedNotExecuted { evidence_digest } => {
                    evidence.insert(evidence_digest.clone());
                }
                RecoveryEffectDisposition::Quarantined {
                    evidence_digest,
                    isolation_evidence_digest,
                } => {
                    evidence.insert(evidence_digest.clone());
                    evidence.insert(isolation_evidence_digest.clone());
                    quarantined.insert(effect.effect_identity_digest.clone());
                }
                RecoveryEffectDisposition::Unresolved { .. } => {
                    return Err(RecoveryError::UnisolatedEffect)
                }
            }
        }
        if unique(&self.unrecovered_effects)? != quarantined {
            return Err(RecoveryError::Incomplete);
        }
        if evidence.len() > RECOVERY_MAX_EVIDENCE {
            return Err(RecoveryError::Bounds);
        }
        Ok(evidence)
    }
}

/// Raw file digests cover both core documents and each evidence file; the report's
/// manifest_digest is a canonical typed identity, so no file/report digest cycle exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverySetFile {
    pub path: String,
    pub byte_length: u64,
    pub digest: Sha256Digest,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverySetV1 {
    pub schema_version: u32,
    pub kind: String,
    pub recovery_tool_build_digest: Sha256Digest,
    pub manifest_digest: Sha256Digest,
    pub files: Vec<RecoverySetFile>,
}
impl RecoverySetV1 {
    pub fn validate(&self) -> Result<(), RecoveryError> {
        if self.schema_version != 1 || self.kind != "insight.platform/recovery-set/v1" {
            return Err(RecoveryError::Version);
        }
        if self.files.len() < 2 || self.files.len() > RECOVERY_MAX_EVIDENCE + 2 {
            return Err(RecoveryError::Bounds);
        }
        let mut names = BTreeSet::new();
        let mut total = 0_u64;
        for file in &self.files {
            let maximum = match file.path.as_str() {
                "manifest.json" => RECOVERY_MANIFEST_LIMITS.max_bytes as u64,
                "verification-report.json" => RECOVERY_REPORT_LIMITS.max_bytes as u64,
                other
                    if other
                        == format!(
                            "evidence/{}",
                            file.digest.as_str().trim_start_matches("sha256:")
                        ) =>
                {
                    RECOVERY_EVIDENCE_MAX_BYTES
                }
                _ => return Err(RecoveryError::Identity),
            };
            if file.byte_length == 0 || file.byte_length > maximum {
                return Err(RecoveryError::Bounds);
            }
            if !names.insert(file.path.as_str()) {
                return Err(RecoveryError::Duplicate);
            }
            total = total
                .checked_add(file.byte_length)
                .ok_or(RecoveryError::Bounds)?;
        }
        if total > RECOVERY_SET_MAX_BYTES {
            return Err(RecoveryError::Bounds);
        }
        if !names.contains("manifest.json") || !names.contains("verification-report.json") {
            return Err(RecoveryError::Incomplete);
        }
        Ok(())
    }
}
