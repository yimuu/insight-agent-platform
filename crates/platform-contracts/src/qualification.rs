use crate::{canonical_digest, HardLimitProfile, Sha256Digest, UtcTimestamp, WorkerManifest};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    str::FromStr,
};

pub const MAX_CANDIDATE_COMPONENT_IMAGES: usize = 256;
pub const MAX_CANDIDATE_WORKER_MANIFESTS: usize = 512;
pub const MAX_COMPONENT_ROLE_BYTES: usize = 128;

/// Canonical, immutable Git object identity. The algorithm tag prevents SHA-1 and SHA-256
/// repositories from assigning the same wire shape different meanings.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GitCommit(String);

impl GitCommit {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for GitCommit {
    type Err = CandidateManifestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let valid = value
            .strip_prefix("sha1:")
            .is_some_and(|hex| valid_lower_hex(hex, 40))
            || value
                .strip_prefix("sha256:")
                .is_some_and(|hex| valid_lower_hex(hex, 64));
        if !valid {
            return Err(invalid(
                "git_commit",
                "must be an algorithm-tagged lowercase Git object ID",
            ));
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Display for GitCommit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for GitCommit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for GitCommit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

/// Closed first-release process/image role used by startup and CI/CD manifests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ComponentRole {
    ManagementApi,
    RuntimeApi,
    SchedulerRecovery,
    ModelWorker,
    CapabilityNativeWorker,
    CapabilityRemoteWorker,
    ContextWorker,
    McpHost,
    SandboxController,
    SandboxWasiExecutor,
    SandboxGvisorExecutor,
    ArtifactGateway,
    ArtifactDataWorker,
    ArtifactMaintenance,
    EgressSecretBroker,
}

impl ComponentRole {
    pub const ALL: &'static [Self] = &[
        Self::ManagementApi,
        Self::RuntimeApi,
        Self::SchedulerRecovery,
        Self::ModelWorker,
        Self::CapabilityNativeWorker,
        Self::CapabilityRemoteWorker,
        Self::ContextWorker,
        Self::McpHost,
        Self::SandboxController,
        Self::SandboxWasiExecutor,
        Self::SandboxGvisorExecutor,
        Self::ArtifactGateway,
        Self::ArtifactDataWorker,
        Self::ArtifactMaintenance,
        Self::EgressSecretBroker,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManagementApi => "management_api",
            Self::RuntimeApi => "runtime_api",
            Self::SchedulerRecovery => "scheduler_recovery",
            Self::ModelWorker => "model_worker",
            Self::CapabilityNativeWorker => "capability_native_worker",
            Self::CapabilityRemoteWorker => "capability_remote_worker",
            Self::ContextWorker => "context_worker",
            Self::McpHost => "mcp_host",
            Self::SandboxController => "sandbox_controller",
            Self::SandboxWasiExecutor => "sandbox_wasi_executor",
            Self::SandboxGvisorExecutor => "sandbox_gvisor_executor",
            Self::ArtifactGateway => "artifact_gateway",
            Self::ArtifactDataWorker => "artifact_data_worker",
            Self::ArtifactMaintenance => "artifact_maintenance",
            Self::EgressSecretBroker => "egress_secret_broker",
        }
    }
}

impl FromStr for ComponentRole {
    type Err = CandidateManifestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|role| role.as_str() == value)
            .ok_or_else(|| invalid("component_images", "component role is not registered"))
    }
}

impl fmt::Display for ComponentRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for ComponentRole {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ComponentRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

/// Content-addressed CI/CD release closure. This is an artifact payload, not a runtime Resource,
/// database aggregate, public management object, or promotion authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateManifest {
    pub git_commit: GitCommit,
    pub contract_digest: Sha256Digest,
    pub database_schema_version: u32,
    pub component_images: BTreeMap<ComponentRole, Sha256Digest>,
    pub worker_manifests: Vec<Sha256Digest>,
    pub deployment_config_digest: Sha256Digest,
    pub hard_limit_profile_digest: Sha256Digest,
    pub policy_baseline_digest: Sha256Digest,
    pub qualification_profile_digest: Sha256Digest,
    pub created_at: UtcTimestamp,
}

pub struct NewCandidateManifest<'a> {
    pub git_commit: GitCommit,
    pub contract_digest: Sha256Digest,
    pub database_schema_version: u32,
    pub component_images: BTreeMap<ComponentRole, Sha256Digest>,
    pub worker_manifests: &'a [WorkerManifest],
    pub deployment_config_digest: Sha256Digest,
    pub hard_limit_profile: &'a HardLimitProfile,
    pub policy_baseline_digest: Sha256Digest,
    pub qualification_profile_digest: Sha256Digest,
    pub created_at: UtcTimestamp,
}

impl CandidateManifest {
    pub fn build(input: NewCandidateManifest<'_>) -> Result<Self, CandidateManifestError> {
        validate_worker_manifest_closure(input.worker_manifests)?;
        input
            .hard_limit_profile
            .validate()
            .map_err(|_| invalid("hard_limit_profile_digest", "hard limit profile is invalid"))?;
        let mut worker_manifests = input
            .worker_manifests
            .iter()
            .map(WorkerManifest::canonical_digest)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid("worker_manifests", "worker manifest is invalid"))?;
        worker_manifests.sort();
        let hard_limit_profile_digest =
            digest(input.hard_limit_profile, "hard_limit_profile_digest")?;
        let candidate = Self {
            git_commit: input.git_commit,
            contract_digest: input.contract_digest,
            database_schema_version: input.database_schema_version,
            component_images: input.component_images,
            worker_manifests,
            deployment_config_digest: input.deployment_config_digest,
            hard_limit_profile_digest,
            policy_baseline_digest: input.policy_baseline_digest,
            qualification_profile_digest: input.qualification_profile_digest,
            created_at: input.created_at,
        };
        candidate.validate_against(input.hard_limit_profile, input.worker_manifests)?;
        Ok(candidate)
    }

    pub fn validate(&self) -> Result<(), CandidateManifestError> {
        if self.database_schema_version == 0 {
            return Err(invalid(
                "database_schema_version",
                "database schema version must be positive",
            ));
        }
        if self.component_images.is_empty()
            || self.component_images.len() > MAX_CANDIDATE_COMPONENT_IMAGES
        {
            return Err(invalid(
                "component_images",
                "component image closure must be non-empty and bounded",
            ));
        }
        if self.worker_manifests.is_empty()
            || self.worker_manifests.len() > MAX_CANDIDATE_WORKER_MANIFESTS
            || !strictly_sorted_unique(&self.worker_manifests)
        {
            return Err(invalid(
                "worker_manifests",
                "worker manifest digests must be non-empty, bounded, sorted and unique",
            ));
        }
        Ok(())
    }

    pub fn validate_against(
        &self,
        hard_limit_profile: &HardLimitProfile,
        worker_manifests: &[WorkerManifest],
    ) -> Result<(), CandidateManifestError> {
        self.validate()?;
        hard_limit_profile
            .validate()
            .map_err(|_| invalid("hard_limit_profile_digest", "hard limit profile is invalid"))?;
        validate_worker_manifest_closure(worker_manifests)?;
        let mut expected_workers = worker_manifests
            .iter()
            .map(WorkerManifest::canonical_digest)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid("worker_manifests", "worker manifest is invalid"))?;
        expected_workers.sort();
        if expected_workers != self.worker_manifests {
            return Err(invalid(
                "worker_manifests",
                "installed worker manifests do not match the candidate closure",
            ));
        }
        if digest(hard_limit_profile, "hard_limit_profile_digest")?
            != self.hard_limit_profile_digest
        {
            return Err(invalid(
                "hard_limit_profile_digest",
                "hard limit profile does not match the candidate closure",
            ));
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, CandidateManifestError> {
        self.validate()?;
        digest(self, "candidate_manifest")
    }
}

fn validate_worker_manifest_closure(
    worker_manifests: &[WorkerManifest],
) -> Result<(), CandidateManifestError> {
    if worker_manifests.is_empty() || worker_manifests.len() > MAX_CANDIDATE_WORKER_MANIFESTS {
        return Err(invalid(
            "worker_manifests",
            "installed worker manifest closure must be non-empty and bounded",
        ));
    }
    let mut roles = BTreeSet::new();
    for manifest in worker_manifests {
        manifest
            .validate()
            .map_err(|_| invalid("worker_manifests", "worker manifest is invalid"))?;
        if !roles.insert(manifest.worker_role.as_str()) {
            return Err(invalid(
                "worker_manifests",
                "worker roles must be unique within one candidate",
            ));
        }
    }
    Ok(())
}

fn digest<T: Serialize>(
    value: &T,
    field: &'static str,
) -> Result<Sha256Digest, CandidateManifestError> {
    let value = serde_json::to_value(value)
        .map_err(|_| invalid(field, "value cannot be serialized canonically"))?;
    canonical_digest(&value)
        .map_err(|_| invalid(field, "value cannot be canonicalized"))?
        .parse()
        .map_err(|_| invalid(field, "canonical digest is invalid"))
}

fn strictly_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn valid_lower_hex(value: &str, expected_length: usize) -> bool {
    value.len() == expected_length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateManifestError {
    pub field: &'static str,
    pub message: &'static str,
}

impl fmt::Display for CandidateManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid CandidateManifest {}: {}",
            self.field, self.message
        )
    }
}

impl Error for CandidateManifestError {}

fn invalid(field: &'static str, message: &'static str) -> CandidateManifestError {
    CandidateManifestError { field, message }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        checked_in_hard_limit_profile, WorkClass, WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION,
    };
    fn sha(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn worker(role: &str, work_class: WorkClass, character: char) -> WorkerManifest {
        WorkerManifest {
            manifest_version: WORKER_MANIFEST_VERSION,
            worker_role: role.to_owned(),
            work_class,
            adapter_runtime_digest: sha(character),
            protocol_version: WORKER_PROTOCOL_VERSION,
            max_concurrency: 8,
            critical_control_reserved_slots: 1,
        }
    }

    fn build_candidate(
        workers: &[WorkerManifest],
        hard_limit_profile: &HardLimitProfile,
    ) -> Result<CandidateManifest, CandidateManifestError> {
        CandidateManifest::build(NewCandidateManifest {
            git_commit: format!("sha1:{}", "a".repeat(40)).parse().unwrap(),
            contract_digest: sha('b'),
            database_schema_version: 6,
            component_images: BTreeMap::from([
                ("runtime_api".parse().unwrap(), sha('c')),
                ("scheduler_recovery".parse().unwrap(), sha('d')),
            ]),
            worker_manifests: workers,
            deployment_config_digest: sha('e'),
            hard_limit_profile,
            policy_baseline_digest: sha('f'),
            qualification_profile_digest: sha('9'),
            created_at: "2026-08-14T12:00:00.000000Z".parse().unwrap(),
        })
    }

    fn candidate(workers: &[WorkerManifest]) -> CandidateManifest {
        build_candidate(workers, &checked_in_hard_limit_profile()).unwrap()
    }

    #[test]
    fn candidate_closes_exact_images_workers_limits_and_identity() {
        let workers = [
            worker("orchestration.primary", WorkClass::Orchestration, '1'),
            worker("sandbox.microvm", WorkClass::Sandbox, '2'),
        ];
        let candidate = candidate(&workers);
        candidate
            .validate_against(&checked_in_hard_limit_profile(), &workers)
            .unwrap();
        assert_eq!(candidate.canonical_digest(), candidate.canonical_digest());

        let mut value = serde_json::to_value(&candidate).unwrap();
        value["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<CandidateManifest>(value).is_err());
    }

    #[test]
    fn candidate_rejects_drift_duplicate_roles_and_noncanonical_worker_order() {
        let workers = [
            worker("orchestration.primary", WorkClass::Orchestration, '1'),
            worker("sandbox.microvm", WorkClass::Sandbox, '2'),
        ];
        let candidate = candidate(&workers);

        let changed = [
            workers[0].clone(),
            worker("sandbox.microvm", WorkClass::Sandbox, '3'),
        ];
        assert_eq!(
            candidate
                .validate_against(&checked_in_hard_limit_profile(), &changed)
                .unwrap_err()
                .field,
            "worker_manifests"
        );

        let duplicate_roles = [
            workers[0].clone(),
            worker("orchestration.primary", WorkClass::Sandbox, '4'),
        ];
        assert_eq!(
            CandidateManifest::build(NewCandidateManifest {
                git_commit: format!("sha1:{}", "a".repeat(40)).parse().unwrap(),
                contract_digest: sha('b'),
                database_schema_version: 6,
                component_images: BTreeMap::from([("runtime_api".parse().unwrap(), sha('c'))]),
                worker_manifests: &duplicate_roles,
                deployment_config_digest: sha('e'),
                hard_limit_profile: &checked_in_hard_limit_profile(),
                policy_baseline_digest: sha('f'),
                qualification_profile_digest: sha('9'),
                created_at: "2026-08-14T12:00:00.000000Z".parse().unwrap(),
            })
            .unwrap_err()
            .field,
            "worker_manifests"
        );

        let mut noncanonical = candidate;
        noncanonical.worker_manifests.reverse();
        assert_eq!(
            noncanonical.validate().unwrap_err().field,
            "worker_manifests"
        );
    }

    #[test]
    fn candidate_rejects_old_or_drifted_hard_limit_contracts() {
        let workers = [worker(
            "orchestration.primary",
            WorkClass::Orchestration,
            '1',
        )];
        let mut old = checked_in_hard_limit_profile();
        old.profile_version = crate::HARD_LIMIT_PROFILE_VERSION - 1;
        assert_eq!(
            build_candidate(&workers, &old).unwrap_err().field,
            "hard_limit_profile_digest"
        );

        let mut drifted = checked_in_hard_limit_profile();
        drifted
            .capability_sandbox
            .runtime_bundle_bytes
            .overflow_outcome = crate::OverflowOutcome::QuotaExceeded;
        assert_eq!(
            build_candidate(&workers, &drifted).unwrap_err().field,
            "hard_limit_profile_digest"
        );
    }

    #[test]
    fn nominal_git_and_component_roles_are_canonical_and_closed() {
        assert!(format!("sha1:{}", "a".repeat(40))
            .parse::<GitCommit>()
            .is_ok());
        assert!(format!("sha256:{}", "b".repeat(64))
            .parse::<GitCommit>()
            .is_ok());
        assert!("ABCDEF".parse::<GitCommit>().is_err());
        assert!("sandbox_gvisor_executor".parse::<ComponentRole>().is_ok());
        assert!("sandbox.microvm-provider".parse::<ComponentRole>().is_err());
        assert!("Sandbox Provider".parse::<ComponentRole>().is_err());
    }
}
