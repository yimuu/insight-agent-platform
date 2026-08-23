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
pub const QUALIFICATION_PROFILE_VERSION: u16 = 1;
pub const QUALIFICATION_EVIDENCE_VERSION: u16 = 1;
pub const MAX_QUALIFICATION_TOOL_VERSIONS: usize = 64;
pub const MAX_QUALIFICATION_ARTIFACTS: usize = 256;
pub const MAX_QUALIFICATION_NAME_BYTES: usize = 128;
pub const MAX_QUALIFICATION_VERSION_BYTES: usize = 256;

/// Qualification layers are evidence strengths, not runtime state or promotion gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationLayer {
    L1Domain,
    L2Repository,
    L3Component,
    L4Topology,
    L5Capacity,
    L6Release,
}

impl QualificationLayer {
    pub const ALL: &'static [Self] = &[
        Self::L1Domain,
        Self::L2Repository,
        Self::L3Component,
        Self::L4Topology,
        Self::L5Capacity,
        Self::L6Release,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::L1Domain => "l1_domain",
            Self::L2Repository => "l2_repository",
            Self::L3Component => "l3_component",
            Self::L4Topology => "l4_topology",
            Self::L5Capacity => "l5_capacity",
            Self::L6Release => "l6_release",
        }
    }
}

/// Closed minimum release qualification matrix from specification 18 section 13.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationGate {
    ArtifactRoleIsolation,
    ArtifactS3KmsFaults,
    BackupRestore,
    CapacityProfile,
    CleanBaselineMigration,
    ComponentProtocols,
    CrossTenantSecurity,
    DomainContracts,
    EventOutboxRecovery,
    GitopsRolloutRollback,
    GvisorAdmissionPolicy,
    GvisorLauncherRbac,
    GvisorRuntime,
    JobLeaseLoss,
    LaneSaturation,
    McpRemoteProtocol,
    ModelInlineToolLoop,
    ReceiptReplay,
    RepositoryTransactions,
    RollingFaultRecovery,
    RunAdmissionActivationRace,
    SecretLogRedaction,
    SignedSupplyChain,
    SustainedSoak,
    UpgradeRollbackRehearsal,
    WasiRuntime,
}

impl QualificationGate {
    pub const ALL: &'static [Self] = &[
        Self::ArtifactRoleIsolation,
        Self::ArtifactS3KmsFaults,
        Self::BackupRestore,
        Self::CapacityProfile,
        Self::CleanBaselineMigration,
        Self::ComponentProtocols,
        Self::CrossTenantSecurity,
        Self::DomainContracts,
        Self::EventOutboxRecovery,
        Self::GitopsRolloutRollback,
        Self::GvisorAdmissionPolicy,
        Self::GvisorLauncherRbac,
        Self::GvisorRuntime,
        Self::JobLeaseLoss,
        Self::LaneSaturation,
        Self::McpRemoteProtocol,
        Self::ModelInlineToolLoop,
        Self::ReceiptReplay,
        Self::RepositoryTransactions,
        Self::RollingFaultRecovery,
        Self::RunAdmissionActivationRace,
        Self::SecretLogRedaction,
        Self::SignedSupplyChain,
        Self::SustainedSoak,
        Self::UpgradeRollbackRehearsal,
        Self::WasiRuntime,
    ];

    pub const fn layer(self) -> QualificationLayer {
        match self {
            Self::DomainContracts => QualificationLayer::L1Domain,
            Self::RepositoryTransactions
            | Self::CleanBaselineMigration
            | Self::RunAdmissionActivationRace
            | Self::JobLeaseLoss
            | Self::ReceiptReplay
            | Self::EventOutboxRecovery
            | Self::CrossTenantSecurity
            | Self::SecretLogRedaction => QualificationLayer::L2Repository,
            Self::ComponentProtocols
            | Self::McpRemoteProtocol
            | Self::WasiRuntime
            | Self::ModelInlineToolLoop => QualificationLayer::L3Component,
            Self::GvisorRuntime
            | Self::GvisorLauncherRbac
            | Self::GvisorAdmissionPolicy
            | Self::ArtifactRoleIsolation
            | Self::ArtifactS3KmsFaults
            | Self::RollingFaultRecovery => QualificationLayer::L4Topology,
            Self::LaneSaturation | Self::CapacityProfile | Self::SustainedSoak => {
                QualificationLayer::L5Capacity
            }
            Self::UpgradeRollbackRehearsal
            | Self::BackupRestore
            | Self::SignedSupplyChain
            | Self::GitopsRolloutRollback => QualificationLayer::L6Release,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DomainContracts => "domain_contracts",
            Self::RepositoryTransactions => "repository_transactions",
            Self::ComponentProtocols => "component_protocols",
            Self::CleanBaselineMigration => "clean_baseline_migration",
            Self::UpgradeRollbackRehearsal => "upgrade_rollback_rehearsal",
            Self::BackupRestore => "backup_restore",
            Self::RunAdmissionActivationRace => "run_admission_activation_race",
            Self::JobLeaseLoss => "job_lease_loss",
            Self::ReceiptReplay => "receipt_replay",
            Self::EventOutboxRecovery => "event_outbox_recovery",
            Self::CrossTenantSecurity => "cross_tenant_security",
            Self::SecretLogRedaction => "secret_log_redaction",
            Self::McpRemoteProtocol => "mcp_remote_protocol",
            Self::WasiRuntime => "wasi_runtime",
            Self::GvisorRuntime => "gvisor_runtime",
            Self::GvisorLauncherRbac => "gvisor_launcher_rbac",
            Self::GvisorAdmissionPolicy => "gvisor_admission_policy",
            Self::ArtifactRoleIsolation => "artifact_role_isolation",
            Self::ArtifactS3KmsFaults => "artifact_s3_kms_faults",
            Self::ModelInlineToolLoop => "model_inline_tool_loop",
            Self::LaneSaturation => "lane_saturation",
            Self::RollingFaultRecovery => "rolling_fault_recovery",
            Self::CapacityProfile => "capacity_profile",
            Self::SustainedSoak => "sustained_soak",
            Self::SignedSupplyChain => "signed_supply_chain",
            Self::GitopsRolloutRollback => "gitops_rollout_rollback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationEnvironmentClass {
    Development,
    Staging,
    Production,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationProfile {
    pub schema_version: u16,
    pub profile_name: String,
    pub environment_class: QualificationEnvironmentClass,
    pub required_gates: Vec<QualificationGate>,
    pub minimum_soak_seconds: u32,
    pub requires_runsc: bool,
    pub requires_multi_node: bool,
    pub requires_signed_supply_chain: bool,
}

impl QualificationProfile {
    pub fn validate(&self) -> Result<(), QualificationManifestError> {
        if self.schema_version != QUALIFICATION_PROFILE_VERSION {
            return Err(qualification_invalid(
                "schema_version",
                "unsupported profile version",
            ));
        }
        if !valid_qualification_name(&self.profile_name) {
            return Err(qualification_invalid(
                "profile_name",
                "profile name is invalid",
            ));
        }
        if self.required_gates.is_empty() || !strictly_sorted_unique(&self.required_gates) {
            return Err(qualification_invalid(
                "required_gates",
                "required gates must be non-empty, canonical and unique",
            ));
        }
        if self
            .required_gates
            .contains(&QualificationGate::SustainedSoak)
            != (self.minimum_soak_seconds > 0)
        {
            return Err(qualification_invalid(
                "minimum_soak_seconds",
                "soak duration must be positive exactly when sustained soak is required",
            ));
        }
        Ok(())
    }

    pub fn validate_for_production_release(&self) -> Result<(), QualificationManifestError> {
        self.validate()?;
        if self.environment_class != QualificationEnvironmentClass::Production
            || self.required_gates != QualificationGate::ALL
            || !self.requires_runsc
            || !self.requires_multi_node
            || !self.requires_signed_supply_chain
        {
            return Err(qualification_invalid(
                "required_gates",
                "production release profile must require the complete L1-L6 closure",
            ));
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, QualificationManifestError> {
        self.validate()?;
        qualification_digest(self, "qualification_profile")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationOutcome {
    Passed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationGateEvidence {
    pub gate: QualificationGate,
    pub layer: QualificationLayer,
    pub outcome: QualificationOutcome,
    pub evidence_digests: Vec<Sha256Digest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationArtifactLink {
    pub name: String,
    pub content_digest: Sha256Digest,
    pub media_type: String,
    pub byte_length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationEvidenceManifest {
    pub schema_version: u16,
    pub qualification_profile_digest: Sha256Digest,
    pub candidate_manifest_digest: Sha256Digest,
    pub topology_digest: Sha256Digest,
    pub seed: u64,
    pub started_at: UtcTimestamp,
    pub completed_at: UtcTimestamp,
    pub tool_versions: BTreeMap<String, String>,
    pub results: Vec<QualificationGateEvidence>,
    pub artifact_links: Vec<QualificationArtifactLink>,
}

impl QualificationEvidenceManifest {
    pub fn validate_against(
        &self,
        profile: &QualificationProfile,
        candidate: &CandidateManifest,
    ) -> Result<(), QualificationManifestError> {
        profile.validate()?;
        candidate.validate().map_err(|_| {
            qualification_invalid("candidate_manifest_digest", "candidate is invalid")
        })?;
        if self.schema_version != QUALIFICATION_EVIDENCE_VERSION {
            return Err(qualification_invalid(
                "schema_version",
                "unsupported evidence version",
            ));
        }
        if self.qualification_profile_digest != profile.canonical_digest()? {
            return Err(qualification_invalid(
                "qualification_profile_digest",
                "qualification profile digest does not match",
            ));
        }
        if candidate.qualification_profile_digest != self.qualification_profile_digest {
            return Err(qualification_invalid(
                "qualification_profile_digest",
                "candidate references a different qualification profile",
            ));
        }
        let candidate_digest = candidate.canonical_digest().map_err(|_| {
            qualification_invalid("candidate_manifest_digest", "candidate cannot be digested")
        })?;
        if self.candidate_manifest_digest != candidate_digest {
            return Err(qualification_invalid(
                "candidate_manifest_digest",
                "candidate manifest digest does not match",
            ));
        }
        if self.completed_at < self.started_at {
            return Err(qualification_invalid(
                "completed_at",
                "completion precedes start",
            ));
        }
        if self.seed > crate::MAX_SAFE_JSON_INTEGER {
            return Err(qualification_invalid(
                "seed",
                "seed exceeds the interoperable JSON integer range",
            ));
        }
        if self.tool_versions.is_empty()
            || self.tool_versions.len() > MAX_QUALIFICATION_TOOL_VERSIONS
            || self.tool_versions.iter().any(|(name, version)| {
                !valid_qualification_name(name)
                    || version.is_empty()
                    || version.len() > MAX_QUALIFICATION_VERSION_BYTES
                    || version.chars().any(char::is_control)
            })
        {
            return Err(qualification_invalid(
                "tool_versions",
                "tool version closure is invalid",
            ));
        }
        let result_gates = self
            .results
            .iter()
            .map(|result| result.gate)
            .collect::<Vec<_>>();
        if result_gates != profile.required_gates {
            return Err(qualification_invalid(
                "results",
                "result gates must exactly match the required profile closure",
            ));
        }
        if self.results.iter().any(|result| {
            result.layer != result.gate.layer()
                || result.evidence_digests.is_empty()
                || !strictly_sorted_unique(&result.evidence_digests)
        }) {
            return Err(qualification_invalid(
                "results",
                "gate layer and evidence digest closure are invalid",
            ));
        }
        if self.artifact_links.is_empty()
            || self.artifact_links.len() > MAX_QUALIFICATION_ARTIFACTS
            || !self
                .artifact_links
                .windows(2)
                .all(|pair| pair[0].name < pair[1].name)
            || self.artifact_links.iter().any(|artifact| {
                !valid_qualification_name(&artifact.name)
                    || artifact.byte_length == 0
                    || artifact.byte_length > crate::MAX_SAFE_JSON_INTEGER
                    || artifact.media_type.is_empty()
                    || artifact.media_type.len() > MAX_QUALIFICATION_NAME_BYTES
                    || artifact.media_type.chars().any(char::is_control)
            })
        {
            return Err(qualification_invalid(
                "artifact_links",
                "artifact links must be non-empty, bounded and canonically named",
            ));
        }
        let artifact_digests = self
            .artifact_links
            .iter()
            .map(|artifact| &artifact.content_digest)
            .collect::<BTreeSet<_>>();
        if self.results.iter().any(|result| {
            result
                .evidence_digests
                .iter()
                .any(|digest| !artifact_digests.contains(digest))
        }) {
            return Err(qualification_invalid(
                "results",
                "every gate evidence digest must resolve to a listed artifact",
            ));
        }
        Ok(())
    }

    pub fn passed(&self) -> bool {
        !self.results.is_empty()
            && self
                .results
                .iter()
                .all(|result| result.outcome == QualificationOutcome::Passed)
    }

    pub fn canonical_digest(
        &self,
        profile: &QualificationProfile,
        candidate: &CandidateManifest,
    ) -> Result<Sha256Digest, QualificationManifestError> {
        self.validate_against(profile, candidate)?;
        qualification_digest(self, "qualification_evidence")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualificationManifestError {
    pub field: &'static str,
    pub message: &'static str,
}

impl fmt::Display for QualificationManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid qualification manifest {}: {}",
            self.field, self.message
        )
    }
}

impl Error for QualificationManifestError {}

fn qualification_invalid(field: &'static str, message: &'static str) -> QualificationManifestError {
    QualificationManifestError { field, message }
}

fn qualification_digest<T: Serialize>(
    value: &T,
    field: &'static str,
) -> Result<Sha256Digest, QualificationManifestError> {
    let value = serde_json::to_value(value)
        .map_err(|_| qualification_invalid(field, "value cannot be serialized canonically"))?;
    canonical_digest(&value)
        .map_err(|_| qualification_invalid(field, "value cannot be canonicalized"))?
        .parse()
        .map_err(|_| qualification_invalid(field, "canonical digest is invalid"))
}

fn valid_qualification_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_QUALIFICATION_NAME_BYTES
        && value.as_bytes()[0].is_ascii_lowercase()
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || (index > 0 && matches!(byte, b'_' | b'-' | b'.'))
        })
}

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
            database_schema_version: 7,
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

    fn production_profile() -> QualificationProfile {
        QualificationProfile {
            schema_version: QUALIFICATION_PROFILE_VERSION,
            profile_name: "production-release".to_owned(),
            environment_class: QualificationEnvironmentClass::Production,
            required_gates: QualificationGate::ALL.to_vec(),
            minimum_soak_seconds: 86_400,
            requires_runsc: true,
            requires_multi_node: true,
            requires_signed_supply_chain: true,
        }
    }

    fn evidence(
        profile: &QualificationProfile,
        candidate: &CandidateManifest,
    ) -> QualificationEvidenceManifest {
        QualificationEvidenceManifest {
            schema_version: QUALIFICATION_EVIDENCE_VERSION,
            qualification_profile_digest: profile.canonical_digest().unwrap(),
            candidate_manifest_digest: candidate.canonical_digest().unwrap(),
            topology_digest: sha('8'),
            seed: 42,
            started_at: "2026-08-23T00:00:00.000000Z".parse().unwrap(),
            completed_at: "2026-08-24T00:00:00.000000Z".parse().unwrap(),
            tool_versions: BTreeMap::from([
                ("helm".to_owned(), "v4.0.0".to_owned()),
                ("kubectl".to_owned(), "v1.35.0".to_owned()),
            ]),
            results: profile
                .required_gates
                .iter()
                .copied()
                .map(|gate| QualificationGateEvidence {
                    gate,
                    layer: gate.layer(),
                    outcome: QualificationOutcome::Passed,
                    evidence_digests: vec![sha('7')],
                })
                .collect(),
            artifact_links: vec![QualificationArtifactLink {
                name: "qualification-bundle".to_owned(),
                content_digest: sha('7'),
                media_type: "application/zstd".to_owned(),
                byte_length: 4096,
            }],
        }
    }

    #[test]
    fn candidate_closes_exact_images_workers_limits_and_identity() {
        let workers = [
            worker("orchestration.primary", WorkClass::Orchestration, '1'),
            worker("sandbox.gvisor", WorkClass::Sandbox, '2'),
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
            worker("sandbox.gvisor", WorkClass::Sandbox, '2'),
        ];
        let candidate = candidate(&workers);

        let changed = [
            workers[0].clone(),
            worker("sandbox.gvisor", WorkClass::Sandbox, '3'),
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
                database_schema_version: 7,
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

    #[test]
    fn production_profile_requires_the_complete_l1_through_l6_matrix() {
        let profile = production_profile();
        profile.validate_for_production_release().unwrap();

        let mut missing_runsc = profile.clone();
        missing_runsc.requires_runsc = false;
        assert!(missing_runsc.validate_for_production_release().is_err());

        let mut missing_gate = profile.clone();
        missing_gate.required_gates.pop();
        assert!(missing_gate.validate_for_production_release().is_err());

        let mut unordered = profile;
        unordered.required_gates.swap(0, 1);
        assert_eq!(unordered.validate().unwrap_err().field, "required_gates");
    }

    #[test]
    fn evidence_requires_one_exact_result_for_every_profile_gate() {
        let workers = [worker(
            "orchestration.primary",
            WorkClass::Orchestration,
            '1',
        )];
        let profile = production_profile();
        let mut candidate = candidate(&workers);
        candidate.qualification_profile_digest = profile.canonical_digest().unwrap();
        let evidence = evidence(&profile, &candidate);
        evidence.validate_against(&profile, &candidate).unwrap();
        assert!(evidence.passed());
        assert_eq!(
            evidence.canonical_digest(&profile, &candidate),
            evidence.canonical_digest(&profile, &candidate)
        );

        let mut missing = evidence.clone();
        missing.results.pop();
        assert_eq!(
            missing
                .validate_against(&profile, &candidate)
                .unwrap_err()
                .field,
            "results"
        );

        let mut unlinked = evidence.clone();
        unlinked.results[0].evidence_digests = vec![sha('5')];
        assert_eq!(
            unlinked
                .validate_against(&profile, &candidate)
                .unwrap_err()
                .field,
            "results"
        );

        let mut wrong_candidate_profile = candidate.clone();
        wrong_candidate_profile.qualification_profile_digest = sha('4');
        let mut wrong_candidate_evidence = evidence.clone();
        wrong_candidate_evidence.candidate_manifest_digest =
            wrong_candidate_profile.canonical_digest().unwrap();
        assert_eq!(
            wrong_candidate_evidence
                .validate_against(&profile, &wrong_candidate_profile)
                .unwrap_err()
                .field,
            "qualification_profile_digest"
        );

        let mut wrong_layer = evidence.clone();
        wrong_layer.results[0].layer = QualificationLayer::L6Release;
        assert_eq!(
            wrong_layer
                .validate_against(&profile, &candidate)
                .unwrap_err()
                .field,
            "results"
        );

        let mut failed = evidence;
        failed.results[0].outcome = QualificationOutcome::Failed;
        failed.validate_against(&profile, &candidate).unwrap();
        assert!(!failed.passed());
    }
}
