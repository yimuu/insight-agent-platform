//! Direct storage-owner commands. No SQL, grant semantics or bootstrap authority is duplicated here.
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ModelPolicyArtifactMaterialV1,
    ModelPolicyBootstrapSeedV1, ResourceId, Sha256Digest, MAX_MODEL_POLICY_DECLARATION_BYTES,
    MAX_MODEL_POLICY_MATERIAL_BYTES,
};
use insight_platform_deployment_contracts::{
    development::{DevelopmentArtifactAuthorityConfigV1, MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES},
    installation::{
        InstallationAdministratorBootstrapV1, InstallationDatabaseEvidenceV1,
        InstallationDatabasePurpose as Purpose, InstallationError as Error, InstallationIdentityV1,
        InstallationInputV1, InstallationModelPolicyArtifactInputsV1, InstallationTopology,
        INSTALLATION_LIMITS, INSTALLATION_MAX_BYTES,
    },
};
use insight_platform_deployment_tooling::private_state::InstallationDirectory;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

const JOURNAL_FILE: &str = "storage-setup.json";
const MODEL_JOURNAL_FILE: &str = "model-storage.json";
const MODEL_INPUTS_FILE: &str = "model-policy-inputs.json";
const MODEL_SEED_FILE: &str = "model-policy-seed.json";
const MODEL_MATERIAL_FILE: &str = "model-policy-material.json";
const MAX_JOURNAL_BYTES: usize = 65_536;
const PURPOSES: [Purpose; 6] = [
    Purpose::Runtime,
    Purpose::Outbox,
    Purpose::History,
    Purpose::SecurityAuthority,
    Purpose::Artifact,
    Purpose::LocalIdentity,
];
const JOURNAL_LIMITS: JsonLimits = JsonLimits {
    max_bytes: MAX_JOURNAL_BYTES,
    max_depth: 5,
    max_properties_per_object: 12,
    max_items_per_array: 6,
    max_string_bytes: 512,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StorageStage {
    Schema,
    DatabaseRoles,
    Bootstrap,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Provision,
    Verify,
}
pub struct ArtifactBootstrapFile<'a> {
    pub name: &'a str,
    pub digest: &'a Sha256Digest,
}
pub struct StorageInputs<'a> {
    pub input: &'a InstallationInputV1,
    pub identity: &'a InstallationIdentityV1,
    pub directory: &'a InstallationDirectory,
    pub binary_directory: &'a Path,
    pub artifact_bootstrap: Option<ArtifactBootstrapFile<'a>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Planned,
    Requested,
    Verified,
    Rejected,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleProgress {
    purpose: Purpose,
    phase: Phase,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    input_digest: Sha256Digest,
    identity_digest: Sha256Digest,
    installation_id: ResourceId,
    schema: Phase,
    roles: Vec<RoleProgress>,
    bootstrap: Phase,
}
impl Journal {
    fn create(inputs: &StorageInputs<'_>) -> Result<Self, Error> {
        Ok(Self {
            schema_version: 1,
            input_digest: inputs.input.digest()?,
            identity_digest: inputs.identity.digest()?,
            installation_id: inputs.identity.installation_id.clone(),
            schema: Phase::Planned,
            roles: PURPOSES
                .into_iter()
                .map(|purpose| RoleProgress {
                    purpose,
                    phase: Phase::Planned,
                })
                .collect(),
            bootstrap: Phase::Planned,
        })
    }
    fn validate(&self, inputs: &StorageInputs<'_>) -> Result<(), Error> {
        if self.schema_version != 1
            || self.input_digest != inputs.input.digest()?
            || self.identity_digest != inputs.identity.digest()?
            || self.installation_id != inputs.identity.installation_id
            || self.roles.len() != PURPOSES.len()
            || !self
                .roles
                .iter()
                .zip(PURPOSES)
                .all(|(role, purpose)| role.purpose == purpose && role.phase != Phase::Rejected)
            || self.bootstrap == Phase::Rejected
            || (self.schema != Phase::Verified
                && self.roles.iter().any(|role| role.phase != Phase::Planned))
            || (self.bootstrap != Phase::Planned
                && self.roles.iter().any(|role| role.phase != Phase::Verified))
        {
            return Err(Error::IdentityDrift);
        }
        Ok(())
    }
    fn persist(&self, directory: &InstallationDirectory) -> Result<(), Error> {
        let bytes = serde_json::to_vec(self).map_err(|_| Error::InvalidInput)?;
        if bytes.len() > MAX_JOURNAL_BYTES {
            return Err(Error::InvalidInput);
        }
        directory.replace(JOURNAL_FILE, &bytes)
    }
}

impl StorageInputs<'_> {
    fn validate(&self, stage: StorageStage) -> Result<(), Error> {
        self.input.validate()?;
        self.identity.validate()?;
        if self.identity.input_digest != self.input.digest()? {
            return Err(Error::IdentityDrift);
        }
        let installed_input = self
            .directory
            .read("input.json", INSTALLATION_MAX_BYTES)?
            .ok_or(Error::Incomplete)?;
        if InstallationInputV1::decode(&installed_input)?.digest()? != self.input.digest()? {
            return Err(Error::ConfigurationDrift);
        }
        let installed_identity = self
            .directory
            .read("identity.json", INSTALLATION_MAX_BYTES)?
            .ok_or(Error::Incomplete)?;
        let installed_identity = parse_strict_json(&installed_identity, INSTALLATION_LIMITS)
            .map_err(|_| Error::InvalidInput)?;
        let installed_identity: InstallationIdentityV1 =
            serde_json::from_value(installed_identity).map_err(|_| Error::InvalidInput)?;
        if installed_identity.digest()? != self.identity.digest()? {
            return Err(Error::IdentityDrift);
        }
        self.database_url()?;
        if stage == StorageStage::Bootstrap {
            let artifact = self.artifact_bootstrap.as_ref().ok_or(Error::Incomplete)?;
            let bytes = self
                .directory
                .read(artifact.name, MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES)?
                .ok_or(Error::Incomplete)?;
            DevelopmentArtifactAuthorityConfigV1::decode(&bytes, artifact.digest)
                .map_err(|_| Error::ConfigurationDrift)?;
            let bytes = self
                .directory
                .read("bootstrap.json", INSTALLATION_MAX_BYTES)?
                .ok_or(Error::Incomplete)?;
            let bootstrap = InstallationAdministratorBootstrapV1::decode(&bytes)?;
            if canonical(&bootstrap)? != canonical(&self.identity.bootstrap)? {
                return Err(Error::IdentityDrift);
            }
        }
        Ok(())
    }
    fn database_url(&self) -> Result<String, Error> {
        let database = &self.input.network.database;
        let host = match self.input.network.topology {
            InstallationTopology::Compose => "postgres".to_owned(),
            InstallationTopology::KubernetesLocal => {
                format!("postgres.{}.svc.cluster.local", self.input.name)
            }
            InstallationTopology::Native => "127.0.0.1".to_owned(),
        };
        let port = if self.input.network.topology == InstallationTopology::Native {
            if database.port < 1024 {
                return Err(Error::InvalidEndpoint);
            }
            database.port
        } else {
            5432
        };
        if database.host != host || database.port != port || database.database != "insight_platform"
        {
            return Err(Error::InvalidEndpoint);
        }
        let password = self
            .directory
            .read("postgres-admin-password", 32)?
            .ok_or(Error::Incomplete)?;
        if password.len() != 32 || !password.iter().all(u8::is_ascii_hexdigit) {
            return Err(Error::CredentialInvalid);
        }
        let password = String::from_utf8(password).map_err(|_| Error::CredentialInvalid)?;
        Ok(format!(
            "postgres://insight_installation_admin:{password}@{host}:{port}/insight_platform"
        ))
    }
}
fn canonical(value: &impl Serialize) -> Result<Sha256Digest, Error> {
    canonical_digest(&serde_json::to_value(value).map_err(|_| Error::InvalidInput)?)
        .map_err(|_| Error::InvalidInput)?
        .parse()
        .map_err(|_| Error::InvalidInput)
}
fn evidence_file(purpose: Purpose) -> String {
    format!("database-role-{}.json", purpose.as_str())
}

/// The command envelope carries an admin URL. Do not derive Debug or print its environment.
struct Invocation {
    binary: PathBuf,
    arguments: Vec<String>,
    environment: BTreeMap<&'static str, String>,
}
impl Invocation {
    fn new(inputs: &StorageInputs<'_>, binary: &'static str) -> Result<Self, Error> {
        if !matches!(
            binary,
            "platform-schema" | "platform-database-role" | "platform-dev-bootstrap"
        ) || !inputs.binary_directory.is_absolute()
            || !inputs.binary_directory.components().all(|part| {
                matches!(
                    part,
                    std::path::Component::RootDir | std::path::Component::Normal(_)
                )
            })
        {
            return Err(Error::InvalidPath);
        }
        for path in inputs.binary_directory.ancestors() {
            let metadata =
                std::fs::symlink_metadata(path).map_err(|_| Error::PrerequisiteUnavailable)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(Error::InvalidPath);
            }
        }
        let binary = inputs
            .binary_directory
            .join(format!("{binary}{}", std::env::consts::EXE_SUFFIX));
        let metadata =
            std::fs::symlink_metadata(&binary).map_err(|_| Error::PrerequisiteUnavailable)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() == 0 {
            return Err(Error::InvalidPath);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            if metadata.permissions().mode() & 0o111 == 0 {
                return Err(Error::InvalidPath);
            }
        }
        Ok(Self {
            binary,
            arguments: Vec::new(),
            environment: BTreeMap::new(),
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Success,
    Rejected,
    Interrupted,
}
trait Runner {
    async fn execute(&self, invocation: &Invocation) -> Result<Outcome, Error>;
}
struct ProcessRunner {
    deadline: Duration,
}
impl Runner for ProcessRunner {
    async fn execute(&self, invocation: &Invocation) -> Result<Outcome, Error> {
        let mut command = tokio::process::Command::new(&invocation.binary);
        command
            .args(&invocation.arguments)
            .env_clear()
            .envs(&invocation.environment)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match crate::process_control::execute(command, self.deadline).await? {
            crate::process_control::ChildOutcome::Exited(status) if status.success() => {
                Ok(Outcome::Success)
            }
            crate::process_control::ChildOutcome::Exited(status) if status.code().is_some() => {
                Ok(Outcome::Rejected)
            }
            _ => Ok(Outcome::Interrupted),
        }
    }
}

/// Run one phase while the caller holds the installation's exclusive private-directory lock.
/// The host must first prove that this exact input/identity and persisted random administrator
/// credential own an exclusive PostgreSQL named volume (or identity-bound Kubernetes PVC).
/// An endpoint accepting that credential alone is insufficient evidence for a shared volume.
/// Under that prerequisite, a crash after schema commit is recovered only by the schema owner's
/// complete read-only verification, followed by this identity's exact role markers and grants.
pub async fn run_storage_stage(
    inputs: &StorageInputs<'_>,
    stage: StorageStage,
    mode: Mode,
) -> Result<(), Error> {
    run_with(
        inputs,
        stage,
        mode,
        &ProcessRunner {
            deadline: Duration::from_secs(60),
        },
    )
    .await
}
async fn run_with<R: Runner>(
    inputs: &StorageInputs<'_>,
    stage: StorageStage,
    mode: Mode,
    runner: &R,
) -> Result<(), Error> {
    inputs.validate(stage)?;
    let mut journal = match inputs.directory.read(JOURNAL_FILE, MAX_JOURNAL_BYTES)? {
        Some(bytes) => serde_json::from_value(
            parse_strict_json(&bytes, JOURNAL_LIMITS).map_err(|_| Error::InvalidInput)?,
        )
        .map_err(|_| Error::InvalidInput)?,
        None if mode == Mode::Provision => {
            let journal = Journal::create(inputs)?;
            journal.persist(inputs.directory)?;
            journal
        }
        None => return Err(Error::Incomplete),
    };
    journal.validate(inputs)?;
    match stage {
        StorageStage::Schema => schema(inputs, &mut journal, mode, runner).await,
        StorageStage::DatabaseRoles => roles(inputs, &mut journal, mode, runner).await,
        StorageStage::Bootstrap => bootstrap(inputs, &mut journal, mode, runner).await,
    }
}
fn successful(outcome: Outcome, rejected: Error) -> Result<(), Error> {
    match outcome {
        Outcome::Success => Ok(()),
        Outcome::Rejected => Err(rejected),
        Outcome::Interrupted => Err(Error::ExternalOutcomeUnknown),
    }
}
async fn schema<R: Runner>(
    inputs: &StorageInputs<'_>,
    journal: &mut Journal,
    mode: Mode,
    runner: &R,
) -> Result<(), Error> {
    if journal.schema == Phase::Rejected {
        return Err(Error::SchemaMismatch);
    }
    if mode == Mode::Verify && journal.schema != Phase::Verified {
        return Err(Error::Incomplete);
    }
    let mut invocation = Invocation::new(inputs, "platform-schema")?;
    invocation
        .environment
        .insert("PLATFORM_DATABASE_URL", inputs.database_url()?);
    let fresh = journal.schema == Phase::Planned;
    invocation
        .arguments
        .push(if fresh { "provision" } else { "verify" }.into());
    if fresh {
        journal.schema = Phase::Requested;
        journal.persist(inputs.directory)?;
    }
    let outcome = runner.execute(&invocation).await?;
    if fresh && outcome == Outcome::Rejected {
        // A known rejection (including a pre-existing schema) is not uncertain completion.
        // Never turn it into an implicit adoption on a later invocation.
        journal.schema = Phase::Rejected;
        journal.persist(inputs.directory)?;
    }
    successful(outcome, Error::SchemaMismatch)?;
    if journal.schema != Phase::Verified {
        journal.schema = Phase::Verified;
        journal.persist(inputs.directory)?;
    }
    Ok(())
}
fn read_evidence(inputs: &StorageInputs<'_>, purpose: Purpose) -> Result<bool, Error> {
    let Some(bytes) = inputs
        .directory
        .read(&evidence_file(purpose), INSTALLATION_MAX_BYTES)?
    else {
        return Ok(false);
    };
    let value =
        parse_strict_json(&bytes, INSTALLATION_LIMITS).map_err(|_| Error::ConfigurationDrift)?;
    let evidence: InstallationDatabaseEvidenceV1 =
        serde_json::from_value(value).map_err(|_| Error::ConfigurationDrift)?;
    evidence.validate()?;
    if evidence.input_digest != inputs.input.digest()?
        || evidence.identity_digest != inputs.identity.digest()?
        || evidence.purpose != purpose
    {
        return Err(Error::IdentityDrift);
    }
    Ok(true)
}
async fn roles<R: Runner>(
    inputs: &StorageInputs<'_>,
    journal: &mut Journal,
    mode: Mode,
    runner: &R,
) -> Result<(), Error> {
    if journal.schema != Phase::Verified {
        return Err(Error::Incomplete);
    }
    for index in 0..journal.roles.len() {
        let role = &journal.roles[index];
        let purpose = role.purpose;
        let evidence = read_evidence(inputs, purpose)?;
        if (mode == Mode::Verify || role.phase == Phase::Verified) && !evidence {
            return Err(Error::ConfigurationDrift);
        }
        if mode == Mode::Verify && role.phase != Phase::Verified {
            return Err(Error::Incomplete);
        }
        if role.phase == Phase::Planned && evidence {
            return Err(Error::ForeignState);
        }
        let mut invocation = Invocation::new(inputs, "platform-database-role")?;
        invocation.arguments = vec![
            "--installation".into(),
            inputs.directory.path("input.json")?.display().to_string(),
            if evidence { "verify" } else { "create" }.into(),
            "--purpose".into(),
            purpose.as_str().into(),
            inputs
                .directory
                .path("input.json")?
                .parent()
                .ok_or(Error::InvalidPath)?
                .display()
                .to_string(),
            inputs
                .directory
                .path(&evidence_file(purpose))?
                .display()
                .to_string(),
        ];
        invocation.environment = BTreeMap::from([
            ("PLATFORM_DATABASE_ROLE_ADMIN_URL", inputs.database_url()?),
            (
                "PLATFORM_INSTALLATION_INPUT_DIGEST",
                inputs.input.digest()?.to_string(),
            ),
            (
                "PLATFORM_INSTALLATION_IDENTITY_DIGEST",
                inputs.identity.digest()?.to_string(),
            ),
        ]);
        if journal.roles[index].phase == Phase::Planned {
            journal.roles[index].phase = Phase::Requested;
            journal.persist(inputs.directory)?;
        }
        // Without an evidence file only the reviewed incomplete, same-marker owner command can
        // resume. The command itself rejects foreign role memberships and ownership.
        successful(
            runner.execute(&invocation).await?,
            Error::ConfigurationDrift,
        )?;
        if !read_evidence(inputs, purpose)? {
            return Err(Error::Incomplete);
        }
        if journal.roles[index].phase != Phase::Verified {
            journal.roles[index].phase = Phase::Verified;
            journal.persist(inputs.directory)?;
        }
    }
    Ok(())
}
async fn bootstrap<R: Runner>(
    inputs: &StorageInputs<'_>,
    journal: &mut Journal,
    mode: Mode,
    runner: &R,
) -> Result<(), Error> {
    if journal.schema != Phase::Verified
        || journal
            .roles
            .iter()
            .any(|role| role.phase != Phase::Verified)
    {
        return Err(Error::Incomplete);
    }
    if mode == Mode::Verify && journal.bootstrap != Phase::Verified {
        return Err(Error::Incomplete);
    }
    let artifact = inputs
        .artifact_bootstrap
        .as_ref()
        .ok_or(Error::Incomplete)?;
    let mut invocation = Invocation::new(inputs, "platform-dev-bootstrap")?;
    invocation.arguments = vec!["--installation-administrator".into()];
    if mode == Mode::Verify || journal.bootstrap == Phase::Verified {
        invocation.arguments.push("--verify".into());
    }
    invocation.environment = BTreeMap::from([
        ("PLATFORM_DATABASE_URL", inputs.database_url()?),
        (
            "PLATFORM_INSTALLATION_BOOTSTRAP_CONFIG",
            inputs
                .directory
                .path("bootstrap.json")?
                .display()
                .to_string(),
        ),
        (
            "PLATFORM_INSTALLATION_BOOTSTRAP_CONFIG_DIGEST",
            canonical(&inputs.identity.bootstrap)?.to_string(),
        ),
        (
            "PLATFORM_DEV_ARTIFACT_BOOTSTRAP_CONFIG",
            inputs.directory.path(artifact.name)?.display().to_string(),
        ),
        (
            "PLATFORM_DEV_ARTIFACT_BOOTSTRAP_CONFIG_DIGEST",
            artifact.digest.to_string(),
        ),
    ]);
    if journal.bootstrap == Phase::Planned {
        journal.bootstrap = Phase::Requested;
        journal.persist(inputs.directory)?;
    }
    successful(
        runner.execute(&invocation).await?,
        Error::ConfigurationDrift,
    )?;
    if journal.bootstrap != Phase::Verified {
        journal.bootstrap = Phase::Verified;
        journal.persist(inputs.directory)?;
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelJournal {
    schema_version: u32,
    input_digest: Sha256Digest,
    identity_digest: Sha256Digest,
    inputs: Phase,
    inputs_digest: Option<Sha256Digest>,
    model: Phase,
    seed_digest: Option<Sha256Digest>,
    material_digest: Option<Sha256Digest>,
}
impl ModelJournal {
    fn load(inputs: &StorageInputs<'_>, mode: Mode) -> Result<Self, Error> {
        inputs.validate(StorageStage::Bootstrap)?;
        let base: Journal = serde_json::from_value(
            parse_strict_json(
                &inputs
                    .directory
                    .read(JOURNAL_FILE, MAX_JOURNAL_BYTES)?
                    .ok_or(Error::Incomplete)?,
                JOURNAL_LIMITS,
            )
            .map_err(|_| Error::InvalidInput)?,
        )
        .map_err(|_| Error::InvalidInput)?;
        base.validate(inputs)?;
        if base.bootstrap != Phase::Verified {
            return Err(Error::Incomplete);
        }
        let journal: Self = match inputs
            .directory
            .read(MODEL_JOURNAL_FILE, MAX_JOURNAL_BYTES)?
        {
            Some(bytes) => serde_json::from_value(
                parse_strict_json(&bytes, JOURNAL_LIMITS).map_err(|_| Error::InvalidInput)?,
            )
            .map_err(|_| Error::InvalidInput)?,
            None if mode == Mode::Verify => return Err(Error::Incomplete),
            None => Self {
                schema_version: 1,
                input_digest: inputs.input.digest()?,
                identity_digest: inputs.identity.digest()?,
                inputs: Phase::Planned,
                inputs_digest: None,
                model: Phase::Planned,
                seed_digest: None,
                material_digest: None,
            },
        };
        if journal.schema_version != 1
            || journal.input_digest != inputs.input.digest()?
            || journal.identity_digest != inputs.identity.digest()?
            || journal.inputs == Phase::Rejected
            || (journal.inputs == Phase::Verified) != journal.inputs_digest.is_some()
            || journal.model == Phase::Rejected
            || (journal.model != Phase::Planned && journal.inputs != Phase::Verified)
            || (journal.model == Phase::Planned
                && (journal.seed_digest.is_some() || journal.material_digest.is_some()))
            || (journal.model != Phase::Planned
                && (journal.seed_digest.is_none() || journal.material_digest.is_none()))
        {
            return Err(Error::IdentityDrift);
        }
        Ok(journal)
    }
    fn save(&self, directory: &InstallationDirectory) -> Result<(), Error> {
        directory.replace(
            MODEL_JOURNAL_FILE,
            &serde_json::to_vec(self).map_err(|_| Error::InvalidInput)?,
        )
    }
}
fn model_invocation(
    inputs: &StorageInputs<'_>,
    command: &'static str,
) -> Result<Invocation, Error> {
    if !matches!(
        command,
        "--installation-model-inputs"
            | "--installation-model-bootstrap"
            | "--installation-model-verify"
    ) {
        return Err(Error::InvalidInput);
    }
    let artifact = inputs
        .artifact_bootstrap
        .as_ref()
        .ok_or(Error::Incomplete)?;
    let mut invocation = Invocation::new(inputs, "platform-dev-bootstrap")?;
    invocation.arguments = vec![command.into()];
    invocation.environment = BTreeMap::from([
        ("PLATFORM_DATABASE_URL", inputs.database_url()?),
        (
            "PLATFORM_INSTALLATION_BOOTSTRAP_CONFIG",
            inputs
                .directory
                .path("bootstrap.json")?
                .display()
                .to_string(),
        ),
        (
            "PLATFORM_INSTALLATION_BOOTSTRAP_CONFIG_DIGEST",
            canonical(&inputs.identity.bootstrap)?.to_string(),
        ),
        (
            "PLATFORM_DEV_ARTIFACT_BOOTSTRAP_CONFIG",
            inputs.directory.path(artifact.name)?.display().to_string(),
        ),
        (
            "PLATFORM_DEV_ARTIFACT_BOOTSTRAP_CONFIG_DIGEST",
            artifact.digest.to_string(),
        ),
        (
            "PLATFORM_INSTALLATION_INPUT_DIGEST",
            inputs.input.digest()?.to_string(),
        ),
        (
            "PLATFORM_INSTALLATION_IDENTITY_DIGEST",
            inputs.identity.digest()?.to_string(),
        ),
        (
            "PLATFORM_INSTALLATION_STATE_DIRECTORY",
            inputs
                .directory
                .path("input.json")?
                .parent()
                .ok_or(Error::InvalidPath)?
                .display()
                .to_string(),
        ),
    ]);
    Ok(invocation)
}
fn model_inputs_file(
    inputs: &StorageInputs<'_>,
) -> Result<Option<InstallationModelPolicyArtifactInputsV1>, Error> {
    let Some(bytes) = inputs
        .directory
        .read(MODEL_INPUTS_FILE, INSTALLATION_MAX_BYTES)?
    else {
        return Ok(None);
    };
    let frozen = InstallationModelPolicyArtifactInputsV1::decode(&bytes)?;
    frozen.validate_for(inputs.input, inputs.identity)?;
    Ok(Some(frozen))
}
/// Freeze the actual initial PostgreSQL Artifact policy tuple once. Completed invocations ask the
/// storage owner to verify immutable base identity, without choosing new mutable policy heads.
pub async fn run_model_inputs(
    inputs: &StorageInputs<'_>,
    mode: Mode,
) -> Result<InstallationModelPolicyArtifactInputsV1, Error> {
    model_inputs_with(
        inputs,
        mode,
        &ProcessRunner {
            deadline: Duration::from_secs(60),
        },
    )
    .await
}
async fn model_inputs_with<R: Runner>(
    inputs: &StorageInputs<'_>,
    mode: Mode,
    runner: &R,
) -> Result<InstallationModelPolicyArtifactInputsV1, Error> {
    let mut journal = ModelJournal::load(inputs, mode)?;
    let existing = model_inputs_file(inputs)?;
    if let Some(existing) = &existing {
        if journal
            .inputs_digest
            .as_ref()
            .is_some_and(|expected| canonical(existing).as_ref() != Ok(expected))
        {
            return Err(Error::ConfigurationDrift);
        }
    }
    if journal.inputs == Phase::Planned && existing.is_some() {
        return Err(Error::ForeignState);
    }
    if journal.inputs == Phase::Verified && existing.is_none() {
        return Err(Error::ConfigurationDrift);
    }
    if mode == Mode::Verify && journal.inputs != Phase::Verified {
        return Err(Error::Incomplete);
    }
    let invocation = model_invocation(inputs, "--installation-model-inputs")?;
    if journal.inputs == Phase::Planned {
        journal.inputs = Phase::Requested;
        journal.save(inputs.directory)?;
    }
    successful(
        runner.execute(&invocation).await?,
        Error::ConfigurationDrift,
    )?;
    let frozen = model_inputs_file(inputs)?.ok_or(Error::Incomplete)?;
    if existing.is_some_and(|existing| existing != frozen) {
        return Err(Error::ConfigurationDrift);
    }
    if journal.inputs != Phase::Verified {
        journal.inputs = Phase::Verified;
        journal.inputs_digest = Some(canonical(&frozen)?);
        journal.save(inputs.directory)?;
    }
    Ok(frozen)
}
/// Bootstrap the ordinary model Policy closure through its owning PostgreSQL command. Completed
/// phase replay is strictly read-only; unknown completion reuses the same frozen seed and material.
pub async fn run_model_policy_stage(
    inputs: &StorageInputs<'_>,
    seed_digest: &Sha256Digest,
    material_digest: &Sha256Digest,
    mode: Mode,
) -> Result<(), Error> {
    model_stage_with(
        inputs,
        seed_digest,
        material_digest,
        mode,
        &ProcessRunner {
            deadline: Duration::from_secs(60),
        },
    )
    .await
}
async fn model_stage_with<R: Runner>(
    inputs: &StorageInputs<'_>,
    seed_digest: &Sha256Digest,
    material_digest: &Sha256Digest,
    mode: Mode,
    runner: &R,
) -> Result<(), Error> {
    let mut journal = ModelJournal::load(inputs, mode)?;
    if journal.inputs != Phase::Verified {
        return Err(Error::Incomplete);
    }
    if mode == Mode::Verify && journal.model != Phase::Verified {
        return Err(Error::Incomplete);
    }
    let frozen = model_inputs_file(inputs)?.ok_or(Error::Incomplete)?;
    if journal.inputs_digest.as_ref() != Some(&canonical(&frozen)?) {
        return Err(Error::ConfigurationDrift);
    }
    let seed = ModelPolicyBootstrapSeedV1::decode(
        &inputs
            .directory
            .read(MODEL_SEED_FILE, MAX_MODEL_POLICY_DECLARATION_BYTES)?
            .ok_or(Error::Incomplete)?,
        seed_digest,
    )
    .map_err(|_| Error::ConfigurationDrift)?;
    let material = ModelPolicyArtifactMaterialV1::decode(
        &inputs
            .directory
            .read(MODEL_MATERIAL_FILE, MAX_MODEL_POLICY_MATERIAL_BYTES)?
            .ok_or(Error::Incomplete)?,
    )
    .map_err(|_| Error::ConfigurationDrift)?;
    if canonical(&material)? != *material_digest
        || seed.tenant_id != inputs.identity.bootstrap.tenant_id
        || seed.installation_principal_id != inputs.identity.bootstrap.installation.principal_id
        || seed.created_by != inputs.identity.bootstrap.administrator.principal_id
        || seed.environment != "development"
        || seed.retention_policy != frozen.retention_policy
        || seed.encryption_domain_id != frozen.encryption_domain_id
        || material.storage_binding_digest != frozen.storage_binding_digest
        || material.seed_digest != *seed_digest
    {
        return Err(Error::IdentityDrift);
    }
    material
        .validate_for(&seed, &material.content_digest, material.size_bytes)
        .map_err(|_| Error::ConfigurationDrift)?;
    if journal.model != Phase::Planned
        && (journal.seed_digest.as_ref() != Some(seed_digest)
            || journal.material_digest.as_ref() != Some(material_digest))
    {
        return Err(Error::ConfigurationDrift);
    }
    let command = if journal.model == Phase::Verified {
        "--installation-model-verify"
    } else {
        "--installation-model-bootstrap"
    };
    let mut invocation = model_invocation(inputs, command)?;
    invocation.environment.extend([
        (
            "PLATFORM_INSTALLATION_MODEL_POLICY_SEED",
            inputs
                .directory
                .path(MODEL_SEED_FILE)?
                .display()
                .to_string(),
        ),
        (
            "PLATFORM_INSTALLATION_MODEL_POLICY_SEED_DIGEST",
            seed_digest.to_string(),
        ),
        (
            "PLATFORM_INSTALLATION_MODEL_POLICY_MATERIAL",
            inputs
                .directory
                .path(MODEL_MATERIAL_FILE)?
                .display()
                .to_string(),
        ),
        (
            "PLATFORM_INSTALLATION_MODEL_POLICY_MATERIAL_DIGEST",
            material_digest.to_string(),
        ),
    ]);
    if journal.model == Phase::Planned {
        journal.model = Phase::Requested;
        journal.seed_digest = Some(seed_digest.clone());
        journal.material_digest = Some(material_digest.clone());
        journal.save(inputs.directory)?;
    }
    successful(
        runner.execute(&invocation).await?,
        Error::ConfigurationDrift,
    )?;
    if journal.model != Phase::Verified {
        journal.model = Phase::Verified;
        journal.save(inputs.directory)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_deployment_contracts::installation::InstallationDatabaseRoleEvidenceV1;
    use insight_platform_deployment_tooling::installation::{compose_input, PreparedInstallation};
    use std::{cell::RefCell, collections::VecDeque};

    struct Fixture {
        _temp: tempfile::TempDir,
        input: InstallationInputV1,
        prepared: PreparedInstallation,
        binaries: PathBuf,
    }
    impl Fixture {
        fn new() -> Self {
            Self::create(false)
        }
        fn create(_model: bool) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = std::fs::canonicalize(temp.path()).unwrap();
            let input = compose_input(
                "storage-command-test",
                format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            )
            .unwrap();
            let prepared =
                PreparedInstallation::prepare(&input, &root.join("installation")).unwrap();
            let binaries = root.join("binaries with spaces");
            std::fs::create_dir(&binaries).unwrap();
            for name in [
                "platform-schema",
                "platform-database-role",
                "platform-dev-bootstrap",
            ] {
                executable(&binaries.join(name), "#!/bin/sh\nexit 0\n");
            }
            Self {
                _temp: temp,
                input,
                prepared,
                binaries,
            }
        }
        fn inputs(&self) -> StorageInputs<'_> {
            StorageInputs {
                input: &self.input,
                identity: self.prepared.identity(),
                directory: self.prepared.directory(),
                binary_directory: &self.binaries,
                artifact_bootstrap: None,
            }
        }
        fn saved(&self) -> Journal {
            serde_json::from_slice(&self.bytes()).unwrap()
        }
        fn bytes(&self) -> Vec<u8> {
            self.prepared
                .directory()
                .read(JOURNAL_FILE, MAX_JOURNAL_BYTES)
                .unwrap()
                .unwrap()
        }
    }
    fn executable(path: &Path, source: &str) {
        std::fs::write(path, source).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
    fn evidence(inputs: &StorageInputs<'_>, purpose: Purpose) -> InstallationDatabaseEvidenceV1 {
        InstallationDatabaseEvidenceV1 {
            schema_version: 1,
            input_digest: inputs.input.digest().unwrap(),
            identity_digest: inputs.identity.digest().unwrap(),
            purpose,
            roles: purpose
                .role_names()
                .iter()
                .map(|name| InstallationDatabaseRoleEvidenceV1 {
                    role_name: (*name).into(),
                    effective_privileges_digest: format!("sha256:{}", "b".repeat(64))
                        .parse()
                        .unwrap(),
                })
                .collect(),
        }
    }
    type RecordedCall = (String, Vec<String>, Vec<&'static str>);
    struct Fake<'a> {
        inputs: &'a StorageInputs<'a>,
        calls: RefCell<Vec<RecordedCall>>,
        outcomes: RefCell<VecDeque<Outcome>>,
        produce_evidence: bool,
        model_snapshot: Option<InstallationModelPolicyArtifactInputsV1>,
    }
    impl<'a> Fake<'a> {
        fn new(inputs: &'a StorageInputs<'a>) -> Self {
            Self {
                inputs,
                calls: RefCell::default(),
                outcomes: RefCell::default(),
                produce_evidence: true,
                model_snapshot: None,
            }
        }
    }
    impl Runner for Fake<'_> {
        async fn execute(&self, invocation: &Invocation) -> Result<Outcome, Error> {
            let binary = invocation.binary.file_name().unwrap().to_str().unwrap();
            self.calls.borrow_mut().push((
                binary.into(),
                invocation.arguments.clone(),
                invocation.environment.keys().copied().collect(),
            ));
            let journal: Journal = serde_json::from_slice(
                &self
                    .inputs
                    .directory
                    .read(JOURNAL_FILE, MAX_JOURNAL_BYTES)?
                    .unwrap(),
            )
            .unwrap();
            let purpose = if binary == "platform-database-role" {
                Some(
                    PURPOSES
                        .into_iter()
                        .find(|purpose| purpose.as_str() == invocation.arguments[4])
                        .unwrap(),
                )
            } else {
                None
            };
            if invocation.arguments[0] == "provision" {
                assert!(journal.schema == Phase::Requested);
            }
            if let Some(purpose) = purpose {
                if invocation.arguments[2] == "create" {
                    assert!(
                        journal
                            .roles
                            .iter()
                            .find(|entry| entry.purpose == purpose)
                            .unwrap()
                            .phase
                            == Phase::Requested
                    );
                }
            }
            let outcome = self
                .outcomes
                .borrow_mut()
                .pop_front()
                .unwrap_or(Outcome::Success);
            if invocation.arguments[0].starts_with("--installation-model-") {
                let journal: ModelJournal = serde_json::from_slice(
                    &self
                        .inputs
                        .directory
                        .read(MODEL_JOURNAL_FILE, MAX_JOURNAL_BYTES)?
                        .unwrap(),
                )
                .unwrap();
                if invocation.arguments[0] == "--installation-model-inputs" {
                    assert!(matches!(journal.inputs, Phase::Requested | Phase::Verified));
                    if outcome == Outcome::Success
                        && self.produce_evidence
                        && self
                            .inputs
                            .directory
                            .read(MODEL_INPUTS_FILE, INSTALLATION_MAX_BYTES)?
                            .is_none()
                    {
                        self.inputs.directory.write_immutable(
                            MODEL_INPUTS_FILE,
                            &serde_json::to_vec(self.model_snapshot.as_ref().unwrap()).unwrap(),
                        )?;
                    }
                } else {
                    assert!(matches!(journal.model, Phase::Requested | Phase::Verified));
                    assert_eq!(
                        invocation.environment["PLATFORM_INSTALLATION_MODEL_POLICY_SEED"],
                        self.inputs
                            .directory
                            .path(MODEL_SEED_FILE)?
                            .display()
                            .to_string()
                    );
                    assert_eq!(
                        invocation.environment["PLATFORM_INSTALLATION_MODEL_POLICY_MATERIAL"],
                        self.inputs
                            .directory
                            .path(MODEL_MATERIAL_FILE)?
                            .display()
                            .to_string()
                    );
                }
            }
            if outcome == Outcome::Success
                && self.produce_evidence
                && invocation
                    .arguments
                    .get(2)
                    .is_some_and(|arg| arg == "create")
            {
                let purpose = purpose.unwrap();
                self.inputs.directory.write_immutable(
                    &evidence_file(purpose),
                    &serde_json::to_vec(&evidence(self.inputs, purpose)).unwrap(),
                )?;
            }
            Ok(outcome)
        }
    }

    #[tokio::test]
    async fn schema_explicit_rejection_is_never_adopted_on_resume() {
        let fixture = Fixture::new();
        let inputs = fixture.inputs();
        let runner = Fake::new(&inputs);
        runner.outcomes.borrow_mut().push_back(Outcome::Rejected);
        assert_eq!(
            run_with(&inputs, StorageStage::Schema, Mode::Provision, &runner).await,
            Err(Error::SchemaMismatch)
        );
        assert!(fixture.saved().schema == Phase::Rejected);
        assert_eq!(
            run_with(&inputs, StorageStage::Schema, Mode::Provision, &runner).await,
            Err(Error::SchemaMismatch)
        );
        assert_eq!(runner.calls.borrow().len(), 1);
    }

    #[tokio::test]
    async fn owned_volume_schema_commit_then_crash_recovers_by_readonly_verification() {
        let fixture = Fixture::new();
        let inputs = fixture.inputs();
        let runner = Fake::new(&inputs);
        // Simulate a committed schema whose response was lost. The host established exclusive
        // volume ownership before entering this module; this does not adopt a shared database.
        runner.outcomes.borrow_mut().push_back(Outcome::Interrupted);
        assert_eq!(
            run_with(&inputs, StorageStage::Schema, Mode::Provision, &runner).await,
            Err(Error::ExternalOutcomeUnknown)
        );
        assert!(fixture.saved().schema == Phase::Requested);
        run_with(&inputs, StorageStage::Schema, Mode::Provision, &runner)
            .await
            .unwrap();
        assert_eq!(runner.calls.borrow()[1].1, ["verify"]);
        let saved = fixture.bytes();
        run_with(&inputs, StorageStage::Schema, Mode::Verify, &runner)
            .await
            .unwrap();
        assert_eq!(saved, fixture.bytes());
        assert_eq!(runner.calls.borrow()[2].2, ["PLATFORM_DATABASE_URL"]);
    }

    #[tokio::test]
    async fn role_failure_preserves_intent_and_completion_requires_actual_owner_evidence() {
        let fixture = Fixture::new();
        let inputs = fixture.inputs();
        let mut runner = Fake::new(&inputs);
        run_with(&inputs, StorageStage::Schema, Mode::Provision, &runner)
            .await
            .unwrap();
        runner.outcomes.borrow_mut().push_back(Outcome::Rejected);
        assert_eq!(
            run_with(
                &inputs,
                StorageStage::DatabaseRoles,
                Mode::Provision,
                &runner
            )
            .await,
            Err(Error::ConfigurationDrift)
        );
        assert!(fixture.saved().roles[0].phase == Phase::Requested);
        runner.produce_evidence = false;
        assert_eq!(
            run_with(
                &inputs,
                StorageStage::DatabaseRoles,
                Mode::Provision,
                &runner
            )
            .await,
            Err(Error::Incomplete)
        );
        assert!(fixture.saved().roles[0].phase == Phase::Requested);
        runner.produce_evidence = true;
        run_with(
            &inputs,
            StorageStage::DatabaseRoles,
            Mode::Provision,
            &runner,
        )
        .await
        .unwrap();
        assert!(fixture
            .saved()
            .roles
            .iter()
            .all(|entry| entry.phase == Phase::Verified));
        let saved = fixture.bytes();
        runner.calls.borrow_mut().clear();
        run_with(&inputs, StorageStage::DatabaseRoles, Mode::Verify, &runner)
            .await
            .unwrap();
        assert_eq!(saved, fixture.bytes());
        let calls = runner.calls.borrow();
        assert_eq!(calls.len(), 6);
        for ((binary, args, environment), purpose) in calls.iter().zip(PURPOSES) {
            assert_eq!(binary, "platform-database-role");
            assert_eq!(args[0], "--installation");
            assert_eq!(args[2], "verify");
            assert_eq!(args[3], "--purpose");
            assert_eq!(args[4], purpose.as_str());
            assert_eq!(
                environment,
                &[
                    "PLATFORM_DATABASE_ROLE_ADMIN_URL",
                    "PLATFORM_INSTALLATION_IDENTITY_DIGEST",
                    "PLATFORM_INSTALLATION_INPUT_DIGEST"
                ]
            );
        }
    }

    #[tokio::test]
    async fn foreign_or_missing_completed_role_evidence_stops_before_child() {
        let fixture = Fixture::new();
        let inputs = fixture.inputs();
        let runner = Fake::new(&inputs);
        run_with(&inputs, StorageStage::Schema, Mode::Provision, &runner)
            .await
            .unwrap();
        let own = evidence(&inputs, Purpose::Runtime);
        inputs
            .directory
            .write_immutable(
                &evidence_file(Purpose::Runtime),
                &serde_json::to_vec(&own).unwrap(),
            )
            .unwrap();
        assert_eq!(
            run_with(
                &inputs,
                StorageStage::DatabaseRoles,
                Mode::Provision,
                &runner
            )
            .await,
            Err(Error::ForeignState)
        );
        let mut state = fixture.saved();
        state.roles[0].phase = Phase::Requested;
        state.persist(inputs.directory).unwrap();
        let mut foreign = own;
        foreign.identity_digest = format!("sha256:{}", "c".repeat(64)).parse().unwrap();
        inputs
            .directory
            .replace(
                &evidence_file(Purpose::Runtime),
                &serde_json::to_vec(&foreign).unwrap(),
            )
            .unwrap();
        assert_eq!(
            run_with(
                &inputs,
                StorageStage::DatabaseRoles,
                Mode::Provision,
                &runner
            )
            .await,
            Err(Error::IdentityDrift)
        );
        std::fs::remove_file(
            inputs
                .directory
                .path(&evidence_file(Purpose::Runtime))
                .unwrap(),
        )
        .unwrap();
        state.roles[0].phase = Phase::Verified;
        state.persist(inputs.directory).unwrap();
        assert_eq!(
            run_with(
                &inputs,
                StorageStage::DatabaseRoles,
                Mode::Provision,
                &runner
            )
            .await,
            Err(Error::ConfigurationDrift)
        );
        assert_eq!(runner.calls.borrow().len(), 1);
    }

    #[tokio::test]
    async fn verify_without_journal_and_invalid_bootstrap_never_create_state_or_start_child() {
        let fixture = Fixture::new();
        let mut inputs = fixture.inputs();
        let runner = Fake::new(&inputs);
        assert_eq!(
            run_with(&inputs, StorageStage::Schema, Mode::Verify, &runner).await,
            Err(Error::Incomplete)
        );
        assert!(inputs
            .directory
            .read(JOURNAL_FILE, MAX_JOURNAL_BYTES)
            .unwrap()
            .is_none());
        assert!(runner.calls.borrow().is_empty());
        drop(runner);
        let digest: Sha256Digest = format!("sha256:{}", "a".repeat(64)).parse().unwrap();
        inputs
            .directory
            .write_immutable("artifact-bootstrap.json", b"{}")
            .unwrap();
        inputs.artifact_bootstrap = Some(ArtifactBootstrapFile {
            name: "artifact-bootstrap.json",
            digest: &digest,
        });
        let runner = Fake::new(&inputs);
        assert_eq!(
            run_with(&inputs, StorageStage::Bootstrap, Mode::Provision, &runner).await,
            Err(Error::ConfigurationDrift)
        );
        assert!(inputs
            .directory
            .read(JOURNAL_FILE, MAX_JOURNAL_BYTES)
            .unwrap()
            .is_none());
        assert!(runner.calls.borrow().is_empty());
    }

    #[tokio::test]
    async fn journal_identity_and_closed_shape_are_checked_before_child() {
        let fixture = Fixture::new();
        let inputs = fixture.inputs();
        let runner = Fake::new(&inputs);
        let mut state = Journal::create(&inputs).unwrap();
        state.identity_digest = format!("sha256:{}", "d".repeat(64)).parse().unwrap();
        state.persist(inputs.directory).unwrap();
        assert_eq!(
            run_with(&inputs, StorageStage::Schema, Mode::Provision, &runner).await,
            Err(Error::IdentityDrift)
        );
        let mut value = serde_json::to_value(Journal::create(&inputs).unwrap()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("extra".into(), true.into());
        inputs
            .directory
            .replace(JOURNAL_FILE, &serde_json::to_vec(&value).unwrap())
            .unwrap();
        assert_eq!(
            run_with(&inputs, StorageStage::Schema, Mode::Provision, &runner).await,
            Err(Error::InvalidInput)
        );
        assert!(runner.calls.borrow().is_empty());
    }

    #[tokio::test]
    async fn bootstrap_uses_owning_config_and_completed_replay_is_readonly() {
        let fixture = Fixture::new();
        let mut inputs = fixture.inputs();
        let authority = insight_platform_deployment_tooling::bootstrap::artifact_authority(
            format!("sha256:{}", "e".repeat(64)).parse().unwrap(),
            inputs.identity.artifact_encryption_domain_id.clone(),
            None,
        )
        .unwrap();
        let digest = canonical(&authority).unwrap();
        inputs
            .directory
            .write_immutable(
                "artifact-bootstrap.json",
                &serde_json::to_vec(&authority).unwrap(),
            )
            .unwrap();
        inputs.artifact_bootstrap = Some(ArtifactBootstrapFile {
            name: "artifact-bootstrap.json",
            digest: &digest,
        });
        let runner = Fake::new(&inputs);
        run_with(&inputs, StorageStage::Schema, Mode::Provision, &runner)
            .await
            .unwrap();
        run_with(
            &inputs,
            StorageStage::DatabaseRoles,
            Mode::Provision,
            &runner,
        )
        .await
        .unwrap();
        runner.outcomes.borrow_mut().push_back(Outcome::Interrupted);
        assert_eq!(
            run_with(&inputs, StorageStage::Bootstrap, Mode::Provision, &runner).await,
            Err(Error::ExternalOutcomeUnknown)
        );
        assert!(fixture.saved().bootstrap == Phase::Requested);
        run_with(&inputs, StorageStage::Bootstrap, Mode::Provision, &runner)
            .await
            .unwrap();
        assert_eq!(
            runner.calls.borrow().last().unwrap().1,
            ["--installation-administrator"]
        );
        assert!(fixture.saved().bootstrap == Phase::Verified);
        let saved = fixture.bytes();
        run_with(&inputs, StorageStage::Bootstrap, Mode::Verify, &runner)
            .await
            .unwrap();
        assert_eq!(
            runner.calls.borrow().last().unwrap().1,
            ["--installation-administrator", "--verify"]
        );
        assert_eq!(saved, fixture.bytes());
        runner.outcomes.borrow_mut().push_back(Outcome::Rejected);
        assert_eq!(
            run_with(&inputs, StorageStage::Bootstrap, Mode::Provision, &runner).await,
            Err(Error::ConfigurationDrift)
        );
        assert_eq!(
            runner.calls.borrow().last().unwrap().1,
            ["--installation-administrator", "--verify"]
        );
        assert_eq!(saved, fixture.bytes());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn direct_child_gets_literal_arguments_closed_environment_and_null_streams() {
        let fixture = Fixture::new();
        let inputs = fixture.inputs();
        executable(&fixture.binaries.join("platform-schema"), "#!/bin/sh\n[ -z \"${HOME+x}\" ] || exit 3\n[ \"$EXPLICIT\" = 'visible fixture' ] || exit 4\n[ \"$1\" = '$(do-not-run); * spaces' ] || exit 5\nif read -r value; then exit 6; fi\nprintf 'sensitive-child-stdout'\nprintf 'sensitive-child-stderr' >&2\nexit 0\n");
        let mut invocation = Invocation::new(&inputs, "platform-schema").unwrap();
        invocation.arguments.push("$(do-not-run); * spaces".into());
        invocation
            .environment
            .insert("EXPLICIT", "visible fixture".into());
        assert_eq!(
            ProcessRunner {
                deadline: Duration::from_secs(2)
            }
            .execute(&invocation)
            .await
            .unwrap(),
            Outcome::Success
        );
        assert!(Invocation::new(&inputs, "/bin/sh").is_err());
        std::fs::remove_file(&invocation.binary).unwrap();
        std::os::unix::fs::symlink("/bin/sh", &invocation.binary).unwrap();
        assert!(matches!(
            Invocation::new(&inputs, "platform-schema"),
            Err(Error::InvalidPath)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn direct_child_nonzero_and_timeout_are_bounded_safe_outcomes() {
        let fixture = Fixture::new();
        let inputs = fixture.inputs();
        let invocation = Invocation::new(&inputs, "platform-schema").unwrap();
        executable(
            &invocation.binary,
            "#!/bin/sh\nprintf 'postgres://private' >&2\nexit 7\n",
        );
        assert_eq!(
            ProcessRunner {
                deadline: Duration::from_secs(2)
            }
            .execute(&invocation)
            .await
            .unwrap(),
            Outcome::Rejected
        );
        executable(&invocation.binary, "#!/bin/sh\nexec /bin/sleep 2\n");
        let start = std::time::Instant::now();
        assert_eq!(
            ProcessRunner {
                deadline: Duration::from_millis(20)
            }
            .execute(&invocation)
            .await
            .unwrap(),
            Outcome::Interrupted
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "timed out direct child must be killed and reaped promptly"
        );
    }

    fn model_base(fixture: &Fixture) -> (Sha256Digest, InstallationModelPolicyArtifactInputsV1) {
        let inputs = fixture.inputs();
        let storage: Sha256Digest = format!("sha256:{}", "e".repeat(64)).parse().unwrap();
        let artifact = insight_platform_deployment_tooling::bootstrap::artifact_authority(
            storage.clone(),
            inputs.identity.artifact_encryption_domain_id.clone(),
            None,
        )
        .unwrap();
        let artifact_digest = canonical(&artifact).unwrap();
        inputs
            .directory
            .write_immutable(
                "artifact-bootstrap.json",
                &serde_json::to_vec(&artifact).unwrap(),
            )
            .unwrap();
        // The runner fixture stands in for the separately tested owning PG bootstrap stage.
        let mut base = Journal::create(&inputs).unwrap();
        base.schema = Phase::Verified;
        for role in &mut base.roles {
            role.phase = Phase::Verified;
        }
        base.bootstrap = Phase::Verified;
        base.persist(inputs.directory).unwrap();
        let frozen = InstallationModelPolicyArtifactInputsV1 {
            schema_version: 1,
            input_digest: inputs.input.digest().unwrap(),
            identity_digest: inputs.identity.digest().unwrap(),
            retention_policy: insight_platform_contracts::ExactVersionRef::new(
                artifact.retention_policy_revision_id,
                format!("sha256:{}", "f".repeat(64)).parse().unwrap(),
            )
            .unwrap(),
            encryption_domain_id: inputs.identity.artifact_encryption_domain_id.clone(),
            storage_binding_digest: storage,
        };
        (artifact_digest, frozen)
    }
    #[tokio::test]
    async fn model_inputs_failure_and_recovery_freeze_exact_tuple_and_reject_later_drift() {
        let fixture = Fixture::create(true);
        let (artifact_digest, snapshot) = model_base(&fixture);
        let mut inputs = fixture.inputs();
        inputs.artifact_bootstrap = Some(ArtifactBootstrapFile {
            name: "artifact-bootstrap.json",
            digest: &artifact_digest,
        });
        let mut runner = Fake::new(&inputs);
        runner.model_snapshot = Some(snapshot.clone());
        runner.outcomes.borrow_mut().push_back(Outcome::Interrupted);
        assert_eq!(
            model_inputs_with(&inputs, Mode::Provision, &runner).await,
            Err(Error::ExternalOutcomeUnknown)
        );
        assert!(ModelJournal::load(&inputs, Mode::Provision).unwrap().inputs == Phase::Requested);
        runner.produce_evidence = false;
        assert_eq!(
            model_inputs_with(&inputs, Mode::Provision, &runner).await,
            Err(Error::Incomplete)
        );
        runner.produce_evidence = true;
        assert_eq!(
            model_inputs_with(&inputs, Mode::Provision, &runner)
                .await
                .unwrap(),
            snapshot
        );
        let journal = inputs
            .directory
            .read(MODEL_JOURNAL_FILE, MAX_JOURNAL_BYTES)
            .unwrap()
            .unwrap();
        assert_eq!(
            model_inputs_with(&inputs, Mode::Verify, &runner)
                .await
                .unwrap(),
            snapshot
        );
        assert_eq!(
            journal,
            inputs
                .directory
                .read(MODEL_JOURNAL_FILE, MAX_JOURNAL_BYTES)
                .unwrap()
                .unwrap()
        );
        let before = runner.calls.borrow().len();
        let mut drift = snapshot;
        drift.storage_binding_digest = format!("sha256:{}", "d".repeat(64)).parse().unwrap();
        inputs
            .directory
            .replace(MODEL_INPUTS_FILE, &serde_json::to_vec(&drift).unwrap())
            .unwrap();
        assert_eq!(
            model_inputs_with(&inputs, Mode::Verify, &runner).await,
            Err(Error::ConfigurationDrift)
        );
        assert_eq!(before, runner.calls.borrow().len());
    }
    #[tokio::test]
    async fn model_policy_unknown_completion_reuses_seed_and_completed_replay_is_readonly() {
        let fixture = Fixture::create(true);
        let (artifact_digest, snapshot) = model_base(&fixture);
        let mut inputs = fixture.inputs();
        inputs.artifact_bootstrap = Some(ArtifactBootstrapFile {
            name: "artifact-bootstrap.json",
            digest: &artifact_digest,
        });
        let mut runner = Fake::new(&inputs);
        runner.model_snapshot = Some(snapshot.clone());
        model_inputs_with(&inputs, Mode::Provision, &runner)
            .await
            .unwrap();
        let seed = crate::model_setup::prepare_seed(
            inputs.input,
            inputs.identity,
            inputs.directory,
            &snapshot,
            3600,
            crate::model_setup::Mode::Provision,
        )
        .unwrap();
        let built =
            insight_platform_registry::model_policy_bootstrap::build_model_policy_bootstrap(&seed)
                .unwrap();
        let seed_digest = seed.canonical_digest().unwrap();
        let generation = "fixture-generation";
        let material=ModelPolicyArtifactMaterialV1{schema_version:1,seed_digest:seed_digest.clone(),content_digest:built.content_digest,size_bytes:built.declaration_bytes.len()as u64,storage_backend:"s3".into(),storage_binding_digest:snapshot.storage_binding_digest.clone(),object_reference_ciphertext:vec![7;32],object_generation:generation.into(),key_id:"arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000001".into(),backend_evidence_digest:canonical(&serde_json::json!({"schema_version":1,"kind":"s3_workload_stage","tenant_id":seed.tenant_id,"artifact_id":seed.authoring_artifact_id,"blob_id":seed.authoring_blob_id,"object_generation":generation,"size_bytes":built.declaration_bytes.len(),"storage_binding_digest":snapshot.storage_binding_digest})).unwrap()};
        let material_digest = canonical(&material).unwrap();
        inputs
            .directory
            .write_immutable(MODEL_MATERIAL_FILE, &serde_json::to_vec(&material).unwrap())
            .unwrap();
        runner.outcomes.borrow_mut().push_back(Outcome::Interrupted);
        assert_eq!(
            model_stage_with(
                &inputs,
                &seed_digest,
                &material_digest,
                Mode::Provision,
                &runner
            )
            .await,
            Err(Error::ExternalOutcomeUnknown)
        );
        let journal = ModelJournal::load(&inputs, Mode::Provision).unwrap();
        assert!(journal.model == Phase::Requested);
        assert_eq!(journal.seed_digest.as_ref(), Some(&seed_digest));
        model_stage_with(
            &inputs,
            &seed_digest,
            &material_digest,
            Mode::Provision,
            &runner,
        )
        .await
        .unwrap();
        assert_eq!(
            runner.calls.borrow().last().unwrap().1,
            ["--installation-model-bootstrap"]
        );
        let saved = inputs
            .directory
            .read(MODEL_JOURNAL_FILE, MAX_JOURNAL_BYTES)
            .unwrap()
            .unwrap();
        model_stage_with(
            &inputs,
            &seed_digest,
            &material_digest,
            Mode::Verify,
            &runner,
        )
        .await
        .unwrap();
        assert_eq!(
            runner.calls.borrow().last().unwrap().1,
            ["--installation-model-verify"]
        );
        assert_eq!(
            saved,
            inputs
                .directory
                .read(MODEL_JOURNAL_FILE, MAX_JOURNAL_BYTES)
                .unwrap()
                .unwrap()
        );
        let mut changed = material;
        changed.object_reference_ciphertext[0] ^= 1;
        inputs
            .directory
            .replace(MODEL_MATERIAL_FILE, &serde_json::to_vec(&changed).unwrap())
            .unwrap();
        let before = runner.calls.borrow().len();
        assert_eq!(
            model_stage_with(
                &inputs,
                &seed_digest,
                &canonical(&changed).unwrap(),
                Mode::Provision,
                &runner
            )
            .await,
            Err(Error::ConfigurationDrift)
        );
        assert_eq!(before, runner.calls.borrow().len());
    }
}
