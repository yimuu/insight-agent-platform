//! One-shot development-profile bootstrap for a fresh Platform PostgreSQL authority.
//!
//! This binary is intentionally separate from the public Gateway. It accepts only a canonical,
//! digest-pinned development config and creates the initial tenant/principal rows through the
//! repository authority. Runtime services retain zero DDL privileges and the `insight` CLI never
//! links a PostgreSQL client.

use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ModelPolicyArtifactMaterialV1,
    ModelPolicyBootstrapSeedV1, Permission, PermissionSet, PrincipalBindingsPayload, PrincipalKind,
    ResourceId, ResourceKind, Sha256Digest, TenantConfig, TenantPrincipalPayload,
    MAX_MODEL_POLICY_DECLARATION_BYTES, MAX_MODEL_POLICY_MATERIAL_BYTES,
};
use insight_platform_deployment_contracts::development::{
    DevelopmentArtifactAuthorityConfigV1, MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES,
};
use insight_platform_deployment_contracts::installation::{
    InstallationIdentityV1, InstallationInputV1, InstallationModelPolicyArtifactInputsV1,
    INSTALLATION_LIMITS, INSTALLATION_MAX_BYTES,
};
use insight_platform_postgres::{
    repository::{
        BootstrapDevelopmentProfile, BootstrapInstallationOperator, BootstrapOutcome,
        DevelopmentArtifactAuthoritySeed, NewPrincipal, NewTenant, NewTenantPrincipal,
        PgRepository,
    },
    verify_schema,
};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{
    error::Error,
    fmt,
    fs::File,
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
};

const CONFIG_PATH_ENV: &str = "PLATFORM_DEV_BOOTSTRAP_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_DEV_BOOTSTRAP_CONFIG_DIGEST";
const ARTIFACT_CONFIG_PATH_ENV: &str = "PLATFORM_DEV_ARTIFACT_BOOTSTRAP_CONFIG";
const ARTIFACT_CONFIG_DIGEST_ENV: &str = "PLATFORM_DEV_ARTIFACT_BOOTSTRAP_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_DATABASE_URL";
const MAX_CONFIG_BYTES: usize = 65_536;
const MODEL_INPUTS_FILE: &str = "model-policy-inputs.json";
const MODEL_INPUTS_TEMP: &str = ".model-policy-inputs.pending.json";
const MODEL_SEED_FILE: &str = "model-policy-seed.json";
const MODEL_MATERIAL_FILE: &str = "model-policy-material.json";
const STATE_DIRECTORY_ENV: &str = "PLATFORM_INSTALLATION_STATE_DIRECTORY";
const MODEL_SEED_ENV: &str = "PLATFORM_INSTALLATION_MODEL_POLICY_SEED";
const MODEL_SEED_DIGEST_ENV: &str = "PLATFORM_INSTALLATION_MODEL_POLICY_SEED_DIGEST";
const MODEL_MATERIAL_ENV: &str = "PLATFORM_INSTALLATION_MODEL_POLICY_MATERIAL";
const MODEL_MATERIAL_DIGEST_ENV: &str = "PLATFORM_INSTALLATION_MODEL_POLICY_MATERIAL_DIGEST";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema_version: u32,
    environment_class: String,
    installation: InstallationConfig,
    developer: DeveloperConfig,
    registry_validator: ServiceIdentityConfig,
    egress_broker: ServiceIdentityConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallationConfig {
    principal_id: String,
    request_id: String,
    authentication_authority_digest: String,
    subject_digest: String,
    evidence_digest: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeveloperConfig {
    tenant_id: String,
    principal_id: String,
    authentication_authority_digest: String,
    subject_digest: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceIdentityConfig {
    principal_id: String,
    authentication_authority_digest: String,
    subject_digest: String,
}

struct BootstrapInput {
    session_principal_kind: PrincipalKind,
    installation_principal_id: ResourceId,
    installation_request_id: ResourceId,
    installation_authentication_authority_digest: Sha256Digest,
    installation_subject_digest: Sha256Digest,
    installation_evidence_digest: Sha256Digest,
    tenant_id: ResourceId,
    developer_principal_id: ResourceId,
    developer_authentication_authority_digest: Sha256Digest,
    developer_subject_digest: Sha256Digest,
    registry_validator_principal_id: ResourceId,
    registry_validator_authentication_authority_digest: Sha256Digest,
    registry_validator_subject_digest: Sha256Digest,
    egress_broker: BootstrapServiceIdentity,
}

struct BootstrapServiceIdentity {
    principal_id: ResourceId,
    authentication_authority_digest: Sha256Digest,
    subject_digest: Sha256Digest,
}

impl Config {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(CONFIG_PATH_ENV)?;
        let bytes = read_bounded_file(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 8,
                max_properties_per_object: 16,
                max_items_per_array: 1,
                max_string_bytes: 512,
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let expected: Sha256Digest = required(CONFIG_DIGEST_ENV)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let actual: Sha256Digest = canonical_digest(&value)
            .map_err(|_| ProcessError::InvalidConfiguration)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if actual != expected {
            return Err(ProcessError::InvalidConfiguration);
        }
        let config: Self =
            serde_json::from_value(value).map_err(|_| ProcessError::InvalidConfiguration)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<BootstrapInput, ProcessError> {
        if self.schema_version != 2 || self.environment_class != "development" {
            return Err(ProcessError::InvalidConfiguration);
        }
        let installation_principal_id =
            parse_id(&self.installation.principal_id, ResourceKind::Principal)?;
        let installation_request_id =
            parse_id(&self.installation.request_id, ResourceKind::ServerRequest)?;
        let tenant_id = parse_id(&self.developer.tenant_id, ResourceKind::Tenant)?;
        let developer_principal_id =
            parse_id(&self.developer.principal_id, ResourceKind::Principal)?;
        let registry_validator_principal_id = parse_id(
            &self.registry_validator.principal_id,
            ResourceKind::Principal,
        )?;
        let egress_broker = BootstrapServiceIdentity {
            principal_id: parse_id(&self.egress_broker.principal_id, ResourceKind::Principal)?,
            authentication_authority_digest: parse_digest(
                &self.egress_broker.authentication_authority_digest,
            )?,
            subject_digest: parse_digest(&self.egress_broker.subject_digest)?,
        };
        if installation_principal_id == developer_principal_id
            || installation_principal_id == registry_validator_principal_id
            || developer_principal_id == registry_validator_principal_id
            || egress_broker.principal_id == installation_principal_id
            || egress_broker.principal_id == developer_principal_id
            || egress_broker.principal_id == registry_validator_principal_id
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(BootstrapInput {
            session_principal_kind: PrincipalKind::AgentAuthor,
            installation_principal_id,
            installation_request_id,
            installation_authentication_authority_digest: parse_digest(
                &self.installation.authentication_authority_digest,
            )?,
            installation_subject_digest: parse_digest(&self.installation.subject_digest)?,
            installation_evidence_digest: parse_digest(&self.installation.evidence_digest)?,
            tenant_id,
            developer_principal_id,
            developer_authentication_authority_digest: parse_digest(
                &self.developer.authentication_authority_digest,
            )?,
            developer_subject_digest: parse_digest(&self.developer.subject_digest)?,
            registry_validator_principal_id,
            registry_validator_authentication_authority_digest: parse_digest(
                &self.registry_validator.authentication_authority_digest,
            )?,
            registry_validator_subject_digest: parse_digest(
                &self.registry_validator.subject_digest,
            )?,
            egress_broker,
        })
    }
}

fn load_artifact_authority(
    private: bool,
) -> Result<DevelopmentArtifactAuthoritySeed, ProcessError> {
    let path = required_absolute_path(ARTIFACT_CONFIG_PATH_ENV)?;
    let bytes = if private {
        read_private_file(&path, MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES)?
    } else {
        read_bounded_file(&path, MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES)?
    };
    let expected = required(ARTIFACT_CONFIG_DIGEST_ENV)?
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let config = DevelopmentArtifactAuthorityConfigV1::decode(&bytes, &expected)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    Ok(DevelopmentArtifactAuthoritySeed {
        authoring_artifact_id: config.authoring_artifact_id,
        authoring_blob_id: config.authoring_blob_id,
        retention_policy_id: config.retention_policy_id,
        retention_policy_revision_id: config.retention_policy_revision_id,
        retention_policy_deployment_id: config.retention_policy_deployment_id,
        artifact_io_policy_id: config.artifact_io_policy_id,
        artifact_io_policy_revision_id: config.artifact_io_policy_revision_id,
        artifact_io_policy_deployment_id: config.artifact_io_policy_deployment_id,
        scheduling_policy_id: config.scheduling_policy_id,
        scheduling_policy_revision_id: config.scheduling_policy_revision_id,
        scheduling_policy_deployment_id: config.scheduling_policy_deployment_id,
        staging_quota_account_id: config.staging_quota_account_id,
        orchestration_quota_account_id: config.orchestration_quota_account_id,
        retention_policy: config.retention_policy,
        artifact_io_policy: config.artifact_io_policy,
        scheduling_policy: config.scheduling_policy,
        staging_quota_bytes: config.staging_quota_bytes,
        orchestration_concurrent_jobs: config.orchestration_concurrent_jobs,
    })
}

fn parse_id(value: &str, expected: ResourceKind) -> Result<ResourceId, ProcessError> {
    ResourceId::parse_expected(value, expected).map_err(|_| ProcessError::InvalidConfiguration)
}

fn parse_digest(value: &str) -> Result<Sha256Digest, ProcessError> {
    value
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)
}

#[derive(Debug)]
enum ProcessError {
    Usage,
    MissingEnvironment(&'static str),
    InvalidConfiguration,
    ReadConfiguration(std::io::Error),
    Database(sqlx::Error),
    Schema(insight_platform_postgres::AuthoritySchemaError),
    Repository(insight_platform_postgres::repository::RepositoryError),
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(formatter, "usage: platform-dev-bootstrap"),
            Self::MissingEnvironment(name) => write!(formatter, "{name} is required"),
            Self::InvalidConfiguration => {
                write!(formatter, "development bootstrap configuration is invalid")
            }
            Self::ReadConfiguration(_) => {
                formatter.write_str("bootstrap configuration is unavailable")
            }
            Self::Database(_) => formatter.write_str("PostgreSQL authority is unavailable"),
            Self::Schema(_) => formatter.write_str("PostgreSQL schema is not verified"),
            Self::Repository(_) => formatter.write_str("development bootstrap was rejected"),
        }
    }
}

impl Error for ProcessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ReadConfiguration(error) => Some(error),
            Self::Database(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::Repository(error) => Some(error),
            Self::Usage | Self::MissingEnvironment(_) | Self::InvalidConfiguration => None,
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        fail(error);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    Native,
    Administrator,
    VerifyAdministrator,
    ModelInputs,
    BootstrapModel,
    VerifyModel,
}
impl Command {
    fn parse(arguments: &[String]) -> Result<Self, ProcessError> {
        match arguments {
            [] => Ok(Self::Native),
            [mode] if mode == "--installation-administrator" => Ok(Self::Administrator),
            [mode, verify] if mode == "--installation-administrator" && verify == "--verify" => {
                Ok(Self::VerifyAdministrator)
            }
            [mode] if mode == "--installation-model-inputs" => Ok(Self::ModelInputs),
            [mode] if mode == "--installation-model-bootstrap" => Ok(Self::BootstrapModel),
            [mode] if mode == "--installation-model-verify" => Ok(Self::VerifyModel),
            _ => Err(ProcessError::Usage),
        }
    }
}
async fn run() -> Result<(), ProcessError> {
    let command = Command::parse(&std::env::args().skip(1).collect::<Vec<_>>())?;
    let input = if command == Command::Native {
        Config::load()?.validate()?
    } else {
        load_installation_administrator()?
    };
    let profile = build_profile(input, load_artifact_authority(command != Command::Native)?)?;
    let context = if matches!(
        command,
        Command::ModelInputs | Command::BootstrapModel | Command::VerifyModel
    ) {
        Some(ModelContext::load(&profile)?)
    } else {
        None
    };
    let model = if matches!(command, Command::BootstrapModel | Command::VerifyModel) {
        Some(
            context
                .as_ref()
                .ok_or(ProcessError::InvalidConfiguration)?
                .model()?,
        )
    } else {
        None
    };
    let database_url = required(DATABASE_URL_ENV)?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .map_err(ProcessError::Database)?;
    verify_schema(&pool).await.map_err(ProcessError::Schema)?;
    let repository = PgRepository::new(pool);
    match command {
        Command::Native | Command::Administrator => {
            let outcome = repository
                .bootstrap_development_profile(profile)
                .await
                .map_err(ProcessError::Repository)?;
            println!(
                "development tenant and developer principal {}",
                match outcome {
                    BootstrapOutcome::Created => "created",
                    BootstrapOutcome::Replayed => "verified",
                }
            );
        }
        Command::VerifyAdministrator => repository
            .verify_installation_profile(&profile)
            .await
            .map_err(ProcessError::Repository)?,
        Command::ModelInputs => {
            context
                .as_ref()
                .ok_or(ProcessError::InvalidConfiguration)?
                .freeze_inputs(&repository, &profile)
                .await?
        }
        Command::BootstrapModel | Command::VerifyModel => {
            let (seed, material) = model.ok_or(ProcessError::InvalidConfiguration)?;
            if command == Command::VerifyModel {
                repository
                    .verify_model_policy_authority(&profile, &seed, &material)
                    .await
                    .map_err(ProcessError::Repository)?;
            } else {
                repository
                    .bootstrap_model_policy_authority(&profile, &seed, &material)
                    .await
                    .map_err(ProcessError::Repository)?;
            }
        }
    }
    Ok(())
}

struct ModelContext {
    root: PathBuf,
    input: InstallationInputV1,
    identity: InstallationIdentityV1,
}
impl ModelContext {
    fn load(profile: &BootstrapDevelopmentProfile) -> Result<Self, ProcessError> {
        let root = required_absolute_path(STATE_DIRECTORY_ENV)?;
        check_private_directory(&root)?;
        let input = InstallationInputV1::decode(&read_private_file(
            &root.join("input.json"),
            INSTALLATION_MAX_BYTES,
        )?)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let encoded = read_private_file(&root.join("identity.json"), INSTALLATION_MAX_BYTES)?;
        let identity: InstallationIdentityV1 = serde_json::from_value(
            parse_strict_json(&encoded, INSTALLATION_LIMITS)
                .map_err(|_| ProcessError::InvalidConfiguration)?,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let input_digest = input
            .digest()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let identity_digest = identity
            .digest()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        if identity.input_digest != input_digest
            || required("PLATFORM_INSTALLATION_INPUT_DIGEST")? != input_digest.as_str()
            || required("PLATFORM_INSTALLATION_IDENTITY_DIGEST")? != identity_digest.as_str()
            || profile.tenant.tenant_id != identity.bootstrap.tenant_id.to_string()
            || profile.installation.principal_id != identity.bootstrap.installation.principal_id
            || profile.developer.principal_id != identity.bootstrap.administrator.principal_id
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        if required_absolute_path("PLATFORM_INSTALLATION_BOOTSTRAP_CONFIG")?
            != root.join("bootstrap.json")
            || required_absolute_path(ARTIFACT_CONFIG_PATH_ENV)?.parent() != Some(root.as_path())
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        let bootstrap = read_private_file(&root.join("bootstrap.json"), MAX_CONFIG_BYTES)?;
        let bootstrap=insight_platform_deployment_contracts::installation::InstallationAdministratorBootstrapV1::decode(&bootstrap).map_err(|_|ProcessError::InvalidConfiguration)?;
        if canonical_digest(
            &serde_json::to_value(bootstrap).map_err(|_| ProcessError::InvalidConfiguration)?,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?
            != canonical_digest(
                &serde_json::to_value(&identity.bootstrap)
                    .map_err(|_| ProcessError::InvalidConfiguration)?,
            )
            .map_err(|_| ProcessError::InvalidConfiguration)?
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        read_private_file(
            &required_absolute_path(ARTIFACT_CONFIG_PATH_ENV)?,
            MAX_DEVELOPMENT_ARTIFACT_BOOTSTRAP_BYTES,
        )?;
        Ok(Self {
            root,
            input,
            identity,
        })
    }
    fn decode_inputs(
        &self,
        bytes: &[u8],
    ) -> Result<InstallationModelPolicyArtifactInputsV1, ProcessError> {
        let inputs = InstallationModelPolicyArtifactInputsV1::decode(bytes)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        inputs
            .validate_for(&self.input, &self.identity)
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        Ok(inputs)
    }
    async fn freeze_inputs(
        &self,
        repository: &PgRepository,
        profile: &BootstrapDevelopmentProfile,
    ) -> Result<(), ProcessError> {
        let target = self.root.join(MODEL_INPUTS_FILE);
        let temporary = self.root.join(MODEL_INPUTS_TEMP);
        if exists(&target)? {
            self.decode_inputs(&read_private_file(&target, INSTALLATION_MAX_BYTES)?)?;
            if exists(&temporary)? {
                return Err(ProcessError::InvalidConfiguration);
            }
            return repository
                .verify_installation_profile(profile)
                .await
                .map_err(ProcessError::Repository);
        }
        if exists(&temporary)? {
            self.decode_inputs(&read_private_file(&temporary, INSTALLATION_MAX_BYTES)?)?;
            repository
                .verify_installation_profile(profile)
                .await
                .map_err(ProcessError::Repository)?;
        } else {
            let (retention_policy, encryption_domain_id, storage_binding_digest) = repository
                .read_model_policy_bootstrap_artifact_inputs(profile)
                .await
                .map_err(ProcessError::Repository)?;
            let inputs = InstallationModelPolicyArtifactInputsV1 {
                schema_version: 1,
                input_digest: self
                    .input
                    .digest()
                    .map_err(|_| ProcessError::InvalidConfiguration)?,
                identity_digest: self
                    .identity
                    .digest()
                    .map_err(|_| ProcessError::InvalidConfiguration)?,
                retention_policy,
                encryption_domain_id,
                storage_binding_digest,
            };
            inputs
                .validate_for(&self.input, &self.identity)
                .map_err(|_| ProcessError::InvalidConfiguration)?;
            let bytes = serde_json::to_vec_pretty(&inputs)
                .map_err(|_| ProcessError::InvalidConfiguration)?;
            write_new_private(&temporary, &bytes)?;
        }
        rename_private_exclusive(&temporary, &target)
    }
    fn model(
        &self,
    ) -> Result<(ModelPolicyBootstrapSeedV1, ModelPolicyArtifactMaterialV1), ProcessError> {
        let seed_path = required_absolute_path(MODEL_SEED_ENV)?;
        let material_path = required_absolute_path(MODEL_MATERIAL_ENV)?;
        if seed_path != self.root.join(MODEL_SEED_FILE)
            || material_path != self.root.join(MODEL_MATERIAL_FILE)
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        let seed_digest: Sha256Digest = required(MODEL_SEED_DIGEST_ENV)?
            .parse()
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let seed = ModelPolicyBootstrapSeedV1::decode(
            &read_private_file(&seed_path, MAX_MODEL_POLICY_DECLARATION_BYTES)?,
            &seed_digest,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let material = ModelPolicyArtifactMaterialV1::decode(&read_private_file(
            &material_path,
            MAX_MODEL_POLICY_MATERIAL_BYTES,
        )?)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let material_digest = canonical_digest(
            &serde_json::to_value(&material).map_err(|_| ProcessError::InvalidConfiguration)?,
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        if material_digest != required(MODEL_MATERIAL_DIGEST_ENV)? {
            return Err(ProcessError::InvalidConfiguration);
        }
        let frozen = self.decode_inputs(&read_private_file(
            &self.root.join(MODEL_INPUTS_FILE),
            INSTALLATION_MAX_BYTES,
        )?)?;
        if seed.tenant_id != self.identity.bootstrap.tenant_id
            || seed.installation_principal_id != self.identity.bootstrap.installation.principal_id
            || seed.created_by != self.identity.bootstrap.administrator.principal_id
            || seed.environment != "development"
            || seed.retention_policy != frozen.retention_policy
            || seed.encryption_domain_id != frozen.encryption_domain_id
            || material.storage_binding_digest != frozen.storage_binding_digest
            || material.seed_digest != seed_digest
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok((seed, material))
    }
}
fn exists(path: &Path) -> Result<bool, ProcessError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(ProcessError::ReadConfiguration(error)),
    }
}
fn check_private_directory(path: &Path) -> Result<(), ProcessError> {
    if !path.is_absolute()
        || path.components().any(|part| {
            !matches!(
                part,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )
        })
    {
        return Err(ProcessError::InvalidConfiguration);
    }
    for ancestor in path.ancestors() {
        let metadata =
            std::fs::symlink_metadata(ancestor).map_err(ProcessError::ReadConfiguration)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(ProcessError::InvalidConfiguration);
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if std::fs::metadata(path)
            .map_err(ProcessError::ReadConfiguration)?
            .mode()
            & 0o7777
            != 0o700
        {
            return Err(ProcessError::InvalidConfiguration);
        }
    }
    Ok(())
}
fn read_private_file(path: &Path, limit: usize) -> Result<Vec<u8>, ProcessError> {
    check_private_directory(path.parent().ok_or(ProcessError::InvalidConfiguration)?)?;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(ProcessError::ReadConfiguration)?;
    let metadata = file.metadata().map_err(ProcessError::ReadConfiguration)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > limit as u64 {
        return Err(ProcessError::InvalidConfiguration);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 || metadata.mode() & 0o7777 != 0o600 {
            return Err(ProcessError::InvalidConfiguration);
        }
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(ProcessError::ReadConfiguration)?;
    if bytes.len() > limit {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}
fn write_new_private(path: &Path, bytes: &[u8]) -> Result<(), ProcessError> {
    check_private_directory(path.parent().ok_or(ProcessError::InvalidConfiguration)?)?;
    if bytes.is_empty() || bytes.len() > INSTALLATION_MAX_BYTES {
        return Err(ProcessError::InvalidConfiguration);
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options
        .open(path)
        .map_err(ProcessError::ReadConfiguration)?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(ProcessError::ReadConfiguration)?;
    File::open(path.parent().ok_or(ProcessError::InvalidConfiguration)?)
        .and_then(|file| file.sync_all())
        .map_err(ProcessError::ReadConfiguration)
}
fn rename_private_exclusive(source: &Path, target: &Path) -> Result<(), ProcessError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        let source = std::ffi::CString::new(source.as_os_str().as_bytes())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        let target = std::ffi::CString::new(target.as_os_str().as_bytes())
            .map_err(|_| ProcessError::InvalidConfiguration)?;
        #[cfg(target_os = "macos")]
        let result = unsafe {
            libc::renameatx_np(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                target.as_ptr(),
                libc::RENAME_EXCL,
            )
        };
        #[cfg(target_os = "linux")]
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                target.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let result = -1;
        if result != 0 {
            return Err(ProcessError::InvalidConfiguration);
        }
    }
    #[cfg(not(unix))]
    return Err(ProcessError::InvalidConfiguration);
    File::open(target.parent().ok_or(ProcessError::InvalidConfiguration)?)
        .and_then(|file| file.sync_all())
        .map_err(ProcessError::ReadConfiguration)
}

fn build_profile(
    input: BootstrapInput,
    artifact_authority: DevelopmentArtifactAuthoritySeed,
) -> Result<BootstrapDevelopmentProfile, ProcessError> {
    let developer_permissions = match input.session_principal_kind {
        PrincipalKind::AgentAuthor => local_developer_permissions()?,
        PrincipalKind::TenantAdmin => local_administrator_permissions()?,
        _ => return Err(ProcessError::InvalidConfiguration),
    };
    let registry_validation_permissions = PermissionSet::new(vec![
        Permission::AgentWrite,
        Permission::SkillWrite,
        Permission::CapabilityWrite,
        Permission::ContextWrite,
        Permission::McpWrite,
        Permission::ModelWrite,
        Permission::SandboxWrite,
        Permission::PolicyWrite,
    ])
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let egress_permissions = PermissionSet::new(vec![Permission::SecretBind])
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    let tenant_id = input.tenant_id.clone();
    let developer_principal_id = input.developer_principal_id.clone();
    let registry_validator_principal_id = input.registry_validator_principal_id.clone();
    let egress_broker_principal_id = input.egress_broker.principal_id.clone();
    let service_principals = vec![
        NewPrincipal {
            principal_id: registry_validator_principal_id.clone(),
            authentication_authority_digest: input
                .registry_validator_authentication_authority_digest,
            subject_digest: input.registry_validator_subject_digest,
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: Vec::new(),
            },
        },
        NewPrincipal {
            principal_id: input.egress_broker.principal_id,
            authentication_authority_digest: input.egress_broker.authentication_authority_digest,
            subject_digest: input.egress_broker.subject_digest,
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: Vec::new(),
            },
        },
    ];
    let tenant_principal_bindings = vec![
        NewTenantPrincipal {
            tenant_id: tenant_id.clone(),
            principal_id: developer_principal_id.clone(),
            principal_kind: input.session_principal_kind,
            payload: TenantPrincipalPayload {
                permissions: developer_permissions,
            },
        },
        NewTenantPrincipal {
            tenant_id: tenant_id.clone(),
            principal_id: registry_validator_principal_id,
            principal_kind: PrincipalKind::ServiceIdentity,
            payload: TenantPrincipalPayload {
                permissions: registry_validation_permissions,
            },
        },
        NewTenantPrincipal {
            tenant_id: tenant_id.clone(),
            principal_id: egress_broker_principal_id,
            principal_kind: PrincipalKind::ServiceIdentity,
            payload: TenantPrincipalPayload {
                permissions: egress_permissions,
            },
        },
    ];
    Ok(BootstrapDevelopmentProfile {
        installation: BootstrapInstallationOperator {
            principal_id: input.installation_principal_id,
            request_id: input.installation_request_id,
            authentication_authority_digest: input.installation_authentication_authority_digest,
            subject_digest: input.installation_subject_digest,
            evidence_digest: input.installation_evidence_digest,
        },
        tenant: NewTenant {
            tenant_id: tenant_id.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        },
        developer: NewPrincipal {
            principal_id: developer_principal_id.clone(),
            authentication_authority_digest: input.developer_authentication_authority_digest,
            subject_digest: input.developer_subject_digest,
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: Vec::new(),
            },
        },
        service_principals,
        tenant_principal_bindings,
        artifact_authority: Some(artifact_authority),
    })
}

/// The local profile deliberately issues one short-lived developer token. Its exact binding must
/// therefore cover every public productization journey that token can drive; a second binding
/// under another principal kind is unusable because principal kind is part of the authenticated
/// identity. This is a development-only closure and intentionally excludes installation,
/// tenant-administration, emergency-stop, Secret inspection/rotation and Artifact maintenance.
fn local_developer_permissions() -> Result<PermissionSet, ProcessError> {
    PermissionSet::new(vec![
        Permission::AgentRead,
        Permission::AgentWrite,
        Permission::AgentPublish,
        Permission::AgentDeploy,
        Permission::AgentActivate,
        Permission::AgentRun,
        Permission::SkillRead,
        Permission::SkillWrite,
        Permission::SkillPublish,
        Permission::SkillBind,
        Permission::SkillActivate,
        Permission::CapabilityRead,
        Permission::CapabilityWrite,
        Permission::CapabilityPublish,
        Permission::CapabilityDeploy,
        Permission::CapabilityActivate,
        Permission::CapabilityBind,
        Permission::CapabilityInvoke,
        Permission::ContextRead,
        Permission::ContextWrite,
        Permission::ContextPublish,
        Permission::ContextDeploy,
        Permission::ContextActivate,
        Permission::ContextQuery,
        Permission::ContextBuildDataset,
        Permission::McpRead,
        Permission::McpWrite,
        Permission::McpDiscover,
        Permission::McpImport,
        Permission::McpPublish,
        Permission::McpDeploy,
        Permission::McpActivate,
        Permission::McpInvoke,
        Permission::ModelRead,
        Permission::ModelWrite,
        Permission::ModelDiscover,
        Permission::ModelImport,
        Permission::ModelPublish,
        Permission::ModelDeploy,
        Permission::ModelActivate,
        Permission::ModelInvoke,
        Permission::SandboxRead,
        Permission::SandboxWrite,
        Permission::SandboxBuild,
        Permission::SandboxPublish,
        Permission::SandboxActivate,
        Permission::SandboxExecute,
        Permission::ArtifactRead,
        Permission::ArtifactWrite,
        Permission::ApprovalRead,
        Permission::ApprovalRespond,
        Permission::InteractionRead,
        Permission::InteractionRespond,
        Permission::PolicyRead,
        Permission::PolicyWrite,
        Permission::PolicyPublish,
        Permission::PolicyActivate,
        Permission::OperationRead,
        Permission::OperationCancel,
        Permission::RuntimeRead,
        Permission::RuntimeControl,
        Permission::RuntimeSignal,
        Permission::SecretBind,
    ])
    .map_err(|_| ProcessError::InvalidConfiguration)
}

fn required(name: &'static str) -> Result<String, ProcessError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(ProcessError::MissingEnvironment(name))
}

fn required_absolute_path(name: &'static str) -> Result<std::path::PathBuf, ProcessError> {
    let path = std::path::PathBuf::from(required(name)?);
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(ProcessError::InvalidConfiguration)
    }
}

fn read_bounded_file(path: &Path, maximum_bytes: usize) -> Result<Vec<u8>, ProcessError> {
    let metadata = std::fs::metadata(path).map_err(ProcessError::ReadConfiguration)?;
    if !metadata.is_file() || metadata.len() > u64::try_from(maximum_bytes).unwrap_or(u64::MAX) {
        return Err(ProcessError::InvalidConfiguration);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(maximum_bytes));
    File::open(path)
        .and_then(|file| file.take(maximum_bytes as u64 + 1).read_to_end(&mut bytes))
        .map_err(ProcessError::ReadConfiguration)?;
    if bytes.len() > maximum_bytes {
        return Err(ProcessError::InvalidConfiguration);
    }
    Ok(bytes)
}

fn fail(error: ProcessError) -> ! {
    eprintln!("platform-dev-bootstrap failed: {error}");
    std::process::exit(match error {
        ProcessError::Usage
        | ProcessError::MissingEnvironment(_)
        | ProcessError::InvalidConfiguration => 2,
        ProcessError::ReadConfiguration(_)
        | ProcessError::Database(_)
        | ProcessError::Schema(_)
        | ProcessError::Repository(_) => 1,
    });
}

fn load_installation_administrator() -> Result<BootstrapInput, ProcessError> {
    use insight_platform_deployment_contracts::installation::InstallationAdministratorBootstrapV1;
    let path = required_absolute_path("PLATFORM_INSTALLATION_BOOTSTRAP_CONFIG")?;
    let bytes = read_private_file(&path, MAX_CONFIG_BYTES)?;
    let value = parse_strict_json(
        &bytes,
        insight_platform_deployment_contracts::installation::INSTALLATION_LIMITS,
    )
    .map_err(|_| ProcessError::InvalidConfiguration)?;
    let expected: Sha256Digest = required("PLATFORM_INSTALLATION_BOOTSTRAP_CONFIG_DIGEST")?
        .parse()
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    if canonical_digest(&value).map_err(|_| ProcessError::InvalidConfiguration)?
        != expected.as_str()
    {
        return Err(ProcessError::InvalidConfiguration);
    }
    let config = InstallationAdministratorBootstrapV1::decode(&bytes)
        .map_err(|_| ProcessError::InvalidConfiguration)?;
    Ok(BootstrapInput {
        session_principal_kind: PrincipalKind::TenantAdmin,
        installation_principal_id: config.installation.principal_id,
        installation_request_id: config.installation_request_id,
        installation_authentication_authority_digest: config
            .installation
            .authentication_authority_digest,
        installation_subject_digest: config.installation.subject_digest,
        installation_evidence_digest: config.installation_evidence_digest,
        tenant_id: config.tenant_id,
        developer_principal_id: config.administrator.principal_id,
        developer_authentication_authority_digest: config
            .administrator
            .authentication_authority_digest,
        developer_subject_digest: config.administrator.subject_digest,
        registry_validator_principal_id: config.registry_validator.principal_id,
        registry_validator_authentication_authority_digest: config
            .registry_validator
            .authentication_authority_digest,
        registry_validator_subject_digest: config.registry_validator.subject_digest,
        egress_broker: BootstrapServiceIdentity {
            principal_id: config.egress_broker.principal_id,
            authentication_authority_digest: config.egress_broker.authentication_authority_digest,
            subject_digest: config.egress_broker.subject_digest,
        },
    })
}
fn local_administrator_permissions() -> Result<PermissionSet, ProcessError> {
    let mut permissions = local_developer_permissions()?.iter().collect::<Vec<_>>();
    permissions.extend([
        Permission::TenantManage,
        Permission::SecretInspect,
        Permission::SecretRotate,
        Permission::SecretRevoke,
    ]);
    PermissionSet::new(permissions).map_err(|_| ProcessError::InvalidConfiguration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn installation_commands_are_closed_and_readonly_modes_are_explicit() {
        let parse = |args: &[&str]| {
            Command::parse(&args.iter().map(|arg| (*arg).into()).collect::<Vec<_>>()).unwrap()
        };
        assert_eq!(parse(&[]), Command::Native);
        assert_eq!(
            parse(&["--installation-administrator"]),
            Command::Administrator
        );
        assert_eq!(
            parse(&["--installation-administrator", "--verify"]),
            Command::VerifyAdministrator
        );
        assert_eq!(
            parse(&["--installation-model-inputs"]),
            Command::ModelInputs
        );
        assert_eq!(
            parse(&["--installation-model-bootstrap"]),
            Command::BootstrapModel
        );
        assert_eq!(
            parse(&["--installation-model-verify"]),
            Command::VerifyModel
        );
        for args in [
            vec!["--verify"],
            vec!["--installation-model-bootstrap", "--verify"],
            vec!["--installation-model-inputs", "/arbitrary/output"],
            vec!["--installation-administrator", "--repair"],
        ] {
            assert!(matches!(
                Command::parse(&args.into_iter().map(str::to_owned).collect::<Vec<_>>()),
                Err(ProcessError::Usage)
            ));
        }
    }
    #[test]
    fn process_errors_do_not_disclose_source_urls_or_private_contents() {
        let canary = "postgres://private-credential@fixture";
        for error in [
            ProcessError::ReadConfiguration(std::io::Error::other(canary)),
            ProcessError::Database(sqlx::Error::Protocol(canary.into())),
        ] {
            assert!(!error.to_string().contains(canary));
            assert!(!error.to_string().contains("private-credential"));
        }
    }
    #[cfg(unix)]
    #[test]
    fn installation_files_are_bounded_private_and_atomic_publication_never_overwrites() {
        use std::os::unix::fs::PermissionsExt as _;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .join(format!(
                "insight-model-bootstrap-test-{}-{nonce}",
                std::process::id()
            ));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(root.clone());
        let target = root.join(MODEL_INPUTS_FILE);
        let pending = root.join(MODEL_INPUTS_TEMP);
        write_new_private(&pending, b"fixture value").unwrap();
        assert!(write_new_private(&pending, b"overwrite").is_err());
        assert!(read_private_file(&pending, 4).is_err());
        assert_eq!(read_private_file(&pending, 32).unwrap(), b"fixture value");
        rename_private_exclusive(&pending, &target).unwrap();
        assert!(!pending.exists());
        write_new_private(&pending, b"different tuple").unwrap();
        assert!(rename_private_exclusive(&pending, &target).is_err());
        assert_eq!(read_private_file(&target, 32).unwrap(), b"fixture value");
        assert!(pending.exists());
        let hardlink = root.join("hardlink.json");
        std::fs::hard_link(&target, &hardlink).unwrap();
        assert!(read_private_file(&target, 32).is_err());
        std::fs::remove_file(hardlink).unwrap();
        let link = root.join("link.json");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(read_private_file(&link, 32).is_err());
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_private_file(&target, 32).is_err());
    }

    fn digest(character: char) -> String {
        format!("sha256:{}", character.to_string().repeat(64))
    }

    fn config() -> Config {
        serde_json::from_value(json!({
            "schema_version": 2,
            "environment_class": "development",
            "installation": {
                "principal_id": "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b90",
                "request_id": "req_0198f1c3-8f49-7c3e-b1f3-773c28367b91",
                "authentication_authority_digest": digest('a'),
                "subject_digest": digest('b'),
                "evidence_digest": digest('c')
            },
            "developer": {
                "tenant_id": "ten_0198f1c3-8f49-7c3e-b1f3-773c28367b92",
                "principal_id": "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b94",
                "authentication_authority_digest": digest('d'),
                "subject_digest": digest('e')
            },
            "registry_validator": {
                "principal_id": "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b95",
                "authentication_authority_digest": digest('f'),
                "subject_digest": digest('1')
            },
            "egress_broker": {
                "principal_id": "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b96",
                "authentication_authority_digest": digest('2'),
                "subject_digest": digest('3')
            }
        }))
        .unwrap()
    }

    #[test]
    fn development_config_accepts_closed_input() {
        let input = config().validate().unwrap();
        assert_eq!(input.tenant_id.kind(), ResourceKind::Tenant);
        assert_eq!(input.developer_principal_id.kind(), ResourceKind::Principal);
    }

    #[test]
    fn current_config_requires_a_distinct_egress_service_identity() {
        let input = config().validate().unwrap();
        assert_eq!(
            input.egress_broker.principal_id.kind(),
            ResourceKind::Principal
        );

        let mut reused = config();
        reused.egress_broker.principal_id = reused.registry_validator.principal_id.clone();
        assert!(matches!(
            reused.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));

        let mut legacy = config();
        legacy.schema_version = 1;
        assert!(matches!(
            legacy.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }

    #[test]
    fn production_environment_is_rejected() {
        let mut config = config();
        config.environment_class = "production".to_owned();
        assert!(matches!(
            config.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }

    #[test]
    fn developer_cannot_reuse_installation_principal() {
        let mut config = config();
        config.developer.principal_id = config.installation.principal_id.clone();
        assert!(matches!(
            config.validate(),
            Err(ProcessError::InvalidConfiguration)
        ));
    }

    #[test]
    fn installation_administrator_is_explicit_and_never_an_installation_operator() {
        assert_eq!(
            config().validate().unwrap().session_principal_kind,
            PrincipalKind::AgentAuthor
        );
        let native = local_developer_permissions().unwrap();
        let administrator = local_administrator_permissions().unwrap();
        for permission in native.iter() {
            assert!(administrator.contains(permission));
        }
        for permission in [
            Permission::TenantManage,
            Permission::SecretInspect,
            Permission::SecretRotate,
            Permission::SecretRevoke,
        ] {
            assert!(administrator.contains(permission));
            assert!(!native.contains(permission));
        }
        for permission in [
            Permission::InstallationManage,
            Permission::TenantEmergencyStop,
        ] {
            assert!(!administrator.contains(permission));
        }
    }

    #[test]
    fn local_developer_permission_closure_covers_public_product_journeys_only() {
        let permissions = local_developer_permissions().unwrap();
        for required in [
            Permission::AgentWrite,
            Permission::AgentRun,
            Permission::ArtifactRead,
            Permission::ArtifactWrite,
            Permission::PolicyPublish,
            Permission::OperationRead,
            Permission::RuntimeRead,
            Permission::RuntimeControl,
            Permission::RuntimeSignal,
            Permission::ApprovalRespond,
            Permission::InteractionRespond,
        ] {
            assert!(permissions.contains(required), "missing {required}");
        }
        for forbidden in [
            Permission::InstallationManage,
            Permission::TenantManage,
            Permission::TenantEmergencyStop,
            Permission::SecretInspect,
            Permission::SecretRotate,
            Permission::SecretRevoke,
            Permission::ArtifactHold,
            Permission::ArtifactRescan,
        ] {
            assert!(!permissions.contains(forbidden), "unexpected {forbidden}");
        }
    }
}
