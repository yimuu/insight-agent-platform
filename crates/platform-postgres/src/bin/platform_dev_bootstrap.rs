//! One-shot development-profile bootstrap for a fresh Platform PostgreSQL authority.
//!
//! This binary is intentionally separate from the public Gateway. It accepts only a canonical,
//! digest-pinned development config and creates the initial tenant/principal rows through the
//! repository authority. Runtime services retain zero DDL privileges and the `insight` CLI never
//! links a PostgreSQL client.

use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ArtifactRetentionPolicy, JsonLimits, Permission,
    PermissionSet, PrincipalBindingsPayload, PrincipalKind, ResourceId, ResourceKind,
    SandboxArtifactIoPolicyDocument, SchedulingPolicyDocument, Sha256Digest, TenantConfig,
    TenantPrincipalPayload,
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
use std::{error::Error, fmt, fs::File, io::Read as _, path::Path};

const CONFIG_PATH_ENV: &str = "PLATFORM_DEV_BOOTSTRAP_CONFIG";
const CONFIG_DIGEST_ENV: &str = "PLATFORM_DEV_BOOTSTRAP_CONFIG_DIGEST";
const ARTIFACT_CONFIG_PATH_ENV: &str = "PLATFORM_DEV_ARTIFACT_BOOTSTRAP_CONFIG";
const ARTIFACT_CONFIG_DIGEST_ENV: &str = "PLATFORM_DEV_ARTIFACT_BOOTSTRAP_CONFIG_DIGEST";
const DATABASE_URL_ENV: &str = "PLATFORM_DATABASE_URL";
const MAX_CONFIG_BYTES: usize = 65_536;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema_version: u32,
    environment_class: String,
    installation: InstallationConfig,
    developer: DeveloperConfig,
    registry_validator: RegistryValidatorConfig,
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
struct RegistryValidatorConfig {
    principal_id: String,
    authentication_authority_digest: String,
    subject_digest: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactAuthorityConfig {
    schema_version: u32,
    environment_class: String,
    authoring_artifact_id: String,
    authoring_blob_id: String,
    retention_policy_id: String,
    retention_policy_revision_id: String,
    retention_policy_deployment_id: String,
    artifact_io_policy_id: String,
    artifact_io_policy_revision_id: String,
    artifact_io_policy_deployment_id: String,
    scheduling_policy_id: String,
    scheduling_policy_revision_id: String,
    scheduling_policy_deployment_id: String,
    staging_quota_account_id: String,
    orchestration_quota_account_id: String,
    retention_policy: ArtifactRetentionPolicy,
    artifact_io_policy: SandboxArtifactIoPolicyDocument,
    scheduling_policy: SchedulingPolicyDocument,
    staging_quota_bytes: i64,
    orchestration_concurrent_jobs: i64,
}

struct BootstrapInput {
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
        if self.schema_version != 1 || self.environment_class != "development" {
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
        if installation_principal_id == developer_principal_id
            || installation_principal_id == registry_validator_principal_id
            || developer_principal_id == registry_validator_principal_id
        {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(BootstrapInput {
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
        })
    }
}

impl ArtifactAuthorityConfig {
    fn load() -> Result<Self, ProcessError> {
        let path = required_absolute_path(ARTIFACT_CONFIG_PATH_ENV)?;
        let bytes = read_bounded_file(&path, MAX_CONFIG_BYTES)?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: MAX_CONFIG_BYTES,
                max_depth: 10,
                max_properties_per_object: 32,
                max_items_per_array: 64,
                max_string_bytes: 512,
            },
        )
        .map_err(|_| ProcessError::InvalidConfiguration)?;
        let expected: Sha256Digest = required(ARTIFACT_CONFIG_DIGEST_ENV)?
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
        if config.schema_version != 1 || config.environment_class != "development" {
            return Err(ProcessError::InvalidConfiguration);
        }
        Ok(config)
    }

    fn into_seed(self) -> Result<DevelopmentArtifactAuthoritySeed, ProcessError> {
        Ok(DevelopmentArtifactAuthoritySeed {
            authoring_artifact_id: parse_id(&self.authoring_artifact_id, ResourceKind::Artifact)?,
            authoring_blob_id: parse_id(&self.authoring_blob_id, ResourceKind::InternalBlob)?,
            retention_policy_id: parse_id(&self.retention_policy_id, ResourceKind::Policy)?,
            retention_policy_revision_id: parse_id(
                &self.retention_policy_revision_id,
                ResourceKind::PolicyRevision,
            )?,
            retention_policy_deployment_id: parse_id(
                &self.retention_policy_deployment_id,
                ResourceKind::PolicyDeployment,
            )?,
            artifact_io_policy_id: parse_id(&self.artifact_io_policy_id, ResourceKind::Policy)?,
            artifact_io_policy_revision_id: parse_id(
                &self.artifact_io_policy_revision_id,
                ResourceKind::PolicyRevision,
            )?,
            artifact_io_policy_deployment_id: parse_id(
                &self.artifact_io_policy_deployment_id,
                ResourceKind::PolicyDeployment,
            )?,
            scheduling_policy_id: parse_id(&self.scheduling_policy_id, ResourceKind::Policy)?,
            scheduling_policy_revision_id: parse_id(
                &self.scheduling_policy_revision_id,
                ResourceKind::PolicyRevision,
            )?,
            scheduling_policy_deployment_id: parse_id(
                &self.scheduling_policy_deployment_id,
                ResourceKind::PolicyDeployment,
            )?,
            staging_quota_account_id: parse_id(
                &self.staging_quota_account_id,
                ResourceKind::QuotaAccount,
            )?,
            orchestration_quota_account_id: parse_id(
                &self.orchestration_quota_account_id,
                ResourceKind::QuotaAccount,
            )?,
            retention_policy: self.retention_policy,
            artifact_io_policy: self.artifact_io_policy,
            scheduling_policy: self.scheduling_policy,
            staging_quota_bytes: self.staging_quota_bytes,
            orchestration_concurrent_jobs: self.orchestration_concurrent_jobs,
        })
    }
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
            Self::ReadConfiguration(error) => write!(
                formatter,
                "cannot read development bootstrap configuration: {error}"
            ),
            Self::Database(error) => {
                write!(formatter, "cannot connect to PostgreSQL authority: {error}")
            }
            Self::Schema(error) => write!(formatter, "PostgreSQL schema is not verified: {error}"),
            Self::Repository(error) => write!(formatter, "development bootstrap rejected: {error}"),
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
    if std::env::args().len() != 1 {
        fail(ProcessError::Usage);
    }
    if let Err(error) = run().await {
        fail(error);
    }
}

async fn run() -> Result<(), ProcessError> {
    let input = Config::load()?.validate()?;
    let artifact_authority = ArtifactAuthorityConfig::load()?.into_seed()?;
    let database_url = required(DATABASE_URL_ENV)?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .map_err(ProcessError::Database)?;
    verify_schema(&pool).await.map_err(ProcessError::Schema)?;
    let repository = PgRepository::new(pool);
    let developer_permissions = local_developer_permissions()?;
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
    let tenant_id = input.tenant_id.clone();
    let developer_principal_id = input.developer_principal_id.clone();
    let registry_validator_principal_id = input.registry_validator_principal_id.clone();
    let outcome = repository
        .bootstrap_development_profile(BootstrapDevelopmentProfile {
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
            registry_validator: NewPrincipal {
                principal_id: registry_validator_principal_id.clone(),
                authentication_authority_digest: input
                    .registry_validator_authentication_authority_digest,
                subject_digest: input.registry_validator_subject_digest,
                installation_bindings: PrincipalBindingsPayload {
                    installation_bindings: Vec::new(),
                },
            },
            tenant_principal_bindings: vec![
                NewTenantPrincipal {
                    tenant_id: tenant_id.clone(),
                    principal_id: developer_principal_id,
                    principal_kind: PrincipalKind::AgentAuthor,
                    payload: TenantPrincipalPayload {
                        permissions: developer_permissions,
                    },
                },
                NewTenantPrincipal {
                    tenant_id,
                    principal_id: registry_validator_principal_id,
                    principal_kind: PrincipalKind::ServiceIdentity,
                    payload: TenantPrincipalPayload {
                        permissions: registry_validation_permissions,
                    },
                },
            ],
            artifact_authority: Some(artifact_authority),
        })
        .await
        .map_err(ProcessError::Repository)?;
    println!(
        "development tenant and developer principal {}",
        match outcome {
            BootstrapOutcome::Created => "created",
            BootstrapOutcome::Replayed => "verified",
        }
    );
    Ok(())
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
        .and_then(|mut file| file.read_to_end(&mut bytes))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn digest(character: char) -> String {
        format!("sha256:{}", character.to_string().repeat(64))
    }

    fn config() -> Config {
        serde_json::from_value(json!({
            "schema_version": 1,
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
