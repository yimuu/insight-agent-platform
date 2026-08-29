//! Product-facing command-line entry points for the Platform `/v1` local development profile.
//!
//! This crate intentionally has no database, worker, or internal RPC dependency. It may inspect
//! host prerequisites and create project-local development state, but all future business
//! mutations must use the public Gateway `/v1` contract.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use insight_platform_contracts::{canonical_digest, ResourceId, ResourceKind};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, PublicKeyData, SanType, PKCS_RSA_SHA256,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, OpenOptions},
    io::Write,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;
use x509_parser::{prelude::FromDer as _, public_key::PublicKey, x509::SubjectPublicKeyInfo};

const PROJECT_DIRECTORY: &str = ".insight";
const PROJECT_STATE_FILE: &str = "project.json";
const PROJECT_GITIGNORE_FILE: &str = ".gitignore";
const PROJECT_KIND: &str = "insight.dev.project/v1";
const IDENTITY_DIRECTORY: &str = "identity";
const IDENTITY_PRIVATE_KEY_FILE: &str = "local-issuer-private-key.pem";
const IDENTITY_JWKS_FILE: &str = "local-issuer-jwks.json";
const IDENTITY_BOOTSTRAP_CONFIG_FILE: &str = "development-bootstrap.json";
const IDENTITY_ACCESS_TOKEN_FILE: &str = "developer-access-token.jwt";
const RUNTIME_DIRECTORY: &str = "runtime";
const RUNTIME_TLS_DIRECTORY: &str = "tls";
const RUNTIME_CA_CERTIFICATE_FILE: &str = "ca.pem";
const RUNTIME_CA_PRIVATE_KEY_FILE: &str = "ca-key.pem";
const RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE: &str = "artifact-gateway.pem";
const RUNTIME_ARTIFACT_GATEWAY_PRIVATE_KEY_FILE: &str = "artifact-gateway-key.pem";
const RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE: &str = "artifact-data.pem";
const RUNTIME_ARTIFACT_DATA_PRIVATE_KEY_FILE: &str = "artifact-data-key.pem";
const RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE: &str = "gateway-client.pem";
const RUNTIME_GATEWAY_CLIENT_PRIVATE_KEY_FILE: &str = "gateway-client-key.pem";
const RUNTIME_ORCHESTRATION_CLIENT_CERTIFICATE_FILE: &str = "orchestration-client.pem";
const RUNTIME_ORCHESTRATION_CLIENT_PRIVATE_KEY_FILE: &str = "orchestration-client-key.pem";
const PUBLIC_GATEWAY_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/public-gateway";
const SCHEDULER_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/scheduler";
const LOCAL_OIDC_AUDIENCE: &str = "insight.platform/v1";
const LOCAL_ACCESS_TOKEN_TTL_SECONDS: i64 = 900;
const EXPECTED_RUSTC_PREFIX: &str = "rustc 1.94.1";
const DEFAULT_PORTS: &[u16] = &[5432, 4222, 8080];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliCommand {
    Doctor {
        json: bool,
    },
    Init {
        root: PathBuf,
        project_name: Option<String>,
    },
    Token {
        root: PathBuf,
    },
    Help,
}

#[derive(Debug)]
pub enum CliError {
    Usage,
    UnknownCommand(String),
    MissingValue(&'static str),
    DuplicateOption(&'static str),
    UnsupportedOption(String),
    InvalidProjectName(String),
    MissingProjectName(String),
    ProjectAlreadyInitialized(String),
    InitializeProject {
        path: String,
        source: std::io::Error,
    },
    ReadLocalIdentity {
        path: String,
        source: std::io::Error,
    },
    InvalidLocalIdentity {
        path: String,
    },
    RotateLocalAccessToken {
        path: String,
        source: std::io::Error,
    },
    InvalidClock,
    DoctorFailed {
        report: String,
    },
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage => write!(
                formatter,
                "usage: insight <doctor [--json] | init [--path <directory>] [--name <name>]>"
            ),
            Self::UnknownCommand(command) => write!(formatter, "unknown command {command:?}"),
            Self::MissingValue(option) => write!(formatter, "missing value for {option}"),
            Self::DuplicateOption(option) => write!(formatter, "duplicate option {option}"),
            Self::UnsupportedOption(option) => write!(formatter, "unsupported option {option:?}"),
            Self::InvalidProjectName(name) => write!(
                formatter,
                "project name {name:?} must contain only ASCII letters, digits, '.', '_' or '-'"
            ),
            Self::MissingProjectName(path) => {
                write!(formatter, "cannot derive a project name from {path}")
            }
            Self::ProjectAlreadyInitialized(path) => {
                write!(formatter, "local project state already exists at {path}")
            }
            Self::InitializeProject { path, source } => {
                write!(
                    formatter,
                    "cannot initialize local project state at {path}: {source}"
                )
            }
            Self::ReadLocalIdentity { path, source } => {
                write!(
                    formatter,
                    "cannot read local identity state at {path}: {source}"
                )
            }
            Self::InvalidLocalIdentity { path } => {
                write!(formatter, "local identity state at {path} is invalid")
            }
            Self::RotateLocalAccessToken { path, source } => {
                write!(
                    formatter,
                    "cannot rotate local access token at {path}: {source}"
                )
            }
            Self::InvalidClock => write!(formatter, "local project clock is before the Unix epoch"),
            Self::DoctorFailed { .. } => {
                write!(
                    formatter,
                    "required development prerequisites are unavailable"
                )
            }
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InitializeProject { source, .. } => Some(source),
            Self::ReadLocalIdentity { source, .. }
            | Self::RotateLocalAccessToken { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl CliError {
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Usage
            | Self::UnknownCommand(_)
            | Self::MissingValue(_)
            | Self::DuplicateOption(_)
            | Self::UnsupportedOption(_)
            | Self::InvalidProjectName(_)
            | Self::MissingProjectName(_) => 2,
            Self::DoctorFailed { .. }
            | Self::InitializeProject { .. }
            | Self::ReadLocalIdentity { .. }
            | Self::InvalidLocalIdentity { .. }
            | Self::RotateLocalAccessToken { .. }
            | Self::InvalidClock => 1,
            Self::ProjectAlreadyInitialized(_) => 1,
        }
    }

    pub fn output(&self) -> Option<&str> {
        match self {
            Self::DoctorFailed { report } => Some(report),
            _ => None,
        }
    }
}

pub fn parse_command(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let Some(command) = arguments.first().and_then(|value| value.to_str()) else {
        return Err(CliError::Usage);
    };
    match command {
        "help" | "--help" | "-h" => Ok(CliCommand::Help),
        "doctor" => parse_doctor(&arguments[1..]),
        "init" => parse_init(&arguments[1..]),
        "token" => parse_token(&arguments[1..]),
        value => Err(CliError::UnknownCommand(value.to_owned())),
    }
}

fn parse_doctor(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    match arguments {
        [] => Ok(CliCommand::Doctor { json: false }),
        [flag] if flag == "--json" => Ok(CliCommand::Doctor { json: true }),
        [flag] => Err(CliError::UnsupportedOption(lossy(flag))),
        _ => Err(CliError::Usage),
    }
}

fn parse_init(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let mut root = None;
    let mut project_name = None;
    let mut cursor = 0;
    while cursor < arguments.len() {
        let flag = arguments[cursor].to_string_lossy();
        match flag.as_ref() {
            "--path" => {
                if root.is_some() {
                    return Err(CliError::DuplicateOption("--path"));
                }
                let Some(value) = arguments.get(cursor + 1) else {
                    return Err(CliError::MissingValue("--path"));
                };
                root = Some(PathBuf::from(value));
                cursor += 2;
            }
            "--name" => {
                if project_name.is_some() {
                    return Err(CliError::DuplicateOption("--name"));
                }
                let Some(value) = arguments.get(cursor + 1).and_then(|value| value.to_str()) else {
                    return Err(CliError::MissingValue("--name"));
                };
                project_name = Some(value.to_owned());
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    Ok(CliCommand::Init {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        project_name,
    })
}

fn parse_token(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let mut root = None;
    let mut cursor = 0;
    while cursor < arguments.len() {
        let flag = arguments[cursor].to_string_lossy();
        match flag.as_ref() {
            "--path" => {
                if root.is_some() {
                    return Err(CliError::DuplicateOption("--path"));
                }
                let Some(value) = arguments.get(cursor + 1) else {
                    return Err(CliError::MissingValue("--path"));
                };
                root = Some(PathBuf::from(value));
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    Ok(CliCommand::Token {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
    })
}

fn lossy(value: &OsString) -> String {
    value.to_string_lossy().into_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalProjectState {
    pub schema_version: u32,
    pub kind: String,
    pub project_name: String,
    pub created_at_unix_seconds: u64,
    pub identity: LocalIdentityState,
    pub profiles: BTreeMap<String, LocalProfileState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalIdentityState {
    pub schema_version: u32,
    pub issuer: String,
    pub audience: String,
    pub key_id: String,
    pub jwks_digest: String,
    pub authentication_authority_digest: String,
    pub tenant_id: String,
    pub developer_principal_id: String,
    pub developer_subject: String,
    pub installation_principal_id: String,
    pub installation_request_id: String,
    pub bootstrap_config_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalProfileState {
    pub state: String,
}

#[derive(Debug, Serialize)]
struct LocalAccessTokenClaims<'a> {
    iss: &'a str,
    aud: &'a str,
    sub: &'a str,
    jti: String,
    iat: i64,
    exp: i64,
    tenant_id: &'a str,
    principal_kind: &'static str,
    authn_strength: &'static str,
}

pub fn initialize_project(
    root: &Path,
    project_name: Option<&str>,
    created_at: SystemTime,
) -> Result<LocalProjectState, CliError> {
    let state_directory = root.join(PROJECT_DIRECTORY);
    if state_directory.exists() {
        return Err(CliError::ProjectAlreadyInitialized(
            state_directory.display().to_string(),
        ));
    }
    let name = project_name
        .map(str::to_owned)
        .or_else(|| derive_project_name(root).ok())
        .ok_or_else(|| CliError::MissingProjectName(root.display().to_string()))?;
    validate_project_name(&name)?;
    let created_at_unix_seconds = created_at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CliError::InvalidClock)?
        .as_secs();
    fs::create_dir(&state_directory).map_err(|source| CliError::InitializeProject {
        path: state_directory.display().to_string(),
        source,
    })?;
    let identity = match initialize_local_identity(&state_directory, created_at_unix_seconds) {
        Ok(identity) => identity,
        Err(error) => {
            let _ = fs::remove_dir_all(&state_directory);
            return Err(error);
        }
    };
    if let Err(error) = initialize_local_runtime_identity(&state_directory) {
        let _ = fs::remove_dir_all(&state_directory);
        return Err(error);
    }
    let state = LocalProjectState {
        schema_version: 1,
        kind: PROJECT_KIND.to_owned(),
        project_name: name,
        created_at_unix_seconds,
        identity,
        profiles: BTreeMap::from([
            (
                "base".to_owned(),
                LocalProfileState {
                    state: "not_provisioned".to_owned(),
                },
            ),
            (
                "full".to_owned(),
                LocalProfileState {
                    state: "not_provisioned".to_owned(),
                },
            ),
        ]),
    };
    let result = write_project_state(&state_directory, &state);
    if let Err(error) = result {
        let _ = fs::remove_dir_all(&state_directory);
        return Err(error);
    }
    Ok(state)
}

fn derive_project_name(root: &Path) -> Result<String, CliError> {
    root.file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| CliError::MissingProjectName(root.display().to_string()))
}

fn validate_project_name(project_name: &str) -> Result<(), CliError> {
    if project_name.is_empty()
        || !project_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(CliError::InvalidProjectName(project_name.to_owned()));
    }
    Ok(())
}

fn write_project_state(state_directory: &Path, state: &LocalProjectState) -> Result<(), CliError> {
    let gitignore_path = state_directory.join(PROJECT_GITIGNORE_FILE);
    write_new(
        &gitignore_path,
        b"# Generated local development state.\n*\n!.gitignore\n",
    )?;
    let state_path = state_directory.join(PROJECT_STATE_FILE);
    let encoded =
        serde_json::to_vec_pretty(state).map_err(|error| CliError::InitializeProject {
            path: state_path.display().to_string(),
            source: std::io::Error::other(error),
        })?;
    write_new(&state_path, &encoded)
}

fn initialize_local_identity(
    state_directory: &Path,
    issued_at_unix_seconds: u64,
) -> Result<LocalIdentityState, CliError> {
    let identity_directory = state_directory.join(IDENTITY_DIRECTORY);
    fs::create_dir(&identity_directory).map_err(|source| CliError::InitializeProject {
        path: identity_directory.display().to_string(),
        source,
    })?;
    let issuer_nonce = Uuid::now_v7();
    let issuer = format!("https://local.insight.platform/{issuer_nonce}");
    let key_id = format!("local-oidc-{issuer_nonce}");
    let tenant_id = fresh_resource_id(ResourceKind::Tenant).to_string();
    let developer_principal_id = fresh_resource_id(ResourceKind::Principal).to_string();
    let installation_principal_id = fresh_resource_id(ResourceKind::Principal).to_string();
    let installation_request_id = fresh_resource_id(ResourceKind::ServerRequest).to_string();
    let developer_subject = format!("developer:{issuer_nonce}");
    let installation_subject = format!("bootstrap:{issuer_nonce}");
    let authentication_authority_digest = tagged_digest(
        "oidc_authentication_authority_v1",
        &issuer,
        &identity_directory,
    )?;
    let developer_subject_digest =
        tagged_digest("oidc_subject_v1", &developer_subject, &identity_directory)?;
    let installation_subject_digest = tagged_digest(
        "oidc_subject_v1",
        &installation_subject,
        &identity_directory,
    )?;
    let installation_evidence_digest = tagged_digest(
        "local_development_bootstrap_evidence_v1",
        &issuer,
        &identity_directory,
    )?;

    let key_pair =
        KeyPair::generate_for(&PKCS_RSA_SHA256).map_err(|_| CliError::InitializeProject {
            path: identity_directory.display().to_string(),
            source: std::io::Error::other("cannot generate local RS256 issuer key"),
        })?;
    let jwks = build_local_jwks(&key_pair, &key_id, &identity_directory)?;
    let jwks_digest = canonical_digest(&jwks).map_err(|_| {
        invalid_local_identity(&identity_directory, "cannot canonicalize local JWKS")
    })?;
    let bootstrap_config = serde_json::json!({
        "schema_version": 1,
        "environment_class": "development",
        "installation": {
            "principal_id": installation_principal_id,
            "request_id": installation_request_id,
            "authentication_authority_digest": authentication_authority_digest,
            "subject_digest": installation_subject_digest,
            "evidence_digest": installation_evidence_digest,
        },
        "developer": {
            "tenant_id": tenant_id,
            "principal_id": developer_principal_id,
            "authentication_authority_digest": authentication_authority_digest,
            "subject_digest": developer_subject_digest,
        },
    });
    let bootstrap_config_digest = canonical_digest(&bootstrap_config).map_err(|_| {
        invalid_local_identity(
            &identity_directory,
            "cannot canonicalize development bootstrap config",
        )
    })?;
    let identity = LocalIdentityState {
        schema_version: 1,
        issuer,
        audience: LOCAL_OIDC_AUDIENCE.to_owned(),
        key_id,
        jwks_digest,
        authentication_authority_digest,
        tenant_id,
        developer_principal_id,
        developer_subject,
        installation_principal_id,
        installation_request_id,
        bootstrap_config_digest,
    };
    write_sensitive_new(
        &identity_directory.join(IDENTITY_PRIVATE_KEY_FILE),
        key_pair.serialize_pem().as_bytes(),
    )?;
    let jwks_bytes =
        serde_json::to_vec_pretty(&jwks).map_err(|error| CliError::InitializeProject {
            path: identity_directory
                .join(IDENTITY_JWKS_FILE)
                .display()
                .to_string(),
            source: std::io::Error::other(error),
        })?;
    write_new(&identity_directory.join(IDENTITY_JWKS_FILE), &jwks_bytes)?;
    let bootstrap_bytes = serde_json::to_vec_pretty(&bootstrap_config).map_err(|error| {
        CliError::InitializeProject {
            path: identity_directory
                .join(IDENTITY_BOOTSTRAP_CONFIG_FILE)
                .display()
                .to_string(),
            source: std::io::Error::other(error),
        }
    })?;
    write_new(
        &identity_directory.join(IDENTITY_BOOTSTRAP_CONFIG_FILE),
        &bootstrap_bytes,
    )?;
    let private_key_der = key_pair.serialize_der();
    let private_key_der = pkcs1_private_key_from_pkcs8(&private_key_der).ok_or_else(|| {
        invalid_local_identity(
            &identity_directory,
            "cannot convert local issuer key to RSA private-key form",
        )
    })?;
    issue_initial_local_access_token(
        &identity_directory,
        &identity,
        private_key_der,
        issued_at_unix_seconds,
    )?;
    Ok(identity)
}

fn initialize_local_runtime_identity(state_directory: &Path) -> Result<(), CliError> {
    let tls_directory = state_directory
        .join(RUNTIME_DIRECTORY)
        .join(RUNTIME_TLS_DIRECTORY);
    fs::create_dir_all(&tls_directory).map_err(|source| CliError::InitializeProject {
        path: tls_directory.display().to_string(),
        source,
    })?;

    let mut ca_params = CertificateParams::new(Vec::<String>::new()).map_err(|_| {
        invalid_local_identity(
            &tls_directory,
            "cannot construct local development certificate CA",
        )
    })?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().map_err(|_| {
        invalid_local_identity(
            &tls_directory,
            "cannot generate local development certificate CA",
        )
    })?;
    let ca_certificate = ca_params.self_signed(&ca_key).map_err(|_| {
        invalid_local_identity(
            &tls_directory,
            "cannot sign local development certificate CA",
        )
    })?;
    write_new(
        &tls_directory.join(RUNTIME_CA_CERTIFICATE_FILE),
        ca_certificate.pem().as_bytes(),
    )?;
    write_sensitive_new(
        &tls_directory.join(RUNTIME_CA_PRIVATE_KEY_FILE),
        ca_key.serialize_pem().as_bytes(),
    )?;
    let issuer = Issuer::new(ca_params, ca_key);

    write_local_leaf_certificate(
        &tls_directory,
        RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE,
        RUNTIME_ARTIFACT_GATEWAY_PRIVATE_KEY_FILE,
        &["localhost"],
        None,
        ExtendedKeyUsagePurpose::ServerAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE,
        RUNTIME_ARTIFACT_DATA_PRIVATE_KEY_FILE,
        &["localhost"],
        None,
        ExtendedKeyUsagePurpose::ServerAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE,
        RUNTIME_GATEWAY_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(PUBLIC_GATEWAY_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        RUNTIME_ORCHESTRATION_CLIENT_CERTIFICATE_FILE,
        RUNTIME_ORCHESTRATION_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(SCHEDULER_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )
}

fn write_local_leaf_certificate(
    tls_directory: &Path,
    certificate_name: &str,
    private_key_name: &str,
    dns_names: &[&str],
    workload_identity: Option<&str>,
    usage: ExtendedKeyUsagePurpose,
    issuer: &Issuer<'_, KeyPair>,
) -> Result<(), CliError> {
    let mut params = CertificateParams::new(
        dns_names
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>(),
    )
    .map_err(|_| invalid_local_identity(tls_directory, "cannot construct local TLS certificate"))?;
    if let Some(workload_identity) = workload_identity {
        params
            .subject_alt_names
            .push(SanType::URI(workload_identity.try_into().map_err(
                |_| invalid_local_identity(tls_directory, "local workload identity is invalid"),
            )?));
    }
    params.use_authority_key_identifier_extension = true;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![usage];
    let key = KeyPair::generate().map_err(|_| {
        invalid_local_identity(tls_directory, "cannot generate local TLS certificate key")
    })?;
    let certificate = params
        .signed_by(&key, issuer)
        .map_err(|_| invalid_local_identity(tls_directory, "cannot sign local TLS certificate"))?;
    write_new(
        &tls_directory.join(certificate_name),
        certificate.pem().as_bytes(),
    )?;
    write_sensitive_new(
        &tls_directory.join(private_key_name),
        key.serialize_pem().as_bytes(),
    )
}

fn fresh_resource_id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).expect("UUID v7 creates a valid resource ID")
}

fn tagged_digest(tag: &str, value: &str, identity_directory: &Path) -> Result<String, CliError> {
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "tag": tag,
        "value": value,
    }))
    .map_err(|_| {
        invalid_local_identity(identity_directory, "cannot construct local identity digest")
    })
}

fn build_local_jwks(
    key_pair: &KeyPair,
    key_id: &str,
    identity_directory: &Path,
) -> Result<serde_json::Value, CliError> {
    let public_key_info = key_pair.subject_public_key_info();
    let (_, public_key_info) = SubjectPublicKeyInfo::from_der(&public_key_info).map_err(|_| {
        invalid_local_identity(identity_directory, "cannot parse local issuer public key")
    })?;
    let PublicKey::RSA(public_key) = public_key_info.parsed().map_err(|_| {
        invalid_local_identity(identity_directory, "cannot parse local issuer RSA key")
    })?
    else {
        return Err(invalid_local_identity(
            identity_directory,
            "local issuer key is not RSA",
        ));
    };
    let modulus = positive_integer(public_key.modulus).ok_or_else(|| {
        invalid_local_identity(identity_directory, "local issuer RSA modulus is invalid")
    })?;
    let exponent = positive_integer(public_key.exponent).ok_or_else(|| {
        invalid_local_identity(identity_directory, "local issuer RSA exponent is invalid")
    })?;
    Ok(serde_json::json!({
        "keys": [{
            "alg": "RS256",
            "e": URL_SAFE_NO_PAD.encode(exponent),
            "kid": key_id,
            "kty": "RSA",
            "n": URL_SAFE_NO_PAD.encode(modulus),
            "use": "sig",
        }],
    }))
}

fn positive_integer(value: &[u8]) -> Option<&[u8]> {
    let value = value.strip_prefix(&[0]).unwrap_or(value);
    (!value.is_empty()).then_some(value)
}

fn pkcs1_private_key_from_pkcs8(value: &[u8]) -> Option<&[u8]> {
    let (outer, remainder) = der_tlv(value, 0x30)?;
    if !remainder.is_empty() {
        return None;
    }
    let (_, outer) = der_tlv(outer, 0x02)?;
    let (_, outer) = der_tlv(outer, 0x30)?;
    let (private_key, _) = der_tlv(outer, 0x04)?;
    Some(private_key)
}

fn der_tlv(value: &[u8], expected_tag: u8) -> Option<(&[u8], &[u8])> {
    let (&tag, remaining) = value.split_first()?;
    if tag != expected_tag {
        return None;
    }
    let (&first_length, remaining) = remaining.split_first()?;
    let (length, remaining) = if first_length & 0x80 == 0 {
        (usize::from(first_length), remaining)
    } else {
        let length_bytes = usize::from(first_length & 0x7f);
        if length_bytes == 0 || length_bytes > std::mem::size_of::<usize>() {
            return None;
        }
        let (encoded_length, remaining) = remaining.split_at_checked(length_bytes)?;
        let length = encoded_length.iter().try_fold(0usize, |length, byte| {
            length.checked_mul(256)?.checked_add(usize::from(*byte))
        })?;
        (length, remaining)
    };
    let (content, remaining) = remaining.split_at_checked(length)?;
    Some((content, remaining))
}

fn issue_initial_local_access_token(
    identity_directory: &Path,
    identity: &LocalIdentityState,
    private_key_der: &[u8],
    issued_at_unix_seconds: u64,
) -> Result<(), CliError> {
    let token = sign_local_access_token(identity, private_key_der, issued_at_unix_seconds)
        .map_err(|_| {
            invalid_local_identity(
                identity_directory,
                "cannot sign local developer access token",
            )
        })?;
    write_sensitive_new(
        &identity_directory.join(IDENTITY_ACCESS_TOKEN_FILE),
        token.as_bytes(),
    )
}

fn sign_local_access_token(
    identity: &LocalIdentityState,
    private_key_der: &[u8],
    issued_at_unix_seconds: u64,
) -> Result<String, ()> {
    let _ = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
    let issued_at = i64::try_from(issued_at_unix_seconds).map_err(|_| ())?;
    let expires_at = issued_at
        .checked_add(LOCAL_ACCESS_TOKEN_TTL_SECONDS)
        .ok_or(())?;
    let claims = LocalAccessTokenClaims {
        iss: &identity.issuer,
        aud: &identity.audience,
        sub: &identity.developer_subject,
        jti: format!("local-token-{}", Uuid::now_v7()),
        iat: issued_at,
        exp: expires_at,
        tenant_id: &identity.tenant_id,
        principal_kind: "agent_author",
        authn_strength: "single_factor",
    };
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(identity.key_id.clone());
    header.typ = Some("JWT".to_owned());
    encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_der(private_key_der),
    )
    .map_err(|_| ())
}

fn rotate_local_access_token(root: &Path, issued_at: SystemTime) -> Result<String, CliError> {
    let state_directory = root.join(PROJECT_DIRECTORY);
    let state = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity(&state_directory, &state.identity)?;
    let private_key_path = state_directory
        .join(IDENTITY_DIRECTORY)
        .join(IDENTITY_PRIVATE_KEY_FILE);
    let private_key = read_bounded_identity_file(&private_key_path)?;
    let key_pair = KeyPair::from_pem(std::str::from_utf8(&private_key).map_err(|_| {
        CliError::InvalidLocalIdentity {
            path: state_directory.display().to_string(),
        }
    })?)
    .map_err(|_| CliError::InvalidLocalIdentity {
        path: state_directory.display().to_string(),
    })?;
    if !key_pair.is_compatible(&PKCS_RSA_SHA256) {
        return Err(CliError::InvalidLocalIdentity {
            path: state_directory.display().to_string(),
        });
    }
    let jwks =
        build_local_jwks(&key_pair, &state.identity.key_id, &state_directory).map_err(|_| {
            CliError::InvalidLocalIdentity {
                path: state_directory.display().to_string(),
            }
        })?;
    let jwks_digest = canonical_digest(&jwks).map_err(|_| CliError::InvalidLocalIdentity {
        path: state_directory.display().to_string(),
    })?;
    if jwks_digest != state.identity.jwks_digest {
        return Err(CliError::InvalidLocalIdentity {
            path: state_directory.display().to_string(),
        });
    }
    let private_key_der = key_pair.serialize_der();
    let private_key_der = pkcs1_private_key_from_pkcs8(&private_key_der).ok_or_else(|| {
        CliError::InvalidLocalIdentity {
            path: state_directory.display().to_string(),
        }
    })?;
    let issued_at = issued_at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CliError::InvalidClock)?
        .as_secs();
    let token =
        sign_local_access_token(&state.identity, private_key_der, issued_at).map_err(|_| {
            CliError::InvalidLocalIdentity {
                path: state_directory.display().to_string(),
            }
        })?;
    write_sensitive_replace(
        &state_directory
            .join(IDENTITY_DIRECTORY)
            .join(IDENTITY_ACCESS_TOKEN_FILE),
        token.as_bytes(),
    )?;
    Ok(token)
}

fn load_local_project_state(state_directory: &Path) -> Result<LocalProjectState, CliError> {
    let path = state_directory.join(PROJECT_STATE_FILE);
    let bytes = read_bounded_identity_file(&path)?;
    serde_json::from_slice(&bytes).map_err(|_| CliError::InvalidLocalIdentity {
        path: state_directory.display().to_string(),
    })
}

fn validate_loaded_local_identity(
    state_directory: &Path,
    identity: &LocalIdentityState,
) -> Result<(), CliError> {
    let invalid = || CliError::InvalidLocalIdentity {
        path: state_directory.display().to_string(),
    };
    if identity.schema_version != 1
        || identity.issuer.is_empty()
        || identity.audience != LOCAL_OIDC_AUDIENCE
        || identity.key_id.is_empty()
        || identity.developer_subject.is_empty()
        || ResourceId::parse_expected(&identity.tenant_id, ResourceKind::Tenant).is_err()
        || ResourceId::parse_expected(&identity.developer_principal_id, ResourceKind::Principal)
            .is_err()
        || ResourceId::parse_expected(&identity.installation_principal_id, ResourceKind::Principal)
            .is_err()
        || ResourceId::parse_expected(
            &identity.installation_request_id,
            ResourceKind::ServerRequest,
        )
        .is_err()
        || identity
            .jwks_digest
            .parse::<insight_platform_contracts::Sha256Digest>()
            .is_err()
        || identity
            .authentication_authority_digest
            .parse::<insight_platform_contracts::Sha256Digest>()
            .is_err()
        || identity
            .bootstrap_config_digest
            .parse::<insight_platform_contracts::Sha256Digest>()
            .is_err()
    {
        return Err(invalid());
    }
    let jwks_path = state_directory
        .join(IDENTITY_DIRECTORY)
        .join(IDENTITY_JWKS_FILE);
    let jwks: serde_json::Value =
        serde_json::from_slice(&read_bounded_identity_file(&jwks_path)?).map_err(|_| invalid())?;
    if canonical_digest(&jwks).ok().as_deref() != Some(identity.jwks_digest.as_str()) {
        return Err(invalid());
    }
    let bootstrap_path = state_directory
        .join(IDENTITY_DIRECTORY)
        .join(IDENTITY_BOOTSTRAP_CONFIG_FILE);
    let bootstrap: serde_json::Value =
        serde_json::from_slice(&read_bounded_identity_file(&bootstrap_path)?)
            .map_err(|_| invalid())?;
    if canonical_digest(&bootstrap).ok().as_deref()
        != Some(identity.bootstrap_config_digest.as_str())
    {
        return Err(invalid());
    }
    Ok(())
}

fn read_bounded_identity_file(path: &Path) -> Result<Vec<u8>, CliError> {
    const MAX_IDENTITY_FILE_BYTES: u64 = 65_536;

    let metadata = fs::metadata(path).map_err(|source| CliError::ReadLocalIdentity {
        path: path.display().to_string(),
        source,
    })?;
    if !metadata.is_file() || metadata.len() > MAX_IDENTITY_FILE_BYTES {
        return Err(CliError::InvalidLocalIdentity {
            path: path.display().to_string(),
        });
    }
    fs::read(path).map_err(|source| CliError::ReadLocalIdentity {
        path: path.display().to_string(),
        source,
    })
}

fn invalid_local_identity(identity_directory: &Path, reason: &str) -> CliError {
    CliError::InitializeProject {
        path: identity_directory.display().to_string(),
        source: std::io::Error::other(reason),
    }
}

fn write_new(path: &Path, contents: &[u8]) -> Result<(), CliError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })?;
    file.write_all(contents)
        .and_then(|_| file.sync_all())
        .map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })
}

fn write_sensitive_new(path: &Path, contents: &[u8]) -> Result<(), CliError> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(path)
        .map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })?;
    file.write_all(contents)
        .and_then(|_| file.sync_all())
        .map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })
}

fn write_sensitive_replace(path: &Path, contents: &[u8]) -> Result<(), CliError> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt as _;

    let temporary = path.with_extension(format!("{}.tmp", Uuid::now_v7()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let result = (|| {
        let mut file =
            options
                .open(&temporary)
                .map_err(|source| CliError::RotateLocalAccessToken {
                    path: temporary.display().to_string(),
                    source,
                })?;
        file.write_all(contents)
            .and_then(|_| file.sync_all())
            .map_err(|source| CliError::RotateLocalAccessToken {
                path: temporary.display().to_string(),
                source,
            })?;
        fs::rename(&temporary, path).map_err(|source| CliError::RotateLocalAccessToken {
            path: path.display().to_string(),
            source,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub trait DoctorProbe {
    fn command(&self, program: &str, arguments: &[&str]) -> Result<String, String>;
    fn port_available(&self, port: u16) -> Result<(), String>;
}

pub struct SystemDoctorProbe;

impl DoctorProbe for SystemDoctorProbe {
    fn command(&self, program: &str, arguments: &[&str]) -> Result<String, String> {
        let output = ProcessCommand::new(program)
            .args(arguments)
            .output()
            .map_err(|error| error.to_string())?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = stdout
            .lines()
            .chain(stderr.lines())
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or("command completed without output");
        if output.status.success() {
            Ok(truncate_detail(detail))
        } else {
            Err(truncate_detail(detail))
        }
    }

    fn port_available(&self, port: u16) -> Result<(), String> {
        TcpListener::bind(("127.0.0.1", port))
            .map(drop)
            .map_err(|error| error.to_string())
    }
}

fn truncate_detail(detail: &str) -> String {
    detail.chars().take(160).collect()
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DoctorReport {
    pub schema_version: u32,
    pub kind: String,
    pub checks: Vec<DoctorCheck>,
    pub ready: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DoctorCheck {
    pub name: String,
    pub required: bool,
    pub status: DoctorStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DoctorStatus {
    Passed,
    Failed,
    Unavailable,
}

pub fn doctor_report(probe: &dyn DoctorProbe) -> DoctorReport {
    let mut checks = Vec::new();
    let rustc = probe.command("rustc", &["--version"]);
    checks.push(version_check("rustc", true, rustc, EXPECTED_RUSTC_PREFIX));
    checks.push(command_check(
        "docker_engine",
        true,
        probe.command("docker", &["version", "--format", "{{.Server.Version}}"]),
    ));
    checks.push(command_check(
        "docker_compose_v2",
        true,
        probe.command("docker", &["compose", "version"]),
    ));
    checks.push(command_check(
        "runsc",
        false,
        probe.command("runsc", &["--version"]),
    ));
    for port in DEFAULT_PORTS {
        checks.push(port_check(*port, probe.port_available(*port)));
    }
    let ready = checks
        .iter()
        .all(|check| !check.required || check.status == DoctorStatus::Passed);
    DoctorReport {
        schema_version: 1,
        kind: "insight.dev.doctor-report/v1".to_owned(),
        checks,
        ready,
    }
}

fn doctor_report_at(
    probe: &dyn DoctorProbe,
    current_directory: &Path,
    now: SystemTime,
) -> DoctorReport {
    let mut report = doctor_report(probe);
    if let Some(check) = local_identity_doctor_check(current_directory, now) {
        report.checks.push(check);
        report.ready = report
            .checks
            .iter()
            .all(|check| !check.required || check.status == DoctorStatus::Passed);
    }
    report
}

fn local_identity_doctor_check(root: &Path, now: SystemTime) -> Option<DoctorCheck> {
    let state_directory = root.join(PROJECT_DIRECTORY);
    match state_directory.try_exists() {
        Ok(false) => return None,
        Err(_) => {
            return Some(DoctorCheck::failed(
                "local_identity",
                true,
                "cannot inspect local project state; fix its permissions or initialize a fresh project"
                    .to_owned(),
            ));
        }
        Ok(true) => {}
    }
    let state = match load_local_project_state(&state_directory).and_then(|state| {
        validate_loaded_local_identity(&state_directory, &state.identity)?;
        Ok(state)
    }) {
        Ok(state) => state,
        Err(_) => {
            return Some(DoctorCheck::failed(
                "local_identity",
                true,
                "local identity state is invalid; initialize a fresh local project".to_owned(),
            ));
        }
    };
    let token_path = state_directory
        .join(IDENTITY_DIRECTORY)
        .join(IDENTITY_ACCESS_TOKEN_FILE);
    let expires_at = read_bounded_identity_file(&token_path)
        .ok()
        .and_then(|token| cached_token_expiry(&token));
    let now = now
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok());
    let Some(expires_at) = expires_at else {
        return Some(DoctorCheck::failed(
            "local_identity",
            true,
            "cached local token is unreadable; run `insight token`".to_owned(),
        ));
    };
    let Some(now) = now else {
        return Some(DoctorCheck::failed(
            "local_identity",
            true,
            "local clock is before the Unix epoch".to_owned(),
        ));
    };
    if expires_at <= now {
        return Some(DoctorCheck::failed(
            "local_identity",
            true,
            "cached local token has expired; run `insight token`".to_owned(),
        ));
    }
    Some(DoctorCheck::passed(
        "local_identity",
        true,
        format!(
            "issuer={} jwks_digest={} bootstrap_config_digest={} token_expires_at_unix_seconds={expires_at}",
            state.identity.issuer,
            state.identity.jwks_digest,
            state.identity.bootstrap_config_digest,
        ),
    ))
}

fn cached_token_expiry(token: &[u8]) -> Option<i64> {
    let token = std::str::from_utf8(token).ok()?;
    let mut sections = token.split('.');
    let _header = sections.next()?;
    let claims = sections.next()?;
    let _signature = sections.next()?;
    if sections.next().is_some() {
        return None;
    }
    let claims = URL_SAFE_NO_PAD.decode(claims).ok()?;
    serde_json::from_slice::<serde_json::Value>(&claims)
        .ok()?
        .get("exp")?
        .as_i64()
}

fn version_check(
    name: &str,
    required: bool,
    result: Result<String, String>,
    required_prefix: &str,
) -> DoctorCheck {
    match result {
        Ok(detail) if detail.starts_with(required_prefix) => {
            DoctorCheck::passed(name, required, detail)
        }
        Ok(detail) => DoctorCheck {
            name: name.to_owned(),
            required,
            status: DoctorStatus::Failed,
            detail: format!("expected {required_prefix}; found {detail}"),
        },
        Err(detail) => DoctorCheck::failed(name, required, detail),
    }
}

fn command_check(name: &str, required: bool, result: Result<String, String>) -> DoctorCheck {
    match result {
        Ok(detail) => DoctorCheck::passed(name, required, detail),
        Err(detail) => DoctorCheck::failed(name, required, detail),
    }
}

fn port_check(port: u16, result: Result<(), String>) -> DoctorCheck {
    match result {
        Ok(()) => DoctorCheck::passed(&format!("port_{port}"), true, "available".to_owned()),
        Err(detail) => DoctorCheck::failed(&format!("port_{port}"), true, detail),
    }
}

impl DoctorCheck {
    fn passed(name: &str, required: bool, detail: String) -> Self {
        Self {
            name: name.to_owned(),
            required,
            status: DoctorStatus::Passed,
            detail,
        }
    }

    fn failed(name: &str, required: bool, detail: String) -> Self {
        Self {
            name: name.to_owned(),
            required,
            status: if required {
                DoctorStatus::Failed
            } else {
                DoctorStatus::Unavailable
            },
            detail,
        }
    }
}

pub fn execute(
    command: CliCommand,
    current_directory: &Path,
    probe: &dyn DoctorProbe,
) -> Result<String, CliError> {
    match command {
        CliCommand::Help => Ok(usage().to_owned()),
        CliCommand::Doctor { json } => {
            let report = doctor_report_at(probe, current_directory, SystemTime::now());
            let output = if json {
                serde_json::to_string_pretty(&report).expect("doctor report is serializable") + "\n"
            } else {
                render_doctor_report(&report)
            };
            if report.ready {
                Ok(output)
            } else {
                Err(CliError::DoctorFailed { report: output })
            }
        }
        CliCommand::Init { root, project_name } => {
            let root = if root.is_absolute() {
                root
            } else {
                current_directory.join(root)
            };
            let state = initialize_project(&root, project_name.as_deref(), SystemTime::now())?;
            Ok(format!(
                "initialized local development state for {} at {}\n",
                state.project_name,
                root.join(PROJECT_DIRECTORY).display()
            ))
        }
        CliCommand::Token { root } => {
            let root = if root.is_absolute() {
                root
            } else {
                current_directory.join(root)
            };
            let token = rotate_local_access_token(&root, SystemTime::now())?;
            Ok(format!("{token}\n"))
        }
    }
}

fn usage() -> &'static str {
    "Insight Platform development CLI\n\nUsage:\n  insight doctor [--json]\n  insight init [--path <directory>] [--name <name>]\n  insight token [--path <directory>]\n\n`token` writes a short-lived local development token and prints it only to stdout. `dev`, `status`, `logs`, and `stop` are added with the real M1 role closure; they are not aliases for the legacy runtime.\n"
}

fn render_doctor_report(report: &DoctorReport) -> String {
    let mut output = String::new();
    for check in &report.checks {
        let required = if check.required {
            "required"
        } else {
            "optional"
        };
        let status = match check.status {
            DoctorStatus::Passed => "passed",
            DoctorStatus::Failed => "failed",
            DoctorStatus::Unavailable => "unavailable",
        };
        output.push_str(&format!(
            "{status:11} {required:8} {:24} {}\n",
            check.name, check.detail
        ));
    }
    output.push_str(if report.ready {
        "development prerequisites are ready\n"
    } else {
        "required development prerequisites are unavailable\n"
    });
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use insight_platform_api::{
        authentication::ExternalCredentialVerifier, oidc::InstalledOidcVerifierConfig,
    };
    use insight_platform_contracts::Sha256Digest;
    use std::{collections::BTreeMap, fs};
    use tempfile::TempDir;

    #[derive(Default)]
    struct FakeProbe {
        commands: BTreeMap<(String, Vec<String>), Result<String, String>>,
        ports: BTreeMap<u16, Result<(), String>>,
    }

    impl FakeProbe {
        fn ready() -> Self {
            let mut probe = Self::default();
            probe.commands.insert(
                ("rustc".to_owned(), vec!["--version".to_owned()]),
                Ok("rustc 1.94.1 (test)".to_owned()),
            );
            probe.commands.insert(
                (
                    "docker".to_owned(),
                    vec![
                        "version".to_owned(),
                        "--format".to_owned(),
                        "{{.Server.Version}}".to_owned(),
                    ],
                ),
                Ok("27.0.0".to_owned()),
            );
            probe.commands.insert(
                (
                    "docker".to_owned(),
                    vec!["compose".to_owned(), "version".to_owned()],
                ),
                Ok("Docker Compose version v2".to_owned()),
            );
            probe.commands.insert(
                ("runsc".to_owned(), vec!["--version".to_owned()]),
                Err("not installed".to_owned()),
            );
            for port in DEFAULT_PORTS {
                probe.ports.insert(*port, Ok(()));
            }
            probe
        }
    }

    impl DoctorProbe for FakeProbe {
        fn command(&self, program: &str, arguments: &[&str]) -> Result<String, String> {
            self.commands
                .get(&(
                    program.to_owned(),
                    arguments.iter().map(|value| (*value).to_owned()).collect(),
                ))
                .cloned()
                .unwrap_or_else(|| Err("unexpected command".to_owned()))
        }

        fn port_available(&self, port: u16) -> Result<(), String> {
            self.ports
                .get(&port)
                .cloned()
                .unwrap_or_else(|| Err("unexpected port".to_owned()))
        }
    }

    #[test]
    fn doctor_accepts_optional_missing_runsc() {
        let report = doctor_report(&FakeProbe::ready());
        assert!(report.ready);
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "runsc" && check.status == DoctorStatus::Unavailable));
    }

    #[test]
    fn doctor_rejects_missing_required_docker() {
        let mut probe = FakeProbe::ready();
        probe.commands.insert(
            (
                "docker".to_owned(),
                vec!["compose".to_owned(), "version".to_owned()],
            ),
            Err("not found".to_owned()),
        );
        let report = doctor_report(&probe);
        assert!(!report.ready);
        assert!(render_doctor_report(&report).contains("docker_compose_v2"));
    }

    #[test]
    fn doctor_reports_local_identity_without_sensitive_values() {
        let directory = TempDir::new().unwrap();
        let state = initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        let report = doctor_report_at(&FakeProbe::ready(), directory.path(), SystemTime::now());
        let check = report
            .checks
            .iter()
            .find(|check| check.name == "local_identity")
            .unwrap();
        assert_eq!(check.status, DoctorStatus::Passed);
        assert!(check.detail.contains(&state.identity.issuer));
        assert!(check.detail.contains(&state.identity.jwks_digest));
        assert!(check
            .detail
            .contains(&state.identity.bootstrap_config_digest));
        assert!(!check.detail.contains("PRIVATE KEY"));
        assert!(!check.detail.contains("eyJ"));
    }

    #[test]
    fn doctor_marks_an_expired_cached_token_as_actionable_failure() {
        let directory = TempDir::new().unwrap();
        initialize_project(directory.path(), Some("demo"), UNIX_EPOCH).unwrap();
        let report = doctor_report_at(
            &FakeProbe::ready(),
            directory.path(),
            UNIX_EPOCH + std::time::Duration::from_secs(LOCAL_ACCESS_TOKEN_TTL_SECONDS as u64 + 1),
        );
        let check = report
            .checks
            .iter()
            .find(|check| check.name == "local_identity")
            .unwrap();
        assert_eq!(check.status, DoctorStatus::Failed);
        assert!(check.detail.contains("insight token"));
        assert!(!report.ready);
    }

    #[test]
    fn init_writes_gitignored_closed_local_state() {
        let directory = TempDir::new().unwrap();
        let state =
            initialize_project(directory.path(), Some("demo-project"), SystemTime::now()).unwrap();
        assert_eq!(state.kind, PROJECT_KIND);
        assert_eq!(state.profiles.len(), 2);
        let root = directory.path().join(PROJECT_DIRECTORY);
        assert_eq!(
            fs::read_to_string(root.join(PROJECT_GITIGNORE_FILE)).unwrap(),
            "# Generated local development state.\n*\n!.gitignore\n"
        );
        let persisted: LocalProjectState =
            serde_json::from_slice(&fs::read(root.join(PROJECT_STATE_FILE)).unwrap()).unwrap();
        assert_eq!(persisted, state);
        let private_key = root
            .join(IDENTITY_DIRECTORY)
            .join(IDENTITY_PRIVATE_KEY_FILE);
        let token = root
            .join(IDENTITY_DIRECTORY)
            .join(IDENTITY_ACCESS_TOKEN_FILE);
        assert!(private_key.is_file());
        assert!(token.is_file());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            assert_eq!(
                fs::metadata(&private_key).unwrap().permissions().mode() & 0o077,
                0
            );
            assert_eq!(
                fs::metadata(&token).unwrap().permissions().mode() & 0o077,
                0
            );
        }
        let jwks: serde_json::Value = serde_json::from_slice(
            &fs::read(root.join(IDENTITY_DIRECTORY).join(IDENTITY_JWKS_FILE)).unwrap(),
        )
        .unwrap();
        let tls = root.join(RUNTIME_DIRECTORY).join(RUNTIME_TLS_DIRECTORY);
        for certificate in [
            RUNTIME_CA_CERTIFICATE_FILE,
            RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE,
            RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE,
            RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE,
            RUNTIME_ORCHESTRATION_CLIENT_CERTIFICATE_FILE,
        ] {
            let bytes = fs::read(tls.join(certificate)).unwrap();
            assert!(bytes.starts_with(b"-----BEGIN CERTIFICATE-----"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            for private_key in [
                RUNTIME_CA_PRIVATE_KEY_FILE,
                RUNTIME_ARTIFACT_GATEWAY_PRIVATE_KEY_FILE,
                RUNTIME_ARTIFACT_DATA_PRIVATE_KEY_FILE,
                RUNTIME_GATEWAY_CLIENT_PRIVATE_KEY_FILE,
                RUNTIME_ORCHESTRATION_CLIENT_PRIVATE_KEY_FILE,
            ] {
                assert_eq!(
                    fs::metadata(tls.join(private_key))
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o077,
                    0
                );
            }
        }
        let verifier = InstalledOidcVerifierConfig {
            issuer: persisted.identity.issuer.clone(),
            audience: persisted.identity.audience.clone(),
            jwks_digest: persisted.identity.jwks_digest.parse().unwrap(),
            jwks: jwks.clone(),
        }
        .install()
        .unwrap();
        let verified = verifier
            .verify(&fs::read_to_string(&token).unwrap(), Utc::now())
            .unwrap();
        assert_eq!(verified.tenant_id.to_string(), persisted.identity.tenant_id);
        assert_eq!(
            verified.authentication_authority_digest,
            persisted
                .identity
                .authentication_authority_digest
                .parse::<Sha256Digest>()
                .unwrap()
        );
        let bootstrap: serde_json::Value = serde_json::from_slice(
            &fs::read(
                root.join(IDENTITY_DIRECTORY)
                    .join(IDENTITY_BOOTSTRAP_CONFIG_FILE),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            canonical_digest(&bootstrap).unwrap(),
            persisted.identity.bootstrap_config_digest
        );
        for path in [
            root.join(PROJECT_STATE_FILE),
            root.join(IDENTITY_DIRECTORY).join(IDENTITY_JWKS_FILE),
            root.join(IDENTITY_DIRECTORY)
                .join(IDENTITY_BOOTSTRAP_CONFIG_FILE),
        ] {
            assert!(!fs::read_to_string(path).unwrap().contains("PRIVATE KEY"));
        }
    }

    #[test]
    fn init_refuses_to_overwrite_existing_state() {
        let directory = TempDir::new().unwrap();
        initialize_project(directory.path(), Some("demo"), UNIX_EPOCH).unwrap();
        assert!(matches!(
            initialize_project(directory.path(), Some("demo"), UNIX_EPOCH),
            Err(CliError::ProjectAlreadyInitialized(_))
        ));
    }

    #[test]
    fn token_rotates_a_verifier_accepted_short_lived_credential() {
        let directory = TempDir::new().unwrap();
        let state = initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        let identity_directory = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(IDENTITY_DIRECTORY);
        let token_path = identity_directory.join(IDENTITY_ACCESS_TOKEN_FILE);
        let original = fs::read_to_string(&token_path).unwrap();
        let rotated = rotate_local_access_token(directory.path(), SystemTime::now()).unwrap();
        assert_eq!(fs::read_to_string(&token_path).unwrap(), rotated);
        assert_ne!(original, rotated);
        let jwks: serde_json::Value =
            serde_json::from_slice(&fs::read(identity_directory.join(IDENTITY_JWKS_FILE)).unwrap())
                .unwrap();
        let verifier = InstalledOidcVerifierConfig {
            issuer: state.identity.issuer,
            audience: state.identity.audience,
            jwks_digest: state.identity.jwks_digest.parse().unwrap(),
            jwks,
        }
        .install()
        .unwrap();
        assert!(verifier.verify(&rotated, Utc::now()).is_ok());
    }

    #[test]
    fn init_rejects_unsafe_project_names() {
        let directory = TempDir::new().unwrap();
        assert!(matches!(
            initialize_project(directory.path(), Some("not safe"), UNIX_EPOCH),
            Err(CliError::InvalidProjectName(_))
        ));
    }

    #[test]
    fn command_parser_rejects_unclosed_options() {
        let arguments = vec![OsString::from("init"), OsString::from("--path")];
        assert!(matches!(
            parse_command(&arguments),
            Err(CliError::MissingValue("--path"))
        ));
    }

    #[test]
    fn command_parser_accepts_token_path() {
        let arguments = vec![
            OsString::from("token"),
            OsString::from("--path"),
            OsString::from("demo"),
        ];
        assert_eq!(
            parse_command(&arguments).unwrap(),
            CliCommand::Token {
                root: PathBuf::from("demo"),
            }
        );
    }
}
