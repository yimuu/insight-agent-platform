//! Product-facing command-line entry points for the Platform `/v1` local development profile.
//!
//! This crate intentionally has no database, worker, or internal RPC dependency. It may inspect
//! host prerequisites and create project-local development state, but all future business
//! mutations must use the public Gateway `/v1` contract.

pub mod agent_compiler;

mod agent;
mod apply;
mod apply_journal;
mod artifact;
mod artifact_journal;
mod dev_profile;
mod full_profile;
mod public_client;
mod release;
mod run;
mod run_journal;
mod task;
mod task_journal;
#[cfg(test)]
mod workspace_assets;

pub use dev_profile::DevProfile;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ArtifactPurpose, ArtifactRetentionPolicy,
    DataClassification, JsonLimits, PublicJobState, ResourceDocument, ResourceId, ResourceKind,
    RunState, SandboxArtifactIoPolicyDocument, SchedulingPolicyDocument, UtcTimestamp, ValueRef,
};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, PublicKeyData, SanType, PKCS_RSA_SHA256,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
#[cfg(unix)]
use std::os::fd::AsRawFd as _;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;
use x509_parser::{
    extensions::GeneralName, pem::parse_x509_pem, prelude::FromDer as _, public_key::PublicKey,
    x509::SubjectPublicKeyInfo,
};

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
const RUNTIME_NATS_SERVER_CERTIFICATE_FILE: &str = "nats-server.pem";
const RUNTIME_NATS_SERVER_PRIVATE_KEY_FILE: &str = "nats-server-key.pem";
const RUNTIME_NATS_CLIENT_CERTIFICATE_FILE: &str = "nats-client.pem";
const RUNTIME_NATS_CLIENT_PRIVATE_KEY_FILE: &str = "nats-client-key.pem";
const RUNTIME_CONFIGURATION_DIRECTORY: &str = "config";
const RUNTIME_GATEWAY_MANAGEMENT_CONFIG_FILE: &str = "gateway-management.json";
const RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE: &str = "gateway-runtime.json";
const RUNTIME_ARTIFACT_GATEWAY_CONFIG_FILE: &str = "artifact-gateway.json";
const RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE: &str = "artifact-bootstrap.json";
const RUNTIME_ARTIFACT_DATA_CONFIG_FILE: &str = "artifact-data.json";
const RUNTIME_ORCHESTRATION_CONFIG_FILE: &str = "orchestration.json";
const RUNTIME_CAPABILITY_NATIVE_CONFIG_FILE: &str = "capability-native.json";
const RUNTIME_REGISTRY_VALIDATION_CONFIG_FILE: &str = "registry-validation.json";
const RUNTIME_SANDBOX_KUBERNETES_CONFIG_FILE: &str = "sandbox-kubernetes.json";
const RUNTIME_CURSOR_KEY_FILE: &str = "run-event-cursor-key";
const RUNTIME_PROFILE_STATE_FILE: &str = "profile.json";
const RUNTIME_PROFILE_SCHEMA_VERSION: u32 = 3;
const RUNTIME_PROFILE_KIND: &str = "insight.dev.runtime-profile/v2";
const RUNTIME_BUILD_STATE_FILE: &str = "build.json";
const RUNTIME_PROCESS_STATE_FILE: &str = "processes.json";
const RUNTIME_PROCESS_SCHEMA_VERSION: u32 = 3;
const RUNTIME_PROCESS_KIND: &str = "insight.dev.process-state/v3";
const RUNTIME_PROCESS_GENERATION_PREFIX: &str = "insight-platform-process-v3-";
const RUNTIME_LIFECYCLE_LOCK_FILE: &str = "lifecycle.lock";
const RUNTIME_COMPOSE_FILE: &str = "compose.yaml";
const RUNTIME_LOG_DIRECTORY: &str = "logs";
const DEV_COMPOSE_BYTES: &[u8] = include_bytes!("../../../deploy/dev/compose.yaml");
const LOCAL_ARTIFACT_BUCKET: &str = "insight-platform-artifacts";
const LOCAL_AWS_ENDPOINT: &str = "https://localhost.localstack.cloud:4566";
const LOCAL_SECRET_READINESS_NAME: &str = "insight/platform/readiness";
const LOCAL_SECRET_NAME_PREFIX: &str = "insight/platform/prepared";
#[cfg(test)]
const LOCAL_TEST_SECRET_READINESS_ARN: &str =
    "arn:aws:secretsmanager:us-east-1:000000000000:secret:insight/platform/readiness-local0";
const PUBLIC_GATEWAY_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/public-gateway";
const SCHEDULER_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/scheduler";
const LOCAL_OIDC_AUDIENCE: &str = "insight.platform/v1";
const LOCAL_ACCESS_TOKEN_TTL_SECONDS: i64 = 900;
const EXPECTED_RUSTC_PREFIX: &str = "rustc 1.94.1";
const MINIMUM_DEVELOPMENT_CPUS: u64 = 4;
const MINIMUM_DEVELOPMENT_MEMORY_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MINIMUM_DEVELOPMENT_DISK_KIB: u64 = 8 * 1024 * 1024;
const DEFAULT_PORTS: &[u16] = &[5432, 4222, 4566];
const DOCTOR_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const DOCTOR_COMMAND_POLL_INTERVAL: Duration = Duration::from_millis(25);
const OPENSANDBOX_SOURCE_COMMIT: &str = "c39b814f36ded4c61d5ac6f9332ee4dfbab86c00";
const OPENSANDBOX_SERVER_IMAGE_DIGEST: &str =
    "sha256:ae8dfbb277f40a39ff01ef35e5e1c10675acfe0fa9db15259b8f323e5efab778";
const OPENSANDBOX_CONTROLLER_IMAGE_DIGEST: &str =
    "sha256:a9a5f73c1785ebd955336ffa313973a35c1a1b662cb7afc4ea82d92021b3532a";
const OPENSANDBOX_EXECD_IMAGE_DIGEST: &str =
    "sha256:6cf7dba2f21f0b536e100563d841ac58a9f31c2b0a081b7ac76796a24d6f47e2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliCommand {
    Doctor {
        json: bool,
    },
    Version {
        json: bool,
    },
    UpdateCheck,
    UpdateApply {
        version: String,
    },
    Init {
        root: PathBuf,
        project_name: Option<String>,
    },
    Token {
        root: PathBuf,
    },
    Dev {
        root: PathBuf,
        profile: DevProfile,
    },
    Start {
        root: PathBuf,
    },
    Status {
        root: PathBuf,
    },
    Logs {
        root: PathBuf,
        role: Option<String>,
    },
    Stop {
        root: PathBuf,
    },
    Reset {
        root: PathBuf,
        confirm: Option<String>,
    },
    Apply {
        root: PathBuf,
        file: PathBuf,
        timeout_seconds: u64,
    },
    OperationWait {
        root: PathBuf,
        operation_id: String,
        timeout_seconds: u64,
    },
    RunCreate {
        root: PathBuf,
        file: PathBuf,
    },
    RunGet {
        root: PathBuf,
        run_id: String,
    },
    RunControl {
        root: PathBuf,
        run_id: String,
        action: CliRunControlAction,
    },
    RunResult {
        root: PathBuf,
        run_id: String,
    },
    RunWatch {
        root: PathBuf,
        run_id: String,
        timeout_seconds: u64,
    },
    TaskGet {
        root: PathBuf,
        task_id: String,
    },
    TaskResolve {
        root: PathBuf,
        task_id: String,
        action: CliTaskAction,
        file: Option<PathBuf>,
    },
    ArtifactGet {
        root: PathBuf,
        artifact_id: String,
    },
    ArtifactRead {
        root: PathBuf,
        artifact_id: String,
        output: PathBuf,
    },
    ArtifactUpload {
        root: PathBuf,
        file: PathBuf,
        purpose: String,
        classification: String,
        media_type: Option<String>,
        display_name: Option<String>,
        timeout_seconds: u64,
    },
    AgentValidate {
        root: PathBuf,
        file: PathBuf,
        online: bool,
        output: agent::AgentOutputOptions,
    },
    AgentPublish {
        root: PathBuf,
        file: PathBuf,
        wait: bool,
        output: agent::AgentOutputOptions,
    },
    AgentList {
        root: PathBuf,
        output: agent::AgentOutputOptions,
    },
    AgentGet {
        root: PathBuf,
        selector: String,
        output: agent::AgentOutputOptions,
    },
    AgentAdopt {
        root: PathBuf,
        name: String,
        agent_id: String,
        output: agent::AgentOutputOptions,
    },
    AgentRun {
        root: PathBuf,
        selector: String,
        input: Option<String>,
        file: Option<PathBuf>,
        detach: bool,
        timeout_seconds: u64,
        output: agent::AgentOutputOptions,
    },
    AgentLogs {
        root: PathBuf,
        selector: String,
        follow: bool,
        output: agent::AgentOutputOptions,
    },
    AgentResult {
        root: PathBuf,
        run_id: String,
        output: agent::AgentOutputOptions,
    },
    AdvancedHelp,
    Help,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliRunControlAction {
    Pause,
    Resume,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliTaskAction {
    SubmitInput,
    Approve,
    Reject,
    Cancel,
}

#[derive(Debug)]
pub enum CliError {
    Usage,
    UnknownCommand(String),
    MissingValue(&'static str),
    DuplicateOption(&'static str),
    UnsupportedOption(String),
    InvalidOptionValue {
        option: &'static str,
        value: String,
    },
    UnsupportedProfile(String),
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
    ReadApplyManifest {
        path: String,
        source: std::io::Error,
    },
    ReadRunRequest {
        path: String,
        source: std::io::Error,
    },
    ReadTaskInput {
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
    WorkspaceUnavailable(String),
    RuntimeUnavailable(String),
    RuntimeState(String),
    PublicClient(public_client::PublicClientError),
    Apply(apply::ApplyError),
    Run(run::RunClientError),
    Task(task::TaskClientError),
    Artifact(artifact::ArtifactClientError),
    Agent(agent::AgentCommandError),
    Release(release::ReleaseError),
    OperationTerminal {
        operation_id: String,
        state: String,
        detail: String,
    },
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
            Self::InvalidOptionValue { option, value } => {
                write!(formatter, "invalid value {value:?} for {option}")
            }
            Self::UnsupportedProfile(profile) => {
                write!(
                    formatter,
                    "unsupported development feature selection {profile:?}"
                )
            }
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
            Self::ReadApplyManifest { path, source } => {
                write!(formatter, "cannot read apply manifest at {path}: {source}")
            }
            Self::ReadRunRequest { path, source } => {
                write!(formatter, "cannot read Run request at {path}: {source}")
            }
            Self::ReadTaskInput { path, source } => {
                write!(formatter, "cannot read Task input at {path}: {source}")
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
            Self::WorkspaceUnavailable(detail) => {
                write!(formatter, "local workspace is unavailable: {detail}")
            }
            Self::RuntimeUnavailable(detail) => write!(
                formatter,
                "local development runtime is unavailable: {detail}"
            ),
            Self::RuntimeState(detail) => write!(
                formatter,
                "local development runtime state is invalid: {detail}"
            ),
            Self::PublicClient(error) => write!(formatter, "{error}"),
            Self::Apply(error) => write!(formatter, "{error}"),
            Self::Run(error) => write!(formatter, "{error}"),
            Self::Task(error) => write!(formatter, "{error}"),
            Self::Artifact(error) => write!(formatter, "{error}"),
            Self::Agent(error) => write!(formatter, "{error}"),
            Self::Release(error) => write!(formatter, "{error}"),
            Self::OperationTerminal {
                operation_id,
                state,
                detail,
            } => write!(
                formatter,
                "operation {operation_id} reached terminal state {state}: {detail}"
            ),
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
            | Self::RotateLocalAccessToken { source, .. }
            | Self::ReadApplyManifest { source, .. }
            | Self::ReadRunRequest { source, .. }
            | Self::ReadTaskInput { source, .. } => Some(source),
            Self::PublicClient(source) => Some(source),
            Self::Apply(source) => Some(source),
            Self::Run(source) => Some(source),
            Self::Task(source) => Some(source),
            Self::Artifact(source) => Some(source),
            Self::Release(source) => Some(source),
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
            | Self::InvalidOptionValue { .. }
            | Self::UnsupportedProfile(_)
            | Self::InvalidProjectName(_)
            | Self::MissingProjectName(_) => 2,
            Self::DoctorFailed { .. }
            | Self::InitializeProject { .. }
            | Self::ReadLocalIdentity { .. }
            | Self::ReadApplyManifest { .. }
            | Self::ReadRunRequest { .. }
            | Self::ReadTaskInput { .. }
            | Self::InvalidLocalIdentity { .. }
            | Self::RotateLocalAccessToken { .. }
            | Self::InvalidClock => 1,
            Self::WorkspaceUnavailable(_)
            | Self::RuntimeUnavailable(_)
            | Self::RuntimeState(_)
            | Self::PublicClient(_)
            | Self::Apply(_)
            | Self::Run(_)
            | Self::Task(_)
            | Self::Artifact(_)
            | Self::Agent(_)
            | Self::Release(_)
            | Self::OperationTerminal { .. } => 1,
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
        "version" => parse_version(&arguments[1..]),
        "update" => parse_update(&arguments[1..]),
        "init" => parse_init(&arguments[1..]),
        "token" => parse_token(&arguments[1..]),
        "dev" => parse_dev(&arguments[1..]),
        "start" => Ok(CliCommand::Start {
            root: parse_path_only(&arguments[1..])?,
        }),
        "status" => parse_status(&arguments[1..]),
        "logs" => parse_logs(&arguments[1..]),
        "stop" => parse_stop(&arguments[1..]),
        "reset" => parse_reset(&arguments[1..]),
        "apply" => parse_apply(&arguments[1..]),
        "operation" => parse_operation(&arguments[1..]),
        "run" => parse_run(&arguments[1..]),
        "task" => parse_task(&arguments[1..]),
        "artifact" => parse_artifact(&arguments[1..]),
        "agent" => parse_agent(&arguments[1..]),
        "advanced" => {
            if arguments.len() == 1
                || (arguments.len() == 2 && matches!(arguments[1].to_str(), Some("--help" | "-h")))
            {
                Ok(CliCommand::AdvancedHelp)
            } else {
                Err(CliError::Usage)
            }
        }
        value => Err(CliError::UnknownCommand(value.to_owned())),
    }
}

fn parse_version(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    match arguments {
        [] => Ok(CliCommand::Version { json: false }),
        [flag] if flag == "--json" => Ok(CliCommand::Version { json: true }),
        [flag] => Err(CliError::UnsupportedOption(lossy(flag))),
        _ => Err(CliError::Usage),
    }
}

fn parse_update(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    match arguments {
        [action] if action == "check" => Ok(CliCommand::UpdateCheck),
        [action, version_flag, version] if action == "apply" && version_flag == "--version" => {
            let version = version
                .to_str()
                .ok_or_else(|| CliError::InvalidOptionValue {
                    option: "--version",
                    value: version.to_string_lossy().into_owned(),
                })?
                .to_owned();
            release::validate_exact_version(&version).map_err(CliError::Release)?;
            Ok(CliCommand::UpdateApply { version })
        }
        [action] if action == "apply" => Err(CliError::MissingValue("--version")),
        [action, ..] if action != "check" && action != "apply" => Err(CliError::UnknownCommand(
            format!("update {}", lossy(action)),
        )),
        _ => Err(CliError::Usage),
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

fn parse_dev(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let mut root = None;
    let mut features = None;
    let mut offline = false;
    let mut from_source = false;
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
            "--features" => {
                if features.is_some() {
                    return Err(CliError::DuplicateOption("--features"));
                }
                let Some(value) = arguments.get(cursor + 1).and_then(|value| value.to_str()) else {
                    return Err(CliError::MissingValue("--features"));
                };
                features = Some(value.to_owned());
                cursor += 2;
            }
            "--offline" => {
                if offline {
                    return Err(CliError::DuplicateOption("--offline"));
                }
                offline = true;
                cursor += 1;
            }
            "--from-source" => {
                if from_source {
                    return Err(CliError::DuplicateOption("--from-source"));
                }
                from_source = true;
                cursor += 1;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    let profile = DevProfile::parse(features.as_deref(), offline, from_source)
        .map_err(CliError::UnsupportedProfile)?;
    Ok(CliCommand::Dev {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        profile,
    })
}

fn parse_status(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    Ok(CliCommand::Status {
        root: parse_path_only(arguments)?,
    })
}

fn parse_stop(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    Ok(CliCommand::Stop {
        root: parse_path_only(arguments)?,
    })
}

fn parse_reset(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let mut root = None;
    let mut confirm = None;
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
            "--confirm" => {
                if confirm.is_some() {
                    return Err(CliError::DuplicateOption("--confirm"));
                }
                let Some(value) = arguments.get(cursor + 1).and_then(|value| value.to_str()) else {
                    return Err(CliError::MissingValue("--confirm"));
                };
                confirm = Some(value.to_owned());
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    Ok(CliCommand::Reset {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        confirm,
    })
}

fn parse_path_only(arguments: &[OsString]) -> Result<PathBuf, CliError> {
    match arguments {
        [] => Ok(PathBuf::from(".")),
        [flag, path] if flag == "--path" => Ok(PathBuf::from(path)),
        [flag] if flag == "--path" => Err(CliError::MissingValue("--path")),
        [flag] => Err(CliError::UnsupportedOption(lossy(flag))),
        _ => Err(CliError::Usage),
    }
}

fn parse_logs(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let mut root = None;
    let mut role = None;
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
            "--role" => {
                if role.is_some() {
                    return Err(CliError::DuplicateOption("--role"));
                }
                let Some(value) = arguments.get(cursor + 1).and_then(|value| value.to_str()) else {
                    return Err(CliError::MissingValue("--role"));
                };
                if !valid_runtime_role(value) {
                    return Err(CliError::UnsupportedOption(value.to_owned()));
                }
                role = Some(value.to_owned());
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    Ok(CliCommand::Logs {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        role,
    })
}

fn parse_operation(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let Some(action) = arguments.first().and_then(|value| value.to_str()) else {
        return Err(CliError::Usage);
    };
    if action != "wait" {
        return Err(CliError::UnknownCommand(format!("operation {action}")));
    }
    let Some(operation_id) = arguments.get(1).and_then(|value| value.to_str()) else {
        return Err(CliError::Usage);
    };
    let mut root = None;
    let mut timeout_seconds = None;
    let mut cursor = 2;
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
            "--timeout-seconds" => {
                if timeout_seconds.is_some() {
                    return Err(CliError::DuplicateOption("--timeout-seconds"));
                }
                let Some(value) = arguments.get(cursor + 1).and_then(|value| value.to_str()) else {
                    return Err(CliError::MissingValue("--timeout-seconds"));
                };
                timeout_seconds = Some(
                    value
                        .parse::<u64>()
                        .ok()
                        .filter(|value| (1..=3_600).contains(value))
                        .ok_or_else(|| CliError::InvalidOptionValue {
                            option: "--timeout-seconds",
                            value: value.to_owned(),
                        })?,
                );
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    Ok(CliCommand::OperationWait {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        operation_id: operation_id.to_owned(),
        timeout_seconds: timeout_seconds.unwrap_or(30),
    })
}

fn parse_apply(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let mut root = None;
    let mut file = None;
    let mut timeout_seconds = None;
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
            "--file" => {
                if file.is_some() {
                    return Err(CliError::DuplicateOption("--file"));
                }
                let Some(value) = arguments.get(cursor + 1) else {
                    return Err(CliError::MissingValue("--file"));
                };
                file = Some(PathBuf::from(value));
                cursor += 2;
            }
            "--timeout-seconds" => {
                if timeout_seconds.is_some() {
                    return Err(CliError::DuplicateOption("--timeout-seconds"));
                }
                let Some(value) = arguments.get(cursor + 1).and_then(|value| value.to_str()) else {
                    return Err(CliError::MissingValue("--timeout-seconds"));
                };
                timeout_seconds = Some(
                    value
                        .parse::<u64>()
                        .ok()
                        .filter(|value| (1..=3_600).contains(value))
                        .ok_or_else(|| CliError::InvalidOptionValue {
                            option: "--timeout-seconds",
                            value: value.to_owned(),
                        })?,
                );
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    Ok(CliCommand::Apply {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        file: file.ok_or(CliError::MissingValue("--file"))?,
        timeout_seconds: timeout_seconds.unwrap_or(30),
    })
}

fn parse_run(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let Some(action) = arguments.first().and_then(|value| value.to_str()) else {
        return Err(CliError::Usage);
    };
    if action == "create" {
        return parse_run_create(&arguments[1..]);
    }
    let Some(run_id) = arguments.get(1).and_then(|value| value.to_str()) else {
        return Err(CliError::Usage);
    };
    if action == "watch" {
        let (root, timeout_seconds) = parse_path_and_timeout(&arguments[2..], 300)?;
        return Ok(CliCommand::RunWatch {
            root,
            run_id: run_id.to_owned(),
            timeout_seconds,
        });
    }
    let root = parse_path_only(&arguments[2..])?;
    match action {
        "get" => Ok(CliCommand::RunGet {
            root,
            run_id: run_id.to_owned(),
        }),
        "result" => Ok(CliCommand::RunResult {
            root,
            run_id: run_id.to_owned(),
        }),
        "pause" | "resume" | "cancel" => Ok(CliCommand::RunControl {
            root,
            run_id: run_id.to_owned(),
            action: match action {
                "pause" => CliRunControlAction::Pause,
                "resume" => CliRunControlAction::Resume,
                "cancel" => CliRunControlAction::Cancel,
                _ => unreachable!(),
            },
        }),
        _ => Err(CliError::UnknownCommand(format!("run {action}"))),
    }
}

fn parse_task(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let Some(action) = arguments.first().and_then(|value| value.to_str()) else {
        return Err(CliError::Usage);
    };
    let Some(task_id) = arguments.get(1).and_then(|value| value.to_str()) else {
        return Err(CliError::Usage);
    };
    if action == "get" {
        return Ok(CliCommand::TaskGet {
            root: parse_path_only(&arguments[2..])?,
            task_id: task_id.to_owned(),
        });
    }
    let task_action = match action {
        "submit-input" => CliTaskAction::SubmitInput,
        "approve" => CliTaskAction::Approve,
        "reject" => CliTaskAction::Reject,
        "cancel" => CliTaskAction::Cancel,
        _ => return Err(CliError::UnknownCommand(format!("task {action}"))),
    };
    let mut root = None;
    let mut file = None;
    let mut cursor = 2;
    while cursor < arguments.len() {
        let flag = arguments[cursor].to_string_lossy();
        match flag.as_ref() {
            "--path" if root.is_none() => {
                let value = arguments
                    .get(cursor + 1)
                    .ok_or(CliError::MissingValue("--path"))?;
                root = Some(PathBuf::from(value));
            }
            "--file" if file.is_none() && task_action == CliTaskAction::SubmitInput => {
                let value = arguments
                    .get(cursor + 1)
                    .ok_or(CliError::MissingValue("--file"))?;
                file = Some(PathBuf::from(value));
            }
            "--path" => return Err(CliError::DuplicateOption("--path")),
            "--file" if task_action == CliTaskAction::SubmitInput => {
                return Err(CliError::DuplicateOption("--file"));
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
        cursor += 2;
    }
    if task_action == CliTaskAction::SubmitInput && file.is_none() {
        return Err(CliError::MissingValue("--file"));
    }
    Ok(CliCommand::TaskResolve {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        task_id: task_id.to_owned(),
        action: task_action,
        file,
    })
}

fn parse_path_and_timeout(
    arguments: &[OsString],
    default_timeout_seconds: u64,
) -> Result<(PathBuf, u64), CliError> {
    let mut root = None;
    let mut timeout_seconds = None;
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
            "--timeout-seconds" => {
                if timeout_seconds.is_some() {
                    return Err(CliError::DuplicateOption("--timeout-seconds"));
                }
                let Some(value) = arguments.get(cursor + 1).and_then(|value| value.to_str()) else {
                    return Err(CliError::MissingValue("--timeout-seconds"));
                };
                timeout_seconds = Some(
                    value
                        .parse::<u64>()
                        .ok()
                        .filter(|seconds| (1..=3_600).contains(seconds))
                        .ok_or_else(|| CliError::InvalidOptionValue {
                            option: "--timeout-seconds",
                            value: value.to_owned(),
                        })?,
                );
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    Ok((
        root.unwrap_or_else(|| PathBuf::from(".")),
        timeout_seconds.unwrap_or(default_timeout_seconds),
    ))
}

fn parse_run_create(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let mut root = None;
    let mut file = None;
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
            "--file" => {
                if file.is_some() {
                    return Err(CliError::DuplicateOption("--file"));
                }
                let Some(value) = arguments.get(cursor + 1) else {
                    return Err(CliError::MissingValue("--file"));
                };
                file = Some(PathBuf::from(value));
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    Ok(CliCommand::RunCreate {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        file: file.ok_or(CliError::MissingValue("--file"))?,
    })
}

fn parse_artifact(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let Some(action) = arguments.first().and_then(|value| value.to_str()) else {
        return Err(CliError::Usage);
    };
    if action == "upload" {
        return parse_artifact_upload(&arguments[1..]);
    }
    let Some(artifact_id) = arguments.get(1).and_then(|value| value.to_str()) else {
        return Err(CliError::Usage);
    };
    match action {
        "get" => Ok(CliCommand::ArtifactGet {
            root: parse_path_only(&arguments[2..])?,
            artifact_id: artifact_id.to_owned(),
        }),
        "read" => parse_artifact_read(artifact_id, &arguments[2..]),
        _ => Err(CliError::UnknownCommand(format!("artifact {action}"))),
    }
}

fn parse_artifact_upload(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let mut root = None;
    let mut file = None;
    let mut purpose = None;
    let mut classification = None;
    let mut media_type = None;
    let mut display_name = None;
    let mut timeout_seconds = None;
    let mut cursor = 0;
    while cursor < arguments.len() {
        let flag = arguments[cursor].to_string_lossy();
        let value = || {
            arguments
                .get(cursor + 1)
                .and_then(|value| value.to_str())
                .ok_or_else(|| {
                    CliError::MissingValue(match flag.as_ref() {
                        "--path" => "--path",
                        "--file" => "--file",
                        "--purpose" => "--purpose",
                        "--classification" => "--classification",
                        "--media-type" => "--media-type",
                        "--display-name" => "--display-name",
                        "--timeout-seconds" => "--timeout-seconds",
                        _ => "artifact upload option",
                    })
                })
        };
        match flag.as_ref() {
            "--path" if root.is_none() => root = Some(PathBuf::from(value()?)),
            "--file" if file.is_none() => file = Some(PathBuf::from(value()?)),
            "--purpose" if purpose.is_none() => purpose = Some(value()?.to_owned()),
            "--classification" if classification.is_none() => {
                classification = Some(value()?.to_owned())
            }
            "--media-type" if media_type.is_none() => media_type = Some(value()?.to_owned()),
            "--display-name" if display_name.is_none() => display_name = Some(value()?.to_owned()),
            "--timeout-seconds" if timeout_seconds.is_none() => {
                let raw = value()?;
                timeout_seconds = Some(
                    raw.parse::<u64>()
                        .ok()
                        .filter(|seconds| (1..=3_600).contains(seconds))
                        .ok_or_else(|| CliError::InvalidOptionValue {
                            option: "--timeout-seconds",
                            value: raw.to_owned(),
                        })?,
                );
            }
            "--path" => return Err(CliError::DuplicateOption("--path")),
            "--file" => return Err(CliError::DuplicateOption("--file")),
            "--purpose" => return Err(CliError::DuplicateOption("--purpose")),
            "--classification" => return Err(CliError::DuplicateOption("--classification")),
            "--media-type" => return Err(CliError::DuplicateOption("--media-type")),
            "--display-name" => return Err(CliError::DuplicateOption("--display-name")),
            "--timeout-seconds" => return Err(CliError::DuplicateOption("--timeout-seconds")),
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
        cursor += 2;
    }
    Ok(CliCommand::ArtifactUpload {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        file: file.ok_or(CliError::MissingValue("--file"))?,
        purpose: purpose.ok_or(CliError::MissingValue("--purpose"))?,
        classification: classification.ok_or(CliError::MissingValue("--classification"))?,
        media_type,
        display_name,
        timeout_seconds: timeout_seconds.unwrap_or(30),
    })
}

fn parse_artifact_read(artifact_id: &str, arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let mut root = None;
    let mut output = None;
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
            "--output" => {
                if output.is_some() {
                    return Err(CliError::DuplicateOption("--output"));
                }
                let Some(value) = arguments.get(cursor + 1) else {
                    return Err(CliError::MissingValue("--output"));
                };
                output = Some(PathBuf::from(value));
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    Ok(CliCommand::ArtifactRead {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        artifact_id: artifact_id.to_owned(),
        output: output.ok_or(CliError::MissingValue("--output"))?,
    })
}

fn valid_runtime_role(value: &str) -> bool {
    matches!(
        value,
        "gateway-management"
            | "gateway-runtime"
            | "artifact-gateway"
            | "artifact-data"
            | "orchestration"
            | "capability-native"
            | "registry-validation"
            | "context-native"
            | "security-authority"
            | "egress-broker"
            | "model-worker"
            | "context-remote"
            | "mcp-host"
            | "mcp-resource-host"
            | "capability-remote"
            | "mcp-discovery"
            | "mcp-subscription"
            | "mcp-cleanup"
            | "context-subscription"
            | "callback-api"
            | "context-dataset"
    )
}

#[derive(Default)]
struct AgentCommonOptions {
    root: Option<PathBuf>,
    output_mode: Option<agent::AgentOutputMode>,
    verbose: bool,
    debug_authority: bool,
}

impl AgentCommonOptions {
    fn parse_flag(&mut self, arguments: &[OsString], cursor: &mut usize) -> Result<bool, CliError> {
        let flag = arguments[*cursor].to_string_lossy();
        match flag.as_ref() {
            "--path" => {
                if self.root.is_some() {
                    return Err(CliError::DuplicateOption("--path"));
                }
                let value = arguments
                    .get(*cursor + 1)
                    .ok_or(CliError::MissingValue("--path"))?;
                self.root = Some(PathBuf::from(value));
                *cursor += 2;
                Ok(true)
            }
            "--output" => {
                if self.output_mode.is_some() {
                    return Err(CliError::DuplicateOption("--output"));
                }
                let value = arguments
                    .get(*cursor + 1)
                    .and_then(|value| value.to_str())
                    .ok_or(CliError::MissingValue("--output"))?;
                self.output_mode = Some(match value {
                    "text" => agent::AgentOutputMode::Text,
                    "json" => agent::AgentOutputMode::Json,
                    _ => {
                        return Err(CliError::InvalidOptionValue {
                            option: "--output",
                            value: value.to_owned(),
                        })
                    }
                });
                *cursor += 2;
                Ok(true)
            }
            "--verbose" => {
                if self.verbose {
                    return Err(CliError::DuplicateOption("--verbose"));
                }
                self.verbose = true;
                *cursor += 1;
                Ok(true)
            }
            "--debug-authority" => {
                if self.debug_authority {
                    return Err(CliError::DuplicateOption("--debug-authority"));
                }
                self.debug_authority = true;
                *cursor += 1;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn root(&self) -> PathBuf {
        self.root.clone().unwrap_or_else(|| PathBuf::from("."))
    }

    fn output(&self) -> agent::AgentOutputOptions {
        agent::AgentOutputOptions {
            mode: self.output_mode.unwrap_or(agent::AgentOutputMode::Text),
            verbose: self.verbose,
            debug_authority: self.debug_authority,
        }
    }
}

fn parse_agent(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let Some(action) = arguments.first().and_then(|value| value.to_str()) else {
        return Err(CliError::Usage);
    };
    match action {
        "validate" | "publish" => parse_agent_compile_command(action, &arguments[1..]),
        "list" => {
            let mut common = AgentCommonOptions::default();
            let mut cursor = 1;
            while cursor < arguments.len() {
                if !common.parse_flag(arguments, &mut cursor)? {
                    return Err(CliError::UnsupportedOption(lossy(&arguments[cursor])));
                }
            }
            Ok(CliCommand::AgentList {
                root: common.root(),
                output: common.output(),
            })
        }
        "get" | "result" | "logs" => {
            let selector = arguments
                .get(1)
                .and_then(|value| value.to_str())
                .ok_or(CliError::Usage)?
                .to_owned();
            let mut common = AgentCommonOptions::default();
            let mut follow = false;
            let mut cursor = 2;
            while cursor < arguments.len() {
                if common.parse_flag(arguments, &mut cursor)? {
                    continue;
                }
                if action == "logs" && arguments[cursor] == "--follow" {
                    if follow {
                        return Err(CliError::DuplicateOption("--follow"));
                    }
                    follow = true;
                    cursor += 1;
                    continue;
                }
                return Err(CliError::UnsupportedOption(lossy(&arguments[cursor])));
            }
            match action {
                "get" => Ok(CliCommand::AgentGet {
                    root: common.root(),
                    selector,
                    output: common.output(),
                }),
                "logs" => Ok(CliCommand::AgentLogs {
                    root: common.root(),
                    selector,
                    follow,
                    output: common.output(),
                }),
                "result" => Ok(CliCommand::AgentResult {
                    root: common.root(),
                    run_id: selector,
                    output: common.output(),
                }),
                _ => unreachable!(),
            }
        }
        "adopt" => parse_agent_adopt(&arguments[1..]),
        "run" => parse_agent_run(&arguments[1..]),
        _ => Err(CliError::UnknownCommand(format!("agent {action}"))),
    }
}

fn parse_agent_compile_command(
    action: &str,
    arguments: &[OsString],
) -> Result<CliCommand, CliError> {
    let mut common = AgentCommonOptions::default();
    let mut file = None;
    let mut online = false;
    let mut wait = true;
    let mut cursor = 0;
    while cursor < arguments.len() {
        if common.parse_flag(arguments, &mut cursor)? {
            continue;
        }
        let flag = arguments[cursor].to_string_lossy();
        match flag.as_ref() {
            "--file" => {
                if file.is_some() {
                    return Err(CliError::DuplicateOption("--file"));
                }
                file = Some(PathBuf::from(
                    arguments
                        .get(cursor + 1)
                        .ok_or(CliError::MissingValue("--file"))?,
                ));
                cursor += 2;
            }
            "--online" if action == "validate" => {
                if online {
                    return Err(CliError::DuplicateOption("--online"));
                }
                online = true;
                cursor += 1;
            }
            "--wait" if action == "publish" => {
                wait = true;
                cursor += 1;
            }
            "--wait=false" if action == "publish" => {
                wait = false;
                cursor += 1;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    let file = file.ok_or(CliError::MissingValue("--file"))?;
    if action == "validate" {
        Ok(CliCommand::AgentValidate {
            root: common.root(),
            file,
            online,
            output: common.output(),
        })
    } else {
        Ok(CliCommand::AgentPublish {
            root: common.root(),
            file,
            wait,
            output: common.output(),
        })
    }
}

fn parse_agent_adopt(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let name = arguments
        .first()
        .and_then(|value| value.to_str())
        .ok_or(CliError::Usage)?
        .to_owned();
    let mut common = AgentCommonOptions::default();
    let mut agent_id = None;
    let mut cursor = 1;
    while cursor < arguments.len() {
        if common.parse_flag(arguments, &mut cursor)? {
            continue;
        }
        if arguments[cursor] == "--agent-id" {
            if agent_id.is_some() {
                return Err(CliError::DuplicateOption("--agent-id"));
            }
            agent_id = Some(
                arguments
                    .get(cursor + 1)
                    .and_then(|value| value.to_str())
                    .ok_or(CliError::MissingValue("--agent-id"))?
                    .to_owned(),
            );
            cursor += 2;
        } else {
            return Err(CliError::UnsupportedOption(lossy(&arguments[cursor])));
        }
    }
    Ok(CliCommand::AgentAdopt {
        root: common.root(),
        name,
        agent_id: agent_id.ok_or(CliError::MissingValue("--agent-id"))?,
        output: common.output(),
    })
}

fn parse_agent_run(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let selector = arguments
        .first()
        .and_then(|value| value.to_str())
        .ok_or(CliError::Usage)?
        .to_owned();
    let mut common = AgentCommonOptions::default();
    let mut input = None;
    let mut file = None;
    let mut detach = false;
    let mut timeout_seconds = None;
    let mut cursor = 1;
    while cursor < arguments.len() {
        if common.parse_flag(arguments, &mut cursor)? {
            continue;
        }
        let flag = arguments[cursor].to_string_lossy();
        match flag.as_ref() {
            "--input" => {
                if input.is_some() {
                    return Err(CliError::DuplicateOption("--input"));
                }
                input = Some(
                    arguments
                        .get(cursor + 1)
                        .and_then(|value| value.to_str())
                        .ok_or(CliError::MissingValue("--input"))?
                        .to_owned(),
                );
                cursor += 2;
            }
            "--file" => {
                if file.is_some() {
                    return Err(CliError::DuplicateOption("--file"));
                }
                file = Some(PathBuf::from(
                    arguments
                        .get(cursor + 1)
                        .ok_or(CliError::MissingValue("--file"))?,
                ));
                cursor += 2;
            }
            "--detach" => {
                if detach {
                    return Err(CliError::DuplicateOption("--detach"));
                }
                detach = true;
                cursor += 1;
            }
            "--timeout-seconds" => {
                if timeout_seconds.is_some() {
                    return Err(CliError::DuplicateOption("--timeout-seconds"));
                }
                let value = arguments
                    .get(cursor + 1)
                    .and_then(|value| value.to_str())
                    .ok_or(CliError::MissingValue("--timeout-seconds"))?;
                timeout_seconds = Some(
                    value
                        .parse::<u64>()
                        .ok()
                        .filter(|seconds| (1..=3_600).contains(seconds))
                        .ok_or_else(|| CliError::InvalidOptionValue {
                            option: "--timeout-seconds",
                            value: value.to_owned(),
                        })?,
                );
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    if input.is_some() == file.is_some() {
        return Err(CliError::Usage);
    }
    Ok(CliCommand::AgentRun {
        root: common.root(),
        selector,
        input,
        file,
        detach,
        timeout_seconds: timeout_seconds.unwrap_or(300),
        output: common.output(),
    })
}

fn lossy(value: &OsString) -> String {
    value.to_string_lossy().into_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LocalProjectState {
    pub schema_version: u32,
    pub kind: String,
    pub project_name: String,
    pub created_at_unix_seconds: u64,
    pub identity: LocalIdentityState,
    pub profiles: BTreeMap<String, LocalProfileState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
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
    pub registry_validator_principal_id: String,
    pub egress_broker_principal_id: String,
    pub installation_principal_id: String,
    pub installation_request_id: String,
    pub bootstrap_config_digest: String,
    pub artifact_encryption_domain_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LocalProfileState {
    pub state: String,
    pub features: Vec<String>,
    pub profile_digest: Option<String>,
    pub release_identity: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RuntimeProfileState {
    schema_version: u32,
    kind: String,
    tenant_id: String,
    identity_digest: String,
    /// Exact release/source provenance currently selected for this project-local closure.
    /// Identity transitions are explicit and cannot be combined with feature-set changes.
    source_fingerprint: String,
    features: Vec<String>,
    profile_digest: String,
    release_identity: String,
    kms_key_arn: String,
    secret_provider_id: ResourceId,
    capability_protocol_profile_revision_id: ResourceId,
    secret_readiness_arn: String,
    s3_bucket: String,
    ports: RuntimePortBindings,
    config_digests: BTreeMap<String, String>,
    tls_identity_digests: BTreeMap<String, String>,
    closure_digest: String,
}

/// The loopback listeners assigned to one local profile.
///
/// These are persisted with the generated configuration so a stopped profile restarts on the
/// same endpoints.  They deliberately are not the well-known dependency ports (PostgreSQL/NATS),
/// which remain part of the fixed Docker development contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RuntimePortBindings {
    gateway_management: u16,
    gateway_runtime: u16,
    artifact_gateway: u16,
    artifact_gateway_observability: u16,
    artifact_data_controller: u16,
    artifact_data_observability: u16,
    orchestration_observability: u16,
    capability_native_observability: u16,
    registry_validation_observability: u16,
    full: full_profile::PortBindings,
}

impl RuntimePortBindings {
    fn allocate() -> Result<Self, CliError> {
        let mut listeners = Vec::with_capacity(28);
        let mut next = || -> Result<u16, CliError> {
            let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(|error| {
                CliError::RuntimeUnavailable(format!(
                    "reserve a loopback port for the local profile: {error}"
                ))
            })?;
            let port = listener
                .local_addr()
                .map_err(|error| {
                    CliError::RuntimeUnavailable(format!(
                        "read a reserved loopback port for the local profile: {error}"
                    ))
                })?
                .port();
            listeners.push(listener);
            Ok(port)
        };
        let ports = Self {
            gateway_management: next()?,
            gateway_runtime: next()?,
            artifact_gateway: next()?,
            artifact_gateway_observability: next()?,
            artifact_data_controller: next()?,
            artifact_data_observability: next()?,
            orchestration_observability: next()?,
            capability_native_observability: next()?,
            registry_validation_observability: next()?,
            full: full_profile::PortBindings::allocate(&mut next)?,
        };
        drop(listeners);
        Ok(ports)
    }

    fn all_ports(&self) -> [u16; 28] {
        [
            self.gateway_management,
            self.gateway_runtime,
            self.artifact_gateway,
            self.artifact_gateway_observability,
            self.artifact_data_controller,
            self.artifact_data_observability,
            self.orchestration_observability,
            self.capability_native_observability,
            self.registry_validation_observability,
            self.full.context_native_observability,
            self.full.artifact_maintenance_observability,
            self.full.security_authority,
            self.full.security_authority_observability,
            self.full.egress_broker,
            self.full.egress_broker_observability,
            self.full.model_worker_observability,
            self.full.remote_context_worker_observability,
            self.full.mcp_host,
            self.full.mcp_host_observability,
            self.full.mcp_resource_host,
            self.full.mcp_resource_host_observability,
            self.full.capability_remote_observability,
            self.full.mcp_discovery_observability,
            self.full.mcp_subscription_observability,
            self.full.mcp_cleanup_observability,
            self.full.context_subscription_observability,
            self.full.callback_api,
            self.full.context_dataset_observability,
        ]
    }

    #[cfg(test)]
    const fn static_test_ports() -> Self {
        Self {
            gateway_management: 8081,
            gateway_runtime: 8080,
            artifact_gateway: 18081,
            artifact_gateway_observability: 19090,
            artifact_data_controller: 19443,
            artifact_data_observability: 19091,
            orchestration_observability: 19092,
            capability_native_observability: 19093,
            registry_validation_observability: 19094,
            full: full_profile::PortBindings::static_test_ports(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RuntimeBuildState {
    schema_version: u32,
    source_fingerprint: String,
}

struct RuntimeProfileSelection<'a> {
    source_fingerprint: &'a str,
    ports: &'a RuntimePortBindings,
    selected_profile: DevProfile,
    release_identity: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RuntimeProcessState {
    schema_version: u32,
    kind: String,
    tenant_id: String,
    profile: String,
    profile_digest: String,
    release_identity: String,
    compose_project: String,
    source_fingerprint: String,
    lifecycle: RuntimeProcessLifecycle,
    processes: BTreeMap<String, RuntimeProcessRecord>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RuntimeProcessLifecycle {
    Starting,
    Running,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RuntimeProcessRecord {
    pid: u32,
    generation: String,
    ready_address: String,
    log_file: String,
}

struct RuntimeProcessBinding {
    tenant_id: String,
    profile: String,
    profile_digest: String,
    release_identity: String,
    compose_project: String,
    source_fingerprint: String,
    expected_processes: BTreeMap<String, String>,
}

#[derive(Debug)]
struct RuntimeLifecycleLock {
    _file: fs::File,
}

struct RuntimeRestartIdentity {
    release_identity: String,
    source_fingerprint: String,
}

#[derive(Clone, Copy)]
enum LocalTlsUsage {
    Server,
    Client,
}

#[derive(Clone, Copy)]
struct LocalTlsIdentitySpec {
    certificate: &'static str,
    private_key: &'static str,
    dns_names: &'static [&'static str],
    workload_identity: Option<&'static str>,
    usage: LocalTlsUsage,
}

struct ExpectedRuntimeClosure {
    config_files: BTreeMap<String, &'static str>,
    processes: BTreeMap<String, String>,
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
    fs::create_dir_all(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
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
        profiles: BTreeMap::from([(
            "starter".to_owned(),
            LocalProfileState {
                state: "not_provisioned".to_owned(),
                features: Vec::new(),
                profile_digest: None,
                release_identity: None,
            },
        )]),
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
    let registry_validator_principal_id = fresh_resource_id(ResourceKind::Principal).to_string();
    let egress_broker_principal_id = fresh_resource_id(ResourceKind::Principal).to_string();
    let installation_principal_id = fresh_resource_id(ResourceKind::Principal).to_string();
    let installation_request_id = fresh_resource_id(ResourceKind::ServerRequest).to_string();
    let artifact_encryption_domain_id =
        fresh_resource_id(ResourceKind::EncryptionDomain).to_string();
    let developer_subject = format!("developer:{issuer_nonce}");
    let registry_validator_subject = format!("registry-validator:{issuer_nonce}");
    let egress_broker_subject = format!("egress-broker:{issuer_nonce}");
    let installation_subject = format!("bootstrap:{issuer_nonce}");
    let authentication_authority_digest = tagged_digest(
        "oidc_authentication_authority_v1",
        &issuer,
        &identity_directory,
    )?;
    let developer_subject_digest =
        tagged_digest("oidc_subject_v1", &developer_subject, &identity_directory)?;
    let registry_validator_subject_digest = tagged_digest(
        "oidc_subject_v1",
        &registry_validator_subject,
        &identity_directory,
    )?;
    let egress_broker_subject_digest = tagged_digest(
        "oidc_subject_v1",
        &egress_broker_subject,
        &identity_directory,
    )?;
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
        "schema_version": 2,
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
        "registry_validator": {
            "principal_id": registry_validator_principal_id,
            "authentication_authority_digest": authentication_authority_digest,
            "subject_digest": registry_validator_subject_digest,
        },
        "egress_broker": {
            "principal_id": egress_broker_principal_id,
            "authentication_authority_digest": authentication_authority_digest,
            "subject_digest": egress_broker_subject_digest,
        },
    });
    let bootstrap_config_digest = canonical_digest(&bootstrap_config).map_err(|_| {
        invalid_local_identity(
            &identity_directory,
            "cannot canonicalize development bootstrap config",
        )
    })?;
    let identity = LocalIdentityState {
        schema_version: 3,
        issuer,
        audience: LOCAL_OIDC_AUDIENCE.to_owned(),
        key_id,
        jwks_digest,
        authentication_authority_digest,
        tenant_id,
        developer_principal_id,
        developer_subject,
        registry_validator_principal_id,
        egress_broker_principal_id,
        installation_principal_id,
        installation_request_id,
        bootstrap_config_digest,
        artifact_encryption_domain_id,
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

    let ca_params = local_runtime_ca_parameters(&tls_directory)?;
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
    write_sensitive_new(
        &tls_directory.join(RUNTIME_CA_PRIVATE_KEY_FILE),
        ca_key.serialize_pem().as_bytes(),
    )?;
    write_new(
        &tls_directory.join(RUNTIME_CA_CERTIFICATE_FILE),
        ca_certificate.pem().as_bytes(),
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
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        RUNTIME_NATS_SERVER_CERTIFICATE_FILE,
        RUNTIME_NATS_SERVER_PRIVATE_KEY_FILE,
        &["localhost"],
        None,
        ExtendedKeyUsagePurpose::ServerAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        RUNTIME_NATS_CLIENT_CERTIFICATE_FILE,
        RUNTIME_NATS_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some("spiffe://insight.platform/workload/local-nats-client"),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    Ok(())
}

fn local_runtime_ca_parameters(tls_directory: &Path) -> Result<CertificateParams, CliError> {
    let mut params = CertificateParams::new(Vec::<String>::new()).map_err(|_| {
        invalid_local_identity(
            tls_directory,
            "cannot construct local development certificate CA",
        )
    })?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    Ok(params)
}

fn read_local_tls_file(path: &Path, sensitive: bool) -> Result<Vec<u8>, String> {
    const MAX_LOCAL_TLS_FILE_BYTES: u64 = 65_536;
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
    if !metadata.file_type().is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_LOCAL_TLS_FILE_BYTES
    {
        return Err(format!("{} is not a bounded physical file", path.display()));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    let opened = file
        .metadata()
        .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
    if !opened.file_type().is_file()
        || opened.len() == 0
        || opened.len() > MAX_LOCAL_TLS_FILE_BYTES
        || {
            #[cfg(unix)]
            {
                opened.nlink() != 1
                    || opened.dev() != metadata.dev()
                    || opened.ino() != metadata.ino()
                    || (sensitive && opened.mode() & 0o077 != 0)
            }
            #[cfg(not(unix))]
            {
                false
            }
        }
    {
        return Err(format!(
            "{} is not a bounded private single-link file",
            path.display()
        ));
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    file.take(MAX_LOCAL_TLS_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    if bytes.len() as u64 > MAX_LOCAL_TLS_FILE_BYTES {
        return Err(format!("{} exceeds the TLS size limit", path.display()));
    }
    Ok(bytes)
}

fn validate_local_tls_directory(tls_directory: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(tls_directory)
        .map_err(|error| format!("cannot inspect {}: {error}", tls_directory.display()))?;
    if !metadata.file_type().is_dir() {
        return Err(format!(
            "{} is not a physical TLS directory",
            tls_directory.display()
        ));
    }
    Ok(())
}

fn local_tls_pair_digest(certificate: &[u8], private_key: &[u8]) -> Result<String, String> {
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "certificate_sha256": format!("sha256:{}", lower_hex(&Sha256::digest(certificate))),
        "private_key_sha256": format!("sha256:{}", lower_hex(&Sha256::digest(private_key))),
    }))
    .map_err(|error| error.to_string())
}

fn parse_single_x509_pem(bytes: &[u8], purpose: &str) -> Result<x509_parser::pem::Pem, String> {
    let (remaining, pem) =
        parse_x509_pem(bytes).map_err(|_| format!("{purpose} is not a PEM-encoded certificate"))?;
    if pem.label != "CERTIFICATE" || !remaining.iter().all(u8::is_ascii_whitespace) {
        return Err(format!("{purpose} is not one exact certificate"));
    }
    Ok(pem)
}

fn validate_local_tls_ca_pair(tls_directory: &Path) -> Result<String, String> {
    validate_local_tls_directory(tls_directory)?;
    let certificate_bytes =
        read_local_tls_file(&tls_directory.join(RUNTIME_CA_CERTIFICATE_FILE), false)?;
    let private_key_bytes =
        read_local_tls_file(&tls_directory.join(RUNTIME_CA_PRIVATE_KEY_FILE), true)?;
    let pem = parse_single_x509_pem(&certificate_bytes, "local TLS CA")?;
    let certificate = pem
        .parse_x509()
        .map_err(|_| "local TLS CA certificate is invalid".to_owned())?;
    let key = KeyPair::from_pem(
        std::str::from_utf8(&private_key_bytes)
            .map_err(|_| "local TLS CA key is not UTF-8 PEM".to_owned())?,
    )
    .map_err(|_| "local TLS CA key is invalid".to_owned())?;
    let basic = certificate
        .basic_constraints()
        .map_err(|_| "local TLS CA has invalid BasicConstraints".to_owned())?;
    let usage = certificate
        .key_usage()
        .map_err(|_| "local TLS CA has invalid KeyUsage".to_owned())?;
    if certificate.issuer() != certificate.subject()
        || !certificate.validity().is_valid()
        || !basic.is_some_and(|constraint| {
            constraint.value.ca && constraint.value.path_len_constraint.is_none()
        })
        || !usage.is_some_and(|usage| {
            usage.value.digital_signature()
                && usage.value.key_cert_sign()
                && usage.value.crl_sign()
                && usage.value.flags == 0b1100001
        })
        || key.subject_public_key_info().as_slice() != certificate.public_key().raw
        || certificate.verify_signature(None).is_err()
    {
        return Err("local TLS CA certificate/key authority is invalid".to_owned());
    }
    local_tls_pair_digest(&certificate_bytes, &private_key_bytes)
}

fn validate_local_tls_leaf_pair(
    tls_directory: &Path,
    spec: LocalTlsIdentitySpec,
) -> Result<String, String> {
    validate_local_tls_ca_pair(tls_directory)?;
    let ca_bytes = read_local_tls_file(&tls_directory.join(RUNTIME_CA_CERTIFICATE_FILE), false)?;
    let ca_pem = parse_single_x509_pem(&ca_bytes, "local TLS CA")?;
    let ca = ca_pem
        .parse_x509()
        .map_err(|_| "local TLS CA certificate is invalid".to_owned())?;
    let certificate_bytes = read_local_tls_file(&tls_directory.join(spec.certificate), false)?;
    let private_key_bytes = read_local_tls_file(&tls_directory.join(spec.private_key), true)?;
    let pem = parse_single_x509_pem(&certificate_bytes, spec.certificate)?;
    let certificate = pem
        .parse_x509()
        .map_err(|_| format!("{} is not a valid X.509 certificate", spec.certificate))?;
    let key = KeyPair::from_pem(
        std::str::from_utf8(&private_key_bytes)
            .map_err(|_| format!("{} is not UTF-8 PEM", spec.private_key))?,
    )
    .map_err(|_| format!("{} is not a valid private key", spec.private_key))?;
    let basic = certificate
        .basic_constraints()
        .map_err(|_| format!("{} has invalid BasicConstraints", spec.certificate))?;
    let key_usage = certificate
        .key_usage()
        .map_err(|_| format!("{} has invalid KeyUsage", spec.certificate))?;
    let extended = certificate
        .extended_key_usage()
        .map_err(|_| format!("{} has invalid ExtendedKeyUsage", spec.certificate))?
        .ok_or_else(|| format!("{} omits ExtendedKeyUsage", spec.certificate))?;
    let san = certificate
        .subject_alternative_name()
        .map_err(|_| format!("{} has invalid SubjectAlternativeName", spec.certificate))?
        .ok_or_else(|| format!("{} omits SubjectAlternativeName", spec.certificate))?;
    let actual_names = san
        .value
        .general_names
        .iter()
        .map(|name| match name {
            GeneralName::DNSName(value) => Ok(format!("dns:{value}")),
            GeneralName::URI(value) => Ok(format!("uri:{value}")),
            _ => Err(format!(
                "{} contains an unsupported subject alternative name",
                spec.certificate
            )),
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut expected_names = spec
        .dns_names
        .iter()
        .map(|name| format!("dns:{name}"))
        .collect::<BTreeSet<_>>();
    if let Some(identity) = spec.workload_identity {
        expected_names.insert(format!("uri:{identity}"));
    }
    let expected_server = matches!(spec.usage, LocalTlsUsage::Server);
    let expected_client = matches!(spec.usage, LocalTlsUsage::Client);
    if certificate.issuer() != ca.subject()
        || !certificate.validity().is_valid()
        || basic.is_some_and(|constraint| constraint.value.ca)
        || key_usage.is_none_or(|usage| usage.value.flags != 1)
        || extended.value.any
        || extended.value.server_auth != expected_server
        || extended.value.client_auth != expected_client
        || extended.value.code_signing
        || extended.value.email_protection
        || extended.value.time_stamping
        || extended.value.ocsp_signing
        || !extended.value.other.is_empty()
        || san.value.general_names.len() != expected_names.len()
        || actual_names != expected_names
        || key.subject_public_key_info().as_slice() != certificate.public_key().raw
        || certificate.verify_signature(Some(ca.public_key())).is_err()
    {
        return Err(format!(
            "{} does not match its key, CA, SAN, or EKU authority",
            spec.certificate
        ));
    }
    local_tls_pair_digest(&certificate_bytes, &private_key_bytes)
}

fn inspect_local_tls_identity_closure(
    tls_directory: &Path,
    selected_profile: DevProfile,
) -> Result<BTreeMap<String, String>, String> {
    validate_local_tls_directory(tls_directory)?;
    let mut digests = BTreeMap::from([(
        RUNTIME_CA_CERTIFICATE_FILE.to_owned(),
        validate_local_tls_ca_pair(tls_directory)?,
    )]);
    for spec in expected_local_tls_leaf_identities(selected_profile).values() {
        digests.insert(
            spec.certificate.to_owned(),
            validate_local_tls_leaf_pair(tls_directory, *spec)?,
        );
    }
    Ok(digests)
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
    write_sensitive_new(
        &tls_directory.join(private_key_name),
        key.serialize_pem().as_bytes(),
    )?;
    write_new(
        &tls_directory.join(certificate_name),
        certificate.pem().as_bytes(),
    )
}

fn local_tls_orphan_exists(path: &Path, sensitive: bool) -> Result<bool, CliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_file()
                && metadata.len() > 0
                && metadata.len() <= 65_536
                && {
                    #[cfg(unix)]
                    {
                        metadata.nlink() == 1 && (!sensitive || metadata.mode() & 0o077 == 0)
                    }
                    #[cfg(not(unix))]
                    {
                        true
                    }
                } =>
        {
            Ok(true)
        }
        Ok(_) => Err(invalid_local_identity(
            path,
            "uncommitted TLS residue is not a bounded physical single-link file",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        }),
    }
}

fn remove_local_tls_orphan(path: &Path, sensitive: bool) -> Result<(), CliError> {
    if local_tls_orphan_exists(path, sensitive)? {
        fs::remove_file(path).map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })?;
        sync_parent_directory(path)?;
    }
    Ok(())
}

fn ensure_local_leaf_certificate(
    tls_directory: &Path,
    spec: LocalTlsIdentitySpec,
    issuer: &Issuer<'_, KeyPair>,
    committed_digest: Option<&str>,
) -> Result<(), CliError> {
    let certificate = tls_directory.join(spec.certificate);
    let private_key = tls_directory.join(spec.private_key);
    let certificate_exists = local_tls_orphan_exists(&certificate, false)?;
    let private_key_exists = local_tls_orphan_exists(&private_key, true)?;
    let observed = (certificate_exists && private_key_exists)
        .then(|| validate_local_tls_leaf_pair(tls_directory, spec));
    if let Some(expected) = committed_digest {
        let observed = observed
            .ok_or_else(|| {
                CliError::RuntimeState(format!(
                    "committed TLS identity {:?} is incomplete",
                    spec.certificate
                ))
            })?
            .map_err(|reason| {
                CliError::RuntimeState(format!(
                    "committed TLS identity {:?} is invalid: {reason}",
                    spec.certificate
                ))
            })?;
        if observed != expected {
            return Err(CliError::RuntimeState(format!(
                "committed TLS identity {:?} drifted from the runtime profile",
                spec.certificate
            )));
        }
        return Ok(());
    }
    if observed.is_some_and(|result| result.is_ok()) {
        return Ok(());
    }
    remove_local_tls_orphan(&certificate, false)?;
    remove_local_tls_orphan(&private_key, true)?;
    write_local_leaf_certificate(
        tls_directory,
        spec.certificate,
        spec.private_key,
        spec.dns_names,
        spec.workload_identity,
        match spec.usage {
            LocalTlsUsage::Server => ExtendedKeyUsagePurpose::ServerAuth,
            LocalTlsUsage::Client => ExtendedKeyUsagePurpose::ClientAuth,
        },
        issuer,
    )?;
    validate_local_tls_leaf_pair(tls_directory, spec)
        .map(|_| ())
        .map_err(|reason| invalid_local_identity(tls_directory, &reason))
}

fn ensure_sensitive_random_file(path: &Path, value: &[u8]) -> Result<(), CliError> {
    if path.exists() {
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| invalid_local_identity(path, "local feature state path has no parent"))?;
    fs::create_dir_all(parent).map_err(|source| CliError::InitializeProject {
        path: parent.display().to_string(),
        source,
    })?;
    write_sensitive_new(path, value)
}

fn expected_local_tls_leaf_identities(
    selected_profile: DevProfile,
) -> BTreeMap<&'static str, LocalTlsIdentitySpec> {
    let mut identities = BTreeMap::from([
        (
            RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE,
                private_key: RUNTIME_ARTIFACT_GATEWAY_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
        ),
        (
            RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE,
                private_key: RUNTIME_ARTIFACT_DATA_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
        ),
        (
            RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE,
                private_key: RUNTIME_GATEWAY_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(PUBLIC_GATEWAY_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
        ),
        (
            RUNTIME_ORCHESTRATION_CLIENT_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_ORCHESTRATION_CLIENT_CERTIFICATE_FILE,
                private_key: RUNTIME_ORCHESTRATION_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(SCHEDULER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
        ),
        (
            RUNTIME_NATS_SERVER_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_NATS_SERVER_CERTIFICATE_FILE,
                private_key: RUNTIME_NATS_SERVER_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
        ),
        (
            RUNTIME_NATS_CLIENT_CERTIFICATE_FILE,
            LocalTlsIdentitySpec {
                certificate: RUNTIME_NATS_CLIENT_CERTIFICATE_FILE,
                private_key: RUNTIME_NATS_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some("spiffe://insight.platform/workload/local-nats-client"),
                usage: LocalTlsUsage::Client,
            },
        ),
    ]);
    let mut insert = |spec: LocalTlsIdentitySpec| {
        identities.insert(spec.certificate, spec);
    };
    if selected_profile.needs_egress() {
        for spec in [
            LocalTlsIdentitySpec {
                certificate: full_profile::SECURITY_AUTHORITY_CERTIFICATE_FILE,
                private_key: full_profile::SECURITY_AUTHORITY_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::EGRESS_BROKER_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::EGRESS_BROKER_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::EGRESS_BROKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::EGRESS_BROKER_CERTIFICATE_FILE,
                private_key: full_profile::EGRESS_BROKER_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
        ] {
            insert(spec);
        }
    }
    if selected_profile.has_model() {
        insert(LocalTlsIdentitySpec {
            certificate: full_profile::MODEL_WORKER_CLIENT_CERTIFICATE_FILE,
            private_key: full_profile::MODEL_WORKER_CLIENT_PRIVATE_KEY_FILE,
            dns_names: &[],
            workload_identity: Some(full_profile::MODEL_WORKER_WORKLOAD_IDENTITY),
            usage: LocalTlsUsage::Client,
        });
    }
    if selected_profile.has_context() {
        for spec in [
            LocalTlsIdentitySpec {
                certificate: full_profile::CONTEXT_WORKER_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::CONTEXT_WORKER_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::CONTEXT_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::CONTEXT_DATASET_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::CONTEXT_DATASET_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::CONTEXT_DATASET_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::CONTEXT_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::CONTEXT_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::CONTEXT_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_RESOURCE_HOST_CERTIFICATE_FILE,
                private_key: full_profile::MCP_RESOURCE_HOST_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_RESOURCE_EGRESS_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_RESOURCE_EGRESS_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_HOST_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
        ] {
            insert(spec);
        }
    }
    if selected_profile.has_remote_capability() {
        insert(LocalTlsIdentitySpec {
            certificate: full_profile::CAPABILITY_REMOTE_CLIENT_CERTIFICATE_FILE,
            private_key: full_profile::CAPABILITY_REMOTE_CLIENT_PRIVATE_KEY_FILE,
            dns_names: &[],
            workload_identity: Some(full_profile::CAPABILITY_WORKER_WORKLOAD_IDENTITY),
            usage: LocalTlsUsage::Client,
        });
    }
    if selected_profile.has_mcp() {
        for spec in [
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_HOST_CERTIFICATE_FILE,
                private_key: full_profile::MCP_HOST_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_RESOURCE_HOST_CERTIFICATE_FILE,
                private_key: full_profile::MCP_RESOURCE_HOST_PRIVATE_KEY_FILE,
                dns_names: &["localhost"],
                workload_identity: None,
                usage: LocalTlsUsage::Server,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_HOST_EGRESS_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_HOST_EGRESS_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_HOST_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_RESOURCE_EGRESS_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_RESOURCE_EGRESS_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_HOST_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_DISCOVERY_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_DISCOVERY_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::MCP_CLEANUP_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::MCP_CLEANUP_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
            LocalTlsIdentitySpec {
                certificate: full_profile::CALLBACK_CLIENT_CERTIFICATE_FILE,
                private_key: full_profile::CALLBACK_CLIENT_PRIVATE_KEY_FILE,
                dns_names: &[],
                workload_identity: Some(full_profile::MCP_CALLBACK_WORKLOAD_IDENTITY),
                usage: LocalTlsUsage::Client,
            },
        ] {
            insert(spec);
        }
    }
    identities
}

fn ensure_selected_feature_identity(
    state_directory: &Path,
    selected_profile: DevProfile,
    committed_digests: Option<&BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>, CliError> {
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    let tls_directory = runtime.join(RUNTIME_TLS_DIRECTORY);
    validate_local_tls_directory(&tls_directory)
        .map_err(|reason| invalid_local_identity(&tls_directory, &reason))?;
    let ca_digest = validate_local_tls_ca_pair(&tls_directory)
        .map_err(|reason| invalid_local_identity(&tls_directory, &reason))?;
    if let Some(expected) =
        committed_digests.and_then(|digests| digests.get(RUNTIME_CA_CERTIFICATE_FILE))
    {
        if &ca_digest != expected {
            return Err(CliError::RuntimeState(
                "committed TLS CA drifted from the runtime profile".to_owned(),
            ));
        }
    }
    if selected_profile.has_features() {
        let ca_key_path = tls_directory.join(RUNTIME_CA_PRIVATE_KEY_FILE);
        let ca_key_pem =
            fs::read_to_string(&ca_key_path).map_err(|source| CliError::InitializeProject {
                path: ca_key_path.display().to_string(),
                source,
            })?;
        let ca_key = KeyPair::from_pem(&ca_key_pem).map_err(|_| {
            invalid_local_identity(&ca_key_path, "local development CA key is invalid")
        })?;
        let issuer = Issuer::new(local_runtime_ca_parameters(&tls_directory)?, ca_key);

        for spec in expected_local_tls_leaf_identities(selected_profile).values() {
            ensure_local_leaf_certificate(
                &tls_directory,
                *spec,
                &issuer,
                committed_digests
                    .and_then(|digests| digests.get(spec.certificate))
                    .map(String::as_str),
            )?;
        }
    }
    if selected_profile.needs_egress() {
        ensure_sensitive_random_file(
            &runtime
                .join(full_profile::MCP_STATE_KEY_DIRECTORY)
                .join(full_profile::MCP_STATE_KEY_FILE),
            &Sha256::digest(Uuid::now_v7().as_bytes()),
        )?;
        ensure_sensitive_random_file(
            &runtime
                .join(full_profile::MCP_OAUTH_STATE_KEY_DIRECTORY)
                .join(full_profile::MCP_OAUTH_STATE_KEY_FILE),
            &Sha256::digest(Uuid::now_v7().as_bytes()),
        )?;
    }
    let observed = inspect_local_tls_identity_closure(&tls_directory, selected_profile)
        .map_err(|reason| invalid_local_identity(&tls_directory, &reason))?;
    if let Some(committed) = committed_digests {
        for (name, expected) in committed {
            if observed.get(name) != Some(expected) {
                return Err(CliError::RuntimeState(format!(
                    "committed TLS identity {name:?} changed during feature preparation"
                )));
            }
        }
    }
    Ok(observed)
}

/// Writes the closed, digest-bound configuration that the independent local Platform roles read.
/// The caller obtains `kms_key_arn` from the pinned local S3/KMS dependency; this function never
/// discovers storage state itself and does not connect to PostgreSQL or any internal RPC.
#[cfg(test)]
pub fn prepare_runtime_profile(
    root: &Path,
    kms_key_arn: &str,
    source_fingerprint: &str,
) -> Result<BTreeMap<String, String>, CliError> {
    prepare_runtime_profile_with_ports(
        root,
        kms_key_arn,
        LOCAL_TEST_SECRET_READINESS_ARN,
        source_fingerprint,
        &RuntimePortBindings::static_test_ports(),
        DevProfile::source_starter(),
        &format!("source:{source_fingerprint}"),
    )
}

fn prepare_runtime_profile_with_ports(
    root: &Path,
    kms_key_arn: &str,
    secret_readiness_arn: &str,
    source_fingerprint: &str,
    ports: &RuntimePortBindings,
    selected_profile: DevProfile,
    release_identity: &str,
) -> Result<BTreeMap<String, String>, CliError> {
    if !local_kms_key_arn_is_valid(kms_key_arn) || !runtime_ports_are_valid(ports) {
        return Err(CliError::InvalidLocalIdentity {
            path: root.join(PROJECT_DIRECTORY).display().to_string(),
        });
    }
    validate_local_secret_readiness_arn(root, secret_readiness_arn)?;
    let state_directory = root.join(PROJECT_DIRECTORY);
    let project = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity(&state_directory, &project.identity)?;
    let runtime_directory = state_directory.join(RUNTIME_DIRECTORY);
    let configuration_directory = runtime_directory.join(RUNTIME_CONFIGURATION_DIRECTORY);
    if configuration_directory.exists() {
        return Err(CliError::ProjectAlreadyInitialized(
            configuration_directory.display().to_string(),
        ));
    }
    fs::create_dir(&configuration_directory).map_err(|source| CliError::InitializeProject {
        path: configuration_directory.display().to_string(),
        source,
    })?;
    let result = prepare_runtime_profile_inner(
        &state_directory,
        &configuration_directory,
        &project.identity,
        kms_key_arn,
        secret_readiness_arn,
        RuntimeProfileSelection {
            source_fingerprint,
            ports,
            selected_profile,
            release_identity,
        },
    );
    if result.is_err() {
        let _ = fs::remove_dir_all(&configuration_directory);
    }
    result
}

fn prepare_runtime_profile_inner(
    state_directory: &Path,
    configuration_directory: &Path,
    identity: &LocalIdentityState,
    kms_key_arn: &str,
    secret_readiness_arn: &str,
    selection: RuntimeProfileSelection<'_>,
) -> Result<BTreeMap<String, String>, CliError> {
    let RuntimeProfileSelection {
        source_fingerprint,
        ports,
        selected_profile,
        release_identity,
    } = selection;
    let jwks_path = state_directory
        .join(IDENTITY_DIRECTORY)
        .join(IDENTITY_JWKS_FILE);
    let jwks: serde_json::Value = serde_json::from_slice(&read_bounded_identity_file(&jwks_path)?)
        .map_err(|_| CliError::InvalidLocalIdentity {
            path: state_directory.display().to_string(),
        })?;
    let catalog = local_artifact_provider_catalog(kms_key_arn)?;
    let secret_provider_id = fresh_resource_id(ResourceKind::SecretProvider);
    let capability_protocol_profile_revision_id = fresh_resource_id(ResourceKind::PolicyRevision);
    let secret_provider_catalog =
        local_secret_provider_catalog(kms_key_arn, secret_readiness_arn, &secret_provider_id)?;
    let scanner_contract_digest = local_digest("artifact-scanner-contract")?;
    let write_storage_binding_digest = catalog["write_storage_binding_digest"]
        .as_str()
        .ok_or_else(|| CliError::InvalidLocalIdentity {
            path: "local artifact provider configuration".to_owned(),
        })?
        .parse()
        .map_err(|_| CliError::InvalidLocalIdentity {
            path: "local artifact provider configuration".to_owned(),
        })?;
    let artifact_io_policy = SandboxArtifactIoPolicyDocument {
        schema_version: 3,
        allowed_input_media_types: vec![
            "application/json".to_owned(),
            "application/octet-stream".to_owned(),
            "application/wasm".to_owned(),
            "text/plain".to_owned(),
        ],
        allowed_output_media_types: vec![
            "application/json".to_owned(),
            "application/octet-stream".to_owned(),
            "application/wasm".to_owned(),
            "text/plain".to_owned(),
        ],
        maximum_input_artifacts: 64,
        maximum_output_artifacts: 64,
        scanner_contract_digest: scanner_contract_digest.parse().map_err(|_| {
            CliError::InvalidLocalIdentity {
                path: "local Artifact I/O policy".to_owned(),
            }
        })?,
        verification_evidence_ttl_milliseconds: 3_600_000,
        verification_retry_backoff_milliseconds: 250,
        write_storage_binding_digest,
        encryption_domain_id: identity
            .artifact_encryption_domain_id
            .parse()
            .map_err(|_| CliError::InvalidLocalIdentity {
                path: "local Artifact I/O policy".to_owned(),
            })?,
        deny_symlink: true,
        deny_hardlink: true,
        deny_device: true,
        deny_fifo: true,
        deny_socket: true,
        deny_sparse_file: true,
        archive_expansion_disabled: true,
    };
    artifact_io_policy
        .validate()
        .map_err(|_| CliError::InvalidLocalIdentity {
            path: "local Artifact I/O policy".to_owned(),
        })?;
    let scanner_ruleset_digest = artifact_io_policy
        .canonical_digest()
        .map_err(|_| CliError::InvalidLocalIdentity {
            path: "local Artifact I/O policy".to_owned(),
        })?
        .to_string();
    let retention_policy = ArtifactRetentionPolicy {
        version: 1,
        minimum_retention_seconds: 3_600,
        gc_grace_seconds: 86_400,
        tombstone_retention_seconds: 2_592_000,
        retain_provenance_sources: true,
        delete_requires_approval: false,
    };
    let scheduling_policy = SchedulingPolicyDocument {
        version: 1,
        weight: 1,
        burst: 2,
        aging_rounds: 2,
    };
    let orchestration_adapter_digest = local_digest("orchestration-worker")?;
    let registry_validator_digest = local_digest("registry-validator")?;
    let registry_validation_profile_digest = local_digest("registry-validation-profile")?;
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    let mut configs = BTreeMap::from([
        (
            "artifact-bootstrap".to_owned(),
            (
                RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "environment_class": "development",
                    "authoring_artifact_id": fresh_resource_id(ResourceKind::Artifact),
                    "authoring_blob_id": fresh_resource_id(ResourceKind::InternalBlob),
                    "retention_policy_id": fresh_resource_id(ResourceKind::Policy),
                    "retention_policy_revision_id": fresh_resource_id(ResourceKind::PolicyRevision),
                    "retention_policy_deployment_id": fresh_resource_id(ResourceKind::PolicyDeployment),
                    "artifact_io_policy_id": fresh_resource_id(ResourceKind::Policy),
                    "artifact_io_policy_revision_id": fresh_resource_id(ResourceKind::PolicyRevision),
                    "artifact_io_policy_deployment_id": fresh_resource_id(ResourceKind::PolicyDeployment),
                    "scheduling_policy_id": fresh_resource_id(ResourceKind::Policy),
                    "scheduling_policy_revision_id": fresh_resource_id(ResourceKind::PolicyRevision),
                    "scheduling_policy_deployment_id": fresh_resource_id(ResourceKind::PolicyDeployment),
                    "staging_quota_account_id": fresh_resource_id(ResourceKind::QuotaAccount),
                    "orchestration_quota_account_id": fresh_resource_id(ResourceKind::QuotaAccount),
                    "retention_policy": retention_policy,
                    "artifact_io_policy": artifact_io_policy,
                    "scheduling_policy": scheduling_policy,
                    "staging_quota_bytes": 67_108_864,
                    "orchestration_concurrent_jobs": 4,
                }),
            ),
        ),
        (
            "gateway-management".to_owned(),
            (
                RUNTIME_GATEWAY_MANAGEMENT_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "role": "management_api",
                    "listen_address": loopback_address(ports.gateway_management),
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                    "registry_validator_digest": registry_validator_digest,
                    "registry_validation_profile_digest": registry_validation_profile_digest,
                    "oidc": local_oidc_config(identity, jwks.clone()),
                }),
            ),
        ),
        (
            "gateway-runtime".to_owned(),
            (
                RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "role": "runtime_api",
                    "listen_address": loopback_address(ports.gateway_runtime),
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                    "registry_validator_digest": registry_validator_digest,
                    "registry_validation_profile_digest": registry_validation_profile_digest,
                    "oidc": local_oidc_config(identity, jwks.clone()),
                    "artifact_gateway": {"endpoint": https_endpoint(ports.artifact_gateway)},
                }),
            ),
        ),
        (
            "artifact-gateway".to_owned(),
            (
                RUNTIME_ARTIFACT_GATEWAY_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "listen_address": loopback_address(ports.artifact_gateway),
                    "observability_listen_address": loopback_address(ports.artifact_gateway_observability),
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "artifact_provider_catalog": catalog,
                    "write_encryption_domain_id": identity.artifact_encryption_domain_id,
                    "scanner_contract_digest": scanner_contract_digest,
                    "scan_evidence_ttl_milliseconds": 3600000,
                    "scan_retry_backoff_milliseconds": 250,
                    "finalize_batch_size": 32,
                    "finalize_poll_milliseconds": 100,
                    "maximum_upload_target_seconds": 300,
                    "maximum_download_bytes": 16777216,
                    "maximum_download_in_flight": 16,
                    "download_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                }),
            ),
        ),
        (
            "artifact-data".to_owned(),
            (
                RUNTIME_ARTIFACT_DATA_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "audience": "data_worker",
                    "controller_listen_address": loopback_address(ports.artifact_data_controller),
                    "observability_listen_address": loopback_address(ports.artifact_data_observability),
                    "read_database_max_connections": 4,
                    "work_database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "artifact_provider_catalog": catalog,
                    "broker": {
                        "maximum_in_flight": 16,
                        "maximum_read_bytes": 67108864,
                        "operation_timeout_milliseconds": 5000,
                    },
                    "rpc": {
                        "maximum_request_bytes": 1048576,
                        "maximum_write_request_bytes": 16777216,
                        "maximum_chunk_bytes": 262144,
                    },
                    "scan_worker": {
                        "scanner_contract_digest": scanner_contract_digest,
                        "ruleset_digest": scanner_ruleset_digest,
                        "claim_batch": 4,
                        "lease_milliseconds": 120000,
                        "receipt_ttl_milliseconds": 3600000,
                        "poll_milliseconds": 250,
                    },
                    "tls_handshake_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                }),
            ),
        ),
        (
            "orchestration".to_owned(),
            (
                RUNTIME_ORCHESTRATION_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "observability_listen_address": loopback_address(ports.orchestration_observability),
                    "worker_manifest": {
                        "manifest_version": 1,
                        "worker_role": "orchestration-worker",
                        "work_class": "orchestration",
                        "adapter_runtime_digest": orchestration_adapter_digest,
                        "protocol_version": 1,
                        "max_concurrency": 4,
                        "critical_control_reserved_slots": 1,
                    },
                    "database": {
                        "business_max_connections": 4,
                        "critical_control_reserved_connections": 2,
                        "process_connection_budget": 6,
                        "acquire_timeout_milliseconds": 5000,
                        "statement_timeout_milliseconds": 30000,
                        "idle_timeout_milliseconds": 60000,
                        "max_lifetime_milliseconds": 600000,
                    },
                    "artifact": {
                        "endpoint": https_endpoint(ports.artifact_data_controller),
                        "tls_server_name": "localhost",
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 5000,
                        "maximum_request_bytes": 1048576,
                        "maximum_chunk_bytes": 262144,
                    },
                    "timing": {
                        "coordinator_coalesce_milliseconds": 5,
                        "coordinator_scan_milliseconds": 500,
                        "coordinator_scan_jitter_milliseconds": 50,
                        "claim_failure_backoff_milliseconds": 100,
                        "drain_grace_milliseconds": 30000,
                        "heartbeat_jitter_milliseconds": 100,
                        "store_retry_backoff_milliseconds": 100,
                        "safety_scan_milliseconds": 500,
                        "safety_scan_jitter_milliseconds": 50,
                        "safety_failure_backoff_milliseconds": 100,
                        "handoff_retry_milliseconds": 100,
                    },
                    "plan_maximum_bytes": 1048576,
                    "safety_shard": {"index": 0, "count": 1},
                }),
            ),
        ),
        (
            "capability-native".to_owned(),
            (
                RUNTIME_CAPABILITY_NATIVE_CONFIG_FILE,
                local_capability_native_config(ports.capability_native_observability)?,
            ),
        ),
        (
            "registry-validation".to_owned(),
            (
                RUNTIME_REGISTRY_VALIDATION_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "observability_listen_address": loopback_address(ports.registry_validation_observability),
                    "worker_manifest": {
                        "manifest_version": 1,
                        "worker_role": "registry-validation-worker",
                        "work_class": "registry_validation",
                        "adapter_runtime_digest": registry_validator_digest,
                        "protocol_version": 1,
                        "max_concurrency": 2,
                        "critical_control_reserved_slots": 1,
                    },
                    "validator_principal_id": identity.registry_validator_principal_id,
                    "validator_digest": registry_validator_digest,
                    "validation_profile_digest": registry_validation_profile_digest,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "claim_batch": 2,
                    "lease_milliseconds": 30000,
                    "receipt_ttl_seconds": 300,
                    "scan_interval_milliseconds": 100,
                    "failure_backoff_milliseconds": 50,
                    "drain_grace_milliseconds": 5000,
                }),
            ),
        ),
    ]);
    let feature_configs = selected_feature_configs(
        &runtime,
        ports,
        identity,
        &catalog,
        &secret_provider_catalog,
        &capability_protocol_profile_revision_id,
        selected_profile,
    )?;
    configs.extend(feature_configs);
    let mut digests = BTreeMap::new();
    for (role, (file_name, config)) in configs {
        let digest = canonical_digest(&config).map_err(|_| CliError::InvalidLocalIdentity {
            path: configuration_directory.display().to_string(),
        })?;
        let bytes =
            serde_json::to_vec_pretty(&config).map_err(|error| CliError::InitializeProject {
                path: configuration_directory
                    .join(file_name)
                    .display()
                    .to_string(),
                source: std::io::Error::other(error),
            })?;
        write_new(&configuration_directory.join(file_name), &bytes)?;
        digests.insert(role, digest);
    }
    let cursor_key = Sha256::digest(Uuid::now_v7().as_bytes());
    write_sensitive_new(&runtime.join(RUNTIME_CURSOR_KEY_FILE), &cursor_key)?;
    let identity_digest =
        local_identity_digest(identity).map_err(|_| CliError::InvalidLocalIdentity {
            path: state_directory.display().to_string(),
        })?;
    let tls_identity_digests =
        inspect_local_tls_identity_closure(&runtime.join(RUNTIME_TLS_DIRECTORY), selected_profile)
            .map_err(|reason| {
                invalid_local_identity(&runtime.join(RUNTIME_TLS_DIRECTORY), &reason)
            })?;
    let mut profile = RuntimeProfileState {
        schema_version: RUNTIME_PROFILE_SCHEMA_VERSION,
        kind: RUNTIME_PROFILE_KIND.to_owned(),
        tenant_id: identity.tenant_id.clone(),
        identity_digest,
        source_fingerprint: source_fingerprint.to_owned(),
        features: selected_profile
            .feature_names()
            .into_iter()
            .map(str::to_owned)
            .collect(),
        profile_digest: selected_profile
            .profile_digest(release_identity)
            .map_err(CliError::RuntimeState)?,
        release_identity: release_identity.to_owned(),
        kms_key_arn: kms_key_arn.to_owned(),
        secret_provider_id,
        capability_protocol_profile_revision_id,
        secret_readiness_arn: secret_readiness_arn.to_owned(),
        s3_bucket: LOCAL_ARTIFACT_BUCKET.to_owned(),
        ports: ports.clone(),
        config_digests: digests.clone(),
        tls_identity_digests,
        closure_digest: String::new(),
    };
    refresh_runtime_profile_closure_digest(&mut profile)?;
    let profile_bytes =
        serde_json::to_vec_pretty(&profile).map_err(|error| CliError::InitializeProject {
            path: runtime
                .join(RUNTIME_PROFILE_STATE_FILE)
                .display()
                .to_string(),
            source: std::io::Error::other(error),
        })?;
    write_new(&runtime.join(RUNTIME_PROFILE_STATE_FILE), &profile_bytes)?;
    Ok(digests)
}

fn selected_feature_configs(
    runtime: &Path,
    ports: &RuntimePortBindings,
    identity: &LocalIdentityState,
    artifact_provider_catalog: &serde_json::Value,
    secret_provider_catalog: &serde_json::Value,
    capability_protocol_profile_revision_id: &ResourceId,
    selected_profile: DevProfile,
) -> Result<BTreeMap<String, (&'static str, serde_json::Value)>, CliError> {
    let mcp_state_key_root = runtime.join(full_profile::MCP_STATE_KEY_DIRECTORY);
    let mcp_state_key_path = mcp_state_key_root.join(full_profile::MCP_STATE_KEY_FILE);
    let mcp_oauth_state_key_root = runtime.join(full_profile::MCP_OAUTH_STATE_KEY_DIRECTORY);
    let mcp_oauth_state_key_path =
        mcp_oauth_state_key_root.join(full_profile::MCP_OAUTH_STATE_KEY_FILE);
    let (mcp_state_key_reference_digest, mcp_oauth_state_key_reference_digest) =
        if selected_profile.needs_egress() {
            (
                format!(
                    "sha256:{}",
                    lower_hex(&Sha256::digest(read_bounded_identity_file(
                        &mcp_state_key_path
                    )?))
                ),
                format!(
                    "sha256:{}",
                    lower_hex(&Sha256::digest(read_bounded_identity_file(
                        &mcp_oauth_state_key_path
                    )?))
                ),
            )
        } else {
            (
                local_digest("unused-mcp-state-key")?,
                local_digest("unused-mcp-oauth-state-key")?,
            )
        };
    let mut configs = full_profile::initial_configs(
        &ports.full,
        artifact_provider_catalog,
        capability_protocol_profile_revision_id,
        full_profile::WorkerDigests {
            context_adapter: &local_digest("context-native-adapter")?,
            context_contract: &local_digest("context-native-contract")?,
            model_adapter: &local_digest("model-worker-adapter")?,
            anthropic_contract: &local_digest("model-anthropic-contract")?,
            openai_contract: &local_digest("model-openai-contract")?,
        },
        full_profile::EgressConfigInputs {
            service_principal_id: &identity.egress_broker_principal_id,
            secret_provider_catalog,
            mcp_state_key_root: &mcp_state_key_root,
            mcp_state_key_path: &mcp_state_key_path,
            mcp_state_key_reference_digest: &mcp_state_key_reference_digest,
            mcp_oauth_state_key_root: &mcp_oauth_state_key_root,
            mcp_oauth_state_key_path: &mcp_oauth_state_key_path,
            mcp_oauth_state_key_reference_digest: &mcp_oauth_state_key_reference_digest,
            artifact_data_worker_port: ports.artifact_data_controller,
        },
    );
    configs.retain(|role, _| selected_profile.includes_role(role));
    if selected_profile.has_sandbox() {
        configs.insert(
            "sandbox-kubernetes".to_owned(),
            (
                RUNTIME_SANDBOX_KUBERNETES_CONFIG_FILE,
                sandbox_kubernetes_config(),
            ),
        );
    }
    Ok(configs)
}

fn sandbox_kubernetes_config() -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "kind": "insight.dev.sandbox-kubernetes/v1",
        "environment_class": "single_node_development",
        "production_qualification": false,
        "provider": "opensandbox_kubernetes_batchsandbox",
        "source": {
            "repository": "https://github.com/opensandbox-group/OpenSandbox",
            "commit": OPENSANDBOX_SOURCE_COMMIT,
        },
        "images": {
            "server": OPENSANDBOX_SERVER_IMAGE_DIGEST,
            "controller": OPENSANDBOX_CONTROLLER_IMAGE_DIGEST,
            "execd": OPENSANDBOX_EXECD_IMAGE_DIGEST,
        },
        "runtime_contract": {
            "lifecycle_schema_digest": "sha256:97dbdff2dbbf571c8e3ac77acd24454bc774275478decc1990918cbf7c518351",
            "batchsandbox_crd_digest": "sha256:6a56fbec00a33acf30a4a9c3418172ad6ac1eba34d081881e6b5dd941cfa59d4",
            "kubernetes_provider_template_digest": "sha256:be829c7a936867d7aff62bf76d5e897b75c65628563ad2d354f4ccb36b30cc4c",
            "runner_protocol_digest": "sha256:b5eac79a3b4f66179341408d811ccfd82978279facbf30f503eb504be0638a4b",
            "container_runtime_digest": "sha256:b2c15f4bdafaa05a3a60919a4e9ea9825cc03f8a09a63a2b603c3c42a7d849cc",
            "network_policy_digest": "sha256:2bc456ef5f8427de8b142de9347d030fec638078dd11df111bc05ef85110e66e",
        },
        "namespaces": {
            "control": "platform-sandbox",
            "workloads": "platform-sandbox-workloads",
        },
        "components": [
            "sandbox-dispatcher",
            "opensandbox-server",
            "opensandbox-controller",
            "sandbox-runner",
        ],
        "network": {
            "default": "direct",
            "supported": ["direct", "disabled"],
            "policies": ["armed-runner-direct", "armed-runner-disabled", "armed-runner-ingress"],
        },
        "qualification": {"L4": "not_run", "L5": "not_run", "L6": "not_run"},
    })
}

fn append_selected_feature_configs(
    state_directory: &Path,
    identity: &LocalIdentityState,
    state: &mut RuntimeProfileState,
    selected_profile: DevProfile,
) -> Result<(), CliError> {
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    let configuration = runtime.join(RUNTIME_CONFIGURATION_DIRECTORY);
    let artifact_catalog = local_artifact_provider_catalog(&state.kms_key_arn)?;
    let secret_catalog = local_secret_provider_catalog(
        &state.kms_key_arn,
        &state.secret_readiness_arn,
        &state.secret_provider_id,
    )?;
    let configs = selected_feature_configs(
        &runtime,
        &state.ports,
        identity,
        &artifact_catalog,
        &secret_catalog,
        &state.capability_protocol_profile_revision_id,
        selected_profile,
    )?;
    for (role, (file_name, config)) in configs {
        let digest = canonical_digest(&config).map_err(|_| CliError::InvalidLocalIdentity {
            path: configuration.display().to_string(),
        })?;
        let path = configuration.join(file_name);
        if let Some(expected) = state.config_digests.get(&role) {
            let existing = fs::read(&path).map_err(|source| CliError::InitializeProject {
                path: path.display().to_string(),
                source,
            })?;
            let existing: serde_json::Value =
                serde_json::from_slice(&existing).map_err(|error| {
                    CliError::RuntimeState(format!(
                        "persisted config {} is invalid: {error}",
                        path.display()
                    ))
                })?;
            let observed = canonical_digest(&existing).map_err(|_| {
                CliError::RuntimeState(format!(
                    "persisted config {} is not canonical JSON",
                    path.display()
                ))
            })?;
            if &observed != expected {
                return Err(CliError::RuntimeState(format!(
                    "persisted config {} drifted from profile authority",
                    path.display()
                )));
            }
        } else {
            let bytes = serde_json::to_vec_pretty(&config).map_err(|error| {
                CliError::RuntimeState(format!("cannot serialize feature config {role}: {error}"))
            })?;
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    let invalid = |detail: &str| {
                        CliError::RuntimeState(format!(
                            "cannot recover additive feature config {}: {detail}",
                            path.display()
                        ))
                    };
                    let observed = canonical_runtime_config_digest(&path, &invalid)?;
                    if observed != digest {
                        return Err(CliError::RuntimeState(format!(
                            "persisted additive feature config {} does not match the generated closure",
                            path.display()
                        )));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    write_new(&path, &bytes)?;
                }
                Err(source) => {
                    return Err(CliError::InitializeProject {
                        path: path.display().to_string(),
                        source,
                    });
                }
            }
            state.config_digests.insert(role.clone(), digest);
        }
    }
    Ok(())
}

fn local_kms_key_arn_is_valid(value: &str) -> bool {
    const PREFIX: &str = "arn:aws:kms:us-east-1:000000000000:key/";
    value
        .strip_prefix(PREFIX)
        .and_then(|suffix| Uuid::parse_str(suffix).ok().map(|uuid| (suffix, uuid)))
        .is_some_and(|(suffix, uuid)| suffix == uuid.hyphenated().to_string())
}

fn runtime_ports_are_valid(ports: &RuntimePortBindings) -> bool {
    let values = ports.all_ports();
    values.iter().all(|port| *port != 0)
        && values.into_iter().collect::<BTreeSet<_>>().len() == values.len()
}

fn local_oidc_config(identity: &LocalIdentityState, jwks: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "issuer": identity.issuer,
        "audience": identity.audience,
        "jwks_digest": identity.jwks_digest,
        "jwks": jwks,
    })
}

fn local_artifact_provider_catalog(kms_key_arn: &str) -> Result<serde_json::Value, CliError> {
    let kms_binding = serde_json::json!({
        "connect_timeout_milliseconds": 5000,
        "endpoint": LOCAL_AWS_ENDPOINT,
        "key_id": kms_key_arn,
        "operation_timeout_milliseconds": 30000,
        "provider": "aws_kms",
        "region": "us-east-1",
        "schema_version": 1,
    });
    let kms_binding_digest =
        canonical_digest(&kms_binding).map_err(|_| CliError::InvalidLocalIdentity {
            path: "local artifact provider configuration".to_owned(),
        })?;
    let storage_binding = serde_json::json!({
        "backend": "s3",
        "bucket": LOCAL_ARTIFACT_BUCKET,
        "connect_timeout_milliseconds": 5000,
        "endpoint": LOCAL_AWS_ENDPOINT,
        "force_path_style": true,
        "kms_binding_digest": kms_binding_digest,
        "maximum_object_bytes": 67108864,
        "operation_timeout_milliseconds": 30000,
        "region": "us-east-1",
        "schema_version": 1,
    });
    let storage_binding_digest =
        canonical_digest(&storage_binding).map_err(|_| CliError::InvalidLocalIdentity {
            path: "local artifact provider configuration".to_owned(),
        })?;
    Ok(serde_json::json!({
        "schema_version": 1,
        "write_storage_binding_digest": storage_binding_digest,
        "s3_storage_bindings": [{
            "schema_version": 1,
            "storage_binding_digest": storage_binding_digest,
            "endpoint": LOCAL_AWS_ENDPOINT,
            "region": "us-east-1",
            "bucket": LOCAL_ARTIFACT_BUCKET,
            "force_path_style": true,
            "kms_binding_digest": kms_binding_digest,
            "connect_timeout_milliseconds": 5000,
            "operation_timeout_milliseconds": 30000,
            "maximum_object_bytes": 67108864,
        }],
        "kms_key_bindings": [{
            "schema_version": 1,
            "kms_binding_digest": kms_binding_digest,
            "endpoint": LOCAL_AWS_ENDPOINT,
            "region": "us-east-1",
            "key_id": kms_key_arn,
            "connect_timeout_milliseconds": 5000,
            "operation_timeout_milliseconds": 30000,
        }],
    }))
}

fn validate_local_secret_readiness_arn(root: &Path, value: &str) -> Result<(), CliError> {
    if local_secret_readiness_arn_is_valid(value) {
        Ok(())
    } else {
        Err(CliError::InvalidLocalIdentity {
            path: root.join(PROJECT_DIRECTORY).display().to_string(),
        })
    }
}

fn local_secret_readiness_arn_is_valid(value: &str) -> bool {
    const PREFIX: &str =
        "arn:aws:secretsmanager:us-east-1:000000000000:secret:insight/platform/readiness-";
    value.strip_prefix(PREFIX).is_some_and(|suffix| {
        suffix.len() == 6 && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })
}

fn local_secret_provider_catalog(
    kms_key_arn: &str,
    readiness_secret_arn: &str,
    provider_id: &ResourceId,
) -> Result<serde_json::Value, CliError> {
    if provider_id.kind() != ResourceKind::SecretProvider {
        return Err(CliError::InvalidLocalIdentity {
            path: "local Secret provider configuration".to_owned(),
        });
    }
    let (authority, _) = readiness_secret_arn.split_once(":secret:").ok_or_else(|| {
        CliError::InvalidLocalIdentity {
            path: "local Secret provider configuration".to_owned(),
        }
    })?;
    let mut provider = serde_json::json!({
        "schema_version": 1,
        "provider_id": provider_id,
        "region": "us-east-1",
        "secrets_endpoint": LOCAL_AWS_ENDPOINT,
        "kms_endpoint": LOCAL_AWS_ENDPOINT,
        "kms_key_arn": kms_key_arn,
        "secret_arn_prefix": format!("{authority}:secret:insight/platform/"),
        "secret_name_prefix": LOCAL_SECRET_NAME_PREFIX,
        "readiness_secret_id": readiness_secret_arn,
        "connect_timeout_milliseconds": 5000,
        "operation_timeout_milliseconds": 30000,
    });
    let digest = canonical_digest(&provider).map_err(|_| CliError::InvalidLocalIdentity {
        path: "local Secret provider configuration".to_owned(),
    })?;
    provider
        .as_object_mut()
        .expect("local provider configuration is an object")
        .insert(
            "provider_config_digest".to_owned(),
            serde_json::Value::String(digest),
        );
    Ok(serde_json::json!({
        "schema_version": 1,
        "providers": [provider],
    }))
}

fn loopback_address(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

fn https_endpoint(port: u16) -> String {
    format!("https://localhost:{port}/")
}

fn local_capability_native_config(observability_port: u16) -> Result<serde_json::Value, CliError> {
    let module_digest = builtin_capability_digest("builtin-echo-module");
    let adapters = serde_json::json!([{
        "adapter_id": "builtin.echo",
        "adapter_version": "1.0.0",
        "module_digest": module_digest,
        "entrypoint_id": "echo.inline",
    }]);
    let adapter_runtime_digest =
        canonical_digest(&adapters).map_err(|_| CliError::InvalidLocalIdentity {
            path: "local native capability configuration".to_owned(),
        })?;
    Ok(serde_json::json!({
        "schema_version": 1,
        "observability_listen_address": loopback_address(observability_port),
        "worker_manifest": {
            "manifest_version": 1,
            "worker_role": "capability.native",
            "work_class": "capability_native",
            "adapter_runtime_digest": adapter_runtime_digest,
            "protocol_version": 1,
            "max_concurrency": 4,
            "critical_control_reserved_slots": 1,
        },
        "installed_adapters": adapters,
        "database": {
            "business_max_connections": 4,
            "critical_control_max_connections": 2,
            "process_connection_budget": 6,
            "acquire_timeout_milliseconds": 5000,
        },
        "timing": {
            "initial_scan_delay_milliseconds": 0,
            "receipt_ttl_milliseconds": 60000,
            "safety_scan_milliseconds": 100,
            "claim_failure_backoff_milliseconds": 50,
            "drain_grace_milliseconds": 5000,
        },
    }))
}

fn builtin_capability_digest(domain: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"insight.platform/v1/capability-worker/builtin\0");
    hasher.update(domain.as_bytes());
    format!("sha256:{}", lower_hex(&hasher.finalize()))
}

fn local_digest(kind: &str) -> Result<String, CliError> {
    canonical_digest(&serde_json::json!({"schema_version": 1, "kind": kind})).map_err(|_| {
        CliError::InvalidLocalIdentity {
            path: "local development configuration".to_owned(),
        }
    })
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn validate_runtime_profile_transition(
    current: &RuntimeProfileState,
    requested: DevProfile,
    requested_release_identity: &str,
    requested_source_fingerprint: &str,
) -> Result<(), CliError> {
    let requested_features = requested
        .feature_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let identity_changed = current.release_identity != requested_release_identity
        || current.source_fingerprint != requested_source_fingerprint;
    if identity_changed {
        if current.features != requested_features {
            return Err(CliError::RuntimeState(
                "an exact release/source transition cannot add or remove development features in the same `insight dev`; first run the new verified release/source with the currently persisted feature set, then run `insight dev` again to add features"
                    .to_owned(),
            ));
        }
        return Ok(());
    }
    if !requested.is_additive_from(&current.features) {
        return Err(CliError::RuntimeState(format!(
            "removing development features is not implicit; stop the profile and use reset after reviewing project-local data (current: {}, requested: {})",
            current.features.join(","),
            requested.feature_names().join(",")
        )));
    }
    Ok(())
}

fn clear_quiescent_runtime_process_state(
    runtime: &Path,
    binding: &RuntimeProcessBinding,
) -> Result<(), CliError> {
    let Some(state) = read_runtime_process_state(runtime, binding)? else {
        return Ok(());
    };
    for process in state.processes.values() {
        match observe_runtime_process(process)? {
            RuntimeProcessObservation::Owned => {
                let detail = if state.lifecycle == RuntimeProcessLifecycle::Starting {
                    "the previous start journal is incomplete and still owns a live Platform process; run `insight stop` to recover it"
                } else {
                    "a local Platform process is already running; use `insight status` or `insight stop`"
                };
                return Err(CliError::RuntimeUnavailable(detail.to_owned()));
            }
            RuntimeProcessObservation::IdentityMismatch => {
                // The numeric PID is live but is not proven to be the recorded Platform process.
                // Never signal it; the exact journal record is stale and may be discarded once
                // every still-owned record has been reconciled.
            }
            RuntimeProcessObservation::Stopped => {}
        }
    }
    remove_runtime_state_file(&runtime.join(RUNTIME_PROCESS_STATE_FILE))
}

fn acquire_runtime_lifecycle_lock(root: &Path) -> Result<RuntimeLifecycleLock, CliError> {
    #[cfg(unix)]
    {
        let state_directory = root.join(PROJECT_DIRECTORY);
        let runtime = state_directory.join(RUNTIME_DIRECTORY);
        for directory in [&state_directory, &runtime] {
            let metadata =
                fs::symlink_metadata(directory).map_err(|source| CliError::InitializeProject {
                    path: directory.display().to_string(),
                    source,
                })?;
            if !metadata.file_type().is_dir() {
                return Err(CliError::RuntimeState(format!(
                    "{} is not a physical project-local directory",
                    directory.display()
                )));
            }
        }
        let path = runtime.join(RUNTIME_LIFECYCLE_LOCK_FILE);
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let mut file = options
            .open(&path)
            .map_err(|source| CliError::InitializeProject {
                path: path.display().to_string(),
                source,
            })?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Err(CliError::RuntimeUnavailable(
                    "another local lifecycle command is already mutating this project; retry after it completes"
                        .to_owned(),
                ));
            }
            return Err(CliError::InitializeProject {
                path: path.display().to_string(),
                source: error,
            });
        }
        let opened = file
            .metadata()
            .map_err(|source| CliError::InitializeProject {
                path: path.display().to_string(),
                source,
            })?;
        let current =
            fs::symlink_metadata(&path).map_err(|source| CliError::InitializeProject {
                path: path.display().to_string(),
                source,
            })?;
        if !opened.file_type().is_file()
            || opened.nlink() != 1
            || opened.dev() != current.dev()
            || opened.ino() != current.ino()
            || opened.mode() & 0o077 != 0
        {
            return Err(CliError::RuntimeState(format!(
                "{} is not the private single-link lifecycle lock file",
                path.display()
            )));
        }
        let owner = format!("schema_version=2 pid={}\n", std::process::id());
        file.set_len(0)
            .and_then(|_| file.write_all(owner.as_bytes()))
            .and_then(|_| file.sync_all())
            .map_err(|source| CliError::InitializeProject {
                path: path.display().to_string(),
                source,
            })?;
        sync_parent_directory(&path)?;
        Ok(RuntimeLifecycleLock { _file: file })
    }
    #[cfg(not(unix))]
    {
        let _ = root;
        Err(CliError::RuntimeUnavailable(
            "local lifecycle locking is currently supported on Unix hosts".to_owned(),
        ))
    }
}

fn run_development_profile(
    workspace: &Path,
    root: &Path,
    profile: DevProfile,
) -> Result<String, CliError> {
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let _lock = acquire_runtime_lifecycle_lock(&root)?;
    run_development_profile_locked(workspace, &root, profile, None)
}

fn run_development_profile_locked(
    workspace: &Path,
    root: &Path,
    profile: DevProfile,
    restart_identity: Option<&RuntimeRestartIdentity>,
) -> Result<String, CliError> {
    let state_directory = root.join(PROJECT_DIRECTORY);
    let mut project = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity(&state_directory, &project.identity)?;
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    ensure_embedded_compose(&runtime)?;
    let compose_project = compose_project_name(&project.identity.tenant_id)?;
    let (workspace, fingerprint, release_identity, verified_release) = if profile.is_from_source() {
        let workspace = workspace_root(workspace)?;
        let fingerprint = workspace_fingerprint(&workspace)?;
        let identity = format!("source:{fingerprint}");
        (workspace, fingerprint, identity, None)
    } else {
        let release =
            release::load_current_release(&state_directory.join("cache"), profile.offline())
                .map_err(CliError::Release)?;
        let identity = format!("release:{}:{}", release.version, release.bundle_digest);
        (
            root.to_path_buf(),
            release.bundle_digest.clone(),
            identity,
            Some(release),
        )
    };
    let expected_profile_digest = profile
        .profile_digest(&release_identity)
        .map_err(CliError::RuntimeState)?;
    if let Some(expected) = restart_identity {
        validate_restart_identity(expected, &release_identity, &fingerprint)?;
    }
    let mut existing_profile_state = read_runtime_profile_state(&runtime, &project.identity)?;
    if let Some(state) = &existing_profile_state {
        let binding = runtime_process_binding(
            &runtime,
            &project.identity.tenant_id,
            &compose_project,
            state,
            &project.identity,
        )?;
        validate_runtime_profile_transition(state, profile, &release_identity, &fingerprint)?;
        clear_quiescent_runtime_process_state(&runtime, &binding)?;
    } else if runtime.join(RUNTIME_PROCESS_STATE_FILE).exists() {
        return Err(CliError::RuntimeState(
            "runtime process state exists without its owning runtime profile".to_owned(),
        ));
    }
    if profile.has_sandbox() {
        ensure_sandbox_kubernetes_dependency()?;
    }
    let prepared_tls_identity_digests = ensure_selected_feature_identity(
        &state_directory,
        profile,
        existing_profile_state
            .as_ref()
            .map(|state| &state.tls_identity_digests),
    )?;
    if let Some(state) = existing_profile_state.as_mut() {
        append_selected_feature_configs(&state_directory, &project.identity, state, profile)?;
    }
    let binary_directory = ensure_runtime_binaries(
        &workspace,
        &runtime,
        &fingerprint,
        profile,
        verified_release.as_ref(),
    )?;
    compose_up(&workspace, &compose_project, &runtime)?;
    let profile_state = match existing_profile_state {
        Some(mut state) => {
            state.features = profile
                .feature_names()
                .into_iter()
                .map(str::to_owned)
                .collect();
            state.profile_digest = expected_profile_digest.clone();
            state.release_identity.clone_from(&release_identity);
            state.source_fingerprint.clone_from(&fingerprint);
            state.tls_identity_digests = prepared_tls_identity_digests;
            refresh_runtime_profile_closure_digest(&mut state)?;
            validate_runtime_profile_state(
                &runtime.join(RUNTIME_PROFILE_STATE_FILE),
                &state,
                &project.identity,
            )?;
            write_runtime_json_replace(&runtime.join(RUNTIME_PROFILE_STATE_FILE), &state)?;
            state
        }
        None => {
            let (kms_key_arn, secret_readiness_arn) =
                initialize_localstack_artifact_dependency(&workspace, &compose_project, &runtime)?;
            let ports = RuntimePortBindings::allocate()?;
            prepare_runtime_profile_with_ports(
                root,
                &kms_key_arn,
                &secret_readiness_arn,
                &fingerprint,
                &ports,
                profile,
                &release_identity,
            )?;
            read_runtime_profile_state(&runtime, &project.identity)?.ok_or_else(|| {
                CliError::RuntimeState(
                    "runtime profile was not persisted after generation".to_owned(),
                )
            })?
        }
    };
    ensure_localstack_artifact_dependency(
        &workspace,
        &compose_project,
        &runtime,
        &profile_state.kms_key_arn,
    )
    .map_err(|_| {
        CliError::RuntimeUnavailable(
            "the exact local S3/KMS dependency is unavailable; this LocalStack Community profile cannot recover Artifact authority after its dependency container is recreated, so restore the original container or initialize a fresh local project"
                .to_owned(),
        )
    })?;
    if profile.has_features() {
        ensure_localstack_secret_dependency(
            &workspace,
            &compose_project,
            &runtime,
            &profile_state.secret_readiness_arn,
            &profile_state.kms_key_arn,
        )?;
    }
    provision_and_bootstrap_authority(
        &binary_directory,
        &runtime,
        &project.identity,
        &profile_state,
    )?;
    let binding = runtime_process_binding(
        &runtime,
        &project.identity.tenant_id,
        &compose_project,
        &profile_state,
        &project.identity,
    )?;
    let mut state = RuntimeProcessState {
        schema_version: RUNTIME_PROCESS_SCHEMA_VERSION,
        kind: RUNTIME_PROCESS_KIND.to_owned(),
        tenant_id: project.identity.tenant_id.clone(),
        profile: profile.label(),
        profile_digest: profile_state.profile_digest.clone(),
        release_identity: profile_state.release_identity.clone(),
        compose_project: compose_project.clone(),
        source_fingerprint: profile_state.source_fingerprint.clone(),
        lifecycle: RuntimeProcessLifecycle::Starting,
        processes: BTreeMap::new(),
    };
    write_runtime_json_replace(&runtime.join(RUNTIME_PROCESS_STATE_FILE), &state)?;
    if let Err(error) = start_profile_processes(
        &binary_directory,
        &runtime,
        &profile_state,
        profile,
        &binding,
        &mut state,
    ) {
        return Err(abort_runtime_start(&runtime, &mut state, error));
    }
    project.profiles = runtime_project_profile_summary(&profile_state, "ready");
    if let Err(error) =
        write_runtime_json_replace(&state_directory.join(PROJECT_STATE_FILE), &project)
    {
        return Err(abort_runtime_start(&runtime, &mut state, error));
    }
    render_runtime_status(&state, &binding)
}

fn validate_restart_identity(
    expected: &RuntimeRestartIdentity,
    release_identity: &str,
    source_fingerprint: &str,
) -> Result<(), CliError> {
    if expected.release_identity == release_identity
        && expected.source_fingerprint == source_fingerprint
    {
        Ok(())
    } else {
        Err(CliError::RuntimeState(
            "`insight start` cannot change the persisted release/source identity; use an explicit `insight dev` transition after reviewing the verified source or release"
                .to_owned(),
        ))
    }
}

fn runtime_status(root: &Path) -> Result<String, CliError> {
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let state_directory = root.join(PROJECT_DIRECTORY);
    let project = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity(&state_directory, &project.identity)?;
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    let profile = read_runtime_profile_state(&runtime, &project.identity)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no local runtime profile exists; run `insight dev` first".to_owned(),
        )
    })?;
    let compose_project = compose_project_name(&project.identity.tenant_id)?;
    let binding = runtime_process_binding(
        &runtime,
        &project.identity.tenant_id,
        &compose_project,
        &profile,
        &project.identity,
    )?;
    let state = read_runtime_process_state(&runtime, &binding)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no local Platform process state exists; run `insight dev` first".to_owned(),
        )
    })?;
    render_runtime_status(&state, &binding)
}

fn runtime_logs(root: &Path, role: Option<&str>) -> Result<String, CliError> {
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let state_directory = root.join(PROJECT_DIRECTORY);
    let project = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity(&state_directory, &project.identity)?;
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    let profile = read_runtime_profile_state(&runtime, &project.identity)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no local runtime profile exists; run `insight dev` first".to_owned(),
        )
    })?;
    let compose_project = compose_project_name(&project.identity.tenant_id)?;
    let binding = runtime_process_binding(
        &runtime,
        &project.identity.tenant_id,
        &compose_project,
        &profile,
        &project.identity,
    )?;
    let state = read_runtime_process_state(&runtime, &binding)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no local Platform process state exists; run `insight dev` first".to_owned(),
        )
    })?;
    let roles = match role {
        Some(role) => vec![role.to_owned()],
        None => state.processes.keys().cloned().collect(),
    };
    let mut output = format!(
        "lifecycle={} complete={}\n",
        match state.lifecycle {
            RuntimeProcessLifecycle::Starting => "starting",
            RuntimeProcessLifecycle::Running => "running",
            RuntimeProcessLifecycle::Stopped => "stopped",
        },
        state.lifecycle != RuntimeProcessLifecycle::Starting,
    );
    for role in roles {
        let process = state.processes.get(&role).ok_or_else(|| {
            let detail = if state.lifecycle == RuntimeProcessLifecycle::Starting
                && binding.expected_processes.contains_key(&role)
            {
                format!("role {role} has not yet been recorded by the incomplete start journal")
            } else {
                format!("no log is registered for role {role}")
            };
            CliError::RuntimeState(detail)
        })?;
        let path = runtime.join(&process.log_file);
        let bytes = fs::read(&path).map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })?;
        let text = String::from_utf8_lossy(&bytes);
        let lines = text.lines().rev().take(100).collect::<Vec<_>>();
        output.push_str(&format!("== {role} ==\n"));
        for line in lines.iter().rev() {
            output.push_str(line);
            output.push('\n');
        }
    }
    Ok(output)
}

fn restart_development_profile(workspace: &Path, root: &Path) -> Result<String, CliError> {
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let _lock = acquire_runtime_lifecycle_lock(&root)?;
    let state_directory = root.join(PROJECT_DIRECTORY);
    let mut project = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity(&state_directory, &project.identity)?;
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    let persisted = read_runtime_profile_state(&runtime, &project.identity)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no local runtime profile exists; run `insight dev` first".to_owned(),
        )
    })?;
    let profile = restart_profile_selection(&persisted)?;
    let compose_project = compose_project_name(&project.identity.tenant_id)?;
    let binding = runtime_process_binding(
        &runtime,
        &project.identity.tenant_id,
        &compose_project,
        &persisted,
        &project.identity,
    )?;
    if let Some(processes) = read_runtime_process_state(&runtime, &binding)? {
        if processes.lifecycle == RuntimeProcessLifecycle::Running
            && processes.processes.values().all(|process| {
                observe_runtime_process(process).ok() == Some(RuntimeProcessObservation::Owned)
                    && http_ready(&process.ready_address)
            })
        {
            let expected_summary = runtime_project_profile_summary(&persisted, "ready");
            if project.profiles != expected_summary {
                project.profiles = expected_summary;
                write_runtime_json_replace(&state_directory.join(PROJECT_STATE_FILE), &project)?;
            }
            return render_runtime_status(&processes, &binding);
        }
    }
    let restart_identity = RuntimeRestartIdentity {
        release_identity: persisted.release_identity,
        source_fingerprint: persisted.source_fingerprint,
    };
    run_development_profile_locked(workspace, &root, profile, Some(&restart_identity))
}

fn restart_profile_selection(persisted: &RuntimeProfileState) -> Result<DevProfile, CliError> {
    let features = (!persisted.features.is_empty()).then(|| persisted.features.join(","));
    let from_source = persisted.release_identity.starts_with("source:");
    DevProfile::parse(features.as_deref(), false, from_source).map_err(CliError::UnsupportedProfile)
}

fn runtime_project_profile_summary(
    persisted: &RuntimeProfileState,
    state: &str,
) -> BTreeMap<String, LocalProfileState> {
    BTreeMap::from([(
        "starter".to_owned(),
        LocalProfileState {
            state: state.to_owned(),
            features: persisted.features.clone(),
            profile_digest: Some(persisted.profile_digest.clone()),
            release_identity: Some(persisted.release_identity.clone()),
        },
    )])
}

fn stop_development_profile(_workspace: &Path, root: &Path) -> Result<String, CliError> {
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let _lock = acquire_runtime_lifecycle_lock(&root)?;
    let state_directory = root.join(PROJECT_DIRECTORY);
    let mut project = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity_for_cleanup(&state_directory, &project.identity)?;
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    let profile =
        read_runtime_profile_state_for_cleanup(&runtime, &project.identity)?.ok_or_else(|| {
            CliError::RuntimeState(
                "no local runtime profile exists; run `insight dev` first".to_owned(),
            )
        })?;
    let compose_project = compose_project_name(&project.identity.tenant_id)?;
    let binding = runtime_process_binding_for_cleanup(
        &runtime,
        &project.identity.tenant_id,
        &compose_project,
        &profile,
        &project.identity,
    )?;
    let mut state = read_runtime_process_state(&runtime, &binding)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no local Platform process state exists; run `insight dev` first".to_owned(),
        )
    })?;
    for process in state.processes.values() {
        stop_process(process)?;
    }
    state.processes.clear();
    state.lifecycle = RuntimeProcessLifecycle::Stopped;
    write_runtime_json_replace(&runtime.join(RUNTIME_PROCESS_STATE_FILE), &state)?;
    if let Some(profile) = project.profiles.get_mut("starter") {
        profile.state = "stopped".to_owned();
    }
    write_runtime_json_replace(&state_directory.join(PROJECT_STATE_FILE), &project)?;
    Ok(
        "stopped local Platform roles; PostgreSQL, NATS and LocalStack dependencies remain ready for durable restart\n"
            .to_owned(),
    )
}

fn reset_local_project(
    workspace: &Path,
    root: &Path,
    confirm: Option<&str>,
) -> Result<String, CliError> {
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let _lock = confirm
        .is_some()
        .then(|| acquire_runtime_lifecycle_lock(&root))
        .transpose()?;
    let state_directory = root.join(PROJECT_DIRECTORY);
    let project = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity_for_cleanup(&state_directory, &project.identity)?;
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    let compose_project = compose_project_name(&project.identity.tenant_id)?;
    if confirm.is_none() {
        return Ok(format!(
            "Reset is destructive and cannot be recovered.\nProject: {}\nLocal state: {}\nDocker volumes: {}\nConfirm with: insight reset --path {} --confirm {}\n",
            project.project_name,
            state_directory.display(),
            compose_project,
            root.display(),
            project.project_name
        ));
    }
    if confirm != Some(project.project_name.as_str()) {
        return Err(CliError::InvalidOptionValue {
            option: "--confirm",
            value: confirm.unwrap_or_default().to_owned(),
        });
    }
    if runtime.join(RUNTIME_PROCESS_STATE_FILE).exists() {
        let profile = read_runtime_profile_state_for_cleanup(&runtime, &project.identity)?
            .ok_or_else(|| {
                CliError::RuntimeState(
                    "runtime process state exists without its owning runtime profile".to_owned(),
                )
            })?;
        let binding = runtime_process_binding_for_cleanup(
            &runtime,
            &project.identity.tenant_id,
            &compose_project,
            &profile,
            &project.identity,
        )?;
        if let Some(state) = read_runtime_process_state(&runtime, &binding)? {
            for process in state.processes.values() {
                match observe_runtime_process(process)? {
                    RuntimeProcessObservation::Owned => {
                        return Err(CliError::RuntimeUnavailable(
                            "local Platform roles are still running; run `insight stop` before reset"
                                .to_owned(),
                        ));
                    }
                    RuntimeProcessObservation::IdentityMismatch => {
                        // This PID is not owned by the journal and is never signalled. The stale
                        // record cannot block cleanup of the exact project-local state.
                    }
                    RuntimeProcessObservation::Stopped => {}
                }
            }
        }
    }
    let mut compose = compose_command(workspace, &compose_project, &runtime);
    compose.args(["down", "--volumes", "--remove-orphans"]);
    run_external(compose, "delete the exact local dependency volumes")?;
    fs::remove_dir_all(&state_directory).map_err(|source| CliError::InitializeProject {
        path: state_directory.display().to_string(),
        source,
    })?;
    Ok(format!(
        "reset {}: removed {} and Docker volumes for {}; this data cannot be recovered\n",
        project.project_name,
        state_directory.display(),
        compose_project
    ))
}

fn workspace_root(root: &Path) -> Result<PathBuf, CliError> {
    let workspace = fs::canonicalize(root)
        .map_err(|source| CliError::WorkspaceUnavailable(source.to_string()))?;
    if workspace.join("Cargo.toml").is_file() && workspace.join("deploy/dev/compose.yaml").is_file()
    {
        Ok(workspace)
    } else {
        Err(CliError::WorkspaceUnavailable(
            "run from a checked-out Insight Agent Platform workspace containing deploy/dev/compose.yaml"
                .to_owned(),
        ))
    }
}

fn compose_project_name(tenant_id: &str) -> Result<String, CliError> {
    let suffix = tenant_id
        .rsplit('_')
        .next()
        .filter(|value| value.len() >= 8)
        .map(|value| &value[..8])
        .ok_or_else(|| CliError::RuntimeState("local tenant identity is malformed".to_owned()))?;
    Ok(format!("insight-{suffix}"))
}

fn compose_up(workspace: &Path, project: &str, runtime: &Path) -> Result<(), CliError> {
    let mut command = compose_command(workspace, project, runtime);
    command.args(["up", "--detach", "--wait"]);
    run_external(
        command,
        "start local PostgreSQL, NATS and S3/KMS dependencies",
    )?;
    wait_for_localstack(workspace, project, runtime)
}

fn compose_command(workspace: &Path, project: &str, runtime: &Path) -> ProcessCommand {
    let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
    let mut command = ProcessCommand::new("docker");
    command
        .current_dir(runtime)
        .env(
            "INSIGHT_DEV_NATS_CA_PATH",
            tls.join(RUNTIME_CA_CERTIFICATE_FILE),
        )
        .env(
            "INSIGHT_DEV_NATS_SERVER_CERT_PATH",
            tls.join(RUNTIME_NATS_SERVER_CERTIFICATE_FILE),
        )
        .env(
            "INSIGHT_DEV_NATS_SERVER_KEY_PATH",
            tls.join(RUNTIME_NATS_SERVER_PRIVATE_KEY_FILE),
        )
        .args([
            "compose",
            "--project-name",
            project,
            "--file",
            RUNTIME_COMPOSE_FILE,
        ]);
    let _ = workspace;
    command
}

fn ensure_embedded_compose(runtime: &Path) -> Result<(), CliError> {
    let path = runtime.join(RUNTIME_COMPOSE_FILE);
    match fs::read(&path) {
        Ok(bytes) if bytes == DEV_COMPOSE_BYTES => Ok(()),
        Ok(_) => Err(CliError::RuntimeState(format!(
            "{} drifted from the CLI release; restore it or use reset after reviewing local data",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            write_new(&path, DEV_COMPOSE_BYTES)
        }
        Err(source) => Err(CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        }),
    }
}

fn initialize_localstack_artifact_dependency(
    workspace: &Path,
    compose_project: &str,
    runtime: &Path,
) -> Result<(String, String), CliError> {
    compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &["s3api", "head-bucket", "--bucket", LOCAL_ARTIFACT_BUCKET],
    )
    .or_else(|_| {
        compose_awslocal(
            workspace,
            compose_project,
            runtime,
            &["s3api", "create-bucket", "--bucket", LOCAL_ARTIFACT_BUCKET],
        )
        .map(|_| String::new())
    })?;
    compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &[
            "s3api",
            "put-bucket-versioning",
            "--bucket",
            LOCAL_ARTIFACT_BUCKET,
            "--versioning-configuration",
            "Status=Enabled",
        ],
    )?;
    let arn = compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &[
            "kms",
            "create-key",
            "--description",
            "insight-local-development",
            "--query",
            "KeyMetadata.Arn",
            "--output",
            "text",
        ],
    )?;
    let arn = arn.trim();
    if !local_kms_key_arn_is_valid(arn) {
        return Err(CliError::RuntimeUnavailable(
            "local KMS did not return a valid key ARN".to_owned(),
        ));
    }
    let secret_arn = compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &[
            "secretsmanager",
            "create-secret",
            "--name",
            LOCAL_SECRET_READINESS_NAME,
            "--secret-string",
            "ready",
            "--kms-key-id",
            arn,
            "--query",
            "ARN",
            "--output",
            "text",
        ],
    )?;
    let secret_arn = secret_arn.trim();
    validate_local_secret_readiness_arn(Path::new("."), secret_arn)?;
    Ok((arn.to_owned(), secret_arn.to_owned()))
}

fn ensure_localstack_artifact_dependency(
    workspace: &Path,
    compose_project: &str,
    runtime: &Path,
    kms_key_arn: &str,
) -> Result<(), CliError> {
    compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &["s3api", "head-bucket", "--bucket", LOCAL_ARTIFACT_BUCKET],
    )?;
    let versioning = compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &[
            "s3api",
            "get-bucket-versioning",
            "--bucket",
            LOCAL_ARTIFACT_BUCKET,
            "--output",
            "json",
        ],
    )?;
    ensure_local_bucket_versioning_enabled(&versioning)?;
    let kms_metadata = compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &[
            "kms",
            "describe-key",
            "--key-id",
            kms_key_arn,
            "--output",
            "json",
        ],
    )?;
    ensure_local_kms_metadata(&kms_metadata, kms_key_arn)?;
    Ok(())
}

fn strict_dependency_json(response: &str, authority: &str) -> Result<serde_json::Value, CliError> {
    const MAX_DEPENDENCY_RESPONSE_BYTES: usize = 65_536;
    parse_strict_json(
        response.as_bytes(),
        JsonLimits {
            max_bytes: MAX_DEPENDENCY_RESPONSE_BYTES,
            max_depth: 32,
            max_properties_per_object: 256,
            max_items_per_array: 256,
            max_string_bytes: 8_192,
        },
    )
    .map_err(|_| {
        CliError::RuntimeUnavailable(format!(
            "local {authority} authority response was not bounded strict JSON"
        ))
    })
}

fn ensure_local_kms_metadata(response: &str, kms_key_arn: &str) -> Result<(), CliError> {
    let response = strict_dependency_json(response, "KMS")?;
    let metadata = response.get("KeyMetadata").ok_or_else(|| {
        CliError::RuntimeUnavailable("local KMS response omitted KeyMetadata".to_owned())
    })?;
    if metadata.get("Arn").and_then(serde_json::Value::as_str) != Some(kms_key_arn)
        || metadata.get("Enabled").and_then(serde_json::Value::as_bool) != Some(true)
        || metadata.get("KeyState").and_then(serde_json::Value::as_str) != Some("Enabled")
        || metadata.get("KeyUsage").and_then(serde_json::Value::as_str) != Some("ENCRYPT_DECRYPT")
        || metadata.get("Origin").and_then(serde_json::Value::as_str) != Some("AWS_KMS")
    {
        return Err(CliError::RuntimeUnavailable(
            "local KMS metadata does not match the persisted enabled encryption authority"
                .to_owned(),
        ));
    }
    Ok(())
}

fn ensure_local_bucket_versioning_enabled(response: &str) -> Result<(), CliError> {
    let response: serde_json::Value = serde_json::from_str(response).map_err(|_| {
        CliError::RuntimeUnavailable(
            "local S3 bucket versioning response was not valid JSON".to_owned(),
        )
    })?;
    if response.get("Status").and_then(serde_json::Value::as_str) != Some("Enabled") {
        return Err(CliError::RuntimeUnavailable(
            "local S3 bucket versioning is not enabled".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_localstack_secret_dependency(
    workspace: &Path,
    compose_project: &str,
    runtime: &Path,
    readiness_secret_arn: &str,
    kms_key_arn: &str,
) -> Result<(), CliError> {
    let metadata = compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &[
            "secretsmanager",
            "describe-secret",
            "--secret-id",
            readiness_secret_arn,
            "--output",
            "json",
        ],
    )?;
    ensure_local_secret_metadata(&metadata, readiness_secret_arn, kms_key_arn)
}

fn ensure_local_secret_metadata(
    response: &str,
    readiness_secret_arn: &str,
    kms_key_arn: &str,
) -> Result<(), CliError> {
    let response = strict_dependency_json(response, "Secrets Manager")?;
    let observed_kms = response.get("KmsKeyId").and_then(serde_json::Value::as_str);
    let expected_key_id = kms_key_arn.rsplit('/').next();
    let kms_matches = observed_kms == Some(kms_key_arn)
        || observed_kms.is_some_and(|value| Some(value) == expected_key_id);
    if response.get("ARN").and_then(serde_json::Value::as_str) != Some(readiness_secret_arn)
        || response.get("Name").and_then(serde_json::Value::as_str)
            != Some(LOCAL_SECRET_READINESS_NAME)
        || !kms_matches
    {
        return Err(CliError::RuntimeUnavailable(
            "local Secrets Manager metadata does not match the persisted readiness/KMS authority"
                .to_owned(),
        ));
    }
    Ok(())
}

fn compose_awslocal(
    workspace: &Path,
    project: &str,
    runtime: &Path,
    args: &[&str],
) -> Result<String, CliError> {
    let mut command = compose_command(workspace, project, runtime);
    command.args(["exec", "--no-TTY", "localstack", "awslocal"]);
    command.args(args);
    run_external(command, "prepare local S3/KMS dependency")
}

fn wait_for_localstack(workspace: &Path, project: &str, runtime: &Path) -> Result<(), CliError> {
    let deadline = SystemTime::now() + Duration::from_secs(30);
    while SystemTime::now() < deadline {
        if compose_awslocal(workspace, project, runtime, &["kms", "list-keys"]).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err(CliError::RuntimeUnavailable(
        "local S3/KMS dependency did not become ready within 30 seconds".to_owned(),
    ))
}

fn ensure_runtime_binaries(
    workspace: &Path,
    runtime: &Path,
    fingerprint: &str,
    profile: DevProfile,
    release: Option<&release::VerifiedRelease>,
) -> Result<PathBuf, CliError> {
    if let Some(release) = release {
        return ensure_prebuilt_runtime_binaries(runtime, profile, release);
    }
    let binary_directory = workspace.join("target/release");
    let binaries = runtime_binary_paths(&binary_directory, profile);
    let cached = read_runtime_json::<RuntimeBuildState>(&runtime.join(RUNTIME_BUILD_STATE_FILE))?
        .is_some_and(|state| state.schema_version == 1 && state.source_fingerprint == fingerprint);
    if cached && binaries.values().all(|path| path.is_file()) {
        return Ok(binary_directory);
    }
    let mut command = ProcessCommand::new("cargo");
    command.current_dir(workspace).args([
        "build",
        "--locked",
        "--release",
        "-p",
        "insight-platform-postgres",
        "--bin",
        "platform-schema",
        "--bin",
        "platform-dev-bootstrap",
        "-p",
        "insight-platform-registry-validation-worker",
        "--bin",
        "platform-registry-validation-worker",
        "-p",
        "insight-platform-gateway",
        "--bin",
        "platform-gateway",
        "-p",
        "insight-platform-artifact-service",
        "--bin",
        "platform-artifact-gateway",
        "--bin",
        "platform-artifact-data-worker",
        "-p",
        "insight-platform-orchestration-worker",
        "--bin",
        "platform-orchestration-worker",
        "-p",
        "insight-platform-capability-worker",
        "--bin",
        "platform-capability-native-worker",
    ]);
    if profile.has_context() {
        command.args([
            "-p",
            "insight-platform-context-worker",
            "--bin",
            "platform-context-worker",
            "--bin",
            "platform-context-dataset-worker",
            "--bin",
            "platform-remote-context-worker",
            "--bin",
            "platform-subscription-context-worker",
        ]);
    }
    if profile.needs_egress() {
        command.args([
            "-p",
            "insight-platform-security-authority",
            "--bin",
            "platform-security-authority",
            "-p",
            "insight-platform-egress-broker",
            "--bin",
            "platform-egress-broker",
        ]);
    }
    if profile.has_model() {
        command.args([
            "-p",
            "insight-platform-model-worker",
            "--bin",
            "platform-model-worker",
        ]);
    }
    if profile.has_mcp() {
        command.args([
            "-p",
            "insight-platform-mcp-service",
            "--bin",
            "platform-mcp-host",
            "--bin",
            "platform-mcp-resource-host",
            "--bin",
            "platform-mcp-discovery-worker",
            "--bin",
            "platform-mcp-subscription-worker",
            "-p",
            "insight-platform-mcp-cleanup-worker",
            "--bin",
            "platform-mcp-cleanup-worker",
            "-p",
            "insight-platform-callback-api",
            "--bin",
            "platform-callback-api",
        ]);
    } else if profile.has_context() {
        command.args([
            "-p",
            "insight-platform-mcp-service",
            "--bin",
            "platform-mcp-resource-host",
        ]);
    }
    if profile.has_remote_capability() {
        command.args([
            "-p",
            "insight-platform-capability-worker",
            "--bin",
            "platform-capability-remote-worker",
        ]);
    }
    run_external(command, "build the changed local Platform role closure")?;
    if !binaries.values().all(|path| path.is_file()) {
        return Err(CliError::RuntimeUnavailable(
            "Cargo completed without all required local Platform binaries".to_owned(),
        ));
    }
    let state = RuntimeBuildState {
        schema_version: 1,
        source_fingerprint: fingerprint.to_owned(),
    };
    write_runtime_json_replace(&runtime.join(RUNTIME_BUILD_STATE_FILE), &state)?;
    Ok(binary_directory)
}

fn ensure_prebuilt_runtime_binaries(
    runtime: &Path,
    profile: DevProfile,
    release: &release::VerifiedRelease,
) -> Result<PathBuf, CliError> {
    let release_root = runtime.join("releases").join(
        release
            .bundle_digest
            .strip_prefix("sha256:")
            .ok_or_else(|| CliError::RuntimeState("release digest is malformed".to_owned()))?,
    );
    let binary_directory = release_root.join("bin");
    let required = runtime_binary_paths(&binary_directory, profile);
    let mut inspect = ProcessCommand::new("docker");
    inspect.args(["image", "inspect", &release.runtime_image]);
    let image_present = run_external(inspect, "verify the exact cached runtime image").is_ok();
    if profile.offline() && !image_present {
        return Err(CliError::RuntimeUnavailable(format!(
            "offline cache misses {}; reconnect and run: docker pull {}",
            release.runtime_image, release.runtime_image
        )));
    }
    if !profile.offline() {
        let mut pull = ProcessCommand::new("docker");
        pull.args(["pull", &release.runtime_image]);
        run_external(pull, "pull the exact signed runtime image")?;
    }
    if required.values().all(|path| path.is_file()) {
        return Ok(binary_directory);
    }
    if binary_directory.exists() {
        return Err(CliError::RuntimeState(format!(
            "prebuilt runtime cache at {} is incomplete; do not fall back to a source build",
            binary_directory.display()
        )));
    }
    fs::create_dir_all(&release_root).map_err(|source| CliError::InitializeProject {
        path: release_root.display().to_string(),
        source,
    })?;
    let staging = release_root.join(format!("bin.{}.tmp", Uuid::now_v7()));
    fs::create_dir(&staging).map_err(|source| CliError::InitializeProject {
        path: staging.display().to_string(),
        source,
    })?;
    let mut create = ProcessCommand::new("docker");
    create.args([
        "create",
        "--entrypoint",
        "/bin/true",
        &release.runtime_image,
    ]);
    let container = run_external(create, "create an exact runtime extraction container")?;
    let extraction = (|| {
        let mut copy = ProcessCommand::new("docker");
        copy.args([
            "cp",
            &format!("{container}:/usr/local/bin/."),
            staging.to_str().ok_or_else(|| {
                CliError::RuntimeState("runtime cache path is not UTF-8".to_owned())
            })?,
        ]);
        run_external(copy, "extract immutable Platform role binaries")?;
        let staged = runtime_binary_paths(&staging, profile);
        if !staged.values().all(|path| path.is_file()) {
            return Err(CliError::RuntimeUnavailable(
                "signed runtime image does not contain the selected role closure".to_owned(),
            ));
        }
        Ok(())
    })();
    let mut remove = ProcessCommand::new("docker");
    remove.args(["rm", "--force", &container]);
    let cleanup = run_external(remove, "remove the runtime extraction container");
    if let Err(error) = extraction {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }
    if let Err(error) = cleanup {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }
    fs::rename(&staging, &binary_directory).map_err(|source| {
        let _ = fs::remove_dir_all(&staging);
        CliError::InitializeProject {
            path: binary_directory.display().to_string(),
            source,
        }
    })?;
    Ok(binary_directory)
}

fn base_runtime_binary_paths(release: &Path) -> BTreeMap<&'static str, PathBuf> {
    let suffix = std::env::consts::EXE_SUFFIX;
    BTreeMap::from([
        (
            "platform-schema",
            release.join(format!("platform-schema{suffix}")),
        ),
        (
            "platform-dev-bootstrap",
            release.join(format!("platform-dev-bootstrap{suffix}")),
        ),
        (
            "platform-registry-validation-worker",
            release.join(format!("platform-registry-validation-worker{suffix}")),
        ),
        (
            "platform-gateway",
            release.join(format!("platform-gateway{suffix}")),
        ),
        (
            "platform-artifact-gateway",
            release.join(format!("platform-artifact-gateway{suffix}")),
        ),
        (
            "platform-artifact-data-worker",
            release.join(format!("platform-artifact-data-worker{suffix}")),
        ),
        (
            "platform-orchestration-worker",
            release.join(format!("platform-orchestration-worker{suffix}")),
        ),
        (
            "platform-capability-native-worker",
            release.join(format!("platform-capability-native-worker{suffix}")),
        ),
    ])
}

fn runtime_binary_paths(release: &Path, profile: DevProfile) -> BTreeMap<&'static str, PathBuf> {
    let mut binaries = base_runtime_binary_paths(release);
    let suffix = std::env::consts::EXE_SUFFIX;
    let mut include = |name: &'static str| {
        binaries.insert(name, release.join(format!("{name}{suffix}")));
    };
    if profile.has_context() {
        for name in [
            "platform-context-worker",
            "platform-context-dataset-worker",
            "platform-remote-context-worker",
            "platform-subscription-context-worker",
            "platform-mcp-resource-host",
        ] {
            include(name);
        }
    }
    if profile.needs_egress() {
        include("platform-security-authority");
        include("platform-egress-broker");
    }
    if profile.has_model() {
        include("platform-model-worker");
    }
    if profile.has_mcp() {
        for name in [
            "platform-mcp-host",
            "platform-mcp-resource-host",
            "platform-mcp-discovery-worker",
            "platform-mcp-subscription-worker",
            "platform-mcp-cleanup-worker",
            "platform-callback-api",
        ] {
            include(name);
        }
    }
    if profile.has_remote_capability() {
        include("platform-capability-remote-worker");
    }
    binaries
}

fn provision_and_bootstrap_authority(
    binary_directory: &Path,
    runtime: &Path,
    identity: &LocalIdentityState,
    profile: &RuntimeProfileState,
) -> Result<(), CliError> {
    let database_url = "postgres://insight:insight@127.0.0.1:5432/insight_platform";
    let binaries = base_runtime_binary_paths(binary_directory);
    let schema = binaries
        .get("platform-schema")
        .ok_or_else(|| CliError::RuntimeState("schema binary path is unavailable".to_owned()))?;
    let verified = ProcessCommand::new(schema)
        .env("PLATFORM_DATABASE_URL", database_url)
        .arg("verify")
        .output()
        .map_err(|error| {
            CliError::RuntimeUnavailable(format!("run schema verification: {error}"))
        })?;
    if !verified.status.success() {
        let mut provision = ProcessCommand::new(schema);
        provision
            .env("PLATFORM_DATABASE_URL", database_url)
            .arg("provision");
        run_external(provision, "provision the fresh local PostgreSQL authority")?;
    }
    let bootstrap = binaries.get("platform-dev-bootstrap").ok_or_else(|| {
        CliError::RuntimeState("development bootstrap binary path is unavailable".to_owned())
    })?;
    let config_path = runtime
        .parent()
        .ok_or_else(|| CliError::RuntimeState("local runtime path has no state parent".to_owned()))?
        .join(IDENTITY_DIRECTORY)
        .join(IDENTITY_BOOTSTRAP_CONFIG_FILE);
    let mut command = ProcessCommand::new(bootstrap);
    command
        .env("PLATFORM_DATABASE_URL", database_url)
        .env("PLATFORM_DEV_BOOTSTRAP_CONFIG", config_path)
        .env(
            "PLATFORM_DEV_BOOTSTRAP_CONFIG_DIGEST",
            &identity.bootstrap_config_digest,
        )
        .env(
            "PLATFORM_DEV_ARTIFACT_BOOTSTRAP_CONFIG",
            runtime
                .join(RUNTIME_CONFIGURATION_DIRECTORY)
                .join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE),
        )
        .env(
            "PLATFORM_DEV_ARTIFACT_BOOTSTRAP_CONFIG_DIGEST",
            profile
                .config_digests
                .get("artifact-bootstrap")
                .ok_or_else(|| {
                    CliError::RuntimeState(
                        "Artifact bootstrap config digest is unavailable".to_owned(),
                    )
                })?,
        );
    run_external(
        command,
        "ensure the exact local tenant and developer authority roots",
    )?;
    Ok(())
}

fn start_profile_processes(
    binary_directory: &Path,
    runtime: &Path,
    profile: &RuntimeProfileState,
    selected_profile: DevProfile,
    binding: &RuntimeProcessBinding,
    state: &mut RuntimeProcessState,
) -> Result<(), CliError> {
    let binaries = runtime_binary_paths(binary_directory, selected_profile);
    let logs = runtime.join(RUNTIME_LOG_DIRECTORY);
    fs::create_dir_all(&logs).map_err(|source| CliError::InitializeProject {
        path: logs.display().to_string(),
        source,
    })?;
    let configuration = runtime.join(RUNTIME_CONFIGURATION_DIRECTORY);
    let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
    let database_url = "postgres://insight:insight@127.0.0.1:5432/insight_platform";
    let common_aws = [
        ("AWS_ACCESS_KEY_ID", "test"),
        ("AWS_SECRET_ACCESS_KEY", "test"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ];
    let mut specs = vec![
        RuntimeLaunchSpec::new(
            "artifact-data",
            binaries["platform-artifact-data-worker"].clone(),
            &loopback_address(profile.ports.artifact_data_observability),
            vec![
                (
                    "PLATFORM_ARTIFACT_DATA_WORKER_CONFIG",
                    config_path(&configuration, RUNTIME_ARTIFACT_DATA_CONFIG_FILE),
                ),
                (
                    "PLATFORM_ARTIFACT_DATA_WORKER_CONFIG_DIGEST",
                    profile.config_digests["artifact-data"].clone(),
                ),
                (
                    "PLATFORM_ARTIFACT_DATA_WORKER_AUDIENCE",
                    "data_worker".to_owned(),
                ),
                (
                    "PLATFORM_ARTIFACT_DATA_WORKER_READ_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_ARTIFACT_DATA_WORKER_WORK_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_ARTIFACT_DATA_WORKER_CLIENT_CA_PATH",
                    tls.join(RUNTIME_CA_CERTIFICATE_FILE).display().to_string(),
                ),
                (
                    "PLATFORM_ARTIFACT_DATA_WORKER_CERT_PATH",
                    tls.join(RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_ARTIFACT_DATA_WORKER_KEY_PATH",
                    tls.join(RUNTIME_ARTIFACT_DATA_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            common_aws
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
        ),
        RuntimeLaunchSpec::new(
            "artifact-gateway",
            binaries["platform-artifact-gateway"].clone(),
            &loopback_address(profile.ports.artifact_gateway_observability),
            vec![
                (
                    "PLATFORM_ARTIFACT_GATEWAY_CONFIG",
                    config_path(&configuration, RUNTIME_ARTIFACT_GATEWAY_CONFIG_FILE),
                ),
                (
                    "PLATFORM_ARTIFACT_GATEWAY_CONFIG_DIGEST",
                    profile.config_digests["artifact-gateway"].clone(),
                ),
                (
                    "PLATFORM_ARTIFACT_GATEWAY_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_ARTIFACT_GATEWAY_CLIENT_CA_PATH",
                    tls.join(RUNTIME_CA_CERTIFICATE_FILE).display().to_string(),
                ),
                (
                    "PLATFORM_ARTIFACT_GATEWAY_CERT_PATH",
                    tls.join(RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_ARTIFACT_GATEWAY_KEY_PATH",
                    tls.join(RUNTIME_ARTIFACT_GATEWAY_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            common_aws
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
        ),
        RuntimeLaunchSpec::new(
            "orchestration",
            binaries["platform-orchestration-worker"].clone(),
            &loopback_address(profile.ports.orchestration_observability),
            vec![
                (
                    "PLATFORM_ORCHESTRATION_WORKER_CONFIG",
                    config_path(&configuration, RUNTIME_ORCHESTRATION_CONFIG_FILE),
                ),
                (
                    "PLATFORM_ORCHESTRATION_WORKER_CONFIG_DIGEST",
                    profile.config_digests["orchestration"].clone(),
                ),
                (
                    "PLATFORM_ORCHESTRATION_WORKER_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_CA_PATH",
                    tls.join(RUNTIME_CA_CERTIFICATE_FILE).display().to_string(),
                ),
                (
                    "PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_CERT_PATH",
                    tls.join(RUNTIME_ORCHESTRATION_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_KEY_PATH",
                    tls.join(RUNTIME_ORCHESTRATION_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            Vec::new(),
        ),
        RuntimeLaunchSpec::new(
            "capability-native",
            binaries["platform-capability-native-worker"].clone(),
            &loopback_address(profile.ports.capability_native_observability),
            vec![
                (
                    "PLATFORM_CAPABILITY_NATIVE_WORKER_CONFIG",
                    config_path(&configuration, RUNTIME_CAPABILITY_NATIVE_CONFIG_FILE),
                ),
                (
                    "PLATFORM_CAPABILITY_NATIVE_WORKER_CONFIG_DIGEST",
                    profile.config_digests["capability-native"].clone(),
                ),
                (
                    "PLATFORM_CAPABILITY_NATIVE_WORKER_DATABASE_URL",
                    database_url.to_owned(),
                ),
            ],
            Vec::new(),
        ),
        RuntimeLaunchSpec::new(
            "registry-validation",
            binaries["platform-registry-validation-worker"].clone(),
            &loopback_address(profile.ports.registry_validation_observability),
            vec![
                (
                    "PLATFORM_REGISTRY_VALIDATION_WORKER_CONFIG",
                    config_path(&configuration, RUNTIME_REGISTRY_VALIDATION_CONFIG_FILE),
                ),
                (
                    "PLATFORM_REGISTRY_VALIDATION_WORKER_CONFIG_DIGEST",
                    profile.config_digests["registry-validation"].clone(),
                ),
                (
                    "PLATFORM_REGISTRY_VALIDATION_WORKER_DATABASE_URL",
                    database_url.to_owned(),
                ),
            ],
            Vec::new(),
        ),
        RuntimeLaunchSpec::new(
            "gateway-management",
            binaries["platform-gateway"].clone(),
            &loopback_address(profile.ports.gateway_management),
            vec![
                (
                    "PLATFORM_GATEWAY_CONFIG",
                    config_path(&configuration, RUNTIME_GATEWAY_MANAGEMENT_CONFIG_FILE),
                ),
                (
                    "PLATFORM_GATEWAY_CONFIG_DIGEST",
                    profile.config_digests["gateway-management"].clone(),
                ),
                ("PLATFORM_GATEWAY_DATABASE_URL", database_url.to_owned()),
                (
                    "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_PATH",
                    runtime.join(RUNTIME_CURSOR_KEY_FILE).display().to_string(),
                ),
                (
                    "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_DIGEST",
                    cursor_key_digest(&runtime.join(RUNTIME_CURSOR_KEY_FILE))?,
                ),
            ],
            Vec::new(),
        ),
        RuntimeLaunchSpec::new(
            "gateway-runtime",
            binaries["platform-gateway"].clone(),
            &loopback_address(profile.ports.gateway_runtime),
            vec![
                (
                    "PLATFORM_GATEWAY_CONFIG",
                    config_path(&configuration, RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE),
                ),
                (
                    "PLATFORM_GATEWAY_CONFIG_DIGEST",
                    profile.config_digests["gateway-runtime"].clone(),
                ),
                ("PLATFORM_GATEWAY_DATABASE_URL", database_url.to_owned()),
                (
                    "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_PATH",
                    runtime.join(RUNTIME_CURSOR_KEY_FILE).display().to_string(),
                ),
                (
                    "PLATFORM_GATEWAY_RUN_EVENT_CURSOR_KEY_DIGEST",
                    cursor_key_digest(&runtime.join(RUNTIME_CURSOR_KEY_FILE))?,
                ),
                (
                    "PLATFORM_GATEWAY_ARTIFACT_CA_PATH",
                    tls.join(RUNTIME_CA_CERTIFICATE_FILE).display().to_string(),
                ),
                (
                    "PLATFORM_GATEWAY_ARTIFACT_CERT_PATH",
                    tls.join(RUNTIME_GATEWAY_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_GATEWAY_ARTIFACT_KEY_PATH",
                    tls.join(RUNTIME_GATEWAY_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            Vec::new(),
        ),
    ];
    if selected_profile.has_features() {
        specs.extend(full_profile_launch_specs(
            binary_directory,
            runtime,
            profile,
            selected_profile,
            database_url,
            &common_aws,
        )?);
    }
    let observed_specs = specs
        .iter()
        .map(|spec| (spec.role.to_owned(), spec.ready_address.clone()))
        .collect::<BTreeMap<_, _>>();
    if observed_specs != binding.expected_processes {
        return Err(CliError::RuntimeState(
            "runtime launch specs do not match the selected process closure".to_owned(),
        ));
    }
    let process_path = runtime.join(RUNTIME_PROCESS_STATE_FILE);
    for spec in specs {
        let role = spec.role.to_owned();
        let record = spawn_runtime_process(&logs, &spec, |record| {
            if state.processes.contains_key(&role) {
                return Err(CliError::RuntimeState(format!(
                    "duplicate runtime launch role {role}"
                )));
            }
            state.processes.insert(role.clone(), record.clone());
            write_runtime_json_replace(&process_path, state)
        })?;
        wait_for_ready(&record)?;
    }
    state.lifecycle = RuntimeProcessLifecycle::Running;
    write_runtime_json_replace(&process_path, state)?;
    Ok(())
}

fn abort_runtime_start(
    runtime: &Path,
    state: &mut RuntimeProcessState,
    cause: CliError,
) -> CliError {
    state.lifecycle = RuntimeProcessLifecycle::Starting;
    let process_path = runtime.join(RUNTIME_PROCESS_STATE_FILE);
    let journal_error = write_runtime_json_replace(&process_path, state).err();
    let mut stop_errors = Vec::new();
    for process in state.processes.values() {
        if let Err(error) = stop_process(process) {
            stop_errors.push(error.to_string());
        }
    }
    if !stop_errors.is_empty() {
        if let Some(error) = journal_error {
            stop_errors.push(format!("persist recovery journal: {error}"));
        }
        return CliError::RuntimeUnavailable(format!(
            "{cause}; start recovery is incomplete and the process journal was retained: {}",
            stop_errors.join("; ")
        ));
    }
    state.processes.clear();
    state.lifecycle = RuntimeProcessLifecycle::Stopped;
    if let Err(error) = write_runtime_json_replace(&process_path, state) {
        let journal_detail = journal_error
            .map(|journal| format!("; recovery journal also failed: {journal}"))
            .unwrap_or_default();
        return CliError::RuntimeUnavailable(format!(
            "{cause}; started processes were stopped but the stopped journal could not be persisted: {error}{journal_detail}"
        ));
    }
    cause
}

fn full_profile_launch_specs(
    binary_directory: &Path,
    runtime: &Path,
    profile: &RuntimeProfileState,
    selected_profile: DevProfile,
    database_url: &str,
    common_aws: &[(&str, &str)],
) -> Result<Vec<RuntimeLaunchSpec>, CliError> {
    let configuration = runtime.join(RUNTIME_CONFIGURATION_DIRECTORY);
    let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
    let launches = full_profile::initial_process_launches(
        full_profile::ProcessPaths {
            release: binary_directory,
            configuration: &configuration,
            tls: &tls,
            ca_certificate_file: RUNTIME_CA_CERTIFICATE_FILE,
            nats_client_certificate_file: RUNTIME_NATS_CLIENT_CERTIFICATE_FILE,
            nats_client_private_key_file: RUNTIME_NATS_CLIENT_PRIVATE_KEY_FILE,
        },
        &profile.ports.full,
        &profile.config_digests,
        selected_profile,
        database_url,
        common_aws,
    )
    .map_err(CliError::RuntimeState)?
    .into_iter()
    .map(|launch| RuntimeLaunchSpec {
        role: launch.role,
        binary: launch.binary,
        ready_address: launch.ready_address,
        environment: launch.environment,
        extra_environment: launch.extra_environment,
    })
    .collect::<Vec<_>>();
    Ok(launches)
}

struct RuntimeLaunchSpec {
    role: &'static str,
    binary: PathBuf,
    ready_address: String,
    environment: Vec<(&'static str, String)>,
    extra_environment: Vec<(String, String)>,
}

impl RuntimeLaunchSpec {
    fn new(
        role: &'static str,
        binary: PathBuf,
        ready_address: &str,
        environment: Vec<(&'static str, String)>,
        extra_environment: Vec<(String, String)>,
    ) -> Self {
        Self {
            role,
            binary,
            ready_address: ready_address.to_owned(),
            environment,
            extra_environment,
        }
    }
}

fn config_path(configuration: &Path, name: &str) -> String {
    configuration.join(name).display().to_string()
}

fn cursor_key_digest(path: &Path) -> Result<String, CliError> {
    let key = read_bounded_identity_file(path)?;
    if key.len() != 32 {
        return Err(CliError::RuntimeState(
            "local run-event cursor key must be exactly 32 bytes".to_owned(),
        ));
    }
    Ok(format!("sha256:{}", lower_hex(&Sha256::digest(key))))
}

fn spawn_runtime_process<F>(
    logs: &Path,
    spec: &RuntimeLaunchSpec,
    before_exec: F,
) -> Result<RuntimeProcessRecord, CliError>
where
    F: FnOnce(&RuntimeProcessRecord) -> Result<(), CliError>,
{
    let log_file = format!("{}/{}.log", RUNTIME_LOG_DIRECTORY, spec.role);
    let path = logs.join(format!("{}.log", spec.role));
    let stdout = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })?;
    let stderr = stdout
        .try_clone()
        .map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })?;
    let generation = format!("{RUNTIME_PROCESS_GENERATION_PREFIX}{}", Uuid::now_v7());
    let mut command = ProcessCommand::new(&spec.binary);
    #[cfg(unix)]
    command.arg0(&generation);
    command
        .envs(spec.environment.iter().map(|(key, value)| (*key, value)))
        .envs(
            spec.extra_environment
                .iter()
                .map(|(key, value)| (key, value)),
        )
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    #[cfg(unix)]
    {
        command.process_group(0);
        let (mut parent_gate, child_gate) = UnixStream::pair().map_err(|error| {
            CliError::RuntimeUnavailable(format!("create {} start gate: {error}", spec.role))
        })?;
        let parent_fd = parent_gate.as_raw_fd();
        let child_fd = child_gate.as_raw_fd();
        // SAFETY: after fork this closure only invokes async-signal-safe libc operations. The
        // parent runs `spawn` on a helper thread because `Command::spawn` waits for pre-exec to
        // finish; the main thread journals the reported PID before sending the one-byte GO.
        unsafe {
            command.pre_exec(move || {
                libc::close(parent_fd);
                let pid = libc::getpid();
                let pid_bytes = std::slice::from_raw_parts(
                    (&raw const pid).cast::<u8>(),
                    std::mem::size_of::<libc::pid_t>(),
                );
                let mut written = 0;
                while written < pid_bytes.len() {
                    let result = libc::write(
                        child_fd,
                        pid_bytes[written..].as_ptr().cast(),
                        pid_bytes.len() - written,
                    );
                    if result < 0 {
                        let error = std::io::Error::last_os_error();
                        if error.raw_os_error() == Some(libc::EINTR) {
                            continue;
                        }
                        return Err(error);
                    }
                    if result == 0 {
                        return Err(std::io::Error::from_raw_os_error(libc::EIO));
                    }
                    written += result as usize;
                }
                let mut go = 0_u8;
                loop {
                    let result = libc::read(child_fd, (&raw mut go).cast(), 1);
                    if result < 0 {
                        let error = std::io::Error::last_os_error();
                        if error.raw_os_error() == Some(libc::EINTR) {
                            continue;
                        }
                        return Err(error);
                    }
                    if result == 0 || go != 1 {
                        return Err(std::io::Error::from_raw_os_error(libc::ECANCELED));
                    }
                    break;
                }
                libc::close(child_fd);
                Ok(())
            });
        }
        std::thread::scope(|scope| {
            let spawn = scope.spawn(move || {
                let _child_gate = child_gate;
                command.spawn()
            });
            let mut pid_bytes = [0_u8; std::mem::size_of::<libc::pid_t>()];
            if let Err(handshake_error) = parent_gate.read_exact(&mut pid_bytes) {
                drop(parent_gate);
                let spawned = spawn.join().map_err(|_| {
                    CliError::RuntimeUnavailable(format!(
                        "start {}: start gate thread panicked",
                        spec.role
                    ))
                })?;
                return match spawned {
                    Err(error) => Err(CliError::RuntimeUnavailable(format!(
                        "start {}: {error}",
                        spec.role
                    ))),
                    Ok(_) => Err(CliError::RuntimeUnavailable(format!(
                        "start {}: PID gate failed: {handshake_error}",
                        spec.role
                    ))),
                };
            }
            let raw_pid = libc::pid_t::from_ne_bytes(pid_bytes);
            let pid = u32::try_from(raw_pid).map_err(|_| {
                CliError::RuntimeUnavailable(format!(
                    "start {}: child reported an invalid PID",
                    spec.role
                ))
            })?;
            let record = RuntimeProcessRecord {
                pid,
                generation,
                ready_address: spec.ready_address.clone(),
                log_file,
            };
            if let Err(error) = before_exec(&record) {
                drop(parent_gate);
                let _ = spawn.join();
                return Err(error);
            }
            if let Err(error) = parent_gate.write_all(&[1]) {
                drop(parent_gate);
                let _ = spawn.join();
                return Err(CliError::RuntimeUnavailable(format!(
                    "release {} after persisting its start journal: {error}",
                    spec.role
                )));
            }
            drop(parent_gate);
            let child = spawn
                .join()
                .map_err(|_| {
                    CliError::RuntimeUnavailable(format!(
                        "start {}: start gate thread panicked",
                        spec.role
                    ))
                })?
                .map_err(|error| {
                    CliError::RuntimeUnavailable(format!("start {}: {error}", spec.role))
                })?;
            if child.id() != record.pid {
                return Err(CliError::RuntimeUnavailable(format!(
                    "start {}: PID gate reported a different process",
                    spec.role
                )));
            }
            Ok(record)
        })
    }
    #[cfg(not(unix))]
    {
        let _ = (command, before_exec, generation, log_file);
        Err(CliError::RuntimeUnavailable(
            "local process supervision is currently supported on Unix hosts".to_owned(),
        ))
    }
}

fn wait_for_ready(process: &RuntimeProcessRecord) -> Result<(), CliError> {
    let deadline = SystemTime::now() + Duration::from_secs(30);
    while SystemTime::now() < deadline {
        match observe_runtime_process(process)? {
            RuntimeProcessObservation::Owned => {}
            RuntimeProcessObservation::Stopped => {
                return Err(CliError::RuntimeUnavailable(format!(
                    "{} exited before readiness; inspect `insight logs --role {}`",
                    process.log_file,
                    process
                        .log_file
                        .trim_end_matches(".log")
                        .rsplit('/')
                        .next()
                        .unwrap_or("unknown")
                )));
            }
            RuntimeProcessObservation::IdentityMismatch => {
                return Err(CliError::RuntimeUnavailable(format!(
                    "process {} no longer matches its recorded generation",
                    process.pid
                )));
            }
        }
        if http_ready(&process.ready_address) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    Err(CliError::RuntimeUnavailable(format!(
        "{} did not become ready within 30 seconds; inspect `insight logs`",
        process.log_file
    )))
}

fn http_ready(address: &str) -> bool {
    let Ok(address) = address.parse() else {
        return false;
    };
    let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(300)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(300)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(300)));
    if stream
        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut response = [0_u8; 64];
    stream
        .read(&mut response)
        .is_ok_and(|length| response[..length].starts_with(b"HTTP/1.1 200"))
}

fn render_runtime_status(
    state: &RuntimeProcessState,
    binding: &RuntimeProcessBinding,
) -> Result<String, CliError> {
    let complete = state.lifecycle != RuntimeProcessLifecycle::Starting;
    let mut output = format!(
        "profile={} lifecycle={} complete={} environment=single-node-development production=false L4=not_run L5=not_run L6=not_run\n",
        state.profile,
        match state.lifecycle {
            RuntimeProcessLifecycle::Starting => "starting",
            RuntimeProcessLifecycle::Running => "running",
            RuntimeProcessLifecycle::Stopped => "stopped",
        },
        complete,
    );
    for (role, expected_address) in &binding.expected_processes {
        if let Some(process) = state.processes.get(role) {
            let status = match observe_runtime_process(process)? {
                RuntimeProcessObservation::Owned if http_ready(&process.ready_address) => "ready",
                RuntimeProcessObservation::Owned => "starting_or_unready",
                RuntimeProcessObservation::Stopped => "stopped",
                RuntimeProcessObservation::IdentityMismatch => "identity_mismatch",
            };
            output.push_str(&format!(
                "{status:20} {role:22} pid={} readiness={}\n",
                process.pid, process.ready_address
            ));
        } else if state.lifecycle == RuntimeProcessLifecycle::Starting {
            output.push_str(&format!(
                "{:<20} {role:22} pid=- readiness={expected_address}\n",
                "not_started"
            ));
        }
    }
    if state.lifecycle == RuntimeProcessLifecycle::Stopped {
        output.push_str("stopped                 selected profile has no running role\n");
    }
    Ok(output)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeProcessObservation {
    Stopped,
    Owned,
    IdentityMismatch,
}

fn observe_runtime_process(
    process: &RuntimeProcessRecord,
) -> Result<RuntimeProcessObservation, CliError> {
    let Some(argv0) = process_argv0(process.pid)? else {
        return Ok(RuntimeProcessObservation::Stopped);
    };
    if argv0 == process.generation {
        Ok(RuntimeProcessObservation::Owned)
    } else {
        Ok(RuntimeProcessObservation::IdentityMismatch)
    }
}

fn process_argv0(pid: u32) -> Result<Option<String>, CliError> {
    #[cfg(target_os = "linux")]
    {
        let path = PathBuf::from(format!("/proc/{pid}/cmdline"));
        return match fs::read(&path) {
            Ok(bytes) => {
                let argv0 = bytes.split(|byte| *byte == 0).next().unwrap_or_default();
                let argv0 = std::str::from_utf8(argv0).map_err(|_| {
                    CliError::RuntimeUnavailable(format!(
                        "inspect process {pid} generation: argv0 is not UTF-8"
                    ))
                })?;
                Ok(Some(argv0.to_owned()))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(CliError::RuntimeUnavailable(format!(
                "inspect process {pid} generation: {error}"
            ))),
        };
    }
    #[cfg(target_os = "macos")]
    {
        let output = ProcessCommand::new("/bin/ps")
            .args(["-ww", "-p", &pid.to_string(), "-o", "command="])
            .output()
            .map_err(|error| {
                CliError::RuntimeUnavailable(format!("inspect process {pid} generation: {error}"))
            })?;
        if !output.status.success() {
            if output.stdout.is_empty() && output.stderr.is_empty() {
                return Ok(None);
            }
            let detail = String::from_utf8_lossy(&output.stderr)
                .trim()
                .chars()
                .take(256)
                .collect::<String>();
            return Err(CliError::RuntimeUnavailable(format!(
                "inspect process {pid} generation: ps rejected the query: {detail}"
            )));
        }
        let command = std::str::from_utf8(&output.stdout).map_err(|_| {
            CliError::RuntimeUnavailable(format!(
                "inspect process {pid} generation: argv0 is not UTF-8"
            ))
        })?;
        Ok(command.split_whitespace().next().map(str::to_owned))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = pid;
        Err(CliError::RuntimeUnavailable(
            "local process identity inspection is supported on macOS and Linux".to_owned(),
        ))
    }
}

fn stop_process(process: &RuntimeProcessRecord) -> Result<(), CliError> {
    #[cfg(unix)]
    {
        match observe_runtime_process(process)? {
            RuntimeProcessObservation::Stopped => return Ok(()),
            RuntimeProcessObservation::Owned => {}
            RuntimeProcessObservation::IdentityMismatch => {
                return Ok(());
            }
        }
        let status = ProcessCommand::new("kill")
            .args(["-TERM", &process.pid.to_string()])
            .status()
            .map_err(|error| {
                CliError::RuntimeUnavailable(format!("stop process {}: {error}", process.pid))
            })?;
        if !status.success() {
            return Err(CliError::RuntimeUnavailable(format!(
                "stop process {}: signal was rejected",
                process.pid
            )));
        }
        let deadline = SystemTime::now() + Duration::from_secs(10);
        while SystemTime::now() < deadline {
            match observe_runtime_process(process)? {
                RuntimeProcessObservation::Owned => {}
                RuntimeProcessObservation::Stopped
                | RuntimeProcessObservation::IdentityMismatch => return Ok(()),
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Err(CliError::RuntimeUnavailable(format!(
            "process {} did not stop after SIGTERM",
            process.pid
        )))
    }
    #[cfg(not(unix))]
    {
        let _ = process;
        Err(CliError::RuntimeUnavailable(
            "local process supervision is currently supported on macOS and Linux".to_owned(),
        ))
    }
}

fn run_external(mut command: ProcessCommand, purpose: &str) -> Result<String, CliError> {
    let output = command
        .output()
        .map_err(|error| CliError::RuntimeUnavailable(format!("{purpose}: {error}")))?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned());
    }
    let detail = String::from_utf8_lossy(&output.stderr)
        .lines()
        .chain(String::from_utf8_lossy(&output.stdout).lines())
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("command failed")
        .chars()
        .take(512)
        .collect::<String>();
    Err(CliError::RuntimeUnavailable(format!("{purpose}: {detail}")))
}

fn expected_runtime_closure(
    profile: &RuntimeProfileState,
    selected: DevProfile,
) -> ExpectedRuntimeClosure {
    let mut config_files = BTreeMap::from([
        (
            "artifact-bootstrap".to_owned(),
            RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE,
        ),
        (
            "artifact-data".to_owned(),
            RUNTIME_ARTIFACT_DATA_CONFIG_FILE,
        ),
        (
            "artifact-gateway".to_owned(),
            RUNTIME_ARTIFACT_GATEWAY_CONFIG_FILE,
        ),
        (
            "capability-native".to_owned(),
            RUNTIME_CAPABILITY_NATIVE_CONFIG_FILE,
        ),
        (
            "gateway-management".to_owned(),
            RUNTIME_GATEWAY_MANAGEMENT_CONFIG_FILE,
        ),
        (
            "gateway-runtime".to_owned(),
            RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE,
        ),
        (
            "orchestration".to_owned(),
            RUNTIME_ORCHESTRATION_CONFIG_FILE,
        ),
        (
            "registry-validation".to_owned(),
            RUNTIME_REGISTRY_VALIDATION_CONFIG_FILE,
        ),
    ]);
    let optional_configs = [
        ("context-native", full_profile::CONTEXT_NATIVE_CONFIG_FILE),
        (
            "security-authority",
            full_profile::SECURITY_AUTHORITY_CONFIG_FILE,
        ),
        ("egress-broker", full_profile::EGRESS_BROKER_CONFIG_FILE),
        ("model-worker", full_profile::MODEL_WORKER_CONFIG_FILE),
        ("context-remote", full_profile::CONTEXT_REMOTE_CONFIG_FILE),
        ("mcp-host", full_profile::MCP_HOST_CONFIG_FILE),
        (
            "mcp-resource-host",
            full_profile::MCP_RESOURCE_HOST_CONFIG_FILE,
        ),
        (
            "capability-remote",
            full_profile::CAPABILITY_REMOTE_CONFIG_FILE,
        ),
        ("mcp-discovery", full_profile::MCP_DISCOVERY_CONFIG_FILE),
        (
            "mcp-subscription",
            full_profile::MCP_SUBSCRIPTION_CONFIG_FILE,
        ),
        ("mcp-cleanup", full_profile::MCP_CLEANUP_CONFIG_FILE),
        (
            "context-subscription",
            full_profile::CONTEXT_SUBSCRIPTION_CONFIG_FILE,
        ),
        ("callback-api", full_profile::CALLBACK_API_CONFIG_FILE),
        ("context-dataset", full_profile::CONTEXT_DATASET_CONFIG_FILE),
    ];
    for (role, file) in optional_configs {
        if selected.includes_role(role) {
            config_files.insert(role.to_owned(), file);
        }
    }
    if selected.has_sandbox() {
        config_files.insert(
            "sandbox-kubernetes".to_owned(),
            RUNTIME_SANDBOX_KUBERNETES_CONFIG_FILE,
        );
    }

    let mut processes = BTreeMap::from([
        (
            "artifact-data".to_owned(),
            loopback_address(profile.ports.artifact_data_observability),
        ),
        (
            "artifact-gateway".to_owned(),
            loopback_address(profile.ports.artifact_gateway_observability),
        ),
        (
            "capability-native".to_owned(),
            loopback_address(profile.ports.capability_native_observability),
        ),
        (
            "gateway-management".to_owned(),
            loopback_address(profile.ports.gateway_management),
        ),
        (
            "gateway-runtime".to_owned(),
            loopback_address(profile.ports.gateway_runtime),
        ),
        (
            "orchestration".to_owned(),
            loopback_address(profile.ports.orchestration_observability),
        ),
        (
            "registry-validation".to_owned(),
            loopback_address(profile.ports.registry_validation_observability),
        ),
    ]);
    let optional_processes = [
        (
            "context-native",
            profile.ports.full.context_native_observability,
        ),
        (
            "security-authority",
            profile.ports.full.security_authority_observability,
        ),
        (
            "egress-broker",
            profile.ports.full.egress_broker_observability,
        ),
        (
            "model-worker",
            profile.ports.full.model_worker_observability,
        ),
        (
            "context-remote",
            profile.ports.full.remote_context_worker_observability,
        ),
        ("mcp-host", profile.ports.full.mcp_host_observability),
        (
            "mcp-resource-host",
            profile.ports.full.mcp_resource_host_observability,
        ),
        (
            "capability-remote",
            profile.ports.full.capability_remote_observability,
        ),
        (
            "mcp-discovery",
            profile.ports.full.mcp_discovery_observability,
        ),
        (
            "mcp-subscription",
            profile.ports.full.mcp_subscription_observability,
        ),
        ("mcp-cleanup", profile.ports.full.mcp_cleanup_observability),
        (
            "context-subscription",
            profile.ports.full.context_subscription_observability,
        ),
        ("callback-api", profile.ports.full.callback_api),
        (
            "context-dataset",
            profile.ports.full.context_dataset_observability,
        ),
    ];
    for (role, port) in optional_processes {
        if selected.includes_role(role) {
            processes.insert(role.to_owned(), loopback_address(port));
        }
    }
    ExpectedRuntimeClosure {
        config_files,
        processes,
    }
}

fn known_runtime_config_files() -> BTreeSet<&'static str> {
    BTreeSet::from([
        RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE,
        RUNTIME_ARTIFACT_DATA_CONFIG_FILE,
        RUNTIME_ARTIFACT_GATEWAY_CONFIG_FILE,
        RUNTIME_CAPABILITY_NATIVE_CONFIG_FILE,
        RUNTIME_GATEWAY_MANAGEMENT_CONFIG_FILE,
        RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE,
        RUNTIME_ORCHESTRATION_CONFIG_FILE,
        RUNTIME_REGISTRY_VALIDATION_CONFIG_FILE,
        RUNTIME_SANDBOX_KUBERNETES_CONFIG_FILE,
        full_profile::CONTEXT_NATIVE_CONFIG_FILE,
        full_profile::SECURITY_AUTHORITY_CONFIG_FILE,
        full_profile::EGRESS_BROKER_CONFIG_FILE,
        full_profile::MODEL_WORKER_CONFIG_FILE,
        full_profile::CONTEXT_REMOTE_CONFIG_FILE,
        full_profile::MCP_HOST_CONFIG_FILE,
        full_profile::MCP_RESOURCE_HOST_CONFIG_FILE,
        full_profile::CAPABILITY_REMOTE_CONFIG_FILE,
        full_profile::MCP_DISCOVERY_CONFIG_FILE,
        full_profile::MCP_SUBSCRIPTION_CONFIG_FILE,
        full_profile::MCP_CLEANUP_CONFIG_FILE,
        full_profile::CONTEXT_SUBSCRIPTION_CONFIG_FILE,
        full_profile::CALLBACK_API_CONFIG_FILE,
        full_profile::CONTEXT_DATASET_CONFIG_FILE,
    ])
}

fn runtime_profile_closure_digest(state: &RuntimeProfileState) -> Result<String, CliError> {
    canonical_digest(&serde_json::json!({
        "schema_version": state.schema_version,
        "kind": state.kind,
        "tenant_id": state.tenant_id,
        "identity_digest": state.identity_digest,
        "source_fingerprint": state.source_fingerprint,
        "features": state.features,
        "profile_digest": state.profile_digest,
        "release_identity": state.release_identity,
        "kms_key_arn": state.kms_key_arn,
        "secret_provider_id": state.secret_provider_id,
        "capability_protocol_profile_revision_id": state.capability_protocol_profile_revision_id,
        "secret_readiness_arn": state.secret_readiness_arn,
        "s3_bucket": state.s3_bucket,
        "ports": state.ports,
        "config_digests": state.config_digests,
        "tls_identity_digests": state.tls_identity_digests,
    }))
    .map_err(|error| CliError::RuntimeState(error.to_string()))
}

fn local_identity_digest(identity: &LocalIdentityState) -> Result<String, CliError> {
    let value = serde_json::to_value(identity)
        .map_err(|error| CliError::RuntimeState(error.to_string()))?;
    canonical_digest(&value).map_err(|error| CliError::RuntimeState(error.to_string()))
}

fn refresh_runtime_profile_closure_digest(state: &mut RuntimeProfileState) -> Result<(), CliError> {
    state.closure_digest = runtime_profile_closure_digest(state)?;
    Ok(())
}

fn read_runtime_profile_state(
    runtime: &Path,
    identity: &LocalIdentityState,
) -> Result<Option<RuntimeProfileState>, CliError> {
    let path = runtime.join(RUNTIME_PROFILE_STATE_FILE);
    let Some(state) = read_runtime_json::<RuntimeProfileState>(&path)? else {
        return Ok(None);
    };
    validate_runtime_profile_state(&path, &state, identity)?;
    Ok(Some(state))
}

fn read_runtime_profile_state_for_cleanup(
    runtime: &Path,
    identity: &LocalIdentityState,
) -> Result<Option<RuntimeProfileState>, CliError> {
    let path = runtime.join(RUNTIME_PROFILE_STATE_FILE);
    let Some(state) = read_runtime_json::<RuntimeProfileState>(&path)? else {
        return Ok(None);
    };
    validate_runtime_profile_state_for_cleanup(&path, &state, identity)?;
    Ok(Some(state))
}

fn validate_runtime_profile_state(
    path: &Path,
    state: &RuntimeProfileState,
    identity: &LocalIdentityState,
) -> Result<DevProfile, CliError> {
    validate_runtime_profile_state_inner(path, state, identity, true)
}

fn validate_runtime_profile_state_for_cleanup(
    path: &Path,
    state: &RuntimeProfileState,
    identity: &LocalIdentityState,
) -> Result<DevProfile, CliError> {
    validate_runtime_profile_state_inner(path, state, identity, false)
}

fn validate_runtime_profile_state_inner(
    path: &Path,
    state: &RuntimeProfileState,
    identity: &LocalIdentityState,
    validate_config_files: bool,
) -> Result<DevProfile, CliError> {
    let invalid = |detail: &str| {
        CliError::RuntimeState(format!(
            "{} is not a valid current runtime profile: {detail}",
            path.display()
        ))
    };
    if state.schema_version != RUNTIME_PROFILE_SCHEMA_VERSION {
        return Err(CliError::RuntimeState(format!(
            "{} has unsupported runtime profile schema_version {}; expected {}",
            path.display(),
            state.schema_version,
            RUNTIME_PROFILE_SCHEMA_VERSION
        )));
    }
    if state.kind != RUNTIME_PROFILE_KIND {
        return Err(CliError::RuntimeState(format!(
            "{} has unsupported runtime profile kind {:?}; expected {:?}",
            path.display(),
            state.kind,
            RUNTIME_PROFILE_KIND
        )));
    }
    let expected_identity_digest =
        local_identity_digest(identity).map_err(|_| invalid("local identity is not canonical"))?;
    if state.tenant_id != identity.tenant_id || state.identity_digest != expected_identity_digest {
        return Err(invalid(
            "tenant_id or identity_digest does not match the current local project identity",
        ));
    }
    if state
        .source_fingerprint
        .parse::<insight_platform_contracts::Sha256Digest>()
        .is_err()
    {
        return Err(invalid("source_fingerprint is not an exact SHA-256 digest"));
    }
    let from_source = if let Some(source) = state.release_identity.strip_prefix("source:") {
        if source != state.source_fingerprint {
            return Err(invalid(
                "source release_identity does not match source_fingerprint",
            ));
        }
        true
    } else if let Some(release) = state.release_identity.strip_prefix("release:") {
        let Some((version, digest)) = release.split_once(':') else {
            return Err(invalid("release_identity is malformed"));
        };
        semver::Version::parse(version)
            .ok()
            .filter(|parsed| {
                parsed.to_string() == version && parsed.pre.is_empty() && parsed.build.is_empty()
            })
            .ok_or_else(|| invalid("release_identity version is not an exact stable version"))?;
        if digest != state.source_fingerprint {
            return Err(invalid(
                "release bundle digest does not match source_fingerprint",
            ));
        }
        false
    } else {
        return Err(invalid("release_identity has an unsupported identity kind"));
    };
    let features = (!state.features.is_empty()).then(|| state.features.join(","));
    let selected_profile = DevProfile::parse(features.as_deref(), false, from_source)
        .map_err(|_| invalid("features are not a closed development feature set"))?;
    let canonical_features = selected_profile
        .feature_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if state.features != canonical_features {
        return Err(invalid("features are not in canonical order"));
    }
    if state
        .profile_digest
        .parse::<insight_platform_contracts::Sha256Digest>()
        .is_err()
    {
        return Err(invalid("profile_digest is not an exact SHA-256 digest"));
    }
    let expected_profile_digest = selected_profile
        .profile_digest(&state.release_identity)
        .map_err(|_| invalid("development profile registry is invalid"))?;
    if state.profile_digest != expected_profile_digest {
        return Err(invalid(
            "profile_digest does not match release_identity and features",
        ));
    }
    if !local_kms_key_arn_is_valid(&state.kms_key_arn) {
        return Err(invalid("kms_key_arn is not the exact local KMS key ARN"));
    }
    if state.secret_provider_id.kind() != ResourceKind::SecretProvider {
        return Err(invalid(
            "secret_provider_id is not a SecretProvider identity",
        ));
    }
    if state.capability_protocol_profile_revision_id.kind() != ResourceKind::PolicyRevision {
        return Err(invalid(
            "capability_protocol_profile_revision_id is not a PolicyRevision identity",
        ));
    }
    if !local_secret_readiness_arn_is_valid(&state.secret_readiness_arn) {
        return Err(invalid("secret_readiness_arn is empty or invalid"));
    }
    if state.s3_bucket != LOCAL_ARTIFACT_BUCKET {
        return Err(invalid("s3_bucket is not the local Artifact authority"));
    }
    if !runtime_ports_are_valid(&state.ports) {
        return Err(invalid("ports must be non-zero and globally unique"));
    }
    if state.config_digests.values().any(|digest| {
        digest
            .parse::<insight_platform_contracts::Sha256Digest>()
            .is_err()
    }) {
        return Err(invalid("config_digests contains an invalid digest"));
    }
    let closure = expected_runtime_closure(state, selected_profile);
    if state
        .config_digests
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != closure
            .config_files
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
    {
        return Err(invalid(
            "config_digests does not match the selected feature closure",
        ));
    }
    if state.tls_identity_digests.values().any(|digest| {
        digest
            .parse::<insight_platform_contracts::Sha256Digest>()
            .is_err()
    }) {
        return Err(invalid("tls_identity_digests contains an invalid digest"));
    }
    let mut expected_tls_identities = expected_local_tls_leaf_identities(selected_profile)
        .keys()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    expected_tls_identities.insert(RUNTIME_CA_CERTIFICATE_FILE.to_owned());
    if state
        .tls_identity_digests
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>()
        != expected_tls_identities
    {
        return Err(invalid(
            "tls_identity_digests does not match the selected feature closure",
        ));
    }
    if state
        .closure_digest
        .parse::<insight_platform_contracts::Sha256Digest>()
        .is_err()
        || runtime_profile_closure_digest(state).ok().as_deref()
            != Some(state.closure_digest.as_str())
    {
        return Err(invalid(
            "closure_digest does not match the runtime profile closure",
        ));
    }
    if !validate_config_files {
        return Ok(selected_profile);
    }
    let configuration = path
        .parent()
        .ok_or_else(|| invalid("profile path has no runtime parent"))?
        .join(RUNTIME_CONFIGURATION_DIRECTORY);
    let configuration_metadata = fs::symlink_metadata(&configuration)
        .map_err(|_| invalid("runtime config directory is unavailable"))?;
    if !configuration_metadata.file_type().is_dir() {
        return Err(invalid(
            "runtime config directory is not a physical directory",
        ));
    }
    let known_files = known_runtime_config_files();
    let entries = fs::read_dir(&configuration)
        .map_err(|_| invalid("runtime config directory is unavailable"))?;
    for entry in entries {
        let entry = entry.map_err(|_| invalid("runtime config directory is unreadable"))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| invalid("runtime config filename is not UTF-8"))?;
        if !known_files.contains(name.as_str()) {
            return Err(invalid(&format!(
                "runtime config directory contains unexpected file {name:?}"
            )));
        }
        canonical_runtime_config_digest(&entry.path(), &invalid)?;
    }
    for (role, file) in &closure.config_files {
        let expected = state
            .config_digests
            .get(role)
            .ok_or_else(|| invalid("selected config digest is unavailable"))?;
        let observed = canonical_runtime_config_digest(&configuration.join(file), &invalid)?;
        if &observed != expected {
            return Err(invalid(&format!(
                "config_digests[{role:?}] does not match {}",
                configuration.join(file).display()
            )));
        }
    }
    let observed_tls = inspect_local_tls_identity_closure(
        &path
            .parent()
            .ok_or_else(|| invalid("profile path has no runtime parent"))?
            .join(RUNTIME_TLS_DIRECTORY),
        selected_profile,
    )
    .map_err(|reason| invalid(&format!("runtime TLS identity is invalid: {reason}")))?;
    if observed_tls != state.tls_identity_digests {
        return Err(invalid(
            "tls_identity_digests does not match the runtime TLS identity closure",
        ));
    }
    Ok(selected_profile)
}

fn canonical_runtime_config_digest(
    path: &Path,
    invalid: &impl Fn(&str) -> CliError,
) -> Result<String, CliError> {
    const MAX_RUNTIME_CONFIG_BYTES: u64 = 4 * 1024 * 1024;
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| invalid(&format!("cannot read runtime config {}", path.display())))?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_RUNTIME_CONFIG_BYTES {
        return Err(invalid(&format!(
            "runtime config {} is not a bounded regular file",
            path.display()
        )));
    }
    let bytes = fs::read(path)
        .map_err(|_| invalid(&format!("cannot read runtime config {}", path.display())))?;
    let value = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: MAX_RUNTIME_CONFIG_BYTES as usize,
            max_depth: 64,
            max_properties_per_object: 4_096,
            max_items_per_array: 4_096,
            max_string_bytes: 1_048_576,
        },
    )
    .map_err(|_| {
        invalid(&format!(
            "runtime config {} is not strict JSON",
            path.display()
        ))
    })?;
    canonical_digest(&value).map_err(|_| {
        invalid(&format!(
            "runtime config {} is not canonical",
            path.display()
        ))
    })
}

fn runtime_process_binding(
    runtime: &Path,
    tenant_id: &str,
    compose_project: &str,
    profile: &RuntimeProfileState,
    identity: &LocalIdentityState,
) -> Result<RuntimeProcessBinding, CliError> {
    let selected = validate_runtime_profile_state(
        &runtime.join(RUNTIME_PROFILE_STATE_FILE),
        profile,
        identity,
    )?;
    Ok(runtime_process_binding_from_selected(
        tenant_id,
        compose_project,
        profile,
        selected,
    ))
}

fn runtime_process_binding_for_cleanup(
    runtime: &Path,
    tenant_id: &str,
    compose_project: &str,
    profile: &RuntimeProfileState,
    identity: &LocalIdentityState,
) -> Result<RuntimeProcessBinding, CliError> {
    let selected = validate_runtime_profile_state_for_cleanup(
        &runtime.join(RUNTIME_PROFILE_STATE_FILE),
        profile,
        identity,
    )?;
    Ok(runtime_process_binding_from_selected(
        tenant_id,
        compose_project,
        profile,
        selected,
    ))
}

fn runtime_process_binding_from_selected(
    tenant_id: &str,
    compose_project: &str,
    profile: &RuntimeProfileState,
    selected: DevProfile,
) -> RuntimeProcessBinding {
    let expected_processes = expected_runtime_closure(profile, selected).processes;
    RuntimeProcessBinding {
        tenant_id: tenant_id.to_owned(),
        profile: selected.label(),
        profile_digest: profile.profile_digest.clone(),
        release_identity: profile.release_identity.clone(),
        compose_project: compose_project.to_owned(),
        source_fingerprint: profile.source_fingerprint.clone(),
        expected_processes,
    }
}

fn read_runtime_process_state(
    runtime: &Path,
    expected: &RuntimeProcessBinding,
) -> Result<Option<RuntimeProcessState>, CliError> {
    let path = runtime.join(RUNTIME_PROCESS_STATE_FILE);
    let Some(state) = read_runtime_json::<RuntimeProcessState>(&path)? else {
        return Ok(None);
    };
    if state.schema_version != RUNTIME_PROCESS_SCHEMA_VERSION {
        return Err(CliError::RuntimeState(format!(
            "{} has unsupported runtime process schema_version {}; expected {}",
            path.display(),
            state.schema_version,
            RUNTIME_PROCESS_SCHEMA_VERSION
        )));
    }
    if state.kind != RUNTIME_PROCESS_KIND {
        return Err(CliError::RuntimeState(format!(
            "{} has unsupported runtime process kind {:?}; expected {:?}",
            path.display(),
            state.kind,
            RUNTIME_PROCESS_KIND
        )));
    }
    if state.tenant_id != expected.tenant_id
        || state.profile != expected.profile
        || state.profile_digest != expected.profile_digest
        || state.release_identity != expected.release_identity
        || state.compose_project != expected.compose_project
        || state.source_fingerprint != expected.source_fingerprint
    {
        return Err(CliError::RuntimeState(format!(
            "{} does not match the current tenant/profile/release/source binding",
            path.display()
        )));
    }
    let mut pids = BTreeSet::new();
    let mut generations = BTreeSet::new();
    let mut ready_addresses = BTreeSet::new();
    for (role, process) in &state.processes {
        let valid_role = !role.is_empty()
            && role.len() <= 64
            && role
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
        let valid_log_file = process.log_file == format!("{RUNTIME_LOG_DIRECTORY}/{role}.log");
        let valid_ready_address = expected
            .expected_processes
            .get(role)
            .is_some_and(|address| address == &process.ready_address);
        if process.pid == 0
            || !valid_role
            || !valid_log_file
            || !valid_ready_address
            || !runtime_process_generation_is_valid(&process.generation)
            || !pids.insert(process.pid)
            || !generations.insert(&process.generation)
            || !ready_addresses.insert(&process.ready_address)
        {
            return Err(CliError::RuntimeState(format!(
                "{} contains an invalid or duplicate process identity",
                path.display()
            )));
        }
    }
    let actual_roles = state.processes.keys().cloned().collect::<BTreeSet<_>>();
    let expected_roles = expected
        .expected_processes
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    match state.lifecycle {
        RuntimeProcessLifecycle::Starting if actual_roles.is_subset(&expected_roles) => {}
        RuntimeProcessLifecycle::Running if actual_roles == expected_roles => {}
        RuntimeProcessLifecycle::Stopped if actual_roles.is_empty() => {}
        RuntimeProcessLifecycle::Starting => {
            return Err(CliError::RuntimeState(format!(
                "{} starting journal is not a subset of the selected process closure",
                path.display()
            )));
        }
        RuntimeProcessLifecycle::Running => {
            return Err(CliError::RuntimeState(format!(
                "{} running state does not contain the exact selected process closure",
                path.display()
            )));
        }
        RuntimeProcessLifecycle::Stopped => {
            return Err(CliError::RuntimeState(format!(
                "{} stopped state must not contain process records",
                path.display()
            )));
        }
    }
    Ok(Some(state))
}

fn runtime_process_generation_is_valid(value: &str) -> bool {
    value
        .strip_prefix(RUNTIME_PROCESS_GENERATION_PREFIX)
        .and_then(|value| Uuid::parse_str(value).ok().map(|uuid| (value, uuid)))
        .is_some_and(|(value, uuid)| {
            uuid.get_version_num() == 7 && value == uuid.hyphenated().to_string()
        })
}

fn read_runtime_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, CliError> {
    const MAX_RUNTIME_STATE_BYTES: u64 = 4 * 1024 * 1024;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(CliError::InitializeProject {
                path: path.display().to_string(),
                source,
            });
        }
    };
    if !metadata.file_type().is_file() || metadata.len() > MAX_RUNTIME_STATE_BYTES {
        return Err(CliError::RuntimeState(format!(
            "{} is not a bounded regular runtime state file",
            path.display()
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })?;
    let opened_metadata = file
        .metadata()
        .map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })?;
    if !opened_metadata.file_type().is_file()
        || opened_metadata.len() > MAX_RUNTIME_STATE_BYTES
        || {
            #[cfg(unix)]
            {
                opened_metadata.nlink() != 1
                    || opened_metadata.dev() != metadata.dev()
                    || opened_metadata.ino() != metadata.ino()
            }
            #[cfg(not(unix))]
            {
                false
            }
        }
    {
        return Err(CliError::RuntimeState(format!(
            "{} is not a bounded single-link runtime state file",
            path.display()
        )));
    }
    let mut bytes = Vec::with_capacity(opened_metadata.len() as usize);
    file.take(MAX_RUNTIME_STATE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })?;
    if bytes.len() as u64 > MAX_RUNTIME_STATE_BYTES {
        return Err(CliError::RuntimeState(format!(
            "{} exceeds the runtime state size limit",
            path.display()
        )));
    }
    let value = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: MAX_RUNTIME_STATE_BYTES as usize,
            max_depth: 64,
            max_properties_per_object: 4_096,
            max_items_per_array: 4_096,
            max_string_bytes: 1_048_576,
        },
    )
    .map_err(|_| CliError::RuntimeState(format!("{} is not valid closed JSON", path.display())))?;
    serde_json::from_value(value)
        .map(Some)
        .map_err(|_| CliError::RuntimeState(format!("{} is not valid closed JSON", path.display())))
}

fn write_runtime_json_replace<T: Serialize>(path: &Path, value: &T) -> Result<(), CliError> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| CliError::RuntimeState(error.to_string()))?;
    let temporary = path.with_extension(format!("{}.tmp", Uuid::now_v7()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|source| CliError::InitializeProject {
                path: temporary.display().to_string(),
                source,
            })?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|source| CliError::InitializeProject {
                path: temporary.display().to_string(),
                source,
            })?;
        fs::rename(&temporary, path).map_err(|source| CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        })?;
        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|source| CliError::InitializeProject {
                    path: parent.display().to_string(),
                    source,
                })?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn remove_runtime_state_file(path: &Path) -> Result<(), CliError> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(CliError::InitializeProject {
                path: path.display().to_string(),
                source,
            });
        }
    }
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| CliError::InitializeProject {
                path: parent.display().to_string(),
                source,
            })?;
    }
    Ok(())
}

fn workspace_fingerprint(workspace: &Path) -> Result<String, CliError> {
    let mut paths = Vec::new();
    for relative in [
        "Cargo.toml",
        "Cargo.lock",
        "crates",
        "contracts",
        "proto",
        "deploy/dev",
    ] {
        collect_fingerprint_paths(workspace, Path::new(relative), &mut paths)?;
    }
    paths.sort();
    let mut hasher = Sha256::new();
    for relative in paths {
        let bytes = fs::read(workspace.join(&relative))
            .map_err(|source| CliError::WorkspaceUnavailable(source.to_string()))?;
        hasher.update(relative.as_os_str().as_encoded_bytes());
        hasher.update([0]);
        hasher.update(bytes);
        hasher.update([b'\n']);
    }
    Ok(format!("sha256:{}", lower_hex(&hasher.finalize())))
}

fn collect_fingerprint_paths(
    workspace: &Path,
    relative: &Path,
    paths: &mut Vec<PathBuf>,
) -> Result<(), CliError> {
    let path = workspace.join(relative);
    let metadata =
        fs::metadata(&path).map_err(|source| CliError::WorkspaceUnavailable(source.to_string()))?;
    if metadata.is_file() {
        paths.push(relative.to_owned());
        return Ok(());
    }
    let mut entries = fs::read_dir(path)
        .map_err(|source| CliError::WorkspaceUnavailable(source.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| CliError::WorkspaceUnavailable(source.to_string()))?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name();
        if name == "target" || name == ".git" {
            continue;
        }
        let child = relative.join(name);
        let metadata = entry
            .metadata()
            .map_err(|source| CliError::WorkspaceUnavailable(source.to_string()))?;
        if metadata.is_dir() {
            collect_fingerprint_paths(workspace, &child, paths)?;
        } else if metadata.is_file() {
            paths.push(child);
        }
    }
    Ok(())
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
    let state: LocalProjectState =
        serde_json::from_slice(&bytes).map_err(|_| CliError::InvalidLocalIdentity {
            path: state_directory.display().to_string(),
        })?;
    if state.schema_version != 1 || state.kind != PROJECT_KIND {
        return Err(CliError::InvalidLocalIdentity {
            path: state_directory.display().to_string(),
        });
    }
    Ok(state)
}

fn validate_loaded_local_identity(
    state_directory: &Path,
    identity: &LocalIdentityState,
) -> Result<(), CliError> {
    validate_loaded_local_identity_for_cleanup(state_directory, identity)?;
    let invalid = || CliError::InvalidLocalIdentity {
        path: state_directory.display().to_string(),
    };

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

fn validate_loaded_local_identity_for_cleanup(
    state_directory: &Path,
    identity: &LocalIdentityState,
) -> Result<(), CliError> {
    let invalid = || CliError::InvalidLocalIdentity {
        path: state_directory.display().to_string(),
    };
    if identity.schema_version != 3
        || identity.issuer.is_empty()
        || identity.audience != LOCAL_OIDC_AUDIENCE
        || identity.key_id.is_empty()
        || identity.developer_subject.is_empty()
        || ResourceId::parse_expected(
            &identity.registry_validator_principal_id,
            ResourceKind::Principal,
        )
        .is_err()
        || ResourceId::parse_expected(
            &identity.egress_broker_principal_id,
            ResourceKind::Principal,
        )
        .is_err()
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
        || ResourceId::parse_expected(
            &identity.artifact_encryption_domain_id,
            ResourceKind::EncryptionDomain,
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

fn sync_parent_directory(path: &Path) -> Result<(), CliError> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| CliError::InitializeProject {
                path: parent.display().to_string(),
                source,
            })?;
    }
    Ok(())
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
        })?;
    sync_parent_directory(path)
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
        })?;
    sync_parent_directory(path)
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
        let output = run_bounded_doctor_command(program, arguments, DOCTOR_COMMAND_TIMEOUT)?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        doctor_command_result(output.status.success(), &stdout, &stderr)
    }

    fn port_available(&self, port: u16) -> Result<(), String> {
        TcpListener::bind(("127.0.0.1", port))
            .map(drop)
            .map_err(|error| error.to_string())
    }
}

fn doctor_command_result(success: bool, stdout: &str, stderr: &str) -> Result<String, String> {
    let detail = [stdout, stderr]
        .into_iter()
        .map(str::trim)
        .find(|detail| !detail.is_empty())
        .unwrap_or("command completed without output");
    if success {
        Ok(detail.to_owned())
    } else {
        Err(concise_doctor_detail(detail))
    }
}

fn run_bounded_doctor_command(
    program: &str,
    arguments: &[&str],
    timeout: Duration,
) -> Result<std::process::Output, String> {
    let mut child = ProcessCommand::new(program)
        .args(arguments)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output().map_err(|error| error.to_string()),
            Ok(None) if started.elapsed() < timeout => {
                std::thread::sleep(DOCTOR_COMMAND_POLL_INTERVAL);
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "command timed out after {} milliseconds; verify the service is responsive and retry",
                    timeout.as_millis()
                ));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.to_string());
            }
        }
    }
}

fn required_kubectl_output(arguments: &[&str], dependency: &str) -> Result<String, CliError> {
    let output = run_bounded_doctor_command("kubectl", arguments, DOCTOR_COMMAND_TIMEOUT).map_err(
        |error| {
            CliError::RuntimeUnavailable(format!(
                "sandbox feature requires {dependency}: kubectl {} failed: {}",
                arguments.join(" "),
                truncate_detail(&error)
            ))
        },
    )?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr);
        return Err(CliError::RuntimeUnavailable(format!(
            "sandbox feature requires {dependency}: kubectl {} failed: {}",
            arguments.join(" "),
            truncate_detail(detail.trim())
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn ensure_sandbox_kubernetes_dependency() -> Result<(), CliError> {
    required_kubectl_output(
        &["version", "--client=true", "--output=json"],
        "an installed kubectl client and reachable local Kubernetes context",
    )?;
    let established = required_kubectl_output(
        &[
            "get",
            "crd",
            "batchsandboxes.sandbox.opensandbox.io",
            "-o",
            "jsonpath={.status.conditions[?(@.type=='Established')].status}",
        ],
        "the Established OpenSandbox BatchSandbox CRD",
    )?;
    if established != "True" {
        return Err(CliError::RuntimeUnavailable(
            "sandbox feature requires the Established OpenSandbox BatchSandbox CRD".to_owned(),
        ));
    }
    required_kubectl_output(
        &[
            "-n",
            "platform-sandbox",
            "wait",
            "--for=condition=Available",
            "deployment/sandbox-dispatcher",
            "deployment/opensandbox-server",
            "deployment/opensandbox-controller",
            "--timeout=5s",
        ],
        "Available Sandbox Dispatcher, OpenSandbox Server, and BatchSandbox Controller deployments",
    )?;
    let policies = required_kubectl_output(
        &[
            "-n",
            "platform-sandbox-workloads",
            "get",
            "networkpolicy",
            "armed-runner-ingress",
            "armed-runner-direct",
            "armed-runner-disabled",
            "-o",
            "name",
        ],
        "the fixed Armed runner ingress, Direct, and Disabled network policies",
    )?;
    for policy in [
        "armed-runner-ingress",
        "armed-runner-direct",
        "armed-runner-disabled",
    ] {
        if !policies.lines().any(|line| line.ends_with(policy)) {
            return Err(CliError::RuntimeUnavailable(format!(
                "sandbox feature requires NetworkPolicy {policy} in platform-sandbox-workloads"
            )));
        }
    }
    let runtimes = required_kubectl_output(
        &[
            "get",
            "nodes",
            "-o",
            "jsonpath={range .items[*]}{.status.nodeInfo.containerRuntimeVersion}{'\\n'}{end}",
        ],
        "a Ready single-node containerd/runc Kubernetes runtime",
    )?;
    if runtimes.lines().next().is_none()
        || runtimes
            .lines()
            .any(|runtime| !runtime.starts_with("containerd://"))
    {
        return Err(CliError::RuntimeUnavailable(
            "sandbox feature requires every selected Kubernetes node to report containerd runtime"
                .to_owned(),
        ));
    }
    let service_types = required_kubectl_output(
        &[
            "-n",
            "platform-sandbox",
            "get",
            "service",
            "opensandbox-server",
            "sandbox-dispatcher",
            "-o",
            "jsonpath={range .items[*]}{.spec.type}{'\\n'}{end}",
        ],
        "internal-only OpenSandbox Server and Dispatcher services",
    )?;
    if service_types.lines().count() != 2 || service_types.lines().any(|kind| kind != "ClusterIP") {
        return Err(CliError::RuntimeUnavailable(
            "sandbox feature requires exactly ClusterIP OpenSandbox Server and Dispatcher services"
                .to_owned(),
        ));
    }
    let ingress = required_kubectl_output(
        &["-n", "platform-sandbox", "get", "ingress", "-o", "name"],
        "a control namespace with no public ingress",
    )?;
    if !ingress.is_empty() {
        return Err(CliError::RuntimeUnavailable(
            "sandbox feature refuses a platform-sandbox namespace containing public Ingress"
                .to_owned(),
        ));
    }
    let source_commit = required_kubectl_output(
        &[
            "-n",
            "platform-sandbox",
            "get",
            "configmap",
            "opensandbox-server-config",
            "-o",
            "jsonpath={.metadata.annotations.insight\\.platform/upstream-commit}",
        ],
        "the source-pinned OpenSandbox Server configuration",
    )?;
    if source_commit != OPENSANDBOX_SOURCE_COMMIT {
        return Err(CliError::RuntimeUnavailable(format!(
            "sandbox feature requires OpenSandbox source commit {OPENSANDBOX_SOURCE_COMMIT}; observed {source_commit}"
        )));
    }
    for (deployment, component, expected_digest) in [
        (
            "opensandbox-server",
            "server",
            OPENSANDBOX_SERVER_IMAGE_DIGEST,
        ),
        (
            "opensandbox-controller",
            "manager",
            OPENSANDBOX_CONTROLLER_IMAGE_DIGEST,
        ),
    ] {
        let image = required_kubectl_output(
            &[
                "-n",
                "platform-sandbox",
                "get",
                "deployment",
                deployment,
                "-o",
                &format!(
                    "jsonpath={{.spec.template.spec.containers[?(@.name=='{component}')].image}}"
                ),
            ],
            "the exact digest-pinned official OpenSandbox images",
        )?;
        if !image.ends_with(expected_digest) {
            return Err(CliError::RuntimeUnavailable(format!(
                "sandbox feature requires {deployment} image {expected_digest}; observed {image}"
            )));
        }
    }
    Ok(())
}

fn truncate_detail(detail: &str) -> String {
    detail.chars().take(160).collect()
}

fn concise_doctor_detail(detail: &str) -> String {
    truncate_detail(
        detail
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or("command completed without output"),
    )
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
    checks.push(version_check(
        "rustc_source_build",
        false,
        rustc,
        EXPECTED_RUSTC_PREFIX,
    ));
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
    checks.push(docker_resource_check(probe.command(
        "docker",
        &["info", "--format", "{{.NCPU}} {{.MemTotal}}"],
    )));
    checks.push(disk_resource_check(probe.command("df", &["-Pk", "."])));
    checks.push(command_check(
        "kubectl_client",
        false,
        probe.command("kubectl", &["version", "--client=true", "--output=json"]),
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

fn docker_resource_check(result: Result<String, String>) -> DoctorCheck {
    let parsed = result.and_then(|detail| {
        let values = detail.split_whitespace().collect::<Vec<_>>();
        if values.len() != 2 {
            return Err(format!("unexpected Docker resource report: {detail}"));
        }
        let cpus = values[0]
            .parse::<u64>()
            .map_err(|_| format!("invalid Docker CPU count: {}", values[0]))?;
        let memory = values[1]
            .parse::<u64>()
            .map_err(|_| format!("invalid Docker memory byte count: {}", values[1]))?;
        if cpus < MINIMUM_DEVELOPMENT_CPUS || memory < MINIMUM_DEVELOPMENT_MEMORY_BYTES {
            return Err(format!(
                "Docker has {cpus} CPUs and {memory} bytes; allocate at least {MINIMUM_DEVELOPMENT_CPUS} CPUs and {MINIMUM_DEVELOPMENT_MEMORY_BYTES} bytes"
            ));
        }
        Ok(format!("{cpus} CPUs, {memory} bytes"))
    });
    command_check("docker_resources", true, parsed)
}

fn disk_resource_check(result: Result<String, String>) -> DoctorCheck {
    let parsed = result.and_then(|detail| {
        let line = detail
            .lines()
            .rfind(|line| !line.trim().is_empty())
            .ok_or_else(|| "df returned no filesystem row".to_owned())?;
        let values = line.split_whitespace().collect::<Vec<_>>();
        if values.len() < 4 {
            return Err(format!("unexpected df report: {line}"));
        }
        let available_kib = values[3]
            .parse::<u64>()
            .map_err(|_| format!("invalid available disk KiB: {}", values[3]))?;
        if available_kib < MINIMUM_DEVELOPMENT_DISK_KIB {
            return Err(format!(
                "{available_kib} KiB available; free at least {MINIMUM_DEVELOPMENT_DISK_KIB} KiB without deleting containers or volumes automatically"
            ));
        }
        Ok(format!("{available_kib} KiB available"))
    });
    command_check("development_disk", true, parsed)
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
            DoctorCheck::passed(name, required, concise_doctor_detail(&detail))
        }
        Ok(detail) => DoctorCheck {
            name: name.to_owned(),
            required,
            status: DoctorStatus::Failed,
            detail: format!(
                "expected {required_prefix}; found {}",
                concise_doctor_detail(&detail)
            ),
        },
        Err(detail) => DoctorCheck::failed(name, required, concise_doctor_detail(&detail)),
    }
}

fn command_check(name: &str, required: bool, result: Result<String, String>) -> DoctorCheck {
    match result {
        Ok(detail) => DoctorCheck::passed(name, required, concise_doctor_detail(&detail)),
        Err(detail) => DoctorCheck::failed(name, required, concise_doctor_detail(&detail)),
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
        CliCommand::AdvancedHelp => Ok(advanced_usage().to_owned()),
        CliCommand::Version { json } => Ok(release::version_output(json)),
        CliCommand::UpdateCheck => release::check_for_update().map_err(CliError::Release),
        CliCommand::UpdateApply { version } => {
            release::apply_update(&version).map_err(CliError::Release)
        }
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
        CliCommand::Dev { root, profile } => {
            let root = resolve_root(current_directory, root);
            run_development_profile(current_directory, &root, profile)
        }
        CliCommand::Start { root } => {
            let root = resolve_root(current_directory, root);
            restart_development_profile(current_directory, &root)
        }
        CliCommand::Status { root } => {
            let root = resolve_root(current_directory, root);
            runtime_status(&root)
        }
        CliCommand::Logs { root, role } => {
            let root = resolve_root(current_directory, root);
            runtime_logs(&root, role.as_deref())
        }
        CliCommand::Stop { root } => {
            let root = resolve_root(current_directory, root);
            stop_development_profile(current_directory, &root)
        }
        CliCommand::Reset { root, confirm } => {
            let root = resolve_root(current_directory, root);
            reset_local_project(current_directory, &root, confirm.as_deref())
        }
        CliCommand::Apply {
            root,
            file,
            timeout_seconds,
        } => {
            let root = resolve_root(current_directory, root);
            let file = resolve_root(current_directory, file);
            apply_local_manifest(&root, &file, timeout_seconds)
        }
        CliCommand::OperationWait {
            root,
            operation_id,
            timeout_seconds,
        } => {
            let root = resolve_root(current_directory, root);
            wait_local_operation(&root, &operation_id, timeout_seconds)
        }
        CliCommand::RunCreate { root, file } => {
            let root = resolve_root(current_directory, root);
            let file = resolve_root(current_directory, file);
            create_local_run(&root, &file)
        }
        CliCommand::RunGet { root, run_id } => {
            let root = resolve_root(current_directory, root);
            read_local_run(&root, &run_id)
        }
        CliCommand::RunControl {
            root,
            run_id,
            action,
        } => {
            let root = resolve_root(current_directory, root);
            control_local_run(&root, &run_id, action)
        }
        CliCommand::RunResult { root, run_id } => {
            let root = resolve_root(current_directory, root);
            read_local_run_result(&root, &run_id)
        }
        CliCommand::RunWatch {
            root,
            run_id,
            timeout_seconds,
        } => {
            let root = resolve_root(current_directory, root);
            let mut output = Vec::new();
            watch_local_run(&root, &run_id, timeout_seconds, &mut output)?;
            String::from_utf8(output)
                .map_err(|_| CliError::RuntimeState("Run watch output is not UTF-8".to_owned()))
        }
        CliCommand::TaskGet { root, task_id } => {
            let root = resolve_root(current_directory, root);
            read_local_task(&root, &task_id)
        }
        CliCommand::TaskResolve {
            root,
            task_id,
            action,
            file,
        } => {
            let root = resolve_root(current_directory, root);
            let file = file.map(|path| resolve_root(current_directory, path));
            resolve_local_task(&root, &task_id, action, file.as_deref())
        }
        CliCommand::ArtifactGet { root, artifact_id } => {
            let root = resolve_root(current_directory, root);
            read_local_artifact(&root, &artifact_id)
        }
        CliCommand::ArtifactRead {
            root,
            artifact_id,
            output,
        } => {
            let root = resolve_root(current_directory, root);
            let output = resolve_root(current_directory, output);
            download_local_artifact(&root, &artifact_id, &output)
        }
        CliCommand::ArtifactUpload {
            root,
            file,
            purpose,
            classification,
            media_type,
            display_name,
            timeout_seconds,
        } => {
            let root = resolve_root(current_directory, root);
            let file = resolve_root(current_directory, file);
            upload_local_artifact(
                &root,
                &file,
                &purpose,
                &classification,
                media_type,
                display_name,
                timeout_seconds,
            )
        }
        CliCommand::AgentValidate {
            root,
            file,
            online,
            output,
        } => {
            let root = resolve_root(current_directory, root);
            let file = resolve_root(current_directory, file);
            validate_local_agent(&root, &file, online, output)
        }
        CliCommand::AgentPublish {
            root,
            file,
            wait,
            output,
        } => {
            let root = resolve_root(current_directory, root);
            let file = resolve_root(current_directory, file);
            publish_local_agent(&root, &file, wait, output)
        }
        CliCommand::AgentList { root, output } => {
            list_local_agents(&resolve_root(current_directory, root), output)
        }
        CliCommand::AgentGet {
            root,
            selector,
            output,
        } => get_local_agent(&resolve_root(current_directory, root), &selector, output),
        CliCommand::AgentAdopt {
            root,
            name,
            agent_id,
            output,
        } => adopt_local_agent(
            &resolve_root(current_directory, root),
            &name,
            &agent_id,
            output,
        ),
        CliCommand::AgentRun {
            root,
            selector,
            input,
            file,
            detach,
            timeout_seconds,
            output,
        } => {
            let file = file.map(|path| resolve_root(current_directory, path));
            run_local_agent(
                &resolve_root(current_directory, root),
                &selector,
                input.as_deref(),
                file.as_deref(),
                detach,
                timeout_seconds,
                output,
            )
        }
        CliCommand::AgentLogs {
            root,
            selector,
            follow,
            output,
        } => read_local_agent_logs(
            &resolve_root(current_directory, root),
            &selector,
            follow,
            output,
        ),
        CliCommand::AgentResult {
            root,
            run_id,
            output,
        } => read_local_agent_result(&resolve_root(current_directory, root), &run_id, output),
    }
}

pub fn execute_to_writer<W: Write>(
    command: CliCommand,
    current_directory: &Path,
    probe: &dyn DoctorProbe,
    writer: &mut W,
) -> Result<(), CliError> {
    if let CliCommand::RunWatch {
        root,
        run_id,
        timeout_seconds,
    } = command
    {
        let root = resolve_root(current_directory, root);
        return watch_local_run(&root, &run_id, timeout_seconds, writer);
    }
    let output = execute(command, current_directory, probe)?;
    writer
        .write_all(output.as_bytes())
        .and_then(|_| writer.flush())
        .map_err(|error| CliError::RuntimeState(format!("cannot write CLI output: {error}")))
}

fn resolve_root(current_directory: &Path, root: PathBuf) -> PathBuf {
    if root.is_absolute() {
        root
    } else {
        current_directory.join(root)
    }
}

fn usage() -> &'static str {
    "Insight Platform\n\nStart:\n  insight init [--path <directory>] [--name <name>]\n  insight dev [--path <directory>] [--features model,remote-capability,context,mcp,sandbox|all] [--offline|--from-source]\n\nAgent journey:\n  insight agent validate --file <agent.yaml>\n  insight agent publish --file <agent.yaml> [--output text|json]\n  insight agent list [--output text|json]\n  insight agent get <name-or-agent-id> [--output text|json]\n  insight agent adopt <name> --agent-id <agt_...>\n  insight agent run <name-or-agent-id> (--input <json>|--file <input.json>) [--detach]\n  insight agent logs <name-or-run-id> [--follow]\n  insight agent result <run-id> [--output text|json]\n\nInstall and update:\n  insight version [--json]\n  insight update check\n  insight update apply --version <exact-version>\n\nUse `insight advanced` for Platform automation, diagnostics, and lifecycle commands.\n"
}

fn advanced_usage() -> &'static str {
    "Insight Platform automation and diagnostics\n\nUsage:\n  insight doctor [--json]\n  insight token [--path <directory>]\n  insight start [--path <directory>]\n  insight status [--path <directory>]\n  insight logs [--path <directory>] [--role <role>]\n  insight stop [--path <directory>]\n  insight reset [--path <directory>] [--confirm <project-name>]\n  insight apply --file <manifest.json> [--path <directory>] [--timeout-seconds <1..3600>]\n  insight run create --file <request.json> [--path <directory>]\n  insight run get|result|watch <run_id> [--path <directory>]\n  insight run pause|resume|cancel <run_id> [--path <directory>]\n  insight task get|approve|reject|cancel <task_id> [--path <directory>]\n  insight task submit-input <task_id> --file <input.json> [--path <directory>]\n  insight artifact upload --file <file> --purpose <purpose> --classification <classification> [options]\n  insight artifact get <artifact_id> [--path <directory>]\n  insight artifact read <artifact_id> --output <file> [--path <directory>]\n  insight operation wait <job_id> [--path <directory>] [--timeout-seconds <1..3600>]\n\n`reset` first prints its exact destructive scope; deletion requires the displayed project name. All mutations use the public `/v1` authority. Receipt, ETag, Operation, and cursor details remain managed by the client unless an Agent command explicitly enables `--debug-authority`.\n"
}

fn validate_local_agent(
    root: &Path,
    file: &Path,
    online: bool,
    output: agent::AgentOutputOptions,
) -> Result<String, CliError> {
    let profile = if online {
        let (client, _) = local_public_http_client(root)?;
        agent::online_compiler_profile(
            &client,
            &root
                .join(PROJECT_DIRECTORY)
                .join(RUNTIME_DIRECTORY)
                .join(RUNTIME_CONFIGURATION_DIRECTORY),
        )
        .map_err(CliError::Agent)?
    } else {
        agent::offline_compiler_profile(
            root,
            &root
                .join(PROJECT_DIRECTORY)
                .join(RUNTIME_DIRECTORY)
                .join(RUNTIME_CONFIGURATION_DIRECTORY),
        )
        .map_err(CliError::Agent)?
    };
    let compiled = agent::compile_project(root, file, profile).map_err(CliError::Agent)?;
    let report = agent::validation_report(&compiled);
    match output.mode {
        agent::AgentOutputMode::Json => render_json(&report),
        agent::AgentOutputMode::Text => {
            let features = if report.required_features.is_empty() {
                "none".to_owned()
            } else {
                report
                    .required_features
                    .iter()
                    .map(|feature| format!("{feature:?}").to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            let mut text = format!(
                "Validated {}\nExecution: {:?}\nRequired features: {features}\n",
                report.agent_name, report.execution_kind
            );
            if output.verbose || output.debug_authority {
                text.push_str(&format!("Manifest: {}\n", report.manifest_digest));
            }
            Ok(text)
        }
    }
}

fn publish_local_agent(
    root: &Path,
    file: &Path,
    wait: bool,
    output: agent::AgentOutputOptions,
) -> Result<String, CliError> {
    if !wait {
        return Err(CliError::InvalidOptionValue {
            option: "--wait",
            value: "false (publication handles are not enabled in this build)".to_owned(),
        });
    }
    ensure_agent_features_enabled(root, file)?;
    let (management_client, tenant_id) = local_public_http_client(root)?;
    let (runtime_client, runtime_tenant_id) = local_runtime_http_client(root)?;
    if tenant_id != runtime_tenant_id {
        return Err(CliError::RuntimeState(
            "management and runtime Gateway tenant identities differ".to_owned(),
        ));
    }
    let profile = agent::online_compiler_profile(
        &management_client,
        &root
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY)
            .join(RUNTIME_CONFIGURATION_DIRECTORY),
    )
    .map_err(CliError::Agent)?;
    let compiled = agent::compile_project(root, file, profile).map_err(CliError::Agent)?;
    let report = agent::publish_agent(
        root,
        &management_client,
        &runtime_client,
        &tenant_id,
        &compiled,
        Duration::from_secs(300),
    )
    .map_err(CliError::Agent)?;
    match output.mode {
        agent::AgentOutputMode::Json => {
            let value = serde_json::json!({
                "schema_version": report.schema_version,
                "agent_name": report.agent_name,
                "agent_id": report.agent_id,
                "state": report.state,
                "environment": report.environment,
                "manifest_digest": (output.verbose || output.debug_authority).then_some(report.manifest_digest),
                "unchanged": report.unchanged,
                "validation_operation_id": output.debug_authority.then_some(report.validation_operation_id).flatten(),
                "active_deployment_id": output.debug_authority.then_some(report.active_deployment_id).flatten()
            });
            render_json(&value)
        }
        agent::AgentOutputMode::Text => {
            let action = if report.unchanged {
                "Unchanged"
            } else {
                "Published"
            };
            let mut text = format!(
                "{action} {} to {}\nStatus: {}\nRun: insight agent run {} --input '{{}}'\n",
                report.agent_name, report.environment, report.state, report.agent_name
            );
            if output.verbose || output.debug_authority {
                text.push_str(&format!("Manifest: {}\n", report.manifest_digest));
            }
            if output.debug_authority {
                text.push_str(&format!(
                    "Agent: {}\nValidation operation: {}\nActive deployment: {}\n",
                    report.agent_id,
                    report
                        .validation_operation_id
                        .as_ref()
                        .map_or_else(|| "unchanged".to_owned(), ToString::to_string),
                    report
                        .active_deployment_id
                        .as_ref()
                        .map_or_else(|| "unknown".to_owned(), ToString::to_string)
                ));
            }
            Ok(text)
        }
    }
}

fn ensure_agent_features_enabled(root: &Path, file: &Path) -> Result<(), CliError> {
    let state_directory = root.join(PROJECT_DIRECTORY);
    let project = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity(&state_directory, &project.identity)?;
    let configuration = root
        .join(PROJECT_DIRECTORY)
        .join(RUNTIME_DIRECTORY)
        .join(RUNTIME_CONFIGURATION_DIRECTORY);
    let compiler_profile =
        agent::offline_compiler_profile(root, &configuration).map_err(CliError::Agent)?;
    let compiled = agent::compile_project(root, file, compiler_profile).map_err(CliError::Agent)?;
    let runtime = root.join(PROJECT_DIRECTORY).join(RUNTIME_DIRECTORY);
    let profile = read_runtime_profile_state(&runtime, &project.identity)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no selected development feature closure exists; run `insight dev` first".to_owned(),
        )
    })?;
    let missing = compiled
        .required_features
        .iter()
        .filter_map(|feature| match feature {
            insight_platform_contracts::AgentRequiredFeature::Model => {
                (!profile.features.iter().any(|value| value == "model")).then_some("model")
            }
        })
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    Err(CliError::RuntimeUnavailable(format!(
        "feature_not_enabled\nThis agent requires: {}\nRestart with: insight dev --features {}",
        missing.join(","),
        missing.join(",")
    )))
}

fn list_local_agents(root: &Path, output: agent::AgentOutputOptions) -> Result<String, CliError> {
    let lock = agent::load_lock(root).map_err(CliError::Agent)?;
    let (client, _) = local_public_http_client(root)?;
    let remote = agent::list_remote_agents(&client);
    let remote_by_id = remote
        .as_ref()
        .ok()
        .map(|items| {
            items
                .iter()
                .map(|item| (item.agent_id.clone(), item))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let items = lock
        .agents
        .iter()
        .map(|(name, entry)| {
            let remote = remote_by_id.get(&entry.agent_id);
            let status = match remote {
                Some(remote) if remote.name == *name && remote.state == insight_platform_contracts::AgentProductState::Ready => "ready",
                Some(_) => "drifted",
                None if remote_by_id.is_empty() => "unreachable",
                None => "drifted",
            };
            serde_json::json!({
                "schema_version": 1,
                "agent_name": name,
                "agent_id": entry.agent_id,
                "state": status,
                "environment": remote.and_then(|item| item.environment.as_deref()).or(entry.environment.as_deref()),
                "manifest_digest": (output.verbose || output.debug_authority).then_some(&entry.manifest_digest),
                "active_deployment_id": output.debug_authority.then_some(entry.active_deployment_id.as_ref()).flatten()
            })
        })
        .collect::<Vec<_>>();
    if output.mode == agent::AgentOutputMode::Json {
        return render_json(&serde_json::json!({
            "schema_version": 1,
            "agents": items
        }));
    }
    if items.is_empty() {
        return Ok("No Agents are managed by this project.\n".to_owned());
    }
    let mut text = String::from("NAME\tSTATE\tENVIRONMENT\tAGENT ID\n");
    for item in &items {
        text.push_str(&format!(
            "{}\t{}\t{}\t{}\n",
            item["agent_name"].as_str().unwrap_or("invalid"),
            item["state"].as_str().unwrap_or("invalid"),
            item["environment"].as_str().unwrap_or("-"),
            item["agent_id"].as_str().unwrap_or("invalid")
        ));
    }
    Ok(text)
}

fn get_local_agent(
    root: &Path,
    selector: &str,
    output: agent::AgentOutputOptions,
) -> Result<String, CliError> {
    let agent_id = agent::resolve_agent_id(root, selector).map_err(CliError::Agent)?;
    let (client, _) = local_public_http_client(root)?;
    let resource = agent::read_resource(&client, &agent_id).map_err(CliError::Agent)?;
    let ResourceDocument::Agent(spec) = &resource.draft.document else {
        return Err(CliError::RuntimeState(
            "Agent Resource returned a non-Agent document".to_owned(),
        ));
    };
    let state = if resource.gate_state == insight_platform_contracts::AdministrativeGate::Enabled {
        "ready"
    } else {
        "draft"
    };
    let value = serde_json::json!({
        "schema_version": 1,
        "agent_name": spec.authoring_name,
        "agent_id": agent_id,
        "display_name": resource.draft.display_name,
        "state": state,
        "required_features": spec.required_features,
        "input_classification": spec.input_classification,
        "default_deadline_seconds": spec.default_deadline_seconds,
        "manifest_digest": (output.verbose || output.debug_authority).then_some(&spec.authoring_package.manifest_digest),
        "resource_version": output.debug_authority.then_some(resource.version),
        "etag": output.debug_authority.then_some(&resource.etag)
    });
    if output.mode == agent::AgentOutputMode::Json {
        render_json(&value)
    } else {
        let mut text = format!(
            "Agent: {}\nName: {}\nStatus: {}\nInput: {}\nDeadline: {} seconds\n",
            agent_id,
            spec.authoring_name,
            state,
            spec.input_classification,
            spec.default_deadline_seconds
        );
        if output.verbose || output.debug_authority {
            text.push_str(&format!(
                "Manifest: {}\n",
                spec.authoring_package.manifest_digest
            ));
        }
        if output.debug_authority {
            text.push_str(&format!("Resource ETag: {}\n", resource.etag));
        }
        Ok(text)
    }
}

fn adopt_local_agent(
    root: &Path,
    name: &str,
    agent_id: &str,
    output: agent::AgentOutputOptions,
) -> Result<String, CliError> {
    let agent_id = ResourceId::parse_expected(agent_id, ResourceKind::Agent).map_err(|_| {
        CliError::InvalidOptionValue {
            option: "--agent-id",
            value: agent_id.to_owned(),
        }
    })?;
    let (client, _) = local_public_http_client(root)?;
    let entry = agent::adopt_agent(root, &client, name, agent_id).map_err(CliError::Agent)?;
    if output.mode == agent::AgentOutputMode::Json {
        render_json(&serde_json::json!({
            "schema_version": 1,
            "agent_name": name,
            "agent_id": entry.agent_id,
            "state": "adopted",
            "environment": entry.environment,
            "manifest_digest": (output.verbose || output.debug_authority).then_some(entry.manifest_digest)
        }))
    } else {
        let mut text = format!("Adopted {name}\nAgent: {}\n", entry.agent_id);
        if output.verbose || output.debug_authority {
            text.push_str(&format!("Manifest: {}\n", entry.manifest_digest));
        }
        Ok(text)
    }
}

#[derive(Debug, Serialize)]
struct AgentRunReportV1<'a> {
    schema_version: u16,
    agent_name: &'a str,
    agent_id: &'a ResourceId,
    run_id: &'a ResourceId,
    run_state: RunState,
    result: Option<&'a ValueRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    authority: Option<serde_json::Value>,
}

#[allow(clippy::too_many_arguments)]
fn run_local_agent(
    root: &Path,
    selector: &str,
    inline_input: Option<&str>,
    input_file: Option<&Path>,
    detach: bool,
    timeout_seconds: u64,
    output: agent::AgentOutputOptions,
) -> Result<String, CliError> {
    let agent_id = agent::resolve_agent_id(root, selector).map_err(CliError::Agent)?;
    let lock = agent::load_lock(root).map_err(CliError::Agent)?;
    let agent_name = lock
        .agents
        .iter()
        .find(|(_, entry)| entry.agent_id == agent_id)
        .map_or_else(|| selector.to_owned(), |(name, _)| name.clone());
    let (management, _) = local_public_http_client(root)?;
    let resource = agent::read_resource(&management, &agent_id).map_err(CliError::Agent)?;
    if resource.gate_state != insight_platform_contracts::AdministrativeGate::Enabled {
        return Err(CliError::RuntimeState(format!(
            "Agent {agent_name:?} has no active Deployment"
        )));
    }
    let ResourceDocument::Agent(spec) = resource.draft.document else {
        return Err(CliError::RuntimeState(
            "Agent Resource returned a non-Agent document".to_owned(),
        ));
    };
    let input_bytes = match (inline_input, input_file) {
        (Some(value), None) => value.as_bytes().to_vec(),
        (None, Some(path)) => read_bounded_run_request(path)?,
        _ => return Err(CliError::Usage),
    };
    let input_value = parse_strict_json(
        &input_bytes,
        JsonLimits {
            max_bytes: 1_048_576,
            max_depth: 64,
            max_properties_per_object: 256,
            max_items_per_array: 4_096,
            max_string_bytes: 262_144,
        },
    )
    .map_err(|error| CliError::RuntimeState(format!("Agent input is invalid JSON: {error}")))?;
    spec.input_schema
        .validate_instance(&input_value)
        .map_err(|error| {
            CliError::RuntimeState(format!("Agent input does not match its schema: {error}"))
        })?;
    let deadline_seconds = u64::from(spec.default_deadline_seconds).min(timeout_seconds);
    let deadline_delta =
        i64::try_from(deadline_seconds).map_err(|_| CliError::InvalidOptionValue {
            option: "--timeout-seconds",
            value: timeout_seconds.to_string(),
        })?;
    let request = run::CreateRunRequestV1 {
        agent_id: agent_id.clone(),
        input: run::CreateRunInputV1 {
            classification: spec.input_classification,
            schema_digest: spec.input_schema.canonical_digest.clone(),
            value: ValueRef::Inline { value: input_value },
        },
        deadline: UtcTimestamp::from_datetime(Utc::now() + ChronoDuration::seconds(deadline_delta)),
    };
    let request_bytes =
        serde_json::to_vec(&request).map_err(|error| CliError::RuntimeState(error.to_string()))?;
    let (runtime, _) = local_runtime_http_client(root)?;
    let created = run::create_run(&runtime, &request_bytes).map_err(CliError::Run)?;
    if lock.agents.contains_key(&agent_name) {
        agent::remember_run(root, &agent_name, created.run_id.clone()).map_err(CliError::Agent)?;
    }
    if detach {
        return render_agent_run_report(&agent_name, &agent_id, &created, None, output);
    }
    let mut event_output = Vec::new();
    let terminal = run::watch_run_with_cursor_journal(
        &runtime,
        &created.run_id,
        Duration::from_secs(timeout_seconds),
        &mut event_output,
        &root.join(PROJECT_DIRECTORY).join("run-events"),
    )
    .map_err(CliError::Run)?;
    let result = if terminal.state == RunState::Succeeded {
        Some(run::read_run_result(&runtime, &terminal.run_id).map_err(CliError::Run)?)
    } else {
        None
    };
    let rendered =
        render_agent_run_report(&agent_name, &agent_id, &terminal, result.as_ref(), output)?;
    if terminal.state != RunState::Succeeded {
        return Err(CliError::RuntimeState(format!(
            "Run {} reached terminal state {}",
            terminal.run_id, terminal.state
        )));
    }
    Ok(rendered)
}

fn render_agent_run_report(
    agent_name: &str,
    agent_id: &ResourceId,
    run: &run::RunViewV1,
    result: Option<&run::RunResultViewV1>,
    output: agent::AgentOutputOptions,
) -> Result<String, CliError> {
    let report = AgentRunReportV1 {
        schema_version: 1,
        agent_name,
        agent_id,
        run_id: &run.run_id,
        run_state: run.state,
        result: result.map(|result| &result.value),
        authority: output.debug_authority.then(|| {
            serde_json::json!({
                "agent_deployment_id": run.agent_deployment_id,
                "run_etag": run.etag
            })
        }),
    };
    if output.mode == agent::AgentOutputMode::Json {
        return render_json(&report);
    }
    let mut text = format!(
        "Run: {}\nAgent: {}\nStatus: {}\n",
        run.run_id, agent_name, run.state
    );
    if let Some(result) = result {
        let value = serde_json::to_string_pretty(&result.value)
            .map_err(|error| CliError::RuntimeState(error.to_string()))?;
        text.push_str(&format!("Result: {value}\n"));
    }
    if output.debug_authority {
        text.push_str(&format!(
            "Deployment: {}\nRun ETag: {}\n",
            run.agent_deployment_id, run.etag
        ));
    }
    Ok(text)
}

fn read_local_agent_logs(
    root: &Path,
    selector: &str,
    follow: bool,
    _output: agent::AgentOutputOptions,
) -> Result<String, CliError> {
    let run_id = match ResourceId::parse_expected(selector, ResourceKind::Run) {
        Ok(run_id) => run_id,
        Err(_) => agent::latest_run_for_agent(root, selector).map_err(CliError::Agent)?,
    };
    let (client, _) = local_runtime_http_client(root)?;
    let mut bytes = Vec::new();
    if follow {
        run::watch_run_with_cursor_journal(
            &client,
            &run_id,
            Duration::from_secs(3_600),
            &mut bytes,
            &root.join(PROJECT_DIRECTORY).join("run-events"),
        )
        .map_err(CliError::Run)?;
    } else {
        run::read_run_events_with_cursor_journal(
            &client,
            &run_id,
            &mut bytes,
            &root.join(PROJECT_DIRECTORY).join("run-events"),
        )
        .map_err(CliError::Run)?;
    }
    String::from_utf8(bytes)
        .map_err(|_| CliError::RuntimeState("Run event output is not UTF-8".to_owned()))
}

fn read_local_agent_result(
    root: &Path,
    run_id: &str,
    output: agent::AgentOutputOptions,
) -> Result<String, CliError> {
    let run_id = parse_run_id(run_id)?;
    let (client, _) = local_runtime_http_client(root)?;
    let result = run::read_run_result(&client, &run_id).map_err(CliError::Run)?;
    if output.mode == agent::AgentOutputMode::Json {
        return render_json(&serde_json::json!({
            "schema_version": 1,
            "run_id": result.run_id,
            "run_state": "succeeded",
            "result": result.value,
            "schema_digest": output.debug_authority.then_some(result.schema_digest),
            "content_digest": output.debug_authority.then_some(result.content_digest)
        }));
    }
    let value = serde_json::to_string_pretty(&result.value)
        .map_err(|error| CliError::RuntimeState(error.to_string()))?;
    let mut text = format!("Run: {}\nResult: {value}\n", result.run_id);
    if output.debug_authority {
        text.push_str(&format!(
            "Schema: {}\nContent: {}\n",
            result.schema_digest, result.content_digest
        ));
    }
    Ok(text)
}

fn apply_local_manifest(
    root: &Path,
    file: &Path,
    timeout_seconds: u64,
) -> Result<String, CliError> {
    const MAX_APPLY_MANIFEST_BYTES: u64 = 1_048_576;

    let metadata = fs::metadata(file).map_err(|source| CliError::ReadApplyManifest {
        path: file.display().to_string(),
        source,
    })?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_APPLY_MANIFEST_BYTES {
        return Err(CliError::ReadApplyManifest {
            path: file.display().to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "manifest size must be within 1..=1048576 bytes",
            ),
        });
    }
    let bytes = fs::read(file).map_err(|source| CliError::ReadApplyManifest {
        path: file.display().to_string(),
        source,
    })?;
    let (client, tenant_id) = local_public_http_client(root)?;
    let report = apply::apply_manifest(
        &client,
        &tenant_id,
        &bytes,
        Duration::from_secs(timeout_seconds),
        &root.join(PROJECT_DIRECTORY).join("apply"),
    )
    .map_err(CliError::Apply)?;
    serde_json::to_string_pretty(&report)
        .map(|value| value + "\n")
        .map_err(|error| CliError::RuntimeState(error.to_string()))
}

#[derive(Debug, Clone, Copy)]
enum LocalGatewaySurface {
    Management,
    Runtime,
}

fn local_public_http_client(
    root: &Path,
) -> Result<(public_client::PublicHttpClient, ResourceId), CliError> {
    local_public_http_client_for(root, LocalGatewaySurface::Management)
}

fn local_runtime_http_client(
    root: &Path,
) -> Result<(public_client::PublicHttpClient, ResourceId), CliError> {
    local_public_http_client_for(root, LocalGatewaySurface::Runtime)
}

fn local_public_http_client_for(
    root: &Path,
    surface: LocalGatewaySurface,
) -> Result<(public_client::PublicHttpClient, ResourceId), CliError> {
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let state_directory = root.join(PROJECT_DIRECTORY);
    let project = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity(&state_directory, &project.identity)?;
    let expected_tenant_id =
        ResourceId::parse_expected(&project.identity.tenant_id, ResourceKind::Tenant).map_err(
            |_| CliError::InvalidLocalIdentity {
                path: state_directory.display().to_string(),
            },
        )?;
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    let profile = read_runtime_profile_state(&runtime, &project.identity)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no local runtime profile exists; run `insight dev` first".to_owned(),
        )
    })?;
    let token_path = state_directory
        .join(IDENTITY_DIRECTORY)
        .join(IDENTITY_ACCESS_TOKEN_FILE);
    let token = String::from_utf8(read_bounded_identity_file(&token_path)?).map_err(|_| {
        CliError::InvalidLocalIdentity {
            path: state_directory.display().to_string(),
        }
    })?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CliError::InvalidClock)?
        .as_secs();
    if cached_token_expiry(token.as_bytes())
        .is_none_or(|expires_at| expires_at <= i64::try_from(now).unwrap_or(i64::MAX))
    {
        return Err(CliError::RuntimeUnavailable(
            "the cached local token has expired; run `insight token` and retry".to_owned(),
        ));
    }
    let port = match surface {
        LocalGatewaySurface::Management => profile.ports.gateway_management,
        LocalGatewaySurface::Runtime => profile.ports.gateway_runtime,
    };
    let client = public_client::PublicHttpClient::new(
        format!("http://127.0.0.1:{port}"),
        token,
        Duration::from_secs(5),
    )
    .map_err(CliError::PublicClient)?;
    Ok((client, expected_tenant_id))
}

fn create_local_run(root: &Path, file: &Path) -> Result<String, CliError> {
    let bytes = read_bounded_run_request(file)?;
    let (client, _) = local_runtime_http_client(root)?;
    let view = run::create_run(&client, &bytes).map_err(CliError::Run)?;
    render_json(&view)
}

fn read_local_run(root: &Path, run_id: &str) -> Result<String, CliError> {
    let run_id = parse_run_id(run_id)?;
    let (client, _) = local_runtime_http_client(root)?;
    let view = run::read_run(&client, &run_id).map_err(CliError::Run)?;
    render_json(&view)
}

fn control_local_run(
    root: &Path,
    run_id: &str,
    action: CliRunControlAction,
) -> Result<String, CliError> {
    let run_id = parse_run_id(run_id)?;
    let action = match action {
        CliRunControlAction::Pause => run::RunControlAction::Pause,
        CliRunControlAction::Resume => run::RunControlAction::Resume,
        CliRunControlAction::Cancel => run::RunControlAction::Cancel,
    };
    let (client, _) = local_runtime_http_client(root)?;
    let view = run::control_run(
        &client,
        &run_id,
        action,
        &root.join(PROJECT_DIRECTORY).join("run-control"),
    )
    .map_err(CliError::Run)?;
    render_json(&view)
}

fn read_local_run_result(root: &Path, run_id: &str) -> Result<String, CliError> {
    let run_id = parse_run_id(run_id)?;
    let (client, _) = local_runtime_http_client(root)?;
    let view = run::read_run_result(&client, &run_id).map_err(CliError::Run)?;
    render_json(&view)
}

fn watch_local_run<W: Write>(
    root: &Path,
    run_id: &str,
    timeout_seconds: u64,
    writer: &mut W,
) -> Result<(), CliError> {
    let run_id = parse_run_id(run_id)?;
    let (client, _) = local_runtime_http_client(root)?;
    run::watch_run(
        &client,
        &run_id,
        Duration::from_secs(timeout_seconds),
        writer,
    )
    .map(|_| ())
    .map_err(CliError::Run)
}

fn read_local_task(root: &Path, task_id: &str) -> Result<String, CliError> {
    let task_id = parse_task_id(task_id)?;
    let (client, _) = local_runtime_http_client(root)?;
    let view = task::read_task(&client, &task_id).map_err(CliError::Task)?;
    render_json(&view)
}

fn resolve_local_task(
    root: &Path,
    task_id: &str,
    action: CliTaskAction,
    file: Option<&Path>,
) -> Result<String, CliError> {
    let task_id = parse_task_id(task_id)?;
    let action = match action {
        CliTaskAction::SubmitInput => task::TaskAction::SubmitInput,
        CliTaskAction::Approve => task::TaskAction::Approve,
        CliTaskAction::Reject => task::TaskAction::Reject,
        CliTaskAction::Cancel => task::TaskAction::Cancel,
    };
    let bytes = file.map(read_bounded_task_input).transpose()?;
    let input = bytes
        .as_deref()
        .map(task::parse_submit_input)
        .transpose()
        .map_err(CliError::Task)?;
    let (client, _) = local_runtime_http_client(root)?;
    let view = task::resolve_task(
        &client,
        &task_id,
        action,
        input.as_ref(),
        &root.join(PROJECT_DIRECTORY).join("task-control"),
    )
    .map_err(CliError::Task)?;
    render_json(&view)
}

fn read_local_artifact(root: &Path, artifact_id: &str) -> Result<String, CliError> {
    let artifact_id = parse_artifact_id(artifact_id)?;
    let (client, _) = local_runtime_http_client(root)?;
    let view = artifact::read_artifact(&client, &artifact_id).map_err(CliError::Artifact)?;
    render_json(&view)
}

fn download_local_artifact(
    root: &Path,
    artifact_id: &str,
    output: &Path,
) -> Result<String, CliError> {
    let artifact_id = parse_artifact_id(artifact_id)?;
    let (client, _) = local_runtime_http_client(root)?;
    let report =
        artifact::download_artifact(&client, &artifact_id, output).map_err(CliError::Artifact)?;
    render_json(&report)
}

#[allow(clippy::too_many_arguments)]
fn upload_local_artifact(
    root: &Path,
    file: &Path,
    purpose: &str,
    classification: &str,
    media_type: Option<String>,
    display_name: Option<String>,
    timeout_seconds: u64,
) -> Result<String, CliError> {
    let purpose = purpose
        .parse::<ArtifactPurpose>()
        .map_err(|_| CliError::InvalidOptionValue {
            option: "--purpose",
            value: purpose.to_owned(),
        })?;
    let classification =
        classification
            .parse::<DataClassification>()
            .map_err(|_| CliError::InvalidOptionValue {
                option: "--classification",
                value: classification.to_owned(),
            })?;
    let (client, tenant_id) = local_runtime_http_client(root)?;
    let uploader = artifact::HttpsArtifactObjectUploader::new().map_err(CliError::Artifact)?;
    let report = artifact::upload_artifact(
        &client,
        &uploader,
        &tenant_id,
        file,
        artifact::ArtifactUploadOptions {
            purpose,
            classification,
            declared_media_type: media_type,
            display_name,
            operation_timeout: Duration::from_secs(timeout_seconds),
        },
        &root.join(PROJECT_DIRECTORY).join("artifact-upload"),
    )
    .map_err(CliError::Artifact)?;
    render_json(&report)
}

fn parse_run_id(value: &str) -> Result<ResourceId, CliError> {
    ResourceId::parse_expected(value, ResourceKind::Run).map_err(|_| CliError::InvalidOptionValue {
        option: "run_id",
        value: value.to_owned(),
    })
}

fn parse_task_id(value: &str) -> Result<ResourceId, CliError> {
    value
        .parse::<ResourceId>()
        .ok()
        .filter(|id| {
            matches!(
                id.kind(),
                ResourceKind::Interaction | ResourceKind::ApprovalTask
            )
        })
        .ok_or_else(|| CliError::InvalidOptionValue {
            option: "task_id",
            value: value.to_owned(),
        })
}

fn parse_artifact_id(value: &str) -> Result<ResourceId, CliError> {
    ResourceId::parse_expected(value, ResourceKind::Artifact).map_err(|_| {
        CliError::InvalidOptionValue {
            option: "artifact_id",
            value: value.to_owned(),
        }
    })
}

fn read_bounded_run_request(file: &Path) -> Result<Vec<u8>, CliError> {
    const MAX_RUN_REQUEST_BYTES: u64 = 1_048_576;

    let metadata = fs::metadata(file).map_err(|source| CliError::ReadRunRequest {
        path: file.display().to_string(),
        source,
    })?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_RUN_REQUEST_BYTES {
        return Err(CliError::ReadRunRequest {
            path: file.display().to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request size must be within 1..=1048576 bytes",
            ),
        });
    }
    fs::read(file).map_err(|source| CliError::ReadRunRequest {
        path: file.display().to_string(),
        source,
    })
}

fn read_bounded_task_input(file: &Path) -> Result<Vec<u8>, CliError> {
    const MAX_TASK_INPUT_BYTES: u64 = 65_536;

    let metadata = fs::metadata(file).map_err(|source| CliError::ReadTaskInput {
        path: file.display().to_string(),
        source,
    })?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_TASK_INPUT_BYTES {
        return Err(CliError::ReadTaskInput {
            path: file.display().to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "input size must be within 1..=65536 bytes",
            ),
        });
    }
    fs::read(file).map_err(|source| CliError::ReadTaskInput {
        path: file.display().to_string(),
        source,
    })
}

fn render_json<T: Serialize>(value: &T) -> Result<String, CliError> {
    serde_json::to_string_pretty(value)
        .map(|value| value + "\n")
        .map_err(|error| CliError::RuntimeState(error.to_string()))
}

fn wait_local_operation(
    root: &Path,
    operation_id: &str,
    timeout_seconds: u64,
) -> Result<String, CliError> {
    let operation_id =
        ResourceId::parse_expected(operation_id, ResourceKind::Job).map_err(|_| {
            CliError::InvalidOptionValue {
                option: "operation_id",
                value: operation_id.to_owned(),
            }
        })?;
    let (client, expected_tenant_id) = local_public_http_client(root)?;
    let operation = client
        .wait_operation(
            &operation_id,
            &expected_tenant_id,
            Duration::from_secs(timeout_seconds),
        )
        .map_err(CliError::PublicClient)?;
    match operation.state {
        PublicJobState::Succeeded => serde_json::to_string_pretty(&operation)
            .map(|value| value + "\n")
            .map_err(|error| CliError::RuntimeState(error.to_string())),
        PublicJobState::Failed
        | PublicJobState::Cancelled
        | PublicJobState::TimedOut
        | PublicJobState::ReconciliationRequired => {
            let detail = operation.error.as_ref().map_or_else(
                || "no public failure detail was provided".to_owned(),
                |error| format!("code={} message={}", error.code, error.message),
            );
            Err(CliError::OperationTerminal {
                operation_id: operation.operation_id.to_string(),
                state: public_job_state_name(operation.state).to_owned(),
                detail,
            })
        }
        PublicJobState::Queued | PublicJobState::Running | PublicJobState::Waiting => Err(
            CliError::PublicClient(public_client::PublicClientError::InvalidResponse(
                "Operation wait returned a non-terminal state".to_owned(),
            )),
        ),
    }
}

const fn public_job_state_name(state: PublicJobState) -> &'static str {
    match state {
        PublicJobState::Queued => "queued",
        PublicJobState::Running => "running",
        PublicJobState::Waiting => "waiting",
        PublicJobState::Succeeded => "succeeded",
        PublicJobState::Failed => "failed",
        PublicJobState::Cancelled => "cancelled",
        PublicJobState::TimedOut => "timed_out",
        PublicJobState::ReconciliationRequired => "reconciliation_required",
    }
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

    #[test]
    fn existing_local_bucket_requires_enabled_versioning() {
        assert!(ensure_local_bucket_versioning_enabled(
            r#"{"Status":"Enabled","MFADelete":"Disabled"}"#
        )
        .is_ok());

        for response in [
            r#"{"Status":"Suspended"}"#,
            r#"{"MFADelete":"Disabled"}"#,
            r#"{"Status":true}"#,
            "not-json",
        ] {
            assert!(matches!(
                ensure_local_bucket_versioning_enabled(response),
                Err(CliError::RuntimeUnavailable(_))
            ));
        }
    }

    #[test]
    fn local_dependency_metadata_is_exact_and_strict() {
        let kms = "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012";
        ensure_local_kms_metadata(
            &serde_json::json!({
                "KeyMetadata": {
                    "Arn": kms,
                    "Enabled": true,
                    "KeyState": "Enabled",
                    "KeyUsage": "ENCRYPT_DECRYPT",
                    "Origin": "AWS_KMS"
                }
            })
            .to_string(),
            kms,
        )
        .unwrap();
        let secret = LOCAL_TEST_SECRET_READINESS_ARN;
        ensure_local_secret_metadata(
            &serde_json::json!({
                "ARN": secret,
                "Name": LOCAL_SECRET_READINESS_NAME,
                "KmsKeyId": "12345678-1234-1234-1234-123456789012"
            })
            .to_string(),
            secret,
            kms,
        )
        .unwrap();

        assert!(ensure_local_kms_metadata(
            r#"{"KeyMetadata":{"Arn":"wrong","Enabled":true,"KeyState":"Enabled","KeyUsage":"ENCRYPT_DECRYPT","Origin":"AWS_KMS"}}"#,
            kms,
        )
        .is_err());
        assert!(ensure_local_secret_metadata(
            &serde_json::json!({
                "ARN": secret,
                "Name": LOCAL_SECRET_READINESS_NAME,
                "KmsKeyId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
            })
            .to_string(),
            secret,
            kms,
        )
        .is_err());
        assert!(
            ensure_local_kms_metadata(r#"{"KeyMetadata":{"Arn":"one","Arn":"two"}}"#, kms,)
                .is_err()
        );
        assert!(!local_secret_readiness_arn_is_valid(
            "arn:aws:secretsmanager:us-east-1:000000000000:secret:insight/platform/readiness-short"
        ));
    }

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
                (
                    "docker".to_owned(),
                    vec![
                        "info".to_owned(),
                        "--format".to_owned(),
                        "{{.NCPU}} {{.MemTotal}}".to_owned(),
                    ],
                ),
                Ok("4 8589934592".to_owned()),
            );
            probe.commands.insert(
                (
                    "df".to_owned(),
                    vec!["-Pk".to_owned(), ".".to_owned()],
                ),
                Ok("Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/test 16777216 1 16777215 1% /".to_owned()),
            );
            probe.commands.insert(
                (
                    "kubectl".to_owned(),
                    vec![
                        "version".to_owned(),
                        "--client=true".to_owned(),
                        "--output=json".to_owned(),
                    ],
                ),
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
    fn doctor_accepts_optional_missing_kubectl() {
        let report = doctor_report(&FakeProbe::ready());
        assert!(report.ready);
        assert!(report.checks.iter().any(
            |check| check.name == "kubectl_client" && check.status == DoctorStatus::Unavailable
        ));
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
    fn doctor_does_not_require_a_source_toolchain_for_prebuilt_development() {
        let mut probe = FakeProbe::ready();
        probe.commands.insert(
            ("rustc".to_owned(), vec!["--version".to_owned()]),
            Err("not installed".to_owned()),
        );
        let report = doctor_report(&probe);
        assert!(report.ready);
        assert!(report.checks.iter().any(|check| {
            check.name == "rustc_source_build"
                && !check.required
                && check.status == DoctorStatus::Unavailable
        }));
    }

    #[test]
    fn doctor_rejects_insufficient_docker_resources_and_disk() {
        let mut probe = FakeProbe::ready();
        probe.commands.insert(
            (
                "docker".to_owned(),
                vec![
                    "info".to_owned(),
                    "--format".to_owned(),
                    "{{.NCPU}} {{.MemTotal}}".to_owned(),
                ],
            ),
            Ok("2 4294967296".to_owned()),
        );
        probe.commands.insert(
            (
                "df".to_owned(),
                vec!["-Pk".to_owned(), ".".to_owned()],
            ),
            Ok("Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/test 16777216 1 1024 99% /".to_owned()),
        );
        let report = doctor_report(&probe);
        assert!(!report.ready);
        assert!(report.checks.iter().any(|check| {
            check.name == "docker_resources" && check.status == DoctorStatus::Failed
        }));
        assert!(report.checks.iter().any(|check| {
            check.name == "development_disk" && check.status == DoctorStatus::Failed
        }));
    }

    #[test]
    fn doctor_parses_macos_df_output_before_concising_detail() {
        let stdout = "Filesystem   1024-blocks      Used  Available Capacity  Mounted on\n/dev/disk3s5  1948404040 540541024 1384722252    29%    /System/Volumes/Data\n";

        let raw = doctor_command_result(true, stdout, "").unwrap();
        assert_eq!(raw, stdout.trim());

        let check = disk_resource_check(Ok(raw));
        assert_eq!(check.status, DoctorStatus::Passed);
        assert_eq!(check.detail, "1384722252 KiB available");
    }

    #[test]
    fn command_checks_keep_multiline_success_details_concise() {
        let check = command_check(
            "multiline_command",
            false,
            doctor_command_result(true, "first line\nsecond line\n", ""),
        );

        assert_eq!(check.status, DoctorStatus::Passed);
        assert_eq!(check.detail, "first line");
    }

    #[cfg(unix)]
    #[test]
    fn system_doctor_probe_times_out_an_unresponsive_command() {
        let started = Instant::now();
        let error =
            run_bounded_doctor_command("/bin/sh", &["-c", "sleep 5"], Duration::from_millis(100))
                .unwrap_err();
        assert!(error.contains("timed out after 100 milliseconds"));
        assert!(started.elapsed() < Duration::from_secs(2));
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
        assert_eq!(state.profiles.len(), 1);
        let root = directory.path().join(PROJECT_DIRECTORY);
        assert_eq!(
            fs::read_to_string(root.join(PROJECT_GITIGNORE_FILE)).unwrap(),
            "# Generated local development state.\n*\n!.gitignore\n"
        );
        let persisted: LocalProjectState =
            serde_json::from_slice(&fs::read(root.join(PROJECT_STATE_FILE)).unwrap()).unwrap();
        assert_eq!(persisted, state);
        assert_eq!(persisted.identity.schema_version, 3);
        assert!(persisted
            .identity
            .egress_broker_principal_id
            .starts_with("prn_"));
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
            RUNTIME_NATS_SERVER_CERTIFICATE_FILE,
            RUNTIME_NATS_CLIENT_CERTIFICATE_FILE,
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
                RUNTIME_NATS_SERVER_PRIVATE_KEY_FILE,
                RUNTIME_NATS_CLIENT_PRIVATE_KEY_FILE,
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
        let mcp_state_key = root
            .join(RUNTIME_DIRECTORY)
            .join(full_profile::MCP_STATE_KEY_DIRECTORY)
            .join(full_profile::MCP_STATE_KEY_FILE);
        assert!(!mcp_state_key.exists());
        let mcp_oauth_state_key = root
            .join(RUNTIME_DIRECTORY)
            .join(full_profile::MCP_OAUTH_STATE_KEY_DIRECTORY)
            .join(full_profile::MCP_OAUTH_STATE_KEY_FILE);
        assert!(!mcp_oauth_state_key.exists());
        assert!(!tls
            .join(full_profile::MODEL_WORKER_CLIENT_CERTIFICATE_FILE)
            .exists());
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
        assert_eq!(bootstrap["schema_version"], 2);
        assert_eq!(
            bootstrap["egress_broker"]["principal_id"],
            persisted.identity.egress_broker_principal_id
        );
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
    fn tls_identity_registry_has_the_closed_profile_counts() {
        for (profile, expected) in [
            (DevProfile::starter(), 7),
            (DevProfile::parse(Some("sandbox"), false, true).unwrap(), 7),
            (DevProfile::parse(Some("model"), false, true).unwrap(), 11),
            (
                DevProfile::parse(Some("remote-capability"), false, true).unwrap(),
                11,
            ),
            (DevProfile::parse(Some("context"), false, true).unwrap(), 15),
            (DevProfile::parse(Some("mcp"), false, true).unwrap(), 18),
            (DevProfile::parse(Some("all"), false, true).unwrap(), 23),
        ] {
            assert_eq!(
                expected_local_tls_leaf_identities(profile).len() + 1,
                expected,
                "{}",
                profile.label()
            );
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum InvalidLeafCase {
        WrongKey,
        WrongCa,
        ExtraSan,
        WrongUri,
        WrongEku,
        Expired,
    }

    fn replace_model_leaf_for_test(tls: &Path, spec: LocalTlsIdentitySpec, case: InvalidLeafCase) {
        if matches!(case, InvalidLeafCase::WrongKey) {
            fs::write(
                tls.join(spec.private_key),
                fs::read(tls.join(full_profile::EGRESS_BROKER_CLIENT_PRIVATE_KEY_FILE)).unwrap(),
            )
            .unwrap();
            return;
        }

        let ca_key = if matches!(case, InvalidLeafCase::WrongCa) {
            KeyPair::generate().unwrap()
        } else {
            KeyPair::from_pem(&fs::read_to_string(tls.join(RUNTIME_CA_PRIVATE_KEY_FILE)).unwrap())
                .unwrap()
        };
        let issuer = Issuer::new(local_runtime_ca_parameters(tls).unwrap(), ca_key);
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        let expected_uri = spec.workload_identity.unwrap();
        let uris = match case {
            InvalidLeafCase::ExtraSan => vec![
                expected_uri,
                "spiffe://insight.platform/workload/unexpected",
            ],
            InvalidLeafCase::WrongUri => {
                vec!["spiffe://insight.platform/workload/unexpected"]
            }
            _ => vec![expected_uri],
        };
        for uri in uris {
            params
                .subject_alt_names
                .push(SanType::URI(uri.try_into().unwrap()));
        }
        params.use_authority_key_identifier_extension = true;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![if matches!(case, InvalidLeafCase::WrongEku) {
            ExtendedKeyUsagePurpose::ServerAuth
        } else {
            ExtendedKeyUsagePurpose::ClientAuth
        }];
        if matches!(case, InvalidLeafCase::Expired) {
            params.not_before = rcgen::date_time_ymd(2000, 1, 1);
            params.not_after = rcgen::date_time_ymd(2001, 1, 1);
        }
        let key = KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &issuer).unwrap();
        fs::write(tls.join(spec.private_key), key.serialize_pem()).unwrap();
        fs::write(tls.join(spec.certificate), certificate.pem()).unwrap();
    }

    #[test]
    fn local_tls_leaf_validation_rejects_wrong_identity_semantics() {
        let model = DevProfile::parse(Some("model"), false, true).unwrap();
        let spec = expected_local_tls_leaf_identities(model)
            [full_profile::MODEL_WORKER_CLIENT_CERTIFICATE_FILE];
        for case in [
            InvalidLeafCase::WrongKey,
            InvalidLeafCase::WrongCa,
            InvalidLeafCase::ExtraSan,
            InvalidLeafCase::WrongUri,
            InvalidLeafCase::WrongEku,
            InvalidLeafCase::Expired,
        ] {
            let directory = TempDir::new().unwrap();
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
            let state_directory = directory.path().join(PROJECT_DIRECTORY);
            let tls = state_directory
                .join(RUNTIME_DIRECTORY)
                .join(RUNTIME_TLS_DIRECTORY);
            ensure_selected_feature_identity(&state_directory, model, None).unwrap();
            replace_model_leaf_for_test(&tls, spec, case);
            assert!(
                validate_local_tls_leaf_pair(&tls, spec).is_err(),
                "{case:?}"
            );
        }
    }

    #[test]
    fn uncommitted_feature_tls_partial_pair_is_rebuilt_but_symlink_is_rejected() {
        let directory = TempDir::new().unwrap();
        initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        let state_directory = directory.path().join(PROJECT_DIRECTORY);
        let runtime = state_directory.join(RUNTIME_DIRECTORY);
        let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
        let model = DevProfile::parse(Some("model"), false, true).unwrap();
        ensure_selected_feature_identity(&state_directory, model, None).unwrap();
        let certificate = tls.join(full_profile::MODEL_WORKER_CLIENT_CERTIFICATE_FILE);
        let private_key = tls.join(full_profile::MODEL_WORKER_CLIENT_PRIVATE_KEY_FILE);
        let original_certificate = fs::read(&certificate).unwrap();
        let original_key = fs::read(&private_key).unwrap();
        ensure_selected_feature_identity(&state_directory, model, None).unwrap();
        assert_eq!(fs::read(&certificate).unwrap(), original_certificate);
        assert_eq!(fs::read(&private_key).unwrap(), original_key);

        fs::remove_file(&certificate).unwrap();
        ensure_selected_feature_identity(&state_directory, model, None).unwrap();
        assert_ne!(fs::read(&private_key).unwrap(), original_key);
        validate_local_tls_leaf_pair(
            &tls,
            expected_local_tls_leaf_identities(model)
                [full_profile::MODEL_WORKER_CLIENT_CERTIFICATE_FILE],
        )
        .unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            fs::remove_file(&certificate).unwrap();
            symlink(tls.join(RUNTIME_CA_CERTIFICATE_FILE), &certificate).unwrap();
            let error =
                ensure_selected_feature_identity(&state_directory, model, None).unwrap_err();
            assert!(error.to_string().contains("bounded physical single-link"));

            fs::remove_file(&certificate).unwrap();
            ensure_selected_feature_identity(&state_directory, model, None).unwrap();
            fs::remove_file(&private_key).unwrap();
            symlink(tls.join(RUNTIME_CA_PRIVATE_KEY_FILE), private_key.as_path()).unwrap();
            let error =
                ensure_selected_feature_identity(&state_directory, model, None).unwrap_err();
            assert!(error.to_string().contains("bounded physical single-link"));
        }
    }

    #[test]
    fn committed_tls_identity_is_never_rebuilt_by_feature_preparation() {
        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let state_directory = directory.path().join(PROJECT_DIRECTORY);
        let runtime = state_directory.join(RUNTIME_DIRECTORY);
        let state = read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
        let private_key = tls.join(RUNTIME_ARTIFACT_GATEWAY_PRIVATE_KEY_FILE);
        let wrong_key = fs::read(tls.join(RUNTIME_ARTIFACT_DATA_PRIVATE_KEY_FILE)).unwrap();
        fs::write(&private_key, &wrong_key).unwrap();
        let model = DevProfile::parse(Some("model"), false, true).unwrap();

        let error = ensure_selected_feature_identity(
            &state_directory,
            model,
            Some(&state.tls_identity_digests),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CliError::RuntimeState(detail) if detail.contains("committed TLS identity")
        ));
        assert_eq!(fs::read(private_key).unwrap(), wrong_key);
    }

    #[cfg(unix)]
    #[test]
    fn tls_identity_closure_rejects_a_symlinked_tls_directory() {
        use std::os::unix::fs::symlink;

        let directory = TempDir::new().unwrap();
        initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        let state_directory = directory.path().join(PROJECT_DIRECTORY);
        let runtime = state_directory.join(RUNTIME_DIRECTORY);
        let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
        let external = directory.path().join("external-tls");
        fs::rename(&tls, &external).unwrap();
        symlink(&external, &tls).unwrap();

        let inspect_error =
            inspect_local_tls_identity_closure(&tls, DevProfile::starter()).unwrap_err();
        assert!(inspect_error.contains("physical TLS directory"));
        let ensure_error = ensure_selected_feature_identity(
            &state_directory,
            DevProfile::parse(Some("model"), false, true).unwrap(),
            None,
        )
        .unwrap_err();
        assert!(ensure_error.to_string().contains("physical TLS directory"));
    }

    #[test]
    fn local_project_state_rejects_legacy_identity_and_open_shapes() {
        let directory = TempDir::new().unwrap();
        let state = initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        let state_directory = directory.path().join(PROJECT_DIRECTORY);

        let mut legacy_identity = state.identity.clone();
        legacy_identity.schema_version = 2;
        legacy_identity.egress_broker_principal_id.clear();
        assert!(matches!(
            validate_loaded_local_identity(&state_directory, &legacy_identity),
            Err(CliError::InvalidLocalIdentity { .. })
        ));

        let mut missing_identity = serde_json::to_value(&state).unwrap();
        missing_identity["identity"]
            .as_object_mut()
            .unwrap()
            .remove("egress_broker_principal_id");
        assert!(serde_json::from_value::<LocalProjectState>(missing_identity).is_err());

        let mut open_profile = serde_json::to_value(&state).unwrap();
        open_profile["profiles"]["starter"]["compatibility_mode"] = serde_json::json!(true);
        assert!(serde_json::from_value::<LocalProjectState>(open_profile).is_err());

        let project_path = state_directory.join(PROJECT_STATE_FILE);
        let mut wrong_schema = serde_json::to_value(&state).unwrap();
        wrong_schema["schema_version"] = serde_json::json!(0);
        let mut wrong_kind = serde_json::to_value(&state).unwrap();
        wrong_kind["kind"] = serde_json::json!("insight.dev.project/legacy");
        for candidate in [wrong_schema, wrong_kind] {
            fs::write(
                &project_path,
                serde_json::to_vec_pretty(&candidate).unwrap(),
            )
            .unwrap();
            assert!(matches!(
                load_local_project_state(&state_directory),
                Err(CliError::InvalidLocalIdentity { .. })
            ));
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
    fn reset_is_a_two_step_project_scoped_destructive_operation() {
        let directory = TempDir::new().unwrap();
        initialize_project(directory.path(), Some("demo"), UNIX_EPOCH).unwrap();
        let preview = reset_local_project(directory.path(), directory.path(), None).unwrap();
        assert!(preview.contains("Project: demo"));
        assert!(preview.contains(".insight"));
        assert!(preview.contains("--confirm demo"));
        assert!(directory.path().join(PROJECT_DIRECTORY).is_dir());
        assert!(matches!(
            reset_local_project(directory.path(), directory.path(), Some("wrong")),
            Err(CliError::InvalidOptionValue {
                option: "--confirm",
                ..
            })
        ));
        assert!(directory.path().join(PROJECT_DIRECTORY).is_dir());
    }

    #[test]
    fn init_creates_a_missing_project_root() {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("fresh-project");
        assert!(!root.exists());
        let state = initialize_project(&root, None, UNIX_EPOCH).unwrap();
        assert_eq!(state.project_name, "fresh-project");
        assert!(root
            .join(PROJECT_DIRECTORY)
            .join(PROJECT_STATE_FILE)
            .is_file());
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
    fn runtime_profile_is_closed_digest_bound_and_keeps_private_material_out_of_config() {
        let directory = TempDir::new().unwrap();
        initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        let digests = prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        assert_eq!(digests.len(), 8);
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let profile: RuntimeProfileState =
            serde_json::from_slice(&fs::read(runtime.join(RUNTIME_PROFILE_STATE_FILE)).unwrap())
                .unwrap();
        assert_eq!(profile.config_digests, digests);
        let configurations = runtime.join(RUNTIME_CONFIGURATION_DIRECTORY);
        for (role, file) in [
            ("artifact-bootstrap", RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE),
            ("gateway-management", RUNTIME_GATEWAY_MANAGEMENT_CONFIG_FILE),
            ("gateway-runtime", RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE),
            ("artifact-gateway", RUNTIME_ARTIFACT_GATEWAY_CONFIG_FILE),
            ("artifact-data", RUNTIME_ARTIFACT_DATA_CONFIG_FILE),
            ("orchestration", RUNTIME_ORCHESTRATION_CONFIG_FILE),
            ("capability-native", RUNTIME_CAPABILITY_NATIVE_CONFIG_FILE),
            (
                "registry-validation",
                RUNTIME_REGISTRY_VALIDATION_CONFIG_FILE,
            ),
        ] {
            let bytes = fs::read(configurations.join(file)).unwrap();
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                canonical_digest(&value).unwrap(),
                profile.config_digests[role]
            );
            assert!(!String::from_utf8(bytes).unwrap().contains("PRIVATE KEY"));
        }
        let bootstrap: serde_json::Value = serde_json::from_slice(
            &fs::read(configurations.join(RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(bootstrap["environment_class"], "development");
        assert_eq!(bootstrap["scheduling_policy"]["version"], 1);
        assert_eq!(bootstrap["scheduling_policy"]["weight"], 1);
        assert_eq!(bootstrap["scheduling_policy"]["burst"], 2);
        assert_eq!(bootstrap["scheduling_policy"]["aging_rounds"], 2);
        assert!(bootstrap["scheduling_policy_id"]
            .as_str()
            .is_some_and(|value| value.starts_with("pol_")));
        assert!(bootstrap["scheduling_policy_revision_id"]
            .as_str()
            .is_some_and(|value| value.starts_with("prev_")));
        assert!(bootstrap["scheduling_policy_deployment_id"]
            .as_str()
            .is_some_and(|value| value.starts_with("pdep_")));
        assert!(bootstrap["orchestration_quota_account_id"]
            .as_str()
            .is_some_and(|value| value.starts_with("qac_")));
        assert_eq!(bootstrap["orchestration_concurrent_jobs"], 4);
        assert_eq!(
            bootstrap["artifact_io_policy"]["scanner_contract_digest"],
            local_digest("artifact-scanner-contract").unwrap()
        );
        let artifact_data: serde_json::Value = serde_json::from_slice(
            &fs::read(configurations.join(RUNTIME_ARTIFACT_DATA_CONFIG_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            canonical_digest(&bootstrap["artifact_io_policy"]).unwrap(),
            artifact_data["scan_worker"]["ruleset_digest"]
                .as_str()
                .unwrap()
        );
        let orchestration: serde_json::Value = serde_json::from_slice(
            &fs::read(configurations.join(RUNTIME_ORCHESTRATION_CONFIG_FILE)).unwrap(),
        )
        .unwrap();
        assert!(orchestration.get("sandbox").is_none());
        for file in [
            full_profile::MODEL_WORKER_CONFIG_FILE,
            full_profile::CONTEXT_NATIVE_CONFIG_FILE,
            full_profile::MCP_HOST_CONFIG_FILE,
        ] {
            assert!(!configurations.join(file).exists(), "{file}");
        }
        assert!(matches!(
            prepare_runtime_profile(
                directory.path(),
                "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            Err(CliError::ProjectAlreadyInitialized(_))
        ));
    }

    #[test]
    fn runtime_profile_reader_rejects_incomplete_or_non_current_state() {
        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let profile_path = runtime.join(RUNTIME_PROFILE_STATE_FILE);
        let current: serde_json::Value =
            serde_json::from_slice(&fs::read(&profile_path).unwrap()).unwrap();
        let refresh_closure = |candidate: &mut serde_json::Value| {
            let mut state: RuntimeProfileState = serde_json::from_value(candidate.clone()).unwrap();
            refresh_runtime_profile_closure_digest(&mut state).unwrap();
            candidate["closure_digest"] = serde_json::Value::String(state.closure_digest);
        };

        let mut missing_ports = current.clone();
        missing_ports.as_object_mut().unwrap().remove("ports");
        let mut missing_full_ports = current.clone();
        missing_full_ports["ports"]
            .as_object_mut()
            .unwrap()
            .remove("full");
        let mut missing_full_port = current.clone();
        missing_full_port["ports"]["full"]
            .as_object_mut()
            .unwrap()
            .remove("egress_broker");
        let mut wrong_schema = current.clone();
        wrong_schema["schema_version"] = serde_json::json!(1);
        let mut wrong_kind = current.clone();
        wrong_kind["kind"] = serde_json::json!("insight.dev.runtime-profile/deleted");
        let mut empty_secret = current.clone();
        empty_secret["secret_readiness_arn"] = serde_json::json!("");
        let mut invalid_secret = current.clone();
        invalid_secret["secret_readiness_arn"] = serde_json::json!(
            "arn:aws:secretsmanager:us-west-2:000000000000:secret:insight/platform/readiness-wrong"
        );
        let mut empty_kms = current.clone();
        empty_kms["kms_key_arn"] = serde_json::json!("");
        let mut zero_port = current.clone();
        zero_port["ports"]["gateway_runtime"] = serde_json::json!(0);
        refresh_closure(&mut zero_port);
        let mut duplicate_port = current.clone();
        duplicate_port["ports"]["gateway_runtime"] =
            duplicate_port["ports"]["gateway_management"].clone();
        refresh_closure(&mut duplicate_port);
        let mut wrong_tenant = current.clone();
        wrong_tenant["tenant_id"] = serde_json::json!("ten_wrong");
        let mut wrong_identity_digest = current.clone();
        wrong_identity_digest["identity_digest"] = serde_json::json!(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let mut wrong_secret_provider = current.clone();
        wrong_secret_provider["secret_provider_id"] =
            serde_json::to_value(fresh_resource_id(ResourceKind::Principal)).unwrap();
        let mut wrong_capability_protocol_revision = current.clone();
        wrong_capability_protocol_revision["capability_protocol_profile_revision_id"] =
            serde_json::to_value(fresh_resource_id(ResourceKind::Principal)).unwrap();
        let mut mismatched_release = current.clone();
        mismatched_release["release_identity"] = serde_json::json!(
            "source:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let mut wrong_profile_digest = current.clone();
        wrong_profile_digest["profile_digest"] = serde_json::json!(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let mut noncanonical_features = current.clone();
        noncanonical_features["features"] = serde_json::json!(["model", "context"]);
        let mut missing_core_config = current.clone();
        missing_core_config["config_digests"]
            .as_object_mut()
            .unwrap()
            .remove("gateway-runtime");
        let mut missing_tls_closure = current.clone();
        missing_tls_closure
            .as_object_mut()
            .unwrap()
            .remove("tls_identity_digests");
        let mut wrong_tls_digest = current.clone();
        wrong_tls_digest["tls_identity_digests"][RUNTIME_CA_CERTIFICATE_FILE] = serde_json::json!(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        refresh_closure(&mut wrong_tls_digest);
        let mut wrong_config_digest = current.clone();
        wrong_config_digest["config_digests"]["gateway-runtime"] = serde_json::json!(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        refresh_closure(&mut wrong_config_digest);
        let mut wrong_closure_digest = current.clone();
        wrong_closure_digest["closure_digest"] = serde_json::json!(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let mut unknown_field = current.clone();
        unknown_field["compatibility_ports"] = serde_json::json!(true);

        for (name, candidate, expected_error) in [
            ("missing ports", missing_ports, "is not valid closed JSON"),
            (
                "missing full ports",
                missing_full_ports,
                "is not valid closed JSON",
            ),
            (
                "missing full-profile port",
                missing_full_port,
                "is not valid closed JSON",
            ),
            (
                "wrong schema",
                wrong_schema,
                "unsupported runtime profile schema_version",
            ),
            ("wrong kind", wrong_kind, "unsupported runtime profile kind"),
            (
                "empty readiness Secret",
                empty_secret,
                "secret_readiness_arn is empty or invalid",
            ),
            (
                "invalid readiness Secret",
                invalid_secret,
                "secret_readiness_arn is empty or invalid",
            ),
            ("empty KMS ARN", empty_kms, "kms_key_arn"),
            ("zero port", zero_port, "ports must be non-zero"),
            ("duplicate port", duplicate_port, "globally unique"),
            (
                "wrong tenant",
                wrong_tenant,
                "does not match the current local project identity",
            ),
            (
                "wrong identity digest",
                wrong_identity_digest,
                "does not match the current local project identity",
            ),
            (
                "wrong SecretProvider identity kind",
                wrong_secret_provider,
                "secret_provider_id is not a SecretProvider identity",
            ),
            (
                "wrong Capability protocol revision identity kind",
                wrong_capability_protocol_revision,
                "capability_protocol_profile_revision_id is not a PolicyRevision identity",
            ),
            (
                "release/source mismatch",
                mismatched_release,
                "does not match source_fingerprint",
            ),
            (
                "profile digest mismatch",
                wrong_profile_digest,
                "profile_digest does not match",
            ),
            (
                "noncanonical features",
                noncanonical_features,
                "features are not in canonical order",
            ),
            (
                "missing core config",
                missing_core_config,
                "config_digests does not match",
            ),
            (
                "missing TLS closure",
                missing_tls_closure,
                "is not valid closed JSON",
            ),
            (
                "valid but wrong TLS digest",
                wrong_tls_digest,
                "tls_identity_digests does not match",
            ),
            (
                "valid but wrong config digest",
                wrong_config_digest,
                "does not match",
            ),
            (
                "wrong closure digest",
                wrong_closure_digest,
                "closure_digest does not match",
            ),
            ("unknown field", unknown_field, "is not valid closed JSON"),
        ] {
            fs::write(
                &profile_path,
                serde_json::to_vec_pretty(&candidate).unwrap(),
            )
            .unwrap();
            let error = read_runtime_profile_state(&runtime, &project.identity).expect_err(name);
            match error {
                CliError::RuntimeState(detail) => assert!(
                    detail.contains(expected_error),
                    "{name}: expected {expected_error:?}, got {detail:?}"
                ),
                other => panic!("{name}: unexpected error: {other}"),
            }
        }

        fs::write(&profile_path, serde_json::to_vec_pretty(&current).unwrap()).unwrap();
        let config_path = runtime
            .join(RUNTIME_CONFIGURATION_DIRECTORY)
            .join(RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE);
        let mut config: serde_json::Value =
            serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
        config["tampered"] = serde_json::json!(true);
        fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        let error = read_runtime_profile_state(&runtime, &project.identity).unwrap_err();
        assert!(matches!(
            error,
            CliError::RuntimeState(detail) if detail.contains("config_digests") && detail.contains("gateway-runtime.json")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn runtime_state_and_config_directory_reject_symlinks_and_duplicate_json() {
        use std::os::unix::fs::symlink;

        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let configuration = runtime.join(RUNTIME_CONFIGURATION_DIRECTORY);
        let physical_configuration = runtime.join("config-physical");
        fs::rename(&configuration, &physical_configuration).unwrap();
        symlink(&physical_configuration, &configuration).unwrap();
        let error = read_runtime_profile_state(&runtime, &project.identity).unwrap_err();
        assert!(matches!(
            error,
            CliError::RuntimeState(detail) if detail.contains("not a physical directory")
        ));
        fs::remove_file(&configuration).unwrap();
        fs::rename(&physical_configuration, &configuration).unwrap();

        let profile = runtime.join(RUNTIME_PROFILE_STATE_FILE);
        let physical_profile = runtime.join("profile-physical.json");
        fs::rename(&profile, &physical_profile).unwrap();
        symlink(&physical_profile, &profile).unwrap();
        let error = read_runtime_profile_state(&runtime, &project.identity).unwrap_err();
        assert!(matches!(
            error,
            CliError::RuntimeState(detail) if detail.contains("bounded regular runtime state file")
        ));
        fs::remove_file(&profile).unwrap();
        fs::rename(&physical_profile, &profile).unwrap();

        let mut duplicate = fs::read_to_string(&profile).unwrap();
        let closing = duplicate.rfind('}').unwrap();
        duplicate.insert_str(closing, ",\n  \"schema_version\": 3\n");
        fs::write(&profile, duplicate).unwrap();
        let error = read_runtime_profile_state(&runtime, &project.identity).unwrap_err();
        assert!(matches!(
            error,
            CliError::RuntimeState(detail) if detail.contains("not valid closed JSON")
        ));
    }

    #[test]
    fn runtime_process_state_is_closed_and_bound_to_current_profile_identity() {
        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let profile = read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        let compose_project = compose_project_name(&project.identity.tenant_id).unwrap();
        let binding = runtime_process_binding(
            &runtime,
            &project.identity.tenant_id,
            &compose_project,
            &profile,
            &project.identity,
        )
        .unwrap();
        let generation = format!("{RUNTIME_PROCESS_GENERATION_PREFIX}{}", Uuid::now_v7());
        let state = RuntimeProcessState {
            schema_version: RUNTIME_PROCESS_SCHEMA_VERSION,
            kind: RUNTIME_PROCESS_KIND.to_owned(),
            tenant_id: binding.tenant_id.clone(),
            profile: binding.profile.clone(),
            profile_digest: binding.profile_digest.clone(),
            release_identity: binding.release_identity.clone(),
            compose_project: binding.compose_project.clone(),
            source_fingerprint: binding.source_fingerprint.clone(),
            lifecycle: RuntimeProcessLifecycle::Starting,
            processes: BTreeMap::from([(
                "gateway-runtime".to_owned(),
                RuntimeProcessRecord {
                    pid: std::process::id(),
                    generation: generation.clone(),
                    ready_address: "127.0.0.1:8080".to_owned(),
                    log_file: "logs/gateway-runtime.log".to_owned(),
                },
            )]),
        };
        let process_path = runtime.join(RUNTIME_PROCESS_STATE_FILE);
        write_runtime_json_replace(&process_path, &state).unwrap();
        assert_eq!(
            read_runtime_process_state(&runtime, &binding)
                .unwrap()
                .unwrap(),
            state
        );
        let running_processes = binding
            .expected_processes
            .iter()
            .enumerate()
            .map(|(index, (role, ready_address))| {
                (
                    role.clone(),
                    RuntimeProcessRecord {
                        pid: 10_000 + u32::try_from(index).unwrap(),
                        generation: format!(
                            "{RUNTIME_PROCESS_GENERATION_PREFIX}{}",
                            Uuid::now_v7()
                        ),
                        ready_address: ready_address.clone(),
                        log_file: format!("{RUNTIME_LOG_DIRECTORY}/{role}.log"),
                    },
                )
            })
            .collect();
        let mut running = state.clone();
        running.lifecycle = RuntimeProcessLifecycle::Running;
        running.processes = running_processes;
        write_runtime_json_replace(&process_path, &running).unwrap();
        assert_eq!(
            read_runtime_process_state(&runtime, &binding)
                .unwrap()
                .unwrap(),
            running
        );
        let mut stopped = state.clone();
        stopped.lifecycle = RuntimeProcessLifecycle::Stopped;
        stopped.processes.clear();
        write_runtime_json_replace(&process_path, &stopped).unwrap();
        assert_eq!(
            read_runtime_process_state(&runtime, &binding)
                .unwrap()
                .unwrap(),
            stopped
        );
        clear_quiescent_runtime_process_state(&runtime, &binding).unwrap();
        assert!(!process_path.exists());
        write_runtime_json_replace(&process_path, &state).unwrap();
        let current = serde_json::to_value(&state).unwrap();
        let mut wrong_schema = current.clone();
        wrong_schema["schema_version"] = serde_json::json!(1);
        let mut wrong_kind = current.clone();
        wrong_kind["kind"] = serde_json::json!("insight.dev.process-state/v1");
        let mut wrong_tenant = current.clone();
        wrong_tenant["tenant_id"] = serde_json::json!("ten_wrong");
        let mut wrong_profile = current.clone();
        wrong_profile["profile_digest"] = serde_json::json!(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let mut missing_generation = current.clone();
        missing_generation["processes"]["gateway-runtime"]
            .as_object_mut()
            .unwrap()
            .remove("generation");
        let mut non_v7_generation = current.clone();
        non_v7_generation["processes"]["gateway-runtime"]["generation"] =
            serde_json::json!("insight-platform-process-v2-00000000-0000-4000-8000-000000000000");
        let mut duplicate_generation = current.clone();
        duplicate_generation["processes"]["gateway-management"] = serde_json::json!({
            "pid": std::process::id(),
            "generation": generation,
            "ready_address": "127.0.0.1:8081",
            "log_file": "logs/gateway-management.log"
        });
        let mut missing_lifecycle = current.clone();
        missing_lifecycle
            .as_object_mut()
            .unwrap()
            .remove("lifecycle");
        let mut partial_running = current.clone();
        partial_running["lifecycle"] = serde_json::json!("running");
        let mut nonempty_stopped = current.clone();
        nonempty_stopped["lifecycle"] = serde_json::json!("stopped");
        let mut wrong_endpoint = current.clone();
        wrong_endpoint["processes"]["gateway-runtime"]["ready_address"] =
            serde_json::json!("127.0.0.1:8081");
        let mut duplicate_pid = current.clone();
        duplicate_pid["processes"]["gateway-management"] = serde_json::json!({
            "pid": std::process::id(),
            "generation": format!("{RUNTIME_PROCESS_GENERATION_PREFIX}{}", Uuid::now_v7()),
            "ready_address": "127.0.0.1:8081",
            "log_file": "logs/gateway-management.log"
        });
        let mut duplicate_address = current.clone();
        duplicate_address["processes"]["gateway-management"] = serde_json::json!({
            "pid": std::process::id().saturating_add(1),
            "generation": format!("{RUNTIME_PROCESS_GENERATION_PREFIX}{}", Uuid::now_v7()),
            "ready_address": "127.0.0.1:8080",
            "log_file": "logs/gateway-management.log"
        });
        let mut extra_role = current.clone();
        extra_role["processes"]["legacy-runner"] = serde_json::json!({
            "pid": std::process::id().saturating_add(2),
            "generation": format!("{RUNTIME_PROCESS_GENERATION_PREFIX}{}", Uuid::now_v7()),
            "ready_address": "127.0.0.1:65535",
            "log_file": "logs/legacy-runner.log"
        });
        let mut unknown_field = current.clone();
        unknown_field["processes"]["gateway-runtime"]["legacy_pid"] = serde_json::json!(true);

        for (name, candidate, expected_error) in [
            (
                "wrong schema",
                wrong_schema,
                "unsupported runtime process schema_version",
            ),
            ("wrong kind", wrong_kind, "unsupported runtime process kind"),
            (
                "wrong tenant",
                wrong_tenant,
                "does not match the current tenant/profile/release/source binding",
            ),
            (
                "wrong profile",
                wrong_profile,
                "does not match the current tenant/profile/release/source binding",
            ),
            (
                "missing generation",
                missing_generation,
                "is not valid closed JSON",
            ),
            (
                "non-v7 generation",
                non_v7_generation,
                "invalid or duplicate process identity",
            ),
            (
                "duplicate generation",
                duplicate_generation,
                "invalid or duplicate process identity",
            ),
            (
                "missing lifecycle",
                missing_lifecycle,
                "is not valid closed JSON",
            ),
            (
                "running with fewer roles",
                partial_running,
                "running state does not contain the exact selected process closure",
            ),
            (
                "stopped with records",
                nonempty_stopped,
                "stopped state must not contain process records",
            ),
            (
                "wrong role endpoint",
                wrong_endpoint,
                "invalid or duplicate process identity",
            ),
            (
                "duplicate pid",
                duplicate_pid,
                "invalid or duplicate process identity",
            ),
            (
                "duplicate address",
                duplicate_address,
                "invalid or duplicate process identity",
            ),
            (
                "extra role",
                extra_role,
                "invalid or duplicate process identity",
            ),
            ("unknown field", unknown_field, "is not valid closed JSON"),
        ] {
            fs::write(
                &process_path,
                serde_json::to_vec_pretty(&candidate).unwrap(),
            )
            .unwrap();
            let error = read_runtime_process_state(&runtime, &binding).expect_err(name);
            match error {
                CliError::RuntimeState(detail) => assert!(
                    detail.contains(expected_error),
                    "{name}: expected {expected_error:?}, got {detail:?}"
                ),
                other => panic!("{name}: unexpected error: {other}"),
            }
        }
    }

    #[test]
    fn runtime_profile_transitions_separate_identity_changes_from_feature_additions() {
        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        let original_fingerprint =
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            original_fingerprint,
        )
        .unwrap();
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let state = read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        let next_fingerprint =
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let next_release = format!("release:1.2.3:{next_fingerprint}");

        validate_runtime_profile_transition(
            &state,
            DevProfile::starter(),
            &next_release,
            next_fingerprint,
        )
        .unwrap();
        let changed_identity_and_features = validate_runtime_profile_transition(
            &state,
            DevProfile::parse(Some("model"), false, false).unwrap(),
            &next_release,
            next_fingerprint,
        )
        .unwrap_err();
        assert!(matches!(
            changed_identity_and_features,
            CliError::RuntimeState(detail) if detail.contains("cannot add or remove development features")
        ));
        validate_runtime_profile_transition(
            &state,
            DevProfile::parse(Some("model"), false, true).unwrap(),
            &state.release_identity,
            &state.source_fingerprint,
        )
        .unwrap();
    }

    #[test]
    fn cleanup_validation_ignores_only_external_config_availability() {
        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let profile = read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        let compose_project = compose_project_name(&project.identity.tenant_id).unwrap();
        let binding = runtime_process_binding_for_cleanup(
            &runtime,
            &project.identity.tenant_id,
            &compose_project,
            &profile,
            &project.identity,
        )
        .unwrap();
        let state = RuntimeProcessState {
            schema_version: RUNTIME_PROCESS_SCHEMA_VERSION,
            kind: RUNTIME_PROCESS_KIND.to_owned(),
            tenant_id: binding.tenant_id.clone(),
            profile: binding.profile.clone(),
            profile_digest: binding.profile_digest.clone(),
            release_identity: binding.release_identity.clone(),
            compose_project: binding.compose_project.clone(),
            source_fingerprint: binding.source_fingerprint.clone(),
            lifecycle: RuntimeProcessLifecycle::Stopped,
            processes: BTreeMap::new(),
        };
        write_runtime_json_replace(&runtime.join(RUNTIME_PROCESS_STATE_FILE), &state).unwrap();
        let config = runtime
            .join(RUNTIME_CONFIGURATION_DIRECTORY)
            .join(RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE);
        fs::write(&config, b"{\"tampered\":true}").unwrap();

        assert!(read_runtime_profile_state(&runtime, &project.identity).is_err());
        let cleanup_profile = read_runtime_profile_state_for_cleanup(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        let cleanup_binding = runtime_process_binding_for_cleanup(
            &runtime,
            &project.identity.tenant_id,
            &compose_project,
            &cleanup_profile,
            &project.identity,
        )
        .unwrap();
        assert!(read_runtime_process_state(&runtime, &cleanup_binding)
            .unwrap()
            .is_some());
        stop_development_profile(directory.path(), directory.path()).unwrap();

        fs::remove_file(config).unwrap();
        assert!(read_runtime_profile_state(&runtime, &project.identity).is_err());
        assert!(
            read_runtime_profile_state_for_cleanup(&runtime, &project.identity)
                .unwrap()
                .is_some()
        );

        let identity_directory = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(IDENTITY_DIRECTORY);
        fs::remove_file(identity_directory.join(IDENTITY_JWKS_FILE)).unwrap();
        fs::remove_file(identity_directory.join(IDENTITY_BOOTSTRAP_CONFIG_FILE)).unwrap();
        assert!(validate_loaded_local_identity(
            &directory.path().join(PROJECT_DIRECTORY),
            &project.identity
        )
        .is_err());
        validate_loaded_local_identity_for_cleanup(
            &directory.path().join(PROJECT_DIRECTORY),
            &project.identity,
        )
        .unwrap();
        stop_development_profile(directory.path(), directory.path()).unwrap();
        let reset_preview = reset_local_project(directory.path(), directory.path(), None).unwrap();
        assert!(reset_preview.contains("Confirm with:"));
    }

    #[test]
    fn committed_tls_identity_drift_fails_full_validation_only() {
        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let runtime = directory
            .path()
            .join(PROJECT_DIRECTORY)
            .join(RUNTIME_DIRECTORY);
        let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
        fs::write(
            tls.join(RUNTIME_ARTIFACT_GATEWAY_CERTIFICATE_FILE),
            fs::read(tls.join(RUNTIME_ARTIFACT_DATA_CERTIFICATE_FILE)).unwrap(),
        )
        .unwrap();
        let error = read_runtime_profile_state(&runtime, &project.identity).unwrap_err();
        assert!(matches!(
            error,
            CliError::RuntimeState(detail) if detail.contains("runtime TLS identity is invalid")
        ));
        assert!(
            read_runtime_profile_state_for_cleanup(&runtime, &project.identity)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn restart_selection_and_summary_follow_validated_runtime_authority() {
        let directory = TempDir::new().unwrap();
        let mut project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let state_directory = directory.path().join(PROJECT_DIRECTORY);
        let runtime = state_directory.join(RUNTIME_DIRECTORY);
        let mut state = read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        let model = DevProfile::parse(Some("model"), false, true).unwrap();
        ensure_selected_feature_identity(&state_directory, model, None).unwrap();
        append_selected_feature_configs(&state_directory, &project.identity, &mut state, model)
            .unwrap();
        state.features = model
            .feature_names()
            .into_iter()
            .map(str::to_owned)
            .collect();
        state.profile_digest = model.profile_digest(&state.release_identity).unwrap();
        state.tls_identity_digests =
            inspect_local_tls_identity_closure(&runtime.join(RUNTIME_TLS_DIRECTORY), model)
                .unwrap();
        refresh_runtime_profile_closure_digest(&mut state).unwrap();
        write_runtime_json_replace(&runtime.join(RUNTIME_PROFILE_STATE_FILE), &state).unwrap();
        let persisted = read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();

        project.profiles = BTreeMap::from([(
            "starter".to_owned(),
            LocalProfileState {
                state: "stopped".to_owned(),
                features: Vec::new(),
                profile_digest: None,
                release_identity: Some(
                    "release:1.2.3:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
                ),
            },
        )]);
        let selected = restart_profile_selection(&persisted).unwrap();
        assert_eq!(selected.feature_names(), model.feature_names());
        assert!(selected.is_from_source());
        let summary = runtime_project_profile_summary(&persisted, "ready");
        assert_ne!(project.profiles, summary);
        assert_eq!(summary["starter"].features, persisted.features);
        assert_eq!(
            summary["starter"].release_identity.as_deref(),
            Some(persisted.release_identity.as_str())
        );

        let expected = RuntimeRestartIdentity {
            release_identity: persisted.release_identity.clone(),
            source_fingerprint: persisted.source_fingerprint.clone(),
        };
        validate_restart_identity(
            &expected,
            &persisted.release_identity,
            &persisted.source_fingerprint,
        )
        .unwrap();
        assert!(validate_restart_identity(
            &expected,
            "release:1.2.3:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        .is_err());
    }

    #[cfg(unix)]
    #[test]
    fn lifecycle_lock_is_single_owner_and_released_by_raii() {
        let directory = TempDir::new().unwrap();
        initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        let first = acquire_runtime_lifecycle_lock(directory.path()).unwrap();
        let owner = fs::read_to_string(
            directory
                .path()
                .join(PROJECT_DIRECTORY)
                .join(RUNTIME_DIRECTORY)
                .join(RUNTIME_LIFECYCLE_LOCK_FILE),
        )
        .unwrap();
        assert_eq!(
            owner,
            format!("schema_version=2 pid={}\n", std::process::id())
        );
        let error = acquire_runtime_lifecycle_lock(directory.path()).unwrap_err();
        assert!(matches!(
            error,
            CliError::RuntimeUnavailable(detail) if detail.contains("already mutating")
        ));
        drop(first);
        acquire_runtime_lifecycle_lock(directory.path()).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn runtime_process_does_not_exec_when_start_journal_persistence_fails() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = TempDir::new().unwrap();
        let marker = directory.path().join("executed");
        let executable = directory.path().join("runtime-fixture.sh");
        fs::write(
            &executable,
            format!("#!/bin/sh\nprintf ran > \"{}\"\n", marker.display()),
        )
        .unwrap();
        let mut permissions = fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&executable, permissions).unwrap();
        let logs = directory.path().join(RUNTIME_LOG_DIRECTORY);
        fs::create_dir(&logs).unwrap();
        let spec = RuntimeLaunchSpec::new(
            "gateway-runtime",
            executable,
            "127.0.0.1:1",
            Vec::new(),
            Vec::new(),
        );
        let error = spawn_runtime_process(&logs, &spec, |_| {
            assert!(!marker.exists());
            Err(CliError::RuntimeState(
                "injected journal persistence failure".to_owned(),
            ))
        })
        .unwrap_err();

        assert!(matches!(
            error,
            CliError::RuntimeState(detail) if detail == "injected journal persistence failure"
        ));
        assert!(!marker.exists());
    }

    #[cfg(unix)]
    #[test]
    fn process_generation_matches_argv0_and_mismatch_is_never_signalled() {
        use std::os::unix::process::CommandExt as _;

        let generation = format!("{RUNTIME_PROCESS_GENERATION_PREFIX}{}", Uuid::now_v7());
        let mut command = ProcessCommand::new("/bin/sleep");
        command
            .arg0(&generation)
            .arg("30")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command.spawn().unwrap();
        let record = RuntimeProcessRecord {
            pid: child.id(),
            generation,
            ready_address: "127.0.0.1:1".to_owned(),
            log_file: "logs/test.log".to_owned(),
        };
        let mut observed = Ok(RuntimeProcessObservation::Stopped);
        for _ in 0..50 {
            observed = observe_runtime_process(&record);
            if !matches!(&observed, Ok(RuntimeProcessObservation::Stopped)) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut mismatched = record.clone();
        mismatched.generation = format!("{RUNTIME_PROCESS_GENERATION_PREFIX}{}", Uuid::now_v7());
        let mismatch_observation = observe_runtime_process(&mismatched);
        let reconciled = stop_process(&mismatched);
        let alive_after_reconciliation = child.try_wait().unwrap().is_none();
        let stopped = stop_process(&record);
        if stopped.is_err() {
            let _ = child.kill();
        }
        let _ = child.wait();

        assert_eq!(observed.unwrap(), RuntimeProcessObservation::Owned);
        assert_eq!(
            mismatch_observation.unwrap(),
            RuntimeProcessObservation::IdentityMismatch
        );
        reconciled.unwrap();
        assert!(alive_after_reconciliation);
        stopped.unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn abort_reconciles_owned_processes_without_signalling_mismatched_pid() {
        use std::os::unix::process::CommandExt as _;

        let directory = TempDir::new().unwrap();
        let runtime = directory.path();
        let owned_generation = format!("{RUNTIME_PROCESS_GENERATION_PREFIX}{}", Uuid::now_v7());
        let mut owned_child = ProcessCommand::new("/bin/sleep")
            .arg0(&owned_generation)
            .arg("30")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let foreign_generation = format!("{RUNTIME_PROCESS_GENERATION_PREFIX}{}", Uuid::now_v7());
        let mut foreign_child = ProcessCommand::new("/bin/sleep")
            .arg0(&foreign_generation)
            .arg("30")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut state = RuntimeProcessState {
            schema_version: RUNTIME_PROCESS_SCHEMA_VERSION,
            kind: RUNTIME_PROCESS_KIND.to_owned(),
            tenant_id: "ten_test".to_owned(),
            profile: "starter".to_owned(),
            profile_digest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            release_identity:
                "source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            compose_project: "insight-test".to_owned(),
            source_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            lifecycle: RuntimeProcessLifecycle::Starting,
            processes: BTreeMap::from([
                (
                    "owned".to_owned(),
                    RuntimeProcessRecord {
                        pid: owned_child.id(),
                        generation: owned_generation,
                        ready_address: "127.0.0.1:1".to_owned(),
                        log_file: "logs/owned.log".to_owned(),
                    },
                ),
                (
                    "stale".to_owned(),
                    RuntimeProcessRecord {
                        pid: foreign_child.id(),
                        generation: format!(
                            "{RUNTIME_PROCESS_GENERATION_PREFIX}{}",
                            Uuid::now_v7()
                        ),
                        ready_address: "127.0.0.1:2".to_owned(),
                        log_file: "logs/stale.log".to_owned(),
                    },
                ),
            ]),
        };
        std::thread::sleep(Duration::from_millis(20));
        let cause = abort_runtime_start(
            runtime,
            &mut state,
            CliError::RuntimeUnavailable("injected".to_owned()),
        );
        assert!(matches!(cause, CliError::RuntimeUnavailable(detail) if detail == "injected"));
        assert_eq!(state.lifecycle, RuntimeProcessLifecycle::Stopped);
        assert!(state.processes.is_empty());
        assert!(foreign_child.try_wait().unwrap().is_none());
        for _ in 0..100 {
            if owned_child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(owned_child.try_wait().unwrap().is_some());
        let _ = foreign_child.kill();
        let _ = foreign_child.wait();
    }

    #[test]
    fn all_features_add_only_the_closed_optional_binary_set() {
        let workspace = Path::new("/workspace");
        let base = runtime_binary_paths(workspace, DevProfile::starter());
        let full = runtime_binary_paths(
            workspace,
            DevProfile::parse(Some("all"), false, true).unwrap(),
        );
        for binary in full_profile::INITIAL_BINARY_NAMES
            .into_iter()
            .filter(|binary| *binary != "platform-artifact-maintenance")
        {
            assert!(!base.contains_key(binary));
            assert!(full.contains_key(binary));
        }
    }

    #[test]
    fn single_feature_requires_only_its_binary_closure() {
        let workspace = Path::new("/workspace");
        let model = runtime_binary_paths(
            workspace,
            DevProfile::parse(Some("model"), false, true).unwrap(),
        );
        assert!(model.contains_key("platform-model-worker"));
        assert!(model.contains_key("platform-security-authority"));
        assert!(model.contains_key("platform-egress-broker"));
        assert!(!model.contains_key("platform-context-worker"));
        assert!(!model.contains_key("platform-mcp-host"));
        assert!(!model.contains_key("platform-sandbox-dispatcher"));
        assert!(!model.contains_key("platform-capability-remote-worker"));
    }

    #[test]
    fn feature_configs_append_without_rotating_starter_authority() {
        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        let kms = "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012";
        let starter = prepare_runtime_profile(
            directory.path(),
            kms,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let state_directory = directory.path().join(PROJECT_DIRECTORY);
        let runtime = state_directory.join(RUNTIME_DIRECTORY);
        let mut state = read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        let model = DevProfile::parse(Some("model"), false, true).unwrap();
        ensure_selected_feature_identity(&state_directory, model, None).unwrap();
        append_selected_feature_configs(&state_directory, &project.identity, &mut state, model)
            .unwrap();
        for (role, digest) in &starter {
            assert_eq!(state.config_digests.get(role), Some(digest));
        }
        assert!(state.config_digests.contains_key("model-worker"));
        assert!(state.config_digests.contains_key("egress-broker"));
        assert!(!state.config_digests.contains_key("context-native"));
        let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
        assert!(tls
            .join(full_profile::MODEL_WORKER_CLIENT_CERTIFICATE_FILE)
            .is_file());
        assert!(!tls
            .join(full_profile::CONTEXT_WORKER_CLIENT_CERTIFICATE_FILE)
            .exists());

        let all = DevProfile::parse(Some("all"), false, true).unwrap();
        ensure_selected_feature_identity(&state_directory, all, None).unwrap();
        append_selected_feature_configs(&state_directory, &project.identity, &mut state, all)
            .unwrap();
        assert_eq!(state.config_digests.len(), 23);
        for role in [
            "context-native",
            "model-worker",
            "capability-remote",
            "mcp-host",
        ] {
            assert!(state.config_digests.contains_key(role), "{role}");
        }
        assert!(state.config_digests.contains_key("sandbox-kubernetes"));
        let sandbox_config = fs::read_to_string(
            runtime
                .join(RUNTIME_CONFIGURATION_DIRECTORY)
                .join(RUNTIME_SANDBOX_KUBERNETES_CONFIG_FILE),
        )
        .unwrap();
        assert!(sandbox_config.contains(OPENSANDBOX_SOURCE_COMMIT));
        assert!(sandbox_config.contains(OPENSANDBOX_SERVER_IMAGE_DIGEST));
        assert!(!sandbox_config.contains("api_key"));
        assert!(!sandbox_config.contains("endpoint"));
        assert!(tls
            .join(full_profile::CONTEXT_WORKER_CLIENT_CERTIFICATE_FILE)
            .is_file());
        let orchestration: serde_json::Value = serde_json::from_slice(
            &fs::read(
                runtime
                    .join(RUNTIME_CONFIGURATION_DIRECTORY)
                    .join(RUNTIME_ORCHESTRATION_CONFIG_FILE),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(orchestration.get("sandbox").is_none());
    }

    #[test]
    fn additive_feature_config_recovers_only_an_exact_preexisting_file() {
        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let state_directory = directory.path().join(PROJECT_DIRECTORY);
        let runtime = state_directory.join(RUNTIME_DIRECTORY);
        let mut state = read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        let selected = DevProfile::parse(Some("model"), false, true).unwrap();
        ensure_selected_feature_identity(&state_directory, selected, None).unwrap();
        let artifact_catalog = local_artifact_provider_catalog(&state.kms_key_arn).unwrap();
        let secret_catalog = local_secret_provider_catalog(
            &state.kms_key_arn,
            &state.secret_readiness_arn,
            &state.secret_provider_id,
        )
        .unwrap();
        let generated = selected_feature_configs(
            &runtime,
            &state.ports,
            &project.identity,
            &artifact_catalog,
            &secret_catalog,
            &state.capability_protocol_profile_revision_id,
            selected,
        )
        .unwrap();
        let (file, config) = generated.get("egress-broker").unwrap();
        let config_path = runtime.join(RUNTIME_CONFIGURATION_DIRECTORY).join(file);
        write_new(&config_path, &serde_json::to_vec_pretty(config).unwrap()).unwrap();

        read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        append_selected_feature_configs(&state_directory, &project.identity, &mut state, selected)
            .unwrap();
        assert!(state.config_digests.contains_key("egress-broker"));

        state.config_digests.remove("egress-broker");
        let mut tampered = config.clone();
        tampered["tampered"] = serde_json::json!(true);
        fs::write(&config_path, serde_json::to_vec_pretty(&tampered).unwrap()).unwrap();
        let error = append_selected_feature_configs(
            &state_directory,
            &project.identity,
            &mut state,
            selected,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            CliError::RuntimeState(detail) if detail.contains("does not match the generated closure")
        ));
    }

    #[test]
    fn remote_capability_additive_recovery_reuses_persisted_protocol_revision() {
        let directory = TempDir::new().unwrap();
        let project =
            initialize_project(directory.path(), Some("demo"), SystemTime::now()).unwrap();
        prepare_runtime_profile(
            directory.path(),
            "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let state_directory = directory.path().join(PROJECT_DIRECTORY);
        let runtime = state_directory.join(RUNTIME_DIRECTORY);
        let mut state = read_runtime_profile_state(&runtime, &project.identity)
            .unwrap()
            .unwrap();
        let selected = DevProfile::parse(Some("remote-capability"), false, true).unwrap();
        ensure_selected_feature_identity(&state_directory, selected, None).unwrap();
        let artifact_catalog = local_artifact_provider_catalog(&state.kms_key_arn).unwrap();
        let secret_catalog = local_secret_provider_catalog(
            &state.kms_key_arn,
            &state.secret_readiness_arn,
            &state.secret_provider_id,
        )
        .unwrap();
        let first_generation = selected_feature_configs(
            &runtime,
            &state.ports,
            &project.identity,
            &artifact_catalog,
            &secret_catalog,
            &state.capability_protocol_profile_revision_id,
            selected,
        )
        .unwrap();
        let (file, config) = first_generation.get("capability-remote").unwrap();
        let path = runtime.join(RUNTIME_CONFIGURATION_DIRECTORY).join(file);
        write_new(&path, &serde_json::to_vec_pretty(config).unwrap()).unwrap();

        let retry_generation = selected_feature_configs(
            &runtime,
            &state.ports,
            &project.identity,
            &artifact_catalog,
            &secret_catalog,
            &state.capability_protocol_profile_revision_id,
            selected,
        )
        .unwrap();
        assert_eq!(
            canonical_digest(config).unwrap(),
            canonical_digest(&retry_generation["capability-remote"].1).unwrap()
        );
        append_selected_feature_configs(&state_directory, &project.identity, &mut state, selected)
            .unwrap();
        assert!(state.config_digests.contains_key("capability-remote"));
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

    #[test]
    fn command_parser_accepts_starter_feature_lifecycle_commands() {
        assert!(matches!(
            parse_command(&[
                OsString::from("dev"),
                OsString::from("--path"),
                OsString::from("demo"),
                OsString::from("--features"),
                OsString::from("context,model"),
                OsString::from("--from-source"),
            ]),
            Ok(CliCommand::Dev {
                root,
                profile,
            }) if root == Path::new("demo")
                && profile.feature_names() == ["context", "model"]
                && profile.is_from_source()
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("start"),
                OsString::from("--path"),
                OsString::from("demo"),
            ]),
            Ok(CliCommand::Start { root }) if root == Path::new("demo")
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("status"),
                OsString::from("--path"),
                OsString::from("demo"),
            ]),
            Ok(CliCommand::Status { root }) if root == Path::new("demo")
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("logs"),
                OsString::from("--path"),
                OsString::from("demo"),
                OsString::from("--role"),
                OsString::from("artifact-data"),
            ]),
            Ok(CliCommand::Logs {
                root,
                role: Some(role),
            }) if root == Path::new("demo") && role == "artifact-data"
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("stop"),
                OsString::from("--path"),
                OsString::from("demo"),
            ]),
            Ok(CliCommand::Stop { root }) if root == Path::new("demo")
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("reset"),
                OsString::from("--path"),
                OsString::from("demo"),
                OsString::from("--confirm"),
                OsString::from("demo-project"),
            ]),
            Ok(CliCommand::Reset { root, confirm: Some(confirm) })
                if root == Path::new("demo") && confirm == "demo-project"
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("logs"),
                OsString::from("--role"),
                OsString::from("registry-validation"),
            ]),
            Ok(CliCommand::Logs {
                role: Some(role),
                ..
            }) if role == "registry-validation"
        ));
    }

    #[test]
    fn command_parser_accepts_bounded_operation_wait() {
        let operation_id = format!("job_{}", Uuid::now_v7());
        assert_eq!(
            parse_command(&[
                OsString::from("operation"),
                OsString::from("wait"),
                OsString::from(&operation_id),
                OsString::from("--path"),
                OsString::from("demo"),
                OsString::from("--timeout-seconds"),
                OsString::from("45"),
            ])
            .unwrap(),
            CliCommand::OperationWait {
                root: PathBuf::from("demo"),
                operation_id,
                timeout_seconds: 45,
            }
        );
        assert!(matches!(
            parse_command(&[
                OsString::from("operation"),
                OsString::from("wait"),
                OsString::from(format!("job_{}", Uuid::now_v7())),
                OsString::from("--timeout-seconds"),
                OsString::from("0"),
            ]),
            Err(CliError::InvalidOptionValue {
                option: "--timeout-seconds",
                ..
            })
        ));
    }

    #[test]
    fn command_parser_accepts_closed_apply_file() {
        assert_eq!(
            parse_command(&[
                OsString::from("apply"),
                OsString::from("--file"),
                OsString::from("policy.apply.json"),
                OsString::from("--path"),
                OsString::from("demo"),
                OsString::from("--timeout-seconds"),
                OsString::from("90"),
            ])
            .unwrap(),
            CliCommand::Apply {
                root: PathBuf::from("demo"),
                file: PathBuf::from("policy.apply.json"),
                timeout_seconds: 90,
            }
        );
        assert!(matches!(
            parse_command(&[OsString::from("apply")]),
            Err(CliError::MissingValue("--file"))
        ));
    }

    #[test]
    fn command_parser_accepts_closed_run_actions() {
        let run_id = format!("run_{}", Uuid::now_v7());
        assert_eq!(
            parse_command(&[
                OsString::from("run"),
                OsString::from("create"),
                OsString::from("--file"),
                OsString::from("run.json"),
                OsString::from("--path"),
                OsString::from("demo"),
            ])
            .unwrap(),
            CliCommand::RunCreate {
                root: PathBuf::from("demo"),
                file: PathBuf::from("run.json"),
            }
        );
        assert_eq!(
            parse_command(&[
                OsString::from("run"),
                OsString::from("pause"),
                OsString::from(&run_id),
                OsString::from("--path"),
                OsString::from("demo"),
            ])
            .unwrap(),
            CliCommand::RunControl {
                root: PathBuf::from("demo"),
                run_id: run_id.clone(),
                action: CliRunControlAction::Pause,
            }
        );
        assert_eq!(
            parse_command(&[
                OsString::from("run"),
                OsString::from("result"),
                OsString::from(&run_id),
            ])
            .unwrap(),
            CliCommand::RunResult {
                root: PathBuf::from("."),
                run_id,
            }
        );
        let watch_id = format!("run_{}", Uuid::now_v7());
        assert_eq!(
            parse_command(&[
                OsString::from("run"),
                OsString::from("watch"),
                OsString::from(&watch_id),
                OsString::from("--timeout-seconds"),
                OsString::from("120"),
                OsString::from("--path"),
                OsString::from("demo"),
            ])
            .unwrap(),
            CliCommand::RunWatch {
                root: PathBuf::from("demo"),
                run_id: watch_id,
                timeout_seconds: 120,
            }
        );
        assert!(matches!(
            parse_command(&[OsString::from("run"), OsString::from("create"),]),
            Err(CliError::MissingValue("--file"))
        ));
    }

    #[test]
    fn command_parser_accepts_closed_artifact_reads() {
        let artifact_id = format!("art_{}", Uuid::now_v7());
        assert_eq!(
            parse_command(&[
                OsString::from("artifact"),
                OsString::from("get"),
                OsString::from(&artifact_id),
                OsString::from("--path"),
                OsString::from("demo"),
            ])
            .unwrap(),
            CliCommand::ArtifactGet {
                root: PathBuf::from("demo"),
                artifact_id: artifact_id.clone(),
            }
        );
        assert_eq!(
            parse_command(&[
                OsString::from("artifact"),
                OsString::from("read"),
                OsString::from(&artifact_id),
                OsString::from("--output"),
                OsString::from("result.bin"),
            ])
            .unwrap(),
            CliCommand::ArtifactRead {
                root: PathBuf::from("."),
                artifact_id,
                output: PathBuf::from("result.bin"),
            }
        );
        assert!(matches!(
            parse_command(&[
                OsString::from("artifact"),
                OsString::from("read"),
                OsString::from(format!("art_{}", Uuid::now_v7())),
            ]),
            Err(CliError::MissingValue("--output"))
        ));

        assert_eq!(
            parse_command(&[
                OsString::from("artifact"),
                OsString::from("upload"),
                OsString::from("--file"),
                OsString::from("input.txt"),
                OsString::from("--purpose"),
                OsString::from("run_input"),
                OsString::from("--classification"),
                OsString::from("internal"),
                OsString::from("--media-type"),
                OsString::from("text/plain"),
                OsString::from("--timeout-seconds"),
                OsString::from("90"),
            ])
            .unwrap(),
            CliCommand::ArtifactUpload {
                root: PathBuf::from("."),
                file: PathBuf::from("input.txt"),
                purpose: "run_input".to_owned(),
                classification: "internal".to_owned(),
                media_type: Some("text/plain".to_owned()),
                display_name: None,
                timeout_seconds: 90,
            }
        );
    }

    #[test]
    fn command_parser_accepts_closed_task_actions() {
        let task_id = format!("int_{}", Uuid::now_v7());
        assert_eq!(
            parse_command(&[
                OsString::from("task"),
                OsString::from("get"),
                OsString::from(&task_id),
                OsString::from("--path"),
                OsString::from("demo"),
            ])
            .unwrap(),
            CliCommand::TaskGet {
                root: PathBuf::from("demo"),
                task_id: task_id.clone(),
            }
        );
        assert_eq!(
            parse_command(&[
                OsString::from("task"),
                OsString::from("submit-input"),
                OsString::from(&task_id),
                OsString::from("--file"),
                OsString::from("input.json"),
            ])
            .unwrap(),
            CliCommand::TaskResolve {
                root: PathBuf::from("."),
                task_id: task_id.clone(),
                action: CliTaskAction::SubmitInput,
                file: Some(PathBuf::from("input.json")),
            }
        );
        assert_eq!(
            parse_command(&[
                OsString::from("task"),
                OsString::from("approve"),
                OsString::from(&task_id),
            ])
            .unwrap(),
            CliCommand::TaskResolve {
                root: PathBuf::from("."),
                task_id,
                action: CliTaskAction::Approve,
                file: None,
            }
        );
        assert!(matches!(
            parse_command(&[
                OsString::from("task"),
                OsString::from("submit-input"),
                OsString::from(format!("int_{}", Uuid::now_v7())),
            ]),
            Err(CliError::MissingValue("--file"))
        ));
    }

    #[test]
    fn command_parser_accepts_the_closed_agent_journey() {
        assert!(matches!(
            parse_command(&[
                OsString::from("agent"),
                OsString::from("validate"),
                OsString::from("--file"),
                OsString::from("agent.yaml"),
                OsString::from("--output"),
                OsString::from("json"),
            ]),
            Ok(CliCommand::AgentValidate {
                file,
                output: agent::AgentOutputOptions {
                    mode: agent::AgentOutputMode::Json,
                    ..
                },
                ..
            }) if file == Path::new("agent.yaml")
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("agent"),
                OsString::from("publish"),
                OsString::from("--file"),
                OsString::from("agent.yaml"),
                OsString::from("--verbose"),
                OsString::from("--debug-authority"),
            ]),
            Ok(CliCommand::AgentPublish {
                wait: true,
                output: agent::AgentOutputOptions {
                    verbose: true,
                    debug_authority: true,
                    ..
                },
                ..
            })
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("agent"),
                OsString::from("run"),
                OsString::from("echo-agent"),
                OsString::from("--input"),
                OsString::from("{\"message\":\"hello\"}"),
                OsString::from("--detach"),
                OsString::from("--timeout-seconds"),
                OsString::from("90"),
            ]),
            Ok(CliCommand::AgentRun {
                selector,
                detach: true,
                timeout_seconds: 90,
                ..
            }) if selector == "echo-agent"
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("agent"),
                OsString::from("run"),
                OsString::from("echo-agent"),
                OsString::from("--input"),
                OsString::from("{}"),
                OsString::from("--file"),
                OsString::from("input.json"),
            ]),
            Err(CliError::Usage)
        ));
    }

    #[test]
    fn command_parser_accepts_version_and_exact_updates() {
        assert!(matches!(
            parse_command(&[OsString::from("version"), OsString::from("--json")]),
            Ok(CliCommand::Version { json: true })
        ));
        assert!(matches!(
            parse_command(&[OsString::from("update"), OsString::from("check")]),
            Ok(CliCommand::UpdateCheck)
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("update"),
                OsString::from("apply"),
                OsString::from("--version"),
                OsString::from("1.2.3"),
            ]),
            Ok(CliCommand::UpdateApply { version }) if version == "1.2.3"
        ));
        assert!(matches!(
            parse_command(&[
                OsString::from("update"),
                OsString::from("apply"),
                OsString::from("--version"),
                OsString::from("latest"),
            ]),
            Err(CliError::Release(_))
        ));
    }

    #[test]
    fn default_help_keeps_platform_automation_behind_advanced_help() {
        let default = usage();
        assert!(default.contains("insight agent publish"));
        assert!(!default.contains("insight apply --file"));
        let advanced = advanced_usage();
        assert!(advanced.contains("insight apply --file"));
        assert!(advanced.contains("insight artifact upload"));
        assert!(default.contains("insight update apply --version <exact-version>"));
    }
}
