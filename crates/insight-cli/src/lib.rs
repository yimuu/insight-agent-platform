//! Product-facing command-line entry points for the Platform `/v1` local development profile.
//!
//! This crate intentionally has no database, worker, or internal RPC dependency. It may inspect
//! host prerequisites and create project-local development state, but all future business
//! mutations must use the public Gateway `/v1` contract.

mod apply;
mod apply_journal;
mod artifact;
mod artifact_journal;
mod full_profile;
mod public_client;
mod run;
mod run_journal;
mod task;
mod task_journal;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use insight_platform_contracts::{
    canonical_digest, ArtifactPurpose, ArtifactRetentionPolicy, DataClassification, PublicJobState,
    ResourceId, ResourceKind, SandboxArtifactIoPolicyDocument, SchedulingPolicyDocument,
};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, PublicKeyData, SanType, PKCS_RSA_SHA256,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
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
const RUNTIME_CURSOR_KEY_FILE: &str = "run-event-cursor-key";
const RUNTIME_PROFILE_STATE_FILE: &str = "profile.json";
const RUNTIME_BUILD_STATE_FILE: &str = "build.json";
const RUNTIME_PROCESS_STATE_FILE: &str = "processes.json";
const RUNTIME_LOG_DIRECTORY: &str = "logs";
const LOCAL_ARTIFACT_BUCKET: &str = "insight-platform-artifacts";
const LOCAL_AWS_ENDPOINT: &str = "https://localhost.localstack.cloud:4566";
const LOCAL_SECRET_READINESS_NAME: &str = "insight/platform/readiness";
const LOCAL_SECRET_NAME_PREFIX: &str = "insight/platform/prepared";
const LOCAL_TEST_SECRET_READINESS_ARN: &str =
    "arn:aws:secretsmanager:us-east-1:000000000000:secret:insight/platform/readiness-local";
const PUBLIC_GATEWAY_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/public-gateway";
const SCHEDULER_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/scheduler";
const LOCAL_OIDC_AUDIENCE: &str = "insight.platform/v1";
const LOCAL_ACCESS_TOKEN_TTL_SECONDS: i64 = 900;
const EXPECTED_RUSTC_PREFIX: &str = "rustc 1.94.1";
const DEFAULT_PORTS: &[u16] = &[5432, 4222, 4566];

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
    Dev {
        root: PathBuf,
        profile: DevProfile,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DevProfile {
    Base,
    Full,
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
                    "unsupported development profile {profile:?}; use base or full"
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
        "init" => parse_init(&arguments[1..]),
        "token" => parse_token(&arguments[1..]),
        "dev" => parse_dev(&arguments[1..]),
        "status" => parse_status(&arguments[1..]),
        "logs" => parse_logs(&arguments[1..]),
        "stop" => parse_stop(&arguments[1..]),
        "apply" => parse_apply(&arguments[1..]),
        "operation" => parse_operation(&arguments[1..]),
        "run" => parse_run(&arguments[1..]),
        "task" => parse_task(&arguments[1..]),
        "artifact" => parse_artifact(&arguments[1..]),
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

fn parse_dev(arguments: &[OsString]) -> Result<CliCommand, CliError> {
    let mut root = None;
    let mut profile = None;
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
            "--profile" => {
                if profile.is_some() {
                    return Err(CliError::DuplicateOption("--profile"));
                }
                let Some(value) = arguments.get(cursor + 1).and_then(|value| value.to_str()) else {
                    return Err(CliError::MissingValue("--profile"));
                };
                profile = Some(match value {
                    "base" => DevProfile::Base,
                    "full" => DevProfile::Full,
                    _ => return Err(CliError::UnsupportedProfile(value.to_owned())),
                });
                cursor += 2;
            }
            _ => return Err(CliError::UnsupportedOption(flag.into_owned())),
        }
    }
    Ok(CliCommand::Dev {
        root: root.unwrap_or_else(|| PathBuf::from(".")),
        profile: profile.unwrap_or(DevProfile::Base),
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
            | "artifact-maintenance"
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
    )
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
    #[serde(default)]
    pub registry_validator_principal_id: String,
    #[serde(default)]
    pub egress_broker_principal_id: String,
    pub installation_principal_id: String,
    pub installation_request_id: String,
    pub bootstrap_config_digest: String,
    pub artifact_encryption_domain_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalProfileState {
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RuntimeProfileState {
    schema_version: u32,
    kind: String,
    /// Provenance of the source tree that first generated this immutable local profile closure.
    /// Build reuse is tracked separately; ordinary source edits must not rotate authority IDs,
    /// ports, TLS material, cursor keys, or policy bindings.
    #[serde(default)]
    source_fingerprint: String,
    kms_key_arn: String,
    #[serde(default)]
    secret_readiness_arn: String,
    s3_bucket: String,
    #[serde(default = "RuntimePortBindings::legacy_defaults")]
    ports: RuntimePortBindings,
    config_digests: BTreeMap<String, String>,
}

/// The loopback listeners assigned to one local profile.
///
/// These are persisted with the generated configuration so a stopped profile restarts on the
/// same endpoints.  They deliberately are not the well-known dependency ports (PostgreSQL/NATS),
/// which remain part of the fixed Docker development contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RuntimePortBindings {
    gateway_management: u16,
    gateway_runtime: u16,
    artifact_gateway: u16,
    artifact_gateway_observability: u16,
    artifact_data_controller: u16,
    artifact_data_guest: u16,
    artifact_data_observability: u16,
    orchestration_observability: u16,
    capability_native_observability: u16,
    registry_validation_observability: u16,
    #[serde(default)]
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
            artifact_data_guest: next()?,
            artifact_data_observability: next()?,
            orchestration_observability: next()?,
            capability_native_observability: next()?,
            registry_validation_observability: next()?,
            full: full_profile::PortBindings::allocate(&mut next)?,
        };
        drop(listeners);
        Ok(ports)
    }

    const fn legacy_defaults() -> Self {
        Self {
            gateway_management: 8081,
            gateway_runtime: 8080,
            artifact_gateway: 18081,
            artifact_gateway_observability: 19090,
            artifact_data_controller: 19443,
            artifact_data_guest: 19444,
            artifact_data_observability: 19091,
            orchestration_observability: 19092,
            capability_native_observability: 19093,
            registry_validation_observability: 19094,
            full: full_profile::PortBindings::legacy_defaults(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RuntimeBuildState {
    schema_version: u32,
    source_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RuntimeProcessState {
    schema_version: u32,
    kind: String,
    profile: String,
    compose_project: String,
    source_fingerprint: String,
    processes: BTreeMap<String, RuntimeProcessRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RuntimeProcessRecord {
    pid: u32,
    ready_address: String,
    log_file: String,
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
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::SECURITY_AUTHORITY_CERTIFICATE_FILE,
        full_profile::SECURITY_AUTHORITY_PRIVATE_KEY_FILE,
        &["localhost"],
        None,
        ExtendedKeyUsagePurpose::ServerAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::EGRESS_BROKER_CLIENT_CERTIFICATE_FILE,
        full_profile::EGRESS_BROKER_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::EGRESS_BROKER_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::MODEL_WORKER_CLIENT_CERTIFICATE_FILE,
        full_profile::MODEL_WORKER_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::MODEL_WORKER_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::CONTEXT_WORKER_CLIENT_CERTIFICATE_FILE,
        full_profile::CONTEXT_WORKER_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::CONTEXT_WORKER_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::MCP_HOST_CERTIFICATE_FILE,
        full_profile::MCP_HOST_PRIVATE_KEY_FILE,
        &["localhost"],
        None,
        ExtendedKeyUsagePurpose::ServerAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::MCP_RESOURCE_HOST_CERTIFICATE_FILE,
        full_profile::MCP_RESOURCE_HOST_PRIVATE_KEY_FILE,
        &["localhost"],
        None,
        ExtendedKeyUsagePurpose::ServerAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::MCP_HOST_EGRESS_CLIENT_CERTIFICATE_FILE,
        full_profile::MCP_HOST_EGRESS_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::MCP_HOST_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::MCP_RESOURCE_EGRESS_CLIENT_CERTIFICATE_FILE,
        full_profile::MCP_RESOURCE_EGRESS_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::MCP_HOST_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::CAPABILITY_REMOTE_CLIENT_CERTIFICATE_FILE,
        full_profile::CAPABILITY_REMOTE_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::CAPABILITY_WORKER_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::MCP_DISCOVERY_CLIENT_CERTIFICATE_FILE,
        full_profile::MCP_DISCOVERY_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::MCP_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE,
        full_profile::MCP_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::MCP_CLEANUP_CLIENT_CERTIFICATE_FILE,
        full_profile::MCP_CLEANUP_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::CALLBACK_CLIENT_CERTIFICATE_FILE,
        full_profile::CALLBACK_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::MCP_CALLBACK_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::CONTEXT_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE,
        full_profile::CONTEXT_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE,
        &[],
        Some(full_profile::CONTEXT_WORKER_WORKLOAD_IDENTITY),
        ExtendedKeyUsagePurpose::ClientAuth,
        &issuer,
    )?;
    write_local_leaf_certificate(
        &tls_directory,
        full_profile::EGRESS_BROKER_CERTIFICATE_FILE,
        full_profile::EGRESS_BROKER_PRIVATE_KEY_FILE,
        &["localhost"],
        None,
        ExtendedKeyUsagePurpose::ServerAuth,
        &issuer,
    )?;
    let mcp_state_directory = state_directory
        .join(RUNTIME_DIRECTORY)
        .join(full_profile::MCP_STATE_KEY_DIRECTORY);
    fs::create_dir(&mcp_state_directory).map_err(|source| CliError::InitializeProject {
        path: mcp_state_directory.display().to_string(),
        source,
    })?;
    write_sensitive_new(
        &mcp_state_directory.join(full_profile::MCP_STATE_KEY_FILE),
        &Sha256::digest(Uuid::now_v7().as_bytes()),
    )?;
    let mcp_oauth_state_directory = state_directory
        .join(RUNTIME_DIRECTORY)
        .join(full_profile::MCP_OAUTH_STATE_KEY_DIRECTORY);
    fs::create_dir(&mcp_oauth_state_directory).map_err(|source| CliError::InitializeProject {
        path: mcp_oauth_state_directory.display().to_string(),
        source,
    })?;
    write_sensitive_new(
        &mcp_oauth_state_directory.join(full_profile::MCP_OAUTH_STATE_KEY_FILE),
        &Sha256::digest(Uuid::now_v7().as_bytes()),
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

/// Writes the closed, digest-bound configuration that the independent local Platform roles read.
/// The caller obtains `kms_key_arn` from the pinned local S3/KMS dependency; this function never
/// discovers storage state itself and does not connect to PostgreSQL or any internal RPC.
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
        &RuntimePortBindings::legacy_defaults(),
    )
}

fn prepare_runtime_profile_with_ports(
    root: &Path,
    kms_key_arn: &str,
    secret_readiness_arn: &str,
    source_fingerprint: &str,
    ports: &RuntimePortBindings,
) -> Result<BTreeMap<String, String>, CliError> {
    if kms_key_arn.is_empty()
        || kms_key_arn.len() > 512
        || !kms_key_arn.is_ascii()
        || kms_key_arn.bytes().any(|byte| byte.is_ascii_control())
    {
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
        source_fingerprint,
        ports,
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
    source_fingerprint: &str,
    ports: &RuntimePortBindings,
) -> Result<BTreeMap<String, String>, CliError> {
    let jwks_path = state_directory
        .join(IDENTITY_DIRECTORY)
        .join(IDENTITY_JWKS_FILE);
    let jwks: serde_json::Value = serde_json::from_slice(&read_bounded_identity_file(&jwks_path)?)
        .map_err(|_| CliError::InvalidLocalIdentity {
            path: state_directory.display().to_string(),
        })?;
    let catalog = local_artifact_provider_catalog(kms_key_arn)?;
    let secret_provider_catalog = local_secret_provider_catalog(kms_key_arn, secret_readiness_arn)?;
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
                    "guest_listen_address": loopback_address(ports.artifact_data_guest),
                    "observability_listen_address": loopback_address(ports.artifact_data_observability),
                    "guest_identity": {
                        "issuer": identity.issuer,
                        "audience": "insight-platform-gvisor-guest",
                        "namespace": "insight-platform-sandbox-guests",
                        "service_account_name": "insight-platform-gvisor-guest",
                        "jwks": jwks,
                        "jwks_digest": identity.jwks_digest,
                    },
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
    configs.extend(full_profile::initial_configs(
        &ports.full,
        &catalog,
        full_profile::WorkerDigests {
            context_adapter: &local_digest("context-native-adapter")?,
            context_contract: &local_digest("context-native-contract")?,
            model_adapter: &local_digest("model-worker-adapter")?,
            anthropic_contract: &local_digest("model-anthropic-contract")?,
            openai_contract: &local_digest("model-openai-contract")?,
        },
        full_profile::EgressConfigInputs {
            service_principal_id: &identity.egress_broker_principal_id,
            secret_provider_catalog: &secret_provider_catalog,
            mcp_state_key_root: &fs::canonicalize(
                runtime.join(full_profile::MCP_STATE_KEY_DIRECTORY),
            )
            .map_err(|source| CliError::InitializeProject {
                path: runtime
                    .join(full_profile::MCP_STATE_KEY_DIRECTORY)
                    .display()
                    .to_string(),
                source,
            })?,
            mcp_state_key_path: &fs::canonicalize(
                runtime
                    .join(full_profile::MCP_STATE_KEY_DIRECTORY)
                    .join(full_profile::MCP_STATE_KEY_FILE),
            )
            .map_err(|source| CliError::InitializeProject {
                path: runtime
                    .join(full_profile::MCP_STATE_KEY_DIRECTORY)
                    .join(full_profile::MCP_STATE_KEY_FILE)
                    .display()
                    .to_string(),
                source,
            })?,
            mcp_state_key_reference_digest: &format!(
                "sha256:{}",
                lower_hex(&Sha256::digest(read_bounded_identity_file(
                    &runtime
                        .join(full_profile::MCP_STATE_KEY_DIRECTORY)
                        .join(full_profile::MCP_STATE_KEY_FILE),
                )?))
            ),
            mcp_oauth_state_key_root: &fs::canonicalize(
                runtime.join(full_profile::MCP_OAUTH_STATE_KEY_DIRECTORY),
            )
            .map_err(|source| CliError::InitializeProject {
                path: runtime
                    .join(full_profile::MCP_OAUTH_STATE_KEY_DIRECTORY)
                    .display()
                    .to_string(),
                source,
            })?,
            mcp_oauth_state_key_path: &fs::canonicalize(
                runtime
                    .join(full_profile::MCP_OAUTH_STATE_KEY_DIRECTORY)
                    .join(full_profile::MCP_OAUTH_STATE_KEY_FILE),
            )
            .map_err(|source| CliError::InitializeProject {
                path: runtime
                    .join(full_profile::MCP_OAUTH_STATE_KEY_DIRECTORY)
                    .join(full_profile::MCP_OAUTH_STATE_KEY_FILE)
                    .display()
                    .to_string(),
                source,
            })?,
            mcp_oauth_state_key_reference_digest: &format!(
                "sha256:{}",
                lower_hex(&Sha256::digest(read_bounded_identity_file(
                    &runtime
                        .join(full_profile::MCP_OAUTH_STATE_KEY_DIRECTORY)
                        .join(full_profile::MCP_OAUTH_STATE_KEY_FILE),
                )?))
            ),
            artifact_data_worker_port: ports.artifact_data_controller,
        },
    ));
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
    let profile = RuntimeProfileState {
        schema_version: 2,
        kind: "insight.dev.runtime-profile/v1".to_owned(),
        source_fingerprint: source_fingerprint.to_owned(),
        kms_key_arn: kms_key_arn.to_owned(),
        secret_readiness_arn: secret_readiness_arn.to_owned(),
        s3_bucket: LOCAL_ARTIFACT_BUCKET.to_owned(),
        ports: ports.clone(),
        config_digests: digests.clone(),
    };
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
    let valid = value.len() <= 2_048
        && value.is_ascii()
        && value.bytes().all(|byte| byte.is_ascii_graphic())
        && value
            .split_once(":secret:")
            .is_some_and(|(authority, name)| {
                authority.starts_with("arn:aws:secretsmanager:us-east-1:")
                    && name.starts_with("insight/platform/readiness-")
            });
    if valid {
        Ok(())
    } else {
        Err(CliError::InvalidLocalIdentity {
            path: root.join(PROJECT_DIRECTORY).display().to_string(),
        })
    }
}

fn local_secret_provider_catalog(
    kms_key_arn: &str,
    readiness_secret_arn: &str,
) -> Result<serde_json::Value, CliError> {
    let (authority, _) = readiness_secret_arn.split_once(":secret:").ok_or_else(|| {
        CliError::InvalidLocalIdentity {
            path: "local Secret provider configuration".to_owned(),
        }
    })?;
    let mut provider = serde_json::json!({
        "schema_version": 1,
        "provider_id": fresh_resource_id(ResourceKind::SecretProvider),
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

fn run_development_profile(
    workspace: &Path,
    root: &Path,
    profile: DevProfile,
) -> Result<String, CliError> {
    if profile == DevProfile::Full {
        return Err(CliError::RuntimeUnavailable(
            "the full profile is not available until its Model, remote Capability, MCP, Context, Egress/Security, Artifact Maintenance and WASI role closure is implemented; use `insight dev --profile base` for the implemented base profile"
                .to_owned(),
        ));
    }
    let workspace = workspace_root(workspace)?;
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let state_directory = root.join(PROJECT_DIRECTORY);
    let project = load_local_project_state(&state_directory)?;
    validate_loaded_local_identity(&state_directory, &project.identity)?;
    let runtime = state_directory.join(RUNTIME_DIRECTORY);
    let compose_project = compose_project_name(&project.identity.tenant_id)?;
    if let Some(existing) = read_runtime_process_state(&runtime)? {
        if existing
            .processes
            .values()
            .any(|process| process_is_running(process.pid))
        {
            return Err(CliError::RuntimeUnavailable(
                "a local Platform process is already running; use `insight status` or `insight stop`"
                    .to_owned(),
            ));
        }
    }

    let fingerprint = workspace_fingerprint(&workspace)?;
    compose_up(&workspace, &compose_project, &runtime)?;
    let profile_state = match read_runtime_profile_state(&runtime)? {
        Some(state) => state,
        None => {
            let (kms_key_arn, secret_readiness_arn) =
                initialize_localstack_artifact_dependency(&workspace, &compose_project, &runtime)?;
            let ports = RuntimePortBindings::allocate()?;
            prepare_runtime_profile_with_ports(
                &root,
                &kms_key_arn,
                &secret_readiness_arn,
                &fingerprint,
                &ports,
            )?;
            read_runtime_profile_state(&runtime)?.ok_or_else(|| {
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
    if profile == DevProfile::Full {
        if profile_state.secret_readiness_arn.is_empty() {
            return Err(CliError::RuntimeUnavailable(
                "the persisted local profile predates the full-profile Secret authority; initialize a fresh local project before starting `--profile full`"
                    .to_owned(),
            ));
        }
        ensure_localstack_secret_dependency(
            &workspace,
            &compose_project,
            &runtime,
            &profile_state.secret_readiness_arn,
        )?;
    }
    ensure_runtime_binaries(&workspace, &runtime, &fingerprint, profile)?;
    provision_and_bootstrap_authority(&workspace, &runtime, &project.identity, &profile_state)?;
    let processes = start_profile_processes(&workspace, &runtime, &profile_state, profile)?;
    let state = RuntimeProcessState {
        schema_version: 1,
        kind: "insight.dev.process-state/v1".to_owned(),
        profile: match profile {
            DevProfile::Base => "base",
            DevProfile::Full => "full",
        }
        .to_owned(),
        compose_project,
        source_fingerprint: fingerprint,
        processes,
    };
    write_runtime_json_replace(&runtime.join(RUNTIME_PROCESS_STATE_FILE), &state)?;
    render_runtime_status(&state)
}

fn runtime_status(root: &Path) -> Result<String, CliError> {
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let runtime = root.join(PROJECT_DIRECTORY).join(RUNTIME_DIRECTORY);
    let state = read_runtime_process_state(&runtime)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no local Platform process state exists; run `insight dev` first".to_owned(),
        )
    })?;
    render_runtime_status(&state)
}

fn runtime_logs(root: &Path, role: Option<&str>) -> Result<String, CliError> {
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let runtime = root.join(PROJECT_DIRECTORY).join(RUNTIME_DIRECTORY);
    let state = read_runtime_process_state(&runtime)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no local Platform process state exists; run `insight dev` first".to_owned(),
        )
    })?;
    let roles = match role {
        Some(role) => vec![role.to_owned()],
        None => state.processes.keys().cloned().collect(),
    };
    let mut output = String::new();
    for role in roles {
        let process = state.processes.get(&role).ok_or_else(|| {
            CliError::RuntimeState(format!("no log is registered for role {role}"))
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

fn stop_development_profile(workspace: &Path, root: &Path) -> Result<String, CliError> {
    workspace_root(workspace)?;
    let root = fs::canonicalize(root).map_err(|source| CliError::InitializeProject {
        path: root.display().to_string(),
        source,
    })?;
    let runtime = root.join(PROJECT_DIRECTORY).join(RUNTIME_DIRECTORY);
    let mut state = read_runtime_process_state(&runtime)?.ok_or_else(|| {
        CliError::RuntimeState(
            "no local Platform process state exists; run `insight dev` first".to_owned(),
        )
    })?;
    for process in state.processes.values() {
        stop_process(process.pid)?;
    }
    state.processes.clear();
    write_runtime_json_replace(&runtime.join(RUNTIME_PROCESS_STATE_FILE), &state)?;
    Ok(
        "stopped local Platform roles; PostgreSQL, NATS and LocalStack dependencies remain ready for durable restart\n"
            .to_owned(),
    )
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
        .current_dir(workspace)
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
            "deploy/dev/compose.yaml",
        ]);
    command
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
    if arn.is_empty() || arn.len() > 512 || !arn.is_ascii() {
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
    compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &[
            "s3api",
            "get-bucket-versioning",
            "--bucket",
            LOCAL_ARTIFACT_BUCKET,
        ],
    )?;
    compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &["kms", "describe-key", "--key-id", kms_key_arn],
    )?;
    Ok(())
}

fn ensure_localstack_secret_dependency(
    workspace: &Path,
    compose_project: &str,
    runtime: &Path,
    readiness_secret_arn: &str,
) -> Result<(), CliError> {
    compose_awslocal(
        workspace,
        compose_project,
        runtime,
        &[
            "secretsmanager",
            "describe-secret",
            "--secret-id",
            readiness_secret_arn,
        ],
    )?;
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
) -> Result<(), CliError> {
    let binaries = runtime_binary_paths(workspace, profile);
    let cached = read_runtime_json::<RuntimeBuildState>(&runtime.join(RUNTIME_BUILD_STATE_FILE))?
        .is_some_and(|state| state.schema_version == 1 && state.source_fingerprint == fingerprint);
    if cached && binaries.values().all(|path| path.is_file()) {
        return Ok(());
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
    if profile == DevProfile::Full {
        command.args([
            "-p",
            "insight-platform-context-worker",
            "--bin",
            "platform-context-worker",
            "--bin",
            "platform-remote-context-worker",
            "--bin",
            "platform-subscription-context-worker",
            "-p",
            "insight-platform-artifact-service",
            "--bin",
            "platform-artifact-maintenance",
            "-p",
            "insight-platform-security-authority",
            "--bin",
            "platform-security-authority",
            "-p",
            "insight-platform-egress-broker",
            "--bin",
            "platform-egress-broker",
            "-p",
            "insight-platform-model-worker",
            "--bin",
            "platform-model-worker",
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
            "insight-platform-capability-worker",
            "--bin",
            "platform-capability-remote-worker",
            "-p",
            "insight-platform-callback-api",
            "--bin",
            "platform-callback-api",
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
    write_runtime_json_replace(&runtime.join(RUNTIME_BUILD_STATE_FILE), &state)
}

fn base_runtime_binary_paths(workspace: &Path) -> BTreeMap<&'static str, PathBuf> {
    let suffix = std::env::consts::EXE_SUFFIX;
    BTreeMap::from([
        (
            "platform-schema",
            workspace
                .join("target/release")
                .join(format!("platform-schema{suffix}")),
        ),
        (
            "platform-dev-bootstrap",
            workspace
                .join("target/release")
                .join(format!("platform-dev-bootstrap{suffix}")),
        ),
        (
            "platform-registry-validation-worker",
            workspace
                .join("target/release")
                .join(format!("platform-registry-validation-worker{suffix}")),
        ),
        (
            "platform-gateway",
            workspace
                .join("target/release")
                .join(format!("platform-gateway{suffix}")),
        ),
        (
            "platform-artifact-gateway",
            workspace
                .join("target/release")
                .join(format!("platform-artifact-gateway{suffix}")),
        ),
        (
            "platform-artifact-data-worker",
            workspace
                .join("target/release")
                .join(format!("platform-artifact-data-worker{suffix}")),
        ),
        (
            "platform-orchestration-worker",
            workspace
                .join("target/release")
                .join(format!("platform-orchestration-worker{suffix}")),
        ),
        (
            "platform-capability-native-worker",
            workspace
                .join("target/release")
                .join(format!("platform-capability-native-worker{suffix}")),
        ),
    ])
}

fn runtime_binary_paths(workspace: &Path, profile: DevProfile) -> BTreeMap<&'static str, PathBuf> {
    let mut binaries = base_runtime_binary_paths(workspace);
    if profile == DevProfile::Full {
        let release = workspace.join("target/release");
        let suffix = std::env::consts::EXE_SUFFIX;
        binaries.extend(
            full_profile::INITIAL_BINARY_NAMES
                .map(|name| (name, release.join(format!("{name}{suffix}")))),
        );
    }
    binaries
}

fn provision_and_bootstrap_authority(
    workspace: &Path,
    runtime: &Path,
    identity: &LocalIdentityState,
    profile: &RuntimeProfileState,
) -> Result<(), CliError> {
    let database_url = "postgres://insight:insight@127.0.0.1:5432/insight_platform";
    let binaries = base_runtime_binary_paths(workspace);
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
    workspace: &Path,
    runtime: &Path,
    profile: &RuntimeProfileState,
    selected_profile: DevProfile,
) -> Result<BTreeMap<String, RuntimeProcessRecord>, CliError> {
    let binaries = runtime_binary_paths(workspace, selected_profile);
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
    if selected_profile == DevProfile::Full {
        specs.extend(full_profile_launch_specs(
            workspace,
            runtime,
            profile,
            database_url,
            &common_aws,
        ));
    }
    let mut started: BTreeMap<String, RuntimeProcessRecord> = BTreeMap::new();
    for spec in specs {
        let record = match spawn_runtime_process(&logs, &spec) {
            Ok(record) => record,
            Err(error) => {
                for process in started.values() {
                    let _ = stop_process(process.pid);
                }
                return Err(error);
            }
        };
        if let Err(error) = wait_for_ready(&record) {
            let _ = stop_process(record.pid);
            for process in started.values() {
                let _ = stop_process(process.pid);
            }
            return Err(error);
        }
        started.insert(spec.role.to_owned(), record);
    }
    Ok(started)
}

fn full_profile_launch_specs(
    workspace: &Path,
    runtime: &Path,
    profile: &RuntimeProfileState,
    database_url: &str,
    common_aws: &[(&str, &str)],
) -> Vec<RuntimeLaunchSpec> {
    let configuration = runtime.join(RUNTIME_CONFIGURATION_DIRECTORY);
    let tls = runtime.join(RUNTIME_TLS_DIRECTORY);
    full_profile::initial_process_launches(
        full_profile::ProcessPaths {
            release: &workspace.join("target/release"),
            configuration: &configuration,
            tls: &tls,
            ca_certificate_file: RUNTIME_CA_CERTIFICATE_FILE,
            nats_client_certificate_file: RUNTIME_NATS_CLIENT_CERTIFICATE_FILE,
            nats_client_private_key_file: RUNTIME_NATS_CLIENT_PRIVATE_KEY_FILE,
        },
        &profile.ports.full,
        &profile.config_digests,
        database_url,
        common_aws,
    )
    .into_iter()
    .map(|launch| RuntimeLaunchSpec {
        role: launch.role,
        binary: launch.binary,
        ready_address: launch.ready_address,
        environment: launch.environment,
        extra_environment: launch.extra_environment,
    })
    .collect()
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

fn spawn_runtime_process(
    logs: &Path,
    spec: &RuntimeLaunchSpec,
) -> Result<RuntimeProcessRecord, CliError> {
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
    let mut command = ProcessCommand::new(&spec.binary);
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
    command.process_group(0);
    let child = command
        .spawn()
        .map_err(|error| CliError::RuntimeUnavailable(format!("start {}: {error}", spec.role)))?;
    Ok(RuntimeProcessRecord {
        pid: child.id(),
        ready_address: spec.ready_address.clone(),
        log_file,
    })
}

fn wait_for_ready(process: &RuntimeProcessRecord) -> Result<(), CliError> {
    let deadline = SystemTime::now() + Duration::from_secs(30);
    while SystemTime::now() < deadline {
        if !process_is_running(process.pid) {
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

fn render_runtime_status(state: &RuntimeProcessState) -> Result<String, CliError> {
    let mut output = String::new();
    for (role, process) in &state.processes {
        let running = process_is_running(process.pid);
        let ready = running && http_ready(&process.ready_address);
        let status = if ready {
            "ready"
        } else if running {
            "starting_or_unready"
        } else {
            "stopped"
        };
        output.push_str(&format!(
            "{status:20} {role:22} pid={} readiness={}\n",
            process.pid, process.ready_address
        ));
    }
    if state.processes.is_empty() {
        output.push_str("stopped                 base profile has no running role\n");
    }
    Ok(output)
}

fn process_is_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        ProcessCommand::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

fn stop_process(pid: u32) -> Result<(), CliError> {
    #[cfg(unix)]
    {
        if !process_is_running(pid) {
            return Ok(());
        }
        let status = ProcessCommand::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .map_err(|error| {
                CliError::RuntimeUnavailable(format!("stop process {pid}: {error}"))
            })?;
        if !status.success() {
            return Err(CliError::RuntimeUnavailable(format!(
                "stop process {pid}: signal was rejected"
            )));
        }
        let deadline = SystemTime::now() + Duration::from_secs(10);
        while SystemTime::now() < deadline {
            if !process_is_running(pid) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Err(CliError::RuntimeUnavailable(format!(
            "process {pid} did not stop after SIGTERM"
        )))
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
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

fn read_runtime_profile_state(runtime: &Path) -> Result<Option<RuntimeProfileState>, CliError> {
    read_runtime_json(&runtime.join(RUNTIME_PROFILE_STATE_FILE))
}

fn read_runtime_process_state(runtime: &Path) -> Result<Option<RuntimeProcessState>, CliError> {
    read_runtime_json(&runtime.join(RUNTIME_PROCESS_STATE_FILE))
}

fn read_runtime_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, CliError> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|_| {
            CliError::RuntimeState(format!("{} is not valid closed JSON", path.display()))
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(CliError::InitializeProject {
            path: path.display().to_string(),
            source,
        }),
    }
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
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
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
    if !matches!(identity.schema_version, 2 | 3)
        || identity.issuer.is_empty()
        || identity.audience != LOCAL_OIDC_AUDIENCE
        || identity.key_id.is_empty()
        || identity.developer_subject.is_empty()
        || ResourceId::parse_expected(
            &identity.registry_validator_principal_id,
            ResourceKind::Principal,
        )
        .is_err()
        || (identity.schema_version == 2 && !identity.egress_broker_principal_id.is_empty())
        || (identity.schema_version == 3
            && ResourceId::parse_expected(
                &identity.egress_broker_principal_id,
                ResourceKind::Principal,
            )
            .is_err())
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
        CliCommand::Dev { root, profile } => {
            let root = resolve_root(current_directory, root);
            run_development_profile(current_directory, &root, profile)
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
    "Insight Platform development CLI\n\nUsage:\n  insight doctor [--json]\n  insight init [--path <directory>] [--name <name>]\n  insight token [--path <directory>]\n  insight dev [--path <directory>] [--profile base|full]\n  insight status [--path <directory>]\n  insight logs [--path <directory>] [--role <role>]\n  insight stop [--path <directory>]\n  insight apply --file <manifest.json> [--path <directory>] [--timeout-seconds <1..3600>]\n  insight run create --file <request.json> [--path <directory>]\n  insight run get <run_id> [--path <directory>]\n  insight run pause|resume|cancel <run_id> [--path <directory>]\n  insight run result <run_id> [--path <directory>]\n  insight run watch <run_id> [--timeout-seconds <1..3600>] [--path <directory>]\n  insight task get <task_id> [--path <directory>]\n  insight task submit-input <task_id> --file <input.json> [--path <directory>]\n  insight task approve|reject|cancel <task_id> [--path <directory>]\n  insight artifact upload --file <file> --purpose <purpose> --classification <classification> [--media-type <type>] [--display-name <name>] [--timeout-seconds <1..3600>] [--path <directory>]\n  insight artifact get <artifact_id> [--path <directory>]\n  insight artifact read <artifact_id> --output <file> [--path <directory>]\n  insight operation wait <job_id> [--path <directory>] [--timeout-seconds <1..3600>]\n\n`token` writes a short-lived local development token and prints it only to stdout. `apply` executes the explicit public Resource lifecycle. `run`, `task`, and `artifact` use only the Runtime Gateway `/v1` authority and emit closed machine-readable views. `run watch` reconnects with the opaque durable cursor and flushes JSON Lines incrementally. Artifact upload isolates its secret-bearing HTTPS target from the OIDC client; downloaded content is integrity-checked before atomic output. `operation wait` preserves a closed Problem response. The implemented `base` profile starts independent Platform roles; `full` remains unavailable until its role closure is implemented.\n"
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
    let profile = read_runtime_profile_state(&runtime)?.ok_or_else(|| {
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
            full_profile::SECURITY_AUTHORITY_CERTIFICATE_FILE,
            full_profile::EGRESS_BROKER_CLIENT_CERTIFICATE_FILE,
            full_profile::EGRESS_BROKER_CERTIFICATE_FILE,
            full_profile::MODEL_WORKER_CLIENT_CERTIFICATE_FILE,
            full_profile::CONTEXT_WORKER_CLIENT_CERTIFICATE_FILE,
            full_profile::MCP_HOST_CERTIFICATE_FILE,
            full_profile::MCP_RESOURCE_HOST_CERTIFICATE_FILE,
            full_profile::MCP_HOST_EGRESS_CLIENT_CERTIFICATE_FILE,
            full_profile::MCP_RESOURCE_EGRESS_CLIENT_CERTIFICATE_FILE,
            full_profile::CAPABILITY_REMOTE_CLIENT_CERTIFICATE_FILE,
            full_profile::MCP_DISCOVERY_CLIENT_CERTIFICATE_FILE,
            full_profile::MCP_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE,
            full_profile::MCP_CLEANUP_CLIENT_CERTIFICATE_FILE,
            full_profile::CONTEXT_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE,
            full_profile::CALLBACK_CLIENT_CERTIFICATE_FILE,
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
                full_profile::SECURITY_AUTHORITY_PRIVATE_KEY_FILE,
                full_profile::EGRESS_BROKER_CLIENT_PRIVATE_KEY_FILE,
                full_profile::EGRESS_BROKER_PRIVATE_KEY_FILE,
                full_profile::MODEL_WORKER_CLIENT_PRIVATE_KEY_FILE,
                full_profile::CONTEXT_WORKER_CLIENT_PRIVATE_KEY_FILE,
                full_profile::MCP_HOST_PRIVATE_KEY_FILE,
                full_profile::MCP_RESOURCE_HOST_PRIVATE_KEY_FILE,
                full_profile::MCP_HOST_EGRESS_CLIENT_PRIVATE_KEY_FILE,
                full_profile::MCP_RESOURCE_EGRESS_CLIENT_PRIVATE_KEY_FILE,
                full_profile::CAPABILITY_REMOTE_CLIENT_PRIVATE_KEY_FILE,
                full_profile::MCP_DISCOVERY_CLIENT_PRIVATE_KEY_FILE,
                full_profile::MCP_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE,
                full_profile::MCP_CLEANUP_CLIENT_PRIVATE_KEY_FILE,
                full_profile::CONTEXT_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE,
                full_profile::CALLBACK_CLIENT_PRIVATE_KEY_FILE,
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
        assert_eq!(fs::read(&mcp_state_key).unwrap().len(), 32);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            assert_eq!(
                fs::metadata(mcp_state_key).unwrap().permissions().mode() & 0o077,
                0
            );
        }
        let mcp_oauth_state_key = root
            .join(RUNTIME_DIRECTORY)
            .join(full_profile::MCP_OAUTH_STATE_KEY_DIRECTORY)
            .join(full_profile::MCP_OAUTH_STATE_KEY_FILE);
        assert_eq!(fs::read(&mcp_oauth_state_key).unwrap().len(), 32);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            assert_eq!(
                fs::metadata(mcp_oauth_state_key)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o077,
                0
            );
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
    fn init_refuses_to_overwrite_existing_state() {
        let directory = TempDir::new().unwrap();
        initialize_project(directory.path(), Some("demo"), UNIX_EPOCH).unwrap();
        assert!(matches!(
            initialize_project(directory.path(), Some("demo"), UNIX_EPOCH),
            Err(CliError::ProjectAlreadyInitialized(_))
        ));
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
            "sha256:test-profile-source",
        )
        .unwrap();
        assert_eq!(digests.len(), 22);
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
            ("context-native", full_profile::CONTEXT_NATIVE_CONFIG_FILE),
            (
                "artifact-maintenance",
                full_profile::ARTIFACT_MAINTENANCE_CONFIG_FILE,
            ),
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
        assert!(matches!(
            prepare_runtime_profile(
                directory.path(),
                "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012",
                "sha256:test-profile-source",
            ),
            Err(CliError::ProjectAlreadyInitialized(_))
        ));
    }

    #[test]
    fn full_profile_adds_only_its_closed_binary_set() {
        let workspace = Path::new("/workspace");
        let base = runtime_binary_paths(workspace, DevProfile::Base);
        let full = runtime_binary_paths(workspace, DevProfile::Full);
        for binary in full_profile::INITIAL_BINARY_NAMES {
            assert!(!base.contains_key(binary));
            assert!(full.contains_key(binary));
        }
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
    fn command_parser_accepts_base_profile_lifecycle_commands() {
        assert!(matches!(
            parse_command(&[
                OsString::from("dev"),
                OsString::from("--path"),
                OsString::from("demo"),
                OsString::from("--profile"),
                OsString::from("base"),
            ]),
            Ok(CliCommand::Dev {
                root,
                profile: DevProfile::Base,
            }) if root == Path::new("demo")
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
}
