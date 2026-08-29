//! Product-facing command-line entry points for the Platform `/v1` local development profile.
//!
//! This crate intentionally has no database, worker, or internal RPC dependency. It may inspect
//! host prerequisites and create project-local development state, but all future business
//! mutations must use the public Gateway `/v1` contract.

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

const PROJECT_DIRECTORY: &str = ".insight";
const PROJECT_STATE_FILE: &str = "project.json";
const PROJECT_GITIGNORE_FILE: &str = ".gitignore";
const PROJECT_KIND: &str = "insight.dev.project/v1";
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
            Self::DoctorFailed { .. } | Self::InitializeProject { .. } | Self::InvalidClock => 1,
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

fn lossy(value: &OsString) -> String {
    value.to_string_lossy().into_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalProjectState {
    pub schema_version: u32,
    pub kind: String,
    pub project_name: String,
    pub created_at_unix_seconds: u64,
    pub profiles: BTreeMap<String, LocalProfileState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalProfileState {
    pub state: String,
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
    let state = LocalProjectState {
        schema_version: 1,
        kind: PROJECT_KIND.to_owned(),
        project_name: name,
        created_at_unix_seconds,
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
    fs::create_dir(&state_directory).map_err(|source| CliError::InitializeProject {
        path: state_directory.display().to_string(),
        source,
    })?;
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
            let report = doctor_report(probe);
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
    }
}

fn usage() -> &'static str {
    "Insight Platform development CLI\n\nUsage:\n  insight doctor [--json]\n  insight init [--path <directory>] [--name <name>]\n\n`dev`, `status`, `logs`, and `stop` are added with the real M1 role closure; they are not aliases for the legacy runtime.\n"
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
    fn init_writes_gitignored_closed_local_state() {
        let directory = TempDir::new().unwrap();
        let state = initialize_project(
            directory.path(),
            Some("demo-project"),
            UNIX_EPOCH + std::time::Duration::from_secs(42),
        )
        .unwrap();
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
}
