//! Immutable CR-216 Armed runner.
//!
//! The runner starts inert, accepts one candidate- and boot-bound signed activation, fsyncs a
//! one-way latch before spawning the published Package argv, and exposes a read-only fixed result
//! operation.

use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{header, Response, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, JsonLimits, Sha256Digest,
};
use insight_platform_sandbox::opensandbox::{
    OpenSandboxId, RunnerBootId, SandboxActivationFrameV1, SandboxContractError,
    SandboxFailureClassV1, SandboxRunnerConfigV1, SandboxRunnerOutcomeV1, SandboxRunnerPhaseV1,
    SandboxRunnerResultFrameV1, SandboxRunnerStateFrameV1, MAX_SANDBOX_RUNNER_CONFIG_BYTES,
    OPENSANDBOX_ID_ENV, SANDBOX_CONTRACT_SCHEMA_VERSION, SANDBOX_RUNNER_CONFIG_DIGEST_ENV,
    SANDBOX_RUNNER_CONFIG_ENV, SANDBOX_RUNNER_UID,
};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read as _, Write as _},
    net::SocketAddr,
    os::unix::{
        fs::MetadataExt as _, fs::OpenOptionsExt as _, fs::PermissionsExt as _,
        process::CommandExt as _,
    },
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Mutex,
    time::{sleep, sleep_until, Instant},
};
use uuid::Uuid;

pub const RUNNER_LISTEN_ADDRESS: &str = "0.0.0.0:18080";
pub const RUNNER_STATE_DIRECTORY: &str = "/run/insight-sandbox/authority";
pub const RUNNER_LATCH_FILE: &str = "activation.latch";
pub const RUNNER_RESULT_FILE: &str = "result.frame";
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerStartupV1 {
    pub sandbox_id: OpenSandboxId,
    pub config: SandboxRunnerConfigV1,
}

impl RunnerStartupV1 {
    pub fn validate(&self) -> Result<(), RunnerError> {
        self.config
            .validate()
            .map_err(|_| RunnerError::InvalidConfiguration)
    }

    pub fn from_environment() -> Result<Self, RunnerError> {
        let raw = std::env::var(SANDBOX_RUNNER_CONFIG_ENV)
            .map_err(|_| RunnerError::InvalidConfiguration)?;
        if raw.len() > MAX_SANDBOX_RUNNER_CONFIG_BYTES {
            return Err(RunnerError::InvalidConfiguration);
        }
        let value = parse_strict_json(
            raw.as_bytes(),
            JsonLimits {
                max_bytes: MAX_SANDBOX_RUNNER_CONFIG_BYTES,
                max_depth: 16,
                max_items_per_array: 64,
                max_properties_per_object: 32,
                max_string_bytes: 16_384,
            },
        )
        .map_err(|_| RunnerError::InvalidConfiguration)?;
        let expected: Sha256Digest = std::env::var(SANDBOX_RUNNER_CONFIG_DIGEST_ENV)
            .map_err(|_| RunnerError::InvalidConfiguration)?
            .parse()
            .map_err(|_| RunnerError::InvalidConfiguration)?;
        let actual: Sha256Digest = canonical_digest(&value)
            .map_err(|_| RunnerError::InvalidConfiguration)?
            .parse()
            .map_err(|_| RunnerError::InvalidConfiguration)?;
        if actual != expected {
            return Err(RunnerError::InvalidConfiguration);
        }
        let config: SandboxRunnerConfigV1 =
            serde_json::from_value(value).map_err(|_| RunnerError::InvalidConfiguration)?;
        let startup = Self {
            sandbox_id: OpenSandboxId::parse(
                std::env::var(OPENSANDBOX_ID_ENV).map_err(|_| RunnerError::InvalidConfiguration)?,
            )
            .map_err(|_| RunnerError::InvalidConfiguration)?,
            config,
        };
        startup.validate()?;
        Ok(startup)
    }
}

#[derive(Debug, Clone)]
struct RunnerStorage {
    directory: PathBuf,
}

impl RunnerStorage {
    fn production() -> Self {
        Self {
            directory: PathBuf::from(RUNNER_STATE_DIRECTORY),
        }
    }

    #[cfg(test)]
    fn test(directory: PathBuf) -> Self {
        Self { directory }
    }

    fn latch_path(&self) -> PathBuf {
        self.directory.join(RUNNER_LATCH_FILE)
    }

    fn result_path(&self) -> PathBuf {
        self.directory.join(RUNNER_RESULT_FILE)
    }

    fn initialize(&self) -> Result<(), RunnerError> {
        fs::create_dir_all(&self.directory).map_err(|_| RunnerError::Storage)?;
        fs::set_permissions(&self.directory, fs::Permissions::from_mode(0o700))
            .map_err(|_| RunnerError::Storage)?;
        let metadata = fs::symlink_metadata(&self.directory).map_err(|_| RunnerError::Storage)?;
        if !metadata.file_type().is_dir()
            || metadata.permissions().mode() & 0o777 != 0o700
            || metadata.uid() != effective_uid()
        {
            return Err(RunnerError::Storage);
        }
        Ok(())
    }

    fn latch(&self, token_digest: &Sha256Digest) -> Result<LatchDisposition, RunnerError> {
        self.initialize()?;
        let path = self.latch_path();
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
        {
            Ok(mut file) => {
                file.write_all(token_digest.as_str().as_bytes())
                    .map_err(|_| RunnerError::Storage)?;
                file.sync_all().map_err(|_| RunnerError::Storage)?;
                sync_directory(&self.directory)?;
                Ok(LatchDisposition::Created)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = read_secure_file(&path, 71).map_err(|_| RunnerError::Storage)?;
                let existing = String::from_utf8(existing).map_err(|_| RunnerError::Storage)?;
                if existing == token_digest.as_str() {
                    Ok(LatchDisposition::Replayed)
                } else {
                    Ok(LatchDisposition::Conflict)
                }
            }
            Err(_) => Err(RunnerError::Storage),
        }
    }

    fn latch_digest(&self) -> Result<Option<Sha256Digest>, RunnerError> {
        match read_secure_file(&self.latch_path(), 71) {
            Ok(value) => String::from_utf8(value)
                .map_err(|_| RunnerError::Storage)?
                .parse()
                .map(Some)
                .map_err(|_| RunnerError::Storage),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(RunnerError::Storage),
        }
    }

    fn read_result(&self, maximum_bytes: u64) -> Result<Option<Vec<u8>>, RunnerError> {
        match read_secure_file(
            &self.result_path(),
            usize::try_from(maximum_bytes).unwrap_or(usize::MAX),
        ) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(RunnerError::Storage),
        }
    }

    fn write_result(&self, bytes: &[u8]) -> Result<(), RunnerError> {
        self.initialize()?;
        let temporary = self
            .directory
            .join(format!(".result-{}.tmp", Uuid::now_v7()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)
            .map_err(|_| RunnerError::Storage)?;
        file.write_all(bytes).map_err(|_| RunnerError::Storage)?;
        file.sync_all().map_err(|_| RunnerError::Storage)?;
        let result_path = self.result_path();
        if fs::symlink_metadata(&result_path).is_ok() {
            return Err(RunnerError::Storage);
        }
        fs::rename(&temporary, result_path).map_err(|_| RunnerError::Storage)?;
        sync_directory(&self.directory)
    }
}

fn read_secure_file(path: &Path, maximum: usize) -> std::io::Result<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file()
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.uid() != effective_uid()
        || metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX)
    {
        return Err(std::io::Error::other("runner state file is not secure"));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    std::io::Read::by_ref(&mut file)
        .take(u64::try_from(maximum).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(std::io::Error::other("runner state file exceeds limit"));
    }
    Ok(bytes)
}

fn effective_uid() -> u32 {
    // SAFETY: `geteuid` has no arguments and only reads the calling process credential.
    unsafe { libc::geteuid() }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LatchDisposition {
    Created,
    Replayed,
    Conflict,
}

#[derive(Debug, Clone)]
struct CurrentState {
    boot_id: RunnerBootId,
    phase: SandboxRunnerPhaseV1,
    result: Option<SandboxRunnerResultFrameV1>,
}

#[derive(Clone)]
pub struct RunnerCore {
    sandbox_id: OpenSandboxId,
    config: Arc<SandboxRunnerConfigV1>,
    storage: RunnerStorage,
    current: Arc<Mutex<CurrentState>>,
    enforce_package_boundary: bool,
}

impl RunnerCore {
    pub fn production(startup: RunnerStartupV1) -> Result<Self, RunnerError> {
        enable_child_subreaper()?;
        Self::new_with_boundary(startup, RunnerStorage::production(), true)
    }

    #[cfg(test)]
    fn new(startup: RunnerStartupV1, storage: RunnerStorage) -> Result<Self, RunnerError> {
        Self::new_with_boundary(startup, storage, false)
    }

    fn new_with_boundary(
        startup: RunnerStartupV1,
        storage: RunnerStorage,
        enforce_package_boundary: bool,
    ) -> Result<Self, RunnerError> {
        startup.validate()?;
        let RunnerStartupV1 { sandbox_id, config } = startup;
        storage.initialize()?;
        let maximum_result_bytes = config.maximum_output_bytes.saturating_add(65_536);
        let stored_result = storage.read_result(maximum_result_bytes)?;
        let result = stored_result
            .as_deref()
            .map(parse_stored_result)
            .transpose()?;
        let (boot_id, phase) = if let Some(result) = &result {
            let phase = match result.result {
                SandboxRunnerOutcomeV1::Succeeded { .. } => SandboxRunnerPhaseV1::Succeeded,
                SandboxRunnerOutcomeV1::Failed { .. } => SandboxRunnerPhaseV1::Failed,
            };
            (result.boot_id.clone(), phase)
        } else if storage.latch_digest()?.is_some() {
            (new_boot_id()?, SandboxRunnerPhaseV1::UnknownPriorActivation)
        } else {
            (new_boot_id()?, SandboxRunnerPhaseV1::Armed)
        };
        Ok(Self {
            sandbox_id,
            config: Arc::new(config),
            storage,
            current: Arc::new(Mutex::new(CurrentState {
                boot_id,
                phase,
                result,
            })),
            enforce_package_boundary,
        })
    }

    pub async fn state_frame(&self) -> Result<SandboxRunnerStateFrameV1, RunnerError> {
        let current = self.current.lock().await;
        self.state_frame_for(&current)
    }

    fn state_frame_for(
        &self,
        current: &CurrentState,
    ) -> Result<SandboxRunnerStateFrameV1, RunnerError> {
        SandboxRunnerStateFrameV1 {
            magic: String::new(),
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            sandbox_id: self.sandbox_id.clone(),
            boot_id: current.boot_id.clone(),
            execution_request_digest: self.config.execution_request_digest.clone(),
            phase: current.phase,
            frame_digest: zero_digest(),
        }
        .seal()
        .map_err(RunnerError::Contract)
    }

    pub async fn activate(
        &self,
        frame: SandboxActivationFrameV1,
    ) -> Result<ActivationDisposition, RunnerError> {
        validate_activation(&self.sandbox_id, &self.config, &frame)?;
        let signature_digest = frame
            .activation_signature
            .digest()
            .map_err(RunnerError::Contract)?;
        let mut current = self.current.lock().await;
        if current.boot_id != frame.boot_id {
            return Err(RunnerError::BootChanged);
        }
        if current.phase == SandboxRunnerPhaseV1::UnknownPriorActivation {
            return Ok(ActivationDisposition::Replayed(
                self.state_frame_for(&current)?,
            ));
        }
        match self.storage.latch(&signature_digest)? {
            LatchDisposition::Conflict => return Err(RunnerError::ActivationConflict),
            LatchDisposition::Replayed => {
                return Ok(ActivationDisposition::Replayed(
                    self.state_frame_for(&current)?,
                ));
            }
            LatchDisposition::Created => {}
        }
        if current.phase != SandboxRunnerPhaseV1::Armed {
            return Err(RunnerError::ActivationConflict);
        }
        current.phase = SandboxRunnerPhaseV1::ActivationLatched;
        let response = self.state_frame_for(&current)?;
        drop(current);

        let runner = self.clone();
        tokio::spawn(async move {
            runner.execute(frame.input).await;
        });
        Ok(ActivationDisposition::Applied(response))
    }

    async fn execute(&self, input: Value) {
        {
            let mut current = self.current.lock().await;
            if current.phase != SandboxRunnerPhaseV1::ActivationLatched {
                return;
            }
            current.phase = SandboxRunnerPhaseV1::Started;
        }
        let boot_id = { self.current.lock().await.boot_id.clone() };
        let result = match execute_package(&self.config, input, self.enforce_package_boundary).await
        {
            Ok(output) => SandboxRunnerOutcomeV1::Succeeded {
                output,
                output_schema_digest: self.config.output_schema_digest.clone(),
                output_digest: zero_digest(),
                declared_output_bytes: 0,
            },
            Err(failure) => SandboxRunnerOutcomeV1::Failed {
                failure_class: failure.class,
                diagnostic_digest: failure.diagnostic_digest,
                diagnostic_bytes: failure.diagnostic_bytes,
            },
        };
        let frame = SandboxRunnerResultFrameV1 {
            magic: String::new(),
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            execution_request_digest: self.config.execution_request_digest.clone(),
            boot_id,
            result,
            frame_digest: zero_digest(),
        }
        .seal();
        let Ok(frame) = frame else {
            return;
        };
        let Ok(bytes) = frame.canonical_bytes() else {
            return;
        };
        if self.storage.write_result(&bytes).is_err() {
            return;
        }
        let mut current = self.current.lock().await;
        current.phase = match frame.result {
            SandboxRunnerOutcomeV1::Succeeded { .. } => SandboxRunnerPhaseV1::Succeeded,
            SandboxRunnerOutcomeV1::Failed { .. } => SandboxRunnerPhaseV1::Failed,
        };
        current.result = Some(frame);
    }

    pub async fn result_bytes(&self) -> Result<Option<Vec<u8>>, RunnerError> {
        let current = self.current.lock().await;
        current
            .result
            .as_ref()
            .map(SandboxRunnerResultFrameV1::canonical_bytes)
            .transpose()
            .map_err(RunnerError::Contract)
    }

    pub fn router(self) -> Router {
        let maximum_body = usize::try_from(self.config.maximum_input_bytes)
            .unwrap_or(usize::MAX)
            .saturating_add(65_536);
        Router::new()
            .route("/v1/state", get(get_state))
            .route("/v1/activate", post(post_activate))
            .route("/v1/result", get(get_result))
            .layer(DefaultBodyLimit::max(maximum_body))
            .with_state(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationDisposition {
    Applied(SandboxRunnerStateFrameV1),
    Replayed(SandboxRunnerStateFrameV1),
}

impl ActivationDisposition {
    fn into_frame(self) -> SandboxRunnerStateFrameV1 {
        match self {
            Self::Applied(frame) | Self::Replayed(frame) => frame,
        }
    }
}

async fn get_state(State(runner): State<RunnerCore>) -> impl IntoResponse {
    match runner.state_frame().await {
        Ok(frame) => canonical_response(StatusCode::OK, &frame),
        Err(_) => safe_problem(StatusCode::INTERNAL_SERVER_ERROR, "sandbox_runner_failure"),
    }
}

async fn post_activate(State(runner): State<RunnerCore>, body: Bytes) -> impl IntoResponse {
    let frame = match parse_activation(&body, runner.config.maximum_input_bytes) {
        Ok(frame) => frame,
        Err(_) => {
            return safe_problem(StatusCode::BAD_REQUEST, "sandbox_activation_invalid");
        }
    };
    match runner.activate(frame).await {
        Ok(disposition) => canonical_response(StatusCode::OK, &disposition.into_frame()),
        Err(RunnerError::ActivationConflict) => {
            safe_problem(StatusCode::CONFLICT, "sandbox_activation_conflict")
        }
        Err(RunnerError::BootChanged) => {
            safe_problem(StatusCode::CONFLICT, "sandbox_runner_boot_changed")
        }
        Err(RunnerError::InvalidActivation | RunnerError::Contract(_)) => {
            safe_problem(StatusCode::BAD_REQUEST, "sandbox_activation_invalid")
        }
        Err(_) => safe_problem(StatusCode::INTERNAL_SERVER_ERROR, "sandbox_runner_failure"),
    }
}

async fn get_result(State(runner): State<RunnerCore>) -> impl IntoResponse {
    match runner.result_bytes().await {
        Ok(Some(bytes)) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, bytes.len())
            .body(Body::from(bytes))
            .unwrap_or_else(|_| safe_response(StatusCode::INTERNAL_SERVER_ERROR)),
        Ok(None) => safe_problem(StatusCode::TOO_EARLY, "sandbox_result_not_ready"),
        Err(_) => safe_problem(StatusCode::INTERNAL_SERVER_ERROR, "sandbox_runner_failure"),
    }
}

fn safe_problem(status: StatusCode, code: &'static str) -> Response<Body> {
    let bytes = serde_json::to_vec(&serde_json::json!({"code":code}))
        .unwrap_or_else(|_| b"{\"code\":\"sandbox_runner_failure\"}".to_vec());
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(Body::from(bytes))
        .unwrap_or_else(|_| safe_response(StatusCode::INTERNAL_SERVER_ERROR))
}

fn canonical_response<T: serde::Serialize>(status: StatusCode, value: &T) -> Response<Body> {
    let bytes = serde_json::to_value(value)
        .ok()
        .and_then(|value| canonical_json(&value).ok());
    let Some(bytes) = bytes else {
        return safe_response(StatusCode::INTERNAL_SERVER_ERROR);
    };
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(Body::from(bytes))
        .unwrap_or_else(|_| safe_response(StatusCode::INTERNAL_SERVER_ERROR))
}

fn safe_response(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::empty())
        .expect("static response is valid")
}

#[derive(Debug)]
struct PackageFailure {
    class: SandboxFailureClassV1,
    diagnostic_digest: Sha256Digest,
    diagnostic_bytes: u32,
}

async fn execute_package(
    config: &SandboxRunnerConfigV1,
    input: Value,
    enforce_package_boundary: bool,
) -> Result<Value, PackageFailure> {
    let input_bytes = canonical_json(&input).map_err(|_| {
        package_failure(
            SandboxFailureClassV1::RunnerFailure,
            b"input-canonicalization",
        )
    })?;
    if u64::try_from(input_bytes.len()).map_or(true, |length| length > config.maximum_input_bytes) {
        return Err(package_failure(
            SandboxFailureClassV1::RunnerFailure,
            b"input-too-large",
        ));
    }
    let mut command = Command::new(&config.package_argv[0]);
    command
        .args(&config.package_argv[1..])
        .env_clear()
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let maximum_processes = libc::rlim_t::from(config.maximum_processes);
    let package_uid = config.package_uid;
    // SAFETY: this closure executes after fork and before exec. It captures only Copy integers and
    // performs credential, capability, session, seccomp, and rlimit syscalls before returning the
    // OS error to `Command`; it does not touch shared application state.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if enforce_package_boundary {
                if libc::geteuid() != SANDBOX_RUNNER_UID || libc::setuid(package_uid) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                drop_package_capabilities()?;
                install_package_seccomp()?;
            }
            let limit = libc::rlimit {
                rlim_cur: maximum_processes,
                rlim_max: maximum_processes,
            };
            if libc::setrlimit(libc::RLIMIT_NPROC, &limit) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
    let mut child = command
        .spawn()
        .map_err(|_| package_failure(SandboxFailureClassV1::RunnerFailure, b"spawn-failed"))?;
    let process_group = child
        .id()
        .and_then(|pid| i32::try_from(pid).ok())
        .ok_or_else(|| {
            package_failure(
                SandboxFailureClassV1::RunnerFailure,
                b"process-group-missing",
            )
        })?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| package_failure(SandboxFailureClassV1::RunnerFailure, b"stdin-missing"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| package_failure(SandboxFailureClassV1::RunnerFailure, b"stdout-missing"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| package_failure(SandboxFailureClassV1::RunnerFailure, b"stderr-missing"))?;
    let stdin_task = tokio::spawn(async move {
        stdin.write_all(&input_bytes).await?;
        stdin.shutdown().await
    });
    let mut stdout_task = tokio::spawn(read_bounded(stdout, config.maximum_output_bytes));
    let mut stderr_task = tokio::spawn(read_bounded(
        stderr,
        u64::from(config.maximum_diagnostic_bytes),
    ));
    let deadline = Instant::now() + Duration::from_millis(config.wall_milliseconds);
    let mut status = None;
    let mut output = None;
    let mut diagnostic = None;
    loop {
        if status.is_some() && output.is_some() && diagnostic.is_some() {
            break;
        }
        tokio::select! {
            result = child.wait(), if status.is_none() => {
                status = Some(result.map_err(|_| package_failure(
                    SandboxFailureClassV1::RunnerFailure,
                    b"wait-failed",
                ))?);
            }
            result = &mut stdout_task, if output.is_none() => {
                match join_bounded(result) {
                    Ok(bytes) => output = Some(bytes),
                    Err(_) => {
                        let _ = terminate_package_group(&mut child, process_group).await;
                        return Err(package_failure(
                            SandboxFailureClassV1::OutputTooLarge,
                            b"output-too-large",
                        ));
                    }
                }
            }
            result = &mut stderr_task, if diagnostic.is_none() => {
                match join_bounded(result) {
                    Ok(bytes) => diagnostic = Some(bytes),
                    Err(_) => {
                        let _ = terminate_package_group(&mut child, process_group).await;
                        return Err(package_failure(
                            SandboxFailureClassV1::PackageFailed,
                            b"diagnostic-too-large",
                        ));
                    }
                }
            }
            () = sleep_until(deadline) => {
                let _ = terminate_package_group(&mut child, process_group).await;
                return Err(package_failure(
                    SandboxFailureClassV1::PackageTimedOut,
                    b"package-timeout",
                ));
            }
        }
    }
    let _ = stdin_task.await;
    let status = status.expect("loop exits only with status");
    let output = output.expect("loop exits only with output");
    let diagnostic = diagnostic.expect("loop exits only with diagnostic");
    terminate_package_group(&mut child, process_group)
        .await
        .map_err(|_| {
            package_failure(
                SandboxFailureClassV1::RunnerFailure,
                b"package-group-not-quiescent",
            )
        })?;
    if !status.success() {
        return Err(package_failure(
            SandboxFailureClassV1::PackageFailed,
            &diagnostic,
        ));
    }
    parse_strict_json(
        &output,
        JsonLimits {
            max_bytes: usize::try_from(config.maximum_output_bytes).unwrap_or(usize::MAX),
            max_depth: 32,
            max_items_per_array: 100_000,
            max_properties_per_object: 1_024,
            max_string_bytes: usize::try_from(config.maximum_output_bytes).unwrap_or(usize::MAX),
        },
    )
    .map_err(|_| package_failure(SandboxFailureClassV1::OutputInvalid, &diagnostic))
}

#[cfg(target_os = "linux")]
unsafe fn drop_package_capabilities() -> std::io::Result<()> {
    const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;

    #[repr(C)]
    struct CapabilityHeader {
        version: u32,
        pid: i32,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CapabilityData {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }

    let header = CapabilityHeader {
        version: LINUX_CAPABILITY_VERSION_3,
        pid: 0,
    };
    let empty = CapabilityData {
        effective: 0,
        permitted: 0,
        inheritable: 0,
    };
    let data = [empty; 2];
    // SAFETY: the kernel reads one fixed header and the two v3 capability words from these
    // stack-resident values and applies them only to the calling post-fork child.
    if unsafe {
        libc::syscall(
            libc::SYS_capset,
            &header as *const CapabilityHeader,
            data.as_ptr(),
        )
    } == 0
    {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
unsafe fn drop_package_capabilities() -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "package capability boundary requires Linux",
    ))
}

async fn read_bounded<R>(mut reader: R, maximum_bytes: u64) -> Result<Vec<u8>, RunnerError>
where
    R: AsyncRead + Unpin,
{
    let capacity = usize::try_from(maximum_bytes.min(65_536)).unwrap_or(65_536);
    let mut bytes = Vec::with_capacity(capacity);
    let mut buffer = [0_u8; 8_192];
    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .map_err(|_| RunnerError::PackageIo)?;
        if count == 0 {
            return Ok(bytes);
        }
        let next = u64::try_from(bytes.len())
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
        if next > maximum_bytes {
            return Err(RunnerError::ResultTooLarge);
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}

async fn terminate_package_group(
    child: &mut tokio::process::Child,
    process_group: i32,
) -> Result<(), RunnerError> {
    // SAFETY: the process group is the positive PID returned for the child after `setsid`.
    let killed = unsafe { libc::kill(-process_group, libc::SIGKILL) };
    if killed != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(RunnerError::PackageBoundary);
        }
    }
    let _ = child.wait().await;
    for _ in 0..100 {
        reap_adopted_children();
        // SAFETY: signal zero only tests whether the exact child process group still exists.
        let present = unsafe { libc::kill(-process_group, 0) };
        if present != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(());
            }
            return Err(RunnerError::PackageBoundary);
        }
        sleep(Duration::from_millis(10)).await;
    }
    Err(RunnerError::PackageBoundary)
}

fn reap_adopted_children() {
    loop {
        let mut status = 0;
        // SAFETY: `waitpid(-1, WNOHANG)` only reaps exited descendants adopted by this runner.
        let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if pid <= 0 {
            break;
        }
    }
}

#[cfg(target_os = "linux")]
fn enable_child_subreaper() -> Result<(), RunnerError> {
    // SAFETY: PR_SET_CHILD_SUBREAPER changes only this process's descendant reparenting policy.
    let result = unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(RunnerError::PackageBoundary)
    }
}

#[cfg(not(target_os = "linux"))]
fn enable_child_subreaper() -> Result<(), RunnerError> {
    Err(RunnerError::PackageBoundary)
}

#[cfg(target_os = "linux")]
unsafe fn install_package_seccomp() -> std::io::Result<()> {
    const BPF_LOAD_SYSCALL: u16 = 0x20;
    const BPF_JUMP_EQUAL: u16 = 0x15;
    const BPF_RETURN: u16 = 0x06;
    const SECCOMP_ACTION_ALLOW: u32 = 0x7fff_0000;
    const SECCOMP_ACTION_ERRNO: u32 = 0x0005_0000;

    const fn statement(code: u16, value: u32) -> libc::sock_filter {
        libc::sock_filter {
            code,
            jt: 0,
            jf: 0,
            k: value,
        }
    }
    const fn deny(syscall: libc::c_long) -> [libc::sock_filter; 2] {
        [
            libc::sock_filter {
                code: BPF_JUMP_EQUAL,
                jt: 0,
                jf: 1,
                k: syscall as u32,
            },
            statement(BPF_RETURN, SECCOMP_ACTION_ERRNO | libc::EPERM as u32),
        ]
    }

    let kill = deny(libc::SYS_kill);
    let tkill = deny(libc::SYS_tkill);
    let tgkill = deny(libc::SYS_tgkill);
    let queue = deny(libc::SYS_rt_sigqueueinfo);
    let thread_queue = deny(libc::SYS_rt_tgsigqueueinfo);
    let pidfd_signal = deny(libc::SYS_pidfd_send_signal);
    let setsid = deny(libc::SYS_setsid);
    let setpgid = deny(libc::SYS_setpgid);
    let unshare = deny(libc::SYS_unshare);
    let filter = [
        statement(BPF_LOAD_SYSCALL, 0),
        kill[0],
        kill[1],
        tkill[0],
        tkill[1],
        tgkill[0],
        tgkill[1],
        queue[0],
        queue[1],
        thread_queue[0],
        thread_queue[1],
        pidfd_signal[0],
        pidfd_signal[1],
        setsid[0],
        setsid[1],
        setpgid[0],
        setpgid[1],
        unshare[0],
        unshare[1],
        statement(BPF_RETURN, SECCOMP_ACTION_ALLOW),
    ];
    let program = libc::sock_fprog {
        len: u16::try_from(filter.len()).expect("fixed seccomp program length fits u16"),
        filter: filter.as_ptr().cast_mut(),
    };
    // SAFETY: both calls use fixed scalar arguments and a stack-resident immutable BPF program.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0
        || unsafe {
            libc::syscall(
                libc::SYS_seccomp,
                1_u32,
                0_u32,
                &program as *const libc::sock_fprog,
            )
        } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
unsafe fn install_package_seccomp() -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "package seccomp boundary requires Linux",
    ))
}

fn join_bounded(
    result: Result<Result<Vec<u8>, RunnerError>, tokio::task::JoinError>,
) -> Result<Vec<u8>, RunnerError> {
    result.map_err(|_| RunnerError::PackageIo)?
}

fn validate_activation(
    sandbox_id: &OpenSandboxId,
    config: &SandboxRunnerConfigV1,
    frame: &SandboxActivationFrameV1,
) -> Result<(), RunnerError> {
    frame
        .validate_wire(config.maximum_input_bytes)
        .map_err(RunnerError::Contract)?;
    let input_bytes = canonical_json(&frame.input).map_err(|_| RunnerError::InvalidActivation)?;
    let input_digest: Sha256Digest = canonical_digest(&frame.input)
        .map_err(|_| RunnerError::InvalidActivation)?
        .parse()
        .map_err(|_| RunnerError::InvalidActivation)?;
    if frame.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
        || &frame.sandbox_id != sandbox_id
        || frame.execution_request_digest != config.execution_request_digest
        || frame.input_schema_digest != config.input_schema_digest
        || frame.input_digest != config.input_digest
        || frame.input_digest != input_digest
        || frame
            .verify_signature(&config.activation_verifying_key)
            .is_err()
        || u64::try_from(input_bytes.len())
            .map_or(true, |length| length > config.maximum_input_bytes)
    {
        return Err(RunnerError::InvalidActivation);
    }
    Ok(())
}

fn parse_activation(
    bytes: &[u8],
    maximum_input_bytes: u64,
) -> Result<SandboxActivationFrameV1, RunnerError> {
    let maximum_bytes = maximum_input_bytes.saturating_add(65_536);
    let maximum_bytes =
        usize::try_from(maximum_bytes).map_err(|_| RunnerError::InvalidActivation)?;
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: maximum_bytes,
            max_depth: 32,
            max_items_per_array: 100_000,
            max_properties_per_object: 1_024,
            max_string_bytes: maximum_bytes,
        },
    )
    .map_err(|_| RunnerError::InvalidActivation)?;
    if canonical_json(&value).map_err(|_| RunnerError::InvalidActivation)? != bytes {
        return Err(RunnerError::InvalidActivation);
    }
    let frame: SandboxActivationFrameV1 =
        serde_json::from_value(value).map_err(|_| RunnerError::InvalidActivation)?;
    frame
        .validate_wire(maximum_input_bytes)
        .map_err(RunnerError::Contract)?;
    Ok(frame)
}

fn parse_stored_result(bytes: &[u8]) -> Result<SandboxRunnerResultFrameV1, RunnerError> {
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: bytes.len().max(1),
            max_depth: 32,
            max_items_per_array: 100_000,
            max_properties_per_object: 1_024,
            max_string_bytes: bytes.len().max(1),
        },
    )
    .map_err(|_| RunnerError::Storage)?;
    if canonical_json(&value).map_err(|_| RunnerError::Storage)? != bytes {
        return Err(RunnerError::Storage);
    }
    let parsed: SandboxRunnerResultFrameV1 =
        serde_json::from_value(value).map_err(|_| RunnerError::Storage)?;
    let resealed = parsed.clone().seal().map_err(RunnerError::Contract)?;
    if resealed.frame_digest != parsed.frame_digest {
        return Err(RunnerError::Storage);
    }
    Ok(parsed)
}

fn package_failure(class: SandboxFailureClassV1, diagnostic: &[u8]) -> PackageFailure {
    let bounded = &diagnostic[..diagnostic.len().min(65_536)];
    PackageFailure {
        class,
        diagnostic_digest: sha256_bytes(bounded),
        diagnostic_bytes: u32::try_from(bounded.len()).unwrap_or(65_536),
    }
}

fn sha256_bytes(bytes: &[u8]) -> Sha256Digest {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded.parse().expect("SHA-256 output is a valid digest")
}

fn zero_digest() -> Sha256Digest {
    format!("sha256:{}", "0".repeat(64))
        .parse()
        .expect("zero placeholder has the digest shape")
}

fn new_boot_id() -> Result<RunnerBootId, RunnerError> {
    RunnerBootId::parse(Uuid::now_v7().to_string()).map_err(RunnerError::Contract)
}

fn sync_directory(path: &Path) -> Result<(), RunnerError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| RunnerError::Storage)
}

#[derive(Debug)]
pub enum RunnerError {
    InvalidConfiguration,
    InvalidActivation,
    ActivationConflict,
    BootChanged,
    Storage,
    PackageIo,
    PackageBoundary,
    ResultTooLarge,
    Contract(SandboxContractError),
}

impl fmt::Display for RunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "runner configuration is invalid",
            Self::InvalidActivation => "runner activation is invalid",
            Self::ActivationConflict => "runner activation signature conflicts",
            Self::BootChanged => "runner boot identity changed",
            Self::Storage => "runner durable state is unavailable",
            Self::PackageIo => "runner package I/O failed",
            Self::PackageBoundary => "runner package boundary failed",
            Self::ResultTooLarge => "runner output exceeded its hard limit",
            Self::Contract(_) => "runner contract is invalid",
        })
    }
}

impl Error for RunnerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            _ => None,
        }
    }
}

pub async fn serve(startup: RunnerStartupV1) -> Result<(), RunnerError> {
    let runner = RunnerCore::production(startup)?;
    let address: SocketAddr = RUNNER_LISTEN_ADDRESS
        .parse()
        .map_err(|_| RunnerError::InvalidConfiguration)?;
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|_| RunnerError::PackageIo)?;
    axum::serve(listener, runner.router())
        .await
        .map_err(|_| RunnerError::PackageIo)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_sandbox::opensandbox::{
        ActivationSignature, OpaqueActivationToken, SANDBOX_PACKAGE_UID,
    };
    use tempfile::TempDir;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn token() -> OpaqueActivationToken {
        OpaqueActivationToken::parse("a".repeat(64)).unwrap()
    }

    fn config() -> SandboxRunnerConfigV1 {
        SandboxRunnerConfigV1 {
            schema_version: 1,
            execution_request_digest: digest('1'),
            input_schema_digest: digest('2'),
            input_digest: canonical_digest(&serde_json::json!({"input":true}))
                .unwrap()
                .parse()
                .unwrap(),
            output_schema_digest: digest('3'),
            activation_verifying_key: token().verifying_key().unwrap(),
            package_uid: SANDBOX_PACKAGE_UID,
            package_argv: vec!["/bin/echo".to_owned(), "{\"ok\":true}".to_owned()],
            maximum_input_bytes: 1_024,
            maximum_output_bytes: 1_024,
            maximum_diagnostic_bytes: 1_024,
            maximum_processes: 64,
            wall_milliseconds: 5_000,
        }
    }

    fn startup() -> RunnerStartupV1 {
        RunnerStartupV1 {
            sandbox_id: OpenSandboxId::parse("sandbox-one").unwrap(),
            config: config(),
        }
    }

    fn activation(boot_id: RunnerBootId) -> SandboxActivationFrameV1 {
        SandboxActivationFrameV1 {
            magic: String::new(),
            schema_version: 1,
            sandbox_id: OpenSandboxId::parse("sandbox-one").unwrap(),
            boot_id,
            execution_request_digest: digest('1'),
            input_schema_digest: digest('2'),
            input_digest: canonical_digest(&serde_json::json!({"input":true}))
                .unwrap()
                .parse()
                .unwrap(),
            declared_input_bytes: 0,
            input: serde_json::json!({"input":true}),
            activation_signature: ActivationSignature::parse("0".repeat(128)).unwrap(),
            frame_digest: zero_digest(),
        }
        .seal_with(&token())
        .unwrap()
    }

    #[tokio::test]
    async fn runner_is_armed_then_latches_once_and_publishes_atomic_result() {
        let temporary = TempDir::new().unwrap();
        let runner = RunnerCore::new(
            startup(),
            RunnerStorage::test(temporary.path().to_path_buf()),
        )
        .unwrap();
        let armed = runner.state_frame().await.unwrap();
        assert_eq!(armed.phase, SandboxRunnerPhaseV1::Armed);
        let frame = activation(armed.boot_id.clone());
        assert!(matches!(
            runner.activate(frame.clone()).await.unwrap(),
            ActivationDisposition::Applied(_)
        ));
        assert!(matches!(
            runner.activate(frame.clone()).await.unwrap(),
            ActivationDisposition::Replayed(_)
        ));
        for _ in 0..100 {
            if runner.result_bytes().await.unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let bytes = runner.result_bytes().await.unwrap().unwrap();
        let result = parse_stored_result(&bytes).unwrap();
        assert!(matches!(
            result.result,
            SandboxRunnerOutcomeV1::Succeeded { output, .. }
                if output == serde_json::json!({"ok":true})
        ));
        assert!(temporary.path().join(RUNNER_LATCH_FILE).is_file());
        assert!(temporary.path().join(RUNNER_RESULT_FILE).is_file());
        assert_eq!(
            fs::metadata(temporary.path().join(RUNNER_LATCH_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(temporary.path().join(RUNNER_RESULT_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(fs::read_dir(temporary.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")));

        let conflicting = SandboxActivationFrameV1 {
            activation_signature: ActivationSignature::parse("b".repeat(128)).unwrap(),
            frame_digest: zero_digest(),
            ..frame
        };
        assert!(matches!(
            runner.activate(conflicting).await,
            Err(RunnerError::InvalidActivation
                | RunnerError::ActivationConflict
                | RunnerError::Contract(_))
        ));
    }

    #[tokio::test]
    async fn restart_with_latch_and_no_result_never_restarts_package() {
        let temporary = TempDir::new().unwrap();
        let storage = RunnerStorage::test(temporary.path().to_path_buf());
        storage.initialize().unwrap();
        assert_eq!(
            storage.latch(&token().digest().unwrap()).unwrap(),
            LatchDisposition::Created
        );
        let restarted = RunnerCore::new(startup(), storage).unwrap();
        let state = restarted.state_frame().await.unwrap();
        assert_eq!(state.phase, SandboxRunnerPhaseV1::UnknownPriorActivation);
        let replay = restarted
            .activate(activation(state.boot_id))
            .await
            .unwrap()
            .into_frame();
        assert_eq!(replay.phase, SandboxRunnerPhaseV1::UnknownPriorActivation);
        assert!(restarted.result_bytes().await.unwrap().is_none());
    }

    #[test]
    fn config_is_closed_bounded_and_rejects_relative_or_shell_entrypoint() {
        let mut value = serde_json::to_value(config()).unwrap();
        value["runtime_installer"] = Value::Bool(true);
        assert!(serde_json::from_value::<SandboxRunnerConfigV1>(value).is_err());
        let mut invalid = config();
        invalid.package_argv = vec!["sh".to_owned(), "-c".to_owned(), "echo bad".to_owned()];
        assert!(invalid.validate().is_err());
        let mut oversized = config();
        oversized.maximum_output_bytes = 268_435_457;
        assert!(oversized.validate().is_err());

        let sealed = activation(RunnerBootId::parse("boot-one").unwrap());
        let mut noncanonical = canonical_json(&serde_json::to_value(sealed).unwrap()).unwrap();
        noncanonical.push(b' ');
        assert!(parse_activation(&noncanonical, 1_024).is_err());
    }
}
